import os
import asyncio
import requests
import time
import subprocess
import json
import re
import sqlite3
from bs4 import BeautifulSoup
from pyrogram import Client, filters, idle
from pyrogram.types import InputMediaPhoto, InputMediaVideo
from pyrogram.errors import FloodWait, RPCError
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

app = Client("tobo_pro_session", api_id=int(API_ID), api_hash=API_HASH, sleep_threshold=60)
DOWNLOAD_DIR = "downloads"
DB_NAME = "bot_archive.db"
if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)

session = requests.Session()
executor = ThreadPoolExecutor(max_workers=8) # As per original
cancel_tasks = {}

# --- 2. DATABASE ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("CREATE TABLE IF NOT EXISTS processed (album_id TEXT PRIMARY KEY)")
    conn.commit(); conn.close()

def is_processed(album_id):
    conn = sqlite3.connect(DB_NAME)
    res = conn.execute("SELECT 1 FROM processed WHERE album_id = ?", (album_id,)).fetchone()
    conn.close(); return res is not None

def mark_processed(album_id):
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("INSERT INTO processed (album_id) VALUES (?)", (album_id,))
        conn.commit(); conn.close()
    except: pass

# --- 3. HELPERS & ANIMATIONS (Original Style) ---
async def safe_edit(msg, text):
    try: await msg.edit_text(text)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        try: await msg.edit_text(text)
        except: pass
    except: pass

def get_human_size(num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0: return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

async def pyrogram_progress(current, total, status_msg, start_time, action_text, topic=""):
    now = time.time()
    if now - start_time[0] > 10: 
        pct = min(100, (current / total) * 100) if total > 0 else 0
        bar = f"[{'█' * int(pct/10)}{'░' * (10 - int(pct/10))}] {pct:.1f}%"
        anims = ["🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘"]
        anim = anims[int(now % len(anims))]
        try:
            await status_msg.edit_text(
                f"{anim} **{action_text}**\n"
                f"Topic: `{topic[:30]}...`\n\n"
                f"{bar}\n"
                f"📦 **Original Size:** {get_human_size(current)} / {get_human_size(total)}"
            )
            start_time[0] = now
        except: pass

def get_video_meta(video_path):
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8')
        data = json.loads(res)
        duration = int(float(data.get('format', {}).get('duration', 0)))
        video_stream = next((s for s in data.get('streams', []) if s['codec_type'] == 'video'), {})
        return duration, int(video_stream.get('width', 1280)), int(video_stream.get('height', 720))
    except: return 0, 1280, 720

# --- 4. NITRO DOWNLOADER (Original Logic) ---
def download_nitro_animated(url, path, size, status_msg, loop, action, topic, segs=4):
    chunk = size // segs
    downloaded_shared = [0]
    start_time = [time.time()]
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/121.0.0.0', 'Referer': 'https://www.erome.com/'}
    def dl_part(s, e, n):
        pp = f"{path}.p{n}"; h = headers.copy(); h['Range'] = f'bytes={s}-{e}'
        try:
            with requests.get(url, headers=h, stream=True, timeout=60) as r:
                with open(pp, 'wb') as f:
                    for chk in r.iter_content(chunk_size=1024*1024):
                        if chk:
                            f.write(chk); downloaded_shared[0] += len(chk)
                            asyncio.run_coroutine_threadsafe(pyrogram_progress(downloaded_shared[0], size, status_msg, start_time, action, topic), loop)
        except: pass
    with ThreadPoolExecutor(max_workers=segs) as ex:
        futures = [ex.submit(dl_part, i*chunk, ((i+1)*chunk-1 if i<segs-1 else size-1), i) for i in range(segs)]
        for fut in futures: fut.result()
    with open(path, 'wb') as f:
        for i in range(segs):
            pp = f"{path}.p{i}"
            if os.path.exists(pp):
                pf = open(pp, 'rb'); f.write(pf.read()); pf.close(); os.remove(pp)

# --- 5. CORE PROCESS (Aggressive Scraper + Error Catching) ---
def scrape_album_details(url):
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/121.0.0.0', 'Referer': 'https://www.erome.com/'}
    try:
        res = session.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Untitled"
        p_l = [img.get('data-src') or img.get('src') for img in soup.select('div.img img')]
        p_l = ['https:' + x if x.startswith('//') else x for x in p_l if x]
        v_l = []
        for v_tag in soup.find_all(['source', 'video', 'a']):
            v_src = v_tag.get('src') or v_tag.get('data-src') or v_tag.get('href')
            if v_src and (".mp4" in v_src or ".gif" in v_src): 
                v_l.append('https:' + v_src if v_src.startswith('//') else v_src)
        # Aggressive Regex from Original Code
        v_l.extend(re.findall(r'https?://[^\s"\'>]+.mp4', res.text))
        v_l = list(dict.fromkeys([v for v in v_l if "erome.com" in v]))
        return title, list(dict.fromkeys(p_l)), v_l
    except: return "Error", [], []

async def process_album(client, chat_id, reply_id, url, username, current, total):
    try: await client.get_chat(chat_id)
    except: pass
    album_id = url.rstrip('/').split('/')[-1]
    if is_processed(album_id): return True
    
    loop = asyncio.get_running_loop()
    title, photos, videos = await loop.run_in_executor(executor, scrape_album_details, url)
    if not photos and not videos: return False
    
    user_folder = os.path.join(DOWNLOAD_DIR, f"{chat_id}_{album_id}")
    os.makedirs(user_folder, exist_ok=True)
    status = await client.send_message(chat_id, f"📡 **[{current}/{total}] Preparing Archive...**", reply_to_message_id=reply_id)

    # ORIGINAL KHMER CAPTION
    caption = (f"🎬 Topic: **{title}**\n"
               f"📂 Album: `{current}/{total}`\n"
               f"📊 Total: `{len(photos)}` Photo | `{len(videos)}` Video\n"
               f"👤 User: `{username.upper()}`\n"
               f"📦 Original Quality")

    if photos:
        photo_media = []
        for i, p_url in enumerate(photos, 1):
            if cancel_tasks.get(chat_id): break
            path = os.path.join(user_folder, f"p_{i}.jpg")
            try:
                def dl_p():
                    r = session.get(p_url, headers={'Referer': 'https://www.erome.com/'}, timeout=15)
                    with open(path, 'wb') as f: f.write(r.content)
                await loop.run_in_executor(executor, dl_p)
                photo_media.append(InputMediaPhoto(path))
            except: pass
        for i in range(0, len(photo_media), 10):
            chunk = photo_media[i:i+10]
            if i == 0: chunk[0].caption = caption
            try: 
                await client.send_media_group(chat_id, chunk, reply_to_message_id=reply_id)
                await asyncio.sleep(2)
            except Exception as e: print(f"Photo Upload Error: {e}")
        for f in os.listdir(user_folder):
            if f.startswith("p_"): os.remove(os.path.join(user_folder, f))

    if videos:
        for v_idx, v_url in enumerate(videos, 1):
            if cancel_tasks.get(chat_id): break
            filepath = os.path.join(user_folder, f"v_{v_idx}" + (".gif" if ".gif" in v_url else ".mp4"))
            try:
                r_head = session.head(v_url, headers={'Referer': 'https://www.erome.com/'})
                size = int(r_head.headers.get('content-length', 0))
                await loop.run_in_executor(executor, download_nitro_animated, v_url, filepath, size, status, loop, f"🎬 Downloading Video {v_idx}/{len(videos)}", title)
                
                final_v = filepath + ".stream.mp4"
                subprocess.run(['ffmpeg', '-i', filepath, '-c', 'copy', '-movflags', 'faststart', final_v, '-y'], stderr=subprocess.DEVNULL)
                if os.path.exists(final_v): os.replace(final_v, filepath)
                
                dur, w, h = get_video_meta(filepath)
                thumb = filepath + ".jpg"
                subprocess.run(['ffmpeg', '-ss', '1', '-i', filepath, '-vframes', '1', thumb, '-y'], stderr=subprocess.DEVNULL)
                
                try:
                    await client.send_video(
                        chat_id=chat_id, video=filepath, thumb=thumb if os.path.exists(thumb) else None,
                        width=w, height=h, duration=dur, supports_streaming=True, 
                        caption=caption if not photos and v_idx == 1 else "",
                        reply_to_message_id=reply_id, progress=pyrogram_progress, 
                        progress_args=(status, [time.time()], f"📤 Uploading Video {v_idx}/{len(videos)}", title)
                    )
                except Exception as e:
                    print(f"Video Upload Error: {e}")
                    await client.send_message(chat_id, f"⚠️ Upload failed for video {v_idx} in `{title}`")
                
                await asyncio.sleep(2)
                if os.path.exists(filepath): os.remove(filepath)
                if os.path.exists(thumb): os.remove(thumb)
            except Exception as e: print(f"Processing Error: {e}")

    mark_processed(album_id)
    await status.delete(); return True

# --- 6. HANDLERS (Formatted Scanner + Original Search Engine) ---

@app.on_message(filters.command("user", prefixes="."))
async def user_cmd(client, message):
    if len(message.command) < 2: return
    chat_id = message.chat.id; raw_input = message.command[1].strip(); cancel_tasks[chat_id] = False
    
    if "/a/" in raw_input:
        await process_album(client, chat_id, message.id, raw_input, "direct", 1, 1)
        return

    query = raw_input.split("erome.com/")[-1].split('/')[0]
    msg = await message.reply(f"🛰 **Initializing Scanner for `{query}`...**")

    all_urls = []
    total_p, total_v = 0, 0
    # Search both profile and search engine as per original code
    scan_targets = [f"https://www.erome.com/{query}", f"https://www.erome.com/search?v={query}"]
    scan_anims = ["🔍", "🔎", "📡", "🛰"]

    for base_url in scan_targets:
        page = 1
        while page <= 15:
            if cancel_tasks.get(chat_id): break
            try:
                res = session.get(f"{base_url}?page={page}" if "?" in base_url else f"{base_url}?page={page}", headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
                soup = BeautifulSoup(res.text, 'html.parser')
                albums = soup.select('div.album-link') 
                if not albums: break
                
                for album in albums:
                    link_tag = album.find('a', class_='album-title')
                    if link_tag:
                        album_url = "https://www.erome.com" + link_tag.get('href')
                        if album_url not in all_urls:
                            all_urls.append(album_url)
                            # Extract stats from listing page
                            p_tag = album.select_one('span.album-photos')
                            v_tag = album.select_one('span.album-videos')
                            if p_tag: total_p += int(re.sub(r'\D', '', p_tag.text))
                            if v_tag: total_v += int(re.sub(r'\D', '', v_tag.text))

                await safe_edit(msg, f"{scan_anims[page%4]} **Scanning Page {page}...**\n\n"
                                     f"📦 Albums: `{len(all_urls)}` 📂\n"
                                     f"🖼 Photos: `{total_p}` 🖼\n"
                                     f"🎬 Videos: `{total_v}` 🎬")
                if "Next" not in res.text: break
                page += 1; await asyncio.sleep(0.5)
            except: break
        if all_urls: break

    if not all_urls: return await safe_edit(msg, f"❌ No content for `{query}`.")
    
    await safe_edit(msg, f"✅ **Scanner Complete!**\n\n"
                         f"📊 Total Albums: `{len(all_urls)}` 📂\n"
                         f"🖼 Total Photos: `{total_p}` 🖼\n"
                         f"🎬 Total Videos: `{total_v}` 🎬")
    await asyncio.sleep(3); await msg.delete()

    for i, url in enumerate(all_urls, 1):
        if cancel_tasks.get(chat_id): break
        await process_album(client, chat_id, message.id, url, query, i, len(all_urls))

@app.on_message(filters.command("reset", prefixes="."))
async def reset_db(client, message):
    conn = sqlite3.connect(DB_NAME); conn.execute("DELETE FROM processed"); conn.commit(); conn.close()
    await message.reply("🧹 **Memory Cleared!**")

@app.on_message(filters.command("cancel", prefixes="."))
async def cancel_cmd(client, message):
    cancel_tasks[message.chat.id] = True
    await message.reply("🛑 **Cancellation Sent!**")

async def main():
    init_db()
    async with app:
        print("LOG: Formatted 100% Original Feature Bot Started!")
        await idle()

if __name__ == "__main__":
    app.run(main())
