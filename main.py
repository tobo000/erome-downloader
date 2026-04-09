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
from pyrogram.types import InputMediaPhoto, InputMediaVideo, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

app = Client("tobo_pro_session", api_id=int(API_ID), api_hash=API_HASH)
DOWNLOAD_DIR = "downloads"
DB_NAME = "bot_archive.db"
if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)
session = requests.Session()

cancel_tasks = {}

# --- 1. DATABASE (Kept logic) ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS processed (album_id TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

def is_processed(album_id):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed WHERE album_id = ?", (album_id,))
    res = cursor.fetchone()
    conn.close()
    return res is not None

def mark_processed(album_id):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO processed (album_id) VALUES (?)", (album_id,))
        conn.commit()
    except: pass
    conn.close()

# --- 2. HELPERS & ANIMATIONS (Kept Moon/Bars) ---

def create_progress_bar(current, total):
    if total <= 0: return "[░░░░░░░░░░] 0%"
    pct = min(100, (current / total) * 100)
    return f"[{'█' * int(pct/10)}{'░' * (10 - int(pct/10))}] {pct:.1f}%"

def get_human_size(num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0: return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

async def update_progress_msg(current, total, status_msg, start_time, action_text, topic=""):
    now = time.time()
    if now - start_time[0] > 4: 
        anims = ["🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘"]
        anim = anims[int(now % len(anims))]
        bar = create_progress_bar(current, total)
        try:
            await status_msg.edit_text(
                f"{anim} **{action_text}**\n"
                f"**Topic:** `{topic}`\n\n"
                f"{bar}\n"
                f"📦 **Original Size:** {get_human_size(current)} / {get_human_size(total)}"
            )
            start_time[0] = now
        except: pass

async def pyrogram_progress(current, total, status_msg, start_time, action_text, topic=""):
    await update_progress_msg(current, total, status_msg, start_time, action_text, topic)

def get_video_meta(video_path):
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8')
        data = json.loads(res)
        duration = int(float(data.get('format', {}).get('duration', 0)))
        video_stream = next((s for s in data.get('streams', []) if s['codec_type'] == 'video'), {})
        width, height = int(video_stream.get('width', 1280)), int(video_stream.get('height', 720))
        return duration, width, height
    except: return 0, 1280, 720

# --- 3. DOWNLOAD ENGINES (Kept Nitro) ---

def download_nitro_animated(url, path, headers, size, status_msg, loop, segs=4, action="Nitro", topic=""):
    chunk = size // segs
    downloaded_shared = [0]
    start_time = [time.time()]
    headers['Referer'] = 'https://www.erome.com/'
    def dl_part(s, e, n):
        pp = f"{path}.p{n}"; h = headers.copy(); h['Range'] = f'bytes={s}-{e}'
        try:
            with requests.get(url, headers=h, stream=True, timeout=60) as r:
                with open(pp, 'wb') as f:
                    for chk in r.iter_content(chunk_size=512*1024):
                        if chk:
                            f.write(chk); downloaded_shared[0] += len(chk)
                            asyncio.run_coroutine_threadsafe(update_progress_msg(downloaded_shared[0], size, status_msg, start_time, action, topic), loop)
        except: pass
    with ThreadPoolExecutor(max_workers=segs) as ex:
        for i in range(segs):
            start = i * chunk
            end = (i + 1) * chunk - 1 if i < segs - 1 else size - 1
            ex.submit(dl_part, start, end, i)
    with open(path, 'wb') as f:
        for i in range(segs):
            pp = f"{path}.p{i}"; pf = open(pp, 'rb'); f.write(pf.read()); pf.close(); os.remove(pp)

async def download_with_bar(url, path, headers, size, status_msg, action, topic=""):
    start_time = [time.time()]
    downloaded = 0
    headers['Referer'] = 'https://www.erome.com/'
    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk); downloaded += len(chunk)
                    await update_progress_msg(downloaded, size, status_msg, start_time, action, topic)

# --- 4. SCRAPER (Kept) ---

def scrape_album_details(url):
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/121.0.0.0', 'Referer': 'https://www.erome.com/'}
    try:
        res = session.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Untitled"
        p_l = [img.get('data-src') or img.get('src') for img in soup.select('div.img img')]
        p_l = ['https:' + x if x.startswith('//') else x for x in p_l if x]
        v_l = []
        for v_tag in soup.find_all(['source', 'video']):
            v_src = v_tag.get('src') or v_tag.get('data-src')
            if v_src: v_l.append('https:' + v_src if v_src.startswith('//') else v_src)
        v_l.extend(re.findall(r'https?://[^\s"\'>]+.mp4', res.text))
        v_l = list(dict.fromkeys([v for v in v_l if "erome.com" in v]))
        v_l.sort(key=lambda x: (re.search(r'(1080|720|high)', x.lower()) is None, x))
        return title, list(dict.fromkeys(p_l)), v_l
    except: return "Error", [], []

# --- 5. CORE DELIVERY ---

async def process_album(client, chat_id, reply_id, url, username, current, total):
    try: await client.get_chat(chat_id)
    except: pass
    album_id = url.rstrip('/').split('/')[-1]
    if is_processed(album_id): return True
    
    title, photos, videos = scrape_album_details(url)
    if not photos and not videos: return False
    
    user_folder = os.path.join(DOWNLOAD_DIR, username, album_id)
    os.makedirs(user_folder, exist_ok=True)
    
    status = await client.send_message(chat_id, f"📡 **[{current}/{total}] Preparing Archive**\nTopic: `{title}`", reply_to_message_id=reply_id)
    album_caption = (f"🎬 Topic: **{title}**\n"
                     f"👤 User: `{username}`\n"
                     f"📦 Original Quality")

    # PHOTOS (Group of 10 with Bar)
    if photos:
        photo_media = []
        for i, p_url in enumerate(photos, 1):
            action_p = f"🖼 Downloading Photo {i}/{len(photos)}"
            path = os.path.join(user_folder, f"p_{i}.jpg")
            try:
                r_h = session.get(p_url, headers={'Referer': 'https://www.erome.com/'}, stream=True, timeout=10)
                p_size = int(r_h.headers.get('content-length', 0))
                await download_with_bar(p_url, path, {}, p_size, status, action_p, title)
                photo_media.append(InputMediaPhoto(path))
            except: pass
        for i in range(0, len(photo_media), 10):
            chunk = photo_media[i:i+10]
            if i == 0: chunk[0].caption = album_caption
            try: await client.send_media_group(chat_id, chunk, reply_to_message_id=reply_id)
            except: pass
        for f in os.listdir(user_folder):
            if f.startswith("p_"): os.remove(os.path.join(user_folder, f))

    # VIDEOS (One-by-One with Bar)
    if videos:
        loop = asyncio.get_event_loop()
        for v_idx, v_url in enumerate(videos, 1):
            filepath = os.path.join(user_folder, f"v_{v_idx}.mp4")
            action_v = f"🎬 Downloading Video {v_idx}/{len(videos)}"
            try:
                headers = {'User-Agent': 'Mozilla/5.0 Chrome/121.0.0.0', 'Referer': 'https://www.erome.com/'}
                with requests.get(v_url, headers=headers, stream=True, timeout=15) as r:
                    size = int(r.headers.get('content-length', 0))
                
                if size > 15*1024*1024:
                    await loop.run_in_executor(None, download_nitro_animated, v_url, filepath, headers, size, status, loop, 4, action_v, title)
                else:
                    await download_with_bar(v_url, filepath, headers, size, status, action_v, title)

                if not os.path.exists(filepath): continue
                
                # Fix for Phone Playback
                subprocess.run(['ffmpeg', '-i', filepath, '-c', 'copy', '-movflags', 'faststart', filepath+'.tmp.mp4', '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if os.path.exists(filepath+'.tmp.mp4'): os.remove(filepath); os.rename(filepath+'.tmp.mp4', filepath)
                
                dur, w, h = get_video_meta(filepath)
                thumb = filepath + ".jpg"
                subprocess.run(['ffmpeg', '-ss', '1', '-i', filepath, '-vframes', '1', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                # Handshake before Upload
                try: await client.get_chat(chat_id)
                except: pass
                
                start_time_up = [time.time()]
                await client.send_video(
                    chat_id=chat_id, video=filepath, thumb=thumb if os.path.exists(thumb) else None,
                    width=w, height=h, duration=dur, supports_streaming=True, 
                    caption=album_caption if not photos and v_idx == 1 else "",
                    reply_to_message_id=reply_id, progress=pyrogram_progress, 
                    progress_args=(status, start_time_up, f"📤 Uploading Video {v_idx}/{len(videos)}", title)
                )
                if os.path.exists(filepath): os.remove(filepath)
                if os.path.exists(thumb): os.remove(thumb)
            except: pass

    try: os.rmdir(user_folder)
    except: pass
    mark_processed(album_id); await status.delete()
    return True

# --- HANDLERS ---

@app.on_message(filters.command("reset", prefixes="."))
async def reset_db(client, message):
    conn = sqlite3.connect(DB_NAME); cursor = conn.cursor()
    cursor.execute("DELETE FROM processed"); conn.commit(); conn.close()
    await message.reply("🧹 **Memory Cleared!**")

@app.on_message(filters.command("user", prefixes="."))
async def user_cmd(client, message):
    if len(message.command) < 2: return
    chat_id = message.chat.id
    try: await client.get_chat(chat_id)
    except: pass
    raw_input = message.command[1].strip()
    if "/a/" in raw_input:
        msg = await message.reply("🛰 **Link Detected...**")
        await process_album(client, chat_id, message.id, raw_input, "direct", 1, 1)
        return await msg.delete()
    query = raw_input.split("erome.com/")[-1].split('/')[0]
    cancel_tasks[chat_id] = False
    msg = await message.reply(f"🛰 **Scanning for `{query}`...**")
    all_urls = []
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/121.0.0.0'}
    for tab in ["", "/reposts"]:
        page = 1
        while True:
            url = f"https://www.erome.com/{query}{tab}?page={page}"
            try:
                res = session.get(url, headers=headers, timeout=15)
                if res.status_code != 200: break
                ids = re.findall(r'/a/([a-zA-Z0-9]+)', res.text)
                if not ids: break
                for aid in ids:
                    f_url = f"https://www.erome.com/a/{aid}"
                    if f_url not in all_urls: all_urls.append(f_url)
                if "Next" not in res.text: break
                page += 1; await msg.edit_text(f"🔍 Found {len(all_urls)} items...")
            except: break
    if not all_urls:
        page = 1
        while page <= 3:
            url = f"https://www.erome.com/search?v={query}&page={page}"
            try:
                res = session.get(url, headers=headers, timeout=15)
                if res.status_code != 200: break
                ids = re.findall(r'/a/([a-zA-Z0-9]+)', res.text)
                if not ids: break
                for aid in ids:
                    f_url = f"https://www.erome.com/a/{aid}"
                    if f_url not in all_urls: all_urls.append(f_url)
                page += 1; await msg.edit_text(f"🔍 Found {len(all_urls)} items...")
            except: break
    if not all_urls: return await msg.edit_text(f"❌ No content.")
    for i, url in enumerate(all_urls, 1):
        if cancel_tasks.get(chat_id): break
        await process_album(client, chat_id, message.id, url, query, i, len(all_urls))
    await msg.delete(); await message.reply(f"🏆 Completed for `{query}`!")

async def main():
    init_db()
    async with app:
        print("LOG: Ready and Working!")
        await idle()

if __name__ == "__main__":
    app.run(main())
