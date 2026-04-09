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
if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)
session = requests.Session()

cancel_tasks = {}

# --- 1. DATABASE ---
def init_db():
    conn = sqlite3.connect("bot_archive.db")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS processed (album_id TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

def is_processed(album_id):
    conn = sqlite3.connect("bot_archive.db")
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed WHERE album_id = ?", (album_id,))
    res = cursor.fetchone()
    conn.close()
    return res is not None

def mark_processed(album_id):
    conn = sqlite3.connect("bot_archive.db")
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO processed (album_id) VALUES (?)", (album_id,))
        conn.commit()
    except: pass
    conn.close()

# --- 2. HELPERS & ANIMATIONS ---

def create_progress_bar(current, total):
    if total <= 0: return "[░░░░░░░░░░] 0%"
    pct = min(100, (current / total) * 100)
    return f"[{'█' * int(pct/10)}{'░' * (10 - int(pct/10))}] {pct:.1f}%"

def get_human_size(num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0: return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

async def update_progress_msg(current, total, status_msg, start_time, action_text):
    now = time.time()
    if now - start_time[0] > 4: 
        anims = ["🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘"]
        anim = anims[int(now % len(anims))]
        bar = create_progress_bar(current, total)
        try:
            await status_msg.edit_text(
                f"{anim} **{action_text}**\n\n{bar}\n📦 **Progress:** {get_human_size(current)} / {get_human_size(total)}"
            )
            start_time[0] = now
        except: pass

async def pyrogram_progress(current, total, status_msg, start_time, action_text):
    await update_progress_msg(current, total, status_msg, start_time, action_text)

def get_video_meta(video_path):
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8')
        data = json.loads(res)
        duration = int(float(data.get('format', {}).get('duration', 0)))
        streams = data.get('streams', [])
        v = next((s for s in streams if s['codec_type'] == 'video'), {})
        width = int(v.get('width', 1280))
        height = int(v.get('height', 720))
        has_audio = any(s['codec_type'] == 'audio' for s in streams)
        return duration, width, height, has_audio
    except: return 0, 1280, 720, True

async def download_with_bar(url, path, headers, size, status_msg, action):
    start_time = [time.time()]
    downloaded = 0
    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    await update_progress_msg(downloaded, size, status_msg, start_time, action)

def download_nitro_animated(url, path, headers, size, status_msg, loop, segs=4, action="Nitro"):
    chunk = size // segs
    downloaded_shared = [0]
    start_time = [time.time()]
    def dl_part(s, e, n):
        pp = f"{path}.p{n}"; h = headers.copy(); h['Range'] = f'bytes={s}-{e}'
        try:
            with requests.get(url, headers=h, stream=True, timeout=60) as r:
                with open(pp, 'wb') as f:
                    for chk in r.iter_content(chunk_size=512*1024):
                        if chk:
                            f.write(chk)
                            downloaded_shared[0] += len(chk)
                            asyncio.run_coroutine_threadsafe(
                                update_progress_msg(downloaded_shared[0], size, status_msg, start_time, action), loop
                            )
        except: pass
    with ThreadPoolExecutor(max_workers=segs) as ex:
        for i in range(segs):
            start = i * chunk
            end = (i + 1) * chunk - 1 if i < segs - 1 else size - 1
            ex.submit(dl_part, start, end, i)
    with open(path, 'wb') as f:
        for i in range(segs):
            pp = f"{path}.p{i}"; pf = open(pp, 'rb'); f.write(pf.read()); pf.close(); os.remove(pp)

# ==========================================
# SCRAPER
# ==========================================

def scrape_album_details(url):
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/121.0.0.0', 'Referer': 'https://www.erome.com/'}
    try:
        res = session.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Untitled"
        p_l = ['https:' + x.get('data-src') if x.get('data-src', '').startswith('//') else x.get('data-src') or x.get('src') for x in soup.select('div.img img')]
        v_l = []
        for v_tag in soup.find_all('source'):
            v_src = v_tag.get('src')
            if v_src and ".mp4" in v_src: v_l.append('https:' + v_src if v_src.startswith('//') else v_src)
        v_l.extend(re.findall(r'https?://[^\s"\'>]+.mp4', res.text))
        v_l = list(dict.fromkeys([v for v in v_l if "erome.com" in v]))
        v_l.sort(key=lambda x: (re.search(r'(1080|720|high)', x.lower()) is None, x))
        return title, list(dict.fromkeys(p_l)), v_l
    except: return "Error", [], []

# ==========================================
# CORE DELIVERY
# ==========================================

async def process_album(client, chat_id, reply_id, url, username, current, total):
    # Peer Resolution Fix: Double check chat before starting
    try: await client.get_chat(chat_id)
    except: pass

    album_id = url.rstrip('/').split('/')[-1]
    if is_processed(album_id): return True
    
    title, photos, videos = scrape_album_details(url)
    if not photos and not videos: return False
    
    user_folder = os.path.join(DOWNLOAD_DIR, username, album_id)
    if not os.path.exists(user_folder): os.makedirs(user_folder, exist_ok=True)
    
    status = await client.send_message(chat_id, f"📡 **[{current}/{total}] Preparing:** `{title}`", reply_to_message_id=reply_id)
    album_caption = f"🎬 **{title}**\n👤 User: `{username}`\n📦 Content: {len(photos)}🖼 {len(videos)}🎬"

    # --- 1. PHOTOS (Groups of 10) ---
    if photos:
        photo_media = []
        for i, p_url in enumerate(photos, 1):
            await status.edit_text(f"🖼 **Downloading Photo {i}/{len(photos)}**\nAlbum: `{title}`")
            path = os.path.join(user_folder, f"p_{i}.jpg")
            try:
                r = session.get(p_url, timeout=30)
                with open(path, 'wb') as f: f.write(r.content)
                photo_media.append(InputMediaPhoto(path))
            except: pass
        
        for i in range(0, len(photo_media), 10):
            chunk = photo_media[i:i+10]
            if i == 0: chunk[0].caption = album_caption
            try: await client.send_media_group(chat_id, chunk, reply_to_message_id=reply_id)
            except: pass
        
        # Cleanup photos
        for f in os.listdir(user_folder):
            if f.startswith("p_"): os.remove(os.path.join(user_folder, f))

    # --- 2. VIDEOS (Sent One-By-One) ---
    if videos:
        loop = asyncio.get_event_loop()
        for v_idx, v_url in enumerate(videos, 1):
            filepath = os.path.join(user_folder, f"v_{v_idx}.mp4")
            headers = {'User-Agent': 'Mozilla/5.0 Chrome/121.0.0.0', 'Referer': url}
            action_name = f"🎬 Downloading Video {v_idx}/{len(videos)}"
            
            try:
                with requests.get(v_url, headers=headers, stream=True, timeout=15) as r:
                    size = int(r.headers.get('content-length', 0))
                
                # Nitro vs Standard Download with Bar
                if size > 15*1024*1024:
                    await loop.run_in_executor(None, download_nitro_animated, v_url, filepath, headers, size, status, loop, 4, action_name)
                else:
                    await download_with_bar(v_url, filepath, headers, size, status, action_name)

                if not os.path.exists(filepath): continue

                dur, w, h, has_audio = get_video_meta(filepath)
                thumb = filepath + ".jpg"
                subprocess.run(['ffmpeg', '-ss', '1', '-i', filepath, '-vframes', '1', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                # --- PEER FIX BEFORE VIDEO UPLOAD ---
                try: await client.get_chat(chat_id)
                except: pass
                
                start_time = [time.time()]
                await client.send_video(
                    chat_id=chat_id, video=filepath, thumb=thumb if os.path.exists(thumb) else None,
                    width=w, height=h, duration=dur, 
                    caption=f"🎬 **Video {v_idx}/{len(videos)}**\n{album_caption}",
                    supports_streaming=True, reply_to_message_id=reply_id,
                    progress=pyrogram_progress, progress_args=(status, start_time, f"📤 Uploading Video {v_idx}/{len(videos)}")
                )
                
                if os.path.exists(filepath): os.remove(filepath)
                if os.path.exists(thumb): os.remove(thumb)
            except Exception as e:
                print(f"Video Error: {e}")

    try: os.rmdir(user_folder)
    except: pass
    mark_processed(album_id)
    await status.delete()
    return True

# --- HANDLERS ---

@app.on_message(filters.command("user", prefixes="."))
async def user_cmd(client, message):
    if len(message.command) < 2: return
    chat_id = message.chat.id
    try: await client.get_chat(chat_id)
    except: pass

    username = message.command[1].strip().split("erome.com/")[-1].split('/')[0]
    cancel_tasks[chat_id] = False
    msg = await message.reply(f"🛰 **Scanning...**")
    
    all_urls = []
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/121.0.0.0', 'Referer': 'https://www.erome.com/'}
    for tab in ["", "/reposts"]:
        page = 1
        while True:
            url = f"https://www.erome.com/{username}{tab}?page={page}"
            try:
                res = session.get(url, headers=headers, timeout=20)
                if res.status_code != 200: break
                ids = re.findall(r'/a/([a-zA-Z0-9]+)', res.text)
                if not ids: break
                for aid in ids:
                    f_url = f"https://www.erome.com/a/{aid}"
                    if f_url not in all_urls: all_urls.append(f_url)
                if "Next" not in res.text: break
                page += 1
                await msg.edit_text(f"🔍 Found {len(all_urls)} items...")
            except: break
    
    for i, url in enumerate(all_urls, 1):
        if cancel_tasks.get(chat_id): break
        await process_album(client, chat_id, message.id, url, username, i, len(all_urls))
        await asyncio.sleep(1)
    await msg.delete(); await message.reply(f"🏆 Completed `{username}`!")

@app.on_callback_query(filters.regex(r"^stop_task|"))
async def handle_stop(client, callback: CallbackQuery):
    cancel_tasks[int(callback.data.split("|")[1])] = True
    await callback.answer("🛑 Stopping...", show_alert=True)

async def main():
    init_db()
    async with app:
        print("LOG: Stable Final Peer-Fixed Version Started!")
        await idle()

if __name__ == "__main__":
    app.run(main())
