import os
import asyncio
import requests
import time
import subprocess
import json
import re
import sqlite3
import random
import gc
from bs4 import BeautifulSoup
from pyrogram import Client, filters, idle, errors
from pyrogram.types import InputMediaPhoto, InputMediaVideo, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import MessageNotModified, FloodWait, PeerIdInvalid
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
sudo_raw = os.getenv("SUDO_USERS", "")
SUDO_USERS = [int(x.strip()) for x in sudo_raw.split(",") if x.strip().isdigit()]

app = Client("tobo_pro_session", api_id=int(API_ID), api_hash=API_HASH, sleep_threshold=120)
DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)

# Global Session for high performance
session = requests.Session()
cancel_tasks = {}
# ThreadPool for Nitro Downloads
executor = ThreadPoolExecutor(max_workers=4)

# --- 2. DATABASE & AUTO-SYNC ---
DB_NAME = "bot_archive.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS processed (url TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

def is_processed(url):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed WHERE url = ?", (url,))
    res = cursor.fetchone()
    conn.close()
    return res is not None

def mark_processed(url):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO processed (url) VALUES (?)", (url,))
        conn.commit()
        # GitHub Auto-Sync logic
        subprocess.run(["git", "add", DB_NAME], capture_output=True)
        subprocess.run(["git", "commit", "-m", "Sync Memory"], capture_output=True)
        subprocess.run(["git", "push"], capture_output=True)
    except: pass
    conn.close()

# --- 3. HELPERS (Your Original Logic) ---
def create_progress_bar(current, total):
    if total <= 0: return "[░░░░░░░░░░] 0%"
    pct = min(100, (current / total) * 100)
    return f"[{'█' * int(pct/10)}{'░' * (10 - int(pct/10))}] {pct:.1f}%"

def get_human_size(num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0: return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

async def edit_status_safe(message, text):
    try: await message.edit_text(text)
    except (MessageNotModified, FloodWait): pass
    except: pass

async def progress_callback(current, total, client, status_msg, start_time, action_text):
    """Animation bar for UPLOADING"""
    now = time.time()
    if now - start_time[0] > 8: 
        bar = create_progress_bar(current, total)
        await edit_status_safe(status_msg, f"🚀 **{action_text}**\n\n{bar}\n📦 **Size:** {get_human_size(current)} / {get_human_size(total)}")
        start_time[0] = now

def get_video_meta(video_path):
    if not os.path.exists(video_path) or os.path.getsize(video_path) == 0:
        return 0, 1280, 720, False
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8')
        data = json.loads(res)
        duration = int(float(data.get('format', {}).get('duration', 0)))
        v = next((s for s in data['streams'] if s['codec_type'] == 'video'), {})
        return duration, int(v.get('width', 1280)), int(v.get('height', 720)), any(s['codec_type'] == 'audio' for s in data['streams'])
    except: return 0, 1280, 720, False

# --- 4. NITRO DOWNLOAD ENGINE (Your specific original request) ---
async def download_with_progress(url, path, headers, size, status_msg, action_text):
    """Animation bar for DOWNLOADING"""
    last_update = [time.time()]
    downloaded = 0
    try:
        with session.get(url, headers=headers, stream=True, timeout=60) as r:
            with open(path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk); downloaded += len(chunk)
                        now = time.time()
                        if now - last_update[0] > 8:
                            bar = create_progress_bar(downloaded, size)
                            await edit_status_safe(status_msg, f"📥 **{action_text}**\n\n{bar}\n📦 **Progress:** {get_human_size(downloaded)}")
                            last_update[0] = now
        return True
    except: return False

def download_nitro(url, path, headers, size, segs=4):
    """Your Original High Speed Nitro logic"""
    chunk = size // segs
    def dl_part(s, e, n):
        pp = f"{path}.p{n}"; h = headers.copy(); h['Range'] = f'bytes={s}-{e}'
        with session.get(url, headers=h, stream=True) as r:
            with open(pp, 'wb') as f:
                for chk in r.iter_content(chunk_size=1024*1024): f.write(chk)
    with ThreadPoolExecutor(max_workers=segs) as ex:
        for i in range(segs): ex.submit(dl_part, i*chunk, (i+1)*chunk-1 if i < size - 1 else size - 1, i)
    with open(path, 'wb') as f:
        for i in range(segs):
            pp = f"{path}.p{i}"
            if os.path.exists(pp):
                with open(pp, 'rb') as pf: f.write(pf.read()); os.remove(pp)

# ==========================================
# SCRAPER ENGINE (V9.09: Deep Link Discovery)
# ==========================================
def scrape_album_details(url):
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/123.0.0.0', 'Referer': 'https://www.erome.com/'}
    try:
        res = session.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Untitled"
        p_l = list(dict.fromkeys([x if x.startswith('http') else 'https:' + x for x in [i.get('data-src') or i.get('src') for i in soup.select('div.img img') if "erome.com" in (i.get('data-src') or i.get('src', ''))] if x]))
        v_l = []
        raw_vids = re.findall(r'https?://[^\s"\'\\>]+erome\.com[^\s"\'\\>]+\.mp4', res.text)
        v_l.extend(raw_vids); v_l = list(dict.fromkeys(v_l))
        v_l.sort(key=lambda x: (re.search(r'(1080|720|high)', x.lower()) is None, x))
        return title, p_l, v_l
    except: return "Error", [], []

def get_all_profile_content_sync(username, status_msg):
    """Refined Scanner for huge profiles (1000+)"""
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/122.0.0.0', 'Referer': 'https://www.erome.com/'}
    all_links = []
    for tab in ["", "/reposts"]:
        page = 1
        while True:
            url = f"https://www.erome.com/{username}{tab}?page={page}"
            try:
                res = session.get(url, headers=headers, timeout=20)
                if res.status_code != 200: break
                album_ids = list(dict.fromkeys(re.findall(r'href=["\'](/a/[a-zA-Z0-9]+)["\']', res.text)))
                if not album_ids: break
                found_new = 0
                for aid in album_ids:
                    full = 'https://www.erome.com' + aid if not aid.startswith('http') else aid
                    if full not in all_links: all_links.append(full); found_new += 1
                asyncio.run_coroutine_threadsafe(edit_status_safe(status_msg, f"🔍 **Scanning `{username}`...**\n🚀 Found: `{len(all_links)}` items\n📄 Page: {page}"), asyncio.get_event_loop())
                if found_new == 0 or "Next" not in res.text: break
                page += 1
                time.sleep(0.6)
            except: break
    return all_links

# ==========================================
# DELIVERY ENGINE
# ==========================================
async def process_album(client, message, url, username, current, total):
    album_id = url.rstrip('/').split('/')[-1]
    if is_processed(url): return True
    title, photos, videos = scrape_album_details(url)
    if not photos and not videos: return False
    
    user_folder = os.path.join(DOWNLOAD_DIR, username)
    if not os.path.exists(user_folder): os.makedirs(user_folder)
    
    # Topic Support
    topic_id = getattr(message, "message_thread_id", None)
    status = await client.send_message(message.chat.id, f"📥 **[{current}/{total}]** Preparing: `{title}`", message_thread_id=topic_id)

    # 📸 PHOTOS (Original MediaGroup Logic)
    if photos:
        p_paths = []
        for i, p_url in enumerate(photos, 1):
            p = os.path.join(user_folder, f"img_{album_id}_{i}.jpg")
            r = session.get(p_url); open(p, 'wb').write(r.content); p_paths.append(p)
            if len(p_paths) == 10 or i == len(photos):
                try: await client.send_media_group(message.chat.id, [InputMediaPhoto(pf, caption=f"🖼 {title}") for pf in p_paths], message_thread_id=topic_id)
                except: pass
                for pf in p_paths: os.remove(pf)
                p_paths = []

    # 🎬 VIDEOS (Nitro + Meta + Animation logic)
    if videos:
        for i, v_url in enumerate(videos, 1):
            filepath = os.path.join(user_folder, f"{album_id}_v{i}.mp4")
            headers = {'User-Agent': 'Mozilla/5.0', 'Referer': url}
            try:
                with session.get(v_url, headers=headers, stream=True, timeout=15) as r:
                    size = int(r.headers.get('content-length', 0))
                
                if size > 15*1024*1024:
                    await edit_status_safe(status, f"📥 Nitro Downloading Video {i}...")
                    await asyncio.get_event_loop().run_in_executor(executor, download_nitro, v_url, filepath, headers, size)
                else:
                    await download_with_progress(v_url, filepath, headers, size, status, f"Downloading Video {i}")
                
                if not os.path.exists(filepath): continue
                dur, w, h, audio = get_video_meta(filepath)
                if not audio: # Silent Audio Fix
                    temp = filepath + ".fix.mp4"
                    subprocess.run(['ffmpeg', '-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100', '-i', filepath, '-c:v', 'copy', '-c:a', 'aac', '-shortest', temp, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    os.remove(filepath); os.rename(temp, filepath)
                
                thumb = filepath + ".jpg"
                subprocess.run(['ffmpeg', '-ss', '00:00:01', '-i', filepath, '-vframes', '1', '-vf', 'scale=320:-1', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                start_time = [time.time()]
                await client.send_video(message.chat.id, filepath, thumb=thumb, duration=dur, width=w, height=h, caption=f"🎬 {title}", supports_streaming=True, message_thread_id=topic_id, progress=progress_callback, progress_args=(client, status, start_time, "Uploading Video"))
                os.remove(filepath); os.remove(thumb)
            except: pass
    
    mark_processed(url) 
    await status.delete()
    gc.collect() # RAM Management
    return True

# ==========================================
# COMMAND HANDLERS
# ==========================================
@app.on_message(filters.command("user", prefixes=".") & filters.user(SUDO_USERS))
async def user_cmd(client, message):
    raw_input = message.command[1].strip()
    username = raw_input.split("erome.com/")[-1].split('/')[0]
    chat_id = message.chat.id
    cancel_tasks[chat_id] = False 
    
    msg = await message.reply(f"🛰 **Scanning profile: {username}...**")
    loop = asyncio.get_event_loop()
    all_urls = await loop.run_in_executor(None, get_all_profile_content_sync, username, msg)
    
    if not all_urls: return await msg.edit_text(f"❌ No items found for `{username}`.")
    
    total = len(all_urls)
    await msg.edit_text(f"✅ Found: `{total}`. Starting archive...", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 STOP", callback_data=f"stop_task|{chat_id}")]]))
    
    for i, url in enumerate(all_urls, 1):
        if cancel_tasks.get(chat_id): break
        await process_album(client, message, url, username, i, total)
        await asyncio.sleep(2) # Stabilize 1GB RAM VPS
    
    await msg.delete(); await client.send_message(chat_id, f"🏆 Done for `{username}`!", message_thread_id=message.message_thread_id)

@app.on_callback_query(filters.regex(r"^stop_task\|"))
async def handle_stop(client, callback: CallbackQuery):
    cancel_tasks[int(callback.data.split("|")[1])] = True
    await callback.answer("Stopping.")

@app.on_message(filters.command("dl", prefixes=".") & filters.user(SUDO_USERS))
async def dl_handler(client, message):
    urls = list(dict.fromkeys([u.strip() for u in message.text.split('\n') if "erome.com/a/" in u]))
    for i, url in enumerate(urls, 1): 
        await process_album(client, message, url, "single", i, len(urls))
    try: await message.delete()
    except: pass

async def main():
    init_db()
    async with app:
        print("LOG: V9.09 Everything Restored & Online!")
        await idle()

if __name__ == "__main__":
    app.run(main())
