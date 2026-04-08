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

# --- 1. CONFIGURATION (Keep your Setup) ---
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
sudo_raw = os.getenv("SUDO_USERS", "")
SUDO_USERS = [int(x.strip()) for x in sudo_raw.split(",") if x.strip().isdigit()]

app = Client("tobo_pro_session", api_id=int(API_ID), api_hash=API_HASH)
DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)
session = requests.Session()

cancel_tasks = {}

# --- 2. DATABASE (Anti-Duplicate) ---
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
        # GitHub Auto-Sync (Optional)
        subprocess.run(["git", "add", DB_NAME], capture_output=True)
        subprocess.run(["git", "commit", "-m", "Sync Memory"], capture_output=True)
        subprocess.run(["git", "push"], capture_output=True)
    except: pass
    conn.close()

# --- 3. HELPERS (Your Pure Original Helpers) ---
def create_progress_bar(current, total):
    if total <= 0: return "[░░░░░░░░░░] 0%"
    pct = min(100, (current / total) * 100)
    return f"[{'█' * int(pct/10)}{'░' * (10 - int(pct/10))}] {pct:.1f}%"

def get_human_size(num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0: return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

async def progress_callback(current, total, client, status_msg, start_time, action_text):
    now = time.time()
    if now - start_time[0] > 7: 
        bar = create_progress_bar(current, total)
        try:
            await status_msg.edit_text(f"🚀 **{action_text}**\n\n{bar}\n📦 **Size:** {get_human_size(current)} / {get_human_size(total)}")
            start_time[0] = now
        except: pass

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

# --- 4. [KEEP OLD CODE] NITRO DOWNLOAD ENGINE ---
def download_nitro(url, path, headers, size, segs=4):
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
# SCRAPER ENGINE (V9.10)
# ==========================================
def scrape_album_details(url):
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/123.0.0.0', 'Referer': 'https://www.erome.com/'}
    try:
        res = session.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Untitled"
        p_l = list(dict.fromkeys([x if x.startswith('http') else 'https:' + x for x in [i.get('data-src') or i.get('src') for i in soup.select('div.img img') if "erome.com" in (i.get('data-src') or i.get('src', ''))] if x]))
        v_l = list(dict.fromkeys(re.findall(r'https?://[^\s"\'\\>]+erome\.com[^\s"\'\\>]+\.mp4', res.text)))
        v_l.sort(key=lambda x: (re.search(r'(1080|720|high)', x.lower()) is None, x))
        return title, p_l, v_l
    except: return "Error", [], []

# ==========================================
# DELIVERY ENGINE (Keep your Original Flow)
# ==========================================
async def process_album(client, message, url, username):
    if is_processed(url): return True
    title, photos, videos = scrape_album_details(url)
    if not photos and not videos: return False
    
    user_folder = os.path.join(DOWNLOAD_DIR, username)
    if not os.path.exists(user_folder): os.makedirs(user_folder)
    
    status = await message.reply_text(f"📥 Archiving: `{title}`")
    
    if photos:
        p_paths = []
        for i, p_url in enumerate(photos, 1):
            p = os.path.join(user_folder, f"img_{i}.jpg")
            r = requests.get(p_url); open(p, 'wb').write(r.content); p_paths.append(p)
            if len(p_paths) == 10 or i == len(photos):
                try: await message.reply_media_group([InputMediaPhoto(pf, caption=f"🖼 {title}") for pf in p_paths])
                except: pass
                for pf in p_paths: os.remove(pf)
                p_paths = []

    if videos:
        for i, v_url in enumerate(videos, 1):
            filepath = os.path.join(user_folder, f"temp_v{i}.mp4")
            try:
                headers = {'User-Agent': 'Mozilla/5.0', 'Referer': url}
                with requests.get(v_url, headers=headers, stream=True, timeout=15) as r:
                    size = int(r.headers.get('content-length', 0))
                
                if size > 15*1024*1024:
                    await status.edit_text(f"📥 Nitro Downloading Video {i}...")
                    download_nitro(v_url, filepath, headers, size)
                else:
                    with open(filepath, 'wb') as f:
                        r = requests.get(v_url, headers=headers, stream=True)
                        for chunk in r.iter_content(chunk_size=1024*1024): f.write(chunk)
                
                dur, w, h, audio = get_video_meta(filepath)
                if not audio: # Audio Fix
                    temp = filepath + ".fix.mp4"
                    subprocess.run(['ffmpeg', '-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100', '-i', filepath, '-c:v', 'copy', '-c:a', 'aac', '-shortest', temp, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    os.remove(filepath); os.rename(temp, filepath)
                
                thumb = filepath + ".jpg"
                subprocess.run(['ffmpeg', '-ss', '00:00:01', '-i', filepath, '-vframes', '1', '-vf', 'scale=320:-1', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                start_time = [time.time()]
                await message.reply_video(filepath, thumb=thumb, duration=dur, width=w, height=h, caption=f"🎬 {title}", supports_streaming=True, progress=progress_callback, progress_args=(client, status, start_time, f"Uploading Video {i}"))
                os.remove(filepath); os.remove(thumb)
            except: pass
    
    mark_processed(url)
    await status.delete()
    return True

# ==========================================
# COMMAND HANDLERS (The "No-Stuck" Lazy Scan)
# ==========================================
@app.on_message(filters.command("user", prefixes=".") & filters.user(SUDO_USERS))
async def user_cmd(client, message):
    raw_input = message.command[1].strip()
    username = raw_input.split("erome.com/")[-1].split('/')[0]
    chat_id = message.chat.id
    cancel_tasks[chat_id] = False 
    
    msg = await message.reply(f"🛰 **Tank Mode Activated: {username}**\nScanning & Processing simultaneously...")
    
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/122.0.0.0'}
    for tab in ["", "/reposts"]:
        page = 1
        while True:
            url = f"https://www.erome.com/{username}{tab}?page={page}"
            try:
                res = session.get(url, headers=headers, timeout=20)
                if res.status_code != 200: break
                album_ids = list(dict.fromkeys(re.findall(r'/a/([a-zA-Z0-9]+)', res.text)))
                
                if not album_ids: break
                
                for aid in album_ids:
                    if cancel_tasks.get(chat_id): break
                    target_url = f"https://www.erome.com/a/{aid}"
                    # [LAZY SCAN FIX] Download immediately after finding
                    await process_album(client, message, target_url, username)
                
                if "Next" not in res.text or cancel_tasks.get(chat_id): break
                page += 1
                await asyncio.sleep(1) # Prevent Erome block
            except: break

    await msg.edit_text(f"🏆 Done for `{username}`!")

@app.on_callback_query(filters.regex(r"^stop_task\|"))
async def handle_stop(client, callback: CallbackQuery):
    cancel_tasks[int(callback.data.split("|")[1])] = True
    await callback.answer("Stopping...")

@app.on_message(filters.command("dl", prefixes=".") & filters.user(SUDO_USERS))
async def dl_handler(client, message):
    urls = list(dict.fromkeys([u.strip() for u in message.text.split('\n') if "erome.com/a/" in u]))
    for url in urls: await process_album(client, message, url, "single")
    try: await message.delete()
    except: pass

async def main():
    init_db()
    async with app:
        print("LOG: V9.10 Final Stable Master (Original Nitro) Online!")
        await idle()

if __name__ == "__main__":
    app.run(main())
