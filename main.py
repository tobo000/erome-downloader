import os
import asyncio
import requests
import time
import subprocess
import json
import re
import sqlite3
import gc
from bs4 import BeautifulSoup
from pyrogram import Client, filters, idle, errors
from pyrogram.types import InputMediaPhoto, InputMediaVideo, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import MessageNotModified, FloodWait
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
sudo_raw = os.getenv("SUDO_USERS", "")
SUDO_USERS = [int(x.strip()) for x in sudo_raw.split(",") if x.strip().isdigit()]

app = Client("tobo_pro_session", api_id=int(API_ID), api_hash=API_HASH, sleep_threshold=180)
DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)
session = requests.Session()

cancel_tasks = {}
# [KEEP] Your original Nitro-style ThreadPool
executor = ThreadPoolExecutor(max_workers=2)

# --- 2. DATABASE ENGINE (Fixed Queue System) ---
DB_NAME = "bot_archive.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    # status: 0=pending, 1=done
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS queue (
            url TEXT PRIMARY KEY, 
            username TEXT, 
            chat_id INTEGER, 
            topic_id INTEGER, 
            status INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()

def add_to_queue(url, username, chat_id, topic_id):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO queue (url, username, chat_id, topic_id, status) VALUES (?, ?, ?, ?, 0)", 
                       (url, username, chat_id, topic_id))
        conn.commit()
    except: pass
    conn.close()

def get_next_task():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT url, username, chat_id, topic_id FROM queue WHERE status = 0 LIMIT 1")
    res = cursor.fetchone()
    conn.close()
    return res

def mark_done(url):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("UPDATE queue SET status = 1 WHERE url = ?", (url,))
    conn.commit()
    # Auto-Sync to GitHub after each task
    try:
        subprocess.run(["git", "add", DB_NAME], capture_output=True)
        subprocess.run(["git", "commit", "-m", "Sync DB"], capture_output=True)
        subprocess.run(["git", "push"], capture_output=True)
    except: pass
    conn.close()

# --- 3. HELPERS (Keep Your Original Code) ---
def create_progress_bar(current, total):
    if total <= 0: return "[░░░░░░░░░░] 0%"
    pct = min(100, (current / total) * 100)
    return f"[{'█' * int(pct/10)}{'░' * (10 - int(pct/10))}] {pct:.1f}%"

def get_human_size(num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0: return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

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

def download_nitro(url, path, headers, size, segs=4):
    """[KEEP] Your original Nitro logic"""
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
# WORKER PROCESSOR (Background Archive)
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

async def background_worker():
    """ម៉ាស៊ីនទាញយកស្ងាត់ៗក្នុង Background ដើម្បីកុំឱ្យ Bot គាំង"""
    while True:
        task = get_next_task()
        if not task:
            await asyncio.sleep(10)
            continue
        
        url, username, chat_id, topic_id = task
        try:
            title, photos, videos = scrape_album_details(url)
            user_folder = os.path.join(DOWNLOAD_DIR, username)
            os.makedirs(user_folder, exist_ok=True)

            if photos:
                p_paths = []
                for i, p_url in enumerate(photos, 1):
                    p = os.path.join(user_folder, f"img_{i}.jpg")
                    open(p, 'wb').write(session.get(p_url).content); p_paths.append(p)
                    if len(p_paths) == 10 or i == len(photos):
                        try: await app.send_media_group(chat_id, [InputMediaPhoto(pf, caption=f"🖼 {title}") for pf in p_paths], message_thread_id=topic_id)
                        except: pass
                        [os.remove(pf) for pf in p_paths]; p_paths = []

            if videos:
                for i, v_url in enumerate(videos, 1):
                    filepath = os.path.join(DOWNLOAD_DIR, "worker_temp.mp4")
                    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': url}
                    with session.get(v_url, headers=headers, stream=True) as r:
                        size = int(r.headers.get('content-length', 0))
                        if size > 15*1024*1024: download_nitro(v_url, filepath, headers, size)
                        else:
                            with open(filepath, 'wb') as f:
                                for chunk in r.iter_content(chunk_size=1024*1024): f.write(chunk)
                    
                    dur, w, h, audio = get_video_meta(filepath)
                    if not audio:
                        temp = filepath + ".fix.mp4"
                        subprocess.run(['ffmpeg', '-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100', '-i', filepath, '-c:v', 'copy', '-c:a', 'aac', '-shortest', temp, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        os.remove(filepath); os.rename(temp, filepath)
                    
                    thumb = filepath + ".jpg"
                    subprocess.run(['ffmpeg', '-ss', '00:00:01', '-i', filepath, '-vframes', '1', '-vf', 'scale=320:-1', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    await app.send_video(chat_id, filepath, thumb=thumb, duration=dur, width=w, height=h, caption=f"🎬 {title}", supports_streaming=True, message_thread_id=topic_id)
                    if os.path.exists(filepath): os.remove(filepath)
                    if os.path.exists(thumb): os.remove(thumb)

            mark_done(url)
            print(f"WORKER: Finished {url}")
            await asyncio.sleep(2)
        except Exception as e:
            print(f"WORKER ERROR: {e}")
            await asyncio.sleep(5)

# ==========================================
# COMMAND HANDLER (Fast Harvesting)
# ==========================================
@app.on_message(filters.command("user", prefixes=".") & filters.user(SUDO_USERS))
async def user_cmd(client, message):
    username = message.command[1].strip().split("erome.com/")[-1].split('/')[0]
    topic_id = message.message_thread_id
    status = await message.reply(f"🕵️‍♂️ **Harvesting all items for {username}...**")

    headers = {'User-Agent': 'Mozilla/5.0 Chrome/122.0.0.0'}
    total = 0
    for tab in ["", "/reposts"]:
        page = 1
        while True:
            url = f"https://www.erome.com/{username}{tab}?page={page}"
            try:
                res = session.get(url, headers=headers, timeout=20)
                ids = re.findall(r'/a/([a-zA-Z0-9]+)', res.text)
                if not ids: break
                for aid in list(dict.fromkeys(ids)):
                    add_to_queue(f"https://www.erome.com/a/{aid}", username, message.chat.id, topic_id)
                    total += 1
                if "Next" not in res.text: break
                page += 1
                if total % 50 == 0: await status.edit_text(f"🔍 **Found:** `{total}` items...")
            except: break

    await status.edit_text(f"✅ **Done!** `{total}` items added to worker.\nArchive is running in background.")

@app.on_message(filters.command("dl", prefixes=".") & filters.user(SUDO_USERS))
async def dl_handler(client, message):
    urls = list(dict.fromkeys([u.strip() for u in message.text.split('\n') if "erome.com/a/" in u]))
    for url in urls:
        add_to_queue(url, "single", message.chat.id, message.message_thread_id)
    await message.reply("✅ Added to queue!")

async def main():
    init_db()
    asyncio.create_task(background_worker())
    async with app:
        print("LOG: V9.51 Background Worker is Online!")
        await idle()

if __name__ == "__main__":
    app.run(main())
