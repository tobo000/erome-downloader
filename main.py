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

# --- CONFIGURATION (Original) ---
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

app = Client("tobo_pro_session", api_id=int(API_ID), api_hash=API_HASH)
DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)
session = requests.Session()

cancel_tasks = {}

# --- 1. DATABASE & AUTO-SYNC (Your New Update) ---
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

def git_sync():
    """Backup to GitHub"""
    try:
        subprocess.run(["git", "add", DB_NAME], check=True)
        subprocess.run(["git", "commit", "-m", "Auto-update memory database"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("LOG: GitHub Sync Complete.")
    except: pass

def mark_processed(url):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO processed (url) VALUES (?)", (url,))
        conn.commit()
        git_sync()
    except: pass
    conn.close()

# --- 2. HELPERS (Original) ---
def create_progress_bar(current, total):
    if total <= 0: return "[░░░░░░░░░░] 0%"
    pct = min(100, (current / total) * 100)
    return f"[{'█' * int(pct/10)}{'░' * (10 - int(pct/10))}] {pct:.1f}%"

def get_human_size(num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0: return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

async def edit_progress_msg(current, total, status_msg, last_update, action_text):
    now = time.time()
    if now - last_update[0] > 5: 
        bar = create_progress_bar(current, total)
        try:
            await status_msg.edit_text(f"🚀 **{action_text}**\n\n{bar}\n📦 **Size:** {get_human_size(current)} / {get_human_size(total)}")
            last_update[0] = now
        except: pass

def get_video_meta(video_path):
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8')
        data = json.loads(res)
        duration = int(float(data.get('format', {}).get('duration', 0)))
        v = next((s for s in data['streams'] if s['codec_type'] == 'video'), {})
        return duration, int(v.get('width', 1280)), int(v.get('height', 720)), any(s['codec_type'] == 'audio' for s in data['streams'])
    except: return 0, 1280, 720, False

# --- 3. DOWNLOAD ENGINE (Nitro Original Logic) ---
async def download_with_progress(url, path, headers, size, status_msg, action_text):
    last_update = [time.time()]
    downloaded = 0
    try:
        with requests.get(url, headers=headers, stream=True, timeout=60) as r:
            with open(path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        await edit_progress_msg(downloaded, size, status_msg, last_update, action_text)
        return True
    except: return False

def download_nitro(url, path, headers, size, segs=4):
    chunk = size // segs
    def dl_part(s, e, n):
        pp = f"{path}.p{n}"; h = headers.copy(); h['Range'] = f'bytes={s}-{e}'
        with session.get(url, headers=h, stream=True) as r:
            with open(pp, 'wb') as f:
                for chk in r.iter_content(chunk_size=1024*1024): f.write(chk)
    with ThreadPoolExecutor(max_workers=segs) as ex:
        for i in range(segs): ex.submit(dl_part, i*chunk, (i+1)*chunk-1 if i < size-1 else size-1, i)
    with open(path, 'wb') as f:
        for i in range(segs):
            pp = f"{path}.p{i}"
            if os.path.exists(pp):
                with open(pp, 'rb') as pf: f.write(pf.read()); os.remove(pp)

# ==========================================
# SCRAPER ENGINE (Original Logic)
# ==========================================
def scrape_album_details(url):
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/123.0.0.0', 'Referer': 'https://www.erome.com/'}
    try:
        res = session.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Untitled"
        p_l = list(dict.fromkeys([x if x.startswith('http') else 'https:' + x for x in [i.get('data-src') or i.get('src') for i in soup.select('div.img img') if "erome.com" in (i.get('data-src') or i.get('src', ''))] if x]))
        v_candidates = []
        for tag in soup.find_all(['video', 'source', 'a']):
            src = tag.get('src') or tag.get('data-src') or tag.get('href')
            if src and ".mp4" in src.lower(): v_candidates.append(src if src.startswith('http') else 'https:' + src)
        v_candidates.extend(re.findall(r'https?://[^\s"\'>]+\.mp4', res.text))
        v_l = list(dict.fromkeys([v for v in v_candidates if "erome.com" in v]))
        v_l.sort(key=lambda x: (re.search(r'(1080|720|high)', x.lower()) is None, x))
        return title, p_l, v_l
    except: return "Error", [], []

async def scan_all_content(username, status_msg):
    all_links = []
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/121.0.0.0'}
    for tab in ["", "/reposts"]:
        page = 1
        while True:
            await status_msg.edit_text(f"🔍 **Scanning `{username}`...**\n🚀 Found: `{len(all_links)}` items")
            url = f"https://www.erome.com/{username}{tab}?page={page}"
            try:
                res = session.get(url, headers=headers, timeout=20)
                if res.status_code != 200: break
                soup = BeautifulSoup(res.text, 'html.parser')
                links = [a['href'] for a in soup.find_all("a", href=True) if "/a/" in a['href'] and "erome.com" not in a['href']]
                if not links: break
                for l in links:
                    full = 'https://www.erome.com' + l
                    if full not in all_links: all_links.append(full)
                if not soup.find("a", string=re.compile("Next", re.I)): break
                page += 1
                await asyncio.sleep(0.5)
            except: break
    return all_links

# ==========================================
# DELIVERY ENGINE (Original Flow)
# ==========================================
async def process_album(client, message, url, username, current, total):
    album_id = url.rstrip('/').split('/')[-1]
    if is_processed(url): return True

    title, photos, videos = scrape_album_details(url)
    if not photos and not videos: return False
    
    user_folder = os.path.join(DOWNLOAD_DIR, username)
    if not os.path.exists(user_folder): os.makedirs(user_folder)
    
    # FIXED: Use reply for automatic Topic support
    status = await message.reply_text(f"📥 **[{current}/{total}]** Preparing: `{title}`")

    if photos:
        p_paths = []
        for i, p_url in enumerate(photos, 1):
            p = os.path.join(user_folder, f"img_{album_id}_{i}.jpg")
            open(p, 'wb').write(requests.get(p_url).content)
            p_paths.append(p)
            if len(p_paths) == 10 or i == len(photos):
                await message.reply_media_group([InputMediaPhoto(pf, caption=f"🖼 {title}") for pf in p_paths])
                for pf in p_paths: os.remove(pf)
                p_paths = []

    if videos:
        for i, v_url in enumerate(videos, 1):
            filepath = os.path.join(user_folder, f"{album_id}_v{i}.mp4")
            headers = {'User-Agent': 'Mozilla/5.0 Chrome/121.0.0.0', 'Referer': url}
            try:
                with requests.get(v_url, headers=headers, stream=True, timeout=15) as r:
                    size = int(r.headers.get('content-length', 0))
                if size > 15*1024*1024:
                    await status.edit_text(f"📥 **[{current}/{total}]** Nitro Downloading Video...")
                    download_nitro(v_url, filepath, headers, size)
                else:
                    await download_with_progress(v_url, filepath, headers, size, status, f"Downloading Video {i}")
                
                dur, w, h, audio = get_video_meta(filepath)
                if not audio:
                    temp = filepath + ".fix.mp4"
                    subprocess.run(['ffmpeg', '-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100', '-i', filepath, '-c:v', 'copy', '-c:a', 'aac', '-shortest', temp, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    os.remove(filepath); os.rename(temp, filepath)
                
                thumb = filepath + ".jpg"
                subprocess.run(['ffmpeg', '-ss', '00:00:01', '-i', filepath, '-vframes', '1', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                start_time = [time.time()]
                await message.reply_video(filepath, thumb=thumb, duration=dur, width=w, height=h, caption=f"🎬 {title}", supports_streaming=True, progress=progress_callback, progress_args=(client, status, start_time, "Uploading Video"))
                os.remove(filepath); os.remove(thumb)
            except: pass
    
    mark_processed(url) 
    await status.delete()
    return True

# ==========================================
# COMMAND HANDLERS
# ==========================================
@app.on_message(filters.command("user", prefixes="."))
async def user_cmd(client, message):
    if len(message.command) < 2: return
    username = message.command[1].strip()
    chat_id = message.chat.id
    cancel_tasks[chat_id] = False 
    
    msg = await message.reply(f"🛰 **Scanning profile: {username}...**")
    all_urls = await scan_all_content(username, msg)
    if not all_urls: return await msg.edit_text("❌ No items found.")
    
    total = len(all_urls)
    await msg.edit_text(f"✅ Found: `{total}`. Starting archive...", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 STOP", callback_data=f"stop_task|{chat_id}")]]))
    
    for i, url in enumerate(all_urls, 1):
        if cancel_tasks.get(chat_id): break
        await process_album(client, message, url, username, i, total)
    
    await msg.delete(); await message.reply(f"🏆 Done for `{username}`!")

@app.on_callback_query(filters.regex(r"^stop_task\|"))
async def handle_stop(client, callback: CallbackQuery):
    cancel_tasks[int(callback.data.split("|")[1])] = True
    await callback.answer("Stopping Task...")

@app.on_message(filters.command("dl", prefixes="."))
async def dl_handler(client, message):
    urls = list(dict.fromkeys([u.strip() for u in message.text.split('\n') if "erome.com/a/" in u]))
    for i, url in enumerate(urls, 1): 
        await process_album(client, message, url, "single", i, len(urls))
    try: await message.delete()
    except: pass

async def main():
    init_db()
    async with app:
        print("LOG: V8.91 (Original Power + Fixes) Online!")
        await idle()

if __name__ == "__main__":
    app.run(main())
