import os
import asyncio
import requests
import time
import subprocess
import json
import re
import sqlite3
import base64
import glob
from bs4 import BeautifulSoup
from pyrogram import Client, filters, idle
from pyrogram.types import InputMediaPhoto, InputMediaVideo
from pyrogram.errors import FloodWait, MessageNotModified
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
ADMIN_IDS = [5549600755, 7010218617]

GH_TOKEN = os.getenv("GH_TOKEN")
GH_REPO = os.getenv("GH_REPO") 
GH_FILE_PATH = "bot_archive.db"

app = Client("tobo_pro_session", api_id=int(API_ID), api_hash=API_HASH, sleep_threshold=2000)
DOWNLOAD_DIR = "downloads"
DB_NAME = "bot_archive.db"

if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)
session = requests.Session()
executor = ThreadPoolExecutor(max_workers=8) 
cancel_tasks = {}

# --- 2. GITHUB SYNC ENGINE ---

def backup_to_github():
    if not GH_TOKEN or not GH_REPO: return
    try:
        url = f"https://api.github.com/repos/{GH_REPO}/contents/{GH_FILE_PATH}"
        headers = {"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        res = requests.get(url, headers=headers)
        sha = res.json().get('sha') if res.status_code == 200 else None
        with open(DB_NAME, "rb") as f: content = base64.b64encode(f.read()).decode()
        data = {"message": f"Sync DB: {time.ctime()}", "content": content, "branch": "main"}
        if sha: data["sha"] = sha
        requests.put(url, headers=headers, data=json.dumps(data))
    except: pass

def download_from_github():
    if not GH_TOKEN or not GH_REPO: return
    try:
        url = f"https://api.github.com/repos/{GH_REPO}/contents/{GH_FILE_PATH}"
        headers = {"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            content = base64.b64decode(res.json()['content'])
            with open(DB_NAME, "wb") as f: f.write(content)
    except: pass

# --- 3. DATABASE LOGIC ---

def init_db():
    download_from_github()
    conn = sqlite3.connect(DB_NAME)
    conn.execute("CREATE TABLE IF NOT EXISTS processed (album_id TEXT PRIMARY KEY)")
    conn.execute("CREATE TABLE IF NOT EXISTS processed_media (media_id TEXT PRIMARY KEY, album_id TEXT)")
    conn.commit(); conn.close()

def is_processed(album_id):
    conn = sqlite3.connect(DB_NAME)
    res = conn.execute("SELECT 1 FROM processed WHERE album_id = ?", (album_id,)).fetchone()
    conn.close(); return res is not None

def is_media_processed(media_url):
    media_id = re.sub(r'\W+', '', media_url)
    conn = sqlite3.connect(DB_NAME)
    res = conn.execute("SELECT 1 FROM processed_media WHERE media_id = ?", (media_id,)).fetchone()
    conn.close(); return res is not None

def mark_media_processed(media_url, album_id):
    media_id = re.sub(r'\W+', '', media_url)
    conn = sqlite3.connect(DB_NAME)
    conn.execute("INSERT OR IGNORE INTO processed_media (media_id, album_id) VALUES (?, ?)", (media_id, album_id))
    conn.commit(); conn.close()

def mark_processed(album_id):
    conn = sqlite3.connect(DB_NAME)
    conn.execute("INSERT OR IGNORE INTO processed (album_id) VALUES (?)", (album_id,))
    conn.commit(); conn.close(); backup_to_github()

# --- 4. HELPERS & ANIMATIONS ---

def create_progress_bar(current, total):
    pct = min(100, (current / total) * 100) if total > 0 else 0
    return f"[{'█' * int(pct/10)}{'░' * (10 - int(pct/10))}] {pct:.1f}%"

def get_human_size(num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0: return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

async def safe_edit(msg, text):
    try: await msg.edit_text(text)
    except (MessageNotModified, FloodWait): pass
    except: pass

async def pyrogram_progress(current, total, status_msg, start_time, action_text, topic=""):
    now = time.time()
    if now - start_time[0] > 8: 
        anims = ["🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘"]
        anim = anims[int(now % 8)]
        bar = create_progress_bar(current, total)
        await safe_edit(status_msg, f"{anim} **{action_text}**\n\n{bar}\n📦 **Progress:** {get_human_size(current)} / {get_human_size(total)}")
        start_time[0] = now

def get_video_meta(video_path):
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8'); data = json.loads(res)
        v = next((s for s in data['streams'] if s['codec_type'] == 'video'), {})
        dur = int(float(data.get('format', {}).get('duration', 0)))
        return dur, int(v.get('width', 1280)), int(v.get('height', 720))
    except: return 0, 1280, 720

def download_nitro_animated(url, path, size, status_msg, loop, action, topic, segs=4):
    chunk = size // segs; downloaded_shared = [0]; start_time = [time.time()]
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
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(dl_part, i*chunk, ((i+1)*chunk-1 if i < segs - 1 else size-1), i) for i in range(segs)]
        for fut in futures: fut.result()
    with open(path, 'wb') as f:
        for i in range(segs):
            pp = f"{path}.p{i}"
            if os.path.exists(pp):
                with open(pp, 'rb') as pf: f.write(pf.read()); pf.close(); os.remove(pp)

# --- 5. SCRAPER ENGINE ---

def scrape_album_details(url):
    h = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        res = session.get(url, headers=h, timeout=20); soup = BeautifulSoup(res.text, 'html.parser')
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Untitled"
        p_l = list(dict.fromkeys([img.get('data-src') or img.get('src') for img in soup.select('div.img img')]))
        p_l = ['https:' + x if x.startswith('//') else x for x in p_l if x]
        v_l = list(dict.fromkeys(re.findall(r'https?://[^\s"\'>]+.(?:mp4|gif)', res.text)))
        v_l = [v for v in v_l if "erome.com" in v]
        return title, p_l, v_l
    except: return "Error", [], []

# --- 6. CORE DELIVERY ---

async def process_album(client, chat_id, reply_id, url, username, current, total):
    album_id = url.rstrip('/').split('/')[-1]
    if is_processed(album_id): return True
    
    loop = asyncio.get_running_loop()
    title, photos, videos = await loop.run_in_executor(executor, scrape_album_details, url)
    if not photos and not videos: return False
    
    user_folder = os.path.join(DOWNLOAD_DIR, f"{chat_id}_{album_id}"); os.makedirs(user_folder, exist_ok=True)
    status = await client.send_message(chat_id, f"📡 **[{current}/{total}] Analyzing: {title}**", reply_to_message_id=reply_id)

    master_cap = f"🎬 Topic: {title}\n📂 Album: {current}/{total}\n👤 User: {username.upper()}\n\n"

    # --- PHOTOS ---
    pending_p = [p for p in photos if not is_media_processed(p)]
    for i in range(0, len(pending_p), 10):
        if cancel_tasks.get(chat_id): break
        chunk = pending_p[i:i+10]; group = []
        for p_url in chunk:
            p_idx = photos.index(p_url) + 1
            p_path = os.path.join(user_folder, f"p_{p_idx}.jpg")
            r = session.get(p_url); open(p_path, 'wb').write(r.content)
            cap = (master_cap if i == 0 and chunk.index(p_url) == 0 else "") + f"🖼 Photo: {p_idx}/{len(photos)} | 📦 {get_human_size(os.path.getsize(p_path))}"
            group.append(InputMediaPhoto(p_path, caption=cap))
        if group:
            try: await client.send_media_group(chat_id, group, reply_to_message_id=reply_id); await asyncio.sleep(2)
            except: pass
            for p_url in chunk: mark_media_processed(p_url, album_id)
        for f in glob.glob(os.path.join(user_folder, "p_*.jpg")): os.remove(f)

    # --- VIDEOS (Strict Playable Fix) ---
    for v_idx, v_url in enumerate(videos, 1):
        if cancel_tasks.get(chat_id) or is_media_processed(v_url): continue
        filepath = os.path.join(user_folder, f"v_{v_idx}.mp4")
        try:
            r_h = session.head(v_url); size = int(r_h.headers.get('content-length', 0))
            await loop.run_in_executor(executor, download_nitro_animated, v_url, filepath, size, status, loop, f"📥 Downloading Video {v_idx}", title)
            
            # Metadata Extraction
            dur, w, h = get_video_meta(filepath)
            thumb = filepath + ".jpg"
            subprocess.run(['ffmpeg', '-ss', '1', '-i', filepath, '-vframes', '1', '-vf', 'scale=320:-1', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            v_cap = (master_cap if v_idx == 1 and not pending_p else "") + f"🎬 Video: {v_idx}/{len(videos)} | 📦 {get_human_size(os.path.getsize(filepath))}"

            # FORCE PLAYABLE VIDEO METADATA
            await client.send_video(
                chat_id=chat_id, video=filepath, 
                thumb=thumb if os.path.exists(thumb) else None,
                width=int(w), height=int(h), duration=int(dur), 
                supports_streaming=True, caption=v_cap,
                reply_to_message_id=reply_id, 
                progress=pyrogram_progress,
                progress_args=(status, [time.time()], f"📤 Uploading Video {v_idx}", title)
            )
            mark_media_processed(v_url, album_id); await asyncio.sleep(2)
            os.remove(filepath); os.remove(thumb)
        except: pass

    mark_processed(album_id); await status.delete(); return True

# --- 7. HANDLERS ---

@app.on_message(filters.command("user", prefixes=".") & (filters.me | filters.user(ADMIN_IDS)))
async def user_cmd(client, message):
    chat_id = message.chat.id; raw_input = message.command[1].strip(); cancel_tasks[chat_id] = False
    query = raw_input.split("/")[-1]; msg = await message.reply("🕵️‍♂️ **Initializing Scraper...**")
    
    all_urls = []; h = {'User-Agent': 'Mozilla/5.0'}
    for page in range(1, 50):
        if cancel_tasks.get(chat_id): break
        await safe_edit(msg, f"🔍 Scanning `{query}` - Page {page}...\nFound: {len(all_urls)} Albums")
        res = session.get(f"https://www.erome.com/{query}?page={page}", headers=h, timeout=15)
        ids = list(dict.fromkeys(re.findall(r'/a/([a-zA-Z0-9]{8})', res.text)))
        if not ids: break
        for aid in ids:
            u = f"https://www.erome.com/a/{aid}"
            if u not in all_urls: all_urls.append(u)
        if "Next" not in res.text: break

    if not all_urls: return await msg.edit_text("❌ No items found.")
    for i, url in enumerate(all_urls, 1):
        if cancel_tasks.get(chat_id): break
        await process_album(client, chat_id, message.id, url, query, i, len(all_urls))
    await message.reply(f"🏆 Archive Complete!")

@app.on_message(filters.command("reset", prefixes=".") & (filters.me | filters.user(ADMIN_IDS)))
async def reset_db(client, message):
    if os.path.exists(DB_NAME): os.remove(DB_NAME)
    init_db(); backup_to_github(); await message.reply("🧹 Memory Cleared!")

@app.on_message(filters.command("cancel", prefixes=".") & (filters.me | filters.user(ADMIN_IDS)))
async def cancel_cmd(client, message):
    cancel_tasks[message.chat.id] = True; await message.reply("🛑 Cancelled!")

@app.on_message(filters.command("dl", prefixes=".") & (filters.me | filters.user(ADMIN_IDS)))
async def dl_handler(client, message):
    urls = list(dict.fromkeys([u.strip() for u in message.text.split('\n') if "erome.com/a/" in u]))
    for i, url in enumerate(urls, 1): await process_album(client, message.chat.id, message.id, url, "single", i, len(urls))
    try: await message.delete()
    except: pass

async def main():
    init_db(); 
    async with app:
        print("LOG: V8.95 Ultra Final Online!"); await idle()

if __name__ == "__main__":
    app.run(main())
