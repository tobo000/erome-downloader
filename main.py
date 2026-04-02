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

# --- 2. HELPERS ---
def create_progress_bar(current, total):
    if total <= 0: return "[░░░░░░░░░░] 0%"
    pct = min(100, (current / total) * 100)
    return f"[{'█' * int(pct/10)}{'░' * (10 - int(pct/10))}] {pct:.1f}%"

def get_human_size(num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0: return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

async def progress_callback(current, total, status_msg, start_time, action_text):
    now = time.time()
    if now - start_time[0] > 5:
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
        streams = data.get('streams', [])
        v = next((s for s in streams if s['codec_type'] == 'video'), {})
        width = int(v.get('width', 1280))
        height = int(v.get('height', 720))
        has_audio = any(s['codec_type'] == 'audio' for s in streams)
        return duration, width, height, has_audio
    except: return 0, 1280, 720, False

def download_nitro(url, path, headers, size, segs=4):
    chunk = size // segs
    def dl_part(s, e, n):
        pp = f"{path}.p{n}"; h = headers.copy(); h['Range'] = f'bytes={s}-{e}'
        try:
            with requests.get(url, headers=h, stream=True, timeout=60) as r:
                with open(pp, 'wb') as f:
                    for chk in r.iter_content(chunk_size=1024*1024): f.write(chk)
        except: pass
    with ThreadPoolExecutor(max_workers=segs) as ex:
        for i in range(segs):
            start = i * chunk
            end = (i + 1) * chunk - 1 if i < segs - 1 else size - 1
            ex.submit(dl_part, start, end, i)
    with open(path, 'wb') as f:
        for i in range(segs):
            pp = f"{path}.p{i}"
            if os.path.exists(pp):
                with open(pp, 'rb') as pf: f.write(pf.read()); os.remove(pp)

# ==========================================
# SCRAPER ENGINE
# ==========================================
def scrape_album_details(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0', 'Referer': 'https://www.erome.com/'}
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
    all_urls = []
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/121.0.0.0', 'Referer': 'https://www.erome.com/'}
    for tab in ["", "/reposts"]:
        page = 1
        while True:
            await status_msg.edit_text(f"🔍 **Scanning `{username}`...**\n🚀 Found: `{len(all_urls)}` items\n📄 Page: {page}")
            url = f"https://www.erome.com/{username}{tab}?page={page}"
            try:
                res = session.get(url, headers=headers, timeout=20)
                if res.status_code != 200: break
                html = res.text
                album_ids = re.findall(r'/a/([a-zA-Z0-9]+)', html)
                if not album_ids: break
                new = 0
                for aid in album_ids:
                    f_url = f"https://www.erome.com/a/{aid}"
                    if f_url not in all_urls: all_urls.append(f_url); new += 1
                if new == 0 or "Next" not in html: break
                page += 1
                await asyncio.sleep(0.5)
            except: break
    return all_urls

# ==========================================
# CORE DELIVERY (V8.83: Final Fix for 0.0B)
# ==========================================
async def process_album(client, chat_id, reply_id, url, username, current, total):
    album_id = url.rstrip('/').split('/')[-1]
    if is_processed(album_id): return True

    title, photos, videos = scrape_album_details(url)
    if not photos and not videos: return False
    
    user_folder = os.path.join(DOWNLOAD_DIR, username)
    if not os.path.exists(user_folder): os.makedirs(user_folder)
    
    status = await client.send_message(chat_id, f"📥 **[{current}/{total}]** Preparing: `{title}`", reply_to_message_id=reply_id)

    # Photos
    if photos:
        p_files = []
        for i, p_url in enumerate(photos, 1):
            path = os.path.join(user_folder, f"img_{album_id}_{i}.jpg")
            try:
                r = session.get(p_url, timeout=30)
                with open(path, 'wb') as f: f.write(r.content)
                if os.path.exists(path): p_files.append(path)
                if len(p_files) == 10 or i == len(photos):
                    if p_files:
                        await client.send_media_group(chat_id, [InputMediaPhoto(pf, caption=f"🖼 {title}") for pf in p_files], reply_to_message_id=reply_id)
                        for pf in p_files: os.remove(pf)
                    p_files = []
            except: pass

    # Videos (Improved Size Handling)
    if videos:
        for v_idx, v_url in enumerate(videos, 1):
            v_name = f"{album_id}_v{v_idx}.mp4"
            filepath = os.path.join(user_folder, v_name)
            headers = {'User-Agent': 'Mozilla/5.0 Chrome/121.0.0.0', 'Referer': url}
            try:
                # Get size via GET stream instead of HEAD to avoid 0.0B
                with requests.get(v_url, headers=headers, stream=True, timeout=15) as r:
                    size = int(r.headers.get('content-length', 0))
                
                await status.edit_text(f"📥 **[{current}/{total}]** Downloading Video {v_idx}/{len(videos)}...\n📦 Size: {get_human_size(size)}")
                
                if size > 15*1024*1024:
                    download_nitro(v_url, filepath, headers, size)
                else:
                    with requests.get(v_url, headers=headers, stream=True, timeout=60) as r:
                        with open(filepath, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=1024*1024): f.write(chunk)
                
                if not os.path.exists(filepath) or os.path.getsize(filepath) < 1000: continue

                dur, w, h, has_audio = get_video_meta(filepath)
                if not has_audio:
                    temp_fix = filepath + ".fix.mp4"
                    subprocess.run(['ffmpeg', '-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100', '-i', filepath, '-c:v', 'copy', '-c:a', 'aac', '-shortest', temp_fix, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    if os.path.exists(temp_fix): os.remove(filepath); os.rename(temp_fix, filepath)
                
                thumb = filepath + ".jpg"
                subprocess.run(['ffmpeg', '-ss', '00:00:01', '-i', filepath, '-vframes', '1', '-q:v', '2', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                start_time = [time.time()]
                await client.send_video(
                    chat_id=chat_id, video=filepath, thumb=thumb if os.path.exists(thumb) else None,
                    width=w, height=h, duration=dur, caption=f"🎬 **{title}**\n📦 {get_human_size(size)}",
                    supports_streaming=True, reply_to_message_id=reply_id,
                    progress=progress_callback, progress_args=(status, start_time, f"Uploading Video {v_idx}/{len(videos)}")
                )
                if os.path.exists(filepath): os.remove(filepath)
                if os.path.exists(thumb): os.remove(thumb)
            except: pass
    
    mark_processed(album_id)
    await status.delete()
    return True

# ==========================================
# COMMAND HANDLERS
# ==========================================
@app.on_message(filters.command("user", prefixes="."))
async def user_cmd(client, message):
    if len(message.command) < 2: return
    input_data = message.command[1].strip()
    username = input_data.split("erome.com/")[-1].split('/')[0].split('?')[0] if "erome.com/" in input_data else input_data
    chat_id = message.chat.id
    cancel_tasks[chat_id] = False 

    msg = await message.reply(f"🛰 **Checking Erome...**")
    all_urls = await scan_all_content(username, msg)
    if not all_urls: return await msg.edit_text(f"❌ No content for `{username}`.")

    total = len(all_urls)
    await msg.edit_text(f"✅ Found: `{total}` items. Archiving...", 
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 STOP", callback_data=f"stop_task|{chat_id}")]]))

    for i, url in enumerate(all_urls, 1):
        if cancel_tasks.get(chat_id): break
        await process_album(client, chat_id, message.id, url, username, i, total)
        await asyncio.sleep(1)
    await msg.delete(); await message.reply(f"🏆 Done for `{username}`!")

@app.on_callback_query(filters.regex(r"^stop_task\|"))
async def handle_stop(client, callback: CallbackQuery):
    cancel_tasks[int(callback.data.split("|")[1])] = True
    await callback.answer("🛑 Stopping...", show_alert=True)

@app.on_message(filters.command("dl", prefixes="."))
async def dl_handler(client, message):
    urls = list(dict.fromkeys([u.strip() for u in message.text.split('\n') if "erome.com/a/" in u]))
    for i, url in enumerate(urls, 1): 
        await process_album(client, message.chat.id, message.id, url, "general", i, len(urls))
    try: await message.delete()
    except: pass

async def main():
    init_db()
    async with app:
        print("LOG: V8.83 Ready (Fixed 0.0B & Document issue)!")
        await idle()

if __name__ == "__main__":
    app.run(main())
