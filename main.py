import os
import asyncio
import aiohttp
import aiofiles
import time
import subprocess
import json
import re
import sqlite3
import gc
from bs4 import BeautifulSoup
from pyrogram import Client, filters, idle
from pyrogram.types import InputMediaPhoto, InputMediaVideo, BotCommand
from pyrogram.errors import FloodWait, MessageNotModified
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
sudo_raw = os.getenv("SUDO_USERS", "")
SUDO_USERS = [int(x.strip()) for x in sudo_raw.split(",") if x.strip().isdigit()]

app = Client("tobo_pro_session", api_id=int(API_ID), api_hash=API_HASH, sleep_threshold=300)
DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)

DB_NAME = "bot_archive.db"

# --- 2. DATABASE ENGINE ---
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
    except: pass
    conn.close()

# --- 3. CORE HELPERS ---
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
        v_stream = next((s for s in data['streams'] if s['codec_type'] == 'video'), {})
        return duration, int(v_stream.get('width', 1280)), int(v_stream.get('height', 720)), any(s['codec_type'] == 'audio' for s in data['streams'])
    except: return 0, 1280, 720, False

async def download_file_tank(url, path):
    """Heavy-duty single-threaded download for 1GB RAM safety"""
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/121.0.0.0', 'Referer': 'https://www.erome.com/'}
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url, timeout=600) as r:
                if r.status != 200: return False
                async with aiofiles.open(path, mode='wb') as f:
                    await f.write(await r.read())
                return True
    except: return False

# ==========================================
# ADVANCED SCRAPER (Non-blocking)
# ==========================================
async def scrape_album_details(url):
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/123.0.0.0'}
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url) as r:
                html = await r.text()
                soup = BeautifulSoup(html, 'html.parser')
                title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Untitled"
                p_l = [x if x.startswith('http') else 'https:' + x for x in [i.get('data-src') or i.get('src') for i in soup.select('div.img img') if "erome.com" in (i.get('data-src') or i.get('src', ''))] if x]
                v_l = list(dict.fromkeys(re.findall(r'https?://[^\s"\'\\>]+erome\.com[^\s"\'\\>]+\.mp4', html)))
                v_l.sort(key=lambda x: (re.search(r'(1080|720|high)', x.lower()) is None, x))
                return title, p_l, v_l
    except: return "Error", [], []

# ==========================================
# DELIVERY ENGINE (The Tank Method)
# ==========================================
async def process_album_tank(client, message, url, username):
    if is_processed(url): return True
    title, photos, videos = await scrape_album_details(url)
    if not photos and not videos: return False
    
    user_folder = os.path.join(DOWNLOAD_DIR, username)
    os.makedirs(user_folder, exist_ok=True)
    
    # Simple status without animation to avoid stuck
    status = await message.reply_text(f"📦 Processing: `{title}`")
    
    # 📸 PHOTOS
    if photos:
        p_paths = []
        for i, p_url in enumerate(photos, 1):
            path = os.path.join(user_folder, f"img_{i}.jpg")
            async with aiohttp.ClientSession() as sess:
                async with sess.get(p_url) as r:
                    if r.status == 200:
                        with open(path, 'wb') as f: f.write(await r.read())
                        p_paths.append(path)
            if len(p_paths) == 10 or i == len(photos):
                try: await message.reply_media_group([InputMediaPhoto(pf, caption=f"🖼 {title}") for pf in p_paths])
                except: pass
                for pf in p_paths: os.remove(pf)
                p_paths = []

    # 🎬 VIDEOS
    if videos:
        for i, v_url in enumerate(videos, 1):
            filepath = os.path.join(user_folder, "temp_video.mp4")
            if await download_file_tank(v_url, filepath):
                dur, w, h, audio = get_video_meta(filepath)
                if not audio:
                    temp = filepath + ".fix.mp4"
                    subprocess.run(['ffmpeg', '-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100', '-i', filepath, '-c:v', 'copy', '-c:a', 'aac', '-shortest', temp, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    os.remove(filepath); os.rename(temp, filepath)
                
                thumb = filepath + ".jpg"
                subprocess.run(['ffmpeg', '-ss', '00:00:01', '-i', filepath, '-vf', 'scale=320:-1', '-vframes', '1', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                await message.reply_video(filepath, thumb=thumb, duration=dur, width=w, height=h, caption=f"🎬 {title}", supports_streaming=True)
                if os.path.exists(filepath): os.remove(filepath)
                if os.path.exists(thumb): os.remove(thumb)

    mark_processed(url)
    await status.delete()
    gc.collect() # Clean RAM
    return True

# ==========================================
# THE TANK COMMAND HANDLER
# ==========================================
@app.on_message(filters.command("user", prefixes=".") & filters.user(SUDO_USERS))
async def user_cmd(client, message):
    raw_input = message.command[1].strip()
    username = raw_input.split("erome.com/")[-1].split('/')[0]
    
    main_msg = await message.reply(f"🚀 **Tank Archive Started for {username}**\nMode: Lazy Scan (Scan & Download)")
    
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/122.0.0.0'}
    processed_in_session = 0

    async with aiohttp.ClientSession(headers=headers) as sess:
        for tab in ["", "/reposts"]:
            page = 1
            while True:
                url = f"https://www.erome.com/{username}{tab}?page={page}"
                async with sess.get(url) as res:
                    if res.status != 200: break
                    html = await res.text()
                    album_ids = list(dict.fromkeys(re.findall(r'/a/([a-zA-Z0-9]+)', html)))
                    
                    if not album_ids: break
                    
                    for aid in album_ids:
                        target_url = f"https://www.erome.com/a/{aid}"
                        # Start downloading immediately after finding a link
                        success = await process_album_tank(client, message, target_url, username)
                        if success: processed_in_session += 1
                    
                    if "Next" not in html: break
                    page += 1
                    await asyncio.sleep(2) # Prevent Erome Block

    await main_msg.edit_text(f"🏆 **Mission Accomplished!**\nUser: `{username}`\nItems: `{processed_in_session}`")

@app.on_message(filters.command("dl", prefixes=".") & filters.user(SUDO_USERS))
async def dl_handler(client, message):
    urls = list(dict.fromkeys([u.strip() for u in message.text.split('\n') if "erome.com/a/" in u]))
    for i, url in enumerate(urls, 1): await process_album_tank(client, message, url, "single")
    try: await message.delete()
    except: pass

async def main():
    init_db()
    async with app:
        print("LOG: V9.07 Tank Edition is Online!")
        await idle()

if __name__ == "__main__":
    app.run(main())
