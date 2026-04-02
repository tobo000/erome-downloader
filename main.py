import os
import asyncio
import aiohttp
import aiofiles
import time
import subprocess
import json
import sqlite3
from bs4 import BeautifulSoup
from pyrogram import Client, filters, idle
from pyrogram.types import InputMediaPhoto, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SUDO_USERS = [int(x.strip()) for x in os.getenv("SUDO_USERS", "").split(",")]

app = Client("tobo_pro_session", api_id=API_ID, api_hash=API_HASH)
DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)

# Global tracker for stop button
cancel_tasks = {}

# --- DATABASE (Prevents Duplicates) ---
def init_db():
    conn = sqlite3.connect("archive_data.db")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS processed (url TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

def is_processed(url):
    conn = sqlite3.connect("archive_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed WHERE url = ?", (url,))
    res = cursor.fetchone()
    conn.close()
    return res is not None

def mark_processed(url):
    conn = sqlite3.connect("archive_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO processed (url) VALUES (?)", (url,))
        conn.commit()
    except: pass
    conn.close()

# --- DOWNLOADING ENGINE ---
async def download_file(url, path):
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.erome.com/'}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=600) as r:
                if r.status == 200:
                    async with aiofiles.open(path, mode='wb') as f:
                        async for chunk in r.content.iter_chunked(1024*1024):
                            await f.write(chunk)
                    return True
    except: pass
    return False

def get_video_meta(video_path):
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8')
        data = json.loads(res)
        duration = int(float(data['format']['duration']))
        v = next(s for s in data['streams'] if s['codec_type'] == 'video')
        return duration, v['width'], v['height']
    except: return 0, 0, 0

# --- DEEP SCRAPER (V8.50) ---
async def scrape_album_details(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as r:
            soup = BeautifulSoup(await r.text(), 'html.parser')
            title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Untitled"
            
            # Find all possible video links
            v_links = []
            for v in soup.find_all('video'):
                src = v.get('src') or v.get('data-src')
                if not src:
                    st = v.find('source')
                    if st: src = st.get('src') or st.get('data-src')
                if src:
                    v_links.append(src if src.startswith('http') else 'https:' + src)

            # Find all image links
            p_links = []
            for img in soup.select('div.img img'):
                src = img.get('data-src') or img.get('src')
                if src and "erome.com" in src:
                    p_links.append(src if src.startswith('http') else 'https:' + src)
            
            return title, list(dict.fromkeys(p_links)), list(dict.fromkeys(v_links))

async def get_all_profile_links(username, status_msg):
    """Robust crawler for both Albums and Reposts"""
    headers = {'User-Agent': 'Mozilla/5.0'}
    all_urls = []
    
    async with aiohttp.ClientSession() as session:
        for sub in ["", "/reposts"]:
            label = "Originals" if sub == "" else "Reposts"
            page = 1
            while True:
                await status_msg.edit_text(f"🕵️‍♂️ Scanning `{username}`\nSection: **{label}**\nPage: **{page}**\nFound: {len(all_urls)} items")
                url = f"https://www.erome.com/{username}{sub}?page={page}"
                
                async with session.get(url, headers=headers) as r:
                    if r.status != 200: break
                    soup = BeautifulSoup(await r.text(), 'html.parser')
                    
                    # Extract album links
                    links = [a['href'] for a in soup.find_all("a", href=True) if "/a/" in a['href']]
                    if not links: break
                    
                    for l in links:
                        full_url = l if l.startswith('http') else 'https://www.erome.com' + l
                        if full_url not in all_urls:
                            all_urls.append(full_url)
                    
                    # Look for Next button
                    if not soup.find("a", string="Next"): break
                    page += 1
                    await asyncio.sleep(0.5) # Anti-ban delay
    return all_urls

# --- CORE ENGINE ---
async def process_single_album(client, chat_id, url, topic_id):
    if is_processed(url): return
    
    title, photos, videos = await scrape_album_details(url)
    if not photos and not videos: return
    
    album_id = url.rstrip('/').split('/')[-1]
    progress = await client.send_message(chat_id, f"📥 **Archiving:** `{title}`\n(📸 {len(photos)} | 🎬 {len(videos)})", message_thread_id=topic_id)

    # Photos
    photo_paths = []
    for i, p_url in enumerate(photos, 1):
        path = os.path.join(DOWNLOAD_DIR, f"{album_id}_p{i}.jpg")
        if await download_file(p_url, path):
            photo_paths.append(path)
        if len(photo_paths) == 10 or i == len(photos):
            if photo_paths:
                try:
                    await client.send_media_group(chat_id, [InputMediaPhoto(p, caption=f"🖼 {title}") for p in photo_paths], message_thread_id=topic_id)
                except: pass
                for p in photo_paths: 
                    if os.path.exists(p): os.remove(p)
                photo_paths = []

    # Videos
    for i, v_url in enumerate(videos, 1):
        path = os.path.join(DOWNLOAD_DIR, f"{album_id}_v{i}.mp4")
        if await download_file(v_url, path):
            dur, w, h = get_video_meta(path)
            thumb = path + ".jpg"
            subprocess.run(['ffmpeg', '-ss', '00:00:01', '-i', path, '-vframes', '1', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.STNULL)
            try:
                await client.send_video(chat_id, path, thumb=thumb, duration=dur, width=w, height=h, caption=f"🎬 {title}", supports_streaming=True, message_thread_id=topic_id)
            except: pass
            if os.path.exists(path): os.remove(path)
            if os.path.exists(thumb): os.remove(thumb)

    mark_processed(url)
    await progress.delete()

# --- COMMANDS ---
@app.on_message(filters.command("user", prefixes=".") & filters.user(SUDO_USERS))
async def user_cmd(client, message):
    if len(message.command) < 2: return
    username = message.command[1]
    chat_id = message.chat.id
    cancel_tasks[chat_id] = False
    topic_id = getattr(message, "message_thread_id", None)

    status = await message.reply(
        f"🕵️‍♂️ **Deep Crawl Starting...**",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Stop Archiving", callback_query_data=f"stop_{chat_id}")]])
    )

    # Scrape both originals and reposts
    urls = await get_all_profile_links(username, status)
    
    await status.edit_text(f"🚀 Found **{len(urls)}** items (Albums + Reposts).\nStarting Download Queue...")

    for i, url in enumerate(urls, 1):
        if cancel_tasks.get(chat_id):
            await message.reply("🛑 **Archive Stopped by Admin.**")
            break
        
        await process_single_album(client, chat_id, url, topic_id)
        await asyncio.sleep(1) # Safe interval

    await status.delete()
    await message.reply(f"🏆 **Archive Complete:** `{username}`")

@app.on_callback_query(filters.regex("^stop_"))
async def stop_callback(client, callback_query: CallbackQuery):
    chat_id = int(callback_query.data.split("_")[1])
    cancel_tasks[chat_id] = True
    await callback_query.answer("Stopping... please wait.", show_alert=True)
    await callback_query.message.edit_text("🛑 **Stopping... completing current item.**")

async def main():
    init_db()
    async with app:
        print("LOG: V8.50 Full Archive is Online!")
        await idle()

if __name__ == "__main__":
    app.run(main())
