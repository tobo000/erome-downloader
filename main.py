import os
import asyncio
import requests
import time
import subprocess
import json
import logging
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.types import InputMediaPhoto
from concurrent.futures import ThreadPoolExecutor

# Setup logging to track errors
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load credentials from .env file
load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

app = Client("erome_downloader_session", api_id=API_ID, api_hash=API_HASH)
DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)
session = requests.Session()

# --- HELPERS ---
def get_human_size(num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0: return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

async def edit_status(message, text, last_update_time, force=False):
    now = time.time()
    if force or (now - last_update_time[0] > 4):
        try:
            await message.edit_text(text)
            last_update_time[0] = now
        except: pass

def get_video_meta(video_path):
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8')
        data = json.loads(res)
        duration = int(float(data['format'].get('duration', 0)))
        v = next(s for s in data['streams'] if s['codec_type'] == 'video')
        return duration, v.get('width', 0), v.get('height', 0)
    except Exception as e:
        logger.error(f"Metadata extraction error: {e}")
        return 0, 0, 0

def download_nitro(url, path, headers, size, segs=4):
    chunk = size // segs
    def dl_part(s, e, n):
        pp = f"{path}.p{n}"
        h = headers.copy()
        h['Range'] = f'bytes={s}-{e}'
        try:
            with session.get(url, headers=h, stream=True, timeout=30) as r:
                with open(pp, 'wb') as f:
                    for chk in r.iter_content(chunk_size=1024*1024): f.write(chk)
        except Exception as e:
            logger.error(f"Part {n} failed: {e}")

    with ThreadPoolExecutor(max_workers=segs) as ex:
        futures = [ex.submit(dl_part, i*chunk, (i+1)*chunk-1 if i < segs-1 else size-1, i) for i in range(segs)]
        for fut in futures: fut.result()

    with open(path, 'wb') as f:
        for i in range(segs):
            pp = f"{path}.p{i}"
            if os.path.exists(pp):
                with open(pp, 'rb') as pf: f.write(pf.read())
                os.remove(pp)

# ==========================================
# SCRAPER ENGINE
# ==========================================
def scrape_album_details(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        res = session.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        title_tag = soup.find("h1") or soup.find("title")
        album_title = title_tag.get_text(strip=True) if title_tag else "Untitled Album"
        
        # Get Videos
        v_l = []
        for video in soup.find_all('video'):
            source = video.find('source')
            src = video.get('src') or (source.get('src') if source else None)
            if src:
                if not src.startswith('http'): src = 'https:' + src
                v_l.append(src)
        
        # Get Photos
        p_l = []
        for img in soup.select('div.img img'):
            src = img.get('data-src') or img.get('src')
            if src and "erome.com" in src:
                if not src.startswith('http'): src = 'https:' + src
                p_l.append(src)

        return album_title, list(dict.fromkeys(p_l)), list(dict.fromkeys(v_l))
    except Exception as e:
        logger.error(f"Scraping error: {e}")
        return "Error", [], []

def get_all_profile_links(username):
    headers = {'User-Agent': 'Mozilla/5.0'}
    all_links = []
    sub_paths = ["", "/reposts"]
    
    for sub in sub_paths:
        page = 1
        while True:
            url = f"https://www.erome.com/{username}{sub}?page={page}"
            try:
                res = session.get(url, headers=headers, timeout=20)
                if res.status_code != 200: break
                soup = BeautifulSoup(res.text, 'html.parser')
                links = [a['href'] for a in soup.find_all("a", href=True) if "/a/" in a['href']]
                if not links: break
                
                for link in links:
                    full_url = link if link.startswith('http') else 'https://www.erome.com' + link
                    all_links.append(full_url)
                
                if not soup.find("a", string=lambda x: x and "Next" in x): break
                page += 1
                if page > 50: break 
            except: break
            
    return list(dict.fromkeys(all_links))

# ==========================================
# CORE PROCESSING ENGINE
# ==========================================
async def process_single_album(client, message, url):
    title, photos, videos = scrape_album_details(url)
    if not photos and not videos: return
    
    album_id = url.rstrip('/').split('/')[-1]
    status_msg = await client.send_message(message.chat.id, f"🔍 Scraping: **{title}**")
    last_edit = [0]

    # Handle Photos
    if photos:
        photo_files = []
        for p_idx, p_url in enumerate(photos, 1):
            filepath = os.path.join(DOWNLOAD_DIR, f"img_{album_id}_{p_idx}.jpg")
            try:
                r = session.get(p_url, timeout=20)
                with open(filepath, 'wb') as f: f.write(r.content)
                photo_files.append(filepath)
                
                if len(photo_files) == 10 or p_idx == len(photos):
                    await client.send_media_group(
                        message.chat.id, 
                        [InputMediaPhoto(pf, caption=f"🖼 **{title}**" if i==0 else "") for i, pf in enumerate(photo_files)],
                        reply_to_message_id=message.id
                    )
                    for pf in photo_files: 
                        if os.path.exists(pf): os.remove(pf)
                    photo_files = []
                    await asyncio.sleep(1)
            except: pass

    # Handle Videos
    if videos:
        for v_idx, v_url in enumerate(videos, 1):
            filename = f"vid_{album_id}_{v_idx}.mp4"
            filepath = os.path.join(DOWNLOAD_DIR, filename)
            headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.erome.com/'}
            thumb = filepath + ".jpg"
            try:
                head = session.head(v_url, headers=headers, allow_redirects=True)
                t_s = int(head.headers.get('content-length', 0))
                
                await edit_status(status_msg, f"📥 **Downloading:** {title}\nVideo {v_idx}/{len(videos)}\nSize: {get_human_size(t_s)}", last_edit, force=True)
                
                if t_s > 15*1024*1024: 
                    download_nitro(v_url, filepath, headers, t_s)
                else:
                    with session.get(v_url, headers=headers, stream=True) as r:
                        with open(filepath, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=1024*1024): f.write(chunk)
                
                dur, w, h = get_video_meta(filepath)
                subprocess.run(['ffmpeg', '-ss', '00:00:01', '-i', filepath, '-vframes', '1', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.STNULL)
                
                await client.send_video(
                    message.chat.id, filepath, thumb=thumb if os.path.exists(thumb) else None,
                    width=w, height=h, duration=dur, 
                    caption=f"🎬 **{title}**\n📦 {get_human_size(t_s)}",
                    supports_streaming=True, reply_to_message_id=message.id
                )
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Upload error: {e}")
            finally:
                if os.path.exists(filepath): os.remove(filepath)
                if os.path.exists(thumb): os.remove(thumb)

    await status_msg.delete()

# ==========================================
# BOT COMMANDS
# ==========================================
@app.on_message(filters.command("dl", prefixes="."))
async def dl_cmd(client, message):
    urls = list(dict.fromkeys([u.strip() for u in message.text.split() if "erome.com/a/" in u]))
    for url in urls: 
        await process_single_album(client, message, url)
        await asyncio.sleep(3)

@app.on_message(filters.command("user", prefixes="."))
async def user_cmd(client, message):
    if len(message.command) < 2: return
    username = message.command[1]
    status = await message.reply(f"🕵️‍♂️ **Crawling Profile:** `{username}`...")
    
    urls = get_all_profile_links(username)
    if not urls: return await status.edit(f"❌ No content found for this user.")
    
    await status.edit(f"🚀 Found **{len(urls)}** items. Archiving now...")
    
    for url in urls:
        try:
            await process_single_album(client, message, url)
            await asyncio.sleep(5) 
        except Exception as e:
            logger.error(f"Profile error at {url}: {e}")

    await message.reply(f"🏆 **Job Finished:** `{username}` fully archived.")
    await status.delete()

if __name__ == "__main__":
    print("Bot is running...")
    app.run()
