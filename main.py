import os
import asyncio
import requests
import time
import subprocess
import json
import re
from bs4 import BeautifulSoup
from pyrogram import Client, filters, idle
from pyrogram.types import InputMediaPhoto, InputMediaVideo
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

if not API_ID or not API_HASH:
    print("❌ FATAL ERROR: Check your .env file!")
    exit(1)

app = Client("tobo_pro_session", api_id=int(API_ID), api_hash=API_HASH)
DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR): 
    os.makedirs(DOWNLOAD_DIR)
session = requests.Session()

# --- HELPERS ---
def create_progress_bar(current, total):
    if total <= 0: return "[░░░░░░░░░░] 0%"
    pct = min(100, current * 100 / total)
    return f"[{'█' * int(pct/10)}{'░' * (10 - int(pct/10))}] {pct:.1f}%"

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
    if not os.path.exists(video_path) or os.path.getsize(video_path) == 0:
        return 0, 1280, 720, False
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8')
        data = json.loads(res)
        duration = int(float(data.get('format', {}).get('duration', 0)))
        v = next(s for s in data['streams'] if s['codec_type'] == 'video')
        has_audio = any(s['codec_type'] == 'audio' for s in data['streams'])
        return duration, v.get('width', 1280), v.get('height', 720), has_audio
    except:
        return 0, 1280, 720, False

def download_nitro(url, path, headers, size, segs=4):
    chunk = size // segs
    def dl_part(s, e, n):
        pp = f"{path}.p{n}"; h = headers.copy(); h['Range'] = f'bytes={s}-{e}'
        try:
            with session.get(url, headers=h, stream=True, timeout=60) as r:
                with open(pp, 'wb') as f:
                    for chk in r.iter_content(chunk_size=1024*1024): f.write(chk)
        except: pass
    with ThreadPoolExecutor(max_workers=segs) as ex:
        for i in range(segs):
            start = i * chunk
            end = (i + 1) * chunk - 1 if i < size - 1 else size - 1
            ex.submit(dl_part, start, end, i)
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
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0'}
    try:
        res = session.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        title_tag = soup.find("h1") or soup.find("title")
        album_title = title_tag.get_text(strip=True) if title_tag else "Untitled"
        
        p_l = list(dict.fromkeys([i.get('data-src') or i.get('src') for i in soup.select('div.img img') if "erome.com" in (i.get('data-src') or i.get('src', ''))]))
        p_l = [x if x.startswith('http') else 'https:' + x for x in p_l if x]
        
        v_candidates = []
        for tag in soup.find_all(['video', 'source', 'a']):
            src = tag.get('src') or tag.get('data-src') or tag.get('href')
            if src and ".mp4" in src.lower():
                v_candidates.append(src if src.startswith('http') else 'https:' + src)
        v_candidates.extend(re.findall(r'https?://[^\s"\'>]+\.mp4', res.text))
        v_l = list(dict.fromkeys([v for v in v_candidates if "erome.com" in v]))
        v_l.sort(key=lambda x: (re.search(r'(1080|720|high)', x.lower()) is None, x))
        return album_title, p_l, v_l
    except: return "Error", [], []

async def get_all_profile_content(username, status_msg):
    """Deep Scanner: Loops through every page of Posts and Reposts correctly"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0'}
    all_links = []
    
    for tab in ["", "/reposts"]:
        page = 1
        tab_name = "Original Posts" if tab == "" else "Reposts"
        while True:
            # Live Status Update
            await status_msg.edit_text(f"🕵️‍♂️ **Scanning {username}...**\nSection: `{tab_name}`\nPage: `{page}`\nItems Found: `{len(all_links)}`")
            
            url = f"https://www.erome.com/{username}{tab}?page={page}"
            try:
                res = session.get(url, headers=headers, timeout=20)
                if res.status_code != 200: break
                soup = BeautifulSoup(res.text, 'html.parser')
                
                # Find only valid album links
                links = [a['href'] for a in soup.find_all("a", href=True) if "/a/" in a['href'] and "erome.com" not in a['href']]
                
                if not links: break
                
                for l in links:
                    full_url = 'https://www.erome.com' + l
                    if full_url not in all_links:
                        all_links.append(full_url)
                
                # Robust 'Next' button detection
                next_page = soup.find("a", string=re.compile("Next", re.I))
                if not next_page: break
                
                page += 1
                await asyncio.sleep(0.5) 
            except: break
            
    return list(dict.fromkeys(all_links))

# ==========================================
# DELIVERY ENGINE
# ==========================================
async def process_album(client, message, url):
    title, photos, videos = scrape_album_details(url)
    if not photos and not videos: return
    
    album_id = url.rstrip('/').split('/')[-1]
    status = await client.send_message(message.chat.id, f"🔍 **Analyzing:** `{title}`", reply_to_message_id=message.id)
    last_edit = [0]

    # 1. Photos
    if photos:
        p_files = []
        for p_idx, p_url in enumerate(photos, 1):
            filepath = os.path.join(DOWNLOAD_DIR, f"img_{album_id}_{p_idx}.jpg")
            try:
                r = session.get(p_url, timeout=30)
                with open(filepath, 'wb') as f: f.write(r.content)
                if os.path.exists(filepath): p_files.append(filepath)
                if len(p_files) == 10 or p_idx == len(photos):
                    if p_files:
                        await client.send_media_group(message.chat.id, [InputMediaPhoto(pf, caption=f"🖼 **{title}**") for pf in p_files], reply_to_message_id=message.id)
                        for pf in p_files: os.remove(pf)
                    p_files = []
            except: pass

    # 2. Videos (Real Video Delivery)
    if videos:
        for v_idx, v_url in enumerate(videos, 1):
            v_name = v_url.split('/')[-1].split('?')[0]
            if ".mp4" not in v_name.lower(): v_name += ".mp4"
            filepath = os.path.join(DOWNLOAD_DIR, f"{album_id}_{v_name}")
            headers = {'User-Agent': 'Mozilla/5.0', 'Referer': url}
            try:
                head = session.head(v_url, headers=headers, allow_redirects=True, timeout=15)
                size = int(head.headers.get('content-length', 0))
                await edit_status(status, f"📥 **Downloading Video {v_idx}/{len(videos)}**\nSize: {get_human_size(size)}", last_edit, force=True)
                
                if size > 15*1024*1024: download_nitro(v_url, filepath, headers, size)
                else:
                    r = session.get(v_url, headers=headers, stream=True, timeout=60)
                    with open(filepath, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024*1024): f.write(chunk)
                
                if not os.path.exists(filepath): continue
                dur, w, h, has_audio = get_video_meta(filepath)
                
                if not has_audio:
                    temp_path = filepath + ".fix.mp4"
                    subprocess.run(['ffmpeg', '-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100', '-i', filepath, '-c:v', 'copy', '-c:a', 'aac', '-shortest', temp_path, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    if os.path.exists(temp_path):
                        os.remove(filepath); os.rename(temp_path, filepath)
                
                thumb = filepath + ".jpg"
                subprocess.run(['ffmpeg', '-ss', '00:00:01', '-i', filepath, '-vframes', '1', '-q:v', '2', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                await client.send_video(message.chat.id, video=filepath, thumb=thumb if os.path.exists(thumb) else None, width=w, height=h, duration=dur, caption=f"🎬 **{title}**\n📦 {get_human_size(size)}", supports_streaming=True, reply_to_message_id=message.id)
                if os.path.exists(filepath): os.remove(filepath)
                if os.path.exists(thumb): os.remove(thumb)
            except: pass

    await status.delete()
    await client.send_message(message.chat.id, f"✅ **COMPLETED:** `{title}`", reply_to_message_id=message.id)

# ==========================================
# COMMAND HANDLERS
# ==========================================
@app.on_message(filters.command("dl", prefixes="."))
async def dl_handler(client, message):
    urls = list(dict.fromkeys([u.strip() for u in message.text.split('\n') if "erome.com/a/" in u]))
    for url in urls: await process_album(client, message, url)
    try: await message.delete()
    except: pass

@app.on_message(filters.command("user", prefixes="."))
async def user_handler(client, message):
    if len(message.command) < 2: return
    username = message.command[1]
    crawl_msg = await message.reply(f"🕵️‍♂️ **Initializing Scanner for `{username}`...**")
    
    urls = await get_all_profile_content(username, crawl_msg)
    
    if not urls:
        return await crawl_msg.edit(f"❌ No content found for `{username}`.")
    
    await crawl_msg.edit(f"🚀 Found **{len(urls)}** items (Originals + Reposts).\nStarting sequential archive...")
    
    for url in urls:
        await process_album(client, message, url)
        await asyncio.sleep(2)
        
    await message.reply(f"🏆 Archive for `{username}` complete! Total items: {len(urls)}")
    try:
        await crawl_msg.delete()
        await message.delete()
    except: pass

async def main():
    async with app:
        print("LOG: Tobo Pro V8.49 Ready (Deep Scanner Fix)!")
        await idle()

if __name__ == "__main__":
    app.run(main())
