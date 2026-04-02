import os
import asyncio
import requests
import time
import subprocess
import json
from bs4 import BeautifulSoup
from pyrogram import Client, filters, idle
from pyrogram.types import InputMediaPhoto
from concurrent.futures import ThreadPoolExecutor

# --- CONFIGURATION ---
# Replace these with your actual credentials
API_ID = 34684478
API_HASH = "3ee498f0d6b06bf3fa8a5b102af12942"

app = Client("tobo_pro_session", api_id=API_ID, api_hash=API_HASH)
DOWNLOAD_DIR = "downloads"

if not os.path.exists(DOWNLOAD_DIR): 
    os.makedirs(DOWNLOAD_DIR)

session = requests.Session()

# --- HELPERS ---

def create_progress_bar(current, total):
    if total == 0: return "[░░░░░░░░░░] 0%"
    pct = current * 100 / total
    return f"[{'█' * int(pct/10)}{'░' * (10 - int(pct/10))}] {pct:.1f}%"

def get_human_size(num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0: return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

async def edit_status(message, text, last_update_time, force=False):
    now = time.time()
    if force or (now - last_update_time[0] > 3):
        try:
            await message.edit_text(text)
            last_update_time[0] = now
        except: 
            pass

def get_video_meta(video_path):
    """Extracts duration, width, and height using FFprobe"""
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8')
        data = json.loads(res)
        duration = int(float(data['format']['duration']))
        v = next(s for s in data['streams'] if s['codec_type'] == 'video')
        return duration, v['width'], v['height']
    except: 
        return 0, 0, 0

def download_nitro(url, path, headers, size, segs=4):
    """Multi-threaded downloader for faster speeds"""
    chunk = size // segs
    
    def dl_part(s, e, n):
        part_path = f"{path}.p{n}"
        h = headers.copy()
        h['Range'] = f'bytes={s}-{e}'
        with session.get(url, headers=h, stream=True) as r:
            with open(part_path, 'wb') as f:
                for chk in r.iter_content(chunk_size=1024*1024):
                    f.write(chk)

    with ThreadPoolExecutor(max_workers=segs) as ex:
        futures = []
        for i in range(segs):
            start = i * chunk
            end = (i + 1) * chunk - 1 if i < segs - 1 else size - 1
            futures.append(ex.submit(dl_part, start, end, i))
        for future in futures:
            future.result()

    # Reassemble parts
    with open(path, 'wb') as f:
        for i in range(segs):
            part_path = f"{path}.p{i}"
            if os.path.exists(part_path):
                with open(part_path, 'rb') as pf: 
                    f.write(pf.read())
                os.remove(part_path)

# --- SCRAPER ENGINE ---

def scrape_album_details(url):
    """Scrapes titles, video links, and image links from an album URL"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
    try:
        res = session.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        title_tag = soup.find("h1") or soup.find("title")
        album_title = title_tag.get_text(strip=True) if title_tag else "Untitled Album"
        
        # Clean up unwanted HTML elements
        for j in soup.find_all(["div", "section"], {"id": ["related_albums", "comments", "footer"]}): 
            j.decompose()

        # Collect video links
        video_links = []
        for v in soup.find_all('video'):
            source = v.find('source')
            src = v.get('src') or v.get('data-src') or (source.get('src') if source else None)
            if src and ".mp4" in src.lower():
                video_links.append(src if src.startswith('http') else 'https:' + src)
        video_links = list(dict.fromkeys(video_links))

        # Collect photo links
        photo_links = []
        for i in soup.select('div.img img'):
            src = i.get('data-src') or i.get('src')
            if src and "erome.com" in src:
                photo_links.append(src if src.startswith('http') else 'https:' + src)
        photo_links = list(dict.fromkeys(photo_links))

        return album_title, photo_links, video_links
    except Exception as e:
        print(f"Scrape Error: {e}")
        return "Error", [], []

def get_all_profile_links(username):
    """Crawls all pages of a user profile including reposts"""
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
                
                if not soup.find("a", string="Next"): break
                page += 1
            except: break
            
    return list(dict.fromkeys(all_links))

# --- PROCESSING ENGINE ---

async def process_single_album(client, message, url, topic_id):
    title, photos, videos = scrape_album_details(url)
    if not photos and not videos: return

    album_id = url.rstrip('/').split('/')[-1]
    status_msg = await client.send_message(message.chat.id, f"🔍 **Crawling:** {title}", message_thread_id=topic_id)
    last_edit = [0]

    # Process Photos (Max 10 per media group)
    if photos:
        photo_files = []
        for p_idx, p_url in enumerate(photos, 1):
            filepath = os.path.join(DOWNLOAD_DIR, f"img_{album_id}_{p_idx}.jpg")
            await edit_status(status_msg, f"📸 **{title}**\nPhoto {p_idx}/{len(photos)}", last_edit)
            try:
                r = session.get(p_url)
                with open(filepath, 'wb') as f: f.write(r.content)
                photo_files.append(filepath)
                if len(photo_files) == 10 or p_idx == len(photos):
                    await client.send_media_group(message.chat.id, [InputMediaPhoto(pf, caption=f"🖼 **{title}**") for pf in photo_files], message_thread_id=topic_id)
                    for pf in photo_files: 
                        if os.path.exists(pf): os.remove(pf)
                    photo_files = []
            except: pass

    # Process Videos
    if videos:
        for v_idx, v_url in enumerate(videos, 1):
            filename = v_url.split('/')[-1].split('?')[0]
            filepath = os.path.join(DOWNLOAD_DIR, f"{album_id}_{filename}")
            headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.erome.com/'}
            try:
                head = session.head(v_url, headers=headers, allow_redirects=True)
                file_size = int(head.headers.get('content-length', 0))
                await edit_status(status_msg, f"📥 **{title}**\nVideo {v_idx}/{len(videos)}\nSize: {get_human_size(file_size)}", last_edit, force=True)
                
                # Use multi-threaded download if file is larger than 15MB
                if file_size > 15*1024*1024:
                    download_nitro(v_url, filepath, headers, file_size)
                else:
                    with session.get(v_url, headers=headers, stream=True) as r:
                        with open(filepath, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=1024*1024): f.write(chunk)
                
                duration, width, height = get_video_meta(filepath)
                thumb = filepath + ".jpg"
                subprocess.run(['ffmpeg', '-ss', '00:00:01', '-i', filepath, '-vframes', '1', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.STNULL)
                
                await client.send_video(
                    chat_id=message.chat.id, 
                    video=filepath, 
                    thumb=thumb, 
                    width=width, 
                    height=height, 
                    duration=duration, 
                    caption=f"🎬 **{title}**\n📦 {get_human_size(file_size)}", 
                    supports_streaming=True, 
                    message_thread_id=topic_id
                )
                if os.path.exists(filepath): os.remove(filepath)
                if os.path.exists(thumb): os.remove(thumb)
            except Exception as e:
                print(f"Video Upload Error: {e}")

    await status_msg.delete()

# --- BOT COMMANDS ---

@app.on_message(filters.command("dl", prefixes="."))
async def dl_cmd(client, message):
    urls = list(dict.fromkeys([u.strip() for u in message.text.split('\n') if "erome.com/a/" in u]))
    topic_id = getattr(message, "message_thread_id", None)
    for url in urls: 
        await process_single_album(client, message, url, topic_id)
    try: await message.delete()
    except: pass

@app.on_message(filters.command("user", prefixes="."))
async def user_cmd(client, message):
    if len(message.command) < 2: return
    username = message.command[1]
    status = await message.reply(f"🕵️‍♂️ **Deep Crawling:** `{username}`\nCollecting all albums and reposts...")
    
    urls = get_all_profile_links(username)
    if not urls: 
        return await status.edit(f"❌ No content found for profile: `{username}`.")

    await status.edit(f"🚀 **Found {len(urls)} items.**\nStarting sequential archive...")
    topic_id = getattr(message, "message_thread_id", None)

    for url in urls:
        await process_single_album(client, message, url, topic_id)
        await asyncio.sleep(2) # Safe delay

    await message.reply(f"🏆 **Mission Complete:** `{username}` fully archived.")
    await status.delete()

async def main():
    async with app:
        print("LOG: V8.33 Deep Crawler is Online!")
        await idle()

if __name__ == "__main__":
    app.run(main())
