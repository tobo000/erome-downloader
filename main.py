import os
import asyncio
import requests
import time
import subprocess
import json
from bs4 import BeautifulSoup
from pyrogram import Client, filters, idle
from pyrogram.types import InputMediaPhoto, InputMediaVideo
from concurrent.futures import ThreadPoolExecutor
import yt_dlp

# --- CONFIGURATION ---
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

app = Client("tobo_pro_session", api_id=API_ID, api_hash=API_HASH)
DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)
session = requests.Session()

def get_human_size(num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0: return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

def get_video_meta(video_path):
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8')
        data = json.loads(res)
        duration = int(float(data['format']['duration']))
        width = 0
        height = 0
        for stream in data['streams']:
            if stream['codec_type'] == 'video':
                width = int(stream['width'])
                height = int(stream['height'])
                break
        return duration, width, height
    except: return 0, 0, 0

def get_video_thumbnail(video_path, thumb_path):
    try:
        subprocess.run(['ffmpeg', '-ss', '00:00:01', '-i', video_path, '-vframes', '1', '-q:v', '2', thumb_path, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.STNULL)
        return thumb_path if os.path.exists(thumb_path) else None
    except: return None

def scrape_erome(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = session.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        for j in soup.find_all(["div", "section"], {"id": ["related_albums", "comments", "footer"]}): j.decompose()
        v_l = list(dict.fromkeys([next((l for l in [v.get('src'), v.get('data-src')] + [st.get('src') for st in v.find_all('source')] if l and ".mp4" in l.lower()), None) for v in soup.find_all('video') if v]))
        v_l = [x if x.startswith('http') else 'https:' + x for x in v_l if x]
        p_l = list(dict.fromkeys([i.get('data-src') or i.get('src') for i in soup.select('div.img img') if "erome.com" in (i.get('data-src') or i.get('src', ''))]))
        p_l = [x if x.startswith('http') else 'https:' + x for x in p_l if x]
        return p_l, v_l
    except: return [], []

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
                with open(pp, 'rb') as pf: f.write(pf.read())
                os.remove(pp)

@app.on_message(filters.me & filters.command("dl", prefixes="."))
async def tobo_downloader(client, message):
    raw_text = message.text.split('\n')
    urls = list(dict.fromkeys([u.strip().split(' ')[-1] for u in raw_text if "http" in u]))
    if not urls: return await message.edit("❌ Provide a link!")
    
    await message.edit(f"🚀 **Tobo Pro V8.7**\nProcessing {len(urls)} album(s)...")

    for idx, url in enumerate(urls, 1):
        if "erome.com" in url:
            photos, videos = scrape_erome(url)
            album_id = url.rstrip('/').split('/')[-1]
            status_msg = await message.reply(f"📂 **Album:** `{album_id}`\n📊 Found: {len(photos)} Photos, {len(videos)} Videos\n⏳ Initializing...")

            # 1. Photos First
            if photos:
                await status_msg.edit(f"📂 **Album:** `{album_id}`\n📸 Uploading Photos...")
                for i in range(0, len(photos), 10):
                    batch = photos[i:i+10]
                    media = [InputMediaPhoto(img) for img in batch]
                    await client.send_media_group(message.chat.id, media)

            # 2. Videos (With Live Download Status)
            if videos:
                video_payload = []
                for v_idx, v_url in enumerate(videos, 1):
                    filename = v_url.split('/')[-1].split('?')[0]
                    filepath = os.path.join(DOWNLOAD_DIR, filename)
                    thumb_path = f"{filepath}.jpg"
                    
                    # Update status for user
                    await status_msg.edit(f"📂 **Album:** `{album_id}`\n📥 Downloading Video `{v_idx}/{len(videos)}`...")
                    
                    try:
                        headers = {'User-Agent': 'Mozilla/5.0', 'Referer': url}
                        head = session.head(v_url, headers=headers, allow_redirects=True)
                        size_bytes = int(head.headers.get('content-length', 0))
                        p_size = get_human_size(size_bytes)
                        
                        if size_bytes > 15*1024*1024:
                            download_nitro(v_url, filepath, headers, size_bytes)
                        else:
                            with session.get(v_url, headers=headers, stream=True) as r:
                                with open(filepath, 'wb') as f:
                                    for chk in r.iter_content(chunk_size=1024*1024): f.write(chk)
                        
                        # Process Meta
                        duration, w, h = get_video_meta(filepath)
                        thumb = get_video_thumbnail(filepath, thumb_path)
                        
                        video_payload.append(InputMediaVideo(
                            filepath, thumb=thumb, width=w, height=h, 
                            duration=duration, supports_streaming=True,
                            caption=f"🎬 Size: {p_size}"
                        ))

                        # Send batch of 10 or the last few
                        if len(video_payload) == 10 or v_idx == len(videos):
                            await status_msg.edit(f"📂 **Album:** `{album_id}`\n📤 Uploading Video Batch to Telegram...")
                            await client.send_media_group(message.chat.id, video_payload)
                            for vid in video_payload:
                                if os.path.exists(vid.media): os.remove(vid.media)
                                if vid.thumb and os.path.exists(vid.thumb): os.remove(vid.thumb)
                            video_payload = []
                    except Exception as e:
                        await message.reply(f"⚠️ Error on Video {v_idx}: {e}")
            
            await status_msg.edit(f"✅ **COMPLETED:** `{album_id}`")

    await message.reply("🏆 **All Tasks Finished!**")

async def start_bot():
    await app.start()
    async for dialog in app.get_dialogs(): pass
    print("LOG: Tobo Pro V8.7 is online!")
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(start_bot())
