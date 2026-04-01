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

# --- CONFIGURATION ---
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

app = Client("tobo_pro_session", api_id=API_ID, api_hash=API_HASH)
DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)
session = requests.Session()

# --- HELPERS ---
def create_progress_bar(current, total):
    if total == 0: return "[░░░░░░░░░░] 0%"
    percentage = current * 100 / total
    finished_blocks = int(percentage / 10)
    remaining_blocks = 10 - finished_blocks
    return f"[{'█' * finished_blocks}{'░' * remaining_blocks}] {percentage:.1f}%"

def get_human_size(num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0: return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

async def edit_status(message, text, last_update_time, force=False):
    now = time.time()
    if force or (now - last_update_time[0] > 3):
        try:
            await message.edit(text)
            last_update_time[0] = now
        except: pass

def get_video_meta(video_path):
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8')
        data = json.loads(res)
        duration = int(float(data['format']['duration']))
        v = next(s for s in data['streams'] if s['codec_type'] == 'video')
        return duration, v['width'], v['height']
    except: return 0, 0, 0

def optimize_video(input_path):
    output_path = input_path + "_ready.mp4"
    try:
        subprocess.run(['ffmpeg', '-i', input_path, '-c', 'copy', '-movflags', 'faststart', output_path, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.STNULL)
        if os.path.exists(output_path): os.replace(output_path, input_path)
    except: pass

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

# ==========================================
# COMMAND HANDLER (.dl)
# ==========================================
@app.on_message(filters.me & filters.command("dl", prefixes="."))
async def tobo_downloader(client, message):
    raw_text = message.text.split('\n')
    urls = list(dict.fromkeys([u.strip().split(' ')[-1] for u in raw_text if "http" in u]))
    if not urls: return await message.edit("❌ No links found.")
    
    # Store messages to delete later
    temp_messages = [message] 
    
    await message.edit(f"🚀 **Tobo Pro V8.18**\nQueue: {len(urls)} Albums")

    for idx, url in enumerate(urls, 1):
        if "erome.com" in url:
            photos, videos = scrape_erome(url)
            album_id = url.rstrip('/').split('/')[-1]
            status_msg = await message.reply(f"📂 **Album:** `{album_id}`\n📊 Found: {len(photos)} Photos, {len(videos)} Videos")
            temp_messages.append(status_msg)
            last_edit = [0]

            # 1. PHOTOS
            if photos:
                photo_files = []
                for p_idx, p_url in enumerate(photos, 1):
                    filename = p_url.split('/')[-1].split('?')[0] or f"img_{p_idx}.jpg"
                    filepath = os.path.join(DOWNLOAD_DIR, filename)
                    bar = create_progress_bar(p_idx, len(photos))
                    await edit_status(status_msg, f"📂 **Album:** `{album_id}`\n📸 **Downloading Photo {p_idx}/{len(photos)}**\n{bar}", last_edit)
                    try:
                        r = session.get(p_url, stream=True)
                        with open(filepath, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=1024): f.write(chunk)
                        photo_files.append(filepath)
                    except: pass
                    if len(photo_files) == 10 or p_idx == len(photos):
                        await edit_status(status_msg, f"📂 **Album:** `{album_id}`\n📤 **Uploading Photo Batch...**", last_edit, force=True)
                        await client.send_media_group(message.chat.id, [InputMediaPhoto(pf) for pf in photo_files])
                        for pf in photo_files: 
                            if os.path.exists(pf): os.remove(pf)
                        photo_files = []

            # 2. VIDEOS
            if videos:
                for v_idx, v_url in enumerate(videos, 1):
                    filename = v_url.split('/')[-1].split('?')[0]
                    filepath = os.path.join(DOWNLOAD_DIR, filename)
                    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': url}
                    with session.get(v_url, headers=headers, stream=True) as r:
                        total_size = int(r.headers.get('content-length', 0))
                        dl_size = 0
                        with open(filepath, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=1024*1024):
                                if chunk:
                                    f.write(chunk)
                                    dl_size += len(chunk)
                                    bar = create_progress_bar(dl_size, total_size)
                                    await edit_status(status_msg, f"📂 **Album:** `{album_id}`\n📥 **Downloading Video {v_idx}/{len(videos)}**\n{bar}\n⚡ {get_human_size(dl_size)} / {get_human_size(total_size)}", last_edit)
                        optimize_video(filepath)
                        dur, w, h = get_video_meta(filepath)
                        thumb = get_video_thumbnail(filepath, f"{filepath}.jpg")
                        await edit_status(status_msg, f"📂 **Album:** `{album_id}`\n📤 **Uploading Video {v_idx}/{len(videos)}...**", last_edit, force=True)
                        await client.send_video(message.chat.id, filepath, thumb=thumb, width=w, height=h, duration=dur, caption=f"🎬 Size: {get_human_size(total_size)}", supports_streaming=True)
                        if os.path.exists(filepath): os.remove(filepath)
                        if thumb and os.path.exists(thumb): os.remove(thumb)
            
            await status_msg.edit(f"✅ **COMPLETED:** `{album_id}`")

    # --- AUTO CLEAN LOGIC ---
    done_msg = await message.reply("🏆 **All tasks finished! Cleaning logs in 5s...**")
    await asyncio.sleep(5)
    for msg in temp_messages:
        try:
            await msg.delete()
        except: pass
    await done_msg.delete()

async def start_bot():
    await app.start()
    print("LOG: Tobo Pro V8.18 Auto-Clean is Online!")
    await idle()

if __name__ == "__main__":
    app.run(start_bot())
