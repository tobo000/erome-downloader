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
    return f"[{'█' * int(percentage/10)}{'░' * (10 - int(percentage/10))}] {percentage:.1f}%"

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
@app.on_message(filters.command("dl", prefixes="."))
async def tobo_downloader(client, message):
    raw_text = message.text.split('\n')
    urls = list(dict.fromkeys([u.strip().split(' ')[-1] for u in raw_text if "http" in u]))
    if not urls: return
    
    # --- BLIND TOPIC DETECTION (V8.26) ---
    # We use getattr to avoid AttributeError if the property doesn't exist
    topic_id = getattr(message, "message_thread_id", None)
    
    temp_status_msgs = []
    
    for idx, url in enumerate(urls, 1):
        if "erome.com" in url:
            photos, videos = scrape_erome(url)
            album_id = url.rstrip('/').split('/')[-1]
            
            # Use app.send_message for maximum stability
            status_msg = await client.send_message(
                chat_id=message.chat.id,
                text=f"🔍 Analyzing: `{album_id}`",
                reply_to_message_id=message.id,
                message_thread_id=topic_id
            )
            temp_status_msgs.append(status_msg)
            last_edit = [0]

            if photos:
                for i in range(0, len(photos), 10):
                    batch = photos[i:i+10]
                    await client.send_media_group(
                        chat_id=message.chat.id,
                        media=[InputMediaPhoto(img) for img in batch],
                        message_thread_id=topic_id
                    )

            if videos:
                video_files = []
                for v_idx, v_url in enumerate(videos, 1):
                    filename = v_url.split('/')[-1].split('?')[0]
                    filepath = os.path.join(DOWNLOAD_DIR, filename)
                    
                    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': url}
                    with session.get(v_url, headers=headers, stream=True) as r:
                        t_s = int(r.headers.get('content-length', 0))
                        d_s = 0
                        with open(filepath, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=1024*1024):
                                if chunk:
                                    f.write(chunk); d_s += len(chunk)
                                    await edit_status(status_msg, f"📥 Video {v_idx}/{len(videos)}\n{create_progress_bar(d_s, t_s)}\n{album_id}", last_edit)
                        
                        optimize_video(filepath)
                        dur, w, h = get_video_meta(filepath)
                        thumb = get_video_thumbnail(filepath, f"{filepath}.jpg")
                        video_files.append({"path": filepath, "thumb": thumb, "w": w, "h": h, "dur": dur, "size": t_s})

                        if len(video_files) == 10 or v_idx == len(videos):
                            media_group = [InputMediaVideo(v["path"], thumb=v["thumb"], width=v["w"], height=v["h"], duration=v["dur"], supports_streaming=True, caption=f"🎬 Size: {get_human_size(v['size'])}") for v in video_files]
                            try:
                                await client.send_media_group(message.chat.id, media_group, message_thread_id=topic_id)
                            except:
                                for v in video_files: 
                                    await client.send_video(message.chat.id, v["path"], thumb=v["thumb"], width=v["w"], height=v["h"], duration=v["dur"], supports_streaming=True, message_thread_id=topic_id)
                            
                            for v in video_files:
                                if os.path.exists(v["path"]): os.remove(v["path"])
                                if v["thumb"] and os.path.exists(v["thumb"]): os.remove(v["thumb"])
                            video_files = []

            await client.send_message(
                chat_id=message.chat.id,
                text=f"✅ COMPLETED: `{album_id}`",
                message_thread_id=topic_id
            )

    # AUTO-CLEANUP
    for msg in temp_status_msgs:
        try: await msg.delete()
        except: pass
    try: await message.delete() 
    except: pass

async def main():
    async with app:
        print("LOG: Tobo Pro V8.26 is Online!")
        await idle()

if __name__ == "__main__":
    app.run(main())
