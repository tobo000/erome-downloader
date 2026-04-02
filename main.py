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
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

if not API_ID or not API_HASH:
    print("❌ FATAL: Check your .env file!")
    exit(1)

app = Client("tobo_pro_session", api_id=int(API_ID), api_hash=API_HASH)
DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)
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
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8')
        data = json.loads(res)
        duration = int(float(data['format']['duration']))
        v = next(s for s in data['streams'] if s['codec_type'] == 'video')
        return duration, v['width'], v['height']
    except: return 0, 0, 0

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

# ==========================================
# SCRAPER ENGINE
# ==========================================
def scrape_album_details(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    try:
        res = session.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        title_tag = soup.find("h1") or soup.find("title")
        album_title = title_tag.get_text(strip=True) if title_tag else "Untitled"
        for j in soup.find_all(["div", "section"], {"id": ["related_albums", "comments", "footer"]}): j.decompose()
        v_l = list(dict.fromkeys([next((l for l in [v.get('src'), v.get('data-src')] + [st.get('src') for st in v.find_all('source')] if l and ".mp4" in l.lower()), None) for v in soup.find_all('video') if v]))
        v_l = [x if x.startswith('http') else 'https:' + x for x in v_l if x]
        p_l = list(dict.fromkeys([i.get('data-src') or i.get('src') for i in soup.select('div.img img') if "erome.com" in (i.get('data-src') or i.get('src', ''))]))
        p_l = [x if x.startswith('http') else 'https:' + x for x in p_l if x]
        return album_title, p_l, v_l
    except: return "Error", [], []

def get_all_profile_content(username):
    headers = {'User-Agent': 'Mozilla/5.0'}
    all_links = []
    for tab in ["", "/reposts"]:
        page = 1
        while True:
            url = f"https://www.erome.com/{username}{tab}?page={page}"
            try:
                res = session.get(url, headers=headers, timeout=20)
                if res.status_code != 200: break
                soup = BeautifulSoup(res.text, 'html.parser')
                links = [a['href'] for a in soup.find_all("a", href=True) if "/a/" in a['href']]
                if not links: break
                for l in links: all_links.append(l if l.startswith('http') else 'https://www.erome.com' + l)
                if not soup.find("a", string=lambda x: x and "Next" in x): break
                page += 1
            except: break
    return list(dict.fromkeys(all_links))

# ==========================================
# DELIVERY ENGINE (V8.36: Reliable Video)
# ==========================================
async def process_album(client, message, url):
    title, photos, videos = scrape_album_details(url)
    if not photos and not videos: return
    
    album_id = url.rstrip('/').split('/')[-1]
    status = await client.send_message(message.chat.id, f"🔍 Analyzing: **{title}**", reply_to_message_id=message.id)
    last_edit = [0]

    # 1. Photos
    if photos:
        p_files = []
        for p_idx, p_url in enumerate(photos, 1):
            filepath = os.path.join(DOWNLOAD_DIR, f"img_{album_id}_{p_idx}.jpg")
            await edit_status(status, f"📸 **{title}**\nPhoto {p_idx}/{len(photos)}\n{create_progress_bar(p_idx, len(photos))}", last_edit)
            try:
                r = session.get(p_url)
                with open(filepath, 'wb') as f: f.write(r.content)
                p_files.append(filepath)
                if len(p_files) == 10 or p_idx == len(photos):
                    await client.send_media_group(message.chat.id, [InputMediaPhoto(pf, caption=f"🖼 **{title}**") for pf in p_files], reply_to_message_id=message.id)
                    for pf in p_files: 
                        if os.path.exists(pf): os.remove(pf)
                    p_files = []
            except: pass

    # 2. Videos (Reliable Payload logic)
    if videos:
        video_payload = []
        for v_idx, v_url in enumerate(videos, 1):
            filename = v_url.split('/')[-1].split('?')[0]
            filepath = os.path.join(DOWNLOAD_DIR, filename)
            headers = {'User-Agent': 'Mozilla/5.0', 'Referer': url}
            try:
                head = session.head(v_url, headers=headers, allow_redirects=True)
                size = int(head.headers.get('content-length', 0))
                await edit_status(status, f"📥 **{title}**\nVideo {v_idx}/{len(videos)}\n{get_human_size(size)}", last_edit, force=True)
                
                if size > 15*1024*1024:
                    download_nitro(v_url, filepath, headers, size)
                else:
                    r = session.get(v_url, stream=True)
                    with open(filepath, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024*1024): f.write(chunk)
                
                dur, w, h = get_video_meta(filepath)
                thumb = filepath + ".jpg"
                subprocess.run(['ffmpeg', '-ss', '00:00:01', '-i', filepath, '-vframes', '1', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.STNULL)
                
                video_payload.append({
                    "path": filepath, "thumb": thumb, "w": w, "h": h, "dur": dur,
                    "cap": f"🎬 **{title}**\n📦 {get_human_size(size)}"
                })

                if len(video_payload) == 10 or v_idx == len(videos):
                    await edit_status(status, f"📤 **{title}**\nUploading video batch...", last_edit, force=True)
                    m_group = [InputMediaVideo(v["path"], thumb=v["thumb"], width=v["w"], height=v["h"], duration=v["dur"], supports_streaming=True, caption=v["cap"]) for v in video_payload]
                    try:
                        await client.send_media_group(message.chat.id, m_group, reply_to_message_id=message.id)
                    except:
                        # Fallback One-by-One if album fails
                        for v in video_payload:
                            await client.send_video(message.chat.id, v["path"], thumb=v["thumb"], width=v["w"], height=v["h"], duration=v["dur"], supports_streaming=True, caption=v["cap"], reply_to_message_id=message.id)
                    
                    for v in video_payload:
                        if os.path.exists(v["path"]): os.remove(v["path"])
                        if os.path.exists(v["thumb"]): os.remove(v["thumb"])
                    video_payload = []
            except: pass

    await status.delete()
    await client.send_message(message.chat.id, f"✅ **COMPLETED:** `{title}`", reply_to_message_id=message.id)

# ==========================================
# COMMAND HANDLERS
# ==========================================
@app.on_message(filters.command("dl", prefixes="."))
async def dl_handler(client, message):
    urls = list(dict.fromkeys([u.strip() for u in message.text.split('\n') if "erome.com/a/" in u]))
    for url in urls: 
        await process_album(client, message, url)
    
    # NEW: Delete command message ONLY AFTER ALL albums are done
    try: await message.delete()
    except: pass

@app.on_message(filters.command("user", prefixes="."))
async def user_handler(client, message):
    if len(message.command) < 2: return
    username = message.command[1]
    
    crawl_msg = await message.reply(f"🕵️‍♂️ **Crawler:** Scanning `{username}`...")
    urls = get_all_profile_content(username)
    
    if not urls:
        return await crawl_msg.edit(f"❌ No content for `{username}`.")
    
    await crawl_msg.edit(f"🚀 Found **{len(urls)}** albums. Sequential mode active...")
    
    for url in urls:
        await process_album(client, message, url)
        await asyncio.sleep(2)
        
    await message.reply(f"🏆 Profile archive for `{username}` complete!")
    
    # NEW: Cleanup
    try:
        await crawl_msg.delete()
        await message.delete()
    except: pass

async def main():
    async with app:
        print("LOG: Tobo Pro V8.36 Ready (Reliable Video Fix)!")
        await idle()

if __name__ == "__main__":
    app.run(main())
