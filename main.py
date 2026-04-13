import os
import asyncio
import requests
import time
import subprocess
import json
import re
import sqlite3
import base64
from bs4 import BeautifulSoup
from pyrogram import Client, filters, idle
from pyrogram.types import InputMediaPhoto, InputMediaVideo
from pyrogram.errors import FloodWait, RPCError
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
ADMIN_IDS = [5549600755, 7010218617]

GH_TOKEN = os.getenv("GH_TOKEN")
GH_REPO = os.getenv("GH_REPO") 
GH_FILE_PATH = "bot_archive.db"

app = Client("tobo_pro_session", api_id=int(API_ID), api_hash=API_HASH, sleep_threshold=120)
DOWNLOAD_DIR = "downloads"
DB_NAME = "bot_archive.db"

if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)

session = requests.Session()
executor = ThreadPoolExecutor(max_workers=8) 
cancel_tasks = {}

# --- 2. GITHUB SYNC ---
def backup_to_github():
    if not GH_TOKEN or not GH_REPO: return
    try:
        url = f"https://api.github.com/repos/{GH_REPO}/contents/{GH_FILE_PATH}"
        headers = {"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        res = requests.get(url, headers=headers)
        sha = res.json().get('sha') if res.status_code == 200 else None
        with open(DB_NAME, "rb") as f:
            content = base64.b64encode(f.read()).decode()
        data = {"message": f"Sync Progress: {time.ctime()}", "content": content, "branch": "main"}
        if sha: data["sha"] = sha
        requests.put(url, headers=headers, data=json.dumps(data))
    except: pass

def download_from_github():
    if not GH_TOKEN or not GH_REPO: return
    try:
        url = f"https://api.github.com/repos/{GH_REPO}/contents/{GH_FILE_PATH}"
        headers = {"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            content = base64.b64decode(res.json()['content'])
            with open(DB_NAME, "wb") as f: f.write(content)
            print("LOG: [GITHUB] Database restored.")
    except: pass

# --- 3. DATABASE LOGIC (WITH AUTO-REPAIR) ---
def init_db():
    download_from_github()
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT album_id FROM processed LIMIT 1")
    except sqlite3.OperationalError:
        cursor.execute("DROP TABLE IF EXISTS processed")
        cursor.execute("DROP TABLE IF EXISTS processed_media")
    conn.execute("CREATE TABLE IF NOT EXISTS processed (album_id TEXT PRIMARY KEY)")
    conn.execute("CREATE TABLE IF NOT EXISTS processed_media (media_id TEXT PRIMARY KEY, album_id TEXT)")
    conn.commit(); conn.close()

def is_processed(album_id):
    try:
        conn = sqlite3.connect(DB_NAME)
        res = conn.execute("SELECT 1 FROM processed WHERE album_id = ?", (album_id,)).fetchone()
        conn.close(); return res is not None
    except: return False

def is_media_processed(media_url):
    media_id = re.sub(r'\W+', '', media_url)
    try:
        conn = sqlite3.connect(DB_NAME)
        res = conn.execute("SELECT 1 FROM processed_media WHERE media_id = ?", (media_id,)).fetchone()
        conn.close(); return res is not None
    except: return False

def mark_media_processed(media_url, album_id):
    media_id = re.sub(r'\W+', '', media_url)
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("INSERT OR IGNORE INTO processed_media (media_id, album_id) VALUES (?, ?)", (media_id, album_id))
        conn.commit(); conn.close(); backup_to_github()
    except: pass

def mark_processed(album_id):
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("INSERT OR IGNORE INTO processed (album_id) VALUES (?)", (album_id,))
        conn.commit(); conn.close(); backup_to_github()
    except: pass

# --- 4. HELPERS & ANIMATIONS (3s) ---
def create_progress_bar(current, total):
    if total <= 0: return "[░░░░░░░░░░] 0%"
    pct = min(100, (current / total) * 100)
    return f"[{'█' * int(pct/10)}{'░' * (10 - int(pct/10))}] {pct:.1f}%"

def get_human_size(num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0: return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

async def safe_edit(msg, text):
    try: await msg.edit_text(text)
    except: pass

async def pyrogram_progress(current, total, status_msg, start_time, action_text, topic=""):
    now = time.time()
    if now - start_time[0] > 3: 
        anims = ["🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘"]
        anim = anims[int(now % len(anims))]
        bar = create_progress_bar(current, total)
        try:
            await status_msg.edit_text(f"{anim} **{action_text}**\nTopic: `{topic[:30]}...`\n\n{bar}\n📦 **Original Size:** {get_human_size(current)} / {get_human_size(total)}")
            start_time[0] = now
        except: pass

def get_video_meta(video_path):
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8'); data = json.loads(res)
        duration = int(float(data.get('format', {}).get('duration', 0)))
        video_stream = next((s for s in data.get('streams', []) if s['codec_type'] == 'video'), {})
        return duration, int(video_stream.get('width', 1280)), int(video_stream.get('height', 720))
    except: return 1, 1280, 720

# --- 5. NITRO DOWNLOAD ENGINE ---
def download_nitro_animated(url, path, size, status_msg, loop, action, topic, segs=4):
    chunk = size // segs; downloaded_shared = [0]; start_time = [time.time()]
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 'Referer': 'https://www.erome.com/'}
    def dl_part(s, e, n):
        pp = f"{path}.p{n}"; h = headers.copy(); h['Range'] = f'bytes={s}-{e}'
        try:
            with requests.get(url, headers=h, stream=True, timeout=60) as r:
                with open(pp, 'wb') as f:
                    for chk in r.iter_content(chunk_size=1024*1024):
                        if chk:
                            f.write(chk); downloaded_shared[0] += len(chk)
                            asyncio.run_coroutine_threadsafe(pyrogram_progress(downloaded_shared[0], size, status_msg, start_time, action, topic), loop)
        except: pass
    with ThreadPoolExecutor(max_workers=segs) as ex:
        futures = [ex.submit(dl_part, i*chunk, ((i+1)*chunk-1 if i<segs-1 else size-1), i) for i in range(segs)]
        for fut in futures: fut.result()
    with open(path, 'wb') as f:
        for i in range(segs):
            pp = f"{path}.p{i}"
            if os.path.exists(pp):
                with open(pp, 'rb') as pf: f.write(pf.read()); pf.close(); os.remove(pp)

# --- 6. CORE DELIVERY ---
def scrape_album_details(url):
    h = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        res = session.get(url, headers=h, timeout=20); soup = BeautifulSoup(res.text, 'html.parser')
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Untitled"
        p_l = [img.get('data-src') or img.get('src') for img in soup.select('div.img img')]
        p_l = ['https:' + x if x.startswith('//') else x for x in p_l if x]
        v_l = list(dict.fromkeys(re.findall(r'https?://[^\s"\'>]+.(?:mp4|gif)', res.text)))
        v_l = [v for v in v_l if "erome.com" in v]
        return title, p_l, v_l
    except: return "Error", [], []

async def process_album(client, chat_id, reply_id, url, username, current, total):
    try: await client.get_chat(chat_id)
    except: pass
    album_id = url.rstrip('/').split('/')[-1]
    if is_processed(album_id): return True
    loop = asyncio.get_running_loop()
    title, all_photos, all_videos = await loop.run_in_executor(executor, scrape_album_details, url)
    if not all_photos and not all_videos: return False
    user_folder = os.path.join(DOWNLOAD_DIR, f"{chat_id}_{album_id}"); os.makedirs(user_folder, exist_ok=True)
    status = await client.send_message(chat_id, f"📡 **[{current}/{total}] Preparing Archive: {title}**", reply_to_message_id=reply_id)

    master_cap = (f"🎬 Topic: **{title}**\n📂 Album: `{current}/{total}`\n📊 Total: `{len(all_photos)}` 🖼 | `{len(all_videos)}` 🎬\n👤 User: `{username.upper()}`\n📦 Quality: **Original Quality**")
    is_master_sent = False

    # Photos
    pending_p = [p for p in all_photos if not is_media_processed(p)]
    if pending_p:
        for i in range(0, len(pending_p), 10):
            if cancel_tasks.get(chat_id): break
            chunk = pending_p[i:i+10]; photo_group = []
            for p_url in chunk:
                p_idx = all_photos.index(p_url); p_path = os.path.join(user_folder, f"p_t_{p_idx}.jpg")
                try:
                    r = session.get(p_url, timeout=15); size_h = get_human_size(len(r.content))
                    with open(p_path, 'wb') as f: f.write(r.content)
                    cap = master_cap if not is_master_sent and p_idx == 0 else f"🖼 Photo: `{p_idx + 1}/{len(all_photos)}` | 📦 Size: `{size_h}`"
                    photo_group.append(InputMediaPhoto(p_path, caption=cap))
                    if p_idx == 0: is_master_sent = True
                except: pass
            if photo_group:
                try: 
                    await client.send_media_group(chat_id, photo_group, reply_to_message_id=reply_id)
                    for p_url in chunk: mark_media_processed(p_url, album_id)
                    await asyncio.sleep(2)
                except: pass
            for f in os.listdir(user_folder):
                if "p_t_" in f: os.remove(os.path.join(user_folder, f))

    # Videos
    for v_idx, v_url in enumerate(all_videos, 1):
        if cancel_tasks.get(chat_id) or is_media_processed(v_url): continue
        is_gif = v_url.lower().endswith(".gif"); filepath = os.path.join(user_folder, f"v_{v_idx}" + (".gif" if is_gif else ".mp4"))
        try:
            r_h = session.head(v_url); size = int(r_h.headers.get('content-length', 0))
            await loop.run_in_executor(executor, download_nitro_animated, v_url, filepath, size, status, loop, f"🎬 Downloading Video {v_idx}", title)
            final_mp4 = filepath.replace(".gif", ".mp4") if is_gif else filepath + ".fixed.mp4"
            if is_gif:
                subprocess.run(['ffmpeg', '-i', filepath, '-movflags', 'faststart', '-pix_fmt', 'yuv420p', '-vf', "scale=trunc(iw/2)*2:trunc(ih/2)*2", final_mp4, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                os.remove(filepath); filepath = final_mp4
            else:
                subprocess.run(['ffmpeg', '-i', filepath, '-vcodec', 'libx264', '-crf', '24', '-pix_fmt', 'yuv420p', '-movflags', 'faststart', final_mp4, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if os.path.exists(final_mp4): os.remove(filepath); filepath = final_mp4
            dur, w, h = get_video_meta(filepath); size_h = get_human_size(os.path.getsize(filepath)); thumb = filepath + ".jpg"
            subprocess.run(['ffmpeg', '-ss', '1', '-i', filepath, '-vframes', '1', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            v_cap = master_cap if not is_master_sent and v_idx == 1 else f"⏳ Duration: `{time.strftime('%M:%S', time.gmtime(dur))}` | 📦 Size: `{size_h}`"
            await client.send_video(chat_id=chat_id, video=filepath, thumb=thumb if os.path.exists(thumb) else None, width=w, height=h, duration=dur, supports_streaming=True, caption=v_cap, reply_to_message_id=reply_id, progress=pyrogram_progress, progress_args=(status, [time.time()], f"📤 Uploading {v_idx}", title))
            mark_media_processed(v_url, album_id); is_master_sent = True; await asyncio.sleep(2); os.remove(filepath); os.remove(thumb)
        except: pass
    mark_processed(album_id); await status.delete(); return True

# --- 7. HANDLERS ---
@app.on_message(filters.command("user", prefixes=".") & filters.user(ADMIN_IDS))
async def user_cmd(client, message):
    chat_id = message.chat.id; raw_input = message.command[1].strip(); cancel_tasks[chat_id] = False
    if "/a/" in raw_input:
        await process_album(client, chat_id, message.id, raw_input, "direct", 1, 1); return
    query = raw_input.split("erome.com/")[-1].split('/')[0]; msg = await message.reply(f"🛰 **Initializing Scanner for `{query}`...**")
    all_urls = []; total_p = 0; total_v = 0
    scan_targets = [f"https://www.erome.com/{query}", f"https://www.erome.com/search?v={query}"]
    h = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}; scan_anims = ["🔍", "🔎", "📡", "🛰"]
    for base_url in scan_targets:
        page = 1
        while page <= 15:
            if cancel_tasks.get(chat_id): break
            try:
                await safe_edit(msg, f"{scan_anims[page%4]} Scanning Page {page}...\n📦 Albums: {len(all_urls)} 📂\n🖼 Photos: {total_p} | 🎬 Videos: {total_v}\n📊 Total Media: {total_p + total_v}")
                res = session.get(f"{base_url}?page={page}" if "?" in base_url else f"{base_url}/?page={page}", headers=h, timeout=15)
                ids = re.findall(r'/a/([a-zA-Z0-9]{8})', res.text)
                if not ids: break
                for aid in list(dict.fromkeys(ids)):
                    u = f"https://www.erome.com/a/{aid}"
                    if u not in all_urls:
                        all_urls.append(u)
                        block = re.search(fr'<a [^>]*href="/a/{aid}"[^>]*>.*?</a>', res.text, re.S)
                        if block:
                            p_m = re.search(r'(\d+) photos', block.group()); v_m = re.search(r'(\d+) videos', block.group())
                            if p_m: total_p += int(p_m.group(1))
                            if v_m: total_v += int(v_m.group(1))
                if "Next" not in res.text: break
                page += 1; await asyncio.sleep(0.5)
            except: break
    if not all_urls: return await msg.edit_text(f"❌ No content found.")
    await safe_edit(msg, f"✅ **Scanner Complete!**\n📊 Total Albums: {len(all_urls)} 📂\n🖼 Photos: {total_p} | 🎬 Videos: {total_v}\n📊 Total Media: {total_p + total_v}")
    await asyncio.sleep(3); await msg.delete()
    for i, url in enumerate(all_urls, 1):
        if cancel_tasks.get(chat_id): break
        await process_album(client, chat_id, message.id, url, query, i, len(all_urls))

@app.on_message(filters.command("reset", prefixes=".") & filters.user(ADMIN_IDS))
async def reset_db(client, message):
    if os.path.exists(DB_NAME): os.remove(DB_NAME)
    init_db(); backup_to_github()
    await message.reply("🧹 **Memory & Database Cleared!**")

@app.on_message(filters.command("cancel", prefixes=".") & filters.user(ADMIN_IDS))
async def cancel_cmd(client, message):
    cancel_tasks[message.chat.id] = True; backup_to_github(); await message.reply("🛑 **Cancelled!**")

async def main():
    init_db(); 
    async with app:
        print("LOG: Final Perfect Bot Started!"); await idle()

if __name__ == "__main__":
    app.run(main())
