import os
import asyncio
import aiohttp
import aiofiles
import time
import subprocess
import json
import re
from bs4 import BeautifulSoup
from pyrogram import Client, filters, idle
from pyrogram.types import InputMediaPhoto, InputMediaVideo, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

app = Client("tobo_pro_session", api_id=int(API_ID), api_hash=API_HASH)
DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)

data_cache = {}

# --- HELPERS ---
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
        return duration, v['width'], v['height'], has_audio
    except: return 0, 1280, 720, False

async def async_download(url, path, referer):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Referer': referer
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=600) as r:
                if r.status == 200:
                    async with aiofiles.open(path, mode='wb') as f:
                        await f.write(await r.read())
                    return True
    except: return False
    return False

# ==========================================
# ROBUST ASYNC SCANNER (V8.58)
# ==========================================
async def full_scan_profile(username, status_msg):
    # Headers ថ្មី ដូចមនុស្សប្រើ Browser ចុងក្រោយបំផុត
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.google.com/',
        'Connection': 'keep-alive'
    }
    results = {"posts": [], "reposts": []}
    icons = ["🔍", "🔎", "📡", "⚡"]
    
    async with aiohttp.ClientSession(headers=headers) as session:
        for tab in ["", "/reposts"]:
            page = 1
            key = "posts" if tab == "" else "reposts"
            while True:
                icon = icons[page % len(icons)]
                await status_msg.edit_text(
                    f"{icon} **Scanning {username}...**\n\n"
                    f"📝 Posts: `{len(results['posts'])}` \n"
                    f"🔁 Reposts: `{len(results['reposts'])}` \n\n"
                    f"*Reading {key} - Page {page}...*"
                )
                
                url = f"https://www.erome.com/{username}{tab}?page={page}"
                try:
                    async with session.get(url, timeout=20) as res:
                        if res.status == 403:
                            await status_msg.edit_text(f"❌ **Blocked by Website (403 Forbidden).**\nTry again later or change IP.")
                            return results
                        if res.status != 200: break
                        
                        html = await res.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # កែសម្រួល selector ដើម្បីឱ្យចាប់យក Link បានកាន់តែច្បាស់
                        found_links = []
                        for a in soup.find_all("a", href=True):
                            href = a['href']
                            # យកតែ Link អាល់ប៊ុម (/a/xxxx) ដែលមិនមែនជា Link ផ្សាយពាណិជ្ជកម្ម
                            if "/a/" in href and "erome.com" not in href and not any(x in href for x in ["facebook", "twitter", "share"]):
                                full_link = 'https://www.erome.com' + href if href.startswith('/') else href
                                found_links.append(full_link)
                        
                        if not found_links: break
                        
                        added_count = 0
                        for link in found_links:
                            if link not in results[key]:
                                results[key].append(link)
                                added_count += 1
                        
                        if added_count == 0: break # End of content
                        
                        # ឆែកមើលប៊ូតុង Next
                        if not soup.find("a", string=re.compile(r"Next", re.I)): break
                        
                        page += 1
                        await asyncio.sleep(1) # បន្ថែមការសម្រាកដើម្បីកុំឱ្យគេ Block
                except Exception as e:
                    print(f"Error: {e}")
                    break
    return results

# ==========================================
# DELIVERY & HANDLERS
# ==========================================
async def scrape_album_details(url):
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/122.0.0.0'}
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url) as r:
                text = await r.text()
                soup = BeautifulSoup(text, 'html.parser')
                title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Untitled"
                p_l = [i.get('data-src') or i.get('src') for i in soup.select('div.img img') if "erome.com" in (i.get('data-src') or i.get('src', ''))]
                v_candidates = re.findall(r'https?://[^\s"\'>]+\.mp4', text)
                v_l = list(dict.fromkeys([v for v in v_candidates if "erome.com" in v]))
                v_l.sort(key=lambda x: (re.search(r'(1080|720|high)', x.lower()) is None, x))
                return title, p_l, v_l
    except: return "Error", [], []

async def process_album(client, message, url):
    title, photos, videos = await scrape_album_details(url)
    if not photos and not videos: return
    album_id = url.rstrip('/').split('/')[-1]
    status = await client.send_message(message.chat.id, f"📥 **Archiving:** `{title}`", reply_to_message_id=message.id)

    if photos:
        p_files = []
        for i, p_url in enumerate(photos, 1):
            path = os.path.join(DOWNLOAD_DIR, f"img_{album_id}_{i}.jpg")
            if await async_download(p_url, path, url):
                p_files.append(path)
                if len(p_files) == 10 or i == len(photos):
                    await client.send_media_group(message.chat.id, [InputMediaPhoto(pf, caption=f"🖼 **{title}**") for pf in p_files], reply_to_message_id=message.id)
                    for pf in p_files: os.remove(pf)
                    p_files = []

    if videos:
        for i, v_url in enumerate(videos, 1):
            path = os.path.join(DOWNLOAD_DIR, f"{album_id}_v{i}.mp4")
            if await async_download(v_url, path, url):
                dur, w, h, has_audio = get_video_meta(path)
                if not has_audio:
                    temp = path + ".fix.mp4"
                    subprocess.run(['ffmpeg', '-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100', '-i', path, '-c:v', 'copy', '-c:a', 'aac', '-shortest', temp, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    if os.path.exists(temp): os.remove(path); os.rename(temp, path)
                thumb = path + ".jpg"
                subprocess.run(['ffmpeg', '-ss', '00:00:01', '-i', path, '-vframes', '1', '-q:v', '2', thumb, '-y'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                await client.send_video(message.chat.id, video=path, thumb=thumb if os.path.exists(thumb) else None, width=w, height=h, duration=dur, caption=f"🎬 **{title}**", supports_streaming=True, reply_to_message_id=message.id)
                os.remove(path); os.remove(thumb) if os.path.exists(thumb) else None
    await status.delete()

@app.on_message(filters.command("user", prefixes="."))
async def user_cmd(client, message):
    if len(message.command) < 2: return
    input_data = message.command[1].strip()
    username = input_data.split("erome.com/")[-1].split('/')[0].split('?')[0] if "erome.com/" in input_data else input_data
    
    msg = await message.reply(f"🛰 **Scanning profile:** `{username}`...")
    results = await full_scan_profile(username, msg)
    
    if not results["posts"] and not results["reposts"]:
        return # Message already updated in full_scan_profile if error
        
    data_cache[username] = results
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📥 Download Posts ({len(results['posts'])})", callback_data=f"dl_p|{username}")],
        [InlineKeyboardButton(f"🔁 Download Reposts ({len(results['reposts'])})", callback_data=f"dl_r|{username}")]
    ])
    await msg.edit_text(f"👤 **User:** `{username}`\n📝 Posts: `{len(results['posts'])}` | 🔁 Reposts: `{len(results['reposts'])}`", reply_markup=buttons)

@app.on_callback_query(filters.regex(r"^dl_(p|r)\|"))
async def handle_dl(client, callback: CallbackQuery):
    action, username = callback.data.split("|")
    key = "posts" if action == "dl_p" else "reposts"
    urls = data_cache.get(username, {}).get(key, [])
    if not urls: return await callback.answer("❌ Empty.", show_alert=True)
    await callback.message.edit_text(f"🚀 Archiving `{username}` {key}...")
    for url in urls:
        await process_album(client, callback.message, url)
        await asyncio.sleep(1)
    await callback.message.reply(f"🏆 Archive complete for {username}!")

async def main():
    async with app:
        print("LOG: V8.58 Online!")
        await idle()

if __name__ == "__main__":
    app.run(main())
