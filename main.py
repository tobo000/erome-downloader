import os
import asyncio
import requests
import time
from bs4 import BeautifulSoup
from pyrogram import Client, filters
from pyrogram.types import InputMediaPhoto
from concurrent.futures import ThreadPoolExecutor
import yt_dlp

# --- CONFIGURATION (Pulling from Environment Variables) ---
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

# If you want to use a bot token instead of user account, add this:
# BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client("tobo_pro_session", api_id=API_ID, api_hash=API_HASH)

DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR): 
    os.makedirs(DOWNLOAD_DIR)

session = requests.Session()

def scrape_erome(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = session.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        for j in soup.find_all(["div", "section"], {"id": ["related_albums", "comments", "footer"]}): 
            j.decompose()
        v_l = list(dict.fromkeys([next((l for l in [v.get('src'), v.get('data-src')] + [st.get('src') for st in v.find_all('source')] if l and ".mp4" in l.lower()), None) for v in soup.find_all('video') if v]))
        v_l = [x if x.startswith('http') else 'https:' + x for x in v_l if x]
        p_l = list(dict.fromkeys([i.get('data-src') or i.get('src') for i in soup.select('div.img img') if "erome.com" in (i.get('data-src') or i.get('src', ''))]))
        p_l = [x if x.startswith('http') else 'https:' + x for x in p_l if x]
        return p_l, v_l
    except: 
        return [], []

def download_nitro(url, path, headers, size, segs=4):
    chunk = size // segs
    def dl_part(s, e, n):
        pp = f"{path}.p{n}"; h = headers.copy(); h['Range'] = f'bytes={s}-{e}'
        with session.get(url, headers=h, stream=True) as r:
            with open(pp, 'wb') as f:
                for chk in r.iter_content(chunk_size=1024*1024): f.write(chk)
    with ThreadPoolExecutor(max_workers=segs) as ex:
        for i in range(segs): ex.submit(dl_part, i*chunk, (i+1)*chunk-1 if i < segs-1 else size-1, i)
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
    if not urls:
        await message.edit("❌ Provide a link!")
        return
    await message.edit(f"🚀 **Tobo Pro V8.1**\nLinks: {len(urls)}\nStatus: Processing...")
    for idx, url in enumerate(urls, 1):
        if "erome.com" in url:
            photos, videos = scrape_erome(url)
            album_id = url.rstrip('/').split('/')[-1]
            header_msg = await message.reply(f"📂 **ALBUM [{idx}/{len(urls)}]:** `{album_id}`\n📊 Found: {len(photos)} Photos, {len(videos)} Videos")
            if photos:
                for i in range(0, len(photos), 10):
                    batch = photos[i:i+10]
                    media = [InputMediaPhoto(img) for img in batch]
                    await client.send_media_group(message.chat.id, media)
            if videos:
                for v_url in videos:
                    filename = v_url.split('/')[-1].split('?')[0]
                    filepath = os.path.join(DOWNLOAD_DIR, filename)
                    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': url}
                    try:
                        head = session.head(v_url, headers=headers, allow_redirects=True)
                        size = int(head.headers.get('content-length', 0))
                        if size > 15*1024*1024:
                            download_nitro(v_url, filepath, headers, size)
                        else:
                            with session.get(v_url, headers=headers, stream=True) as r:
                                with open(filepath, 'wb') as f:
                                    for chk in r.iter_content(chunk_size=1024*1024): f.write(chk)
                        await client.send_video(message.chat.id, filepath, caption=f"✅ `{filename}`")
                        os.remove(filepath)
                    except: pass
            await header_msg.edit(f"✅ **ALBUM COMPLETED:** `{album_id}`")
        else:
            status = await message.reply(f"🌐 **Social:** `{url}`\nDownloading...")
            ydl_opts = {'outtmpl': f'{DOWNLOAD_DIR}/%(title)s.%(ext)s', 'quiet': True}
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    file_path = ydl.prepare_filename(info)
                    await client.send_video(message.chat.id, file_path, caption=f"✅ {info['title']}")
                    os.remove(file_path)
                    await status.delete()
            except Exception as e:
                await status.edit(f"❌ Error: {str(e)}")
    await message.reply("🏆 **All Tasks Completed!**")

print("🚀 Tobo Pro Userbot is starting...")
app.run()
