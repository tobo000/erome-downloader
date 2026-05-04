import os
import asyncio
import requests
import time
import subprocess
import json
import re
import sqlite3
import base64
import hashlib
import pickle
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Optional
from bs4 import BeautifulSoup
from pyrogram import Client, filters, idle
from pyrogram.types import (
    InputMediaPhoto, InputMediaVideo,
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery
)
from pyrogram.errors import FloodWait, RPCError
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# ============================================
# CONFIGURATION
# ============================================
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
ADMIN_IDS = [5549600755, 7010218617]
GH_TOKEN = os.getenv("GH_TOKEN")
GH_REPO = os.getenv("GH_REPO")
GH_FILE_PATH = "bot_archive.db"

if not API_ID or not API_HASH:
    raise ValueError("Missing API_ID or API_HASH in .env file")
try:
    API_ID = int(API_ID)
except ValueError:
    raise ValueError("API_ID must be an integer")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler('bot_full.log'), logging.FileHandler('bot_errors.log')]
)
logger = logging.getLogger(__name__)
for handler in logger.handlers:
    if handler.baseFilename and 'errors' in handler.baseFilename:
        handler.setLevel(logging.ERROR)

app = Client("tobo_pro_session", api_id=API_ID, api_hash=API_HASH, sleep_threshold=600, max_concurrent_transmissions=1, workers=4)

DOWNLOAD_DIR = "downloads"
DB_NAME = "bot_archive.db"
CACHE_DIR = "cache"
CHECKPOINT_DIR = "checkpoints"

for directory in [DOWNLOAD_DIR, CACHE_DIR, CHECKPOINT_DIR]:
    if not os.path.exists(directory): os.makedirs(directory)

session = requests.Session()
executor = ThreadPoolExecutor(max_workers=8)
cancel_tasks = {}
chat_locks = {}
scanned_urls = {}

# ============================================
# RATE LIMIT OPTIMIZER
# ============================================
class RateLimitOptimizer:
    def __init__(self):
        self.flood_history = []; self.current_delay = 15; self.min_delay = 5; self.max_delay = 60
        self.consecutive_success = 0; self.consecutive_floods = 0; self.total_uploads = 0; self.total_floods = 0

    def record_success(self, file_size: int):
        self.total_uploads += 1; self.consecutive_success += 1; self.consecutive_floods = 0
        if self.consecutive_success >= 5: self._decrease_delay(); self.consecutive_success = 0

    def record_flood(self, wait_time: int, file_size: int):
        self.total_uploads += 1; self.total_floods += 1; self.consecutive_floods += 1; self.consecutive_success = 0
        self.flood_history.append({'timestamp': time.time(), 'wait_time': wait_time, 'file_size': file_size})
        if len(self.flood_history) > 100: self.flood_history = self.flood_history[-100:]
        if self.consecutive_floods >= 2: self._increase_delay(); self.consecutive_floods = 0

    def _increase_delay(self):
        old = self.current_delay; self.current_delay = min(self.max_delay, self.current_delay + 10)
        if self.current_delay != old: print(f"⚡ Rate: Increased delay {old}s → {self.current_delay}s")

    def _decrease_delay(self):
        if self.total_uploads > 10 and (self.total_floods / max(self.total_uploads, 1)) < 0.05:
            old = self.current_delay; self.current_delay = max(self.min_delay, self.current_delay - 3)
            if self.current_delay != old: print(f"⚡ Rate: Decreased delay {old}s → {self.current_delay}s")

    def get_upload_delay(self, file_size: int = 0) -> int:
        mb = file_size / (1024 * 1024) if file_size > 0 else 0
        if mb > 500: return self.current_delay + 15
        if mb > 200: return self.current_delay + 8
        if mb > 100: return self.current_delay + 3
        return self.current_delay

    def get_flood_buffer(self, wait_time: int) -> int:
        buffer = wait_time + 5
        recent = len([f for f in self.flood_history if time.time() - f['timestamp'] < 300])
        if recent > 5: buffer += 25
        elif recent > 3: buffer += 15
        elif recent > 1: buffer += 10
        return buffer

    def get_dashboard_text(self) -> str:
        fr = self.total_floods / max(self.total_uploads, 1)
        return f"⚡ **Rate Optimizer**\nDelay: `{self.current_delay}s` | Uploads: `{self.total_uploads}` | Floods: `{self.total_floods}` ({fr:.1%})\n"

rate_optimizer = RateLimitOptimizer()

# ============================================
# ERROR NOTIFIER
# ============================================
class ErrorNotifier:
    def __init__(self): self.error_count = 0
    def notify(self, error_type, message, details=""):
        self.error_count += 1
        print(f"\n{'='*60}\n⚠️  ERROR #{self.error_count} | {time.strftime('%H:%M:%S')}\n{'='*60}\nType: {error_type}\nMessage: {message[:200]}\n{'='*60}\n")
    def album_report(self, album_id, title, photos, videos, dp, up, dv, uv, mp, mv, success, github):
        status = "✅ COMPLETE" if (success and mp==0 and mv==0 and github) else "❌ ISSUES"
        print(f"\n{'='*60}\n📊 ALBUM REPORT | {status}\n{'='*60}\nAlbum: {album_id}\nTitle: {title[:50]}\nPhotos: {dp}/{photos} | Videos: {dv}/{videos}\nMissing: {mp}p, {mv}v\nGitHub: {'✅' if github else '❌'}\n{'='*60}\n")

error_notifier = ErrorNotifier()

# ============================================
# MEDIA TRACKER
# ============================================
class MediaTracker:
    def __init__(self): self.pending_albums = {}
    def register_album(self, aid, title, photos, videos):
        self.pending_albums[aid] = {'title': title, 'photos': photos, 'videos': videos, 'downloaded': {'photos': [], 'videos': []}, 'uploaded': {'photos': [], 'videos': []}}
    def mark_downloaded(self, aid, mt, url):
        if aid in self.pending_albums: self.pending_albums[aid]['downloaded'][mt].append(url)
    def mark_uploaded(self, aid, mt, url):
        if aid in self.pending_albums: self.pending_albums[aid]['uploaded'][mt].append(url)
    def get_missing_media(self, aid):
        if aid not in self.pending_albums: return {'photos': [], 'videos': []}
        a = self.pending_albums[aid]; m = {'photos': [], 'videos': []}
        for mt in ['photos', 'videos']: m[mt] = [u for u in a['downloaded'][mt] if u not in a['uploaded'][mt]]
        return m
    def cleanup_album(self, aid):
        if aid in self.pending_albums: del self.pending_albums[aid]

media_tracker = MediaTracker()

def get_chat_lock(chat_id):
    if chat_id not in chat_locks: chat_locks[chat_id] = asyncio.Lock()
    return chat_locks[chat_id]

# ============================================
# SMART QUEUE
# ============================================
class SmartQueue:
    def __init__(self):
        self.queue = asyncio.Queue(); self.active_tasks = {}; self.completed_tasks = set(); self.failed_tasks = {}
        self.max_concurrent = 2; self.is_paused = False

    async def add_task(self, task_id, task_func, priority=0, **kwargs):
        await self.queue.put((priority, task_id, task_func, kwargs)); print(f"📥 Queue: {task_id}")

    async def process_queue(self):
        while not self.queue.empty():
            if self.is_paused: await asyncio.sleep(1); continue
            completed = [tid for tid, t in self.active_tasks.items() if t.done()]
            for tid in completed: del self.active_tasks[tid]
            while len(self.active_tasks) >= self.max_concurrent:
                completed = [tid for tid, t in self.active_tasks.items() if t.done()]
                for tid in completed: del self.active_tasks[tid]
                await asyncio.sleep(0.5)
            priority, task_id, task_func, kwargs = await self.queue.get()
            task = asyncio.create_task(task_func(**kwargs))
            self.active_tasks[task_id] = task
            task.add_done_callback(lambda t, tid=task_id: self._task_completed(tid, t))
        if self.active_tasks: await asyncio.gather(*self.active_tasks.values(), return_exceptions=True)

    def _task_completed(self, task_id, task):
        try:
            if task.result(): self.completed_tasks.add(task_id)
            else: self.failed_tasks[task_id] = "Failed"
        except Exception as e: self.failed_tasks[task_id] = str(e)

    def get_stats(self):
        return {'queue_size': self.queue.qsize(), 'active_tasks': len(self.active_tasks), 'completed': len(self.completed_tasks), 'failed': len(self.failed_tasks), 'is_paused': self.is_paused}
    def pause(self): self.is_paused = True
    def resume(self): self.is_paused = False
    def cancel_all(self):
        while not self.queue.empty():
            try: self.queue.get_nowait()
            except: pass
        for task_id, task in list(self.active_tasks.items()): task.cancel()

smart_queue = SmartQueue()

# ============================================
# CHECKPOINT MANAGER
# ============================================
class CheckpointManager:
    def __init__(self): self.checkpoint_dir = Path(CHECKPOINT_DIR)
    def save_checkpoint(self, aid, state):
        f = self.checkpoint_dir / f"{aid}.json"; state['timestamp'] = time.time()
        try: 
            with open(f, 'w') as fp: json.dump(state, fp)
        except: pass
    def load_checkpoint(self, aid):
        f = self.checkpoint_dir / f"{aid}.json"
        if f.exists():
            try:
                with open(f) as fp:
                    s = json.load(fp)
                    if time.time() - s.get('timestamp', 0) < 86400: return s
            except: pass
        return None
    def clear_checkpoint(self, aid):
        f = self.checkpoint_dir / f"{aid}.json"
        if f.exists():
            try: f.unlink()
            except: pass

checkpoint_manager = CheckpointManager()

# ============================================
# SMART CACHE
# ============================================
class SmartCache:
    def __init__(self, cache_dir=CACHE_DIR, ttl_hours=1):
        self.cache_dir = Path(cache_dir); self.cache_dir.mkdir(exist_ok=True); self.ttl = timedelta(hours=ttl_hours)
    def _key(self, data): return hashlib.md5(data.encode()).hexdigest()
    def get_cached_album(self, url):
        f = self.cache_dir / f"album_{self._key(url)}.pickle"
        if f.exists():
            try:
                with open(f, 'rb') as fp:
                    d = pickle.load(fp)
                    if datetime.now() - d['timestamp'] < self.ttl: return d['content']
            except: pass
        return None
    def cache_album(self, url, content):
        f = self.cache_dir / f"album_{self._key(url)}.pickle"
        try:
            with open(f, 'wb') as fp: pickle.dump({'timestamp': datetime.now(), 'content': content}, fp)
        except: pass
    def clean_old_cache(self):
        count = 0
        for f in self.cache_dir.glob("*.pickle"):
            try:
                with open(f, 'rb') as fp:
                    if datetime.now() - pickle.load(fp)['timestamp'] > timedelta(hours=24): f.unlink(); count += 1
            except:
                try: f.unlink(); count += 1
                except: pass
        return count

smart_cache = SmartCache()

# ============================================
# LIVE DASHBOARD
# ============================================
class LiveDashboard:
    def __init__(self):
        self.start_time = time.time(); self.total_downloaded = 0; self.current_speed = 0; self.last_update = time.time()
    def update_speed(self, b):
        n = time.time(); d = n - self.last_update
        if d > 0: self.current_speed = b / d
        self.total_downloaded += b; self.last_update = n
    def get_dashboard_text(self, qs=None):
        e = time.strftime('%H:%M:%S', time.gmtime(time.time() - self.start_time))
        t = f"**Live Dashboard**\n━━━━━━━━━━━━━━━━━━━━\nUptime: `{e}`\nDownloaded: `{get_human_size(self.total_downloaded)}`\nSpeed: `{get_human_size(self.current_speed)}`/s\n"
        if qs:
            st = "⏸ PAUSED" if qs.get('is_paused') else "▶️ RUNNING"
            t += f"━━━━━━━━━━━━━━━━━━━━\nStatus: `{st}`\nQueue: `{qs.get('queue_size',0)}`\nActive: `{qs.get('active_tasks',0)}`\nDone: `{qs.get('completed',0)}`\nFailed: `{qs.get('failed',0)}`\n━━━━━━━━━━━━━━━━━━━━\n"
        return t

live_dashboard = LiveDashboard()

# ============================================
# SMART COMPRESSOR
# ============================================
class SmartCompressor:
    PROFILES = {'light': {'crf': 23, 'preset': 'fast'}, 'medium': {'crf': 28, 'preset': 'medium'}, 'maximum': {'crf': 32, 'preset': 'slow'}}
    def should_compress(self, fp): return os.path.exists(fp) and os.path.getsize(fp) / (1024*1024) > 100
    def auto_select_profile(self, fp):
        mb = os.path.getsize(fp) / (1024*1024)
        if mb > 500: return self.PROFILES['maximum']
        if mb > 200: return self.PROFILES['medium']
        return self.PROFILES['light']
    def compress_video(self, inp, profile=None):
        if not self.should_compress(inp): return None
        if profile is None: profile = self.auto_select_profile(inp)
        out = f"{inp}.compressed.mp4"
        try:
            osize = os.path.getsize(inp)
            subprocess.run(['ffmpeg', '-i', inp, '-c:v', 'libx264', '-crf', str(profile['crf']), '-preset', profile['preset'], '-c:a', 'aac', '-b:a', '128k', '-movflags', 'faststart', '-y', out], stderr=subprocess.DEVNULL, timeout=300)
            if os.path.exists(out): print(f"📦 Compressed: {(1 - os.path.getsize(out)/osize)*100:.1f}%"); os.remove(inp); os.rename(out, inp); return inp
        except: pass
        return None
    def convert_gif_to_mp4(self, gif):
        mp4 = gif.replace('.gif', '.mp4')
        try:
            subprocess.run(['ffmpeg', '-i', gif, '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-movflags', 'faststart', '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2', '-y', mp4], stderr=subprocess.DEVNULL, timeout=120)
            if os.path.exists(mp4) and os.path.getsize(mp4) > 0: os.remove(gif); return mp4
        except: pass
        return None

smart_compressor = SmartCompressor()

# ============================================
# KEYBOARD MANAGER
# ============================================
class KeyboardManager:
    @staticmethod
    def get_control_keyboard():
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("⏸ Pause", callback_data="pause_all"), InlineKeyboardButton("▶️ Resume", callback_data="resume_all"), InlineKeyboardButton("🛑 Cancel", callback_data="cancel_all")],
            [InlineKeyboardButton("📊 Dashboard", callback_data="show_dashboard"), InlineKeyboardButton("🧹 Clean Cache", callback_data="clean_cache")]
        ])
    
    @staticmethod
    def get_download_options_keyboard(total_albums: int, query: str):
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"📥 Download All ({total_albums})", callback_data=f"dlall_{query}")],
            [InlineKeyboardButton("✏️ Custom Range", callback_data=f"dlcustom_{query}")],
        ])

keyboard_manager = KeyboardManager()

# ============================================
# GITHUB SYNC
# ============================================
def backup_to_github():
    if not GH_TOKEN or not GH_REPO: return False
    try:
        url = f"https://api.github.com/repos/{GH_REPO}/contents/{GH_FILE_PATH}"
        headers = {"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        res = requests.get(url, headers=headers)
        sha = res.json().get('sha') if res.status_code == 200 else None
        with open(DB_NAME, "rb") as f: content = base64.b64encode(f.read()).decode()
        db_size = os.path.getsize(DB_NAME)
        data = {"message": f"Sync: {time.strftime('%Y-%m-%d %H:%M:%S')} | {get_human_size(db_size)}", "content": content, "branch": "main"}
        if sha: data["sha"] = sha
        put_res = requests.put(url, headers=headers, data=json.dumps(data))
        if put_res.status_code in [200, 201]: print(f"☁️  GitHub Sync: Success! ({get_human_size(db_size)})"); return True
        else: print(f"❌ GitHub Sync: Failed"); return False
    except Exception as e: print(f"❌ GitHub Sync: {e}"); return False

def download_from_github():
    if not GH_TOKEN or not GH_REPO: return
    try:
        url = f"https://api.github.com/repos/{GH_REPO}/contents/{GH_FILE_PATH}"
        headers = {"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            content = base64.b64decode(res.json()['content'])
            with open(DB_NAME, "wb") as f: f.write(content)
            print(f"✅ GitHub Restore: {get_human_size(len(content))}")
    except: pass

# ============================================
# DATABASE
# ============================================
def init_db():
    restored = download_from_github()
    conn = sqlite3.connect(DB_NAME)
    conn.execute("CREATE TABLE IF NOT EXISTS fully_processed (album_id TEXT PRIMARY KEY, title TEXT, photos_count INTEGER, videos_count INTEGER, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    conn.execute("CREATE TABLE IF NOT EXISTS error_log (id INTEGER PRIMARY KEY AUTOINCREMENT, album_id TEXT, error_type TEXT, error_message TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    conn.execute("CREATE TABLE IF NOT EXISTS failed_albums (album_id TEXT PRIMARY KEY, url TEXT, title TEXT, photos_count INTEGER, videos_count INTEGER, error_type TEXT, error_message TEXT, retry_count INTEGER DEFAULT 0, max_retries INTEGER DEFAULT 3, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    conn.commit(); conn.close()
    print("✅ Database initialized")
    if not restored: print("☁️  GitHub: Uploading fresh database..."); backup_to_github()

def is_fully_processed(album_id):
    conn = sqlite3.connect(DB_NAME)
    res = conn.execute("SELECT 1 FROM fully_processed WHERE album_id = ?", (album_id,)).fetchone()
    conn.close(); return res is not None

def mark_fully_processed(album_id, title, pc, vc):
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("INSERT OR IGNORE INTO fully_processed (album_id, title, photos_count, videos_count) VALUES (?,?,?,?)", (album_id, title, pc, vc))
        conn.execute("DELETE FROM failed_albums WHERE album_id = ?", (album_id,))
        conn.commit(); conn.close()
        result = backup_to_github()
        print(f"   ✅ Album marked complete + GitHub: {'✅' if result else '❌'}")
        return result
    except: return False

def mark_failed(album_id, url, title, pc, vc, error_type, error_message):
    try:
        conn = sqlite3.connect(DB_NAME)
        existing = conn.execute("SELECT retry_count FROM failed_albums WHERE album_id = ?", (album_id,)).fetchone()
        if existing: conn.execute("UPDATE failed_albums SET error_type=?, error_message=?, retry_count=retry_count+1, timestamp=CURRENT_TIMESTAMP WHERE album_id=?", (error_type, error_message, album_id))
        else: conn.execute("INSERT INTO failed_albums (album_id, url, title, photos_count, videos_count, error_type, error_message) VALUES (?,?,?,?,?,?,?)", (album_id, url, title, pc, vc, error_type, error_message))
        conn.commit(); conn.close(); backup_to_github()
        print(f"   ⚠️  Album marked failed + GitHub synced")
    except: pass

def get_failed_albums():
    conn = sqlite3.connect(DB_NAME)
    rows = conn.execute("SELECT album_id, url, title, photos_count, videos_count, error_type, retry_count, max_retries FROM failed_albums WHERE retry_count < max_retries ORDER BY retry_count ASC").fetchall()
    conn.close()
    return [{'album_id': r[0], 'url': r[1], 'title': r[2], 'photos_count': r[3], 'videos_count': r[4], 'error_type': r[5], 'retry_count': r[6], 'max_retries': r[7]} for r in rows]

def log_error_to_db(album_id, error_type, error_message):
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("INSERT INTO error_log (album_id, error_type, error_message) VALUES (?,?,?)", (album_id, error_type, error_message))
        conn.commit(); conn.close()
    except: pass

# ============================================
# HELPERS
# ============================================
def create_progress_bar(current, total):
    if total <= 0: return "[__________] 0%"
    pct = min(100, (current/total)*100)
    return f"[{'█'*int(pct/10)}{'░'*(10-int(pct/10))}] {pct:.1f}%"

def get_human_size(num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0: return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

async def safe_edit(msg, text):
    try: await msg.edit_text(text)
    except: pass

def get_video_meta(video_path):
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8'); data = json.loads(res)
        duration = int(float(data.get('format', {}).get('duration', 0)))
        video_stream = next((s for s in data.get('streams', []) if s['codec_type'] == 'video'), {})
        return duration, int(video_stream.get('width', 1280)), int(video_stream.get('height', 720))
    except: return 1, 1280, 720

def is_gif_url(url): return '.gif' in url.lower()

# ============================================
# PLATFORM DETECTION
# ============================================
def detect_platform(url: str) -> str:
    url_lower = url.lower()
    if 'erome.com' in url_lower: return 'erome'
    if 'mega.nz' in url_lower or 'mega.co.nz' in url_lower: return 'mega'
    return 'unknown'

# ============================================
# NITRO DOWNLOAD (Erome)
# ============================================
def download_nitro_animated(url, path, size, status_msg, loop, action, topic, segs=4):
    chunk = size // segs; downloaded_shared = [0]; start_time = [time.time()]
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.erome.com/'}
    def dl_part(s, e, n):
        pp = f"{path}.p{n}"; h = headers.copy(); h['Range'] = f'bytes={s}-{e}'
        try:
            with requests.get(url, headers=h, stream=True, timeout=60) as r:
                with open(pp, 'wb') as f:
                    for chk in r.iter_content(chunk_size=1024*1024):
                        if chk: f.write(chk); downloaded_shared[0] += len(chk); live_dashboard.update_speed(len(chk))
        except: pass
    with ThreadPoolExecutor(max_workers=segs) as ex:
        futures = [ex.submit(dl_part, i*chunk, ((i+1)*chunk-1 if i<segs-1 else size-1), i) for i in range(segs)]
        for future in futures: future.result()
    with open(path, 'wb') as f:
        for i in range(segs):
            pp = f"{path}.p{i}"
            if os.path.exists(pp):
                with open(pp, 'rb') as pf: f.write(pf.read())
                os.remove(pp)

def download_simple_file(url, path):
    try:
        r = session.get(url, headers={'Referer': 'https://www.erome.com/'}, timeout=30)
        with open(path, 'wb') as f: f.write(r.content)
        return True
    except: return False

# ============================================
# MEGA DOWNLOAD (megatools)
# ============================================
def download_mega_file(url: str, path: str) -> bool:
    try:
        print(f"   📥 Mega Download: {os.path.basename(path)}")
        download_dir = os.path.dirname(path)
        result = subprocess.run(['megadl', '--path', download_dir, url], capture_output=True, text=True, timeout=600)
        if result.returncode == 0:
            for f in os.listdir(download_dir):
                f_path = os.path.join(download_dir, f)
                if os.path.isfile(f_path) and f != os.path.basename(path):
                    if os.path.getsize(f_path) > 0: os.rename(f_path, path); break
            if os.path.exists(path) and os.path.getsize(path) > 0:
                print(f"   ✅ Mega download: {get_human_size(os.path.getsize(path))}"); return True
        return False
    except Exception as e: print(f"   ❌ Mega error: {e}"); return False

# ============================================
# MEGA SCRAPER
# ============================================
def scrape_mega(url: str) -> Tuple[str, List[str], List[str]]:
    cached = smart_cache.get_cached_album(url)
    if cached: return cached
    try:
        result = subprocess.run(['megals', '--export', url], capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            lines = result.stdout.strip().split('\n')
            title = "Mega Folder"; photos, videos = [], []
            for line in lines:
                parts = line.strip().split(' ', 1)
                if len(parts) == 2:
                    file_url, file_name = parts
                    if any(file_name.lower().endswith(e) for e in ['.mp4','.webm','.mov','.mkv','.avi']): videos.append(file_url)
                    elif any(file_name.lower().endswith(e) for e in ['.jpg','.jpeg','.png','.gif','.webp','.bmp']): photos.append(file_url)
            if not photos and not videos: videos = [url]
        else:
            title = "Mega Download"; photos, videos = [], [url]
        res = (title, photos, videos); smart_cache.cache_album(url, res); return res
    except:
        title = "Mega Download"; photos, videos = [], [url]
        res = (title, photos, videos); smart_cache.cache_album(url, res); return res

# ============================================
# EROME SCRAPER
# ============================================
def scrape_erome(url: str) -> Tuple[str, List[str], List[str]]:
    cached = smart_cache.get_cached_album(url)
    if cached: return cached
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.erome.com/'}
    try:
        res = session.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Untitled"
        all_media = []
        for img in soup.select('div.img img'):
            src = img.get('data-src') or img.get('src')
            if src:
                if src.startswith('//'): src = 'https:' + src
                all_media.append(src)
        gifs = [x for x in all_media if '.gif' in x.lower()]
        photos = [x for x in all_media if '.gif' not in x.lower()]
        v_l = []
        for v_tag in soup.find_all(['source', 'video']):
            v_src = v_tag.get('src') or v_tag.get('data-src')
            if v_src and ".mp4" in v_src:
                if v_src.startswith('//'): v_src = 'https:' + v_src
                v_l.append(v_src)
        v_l.extend(re.findall(r'https?://[^\s"\'>]+.mp4', res.text))
        v_l = list(dict.fromkeys([v for v in v_l if "erome.com" in v]))
        result = (title, list(dict.fromkeys(photos)), list(dict.fromkeys(gifs + v_l)))
        smart_cache.cache_album(url, result)
        return result
    except: return "Error", [], []

# ============================================
# MAIN SCRAPER
# ============================================
def scrape_album_details(url: str) -> Tuple[str, List[str], List[str]]:
    cached = smart_cache.get_cached_album(url)
    if cached: return cached
    platform = detect_platform(url)
    if platform == 'erome': return scrape_erome(url)
    if platform == 'mega': return scrape_mega(url)
    return "Error", [], []

# ============================================
# CORE DELIVERY
# ============================================
async def process_album(client, chat_id, reply_id, url, username, current, total):
    album_id = url.rstrip('/').split('/')[-1]
    if is_fully_processed(album_id): print(f"⏭️  Skipping: {album_id}"); return True

    platform = detect_platform(url)
    title, photos, videos = await asyncio.get_event_loop().run_in_executor(executor, scrape_album_details, url)
    if not photos and not videos: return False

    media_tracker.register_album(album_id, title, photos, videos)
    print(f"\n📊 [{current}/{total}] [{platform.upper()}] Downloading: {title[:50]}")
    print(f"   Photos: {len(photos)} | Videos: {len(videos)}")

    user_folder = os.path.join(DOWNLOAD_DIR, f"{chat_id}_{album_id}")
    os.makedirs(user_folder, exist_ok=True)
    loop = asyncio.get_event_loop()

    downloaded_photos, downloaded_videos = [], []

    # Download Photos
    if photos:
        for i, p_url in enumerate(photos, 1):
            if cancel_tasks.get(chat_id): break
            path = os.path.join(user_folder, f"p_{i}.jpg")
            try:
                if platform == 'mega':
                    if not await loop.run_in_executor(executor, download_mega_file, p_url, path): continue
                else:
                    def dl_p():
                        r = session.get(p_url, headers={'Referer': 'https://www.erome.com/'}, timeout=15)
                        with open(path, 'wb') as f: f.write(r.content)
                    await loop.run_in_executor(executor, dl_p)
                media_tracker.mark_downloaded(album_id, 'photos', p_url)
                live_dashboard.update_speed(os.path.getsize(path))
                downloaded_photos.append((path, f"Photo `{i}/{len(photos)}` | `{get_human_size(os.path.getsize(path))}`", p_url))
            except Exception as e: error_notifier.notify("Photo Download", str(e), album_id); log_error_to_db(album_id, "photo_download", str(e))

    # Download Videos
    if videos:
        for v_idx, v_url in enumerate(videos, 1):
            if cancel_tasks.get(chat_id): break
            is_gif = is_gif_url(v_url)
            filepath = os.path.join(user_folder, f"v_{v_idx}.{'gif' if is_gif else 'mp4'}")
            try:
                if platform == 'mega':
                    if not await loop.run_in_executor(executor, download_mega_file, v_url, filepath): continue
                elif is_gif:
                    if not await loop.run_in_executor(executor, download_simple_file, v_url, filepath): continue
                    converted = smart_compressor.convert_gif_to_mp4(filepath)
                    if converted: filepath = converted; is_gif = False
                    else: continue
                else:
                    r_head = session.head(v_url, headers={'Referer': 'https://www.erome.com/'})
                    size = int(r_head.headers.get('content-length', 0))
                    if size == 0: continue
                    await loop.run_in_executor(executor, download_nitro_animated, v_url, filepath, size, None, loop, f"DL Video {v_idx}", title)
                    final_v = filepath + ".stream.mp4"
                    subprocess.run(['ffmpeg', '-i', filepath, '-c', 'copy', '-movflags', 'faststart', final_v, '-y'], stderr=subprocess.DEVNULL)
                    if os.path.exists(final_v): os.remove(filepath); os.rename(final_v, filepath)
                media_tracker.mark_downloaded(album_id, 'videos', v_url)
                smart_compressor.compress_video(filepath)
                dur, w, h = get_video_meta(filepath)
                thumb = filepath + ".jpg"
                subprocess.run(['ffmpeg', '-ss', '1', '-i', filepath, '-vframes', '1', thumb, '-y'], stderr=subprocess.DEVNULL)
                mt = "GIF" if is_gif else "Video"
                downloaded_videos.append((filepath, thumb, w, h, dur, f"{mt} `{v_idx}/{len(videos)}` | `{time.strftime('%M:%S', time.gmtime(dur))}` | `{get_human_size(os.path.getsize(filepath))}`", is_gif, v_url))
            except Exception as e: error_notifier.notify("Video Download", str(e), album_id); log_error_to_db(album_id, "video_download", str(e))

    print(f"   ✅ Downloaded: {len(downloaded_photos)}p, {len(downloaded_videos)}v")

    # Upload Phase
    chat_lock = get_chat_lock(chat_id)
    uploaded_p, uploaded_v = 0, 0
    all_uploaded = True

    async with chat_lock:
        status = await client.send_message(chat_id, f"[{current}/{total}] Uploading: {title}", reply_to_message_id=reply_id)
        gif_count = sum(1 for v in downloaded_videos if v[6])
        master_caption = f"**{title}**\n━━━━━━━━━━━━━━━━\nAlbum: `{current}/{total}`\nContent: `{len(downloaded_photos)}` Photos | `{len(downloaded_videos)}` Videos{' | ' + str(gif_count) + ' GIFs' if gif_count else ''}\nPlatform: `{platform.upper()}`\nUser: `{username.upper()}`\nQuality: Original\n━━━━━━━━━━━━━━━━"
        master_caption_sent = False

        # Upload Photos
        if downloaded_photos:
            photo_media = []
            for idx, (path, caption, p_url) in enumerate(downloaded_photos, 1):
                fc = master_caption + f"\n\n{caption}" if not master_caption_sent else caption
                if idx == 1: master_caption_sent = True
                photo_media.append(InputMediaPhoto(path, caption=fc))
            for i in range(0, len(photo_media), 10):
                for _ in range(3):
                    try: await client.send_media_group(chat_id, photo_media[i:i+10], reply_to_message_id=reply_id); await asyncio.sleep(3); break
                    except FloodWait as e:
                        wt = e.value if hasattr(e,'value') else 15; print(f"   ⏳ FloodWait photos: {wt}s"); await asyncio.sleep(wt + 5)
                        if not app.is_connected: await app.start()
                    except: await asyncio.sleep(5)
            for path, caption, p_url in downloaded_photos: media_tracker.mark_uploaded(album_id, 'photos', p_url); uploaded_p += 1
            for path, caption, p_url in downloaded_photos:
                try:
                    if os.path.exists(path): os.remove(path)
                except: pass

        # Upload Videos
        if downloaded_videos:
            for idx, (filepath, thumb, w, h, dur, caption, is_gif, v_url) in enumerate(downloaded_videos, 1):
                mt = "GIF" if is_gif else "Video"
                fc = master_caption + f"\n\n{caption}" if not master_caption_sent else caption
                if idx == 1: master_caption_sent = True
                file_size = os.path.getsize(filepath)
                upload_success = False
                pre_delay = rate_optimizer.get_upload_delay(file_size)
                for _ in range(3):
                    try:
                        if idx > 1: await asyncio.sleep(pre_delay)
                        await client.send_video(chat_id=chat_id, video=filepath, thumb=thumb if os.path.exists(thumb) else None, width=w, height=h, duration=dur, supports_streaming=True, caption=fc, reply_to_message_id=reply_id)
                        upload_success = True; media_tracker.mark_uploaded(album_id, 'videos', v_url); uploaded_v += 1
                        rate_optimizer.record_success(file_size); print(f"   ✅ {mt} {idx} uploaded"); break
                    except FloodWait as e:
                        wt = e.value if hasattr(e,'value') else 15; rate_optimizer.record_flood(wt, file_size)
                        fb = rate_optimizer.get_flood_buffer(wt); print(f"   ⏳ FloodWait: {wt}s (buffer: {fb}s)"); await asyncio.sleep(fb)
                        if not app.is_connected: await app.start()
                    except RPCError as e:
                        if "FILE_PART_X_MISSING" in str(e): await asyncio.sleep(30)
                        else: await asyncio.sleep(20)
                    except: await asyncio.sleep(20)
                if not upload_success: all_uploaded = False; error_notifier.notify("Video Upload", f"Failed: {idx}", album_id)
                try:
                    if os.path.exists(filepath): os.remove(filepath)
                    if thumb and os.path.exists(thumb): os.remove(thumb)
                except: pass

        try: await status.delete()
        except: pass

    # Verify & Mark
    missing = media_tracker.get_missing_media(album_id); mp, mv = len(missing['photos']), len(missing['videos'])
    if all_uploaded and mp == 0 and mv == 0: github_synced = mark_fully_processed(album_id, title, len(photos), len(videos)); album_success = True
    else: mark_failed(album_id, url, title, len(photos), len(videos), "upload_incomplete", f"Missing: {mp}p, {mv}v"); github_synced = False; album_success = False
    error_notifier.album_report(album_id, title, len(photos), len(videos), len(downloaded_photos), uploaded_p, len(downloaded_videos), uploaded_v, mp, mv, album_success, github_synced)
    media_tracker.cleanup_album(album_id)
    return album_success

# ============================================
# RETRY FAILED
# ============================================
async def retry_failed_albums(client, chat_id, reply_id):
    failed = get_failed_albums()
    if failed:
        print(f"\n{'='*60}\n🔄 RETRYING {len(failed)} FAILED ALBUMS\n{'='*60}")
        for album in failed:
            if is_fully_processed(album['album_id']): continue
            print(f"   🔄 {album['album_id']} ({album['retry_count']+1}/{album['max_retries']})")
            await smart_queue.add_task(f"retry_{album['album_id']}", process_album, priority=0, client=client, chat_id=chat_id, reply_id=reply_id, url=album['url'], username="retry", current=album['retry_count']+1, total=album['max_retries'])
        await smart_queue.process_queue()
        print("✅ Retry complete\n")

# ============================================
# START DOWNLOAD RANGE
# ============================================
async def start_download_range(client, chat_id, reply_id, query, all_urls):
    """Start downloading selected range of albums"""
    for i, url in enumerate(all_urls, 1):
        if cancel_tasks.get(chat_id): break
        await smart_queue.add_task(f"{query}_{i}", process_album, priority=i, client=client, chat_id=chat_id, reply_id=reply_id, url=url, username=query, current=i, total=len(all_urls))
    await smart_queue.process_queue()
    stats = smart_queue.get_stats(); failed = get_failed_albums()
    await client.send_message(chat_id, f"**✅ Done!**\n━━━━━━━━━━━━━━━━\nSuccess: `{stats['completed']}`\nFailed: `{stats['failed']}`\nRetry: `{len(failed)}`\n━━━━━━━━━━━━━━━━\n\n_.retry | .failed | .rate_")

# ============================================
# CALLBACK HANDLERS
# ============================================
@app.on_callback_query(filters.user(ADMIN_IDS))
async def handle_callbacks(client, callback_query):
    data = callback_query.data
    chat_id = callback_query.message.chat.id
    
    try:
        if data.startswith("dlall_"):
            query = data.replace("dlall_", "")
            if query in scanned_urls:
                all_urls = scanned_urls[query]
                await callback_query.answer(f"Starting download of all {len(all_urls)} albums!")
                await callback_query.message.edit_text(f"**✅ Downloading ALL {len(all_urls)} albums!**")
                await start_download_range(client, chat_id, callback_query.message.id, query, all_urls)
            else:
                await callback_query.answer("❌ Scan data expired. Please scan again.")
        
        elif data.startswith("dlcustom_"):
            query = data.replace("dlcustom_", "")
            total = len(scanned_urls.get(query, []))
            await callback_query.message.reply(
                f"**✏️ Custom Range**\n\n"
                f"Total albums: `{total}`\n\n"
                f"Send command:\n"
                f"`.user {query} START-END`\n\n"
                f"**Examples:**\n"
                f"`.user {query} 1-50`\n"
                f"`.user {query} 100-200`\n"
                f"`.user {query} 500-1000`"
            )
            await callback_query.answer("Send custom range!")
        
        elif data == "show_dashboard":
            await callback_query.message.reply(live_dashboard.get_dashboard_text(smart_queue.get_stats()) + "\n" + rate_optimizer.get_dashboard_text(), reply_markup=keyboard_manager.get_control_keyboard())
            await callback_query.answer("Dashboard!")
        elif data == "pause_all": smart_queue.pause(); await callback_query.answer("Paused!")
        elif data == "resume_all": smart_queue.resume(); await callback_query.answer("Resumed!")
        elif data == "cancel_all": smart_queue.cancel_all(); cancel_tasks[chat_id] = True; await callback_query.answer("Cancelled!")
        elif data == "clean_cache": await callback_query.answer(f"Cleaned {smart_cache.clean_old_cache()} files!")
    except Exception as e:
        print(f"Callback error: {e}")
        try: await callback_query.answer("Error!")
        except: pass

# ============================================
# COMMAND HANDLERS
# ============================================
@app.on_message(filters.command("start", prefixes=".") & filters.user(ADMIN_IDS))
async def start_cmd(client, message):
    await message.reply(
        "**🤖 Bot Started!**\n\n"
        "**Platforms:** Erome.com | Mega.nz\n\n"
        "**Commands:**\n"
        ".user `<username>` - Scan + choose range\n"
        ".user `<username> <start>-<end>` - Custom range\n"
        ".user `<username> all` - Download all\n"
        ".user `<url>` - Direct album/file\n"
        ".retry - Retry failed\n.failed - Show failed\n"
        ".rate - Rate optimizer\n.dashboard - Live status\n"
        ".errors - Errors\n.missing - Missing\n"
        ".cancel - Stop\n.stats - Statistics\n.reset - Reset DB\n\n"
        "**Examples:**\n"
        ".user Ashpaul69         → Scan + choose\n"
        ".user Ashpaul69 1-50    → Albums 1 to 50\n"
        ".user Ashpaul69 all     → All albums\n"
        ".user https://erome.com/a/abc",
        reply_markup=keyboard_manager.get_control_keyboard()
    )

@app.on_message(filters.command("retry", prefixes=".") & filters.user(ADMIN_IDS))
async def retry_cmd(client, message):
    failed = get_failed_albums()
    if failed: await message.reply(f"🔄 Retrying {len(failed)}..."); await retry_failed_albums(client, message.chat.id, message.id); await message.reply("✅ Done!")
    else: await message.reply("✅ No failed!")

@app.on_message(filters.command("failed", prefixes=".") & filters.user(ADMIN_IDS))
async def failed_cmd(client, message):
    failed = get_failed_albums()
    if failed: await message.reply("**Failed:**\n" + "\n".join([f"- `{f['album_id']}`: {f['error_type']} ({f['retry_count']}/{f['max_retries']})" for f in failed[:20]]))
    else: await message.reply("✅ No failed!")

@app.on_message(filters.command("rate", prefixes=".") & filters.user(ADMIN_IDS))
async def rate_cmd(client, message):
    if len(message.command) > 1:
        try: d = int(message.command[1]); old = rate_optimizer.current_delay; rate_optimizer.current_delay = max(rate_optimizer.min_delay, min(rate_optimizer.max_delay, d)); await message.reply(f"⚡ Delay: `{old}s` → `{rate_optimizer.current_delay}s`")
        except: await message.reply("Usage: `.rate <seconds>`")
    else: await message.reply(rate_optimizer.get_dashboard_text())

@app.on_message(filters.command("cancel", prefixes=".") & filters.user(ADMIN_IDS))
async def cancel_cmd(client, message): cancel_tasks[message.chat.id] = True; smart_queue.cancel_all(); await message.reply("**🛑 Cancelled!**")

@app.on_message(filters.command("dashboard", prefixes=".") & filters.user(ADMIN_IDS))
async def dashboard_cmd(client, message): await message.reply(live_dashboard.get_dashboard_text(smart_queue.get_stats()) + "\n" + rate_optimizer.get_dashboard_text(), reply_markup=keyboard_manager.get_control_keyboard())

@app.on_message(filters.command("errors", prefixes=".") & filters.user(ADMIN_IDS))
async def errors_cmd(client, message):
    try:
        conn = sqlite3.connect(DB_NAME); rows = conn.execute("SELECT album_id, error_type, error_message, timestamp FROM error_log ORDER BY timestamp DESC LIMIT 20").fetchall(); conn.close()
        if rows: await message.reply("**Errors:**\n" + "\n".join([f"- `{r[0]}`: {r[1]}" for r in rows]))
        else: await message.reply("✅ No errors!")
    except: pass

@app.on_message(filters.command("missing", prefixes=".") & filters.user(ADMIN_IDS))
async def missing_cmd(client, message):
    if media_tracker.pending_albums:
        t = "**Pending:**\n"
        for aid in list(media_tracker.pending_albums.keys())[:10]: m = media_tracker.get_missing_media(aid); t += f"- `{aid}`: {len(m['photos'])}p, {len(m['videos'])}v missing\n"
        await message.reply(t)
    else: await message.reply("✅ No pending!")

@app.on_message(filters.command("user", prefixes=".") & filters.user(ADMIN_IDS))
async def user_cmd(client, message):
    if len(message.command) < 2:
        await message.reply("**Usage:** `.user <username or URL>`\n\n**Examples:**\n.user Ashpaul69\n.user Ashpaul69 1-50\n.user Ashpaul69 all")
        return

    chat_id = message.chat.id; cancel_tasks[chat_id] = False
    args = message.command[1:]
    raw_input = args[0].strip()
    range_param = args[1].strip() if len(args) > 1 else None

    # Auto-detect platform
    if not raw_input.startswith('http'): platform = 'erome'; query = raw_input
    else: platform = detect_platform(raw_input); query = raw_input.split("erome.com/")[-1].split('/')[0] if platform == 'erome' else raw_input

    print(f"   🔍 Detected: {platform.upper()} | Input: {raw_input} | Range: {range_param}")

    # Direct album/file URL
    if platform in ['erome', 'mega'] and ('/a/' in raw_input or '/folder/' in raw_input or '/file/' in raw_input or '/#F!' in raw_input):
        await process_album(client, chat_id, message.id, raw_input, "direct", 1, 1); return

    # Mega.nz
    if platform == 'mega': await process_album(client, chat_id, message.id, raw_input, "mega", 1, 1); return

    # Erome - SCAN FIRST
    if platform == 'erome':
        msg = await message.reply(f"**🔍 Scanning Erome: `{query}`...**")
        all_urls = []; headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.erome.com/'}
        
        # Profile pages
        page = 1
        while True:
            if cancel_tasks.get(chat_id): break
            try:
                res = session.get(f"https://www.erome.com/{query}?page={page}", headers=headers, timeout=15)
                if res.status_code != 200: break
                ids = list(dict.fromkeys(re.findall(r'/a/([a-zA-Z0-9]{8})', res.text)))
                if not ids: break
                for aid in ids:
                    f_url = f"https://www.erome.com/a/{aid}"
                    if f_url not in all_urls: all_urls.append(f_url)
                await safe_edit(msg, f"**🔍 Scanning `{query}`**\nPage: `{page}` | Found: `{len(all_urls)}` albums")
                if "Next" not in res.text: break
                page += 1; await asyncio.sleep(0.3)
            except: break
        profile_pages = page
        
        # Search pages
        search_page = 1
        while True:
            if cancel_tasks.get(chat_id): break
            try:
                res = session.get(f"https://www.erome.com/search?v={query}&page={search_page}", headers=headers, timeout=15)
                if res.status_code != 200: break
                ids = list(dict.fromkeys(re.findall(r'/a/([a-zA-Z0-9]{8})', res.text)))
                if not ids: break
                for aid in ids:
                    f_url = f"https://www.erome.com/a/{aid}"
                    if f_url not in all_urls: all_urls.append(f_url)
                if "Next" not in res.text: break
                search_page += 1; await asyncio.sleep(0.3)
            except: break
        
        if not all_urls: return await msg.edit_text(f"**❌ No content found for `{query}`**")
        
        # Store for later use
        scanned_urls[query] = all_urls
        
        print(f"\n{'='*60}\n✅ SCAN: {query} | {len(all_urls)} albums | {profile_pages}+{search_page} pages\n{'='*60}\n")
        
        # Check for range parameter
        if range_param:
            if range_param.lower() == 'all':
                await msg.edit_text(f"**✅ Downloading ALL {len(all_urls)} albums!**")
                await start_download_range(client, chat_id, message.id, query, all_urls)
            elif '-' in range_param:
                try:
                    start, end = range_param.split('-')
                    start, end = int(start.strip()), int(end.strip())
                    start = max(1, start); end = min(len(all_urls), end)
                    if start > end: start, end = end, start
                    selected_urls = all_urls[start-1:end]
                    await msg.edit_text(f"**✅ Downloading albums {start} to {end} ({len(selected_urls)} albums)!**")
                    await start_download_range(client, chat_id, message.id, query, selected_urls)
                except:
                    await msg.edit_text(f"**❌ Invalid range. Use like: `.user {query} 1-50`**")
            else:
                await msg.edit_text(f"**❌ Invalid option. Use `.user {query} all` or `.user {query} 1-50`**")
        else:
            # Show choice buttons
            await msg.edit_text(
                f"**✅ Scan Complete!**\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"User: `{query}`\n"
                f"Albums: `{len(all_urls)}`\n"
                f"Profile Pages: `{profile_pages}`\n"
                f"Search Pages: `{search_page}`\n"
                f"━━━━━━━━━━━━━━━━\n\n"
                f"_Choose download option:_",
                reply_markup=keyboard_manager.get_download_options_keyboard(len(all_urls), query)
            )

@app.on_message(filters.command("reset", prefixes=".") & filters.user(ADMIN_IDS))
async def reset_db(client, message):
    if os.path.exists(DB_NAME): os.remove(DB_NAME)
    init_db(); backup_to_github()
    await message.reply("**🧹 Database Reset!**")

@app.on_message(filters.command("stats", prefixes=".") & filters.user(ADMIN_IDS))
async def stats_cmd(client, message):
    conn = sqlite3.connect(DB_NAME)
    processed = conn.execute("SELECT COUNT(*) FROM fully_processed").fetchone()[0]
    errors = conn.execute("SELECT COUNT(*) FROM error_log").fetchone()[0]
    failed = conn.execute("SELECT COUNT(*) FROM failed_albums WHERE retry_count < max_retries").fetchone()[0]
    conn.close()
    await message.reply(f"**📊 Statistics**\nProcessed: `{processed}`\nErrors: `{errors}`\nFailed: `{failed}`\n\n{live_dashboard.get_dashboard_text(smart_queue.get_stats())}\n{rate_optimizer.get_dashboard_text()}")

# ============================================
# MAIN
# ============================================
async def main():
    init_db()
    async with app:
        print("\n" + "=" * 60)
        print("🚀 BOT STARTED - Erome + Mega.nz")
        print("=" * 60)
        print("✅ GitHub Sync: Active")
        print("✅ Erome: .user Ashpaul69 (scan + choose)")
        print("✅ Erome: .user Ashpaul69 1-50 (range)")
        print("✅ Erome: .user Ashpaul69 all (all)")
        print("✅ Mega: .user https://mega.nz/file/abc")
        print("=" * 60 + "\n")
        await retry_failed_albums(app, ADMIN_IDS[0], 0)
        await idle()

if __name__ == "__main__":
    try: app.run(main())
    except KeyboardInterrupt: print("\n👋 Bot stopped")
    except Exception as e: print(f"\n❌ Fatal: {e}")
