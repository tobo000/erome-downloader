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
# 1. CONFIGURATION
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
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot_full.log'),
        logging.FileHandler('bot_errors.log')
    ]
)
logger = logging.getLogger(__name__)

for handler in logger.handlers:
    if handler.baseFilename and 'errors' in handler.baseFilename:
        handler.setLevel(logging.ERROR)

app = Client(
    "tobo_pro_session", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    sleep_threshold=600,
    max_concurrent_transmissions=1,
    workers=4
)

DOWNLOAD_DIR = "downloads"
DB_NAME = "bot_archive.db"
CACHE_DIR = "cache"
CHECKPOINT_DIR = "checkpoints"

for directory in [DOWNLOAD_DIR, CACHE_DIR, CHECKPOINT_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

session = requests.Session()
executor = ThreadPoolExecutor(max_workers=8)
cancel_tasks = {}
chat_locks = {}

# ============================================
# RATE LIMIT OPTIMIZER
# ============================================
class RateLimitOptimizer:
    """Learns from FloodWait patterns and auto-adjusts upload delays"""
    def __init__(self):
        self.flood_history = []
        self.current_delay = 15
        self.min_delay = 5
        self.max_delay = 60
        self.target_flood_rate = 0.05
        self.consecutive_success = 0
        self.consecutive_floods = 0
        self.total_uploads = 0
        self.total_floods = 0
        self.last_adjustment = time.time()
        self.adjustment_cooldown = 60
    
    def record_success(self, file_size: int):
        self.total_uploads += 1
        self.consecutive_success += 1
        self.consecutive_floods = 0
        if self.consecutive_success >= 5:
            self._decrease_delay()
            self.consecutive_success = 0
    
    def record_flood(self, wait_time: int, file_size: int):
        self.total_uploads += 1
        self.total_floods += 1
        self.consecutive_floods += 1
        self.consecutive_success = 0
        self.flood_history.append({'timestamp': time.time(), 'wait_time': wait_time, 'file_size': file_size})
        if len(self.flood_history) > 100:
            self.flood_history = self.flood_history[-100:]
        if self.consecutive_floods >= 2:
            self._increase_delay()
            self.consecutive_floods = 0
    
    def _increase_delay(self):
        old_delay = self.current_delay
        self.current_delay = min(self.max_delay, self.current_delay + 10)
        if self.current_delay != old_delay:
            print(f"⚡ Rate Optimizer: Increased delay {old_delay}s → {self.current_delay}s")
    
    def _decrease_delay(self):
        if time.time() - self.last_adjustment < self.adjustment_cooldown:
            return
        old_delay = self.current_delay
        if self.total_uploads > 10:
            flood_rate = self.total_floods / self.total_uploads
            if flood_rate < self.target_flood_rate:
                self.current_delay = max(self.min_delay, self.current_delay - 3)
                if self.current_delay != old_delay:
                    print(f"⚡ Rate Optimizer: Decreased delay {old_delay}s → {self.current_delay}s (flood rate: {flood_rate:.1%})")
        self.last_adjustment = time.time()
    
    def get_upload_delay(self, file_size: int = 0) -> int:
        size_mb = file_size / (1024 * 1024) if file_size > 0 else 0
        if size_mb > 500:
            return self.current_delay + 10
        elif size_mb > 200:
            return self.current_delay + 5
        elif size_mb > 100:
            return self.current_delay + 2
        return self.current_delay
    
    def get_flood_buffer(self, wait_time: int) -> int:
        buffer = wait_time
        recent_floods = len([f for f in self.flood_history if time.time() - f['timestamp'] < 300])
        if recent_floods > 5:
            buffer += 20
        elif recent_floods > 3:
            buffer += 15
        elif recent_floods > 1:
            buffer += 10
        else:
            buffer += 5
        return buffer
    
    def get_stats(self) -> Dict:
        flood_rate = self.total_floods / max(self.total_uploads, 1)
        recent_floods = len([f for f in self.flood_history if time.time() - f['timestamp'] < 300])
        return {
            'current_delay': self.current_delay,
            'total_uploads': self.total_uploads,
            'total_floods': self.total_floods,
            'flood_rate': f"{flood_rate:.1%}",
            'recent_floods': recent_floods,
            'consecutive_success': self.consecutive_success
        }
    
    def get_dashboard_text(self) -> str:
        stats = self.get_stats()
        return (
            f"⚡ **Rate Optimizer**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Current Delay: `{stats['current_delay']}s`\n"
            f"Uploads: `{stats['total_uploads']}`\n"
            f"Floods: `{stats['total_floods']}` ({stats['flood_rate']})\n"
            f"Recent Floods: `{stats['recent_floods']}`\n"
            f"Success Streak: `{stats['consecutive_success']}`\n"
        )

rate_optimizer = RateLimitOptimizer()

# ============================================
# ERROR NOTIFICATION SYSTEM
# ============================================
class ErrorNotifier:
    def __init__(self):
        self.error_count = 0
        self.warning_count = 0
    
    def notify(self, error_type: str, message: str, details: str = ""):
        self.error_count += 1
        border = "=" * 60
        error_msg = (
            f"\n{border}\n"
            f"⚠️  BOT ERROR #{self.error_count} | {time.strftime('%H:%M:%S')}\n"
            f"{border}\n"
            f"Type: {error_type}\n"
            f"Message: {message[:200]}\n"
        )
        if details:
            error_msg += f"Details: {details[:150]}\n"
        error_msg += f"{border}\n"
        print(error_msg)
        logger.error(f"[ERROR #{self.error_count}] {error_type}: {message} | {details}")
    
    def warning(self, warning_type: str, message: str):
        self.warning_count += 1
        print(f"⚠️  WARNING [{warning_type}]: {message[:150]}")
        logger.warning(f"[WARNING] {warning_type}: {message}")
    
    def success(self, message: str):
        print(f"✅ {message}")
        logger.info(f"[SUCCESS] {message}")
    
    def album_report(self, album_id: str, title: str, photos: int, videos: int, 
                     downloaded_p: int, uploaded_p: int, downloaded_v: int, uploaded_v: int,
                     missing_p: int, missing_v: int, success: bool, github_synced: bool):
        border = "=" * 60
        status = "✅ COMPLETE + GITHUB" if (success and missing_p == 0 and missing_v == 0 and github_synced) else "❌ ISSUES"
        report = (
            f"\n{border}\n"
            f"📊 ALBUM REPORT | {status}\n"
            f"{border}\n"
            f"Album ID: {album_id}\n"
            f"Title: {title[:50]}\n"
            f"Photos: {downloaded_p}/{photos} | Videos: {downloaded_v}/{videos}"
        )
        if missing_p > 0 or missing_v > 0:
            report += f"\n⚠️  MISSING: {missing_p}p, {missing_v}v"
        report += f"\nGitHub: {'✅' if github_synced else '❌'}\n{border}\n"
        print(report)
        logger.info(report)

error_notifier = ErrorNotifier()

# ============================================
# MEDIA TRACKING SYSTEM
# ============================================
class MediaTracker:
    def __init__(self):
        self.pending_albums = {}
    
    def register_album(self, album_id: str, title: str, photos: List[str], videos: List[str]):
        self.pending_albums[album_id] = {
            'title': title, 'photos': photos, 'videos': videos,
            'downloaded': {'photos': [], 'videos': []},
            'uploaded': {'photos': [], 'videos': []},
            'timestamp': time.time()
        }
    
    def mark_downloaded(self, album_id: str, media_type: str, url: str):
        if album_id in self.pending_albums:
            self.pending_albums[album_id]['downloaded'][media_type].append(url)
    
    def mark_uploaded(self, album_id: str, media_type: str, url: str):
        if album_id in self.pending_albums:
            self.pending_albums[album_id]['uploaded'][media_type].append(url)
    
    def get_missing_media(self, album_id: str) -> Dict:
        if album_id not in self.pending_albums:
            return {'photos': [], 'videos': []}
        album = self.pending_albums[album_id]
        missing = {'photos': [], 'videos': []}
        for media_type in ['photos', 'videos']:
            downloaded_urls = album['downloaded'][media_type]
            uploaded_urls = album['uploaded'][media_type]
            missing[media_type] = [url for url in downloaded_urls if url not in uploaded_urls]
        return missing
    
    def cleanup_album(self, album_id: str):
        if album_id in self.pending_albums:
            del self.pending_albums[album_id]

media_tracker = MediaTracker()

def get_chat_lock(chat_id: int) -> asyncio.Lock:
    if chat_id not in chat_locks:
        chat_locks[chat_id] = asyncio.Lock()
    return chat_locks[chat_id]

# ============================================
# SMART QUEUE SYSTEM
# ============================================
class SmartQueue:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.active_tasks = {}
        self.completed_tasks = set()
        self.failed_tasks = {}
        self.max_concurrent = 3
        self.is_paused = False
    
    async def add_task(self, task_id: str, task_func, priority: int = 0, **kwargs):
        await self.queue.put((priority, task_id, task_func, kwargs))
        print(f"📥 Queue: {task_id}")
    
    async def process_queue(self, progress_callback=None):
        while not self.queue.empty():
            if self.is_paused:
                await asyncio.sleep(1)
                continue
            completed = [tid for tid, task in self.active_tasks.items() if task.done()]
            for tid in completed:
                del self.active_tasks[tid]
            while len(self.active_tasks) >= self.max_concurrent:
                completed = [tid for tid, task in self.active_tasks.items() if task.done()]
                for tid in completed:
                    del self.active_tasks[tid]
                await asyncio.sleep(0.5)
            priority, task_id, task_func, kwargs = await self.queue.get()
            task = asyncio.create_task(task_func(**kwargs))
            self.active_tasks[task_id] = task
            task.add_done_callback(lambda t, tid=task_id: self._task_completed(tid, t))
        if self.active_tasks:
            await asyncio.gather(*self.active_tasks.values(), return_exceptions=True)
    
    def _task_completed(self, task_id: str, task: asyncio.Task):
        try:
            result = task.result()
            if result:
                self.completed_tasks.add(task_id)
            else:
                self.failed_tasks[task_id] = "Task returned False"
        except Exception as e:
            self.failed_tasks[task_id] = str(e)
    
    def get_stats(self) -> Dict:
        return {
            'queue_size': self.queue.qsize(),
            'active_tasks': len(self.active_tasks),
            'completed': len(self.completed_tasks),
            'failed': len(self.failed_tasks),
            'is_paused': self.is_paused
        }
    
    def pause(self):
        self.is_paused = True
    
    def resume(self):
        self.is_paused = False
    
    def cancel_all(self):
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
            except:
                pass
        for task_id, task in list(self.active_tasks.items()):
            task.cancel()

smart_queue = SmartQueue()

# ============================================
# CHECKPOINT MANAGER
# ============================================
class CheckpointManager:
    def __init__(self):
        self.checkpoint_dir = Path(CHECKPOINT_DIR)
    
    def save_checkpoint(self, album_id: str, state: Dict):
        checkpoint_file = self.checkpoint_dir / f"{album_id}.json"
        state['timestamp'] = time.time()
        try:
            with open(checkpoint_file, 'w') as f:
                json.dump(state, f)
        except:
            pass
    
    def load_checkpoint(self, album_id: str) -> Optional[Dict]:
        checkpoint_file = self.checkpoint_dir / f"{album_id}.json"
        if checkpoint_file.exists():
            try:
                with open(checkpoint_file, 'r') as f:
                    state = json.load(f)
                if time.time() - state.get('timestamp', 0) < 86400:
                    return state
            except:
                pass
        return None
    
    def clear_checkpoint(self, album_id: str):
        checkpoint_file = self.checkpoint_dir / f"{album_id}.json"
        if checkpoint_file.exists():
            try:
                checkpoint_file.unlink()
            except:
                pass

checkpoint_manager = CheckpointManager()

# ============================================
# SMART CACHE
# ============================================
class SmartCache:
    def __init__(self, cache_dir: str = "cache", ttl_hours: int = 1):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.ttl = timedelta(hours=ttl_hours)
    
    def _get_cache_key(self, data: str) -> str:
        return hashlib.md5(data.encode()).hexdigest()
    
    def get_cached_album(self, url: str) -> Optional[Tuple]:
        cache_file = self.cache_dir / f"album_{self._get_cache_key(url)}.pickle"
        if cache_file.exists():
            try:
                with open(cache_file, 'rb') as f:
                    data = pickle.load(f)
                    if datetime.now() - data['timestamp'] < self.ttl:
                        return data['content']
            except:
                pass
        return None
    
    def cache_album(self, url: str, content: Tuple):
        cache_file = self.cache_dir / f"album_{self._get_cache_key(url)}.pickle"
        try:
            data = {'timestamp': datetime.now(), 'content': content}
            with open(cache_file, 'wb') as f:
                pickle.dump(data, f)
        except:
            pass
    
    def clean_old_cache(self) -> int:
        count = 0
        for cache_file in self.cache_dir.glob("*.pickle"):
            try:
                with open(cache_file, 'rb') as f:
                    data = pickle.load(f)
                    if datetime.now() - data['timestamp'] > timedelta(hours=24):
                        cache_file.unlink()
                        count += 1
            except:
                try:
                    cache_file.unlink()
                    count += 1
                except:
                    pass
        return count

smart_cache = SmartCache()

# ============================================
# LIVE DASHBOARD
# ============================================
class LiveDashboard:
    def __init__(self):
        self.start_time = time.time()
        self.total_downloaded = 0
        self.current_speed = 0
        self.last_update = time.time()
    
    def update_speed(self, bytes_downloaded: int):
        now = time.time()
        time_diff = now - self.last_update
        if time_diff > 0:
            self.current_speed = bytes_downloaded / time_diff
        self.total_downloaded += bytes_downloaded
        self.last_update = now
    
    def get_dashboard_text(self, queue_stats: Dict = None) -> str:
        elapsed = time.strftime('%H:%M:%S', time.gmtime(time.time() - self.start_time))
        text = (
            f"**Live Dashboard**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Uptime: `{elapsed}`\n"
            f"Downloaded: `{get_human_size(self.total_downloaded)}`\n"
            f"Speed: `{get_human_size(self.current_speed)}`/s\n"
        )
        if queue_stats:
            status = "PAUSED" if queue_stats.get('is_paused') else "RUNNING"
            text += (
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Status: `{status}`\n"
                f"Queue: `{queue_stats.get('queue_size', 0)}`\n"
                f"Active: `{queue_stats.get('active_tasks', 0)}`\n"
                f"Done: `{queue_stats.get('completed', 0)}`\n"
                f"Failed: `{queue_stats.get('failed', 0)}`\n"
            )
        return text

live_dashboard = LiveDashboard()

# ============================================
# SMART COMPRESSOR
# ============================================
class SmartCompressor:
    COMPRESSION_PROFILES = {
        'light': {'crf': 23, 'preset': 'fast'},
        'medium': {'crf': 28, 'preset': 'medium'},
        'maximum': {'crf': 32, 'preset': 'slow'}
    }
    
    def should_compress(self, filepath: str) -> bool:
        if not os.path.exists(filepath):
            return False
        return os.path.getsize(filepath) / (1024 * 1024) > 100
    
    def auto_select_profile(self, filepath: str) -> Dict:
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        if size_mb > 500:
            return self.COMPRESSION_PROFILES['maximum']
        elif size_mb > 200:
            return self.COMPRESSION_PROFILES['medium']
        return self.COMPRESSION_PROFILES['light']
    
    def compress_video(self, input_path: str, profile: Dict = None) -> Optional[str]:
        if not self.should_compress(input_path):
            return None
        if profile is None:
            profile = self.auto_select_profile(input_path)
        output_path = f"{input_path}.compressed.mp4"
        try:
            original_size = os.path.getsize(input_path)
            subprocess.run(
                ['ffmpeg', '-i', input_path, '-c:v', 'libx264', '-crf', str(profile['crf']),
                 '-preset', profile['preset'], '-c:a', 'aac', '-b:a', '128k',
                 '-movflags', 'faststart', '-y', output_path],
                stderr=subprocess.DEVNULL, timeout=300
            )
            if os.path.exists(output_path):
                print(f"📦 Compressed: {(1 - os.path.getsize(output_path)/original_size) * 100:.1f}% reduction")
                os.remove(input_path)
                os.rename(output_path, input_path)
                return input_path
        except:
            pass
        return None
    
    def convert_gif_to_mp4(self, gif_path: str) -> Optional[str]:
        mp4_path = gif_path.replace('.gif', '.mp4')
        try:
            subprocess.run(
                ['ffmpeg', '-i', gif_path, '-c:v', 'libx264', '-pix_fmt', 'yuv420p',
                 '-movflags', 'faststart', '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
                 '-y', mp4_path],
                stderr=subprocess.DEVNULL, timeout=120
            )
            if os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 0:
                os.remove(gif_path)
                return mp4_path
        except:
            pass
        return None

smart_compressor = SmartCompressor()

# ============================================
# KEYBOARD MANAGER
# ============================================
class KeyboardManager:
    @staticmethod
    def get_control_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Pause", callback_data="pause_all"),
                InlineKeyboardButton("Resume", callback_data="resume_all"),
                InlineKeyboardButton("Cancel", callback_data="cancel_all")
            ],
            [
                InlineKeyboardButton("Dashboard", callback_data="show_dashboard"),
                InlineKeyboardButton("Clean Cache", callback_data="clean_cache")
            ]
        ])

keyboard_manager = KeyboardManager()

# ============================================
# GITHUB SYNC ENGINE
# ============================================
def backup_to_github() -> bool:
    """Sync database to GitHub"""
    if not GH_TOKEN or not GH_REPO:
        return False
    try:
        url = f"https://api.github.com/repos/{GH_REPO}/contents/{GH_FILE_PATH}"
        headers = {"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        res = requests.get(url, headers=headers)
        sha = res.json().get('sha') if res.status_code == 200 else None
        
        with open(DB_NAME, "rb") as f:
            content = base64.b64encode(f.read()).decode()
        
        db_size = os.path.getsize(DB_NAME)
        commit_message = f"Sync: {time.strftime('%Y-%m-%d %H:%M:%S')} | {get_human_size(db_size)}"
        
        data = {"message": commit_message, "content": content, "branch": "main"}
        if sha:
            data["sha"] = sha
        
        put_res = requests.put(url, headers=headers, data=json.dumps(data))
        
        if put_res.status_code in [200, 201]:
            print(f"☁️  GitHub Sync: Success! ({get_human_size(db_size)})")
            return True
        else:
            print(f"❌ GitHub Sync: Failed (Status: {put_res.status_code})")
            return False
    except Exception as e:
        print(f"❌ GitHub Sync: Error - {e}")
        return False

def download_from_github():
    """Restore database from GitHub"""
    if not GH_TOKEN or not GH_REPO:
        return
    try:
        url = f"https://api.github.com/repos/{GH_REPO}/contents/{GH_FILE_PATH}"
        headers = {"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            content = base64.b64decode(res.json()['content'])
            with open(DB_NAME, "wb") as f:
                f.write(content)
            print(f"✅ GitHub Restore: Success! ({get_human_size(len(content))})")
    except Exception as e:
        print(f"❌ GitHub Restore: Error - {e}")

# ============================================
# DATABASE LOGIC
# ============================================
def init_db():
    """Initialize database"""
    download_from_github()
    conn = sqlite3.connect(DB_NAME)
    conn.execute("CREATE TABLE IF NOT EXISTS fully_processed (album_id TEXT PRIMARY KEY, title TEXT, photos_count INTEGER, videos_count INTEGER, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    conn.execute("CREATE TABLE IF NOT EXISTS error_log (id INTEGER PRIMARY KEY AUTOINCREMENT, album_id TEXT, error_type TEXT, error_message TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    conn.execute("CREATE TABLE IF NOT EXISTS failed_albums (album_id TEXT PRIMARY KEY, url TEXT, title TEXT, photos_count INTEGER, videos_count INTEGER, error_type TEXT, error_message TEXT, retry_count INTEGER DEFAULT 0, max_retries INTEGER DEFAULT 3, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    conn.commit()
    conn.close()
    print("✅ Database initialized")

def is_fully_processed(album_id: str) -> bool:
    """Check if album was fully processed"""
    conn = sqlite3.connect(DB_NAME)
    res = conn.execute("SELECT 1 FROM fully_processed WHERE album_id = ?", (album_id,)).fetchone()
    conn.close()
    return res is not None

def mark_fully_processed(album_id: str, title: str, photos_count: int, videos_count: int) -> bool:
    """Mark album as fully processed"""
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("INSERT OR IGNORE INTO fully_processed (album_id, title, photos_count, videos_count) VALUES (?, ?, ?, ?)", 
                     (album_id, title, photos_count, videos_count))
        conn.execute("DELETE FROM failed_albums WHERE album_id = ?", (album_id,))
        conn.commit()
        conn.close()
        return backup_to_github()
    except:
        return False

def mark_failed(album_id: str, url: str, title: str, photos_count: int, videos_count: int, error_type: str, error_message: str):
    """Mark album as failed for retry"""
    try:
        conn = sqlite3.connect(DB_NAME)
        existing = conn.execute("SELECT retry_count FROM failed_albums WHERE album_id = ?", (album_id,)).fetchone()
        if existing:
            conn.execute(
                "UPDATE failed_albums SET error_type = ?, error_message = ?, retry_count = retry_count + 1, timestamp = CURRENT_TIMESTAMP WHERE album_id = ?",
                (error_type, error_message, album_id)
            )
        else:
            conn.execute(
                "INSERT INTO failed_albums (album_id, url, title, photos_count, videos_count, error_type, error_message) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (album_id, url, title, photos_count, videos_count, error_type, error_message)
            )
        conn.commit()
        conn.close()
        backup_to_github()
    except:
        pass

def get_failed_albums() -> List[Dict]:
    """Get albums that need retry"""
    conn = sqlite3.connect(DB_NAME)
    rows = conn.execute(
        "SELECT album_id, url, title, photos_count, videos_count, error_type, retry_count, max_retries FROM failed_albums WHERE retry_count < max_retries ORDER BY retry_count ASC"
    ).fetchall()
    conn.close()
    return [
        {
            'album_id': row[0], 'url': row[1], 'title': row[2],
            'photos_count': row[3], 'videos_count': row[4],
            'error_type': row[5], 'retry_count': row[6], 'max_retries': row[7]
        }
        for row in rows
    ]

def log_error_to_db(album_id: str, error_type: str, error_message: str):
    """Log error to database"""
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("INSERT INTO error_log (album_id, error_type, error_message) VALUES (?, ?, ?)", 
                     (album_id, error_type, error_message))
        conn.commit()
        conn.close()
    except:
        pass

# ============================================
# HELPERS & MOON ANIMATIONS
# ============================================
def create_progress_bar(current: int, total: int) -> str:
    if total <= 0:
        return "[░░░░░░░░░░] 0%"
    pct = min(100, (current / total) * 100)
    return f"[{'█' * int(pct/10)}{'░' * (10 - int(pct/10))}] {pct:.1f}%"

def get_human_size(num: float) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

async def safe_edit(msg, text: str):
    try:
        await msg.edit_text(text)
    except:
        pass

async def pyrogram_progress(current: int, total: int, status_msg, start_time: List[float], action_text: str, topic: str = ""):
    now = time.time()
    if now - start_time[0] > 3:
        anims = ["🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘"]
        anim = anims[int(now % len(anims))]
        bar = create_progress_bar(current, total)
        try:
            await status_msg.edit_text(
                f"{anim} **{action_text}**\n"
                f"Topic: `{topic[:30]}...`\n\n"
                f"{bar}\n"
                f"Original Size: {get_human_size(current)} / {get_human_size(total)}"
            )
            start_time[0] = now
        except:
            pass

def get_video_meta(video_path: str) -> Tuple[int, int, int]:
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', video_path]
        res = subprocess.check_output(cmd).decode('utf-8')
        data = json.loads(res)
        duration = int(float(data.get('format', {}).get('duration', 0)))
        video_stream = next((s for s in data.get('streams', []) if s['codec_type'] == 'video'), {})
        return duration, int(video_stream.get('width', 1280)), int(video_stream.get('height', 720))
    except:
        return 1, 1280, 720

def is_gif_url(url: str) -> bool:
    return '.gif' in url.lower()

# ============================================
# NITRO DOWNLOAD ENGINE
# ============================================
def download_nitro_animated(url: str, path: str, size: int, status_msg, loop, action: str, topic: str, segs: int = 4):
    chunk = size // segs
    downloaded_shared = [0]
    start_time = [time.time()]
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.erome.com/'}
    
    def dl_part(s: int, e: int, n: int):
        pp = f"{path}.p{n}"
        h = headers.copy()
        h['Range'] = f'bytes={s}-{e}'
        try:
            with requests.get(url, headers=h, stream=True, timeout=60) as r:
                with open(pp, 'wb') as f:
                    for chk in r.iter_content(chunk_size=1024*1024):
                        if chk:
                            f.write(chk)
                            downloaded_shared[0] += len(chk)
                            live_dashboard.update_speed(len(chk))
                            if status_msg:
                                asyncio.run_coroutine_threadsafe(
                                    pyrogram_progress(downloaded_shared[0], size, status_msg, start_time, action, topic), loop
                                )
        except:
            pass

    with ThreadPoolExecutor(max_workers=segs) as ex:
        futures = []
        for i in range(segs):
            start = i * chunk
            end = (i + 1) * chunk - 1 if i < segs - 1 else size - 1
            futures.append(ex.submit(dl_part, start, end, i))
        for future in futures:
            future.result()

    with open(path, 'wb') as f:
        for i in range(segs):
            pp = f"{path}.p{i}"
            if os.path.exists(pp):
                with open(pp, 'rb') as pf:
                    f.write(pf.read())
                os.remove(pp)

def download_simple_file(url: str, path: str) -> bool:
    try:
        r = session.get(url, headers={'Referer': 'https://www.erome.com/'}, timeout=30)
        with open(path, 'wb') as f:
            f.write(r.content)
        return True
    except:
        return False

# ============================================
# SCRAPER WITH CACHE
# ============================================
def scrape_album_details(url: str) -> Tuple[str, List[str], List[str]]:
    cached = smart_cache.get_cached_album(url)
    if cached:
        return cached
    
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.erome.com/'}
    try:
        res = session.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Untitled"
        
        all_media = []
        for img in soup.select('div.img img'):
            src = img.get('data-src') or img.get('src')
            if src:
                if src.startswith('//'):
                    src = 'https:' + src
                all_media.append(src)
        
        gifs = [x for x in all_media if '.gif' in x.lower()]
        photos = [x for x in all_media if '.gif' not in x.lower()]
        
        v_l = []
        for v_tag in soup.find_all(['source', 'video']):
            v_src = v_tag.get('src') or v_tag.get('data-src')
            if v_src and ".mp4" in v_src:
                if v_src.startswith('//'):
                    v_src = 'https:' + v_src
                v_l.append(v_src)
        
        v_l.extend(re.findall(r'https?://[^\s"\'>]+.mp4', res.text))
        v_l = list(dict.fromkeys([v for v in v_l if "erome.com" in v]))
        
        result = (title, list(dict.fromkeys(photos)), list(dict.fromkeys(gifs + v_l)))
        smart_cache.cache_album(url, result)
        return result
    except:
        return "Error", [], []

# ============================================
# CORE DELIVERY SYSTEM
# ============================================
async def process_album(client, chat_id, reply_id, url, username, current, total):
    """Process album - download 3 concurrent, upload sequential with rate optimizer"""
    album_id = url.rstrip('/').split('/')[-1]
    
    # Skip if fully processed
    if is_fully_processed(album_id):
        print(f"⏭️  Skipping (fully processed): {album_id}")
        return True
    
    # Scrape
    title, photos, videos = await asyncio.get_event_loop().run_in_executor(
        executor, scrape_album_details, url
    )
    
    if not photos and not videos:
        return False
    
    # Register with tracker
    media_tracker.register_album(album_id, title, photos, videos)
    print(f"\n📊 [{current}/{total}] Downloading: {title[:50]}")
    print(f"   Photos: {len(photos)} | Videos: {len(videos)}")
    
    user_folder = os.path.join(DOWNLOAD_DIR, f"{chat_id}_{album_id}")
    os.makedirs(user_folder, exist_ok=True)
    
    loop = asyncio.get_event_loop()
    
    # ============================================
    # PHASE 1: DOWNLOAD (3 concurrent - NO LOCK)
    # ============================================
    downloaded_photos = []  # List of (path, caption, url)
    downloaded_videos = []  # List of (filepath, thumb, w, h, dur, caption, is_gif, url)
    
    # Download Photos
    if photos:
        for i, p_url in enumerate(photos, 1):
            if cancel_tasks.get(chat_id):
                break
            path = os.path.join(user_folder, f"p_{i}.jpg")
            try:
                def dl_p():
                    r = session.get(p_url, headers={'Referer': 'https://www.erome.com/'}, timeout=15)
                    with open(path, 'wb') as f:
                        f.write(r.content)
                await loop.run_in_executor(executor, dl_p)
                
                media_tracker.mark_downloaded(album_id, 'photos', p_url)
                live_dashboard.update_speed(os.path.getsize(path))
                size_h = get_human_size(os.path.getsize(path))
                caption = f"Photo `{i}/{len(photos)}` | `{size_h}`"
                downloaded_photos.append((path, caption, p_url))
            except Exception as e:
                error_notifier.notify("Photo Download", str(e), album_id)
                log_error_to_db(album_id, "photo_download", str(e))
    
    # Download Videos/GIFs
    if videos:
        for v_idx, v_url in enumerate(videos, 1):
            if cancel_tasks.get(chat_id):
                break
            
            is_gif = is_gif_url(v_url)
            filepath = os.path.join(user_folder, f"v_{v_idx}.{'gif' if is_gif else 'mp4'}")
            thumb = None
            
            try:
                if is_gif:
                    success = await loop.run_in_executor(executor, download_simple_file, v_url, filepath)
                    if not success:
                        continue
                    converted = smart_compressor.convert_gif_to_mp4(filepath)
                    if converted:
                        filepath = converted
                        is_gif = False
                    else:
                        continue
                else:
                    r_head = session.head(v_url, headers={'Referer': 'https://www.erome.com/'})
                    size = int(r_head.headers.get('content-length', 0))
                    
                    if size == 0:
                        continue
                    
                    await loop.run_in_executor(
                        executor, download_nitro_animated,
                        v_url, filepath, size, None, loop, f"Downloading Video {v_idx}", title
                    )
                    
                    # Faststart processing
                    final_v = filepath + ".stream.mp4"
                    subprocess.run(
                        ['ffmpeg', '-i', filepath, '-c', 'copy', '-movflags', 'faststart', final_v, '-y'],
                        stderr=subprocess.DEVNULL
                    )
                    if os.path.exists(final_v):
                        os.remove(filepath)
                        os.rename(final_v, filepath)
                
                media_tracker.mark_downloaded(album_id, 'videos', v_url)
                
                # Auto Compression
                smart_compressor.compress_video(filepath)
                
                # Get video metadata
                dur, w, h = get_video_meta(filepath)
                size_h = get_human_size(os.path.getsize(filepath))
                thumb = filepath + ".jpg"
                subprocess.run(
                    ['ffmpeg', '-ss', '1', '-i', filepath, '-vframes', '1', thumb, '-y'],
                    stderr=subprocess.DEVNULL
                )
                
                duration_str = time.strftime('%M:%S', time.gmtime(dur))
                media_type = "GIF" if is_gif else "Video"
                caption = f"{media_type} `{v_idx}/{len(videos)}` | `{duration_str}` | `{size_h}`"
                
                downloaded_videos.append((filepath, thumb, w, h, dur, caption, is_gif, v_url))
                
            except Exception as e:
                error_notifier.notify("Video Download", str(e), album_id)
                log_error_to_db(album_id, "video_download", str(e))
    
    print(f"   ✅ Downloaded: {len(downloaded_photos)}p, {len(downloaded_videos)}v")
    
    # ============================================
    # PHASE 2: UPLOAD (Sequential per chat - WITH LOCK + RATE OPTIMIZER)
    # ============================================
    chat_lock = get_chat_lock(chat_id)
    uploaded_p, uploaded_v = 0, 0
    all_uploaded = True
    
    async with chat_lock:
        print(f"   🔒 Upload lock acquired for {album_id}")
        
        # Send status
        status = await client.send_message(
            chat_id,
            f"[{current}/{total}] Uploading: {title}",
            reply_to_message_id=reply_id
        )
        
        # Master Caption
        gif_count = sum(1 for v in downloaded_videos if v[6])  # is_gif
        gif_info = f" | {gif_count} GIFs" if gif_count > 0 else ""
        master_caption = (
            f"**{title}**\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"Album: `{current}/{total}`\n"
            f"Content: `{len(downloaded_photos)}` Photos | `{len(downloaded_videos)}` Videos{gif_info}\n"
            f"User: `{username.upper()}`\n"
            f"Quality: Original\n"
            f"━━━━━━━━━━━━━━━━"
        )
        
        master_caption_sent = False
        
        # Upload Photos
        if downloaded_photos:
            photo_media = []
            for idx, (path, caption, p_url) in enumerate(downloaded_photos, 1):
                full_caption = master_caption + f"\n\n{caption}" if not master_caption_sent else caption
                if idx == 1:
                    master_caption_sent = True
                photo_media.append(InputMediaPhoto(path, caption=full_caption))
            
            # Send in groups of 10
            for i in range(0, len(photo_media), 10):
                chunk = photo_media[i:i+10]
                for attempt in range(3):
                    try:
                        await client.send_media_group(chat_id, chunk, reply_to_message_id=reply_id)
                        await asyncio.sleep(3)
                        break
                    except FloodWait as e:
                        wait_time = e.value if hasattr(e, 'value') else 15
                        await asyncio.sleep(wait_time + 5)
                    except Exception as e:
                        if attempt < 2:
                            await asyncio.sleep(5)
            
            # Mark uploaded and cleanup
            for path, caption, p_url in downloaded_photos:
                media_tracker.mark_uploaded(album_id, 'photos', p_url)
                uploaded_p += 1
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except:
                    pass
        
        # Upload Videos (one by one with Rate Optimizer)
        if downloaded_videos:
            for idx, (filepath, thumb, w, h, dur, caption, is_gif, v_url) in enumerate(downloaded_videos, 1):
                media_type = "GIF" if is_gif else "Video"
                full_caption = master_caption + f"\n\n{caption}" if not master_caption_sent else caption
                if idx == 1:
                    master_caption_sent = True
                
                file_size = os.path.getsize(filepath)
                upload_success = False
                
                # Get optimized delay from rate optimizer
                pre_upload_delay = rate_optimizer.get_upload_delay(file_size)
                
                for attempt in range(3):
                    try:
                        # Add pre-upload delay based on optimizer
                        if attempt > 0 or idx > 1:
                            print(f"⚡ Rate: Waiting {pre_upload_delay}s before upload ({get_human_size(file_size)})")
                            await asyncio.sleep(pre_upload_delay)
                        
                        logger.info(f"Uploading {media_type} {idx}/{len(downloaded_videos)} ({get_human_size(file_size)})")
                        
                        start_time_up = [time.time()]
                        await client.send_video(
                            chat_id=chat_id,
                            video=filepath,
                            thumb=thumb if os.path.exists(thumb) else None,
                            width=w, height=h, duration=dur,
                            supports_streaming=True,
                            caption=full_caption,
                            reply_to_message_id=reply_id,
                            progress=pyrogram_progress,
                            progress_args=(status, start_time_up, f"Uploading {media_type} {idx}/{len(downloaded_videos)}", title)
                        )
                        
                        upload_success = True
                        media_tracker.mark_uploaded(album_id, 'videos', v_url)
                        uploaded_v += 1
                        
                        # Record success in rate optimizer
                        rate_optimizer.record_success(file_size)
                        print(f"   ✅ {media_type} {idx} uploaded (delay: {pre_upload_delay}s)")
                        break
                        
                    except FloodWait as e:
                        wait_time = e.value if hasattr(e, 'value') else 15
                        
                        # Record flood and get optimized buffer
                        rate_optimizer.record_flood(wait_time, file_size)
                        flood_buffer = rate_optimizer.get_flood_buffer(wait_time)
                        
                        print(f"   ⏳ FloodWait: {wait_time}s (buffer: {flood_buffer}s, new delay: {rate_optimizer.current_delay}s)")
                        await asyncio.sleep(flood_buffer)
                        
                    except RPCError as e:
                        if "FILE_PART_X_MISSING" in str(e):
                            print(f"   ⏳ File part missing - waiting 25s")
                            await asyncio.sleep(25)
                        elif attempt < 2:
                            await asyncio.sleep(15)
                    
                    except Exception as e:
                        logger.error(f"Upload error: {e}")
                        if attempt < 2:
                            await asyncio.sleep(15)
                
                if not upload_success:
                    all_uploaded = False
                    error_notifier.notify("Video Upload", f"Failed: {idx}", album_id)
                    log_error_to_db(album_id, "video_upload", f"Failed video {idx}")
                
                # Cleanup this video
                try:
                    if os.path.exists(filepath):
                        os.remove(filepath)
                    if thumb and os.path.exists(thumb):
                        os.remove(thumb)
                except:
                    pass
        
        # Cleanup status
        try:
            await status.delete()
        except:
            pass
        
        print(f"   🔓 Upload lock released for {album_id}")
    
    # ============================================
    # PHASE 3: VERIFY & MARK
    # ============================================
    missing = media_tracker.get_missing_media(album_id)
    missing_p = len(missing['photos'])
    missing_v = len(missing['videos'])
    
    # Only mark as fully processed if ALL media uploaded successfully
    if all_uploaded and missing_p == 0 and missing_v == 0:
        github_synced = mark_fully_processed(album_id, title, len(photos), len(videos))
        album_success = True
    else:
        # Mark as failed for retry
        mark_failed(album_id, url, title, len(photos), len(videos),
                   "upload_incomplete", f"Missing: {missing_p}p, {missing_v}v")
        github_synced = False
        album_success = False
    
    error_notifier.album_report(
        album_id, title, len(photos), len(videos),
        len(downloaded_photos), uploaded_p,
        len(downloaded_videos), uploaded_v,
        missing_p, missing_v, album_success, github_synced
    )
    
    if missing_p > 0 or missing_v > 0:
        print(f"   ⚠️  MISSING: {missing_p} photos, {missing_v} videos - Marked for RETRY")
    
    media_tracker.cleanup_album(album_id)
    return album_success

# ============================================
# RETRY FAILED ALBUMS
# ============================================
async def retry_failed_albums(client, chat_id, reply_id):
    """Retry albums that previously failed"""
    failed = get_failed_albums()
    
    if failed:
        print(f"\n{'='*60}")
        print(f"🔄 RETRYING {len(failed)} FAILED ALBUMS")
        print(f"{'='*60}")
        
        for album in failed:
            # Check if already fully processed
            if is_fully_processed(album['album_id']):
                print(f"   ✅ Already processed: {album['album_id']}")
                continue
            
            print(f"   🔄 Retrying: {album['album_id']} (attempt {album['retry_count']+1}/{album['max_retries']})")
            await smart_queue.add_task(
                f"retry_{album['album_id']}",
                process_album,
                priority=0,
                client=client, chat_id=chat_id, reply_id=reply_id,
                url=album['url'], username="retry",
                current=album['retry_count'] + 1, total=album['max_retries']
            )
        
        await smart_queue.process_queue()
        print(f"✅ Retry complete\n")
    else:
        print(f"✅ No failed albums to retry")

# ============================================
# CALLBACK HANDLERS
# ============================================
@app.on_callback_query(filters.user(ADMIN_IDS))
async def handle_callbacks(client: Client, callback_query: CallbackQuery):
    """Handle all inline button callbacks"""
    data = callback_query.data
    
    try:
        if data == "show_dashboard":
            stats = smart_queue.get_stats()
            dashboard_text = live_dashboard.get_dashboard_text(stats)
            rate_text = rate_optimizer.get_dashboard_text()
            await callback_query.message.reply(
                dashboard_text + "\n" + rate_text,
                reply_markup=keyboard_manager.get_control_keyboard()
            )
            await callback_query.answer("Dashboard updated!")
        
        elif data == "pause_all":
            smart_queue.pause()
            await callback_query.answer("Queue paused!")
        
        elif data == "resume_all":
            smart_queue.resume()
            await callback_query.answer("Queue resumed!")
        
        elif data == "cancel_all":
            smart_queue.cancel_all()
            cancel_tasks[callback_query.message.chat.id] = True
            await callback_query.answer("All tasks cancelled!")
        
        elif data == "clean_cache":
            count = smart_cache.clean_old_cache()
            await callback_query.answer(f"Cleaned {count} cache files!")
    
    except Exception as e:
        logger.error(f"Callback error: {e}")
        try:
            await callback_query.answer("Error processing request")
        except:
            pass

# ============================================
# COMMAND HANDLERS
# ============================================
@app.on_message(filters.command("start", prefixes=".") & filters.user(ADMIN_IDS))
async def start_cmd(client: Client, message):
    """Start command - show help"""
    await message.reply(
        "**Bot Started!**\n\n"
        "**Commands:**\n"
        "`.user <username>` - Download all content\n"
        "`.retry` - Retry failed albums\n"
        "`.failed` - Show failed albums\n"
        "`.rate [seconds]` - View/set upload delay\n"
        "`.dashboard` - Live status + Rate optimizer\n"
        "`.errors` - Recent errors\n"
        "`.missing` - Check missing media\n"
        "`.cancel` - Stop all operations\n"
        "`.stats` - Statistics\n"
        "`.reset` - Reset database\n\n"
        "**Features:**\n"
        "- Download 3 albums concurrently\n"
        "- Upload sequential (no mixing)\n"
        "- Rate Limit Optimizer (auto-adaptive)\n"
        "- GitHub sync (resume on new VPS)\n"
        "- Failed album tracking + auto-retry\n"
        "- GIFs → MP4 | Compression | Anti-Flood",
        reply_markup=keyboard_manager.get_control_keyboard()
    )

@app.on_message(filters.command("retry", prefixes=".") & filters.user(ADMIN_IDS))
async def retry_cmd(client: Client, message):
    """Manually retry failed albums"""
    failed = get_failed_albums()
    if failed:
        await message.reply(f"🔄 Retrying {len(failed)} failed albums...")
        await retry_failed_albums(client, message.chat.id, message.id)
        await message.reply("✅ Retry complete!")
    else:
        await message.reply("✅ No failed albums to retry!")

@app.on_message(filters.command("failed", prefixes=".") & filters.user(ADMIN_IDS))
async def failed_cmd(client: Client, message):
    """Show failed albums"""
    failed = get_failed_albums()
    if failed:
        text = f"**Failed Albums ({len(failed)}):**\n"
        for f in failed[:20]:
            text += f"- `{f['album_id']}`: {f['error_type']} (retry {f['retry_count']}/{f['max_retries']})\n"
        await message.reply(text)
    else:
        await message.reply("✅ No failed albums!")

@app.on_message(filters.command("rate", prefixes=".") & filters.user(ADMIN_IDS))
async def rate_cmd(client: Client, message):
    """Show or adjust rate optimizer"""
    if len(message.command) > 1:
        try:
            new_delay = int(message.command[1])
            old_delay = rate_optimizer.current_delay
            rate_optimizer.current_delay = max(rate_optimizer.min_delay, min(rate_optimizer.max_delay, new_delay))
            await message.reply(f"⚡ Rate delay adjusted: `{old_delay}s` → `{rate_optimizer.current_delay}s`")
        except:
            await message.reply("Usage: `.rate <seconds>`\nExample: `.rate 10`")
    else:
        await message.reply(rate_optimizer.get_dashboard_text())

@app.on_message(filters.command("cancel", prefixes=".") & filters.user(ADMIN_IDS))
async def cancel_cmd(client: Client, message):
    """Cancel all operations"""
    cancel_tasks[message.chat.id] = True
    smart_queue.cancel_all()
    await message.reply("**Cancellation sent! All operations stopped.**")

@app.on_message(filters.command("dashboard", prefixes=".") & filters.user(ADMIN_IDS))
async def dashboard_cmd(client: Client, message):
    """Show live dashboard with rate optimizer"""
    stats = smart_queue.get_stats()
    dashboard_text = live_dashboard.get_dashboard_text(stats)
    rate_text = rate_optimizer.get_dashboard_text()
    keyboard = keyboard_manager.get_control_keyboard()
    await message.reply(dashboard_text + "\n" + rate_text, reply_markup=keyboard)

@app.on_message(filters.command("errors", prefixes=".") & filters.user(ADMIN_IDS))
async def errors_cmd(client: Client, message):
    """Show recent errors"""
    try:
        conn = sqlite3.connect(DB_NAME)
        rows = conn.execute(
            "SELECT album_id, error_type, error_message, timestamp FROM error_log ORDER BY timestamp DESC LIMIT 20"
        ).fetchall()
        conn.close()
        
        if rows:
            text = "**Recent Errors:**\n"
            for row in rows:
                text += f"- `{row[0]}`: {row[1]} - {row[2][:80]}\n"
            await message.reply(text)
        else:
            await message.reply("✅ No errors logged!")
    except Exception as e:
        await message.reply(f"Error reading logs: {e}")

@app.on_message(filters.command("missing", prefixes=".") & filters.user(ADMIN_IDS))
async def missing_cmd(client: Client, message):
    """Check for missing media"""
    if media_tracker.pending_albums:
        text = "**Pending Albums:**\n"
        for album_id in list(media_tracker.pending_albums.keys())[:10]:
            missing = media_tracker.get_missing_media(album_id)
            text += f"- `{album_id}`: {len(missing['photos'])}p, {len(missing['videos'])}v missing\n"
        await message.reply(text)
    else:
        await message.reply("✅ No pending albums!")

@app.on_message(filters.command("user", prefixes=".") & filters.user(ADMIN_IDS))
async def user_cmd(client: Client, message):
    """Download ALL content from Erome user"""
    if len(message.command) < 2:
        await message.reply("Usage: `.user <username or URL>`")
        return
    
    chat_id = message.chat.id
    raw_input = message.command[1].strip()
    cancel_tasks[chat_id] = False
    
    # Direct album URL
    if "/a/" in raw_input:
        await process_album(client, chat_id, message.id, raw_input, "direct", 1, 1)
        return

    # Extract username
    query = raw_input.split("erome.com/")[-1].split('/')[0]
    msg = await message.reply(f"**Starting full scan for `{query}`...**\n_Scanning all pages until exhausted_")
    
    all_urls = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.erome.com/',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9'
    }
    
    total_pages_scanned = 0
    
    # SCAN 1: User Profile (Posts) - ALL pages
    page = 1
    while True:
        if cancel_tasks.get(chat_id):
            break
        try:
            url = f"https://www.erome.com/{query}?page={page}"
            res = session.get(url, headers=headers, timeout=15)
            if res.status_code != 200:
                break
            ids = re.findall(r'/a/([a-zA-Z0-9]{8})', res.text)
            if not ids:
                break
            ids = list(dict.fromkeys(ids))
            for aid in ids:
                album_url = f"https://www.erome.com/a/{aid}"
                if album_url not in all_urls:
                    all_urls.append(album_url)
            total_pages_scanned += 1
            await safe_edit(msg, f"**Scanning `{query}`**\nPage: `{page}` | Found: `{len(all_urls)}` albums")
            if "Next" not in res.text:
                break
            page += 1
            await asyncio.sleep(0.3)
        except:
            break
    profile_pages = page
    
    # SCAN 2: Search/Reposts - ALL pages
    search_page = 1
    while True:
        if cancel_tasks.get(chat_id):
            break
        try:
            url = f"https://www.erome.com/search?v={query}&page={search_page}"
            res = session.get(url, headers=headers, timeout=15)
            if res.status_code != 200:
                break
            ids = re.findall(r'/a/([a-zA-Z0-9]{8})', res.text)
            if not ids:
                break
            ids = list(dict.fromkeys(ids))
            for aid in ids:
                album_url = f"https://www.erome.com/a/{aid}"
                if album_url not in all_urls:
                    all_urls.append(album_url)
            total_pages_scanned += 1
            if "Next" not in res.text:
                break
            search_page += 1
            await asyncio.sleep(0.3)
        except:
            break
    search_pages = search_page
    
    if not all_urls:
        return await msg.edit_text(f"**No content found for `{query}`**")
    
    print(f"\n{'='*60}")
    print(f"✅ SCAN COMPLETE: {query}")
    print(f"{'='*60}")
    print(f"Albums: {len(all_urls)}")
    print(f"Profile pages: {profile_pages}")
    print(f"Search pages: {search_pages}")
    print(f"{'='*60}\n")
    
    await msg.edit_text(
        f"**Scan Complete!**\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"User: `{query}`\n"
        f"Albums found: `{len(all_urls)}`\n"
        f"Profile pages: `{profile_pages}`\n"
        f"Search pages: `{search_pages}`\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"Smart Queue: `3` concurrent\n"
        f"Rate Optimizer: Active\n"
        f"GitHub Sync: Active\n"
        f"_Starting downloads..._"
    )
    
    # Add all albums to smart queue
    for i, url in enumerate(all_urls, 1):
        if cancel_tasks.get(chat_id):
            break
        await smart_queue.add_task(
            f"{query}_{i}",
            process_album,
            priority=i,
            client=client, chat_id=chat_id, reply_id=message.id,
            url=url, username=query, current=i, total=len(all_urls)
        )
    
    # Process queue
    await smart_queue.process_queue()
    
    # Final status
    stats = smart_queue.get_stats()
    failed = get_failed_albums()
    
    await message.reply(
        f"**All tasks completed!**\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"Success: `{stats['completed']}`\n"
        f"Failed: `{stats['failed']}`\n"
        f"Failed for retry: `{len(failed)}`\n"
        f"Total albums: `{len(all_urls)}`\n\n"
        f"_.retry - Retry failed albums_\n"
        f"_.failed - Show failed albums_\n"
        f"_.rate - View rate optimizer_"
    )

@app.on_message(filters.command("reset", prefixes=".") & filters.user(ADMIN_IDS))
async def reset_db(client: Client, message):
    """Reset database"""
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
    init_db()
    backup_to_github()
    await message.reply("**Memory Cleared! Database reset successfully.**")

@app.on_message(filters.command("stats", prefixes=".") & filters.user(ADMIN_IDS))
async def stats_cmd(client: Client, message):
    """Show bot statistics"""
    conn = sqlite3.connect(DB_NAME)
    processed_count = conn.execute("SELECT COUNT(*) FROM fully_processed").fetchone()[0]
    error_count = conn.execute("SELECT COUNT(*) FROM error_log").fetchone()[0]
    failed_count = conn.execute("SELECT COUNT(*) FROM failed_albums WHERE retry_count < max_retries").fetchone()[0]
    conn.close()
    
    queue_stats = smart_queue.get_stats()
    dashboard_text = live_dashboard.get_dashboard_text(queue_stats)
    rate_text = rate_optimizer.get_dashboard_text()
    
    await message.reply(
        f"**Bot Statistics**\n\n"
        f"Fully Processed: `{processed_count}`\n"
        f"Errors Logged: `{error_count}`\n"
        f"Failed (retry): `{failed_count}`\n\n"
        f"{dashboard_text}\n{rate_text}"
    )

# ============================================
# MAIN ENTRY POINT
# ============================================
async def main():
    """Main function to start the bot"""
    init_db()
    async with app:
        print("\n" + "=" * 60)
        print("🚀 BOT STARTED SUCCESSFULLY!")
        print("=" * 60)
        print("✅ Download: 3 albums concurrently")
        print("✅ Upload: Sequential + Rate Optimizer")
        print("✅ GitHub Sync: Fully Processed Only")
        print("✅ Failed Tracking: Auto-Retry on Startup")
        print("✅ Scanner: ALL Pages (Posts + Reposts)")
        print("✅ GIF→MP4 | Compression | Anti-Flood")
        print("✅ Rate Optimizer: Auto-Adaptive Delays")
        print("=" * 60)
        print("Commands:")
        print("  .user | .retry | .failed | .rate")
        print("  .dashboard | .errors | .missing")
        print("  .stats | .cancel | .reset")
        print("=" * 60 + "\n")
        
        # Auto-retry failed albums on startup
        await retry_failed_albums(app, ADMIN_IDS[0], 0)
        
        await idle()

if __name__ == "__main__":
    try:
        app.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
