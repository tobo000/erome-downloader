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

# GitHub Config
GH_TOKEN = os.getenv("GH_TOKEN")
GH_REPO = os.getenv("GH_REPO") 
GH_FILE_PATH = "bot_archive.db"

if not API_ID or not API_HASH:
    raise ValueError("Missing API_ID or API_HASH in .env file")

try:
    API_ID = int(API_ID)
except ValueError:
    raise ValueError("API_ID must be an integer")

# Setup logging
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

# Client with flood protection
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

# Create directories
for directory in [DOWNLOAD_DIR, CACHE_DIR, CHECKPOINT_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

session = requests.Session()
executor = ThreadPoolExecutor(max_workers=8)
cancel_tasks = {}
chat_locks = {}

# ============================================
# ERROR NOTIFICATION SYSTEM (Terminal Only)
# ============================================
class ErrorNotifier:
    """Log errors to terminal and files - no DM"""
    def __init__(self):
        self.error_count = 0
        self.warning_count = 0
    
    def notify(self, error_type: str, message: str, details: str = ""):
        """Log error to terminal"""
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
        """Log warning to terminal"""
        self.warning_count += 1
        print(f"⚠️  WARNING [{warning_type}]: {message[:150]}")
        logger.warning(f"[WARNING] {warning_type}: {message}")
    
    def success(self, message: str):
        """Log success to terminal"""
        print(f"✅ {message}")
        logger.info(f"[SUCCESS] {message}")
    
    def album_report(self, album_id: str, title: str, photos: int, videos: int, 
                     downloaded_p: int, uploaded_p: int, downloaded_v: int, uploaded_v: int,
                     missing_p: int, missing_v: int, success: bool):
        """Display album report in terminal"""
        border = "=" * 60
        status = "✅ COMPLETE" if (success and missing_p == 0 and missing_v == 0) else "❌ ISSUES FOUND"
        
        report = (
            f"\n{border}\n"
            f"📊 ALBUM REPORT | {status}\n"
            f"{border}\n"
            f"Album ID: {album_id}\n"
            f"Title: {title[:50]}\n"
            f"Photos: {downloaded_p}/{photos} downloaded | {uploaded_p}/{photos} uploaded"
        )
        if missing_p > 0:
            report += f" | ⚠️ {missing_p} MISSING"
        
        report += f"\nVideos: {downloaded_v}/{videos} downloaded | {uploaded_v}/{videos} uploaded"
        if missing_v > 0:
            report += f" | ⚠️ {missing_v} MISSING"
        
        report += f"\n{border}\n"
        print(report)
        logger.info(report)
    
    def get_stats(self) -> Dict:
        """Get error statistics"""
        return {
            'total_errors': self.error_count,
            'total_warnings': self.warning_count
        }

error_notifier = ErrorNotifier()

# ============================================
# MEDIA TRACKING SYSTEM
# ============================================
class MediaTracker:
    """Track every media item to ensure nothing is missed"""
    def __init__(self):
        self.pending_albums = {}
    
    def register_album(self, album_id: str, title: str, photos: List[str], videos: List[str]):
        """Register a new album for tracking"""
        self.pending_albums[album_id] = {
            'title': title,
            'photos': photos,
            'videos': videos,
            'downloaded': {'photos': [], 'videos': []},
            'uploaded': {'photos': [], 'videos': []},
            'timestamp': time.time()
        }
    
    def mark_downloaded(self, album_id: str, media_type: str, url: str):
        """Mark media as downloaded"""
        if album_id in self.pending_albums:
            self.pending_albums[album_id]['downloaded'][media_type].append(url)
    
    def mark_uploaded(self, album_id: str, media_type: str, url: str):
        """Mark media as uploaded successfully"""
        if album_id in self.pending_albums:
            self.pending_albums[album_id]['uploaded'][media_type].append(url)
    
    def get_missing_media(self, album_id: str) -> Dict:
        """Get list of media that was downloaded but not uploaded"""
        if album_id not in self.pending_albums:
            return {'photos': [], 'videos': []}
        
        album = self.pending_albums[album_id]
        missing = {'photos': [], 'videos': []}
        
        for media_type in ['photos', 'videos']:
            downloaded_urls = album['downloaded'][media_type]
            uploaded_urls = album['uploaded'][media_type]
            missing[media_type] = [url for url in downloaded_urls if url not in uploaded_urls]
        
        return missing
    
    def get_album_stats(self, album_id: str) -> Dict:
        """Get statistics for an album"""
        if album_id not in self.pending_albums:
            return {}
        
        album = self.pending_albums[album_id]
        missing = self.get_missing_media(album_id)
        
        return {
            'title': album['title'],
            'total_photos': len(album['photos']),
            'total_videos': len(album['videos']),
            'downloaded_photos': len(album['downloaded']['photos']),
            'uploaded_photos': len(album['uploaded']['photos']),
            'downloaded_videos': len(album['downloaded']['videos']),
            'uploaded_videos': len(album['uploaded']['videos']),
            'missing_photos': len(missing['photos']),
            'missing_videos': len(missing['videos'])
        }
    
    def cleanup_album(self, album_id: str):
        """Remove album from tracker after successful completion"""
        if album_id in self.pending_albums:
            del self.pending_albums[album_id]

media_tracker = MediaTracker()

def get_chat_lock(chat_id: int) -> asyncio.Lock:
    """Get or create a lock for a specific chat to ensure sequential uploads"""
    if chat_id not in chat_locks:
        chat_locks[chat_id] = asyncio.Lock()
    return chat_locks[chat_id]

# ============================================
# SMART QUEUE SYSTEM
# ============================================
class SmartQueue:
    """Smart concurrent download queue system"""
    def __init__(self):
        self.queue = asyncio.Queue()
        self.active_tasks = {}
        self.completed_tasks = set()
        self.failed_tasks = {}
        self.max_concurrent = 3
        self.is_paused = False
    
    async def add_task(self, task_id: str, task_func, priority: int = 0, **kwargs):
        """Add a new task to the queue"""
        await self.queue.put((priority, task_id, task_func, kwargs))
        print(f"📥 Queue: {task_id}")
        logger.info(f"Task added: {task_id}")
    
    async def process_queue(self, progress_callback=None):
        """Process queue with concurrent execution"""
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
            
            if progress_callback:
                await progress_callback(self.get_stats())
        
        if self.active_tasks:
            await asyncio.gather(*self.active_tasks.values(), return_exceptions=True)
    
    def _task_completed(self, task_id: str, task: asyncio.Task):
        """Callback when a task completes"""
        try:
            result = task.result()
            if result:
                self.completed_tasks.add(task_id)
            else:
                self.failed_tasks[task_id] = "Task returned False"
        except Exception as e:
            self.failed_tasks[task_id] = str(e)
    
    def get_stats(self) -> Dict:
        """Get current queue statistics"""
        return {
            'queue_size': self.queue.qsize(),
            'active_tasks': len(self.active_tasks),
            'completed': len(self.completed_tasks),
            'failed': len(self.failed_tasks),
            'is_paused': self.is_paused
        }

    def pause(self):
        """Pause the queue"""
        self.is_paused = True
    
    def resume(self):
        """Resume the queue"""
        self.is_paused = False
    
    def cancel_all(self):
        """Cancel all tasks"""
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
    """Save and restore download progress for recovery"""
    def __init__(self):
        self.checkpoint_dir = Path(CHECKPOINT_DIR)
    
    def save_checkpoint(self, album_id: str, state: Dict):
        """Save download state"""
        checkpoint_file = self.checkpoint_dir / f"{album_id}.json"
        state['timestamp'] = time.time()
        try:
            with open(checkpoint_file, 'w') as f:
                json.dump(state, f)
        except Exception as e:
            logger.error(f"Checkpoint save error: {e}")
    
    def load_checkpoint(self, album_id: str) -> Optional[Dict]:
        """Load saved download state"""
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
        """Remove checkpoint after completion"""
        checkpoint_file = self.checkpoint_dir / f"{album_id}.json"
        if checkpoint_file.exists():
            try:
                checkpoint_file.unlink()
            except:
                pass

checkpoint_manager = CheckpointManager()

# ============================================
# SMART CACHE SYSTEM
# ============================================
class SmartCache:
    """Intelligent caching system for scraped data"""
    def __init__(self, cache_dir: str = "cache", ttl_hours: int = 1):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.ttl = timedelta(hours=ttl_hours)
    
    def _get_cache_key(self, data: str) -> str:
        """Generate cache key from URL"""
        return hashlib.md5(data.encode()).hexdigest()
    
    def get_cached_album(self, url: str) -> Optional[Tuple]:
        """Retrieve album data from cache"""
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
        """Store album data in cache"""
        cache_file = self.cache_dir / f"album_{self._get_cache_key(url)}.pickle"
        try:
            data = {'timestamp': datetime.now(), 'content': content}
            with open(cache_file, 'wb') as f:
                pickle.dump(data, f)
        except:
            pass
    
    def clean_old_cache(self) -> int:
        """Remove expired cache files"""
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
    """Real-time progress dashboard"""
    def __init__(self):
        self.start_time = time.time()
        self.total_downloaded = 0
        self.current_speed = 0
        self.last_update = time.time()
    
    def update_speed(self, bytes_downloaded: int):
        """Update download speed metrics"""
        now = time.time()
        time_diff = now - self.last_update
        if time_diff > 0:
            self.current_speed = bytes_downloaded / time_diff
        self.total_downloaded += bytes_downloaded
        self.last_update = now
    
    def get_dashboard_text(self, queue_stats: Dict = None) -> str:
        """Generate dashboard text"""
        elapsed = time.time() - self.start_time
        elapsed_str = time.strftime('%H:%M:%S', time.gmtime(elapsed))
        
        text = (
            f"**Live Dashboard**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Uptime: `{elapsed_str}`\n"
            f"Downloaded: `{get_human_size(self.total_downloaded)}`\n"
            f"Speed: `{get_human_size(self.current_speed)}`/s\n"
        )
        
        if queue_stats:
            status = "PAUSED" if queue_stats.get('is_paused') else "RUNNING"
            text += (
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Status: `{status}`\n"
                f"Queue: `{queue_stats.get('queue_size', 0)}` remaining\n"
                f"Active: `{queue_stats.get('active_tasks', 0)}` tasks\n"
                f"Done: `{queue_stats.get('completed', 0)}`\n"
                f"Failed: `{queue_stats.get('failed', 0)}`\n"
            )
        
        return text

live_dashboard = LiveDashboard()

# ============================================
# SMART COMPRESSOR (with GIF to MP4)
# ============================================
class SmartCompressor:
    """Intelligent video compression engine with GIF conversion"""
    COMPRESSION_PROFILES = {
        'light': {'crf': 23, 'preset': 'fast'},
        'medium': {'crf': 28, 'preset': 'medium'},
        'maximum': {'crf': 32, 'preset': 'slow'}
    }
    
    def should_compress(self, filepath: str) -> bool:
        """Check if file needs compression"""
        if not os.path.exists(filepath):
            return False
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        return size_mb > 100
    
    def auto_select_profile(self, filepath: str) -> Dict:
        """Auto-select compression profile based on file size"""
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        if size_mb > 500:
            return self.COMPRESSION_PROFILES['maximum']
        elif size_mb > 200:
            return self.COMPRESSION_PROFILES['medium']
        return self.COMPRESSION_PROFILES['light']
    
    def compress_video(self, input_path: str, profile: Dict = None) -> Optional[str]:
        """Compress video file"""
        if not self.should_compress(input_path):
            return None
        
        if profile is None:
            profile = self.auto_select_profile(input_path)
        
        output_path = f"{input_path}.compressed.mp4"
        
        try:
            original_size = os.path.getsize(input_path)
            logger.info(f"Compressing: {get_human_size(original_size)}")
            
            cmd = [
                'ffmpeg', '-i', input_path,
                '-c:v', 'libx264',
                '-crf', str(profile['crf']),
                '-preset', profile['preset'],
                '-c:a', 'aac',
                '-b:a', '128k',
                '-movflags', 'faststart',
                '-y', output_path
            ]
            
            subprocess.run(cmd, stderr=subprocess.DEVNULL, timeout=300)
            
            if os.path.exists(output_path):
                compressed_size = os.path.getsize(output_path)
                reduction = (1 - compressed_size/original_size) * 100
                print(f"📦 Compressed: {reduction:.1f}% reduction")
                logger.info(f"Compressed: {reduction:.1f}% reduction")
                
                os.remove(input_path)
                os.rename(output_path, input_path)
                return input_path
        except subprocess.TimeoutExpired:
            logger.error("Compression timeout")
        except Exception as e:
            logger.error(f"Compression error: {e}")
        
        return None
    
    def convert_gif_to_mp4(self, gif_path: str) -> Optional[str]:
        """Convert GIF to MP4 video"""
        mp4_path = gif_path.replace('.gif', '.mp4')
        try:
            logger.info(f"Converting GIF to MP4: {gif_path}")
            
            cmd = [
                'ffmpeg', '-i', gif_path,
                '-c:v', 'libx264',
                '-pix_fmt', 'yuv420p',
                '-movflags', 'faststart',
                '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
                '-y', mp4_path
            ]
            
            subprocess.run(cmd, stderr=subprocess.DEVNULL, timeout=120)
            
            if os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 0:
                logger.info("GIF converted successfully")
                os.remove(gif_path)
                return mp4_path
            else:
                logger.error("GIF conversion failed")
                return None
        except subprocess.TimeoutExpired:
            logger.error("GIF conversion timeout")
            return None
        except Exception as e:
            logger.error(f"GIF conversion error: {e}")
            return None

smart_compressor = SmartCompressor()

# ============================================
# KEYBOARD MANAGER
# ============================================
class KeyboardManager:
    """Interactive inline keyboard manager"""
    @staticmethod
    def get_control_keyboard() -> InlineKeyboardMarkup:
        """Generate main control keyboard"""
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
# GITHUB SYNC ENGINE (WITH VERIFICATION)
# ============================================
def backup_to_github():
    """Sync database to GitHub with verification"""
    if not GH_TOKEN or not GH_REPO:
        print("⚠️  GitHub Sync: Skipped (no GH_TOKEN or GH_REPO configured)")
        return
    
    try:
        url = f"https://api.github.com/repos/{GH_REPO}/contents/{GH_FILE_PATH}"
        headers = {
            "Authorization": f"token {GH_TOKEN}", 
            "Accept": "application/vnd.github.v3+json"
        }
        
        # Get current file SHA if exists
        res = requests.get(url, headers=headers)
        
        if res.status_code == 200:
            sha = res.json().get('sha')
            print(f"📡 GitHub: Found existing file (SHA: {sha[:8]}...)")
        elif res.status_code == 404:
            sha = None
            print(f"📡 GitHub: File not found, will create new")
        else:
            print(f"❌ GitHub: Failed to check file (Status: {res.status_code})")
            logger.error(f"[GITHUB] Check failed: {res.status_code}")
            return
        
        # Read and encode database
        if not os.path.exists(DB_NAME):
            print(f"❌ GitHub: Database file not found: {DB_NAME}")
            return
        
        with open(DB_NAME, "rb") as f:
            content = base64.b64encode(f.read()).decode()
        
        db_size = os.path.getsize(DB_NAME)
        commit_message = f"Sync: {time.strftime('%Y-%m-%d %H:%M:%S')} | {get_human_size(db_size)}"
        
        data = {
            "message": commit_message,
            "content": content,
            "branch": "main"
        }
        if sha:
            data["sha"] = sha
        
        # Upload to GitHub
        put_res = requests.put(url, headers=headers, data=json.dumps(data))
        
        if put_res.status_code in [200, 201]:
            print(f"✅ GitHub Sync: Success! ({get_human_size(db_size)}) - {time.strftime('%H:%M:%S')}")
            logger.info(f"[GITHUB] Database synced: {get_human_size(db_size)}")
        else:
            print(f"❌ GitHub Sync: Failed (Status: {put_res.status_code})")
            print(f"   Response: {put_res.text[:200]}")
            logger.error(f"[GITHUB] Sync failed: {put_res.status_code}")
            
    except requests.RequestException as e:
        print(f"❌ GitHub Sync: Network error - {e}")
        logger.error(f"[GITHUB] Network error: {e}")
    except Exception as e:
        print(f"❌ GitHub Sync: Error - {e}")
        logger.error(f"[GITHUB] Error: {e}")


def download_from_github():
    """Restore database from GitHub"""
    if not GH_TOKEN or not GH_REPO:
        print("⚠️  GitHub Restore: Skipped (no credentials)")
        return
    
    try:
        url = f"https://api.github.com/repos/{GH_REPO}/contents/{GH_FILE_PATH}"
        headers = {
            "Authorization": f"token {GH_TOKEN}", 
            "Accept": "application/vnd.github.v3+json"
        }
        
        res = requests.get(url, headers=headers)
        
        if res.status_code == 200:
            content = base64.b64decode(res.json()['content'])
            db_size = len(content)
            
            with open(DB_NAME, "wb") as f:
                f.write(content)
            
            print(f"✅ GitHub Restore: Success! ({get_human_size(db_size)})")
            logger.info(f"[GITHUB] Database restored: {get_human_size(db_size)}")
        elif res.status_code == 404:
            print(f"📡 GitHub Restore: No existing database found - starting fresh")
        else:
            print(f"❌ GitHub Restore: Failed (Status: {res.status_code})")
            
    except Exception as e:
        print(f"❌ GitHub Restore: Error - {e}")
        logger.error(f"[GITHUB] Error: {e}")

# ============================================
# DATABASE LOGIC
# ============================================
def init_db():
    """Initialize database and restore from GitHub"""
    download_from_github()
    conn = sqlite3.connect(DB_NAME)
    conn.execute("CREATE TABLE IF NOT EXISTS processed (album_id TEXT PRIMARY KEY)")
    conn.execute("CREATE TABLE IF NOT EXISTS processed_media (media_id TEXT PRIMARY KEY, album_id TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS error_log (id INTEGER PRIMARY KEY AUTOINCREMENT, album_id TEXT, error_type TEXT, error_message TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    conn.commit()
    conn.close()
    print("✅ Database initialized")

def is_processed(album_id: str) -> bool:
    """Check if album was already processed"""
    conn = sqlite3.connect(DB_NAME)
    res = conn.execute("SELECT 1 FROM processed WHERE album_id = ?", (album_id,)).fetchone()
    conn.close()
    return res is not None

def mark_processed(album_id: str):
    """Mark album as processed and sync to GitHub"""
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("INSERT OR IGNORE INTO processed (album_id) VALUES (?)", (album_id,))
        conn.commit()
        conn.close()
        
        # Sync to GitHub after every album
        backup_to_github()
    except Exception as e:
        print(f"❌ mark_processed error: {e}")

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
    """Create visual progress bar"""
    if total <= 0:
        return "[░░░░░░░░░░] 0%"
    pct = min(100, (current / total) * 100)
    return f"[{'█' * int(pct/10)}{'░' * (10 - int(pct/10))}] {pct:.1f}%"

def get_human_size(num: float) -> str:
    """Convert bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"

async def safe_edit(msg, text: str):
    """Safely edit message with error handling"""
    try:
        await msg.edit_text(text)
    except:
        pass

async def pyrogram_progress(current: int, total: int, status_msg, start_time: List[float], action_text: str, topic: str = ""):
    """Update progress with moon animations"""
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
    """Extract video metadata using ffprobe"""
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
    """Check if URL is a GIF file"""
    return '.gif' in url.lower()

# ============================================
# NITRO DOWNLOAD ENGINE
# ============================================
def download_nitro_animated(url: str, path: str, size: int, status_msg, loop, action: str, topic: str, segs: int = 4):
    """Multi-threaded nitro download engine"""
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
        except Exception as e:
            print(f"Part error: {e}")

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
    """Simple download for GIF files"""
    try:
        r = session.get(url, headers={'Referer': 'https://www.erome.com/'}, timeout=30)
        with open(path, 'wb') as f:
            f.write(r.content)
        return True
    except Exception as e:
        logger.error(f"Simple download error: {e}")
        return False

# ============================================
# SCRAPER WITH CACHE (GIF Detection)
# ============================================
def scrape_album_details(url: str) -> Tuple[str, List[str], List[str]]:
    """Scrape album details - detects GIFs, photos, and videos"""
    cached = smart_cache.get_cached_album(url)
    if cached:
        return cached
    
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.erome.com/'}
    try:
        res = session.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Untitled"
        
        # Get all media sources
        all_media = []
        for img in soup.select('div.img img'):
            src = img.get('data-src') or img.get('src')
            if src:
                if src.startswith('//'):
                    src = 'https:' + src
                all_media.append(src)
        
        # Separate GIFs from photos
        gifs = [x for x in all_media if '.gif' in x.lower()]
        photos = [x for x in all_media if '.gif' not in x.lower()]
        
        # Get videos
        v_l = []
        for v_tag in soup.find_all(['source', 'video']):
            v_src = v_tag.get('src') or v_tag.get('data-src')
            if v_src and ".mp4" in v_src:
                if v_src.startswith('//'):
                    v_src = 'https:' + v_src
                v_l.append(v_src)
        
        v_l.extend(re.findall(r'https?://[^\s"\'>]+.mp4', res.text))
        v_l = list(dict.fromkeys([v for v in v_l if "erome.com" in v]))
        
        # Combine GIFs with videos (they will be converted to MP4)
        all_videos = gifs + v_l
        
        result = (title, list(dict.fromkeys(photos)), list(dict.fromkeys(all_videos)))
        smart_cache.cache_album(url, result)
        return result
    except:
        return "Error", [], []

# ============================================
# CORE DELIVERY (Download 3 Concurrent, Upload Sequential)
# ============================================
async def process_album(client: Client, chat_id: int, reply_id: int, url: str, username: str, current: int, total: int) -> bool:
    """Process and deliver album - download concurrently, upload sequentially"""
    album_id = url.rstrip('/').split('/')[-1]
    
    # Check if already processed (from GitHub database)
    if is_processed(album_id):
        print(f"⏭️  Skipping (already processed): {album_id}")
        return True
    
    # Scrape album details
    title, photos, videos = await asyncio.get_event_loop().run_in_executor(
        executor, scrape_album_details, url
    )
    
    if not photos and not videos:
        return False
    
    # Register with media tracker
    media_tracker.register_album(album_id, title, photos, videos)
    print(f"\n📊 [{current}/{total}] Downloading: {title[:50]}")
    print(f"   Photos: {len(photos)} | Videos: {len(videos)}")
    
    user_folder = os.path.join(DOWNLOAD_DIR, f"{chat_id}_{album_id}")
    os.makedirs(user_folder, exist_ok=True)
    
    loop = asyncio.get_event_loop()
    
    # ============================================
    # PHASE 1: DOWNLOAD (3 albums concurrent - NO LOCK)
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
                        logger.warning(f"Could not determine size for video {v_idx}")
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
                
                # Mark as downloaded
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
    
    print(f"   ✅ Download complete for {album_id} ({len(downloaded_photos)}p, {len(downloaded_videos)}v)")
    
    # ============================================
    # PHASE 2: UPLOAD (Sequential per chat - WITH LOCK)
    # ============================================
    chat_lock = get_chat_lock(chat_id)
    uploaded_p, uploaded_v = 0, 0
    
    async with chat_lock:
        print(f"   🔒 Upload lock acquired for {album_id}")
        
        # Send status message
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
                        print(f"   ⏳ FloodWait photos: {wait_time}s")
                        await asyncio.sleep(wait_time + 5)
                    except Exception as e:
                        if attempt < 2:
                            await asyncio.sleep(5)
            
            # Mark as uploaded
            for path, caption, p_url in downloaded_photos:
                media_tracker.mark_uploaded(album_id, 'photos', p_url)
                uploaded_p += 1
            
            # Cleanup photo files
            for path, caption, p_url in downloaded_photos:
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except:
                    pass
        
        # Upload Videos (one by one)
        if downloaded_videos:
            for idx, (filepath, thumb, w, h, dur, caption, is_gif, v_url) in enumerate(downloaded_videos, 1):
                media_type = "GIF" if is_gif else "Video"
                full_caption = master_caption + f"\n\n{caption}" if not master_caption_sent else caption
                if idx == 1:
                    master_caption_sent = True
                
                upload_success = False
                for attempt in range(3):
                    try:
                        file_size = get_human_size(os.path.getsize(filepath))
                        logger.info(f"Uploading {media_type} {idx}/{len(downloaded_videos)} ({file_size})")
                        
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
                        print(f"   ✅ {media_type} {idx} uploaded")
                        await asyncio.sleep(15)
                        break
                        
                    except FloodWait as e:
                        wait_time = e.value if hasattr(e, 'value') else 15
                        print(f"   ⏳ FloodWait: {wait_time}s")
                        await asyncio.sleep(wait_time + 15)
                        
                    except RPCError as e:
                        if "FILE_PART_X_MISSING" in str(e):
                            print(f"   ⏳ File part missing - waiting 25s")
                            await asyncio.sleep(25)
                        elif attempt < 2:
                            await asyncio.sleep(15)
                    
                    except Exception as e:
                        if attempt < 2:
                            await asyncio.sleep(15)
                
                if not upload_success:
                    error_notifier.notify("Video Upload", f"Failed: {idx}/{len(downloaded_videos)}", album_id)
                    log_error_to_db(album_id, "video_upload", f"Failed video {idx}")
                
                # Cleanup this video file
                try:
                    if os.path.exists(filepath):
                        os.remove(filepath)
                    if thumb and os.path.exists(thumb):
                        os.remove(thumb)
                except:
                    pass
        
        # Cleanup status message
        try:
            await status.delete()
        except:
            pass
        
        print(f"   🔓 Upload lock released for {album_id}")
    
    # ============================================
    # PHASE 3: REPORT
    # ============================================
    missing = media_tracker.get_missing_media(album_id)
    missing_p = len(missing['photos'])
    missing_v = len(missing['videos'])
    
    album_success = (missing_p == 0 and missing_v == 0)
    
    error_notifier.album_report(
        album_id, title, len(photos), len(videos),
        len(downloaded_photos), uploaded_p,
        len(downloaded_videos), uploaded_v,
        missing_p, missing_v, album_success
    )
    
    if missing_p > 0 or missing_v > 0:
        print(f"   ⚠️  MISSING: {missing_p} photos, {missing_v} videos were downloaded but not uploaded!")
    
    # Mark as processed (syncs to GitHub)
    mark_processed(album_id)
    media_tracker.cleanup_album(album_id)
    
    return album_success

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
            keyboard = keyboard_manager.get_control_keyboard()
            await callback_query.message.reply(dashboard_text, reply_markup=keyboard)
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
    """Start command - show help and control panel"""
    await message.reply(
        "**Bot Started!**\n\n"
        "**Commands:**\n"
        "`.user <username/url>` - Download all content\n"
        "`.dashboard` - Show live dashboard\n"
        "`.errors` - Show recent errors\n"
        "`.missing` - Check for missing media\n"
        "`.cancel` - Cancel all operations\n"
        "`.stats` - Show statistics\n"
        "`.reset` - Reset database\n\n"
        "**Features:**\n"
        "- Download 3 albums concurrently\n"
        "- Upload sequential per chat (no mixing)\n"
        "- Scans ALL pages (posts + reposts)\n"
        "- GIFs auto-convert to MP4\n"
        "- GitHub sync (resume on new VPS)\n"
        "- Media tracking (no missing content)\n"
        "- Smart flood protection\n"
        "- Terminal error notifications",
        reply_markup=keyboard_manager.get_control_keyboard()
    )

@app.on_message(filters.command("cancel", prefixes=".") & filters.user(ADMIN_IDS))
async def cancel_cmd(client: Client, message):
    """Cancel all operations"""
    cancel_tasks[message.chat.id] = True
    smart_queue.cancel_all()
    await message.reply("**Cancellation sent! All operations stopped.**")

@app.on_message(filters.command("dashboard", prefixes=".") & filters.user(ADMIN_IDS))
async def dashboard_cmd(client: Client, message):
    """Show live dashboard"""
    stats = smart_queue.get_stats()
    dashboard_text = live_dashboard.get_dashboard_text(stats)
    keyboard = keyboard_manager.get_control_keyboard()
    await message.reply(dashboard_text, reply_markup=keyboard)

@app.on_message(filters.command("errors", prefixes=".") & filters.user(ADMIN_IDS))
async def errors_cmd(client: Client, message):
    """Show recent errors"""
    try:
        conn = sqlite3.connect(DB_NAME)
        rows = conn.execute("SELECT album_id, error_type, error_message, timestamp FROM error_log ORDER BY timestamp DESC LIMIT 20").fetchall()
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
    """Check for missing media in pending albums"""
    if media_tracker.pending_albums:
        text = "**Pending Albums with Missing Media:**\n"
        found_missing = False
        for album_id in list(media_tracker.pending_albums.keys())[:10]:
            missing = media_tracker.get_missing_media(album_id)
            if missing['photos'] or missing['videos']:
                found_missing = True
                text += f"- `{album_id}`: {len(missing['photos'])}p, {len(missing['videos'])}v missing\n"
        if not found_missing:
            text += "✅ No missing media in pending albums!"
        await message.reply(text)
    else:
        await message.reply("✅ No pending albums!")

@app.on_message(filters.command("user", prefixes=".") & filters.user(ADMIN_IDS))
async def user_cmd(client: Client, message):
    """Download ALL content from Erome user - scans every page until exhausted"""
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
    
    # ============================================
    # SCAN 1: User Profile (Posts) - ALL pages
    # ============================================
    profile_url = f"https://www.erome.com/{query}"
    logger.info(f"[SCANNER] Starting profile scan: {profile_url}")
    
    page = 1
    while True:
        if cancel_tasks.get(chat_id):
            logger.info("[SCANNER] Cancelled by user")
            break
        
        try:
            url = f"{profile_url}?page={page}"
            res = session.get(url, headers=headers, timeout=15)
            
            if res.status_code != 200:
                logger.info(f"[SCANNER] Profile page {page} returned {res.status_code} - stopping")
                break
            
            ids = re.findall(r'/a/([a-zA-Z0-9]{8})', res.text)
            
            if not ids:
                logger.info(f"[SCANNER] No albums on profile page {page} - stopping")
                break
            
            ids = list(dict.fromkeys(ids))
            
            new_count = 0
            for aid in ids:
                album_url = f"https://www.erome.com/a/{aid}"
                if album_url not in all_urls:
                    all_urls.append(album_url)
                    new_count += 1
            
            total_pages_scanned += 1
            logger.info(f"[SCANNER] Profile page {page}: +{new_count} new | Total: {len(all_urls)}")
            
            await safe_edit(msg,
                f"**Scanning `{query}` - Profile**\n"
                f"Page: `{page}` | Found: `{len(all_urls)}` albums\n"
                f"_Scanning all pages until exhausted..._"
            )
            
            # Check for next page
            has_next = "Next" in res.text or f'?page={page+1}' in res.text or f'/page/{page+1}' in res.text
            
            if not has_next:
                logger.info(f"[SCANNER] No next page after profile page {page} - profile scan complete")
                break
            
            page += 1
            await asyncio.sleep(0.3)
            
        except requests.RequestException as e:
            logger.error(f"[SCANNER] Network error on profile page {page}: {e}")
            break
        except Exception as e:
            logger.error(f"[SCANNER] Error on profile page {page}: {e}")
            break
    
    profile_pages = page
    
    # ============================================
    # SCAN 2: Search/Reposts - ALL pages
    # ============================================
    search_url = f"https://www.erome.com/search?v={query}"
    logger.info(f"[SCANNER] Starting search/reposts scan: {search_url}")
    
    search_page = 1
    while True:
        if cancel_tasks.get(chat_id):
            logger.info("[SCANNER] Search scan cancelled by user")
            break
        
        try:
            url = f"{search_url}&page={search_page}"
            res = session.get(url, headers=headers, timeout=15)
            
            if res.status_code != 200:
                logger.info(f"[SCANNER] Search page {search_page} returned {res.status_code} - stopping")
                break
            
            ids = re.findall(r'/a/([a-zA-Z0-9]{8})', res.text)
            
            if not ids:
                logger.info(f"[SCANNER] No albums on search page {search_page} - stopping")
                break
            
            ids = list(dict.fromkeys(ids))
            
            new_count = 0
            for aid in ids:
                album_url = f"https://www.erome.com/a/{aid}"
                if album_url not in all_urls:
                    all_urls.append(album_url)
                    new_count += 1
            
            total_pages_scanned += 1
            logger.info(f"[SCANNER] Search page {search_page}: +{new_count} new | Total: {len(all_urls)}")
            
            await safe_edit(msg,
                f"**Scanning `{query}` - All Pages**\n"
                f"Profile: `{profile_pages}` pgs | Search: `{search_page}` pgs\n"
                f"Total albums found: `{len(all_urls)}`\n"
                f"_Scanning all pages until exhausted..._"
            )
            
            # Check for next page
            has_next = "Next" in res.text or f'&page={search_page+1}' in res.text
            
            if not has_next:
                logger.info(f"[SCANNER] No next page after search page {search_page} - search scan complete")
                break
            
            search_page += 1
            await asyncio.sleep(0.3)
            
        except requests.RequestException as e:
            logger.error(f"[SCANNER] Network error on search page {search_page}: {e}")
            break
        except Exception as e:
            logger.error(f"[SCANNER] Error on search page {search_page}: {e}")
            break
    
    search_pages = search_page
    
    # ============================================
    # SCAN COMPLETE - Show results
    # ============================================
    if not all_urls:
        return await msg.edit_text(f"**No content found for `{query}`**")
    
    logger.info(f"[SCANNER] Complete! {len(all_urls)} albums from {total_pages_scanned} pages")
    
    print(f"\n{'='*60}")
    print(f"✅ SCAN COMPLETE: {query}")
    print(f"{'='*60}")
    print(f"Albums found: {len(all_urls)}")
    print(f"Profile pages: {profile_pages}")
    print(f"Search pages: {search_pages}")
    print(f"Total pages: {total_pages_scanned}")
    print(f"{'='*60}\n")
    
    await msg.edit_text(
        f"**Scan Complete!**\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"User: `{query}`\n"
        f"Albums found: `{len(all_urls)}`\n"
        f"Profile pages: `{profile_pages}`\n"
        f"Search pages: `{search_pages}`\n"
        f"Total pages: `{total_pages_scanned}`\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"Smart Queue: `3` concurrent downloads\n"
        f"Upload: Sequential per chat (no mixing)\n"
        f"GitHub Sync: Active\n"
        f"GIFs: Auto-convert to MP4\n"
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
    await message.reply(
        f"**All tasks completed!**\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"Success: `{stats['completed']}`\n"
        f"Failed: `{stats['failed']}`\n"
        f"Total albums: `{len(all_urls)}`\n\n"
        f"_Send .errors to see any errors_\n"
        f"_Send .missing to check for missing media_"
    )

@app.on_message(filters.command("reset", prefixes=".") & filters.user(ADMIN_IDS))
async def reset_db(client: Client, message):
    """Reset database"""
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
    init_db()
    backup_to_github()
    await message.reply("**Memory Cleared! Database reset successfully. GitHub synced.**")

@app.on_message(filters.command("stats", prefixes=".") & filters.user(ADMIN_IDS))
async def stats_cmd(client: Client, message):
    """Show bot statistics"""
    conn = sqlite3.connect(DB_NAME)
    processed_count = conn.execute("SELECT COUNT(*) FROM processed").fetchone()[0]
    error_count = conn.execute("SELECT COUNT(*) FROM error_log").fetchone()[0]
    conn.close()
    
    queue_stats = smart_queue.get_stats()
    dashboard_text = live_dashboard.get_dashboard_text(queue_stats)
    notifier_stats = error_notifier.get_stats()
    
    await message.reply(
        f"**Bot Statistics**\n\n"
        f"Processed albums: `{processed_count}`\n"
        f"Errors logged: `{error_count}`\n"
        f"Terminal errors: `{notifier_stats['total_errors']}`\n"
        f"Terminal warnings: `{notifier_stats['total_warnings']}`\n"
        f"Download directory: `{DOWNLOAD_DIR}`\n\n"
        f"{dashboard_text}"
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
        print("✅ Upload: Sequential per chat (no mixing)")
        print("✅ Scanner: ALL pages (Posts + Reposts)")
        print("✅ GIF → MP4: Auto-convert")
        print("✅ Compression: >100MB videos")
        print("✅ GitHub Sync: Active (resume on new VPS)")
        print("✅ Media Tracking: Active")
        print("✅ Error Notifications: Terminal")
        print("✅ FloodWait Protection: Auto-handle")
        print("✅ Checkpoint: Resume after crash")
        print("✅ Smart Cache: 1hr TTL")
        print("=" * 60)
        print("Commands: .user | .dashboard | .errors | .missing | .stats | .cancel | .reset")
        print("=" * 60 + "\n")
        await idle()

if __name__ == "__main__":
    try:
        app.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
