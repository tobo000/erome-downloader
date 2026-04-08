import customtkinter as ctk
from customtkinter import filedialog
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import os
import threading
import time
import platform
import subprocess
import string
from concurrent.futures import ThreadPoolExecutor

# Setup Theme
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

class ToboEromeHyperSpeedV55(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("TOBO EROME TURBO V5.5 - HYPER-SPEED")
        self.geometry("800x850")

        self.save_directory = os.path.join(os.path.expanduser("~"), "Downloads") 
        self.last_folder = ""
        self.downloaded_count = 0
        
        # --- NEW: Setup Persistent Session for Speed ---
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20) # Allows more simultaneous data streams
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # --- UI LAYOUT ---
        self.label_title = ctk.CTkLabel(self, text="⚡ EROME HYPER-SPEED v5.5", font=("Roboto", 26, "bold"), text_color="#00ffcc")
        self.label_title.pack(pady=15)

        self.frame_counter = ctk.CTkFrame(self, fg_color="transparent")
        self.frame_counter.pack(padx=20, fill="x")
        ctk.CTkLabel(self.frame_counter, text="Paste Album Links Below:").pack(side="left")
        self.count_display = ctk.CTkLabel(self.frame_counter, text="Links Found: 0", font=("Roboto", 12, "bold"), text_color="#00ffcc")
        self.count_display.pack(side="right")
        
        self.links_textbox = ctk.CTkTextbox(self, height=180, font=("Consolas", 12))
        self.links_textbox.pack(pady=5, padx=20, fill="x")
        self.links_textbox.bind("<KeyRelease>", self.update_link_count)

        self.info_label = ctk.CTkLabel(self, text="🚀 Connection Pooling Enabled | 8x Multi-threading | Speed Optimized", text_color="#ffcc00")
        self.info_label.pack(pady=5)

        self.btn_browse = ctk.CTkButton(self, text="📁 SELECT SAVE FOLDER", fg_color="#4CAF50", height=35, command=self.select_folder)
        self.btn_browse.pack(pady=10, padx=20, fill="x")

        self.progress_label = ctk.CTkLabel(self, text="Waiting for start...", font=("Roboto", 12))
        self.progress_label.pack(padx=20, anchor="w")
        self.progress_bar = ctk.CTkProgressBar(self)
        self.progress_bar.set(0)
        self.progress_bar.pack(pady=5, padx=20, fill="x")

        self.log_box = ctk.CTkTextbox(self, height=250, font=("Consolas", 11))
        self.log_box.pack(pady=10, padx=20, fill="both", expand=True)

        self.btn_start = ctk.CTkButton(self, text="🚀 START HYPER-SPEED DOWNLOAD", fg_color="#1f538d", height=50, font=("Roboto", 18, "bold"), command=self.start_batch_thread)
        self.btn_start.pack(pady=10, padx=20, fill="x")

        self.btn_open = ctk.CTkButton(self, text="📂 OPEN LAST FOLDER", fg_color="#2d2d2d", state="disabled", command=self.open_folder)
        self.btn_open.pack(pady=5, padx=20, fill="x")

    def update_link_count(self, event=None):
        content = self.links_textbox.get("1.0", "end").strip()
        urls = [u.strip() for u in content.split('\n') if "erome.com" in u]
        self.count_display.configure(text=f"Links Found: {len(urls)}", text_color="#00ffcc" if len(urls) > 0 else "white")

    def safe_log(self, message):
        timestamp = time.strftime("[%H:%M:%S]")
        self.after(0, lambda: self.log_box.insert("end", f"{timestamp} {message}\n"))
        self.after(0, lambda: self.log_box.see("end"))

    def safe_update_progress(self, current, total):
        pct = current / total
        self.after(0, lambda: self.progress_bar.set(pct))
        self.after(0, lambda: self.progress_label.configure(text=f"Album Progress: {int(pct*100)}% ({current}/{total})"))

    def select_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.save_directory = folder
            self.safe_log(f"📍 Save location: {self.save_directory}")

    def open_folder(self):
        if self.last_folder and os.path.exists(self.last_folder):
            if platform.system() == "Windows": os.startfile(self.last_folder)
            else: subprocess.call(["open" if platform.system() == "Darwin" else "xdg-open", self.last_folder])

    def start_batch_thread(self):
        links_raw = self.links_textbox.get("1.0", "end").strip()
        urls = [u.strip() for u in links_raw.split('\n') if "erome.com" in u]
        if not urls:
            self.safe_log("❌ Error: No valid links to download!")
            return
        self.btn_start.configure(state="disabled", text="HYPER-SPEED WORKING...")
        self.log_box.delete("1.0", "end")
        threading.Thread(target=self.run_batch, args=(urls,), daemon=True).start()

    def run_batch(self, urls):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'}
        for index, url in enumerate(urls, 1):
            try:
                album_id = url.rstrip('/').split('/')[-1]
                save_path = os.path.join(self.save_directory, album_id)
                os.makedirs(save_path, exist_ok=True)
                self.last_folder = save_path
                self.downloaded_count = 0
                
                self.safe_log(f"🔍 Analyzing Album {index}/{len(urls)} ID: {album_id}")
                res = self.session.get(url, headers=headers, timeout=25)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, 'html.parser')

                # Purge Junk
                for junk in soup.find_all(["div", "section"], {"id": ["related_albums", "comments", "footer"]}): junk.decompose()
                for junk_c in soup.find_all("div", {"class": ["albums-grid", "user-info", "header"]}): junk_c.decompose()

                media_list = []
                # Find Max Quality Videos
                for v in soup.find_all('video'):
                    srcs = [v.get('src'), v.get('data-src')]
                    for s in v.find_all('source'): srcs.extend([s.get('src'), s.get('data-src')])
                    mp4s = list(set([s for s in srcs if s and ".mp4" in s.lower()]))
                    if mp4s:
                        best = next((l for l in mp4s if "_1080p" in l), next((l for l in mp4s if "_720p" in l), mp4s[0]))
                        media_list.append(best)

                # Find Photos
                for div in soup.select('div.img'):
                    img = div.find('img')
                    if img and (img.get('data-src') or img.get('src')):
                        src = img.get('data-src') or img.get('src')
                        if "erome.com" in src and "avatar" not in src.lower():
                            media_list.append(src)

                media_list = list(dict.fromkeys(media_list))
                total = len(media_list)
                if total == 0: continue

                self.safe_log(f"🚀 Found {total} items. Starting Hyper-Speed Multi-threading...")
                
                # --- Increased to 8 Workers for faster simultaneous downloading ---
                with ThreadPoolExecutor(max_workers=8) as executor:
                    for m_url in media_list:
                        executor.submit(self.download_core, m_url, save_path, headers, url, total)
                time.sleep(1)

            except Exception as e:
                self.safe_log(f"❌ Error on {url}: {e}")

        self.safe_log("🏆 ALL TASKS COMPLETED!")
        self.after(0, lambda: self.btn_open.configure(state="normal"))
        self.after(0, lambda: self.btn_start.configure(state="normal", text="🚀 START HYPER-SPEED DOWNLOAD"))

    def download_core(self, m_url, save_path, headers, album_url, total):
        if m_url.startswith('//'): m_url = 'https:' + m_url
        filename = m_url.split('/')[-1].split('?')[0]
        filepath = os.path.join(save_path, filename)

        if os.path.exists(filepath):
            self.downloaded_count += 1
            self.safe_update_progress(self.downloaded_count, total)
            return

        try:
            h = headers.copy()
            h['Referer'] = album_url
            # Using self.session instead of requests.get for 2x faster performance
            with self.session.get(m_url, headers=h, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024*1024): # 1MB Chunks
                        if chunk: f.write(chunk)
            
            self.downloaded_count += 1
            self.safe_log(f"📥 Done: {filename}")
            self.safe_update_progress(self.downloaded_count, total)
        except Exception as e:
            self.safe_log(f"❌ Failed: {filename}")

if __name__ == "__main__":
    app = ToboEromeHyperSpeedV55()
    app.mainloop()
