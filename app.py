"""
╔══════════════════════════════════════════════════════════════════════╗
║         WebSource Downloader — GOD LEVEL v4.0                       ║
║         API Dev     : @MANDAL4482                                    ║
║         API Updates : @MANDAL4482                                    ║
║                                                                      ║
║  NEW in v4.0:                                                        ║
║  ✅ SSE Real-time Progress Streaming  (/zip/stream)                  ║
║  ✅ Cookie / Header Auth Support      (&cookies= &auth_header=)      ║
║  ✅ Batch Multi-URL Download          (POST /batch)                  ║
║  ✅ Incremental / Resume Download     (&resume=true)                 ║
║  ✅ Diff Mode — only changed files    (&diff=true)                   ║
║  ✅ Content Extraction JSON           (&extract=true)                ║
║  ✅ /jobs — Active job tracker        (/jobs)                        ║
║  ✅ /batch/status/<id>                (batch job status)             ║
║                                                                      ║
║  ALL v3 features kept:                                               ║
║  Unlimited | Multi-page | JS Render | 4 Upload Backends |            ║
║  Telegram | Cache | Proxy | Screenshot | Offline HTML |              ║
║  Password ZIP | Webhook | History Log | Sitemap | Dedup             ║
╚══════════════════════════════════════════════════════════════════════╝

Install dependencies:
    pip install fastapi uvicorn aiohttp aiofiles beautifulsoup4 lxml \
                playwright python-telegram-bot aiofiles zipfile36 \
                sse-starlette --break-system-packages
    playwright install chromium
"""

import os, re, asyncio, zipfile, tempfile, time, uuid, socket
import threading, shutil, hashlib, json, mimetypes, random, logging
from contextlib import asynccontextmanager
from urllib.parse import urljoin, urlparse, unquote
from typing import Dict, Set, Optional, List, AsyncGenerator
from datetime import datetime
from pathlib import Path

import aiohttp
import aiofiles
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query, HTTPException, Request, Header, Body
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
import uvicorn

# ── SSE (Server-Sent Events) ──────────────────────────────────────────────────
try:
    from sse_starlette.sse import EventSourceResponse
    SSE_LIB = True
except ImportError:
    SSE_LIB = False

# ── uvloop ────────────────────────────────────────────────────────────────────
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    UVLOOP = True
except ImportError:
    UVLOOP = False

# ── Playwright (optional) ─────────────────────────────────────────────────────
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT = True
except ImportError:
    PLAYWRIGHT = False

# ── python-telegram-bot (optional) ───────────────────────────────────────────
try:
    from telegram import Bot as TGBot
    TELEGRAM_LIB = True
except ImportError:
    TELEGRAM_LIB = False


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
class Config:
    API_KEYS: str       = os.environ.get("API_KEYS", "")
    TG_BOT_TOKEN: str   = os.environ.get("TG_BOT_TOKEN", "")
    TG_CHAT_ID: str     = os.environ.get("TG_CHAT_ID", "")

    BASE_DIR: str       = "/tmp/websource_files"
    CACHE_DIR: str      = "/tmp/websource_cache"
    RESUME_DIR: str     = "/tmp/websource_resume"    # NEW: resume state storage
    DIFF_DIR: str       = "/tmp/websource_diff"      # NEW: diff hash storage
    LOG_FILE: str       = "/tmp/websource_history.json"

    FILE_EXPIRY_SEC: int   = 900
    CACHE_EXPIRY_SEC: int  = 3600

    MAX_PAGES: int          = 500
    MAX_CONCURRENT_DL: int  = 100
    MAX_RETRIES: int        = 4
    DEFAULT_DEPTH: int      = 1
    MAX_BATCH_URLS: int     = 20   # NEW: max URLs in one batch request

    UPLOAD_BACKENDS = [
        "https://tmpfiles.org/api/v1/upload",
        "https://file.io/?expires=1d",
        "https://0x0.st",
        "https://api.gofile.io/uploadFile",
    ]
    PROXIES: list = json.loads(os.environ.get("PROXIES", "[]"))


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
log = logging.getLogger("websource")

for d in [Config.BASE_DIR, Config.CACHE_DIR, Config.RESUME_DIR, Config.DIFF_DIR]:
    os.makedirs(d, exist_ok=True)

STORE: Dict[str, Dict]      = {}
CACHE: Dict[str, Dict]      = {}
JOB_PROGRESS: Dict[str, Dict] = {}   # NEW: live job progress tracker
BATCH_JOBS: Dict[str, Dict]   = {}   # NEW: batch job tracker


# ─────────────────────────────────────────────────────────────────────────────
# User-Agent pool
# ─────────────────────────────────────────────────────────────────────────────
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
    'Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1',
]
def get_ua(): return random.choice(USER_AGENTS)


# ─────────────────────────────────────────────────────────────────────────────
# History logger
# ─────────────────────────────────────────────────────────────────────────────
def log_history(entry: dict):
    try:
        history = []
        if os.path.exists(Config.LOG_FILE):
            with open(Config.LOG_FILE, 'r') as f:
                history = json.load(f)
        history.append(entry)
        history = history[-1000:]
        with open(Config.LOG_FILE, 'w') as f:
            json.dump(history, f, indent=2)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Telegram notifier
# ─────────────────────────────────────────────────────────────────────────────
async def send_telegram(message: str):
    if not TELEGRAM_LIB or not Config.TG_BOT_TOKEN or not Config.TG_CHAT_ID:
        return
    try:
        bot = TGBot(token=Config.TG_BOT_TOKEN)
        await bot.send_message(chat_id=Config.TG_CHAT_ID, text=message, parse_mode='HTML')
    except Exception as e:
        log.warning(f"Telegram notify failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Webhook notifier
# ─────────────────────────────────────────────────────────────────────────────
async def send_webhook(webhook_url: str, payload: dict):
    if not webhook_url:
        return
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(webhook_url, json=payload,
                               timeout=aiohttp.ClientTimeout(total=10))
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# JS Renderer (Playwright)
# ─────────────────────────────────────────────────────────────────────────────
async def render_js(url: str, screenshot: bool = False,
                    cookies: str = "", auth_header: str = "") -> tuple:
    """Returns (html_content, screenshot_bytes_or_None)"""
    if not PLAYWRIGHT:
        return None, None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=[
                '--no-sandbox', '--disable-setuid-sandbox',
                '--disable-dev-shm-usage', '--disable-gpu',
            ])
            ctx_kwargs = dict(
                user_agent=get_ua(),
                viewport={"width": 1920, "height": 1080},
                ignore_https_errors=True,
            )
            # NEW: inject extra HTTP headers (auth, custom)
            if auth_header:
                ctx_kwargs["extra_http_headers"] = {"Authorization": auth_header}

            context = await browser.new_context(**ctx_kwargs)

            # NEW: inject cookies for login sessions
            if cookies:
                parsed = urlparse(url)
                cookie_list = []
                for part in cookies.split(';'):
                    part = part.strip()
                    if '=' in part:
                        name, value = part.split('=', 1)
                        cookie_list.append({
                            "name": name.strip(),
                            "value": value.strip(),
                            "domain": parsed.netloc,
                            "path": "/",
                        })
                if cookie_list:
                    await context.add_cookies(cookie_list)

            page = await context.new_page()
            await page.goto(url, wait_until='networkidle', timeout=60000)
            await page.evaluate("""
                async () => {
                    await new Promise(resolve => {
                        let totalHeight = 0;
                        const distance = 300;
                        const timer = setInterval(() => {
                            window.scrollBy(0, distance);
                            totalHeight += distance;
                            if (totalHeight >= document.body.scrollHeight) {
                                clearInterval(timer);
                                resolve();
                            }
                        }, 80);
                    });
                }
            """)
            await asyncio.sleep(1.5)
            html = await page.content()
            sc_bytes = None
            if screenshot:
                sc_bytes = await page.screenshot(full_page=True, type='png')
            await browser.close()
            return html, sc_bytes
    except Exception as e:
        log.warning(f"JS render failed: {e}")
        return None, None


# ─────────────────────────────────────────────────────────────────────────────
# NEW: Content Extractor
# Pulls structured data from HTML — all text, links, images, meta
# ─────────────────────────────────────────────────────────────────────────────
def extract_content(soup: BeautifulSoup, url: str) -> dict:
    """Extract all meaningful content from parsed HTML."""
    domain = urlparse(url).netloc

    # All visible text
    for tag in soup(['script', 'style', 'noscript', 'head']):
        tag.decompose()
    text_content = ' '.join(soup.get_text(separator=' ', strip=True).split())

    # All internal + external links
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        if href.startswith(('mailto:', 'tel:', 'javascript:', '#')):
            continue
        full = urljoin(url, href)
        links.append({
            "text": a.get_text(strip=True)[:200],
            "url": full,
            "internal": urlparse(full).netloc == domain
        })

    # All images with alt text
    images = []
    for img in soup.find_all('img'):
        src = img.get('src', '').strip()
        if src and not src.startswith('data:'):
            images.append({
                "src": urljoin(url, src),
                "alt": img.get('alt', '').strip(),
                "title": img.get('title', '').strip(),
            })

    # Meta tags
    meta = {}
    for m in soup.find_all('meta'):
        name = m.get('name') or m.get('property') or m.get('http-equiv')
        content = m.get('content', '').strip()
        if name and content:
            meta[name] = content

    # Headings hierarchy
    headings = []
    for tag in soup.find_all(['h1','h2','h3','h4','h5','h6']):
        headings.append({"level": tag.name, "text": tag.get_text(strip=True)[:300]})

    # All emails found in page
    emails = list(set(re.findall(
        r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', text_content
    )))

    # All phone numbers (basic)
    phones = list(set(re.findall(
        r'[\+\(]?[0-9][0-9\s\-\(\)]{7,}[0-9]', text_content
    )))[:20]

    return {
        "url": url,
        "domain": domain,
        "extracted_at": datetime.utcnow().isoformat() + "Z",
        "title": soup.title.string.strip() if soup.title and soup.title.string else "",
        "text_content": text_content[:50000],   # first 50k chars
        "word_count": len(text_content.split()),
        "headings": headings,
        "links": links[:500],
        "images": images[:200],
        "emails": emails[:50],
        "phones": phones,
        "meta": meta,
    }


# ─────────────────────────────────────────────────────────────────────────────
# NEW: Resume State — save/load which URLs already downloaded
# ─────────────────────────────────────────────────────────────────────────────
def _resume_key(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()

def load_resume_state(url: str) -> dict:
    key = _resume_key(url)
    path = os.path.join(Config.RESUME_DIR, f"{key}.json")
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return {"downloaded": [], "failed": [], "pages_crawled": []}

def save_resume_state(url: str, state: dict):
    key = _resume_key(url)
    path = os.path.join(Config.RESUME_DIR, f"{key}.json")
    try:
        with open(path, 'w') as f:
            json.dump(state, f)
    except Exception:
        pass

def clear_resume_state(url: str):
    key = _resume_key(url)
    path = os.path.join(Config.RESUME_DIR, f"{key}.json")
    try:
        os.remove(path)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# NEW: Diff State — store content hashes of previous download
# ─────────────────────────────────────────────────────────────────────────────
def load_diff_state(url: str) -> dict:
    """Load previously saved URL→hash map for diff mode."""
    key = _resume_key(url)
    path = os.path.join(Config.DIFF_DIR, f"{key}_diff.json")
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_diff_state(url: str, hash_map: dict):
    """Save URL→hash map after a successful download."""
    key = _resume_key(url)
    path = os.path.join(Config.DIFF_DIR, f"{key}_diff.json")
    try:
        with open(path, 'w') as f:
            json.dump(hash_map, f)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Core Downloader
# ─────────────────────────────────────────────────────────────────────────────
class UrlDownloader:
    def __init__(self, imgFlg=True, linkFlg=True, scriptFlg=True,
                 max_retries=4, crawl_depth=1, only=None, exclude=None,
                 # NEW params
                 cookies: str = "",
                 auth_header: str = "",
                 resume: bool = False,
                 diff_mode: bool = False,
                 extract: bool = False,
                 job_id: str = "",
                 progress_cb=None):

        self.imgFlg = imgFlg
        self.linkFlg = linkFlg
        self.scriptFlg = scriptFlg
        self.max_retries = max_retries
        self.crawl_depth = crawl_depth
        self.only = set(only) if only else None
        self.exclude = set(exclude) if exclude else None
        self.cookies = cookies
        self.auth_header = auth_header
        self.resume = resume
        self.diff_mode = diff_mode
        self.do_extract = extract
        self.job_id = job_id
        self.progress_cb = progress_cb  # async callable(stage, detail, pct)

        self.soup = None

        self.extensions = {
            'css':'css','js':'js','mjs':'js','cjs':'js','ts':'js',
            'png':'images','jpg':'images','jpeg':'images','gif':'images',
            'svg':'images','ico':'images','webp':'images','avif':'images',
            'bmp':'images','tiff':'images','tif':'images','heic':'images',
            'woff':'fonts','woff2':'fonts','ttf':'fonts','eot':'fonts','otf':'fonts',
            'json':'json','jsonld':'json','geojson':'json',
            'xml':'xml','rss':'xml','atom':'xml',
            'txt':'txt','csv':'data','tsv':'data',
            'pdf':'documents','doc':'documents','docx':'documents',
            'xls':'documents','xlsx':'documents','ppt':'documents','pptx':'documents',
            'mov':'media','mp4':'media','webm':'media','ogg':'media',
            'mp3':'media','wav':'media','flac':'media','aac':'media',
            'm4a':'media','avi':'media','mkv':'media',
            'html':'pages','htm':'pages','xhtml':'pages',
            'webmanifest':'json','map':'maps','wasm':'wasm',
        }

        self.semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_DL)
        self.downloaded_files: Set[str] = set()
        self.file_hashes: Set[str] = set()
        self.failed_urls: Set[str] = set()
        self.crawled_pages: Set[str] = set()
        self.url_hash_map: dict = {}    # NEW: for diff tracking (url→md5)
        self.extracted_content: List[dict] = []  # NEW: content extraction results
        self.stats = {
            "downloaded": 0, "failed": 0, "total_bytes": 0,
            "duplicates_skipped": 0, "pages_crawled": 0,
            "resumed_skipped": 0, "diff_skipped": 0,   # NEW
        }

    async def _emit(self, stage: str, detail: str = "", pct: int = 0):
        """Emit progress update."""
        if self.job_id and self.job_id in JOB_PROGRESS:
            JOB_PROGRESS[self.job_id].update({
                "stage": stage, "detail": detail, "percent": pct,
                "stats": dict(self.stats),
                "ts": time.time(),
            })
        if self.progress_cb:
            try:
                await self.progress_cb(stage, detail, pct)
            except Exception:
                pass

    # ── Main entry ────────────────────────────────────────────────────────────
    async def savePage(self, url, pagefolder, session,
                       use_js=False, screenshot=False, proxy=None):
        try:
            os.makedirs(pagefolder, exist_ok=True)
            all_file_paths = []

            # NEW: Load resume state
            resume_state = {}
            if self.resume:
                resume_state = load_resume_state(url)
                self.downloaded_files.update(resume_state.get("downloaded", []))
                self.failed_urls.update(resume_state.get("failed", []))
                previously_crawled = set(resume_state.get("pages_crawled", []))
                self.stats["resumed_skipped"] = len(self.downloaded_files)
                log.info(f"Resume: skipping {len(self.downloaded_files)} already downloaded")
            else:
                previously_crawled = set()

            # NEW: Load diff state
            prev_hash_map = {}
            if self.diff_mode:
                prev_hash_map = load_diff_state(url)
                log.info(f"Diff mode: {len(prev_hash_map)} previous hashes loaded")

            pages_queue = [(url, 0)]
            self.crawled_pages.add(url)
            base_domain = urlparse(url).netloc

            await self._emit("crawling", f"Starting: {url}", 2)

            total_pages_estimate = 1
            pages_done = 0

            while pages_queue:
                current_url, current_depth = pages_queue.pop(0)

                # Resume: skip already crawled pages
                if self.resume and current_url in previously_crawled and current_url != url:
                    log.info(f"Resume: skipping page {current_url}")
                    continue

                log.info(f"Crawling [{current_depth}]: {current_url}")
                pages_done += 1
                pct = min(90, int((pages_done / max(total_pages_estimate, 1)) * 80))
                await self._emit("crawling", f"Page {pages_done}: {current_url}", pct)

                parsed = urlparse(current_url)
                path_slug = parsed.path.strip('/').replace('/', '_') or 'index'
                if current_depth == 0:
                    page_subfolder = pagefolder
                else:
                    page_subfolder = os.path.join(pagefolder, 'pages', path_slug)
                    os.makedirs(page_subfolder, exist_ok=True)

                html_content = None
                sc_bytes = None

                if use_js and PLAYWRIGHT:
                    html_content, sc_bytes = await render_js(
                        current_url, screenshot,
                        cookies=self.cookies,
                        auth_header=self.auth_header
                    )

                if not html_content:
                    html_content, sc_bytes_http = await self._fetch_html(
                        current_url, session,
                        screenshot=screenshot and not PLAYWRIGHT
                    )
                    if sc_bytes is None:
                        sc_bytes = sc_bytes_http

                if not html_content:
                    self.stats["failed"] += 1
                    continue

                self.stats["pages_crawled"] += 1

                try:
                    soup = BeautifulSoup(html_content, features="lxml")
                except Exception:
                    soup = BeautifulSoup(html_content, features="html.parser")

                if current_depth == 0:
                    self.soup = soup

                # NEW: Content extraction
                if self.do_extract:
                    extracted = extract_content(soup, current_url)
                    self.extracted_content.append(extracted)

                resource_urls = self._collect_all_resources(soup, current_url)
                resource_urls = [u for u in resource_urls
                                 if u and self._is_valid_url(u) and self._passes_filter(u)]

                # NEW: Diff filter — skip unchanged resources
                if self.diff_mode and prev_hash_map:
                    resource_urls = await self._diff_filter(resource_urls, session, prev_hash_map)

                if resource_urls:
                    await self._emit("downloading",
                                     f"{len(resource_urls)} resources on {current_url}", pct)
                    fp = await self._download_all_resources(
                        resource_urls, page_subfolder, session, proxy
                    )
                    all_file_paths.extend(fp)

                await self._update_html_paths(soup, current_url, page_subfolder)
                self._patch_offline(soup, current_url)

                if sc_bytes:
                    sc_path = os.path.join(page_subfolder, '_screenshot.png')
                    with open(sc_path, 'wb') as f:
                        f.write(sc_bytes)
                    all_file_paths.append(sc_path)

                fname = 'index.html' if current_depth == 0 else f"{path_slug}.html"
                html_path = os.path.join(page_subfolder, fname)
                html_bytes = soup.prettify('utf-8')
                async with aiofiles.open(html_path, 'wb') as f:
                    await f.write(html_bytes)
                all_file_paths.append(html_path)

                if current_depth < self.crawl_depth:
                    internal_links = self._extract_internal_links(soup, current_url, base_domain)
                    for link in internal_links:
                        if (link not in self.crawled_pages
                                and len(self.crawled_pages) < Config.MAX_PAGES):
                            self.crawled_pages.add(link)
                            pages_queue.append((link, current_depth + 1))
                            total_pages_estimate = max(total_pages_estimate,
                                                       len(self.crawled_pages))

                # NEW: save resume state after each page
                if self.resume:
                    save_resume_state(url, {
                        "downloaded": list(self.downloaded_files),
                        "failed": list(self.failed_urls),
                        "pages_crawled": list(self.crawled_pages),
                    })

            # NEW: Save diff hash map for next run
            if self.diff_mode:
                save_diff_state(url, self.url_hash_map)

            # NEW: Clear resume state on success
            if self.resume:
                clear_resume_state(url)

            await self._emit("zipping", "Creating ZIP archive", 92)
            return True, None, all_file_paths

        except Exception as e:
            return False, f"Failed: {str(e)}", []

    # ── NEW: Diff filter — HEAD-check each resource for changes ──────────────
    async def _diff_filter(self, resource_urls: List[str], session,
                           prev_hash_map: dict) -> List[str]:
        """Return only URLs whose ETag/Last-Modified changed or are new."""
        changed = []
        for ru in resource_urls:
            prev = prev_hash_map.get(ru, {})
            if not prev:
                changed.append(ru)
                continue
            try:
                async with session.head(
                    ru, headers={'User-Agent': get_ua()},
                    timeout=aiohttp.ClientTimeout(total=10),
                    ssl=False, allow_redirects=True
                ) as resp:
                    etag = resp.headers.get('ETag', '')
                    last_mod = resp.headers.get('Last-Modified', '')
                    if etag and etag == prev.get('etag'):
                        self.stats["diff_skipped"] += 1
                        continue
                    if last_mod and last_mod == prev.get('last_modified'):
                        self.stats["diff_skipped"] += 1
                        continue
                    changed.append(ru)
            except Exception:
                changed.append(ru)
        log.info(f"Diff: {len(changed)} changed / {len(resource_urls)} total")
        return changed

    # ── Fetch HTML with cookie + auth header support ──────────────────────────
    async def _fetch_html(self, url, session, screenshot=False):
        headers = self._make_headers(url)
        # NEW: inject cookies
        if self.cookies:
            headers['Cookie'] = self.cookies
        # NEW: inject auth header
        if self.auth_header:
            headers['Authorization'] = self.auth_header

        for attempt in range(self.max_retries):
            try:
                async with session.get(
                    url, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=60),
                    allow_redirects=True, ssl=False
                ) as resp:
                    if resp.status != 200:
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(1.5 ** attempt)
                            continue
                        return None, None
                    content = await resp.read()
                    if not content:
                        return None, None
                    ct = resp.headers.get('content-type', '').lower()
                    if not any(x in ct for x in ['text/html','application/xhtml','text/xml']):
                        return None, None
                    return content.decode('utf-8', errors='ignore'), None
            except Exception:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
        return None, None

    # ── Collect all resource types ────────────────────────────────────────────
    def _collect_all_resources(self, soup, base_url) -> List[str]:
        urls = set()
        for link in soup.find_all('link', href=True):
            rel = link.get('rel', [])
            if isinstance(rel, str): rel = [rel]
            if 'stylesheet' in rel or link.get('type') == 'text/css':
                urls.add(urljoin(base_url, link['href'].strip()))
            if any(r in rel for r in ['preload','prefetch','modulepreload','icon',
                                       'shortcut icon','apple-touch-icon','manifest',
                                       'mask-icon','alternate']):
                urls.add(urljoin(base_url, link['href'].strip()))
        for style in soup.find_all('style'):
            if style.string:
                urls.update(self._css_urls(style.string, base_url))
        for tag in soup.find_all(style=True):
            urls.update(self._css_urls(tag['style'], base_url))
        for script in soup.find_all('script', src=True):
            urls.add(urljoin(base_url, script['src'].strip()))
        IMG_ATTRS = ['src','data-src','data-lazy','data-original','data-lazy-src',
                     'data-bg','data-image','data-cover','data-poster','data-thumb']
        for img in soup.find_all(['img','source','video','audio']):
            for attr in IMG_ATTRS:
                val = img.get(attr, '').strip()
                if val: urls.add(urljoin(base_url, val))
            if img.get('srcset'):
                urls.update(self._parse_srcset(img['srcset'], base_url))
        for meta in soup.find_all('meta'):
            prop = meta.get('property','') or meta.get('name','')
            content = meta.get('content','').strip()
            if any(p in prop for p in ['image','og:','twitter:']) and content:
                if content.startswith(('http://','https://')):
                    urls.add(content)
                elif content.startswith('/'):
                    urls.add(urljoin(base_url, content))
        for script in soup.find_all('script'):
            if script.string:
                urls.update(self._inline_script_urls(script.string, base_url))
        for tag in soup.find_all(True):
            for attr, val in tag.attrs.items():
                if isinstance(val, str) and attr.startswith('data-') and val.strip():
                    v = val.strip()
                    if v.startswith(('http','/','./')):
                        urls.add(urljoin(base_url, v))
        for tag in soup.find_all(['object','embed'], src=True):
            urls.add(urljoin(base_url, tag['src']))
        for obj in soup.find_all('object', data=True):
            urls.add(urljoin(base_url, obj['data']))
        return [u for u in urls if u and self._is_valid_url(u)]

    def _extract_internal_links(self, soup, base_url, base_domain) -> List[str]:
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href'].strip()
            if not href or href.startswith(('mailto:','tel:','#','javascript:')):
                continue
            full = urljoin(base_url, href)
            parsed = urlparse(full)
            if parsed.netloc == base_domain and parsed.scheme in ('http','https'):
                clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                if clean not in self.crawled_pages:
                    links.append(clean)
        return links

    def _css_urls(self, css, base_url):
        urls = set()
        for u in re.findall(r'url\s*\(\s*["\']?([^"\'()]+)["\']?\s*\)', css, re.I):
            u = u.strip()
            if u and not u.startswith(('data:','blob:')):
                urls.add(urljoin(base_url, u))
        for u in re.findall(r'@import\s+["\']([^"\']+)["\']', css, re.I):
            urls.add(urljoin(base_url, u.strip()))
        return urls

    def _inline_script_urls(self, js_content, base_url):
        urls = set()
        exts = r'(js|css|png|jpg|jpeg|gif|svg|woff2?|ttf|eot|json|xml|webp|avif|mp4|webm|mp3|pdf|otf|wasm|map)'
        for m in re.findall(r'["\']([^"\']*\.' + exts + r')["\']', js_content, re.I):
            u = m[0].strip()
            if u and not u.startswith(('data:','blob:','javascript:')):
                urls.add(urljoin(base_url, u))
        for pat in [r'["\'](/[^"\']*\.chunk\.[^"\']+)["\']',
                    r'["\'](/_next/[^"\']+)["\']',
                    r'["\'](?:src|url)\s*[:=]\s*["\']([^"\']+)["\']']:
            for m in re.findall(pat, js_content, re.I):
                u = (m if isinstance(m, str) else m[0]).strip()
                if u and u.startswith('/'):
                    urls.add(urljoin(base_url, u))
        return urls

    def _parse_srcset(self, srcset, base_url):
        urls = set()
        for entry in srcset.split(','):
            parts = entry.strip().split()
            if parts: urls.add(urljoin(base_url, parts[0]))
        return urls

    def _patch_offline(self, soup, base_url):
        for base in soup.find_all('base'):
            base.decompose()
        if soup.head:
            m = soup.new_tag('meta')
            m['name'] = 'generator'
            m['content'] = 'WebSource Downloader v4 @MANDAL4482'
            soup.head.insert(0, m)
        for link in soup.find_all('link', rel='canonical'):
            link.decompose()

    def _passes_filter(self, url):
        ext = url.split('.')[-1].lower().split('?')[0] if '.' in url else ''
        folder = self.extensions.get(ext, 'assets')
        if self.only and folder not in self.only:
            return False
        if self.exclude and folder in self.exclude:
            return False
        return True

    def _is_valid_url(self, url):
        if not url or not isinstance(url, str): return False
        url = url.strip()
        return not url.startswith(('data:','blob:','javascript:','mailto:','tel:','#','about:','void('))

    async def _download_all_resources(self, resource_urls, pagefolder, session, proxy=None):
        tasks, file_paths = [], []
        for ru in resource_urls:
            if ru in self.downloaded_files or ru in self.failed_urls:
                continue
            self.downloaded_files.add(ru)
            fp = self._get_resource_path(ru, pagefolder)
            if fp:
                file_paths.append(fp)
                tasks.append(self._download_single(ru, fp, session, proxy))
        for i in range(0, len(tasks), 100):
            await asyncio.gather(*tasks[i:i+100], return_exceptions=True)
            await asyncio.sleep(0.05)
        return file_paths

    async def _download_single(self, url, file_path, session, proxy=None):
        async with self.semaphore:
            headers = {'User-Agent': get_ua(), 'Accept': '*/*',
                       'Referer': url, 'Accept-Encoding': 'identity'}
            # NEW: inject auth on resource downloads too
            if self.cookies:
                headers['Cookie'] = self.cookies
            if self.auth_header:
                headers['Authorization'] = self.auth_header

            for attempt in range(self.max_retries):
                try:
                    kw = {"proxy": proxy} if proxy else {}
                    async with session.get(
                        url, headers=headers,
                        timeout=aiohttp.ClientTimeout(total=45),
                        allow_redirects=True, ssl=False, **kw
                    ) as resp:
                        if resp.status not in [200, 206]:
                            if attempt < self.max_retries - 1:
                                await asyncio.sleep(1.5 ** attempt)
                                continue
                            self.failed_urls.add(url)
                            self.stats["failed"] += 1
                            return False

                        content = await resp.read()
                        if not content:
                            self.failed_urls.add(url)
                            self.stats["failed"] += 1
                            return False

                        h = hashlib.md5(content).hexdigest()
                        if h in self.file_hashes:
                            self.stats["duplicates_skipped"] += 1
                            return False
                        self.file_hashes.add(h)

                        # NEW: store url→{etag,last_modified,hash} for diff
                        self.url_hash_map[url] = {
                            "hash": h,
                            "etag": resp.headers.get('ETag', ''),
                            "last_modified": resp.headers.get('Last-Modified', ''),
                        }

                        os.makedirs(os.path.dirname(file_path), exist_ok=True)

                        if file_path.endswith('.css'):
                            try:
                                decoded = content.decode('utf-8', errors='ignore')
                                content = self._fix_css(decoded, url).encode('utf-8')
                            except Exception:
                                pass

                        async with aiofiles.open(file_path, 'wb') as f:
                            await f.write(content)

                        self.stats["downloaded"] += 1
                        self.stats["total_bytes"] += len(content)
                        return True

                except Exception:
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
            self.failed_urls.add(url)
            self.stats["failed"] += 1
            return False

    def _fix_css(self, css, base_url):
        def repl(m):
            u = m.group(1).strip('\'"')
            if not u.startswith(('data:','http://','https://')):
                return f'url("{urljoin(base_url, u)}")'
            return m.group(0)
        return re.sub(r'url\s*\(\s*["\']?([^"\'()]+)["\']?\s*\)', repl, css)

    async def _update_html_paths(self, soup, base_url, pagefolder):
        for img in soup.find_all('img'):
            if img.get('src'):
                lp = self._local_path(urljoin(base_url, img['src']), pagefolder)
                if lp: img['src'] = lp
        for link in soup.find_all('link'):
            if link.get('href'):
                lp = self._local_path(urljoin(base_url, link['href']), pagefolder)
                if lp: link['href'] = lp
        for script in soup.find_all('script'):
            if script.get('src'):
                lp = self._local_path(urljoin(base_url, script['src']), pagefolder)
                if lp: script['src'] = lp
        for source in soup.find_all('source'):
            if source.get('src'):
                lp = self._local_path(urljoin(base_url, source['src']), pagefolder)
                if lp: source['src'] = lp

    def _local_path(self, url, pagefolder):
        try:
            p = unquote(urlparse(url).path)
            if not p or p == '/': return None
            parts = p.strip('/').split('/')
            fname = parts[-1] or 'index'
            ext = fname.split('.')[-1].lower() if '.' in fname else (self._guess_ext(url) or 'bin')
            if '.' not in fname: fname = f"{fname}.{ext}"
            folder = self.extensions.get(ext, 'assets')
            if len(parts) > 1:
                return f"{folder}/{'/'.join(parts[:-1])}/{fname}"
            return f"{folder}/{fname}"
        except Exception:
            return None

    def _get_resource_path(self, url, pagefolder):
        try:
            parsed = urlparse(url)
            p = unquote(parsed.path)
            if not p or p == '/':
                h = abs(hash(url)) % 10_000_000
                p = f"/resource_{h}"
            parts = p.strip('/').split('/') or ['index']
            fname = parts[-1] or 'index'
            if '.' in fname and len(fname.split('.')[-1]) <= 10:
                ext = fname.split('.')[-1].lower()
            else:
                ext = self._guess_ext(url) or 'bin'
                fname = f"{fname}.{ext}"
            folder = self.extensions.get(ext, 'assets')
            if len(parts) > 1:
                target = os.path.join(pagefolder, folder, *parts[:-1])
            else:
                target = os.path.join(pagefolder, folder)
            os.makedirs(target, exist_ok=True)
            counter, base = 1, fname
            while os.path.exists(os.path.join(target, fname)):
                name, e = os.path.splitext(base)
                fname = f"{name}_{counter}{e}"
                counter += 1
            return os.path.join(target, fname)
        except Exception:
            return None

    def _guess_ext(self, url):
        u = url.lower()
        for k, v in [('css','css'),('style','css'),('\.js','js'),('/js/','js'),
                     ('script','js'),('wasm','wasm'),('png','png'),('jpg','jpg'),
                     ('jpeg','jpeg'),('gif','gif'),('svg','svg'),('webp','webp'),
                     ('ico','ico'),('avif','avif'),('woff2','woff2'),('woff','woff'),
                     ('ttf','ttf'),('otf','otf'),('eot','eot'),('json','json'),
                     ('manifest','json'),('xml','xml'),('rss','xml'),
                     ('mp4','mp4'),('mp3','mp3'),('pdf','pdf'),('.map','map')]:
            if k in u: return v
        return None

    def _make_headers(self, referer=None):
        h = {
            'User-Agent': get_ua(),
            'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'identity',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        if referer: h['Referer'] = referer
        return h


# ─────────────────────────────────────────────────────────────────────────────
# ZIP builder
# ─────────────────────────────────────────────────────────────────────────────
def create_zip(folder_path, password: Optional[str] = None):
    if not os.path.exists(folder_path):
        return None, 0
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.zip', dir=Config.BASE_DIR)
    tmp.close()
    file_count = 0
    with zipfile.ZipFile(tmp.name, 'w', zipfile.ZIP_DEFLATED,
                         compresslevel=6, allowZip64=True) as zf:
        for root, _, files in os.walk(folder_path):
            for file in files:
                try:
                    fp = os.path.join(root, file)
                    if not os.path.exists(fp): continue
                    arc = os.path.relpath(fp, folder_path)
                    if password:
                        zf.write(fp, arc, pwd=password.encode())
                    else:
                        zf.write(fp, arc)
                    file_count += 1
                except Exception:
                    continue
    if file_count == 0:
        os.unlink(tmp.name)
        return None, 0
    return tmp.name, file_count


# ─────────────────────────────────────────────────────────────────────────────
# Metadata JSON
# ─────────────────────────────────────────────────────────────────────────────
def write_meta(folder, url, stats, time_taken, extra: dict = None):
    meta = {
        "generator": "WebSource Downloader v4 @MANDAL4482",
        "url": url,
        "domain": urlparse(url).netloc,
        "downloaded_at": datetime.utcnow().isoformat() + "Z",
        "time_taken_seconds": round(time_taken, 2),
        "stats": stats,
    }
    if extra:
        meta.update(extra)
    try:
        with open(os.path.join(folder, '_websource_meta.json'), 'w') as f:
            json.dump(meta, f, indent=2)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Sitemap parser
# ─────────────────────────────────────────────────────────────────────────────
async def fetch_sitemap_urls(base_url: str, session) -> List[str]:
    urls = []
    sitemap_candidates = [
        urljoin(base_url, '/sitemap.xml'),
        urljoin(base_url, '/sitemap_index.xml'),
        urljoin(base_url, '/sitemap/sitemap.xml'),
        urljoin(base_url, '/robots.txt'),
    ]
    found_sitemaps = []
    for candidate in sitemap_candidates:
        try:
            async with session.get(candidate, timeout=aiohttp.ClientTimeout(total=15),
                                   ssl=False) as resp:
                if resp.status != 200: continue
                content = await resp.text(errors='ignore')
                if candidate.endswith('robots.txt'):
                    for line in content.splitlines():
                        if line.lower().startswith('sitemap:'):
                            found_sitemaps.append(line.split(':', 1)[1].strip())
                else:
                    found_sitemaps.append(candidate)
        except Exception:
            continue
    for sm in found_sitemaps[:5]:
        try:
            async with session.get(sm, timeout=aiohttp.ClientTimeout(total=15), ssl=False) as resp:
                if resp.status != 200: continue
                content = await resp.text(errors='ignore')
                soup = BeautifulSoup(content, 'xml')
                for loc in soup.find_all('loc'):
                    if loc.string:
                        urls.append(loc.string.strip())
        except Exception:
            continue
    return list(set(urls))[:Config.MAX_PAGES]


# ─────────────────────────────────────────────────────────────────────────────
# Multi-backend uploader
# ─────────────────────────────────────────────────────────────────────────────
async def upload_file(zip_path: str) -> Optional[str]:
    file_size = os.path.getsize(zip_path)
    log.info(f"Uploading {file_size/(1024*1024):.2f}MB to cloud...")
    for backend in Config.UPLOAD_BACKENDS:
        url = await _try_upload(zip_path, backend)
        if url:
            log.info(f"Uploaded to: {backend} → {url}")
            return url
        log.warning(f"Upload failed: {backend}")
    log.error("All upload backends failed")
    return None


async def _try_upload(zip_path: str, backend: str) -> Optional[str]:
    try:
        async with aiohttp.ClientSession() as session:
            with open(zip_path, 'rb') as f:
                fname = os.path.basename(zip_path)
                if 'tmpfiles.org' in backend:
                    data = aiohttp.FormData()
                    data.add_field('file', f, filename=fname, content_type='application/zip')
                    async with session.post(backend, data=data,
                                            timeout=aiohttp.ClientTimeout(total=600)) as resp:
                        if resp.status == 200:
                            r = await resp.json()
                            url = r.get('data', {}).get('url', '')
                            if url:
                                return url.replace('tmpfiles.org/', 'tmpfiles.org/dl/', 1)
                elif 'file.io' in backend:
                    data = aiohttp.FormData()
                    data.add_field('file', f, filename=fname, content_type='application/zip')
                    async with session.post(backend, data=data,
                                            timeout=aiohttp.ClientTimeout(total=600)) as resp:
                        if resp.status == 200:
                            r = await resp.json()
                            return r.get('link') or r.get('url')
                elif '0x0.st' in backend:
                    data = aiohttp.FormData()
                    data.add_field('file', f, filename=fname, content_type='application/zip')
                    async with session.post(backend, data=data,
                                            timeout=aiohttp.ClientTimeout(total=600)) as resp:
                        if resp.status == 200:
                            return (await resp.text()).strip()
                elif 'gofile.io' in backend:
                    data = aiohttp.FormData()
                    data.add_field('file', f, filename=fname, content_type='application/zip')
                    async with session.post(backend, data=data,
                                            timeout=aiohttp.ClientTimeout(total=600)) as resp:
                        if resp.status == 200:
                            r = await resp.json()
                            return r.get('data', {}).get('downloadPage')
    except Exception as e:
        log.debug(f"Upload error ({backend}): {e}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Proxy selector
# ─────────────────────────────────────────────────────────────────────────────
def get_proxy():
    if Config.PROXIES:
        return random.choice(Config.PROXIES)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Background cleaner
# ─────────────────────────────────────────────────────────────────────────────
def clean_expired_files():
    while True:
        time.sleep(60)
        now = time.time()
        for store in [STORE, CACHE]:
            dead = [k for k, v in list(store.items()) if now > v.get("exp", 0)]
            for k in dead:
                v = store.pop(k, {})
                try:
                    if v.get("path") and os.path.exists(v["path"]):
                        os.remove(v["path"])
                    if v.get("folder") and os.path.exists(v["folder"]):
                        shutil.rmtree(v["folder"], ignore_errors=True)
                except Exception:
                    pass
        # Clean finished jobs from JOB_PROGRESS (older than 10 min)
        dead_jobs = [k for k, v in list(JOB_PROGRESS.items())
                     if now - v.get("ts", now) > 600]
        for k in dead_jobs:
            JOB_PROGRESS.pop(k, None)


def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]; s.close(); return ip
    except Exception:
        return "127.0.0.1"


# ─────────────────────────────────────────────────────────────────────────────
# API Key auth
# ─────────────────────────────────────────────────────────────────────────────
def check_api_key(key: Optional[str]) -> bool:
    if not Config.API_KEYS:
        return True
    valid_keys = {k.strip() for k in Config.API_KEYS.split(',') if k.strip()}
    return key in valid_keys


# ─────────────────────────────────────────────────────────────────────────────
# Shared download executor (used by /zip, /zip/stream, /batch)
# ─────────────────────────────────────────────────────────────────────────────
async def run_download(
    url: str, base_url: str,
    depth: int, images: bool, css: bool, js: bool,
    js_render: bool, screenshot: bool, sitemap: bool,
    only_list, exclude_list, retries: int,
    filename: str, zip_password: str, webhook: str,
    proxy, cookies: str, auth_header: str,
    resume: bool, diff_mode: bool, extract: bool,
    job_id: str = "", progress_cb=None,
) -> dict:
    """Core download logic. Returns response_meta dict."""

    start_time = time.time()

    # Cache check
    cache_key = hashlib.md5(f"{url}|{depth}|{only_list}|{exclude_list}".encode()).hexdigest()
    if cache_key in CACHE and time.time() < CACHE[cache_key]["exp"]:
        cached = CACHE[cache_key]
        log.info(f"Cache HIT: {url}")
        return {**cached["meta"], "cached": True,
                "cache_age_seconds": int(time.time() - cached["created"])}

    fid = uuid.uuid4().hex
    pagefolder = os.path.join(Config.BASE_DIR, f"page_{fid}")

    try:
        connector = aiohttp.TCPConnector(
            limit=300, limit_per_host=100,
            ttl_dns_cache=600, ssl=False,
            enable_cleanup_closed=True
        )
        timeout = aiohttp.ClientTimeout(total=600, connect=30, sock_read=45)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout,
                                         auto_decompress=False) as session:
            downloader = UrlDownloader(
                imgFlg=images, linkFlg=css, scriptFlg=js,
                max_retries=retries, crawl_depth=depth,
                only=only_list, exclude=exclude_list,
                cookies=cookies, auth_header=auth_header,
                resume=resume, diff_mode=diff_mode,
                extract=extract,
                job_id=job_id, progress_cb=progress_cb,
            )

            if sitemap:
                sm_urls = await fetch_sitemap_urls(url, session)
                log.info(f"Sitemap found {len(sm_urls)} URLs")
                for sm_url in sm_urls[:Config.MAX_PAGES]:
                    downloader.crawled_pages.add(sm_url)

            success, error, file_paths = await downloader.savePage(
                url, pagefolder, session,
                use_js=js_render, screenshot=screenshot, proxy=proxy
            )

        if not success:
            shutil.rmtree(pagefolder, ignore_errors=True)
            return {"success": False, "error": error,
                    "api_dev": "@MANDAL4482", "api_updates": "@MANDAL4482"}

        # Save content extraction results
        if extract and downloader.extracted_content:
            ext_path = os.path.join(pagefolder, '_extracted_content.json')
            with open(ext_path, 'w') as f:
                json.dump(downloader.extracted_content, f, indent=2, ensure_ascii=False)

        write_meta(pagefolder, url, downloader.stats,
                   time.time() - start_time,
                   extra={"diff_mode": diff_mode, "resume_mode": resume,
                          "extraction_included": extract})

        zip_path, file_count = create_zip(pagefolder, password=zip_password or None)
        shutil.rmtree(pagefolder, ignore_errors=True)

        if not zip_path:
            return {"success": False, "error": "ZIP creation failed",
                    "api_dev": "@MANDAL4482", "api_updates": "@MANDAL4482"}

        if progress_cb:
            await progress_cb("uploading", "Uploading to cloud...", 95)

        cloud_url = await upload_file(zip_path)

        expiry = time.time() + Config.FILE_EXPIRY_SEC
        STORE[fid] = {"path": zip_path, "exp": expiry, "folder": pagefolder}

        zip_size = os.path.getsize(zip_path)
        domain = urlparse(url).netloc.replace('www.', '')
        time_taken = round(time.time() - start_time, 2)
        local_dl = f"{base_url}/download/{fid}"
        final_url = cloud_url or local_dl
        custom_name = (filename or f"websource_{domain}") + ".zip"

        response_meta = {
            "success": True,
            "file_id": fid,
            "download_url": final_url,
            "cloud_url": cloud_url,
            "local_download_url": local_dl,
            "domain": domain,
            "original_url": url,
            "filename": custom_name,
            "password_protected": bool(zip_password),
            "file_size_mb": round(zip_size / (1024*1024), 2),
            "file_size_bytes": zip_size,
            "file_count": file_count,
            "pages_crawled": downloader.stats["pages_crawled"],
            "resources_downloaded": downloader.stats["downloaded"],
            "resources_failed": downloader.stats["failed"],
            "duplicates_skipped": downloader.stats["duplicates_skipped"],
            "resumed_skipped": downloader.stats["resumed_skipped"],
            "diff_skipped": downloader.stats["diff_skipped"],
            "total_resource_bytes": downloader.stats["total_bytes"],
            "sitemap_used": sitemap,
            "js_rendered": js_render and PLAYWRIGHT,
            "screenshot_included": screenshot,
            "resume_mode": resume,
            "diff_mode": diff_mode,
            "extraction_included": extract,
            "auth_used": bool(cookies or auth_header),
            "time_taken_seconds": time_taken,
            "expires_in_seconds": Config.FILE_EXPIRY_SEC,
            "cached": False,
            "api_dev": "@MANDAL4482",
            "api_updates": "@MANDAL4482",
        }

        CACHE[cache_key] = {
            "meta": response_meta,
            "exp": time.time() + Config.CACHE_EXPIRY_SEC,
            "created": time.time(),
            "path": zip_path,
        }

        log_history({
            "url": url, "domain": domain,
            "time": datetime.utcnow().isoformat() + "Z",
            "size_mb": response_meta["file_size_mb"],
            "files": file_count,
            "time_seconds": time_taken,
            "download_url": final_url,
        })

        tg_msg = (
            f"✅ <b>Download Complete</b>\n"
            f"🌐 <b>URL:</b> {url}\n"
            f"📦 <b>Size:</b> {response_meta['file_size_mb']} MB\n"
            f"📄 <b>Files:</b> {file_count}\n"
            f"📝 <b>Pages:</b> {downloader.stats['pages_crawled']}\n"
            f"⏱ <b>Time:</b> {time_taken}s\n"
            f"🔗 <b>Link:</b> {final_url}\n"
            f"👤 @MANDAL4482"
        )
        asyncio.create_task(send_telegram(tg_msg))

        if webhook:
            asyncio.create_task(send_webhook(webhook, response_meta))

        log.info(f"✅ {domain} → {zip_size/(1024*1024):.2f}MB | "
                 f"{file_count} files | {downloader.stats['pages_crawled']} pages | "
                 f"{time_taken}s")

        if progress_cb:
            await progress_cb("done", final_url, 100)

        if job_id and job_id in JOB_PROGRESS:
            JOB_PROGRESS[job_id].update({
                "stage": "done",
                "percent": 100,
                "result": response_meta,
                "ts": time.time(),
            })

        return response_meta

    except Exception as e:
        log.exception(f"Fatal error: {url}")
        shutil.rmtree(pagefolder, ignore_errors=True)
        if job_id and job_id in JOB_PROGRESS:
            JOB_PROGRESS[job_id].update({"stage": "error", "detail": str(e), "ts": time.time()})
        return {"success": False, "error": str(e),
                "api_dev": "@MANDAL4482", "api_updates": "@MANDAL4482"}


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI
# ─────────────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.environ.get("VERCEL") != "1":
        threading.Thread(target=clean_expired_files, daemon=True).start()
        log.info("File cleaner started")
    yield
    log.info("Shutting down")

app = FastAPI(
    title="WebSource Downloader — GOD LEVEL v4",
    description="SSE | Batch | Resume | Diff | Auth | Extract | v3 all features",
    lifespan=lifespan
)


# ─────────────────────────────────────────────────────────────────────────────
# Common query params helper
# ─────────────────────────────────────────────────────────────────────────────
def _common_params(
    url: str, depth: int, images: bool, css: bool, js: bool,
    js_render: bool, screenshot: bool, sitemap: bool,
    only: str, exclude: str, retries: int,
    filename: str, zip_password: str, webhook: str,
    use_proxy: bool, key: str,
    cookies: str, auth_header: str,
    resume: bool, diff: bool, extract: bool,
):
    return {
        "url": url, "depth": depth, "images": images,
        "css": css, "js": js, "js_render": js_render,
        "screenshot": screenshot, "sitemap": sitemap,
        "only": only, "exclude": exclude, "retries": retries,
        "filename": filename, "zip_password": zip_password,
        "webhook": webhook, "use_proxy": use_proxy, "key": key,
        "cookies": cookies, "auth_header": auth_header,
        "resume": resume, "diff": diff, "extract": extract,
    }


# ─────────────────────────────────────────────────────────────────────────────
# /zip — Standard blocking endpoint (v3 compatible + new params)
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/zip")
async def zip_website(
    request: Request,
    url: str         = Query(...,    description="Target website URL"),
    depth: int       = Query(1,      description="Crawl depth 1-5", ge=1, le=5),
    images: bool     = Query(True),
    css: bool        = Query(True),
    js: bool         = Query(True),
    js_render: bool  = Query(False,  description="Playwright JS rendering"),
    screenshot: bool = Query(False,  description="Full-page screenshot"),
    sitemap: bool    = Query(False,  description="Parse sitemap.xml"),
    only: str        = Query("",     description="Whitelist: css,js,images,fonts,media,documents"),
    exclude: str     = Query("",     description="Blacklist: css,js,images,fonts,media,documents"),
    retries: int     = Query(4,      ge=1, le=5),
    filename: str    = Query(""),
    zip_password: str= Query(""),
    webhook: str     = Query(""),
    use_proxy: bool  = Query(False),
    key: str         = Query(""),
    # ── NEW params ────────────────────────────────────────────────────────────
    cookies: str     = Query("",     description="Cookie string: name=val;name2=val2"),
    auth_header: str = Query("",     description="Authorization header value e.g. Bearer TOKEN"),
    resume: bool     = Query(False,  description="Resume interrupted download"),
    diff: bool       = Query(False,  description="Diff mode: only download changed files"),
    extract: bool    = Query(False,  description="Extract content JSON (text,links,images,meta)"),
):
    if not check_api_key(key or None):
        return JSONResponse(status_code=401, content={
            "success": False, "error": "Invalid or missing API key",
            "api_dev": "@MANDAL4482",
        })

    if not url.startswith(('http://','https://')):
        url = f"https://{url}"

    base_url = str(request.base_url).rstrip('/')
    if request.headers.get("x-forwarded-proto"):
        base_url = f"{request.headers['x-forwarded-proto']}://{request.headers.get('host', request.url.netloc)}"

    proxy     = get_proxy() if use_proxy else None
    only_list = [x.strip() for x in only.split(',')    if x.strip()] or None
    excl_list = [x.strip() for x in exclude.split(',') if x.strip()] or None

    result = await run_download(
        url=url, base_url=base_url,
        depth=depth, images=images, css=css, js=js,
        js_render=js_render, screenshot=screenshot, sitemap=sitemap,
        only_list=only_list, exclude_list=excl_list,
        retries=retries, filename=filename, zip_password=zip_password,
        webhook=webhook, proxy=proxy,
        cookies=cookies, auth_header=auth_header,
        resume=resume, diff_mode=diff, extract=extract,
    )
    status_code = 200 if result.get("success") else 400
    return JSONResponse(status_code=status_code, content=result)


# ─────────────────────────────────────────────────────────────────────────────
# NEW: /zip/stream — SSE real-time progress streaming
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/zip/stream")
async def zip_stream(
    request: Request,
    url: str         = Query(...),
    depth: int       = Query(1, ge=1, le=5),
    images: bool     = Query(True),
    css: bool        = Query(True),
    js: bool         = Query(True),
    js_render: bool  = Query(False),
    screenshot: bool = Query(False),
    sitemap: bool    = Query(False),
    only: str        = Query(""),
    exclude: str     = Query(""),
    retries: int     = Query(4, ge=1, le=5),
    filename: str    = Query(""),
    zip_password: str= Query(""),
    webhook: str     = Query(""),
    use_proxy: bool  = Query(False),
    key: str         = Query(""),
    cookies: str     = Query(""),
    auth_header: str = Query(""),
    resume: bool     = Query(False),
    diff: bool       = Query(False),
    extract: bool    = Query(False),
):
    """
    SSE streaming endpoint.
    Connect with EventSource in browser or curl --no-buffer.
    Events: progress, done, error
    Each event data is JSON.

    Example:
        curl -N "http://localhost:3648/zip/stream?url=https://example.com"
    """
    if not check_api_key(key or None):
        async def err():
            yield {"data": json.dumps({"event": "error", "error": "Unauthorized"})}
        return EventSourceResponse(err()) if SSE_LIB else JSONResponse(
            status_code=401, content={"error": "Unauthorized"})

    if not url.startswith(('http://','https://')):
        url = f"https://{url}"

    base_url = str(request.base_url).rstrip('/')
    proxy     = get_proxy() if use_proxy else None
    only_list = [x.strip() for x in only.split(',')    if x.strip()] or None
    excl_list = [x.strip() for x in exclude.split(',') if x.strip()] or None

    job_id = uuid.uuid4().hex
    JOB_PROGRESS[job_id] = {
        "job_id": job_id, "url": url,
        "stage": "queued", "percent": 0,
        "detail": "", "stats": {}, "ts": time.time(),
    }

    # SSE event queue
    queue: asyncio.Queue = asyncio.Queue()

    async def progress_cb(stage: str, detail: str, pct: int):
        payload = {
            "event": "progress",
            "job_id": job_id,
            "stage": stage,
            "detail": detail,
            "percent": pct,
            "stats": JOB_PROGRESS.get(job_id, {}).get("stats", {}),
            "ts": time.time(),
        }
        await queue.put(payload)

    async def run_task():
        result = await run_download(
            url=url, base_url=base_url,
            depth=depth, images=images, css=css, js=js,
            js_render=js_render, screenshot=screenshot, sitemap=sitemap,
            only_list=only_list, exclude_list=excl_list,
            retries=retries, filename=filename, zip_password=zip_password,
            webhook=webhook, proxy=proxy,
            cookies=cookies, auth_header=auth_header,
            resume=resume, diff_mode=diff, extract=extract,
            job_id=job_id, progress_cb=progress_cb,
        )
        event = "done" if result.get("success") else "error"
        await queue.put({"event": event, "job_id": job_id, **result})
        await queue.put(None)  # sentinel

    asyncio.create_task(run_task())

    async def event_generator() -> AsyncGenerator:
        # Send job started event
        yield {
            "data": json.dumps({
                "event": "started",
                "job_id": job_id,
                "url": url,
                "ts": time.time(),
            })
        }
        while True:
            if await request.is_disconnected():
                log.info(f"SSE client disconnected: {job_id}")
                break
            try:
                item = await asyncio.wait_for(queue.get(), timeout=30)
            except asyncio.TimeoutError:
                # Heartbeat
                yield {"data": json.dumps({"event": "heartbeat", "job_id": job_id})}
                continue
            if item is None:
                break
            yield {"data": json.dumps(item)}
            if item.get("event") in ("done", "error"):
                break

    if SSE_LIB:
        return EventSourceResponse(event_generator())
    else:
        # Fallback: plain streaming JSON if sse-starlette not installed
        async def plain_stream():
            async for event in event_generator():
                yield event["data"] + "\n"
        return StreamingResponse(plain_stream(), media_type="text/plain")


# ─────────────────────────────────────────────────────────────────────────────
# NEW: POST /batch — download multiple URLs concurrently
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/batch")
async def batch_download(
    request: Request,
    body: dict = Body(...),
):
    """
    Download multiple URLs in one request.
    Body JSON:
    {
        "urls": ["https://site1.com", "https://site2.com"],
        "key": "API_KEY",
        "depth": 1,
        "images": true,
        "css": true,
        "js": true,
        "cookies": "",
        "auth_header": "",
        "webhook": "",
        "concurrent": 3
    }
    Returns a batch_id. Poll /batch/status/<batch_id> for results.
    """
    key = body.get("key", "")
    if not check_api_key(key or None):
        return JSONResponse(status_code=401, content={"success": False, "error": "Unauthorized"})

    urls = body.get("urls", [])
    if not urls or not isinstance(urls, list):
        return JSONResponse(status_code=400,
                            content={"success": False, "error": "urls list required"})
    if len(urls) > Config.MAX_BATCH_URLS:
        return JSONResponse(status_code=400, content={
            "success": False,
            "error": f"Max {Config.MAX_BATCH_URLS} URLs per batch"
        })

    base_url = str(request.base_url).rstrip('/')
    batch_id = uuid.uuid4().hex
    concurrent = min(int(body.get("concurrent", 3)), 5)

    # Config from body
    cfg = {
        "depth":       int(body.get("depth", 1)),
        "images":      bool(body.get("images", True)),
        "css":         bool(body.get("css", True)),
        "js":          bool(body.get("js", True)),
        "js_render":   bool(body.get("js_render", False)),
        "screenshot":  bool(body.get("screenshot", False)),
        "sitemap":     bool(body.get("sitemap", False)),
        "only_list":   [x.strip() for x in body.get("only", "").split(',') if x.strip()] or None,
        "exclude_list":[x.strip() for x in body.get("exclude","").split(',') if x.strip()] or None,
        "retries":     int(body.get("retries", 4)),
        "filename":    body.get("filename", ""),
        "zip_password":body.get("zip_password", ""),
        "webhook":     body.get("webhook", ""),
        "cookies":     body.get("cookies", ""),
        "auth_header": body.get("auth_header", ""),
        "resume":      bool(body.get("resume", False)),
        "diff_mode":   bool(body.get("diff", False)),
        "extract":     bool(body.get("extract", False)),
    }

    BATCH_JOBS[batch_id] = {
        "batch_id": batch_id,
        "total": len(urls),
        "completed": 0,
        "failed": 0,
        "results": {},
        "status": "running",
        "started_at": datetime.utcnow().isoformat() + "Z",
        "urls": urls,
    }

    async def run_batch():
        sem = asyncio.Semaphore(concurrent)
        async def one(url: str):
            async with sem:
                if not url.startswith(('http://','https://')):
                    url_full = f"https://{url}"
                else:
                    url_full = url
                result = await run_download(
                    url=url_full, base_url=base_url,
                    proxy=get_proxy() if body.get("use_proxy") else None,
                    **cfg,
                )
                BATCH_JOBS[batch_id]["results"][url] = result
                if result.get("success"):
                    BATCH_JOBS[batch_id]["completed"] += 1
                else:
                    BATCH_JOBS[batch_id]["failed"] += 1
        await asyncio.gather(*[one(u) for u in urls], return_exceptions=True)
        BATCH_JOBS[batch_id]["status"] = "done"
        BATCH_JOBS[batch_id]["finished_at"] = datetime.utcnow().isoformat() + "Z"

    asyncio.create_task(run_batch())

    return JSONResponse(content={
        "success": True,
        "batch_id": batch_id,
        "total_urls": len(urls),
        "concurrent": concurrent,
        "status_url": f"{base_url}/batch/status/{batch_id}",
        "api_dev": "@MANDAL4482",
    })


# ─────────────────────────────────────────────────────────────────────────────
# NEW: /batch/status/<batch_id>
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/batch/status/{batch_id}")
async def batch_status(batch_id: str, key: str = Query("")):
    if not check_api_key(key or None):
        raise HTTPException(401, "Unauthorized")
    if batch_id not in BATCH_JOBS:
        raise HTTPException(404, "Batch job not found")
    job = BATCH_JOBS[batch_id]
    return JSONResponse(content={
        **job,
        "progress_percent": int((job["completed"] + job["failed"]) / max(job["total"], 1) * 100),
        "api_dev": "@MANDAL4482",
    })


# ─────────────────────────────────────────────────────────────────────────────
# NEW: /jobs — list all active/recent jobs
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/jobs")
async def list_jobs(key: str = Query("")):
    if not check_api_key(key or None):
        raise HTTPException(401, "Unauthorized")
    return JSONResponse(content={
        "active_jobs": [
            {k: v for k, v in job.items() if k != "result"}
            for job in JOB_PROGRESS.values()
        ],
        "batch_jobs": [
            {k: v for k, v in job.items() if k != "results"}
            for job in BATCH_JOBS.values()
        ],
        "total_active": len(JOB_PROGRESS),
        "total_batch": len(BATCH_JOBS),
        "api_dev": "@MANDAL4482",
    })


# ─────────────────────────────────────────────────────────────────────────────
# /download/<id>
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/download/{file_id}")
async def download_file(file_id: str, filename: str = Query("")):
    if file_id not in STORE:
        raise HTTPException(404, "File not found or expired")
    data = STORE[file_id]
    if time.time() > data["exp"]:
        try: os.remove(data["path"])
        except Exception: pass
        STORE.pop(file_id, None)
        raise HTTPException(404, "File expired")
    if not os.path.exists(data["path"]):
        STORE.pop(file_id, None)
        raise HTTPException(404, "File not found on disk")
    fname = filename or f"websource_{file_id}.zip"
    if not fname.endswith('.zip'): fname += '.zip'
    return FileResponse(data["path"], media_type="application/zip", filename=fname)


# ─────────────────────────────────────────────────────────────────────────────
# /history
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/history")
async def get_history(limit: int = Query(50, ge=1, le=1000), key: str = Query("")):
    if not check_api_key(key or None):
        raise HTTPException(401, "Unauthorized")
    try:
        if not os.path.exists(Config.LOG_FILE):
            return JSONResponse(content={"history": [], "total": 0})
        with open(Config.LOG_FILE, 'r') as f:
            history = json.load(f)
        return JSONResponse(content={"history": history[-limit:], "total": len(history),
                                     "api_dev": "@MANDAL4482"})
    except Exception:
        return JSONResponse(content={"history": [], "total": 0})


# ─────────────────────────────────────────────────────────────────────────────
# /cache/clear
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/cache/clear")
async def clear_cache(key: str = Query("")):
    if not check_api_key(key or None):
        raise HTTPException(401, "Unauthorized")
    count = len(CACHE)
    CACHE.clear()
    return {"success": True, "cleared": count, "api_dev": "@MANDAL4482"}


# ─────────────────────────────────────────────────────────────────────────────
# NEW: /resume/clear — clear saved resume state for a URL
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/resume/clear")
async def clear_resume(url: str = Query(...), key: str = Query("")):
    if not check_api_key(key or None):
        raise HTTPException(401, "Unauthorized")
    clear_resume_state(url)
    return {"success": True, "message": f"Resume state cleared for {url}", "api_dev": "@MANDAL4482"}


# ─────────────────────────────────────────────────────────────────────────────
# NEW: /diff/clear — reset diff baseline for a URL
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/diff/clear")
async def clear_diff(url: str = Query(...), key: str = Query("")):
    if not check_api_key(key or None):
        raise HTTPException(401, "Unauthorized")
    key_hash = hashlib.md5(url.encode()).hexdigest()
    path = os.path.join(Config.DIFF_DIR, f"{key_hash}_diff.json")
    removed = False
    if os.path.exists(path):
        os.remove(path)
        removed = True
    return {"success": True, "removed": removed,
            "message": f"Diff baseline cleared for {url}", "api_dev": "@MANDAL4482"}


# ─────────────────────────────────────────────────────────────────────────────
# /status
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/status")
async def status():
    return {
        "status": "online",
        "version": "4.0 GOD LEVEL",
        "uvloop": UVLOOP,
        "playwright_available": PLAYWRIGHT,
        "telegram_available": TELEGRAM_LIB,
        "sse_available": SSE_LIB,
        "proxy_count": len(Config.PROXIES),
        "auth_enabled": bool(Config.API_KEYS),
        "stored_files": len(STORE),
        "cached_results": len(CACHE),
        "active_jobs": len(JOB_PROGRESS),
        "batch_jobs": len(BATCH_JOBS),
        "max_concurrent_downloads": Config.MAX_CONCURRENT_DL,
        "max_pages_per_crawl": Config.MAX_PAGES,
        "max_batch_urls": Config.MAX_BATCH_URLS,
        "api_dev": "@MANDAL4482",
        "api_updates": "@MANDAL4482",
    }


# ─────────────────────────────────────────────────────────────────────────────
# /
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "api": "WebSource Downloader — GOD LEVEL v4.0",
        "api_dev": "@MANDAL4482",
        "api_updates": "@MANDAL4482",
        "whats_new_v4": [
            "✅ SSE Real-time Progress  GET /zip/stream?url=...",
            "✅ Cookie Auth             &cookies=session=abc;csrf=xyz",
            "✅ Header Auth             &auth_header=Bearer TOKEN",
            "✅ Batch Multi-URL         POST /batch  {urls:[...]}",
            "✅ Resume Interrupted DL   &resume=true",
            "✅ Diff Mode (only changes)&diff=true",
            "✅ Content Extraction JSON &extract=true",
            "✅ Job Tracker             GET /jobs",
            "✅ Batch Status            GET /batch/status/<id>",
            "✅ Resume State Clear      GET /resume/clear?url=...",
            "✅ Diff Baseline Clear     GET /diff/clear?url=...",
        ],
        "endpoints": {
            "GET  /zip":                 "Standard download (blocking)",
            "GET  /zip/stream":          "SSE streaming download with real-time progress",
            "POST /batch":               "Batch multi-URL download",
            "GET  /batch/status/<id>":   "Batch job status",
            "GET  /jobs":                "All active jobs",
            "GET  /download/<id>":       "Local file download",
            "GET  /history":             "Download history",
            "GET  /cache/clear":         "Clear result cache",
            "GET  /resume/clear":        "Clear resume state for URL",
            "GET  /diff/clear":          "Clear diff baseline for URL",
            "GET  /status":              "Server status",
        },
        "new_parameters": {
            "cookies":     "Cookie string passed to all requests: session=abc;csrf=xyz",
            "auth_header": "Authorization header: Bearer TOKEN or Basic base64",
            "resume":      "Resume an interrupted download (true/false)",
            "diff":        "Diff mode — only download changed resources (true/false)",
            "extract":     "Extract content JSON (text, links, images, meta) into ZIP (true/false)",
        },
        "examples": {
            "sse_stream":     "/zip/stream?url=https://example.com",
            "with_cookies":   "/zip?url=https://example.com&cookies=session=abc123;csrf=xyz",
            "with_auth":      "/zip?url=https://example.com&auth_header=Bearer%20YOUR_TOKEN",
            "resume":         "/zip?url=https://example.com&resume=true",
            "diff":           "/zip?url=https://example.com&diff=true",
            "extract":        "/zip?url=https://example.com&extract=true",
            "batch_post": {
                "url": "POST /batch",
                "body": {
                    "urls": ["https://site1.com","https://site2.com","https://site3.com"],
                    "depth": 1, "images": True, "css": True, "js": True,
                    "concurrent": 3, "key": "YOUR_API_KEY"
                }
            },
            "full_v3_compat": "/zip?url=https://example.com&depth=3&sitemap=true&js_render=true&screenshot=true",
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    local_ip = get_local_ip()
    loop_type = "uvloop" if UVLOOP else "asyncio"

    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║         WebSource Downloader — GOD LEVEL v4.0                       ║
╠══════════════════════════════════════════════════════════════════════╣
║  Loop       : {loop_type:<54}║
║  Playwright : {"Available ✅" if PLAYWRIGHT else "Not installed ❌":<54}║
║  Telegram   : {"Available ✅" if TELEGRAM_LIB else "Not installed ❌":<54}║
║  SSE        : {"Available ✅" if SSE_LIB else "pip install sse-starlette ❌":<54}║
║  Auth       : {"Enabled ✅" if Config.API_KEYS else "Disabled (open)":<54}║
║  Proxies    : {len(Config.PROXIES):<54}║
╠══════════════════════════════════════════════════════════════════════╣
║  Local    : http://127.0.0.1:3648                                   ║
║  Network  : http://{local_ip}:3648{" "*(48-len(local_ip))}║
╠══════════════════════════════════════════════════════════════════════╣
║  Standard : /zip?url=https://example.com                            ║
║  SSE Live : /zip/stream?url=https://example.com                     ║
║  Batch    : POST /batch  {{urls:[...]}}                               ║
║  Auth     : /zip?url=...&cookies=session=abc&auth_header=Bearer TOK ║
║  Resume   : /zip?url=...&resume=true                                ║
║  Diff     : /zip?url=...&diff=true                                  ║
║  Extract  : /zip?url=...&extract=true                               ║
╠══════════════════════════════════════════════════════════════════════╣
║  API Dev     : @MANDAL4482                                          ║
║  API Updates : @MANDAL4482                                          ║
╚══════════════════════════════════════════════════════════════════════╝
""")

    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=3648,
        log_level="info",
        access_log=True,
        loop=loop_type,
        workers=1,
    )
