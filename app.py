"""
╔══════════════════════════════════════════════════════════════════════╗
║   WebSource Downloader — SIMPLE & STRONG (v5.0)                      ║
║   Ek hi kaam: website ka URL do → pura site (HTML/CSS/JS/img)        ║
║   asli browser ki tarah download hoke ZIP ban jaye.                  ║
╚══════════════════════════════════════════════════════════════════════╝

Install:
    pip install fastapi uvicorn playwright beautifulsoup4 httpx --break-system-packages
    playwright install chromium
    playwright install-deps chromium     # linux/render pe zaroori

Run:
    python app.py            (PORT env se port badal sakte ho)

Use:
    GET /zip?url=https://example.com
    GET /zip?url=https://example.com&pages=10        (multi-page crawl)
    GET /download/<file_id>                          (zip download)

Kyun ye kaam karta hai jahan purana fail hota tha:
  - Har page asli Chromium me khulta hai (JS/React/Vercel sites bhi).
  - Resources hum guess nahi karte — jo browser khud network par load
    karta hai, wahi bytes response se seedha save karte hain.
  - Uske baad HTML/CSS ke andar ke saare link local file paths me
    rewrite hote hain, to ZIP offline khulti hai.
"""

import asyncio
import hashlib
import mimetypes
import os
import re
import shutil
import tempfile
import time
import uuid
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import unquote, urljoin, urlparse, urlsplit

from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse

from playwright.async_api import async_playwright

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

FILE_TTL = 3600  # 1 hour
STORE: Dict[str, dict] = {}
WORK_ROOT = Path(tempfile.gettempdir()) / "websource_v5"
WORK_ROOT.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="WebSource Downloader v5")


# ──────────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────────
def norm_url(u: str) -> str:
    u = u.strip()
    if not u:
        raise HTTPException(400, "url missing")
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    return u


def strip_frag(u: str) -> str:
    s = urlsplit(u)
    return s._replace(fragment="").geturl()


def same_site(a: str, b: str) -> bool:
    ha = urlparse(a).netloc.lower().removeprefix("www.")
    hb = urlparse(b).netloc.lower().removeprefix("www.")
    return ha == hb


def ext_from(url: str, content_type: str) -> str:
    path = urlparse(url).path
    e = os.path.splitext(path)[1].lower()
    if e and len(e) <= 6 and re.fullmatch(r"\.[a-z0-9]+", e):
        return e
    ct = (content_type or "").split(";")[0].strip()
    return mimetypes.guess_extension(ct) or ".bin"


def safe_name(url: str, content_type: str) -> str:
    """Deterministic, collision-free, filesystem-safe asset filename."""
    p = urlparse(url)
    base = os.path.basename(unquote(p.path)) or "asset"
    base = re.sub(r"[^A-Za-z0-9._-]", "_", base)[:60]
    stem = os.path.splitext(base)[0] or "asset"
    e = ext_from(url, content_type)
    h = hashlib.md5(url.encode()).hexdigest()[:8]
    return f"{stem}_{h}{e}"


def page_filename(url: str, is_entry: bool) -> str:
    if is_entry:
        return "index.html"
    p = urlparse(url)
    path = unquote(p.path).strip("/")
    if not path:
        return "index.html"
    name = re.sub(r"[^A-Za-z0-9._-]", "_", path.replace("/", "_"))[:70]
    if not name.endswith((".html", ".htm")):
        name += ".html"
    h = hashlib.md5(strip_frag(url).encode()).hexdigest()[:6]
    return f"{os.path.splitext(name)[0]}_{h}.html"


ASSET_CT = ("text/css", "javascript", "image/", "font", "video", "audio", "json", "octet-stream")


def is_asset_ct(ct: str) -> bool:
    ct = (ct or "").lower()
    return any(k in ct for k in ASSET_CT)


# ──────────────────────────────────────────────────────────────────────
# core downloader
# ──────────────────────────────────────────────────────────────────────
class SiteDownloader:
    def __init__(self, entry: str, max_pages: int, timeout_ms: int = 60000):
        self.entry = strip_frag(norm_url(entry))
        self.max_pages = max(1, min(max_pages, 50))
        self.timeout_ms = timeout_ms
        self.out = WORK_ROOT / uuid.uuid4().hex
        self.assets_dir = self.out / "assets"
        self.out.mkdir(parents=True, exist_ok=True)
        self.assets_dir.mkdir(parents=True, exist_ok=True)

        self.asset_map: Dict[str, str] = {}       # abs url -> assets/<file>
        self.page_map: Dict[str, str] = {}        # abs url -> <file>.html
        self.saved_assets: Set[str] = set()
        self.failed: List[str] = []
        self.total_bytes = 0

    # ---------- browser capture ----------
    async def run(self) -> dict:
        t0 = time.time()
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            ctx = await browser.new_context(
                user_agent=UA,
                viewport={"width": 1440, "height": 900},
                locale="en-US",
                ignore_https_errors=True,
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                },
            )
            # bot-detection ko chakma
            await ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            )

            queue: List[str] = [self.entry]
            seen: Set[str] = {self.entry}
            pages: List[Tuple[str, str, str]] = []  # (url, html, filename)

            while queue and len(pages) < self.max_pages:
                url = queue.pop(0)
                html, links = await self._capture_page(ctx, url)
                if html is None:
                    self.failed.append(url)
                    continue
                fname = page_filename(url, is_entry=(url == self.entry))
                self.page_map[url] = fname
                pages.append((url, html, fname))

                if len(pages) < self.max_pages:
                    for l in links:
                        l = strip_frag(l)
                        if l in seen or not same_site(l, self.entry):
                            continue
                        if re.search(r"\.(zip|pdf|png|jpe?g|gif|svg|mp4|css|js|ico|webp)$", urlparse(l).path, re.I):
                            continue
                        seen.add(l)
                        queue.append(l)

            await ctx.close()
            await browser.close()

        if not pages:
            raise HTTPException(502, f"Page load nahi hua: {self.entry}. Site block kar rahi hai ya URL galat hai.")

        # rewrite + write html
        for url, html, fname in pages:
            fixed = self._rewrite_html(html, url)
            (self.out / fname).write_text(fixed, encoding="utf-8")

        # rewrite urls inside css files
        self._rewrite_css_files()

        return {
            "pages": len(pages),
            "assets": len(self.saved_assets),
            "failed": len(self.failed),
            "bytes": self.total_bytes,
            "seconds": round(time.time() - t0, 2),
        }

    async def _capture_page(self, ctx, url: str):
        page = await ctx.new_page()
        pending = []

        async def on_response(resp):
            try:
                u = strip_frag(resp.url)
                if not u.startswith(("http://", "https://")) or u in self.saved_assets:
                    return
                if resp.status >= 400:
                    return
                ct = (resp.headers or {}).get("content-type", "")
                if not is_asset_ct(ct):
                    return
                body = await resp.body()
                if not body:
                    return
                name = safe_name(u, ct)
                (self.assets_dir / name).write_bytes(body)
                self.asset_map[u] = f"assets/{name}"
                self.saved_assets.add(u)
                self.total_bytes += len(body)
            except Exception:
                pass

        page.on("response", lambda r: pending.append(asyncio.ensure_future(on_response(r))))

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            # lazy-load images ko trigger karo
            await page.evaluate(
                """async () => {
                    const step = 600;
                    for (let y = 0; y < document.body.scrollHeight; y += step) {
                        window.scrollTo(0, y);
                        await new Promise(r => setTimeout(r, 60));
                    }
                    window.scrollTo(0, 0);
                }"""
            )
            await page.wait_for_timeout(1200)
            html = await page.content()
            links = await page.eval_on_selector_all(
                "a[href]", "els => els.map(e => e.href)"
            )
        except Exception:
            html, links = None, []

        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        await page.close()
        return html, links or []

    # ---------- rewriting ----------
    def _local_for(self, abs_url: str) -> Optional[str]:
        abs_url = strip_frag(abs_url)
        if abs_url in self.page_map:
            return self.page_map[abs_url]
        if abs_url in self.asset_map:
            return self.asset_map[abs_url]
        return None

    def _rewrite_html(self, html: str, base: str) -> str:
        soup = BeautifulSoup(html, "html.parser")

        attrs = ("src", "href", "poster", "data-src")
        for tag in soup.find_all(True):
            for a in attrs:
                v = tag.get(a)
                if not v or v.startswith(("data:", "mailto:", "tel:", "javascript:", "#")):
                    continue
                local = self._local_for(urljoin(base, v))
                if local:
                    tag[a] = local
            if tag.get("srcset"):
                parts = []
                for chunk in tag["srcset"].split(","):
                    bits = chunk.strip().split()
                    if not bits:
                        continue
                    local = self._local_for(urljoin(base, bits[0]))
                    bits[0] = local or bits[0]
                    parts.append(" ".join(bits))
                tag["srcset"] = ", ".join(parts)
            if tag.get("integrity"):
                del tag["integrity"]
            if tag.get("crossorigin"):
                del tag["crossorigin"]

        # inline <style> ke andar ke url()
        for st in soup.find_all("style"):
            if st.string:
                st.string.replace_with(self._rewrite_css_text(st.string, base, in_root=True))

        # <base> tag hata do, warna offline paths tootenge
        for b in soup.find_all("base"):
            b.decompose()

        return str(soup)

    def _rewrite_css_text(self, css: str, base: str, in_root: bool) -> str:
        prefix = "" if in_root else ""

        def repl(m):
            raw = m.group(2).strip()
            if raw.startswith(("data:", "#")):
                return m.group(0)
            local = self._local_for(urljoin(base, raw))
            if not local:
                return m.group(0)
            if not in_root:
                local = local.split("/")[-1]  # css file assets/ ke andar hai
            return f"{m.group(1)}{prefix}{local}{m.group(3)}"

        css = re.sub(r"(url\(\s*['\"]?)([^'\")]+)(['\"]?\s*\))", repl, css)
        return css

    def _rewrite_css_files(self):
        for url, rel in list(self.asset_map.items()):
            if not rel.endswith(".css"):
                continue
            f = self.out / rel
            try:
                text = f.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            f.write_text(self._rewrite_css_text(text, url, in_root=False), encoding="utf-8")

    # ---------- zip ----------
    def zip_it(self, domain: str) -> Path:
        zip_path = WORK_ROOT / f"websource_{domain}_{uuid.uuid4().hex[:8]}.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as z:
            for p in self.out.rglob("*"):
                if p.is_file():
                    z.write(p, p.relative_to(self.out))
        shutil.rmtree(self.out, ignore_errors=True)
        return zip_path


def cleanup():
    now = time.time()
    for fid, meta in list(STORE.items()):
        if now - meta["created"] > FILE_TTL:
            Path(meta["path"]).unlink(missing_ok=True)
            STORE.pop(fid, None)


# ──────────────────────────────────────────────────────────────────────
# API
# ──────────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {
        "name": "WebSource Downloader v5",
        "usage": "/zip?url=https://example.com&pages=1",
        "note": "Sirf URL do — pura site browser ki tarah download hokar ZIP milega.",
    }


@app.get("/health")
def health():
    return {"ok": True, "jobs": len(STORE)}


@app.get("/zip")
async def zip_website(
    url: str = Query(..., description="Website URL"),
    pages: int = Query(1, ge=1, le=50, description="Kitne pages crawl karne hain"),
):
    cleanup()
    target = norm_url(url)
    dl = SiteDownloader(target, max_pages=pages)
    try:
        stats = await dl.run()
    except HTTPException:
        shutil.rmtree(dl.out, ignore_errors=True)
        raise
    except Exception as e:
        shutil.rmtree(dl.out, ignore_errors=True)
        raise HTTPException(500, f"Download fail: {type(e).__name__}: {e}")

    domain = urlparse(target).netloc.replace(":", "_")
    zip_path = dl.zip_it(domain)
    size = zip_path.stat().st_size

    if stats["pages"] == 0 or size < 500:
        zip_path.unlink(missing_ok=True)
        raise HTTPException(502, "Kuch download nahi hua — site ne block kar diya.")

    fid = uuid.uuid4().hex
    STORE[fid] = {
        "path": str(zip_path),
        "filename": f"websource_{domain}.zip",
        "created": time.time(),
    }

    return JSONResponse(
        {
            "success": True,
            "file_id": fid,
            "download_url": f"/download/{fid}",
            "domain": domain,
            "original_url": target,
            "filename": STORE[fid]["filename"],
            "pages_downloaded": stats["pages"],
            "resources_downloaded": stats["assets"],
            "resources_failed": stats["failed"],
            "zip_size_bytes": size,
            "zip_size_mb": round(size / 1048576, 2),
            "total_resource_bytes": stats["bytes"],
            "time_taken_seconds": stats["seconds"],
            "expires_in_seconds": FILE_TTL,
        }
    )


@app.get("/download/{file_id}")
def download(file_id: str):
    cleanup()
    meta = STORE.get(file_id)
    if not meta or not Path(meta["path"]).exists():
        raise HTTPException(404, "File expired ya nahi mili")
    return FileResponse(meta["path"], filename=meta["filename"], media_type="application/zip")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
