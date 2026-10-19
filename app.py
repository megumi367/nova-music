# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════
  NOVA MUSIC — Backend Server
═══════════════════════════════════════════════════════════════════
  Chức năng:
    • SoundCloud streaming proxy (direct + HLS)
    • Search + Suggest
    • Lyrics API
    • Party Mode (Nghe chung đồng bộ)
    • Download MP3 (yt-dlp + ffmpeg)
    • PWA (manifest, service worker, icon)
    • Auto cleanup + Thread-safe cache
═══════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

# ─────────── Standard Library ───────────
import os
import re
import io
import time
import uuid
import json
import base64
import shutil
import signal
import logging
import tempfile
import threading
import subprocess
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

# ─────────── Third-party ───────────
import requests
import yt_dlp
from flask import (
    Flask, request, jsonify, Response, stream_with_context,
    send_file, send_from_directory, abort
)
from flask_compress import Compress


# ═══════════════════════════════════════════════════════════════════
# 1. LOGGING
# ═══════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)-7s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('nova')


# ═══════════════════════════════════════════════════════════════════
# 2. CONSTANTS
# ═══════════════════════════════════════════════════════════════════
BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR     = os.path.join(BASE_DIR, 'templates')
STATIC_DIR       = os.path.join(BASE_DIR, 'static')
DOWNLOAD_DIR     = os.path.join(BASE_DIR, 'downloads')

SERVER_START     = time.time()
CACHE_TTL        = 7000       # 2 giờ — cache stream info
CLEANUP_INTERVAL = 3600       # 1 giờ — chạy cleanup
DOWNLOAD_TTL     = 3600       # 1 giờ — xoá file download
PARTY_TIMEOUT    = 60         # 60s — timeout member inactive
PARTY_ROOM_TTL   = 3600       # 1 giờ — xoá phòng không hoạt động

# SoundCloud
SC_UA       = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
               'AppleWebKit/537.36 (KHTML, like Gecko) '
               'Chrome/122.0.0.0 Safari/537.36')
SC_REFERER  = 'https://soundcloud.com/'
SC_ORIGIN   = 'https://soundcloud.com'

# Placeholder icon
PLACEHOLDER_THUMB = (
    "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' "
    "viewBox='0 0 24 24' fill='%23333'%3E%3Cpath "
    "d='M12 3v10.55A4 4 0 1 0 14 17V7h4V3h-6z'/%3E%3C/svg%3E"
)


# ═══════════════════════════════════════════════════════════════════
# 3. APP INIT
# ═══════════════════════════════════════════════════════════════════
app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)
app.config['JSON_AS_ASCII'] = False
app.config['MAX_CONTENT_LENGTH'] = 32 * 1024 * 1024  # 32MB
Compress(app)

# Đảm bảo thư mục tồn tại
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════
# 4. GLOBAL STATE (thread-safe)
# ═══════════════════════════════════════════════════════════════════
_stream_cache: Dict[str, Dict[str, Any]]   = {}   # {key: {data, ts}}
_download_tasks: Dict[str, Dict[str, Any]] = {}   # {tid: {status, ...}}
_party_rooms: Dict[str, Dict[str, Any]]    = {}   # {code: {members, playing, ...}}

_stream_lock  = threading.RLock()
_download_lock = threading.RLock()
_party_lock   = threading.RLock()

# Thread pool cho yt-dlp blocking calls
_executor = ThreadPoolExecutor(max_workers=16, thread_name_prefix='nova-worker')


# ═══════════════════════════════════════════════════════════════════
# 5. COOKIES (Optional — cho track age-restricted / geo-blocked)
# ═══════════════════════════════════════════════════════════════════
COOKIES_FILE: Optional[str] = None

def _init_cookies() -> None:
    global COOKIES_FILE
    env_val = os.environ.get('NOVA_COOKIES')
    if not env_val:
        return
    try:
        if os.path.exists(env_val):
            COOKIES_FILE = env_val
            log.info(f"Cookies: loaded from path → {env_val}")
        else:
            tmp = tempfile.NamedTemporaryFile(
                mode='w', suffix='.txt', delete=False, encoding='utf-8'
            )
            tmp.write(env_val)
            tmp.close()
            COOKIES_FILE = tmp.name
            log.info("Cookies: loaded from env var")
    except Exception as e:
        log.warning(f"Cookies: failed to load — {e}")

_init_cookies()


# ═══════════════════════════════════════════════════════════════════
# 6. FFMPEG DETECTION
# ═══════════════════════════════════════════════════════════════════
FFMPEG_OK = False

def _detect_ffmpeg() -> None:
    global FFMPEG_OK
    try:
        result = subprocess.run(
            ['ffmpeg', '-version'],
            capture_output=True, timeout=3, check=True
        )
        FFMPEG_OK = True
        version_line = result.stdout.decode(errors='ignore').split('\n')[0]
        log.info(f"FFmpeg: OK → {version_line[:60]}")
    except (FileNotFoundError, subprocess.CalledProcessError):
        log.warning("FFmpeg: NOT FOUND — tải nhạc sẽ dùng định dạng gốc")
    except Exception as e:
        log.warning(f"FFmpeg: detect error — {e}")

_detect_ffmpeg()


# ═══════════════════════════════════════════════════════════════════
# 7. BACKGROUND CLEANUP WORKER
# ═══════════════════════════════════════════════════════════════════
def _cleanup_worker() -> None:
    """Chạy mỗi giờ: dọn cache, file tải, task, party room."""
    while True:
        time.sleep(CLEANUP_INTERVAL)
        now = time.time()
        try:
            # ─── Xoá file download cũ ───
            if os.path.isdir(DOWNLOAD_DIR):
                for name in os.listdir(DOWNLOAD_DIR):
                    path = os.path.join(DOWNLOAD_DIR, name)
                    try:
                        if os.path.isfile(path) and os.stat(path).st_mtime < now - DOWNLOAD_TTL:
                            os.remove(path)
                            log.debug(f"Cleanup file: {name}")
                    except OSError:
                        pass

            # ─── Xoá download tasks xong ───
            with _download_lock:
                for tid in list(_download_tasks.keys()):
                    task = _download_tasks[tid]
                    if task.get('status') in ('done', 'error'):
                        if now - task.get('_ts', now) > DOWNLOAD_TTL:
                            del _download_tasks[tid]

            # ─── Xoá stream cache cũ ───
            with _stream_lock:
                for key in list(_stream_cache.keys()):
                    if now - _stream_cache[key]['ts'] > CACHE_TTL * 2:
                        del _stream_cache[key]

            # ─── Xoá party room không dùng ───
            with _party_lock:
                for code in list(_party_rooms.keys()):
                    room = _party_rooms[code]
                    last_seen = max(
                        (m['seen'] for m in room['members'].values()),
                        default=0
                    )
                    if now - last_seen > PARTY_ROOM_TTL:
                        del _party_rooms[code]
                        log.info(f"Cleanup party room: {code}")

            log.debug("Cleanup cycle finished")
        except Exception as e:
            log.error(f"Cleanup error: {e}")

# Khởi động cleanup thread
threading.Thread(target=_cleanup_worker, daemon=True, name='nova-cleaner').start()


# ═══════════════════════════════════════════════════════════════════
# 8. YT-DLP HELPERS
# ═══════════════════════════════════════════════════════════════════
def _base_opts() -> Dict[str, Any]:
    """Trả về dict options chung cho yt-dlp."""
    opts: Dict[str, Any] = {
        'quiet': True,
        'no_warnings': True,
        'nocheckcertificate': True,
        'socket_timeout': 20,
        'retries': 3,
        'fragment_retries': 3,
        'noplaylist': True,
        'extractor_retries': 3,
        'geo_bypass': True,
        'http_headers': {
            'User-Agent': SC_UA,
            'Referer': SC_REFERER,
            'Origin': SC_ORIGIN,
        },
    }
    if COOKIES_FILE and os.path.exists(COOKIES_FILE):
        opts['cookiefile'] = COOKIES_FILE
    return opts


def _run_with_timeout(fn, timeout: float = 30, *args, **kwargs):
    """Chạy fn trong executor với timeout."""
    future = _executor.submit(fn, *args, **kwargs)
    try:
        return future.result(timeout=timeout)
    except FutureTimeoutError:
        raise TimeoutError(f"Timeout sau {timeout}s")
    except Exception:
        raise


def _normalize_sc_url(vid: str) -> str:
    """
    Chuẩn hoá ID/URL SoundCloud về dạng canonical:
      • '123456' → https://soundcloud.com/tracks/123456
      • 'soundcloud:tracks:123456' → https://soundcloud.com/tracks/123456
      • 'artist/track-name' → https://soundcloud.com/artist/track-name
    """
    if not vid:
        raise ValueError("Empty video_id")

    vid = str(vid).strip()

    # Numeric ID
    if vid.isdigit():
        return f"https://soundcloud.com/tracks/{vid}"

    decoded = urllib.parse.unquote(vid)

    # soundcloud:tracks:NNN
    m = re.search(r'soundcloud[:\/]tracks[:\/](\d+)', decoded)
    if m:
        return f"https://soundcloud.com/tracks/{m.group(1)}"

    # api.soundcloud.com/tracks/NNN
    m2 = re.search(r'api\.soundcloud\.com/tracks/(\d+)', decoded)
    if m2:
        return f"https://soundcloud.com/tracks/{m2.group(1)}"

    # artist/slug
    if not decoded.startswith('http') and '/' in decoded:
        return f"https://soundcloud.com/{decoded}"

    return decoded


def _extract_info(url: str, timeout: float = 25) -> Dict[str, Any]:
    """Gọi yt-dlp extract_info (blocking, có timeout)."""
    def _do():
        opts = _base_opts()
        opts['format'] = 'bestaudio/best'
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)

    return _run_with_timeout(_do, timeout=timeout)


def _extract_stream(vid: str, timeout: float = 25) -> Dict[str, Any]:
    """Extract stream URL để phát nhạc."""
    url = _normalize_sc_url(vid)
    info = _extract_info(url, timeout=timeout)

    # Lấy stream URL
    stream_url = info.get('url')
    protocol = info.get('protocol', '')

    if not stream_url:
        formats = info.get('formats') or []
        for fmt in reversed(formats):
            if fmt.get('url'):
                stream_url = fmt['url']
                protocol = fmt.get('protocol', '')
                break

    if not stream_url:
        raise ValueError("Không tìm thấy URL stream")

    is_hls = 'm3u8' in stream_url.lower() or 'm3u8' in protocol.lower()

    # Thumbnail
    thumbnails = info.get('thumbnails') or []
    thumb_url = info.get('thumbnail') or (
        thumbnails[-1].get('url') if thumbnails else PLACEHOLDER_THUMB
    )

    return {
        'title':     info.get('title') or 'Untitled',
        'thumbnail': thumb_url,
        'duration':  info.get('duration'),
        'uploader':  (
            info.get('uploader')
            or info.get('channel')
            or info.get('creator')
            or 'Unknown'
        ),
        'id':        str(info.get('id') or vid),
        'url':       stream_url,
        'is_hls':    is_hls,
        'protocol':  protocol,
    }


def _get_cached_stream(vid: str) -> Dict[str, Any]:
    """Lấy stream info, ưu tiên cache."""
    key = f"sc:{vid}"
    now = time.time()

    with _stream_lock:
        if key in _stream_cache:
            entry = _stream_cache[key]
            if now - entry['ts'] < CACHE_TTL:
                return entry['data']

    # Cache miss → extract
    data = _extract_stream(vid)

    with _stream_lock:
        _stream_cache[key] = {'data': data, 'ts': now}

    return data


def _do_search(query: str, limit: int = 24) -> List[Dict[str, Any]]:
    """Tìm kiếm SoundCloud."""
    def _do():
        opts = _base_opts()
        opts.update({
            'extract_flat': True,
            'skip_download': True,
            'playlistend': limit,
        })
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(f"scsearch{limit}:{query}", download=False)

    try:
        info = _run_with_timeout(_do, timeout=20)
    except Exception as e:
        log.warning(f"Search '{query}' failed: {e}")
        return []

    results: List[Dict[str, Any]] = []
    for entry in (info.get('entries') or []):
        if not entry:
            continue

        entry_url = entry.get('webpage_url') or entry.get('url')
        if not entry_url or 'api.soundcloud.com' in entry_url:
            continue

        thumbnails = entry.get('thumbnails') or []
        thumb = next(
            (t['url'] for t in reversed(thumbnails) if t.get('url')),
            PLACEHOLDER_THUMB
        )

        duration = entry.get('duration')
        if duration and duration > 100000:
            duration = duration / 1000

        results.append({
            'id':       entry_url,
            'title':    entry.get('title') or 'Untitled',
            'duration': duration,
            'thumb':    thumb,
            'channel':  entry.get('uploader') or entry.get('channel') or 'SoundCloud',
        })

    return results


# ═══════════════════════════════════════════════════════════════════
# 9. BASE64 HELPERS (dùng cho HLS proxy token)
# ═══════════════════════════════════════════════════════════════════
def _b64_encode(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip('=')


def _b64_decode(s: str) -> str:
    return base64.urlsafe_b64decode(s + '=' * (-len(s) % 4)).decode()


# ═══════════════════════════════════════════════════════════════════
# 10. ROUTES — STATIC & PWA
# ═══════════════════════════════════════════════════════════════════
@app.route('/')
def route_index():
    """Serve index.html."""
    return send_from_directory(TEMPLATE_DIR, 'index.html')


@app.route('/manifest.json')
def route_manifest():
    """PWA Manifest."""
    data = {
        'name': 'Nova Music',
        'short_name': 'Nova',
        'description': 'Cosmic Sound Music Player',
        'start_url': '/',
        'scope': '/',
        'display': 'standalone',
        'orientation': 'portrait',
        'background_color': '#05030a',
        'theme_color': '#a855f7',
        'lang': 'vi',
        'icons': [
            {
                'src': '/icon.svg',
                'sizes': 'any',
                'type': 'image/svg+xml',
                'purpose': 'any maskable'
            },
        ]
    }
    return jsonify(data), 200, {'Cache-Control': 'public, max-age=86400'}


@app.route('/icon.svg')
def route_icon():
    """PWA icon (SVG)."""
    svg = '''<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg">
<defs>
  <radialGradient id="bg" cx="50%" cy="42%" r="65%">
    <stop offset="0%" stop-color="#3a1060"/>
    <stop offset="55%" stop-color="#0a0520"/>
    <stop offset="100%" stop-color="#02010a"/>
  </radialGradient>
  <filter id="glow" x="-40%" y="-40%" width="180%" height="180%">
    <feGaussianBlur stdDeviation="7" result="b"/>
    <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
  </filter>
</defs>
<circle cx="256" cy="256" r="256" fill="url(#bg)"/>
<circle cx="256" cy="216" r="148" fill="none" stroke="#a855f7"
        stroke-width="10" filter="url(#glow)" opacity="0.85"/>
<g fill="#000">
  <path d="M 158 512 L 168 372 C 173 318, 200 288, 230 276
           L 256 296 L 282 276 C 312 288, 339 318, 344 372 L 354 512 Z"/>
  <rect x="234" y="236" width="44" height="66"/>
  <ellipse cx="256" cy="196" rx="66" ry="74"/>
  <path d="M 192 160 L 210 100 L 228 160 Z"/>
  <path d="M 234 150 L 256 60 L 278 150 Z"/>
  <path d="M 284 160 L 302 100 L 320 160 Z"/>
</g>
<g filter="url(#glow)">
  <path d="M 256 120 L 270 184 L 340 196 L 270 208 L 256 272
           L 242 208 L 172 196 L 242 184 Z" fill="#fff"/>
</g>
</svg>'''
    return Response(svg, mimetype='image/svg+xml')


@app.route('/sw.js')
def route_sw():
    """Service worker."""
    sw = '''
const CACHE = 'nova-v1';
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(
        keys.filter(k => k !== CACHE).map(k => caches.delete(k))
      ))
      .then(() => self.clients.claim())
  );
});
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);
  if (url.pathname === '/' || url.pathname === '/sw.js' ||
      url.pathname.startsWith('/api/')) {
    return;
  }
  if (url.pathname === '/icon.svg' || url.pathname === '/manifest.json') {
    e.respondWith(
      caches.match(e.request).then(cached =>
        cached || fetch(e.request).then(r => {
          const clone = r.clone();
          caches.open(CACHE).then(cc => cc.put(e.request, clone));
          return r;
        })
      )
    );
  }
});
'''
    return Response(sw, mimetype='application/javascript')


@app.route('/api/ping')
def route_ping():
    """Health check."""
    return jsonify({
        'pong': True,
        'uptime': round(time.time() - SERVER_START, 2),
    })


@app.route('/api/version')
def route_version():
    """Version info."""
    return jsonify({
        'version': '1.0.0',
        'start':   SERVER_START,
        'ffmpeg':  FFMPEG_OK,
        'cookies': bool(COOKIES_FILE),
    })


# ═══════════════════════════════════════════════════════════════════
# 11. ROUTES — SEARCH & SUGGEST
# ═══════════════════════════════════════════════════════════════════
@app.route('/api/search')
def route_search():
    """Tìm kiếm SoundCloud."""
    q = (request.args.get('q') or '').strip()
    if not q:
        return jsonify([])

    try:
        results = _do_search(q)
        return jsonify(results)
    except Exception as e:
        log.error(f"Search error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/suggest')
def route_suggest():
    """Gợi ý tìm kiếm (Google suggest)."""
    q = (request.args.get('q') or '').strip()
    if not q or len(q) < 2:
        return jsonify([])

    try:
        r = requests.get(
            'https://suggestqueries.google.com/complete/search',
            params={'client': 'firefox', 'q': q},
            timeout=3,
            headers={'User-Agent': SC_UA},
        )
        suggestions = r.json()[1][:8]
        return jsonify(suggestions)
    except Exception as e:
        log.debug(f"Suggest error: {e}")
        return jsonify([])


# ═══════════════════════════════════════════════════════════════════
# 12. ROUTES — PLAYBACK
# ═══════════════════════════════════════════════════════════════════
@app.route('/api/play', methods=['POST'])
def route_play():
    """Lấy thông tin stream để phát."""
    try:
        body = request.get_json(silent=True) or {}
        vid = body.get('video_id')

        if not vid:
            return jsonify({'error': 'Missing video_id'}), 400

        info = _get_cached_stream(vid)

        response = {
            'title':     info['title'],
            'thumbnail': info['thumbnail'],
            'duration':  info['duration'],
            'uploader':  info['uploader'],
            'id':        info['id'],
            'is_hls':    info['is_hls'],
        }

        if info['is_hls']:
            response['hls_token'] = _b64_encode(info['url'])
        else:
            response['url'] = info['url']

        return jsonify(response)

    except Exception as e:
        log.error(f"Play error: {e}")
        return jsonify({'error': str(e)}), 400


@app.route('/api/parse', methods=['POST'])
def route_parse():
    """Parse URL SoundCloud (dán link)."""
    try:
        body = request.get_json(silent=True) or {}
        url = (body.get('url') or '').strip()

        if not url:
            return jsonify({'error': 'Missing URL'}), 400

        if 'soundcloud.com' not in url:
            return jsonify({'error': 'Chỉ hỗ trợ SoundCloud'}), 400

        normalized = _normalize_sc_url(url)
        info = _get_cached_stream(normalized)

        return jsonify({
            'video_id':  normalized,
            'title':     info['title'],
            'thumbnail': info['thumbnail'],
            'duration':  info['duration'],
            'uploader':  info['uploader'],
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 400


def _rewrite_m3u8(content: str, base_url: str) -> str:
    """Rewrite m3u8 playlist → proxy qua server."""
    lines = []
    for line in content.split('\n'):
        s = line.strip()
        if not s:
            lines.append(line)
            continue

        if s.startswith('#'):
            # Rewrite URI="..." attribute
            if 'URI="' in s:
                s = re.sub(
                    r'URI="([^"]+)"',
                    lambda m: (
                        f'URI="/hls_proxy?t='
                        f'{_b64_encode(urllib.parse.urljoin(base_url, m.group(1)))}"'
                    ),
                    s
                )
            lines.append(s)
        else:
            # Rewrite segment URL
            joined = urllib.parse.urljoin(base_url, s)
            lines.append(f'/hls_proxy?t={_b64_encode(joined)}')

    return '\n'.join(lines)


@app.route('/hls_proxy')
def route_hls_proxy():
    """Proxy HLS playlist và segment."""
    token = request.args.get('t', '')
    if not token:
        return "Missing token", 400

    try:
        url = _b64_decode(token)
    except Exception:
        return "Bad token", 400

    if not url.startswith('http'):
        return "Bad URL", 400

    try:
        resp = requests.get(
            url,
            headers={'User-Agent': SC_UA, 'Referer': SC_REFERER},
            stream=True,
            timeout=20,
        )
    except Exception as e:
        return f"Proxy error: {e}", 502

    content_type = (resp.headers.get('Content-Type') or '').lower()
    is_playlist = (
        'mpegurl' in content_type
        or url.lower().split('?')[0].endswith('.m3u8')
    )

    if is_playlist:
        try:
            text = resp.text
        except Exception:
            return "Read error", 502

        return Response(
            _rewrite_m3u8(text, url),
            mimetype='application/vnd.apple.mpegurl',
            headers={
                'Access-Control-Allow-Origin': '*',
                'Cache-Control': 'no-cache',
            }
        )

    def _gen():
        try:
            for chunk in resp.iter_content(16384):
                if chunk:
                    yield chunk
        finally:
            try:
                resp.close()
            except Exception:
                pass

    return Response(
        stream_with_context(_gen()),
        mimetype=content_type or 'audio/mpeg',
        headers={
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'no-cache',
        }
    )


@app.route('/stream/<path:vid>')
def route_stream(vid: str):
    """Stream audio trực tiếp (không phải HLS)."""
    try:
        info = _get_cached_stream(vid)
    except Exception as e:
        return f"Stream error: {e}", 500

    if info['is_hls']:
        return "Track này dùng HLS — dùng /hls_proxy", 400

    headers = {
        'User-Agent': SC_UA,
        'Referer': SC_REFERER,
    }
    if 'Range' in request.headers:
        headers['Range'] = request.headers['Range']

    try:
        upstream = requests.get(
            info['url'],
            stream=True,
            headers=headers,
            timeout=30,
        )
    except Exception as e:
        return f"Upstream error: {e}", 502

    out_headers = {
        'Accept-Ranges': 'bytes',
        'Content-Type': upstream.headers.get('Content-Type', 'audio/mpeg'),
        'Cache-Control': 'no-cache',
        'Access-Control-Allow-Origin': '*',
    }
    for key in ('Content-Length', 'Content-Range'):
        if key in upstream.headers:
            out_headers[key] = upstream.headers[key]

    return Response(
        stream_with_context(upstream.iter_content(65536)),
        status=upstream.status_code,
        headers=out_headers,
    )


# ═══════════════════════════════════════════════════════════════════
# 13. ROUTES — LYRICS
# ═══════════════════════════════════════════════════════════════════
@app.route('/api/lyrics')
def route_lyrics():
    """Lấy lyrics từ lyrics.ovh."""
    artist = (request.args.get('artist') or '').strip()
    title  = (request.args.get('title') or '').strip()

    if not artist or not title:
        return jsonify({'error': 'Missing artist or title'}), 400

    try:
        url = (
            f"https://api.lyrics.ovh/v1/"
            f"{urllib.parse.quote(artist)}/"
            f"{urllib.parse.quote(title)}"
        )
        r = requests.get(url, timeout=6)
        if r.status_code == 200:
            return jsonify(r.json())
    except Exception as e:
        log.debug(f"Lyrics error: {e}")

    return jsonify({'error': 'Not found'}), 404


# ═══════════════════════════════════════════════════════════════════
# 14. ROUTES — PARTY (NGHE CHUNG)
# ═══════════════════════════════════════════════════════════════════
@app.route('/api/party/create', methods=['POST'])
def route_party_create():
    """Tạo phòng nghe chung."""
    try:
        body = request.get_json(silent=True) or {}
        user = (body.get('user') or 'Host').strip()[:20] or 'Host'

        # Generate unique code
        code = ''
        for _ in range(20):
            candidate = uuid.uuid4().hex[:6].upper()
            with _party_lock:
                if candidate not in _party_rooms:
                    code = candidate
                    break

        if not code:
            return jsonify({'error': 'Không tạo được mã phòng'}), 500

        sid = uuid.uuid4().hex[:12]

        with _party_lock:
            _party_rooms[code] = {
                'members':  {sid: {'name': user, 'seen': time.time()}},
                'queue':    [],
                'playing':  None,
                'progress': 0,
                'chat':     [],
                'host':     sid,
                'created':  time.time(),
            }

        log.info(f"Party room {code} created by '{user}'")
        return jsonify({'code': code, 'sid': sid, 'is_host': True})

    except Exception as e:
        log.error(f"Party create error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/party/join', methods=['POST'])
def route_party_join():
    """Tham gia phòng."""
    try:
        body = request.get_json(silent=True) or {}
        code = (body.get('code') or '').upper().strip()
        user = (body.get('user') or 'Khách').strip()[:20] or 'Khách'

        if not code:
            return jsonify({'error': 'Missing code'}), 400

        with _party_lock:
            if code not in _party_rooms:
                return jsonify({'error': 'Phòng không tồn tại'}), 404

            sid = uuid.uuid4().hex[:12]
            _party_rooms[code]['members'][sid] = {
                'name': user, 'seen': time.time()
            }

        log.info(f"User '{user}' joined party {code}")
        return jsonify({'code': code, 'sid': sid, 'is_host': False})

    except Exception as e:
        log.error(f"Party join error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/party/sync', methods=['POST'])
def route_party_sync():
    """Poll state + gửi action."""
    try:
        body = request.get_json(silent=True) or {}
        code = (body.get('code') or '').upper()
        sid  = body.get('sid')

        with _party_lock:
            if code not in _party_rooms:
                return jsonify({'error': 'Phòng đóng'}), 404

            room = _party_rooms[code]

            # Update heartbeat
            if sid in room['members']:
                room['members'][sid]['seen'] = time.time()

            # Cleanup dead members
            now = time.time()
            for msid in list(room['members'].keys()):
                if now - room['members'][msid]['seen'] > PARTY_TIMEOUT:
                    del room['members'][msid]

            if not room['members']:
                del _party_rooms[code]
                return jsonify({'error': 'Phòng đóng'}), 404

            # Handle actions
            action = body.get('action')

            if action == 'play':
                room['playing'] = body.get('track')
                room['progress'] = 0

            elif action == 'sync':
                room['progress'] = body.get('progress', 0)

            elif action == 'chat' and sid in room['members']:
                text = (body.get('text') or '').strip()[:200]
                if text:
                    room['chat'].append({
                        'user': room['members'][sid]['name'],
                        'text': text,
                        'ts':   now,
                    })
                    # Giữ tối đa 100 tin nhắn
                    if len(room['chat']) > 100:
                        room['chat'] = room['chat'][-100:]

            elif action == 'add' and body.get('track'):
                room['queue'].append(body['track'])

            elif action == 'remove':
                idx = body.get('idx')
                if isinstance(idx, int) and 0 <= idx < len(room['queue']):
                    room['queue'].pop(idx)

            # Return state
            return jsonify({
                'code':     code,
                'queue':    room['queue'],
                'playing':  room['playing'],
                'progress': room['progress'],
                'members':  [
                    {'name': m['name'], 'sid': k}
                    for k, m in room['members'].items()
                ],
                'chat':     room['chat'][-30:],
                'host':     room['host'],
            })

    except Exception as e:
        log.error(f"Party sync error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/party/leave', methods=['POST'])
def route_party_leave():
    """Rời phòng."""
    try:
        body = request.get_json(silent=True) or {}
        code = (body.get('code') or '').upper()
        sid  = body.get('sid')

        with _party_lock:
            if code in _party_rooms and sid in _party_rooms[code]['members']:
                del _party_rooms[code]['members'][sid]
                if not _party_rooms[code]['members']:
                    del _party_rooms[code]

        return jsonify({'ok': True})

    except Exception:
        return jsonify({'ok': False}), 500


# ═══════════════════════════════════════════════════════════════════
# 15. ROUTES — DOWNLOAD
# ═══════════════════════════════════════════════════════════════════
@app.route('/api/download', methods=['POST'])
def route_download():
    """Bắt đầu tải nhạc (async)."""
    try:
        body = request.get_json(silent=True) or {}
        vid = body.get('video_id')

        if not vid:
            return jsonify({'error': 'Missing video_id'}), 400

        tid = str(uuid.uuid4())

        with _download_lock:
            _download_tasks[tid] = {
                'status':   'downloading',
                'progress': '0%',
                '_ts':      time.time(),
            }

        def _download_task():
            try:
                opts = _base_opts()
                opts['outtmpl'] = os.path.join(DOWNLOAD_DIR, f"{tid}.%(ext)s")
                opts['format'] = 'bestaudio/best'

                def _hook(d):
                    if d['status'] == 'downloading':
                        _download_tasks[tid]['progress'] = (
                            d.get('_percent_str', '').strip()
                        )
                    elif d['status'] == 'finished':
                        _download_tasks[tid]['status'] = 'processing'

                opts['progress_hooks'] = [_hook]

                if FFMPEG_OK:
                    opts['postprocessors'] = [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'mp3',
                        'preferredquality': '320',
                    }]

                url = _normalize_sc_url(vid)
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    filename = ydl.prepare_filename(info)

                    if FFMPEG_OK:
                        filename = os.path.splitext(filename)[0] + '.mp3'

                    _download_tasks[tid].update({
                        'status':   'done',
                        'file':     filename,
                        'title':    info.get('title', 'audio'),
                        'uploader': info.get('uploader', 'Unknown'),
                    })
                    log.info(f"Download done: {tid} — {info.get('title')}")

            except Exception as e:
                _download_tasks[tid].update({
                    'status': 'error',
                    'error':  str(e),
                })
                log.error(f"Download failed: {tid} — {e}")

        threading.Thread(
            target=_download_task,
            daemon=True,
            name=f'nova-dl-{tid[:8]}'
        ).start()

        return jsonify({'task_id': tid})

    except Exception as e:
        log.error(f"Download create error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/download_status/<tid>')
def route_download_status(tid: str):
    """Kiểm tra trạng thái download."""
    with _download_lock:
        task = _download_tasks.get(tid, {'status': 'not_found'})
        return jsonify(task)


@app.route('/api/download_file/<tid>')
def route_download_file(tid: str):
    """Tải file đã download xong."""
    with _download_lock:
        task = _download_tasks.get(tid)

    if not task or task.get('status') != 'done':
        return "Not ready", 404

    file_path = task.get('file')
    if not file_path or not os.path.exists(file_path):
        return "File removed", 404

    safe_name = re.sub(
        r'[^\w\s\-\.]', '',
        task.get('title', 'audio')
    )[:80].strip() or 'audio'

    ext = os.path.splitext(file_path)[1] or '.mp3'

    return send_file(
        file_path,
        as_attachment=True,
        download_name=safe_name + ext,
        mimetype='audio/mpeg',
    )


# ═══════════════════════════════════════════════════════════════════
# 16. ERROR HANDLERS
# ═══════════════════════════════════════════════════════════════════
@app.errorhandler(404)
def _handle_404(e):
    """Nếu API → JSON, nếu khác → trả index.html (SPA fallback)."""
    if request.path.startswith('/api/') or request.path.startswith('/stream/'):
        return jsonify({'error': 'Not found'}), 404

    try:
        return send_from_directory(TEMPLATE_DIR, 'index.html')
    except Exception:
        return "Not found", 404


@app.errorhandler(500)
def _handle_500(e):
    return jsonify({'error': 'Internal server error'}), 500


@app.errorhandler(413)
def _handle_413(e):
    return jsonify({'error': 'Payload too large'}), 413


# ═══════════════════════════════════════════════════════════════════
# 17. GRACEFUL SHUTDOWN
# ═══════════════════════════════════════════════════════════════════
def _graceful_shutdown(signum, frame):
    """Dừng executor khi nhận SIGTERM/SIGINT."""
    log.info("Shutting down...")
    try:
        _executor.shutdown(wait=False, cancel_futures=True)
    except Exception:
        pass
    log.info("Goodbye 👋")
    os._exit(0)

try:
    signal.signal(signal.SIGTERM, _graceful_shutdown)
    signal.signal(signal.SIGINT, _graceful_shutdown)
except Exception:
    pass


# ═══════════════════════════════════════════════════════════════════
# 18. ENTRYPOINT
# ═══════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))

    log.info("═══════════════════════════════════════════════")
    log.info("  NOVA MUSIC SERVER")
    log.info("═══════════════════════════════════════════════")
    log.info(f"  Port     : {port}")
    log.info(f"  FFmpeg   : {'✅ ON' if FFMPEG_OK else '❌ OFF'}")
    log.info(f"  Cookies  : {'✅ YES' if COOKIES_FILE else '❌ NO'}")
    log.info(f"  Template : {TEMPLATE_DIR}")
    log.info(f"  Download : {DOWNLOAD_DIR}")
    log.info("═══════════════════════════════════════════════")
    log.info(f"  → Mở: http://localhost:{port}")
    log.info("═══════════════════════════════════════════════")

    app.run(
        host='0.0.0.0',
        port=port,
        threaded=True,
        debug=False,
        use_reloader=False,
)
