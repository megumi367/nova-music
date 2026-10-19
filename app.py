from flask import Flask, request, jsonify, Response, stream_with_context, send_file, send_from_directory
from flask_compress import Compress
import yt_dlp, requests, re, time, traceback, os, uuid, threading, subprocess, base64, urllib.parse, tempfile, io, hmac, hashlib
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from collections import defaultdict, deque
from functools import wraps

try:
    from PIL import Image, ImageDraw, ImageFilter, ImageFont
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

app = Flask(__name__)
try:
    app.json.ensure_ascii = False
except Exception:
    app.config['JSON_AS_ASCII'] = False
Compress(app)

stream_cache = {}
tasks = {}
PARTY_ROOMS = {}
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
_exec = ThreadPoolExecutor(max_workers=8)

_APP_VERSION = int(time.time())
_SECRET = os.environ.get('NOVA_SECRET') or 'nova-hls-secret-v1-stable'

# ========== RATE LIMIT ==========
_rate_store = defaultdict(lambda: deque(maxlen=200))

def rate_limit(max_per_min=60):
    def deco(fn):
        @wraps(fn)
        def wrapper(*a, **kw):
            ip = (request.headers.get('X-Forwarded-For') or request.remote_addr or 'unknown').split(',')[0].strip()
            now = time.time()
            q = _rate_store[ip]
            while q and now - q[0] > 60:
                q.popleft()
            if len(q) >= max_per_min:
                return jsonify({'error': 'Quá nhiều yêu cầu. Thử lại sau 1 phút.'}), 429
            q.append(now)
            return fn(*a, **kw)
        return wrapper
    return deco

# ========== TOKEN SIGNING (HMAC) ==========
def b64e(s):
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip('=')

def b64d(s):
    return base64.urlsafe_b64decode(s + '=' * (-len(s) % 4)).decode()

def sign_token(url):
    """Ký URL bằng HMAC để chống SSRF (chỉ server mới tạo được token hợp lệ)."""
    raw = b64e(url)
    sig = hmac.new(_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()[:16]
    return raw + '.' + sig

def verify_token(token):
    """Xác thực token. Trả về URL nếu hợp lệ, None nếu sai."""
    if not token or '.' not in token:
        return None
    try:
        raw, sig = token.rsplit('.', 1)
        expected = hmac.new(_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()[:16]
        if not hmac.compare_digest(sig, expected):
            return None
        return b64d(raw)
    except Exception:
        return None

# ========== SSRF WHITELIST (chỉ cho proxy_image) ==========
PROXY_ALLOWED_HOSTS = {
    'sndcdn.com', 'soundcloud.com', 'soundcloud.cloud',
    'mzstatic.com', 'ytimg.com',
    'gstatic.com', 'googleusercontent.com',
    'akamaized.net', 'cloudfront.net', 'fastly.net',
    'scdn.co', 'twimg.com',
}

def _is_proxy_allowed(url):
    try:
        p = urllib.parse.urlparse(url)
        if p.scheme not in ('http', 'https'):
            return False
        host = (p.hostname or '').lower()
        if not host:
            return False
        for h in PROXY_ALLOWED_HOSTS:
            if host == h or host.endswith('.' + h):
                return True
        return False
    except Exception:
        return False

# ========== MIME MAP ==========
MIME_MAP = {
    '.mp3': 'audio/mpeg', '.m4a': 'audio/mp4', '.mp4': 'audio/mp4',
    '.webm': 'audio/webm', '.opus': 'audio/opus', '.ogg': 'audio/ogg',
    '.wav': 'audio/wav', '.flac': 'audio/flac',
}

_ICON_CACHE = {}

# ========== COOKIES ==========
_cookies_env = os.environ.get('NOVA_COOKIES') or None
COOKIES_FILE = None
if _cookies_env:
    if os.path.exists(_cookies_env):
        COOKIES_FILE = _cookies_env
    else:
        _tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8')
        _tmp.write(_cookies_env)
        _tmp.close()
        COOKIES_FILE = _tmp.name

FFMPEG_OK = False
try:
    subprocess.run(['ffmpeg', '-version'], capture_output=True, timeout=3, check=True)
    FFMPEG_OK = True
except: pass

# ========== CLEANUP ==========
def cleanup_downloads_loop():
    while True:
        time.sleep(3600)
        now = time.time()
        try:
            if os.path.exists(DOWNLOAD_DIR):
                for filename in os.listdir(DOWNLOAD_DIR):
                    filepath = os.path.join(DOWNLOAD_DIR, filename)
                    if os.path.isfile(filepath) and os.stat(filepath).st_mtime < now - 7200:
                        os.remove(filepath)
            for tid in list(tasks.keys()):
                if tasks[tid].get('status') in ('done', 'error'):
                    if now - tasks[tid].get('_ts', now) > 7200:
                        del tasks[tid]
            for ip in list(_rate_store.keys()):
                if not _rate_store[ip] or now - _rate_store[ip][-1] > 600:
                    del _rate_store[ip]
            for code in list(PARTY_ROOMS.keys()):
                try: clean_room(code)
                except Exception: pass
        except Exception as e:
            print("Cleanup error:", e)

threading.Thread(target=cleanup_downloads_loop, daemon=True).start()

# ========== CORE ==========
def base_opts():
    o = {
        'quiet': True, 'no_warnings': True, 'nocheckcertificate': True,
        'socket_timeout': 20, 'retries': 3, 'fragment_retries': 3,
        'noplaylist': True, 'extractor_retries': 3,
        'age_limit': None, 'geo_bypass': True,
    }
    if COOKIES_FILE and os.path.exists(COOKIES_FILE):
        o['cookiefile'] = COOKIES_FILE
    return o

def run_to(fn, timeout=30, *args):
    f = _exec.submit(fn, *args)
    try:
        return f.result(timeout=timeout)
    except FutureTimeout:
        raise Exception(f"Timeout {timeout}s — chạy: pip install -U yt-dlp")

def norm_sc(vid):
    vid = str(vid).strip()
    if vid.isdigit():
        return f"https://soundcloud.com/tracks/{vid}"
    u = urllib.parse.unquote(vid)
    m = re.search(r'soundcloud[:\/]tracks[:\/](\d+)', u)
    if m: return f"https://soundcloud.com/tracks/{m.group(1)}"
    m2 = re.search(r'api\.soundcloud\.com/tracks/(\d+)', u)
    if m2: return f"https://soundcloud.com/tracks/{m2.group(1)}"
    if not u.startswith('http') and '/' in u: return f"https://soundcloud.com/{u}"
    if u.startswith('http'): return u
    raise Exception("SC URL không hợp lệ")

def extract_soundcloud(vid, timeout=25):
    def _do():
        o = base_opts()
        o['format'] = 'bestaudio/best'
        o['http_headers'] = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
            'Referer': 'https://soundcloud.com/',
            'Origin': 'https://soundcloud.com',
        }
        with yt_dlp.YoutubeDL(o) as y:
            return y.extract_info(vid, download=False)
    return run_to(_do, timeout=timeout)

def extract_audio(vid, timeout=30):
    """QUAN TRỌNG: Ưu tiên progressive (HTTP) hơn HLS — ổn định hơn nhiều."""
    info = extract_soundcloud(vid, timeout=timeout)
    fmts = info.get('formats') or []

    best_progressive = None
    best_hls = None

    for f in reversed(fmts):
        if not f.get('url'): continue
        proto = (f.get('protocol') or '').lower()
        url_lower = f['url'].lower()
        is_hls = 'm3u8' in proto or 'm3u8' in url_lower
        if is_hls:
            if not best_hls: best_hls = f
        else:
            if not best_progressive: best_progressive = f

    top_url = info.get('url')
    top_proto = (info.get('protocol') or '').lower()
    top_is_hls = 'm3u8' in top_proto or 'm3u8' in (top_url or '').lower()

    # Chọn: progressive > top non-HLS > hls > top
    chosen = None
    if best_progressive:
        chosen = best_progressive
    elif top_url and not top_is_hls:
        chosen = {'url': top_url, 'protocol': info.get('protocol'),
                  'http_headers': info.get('http_headers', {})}
    elif best_hls:
        chosen = best_hls
    elif top_url:
        chosen = {'url': top_url, 'protocol': info.get('protocol'),
                  'http_headers': info.get('http_headers', {})}

    if not chosen or not chosen.get('url'):
        raise Exception("Không có URL stream")

    u = chosen['url']
    proto = (chosen.get('protocol') or '').lower()
    is_hls = 'm3u8' in u.lower() or 'm3u8' in proto
    thumb = info.get('thumbnail') or (info.get('thumbnails') or [{}])[-1].get('url')

    headers = dict(chosen.get('http_headers') or info.get('http_headers') or {})
    headers.setdefault('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36')
    headers.setdefault('Referer', 'https://soundcloud.com/')

    print(f"[EXTRACT] {vid} → {'HLS' if is_hls else 'PROGRESSIVE'} | {u[:80]}...")

    return {
        'title': info.get('title'),
        'thumbnail': thumb,
        'duration': info.get('duration'),
        'uploader': info.get('uploader') or info.get('channel') or info.get('creator'),
        'source': 'soundcloud',
        'id': info.get('id') or vid,
        'url': u,
        'is_hls': is_hls,
        'headers': headers,
    }

def get_cached(vid, force=False):
    key = f"sc:{vid}"
    now = time.time()
    if not force and key in stream_cache and now - stream_cache[key]['ts'] < stream_cache[key]['ttl']:
        return stream_cache[key]['data']
    info = extract_audio(vid)
    # TTL 3600s (1 giờ) — SC signature thường 30-60 phút
    ttl = 3600
    stream_cache[key] = {'data': info, 'ts': now, 'ttl': ttl}
    return info

def build_sc_url(e):
    for k in ('webpage_url', 'permalink_url', 'original_url'):
        u = e.get(k) or ''
        if isinstance(u, str) and u.startswith('http') and 'api.soundcloud.com' not in u and 'soundcloud.com' in u:
            return u
    u = urllib.parse.unquote(str(e.get('url') or ''))
    m = re.search(r'soundcloud[:\/]tracks[:\/](\d+)', u)
    if m: return f"https://soundcloud.com/tracks/{m.group(1)}"
    tid = e.get('id')
    if tid and str(tid).isdigit(): return f"https://soundcloud.com/tracks/{tid}"
    return None

def sc_search(q, limit=20):
    def _do():
        o = base_opts()
        o.update({'extract_flat': True, 'skip_download': True})
        with yt_dlp.YoutubeDL(o) as y:
            return y.extract_info(f"scsearch{limit}:{q}", download=False)
    info = run_to(_do, timeout=25)
    out = []
    for e in (info.get('entries') or []):
        if not e: continue
        u = build_sc_url(e)
        if not u: continue
        t = e.get('thumbnails') or []
        thumb = next((x['url'] for x in reversed(t) if x.get('url')), None) or ''
        dur = e.get('duration')
        if dur and dur > 100000: dur /= 1000
        out.append({
            'id': u, 'url': u, 'title': e.get('title') or '', 'duration': dur,
            'thumb': thumb, 'channel': e.get('uploader') or e.get('channel') or '', 'source': 'soundcloud'
        })
    return out

def detect(url):
    if 'soundcloud.com' in url: return 'soundcloud'
    return None

# ========== ROUTES ==========
@app.route('/api/ping')
def ping():
    return 'pong', 200

@app.route('/api/version')
def api_version():
    return jsonify({'v': _APP_VERSION}), 200, {
        'Cache-Control': 'no-cache, no-store, must-revalidate'
    }

@app.route('/api/proxy_image')
def proxy_image():
    url = request.args.get('url')
    if not url: return "Missing URL", 400
    if not _is_proxy_allowed(url):
        return "Domain không được phép", 403
    try:
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        return Response(r.content, mimetype=r.headers.get('Content-Type', 'image/jpeg'),
                        headers={'Cache-Control': 'public, max-age=86400'})
    except Exception:
        return "Error", 502

@app.route('/')
def index():
    return send_from_directory('templates', 'index.html'), 200, {
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache', 'Expires': '0',
    }

@app.route('/api/sc_search')
@rate_limit(30)
def sc_search_route():
    q = request.args.get('q', '').strip()
    if not q: return jsonify([])
    try:
        r = sc_search(q)
        return jsonify(r if r else {'error': 'Không có kết quả'}), (200 if r else 404)
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/suggest')
def suggest():
    q = request.args.get('q', '').strip()
    if not q or len(q) < 2: return jsonify([])
    try:
        r = requests.get(
            'https://suggestqueries.google.com/complete/search',
            params={'client': 'firefox', 'q': q}, timeout=3
        )
        return jsonify(r.json()[1][:8])
    except Exception:
        return jsonify([])

@app.route('/api/play', methods=['POST'])
@rate_limit(60)
def play():
    try:
        b = request.json or {}
        vid = b.get('video_id')
        if not vid:
            return jsonify({'error': 'Thiếu video_id'}), 400
        vid = norm_sc(vid)
        info = get_cached(vid)
        resp = {
            'title': info['title'], 'thumbnail': info['thumbnail'], 'duration': info['duration'],
            'uploader': info['uploader'], 'source': 'soundcloud', 'id': info['id'],
            'is_hls': info.get('is_hls', False),
        }
        if info.get('is_hls'):
            resp['hls_token'] = sign_token(info['url'])
        else:
            resp['url'] = info['url']
        return jsonify(resp)
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 400

@app.route('/api/parse', methods=['POST'])
@rate_limit(20)
def parse():
    try:
        url = (request.json.get('url') or '').strip()
        if not url: return jsonify({'error': 'Chưa có link'}), 400
        src = detect(url)
        if not src: return jsonify({'error': 'Chỉ hỗ trợ SoundCloud'}), 400
        nurl = norm_sc(url)
        info = get_cached(nurl)
        return jsonify({
            'video_id': nurl, 'source': 'soundcloud', 'title': info['title'],
            'thumbnail': info['thumbnail'], 'duration': info['duration'], 'uploader': info['uploader']
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 400

@app.route('/api/lyrics')
def lyrics():
    artist = request.args.get('artist', '')
    title = request.args.get('title', '')
    if not artist or not title:
        return jsonify({'error': 'Thiếu thông tin'}), 400
    try:
        r = requests.get(f"https://api.lyrics.ovh/v1/{artist}/{title}", timeout=6)
        if r.status_code == 200:
            return jsonify(r.json())
        return jsonify({'error': 'Không có lời'}), 404
    except Exception:
        return jsonify({'error': 'Không có lời'}), 404

# ========== HLS PROXY ==========
def rewrite_m3u8(content, base_url):
    out = []
    for line in content.split('\n'):
        s = line.strip()
        if not s:
            out.append(line); continue
        if s.startswith('#'):
            if 'URI="' in s:
                def repl(m):
                    new_url = urllib.parse.urljoin(base_url, m.group(1))
                    return f'URI="/hls_proxy?t={sign_token(new_url)}"'
                s = re.sub(r'URI="([^"]+)"', repl, s)
            out.append(s)
        else:
            new_url = urllib.parse.urljoin(base_url, s)
            out.append(f'/hls_proxy?t={sign_token(new_url)}')
    return '\n'.join(out)

@app.route('/hls_proxy')
def hls_proxy():
    token = request.args.get('t', '')
    if not token: return "Missing token", 400

    url = verify_token(token)
    if not url:
        print(f"[HLS] BAD TOKEN")
        return "Token không hợp lệ", 403
    if not url.startswith('http'):
        return "Bad URL", 400

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
        'Referer': 'https://soundcloud.com/',
        'Origin': 'https://soundcloud.com',
        'Accept': '*/*',
    }

    try:
        r = requests.get(url, headers=headers, stream=True, timeout=25, allow_redirects=True)
    except Exception as e:
        print(f"[HLS] FETCH ERR: {e} | {url[:100]}")
        return f"Error: {e}", 502

    if r.status_code >= 400:
        print(f"[HLS] HTTP {r.status_code}: {url[:100]}")
        return f"Upstream {r.status_code}", 502

    ct = (r.headers.get('Content-Type') or '').lower()
    path_lower = url.lower().split('?')[0]
    is_playlist = ('mpegurl' in ct or 'm3u8' in ct
                   or path_lower.endswith('.m3u8')
                   or url.lower().split('?')[0].endswith('.m3u8'))

    if is_playlist:
        try:
            content = r.text
            if not content.strip().startswith('#EXTM3U'):
                print(f"[HLS] NOT M3U8: {content[:80]}")
                return "Không phải m3u8", 502
        except Exception as e:
            return f"Read error: {e}", 502

        rewritten = rewrite_m3u8(content, url)
        return Response(
            rewritten,
            mimetype='application/vnd.apple.mpegurl',
            headers={
                'Access-Control-Allow-Origin': '*',
                'Cache-Control': 'no-cache',
                'Content-Length': str(len(rewritten)),
            }
        )

    def gen():
        try:
            for chunk in r.iter_content(16384):
                if chunk: yield chunk
        except Exception as e:
            print(f"[HLS] SEG ERR: {e}")
        finally:
            try: r.close()
            except Exception: pass

    resp_headers = {
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-cache',
        'Accept-Ranges': 'bytes',
    }
    for k in ('Content-Type', 'Content-Length', 'Content-Range'):
        if k in r.headers:
            resp_headers[k] = r.headers[k]
    if 'Content-Type' not in resp_headers:
        resp_headers['Content-Type'] = 'video/mp2t' if path_lower.endswith('.ts') else 'audio/aac'

    return Response(stream_with_context(gen()), status=r.status_code, headers=resp_headers)

# ========== DIAGNOSTIC ==========
@app.route('/api/hls_test')
def hls_test():
    """Test endpoint — dùng để debug HLS. Ví dụ: /api/hls_test?t=<token>"""
    token = request.args.get('t', '')
    url = verify_token(token) if token else request.args.get('url', '')
    if not url:
        return jsonify({'error': 'Cần ?t=<token> hoặc ?url=<url>'}), 400
    try:
        r = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://soundcloud.com/'
        }, timeout=15, stream=True, allow_redirects=True)
        ct = r.headers.get('Content-Type') or ''
        preview = ''
        if 'mpegurl' in ct.lower() or url.endswith('.m3u8'):
            try: preview = r.text[:1000]
            except: preview = '(read error)'
        else:
            preview = f'(binary, {len(r.content)} bytes)'
        return jsonify({
            'status': r.status_code,
            'content_type': ct,
            'url': url,
            'final_url': r.url,
            'preview': preview,
            'is_m3u8': preview.strip().startswith('#EXTM3U') if isinstance(preview, str) else False,
        })
    except Exception as e:
        return jsonify({'error': str(e), 'url': url}), 500

# ========== STREAM ==========
@app.route('/stream/<path:vid>', methods=['GET', 'HEAD'])
def stream(vid):
    try:
        vid = norm_sc(vid)
        info = get_cached(vid)
    except Exception as e:
        traceback.print_exc()
        return f"Lỗi: {e}", 500

    if info.get('is_hls'):
        # Fallback: nếu client vào /stream/ mà info là HLS (hiếm), redirect sang HLS proxy
        return jsonify({'error': 'HLS stream — dùng /hls_proxy', 'hls_token': sign_token(info['url'])}), 400

    hdrs = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://soundcloud.com/',
    }
    if info.get('headers'):
        hdrs.update(info['headers'])

    rng = request.headers.get('Range', '')
    if rng:
        hdrs['Range'] = rng

    try:
        req = requests.get(info['url'], stream=True, headers=hdrs, timeout=30)
    except Exception as e:
        return f"Stream error: {e}", 502

    # URL có thể hết hạn → force refresh
    if req.status_code in (401, 403):
        print(f"[STREAM] {req.status_code} — refreshing cache for {vid}")
        try:
            info = get_cached(vid, force=True)
            try: req.close()
            except: pass
            try:
                req = requests.get(info['url'], stream=True, headers=hdrs, timeout=30)
            except Exception as e:
                return f"Stream refresh error: {e}", 502
        except Exception as e:
            return f"Refresh failed: {e}", 502

    h = {'Accept-Ranges': 'bytes', 'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'}
    for k in ('Content-Type', 'Content-Length', 'Content-Range'):
        if k in req.headers:
            h[k] = req.headers[k]

    if 'Content-Type' not in h:
        ext = info['url'].split('?')[0].split('.')[-1].lower()
        h['Content-Type'] = MIME_MAP.get('.' + ext, 'audio/mp4')

    if request.method == 'HEAD':
        try: req.close()
        except: pass
        return Response(status=req.status_code, headers=h)

    return Response(
        stream_with_context(req.iter_content(65536)),
        status=req.status_code, headers=h
    )

# ========== DOWNLOAD ==========
def dl_task(tid, vid):
    def hook(d):
        if d['status'] == 'downloading':
            tasks[tid]['progress'] = d.get('_percent_str', '').strip()
        elif d['status'] == 'finished':
            tasks[tid]['status'] = 'processing'
    try:
        o = base_opts()
        o['outtmpl'] = os.path.join(DOWNLOAD_DIR, f"{tid}.%(ext)s")
        o['progress_hooks'] = [hook]
        o['http_headers'] = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://soundcloud.com/'}
        url = norm_sc(vid)

        if FFMPEG_OK:
            o['format'] = 'bestaudio/best'
            o['postprocessors'] = [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}]
        else:
            o['format'] = 'bestaudio[ext=mp3]/bestaudio[ext=m4a]/bestaudio/best'

        with yt_dlp.YoutubeDL(o) as y:
            info = y.extract_info(url, download=True)
            fn = y.prepare_filename(info)
            if o.get('postprocessors'):
                fn = os.path.splitext(fn)[0] + '.mp3'
            tasks[tid].update({'status': 'done', 'file': fn, 'title': info.get('title', 'audio'), 'source': 'soundcloud'})
    except Exception as e:
        traceback.print_exc()
        tasks[tid].update({'status': 'error', 'error': str(e)})

@app.route('/api/download', methods=['POST'])
@rate_limit(10)
def download():
    b = request.json
    vid = b.get('video_id')
    if not vid: return jsonify({'error': 'Thiếu video_id'}), 400
    tid = str(uuid.uuid4())
    tasks[tid] = {'status': 'downloading', 'progress': '0%', 'source': 'soundcloud', '_ts': time.time()}
    threading.Thread(target=dl_task, args=(tid, vid), daemon=True).start()
    return jsonify({'task_id': tid})

@app.route('/api/download_status/<tid>')
def dl_status(tid):
    return jsonify(tasks.get(tid, {'status': 'not_found'}))

@app.route('/api/download_file/<tid>')
def dl_file(tid):
    t = tasks.get(tid)
    if not t or t.get('status') != 'done': return "Chưa xong", 404
    if not os.path.exists(t['file']): return "File đã xoá", 404
    ext = os.path.splitext(t['file'])[1].lower()
    safe = re.sub(r'[^\w\s\-\.]', '', t['title'])[:80].strip() or 'audio'
    mime = MIME_MAP.get(ext, 'application/octet-stream')
    return send_file(t['file'], as_attachment=True, download_name=safe + ext, mimetype=mime)

# ========== PARTY ==========
def clean_room(code):
    room = PARTY_ROOMS.get(code)
    if not room: return
    now = time.time()
    for sid, m in list(room['members'].items()):
        if now - m.get('last_seen', 0) > 60:
            del room['members'][sid]
    if not room['members']:
        del PARTY_ROOMS[code]
        return
    host_sid = room.get('host_sid')
    if host_sid not in room['members']:
        new_sid = next(iter(room['members']))
        room['host_sid'] = new_sid
        room['host'] = room['members'][new_sid]['name']

@app.route('/api/party/create', methods=['POST', 'GET'])
def party_create():
    try:
        d = request.get_json(silent=True) or request.args.to_dict() or {}
        user = (d.get('user') or 'Host').strip()[:20]
        for _ in range(20):
            code = uuid.uuid4().hex[:6].upper()
            if code not in PARTY_ROOMS: break
        sid = uuid.uuid4().hex[:10]
        PARTY_ROOMS[code] = {
            'queue': [],
            'members': {sid: {'name': user, 'last_seen': time.time(), 'sid': sid}},
            'playing': None, 'position': 0, 'is_playing': False,
            'chat': [], 'host': user, 'host_sid': sid, 'created': time.time(),
        }
        return jsonify({'code': code, 'sid': sid, 'is_host': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/party/join', methods=['POST'])
def party_join():
    try:
        d = request.get_json(silent=True) or {}
        code = (d.get('code') or '').upper().strip()
        user = (d.get('user') or 'Khách').strip()[:20]
        if not code: return jsonify({'error': 'Thiếu mã phòng'}), 400
        if code not in PARTY_ROOMS: return jsonify({'error': 'Không tìm thấy phòng'}), 404
        room = PARTY_ROOMS[code]
        sid = uuid.uuid4().hex[:10]
        is_host = False
        if room.get('host_sid') not in room['members'] and user == room.get('host'):
            room['host_sid'] = sid
            is_host = True
        room['members'][sid] = {'name': user, 'last_seen': time.time(), 'sid': sid}
        return jsonify({'code': code, 'sid': sid, 'is_host': is_host})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/party/state', methods=['POST'])
def party_state():
    try:
        d = request.get_json(silent=True) or {}
        code = (d.get('code') or '').upper()
        sid = d.get('sid')
        if code not in PARTY_ROOMS: return jsonify({'error': 'Phòng đã đóng'}), 404
        room = PARTY_ROOMS[code]
        if sid and sid in room['members']:
            room['members'][sid]['last_seen'] = time.time()
        clean_room(code)
        if code not in PARTY_ROOMS: return jsonify({'error': 'Phòng đã đóng'}), 404
        room = PARTY_ROOMS[code]
        return jsonify({
            'queue': room['queue'],
            'members': [{'name': m['name'], 'sid': k} for k, m in room['members'].items()],
            'playing': room['playing'], 'position': room['position'],
            'is_playing': room['is_playing'], 'chat': room['chat'][-30:],
            'host': room.get('host'), 'host_sid': room.get('host_sid'),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/party/leave', methods=['POST'])
def party_leave():
    try:
        d = request.get_json(silent=True) or {}
        code = (d.get('code') or '').upper()
        sid = d.get('sid')
        if code in PARTY_ROOMS and sid in PARTY_ROOMS[code]['members']:
            del PARTY_ROOMS[code]['members'][sid]
            clean_room(code)
        return jsonify({'ok': True})
    except Exception:
        return jsonify({'ok': True})

@app.route('/api/party/play', methods=['POST'])
def party_play():
    try:
        d = request.get_json(silent=True) or {}
        code = (d.get('code') or '').upper()
        if code in PARTY_ROOMS:
            PARTY_ROOMS[code]['playing'] = d.get('track')
            PARTY_ROOMS[code]['position'] = 0
            PARTY_ROOMS[code]['is_playing'] = True
        return jsonify({'ok': True})
    except Exception:
        return jsonify({'ok': False}), 500

@app.route('/api/party/sync', methods=['POST'])
def party_sync():
    try:
        d = request.get_json(silent=True) or {}
        code = (d.get('code') or '').upper()
        if code in PARTY_ROOMS:
            PARTY_ROOMS[code]['position'] = d.get('position', 0)
            PARTY_ROOMS[code]['is_playing'] = d.get('is_playing', True)
        return jsonify({'ok': True})
    except Exception:
        return jsonify({'ok': False}), 500

@app.route('/api/party/add', methods=['POST'])
def party_add():
    try:
        d = request.get_json(silent=True) or {}
        code = (d.get('code') or '').upper()
        t = d.get('track')
        if code in PARTY_ROOMS and t:
            PARTY_ROOMS[code]['queue'].append(t)
        return jsonify({'ok': True})
    except Exception:
        return jsonify({'ok': False}), 500

@app.route('/api/party/remove', methods=['POST'])
def party_remove():
    try:
        d = request.get_json(silent=True) or {}
        code = (d.get('code') or '').upper()
        i = d.get('idx')
        if code in PARTY_ROOMS and isinstance(i, int) and 0 <= i < len(PARTY_ROOMS[code]['queue']):
            PARTY_ROOMS[code]['queue'].pop(i)
        return jsonify({'ok': True})
    except Exception:
        return jsonify({'ok': False}), 500

@app.route('/api/party/chat', methods=['POST'])
def party_chat():
    try:
        d = request.get_json(silent=True) or {}
        code = (d.get('code') or '').upper()
        user = (d.get('user') or 'Khách')[:20]
        text = (d.get('text') or '').strip()[:200]
        if code in PARTY_ROOMS and text:
            PARTY_ROOMS[code]['chat'].append({'user': user, 'text': text, 'ts': time.time()})
            if len(PARTY_ROOMS[code]['chat']) > 100:
                PARTY_ROOMS[code]['chat'] = PARTY_ROOMS[code]['chat'][-100:]
        return jsonify({'ok': True})
    except Exception:
        return jsonify({'ok': False}), 500

# ========== ICONS ==========
def _render_nova_icon(size):
    if not HAS_PIL: return None
    try:
        import math
        img = Image.new('RGBA', (size, size), (2, 1, 10, 255))
        cx, cy = size // 2, size // 2
        R = size / 512.0
        grad = Image.radial_gradient('L').resize((size, size), Image.LANCZOS)
        mask = Image.eval(grad, lambda x: int((255 - x) * 0.65))
        purple = Image.new('RGBA', (size, size), (58, 16, 96, 255))
        dark = Image.new('RGBA', (size, size), (2, 1, 10, 255))
        img = Image.composite(purple, dark, mask)
        spike = Image.new('RGBA', (size, size), (0, 0, 0, 0))
        sd = ImageDraw.Draw(spike)
        for angle_deg in (0, 90, 180, 270):
            a = math.radians(angle_deg - 90)
            apex_r, base_r = int(248 * R), int(172 * R)
            spread = math.radians(8)
            apex = (cx + apex_r * math.cos(a), cy + apex_r * math.sin(a))
            p1 = (cx + base_r * math.cos(a - spread), cy + base_r * math.sin(a - spread))
            p2 = (cx + base_r * math.cos(a + spread), cy + base_r * math.sin(a + spread))
            sd.polygon([apex, p1, p2], fill=(167, 139, 250, 240))
        img = Image.alpha_composite(img, spike.filter(ImageFilter.GaussianBlur(radius=max(2, size*0.022))))
        img = Image.alpha_composite(img, spike)
        ring = Image.new('RGBA', (size, size), (0, 0, 0, 0))
        rd = ImageDraw.Draw(ring)
        ring_cy, ring_r = int(cy - 40 * R), int(138 * R)
        ring_w = max(8, int(22 * R))
        rd.ellipse([cx-ring_r, ring_cy-ring_r, cx+ring_r, ring_cy+ring_r],
                   outline=(183, 148, 255, 255), width=ring_w)
        img = Image.alpha_composite(img, ring.filter(ImageFilter.GaussianBlur(radius=max(3, size*0.045))))
        img = Image.alpha_composite(img, ring.filter(ImageFilter.GaussianBlur(radius=max(1, size*0.012))))
        img = Image.alpha_composite(img, ring)
        sil = Image.new('RGBA', (size, size), (0, 0, 0, 0))
        s = ImageDraw.Draw(sil)
        shoulder_y = int(cy + 110 * R)
        s.polygon([
            (cx - int(132*R), size), (cx + int(132*R), size),
            (cx + int(88*R), shoulder_y), (cx - int(88*R), shoulder_y),
        ], fill=(0, 0, 0, 255))
        s.rectangle([cx-int(28*R), int(cy + 15*R), cx+int(28*R), shoulder_y + int(12*R)], fill=(0,0,0,255))
        head_r, head_cy = int(70 * R), int(cy - 15 * R)
        s.ellipse([cx-head_r, head_cy-head_r, cx+head_r, head_cy+head_r], fill=(0,0,0,255))
        crown_y = int(head_cy - head_r + int(18*R))
        s.polygon([(cx-int(52*R), crown_y+int(18*R)), (cx-int(40*R), crown_y-int(42*R)), (cx-int(20*R), crown_y+int(18*R))], fill=(0,0,0,255))
        s.polygon([(cx-int(22*R), crown_y+int(18*R)), (cx, crown_y-int(82*R)), (cx+int(22*R), crown_y+int(18*R))], fill=(0,0,0,255))
        s.polygon([(cx+int(20*R), crown_y+int(18*R)), (cx+int(40*R), crown_y-int(42*R)), (cx+int(52*R), crown_y+int(18*R))], fill=(0,0,0,255))
        img = Image.alpha_composite(img, sil)
        beam = Image.new('RGBA', (size, size), (0, 0, 0, 0))
        bd = ImageDraw.Draw(beam)
        beam_top, beam_w = int(head_cy + int(10*R)), max(2, int(3 * R))
        bd.rectangle([cx - beam_w, beam_top, cx + beam_w, size], fill=(212, 187, 255, 220))
        img = Image.alpha_composite(img, beam.filter(ImageFilter.GaussianBlur(radius=max(2, size*0.014))))
        img = Image.alpha_composite(img, beam)
        star_cx, star_cy = cx, head_cy + int(5*R)
        star_outer, star_inner = int(46 * R), int(8 * R)
        pts = []
        for i in range(8):
            angle = math.radians(i * 45 - 90)
            r = star_outer if i % 2 == 0 else star_inner
            pts.append((star_cx + r * math.cos(angle), star_cy + r * math.sin(angle)))
        sg = Image.new('RGBA', (size, size), (0, 0, 0, 0))
        ImageDraw.Draw(sg).polygon(pts, fill=(200, 150, 255, 255))
        img = Image.alpha_composite(img, sg.filter(ImageFilter.GaussianBlur(radius=max(3, size*0.04))))
        sw = Image.new('RGBA', (size, size), (0, 0, 0, 0))
        ImageDraw.Draw(sw).polygon(pts, fill=(255, 255, 255, 255))
        img = Image.alpha_composite(img, sw.filter(ImageFilter.GaussianBlur(radius=max(1, size*0.012))))
        ss = Image.new('RGBA', (size, size), (0, 0, 0, 0))
        ImageDraw.Draw(ss).polygon(pts, fill=(255, 255, 255, 255))
        img = Image.alpha_composite(img, ss)
        buf = io.BytesIO()
        img.save(buf, format='PNG', optimize=True)
        return buf.getvalue()
    except Exception as e:
        traceback.print_exc()
        return None

@app.route('/icon-v3-<int:size>.png')
def pwa_icon_png_v3(size):
    if size not in (96, 128, 144, 152, 180, 192, 256, 384, 512):
        return "Bad size", 404
    data = _ICON_CACHE.get(size) or _render_nova_icon(size)
    if data:
        _ICON_CACHE[size] = data
    if data is None:
        return pwa_icon_svg()
    return Response(data, mimetype='image/png', headers={
        'Cache-Control': 'public, max-age=31536000, immutable',
        'Content-Length': str(len(data)),
    })

@app.route('/icon.svg')
def pwa_icon_svg():
    svg = '''<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg">
  <defs><radialGradient id="bg" cx="50%" cy="45%" r="70%"><stop offset="0%" stop-color="#3a1060"/><stop offset="55%" stop-color="#0a0520"/><stop offset="100%" stop-color="#02010a"/></radialGradient></defs>
  <circle cx="256" cy="256" r="256" fill="url(#bg)"/>
  <circle cx="256" cy="216" r="140" fill="none" stroke="#c4a3ff" stroke-width="14"/>
  <g fill="#000">
    <path d="M 120 512 L 140 390 C 150 340, 190 300, 220 290 L 256 310 L 292 290 C 322 300, 362 340, 372 390 L 392 512 Z"/>
    <ellipse cx="256" cy="210" rx="72" ry="80"/>
    <path d="M 190 175 L 210 105 L 230 175 Z"/><path d="M 232 165 L 256 65 L 280 165 Z"/><path d="M 282 175 L 302 105 L 322 175 Z"/>
  </g>
  <path d="M 256 155 L 264 198 L 308 208 L 264 218 L 256 262 L 248 218 L 204 208 L 248 198 Z" fill="#ffffff"/>
</svg>'''
    return Response(svg, mimetype='image/svg+xml', headers={'Cache-Control': 'public, max-age=604800'})

@app.route('/manifest.json')
def manifest():
    data = {
        'name': 'Nova Music Universe', 'short_name': 'Nova',
        'description': 'Nghe nhạc SoundCloud Premium',
        'start_url': '/?source=pwa', 'scope': '/', 'display': 'standalone',
        'display_override': ['window-controls-overlay', 'standalone', 'minimal-ui'],
        'orientation': 'portrait-primary', 'background_color': '#030305',
        'theme_color': '#8b5cf6', 'lang': 'vi', 'dir': 'ltr',
        'categories': ['music', 'entertainment'], 'prefer_related_applications': False,
        'icons': [
            {'src': '/icon.svg', 'sizes': 'any', 'type': 'image/svg+xml', 'purpose': 'any maskable'},
            {'src': '/icon-v3-192.png', 'sizes': '192x192', 'type': 'image/png', 'purpose': 'any'},
            {'src': '/icon-v3-192.png', 'sizes': '192x192', 'type': 'image/png', 'purpose': 'maskable'},
            {'src': '/icon-v3-512.png', 'sizes': '512x512', 'type': 'image/png', 'purpose': 'any'},
            {'src': '/icon-v3-512.png', 'sizes': '512x512', 'type': 'image/png', 'purpose': 'maskable'},
        ],
        'shortcuts': [
            {'name': 'Tìm nhạc', 'short_name': 'Search', 'url': '/?nav=search', 'icons': [{'src': '/icon-v3-192.png', 'sizes': '192x192'}]},
            {'name': 'Nghe chung', 'short_name': 'Party', 'url': '/?nav=party', 'icons': [{'src': '/icon-v3-192.png', 'sizes': '192x192'}]},
        ],
    }
    return jsonify(data), 200, {'Cache-Control': 'no-cache, no-store, must-revalidate'}

@app.route('/.well-known/assetlinks.json')
def assetlinks():
    data = [{
        "relation": ["delegate_permission/common.handle_all_urls"],
        "target": {
            "namespace": "android_app", "package_name": "com.nova.music",
            "sha256_cert_fingerprints": ["EC:9B:D9:C2:FC:BA:68:45:A0:C1:F8:AF:E0:15:92:52:F4:66:AA:01:56:66:28:5F:9A:57:E9:D5:6A:26:02:6F"]
        }
    }]
    return jsonify(data), 200, {'Content-Type': 'application/json', 'Cache-Control': 'no-cache'}

@app.route('/sw.js')
def sw():
    s = """const C='nova-v22';
self.addEventListener('install',e=>{self.skipWaiting()});
self.addEventListener('activate',e=>{
  e.waitUntil(caches.keys().then(k=>Promise.all(k.filter(x=>x!==C).map(x=>caches.delete(x)))).then(()=>self.clients.claim()));
});
self.addEventListener('fetch',e=>{
  const u=new URL(e.request.url);
  if(u.pathname==='/'||u.pathname.endsWith('.html')||u.pathname==='/sw.js'){
    e.respondWith(fetch(e.request).catch(()=>caches.match(e.request)));return;
  }
  if(u.pathname.startsWith('/icon-')||u.pathname==='/icon.svg'){
    e.respondWith(caches.match(e.request).then(c=>c||fetch(e.request).then(r=>{
      const cl=r.clone();caches.open(C).then(cc=>cc.put(e.request,cl));return r;
    })));return;
  }
  if(u.pathname==='/manifest.json'||u.pathname.startsWith('/static/')){
    e.respondWith(caches.match(e.request).then(c=>c||fetch(e.request).then(r=>{
      const cl=r.clone();caches.open(C).then(cc=>cc.put(e.request,cl));return r;
    })));
  }
});
self.addEventListener('message',e=>{if(e.data==='SKIP_WAITING')self.skipWaiting();});
"""
    return Response(s, mimetype='application/javascript', headers={'Cache-Control': 'no-cache, no-store, must-revalidate'})

@app.route('/api/debug')
def debug():
    import yt_dlp as y
    return jsonify({
        'version': _APP_VERSION, 'yt_dlp': y.version.__version__,
        'ffmpeg': FFMPEG_OK, 'cookies': bool(COOKIES_FILE),
        'cache': len(stream_cache), 'rooms': len(PARTY_ROOMS),
        'tasks': len(tasks), 'icon_cache': len(_ICON_CACHE),
        'pillow': HAS_PIL, 'hmac_enabled': True,
    })

if __name__ == '__main__':
    import yt_dlp as y
    print(f"\n✅ Nova — yt-dlp {y.version.__version__}")
    print(f"   ffmpeg: {'✅' if FFMPEG_OK else '⚠️'}")
    print(f"   cookies: {'✅' if COOKIES_FILE else '❌'}")
    print(f"   pillow: {'✅' if HAS_PIL else '❌'}")
    print(f"   version: {_APP_VERSION}")
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=False, port=port, threaded=True, host='0.0.0.0')
