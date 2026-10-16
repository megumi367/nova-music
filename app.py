from flask import Flask, request, jsonify, Response, stream_with_context, send_file, send_from_directory
import yt_dlp, requests, re, time, traceback, os, uuid, threading, subprocess, base64, urllib.parse, tempfile
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

stream_cache = {}
tasks = {}
PARTY_ROOMS = {}
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
_exec = ThreadPoolExecutor(max_workers=8)

# === COOKIES từ ENV (cho Render) ===
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
        raise Exception(f"Timeout {timeout}s")


# ============ YOUTUBE ============
YT_CLIENTS = [['ios'], ['web_embedded'], ['android_vr'], ['tv_embedded'], ['mweb'], ['android']]


def extract_youtube(vid, mode='audio', timeout=30):
    def _try(client_list):
        o = base_opts()
        if mode == 'video':
            o['format'] = ('best[ext=mp4][height<=720][vcodec!=none][acodec!=none]/'
                           'best[ext=mp4][height<=1080][vcodec!=none][acodec!=none]/'
                           'best[ext=mp4][vcodec!=none][acodec!=none]/best[height<=720]/best')
        else:
            o['format'] = 'bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio[ext=mp3]/bestaudio/best'
        o['extractor_args'] = {'youtube': {'player_client': client_list}}
        o['http_headers'] = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        url = vid if str(vid).startswith('http') else f"https://www.youtube.com/watch?v={vid}"
        with yt_dlp.YoutubeDL(o) as y:
            return y.extract_info(url, download=False)

    last_err = None
    for clients in YT_CLIENTS:
        try:
            info = _try(clients)
            u = info.get('url')
            if not u:
                fmts = info.get('formats') or []
                for f in reversed(fmts):
                    if f.get('url'):
                        if mode == 'audio' and f.get('vcodec') != 'none':
                            continue
                        u = f['url']
                        info['protocol'] = f.get('protocol')
                        info['http_headers'] = f.get('http_headers', info.get('http_headers', {}))
                        break
            if u:
                return info
        except Exception as e:
            last_err = e
            time.sleep(.3)
            continue
    raise Exception(f"YouTube fail: {last_err}")


# ============ SOUNDCLOUD ============
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


# ============ TIKTOK ============
def extract_tiktok(vid, mode='audio', timeout=25):
    def _do():
        o = base_opts()
        o['format'] = 'bestaudio/best' if mode == 'audio' else 'best[ext=mp4]/best'
        o['http_headers'] = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.tiktok.com/'}
        with yt_dlp.YoutubeDL(o) as y:
            return y.extract_info(vid, download=False)
    return run_to(_do, timeout=timeout)


def extract_audio(vid, source='youtube', mode='audio', timeout=30):
    if source == 'youtube':
        info = extract_youtube(vid, mode=mode, timeout=timeout)
    elif source == 'soundcloud':
        info = extract_soundcloud(vid, timeout=timeout)
    elif source == 'tiktok':
        info = extract_tiktok(vid, mode=mode, timeout=timeout)
    else:
        raise Exception(f"Unknown source: {source}")

    u = info.get('url')
    if not u:
        fmts = info.get('formats') or []
        for f in reversed(fmts):
            if f.get('url'):
                if mode == 'audio' and source == 'youtube' and f.get('vcodec') != 'none':
                    continue
                u = f['url']
                info['protocol'] = f.get('protocol')
                info['http_headers'] = f.get('http_headers', info.get('http_headers', {}))
                break
    if not u:
        raise Exception("Không có URL stream")

    proto = (info.get('protocol') or '').lower()
    is_hls = 'm3u8' in u.lower() or 'm3u8' in proto
    thumb = info.get('thumbnail') or (info.get('thumbnails') or [{}])[-1].get('url')

    return {
        'title': info.get('title'),
        'thumbnail': thumb,
        'duration': info.get('duration'),
        'uploader': info.get('uploader') or info.get('channel') or info.get('creator'),
        'source': source,
        'id': info.get('id') or vid,
        'url': u,
        'is_hls': is_hls,
        'headers': info.get('http_headers', {}) or {'User-Agent': 'Mozilla/5.0'},
    }


def get_cached(vid, source='youtube', mode='audio'):
    key = f"{source}:{mode}:{vid}"
    now = time.time()
    if key in stream_cache and now - stream_cache[key]['ts'] < stream_cache[key]['ttl']:
        return stream_cache[key]['data']
    info = extract_audio(vid, source, mode)
    ttl = 2400 if source == 'youtube' else 7000
    stream_cache[key] = {'data': info, 'ts': now, 'ttl': ttl}
    return info


def b64e(s): return base64.urlsafe_b64encode(s.encode()).decode().rstrip('=')
def b64d(s): return base64.urlsafe_b64decode(s + '=' * (-len(s) % 4)).decode()


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


def yt_search(q, limit=20):
    def _do():
        o = base_opts()
        o.update({'extract_flat': 'in_playlist', 'skip_download': True, 'playlistend': limit})
        o['extractor_args'] = {'youtube': {'player_client': ['ios', 'web_embedded', 'mweb']}}
        with yt_dlp.YoutubeDL(o) as y:
            return y.extract_info(f"ytsearch{limit}:{q}", download=False)
    info = run_to(_do, timeout=25)
    out = []
    for e in (info.get('entries') or []):
        if not e or not e.get('id'): continue
        t = e.get('thumbnails') or []
        thumb = next((x['url'] for x in reversed(t) if x.get('url')), None) or f"https://i.ytimg.com/vi/{e['id']}/mqdefault.jpg"
        out.append({
            'id': e['id'], 'title': e.get('title') or '', 'duration': e.get('duration'),
            'thumb': thumb, 'channel': e.get('uploader') or e.get('channel') or '', 'source': 'youtube'
        })
    return out


def sc_search(q, limit=20):
    def _do():
        o = base_opts()
        o.update({'extract_flat': True, 'skip_download': True, 'playlistend': limit})
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
    if 'youtube.com' in url or 'youtu.be' in url: return 'youtube'
    if 'soundcloud.com' in url: return 'soundcloud'
    if 'tiktok.com' in url: return 'tiktok'
    return None


# ============ ROUTES ============
@app.route('/api/ping')
def ping():
    return 'pong', 200


@app.route('/')
def index():
    return send_from_directory('templates', 'index.html')


@app.route('/api/search')
def search():
    q = request.args.get('q', '').strip()
    limit = int(request.args.get('limit', 20))
    if not q: return jsonify([])
    try:
        r = yt_search(q, limit)
        return jsonify(r if r else {'error': 'Không có kết quả'}), (200 if r else 404)
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/sc_search')
def sc_search_route():
    q = request.args.get('q', '').strip()
    limit = int(request.args.get('limit', 20))
    if not q: return jsonify([])
    try:
        r = sc_search(q, limit)
        return jsonify(r if r else {'error': 'Không có kết quả'}), (200 if r else 404)
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/suggest')
def suggest():
    q = request.args.get('q', '').strip()
    if not q or len(q) < 2: return jsonify([])
    try:
        r = requests.get('https://suggestqueries.google.com/complete/search',
                         params={'client': 'firefox', 'ds': 'yt', 'q': q}, timeout=3)
        return jsonify(r.json()[1][:8])
    except:
        return jsonify([])


@app.route('/api/play', methods=['POST'])
def play():
    try:
        b = request.json or {}
        vid = b.get('video_id')
        source = b.get('source', 'youtube')
        mode = b.get('mode', 'audio')
        if not vid:
            return jsonify({'error': 'Thiếu video_id'}), 400
        if source == 'soundcloud':
            vid = norm_sc(vid)
        info = get_cached(vid, source, mode)
        resp = {
            'title': info['title'], 'thumbnail': info['thumbnail'], 'duration': info['duration'],
            'uploader': info['uploader'], 'source': source, 'id': info['id'],
            'is_hls': info.get('is_hls', False),
        }
        if info.get('is_hls'):
            resp['hls_token'] = b64e(info['url'])
        else:
            resp['url'] = info['url']
        return jsonify(resp)
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 400


@app.route('/api/parse', methods=['POST'])
def parse():
    try:
        url = (request.json.get('url') or '').strip()
        if not url: return jsonify({'error': 'Chưa có link'}), 400
        src = detect(url)
        if not src: return jsonify({'error': 'Chỉ hỗ trợ YouTube, SoundCloud, TikTok'}), 400
        if src == 'youtube':
            m = re.search(r'(?:v=|youtu\.be/|shorts/|embed/)([A-Za-z0-9_-]{11})', url)
            vid = m.group(1) if m else url
            info = get_cached(vid, 'youtube', 'audio')
            video_id = vid
        else:
            nurl = norm_sc(url) if src == 'soundcloud' else url
            info = get_cached(nurl, src, 'audio')
            video_id = nurl
        return jsonify({
            'video_id': video_id, 'source': src, 'title': info['title'],
            'thumbnail': info['thumbnail'], 'duration': info['duration'], 'uploader': info['uploader']
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 400


@app.route('/api/lyrics')
def lyrics():
    artist = request.args.get('artist', '').strip()
    title = request.args.get('title', '').strip()
    duration = request.args.get('duration', '').strip()
    if not title:
        return jsonify({'error': 'Thiếu thông tin'}), 400
    hdr = {'User-Agent': 'NovaMusic/1.0 (https://github.com/megumi367/nova-music)'}

    # 1. lrclib.net /get — exact match (có synced)
    try:
        params = {'artist_name': artist, 'track_name': title}
        if duration:
            try: params['duration'] = int(float(duration))
            except: pass
        r = requests.get('https://lrclib.net/api/get', params=params, headers=hdr, timeout=8)
        if r.status_code == 200:
            d = r.json()
            if d.get('syncedLyrics'):
                return jsonify({'lyrics': d['syncedLyrics'], 'synced': True, 'source': 'lrclib'})
            if d.get('plainLyrics'):
                return jsonify({'lyrics': d['plainLyrics'], 'synced': False, 'source': 'lrclib'})
    except Exception:
        pass

    # 2. lrclib.net /search — fuzzy
    try:
        q = f"{artist} {title}".strip()
        r = requests.get('https://lrclib.net/api/search', params={'q': q}, headers=hdr, timeout=8)
        if r.status_code == 200:
            for item in (r.json() or [])[:5]:
                if item.get('syncedLyrics'):
                    return jsonify({'lyrics': item['syncedLyrics'], 'synced': True, 'source': 'lrclib-search'})
                if item.get('plainLyrics'):
                    return jsonify({'lyrics': item['plainLyrics'], 'synced': False, 'source': 'lrclib-search'})
    except Exception:
        pass

    # 3. lyrics.ovh — fallback
    if artist:
        try:
            r = requests.get(f"https://api.lyrics.ovh/v1/{artist}/{title}", headers=hdr, timeout=6)
            if r.status_code == 200 and (r.json() or {}).get('lyrics'):
                return jsonify({'lyrics': r.json()['lyrics'], 'synced': False, 'source': 'lyrics.ovh'})
        except Exception:
            pass

    return jsonify({'error': 'Không có lời'}), 404


@app.route('/hls_proxy')
def hls_proxy():
    token = request.args.get('t', '')
    if not token: return "Missing", 400
    try:
        url = b64d(token)
    except:
        return "Bad token", 400
    if not url.startswith('http'): return "Bad URL", 400
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://soundcloud.com/'}
    try:
        r = requests.get(url, headers=headers, stream=True, timeout=20)
    except Exception as e:
        return f"Error: {e}", 502
    ct = (r.headers.get('Content-Type') or '').lower()
    is_pl = 'mpegurl' in ct or url.lower().split('?')[0].endswith('.m3u8')
    if is_pl:
        try:
            c = r.text
        except:
            return "Read error", 502
        return Response(rewrite_m3u8(c, url), mimetype='application/vnd.apple.mpegurl',
                        headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-cache'})

    def gen():
        try:
            for chunk in r.iter_content(16384):
                if chunk: yield chunk
        except: pass
        finally:
            try: r.close()
            except: pass
    return Response(stream_with_context(gen()), mimetype=ct or 'audio/aac',
                    headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-cache'})


@app.route('/stream/<path:vid>')
def stream(vid):
    mode = request.args.get('mode', 'audio')
    source = request.args.get('source', 'youtube')
    try:
        if source == 'soundcloud':
            vid = norm_sc(vid)
        info = get_cached(vid, source, mode)
    except Exception as e:
        traceback.print_exc()
        return f"Lỗi: {e}", 500
    if info.get('is_hls'):
        return "Dùng /hls_proxy", 400
    hdrs = {'User-Agent': 'Mozilla/5.0'}
    if info.get('headers'):
        hdrs.update(info['headers'])
    rng = request.headers.get('Range', '')
    if rng:
        hdrs['Range'] = rng
    try:
        req = requests.get(info['url'], stream=True, headers=hdrs, timeout=30)
    except Exception as e:
        return f"Stream error: {e}", 502
    h = {'Accept-Ranges': 'bytes', 'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*'}
    for k in ('Content-Type', 'Content-Length', 'Content-Range'):
        if k in req.headers:
            h[k] = req.headers[k]
    if 'Content-Type' not in h:
        ext = info['url'].split('?')[0].split('.')[-1].lower()
        h['Content-Type'] = {'m4a': 'audio/mp4', 'mp4': 'video/mp4',
                             'webm': 'video/webm', 'mp3': 'audio/mpeg'}.get(ext, 'audio/mp4')
    return Response(stream_with_context(req.iter_content(65536)),
                    status=req.status_code, headers=h)


def dl_task(tid, vid, fmt, source):
    def hook(d):
        if d['status'] == 'downloading':
            tasks[tid]['progress'] = d.get('_percent_str', '').strip()
        elif d['status'] == 'finished':
            tasks[tid]['status'] = 'processing'
    try:
        o = base_opts()
        o['outtmpl'] = os.path.join(DOWNLOAD_DIR, f"{tid}.%(ext)s")
        o['progress_hooks'] = [hook]
        o['http_headers'] = {'User-Agent': 'Mozilla/5.0'}
        if source == 'soundcloud':
            url = norm_sc(vid)
            o['http_headers']['Referer'] = 'https://soundcloud.com/'
            if FFMPEG_OK:
                o['format'] = 'bestaudio/best'
                o['postprocessors'] = [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}]
            else:
                o['format'] = 'bestaudio[ext=mp3]/bestaudio[ext=m4a]/bestaudio/best'
        elif source == 'tiktok':
            url = vid
            o['http_headers']['Referer'] = 'https://www.tiktok.com/'
            if fmt in ('mp4', 'video'):
                o['format'] = 'best[ext=mp4]/best'
            elif FFMPEG_OK:
                o['format'] = 'bestaudio/best'
                o['postprocessors'] = [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}]
            else:
                o['format'] = 'bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio/best'
        else:
            url = vid if str(vid).startswith('http') else f"https://www.youtube.com/watch?v={vid}"
            o['extractor_args'] = {'youtube': {'player_client': ['ios', 'web_embedded', 'mweb']}}
            if fmt == 'mp3':
                if FFMPEG_OK:
                    o['format'] = 'bestaudio/best'
                    o['postprocessors'] = [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}]
                else:
                    o['format'] = 'bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio/best'
            else:
                if FFMPEG_OK:
                    o['format'] = f'bestvideo[height<={fmt}]+bestaudio/best[height<={fmt}]/best'
                    o['merge_output_format'] = 'mp4'
                else:
                    o['format'] = f'best[ext=mp4][height<={fmt}]/best[ext=mp4]/best'
        with yt_dlp.YoutubeDL(o) as y:
            info = y.extract_info(url, download=True)
            fn = y.prepare_filename(info)
            if o.get('postprocessors'):
                fn = os.path.splitext(fn)[0] + '.mp3'
            tasks[tid].update({'status': 'done', 'file': fn, 'title': info.get('title', 'audio'), 'source': source})
    except Exception as e:
        traceback.print_exc()
        tasks[tid].update({'status': 'error', 'error': str(e)})


@app.route('/api/download', methods=['POST'])
def download():
    b = request.json
    vid = b.get('video_id'); fmt = b.get('format', 'mp3'); source = b.get('source', 'youtube')
    if not vid: return jsonify({'error': 'Thiếu video_id'}), 400
    if source == 'soundcloud': fmt = 'mp3'
    tid = str(uuid.uuid4())
    tasks[tid] = {'status': 'downloading', 'progress': '0%', 'source': source}
    threading.Thread(target=dl_task, args=(tid, vid, fmt, source), daemon=True).start()
    return jsonify({'task_id': tid})


@app.route('/api/download_status/<tid>')
def dl_status(tid):
    return jsonify(tasks.get(tid, {'status': 'not_found'}))


@app.route('/api/download_file/<tid>')
def dl_file(tid):
    t = tasks.get(tid)
    if not t or t.get('status') != 'done': return "Chưa xong", 404
    if not os.path.exists(t['file']): return "File đã xoá", 404
    ext = os.path.splitext(t['file'])[1]
    safe = re.sub(r'[^\w\s\-\.]', '', t['title'])[:80].strip() or 'audio'
    mime = 'audio/mpeg' if ext == '.mp3' else 'audio/mp4' if ext in ('.m4a', '.aac') else 'video/mp4'
    return send_file(t['file'], as_attachment=True, download_name=safe + ext, mimetype=mime)


# ============ PARTY ============
def clean_room(code):
    room = PARTY_ROOMS.get(code)
    if not room: return
    now = time.time()
    for sid, m in list(room['members'].items()):
        if now - m.get('last_seen', 0) > 60:
            del room['members'][sid]
    if not room['members']:
        del PARTY_ROOMS[code]


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
            'queue': [], 'members': {sid: {'name': user, 'last_seen': time.time(), 'sid': sid}},
            'playing': None, 'position': 0, 'is_playing': False,
            'chat': [], 'host': user, 'created': time.time(),
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
        sid = uuid.uuid4().hex[:10]
        PARTY_ROOMS[code]['members'][sid] = {'name': user, 'last_seen': time.time(), 'sid': sid}
        return jsonify({'code': code, 'sid': sid, 'is_host': user == PARTY_ROOMS[code].get('host')})
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
            'code': code,
            'queue': room['queue'],
            'members': [{'name': m['name'], 'sid': k} for k, m in room['members'].items()],
            'playing': room['playing'], 'position': room['position'],
            'is_playing': room['is_playing'], 'chat': room['chat'][-30:],
            'host': room.get('host'),
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
    except:
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
    except:
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
    except:
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
    except:
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
    except:
        return jsonify({'ok': False}), 500


@app.route('/manifest.json')
def manifest():
    return jsonify({
        'name': 'Nova Music', 'short_name': 'Nova', 'start_url': '/',
        'display': 'standalone', 'background_color': '#07030e',
        'theme_color': '#a78bfa', 'orientation': 'portrait'
    })


@app.route('/sw.js')
def sw():
    s = """const C='nova-v5';const SH=['/','/manifest.json'];
self.addEventListener('install',e=>{e.waitUntil(caches.open(C).then(c=>c.addAll(SH)).then(()=>self.skipWaiting()))});
self.addEventListener('activate',e=>{e.waitUntil(caches.keys().then(k=>Promise.all(k.filter(x=>x!==C).map(x=>caches.delete(x)))).then(()=>self.clients.claim()))});
self.addEventListener('fetch',e=>{const u=new URL(e.request.url);
if(u.pathname.startsWith('/static/')||u.pathname==='/'||u.pathname==='/manifest.json'){
e.respondWith(caches.match(e.request).then(c=>c||fetch(e.request).then(r=>{const cl=r.clone();caches.open(C).then(cc=>cc.put(e.request,cl));return r})));}});"""
    return Response(s, mimetype='application/javascript')


@app.route('/api/debug')
def debug():
    import yt_dlp as y
    return jsonify({
        'yt_dlp': y.version.__version__, 'ffmpeg': FFMPEG_OK,
        'cookies': bool(COOKIES_FILE),
        'cache': len(stream_cache), 'rooms': len(PARTY_ROOMS)
    })


if __name__ == '__main__':
    import yt_dlp as y
    print(f"\n✅ Nova — yt-dlp {y.version.__version__}")
    print(f"   ffmpeg: {'✅' if FFMPEG_OK else '⚠️'}")
    print(f"   cookies: {'✅' if COOKIES_FILE else '❌'}")
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, port=port, threaded=True, host='0.0.0.0')
