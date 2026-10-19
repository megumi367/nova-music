from flask import Flask, request, jsonify, Response, stream_with_context, send_file, send_from_directory
from flask_compress import Compress
import yt_dlp, requests, re, time, os, uuid, threading, subprocess, base64, urllib.parse, tempfile
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

app = Flask(__name__, template_folder='templates', static_folder='static')
app.config['JSON_AS_ASCII'] = False
Compress(app)

stream_cache, tasks, PARTY_ROOMS = {}, {}, {}
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
_exec = ThreadPoolExecutor(max_workers=16)

COOKIES_FILE = None
_cookies_env = os.environ.get('NOVA_COOKIES')
if _cookies_env:
    if os.path.exists(_cookies_env): COOKIES_FILE = _cookies_env
    else:
        _tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8')
        _tmp.write(_cookies_env); _tmp.close(); COOKIES_FILE = _tmp.name

FFMPEG_OK = False
try:
    subprocess.run(['ffmpeg', '-version'], capture_output=True, timeout=3, check=True)
    FFMPEG_OK = True
except: pass

def cleanup_loop():
    while True:
        time.sleep(3600)
        now = time.time()
        try:
            for f in os.listdir(DOWNLOAD_DIR):
                fp = os.path.join(DOWNLOAD_DIR, f)
                if os.path.isfile(fp) and os.stat(fp).st_mtime < now - 3600: os.remove(fp)
            for t in list(tasks.keys()):
                if tasks[t].get('status') in ('done', 'error') and now - tasks[t].get('_ts', now) > 3600: del tasks[t]
            for k in list(stream_cache.keys()):
                if now - stream_cache[k]['ts'] > 7000: del stream_cache[k]
        except: pass
threading.Thread(target=cleanup_loop, daemon=True).start()

def base_opts():
    o = {'quiet': True, 'no_warnings': True, 'nocheckcertificate': True, 'socket_timeout': 20,
         'retries': 3, 'fragment_retries': 3, 'noplaylist': True, 'extractor_retries': 3, 'geo_bypass': True}
    if COOKIES_FILE and os.path.exists(COOKIES_FILE): o['cookiefile'] = COOKIES_FILE
    return o

def run_to(fn, timeout=30, *args):
    f = _exec.submit(fn, *args)
    try: return f.result(timeout=timeout)
    except FutureTimeout: raise Exception("Timeout")

def norm_sc(vid):
    vid = str(vid).strip()
    if vid.isdigit(): return f"https://soundcloud.com/tracks/{vid}"
    u = urllib.parse.unquote(vid)
    m = re.search(r'soundcloud[:\/]tracks[:\/](\d+)', u)
    if m: return f"https://soundcloud.com/tracks/{m.group(1)}"
    if not u.startswith('http') and '/' in u: return f"https://soundcloud.com/{u}"
    return u

def extract_audio(vid):
    def _do():
        o = base_opts(); o['format'] = 'bestaudio/best'
        o['http_headers'] = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://soundcloud.com/'}
        with yt_dlp.YoutubeDL(o) as y: return y.extract_info(vid, download=False)
    info = run_to(_do)
    u = info.get('url')
    if not u:
        for f in reversed(info.get('formats') or []):
            if f.get('url'): u = f['url']; info['protocol'] = f.get('protocol'); break
    if not u: raise Exception("No stream URL")
    is_hls = 'm3u8' in u.lower() or 'm3u8' in (info.get('protocol') or '').lower()
    return {
        'title': info.get('title'), 'thumbnail': info.get('thumbnail') or (info.get('thumbnails') or [{}])[-1].get('url'),
        'duration': info.get('duration'), 'uploader': info.get('uploader') or info.get('channel'),
        'id': info.get('id') or vid, 'url': u, 'is_hls': is_hls
    }

def get_cached(vid):
    key = f"sc:{vid}"
    if key in stream_cache and time.time() - stream_cache[key]['ts'] < 7000: return stream_cache[key]['data']
    info = extract_audio(vid)
    stream_cache[key] = {'data': info, 'ts': time.time()}
    return info

def b64e(s): return base64.urlsafe_b64encode(s.encode()).decode().rstrip('=')
def b64d(s): return base64.urlsafe_b64decode(s + '=' * (-len(s) % 4)).decode()

def sc_search(q, limit=24):
    def _do():
        o = base_opts(); o.update({'extract_flat': True, 'skip_download': True, 'playlistend': limit})
        with yt_dlp.YoutubeDL(o) as y: return y.extract_info(f"scsearch{limit}:{q}", download=False)
    try:
        info = run_to(_do, timeout=20)
        out = []
        for e in (info.get('entries') or []):
            if not e or not e.get('url'): continue
            thumb = next((x['url'] for x in reversed(e.get('thumbnails') or []) if x.get('url')), '')
            dur = e.get('duration')
            if dur and dur > 100000: dur /= 1000
            out.append({'id': e.get('url'), 'title': e.get('title'), 'duration': dur, 'thumb': thumb, 'channel': e.get('uploader')})
        return out
    except: return []

# ============ ROUTES ============
@app.route('/')
def index(): return send_from_directory('templates', 'index.html')

@app.route('/api/search')
def api_search():
    q = request.args.get('q', '').strip()
    return jsonify(sc_search(q) if q else [])

@app.route('/api/suggest')
def api_suggest():
    q = request.args.get('q', '').strip()
    if not q or len(q) < 2: return jsonify([])
    try:
        r = requests.get('https://suggestqueries.google.com/complete/search', params={'client': 'firefox', 'q': q}, timeout=3)
        return jsonify(r.json()[1][:6])
    except: return jsonify([])

@app.route('/api/lyrics')
def api_lyrics():
    artist = request.args.get('artist', '')
    title = request.args.get('title', '')
    if not artist or not title: return jsonify({'error': 'Thiếu thông tin'}), 400
    try:
        r = requests.get(f"https://api.lyrics.ovh/v1/{urllib.parse.quote(artist)}/{urllib.parse.quote(title)}", timeout=6)
        if r.status_code == 200: return jsonify(r.json())
    except: pass
    return jsonify({'error': 'Không tìm thấy lời'}), 404

@app.route('/api/play', methods=['POST'])
def api_play():
    try:
        vid = norm_sc(request.json.get('video_id'))
        info = get_cached(vid)
        resp = {'title': info['title'], 'thumbnail': info['thumbnail'], 'duration': info['duration'], 'uploader': info['uploader'], 'id': info['id'], 'is_hls': info['is_hls']}
        if info['is_hls']: resp['hls_token'] = b64e(info['url'])
        else: resp['url'] = info['url']
        return jsonify(resp)
    except Exception as e: return jsonify({'error': str(e)}), 400

def rewrite_m3u8(c, base):
    out = []
    for line in c.split('\n'):
        s = line.strip()
        if not s: out.append(line); continue
        if s.startswith('#'):
            if 'URI="' in s: s = re.sub(r'URI="([^"]+)"', lambda m: f'URI="/hls_proxy?t={b64e(urllib.parse.urljoin(base, m.group(1)))}"', s)
            out.append(s)
        else: out.append(f'/hls_proxy?t={b64e(urllib.parse.urljoin(base, s))}')
    return '\n'.join(out)

@app.route('/hls_proxy')
def hls_proxy():
    try: url = b64d(request.args.get('t', ''))
    except: return "Bad token", 400
    try: r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, stream=True, timeout=20)
    except Exception as e: return f"Error: {e}", 502
    if 'mpegurl' in r.headers.get('Content-Type', '').lower() or url.endswith('.m3u8'):
        return Response(rewrite_m3u8(r.text, url), mimetype='application/vnd.apple.mpegurl')
    return Response(stream_with_context(r.iter_content(16384)), mimetype=r.headers.get('Content-Type', 'audio/mpeg'))

@app.route('/stream/<path:vid>')
def stream(vid):
    try: info = get_cached(norm_sc(vid))
    except Exception as e: return f"Lỗi: {e}", 500
    hdrs = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://soundcloud.com/'}
    if 'Range' in request.headers: hdrs['Range'] = request.headers['Range']
    req = requests.get(info['url'], stream=True, headers=hdrs, timeout=30)
    h = {'Accept-Ranges': 'bytes', 'Content-Type': req.headers.get('Content-Type', 'audio/mpeg'),
         'Access-Control-Allow-Origin': '*'}
    for k in ('Content-Length', 'Content-Range'):
        if k in req.headers: h[k] = req.headers[k]
    return Response(stream_with_context(req.iter_content(65536)), status=req.status_code, headers=h)

# ============ PARTY ============
@app.route('/api/party/create', methods=['POST'])
def party_create():
    code = uuid.uuid4().hex[:6].upper()
    sid = uuid.uuid4().hex[:12]
    PARTY_ROOMS[code] = {'members': {sid: {'name': request.json.get('user', 'Host'), 'seen': time.time()}},
                         'queue': [], 'playing': None, 'progress': 0, 'chat': [], 'host': sid}
    return jsonify({'code': code, 'sid': sid, 'is_host': True})

@app.route('/api/party/join', methods=['POST'])
def party_join():
    code = request.json.get('code', '').upper()
    if code not in PARTY_ROOMS: return jsonify({'error': 'Phòng không tồn tại'}), 404
    sid = uuid.uuid4().hex[:12]
    PARTY_ROOMS[code]['members'][sid] = {'name': request.json.get('user', 'Khách'), 'seen': time.time()}
    return jsonify({'code': code, 'sid': sid, 'is_host': False})

@app.route('/api/party/sync', methods=['POST'])
def party_sync():
    data = request.json
    code, sid = data.get('code', '').upper(), data.get('sid')
    if code not in PARTY_ROOMS: return jsonify({'error': 'Phòng đóng'}), 404
    room = PARTY_ROOMS[code]
    if sid in room['members']: room['members'][sid]['seen'] = time.time()
    for k, m in list(room['members'].items()):
        if time.time() - m['seen'] > 60: del room['members'][k]
    if not room['members']: del PARTY_ROOMS[code]; return jsonify({'error': 'Phòng đóng'}), 404
    act = data.get('action')
    if act == 'play': room['playing'] = data.get('track'); room['progress'] = 0
    elif act == 'sync': room['progress'] = data.get('progress')
    elif act == 'chat' and sid in room['members']: room['chat'].append({'user': room['members'][sid]['name'], 'text': data.get('text')})
    elif act == 'add': room['queue'].append(data.get('track'))
    return jsonify({
        'queue': room['queue'], 'playing': room['playing'], 'progress': room['progress'],
        'members': [m['name'] for m in room['members'].values()],
        'chat': room['chat'][-30:], 'host': room['host']
    })

# ============ DOWNLOAD ============
@app.route('/api/download', methods=['POST'])
def download():
    vid = request.json.get('video_id'); tid = str(uuid.uuid4())
    tasks[tid] = {'status': 'downloading', '_ts': time.time()}
    def _dl():
        try:
            o = base_opts(); o['outtmpl'] = os.path.join(DOWNLOAD_DIR, f"{tid}.%(ext)s"); o['format'] = 'bestaudio/best'
            if FFMPEG_OK: o['postprocessors'] = [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '320'}]
            with yt_dlp.YoutubeDL(o) as y:
                info = y.extract_info(norm_sc(vid), download=True)
                fn = y.prepare_filename(info)
                if FFMPEG_OK: fn = os.path.splitext(fn)[0] + '.mp3'
                tasks[tid].update({'status': 'done', 'file': fn, 'title': info.get('title', 'audio')})
        except Exception as e: tasks[tid].update({'status': 'error', 'error': str(e)})
    threading.Thread(target=_dl, daemon=True).start()
    return jsonify({'task_id': tid})

@app.route('/api/download_status/<tid>')
def dl_status(tid): return jsonify(tasks.get(tid, {'status': 'not_found'}))

@app.route('/api/download_file/<tid>')
def dl_file(tid):
    t = tasks.get(tid)
    if t and t.get('status') == 'done' and os.path.exists(t['file']):
        return send_file(t['file'], as_attachment=True, download_name=re.sub(r'[^\w\s\-\.]', '', t['title'])[:80] + '.mp3')
    return "Lỗi", 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, threaded=True)
