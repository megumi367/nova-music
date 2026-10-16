from flask import Flask, request, jsonify, Response, stream_with_context, send_file, send_from_directory
import yt_dlp, requests, re, time, traceback, os, uuid, threading, subprocess, base64, urllib.parse

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
url_cache = {}
tasks = {}
COOKIES_FILE = None
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

FFMPEG_OK = False
try:
    subprocess.run(['ffmpeg','-version'], capture_output=True, timeout=3, check=True)
    FFMPEG_OK = True
except: pass

# ==================== URL CACHE LOCKS ====================
_url_locks = {}
_url_locks_guard = threading.Lock()

def _get_lock(key):
    with _url_locks_guard:
        if key not in _url_locks:
            _url_locks[key] = threading.Lock()
        return _url_locks[key]

def base_opts():
    o = {'quiet': True, 'no_warnings': True, 'nocheckcertificate': True}
    if COOKIES_FILE: o['cookiefile'] = COOKIES_FILE
    return o

def b64e(s): return base64.urlsafe_b64encode(s.encode()).decode().rstrip('=')
def b64d(s): return base64.urlsafe_b64decode(s + '=' * (-len(s) % 4)).decode()

def build_sc_url(e):
    u = e.get('url') or e.get('webpage_url') or ''
    if u.startswith('http') and 'soundcloud.com' in u: return u
    p = e.get('permalink_url') or ''
    if p.startswith('http'): return p
    up = e.get('uploader') or e.get('channel') or e.get('uploader_id') or ''
    if up and p and not p.startswith('http'): return f"https://soundcloud.com/{up}/{p}"
    return None

def detect_source(url):
    if 'youtube.com' in url or 'youtu.be' in url: return 'youtube'
    if 'soundcloud.com' in url: return 'soundcloud'
    if 'tiktok.com' in url: return 'tiktok'
    return None

def get_stream_url(vid, mode='audio', source='youtube'):
    key = f"{source}_{vid}_{mode}"
    c = url_cache.get(key)
    if c and time.time() - c['ts'] < 7200:
        return c['data']

    lock = _get_lock(key)
    with lock:
        # double-check sau khi acquire lock
        c = url_cache.get(key)
        if c and time.time() - c['ts'] < 7200:
            return c['data']

        o = base_opts()

        if source == 'soundcloud':
            if not vid.startswith('http'):
                if vid and '/' in vid and 'soundcloud.com' not in vid: vid = f"https://soundcloud.com/{vid}"
                else: raise Exception("SC cần URL đầy đủ")
            o['http_headers'] = {'User-Agent':'Mozilla/5.0','Referer':'https://soundcloud.com/','Origin':'https://soundcloud.com'}
            o['geo_bypass'] = True
            attempts = [
                {'format':'bestaudio[ext=mp3]/bestaudio[protocol^=http]','extractor_args':{'soundcloud':{'formats':['http_mp3','progressive']}}},
                {'format':'bestaudio/best','extractor_args':{'soundcloud':{'formats':['http_mp3','hls_mp3','hls_aac','http_aac']}}},
                {'format':'bestaudio/best'},
            ]
            last=None; i=None
            for x in attempts:
                try:
                    with yt_dlp.YoutubeDL({**o,**x}) as y: i = y.extract_info(vid, download=False)
                    break
                except Exception as e: last=e; time.sleep(.3)
            if i is None: raise Exception(f"SC lỗi: {last}")
            u = i.get('url')
            if not u:
                for f in (i.get('formats') or []):
                    if f.get('url'): u = f['url']; break
            if not u: raise Exception("SC không có URL")
            proto = i.get('protocol','') or ''
            is_hls = 'm3u8' in u.lower() or 'hls' in proto.lower()
            dur = i.get('duration')
            if dur and dur > 100000: dur /= 1000
            d = {'url':u,'title':i.get('title'),'thumbnail':i.get('thumbnail') or (i.get('thumbnails') or [{}])[-1].get('url'),
                 'duration':dur,'uploader':i.get('uploader') or i.get('channel'),
                 'source':'soundcloud','id':vid,'is_hls':is_hls,
                 'headers':{'Referer':'https://soundcloud.com/','User-Agent':'Mozilla/5.0'}}

        elif source == 'tiktok':
            o['format'] = 'bestaudio/best' if mode == 'audio' else 'best[ext=mp4]/best'
            o['http_headers'] = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36','Referer':'https://www.tiktok.com/'}
            with yt_dlp.YoutubeDL(o) as y: i = y.extract_info(vid, download=False)
            u = i.get('url')
            if not u:
                for f in (i.get('formats') or []):
                    if f.get('url'): u = f['url']; break
            if not u: raise Exception("TikTok không có URL")
            d = {'url':u,'title':i.get('title') or i.get('description') or 'TikTok Video',
                 'thumbnail':i.get('thumbnail') or (i.get('thumbnails') or [{}])[-1].get('url'),
                 'duration':i.get('duration'),'uploader':i.get('uploader') or i.get('creator') or i.get('channel'),
                 'source':'tiktok','id':vid,'is_hls':False,
                 'headers':{'Referer':'https://www.tiktok.com/','User-Agent':'Mozilla/5.0'}}

        else:
            if FFMPEG_OK: o['format'] = 'bestaudio[ext=m4a]/bestaudio/best' if mode=='audio' else 'best[height<=720][ext=mp4]/best[height<=720]/best'
            else: o['format'] = 'bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio/best' if mode=='audio' else 'best[ext=mp4][height<=720]/best[ext=mp4]/best'
            with yt_dlp.YoutubeDL(o) as y: i = y.extract_info(vid if vid.startswith('http') else f"https://www.youtube.com/watch?v={vid}", download=False)
            d = {'url':i['url'],'title':i.get('title'),'thumbnail':i.get('thumbnail'),
                 'duration':i.get('duration'),'uploader':i.get('uploader'),
                 'source':'youtube','id':vid,'is_hls':False,'headers':{'User-Agent':'Mozilla/5.0'}}

        url_cache[key] = {'data':d,'ts':time.time()}
        return d

def yt_search(q, limit=20):
    o = base_opts(); o.update({'extract_flat':'in_playlist','skip_download':True,'playlistend':limit})
    with yt_dlp.YoutubeDL(o) as y: info = y.extract_info(f"ytsearch{limit}:{q}", download=False)
    out=[]
    for e in (info.get('entries') or []):
        if not e or not e.get('id'): continue
        t = e.get('thumbnails') or []
        thumb = next((x['url'] for x in reversed(t) if x.get('url')), None) or f"https://i.ytimg.com/vi/{e['id']}/mqdefault.jpg"
        out.append({'id':e['id'],'title':e.get('title') or '','duration':e.get('duration'),
                    'thumb':thumb,'channel':e.get('uploader') or e.get('channel') or '','source':'youtube'})
    return out

def sc_search(q, limit=20):
    o = base_opts(); o.update({'extract_flat':True,'skip_download':True,'playlistend':limit})
    with yt_dlp.YoutubeDL(o) as y: info = y.extract_info(f"scsearch{limit}:{q}", download=False)
    out=[]
    for e in (info.get('entries') or []):
        if not e: continue
        u = build_sc_url(e)
        if not u: continue
        t = e.get('thumbnails') or []
        thumb = next((x['url'] for x in reversed(t) if x.get('url')), None) or ''
        dur = e.get('duration')
        if dur and dur > 100000: dur /= 1000
        out.append({'id':u,'url':u,'title':e.get('title') or '','duration':dur,
                    'thumb':thumb,'channel':e.get('uploader') or e.get('channel') or '','source':'soundcloud'})
    return out

@app.route('/')
def index(): return send_from_directory('templates','index.html')

@app.route('/api/search')
def search():
    q = request.args.get('q','').strip()
    if not q: return jsonify([])
    try:
        r = yt_search(q)
        return jsonify(r if r else {'error':'Không có kết quả'}), (200 if r else 404)
    except Exception as e: return jsonify({'error':str(e)}), 500

@app.route('/api/sc_search')
def sc_search_route():
    q = request.args.get('q','').strip()
    if not q: return jsonify([])
    try:
        r = sc_search(q)
        return jsonify(r if r else {'error':'Không có kết quả'}), (200 if r else 404)
    except Exception as e: return jsonify({'error':str(e)}), 500

@app.route('/api/suggest')
def suggest():
    q = request.args.get('q','').strip()
    if not q or len(q) < 2: return jsonify([])
    try:
        r = requests.get('https://suggestqueries.google.com/complete/search', params={'client':'firefox','ds':'yt','q':q}, timeout=3)
        return jsonify(r.json()[1][:8])
    except: return jsonify([])

@app.route('/api/play', methods=['POST'])
def play():
    try:
        b = request.json
        info = get_stream_url(b.get('video_id'), b.get('mode','audio'), b.get('source','youtube'))
        if info.get('source') in ('soundcloud','tiktok'):
            resp = {'title':info.get('title'),'thumbnail':info.get('thumbnail'),'duration':info.get('duration'),
                    'uploader':info.get('uploader'),'source':info.get('source'),'id':info.get('id'),
                    'is_hls':info.get('is_hls',False)}
            if info.get('is_hls') and info.get('url'): resp['hls_token'] = b64e(info['url'])
            return jsonify(resp)
        return jsonify(info)
    except Exception as e: return jsonify({'error':str(e)}), 400

@app.route('/api/parse', methods=['POST'])
def parse():
    url = (request.json.get('url') or '').strip()
    if not url: return jsonify({'error':'Chưa có link'}), 400
    src = detect_source(url)
    if not src: return jsonify({'error':'Chỉ hỗ trợ YouTube, SoundCloud, TikTok'}), 400
    try:
        if src == 'youtube':
            m = re.search(r'(?:v=|youtu\.be/|shorts/|embed/)([A-Za-z0-9_-]{11})', url)
            vid = m.group(1) if m else url
            with yt_dlp.YoutubeDL(base_opts()) as y:
                i = y.extract_info(f"https://www.youtube.com/watch?v={vid}", download=False)
            return jsonify({'video_id':vid,'source':'youtube','title':i.get('title'),'thumbnail':i.get('thumbnail'),
                            'duration':i.get('duration'),'uploader':i.get('uploader')})
        else:
            info = get_stream_url(url,'audio',src)
            return jsonify({'video_id':url,'source':src,'title':info.get('title'),'thumbnail':info.get('thumbnail'),
                            'duration':info.get('duration'),'uploader':info.get('uploader')})
    except Exception as e:
        return jsonify({'error':str(e)}), 400

@app.route('/api/similar', methods=['POST'])
def similar():
    data = request.json
    title = (data.get('title') or '').strip()
    channel = (data.get('channel') or '').strip()
    source = data.get('source', 'youtube')
    if not title and not channel: return jsonify({'error': 'Thiếu thông tin'}), 400
    noise = r'\b(official|video|audio|lyrics|lyric|mv|m/v|hd|4k|remastered|remaster|prod\.?|ft\.?|feat\.?|featuring|topic|records|music|version|edit|live|acoustic|instrumental|original)\b'
    clean = re.sub(noise, '', title, flags=re.IGNORECASE)
    clean = re.sub(r'[\(\[\{].*?[\)\]\}]', '', clean)
    clean = re.sub(r'[^\w\s\u00C0-\u024F\u1E00-\u1EFF]', ' ', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    queries = []
    if channel and len(channel) < 40: queries.append(channel)
    if clean and len(clean) > 3: queries.append(clean)
    if channel and clean: queries.append(f'{clean} {channel}')
    all_results = []; seen_ids = set()
    for q in queries:
        try:
            results = sc_search(q, 12) if source == 'soundcloud' else yt_search(q, 12)
            for r in results:
                if r['id'] not in seen_ids:
                    seen_ids.add(r['id']); all_results.append(r)
        except: pass
        if len(all_results) >= 20: break
    return jsonify(all_results[:20])

@app.route('/api/lyrics')
def lyrics():
    artist = request.args.get('artist',''); title = request.args.get('title','')
    if not artist or not title: return jsonify({'error':'Thiếu thông tin'}), 400
    try:
        r = requests.get(f"https://api.lyrics.ovh/v1/{artist}/{title}", timeout=6)
        if r.status_code == 200: return jsonify(r.json())
        return jsonify({'error':'Không có lời'}), 404
    except: return jsonify({'error':'Không có lời'}), 404

def rewrite_m3u8(c, base):
    out=[]
    for line in c.split('\n'):
        s = line.strip()
        if not s: out.append(line); continue
        if s.startswith('#'):
            if 'URI="' in s:
                def repl(m): return f'URI="/hls_proxy?t={b64e(urllib.parse.urljoin(base, m.group(1)))}"'
                s = re.sub(r'URI="([^"]+)"', repl, s)
            out.append(s)
        else: out.append(f'/hls_proxy?t={b64e(urllib.parse.urljoin(base, s))}')
    return '\n'.join(out)

@app.route('/hls_proxy')
def hls_proxy():
    token = request.args.get('t','')
    if not token: return "Missing", 400
    try: url = b64d(token)
    except: return "Bad token", 400
    if not url.startswith('http'): return "Bad URL", 400
    headers = {'User-Agent':'Mozilla/5.0','Referer':request.headers.get('Referer','https://soundcloud.com/')}
    try: r = requests.get(url, headers=headers, stream=True, timeout=20)
    except Exception as e: return f"Error: {e}", 502
    ct = (r.headers.get('Content-Type') or '').lower()
    is_pl = 'mpegurl' in ct or url.lower().split('?')[0].endswith('.m3u8')
    if is_pl:
        try: c = r.text
        except: return "Read error", 502
        return Response(rewrite_m3u8(c, url), mimetype='application/vnd.apple.mpegurl',
                        headers={'Access-Control-Allow-Origin':'*','Cache-Control':'no-cache'})
    def gen():
        try:
            for chunk in r.iter_content(16384):
                if chunk: yield chunk
        except: pass
        finally:
            try: r.close()
            except: pass
    return Response(stream_with_context(gen()), mimetype=ct or 'audio/aac',
                    headers={'Access-Control-Allow-Origin':'*','Cache-Control':'no-cache'})

@app.route('/stream/<path:vid>')
def stream(vid):
    mode = request.args.get('mode','audio'); source = request.args.get('source','youtube')
    try: info = get_stream_url(vid, mode, source)
    except Exception as e: return f"Lỗi: {e}", 500
    if info.get('is_hls'): return "Dùng /hls_proxy", 400
    hdrs = {'User-Agent':'Mozilla/5.0'}
    if info.get('headers'): hdrs.update(info['headers'])
    rng = request.headers.get('Range','')
    if rng: hdrs['Range'] = rng
    try: req = requests.get(info['url'], stream=True, headers=hdrs, timeout=30)
    except Exception as e: return f"Stream error: {e}", 502
    h = {'Accept-Ranges':'bytes','Cache-Control':'no-cache'}
    for k in ('Content-Type','Content-Length','Content-Range'):
        if k in req.headers: h[k] = req.headers[k]
    return Response(stream_with_context(req.iter_content(16384)), status=req.status_code, headers=h)

def download_task(tid, vid, fmt, source='youtube'):
    def hook(d):
        if d['status'] == 'downloading': tasks[tid]['progress'] = d.get('_percent_str','').strip()
        elif d['status'] == 'finished': tasks[tid]['status'] = 'processing'
    try:
        outtmpl = os.path.join(DOWNLOAD_DIR, f"{tid}.%(ext)s")
        o = base_opts(); o['outtmpl'] = outtmpl; o['progress_hooks'] = [hook]; o['retries'] = 5; o['fragment_retries'] = 5
        o['http_headers'] = {'User-Agent':'Mozilla/5.0'}
        if source == 'soundcloud':
            url = vid if vid.startswith('http') else f"https://soundcloud.com/{vid}"
            o['http_headers']['Referer'] = 'https://soundcloud.com/'
            if FFMPEG_OK:
                o['format'] = 'bestaudio/best'
                o['postprocessors'] = [{'key':'FFmpegExtractAudio','preferredcodec':'mp3','preferredquality':'192'}]
            else: o['format'] = 'bestaudio[ext=mp3]/bestaudio[ext=m4a]/bestaudio/best'
        elif source == 'tiktok':
            url = vid
            o['http_headers']['Referer'] = 'https://www.tiktok.com/'
            if fmt == 'mp4' or fmt == 'video': o['format'] = 'best[ext=mp4]/best'
            else:
                if FFMPEG_OK:
                    o['format'] = 'bestaudio/best'
                    o['postprocessors'] = [{'key':'FFmpegExtractAudio','preferredcodec':'mp3','preferredquality':'192'}]
                else: o['format'] = 'bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio/best'
        else:
            url = f"https://www.youtube.com/watch?v={vid}"
            if fmt == 'mp3':
                if FFMPEG_OK:
                    o['format'] = 'bestaudio/best'
                    o['postprocessors'] = [{'key':'FFmpegExtractAudio','preferredcodec':'mp3','preferredquality':'192'}]
                else: o['format'] = 'bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio/best'
            else:
                if FFMPEG_OK:
                    o['format'] = f'bestvideo[height<={fmt}]+bestaudio/best[height<={fmt}]/best'
                    o['merge_output_format'] = 'mp4'
                else: o['format'] = f'best[ext=mp4][height<={fmt}]/best[ext=mp4]/best'
        with yt_dlp.YoutubeDL(o) as y:
            info = y.extract_info(url, download=True)
            fn = y.prepare_filename(info)
            if 'postprocessors' in o and o['postprocessors']: fn = os.path.splitext(fn)[0] + '.mp3'
            tasks[tid].update({'status':'done','file':fn,'title':info.get('title','audio'),'source':source})
    except Exception as e:
        traceback.print_exc(); tasks[tid].update({'status':'error','error':str(e)})

@app.route('/api/download', methods=['POST'])
def download():
    b = request.json
    vid = b.get('video_id'); fmt = b.get('format','mp3'); source = b.get('source','youtube')
    if not vid: return jsonify({'error':'Thiếu video_id'}), 400
    if source == 'soundcloud': fmt = 'mp3'
    tid = str(uuid.uuid4())
    tasks[tid] = {'status':'downloading','progress':'0%','source':source}
    threading.Thread(target=download_task, args=(tid, vid, fmt, source), daemon=True).start()
    return jsonify({'task_id':tid})

@app.route('/api/download_status/<tid>')
def download_status(tid): return jsonify(tasks.get(tid, {'status':'not_found'}))

@app.route('/api/download_file/<tid>')
def download_file(tid):
    t = tasks.get(tid)
    if not t or t.get('status') != 'done': return "Chưa xong", 404
    if not os.path.exists(t['file']): return "File đã bị xoá", 404
    ext = os.path.splitext(t['file'])[1]
    safe = re.sub(r'[^\w\s\-\.]', '', t['title'])[:80].strip() or 'audio'
    mime = 'audio/mpeg' if ext == '.mp3' else 'audio/mp4' if ext in ('.m4a','.aac') else 'video/mp4'
    return send_file(t['file'], as_attachment=True, download_name=safe + ext, mimetype=mime)

# ==================== PARTY MODE ====================
PARTY_ROOMS = {}

def cleanup_room(code):
    room = PARTY_ROOMS.get(code)
    if not room: return
    now = time.time()
    for sid, m in list(room['members'].items()):
        if now - m.get('last_seen', 0) > 30:
            del room['members'][sid]
    if not room['members']: del PARTY_ROOMS[code]

@app.route('/api/party/create', methods=['POST', 'GET'])
def party_create():
    try:
        data = request.get_json(silent=True) or request.args.to_dict() or {}
        user = (data.get('user') or 'Host').strip()[:20]
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
        return jsonify({'error': f'Server error: {e}'}), 500

@app.route('/api/party/join', methods=['POST'])
def party_join():
    try:
        data = request.get_json(silent=True) or {}
        code = (data.get('code') or '').upper().strip()
        user = (data.get('user') or 'Khách').strip()[:20]
        if not code: return jsonify({'error': 'Thiếu mã phòng'}), 400
        if code not in PARTY_ROOMS: return jsonify({'error': 'Không tìm thấy phòng'}), 404
        sid = uuid.uuid4().hex[:10]
        PARTY_ROOMS[code]['members'][sid] = {'name': user, 'last_seen': time.time(), 'sid': sid}
        return jsonify({'code': code, 'sid': sid, 'is_host': user == PARTY_ROOMS[code].get('host')})
    except Exception as e: return jsonify({'error': str(e)}), 500

@app.route('/api/party/state', methods=['POST'])
def party_state():
    try:
        data = request.get_json(silent=True) or {}
        code = (data.get('code') or '').upper()
        sid = data.get('sid')
        if code not in PARTY_ROOMS: return jsonify({'error': 'Phòng đã đóng'}), 404
        room = PARTY_ROOMS[code]
        if sid and sid in room['members']: room['members'][sid]['last_seen'] = time.time()
        cleanup_room(code)
        if code not in PARTY_ROOMS: return jsonify({'error': 'Phòng đã đóng'}), 404
        room = PARTY_ROOMS[code]
        return jsonify({
            'queue': room['queue'],
            'members': [{'name': m['name'], 'sid': k} for k, m in room['members'].items()],
            'playing': room['playing'], 'position': room['position'],
            'is_playing': room['is_playing'], 'chat': room['chat'][-30:],
            'host': room.get('host'),
        })
    except Exception as e: return jsonify({'error': str(e)}), 500

@app.route('/api/party/leave', methods=['POST'])
def party_leave():
    try:
        data = request.get_json(silent=True) or {}
        code = (data.get('code') or '').upper()
        sid = data.get('sid')
        if code in PARTY_ROOMS and sid in PARTY_ROOMS[code]['members']:
            del PARTY_ROOMS[code]['members'][sid]
            cleanup_room(code)
        return jsonify({'ok': True})
    except: return jsonify({'ok': True})

@app.route('/api/party/add', methods=['POST'])
def party_add():
    try:
        data = request.get_json(silent=True) or {}
        code = (data.get('code') or '').upper()
        track = data.get('track')
        if code in PARTY_ROOMS and track: PARTY_ROOMS[code]['queue'].append(track)
        return jsonify({'ok': True})
    except: return jsonify({'ok': False}), 500

@app.route('/api/party/remove', methods=['POST'])
def party_remove():
    try:
        data = request.get_json(silent=True) or {}
        code = (data.get('code') or '').upper()
        idx = data.get('idx')
        if code in PARTY_ROOMS and isinstance(idx, int) and 0 <= idx < len(PARTY_ROOMS[code]['queue']):
            PARTY_ROOMS[code]['queue'].pop(idx)
        return jsonify({'ok': True})
    except: return jsonify({'ok': False}), 500

@app.route('/api/party/play', methods=['POST'])
def party_play():
    try:
        data = request.get_json(silent=True) or {}
        code = (data.get('code') or '').upper()
        if code in PARTY_ROOMS:
            PARTY_ROOMS[code]['playing'] = data.get('track')
            PARTY_ROOMS[code]['position'] = 0
            PARTY_ROOMS[code]['is_playing'] = True
        return jsonify({'ok': True})
    except: return jsonify({'ok': False}), 500

@app.route('/api/party/sync', methods=['POST'])
def party_sync():
    try:
        data = request.get_json(silent=True) or {}
        code = (data.get('code') or '').upper()
        if code in PARTY_ROOMS:
            PARTY_ROOMS[code]['position'] = data.get('position', 0)
            PARTY_ROOMS[code]['is_playing'] = data.get('is_playing', True)
        return jsonify({'ok': True})
    except: return jsonify({'ok': False}), 500

@app.route('/api/party/chat', methods=['POST'])
def party_chat():
    try:
        data = request.get_json(silent=True) or {}
        code = (data.get('code') or '').upper()
        user = (data.get('user') or 'Khách')[:20]
        text = (data.get('text') or '').strip()[:200]
        if code in PARTY_ROOMS and text:
            PARTY_ROOMS[code]['chat'].append({'user': user, 'text': text, 'ts': time.time()})
            if len(PARTY_ROOMS[code]['chat']) > 100: PARTY_ROOMS[code]['chat'] = PARTY_ROOMS[code]['chat'][-100:]
        return jsonify({'ok': True})
    except: return jsonify({'ok': False}), 500

@app.route('/manifest.json')
def manifest():
    return jsonify({'name':'Nova Music · Studio','short_name':'Nova','start_url':'/','display':'standalone',
                    'background_color':'#060608','theme_color':'#a78bfa','orientation':'portrait'})

@app.route('/sw.js')
def service_worker():
    sw = """const CACHE='nova-v17';const SHELL=['/manifest.json'];
self.addEventListener('install',e=>{e.waitUntil(caches.open(CACHE).then(c=>c.addAll(SHELL)).then(()=>self.skipWaiting()));});
self.addEventListener('activate',e=>{e.waitUntil(caches.keys().then(ks=>Promise.all(ks.filter(k=>k!==CACHE).map(k=>caches.delete(k)))).then(()=>self.clients.claim()));});
self.addEventListener('fetch',e=>{const u=new URL(e.request.url);if(e.request.method!=='GET')return;
if(u.pathname==='/'){e.respondWith(fetch(e.request).then(r=>{const cl=r.clone();caches.open(CACHE).then(c=>c.put(e.request,cl));return r;}).catch(()=>caches.match(e.request)));return;}
if(u.pathname.startsWith('/static/')||u.pathname==='/manifest.json'){
e.respondWith(caches.match(e.request).then(c=>c||fetch(e.request).then(r=>{const cl=r.clone();caches.open(CACHE).then(cc=>cc.put(e.request,cl));return r;})));}});"""
    return Response(sw, mimetype='application/javascript', headers={'Cache-Control':'no-cache'})

@app.route('/api/debug')
def debug():
    import yt_dlp as y
    return jsonify({
        'version': 'v17-studio',
        'yt_dlp': y.version.__version__,
        'ffmpeg': FFMPEG_OK,
        'party_rooms': len(PARTY_ROOMS),
        'url_cache': len(url_cache),
        'tasks': len(tasks),
    })

if __name__ == '__main__':
    import yt_dlp as y
    print(f"\n╔════════════════════════════════════════╗")
    print(f"║  ✨ Nova v17 · Studio                  ║")
    print(f"╚════════════════════════════════════════╝")
    print(f"   yt-dlp:     {y.version.__version__}")
    print(f"   ffmpeg:     {'✅ OK' if FFMPEG_OK else '⚠️  Thiếu (chỉ audio m4a)'}")
    print(f"   Party:      ✅ HTTP polling")
    print(f"   Auto-next:  ✅ watchdog + prefetch + race-guard")
    print(f"   UI:         ✅ Studio redesign")
    print(f"   Debug:      http://localhost:5000/api/debug\n")
    app.run(debug=True, port=5000, threaded=True, host='0.0.0.0')
