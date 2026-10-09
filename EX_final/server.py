import argparse, socket, json, time, threading, collections
import urllib.request, urllib.error
from typing import Any, Dict
from google import genai

# ═══════════════════════════════════════════════════════════
# הגדרות HTTP Download
# ═══════════════════════════════════════════════════════════
MAX_FILE_SIZE = 2048 * 1024
DOWNLOAD_TIMEOUT = 10            # שניות לניסיון הורדה
ALLOWED_CONTENT_TYPES = (        # סוגי קבצים מותרים
    "text/", "image/", "application/json", "application/pdf",
    "application/octet-stream"
)
from rudp import RUDPSocket
RUDP_PORT_OFFSET = 1

# ═══════════════════════════════════════════════════════════
# [UNCHANGED] LRU Cache
# ═══════════════════════════════════════════════════════════
class LRUCache:
    # LRU זוכר את הסדר של מה שהכנסנו, אם אין מקום מוציא את האחרון שהיה בשימוש.
    def __init__(self, capacity: int = 128):
        self.capacity = capacity
        self._d = collections.OrderedDict()

    def get(self, key):
        if key not in self._d:
            return None
        self._d.move_to_end(key)
        return self._d[key]

    def set(self, key, value):
        self._d[key] = value
        self._d.move_to_end(key)
        if len(self._d) > self.capacity:
            self._d.popitem(last=False)



def call_gemini(prompt: str) -> str:
    client = genai.Client()
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt
        )
        return response.text
    except Exception as e:
        return f"[GEMINI-ERROR] {e}"


# ═══════════════════════════════════════════════════════════
#  הורדת קובץ מה-URL
# ═══════════════════════════════════════════════════════════
def download_file(url: str) -> tuple:
    # ── בדיקת URL תקין ──
    if not url.startswith(("http://", "https://")):
        return {"ok": False, "error": "Invalid URL: must start with http:// or https://"}, None

    filename = url.split("/")[-1].split("?")[0] or "downloaded_file"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "RUDP-HTTP-Client/1.0"})
        with urllib.request.urlopen(req, timeout=DOWNLOAD_TIMEOUT) as response:
            content_type = response.headers.get("Content-Type", "application/octet-stream")

            if not any(content_type.startswith(ct) for ct in ALLOWED_CONTENT_TYPES):
                return {"ok": False, "error": f"Content-Type not allowed: {content_type}"}, None

            data = response.read(MAX_FILE_SIZE + 1)
            if len(data) > MAX_FILE_SIZE:
                return {"ok": False, "error": f"File too large (max {MAX_FILE_SIZE // 1024}KB)"}, None

            print(f"[DOWNLOAD] {url} → {len(data)} bytes ({content_type})")
            header = {
                "ok": True,
                "filename": filename,
                "content_type": content_type,
                "size": len(data)
            }
            return header, data

    except urllib.error.HTTPError as e:
        return {"ok": False, "error": f"HTTP {e.code}: {e.reason}"}, None
    except urllib.error.URLError as e:
        return {"ok": False, "error": f"URL error: {e.reason}"}, None
    except Exception as e:
        return {"ok": False, "error": f"Download failed: {e}"}, None


# ═══════════════════════════════════════════════════════════
#  טיפול בבקשה (calc או gpt)
# ═══════════════════════════════════════════════════════════
def handle_request(msg: Dict[str, Any], cache: LRUCache) -> Dict[str, Any]:
    mode = msg.get("mode")
    data = msg.get("data") or {}
    options = msg.get("options") or {}
    use_cache = bool(options.get("cache", True))

    started = time.time()
    # המפתח למטמון הוא כל ההודעה כדי לוודא שזה אותה בקשה בדיוק
    cache_key = json.dumps(msg, sort_keys=True)

    # אם כבר חישבנו את זה פעם אחת אז לא צריך שוב
    if use_cache:
        hit = cache.get(cache_key)
        if hit is not None:
            return {"ok": True, "result": hit, "meta": {"from_cache": True, "took_ms": int((time.time()-started)*1000)}}

    try:
        if mode == "gpt":
            prompt = data.get("prompt")
            if not prompt or not isinstance(prompt, str):
                return {"ok": False, "error": "Bad request: 'prompt' is required (string)"}
            res = call_gemini(prompt)
        elif mode == "download":
            url = data.get("url")
            if not url or not isinstance(url, str):
                return {"ok": False, "error": "Bad request: 'url' is required (string)"}
            import base64
            header, raw = download_file(url)
            took = int((time.time() - started) * 1000)
            if not header["ok"]:
                header["meta"] = {"from_cache": False, "took_ms": took}
                return header
            # TCP: שולחים הכל ב-JSON אחד עם base64
            header["data"] = base64.b64encode(raw).decode("utf-8")
            header["meta"] = {"from_cache": False, "took_ms": took}
            return header
        else:
            return {"ok": False, "error": "Bad request: unknown mode (use: gpt, download)"}

        took = int((time.time()-started)*1000)
        if use_cache:
            cache.set(cache_key, res)
        return {"ok": True, "result": res, "meta": {"from_cache": False, "took_ms": took}}
    except Exception as e:
        return {"ok": False, "error": f"Server error: {e}"}


# ═══════════════════════════════════════════════════════════
# טיפול בלקוח TCP בודד
# ═══════════════════════════════════════════════════════════
def handle_tcp_client(conn: socket.socket, addr, cache: LRUCache):
    with conn:
        try:
            raw = b""
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                raw += chunk
                # אם קיבלנו שתי הודעות בפעם אחת, נפצל לפי ירידת שורה
                while b"\n" in raw:
                    line, _, rest = raw.partition(b"\n")
                    raw = rest
                    msg = json.loads(line.decode("utf-8"))
                    resp = handle_request(msg, cache)
                    # כדי שהלקוח ידע שסיימנו מוסיפים \n
                    out = (json.dumps(resp, ensure_ascii=False) + "\n").encode("utf-8")
                    conn.sendall(out)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════
#  טיפול בלקוח RUDP בודד
# ═══════════════════════════════════════════════════════════
def handle_rudp_client(data: bytes, addr: tuple, cache: LRUCache, rudp_sock: RUDPSocket):
    try:
        msg = json.loads(data.decode("utf-8").strip())

        # ── מצב download: שולחים header + bytes בנפרד ──
        if msg.get("mode") == "download":
            url = (msg.get("data") or {}).get("url", "")
            if not url:
                err = (json.dumps({"ok": False, "error": "url required"}) + "\n").encode()
                rudp_sock.send_message(err, addr)
                return

            started = time.time()
            header, raw = download_file(url)
            took = int((time.time() - started) * 1000)
            header["meta"] = {"from_cache": False, "took_ms": took}

            # שליחה 1: header ב-JSON
            rudp_sock.send_message((json.dumps(header, ensure_ascii=False) + "\n").encode(), addr)

            # שליחה 2: bytes גולמיים (רק אם הצליח)
            if header["ok"] and raw:
                rudp_sock.send_message(raw, addr)
        else:
            # ── calc / gpt: כרגיל ──
            resp = handle_request(msg, cache)
            out = (json.dumps(resp, ensure_ascii=False) + "\n").encode("utf-8")
            rudp_sock.send_message(out, addr)

    except Exception as e:
        print(f"[RUDP] Error handling client {addr}: {e}")


# ═══════════════════════════════════════════════════════════
# לולאת האזנה TCP
# ═══════════════════════════════════════════════════════════
def serve_tcp(host: str, port: int, cache: LRUCache):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen(16)
        print(f"[TCP] Listening on {host}:{port}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_tcp_client, args=(conn, addr, cache), daemon=True).start()

# ═══════════════════════════════════════════════════════════
# לולאת האזנה RUDP
# ═══════════════════════════════════════════════════════════
def serve_rudp(host: str, port: int, cache: LRUCache):
    rudp_sock = RUDPSocket()
    rudp_sock.bind((host, port))
    print(f"[RUDP] Listening on {host}:{port}")
    while True:
        try:
            # מחכים להודעה שלמה מלקוח כלשהו
            data, addr = rudp_sock.recv_message()
            # כל לקוח מטופל ב-thread נפרד
            threading.Thread(target=handle_rudp_client, args=(data, addr, cache, rudp_sock), daemon=True).start()
        except Exception as e:
            print(f"[RUDP] Error: {e}")


# ═══════════════════════════════════════════════════════════
# main - מפעיל TCP ו-RUDP במקביל
# ═══════════════════════════════════════════════════════════
def main():
    ap = argparse.ArgumentParser(description="JSON server (calc/gpt) - TCP + RUDP")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5555)
    ap.add_argument("--cache-size", type=int, default=128)
    args = ap.parse_args()

    cache = LRUCache(args.cache_size)
    rudp_port = args.port + RUDP_PORT_OFFSET  # [NEW] פורט RUDP = 5556

    # הפעלת RUDP ב-thread נפרד
    threading.Thread(target=serve_rudp, args=(args.host, rudp_port, cache), daemon=True).start()

    # הפעלת TCP (בלולאה הראשית)
    serve_tcp(args.host, args.port, cache)
if __name__ == "__main__":
    main()