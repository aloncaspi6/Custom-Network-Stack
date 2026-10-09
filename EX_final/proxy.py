import argparse
import socket
import threading
import json

# [NEW] ייבוא מודול RUDP
from rudp import RUDPSocket

RUDP_PORT_OFFSET = 1  # [NEW] פורט RUDP = פורט TCP + 1

proxy_cache = {}


# ═══════════════════════════════════════════════════════════
# [UNCHANGED] לוגיקת ה-proxy - מעביר בקשה לשרת ב-TCP
# ═══════════════════════════════════════════════════════════
def proxy(req, server_host, server_port):
    mode = req.get("mode")
    options = req.get("options", {})
    use_cache = options.get("cache", True)

    if mode == "gpt":
        prompt = req.get("data", {}).get("prompt", "")
        cache_key = ("gpt", prompt)
    elif mode == "download":
        url = req.get("data", {}).get("url", "")
        cache_key = ("download", url)
    else:
        cache_key = None

    if use_cache and cache_key in proxy_cache:
        cached_resp = proxy_cache[cache_key]
        cached_copy = dict(cached_resp)
        meta = dict(cached_copy.get("meta", {}))
        meta["from_cache"] = "proxy"
        cached_copy["meta"] = meta
        return cached_copy

    # תמיד מעביר לשרת ב-TCP
    with socket.create_connection((server_host, server_port)) as s:
        req_json = json.dumps(req) + "\n"
        s.sendall(req_json.encode("utf-8"))

        resp_data = b""
        while not resp_data.endswith(b"\n"):
            chunk = s.recv(4096)
            if not chunk:
                break
            resp_data += chunk

    resp_obj = json.loads(resp_data.decode("utf-8"))

    if use_cache and cache_key is not None and resp_obj.get("ok"):
        proxy_cache[cache_key] = resp_obj

    return resp_obj


# ═══════════════════════════════════════════════════════════
# [UNCHANGED] טיפול בלקוח TCP
# ═══════════════════════════════════════════════════════════
def handle_tcp_client(conn, server_host, server_port):
    with conn:
        try:
            data = b""
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                data += chunk

                while b"\n" in data:
                    line, _, rest = data.partition(b"\n")
                    data = rest

                    req = json.loads(line.decode("utf-8"))
                    resp_obj = proxy(req, server_host, server_port)
                    resp_json = json.dumps(resp_obj) + "\n"
                    conn.sendall(resp_json.encode("utf-8"))

        except Exception:
            pass


# ═══════════════════════════════════════════════════════════
# [NEW] טיפול בלקוח RUDP
# הפרוקסי מקבל RUDP מהלקוח, מעביר TCP לשרת, מחזיר RUDP ללקוח
# ═══════════════════════════════════════════════════════════
def handle_rudp_client(data: bytes, addr: tuple, server_host: str, server_port: int, rudp_sock: RUDPSocket):
    import base64
    try:
        req = json.loads(data.decode("utf-8").strip())

        # ── download: מקבלים מהשרת JSON עם base64, מחזירים header + bytes נפרדים ──
        if req.get("mode") == "download":
            resp_obj = proxy(req, server_host, server_port)

            if not resp_obj.get("ok"):
                # שגיאה - שלח רק header
                rudp_sock.send_message((json.dumps(resp_obj) + "\n").encode(), addr)
                return

            # חלץ את ה-bytes מה-base64
            raw_bytes = base64.b64decode(resp_obj.pop("data"))

            # שליחה 1: header בלי ה-data
            rudp_sock.send_message((json.dumps(resp_obj) + "\n").encode(), addr)

            # שליחה 2: bytes גולמיים
            rudp_sock.send_message(raw_bytes, addr)

        else:
            # calc / gpt - כרגיל
            resp_obj = proxy(req, server_host, server_port)
            rudp_sock.send_message((json.dumps(resp_obj) + "\n").encode("utf-8"), addr)

    except Exception as e:
        print(f"[PROXY-RUDP] Error handling client {addr}: {e}")


# ═══════════════════════════════════════════════════════════
# [NEW] לולאת האזנה RUDP
# ═══════════════════════════════════════════════════════════
def serve_rudp(listen_host: str, listen_port: int, server_host: str, server_port: int):
    rudp_sock = RUDPSocket()
    rudp_sock.bind((listen_host, listen_port))
    print(f"[PROXY-RUDP] Listening on {listen_host}:{listen_port} -> {server_host}:{server_port} (TCP)")
    while True:
        try:
            data, addr = rudp_sock.recv_message()
            threading.Thread(
                target=handle_rudp_client,
                args=(data, addr, server_host, server_port, rudp_sock),
                daemon=True
            ).start()
        except Exception as e:
            print(f"[PROXY-RUDP] Error: {e}")


# ═══════════════════════════════════════════════════════════
# [MODIFIED] main - מפעיל TCP ו-RUDP במקביל
# ═══════════════════════════════════════════════════════════
def main():
    ap = argparse.ArgumentParser(description="Transparent proxy - TCP + RUDP")
    ap.add_argument("--listen-host", default="127.0.0.2")
    ap.add_argument("--listen-port", type=int, default=5554)
    ap.add_argument("--server-host", default="127.0.0.1")
    ap.add_argument("--server-port", type=int, default=5555)
    args = ap.parse_args()

    rudp_port = args.listen_port + RUDP_PORT_OFFSET  # [NEW] פורט RUDP = 5555

    # [NEW] הפעלת RUDP ב-thread נפרד
    threading.Thread(
        target=serve_rudp,
        args=(args.listen_host, rudp_port, args.server_host, args.server_port),
        daemon=True
    ).start()

    # [UNCHANGED] הפעלת TCP בלולאה הראשית
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((args.listen_host, args.listen_port))
        s.listen(16)
        print(f"[PROXY-TCP] Listening on {args.listen_host}:{args.listen_port} -> {args.server_host}:{args.server_port}")

        while True:
            conn, _ = s.accept()
            threading.Thread(
                target=handle_tcp_client,
                args=(conn, args.server_host, args.server_port),
                daemon=True
            ).start()


if __name__ == "__main__":
    main()