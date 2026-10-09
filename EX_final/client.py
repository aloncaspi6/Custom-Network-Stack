import argparse, socket, json, uuid, os
from rudp import RUDPSocket

RUDP_PORT_OFFSET = 1  # [NEW] פורט ה-RUDP = פורט TCP + 1 (5556 במקום 5555)


# ═══════════════════════════════════════════════════════════
#  פונקציה לשליחה/קבלה מעל TCP
# ═══════════════════════════════════════════════════════════
def request(sock: socket.socket, payload: dict) -> dict:
    data = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
    sock.sendall(data)
    buff = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk: return {"ok": False, "error": "Connection closed"}
        buff += chunk
        if b"\n" in buff:
            line, _, _ = buff.partition(b"\n")
            return json.loads(line.decode("utf-8"))
# ═══════════════════════════════════════════════════════════
# פונקציה לשליחה/קבלה מעל RUDP
# מקבל socket קיים כדי לשמור על אותו פורט לאורך כל הסשן
# ═══════════════════════════════════════════════════════════
def request_rudp(rudp_sock: RUDPSocket, payload: dict, addr: tuple) -> dict:
    rudp_sock.last_ack = -1
    rudp_sock.dup_acks = 0
    rudp_sock.cwnd = 1
    rudp_sock.rwnd = 32
    try:
        data = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
        rudp_sock.send_message(data, addr)
        if payload.get("mode") == "download":
            header_raw, _ = rudp_sock.recv_message(timeout=60.0)
            header = json.loads(header_raw.decode("utf-8").strip())
            if not header.get("ok"):
                return header
            # קבלת הקובץ עצמו
            file_bytes, _ = rudp_sock.recv_message(timeout=30.0)
            header["_raw_bytes"] = file_bytes   # שמור bytes לשמירה לדיסק
            return header
        else:
            response, _ = rudp_sock.recv_message(timeout=10.0)
            return json.loads(response.decode("utf-8").strip())

    except Exception as e:
        return {"ok": False, "error": f"RUDP error: {e}"}


# ═══════════════════════════════════════════════════════════
# [UNCHANGED] קבלת IP מ-DHCP
# ═══════════════════════════════════════════════════════════
def get_my_ip_from_dhcp(cid):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(3)
        try:
            s.sendto(json.dumps({"type": "DISCOVER", "client_id": cid}).encode(), ("127.0.0.1", 6767))
            data, addr = s.recvfrom(1024)
            offer = json.loads(data.decode())

            if offer.get("type") == "NAK":
                print(f"[DHCP] NAK: {offer.get('reason')}")
                return None

            offered_ip = offer.get("assigned_ip")
            if not offered_ip: return None

            s.sendto(json.dumps({
                "type": "REQUEST",
                "client_id": cid,
                "requested_ip": offered_ip
            }).encode(), ("127.0.0.1", 6767))

            data, _ = s.recvfrom(1024)
            ack = json.loads(data.decode())

            if ack.get("type") == "NAK":
                print(f"[DHCP] NAK: {ack.get('reason')}")
                return None

            if ack.get("type") == "ACK" or ack.get("status") == "success":
                print(f"[DHCP] Handshake Complete! My IP: {offered_ip}")
                return offered_ip

        except Exception as e:
            print(f"[DHCP] Handshake failed: {e}")
            return None


# ═══════════════════════════════════════════════════════════
#  שאלת DNS
# ═══════════════════════════════════════════════════════════
def query_dns(domain: str, client_id="Unknown") -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(3)
            query = {"type": "QUERY", "domain": domain, "client_id": client_id}
            s.sendto(json.dumps(query).encode('utf-8'), ("127.0.0.1", 5454))
            print(f"[DNS] Querying '{domain}'...")
            data, _ = s.recvfrom(1024)
            response = json.loads(data.decode('utf-8'))
            if response.get("type") == "RESPONSE":
                ip = response.get("ip")
                print(f"[DNS] Resolved '{domain}' → {ip}")
                return ip
            else:
                print(f"[DNS] Domain '{domain}' not found")
                return None
    except Exception as e:
        print(f"[DNS] Query failed: {e}")
        return None

# ═══════════════════════════════════════════════════════════
#  שחרור IP ל-DHCP
# ═══════════════════════════════════════════════════════════
def release_ip(cid):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(1)
            s.sendto(json.dumps({"type": "RELEASE", "client_id": cid}).encode(),
                     ("127.0.0.1", 6767))
    except:
        pass


def save_downloaded_file(resp: dict, save_dir: str = "."):
    import base64
    if not resp.get("ok"):
        print(f"[DOWNLOAD] Error: {resp.get('error')}")
        return

    filename = resp.get("filename", "downloaded_file")

    # RUDP שולח bytes גולמיים, TCP שולח base64
    if "_raw_bytes" in resp:
        data = resp["_raw_bytes"]
    elif "data" in resp:
        data = base64.b64decode(resp["data"])
    else:
        print("[DOWNLOAD] Error: no file data in response")
        return

    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, filename)
    counter = 1
    while os.path.exists(save_path):
        name, _, ext = filename.rpartition(".")
        save_path = os.path.join(save_dir, f"{name}_{counter}.{ext}")
        counter += 1

    with open(save_path, "wb") as f:
        f.write(data)
    print(f"[DOWNLOAD] ✓ Saved '{save_path}' ({resp.get('size', len(data))} bytes, {resp.get('content_type', '?')})")


def main():
    client_id = f"User-{str(uuid.uuid4())[:8]}"

    my_ip = get_my_ip_from_dhcp(client_id)
    if not my_ip:
        print("DHCP Error: Could not join network.")
        return

    ap = argparse.ArgumentParser(description="Client (calc/gpt over JSON TCP/RUDP)")
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--mode", choices=["gpt", "download"])
    ap.add_argument("--prompt", help="Prompt for mode=gpt")
    ap.add_argument("--url", help="URL for mode=download")
    ap.add_argument("--save-dir", default=".", help="Directory to save downloaded files")
    ap.add_argument("--no-cache", action="store_true")
    ap.add_argument("--use-proxy", action="store_true")
    ap.add_argument("--direct", action="store_true")
    ap.add_argument("--rudp", action="store_true", help="Use RUDP instead of TCP")
    args = ap.parse_args()

    # בחירת proxy/direct
    if args.use_proxy:
        use_proxy = True
    elif args.direct:
        use_proxy = False
    else:
        while True:
            print("\n[CONNECTION MODE]")
            print("1. Direct connection to server")
            print("2. Connect via proxy")
            choice = input("Choose (1/2): ").strip()
            if choice == "2":
                use_proxy = True
                break
            elif choice == "1" or choice == "":
                use_proxy = False
                break
            else:
                print(f"[ERROR] Invalid choice '{choice}'. Please enter 1 or 2.")

    # ═══════════════════════════════════════════════════════
    # [NEW] בחירת פרוטוקול TCP או RUDP
    # ═══════════════════════════════════════════════════════
    if args.rudp:
        use_rudp = True
    else:
        while True:
            print("\n[PROTOCOL]")
            print("1. TCP")
            print("2. RUDP (Reliable UDP)")
            choice = input("Choose (1/2): ").strip()
            if choice == "1" or choice == "":
                use_rudp = False
                break
            elif choice == "2":
                use_rudp = True
                break
            else:
                print(f"[ERROR] Invalid choice '{choice}'. Please enter 1 or 2.")

    print(f"[PROTOCOL] Using {'RUDP' if use_rudp else 'TCP'}")

    # [UNCHANGED] קביעת domain ופורט
    if use_proxy:
        domain = "proxy.local"
        default_port = 5554
        print(f"[MODE] Using PROXY mode")
    else:
        domain = "server.local"
        default_port = 5555
        print(f"[MODE] Using DIRECT mode")

    target_port = args.port if args.port else default_port

    # ═══════════════════════════════════════════════════════
    # [NEW] אם RUDP - הפורט הוא default_port + 1
    # ═══════════════════════════════════════════════════════
    if use_rudp:
        target_port = target_port + RUDP_PORT_OFFSET

    #  שאלת DNS
    print(f"[DNS] Looking up '{domain}'...")
    resolved_ip = query_dns(domain, client_id=client_id)
    if resolved_ip:
        target_host = resolved_ip
        print(f"[DNS] Connecting to {domain} ({resolved_ip}:{target_port})")
    else:
        print(f"[DNS] Failed! Using fallback: 127.0.0.1:{target_port}")
        target_host = "127.0.0.1"
    sock = None
    rudp_sock = None
    if use_rudp:
        rudp_sock = RUDPSocket()
        rudp_sock._sock.bind(("127.0.0.1", 0))

    try:
        if args.mode:
            #  מצב שורת פקודה
            payload = {
                "mode": args.mode,
                "data": ({"prompt": args.prompt} if args.mode == 'gpt'
                         else {"url": args.url}),
                "options": {"cache": not args.no_cache}
            }
            # ═══════════════════════════════════════════════
            # [NEW] ניתוב לפי פרוטוקול
            # ═══════════════════════════════════════════════
            if use_rudp:
                resp = request_rudp(rudp_sock, payload, (target_host, target_port))
            else:
                sock = socket.create_connection((target_host, target_port), timeout=30)
                resp = request(sock, payload)
            if args.mode == "download":
                save_downloaded_file(resp, args.save_dir if args.save_dir else ".")
            else:
                print(json.dumps(resp, ensure_ascii=False, indent=2))
            return

        # מצב אינטראקטיבי
        mode = None
        while True:
            if mode not in ["gpt", "download"]:
                mode = input("Enter gpt / download: ").strip().lower()
                if mode == "quit": break
                continue
            if mode == 'gpt':
                prompt = input("Enter prompt (or quit / change mode): ").strip()
                if prompt.lower() == "quit": break
                if prompt.lower() == "change mode":
                    mode = None
                    continue
                payload = {"mode": "gpt", "data": {"prompt": prompt},
                           "options": {"cache": not args.no_cache}}

            elif mode == "download":
                url = input("Enter URL to download (or quit / change mode): ").strip()
                if url.lower() == "quit": break
                if url.lower() == "change mode":
                    mode = None
                    continue
                payload = {"mode": "download", "data": {"url": url}, "options": {"cache": not args.no_cache}}

            if use_rudp:
                resp = request_rudp(rudp_sock, payload, (target_host, target_port))
            else:
                if sock is None:
                    sock = socket.create_connection((target_host, target_port), timeout=30)
                resp = request(sock, payload)
            if payload.get("mode") == "download":
                save_downloaded_file(resp, args.save_dir if args.save_dir else ".")
            else:
                print(json.dumps(resp, ensure_ascii=False, indent=2))
                
    except Exception as e:
        print(f"Connection error: {e}")
    finally:
        if sock:
            sock.close()
            print("[TCP] Connection closed")
        if rudp_sock:
            rudp_sock.close()
            print("[RUDP] Connection closed")
        if my_ip:
            release_ip(client_id)
            print(f"[DHCP] Released IP address: {my_ip}")
        print(f"[CLIENT] {client_id} is down. Goodbye! 👋")


if __name__ == "__main__":
    main()