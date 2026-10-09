import socket
import json
import time

# נסיון לייבא את dnspython (לפנייה לאינטרנט)
try:
    import dns.resolver

    FORWARDING_ENABLED = True
except ImportError:
    FORWARDING_ENABLED = False
    print("[WARNING] dnspython not installed - forwarding disabled")
    print("          Install with: pip install dnspython")

# ═══════════════════════════════════════════════════════════
# הגדרות TTL
# ═══════════════════════════════════════════════════════════
LOCAL_TTL = 86400  # 24 שעות לדומיינים מקומיים (קבועים)
INTERNET_TTL = 300  # 5 דקות לדומיינים מהאינטרנט (עלולים להשתנות)

# ═══════════════════════════════════════════════════════════
# טבלת DNS - דומיינים פנימיים של הפרויקט
# ═══════════════════════════════════════════════════════════
DNS_TABLE = {
    "server.local": "127.0.0.1",
    "proxy.local": "127.0.0.2",
}

# Cache לדומיינים שנשאלו מהאינטרנט
# פורמט: domain → (ip, expiry_time)
dns_cache = {}


def query_internet(domain):
    """פונה ל-DNS חיצוני (8.8.8.8) כדי לפתור domain"""
    if not FORWARDING_ENABLED:
        return None

    try:
        answers = dns.resolver.resolve(domain, 'A')
        ip = str(answers[0])
        print(f"[INTERNET] {domain} → {ip}")
        return ip
    except:
        return None


def lookup(domain):
    # שלב 1: בדוק בטבלה הסטטית
    if domain in DNS_TABLE:
        return DNS_TABLE[domain], LOCAL_TTL

    # שלב 2: בדוק ב-cache (אם עדיין תקף)
    if domain in dns_cache:
        ip, expiry = dns_cache[domain]
        if time.time() < expiry:
            print(f"[CACHE] {domain} → {ip}")
            return ip, INTERNET_TTL
        else:
            del dns_cache[domain]  # פג תוקף

    # שלב 3: פנה לאינטרנט (רק אם זה לא .local)
    if not domain.endswith(".local"):
        ip = query_internet(domain)
        if ip:
            # שמור ב-cache ל-5 דקות
            dns_cache[domain] = (ip, time.time() + INTERNET_TTL)
            return ip, INTERNET_TTL

    # לא מצאנו
    return None, 0


# ═══════════════════════════════════════════════════════════
# הגדרת שרת UDP
# ═══════════════════════════════════════════════════════════
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(("127.0.0.1", 5454))

print(f"[DNS] Server running on 127.0.0.1:5454")
print(f"[DNS] Static domains: {len(DNS_TABLE)}")
print(f"[DNS] Local TTL: {LOCAL_TTL}s ({LOCAL_TTL // 3600}h)")
print(f"[DNS] Internet TTL: {INTERNET_TTL}s ({INTERNET_TTL // 60}m)")
print(f"[DNS] Forwarding: {'ON' if FORWARDING_ENABLED else 'OFF'}")
print(f"[DNS] Waiting for queries...\n")

# ═══════════════════════════════════════════════════════════
# לולאה ראשית
# ═══════════════════════════════════════════════════════════
try:
    while True:
        # קבלת query
        data, addr = sock.recvfrom(1024)

        try:
            msg = json.loads(data.decode('utf-8'))
        except:
            continue

        domain = msg.get("domain", "").lower().strip()
        if not domain:
            continue

        print(f"[QUERY] {domain}")

        # חיפוש
        ip, ttl = lookup(domain)

        # בניית תשובה
        if ip:
            response = {"type": "RESPONSE", "domain": domain, "ip": ip, "ttl": ttl}
            print(f"[FOUND] {domain} → {ip} (TTL: {ttl}s)\n")
        else:
            response = {"type": "NXDOMAIN", "domain": domain, "reason": "Not found"}
            print(f"[NOT FOUND] {domain}\n")

        # שליחה ללקוח
        sock.sendto(json.dumps(response).encode('utf-8'), addr)

except KeyboardInterrupt:
    print(f"\n[DNS] Shutting down. Cache had {len(dns_cache)} entries.")
finally:
    sock.close()