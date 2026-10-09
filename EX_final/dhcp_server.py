import socket
import json
import time

DHCP_SERVER_IP = "127.0.0.1"
DHCP_SERVER_PORT = 6767
POOL_START, POOL_END = 100, 199
SUBNET_MASK = "255.255.255.0"
GATEWAY = "192.168.1.1"
DNS_SERVER = "192.168.1.2"
LEASE_TIME = 600  # 10 דקות = 600 שניות
OFFER_TIMEOUT = 10  # 10 שניות


class DHCPServer:
    def __init__(self):
        self.available_ips = [f"192.168.1.{i}" for i in range(POOL_START, POOL_END + 1)]
        self.assigned_ips = {}  # כתובות שנעולות סופית (client_id -> (ip, expiry))
        self.pending_offers = {}  # הצעות זמניות (client_id -> (ip, timestamp))

    def cleanup(self):
        now = time.time()

        # ניקוי offers שפגו (לקוח לא השלים REQUEST)
        expired_offers = [cid for cid, (ip, t) in self.pending_offers.items() if now - t > OFFER_TIMEOUT]
        for cid in expired_offers:
            ip, _ = self.pending_offers.pop(cid)
            self.available_ips.insert(0, ip)
            print(f"[CLEANUP] Offer expired: {ip}")

        # ניקוי leases שפגו (עבר LEASE_TIME)
        expired_leases = [cid for cid, (ip, exp) in self.assigned_ips.items() if now > exp]
        for cid in expired_leases:
            ip, _ = self.assigned_ips.pop(cid)
            self.available_ips.insert(0, ip)
            print(f"[CLEANUP] Lease expired: {ip} from {cid}")

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind((DHCP_SERVER_IP, DHCP_SERVER_PORT))
            s.settimeout(1)
            print(f"[DHCP] Server is running. Pool: {len(self.available_ips)} IPs")
            print(f"[DHCP] Lease time: {LEASE_TIME}s, Offer timeout: {OFFER_TIMEOUT}s")

            while True:
                try:
                    data, addr = s.recvfrom(1024)
                    msg = json.loads(data.decode())
                    m_type = msg.get("type")
                    c_id = msg.get("client_id")
                    if not c_id:
                        continue

                    # ═══════════════════════════════════════════════════════════
                    # שלב 1: DISCOVER -> השרת שולח OFFER
                    # ═══════════════════════════════════════════════════════════
                    if m_type == "DISCOVER":
                        # --- טיפול במקרה 1: הלקוח כבר קיבל IP נעול (assigned) ---
                        # אם יש ללקוח הזה כבר כתובת נעולה, נחזיר לו אותה
                        if c_id in self.assigned_ips:
                            ip, _ = self.assigned_ips[c_id]
                            print(f"[DHCP] DISCOVER from {c_id} -> Re-offering assigned IP: {ip}")

                        # --- טיפול במקרה 2: הלקוח שלח DISCOVER פעמיים ברצף ---
                        # אם כבר יש לו offer ממתין, נחזיר לו את אותו IP (לא נוציא IP חדש!)
                        elif c_id in self.pending_offers:
                            ip, _ = self.pending_offers[c_id]
                            print(f"[DHCP] DISCOVER from {c_id} -> Re-offering pending IP: {ip}")

                        # --- מקרה רגיל: לקוח חדש, צריך IP חדש מה-pool ---
                        elif self.available_ips:
                            ip = self.available_ips.pop(0)
                            self.pending_offers[c_id] = (ip, time.time())
                            print(f"[DHCP] DISCOVER from {c_id} -> New offer: {ip}")

                        # --- מקרה קצה: אין IPs זמינים! ---
                        else:
                            resp = {"type": "NAK", "reason": "Pool empty"}
                            s.sendto(json.dumps(resp).encode(), addr)
                            print(f"[NAK] Pool empty for {c_id}")
                            continue

                        # שליחת OFFER ללקוח עם כל הפרטים
                        resp = {
                            "type": "OFFER",
                            "assigned_ip": ip,
                            "subnet": SUBNET_MASK,
                            "gateway": GATEWAY,
                            "dns": DNS_SERVER,
                            "lease_time": LEASE_TIME
                        }
                        s.sendto(json.dumps(resp).encode(), addr)

                    # ═══════════════════════════════════════════════════════════
                    # שלב 2: REQUEST -> הלקוח מאשר, השרת שולח ACK ונועל
                    # ═══════════════════════════════════════════════════════════
                    elif m_type == "REQUEST":
                        requested_ip = msg.get("requested_ip")

                        # --- טיפול במקרה: ה-IP שהתבקש תואם להצעה ממתינה ---
                        if c_id in self.pending_offers and self.pending_offers[c_id][0] == requested_ip:
                            expiry = time.time() + LEASE_TIME
                            self.assigned_ips[c_id] = (requested_ip, expiry)
                            del self.pending_offers[c_id]  # מוחקים מהממתינים, עכשיו נעול סופית

                            resp = {"type": "ACK", "status": "success"}
                            s.sendto(json.dumps(resp).encode(), addr)
                            print(f"[DHCP] ACK: {requested_ip} locked for {c_id}")

                        # --- טיפול במקרה: הלקוח מבקש IP שכבר נעול לו (RENEW) ---
                        elif c_id in self.assigned_ips and self.assigned_ips[c_id][0] == requested_ip:
                            # חידוש ה-lease (הארכת תוקף)
                            expiry = time.time() + LEASE_TIME
                            self.assigned_ips[c_id] = (requested_ip, expiry)

                            resp = {"type": "ACK", "status": "success", "renewed": True}
                            s.sendto(json.dumps(resp).encode(), addr)
                            print(f"[DHCP] RENEW: {requested_ip} lease extended for {c_id}")

                        # --- מקרה קצה: IP לא תואם או לא קיים offer ---
                        else:
                            resp = {"type": "NAK", "reason": "No matching offer or IP already assigned"}
                            s.sendto(json.dumps(resp).encode(), addr)
                            print(f"[NAK] Bad REQUEST from {c_id} for IP {requested_ip}")

                    # ═══════════════════════════════════════════════════════════
                    # שלב 3: RELEASE -> שחרור כתובת חזרה למחסנית
                    # ═══════════════════════════════════════════════════════════
                    elif m_type == "RELEASE":
                        if c_id in self.assigned_ips:
                            ip, _ = self.assigned_ips.pop(c_id)
                            self.available_ips.insert(0, ip)
                            print(f"[DHCP] RELEASE: {ip} returned to pool from {c_id}")

                        else:
                            print(f"[DHCP] RELEASE ignored: {c_id} has no assigned IP")

                except socket.timeout:
                    # כל שנייה עושים cleanup (בודקים תפוגות)
                    self.cleanup()
                except Exception as e:
                    print(f"[ERROR] {e}")


if __name__ == "__main__":
    DHCPServer().start()