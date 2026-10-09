import socket
import struct
import threading
import time
import random

# ═══════════════════════════════════════════════════════════
# קבועים
# ═══════════════════════════════════════════════════════════
MAX_SEGMENT_SIZE = 1400       # MSS - גודל מקסימלי לכל חבילה (bytes)
TIMEOUT = 15.0                 # זמן המתנה ל-ACK לפני retransmit (שניות)
MAX_RETRIES = 10              # מספר מקסימלי של ניסיונות retransmit
WINDOW_SIZE_INIT = 1          # גודל חלון התחלתי (Slow Start)
SSTHRESH_INIT = 16            # סף Slow Start התחלתי
MAX_WINDOW = 32               # גודל חלון מקסימלי
# Flags
FLAG_DATA = 0x01
FLAG_ACK  = 0x02
FLAG_SYN  = 0x04
FLAG_FIN  = 0x08

# Header format: seq(4) + ack(4) + flags(1) + window(2) + length(4) = 15 bytes
HEADER_FORMAT = "!IIBHi"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 15 bytes


# ═══════════════════════════════════════════════════════════
# RUDPPacket - מבנה חבילה
# ═══════════════════════════════════════════════════════════
class RUDPPacket:
    def __init__(self, seq=0, ack=0, flags=0, window=MAX_WINDOW, data=b""):
        self.seq    = seq
        self.ack    = ack
        self.flags  = flags
        self.window = window   # receiver window (flow control)
        self.data   = data

    # ── בדיקות flags ──
    def is_ack(self):  return bool(self.flags & FLAG_ACK)
    def is_syn(self):  return bool(self.flags & FLAG_SYN)
    def is_fin(self):  return bool(self.flags & FLAG_FIN)
    def is_data(self): return bool(self.flags & FLAG_DATA)

    def encode(self) -> bytes:
        """סריאליזציה ל-bytes לשליחה"""
        header = struct.pack(
            HEADER_FORMAT,
            self.seq,
            self.ack,
            self.flags,
            self.window,
            len(self.data)
        )
        return header + self.data

    @staticmethod
    def decode(raw: bytes) -> "RUDPPacket":
        if len(raw) < HEADER_SIZE:
            raise ValueError(f"Packet too short: {len(raw)} bytes")
        seq, ack, flags, window, length = struct.unpack(HEADER_FORMAT, raw[:HEADER_SIZE])
        data = raw[HEADER_SIZE:HEADER_SIZE + length]
        return RUDPPacket(seq=seq, ack=ack, flags=flags, window=window, data=data)

# ═══════════════════════════════════════════════════════════
# RUDPSocket - הלב של המימוש
# ═══════════════════════════════════════════════════════════
class RUDPSocket:
    def __init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.settimeout(TIMEOUT)
        self._lock = threading.Lock()

        # ── Congestion Control ──
        self.cwnd      = WINDOW_SIZE_INIT   # congestion window
        self.ssthresh  = SSTHRESH_INIT      # slow start threshold
        self.dup_acks  = 0                  # מונה duplicate ACKs
        self.last_ack  = -1                 # ACK האחרון שקיבלנו

        # ── Flow Control ──
        self.rwnd = MAX_WINDOW              # receiver window של הצד השני

    def bind(self, addr: tuple):
        """האזנה על כתובת (שרת)"""
        self._sock.bind(addr)
        print(f"[RUDP] Bound to {addr[0]}:{addr[1]}")

    def close(self):
        self._sock.close()

    # ═══════════════════════════════════════════════
    # שליחת הודעה שלמה (מחלקת לחבילות אם צריך)
    # ═══════════════════════════════════════════════
    def send_message(self, data: bytes, addr: tuple) -> bool:
        # פיצול הנתונים ל-segments
        segments = []
        for i in range(0, max(len(data), 1), MAX_SEGMENT_SIZE):
            segments.append(data[i:i + MAX_SEGMENT_SIZE])

        total = len(segments)
        base  = 0       # הסגמנט הראשון שעדיין לא אושר
        next_seq = 0    # הסגמנט הבא לשליחה

        # מילון: seq → (packet, timestamp)
        in_flight = {}

        while base < total:
            # ── חלון אפקטיבי = min(cwnd, rwnd) ──
            effective_window = min(int(self.cwnd), self.rwnd, MAX_WINDOW)

            # שלח חבילות חדשות כל עוד יש מקום בחלון
            while next_seq < total and (next_seq - base) < effective_window:
                is_last = (next_seq == total - 1)
                flags = FLAG_DATA | (FLAG_FIN if is_last else 0)
                pkt = RUDPPacket(
                    seq=next_seq,
                    flags=flags,
                    window=MAX_WINDOW,
                    data=segments[next_seq]
                )
                self._send_packet(pkt, addr)
                in_flight[next_seq] = (pkt, time.time())
                next_seq += 1

            # המתן ל-ACK
            try:
                ack_pkt, _ = self._recv_packet()

                if ack_pkt.is_ack():
                    ack_num = ack_pkt.ack
                    self.rwnd = max(1, ack_pkt.window)  # עדכון flow control

                    if ack_num > self.last_ack:
                        # ── ACK חדש ──
                        self.dup_acks = 0
                        self.last_ack = ack_num

                        # הסרת חבילות שאושרו
                        for s in list(in_flight.keys()):
                            if s < ack_num:
                                del in_flight[s]
                        base = ack_num

                        # ── Congestion Control: עדכון cwnd ──
                        self._on_new_ack()

                    elif ack_num == self.last_ack:
                        # ── Duplicate ACK ──
                        self.dup_acks += 1
                        print(f"[RUDP] Dup ACK #{self.dup_acks} for seg {ack_num}")

                        if self.dup_acks == 3:
                            # ── Fast Retransmit ──
                            print(f"[RUDP] Fast Retransmit! seg {base}")
                            self._on_loss()  # עדכון ssthresh + cwnd
                            if base in in_flight:
                                lost_pkt, _ = in_flight[base]
                                self._send_packet(lost_pkt, addr)
                                in_flight[base] = (lost_pkt, time.time())

            except socket.timeout:
                # ── Timeout: retransmit כל מה שב-flight ──
                print(f"[RUDP] Timeout! Retransmitting from seg {base}")
                self._on_loss()

                retries = 0
                for seq_num in sorted(in_flight.keys()):
                    pkt, _ = in_flight[seq_num]
                    self._send_packet(pkt, addr)
                    in_flight[seq_num] = (pkt, time.time())
                    retries += 1
                    if retries >= MAX_RETRIES:
                        print(f"[RUDP] Max retries reached, giving up")
                        return False

        print(f"[RUDP] All {total} segments acknowledged ✓")
        return True

    # ═══════════════════════════════════════════════
    # קבלת הודעה שלמה (מאחה חבילות)
    # ═══════════════════════════════════════════════
    def recv_message(self, timeout: float = 30.0) -> tuple:
        self._sock.settimeout(timeout)
        segments    = {}
        sender_addr = None
        expected_seq = 0
        fin_seq      = None   # seq של החבילה האחרונה (FIN)

        while True:
            try:
                pkt, addr = self._recv_packet()

                if not pkt.is_data():
                    continue

                if sender_addr is None:
                    sender_addr = addr

                print(f"[RUDP] Received seg {pkt.seq} from {addr}")

                if pkt.seq not in segments:
                    segments[pkt.seq] = pkt.data

                # שמור את ה-seq של ה-FIN
                if pkt.is_fin():
                    fin_seq = pkt.seq

                ack_seq = self._next_expected(segments, expected_seq)
                ack_pkt = RUDPPacket(
                    seq=0,
                    ack=ack_seq,
                    flags=FLAG_ACK,
                    window=max(1, MAX_WINDOW - len(segments))
                )
                self._send_packet(ack_pkt, addr)
                expected_seq = ack_seq

                # סיום רק אם קיבלנו FIN וכל הסגמנטים לפניו
                if fin_seq is not None and all(s in segments for s in range(fin_seq + 1)):
                    break

            except socket.timeout:
                # שבור רק אם קיבלנו הכל (FIN + כל הסגמנטים)
                if fin_seq is not None and all(s in segments for s in range(fin_seq + 1)):
                    break
                # אם עדיין חסרים segments - זרוק timeout
                raise

        data = b"".join(segments[i] for i in sorted(segments.keys()))
        return data, sender_addr

    # ═══════════════════════════════════════════════
    # Congestion Control - עדכון cwnd
    # ═══════════════════════════════════════════════
    def _on_new_ack(self):
        if self.cwnd < self.ssthresh:
            self.cwnd += 1
        else:
            self.cwnd += 1.0 / self.cwnd

    def _on_loss(self):
        self.ssthresh = max(2, int(self.cwnd / 2))
        self.cwnd = WINDOW_SIZE_INIT
        self.dup_acks = 0

    # ═══════════════════════════════════════════════
    # פונקציות עזר פנימיות
    # ═══════════════════════════════════════════════
    #  קבוע לשליטה על אחוז איבוד החבילות המדומה
    SIMULATE_LOSS_RATE = 0.1  # 10% סיכוי לאיבוד - שנה ל-0.0 לכיבוי

    def _send_packet(self, pkt: RUDPPacket, addr: tuple):
        # דימוי איבוד חבילות - מדלג על שליחה בהסתברות SIMULATE_LOSS_RATE
        # ACK לא נאבד - רק DATA, כדי לדמות מציאות אמיתית
        if pkt.is_data() and random.random() < self.SIMULATE_LOSS_RATE:
            print(f"[LOSS] Simulated packet loss! seq={pkt.seq}")
        self._sock.sendto(pkt.encode(), addr)

    def _recv_packet(self) -> tuple:
        raw, addr = self._sock.recvfrom(MAX_SEGMENT_SIZE + HEADER_SIZE + 64)
        pkt = RUDPPacket.decode(raw)
        # דימוי איבוד בצד המקבל - החבילה נראית ב-Wireshark אבל נזרקת
        # ACK לא נאבד - רק DATA, כדי לדמות מציאות אמיתית
        if pkt.is_data() and random.random() < self.SIMULATE_LOSS_RATE:
            raise socket.timeout
        return pkt, addr
    def _next_expected(self, segments: dict, start: int) -> int:
        seq = start
        while seq in segments:
            seq += 1
        return seq


# ═══════════════════════════════════════════════════════════
# פונקציות נוחות לשימוש מהקליינט/שרת
# ═══════════════════════════════════════════════════════════
def rudp_send(data: bytes, addr: tuple) -> bool:
    sock = RUDPSocket()
    try:
        return sock.send_message(data, addr)
    finally:
        sock.close()


def rudp_recv(bind_addr: tuple, timeout: float = 30.0) -> tuple:
    sock = RUDPSocket()
    sock.bind(bind_addr)
    try:
        return sock.recv_message(timeout=timeout)
    finally:
        sock.close()