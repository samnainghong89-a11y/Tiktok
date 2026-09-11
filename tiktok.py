import threading
import re
import time
import os
import sys
import json
import socket
from flask import Flask, jsonify, render_template_string, request
from TikTokLive import TikTokLiveClient
from TikTokLive.client.web.web_settings import WebDefaults
from TikTokLive.events import ConnectEvent, CommentEvent, GiftEvent, DisconnectEvent

# ---------------------------------------------------------
# ป้องกัน UnicodeEncodeError เมื่อรันด้วย PyInstaller (--noconsole) และใช้ Emoji ใน print()
# ---------------------------------------------------------
if getattr(sys, "frozen", False):
    # เมื่อรันเป็น .exe จะเปลี่ยนการ print ไปลงไฟล์ app.log ด้วยรหัส UTF-8
    log_path = os.path.join(os.path.dirname(sys.executable), "app.log")
    log_file = open(log_path, "a", encoding="utf-8", buffering=1)
    sys.stdout = log_file
    sys.stderr = log_file
else:
    if sys.stdout is not None and hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if sys.stderr is not None and hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

app = Flask(__name__)

# Euler Stream API key (ใช้เซ็น signature ตอนเชื่อมต่อ TikTok LIVE เพื่อเพิ่ม rate limit)
# ต้องตั้งผ่าน WebDefaults ก่อนสร้าง client ตัวแรกเสมอ (ตั้งทีเดียวตอนโปรแกรมเริ่ม)
# อ่านจาก environment variable ก่อน (ตั้งค่าใน dashboard ของ cloud) ถ้าไม่ได้ตั้งไว้ค่อย fallback เป็นค่าเดิม
WebDefaults.tiktok_sign_api_key = os.environ.get(
    "EULER_API_KEY",
    "euler_MzllNmEwNzFlNjZhZjMxZjEyZDcxZWZlNTVhOTYzNDM2NzNkZTUwZjYxMjU5YzE1ZDAyYTg5",
)

# ให้ TikTok ส่งข้อมูล "รายชื่อของขวัญ" กลับมาเป็นภาษาไทย (ตามที่ TikTok ตั้งชื่อไว้จริงๆ)
if WebDefaults.web_client_params is None:
    WebDefaults.web_client_params = {}
WebDefaults.web_client_params["app_language"] = "th-TH"

# แคชชื่อของขวัญภาษาไทยจาก TikTok: {gift_id: "ชื่อไทยจาก TikTok"}
gift_name_th_map = {}
gift_name_th_lock = threading.Lock()


def get_local_ip():
    """หา IP วง LAN ของเครื่องนี้ เพื่อให้มือถือ (WiFi วงเดียวกัน) เข้าดูหน้าเว็บได้"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # ไม่ได้ส่งข้อมูลจริง แค่ให้ระบบเลือก network interface ที่จะใช้ออกเน็ต
        # เพื่อจะได้รู้ว่า IP วง LAN ของเครื่องนี้คืออะไร
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


# ตอนรันเป็น .py ปกติ ใช้โฟลเดอร์เดียวกับไฟล์สคริปต์
# ถ้าเอาไปแปลงเป็น .exe ในอนาคต (PyInstaller --onefile) จะใช้โฟลเดอร์ที่ .exe อยู่แทน
if getattr(sys, "frozen", False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "queue_data.json")

queue_list = []
queue_id_counter = 1
queue_lock = threading.Lock()

history_list = []
history_lock = threading.Lock()

current_username = ""
listener_thread = None

SYSTEM_KEYWORDS = [
    "ยินดีต้อนรับสู่", "หลักเกณฑ์สำหรับชุมชน", "ถูกกรองออก",
    "โต้ตอบกับผู้อื่น", "อายุ 18 ปี", "เข้าร่วมแล้ว"
]

# ---------------------------------------------------------
# ระบบเลขรุ่นการเชื่อมต่อ + สถานะ + โหมดเฝ้าเชื่อมต่อตลอด
# ---------------------------------------------------------
# ทุกครั้งที่เริ่มเชื่อมต่อ (ครั้งแรกหรือ auto-retry) เลขรุ่นจะเพิ่มขึ้น
# handler ของ client แต่ละตัวจะเช็คเลขรุ่นของตัวเองก่อนทำงานทุกครั้ง
# ถ้าไม่ตรงกับรุ่นปัจจุบัน (เช่น client เก่าที่ยังไม่ตายสนิทตอนต่อใหม่) จะถูกข้ามไปเลย
# กันปัญหาข้อมูลซ้ำเวลามีการเชื่อมต่อซ้อนกันชั่วคราว
_generation_lock = threading.Lock()
_connection_generation = 0

_status_lock = threading.Lock()
_connection_status = "⚪ ยังไม่ได้เชื่อมต่อ"

_auto_connecting = False  # True = ให้ระบบเฝ้าเชื่อมต่อใหม่ตลอดเวลาเมื่อหลุด จนกว่าจะกดหยุด


def bump_generation():
    global _connection_generation
    with _generation_lock:
        _connection_generation += 1
        return _connection_generation


def get_active_generation():
    with _generation_lock:
        return _connection_generation


def set_status(msg):
    global _connection_status
    with _status_lock:
        _connection_status = msg


def get_status():
    with _status_lock:
        return _connection_status


def save_queue_to_disk():
    """บันทึกคิวปัจจุบันและประวัติลงไฟล์ ให้เปิดโปรแกรมใหม่แล้วข้อมูลไม่หาย"""
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "queue_list": queue_list,
                    "queue_id_counter": queue_id_counter,
                    "history_list": history_list
                },
                f, ensure_ascii=False, indent=2,
            )
    except Exception as e:
        print(f"⚠️ [Save Error]: {e}")


def load_queue_from_disk():
    """โหลดคิวและประวัติที่เคยบันทึกไว้กลับมาตอนเปิดโปรแกรม"""
    global queue_list, queue_id_counter, history_list
    if not os.path.exists(DATA_FILE):
        return
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        with queue_lock:
            queue_list = data.get("queue_list", [])
            queue_id_counter = data.get("queue_id_counter", 1)
        with history_lock:
            history_list = data.get("history_list", [])
        print(f"💾 โหลดคิวเดิมกลับมาแล้ว ({len(queue_list)} รายการ, ประวัติ {len(history_list)} รายการ)")
    except Exception as e:
        print(f"⚠️ [Load Error]: {e}")


def clean_nickname(raw_name):
    """ลบข้อความระบบ/ยศที่ไม่ต้องการออกจากชื่อที่แสดงใน LIVE"""
    if not raw_name:
        return ""

    name = str(raw_name).strip()

    for kw in SYSTEM_KEYWORDS:
        if kw in name:
            return ""

    name = re.sub(r'^[A-Za-z]?\d+\s*', '', name)
    name = re.sub(r'[🧡🦊]\s*Foxy\s*', '', name, flags=re.IGNORECASE)
    return name.strip()


def get_user_key(event):
    """ใช้ unique_id เป็นหลักในการจับคู่คนส่งของขวัญกับคอมเมนต์"""
    user = getattr(event, "user", None)
    if user is None:
        return ""

    unique_id = getattr(user, "unique_id", None)
    if unique_id:
        return str(unique_id).strip().lower()

    nickname = getattr(user, "nickname", None)
    return str(nickname or "").strip().lower()


GIFT_NAME_TH = {
    "rose": "กุหลาบ", "tiktok": "ทิกทอก", "gg": "จีจี",
    "ice cream cone": "ไอศกรีมโคน", "ice cream": "ไอศกรีม",
    "perfume": "น้ำหอม", "finger heart": "หัวใจนิ้ว",
    "sunglasses": "แว่นกันแดด", "little crown": "มงกุฎน้อย",
    "corgi": "คอร์กี้", "doughnut": "โดนัท", "hand heart": "หัวใจมือ",
    "love you": "รักคุณ", "cheer for you": "เชียร์คุณ", "star": "ดาว",
    "diamond": "เพชร", "music note": "โน้ตดนตรี", "thumbs up": "ไลก์",
    "confetti": "คอนเฟตตี", "fire": "ไฟ", "heart": "หัวใจ",
    "galaxy": "กาแล็กซี", "universe": "จักรวาล",
    "lightning bolt": "สายฟ้า", "adore you": "ชื่นชมคุณ",
    "friendship necklace": "สร้อยมิตรภาพ", "team bracelet": "สร้อยข้อมือทีม",
    "rosa": "กุหลาบ", "money gun": "ปืนแบงก์", "swan": "หงส์",
    "coral": "ปะการัง", "leon the kitten": "ลูกแมวลีออน",
}


def translate_gift_name(gift_id, gift_name):
    """
    เอาชื่อไทยจริงของของขวัญที่ดึงมาจาก TikTok (ตาม gift_id) มาก่อนเป็นอันดับแรก
    ถ้าไม่เจอ (เช่นยังไม่เคยโหลดรายการของขวัญ) ค่อย fallback ไปใช้ตารางแปลสำรอง
    """
    name = str(gift_name).strip()

    with gift_name_th_lock:
        th_name = gift_name_th_map.get(gift_id)
    if th_name:
        return f"{th_name} ({name})"

    th_fallback = GIFT_NAME_TH.get(name.lower())
    return f"{th_fallback} ({name})" if th_fallback else name


def add_gift_to_queue(user_key, nickname, gift_name, count):
    """เพิ่มของขวัญเข้า queue และรวมของขวัญใหม่ของคนเดิม"""
    global queue_id_counter

    with queue_lock:
        found = False
        for item in queue_list:
            if item["user_key"] == user_key and item["type"] == "gift":
                item["gifts"].append({
                    "name": gift_name,
                    "count": count
                })
                found = True
                break

        if not found:
            queue_list.append({
                "id": queue_id_counter,
                "type": "gift",
                "user_key": user_key,
                "nickname": nickname,
                "gifts": [{
                    "name": gift_name,
                    "count": count
                }],
                "message": None,
                "waiting": True,
                "sequence": queue_id_counter
            })
            queue_id_counter += 1

    save_queue_to_disk()


def add_comment(user_key, nickname, comment):
    """รับข้อความจากคนที่มีของขวัญรออยู่เท่านั้น (คอมเมนต์เฉยๆ ที่ไม่มีของขวัญจะไม่ถูกเก็บเข้าคิว)"""
    with queue_lock:
        for item in queue_list:
            if item["user_key"] == user_key and item["message"] is None:
                item["message"] = comment
                item["waiting"] = False
                break
        else:
            return  # ไม่มีของขวัญรออยู่ -> ไม่ต้องเก็บคอมเมนต์นี้

    save_queue_to_disk()


def run_tiktok_listener(username, gen):
    """ฟังก์ชันรันตัวดักจับ TikTok ตาม Username ที่ระบุ (ผูกกับเลขรุ่น gen)"""
    clean_uname = username.lstrip("@").strip()
    if not clean_uname:
        return

    def is_current_gen():
        return gen == get_active_generation()

    client = TikTokLiveClient(unique_id=clean_uname)

    @client.on(ConnectEvent)
    async def on_connect(event: ConnectEvent):
        if not is_current_gen():
            return
        set_status(f"🟢 เชื่อมต่อกับ @{clean_uname} สำเร็จ")
        print(f"🎉 [TikTok] เชื่อมต่อกับไลฟ์ @{clean_uname} สำเร็จแล้ว!")

        # ดึงรายชื่อของขวัญจริงจาก TikTok (เป็นภาษาไทย เพราะตั้ง app_language=th-TH ไว้) มาแคชไว้ใช้แปลชื่อ
        try:
            gift_info = client.gift_info or {}
            gifts = gift_info.get("gifts", [])
            with gift_name_th_lock:
                for g in gifts:
                    gid = g.get("id") or g.get("gift_id")
                    name_th = g.get("name")
                    if gid is not None and name_th:
                        gift_name_th_map[gid] = str(name_th).strip()
            print(f"🎁 โหลดรายชื่อของขวัญจาก TikTok แล้ว ({len(gifts)} รายการ)")
        except Exception as e:
            print(f"⚠️ [Gift Info Error]: {e}")

    @client.on(GiftEvent)
    async def on_gift(event: GiftEvent):
        if not is_current_gen():
            return
        try:
            # ของขวัญแบบ combo (เช่น Rose) ยิง event หลายรอบระหว่างกดค้าง
            # นับเฉพาะรอบสุดท้ายที่ combo จบแล้ว (repeat_end) กันนับซ้ำ
            if event.gift.streakable and not event.repeat_end:
                return

            nickname = clean_nickname(event.user.nickname)
            user_key = get_user_key(event)
            gift_id = getattr(event.gift, "id", None) or getattr(event.gift, "gift_id", None)
            gift_name = translate_gift_name(gift_id, str(event.gift.name))
            count = getattr(event, "repeat_count", 1)

            if not nickname or not user_key or not gift_name:
                return

            add_gift_to_queue(user_key, nickname, gift_name, count)
            print(f"🎁 [GIFT]: {nickname} -> {gift_name} x{count}")
        except Exception as e:
            print(f"⚠️ [GIFT Error]: {e}")

    @client.on(CommentEvent)
    async def on_comment(event: CommentEvent):
        if not is_current_gen():
            return
        try:
            nickname = clean_nickname(event.user.nickname)
            user_key = get_user_key(event)
            comment = str(event.comment).strip()

            if not nickname or not user_key or not comment:
                return

            add_comment(user_key, nickname, comment)
            print(f"💬 [CHAT]: {nickname} -> {comment}")
        except Exception as e:
            print(f"⚠️ [CHAT Error]: {e}")

    @client.on(DisconnectEvent)
    async def on_disconnect(event: DisconnectEvent):
        if not is_current_gen():
            return
        set_status("🔴 การเชื่อมต่อหลุด")
        print("❌ [TikTok] การเชื่อมต่อหลุด")

    try:
        client.run(fetch_gift_info=True)
    except Exception as e:
        if is_current_gen():
            set_status(f"🔴 เชื่อมต่อไม่สำเร็จ ({e})")
        print(f"💥 [TikTok Error]: {e}")

    # ถึงตรงนี้แปลว่า client หยุดทำงานแล้ว (ไม่ว่าจะ error หรือไลฟ์จบ/หลุดเฉยๆ)
    if is_current_gen():
        set_status("🔴 การเชื่อมต่อหยุดทำงาน")

    # โหมดเฝ้าเชื่อมต่อตลอด: ถ้ายังเปิดอยู่ และไม่มีใครกดเชื่อมต่อใหม่/หยุดระหว่างนี้
    # ให้รอ 5 วิ แล้วลองใหม่ให้เองไปเรื่อยๆ จนกว่าจะกด "หยุดเชื่อมต่อ"
    if _auto_connecting and is_current_gen():
        time.sleep(5)
        if _auto_connecting and is_current_gen():
            set_status("🔄 กำลังลองเชื่อมต่อใหม่...")
            start_new_attempt(username)


def start_new_attempt(username):
    global listener_thread
    gen = bump_generation()
    listener_thread = threading.Thread(
        target=run_tiktok_listener,
        args=(username, gen),
        daemon=True,
        name="TikTokListener"
    )
    listener_thread.start()


# ==========================================
# 🌐 หน้าเว็บเพิ่มช่องกรอก Username
# ==========================================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="th">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>คิวรอตอบ TikTok LIVE</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Prompt:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: 'Prompt', sans-serif;
            background: radial-gradient(circle at top, #1a1a2e 0%, #0d0d16 60%);
            color: #f1f1f1;
            padding: 20px;
            margin: 0;
            min-height: 100vh;
        }
        h2 {
            margin: 0 0 16px;
            font-weight: 600;
            font-size: 1.5em;
            display: flex;
            align-items: center;
            gap: 8px;
            background: linear-gradient(90deg, #00e676, #29b6f6);
            -webkit-background-clip: text;
            background-clip: text;
            color: transparent;
        }
        .config-panel {
            background: rgba(30, 30, 45, 0.85);
            backdrop-filter: blur(6px);
            padding: 16px;
            border-radius: 14px;
            margin-bottom: 16px;
            display: flex;
            gap: 10px;
            align-items: center;
            flex-wrap: wrap;
            border: 1px solid rgba(255,255,255,0.06);
        }
        .config-panel input {
            padding: 10px 14px;
            border-radius: 8px;
            border: 1px solid #3a3a4d;
            background: #1b1b28;
            color: #fff;
            font-family: 'Prompt', sans-serif;
            font-size: 0.95em;
            flex: 1;
            min-width: 200px;
            outline: none;
            transition: border-color 0.2s;
        }
        .config-panel input:focus { border-color: #29b6f6; }
        button {
            font-family: 'Prompt', sans-serif;
            color: white;
            border: none;
            padding: 10px 18px;
            border-radius: 8px;
            cursor: pointer;
            font-weight: 600;
            font-size: 0.9em;
            transition: transform 0.12s ease, filter 0.12s ease;
        }
        button:hover { filter: brightness(1.12); transform: translateY(-1px); }
        button:active { transform: translateY(0); }
        .btn-connect { background: linear-gradient(135deg, #29b6f6, #1e88e5); }
        .btn-stop { background: linear-gradient(135deg, #ff5252, #c62828); }
        #status-text {
            font-size: 0.85em;
            font-weight: 500;
            color: #00e676;
            width: 100%;
            margin-top: 2px;
            padding: 6px 12px;
            background: rgba(0, 230, 118, 0.08);
            border-radius: 20px;
            display: inline-block;
        }
        .btn-group { margin-bottom: 18px; display: flex; gap: 10px; }
        .btn-clear-all { background: linear-gradient(135deg, #ff5252, #b71c1c); }

        #queue-container {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 14px;
        }
        .empty-msg { color: #777; grid-column: 1 / -1; text-align: center; padding: 30px 0; }

        .card {
            background: #1a1a26;
            border-radius: 14px;
            padding: 16px;
            border: 1px solid rgba(255,255,255,0.05);
            border-left: 5px solid #29b6f6;
            box-shadow: 0 4px 14px rgba(0,0,0,0.25);
            transition: transform 0.15s ease, box-shadow 0.15s ease;
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        .card:hover { transform: translateY(-2px); box-shadow: 0 8px 20px rgba(0,0,0,0.35); }
        .card.ready { border-left-color: #00e676; }
        .card.pending { border-left-color: #ffb74d; }

        .nickname { font-weight: 600; font-size: 1.05em; color: #ffeb3b; display: flex; align-items: center; gap: 6px; }
        .gift-line { font-size: 0.95em; color: #ff8fb1; background: rgba(233,30,99,0.12); padding: 4px 10px; border-radius: 8px; display: inline-block; width: fit-content; }
        .message { font-size: 1.1em; color: #fff; white-space: pre-wrap; overflow-wrap: anywhere; line-height: 1.4; }
        .waiting-badge { color: #ffb74d; font-weight: 600; font-size: 0.9em; background: rgba(255,183,77,0.12); padding: 4px 10px; border-radius: 8px; display: inline-block; width: fit-content; }

        .card-actions { margin-top: auto; display: flex; justify-content: flex-end; gap: 8px; padding-top: 6px; }
        .done-btn { background: linear-gradient(135deg, #00e676, #00b859); padding: 8px 14px; font-size: 0.85em; }
        .delete-btn { background: linear-gradient(135deg, #757575, #4a4a4a); padding: 8px 14px; font-size: 0.85em; }
        .btn-history { background: linear-gradient(135deg, #ab47bc, #7b1fa2); }

        .modal-overlay {
            display: none;
            position: fixed;
            inset: 0;
            background: rgba(0,0,0,0.65);
            z-index: 100;
            padding: 20px;
            overflow-y: auto;
        }
        .modal-overlay.open { display: block; }
        .modal-box {
            max-width: 700px;
            margin: 0 auto;
            background: #16161f;
            border-radius: 14px;
            padding: 20px;
            border: 1px solid rgba(255,255,255,0.08);
        }
        .modal-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; }
        .modal-close { background: linear-gradient(135deg, #757575, #4a4a4a); padding: 6px 14px; font-size: 0.85em; }
        .history-item {
            background: #1a1a26;
            border-radius: 10px;
            padding: 12px 14px;
            margin-bottom: 10px;
            border-left: 4px solid #00e676;
        }

        @media (max-width: 480px) {
            #queue-container { grid-template-columns: 1fr; }
            h2 { font-size: 1.25em; }
        }
    </style>
</head>
<body>
    <h2>📋 คิวรอตอบ (TikTok LIVE)</h2>

    <div class="config-panel">
        <input type="text" id="username-input" placeholder="ใส่ Username ช่องไลฟ์ (เช่น username)" value="">
        <button class="btn-connect" onclick="updateUsername()">🔌 เชื่อมต่อ</button>
        <button class="btn-stop" onclick="stopConnection()">⏹️ หยุดเชื่อมต่อ</button>
        <div id="status-text">สถานะ: ยังไม่ได้เชื่อมต่อช่องใดๆ</div>
    </div>

    <div class="btn-group">
        <button class="btn-clear-all" onclick="clearAllQueue()">🗑️ ลบทั้งหมด (ทุกรายการ)</button>
        <button class="btn-history" onclick="openHistory()">📜 ดูประวัติที่ตอบแล้ว</button>
    </div>

    <div id="queue-container">กรุณากรอก Username ด้านบนเพื่อเริ่มดักจับแชท...</div>

    <div class="modal-overlay" id="history-modal">
        <div class="modal-box">
            <div class="modal-header">
                <h2 style="margin:0;">📜 ประวัติที่ตอบแล้ว</h2>
                <button class="modal-close" onclick="closeHistory()">✖ ปิด</button>
            </div>
            <div id="history-container">กำลังโหลด...</div>
        </div>
    </div>

    <script>
        function escapeHtml(value) {
            const div = document.createElement('div');
            div.textContent = String(value ?? '');
            return div.innerHTML;
        }

        async function updateUsername() {
            const username = document.getElementById('username-input').value.trim();
            if (!username) {
                alert('กรุณากรอก Username ก่อนครับ');
                return;
            }

            try {
                const res = await fetch('/api/set_username', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ username: username })
                });
                await res.json();
            } catch (err) {
                console.error(err);
            }
        }

        async function stopConnection() {
            try {
                await fetch('/api/stop_connection', { method: 'POST' });
            } catch (err) {
                console.error(err);
            }
        }

        async function fetchQueue() {
            try {
                const res = await fetch('/api/get_queue', { cache: 'no-store' });
                if (!res.ok) return;
                const result = await res.json();
                const container = document.getElementById('queue-container');
                const statusEl = document.getElementById('status-text');

                if (result.connection_status) {
                    statusEl.textContent = `สถานะ: ${result.connection_status}`;
                }

                if (!Array.isArray(result.data) || result.data.length === 0) {
                    container.innerHTML = '<p class="empty-msg">ไม่มีคิวรอตอบในขณะนี้...</p>';
                    return;
                }

                container.innerHTML = result.data.map(item => {
                    const isPending = !item.message;
                    const gifts = item.gifts.map(g => `🎁 ${escapeHtml(g.name)} x${escapeHtml(g.count)}`).join('<br>');
                    const messageHtml = item.message
                        ? `<div class="message">💬 ${escapeHtml(item.message)}</div>`
                        : `<div class="waiting-badge">⏳ รอข้อความจากผู้ส่ง</div>`;

                    // รายการที่ยังไม่มีคำถามมา (isPending) ให้เป็นปุ่ม "ลบ" เพราะยังไม่มีอะไรให้ "ตอบ"
                    // รายการที่มีคำถามพร้อมตอบแล้ว ให้เป็นปุ่ม "ตอบแล้ว"
                    const actionBtn = isPending
                        ? `<button class="delete-btn" onclick="removeQueue(${Number(item.id)})">🗑️ ลบ</button>`
                        : `<button class="done-btn" onclick="removeQueue(${Number(item.id)})">✅ ตอบแล้ว</button>`;

                    return `
                        <div class="card ${isPending ? 'pending' : 'ready'}">
                            <div class="nickname">👤 ${escapeHtml(item.nickname)}</div>
                            <div class="gift-line">${gifts}</div>
                            ${messageHtml}
                            <div class="card-actions">${actionBtn}</div>
                        </div>
                    `;
                }).join('');
            } catch (err) {
                console.error(err);
            }
        }

        async function removeQueue(id) {
            await fetch('/api/remove_queue', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ id: id })
            });
            fetchQueue();
        }

        async function clearAllQueue() {
            await fetch('/api/clear_queue', { method: 'POST' });
            fetchQueue();
        }

        async function openHistory() {
            document.getElementById('history-modal').classList.add('open');
            const container = document.getElementById('history-container');
            container.innerHTML = 'กำลังโหลด...';
            try {
                const res = await fetch('/api/get_history', { cache: 'no-store' });
                const result = await res.json();
                const items = Array.isArray(result.data) ? result.data : [];

                if (items.length === 0) {
                    container.innerHTML = '<p class="empty-msg">ยังไม่มีประวัติที่ตอบแล้ว</p>';
                    return;
                }

                container.innerHTML = items.map(item => {
                    const gifts = item.gifts.map(g => `🎁 ${escapeHtml(g.name)} x${escapeHtml(g.count)}`).join('<br>');
                    return `
                        <div class="history-item">
                            <div class="nickname">👤 ${escapeHtml(item.nickname)}</div>
                            <div class="gift-line">${gifts}</div>
                            <div class="message">💬 ${escapeHtml(item.message)}</div>
                        </div>
                    `;
                }).join('');
            } catch (err) {
                console.error(err);
                container.innerHTML = '<p class="empty-msg">โหลดประวัติไม่สำเร็จ</p>';
            }
        }

        function closeHistory() {
            document.getElementById('history-modal').classList.remove('open');
        }

        setInterval(fetchQueue, 1500);
        fetchQueue();
    </script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/set_username", methods=["POST"])
def set_username():
    global current_username, _auto_connecting
    data = request.get_json(silent=True) or {}
    username = data.get("username", "").strip()

    if not username:
        return jsonify({"status": "error", "message": "กรุณากรอก Username"}), 400

    current_username = username
    _auto_connecting = True  # เปิดโหมดเฝ้าเชื่อมต่อตลอด (หลุดแล้วลองใหม่เองทุก 5 วิ)
    set_status("🔄 กำลังเชื่อมต่อ...")
    start_new_attempt(username)

    return jsonify({"status": "success", "username": username})


@app.route("/api/stop_connection", methods=["POST"])
def stop_connection():
    global _auto_connecting
    _auto_connecting = False
    bump_generation()  # ทิ้ง client เดิม (ถ้ายังไม่ตายสนิท) ไม่ให้ส่งข้อมูลเข้าคิวอีก
    set_status("⏹️ หยุดการเชื่อมต่อแล้ว")
    return jsonify({"status": "stopped"})


@app.route("/api/get_queue", methods=["GET"])
def get_queue():
    with queue_lock:
        def sort_key(item):
            priority = 0 if item["message"] else 1  # พร้อมตอบ (มีข้อความ) ขึ้นก่อน รอข้อความอยู่ท้าย
            return (priority, item["sequence"])

        data = sorted([dict(item) for item in queue_list], key=sort_key)

    return jsonify({"status": "success", "data": data, "connection_status": get_status()})


@app.route("/api/remove_queue", methods=["POST"])
def remove_queue():
    global queue_list
    data = request.get_json(silent=True) or {}
    item_id = data.get("id")

    try:
        item_id = int(item_id)
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "id ไม่ถูกต้อง"}), 400

    with queue_lock:
        target = None
        remaining = []
        for item in queue_list:
            if item["id"] == item_id:
                target = item
            else:
                remaining.append(item)
        queue_list = remaining

    # ถ้ารายการนี้มีข้อความ (ตอบแล้ว) ให้เก็บเข้าประวัติแทนการลบทิ้ง
    if target and target.get("message"):
        with history_lock:
            history_list.insert(0, target)  # รายการล่าสุดอยู่บนสุด

    save_queue_to_disk()
    return jsonify({"status": "success"})


@app.route("/api/get_history", methods=["GET"])
def get_history():
    with history_lock:
        data = list(history_list)
    return jsonify({"status": "success", "data": data})


@app.route("/api/clear_queue", methods=["POST"])
def clear_queue():
    global queue_list
    with queue_lock:
        queue_list = []
    save_queue_to_disk()
    return jsonify({"status": "cleared"})


# โหลดคิวเก่ากลับมาทันทีที่ไฟล์นี้ถูก import (ไม่ใช่แค่ตอนรันตรงๆ)
# เพื่อให้ทำงานถูกต้องทั้งตอนรัน `python tiktok.py` และตอนรันผ่าน WSGI server บน cloud
load_queue_from_disk()

if __name__ == "__main__":
    # PORT: cloud ส่วนใหญ่ (Render, Railway ฯลฯ) จะกำหนด port ผ่าน environment variable นี้ให้เอง
    # ถ้ารันในเครื่องตัวเอง (ไม่มีค่านี้) จะ fallback ไปที่ 5000 ตามเดิม
    port = int(os.environ.get("PORT", 5000))
    lan_ip = get_local_ip()

    print("==========================================")
    print("🚀 ระบบจัดการคิว TikTok LIVE พร้อมทำงาน")
    print("")
    print("📱 เปิดดูจากมือถือ/เบราว์เซอร์ (ในเครื่องเดียวกันหรือ LAN เดียวกัน):")
    print(f"   http://{lan_ip}:{port}")
    print("==========================================")

    # รันเป็นเว็บเซิร์ฟเวอร์ตรงๆ (ไม่มีหน้าต่างโปรแกรมแยกอีกต่อไป เพราะ cloud ไม่มีจอ)
    # เข้าใช้งานผ่านเบราว์เซอร์แทน webview
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)