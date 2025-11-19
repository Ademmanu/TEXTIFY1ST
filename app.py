import os
import time
import json
import threading
import traceback
import logging
from datetime import datetime, timedelta
from typing import List

import psycopg2
import psycopg2.extras
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify
import requests

# ========== CONFIG ==========
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
OWNER_USERNAME = os.environ.get("OWNER_USERNAME", "justmemmy")
MAX_ALLOWED_USERS = int(os.environ.get("MAX_ALLOWED_USERS", "50"))
MAX_QUEUE_PER_USER = int(os.environ.get("MAX_QUEUE_PER_USER", "50"))
MAINTENANCE_START_HOUR_WAT = 3
MAINTENANCE_END_HOUR_WAT = 4
REQUESTS_TIMEOUT = float(os.environ.get("REQUESTS_TIMEOUT", "10"))

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None

# ========== POSTGRES DB HELPERS ==========
_db_lock = threading.Lock()
def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.DictCursor)

def db_execute(query, params=(), fetch=False):
    with _db_lock, get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
            conn.commit()

def init_db():
    queries = [
        """CREATE TABLE IF NOT EXISTS allowed_users (
               user_id BIGINT PRIMARY KEY,
               username TEXT,
               added_at TIMESTAMPTZ,
               is_admin INTEGER DEFAULT 0
           )""",
        """CREATE TABLE IF NOT EXISTS tasks (
               id SERIAL PRIMARY KEY,
               user_id BIGINT,
               username TEXT,
               text TEXT,
               words_json TEXT,
               total_words INTEGER,
               status TEXT,
               created_at TIMESTAMPTZ,
               started_at TIMESTAMPTZ,
               finished_at TIMESTAMPTZ
           )""",
        """CREATE TABLE IF NOT EXISTS split_logs (
               id SERIAL PRIMARY KEY,
               user_id BIGINT,
               username TEXT,
               words INTEGER,
               created_at TIMESTAMPTZ
           )""",
        """CREATE TABLE IF NOT EXISTS sent_messages (
               id SERIAL PRIMARY KEY,
               chat_id BIGINT,
               message_id INTEGER,
               sent_at TIMESTAMPTZ,
               deleted INTEGER DEFAULT 0
           )""",
    ]
    with get_conn() as conn:
        with conn.cursor() as cur:
            for q in queries:
                cur.execute(q)
        conn.commit()

# ========== UTILITY & INITIALIZE ==========

def get_now_iso():
    return datetime.utcnow().isoformat()

def split_text_into_words(text: str) -> List[str]:
    return [w for w in text.strip().split() if w]

def compute_interval(total_words: int) -> float:
    if total_words <= 150:
        return 0.25
    elif total_words <= 300:
        return 0.35
    elif total_words <= 1000:
        return 0.45
    else:
        return 0.55

def is_maintenance_now() -> bool:
    utc_now = datetime.utcnow()
    wat_now = utc_now + timedelta(hours=1)
    h = wat_now.hour
    if MAINTENANCE_START_HOUR_WAT < MAINTENANCE_END_HOUR_WAT:
        return MAINTENANCE_START_HOUR_WAT <= h < MAINTENANCE_END_HOUR_WAT
    return h >= MAINTENANCE_START_HOUR_WAT or h < MAINTENANCE_END_HOUR_WAT

# ========== TELEGRAM HELPERS ==========

def tg_call(method: str, payload: dict):
    if not TELEGRAM_API:
        logger.error("tg_call attempted but TELEGRAM_API not configured")
        return None
    url = f"{TELEGRAM_API}/{method}"
    try:
        resp = requests.post(url, json=payload, timeout=REQUESTS_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            logger.error("Telegram API error for %s: %s", method, data)
        return data
    except Exception:
        logger.exception(f"tg_call failed [{method}]")
        return None

def send_message(chat_id: int, text: str, parse_mode: str = "Markdown"):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    data = tg_call("sendMessage", payload)
    if data and data.get("result"):
        mid = data["result"].get("message_id")
        if mid:
            record_sent_message(chat_id, mid)
        return data["result"]
    return None

def delete_message(chat_id: int, message_id: int):
    payload = {"chat_id": chat_id, "message_id": message_id}
    data = tg_call("deleteMessage", payload)
    if data and data.get("ok"):
        mark_message_deleted(chat_id, message_id)
    return data

def set_webhook():
    if not TELEGRAM_API or not WEBHOOK_URL:
        logger.warning("Cannot set webhook: TELEGRAM_TOKEN or WEBHOOK_URL not configured")
        return None
    try:
        resp = requests.post(f"{TELEGRAM_API}/setWebhook", json={"url": WEBHOOK_URL}, timeout=REQUESTS_TIMEOUT)
        resp.raise_for_status()
        logger.info("Webhook set response: %s", resp.text)
        return resp.json()
    except Exception:
        logger.exception("Failed to set webhook")
        return None

# ========== INITIALIZE DB & ALLOWED USERS ==========

init_db()
try:
    res = db_execute("SELECT user_id FROM allowed_users WHERE user_id = %s", (OWNER_ID,), fetch=True)
    if not res:
        db_execute(
            "INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (%s, %s, %s, %s)",
            (OWNER_ID, OWNER_USERNAME, get_now_iso(), 1)
        )
except Exception:
    logger.exception("Error ensuring owner in allowed_users")

# ========== PER-USER LOCKS ==========

user_locks = {}
user_locks_lock = threading.Lock()
def get_user_lock(user_id):
    with user_locks_lock:
        if user_id not in user_locks:
            user_locks[user_id] = threading.Lock()
        return user_locks[user_id]

# ========== TASK/STATS QUERIES ==========

def enqueue_task(user_id: int, username: str, text: str) -> dict:
    words = split_text_into_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    q = db_execute(
        "SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status IN ('queued','running','paused')",
        (user_id,), fetch=True
    )
    pending = q[0][0] if q else 0
    if pending >= MAX_QUEUE_PER_USER:
        return {"ok": False, "reason": "queue_full", "queue_size": pending}
    db_execute(
        "INSERT INTO tasks (user_id, username, text, words_json, total_words, status, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (user_id, username, text, json.dumps(words), total, "queued", get_now_iso()),
    )
    return {"ok": True, "total_words": total, "queue_size": pending + 1}

def get_next_task_for_user(user_id: int):
    rows = db_execute(
        "SELECT id, words_json, total_words, text FROM tasks WHERE user_id = %s AND status = 'queued' ORDER BY id ASC LIMIT 1",
        (user_id,), fetch=True
    )
    if not rows:
        return None
    r = rows[0]
    return {"id": r['id'], "words": json.loads(r['words_json']), "total_words": r['total_words'], "text": r['text']}

def set_task_status(task_id: int, status: str):
    now = get_now_iso()
    if status == "running":
        db_execute("UPDATE tasks SET status = %s, started_at = %s WHERE id = %s", (status, now, task_id))
    elif status in ("done", "cancelled"):
        db_execute("UPDATE tasks SET status = %s, finished_at = %s WHERE id = %s", (status, now, task_id))
    else:
        db_execute("UPDATE tasks SET status = %s WHERE id = %s", (status, task_id))

def mark_task_paused(task_id: int): set_task_status(task_id, "paused")
def mark_task_resumed(task_id: int): set_task_status(task_id, "running")
def mark_task_done(task_id: int): set_task_status(task_id, "done")

def cancel_active_task_for_user(user_id: int):
    rows = db_execute("SELECT id FROM tasks WHERE user_id = %s AND status IN ('queued','running','paused')", (user_id,), fetch=True)
    count = 0
    for r in rows:
        db_execute("UPDATE tasks SET status = %s WHERE id = %s", ("cancelled", r["id"]))
        count += 1
    return count

def record_split_log(user_id: int, username: str, words: int):
    db_execute("INSERT INTO split_logs (user_id, username, words, created_at) VALUES (%s, %s, %s, %s)",
               (user_id, username, words, get_now_iso()))

def record_sent_message(chat_id: int, message_id: int):
    db_execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (%s, %s, %s, 0)",
               (chat_id, message_id, get_now_iso()))

def mark_message_deleted(chat_id: int, message_id: int):
    db_execute("UPDATE sent_messages SET deleted = 1 WHERE chat_id = %s AND message_id = %s", (chat_id, message_id))

def get_messages_older_than(days=1):
    cutoff = datetime.utcnow() - timedelta(days=days)
    rows = db_execute("SELECT chat_id, message_id, sent_at FROM sent_messages WHERE deleted = 0", fetch=True)
    res = []
    for r in rows:
        try:
            sent_at = r["sent_at"]
            if isinstance(sent_at, str):
                sent_at = datetime.fromisoformat(sent_at)
        except Exception:
            continue
        if sent_at < cutoff:
            res.append({"chat_id": r["chat_id"], "message_id": r["message_id"]})
    return res

def is_allowed(user_id: int) -> bool:
    user = db_execute("SELECT 1 FROM allowed_users WHERE user_id = %s", (user_id,), fetch=True)
    return bool(user)

def is_admin(user_id: int) -> bool:
    rows = db_execute("SELECT is_admin FROM allowed_users WHERE user_id = %s", (user_id,), fetch=True)
    return bool(rows and rows[0]['is_admin'])

# ========== CORE PRECISION SPLITTER ==========

_worker_stop = threading.Event()
def process_user_queue(user_id: int, chat_id: int, username: str):
    lock = get_user_lock(user_id)
    if not lock.acquire(blocking=False):
        return
    try:
        while True:
            task = get_next_task_for_user(user_id)
            if not task:
                break
            task_id, words, total = task["id"], task["words"], task["total_words"]
            set_task_status(task_id, "running")
            qcount = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status = 'queued'", (user_id,), fetch=True)[0][0]
            interval = compute_interval(total)
            est_seconds = interval * total
            est_str = str(timedelta(seconds=int(est_seconds)))
            send_message(chat_id, f"🚀 Task Starting! (Waiting: {qcount})\nWords: {total}. Time per word: **{interval}s**\n⏳ Estimated total time: **{est_str}**")
            i = 0
            next_send_time = time.monotonic() + 0.1
            while i < total:
                if i % 5 == 0 or i == 0:
                    status_row = db_execute("SELECT status FROM tasks WHERE id = %s", (task_id,), fetch=True)
                    if not status_row:
                        break
                    status = status_row[0]['status']
                if status == "paused":
                    send_message(chat_id, "⏸️ **Task Paused!** Waiting for /resume to continue.")
                    while True:
                        time.sleep(1)
                        status_row = db_execute("SELECT status FROM tasks WHERE id = %s", (task_id,), fetch=True)
                        if not status_row:
                            break
                        status = status_row[0]['status']
                        if status == "running":
                            send_message(chat_id, "▶️ **Welcome Back!** Resuming word transmission now.")
                            break
                        if status == "cancelled":
                            break
                    if status == "cancelled":
                        break
                if status == "cancelled":
                    break
                now = time.monotonic()
                wait = next_send_time - now
                if wait > 0:
                    time.sleep(wait)
                send_message(chat_id, words[i])
                i += 1
                next_send_time += interval
            final_status_row = db_execute("SELECT status FROM tasks WHERE id = %s", (task_id,), fetch=True)
            final_status = final_status_row[0]['status'] if final_status_row else "done"
            if final_status != "cancelled":
                mark_task_done(task_id)
                record_split_log(user_id, username, total)
                send_message(chat_id, "✅ **All Done!** Split finished successfully.")
            else:
                send_message(chat_id, "🛑 **Active Task Stopped!** The current word transmission has been terminated.")
            qcount_after = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status = 'queued'", (user_id,), fetch=True)[0][0]
            if qcount_after > 0:
                send_message(chat_id, "⏩ **Next Task Starting Soon!** Your next queued task is ready for processing.")
    finally:
        lock.release()

def global_worker_loop():
    while not _worker_stop.is_set():
        try:
            # Use DISTINCT ON for Postgres correctness!
            rows = db_execute("""
                SELECT DISTINCT ON (user_id) user_id, username
                FROM tasks
                WHERE status = 'queued'
                ORDER BY user_id, created_at ASC
            """, fetch=True)
            for r in rows:
                user_id, username = r['user_id'], r['username'] or ""
                t = threading.Thread(target=process_user_queue, args=(user_id, user_id, username), daemon=True)
                t.start()
            time.sleep(2)
        except Exception:
            traceback.print_exc()
            time.sleep(5)

# ========== SCHEDULER ==========
scheduler = BackgroundScheduler()

def hourly_owner_stats():
    cutoff = datetime.utcnow() - timedelta(hours=1)
    rows = db_execute("""
        SELECT user_id, MAX(username) AS username, SUM(words)
        FROM split_logs
        WHERE created_at >= %s
        GROUP BY user_id
    """, (cutoff.isoformat(),), fetch=True)
    if not rows:
        send_message(OWNER_ID, f"🕐 **Last 1 Hour Activity**\nNo splits in the last 1 hour.")
        return
    lines = []
    total_words = 0
    for r in rows:
        uid = r['user_id']
        wsum = int(r['sum'] or 0) if 'sum' in r else int(r[2] or 0)
        username = r['username']
        total_words += wsum
        lines.append(f"{uid} @{username or ''} - {wsum} words")
    body = "🕐 **Last 1 Hour Activity**\n" + "\n".join(lines) + f"\n\nTotal words: {total_words}"
    send_message(OWNER_ID, body)

def delete_old_bot_messages():
    msgs = get_messages_older_than(days=1)
    for m in msgs:
        try:
            delete_message(m["chat_id"], m["message_id"])
        except Exception:
            mark_message_deleted(m["chat_id"], m["message_id"])

def maintenance_hourly_health():
    send_message(OWNER_ID, "👑 **Bot Health Check**\nBot is running. Time: " + get_now_iso())

scheduler.add_job(hourly_owner_stats, "interval", hours=1, next_run_time=datetime.utcnow() + timedelta(seconds=10))
scheduler.add_job(delete_old_bot_messages, "interval", minutes=30, next_run_time=datetime.utcnow() + timedelta(seconds=20))
scheduler.add_job(maintenance_hourly_health, "interval", hours=1, next_run_time=datetime.utcnow() + timedelta(seconds=15))
scheduler.start()

_worker_thread = threading.Thread(target=global_worker_loop, daemon=True)
_worker_thread.start()

# ========== FLASK WEBHOOK/APIS ==========

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update_json = request.get_json(force=True)
    except Exception:
        return "no json", 400
    try:
        if "message" in update_json:
            msg = update_json["message"]
            user = msg.get("from", {})
            user_id = user.get("id")
            username = user.get("username") or user.get("first_name") or ""
            text = msg.get("text") or ""
            if text.startswith("/"):
                parts = text.split(None, 1)
                command = parts[0].split("@")[0].lower()
                args = parts[1] if len(parts) > 1 else ""
                return handle_command(user_id, username, command, args)
            else:
                return handle_new_text(user_id, username, text)
    except Exception:
        logger.exception("Error handling webhook update")
    return jsonify({"ok": True})

def get_queue_size(user_id):
    q = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status = 'queued'", (user_id,), fetch=True)
    return q[0][0] if q else 0

def handle_command(user_id: int, username: str, command: str, args: str):
    if not is_allowed(user_id) and command not in ("/start", "/help"):
        send_message(user_id, f"❌ **Sorry!** You are not allowed to use this bot. A request has been sent to the owner.")
        send_message(OWNER_ID, f"⚠️ Unallowed access attempt by @{username or user_id} ({user_id}). Message: {args or '[no text]'}")
        return jsonify({"ok": True})
    if command == "/start":
        body = (
            f"👋 **Welcome, @{username or user_id}!**\n"
            "I split your text into individual word messages. Commands:\n"
            "/start /example /pause /resume /status /stop /stats /about\n"
            "Owner-only: /adduser /removeuser /listusers /botinfo /broadcast\n"
            "How to use: send any text to split it into words."
        )
        send_message(user_id, body)
        return jsonify({"ok": True})

    if command == "/example":
        sample = "This is a demo split"
        send_message(user_id, "Running example split...")
        enqueue_task(user_id, username, sample)
        send_message(user_id, f"📝 **Queued!** You currently have **{get_queue_size(user_id)}** task(s) waiting in line. Your current task must finish first.")
        return jsonify({"ok": True})

    if command == "/pause":
        rows = db_execute("SELECT id FROM tasks WHERE user_id = %s AND status = 'running' ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        if not rows:
            send_message(user_id, "❌ **Sorry!** No active task found for you to pause.")
            return jsonify({"ok": True})
        task_id = rows[0]['id']
        mark_task_paused(task_id)
        send_message(user_id, "⏸️ **Task Paused!** Waiting for /resume to continue.")
        return jsonify({"ok": True})

    if command == "/resume":
        rows = db_execute("SELECT id FROM tasks WHERE user_id = %s AND status = 'paused' ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        if not rows:
            send_message(user_id, "❌ **Sorry!** No paused task found for you to resume.")
            return jsonify({"ok": True})
        task_id = rows[0]['id']
        mark_task_resumed(task_id)
        send_message(user_id, "▶️ **Welcome Back!** Resuming word transmission now.")
        return jsonify({"ok": True})

    if command == "/status":
        active = db_execute("SELECT id, status, total_words, created_at FROM tasks WHERE user_id = %s AND status IN ('running','paused') ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        queued = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status = 'queued'", (user_id,), fetch=True)[0][0]
        if active:
            aid, status, total_words, created_at = active[0]['id'], active[0]['status'], active[0]['total_words'], active[0]['created_at']
            send_message(user_id, f"📊 **Current Status Check**\nStatus: {status}\nRemaining words: {total_words}\nQueue size: {queued}")
        else:
            if queued > 0:
                send_message(user_id, f"📝 **Status: Waiting.** Your first task is waiting in line. Queue size: {queued}")
            else:
                send_message(user_id, "📊 You have no active or queued tasks.")
        return jsonify({"ok": True})

    if command == "/stop":
        queued_rows = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status = 'queued'", (user_id,), fetch=True)
        queued_count = queued_rows[0][0] if queued_rows else 0
        stopped = cancel_active_task_for_user(user_id)
        if stopped > 0:
            send_message(user_id, f"🛑 **Active Task Stopped!** The current word transmission has been terminated. All your queued tasks have also been cleared.")
        elif queued_count > 0:
            db_execute("UPDATE tasks SET status = 'cancelled' WHERE user_id = %s AND status = 'queued'", (user_id,))
            send_message(user_id, f"🛑 **Queued Tasks Cleared!** You had **{queued_count}** task(s) waiting that have been removed.")
        else:
            send_message(user_id, "ℹ️ You had no active or queued tasks.")
        return jsonify({"ok": True})

    if command == "/stats":
        cutoff = datetime.utcnow() - timedelta(hours=12)
        rows = db_execute("SELECT SUM(words) FROM split_logs WHERE created_at >= %s", (cutoff.isoformat(),), fetch=True)
        words = int(rows[0][0] or 0)
        send_message(user_id, f"🕰️ **Your Recent Activity (Last 12 Hours)**\nYou have split **{words}** words! Keep up the great work! 💪 📝")
        return jsonify({"ok": True})

    if command == "/about":
        body = (
            "*About This Bot*\n"
            "I split texts into individual word messages. Speed control is dynamic based on total words.\n"
            "Features: queueing, pause/resume, maintenance window, hourly owner stats, auto-delete bot messages older than 24h.\n"
            f"Developer: @{OWNER_USERNAME}"
        )
        send_message(user_id, body)
        return jsonify({"ok": True})

    if command == "/adduser":
        if not is_admin(user_id):
            send_message(user_id, "❌ You are not allowed to use this command.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /adduser <telegram_user_id> [username]")
            return jsonify({"ok": True})
        parts = args.split()
        try:
            target_id = int(parts[0])
        except Exception:
            send_message(user_id, "Invalid user id. Must be numeric.")
            return jsonify({"ok": True})
        uname = parts[1] if len(parts) > 1 else ""
        count = db_execute("SELECT COUNT(*) FROM allowed_users", fetch=True)[0][0]
        if count >= MAX_ALLOWED_USERS:
            send_message(user_id, f"Cannot add more users. Max allowed users: {MAX_ALLOWED_USERS}")
            return jsonify({"ok": True})
        db_execute("INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (%s, %s, %s, 0) ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username", (target_id, uname, get_now_iso()))
        send_message(user_id, f"✅ User {target_id} added to allowed list.")
        send_message(target_id, "✅ You have been added to the Word Splitter bot. Send any text to start.")
        return jsonify({"ok": True})

    if command == "/removeuser":
        if not is_admin(user_id):
            send_message(user_id, "❌ You are not allowed to use this command.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /removeuser <telegram_user_id>")
            return jsonify({"ok": True})
        try:
            target_id = int(args.split()[0])
        except Exception:
            send_message(user_id, "Invalid user id.")
            return jsonify({"ok": True})
        db_execute("DELETE FROM allowed_users WHERE user_id = %s", (target_id,))
        send_message(user_id, f"✅ User {target_id} removed from allowed list.")
        send_message(target_id, "❌ You have been removed from the Word Splitter bot. Contact owner to regain access.")
        return jsonify({"ok": True})

    if command == "/listusers":
        if not is_admin(user_id):
            send_message(user_id, "❌ You are not allowed to use this command.")
            return jsonify({"ok": True})
        rows = db_execute("SELECT user_id, username, is_admin, added_at FROM allowed_users", fetch=True)
        lines = []
        for r in rows:
            lines.append(f"{r['user_id']} @{r['username'] or ''} admin={bool(r['is_admin'])} added={r['added_at']}")
        body = "Allowed users:\n" + ("\n".join(lines) if lines else "(none)")
        send_message(user_id, body)
        return jsonify({"ok": True})

    if command == "/botinfo":
        if user_id != OWNER_ID:
            send_message(user_id, "❌ Only the bot owner can use /botinfo")
            return jsonify({"ok": True})
        total_allowed = db_execute("SELECT COUNT(*) FROM allowed_users", fetch=True)[0][0]
        active_tasks = db_execute("SELECT COUNT(*) FROM tasks WHERE status IN ('running','paused')", fetch=True)[0][0]
        queued_tasks = db_execute("SELECT COUNT(*) FROM tasks WHERE status = 'queued'", fetch=True)[0][0]
        rows = db_execute("SELECT user_id, SUM(words) FROM split_logs WHERE created_at >= %s", ((datetime.utcnow() - timedelta(hours=2)).isoformat(),), fetch=True)
        peruser_lines = []
        for r in rows:
            peruser_lines.append(f"{r['user_id']} - {r['sum'] or 0}")
        body = (
            f"Bot status: Online\n"
            f"Allowed users: {total_allowed}\n"
            f"Active tasks: {active_tasks}\n"
            f"Queued tasks: {queued_tasks}\n"
            f"User stats (last 2h):\n" + ("\n".join(peruser_lines) if peruser_lines else "No activity")
        )
        send_message(user_id, body)
        return jsonify({"ok": True})

    if command == "/broadcast":
        if user_id != OWNER_ID:
            send_message(user_id, "❌ Only owner can broadcast")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /broadcast <message>")
            return jsonify({"ok": True})
        rows = db_execute("SELECT user_id FROM allowed_users", fetch=True)
        count = 0
        fails = 0
        for r in rows:
            try:
                send_message(r['user_id'], f"📣 Broadcast from owner:\n\n{args}")
                count += 1
            except Exception:
                fails += 1
        send_message(user_id, f"Broadcast complete. Success: {count}, Failed: {fails}")
        return jsonify({"ok": True})

    send_message(user_id, "Unknown command.")
    return jsonify({"ok": True})


def handle_new_text(user_id: int, username: str, text: str):
    if not is_allowed(user_id):
        send_message(user_id, "❌ **Sorry!** You are not allowed to use this bot. A request has been sent to the owner.")
        send_message(OWNER_ID, f"⚠️ Unallowed access attempt by @{username or user_id} ({user_id}). Message: {text}")
        return jsonify({"ok": True})
    if is_maintenance_now():
        send_message(user_id, "🛠️ **Maintenance in Progress!** New tasks are blocked. Please try again after 4 AM WAT.")
        send_message(OWNER_ID, f"🛠️ Maintenance attempted during window by {user_id}.")
        return jsonify({"ok": True})
    res = enqueue_task(user_id, username, text)
    if not res["ok"]:
        if res.get("reason") == "empty":
            send_message(user_id, "Empty or whitespace-only text. Nothing to split.")
            return jsonify({"ok": True})
        if res.get("reason") == "queue_full":
            send_message(user_id, f"❌ Your queue is full ({res['queue_size']}). Please /stop or wait for tasks to finish.")
            return jsonify({"ok": True})
    qsize = get_queue_size(user_id)
    if qsize > 1:
        send_message(user_id, f"📝 **Queued!** You currently have **{qsize}** task(s) waiting in line. Your current task must finish first.")
    else:
        send_message(user_id, f"🚀 Task queued and will start shortly. Words: {res['total_words']}. Time per word: **{compute_interval(res['total_words'])}s**")
    return jsonify({"ok": True})

@app.route("/", methods=["GET", "POST"])
def root_forward():
    if request.method == "POST":
        logger.info("Received POST at root; forwarding to /webhook")
        try:
            return webhook()
        except Exception:
            logger.exception("Forwarding POST to webhook failed")
            return jsonify({"ok": False, "error": "forward failed"}), 200
    return "Word Splitter Bot is running.", 200

@app.route("/health", methods=["GET", "HEAD"])
@app.route("/health/", methods=["GET", "HEAD"])
def health():
    logger.info("Health check from %s method=%s", request.remote_addr, request.method)
    return jsonify({"ok": True, "time": datetime.utcnow().isoformat()}), 200

@app.route("/debug/routes", methods=["GET"])
def debug_routes():
    try:
        rules = sorted(str(r) for r in app.url_map.iter_rules())
        return jsonify({"routes": rules}), 200
    except Exception:
        logger.exception("Failed to list routes")
        return jsonify({"ok": False, "error": "failed to list routes"}), 500

if __name__ == "__main__":
    try:
        set_webhook()
    except Exception:
        logger.exception("Failed to set webhook at startup")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
