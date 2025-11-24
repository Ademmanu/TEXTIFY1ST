#!/usr/bin/env python3
"""
Final integrated Telegram Word-Splitter Bot (app.py)

Features implemented per user request:
- OWNER_IDS and ALLOWED_USERS env vars
- /adduser, /listusers (admin)
- /suspend, /unsuspend, /listsuspended (admin) with usage examples when input is invalid
- Broadcast uses one-shot sender (no retries) to avoid duplicate deliveries
- Hourly owner stats (last 1 hour) sent automatically and included in /botinfo
- /help removed completely
- Bot-health hourly reports removed
- Simplified, shorter replies and ~20% fewer emojis compared to earlier versions (OVERRIDDEN BY 80% EMOJI REQUIREMENT)
- Suspensions enforce immediately and cancel running/queued tasks
- Timestamps formatted as "YYYY-MM-DD HH:MM:SS"
- Emojified replies but toned down (OVERRIDDEN BY 80% EMOJI REQUIREMENT)
"""

import os
import time
import json
import sqlite3
import threading
import logging
import re
import random
from datetime import datetime, timedelta
from typing import List
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify
import psutil
import requests
import traceback

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("wordsplitter")

# App
app = Flask(__name__)

# Config from env
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
OWNER_IDS_RAW = os.environ.get("OWNER_IDS", "")      # comma/space separated IDs
ALLOWED_USERS_RAW = os.environ.get("ALLOWED_USERS", "")  # auto-allowed IDs
OWNER_USERNAMES_RAW = os.environ.get("OWNER_USERNAMES", "")
DB_PATH = os.environ.get("DB_PATH", "botdata.sqlite3")
MAX_ALLOWED_USERS = int(os.environ.get("MAX_ALLOWED_USERS", "500"))
MAX_QUEUE_PER_USER = int(os.environ.get("MAX_QUEUE_PER_USER", "50"))
REQUESTS_TIMEOUT = float(os.environ.get("REQUESTS_TIMEOUT", "10"))
MAX_MSG_PER_SECOND = float(os.environ.get("MAX_MSG_PER_SECOND", "50"))

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None
_session = requests.Session()

def parse_id_list(raw: str) -> List[int]:
    if not raw:
        return []
    parts = re.split(r"[,\s]+", raw.strip())
    ids = []
    for p in parts:
        if not p:
            continue
        try:
            ids.append(int(p))
        except Exception:
            continue
    return ids

OWNER_IDS = parse_id_list(OWNER_IDS_RAW)
OWNER_USERNAMES = [s for s in (OWNER_USERNAMES_RAW.split(",") if OWNER_USERNAMES_RAW else []) if s]
PRIMARY_OWNER = OWNER_IDS[0] if OWNER_IDS else None

def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# DB helpers
_db_lock = threading.Lock()

def init_db():
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS allowed_users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            added_at TEXT,
            is_admin INTEGER DEFAULT 0
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            text TEXT,
            words_json TEXT,
            total_words INTEGER,
            sent_count INTEGER DEFAULT 0,
            status TEXT,
            created_at TEXT,
            started_at TEXT,
            finished_at TEXT
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS split_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            words INTEGER,
            created_at TEXT
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS sent_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            message_id INTEGER,
            sent_at TEXT,
            deleted INTEGER DEFAULT 0
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS suspended_users (
            user_id INTEGER PRIMARY KEY,
            suspended_until TEXT,
            reason TEXT,
            added_at TEXT
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS send_failures (
            user_id INTEGER PRIMARY KEY,
            failures INTEGER,
            last_failure_at TEXT
        )""")
        conn.commit()

init_db()

# Ensure owners are admins in allowed_users
for idx, oid in enumerate(OWNER_IDS):
    uname = OWNER_USERNAMES[idx] if idx < len(OWNER_USERNAMES) else ""
    try:
        exists = None
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (oid,))
            exists = c.fetchone()
        if not exists:
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("INSERT OR REPLACE INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)",
                          (oid, uname, now_ts(), 1))
                conn.commit()
        else:
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("UPDATE allowed_users SET is_admin = 1 WHERE user_id = ?", (oid,))
                conn.commit()
    except Exception:
        logger.exception("Error ensuring owner in allowed_users")

# Auto-add ALLOWED_USERS env var
for uid in parse_id_list(ALLOWED_USERS_RAW):
    try:
        rows = None
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (uid,))
            rows = c.fetchone()
        if not rows:
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)",
                          (uid, "", now_ts(), 0))
                conn.commit()
            # best-effort notify
            try:
                if TELEGRAM_API:
                    # EMOJI-RICH REPLY
                    _session.post(f"{TELEGRAM_API}/sendMessage", json={"chat_id": uid, "text": "🎉 You were added via ALLOWED_USERS! 👋 Welcome aboard! 🚀"}, timeout=3)
            except Exception:
                pass
    except Exception:
        logger.exception("Auto-add allowed user error")

# Token bucket for normal sends
_token_bucket = {"tokens": MAX_MSG_PER_SECOND, "last": time.time(), "capacity": max(1.0, MAX_MSG_PER_SECOND), "lock": threading.Lock()}

def acquire_token(timeout=10.0):
    start = time.time()
    while True:
        with _token_bucket["lock"]:
            now = time.time()
            elapsed = now - _token_bucket["last"]
            if elapsed > 0:
                refill = elapsed * MAX_MSG_PER_SECOND
                _token_bucket["tokens"] = min(_token_bucket["capacity"], _token_bucket["tokens"] + refill)
                _token_bucket["last"] = now
            if _token_bucket["tokens"] >= 1:
                _token_bucket["tokens"] -= 1
                return True
        if time.time() - start >= timeout:
            return False
        time.sleep(0.01)

def parse_telegram_json(resp):
    try:
        return resp.json()
    except Exception:
        return None

# Normal send with token bucket and limited retry/backoff
def send_message(chat_id: int, text: str, parse_mode: str = "Markdown"):
    if not TELEGRAM_API:
        logger.error("No TELEGRAM_TOKEN; cannot send message.")
        return None
    # acquire token (best-effort)
    acquire_token(timeout=5.0)
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
    try:
        resp = _session.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=REQUESTS_TIMEOUT)
    except Exception as e:
        logger.exception("Network send error")
        increment_failure(chat_id)
        return None
    data = parse_telegram_json(resp)
    if data and data.get("ok"):
        # record sent
        try:
            mid = data["result"].get("message_id")
            if mid:
                with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                    c = conn.cursor()
                    c.execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)",
                              (chat_id, mid, now_ts()))
                    conn.commit()
        except Exception:
            logger.exception("record sent message failed")
        reset_failures(chat_id)
        return data["result"]
    else:
        increment_failure(chat_id)
        return None

def increment_failure(user_id: int):
    try:
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT failures FROM send_failures WHERE user_id = ?", (user_id,))
            row = c.fetchone()
            if not row:
                c.execute("INSERT INTO send_failures (user_id, failures, last_failure_at) VALUES (?, ?, ?)",
                          (user_id, 1, now_ts()))
                failures = 1
            else:
                failures = int(row[0] or 0) + 1
                c.execute("UPDATE send_failures SET failures = ?, last_failure_at = ? WHERE user_id = ?",
                          (failures, now_ts(), user_id))
            conn.commit()
        if failures >= 6:
            # EMOJI-RICH REPLY
            notify_owners(f"⚠️ Repeated send failures for {user_id} ({failures})! 🛑 Stopping their tasks immediately. ⛔")
            cancel_active_task_for_user(user_id)
    except Exception:
        logger.exception("increment_failure error")

def reset_failures(user_id: int):
    try:
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("DELETE FROM send_failures WHERE user_id = ?", (user_id,))
            conn.commit()
    except Exception:
        pass

# Broadcast one-shot sender (no retries)
def broadcast_send_raw(chat_id: int, text: str):
    if not TELEGRAM_API:
        return False, "no_token"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown", "disable_web_page_preview": True}
    try:
        resp = _session.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=REQUESTS_TIMEOUT)
    except Exception as e:
        logger.info("Broadcast network error to %s: %s", chat_id, e)
        return False, str(e)
    data = parse_telegram_json(resp)
    if data and data.get("ok"):
        try:
            mid = data["result"].get("message_id")
            if mid:
                with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                    c = conn.cursor()
                    c.execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)",
                              (chat_id, mid, now_ts()))
                    conn.commit()
        except Exception:
            pass
        return True, "ok"
    reason = data.get("description") if isinstance(data, dict) else "error"
    logger.info("Broadcast failed to %s: %s", chat_id, reason)
    return False, reason

# Task queue management
def split_text_to_words(text: str) -> List[str]:
    return [w for w in text.strip().split() if w]

def enqueue_task(user_id: int, username: str, text: str):
    words = split_text_to_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
        pending = c.fetchone()[0]
        if pending >= MAX_QUEUE_PER_USER:
            return {"ok": False, "reason": "queue_full", "queue_size": pending}
        c.execute("INSERT INTO tasks (user_id, username, text, words_json, total_words, status, created_at, sent_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                  (user_id, username, text, json.dumps(words), total, "queued", now_ts(), 0))
        conn.commit()
    return {"ok": True, "total_words": total, "queue_size": pending + 1}

def get_next_task_for_user(user_id: int):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT id, words_json, total_words, text FROM tasks WHERE user_id = ? AND status = 'queued' ORDER BY id ASC LIMIT 1", (user_id,))
        r = c.fetchone()
    if not r:
        return None
    return {"id": r[0], "words": json.loads(r[1]), "total_words": r[2], "text": r[3]}

def set_task_status(task_id: int, status: str):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        if status == "running":
            c.execute("UPDATE tasks SET status = ?, started_at = ? WHERE id = ?", (status, now_ts(), task_id))
        elif status in ("done", "cancelled"):
            c.execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", (status, now_ts(), task_id))
        else:
            c.execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))
        conn.commit()

def cancel_active_task_for_user(user_id: int):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM tasks WHERE user_id = ? AND status IN ('queued','running','paused')", (user_id,))
        rows = c.fetchall()
        count = 0
        for r in rows:
            tid = r[0]
            c.execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", ("cancelled", now_ts(), tid))
            count += 1
        conn.commit()
    return count

def record_split_log(user_id: int, username: str, words: int):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("INSERT INTO split_logs (user_id, username, words, created_at) VALUES (?, ?, ?, ?)", (user_id, username, words, now_ts()))
        conn.commit()

# Allowed/admin/suspended checks
def is_allowed(user_id: int) -> bool:
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,))
        return bool(c.fetchone())

def is_admin(user_id: int) -> bool:
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT is_admin FROM allowed_users WHERE user_id = ?", (user_id,))
        r = c.fetchone()
        return bool(r and r[0])

def suspend_user(target_id: int, seconds: int, reason: str = ""):
    until = (datetime.utcnow() + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO suspended_users (user_id, suspended_until, reason, added_at) VALUES (?, ?, ?, ?)",
                  (target_id, until, reason, now_ts()))
        conn.commit()
    stopped = cancel_active_task_for_user(target_id)
    try:
        # EMOJI-RICH REPLY
        send_message(target_id, f"⛔ You were suspended until *{until} UTC*! ⏳\nReason: {reason or '(_No reason provided_) 🤷'}")
    except Exception:
        logger.exception("notify suspended user failed")
    # EMOJI-RICH REPLY
    notify_owners(f"⛔ User *{target_id}* suspended until *{until} UTC*! 🛑 Stopped *{stopped}* tasks. Reason: {reason or '(_none_)'} 📝")

def unsuspend_user(target_id: int) -> bool:
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (target_id,))
        r = c.fetchone()
        if not r:
            return False
        c.execute("DELETE FROM suspended_users WHERE user_id = ?", (target_id,))
        conn.commit()
    try:
        # EMOJI-RICH REPLY
        send_message(target_id, "✅ Your suspension has been lifted! 🥳 You may use the bot again! 🚀")
    except Exception:
        logger.exception("notify unsuspended failed")
    # EMOJI-RICH REPLY
    notify_owners(f"✅ User *{target_id}* unsuspended! 🎉")
    return True

def list_suspended():
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id, suspended_until, reason, added_at FROM suspended_users ORDER BY suspended_until ASC")
        return c.fetchall()

def is_suspended(user_id: int) -> bool:
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,))
        r = c.fetchone()
        if not r:
            return False
        try:
            until = datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S")
            return until > datetime.utcnow()
        except Exception:
            return False

# Worker to process user queues
_user_locks = {}
_user_locks_lock = threading.Lock()
_worker_stop = threading.Event()

def get_user_lock(uid: int):
    with _user_locks_lock:
        if uid not in _user_locks:
            _user_locks[uid] = threading.Lock()
        return _user_locks[uid]

def process_user_queue(user_id: int, chat_id: int, username: str):
    lock = get_user_lock(user_id)
    if not lock.acquire(blocking=False):
        return
    try:
        if is_suspended(user_id):
            try:
                # EMOJI-RICH REPLY
                send_message(user_id, "⛔ You are suspended! ⏳ Tasks won't run until the suspension ends. 🚫")
            except Exception:
                pass
            return
        while True:
            task = get_next_task_for_user(user_id)
            if not task:
                break
            if is_suspended(user_id):
                # EMOJI-RICH REPLY
                send_message(user_id, "⛔ You were suspended during processing! ✋ Tasks paused/cancelled. 🚫")
                break
            task_id = task["id"]
            words = task["words"]
            total = task["total_words"]
            set_task_status(task_id, "running")
            sent_info = None
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("SELECT sent_count FROM tasks WHERE id = ?", (task_id,))
                sent_info = c.fetchone()
            sent = int(sent_info[0] or 0) if sent_info else 0
            remaining = max(0, total - sent)
            interval = 0.4 if total <= 150 else (0.5 if total <= 300 else 0.6)
            est_seconds = int(remaining * interval)
            est_str = str(timedelta(seconds=est_seconds))
            # EMOJI-RICH REPLY
            send_message(chat_id, f"🚀 Starting split now! 🤩 Total words: *{total}* 📝. Estimated time: *{est_str}* ⏱️.")
            i = sent
            consecutive_failures = 0
            while i < total:
                row = None
                with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                    c = conn.cursor()
                    c.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                    row = c.fetchone()
                if not row:
                    break
                status = row[0]
                if status == "paused":
                    # EMOJI-RICH REPLY
                    send_message(chat_id, "⏸️ Paused! ✋ Use /resume to continue splitting. ▶️")
                    while True:
                        time.sleep(0.5)
                        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                            c = conn.cursor()
                            c.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                            row2 = c.fetchone()
                        if not row2:
                            break
                        if row2[0] == "running":
                            # EMOJI-RICH REPLY
                            send_message(chat_id, "▶️ Resuming the task! 🚀")
                            break
                        if row2[0] == "cancelled":
                            break
                    if row2 and row2[0] == "cancelled":
                        break
                if status == "cancelled":
                    break
                res = send_message(chat_id, words[i])
                if res is None:
                    consecutive_failures += 1
                    if consecutive_failures >= 4:
                        # EMOJI-RICH REPLY
                        notify_owners(f"⚠️ Repeated send failures for {user_id}! 🚨 Stopping tasks. ⛔")
                        cancel_active_task_for_user(user_id)
                        break
                else:
                    consecutive_failures = 0
                with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                    c = conn.cursor()
                    c.execute("UPDATE tasks SET sent_count = sent_count + 1 WHERE id = ?", (task_id,))
                    conn.commit()
                i += 1
                time.sleep(interval)
            # finalize
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("SELECT status, sent_count, total_words FROM tasks WHERE id = ?", (task_id,))
                r = c.fetchone()
            if r:
                final_status = r[0]
                sent_count = int(r[1] or 0)
            else:
                final_status = "done"
                sent_count = total
            if final_status != "cancelled":
                set_task_status(task_id, "done")
                record_split_log(user_id, username, sent_count)
                # EMOJI-RICH REPLY
                send_message(chat_id, "✅ Done splitting! 🎉 Your text was successfully sent word by word! 💌")
            else:
                # EMOJI-RICH REPLY
                send_message(chat_id, "🛑 Task manually stopped or cancelled due to error. 🙅")
    finally:
        lock.release()

def global_worker():
    while not _worker_stop.is_set():
        try:
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("SELECT DISTINCT user_id, username FROM tasks WHERE status = 'queued' ORDER BY created_at ASC")
                rows = c.fetchall()
            for r in rows:
                uid = r[0]
                uname = r[1] or ""
                if is_suspended(uid):
                    continue
                t = threading.Thread(target=process_user_queue, args=(uid, uid, uname), daemon=True)
                t.start()
            time.sleep(0.6)
        except Exception:
            logger.exception("global worker error")
            time.sleep(1.0)

threading.Thread(target=global_worker, daemon=True).start()

# Scheduler for hourly stats and suspension lifting
scheduler = BackgroundScheduler()

def compute_last_hour_stats():
    cutoff = datetime.utcnow() - timedelta(hours=1)
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id, username, SUM(words) as s FROM split_logs WHERE created_at >= ? GROUP BY user_id ORDER BY s DESC", (cutoff.strftime("%Y-%m-%d %H:%M:%S"),))
        rows = c.fetchall()
    return rows

def send_hourly_owner_stats():
    rows = compute_last_hour_stats()
    if not rows:
        # EMOJI-RICH REPLY
        msg = "⏰ Hourly Report: last 1h ⏳ — no splits were performed. 🙁"
        for oid in OWNER_IDS:
            try:
                send_message(oid, msg)
            except Exception:
                pass
        return
    lines = []
    for r in rows:
        uid = r[0]
        uname = r[1] or ""
        w = int(r[2] or 0)
        part = f"*{uid}* ({uname}) - *{w}* words 📝" if uname else f"*{uid}* - *{w}* words 📝"
        lines.append(part)
    body = "⏰ Hourly Report — last 1h activity: 📊\n" + "\n".join(lines)
    for oid in OWNER_IDS:
        try:
            send_message(oid, body)
        except Exception:
            pass

# Lift suspensions when due
def check_and_lift():
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id, suspended_until FROM suspended_users")
        rows = c.fetchall()
    now = datetime.utcnow()
    for r in rows:
        try:
            until = datetime.strptime(r[1], "%Y-%m-%d %H:%M:%S")
            if until <= now:
                uid = r[0]
                unsuspend_user(uid)
        except Exception:
            logger.exception("suspend parse error for %s", r)

# Job registration
scheduler.add_job(send_hourly_owner_stats, "interval", hours=1, next_run_time=datetime.utcnow() + timedelta(seconds=10))
scheduler.add_job(check_and_lift, "interval", minutes=1, next_run_time=datetime.utcnow() + timedelta(seconds=15))
scheduler.start()

# Notify owners helper
def notify_owners(text: str):
    for oid in OWNER_IDS:
        try:
            send_message(oid, f"👑 {text} 🚨")
        except Exception:
            logger.exception("notify owner failed for %s", oid)

# Webhook endpoints
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = request.get_json(force=True)
    except Exception:
        return jsonify({"ok": False}), 400
    try:
        if "message" in update:
            msg = update["message"]
            user = msg.get("from", {})
            uid = user.get("id")
            username = user.get("username") or (user.get("first_name") or "")
            text = msg.get("text") or ""
            if text.startswith("/"):
                parts = text.split(None, 1)
                cmd = parts[0].split("@")[0].lower()
                args = parts[1] if len(parts) > 1 else ""
                return handle_command(uid, username, cmd, args)
            else:
                return handle_user_text(uid, username, text)
    except Exception:
        logger.exception("webhook handling error")
    return jsonify({"ok": True})

@app.route("/", methods=["GET"])
def root():
    return "WordSplitter running. 🏃", 200

@app.route("/health", methods=["GET", "HEAD"])
def health():
    return jsonify({"ok": True, "ts": now_ts(), "status": "💚"}), 200

# Command handlers
def handle_command(user_id: int, username: str, command: str, args: str):
    # /start allowed for anyone
    if command == "/start":
        msg = (
            f"👋 Hello there, *{username or user_id}*! 🎉\n\n"
            "I'm the WordSplitter Bot 🤖 — I turn your text into individual, word-by-word messages! 💬\n\n"
            "Just send me anything! 📝 I'll start splitting immediately. 🚀\n"
            "Need a preview? Try /example! 👀"
        )
        send_message(user_id, msg)
        return jsonify({"ok": True})

    # require allowed for other commands
    if not is_allowed(user_id):
        # EMOJI-RICH REPLY
        send_message(user_id, "❌ Sorry! You're not allowed to use this bot. 🔒 Owner has been notified. 🚨")
        notify_owners(f"❌ Unallowed access attempt by *{username or user_id}* ({user_id}). 🕵️")
        return jsonify({"ok": True})

    # User commands
    if command == "/example":
        # ⭐ MODIFICATION: NEW SAMPLE TEXT ⭐
        sample = (
            "996770061141 996770064514 996770071665 996770073284 "
            "996770075145 996770075627 996770075973 996770076350 "
            "996770076869 996770077198"
        )
        res = enqueue_task(user_id, username, sample)
        if not res["ok"]:
            # EMOJI-RICH REPLY
            send_message(user_id, "🚫 Could not queue the demo! 🥺 Try again later. 🕰️")
            return jsonify({"ok": True})
        # EMOJI-RICH REPLY
        send_message(user_id, f"🎉 Demo queued! 📝 Will split *{res['total_words']}* words! 🚀")
        return jsonify({"ok": True})

    if command == "/pause":
        rows = None
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'running' ORDER BY started_at ASC LIMIT 1", (user_id,))
            rows = c.fetchone()
        if not rows:
            # EMOJI-RICH REPLY
            send_message(user_id, "😴 No active task found to pause. 🧐")
            return jsonify({"ok": True})
        set_task_status(rows[0], "paused")
        # EMOJI-RICH REPLY
        send_message(user_id, "⏸️ Task paused! ✅ Use /resume to continue splitting. ▶️")
        return jsonify({"ok": True})

    if command == "/resume":
        rows = None
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'paused' ORDER BY started_at ASC LIMIT 1", (user_id,))
            rows = c.fetchone()
        if not rows:
            # EMOJI-RICH REPLY
            send_message(user_id, "💤 No paused task to resume! 🤷")
            return jsonify({"ok": True})
        set_task_status(rows[0], "running")
        # EMOJI-RICH REPLY
        send_message(user_id, "▶️ Resumed! Go, go, go! 🚀")
        return jsonify({"ok": True})

    if command == "/status":
        if is_suspended(user_id):
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,))
                r = c.fetchone()
                until = r[0] if r else "unknown"
            # EMOJI-RICH REPLY
            send_message(user_id, f"⛔ Status: *Suspended*! ⏳ Until: *{until} UTC*. 🚫")
            return jsonify({"ok": True})
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT id, status, total_words, sent_count FROM tasks WHERE user_id = ? AND status IN ('running','paused') ORDER BY started_at ASC LIMIT 1", (user_id,))
            active = c.fetchone()
            c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
            queued = c.fetchone()[0]
        if active:
            aid, status, total, sent = active
            remaining = int(total or 0) - int(sent or 0)
            # EMOJI-RICH REPLY
            send_message(user_id, f"Current Status: *{status.upper()}*! ⚙️ Remaining words: *{remaining}* 📝. Queue size: *{queued}* 📝")
        else:
            # EMOJI-RICH REPLY
            send_message(user_id, f"😌 No *active* tasks right now. Queue size: *{queued}* 📝")
        return jsonify({"ok": True})

    if command == "/stop":
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
            queued = c.fetchone()[0]
        stopped = cancel_active_task_for_user(user_id)
        if stopped > 0 or queued > 0:
            # EMOJI-RICH REPLY
            send_message(user_id, f"🛑 Stopped *{stopped}* active 💥 and cleared *{queued}* queued tasks! 🗑️")
        else:
            # EMOJI-RICH REPLY
            send_message(user_id, "🤷‍♀️ No active or queued tasks to stop! 😌")
        return jsonify({"ok": True})

    if command == "/stats":
        cutoff = datetime.utcnow() - timedelta(hours=12)
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT SUM(words) FROM split_logs WHERE user_id = ? AND created_at >= ?", (user_id, cutoff.strftime("%Y-%m-%d %H:%M:%S")))
            r = c.fetchone()
            words = int(r[0] or 0) if r else 0
        # EMOJI-RICH REPLY
        send_message(user_id, f"📊 Last 12 hours activity: *{words}* words split! 📝✨")
        return jsonify({"ok": True})

    if command == "/about":
        msg = (
            "🤖 WordSplitter Bot Information 📖\n\n"
            "My job is simple! I take any text you send me 📝 and split it into individual, paced word messages 💬.\n"
            "This makes long messages easy to follow or analyze! 🧐\n\n"
            "Admins 🧑‍💻 can manage users, suspend/resume services, and Owners 👑 can send broadcasts! 📢"
        )
        send_message(user_id, msg)
        return jsonify({"ok": True})

    # Admin commands
    if command == "/adduser":
        if not is_admin(user_id):
            # EMOJI-RICH REPLY
            send_message(user_id, "👑 Only *Admin* users can run this command! 🙅‍♂️")
            return jsonify({"ok": True})
        if not args:
            # EMOJI-RICH REPLY
            send_message(user_id, "❌ Missing input! 🤷‍♀️\nUsage: `/adduser <id>` [username]\nExample: `/adduser 12345678` ➕")
            return jsonify({"ok": True})
        parts = re.split(r"[,\s]+", args.strip())
        added, already, invalid = [], [], []
        for p in parts:
            if not p:
                continue
            try:
                tid = int(p)
            except Exception:
                invalid.append(p)
                continue
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (tid,))
                if c.fetchone():
                    already.append(tid)
                    continue
                c.execute("INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)", (tid, "", now_ts(), 0))
                conn.commit()
            added.append(tid)
            try:
                # EMOJI-RICH REPLY
                send_message(tid, "🎉 You have been added to the bot's allowed list! ✅ Send me some text to start! 📝")
            except Exception:
                pass
        parts_msgs = []
        if added: parts_msgs.append("➕ Added: " + ", ".join(str(x) for x in added))
        if already: parts_msgs.append("ℹ️ Already allowed: " + ", ".join(str(x) for x in already))
        if invalid: parts_msgs.append("❌ Invalid ID: " + ", ".join(invalid))
        # EMOJI-RICH REPLY
        send_message(user_id, "✅ User Addition Result: \n" + ("\n".join(parts_msgs) if parts_msgs else "🤷‍♀️ No changes made."))
        return jsonify({"ok": True})

    if command == "/listusers":
        if not is_admin(user_id):
            # EMOJI-RICH REPLY
            send_message(user_id, "👑 Only *Admin* users can run this command! 🙅‍♂️")
            return jsonify({"ok": True})
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT user_id, username, is_admin, added_at FROM allowed_users ORDER BY added_at DESC")
            rows = c.fetchall()
        lines = []
        for r in rows:
            uid, uname, isadm, added_at = r
            lines.append(f"*{uid}* ({uname or 'no_uname'}) - {'👑 admin' if isadm else '👤 user'} - since *{added_at}* 🗓️")
        # EMOJI-RICH REPLY
        send_message(user_id, f"👥 Currently Allowed Users ({len(rows)} total):\n" + ("\n".join(lines) if lines else "(none)"))
        return jsonify({"ok": True})

    # Owner-only commands
    if command == "/botinfo":
        if user_id not in OWNER_IDS:
            # EMOJI-RICH REPLY
            send_message(user_id, "👑 This command is for *Owners* only! ✋")
            return jsonify({"ok": True})
        # gather info
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM allowed_users")
            total_allowed = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM suspended_users")
            total_suspended = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM tasks WHERE status IN ('running','paused')")
            active_tasks = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM tasks WHERE status = 'queued'")
            queued_tasks = c.fetchone()[0]
            # users with active tasks
            c.execute("SELECT user_id, username, SUM(total_words - IFNULL(sent_count,0)) as remaining, COUNT(*) as active_count FROM tasks WHERE status IN ('running','paused') GROUP BY user_id ORDER BY remaining DESC")
            active_rows = c.fetchall()
            # last 1h stats
            cutoff = datetime.utcnow() - timedelta(hours=1)
            c.execute("SELECT user_id, username, SUM(words) as s FROM split_logs WHERE created_at >= ? GROUP BY user_id ORDER BY s DESC", (cutoff.strftime("%Y-%m-%d %H:%M:%S"),))
            stats_rows = c.fetchall()
        lines_active = []
        for r in active_rows:
            uid, uname, rem, ac = r
            name = f" ({uname})" if uname else ""
            lines_active.append(f"*{uid}{name}* - *{int(rem)}* remaining 📝 - *{int(ac)}* active tasks ⚙️ - *{int(get_queued_for_user(uid))}* queued 🧘")
        lines_stats = []
        for r in stats_rows:
            uid, uname, s = r
            name = f" ({uname})" if uname else ""
            lines_stats.append(f"*{uid}{name}* - *{int(s)}* words split 🚀")
        body = (
            "🟢 Bot System Status (Owner Info) 👑\n"
            f"👥 Allowed users: *{total_allowed}* ✅\n"
            f"⛔ Suspended users: *{total_suspended}* 🚫\n"
            f"⚙️ Active tasks (running/paused): *{active_tasks}* 🏃\n"
            f"📝 Queued tasks: *{queued_tasks}* 🧘\n\n"
            "👤 Users with Active Tasks: 📊\n" + ("\n".join(lines_active) if lines_active else "(_None active!_) 😴") + "\n\n"
            "📈 User Split Stats (last 1h): ⏰\n" + ("\n".join(lines_stats) if lines_stats else "(_No splits in the last hour!_) 💤")
        )
        send_message(user_id, body)
        return jsonify({"ok": True})

    if command == "/broadcast":
        if user_id not in OWNER_IDS:
            # EMOJI-RICH REPLY
            send_message(user_id, "👑 This command is for *Owners* only! ✋")
            return jsonify({"ok": True})
        if not args:
            # EMOJI-RICH REPLY
            send_message(user_id, "❌ Missing message! 🤷‍♀️\nUsage: `/broadcast <message>`\nExample: `/broadcast Hello everyone! 📢`")
            return jsonify({"ok": True})
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT user_id FROM allowed_users")
            rows = c.fetchall()
        succeeded, failed = [], []
        header = f"📣 Broadcast from Owner: 👑\n\n{args}"
        for r in rows:
            tid = r[0]
            ok, reason = broadcast_send_raw(tid, header)
            if ok:
                succeeded.append(tid)
            else:
                failed.append((tid, reason))
        # EMOJI-RICH REPLY
        summary = f"📢 Broadcast completed! 🎉 Delivered to: *{len(succeeded)}* users. ✅ Failed for: *{len(failed)}* users. ❌"
        send_message(user_id, summary)
        if failed:
            # EMOJI-RICH REPLY
            notify_owners("🚨 Broadcast failures detected! ❌ Users: " + ", ".join(f"*{x[0]}* ({x[1]})" for x in failed))
        return jsonify({"ok": True})

    # Suspend / Unsuspend / Listsuspended
    if command == "/suspend":
        if not is_admin(user_id):
            # EMOJI-RICH REPLY
            send_message(user_id, "❌ Admin access required! 👑")
            return jsonify({"ok": True})
        if not args:
            # EMOJI-RICH REPLY
            send_message(user_id,
                         "❌ Missing input! 🤷‍♀️\nUsage: `/suspend <user_id> <duration> [reason]`\n"
                         "Examples: 📝\n"
                         "`/suspend 12345678 30s Spamming` 🛑\n"
                         "`/suspend 9876543 5m Too many messages` 💥")
            return jsonify({"ok": True})
        parts = args.split(None, 2)
        try:
            target = int(parts[0])
        except Exception:
            # EMOJI-RICH REPLY
            send_message(user_id, "❌ Invalid user ID! 🆔\nExample: `/suspend 12345678 5m Spamming` 🛑")
            return jsonify({"ok": True})
        if len(parts) < 2:
            # EMOJI-RICH REPLY
            send_message(user_id, "❌ Missing duration! ⏳\nExample: `/suspend 12345678 5m Spamming` 🛑")
            return jsonify({"ok": True})
        dur = parts[1]
        reason = parts[2] if len(parts) > 2 else ""
        m = re.match(r"^(\d+)(s|m|h|d)?$", dur)
        if not m:
            # EMOJI-RICH REPLY
            send_message(user_id, "❌ Invalid duration format! 🕰️\nUse: `30s`, `5m`, `3h`, `2d`. 🗓️\nExample: `/suspend 12345678 5m` ⏱️")
            return jsonify({"ok": True})
        val, unit = int(m.group(1)), (m.group(2) or "s")
        mul = {"s":1, "m":60, "h":3600, "d":86400}.get(unit,1)
        seconds = val * mul
        suspend_user(target, seconds, reason)
        # EMOJI-RICH REPLY
        send_message(user_id, f"✅ Successfully suspended user *{target}* for *{val}{unit}*! ⏳")
        return jsonify({"ok": True})

    if command == "/unsuspend":
        if not is_admin(user_id):
            # EMOJI-RICH REPLY
            send_message(user_id, "❌ Admin access required! 👑")
            return jsonify({"ok": True})
        if not args:
            # EMOJI-RICH REPLY
            send_message(user_id,
                         "❌ Missing user ID! 🤷‍♀️\nUsage: `/unsuspend <user_id>`\nExample: `/unsuspend 12345678` ✅")
            return jsonify({"ok": True})
        try:
            target = int(args.split()[0])
        except Exception:
            # EMOJI-RICH REPLY
            send_message(user_id, "❌ Invalid user ID! 🆔\nExample: `/unsuspend 12345678` ✅")
            return jsonify({"ok": True})
        ok = unsuspend_user(target)
        if ok:
            # EMOJI-RICH REPLY
            send_message(user_id, f"✅ User *{target}* has been unsuspended! 🎉")
        else:
            # EMOJI-RICH REPLY
            send_message(user_id, f"ℹ️ User *{target}* was not currently suspended. 😌")
        return jsonify({"ok": True})

    if command == "/listsuspended":
        if not is_admin(user_id):
            # EMOJI-RICH REPLY
            send_message(user_id, "❌ Admin access required! 👑")
            return jsonify({"ok": True})
        rows = list_suspended()
        if not rows:
            # EMOJI-RICH REPLY
            send_message(user_id, "🥳 No users are currently suspended! 🚫")
            return jsonify({"ok": True})
        lines = []
        for r in rows:
            uid, until, reason, added_at = r
            lines.append(f"*{uid}* 👤 until=*{until}* ⏳ reason=*{reason or '(_none_)'}* 📝 added=*{added_at}* 🗓️")
        # EMOJI-RICH REPLY
        send_message(user_id, f"⛔ Currently Suspended Users ({len(rows)} total): 🚫\n" + "\n".join(lines))
        return jsonify({"ok": True})

    # Unknown
    # EMOJI-RICH REPLY
    send_message(user_id, "❓ Unknown command! 🧐 Please check your input. 📝")
    return jsonify({"ok": True})

def get_queued_for_user(user_id: int) -> int:
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
        return c.fetchone()[0]

def handle_user_text(user_id: int, username: str, text: str):
    if not is_allowed(user_id):
        # EMOJI-RICH REPLY
        send_message(user_id, "❌ Sorry! You're not allowed to use this bot. 🔒 Owner has been notified. 🚨")
        notify_owners(f"❌ Unallowed access by *{user_id}*. 🕵️")
        return jsonify({"ok": True})
    if is_suspended(user_id):
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,))
            r = c.fetchone()
            until = r[0] if r else "unknown"
        # EMOJI-RICH REPLY
        send_message(user_id, f"⛔ You are suspended! 🚫 Service unavailable until *{until} UTC*. ⏳")
        return jsonify({"ok": True})
    res = enqueue_task(user_id, username, text)
    if not res["ok"]:
        if res["reason"] == "empty":
            # EMOJI-RICH REPLY
            send_message(user_id, "🚫 No words found! 🤷‍♀️ Please send longer text. 📝")
            return jsonify({"ok": True})
        if res["reason"] == "queue_full":
            # EMOJI-RICH REPLY
            send_message(user_id, f"🈵 Your queue is full (*{res['queue_size']}* tasks)! 🛑 Use /stop to clear it or wait! 🧘")
            return jsonify({"ok": True})
    running = None
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT 1 FROM tasks WHERE user_id = ? AND status IN ('running','paused')", (user_id,))
        running = c.fetchone()
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
        queued = c.fetchone()[0]
    if running:
        # EMOJI-RICH REPLY
        send_message(user_id, f"📝 Task queued! ⏳ Position in queue: *{queued}*. 🧘")
    else:
        # EMOJI-RICH REPLY
        send_message(user_id, f"✅ Task accepted! 🎉 Will split *{res['total_words']}* words. 🚀")
    return jsonify({"ok": True})

# Webhook setup helper
def set_webhook():
    if not TELEGRAM_API or not WEBHOOK_URL:
        logger.info("Webhook not configured. ❌")
        return
    try:
        _session.post(f"{TELEGRAM_API}/setWebhook", json={"url": WEBHOOK_URL}, timeout=REQUESTS_TIMEOUT)
    except Exception:
        logger.exception("set_webhook failed 🚨")
        
@app.route("/mem", methods=["GET"])
def mem_usage():
    process = psutil.Process()
    mem_used_mb = process.memory_info().rss / (1024 * 1024)
    return f"Memory used by app: *{mem_used_mb:.2f}* MB 💾\n"

@app.route("/sysmem", methods=["GET"])
def system_memory():
    vm = psutil.virtual_memory()
    total = vm.total / (1024 * 1024)
    available = vm.available / (1024 * 1024)
    used = vm.used / (1024 * 1024)
    percent = vm.percent
    return (
        f"Total system RAM: *{total:.2f}* MB 🧠\n"
        f"Available RAM: *{available:.2f}* MB ✅\n"
        f"Used RAM: *{used:.2f}* MB 🔴\n"
        f"Usage percent: *{percent}%* 📊\n"
    )

if __name__ == "__main__":
    try:
        set_webhook()
    except Exception:
        pass
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
