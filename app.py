#!/usr/bin/env python3
"""
WordSplitter Telegram Bot - optimized for long-running, moderate-concurrency hosting

Major runtime improvements:
- Single shared SQLite connection (check_same_thread=False) with WAL mode and tuned pragmas.
- DB access serialized via a lock to avoid frequent open/close and reduce sqlite "database is locked" errors.
- Active sender concurrency capped via a semaphore (MAX_CONCURRENT_WORKERS, default 25).
- Token bucket rewritten with Condition to avoid busy-wait CPU usage.
- Requests Session pool increased for higher concurrency.
- Periodic cleanup job to prune old split_logs and sent_messages to avoid unbounded DB growth.
- Graceful shutdown handlers to stop scheduler and user workers on SIGTERM/SIGINT.
- Maintains original behavior and message contents where possible.
"""

import os
import time
import json
import sqlite3
import threading
import logging
import re
import signal
from datetime import datetime, timedelta
from typing import List, Dict
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify
import requests

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("wordsplitter")

app = Flask(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
OWNER_IDS_RAW = os.environ.get("OWNER_IDS", "")      # comma/space separated IDs
ALLOWED_USERS_RAW = os.environ.get("ALLOWED_USERS", "")  # comma/space separated IDs
DB_PATH = os.environ.get("DB_PATH", "botdata.sqlite3")
MAX_QUEUE_PER_USER = int(os.environ.get("MAX_QUEUE_PER_USER", "5"))
MAX_MSG_PER_SECOND = float(os.environ.get("MAX_MSG_PER_SECOND", "50"))
REQUESTS_TIMEOUT = float(os.environ.get("REQUESTS_TIMEOUT", "10"))
MAX_CONCURRENT_WORKERS = int(os.environ.get("MAX_CONCURRENT_WORKERS", "25"))
LOG_RETENTION_DAYS = int(os.environ.get("LOG_RETENTION_DAYS", "30"))

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None

# Configure requests session with larger connection pool
_session = requests.Session()
try:
    from requests.adapters import HTTPAdapter
    adapter = HTTPAdapter(pool_connections=MAX_CONCURRENT_WORKERS*2, pool_maxsize=max(20, MAX_CONCURRENT_WORKERS*2))
    _session.mount("https://", adapter)
    _session.mount("http://", adapter)
except Exception:
    pass

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
PRIMARY_OWNER = OWNER_IDS[0] if OWNER_IDS else None
ALLOWED_USERS = parse_id_list(ALLOWED_USERS_RAW)

NIGERIA_TZ_OFFSET = timedelta(hours=1)
def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
def utc_to_wat_ts(utc_ts: str) -> str:
    try:
        utc_dt = datetime.strptime(utc_ts, "%Y-%m-%d %H:%M:%S")
        wat_dt = utc_dt + NIGERIA_TZ_OFFSET
        return wat_dt.strftime("%Y-%m-%d %H:%M:%S WAT")
    except Exception:
        return f"{utc_ts} (UTC error)"

# Helper formatting functions added per request
def at_username(u: str) -> str:
    """
    Return username WITHOUT the leading '@'. Usernames are displayed plain
    (no @ prefix) and are NOT clickable (no mention entities).
    """
    if not u:
        return ""
    return u.lstrip("@")

def label_for_self(viewer_id: int, username: str) -> str:
    """
    Return a short label when speaking to the user about themselves.
    Regular users do NOT see numeric IDs. Owners may see their own ID.
    """
    if username:
        if viewer_id in OWNER_IDS:
            return f"{at_username(username)} (ID: {viewer_id})"
        return f"{at_username(username)}"
    return f"(ID: {viewer_id})" if viewer_id in OWNER_IDS else ""

def label_for_owner_view(target_id: int, target_username: str) -> str:
    """
    When showing a user in owner-facing messages, include username (no '@') if available,
    otherwise show numeric id. Always include numeric id for clarity to owners.
    """
    if target_username:
        return f"{at_username(target_username)} (ID: {target_id})"
    return str(target_id)

# Replace plain "Owner" mentions with Owner (@justmemmy) in messages.
OWNER_TAG = "Owner (@justmemmy)"

_db_lock = threading.Lock()
GLOBAL_DB_CONN: sqlite3.Connection = None

def _ensure_db_parent(dirpath: str):
    try:
        if dirpath and not os.path.exists(dirpath):
            os.makedirs(dirpath, exist_ok=True)
    except Exception as e:
        logger.warning("Could not create DB parent directory %s: %s", dirpath, e)

def init_db():
    """
    Initialize the DB and create a single global connection with tuned pragmas.
    """
    global DB_PATH, GLOBAL_DB_CONN
    parent = os.path.dirname(os.path.abspath(DB_PATH))
    if parent:
        _ensure_db_parent(parent)

    try:
        # Create connection with shared threading allowed.
        conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-2000;")  # ~2MB cache
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=30000;")  # 30s busy timeout
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS allowed_users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            added_at TEXT
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
        GLOBAL_DB_CONN = conn
        logger.info("DB initialized at %s", DB_PATH)
    except Exception:
        logger.exception("Failed to open DB at %s, falling back to in-memory DB", DB_PATH)
        DB_PATH = ":memory:"
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA cache_size=-2000;")
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("PRAGMA busy_timeout=30000;")
            c = conn.cursor()
            c.execute("""
            CREATE TABLE IF NOT EXISTS allowed_users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                added_at TEXT
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
            GLOBAL_DB_CONN = conn
            logger.info("In-memory DB initialized")
        except Exception:
            GLOBAL_DB_CONN = None
            logger.exception("Failed to initialize in-memory DB; DB operations may fail")

init_db()

# Ensure owners auto-added as allowed (never suspended)
for oid in OWNER_IDS:
    try:
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (oid,))
            exists = c.fetchone()
            if not exists:
                c.execute("INSERT OR REPLACE INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)", (oid, "", now_ts()))
                GLOBAL_DB_CONN.commit()
    except Exception:
        logger.exception("Error ensuring owner in allowed_users")

# Ensure all ALLOWED_USERS auto-added as allowed at startup (skip owners)
for uid in ALLOWED_USERS:
    if uid in OWNER_IDS:
        continue
    try:
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (uid,))
            rows = c.fetchone()
            if not rows:
                c.execute("INSERT INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)",
                          (uid, "", now_ts()))
                GLOBAL_DB_CONN.commit()
        try:
            if TELEGRAM_API:
                _session.post(f"{TELEGRAM_API}/sendMessage", json={
                    "chat_id": uid, "text": "✅ You have been added. Send any text to start."
                }, timeout=3)
        except Exception:
            pass
    except Exception:
        logger.exception("Auto-add allowed user error")

# Token bucket reworked using Condition to avoid busy-wait
class TokenBucket:
    def __init__(self, rate_per_sec: float):
        self.capacity = max(1.0, rate_per_sec)
        self.tokens = self.capacity
        self.rate = rate_per_sec
        self.last = time.monotonic()
        self.cond = threading.Condition()

    def acquire(self, timeout=10.0) -> bool:
        end = time.monotonic() + timeout
        with self.cond:
            while True:
                now = time.monotonic()
                elapsed = now - self.last
                if elapsed > 0:
                    refill = elapsed * self.rate
                    self.tokens = min(self.capacity, self.tokens + refill)
                    self.last = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return True
                remaining = end - time.monotonic()
                if remaining <= 0:
                    return False
                # Wait a short time or until tokens may have been refilled
                wait_time = min(remaining, max(0.01, (1.0 / max(1.0, self.rate))))
                self.cond.wait(timeout=wait_time)

    def notify_all(self):
        with self.cond:
            self.cond.notify_all()

_token_bucket = TokenBucket(MAX_MSG_PER_SECOND)

def acquire_token(timeout=10.0):
    return _token_bucket.acquire(timeout=timeout)

def parse_telegram_json(resp):
    try:
        return resp.json()
    except Exception:
        return None

def _utf16_len(s: str) -> int:
    """
    Return length in UTF-16 code units for a Python string.
    Telegram requires offsets/lengths in UTF-16 code units.
    We use utf-16-le encoding and divide bytes by 2 to get code units.
    """
    if not s:
        return 0
    return len(s.encode("utf-16-le")) // 2

def _build_entities_for_text(text: str):
    """
    Build a list of Telegram message entities for:
    - plain numeric tokens -> type "code" (monospace, copyable)
    """
    if not text:
        return None
    entities = []
    for m in re.finditer(r"\b\d+\b", text):
        py_start = m.start()
        py_end = m.end()
        utf16_offset = _utf16_len(text[:py_start])
        utf16_length = _utf16_len(text[py_start:py_end])
        entities.append({"type": "code", "offset": utf16_offset, "length": utf16_length})
    return entities if entities else None

def increment_failure(user_id: int):
    try:
        if user_id in OWNER_IDS:
            logger.warning("Send failure for owner %s; recording but not notifying owners to avoid recursion/loops.", user_id)
            with _db_lock:
                c = GLOBAL_DB_CONN.cursor()
                c.execute("SELECT failures FROM send_failures WHERE user_id = ?", (user_id,))
                row = c.fetchone()
                if not row:
                    c.execute("INSERT INTO send_failures (user_id, failures, last_failure_at) VALUES (?, ?, ?)",
                              (user_id, 1, now_ts()))
                else:
                    failures = int(row[0] or 0) + 1
                    c.execute("UPDATE send_failures SET failures = ?, last_failure_at = ? WHERE user_id = ?",
                              (failures, now_ts(), user_id))
                GLOBAL_DB_CONN.commit()
            return

        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("SELECT failures FROM send_failures WHERE user_id = ?", (user_id,))
            row = c.fetchone()
            if not row:
                c.execute("INSERT INTO send_failures (user_id, failures, last_failure_at) VALUES (?, ?, ?)",(user_id, 1, now_ts()))
                failures = 1
            else:
                failures = int(row[0] or 0) + 1
                c.execute("UPDATE send_failures SET failures = ?, last_failure_at = ? WHERE user_id = ?",(failures, now_ts(), user_id))
            GLOBAL_DB_CONN.commit()
        if failures >= 6:
            notify_owners(f"⚠️ Repeated send failures for {user_id} ({failures}). Stopping their tasks. 🛑")
            cancel_active_task_for_user(user_id)
    except Exception:
        logger.exception("increment_failure error")

def reset_failures(user_id: int):
    try:
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("DELETE FROM send_failures WHERE user_id = ?", (user_id,))
            GLOBAL_DB_CONN.commit()
    except Exception:
        pass

def send_message(chat_id: int, text: str):
    """
    Send plain text (no parse_mode). Numeric IDs inside the text are sent
    as monospace (code) via the 'entities' parameter so they are copyable.
    Usernames are plain text without leading '@' and not clickable.
    """
    if not TELEGRAM_API:
        logger.error("No TELEGRAM_TOKEN; cannot send message.")
        return None
    acquired = acquire_token(timeout=5.0)
    if not acquired:
        logger.warning("Token acquire timed out; dropping send to %s", chat_id)
        increment_failure(chat_id)
        return None
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    entities = _build_entities_for_text(text)
    if entities:
        payload["entities"] = entities
    try:
        resp = _session.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=REQUESTS_TIMEOUT)
    except Exception:
        logger.exception("Network send error")
        increment_failure(chat_id)
        return None
    data = parse_telegram_json(resp)
    if data and data.get("ok"):
        try:
            mid = data["result"].get("message_id")
            if mid:
                with _db_lock:
                    c = GLOBAL_DB_CONN.cursor()
                    c.execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)",(chat_id, mid, now_ts()))
                    GLOBAL_DB_CONN.commit()
        except Exception:
            logger.exception("record sent message failed")
        reset_failures(chat_id)
        return data["result"]
    else:
        increment_failure(chat_id)
        return None

def broadcast_send_raw(chat_id: int, text: str):
    """
    Send a plain broadcast message; numeric IDs will be marked as code entities.
    """
    if not TELEGRAM_API:
        return False, "no_token"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    entities = _build_entities_for_text(text)
    if entities:
        payload["entities"] = entities
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
                with _db_lock:
                    c = GLOBAL_DB_CONN.cursor()
                    c.execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)",
                              (chat_id, mid, now_ts()))
                    GLOBAL_DB_CONN.commit()
        except Exception:
            pass
        return True, "ok"
    reason = data.get("description") if isinstance(data, dict) else "error"
    logger.info("Broadcast failed to %s: %s", chat_id, reason)
    return False, reason

def broadcast_to_all_allowed(text: str):
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        c.execute("SELECT user_id FROM allowed_users")
        rows = c.fetchall()
    for r in rows:
        tid = r[0]
        if not is_suspended(tid):
            broadcast_send_raw(tid, text)

def cancel_all_tasks():
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        c.execute("UPDATE tasks SET status = ?, finished_at = ? WHERE status IN ('queued','running','paused')", ("cancelled", now_ts()))
        GLOBAL_DB_CONN.commit()
        count = c.rowcount
    return count

def split_text_to_words(text: str) -> List[str]:
    return [w for w in text.strip().split() if w]

def enqueue_task(user_id: int, username: str, text: str):
    words = split_text_to_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
        pending = c.fetchone()[0]
        if pending >= MAX_QUEUE_PER_USER:
            return {"ok": False, "reason": "queue_full", "queue_size": pending}
        try:
            c.execute("INSERT INTO tasks (user_id, username, text, words_json, total_words, status, created_at, sent_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                      (user_id, username, text, json.dumps(words), total, "queued", now_ts(), 0))
            GLOBAL_DB_CONN.commit()
        except Exception:
            logger.exception("enqueue_task db error")
            return {"ok": False, "reason": "db_error"}
    return {"ok": True, "total_words": total, "queue_size": pending + 1}

def get_next_task_for_user(user_id: int):
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        c.execute("SELECT id, words_json, total_words, text FROM tasks WHERE user_id = ? AND status = 'queued' ORDER BY id ASC LIMIT 1", (user_id,))
        r = c.fetchone()
    if not r:
        return None
    return {"id": r[0], "words": json.loads(r[1]) if r[1] else split_text_to_words(r[3]), "total_words": r[2], "text": r[3]}

def set_task_status(task_id: int, status: str):
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        if status == "running":
            c.execute("UPDATE tasks SET status = ?, started_at = ? WHERE id = ?", (status, now_ts(), task_id))
        elif status in ("done", "cancelled"):
            c.execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", (status, now_ts(), task_id))
        else:
            c.execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))
        GLOBAL_DB_CONN.commit()

def cancel_active_task_for_user(user_id: int):
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        c.execute("SELECT id FROM tasks WHERE user_id = ? AND status IN ('queued','running','paused')", (user_id,))
        rows = c.fetchall()
        count = 0
        for r in rows:
            tid = r[0]
            c.execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", ("cancelled", now_ts(), tid))
            count += 1
        GLOBAL_DB_CONN.commit()
    notify_user_worker(user_id)
    return count

def record_split_log(user_id: int, username: str, count: int = 1):
    try:
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            now = now_ts()
            entries = [(user_id, username, 1, now) for _ in range(count)]
            c.executemany("INSERT INTO split_logs (user_id, username, words, created_at) VALUES (?, ?, ?, ?)", entries)
            GLOBAL_DB_CONN.commit()
    except Exception:
        logger.exception("record_split_log error")

def is_allowed(user_id: int) -> bool:
    # Owners are always allowed
    if user_id in OWNER_IDS:
        return True
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,))
        return bool(c.fetchone())

def suspend_user(target_id: int, seconds: int, reason: str = ""):
    until_utc_str = (datetime.utcnow() + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")
    until_wat_str = utc_to_wat_ts(until_utc_str)
    try:
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("INSERT OR REPLACE INTO suspended_users (user_id, suspended_until, reason, added_at) VALUES (?, ?, ?, ?)",
                      (target_id, until_utc_str, reason, now_ts()))
            GLOBAL_DB_CONN.commit()
    except Exception:
        logger.exception("suspend_user db error")
    stopped = cancel_active_task_for_user(target_id)
    try:
        reason_text = f"\nReason: {reason}" if reason else ""
        send_message(target_id, f"⛔ You have been suspended until {until_wat_str} by {OWNER_TAG}.{reason_text}")
    except Exception:
        logger.exception("notify suspended user failed")
    notify_owners(f"🔒 User suspended: {label_for_owner_view(target_id, fetch_display_username(target_id))} suspended_until={until_wat_str} by {OWNER_TAG} reason={reason}")

def unsuspend_user(target_id: int) -> bool:
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (target_id,))
        r = c.fetchone()
        if not r:
            return False
        c.execute("DELETE FROM suspended_users WHERE user_id = ?", (target_id,))
        GLOBAL_DB_CONN.commit()
    try:
        send_message(target_id, f"✅ You have been unsuspended by {OWNER_TAG}.")
    except Exception:
        logger.exception("notify unsuspended failed")
    notify_owners(f"🔓 Manual unsuspend: {label_for_owner_view(target_id, fetch_display_username(target_id))} by {OWNER_TAG}.")
    return True

def list_suspended():
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        c.execute("SELECT user_id, suspended_until, reason, added_at FROM suspended_users ORDER BY suspended_until ASC")
        return c.fetchall()

def is_suspended(user_id: int) -> bool:
    if user_id in OWNER_IDS:
        return False
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,))
        r = c.fetchone()
    if not r:
        return False
    try:
        until = datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S")
        return until > datetime.utcnow()
    except Exception:
        return False

def notify_owners(text: str):
    for oid in OWNER_IDS:
        try:
            send_message(oid, text)
        except Exception:
            logger.exception("notify owner failed for %s", oid)

_user_workers_lock = threading.Lock()
_user_workers: Dict[int, Dict[str, object]] = {}

_active_workers_semaphore = threading.Semaphore(MAX_CONCURRENT_WORKERS)

def notify_user_worker(user_id: int):
    with _user_workers_lock:
        info = _user_workers.get(user_id)
        if info and "wake" in info:
            try:
                info["wake"].set()
            except Exception:
                pass

def start_user_worker_if_needed(user_id: int):
    with _user_workers_lock:
        info = _user_workers.get(user_id)
        if info:
            thr = info.get("thread")
            if thr and thr.is_alive():
                return
        wake = threading.Event()
        stop = threading.Event()
        thr = threading.Thread(target=per_user_worker_loop, args=(user_id, wake, stop), daemon=True)
        _user_workers[user_id] = {"thread": thr, "wake": wake, "stop": stop}
        thr.start()
        logger.info("Started worker for user %s", user_id)

def stop_user_worker(user_id: int, join_timeout: float = 0.5):
    with _user_workers_lock:
        info = _user_workers.get(user_id)
        if not info:
            return
        try:
            info["stop"].set()
            info["wake"].set()
            thr = info.get("thread")
            if thr and thr.is_alive():
                thr.join(join_timeout)
        except Exception:
            logger.exception("Error stopping worker for %s", user_id)
        finally:
            _user_workers.pop(user_id, None)
            logger.info("Stopped worker for user %s", user_id)

def per_user_worker_loop(user_id: int, wake_event: threading.Event, stop_event: threading.Event):
    logger.info("Worker loop starting for user %s", user_id)
    acquired_semaphore = False
    try:
        # Cache uname for stat and log outside the main loop
        uname_for_stat = fetch_display_username(user_id) or str(user_id)

        while not stop_event.is_set():
            if is_suspended(user_id):
                cancel_active_task_for_user(user_id)
                try:
                    send_message(user_id, f"⛔ You have been suspended; stopping your task.")
                except Exception:
                    pass
                while is_suspended(user_id) and not stop_event.is_set():
                    wake_event.wait(timeout=5.0)
                    wake_event.clear()
                continue

            task = get_next_task_for_user(user_id)
            if not task:
                # Wait for a new task/wake up
                wake_event.wait(timeout=1.0)
                wake_event.clear()
                continue

            task_id = task["id"]
            words = task["words"]
            total = int(task["total_words"] or len(words))

            with _db_lock:
                c = GLOBAL_DB_CONN.cursor()
                c.execute("SELECT sent_count, status FROM tasks WHERE id = ?", (task_id,))
                sent_info = c.fetchone()

            if not sent_info or sent_info[1] == "cancelled":
                continue

            # Acquire semaphore to limit the number of concurrent active senders
            # Block until available, but allow stop_event to wake via wake_event
            while not stop_event.is_set():
                acquired = _active_workers_semaphore.acquire(timeout=1.0)
                if acquired:
                    acquired_semaphore = True
                    break
                # If not acquired, check if the task got cancelled
                with _db_lock:
                    c = GLOBAL_DB_CONN.cursor()
                    c.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                    row_check = c.fetchone()
                if not row_check or row_check[0] == "cancelled":
                    break

            # Re-check status after acquiring semaphore
            with _db_lock:
                c = GLOBAL_DB_CONN.cursor()
                c.execute("SELECT sent_count, status FROM tasks WHERE id = ?", (task_id,))
                sent_info = c.fetchone()
            if not sent_info or sent_info[1] == "cancelled":
                if acquired_semaphore:
                    _active_workers_semaphore.release()
                    acquired_semaphore = False
                continue

            sent = int(sent_info[0] or 0)
            set_task_status(task_id, "running")

            # Dynamic interval
            interval = 0.5 if total <= 150 else (0.6 if total <= 300 else 0.7)
            est_seconds = int((total - sent) * interval)
            est_str = str(timedelta(seconds=est_seconds))
            try:
                send_message(user_id, f"🚀 Starting your split now. Words: {total}. Estimated time: {est_str}")
            except Exception:
                pass

            i = sent
            last_send_time = time.monotonic()

            while i < total and not stop_event.is_set():
                with _db_lock:
                    c = GLOBAL_DB_CONN.cursor()
                    c.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                    row = c.fetchone()
                if not row:
                    break
                status = row[0]
                if status == "cancelled" or is_suspended(user_id):
                    break

                if status == "paused":
                    try:
                        send_message(user_id, f"⏸️ Task paused…")
                    except Exception:
                        pass
                    # Pause loop
                    while True:
                        wake_event.wait(timeout=0.7)
                        wake_event.clear()
                        if stop_event.is_set():
                            break
                        with _db_lock:
                            c_check = GLOBAL_DB_CONN.cursor()
                            c_check.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                            row2 = c_check.fetchone()
                        if not row2 or row2[0] == "cancelled" or is_suspended(user_id):
                            break
                        if row2[0] == "running":
                            try:
                                send_message(user_id, "▶️ Resuming your task now.")
                            except Exception:
                                pass
                            last_send_time = time.monotonic()
                            break
                    if status == "cancelled" or is_suspended(user_id) or stop_event.is_set():
                        if is_suspended(user_id):
                            set_task_status(task_id, "cancelled")
                            try: send_message(user_id, "⛔ You have been suspended; stopping your task.")
                            except Exception: pass
                        break

                # --- Send Word and Log ---
                try:
                    send_message(user_id, words[i])
                    record_split_log(user_id, uname_for_stat, 1)
                except Exception:
                    record_split_log(user_id, uname_for_stat, 1)

                i += 1

                # update sent_count
                try:
                    with _db_lock:
                        c = GLOBAL_DB_CONN.cursor()
                        c.execute("UPDATE tasks SET sent_count = ? WHERE id = ?", (i, task_id))
                        GLOBAL_DB_CONN.commit()
                except Exception:
                    logger.exception("Failed to update sent_count for task %s", task_id)

                # Check wake_event quickly
                if wake_event.is_set():
                    wake_event.clear()
                    continue

                now = time.monotonic()
                elapsed = now - last_send_time
                remaining_time = interval - elapsed
                if remaining_time > 0:
                    # Sleep the precise remaining time
                    time.sleep(remaining_time)
                last_send_time = time.monotonic()

                if is_suspended(user_id):
                    break

            # Finalize task
            with _db_lock:
                c = GLOBAL_DB_CONN.cursor()
                c.execute("SELECT status, sent_count FROM tasks WHERE id = ?", (task_id,))
                r = c.fetchone()

            final_status = r[0] if r else "done"
            if final_status not in ("cancelled", "paused"):
                set_task_status(task_id, "done")
                try:
                    send_message(user_id, f"✅ All done!")
                except Exception:
                    pass
            elif final_status == "cancelled":
                try:
                    send_message(user_id, f"🛑 Task stopped.")
                except Exception:
                    pass

            # Release semaphore slot (no matter why loop ended)
            if acquired_semaphore:
                try:
                    _active_workers_semaphore.release()
                except Exception:
                    pass
                acquired_semaphore = False

    except Exception:
        logger.exception("Worker error for user %s", user_id)
    finally:
        if acquired_semaphore:
            try:
                _active_workers_semaphore.release()
            except Exception:
                pass
        with _user_workers_lock:
            _user_workers.pop(user_id, None)
        logger.info("Worker loop exiting for user %s", user_id)

def fetch_display_username(user_id: int):
    # Always show most recent username for user if available, from logs or allowed_users.
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        c.execute("SELECT username FROM split_logs WHERE user_id = ? ORDER BY created_at DESC LIMIT 1", (user_id,))
        r = c.fetchone()
        if r and r[0]:
            return r[0]
        c.execute("SELECT username FROM allowed_users WHERE user_id = ?", (user_id,))
        r2 = c.fetchone()
        if r2 and r2[0]:
            return r2[0]
    return ""

def compute_last_hour_stats():
    cutoff = datetime.utcnow() - timedelta(hours=1)
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        c.execute("""
            SELECT user_id, username, COUNT(*) as s
            FROM split_logs
            WHERE created_at >= ?
            GROUP BY user_id, username
            ORDER BY s DESC
        """, (cutoff.strftime("%Y-%m-%d %H:%M:%S"),))
        rows = c.fetchall()
    stat_map = {}
    for uid, uname, s in rows:
        stat_map[uid] = {"uname": uname, "words": stat_map.get(uid,{}).get("words",0)+int(s)}
    return [(k, v["uname"], v["words"]) for k, v in stat_map.items()]

def compute_last_12h_stats(user_id: int):
    cutoff = datetime.utcnow() - timedelta(hours=12)
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        c.execute("""
            SELECT COUNT(*) FROM split_logs WHERE user_id = ? AND created_at >= ?
        """, (user_id, cutoff.strftime("%Y-%m-%d %H:%M:%S")))
        r = c.fetchone()
        return int(r[0] or 0)

def send_hourly_owner_stats():
    rows = compute_last_hour_stats()
    if not rows:
        msg = "📊 Hourly Report: no splits in the last hour."
        for oid in OWNER_IDS:
            try:
                send_message(oid, msg)
            except Exception:
                pass
        return
    lines = []
    for uid, uname, w in rows:
        uname_for_stat = at_username(uname) if uname else fetch_display_username(uid)
        lines.append(f"{uid} ({uname_for_stat}) - {w} words sent")
    body = "📊 Report - last 1h:\n" + "\n".join(lines)
    for oid in OWNER_IDS:
        try:
            send_message(oid, body)
        except Exception:
            pass

def check_and_lift():
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
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

def prune_old_logs():
    """
    Periodic job to prune split_logs and sent_messages older than LOG_RETENTION_DAYS.
    Keeps DB small for long-running unlimited usage.
    """
    try:
        cutoff = (datetime.utcnow() - timedelta(days=LOG_RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("DELETE FROM split_logs WHERE created_at < ?", (cutoff,))
            deleted1 = c.rowcount
            c.execute("DELETE FROM sent_messages WHERE sent_at < ?", (cutoff,))
            deleted2 = c.rowcount
            GLOBAL_DB_CONN.commit()
        if deleted1 or deleted2:
            logger.info("Pruned logs: split_logs=%s sent_messages=%s", deleted1, deleted2)
    except Exception:
        logger.exception("prune_old_logs error")

scheduler = BackgroundScheduler()
scheduler.add_job(send_hourly_owner_stats, "interval", hours=1, next_run_time=datetime.utcnow() + timedelta(seconds=10), timezone='UTC')
scheduler.add_job(check_and_lift, "interval", minutes=1, next_run_time=datetime.utcnow() + timedelta(seconds=15), timezone='UTC')
# Daily cleanup at a gentle time interval
scheduler.add_job(prune_old_logs, "interval", hours=24, next_run_time=datetime.utcnow() + timedelta(seconds=30), timezone='UTC')
scheduler.start()

# Graceful shutdown to stop scheduler and workers nicely
def _graceful_shutdown(signum, frame):
    logger.info("Graceful shutdown signal received (%s). Stopping scheduler and workers...", signum)
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    # Stop user workers
    with _user_workers_lock:
        keys = list(_user_workers.keys())
    for k in keys:
        stop_user_worker(k, join_timeout=1.0)
    try:
        if GLOBAL_DB_CONN:
            GLOBAL_DB_CONN.close()
    except Exception:
        pass
    logger.info("Shutdown completed. Exiting.")
    # If running under Flask dev server, os._exit to ensure process ends
    try:
        import os
        os._exit(0)
    except Exception:
        pass

signal.signal(signal.SIGTERM, _graceful_shutdown)
signal.signal(signal.SIGINT, _graceful_shutdown)

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

            # Only update username for existing/allowed users.
            try:
                with _db_lock:
                    c = GLOBAL_DB_CONN.cursor()
                    c.execute("UPDATE allowed_users SET username = ? WHERE user_id = ?", (username or "", uid))
                    GLOBAL_DB_CONN.commit()
            except Exception:
                logger.exception("webhook: update allowed_users username failed")

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
    return "WordSplitter running.", 200

@app.route("/health", methods=["GET", "HEAD"])
def health():
    return jsonify({"ok": True, "ts": now_ts()}), 200

def get_user_task_counts(user_id: int):
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status IN ('running','paused')", (user_id,))
        active = int(c.fetchone()[0] or 0)
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
        queued = int(c.fetchone()[0] or 0)
    return active, queued

def handle_command(user_id: int, username: str, command: str, args: str):
    def is_owner(u): return u in OWNER_IDS

    # Ensure /start and /about are always functional for everyone
    if command == "/start":
        who = label_for_self(user_id, username) or "there"
        msg = (
            f"👋 Hi {who}!\n\n"
            "I split your text into individual word messages. ✂️📤\n\n"
            f"{OWNER_TAG} commands:\n"
            " /adduser /listusers /listsuspended /botinfo /broadcast /suspend /unsuspend\n\n"
            "User commands:\n"
            " /start /example /pause /resume /status /stop /stats /about\n\n"
            "Just send any text and I'll split it for you. 🚀"
        )
        send_message(user_id, msg)
        return jsonify({"ok": True})

    if command == "/about":
        msg = (
            "ℹ️ About:\n"
            "I split texts into single words. ✂️\n\n"
            "Features:\n"
            "queueing, pause/resume,\n"
            "hourly owner stats, rate-limited sending. ⚖️"
        )
        send_message(user_id, msg)
        return jsonify({"ok": True})

    if user_id not in OWNER_IDS and not is_allowed(user_id):
        send_message(user_id, f"🚫 Sorry, you are not allowed. {OWNER_TAG} notified.\nYour ID: {user_id}")
        notify_owners(f"🚨 Unallowed access attempt by {at_username(username) if username else user_id} (ID: {user_id}).")
        return jsonify({"ok": True})

    if command == "/example":
        sample = "\n".join([
            "996770061141", "996770064514", "996770071665", "996770073284",
            "996770075145", "996770075627", "996770075973", "996770076350",
            "996770076869", "996770077101"
        ])
        res = enqueue_task(user_id, username, sample)
        if not res["ok"]:
            send_message(user_id, "❗ Could not queue demo. Try later.")
            return jsonify({"ok": True})
        start_user_worker_if_needed(user_id)
        notify_user_worker(user_id)
        active, queued = get_user_task_counts(user_id)
        if active:
            send_message(user_id, f"✅ Task added. Words: {res['total_words']}.\nQueue position: {queued}")
        else:
            send_message(user_id, f"✅ Task added. Words: {res['total_words']}.")
        return jsonify({"ok": True})

    if command == "/pause":
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'running' ORDER BY started_at ASC LIMIT 1", (user_id,))
            rows = c.fetchone()
        if not rows:
            send_message(user_id, "ℹ️ No active task to pause.")
            return jsonify({"ok": True})
        set_task_status(rows[0], "paused")
        notify_user_worker(user_id)
        send_message(user_id, "⏸️ Paused. Use /resume to continue.")
        return jsonify({"ok": True})

    if command == "/resume":
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'paused' ORDER BY started_at ASC LIMIT 1", (user_id,))
            rows = c.fetchone()
        if not rows:
            send_message(user_id, "ℹ️ No paused task to resume.")
            return jsonify({"ok": True})
        set_task_status(rows[0], "running")
        notify_user_worker(user_id)
        send_message(user_id, "▶️ Resuming your task now.")
        return jsonify({"ok": True})

    if command == "/status":
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("SELECT id, status, total_words, sent_count FROM tasks WHERE user_id = ? AND status IN ('running','paused') ORDER BY started_at ASC LIMIT 1", (user_id,))
            active = c.fetchone()
            c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
            queued = c.fetchone()[0]
        if active:
            aid, status, total, sent = active
            remaining = int(total or 0) - int(sent or 0)
            send_message(user_id, f"ℹ️ Status: {status}\nRemaining words: {remaining}\nQueue size: {queued}")
        elif queued > 0:
            send_message(user_id, f"⏳ Waiting. Queue size: {queued}")
        else:
            send_message(user_id, "✅ You have no active or queued tasks.")
        return jsonify({"ok": True})

    if command == "/stop":
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
            queued = c.fetchone()[0]
        stopped = cancel_active_task_for_user(user_id)
        stop_user_worker(user_id)
        if stopped > 0 or queued > 0:
            send_message(user_id, "🛑 Active task stopped. Your queued tasks were cleared too.")
        else:
            send_message(user_id, "ℹ️ You had no active or queued tasks.")
        return jsonify({"ok": True})

    if command == "/stats":
        words = compute_last_12h_stats(user_id)
        send_message(user_id, f"📊 Your last 12 hours: {words} words split")
        return jsonify({"ok": True})

    if command == "/adduser":
        if not is_owner(user_id):
            send_message(user_id, f"🔒 {OWNER_TAG} only.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /adduser <user_id> [username]")
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
            with _db_lock:
                c = GLOBAL_DB_CONN.cursor()
                c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (tid,))
                if c.fetchone():
                    already.append(tid)
                    continue
                c.execute("INSERT INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)", (tid, "", now_ts()))
                GLOBAL_DB_CONN.commit()
            added.append(tid)
            try:
                send_message(tid, f"✅ You have been added. Send any text to start.")
            except Exception:
                pass
        parts_msgs = []
        if added: parts_msgs.append("Added: " + ", ".join(str(x) for x in added))
        if already: parts_msgs.append("Already present: " + ", ".join(str(x) for x in already))
        if invalid: parts_msgs.append("Invalid: " + ", ".join(invalid))
        send_message(user_id, "✅ " + ("; ".join(parts_msgs) if parts_msgs else "No changes"))
        return jsonify({"ok": True})

    if command == "/listusers":
        if not is_owner(user_id):
            send_message(user_id, f"🔒 {OWNER_TAG} only.")
            return jsonify({"ok": True})
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("SELECT user_id, username, added_at FROM allowed_users ORDER BY added_at DESC")
            rows = c.fetchall()
        lines = []
        for r in rows:
            uid, uname, added_at_utc = r
            uname_s = f"({at_username(uname)})" if uname else "(no username)"
            added_at_wat = utc_to_wat_ts(added_at_utc)
            lines.append(f"{uid} {uname_s} added={added_at_wat}")
        send_message(user_id, "👥 Allowed users:\n" + ("\n".join(lines) if lines else "(none)"))
        return jsonify({"ok": True})

    if command == "/listsuspended":
        if not is_owner(user_id):
            send_message(user_id, f"🔒 {OWNER_TAG} only.")
            return jsonify({"ok": True})
        for row in list_suspended()[:]:
            uid, until_utc, reason, added_at_utc = row
            until_dt = datetime.strptime(until_utc, "%Y-%m-%d %H:%M:%S")
            if until_dt <= datetime.utcnow():
                unsuspend_user(uid)
        rows = list_suspended()
        if not rows:
            send_message(user_id, "✅ No suspended users.")
            return jsonify({"ok": True})
        lines = []
        for r in rows:
            uid, until_utc, reason, added_at_utc = r
            until_wat = utc_to_wat_ts(until_utc)
            added_wat = utc_to_wat_ts(added_at_utc)
            uname = fetch_display_username(uid)
            uname_s = f"({at_username(uname)})" if uname else ""
            lines.append(f"{uid} {uname_s} suspended_until={until_wat} by={OWNER_TAG} reason={reason}")
        send_message(user_id, "🚫 Suspended users:\n" + "\n".join(lines))
        return jsonify({"ok": True})

    if command == "/botinfo":
        if not is_owner(user_id):
            send_message(user_id, f"🔒 {OWNER_TAG} only.")
            return jsonify({"ok": True})
        active_rows, queued_tasks = [], 0
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("SELECT user_id, username, SUM(total_words - IFNULL(sent_count,0)) as remaining, COUNT(*) as active_count FROM tasks WHERE status IN ('running','paused') GROUP BY user_id")
            active_rows = c.fetchall()
            c.execute("SELECT COUNT(*) FROM tasks WHERE status = 'queued'")
            queued_tasks = c.fetchone()[0]
        queued_counts = {}
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("SELECT user_id, COUNT(*) FROM tasks WHERE status = 'queued' GROUP BY user_id")
            for row in c.fetchall():
                queued_counts[row[0]] = row[1]
        stats_rows = compute_last_hour_stats()
        lines_active = []
        for r in active_rows:
            uid, uname, rem, ac = r
            if not uname:
                uname = fetch_display_username(uid)
            name = f" ({at_username(uname)})" if uname else ""
            queued_for_user = queued_counts.get(uid, 0)
            lines_active.append(f"{uid}{name} - {int(rem)} remaining - {int(ac)} active - {queued_for_user} queued")
        lines_stats = []
        for uid, uname, s in stats_rows:
            uname_final = at_username(uname) if uname else fetch_display_username(uid)
            lines_stats.append(f"{uid} ({uname_final}) - {int(s)} words sent")
        total_allowed = 0
        total_suspended = 0
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("SELECT COUNT(*) FROM allowed_users")
            total_allowed = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM suspended_users")
            total_suspended = c.fetchone()[0]
        body = (
            f"🤖 Bot status: Online\n"
            f"👥 Allowed users: {total_allowed}\n"
            f"🚫 Suspended users: {total_suspended}\n"
            f"⚙️ Active tasks: {len(active_rows)}\n"
            f"📨 Queued tasks: {queued_tasks}\n\n"
            "Users with active tasks:\n" + ("\n".join(lines_active) if lines_active else "(none)") + "\n\n"
            "User stats (last 1h):\n" + ("\n".join(lines_stats) if lines_stats else "(none)")
        )
        send_message(user_id, body)
        return jsonify({"ok": True})

    if command == "/broadcast":
        if not is_owner(user_id):
            send_message(user_id, f"🔒 {OWNER_TAG} only.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /broadcast <message>")
            return jsonify({"ok": True})
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("SELECT user_id FROM allowed_users")
            rows = c.fetchall()
        succeeded, failed = [], []
        header = f"📣 Broadcast from {OWNER_TAG}:\n\n{args}"
        for r in rows:
            tid = r[0]
            ok, reason = broadcast_send_raw(tid, header)
            if ok:
                succeeded.append(tid)
            else:
                failed.append((tid, reason))
        summary = f"📨 Broadcast done. Success: {len(succeeded)}, Failed: {len(failed)}"
        send_message(user_id, summary)
        if failed:
            notify_owners("⚠️ Broadcast failures: " + ", ".join(f"{x[0]}({x[1]})" for x in failed))
        return jsonify({"ok": True})

    if command == "/suspend":
        if not is_owner(user_id):
            send_message(user_id, f"🔒 {OWNER_TAG} only.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /suspend <telegram_user_id> <duration> [reason]\nExample: /suspend 8282747479 30s Too many requests")
            return jsonify({"ok": True})
        parts = args.split()
        try:
            target = int(parts[0])
        except Exception:
            send_message(user_id, "Invalid user id.")
            return jsonify({"ok": True})
        if len(parts) < 2:
            send_message(user_id, "Missing duration.")
            return jsonify({"ok": True})
        dur = parts[1]
        reason = " ".join(parts[2:]) if len(parts) > 2 else ""
        m = re.match(r"^(\d+)(s|m|h|d)?$", dur)
        if not m:
            send_message(user_id, "Invalid duration format. Examples: 30s 10m 2h 1d")
            return jsonify({"ok": True})
        val, unit = int(m.group(1)), (m.group(2) or "s")
        mul = {"s":1, "m":60, "h":3600, "d":86400}.get(unit,1)
        seconds = val * mul
        suspend_user(target, seconds, reason)
        reason_part = f" Reason: {reason}" if reason else ""
        send_message(user_id, f"🔒 User {label_for_owner_view(target, fetch_display_username(target))} suspended until {utc_to_wat_ts((datetime.utcnow() + timedelta(seconds=seconds)).strftime('%Y-%m-%d %H:%M:%S'))}{reason_part}")
        return jsonify({"ok": True})

    if command == "/unsuspend":
        if not is_owner(user_id):
            send_message(user_id, f"🔒 {OWNER_TAG} only.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /unsuspend <telegram_user_id>")
            return jsonify({"ok": True})
        try:
            target = int(args.split()[0])
        except Exception:
            send_message(user_id, "Invalid user id.")
            return jsonify({"ok": True})
        ok = unsuspend_user(target)
        if ok:
            send_message(user_id, f"✅ User {label_for_owner_view(target, fetch_display_username(target))} unsuspended.")
        else:
            send_message(user_id, f"ℹ️ User {target} is not suspended.")
        return jsonify({"ok": True})

    send_message(user_id, "❓ Unknown command.")
    return jsonify({"ok": True})

def handle_user_text(user_id: int, username: str, text: str):
    # Owners are always allowed; regular users must be in allowed_users
    if user_id not in OWNER_IDS and not is_allowed(user_id):
        send_message(user_id, f"🚫 Sorry, you are not allowed. {OWNER_TAG} notified.\nYour ID: {user_id}")
        notify_owners(f"🚨 Unallowed access attempt by {at_username(username) if username else user_id} (ID: {user_id}).")
        return jsonify({"ok": True})
    if is_suspended(user_id):
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,))
            r = c.fetchone()
            until_utc = r[0] if r else "unknown"
            until_wat = utc_to_wat_ts(until_utc)
        send_message(user_id, f"⛔ You have been suspended until {until_wat} by {OWNER_TAG}.")
        return jsonify({"ok": True})
    res = enqueue_task(user_id, username, text)
    if not res["ok"]:
        if res["reason"] == "empty":
            send_message(user_id, "⚠️ Empty text. Nothing to split.")
            return jsonify({"ok": True})
        if res["reason"] == "queue_full":
            send_message(user_id, f"⏳ Your queue is full ({res['queue_size']}). Use /stop or wait.")
            return jsonify({"ok": True})
        send_message(user_id, "❗ Could not queue task. Try later.")
        return jsonify({"ok": True})
    start_user_worker_if_needed(user_id)
    notify_user_worker(user_id)
    active, queued = get_user_task_counts(user_id)
    if active:
        send_message(user_id, f"✅ Task added. Words: {res['total_words']}.\nQueue position: {queued}")
    else:
        send_message(user_id, f"✅ Task added. Words: {res['total_words']}.")
    return jsonify({"ok": True})

def set_webhook():
    if not TELEGRAM_API or not WEBHOOK_URL:
        logger.info("Webhook not configured.")
        return
    try:
        _session.post(f"{TELEGRAM_API}/setWebhook", json={"url": WEBHOOK_URL}, timeout=REQUESTS_TIMEOUT)
    except Exception:
        logger.exception("set_webhook failed")

if __name__ == "__main__":
    try:
        set_webhook()
    except Exception:
        pass
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
