#!/usr/bin/env python3

import os
import time
import json
import sqlite3
import threading
import logging
import re
import signal
import math
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
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
FAILURE_NOTIFY_THRESHOLD = int(os.environ.get("FAILURE_NOTIFY_THRESHOLD", "6"))
PERMANENT_SUSPEND_DAYS = int(os.environ.get("PERMANENT_SUSPEND_DAYS", "365"))

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

def at_username(u: str) -> str:
    if not u:
        return ""
    return u.lstrip("@")

def label_for_self(viewer_id: int, username: str) -> str:
    if username:
        if viewer_id in OWNER_IDS:
            return f"{at_username(username)} (ID: {viewer_id})"
        return f"{at_username(username)}"
    return f"(ID: {viewer_id})" if viewer_id in OWNER_IDS else ""

def label_for_owner_view(target_id: int, target_username: str) -> str:
    if target_username:
        return f"{at_username(target_username)} (ID: {target_id})"
    return str(target_id)

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
    Also creates tables and runs lightweight schema migration for added columns.
    """
    global DB_PATH, GLOBAL_DB_CONN
    parent = os.path.dirname(os.path.abspath(DB_PATH))
    if parent:
        _ensure_db_parent(parent)

    def _create_schema(conn):
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
        # New schema: send_failures with notified and last error details
        c.execute("""
        CREATE TABLE IF NOT EXISTS send_failures (
            user_id INTEGER PRIMARY KEY,
            failures INTEGER,
            last_failure_at TEXT,
            notified INTEGER DEFAULT 0,
            last_error_code INTEGER,
            last_error_desc TEXT
        )""")
        conn.commit()

    try:
        conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-2000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=30000;")
        _create_schema(conn)
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
            _create_schema(conn)
            GLOBAL_DB_CONN = conn
            logger.info("In-memory DB initialized")
        except Exception:
            GLOBAL_DB_CONN = None
            logger.exception("Failed to initialize in-memory DB; DB operations may fail")

def ensure_send_failures_columns():
    """
    Ensure migration: if older DB lacks columns (notified, last_error_code, last_error_desc),
    try to add them via ALTER TABLE (best effort).
    """
    try:
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("PRAGMA table_info(send_failures)")
            cols = [r[1] for r in c.fetchall()]
            to_add = []
            if "notified" not in cols:
                to_add.append("ALTER TABLE send_failures ADD COLUMN notified INTEGER DEFAULT 0")
            if "last_error_code" not in cols:
                to_add.append("ALTER TABLE send_failures ADD COLUMN last_error_code INTEGER")
            if "last_error_desc" not in cols:
                to_add.append("ALTER TABLE send_failures ADD COLUMN last_error_desc TEXT")
            for stmt in to_add:
                try:
                    c.execute(stmt)
                except Exception:
                    # Ignore - maybe older sqlite can't alter; it's best-effort
                    logger.debug("Migration statement failed: %s", stmt)
            GLOBAL_DB_CONN.commit()
    except Exception:
        logger.exception("ensure_send_failures_columns failed")

# Initialize DB and run migration check
init_db()
if GLOBAL_DB_CONN:
    ensure_send_failures_columns()

# Ensure owners auto-added as allowed
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

# Ensure provided ALLOWED_USERS auto-added
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

# Token bucket using Condition to avoid busy-wait loops
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
    if not s:
        return 0
    return len(s.encode("utf-16-le")) // 2

def _build_entities_for_text(text: str):
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

# Failure handling helpers

def is_permanent_telegram_error(code: int, description: str = "") -> bool:
    """
    Consider 400 and 403 errors as permanent/unrecoverable for our bot (e.g., chat not found or bot blocked).
    Some 400 codes might be transient in rare cases, but in practice 400/403 for sendMessage indicates permanent.
    """
    try:
        if code in (400, 403):
            return True
    except Exception:
        pass
    # Additional heuristic checks on description
    if description:
        desc = description.lower()
        if "bot was blocked" in desc or "chat not found" in desc or "user is deactivated" in desc or "forbidden" in desc:
            return True
    return False

def mark_user_permanently_unreachable(user_id: int, error_code: int = None, description: str = ""):
    """
    Record a permanent failure and suspend the user for a long duration so we stop retrying.
    Notify owners once. Owners are never suspended.
    """
    try:
        if user_id in OWNER_IDS:
            # For owners, just log but don't suspend
            with _db_lock:
                c = GLOBAL_DB_CONN.cursor()
                c.execute("INSERT OR REPLACE INTO send_failures (user_id, failures, last_failure_at, notified, last_error_code, last_error_desc) VALUES (?, ?, ?, ?, ?, ?)",
                          (user_id, FAILURE_NOTIFY_THRESHOLD, now_ts(), 1, error_code, description))
                GLOBAL_DB_CONN.commit()
            notify_owners(f"⚠️ Repeated send failures for owner {user_id}. Please investigate. Error: {error_code} {description}")
            return

        # Save into send_failures and set notified flag
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("INSERT OR REPLACE INTO send_failures (user_id, failures, last_failure_at, notified, last_error_code, last_error_desc) VALUES (?, ?, ?, ?, ?, ?)",
                      (user_id, 999, now_ts(), 1, error_code, description))
            GLOBAL_DB_CONN.commit()

        # Cancel tasks and suspend the user for PERMANENT_SUSPEND_DAYS
        cancel_active_task_for_user(user_id)
        suspend_user(user_id, PERMANENT_SUSPEND_DAYS * 24 * 3600, f"Permanent send failure: {error_code} {description}")

        # Notify owners once
        notify_owners(f"⚠️ Repeated send failures for {user_id} ({error_code}). Stopping their tasks. 🛑 Error: {description}")
    except Exception:
        logger.exception("mark_user_permanently_unreachable failed for %s", user_id)

def record_failure(user_id: int, inc: int = 1, error_code: int = None, description: str = "", is_permanent: bool = False):
    """
    Increment or set failure data in DB; if threshold reached, notify owners once and optionally
    escalate to permanent handling.
    """
    try:
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("SELECT failures, notified FROM send_failures WHERE user_id = ?", (user_id,))
            row = c.fetchone()
            if not row:
                failures = inc
                notified = 0
                c.execute("INSERT INTO send_failures (user_id, failures, last_failure_at, notified, last_error_code, last_error_desc) VALUES (?, ?, ?, ?, ?, ?)",
                          (user_id, failures, now_ts(), 0, error_code, description))
            else:
                failures = int(row[0] or 0) + inc
                notified = int(row[1] or 0)
                c.execute("UPDATE send_failures SET failures = ?, last_failure_at = ?, last_error_code = ?, last_error_desc = ? WHERE user_id = ?",
                          (failures, now_ts(), error_code, description, user_id))
            GLOBAL_DB_CONN.commit()

        # If it's marked permanent by caller OR heuristics determine it's permanent, escalate now:
        if is_permanent or is_permanent_telegram_error(error_code or 0, description):
            # Mark permanent and suspend/cancel
            mark_user_permanently_unreachable(user_id, error_code, description)
            return

        # Notify owners only once when threshold reached
        if failures >= FAILURE_NOTIFY_THRESHOLD and notified == 0:
            try:
                with _db_lock:
                    c = GLOBAL_DB_CONN.cursor()
                    c.execute("UPDATE send_failures SET notified = 1 WHERE user_id = ?", (user_id,))
                    GLOBAL_DB_CONN.commit()
            except Exception:
                logger.exception("Failed to set notified flag for %s", user_id)
            notify_owners(f"⚠️ Repeated send failures for {user_id} ({failures}). Stopping their tasks. 🛑")
            # Take a cautious step: cancel their active tasks to reduce wasted sends (but don't suspend permanently)
            cancel_active_task_for_user(user_id)
    except Exception:
        logger.exception("record_failure error for %s", user_id)

def reset_failures(user_id: int):
    try:
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("DELETE FROM send_failures WHERE user_id = ?", (user_id,))
            GLOBAL_DB_CONN.commit()
    except Exception:
        logger.exception("reset_failures failed for %s", user_id)

def send_message(chat_id: int, text: str, reply_markup: Optional[Dict] = None):
    """
    Send plain text (no parse_mode). Numeric IDs inside the text are sent
    as monospace (code) via the 'entities' parameter so they are copyable.
    Includes retry/backoff for transient errors and 429 handling.
    """
    if not TELEGRAM_API:
        logger.error("No TELEGRAM_TOKEN; cannot send message.")
        return None

    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    entities = _build_entities_for_text(text)
    if entities:
        payload["entities"] = entities
    if reply_markup:
        payload["reply_markup"] = reply_markup

    # Acquire token before attempting send
    if not acquire_token(timeout=5.0):
        logger.warning("Token acquire timed out; dropping send to %s", chat_id)
        record_failure(chat_id, inc=1, description="token_acquire_timeout")
        return None

    # We'll attempt a small number of retries for transient/network errors
    max_attempts = 3
    attempt = 0
    backoff_base = 0.5
    while attempt < max_attempts:
        attempt += 1
        try:
            resp = _session.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=REQUESTS_TIMEOUT)
        except requests.exceptions.RequestException as e:
            logger.warning("Network send error to %s (attempt %s): %s", chat_id, attempt, e)
            # transient network error: retry with backoff
            if attempt >= max_attempts:
                record_failure(chat_id, inc=1, description=str(e))
                return None
            time.sleep(backoff_base * (2 ** (attempt - 1)))
            continue

        data = parse_telegram_json(resp)
        if not isinstance(data, dict):
            # unexpected response; treat as transient
            logger.warning("Unexpected non-json response for sendMessage to %s", chat_id)
            if attempt >= max_attempts:
                record_failure(chat_id, inc=1, description="non_json_response")
                return None
            time.sleep(backoff_base * (2 ** (attempt - 1)))
            continue

        if data.get("ok"):
            # Success: record message and reset any existing failure state for this user
            try:
                mid = data["result"].get("message_id")
                if mid:
                    with _db_lock:
                        c = GLOBAL_DB_CONN.cursor()
                        c.execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)",(chat_id, mid, now_ts()))
                        GLOBAL_DB_CONN.commit()
            except Exception:
                logger.exception("record sent message failed")
            # Reset failures since send succeeded
            reset_failures(chat_id)
            return data["result"]

        # Not ok -> inspect error details
        error_code = data.get("error_code")
        description = data.get("description", "")
        params = data.get("parameters") or {}
        # If rate limited and retry_after present, sleep then retry
        if error_code == 429:
            retry_after = params.get("retry_after")
            if retry_after is None:
                # If no retry_after, be conservative and wait a short time
                retry_after = 1
            try:
                retry_after = int(retry_after)
            except Exception:
                retry_after = 1
            logger.info("Rate limited for %s: retry_after=%s", chat_id, retry_after)
            # Wait and retry (counts as transient)
            time.sleep(max(0.5, retry_after))
            # on next iteration we will try again
            if attempt >= max_attempts:
                record_failure(chat_id, inc=1, error_code=error_code, description=description)
                return None
            continue

        # Permanent errors (400/403 etc.) - escalate immediately
        if is_permanent_telegram_error(error_code or 0, description):
            logger.info("Permanent error for %s: %s %s", chat_id, error_code, description)
            record_failure(chat_id, inc=1, error_code=error_code, description=description, is_permanent=True)
            return None

        # Other errors - treat as transient, retry a few times
        logger.warning("Transient/send error for %s: %s %s", chat_id, error_code, description)
        if attempt >= max_attempts:
            record_failure(chat_id, inc=1, error_code=error_code, description=description)
            return None
        time.sleep(backoff_base * (2 ** (attempt - 1)))

def broadcast_send_raw(chat_id: int, text: str):
    if not TELEGRAM_API:
        return False, "no_token"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    entities = _build_entities_for_text(text)
    if entities:
        payload["entities"] = entities
    
    # Acquire token for broadcast
    if not acquire_token(timeout=5.0):
        logger.warning("Token acquire timed out for broadcast to %s", chat_id)
        return False, "token_acquire_timeout"
    
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
    error_code = data.get("error_code") if isinstance(data, dict) else None
    logger.info("Broadcast failed to %s: %s", chat_id, reason)
    # Record a failure for non-ok broadcast attempts
    try:
        record_failure(chat_id, inc=1, error_code=error_code, description=reason)
    except Exception:
        pass
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

            # Acquire concurrency semaphore
            while not stop_event.is_set():
                acquired = _active_workers_semaphore.acquire(timeout=1.0)
                if acquired:
                    acquired_semaphore = True
                    break
                with _db_lock:
                    c = GLOBAL_DB_CONN.cursor()
                    c.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                    row_check = c.fetchone()
                if not row_check or row_check[0] == "cancelled":
                    break

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

                try:
                    send_message(user_id, words[i])
                    record_split_log(user_id, uname_for_stat, 1)
                except Exception:
                    record_split_log(user_id, uname_for_stat, 1)

                i += 1

                try:
                    with _db_lock:
                        c = GLOBAL_DB_CONN.cursor()
                        c.execute("UPDATE tasks SET sent_count = ? WHERE id = ?", (i, task_id))
                        GLOBAL_DB_CONN.commit()
                except Exception:
                    logger.exception("Failed to update sent_count for task %s", task_id)

                if wake_event.is_set():
                    wake_event.clear()
                    continue

                now = time.monotonic()
                elapsed = now - last_send_time
                remaining_time = interval - elapsed
                if remaining_time > 0:
                    time.sleep(remaining_time)
                last_send_time = time.monotonic()

                if is_suspended(user_id):
                    break

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
scheduler.add_job(prune_old_logs, "interval", hours=24, next_run_time=datetime.utcnow() + timedelta(seconds=30), timezone='UTC')
scheduler.start()

def _graceful_shutdown(signum, frame):
    logger.info("Graceful shutdown signal received (%s). Stopping scheduler and workers...", signum)
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
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
    try:
        import os
        os._exit(0)
    except Exception:
        pass

signal.signal(signal.SIGTERM, _graceful_shutdown)
signal.signal(signal.SIGINT, _graceful_shutdown)

# Owner operations state management - now blocks task processing
_owner_ops_lock = threading.Lock()
_owner_ops_state: Dict[int, Dict] = {}

def get_owner_state(user_id: int) -> Optional[Dict]:
    with _owner_ops_lock:
        return _owner_ops_state.get(user_id)

def set_owner_state(user_id: int, state: Dict):
    with _owner_ops_lock:
        _owner_ops_state[user_id] = state

def clear_owner_state(user_id: int):
    with _owner_ops_lock:
        _owner_ops_state.pop(user_id, None)

def is_owner_in_operation(user_id: int) -> bool:
    """Check if owner is currently in an operation that requires input"""
    with _owner_ops_lock:
        return user_id in _owner_ops_state

def get_user_tasks_preview(user_id: int, hours: int, page: int = 0) -> Tuple[List[Dict], int, int]:
    """Get tasks preview for a user within specified hours, paginated"""
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        c.execute("""
            SELECT id, text, created_at, total_words, sent_count
            FROM tasks 
            WHERE user_id = ? AND created_at >= ?
            ORDER BY created_at DESC
        """, (user_id, cutoff.strftime("%Y-%m-%d %H:%M:%S")))
        rows = c.fetchall()
    
    tasks = []
    for r in rows:
        task_id, text, created_at, total_words, sent_count = r
        words = split_text_to_words(text)
        preview = " ".join(words[:2]) if len(words) >= 2 else words[0] if words else "(empty)"
        tasks.append({
            "id": task_id,
            "preview": preview,
            "created_at": utc_to_wat_ts(created_at),
            "total_words": total_words,
            "sent_count": sent_count
        })
    
    total_tasks = len(tasks)
    page_size = 20
    start_idx = page * page_size
    end_idx = start_idx + page_size
    paginated_tasks = tasks[start_idx:end_idx]
    
    total_pages = (total_tasks + page_size - 1) // page_size
    
    return paginated_tasks, total_tasks, total_pages

def send_ownersets_menu(owner_id: int):
    """Send the main owner menu with inline buttons"""
    menu_text = f"👑 Owner Menu {OWNER_TAG}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nSelect an operation:"
    
    # Updated layout as requested:
    # - 4 rows only
    # - Fourth row has only "Check User Preview"
    # - No "Close Menu" button
    keyboard = [
        [{"text": "📊 Bot Info", "callback_data": "owner_botinfo"}, {"text": "👥 List Users", "callback_data": "owner_listusers"}],
        [{"text": "🚫 List Suspended", "callback_data": "owner_listsuspended"}, {"text": "➕ Add User", "callback_data": "owner_adduser"}],
        [{"text": "⏸️ Suspend User", "callback_data": "owner_suspend"}, {"text": "▶️ Unsuspend User", "callback_data": "owner_unsuspend"}],
        [{"text": "🔍 Check User Preview", "callback_data": "owner_checkpreview"}]
    ]
    
    reply_markup = {"inline_keyboard": keyboard}
    send_message(owner_id, menu_text, reply_markup)

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = request.get_json(force=True)
    except Exception:
        return jsonify({"ok": False}), 400
    
    try:
        # Handle callback queries (inline button presses)
        if "callback_query" in update:
            callback = update["callback_query"]
            user = callback.get("from", {})
            uid = user.get("id")
            data = callback.get("data", "")
            
            # Check if user is an owner
            if uid not in OWNER_IDS:
                try:
                    _session.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
                        "callback_query_id": callback.get("id"),
                        "text": "⛔ Owner only."
                    }, timeout=2)
                except Exception:
                    pass
                return jsonify({"ok": True})
            
            # Handle callback data
            if data == "owner_close":
                try:
                    _session.post(f"{TELEGRAM_API}/deleteMessage", json={
                        "chat_id": callback["message"]["chat"]["id"],
                        "message_id": callback["message"]["message_id"]
                    }, timeout=2)
                except Exception:
                    pass
                try:
                    _session.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
                        "callback_query_id": callback.get("id"),
                        "text": "✅ Menu closed."
                    }, timeout=2)
                except Exception:
                    pass
                clear_owner_state(uid)
                return jsonify({"ok": True})
            
            elif data == "owner_botinfo":
                # Get bot info and edit the message to show it, keeping the menu buttons
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
                    uid2, uname, rem, ac = r
                    if not uname:
                        uname = fetch_display_username(uid2)
                    name = f" ({at_username(uname)})" if uname else ""
                    queued_for_user = queued_counts.get(uid2, 0)
                    lines_active.append(f"{uid2}{name} - {int(rem)} remaining - {int(ac)} active - {queued_for_user} queued")
                lines_stats = []
                for uid2, uname, s in stats_rows:
                    uname_final = at_username(uname) if uname else fetch_display_username(uid2)
                    lines_stats.append(f"{uid2} ({uname_final}) - {int(s)} words sent")
                total_allowed = 0
                total_suspended = 0
                with _db_lock:
                    c = GLOBAL_DB_CONN.cursor()
                    c.execute("SELECT COUNT(*) FROM allowed_users")
                    total_allowed = c.fetchone()[0]
                    c.execute("SELECT COUNT(*) FROM suspended_users")
                    total_suspended = c.fetchone()[0]
                body = (
                    f"🤖 Bot Status\n"
                    f"👥 Allowed users: {total_allowed}\n"
                    f"🚫 Suspended users: {total_suspended}\n"
                    f"⚙️ Active tasks: {len(active_rows)}\n"
                    f"📨 Queued tasks: {queued_tasks}\n\n"
                    "Users with active tasks:\n" + ("\n".join(lines_active) if lines_active else "(none)") + "\n\n"
                    "User stats (last 1h):\n" + ("\n".join(lines_stats) if lines_stats else "(none)")
                )
                
                # Edit the message to show bot info, keeping the same menu buttons
                menu_text = f"👑 Owner Menu {OWNER_TAG}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{body}"
                keyboard = [
                    [{"text": "📊 Bot Info", "callback_data": "owner_botinfo"}, {"text": "👥 List Users", "callback_data": "owner_listusers"}],
                    [{"text": "🚫 List Suspended", "callback_data": "owner_listsuspended"}, {"text": "➕ Add User", "callback_data": "owner_adduser"}],
                    [{"text": "⏸️ Suspend User", "callback_data": "owner_suspend"}, {"text": "▶️ Unsuspend User", "callback_data": "owner_unsuspend"}],
                    [{"text": "🔍 Check User Preview", "callback_data": "owner_checkpreview"}]
                ]
                
                try:
                    _session.post(f"{TELEGRAM_API}/editMessageText", json={
                        "chat_id": callback["message"]["chat"]["id"],
                        "message_id": callback["message"]["message_id"],
                        "text": menu_text,
                        "reply_markup": {"inline_keyboard": keyboard}
                    }, timeout=2)
                except Exception:
                    pass
                try:
                    _session.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
                        "callback_query_id": callback.get("id"),
                        "text": "✅ Bot info loaded."
                    }, timeout=2)
                except Exception:
                    pass
                return jsonify({"ok": True})
            
            elif data == "owner_listusers":
                with _db_lock:
                    c = GLOBAL_DB_CONN.cursor()
                    c.execute("SELECT user_id, username, added_at FROM allowed_users ORDER BY added_at DESC")
                    rows = c.fetchall()
                lines = []
                for r in rows:
                    uid2, uname, added_at_utc = r
                    uname_s = f"({at_username(uname)})" if uname else "(no username)"
                    added_at_wat = utc_to_wat_ts(added_at_utc)
                    lines.append(f"{uid2} {uname_s} added={added_at_wat}")
                body = "👥 Allowed users:\n" + ("\n".join(lines) if lines else "(none)")
                
                # Edit the message to show user list, keeping the same menu buttons
                menu_text = f"👑 Owner Menu {OWNER_TAG}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{body}"
                keyboard = [
                    [{"text": "📊 Bot Info", "callback_data": "owner_botinfo"}, {"text": "👥 List Users", "callback_data": "owner_listusers"}],
                    [{"text": "🚫 List Suspended", "callback_data": "owner_listsuspended"}, {"text": "➕ Add User", "callback_data": "owner_adduser"}],
                    [{"text": "⏸️ Suspend User", "callback_data": "owner_suspend"}, {"text": "▶️ Unsuspend User", "callback_data": "owner_unsuspend"}],
                    [{"text": "🔍 Check User Preview", "callback_data": "owner_checkpreview"}]
                ]
                
                try:
                    _session.post(f"{TELEGRAM_API}/editMessageText", json={
                        "chat_id": callback["message"]["chat"]["id"],
                        "message_id": callback["message"]["message_id"],
                        "text": menu_text,
                        "reply_markup": {"inline_keyboard": keyboard}
                    }, timeout=2)
                except Exception:
                    pass
                try:
                    _session.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
                        "callback_query_id": callback.get("id"),
                        "text": "✅ User list loaded."
                    }, timeout=2)
                except Exception:
                    pass
                return jsonify({"ok": True})
            
            elif data == "owner_listsuspended":
                # Auto-unsuspend expired ones first
                for row in list_suspended()[:]:
                    uid2, until_utc, reason, added_at_utc = row
                    until_dt = datetime.strptime(until_utc, "%Y-%m-%d %H:%M:%S")
                    if until_dt <= datetime.utcnow():
                        unsuspend_user(uid2)
                rows = list_suspended()
                if not rows:
                    body = "✅ No suspended users."
                else:
                    lines = []
                    for r in rows:
                        uid2, until_utc, reason, added_at_utc = r
                        until_wat = utc_to_wat_ts(until_utc)
                        added_wat = utc_to_wat_ts(added_at_utc)
                        uname = fetch_display_username(uid2)
                        uname_s = f"({at_username(uname)})" if uname else ""
                        lines.append(f"{uid2} {uname_s} until={until_wat} reason={reason}")
                    body = "🚫 Suspended users:\n" + "\n".join(lines)
                
                # Edit the message to show suspended list, keeping the same menu buttons
                menu_text = f"👑 Owner Menu {OWNER_TAG}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{body}"
                keyboard = [
                    [{"text": "📊 Bot Info", "callback_data": "owner_botinfo"}, {"text": "👥 List Users", "callback_data": "owner_listusers"}],
                    [{"text": "🚫 List Suspended", "callback_data": "owner_listsuspended"}, {"text": "➕ Add User", "callback_data": "owner_adduser"}],
                    [{"text": "⏸️ Suspend User", "callback_data": "owner_suspend"}, {"text": "▶️ Unsuspend User", "callback_data": "owner_unsuspend"}],
                    [{"text": "🔍 Check User Preview", "callback_data": "owner_checkpreview"}]
                ]
                
                try:
                    _session.post(f"{TELEGRAM_API}/editMessageText", json={
                        "chat_id": callback["message"]["chat"]["id"],
                        "message_id": callback["message"]["message_id"],
                        "text": menu_text,
                        "reply_markup": {"inline_keyboard": keyboard}
                    }, timeout=2)
                except Exception:
                    pass
                try:
                    _session.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
                        "callback_query_id": callback.get("id"),
                        "text": "✅ Suspended list loaded."
                    }, timeout=2)
                except Exception:
                    pass
                return jsonify({"ok": True})
            
            elif data == "owner_backtomenu":
                send_ownersets_menu(uid)
                try:
                    _session.post(f"{TELEGRAM_API}/deleteMessage", json={
                        "chat_id": callback["message"]["chat"]["id"],
                        "message_id": callback["message"]["message_id"]
                    }, timeout=2)
                except Exception:
                    pass
                try:
                    _session.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
                        "callback_query_id": callback.get("id"),
                        "text": "✅ Returning to menu."
                    }, timeout=2)
                except Exception:
                    pass
                return jsonify({"ok": True})
            
            elif data.startswith("owner_checkpreview_"):
                # Handle pagination for check preview - FIXED VERSION
                parts = data.split("_")
                if len(parts) == 4:  # owner_checkpreview_userid_page
                    target_user = int(parts[2])
                    page = int(parts[3])
                    # We need to get the hours from state or use default
                    state = get_owner_state(uid)
                    hours = state.get("hours", 24) if state else 24
                    
                    tasks, total_tasks, total_pages = get_user_tasks_preview(target_user, hours, page)
                    
                    if not tasks:
                        body = f"📋 No tasks found for user {target_user} in the last {hours} hours."
                    else:
                        lines = []
                        for task in tasks:
                            lines.append(f"🕒 {task['created_at']}\n📝 Preview: {task['preview']}\n📊 Progress: {task['sent_count']}/{task['total_words']} words")
                        
                        body = f"📋 Task preview for user {target_user} (last {hours}h, page {page+1}/{total_pages}):\n\n" + "\n\n".join(lines)
                    
                    # Create keyboard with navigation buttons if needed
                    keyboard = []
                    nav_buttons = []
                    if page > 0:
                        nav_buttons.append({"text": "⬅️ Previous", "callback_data": f"owner_checkpreview_{target_user}_{page-1}"})
                    if page + 1 < total_pages:
                        nav_buttons.append({"text": "Next ➡️", "callback_data": f"owner_checkpreview_{target_user}_{page+1}"})
                    if nav_buttons:
                        keyboard.append(nav_buttons)
                    
                    # Add the regular menu buttons
                    keyboard.extend([
                        [{"text": "📊 Bot Info", "callback_data": "owner_botinfo"}, {"text": "👥 List Users", "callback_data": "owner_listusers"}],
                        [{"text": "🚫 List Suspended", "callback_data": "owner_listsuspended"}, {"text": "➕ Add User", "callback_data": "owner_adduser"}],
                        [{"text": "⏸️ Suspend User", "callback_data": "owner_suspend"}, {"text": "▶️ Unsuspend User", "callback_data": "owner_unsuspend"}],
                        [{"text": "🔍 Check User Preview", "callback_data": "owner_checkpreview"}]
                    ])
                    
                    menu_text = f"👑 Owner Menu {OWNER_TAG}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{body}"
                    
                    try:
                        _session.post(f"{TELEGRAM_API}/editMessageText", json={
                            "chat_id": callback["message"]["chat"]["id"],
                            "message_id": callback["message"]["message_id"],
                            "text": menu_text,
                            "reply_markup": {"inline_keyboard": keyboard}
                        }, timeout=2)
                    except Exception:
                        pass
                    try:
                        _session.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
                            "callback_query_id": callback.get("id")
                        }, timeout=2)
                    except Exception:
                        pass
                return jsonify({"ok": True})
            
            elif data in ["owner_adduser", "owner_suspend", "owner_unsuspend", "owner_checkpreview"]:
                # Operations that require additional input - SEND NEW MESSAGE instead of editing
                operation = data.replace("owner_", "")
                set_owner_state(uid, {"operation": operation, "step": 0})
                
                prompts = {
                    "adduser": "👤 Please send the User ID to add (you can add multiple IDs separated by spaces or commas):",
                    "suspend": "⏸️ Please send:\n1. User ID\n2. Duration (e.g., 30s, 10m, 2h, 1d)\n3. Optional reason\n\nExample: 123456789 30s Too many requests",
                    "unsuspend": "▶️ Please send the User ID to unsuspend:",
                    "checkpreview": "🔍 Please send the User ID to check:"
                }
                
                # Send a NEW message instead of editing the menu
                cancel_keyboard = {"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": "owner_cancelinput"}]]}
                
                try:
                    # Send new message for input
                    send_message(uid, f"⚠️ {prompts[operation]}\n\nPlease send the requested information as a text message.", cancel_keyboard)
                except Exception:
                    pass
                try:
                    _session.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
                        "callback_query_id": callback.get("id"),
                        "text": "ℹ️ Please check your new message."
                    }, timeout=2)
                except Exception:
                    pass
                return jsonify({"ok": True})
            
            elif data == "owner_cancelinput":
                clear_owner_state(uid)
                try:
                    _session.post(f"{TELEGRAM_API}/deleteMessage", json={
                        "chat_id": callback["message"]["chat"]["id"],
                        "message_id": callback["message"]["message_id"]
                    }, timeout=2)
                except Exception:
                    pass
                try:
                    _session.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
                        "callback_query_id": callback.get("id"),
                        "text": "❌ Operation cancelled."
                    }, timeout=2)
                except Exception:
                    pass
                return jsonify({"ok": True})
            
            # Answer callback query to remove loading state
            try:
                _session.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
                    "callback_query_id": callback.get("id")
                }, timeout=2)
            except Exception:
                pass
            
            return jsonify({"ok": True})
        
        # Handle regular messages
        if "message" in update:
            msg = update["message"]
            user = msg.get("from", {})
            uid = user.get("id")
            username = user.get("username") or (user.get("first_name") or "")
            text = msg.get("text") or ""

            # Update username only for existing/allowed users
            try:
                with _db_lock:
                    c = GLOBAL_DB_CONN.cursor()
                    c.execute("UPDATE allowed_users SET username = ? WHERE user_id = ?", (username or "", uid))
                    GLOBAL_DB_CONN.commit()
            except Exception:
                logger.exception("webhook: update allowed_users username failed")

            # Check if owner is in input mode - handle BEFORE checking commands or regular text
            # IMPORTANT: This check comes FIRST to block task processing for owners in operation
            if uid in OWNER_IDS and is_owner_in_operation(uid):
                state = get_owner_state(uid)
                if state:
                    operation = state.get("operation")
                    step = state.get("step", 0)
                    
                    if operation == "adduser":
                        parts = re.split(r"[,\s]+", text.strip())
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
                        result_msg = "✅ " + ("; ".join(parts_msgs) if parts_msgs else "No changes")
                        
                        clear_owner_state(uid)
                        # Send completion message
                        send_message(uid, f"{result_msg}\n\nUse /ownersets again to access the menu. 😊")
                        return jsonify({"ok": True})
                    
                    elif operation == "suspend":
                        if step == 0:
                            parts = text.split()
                            if len(parts) < 2:
                                send_message(uid, "⚠️ Please provide both User ID and duration. Example: 123456789 30s")
                                return jsonify({"ok": True})
                            
                            try:
                                target = int(parts[0])
                            except Exception:
                                send_message(uid, "❌ Invalid User ID. Please try again.")
                                return jsonify({"ok": True})
                            
                            dur = parts[1]
                            reason = " ".join(parts[2:]) if len(parts) > 2 else ""
                            m = re.match(r"^(\d+)(s|m|h|d)?$", dur)
                            if not m:
                                send_message(uid, "❌ Invalid duration format. Examples: 30s 10m 2h 1d")
                                return jsonify({"ok": True})
                            
                            val, unit = int(m.group(1)), (m.group(2) or "s")
                            mul = {"s":1, "m":60, "h":3600, "d":86400}.get(unit,1)
                            seconds = val * mul
                            
                            suspend_user(target, seconds, reason)
                            reason_part = f"\nReason: {reason}" if reason else ""
                            until_wat = utc_to_wat_ts((datetime.utcnow() + timedelta(seconds=seconds)).strftime('%Y-%m-%d %H:%M:%S'))
                            
                            clear_owner_state(uid)
                            # Send completion message
                            send_message(uid, f"✅ User {label_for_owner_view(target, fetch_display_username(target))} suspended until {until_wat}.{reason_part}\n\nUse /ownersets again to access the menu. 😊")
                            return jsonify({"ok": True})
                    
                    elif operation == "unsuspend":
                        try:
                            target = int(text.strip())
                        except Exception:
                            send_message(uid, "❌ Invalid User ID. Please try again.")
                            return jsonify({"ok": True})
                        
                        ok = unsuspend_user(target)
                        if ok:
                            result = f"✅ User {label_for_owner_view(target, fetch_display_username(target))} unsuspended."
                        else:
                            result = f"ℹ️ User {target} is not suspended."
                        
                        clear_owner_state(uid)
                        # Send completion message
                        send_message(uid, f"{result}\n\nUse /ownersets again to access the menu. 😊")
                        return jsonify({"ok": True})
                    
                    elif operation == "checkpreview":
                        if step == 0:
                            # First step: get user ID
                            try:
                                target_user = int(text.strip())
                            except Exception:
                                send_message(uid, "❌ Invalid User ID. Please try again.")
                                return jsonify({"ok": True})
                            
                            # Save user ID and ask for hours
                            state["user_id"] = target_user
                            state["step"] = 1
                            set_owner_state(uid, state)
                            
                            cancel_keyboard = {"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": "owner_cancelinput"}]]}
                            send_message(uid, "⏰ How many hours back should I check? (e.g., 1, 6, 24, 168):", cancel_keyboard)
                            return jsonify({"ok": True})
                        
                        elif step == 1:
                            # Second step: get hours
                            try:
                                hours = int(text.strip())
                                if hours <= 0:
                                    raise ValueError
                            except Exception:
                                send_message(uid, "❌ Please enter a valid positive number of hours.")
                                return jsonify({"ok": True})
                            
                            target_user = state.get("user_id")
                            state["hours"] = hours
                            set_owner_state(uid, state)
                            
                            # Get tasks preview
                            tasks, total_tasks, total_pages = get_user_tasks_preview(target_user, hours, 0)
                            
                            if not tasks:
                                body = f"📋 No tasks found for user {target_user} in the last {hours} hours."
                            else:
                                lines = []
                                for task in tasks:
                                    lines.append(f"🕒 {task['created_at']}\n📝 Preview: {task['preview']}\n📊 Progress: {task['sent_count']}/{task['total_words']} words")
                                
                                body = f"📋 Task preview for user {target_user} (last {hours}h, page 1/{total_pages}):\n\n" + "\n\n".join(lines)
                            
                            # Create keyboard with navigation if needed
                            keyboard = []
                            nav_buttons = []
                            if total_pages > 1:
                                nav_buttons.append({"text": "Next ➡️", "callback_data": f"owner_checkpreview_{target_user}_1"})
                            if nav_buttons:
                                keyboard.append(nav_buttons)
                            
                            # Add the regular menu buttons
                            keyboard.extend([
                                [{"text": "📊 Bot Info", "callback_data": "owner_botinfo"}, {"text": "👥 List Users", "callback_data": "owner_listusers"}],
                                [{"text": "🚫 List Suspended", "callback_data": "owner_listsuspended"}, {"text": "➕ Add User", "callback_data": "owner_adduser"}],
                                [{"text": "⏸️ Suspend User", "callback_data": "owner_suspend"}, {"text": "▶️ Unsuspend User", "callback_data": "owner_unsuspend"}],
                                [{"text": "🔍 Check User Preview", "callback_data": "owner_checkpreview"}]
                            ])
                            
                            clear_owner_state(uid)
                            menu_text = f"👑 Owner Menu {OWNER_TAG}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{body}"
                            
                            # Send the preview with menu buttons
                            send_message(uid, menu_text, {"inline_keyboard": keyboard})
                            return jsonify({"ok": True})
            
            # Handle commands
            if text.startswith("/"):
                parts = text.split(None, 1)
                cmd = parts[0].split("@")[0].lower()
                args = parts[1] if len(parts) > 1 else ""
                
                # Clear any existing owner state when new command comes
                clear_owner_state(uid)
                
                # Handle /ownersets command
                if cmd == "/ownersets":
                    if uid not in OWNER_IDS:
                        send_message(uid, f"🚫 Owner only. {OWNER_TAG} notified.")
                        notify_owners(f"🚨 Unallowed /ownersets attempt by {at_username(username) if username else uid} (ID: {uid}).")
                        return jsonify({"ok": True})
                    send_ownersets_menu(uid)
                    return jsonify({"ok": True})
                else:
                    return handle_command(uid, username, cmd, args)
            else:
                # Handle regular text input
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

    if command == "/start":
        who = label_for_self(user_id, username) or "there"
        msg = (
            f"👋 Hi {who}!\n\n"
            "I split your text into individual word messages. ✂️📤\n\n"
            f"{OWNER_TAG} command:\n"
            " /ownersets - Owner management menu\n\n"
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

    send_message(user_id, "❓ Unknown command.")
    return jsonify({"ok": True})

def handle_user_text(user_id: int, username: str, text: str):
    # BLOCK OWNER TASK PROCESSING: If owner is in operation mode, don't process their text as task
    if user_id in OWNER_IDS and is_owner_in_operation(user_id):
        # This should not happen if operations are processed correctly
        logger.warning(f"Owner {user_id} text reached handle_user_text while in operation state. Text: {text[:50]}...")
        # Don't process it as a task
        return jsonify({"ok": True})
    
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
