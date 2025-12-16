#!/usr/bin/env python3
"""
WordSplitter Telegram Bot - with ownersets command grouping and user preview feature
"""

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

# User state management for multi-step commands
class UserState:
    def __init__(self):
        self.states = {}
        self.lock = threading.Lock()
    
    def set_state(self, user_id: int, state: str, data: dict = None):
        with self.lock:
            self.states[user_id] = {
                "state": state,
                "data": data or {},
                "timestamp": time.time()
            }
    
    def get_state(self, user_id: int) -> Optional[dict]:
        with self.lock:
            state = self.states.get(user_id)
            # Cleanup old states (older than 1 hour)
            if state and time.time() - state["timestamp"] > 3600:
                del self.states[user_id]
                return None
            return state
    
    def clear_state(self, user_id: int):
        with self.lock:
            self.states.pop(user_id, None)
    
    def cleanup_old_states(self):
        with self.lock:
            current_time = time.time()
            to_remove = []
            for user_id, state in self.states.items():
                if current_time - state["timestamp"] > 3600:
                    to_remove.append(user_id)
            for user_id in to_remove:
                del self.states[user_id]

user_state = UserState()

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

def send_message(chat_id: int, text: str, reply_markup: dict = None):
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

def send_inline_keyboard(chat_id: int, text: str, buttons: List[List[Dict]]):
    """Send a message with inline keyboard buttons."""
    keyboard = {"inline_keyboard": buttons}
    return send_message(chat_id, text, reply_markup=keyboard)

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

def get_user_task_preview(user_id: int, hours: int) -> List[Tuple[str, str]]:
    """Get first two words of each task for a user in the specified hours"""
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    with _db_lock:
        c = GLOBAL_DB_CONN.cursor()
        c.execute("""
            SELECT text, created_at FROM tasks 
            WHERE user_id = ? AND created_at >= ? AND status = 'done'
            ORDER BY created_at DESC
        """, (user_id, cutoff.strftime("%Y-%m-%d %H:%M:%S")))
        rows = c.fetchall()
    
    previews = []
    for text, created_at in rows:
        words = split_text_to_words(text)
        if len(words) >= 2:
            preview = " ".join(words[:2])
        elif len(words) == 1:
            preview = words[0]
        else:
            preview = "(empty)"
        previews.append((preview, created_at))
    
    return previews

# Worker functions (unchanged from original)
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
    # ... (unchanged from original)
    pass

# Scheduler tasks
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

# New: State management for multi-step commands
def handle_ownersets_command(user_id: int, username: str):
    """Send ownersets inline keyboard"""
    buttons = [
        [{"text": "➕ Add User", "callback_data": "add_user"}],
        [{"text": "👥 List Users", "callback_data": "list_users"}],
        [{"text": "🚫 List Suspended", "callback_data": "list_suspended"}],
        [{"text": "🤖 Bot Info", "callback_data": "bot_info"}],
        [{"text": "📣 Broadcast", "callback_data": "broadcast"}],
        [{"text": "⏸️ Suspend User", "callback_data": "suspend_user"}],
        [{"text": "▶️ Unsuspend User", "callback_data": "unsuspend_user"}],
        [{"text": "🔍 Check User Preview", "callback_data": "check_preview"}],
        [{"text": "❌ Cancel", "callback_data": "cancel"}]
    ]
    send_inline_keyboard(user_id, "🔧 Owner Settings Menu:", buttons)

def handle_ownersets_state(user_id: int, text: str):
    """Handle multi-step owner commands based on state"""
    state = user_state.get_state(user_id)
    if not state:
        return False
    
    action = state["data"].get("action")
    
    if action == "add_user":
        # Step 1: Got user ID
        try:
            target_id = int(text.strip())
            # Move to step 2: ask for username
            user_state.set_state(user_id, "ownersets", {
                "action": "add_user_username",
                "target_id": target_id
            })
            send_message(user_id, f"Got user ID: {target_id}. Now send the username (or send '-' to skip):")
            return True
        except ValueError:
            send_message(user_id, "❌ Invalid user ID. Please enter a numeric user ID:")
            return True
    
    elif action == "add_user_username":
        # Step 2: Got username
        target_id = state["data"]["target_id"]
        username = text.strip() if text.strip() != "-" else ""
        
        try:
            with _db_lock:
                c = GLOBAL_DB_CONN.cursor()
                c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (target_id,))
                if c.fetchone():
                    send_message(user_id, f"⚠️ User {target_id} is already allowed.")
                else:
                    c.execute("INSERT INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)", 
                              (target_id, username, now_ts()))
                    GLOBAL_DB_CONN.commit()
                    
                    # Try to notify the user
                    try:
                        send_message(target_id, f"✅ You have been added to WordSplitter by {OWNER_TAG}.\nSend any text to start splitting!")
                    except Exception:
                        pass
                    
                    username_part = f" with username: {username}" if username else ""
                    send_message(user_id, f"✅ User {target_id}{username_part} has been added.")
        except Exception as e:
            logger.exception("Error adding user")
            send_message(user_id, f"❌ Error adding user: {str(e)}")
        
        user_state.clear_state(user_id)
        return True
    
    elif action == "broadcast":
        # Send broadcast to all users
        with _db_lock:
            c = GLOBAL_DB_CONN.cursor()
            c.execute("SELECT user_id FROM allowed_users")
            rows = c.fetchall()
        
        total = len(rows)
        success = 0
        failed = []
        
        broadcast_text = f"📣 Broadcast from {OWNER_TAG}:\n\n{text}"
        
        for row in rows:
            uid = row[0]
            try:
                send_message(uid, broadcast_text)
                success += 1
            except Exception as e:
                failed.append(uid)
        
        # Send summary
        summary = f"📨 Broadcast completed:\n✅ Success: {success}\n❌ Failed: {len(failed)}"
        if failed:
            summary += f"\n\nFailed users: {', '.join(map(str, failed[:10]))}"
            if len(failed) > 10:
                summary += f" ... and {len(failed) - 10} more"
        
        send_message(user_id, summary)
        user_state.clear_state(user_id)
        return True
    
    elif action == "suspend_user":
        # Step 1: Got user ID to suspend
        try:
            target_id = int(text.strip())
            # Move to step 2: ask for duration
            user_state.set_state(user_id, "ownersets", {
                "action": "suspend_duration",
                "target_id": target_id
            })
            send_message(user_id, f"Got user ID: {target_id}. Now send the duration (e.g., 1h, 30m, 2d):")
            return True
        except ValueError:
            send_message(user_id, "❌ Invalid user ID. Please enter a numeric user ID:")
            return True
    
    elif action == "suspend_duration":
        # Step 2: Got duration
        target_id = state["data"]["target_id"]
        duration_text = text.strip().lower()
        
        # Parse duration
        match = re.match(r"^(\d+)([smhd])$", duration_text)
        if not match:
            send_message(user_id, "❌ Invalid duration format. Use format like: 1h, 30m, 2d")
            return True
        
        amount = int(match.group(1))
        unit = match.group(2)
        
        # Convert to seconds
        multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        seconds = amount * multipliers.get(unit, 1)
        
        # Move to step 3: ask for reason
        user_state.set_state(user_id, "ownersets", {
            "action": "suspend_reason",
            "target_id": target_id,
            "seconds": seconds
        })
        send_message(user_id, f"Duration set: {duration_text}. Now send the reason (or send '-' for no reason):")
        return True
    
    elif action == "suspend_reason":
        # Step 3: Got reason
        target_id = state["data"]["target_id"]
        seconds = state["data"]["seconds"]
        reason = text.strip() if text.strip() != "-" else ""
        
        # Suspend the user
        suspend_user(target_id, seconds, reason)
        
        duration_str = ""
        if seconds >= 86400:
            duration_str = f"{seconds//86400}d"
        elif seconds >= 3600:
            duration_str = f"{seconds//3600}h"
        elif seconds >= 60:
            duration_str = f"{seconds//60}m"
        else:
            duration_str = f"{seconds}s"
        
        reason_part = f" for: {reason}" if reason else ""
        send_message(user_id, f"✅ User {target_id} suspended for {duration_str}{reason_part}.")
        user_state.clear_state(user_id)
        return True
    
    elif action == "unsuspend_user":
        # Unsuspend user
        try:
            target_id = int(text.strip())
            if unsuspend_user(target_id):
                send_message(user_id, f"✅ User {target_id} has been unsuspended.")
            else:
                send_message(user_id, f"ℹ️ User {target_id} was not suspended.")
        except ValueError:
            send_message(user_id, "❌ Invalid user ID.")
        
        user_state.clear_state(user_id)
        return True
    
    elif action == "check_preview_user":
        # Step 1: Got user ID for preview
        try:
            target_id = int(text.strip())
            # Move to step 2: ask for hours
            user_state.set_state(user_id, "ownersets", {
                "action": "check_preview_hours",
                "target_id": target_id
            })
            send_message(user_id, f"Got user ID: {target_id}. Now send the number of hours to check (1-168):")
            return True
        except ValueError:
            send_message(user_id, "❌ Invalid user ID. Please enter a numeric user ID:")
            return True
    
    elif action == "check_preview_hours":
        # Step 2: Got hours
        try:
            hours = int(text.strip())
            if hours < 1 or hours > 168:
                send_message(user_id, "❌ Please enter hours between 1 and 168:")
                return True
            
            target_id = state["data"]["target_id"]
            previews = get_user_task_preview(target_id, hours)
            
            if not previews:
                send_message(user_id, f"📭 No tasks found for user {target_id} in the last {hours} hours.")
            else:
                username = fetch_display_username(target_id) or str(target_id)
                message = f"🔍 Task preview for {username} (last {hours}h):\n\n"
                
                for i, (preview, created_at) in enumerate(previews[:50], 1):  # Limit to 50 tasks
                    wat_time = utc_to_wat_ts(created_at)
                    message += f"{i}. {preview} [...]\n   📅 {wat_time}\n\n"
                
                if len(previews) > 50:
                    message += f"\n... and {len(previews) - 50} more tasks."
                
                # Send in chunks if too long
                if len(message) > 4000:
                    chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
                    for chunk in chunks:
                        send_message(user_id, chunk)
                else:
                    send_message(user_id, message)
            
            user_state.clear_state(user_id)
            return True
        except ValueError:
            send_message(user_id, "❌ Invalid number. Please enter a number between 1 and 168:")
            return True
    
    return False

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
            user = callback["from"]
            user_id = user["id"]
            data = callback["data"]
            
            # Answer callback query
            try:
                _session.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
                    "callback_query_id": callback["id"]
                }, timeout=3)
            except Exception:
                pass
            
            # Handle callback data
            if data == "cancel":
                user_state.clear_state(user_id)
                send_message(user_id, "✅ Operation cancelled.")
                return jsonify({"ok": True})
            
            # Check if user is owner
            if user_id not in OWNER_IDS:
                send_message(user_id, "❌ Owner-only command.")
                return jsonify({"ok": True})
            
            # Handle different callback actions
            if data == "add_user":
                user_state.set_state(user_id, "ownersets", {"action": "add_user"})
                send_message(user_id, "👤 Enter the user ID to add:")
            
            elif data == "list_users":
                with _db_lock:
                    c = GLOBAL_DB_CONN.cursor()
                    c.execute("SELECT user_id, username, added_at FROM allowed_users ORDER BY added_at DESC")
                    rows = c.fetchall()
                
                if not rows:
                    send_message(user_id, "📭 No users in the database.")
                else:
                    message = "👥 Allowed Users:\n\n"
                    for uid, username, added_at in rows:
                        username_display = f"@{username}" if username else "(no username)"
                        added_wat = utc_to_wat_ts(added_at)
                        message += f"• {uid} - {username_display}\n  Added: {added_wat}\n\n"
                    
                    # Send in chunks if too long
                    if len(message) > 4000:
                        chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
                        for chunk in chunks:
                            send_message(user_id, chunk)
                    else:
                        send_message(user_id, message)
            
            elif data == "list_suspended":
                rows = list_suspended()
                if not rows:
                    send_message(user_id, "✅ No suspended users.")
                else:
                    message = "🚫 Suspended Users:\n\n"
                    for uid, until_utc, reason, added_at in rows:
                        username = fetch_display_username(uid) or str(uid)
                        until_wat = utc_to_wat_ts(until_utc)
                        added_wat = utc_to_wat_ts(added_at)
                        reason_display = f" for: {reason}" if reason else ""
                        message += f"• {username}\n  Until: {until_wat}{reason_display}\n  Added: {added_wat}\n\n"
                    
                    send_message(user_id, message)
            
            elif data == "bot_info":
                # Get bot stats
                with _db_lock:
                    c = GLOBAL_DB_CONN.cursor()
                    c.execute("SELECT COUNT(*) FROM allowed_users")
                    total_users = c.fetchone()[0]
                    
                    c.execute("SELECT COUNT(*) FROM suspended_users")
                    suspended_users = c.fetchone()[0]
                    
                    c.execute("SELECT COUNT(*) FROM tasks WHERE status IN ('queued', 'running', 'paused')")
                    active_tasks = c.fetchone()[0]
                    
                    c.execute("SELECT COUNT(*) FROM split_logs WHERE created_at >= ?", 
                              ((datetime.utcnow() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),))
                    last_hour_words = c.fetchone()[0]
                
                message = (
                    f"🤖 Bot Status\n"
                    f"────────────\n"
                    f"• Total Users: {total_users}\n"
                    f"• Suspended: {suspended_users}\n"
                    f"• Active Tasks: {active_tasks}\n"
                    f"• Last Hour Words: {last_hour_words}\n"
                    f"• Max Queue/User: {MAX_QUEUE_PER_USER}\n"
                    f"• Rate Limit: {MAX_MSG_PER_SECOND}/sec\n"
                    f"• Workers: {MAX_CONCURRENT_WORKERS}\n"
                    f"• Log Retention: {LOG_RETENTION_DAYS} days\n"
                )
                send_message(user_id, message)
            
            elif data == "broadcast":
                user_state.set_state(user_id, "ownersets", {"action": "broadcast"})
                send_message(user_id, "📣 Enter the broadcast message:")
            
            elif data == "suspend_user":
                user_state.set_state(user_id, "ownersets", {"action": "suspend_user"})
                send_message(user_id, "⏸️ Enter the user ID to suspend:")
            
            elif data == "unsuspend_user":
                user_state.set_state(user_id, "ownersets", {"action": "unsuspend_user"})
                send_message(user_id, "▶️ Enter the user ID to unsuspend:")
            
            elif data == "check_preview":
                user_state.set_state(user_id, "ownersets", {"action": "check_preview_user"})
                send_message(user_id, "🔍 Enter the user ID to check:")
            
            return jsonify({"ok": True})
        
        # Handle regular messages
        if "message" in update:
            msg = update["message"]
            user = msg.get("from", {})
            uid = user.get("id")
            username = user.get("username") or (user.get("first_name") or "")
            text = msg.get("text") or ""
            
            # Update username in database
            try:
                with _db_lock:
                    c = GLOBAL_DB_CONN.cursor()
                    c.execute("UPDATE allowed_users SET username = ? WHERE user_id = ?", (username or "", uid))
                    GLOBAL_DB_CONN.commit()
            except Exception:
                logger.exception("webhook: update allowed_users username failed")
            
            # Check if user is in a state (multi-step command)
            if user_state.get_state(uid):
                if handle_ownersets_state(uid, text):
                    return jsonify({"ok": True})
            
            # Handle commands
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
    
    # Clear any existing state for commands
    user_state.clear_state(user_id)
    
    if command == "/start":
        who = label_for_self(user_id, username) or "there"
        if is_owner(user_id):
            msg = (
                f"👋 Hi {who}!\n\n"
                "I split your text into individual word messages. ✂️📤\n\n"
                f"{OWNER_TAG} commands:\n"
                " /ownersets - Owner settings menu\n\n"
                "User commands:\n"
                " /start /example /pause /resume /status /stop /stats /about\n\n"
                "Just send any text and I'll split it for you. 🚀"
            )
        else:
            msg = (
                f"👋 Hi {who}!\n\n"
                "I split your text into individual word messages. ✂️📤\n\n"
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
    
    if command == "/ownersets":
        if not is_owner(user_id):
            send_message(user_id, f"🔒 {OWNER_TAG} only.")
            return jsonify({"ok": True})
        handle_ownersets_command(user_id, username)
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
