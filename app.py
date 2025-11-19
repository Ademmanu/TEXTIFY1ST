#!/usr/bin/env python3
"""
Telegram Word-Splitter Bot (Webhook-ready) using PostgreSQL (V5.2).

Features:
1. Full PostgreSQL implementation (psycopg2) for high concurrency.
2. Fixes the /example command.
3. Ensures immediate and reliable split execution via improved lock handling.
4. Uses %s placeholders for PostgreSQL queries.
"""
import os
import time
import json
import threading
import traceback
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Any
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify
import requests
import psycopg2
from urllib.parse import urlparse

# --- Configuration ---
name = "word_splitter_bot"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(name)

app = Flask(name)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "") 
OWNER_ID = int(os.environ.get("OWNER_ID", "0")) 
OWNER_USERNAME = os.environ.get("OWNER_USERNAME", "justmemmy") 
MAX_ALLOWED_USERS = int(os.environ.get("MAX_ALLOWED_USERS", "50"))
MAX_QUEUE_PER_USER = int(os.environ.get("MAX_QUEUE_PER_USER", "50"))
MAINTENANCE_START_HOUR_WAT = 3 # 03:00 WAT
MAINTENANCE_END_HOUR_WAT = 4 # 04:00 WAT
DATABASE_URL = os.environ.get("DATABASE_URL")
REQUESTS_TIMEOUT = float(os.environ.get("REQUESTS_TIMEOUT", "10"))

if not TELEGRAM_TOKEN or not WEBHOOK_URL or not OWNER_ID or not DATABASE_URL:
    logger.error("CRITICAL: TELEGRAM_TOKEN, WEBHOOK_URL, OWNER_ID, and DATABASE_URL must be set.")
    exit(1)

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# --- DB helper for PostgreSQL ---

_db_lock = threading.Lock()
_conn = None

def get_db_connection() -> psycopg2.extensions.connection:
    """Connects to the database and returns the connection."""
    global _conn
    if _conn is None or _conn.closed != 0:
        logger.info("Reconnecting to PostgreSQL...")
        try:
            _conn = psycopg2.connect(DATABASE_URL, sslmode='require') 
            _conn.autocommit = True  
        except Exception:
            logger.exception("Failed to connect to PostgreSQL")
            raise
    return _conn

def init_db():
    try:
        conn = get_db_connection()
        with conn.cursor() as c:
            # allowed_users table
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS allowed_users (
                    user_id BIGINT PRIMARY KEY,
                    username VARCHAR(255),
                    added_at TIMESTAMP WITH TIME ZONE,
                    is_admin BOOLEAN DEFAULT FALSE
                )
                """
            )
            # tasks table
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    username VARCHAR(255),
                    text TEXT,
                    words_json TEXT,
                    total_words INTEGER,
                    status VARCHAR(50),
                    current_index INTEGER DEFAULT 0,
                    created_at TIMESTAMP WITH TIME ZONE,
                    started_at TIMESTAMP WITH TIME ZONE,
                    finished_at TIMESTAMP WITH TIME ZONE
                )
                """
            )
            # split_logs table
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS split_logs (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    username VARCHAR(255),
                    words INTEGER,
                    created_at TIMESTAMP WITH TIME ZONE
                )
                """
            )
            # sent_messages table
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS sent_messages (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    message_id BIGINT,
                    sent_at TIMESTAMP WITH TIME ZONE,
                    deleted BOOLEAN DEFAULT FALSE
                )
                """
            )
            conn.commit()
            
            # Ensure owner is admin
            c.execute(
                "INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (%s, %s, NOW(), TRUE) ON CONFLICT (user_id) DO UPDATE SET is_admin = excluded.is_admin",
                (OWNER_ID, OWNER_USERNAME)
            )
            conn.commit()
            logger.info("PostgreSQL initialization complete and owner ensured.")
    except Exception:
        logger.exception("FATAL: Error during PostgreSQL initialization.")
        exit(1)

def db_execute(query: str, params: tuple = (), fetch: bool = False) -> Any:
    """
    Executes a database query with connection resilience (retry on Interface/Operational Errors).
    """
    global _conn
    
    for attempt in range(2):
        with _db_lock:
            try:
                conn = get_db_connection()
                with conn.cursor() as c:
                    c.execute(query, params)
                    if fetch:
                        return c.fetchall()
                    return None
            except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
                logger.warning(f"DB Interface Error (Attempt {attempt+1}): {e}. Resetting connection.")
                if _conn and _conn.closed == 0:
                    try: _conn.close() 
                    except: pass
                _conn = None 
                
                if attempt == 1:
                    logger.error(f"Failed to execute query after retry: {query}")
                    raise 
            except Exception:
                logger.error(f"DB Execution Failed: {query}")
                raise

# Initialize DB
init_db()

# In-memory per-user locks
user_locks = {}
user_locks_lock = threading.Lock()

def get_user_lock(user_id):
    """Returns the unique lock for a given user_id."""
    with user_locks_lock:
        if user_id not in user_locks:
            user_locks[user_id] = threading.Lock()
        return user_locks[user_id]

# --- Utilities ---

def get_now_iso():
    return datetime.utcnow().isoformat()

def split_text_into_words(text: str) -> List[str]:
    return [w for w in text.strip().split() if w]

def compute_interval(total_words: int) -> float:
    if total_words <= 150:
        return 0.4
    elif total_words <= 300:
        return 0.5
    else:
        return 0.6

def is_maintenance_now() -> bool:
    utc_now = datetime.utcnow()
    wat_now = utc_now + timedelta(hours=1)
    h = wat_now.hour
    if MAINTENANCE_START_HOUR_WAT < MAINTENANCE_END_HOUR_WAT:
        return MAINTENANCE_START_HOUR_WAT <= h < MAINTENANCE_END_HOUR_WAT
    return h >= MAINTENANCE_START_HOUR_WAT or h < MAINTENANCE_END_HOUR_WAT

# --- Telegram API helpers (requests) ---

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
            logger.error("Telegram API error: %s", data)
            return data
    except Exception:
        logger.exception("tg_call failed")
        return None

def send_message(chat_id: int, text: str, parse_mode: str = "Markdown"):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
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
        db_execute("UPDATE sent_messages SET deleted = TRUE WHERE chat_id = %s AND message_id = %s", (chat_id, message_id))
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

# --- Authorization helpers ---

def is_allowed(user_id: int) -> bool:
    rows = db_execute("SELECT 1 FROM allowed_users WHERE user_id = %s", (user_id,), fetch=True)
    return bool(rows)

def is_admin(user_id: int) -> bool:
    rows = db_execute("SELECT is_admin FROM allowed_users WHERE user_id = %s", (user_id,), fetch=True)
    if not rows:
        return False
    return bool(rows[0][0])

# --- Task management ---

def enqueue_task(user_id: int, username: str, text: str) -> dict:
    words = split_text_into_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    
    q_result = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status IN ('queued','running','paused')", (user_id,), fetch=True)
    pending = q_result[0][0] if q_result else 0
    if pending >= MAX_QUEUE_PER_USER:
        return {"ok": False, "reason": "queue_full", "queue_size": pending}
    
    rows = db_execute(
        "INSERT INTO tasks (user_id, username, text, words_json, total_words, status, current_index, created_at) VALUES (%s, %s, %s, %s, %s, 'queued', 0, NOW()) RETURNING id",
        (user_id, username, text, json.dumps(words), total),
        fetch=True
    )
    new_task_id = rows[0][0] if rows else None

    return {"ok": True, "total_words": total, "queue_size": pending + 1, "task_id": new_task_id, "words": words}

def get_next_task_for_user(user_id: int) -> Optional[dict]:
    """Retrieves the next queued task for a user."""
    rows = db_execute(
        "SELECT id, words_json, total_words, text, current_index FROM tasks WHERE user_id = %s AND status = 'queued' ORDER BY id ASC LIMIT 1",
        (user_id,),
        fetch=True,
    )
    if not rows:
        return None
    r = rows[0]
    return {"id": r[0], "words": json.loads(r[1]), "total_words": r[2], "text": r[3], "current_index": r[4]}

def get_running_task_for_user(user_id: int) -> Optional[dict]:
    """Retrieves the active task for a user (running or paused)."""
    rows = db_execute(
        "SELECT id, words_json, total_words, text, current_index, status FROM tasks WHERE user_id = %s AND status IN ('running', 'paused') ORDER BY id ASC LIMIT 1",
        (user_id,),
        fetch=True,
    )
    if not rows:
        return None
    r = rows[0]
    return {"id": r[0], "words": json.loads(r[1]), "total_words": r[2], "text": r[3], "current_index": r[4], "status": r[5]}

def set_task_status(task_id: int, status: str):
    if status == "running":
        db_execute("UPDATE tasks SET status = %s, started_at = NOW() WHERE id = %s", (status, task_id))
    elif status in ("done", "cancelled"):
        db_execute("UPDATE tasks SET status = %s, finished_at = NOW() WHERE id = %s", (status, task_id))
    else:
        db_execute("UPDATE tasks SET status = %s WHERE id = %s", (status, task_id))

def mark_task_paused(task_id: int, current_index: int):
    """Sets status to paused and stores the index of the next word to send."""
    db_execute("UPDATE tasks SET status = 'paused', current_index = %s WHERE id = %s", (current_index, task_id))

def mark_task_resumed(task_id: int):
    """Sets status to running."""
    set_task_status(task_id, "running")

def mark_task_done(task_id: int):
    set_task_status(task_id, "done")

def cancel_active_task_for_user(user_id: int):
    rows = db_execute("SELECT id FROM tasks WHERE user_id = %s AND status IN ('queued','running','paused')", (user_id,), fetch=True)
    count = 0
    for r in rows:
        db_execute("UPDATE tasks SET status = %s WHERE id = %s", ("cancelled", r[0]))
        count += 1
    return count

def record_split_log(user_id: int, username: str, words: int):
    db_execute("INSERT INTO split_logs (user_id, username, words, created_at) VALUES (%s, %s, %s, NOW())",
        (user_id, username, words))

def record_sent_message(chat_id: int, message_id: int):
    db_execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (%s, %s, NOW(), FALSE)",
        (chat_id, message_id))

def get_messages_older_than(days=1):
    cutoff = datetime.utcnow() - timedelta(days=days)
    rows = db_execute("SELECT chat_id, message_id FROM sent_messages WHERE deleted = FALSE AND sent_at < %s", (cutoff,), fetch=True)
    return [{"chat_id": r[0], "message_id": r[1]} for r in rows]

def get_queue_size(user_id):
    q = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status = 'queued'", (user_id,), fetch=True)
    return q[0][0] if q and q[0][0] else 0

# --- Background Scheduler Logic ---

def release_user_lock_if_held(user_id: int):
    """Helper to safely release the lock if it's currently held."""
    lock = get_user_lock(user_id)
    if lock.locked():
        try:
            lock.release()
            # Immediately try to start the next task for this user
            threading.Thread(target=_start_next_queued_task_if_exists, args=(user_id, user_id, ""), daemon=True).start()
        except RuntimeError:
            pass 

def send_word_and_schedule_next(task_id: int, user_id: int, chat_id: int, words: List[str], index: int, interval: float, username: str):
    """Sends a single word and schedules the next word to be sent if the task is running."""
    
    lock = get_user_lock(user_id)
    
    if not lock.acquire(blocking=False):
        logger.warning(f"Scheduler failed to acquire lock for task {task_id}. Aborting scheduler job.")
        return
    
    try:
        # 1. Check current status
        status_row = db_execute("SELECT status FROM tasks WHERE id = %s", (task_id,), fetch=True)
        if not status_row:
            logger.warning(f"Task {task_id} disappeared during split.")
            return

        status = status_row[0][0]
        
        if status == "cancelled":
            set_task_status(task_id, "cancelled")
            send_message(chat_id, "🛑 Active Task Stopped! The current word transmission has been terminated.")
            return
        
        if status == "paused":
            mark_task_paused(task_id, index)
            send_message(chat_id, "⏸️ Task Paused! Waiting for /resume to continue.")
            return

        if status != "running":
            return
        
        # 2. Send the current word
        if index < len(words):
            send_message(chat_id, words[index])
            
            # Update the current index in DB
            db_execute("UPDATE tasks SET current_index = %s WHERE id = %s", (index + 1, task_id))
        else:
            return

        # 3. Determine next action
        next_index = index + 1
        
        if next_index < len(words):
            # Schedule the next word
            scheduler.add_job(
                send_word_and_schedule_next, 
                "date", 
                run_date=datetime.utcnow() + timedelta(seconds=interval), 
                kwargs={
                    "task_id": task_id, 
                    "user_id": user_id, 
                    "chat_id": chat_id, 
                    "words": words, 
                    "index": next_index, 
                    "interval": interval, 
                    "username": username
                },
                id=f"word_split_{task_id}_{next_index}", 
                replace_existing=True
            )
        else:
            # Task is complete
            mark_task_done(task_id)
            record_split_log(user_id, username, len(words))
            send_message(chat_id, "✅ All Done! Split finished successfully.")
            
            qcount_after = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status = 'queued'", (user_id,), fetch=True)[0][0]
            if qcount_after > 0:
                send_message(chat_id, "⏩ Next Task Starting Soon! Your next queued task is ready for processing.")
            
    except Exception:
        logger.exception(f"Fatal error in scheduled word transmission for task {task_id}")
        set_task_status(task_id, "cancelled")
        send_message(chat_id, "❌ Critical Error: Word transmission failed unexpectedly and has been stopped.")
    finally:
        # Crucial: Release the lock immediately after *this* scheduled job finishes its work
        if lock.locked():
             lock.release()
             # If released due to completion/cancellation/pause, try starting the next task.
             final_status_row = db_execute("SELECT status FROM tasks WHERE id = %s", (task_id,), fetch=True)
             final_status = final_status_row[0][0] if final_status_row else "error"
             if final_status in ('done', 'cancelled'):
                 threading.Thread(target=_start_next_queued_task_if_exists, args=(user_id, user_id, username), daemon=True).start()

# --- Background worker to process tasks for users ---

def _start_next_queued_task_if_exists(user_id: int, chat_id: int, username: str):
    """Acquires the lock, starts the task, and schedules the first word."""
    lock = get_user_lock(user_id)
    
    if not lock.acquire(blocking=False):
        return
    
    try:
        task = get_next_task_for_user(user_id)
        if not task:
            return 

        task_id = task["id"]
        words = task["words"]
        total = task["total_words"]
        set_task_status(task_id, "running")
        
        qcount = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status = 'queued'", (user_id,), fetch=True)[0][0]
        interval = compute_interval(total)
        est_seconds = interval * total
        est_str = str(timedelta(seconds=int(est_seconds)))

        start_msg = (
            f"🚀 Task Starting! (Waiting: {qcount})\n"
            f"Words: {total}. Time per word: {interval}s\n"
            f"⏳ Estimated total time: {est_str}"
        )
        send_message(chat_id, start_msg)
        
        # Schedule the very first word immediately (0.1s delay for stability)
        scheduler.add_job(
            send_word_and_schedule_next, 
            "date", 
            run_date=datetime.utcnow() + timedelta(seconds=0.1), 
            kwargs={
                "task_id": task_id, 
                "user_id": user_id, 
                "chat_id": chat_id, 
                "words": words, 
                "index": 0, 
                "interval": interval, 
                "username": username
            },
            id=f"word_split_{task_id}_0", 
            replace_existing=True
        )
        
    except Exception:
        logger.exception("Error in _start_next_queued_task_if_exists")
        if lock.locked():
             lock.release()
    finally:
        # Lock is held until released by the scheduler chain or on error above
        pass

_worker_stop = threading.Event()

def global_worker_loop():
    """Runs less often and only checks for queued users not currently locked."""
    while not _worker_stop.is_set():
        try:
            rows = db_execute("SELECT DISTINCT user_id, username FROM tasks WHERE status = 'queued' ORDER BY created_at ASC", fetch=True)
            for r in rows:
                user_id = r[0]
                username = r[1] or ""
                if not get_user_lock(user_id).locked():
                     t = threading.Thread(target=_start_next_queued_task_if_exists, args=(user_id, user_id, username), daemon=True)
                     t.start()
            time.sleep(5) 
        except Exception:
            traceback.print_exc()
            time.sleep(10)

# --- Scheduler Jobs ---
scheduler = BackgroundScheduler()

def delete_old_bot_messages():
    msgs = get_messages_older_than(days=1)
    for m in msgs:
        try:
            delete_message(m["chat_id"], m["message_id"])
        except Exception:
            db_execute("UPDATE sent_messages SET deleted = TRUE WHERE chat_id = %s AND message_id = %s", (m["chat_id"], m["message_id"]))

scheduler.add_job(lambda: send_message(OWNER_ID, "Health Check"), "interval", hours=1) 
scheduler.add_job(delete_old_bot_messages, "interval", minutes=30)
scheduler.start()

_worker_thread = threading.Thread(target=global_worker_loop, daemon=True)
_worker_thread.start()

# --- Flask Webhook and Command Handlers ---

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
        return jsonify({"ok": True})
    except Exception: 
        logger.exception("Error handling webhook update") 
        return jsonify({"ok": True}) 

def handle_command(user_id: int, username: str, command: str, args: str):
    
    if not is_allowed(user_id) and command not in ("/start", "/help", "/example"):
        send_message(user_id, f"❌ Sorry! You are not allowed to use this bot.")
        return jsonify({"ok": True})
        
    if command == "/start":
        body = (
            f"👋 Welcome, @{username or user_id}!\n"
            "I split your text into individual word messages. Commands:\n"
            "/start /example /pause /resume /status /stop /stats /about\n"
            "Owner-only: /adduser /removeuser /listusers /botinfo /broadcast\n"
            "How to use: send any text to split it into words."
        )
        send_message(user_id, body)
        return jsonify({"ok": True})
        
    if command == "/example": 
        example_text = "This is an example text to demonstrate the word splitting feature of the bot. It should start immediately."
        return handle_new_text(user_id, username, example_text)

    if command == "/pause": 
        active_task = get_running_task_for_user(user_id)
        if not active_task or active_task["status"] != "running": 
            send_message(user_id, "❌ **Sorry!** No active task found for you to pause.") 
            return jsonify({"ok": True}) 
        
        task_id = active_task["id"]
        set_task_status(task_id, "paused") 
        send_message(user_id, "⏸️ **Task Paused!** Waiting for /resume to continue.")
        return jsonify({"ok": True}) 
        
    if command == "/resume": 
        paused_task = get_running_task_for_user(user_id)
        if not paused_task or paused_task["status"] != "paused": 
            send_message(user_id, "❌ **Sorry!** No paused task found for you to resume.") 
            return jsonify({"ok": True}) 
            
        task_id = paused_task["id"]
        words = paused_task["words"]
        total = paused_task["total_words"]
        current_index = paused_task["current_index"]
        interval = compute_interval(total) 

        lock = get_user_lock(user_id)
        if not lock.acquire(blocking=False):
            send_message(user_id, "⚠️ Cannot resume: Another process is using the lock. Try again.")
            return jsonify({"ok": True})

        try:
            mark_task_resumed(task_id)
            send_message(user_id, "▶️ **Welcome Back!** Resuming word transmission now.") 
            
            scheduler.add_job(
                send_word_and_schedule_next, 
                "date", 
                run_date=datetime.utcnow() + timedelta(seconds=0.1), 
                kwargs={
                    "task_id": task_id, 
                    "user_id": user_id, 
                    "chat_id": user_id, 
                    "words": words, 
                    "index": current_index, 
                    "interval": interval, 
                    "username": username
                },
                id=f"word_split_{task_id}_{current_index}", 
                replace_existing=True
            )
        except Exception:
            if lock.locked(): lock.release()
            logger.exception("Error resuming task")
            send_message(user_id, "❌ Failed to resume due to an internal error.")
        finally:
            pass 

        return jsonify({"ok": True}) 
        
    if command == "/status": 
        active = get_running_task_for_user(user_id)
        queued = get_queue_size(user_id)

        if active: 
            status, total_words, current_index = active["status"], active["total_words"], active["current_index"]
            send_message(user_id, f"📊 **Current Status Check**\nStatus: {status}\nRemaining words: {total_words - current_index}\nQueue size: {queued}") 
        else: 
            if queued > 0: 
                send_message(user_id, f"📝 **Status: Waiting.** Your first task is waiting in line. Queue size: {queued}") 
            else: 
                send_message(user_id, "📊 You have no active or queued tasks.") 
        return jsonify({"ok": True}) 
        
    if command == "/stop": 
        stopped = cancel_active_task_for_user(user_id)
        
        release_user_lock_if_held(user_id)
        
        if stopped > 0:
            send_message(user_id, f"🛑 **Active Task Stopped!** All your queued tasks have also been cleared.")
        else:
            send_message(user_id, "ℹ️ You had no active or queued tasks.") 
        return jsonify({"ok": True}) 

    if command == "/stats": 
        cutoff = datetime.utcnow() - timedelta(hours=12) 
        rows = db_execute("SELECT SUM(words) FROM split_logs WHERE user_id = %s AND created_at >= %s", (user_id, cutoff), fetch=True) 
        words = int(rows[0][0] or 0) 
        send_message(user_id, f"🕰️ **Your Recent Activity (Last 12 Hours)**\nYou have split **{words}** words! 💪 📝") 
        return jsonify({"ok": True}) 

    if command == "/adduser": 
        if not is_admin(user_id): send_message(user_id, "❌ You are not allowed to use this command.") ; return jsonify({"ok": True}) 
        if not args: send_message(user_id, "Usage: /adduser <telegram_user_id> [username]") ; return jsonify({"ok": True}) 
        parts = args.split() 
        try: target_id = int(parts[0]) 
        except Exception: send_message(user_id, "Invalid user id. Must be numeric.") ; return jsonify({"ok": True}) 
        uname = parts[1] if len(parts) > 1 else "" 
        count = db_execute("SELECT COUNT(*) FROM allowed_users", fetch=True)[0][0] 
        if count >= MAX_ALLOWED_USERS: send_message(user_id, f"Cannot add more users. Max allowed users: {MAX_ALLOWED_USERS}") ; return jsonify({"ok": True}) 
        db_execute("INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (%s, %s, NOW(), FALSE) ON CONFLICT (user_id) DO NOTHING", (target_id, uname)) 
        send_message(user_id, f"✅ User {target_id} added to allowed list.") 
        send_message(target_id, "✅ You have been added to the Word Splitter bot. Send any text to start.") 
        return jsonify({"ok": True}) 
        
    if command == "/removeuser": 
        if not is_admin(user_id): send_message(user_id, "❌ You are not allowed to use this command.") ; return jsonify({"ok": True}) 
        if not args: send_message(user_id, "Usage: /removeuser <telegram_user_id>") ; return jsonify({"ok": True}) 
        try: target_id = int(args.split()[0]) 
        except Exception: send_message(user_id, "Invalid user id.") ; return jsonify({"ok": True}) 
        db_execute("DELETE FROM allowed_users WHERE user_id = %s", (target_id,)) 
        send_message(user_id, f"✅ User {target_id} removed from allowed list.") 
        return jsonify({"ok": True}) 
        
    if command == "/listusers": 
        if not is_admin(user_id): send_message(user_id, "❌ You are not allowed to use this command.") ; return jsonify({"ok": True}) 
        rows = db_execute("SELECT user_id, username, is_admin, added_at FROM allowed_users", fetch=True) 
        lines = [] 
        for r in rows: 
             # Check if r[3] is a datetime object, convert it to isoformat if it is
            date_str = r[3].isoformat() if isinstance(r[3], datetime) else str(r[3])
            lines.append(f"{r[0]} @{r[1] or ''} admin={r[2]} added={date_str}") 
        body = "Allowed users:\n" + ("\n".join(lines) if lines else "(none)") 
        send_message(user_id, body) 
        return jsonify({"ok": True}) 
        
    if command == "/botinfo": 
        if user_id != OWNER_ID: send_message(user_id, "❌ Only the bot owner can use /botinfo") ; return jsonify({"ok": True}) 
        total_allowed = db_execute("SELECT COUNT(*) FROM allowed_users", fetch=True)[0][0] 
        active_tasks = db_execute("SELECT COUNT(*) FROM tasks WHERE status IN ('running','paused')", fetch=True)[0][0] 
        queued_tasks = db_execute("SELECT COUNT(*) FROM tasks WHERE status = 'queued'", fetch=True)[0][0] 
        rows = db_execute("SELECT user_id, SUM(words) FROM split_logs WHERE created_at >= NOW() - INTERVAL '2 hours' GROUP BY user_id", fetch=True) 
        peruser_lines = [] 
        for r in rows: peruser_lines.append(f"{r[0]} - {r[1] or 0}") 
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
        if user_id != OWNER_ID: send_message(user_id, "❌ Only owner can broadcast") ; return jsonify({"ok": True}) 
        if not args: send_message(user_id, "Usage: /broadcast <message>") ; return jsonify({"ok": True}) 
        rows = db_execute("SELECT user_id FROM allowed_users", fetch=True) 
        count = 0 
        fails = 0 
        for r in rows: 
            try: 
                send_message(r[0], f"📣 Broadcast from owner:\n\n{args}") 
                count += 1 
            except Exception: 
                fails += 1 
        send_message(user_id, f"Broadcast complete. Success: {count}, Failed: {fails}") 
        return jsonify({"ok": True}) 
        
    send_message(user_id, "Unknown command.") 
    return jsonify({"ok": True}) 


def handle_new_text(user_id: int, username: str, text: str):
    if not is_allowed(user_id) or is_maintenance_now():
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
    
    if qsize == 1:
        # CRITICAL START: Start the first task immediately
        threading.Thread(target=_start_next_queued_task_if_exists, args=(user_id, user_id, username), daemon=True).start()
        send_message(user_id, f"🚀 Task queued and **starting now**. Words: {res['total_words']}. Time per word: {compute_interval(res['total_words'])}s")
    else:
        send_message(user_id, f"📝 Queued! You currently have {qsize} task(s) waiting in line. Your current task must finish first.")
        
    return jsonify({"ok": True})

# --- Health and Root Endpoints ---

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
    try:
        db_execute("SELECT 1", fetch=True)
        return jsonify({"ok": True, "time": datetime.utcnow().isoformat()}), 200
    except Exception:
        logger.exception("Health check failed due to DB connection error.")
        return jsonify({"ok": False, "error": "Database connection failed"}), 500

if __name__ == "__main__":
    try:
        set_webhook()
    except Exception:
        logger.exception("Failed to set webhook at startup")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
