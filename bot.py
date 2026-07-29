#!/usr/bin/env python3
"""
================================================================================
 Enterprise Telegram Collage Creator Bot & System Maintenance Suite
================================================================================
"""

import os
import sys
import io
import gc
import math
import time
import shutil
import glob
import logging
import sqlite3
import asyncio
import tempfile
import traceback
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Dict, Any, Optional, Set, Union
from PIL import Image, ImageOps, ImageDraw, ImageFont

from telegram import (
    Update,
    User,
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.error import TelegramError, BadRequest, Forbidden, NetworkError


# ==============================================================================
# SECTION 1: GLOBAL CONFIGURATION, PATHS & LOGGING SYSTEM
# ==============================================================================

BASE_DIR = Path(__file__).resolve().parent
TEMP_DIR = BASE_DIR / "tmp_bot_garbage"
LOG_DIR = BASE_DIR / "logs"

# Ensure directories exist
TEMP_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FORMAT = "%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("CollageBotSuite")

# Core Environment Configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", "8824882366:AAFwQPwk3CZZ2XPZkY_LoGw7unb103sCulk")
ADMIN_ID = int(os.getenv("ADMIN_ID", "2077444542"))
DB_FILE = str(BASE_DIR / "collage_bot_v2.db")

# Collage & Billing Constants
MAX_COLLAGE_LIMIT = 12
DEFAULT_FREE_TRIALS = 3
COIN_COST_PER_COLLAGE = 1

# Garbage Collection & Retention Constants
GARBAGE_BUFFER_TTL_HOURS = 2    # Unfinished upload queues older than 2 hrs are garbage
GARBAGE_LOG_TTL_DAYS = 30        # Activity logs older than 30 days are purged
GARBAGE_TEMP_FILE_TTL_MINS = 30  # Orphaned disk temp files older than 30 mins are deleted
GARBAGE_RUN_INTERVAL_SEC = 300   # Auto GC background job runs every 5 minutes

# Layout Visual Specs
SLOT_HEIGHT = 1000
GAP_SIZE = 8
BG_COLOR = (240, 240, 240)

# Message Status Tracker (Keeps track of message IDs for editing update counts)
USER_STATUS_MSGS: Dict[int, int] = {}


# ==============================================================================
# SECTION 2: DATABASE PERSISTENCE LAYER WITH GC SUPPORT
# ==============================================================================

class DatabaseManager:
    """
    Handles SQLite transactions, user entitlements, buffered file queues,
    audit activity logging, and database garbage collection routines.
    """

    def __init__(self, db_path: str = DB_FILE):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _init_db(self) -> None:
        """Initializes relational schema with index optimizations."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Users table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    free_trials INTEGER DEFAULT 3,
                    coins INTEGER DEFAULT 0,
                    subscription_until TEXT DEFAULT NULL,
                    created_at TEXT NOT NULL,
                    last_active TEXT NOT NULL
                )
                """
            )

            # Activity logs table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS activity_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
                )
                """
            )

            # Buffered photo storage queue
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS photo_buffer (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    file_id TEXT NOT NULL,
                    file_path TEXT DEFAULT NULL,
                    added_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
                )
                """
            )

            # Garbage Collection Metrics Audit table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS gc_audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_timestamp TEXT NOT NULL,
                    stale_buffers_deleted INTEGER NOT NULL,
                    old_logs_purged INTEGER NOT NULL,
                    temp_files_removed INTEGER NOT NULL,
                    bytes_reclaimed INTEGER NOT NULL,
                    execution_time_ms REAL NOT NULL
                )
                """
            )

            # Create Indexes for query performance & GC sweeping
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_photo_buffer_user ON photo_buffer(user_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_photo_buffer_time ON photo_buffer(added_at);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_time ON activity_logs(timestamp);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_sub ON users(subscription_until);")

            conn.commit()
            logger.info("Database initialized successfully with garbage auditing schemas.")

    def register_user_if_not_exists(self, user: User) -> Dict[str, Any]:
        """Registers or updates user active status."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE user_id = ?", (user.id,))
            row = cursor.fetchone()

            now_iso = datetime.now(timezone.utc).isoformat()

            if not row:
                cursor.execute(
                    """
                    INSERT INTO users (user_id, username, first_name, free_trials, coins, subscription_until, created_at, last_active)
                    VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
                    """,
                    (user.id, user.username, user.first_name, DEFAULT_FREE_TRIALS, 0, now_iso, now_iso),
                )
                conn.commit()
                self.log_activity(user.id, "Registered new account.")
                return self.get_user(user.id)
            else:
                cursor.execute("UPDATE users SET last_active = ? WHERE user_id = ?", (now_iso, user.id))
                conn.commit()

            return dict(row)

    def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def update_coins(self, user_id: int, delta: int) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            if not row:
                return 0

            new_coins = max(0, row["coins"] + delta)
            cursor.execute("UPDATE users SET coins = ? WHERE user_id = ?", (new_coins, user_id))
            conn.commit()

            self.log_activity(user_id, f"Coins modified by {delta}. New balance: {new_coins}")
            return new_coins

    def set_subscription(self, user_id: int, days: int) -> datetime:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT subscription_until FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()

            now_utc = datetime.now(timezone.utc)
            base_time = now_utc

            if row and row["subscription_until"]:
                try:
                    existing_expiry = datetime.fromisoformat(row["subscription_until"])
                    if existing_expiry > now_utc:
                        base_time = existing_expiry
                except ValueError:
                    pass

            new_expiry = base_time + timedelta(days=days)
            cursor.execute(
                "UPDATE users SET subscription_until = ? WHERE user_id = ?",
                (new_expiry.isoformat(), user_id),
            )
            conn.commit()

            self.log_activity(user_id, f"Subscription extended by {days}d. Expires: {new_expiry.isoformat()}")
            return new_expiry

    def expire_subscription(self, user_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET subscription_until = NULL WHERE user_id = ?", (user_id,))
            conn.commit()
            self.log_activity(user_id, "Subscription cancelled/expired by admin.")
            return True

    def deduct_trial_or_coin(self, user_id: int, cost: int) -> Tuple[bool, str]:
        user = self.get_user(user_id)
        if not user:
            return False, "User not found"

        sub_until = user.get("subscription_until")
        now_utc = datetime.now(timezone.utc)
        if sub_until:
            try:
                expiry = datetime.fromisoformat(sub_until)
                if expiry > now_utc:
                    self.log_activity(user_id, "Generated collage using subscription.")
                    return True, "subscription"
            except ValueError:
                pass

        trials = user.get("free_trials", 0)
        coins = user.get("coins", 0)

        if trials >= cost:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE users SET free_trials = free_trials - ? WHERE user_id = ?",
                    (cost, user_id),
                )
                conn.commit()
            self.log_activity(user_id, f"Deducted {cost} free trial(s). Remaining: {trials - cost}")
            return True, "free_trials"

        if coins >= cost:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE users SET coins = coins - ? WHERE user_id = ?", (cost, user_id)
                )
                conn.commit()
            self.log_activity(user_id, f"Deducted {cost} coin(s). Remaining: {coins - cost}")
            return True, "coins"

        return False, "Insufficient balance"

    def add_photo_to_buffer(self, user_id: int, file_id: str, file_path: Optional[str] = None) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now_iso = datetime.now(timezone.utc).isoformat()
            cursor.execute(
                "INSERT INTO photo_buffer (user_id, file_id, file_path, added_at) VALUES (?, ?, ?, ?)",
                (user_id, file_id, file_path, now_iso),
            )
            conn.commit()
            cursor.execute("SELECT COUNT(*) as cnt FROM photo_buffer WHERE user_id = ?", (user_id,))
            return cursor.fetchone()["cnt"]

    def get_photo_buffer(self, user_id: int) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, file_id, file_path, added_at FROM photo_buffer WHERE user_id = ? ORDER BY id ASC",
                (user_id,),
            )
            return [dict(r) for r in cursor.fetchall()]

    def clear_photo_buffer(self, user_id: int) -> Tuple[int, List[str]]:
        """Clears buffer and returns deleted local disk file paths for physical garbage removal."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT file_path FROM photo_buffer WHERE user_id = ? AND file_path IS NOT NULL", (user_id,))
            paths = [r["file_path"] for r in cursor.fetchall() if r["file_path"]]

            cursor.execute("DELETE FROM photo_buffer WHERE user_id = ?", (user_id,))
            deleted_count = cursor.rowcount
            conn.commit()

            return deleted_count, paths

    def log_activity(self, user_id: int, action: str) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now_iso = datetime.now(timezone.utc).isoformat()
            cursor.execute(
                "INSERT INTO activity_logs (user_id, action, timestamp) VALUES (?, ?, ?)",
                (user_id, action, now_iso),
            )
            conn.commit()

    def get_activity_history(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT action, timestamp FROM activity_logs WHERE user_id = ? ORDER BY id DESC LIMIT ?",
                (user_id, limit),
            )
            return [dict(r) for r in cursor.fetchall()]

    # --------------------------------------------------------------------------
    # DATABASE GARBAGE COLLECTION QUERIES
    # --------------------------------------------------------------------------

    def purge_stale_photo_buffers(self, ttl_hours: int = GARBAGE_BUFFER_TTL_HOURS) -> Tuple[int, List[str]]:
        """Purges unsubmitted user upload queues older than TTL hours."""
        threshold_iso = (datetime.now(timezone.utc) - timedelta(hours=ttl_hours)).isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT file_path FROM photo_buffer WHERE added_at < ? AND file_path IS NOT NULL", (threshold_iso,))
            disk_paths = [r["file_path"] for r in cursor.fetchall() if r["file_path"]]

            cursor.execute("DELETE FROM photo_buffer WHERE added_at < ?", (threshold_iso,))
            purged_count = cursor.rowcount
            conn.commit()

            return purged_count, disk_paths

    def purge_old_activity_logs(self, ttl_days: int = GARBAGE_LOG_TTL_DAYS) -> int:
        """Purges historic activity log records older than TTL days."""
        threshold_iso = (datetime.now(timezone.utc) - timedelta(days=ttl_days)).isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM activity_logs WHERE timestamp < ?", (threshold_iso,))
            purged_count = cursor.rowcount
            conn.commit()
            return purged_count

    def log_gc_run(
        self,
        stale_buffers: int,
        old_logs: int,
        temp_files: int,
        bytes_reclaimed: int,
        exec_ms: float,
    ) -> None:
        """Records Garbage Collector run statistics into audit table."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now_iso = datetime.now(timezone.utc).isoformat()
            cursor.execute(
                """
                INSERT INTO gc_audit_logs 
                (run_timestamp, stale_buffers_deleted, old_logs_purged, temp_files_removed, bytes_reclaimed, execution_time_ms)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (now_iso, stale_buffers, old_logs, temp_files, bytes_reclaimed, exec_ms),
            )
            conn.commit()

    def get_gc_stats(self) -> Dict[str, Any]:
        """Fetches total garbage collection metrics across bot lifetime."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT 
                    COUNT(*) as total_runs,
                    SUM(stale_buffers_deleted) as total_buffers_cleaned,
                    SUM(old_logs_purged) as total_logs_purged,
                    SUM(temp_files_removed) as total_temp_files_removed,
                    SUM(bytes_reclaimed) as total_bytes_reclaimed
                FROM gc_audit_logs
                """
            )
            row = cursor.fetchone()
            return {
                "total_runs": row["total_runs"] or 0,
                "total_buffers_cleaned": row["total_buffers_cleaned"] or 0,
                "total_logs_purged": row["total_logs_purged"] or 0,
                "total_temp_files_removed": row["total_temp_files_removed"] or 0,
                "total_bytes_reclaimed": row["total_bytes_reclaimed"] or 0,
            }

    def vacuum_database(self) -> None:
        """Executes SQLite VACUUM to defragment database storage and reclaim disk space."""
        with self._get_connection() as conn:
            conn.execute("VACUUM;")
            logger.info("SQLite database VACUUM executed successfully.")


db = DatabaseManager()
# ==============================================================================
# SECTION 3: AUTOMATED GARBAGE COLLECTION & SYSTEM CLEANUP ENGINE
# ==============================================================================

class GarbageCollector:
    """
    Multi-tiered automated system maintenance and garbage collection subsystem:
    1. Disk Garbage Collector (Orphaned temp files, stale cache files)
    2. Session Garbage Collector (Abandoned photo buffers > 2 hours)
    3. Database Garbage Collector (Pruning logs > 30 days, WAL truncation, VACUUM)
    4. Memory Garbage Collector (Explicit Python GC calls & PIL reference sweeps)
    """

    def __init__(self, temp_directory: Path = TEMP_DIR):
        self.temp_directory = temp_directory
        self.is_running = False

    def clean_disk_garbage(self, max_age_minutes: int = GARBAGE_TEMP_FILE_TTL_MINS) -> Tuple[int, int]:
        """Sweeps temp directory and removes orphaned images/files older than max_age_minutes."""
        files_removed = 0
        bytes_reclaimed = 0
        now = time.time()
        cutoff = now - (max_age_minutes * 60)

        try:
            for item in self.temp_directory.glob("*"):
                if item.is_file():
                    file_stat = item.stat()
                    if file_stat.st_mtime < cutoff:
                        file_size = file_stat.st_size
                        try:
                            item.unlink()
                            files_removed += 1
                            bytes_reclaimed += file_size
                            logger.debug(f"Garbage Collector unlinked disk file: {item.name}")
                        except Exception as err:
                            logger.warning(f"Failed to unlink temp file {item}: {err}")
        except Exception as e:
            logger.error(f"Error during disk garbage sweep: {e}")

        return files_removed, bytes_reclaimed

    def clean_session_garbage(self) -> Tuple[int, int]:
        """Cleans stale user photo buffer entries from database and unlinks associated temp files."""
        stale_buffers_count, orphan_file_paths = db.purge_stale_photo_buffers(ttl_hours=GARBAGE_BUFFER_TTL_HOURS)
        bytes_reclaimed = 0

        for path_str in orphan_file_paths:
            if path_str:
                p = Path(path_str)
                if p.exists() and p.is_file():
                    try:
                        bytes_reclaimed += p.stat().st_size
                        p.unlink()
                    except Exception as err:
                        logger.warning(f"Failed unlinking stale buffer file {p}: {err}")

        return stale_buffers_count, bytes_reclaimed

    def clean_database_garbage(self) -> int:
        """Purges old activity logs beyond configured retention limit."""
        return db.purge_old_activity_logs(ttl_days=GARBAGE_LOG_TTL_DAYS)

    def clean_memory_garbage(self) -> Dict[str, int]:
        """Triggers Python runtime garbage collection and returns uncollected object counts."""
        unreachable_before = gc.get_count()
        collected = gc.collect()
        unreachable_after = gc.get_count()

        return {
            "collected_objects": collected,
            "gen0": unreachable_after[0],
            "gen1": unreachable_after[1],
            "gen2": unreachable_after[2],
        }

    def run_full_garbage_collection(self) -> Dict[str, Any]:
        """
        Executes a comprehensive, full-spectrum Garbage Collection cycle.
        Measures execution time and logs audit entries.
        """
        start_time = time.perf_counter()
        logger.info("🧹 Starting Garbage Collection Cycle...")

        # Tier 1: Session GC
        stale_buffers, buffer_bytes = self.clean_session_garbage()

        # Tier 2: Disk GC
        temp_files, disk_bytes = self.clean_disk_garbage()

        # Tier 3: Database GC
        purged_logs = self.clean_database_garbage()

        # Tier 4: Memory GC
        mem_stats = self.clean_memory_garbage()

        total_bytes = buffer_bytes + disk_bytes
        elapsed_ms = (time.perf_counter() - start_time) * 1000.0

        # Audit persistence
        db.log_gc_run(
            stale_buffers=stale_buffers,
            old_logs=purged_logs,
            temp_files=temp_files,
            bytes_reclaimed=total_bytes,
            exec_ms=elapsed_ms,
        )

        logger.info(
            f"✨ Garbage Collection Complete in {elapsed_ms:.2f}ms | "
            f"Stale Buffers: {stale_buffers} | Temp Files: {temp_files} | "
            f"Logs Purged: {purged_logs} | Bytes Reclaimed: {total_bytes / (1024*1024):.2f} MB"
        )

        return {
            "stale_buffers_deleted": stale_buffers,
            "temp_files_removed": temp_files,
            "old_logs_purged": purged_logs,
            "bytes_reclaimed": total_bytes,
            "execution_time_ms": elapsed_ms,
            "memory_stats": mem_stats,
        }

    async def start_scheduled_garbage_collector(self, interval_seconds: int = GARBAGE_RUN_INTERVAL_SEC) -> None:
        """Background asynchronous task that periodically executes Garbage Collection."""
        self.is_running = True
        logger.info(f"Garbage Collector Background Worker initialized (Interval: {interval_seconds}s).")

        while self.is_running:
            try:
                await asyncio.sleep(interval_seconds)
                # Run GC in background executor thread to avoid blocking Telegram async event loop
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.run_full_garbage_collection)
            except asyncio.CancelledError:
                logger.info("Garbage Collector background loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in Garbage Collector scheduled task: {e}", exc_info=True)


gc_engine = GarbageCollector()


# ==============================================================================
# SECTION 4: ADVANCED HIGH-QUALITY COLLAGE GENERATION ENGINE
# ==============================================================================

class CollageEngine:
    """
    Renders high-definition image collages dynamically using requested rules.
    """

    @staticmethod
    def get_layout_structure(num_images: int) -> List[int]:
        layout_matrix = {
            1: [1],
            2: [2],
            3: [3],
            4: [2, 2],
            5: [3, 2],
            6: [3, 3],
            7: [4, 3],
            8: [4, 4],
            9: [5, 4],
            10: [5, 5],
            11: [6, 5],
            12: [4, 4, 4],
        }
        return layout_matrix.get(num_images, [num_images])

    @classmethod
    def render_collage(cls, image_bytes_list: List[bytes]) -> io.BytesIO:
        num_images = len(image_bytes_list)
        if num_images == 0:
            raise ValueError("Cannot create collage from empty image list.")
        if num_images > MAX_COLLAGE_LIMIT:
            raise ValueError(f"Exceeded max limit of {MAX_COLLAGE_LIMIT} images per collage.")

        pil_images: List[Image.Image] = []
        for raw in image_bytes_list:
            try:
                img = Image.open(io.BytesIO(raw)).convert("RGB")
                pil_images.append(img)
            except Exception as e:
                logger.error(f"Failed to decode image byte buffer: {e}")

        if not pil_images:
            raise RuntimeError("All supplied image buffers were unreadable or corrupt.")

        row_counts = cls.get_layout_structure(len(pil_images))
        num_rows = len(row_counts)
        max_cols = max(row_counts)

        # Dynamic Aspect Ratio Math
        aspect_ratios = [img.height / img.width for img in pil_images if img.width > 0]
        avg_aspect_ratio = sum(aspect_ratios) / len(aspect_ratios) if aspect_ratios else 1.77

        slot_width = int(SLOT_HEIGHT / avg_aspect_ratio)

        total_canvas_width = (max_cols * slot_width) + ((max_cols + 1) * GAP_SIZE)
        total_canvas_height = (num_rows * SLOT_HEIGHT) + ((num_rows + 1) * GAP_SIZE)

        canvas = Image.new("RGB", (total_canvas_width, total_canvas_height), BG_COLOR)

        image_idx = 0
        current_y_offset = GAP_SIZE

        for row_idx, items_in_row in enumerate(row_counts):
            row_width = (items_in_row * slot_width) + ((items_in_row - 1) * GAP_SIZE)
            row_start_x = (total_canvas_width - row_width) // 2

            for col_idx in range(items_in_row):
                if image_idx >= len(pil_images):
                    break

                source_img = pil_images[image_idx]

                fitted_img = ImageOps.contain(
                    source_img,
                    (slot_width, SLOT_HEIGHT),
                    method=Image.Resampling.LANCZOS,
                )

                cell_left_x = row_start_x + col_idx * (slot_width + GAP_SIZE)
                cell_top_y = current_y_offset

                center_x = cell_left_x + (slot_width - fitted_img.width) // 2
                center_y = cell_top_y + (SLOT_HEIGHT - fitted_img.height) // 2

                canvas.paste(fitted_img, (center_x, center_y))
                image_idx += 1

            current_y_offset += SLOT_HEIGHT + GAP_SIZE

        output_buffer = io.BytesIO()
        canvas.save(output_buffer, format="JPEG", quality=95, optimize=True)
        output_buffer.seek(0)

        # Close Pillow images to free memory immediately
        for img in pil_images:
            img.close()

        return output_buffer
# ==============================================================================
# SECTION 5: USER TELEGRAM COMMAND HANDLERS
# ==============================================================================

def format_subscription_status(user_data: Dict[str, Any]) -> str:
    sub_until = user_data.get("subscription_until")
    if not sub_until:
        return "❌ Inactive"

    try:
        expiry_dt = datetime.fromisoformat(sub_until)
        now_utc = datetime.now(timezone.utc)

        if expiry_dt <= now_utc:
            return "❌ Expired"

        remaining = expiry_dt - now_utc
        days = remaining.days
        hours = remaining.seconds // 3600
        minutes = (remaining.seconds % 3600) // 60

        formatted_date = expiry_dt.strftime("%d %b %Y at %H:%M:%S UTC")
        return f"✅ Active\n  • **Expires**: `{formatted_date}`\n  • **Time Remaining**: `{days}d {hours}h {minutes}m`"
    except Exception:
        return "❌ Invalid Expiry Format"


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message:
        return

    db_user = db.register_user_if_not_exists(user)
    sub_status = format_subscription_status(db_user)

    welcome_text = (
        f"👋 **Welcome to Collage Creator Bot, {user.first_name}!**\n\n"
        f"📷 Send photos individually or in batches.\n"
        f"⚡ Type `/done` when finished to generate your collage.\n"
        f"🧹 Send `/clear` to reset your uploaded photo queue.\n\n"
        f"💳 **Your Profile Balance**:\n"
        f"• 🎁 **Free Trials Remaining**: `{db_user['free_trials']}`\n"
        f"• 🪙 **Coins Balance**: `{db_user['coins']}`\n"
        f"• 👑 **Subscription**: {sub_status}\n\n"
        f"♻️ *Note: Unfinished upload sessions are automatically cleaned by Garbage Collection after {GARBAGE_BUFFER_TTL_HOURS} hours.*"
    )

    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📜 Account Status", callback_data="check_status")],
            [InlineKeyboardButton("ℹ️ Layout Guide", callback_data="show_help")],
        ]
    )
    await update.message.reply_text(welcome_text, parse_mode="Markdown", reply_markup=keyboard)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    help_text = (
        "📖 **Collage Layout Grid Rules**:\n\n"
        "• 2 photos  → 2×1\n"
        "• 3 photos  → Single Line 3\n"
        "• 4 photos  → 2×2\n"
        "• 5 photos  → 3 top / 2 bottom\n"
        "• 6 photos  → 3×2\n"
        "• 7 photos  → 4 top / 3 bottom\n"
        "• 8 photos  → 4×2\n"
        "• 9 photos  → 5 top / 4 bottom\n"
        "• 10 photos → 5×2\n"
        "• 11 photos → 6 top / 5 bottom\n"
        "• 12 photos → 4×3\n\n"
        "🔢 Uploading >12 photos will automatically split your request into multiple collages!"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message:
        return

    db_user = db.register_user_if_not_exists(user)
    sub_status = format_subscription_status(db_user)
    pending = len(db.get_photo_buffer(user.id))

    status_text = (
        f"👤 **Account Profile**: `{user.id}`\n\n"
        f"🎁 **Free Trials**: `{db_user['free_trials']}`\n"
        f"🪙 **Coins Balance**: `{db_user['coins']}`\n"
        f"👑 **Subscription**: {sub_status}\n\n"
        f"📥 **Buffered Upload Queue**: `{pending}` photo(s)"
    )
    await update.message.reply_text(status_text, parse_mode="Markdown")


async def clear_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message:
        return

    deleted_count, disk_paths = db.clear_photo_buffer(user.id)
    USER_STATUS_MSGS.pop(user.id, None)

    for path in disk_paths:
        try:
            p = Path(path)
            if p.exists():
                p.unlink()
        except Exception as e:
            logger.warning(f"Failed removing cleared photo file {path}: {e}")

    await update.message.reply_text(f"🧹 Cleared `{deleted_count}` buffered photo(s) from memory and disk.", parse_mode="Markdown")


async def photo_receiver(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.photo:
        return

    db.register_user_if_not_exists(user)
    photo_item = update.message.photo[-1]

    # Download file locally
    file_obj = await photo_item.get_file()
    temp_file_path = TEMP_DIR / f"upload_{user.id}_{int(time.time()*1000)}.jpg"
    await file_obj.download_to_drive(custom_path=temp_file_path)

    count = db.add_photo_to_buffer(user.id, photo_item.file_id, str(temp_file_path))

    msg_text = f"📷 **Photos received:** `{count}` in buffer.\n\nSend more or type `/done` when finished!"

    # Edit the existing message to update total count instead of sending a new message repeatedly
    last_msg_id = USER_STATUS_MSGS.get(user.id)
    if last_msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=last_msg_id,
                text=msg_text,
                parse_mode="Markdown",
            )
            return
        except Exception:
            pass  # Fall back to sending a new message if editing fails

    sent_msg = await update.message.reply_text(msg_text, parse_mode="Markdown")
    USER_STATUS_MSGS[user.id] = sent_msg.message_id


async def done_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message:
        return

    db_user = db.register_user_if_not_exists(user)
    buffer_items = db.get_photo_buffer(user.id)

    if not buffer_items:
        await update.message.reply_text("❌ No photos found in upload buffer. Send photos first!")
        return

    # Clear status tracker message
    USER_STATUS_MSGS.pop(user.id, None)

    total_photos = len(buffer_items)
    required_collages = math.ceil(total_photos / MAX_COLLAGE_LIMIT)

    has_access, payment_method = db.deduct_trial_or_coin(user.id, required_collages)

    if not has_access:
        await update.message.reply_text(
            f"⚠️ **Insufficient Balance!**\n\n"
            f"• Uploaded photos: `{total_photos}`\n"
            f"• Required collages: `{required_collages}`\n"
            f"• Trials Left: `{db_user['free_trials']}` | Coins: `{db_user['coins']}`\n\n"
            f"Contact Admin to purchase coins or subscriptions!",
            parse_mode="Markdown",
        )
        return

    status_msg = await update.message.reply_text(f"⚙️ Processing {total_photos} photo(s) into `{required_collages}` collage(s)...", parse_mode="Markdown")

    chunks = [buffer_items[i:i + MAX_COLLAGE_LIMIT] for i in range(0, total_photos, MAX_COLLAGE_LIMIT)]

    try:
        for idx, chunk in enumerate(chunks, start=1):
            image_bytes_list: List[bytes] = []

            await status_msg.edit_text(f"⚙️ Rendering **Collage {idx} of {len(chunks)}**...", parse_mode="Markdown")

            for item in chunk:
                local_path = item.get("file_path")
                # Try reading from disk first
                if local_path and Path(local_path).exists():
                    try:
                        with open(local_path, "rb") as f:
                            image_bytes_list.append(f.read())
                        continue
                    except Exception as err:
                        logger.warning(f"Failed reading disk file {local_path}: {err}")

                # Download with retry mechanism to prevent network timeouts
                for attempt in range(3):
                    try:
                        f_obj = await context.bot.get_file(item["file_id"], read_timeout=60, write_timeout=60)
                        b_data = await f_obj.download_as_bytearray()
                        image_bytes_list.append(bytes(b_data))
                        break
                    except (NetworkError, TelegramError) as dl_err:
                        if attempt == 2:
                            logger.error(f"Failed downloading file_id {item['file_id']} after 3 attempts: {dl_err}")
                        await asyncio.sleep(2)

            if not image_bytes_list:
                logger.warning(f"Skipping empty or unreadable collage chunk {idx}")
                continue

            collage_jpeg = CollageEngine.render_collage(image_bytes_list)

            # Send collage with custom timeout
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=collage_jpeg,
                caption=f"✨ **Collage Part {idx} of {len(chunks)}** ({len(image_bytes_list)} photos)",
                parse_mode="Markdown",
                read_timeout=60,
                write_timeout=60,
            )

            # Small pause between chunks to keep connection stable
            await asyncio.sleep(2)

        # Clear buffer only after all parts are rendered
        _, disk_paths = db.clear_photo_buffer(user.id)
        for path_str in disk_paths:
            if path_str:
                p = Path(path_str)
                if p.exists():
                    try:
                        p.unlink()
                    except Exception as unl_err:
                        logger.warning(f"Failed cleaning temp file {p}: {unl_err}")

        gc_engine.clean_memory_garbage()
        await status_msg.edit_text("✅ All collages generated and delivered successfully!")

    except Exception as e:
        logger.error(f"Error producing collage for user {user.id}: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Failed to generate collage. Details: `{str(e)}`", parse_mode="Markdown")
# ==============================================================================
# SECTION 6: ADMIN PANEL & GARBAGE COLLECTION CONTROL COMMANDS
# ==============================================================================

def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user or user.id != ADMIN_ID:
            if update.message:
                await update.message.reply_text("⛔ **Unauthorized Access**: Admin only command.")
            return
        return await func(update, context)
    return wrapper


@admin_only
async def addcoins_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        target_id = int(context.args[0])
        amount = int(context.args[1])
        new_bal = db.update_coins(target_id, amount)
        await update.message.reply_text(f"✅ Added {amount} coins to `{target_id}`. New Balance: `{new_bal}`", parse_mode="Markdown")
    except Exception:
        await update.message.reply_text("Usage: `/addcoins <user_id> <amount>`", parse_mode="Markdown")


@admin_only
async def removecoins_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        target_id = int(context.args[0])
        amount = int(context.args[1])
        new_bal = db.update_coins(target_id, -amount)
        await update.message.reply_text(f"✅ Removed coins from `{target_id}`. New Balance: `{new_bal}`", parse_mode="Markdown")
    except Exception:
        await update.message.reply_text("Usage: `/removecoins <user_id> <amount>`", parse_mode="Markdown")


@admin_only
async def addsub_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    plans = {"1d": 1, "3d": 3, "7d": 7, "31d": 31, "365d": 365}
    try:
        target_id = int(context.args[0])
        plan_str = context.args[1].lower() if len(context.args) > 1 else "31d"

        if plan_str not in plans:
            await update.message.reply_text(f"Invalid plan. Options: {', '.join(plans.keys())}")
            return

        days = plans[plan_str]
        new_expiry = db.set_subscription(target_id, days)
        formatted_date = new_expiry.strftime("%Y-%m-%d %H:%M:%S UTC")

        await update.message.reply_text(
            f"🎉 **Subscription Granted!**\n"
            f"• **User ID**: `{target_id}`\n"
            f"• **Plan**: `{days} Days`\n"
            f"• **Exact End Time**: `{formatted_date}`",
            parse_mode="Markdown",
        )
    except Exception:
        await update.message.reply_text("Usage: `/addsub <user_id> [1d|3d|7d|31d|365d]`", parse_mode="Markdown")


@admin_only
async def removesub_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        target_id = int(context.args[0])
        db.expire_subscription(target_id)
        await update.message.reply_text(f"🛑 Subscription immediately expired for user `{target_id}`.", parse_mode="Markdown")
    except Exception:
        await update.message.reply_text("Usage: `/removesub <user_id>`", parse_mode="Markdown")


@admin_only
async def userinfo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        target_id = int(context.args[0])
        u_data = db.get_user(target_id)
        if not u_data:
            await update.message.reply_text("❌ User not found.")
            return

        sub_desc = format_subscription_status(u_data)
        logs = db.get_activity_history(target_id, limit=6)
        log_str = "\n".join([f"  • `[{l['timestamp'][:16]}]` {l['action']}" for l in logs]) if logs else "  • No history."

        msg = (
            f"👤 **User Full Profile**: `{target_id}`\n\n"
            f"💰 **Coins**: `{u_data['coins']}`\n"
            f"🎁 **Free Trials**: `{u_data['free_trials']}`\n"
            f"👑 **Subscription**:\n{sub_desc}\n\n"
            f"📜 **Activity Log History**:\n{log_str}"
        )
        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception:
        await update.message.reply_text("Usage: `/userinfo <user_id>`", parse_mode="Markdown")


@admin_only
async def run_gc_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """`/clean_garbage` - Forces manual Garbage Collection sweep immediately."""
    status_msg = await update.message.reply_text("🧹 Executing manual Garbage Collection sweep...")

    loop = asyncio.get_running_loop()
    res = await loop.run_in_executor(None, gc_engine.run_full_garbage_collection)

    bytes_mb = res["bytes_reclaimed"] / (1024 * 1024)
    msg = (
        f"✨ **Garbage Collection Sweep Complete!**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"• **Stale Sessions Cleaned**: `{res['stale_buffers_deleted']}`\n"
        f"• **Temp Files Unlinked**: `{res['temp_files_removed']}`\n"
        f"• **Historic Logs Purged**: `{res['old_logs_purged']}`\n"
        f"• **Disk Space Reclaimed**: `{bytes_mb:.2f} MB`\n"
        f"• **Execution Time**: `{res['execution_time_ms']:.2f} ms`\n"
        f"• **Python Objects Collected**: `{res['memory_stats']['collected_objects']}`"
    )
    await status_msg.edit_text(msg, parse_mode="Markdown")


@admin_only
async def gc_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """`/garbage_stats` - Displays cumulative Garbage Collection analytics."""
    stats = db.get_gc_stats()
    mb_reclaimed = stats["total_bytes_reclaimed"] / (1024 * 1024)

    msg = (
        f"📊 **Garbage Collection Cumulative Metrics**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"• **Total GC Cycles Executed**: `{stats['total_runs']}`\n"
        f"• **Total Stale Buffers Purged**: `{stats['total_buffers_cleaned']}`\n"
        f"• **Total Temp Files Deleted**: `{stats['total_temp_files_removed']}`\n"
        f"• **Total Log Rows Cleaned**: `{stats['total_logs_purged']}`\n"
        f"• **Total Disk Space Reclaimed**: `{mb_reclaimed:.2f} MB`"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


@admin_only
async def vacuum_db_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """`/vacuum` - Triggers SQLite DB defragmentation and WAL compaction."""
    status_msg = await update.message.reply_text("⚡ Executing Database VACUUM...")
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.vacuum_database)
    await status_msg.edit_text("✅ **Database VACUUM complete!** SQLite file defragmented and compacted.", parse_mode="Markdown")


# ==============================================================================
# SECTION 7: CALLBACK ROUTING & APPLICATION SETUP
# ==============================================================================

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.user:
        return
    await query.answer()

    if query.data == "check_status":
        await status_command(update, context)
    elif query.data == "show_help":
        await help_command(update, context)


async def post_init_setup(application: Application) -> None:
    """Registers bot menu commands & launches Garbage Collection background worker."""
    commands = [
        BotCommand("start", "Start bot & view balance"),
        BotCommand("done", "Generate collages from photos"),
        BotCommand("status", "Check status & subscription"),
        BotCommand("clear", "Clear photo upload buffer"),
        BotCommand("help", "View layout guide"),
    ]
    await application.bot.set_my_commands(commands)

    # Launch background Garbage Collector job loop
    asyncio.create_task(gc_engine.start_scheduled_garbage_collector())
    logger.info("Garbage Collection background worker task scheduled.")


def main() -> None:
    if BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN_HERE" or not BOT_TOKEN:
        print("ERROR: BOT_TOKEN is missing! Set it in your environment variables.")
        sys.exit(1)

    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init_setup).build()

    # User Command Routes
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler(["status", "profile"], status_command))
    app.add_handler(CommandHandler("clear", clear_command))
    app.add_handler(CommandHandler("done", done_command))

    # Message & Photo Upload Handler
    app.add_handler(MessageHandler(filters.PHOTO, photo_receiver))
    app.add_handler(CallbackQueryHandler(callback_router))

    # Admin Command Routes
    app.add_handler(CommandHandler("addcoins", addcoins_cmd))
    app.add_handler(CommandHandler("removecoins", removecoins_cmd))
    app.add_handler(CommandHandler("addsub", addsub_cmd))
    app.add_handler(CommandHandler("removesub", removesub_cmd))
    app.add_handler(CommandHandler("userinfo", userinfo_cmd))

    # Garbage System Admin Control Routes
    app.add_handler(CommandHandler("clean_garbage", run_gc_command))
    app.add_handler(CommandHandler("garbage_stats", gc_stats_command))
    app.add_handler(CommandHandler("vacuum", vacuum_db_command))

    logger.info("Bot & Garbage Collector system starting polling...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
