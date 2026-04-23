#!/usr/bin/env python3
"""自动发布 SQLite 存储层。"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from app.models import AccountConfig, PublishCandidate, ScriptMetadata


def _shared_data_dir() -> Path:
    root = os.environ.get("OPENCLAW_SHARED_DATA_DIR", str(Path.home() / ".openclaw" / "shared" / "data"))
    path = Path(root)
    path.mkdir(parents=True, exist_ok=True)
    return path


def default_db_path() -> Path:
    override = os.environ.get("SHORT_VIDEO_AUTO_PUBLISH_DB_PATH")
    if override:
        return Path(override)
    return _shared_data_dir() / "short_video_auto_publish.sqlite3"


def default_video_dir() -> Path:
    override = os.environ.get("SHORT_VIDEO_AUTO_PUBLISH_VIDEO_DIR")
    if override:
        return Path(override)
    return _shared_data_dir() / "short_video_auto_publish_videos"


def build_canonical_script_key(source_record_id: str, script_slot: str) -> str:
    return f"{str(source_record_id or '').strip()}:{str(script_slot or '').strip()}"


class AutoPublishDB:
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = Path(db_path) if db_path else default_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _initialize(self) -> None:
        with self._connect() as conn:
            tables = {
                str(row["name"] or "")
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
            if "script_metadata" not in tables:
                self._create_schema(conn)
                return

            metadata_columns = {
                str(row["name"] or "")
                for row in conn.execute("PRAGMA table_info(script_metadata)").fetchall()
            }
            if "canonical_script_key" not in metadata_columns:
                self._migrate_to_canonical_schema(conn)

            self._ensure_indexes(conn)

    def _create_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS script_metadata (
                canonical_script_key TEXT PRIMARY KEY,
                script_id TEXT NOT NULL,
                source_record_id TEXT NOT NULL,
                script_slot TEXT NOT NULL,
                task_no TEXT NOT NULL,
                store_id TEXT,
                product_id TEXT,
                parent_slot TEXT,
                direction_label TEXT,
                variant_strength TEXT,
                target_country TEXT,
                product_type TEXT,
                content_family_key TEXT,
                script_text TEXT,
                short_video_title TEXT,
                title_source TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(source_record_id, script_slot)
            );

            CREATE TABLE IF NOT EXISTS video_assets (
                canonical_script_key TEXT PRIMARY KEY,
                script_id TEXT NOT NULL,
                run_manager_record_id TEXT,
                video_source_type TEXT,
                video_source_value TEXT,
                local_file_path TEXT,
                download_status TEXT NOT NULL DEFAULT '待下载',
                run_video_status TEXT,
                publish_status TEXT NOT NULL DEFAULT '待排期',
                account_id TEXT,
                account_name TEXT,
                planned_publish_at TEXT,
                published_at TEXT,
                publish_task_id TEXT,
                publish_result TEXT,
                error_message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(canonical_script_key) REFERENCES script_metadata(canonical_script_key)
            );

            CREATE TABLE IF NOT EXISTS account_configs (
                account_id TEXT PRIMARY KEY,
                account_name TEXT NOT NULL,
                store_id TEXT NOT NULL,
                account_status TEXT NOT NULL,
                publish_time_1 TEXT,
                publish_time_2 TEXT,
                publish_time_3 TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS publish_slots (
                slot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                account_name TEXT NOT NULL,
                scheduled_for TEXT NOT NULL,
                canonical_script_key TEXT,
                script_id TEXT,
                schedule_status TEXT NOT NULL DEFAULT '待排期',
                publish_task_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(account_id, scheduled_for)
            );
            """
        )
        self._ensure_indexes(conn)

    def _ensure_indexes(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_script_metadata_display_id
            ON script_metadata(script_id);

            CREATE INDEX IF NOT EXISTS idx_script_metadata_source_slot
            ON script_metadata(source_record_id, script_slot);

            CREATE INDEX IF NOT EXISTS idx_script_metadata_store_product
            ON script_metadata(store_id, product_id);

            CREATE INDEX IF NOT EXISTS idx_video_assets_publish_status
            ON video_assets(publish_status, download_status);

            CREATE INDEX IF NOT EXISTS idx_video_assets_display_id
            ON video_assets(script_id);

            CREATE INDEX IF NOT EXISTS idx_publish_slots_schedule
            ON publish_slots(schedule_status, scheduled_for);

            CREATE INDEX IF NOT EXISTS idx_publish_slots_canonical_key
            ON publish_slots(canonical_script_key);
            """
        )

    def _migrate_to_canonical_schema(self, conn: sqlite3.Connection) -> None:
        legacy_tables = {
            str(row["name"] or "")
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        if "script_metadata" in legacy_tables:
            conn.execute("ALTER TABLE script_metadata RENAME TO script_metadata_legacy")
        if "video_assets" in legacy_tables:
            conn.execute("ALTER TABLE video_assets RENAME TO video_assets_legacy")
        if "publish_slots" in legacy_tables:
            conn.execute("ALTER TABLE publish_slots RENAME TO publish_slots_legacy")

        self._create_schema(conn)

        if "script_metadata" in legacy_tables:
            conn.execute(
                """
                INSERT INTO script_metadata (
                    canonical_script_key, script_id, source_record_id, script_slot, task_no, store_id, product_id,
                    parent_slot, direction_label, variant_strength, target_country, product_type,
                    content_family_key, script_text, short_video_title, title_source, created_at, updated_at
                )
                SELECT
                    source_record_id || ':' || script_slot,
                    script_id,
                    source_record_id,
                    script_slot,
                    task_no,
                    store_id,
                    product_id,
                    parent_slot,
                    direction_label,
                    variant_strength,
                    target_country,
                    product_type,
                    content_family_key,
                    script_text,
                    short_video_title,
                    title_source,
                    created_at,
                    updated_at
                FROM script_metadata_legacy
                """
            )

        if "video_assets" in legacy_tables:
            conn.execute(
                """
                INSERT INTO video_assets (
                    canonical_script_key, script_id, run_manager_record_id, video_source_type, video_source_value,
                    local_file_path, download_status, run_video_status, publish_status, account_id, account_name,
                    planned_publish_at, published_at, publish_task_id, publish_result, error_message,
                    created_at, updated_at
                )
                SELECT
                    COALESCE(sm.source_record_id || ':' || sm.script_slot, '__legacy__:' || va.script_id),
                    COALESCE(sm.script_id, va.script_id),
                    va.run_manager_record_id,
                    va.video_source_type,
                    va.video_source_value,
                    va.local_file_path,
                    va.download_status,
                    va.run_video_status,
                    va.publish_status,
                    va.account_id,
                    va.account_name,
                    va.planned_publish_at,
                    va.published_at,
                    va.publish_task_id,
                    va.publish_result,
                    va.error_message,
                    va.created_at,
                    va.updated_at
                FROM video_assets_legacy va
                LEFT JOIN script_metadata_legacy sm ON sm.script_id = va.script_id
                """
            )

        if "publish_slots" in legacy_tables:
            conn.execute(
                """
                INSERT INTO publish_slots (
                    slot_id, store_id, account_id, account_name, scheduled_for,
                    canonical_script_key, script_id, schedule_status, publish_task_id, created_at, updated_at
                )
                SELECT
                    ps.slot_id,
                    ps.store_id,
                    ps.account_id,
                    ps.account_name,
                    ps.scheduled_for,
                    CASE
                        WHEN ps.script_id IS NULL OR ps.script_id = '' THEN NULL
                        WHEN sm.source_record_id IS NOT NULL THEN sm.source_record_id || ':' || sm.script_slot
                        ELSE '__legacy__:' || ps.script_id
                    END,
                    ps.script_id,
                    ps.schedule_status,
                    ps.publish_task_id,
                    ps.created_at,
                    ps.updated_at
                FROM publish_slots_legacy ps
                LEFT JOIN script_metadata_legacy sm ON sm.script_id = ps.script_id
                """
            )

        conn.execute("DROP TABLE IF EXISTS script_metadata_legacy")
        conn.execute("DROP TABLE IF EXISTS video_assets_legacy")
        conn.execute("DROP TABLE IF EXISTS publish_slots_legacy")

    @staticmethod
    def _canonical_from_row(row: sqlite3.Row) -> str:
        return str(row["canonical_script_key"] or "").strip()

    def _resolve_canonical_for_write(self, *, canonical_script_key: str = "", script_id: str = "") -> str:
        if canonical_script_key:
            return canonical_script_key
        row = self.get_script_metadata(script_id)
        if row is not None:
            return str(row["canonical_script_key"] or "").strip()
        return script_id

    def upsert_script_metadata(self, items: Iterable[ScriptMetadata]) -> int:
        rows = list(items)
        if not rows:
            return 0
        now = self._now_text()
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO script_metadata (
                    canonical_script_key, script_id, source_record_id, script_slot, task_no, store_id, product_id,
                    parent_slot, direction_label, variant_strength, target_country, product_type,
                    content_family_key, script_text, short_video_title, title_source, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(canonical_script_key) DO UPDATE SET
                    script_id = excluded.script_id,
                    source_record_id = excluded.source_record_id,
                    script_slot = excluded.script_slot,
                    task_no = excluded.task_no,
                    store_id = excluded.store_id,
                    product_id = excluded.product_id,
                    parent_slot = excluded.parent_slot,
                    direction_label = excluded.direction_label,
                    variant_strength = excluded.variant_strength,
                    target_country = excluded.target_country,
                    product_type = excluded.product_type,
                    content_family_key = excluded.content_family_key,
                    script_text = excluded.script_text,
                    short_video_title = excluded.short_video_title,
                    title_source = excluded.title_source,
                    updated_at = excluded.updated_at
                """,
                [
                    (
                        item.canonical_script_key or build_canonical_script_key(item.source_record_id, item.script_slot),
                        item.script_id,
                        item.source_record_id,
                        item.script_slot,
                        item.task_no,
                        item.store_id,
                        item.product_id,
                        item.parent_slot,
                        item.direction_label,
                        item.variant_strength,
                        item.target_country,
                        item.product_type,
                        item.content_family_key,
                        item.script_text,
                        item.short_video_title,
                        item.title_source,
                        now,
                        now,
                    )
                    for item in rows
                ],
            )
        return len(rows)

    def build_metadata_lookup(self) -> Dict[tuple, Dict[str, str]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT canonical_script_key, source_record_id, script_slot, script_id, store_id, product_id,
                       parent_slot, direction_label, variant_strength, short_video_title,
                       title_source, script_text, target_country, product_type, content_family_key, task_no
                FROM script_metadata
                """
            ).fetchall()
        return {
            (str(row["source_record_id"]), str(row["script_slot"])): {
                "canonical_script_key": str(row["canonical_script_key"] or ""),
                "script_id": str(row["script_id"] or ""),
                "store_id": str(row["store_id"] or ""),
                "product_id": str(row["product_id"] or ""),
                "parent_slot": str(row["parent_slot"] or ""),
                "direction_label": str(row["direction_label"] or ""),
                "variant_strength": str(row["variant_strength"] or ""),
                "short_video_title": str(row["short_video_title"] or ""),
                "title_source": str(row["title_source"] or ""),
                "script_text": str(row["script_text"] or ""),
                "target_country": str(row["target_country"] or ""),
                "product_type": str(row["product_type"] or ""),
                "content_family_key": str(row["content_family_key"] or ""),
                "task_no": str(row["task_no"] or ""),
            }
            for row in rows
        }

    def get_script_metadata(self, identifier: str) -> Optional[sqlite3.Row]:
        text = str(identifier or "").strip()
        if not text:
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM script_metadata WHERE canonical_script_key = ?",
                (text,),
            ).fetchone()
            if row is not None:
                return row
            rows = conn.execute(
                "SELECT * FROM script_metadata WHERE script_id = ? ORDER BY updated_at DESC, canonical_script_key ASC LIMIT 2",
                (text,),
            ).fetchall()
        if len(rows) == 1:
            return rows[0]
        return None

    def list_script_metadata(self, limit: Optional[int] = None) -> List[ScriptMetadata]:
        sql = """
            SELECT canonical_script_key, script_id, source_record_id, script_slot, task_no, store_id, product_id,
                   parent_slot, direction_label, variant_strength, target_country, product_type,
                   content_family_key, script_text, short_video_title, title_source
            FROM script_metadata
            ORDER BY updated_at ASC, script_id ASC, canonical_script_key ASC
        """
        params: tuple = ()
        if limit is not None:
            sql += " LIMIT ?"
            params = (limit,)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [
            ScriptMetadata(
                canonical_script_key=str(row["canonical_script_key"] or ""),
                script_id=str(row["script_id"] or ""),
                source_record_id=str(row["source_record_id"] or ""),
                script_slot=str(row["script_slot"] or ""),
                task_no=str(row["task_no"] or ""),
                store_id=str(row["store_id"] or ""),
                product_id=str(row["product_id"] or ""),
                parent_slot=str(row["parent_slot"] or ""),
                direction_label=str(row["direction_label"] or ""),
                variant_strength=str(row["variant_strength"] or ""),
                target_country=str(row["target_country"] or ""),
                product_type=str(row["product_type"] or ""),
                content_family_key=str(row["content_family_key"] or ""),
                script_text=str(row["script_text"] or ""),
                short_video_title=str(row["short_video_title"] or ""),
                title_source=str(row["title_source"] or ""),
            )
            for row in rows
        ]

    def get_video_asset(self, identifier: str) -> Optional[sqlite3.Row]:
        text = str(identifier or "").strip()
        if not text:
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM video_assets WHERE canonical_script_key = ?",
                (text,),
            ).fetchone()
            if row is not None:
                return row
            rows = conn.execute(
                "SELECT * FROM video_assets WHERE script_id = ? ORDER BY updated_at DESC, canonical_script_key ASC LIMIT 2",
                (text,),
            ).fetchall()
        if len(rows) == 1:
            return rows[0]
        return None

    def upsert_video_asset(
        self,
        *,
        script_id: str,
        run_manager_record_id: str,
        video_source_type: str,
        video_source_value: str,
        local_file_path: str,
        download_status: str,
        run_video_status: str,
        publish_status: str = "待排期",
        canonical_script_key: str = "",
    ) -> None:
        now = self._now_text()
        resolved_key = self._resolve_canonical_for_write(
            canonical_script_key=canonical_script_key,
            script_id=script_id,
        )
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO video_assets (
                    canonical_script_key, script_id, run_manager_record_id, video_source_type, video_source_value,
                    local_file_path, download_status, run_video_status, publish_status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(canonical_script_key) DO UPDATE SET
                    script_id = excluded.script_id,
                    run_manager_record_id = excluded.run_manager_record_id,
                    video_source_type = excluded.video_source_type,
                    video_source_value = excluded.video_source_value,
                    local_file_path = excluded.local_file_path,
                    download_status = excluded.download_status,
                    run_video_status = excluded.run_video_status,
                    publish_status = CASE
                        WHEN video_assets.publish_status IN ('已排期', '已发布') THEN video_assets.publish_status
                        ELSE excluded.publish_status
                    END,
                    updated_at = excluded.updated_at
                """,
                (
                    resolved_key,
                    script_id,
                    run_manager_record_id,
                    video_source_type,
                    video_source_value,
                    local_file_path,
                    download_status,
                    run_video_status,
                    publish_status,
                    now,
                    now,
                ),
            )

    def upsert_account_configs(self, accounts: Iterable[AccountConfig]) -> int:
        rows = list(accounts)
        if not rows:
            return 0
        now = self._now_text()
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO account_configs (
                    account_id, account_name, store_id, account_status,
                    publish_time_1, publish_time_2, publish_time_3, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    account_name = excluded.account_name,
                    store_id = excluded.store_id,
                    account_status = excluded.account_status,
                    publish_time_1 = excluded.publish_time_1,
                    publish_time_2 = excluded.publish_time_2,
                    publish_time_3 = excluded.publish_time_3,
                    updated_at = excluded.updated_at
                """,
                [
                    (
                        item.account_id,
                        item.account_name,
                        item.store_id,
                        item.account_status,
                        item.publish_time_1,
                        item.publish_time_2,
                        item.publish_time_3,
                        now,
                        now,
                    )
                    for item in rows
                ],
            )
        return len(rows)

    def generate_future_slots(self, now: datetime, window_hours: int = 24) -> int:
        end_at = now + timedelta(hours=window_hours)
        created = 0
        with self._connect() as conn:
            accounts = conn.execute(
                """
                SELECT account_id, account_name, store_id, publish_time_1, publish_time_2, publish_time_3
                FROM account_configs
                WHERE account_status = '可用'
                """
            ).fetchall()
            for account in accounts:
                times = [
                    str(account["publish_time_1"] or "").strip(),
                    str(account["publish_time_2"] or "").strip(),
                    str(account["publish_time_3"] or "").strip(),
                ]
                current_day = now.date()
                final_day = end_at.date()
                while current_day <= final_day:
                    for hhmm in times:
                        parsed = self._parse_hhmm(hhmm)
                        if parsed is None:
                            continue
                        slot_time = datetime.combine(current_day, parsed)
                        if slot_time < now or slot_time > end_at:
                            continue
                        before = conn.total_changes
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO publish_slots (
                                store_id, account_id, account_name, scheduled_for, schedule_status, created_at, updated_at
                            ) VALUES (?, ?, ?, ?, '待排期', ?, ?)
                            """,
                            (
                                str(account["store_id"] or ""),
                                str(account["account_id"] or ""),
                                str(account["account_name"] or ""),
                                slot_time.strftime("%Y-%m-%d %H:%M:%S"),
                                self._now_text(),
                                self._now_text(),
                            ),
                        )
                        if conn.total_changes > before:
                            created += 1
                    current_day += timedelta(days=1)
        return created

    @staticmethod
    def _parse_hhmm(raw_value: str) -> Optional[time]:
        text = str(raw_value or "").strip()
        if not text:
            return None
        try:
            hour, minute = text.split(":", 1)
            return time(hour=int(hour), minute=int(minute))
        except ValueError:
            return None

    def list_pending_slots(self, now: datetime, window_hours: int = 24) -> List[sqlite3.Row]:
        end_at = now + timedelta(hours=window_hours)
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT *
                FROM publish_slots
                WHERE scheduled_for >= ?
                  AND scheduled_for <= ?
                  AND (canonical_script_key IS NULL OR schedule_status = '待排期')
                ORDER BY scheduled_for ASC, account_id ASC
                """,
                (now.strftime("%Y-%m-%d %H:%M:%S"), end_at.strftime("%Y-%m-%d %H:%M:%S")),
            ).fetchall()

    def list_ready_candidates(self, store_id: str) -> List[PublishCandidate]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT sm.canonical_script_key, sm.script_id, sm.store_id, sm.product_id, sm.content_family_key,
                       sm.short_video_title, va.local_file_path, va.video_source_type, va.video_source_value,
                       sm.source_record_id, sm.script_slot
                FROM script_metadata sm
                INNER JOIN video_assets va ON va.canonical_script_key = sm.canonical_script_key
                WHERE sm.store_id = ?
                  AND COALESCE(sm.short_video_title, '') <> ''
                  AND COALESCE(va.local_file_path, '') <> ''
                  AND va.download_status = '下载成功'
                  AND va.publish_status = '待排期'
                ORDER BY sm.updated_at ASC, sm.script_id ASC, sm.canonical_script_key ASC
                """,
                (store_id,),
            ).fetchall()
        return [
            PublishCandidate(
                canonical_script_key=str(row["canonical_script_key"] or ""),
                script_id=str(row["script_id"] or ""),
                store_id=str(row["store_id"] or ""),
                product_id=str(row["product_id"] or ""),
                content_family_key=str(row["content_family_key"] or ""),
                short_video_title=str(row["short_video_title"] or ""),
                local_file_path=str(row["local_file_path"] or ""),
                publish_video_value=(
                    str(row["video_source_value"] or "").strip()
                    if str(row["video_source_type"] or "").strip() == "link"
                    and str(row["video_source_value"] or "").strip().startswith(("http://", "https://"))
                    else str(row["local_file_path"] or "")
                ),
                source_record_id=str(row["source_record_id"] or ""),
                script_slot=str(row["script_slot"] or ""),
            )
            for row in rows
        ]

    def recycle_dryrun_schedules(self) -> int:
        now = self._now_text()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT canonical_script_key
                FROM publish_slots
                WHERE publish_task_id LIKE 'dryrun-%'
                """
            ).fetchall()
            canonical_keys = [
                str(row["canonical_script_key"] or "")
                for row in rows
                if str(row["canonical_script_key"] or "").strip()
            ]
            conn.execute(
                """
                UPDATE publish_slots
                SET canonical_script_key = NULL,
                    script_id = NULL,
                    schedule_status = '待排期',
                    publish_task_id = NULL,
                    updated_at = ?
                WHERE publish_task_id LIKE 'dryrun-%'
                """,
                (now,),
            )
            for canonical_key in canonical_keys:
                conn.execute(
                    """
                    UPDATE video_assets
                    SET publish_status = '待排期',
                        account_id = NULL,
                        account_name = NULL,
                        planned_publish_at = NULL,
                        publish_task_id = NULL,
                        publish_result = NULL,
                        error_message = NULL,
                        updated_at = ?
                    WHERE canonical_script_key = ?
                      AND publish_task_id LIKE 'dryrun-%'
                    """,
                    (now, canonical_key),
                )
        return len(canonical_keys)

    def has_recent_product_conflict(self, account_id: str, product_id: str, target_time: datetime, hours: int = 72) -> bool:
        start_at = (target_time - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        end_at = target_time.strftime("%Y-%m-%d %H:%M:%S")
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM publish_slots ps
                INNER JOIN script_metadata sm ON sm.canonical_script_key = ps.canonical_script_key
                WHERE ps.account_id = ?
                  AND sm.product_id = ?
                  AND ps.scheduled_for >= ?
                  AND ps.scheduled_for <= ?
                  AND ps.schedule_status IN ('已排期', '已发布')
                LIMIT 1
                """,
                (account_id, product_id, start_at, end_at),
            ).fetchone()
        return row is not None

    def has_recent_family_conflict(self, store_id: str, content_family_key: str, target_time: datetime, hours: int = 48) -> bool:
        start_at = (target_time - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        end_at = target_time.strftime("%Y-%m-%d %H:%M:%S")
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM publish_slots ps
                INNER JOIN script_metadata sm ON sm.canonical_script_key = ps.canonical_script_key
                WHERE ps.store_id = ?
                  AND sm.content_family_key = ?
                  AND ps.scheduled_for >= ?
                  AND ps.scheduled_for <= ?
                  AND ps.schedule_status IN ('已排期', '已发布')
                LIMIT 1
                """,
                (store_id, content_family_key, start_at, end_at),
            ).fetchone()
        return row is not None

    def assign_slot(
        self,
        *,
        slot_id: int,
        script_id: str,
        publish_task_id: str,
        account_id: str,
        account_name: str,
        planned_publish_at: datetime,
        canonical_script_key: str = "",
    ) -> None:
        now = self._now_text()
        planned_text = planned_publish_at.strftime("%Y-%m-%d %H:%M:%S")
        resolved_key = self._resolve_canonical_for_write(
            canonical_script_key=canonical_script_key,
            script_id=script_id,
        )
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE publish_slots
                SET canonical_script_key = ?, script_id = ?, schedule_status = '已排期',
                    publish_task_id = ?, updated_at = ?
                WHERE slot_id = ?
                """,
                (resolved_key, script_id, publish_task_id, now, slot_id),
            )
            conn.execute(
                """
                UPDATE video_assets
                SET script_id = ?, publish_status = '已排期',
                    account_id = ?, account_name = ?, planned_publish_at = ?, published_at = NULL,
                    publish_task_id = ?, publish_result = NULL, error_message = NULL, updated_at = ?
                WHERE canonical_script_key = ?
                """,
                (script_id, account_id, account_name, planned_text, publish_task_id, now, resolved_key),
            )

    def mark_publish_result(
        self,
        *,
        script_id: str,
        publish_task_id: str,
        schedule_status: str,
        publish_status: str,
        publish_result: str,
        published_at: Optional[str] = None,
        error_message: str = "",
        canonical_script_key: str = "",
    ) -> None:
        now = self._now_text()
        resolved_key = self._resolve_canonical_for_write(
            canonical_script_key=canonical_script_key,
            script_id=script_id,
        )
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE publish_slots
                SET schedule_status = ?, updated_at = ?
                WHERE publish_task_id = ?
                """,
                (schedule_status, now, publish_task_id),
            )
            conn.execute(
                """
                UPDATE video_assets
                SET publish_status = ?, publish_result = ?, published_at = COALESCE(?, published_at),
                    error_message = ?, updated_at = ?
                WHERE canonical_script_key = ?
                """,
                (publish_status, publish_result, published_at, error_message, now, resolved_key),
            )

    def list_scheduled_tasks(self) -> List[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT ps.slot_id, ps.publish_task_id, ps.scheduled_for, ps.account_id, ps.account_name,
                       ps.store_id, ps.canonical_script_key, ps.script_id, va.local_file_path,
                       sm.short_video_title, sm.product_id, sm.content_family_key
                FROM publish_slots ps
                INNER JOIN video_assets va ON va.canonical_script_key = ps.canonical_script_key
                INNER JOIN script_metadata sm ON sm.canonical_script_key = ps.canonical_script_key
                WHERE ps.schedule_status = '已排期'
                ORDER BY ps.scheduled_for ASC
                """
            ).fetchall()

    def cleanup_published_videos(
        self,
        *,
        older_than_days: int = 30,
        base_dir: Optional[Path] = None,
        now: Optional[datetime] = None,
    ) -> Dict[str, int]:
        retention_days = max(int(older_than_days or 0), 0)
        if retention_days <= 0:
            return {"candidates": 0, "deleted": 0, "missing": 0, "cleared": 0, "skipped": 0}

        current_time = now or datetime.now()
        cutoff = (current_time - timedelta(days=retention_days)).strftime("%Y-%m-%d %H:%M:%S")
        scope_dir = Path(base_dir).resolve(strict=False) if base_dir else None

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT canonical_script_key, local_file_path
                FROM video_assets
                WHERE publish_status = '已发布'
                  AND COALESCE(local_file_path, '') <> ''
                  AND COALESCE(published_at, '') <> ''
                  AND published_at <= ?
                ORDER BY published_at ASC, canonical_script_key ASC
                """,
                (cutoff,),
            ).fetchall()

            deleted = 0
            missing = 0
            cleared = 0
            skipped = 0
            updated_at = self._now_text()

            for row in rows:
                canonical_key = str(row["canonical_script_key"] or "").strip()
                raw_path = str(row["local_file_path"] or "").strip()
                if not canonical_key or not raw_path:
                    skipped += 1
                    continue

                file_path = Path(raw_path)
                resolved_path = file_path.resolve(strict=False)
                if scope_dir is not None and not resolved_path.is_relative_to(scope_dir):
                    skipped += 1
                    continue

                if file_path.exists():
                    try:
                        file_path.unlink()
                        deleted += 1
                    except OSError:
                        skipped += 1
                        continue
                else:
                    missing += 1

                conn.execute(
                    """
                    UPDATE video_assets
                    SET local_file_path = '',
                        download_status = '已清理',
                        updated_at = ?
                    WHERE canonical_script_key = ?
                    """,
                    (updated_at, canonical_key),
                )
                cleared += 1

        return {
            "candidates": len(rows),
            "deleted": deleted,
            "missing": missing,
            "cleared": cleared,
            "skipped": skipped,
        }

    def list_publish_report_rows(self) -> List[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT sm.canonical_script_key,
                       sm.script_id,
                       sm.source_record_id,
                       sm.script_slot,
                       sm.task_no,
                       sm.store_id,
                       sm.product_id,
                       sm.content_family_key,
                       sm.short_video_title,
                       va.run_manager_record_id,
                       va.video_source_type,
                       va.video_source_value,
                       va.local_file_path,
                       va.download_status,
                       va.run_video_status,
                       va.publish_status,
                       va.account_id,
                       va.account_name,
                       va.planned_publish_at,
                       va.published_at,
                       va.publish_task_id,
                       va.publish_result,
                       va.error_message,
                       ps.schedule_status AS latest_schedule_status,
                       ps.scheduled_for AS latest_slot_scheduled_for,
                       ps.updated_at AS slot_updated_at,
                       va.updated_at AS asset_updated_at,
                       sm.updated_at AS metadata_updated_at
                FROM script_metadata sm
                INNER JOIN video_assets va ON va.canonical_script_key = sm.canonical_script_key
                LEFT JOIN publish_slots ps ON ps.slot_id = (
                    SELECT ps2.slot_id
                    FROM publish_slots ps2
                    WHERE ps2.canonical_script_key = sm.canonical_script_key
                    ORDER BY COALESCE(ps2.updated_at, ps2.created_at) DESC, ps2.slot_id DESC
                    LIMIT 1
                )
                ORDER BY COALESCE(va.planned_publish_at, ps.scheduled_for, ''), sm.script_id, sm.canonical_script_key
                """
            ).fetchall()
