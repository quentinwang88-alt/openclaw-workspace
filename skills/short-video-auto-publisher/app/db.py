#!/usr/bin/env python3
"""自动发布 SQLite 存储层。"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

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


def is_nurture_candidate(candidate: PublishCandidate) -> bool:
    markers = " ".join(
        str(getattr(candidate, attr, "") or "").strip()
        for attr in ("script_source", "publish_purpose", "content_branch")
    )
    return (
        str(candidate.script_source or "").strip() == "养号复刻"
        or str(candidate.publish_purpose or "").strip() == "养号"
        or str(candidate.content_branch or "").strip() == "非商品展示型"
        or "种草脚本" in markers
        or "种草" in markers
        or "SEEDING_ORGANIC" in markers
    )


def is_opv_initialization_candidate(candidate: PublishCandidate) -> bool:
    """Return whether a candidate belongs to the OPV organic nurture pool."""
    return (
        str(candidate.canonical_script_key or "").strip().startswith("opv:")
        and str(candidate.script_source or "").strip() == "图文养号"
        and str(candidate.publish_purpose or "").strip() == "养号"
    )


def _candidate_context(script_text: str) -> Dict[str, str]:
    try:
        payload = json.loads(str(script_text or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        key: str(payload.get(key) or "").strip()
        for key in ("recipe_id", "theme_id", "source_product_id")
    }


def _photo_candidate_paths(raw_manifest: str, script_text: str, script_id: str) -> List[str]:
    """Read only a released, ordered photo manifest bound to its queue metadata.

    File bytes and the current RDS revision are rechecked by the release gate
    immediately before upload. Candidate scans never initialize the OPV DB.
    """
    manifest = json.loads(raw_manifest)
    context = json.loads(script_text)
    if (not isinstance(manifest, dict) or not isinstance(context, dict)
            or manifest.get("schema_version") != "opv-photo-release-v1"
            or manifest.get("media_kind") != "native_photo"
            or manifest.get("task_id") != script_id
            or context.get("release_manifest") != manifest):
        raise ValueError("照片候选与冻结发布清单不匹配")
    unsigned = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    digest = hashlib.sha256(json.dumps(unsigned, ensure_ascii=False, sort_keys=True,
                                      separators=(",", ":")).encode()).hexdigest()
    if manifest.get("manifest_sha256") != digest:
        raise ValueError("照片发布清单指纹不匹配")
    slides = manifest.get("slides")
    if not isinstance(slides, list) or not slides:
        raise ValueError("照片发布清单为空")
    paths = []
    for index, slide in enumerate(slides, start=1):
        if not isinstance(slide, dict) or type(slide.get("index")) is not int or slide["index"] != index:
            raise ValueError("照片发布清单顺序无效")
        raw_path = slide.get("path")
        if not isinstance(raw_path, str) or not raw_path.strip():
            raise ValueError("照片发布清单缺少路径")
        path = Path(raw_path)
        if not path.is_absolute() or not path.is_file() or not slide.get("sha256"):
            raise ValueError("照片发布素材不存在或缺少指纹")
        paths.append(str(path))
    return paths


class AutoPublishDB:
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = Path(db_path) if db_path else default_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def is_mixcut_scheduling_paused(self) -> bool:
        explicit = str(os.environ.get("SHORT_VIDEO_AUTO_PUBLISH_PAUSE_MIXCUT", "") or "").strip().lower()
        if explicit:
            return explicit in {"1", "true", "yes", "y", "是", "暂停", "paused"}

        config_path = Path(
            os.environ.get(
                "SHORT_VIDEO_AUTO_PUBLISH_CONFIG_PATH",
                "/Users/likeu3/.openclaw/shared/data/short_video_auto_publisher_config.json",
            )
        )
        default_path = default_db_path().resolve(strict=False)
        if self.db_path.resolve(strict=False) != default_path and not os.environ.get("SHORT_VIDEO_AUTO_PUBLISH_CONFIG_PATH"):
            return False
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
        except Exception:
            return False
        if not isinstance(payload, dict):
            return False
        for key in ("mixcut_scheduling_paused", "pause_mixcut_scheduling", "disable_mixcut_scheduling"):
            value = payload.get(key)
            if isinstance(value, bool):
                return value
            if str(value or "").strip().lower() in {"1", "true", "yes", "y", "是", "暂停", "paused"}:
                return True
        return False

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS account_channel_bindings (
                    account_id TEXT NOT NULL,
                    publish_channel TEXT NOT NULL,
                    content_scope TEXT NOT NULL DEFAULT 'all',
                    publish_time_1 TEXT,
                    publish_time_2 TEXT,
                    publish_time_3 TEXT,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    source_record_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (account_id, publish_channel)
                )
                """
            )
            conn.execute("""
                CREATE TABLE IF NOT EXISTS publish_candidate_retries (
                    canonical_script_key TEXT PRIMARY KEY,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_retry_at TEXT,
                    blocked INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    updated_at TEXT
                )
            """)
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

            self._ensure_column(conn, "script_metadata", "script_source", "TEXT")
            self._ensure_column(conn, "script_metadata", "publish_purpose", "TEXT")
            self._ensure_column(conn, "script_metadata", "cart_enabled", "TEXT")
            self._ensure_column(conn, "script_metadata", "content_branch", "TEXT")
            self._ensure_column(conn, "script_metadata", "audio_mode", "TEXT")
            self._ensure_column(conn, "video_assets", "media_kind", "TEXT NOT NULL DEFAULT 'video'")
            self._ensure_column(conn, "video_assets", "photo_manifest_json", "TEXT")
            self._ensure_column(conn, "account_configs", "nurture_enabled", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "account_configs", "nurture_daily_count", "INTEGER NOT NULL DEFAULT 2")
            self._ensure_column(conn, "account_configs", "nurture_only", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "account_configs", "initialization_enabled", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "account_configs", "publish_channel", "TEXT NOT NULL DEFAULT 'GeeLark'")
            self._ensure_column(conn, "account_configs", "organic_capable", "INTEGER")
            self._ensure_column(conn, "account_configs", "shoppable_capable", "INTEGER")
            self._ensure_column(conn, "account_configs", "organic_auth_id", "TEXT")
            self._ensure_column(conn, "account_configs", "shoppable_auth_id", "TEXT")
            self._ensure_column(conn, "account_configs", "capability_status", "TEXT NOT NULL DEFAULT 'unknown'")
            self._ensure_column(conn, "account_configs", "capability_checked_at", "TEXT")
            self._ensure_column(conn, "account_configs", "capability_error", "TEXT")
            self._ensure_column(conn, "account_configs", "publish_profile_id", "TEXT")
            self._ensure_column(conn, "publish_slots", "publish_channel_used", "TEXT")
            self._ensure_column(conn, "account_configs", "provider_connection_uid", "TEXT")
            self._ensure_column(conn, "account_configs", "account_timezone", "TEXT")
            self._ensure_column(conn, "account_configs", "content_video_capable", "INTEGER")
            self._ensure_column(conn, "account_configs", "content_photo_capable", "INTEGER")
            self._ensure_column(conn, "account_configs", "shop_video_capable", "INTEGER")
            self._ensure_column(conn, "account_configs", "shop_photo_capable", "INTEGER")
            self._ensure_column(conn, "account_configs", "direct_post_capable", "INTEGER")
            self._ensure_column(conn, "account_configs", "delivery_mode", "TEXT")
            self._ensure_column(conn, "account_configs", "provider_health", "TEXT")
            self._ensure_column(conn, "account_configs", "provider_checked_at", "TEXT")
            self._ensure_column(conn, "account_configs", "provider_error", "TEXT")
            # OPV 定位账号：内容定位缓存 + 图文领取范围（2026-09-15）。
            self._ensure_column(conn, "account_configs", "photo_content_profile_json", "TEXT")
            self._ensure_column(conn, "account_configs", "photo_claim_scope",
                                "TEXT NOT NULL DEFAULT 'store_pool'")
            self._ensure_column(conn, "script_metadata", "target_publish_account_id", "TEXT")
            self._ensure_publisher_profiles_table(conn)
            self._ensure_column(conn, "publish_slots", "bgm_json", "TEXT")
            self._ensure_column(conn, "publish_slots", "submission_context_json", "TEXT")
            self._ensure_column(conn, "publish_slots", "platform_post_id", "TEXT")
            self._ensure_column(conn, "publish_slots", "platform_post_url", "TEXT")
            self._ensure_column(conn, "publish_slots", "published_at", "TEXT")
            self._ensure_column(conn, "publish_slots", "error_message", "TEXT")
            self._ensure_column(conn, "publish_slots", "slot_source", "TEXT NOT NULL DEFAULT 'auto'")
            self._ensure_column(conn, "publish_slots", "manual_request_record_id", "TEXT")
            self._ensure_column(conn, "publish_slots", "title_override", "TEXT")
            self._ensure_column(conn, "publish_slots", "channel_override", "TEXT")
            self._ensure_disabled_products_table(conn)
            self._ensure_product_schedule_preferences_table(conn)
            self._ensure_notification_log_table(conn)
            self._ensure_manual_publish_requests_table(conn)
            self._ensure_publish_task_history_table(conn)
            self._ensure_script_pool_bindings_table(conn)
            self._ensure_indexes(conn)

    def _ensure_column(self, conn: sqlite3.Connection, table_name: str, column_name: str, column_def: str) -> None:
        columns = {
            str(row["name"] or "")
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if column_name not in columns:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")

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
                script_source TEXT,
                publish_purpose TEXT,
                cart_enabled TEXT,
                content_branch TEXT,
                audio_mode TEXT,
                target_publish_account_id TEXT,
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
                media_kind TEXT NOT NULL DEFAULT 'video',
                photo_manifest_json TEXT,
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
                publish_channel TEXT NOT NULL DEFAULT 'GeeLark',
                publish_time_1 TEXT,
                publish_time_2 TEXT,
                publish_time_3 TEXT,
                nurture_enabled INTEGER NOT NULL DEFAULT 0,
                nurture_daily_count INTEGER NOT NULL DEFAULT 2,
                nurture_only INTEGER NOT NULL DEFAULT 0,
                initialization_enabled INTEGER NOT NULL DEFAULT 0,
                organic_capable INTEGER,
                shoppable_capable INTEGER,
                organic_auth_id TEXT,
                shoppable_auth_id TEXT,
                capability_status TEXT NOT NULL DEFAULT 'unknown',
                capability_checked_at TEXT,
                capability_error TEXT,
                publish_profile_id TEXT,
                provider_connection_uid TEXT,
                account_timezone TEXT,
                content_video_capable INTEGER,
                content_photo_capable INTEGER,
                shop_video_capable INTEGER,
                shop_photo_capable INTEGER,
                direct_post_capable INTEGER,
                delivery_mode TEXT,
                provider_health TEXT,
                provider_checked_at TEXT,
                provider_error TEXT,
                photo_content_profile_json TEXT,
                photo_claim_scope TEXT NOT NULL DEFAULT 'store_pool',
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
                submission_context_json TEXT,
                schedule_status TEXT NOT NULL DEFAULT '待排期',
                publish_task_id TEXT,
                bgm_json TEXT,
                platform_post_id TEXT,
                platform_post_url TEXT,
                published_at TEXT,
                error_message TEXT,
                slot_source TEXT NOT NULL DEFAULT 'auto',
                manual_request_record_id TEXT,
                title_override TEXT,
                channel_override TEXT,
                publish_channel_used TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(account_id, scheduled_for)
            );
            """
        )
        self._ensure_disabled_products_table(conn)
        self._ensure_product_schedule_preferences_table(conn)
        self._ensure_notification_log_table(conn)
        self._ensure_manual_publish_requests_table(conn)
        self._ensure_publish_task_history_table(conn)
        self._ensure_script_pool_bindings_table(conn)
        self._ensure_publisher_profiles_table(conn)
        self._ensure_indexes(conn)

    def _ensure_script_pool_bindings_table(self, conn: sqlite3.Connection) -> None:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS script_pool_bindings (
                canonical_script_key TEXT PRIMARY KEY,
                platform_product_id TEXT NOT NULL DEFAULT '',
                target_language TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            )
        """)

    def _ensure_publisher_profiles_table(self, conn: sqlite3.Connection) -> None:
        """发布渠道配置文件（如 CreatOK workspace）。API Key 只存环境变量名。"""
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS publisher_profiles (
                profile_id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                workspace_name TEXT,
                api_key_env_name TEXT NOT NULL,
                cli_path TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                last_health_check_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

    def _ensure_disabled_products_table(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS disabled_products (
                product_id TEXT PRIMARY KEY,
                reason TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

    def _ensure_product_schedule_preferences_table(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS product_schedule_preferences (
                store_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                schedule_strategy TEXT NOT NULL DEFAULT '普通',
                schedule_note TEXT,
                priority_updated_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(store_id, product_id)
            )
            """
        )

    def _ensure_notification_log_table(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS publish_notifications (
                notification_key TEXT PRIMARY KEY,
                channel TEXT NOT NULL,
                payload TEXT,
                sent_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

    def _ensure_manual_publish_requests_table(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_publish_requests (
                record_id TEXT PRIMARY KEY,
                canonical_script_key TEXT,
                script_id TEXT,
                store_id TEXT,
                account_id TEXT,
                account_name TEXT,
                scheduled_for TEXT,
                publish_channel TEXT,
                product_id TEXT,
                short_video_title TEXT,
                local_file_path TEXT,
                publish_task_id TEXT,
                request_status TEXT,
                error_message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

    def _ensure_publish_task_history_table(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS publish_task_history (
                history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                slot_id INTEGER,
                canonical_script_key TEXT,
                script_id TEXT,
                account_id TEXT,
                account_name TEXT,
                scheduled_for TEXT,
                publish_task_id TEXT,
                event_type TEXT NOT NULL,
                event_detail TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO publish_task_history (
                slot_id, canonical_script_key, script_id, account_id, account_name,
                scheduled_for, publish_task_id, event_type, event_detail, created_at
            )
            SELECT ps.slot_id, ps.canonical_script_key, ps.script_id, ps.account_id, ps.account_name,
                   ps.scheduled_for, ps.publish_task_id, 'baseline', '升级时回填现有任务',
                   COALESCE(ps.created_at, ps.updated_at)
            FROM publish_slots ps
            WHERE COALESCE(ps.publish_task_id, '') <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM publish_task_history history
                  WHERE history.publish_task_id = ps.publish_task_id
                    AND history.event_type IN ('baseline', 'created')
              )
            """
        )

    def _ensure_indexes(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_script_metadata_display_id
            ON script_metadata(script_id);

            CREATE INDEX IF NOT EXISTS idx_script_metadata_source_slot
            ON script_metadata(source_record_id, script_slot);

            CREATE INDEX IF NOT EXISTS idx_script_metadata_store_product
            ON script_metadata(store_id, product_id);

            CREATE INDEX IF NOT EXISTS idx_script_metadata_product
            ON script_metadata(product_id);

            CREATE INDEX IF NOT EXISTS idx_video_assets_publish_status
            ON video_assets(publish_status, download_status);

            CREATE INDEX IF NOT EXISTS idx_video_assets_display_id
            ON video_assets(script_id);

            CREATE INDEX IF NOT EXISTS idx_publish_slots_schedule
            ON publish_slots(schedule_status, scheduled_for);

            CREATE INDEX IF NOT EXISTS idx_publish_slots_canonical_key
            ON publish_slots(canonical_script_key);

            CREATE INDEX IF NOT EXISTS idx_publish_slots_task_id
            ON publish_slots(publish_task_id);

            CREATE INDEX IF NOT EXISTS idx_publish_slots_manual_request
            ON publish_slots(manual_request_record_id);

            CREATE INDEX IF NOT EXISTS idx_manual_publish_requests_status
            ON manual_publish_requests(request_status, scheduled_for);

            CREATE INDEX IF NOT EXISTS idx_product_schedule_preferences_strategy
            ON product_schedule_preferences(schedule_strategy, store_id, product_id);

            CREATE INDEX IF NOT EXISTS idx_publish_task_history_task
            ON publish_task_history(publish_task_id, created_at);

            CREATE INDEX IF NOT EXISTS idx_publish_task_history_script
            ON publish_task_history(canonical_script_key, created_at);
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
                    content_family_key, script_text, short_video_title, title_source,
                    script_source, publish_purpose, cart_enabled, content_branch, audio_mode,
                    target_publish_account_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    script_source = excluded.script_source,
                    publish_purpose = excluded.publish_purpose,
                    cart_enabled = excluded.cart_enabled,
                    content_branch = excluded.content_branch,
                    audio_mode = excluded.audio_mode,
                    target_publish_account_id = excluded.target_publish_account_id,
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
                        item.script_source,
                        item.publish_purpose,
                        item.cart_enabled,
                        item.content_branch,
                        item.audio_mode,
                        str(getattr(item, "target_publish_account_id", "") or ""),
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
                       title_source, script_text, target_country, product_type, content_family_key, task_no,
                       script_source, publish_purpose, cart_enabled, content_branch, audio_mode
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
                "script_source": str(row["script_source"] or ""),
                "publish_purpose": str(row["publish_purpose"] or ""),
                "cart_enabled": str(row["cart_enabled"] or ""),
                "content_branch": str(row["content_branch"] or ""),
                "audio_mode": str(row["audio_mode"] or ""),
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
                   content_family_key, script_text, short_video_title, title_source,
                   script_source, publish_purpose, cart_enabled, content_branch, audio_mode
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
                script_source=str(row["script_source"] or ""),
                publish_purpose=str(row["publish_purpose"] or ""),
                cart_enabled=str(row["cart_enabled"] or ""),
                content_branch=str(row["content_branch"] or ""),
                audio_mode=str(row["audio_mode"] or ""),
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
        local_file_path: Optional[str],
        download_status: str,
        run_video_status: str,
        publish_status: str = "待排期",
        canonical_script_key: str = "",
        media_kind: str = "video",
        photo_manifest_json: Optional[Dict[str, Any]] = None,
    ) -> None:
        if media_kind not in {"video", "native_photo"}:
            raise ValueError(f"不支持的发布素材类型：{media_kind}")
        if media_kind == "native_photo":
            if local_file_path:
                raise ValueError("原生照片必须使用图片清单，不能设置视频路径")
            local_file_path = None
            if not isinstance(photo_manifest_json, dict):
                raise ValueError("原生照片缺少图片清单")
        elif photo_manifest_json is not None:
            raise ValueError("视频素材不能携带照片清单")
        photo_json = (json.dumps(photo_manifest_json, ensure_ascii=False, sort_keys=True)
                      if photo_manifest_json is not None else None)
        now = self._now_text()
        resolved_key = self._resolve_canonical_for_write(
            canonical_script_key=canonical_script_key,
            script_id=script_id,
        )
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT media_kind,photo_manifest_json,publish_status FROM video_assets "
                "WHERE canonical_script_key=?", (resolved_key,),
            ).fetchone()
            if (existing and "native_photo" in {existing["media_kind"], media_kind}
                    and existing["publish_status"] in {"提交中", "提交结果不明", "已排期", "已发布"}
                    and (existing["media_kind"] != media_kind or existing["photo_manifest_json"] != photo_json)):
                raise ValueError("照片已进入发布流程，不能替换冻结图片清单")
            conn.execute(
                """
                INSERT INTO video_assets (
                    canonical_script_key, script_id, run_manager_record_id, video_source_type, video_source_value,
                    local_file_path, download_status, run_video_status, publish_status, created_at, updated_at,
                    media_kind, photo_manifest_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(canonical_script_key) DO UPDATE SET
                    script_id = excluded.script_id,
                    run_manager_record_id = excluded.run_manager_record_id,
                    video_source_type = excluded.video_source_type,
                    video_source_value = excluded.video_source_value,
                    local_file_path = excluded.local_file_path,
                    media_kind = excluded.media_kind,
                    photo_manifest_json = excluded.photo_manifest_json,
                    download_status = excluded.download_status,
                    run_video_status = excluded.run_video_status,
                    publish_status = CASE
                        WHEN video_assets.publish_status IN ('提交中', '提交结果不明', '已排期', '已发布', '发布失败', '已跳过') THEN video_assets.publish_status
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
                    media_kind,
                    photo_json,
                ),
            )

    def mark_video_waiting_voiceover(self, identifier: str) -> int:
        resolved_key = self._resolve_canonical_for_write(canonical_script_key=identifier)
        if not resolved_key:
            return 0
        with self._connect() as conn:
            return int(
                conn.execute(
                    """
                    UPDATE video_assets
                    SET publish_status='等待口播', updated_at=?
                    WHERE canonical_script_key=?
                      AND publish_status IN ('待排期', '发布失败')
                    """,
                    (self._now_text(), resolved_key),
                ).rowcount
                or 0
            )

    def update_short_video_title(
        self,
        *,
        canonical_script_key: str,
        short_video_title: str,
        title_source: str = "run_manager_manual",
    ) -> int:
        resolved_key = self._resolve_canonical_for_write(canonical_script_key=canonical_script_key)
        title = str(short_video_title or "").strip()
        if not resolved_key or not title:
            return 0
        now = self._now_text()
        with self._connect() as conn:
            return int(
                conn.execute(
                    """
                    UPDATE script_metadata
                    SET short_video_title = ?,
                        title_source = ?,
                        updated_at = ?
                    WHERE canonical_script_key = ?
                    """,
                    (title, str(title_source or "run_manager_manual").strip(), now, resolved_key),
                ).rowcount
                or 0
            )

    def upsert_manual_publish_request(
        self,
        *,
        record_id: str,
        canonical_script_key: str,
        script_id: str,
        store_id: str,
        account_id: str,
        account_name: str,
        scheduled_for: str,
        publish_channel: str,
        product_id: str,
        short_video_title: str,
        local_file_path: str,
        publish_task_id: str = "",
        request_status: str = "待创建",
        error_message: str = "",
    ) -> None:
        now = self._now_text()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO manual_publish_requests (
                    record_id, canonical_script_key, script_id, store_id, account_id, account_name,
                    scheduled_for, publish_channel, product_id, short_video_title, local_file_path,
                    publish_task_id, request_status, error_message, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_id) DO UPDATE SET
                    canonical_script_key = excluded.canonical_script_key,
                    script_id = excluded.script_id,
                    store_id = excluded.store_id,
                    account_id = excluded.account_id,
                    account_name = excluded.account_name,
                    scheduled_for = excluded.scheduled_for,
                    publish_channel = excluded.publish_channel,
                    product_id = excluded.product_id,
                    short_video_title = excluded.short_video_title,
                    local_file_path = excluded.local_file_path,
                    publish_task_id = COALESCE(NULLIF(excluded.publish_task_id, ''), manual_publish_requests.publish_task_id),
                    request_status = excluded.request_status,
                    error_message = excluded.error_message,
                    updated_at = excluded.updated_at
                """,
                (
                    record_id,
                    canonical_script_key,
                    script_id,
                    store_id,
                    account_id,
                    account_name,
                    scheduled_for,
                    publish_channel,
                    product_id,
                    short_video_title,
                    local_file_path,
                    publish_task_id,
                    request_status,
                    error_message,
                    now,
                    now,
                ),
            )

    def mark_manual_publish_request(
        self,
        *,
        record_id: str,
        request_status: str,
        publish_task_id: str = "",
        error_message: str = "",
    ) -> None:
        now = self._now_text()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE manual_publish_requests
                SET request_status = ?,
                    publish_task_id = COALESCE(NULLIF(?, ''), publish_task_id),
                    error_message = ?,
                    updated_at = ?
                WHERE record_id = ?
                """,
                (request_status, publish_task_id, error_message, now, record_id),
            )

    def get_account_config_by_name(self, account_name: str) -> Optional[sqlite3.Row]:
        text = str(account_name or "").strip()
        if not text:
            return None
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM account_configs
                WHERE account_name = ?
                ORDER BY account_status = '可用' DESC, account_id ASC
                LIMIT 2
                """,
                (text,),
            ).fetchall()
        if len(rows) == 1:
            return rows[0]
        return None

    def get_slot_for_account_time(self, *, account_id: str, scheduled_for: str) -> Optional[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT *
                FROM publish_slots
                WHERE account_id = ?
                  AND scheduled_for = ?
                LIMIT 1
                """,
                (str(account_id or "").strip(), str(scheduled_for or "").strip()),
            ).fetchone()

    def get_manual_publish_slot(self, record_id: str) -> Optional[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT *
                FROM publish_slots
                WHERE manual_request_record_id = ?
                ORDER BY updated_at DESC, slot_id DESC
                LIMIT 1
                """,
                (str(record_id or "").strip(),),
            ).fetchone()

    def assign_manual_slot(
        self,
        *,
        record_id: str,
        store_id: str,
        account_id: str,
        account_name: str,
        scheduled_for: str,
        canonical_script_key: str,
        script_id: str,
        publish_task_id: str,
        title_override: str,
        channel_override: str,
    ) -> Dict[str, Any]:
        now = self._now_text()
        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT *
                FROM publish_slots
                WHERE account_id = ?
                  AND scheduled_for = ?
                LIMIT 1
                """,
                (account_id, scheduled_for),
            ).fetchone()
            if existing is not None:
                existing_task_id = str(existing["publish_task_id"] or "").strip()
                existing_status = str(existing["schedule_status"] or "").strip()
                existing_record_id = str(existing["manual_request_record_id"] or "").strip()
                if existing_status == "已发布" or (
                    existing_status == "已排期"
                    and existing_task_id
                    and existing_record_id != str(record_id or "").strip()
                ):
                    return {
                        "assigned": 0,
                        "conflict": 1,
                        "error": (
                            "已有远端发布任务冲突："
                            f"slot_id={existing['slot_id']}，"
                            f"任务ID={existing_task_id or '-'}，"
                            f"状态={existing_status}"
                        ),
                    }
                conn.execute(
                    """
                    UPDATE publish_slots
                    SET store_id = ?,
                        account_name = ?,
                        canonical_script_key = ?,
                        script_id = ?,
                        schedule_status = '已排期',
                        publish_task_id = ?,
                        error_message = NULL,
                        slot_source = 'manual',
                        manual_request_record_id = ?,
                        title_override = ?,
                        channel_override = ?,
                        updated_at = ?
                    WHERE slot_id = ?
                    """,
                    (
                        store_id,
                        account_name,
                        canonical_script_key,
                        script_id,
                        publish_task_id,
                        record_id,
                        title_override,
                        channel_override,
                        now,
                        int(existing["slot_id"]),
                    ),
                )
                slot_id = int(existing["slot_id"])
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO publish_slots (
                        store_id, account_id, account_name, scheduled_for,
                        canonical_script_key, script_id, schedule_status, publish_task_id,
                        error_message, slot_source, manual_request_record_id, title_override, channel_override,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, '已排期', ?, NULL, 'manual', ?, ?, ?, ?, ?)
                    """,
                    (
                        store_id,
                        account_id,
                        account_name,
                        scheduled_for,
                        canonical_script_key,
                        script_id,
                        publish_task_id,
                        record_id,
                        title_override,
                        channel_override,
                        now,
                        now,
                    ),
                )
                slot_id = int(cursor.lastrowid)

            conn.execute(
                """
                UPDATE video_assets
                SET publish_status = '已排期',
                    account_id = ?,
                    account_name = ?,
                    planned_publish_at = ?,
                    published_at = NULL,
                    publish_task_id = ?,
                    publish_result = NULL,
                    error_message = NULL,
                    updated_at = ?
                WHERE canonical_script_key = ?
                """,
                (account_id, account_name, scheduled_for, publish_task_id, now, canonical_script_key),
            )
            conn.execute(
                """
                UPDATE manual_publish_requests
                SET publish_task_id = ?,
                    request_status = '已创建',
                    error_message = '',
                    updated_at = ?
                WHERE record_id = ?
                """,
                (publish_task_id, now, record_id),
            )
        return {"assigned": 1, "conflict": 0, "slot_id": slot_id}

    def disable_product(self, product_id: str, reason: str = "") -> Dict[str, int]:
        product = str(product_id or "").strip()
        if not product:
            return {"disabled_products": 0, "video_assets_skipped": 0, "slots_cancelled": 0}
        now = self._now_text()
        message = str(reason or "产品已下架，停止自动发布").strip()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO disabled_products (product_id, reason, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                    reason = excluded.reason,
                    updated_at = excluded.updated_at
                """,
                (product, message, now, now),
            )
            assets = conn.execute(
                """
                UPDATE video_assets
                SET publish_status = '已跳过',
                    publish_result = '已跳过',
                    error_message = ?,
                    updated_at = ?
                WHERE canonical_script_key IN (
                    SELECT canonical_script_key
                    FROM script_metadata
                    WHERE product_id = ?
                )
                  AND publish_status IN ('待排期', '已排期', '发布失败')
                """,
                (message, now, product),
            ).rowcount
            slots = conn.execute(
                """
                UPDATE publish_slots
                SET schedule_status = '已取消',
                    updated_at = ?
                WHERE canonical_script_key IN (
                    SELECT canonical_script_key
                    FROM script_metadata
                    WHERE product_id = ?
                )
                  AND schedule_status IN ('待排期', '已排期')
                """,
                (now, product),
            ).rowcount
        return {
            "disabled_products": 1,
            "video_assets_skipped": int(assets or 0),
            "slots_cancelled": int(slots or 0),
        }

    def disable_account(self, account_id: str, reason: str = "") -> Dict[str, int]:
        account = str(account_id or "").strip()
        if not account:
            return {"paused_accounts": 0, "future_slots_cancelled": 0, "assets_skipped": 0}
        now = self._now_text()
        message = str(reason or "账号连续发布失败，暂停自动发布").strip()
        with self._connect() as conn:
            paused = conn.execute(
                """
                UPDATE account_configs
                SET account_status = '暂停', updated_at = ?
                WHERE account_id = ?
                  AND account_status <> '暂停'
                """,
                (now, account),
            ).rowcount
            future_rows = conn.execute(
                """
                SELECT DISTINCT canonical_script_key
                FROM publish_slots
                WHERE account_id = ?
                  AND scheduled_for >= ?
                  AND schedule_status IN ('待排期', '已排期')
                  AND canonical_script_key IS NOT NULL
                """,
                (account, now),
            ).fetchall()
            canonical_keys = [
                str(row["canonical_script_key"] or "").strip()
                for row in future_rows
                if str(row["canonical_script_key"] or "").strip()
            ]
            slots = conn.execute(
                """
                UPDATE publish_slots
                SET schedule_status = '已取消',
                    error_message = ?,
                    updated_at = ?
                WHERE account_id = ?
                  AND scheduled_for >= ?
                  AND schedule_status IN ('待排期', '已排期')
                """,
                (message, now, account, now),
            ).rowcount
            assets = 0
            for canonical_key in canonical_keys:
                assets += conn.execute(
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
                      AND publish_status = '已排期'
                    """,
                    (now, canonical_key),
                ).rowcount
        return {
            "paused_accounts": int(paused or 0),
            "future_slots_cancelled": int(slots or 0),
            "assets_requeued": int(assets or 0),
        }

    @staticmethod
    def normalize_schedule_strategy(value: str) -> str:
        text = str(value or "").strip()
        if text in {"优先", "暂停"}:
            return text
        return "普通"

    def upsert_product_schedule_preferences(self, preferences: Iterable[Dict[str, Any]]) -> Dict[str, int]:
        rows = [
            {
                "store_id": str(item.get("store_id") or "").strip(),
                "product_id": str(item.get("product_id") or "").strip(),
                "schedule_strategy": self.normalize_schedule_strategy(str(item.get("schedule_strategy") or "")),
                "schedule_note": str(item.get("schedule_note") or "").strip(),
            }
            for item in preferences
        ]
        rows = [item for item in rows if item["store_id"] and item["product_id"]]
        if not rows:
            return {"preferences_upserted": 0, "priority_timestamps_updated": 0}

        now = self._now_text()
        upserted = 0
        priority_timestamps_updated = 0
        with self._connect() as conn:
            for item in rows:
                existing = conn.execute(
                    """
                    SELECT schedule_strategy, schedule_note, priority_updated_at
                    FROM product_schedule_preferences
                    WHERE store_id = ? AND product_id = ?
                    """,
                    (item["store_id"], item["product_id"]),
                ).fetchone()
                old_strategy = str(existing["schedule_strategy"] or "普通").strip() if existing else "普通"
                old_note = str(existing["schedule_note"] or "").strip() if existing else ""
                old_priority_updated_at = str(existing["priority_updated_at"] or "").strip() if existing else ""
                priority_updated_at = old_priority_updated_at
                if item["schedule_strategy"] == "优先" and (
                    old_strategy != "优先" or old_note != item["schedule_note"]
                ):
                    priority_updated_at = now
                    priority_timestamps_updated += 1
                if item["schedule_strategy"] != "优先":
                    priority_updated_at = ""

                conn.execute(
                    """
                    INSERT INTO product_schedule_preferences (
                        store_id, product_id, schedule_strategy, schedule_note,
                        priority_updated_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(store_id, product_id) DO UPDATE SET
                        schedule_strategy = excluded.schedule_strategy,
                        schedule_note = excluded.schedule_note,
                        priority_updated_at = excluded.priority_updated_at,
                        updated_at = excluded.updated_at
                    """,
                    (
                        item["store_id"],
                        item["product_id"],
                        item["schedule_strategy"],
                        item["schedule_note"],
                        priority_updated_at,
                        now,
                        now,
                    ),
                )
                upserted += 1
        return {
            "preferences_upserted": upserted,
            "priority_timestamps_updated": priority_timestamps_updated,
        }

    def enforce_retry_limit(self, max_auto_retries: int = 2, reason: str = "") -> Dict[str, Any]:
        """把超过自动重试上限的视频移出自动排期池。

        这里的 max_auto_retries 表示“失败后最多再自动尝试几次”。超过上限后不再
        自动复活，否则同一个异常脚本会在每轮补排中反复进入候选池。
        """
        limit = max(0, int(max_auto_retries))
        now = self._now_text()
        message = str(reason or f"超过自动重试上限，已停止自动重排；失败次数 > {limit}").strip()
        with self._connect() as conn:
            rows = conn.execute(
                """
                WITH failed AS (
                    SELECT canonical_script_key, COUNT(*) AS failed_count
                    FROM publish_slots
                    WHERE schedule_status = '发布失败'
                    GROUP BY canonical_script_key
                )
                SELECT va.canonical_script_key,
                       va.script_id,
                       va.publish_task_id,
                       va.planned_publish_at,
                       va.account_id,
                       va.account_name,
                       failed.failed_count
                FROM failed
                INNER JOIN video_assets va ON va.canonical_script_key = failed.canonical_script_key
                WHERE failed.failed_count > ?
                  AND va.publish_status IN ('待排期', '已排期', '发布失败', '已跳过')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM publish_slots published
                      WHERE published.canonical_script_key = va.canonical_script_key
                        AND published.schedule_status = '已发布'
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM publish_slots active
                      WHERE active.canonical_script_key = va.canonical_script_key
                        AND active.schedule_status = '已排期'
                        AND COALESCE(active.publish_task_id, '') <> ''
                  )
                ORDER BY failed.failed_count DESC, va.script_id ASC
                """,
                (limit,),
            ).fetchall()
            keys = [
                str(row["canonical_script_key"] or "").strip()
                for row in rows
                if str(row["canonical_script_key"] or "").strip()
            ]
            remote_task_ids = [
                str(row["publish_task_id"] or "").strip()
                for row in rows
                if str(row["publish_task_id"] or "").strip()
            ]
            skipped = 0
            slots = 0
            for key in keys:
                skipped += conn.execute(
                    """
                    UPDATE video_assets
                    SET publish_status = '已跳过',
                        account_id = NULL,
                        account_name = NULL,
                        planned_publish_at = NULL,
                        publish_task_id = NULL,
                        publish_result = '发布失败',
                        error_message = ?,
                        updated_at = ?
                    WHERE canonical_script_key = ?
                      AND publish_status IN ('待排期', '已排期', '发布失败', '已跳过')
                    """,
                    (message, now, key),
                ).rowcount
                slots += conn.execute(
                    """
                    UPDATE publish_slots
                    SET schedule_status = '已取消',
                        error_message = ?,
                        updated_at = ?
                    WHERE canonical_script_key = ?
                      AND schedule_status IN ('待排期', '已排期')
                    """,
                    (message, now, key),
                ).rowcount

        return {
            "max_auto_retries": limit,
            "candidates": len(rows),
            "video_assets_skipped": int(skipped or 0),
            "video_assets_requeued": 0,
            "active_slots_cancelled": int(slots or 0),
            "remote_task_ids": remote_task_ids,
            "items": [
                {
                    "script_id": str(row["script_id"] or ""),
                    "canonical_script_key": str(row["canonical_script_key"] or ""),
                    "failed_count": int(row["failed_count"] or 0),
                    "skipped": True,
                    "account_id": str(row["account_id"] or ""),
                    "account_name": str(row["account_name"] or ""),
                    "planned_publish_at": str(row["planned_publish_at"] or ""),
                    "publish_task_id": str(row["publish_task_id"] or ""),
                }
                for row in rows
            ],
        }

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
                    publish_channel, publish_time_1, publish_time_2, publish_time_3,
                    nurture_enabled, nurture_daily_count, nurture_only, initialization_enabled,
                    publish_profile_id, provider_connection_uid, account_timezone,
                    delivery_mode, provider_health, provider_checked_at,
                    photo_content_profile_json, photo_claim_scope,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    account_name = excluded.account_name,
                    store_id = excluded.store_id,
                    account_status = excluded.account_status,
                    publish_channel = excluded.publish_channel,
                    publish_time_1 = excluded.publish_time_1,
                    publish_time_2 = excluded.publish_time_2,
                    publish_time_3 = excluded.publish_time_3,
                    nurture_enabled = excluded.nurture_enabled,
                    nurture_daily_count = excluded.nurture_daily_count,
                    nurture_only = excluded.nurture_only,
                    initialization_enabled = excluded.initialization_enabled,
                    publish_profile_id = COALESCE(NULLIF(excluded.publish_profile_id, ''), account_configs.publish_profile_id),
                    provider_connection_uid = COALESCE(NULLIF(excluded.provider_connection_uid, ''), account_configs.provider_connection_uid),
                    account_timezone = COALESCE(NULLIF(excluded.account_timezone, ''), account_configs.account_timezone),
                    delivery_mode = COALESCE(NULLIF(excluded.delivery_mode, ''), account_configs.delivery_mode),
                    provider_health = COALESCE(NULLIF(excluded.provider_health, ''), account_configs.provider_health),
                    provider_checked_at = COALESCE(NULLIF(excluded.provider_checked_at, ''), account_configs.provider_checked_at),
                    photo_content_profile_json = COALESCE(NULLIF(excluded.photo_content_profile_json, ''), account_configs.photo_content_profile_json),
                    photo_claim_scope = COALESCE(NULLIF(excluded.photo_claim_scope, ''), account_configs.photo_claim_scope),
                    updated_at = excluded.updated_at
                """,
                [
                    (
                        item.account_id,
                        item.account_name,
                        item.store_id,
                        item.account_status,
                        item.publish_channel,
                        item.publish_time_1,
                        item.publish_time_2,
                        item.publish_time_3,
                        1 if item.nurture_enabled else 0,
                        int(item.nurture_daily_count or 2),
                        1 if item.nurture_only else 0,
                        1 if item.initialization_enabled else 0,
                        str(item.publish_profile_id or "").strip(),
                        str(item.provider_connection_uid or "").strip(),
                        str(item.account_timezone or "").strip(),
                        str(item.delivery_mode or "").strip(),
                        str(item.provider_health or "").strip(),
                        str(item.provider_checked_at or "").strip(),
                        str(getattr(item, "photo_content_profile_json", "") or "").strip(),
                        str(getattr(item, "photo_claim_scope", "") or "").strip(),
                        now,
                        now,
                    )
                    for item in rows
                ],
            )
            active_keys = {
                (str(item.store_id or "").strip(), str(item.account_name or "").strip())
                for item in rows
                if str(item.store_id or "").strip() and str(item.account_name or "").strip()
            }
            for store_id, account_name in active_keys:
                current_ids = [
                    str(item.account_id or "").strip()
                    for item in rows
                    if str(item.store_id or "").strip() == store_id
                    and str(item.account_name or "").strip() == account_name
                    and str(item.account_id or "").strip()
                ]
                if not current_ids:
                    continue
                placeholders = ",".join("?" for _ in current_ids)
                stale_accounts = conn.execute(
                    f"""
                    SELECT account_id
                    FROM account_configs
                    WHERE store_id = ?
                      AND account_name = ?
                      AND account_id NOT IN ({placeholders})
                      AND account_status <> '暂停'
                    """,
                    (store_id, account_name, *current_ids),
                ).fetchall()
                for stale in stale_accounts:
                    stale_account_id = str(stale["account_id"] or "").strip()
                    if not stale_account_id:
                        continue
                    conn.execute(
                        """
                        UPDATE account_configs
                        SET account_status = '暂停', updated_at = ?
                        WHERE account_id = ?
                        """,
                        (now, stale_account_id),
                    )
                    conn.execute(
                        """
                        UPDATE publish_slots
                        SET schedule_status = '已取消',
                            error_message = ?,
                            updated_at = ?
                        WHERE account_id = ?
                          AND scheduled_for >= ?
                          AND schedule_status IN ('待排期', '已排期')
                        """,
                        ("账号ID已在源表更新，旧账号自动暂停并取消未来排期", now, stale_account_id, now),
                    )
        return len(rows)

    def replace_account_channel_bindings(
        self, account_id: str, bindings: Iterable[Dict[str, Any]]
    ) -> int:
        """Replace one account's channel bindings (content-scope split)."""
        account_id = str(account_id or "").strip()
        now_text = self._now_text()
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM account_channel_bindings WHERE account_id = ?",
                (account_id,),
            )
            written = 0
            for binding in bindings:
                channel = str(binding.get("publish_channel") or "").strip()
                if not channel:
                    continue
                conn.execute(
                    """
                    INSERT INTO account_channel_bindings (
                        account_id, publish_channel, content_scope,
                        publish_time_1, publish_time_2, publish_time_3,
                        enabled, source_record_id, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_id,
                        channel,
                        str(binding.get("content_scope") or "all"),
                        binding.get("publish_time_1"),
                        binding.get("publish_time_2"),
                        binding.get("publish_time_3"),
                        1 if binding.get("enabled", True) else 0,
                        binding.get("source_record_id"),
                        now_text,
                        now_text,
                    ),
                )
                written += 1
            return written

    def list_account_channel_bindings(self, account_id: str = "") -> List[sqlite3.Row]:
        with self._connect() as conn:
            if account_id:
                return conn.execute(
                    """
                    SELECT * FROM account_channel_bindings
                    WHERE account_id = ? AND enabled = 1
                    ORDER BY publish_channel
                    """,
                    (str(account_id).strip(),),
                ).fetchall()
            return conn.execute(
                """
                SELECT * FROM account_channel_bindings
                WHERE enabled = 1
                ORDER BY account_id, publish_channel
                """
            ).fetchall()

    def mark_slot_channel(self, slot_id: int, publish_channel: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE publish_slots SET publish_channel_used = ?, updated_at = ? "
                "WHERE slot_id = ?",
                (str(publish_channel or ""), self._now_text(), int(slot_id)),
            )

    def generate_future_slots(self, now: datetime, window_hours: int = 24) -> int:
        end_at = now + timedelta(hours=window_hours)
        created = 0
        with self._connect() as conn:
            accounts = conn.execute(
                """
                SELECT account_id, account_name, store_id, publish_time_1, publish_time_2, publish_time_3,
                       publish_channel
                FROM account_configs
                WHERE account_status = '可用'
                  AND COALESCE(publish_channel, 'GeeLark') NOT IN ('手动', '暂停')
                """
            ).fetchall()
            bindings_by_account: Dict[str, List[sqlite3.Row]] = {}
            for binding in conn.execute(
                "SELECT * FROM account_channel_bindings WHERE enabled = 1"
            ).fetchall():
                bindings_by_account.setdefault(
                    str(binding["account_id"] or ""), []
                ).append(binding)
            for account in accounts:
                account_bindings = bindings_by_account.get(
                    str(account["account_id"] or ""), []
                )
                if account_bindings:
                    # Dual-channel accounts: each binding owns its schedule
                    # windows and channel stamp (rows come from the account
                    # sheet, one row per channel).
                    windows = [
                        (
                            [
                                str(b["publish_time_1"] or "").strip(),
                                str(b["publish_time_2"] or "").strip(),
                                str(b["publish_time_3"] or "").strip(),
                            ],
                            str(b["publish_channel"] or ""),
                        )
                        for b in account_bindings
                    ]
                else:
                    windows = [
                        (
                            [
                                str(account["publish_time_1"] or "").strip(),
                                str(account["publish_time_2"] or "").strip(),
                                str(account["publish_time_3"] or "").strip(),
                            ],
                            "",
                        )
                    ]
                for times, window_channel in windows:
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
                            # Backfill legacy slots at this window that predate
                            # channel stamping (INSERT OR IGNORE skips them).
                            conn.execute(
                                """
                                UPDATE publish_slots SET publish_channel_used = ?, updated_at = ?
                                WHERE account_id = ?
                                  AND scheduled_for LIKE ?
                                  AND publish_channel_used IS NULL
                                  AND schedule_status = '待排期'
                                """,
                                (
                                    window_channel or str(account["publish_channel"] or ""),
                                    self._now_text(),
                                    str(account["account_id"] or ""),
                                    "% " + parsed.strftime("%H:%M") + ":00",
                                ),
                            )
                            before = conn.total_changes
                            conn.execute(
                                """
                                INSERT OR IGNORE INTO publish_slots (
                                    store_id, account_id, account_name, scheduled_for,
                                    schedule_status, publish_channel_used, created_at, updated_at
                                ) VALUES (?, ?, ?, ?, '待排期', ?, ?, ?)
                                """,
                                (
                                    str(account["store_id"] or ""),
                                    str(account["account_id"] or ""),
                                    str(account["account_name"] or ""),
                                    slot_time.strftime("%Y-%m-%d %H:%M:%S"),
                                    window_channel or str(account["publish_channel"] or ""),
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
                SELECT ps.*, ac.publish_channel AS publish_channel
                FROM publish_slots ps
                INNER JOIN account_configs ac ON ac.account_id = ps.account_id
                WHERE ps.scheduled_for >= ?
                  AND ps.scheduled_for <= ?
                  AND ps.schedule_status = '待排期'
                  AND ac.account_status = '可用'
                  AND COALESCE(ac.publish_channel, 'GeeLark') NOT IN ('手动', '暂停')
                ORDER BY ps.scheduled_for ASC, ps.account_id ASC
                """,
                (now.strftime("%Y-%m-%d %H:%M:%S"), end_at.strftime("%Y-%m-%d %H:%M:%S")),
            ).fetchall()

    def list_ready_candidates(self, store_id: str) -> List[PublishCandidate]:
        pause_mixcut = 1 if self.is_mixcut_scheduling_paused() else 0
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT sm.canonical_script_key, sm.script_id, sm.store_id, sm.product_id, sm.content_family_key,
                       sm.short_video_title, va.local_file_path, va.video_source_type, va.video_source_value,
                       va.media_kind, va.photo_manifest_json,
                       sm.source_record_id, sm.script_slot,
                       sm.script_source, sm.publish_purpose, sm.cart_enabled, sm.content_branch, sm.audio_mode,
                       sm.target_country, sm.script_text, sm.target_publish_account_id,
                       pool.platform_product_id, pool.canonical_script_key AS pool_key,
                       COALESCE(psp.schedule_strategy, '普通') AS schedule_strategy,
                       COALESCE(psp.priority_updated_at, '') AS priority_updated_at
                FROM script_metadata sm
                INNER JOIN video_assets va ON va.canonical_script_key = sm.canonical_script_key
                LEFT JOIN script_pool_bindings pool ON pool.canonical_script_key = sm.canonical_script_key
                LEFT JOIN product_schedule_preferences psp
                    ON psp.store_id = sm.store_id AND psp.product_id = sm.product_id
                WHERE sm.store_id = ?
                  AND COALESCE(sm.short_video_title, '') <> ''
                  AND (
                      (va.media_kind = 'video' AND COALESCE(va.local_file_path, '') <> '')
                      OR (va.media_kind = 'native_photo' AND COALESCE(va.photo_manifest_json, '') <> '')
                  )
                  AND va.download_status = '下载成功'
                  AND va.publish_status = '待排期'
                  AND sm.canonical_script_key NOT LIKE 'manual:%'
                  AND COALESCE(psp.schedule_strategy, '普通') <> '暂停'
                  AND NOT (
                      ? = 1
                      AND (
                          COALESCE(sm.script_source, '') LIKE '%混剪%'
                          OR COALESCE(sm.publish_purpose, '') LIKE '%混剪%'
                          OR COALESCE(sm.content_branch, '') LIKE '%混剪%'
                          OR sm.canonical_script_key LIKE 'mixcut:%'
                          OR COALESCE(va.video_source_type, '') = 'mixcut_output'
                      )
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM publish_slots published
                      WHERE published.canonical_script_key = sm.canonical_script_key
                        AND published.schedule_status = '已发布'
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM publish_slots active
                      WHERE active.canonical_script_key = sm.canonical_script_key
                        AND (
                            active.schedule_status = '提交中'
                            OR (
                                active.schedule_status = '已排期'
                                AND COALESCE(active.publish_task_id, '') <> ''
                            )
                        )
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM disabled_products dp
                      WHERE dp.product_id = sm.product_id
                  )
                ORDER BY CASE WHEN COALESCE(psp.schedule_strategy, '普通') = '优先' THEN 0 ELSE 1 END ASC,
                         COALESCE(psp.priority_updated_at, '') DESC,
                         CASE
                             WHEN COALESCE(sm.product_id, '') = ''
                                  OR COALESCE(sm.cart_enabled, '') = '否'
                                  OR COALESCE(sm.script_source, '') = '养号复刻'
                                  OR COALESCE(sm.publish_purpose, '') = '养号'
                                  OR COALESCE(sm.content_branch, '') = '非商品展示型'
                             THEN 9
                             WHEN COALESCE(sm.script_source, '') LIKE '%复刻%'
                                  OR COALESCE(sm.publish_purpose, '') LIKE '%复刻%'
                             THEN 0
                             WHEN COALESCE(sm.script_source, '') LIKE '%混剪%'
                                  OR COALESCE(sm.publish_purpose, '') LIKE '%混剪%'
                             THEN 1
                             ELSE 2
                         END ASC,
                         sm.updated_at ASC,
                         sm.script_id ASC,
                         sm.canonical_script_key ASC
                """,
                (store_id, pause_mixcut),
            ).fetchall()
        candidates: List[PublishCandidate] = []
        for row in rows:
            context = _candidate_context(str(row["script_text"] or ""))
            is_photo = row["media_kind"] == "native_photo"
            video_value = (
                str(row["video_source_value"] or "").strip()
                if str(row["video_source_type"] or "").strip() == "link"
                and str(row["video_source_value"] or "").strip().startswith(("http://", "https://"))
                else str(row["local_file_path"] or "")
            )
            if is_photo:
                try:
                    media_paths = _photo_candidate_paths(
                        str(row["photo_manifest_json"] or ""), str(row["script_text"] or ""),
                        str(row["script_id"] or ""),
                    )
                except (ValueError, TypeError, OSError):
                    # Incomplete/stale releases never become publish candidates.
                    continue
            else:
                media_paths = [video_value]
            candidates.append(
                PublishCandidate(
                    canonical_script_key=str(row["canonical_script_key"] or ""),
                    script_id=str(row["script_id"] or ""),
                    store_id=str(row["store_id"] or ""),
                    product_id=str(row["product_id"] or ""),
                    content_family_key=str(row["content_family_key"] or ""),
                    short_video_title=str(row["short_video_title"] or ""),
                    local_file_path=str(row["local_file_path"] or ""),
                    publish_video_value="" if is_photo else video_value,
                    content_type="photo" if is_photo else "video",
                    media_paths=media_paths,
                    source_record_id=str(row["source_record_id"] or ""),
                    script_slot=str(row["script_slot"] or ""),
                    script_source=str(row["script_source"] or ""),
                    publish_purpose=str(row["publish_purpose"] or ""),
                    cart_enabled=str(row["cart_enabled"] or ""),
                    content_branch=str(row["content_branch"] or ""),
                    audio_mode=str(row["audio_mode"] or ""),
                    schedule_strategy=str(row["schedule_strategy"] or "普通"),
                    priority_updated_at=str(row["priority_updated_at"] or ""),
                    platform_product_id=str(row["platform_product_id"] or ""),
                    script_pool_registered=bool(row["pool_key"]),
                    target_country=str(row["target_country"] or ""),
                    script_text=str(row["script_text"] or ""),
                    target_publish_account_id=(
                        str(row["target_publish_account_id"] or "")
                        or str(context.get("target_publish_account_id") or "")
                    ),
                    recipe_id=context.get("recipe_id", ""),
                    theme_id=context.get("theme_id", ""),
                    place=str(context.get("place") or ""),
                    source_product_id=(
                        context.get("source_product_id", "")
                        or str(row["product_id"] or "")
                    ),
                )
            )
        return candidates

    def get_account_initialization_progress(
        self, account_id: str, target_count: int = 3
    ) -> Dict[str, Any]:
        """Compute initialization progress from existing OPV publish records."""
        resolved_account_id = str(account_id or "").strip()
        target = max(1, int(target_count or 3))
        with self._connect() as conn:
            account = conn.execute(
                "SELECT * FROM account_configs WHERE account_id = ?",
                (resolved_account_id,),
            ).fetchone()
            rows = conn.execute(
                """
                SELECT ps.canonical_script_key, ps.schedule_status, ps.publish_task_id,
                       COALESCE(va.published_at, ps.scheduled_for) AS published_at
                FROM publish_slots ps
                INNER JOIN script_metadata sm
                    ON sm.canonical_script_key = ps.canonical_script_key
                LEFT JOIN video_assets va
                    ON va.canonical_script_key = ps.canonical_script_key
                WHERE ps.account_id = ?
                  AND ps.canonical_script_key LIKE 'opv:%'
                  AND sm.script_source = '图文养号'
                  AND sm.publish_purpose = '养号'
                  AND ps.schedule_status IN ('已排期', '已发布')
                ORDER BY ps.scheduled_for, ps.slot_id
                """,
                (resolved_account_id,),
            ).fetchall()

        published_keys = {
            str(row["canonical_script_key"] or "").strip()
            for row in rows
            if str(row["schedule_status"] or "").strip() == "已发布"
        }
        scheduled_keys = {
            str(row["canonical_script_key"] or "").strip()
            for row in rows
            if str(row["schedule_status"] or "").strip() == "已排期"
            and str(row["publish_task_id"] or "").strip()
        } - published_keys
        published_times = [
            str(row["published_at"] or "").strip()
            for row in rows
            if str(row["schedule_status"] or "").strip() == "已发布"
            and str(row["published_at"] or "").strip()
        ]
        published_count = len(published_keys)
        scheduled_count = len(scheduled_keys)
        remaining_count = max(0, target - published_count - scheduled_count)
        enabled = bool(account and int(account["initialization_enabled"] or 0))
        if published_count >= target:
            status = "COMPLETED"
        elif not enabled:
            status = "DISABLED"
        elif published_count or scheduled_count:
            status = "RUNNING"
        else:
            status = "PENDING"
        return {
            "account_id": resolved_account_id,
            "enabled": enabled,
            "target_count": target,
            "published_count": published_count,
            "scheduled_count": scheduled_count,
            "remaining_count": remaining_count,
            "status": status,
            "first_published_at": min(published_times) if published_times else "",
            "last_published_at": max(published_times) if published_times else "",
        }

    def list_initializing_accounts(
        self, *, target_count: int = 3, store_id: str = "", include_completed: bool = False
    ) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            sql = """
                SELECT account_id, account_name, store_id
                FROM account_configs
                WHERE initialization_enabled = 1
            """
            params: List[Any] = []
            if str(store_id or "").strip():
                sql += " AND store_id = ?"
                params.append(str(store_id).strip())
            sql += " ORDER BY store_id, account_name, account_id"
            accounts = conn.execute(sql, params).fetchall()
        output: List[Dict[str, Any]] = []
        for account in accounts:
            progress = self.get_account_initialization_progress(
                str(account["account_id"] or ""), target_count=target_count
            )
            if not include_completed and progress["status"] == "COMPLETED":
                continue
            progress.update(
                {
                    "account_name": str(account["account_name"] or ""),
                    "store_id": str(account["store_id"] or ""),
                }
            )
            output.append(progress)
        return output

    def list_account_initialization_recipe_ids(self, account_id: str) -> set[str]:
        """Return recipes already reserved or published for an initializing account."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT sm.script_text
                FROM publish_slots ps
                INNER JOIN script_metadata sm
                    ON sm.canonical_script_key = ps.canonical_script_key
                WHERE ps.account_id = ?
                  AND ps.canonical_script_key LIKE 'opv:%'
                  AND sm.script_source = '图文养号'
                  AND sm.publish_purpose = '养号'
                  AND (
                      ps.schedule_status = '已发布'
                      OR (
                          ps.schedule_status = '已排期'
                          AND COALESCE(ps.publish_task_id, '') <> ''
                      )
                  )
                """,
                (str(account_id or "").strip(),),
            ).fetchall()
        return {
            recipe_id
            for row in rows
            if (recipe_id := _candidate_context(str(row["script_text"] or "")).get("recipe_id", ""))
        }

    def count_scheduled_initialization_for_account_day(
        self, account_id: str, target_time: datetime
    ) -> int:
        """Count OPV initialization reservations on the account's local calendar day."""
        day_start = datetime.combine(target_time.date(), time.min).strftime("%Y-%m-%d %H:%M:%S")
        day_end = datetime.combine(target_time.date(), time.max).strftime("%Y-%m-%d %H:%M:%S")
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT ps.canonical_script_key) AS count
                FROM publish_slots ps
                INNER JOIN script_metadata sm
                    ON sm.canonical_script_key = ps.canonical_script_key
                WHERE ps.account_id = ?
                  AND ps.scheduled_for >= ?
                  AND ps.scheduled_for <= ?
                  AND ps.canonical_script_key LIKE 'opv:%'
                  AND sm.script_source = '图文养号'
                  AND sm.publish_purpose = '养号'
                  AND (
                      ps.schedule_status = '已发布'
                      OR (
                          ps.schedule_status = '已排期'
                          AND COALESCE(ps.publish_task_id, '') <> ''
                      )
                  )
                """,
                (str(account_id or "").strip(), day_start, day_end),
            ).fetchone()
        return int(row["count"] or 0) if row else 0

    def count_available_initialization_content(self, *, store_id: str = "") -> int:
        """Count unassigned, render-ready OPV nurture videos."""
        params: List[Any] = []
        store_filter = ""
        if str(store_id or "").strip():
            store_filter = " AND sm.store_id = ?"
            params.append(str(store_id).strip())
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT COUNT(DISTINCT sm.canonical_script_key) AS count
                FROM script_metadata sm
                INNER JOIN video_assets va
                    ON va.canonical_script_key = sm.canonical_script_key
                WHERE sm.canonical_script_key LIKE 'opv:%'
                  AND sm.script_source = '图文养号'
                  AND sm.publish_purpose = '养号'
                  AND COALESCE(sm.short_video_title, '') <> ''
                  AND COALESCE(va.local_file_path, '') <> ''
                  AND va.download_status = '下载成功'
                  AND va.publish_status = '待排期'
                  {store_filter}
                  AND NOT EXISTS (
                      SELECT 1 FROM publish_slots ps
                      WHERE ps.canonical_script_key = sm.canonical_script_key
                        AND ps.schedule_status IN ('已排期', '已发布')
                  )
                """,
                params,
            ).fetchone()
        return int(row["count"] or 0) if row else 0

    def initialization_supply_summary(
        self, *, target_count: int = 3, store_id: str = ""
    ) -> Dict[str, int]:
        accounts = self.list_initializing_accounts(
            target_count=target_count, store_id=store_id, include_completed=True
        )
        required_items = sum(int(item["remaining_count"] or 0) for item in accounts)
        scheduled_items = sum(int(item["scheduled_count"] or 0) for item in accounts)
        ready_pool_items = self.count_available_initialization_content(store_id=store_id)
        completed = sum(1 for item in accounts if item["status"] == "COMPLETED")
        return {
            "initialization_accounts": len(accounts),
            "initialization_pending": len(accounts) - completed,
            "initialization_completed": completed,
            "required_items": required_items,
            "scheduled_items": scheduled_items,
            "ready_pool_items": ready_pool_items,
            "content_gap": max(0, required_items - ready_pool_items),
        }

    def get_account_config(self, account_id: str) -> Optional[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM account_configs WHERE account_id = ?",
                (account_id,),
            ).fetchone()

    def list_account_configs(self, *, publish_channel: str = "") -> List[sqlite3.Row]:
        with self._connect() as conn:
            if publish_channel:
                return conn.execute(
                    """
                    SELECT * FROM account_configs
                    WHERE publish_channel = ?
                    ORDER BY store_id, account_name, account_id
                    """,
                    (str(publish_channel).strip(),),
                ).fetchall()
            return conn.execute(
                "SELECT * FROM account_configs ORDER BY store_id, account_name, account_id"
            ).fetchall()

    def update_account_capabilities(self, capabilities: Iterable[Dict[str, Any]]) -> int:
        rows = list(capabilities)
        if not rows:
            return 0
        now = self._now_text()
        with self._connect() as conn:
            updated = 0
            for item in rows:
                updated += int(
                    conn.execute(
                        """
                        UPDATE account_configs
                        SET organic_capable = ?,
                            shoppable_capable = ?,
                            organic_auth_id = ?,
                            shoppable_auth_id = ?,
                            capability_status = 'ok',
                            capability_checked_at = ?,
                            capability_error = NULL,
                            updated_at = ?
                        WHERE account_id = ?
                        """,
                        (
                            1 if item.get("organic_capable") else 0,
                            1 if item.get("shoppable_capable") else 0,
                            str(item.get("organic_auth_id") or ""),
                            str(item.get("shoppable_auth_id") or ""),
                            now,
                            now,
                            str(item.get("account_id") or "").strip(),
                        ),
                    ).rowcount
                    or 0
                )
        return updated

    def mark_account_capability_error(self, account_ids: Iterable[str], error_message: str) -> int:
        ids = [str(item or "").strip() for item in account_ids if str(item or "").strip()]
        if not ids:
            return 0
        now = self._now_text()
        with self._connect() as conn:
            updated = 0
            for account_id in ids:
                updated += int(
                    conn.execute(
                        """
                        UPDATE account_configs
                        SET capability_status = CASE
                                WHEN capability_status = 'ok' THEN capability_status
                                ELSE 'error'
                            END,
                            capability_error = ?,
                            updated_at = ?
                        WHERE account_id = ?
                        """,
                        (str(error_message or "").strip()[:1000], now, account_id),
                    ).rowcount
                    or 0
                )
        return updated

    def upsert_publisher_profile(self, profile: Dict[str, Any]) -> int:
        """写入发布渠道配置文件（API Key 只存环境变量名）。"""
        profile_id = str(profile.get("profile_id") or "").strip()
        if not profile_id:
            return 0
        now = self._now_text()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO publisher_profiles (
                    profile_id, provider, workspace_name, api_key_env_name, cli_path,
                    enabled, last_health_check_at, last_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(profile_id) DO UPDATE SET
                    provider = excluded.provider,
                    workspace_name = excluded.workspace_name,
                    api_key_env_name = excluded.api_key_env_name,
                    cli_path = excluded.cli_path,
                    enabled = excluded.enabled,
                    last_health_check_at = excluded.last_health_check_at,
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at
                """,
                (
                    profile_id,
                    str(profile.get("provider") or "").strip(),
                    str(profile.get("workspace_name") or "").strip(),
                    str(profile.get("api_key_env_name") or "").strip(),
                    str(profile.get("cli_path") or "").strip(),
                    1 if profile.get("enabled", True) else 0,
                    profile.get("last_health_check_at") or None,
                    profile.get("last_error") or None,
                    now,
                    now,
                ),
            )
        return 1

    def get_publisher_profile(self, profile_id: str) -> Optional[sqlite3.Row]:
        profile_id = str(profile_id or "").strip()
        if not profile_id:
            return None
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM publisher_profiles WHERE profile_id = ?",
                (profile_id,),
            ).fetchone()

    def list_publisher_profiles(self, *, provider: str = "") -> List[sqlite3.Row]:
        with self._connect() as conn:
            if provider:
                return conn.execute(
                    "SELECT * FROM publisher_profiles WHERE provider = ? ORDER BY profile_id",
                    (str(provider).strip(),),
                ).fetchall()
            return conn.execute("SELECT * FROM publisher_profiles ORDER BY profile_id").fetchall()

    def update_account_provider_info(
        self,
        account_id: str,
        *,
        connection_uid: str = "",
        timezone: str = "",
        content_video: Optional[bool] = None,
        content_photo: Optional[bool] = None,
        shop_video: Optional[bool] = None,
        shop_photo: Optional[bool] = None,
        direct_post: Optional[bool] = None,
        delivery_mode: str = "",
        health: str = "ok",
        error: str = "",
        profile_id: str = "",
    ) -> int:
        """把 CreatOK 连接发现/能力结果回写账号配置。"""
        account_id = str(account_id or "").strip()
        if not account_id:
            return 0
        now = self._now_text()
        with self._connect() as conn:
            return int(
                conn.execute(
                    """
                    UPDATE account_configs
                    SET provider_connection_uid = CASE
                            WHEN ? = '' THEN provider_connection_uid ELSE ? END,
                        account_timezone = CASE
                            WHEN ? = '' THEN account_timezone ELSE ? END,
                        content_video_capable = COALESCE(?, content_video_capable),
                        content_photo_capable = COALESCE(?, content_photo_capable),
                        shop_video_capable = COALESCE(?, shop_video_capable),
                        shop_photo_capable = COALESCE(?, shop_photo_capable),
                        direct_post_capable = COALESCE(?, direct_post_capable),
                        delivery_mode = CASE
                            WHEN ? = '' THEN delivery_mode ELSE ? END,
                        publish_profile_id = CASE
                            WHEN ? = '' THEN publish_profile_id ELSE ? END,
                        provider_health = ?,
                        provider_checked_at = ?,
                        provider_error = ?,
                        updated_at = ?
                    WHERE account_id = ?
                    """,
                    (
                        connection_uid, connection_uid,
                        timezone, timezone,
                        None if content_video is None else (1 if content_video else 0),
                        None if content_photo is None else (1 if content_photo else 0),
                        None if shop_video is None else (1 if shop_video else 0),
                        None if shop_photo is None else (1 if shop_photo else 0),
                        None if direct_post is None else (1 if direct_post else 0),
                        delivery_mode, delivery_mode,
                        profile_id, profile_id,
                        str(health or "").strip(),
                        now,
                        str(error or "").strip()[:1000],
                        now,
                        account_id,
                    ),
                ).rowcount
                or 0
            )

    def mark_account_provider_error(self, account_id: str, error_message: str) -> int:
        """记录渠道侧连接/能力校验失败；已有 ok 状态不被降级。"""
        account_id = str(account_id or "").strip()
        if not account_id:
            return 0
        now = self._now_text()
        with self._connect() as conn:
            return int(
                conn.execute(
                    """
                    UPDATE account_configs
                    SET provider_health = CASE
                            WHEN provider_health = 'ok' THEN provider_health ELSE 'error' END,
                        provider_checked_at = ?,
                        provider_error = ?,
                        updated_at = ?
                    WHERE account_id = ?
                    """,
                    (now, str(error_message or "").strip()[:1000], now, account_id),
                ).rowcount
                or 0
            )

    def get_creatok_connection_cache(self, account_id: str) -> Optional[sqlite3.Row]:
        account_id = str(account_id or "").strip()
        if not account_id:
            return None
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT account_id, publish_profile_id, provider_connection_uid, account_timezone,
                       content_video_capable, content_photo_capable, shop_video_capable,
                       shop_photo_capable, direct_post_capable, delivery_mode,
                       provider_health, provider_checked_at, provider_error
                FROM account_configs
                WHERE account_id = ?
                """,
                (account_id,),
            ).fetchone()

    def get_publish_slot_by_task_id(self, publish_task_id: str) -> Optional[sqlite3.Row]:
        task_id = str(publish_task_id or "").strip()
        if not task_id:
            return None
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT * FROM publish_slots
                WHERE publish_task_id = ?
                ORDER BY slot_id DESC
                LIMIT 1
                """,
                (task_id,),
            ).fetchone()

    def requeue_terminated_task(self, publish_task_id: str, *, reason: str = "") -> Dict[str, Any]:
        task_id = str(publish_task_id or "").strip()
        message = str(reason or "远端任务已终止，视频重新进入待排期池").strip()
        now = self._now_text()
        with self._connect() as conn:
            slot = conn.execute(
                """
                SELECT * FROM publish_slots
                WHERE publish_task_id = ?
                ORDER BY slot_id DESC
                LIMIT 1
                """,
                (task_id,),
            ).fetchone()
            if slot is None:
                return {"requeued": 0, "reason": "task_not_found", "publish_task_id": task_id}
            if str(slot["schedule_status"] or "") == "已发布":
                return {"requeued": 0, "reason": "already_published", "publish_task_id": task_id}
            conn.execute(
                """
                INSERT INTO publish_task_history (
                    slot_id, canonical_script_key, script_id, account_id, account_name,
                    scheduled_for, publish_task_id, event_type, event_detail, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'terminated', ?, ?)
                """,
                (
                    int(slot["slot_id"]), str(slot["canonical_script_key"] or ""),
                    str(slot["script_id"] or ""), str(slot["account_id"] or ""),
                    str(slot["account_name"] or ""), str(slot["scheduled_for"] or ""),
                    task_id, message, now,
                ),
            )
            conn.execute(
                """
                UPDATE publish_slots
                SET schedule_status = '已取消', error_message = ?, updated_at = ?
                WHERE slot_id = ?
                """,
                (message, now, int(slot["slot_id"])),
            )
            asset_changes = conn.execute(
                """
                UPDATE video_assets
                SET publish_status = '待排期', account_id = NULL, account_name = NULL,
                    planned_publish_at = NULL, published_at = NULL, publish_task_id = NULL,
                    publish_result = NULL, error_message = NULL, updated_at = ?
                WHERE canonical_script_key = ? AND publish_task_id = ?
                """,
                (now, str(slot["canonical_script_key"] or ""), task_id),
            ).rowcount
        return {
            "requeued": int(asset_changes or 0),
            "publish_task_id": task_id,
            "script_id": str(slot["script_id"] or ""),
            "canonical_script_key": str(slot["canonical_script_key"] or ""),
            "slot_id": int(slot["slot_id"]),
        }

    def list_publish_task_history(self, publish_task_id: str = "") -> List[sqlite3.Row]:
        with self._connect() as conn:
            if publish_task_id:
                return conn.execute(
                    """
                    SELECT * FROM publish_task_history
                    WHERE publish_task_id = ?
                    ORDER BY history_id
                    """,
                    (str(publish_task_id).strip(),),
                ).fetchall()
            return conn.execute(
                "SELECT * FROM publish_task_history ORDER BY history_id"
            ).fetchall()

    def list_account_publish_channels(self) -> Dict[str, str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT account_id, publish_channel
                FROM account_configs
                WHERE COALESCE(account_id, '') <> ''
                """
            ).fetchall()
        return {
            str(row["account_id"] or "").strip(): str(row["publish_channel"] or "").strip()
            for row in rows
            if str(row["account_id"] or "").strip()
        }

    def count_scheduled_nurture_for_account_day(self, account_id: str, target_time: datetime) -> int:
        day_start = datetime.combine(target_time.date(), time.min).strftime("%Y-%m-%d %H:%M:%S")
        day_end = datetime.combine(target_time.date(), time.max).strftime("%Y-%m-%d %H:%M:%S")
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM publish_slots ps
                INNER JOIN script_metadata sm ON sm.canonical_script_key = ps.canonical_script_key
                WHERE ps.account_id = ?
                  AND ps.scheduled_for >= ?
                  AND ps.scheduled_for <= ?
                  AND ps.schedule_status IN ('已排期', '已发布')
                  AND (
                      sm.script_source = '养号复刻'
                      OR sm.publish_purpose = '养号'
                      OR sm.content_branch = '非商品展示型'
                      OR sm.script_source = '种草脚本'
                      OR sm.publish_purpose = '种草'
                      OR sm.content_branch = 'SEEDING_ORGANIC'
                  )
                """,
                (account_id, day_start, day_end),
            ).fetchone()
        return int(row["count"] or 0)

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
                    error_message = NULL,
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

    def count_recent_product_for_account(self, account_id: str, product_id: str, target_time: datetime, hours: int = 24) -> int:
        if not str(product_id or "").strip():
            return 0
        start_at = (target_time - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        end_at = target_time.strftime("%Y-%m-%d %H:%M:%S")
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM publish_slots ps
                INNER JOIN script_metadata sm ON sm.canonical_script_key = ps.canonical_script_key
                WHERE ps.account_id = ?
                  AND sm.product_id = ?
                  AND ps.scheduled_for >= ?
                  AND ps.scheduled_for <= ?
                  AND ps.schedule_status IN ('已排期', '已发布')
                """,
                (account_id, product_id, start_at, end_at),
            ).fetchone()
        return int(row["count"] or 0) if row else 0

    def has_recent_product_conflict(self, account_id: str, product_id: str, target_time: datetime, hours: int = 24) -> bool:
        return self.count_recent_product_for_account(account_id, product_id, target_time, hours=hours) > 0

    def has_recent_family_conflict(self, store_id: str, content_family_key: str, target_time: datetime, hours: int = 48) -> bool:
        if not str(content_family_key or "").strip():
            return False
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

    def recent_place_conflict(self, account_id: str, place: str, target_time: datetime, hours: int = 24) -> bool:
        """同账号同景点时间隔离：N 小时内已排/已发过同一景点则冲突。

        place 存于 script_text JSON 的 context.place（OPV 桥接层写入）。
        SQLite json_extract 在旧版本不可用时返回 0 行，等价于无冲突。
        """
        if not str(place or "").strip() or not str(account_id or "").strip():
            return False
        start_at = (target_time - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        end_at = target_time.strftime("%Y-%m-%d %H:%M:%S")
        with self._connect() as conn:
            try:
                row = conn.execute(
                    """
                    SELECT 1
                    FROM publish_slots ps
                    INNER JOIN script_metadata sm ON sm.canonical_script_key = ps.canonical_script_key
                    WHERE ps.account_id = ?
                      AND json_extract(sm.script_text, '$.place') = ?
                      AND ps.scheduled_for >= ? AND ps.scheduled_for <= ?
                      AND ps.schedule_status IN ('已排期', '已发布')
                    LIMIT 1
                    """,
                    (account_id, str(place).strip(), start_at, end_at),
                ).fetchone()
            except sqlite3.OperationalError:
                return False
        return row is not None

    def get_active_script_assignment(self, canonical_script_key: str, *, exclude_slot_id: int = 0) -> Optional[sqlite3.Row]:
        resolved_key = self._resolve_canonical_for_write(canonical_script_key=canonical_script_key)
        if not resolved_key:
            return None
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT slot_id, account_id, account_name, scheduled_for, schedule_status,
                       publish_task_id, bgm_json, error_message
                FROM publish_slots
                WHERE canonical_script_key = ?
                  AND (
                      schedule_status = '已发布'
                      OR schedule_status = '提交中'
                      OR (
                          schedule_status = '已排期'
                          AND COALESCE(publish_task_id, '') <> ''
                      )
                  )
                  AND (? <= 0 OR slot_id <> ?)
                ORDER BY scheduled_for ASC, slot_id ASC
                LIMIT 1
                """,
                (resolved_key, int(exclude_slot_id or 0), int(exclude_slot_id or 0)),
            ).fetchone()

    def has_active_script_assignment(self, canonical_script_key: str, *, exclude_slot_id: int = 0) -> bool:
        return self.get_active_script_assignment(canonical_script_key, exclude_slot_id=exclude_slot_id) is not None

    def reserve_slot_for_submission(
        self,
        *,
        slot_id: int,
        canonical_script_key: str,
        script_id: str,
        account_id: str,
        account_name: str,
        planned_publish_at: datetime,
        submission_context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Atomically reserve content before calling a remote publisher.

        The immediate transaction serializes competing scheduler processes, so
        only one of them can move a canonical content item into ``提交中``.
        """
        resolved_key = self._resolve_canonical_for_write(
            canonical_script_key=canonical_script_key,
            script_id=script_id,
        )
        if not resolved_key:
            return False
        now = self._now_text()
        planned_text = planned_publish_at.strftime("%Y-%m-%d %H:%M:%S")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            slot = conn.execute(
                "SELECT schedule_status FROM publish_slots WHERE slot_id = ?",
                (int(slot_id),),
            ).fetchone()
            if not slot or str(slot["schedule_status"] or "") != "待排期":
                return False
            duplicate = conn.execute(
                """
                SELECT slot_id
                FROM publish_slots
                WHERE canonical_script_key = ?
                  AND slot_id <> ?
                  AND (
                      schedule_status IN ('提交中', '已发布')
                      OR (
                          schedule_status = '已排期'
                          AND COALESCE(publish_task_id, '') <> ''
                      )
                  )
                LIMIT 1
                """,
                (resolved_key, int(slot_id)),
            ).fetchone()
            if duplicate:
                return False
            updated = conn.execute(
                """
                UPDATE publish_slots
                SET canonical_script_key = ?, script_id = ?, schedule_status = '提交中',
                    publish_task_id = NULL, error_message = NULL, updated_at = ?,
                    submission_context_json = ?
                WHERE slot_id = ? AND schedule_status = '待排期'
                """,
                (resolved_key, script_id, now,
                 json.dumps(submission_context or {}, ensure_ascii=False, sort_keys=True), int(slot_id)),
            ).rowcount
            if not updated:
                return False
            conn.execute(
                """
                UPDATE video_assets
                SET publish_status = '提交中', account_id = ?, account_name = ?,
                    planned_publish_at = ?, publish_task_id = NULL,
                    error_message = NULL, updated_at = ?
                WHERE canonical_script_key = ?
                """,
                (account_id, account_name, planned_text, now, resolved_key),
            )
        return True

    def mark_submission_ambiguous(self, slot_id: int, reason: str) -> None:
        """Retain the submitting state so every existing duplicate guard applies."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE publish_slots SET error_message=?, updated_at=? "
                "WHERE slot_id=? AND schedule_status='提交中'",
                (f"提交结果不明；仅对账，禁止自动重发：{reason}", self._now_text(), int(slot_id)),
            )
            conn.execute(
                "UPDATE video_assets SET error_message=?, updated_at=? WHERE canonical_script_key="
                "(SELECT canonical_script_key FROM publish_slots WHERE slot_id=? AND schedule_status='提交中')",
                (f"提交结果不明；仅对账，禁止自动重发：{reason}", self._now_text(), int(slot_id)),
            )

    def record_confirmed_submission(self, slot_id: int, task_id: str) -> None:
        """Keep the remote receipt in the reservation until assignment commits."""
        if not str(task_id or "").strip():
            raise ValueError("远端创建返回空任务ID，保留占位待对账")
        with self._connect() as conn:
            row = conn.execute(
                "SELECT submission_context_json FROM publish_slots WHERE slot_id=? AND schedule_status='提交中'",
                (int(slot_id),),
            ).fetchone()
            if row is None:
                raise RuntimeError("远端已创建，但本地提交占位不存在，禁止重发")
            context = json.loads(row["submission_context_json"] or "{}")
            context["confirmed_task_id"] = str(task_id)
            conn.execute(
                "UPDATE publish_slots SET submission_context_json=?, updated_at=? WHERE slot_id=?",
                (json.dumps(context, ensure_ascii=False, sort_keys=True), self._now_text(), int(slot_id)),
            )

    def merge_submission_context(self, slot_id: int, updates: Dict[str, Any]) -> None:
        """Persist channel diagnostics while the submission reservation is held."""
        if not isinstance(updates, dict) or not updates:
            return
        with self._connect() as conn:
            row = conn.execute(
                "SELECT submission_context_json FROM publish_slots WHERE slot_id=? AND schedule_status='提交中'",
                (int(slot_id),),
            ).fetchone()
            if row is None:
                raise RuntimeError("提交占位不存在，无法保存发布诊断信息")
            context = json.loads(row["submission_context_json"] or "{}")
            context.update(updates)
            conn.execute(
                "UPDATE publish_slots SET submission_context_json=?, updated_at=? WHERE slot_id=?",
                (json.dumps(context, ensure_ascii=False, sort_keys=True), self._now_text(), int(slot_id)),
            )

    def candidate_retry_ready(self, canonical_key: str, now: datetime) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT blocked, next_retry_at FROM publish_candidate_retries WHERE canonical_script_key=?",
                (canonical_key,),
            ).fetchone()
        return row is None or (
            not row["blocked"] and str(row["next_retry_at"] or "") <= now.strftime("%Y-%m-%d %H:%M:%S")
        )

    def record_candidate_failure(self, canonical_key: str, now: datetime, error: str, *, retryable: bool) -> None:
        """Bound pre-submit failures to three rounds, independently per content."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT attempts FROM publish_candidate_retries WHERE canonical_script_key=?", (canonical_key,),
            ).fetchone()
            attempts = int(row["attempts"] or 0) + 1 if row else 1
            blocked = not retryable or attempts >= 3
            next_retry = (now + timedelta(hours=2 ** min(attempts - 1, 2))).strftime("%Y-%m-%d %H:%M:%S")
            conn.execute("""
                INSERT INTO publish_candidate_retries
                    (canonical_script_key, attempts, next_retry_at, blocked, last_error, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(canonical_script_key) DO UPDATE SET
                    attempts=excluded.attempts, next_retry_at=excluded.next_retry_at,
                    blocked=excluded.blocked, last_error=excluded.last_error, updated_at=excluded.updated_at
            """, (canonical_key, attempts, next_retry, int(blocked), str(error)[:1000], now.strftime("%Y-%m-%d %H:%M:%S")))
            summary = (
                f"已停止自动重试（{attempts}/3）：{error}"
                if blocked else f"等待自动重试（{attempts}/3，下次 {next_retry}）：{error}"
            )
            conn.execute(
                "UPDATE video_assets SET error_message=?, updated_at=? "
                "WHERE canonical_script_key=? AND COALESCE(publish_task_id,'')=''",
                (summary[:1000], now.strftime("%Y-%m-%d %H:%M:%S"), canonical_key),
            )

    def reset_candidate_retry(self, canonical_key: str) -> None:
        """Explicit operator action only; normal scans never reset failures."""
        with self._connect() as conn:
            conn.execute("DELETE FROM publish_candidate_retries WHERE canonical_script_key=?", (canonical_key,))
            conn.execute(
                "UPDATE video_assets SET error_message=NULL WHERE canonical_script_key=? "
                "AND (error_message LIKE '等待自动重试%' OR error_message LIKE '已停止自动重试%')",
                (canonical_key,),
            )

    def list_unresolved_submissions(self) -> List[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM publish_slots WHERE schedule_status='提交中' "
                "AND COALESCE(publish_task_id,'')='' ORDER BY slot_id"
            ).fetchall()

    def release_slot_submission_reservation(self, slot_id: int, reason: str = "") -> int:
        """Release a local reservation after a confirmed remote-create failure."""
        now = self._now_text()
        message = str(reason or "创建发布任务失败，已释放本地占位").strip()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT canonical_script_key FROM publish_slots WHERE slot_id = ? AND schedule_status = '提交中'",
                (int(slot_id),),
            ).fetchone()
            if not row:
                return 0
            canonical_key = str(row["canonical_script_key"] or "")
            updated = conn.execute(
                """
                UPDATE publish_slots
                SET canonical_script_key = NULL, script_id = NULL,
                    schedule_status = '待排期', publish_task_id = NULL,
                    bgm_json = NULL, error_message = ?, updated_at = ?
                WHERE slot_id = ? AND schedule_status = '提交中'
                """,
                (message, now, int(slot_id)),
            ).rowcount
            if updated and canonical_key:
                conn.execute(
                    """
                    UPDATE video_assets
                    SET publish_status = '待排期', account_id = NULL,
                        account_name = NULL, planned_publish_at = NULL,
                        publish_task_id = NULL, error_message = ?, updated_at = ?
                    WHERE canonical_script_key = ? AND publish_status = '提交中'
                    """,
                    (message, now, canonical_key),
                )
        return int(updated or 0)

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
        allow_duplicate: bool = False,
        bgm_json: str = "",
    ) -> bool:
        now = self._now_text()
        planned_text = planned_publish_at.strftime("%Y-%m-%d %H:%M:%S")
        resolved_key = self._resolve_canonical_for_write(
            canonical_script_key=canonical_script_key,
            script_id=script_id,
        )
        with self._connect() as conn:
            if resolved_key and not allow_duplicate:
                duplicate = conn.execute(
                    """
                    SELECT slot_id, account_id, account_name, scheduled_for, schedule_status, publish_task_id
                    FROM publish_slots
                    WHERE canonical_script_key = ?
                      AND (
                          schedule_status = '已发布'
                          OR schedule_status = '提交中'
                          OR (
                              schedule_status = '已排期'
                              AND COALESCE(publish_task_id, '') <> ''
                          )
                      )
                      AND slot_id <> ?
                    ORDER BY scheduled_for ASC, slot_id ASC
                    LIMIT 1
                    """,
                    (resolved_key, int(slot_id)),
                ).fetchone()
                if duplicate:
                    reason = (
                        "内部脚本键已被其他发布槽位占用，等待重新选择候选："
                        f"slot_id={duplicate['slot_id']}，"
                        f"账号={duplicate['account_name'] or duplicate['account_id']}，"
                        f"时间={duplicate['scheduled_for']}"
                    )
                    conn.execute(
                        """
                        UPDATE publish_slots
                        SET error_message = ?,
                            updated_at = ?
                        WHERE slot_id = ?
                          AND schedule_status = '待排期'
                        """,
                        (reason, now, slot_id),
                    )
                    return False
            conn.execute(
                """
                UPDATE publish_slots
                SET canonical_script_key = ?, script_id = ?, schedule_status = '已排期',
                    publish_task_id = ?, bgm_json = ?, error_message = NULL, updated_at = ?
                WHERE slot_id = ?
                """,
                (resolved_key, script_id, publish_task_id, bgm_json or None, now, slot_id),
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
            conn.execute(
                """
                INSERT INTO publish_task_history (
                    slot_id, canonical_script_key, script_id, account_id, account_name,
                    scheduled_for, publish_task_id, event_type, event_detail, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'created', '', ?)
                """,
                (
                    int(slot_id), resolved_key, script_id, account_id, account_name,
                    planned_text, publish_task_id, now,
                ),
            )
        return True

    def recent_bgm_use_counts(
        self, account_id: str, *, since: datetime
    ) -> Dict[str, int]:
        """Count active/published BGM reservations; cancelled slots release use."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT bgm_json
                FROM publish_slots
                WHERE account_id = ?
                  AND scheduled_for >= ?
                  AND schedule_status IN ('已排期', '已发布')
                  AND COALESCE(bgm_json, '') <> ''
                """,
                (account_id, since.strftime("%Y-%m-%d %H:%M:%S")),
            ).fetchall()
        counts: Dict[str, int] = {}
        for row in rows:
            try:
                payload = json.loads(str(row["bgm_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            music_id = str(payload.get("music_id") or "").strip()
            if music_id:
                counts[music_id] = counts.get(music_id, 0) + 1
        return counts

    def update_slot_bgm_json(self, slot_id: int, bgm_json: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE publish_slots SET bgm_json = ?, updated_at = ? WHERE slot_id = ?",
                (str(bgm_json or "") or None, self._now_text(), int(slot_id)),
            )

    def cancel_slot(self, slot_id: int, reason: str = "") -> int:
        now = self._now_text()
        message = str(reason or "创建发布任务失败，已跳过当前槽位").strip()
        with self._connect() as conn:
            return int(
                conn.execute(
                    """
                    UPDATE publish_slots
                    SET schedule_status = '已取消',
                        error_message = ?,
                        updated_at = ?
                    WHERE slot_id = ?
                      AND schedule_status IN ('待排期', '已排期')
                    """,
                    (message, now, slot_id),
                ).rowcount
                or 0
            )

    def mark_slot_pending_reason(self, slot_id: int, reason: str = "") -> int:
        now = self._now_text()
        message = str(reason or "暂未找到符合规则的候选视频，等待后续自动补排").strip()
        with self._connect() as conn:
            return int(
                conn.execute(
                    """
                    UPDATE publish_slots
                    SET error_message = ?,
                        updated_at = ?
                    WHERE slot_id = ?
                      AND schedule_status = '待排期'
                    """,
                    (message, now, slot_id),
                ).rowcount
                or 0
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
        platform_post_id: Optional[str] = None,
        platform_post_url: Optional[str] = None,
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
                SET schedule_status = ?, error_message = ?, updated_at = ?,
                    platform_post_id = COALESCE(NULLIF(?, ''), platform_post_id),
                    platform_post_url = COALESCE(NULLIF(?, ''), platform_post_url),
                    published_at = COALESCE(NULLIF(?, ''), published_at)
                WHERE publish_task_id = ?
                """,
                (schedule_status, error_message, now, platform_post_id, platform_post_url,
                 published_at, publish_task_id),
            )
            conn.execute(
                """
                UPDATE video_assets
                SET publish_status = ?, publish_result = ?, published_at = COALESCE(?, published_at),
                    error_message = ?, updated_at = ?
                WHERE canonical_script_key = ?
                  AND (? = '已发布' OR COALESCE(publish_task_id, '') = '' OR publish_task_id = ?)
                """,
                (
                    publish_status, publish_result, published_at, error_message, now,
                    resolved_key, publish_status, publish_task_id,
                ),
            )

    def mark_manual_publish_result(
        self,
        *,
        canonical_script_key: str = "",
        script_id: str = "",
        scheduled_for: str = "",
        published_at: Optional[str] = None,
        note: str = "",
    ) -> bool:
        resolved_key = self._resolve_canonical_for_write(
            canonical_script_key=canonical_script_key,
            script_id=script_id,
        )
        if not resolved_key:
            return False
        now = self._now_text()
        published_text = str(published_at or "").strip() or now
        manual_note = str(note or "").strip() or "运营人工发布成功"
        slot_params: List[Any] = ["已发布", manual_note, now, resolved_key]
        slot_filter = "canonical_script_key = ?"
        if str(scheduled_for or "").strip():
            slot_filter += " AND scheduled_for = ?"
            slot_params.append(str(scheduled_for).strip())
        with self._connect() as conn:
            cursor = conn.execute(
                f"""
                UPDATE publish_slots
                SET schedule_status = ?, error_message = ?, updated_at = ?
                WHERE {slot_filter}
                """,
                tuple(slot_params),
            )
            conn.execute(
                """
                UPDATE publish_slots
                SET schedule_status = '已取消',
                    error_message = ?,
                    updated_at = ?
                WHERE canonical_script_key = ?
                  AND schedule_status IN ('待排期', '已排期')
                  AND NOT (scheduled_for = ? AND schedule_status = '已发布')
                """,
                (f"已人工发布，取消同脚本后续自动排期；{manual_note}", now, resolved_key, str(scheduled_for or "").strip()),
            )
            conn.execute(
                """
                UPDATE video_assets
                SET publish_status = '已发布', publish_result = '人工发布成功',
                    published_at = COALESCE(NULLIF(?, ''), published_at), error_message = ?,
                    updated_at = ?
                WHERE canonical_script_key = ?
                """,
                (published_text, manual_note, now, resolved_key),
            )
        return cursor.rowcount > 0

    def list_scheduled_tasks(self) -> List[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT ps.slot_id, ps.publish_task_id, ps.scheduled_for, ps.account_id, ps.account_name,
                       ps.schedule_status, ps.platform_post_id, ps.platform_post_url, ps.published_at,
                       va.media_kind,
                       ps.store_id, ps.canonical_script_key, ps.script_id, va.local_file_path,
                       sm.short_video_title, sm.product_id, sm.content_family_key, ps.bgm_json,
                       COALESCE(ac.publish_channel, '') AS publish_channel
                FROM publish_slots ps
                INNER JOIN video_assets va ON va.canonical_script_key = ps.canonical_script_key
                INNER JOIN script_metadata sm ON sm.canonical_script_key = ps.canonical_script_key
                LEFT JOIN account_configs ac ON ac.account_id = ps.account_id
                WHERE (
                    ps.schedule_status = '已排期'
                    OR (
                        ps.schedule_status = '已取消'
                        AND COALESCE(ps.error_message, '') LIKE '%超过自动重试上限%'
                        AND COALESCE(ps.publish_task_id, '') <> ''
                        AND ps.scheduled_for <= ?
                    )
                    OR (
                        ps.schedule_status = '已取消'
                        AND COALESCE(ps.publish_task_id, '') LIKE 'neobund:%'
                        AND NOT EXISTS (
                            SELECT 1 FROM publish_task_history history
                            WHERE history.publish_task_id = ps.publish_task_id
                              AND history.event_type = 'terminated'
                        )
                    )
                )
                ORDER BY ps.scheduled_for ASC
                """,
                (self._now_text(),),
            ).fetchall()

    def list_bgm_run_manager_rows(self) -> List[sqlite3.Row]:
        """Rows requiring BGM observability in the source run-manager table."""
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT va.run_manager_record_id, sm.audio_mode,
                       COALESCE(ps.bgm_json, '') AS bgm_json,
                       COALESCE(ps.schedule_status, '') AS schedule_status,
                       COALESCE(ps.error_message, '') AS error_message
                FROM video_assets va
                INNER JOIN script_metadata sm ON sm.canonical_script_key = va.canonical_script_key
                LEFT JOIN publish_slots ps ON ps.slot_id = (
                    SELECT latest.slot_id
                    FROM publish_slots latest
                    WHERE latest.canonical_script_key = va.canonical_script_key
                    ORDER BY latest.updated_at DESC, latest.slot_id DESC
                    LIMIT 1
                )
                WHERE COALESCE(va.run_manager_record_id, '') <> ''
                  AND (
                    COALESCE(sm.audio_mode, '') IN ('silent_source_platform_bgm', 'platform_auto_bgm', 'generated_nonvoice', 'clean_voice')
                    OR COALESCE(ps.bgm_json, '') <> ''
                  )
                ORDER BY va.updated_at DESC, va.run_manager_record_id ASC
                """
            ).fetchall()

    def cleanup_published_videos(
        self,
        *,
        older_than_days: int = 60,
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
                       COALESCE(ac.publish_channel, '') AS publish_channel,
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
                LEFT JOIN account_configs ac
                    ON ac.account_id = COALESCE(NULLIF(va.account_id, ''), ps.account_id)
                ORDER BY COALESCE(va.planned_publish_at, ps.scheduled_for, ''), sm.script_id, sm.canonical_script_key
                """
            ).fetchall()

    def build_daily_publish_summary(self, day: str) -> Dict[str, Any]:
        start_at = f"{str(day or '').strip()} 00:00:00"
        end_at = (datetime.strptime(str(day or "").strip(), "%Y-%m-%d") + timedelta(days=1)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        with self._connect() as conn:
            totals = conn.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN schedule_status = '已发布' THEN 1 ELSE 0 END) AS published,
                       SUM(CASE WHEN schedule_status = '发布失败' THEN 1 ELSE 0 END) AS failed,
                       SUM(CASE WHEN schedule_status = '已排期' THEN 1 ELSE 0 END) AS scheduled,
                       SUM(CASE WHEN schedule_status = '待排期' THEN 1 ELSE 0 END) AS pending,
                       SUM(CASE WHEN schedule_status = '已取消' THEN 1 ELSE 0 END) AS cancelled
                FROM publish_slots
                WHERE scheduled_for >= ?
                  AND scheduled_for < ?
                """,
                (start_at, end_at),
            ).fetchone()
            failures = conn.execute(
                """
                SELECT ps.scheduled_for,
                       ps.canonical_script_key,
                       ps.store_id,
                       ps.account_id,
                       ps.account_name,
                       ps.script_id,
                       ps.publish_task_id,
                       CASE
                           WHEN COALESCE(ac.publish_channel, '') <> '' THEN ac.publish_channel
                           WHEN COALESCE(ps.publish_task_id, '') LIKE 'neobund:%' THEN 'NeoBund'
                           ELSE 'GeeLark'
                       END AS publish_channel,
                       sm.product_id,
                       COALESCE(NULLIF(ps.error_message, ''), '未返回失败原因') AS error_message
                FROM publish_slots ps
                LEFT JOIN script_metadata sm ON sm.canonical_script_key = ps.canonical_script_key
                LEFT JOIN account_configs ac ON ac.account_id = ps.account_id
                WHERE ps.scheduled_for >= ?
                  AND ps.scheduled_for < ?
                  AND ps.schedule_status = '发布失败'
                ORDER BY ps.store_id ASC, ps.account_name ASC, ps.scheduled_for ASC
                """,
                (start_at, end_at),
            ).fetchall()
            abnormal_cancellations = conn.execute(
                """
                SELECT ps.scheduled_for,
                       ps.canonical_script_key,
                       ps.store_id,
                       ps.account_id,
                       ps.account_name,
                       ps.script_id,
                       ps.publish_task_id,
                       CASE
                           WHEN COALESCE(ac.publish_channel, '') <> '' THEN ac.publish_channel
                           WHEN COALESCE(ps.publish_task_id, '') LIKE 'neobund:%' THEN 'NeoBund'
                           ELSE 'GeeLark'
                       END AS publish_channel,
                       sm.product_id,
                       COALESCE(NULLIF(ps.error_message, ''), '超过自动重试上限') AS error_message
                FROM publish_slots ps
                LEFT JOIN script_metadata sm ON sm.canonical_script_key = ps.canonical_script_key
                LEFT JOIN account_configs ac ON ac.account_id = ps.account_id
                WHERE ps.scheduled_for >= ?
                  AND ps.scheduled_for < ?
                  AND ps.schedule_status = '已取消'
                  AND COALESCE(ps.error_message, '') LIKE '%超过自动重试上限%'
                ORDER BY ps.store_id ASC, ps.account_name ASC, ps.scheduled_for ASC
                """,
                (start_at, end_at),
            ).fetchall()
            account_rows = conn.execute(
                """
                SELECT ps.store_id,
                       ps.account_id,
                       ps.account_name,
                       CASE
                           WHEN COALESCE(ac.publish_channel, '') <> '' THEN ac.publish_channel
                           WHEN COALESCE(ps.publish_task_id, '') LIKE 'neobund:%' THEN 'NeoBund'
                           ELSE 'GeeLark'
                       END AS publish_channel,
                       COUNT(*) AS total,
                       SUM(CASE WHEN ps.schedule_status = '已发布' THEN 1 ELSE 0 END) AS published,
                       SUM(CASE WHEN ps.schedule_status = '发布失败' THEN 1 ELSE 0 END) AS failed,
                       SUM(
                           CASE
                               WHEN ps.schedule_status = '已取消'
                                    AND COALESCE(ps.error_message, '') LIKE '%超过自动重试上限%'
                               THEN 1 ELSE 0
                           END
                       ) AS abnormal_cancelled
                FROM publish_slots ps
                LEFT JOIN account_configs ac ON ac.account_id = ps.account_id
                WHERE ps.scheduled_for >= ?
                  AND ps.scheduled_for < ?
                GROUP BY ps.store_id, ps.account_id, ps.account_name, publish_channel
                HAVING total > 0
                ORDER BY failed DESC, abnormal_cancelled DESC, published DESC, ps.account_name ASC
                """,
                (start_at, end_at),
            ).fetchall()
            channel_rows = conn.execute(
                """
                SELECT publish_channel,
                       COUNT(*) AS total,
                       SUM(CASE WHEN schedule_status = '已发布' THEN 1 ELSE 0 END) AS published,
                       SUM(CASE WHEN schedule_status = '发布失败' THEN 1 ELSE 0 END) AS failed,
                       SUM(CASE WHEN schedule_status = '已排期' THEN 1 ELSE 0 END) AS scheduled,
                       SUM(CASE WHEN schedule_status = '待排期' THEN 1 ELSE 0 END) AS pending,
                       SUM(CASE WHEN schedule_status = '已取消' THEN 1 ELSE 0 END) AS cancelled,
                       SUM(
                           CASE
                               WHEN schedule_status = '已取消'
                                    AND COALESCE(error_message, '') LIKE '%超过自动重试上限%'
                               THEN 1 ELSE 0
                           END
                       ) AS abnormal_cancelled
                FROM (
                    SELECT ps.schedule_status,
                           ps.error_message,
                           CASE
                               WHEN COALESCE(ac.publish_channel, '') <> '' THEN ac.publish_channel
                               WHEN COALESCE(ps.publish_task_id, '') LIKE 'neobund:%' THEN 'NeoBund'
                               ELSE 'GeeLark'
                           END AS publish_channel
                    FROM publish_slots ps
                    LEFT JOIN account_configs ac ON ac.account_id = ps.account_id
                    WHERE ps.scheduled_for >= ?
                      AND ps.scheduled_for < ?
                )
                GROUP BY publish_channel
                ORDER BY published DESC, failed DESC, scheduled DESC, total DESC, publish_channel ASC
                """,
                (start_at, end_at),
            ).fetchall()

        return {
            "date": str(day or "").strip(),
            "total": int(totals["total"] or 0),
            "published": int(totals["published"] or 0),
            "failed": int(totals["failed"] or 0),
            "scheduled": int(totals["scheduled"] or 0),
            "pending": int(totals["pending"] or 0),
            "cancelled": int(totals["cancelled"] or 0),
            "abnormal_cancelled": len(abnormal_cancellations),
            "failures": [dict(row) for row in failures],
            "abnormal_cancellations": [dict(row) for row in abnormal_cancellations],
            "accounts": [dict(row) for row in account_rows],
            "channels": [dict(row) for row in channel_rows],
        }

    def list_store_publish_summary_rows(
        self,
        *,
        this_week_start: str,
        this_week_end: str,
        last_week_start: str,
        last_week_end: str,
        current_month_start: str,
        current_month_end: str,
        previous_month_start: str,
        previous_month_end: str,
    ) -> List[Dict[str, Any]]:
        min_start = min(this_week_start, last_week_start, current_month_start, previous_month_start)
        max_end = max(this_week_end, last_week_end, current_month_end, previous_month_end)
        with self._connect() as conn:
            rows = conn.execute(
                """
                WITH published AS (
                    SELECT sm.store_id,
                           COUNT(DISTINCT sm.canonical_script_key) AS script_count,
                           COUNT(DISTINCT sm.product_id) AS product_count,
                           MAX(ps.scheduled_for) AS latest_published_at,
                               SUM(CASE WHEN ps.scheduled_for >= ? AND ps.scheduled_for < ? THEN 1 ELSE 0 END)
                                   AS this_week_published,
                               SUM(CASE WHEN ps.scheduled_for >= ? AND ps.scheduled_for < ?
                                        AND (
                                            CASE
                                                WHEN COALESCE(ps.publish_task_id, '') LIKE 'neobund:%' THEN 'NeoBund'
                                                WHEN COALESCE(ps.publish_task_id, '') LIKE 'dryrun-%' THEN 'DryRun'
                                                ELSE COALESCE(NULLIF(ac.publish_channel, ''), 'GeeLark')
                                            END
                                        ) = 'GeeLark'
                                        THEN 1 ELSE 0 END) AS this_week_geelark_published,
                               SUM(CASE WHEN ps.scheduled_for >= ? AND ps.scheduled_for < ?
                                        AND (
                                            CASE
                                                WHEN COALESCE(ps.publish_task_id, '') LIKE 'neobund:%' THEN 'NeoBund'
                                                WHEN COALESCE(ps.publish_task_id, '') LIKE 'dryrun-%' THEN 'DryRun'
                                                ELSE COALESCE(NULLIF(ac.publish_channel, ''), 'GeeLark')
                                            END
                                        ) = 'NeoBund'
                                        THEN 1 ELSE 0 END) AS this_week_neobund_published,
                               SUM(CASE WHEN ps.scheduled_for >= ? AND ps.scheduled_for < ? THEN 1 ELSE 0 END)
                                   AS last_week_published,
                           SUM(CASE WHEN ps.scheduled_for >= ? AND ps.scheduled_for < ? THEN 1 ELSE 0 END)
                               AS current_month_published,
                           SUM(CASE WHEN ps.scheduled_for >= ? AND ps.scheduled_for < ? THEN 1 ELSE 0 END)
                               AS previous_month_published
                        FROM publish_slots ps
                        INNER JOIN script_metadata sm ON sm.canonical_script_key = ps.canonical_script_key
                        LEFT JOIN account_configs ac ON ac.account_id = ps.account_id
                        WHERE ps.schedule_status = '已发布'
                      AND ps.scheduled_for >= ?
                      AND ps.scheduled_for < ?
                      AND COALESCE(sm.store_id, '') <> ''
                    GROUP BY sm.store_id
                ),
                pending AS (
                    SELECT sm.store_id,
                           COUNT(DISTINCT sm.canonical_script_key) AS pending_video_count,
                           COUNT(DISTINCT sm.product_id) AS pending_product_count
                    FROM script_metadata sm
                    INNER JOIN video_assets va ON va.canonical_script_key = sm.canonical_script_key
                    WHERE COALESCE(sm.store_id, '') <> ''
                      AND COALESCE(sm.product_id, '') <> ''
                      AND COALESCE(sm.short_video_title, '') <> ''
                      AND COALESCE(va.local_file_path, '') <> ''
                      AND va.download_status = '下载成功'
                      AND va.publish_status = '待排期'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM disabled_products dp
                          WHERE dp.product_id = sm.product_id
                      )
                    GROUP BY sm.store_id
                ),
                scheduled AS (
                    SELECT sm.store_id,
                           COUNT(DISTINCT ps.canonical_script_key) AS scheduled_unpublished_count
                    FROM publish_slots ps
                    INNER JOIN script_metadata sm ON sm.canonical_script_key = ps.canonical_script_key
                    WHERE ps.schedule_status = '已排期'
                      AND COALESCE(sm.store_id, '') <> ''
                    GROUP BY sm.store_id
                ),
                prefs AS (
                    SELECT store_id,
                           MAX(CASE WHEN schedule_strategy = '优先' THEN 1 ELSE 0 END) AS has_priority,
                           MAX(CASE WHEN schedule_strategy = '暂停' THEN 1 ELSE 0 END) AS has_paused,
                           MAX(priority_updated_at) AS priority_updated_at
                    FROM product_schedule_preferences
                    GROUP BY store_id
                ),
                keys AS (
                    SELECT store_id FROM published
                    UNION
                    SELECT store_id FROM pending
                    UNION
                    SELECT store_id FROM scheduled
                )
                SELECT keys.store_id,
                       '' AS product_id,
                       '' AS source_record_id,
                       COALESCE(published.script_count, 0) AS script_count,
                       COALESCE(published.product_count, pending.pending_product_count, 0) AS product_count,
                       COALESCE(pending.pending_video_count, 0) AS pending_video_count,
                       COALESCE(scheduled.scheduled_unpublished_count, 0) AS scheduled_unpublished_count,
                       CASE
                           WHEN COALESCE(prefs.has_priority, 0) = 1 THEN '优先'
                           WHEN COALESCE(prefs.has_paused, 0) = 1 THEN '暂停'
                           ELSE '普通'
                       END AS schedule_strategy,
                       '' AS schedule_note,
                       COALESCE(prefs.priority_updated_at, '') AS priority_updated_at,
                       COALESCE(published.latest_published_at, '') AS latest_published_at,
                           COALESCE(published.this_week_published, 0) AS this_week_published,
                           COALESCE(published.this_week_geelark_published, 0) AS this_week_geelark_published,
                           COALESCE(published.this_week_neobund_published, 0) AS this_week_neobund_published,
                           COALESCE(published.last_week_published, 0) AS last_week_published,
                       COALESCE(published.current_month_published, 0) AS current_month_published,
                       COALESCE(published.previous_month_published, 0) AS previous_month_published
                FROM keys
                LEFT JOIN published ON published.store_id = keys.store_id
                LEFT JOIN pending ON pending.store_id = keys.store_id
                LEFT JOIN scheduled ON scheduled.store_id = keys.store_id
                LEFT JOIN prefs ON prefs.store_id = keys.store_id
                WHERE COALESCE(published.this_week_published, 0) > 0
                   OR COALESCE(published.last_week_published, 0) > 0
                   OR COALESCE(published.current_month_published, 0) > 0
                   OR COALESCE(published.previous_month_published, 0) > 0
                   OR COALESCE(pending.pending_video_count, 0) > 0
                   OR COALESCE(scheduled.scheduled_unpublished_count, 0) > 0
                ORDER BY this_week_published DESC, current_month_published DESC, keys.store_id ASC
                """,
                (
                        this_week_start,
                        this_week_end,
                        this_week_start,
                        this_week_end,
                        this_week_start,
                        this_week_end,
                        last_week_start,
                    last_week_end,
                    current_month_start,
                    current_month_end,
                    previous_month_start,
                    previous_month_end,
                    min_start,
                    max_end,
                ),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_product_publish_summary_rows(
        self,
        *,
        this_week_start: str,
        this_week_end: str,
        last_week_start: str,
        last_week_end: str,
        current_month_start: str,
        current_month_end: str,
        previous_month_start: str,
        previous_month_end: str,
    ) -> List[Dict[str, Any]]:
        min_start = min(this_week_start, last_week_start, current_month_start, previous_month_start)
        max_end = max(this_week_end, last_week_end, current_month_end, previous_month_end)
        with self._connect() as conn:
            rows = conn.execute(
                """
                WITH published AS (
                    SELECT sm.store_id,
                           sm.product_id,
                           MIN(sm.source_record_id) AS source_record_id,
                           COUNT(DISTINCT sm.canonical_script_key) AS script_count,
                           MAX(ps.scheduled_for) AS latest_published_at,
                               SUM(CASE WHEN ps.scheduled_for >= ? AND ps.scheduled_for < ? THEN 1 ELSE 0 END)
                                   AS this_week_published,
                               SUM(CASE WHEN ps.scheduled_for >= ? AND ps.scheduled_for < ?
                                        AND (
                                            CASE
                                                WHEN COALESCE(ps.publish_task_id, '') LIKE 'neobund:%' THEN 'NeoBund'
                                                WHEN COALESCE(ps.publish_task_id, '') LIKE 'dryrun-%' THEN 'DryRun'
                                                ELSE COALESCE(NULLIF(ac.publish_channel, ''), 'GeeLark')
                                            END
                                        ) = 'GeeLark'
                                        THEN 1 ELSE 0 END) AS this_week_geelark_published,
                               SUM(CASE WHEN ps.scheduled_for >= ? AND ps.scheduled_for < ?
                                        AND (
                                            CASE
                                                WHEN COALESCE(ps.publish_task_id, '') LIKE 'neobund:%' THEN 'NeoBund'
                                                WHEN COALESCE(ps.publish_task_id, '') LIKE 'dryrun-%' THEN 'DryRun'
                                                ELSE COALESCE(NULLIF(ac.publish_channel, ''), 'GeeLark')
                                            END
                                        ) = 'NeoBund'
                                        THEN 1 ELSE 0 END) AS this_week_neobund_published,
                               SUM(CASE WHEN ps.scheduled_for >= ? AND ps.scheduled_for < ? THEN 1 ELSE 0 END)
                                   AS last_week_published,
                           SUM(CASE WHEN ps.scheduled_for >= ? AND ps.scheduled_for < ? THEN 1 ELSE 0 END)
                               AS current_month_published,
                           SUM(CASE WHEN ps.scheduled_for >= ? AND ps.scheduled_for < ? THEN 1 ELSE 0 END)
                               AS previous_month_published
                        FROM publish_slots ps
                        INNER JOIN script_metadata sm ON sm.canonical_script_key = ps.canonical_script_key
                        LEFT JOIN account_configs ac ON ac.account_id = ps.account_id
                        WHERE ps.schedule_status = '已发布'
                      AND ps.scheduled_for >= ?
                      AND ps.scheduled_for < ?
                      AND COALESCE(sm.store_id, '') <> ''
                      AND COALESCE(sm.product_id, '') <> ''
                    GROUP BY sm.store_id, sm.product_id
                ),
                pending AS (
                    SELECT sm.store_id,
                           sm.product_id,
                           MIN(sm.source_record_id) AS source_record_id,
                           COUNT(DISTINCT sm.canonical_script_key) AS pending_video_count
                    FROM script_metadata sm
                    INNER JOIN video_assets va ON va.canonical_script_key = sm.canonical_script_key
                    WHERE COALESCE(sm.store_id, '') <> ''
                      AND COALESCE(sm.product_id, '') <> ''
                      AND COALESCE(sm.short_video_title, '') <> ''
                      AND COALESCE(va.local_file_path, '') <> ''
                      AND va.download_status = '下载成功'
                      AND va.publish_status = '待排期'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM disabled_products dp
                          WHERE dp.product_id = sm.product_id
                      )
                    GROUP BY sm.store_id, sm.product_id
                ),
                scheduled AS (
                    SELECT sm.store_id,
                           sm.product_id,
                           MIN(sm.source_record_id) AS source_record_id,
                           COUNT(DISTINCT ps.canonical_script_key) AS scheduled_unpublished_count
                    FROM publish_slots ps
                    INNER JOIN script_metadata sm ON sm.canonical_script_key = ps.canonical_script_key
                    WHERE ps.schedule_status = '已排期'
                      AND COALESCE(sm.store_id, '') <> ''
                      AND COALESCE(sm.product_id, '') <> ''
                    GROUP BY sm.store_id, sm.product_id
                ),
                keys AS (
                    SELECT store_id, product_id FROM published
                    UNION
                    SELECT store_id, product_id FROM pending
                    UNION
                    SELECT store_id, product_id FROM scheduled
                )
                SELECT keys.store_id,
                       keys.product_id,
                       COALESCE(published.source_record_id, pending.source_record_id, scheduled.source_record_id, '') AS source_record_id,
                       COALESCE(published.script_count, 0) AS script_count,
                       COALESCE(pending.pending_video_count, 0) AS pending_video_count,
                       COALESCE(scheduled.scheduled_unpublished_count, 0) AS scheduled_unpublished_count,
                       COALESCE(psp.schedule_strategy, '普通') AS schedule_strategy,
                       COALESCE(psp.schedule_note, '') AS schedule_note,
                       COALESCE(psp.priority_updated_at, '') AS priority_updated_at,
                       COALESCE(published.latest_published_at, '') AS latest_published_at,
                           COALESCE(published.this_week_published, 0) AS this_week_published,
                           COALESCE(published.this_week_geelark_published, 0) AS this_week_geelark_published,
                           COALESCE(published.this_week_neobund_published, 0) AS this_week_neobund_published,
                           COALESCE(published.last_week_published, 0) AS last_week_published,
                       COALESCE(published.current_month_published, 0) AS current_month_published,
                       COALESCE(published.previous_month_published, 0) AS previous_month_published
                FROM keys
                LEFT JOIN published ON published.store_id = keys.store_id AND published.product_id = keys.product_id
                LEFT JOIN pending ON pending.store_id = keys.store_id AND pending.product_id = keys.product_id
                LEFT JOIN scheduled ON scheduled.store_id = keys.store_id AND scheduled.product_id = keys.product_id
                LEFT JOIN product_schedule_preferences psp
                    ON psp.store_id = keys.store_id AND psp.product_id = keys.product_id
                WHERE COALESCE(published.this_week_published, 0) > 0
                   OR COALESCE(published.last_week_published, 0) > 0
                   OR COALESCE(published.current_month_published, 0) > 0
                   OR COALESCE(published.previous_month_published, 0) > 0
                   OR COALESCE(pending.pending_video_count, 0) > 0
                   OR COALESCE(scheduled.scheduled_unpublished_count, 0) > 0
                ORDER BY keys.store_id ASC, this_week_published DESC, current_month_published DESC, pending_video_count DESC, keys.product_id ASC
                """,
                (
                        this_week_start,
                        this_week_end,
                        this_week_start,
                        this_week_end,
                        this_week_start,
                        this_week_end,
                        last_week_start,
                    last_week_end,
                    current_month_start,
                    current_month_end,
                    previous_month_start,
                    previous_month_end,
                    min_start,
                    max_end,
                ),
            ).fetchall()
        return [dict(row) for row in rows]

    def has_sent_notification(self, notification_key: str) -> bool:
        key = str(notification_key or "").strip()
        if not key:
            return False
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM publish_notifications WHERE notification_key = ? LIMIT 1",
                (key,),
            ).fetchone()
        return row is not None

    def mark_notification_sent(self, notification_key: str, channel: str, payload: str = "") -> None:
        key = str(notification_key or "").strip()
        if not key:
            return
        now = self._now_text()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO publish_notifications (notification_key, channel, payload, sent_at, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(notification_key) DO UPDATE SET
                    channel = excluded.channel,
                    payload = excluded.payload,
                    sent_at = excluded.sent_at
                """,
                (key, str(channel or "").strip(), str(payload or ""), now, now),
            )

    def list_manual_publish_queue(self, day: str, include_published: bool = False) -> List[Dict[str, Any]]:
        start_at = f"{str(day or '').strip()} 00:00:00"
        end_at = (datetime.strptime(str(day or "").strip(), "%Y-%m-%d") + timedelta(days=1)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        statuses = ("已排期", "发布失败")
        if include_published:
            statuses = ("已排期", "发布失败", "已发布")
        placeholders = ",".join("?" for _ in statuses)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT ps.scheduled_for,
                       ps.canonical_script_key,
                       ps.store_id,
                       ps.account_id,
                       ps.account_name,
                       ps.script_id,
                       ps.schedule_status,
                       ps.publish_task_id,
                       ps.error_message AS slot_error_message,
                       COALESCE(ac.publish_channel, '') AS publish_channel,
                       sm.product_id,
                       sm.short_video_title,
                       sm.script_source,
                       sm.publish_purpose,
                       sm.cart_enabled,
                       va.video_source_type,
                       va.video_source_value,
                       va.local_file_path,
                       va.publish_status,
                       va.publish_result,
                       va.published_at,
                       va.error_message AS asset_error_message
                FROM publish_slots ps
                INNER JOIN script_metadata sm ON sm.canonical_script_key = ps.canonical_script_key
                INNER JOIN video_assets va ON va.canonical_script_key = ps.canonical_script_key
                LEFT JOIN account_configs ac ON ac.account_id = ps.account_id
                WHERE ps.scheduled_for >= ?
                  AND ps.scheduled_for < ?
                  AND ps.schedule_status IN ({placeholders})
                ORDER BY ps.scheduled_for ASC, ps.store_id ASC, ps.account_name ASC, ps.script_id ASC
                """,
                (start_at, end_at, *statuses),
            ).fetchall()
        return [dict(row) for row in rows]
