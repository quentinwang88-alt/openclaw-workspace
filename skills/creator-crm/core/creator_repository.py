#!/usr/bin/env python3
"""Creator CRM long-term creator pool backed by SQLite/MySQL RDS."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, unquote, urlparse


MYSQL_SCHEMES = {"mysql", "mysql+pymysql"}
DEFAULT_ANALYSIS_VERSION = "creator-crm-v1"
DEFAULT_TABLE_PREFIX = "creator_crm_"


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_tiktok_handle(value: Any) -> str:
    if not value:
        return ""

    text = str(value).strip().lower()
    if not text:
        return ""

    if "?" in text:
        text = text.split("?", 1)[0]
    text = text.rstrip("/")

    for prefix in (
        "https://www.tiktok.com/@",
        "https://tiktok.com/@",
        "http://www.tiktok.com/@",
        "http://tiktok.com/@",
        "www.tiktok.com/@",
        "tiktok.com/@",
    ):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break

    if text.startswith("@"):
        text = text[1:]

    return text.strip().strip("/")


def extract_kalodata_creator_id(kalodata_url: Any) -> str:
    if not kalodata_url:
        return ""

    try:
        parsed = urlparse(str(kalodata_url))
        params = parse_qs(parsed.query)
        for key in ("creator_id", "creatorId", "id", "uid"):
            value = params.get(key, [""])[0]
            if value:
                return str(value).strip()
    except Exception:
        pass

    match = re.search(r"(?:creator[_-]?id|id|uid)[=/]([a-zA-Z0-9_-]+)", str(kalodata_url))
    return match.group(1).strip() if match else ""


def build_creator_aliases(
    *,
    tk_handle: Any = None,
    tk_url: Any = None,
    kalodata_url: Any = None,
    raw_fields: Optional[Dict[str, Any]] = None,
    record_id: Any = None,
) -> List[Tuple[str, str, str]]:
    fields = raw_fields or {}
    aliases: List[Tuple[str, str, str]] = []

    for field_name in ("TikTok creator_id", "tiktok_creator_id", "creator_id", "达人ID", "达人id"):
        value = fields.get(field_name)
        if value:
            normalized = str(value).strip().lower()
            aliases.append(("tiktok_creator_id", normalized, f"tiktok_id:{normalized}"))
            break

    kalodata_creator_id = extract_kalodata_creator_id(kalodata_url)
    if kalodata_creator_id:
        normalized = kalodata_creator_id.lower()
        aliases.append(("kalodata_creator_id", normalized, f"kalodata_id:{normalized}"))

    handle = normalize_tiktok_handle(tk_handle)
    if handle:
        aliases.append(("tiktok_handle", handle, f"handle:{handle}"))

    url_handle = normalize_tiktok_handle(tk_url)
    if url_handle and url_handle != handle:
        aliases.append(("tiktok_handle", url_handle, f"handle:{url_handle}"))

    if record_id:
        value = str(record_id).strip()
        aliases.append(("source_record_id", value, f"record:{value}"))

    deduped: List[Tuple[str, str, str]] = []
    seen = set()
    for alias in aliases:
        if alias[2] in seen:
            continue
        seen.add(alias[2])
        deduped.append(alias)
    return deduped


def stable_creator_uid(alias_keys: Sequence[str]) -> str:
    stable_keys = sorted(key for key in alias_keys if not key.startswith("record:"))
    seed = "|".join(stable_keys or sorted(alias_keys))
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:20]


def _is_mysql_url(database_url: str) -> bool:
    return urlparse(database_url).scheme in MYSQL_SCHEMES


class Database:
    def __init__(self, database_url: str, table_prefix: str = DEFAULT_TABLE_PREFIX, timeout: int = 30):
        self.database_url = database_url
        self.table_prefix = table_prefix
        self.timeout = timeout
        self.is_mysql = _is_mysql_url(database_url)

    def connect(self):
        if self.is_mysql:
            try:
                import pymysql
            except ImportError as exc:
                raise RuntimeError("PyMySQL is required for Creator CRM RDS mode. Run: python3 -m pip install --user pymysql") from exc

            parsed = urlparse(self.database_url)
            query = parse_qs(parsed.query)
            return pymysql.connect(
                host=parsed.hostname,
                port=parsed.port or 3306,
                user=unquote(parsed.username or ""),
                password=unquote(parsed.password or ""),
                database=parsed.path.lstrip("/"),
                charset=query.get("charset", ["utf8mb4"])[0],
                autocommit=False,
                connect_timeout=self.timeout,
                read_timeout=max(self.timeout, 30),
                write_timeout=max(self.timeout, 30),
                cursorclass=pymysql.cursors.DictCursor,
            )

        db_path = Path(self.database_url)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(db_path), timeout=self.timeout)
        connection.row_factory = sqlite3.Row
        return connection

    def name(self, table_name: str) -> str:
        return f"{self.table_prefix}{table_name}"

    def execute(self, connection, sql: str, params: Sequence[Any] = ()):
        rewritten = sql if self.is_mysql else sql.replace("%s", "?")
        cursor = connection.cursor()
        cursor.execute(rewritten, tuple(params))
        return cursor


@dataclass
class CreatorPoolMatch:
    creator_uid: str
    analysis: Optional[Dict[str, Any]]
    relationship: Optional[Dict[str, Any]]

    @property
    def has_complete_analysis(self) -> bool:
        return bool(self.analysis and self.analysis.get("analysis_status") == "complete")

    @property
    def has_relationship(self) -> bool:
        return bool(self.relationship and self.relationship.get("relationship_stage"))


class CreatorRepository:
    """Repository for creator identity, reusable analysis and relationship state."""

    def __init__(
        self,
        database_url: str,
        table_prefix: str = DEFAULT_TABLE_PREFIX,
        analysis_version: str = DEFAULT_ANALYSIS_VERSION,
        auto_create: bool = True,
    ):
        if not database_url:
            raise ValueError("database_url is required")
        self.db = Database(database_url, table_prefix=table_prefix)
        self.analysis_version = analysis_version
        if auto_create:
            self.ensure_schema()

    @classmethod
    def from_env(cls, database_url: str = "", auto_create: bool = True) -> Optional["CreatorRepository"]:
        resolved_url = (
            database_url
            or os.environ.get("CREATOR_CRM_DATABASE_URL")
            or os.environ.get("LIKEU_AI_DATABASE_URL")
            or ""
        ).strip()
        if not resolved_url:
            return None
        return cls(
            resolved_url,
            table_prefix=os.environ.get("CREATOR_CRM_TABLE_PREFIX", DEFAULT_TABLE_PREFIX),
            analysis_version=os.environ.get("CREATOR_CRM_ANALYSIS_VERSION", DEFAULT_ANALYSIS_VERSION),
            auto_create=auto_create,
        )

    def ensure_schema(self) -> None:
        with self.db.connect() as connection:
            cursor = connection.cursor()
            if self.db.is_mysql:
                self._ensure_mysql_schema(cursor)
            else:
                self._ensure_sqlite_schema(cursor)
            connection.commit()

    def _ensure_mysql_schema(self, cursor) -> None:
        profile = self.db.name("creator_profiles")
        aliases = self.db.name("creator_aliases")
        analysis = self.db.name("creator_analysis")
        relationship = self.db.name("creator_relationship")
        events = self.db.name("creator_contact_events")
        source_map = self.db.name("creator_source_map")
        assets = self.db.name("creator_assets")

        statements = [
            f"""
            CREATE TABLE IF NOT EXISTS `{profile}` (
              creator_uid VARCHAR(40) PRIMARY KEY,
              platform VARCHAR(32) NOT NULL DEFAULT 'tiktok',
              primary_handle VARCHAR(255),
              tiktok_url TEXT,
              kalodata_url TEXT,
              kalodata_creator_id VARCHAR(128),
              display_name VARCHAR(255),
              country VARCHAR(64),
              first_source VARCHAR(255),
              created_at DATETIME NOT NULL,
              updated_at DATETIME NOT NULL,
              KEY idx_handle (primary_handle),
              KEY idx_kalodata_creator_id (kalodata_creator_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            f"""
            CREATE TABLE IF NOT EXISTS `{aliases}` (
              alias_key VARCHAR(255) PRIMARY KEY,
              creator_uid VARCHAR(40) NOT NULL,
              alias_type VARCHAR(64) NOT NULL,
              alias_value VARCHAR(512) NOT NULL,
              created_at DATETIME NOT NULL,
              updated_at DATETIME NOT NULL,
              KEY idx_creator_uid (creator_uid),
              CONSTRAINT fk_{aliases}_creator FOREIGN KEY (creator_uid)
                REFERENCES `{profile}`(creator_uid) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            f"""
            CREATE TABLE IF NOT EXISTS `{analysis}` (
              creator_uid VARCHAR(40) PRIMARY KEY,
              analysis_status VARCHAR(32) NOT NULL DEFAULT 'partial',
              analysis_version VARCHAR(64) NOT NULL,
              video_final_score DECIMAL(4,1),
              score_reason TEXT,
              vibe_tag VARCHAR(255),
              vibe_reason TEXT,
              main_category VARCHAR(255),
              sub_category VARCHAR(255),
              tag_reason TEXT,
              screenshot_refs_json JSON,
              sample_video_refs_json JSON,
              analyzed_at DATETIME,
              updated_at DATETIME NOT NULL,
              KEY idx_status_version (analysis_status, analysis_version),
              CONSTRAINT fk_{analysis}_creator FOREIGN KEY (creator_uid)
                REFERENCES `{profile}`(creator_uid) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            f"""
            CREATE TABLE IF NOT EXISTS `{relationship}` (
              creator_uid VARCHAR(40) PRIMARY KEY,
              relationship_stage VARCHAR(64),
              current_action VARCHAR(255),
              last_contacted_at DATETIME,
              last_reply_at DATETIME,
              owner VARCHAR(128),
              outreach_batch_no VARCHAR(128),
              planned_product VARCHAR(255),
              notes TEXT,
              created_at DATETIME NOT NULL,
              updated_at DATETIME NOT NULL,
              KEY idx_relationship_stage (relationship_stage),
              KEY idx_outreach_batch_no (outreach_batch_no),
              CONSTRAINT fk_{relationship}_creator FOREIGN KEY (creator_uid)
                REFERENCES `{profile}`(creator_uid) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            f"""
            CREATE TABLE IF NOT EXISTS `{events}` (
              id BIGINT AUTO_INCREMENT PRIMARY KEY,
              creator_uid VARCHAR(40) NOT NULL,
              event_type VARCHAR(64) NOT NULL,
              event_time DATETIME NOT NULL,
              operator VARCHAR(128),
              batch_no VARCHAR(128),
              product_name VARCHAR(255),
              detail_json JSON,
              created_at DATETIME NOT NULL,
              KEY idx_creator_time (creator_uid, event_time),
              CONSTRAINT fk_{events}_creator FOREIGN KEY (creator_uid)
                REFERENCES `{profile}`(creator_uid) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            f"""
            CREATE TABLE IF NOT EXISTS `{source_map}` (
              source_key VARCHAR(255) PRIMARY KEY,
              creator_uid VARCHAR(40) NOT NULL,
              app_token VARCHAR(128),
              table_id VARCHAR(128),
              record_id VARCHAR(128),
              source_status VARCHAR(128),
              raw_fields_json JSON,
              created_at DATETIME NOT NULL,
              updated_at DATETIME NOT NULL,
              KEY idx_creator_uid (creator_uid),
              CONSTRAINT fk_{source_map}_creator FOREIGN KEY (creator_uid)
                REFERENCES `{profile}`(creator_uid) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            f"""
            CREATE TABLE IF NOT EXISTS `{assets}` (
              asset_id VARCHAR(64) PRIMARY KEY,
              creator_uid VARCHAR(40),
              asset_type VARCHAR(64) NOT NULL,
              storage_provider VARCHAR(32) NOT NULL,
              bucket VARCHAR(255),
              object_key VARCHAR(1024) NOT NULL,
              public_url TEXT,
              source_path TEXT,
              file_name VARCHAR(255),
              file_size BIGINT,
              file_hash VARCHAR(128),
              retention_days INT NOT NULL DEFAULT 30,
              storage_status VARCHAR(32) NOT NULL DEFAULT 'uploaded',
              created_at DATETIME NOT NULL,
              expires_at DATETIME,
              deleted_at DATETIME,
              meta_json JSON,
              KEY idx_creator_uid (creator_uid),
              KEY idx_expires_status (expires_at, storage_status),
              CONSTRAINT fk_{assets}_creator FOREIGN KEY (creator_uid)
                REFERENCES `{profile}`(creator_uid) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
        ]
        for statement in statements:
            cursor.execute(statement)

    def _ensure_sqlite_schema(self, cursor) -> None:
        profile = self.db.name("creator_profiles")
        aliases = self.db.name("creator_aliases")
        analysis = self.db.name("creator_analysis")
        relationship = self.db.name("creator_relationship")
        events = self.db.name("creator_contact_events")
        source_map = self.db.name("creator_source_map")
        assets = self.db.name("creator_assets")

        statements = [
            f"""
            CREATE TABLE IF NOT EXISTS {profile} (
              creator_uid TEXT PRIMARY KEY,
              platform TEXT NOT NULL DEFAULT 'tiktok',
              primary_handle TEXT,
              tiktok_url TEXT,
              kalodata_url TEXT,
              kalodata_creator_id TEXT,
              display_name TEXT,
              country TEXT,
              first_source TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {aliases} (
              alias_key TEXT PRIMARY KEY,
              creator_uid TEXT NOT NULL,
              alias_type TEXT NOT NULL,
              alias_value TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {analysis} (
              creator_uid TEXT PRIMARY KEY,
              analysis_status TEXT NOT NULL DEFAULT 'partial',
              analysis_version TEXT NOT NULL,
              video_final_score REAL,
              score_reason TEXT,
              vibe_tag TEXT,
              vibe_reason TEXT,
              main_category TEXT,
              sub_category TEXT,
              tag_reason TEXT,
              screenshot_refs_json TEXT,
              sample_video_refs_json TEXT,
              analyzed_at TEXT,
              updated_at TEXT NOT NULL
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {relationship} (
              creator_uid TEXT PRIMARY KEY,
              relationship_stage TEXT,
              current_action TEXT,
              last_contacted_at TEXT,
              last_reply_at TEXT,
              owner TEXT,
              outreach_batch_no TEXT,
              planned_product TEXT,
              notes TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {events} (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              creator_uid TEXT NOT NULL,
              event_type TEXT NOT NULL,
              event_time TEXT NOT NULL,
              operator TEXT,
              batch_no TEXT,
              product_name TEXT,
              detail_json TEXT,
              created_at TEXT NOT NULL
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {source_map} (
              source_key TEXT PRIMARY KEY,
              creator_uid TEXT NOT NULL,
              app_token TEXT,
              table_id TEXT,
              record_id TEXT,
              source_status TEXT,
              raw_fields_json TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {assets} (
              asset_id TEXT PRIMARY KEY,
              creator_uid TEXT,
              asset_type TEXT NOT NULL,
              storage_provider TEXT NOT NULL,
              bucket TEXT,
              object_key TEXT NOT NULL,
              public_url TEXT,
              source_path TEXT,
              file_name TEXT,
              file_size INTEGER,
              file_hash TEXT,
              retention_days INTEGER NOT NULL DEFAULT 30,
              storage_status TEXT NOT NULL DEFAULT 'uploaded',
              created_at TEXT NOT NULL,
              expires_at TEXT,
              deleted_at TEXT,
              meta_json TEXT
            )
            """,
            f"CREATE INDEX IF NOT EXISTS idx_{aliases}_creator_uid ON {aliases}(creator_uid)",
            f"CREATE INDEX IF NOT EXISTS idx_{analysis}_status_version ON {analysis}(analysis_status, analysis_version)",
            f"CREATE INDEX IF NOT EXISTS idx_{relationship}_stage ON {relationship}(relationship_stage)",
            f"CREATE INDEX IF NOT EXISTS idx_{source_map}_creator_uid ON {source_map}(creator_uid)",
            f"CREATE INDEX IF NOT EXISTS idx_{assets}_expires_status ON {assets}(expires_at, storage_status)",
        ]
        for statement in statements:
            cursor.execute(statement)

    def upsert_creator_from_payload(
        self,
        payload: Dict[str, Any],
        *,
        app_token: str = "",
        table_id: str = "",
        source_status: str = "",
    ) -> str:
        aliases = build_creator_aliases(
            tk_handle=payload.get("tk_handle"),
            tk_url=payload.get("tk_url"),
            kalodata_url=payload.get("kalodata_url"),
            raw_fields=payload.get("raw_fields") or {},
            record_id=payload.get("record_id"),
        )
        return self.upsert_creator(
            aliases=aliases,
            tk_handle=payload.get("tk_handle"),
            tk_url=payload.get("tk_url"),
            kalodata_url=payload.get("kalodata_url"),
            raw_fields=payload.get("raw_fields") or {},
            record_id=payload.get("record_id"),
            app_token=app_token,
            table_id=table_id,
            source_status=source_status,
        )

    def upsert_creator(
        self,
        *,
        aliases: List[Tuple[str, str, str]],
        tk_handle: Any = None,
        tk_url: Any = None,
        kalodata_url: Any = None,
        raw_fields: Optional[Dict[str, Any]] = None,
        record_id: Any = None,
        app_token: str = "",
        table_id: str = "",
        source_status: str = "",
    ) -> str:
        alias_keys = [alias_key for _, _, alias_key in aliases]
        creator_uid = self.find_creator_uid(alias_keys) or stable_creator_uid(alias_keys)
        timestamp = now_iso()
        fields = raw_fields or {}
        source_key = f"{app_token}:{table_id}:{record_id}" if app_token and table_id and record_id else ""

        with self.db.connect() as connection:
            self._execute(
                connection,
                "profile_upsert",
                {
                    "creator_uid": creator_uid,
                    "primary_handle": normalize_tiktok_handle(tk_handle) or str(tk_handle or ""),
                    "tiktok_url": str(tk_url or ""),
                    "kalodata_url": str(kalodata_url or ""),
                    "kalodata_creator_id": extract_kalodata_creator_id(kalodata_url),
                    "display_name": str(fields.get("达人名称") or fields.get("昵称") or ""),
                    "country": str(fields.get("国家") or fields.get("市场") or ""),
                    "first_source": source_key,
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
            )
            for alias_type, alias_value, alias_key in aliases:
                self._execute(
                    connection,
                    "alias_upsert",
                    {
                        "alias_key": alias_key,
                        "creator_uid": creator_uid,
                        "alias_type": alias_type,
                        "alias_value": alias_value,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                    },
                )
            if source_key:
                self._execute(
                    connection,
                    "source_upsert",
                    {
                        "source_key": source_key,
                        "creator_uid": creator_uid,
                        "app_token": app_token,
                        "table_id": table_id,
                        "record_id": str(record_id),
                        "source_status": source_status,
                        "raw_fields_json": json.dumps(fields, ensure_ascii=False, default=str),
                        "created_at": timestamp,
                        "updated_at": timestamp,
                    },
                )
            connection.commit()
        return creator_uid

    def find_creator_uid(self, alias_keys: Iterable[str]) -> Optional[str]:
        keys = [key for key in alias_keys if key]
        if not keys:
            return None

        placeholders = ",".join(["%s"] * len(keys))
        sql = f"SELECT creator_uid FROM {self._table('creator_aliases')} WHERE alias_key IN ({placeholders}) LIMIT 1"
        with self.db.connect() as connection:
            cursor = self.db.execute(connection, sql, keys)
            row = cursor.fetchone()
            return self._row_get(row, "creator_uid") if row else None

    def lookup_by_payload(self, payload: Dict[str, Any]) -> Optional[CreatorPoolMatch]:
        aliases = build_creator_aliases(
            tk_handle=payload.get("tk_handle"),
            tk_url=payload.get("tk_url"),
            kalodata_url=payload.get("kalodata_url"),
            raw_fields=payload.get("raw_fields") or {},
            record_id=payload.get("record_id"),
        )
        alias_keys = [alias_key for _, _, alias_key in aliases]
        creator_uid = self.find_creator_uid(alias_keys)
        if not creator_uid:
            return None
        return CreatorPoolMatch(
            creator_uid=creator_uid,
            analysis=self.get_analysis(creator_uid),
            relationship=self.get_relationship(creator_uid),
        )

    def get_analysis(self, creator_uid: str) -> Optional[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._table('creator_analysis')} WHERE creator_uid = %s LIMIT 1"
        with self.db.connect() as connection:
            cursor = self.db.execute(connection, sql, (creator_uid,))
            row = cursor.fetchone()
            return self._row_to_dict(row) if row else None

    def get_relationship(self, creator_uid: str) -> Optional[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._table('creator_relationship')} WHERE creator_uid = %s LIMIT 1"
        with self.db.connect() as connection:
            cursor = self.db.execute(connection, sql, (creator_uid,))
            row = cursor.fetchone()
            return self._row_to_dict(row) if row else None

    def upsert_analysis_from_result(
        self,
        creator_uid: str,
        *,
        scoring_result: Optional[Dict[str, Any]] = None,
        category_result: Optional[Dict[str, Any]] = None,
        vibe_result: Optional[Dict[str, Any]] = None,
        screenshot_refs: Optional[List[Any]] = None,
        sample_video_refs: Optional[List[Any]] = None,
        analysis_status: str = "complete",
    ) -> None:
        timestamp = now_iso()
        scoring_result = scoring_result or {}
        category_result = category_result or {}
        vibe_result = vibe_result or {}

        self._upsert_analysis(
            {
                "creator_uid": creator_uid,
                "analysis_status": analysis_status,
                "analysis_version": self.analysis_version,
                "video_final_score": scoring_result.get("final_star_rating"),
                "score_reason": scoring_result.get("analysis_reason") or "",
                "vibe_tag": vibe_result.get("vibe_tag") or "",
                "vibe_reason": vibe_result.get("vibe_reason") or "",
                "main_category": category_result.get("main_category_1") or "",
                "sub_category": category_result.get("sub_category_1") or "",
                "tag_reason": category_result.get("analysis_reason") or "",
                "screenshot_refs_json": json.dumps(screenshot_refs or [], ensure_ascii=False, default=str),
                "sample_video_refs_json": json.dumps(sample_video_refs or [], ensure_ascii=False, default=str),
                "analyzed_at": timestamp if analysis_status == "complete" else None,
                "updated_at": timestamp,
            }
        )

    def record_asset(
        self,
        *,
        creator_uid: str = "",
        asset_type: str,
        storage_provider: str,
        bucket: str = "",
        object_key: str,
        public_url: str = "",
        source_path: str = "",
        file_name: str = "",
        file_size: Optional[int] = None,
        file_hash: str = "",
        retention_days: int = 30,
        meta: Optional[Dict[str, Any]] = None,
    ) -> str:
        timestamp = now_iso()
        expires_at = (datetime.now() + timedelta(days=max(0, retention_days))).isoformat(timespec="seconds") if retention_days > 0 else None
        seed = f"{storage_provider}|{bucket}|{object_key}|{timestamp}"
        asset_id = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:24]
        values = {
            "asset_id": asset_id,
            "creator_uid": creator_uid or None,
            "asset_type": asset_type,
            "storage_provider": storage_provider,
            "bucket": bucket,
            "object_key": object_key,
            "public_url": public_url,
            "source_path": source_path,
            "file_name": file_name,
            "file_size": file_size,
            "file_hash": file_hash,
            "retention_days": retention_days,
            "storage_status": "uploaded",
            "created_at": timestamp,
            "expires_at": expires_at,
            "deleted_at": None,
            "meta_json": json.dumps(meta or {}, ensure_ascii=False, default=str),
        }
        with self.db.connect() as connection:
            self._execute(connection, "asset_upsert", values)
            connection.commit()
        return asset_id

    def list_expired_assets(self, *, limit: int = 200, asset_type: str = "grid_image") -> List[Dict[str, Any]]:
        timestamp = now_iso()
        sql = (
            f"SELECT * FROM {self._table('creator_assets')} "
            "WHERE storage_status = %s AND expires_at IS NOT NULL AND expires_at <= %s"
        )
        params: List[Any] = ["uploaded", timestamp]
        if asset_type:
            sql += " AND asset_type = %s"
            params.append(asset_type)
        sql += " ORDER BY expires_at ASC LIMIT %s"
        params.append(limit)
        with self.db.connect() as connection:
            cursor = self.db.execute(connection, sql, params)
            return [self._row_to_dict(row) for row in cursor.fetchall()]

    def mark_asset_deleted(self, asset_id: str) -> None:
        sql = f"UPDATE {self._table('creator_assets')} SET storage_status = %s, deleted_at = %s WHERE asset_id = %s"
        with self.db.connect() as connection:
            self.db.execute(connection, sql, ("deleted", now_iso(), asset_id))
            connection.commit()

    def _upsert_analysis(self, values: Dict[str, Any]) -> None:
        with self.db.connect() as connection:
            self._execute(connection, "analysis_upsert", values)
            connection.commit()

    def _execute(self, connection, statement_name: str, values: Dict[str, Any]) -> None:
        table_sql = {
            "profile_upsert": (
                "creator_profiles",
                ["creator_uid", "platform", "primary_handle", "tiktok_url", "kalodata_url", "kalodata_creator_id", "display_name", "country", "first_source", "created_at", "updated_at"],
                ["creator_uid", "'tiktok'", "primary_handle", "tiktok_url", "kalodata_url", "kalodata_creator_id", "display_name", "country", "first_source", "created_at", "updated_at"],
                ["primary_handle", "tiktok_url", "kalodata_url", "kalodata_creator_id", "display_name", "country", "updated_at"],
            ),
            "alias_upsert": (
                "creator_aliases",
                ["alias_key", "creator_uid", "alias_type", "alias_value", "created_at", "updated_at"],
                ["alias_key", "creator_uid", "alias_type", "alias_value", "created_at", "updated_at"],
                ["creator_uid", "alias_type", "alias_value", "updated_at"],
            ),
            "source_upsert": (
                "creator_source_map",
                ["source_key", "creator_uid", "app_token", "table_id", "record_id", "source_status", "raw_fields_json", "created_at", "updated_at"],
                ["source_key", "creator_uid", "app_token", "table_id", "record_id", "source_status", "raw_fields_json", "created_at", "updated_at"],
                ["creator_uid", "source_status", "raw_fields_json", "updated_at"],
            ),
            "analysis_upsert": (
                "creator_analysis",
                ["creator_uid", "analysis_status", "analysis_version", "video_final_score", "score_reason", "vibe_tag", "vibe_reason", "main_category", "sub_category", "tag_reason", "screenshot_refs_json", "sample_video_refs_json", "analyzed_at", "updated_at"],
                ["creator_uid", "analysis_status", "analysis_version", "video_final_score", "score_reason", "vibe_tag", "vibe_reason", "main_category", "sub_category", "tag_reason", "screenshot_refs_json", "sample_video_refs_json", "analyzed_at", "updated_at"],
                ["analysis_status", "analysis_version", "video_final_score", "score_reason", "vibe_tag", "vibe_reason", "main_category", "sub_category", "tag_reason", "screenshot_refs_json", "sample_video_refs_json", "analyzed_at", "updated_at"],
            ),
            "asset_upsert": (
                "creator_assets",
                ["asset_id", "creator_uid", "asset_type", "storage_provider", "bucket", "object_key", "public_url", "source_path", "file_name", "file_size", "file_hash", "retention_days", "storage_status", "created_at", "expires_at", "deleted_at", "meta_json"],
                ["asset_id", "creator_uid", "asset_type", "storage_provider", "bucket", "object_key", "public_url", "source_path", "file_name", "file_size", "file_hash", "retention_days", "storage_status", "created_at", "expires_at", "deleted_at", "meta_json"],
                ["creator_uid", "public_url", "source_path", "file_size", "file_hash", "retention_days", "storage_status", "expires_at", "deleted_at", "meta_json"],
            ),
        }
        table_name, columns, source_keys, update_columns = table_sql[statement_name]
        params = []
        insert_values = []
        for source_key in source_keys:
            if source_key.startswith("'") and source_key.endswith("'"):
                insert_values.append(source_key)
            else:
                insert_values.append("%s")
                params.append(values.get(source_key))

        if self.db.is_mysql:
            updates = ", ".join(f"{column}=VALUES({column})" for column in update_columns)
            sql = (
                f"INSERT INTO {self._table(table_name)} ({', '.join(columns)}) "
                f"VALUES ({', '.join(insert_values)}) "
                f"ON DUPLICATE KEY UPDATE {updates}"
            )
        else:
            updates = ", ".join(f"{column}=excluded.{column}" for column in update_columns)
            sql = (
                f"INSERT INTO {self._table(table_name)} ({', '.join(columns)}) "
                f"VALUES ({', '.join(insert_values)}) "
                f"ON CONFLICT({columns[0]}) DO UPDATE SET {updates}"
            )
        self.db.execute(connection, sql, params)

    def _table(self, table_name: str) -> str:
        name = self.db.name(table_name)
        return f"`{name}`" if self.db.is_mysql else name

    @staticmethod
    def _row_get(row: Any, key: str) -> Any:
        if isinstance(row, sqlite3.Row):
            return row[key]
        return row.get(key)

    @classmethod
    def _row_to_dict(cls, row: Any) -> Dict[str, Any]:
        if isinstance(row, sqlite3.Row):
            return {key: row[key] for key in row.keys()}
        return dict(row)
