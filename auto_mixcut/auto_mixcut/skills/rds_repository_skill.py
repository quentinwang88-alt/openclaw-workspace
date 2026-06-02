from __future__ import annotations

from pathlib import Path

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext


class RDSRepositorySkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def init_db(self) -> Result:
        if getattr(self.ctx.repo, "dialect", "sqlite") == "mysql":
            ensure = getattr(self.ctx.repo, "ensure_llm_router_tables", None)
            if callable(ensure):
                ensure()
                compatible = _ensure_mysql_runtime_compatibility_columns(self.ctx)
                if not compatible.success:
                    return compatible
                return Result.ok({"migrations": ["ensure_llm_router_tables", "ensure_runtime_compatibility_columns"], "db_provider": "mysql"})
            return Result.fail("MYSQL_MIGRATION_UNAVAILABLE", "mysql repository cannot initialize tables")
        migrations_dir = self.ctx.settings.root_dir / "migrations"
        sql_files = sorted(m for m in migrations_dir.glob("*.sql") if not m.name.endswith("_mysql_init.sql"))
        for migration in sql_files:
            res = self.ctx.repo.migrate(migration)
            if not res.success:
                return res
        compatible = _ensure_runtime_compatibility_columns(self.ctx)
        if not compatible.success:
            return compatible
        return Result.ok({"migrations": [m.name for m in sql_files]})

    def create_product_task(
        self,
        product_id: str,
        product_name: str,
        market: str,
        category: str,
        requested_count: int,
        shop_id: str = "",
        priority: str = "normal",
    ) -> Result:
        product = self.ctx.repo.upsert(
            "products",
            "product_id",
            {
                "product_id": product_id,
                "product_name": product_name,
                "market": market,
                "category": category,
                "shop_id": shop_id,
                "priority": priority,
                "anchor_status": "pending",
                "product_status": "active",
            },
        )
        if not product.success:
            return product
        task_id = new_id("TASK")
        task = self.ctx.repo.upsert(
            "content_tasks",
            "task_id",
            {
                "task_id": task_id,
                "product_id": product_id,
                "task_type": "mixcut",
                "requested_variant_count": requested_count,
                "task_status": "CREATED",
                "anchor_status_at_task_start": "pending",
                "created_by": "cli",
            },
        )
        return task if not task.success else Result.ok({"task_id": task_id, "product_id": product_id})


def _ensure_runtime_compatibility_columns(ctx: SkillContext) -> Result:
    additions = {
        "products": {"preferred_mood": "TEXT"},
        "assets": {"source_identity": "TEXT", "scene_tag": "TEXT"},
        "segments": {"visual_phash": "TEXT"},
        "segment_tags": {"text_overlay_risk": "TEXT", "text_language": "TEXT", "text_overlay_reason": "TEXT"},
        "outputs": {
            "bgm_output_oss_object_id": "TEXT",
            "human_feedback_reason": "TEXT",
            "remix_plan_json": "TEXT",
            "final_qc_json": "TEXT",
            "bgm_plan_json": "TEXT",
            "avg_completion_rate": "REAL",
            "published_at": "TEXT",
        },
        "bgm_tracks": {
            "audio_analysis_json": "TEXT",
            "audio_analyzed_at": "TEXT",
            "audio_tag_source": "TEXT",
            "audio_tag_confidence": "TEXT",
        },
    }
    try:
        with ctx.repo.connect() as conn:
            for table, columns in additions.items():
                existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
                for column, spec in columns.items():
                    if column not in existing:
                        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {spec}")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS segment_visual_fingerprints (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  fingerprint_id TEXT NOT NULL UNIQUE,
                  product_id TEXT,
                  segment_id TEXT,
                  source_type TEXT,
                  phash TEXT,
                  hash_method TEXT,
                  frame_count INTEGER,
                  created_at TEXT,
                  updated_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_diversity_alerts (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  alert_id TEXT NOT NULL UNIQUE,
                  product_id TEXT NOT NULL,
                  reason TEXT,
                  phase TEXT,
                  ai_ratio_cap REAL,
                  trusted_real_anchor_count INTEGER,
                  alert_json TEXT,
                  status TEXT,
                  created_at TEXT,
                  updated_at TEXT
                )
                """
            )
        return Result.ok()
    except Exception as exc:
        return Result.fail("MIGRATION_FAILED", str(exc), {"db_path": str(ctx.settings.db_path)})


def _ensure_mysql_runtime_compatibility_columns(ctx: SkillContext) -> Result:
    additions = {
        "products": {"preferred_mood": "JSON"},
        "assets": {"source_identity": "VARCHAR(256)", "scene_tag": "VARCHAR(256)"},
        "segments": {"visual_phash": "VARCHAR(64)"},
        "segment_tags": {"text_overlay_risk": "VARCHAR(64)", "text_language": "VARCHAR(64)", "text_overlay_reason": "TEXT"},
        "outputs": {
            "bgm_output_oss_object_id": "VARCHAR(128)",
            "human_feedback_reason": "TEXT",
            "remix_plan_json": "JSON",
            "final_qc_json": "JSON",
            "bgm_plan_json": "JSON",
            "avg_completion_rate": "DECIMAL(6, 4)",
            "published_at": "DATETIME",
        },
        "bgm_tracks": {
            "audio_analysis_json": "JSON",
            "audio_analyzed_at": "DATETIME",
            "audio_tag_source": "VARCHAR(64)",
            "audio_tag_confidence": "VARCHAR(32)",
        },
    }
    try:
        with ctx.repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bgm_tracks (
                      id BIGINT PRIMARY KEY AUTO_INCREMENT,
                      bgm_id VARCHAR(128) NOT NULL UNIQUE,
                      track_name VARCHAR(512),
                      artist_name VARCHAR(256),
                      source_platform VARCHAR(128),
                      source_url TEXT,
                      file_name VARCHAR(512),
                      download_version VARCHAR(64),
                      duration_ms INT,
                      file_size BIGINT,
                      audio_format VARCHAR(64),
                      sample_rate INT,
                      channels INT,
                      official_tags_json JSON,
                      license_note TEXT,
                      oss_object_id VARCHAR(128),
                      local_file_path TEXT,
                      mood_tags_json JSON,
                      energy_level VARCHAR(32),
                      vocal_type VARCHAR(64),
                      category_tags_json JSON,
                      template_tags_json JSON,
                      recommended_start_sec DECIMAL(8, 2) DEFAULT 0,
                      default_volume DECIMAL(5, 3) DEFAULT 0.2,
                      fade_in_ms INT DEFAULT 500,
                      fade_out_ms INT DEFAULT 800,
                      suitable_for_intro TINYINT DEFAULT 1,
                      loop_friendly TINYINT DEFAULT 0,
                      voiceover_friendly TINYINT DEFAULT 1,
                      ai_suggested_tags_json JSON,
                      tag_diff_json JSON,
                      tag_confidence VARCHAR(32),
                      tag_review_required TINYINT DEFAULT 0,
                      bgm_tag_status VARCHAR(64) DEFAULT 'untagged',
                      bgm_tagged_at DATETIME,
                      bgm_tag_prompt_version VARCHAR(64),
                      bgm_tag_reason TEXT,
                      existing_human_tags_json JSON,
                      allowed_labels_json JSON,
                      mix_constraints_json JSON,
                      performance_tags_json JSON,
                      usage_count INT DEFAULT 0,
                      created_at DATETIME,
                      updated_at DATETIME
                    )
                    """
                )
                for table, columns in additions.items():
                    if not _mysql_table_exists(ctx, table):
                        continue
                    for column, spec in columns.items():
                        if not ctx.repo._has_column(table, column):
                            cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {spec}")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS segment_visual_fingerprints (
                      id BIGINT PRIMARY KEY AUTO_INCREMENT,
                      fingerprint_id VARCHAR(128) NOT NULL UNIQUE,
                      product_id VARCHAR(128),
                      segment_id VARCHAR(128),
                      source_type VARCHAR(64),
                      phash VARCHAR(64),
                      hash_method VARCHAR(64),
                      frame_count INT,
                      created_at DATETIME,
                      updated_at DATETIME
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ai_diversity_alerts (
                      id BIGINT PRIMARY KEY AUTO_INCREMENT,
                      alert_id VARCHAR(128) NOT NULL UNIQUE,
                      product_id VARCHAR(128) NOT NULL,
                      reason VARCHAR(256),
                      phase VARCHAR(32),
                      ai_ratio_cap DECIMAL(5, 3),
                      trusted_real_anchor_count INT,
                      alert_json JSON,
                      status VARCHAR(64),
                      created_at DATETIME,
                      updated_at DATETIME
                    )
                    """
                )
        return Result.ok()
    except Exception as exc:
        return Result.fail("MIGRATION_FAILED", str(exc), {"db_provider": "mysql"})


def _mysql_table_exists(ctx: SkillContext, table: str) -> bool:
    try:
        with ctx.repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema=%s AND table_name=%s
                    LIMIT 1
                    """,
                    (ctx.repo.database, table),
                )
                return cur.fetchone() is not None
    except Exception:
        return False
