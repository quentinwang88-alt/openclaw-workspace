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
                core = _ensure_mysql_core_tables(self.ctx)
                if not core.success:
                    return core
                ensure()
                compatible = _ensure_mysql_runtime_compatibility_columns(self.ctx)
                if not compatible.success:
                    return compatible
                return Result.ok({"migrations": ["ensure_mysql_core_tables", "ensure_llm_router_tables", "ensure_runtime_compatibility_columns"], "db_provider": "mysql"})
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


def _ensure_mysql_core_tables(ctx: SkillContext) -> Result:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS products (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          product_id VARCHAR(128) NOT NULL UNIQUE,
          product_name VARCHAR(512),
          market VARCHAR(64),
          category VARCHAR(128),
          shop_id VARCHAR(128),
          priority VARCHAR(64),
          product_anchor_json JSON,
          preferred_mood JSON,
          anchor_status VARCHAR(64) DEFAULT 'pending',
          anchor_version VARCHAR(64),
          anchor_confirmed_at DATETIME,
          anchor_confirmed_by VARCHAR(128),
          product_status VARCHAR(64),
          created_at DATETIME,
          updated_at DATETIME
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS content_tasks (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          task_id VARCHAR(128) NOT NULL UNIQUE,
          product_id VARCHAR(128) NOT NULL,
          task_type VARCHAR(64),
          requested_variant_count INT DEFAULT 0,
          allowed_variant_count INT DEFAULT 0,
          actual_variant_count INT DEFAULT 0,
          material_tier VARCHAR(64),
          material_status VARCHAR(64),
          task_status VARCHAR(128),
          anchor_required TINYINT DEFAULT 1,
          anchor_status_at_task_start VARCHAR(64),
          blocked_reason TEXT,
          failure_reason TEXT,
          created_by VARCHAR(128),
          created_at DATETIME,
          updated_at DATETIME,
          KEY idx_content_tasks_product (product_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS oss_objects (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          object_id VARCHAR(128) NOT NULL UNIQUE,
          bucket VARCHAR(256),
          object_key TEXT,
          object_type VARCHAR(64),
          file_name VARCHAR(512),
          file_ext VARCHAR(64),
          mime_type VARCHAR(128),
          file_size BIGINT,
          file_hash VARCHAR(128),
          storage_status VARCHAR(64),
          lifecycle_policy VARCHAR(128),
          expire_at DATETIME,
          created_at DATETIME,
          updated_at DATETIME
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS assets (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          asset_id VARCHAR(128) NOT NULL UNIQUE,
          product_id VARCHAR(128) NOT NULL,
          source_type VARCHAR(64),
          source_trust_level VARCHAR(64),
          product_binding_type VARCHAR(64),
          media_type VARCHAR(64),
          original_oss_object_id VARCHAR(128),
          normalized_oss_object_id VARCHAR(128),
          file_status VARCHAR(64),
          probe_status VARCHAR(64),
          duration_ms INT,
          width INT,
          height INT,
          fps DECIMAL(8, 3),
          codec VARCHAR(128),
          orientation VARCHAR(64),
          has_audio TINYINT,
          probe_json JSON,
          has_watermark VARCHAR(64) DEFAULT 'pending',
          watermark_type VARCHAR(128),
          watermark_position VARCHAR(128),
          watermark_confidence VARCHAR(64),
          watermark_reason TEXT,
          watermark_checked_at DATETIME,
          risk_level VARCHAR(64),
          asset_status VARCHAR(64),
          local_file_status VARCHAR(64),
          ai_tag_status VARCHAR(64),
          human_review_status VARCHAR(64),
          source_identity VARCHAR(256),
          scene_tag VARCHAR(256),
          generation_job_id VARCHAR(128),
          generation_type VARCHAR(128),
          generation_model VARCHAR(128),
          generation_prompt TEXT,
          created_at DATETIME,
          updated_at DATETIME,
          KEY idx_assets_product (product_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS segments (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          segment_id VARCHAR(128) NOT NULL UNIQUE,
          asset_id VARCHAR(128) NOT NULL,
          product_id VARCHAR(128) NOT NULL,
          segment_oss_object_id VARCHAR(128),
          thumbnail_oss_object_id VARCHAR(128),
          start_ms INT,
          end_ms INT,
          duration_ms INT,
          width INT,
          height INT,
          fps DECIMAL(8, 3),
          segment_status VARCHAR(64),
          source_type VARCHAR(64),
          source_trust_level VARCHAR(64),
          product_binding_type VARCHAR(64),
          product_match_status VARCHAR(64),
          product_match_confidence VARCHAR(64),
          product_match_review_required TINYINT,
          product_match_reason TEXT,
          effective_roles_json JSON,
          effective_roles_updated_at DATETIME,
          effective_roles_reason TEXT,
          frame_consistency_score DECIMAL(8, 4),
          frame_consistency_status VARCHAR(64),
          frame_consistency_reason TEXT,
          visual_phash VARCHAR(64),
          is_image_generated TINYINT DEFAULT 0,
          usage_count INT DEFAULT 0,
          used_in_outputs_count INT DEFAULT 0,
          avg_first_3s_retention DECIMAL(8, 4),
          avg_completion_rate DECIMAL(8, 4),
          performance_score DECIMAL(8, 4),
          last_performance_update DATETIME,
          anchor_match_level VARCHAR(64),
          anchor_check_reason TEXT,
          allowed_core_roles_json JSON,
          allowed_soft_roles_json JSON,
          segment_type VARCHAR(128),
          created_at DATETIME,
          updated_at DATETIME,
          KEY idx_segments_product (product_id),
          KEY idx_segments_asset (asset_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS segment_frames (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          frame_id VARCHAR(128) NOT NULL UNIQUE,
          segment_id VARCHAR(128) NOT NULL,
          frame_index INT,
          timestamp_ms INT,
          oss_object_id VARCHAR(128),
          created_at DATETIME,
          KEY idx_segment_frames_segment (segment_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS segment_tags (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          segment_id VARCHAR(128) NOT NULL,
          tag_source VARCHAR(128),
          primary_shot_role VARCHAR(64),
          secondary_roles_json JSON,
          product_visibility VARCHAR(64),
          hook_strength VARCHAR(64),
          mixcut_usability VARCHAR(64),
          risk_level VARCHAR(64),
          confidence VARCHAR(64),
          needs_human_review TINYINT,
          reason TEXT,
          text_overlay_risk VARCHAR(64),
          text_language VARCHAR(64),
          text_overlay_reason TEXT,
          reviewer_id VARCHAR(128),
          reviewed_at DATETIME,
          created_at DATETIME,
          KEY idx_segment_tags_segment (segment_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ai_batches (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          ai_batch_id VARCHAR(128) NOT NULL UNIQUE,
          product_id VARCHAR(128),
          batch_type VARCHAR(64),
          status VARCHAR(64),
          total_segments INT DEFAULT 0,
          completed_segments INT DEFAULT 0,
          failed_segments INT DEFAULT 0,
          model_tier VARCHAR(64),
          prompt_version VARCHAR(64),
          created_at DATETIME,
          updated_at DATETIME
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ai_tag_runs (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          tag_run_id VARCHAR(128) NOT NULL UNIQUE,
          ai_batch_id VARCHAR(128),
          segment_id VARCHAR(128),
          model_tier VARCHAR(64),
          model_name VARCHAR(256),
          prompt_version VARCHAR(64),
          run_type VARCHAR(64),
          temperature DECIMAL(6, 3),
          raw_response JSON,
          parsed_success TINYINT,
          error_message TEXT,
          created_at DATETIME
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS mixcut_batches (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          batch_id VARCHAR(128) NOT NULL UNIQUE,
          product_id VARCHAR(128),
          task_id VARCHAR(128),
          requested_count INT,
          allowed_count INT,
          rendered_count INT,
          batch_status VARCHAR(64),
          material_tier VARCHAR(64),
          template_pool_json JSON,
          experiment_batch VARCHAR(256),
          created_at DATETIME,
          updated_at DATETIME
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS render_plans (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          render_plan_id VARCHAR(128) NOT NULL UNIQUE,
          batch_id VARCHAR(128),
          product_id VARCHAR(128),
          variant_no INT,
          template_id VARCHAR(128),
          planned_duration_ms INT,
          plan_json JSON,
          quality_gate_status VARCHAR(64),
          render_status VARCHAR(64),
          created_at DATETIME,
          KEY idx_render_plans_batch (batch_id),
          KEY idx_render_plans_product (product_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS outputs (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          output_id VARCHAR(128) NOT NULL UNIQUE,
          batch_id VARCHAR(128),
          product_id VARCHAR(128),
          variant_no INT,
          template_id VARCHAR(128),
          output_oss_object_id VARCHAR(128),
          bgm_output_oss_object_id VARCHAR(128),
          cover_oss_object_id VARCHAR(128),
          duration_ms INT,
          width INT,
          height INT,
          render_status VARCHAR(64),
          machine_quality_status VARCHAR(64),
          human_quality_status VARCHAR(64),
          human_feedback_reason TEXT,
          remix_plan_json JSON,
          final_qc_json JSON,
          bgm_plan_json JSON,
          avg_completion_rate DECIMAL(8, 4),
          published_at DATETIME,
          feishu_preview_status VARCHAR(64),
          feishu_record_id VARCHAR(128),
          preview_expire_at DATETIME,
          experiment_group VARCHAR(128),
          experiment_batch VARCHAR(128),
          created_at DATETIME,
          updated_at DATETIME,
          KEY idx_outputs_batch (batch_id),
          KEY idx_outputs_product (product_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS output_segments (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          output_id VARCHAR(128) NOT NULL,
          segment_id VARCHAR(128) NOT NULL,
          asset_id VARCHAR(128),
          slot_index INT,
          role_used VARCHAR(64),
          start_ms_in_output INT,
          end_ms_in_output INT,
          created_at DATETIME,
          KEY idx_output_segments_output (output_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS feishu_sync_records (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          sync_id VARCHAR(128) NOT NULL UNIQUE,
          object_type VARCHAR(128),
          object_id VARCHAR(128),
          feishu_table VARCHAR(128),
          feishu_record_id VARCHAR(128),
          sync_status VARCHAR(64),
          expire_at DATETIME,
          cleanup_status VARCHAR(64),
          created_at DATETIME,
          updated_at DATETIME
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ai_generation_jobs (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          job_id VARCHAR(128) NOT NULL UNIQUE,
          product_id VARCHAR(128) NOT NULL,
          market VARCHAR(64),
          category VARCHAR(128),
          segment_type VARCHAR(128),
          requested_count INT DEFAULT 0,
          generated_count INT DEFAULT 0,
          accepted_count INT DEFAULT 0,
          imported_segment_count INT DEFAULT 0,
          strict_pass_count INT DEFAULT 0,
          soft_pass_count INT DEFAULT 0,
          uncertain_count INT DEFAULT 0,
          fail_count INT DEFAULT 0,
          prompt_template_id VARCHAR(128),
          prompt_version VARCHAR(64),
          prompt_text TEXT,
          reference_asset_ids JSON,
          reference_image_oss_ids JSON,
          generation_type VARCHAR(128),
          model_name VARCHAR(128),
          status VARCHAR(64),
          failure_reason TEXT,
          feishu_record_id VARCHAR(128),
          scene_preference TEXT,
          style_preference TEXT,
          character_requirement TEXT,
          notes TEXT,
          created_at DATETIME,
          updated_at DATETIME
        )
        """,
    ]
    try:
        with ctx.repo.connect() as conn:
            with conn.cursor() as cur:
                for statement in statements:
                    cur.execute(statement)
        return Result.ok()
    except Exception as exc:
        return Result.fail("MYSQL_CORE_SCHEMA_FAILED", str(exc), {"db_provider": "mysql"})


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
