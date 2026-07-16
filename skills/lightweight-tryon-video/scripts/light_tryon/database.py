from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Sequence

from .models import PlannedJob, ProductInput
from .utils import json_dumps, json_loads, normalized_list, now_iso, read_json


TEMPLATE_TABLES = {
    "scene": "scene_templates",
    "action": "action_templates",
    "styling": "styling_templates",
    "subtitle": "subtitle_templates",
    "persona": "persona_templates",
    "duration": "duration_templates",
    "shot_plan": "shot_plan_templates",
}

ID_COLUMNS = {
    "scene_templates": "scene_id",
    "action_templates": "action_id",
    "styling_templates": "styling_id",
    "subtitle_templates": "subtitle_id",
    "persona_templates": "persona_id",
    "duration_templates": "duration_id",
    "shot_plan_templates": "shot_plan_id",
    "products_for_light_video": "product_id",
    "video_jobs": "job_id",
    "product_visual_plans": "visual_plan_id",
}

JSON_COLUMNS = {
    "scene_templates": {"required_anchors", "optional_anchors", "forbidden_elements", "applicable_categories", "reference_images", "background_type_pool", "edge_decor_pool", "source_payload"},
    "action_templates": {"applicable_categories", "action_steps", "applicable_scenes", "applicable_shot_profiles", "free_hand_action", "source_payload"},
    "styling_templates": {"applicable_top_type", "applicable_product_type", "product_fit", "bottom_color", "bottom_fit", "vibe_tag", "source_payload"},
    "subtitle_templates": {"applicable_category", "markets", "selling_point_angle", "source_payload"},
    "persona_templates": {"account_ids", "markets", "reference_images", "brand_logo_images", "fixed_accessories", "vibe", "source_payload"},
    "products_for_light_video": {
        "product_images",
        "core_selling_points",
        "recommended_scene_pool",
        "recommended_action_pool",
        "recommended_styling_pool",
        "subtitle_angle_pool",
    },
    "shot_plan_templates": {"applicable_categories", "single_sequence", "five_sequence", "fallback_cycle", "source_payload"},
    "video_jobs": {"prompt_payload", "qc_result", "template_versions", "template_snapshots", "raw_video_attachments", "final_video_attachments"},
    "job_attempts": {"request_payload", "response_payload"},
    "source_script_requests": {"job_ids", "visual_plan_ids", "source_payload"},
    "product_visual_plans": {"product_images", "outfit_image_attachments", "outfit_request_payload", "outfit_qc_result", "job_ids"},
    "visual_plan_attempts": {"request_payload", "response_payload"},
}

JSON_OBJECT_COLUMNS = {
    "prompt_payload", "qc_result", "request_payload", "response_payload", "template_versions",
    "template_snapshots", "source_payload", "outfit_request_payload", "outfit_qc_result",
}


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scene_templates (
    scene_id TEXT PRIMARY KEY,
    scene_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('enabled','disabled','testing')),
    priority INTEGER NOT NULL DEFAULT 0,
    usage_ratio INTEGER NOT NULL DEFAULT 0 CHECK (usage_ratio BETWEEN 0 AND 100),
    scene_type TEXT NOT NULL,
    room_type TEXT,
    wall_color TEXT,
    floor_type TEXT,
    floor_color TEXT,
    ceiling_type TEXT,
    ceiling_light_type TEXT,
    bed_position TEXT,
    bed_style TEXT,
    bed_sheet_color TEXT,
    curtain_position TEXT,
    curtain_color TEXT,
    shelf_position TEXT,
    shelf_style TEXT,
    scene_style TEXT NOT NULL DEFAULT '',
    background_type_pool TEXT NOT NULL DEFAULT '[]',
    background_cleanliness TEXT NOT NULL DEFAULT '',
    edge_decor_pool TEXT NOT NULL DEFAULT '[]',
    decor_count TEXT NOT NULL DEFAULT '',
    decor_position TEXT NOT NULL DEFAULT '',
    key_light_direction TEXT NOT NULL DEFAULT '',
    required_anchors TEXT NOT NULL DEFAULT '[]',
    optional_anchors TEXT NOT NULL DEFAULT '[]',
    forbidden_elements TEXT NOT NULL DEFAULT '[]',
    framing_type TEXT,
    shot_type TEXT NOT NULL,
    camera_angle TEXT,
    camera_height TEXT,
    subject_position TEXT,
    lighting_style TEXT,
    lighting_temp TEXT,
    movement_boundary TEXT,
    prompt_core TEXT NOT NULL,
    prompt_negative TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS action_templates (
    action_id TEXT PRIMARY KEY,
    action_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('enabled','disabled','testing')),
    applicable_categories TEXT NOT NULL DEFAULT '["*"]',
    action_type TEXT NOT NULL,
    action_steps TEXT NOT NULL DEFAULT '[]',
    duration_suggestion TEXT,
    movement_level TEXT NOT NULL,
    hand_usage TEXT,
    body_rotation TEXT,
    risk_level TEXT NOT NULL CHECK (risk_level IN ('low','medium','high')),
    prompt_core TEXT NOT NULL,
    prompt_negative TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shot_plan_templates (
    shot_plan_id TEXT PRIMARY KEY,
    shot_plan_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('enabled','disabled','testing')),
    priority INTEGER NOT NULL DEFAULT 0,
    applicable_categories TEXT NOT NULL DEFAULT '["*"]',
    single_sequence TEXT NOT NULL DEFAULT '[]',
    five_sequence TEXT NOT NULL DEFAULT '[]',
    fallback_cycle TEXT NOT NULL DEFAULT '[]',
    config_version TEXT NOT NULL DEFAULT 'V1',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS styling_templates (
    styling_id TEXT PRIMARY KEY,
    styling_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('enabled','disabled','testing')),
    applicable_top_type TEXT NOT NULL DEFAULT '[]',
    applicable_product_type TEXT NOT NULL DEFAULT '[]',
    top_fit TEXT,
    bottom_type TEXT,
    bottom_color TEXT,
    bottom_fit TEXT,
    inner_type TEXT NOT NULL DEFAULT '',
    inner_color TEXT NOT NULL DEFAULT '',
    inner_requirements TEXT NOT NULL DEFAULT '',
    accessory_level TEXT,
    footwear_visibility TEXT,
    vibe_tag TEXT,
    prompt_core TEXT NOT NULL,
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS subtitle_templates (
    subtitle_id TEXT PRIMARY KEY,
    subtitle_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('enabled','disabled','testing')),
    market TEXT NOT NULL,
    language TEXT NOT NULL,
    subtitle_style TEXT NOT NULL,
    applicable_category TEXT NOT NULL DEFAULT '["*"]',
    opening_text TEXT NOT NULL DEFAULT '',
    middle_text TEXT NOT NULL DEFAULT '',
    ending_text TEXT NOT NULL DEFAULT '',
    char_limit INTEGER NOT NULL DEFAULT 30,
    tone TEXT,
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS persona_templates (
    persona_id TEXT PRIMARY KEY,
    persona_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('enabled','disabled','testing')),
    gender TEXT,
    age_group TEXT,
    body_type TEXT,
    hair_style TEXT,
    hair_color TEXT,
    skin_tone TEXT,
    phone_style TEXT,
    phone_case_color TEXT,
    face_visibility TEXT,
    makeup_style TEXT,
    vibe TEXT,
    prompt_core TEXT NOT NULL,
    prompt_negative TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS duration_templates (
    duration_id TEXT PRIMARY KEY,
    seconds INTEGER NOT NULL CHECK (seconds BETWEEN 1 AND 60),
    status TEXT NOT NULL CHECK (status IN ('enabled','disabled','testing')),
    weight INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS products_for_light_video (
    product_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    market TEXT NOT NULL,
    language TEXT NOT NULL,
    category TEXT NOT NULL,
    sub_category TEXT NOT NULL DEFAULT '',
    product_title TEXT NOT NULL DEFAULT '',
    product_images TEXT NOT NULL DEFAULT '[]',
    core_selling_points TEXT NOT NULL DEFAULT '[]',
    recommended_scene_pool TEXT NOT NULL DEFAULT '[]',
    recommended_action_pool TEXT NOT NULL DEFAULT '[]',
    recommended_styling_pool TEXT NOT NULL DEFAULT '[]',
    subtitle_angle_pool TEXT NOT NULL DEFAULT '[]',
    target_publish_count INTEGER NOT NULL DEFAULT 4 CHECK (target_publish_count BETWEEN 1 AND 100),
    status TEXT NOT NULL DEFAULT 'ready',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS video_jobs (
    job_id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL REFERENCES products_for_light_video(product_id) ON DELETE RESTRICT,
    market TEXT NOT NULL,
    language TEXT NOT NULL,
    persona_id TEXT NOT NULL REFERENCES persona_templates(persona_id) ON DELETE RESTRICT,
    scene_id TEXT NOT NULL REFERENCES scene_templates(scene_id) ON DELETE RESTRICT,
    shot_profile_id TEXT NOT NULL DEFAULT 'SHOT_FULL_FIXED',
    action_id TEXT NOT NULL REFERENCES action_templates(action_id) ON DELETE RESTRICT,
    styling_id TEXT NOT NULL REFERENCES styling_templates(styling_id) ON DELETE RESTRICT,
    subtitle_id TEXT NOT NULL REFERENCES subtitle_templates(subtitle_id) ON DELETE RESTRICT,
    duration_seconds INTEGER NOT NULL CHECK (duration_seconds BETWEEN 8 AND 10),
    variant_no INTEGER NOT NULL CHECK (variant_no >= 1),
    publish_priority INTEGER NOT NULL DEFAULT 0,
    generation_status TEXT NOT NULL DEFAULT 'pending' CHECK (generation_status IN ('pending','generating','success','failed','retrying')),
    qc_status TEXT NOT NULL DEFAULT 'pending' CHECK (qc_status IN ('pending','passed','failed','manual_review')),
    output_video_path TEXT NOT NULL DEFAULT '',
    raw_video_path TEXT NOT NULL DEFAULT '',
    output_cover_path TEXT NOT NULL DEFAULT '',
    prompt_payload TEXT NOT NULL DEFAULT '{}',
    prompt_version TEXT NOT NULL DEFAULT 'unbuilt',
    qc_result TEXT NOT NULL DEFAULT '{}',
    plan_version TEXT NOT NULL DEFAULT 'v1',
    visual_plan_id TEXT NOT NULL DEFAULT '',
    outfit_image_path TEXT NOT NULL DEFAULT '',
    outfit_image_url TEXT NOT NULL DEFAULT '',
    outfit_image_version TEXT NOT NULL DEFAULT '',
    legacy_job INTEGER NOT NULL DEFAULT 1,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(product_id, plan_version, variant_no)
);

CREATE TABLE IF NOT EXISTS job_attempts (
    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES video_jobs(job_id) ON DELETE CASCADE,
    attempt_no INTEGER NOT NULL,
    provider TEXT NOT NULL,
    status TEXT NOT NULL,
    request_payload TEXT NOT NULL DEFAULT '{}',
    response_payload TEXT NOT NULL DEFAULT '{}',
    error_message TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE(job_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS feishu_sync_runs (
    batch_id TEXT PRIMARY KEY,
    sync_type TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    created_count INTEGER NOT NULL DEFAULT 0,
    updated_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS feishu_sync_items (
    item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES feishu_sync_runs(batch_id) ON DELETE CASCADE,
    table_role TEXT NOT NULL,
    feishu_record_id TEXT NOT NULL DEFAULT '',
    business_id TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL,
    status TEXT NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS manual_review_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES video_jobs(job_id) ON DELETE CASCADE,
    review_fingerprint TEXT NOT NULL,
    feishu_record_id TEXT NOT NULL DEFAULT '',
    manual_review_status TEXT NOT NULL DEFAULT '',
    need_regeneration TEXT NOT NULL DEFAULT '',
    regeneration_strategy TEXT NOT NULL DEFAULT '',
    regeneration_job_id TEXT NOT NULL DEFAULT '',
    processed_at TEXT NOT NULL,
    UNIQUE(job_id, review_fingerprint)
);

CREATE TABLE IF NOT EXISTS source_script_requests (
    source_record_id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL DEFAULT '',
    source_product_code TEXT NOT NULL DEFAULT '',
    requested_count INTEGER NOT NULL DEFAULT 0,
    config_version TEXT NOT NULL DEFAULT 'V1',
    source_hash TEXT NOT NULL DEFAULT '',
    source_payload TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT '',
    job_ids TEXT NOT NULL DEFAULT '[]',
    visual_plan_ids TEXT NOT NULL DEFAULT '[]',
    last_processed_at TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS product_visual_plans (
    visual_plan_id TEXT PRIMARY KEY,
    source_record_id TEXT NOT NULL DEFAULT '',
    product_id TEXT NOT NULL REFERENCES products_for_light_video(product_id) ON DELETE RESTRICT,
    product_code TEXT NOT NULL DEFAULT '',
    product_name TEXT NOT NULL DEFAULT '',
    product_images TEXT NOT NULL DEFAULT '[]',
    scene_id TEXT NOT NULL REFERENCES scene_templates(scene_id) ON DELETE RESTRICT,
    scene_name TEXT NOT NULL DEFAULT '',
    styling_id TEXT NOT NULL REFERENCES styling_templates(styling_id) ON DELETE RESTRICT,
    styling_name TEXT NOT NULL DEFAULT '',
    resolved_bottom_color TEXT NOT NULL DEFAULT '',
    resolved_bottom_fit TEXT NOT NULL DEFAULT '',
    resolved_inner_type TEXT NOT NULL DEFAULT '',
    resolved_inner_color TEXT NOT NULL DEFAULT '',
    resolved_outerwear_state TEXT NOT NULL DEFAULT '',
    resolved_background_type TEXT NOT NULL DEFAULT '',
    resolved_edge_decor TEXT NOT NULL DEFAULT '',
    resolved_key_light_direction TEXT NOT NULL DEFAULT '',
    persona_id TEXT NOT NULL REFERENCES persona_templates(persona_id) ON DELETE RESTRICT,
    per_plan_video_count INTEGER NOT NULL DEFAULT 1 CHECK (per_plan_video_count IN (0,1,5)),
    plan_version INTEGER NOT NULL DEFAULT 1,
    plan_fingerprint TEXT NOT NULL,
    plan_status TEXT NOT NULL DEFAULT 'active' CHECK (plan_status IN ('active','superseded','disabled','failed')),
    outfit_image_status TEXT NOT NULL DEFAULT 'pending' CHECK (outfit_image_status IN ('pending','generating','pending_review','confirmed','regenerate','failed')),
    outfit_image_path TEXT NOT NULL DEFAULT '',
    outfit_image_url TEXT NOT NULL DEFAULT '',
    outfit_image_attachments TEXT NOT NULL DEFAULT '[]',
    outfit_image_version TEXT NOT NULL DEFAULT '',
    outfit_request_payload TEXT NOT NULL DEFAULT '{}',
    outfit_qc_result TEXT NOT NULL DEFAULT '{}',
    operator_feedback TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    job_ids TEXT NOT NULL DEFAULT '[]',
    feishu_record_id TEXT NOT NULL DEFAULT '',
    sync_status TEXT NOT NULL DEFAULT '',
    last_synced_at TEXT NOT NULL DEFAULT '',
    sync_error TEXT NOT NULL DEFAULT '',
    confirmed_at TEXT NOT NULL DEFAULT '',
    superseded_at TEXT NOT NULL DEFAULT '',
    review_source_hash TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS visual_plan_attempts (
    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    visual_plan_id TEXT NOT NULL REFERENCES product_visual_plans(visual_plan_id) ON DELETE CASCADE,
    attempt_no INTEGER NOT NULL,
    provider TEXT NOT NULL,
    status TEXT NOT NULL,
    request_payload TEXT NOT NULL DEFAULT '{}',
    response_payload TEXT NOT NULL DEFAULT '{}',
    error_message TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE(visual_plan_id, attempt_no)
);

CREATE INDEX IF NOT EXISTS idx_video_jobs_generation ON video_jobs(generation_status, publish_priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_video_jobs_qc ON video_jobs(qc_status, updated_at);
CREATE INDEX IF NOT EXISTS idx_video_jobs_product ON video_jobs(product_id, variant_no);
CREATE INDEX IF NOT EXISTS idx_job_attempts_job ON job_attempts(job_id, attempt_no);
CREATE INDEX IF NOT EXISTS idx_feishu_sync_items_batch ON feishu_sync_items(batch_id, status);
CREATE INDEX IF NOT EXISTS idx_manual_review_events_job ON manual_review_events(job_id, processed_at);
CREATE INDEX IF NOT EXISTS idx_source_script_requests_product ON source_script_requests(product_id, updated_at);
CREATE INDEX IF NOT EXISTS idx_visual_plans_source ON product_visual_plans(source_record_id, plan_status, updated_at);
CREATE INDEX IF NOT EXISTS idx_visual_plans_product ON product_visual_plans(product_id, plan_status, updated_at);
CREATE INDEX IF NOT EXISTS idx_visual_plan_attempts ON visual_plan_attempts(visual_plan_id, attempt_no);
"""


MIGRATION_COLUMNS: dict[str, dict[str, str]] = {
    "shot_plan_templates": {
        "feishu_record_id": "TEXT NOT NULL DEFAULT ''",
        "feishu_updated_at": "TEXT NOT NULL DEFAULT ''",
        "sync_status": "TEXT NOT NULL DEFAULT ''",
        "last_synced_at": "TEXT NOT NULL DEFAULT ''",
        "sync_error": "TEXT NOT NULL DEFAULT ''",
        "source_hash": "TEXT NOT NULL DEFAULT ''",
        "source_payload": "TEXT NOT NULL DEFAULT '{}'",
    },
    "scene_templates": {
        "camera_motion": "TEXT NOT NULL DEFAULT 'fixed'",
        "applicable_categories": "TEXT NOT NULL DEFAULT '[\"*\"]'",
        "reference_images": "TEXT NOT NULL DEFAULT '[]'",
        "aspect_ratio": "TEXT NOT NULL DEFAULT '9:16'",
        "subject_scale": "TEXT NOT NULL DEFAULT ''",
        "lighting_level": "TEXT NOT NULL DEFAULT ''",
        "lighting_tone": "TEXT NOT NULL DEFAULT ''",
        "consistency_prompt": "TEXT NOT NULL DEFAULT ''",
        "scene_style": "TEXT NOT NULL DEFAULT ''",
        "background_type_pool": "TEXT NOT NULL DEFAULT '[]'",
        "background_cleanliness": "TEXT NOT NULL DEFAULT ''",
        "edge_decor_pool": "TEXT NOT NULL DEFAULT '[]'",
        "decor_count": "TEXT NOT NULL DEFAULT ''",
        "decor_position": "TEXT NOT NULL DEFAULT ''",
        "key_light_direction": "TEXT NOT NULL DEFAULT ''",
        "config_version": "TEXT NOT NULL DEFAULT 'V1'",
        "feishu_record_id": "TEXT NOT NULL DEFAULT ''",
        "feishu_updated_at": "TEXT NOT NULL DEFAULT ''",
        "sync_status": "TEXT NOT NULL DEFAULT ''",
        "last_synced_at": "TEXT NOT NULL DEFAULT ''",
        "sync_error": "TEXT NOT NULL DEFAULT ''",
        "source_hash": "TEXT NOT NULL DEFAULT ''",
        "source_payload": "TEXT NOT NULL DEFAULT '{}'",
    },
    "action_templates": {
        "priority": "INTEGER NOT NULL DEFAULT 0",
        "applicable_scenes": "TEXT NOT NULL DEFAULT '[]'",
        "applicable_shot_profiles": "TEXT NOT NULL DEFAULT '[]'",
        "free_hand_action": "TEXT NOT NULL DEFAULT '[]'",
        "forward_movement": "TEXT NOT NULL DEFAULT ''",
        "movement_speed": "TEXT NOT NULL DEFAULT ''",
        "config_version": "TEXT NOT NULL DEFAULT 'V1'",
        "feishu_record_id": "TEXT NOT NULL DEFAULT ''",
        "feishu_updated_at": "TEXT NOT NULL DEFAULT ''",
        "sync_status": "TEXT NOT NULL DEFAULT ''",
        "last_synced_at": "TEXT NOT NULL DEFAULT ''",
        "sync_error": "TEXT NOT NULL DEFAULT ''",
        "source_hash": "TEXT NOT NULL DEFAULT ''",
        "source_payload": "TEXT NOT NULL DEFAULT '{}'",
    },
    "styling_templates": {
        "priority": "INTEGER NOT NULL DEFAULT 0",
        "product_fit": "TEXT NOT NULL DEFAULT '[]'",
        "inner_type": "TEXT NOT NULL DEFAULT ''",
        "inner_color": "TEXT NOT NULL DEFAULT ''",
        "inner_requirements": "TEXT NOT NULL DEFAULT ''",
        "suitable_color_rules": "TEXT NOT NULL DEFAULT ''",
        "forbidden_pairings": "TEXT NOT NULL DEFAULT ''",
        "config_version": "TEXT NOT NULL DEFAULT 'V1'",
        "feishu_record_id": "TEXT NOT NULL DEFAULT ''",
        "feishu_updated_at": "TEXT NOT NULL DEFAULT ''",
        "sync_status": "TEXT NOT NULL DEFAULT ''",
        "last_synced_at": "TEXT NOT NULL DEFAULT ''",
        "sync_error": "TEXT NOT NULL DEFAULT ''",
        "source_hash": "TEXT NOT NULL DEFAULT ''",
        "source_payload": "TEXT NOT NULL DEFAULT '{}'",
    },
    "subtitle_templates": {
        "priority": "INTEGER NOT NULL DEFAULT 0",
        "markets": "TEXT NOT NULL DEFAULT '[]'",
        "selling_point_angle": "TEXT NOT NULL DEFAULT '[]'",
        "subtitle_type": "TEXT NOT NULL DEFAULT ''",
        "display_mode": "TEXT NOT NULL DEFAULT '开场加结尾'",
        "allow_ai_rewrite": "TEXT NOT NULL DEFAULT '不允许'",
        "need_chinese_translation": "TEXT NOT NULL DEFAULT '不需要'",
        "config_version": "TEXT NOT NULL DEFAULT 'V1'",
        "feishu_record_id": "TEXT NOT NULL DEFAULT ''",
        "feishu_updated_at": "TEXT NOT NULL DEFAULT ''",
        "sync_status": "TEXT NOT NULL DEFAULT ''",
        "last_synced_at": "TEXT NOT NULL DEFAULT ''",
        "sync_error": "TEXT NOT NULL DEFAULT ''",
        "source_hash": "TEXT NOT NULL DEFAULT ''",
        "source_payload": "TEXT NOT NULL DEFAULT '{}'",
    },
    "persona_templates": {
        "priority": "INTEGER NOT NULL DEFAULT 0",
        "account_ids": "TEXT NOT NULL DEFAULT '[]'",
        "markets": "TEXT NOT NULL DEFAULT '[]'",
        "reference_images": "TEXT NOT NULL DEFAULT '[]'",
        "brand_overlay_enabled": "TEXT NOT NULL DEFAULT 'disabled'",
        "brand_logo_images": "TEXT NOT NULL DEFAULT '[]'",
        "brand_display_name": "TEXT NOT NULL DEFAULT ''",
        "brand_style_preset": "TEXT NOT NULL DEFAULT 'cream_serif'",
        "brand_primary_color": "TEXT NOT NULL DEFAULT 'cream_white'",
        "brand_default_series_title": "TEXT NOT NULL DEFAULT ''",
        "height_impression": "TEXT NOT NULL DEFAULT ''",
        "fixed_accessories": "TEXT NOT NULL DEFAULT '[]'",
        "default_scene_id": "TEXT NOT NULL DEFAULT ''",
        "consistency_version": "TEXT NOT NULL DEFAULT 'PERSONA_V1'",
        "config_version": "TEXT NOT NULL DEFAULT 'V1'",
        "feishu_record_id": "TEXT NOT NULL DEFAULT ''",
        "feishu_updated_at": "TEXT NOT NULL DEFAULT ''",
        "sync_status": "TEXT NOT NULL DEFAULT ''",
        "last_synced_at": "TEXT NOT NULL DEFAULT ''",
        "sync_error": "TEXT NOT NULL DEFAULT ''",
        "source_hash": "TEXT NOT NULL DEFAULT ''",
        "source_payload": "TEXT NOT NULL DEFAULT '{}'",
    },
    "products_for_light_video": {
        "enable_light_video": "INTEGER NOT NULL DEFAULT 1",
        "default_persona_id": "TEXT NOT NULL DEFAULT ''",
        "account_id": "TEXT NOT NULL DEFAULT ''",
        "generation_priority": "TEXT NOT NULL DEFAULT 'medium'",
        "light_video_status": "TEXT NOT NULL DEFAULT 'pending'",
        "light_video_notes": "TEXT NOT NULL DEFAULT ''",
        "source_script_record_id": "TEXT NOT NULL DEFAULT ''",
        "source_product_code": "TEXT NOT NULL DEFAULT ''",
        "shot_plan_id": "TEXT NOT NULL DEFAULT ''",
    },
    "video_jobs": {
        "shot_profile_id": "TEXT NOT NULL DEFAULT 'SHOT_FULL_FIXED'",
        "shot_plan_id": "TEXT NOT NULL DEFAULT ''",
        "account_id": "TEXT NOT NULL DEFAULT ''",
        "template_versions": "TEXT NOT NULL DEFAULT '{}'",
        "template_snapshots": "TEXT NOT NULL DEFAULT '{}'",
        "parent_job_id": "TEXT NOT NULL DEFAULT ''",
        "manual_review_status": "TEXT NOT NULL DEFAULT 'pending'",
        "manual_review_reason": "TEXT NOT NULL DEFAULT ''",
        "need_regeneration": "TEXT NOT NULL DEFAULT 'no'",
        "regeneration_strategy": "TEXT NOT NULL DEFAULT ''",
        "publish_status": "TEXT NOT NULL DEFAULT 'unscheduled'",
        "published_at": "TEXT NOT NULL DEFAULT ''",
        "publish_url": "TEXT NOT NULL DEFAULT ''",
        "operator_notes": "TEXT NOT NULL DEFAULT ''",
        "review_version": "INTEGER NOT NULL DEFAULT 0",
        "review_processed_at": "TEXT NOT NULL DEFAULT ''",
        "feishu_review_record_id": "TEXT NOT NULL DEFAULT ''",
        "review_sync_status": "TEXT NOT NULL DEFAULT ''",
        "review_last_synced_at": "TEXT NOT NULL DEFAULT ''",
        "review_sync_error": "TEXT NOT NULL DEFAULT ''",
        "output_video_url": "TEXT NOT NULL DEFAULT ''",
        "output_cover_url": "TEXT NOT NULL DEFAULT ''",
        "views": "INTEGER NOT NULL DEFAULT 0",
        "product_clicks": "INTEGER NOT NULL DEFAULT 0",
        "gmv": "REAL NOT NULL DEFAULT 0",
        "metrics_updated_at": "TEXT NOT NULL DEFAULT ''",
        "review_source_hash": "TEXT NOT NULL DEFAULT ''",
        "source_script_record_id": "TEXT NOT NULL DEFAULT ''",
        "visual_plan_id": "TEXT NOT NULL DEFAULT ''",
        "outfit_image_path": "TEXT NOT NULL DEFAULT ''",
        "outfit_image_url": "TEXT NOT NULL DEFAULT ''",
        "outfit_image_version": "TEXT NOT NULL DEFAULT ''",
        "legacy_job": "INTEGER NOT NULL DEFAULT 1",
        "raw_video_attachments": "TEXT NOT NULL DEFAULT '[]'",
        "final_video_attachments": "TEXT NOT NULL DEFAULT '[]'",
        "review_video_source_hash": "TEXT NOT NULL DEFAULT ''",
        "review_video_process_status": "TEXT NOT NULL DEFAULT ''",
        "review_video_process_error": "TEXT NOT NULL DEFAULT ''",
        "review_video_processed_at": "TEXT NOT NULL DEFAULT ''",
        "generation_channel": "TEXT NOT NULL DEFAULT 'no_generate'",
        "generation_model": "TEXT NOT NULL DEFAULT 'Seedance 2.0'",
        "generation_rerun": "INTEGER NOT NULL DEFAULT 0",
        "run_manager_record_id": "TEXT NOT NULL DEFAULT ''",
        "run_manager_sync_status": "TEXT NOT NULL DEFAULT 'not_submitted'",
        "run_manager_sync_error": "TEXT NOT NULL DEFAULT ''",
        "run_manager_last_synced_at": "TEXT NOT NULL DEFAULT ''",
        "run_manager_trace_id": "TEXT NOT NULL DEFAULT ''",
        "run_manager_result_status": "TEXT NOT NULL DEFAULT ''",
        "run_manager_source_hash": "TEXT NOT NULL DEFAULT ''",
    },
    "source_script_requests": {
        "source_product_code": "TEXT NOT NULL DEFAULT ''",
        "visual_plan_ids": "TEXT NOT NULL DEFAULT '[]'",
    },
    "product_visual_plans": {
        "review_source_hash": "TEXT NOT NULL DEFAULT ''",
        "outfit_image_attachments": "TEXT NOT NULL DEFAULT '[]'",
        "resolved_bottom_color": "TEXT NOT NULL DEFAULT ''",
        "resolved_bottom_fit": "TEXT NOT NULL DEFAULT ''",
        "resolved_inner_type": "TEXT NOT NULL DEFAULT ''",
        "resolved_inner_color": "TEXT NOT NULL DEFAULT ''",
        "resolved_outerwear_state": "TEXT NOT NULL DEFAULT ''",
        "resolved_background_type": "TEXT NOT NULL DEFAULT ''",
        "resolved_edge_decor": "TEXT NOT NULL DEFAULT ''",
        "resolved_key_light_direction": "TEXT NOT NULL DEFAULT ''",
    },
}


class LightTryonDB:
    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_schema(self) -> None:
        with self.connection() as conn:
            conn.executescript(SCHEMA_SQL)
            for table, columns in MIGRATION_COLUMNS.items():
                existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
                for name, definition in columns.items():
                    if name not in existing:
                        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_jobs_visual_plan ON video_jobs(visual_plan_id, variant_no)")
            conn.execute(
                "UPDATE video_jobs SET shot_profile_id='SHOT_UPPER_FIXED' "
                "WHERE scene_id IN ('SCENE_B_001','SCENE_D_001') AND shot_profile_id='SHOT_FULL_FIXED'"
            )
            conn.execute(
                "UPDATE video_jobs SET shot_profile_id='SHOT_UPPER_PUSH_IN' "
                "WHERE scene_id='SCENE_E_001' AND shot_profile_id='SHOT_FULL_FIXED'"
            )
            conn.execute(
                "INSERT INTO schema_meta(key, value) VALUES('schema_version','2.2.0') "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
            )

    def _columns(self, conn: sqlite3.Connection, table: str) -> set[str]:
        if table not in ID_COLUMNS and table != "job_attempts":
            raise ValueError(f"不支持的数据表: {table}")
        return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}

    def _encode(self, table: str, data: dict[str, Any]) -> dict[str, Any]:
        encoded = data.copy()
        for column in JSON_COLUMNS.get(table, set()):
            if column not in encoded:
                continue
            value = encoded[column]
            if isinstance(value, str):
                parsed = json_loads(value, None)
                if column in JSON_OBJECT_COLUMNS:
                    value = parsed if isinstance(parsed, dict) else {}
                else:
                    value = parsed if isinstance(parsed, list) else normalized_list(value)
            encoded[column] = json_dumps(value)
        return encoded

    def _decode_row(self, table: str, row: sqlite3.Row | dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        decoded = dict(row)
        for column in JSON_COLUMNS.get(table, set()):
            if column in decoded:
                raw = decoded[column]
                parsed = json_loads(raw, None)
                if column in JSON_OBJECT_COLUMNS:
                    decoded[column] = parsed if isinstance(parsed, dict) else {}
                else:
                    decoded[column] = parsed if isinstance(parsed, list) else normalized_list(raw)
        return decoded

    def _upsert(self, table: str, data: dict[str, Any], *, conn: sqlite3.Connection | None = None) -> None:
        owns_conn = conn is None
        target = conn or self.connect()
        try:
            columns = self._columns(target, table)
            payload = self._encode(table, {key: value for key, value in data.items() if key in columns})
            now = now_iso()
            if "created_at" in columns:
                payload.setdefault("created_at", now)
            if "updated_at" in columns:
                payload["updated_at"] = now
            id_column = ID_COLUMNS[table]
            if not payload.get(id_column):
                raise ValueError(f"{table} 缺少主键 {id_column}")
            names = list(payload)
            placeholders = ",".join("?" for _ in names)
            updates = [name for name in names if name not in {id_column, "created_at"}]
            update_sql = ",".join(f"{name}=excluded.{name}" for name in updates)
            target.execute(
                f"INSERT INTO {table} ({','.join(names)}) VALUES ({placeholders}) "
                f"ON CONFLICT({id_column}) DO UPDATE SET {update_sql}",
                [payload[name] for name in names],
            )
            if owns_conn:
                target.commit()
        except Exception:
            if owns_conn:
                target.rollback()
            raise
        finally:
            if owns_conn:
                target.close()

    def seed_templates(self, template_path: str | Path) -> dict[str, int]:
        payload = read_json(template_path)
        mapping = {
            "scenes": "scene_templates",
            "actions": "action_templates",
            "stylings": "styling_templates",
            "subtitles": "subtitle_templates",
            "personas": "persona_templates",
            "durations": "duration_templates",
            "shot_plans": "shot_plan_templates",
        }
        counts: dict[str, int] = {}
        with self.connection() as conn:
            for source, table in mapping.items():
                rows = payload.get(source, [])
                for row in rows:
                    self._upsert(table, row, conn=conn)
                counts[source] = len(rows)
            conn.execute(
                "INSERT INTO schema_meta(key, value) VALUES('template_version',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(payload.get("template_version") or "unknown"),),
            )
        return counts

    def list_templates(self, kind: str, status: str | None = None) -> list[dict[str, Any]]:
        table = TEMPLATE_TABLES.get(kind)
        if not table:
            raise ValueError(f"模板类型必须是: {', '.join(TEMPLATE_TABLES)}")
        query = f"SELECT * FROM {table}"
        params: list[Any] = []
        if status:
            query += " WHERE status=?"
            params.append(status)
        priority_column = "priority" if kind in {"scene", "shot_plan"} else ID_COLUMNS[table]
        query += f" ORDER BY {priority_column} DESC" if kind in {"scene", "shot_plan"} else f" ORDER BY {priority_column}"
        with self.connection() as conn:
            return [self._decode_row(table, row) or {} for row in conn.execute(query, params)]

    def get_template(self, kind: str, template_id: str) -> dict[str, Any] | None:
        table = TEMPLATE_TABLES.get(kind)
        if not table:
            raise ValueError(f"未知模板类型: {kind}")
        id_column = ID_COLUMNS[table]
        with self.connection() as conn:
            return self._decode_row(table, conn.execute(f"SELECT * FROM {table} WHERE {id_column}=?", (template_id,)).fetchone())

    def upsert_template(self, kind: str, data: dict[str, Any]) -> None:
        table = TEMPLATE_TABLES.get(kind)
        if not table:
            raise ValueError(f"未知模板类型: {kind}")
        self._upsert(table, data)

    def set_template_status(self, kind: str, template_id: str, status: str) -> None:
        if status not in {"enabled", "disabled", "testing"}:
            raise ValueError("模板状态必须是 enabled / disabled / testing")
        table = TEMPLATE_TABLES.get(kind)
        if not table:
            raise ValueError(f"未知模板类型: {kind}")
        id_column = ID_COLUMNS[table]
        with self.connection() as conn:
            cursor = conn.execute(
                f"UPDATE {table} SET status=?, updated_at=? WHERE {id_column}=?",
                (status, now_iso(), template_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"找不到模板: {template_id}")

    def delete_template(self, kind: str, template_id: str) -> None:
        table = TEMPLATE_TABLES.get(kind)
        if not table:
            raise ValueError(f"未知模板类型: {kind}")
        id_column = ID_COLUMNS[table]
        with self.connection() as conn:
            cursor = conn.execute(f"DELETE FROM {table} WHERE {id_column}=?", (template_id,))
            if cursor.rowcount != 1:
                raise KeyError(f"找不到模板: {template_id}")

    def upsert_product(self, product: ProductInput | dict[str, Any]) -> dict[str, Any]:
        normalized = product if isinstance(product, ProductInput) else ProductInput.from_dict(product)
        self._upsert("products_for_light_video", normalized.to_dict())
        return self.get_product(normalized.product_id) or {}

    def get_product(self, product_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM products_for_light_video WHERE product_id=?", (product_id,)).fetchone()
            return self._decode_row("products_for_light_video", row)

    def list_products(self, status: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM products_for_light_video"
        params: list[Any] = []
        if status:
            query += " WHERE status=?"
            params.append(status)
        query += " ORDER BY updated_at DESC, product_id"
        with self.connection() as conn:
            return [self._decode_row("products_for_light_video", row) or {} for row in conn.execute(query, params)]

    def create_jobs(self, jobs: Sequence[PlannedJob]) -> dict[str, int]:
        created = 0
        existing = 0
        now = now_iso()
        with self.connection() as conn:
            for job in jobs:
                data = job.to_dict()
                columns = list(data) + ["created_at", "updated_at"]
                values = [data[name] for name in data] + [now, now]
                cursor = conn.execute(
                    f"INSERT OR IGNORE INTO video_jobs ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                    values,
                )
                if cursor.rowcount:
                    created += 1
                else:
                    existing += 1
        return {"created": created, "existing": existing}

    def upsert_visual_plan(self, data: dict[str, Any]) -> dict[str, Any]:
        existing = self.get_visual_plan(str(data.get("visual_plan_id") or ""))
        payload = {**(existing or {}), **data}
        self._upsert("product_visual_plans", payload)
        return self.get_visual_plan(str(payload["visual_plan_id"])) or {}

    def get_visual_plan(self, visual_plan_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM product_visual_plans WHERE visual_plan_id=?",
                (visual_plan_id,),
            ).fetchone()
            return self._decode_row("product_visual_plans", row)

    def list_visual_plans(
        self,
        *,
        source_record_id: str | None = None,
        product_id: str | None = None,
        plan_status: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        for column, value in (
            ("source_record_id", source_record_id),
            ("product_id", product_id),
            ("plan_status", plan_status),
        ):
            if value:
                clauses.append(f"{column}=?")
                params.append(value)
        query = "SELECT * FROM product_visual_plans"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at, visual_plan_id"
        with self.connection() as conn:
            return [self._decode_row("product_visual_plans", row) or {} for row in conn.execute(query, params)]

    def supersede_visual_plans(self, source_record_id: str, keep_ids: Sequence[str]) -> int:
        keep = [str(item) for item in keep_ids if str(item)]
        params: list[Any] = [now_iso(), now_iso(), source_record_id]
        exclusion = ""
        if keep:
            exclusion = f" AND visual_plan_id NOT IN ({','.join('?' for _ in keep)})"
            params.extend(keep)
        with self.connection() as conn:
            cursor = conn.execute(
                "UPDATE product_visual_plans SET plan_status='superseded', superseded_at=?, updated_at=? "
                "WHERE source_record_id=? AND plan_status='active'" + exclusion,
                params,
            )
            return int(cursor.rowcount)

    def set_visual_plan_outfit(
        self,
        visual_plan_id: str,
        *,
        status: str,
        image_path: str = "",
        image_url: str = "",
        image_version: str = "",
        image_attachments: list[dict[str, Any]] | None = None,
        feedback: str = "",
        error: str = "",
        qc_result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        allowed = {"pending", "generating", "pending_review", "confirmed", "regenerate", "failed"}
        if status not in allowed:
            raise ValueError(f"非法穿搭图状态: {status}")
        current = self.get_visual_plan(visual_plan_id)
        if not current:
            raise KeyError(f"找不到视觉方案: {visual_plan_id}")
        if status == "confirmed" and not (image_path or image_url or current.get("outfit_image_path") or current.get("outfit_image_url")):
            raise ValueError("确认穿搭图前必须先写入图片路径或 URL")
        confirmed_at = now_iso() if status == "confirmed" else str(current.get("confirmed_at") or "")
        with self.connection() as conn:
            conn.execute(
                "UPDATE product_visual_plans SET outfit_image_status=?, outfit_image_path=?, outfit_image_url=?, outfit_image_attachments=?, "
                "outfit_image_version=?, operator_feedback=?, error_message=?, outfit_qc_result=?, confirmed_at=?, updated_at=? "
                "WHERE visual_plan_id=?",
                (
                    status,
                    image_path or current.get("outfit_image_path") or "",
                    image_url or current.get("outfit_image_url") or "",
                    json_dumps(image_attachments if image_attachments is not None else current.get("outfit_image_attachments") or []),
                    image_version or current.get("outfit_image_version") or "",
                    feedback,
                    str(error)[:2000],
                    json_dumps(qc_result or current.get("outfit_qc_result") or {}),
                    confirmed_at,
                    now_iso(),
                    visual_plan_id,
                ),
            )
        return self.get_visual_plan(visual_plan_id) or {}

    def record_visual_plan_attempt(
        self,
        visual_plan_id: str,
        *,
        provider: str,
        status: str,
        request_payload: dict[str, Any],
        response_payload: dict[str, Any] | None = None,
        error: str = "",
    ) -> int:
        with self.connection() as conn:
            attempt_no = int(conn.execute(
                "SELECT COALESCE(MAX(attempt_no),0)+1 FROM visual_plan_attempts WHERE visual_plan_id=?",
                (visual_plan_id,),
            ).fetchone()[0])
            conn.execute(
                "INSERT INTO visual_plan_attempts(visual_plan_id,attempt_no,provider,status,request_payload,response_payload,error_message,started_at,finished_at) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    visual_plan_id, attempt_no, provider, status, json_dumps(request_payload),
                    json_dumps(response_payload or {}), str(error)[:2000], now_iso(),
                    now_iso() if status in {"success", "failed"} else None,
                ),
            )
            return attempt_no

    def list_jobs(
        self,
        *,
        product_id: str | None = None,
        generation_status: str | None = None,
        qc_status: str | None = None,
        source_script_record_id: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if product_id:
            clauses.append("product_id=?")
            params.append(product_id)
        if generation_status:
            clauses.append("generation_status=?")
            params.append(generation_status)
        if qc_status:
            clauses.append("qc_status=?")
            params.append(qc_status)
        if source_script_record_id:
            clauses.append("source_script_record_id=?")
            params.append(source_script_record_id)
        query = "SELECT * FROM video_jobs"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY publish_priority DESC, created_at, variant_no"
        if limit:
            query += " LIMIT ?"
            params.append(int(limit))
        with self.connection() as conn:
            return [self._decode_row("video_jobs", row) or {} for row in conn.execute(query, params)]

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM video_jobs WHERE job_id=?", (job_id,)).fetchone()
            return self._decode_row("video_jobs", row)

    def get_source_request(self, source_record_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM source_script_requests WHERE source_record_id=?",
                (source_record_id,),
            ).fetchone()
            return self._decode_row("source_script_requests", row)

    def upsert_source_request(self, data: dict[str, Any]) -> None:
        now = now_iso()
        payload = data.copy()
        payload.setdefault("created_at", now)
        payload["updated_at"] = now
        for name in JSON_COLUMNS["source_script_requests"]:
            if name in payload and not isinstance(payload[name], str):
                payload[name] = json_dumps(payload[name])
        columns = {
            "source_record_id", "product_id", "source_product_code", "requested_count", "config_version", "source_hash",
            "source_payload", "status", "job_ids", "visual_plan_ids", "last_processed_at", "error_message", "created_at", "updated_at",
        }
        payload = {key: value for key, value in payload.items() if key in columns}
        if not payload.get("source_record_id"):
            raise ValueError("source_script_requests 缺少 source_record_id")
        names = list(payload)
        updates = [name for name in names if name not in {"source_record_id", "created_at"}]
        with self.connection() as conn:
            conn.execute(
                f"INSERT INTO source_script_requests ({','.join(names)}) VALUES ({','.join('?' for _ in names)}) "
                f"ON CONFLICT(source_record_id) DO UPDATE SET {','.join(f'{name}=excluded.{name}' for name in updates)}",
                [payload[name] for name in names],
            )

    def get_job_context(self, job_id: str) -> dict[str, Any]:
        job = self.get_job(job_id)
        if not job:
            raise KeyError(f"找不到任务: {job_id}")
        product = self.get_product(job["product_id"])
        if not product:
            raise KeyError(f"任务 {job_id} 的商品不存在: {job['product_id']}")
        context = {"job": job, "product": product}
        visual_plan_id = str(job.get("visual_plan_id") or "")
        if visual_plan_id:
            visual_plan = self.get_visual_plan(visual_plan_id)
            if not visual_plan:
                raise KeyError(f"任务 {job_id} 的视觉方案不存在: {visual_plan_id}")
            if visual_plan.get("outfit_image_status") != "confirmed":
                raise ValueError(f"任务 {job_id} 的视觉方案穿搭图尚未确认")
            context["visual_plan"] = visual_plan
        for key, kind in (("scene", "scene"), ("action", "action"), ("styling", "styling"), ("subtitle", "subtitle"), ("persona", "persona")):
            template = self.get_template(kind, job[f"{key}_id"])
            if not template:
                raise KeyError(f"任务 {job_id} 缺少 {kind} 模板")
            context[key] = template
        shot_plan_id = str(job.get("shot_plan_id") or "")
        if shot_plan_id:
            shot_plan = self.get_template("shot_plan", shot_plan_id)
            if not shot_plan:
                raise KeyError(f"任务 {job_id} 缺少镜头方案模板: {shot_plan_id}")
            context["shot_plan"] = shot_plan
        return context

    def update_prompt(self, job_id: str, prompt_payload: dict[str, Any], prompt_version: str) -> None:
        with self.connection() as conn:
            cursor = conn.execute(
                "UPDATE video_jobs SET prompt_payload=?, prompt_version=?, template_versions=?, template_snapshots=?, updated_at=? WHERE job_id=?",
                (
                    json_dumps(prompt_payload),
                    prompt_version,
                    json_dumps(prompt_payload.get("template_versions") or {}),
                    json_dumps(prompt_payload.get("template_snapshots") or {}),
                    now_iso(),
                    job_id,
                ),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"找不到任务: {job_id}")

    def set_generation_preferences(
        self,
        job_id: str,
        *,
        channel: str,
        model: str,
        duration_seconds: int,
        rerun: bool = False,
    ) -> None:
        if channel not in {"no_generate", "auto", "jimeng", "imini"}:
            raise ValueError(f"非法生成渠道: {channel}")
        if model not in {"Seedance 2.0", "Seedance 2.0 VIP"}:
            raise ValueError(f"非法生成模型: {model}")
        if int(duration_seconds) not in {8, 10}:
            raise ValueError("视频时长只支持 8 秒或 10 秒")
        with self.connection() as conn:
            cursor = conn.execute(
                "UPDATE video_jobs SET generation_channel=?, generation_model=?, duration_seconds=?, generation_rerun=?, "
                "run_manager_sync_status=CASE WHEN ?='no_generate' THEN 'not_submitted' ELSE 'pending' END, updated_at=? WHERE job_id=?",
                (channel, model, int(duration_seconds), 1 if rerun else 0, channel, now_iso(), job_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"找不到任务: {job_id}")

    def update_run_manager_sync(
        self,
        job_id: str,
        *,
        record_id: str = "",
        status: str,
        error: str = "",
        trace_id: str = "",
        result_status: str = "",
        source_hash: str = "",
        clear_rerun: bool = False,
    ) -> None:
        with self.connection() as conn:
            cursor = conn.execute(
                "UPDATE video_jobs SET run_manager_record_id=CASE WHEN ?='' THEN run_manager_record_id ELSE ? END, "
                "run_manager_sync_status=?, run_manager_sync_error=?, run_manager_last_synced_at=?, "
                "run_manager_trace_id=CASE WHEN ?='' THEN run_manager_trace_id ELSE ? END, "
                "run_manager_result_status=CASE WHEN ?='' THEN run_manager_result_status ELSE ? END, "
                "run_manager_source_hash=CASE WHEN ?='' THEN run_manager_source_hash ELSE ? END, "
                "generation_rerun=CASE WHEN ? THEN 0 ELSE generation_rerun END, updated_at=? WHERE job_id=?",
                (
                    record_id, record_id, status, str(error)[:2000], now_iso(), trace_id, trace_id,
                    result_status, result_status, source_hash, source_hash, 1 if clear_rerun else 0, now_iso(), job_id,
                ),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"找不到任务: {job_id}")

    def set_run_manager_result(
        self,
        job_id: str,
        *,
        attachments: list[dict[str, Any]],
        trace_id: str = "",
    ) -> None:
        with self.connection() as conn:
            cursor = conn.execute(
                "UPDATE video_jobs SET generation_status='success', qc_status='pending', last_error='', "
                "raw_video_attachments=?, run_manager_sync_status='returned', run_manager_result_status='uploaded', "
                "run_manager_trace_id=CASE WHEN ?='' THEN run_manager_trace_id ELSE ? END, updated_at=? WHERE job_id=?",
                (json_dumps(attachments), trace_id, trace_id, now_iso(), job_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"找不到任务: {job_id}")

    def claim_pending_jobs(self, limit: int, provider: str) -> list[dict[str, Any]]:
        claimed: list[dict[str, Any]] = []
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                "SELECT * FROM video_jobs WHERE generation_status IN ('pending','retrying') "
                "AND prompt_version != 'unbuilt' "
                "AND (visual_plan_id='' OR EXISTS (SELECT 1 FROM product_visual_plans vp WHERE vp.visual_plan_id=video_jobs.visual_plan_id "
                "AND vp.plan_status='active' AND vp.outfit_image_status='confirmed')) "
                "ORDER BY publish_priority DESC, created_at, variant_no LIMIT ?",
                (int(limit),),
            ).fetchall()
            for row in rows:
                job = self._decode_row("video_jobs", row) or {}
                attempt_no = int(job.get("retry_count") or 0) + 1
                conn.execute(
                    "UPDATE video_jobs SET generation_status='generating', retry_count=?, last_error='', updated_at=? WHERE job_id=?",
                    (attempt_no, now_iso(), job["job_id"]),
                )
                conn.execute(
                    "INSERT INTO job_attempts(job_id,attempt_no,provider,status,request_payload,started_at) VALUES(?,?,?,?,?,?)",
                    (job["job_id"], attempt_no, provider, "started", json_dumps(job.get("prompt_payload") or {}), now_iso()),
                )
                job["generation_status"] = "generating"
                job["retry_count"] = attempt_no
                claimed.append(job)
            conn.commit()
            return claimed
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def complete_generation(self, job_id: str, response: dict[str, Any]) -> None:
        path = str(response.get("output_video_path") or "").strip()
        if not path:
            raise ValueError("生成结果缺少 output_video_path")
        with self.connection() as conn:
            job = conn.execute("SELECT retry_count FROM video_jobs WHERE job_id=?", (job_id,)).fetchone()
            if not job:
                raise KeyError(f"找不到任务: {job_id}")
            conn.execute(
                "UPDATE video_jobs SET generation_status='success', qc_status='pending', output_video_path=?, raw_video_path=?, output_cover_path=?, last_error='', updated_at=? WHERE job_id=?",
                (path, path, str(response.get("output_cover_path") or ""), now_iso(), job_id),
            )
            conn.execute(
                "UPDATE job_attempts SET status='success', response_payload=?, finished_at=? WHERE job_id=? AND attempt_no=?",
                (json_dumps(response), now_iso(), job_id, int(job["retry_count"])),
            )

    def fail_generation(self, job_id: str, error: str, *, retryable: bool = True) -> None:
        with self.connection() as conn:
            job = conn.execute("SELECT retry_count FROM video_jobs WHERE job_id=?", (job_id,)).fetchone()
            if not job:
                raise KeyError(f"找不到任务: {job_id}")
            status = "retrying" if retryable else "failed"
            conn.execute(
                "UPDATE video_jobs SET generation_status=?, last_error=?, updated_at=? WHERE job_id=?",
                (status, str(error)[:2000], now_iso(), job_id),
            )
            conn.execute(
                "UPDATE job_attempts SET status='failed', error_message=?, finished_at=? WHERE job_id=? AND attempt_no=?",
                (str(error)[:2000], now_iso(), job_id, int(job["retry_count"])),
            )

    def reset_job(self, job_id: str, *, clear_output: bool = False) -> None:
        with self.connection() as conn:
            assignments = ["generation_status='pending'", "qc_status='pending'", "last_error=''", "updated_at=?"]
            params: list[Any] = [now_iso()]
            if clear_output:
                assignments.extend(["output_video_path=''", "raw_video_path=''", "output_cover_path=''", "qc_result='{}'"])
            params.append(job_id)
            cursor = conn.execute(f"UPDATE video_jobs SET {','.join(assignments)} WHERE job_id=?", params)
            if cursor.rowcount != 1:
                raise KeyError(f"找不到任务: {job_id}")

    def set_postprocessed_video(self, job_id: str, output_path: str) -> None:
        self.set_postprocessed_assets(job_id, output_path)

    def set_postprocessed_assets(self, job_id: str, output_path: str, cover_path: str = "") -> None:
        with self.connection() as conn:
            cursor = conn.execute(
                "UPDATE video_jobs SET output_video_path=?, output_cover_path=CASE WHEN ?='' THEN output_cover_path ELSE ? END, "
                "qc_status='pending', qc_result='{}', updated_at=? WHERE job_id=?",
                (str(output_path), str(cover_path), str(cover_path), now_iso(), job_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"找不到任务: {job_id}")

    def update_review_video_processing(
        self,
        job_id: str,
        *,
        status: str,
        source_hash: str,
        raw_path: str = "",
        raw_attachments: list[dict[str, Any]] | None = None,
        final_attachments: list[dict[str, Any]] | None = None,
        error: str = "",
        processed_at: str = "",
    ) -> None:
        with self.connection() as conn:
            cursor = conn.execute(
                "UPDATE video_jobs SET review_video_process_status=?, review_video_source_hash=?, "
                "raw_video_path=CASE WHEN ?='' THEN raw_video_path ELSE ? END, "
                "raw_video_attachments=?, final_video_attachments=CASE WHEN ? IS NULL THEN final_video_attachments ELSE ? END, "
                "review_video_process_error=?, review_video_processed_at=CASE WHEN ?='' THEN review_video_processed_at ELSE ? END, updated_at=? "
                "WHERE job_id=?",
                (
                    str(status), str(source_hash), str(raw_path), str(raw_path),
                    json_dumps(raw_attachments or []),
                    None if final_attachments is None else 1,
                    json_dumps(final_attachments or []),
                    str(error)[:2000], str(processed_at), str(processed_at), now_iso(), job_id,
                ),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"找不到任务: {job_id}")

    def update_review_sync(self, job_id: str, record_id: str, status: str, error: str = "") -> None:
        with self.connection() as conn:
            cursor = conn.execute(
                "UPDATE video_jobs SET feishu_review_record_id=?, review_sync_status=?, review_last_synced_at=?, review_sync_error=?, updated_at=? WHERE job_id=?",
                (record_id, status, now_iso(), str(error)[:2000], now_iso(), job_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"找不到任务: {job_id}")

    def apply_manual_review(
        self,
        job_id: str,
        *,
        manual_review_status: str,
        manual_review_reason: str,
        need_regeneration: str,
        regeneration_strategy: str,
        publish_status: str,
        operator_notes: str,
        source_hash: str,
        review_version: int,
    ) -> None:
        with self.connection() as conn:
            cursor = conn.execute(
                "UPDATE video_jobs SET manual_review_status=?, manual_review_reason=?, need_regeneration=?, "
                "regeneration_strategy=?, publish_status=?, operator_notes=?, review_source_hash=?, review_version=?, "
                "review_processed_at=?, updated_at=? WHERE job_id=?",
                (
                    manual_review_status,
                    manual_review_reason,
                    need_regeneration,
                    regeneration_strategy,
                    publish_status,
                    operator_notes,
                    source_hash,
                    int(review_version),
                    now_iso(),
                    now_iso(),
                    job_id,
                ),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"找不到任务: {job_id}")

    def get_review_event(self, job_id: str, review_fingerprint: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM manual_review_events WHERE job_id=? AND review_fingerprint=?",
                (job_id, review_fingerprint),
            ).fetchone()
            return dict(row) if row else None

    def record_review_event(
        self,
        job_id: str,
        review_fingerprint: str,
        *,
        feishu_record_id: str,
        manual_review_status: str,
        need_regeneration: str,
        regeneration_strategy: str,
        regeneration_job_id: str,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO manual_review_events(job_id,review_fingerprint,feishu_record_id,manual_review_status,"
                "need_regeneration,regeneration_strategy,regeneration_job_id,processed_at) VALUES(?,?,?,?,?,?,?,?)",
                (
                    job_id,
                    review_fingerprint,
                    feishu_record_id,
                    manual_review_status,
                    need_regeneration,
                    regeneration_strategy,
                    regeneration_job_id,
                    now_iso(),
                ),
            )

    def create_regeneration_job(self, parent_job_id: str, review_fingerprint: str, strategy: str) -> dict[str, Any]:
        parent = self.get_job(parent_job_id)
        if not parent:
            raise KeyError(f"找不到父任务: {parent_job_id}")
        child_id = f"{parent_job_id}_R_{review_fingerprint[:8]}"
        existing = self.get_job(child_id)
        if existing:
            return existing
        scene_id = parent["scene_id"]
        action_id = parent["action_id"]
        styling_id = parent["styling_id"]
        if parent.get("visual_plan_id") and strategy in {"change_scene", "change_styling"}:
            raise ValueError("已绑定产品穿搭图的视频不能直接更换场景或搭配；请在原始脚本表新增选择并生成新的视觉方案")
        if strategy == "change_action":
            choices = [row for row in self.list_templates("action", "enabled") if row["action_id"] != action_id]
            if not choices:
                raise ValueError("没有可替换的启用动作模板")
            action_id = choices[0]["action_id"]
        elif strategy == "change_scene":
            choices = [row for row in self.list_templates("scene", "enabled") if row["scene_id"] != scene_id]
            if not choices:
                raise ValueError("没有可替换的启用场景模板")
            scene_id = choices[0]["scene_id"]
        elif strategy == "change_styling":
            choices = [row for row in self.list_templates("styling", "enabled") if row["styling_id"] != styling_id]
            if not choices:
                raise ValueError("没有可替换的启用搭配模板")
            styling_id = choices[0]["styling_id"]
        with self.connection() as conn:
            max_variant = conn.execute(
                "SELECT COALESCE(MAX(variant_no),0) FROM video_jobs WHERE product_id=?",
                (parent["product_id"],),
            ).fetchone()[0]
            now = now_iso()
            conn.execute(
                "INSERT INTO video_jobs(job_id,product_id,market,language,persona_id,scene_id,shot_profile_id,shot_plan_id,action_id,styling_id,subtitle_id,"
                "duration_seconds,variant_no,publish_priority,generation_status,qc_status,plan_version,parent_job_id,account_id,"
                "visual_plan_id,outfit_image_path,outfit_image_url,outfit_image_version,legacy_job,source_script_record_id,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    child_id,
                    parent["product_id"],
                    parent["market"],
                    parent["language"],
                    parent["persona_id"],
                    scene_id,
                    parent.get("shot_profile_id") or "SHOT_FULL_FIXED",
                    parent.get("shot_plan_id") or "",
                    action_id,
                    styling_id,
                    parent["subtitle_id"],
                    parent["duration_seconds"],
                    int(max_variant) + 1,
                    parent["publish_priority"],
                    "pending",
                    "pending",
                    f"regen-{review_fingerprint[:12]}",
                    parent_job_id,
                    parent.get("account_id") or "",
                    parent.get("visual_plan_id") or "",
                    parent.get("outfit_image_path") or "",
                    parent.get("outfit_image_url") or "",
                    parent.get("outfit_image_version") or "",
                    int(parent.get("legacy_job") or 0),
                    parent.get("source_script_record_id") or "",
                    now,
                    now,
                ),
            )
        return self.get_job(child_id) or {}

    def start_sync_run(self, batch_id: str, sync_type: str) -> None:
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO feishu_sync_runs(batch_id,sync_type,status,started_at) VALUES(?,?,?,?)",
                (batch_id, sync_type, "running", now_iso()),
            )

    def log_sync_item(
        self,
        batch_id: str,
        table_role: str,
        feishu_record_id: str,
        business_id: str,
        operation: str,
        status: str,
        error: str = "",
    ) -> None:
        timestamp = now_iso()
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO feishu_sync_items(batch_id,table_role,feishu_record_id,business_id,operation,status,error_message,started_at,finished_at) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (batch_id, table_role, feishu_record_id, business_id, operation, status, str(error)[:2000], timestamp, timestamp),
            )

    def finish_sync_run(self, batch_id: str, summary: dict[str, int], status: str = "success", error: str = "") -> None:
        with self.connection() as conn:
            conn.execute(
                "UPDATE feishu_sync_runs SET status=?, finished_at=?, created_count=?, updated_count=?, skipped_count=?, failed_count=?, error_message=? WHERE batch_id=?",
                (
                    status,
                    now_iso(),
                    int(summary.get("created") or 0),
                    int(summary.get("updated") or 0),
                    int(summary.get("skipped") or 0),
                    int(summary.get("failed") or 0),
                    str(error)[:2000],
                    batch_id,
                ),
            )

    def apply_qc(self, job_id: str, qc_status: str, qc_result: dict[str, Any]) -> None:
        if qc_status not in {"pending", "passed", "failed", "manual_review"}:
            raise ValueError(f"非法 qc_status: {qc_status}")
        with self.connection() as conn:
            cursor = conn.execute(
                "UPDATE video_jobs SET qc_status=?, qc_result=?, updated_at=? WHERE job_id=?",
                (qc_status, json_dumps(qc_result), now_iso(), job_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"找不到任务: {job_id}")

    def stats(self) -> dict[str, Any]:
        with self.connection() as conn:
            counts = {
                "products": conn.execute("SELECT COUNT(*) FROM products_for_light_video").fetchone()[0],
                "jobs": conn.execute("SELECT COUNT(*) FROM video_jobs").fetchone()[0],
                "visual_plans": conn.execute("SELECT COUNT(*) FROM product_visual_plans").fetchone()[0],
            }
            counts["generation"] = {
                row[0]: row[1]
                for row in conn.execute("SELECT generation_status,COUNT(*) FROM video_jobs GROUP BY generation_status")
            }
            counts["qc"] = {
                row[0]: row[1]
                for row in conn.execute("SELECT qc_status,COUNT(*) FROM video_jobs GROUP BY qc_status")
            }
            return counts
