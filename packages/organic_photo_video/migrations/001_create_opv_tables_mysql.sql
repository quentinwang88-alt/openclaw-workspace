-- Organic Photo Video (OPV) V1 schema for MySQL 8 / Aliyun RDS.
-- The image group is the canonical content asset; the MP4 is a publish carrier.
-- Feishu synchronization is intentionally held in an outbox while API quota is unavailable.

CREATE TABLE IF NOT EXISTS opv_market_pack (
    market_pack_id VARCHAR(64) PRIMARY KEY,
    pack_key VARCHAR(96) NOT NULL,
    pack_version INT NOT NULL DEFAULT 1,
    target_country VARCHAR(16) NOT NULL,
    target_locale VARCHAR(16) NOT NULL,
    climate_zone VARCHAR(64) NULL,
    season_key VARCHAR(32) NOT NULL DEFAULT 'all_season',
    pack_name VARCHAR(191) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'draft',
    visual_rules_json JSON NOT NULL,
    copy_rules_json JSON NOT NULL,
    topic_rules_json JSON NOT NULL,
    safety_rules_json JSON NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_market_pack_version (pack_key, pack_version),
    KEY idx_opv_market_pack_market (target_country, target_locale, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opv_theme_catalog (
    theme_id VARCHAR(64) PRIMARY KEY,
    theme_key VARCHAR(96) NOT NULL,
    theme_version INT NOT NULL DEFAULT 1,
    theme_name VARCHAR(191) NOT NULL,
    description TEXT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'draft',
    applicable_markets_json JSON NOT NULL,
    product_match_rules_json JSON NOT NULL,
    content_plan_rules_json JSON NOT NULL,
    default_storyboard_json JSON NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_theme_version (theme_key, theme_version),
    KEY idx_opv_theme_status (status, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opv_render_preset (
    render_preset_id VARCHAR(64) PRIMARY KEY,
    preset_key VARCHAR(96) NOT NULL,
    preset_version INT NOT NULL DEFAULT 1,
    preset_name VARCHAR(191) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'draft',
    width_px INT NOT NULL DEFAULT 1080,
    height_px INT NOT NULL DEFAULT 1920,
    fps INT NOT NULL DEFAULT 30,
    target_duration_ms INT NOT NULL DEFAULT 12500,
    codec VARCHAR(32) NOT NULL DEFAULT 'h264',
    render_mode VARCHAR(32) NOT NULL DEFAULT 'still_slideshow',
    motion_rules_json JSON NOT NULL,
    transition_rules_json JSON NOT NULL,
    text_overlay_rules_json JSON NOT NULL,
    audio_rules_json JSON NOT NULL,
    output_rules_json JSON NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_render_preset_version (preset_key, preset_version),
    KEY idx_opv_render_preset_status (status, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opv_account_profile (
    account_id VARCHAR(64) PRIMARY KEY,
    account_code VARCHAR(96) NOT NULL,
    account_name VARCHAR(191) NOT NULL,
    platform VARCHAR(32) NOT NULL DEFAULT 'tiktok',
    target_country VARCHAR(16) NOT NULL,
    default_locale VARCHAR(16) NOT NULL,
    timezone VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'testing',
    persona_ref_id VARCHAR(96) NULL,
    persona_snapshot_json JSON NOT NULL,
    visual_identity_json JSON NOT NULL,
    allowed_style_refs_json JSON NOT NULL,
    allowed_look_refs_json JSON NOT NULL,
    allowed_scene_refs_json JSON NOT NULL,
    core_scene_refs_json JSON NOT NULL,
    default_market_pack_id VARCHAR(64) NULL,
    default_render_preset_id VARCHAR(64) NULL,
    operating_rules_json JSON NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_account_code (account_code),
    KEY idx_opv_account_market (target_country, status),
    CONSTRAINT fk_opv_account_market_pack
        FOREIGN KEY (default_market_pack_id) REFERENCES opv_market_pack(market_pack_id),
    CONSTRAINT fk_opv_account_render_preset
        FOREIGN KEY (default_render_preset_id) REFERENCES opv_render_preset(render_preset_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opv_content_task (
    task_id VARCHAR(64) PRIMARY KEY,
    idempotency_key CHAR(64) NOT NULL,
    source_type VARCHAR(32) NOT NULL DEFAULT 'manual',
    source_record_id VARCHAR(191) NULL,
    account_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(191) NOT NULL,
    product_snapshot_json JSON NOT NULL,
    target_country VARCHAR(16) NOT NULL,
    target_locale VARCHAR(16) NOT NULL,
    market_pack_id VARCHAR(64) NULL,
    theme_id VARCHAR(64) NULL,
    topic_text TEXT NULL,
    task_status VARCHAR(32) NOT NULL DEFAULT 'draft',
    current_stage VARCHAR(64) NOT NULL DEFAULT 'intake',
    priority VARCHAR(16) NOT NULL DEFAULT 'normal',
    requested_shot_count SMALLINT NOT NULL DEFAULT 5,
    retry_count INT NOT NULL DEFAULT 0,
    max_retry_count INT NOT NULL DEFAULT 2,
    plan_json JSON NULL,
    copy_json JSON NULL,
    group_qa_json JSON NULL,
    selected_render_id VARCHAR(64) NULL,
    failure_code VARCHAR(96) NULL,
    failure_detail TEXT NULL,
    feishu_record_id VARCHAR(191) NULL,
    created_by VARCHAR(96) NOT NULL DEFAULT 'manual',
    started_at DATETIME(6) NULL,
    completed_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_task_idempotency (idempotency_key),
    UNIQUE KEY uq_opv_task_source (source_type, source_record_id),
    KEY idx_opv_task_queue (task_status, priority, created_at),
    KEY idx_opv_task_product (product_id, created_at),
    KEY idx_opv_task_account (account_id, created_at),
    CONSTRAINT fk_opv_task_account
        FOREIGN KEY (account_id) REFERENCES opv_account_profile(account_id),
    CONSTRAINT fk_opv_task_market_pack
        FOREIGN KEY (market_pack_id) REFERENCES opv_market_pack(market_pack_id),
    CONSTRAINT fk_opv_task_theme
        FOREIGN KEY (theme_id) REFERENCES opv_theme_catalog(theme_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opv_content_shot (
    shot_id VARCHAR(64) PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL,
    slot_index SMALLINT NOT NULL,
    slot_role VARCHAR(32) NOT NULL,
    shot_version INT NOT NULL DEFAULT 1,
    shot_status VARCHAR(32) NOT NULL DEFAULT 'planned',
    narrative_purpose TEXT NULL,
    duration_ms INT NOT NULL,
    motion_preset VARCHAR(64) NOT NULL DEFAULT 'slow_push',
    transition_in VARCHAR(32) NOT NULL DEFAULT 'cut',
    transition_out VARCHAR(32) NOT NULL DEFAULT 'cut',
    overlay_text TEXT NULL,
    generation_prompt LONGTEXT NULL,
    negative_prompt LONGTEXT NULL,
    source_refs_json JSON NOT NULL,
    generation_provider VARCHAR(64) NULL,
    generation_model VARCHAR(96) NULL,
    generation_request_id VARCHAR(191) NULL,
    image_oss_object_id VARCHAR(191) NULL,
    image_url TEXT NULL,
    image_sha256 CHAR(64) NULL,
    image_width INT NULL,
    image_height INT NULL,
    is_selected TINYINT(1) NOT NULL DEFAULT 0,
    qa_status VARCHAR(32) NOT NULL DEFAULT 'pending',
    qa_json JSON NULL,
    failure_detail TEXT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_shot_slot_version (task_id, slot_index, shot_version),
    KEY idx_opv_shot_queue (shot_status, qa_status, created_at),
    KEY idx_opv_shot_task_selected (task_id, is_selected, slot_index),
    CONSTRAINT fk_opv_shot_task
        FOREIGN KEY (task_id) REFERENCES opv_content_task(task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opv_video_render (
    render_id VARCHAR(64) PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL,
    render_version INT NOT NULL DEFAULT 1,
    render_preset_id VARCHAR(64) NOT NULL,
    render_mode VARCHAR(32) NOT NULL DEFAULT 'still_slideshow',
    render_engine VARCHAR(64) NOT NULL DEFAULT 'ffmpeg',
    render_status VARCHAR(32) NOT NULL DEFAULT 'queued',
    shot_selection_json JSON NOT NULL,
    timeline_json JSON NOT NULL,
    copy_snapshot_json JSON NOT NULL,
    bgm_ref_id VARCHAR(96) NULL,
    bgm_oss_object_id VARCHAR(191) NULL,
    duration_ms INT NULL,
    output_oss_object_id VARCHAR(191) NULL,
    output_url TEXT NULL,
    output_sha256 CHAR(64) NULL,
    output_metadata_json JSON NULL,
    qc_status VARCHAR(32) NOT NULL DEFAULT 'pending',
    qc_json JSON NULL,
    publish_ready TINYINT(1) NOT NULL DEFAULT 0,
    failure_detail TEXT NULL,
    started_at DATETIME(6) NULL,
    completed_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_render_version (task_id, render_version),
    KEY idx_opv_render_queue (render_status, qc_status, created_at),
    CONSTRAINT fk_opv_render_task
        FOREIGN KEY (task_id) REFERENCES opv_content_task(task_id),
    CONSTRAINT fk_opv_render_preset
        FOREIGN KEY (render_preset_id) REFERENCES opv_render_preset(render_preset_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opv_publish_record (
    publish_id VARCHAR(64) PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL,
    render_id VARCHAR(64) NOT NULL,
    account_id VARCHAR(64) NOT NULL,
    platform VARCHAR(32) NOT NULL DEFAULT 'tiktok',
    publish_status VARCHAR(32) NOT NULL DEFAULT 'ready',
    publish_mode VARCHAR(32) NOT NULL DEFAULT 'manual',
    external_post_id VARCHAR(191) NULL,
    external_post_url TEXT NULL,
    caption_snapshot_json JSON NOT NULL,
    cover_shot_id VARCHAR(64) NULL,
    operator_name VARCHAR(96) NULL,
    published_at DATETIME(6) NULL,
    platform_metadata_json JSON NULL,
    failure_detail TEXT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_publish_render (render_id),
    KEY idx_opv_publish_account_time (account_id, published_at),
    KEY idx_opv_publish_status (publish_status, created_at),
    CONSTRAINT fk_opv_publish_task
        FOREIGN KEY (task_id) REFERENCES opv_content_task(task_id),
    CONSTRAINT fk_opv_publish_render
        FOREIGN KEY (render_id) REFERENCES opv_video_render(render_id),
    CONSTRAINT fk_opv_publish_account
        FOREIGN KEY (account_id) REFERENCES opv_account_profile(account_id),
    CONSTRAINT fk_opv_publish_cover_shot
        FOREIGN KEY (cover_shot_id) REFERENCES opv_content_shot(shot_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opv_metric_snapshot (
    metric_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    publish_id VARCHAR(64) NOT NULL,
    captured_after_hours INT NOT NULL DEFAULT 24,
    captured_at DATETIME(6) NOT NULL,
    source_type VARCHAR(32) NOT NULL DEFAULT 'manual',
    view_count BIGINT NOT NULL DEFAULT 0,
    like_count BIGINT NOT NULL DEFAULT 0,
    comment_count BIGINT NOT NULL DEFAULT 0,
    share_count BIGINT NOT NULL DEFAULT 0,
    save_count BIGINT NOT NULL DEFAULT 0,
    profile_visit_count BIGINT NOT NULL DEFAULT 0,
    follower_gain_count BIGINT NOT NULL DEFAULT 0,
    avg_watch_time_ms BIGINT NULL,
    completion_rate DECIMAL(8,6) NULL,
    engagement_rate DECIMAL(8,6) NULL,
    raw_metrics_json JSON NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_metric_window (publish_id, captured_after_hours),
    KEY idx_opv_metric_captured (captured_at),
    CONSTRAINT fk_opv_metric_publish
        FOREIGN KEY (publish_id) REFERENCES opv_publish_record(publish_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opv_look_feedback (
    feedback_id VARCHAR(64) PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL,
    account_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(191) NOT NULL,
    look_ref_id VARCHAR(96) NULL,
    feedback_type VARCHAR(32) NOT NULL,
    decision VARCHAR(32) NOT NULL,
    reason_codes_json JSON NOT NULL,
    score_json JSON NOT NULL,
    evidence_json JSON NULL,
    promotion_status VARCHAR(32) NOT NULL DEFAULT 'not_requested',
    successful_look_ref_id VARCHAR(96) NULL,
    reviewer VARCHAR(96) NULL,
    notes TEXT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    KEY idx_opv_feedback_decision (decision, promotion_status, created_at),
    KEY idx_opv_feedback_product (product_id, created_at),
    CONSTRAINT fk_opv_feedback_task
        FOREIGN KEY (task_id) REFERENCES opv_content_task(task_id),
    CONSTRAINT fk_opv_feedback_account
        FOREIGN KEY (account_id) REFERENCES opv_account_profile(account_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opv_feishu_outbox (
    outbox_id VARCHAR(64) PRIMARY KEY,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(191) NOT NULL,
    operation VARCHAR(96) NOT NULL,
    target_app_token VARCHAR(191) NULL,
    target_table_id VARCHAR(191) NULL,
    target_record_id VARCHAR(191) NULL,
    payload_json JSON NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'holding',
    attempts INT NOT NULL DEFAULT 0,
    not_before DATETIME(6) NULL,
    last_error TEXT NULL,
    synced_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_outbox_operation (aggregate_type, aggregate_id, operation),
    KEY idx_opv_outbox_status (status, not_before, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
