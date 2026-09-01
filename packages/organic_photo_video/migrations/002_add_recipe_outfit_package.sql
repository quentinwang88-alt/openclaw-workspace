-- OPV capability upgrade 002: product-driven content recipes, outfit plans,
-- and content packages. Purely additive: new tables + nullable columns only.

ALTER TABLE opv_content_task
    ADD COLUMN recipe_id VARCHAR(64) NULL,
    ADD COLUMN recipe_version INT NULL,
    ADD COLUMN content_goal VARCHAR(64) NULL,
    ADD COLUMN hook_strategy VARCHAR(64) NULL,
    ADD COLUMN outfit_plan_json JSON NULL,
    ADD COLUMN storyboard_version VARCHAR(32) NULL,
    ADD COLUMN content_package_id VARCHAR(64) NULL,
    ADD COLUMN product_facts_json JSON NULL;

ALTER TABLE opv_content_shot
    ADD COLUMN narrative_function VARCHAR(32) NULL,
    ADD COLUMN product_focus VARCHAR(64) NULL,
    ADD COLUMN overlay_spec_json JSON NULL,
    ADD COLUMN transition_hint VARCHAR(32) NULL,
    ADD COLUMN continuity_constraints_json JSON NULL;

CREATE TABLE IF NOT EXISTS opv_content_recipe (
    recipe_id VARCHAR(64) PRIMARY KEY,
    recipe_key VARCHAR(96) NOT NULL,
    recipe_version INT NOT NULL DEFAULT 1,
    content_goal VARCHAR(64) NOT NULL,
    anchor_slot SMALLINT NOT NULL DEFAULT 1,
    shot_count SMALLINT NOT NULL DEFAULT 5,
    hook_types_json JSON NOT NULL,
    story_structure_json JSON NOT NULL,
    copy_style_json JSON NOT NULL,
    render_profile_id VARCHAR(64) NULL,
    quality_profile_id VARCHAR(64) NULL,
    suitable_topics_json JSON NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'draft',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_recipe_version (recipe_key, recipe_version),
    KEY idx_opv_recipe_status (status, content_goal)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opv_render_profile (
    render_profile_id VARCHAR(64) PRIMARY KEY,
    profile_key VARCHAR(96) NOT NULL,
    profile_version INT NOT NULL DEFAULT 1,
    aspect_ratio VARCHAR(16) NOT NULL DEFAULT '9:16',
    duration_min_ms INT NOT NULL DEFAULT 10000,
    duration_max_ms INT NOT NULL DEFAULT 15000,
    fps INT NOT NULL DEFAULT 30,
    codec VARCHAR(32) NOT NULL DEFAULT 'h264',
    hook_window_ms INT NOT NULL DEFAULT 2000,
    motion_rules_json JSON NOT NULL,
    transition_rules_json JSON NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'draft',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_render_profile_version (profile_key, profile_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opv_quality_profile (
    quality_profile_id VARCHAR(64) PRIMARY KEY,
    profile_key VARCHAR(96) NOT NULL,
    profile_version INT NOT NULL DEFAULT 1,
    dimensions_json JSON NOT NULL,
    min_overall_score DECIMAL(5,2) NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'draft',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_quality_profile_version (profile_key, profile_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opv_content_package (
    content_package_id VARCHAR(64) PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL,
    product_snapshot_id VARCHAR(96) NULL,
    recipe_id VARCHAR(64) NULL,
    theme_id VARCHAR(64) NULL,
    outfit_plan_id VARCHAR(96) NULL,
    storyboard_version VARCHAR(32) NULL,
    selected_image_ids_json JSON NOT NULL,
    cover_image_id VARCHAR(64) NULL,
    cover_title VARCHAR(4000) NULL,
    caption TEXT NULL,
    hashtags_json JSON NOT NULL,
    render_ids_json JSON NOT NULL,
    qa_summary_json JSON NULL,
    content_fingerprint CHAR(64) NULL,
    generation_lineage_json JSON NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'planning',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    KEY idx_opv_package_task (task_id, status),
    KEY idx_opv_package_status (status, updated_at),
    CONSTRAINT fk_opv_package_task
        FOREIGN KEY (task_id) REFERENCES opv_content_task(task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
