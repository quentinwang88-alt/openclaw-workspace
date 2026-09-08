-- Native-photo MVP.  Existing video rows keep their current behavior through
-- explicit defaults/nullability; this migration does not rewrite old content.

ALTER TABLE opv_content_task
    MODIFY COLUMN product_id VARCHAR(191) NULL,
    ADD COLUMN media_kind VARCHAR(24) NOT NULL DEFAULT 'video' AFTER product_snapshot_json,
    ADD COLUMN category_key VARCHAR(32) NULL AFTER media_kind,
    ADD COLUMN product_mode VARCHAR(32) NULL AFTER category_key,
    ADD KEY idx_opv_task_media_queue (media_kind, task_status, priority, created_at),
    ADD KEY idx_opv_task_category (category_key, target_country, created_at);

ALTER TABLE opv_content_shot
    MODIFY COLUMN duration_ms INT NULL;

ALTER TABLE opv_content_recipe
    ADD COLUMN recipe_spec_json JSON NULL AFTER suitable_topics_json;

ALTER TABLE opv_content_package
    ADD COLUMN photo_manifest_json JSON NULL AFTER generation_lineage_json;

ALTER TABLE opv_publish_record
    MODIFY COLUMN render_id VARCHAR(64) NULL,
    ADD COLUMN media_kind VARCHAR(24) NOT NULL DEFAULT 'video' AFTER account_id,
    ADD COLUMN content_package_id VARCHAR(64) NULL AFTER media_kind,
    ADD COLUMN revision_id VARCHAR(64) NULL AFTER content_package_id,
    ADD COLUMN main_slot_id BIGINT NULL AFTER revision_id,
    ADD COLUMN publisher_account_id VARCHAR(191) NULL AFTER main_slot_id,
    ADD COLUMN publish_channel VARCHAR(64) NULL AFTER publisher_account_id,
    ADD COLUMN provider_task_id VARCHAR(191) NULL AFTER publish_channel,
    ADD COLUMN publish_key CHAR(64) NULL AFTER provider_task_id,
    ADD COLUMN release_manifest_json JSON NULL AFTER publish_key,
    ADD UNIQUE KEY uq_opv_publish_key (publish_key),
    ADD KEY idx_opv_publish_package (content_package_id, revision_id),
    ADD KEY idx_opv_publish_provider (publish_channel, provider_task_id),
    ADD CONSTRAINT fk_opv_publish_package
        FOREIGN KEY (content_package_id) REFERENCES opv_content_package(content_package_id),
    ADD CONSTRAINT fk_opv_publish_revision
        FOREIGN KEY (revision_id) REFERENCES opv_task_revision(revision_id);

CREATE TABLE IF NOT EXISTS opv_asset_set (
    asset_set_id VARCHAR(64) PRIMARY KEY,
    asset_set_key VARCHAR(96) NOT NULL,
    asset_set_version INT NOT NULL DEFAULT 1,
    category_key VARCHAR(32) NOT NULL,
    market VARCHAR(16) NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'draft',
    tags_json JSON NOT NULL,
    manifest_json JSON NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_asset_set_version (asset_set_key, asset_set_version),
    KEY idx_opv_asset_set_lookup (category_key, market, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
