-- Workflow V2: immutable planning revisions and append-only scoped reviews.
-- Existing rows remain valid until a task is explicitly resumed in V2.
ALTER TABLE opv_content_task
    ADD COLUMN workflow_version INT NOT NULL DEFAULT 1,
    ADD COLUMN active_revision_id VARCHAR(64) NULL,
    ADD COLUMN released_revision_id VARCHAR(64) NULL,
    ADD COLUMN row_version INT NOT NULL DEFAULT 1,
    ADD KEY idx_opv_task_active_revision (active_revision_id),
    ADD KEY idx_opv_task_released_revision (released_revision_id);

ALTER TABLE opv_content_shot
    ADD COLUMN origin_revision_id VARCHAR(64) NULL,
    ADD COLUMN input_fingerprint CHAR(64) NULL,
    ADD KEY idx_opv_shot_revision (origin_revision_id, slot_index, shot_version);

ALTER TABLE opv_video_render
    ADD COLUMN origin_revision_id VARCHAR(64) NULL,
    ADD COLUMN input_fingerprint CHAR(64) NULL,
    ADD KEY idx_opv_render_revision (origin_revision_id, render_version);

CREATE TABLE IF NOT EXISTS opv_task_revision (
    revision_id VARCHAR(64) PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL,
    revision_no INT NOT NULL,
    parent_revision_id VARCHAR(64) NULL,
    plan_snapshot_json JSON NOT NULL,
    input_snapshot_hash CHAR(64) NOT NULL,
    asset_manifest_json JSON NOT NULL,
    selection_hash CHAR(64) NOT NULL,
    rework_spec_json JSON NOT NULL,
    revision_status VARCHAR(32) NOT NULL DEFAULT 'working',
    lock_version INT NOT NULL DEFAULT 1,
    created_by VARCHAR(96) NOT NULL DEFAULT 'system',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_task_revision_no (task_id, revision_no),
    KEY idx_opv_revision_task_status (task_id, revision_status, revision_no),
    CONSTRAINT fk_opv_revision_task FOREIGN KEY (task_id) REFERENCES opv_content_task(task_id),
    CONSTRAINT fk_opv_revision_parent FOREIGN KEY (parent_revision_id) REFERENCES opv_task_revision(revision_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opv_quality_review (
    review_id VARCHAR(64) PRIMARY KEY,
    revision_id VARCHAR(64) NOT NULL,
    scope VARCHAR(32) NOT NULL,
    target_id VARCHAR(96) NOT NULL,
    input_fingerprint CHAR(64) NOT NULL,
    quality_profile_id VARCHAR(64) NOT NULL,
    quality_profile_version INT NOT NULL,
    decision VARCHAR(32) NOT NULL,
    reviewer_type VARCHAR(32) NOT NULL,
    dimensions_json JSON NOT NULL,
    reason_codes_json JSON NOT NULL,
    evidence_json JSON NOT NULL,
    reviewer VARCHAR(96) NOT NULL DEFAULT 'system',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    KEY idx_opv_quality_review_lookup (revision_id, scope, target_id, input_fingerprint, created_at),
    CONSTRAINT fk_opv_quality_revision FOREIGN KEY (revision_id) REFERENCES opv_task_revision(revision_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
