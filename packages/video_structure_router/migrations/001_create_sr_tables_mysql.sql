CREATE TABLE IF NOT EXISTS sr_selection_run (
    selection_run_id VARCHAR(64) PRIMARY KEY,
    request_id VARCHAR(191) NOT NULL,
    consumer_flow VARCHAR(64) NOT NULL,
    product_code VARCHAR(191) NULL,
    target_country VARCHAR(32) NULL,
    category VARCHAR(128) NULL,
    product_type VARCHAR(128) NULL,
    direction_count INT NOT NULL,
    duration_seconds DOUBLE NULL,
    policy_version VARCHAR(64) NOT NULL,
    random_seed BIGINT NOT NULL DEFAULT 0,
    selection_status VARCHAR(32) NOT NULL,
    degraded_reasons_json JSON NULL,
    request_json JSON NOT NULL,
    input_snapshot_json JSON NOT NULL,
    data_snapshot_hash VARCHAR(64) NOT NULL,
    selected_count INT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_sr_selection_product (product_code),
    KEY idx_sr_selection_created (created_at),
    KEY idx_sr_selection_snapshot (data_snapshot_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS sr_direction_assignment (
    direction_assignment_id VARCHAR(64) PRIMARY KEY,
    selection_run_id VARCHAR(64) NOT NULL,
    direction_index INT NOT NULL,
    output_slot VARCHAR(32) NOT NULL,
    direction_role VARCHAR(64) NOT NULL,
    candidate_key VARCHAR(255) NOT NULL,
    source_kind VARCHAR(64) NOT NULL,
    source_run_id VARCHAR(128) NULL,
    cluster_id INT NULL,
    cluster_version VARCHAR(64) NULL,
    prototype_id VARCHAR(191) NULL,
    evidence_tier VARCHAR(64) NOT NULL,
    macro_family_key VARCHAR(255) NOT NULL,
    visual_archetype_key VARCHAR(512) NOT NULL,
    structure_contract_json JSON NOT NULL,
    selection_score DOUBLE NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_sr_direction_slot (selection_run_id, direction_index),
    KEY idx_sr_direction_cluster (source_run_id, cluster_id, cluster_version),
    KEY idx_sr_direction_family (macro_family_key),
    CONSTRAINT fk_sr_direction_selection
        FOREIGN KEY (selection_run_id) REFERENCES sr_selection_run(selection_run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS sr_application_binding (
    binding_id VARCHAR(64) PRIMARY KEY,
    selection_run_id VARCHAR(64) NOT NULL,
    direction_assignment_id VARCHAR(64) NOT NULL,
    consumer_flow VARCHAR(64) NOT NULL,
    consumer_run_id VARCHAR(191) NULL,
    record_id VARCHAR(191) NULL,
    product_code VARCHAR(191) NULL,
    script_id VARCHAR(191) NULL,
    content_id VARCHAR(191) NULL,
    video_prompt_id VARCHAR(191) NULL,
    production_video_id VARCHAR(191) NULL,
    application_stage VARCHAR(64) NOT NULL,
    metadata_json JSON NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_sr_binding_identity (
        direction_assignment_id,
        consumer_flow,
        consumer_run_id,
        application_stage
    ),
    KEY idx_sr_binding_script (script_id),
    KEY idx_sr_binding_content (content_id),
    KEY idx_sr_binding_video (production_video_id),
    CONSTRAINT fk_sr_binding_selection
        FOREIGN KEY (selection_run_id) REFERENCES sr_selection_run(selection_run_id),
    CONSTRAINT fk_sr_binding_direction
        FOREIGN KEY (direction_assignment_id) REFERENCES sr_direction_assignment(direction_assignment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
