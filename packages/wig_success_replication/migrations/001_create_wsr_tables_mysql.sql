-- V1 Lite persistence. Apply explicitly; importing the Python package never migrates RDS.
CREATE TABLE IF NOT EXISTS wsr_mother (
    mother_id VARCHAR(96) PRIMARY KEY,
    feishu_record_id VARCHAR(191) NOT NULL,
    name VARCHAR(255) NOT NULL,
    source_product_id VARCHAR(191) NOT NULL,
    current_version INT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_wsr_mother_feishu_record (feishu_record_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS wsr_mother_version (
    mother_id VARCHAR(96) NOT NULL,
    version INT NOT NULL,
    feishu_record_id VARCHAR(191) NOT NULL,
    name VARCHAR(255) NOT NULL,
    source_product_id VARCHAR(191) NOT NULL,
    source_script LONGTEXT NOT NULL,
    source_hash CHAR(64) NOT NULL,
    draft_json JSON NOT NULL,
    review_json JSON NOT NULL,
    contract_json JSON NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (mother_id, version),
    UNIQUE KEY uq_wsr_mother_source_hash (mother_id, source_hash),
    KEY idx_wsr_mother_version_status (status),
    CONSTRAINT fk_wsr_mother_version_mother FOREIGN KEY (mother_id) REFERENCES wsr_mother(mother_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS wsr_product_fact_version (
    product_id VARCHAR(191) NOT NULL,
    version INT NOT NULL,
    source_hash CHAR(64) NOT NULL,
    fact_json JSON NOT NULL,
    human_summary TEXT NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (product_id, version),
    UNIQUE KEY uq_wsr_product_source_hash (product_id, source_hash),
    KEY idx_wsr_product_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS wsr_replication_batch (
    batch_id VARCHAR(64) PRIMARY KEY,
    idempotency_key CHAR(64) NOT NULL,
    mother_id VARCHAR(96) NOT NULL,
    mother_version INT NOT NULL,
    product_ids_json JSON NOT NULL,
    special_requirements TEXT NULL,
    status VARCHAR(32) NOT NULL,
    summary TEXT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    completed_at DATETIME(6) NULL,
    UNIQUE KEY uq_wsr_batch_idempotency (idempotency_key),
    KEY idx_wsr_batch_mother (mother_id, mother_version),
    CONSTRAINT fk_wsr_batch_mother_version FOREIGN KEY (mother_id, mother_version)
        REFERENCES wsr_mother_version(mother_id, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS wsr_replication_batch_product (
    batch_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(191) NOT NULL,
    relationship VARCHAR(32) NOT NULL,
    product_fact_version INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    error_detail TEXT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (batch_id, product_id),
    KEY idx_wsr_batch_product_status (status),
    CONSTRAINT fk_wsr_batch_product_batch FOREIGN KEY (batch_id) REFERENCES wsr_replication_batch(batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS wsr_replication_prompt (
    prompt_id VARCHAR(64) PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    mother_id VARCHAR(96) NOT NULL,
    mother_version INT NOT NULL,
    product_id VARCHAR(191) NOT NULL,
    variant_type VARCHAR(64) NOT NULL,
    variant_key VARCHAR(64) NOT NULL,
    mutation_key VARCHAR(96) NOT NULL,
    change_summary TEXT NOT NULL,
    full_prompt LONGTEXT NOT NULL,
    prompt_hash CHAR(64) NOT NULL,
    review_status VARCHAR(32) NOT NULL DEFAULT 'pending_review',
    feishu_record_id VARCHAR(191) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_wsr_prompt_variant (batch_id, product_id, variant_key, mutation_key),
    UNIQUE KEY uq_wsr_prompt_content (mother_id, mother_version, product_id, prompt_hash),
    KEY idx_wsr_prompt_review (review_status),
    CONSTRAINT fk_wsr_prompt_batch FOREIGN KEY (batch_id) REFERENCES wsr_replication_batch(batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS wsr_model_run (
    run_id VARCHAR(64) PRIMARY KEY,
    task_type VARCHAR(64) NOT NULL,
    entity_id VARCHAR(191) NOT NULL,
    model VARCHAR(96) NOT NULL,
    reasoning_effort VARCHAR(32) NOT NULL,
    schema_name VARCHAR(96) NOT NULL,
    schema_version VARCHAR(32) NOT NULL,
    provider_request_id VARCHAR(191) NULL,
    retry_count INT NOT NULL DEFAULT 0,
    duration_ms BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    error_summary TEXT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    KEY idx_wsr_model_run_entity (task_type, entity_id),
    KEY idx_wsr_model_run_status (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS wsr_feishu_outbox (
    outbox_id VARCHAR(64) PRIMARY KEY,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(191) NOT NULL,
    operation VARCHAR(96) NOT NULL,
    payload_json JSON NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    attempts INT NOT NULL DEFAULT 0,
    error_detail TEXT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_wsr_outbox_operation (aggregate_type, aggregate_id, operation),
    KEY idx_wsr_outbox_pending (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
