-- Freeze batch intent before creating any child task. Runtime leases and
-- pending UI fields are coordination metadata, not creative revision inputs.
CREATE TABLE IF NOT EXISTS opv_production_batch (
    batch_id VARCHAR(64) PRIMARY KEY,
    source_type VARCHAR(64) NOT NULL DEFAULT 'feishu_opv',
    source_record_id VARCHAR(191) NOT NULL,
    expected_count INT NOT NULL,
    manifest_json JSON NOT NULL,
    pending_fields_json JSON NULL,
    batch_status VARCHAR(32) NOT NULL DEFAULT 'planned',
    lock_version INT NOT NULL DEFAULT 1,
    run_owner VARCHAR(96) NULL,
    lease_until DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_batch_source (source_type, source_record_id),
    KEY idx_opv_batch_lease (batch_status, lease_until)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
