-- Explicitly apply after 001 + 002; never executed on Python import.
-- The migration runner must inspect columns/indexes before replaying ALTERs.
-- Historical purpose/cart are unknown until explicitly classified by an operator.
-- New generation always writes an explicit purpose and cart snapshot.
ALTER TABLE wsr_replication_batch
    ADD COLUMN publish_purpose VARCHAR(16) NOT NULL DEFAULT '未分类';
ALTER TABLE wsr_replication_batch
    ADD COLUMN cart_enabled TINYINT(1) NULL DEFAULT NULL;
ALTER TABLE wsr_replication_batch
    ADD COLUMN handoff_context_json JSON NULL;

ALTER TABLE wsr_replication_prompt
    ADD COLUMN publish_purpose VARCHAR(16) NOT NULL DEFAULT '未分类';
ALTER TABLE wsr_replication_prompt
    ADD COLUMN cart_enabled TINYINT(1) NULL DEFAULT NULL;
ALTER TABLE wsr_replication_prompt
    ADD COLUMN handoff_context_json JSON NULL;

ALTER TABLE wsr_replication_prompt DROP INDEX uq_wsr_prompt_sequence;
ALTER TABLE wsr_replication_prompt
    ADD UNIQUE KEY uq_wsr_prompt_sequence (mother_id,mother_version,product_id,publish_purpose,sequence_no);
ALTER TABLE wsr_replication_prompt DROP INDEX uq_wsr_prompt_content;
ALTER TABLE wsr_replication_prompt
    ADD UNIQUE KEY uq_wsr_prompt_content (mother_id,mother_version,product_id,publish_purpose,prompt_hash);

CREATE TABLE IF NOT EXISTS wsr_script_pool_binding (
    prompt_id VARCHAR(64) PRIMARY KEY,
    target_record_id VARCHAR(191) NULL,
    last_exported_hash CHAR(64) NOT NULL,
    script_id VARCHAR(191) NOT NULL DEFAULT '',
    metadata_json JSON NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_wsr_script_pool_target (target_record_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
