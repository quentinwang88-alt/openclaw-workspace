-- V1.2 cumulative sequence slots, replication modes and creative signatures.
ALTER TABLE wsr_replication_prompt
    ADD COLUMN sequence_no INT NULL AFTER prompt_hash;

ALTER TABLE wsr_replication_prompt
    ADD COLUMN replication_mode VARCHAR(32) NULL AFTER sequence_no;

ALTER TABLE wsr_replication_prompt
    ADD COLUMN creative_route VARCHAR(64) NULL AFTER replication_mode;

ALTER TABLE wsr_replication_prompt
    ADD COLUMN creative_signature_json JSON NULL AFTER creative_route;

ALTER TABLE wsr_replication_prompt
    ADD COLUMN planner_version VARCHAR(32) NULL AFTER creative_signature_json;

UPDATE wsr_replication_prompt AS prompt
JOIN (
    SELECT
        prompt_id,
        ROW_NUMBER() OVER (
            PARTITION BY mother_id, mother_version, product_id
            ORDER BY created_at, prompt_id
        ) AS generated_sequence_no
    FROM wsr_replication_prompt
) AS ranked ON ranked.prompt_id = prompt.prompt_id
SET
    prompt.sequence_no = COALESCE(prompt.sequence_no, ranked.generated_sequence_no),
    prompt.replication_mode = COALESCE(
        prompt.replication_mode,
        CASE WHEN ranked.generated_sequence_no <= 3 THEN 'high_fidelity' ELSE 'general' END
    ),
    prompt.creative_route = COALESCE(
        prompt.creative_route,
        CASE ranked.generated_sequence_no
            WHEN 1 THEN 'H1'
            WHEN 2 THEN 'H2'
            WHEN 3 THEN 'H3'
            ELSE CONCAT('LEGACY-', ranked.generated_sequence_no)
        END
    ),
    prompt.variant_type = CASE ranked.generated_sequence_no
        WHEN 1 THEN 'high_fidelity_h1'
        WHEN 2 THEN 'high_fidelity_h2'
        WHEN 3 THEN 'high_fidelity_h3'
        ELSE prompt.variant_type
    END,
    prompt.creative_signature_json = COALESCE(
        prompt.creative_signature_json,
        JSON_OBJECT('legacy', TRUE, 'prompt_hash', prompt.prompt_hash)
    ),
    prompt.planner_version = COALESCE(prompt.planner_version, 'legacy-backfill-v1');

ALTER TABLE wsr_replication_prompt
    MODIFY COLUMN sequence_no INT NOT NULL,
    MODIFY COLUMN replication_mode VARCHAR(32) NOT NULL,
    MODIFY COLUMN creative_route VARCHAR(64) NOT NULL,
    MODIFY COLUMN creative_signature_json JSON NOT NULL,
    MODIFY COLUMN planner_version VARCHAR(32) NOT NULL;

ALTER TABLE wsr_replication_prompt
    ADD UNIQUE KEY uq_wsr_prompt_sequence (
        mother_id, mother_version, product_id, sequence_no
    );
