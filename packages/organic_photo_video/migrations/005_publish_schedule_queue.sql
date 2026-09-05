-- Publish scheduling stays on opv_publish_record: no second source of truth.
ALTER TABLE opv_publish_record
    ADD COLUMN planned_publish_at DATETIME(6) NULL AFTER operator_name,
    ADD COLUMN submitted_at DATETIME(6) NULL AFTER planned_publish_at,
    ADD KEY idx_opv_publish_due (publish_status, planned_publish_at);
