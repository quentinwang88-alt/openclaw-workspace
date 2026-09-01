-- OPV correctness upgrade 003.
-- Apply before deploying code that persists ContentShot.outfit_state_ref.
ALTER TABLE opv_content_shot
    ADD COLUMN outfit_state_ref VARCHAR(32) NULL;
