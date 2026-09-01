-- One reusable content package owns all render versions for one task.
-- This intentionally fails if historical duplicates exist; inspect and
-- reconcile lineage explicitly before applying instead of deleting it here.

ALTER TABLE opv_content_package
    ADD UNIQUE KEY uq_opv_content_package_task (task_id);
