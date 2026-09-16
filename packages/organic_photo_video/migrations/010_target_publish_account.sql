-- 目标发布账号：内容定位驱动的图文生产与发布（2026-09-15）。
-- ``account_id`` 仍是内部 OPV 生产资料外键；本列只承载真实 TikTok 投递账号。
-- 旧行为：NULL/空 ＝ 沿用店铺公共池，不受本轮账号隔离影响。

ALTER TABLE opv_content_task
    ADD COLUMN target_publish_account_id VARCHAR(191) NULL AFTER row_version,
    ADD KEY idx_opv_task_target_account (target_publish_account_id);
