# Feishu Workbench Tables

V1.0 uses four short-lived workbench tables:

1. 商品内容任务表
2. 商品锚点卡确认队列
3. 人工复核队列表
4. 成片质检表

The implementation stores sync state in `feishu_sync_records`. Production Feishu connectors should map rows by `object_type` and `object_id`; video and image files stay in OSS and Feishu only receives signed preview URLs.
