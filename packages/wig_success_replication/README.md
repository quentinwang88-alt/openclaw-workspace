# Wig Success Replication — V1 Lite

独立领域包，负责“成功脚本 → 人工确认母版 → 人工选择产品 → 完整复刻提示词”。V1 Lite 不做产品匹配度评分；产品选入即代表运营已完成人工适配判断，程序只执行事实确认、禁止卖点、单变量、完整自包含与幂等检查。

固定模型合同：OpenAI Responses API、`gpt-5.6-sol`、`high`、原生 strict JSON Schema；不包含模型或 CLI 降级。导入包、运行测试和 `build_application()` 都不会执行 RDS migration、飞书写回或模型调用。

```bash
python3 -m wig_success_replication.schema_export
python3 -m pytest -q
```

MySQL migration 位于 `migrations/001_create_wsr_tables_mysql.sql`，必须由运维显式执行。
