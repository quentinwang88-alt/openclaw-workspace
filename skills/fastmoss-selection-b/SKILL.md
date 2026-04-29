# FastMoss 选品方案 B

独立 B 服务入口，负责飞书批次表附件中转、本地 SQLite 归档、规则筛选、轻量选品工作台回写、Accio 协作、Hermes 终判和轻量复盘同步。

## 入口

```bash
python3 skills/fastmoss-selection-b/run_pipeline.py init-db
python3 skills/fastmoss-selection-b/run_pipeline.py run-once
python3 skills/fastmoss-selection-b/run_pipeline.py collect-accio
python3 skills/fastmoss-selection-b/run_pipeline.py sync-followup
```

## 必要配置

- `FASTMOSS_B_CONFIG_TABLE_URL`
- `FASTMOSS_B_BATCH_TABLE_URL`
- `FASTMOSS_B_WORKSPACE_TABLE_URL`
- `FASTMOSS_B_FOLLOWUP_TABLE_URL`

可选：

- `FASTMOSS_B_HERMES_COMMAND`
- `FASTMOSS_B_ACCIO_TIMEOUT_HOURS`
- `FASTMOSS_B_DATA_ROOT`

## 实现原则

- FastMoss 原始快照只进本地 SQLite
- 飞书只存批次状态、shortlist 决策信息、轻量复盘结果
- 所有回写由 B 服务统一完成
- 同一批次、同一商品、同一 work_id 都做幂等 upsert
- 选品工作台不放价格解析中间字段、规则调试字段、Accio/Hermes 原始明细
- 跟进复盘表只承接 `是否进入跟进 = true` 的商品
