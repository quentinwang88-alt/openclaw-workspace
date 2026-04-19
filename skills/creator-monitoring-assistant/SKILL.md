---
name: creator-monitoring-assistant
description: |
  达人经营监控助手 V1。导入每周达人经营 Excel，入库 PostgreSQL/SQLite，
  计算周指标、滚动指标和规则标签，并将结果串行同步到飞书单张“达人当前动作表”。
  适用于不依赖大模型的结构化周经营监控。
---

# Creator Monitoring Assistant

## 核心能力

当用户提供一份周报 Excel 时，运行这条流水线：

1. 导入 Excel 原始数据
2. 同步达人主数据
3. 生成 clean 层
4. 计算周指标、环比、4 周滚动与生命周期指标
5. 按规则生成主标签、风险标签、优先级和建议动作
6. 将结果同步到飞书单表，且不覆盖人工字段

## 运行入口

```bash
python3 skills/creator-monitoring-assistant/run_pipeline.py --init-db --stat-week "2026-W13" --source-file-path "/path/to/report.xlsx" --platform "tiktok" --country "th" --store "泰国服装1店"
```

已初始化数据库后，重复跑某一周：

```bash
python3 skills/creator-monitoring-assistant/run_pipeline.py --stat-week "2026-W13" --source-file-path "/path/to/report.xlsx" --platform "tiktok" --country "th" --store "泰国服装1店"
```

## 同步约束

- 飞书同步层必须串行写入同一张表
- `record_key = creator_key = platform + ":" + country + ":" + store + ":" + normalized_creator_name`
- 程序只更新系统字段
- 不覆盖 `负责人`、`跟进状态`、`人工备注`
- 即使误对历史周开启飞书同步，系统也只会把该店铺数据库里最新周写到飞书当前动作表
- 多周回补场景应优先使用 `creator-monitoring-openclaw`，不要直接把历史周逐周同步到飞书

## 配置

优先读环境变量：

- `DATABASE_URL`
- `FEISHU_APP_ID`
- `FEISHU_APP_SECRET`
- `FEISHU_APP_TOKEN`
- `FEISHU_TABLE_ID`
- `STORE_DEFAULT`
- `MIN_VALID_GMV`
- `MIN_VALID_ORDER_COUNT`
- `MIN_VALID_CONTENT_ACTION_COUNT`
- `MIN_VALID_SAMPLE_COUNT`

如果未显式提供飞书 app_id / app_secret，会尝试读取 `~/.openclaw/openclaw.json` 中的飞书配置。

默认数据库已统一到：

- `/Users/likeu3/.openclaw/shared/data/creator_monitoring.sqlite3`

不再推荐把正式数据写在 skill 工作区自己的 `data/` 目录里。
