---
name: hermes-product-analysis
description: |
  Hermes 选品与市场洞察 skill。用于从飞书标准化商品快照表消费数据，运行市场洞察 Agent、方向卡动态更新、direction_execution_brief、方向样本商品池、任务驱动选品 Agent，并支持周度“市场洞察 → brief ready → 选品评分”闭环。
---

# Hermes Product Analysis

## 当前定位

Hermes 现在按三层处理：

1. 上游标准化商品快照：由外部采集/标准化层完成，Hermes 只消费，不重新清洗。
2. Market Agent：从标准化快照生成市场报告、方向动态字段、`direction_execution_brief`、方向样本商品池。
3. Selection Agent：读取同批商品快照和最新可消费 brief，执行任务驱动选品评分，并把产品分配到任务池。

核心约束：

- 不实现抓取、主图补充、上架日期解析、价格标准化等 Layer 1 / Layer 2 能力。
- 所有流程必须按 `crawl_batch_id + market_id + category_id` 隔离。
- 市场报告通过 `direction_execution_brief` 影响选品，不直接给方向下所有商品加分。
- 如果同批 brief 不 ready，Selection Agent 先等待/重试；超过重试后才允许 previous brief 或 fallback brief，并打风险标记。

## 默认入口：传统选品分析

```bash
python3 /Users/likeu3/Desktop/skills/workspace/skills/hermes-product-analysis/run_pipeline.py validate-configs
python3 /Users/likeu3/Desktop/skills/workspace/skills/hermes-product-analysis/run_pipeline.py run-once
```

常用参数：

```bash
python3 /Users/likeu3/Desktop/skills/workspace/skills/hermes-product-analysis/run_pipeline.py run-once \
  --config-dir /Users/likeu3/Desktop/skills/workspace/skills/hermes-product-analysis/configs/table_configs \
  --limit-per-table 20
```

## 市场洞察入口

```bash
python3 /Users/likeu3/Desktop/skills/workspace-archive-20260419-131447/skills/hermes-product-analysis/run_pipeline.py market-insight-validate-configs
python3 /Users/likeu3/Desktop/skills/workspace-archive-20260419-131447/skills/hermes-product-analysis/run_pipeline.py market-insight-run \
  --table-id vn_fastmoss_hair_product_ranking \
  --limit-per-table 20 \
  --max-workers 1
```

## 标准化快照导入

从飞书标准化商品快照表导入同一批数据到 Market / Selection 各自原始快照表：

```bash
python3 /Users/likeu3/Desktop/skills/workspace-archive-20260419-131447/skills/hermes-product-analysis/scripts/import_standardized_snapshots.py \
  --feishu-url "<标准化商品快照表URL>" \
  --db-path /Users/likeu3/Desktop/skills/workspace-archive-20260419-131447/skills/hermes-product-analysis/artifacts/agent_runtime.sqlite3 \
  --agent both \
  --crawl-batch-id "<crawl_batch_id>" \
  --market-id MY \
  --category-id earrings
```

只消费满足以下条件的行：

- `data_status = ready_for_agents`
- `is_valid = true`
- `crawl_batch_id`、`market_id`、`category_id` 非空

## 周度 Selection Agent 触发预检

用于周二自动任务。这个脚本只做计划判断，不直接跑选品：

```bash
python3 /Users/likeu3/Desktop/skills/workspace-archive-20260419-131447/skills/hermes-product-analysis/scripts/plan_weekly_selection_runs.py \
  --feishu-url "<标准化商品快照表URL>" \
  --db-path /Users/likeu3/Desktop/skills/workspace-archive-20260419-131447/skills/hermes-product-analysis/artifacts/agent_runtime.sqlite3 \
  --retry-attempt 0
```

输出会说明每个 `crawl_batch_id + market_id + category_id`：

- 是否是新批次；
- 是否已成功跑过 Selection Agent；
- 同批 `batch_data_hash` 是否变化，需要重跑；
- 同批 market brief 是否 ready；
- 是否应该触发选品评分；
- 是否需要等待市场 brief。

重试建议：

- 周二 10:00：`--retry-attempt 0`
- 周二 12:00：`--retry-attempt 1`
- 周二 15:00：`--retry-attempt 2`
- 周三 10:00：`--retry-attempt 3`

## 方向卡与 brief 回填 / 迁移

把已有最新方向卡补齐到 Agent DB 新格式：

```bash
python3 /Users/likeu3/Desktop/skills/workspace-archive-20260419-131447/skills/hermes-product-analysis/scripts/backfill_latest_direction_cards_to_agent_store.py
```

该脚本会：

- 检查最新 `market_direction_cards`；
- 将 brief 里的旧式中文 `direction_id` 规范成 `{market_id}__{category_id}__{direction_name}`；
- 补齐 `direction_action`、`task_type`、`target_pool`；
- 补齐 `product_selection_requirements`、`positive_signals`、`negative_signals`、`sample_pool_requirements`、`content_requirements`；
- 写入 `market_direction_report`、`market_direction_snapshot`、`direction_execution_brief`。

## 方向样本商品池

生成每个方向的头部 / 新品样本池：

```bash
python3 /Users/likeu3/Desktop/skills/workspace-archive-20260419-131447/skills/hermes-product-analysis/scripts/generate_direction_sample_pool.py
python3 /Users/likeu3/Desktop/skills/workspace-archive-20260419-131447/skills/hermes-product-analysis/scripts/sync_direction_sample_pool_to_feishu.py
```

样本池原则：

- 一行一个商品；
- Top 样本按商品级 `sales_7d` 排序；
- 代表新品必须基于 `product_age_days <= 90`；
- 必须保留图片、价格、7日销量、上架天数、FastMoss 链接；
- 不允许把方向层中位数、目标价格带、代表商品拼接字段冒充商品级字段。

## 周度闭环推荐排期

默认时区：`Asia/Shanghai`。

每周一：

- 11:00 前：上游标准化商品快照 ready；
- 11:30：Market Agent 检查新批次；
- 12:00-16:00：导入、市场洞察、方向动态字段、brief、样本池、飞书回写；

每周二：

- 10:00：Selection Agent 预检增量；
- 10:15：检查同批 brief 是否 ready；
- 10:30-12:00：选品评分、任务池分配、飞书回写；

每 4 周：

- 生成方向卡月度复盘任务；
- 只在人工确认后更新 stable fields；
- 未批准前继续使用上一版 stable fields + 本周 dynamic fields。

## 数据库职责

核心表：

- `agent_import_log`
- `market_raw_product_snapshot`
- `selection_raw_product_snapshot`
- `market_direction_report`
- `market_direction_snapshot`
- `direction_execution_brief`
- `direction_sample_pool`
- `product_selection_score`
- `product_task_pool_result`
- `human_override_log`
- `market_agent_run_log`
- `selection_run_log`
- `agent_run_lock`

`direction_execution_brief` 是市场洞察给选品 Agent 的核心输出。

## 选品任务池

Selection Agent 最终必须输出明确任务池，而不是只输出“推荐/观察/淘汰”：

- `test_product_pool`
- `content_baseline_pool`
- `new_winner_analysis_pool`
- `replacement_candidate_pool`
- `category_review_pool`
- `observe_pool`
- `head_reference_pool`
- `manual_review_pool`
- `eliminate`

brief 缺失时允许 fallback，但必须打：

- `brief_auto_generated`

同批 brief 不存在但用了最近可消费 brief 时必须打：

- `brief_from_previous_batch`

## 历史边界

- 只支持 `发饰` 和 `轻上装`
- 没有 `product_images` 不进入分析
- 人工类目优先，无人工类目才调 Hermes 识别
- Hermes 只能返回固定 JSON，不允许自由文本
- 低置信度、其他类目、无法判断统一落到 `待人工确认类目`

## 配置约定

- 每张表一份 JSON 配置，放在 `configs/table_configs/`
- 配置主体沿用 phase1 方案，并额外允许 `source.feishu_url` 或 `source.app_token + source.bitable_table_id`
- 业务规则细则见 `references/phase1_rules.md`
- 市场洞察输出结构建议见 `references/market_insight_output_design.md`
- 提示词在 `prompts/`
- 输出 schema 在 `schemas/`

## 验证命令

```bash
python3 -m py_compile \
  src/agent_data_store.py \
  src/direction_card_update.py \
  src/agent_status_writer.py \
  selection/weekly_incremental_trigger.py \
  scripts/plan_weekly_selection_runs.py

python3 -m unittest \
  tests.test_agent_data_store \
  tests.test_direction_card_update \
  tests.test_weekly_incremental_trigger \
  tests.test_standardized_snapshot \
  tests.test_product_selection_v2
```
