# voc-insight Schema

## RDS 连接

- 驱动：`pymysql`
- 默认读取环境变量 `VOC_INSIGHT_DATABASE_URL`，回退 `LIKEU_AI_DATABASE_URL`，再回退 `HERMES_AGENT_DATABASE_URL`
- 运行前需清除代理：`env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy`
- 跑 RDS 命令前请确认本机已 `pip install --user pymysql`

## 读取的现有表（Fastmoss VOC 证据层）

由上游 Fastmoss VOC 抓取/富化管线写入，voc-insight 只读。

### fastmoss_voc_export_batch
批次元数据。PK `batch_id`。关键字段：`market`、`category_key`、`classic_product_count`、`new_product_count`、`product_count`、`voc_count`、`quality_status`、`manifest_json`。
> 实测可能为空；缺失时从 `fastmoss_voc_insight_pack` + `fastmoss_voc_product_snapshot` + `fastmoss_voc_enriched` 聚合推导。

### fastmoss_voc_insight_pack
PK `insight_pack_id`，UQ `(batch_id, export_version)`。字段：`batch_id`、`market`、`category_key`、`quality_json`、`product_anchor_card_json`、`flow_issue_count`。一个 batch 一个 pack。

### fastmoss_voc_product_snapshot
PK `voc_product_snapshot_id`。字段：`batch_id`、`pool_type`(`classic`/`new`)、`market`、`category_key`、`fastmoss_product_id`、`product_title`、`category_path`、`sales_metric_json`。

### fastmoss_voc_raw
PK `voc_id`。原始 VOC 文本：`batch_id`、`fastmoss_product_id`、`voc_rank`、`voc_text`、`normalized_voc_text`、`voc_text_hash`、`pool_type`。

### fastmoss_voc_enriched ⭐ 核心证据
PK `enriched_voc_id`，UQ `(insight_pack_id, evidence_id)`。富化后的证据行，voc-insight 主要从这里聚合。
关键字段：
- `evidence_id`：证据唯一键，格式 `{batch_id}:{fastmoss_product_id}:{voc_rank 3位补零}`
- `fastmoss_product_id`、`batch_id`、`market`、`category_key`、`pool_type`
- `product_form`、`product_form_label`、`product_pack_type`(`single_or_unspecified` / `bulk_or_assorted_set`)
- `sentiment`(`positive`/`neutral`/`mixed`/`negative`)、`sentiment_score`
- `signal_tags_json`：JSON 数组，标签如 `appearance_cute_color`、`hold_quality`、`value_quantity`、`fast_shipping`、`slow_shipping`、`fulfillment_missing`
- `voc_text`、`translation_zh_hint`、`attribute_segments_json`、`product_style_tags_json`

### fastmoss_voc_product_form_summary
PK `form_summary_id`，UQ 见索引。每形态聚合：`batch_id`、`product_form`、`product_form_label`、`product_count`、`voc_count`、`sentiment_counts_json`、`pack_type_counts_json`、`style_tag_counts_json`、`top_signal_tags_json`(`[{tag,count}]`)、`product_ids_json`。

### fastmoss_voc_insight / fastmoss_voc_hook / fastmoss_voc_insight_evidence_map
上游已生成的 **batch/类目级** 洞察、本地化口播 hook、洞察↔证据映射。voc-insight 可读取但不依赖；本 skill 生成的是 **form 级** 与 **跨形态 category 级** 洞察，与上游 batch 级洞察互补。

### fastmoss_voc_flow_issue
数据质量/流程问题：`issue_id`、`severity`、`title`、`detail`。voc-insight 把严重 issue 透传到 run summary。

### fastmoss_voc_product_recommendation ⭐ 商品级回写
PK `id`，UQ `recommendation_id`。voc-insight 商品级结果写入此表。
- `recommendation_id`：`{batch_id}__{product_id}__{product_form}__{usecase}_voc_det_v1`（det_v1 后缀区别于上游 LLM 版 `ads_voc_reco_v0`，避免互相覆盖）
- `batch_id`、`insight_pack_id`、`product_id`、`market`、`category_key`、`mixcut_category`
- `quality_status`(`ok`/`warning`/`insufficient`)、`recommendation_status`(`det_generated`，待人工确认)
- `primary_selling_points_json`、`risk_guards_json`、`skipped_insights_json`（均 JSON 数组）
- `source_payload_json`：完整 run payload 快照

## 新增输出表（voc-insight 创建）

DDL 同时内嵌在 `scripts/run_voc_insight.py` 的 `DDL_SQL` 中，`--write` 时自动 `CREATE TABLE IF NOT EXISTS`。

### voc_insight_run
每次运行的元数据。

| 字段 | 类型 | 说明 |
|------|------|------|
| run_id | varchar(191) PK | `{batch_id}__{scope}__{key}__{UTC ts}` |
| batch_id | varchar(191) | |
| market | varchar(32) | |
| category_key | varchar(191) | |
| scope | varchar(32) | `category`/`form`/`product` |
| product_form | varchar(191) NULL | scope=form/product 时填 |
| product_id | varchar(191) NULL | scope=product 时填 |
| usecase | varchar(64) NULL | |
| params_json | longtext | CLI 参数快照 |
| status | varchar(64) | `running`/`completed`/`error` |
| started_at | varchar(64) | ISO8601 |
| finished_at | varchar(64) NULL | |
| summary_json | longtext | 计数/置信度/质量摘要 |
| error_message | longtext NULL | |
| created_at / updated_at | varchar(64) | |

索引：`idx_voc_run_batch(batch_id)`。

### voc_form_insight_artifact
每个 (run, product_form) 一行，`insight_payload_json` 存该形态全部洞察数组。

| 字段 | 类型 | 说明 |
|------|------|------|
| artifact_id | varchar(191) PK | `{run_id}__form__{product_form}` |
| run_id | varchar(191) | |
| batch_id | varchar(191) | |
| market | varchar(32) | |
| category_key | varchar(191) | |
| product_form | varchar(191) | |
| confidence_level | varchar(64) | 该形态置信度 |
| product_count | int | |
| voc_count | int | |
| insight_payload_json | json | `[{insight}, ...]` |
| evidence_refs_json | json | 该形态聚合证据引用 |
| created_at / updated_at | varchar(64) | |

索引：`uq_voc_form_artifact(run_id, product_form)`、`idx_voc_form_batch(batch_id, product_form)`。

### voc_category_insight_artifact
每个 run 一行，`insight_payload_json` 存跨形态升级后的类目通用洞察数组。

| 字段 | 类型 | 说明 |
|------|------|------|
| artifact_id | varchar(191) PK | `{run_id}__category` |
| run_id | varchar(191) UQ | |
| batch_id | varchar(191) | |
| market | varchar(32) | |
| category_key | varchar(191) | |
| confidence_level | varchar(64) | `category_candidate` / `observe_only` |
| covered_forms_json | json | 覆盖形态列表 |
| product_count | int | |
| voc_count | int | |
| insight_payload_json | json | `[{insight}, ...]` |
| evidence_refs_json | json | 跨形态聚合证据引用 |
| created_at / updated_at | varchar(64) | |

### voc_insight_polish
LLM 表达润色结果。下游 ADS hook 池必须同时过滤 `batch_id`、最新 `run_id`、`usecase`、`polish_status`、`claim_validation_status` 与 `hook_eligible`。

| 字段 | 类型 | 说明 |
|------|------|------|
| polish_id | varchar(191) PK | `{run_id}__{insight_id}__{scope}__{usecase}` |
| batch_id / run_id | varchar(191) | 必须用于定位最新批次结果 |
| insight_id / scope / scope_key | varchar | 原始洞察定位 |
| usecase | varchar(64) | 如 `ads_mixcut` |
| confidence / insight_type / insight_role | varchar(64) | 规则层字段，LLM 不得改写 |
| recommended_usecases_json / not_for_usecases_json | json | usecase 适用范围 |
| risk_notes_json / evidence_refs_json | json | 风险备注与证据引用 |
| hook_eligible | tinyint | 是否允许进入对应 usecase 的 hook 池 |
| claim_validation_status | varchar(32) | `passed` / `failed` / `pending` |
| claim_validation_notes_json | json | claim 二次校验原因 |
| hooks_json / reason_zh / raw_llm_json | json/text | LLM 润色结果 |

### voc_ads_hook_package
商品级 ADS 钩子候选包。由 `scripts/build_ads_hook_package.py` 创建/更新。

| 字段 | 类型 | 说明 |
|------|------|------|
| package_id | varchar(191) PK | `{batch_id}__{run_id}__{product_id}__{usecase}` |
| batch_id / run_id | varchar(191) | VOC 批次与类目 artifact run |
| product_id / product_form | varchar | 商品与形态 |
| usecase | varchar(64) | 目前主要为 `ads_mixcut` |
| readiness_status | varchar(64) | `ready_for_hook_package` / `smoke_ready_unconfirmed` / `needs_manual_confirmation` / `blocked` |
| manual_confirmation_status | varchar(64) | `confirmed_by_status` / `confirmed_by_insight_id` / `confirmed_by_text` / `pending` / `rejected` |
| hook_candidate_count | int | 候选卖点数量 |
| requested_hook_count | int | 本次确认后期望生成的钩子片段总数 |
| payload_json | json | 商品锚点、确认信息、候选卖点、类目表达参考 hook |
| created_at / updated_at | varchar(64) | |

正式投流混剪只读：

```sql
usecase = 'ads_mixcut'
AND readiness_status = 'ready_for_hook_package'
AND manual_confirmation_status LIKE 'confirmed%'
```

`smoke_ready_unconfirmed` 只能用于效果查看，不能进入生产投流链路。

## 字段约定

- 时间戳统一 ISO8601 字符串（与现有 voc 表一致，用 `varchar(64)`）。
- JSON 列用 MySQL 原生 `json` 类型（与 `fastmoss_voc_product_recommendation` 一致），pymysql 写入传 JSON 字符串即可。
- ID 用 `varchar(191)`，与现有 voc 表对齐。
- 字符集 `utf8mb4`。
