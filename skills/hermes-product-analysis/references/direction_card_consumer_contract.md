# 市场洞察 -> 选品评分接口契约

## schema

- schema_version: `2026-04-23.v1`
- contract source: [direction_card_consumer_contract.json](/Users/likeu3/Desktop/skills/workspace-archive-20260419-131447/skills/hermes-product-analysis/configs/direction_card_consumer_contract.json)

## 下游必读字段

- `direction_canonical_key`
- `direction_family`
- `direction_tier`
- `core_elements`
- `scene_tags`
- `target_price_bands`

## 可选字段

- `top_value_points`
- `decision_confidence`
- `top_forms`
- `top_silhouette_forms`
- `top_length_forms`
- `style_cluster` / `style_main`
- `default_content_route_preference`

## 语义规则

- `direction_tier` 只做解释和分层展示，不直接改单品分数。
- `top_value_points` 只做辅助理解和弱信号匹配，不参与方向拆分。
- `uncovered` 表示当前方向卡体系下找不到足够合适的方向，不等于商品本身市场不匹配。
- 当 `decision_confidence = low` 时，下游会降低 `market_match_score` 的有效上限，并在理由中标记 `direction_confidence_low`。

## fallback_strategy

- 缺少方向卡时，返回 `market_match_status = uncovered`。
- `uncovered` 时，`market_match_score = null`，`core_score` 仅使用可用维度重归一化，不按 0 分处理。
- 如果方向卡缺少 required fields，会触发 `schema_mismatch`：
  - matcher 会把 `decision_confidence` 下调为 `low`
  - `contract_warning` 会写入匹配结果
  - 下游仍允许继续评分，但会降低 confidence，并在必要时把商品送入人工复核/备用池

## on_schema_mismatch

- 旧方向卡缺字段时，不直接中断评分。
- 影响顺序：
  1. 标记 `contract_warning`
  2. 下调 `decision_confidence`
  3. 若无法形成有效匹配，则转为 `uncovered`
- schema 变化不应无声改分；任何 mismatch 都必须能在 SQLite 明细和 diff 报告里追到。
