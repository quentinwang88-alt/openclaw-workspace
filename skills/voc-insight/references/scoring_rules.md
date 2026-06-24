# voc-insight Scoring Rules

## 形态级样本门槛

按 `(product_count, voc_count)` 判定置信度，逐级递进：

| 置信度 | 商品数 | VOC 数 | 含义 |
|--------|--------|--------|------|
| `observe_only` | <3 **或** <10 | — | 只输出观察，不生成推荐卖点 |
| `partial_candidate` | ≥3 | ≥10 | 可用于 creator_brief / content_copy，**不进 ads/selection** |
| `form_candidate` | ≥5 | ≥30 | 可用于 content / creator / ads（ads 谨慎） |
| `ads_candidate` | ≥8 | ≥60 | 全 usecase 可用（ads_mixcut / content / creator / selection） |

> 取满足的最高档。任一维度不满足即降到下一档。`observe_only` 是兜底。

判定函数：`form_confidence(products, voc)`。

## 类目通用痛点门槛（跨形态升级）

一个信号要升级成 `category_candidate`，必须**同时**满足：

- 覆盖 **≥3** 个商品形态
- 覆盖 **≥10** 个商品
- 总 VOC **≥20**
- 单一形态贡献 **≤60%**
- 单一商品贡献 **≤30%**

不满足 80 VOC / 形态数 / 商品数 → `observe_only`（仍输出观察）；
满足样本量但有贡献偏态 → `partial_candidate` 并打 `single_form_bias` / `single_product_bias`。

判定函数：`category_confidence(...)`。

## classic / new 池

- 若批次只覆盖 `classic` 池 → 所有洞察加 `risk_notes: ["classic_only_evidence"]`，置信度不享受 new+classic 加成（V1 暂未做加成，仅打标）。
- new 池覆盖时同理可标 `new_only_evidence`（V1 预留）。

## 洞察类型映射

`signal_tag` → `insight_type` + `insight_role`（确定性映射，对齐上游 `fastmoss_voc_insight` 命名）：

| signal_tag | insight_type | insight_role | insight_id | usage_lane | ADS 主 hook |
|------------|--------------|--------------|------------|------------|-------------|
| `appearance_cute_color` | `selling_point` | `product_core_selling_point` | `selling_appearance_cute_color` | `video_hook` | ✅ |
| `hold_quality` | `selling_point` | `product_core_selling_point` | `selling_hold_quality` | `video_hook` | ✅ |
| `fast_shipping` | `selling_point` | `fulfillment_trust` | `selling_fast_shipping` | `copy_only` | ❌ 仅辅助信任标签 |
| `value_quantity` | `price_value` | `offer_selling_point` | `selling_value_quantity` | `copy_only` | ❌ 默认不进通用 hook；仅商品/offer 明确匹配时可人工采用 |
| `slow_shipping` | `fulfillment_issue` | `risk_guard` | `pain_slow_shipping` | `reject` | ❌ 只作风险 |
| `fulfillment_missing` | `fulfillment_issue` | `risk_guard` | `pain_fulfillment_missing` | `reject` | ❌ 只作风险 |
| 未知 tag | `selling_point`（正情绪）/ `pain_point`（负情绪） | `product_core_selling_point` / `risk_guard` | `selling_{tag}` / `pain_{tag}` | `video_support` / `reject` | 视视频可证明性 |

## Guardrails（强制内置）

1. **pain_point / fulfillment_issue 不得直接当卖点** → 商品级一律进 `risk_guards`，`recommended_usecases=[]`、`not_for_usecases=全部`。
2. **value_quantity / price_value 只给组合装/多件装**：商品 `product_pack_type` 必须是 `bulk_or_assorted_set`，否则 `skip`（reason: `value_quantity_requires_set_or_multipack`）。
3. **fulfillment_trust 不进 ADS 主 hook 池**：可作为履约/信任辅助标签，不作为通用产品卖点。
4. **offer_selling_point 不进通用 ADS 主 hook 池**：只有商品或 offer 明确是组合装/多件装时，才可由商品级/人工确认采用。
5. **形态不匹配**：`scope=form` 的洞察只推荐给同形态商品，跨形态 → `skip`（reason: `form_mismatch`）。
6. **样本低于 partial_candidate**（即 `observe_only`）的卖点洞察 → 不生成推荐，只出观察（reason: `low_sample_observe_only`）。
7. **单商品贡献过高**（类目级 >30%）→ 标 `single_product_bias`，置信度降到 `partial_candidate`。
8. **ADS 主 hook 必须可视频证明**：`usage_lane in (video_hook, video_support)` 且 `video_fit_score >= 70`，否则不进入 `voc_ads_hook_package`。
9. **视频脚本消费证明动作**：下游优先读取 `visual_proof_zh` / `required_action_zh` / `proof_shot_list`，`product_selling_point` 只作为来源说明。
8. **单形态贡献过高**（类目级 >60%）→ 标 `single_form_bias`，置信度降到 `partial_candidate`。
9. **类目通用痛点必须跨多个形态成立**（≥3 形态），否则不升级。
10. **商品无本批次 VOC 证据** → `quality_status=insufficient`，`skipped` 给出 `no_enriched_voc` 原因。

## usecase 适用范围

| 置信度 | recommended_usecases | not_for_usecases |
|--------|----------------------|------------------|
| `ads_candidate` | ads_mixcut, content_copy, creator_brief, selection | — |
| `form_candidate` | content_copy, creator_brief, ads_mixcut | — |
| `partial_candidate` | creator_brief, content_copy | ads_mixcut, selection |
| `category_candidate` + `product_core_selling_point` | ads_mixcut, content_copy, creator_brief, selection | — |
| `category_candidate` + `fulfillment_trust` | content_copy, creator_brief | ads_mixcut, selection |
| `category_candidate` + `offer_selling_point` | content_copy, creator_brief | ads_mixcut, selection |
| `observe_only` | — | 全部 |
| 风险型(fulfillment_issue/pain_point) | — | 全部（不进卖点） |

## 商品级推荐决策

对每个候选洞察按顺序判定 `decision`：

1. `form_mismatch` → `skip`
2. `observe_only` 卖点 → `skip`（low_sample）
3. `fulfillment_issue` / `pain_point` → `risk_guard`
4. `fulfillment_trust` 且 usecase=`ads_mixcut` → `skip`（只作辅助信任标签）
5. `price_value` 且非组合装 → `skip`
6. 商品自身无该信号证据 → `skip`（no_product_level_evidence）
7. 否则 → `secondary`；其中商品级证据数最多的 `product_core_selling_point` 提升为 `primary`

`quality_status`：
- 有 `primary` → `ok`
- 无 primary 但有 secondary/risk → `warning`
- 全无 → `insufficient`

## 候选选择（form vs category）

同一 `insight_id` 在 form 级和 category 级都可能存在时：
- form 级置信度 ≠ `observe_only` → 优先用 **form 级**（更具体）
- 否则回退到 **category 级**（避免薄形态掩盖强类目结论）
- 都没有则保留 form observe_only 观察（会被 guardrail 跳过并记录原因）

这保证 `assorted_clip_set` 这类 observe_only 形态的商品，仍能用到 `appearance_cute_color` 的类目级 `category_candidate` 结论。
