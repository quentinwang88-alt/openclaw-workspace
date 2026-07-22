# voc-insight Output Contract

## 顶层结构

```json
{
  "batch_id": "FM_TH_HAIRCLIP_20260622_165607",
  "market": "TH",
  "category_key": "hair_clip",
  "scope": "category | form | product",
  "insight_pack_id": "FM_TH_HAIRCLIP_20260622_165607__voc_insight_pack_v0",
  "batch_meta": {
    "pool_types": ["classic"],
    "classic_only": true,
    "enriched_product_count": 35,
    "enriched_voc_count": 165,
    "snapshot_product_count": 40,
    "severe_flow_issues": []
  },
  "form_summaries": [ { "product_form": "...", "label": "...", "product_count": 7, "voc_count": 24,
                        "confidence": "partial_candidate", "top_signal_tags": [...] } ],
  "form_artifacts": [ FormArtifact ],
  "category_artifact": CategoryArtifact | null,
  "product_recommendation": ProductRecommendation | null,
  "run_id": "...",
  "written": false,
  "dry_run": true,
  "summary": { "forms": 11, "category_insights": 5, "product_reco_quality": "ok", "classic_only": true, "severe_issues": 0 }
}
```

- `scope=form` 且指定 `--product-form` 时，`form_artifacts` 只含该形态。
- `scope=product` 时，`form_artifacts` / `category_artifact` 仍会带（用于追溯候选来源），`product_recommendation` 是主输出。

## Insight 对象（统一结构）

每个洞察对象：

```json
{
  "insight_id": "selling_appearance_cute_color",
  "insight_type": "selling_point",
  "insight_role": "product_core_selling_point",
  "scope": "form | category",
  "scope_key": "basic_hair_clip | hair_clip",
  "title_zh": "款式好看、颜色/造型容易被认可",
  "local_voice": "สวย น่ารัก สีตรงใจ",
  "confidence": "partial_candidate | form_candidate | ads_candidate | category_candidate | category_ads_candidate | observe_only",
  "product_count": 5,
  "evidence_count": 22,
  "signal_tags": ["appearance_cute_color"],
  "recommended_usecases": ["creator_brief", "content_copy"],
  "not_for_usecases": ["ads_mixcut", "selection"],
  "risk_notes": ["classic_only_evidence", "value_quantity_requires_set_or_multipack"],
  "evidence_examples": ["คุณภาพของงาน: ตรงตามรูป ดี สี: สวย", "..."],
  "evidence_refs": ["FM_TH_HAIRCLIP_20260622_165607:1729659517276948599:001", "..."],
  "sentiment_counts": {"positive": 18, "mixed": 4},
  "pack_type_counts": {"single_or_unspecified": 5}
}
```

category 级洞察额外字段：
- `covered_forms`: `["basic_hair_clip", "bow_clip", ...]`

`insight_role` 取值：

| role | 含义 | ADS 主 hook |
|------|------|-------------|
| `product_core_selling_point` | 产品本身卖点，如颜值、款式、稳定性、质感 | 可进入 |
| `fulfillment_trust` | 发货快等履约信任信号 | 默认不进入 |
| `offer_selling_point` | 组合装、数量、价格值等 offer 信号 | 默认不进入；商品/offer 明确匹配后人工采用 |
| `risk_guard` | 风险/避坑提示 | 不进入 |
- `max_form_contrib`: 0.213
- `max_product_contrib`: 0.041

## FormArtifact

```json
{
  "product_form": "basic_hair_clip",
  "confidence_level": "partial_candidate",
  "product_count": 7,
  "voc_count": 24,
  "insights": [ Insight, ... ],
  "evidence_refs": [ "evidence_id", ... ]
}
```

## CategoryArtifact

```json
{
  "confidence_level": "category_candidate",
  "covered_forms": ["assorted_clip_set", "basic_hair_clip", "..."],
  "product_count": 35,
  "voc_count": 165,
  "insights": [ Insight, ... ],
  "evidence_refs": [ "evidence_id", ... ],
  "classic_only": true
}
```

## ProductRecommendation

```json
{
  "product_id": "1729659517276948599",
  "product_form": "basic_hair_clip",
  "product_pack_type": "single_or_unspecified",
  "usecase": "ads_mixcut",
  "quality_status": "ok | warning | insufficient",
  "coverage": {
    "form_confidence": "partial_candidate",
    "category_confidence": "category_candidate",
    "source": "form | category | mixed | none"
  },
  "primary_selling_points": [ RecoEntry ],
  "secondary_selling_points": [ RecoEntry ],
  "risk_guards": [ RecoEntry ],
  "skipped_insights": [ RecoEntry ]
}
```

### RecoEntry

```json
{
  "insight_id": "selling_appearance_cute_color",
  "insight_type": "selling_point",
  "title": "款式好看、颜色/造型容易被认可",
  "local_title": "สวย น่ารัก สีตรงใจ",
  "decision": "primary | secondary | risk_guard | skip",
  "reason": "matched: 4 product-level evidence rows",
  "confidence": "partial_candidate",
  "scope": "form | category",
  "scope_key": "basic_hair_clip | hair_clip",
  "product_form": "basic_hair_clip",
  "insight_role": "product_core_selling_point",
  "signal_tags": ["appearance_cute_color"],
  "evidence_count": 22,
  "product_count": 5,
  "product_evidence_count": 4,
  "evidence_refs": [ "evidence_id", ... ],
  "evidence_examples": [ "...", "..." ],
  "risk_notes": ["classic_only_evidence"],
  "selling_point": "款式好看、颜色/造型容易被认可"
}
```

## decision 取值

| decision | 含义 | 落点 |
|----------|------|------|
| `primary` | 主推卖点（商品级证据最多的一条） | primary_selling_points |
| `secondary` | 辅助卖点 | secondary_selling_points |
| `risk_guard` | 风险/避坑提示（pain/fulfillment） | risk_guards |
| `skip` | 不推荐，带 reason | skipped_insights |

## AdsHookPackage

```json
{
  "package_id": "{batch_id}__{run_id}__{product_id}__ads_mixcut",
  "batch_id": "FM_TH_HAIRCLIP_20260622_165607",
  "run_id": "FM_TH_HAIRCLIP_20260622_165607__category__all__20260623T072932Z",
  "product_id": "1735876226192016437",
  "product_form": "basic_hair_clip",
  "usecase": "ads_mixcut",
  "readiness_status": "ready_for_hook_package | smoke_ready_unconfirmed | needs_manual_confirmation | blocked",
  "manual_confirmation_status": "confirmed_by_status | confirmed_by_insight_id | confirmed_by_text | pending | rejected",
  "requested_hook_count": 3,
  "manual_confirmation": {
    "source": "feishu_task | cli | none",
    "raw_status": "已确认",
    "confirmed_insight_ids": ["selling_hold_quality"],
    "confirmed_texts": [],
    "target_hook_count": 3
  },
  "product_anchor": {
    "anchor_status": "confirmed",
    "anchor_summary": {
      "hard_anchors": ["..."],
      "display_anchors": ["..."],
      "key_visual_constraints": ["..."],
      "distortion_alerts": ["..."]
    }
  },
  "hook_candidates": [
    {
      "candidate_id": "1735876226192016437__selling_hold_quality__0",
      "insight_id": "selling_hold_quality",
      "insight_role": "product_core_selling_point",
      "hook_intent": "contrast_reveal",
      "requested_hook_count": 3,
      "product_selling_point": "夹住碎发或局部头发后更利落",
      "usage_lane": "video_hook",
      "video_fit_score": 80,
      "visual_proof_zh": "碎发或局部头发从松散到被夹住后更利落",
      "required_action_zh": "模特用发夹夹住侧边碎发或局部头发，手离开发夹后轻微转头，镜头看见整理后的利落状态",
      "proof_shot_list": ["侧边碎发或局部头发松散", "发夹夹住头发", "手离开后轻转头展示更利落"],
      "forbidden_claims": ["不要写防滑、全天稳固", "不要写不疼、不伤发"],
      "category_reference_hooks": ["..."],
      "merge_instruction": "Use category hooks only as expression references; final prompt must show the product anchors and VOC visual proof action."
    }
  ]
}
```

下游正式投流混剪只能读取：

```sql
usecase = 'ads_mixcut'
AND readiness_status = 'ready_for_hook_package'
AND manual_confirmation_status LIKE 'confirmed%'
```

## reason 枚举

- `matched: N product-level evidence rows`
- `form_mismatch: insight form=X != product form=Y`
- `low_sample_observe_only: no strong conclusion`
- `value_quantity_requires_set_or_multipack: product_pack=...`
- `risk_guard: fulfillment_issue is a risk/pain signal, not a selling point`
- `no_product_level_evidence: product has 0 VOC rows tagged ...`
- `no_enriched_voc: product has 0 enriched rows in this batch`

## 落库映射

| 输出 | RDS 表 | 说明 |
|------|--------|------|
| 顶层 run | `voc_insight_run` | run_id, status, summary_json |
| FormArtifact | `voc_form_insight_artifact` | 每 (run, form) 一行，`insight_payload_json`=insights 数组 |
| CategoryArtifact | `voc_category_insight_artifact` | 每 run 一行 |
| ProductRecommendation | `fastmoss_voc_product_recommendation` | `recommendation_id={batch}__{pid}__{form}__{usecase}_voc_det_v1`；`primary_selling_points_json` 存 primary+secondary（带 decision 字段）；`risk_guards_json`、`skipped_insights_json` 分别存；`source_payload_json` 存完整 reco；`recommendation_status=det_generated` 待人工确认 |
| AdsHookPackage | `voc_ads_hook_package` | 商品级 ADS 钩子候选包；正式投流只读 `ready_for_hook_package` |
