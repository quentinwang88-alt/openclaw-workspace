---
name: hermes-market-direction-flow-audit
description: "Audit whether a Hermes product-analysis market-direction workflow already supports a category, and distinguish code/config support from actual generated artifacts."
author: Hermes Agent
---

# Hermes Market Direction Flow Audit

Use this when:
- The user asks whether the latest 市场方向卡 / market-direction-card flow already supports a category (e.g. earrings)
- The user wants to know whether support is merely configured in code or already produced real analysis outputs
- The request is scoped to the existing `hermes-product-analysis` project

## Workspace rule
Default to the Hermes product-analysis project only:

```bash
/Users/likeu3/Desktop/skills/workspace/skills/hermes-product-analysis
```

If missing, first use the archived workspace path:

```bash
/Users/likeu3/Desktop/skills/workspace-archive-*/skills/hermes-product-analysis
```

Observed usable path:

```bash
/Users/likeu3/Desktop/skills/workspace-archive-20260419-131447/skills/hermes-product-analysis
```

Do not broaden search outside the project unless the user explicitly asks.

## Audit goal
Always answer **both** questions separately:
1. Has the category been added to the latest workflow/config/code path?
2. Has the category actually produced artifacts / latest outputs yet?

These are different; do not conflate them.

## Required checks

### 1) Load relevant skill first
Load `hermes-product-analysis-pipeline` for project-path and workflow context.

### 2) Verify category registration
Inspect:
- `configs/market_category_profiles.yaml`
- `selection/category_resolution.py`
- `src/market_category_profile.py`

Confirm:
- category alias resolution exists
- market/category profiles exist for VN / TH / MY as applicable
- canonical direction IDs are namespaced like `<MARKET>__<category>__...`

### 3) Verify category base configs exist
Inspect for category-specific files:
- `configs/base/categories/<category>/directions.yaml`
- `configs/base/categories/<category>/scoring.yaml`
- `configs/base/categories/<category>/product_anchor_schema.yaml`
- `configs/market_insight_taxonomies/<category>_v1.json`
- `configs/markets/<MARKET>/<category>/profile.yaml`
- `configs/markets/<MARKET>/<category>/prompts.yaml`
- `configs/markets/<MARKET>/<category>/tag_dictionary.yaml`

### 4) Verify latest flow actually uses these configs
Inspect code paths that generate direction-card outputs:
- `src/market_insight_pipeline.py`
- `src/market_insight_aggregator.py`
- `src/market_insight_report_generator.py`
- `src/market_insight_writer.py`
- optional helper: `scripts/reaggregate_latest_market_insight.py`

Look for this chain:
- score products
- build direction cards
- apply decision layer
- generate report
- write artifacts including:
  - `market_direction_cards.json`
  - `direction_sample_pool.json`
  - `direction_sample_pool_diagnostics.json`
  - `market_insight_report.json/.md`
  - `artifacts/market_insight/latest/<MARKET>__<category>.json`

### 5) Verify tests
Run category-specific and routing tests with `unittest` if `pytest` is unavailable:

```bash
python3 -m unittest \
  tests.test_earrings_direction_config \
  tests.test_market_category_profile_loader \
  tests.test_category_registry \
  tests.test_category_resolution \
  tests.test_category_isolation \
  tests.test_earrings_scoring_config
```

Important lesson:
- In this environment `pytest` may be missing (`No module named pytest`), so fall back to `python3 -m unittest`.

### 6) Check artifact reality, not just code support
Inspect:
- `artifacts/market_insight/latest/*.json`
- `artifacts/market_insight/**/*<category>*`

Interpretation:
- If configs/tests/code exist but no `latest/*<category>*.json` or run directories exist, report:
  - **category support is implemented**
  - **actual market-direction artifacts have not yet been generated**

## Reporting template
Report in three sections:

### Conclusion
- Supported in latest flow: yes/no
- Actual artifacts found: yes/no

### Evidence
- key config files found
- key code path confirms
- tests passed / failed
- artifact paths found / missing

### Business interpretation
Use wording like:
- “代码层面已接入” / implemented in workflow
- “产物层面尚未出数” / no generated outputs yet

## Pitfalls
- Do not say “already added” based only on a category name appearing once.
- Do not say “already running” unless artifacts exist.
- Do not rely on git history here; this repo may not have useful tracked history in the current workspace snapshot.
- Prefer `unittest` over `pytest` when the latter is unavailable.
