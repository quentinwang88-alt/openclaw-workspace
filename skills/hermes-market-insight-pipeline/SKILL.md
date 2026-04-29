---
name: hermes-market-insight-pipeline
description: "Run Hermes Market Insight against a Feishu ranking table: inspect schema, create or map market-insight config + direction-card output table, execute the pipeline, monitor progress via artifacts, and verify final report/output state."
author: Hermes Agent
---

# Hermes Market Insight Pipeline

Use this when:
- The user sends a Feishu ranking-table link and wants the **same Market Insight workflow as before**
- The work should stay inside the existing `hermes-product-analysis` project
- You need to create or reuse the direction-card output table and publish the report/doc

## Preferred project directory
Start here first:

```bash
/Users/likeu3/Desktop/skills/workspace-archive-20260419-131447/skills/hermes-product-analysis
```

Do not broaden search unless this path is missing.

## Relevant commands/files
Confirm these exist:
- `run_pipeline.py`
- `scripts/create_market_direction_table.py`
- `configs/market_insight_table_configs/*.json`
- `configs/market_insight_feishu_outputs/*.json`

Validate available market-insight configs:

```bash
python3 run_pipeline.py market-insight-validate-configs
```

## Required execution order
1. Inspect the source Feishu table fields and sample rows.
2. Determine country/category/input mode from live data, not guesswork.
3. Create or update a matching `market_insight_table_config` JSON.
4. Create or reuse a direction-card output table in the same Feishu app.
5. Create the matching `market_insight_feishu_output` JSON.
6. Run market insight.
7. Monitor via artifact progress JSON and output-table record count.
8. Verify final report/doc and output table state.

## Live inspection pattern
Use the project client against the exact Feishu URL to inspect fields and a few sample records. Look especially for:
- `国家/地区`
- `商品分类`
- `商品名称`
- `图片` / `商品图片`
- `7天销量`
- `7天销售额`
- `TikTok商品落地页地址` / `FastMoss商品详情页地址`
- `预估商品上架时间`

This is the fastest way to infer:
- market = `TH` / `VN`
- category = e.g. `时尚配件` -> `hair_accessory`
- mode = typically `product_ranking`

## Config creation pattern
If the incoming table does not match an existing config, create a new JSON in:

```bash
configs/market_insight_table_configs/<table_id>.json
```

Typical FastMoss hair ranking config:
- `input_mode`: prefer `auto` when the live field names already match the standard ranking schema
- `source_scope`: `official` for full production runs
- `default_country`: inferred from live records, e.g. `TH` / `MY` / `VN`
- `default_category`: `hair_accessory` when `商品分类` is `时尚配件` and samples are clearly hair accessories
- `source_currency`: inferred from live price prefix, e.g. `THB` for `฿`, `MYR` for `RM`
- `price_to_cny_rate`: fetch live FX when needed instead of guessing
- `price_band_step_rmb`: `5` for low-ticket hair accessories
- `price_scale_divisor`: `1`

Preserve the standard `field_map` used by existing FastMoss ranking configs.

## Direction-card output table creation
Create/reuse the output table with:

```bash
python3 scripts/create_market_direction_table.py \
  --feishu-url '<SOURCE_FEISHU_URL>' \
  --table-name '<Human readable output table name>'
```

Record from the command output:
- `table_id`
- `view_id`
- `open_url`
- created/skipped fields

Then create an output config in:

```bash
configs/market_insight_feishu_outputs/<output_id>.json
```

Typical shape:

```json
{
  "output_id": "th_hair_accessory_direction_cards",
  "country": "TH",
  "category": "hair_accessory",
  "latest_key": "TH__hair_accessory",
  "purge_target_scope": true,
  "target": {
    "feishu_url": "<output_table_open_url>"
  }
}
```

## Running the pipeline
If the user requires a full official run, do **not** do a smoke test first and do **not** pass `--limit-per-table`.

Preferred full-run command:

```bash
python3 run_pipeline.py market-insight-run \
  --table-id <TABLE_ID> \
  --max-workers 4 \
  --source-scope official \
  --feishu-output-config <OUTPUT_CONFIG_PATH> \
  --sync-every-completions 1
```

Only use a smaller smoke test when the user explicitly allows it.

## Important runtime lesson: avoid silent long hangs
A direct foreground/background tool launch may appear stuck with no logs. The observed reliable approach was:
- run detached with `execute_code` / `subprocess.Popen(..., start_new_session=True)`
- set `PYTHONUNBUFFERED=1`
- set `HERMES_PRODUCT_ANALYSIS_TIMEOUT_SECONDS=60` so per-item Hermes calls fail fast instead of stalling the whole run
- then monitor artifact progress JSON plus output-table record count

Detached launcher pattern:

```python
import os, subprocess
cmd = [
  'python3', 'run_pipeline.py', 'market-insight-run',
  '--table-id', '<TABLE_ID>',
  '--max-workers', '4',
  '--source-scope', 'official',
  '--feishu-output-config', '<OUTPUT_CONFIG_PATH>',
  '--sync-every-completions', '1',
]
env = os.environ.copy()
env['PYTHONUNBUFFERED'] = '1'
env['HERMES_PRODUCT_ANALYSIS_TIMEOUT_SECONDS'] = '60'
with open('/tmp/market_insight.log', 'ab', buffering=0) as f:
    subprocess.Popen(cmd, cwd=PROJECT_DIR, env=env, stdout=f, stderr=subprocess.STDOUT, start_new_session=True)
```

## Monitoring progress
The best indicators are:
1. `artifacts/market_insight/<COUNTRY>__<CATEGORY>/<run_id>/market_insight_progress.json`
2. output-table record count
3. final `market_insight_report_delivery.json`

Progress JSON fields to watch:
- `completed_product_count`
- `total_product_count`
- `direction_count`
- `run_status`
- `report_doc_url`
- `notification_status`

Even if no live stdout is visible, the run is healthy if `completed_product_count` increases.

## Verification checklist
At the end verify all of these:
- `run_status == "completed"`
- `completed_product_count == total_product_count`
- latest index file exists:
  - `artifacts/market_insight/latest/<COUNTRY>__<CATEGORY>.json`
- `report_doc_url` is populated
- output table matches final cards

Read these files:
- `market_insight_progress.json`
- `market_direction_cards.json`
- `market_insight_report.md`
- `market_insight_report_delivery.json`

## Important pitfall: stale partial output rows
During iterative test runs, a smoke test may leave rows in the direction-card table that do not match the final full run.

If final artifacts show `market_direction_cards.json` is empty or differs from the table contents, clean the target-scope rows (`国家 == target country` and `类目 == target category`) using the Feishu client from `src.market_insight_feishu_sync.resolve_target_client`, then re-check record count.

Do not use `src.feishu.build_bitable_client` for batch delete helpers; the batch delete method is available on the sync client path used by market-insight Feishu sync.

## Reporting format
Default brief reply:
- `table_id`
- whether it ran successfully
- direction-card count
- report doc URL

If the user asks for more detail, optionally add:
- matched source market/category/mode
- created config IDs / output table URL
- completed count / total count
- summary conclusion from `market_insight_report.md`
- any cleanup performed on stale output rows
