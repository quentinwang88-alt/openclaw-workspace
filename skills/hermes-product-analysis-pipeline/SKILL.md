---
name: hermes-product-analysis-pipeline
description: "Run the existing Hermes product-analysis project against a Feishu table using only the project directory: locate table_config, ensure output fields, then process in small writeback batches until pending rows are empty."
author: Hermes Agent
---

# Hermes Product Analysis Pipeline

Use this when:
- The user wants a Feishu table processed through the existing Hermes product-analysis project
- The user explicitly wants the current pipeline and table_config used
- The user does not want browser-cookie work, clientvars reverse-engineering, field-ID digging, or wide filesystem searches

## Allowed workspace
Default to only this directory unless the user says otherwise:

```bash
/Users/likeu3/Desktop/skills/workspace/skills/hermes-product-analysis
```

If that exact path does not exist, first look for the same project under the archived workspace path before doing any broader search:

```bash
/Users/likeu3/Desktop/skills/workspace-archive-*/skills/hermes-product-analysis
```

In the observed environment, the live usable directory was:

```bash
/Users/likeu3/Desktop/skills/workspace-archive-20260419-131447/skills/hermes-product-analysis
```

Do not search `~/.hermes`, `~/Library`, Chrome data, or the whole home directory when the request is scoped to this project. A narrow fallback search under `~/Desktop/skills/` is acceptable only when the default project path is missing.

## Required execution order

1. Only inside the project dir, locate:
   - `run_pipeline.py`
   - `scripts/run_until_done.py`
   - `scripts/ensure_output_fields.py`
   - `configs/table_configs/*.json`
2. Identify the matching `table_config` for the target Feishu table.
3. Run field check first.
4. Then run the batch pipeline.
5. Verify pending rows are now zero.

## Discovery command

```bash
python3 run_pipeline.py validate-configs
```

Use file search/read tools only inside the project directory to inspect configs.

## Field check / 补字段

Run:

```bash
python3 scripts/ensure_output_fields.py --feishu-url '<FEISHU_TABLE_URL>'
```

Interpretation:
- `created` non-empty → fields were added successfully
- all fields in `skipped` → fields already existed, proceed directly

## Batch run settings
When the user says small batches / single concurrency, use:

```bash
python3 scripts/run_until_done.py \
  --table-id <TABLE_CONFIG_ID> \
  --batch-size 10 \
  --max-workers 1 \
  --sleep-seconds 0
```

This script already handles:
- reading pending rows
- processing in rounds
- writing back after each batch
- stopping only when the queue is empty

## Runtime monitoring note
If `run_until_done.py` is launched as a background process, its JSON round logs may not stream incrementally through the process tool and can appear only after exit due to buffering. In that case, do not abandon the run or switch to Feishu reverse-engineering. Keep the batch process running and verify progress by either:
- checking for active child `hermes chat -Q --source tool` processes, and/or
- periodically re-counting pending rows with the project adapter.

## Verification
After completion, verify with the project’s own adapter instead of external reverse-engineering:

## Verification
After completion, verify with the project’s own adapter instead of external reverse-engineering:

```bash
python3 - <<'PY'
from pathlib import Path
from src.table_adapter import TableAdapter
adapter = TableAdapter()
configs = adapter.load_table_configs(Path('configs/table_configs'))
config = [c for c in configs if c.table_id == '<TABLE_CONFIG_ID>'][0]
client = adapter.get_client(config)
recs = adapter.read_pending_records(config, client, limit=None, record_scope='pending')
print(len(recs))
PY
```

Expected result: `0`

## Reporting format
Report back briefly with:
- matched config name / table_id
- field-check result (`created` vs `skipped`)
- round summaries
- total processed / completed / failed
- pending remaining

## Important lesson
If the user explicitly says use the existing Hermes product-analysis pipeline and do not reverse-engineer Feishu internals, do not fall back to clientvars/cookies/browser-data exploration unless the project scripts fail inside this directory and you need to explain the blocker first.
