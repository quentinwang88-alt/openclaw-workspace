---
name: creator-monitoring-openclaw
description: |
  Run the Creator Monitoring Assistant pipeline from OpenClaw, especially for
  historical week backfills plus a final refresh of the Feishu current-action
  table. Use this when the user wants OpenClaw to import one or more weekly
  Excel files, including files sent in Feishu chat and landed in
  ~/.openclaw/media/inbound, rebuild metrics/tags in order, and only sync the
  final target week to Feishu. This is the right skill for requests like
  “补前两周”, “刷新当前动作表”, “读取我刚在飞书里发的文件”, and cases where
  the user specifies a 店铺 name.
---

# Creator Monitoring OpenClaw

Use this skill when the user wants OpenClaw to execute the full creator-monitoring
pipeline instead of typing several shell commands manually.

Default project root:

- `<repo>/skills/creator-monitoring-assistant`

Default store setup:

- `platform=tiktok`
- `country=th`
- `store=泰国服装1店`

## What this skill does

1. Runs one or more weekly Excel imports in chronological order
2. Disables Feishu sync for history backfill weeks
3. Refreshes one chosen final week into Feishu current-action table
4. If the final sync week is already in the database, it can refresh from DB
   without requiring that week's Excel again
5. Runs a final Feishu repair pass so stale older-week rows are removed and the
   table ends with only the latest current-action rows

## How to run

Use the bundled script:

`scripts/run_backfill.py`

Required inputs:

- one or more `--week-file WEEK=/absolute/path.xlsx`
  or `--week-file WEEK=原始文件名.xlsx`
- `--store`
- `--sync-week WEEK`

If `--sync-week` is one of the imported weeks, the script will reuse that file.
If not, the script will try these fallbacks in order:

1. use `--sync-file` if provided
2. if the target week already exists in the database, refresh Feishu directly
   from the database
3. otherwise fail and ask for the missing final-week file

When the value after `WEEK=` is not an existing local path, the script tries to
resolve it from:

- `/Users/likeu3/.openclaw/media/inbound`

This is the preferred mode when the file was sent in Feishu chat.

## Example

```bash
python3 <repo>/skills/creator-monitoring-openclaw/scripts/run_backfill.py \
  --week-file "2026-W10=Transaction_Analysis_Creator_List_20260302-20260308.xlsx" \
  --week-file "2026-W11=Transaction_Analysis_Creator_List_20260309-20260315.xlsx" \
  --platform tiktok \
  --country th \
  --store "泰国服装1店" \
  --sync-week 2026-W13
```

## Operating rules

- Always run history weeks with `FEISHU_ENABLE_SYNC=false`
- Only the final chosen sync week should use `FEISHU_ENABLE_SYNC=true`
- Keep week order chronological
- Prefer inbound attachment resolution over guessing from `Downloads`
- If the user says “补前两周并刷新当前表”, interpret that as:
  history backfill first, final latest-week sync last
- If the latest week already exists in the database, do not ask the user to
  resend that Excel unless the DB lookup fails
- If the file came from Feishu chat, resolve it from `~/.openclaw/media/inbound`
  and do not sweep unrelated local folders
- After the final sync, always run the repair step so the Feishu table is
  collapsed back to the latest week only

## Notes

- The project reads Feishu app credentials from environment variables or
  `~/.openclaw/openclaw.json`
- If you want this skill to target a dedicated current-action table, set
  `CREATOR_MONITORING_FEISHU_APP_TOKEN` and
  `CREATOR_MONITORING_FEISHU_TABLE_ID` in your local `.env`
- The creator-monitoring shared database is now:
  `/Users/likeu3/.openclaw/shared/data/creator_monitoring.sqlite3`
- The current-action table uses:
  `record_key = platform + ":" + country + ":" + store + ":" + normalized_creator_name`
