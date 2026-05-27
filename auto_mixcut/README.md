# AutoMixcut

TikTok Shop 商品智能混剪系统 V1.0 工程骨架。

This project implements a runnable local version of the V1.0 pipeline:

`anchor -> upload -> probe -> watermark -> segment -> frames -> AI tagging -> AI generated consistency -> effective_roles -> readiness -> render plan -> render -> quality gate -> Feishu preview sync`

## Architecture

- OSS: `LocalOSS` adapter in development; production can replace it with Aliyun OSS.
- RDS: SQLite repository in development; production schema is represented by `migrations/001_mysql_init.sql`.
- OpenClaw: `AutoMixcutOrchestratorAgent` coordinates state and delegates work to skills.
- LLM: all calls go through `llm_router_skill`; local tests use deterministic mock model responses.
- Media: real mode shells out to `ffprobe`/`ffmpeg`; tests use `AUTO_MIXCUT_MOCK_FFMPEG=1`.
- Feishu: workbench sync is represented by `feishu_sync_records`; production connector can map the same records into Bitable tables.

## Quick Start

```bash
cd /Users/likeu3/auto_mixcut
AUTO_MIXCUT_MOCK_FFMPEG=1 python3 -m auto_mixcut.cli init-db
AUTO_MIXCUT_MOCK_FFMPEG=1 python3 -m auto_mixcut.cli create-task --product-id VN_HAIR_001 --name "Pearl bow hair clip" --market VN --category hair_accessories --count 2
AUTO_MIXCUT_MOCK_FFMPEG=1 python3 -m auto_mixcut.cli anchor --product-id VN_HAIR_001
AUTO_MIXCUT_MOCK_FFMPEG=1 python3 -m auto_mixcut.cli confirm-anchor --product-id VN_HAIR_001
```

Upload local素材 after anchor confirmation:

```bash
AUTO_MIXCUT_MOCK_FFMPEG=1 python3 -m auto_mixcut.cli upload --product-id VN_HAIR_001 --file /path/to/asset.mp4
AUTO_MIXCUT_MOCK_FFMPEG=1 python3 -m auto_mixcut.cli batch --product-id VN_HAIR_001
```

## Implemented Skills

- `product_anchor_skill`
- `oss_storage_skill`
- `rds_repository_skill`
- `media_probe_skill`
- `watermark_detect_skill`
- `segment_skill`
- `frame_sample_skill`
- `llm_router_skill`
- `ai_tagging_skill`
- `ai_generated_consistency_skill`
- `effective_role_skill`
- `golden_benchmark_skill`
- `readiness_check_skill`
- `render_plan_skill`
- `render_skill`
- `quality_gate_skill`
- `feishu_review_skill`
- `cleanup_skill`

## Notes

The current implementation is deliberately adapter-first. It already enforces the important V1.0 rules locally:

- confirmed anchors are required before material upload;
- low trust / risky source watermark hits are rejected before segmentation;
- readiness and render plan read `effective_roles_json`, not raw AI roles;
- low trust segments cannot become hero/detail/result by default;
- outputs write `output_segments` lineage and OSS object rows;
- all model calls are recorded in `llm_calls`.
