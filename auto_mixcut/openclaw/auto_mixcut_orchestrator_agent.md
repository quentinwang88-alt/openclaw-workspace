# AutoMixcut Orchestrator Agent

This agent coordinates the AutoMixcut state machine and delegates concrete work to skills.

Responsibilities:

- Read current task state from RDS.
- Decide the next skill.
- Call skills and persist Result objects.
- Retry or fail isolated product/asset/segment/output units.
- Route all model calls through `llm_router_skill`.
- Sync Feishu workbench queues and clean preview records.

The agent must not directly execute FFmpeg, write scattered SQL, call models, upload OSS files, or process large files.
