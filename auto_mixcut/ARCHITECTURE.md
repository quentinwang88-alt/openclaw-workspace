# Auto Mixcut Architecture Guardrails

This document defines the production boundaries for the auto_mixcut factory.
It is intentionally short and operational: when changing the pipeline, preserve
these ownership rules first.

## Production Entry Points

Only these scripts are production entry points:

- `scripts/run_mixcut_task_scanner.py`: scans Feishu/RDS tasks and dispatches the next station.
- `scripts/run_mixcut_guard.py`: processes material, imports returns, advances render/QC.
- `scripts/run_ai_supplement_heartbeat.py`: the only default real-submit entry for AI supplement packages.
- `scripts/run_ads_mixcut_unattended.py`: transitional ADS wrapper. It may prepare packages and call guard, but must not directly submit by default.

All other scripts are station helpers, migrations, one-off repair tools, smoke tests, or legacy/debug utilities. They should not be called from the scanner unless promoted here.

## Ownership Rules

- Task state writes belong to scanner/guard/heartbeat and should move toward a single state reducer.
- Real AI video submission belongs to `run_ai_supplement_heartbeat.py` and the Feishu-consuming worker.
- Prompt package state normalization belongs to `AISupplementGatewaySkill`.
- Material eligibility belongs to `MaterialPolicySkill`.
- Rendering selection belongs to `RenderPlanSkill`, but it must consume material policy instead of duplicating source-risk rules.
- Final machine QC belongs to `QualityGateSkill`, but it must consume material policy for first-slot/source eligibility.

## Forbidden Defaults

- ADS full-run must not directly call `segment-package-worker` unless an explicit escape hatch is enabled.
- Guard must not directly submit AI packages unless an explicit escape hatch is enabled.
- Low-trust repost/competitor material must not be used as ADS first slot by default.
- Published/exposed material must not be reused for ADS mixcut by default.
- Scripts must not silently fall back from ADS hook templates to unrelated output modes without recording the fallback reason.

## Canonical Modules

- Domain constants: `auto_mixcut/domain/`
- Runtime flags: `auto_mixcut/config/factory_config.py`
- Prompt package state: `auto_mixcut/skills/ai_supplement_gateway_skill.py`
- Material policy: `auto_mixcut/skills/material_policy_skill.py`

## Environment Escape Hatches

Escape hatches are for emergency recovery and tests, not normal production:

- `AUTO_MIXCUT_ALLOW_DIRECT_SUBMIT=1`: allows ADS wrapper direct submit.
- `AUTO_MIXCUT_GUARD_SUBMIT_AI_PACKAGES=1`: allows guard direct submit.
- `AUTO_MIXCUT_ADS_ALLOW_LOW_TRUST_FIRST_SLOT=1`: allows low-trust ADS first-slot candidates.

Any new escape hatch must be added to `FactoryConfig` and covered by a guardrail test.
