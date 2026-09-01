# Organic Photo Video

TikTok 图文穿搭养号工作流的数据与方案包。内容资产是 5 张连续图组，发布载体是轻动态竖屏 MP4。

## 文件

- `docs/PRODUCT_SOLUTION.md`：产品与技术落地方案。
- `docs/DEVELOPMENT_PLAN_V1.md`：整合图组、视频化、NeoBund 热点 BGM、发布和数据闭环的完整开发方案。
- `docs/MODEL_HANDOFF.md`：供后续模型直接接手开发的背景、现状、合同、顺序与禁止事项。
- `migrations/001_create_opv_tables_mysql.sql`：MySQL 8 / RDS 幂等建表脚本。
- `scripts/apply_rds_migration.py`：带 SHA-256 二次确认的显式安装器。
- `domain/statuses.py`：任务/图片/渲染/发布/Outbox 状态机与受控迁移。
- `domain/models.py`：11 张 `opv_` 表的数据模型（UTC 时间、JSON 字段、Decimal/bool 转换）。
- `domain/contracts.py`：`opv-*-v1` 配置与 Plan/Shot/BGM JSON 合同校验（纯标准库）。
- `config/`：版本化 Market Pack、Theme、Render Preset JSON 与账号导入示例。
- `config/loader.py`：配置加载 + 合同校验入口。
- `repositories/rds_repository.py`：RDS 读写层，含幂等任务创建与受控状态迁移。
- `services/task_intake.py`：Stage A 幂等 Task Intake（含 idempotency key 推导合同）。
- `scripts/seed_reference_data.py`：Market Pack / Theme / Render Preset 种子脚本（默认 dry-run）。
- `scripts/import_account_profile.py`：账号资产绑定导入脚本（默认 dry-run，拒绝导入示例文件）。
- `scripts/preflight_task.py`：生成/发布前只读预检（不调用飞书）。
- `services/visual_qa.py`：单图与五图组视觉模型 QA 合同和门控。
- `scripts/run_visual_qa.py`：复用 creator-crm 视觉模型线路执行并落库 QA。
- `config/experiments/TH_BASELINE_6RUN_V1.json`：泰国基线 6 组试跑合同。
- `tests/`：标准库 unittest 套件（`python3 -m unittest discover -s tests`）。

## 建表

先预览迁移摘要：

```bash
python3 scripts/apply_rds_migration.py
```

再使用预览输出的完整 SHA-256 显式应用：

```bash
python3 scripts/apply_rds_migration.py --apply --confirm-sha256 <sha256>
```

脚本读取 `ORGANIC_PHOTO_VIDEO_DATABASE_URL`，未配置时复用 `LIKEU_AI_DATABASE_URL`。不会调用飞书 API，也不会创建种子任务。

## Phase 0 进度（2026-08-30）

已完成：

- 版本化配置：`MP_TH_DEFAULT_V1` 泰国 Market Pack、8 个泰国 Theme、`RP_STILL_VERTICAL_12S_V1` 12.5 秒渲染预设、账号导入合同与示例（示例禁止直接导入）。
- Domain：状态机常量与受控迁移、11 张表数据模型、`opv-*-v1` JSON 合同校验。
- Repository：覆盖全部 11 张表的读写；任务创建依赖 `uq_opv_task_idempotency` 幂等；状态迁移带 SQL 乐观守卫；BGM 仅按 `opv-bgm-v1` 合同存 `platform_metadata_json`。
- Task Intake：显式 key 或“账号+商品+来源+主题+选题+UTC 日期”推导 sha256 幂等键；要求商品参考图、激活的 Market Pack 和可解析的账号绑定。
- 82 个单元测试全部通过；测试不依赖真实 RDS，不调用飞书。

种子与导入（默认 dry-run，确认输出后加 `--apply`）：

```bash
python3 scripts/seed_reference_data.py            # 预览：1 pack + 8 themes + 1 preset
python3 scripts/seed_reference_data.py --apply    # 写入 RDS
python3 scripts/import_account_profile.py <账号.json>   # 校验账号绑定
python3 scripts/import_account_profile.py <账号.json> --apply
```

下一步（Phase 1）：~~Content Planner 输出 `opv-plan-v1`~~（已完成：`services/content_planner.py` + `services/asset_resolver.py` + `THEME_TH_TRAVEL_DEPARTURE_V1`，96 个测试通过，首个真实任务 `opv_task_20260830_a629e64d2dda` 已推进到 planned）、Hero-first 图片生产、单图/图组 QA 与人工审核接口。仍未解决：泰国试点账号、正式 Persona 选择、NeoBund 热点 BGM 真实字段抓取（见 `docs/MODEL_HANDOFF.md` 第 15 节）。

## 能力升级（产品驱动型图文内容）

统一入口：`services/content_story.py` 的 `generate_product_image_story(product_id, market, language, recipe_id, theme_id, variant_count)`。
内容配方（痛点解决/场景方案/视觉变化）、渲染与质检 profile、穿搭规则库见 `config/recipes/`、`config/profiles/`、`config/outfit/`。
首轮三配方验收与完整测试报告见 `docs/CAPABILITY_UPGRADE_SUMMARY_20260831.md`。
