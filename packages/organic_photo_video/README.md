# Organic Photo Video

TikTok 图文穿搭养号工作流的数据与方案包。内容资产是 5 张连续图组，发布载体是轻动态竖屏 MP4。

新增独立预设 **TH｜五套穿搭｜拆解首图**：P1 拆解 A 套，P2–P5 分别展示其他四套；候选不足时按实际 1–4 套生成，不重复凑数。使用与数据合同见 [多穿实现说明](docs/MULTI_LOOK_IMPLEMENTATION_20260904.md)。现有预设和 BGM/发布流程保持不变。

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

当前 V2 主链仅做技术检查：锚点生成并自动选取 → 补齐五图 → 文件解码/尺寸记录/哈希与冻结选择校验 → 渲染 → 成片媒体 QC → 回写成片。
不再调用锚点、单图、组图或成片独立视觉模型，不需要三轮人工放行。
`technical` 检查记录明确标注 `technical_only`、`visual_review_performed=false`，不伪造 human/model 审核通过。
技术完成不代表审美合格，也不代表授权发布；仍需操作者查看预览并明确勾选“确认发布”。
专项视觉诊断保留在 `scripts/run_visual_qa.py` 等独立工具，生产入口的 `--run-visual-qa` 已停用。

## 飞书图文养号生产入口

生产任务表沿用产品编码、生产预设、生成数量、执行、进度、
预览/成片、确认发布、审核、备注，并为 V2 增加审核方式、审核阶段、重试审核和系统管理的审核批次。
RDS 仍是唯一事实源，飞书记录 ID 只作为外部
幂等键。安装与巡检命令：

```bash
python3 scripts/ensure_feishu_task_table.py
python3 scripts/run_feishu_tasks.py --dry-run
python3 scripts/run_feishu_tasks.py
```

“生成数量”支持 1–9，V1 预设仍按原自动生成五图和视频流程执行。
V2 预设统一技术直通；同一商品的多套有效图包会按批次稳定轮换，失败重试不换包。
一行完成后如需再次生产，新增一行并重新填写数量，避免不同批次混组。

新增生产预设 `TH｜穿搭拆解首图｜均衡变体`：继续复用账号当前的 Look
穿搭模板库，P2 先生成并冻结真人、商品和完整穿搭，P1 再以程序化方式合成
“真人上身 + 目标商品 + 搭配单品 + 泰文清单”的首图。首图不调用图片模型
生成文字，不渲染价格、店铺、订单或平台 UI；批量任务会稳定轮换四种版式、
四种文案和冷暖底色。若重做 P2，系统会自动重做整组，避免 P1 与后续图失配。

V2 旧“审核方式”“通过”“重试审核”不再驱动生产；需明确勾选“执行”启动或续跑。
已有 anchor_review/image_generating 等历史任务复用当前 revision 的已生成锚点与选中素材，无需新建任务。
缺少或失败的镜头可在下一次执行补齐；哈希变化、不可解码素材应指定镜头返工。未知在途生成/渲染禁止重复提交。
技术检查失败暂停并保留成果，同批其他任务可继续。已完成任务不会因为重复巡检再次生成或审核。
飞书“审核阶段”显示技术检查/技术完成；新版枚举请运行 `ensure_feishu_task_table.py` 同步，无新增 RDS 迁移。
历史三关操作文档 [飞书 V2 接入说明](docs/FEISHU_WORKFLOW_V2_20260903.md) 仅描述旧审核模式，不再是生产默认。

历史 V1 中已经停在“待审核”的记录现在保持等待，需明确选择“通过”续跑；
V1 新建任务原有的自动生成和渲染行为不变。

成片完成后勾选“确认发布”才会进入发布队列；旧字段“审核=排期发布”仅用于
兼容历史任务。排期 worker：

```bash
python3 scripts/run_publish_scheduler.py --dry-run
python3 scripts/run_publish_scheduler.py
```

worker 按账号本地发布窗口分配档位，在目标时间前 120 分钟内读取
NeoBund 对应国家的热点 BGM，选择成功后才提交定时任务；选歌失败不会
自动静音发布，而是保留任务并顺延档位。NeoBund 时间自动转换为北京时间。
macOS 用户级 LaunchAgent 已按 2 分钟扫描飞书、5 分钟处理发布队列运行。

共享商品图包同步（按 SKU/颜色导入全部 active 最新版本）：

```bash
python3 scripts/sync_product_reference_packs.py --product-id <产品编码> --apply
```

若共享表尚无该商品图包，可继续用 `scripts/upsert_product_reference_pack.py`
从运营确认的本地图片建包；系统不从单张图猜测或伪造其他颜色。
