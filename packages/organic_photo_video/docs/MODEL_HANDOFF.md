# Organic Photo Video：模型接手与开发上下文

> 用途：供后续 Codex、OpenClaw Agent 或其他开发模型快速了解背景并继续开发。
>
> 当前状态日期：2026-08-30。
>
> 本文是接手入口；产品细节参考 `PRODUCT_SOLUTION.md`，完整开发拆解参考 `DEVELOPMENT_PLAN_V1.md`。

## 1. 一句话任务定义

开发一条 TikTok 女装养号内容工作流：基于商品、账号 Persona、Look 和 Scene 生成 5 张连续一致的 9:16 穿搭图片，将图片渲染为约 12.5 秒竖屏短视频，在 NeoBund 发布阶段挂载目标账号可用的当地热点 BGM，发布后记录 24 小时数据，并把表现好的组合沉淀为 Successful Look。

## 2. 业务背景

用户经营 TikTok 女装及相关跨境业务，需要一套低成本的自然流内容实验层。

这条工作流的第一目标不是直接卖货，而是：

- 稳定养号和建立账号视觉风格。
- 低成本测试商品、穿搭、人物、场景、主题和音乐。
- 每个运营每天用约 30–60 分钟审核并发布 5–10 条内容。
- 将经过图片内容和自然流数据验证的组合升级为 AI 视频、挂车视频或广告素材。

长期闭环：

`图片内容探索 → 人工确认 → 自然流数据验证 → Successful Look → 视频放大`

## 3. 已确认且不要反复讨论的产品决策

### 3.1 内容与载体

- 核心内容资产是 5 张连续穿搭图片，不是视频脚本。
- TikTok 第一阶段以 MP4 视频形式发布，不使用 Photo Mode。
- MP4 由 FFmpeg 对静态图片做轻运动和转场，不调用高成本 AI 视频生成模型。
- 默认视频规格：1080 × 1920、9:16、30fps、约 12.5 秒、H.264。

### 3.2 业务边界

以下三条路线保持独立，不允许强行合表或共用任务状态机：

1. 现有 TikTok Shop 1:1 商品图包：服务商品展示。
2. 新增 Organic Photo Video：服务养号、自然流和低成本实验。
3. 现有轻量试穿视频：服务已验证 Look 的视频放大。

它们只共享底层能力：Product Truth、人物/Look/Scene 资产、图片模型、OSS、QA、FFmpeg、发布和数据基础设施。

### 3.3 图片生成

- 使用 Hero-first 策略。
- 只先生成 P1 Hero；P1 通过 QA 后才能生成 P2–P5。
- P1 是人物、商品、Look 和色调锚点。
- 后续图片必须引用锚点，不能五张图独立随机生成。
- Look 选择优先级固定为：`Successful Look > Look Template > AI Exploration`。
- 单图 QA 后必须再做跨图一致性 QA。

### 3.4 BGM

- 默认策略是 `platform_hot_bgm`。
- 正式 BGM 不烧入视频母版，而是在发布准备阶段通过 NeoBund 挂载 TikTok 平台原生热点音乐。
- 必须按“Organic 账号 + 国家 + 账号实际可用曲库”选歌，不能只按国家榜单。
- 保留 `embedded_bgm` 和 `no_bgm` 两种降级策略。
- 禁止本地正式 BGM 和平台音乐形成双音轨。
- 热点音乐应在计划发布时间前 30–120 分钟选择，不能在图片规划阶段锁死。

### 3.5 多国家

- 泰国、越南、墨西哥使用同一套工作流。
- 国家差异放进版本化 Market Pack，不复制代码和任务表。
- Persona 用于账号连续性，不绑定国籍刻板印象。
- 泰国用 `th-TH`，越南用 `vi-VN`，墨西哥用 `es-MX`。
- 越南至少区分北部和南部气候；墨西哥区分高原城市和热带/沿海气候。

### 3.6 数据与飞书

- RDS MySQL 是当前唯一事实源。
- 飞书 API 当前超额度，禁止本阶段创建飞书表、字段或进行持续同步。
- 飞书恢复后只作为运营工作台，不重新成为核心状态源。
- `opv_feishu_outbox` 默认 `holding`，没有明确切换前不得发送飞书 API 请求。

## 4. 当前已经具备的能力

### 4.1 TikTok 商品图包

位置：

`/Users/likeu3/.openclaw/workspace/skills/tiktok-fashion-image-pack`

可复用：

- Product Truth。
- gpt-image-2 图片生成。
- 单图 QA、反馈修图。
- 标题本地化思路。
- 图片上传和任务处理经验。

不能直接复用其业务编排：

- 当前主要输出 1:1 电商商品图。
- 场景图彼此独立生成。
- 没有 Hero 锚点和跨图一致性 QA。
- 不能把商品图包直接改名当作养号工作流。

### 4.2 轻量试穿视频与资产库

位置：

`/Users/likeu3/.openclaw/workspace/skills/lightweight-tryon-video`

现有 SQLite：

`/Users/likeu3/.openclaw/workspace/skills/lightweight-tryon-video/var/light_tryon.sqlite3`

已盘点资产：

- 25 个启用的 Styling/Look 模板。
- 5 个 Persona 模板，当前都处于 testing；其中只有 2 个有人物参考图。
- 2 个启用 Scene，每个当前只有 1 张参考图。
- 15 个 active confirmed visual plans。

开发注意：

- 可以复用资产和引用方式，但不要直接复用轻量视频任务表。
- 正式试点前，Persona 应补到每个正式人物至少 3–5 张稳定参考图。
- Scene 需要增加国家、城市、气候和账号适用范围。

### 4.3 图片和模型

- 图片生成：现有 gpt-image-2 多参考图 edit 路线。
- 规划、商品理解、视觉 QA：架构建议使用 gpt-5.6-terra。
- 认证和重试优先复用现有 Codex/OpenAI 路线，不创建新的模型认证体系。

### 4.4 视频和发布

- FFmpeg、媒体探测、OSS 和基础视频 QC 可从现有 auto_mixcut 体系复用。
- NeoBund 发布适配器位置：

`/Users/likeu3/.openclaw/workspace/skills/short-video-auto-publisher/app/neobund_publish.py`

- 当前代码已经支持 Organic 非带货视频发布，使用 `authType=2`。
- 当前 Organic commit Payload 还没有音乐参数。
- NeoBund 页面已具备对应国家热点 BGM 选择能力，但本地代码尚未接入。
- `short-video-auto-publisher/SKILL.md` 中“仅支持 Shoppable”的部分描述已经落后于代码，不应据此否定 Organic 能力。

## 5. 当前已经完成的工作

### 5.1 架构图

- SVG：`/Users/likeu3/.codex/visualizations/2026/08/30/01a0528f-4bb2-7503-87a6-0bf1ce301e6c/tiktok-organic-photo-architecture.svg`
- PNG：`/Users/likeu3/.codex/visualizations/2026/08/30/01a0528f-4bb2-7503-87a6-0bf1ce301e6c/tiktok-organic-photo-architecture.png`

颜色：绿色为现有复用，橙色为需要调整，蓝色为新增。

### 5.2 方案文档

- 产品方案：`packages/organic_photo_video/docs/PRODUCT_SOLUTION.md`
- 完整开发方案：`packages/organic_photo_video/docs/DEVELOPMENT_PLAN_V1.md`
- 本接手文档：`packages/organic_photo_video/docs/MODEL_HANDOFF.md`

### 5.3 RDS 表

RDS：

- MySQL 版本：8.0.36。
- 数据库：`likeu_ai_database`。
- 连接信息从 `LIKEU_AI_DATABASE_URL` 读取，严禁在日志、文档或提交中输出完整 URL 和密码。

迁移文件：

`packages/organic_photo_video/migrations/001_create_opv_tables_mysql.sql`

安装器：

`packages/organic_photo_video/scripts/apply_rds_migration.py`

迁移 SHA-256：

`359289c14a2c6e04fa6693e4b194cda425bc26758144e05c93e8b554d7f05a8b`

已经在 RDS 正式创建 11 张表，当前均为空表：

- `opv_account_profile`
- `opv_market_pack`
- `opv_theme_catalog`
- `opv_render_preset`
- `opv_content_task`
- `opv_content_shot`
- `opv_video_render`
- `opv_publish_record`
- `opv_metric_snapshot`
- `opv_look_feedback`
- `opv_feishu_outbox`

表结构只新增 `opv_` 前缀对象，没有修改现有业务表。内部外键已经验收。`opv_feishu_outbox.status` 默认值已验证为 `holding`。

### 5.4 尚未完成

- 没有把 Market Pack、Theme、Render Preset 种子写入 RDS（配置文件和 dry-run 脚本已就绪，等待用户确认后 `--apply`）。
- 没有导入真实试点账号绑定数据（导入合同、校验脚本和示例已就绪）。
- 没有 Content Planner（`opv-plan-v1` 合同与校验已实现，Planner 本体未实现）。
- 没有实现 Hero-first 图片生产。
- 没有实现图组 QA。
- 没有实现静态图视频渲染器。
- 没有 Orchestrator 编排层。
- 没有捕获 NeoBund 热点音乐 API 和 commit 音乐字段。
- 没有运行真实端到端试点任务。
- 没有创建或调整任何飞书表。

### 5.5 Phase 0 已完成（2026-08-30）

- 版本化配置：`config/market_packs/MP_TH_DEFAULT_v1.json`、8 个泰国 Theme（`config/themes/`）、`config/render_presets/RP_STILL_VERTICAL_12S_V1.json`、账号导入合同示例 `config/examples/account_profile_example.json`（示例带防护，禁止直接导入正式库）。
- `domain/statuses.py`：任务/Shot/渲染/发布/Outbox 状态机常量、受控迁移表、失败恢复入口。
- `domain/models.py`：11 张 `opv_` 表 dataclass 模型，UTC 朴素时间、JSON 字段序列化、Decimal→float、TINYINT→bool。
- `domain/contracts.py`：`opv-market-pack-v1` / `opv-theme-v1` / `opv-render-preset-v1` / `opv-account-profile-v1` / `opv-plan-v1` / `opv-bgm-v1` 校验（纯标准库，Python 3.9 兼容）。
- `config/loader.py`：配置加载即校验，坏文件在加载期失败。
- `repositories/rds_repository.py`：全部 11 张表读写；`create_task_idempotent` 依赖唯一键幂等；状态迁移带 Python + SQL 双重守卫（rowcount=0 报 `StaleStatusError`）；连接 URL 只从环境变量读取且不落日志。
- `services/task_intake.py`：幂等 Task Intake；显式 key 或“账号+商品+来源+主题+选题+UTC 日期”sha256 推导；强制商品参考图、active Market Pack、可解析账号绑定；任务快照含 `intake_context`。
- `scripts/seed_reference_data.py` / `scripts/import_account_profile.py`：默认 dry-run，`--apply` 才写库。
- `tests/`：82 个 unittest 全部通过，不依赖真实 RDS、不调用飞书。

不要把“表已建好”误认为“工作流已经开发完成”；同样，Phase 0 完成不等于图片/视频/发布链路可用。

## 6. 标准业务流程

### Stage A：Task Intake

输入：

- `account_id`
- `product_id`
- 商品参考图
- 可选 Theme 或运营要求

处理：

- 读取账号、Market Pack、Persona、Look、Scene 和 Render Preset。
- 调用 Product Truth。
- 把商品和资产保存为任务快照。
- 用 `idempotency_key` 防止重复任务。

### Stage B：Content Planning

输出结构化 Plan：

- Theme 与 Topic。
- Persona、Look、Scene。
- P1–P5 Storyboard。
- 当地语言标题、Caption、Hashtag 和屏幕短句。
- 视频时长、动效、转场和 `audio_policy`。

### Stage C：Hero-first Image Generation

1. 生成 P1 Hero。
2. QA 通过后生成 P2/P3。
3. 基于合格图片生成 P4/P5。
4. 每个槽位支持独立版本和局部重试。
5. 最后执行图组 QA。

### Stage D：Human Image Review

人工动作：

- 整组通过。
- 指定某张重生。
- 指定某张修图。
- 整组驳回。

V1 不允许机器绕过人工最终确认直接进入发布。

### Stage E：Still-image Video Render

默认时间轴：

- P1：0–2.2s，Hero + Topic Hook。
- P2：2.2–4.8s，完整 Look。
- P3：4.8–7.3s，生活状态。
- P4：7.3–9.5s，搭配细节。
- P5：9.5–12.5s，第二机位或总结。

输出标准母版并执行媒体和视觉 QC。

### Stage F：NeoBund BGM and Publish Preparation

1. 在发布时间前 30–120 分钟查询 Organic 账号可用热点音乐。
2. 按内容匹配、国家热度、节奏适配和近期去重评分。
3. 生成 Top 3，默认选择 Top 1。
4. 记录计划音乐。
5. 创建 NeoBund Organic 发布任务。
6. 回读实际音乐和发布任务 ID。

V1 选歌评分：

- 内容情绪匹配：35%。
- 当地热度：30%。
- 节奏/时长适配：20%。
- 账号近期去重：15%。

### Stage G：Metrics and Learning

- V1 至少记录 24h 播放、点赞、评论、收藏、分享和主页访问。
- 数据必须关联账号、商品、Theme、Look、Scene、Persona、视频和 BGM。
- 系统只生成 Successful Look 候选，最终沉淀需要人工确认。

## 7. 建议状态机

主状态：

`draft → planned → hero_generating → image_generating → image_review → rendering → video_review → publish_preparing → ready_to_publish → publishing → published → metrics_collected → archived`

任一阶段可进入 `failed`。

恢复规则：

- Hero 失败只重跑 Hero。
- 单张图片失败只增加对应 `shot_version`。
- 图组 QA 失败只处理不合格槽位。
- 视频失败复用图片重新渲染。
- BGM 查询失败进行策略降级，不重做图片和视频。
- 发布接口响应不明确时先回查，禁止直接重复提交。

## 8. 建议的数据合同

### 8.1 Plan JSON 最小结构

```json
{
  "schema_version": "opv-plan-v1",
  "market_pack": {
    "id": "MP_TH_DEFAULT",
    "version": 1,
    "country": "TH",
    "locale": "th-TH"
  },
  "theme": {
    "id": "THEME_PETITE_PROPORTION",
    "topic": "本次当地语言具体选题"
  },
  "persona": {
    "ref_id": "PERSONA_REF",
    "snapshot": {}
  },
  "look": {
    "ref_id": "LOOK_REF",
    "source_type": "look_template",
    "snapshot": {}
  },
  "scene": {
    "ref_id": "SCENE_REF",
    "snapshot": {}
  },
  "copy": {
    "title": "",
    "caption": "",
    "hashtags": [],
    "cover_text": ""
  },
  "audio_policy": {
    "strategy": "platform_hot_bgm",
    "fallback": "no_bgm"
  },
  "shots": []
}
```

### 8.2 Shot 最小结构

```json
{
  "slot_index": 1,
  "slot_role": "hero",
  "purpose": "完整穿搭与首屏吸引力",
  "duration_ms": 2200,
  "motion_preset": "slow_push",
  "transition_out": "short_dissolve",
  "overlay_text": "",
  "generation_prompt": "",
  "source_refs": []
}
```

### 8.3 BGM Selection 最小结构

MVP 暂时写入 `opv_publish_record.platform_metadata_json`：

```json
{
  "audio_strategy": "platform_hot_bgm",
  "selection_policy_version": "opv-bgm-v1",
  "country": "TH",
  "auth_id": "",
  "selected": {
    "music_id": "",
    "title": "",
    "rank": null,
    "selected_at": "",
    "score": null
  },
  "actual": {
    "music_id": "",
    "title": "",
    "confirmed_at": ""
  },
  "fallback_reason": null
}
```

不要在没有抓到 NeoBund 实际字段前，把示例中的字段名直接作为 API Payload。

## 9. 推荐代码结构

目标目录：

```text
packages/organic_photo_video/
├── config/
│   ├── market_packs/
│   ├── themes/
│   └── render_presets/
├── domain/
│   ├── models.py
│   ├── statuses.py
│   └── contracts.py
├── application/
│   └── orchestrator.py
├── services/
│   ├── product_truth_adapter.py
│   ├── asset_resolver.py
│   ├── content_planner.py
│   ├── image_generator.py
│   ├── image_qa.py
│   ├── video_renderer.py
│   ├── neobund_music.py
│   ├── neobund_publisher.py
│   └── metrics_evaluator.py
├── repositories/
│   └── rds_repository.py
├── migrations/
├── scripts/
└── tests/
```

MVP 使用 Adapter 包装现有能力，暂时不要大规模重构原 Skill。等 OPV 流程稳定后，再抽取真正通用的 Product Truth、Asset Provider、Image Service 和 QA 包。

## 10. 下一位开发模型的执行顺序

### 第一步：阅读和环境检查

1. 先阅读 `/Users/likeu3/.openclaw/workspace/AGENTS.md`。
2. 阅读本文件和 `DEVELOPMENT_PLAN_V1.md`。
3. 检查工作区未提交改动，保护用户现有文件。
4. 确认不得调用飞书 API。
5. RDS 只通过环境变量连接，不能输出凭据。

### 第二步：实现阶段 0 配置

先通过版本化 JSON 或 YAML 建立：

- 泰国默认 Market Pack。
- 8 个 Theme。
- 1 个 12.5 秒 Render Preset。
- 一个试点账号的资产绑定数据导入合同。

不要在缺少真实账号和 Persona 选择时虚构正式账号种子数据；可以提供示例或导入脚本。

### 第三步：实现最小代码骨架

优先顺序：

1. Domain models、状态常量和 JSON 合同校验。
2. RDS Repository。
3. Task Intake 和幂等创建。
4. Planner Adapter。
5. Hero-first Orchestrator。
6. 图片 QA 和人工审核接口。
7. FFmpeg Renderer。
8. NeoBund BGM 与 Publisher。
9. Metrics 和 Successful Look。

每一步都需要单元测试；不要一开始写一个巨大脚本串完整流程。

### 第四步：首个端到端切片

首个可验收切片应是：

`一条测试任务 → 生成 Plan → 生成/模拟 5 个 Shot → 人工通过 → 渲染 MP4 → 写入 RDS`

首个切片不要求立即真实发布。视频和 RDS 状态稳定后，再接 NeoBund 音乐和正式发布。

## 11. 开发阶段和完成定义

### Phase 0：配置与代码骨架

完成定义：

- 配置有 schema/version。
- Repository 可以读写 11 张 OPV 表。
- 任务创建幂等。
- 不依赖飞书。

### Phase 1：图组生产

完成定义：

- Planner 输出结构化 JSON。
- Hero 失败不会继续生成后续图片。
- 每个 Shot 可独立重试并保留版本。
- 图组通过机器初筛和人工确认后才能渲染。

### Phase 2：视频化

完成定义：

- 输出 1080 × 1920、10–15 秒 MP4。
- 无黑帧、损坏、字幕越界和双音轨。
- 时间轴、图片版本和输出哈希写入 RDS。

### Phase 3：NeoBund BGM 与发布

完成定义：

- 热点音乐来自目标 Organic 账号真实可用曲库。
- 选歌、降级和去重有测试。
- 发布结果不明确时不会重复创建任务。
- 保存计划音乐与实际音乐。

### Phase 4：数据反馈

完成定义：

- 24h 指标关联全部内容维度。
- 能输出 Successful Look 候选及证据。
- 自动候选不能绕过人工确认。

### Phase 5：飞书恢复

完成定义：

- 飞书从 RDS 同步，不反向控制核心状态。
- outbox 分批释放、幂等、限速、可断点续跑。
- 不因飞书失败阻塞图片和视频生产。

## 12. MVP 验收标准

- 同一输入重复调用不会生成重复任务。
- Hero 未通过时 P2–P5 不启动。
- 5 张图的人物、商品和 Look 跨图一致。
- 单张失败能局部重跑。
- 视频满足尺寸、时长、编码、字幕安全区和封面要求。
- NeoBund 使用账号实际可用的当地热点音乐。
- 没有本地 BGM 与平台 BGM 双音轨。
- NeoBund 响应不明确时先查询再决定是否重试。
- 24h 指标可以关联账号、商品、Theme、Look、Scene、Persona 和 BGM。
- 飞书不可用时整条生产链仍可运行。

## 13. 首轮试点范围

- 国家：泰国。
- 账号：1 个。
- Persona：1 个正式版本。
- 核心场景：2–3 个。
- Look：6–8 个。
- Theme：8 个。
- 首轮内容：20–30 条。
- 日发布：5–10 条。
- 数据观察：2–4 周后再决定是否自动生成 Successful Look 候选。

首轮优先验证：

- 账号视觉稳定性。
- Hero 首屏吸引力。
- 商品和人物跨图一致性。
- 运营审核时间。
- NeoBund 热点 BGM 获取与挂载成功率。
- Theme、Look、Scene、BGM 的自然流差异。

## 14. 重要风险与禁止事项

- 禁止调用飞书 API 建表、加字段或批量同步，直到用户明确说明额度恢复。
- 禁止把 Organic Photo Video 直接塞入现有商品图包或轻量试穿视频任务表。
- 禁止五张图独立生成后只靠 QA 补救。
- 禁止不支持的国家/语言静默回退成泰语。
- 禁止在本地烧入正式 BGM 后再挂平台音乐。
- 禁止猜测 NeoBund 音乐接口字段；必须先抓真实请求。
- 禁止在 NeoBund 返回空响应时直接重复提交发布任务。
- 禁止把未确认的 AI Look 自动写成 Successful Look。
- 禁止在日志、测试快照或文档中输出 RDS、NeoBund、OpenAI、飞书凭据。
- 禁止为了“统一”而进行超出 MVP 范围的大重构。

## 15. 尚需用户确认但不阻塞代码骨架的问题

- 泰国首个试点账号 ID。
- 5 个 Persona 中选择哪个作为正式 Persona，以及补充哪些参考图。
- 首轮 6–8 个 Look 和 2–3 个核心 Scene。
- 首轮商品输入来源。
- NeoBund 热点 BGM 页面对应的真实请求字段。
- 视频母版最终使用“无音频流”还是“静音 AAC 轨”，需结合 NeoBund 上传实测决定。
- 24h 数据由人工录入、NeoBund 回读还是 TikTok 数据接口获取。

这些问题不阻塞 Domain、Repository、配置 schema、Task Intake、Planner 合同和 Renderer 单元测试开发。

## 16. 可直接交给下一模型的任务描述

```text
继续开发 /Users/likeu3/.openclaw/workspace/packages/organic_photo_video。

先完整阅读：
1. /Users/likeu3/.openclaw/workspace/AGENTS.md
2. docs/MODEL_HANDOFF.md
3. docs/DEVELOPMENT_PLAN_V1.md
4. migrations/001_create_opv_tables_mysql.sql

当前 RDS 的 11 张 opv_ 表已经创建且为空，飞书 API 额度受限，禁止调用飞书。
不要重复建项目、重复建表或把工作流并入现有商品图包/轻量试穿视频。

下一开发目标是 Phase 0：建立版本化 Market Pack、Theme、Render Preset 配置 schema，
实现 Domain models、状态常量、RDS Repository 和幂等 Task Intake，并补齐单元测试。

必须保护用户现有改动，不输出任何连接凭据。完成后报告修改文件、测试结果、尚未解决的问题和下一步。
```
