# AI 图文穿搭养号工作流整体开发方案 V1

## 1. 方案结论

系统生产的核心资产是“一组人物、商品、Look 和场景连续一致的穿搭图片”，TikTok 发布载体暂定为竖屏短视频，而不是 Photo Mode。

完整链路：

`商品 → 账号与市场规则 → Theme/Topic → Look/Persona/Scene → 5 张图组 → 单图与图组 QA → 静态图视频化 → NeoBund 原生热点 BGM → 发布 → 24h 数据 → Successful Look`

三条现有业务路线保持独立：

- TikTok Shop 1:1 商品图包：继续服务商品展示。
- Organic Photo Video：本方案新增，服务养号与自然流测试。
- 轻量试穿视频：继续服务已验证 Look 的视频放大。

三条路线只共享商品识别、资产库、模型、OSS、QA、FFmpeg、发布和数据能力，不共用业务任务表和编排逻辑。

## 2. V1 目标与非目标

### V1 目标

- 一个运营每天用 30–60 分钟审核并发布 5–10 条内容。
- 每条任务默认生成 5 张 9:16 图片和 1 条 10–15 秒 MP4。
- 同一账号稳定使用 1 个正式 Persona、2–3 个核心场景和有限 Look 池。
- 图片规划、生成、机器初筛、视频渲染、NeoBund 发布准备由系统完成。
- 运营只负责最终选图、视频确认、发布异常处理和数据复核。
- RDS 暂时作为唯一事实源，不依赖飞书表运行。

### V1 非目标

- 不开发高成本、长时长的 AI 动态视频生成。
- 不追求全自动发布和全自动学习闭环。
- 不一次铺开所有账号、国家、Persona 和场景。
- 不重构现有商品图包或轻量试穿视频主流程。
- 不在飞书 API 额度受限期间创建表格、字段或持续同步。

## 3. 整体应用架构

```text
运营入口 / 后续飞书工作台
        │
        ▼
RDS：账号、市场包、主题、任务、图组、渲染、发布、指标
        │
        ▼
Organic Photo Video Orchestrator
        ├── Product Truth Adapter
        ├── Account / Market Pack Resolver
        ├── Persona / Look / Scene Asset Resolver
        ├── Content Planner
        ├── Hero-first Image Generator
        ├── Single-image + Group QA
        ├── Still-image Video Renderer
        ├── NeoBund Hot BGM Selector
        ├── NeoBund Organic Publisher
        └── Metrics / Successful Look Evaluator
        │
        ▼
共享基础设施：gpt-5.6-terra、gpt-image-2、OSS、FFmpeg、RDS、重试与日志
```

## 4. 能力建设清单

### 4.1 现有能力直接复用

- 商品原图、商品基础资料和商品 ID。
- Product Truth 商品识别框架。
- gpt-image-2 多参考图生成与编辑能力。
- 现有人物、Look、场景资产及其历史验证数据。
- OSS 上传、缓存、断点恢复和附件处理。
- FFmpeg 视频编码、媒体探测与基础 QC。
- NeoBund 文件上传、Organic 非带货发布和任务状态查询。
- RDS MySQL 8 连接、JSON 字段和幂等迁移规范。

### 4.2 现有能力需要调整

- Product Truth：补充服装版型、长度、材质、搭配限制、气候和身材比例字段。
- Asset Resolver：增加账号、国家、语言、气候、季节过滤，并在任务中保存资产快照。
- Image Service：新增 9:16 输出、Hero 锚点、多参考图约束和后续图片有限并发。
- QA：从单图商品检查扩展到人物、商品、Look、色调的跨图一致性检查。
- Copy：补齐泰语、越南语和墨西哥西语，禁止不支持组合静默回退成泰语。
- FFmpeg：新增静态图时间轴、轻运动、短转场、文字安全区和静音母版输出。
- NeoBund Adapter：接入国家热点 BGM 查询、账号可用性过滤、音乐参数提交和结果回读。

### 4.3 需要新增

- Organic Photo Video Orchestrator。
- Market Pack、Theme Catalog 和账号视觉身份配置。
- Content Planner 与结构化 Plan JSON。
- Hero-first 图组生产机制。
- 图组级 QA 和人工选图工作流。
- 平台原生热点 BGM 选择器。
- 图文视频发布和 24h 指标闭环。
- Successful Look 候选与人工沉淀机制。

## 5. 模块设计

### 5.1 Task Intake

职责：创建幂等任务、锁定商品和账号、校验必要输入。

必填输入：

- `account_id`
- `product_id`
- 商品参考图

系统自动补充：

- 目标国家与语言
- Market Pack
- Persona
- 允许使用的 Style、Look 和 Scene
- 默认视频渲染预设

商品、Persona、Look、Scene 都在任务中保存快照，确保历史任务可以复现。

### 5.2 Content Planner

Planner 输出一次性 `plan_json`，核心结构包括：

- `theme_id`：长期主题。
- `topic`：本次具体选题。
- `persona_ref`：人物资产及版本。
- `look_ref`：Look 来源和搭配明细。
- `scene_ref`：核心场景或探索场景。
- `market_pack_version`：本地化规则版本。
- `copy`：标题、Caption、Hashtag、首屏文字。
- `shots`：P1–P5 的目的、Prompt、时长、动效和字幕。
- `audio_policy`：默认 `platform_hot_bgm`。

Look 选择优先级固定为：

`Successful Look > Look Template > AI Exploration`

场景选择比例建议：

- 70% 使用账号 2–3 个核心场景。
- 30% 使用允许场景池进行探索。

### 5.3 Hero-first Image Generator

固定顺序：

1. 只生成 P1 Hero。
2. 执行商品、人物、Look 和 AI 缺陷 QA。
3. P1 不通过时修正或重生，禁止直接生成 P2–P5。
4. P1 通过后，将其作为人物、商品、搭配和色调锚点。
5. P2/P3 可有限并发，P4/P5 在已有合格图基础上生成。

默认图片槽位：

- P1 Hero：完整 Look 和首屏吸引力。
- P2 Full Look：上下装关系和身材比例。
- P3 Lifestyle：走路、看手机、喝咖啡等自然状态。
- P4 Detail：商品结构、鞋、包或配饰细节。
- P5 Second Angle：第二机位或轻总结。

### 5.4 QA

单图 QA：

- 商品颜色、长度、领型、廓形、口袋、拉链、Logo 和关键结构。
- 人物脸、发型、年龄感、身材和手部。
- Look 中上下装、鞋、包和配饰。
- 衣物融合、肢体异常、错误文字和明显 AI 痕迹。

图组 QA：

- P1–P5 是否为同一个人。
- 主商品和搭配是否连续一致。
- 场景切换是否符合 Storyboard。
- 色调、清晰度和真实手机拍摄感是否统一。
- 非“一衣多穿”主题不得擅自更换下装或鞋包。

机器只负责初筛和给出修正建议，V1 保留人工最终选择。

### 5.5 Still-image Video Renderer

默认预设：

- 1080 × 1920，9:16，30fps。
- H.264 视频，AAC 静音轨或无声母版，以 NeoBund 实测兼容性为准。
- 默认 12.5 秒，可配置 10–15 秒。
- 不调用 AI 视频模型。
- 动效仅使用慢推近、轻平移、细节放大和短叠化。
- 每屏最多一条短句，保留 TikTok UI 安全区。

时间轴：

- P1：0–2.2s，Topic Hook + 慢推近。
- P2：2.2–4.8s，完整 Look + 轻平移。
- P3：4.8–7.3s，生活状态 + 轻推近。
- P4：7.3–9.5s，搭配细节 + 局部放大。
- P5：9.5–12.5s，第二机位 + 轻结尾。

视频 QA：

- 分辨率、帧率、时长、黑帧、损坏文件。
- 字幕超界、遮脸、遮商品和语言错误。
- 图片顺序、封面和首屏停留时长。
- 是否存在双音轨或意外原声。

## 6. NeoBund 热点 BGM 方案

### 6.1 默认策略

默认使用 `platform_hot_bgm`：成片不烧入正式 BGM，在发布准备阶段通过 NeoBund 挂载 TikTok 平台原生音乐。

保留两个降级策略：

- `embedded_bgm`：NeoBund 音乐能力不可用时，使用本地有授权的 BGM。
- `no_bgm`：特殊实验或平台异常时无音乐发布。

禁止本地已经烧入正式 BGM 后再叠加 NeoBund 音乐。

### 6.2 选歌时机

热点音乐变化快，不能在图片规划阶段锁死。建议在计划发布时间前 30–120 分钟进入 `publish_preparing`：

1. 按 NeoBund Organic 账号查询该账号实际可用的国家热点音乐。
2. 过滤版权、账号权限、地区和时长不适配音乐。
3. 根据 Theme、Topic、画面情绪、节奏和账号近期使用记录评分。
4. 生成 Top 3 候选，默认自动选择 Top 1。
5. 将音乐 ID、名称、国家、榜单位置、选择时间和策略版本写入发布记录。
6. 提交发布后回读实际挂载音乐，不能只保存计划值。

V1 评分建议：

- 内容情绪匹配：35%。
- 当地热度：30%。
- 节奏与视频时长适配：20%。
- 账号近期去重：15%。

同一账号建议 3 天内同一首音乐最多使用 2 次。热点音乐接口失败时降级，不阻塞整条内容发布。

### 6.3 NeoBund 适配开发

现有本地适配器已经支持 Organic 发布，但提交 Payload 尚未包含音乐参数。开发时不得猜测字段名，需要从 NeoBund 当前页面捕获：

- 热点音乐列表接口。
- 国家、账号和曲库权限筛选参数。
- 音乐 ID、使用片段、原声音量和音乐音量字段。
- Organic commit 实际提交字段。
- 任务详情或发布结果中的实际音乐信息。

完成接口确认后再扩展：

- `NeoBundClient.list_trending_music(...)`
- `NeoBundPublishAdapter.select_music(...)`
- `create_scheduled_task(..., music_selection=...)`
- 音乐查询、选择、提交、失败降级和结果回读测试。

## 7. 多国家兼容

不复制国家工作流。国家差异通过可版本化 Market Pack 表达，账号配置可以覆盖默认规则。

### 泰国

- 语言：`th-TH`。
- 优先热湿气候规则、住宅、商场、咖啡店等长期场景。
- 厚冬装必须匹配旅行、空调环境或明确季节主题。
- BGM 使用对应泰国 Organic 账号实际可见热点曲库。

### 越南

- 语言：`vi-VN`。
- 至少区分北部季节变化和南部热湿气候。
- 不允许用一个全国统一气候规则决定 Look。
- BGM 使用越南账号可用热点曲库。

### 墨西哥

- 语言：`es-MX`。
- 区分高原城市和热带/沿海气候。
- 文案使用墨西哥本地表达包，不直接翻译泰语模板。
- BGM 使用墨西哥账号可用热点曲库。

Persona 用于账号视觉连续性，不绑定国籍刻板印象。

## 8. 状态机与失败恢复

主状态：

`draft → planned → hero_generating → image_generating → image_review → rendering → video_review → publish_preparing → ready_to_publish → publishing → published → metrics_collected → archived`

任一阶段可进入 `failed`，必须记录：

- `current_stage`
- `failure_code`
- `failure_detail`
- `retry_count`
- 可恢复的上一成功产物

重试规则：

- Hero 失败：只重跑 Hero。
- 单张图失败：只重跑对应槽位并增加 `shot_version`。
- 图组 QA 失败：只重生不合格图片。
- 视频失败：复用图片重新渲染。
- BGM 查询失败：降级，不重渲染图片或视频。
- 发布结果不明确：先按任务 ID、备注、标题和发布时间查询，禁止直接重复提交。

## 9. RDS 数据设计

已完成 11 张表：

- `opv_market_pack`：国家、语言、气候和内容规则。
- `opv_theme_catalog`：Theme 和默认 Storyboard。
- `opv_render_preset`：视频、文字和音频策略。
- `opv_account_profile`：账号 Persona、视觉身份和资产边界。
- `opv_content_task`：主任务、快照、计划、状态和错误。
- `opv_content_shot`：P1–P5 图片、Prompt、时长、动效和 QA。
- `opv_video_render`：时间轴、视频输出和视频 QA。
- `opv_publish_record`：发布任务和平台结果。
- `opv_metric_snapshot`：2h/24h/72h 指标快照。
- `opv_look_feedback`：人工反馈和 Successful Look 候选。
- `opv_feishu_outbox`：未来飞书同步队列，当前默认 `holding`。

V1 可以先将 BGM 策略存入 `opv_render_preset.audio_rules_json`，将计划和实际挂载音乐存入 `opv_publish_record.platform_metadata_json`。

当开始进行 BGM 独立效果分析时，再增加 `opv_bgm_selection` 明细表，记录候选池、评分、选择原因、榜单快照和最终使用结果，不在 MVP 阶段提前扩表。

## 10. 建议代码结构

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

MVP 先通过 Adapter 复用现有能力，不立即拆分或重构原 Skill。等流程稳定后，再把真正通用的 Product Truth、Asset Provider、Image Service 和 QA 抽成共享包。

## 11. 开发阶段

### 阶段 0：基础数据准备

- 已完成 RDS 建表。
- 新增泰国 V1 Market Pack。
- 新增 8 个 Theme。
- 新增 1 个 12.5 秒 Render Preset。
- 配置 1 个试点账号、1 个正式 Persona、2–3 个核心场景、6–8 个 Look。

### 阶段 1：图组生产 MVP

- Task Intake 和状态机。
- Content Planner。
- Hero-first 图片生成。
- 单图与图组 QA。
- 人工确认接口或命令行工作台。

交付物：输入商品和账号，可以稳定得到一套待审核的 5 张图组与文案。

### 阶段 2：视频化

- FFmpeg 静态图时间轴。
- 轻运动、转场、字幕和封面。
- 视频媒体探测与 QA。
- 无声/静音标准母版输出。

交付物：每套合格图组稳定输出 1 条 9:16 MP4。

### 阶段 3：NeoBund 发布与热点 BGM

- 捕获并确认 NeoBund 音乐接口。
- 账号/国家热点音乐查询。
- 内容匹配、去重和 Top 3 选择。
- Organic 发布参数扩展。
- 发布结果和实际 BGM 回读。
- 异常降级与重复提交保护。

交付物：合格视频可以挂载账号可用的当地热点 BGM 并创建 NeoBund 发布任务。

### 阶段 4：指标与模板沉淀

- 人工回填或平台回读 24h 指标。
- 账号、Theme、Look、Scene、BGM 维度分析。
- Successful Look 候选生成。
- 人工确认后写回现有 Look 资产体系。

### 阶段 5：飞书恢复

- 以 RDS 为事实源创建飞书运营视图。
- 将 `opv_feishu_outbox.status` 从 `holding` 按批次释放为 `pending`。
- 首次同步采用限速、分页、幂等和断点续跑。
- 飞书只做任务操作和人工审核，不承担核心业务状态机。

## 12. MVP 验收标准

- 同一任务重复提交不产生重复主任务。
- Hero 未通过时不生成 P2–P5。
- 5 张图的人物、商品和 Look 跨图一致性达到人工可发布标准。
- 任一图片失败可以局部重跑，不需要整组重生。
- MP4 为 1080 × 1920、10–15 秒、无黑帧、无字幕越界。
- NeoBund 挂载的是目标 Organic 账号实际可用的当地音乐。
- 不出现本地 BGM 与平台 BGM 双音轨。
- 发布请求结果不明确时不会重复创建任务。
- 发布记录保存计划 BGM 和实际 BGM。
- 24h 指标可以关联到账号、商品、Theme、Look、Scene 和 BGM。
- 飞书额度受限期间系统可以完全依赖 RDS 独立运行。

## 13. 首轮试跑建议

- 市场：泰国。
- 账号：1 个。
- Persona：1 个正式版本。
- 核心场景：2–3 个。
- Look：6–8 个。
- Theme：8 个。
- 内容量：20–30 条。
- 日发布量：5–10 条。
- 观察周期：至少覆盖 2–4 周，之后再决定是否自动沉淀 Successful Look。

首轮重点不看直接成交，优先验证：账号视觉稳定性、首屏留人能力、图片一致性、运营审核耗时、NeoBund 热点 BGM 成功率和不同组合的自然流表现。
