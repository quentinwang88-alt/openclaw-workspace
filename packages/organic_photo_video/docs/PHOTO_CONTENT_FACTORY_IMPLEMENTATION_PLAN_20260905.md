# 图文内容工厂 & 初始账号流量测试——基于当前程序的实施方案

审查日期：2026-09-05。范围：当前本机工作树、RDS 只读查询、发布 SQLite、素材 SQLite、飞书工作台字段、NeoBund 与 TikTok 官方公开接口资料。本轮仅审查与方案设计，没有改业务代码、迁移数据库、生成图片、修改账号或提交发布。

**建议：继续扩展 `packages/organic_photo_video`，保留统一发布系统；局部解除“必须有商品、必须走女装穿搭、默认五图”的绑定。Recipe 已经存在，无需重新抽出一套；真正要补的是可复用成品素材集、Benchmark 执行记录、准确的发布归因和播放数据回收。**

需要先区分两个交付：

- **低成本图片内容 + MP4 发布**：现有主链已经能执行，不使用 AI 视频生成模型。第一阶段最快在这条链上产生业务价值。
- **TikTok 原生可滑动多图帖子**：本地尚未接入；NeoBund 公开多图 API 的已说明用途是带货图文，不能把它当作已验证的无商品养号接口。若原生多图是第一版硬要求，必须把对应授权、无商品能力与数据回收的验证列为立项前置项。

## 1. 当前系统现状与证据

### 1.1 本轮实际核对结果

本轮读取约在北京时间 11:20–11:25，运行库会继续变化，以下是快照，不是累计生产承诺。

| 对象 | 本轮结果 | 意义 |
| --- | --- | --- |
| RDS `opv_` 表 | 19 张，迁移 001–008 已记录 | 不需要从零建内容数据库 |
| Market Pack / Theme / Recipe | 1 / 10 / 5 | 已有业务配置层，当前只有 TH |
| OPV 账号配置 | 1 个，`OPV_TH_TEST_001`，TH、testing | 这是生产配置，不能等同全部实际 TikTok 发布账号 |
| 内容任务 / 内容包 / 图片记录 | 76 / 73 / 408 | 有生产历史；图片记录数不等于可复用合格图片数 |
| 任务涉及商品 | 3 个 | 不是跨多类目充分验证的通用生产系统 |
| OPV 发布记录 / 指标快照 | 8 / 0 | 预留了 Performance 表，但未形成实际回收闭环 |
| 主发布库 OPV 任务 | 18 条：9 已发布、9 已排期 | 主发布库与旧 OPV 发布表并未统一投影，不能相加当作不同帖子 |
| 素材库人物 / 穿搭 / 场景 | 12 / 36 / 7 条 | 人物 8 testing、4 disabled；穿搭 33 enabled、3 testing；场景 2 enabled、1 testing、4 disabled |
| 飞书图文工作台 | 实际读取 13 个字段 | 现有 UI 是飞书工作台，不是本模块独立的 React/Vue 后台 |

发现配置漂移：本地 `RECIPE_OUTFIT_BREAKDOWN_V1.json` 的 `recipe_version=3`，RDS 同 ID 当前为 2。后续需明确配置发布与版本冻结，不能假设修改文件就改变了线上执行版本。既有 2026-09-05 审查文档中的 66 条任务是较早快照，本轮为 76 条。

### 1.2 穿搭生产实际如何工作

```mermaid
flowchart LR
    F[飞书：产品编码、生产预设、数量、执行] --> B[冻结批次与幂等任务]
    B --> P[商品图包 + 人物/穿搭/场景 + Market/Theme/Recipe]
    P --> R[冻结 Plan 与 Revision]
    R --> I[锚点生图、后续图、部分程序化拼贴]
    I --> Q[文件/哈希/选图技术检查]
    Q --> V[FFmpeg 成片与媒体检查]
    V --> H[预览与确认发布]
    H --> M[MainScheduleBridge]
    M --> S[主发布库：店铺池、实际账号、排期、NeoBund]
```

具体实现：

1. `services/feishu_workflow.py` 读取工作台；`ProductionPresetCatalog` 把预设转成任务规格，当前规格写入固定 TH 生产配置账号。
2. `ProductReferenceResolver` 获取共享商品图包及必要的运营任务图片；`BatchDiversityPlanner` 选择人物、穿搭、场景、图包并冻结批次。失败续跑复用原清单。
3. `TaskIntakeService` 建 `opv_content_task`。当前强制非空 `product_id`、至少一张商品参考图。
4. `ContentPlannerService` 从 RDS 读 Market、Theme、Recipe、渲染配置，直接调用 `build_outfit_plan`、`build_outfit_states`。已有局部 Recipe 驱动，但执行仍以服装为中心。
5. `HeroFirstProducer` 先生成视觉锚点再补图，支持当前 revision 已选素材复用。图片模型经现有 `skills/openai-image` 适配器调用；本模块声明 `gpt-image-2`、high 质量。
6. `ShotProducerRouter` 目前只有 `generated_photo`、`composite_board`。拼贴画布可以无 AI，但 `OutfitDecomposer` 缺少单品抠图时仍会调用生图；不能把“程序化首图”计作整条零生图。
7. 文案主要是 `copy_writer.py` 的规则式泰语方案和 `board_copy_writer.py` 的固定泰语句库，不是每条都走大模型生成。图像 Prompt 由 Python 根据冻结规则拼接，另有素材库 `prompt_core`。
8. V2 生产默认是技术检查直通；独立视觉模型 QA 工具仍存在，但不是默认生产链。技术通过不能代表文案自然、假发一致或审美合格。继续复用人工预览及“确认发布”，无需恢复重型逐图模型审核。

主要代码依据：[任务入口](../services/task_intake.py)、[内容规划](../services/content_planner.py)、[图片适配器](../services/image_generator.py)、[生产流程](../services/technical_production.py)、[发布桥](../services/main_schedule_bridge.py)。

### 1.3 已有、部分已有、缺失、无需建设

| 状态 | 能力 |
| --- | --- |
| 已有，可复用 | 版本化 Market Pack、Theme、ContentRecipe；ContentTask、Shot、Package、Revision；人物/穿搭/场景库；商品图包；图片调用、拼贴、文字叠加、FFmpeg；技术检查、人工发布确认；统一多账号排班、NeoBund 提交与结果回查、防重复与故障恢复 |
| 部分已有 | Recipe 与排版分离；批次多样性；跨任务成品图复用；账号初始化；发布记录与真实帖子关联；指标存储；跨内容类型标签；市场与店铺路由 |
| 缺失 | 完整 NO_PRODUCT 路径；MX 假发图文适配；成品 Asset Set 可检索与使用记录；冻结 Benchmark Pack/Run；指定账号的测试排期约束；逐帖子播放采集；同时间窗口评分与运营视图 |
| 本期无需建设 | 第二套内容任务/账号/发布系统；独立 Market/Category/Theme 多层后台；Prompt SaaS；复杂统计/机器学习评分；实时天气；自动 70/30 分流；一次覆盖所有国家与类目的生成策略 |

### 1.4 国家、店铺、账号、类目关系

当前存在两个层次：

- **生产层 RDS**：`opv_account_profile` 保存国家、locale、timezone、默认 Market Pack、人物、允许的 Look/Scene。`opv_content_task.account_id` 外键指向它。
- **发布层 SQLite**：`account_configs` 保存实际账号、店铺、发布渠道、三个时间档、养号开关/数量、初始化开关、NeoBund 授权与能力。`publish_slots` 绑定实际账号与具体排期。

`main_publish_routes.json` 当前仅有 TH → THFZ01。Bridge 将 OPV 成片送入店铺内容池，排班器再选择实际账号；没有把生产 `task.account_id` 当作必选发布账号。

类别目前散落在商品快照 `category`、`product_type`、Theme 的匹配规则、穿搭库适用类目、发布 `product_type`；不是独立的标准 Category 主表。运营端已经有市场/类目选项，也有店铺 ID，不能重新建立一套店铺主数据。新增稳定 `category_key`，把 `outerwear` 等保留为细分类型。

主发布库已存在 TH 女装养号账号，也有 MXJF01 对应的墨西哥假发养号账号与 organic 能力。它们有不同历史内容，且本轮可养号目标账号的初始化开关均关闭；**不能把现有账号全部当作零历史新号**。

### 1.5 数据回收与商业内容连接

`opv_metric_snapshot` 已包含 views、likes、comments、shares、saves、profile visits、followers gain、观看时长、完播率、互动率，以及 24 小时等窗口唯一键。Repository 有读写方法，但未发现接入主工作流的 OPV collector；本轮记录数为 0。

其他相关能力也已检查：轻量试穿的飞书映射有“播放量/数据回收时间”，creator-crm 有达人视频/第三方数据读取，但均未证明能按自有新号、刚发布帖子和统一 24/72 小时窗口回收。不能直接当作账号测试闭环。

发布元数据已有 `content_family_key`、`direction_label`、`content_branch`、`recipe_id`、`theme_id`、`source_product_id`；轻量视频另有内容策略与叙事变体；运营有“每日内容实验卡”“视频方向”字段。这些可以连接后续 AI 视频，但目前没有贯穿图文→视频→广告的稳定主题签名。

## 2. Gap Analysis：真正缺什么

| 能力 | 当前情况 | 改造程度 | 最短建议 |
| --- | --- | --- | --- |
| Market | RDS+JSON 已有，只有 TH，部分规则混入女装及热带气候 | 轻改 | 新增 MX Pack；任务单独保存内容目的地/温度，避免泰国受众等于泰国天气 |
| Category | 商品/穿搭/发布多处字段，未统一 | 轻改 | `womenswear`、`wig` 稳定 key + 两份配置，不建分类树 |
| Theme | 10 个 TH 场景/选题对象 | 轻改 | 保留现有 Theme；CHOICE/GUIDE 等作为 `theme_types` 标签，不建平行 Theme 表 |
| Recipe | 已有 5 个对象、RDS 表、JSON 种子、结构与版本 | 轻中改 | 增变量规范、适用范围、生成策略、视觉规则；新增 8 个业务配方配置 |
| Template | Layout JSON、RenderPreset/Profile 已有 | 局部改 | 首期每 Recipe 一种执行形式；添加简单卡片排版，冻结 template_id/version |
| Prompt | Python 拼接 + JSON 文案 + 素材 prompt_core | 小改 | 执行层保留，记录 compiler_version；不用新 Prompt 服务 |
| 无商品 | 入口、图片校验、穿搭规划都需要商品 | 必须局部改 | 产品参与度显式化，NO_PRODUCT 不造虚拟商品，不强制商品参考图 |
| MX 假发 | 同仓已有假发事实卡/参考图，OPV 无 MX 生产路由 | 必须局部改 | 复用事实卡和素材来源，新增 hair_plan/头发 Prompt/西语文案 |
| 素材复用 | 可复用输入、重试选图、部分抠图；缺成品检索与组合 | 必须补 | Asset Set 清单 + reuse producer；缺图停在补素材，不默认自动花费生图 |
| Benchmark | 已有初始化 3 条/每天 1 条/优先不同 Recipe | 中改 | 在此基础上添加固定 Pack、每号 Run、D1–D3 确定分配与结果 |
| 自动发布 | 已工作，有防重复与对账 | 复用+补归因 | 保留现有 scheduler；补指定账号约束、实际 TikTok Post ID |
| 原生多图 | 本地没有；供应商有带货图文 API 文档 | 能力验证项 | 无商品路径未证实；首期默认图片 MP4，有硬要求则单列前置验证 |
| Performance | 表与 CRUD 有，数据为空 | 必须补 | 真实 ID、发布时间、views、采集时刻、数据状态；互动缺失可空 |
| 账号分层 | 无可靠指标闭环 | 新增轻服务 | 中位数、命中数、可选互动，人工可改阈值与最终决策 |
| Stable/Explore | 有多样性及 core/exploration 配置，非绩效驱动池 | 小改 | 先做标签；新配方默认 Explore，验证后运营升 Stable |

## 3. 推荐架构：在现有模块里增加少量能力

### 3.1 哪些实体独立，哪些不独立

| 概念 | 建议载体 | 原因 |
| --- | --- | --- |
| Market | 继续 `opv_market_pack` | 已存在并被引用 |
| Category | 标准 key + `config/categories/*.json` | 两个业务类目不值得建分类服务 |
| Theme / Topic | 继续 `opv_theme_catalog`；通用母题用多选标签 | 现有 Theme 更接近“本地选题”，没有必要强行改名和迁移历史 |
| Recipe | 继续 `opv_content_recipe` | 已经是业务结构，不应降级成 Prompt 文本 |
| Template | `config/layouts/` 的独立版本配置，任务存 ID+版本+快照 | 逻辑可独立于排版，但不必立即独立建表 |
| Scene / Style | 继续素材库/适配配置；任务冻结具体值 | 女装已有可用库；假发无需创建假穿搭记录 |
| Asset | 继续 `opv_content_shot` 及已有图片文件 | 不复制一套媒体存储 |
| Asset Set | **新增小表 `opv_asset_set`** | 成品图集合需要跨任务检索、冻结身份、配对与人工启停 |
| Benchmark Pack | `config/experiments/` 版本配置，运营配置投影 | 不另建一套生产配方 |
| Benchmark Run | **新增小表 `opv_benchmark_run`** | 一个账号一轮测试，需要持久化进度、快照和结果 |
| Post / Performance | 继续 `opv_publish_record` / `opv_metric_snapshot` | 补统一发布投影，不再新建 Post 和 Performance 双份表 |

**MVP 新业务表建议只有两张：Asset Set、Benchmark Run。** 任务与发布仍使用已有表；Pack 的每个日槽直接关联 Content Task，不另建 PackItem、ExperimentAssignment、ScoreHistory 等整组新表。

### 3.2 最小执行改造

在现有 Planner 的业务规划部分增加 `category_key` 分支：

- `womenswear`：继续 `outfit_planner`、Look 选择、穿搭一致性与商品图包。
- `wig`：新的 `hair_planner` 输出头发状态、人物锁、镜头和素材需求；不调用服装内搭/下装/鞋履校验。
- 纯文字卡、已冻结素材组合：直接输出相同的 Plan/Shot 合同，跳过不需要的人物/商品生成步骤。

所有分支共用 ContentTask、Revision、ShotProducerRouter、技术检查、Package、渲染、飞书和主发布桥。只拆“生成什么”与“如何校验这种类目”，不复制整条 Workflow。

现有 `ShotProducerRouter` 增加 `reused_asset` 与 `template_card`。不要仅在图片生成器里加一个 `if reuse`：Planner、锚点依赖、AssetReadiness、组图合同、发布冻结都必须认识“这张已经有合法来源，不需要再生图”。

现有 Recipe 配置允许 1–10 张，但 Plan 校验除 `multi_look` 外仍固定 5 张。第一版 8 个配方统一 5 页最省改动；同时把新合同的页数校验改为读取冻结 Recipe，而不是继续增加针对 Recipe ID 的特判。旧任务沿用原合同。

### 3.3 Recipe 放在哪里，以及运营如何修改

**RDS 是执行时的配置来源，版本化 JSON 用于种子、评审和导出。** 继续现有 loader/validator/repository，扩展一个 `recipe_spec_json`，内容包括：

```json
{
  "schema_version": "opv-recipe-spec-v2",
  "category_key": "womenswear",
  "applicable_markets": ["TH"],
  "theme_types": ["GUIDE", "SCENARIO"],
  "purpose_modes": ["NURTURE", "ACCOUNT_BENCHMARK", "TOPIC_EXPLORE"],
  "product_modes": ["NO_PRODUCT", "SOFT_PRODUCT"],
  "variables_schema": {
    "destination": {"enum": ["Seoul", "Tokyo", "Osaka", "Shanghai", "Beijing", "Harbin"]},
    "temperature_c": {"enum": [15, 10, 5, 0, -5]},
    "scene": {"enum": ["Airport", "Shopping", "Cafe", "Walking", "Photo", "Night"]},
    "height_cm": {"enum": [150, 155, 160]},
    "style": {"enum": ["korean_clean", "relaxed_travel"]}
  },
  "allowed_templates": ["TRAVEL_GUIDE_5P_V1"],
  "visual_rules": {"same_identity": true, "look_count": 3},
  "production_policy": {"preferred": "ASSET_REUSE", "on_missing": "NEEDS_ASSET"}
}
```

页数、每页角色继续用已有 `shot_count` / `story_structure_json`；文案继续 `copy_style_json`。不要在新 JSON 再保存一份互相矛盾的完整结构。

运营在飞书“图文配方与测试配置”编辑简短字段；点击现有风格的“应用配置”后，经校验生成新版本、同步 RDS，旧任务不变。复杂变量 JSON 可由开发维护，运营以枚举/范围字段操作。不要让开发文件自动覆盖运营改动：只导入显式指定配置版本；已被任务引用的版本不可原地覆盖。

## 4. 第一阶段 MVP 边界

| 层级 | 范围 |
| --- | --- |
| 必须做 | 固定实际账号和真实帖子归因；views 采集或明确标记的人工导入通道；24/72 小时窗口与缺失状态；NO_PRODUCT/SOFT_PRODUCT；TH/MX 分类执行；成品素材组合；8 个 Recipe 各一种基础执行形式；Benchmark 3 条测试包；可解释评分；人工预览、确认发布与结果查看 |
| 建议做 | 首批先交付 TH Pick Your Look/Petite、MX Pick Your Hair/Before–After；数据源有授权时自动采集；素材库计次与简单去重；版本漂移校验；成本/返工/队列等待记录；Stable/Explore 标签；内容方向签名 |
| 暂缓做 | 每个 Recipe 第二版式；实时天气；全自动选题；自动 Stable/Explore 比例；复杂因果实验和 ML；新独立后台；全域 DAM 素材中台；自动生成视频/广告预算分配；无授权情况下强行自建 TikTok OAuth 产品 |

自动数据回收是目标；**人工导入仅作为 API 能力未确认时的降级 MVP，不可据此宣称完整自动化交付**。NO_PRODUCT 原生多图也是同样的条件项，不能用带货图文暗中替代。

## 5. TH 女装：四个 Recipe 的具体落地

共用 TH 本地化、人物库、Look 库、图包、文字叠加与 FFmpeg。先用纯色/干净背景，把旅行场景作为内容语义；有可用场景图再组合，避免每个目的地都重新生成城市背景。

| Recipe | 配置与五页结构 | 已有能力与必要扩展 | AI 使用方式 | Benchmark |
| --- | --- | --- | --- | --- |
| TH-01 Travel Outfit | 封面目的地+温度 → Look A/B/C → 同主题回顾+轻互动；变量 destination、temperature_c、scene、style、height_cm/body_type、season | 复用 scene_solution / travel_departure 的部分逻辑与多 Look；新增“目的地气候”优先级和三套不同 Look 序列，不能沿用机场出发一套衣服五角度 | 文案从审定泰语句式填变量；有适配成品 Look 时 0 生图；新 Look 才生成并入库 | TH_BENCH_02：固定 Seoul、5°C、同风格/同页数/同长度文案 |
| TH-02 Petite Styling | 同人对比封面 → 短外套比例 → 高腰搭配 → 裤鞋同色 → 互动；一次只强调一个主技巧 | 复用 petite Theme、pain_point/controlled_outfit_change；新增明确身高标签与对比约束。现有 petite Theme 避免厚叠穿，与寒冷旅行用途不能直接混用 | 优先复用同人物、同镜头成品对；新配对一次生成后复用 | TH_BENCH_03：155cm，固定一种技巧与服装季节 |
| TH-03 Temperature Dressing | 温度 Hook → 基础层 → 中间层 → 外层与鞋 → 收藏/选择；首版每条只讲一个温度 | 复用穿搭规则与 GUIDE 卡；新增 temperature_c、wind_context、indoor_outdoor、layering_level；不接天气 | 图标/文字卡和既有穿搭可全程序化；缺特定冬装示范时单独补素材 | 日常养号和方向探索；与 TH-01 区别是按层次说明，不依赖目的地 |
| TH-04 Pick Your Look | A/B/C/D 四宫格首图 → A → B → C → D+选择 CTA | 复用多 Look 选择与图组；新增 choice_grid 排版、选项稳定映射；现有 multi_look 是同商品最多五套，不能直接当四套自由组合完成 | 4 张合格成品图一次选取，封面/字母/排版无需生图；不是每条重新生成 4 人 | TH_BENCH_01，最先上线 |

具体文案以审定本地句库为主，例如选择型用“ลุคไหนใช่คุณ A, B, C หรือ D?”；旅行型可用“ไปโซล 5°C แต่งตัวยังไงดี?”。这些是起稿示例，首轮要经泰语运营校对，不能仅以出现泰文字母判定自然。

关键规则：

- 身高/体型由运营或素材元数据提供，不从生成图片声称测得精确 155cm。
- 5°C、0°C、−5°C 是内容变量，不是对每套衣服保暖能力的保证；风雨/室内外背景要一致，不让热带 Market 规则否定冬季旅行搭配。
- 禁用当前旧配方中“直接高 5 公分”“贵 3 倍”等未经证实的量化话术作为 Benchmark。弱营销内容不含价格、链接、促销或强购买 CTA。

## 6. MX 假发：不能只替换文案

已经有 `packages/wig_success_replication`：`ProductFactCard.Appearance` 包含 length、texture、color、bangs、layers、face_framing，另有已确认卖点、禁用主张、证据与确认状态；生产入口也已经处理人物参考图、商品图和店铺。**复用它的已确认事实与参考来源，不触发整套视频脚本复刻/新一轮事实提取。**

新 `hair_plan` 至少包含：`identity_ref`、`hair_state_id`、长度/卷度/发色/刘海、脸型标签、妆容、服装、镜头、光线、`comparison_pair_id`。NO_PRODUCT 用运营认可的 hairstyle 素材描述，不冒充具体商品事实。

| Recipe | 配置与结构 | 假发特有要求 | 低成本执行 | Benchmark |
| --- | --- | --- | --- | --- |
| MX-01 Before / After | 对照 Hook → Antes → Después → 发型角度/局部 → CTA | 同人物、脸型、妆容、衣服、机位与光线，只改变头发。不得靠换脸/大幅磨皮制造差距 | 合格 before/after 对保存成 Asset Set；复用两张主图和已有角度生成五页。没有可信细节图时第 4 页改为已有侧面，不编发际线/发网细节 | MX_BENCH_02，先做 |
| MX-02 Pick Your Hair | 四宫格 → A/B/C/D，D 页带 CTA | 同人物或匹配的人物组；第一批固定一个对比维度，例如 bob/largo，或直/卷，不同时大改脸型、妆容和色彩 | 复用同身份发型素材；程序化字母标识与拼贴 | MX_BENCH_01；另冻结黑/棕变体为 MX_BENCH_03，不新增第九个 Recipe |
| MX-03 Face Shape Match | 脸型 Hook → 三种可选造型 → 选择/建议 | 标签 cara redonda/ovalada/cuadrada/alargada；同一帖子固定脸型；把发型建议表达为可尝试的风格，而非客观诊断或唯一正确答案 | 首期人工标注素材脸型，规则选图；不开发自动脸型识别；适配素材不足时先补库 | 先作为 Explore，不作为最早账号测试核心 |
| MX-04 Occasion Hair | 场合 Hook → 三种造型 → CTA | cita/fiesta/trabajo/fin de semana；近景/半身突出头发长度、卷度与轮廓。无需全身鞋包、衣服拆解、换装动画 | 复用同光线发型素材；场合可由文案与装饰表达，不必生成新背景 | 日常养号与场合方向测试 |

需新增 `MP_MX_DEFAULT`、`es-MX` 文案句库、类别词汇、accent/问号字符排版与长度检查、头发一致性 Prompt。示例：“¿Cuál elegirías: A, B, C o D?”、“¿Lista para un cambio de look?”。西语不像泰语可只看 Unicode 区段，需要模板审定及运营抽检。

MX 人物参考中的“固定发型”与换假发目标会冲突：人物锁只固定身份、脸部和妆容，头发由当前 `hair_state` 决定。假发细节检查也不应调用现有“领口到大腿/腰线/鞋履”约束。

## 7. 图片分层、Asset Set 与成本控制

| 生产模式 | 具体行为 | 缺素材时 | 主要验收 |
| --- | --- | --- | --- |
| TEMPLATE_ONLY | 文字/配色/清单/固定素材在模板中组合 | `NEEDS_ASSET` 或缺变量；不偷偷生图 | 外部图片模型调用为 0，字完整、可读、与图片一致 |
| ASSET_REUSE | 按 market/category/标签/同人/配对关系选已通过素材集 | 明确缺哪类资产，运营选择补图 | 0 新图片调用；新任务保留来源、哈希、选择顺序与使用记录 |
| AI_GENERATE | 经现有模型适配生成缺失主图，再进入同一生产流程 | 失败按原 revision 续跑与预算处理 | 每次调用可追踪，不在未知提交状态重复收费 |

`opv_asset_set` 只保存组合清单和检索标签，引用已有 Shot 或已导入的图片文件，不复制二进制文件。必须区分“人物参考”“商品参考”“可直接成片图片”：只有参考图，并不意味着零成本产出已经就绪。

现有轻量试穿 `media_assets` 虽有哈希与标签，但依赖非空商品外键、视频验码/抽帧与视频片段标注。强改它来装无商品图文会牵动另一条正在运行的流程；本期用 OPV 小型 Asset Set 索引更短，日后有明确收益再统一索引。

成本先记实测，不填模型单价假设：`image_call_count`、`retry_image_call_count`、`reused_image_count`、LLM 调用/可获得 token、模型版本、生成/渲染耗时、返工次数、人工分钟数。供应商没有返回费用时成本为未知，用可配置估算，不能把 0 元写成事实。

建议指标：复用路径 0 新图调用、同批已完成任务重复巡检 0 再生成；记录每个 Recipe 的一次成片率和单位成本。缓存命中必须基于源素材哈希、配方/模板/编译器版本及变量，不能仅依据 Prompt 文本。

## 8. 数据结构调整：建议，不执行迁移

### 8.1 扩展现有 RDS 表

| 表 | 字段/变更 | 理由 |
| --- | --- | --- |
| `opv_content_recipe` | `recipe_spec_json JSON NULL` | 保存适用市场/类目、母题、变量规范、允许模板、视觉与成本规则；已有结构/文案字段继续用 |
| `opv_content_task` | `product_id` 改可空；`category_key VARCHAR(32)`；`purpose VARCHAR(32)`；`product_mode VARCHAR(32)`；`production_mode VARCHAR(32)`；`template_id VARCHAR(64)`；`template_version INT`；`content_context_json JSON` | 真正支持无商品；使关键筛选字段可查询；变量/标签/编译器/主题签名等放 JSON |
| `opv_content_task` | `benchmark_run_id VARCHAR(64) NULL`、`benchmark_slot SMALLINT NULL`、`asset_set_id VARCHAR(64) NULL` | 冻结本任务属于哪个账号测试日槽/素材组合；唯一键 `(benchmark_run_id,benchmark_slot)` 防重建 |
| `opv_account_profile` | `publisher_account_id VARCHAR(191) NULL` | 关联主账号源；旧虚拟生产配置不冒充实际账号。新 Benchmark 为每个实际账号建/导入对应配置，继承原风格设置 |
| `opv_publish_record` | `main_slot_id BIGINT NULL`、`publisher_account_id VARCHAR(191) NULL`、`external_task_id VARCHAR(191) NULL` | 将统一排期投影回来，并区分供应商任务 ID、实际账号、真实帖子 ID；`external_post_id` 以后只表示 TikTok 帖子 |
| `opv_publish_record` | 在既有 `platform_metadata_json` 加 `api_variant`、发布时间来源/可信度、Recipe/Template/类别/实验冻结快照 | 同 ID 字段在不同 API 协议可能含义不同；报表不得使用后来被修改的配置 |
| `opv_metric_snapshot` | 七个计数列改可空；新增 `data_status VARCHAR(32)`、`source_updated_at DATETIME(6) NULL` | 区分缺失与真实 0；区分当前抓取时间和供应商缓存更新时间 |

`captured_after_hours` 继续表示 24/72 的目标窗口，`captured_at` 表示实际采集时间。已有 `raw_metrics_json` 保存字段可用性、原始响应、时效、错误和人工证据。不为可选指标再建多张表。

生产模式定义于任务层；每张 Shot 的真实执行来源/费用、源 Asset Set 元素等放现有 source_refs/冻结 manifest。需要统计的总调用数先写 Task 的 execution stats JSON，不先建立成本账单系统。

### 8.2 真正新增的两张表

**`opv_asset_set`**

- `asset_set_id VARCHAR(64) PRIMARY KEY`，`set_key VARCHAR(96)`，`set_version INT`，唯一 `(set_key,set_version)`。
- `market VARCHAR(16)`、`category_key VARCHAR(32)`、`status VARCHAR(32)`、`tags_json JSON`。
- `manifest_json JSON`：每张图的原 Shot ID/文件位置、sha256、宽高、角色、人物身份、头发/穿搭状态、可用范围、来源与使用授权说明；before/after 必须有配对 ID。
- `created_at/updated_at`。引用库外资产只做应用层校验，不建立跨库外键。
- 一个 Set 是某次 Recipe 可用的素材组合；一次冻结后不原地替换内容，新图产生新版本。

**`opv_benchmark_run`**

- `run_id VARCHAR(64) PRIMARY KEY`；`idempotency_key CHAR(64) UNIQUE`。
- `publisher_account_id VARCHAR(191)`、`market VARCHAR(16)`、`category_key VARCHAR(32)`、`cohort_key VARCHAR(96)`。
- `pack_id VARCHAR(64)`、`pack_version INT`、`pack_snapshot_json JSON`，包含 Recipe/Template/变量范围、3 个槽、载体、BGM 规则、观察窗口、阈值版本。
- `status VARCHAR(32)`、`started_at`、`completed_at`、`result_json JSON`、`created_at/updated_at`。
- `result_json` 包含有效样本数、每帖时间窗口、median/hit/engagement、A/B/C/D 或未评级、原因、运营决定。首次“新号初始测试”应只允许一个活动 Run，复测显式创建下一轮。

不额外建测试条目表：3 个槽配置在 pack_snapshot，实际任务用 `benchmark_run_id + benchmark_slot` 关联。日期漂移、缺样本、失败原因通过任务与发布表可追查。

### 8.3 枚举、关系与迁移策略

主要枚举：

- `category_key`：`womenswear / wig`，可扩展；细分商品类型另保留。
- `purpose`：`NURTURE / ACCOUNT_BENCHMARK / TOPIC_EXPLORE`。
- `product_mode`：`NO_PRODUCT / SOFT_PRODUCT / PRODUCT_LED`。这是内容语义，不能替代平台的挂车/商业披露字段。
- `production_mode`：`TEMPLATE_ONLY / ASSET_REUSE / AI_GENERATE`。
- 标签 `pool`：`STABLE / EXPLORE`；母题为 CHOICE/COMPARISON/GUIDE/MATCH/TRANSFORMATION/INSPIRATION/SCENARIO/CHECKLIST。
- Run 状态：`PLANNED / RUNNING / WAITING_METRICS / COMPLETED / NEEDS_ATTENTION / CANCELLED`；grade 为 A/B/C/D 或空，不把未完成测试塞进 D。
- `data_status`：`OK / PENDING / UNAVAILABLE / ERROR / STALE`。

RDS 内：Task→BenchmarkRun、Task→AssetSet 可以加外键；现有 Package→Task、Shot→Task、Metric→Publish 等继续使用。主账号/排班在 SQLite，RDS 的 `publisher_account_id/main_slot_id` 是带验证的逻辑关联，**不能声明跨数据库物理外键**。

发布侧继续修改既有 `app/db.py` 的幂等升级机制：在 `script_metadata` 补 `target_account_id`、`benchmark_run_id/slot`、计划测试日期，并透传到 `PublishCandidate`；为 `publish_slots` 补 `platform_post_id`、真实发布时间来源，在既有 `publish_task_history` 保留变更证据，再做幂等 RDS 投影。唯一 `main_slot_id` 保证对账重跑不生成第二条 Post。`provider/remote_task_id` 的去重需带 API 变体及授权域，不能假设所有供应商任务 ID 全局唯一。

RDS 新迁移用 009 起，保留 001–008 原文件。顺序为新增可空字段/表 → 兼容读写 → 校验/回填历史 → 才收紧新路径校验。旧 Task 没有 product_mode 时保持原商品流程，不机械判断全部是 SOFT_PRODUCT；没有可信来源的旧帖子 ID 和指标不猜填。

`opv_publish_record` 当前 render 唯一键可保留：每个新测试账号拥有自己的任务和渲染记录，跨账号复用源图片而非共用一条 Render/Publish。未来如确需同一成片多账号分发，再评估一对多发布关系，本期无需先改。

## 9. 页面/UI 调整：继续用飞书

| 现有页面/表 | 保留 | 调整 |
| --- | --- | --- |
| 图文工作台 `tblj3x846gU3rshB` | 13 个实际字段：产品编码、生产预设、生成数量、执行、进度、预览/成片、确认发布、审核、审核方式、审核阶段、审核批次、重试审核、备注 | 增市场、类目、用途、产品参与、素材模式、变量、目标账号/测试包。NO_PRODUCT 时产品编码可空；Recipe/Template/版本由预设带出且可查看 |
| 现有账号管理表 | 账号 ID/名称、店铺、状态、渠道、发布时间1–3、是否开启养号、每日条数、是否仅养号、是否账号初始化 | 增目标市场/时区或显示现有映射，测试包、测试轮次、有效样本数、中位播放、命中数、等级、处理建议；账号启停仍只有原表可控 |
| 既有发布记录/排班视图 | 实际账号、计划时间、提交状态、失败原因 | 增真实帖子链接/ID、Recipe、Template、测试日槽、24h/72h 数据状态。必须能按单条成片查看，不能只有整批“已发布” |
| 人物/穿搭/场景库 | 已有素材管理与启停 | 继续管理 TH 输入；MX 复用人物来源，头发素材组合在 Asset Set 视图管理，不要求填写内搭和下装 |
| 新轻量“图文配方与测试配置”表 | 无需独立 Web 后台 | 一种配置表两类记录：Recipe 和 Benchmark Pack；展示变量范围、页序、允许模板、启用状态、版本、阈值；写回经现有客户端和验证器 |
| 新“图文素材集”视图/表 | 数据源为 `opv_asset_set` | 看缩略图、标签、同人配对、状态、使用次数、可用 Recipe；不再上传一份重复原图 |

内容任务仍在图文工作台，账号结果仍在账号表。Task/Benchmark 明细可用飞书关联记录或附件报告，首版不做复杂仪表盘。参数覆盖在点击执行时冻结；执行中修改表格不应改变已经排期内容。

前端示例：选择“TH / 女装 / 账号测试 / 3 / 无商品 / 指定账号组”，先展示每账号 3 条的任务清单、总发布数和素材来源；确认执行生成，查看预览，再沿用“确认发布”。数量必须明确是“每账号内容数”，防止 3 个账号×3 条被错误地生产为 3 条总量。

## 10. 发布、原生多图与数据回收

### 10.1 当前应复用哪条发布链

继续 `MainScheduleBridge → short-video-auto-publisher`。Bridge 已将 `recipe_id/theme_id/content_package_id/source_product_id` 写进 `script_text` JSON，`canonical_script_key=opv:<task_id>` 可反查源任务，养号时清空挂车商品并标记不挂车。既有 `content_family_key` 可继续用于一般去重，但不能代替细粒度 Theme/变量归因。

OPV 旧的 `scripts/run_publish_scheduler.py` 默认已经阻止独立运行，需要显式 legacy 参数才可启用；不要根据旧 README 把它重新作为第二个常驻调度器。发布执行状态仍由主 SQLite 管理，RDS Post 仅作幂等分析投影；RDS 投影失败只能重做同步，不能触发再次发帖。

当前旧接口使用单视频上传与 `attachFileId`，不是图片列表。保留原通道，不因为评估新官方 OpenAPI 就在本期整体迁移认证、音乐、发布重试。

公开官方资料的关键边界：NeoBund OpenAPI v1 的图片发布接收 1–35 张 `images`，要求有对应图文带货能力的达人授权，类型为关联商品/店铺/一图一品，未列无商品 organic 选项。新接口的日期是 UTC，`tiktokVideoId` 与内部 AI 资产 `videoId` 也要区分；新旧接口不能共用未经区分的状态/ID 解析。公开文档未给出本项目所需播放指标查询合同。[NeoBund 官方 API 文档](https://www.neobund.ai/docs/openapi-v1)

TikTok 自身存在原生照片发布 API，图片由有序 URL 数组提交，带封面索引；这只证明平台能力，不能证明当前 NeoBund 账号可用，也不能直接复用 NeoBund 授权调用自建应用。[TikTok Photo API](https://developers.tiktok.com/docs/en/content-posting-api-reference-photo-post)

如后续验证原生无商品路径可用：从冻结 Package 导出有序 `images[{slot,asset_id,sha256,source}]`，适配器负责上传/映射供应商图片 ID、封面、音乐、标题、时区；增加媒体类型和图片就绪检查，不强造 MP4 Render 通过验收。照片与 MP4 用不同 Benchmark cohort/阈值，不能混算播放分层。

### 10.2 必须补齐 Post 映射

推荐唯一链：

```text
真实 TikTok 账号（主 account_configs）
  → BenchmarkRun（每账号每轮）
  → ContentTask（指定账号、日槽、Recipe/Template/变量/Asset Set）
  → Package / Render
  → 主 publish_slots（实际发布账号、NeoBund task ID）
  → opv_publish_record（投影，真实 TikTok Post ID）
  → opv_metric_snapshot（24h / 72h）
```

现有风险：主 `PublishTaskStatus` 没有返回帖子 ID；旧 OPV 直发曾用供应商任务 ID 写 `external_post_id`；主桥后的任务也不自动形成完整 OPV Post 投影。必须分开记录本地 slot_id、供应商 task_id、平台 post_id。

发布成功必须由目标任务精确匹配确认，HTTP 成功或远端建任务成功均不能当作已公开发布。保留已修复的防串号/未知状态逻辑；具体状态值按当前 API 变体解释。

“发布时间”也必须区分：计划时间、提交时间、回查时间、真实公开发布时间。缺真实时间时记录其来源/精度，不能拿计划时间计算 24 小时效果。

### 10.3 最小数据回收路径

按优先级验证并接一个数据源即可：

1. NeoBund 是否可对已有目标帖子返回真实 post_id、公开时间、累计 views：对已发布的少量帖子只读验一次，明确字段、时效和权限；本轮没有证明该指标能力存在。
2. 若已有自有 TikTok 应用及对应用户 `video.list` 授权，可使用官方查询。其视频查询支持 view/like/comment/share 等字段，单批最多 20 个 ID；不能因为 NeoBund 可发视频，就假设本程序已有此授权。[TikTok Query Videos](https://developers.tiktok.com/doc/tiktok-api-v2-video-query/)
3. 若两者都没有，先提供运营后台导出/手填的严格导入：账号 ID、Post ID/链接、真实发布时间、views、采集时刻。沿用同一个快照库和评分器；页面标明“人工数据”，后续只替换采集器。

Collector 只查到期、有真实帖子 ID 的记录，与生成扫描解耦；每小时扫描，失败重试，401/权限不足进入可见错误。独立的数值 0 才是零播放，空响应、查不到、未公开、接口失败都是不同状态。

24h 建议可比窗口 24–28 小时、72h 为 72–76 小时，容差可配置；保存实际年龄。错过窗口记录 STALE/late，不把 96 小时累计播放硬标成 24 小时。重复回查不更新已经用于评分的旧时间窗口为新累计值；修正需保留数据版本/证据。

可选互动拿不到不阻塞。最少数据是：**真实账号 + 真实 Post + 公开发布时间 + 同口径 views + 采集时间/有效状态 + 对应 Benchmark 槽与载体**。saves/profile visits/followers gain 先空；账号级粉丝变化不能无证据归因到单帖。

## 11. Benchmark 内容池、测试流程与评分

### 11.1 Pack 复用普通 Recipe

| Pack | D1 | D2 | D3 |
| --- | --- | --- | --- |
| TH_BENCH_V1 | TH_BENCH_01：Pick Your Look | TH_BENCH_02：Seoul 5°C Travel | TH_BENCH_03：155cm Petite |
| MX_BENCH_V1 | MX_BENCH_01：Pick Your Hair | MX_BENCH_02：Before/After | MX_BENCH_03：Black vs Brown，引用 MX-02 |

Pack 冻结 Recipe/Template 版本、页数、文案长度、主体/光线质量档、变量分布、BGM 选择范围、是否无商品、发布时间窗口和媒体载体。Benchmark 不是新生产引擎，也不是把普通随机任务贴一个 benchmark 标签就完成。

已有初始化逻辑按 3 条目标、每天最多 1 条、优先不同 Recipe 工作，但“优先不同”不等于固定 A/B/C。新增指定日槽约束后，原初始化完成只代表发布完成，评分需等待指标；历史普通 OPV 不能自动计入本轮三条。

### 11.2 多账号低成本复用与内容差异

推荐共享**同一配方与难度/质量规范**，为每个 Benchmark 条目准备多个真实不同的 Asset Set，再按账号均衡分配：例如三种合格人物/造型组合循环交叉使用，避免某一账号永远拿到最强视觉版本。

- 首批每个条目准备 3 个合格素材版本只是小样本启动建议；扩大账号数前需增加实际内容多样性，并统计来源素材使用次数。
- 每账号每槽生成自己的任务/文件；素材可以复用，Recipe/Template/文字量保持可比。文案用有限且已校对的自然变体，变化写入日志。
- CHOICE 可改变选项顺序，但 A/B/C/D 标注必须随内容更新；Before/After、教学步骤不打乱语义顺序。
- Benchmark 首轮固定模板；只有研究 Template 时才改变排版。BGM 使用同规则/有限池并记录，避免热点曲随机性成为主要干扰。
- 微调标点、镜像、裁边、换顺序都不能承诺消除平台重复内容判断。多素材版本应有实际内容差异，不把微变当作绕过检测机制。

TikTok 对未有新创意改动的重复内容有推荐限制；对真实感 AI 人物/场景有标注要求。应保留素材来源与适当 AI 披露，并在测试组保持相同披露规则。[TikTok 内容真实性规则](https://www.tiktok.com/community-guidelines/en/integrity-authenticity/)

### 11.3 一轮业务流程

```text
账号进入测试池 → 核对真实账号/市场/类目/授权/时区/历史状态
→ 领取 Pack，冻结每号 3 个日槽与素材组合
→ 生成或组合素材 → 人工看预览 → 确认发布
→ D1 A → D2 B → D3 C（同一账号每天 1 条，不混入其他自动发布）
→ 每帖实际发布后 24h 采集 → D4 起可出初筛
→ 每帖实际发布后 72h 采集 → D6 起出本轮等级
→ A：进入视频测试候选；B：继续养号；C：补一轮；D：检查异常并暂停自动追加
```

已有主调度滚动窗口为 48 小时，D3 可以下一次扫描滚动进入，不必另建三天 scheduler。错过档位顺延并记录实际顺序，不补发成一天三条。

建议使用账号目标地区相近本地时段，例如 19:00–21:00 内的固定账号档位；这是实验控制建议，不是“最佳发布时间”的平台结论。TH 使用 Asia/Bangkok，MX 按账号真实运营地区选择 IANA timezone，不能将整个墨西哥写死单一时区。

旧主库排期使用无时区时间字符串，OPV 独立旧队列则有 timezone 转换。新 Benchmark 生成 UTC 时刻并保存 local timezone/日期，在现有适配器边界转成它要求的时间，不更改旧排期含义。测试期阻止普通自动任务抢占；人工额外发布只记录为混杂，不能假装实验仍完全标准。

### 11.4 最小评分，不声称测出平台内部权重

只比较同 market/category/载体/Pack 版本/观察窗口/相近账号历史 cohort 的结果。三条内容只能做初筛，产物命名“初始分发表现”，不要把 A/B/C/D 描述为读到了 TikTok 内部权重或已证实限流。

```text
median_views = median(v1, v2, v3)
hit_count = count(vi >= 当前市场/类目/载体/窗口的 normal_views)
hit_rate = hit_count / 3
engagement = sum(likes + comments + shares) / sum(views)
```

Engagement 仅在参与计算的帖子对应字段齐全且总 views>0 时计算；缺失不补 0。也可展示每帖互动率，第一版不用混入一个难解释的总分。

| 结果 | 可配置判定建议 | 业务动作 |
| --- | --- | --- |
| A | 3 条有效；median ≥ good_views 且至少 2/3 达 normal_views | 值得投入一轮视频测试，主题也看多账号表现 |
| B | 3 条有效；median ≥ normal_views | 正常继续养号 |
| C | 有 3 条有效数据但未达 B，且不满足疑似异常条件 | 再测 3 条，检查内容与素材质量 |
| D | 3 条成熟有效样本持续近零/低于 anomaly_floor、0 命中，且已核实帖子公开/账号/数据无误 | 标记疑似异常，人工复核；不自动删除账号，也不当作已证实平台惩罚 |
| 未评级 | 少于 3 条、缺 Post ID/时间、数据过期/错误、未到观察窗 | 等待/补数据，不进入 C/D |

`good_views ≥ normal_views > anomaly_floor ≥ 0`，阈值校验应拒绝顺序错误。TH/MX 分别配置；目前本库没有指标，**无法据此给出可信的固定播放阈值**。第一批先收集参考账号基线：建议每市场至少 3 个测试号并配一个已知可正常发布的参考号，在同一周跑相同 Pack。现有 MX 只读快照仅见一个可养号目标，尚不足做多账号比较。

初期可由运营给临时阈值并标明“试行”；也可由参考组冻结 normal_views 与 good_views 的相对尺度，但不能用本批所有新号的中位数自动把一半强行判正常。阈值在 Run 开始冻结，后改阈值需显式重算且保留原规则版本。

单条爆发只保留帖子成绩，不因它单独把整个账号升 A。内容方向升级为视频也应看多个账号/多个素材集的一致表现，防止把“强账号”误判为“强主题”。

## 12. 跨图文、视频、广告的轻量连接

保留现有 `source_product_id`、Theme、Recipe、`content_family_key`，增加任务 JSON 中稳定 `topic_signature`：

```text
TH | womenswear | travel_outfit | Seoul | 5C | short_puffer | korean_clean
MX | wig | occasion_hair | fiesta | bob | brown | slight_wave
```

签名由规范化字段生成，不用整句文案做主键；产品 ID 可空。后续视频/广告任务在现有元数据附带 `source_content_package_id`、`topic_signature`、`source_benchmark_run_id`。先实现追溯和按标签汇总，不开发自动视频预算或广告归因服务。

新增 VN 发饰时，可复用 Market/Category 配置入口、Asset Set、卡片生成、任务、发布与指标；若需要新的佩戴/发夹视觉一致性，增加一个类别适配即可。**数据结构可扩展不等于新类目高质量生图完全零代码。**

## 13. 开发任务拆分与验收

### Phase 1：把“发到哪个账号、表现如何”接通（约 3–4 个工作日）

依赖：现有发布恢复修复保持有效；至少有可读数据来源和可用目标账号。第一天先验证指标来源及原生多图边界，不把未知接口放到最后才发现。

| 模块/文件 | 工作 | 验收 |
| --- | --- | --- |
| `skills/short-video-auto-publisher/app/models.py`、`neobund_publish.py`、`scheduler.py`、`db.py` | 持久化平台 Post ID 与真实时间/来源，保留 API 变体；让候选指定账号和 Benchmark 日槽；沿用初始化每日 1 条 | 测试包 A/B/C 只能分到目标账号；错账号/错任务响应不成功；排期≠已发布；未知提交不重复 |
| `services/main_schedule_bridge.py`、`repositories/rds_repository.py`、新轻量 `services/publish_projection.py` | 主 slot 幂等投影为 OPV Post；保留生产配置与实际账号区别 | 已有发布对账可重复执行；Post→Account→Task→Recipe 完整，不重复插入 |
| 新 `services/metrics_collector.py`、`services/benchmark_service.py` 及只读/导入入口 | 24h/72h views、缺失状态、3 条评分；先接一个验证过的数据源 | 真 0/NULL/401/晚采集各自处理；未满 3 条不评级；一次爆发不抬高中位数 |
| 新 009 起 migration、既有账号表/发布视图 | 新字段、Benchmark Run、配置快照和结果投影 | 不修改旧排期；旧表/旧任务可读；阈值修改不污染已冻结 Run |

这一阶段可先用已有合格 TH 成片验证回收链，不必等待全部新 Recipe。若只有人工数据导入，验收单上明确写“发布自动、数据人工导入”。

### Phase 2：低成本生产 TH/MX 首批内容（约 4–5 个工作日）

依赖 Phase 1 的来源/账户与配置合同稳定。

| 模块/文件 | 工作 | 验收 |
| --- | --- | --- |
| `domain/models.py`、`domain/contracts.py`、`task_intake.py`、`content_story.py`、`feishu_workflow.py` | 无商品入口、标准类目/用途/变量、配方页数合同、变量冻结 | 空 product_id 的合法任务可跑；PRODUCT_LED 缺商品仍拒绝；不造占位商品 |
| `content_planner.py`、`asset_readiness.py`、`image_generator.py`；新增 `hair_planner.py` / 类别 Prompt 适配 | 女装逻辑保留，假发头发状态和人物锁独立 | MX 输出不出现腰线/下装/鞋履假约束；Before/After 同人；无商品仍有合法素材来源 |
| `shot_producer_router.py`、`hero_first.py`、`workflow_v2.py`；新增 `asset_set_resolver.py`、`simple_card_renderer.py` | Asset Set 检索、reuse/card producer、技术检查与冻结选择 | 已有素材生成 0 新图调用；改哈希会被拒绝；重跑不换图；组图、标题、选项匹配 |
| `config/market_packs/`、`categories/`、`recipes/`、`layouts/`、`copy/`、`experiments/`、`main_publish_routes.json` | TH/MX 配置和 8 Recipe 初版；先验收 TH-04/02、MX-02/01，再扩其余四种 | 每 Recipe 至少 2 套不同变量样品；母语运营确认；MX 能进入原主发布池且定位正确账号 |
| 现有工作台与新增两种轻量配置/素材视图 | 操作入口、版本应用、素材/成本来源可见 | 运营不改 Python 即可启停、改变量范围、选择素材与测试包；旧流程仍可操作 |

首版最多 8 个实际执行形式，共享少量渲染布局。素材不足的 Recipe 可保持 draft，但不能把仅有 JSON 的 8 个配置称为 8 个已验收能力。

### Phase 3：真实小批量验收与扩展（后续 3–5 个工作日，含观察等待）

1. 两市场分别进行 3 个测试号×3 条的最小受控试验，参考号另算；若可用新号不足，先验证生产/发布/回收，不声称已完成跨账号筛选。
2. 在 24h/72h 数据窗核对真实帖子、顺序、未挂车、音乐、可见性、采集状态、分层解释；观察等待可以与 Phase 2 后半段重叠。
3. 故障验收：单条素材缺失、单号授权失效、远端提交超时、采集延迟、不足三条，均不重复发布、不误判 D、不让整批丢失。
4. 已有生产扫描会在锁内串行执行较长生成；复用模式先减负。需要放大 AI 补图量时，再把扫描缩成入队、worker 有界执行；不要靠提高定时频率解决容量。
5. 有数据后再选第二模板、提升 Stable、扩原生多图或下一个类目。不同媒体载体独立 cohort，不混用阈值。

**1–2 周优先级判断**：一名熟悉仓库的开发加运营配合，约 8–10 个开发日可争取完成 Phase 1+2 的受控 MVP，取决于数据权限与素材可用性；完整 8 配方视觉验收、多账号真实观察、原生无商品多图都不能无条件承诺同期完成。如果时间只能保一条业务链，优先“复用素材的选择/对比四配方 + 指定账号三日排期 + 真实 Post/views + 分层”，再补其余四配方，先不要投入第二版式。

## 14. Q1–Q7 集中回答

**Q1：距离通用工厂有多远？**

已有主要骨架，适合在原模块做局部改造，不需要旁建系统；也不是仅新增几份 JSON 就完成。强绑定集中在商品必填、服装规划/素材校验、泰语默认、部分五页/衣物排版约束。发布与回收的关键差距集中在真实账号/帖子归因和 collector。

**Q2：五层都要成为独立实体吗？**

不用。Market/Theme/Recipe 已有表继续使用；Category 是 key+配置；通用 Theme 是标签；Template 是独立版本配置并冻结到任务。独立概念不等于每个概念都建表/后台。

**Q3：Recipe 最合理怎么做？**

扩现有 `opv_content_recipe`，RDS 执行、JSON 版本种子/导出、飞书运营编辑投影。复用 story_structure/copy_style；只补变量、视觉、素材策略。不要把长期业务规则放回一段 Prompt。

**Q4：同一 Benchmark 怎么跨账号低成本用？**

同配方/模板/质量档，多组有实质差异的成品素材，均衡分配并记录；有限文案变体与合法页序变化作为补充。每号独立任务和发布，源图片复用。不能保证微调可消除平台重复内容问题，也不应通过镜像/裁边把重复帖子伪装成新内容。

**Q5：现有数据足够判断吗？**

不够。OPV 指标表为空、真实帖子 ID 未完整贯通。最低补真实账号、帖子 ID、公开时间、views、采集时间/状态和测试槽归属；互动可选。判断是初始分发表现筛选，不是平台内部权重测量。

**Q6：MX 可以复用哪些，必须新增哪些？**

任务/冻结/参考素材下载及哈希/图片调用/卡片/字幕/渲染/发布/回收都可共用，假发项目的已确认外观事实可读取。必须增加 MX 文案、hair_plan、头发与身份分离的一致性约束、同人对照素材集及脸型/场合规则；不套用服装鞋包/腰线/衣物拆解。

**Q7：1–2 周最值的工作是什么？**

先补真实 Post 与 views，再把现有初始化升级为指定账号的固定 3 条 Pack；同时开通无商品素材复用与 TH/MX 选择/对比模板。四个核心 Recipe 先跑通并实测，其余四个随后补齐。第二版式、自动策略分流、全新后台均排后。

## 15. 审查证据索引与验证边界

| 事实 | 文件定位 |
| --- | --- |
| 任务强制商品及参考图 | `services/task_intake.py:219`，`services/content_story.py:67` |
| Recipe 对象已经存在 | `domain/models.py:1204`，`migrations/002_add_recipe_outfit_package.sql:21` |
| 除 multi_look 外 Plan 固定五页 | `domain/contracts.py:61` |
| Planner 直接进入穿搭逻辑 | `services/content_planner.py:931` |
| 素材读取复用旧库 | `services/asset_resolver.py:21` |
| 素材就绪仍要求商品、人物、内搭/下装 | `services/asset_readiness.py:26` |
| 两种 Shot producer | `services/shot_producer_router.py:14` |
| 泰语文案与固定 fallback | `services/copy_writer.py:27`，`services/board_copy_writer.py:46` |
| 默认图片模型复用 | `services/image_generator.py:378` |
| 拼贴也可能需要额外生图 | `services/outfit_decomposer.py:300` |
| 技术检查主链 | `services/technical_production.py:119` |
| 飞书字段与批次 | `services/feishu_workflow.py:38`，`scripts/ensure_feishu_task_table.py:119` |
| 统一发布的内容血缘 | `services/main_schedule_bridge.py:159` |
| 实际账号配置/结果合同 | `skills/short-video-auto-publisher/app/models.py:36`、`:94`（仓库相对路径） |
| 已有初始化 3 条/日限 1 条 | `skills/short-video-auto-publisher/app/scheduler.py:589`、`:822`（仓库相对路径） |
| 指标模型/CRUD | `domain/models.py:1008`、`repositories/rds_repository.py:1168` |
| 假发事实卡 | `packages/wig_success_replication/wig_success_replication/models.py:212`（仓库相对路径） |
| 现有视频素材库约束 | `skills/lightweight-tryon-video/scripts/light_tryon/database.py:497`（仓库相对路径） |

本轮执行了 60 个现有离线测试：config loader 15、task intake 14、main schedule bridge 9、contracts 22，全部通过。测试使用本地假数据/临时库，没有调用真实发布与图片模型。它们确认被审查骨架可用，不代表尚未实现的 NO_PRODUCT、MX、Benchmark 或数据采集已验收。

线上核对均为读取：RDS 使用只读事务；SQLite 使用 `mode=ro`；图文飞书工作台使用已审查的 `--dry-run` 字段读取。没有本轮新生成内容的视觉验收，也没有实际创建新发布，因此方案中吞吐、阈值、原生无商品发布能力均未冒充实测结果。
