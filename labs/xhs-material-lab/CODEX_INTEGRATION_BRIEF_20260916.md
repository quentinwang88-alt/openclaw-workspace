# 小红书穿搭素材 × OPV 生产能力结合 · 背景交接（给 Codex 的分析输入）

> 日期：2026-09-16 ｜ 写给：Codex（技术分析）｜ 产出方：小红书素材实验室（预研报告见 `docs/XHS_OUTFIT_MATERIAL_INTAKE_PRE_RESEARCH_20260915.md`）
> 你的任务：**分析这批素材怎么和现有图文生产能力（packages/organic_photo_video，下称 OPV）结合起来**，输出可行的集成路径设计与建议。本文只交代背景与约束，不预设结论。

---

## 一、需求一句话

老板要给图文模块加「小红书穿搭素材入口」：运营按主题收集小红书穿搭图文 → 审选 → **把素材变成 OPV 生成链可用的输入**（大概率是参考图/风格/场景参考），提升图文的搭配质感与选题效率。**不是**要把小红书图直接发布（授权红线见 §四）。

## 二、素材资产现状（已就绪，在实验室隔离目录）

位置：`labs/xhs-material-lab/`（独立沙盒，与 OPV 零耦合）

- **素材库**：`var/material_library.sqlite3`。核心表 `notes`（note_id 唯一、标题/正文/作者/主题/点赞收藏/来源链接/xsec_token/审核状态 selected|pending_review|rejected/授权字段 authorization=reference_only/feishu_record_id 映射）+ `note_images`（note_pk+seq 组图顺序、file_path、sha256 全库唯一去重）+ `fetch_runs`（批次审计）。
- **图片文件**：`out/download/<note_id>/<note_id>_<序号>.jpeg`，**整篇笔记一组、文件名序号即组图原始顺序**。
- **规模**：67 篇完整笔记 / 517 张图 / 317MB。其中按主题搜索来的 56 篇：旅游冬装 20（108+104 图）、显高搭配 16（177 图）、轻上装 20（100 图）；运营已在飞书审核表勾选 **10 篇已选用**（全是旅游冬装，108 图），其余 45 篇待审、1 篇 token 失效。
- **审核表**（运营在用）：飞书 wiki `TV1fwhpj7il85VkfBkZciwVBn4g` / table `tblrb90WgyvV64tL`，字段含 素材标题/笔记ID/主题/审核状态/抓取状态/作者/点赞/收藏/笔记链接/素材图片(封面)/备注。同步脚本 `scripts/feishu_review_sync.py`（幂等）。
- **采集能力**（已验证可用，均无需碰 OPV）：按链接抓取=`scripts/fetch_notes.py`（XHS-Downloader，无 Cookie，图片下载 100% 完整）；按主题搜索=xiaohongshu-mcp（专用小号已登录，站内搜索 note_type=图文，`scripts/search_to_library.py` 入库）。搜索→抓取→入库→回写审核表全链路当日跑通，零人工介入。

## 三、OPV 现有生产能力地图（结合点在这里）

任务流：飞书任务表（字段名驱动状态机，`services/feishu_workflow.py:31-80`）→ 生成 → QA → release manifest → 发布桥接（`services/main_schedule_bridge.py` 写 `short_video_auto_publish.sqlite3`）。与本需求最相关的三段：

1. **参考图供给链（最可能的结合点）**
   - 任务表已有字段：「完整穿搭素材（可选）」（legacy「图文参考图」）——运营可挂附件图，`feishu_workflow` 读取后进生成链。
   - `services/photo_style_reference_supply.py`：参考图分角色 OUTFIT / VISUAL_STYLE / ENVIRONMENT（ENVIRONMENT 单用途；固定背景任务不发 ENVIRONMENT-only 参考）。`look_a` 首图有安全区逻辑。
   - **外部只读 sqlite 供给先例**：`services/asset_resolver.py` 只读 `skills/lightweight-tryon-video/var/light_tryon.sqlite3`（look/scene 引用）——素材库想进生产，照这个模式做只读适配器是最顺的路径。
   - 参考图会进 `reference_contracts` 的 input_sha256 冻结缓存（注意：新增键必须条件性加入 input_contract，否则旧缓存链会误命中——历史踩坑）。
2. **风格/视觉执行**：`style_profile` + `color_grading_plan`（`photo_reference_vision.py`）已支持从参考图提取色彩/视觉基准贯穿生成；视觉预设（visual_preset）快照 v2 携带 background/photography/layout 有效值。
3. **门禁与发布**：`services/release_gate.py`（三方一致性、成片 sha256 复核、目标账号一致性）；发布前人工确认维度。**已有一条成文红线：带第三方水印的参考图不得作为发布素材**（`docs/OPV_REAL_GENERATION_E2E_TEST_PLAN_20260913.md:193`）。

默认关闭的实验开关先例：`config/experiments/TH_BASELINE_6RUN_V1.json`（`generation_only_first: true` 等实验合同）；飞书入口分组 `entry_group: trial`（`config/feishu_production_presets.json`）。

## 四、硬约束（设计必须绕开的）

1. **授权红线**：素材 `authorization=reference_only`——小红书图带作者水印、未取得商用授权。**只能作为生成参考（搭配/配色/场景/版式灵感），原图和"轻度改图"都不得进入发布产物**；release 链要能证明成片非素材原图。
2. **隔离红线**：OPV 冻结契约代码（task_intake / release_gate / main_schedule_bridge / workflow_v2 / rds_repository 签名）不改；只允许 additive 新文件 + 默认关闭开关；采集工具（GPL/浏览器自动化）永远留在实验室，OPV 只面对素材库这个稳定接口。
3. **审选 gating**：只有 `status='selected'` 的素材可被生产消费；素材库是唯一事实源（飞书表是审核界面，不是供给源）。
4. **组图语义**：穿搭合集的价值在"一组图讲一套搭配"，消费侧应能感知 seq 顺序（如一衣多穿参考），不要当成散图池。
5. 生产机=本机（OPV 扫描器/发布调度在跑），任何新调度都要走正式流程。

## 五、请你分析输出的问题

1. **集成路径对比**（至少评估三条，给出推荐与理由）：
   - A. 零代码：运营把已选用素材图手动挂到任务行「完整穿搭素材（可选）」（现状就支持）——评估够不够用、瓶颈在哪；
   - B. 轻适配：新增只读供给适配器（照 asset_resolver 模式）读素材库，按任务主题自动建议/注入参考图，默认关闭实验开关——评估注入点选在哪（supply 之前？feishu_workflow 读取参考图之后？）、如何映射参考角色（穿搭合集→OUTFIT？风格→VISUAL_STYLE？场景→ENVIRONMENT？）、如何过 reference_contracts 冻结链；
   - C. 深度结合：从素材提取结构化风格特征（配色板/版式/场景标签）进 style_profile 或将来的场景库（Phase 4 设计落点在 OPV 交付报告 §十二）——评估价值与成本。
2. **素材→参考的映射规则**：一篇文章 N 张图怎么选（封面？seq 前几？运营在审核表加"用途标签"？）；主题（旅游冬装/显高搭配/轻上装）怎么对上 OPV 的主题/预设体系。
3. **水印与授权的机器可执行防线**：除了"只作参考"的流程约束，要不要加技术门禁（如 release 前比对成片与素材 sha256/感知哈希）。
4. **成本与排期建议**：哪条路径可以本周试、哪条要正式立项；对 OPV 主线（排版验收、七样 P2/P3）的干扰评估。

## 六、快速上手

```bash
# 素材库盘点
cd labs/xhs-material-lab && python3 scripts/material_lab.py status
sqlite3 var/material_library.sqlite3 "SELECT theme,status,COUNT(*) FROM notes WHERE origin='search' GROUP BY 1,2"
# 已选用素材清单（含本地路径）
sqlite3 var/material_library.sqlite3 "SELECT n.note_id,n.title,i.file_path FROM notes n JOIN note_images i ON i.note_pk=n.id WHERE n.status='selected' ORDER BY n.id,i.seq"
# OPV 参考供给链入口（阅读顺序）
packages/organic_photo_video/services/{feishu_workflow,photo_style_reference_supply,asset_resolver,image_generator,release_gate}.py
```

预研全记录：`docs/XHS_OUTFIT_MATERIAL_INTAKE_PRE_RESEARCH_20260915.md`（§5 隔离设计 / §7 接入路径分层 / §8a §8b 两段试点结果）。
