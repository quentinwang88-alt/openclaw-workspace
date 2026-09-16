# 小红书穿搭素材入口 · 技术预研（2026-09-15）

> 需求：给图文模块（OPV）增加「小红书穿搭素材入口」，先做"粘贴链接→自动入库"，再加"按主题自动搜索"。
> 硬约束：**做好隔离，不影响现有图文板块**。
> 结论先行：路线可行，且比预期更适合隔离集成；已在本机完成安装级验证；建议按 §8 两段式试点推进，试点前不接入 OPV 任何代码。

---

## 1. 结论速览（TL;DR）

1. **按链接入库**：选 **XHS-Downloader**（GPL-3.0，活跃维护，最新 release 2.8 / 2026-09-12）。图文图片下载**不需要登录 Cookie**；除 CLI 外自带 **API 模式（POST /xhs/detail，端口 5556）、MCP 模式、可作为 Python 库调用**三种集成形态，天然支持"采集程序独立运行"的隔离架构。本机已装通（独立 uv venv，Python 3.12.13，库导入 + CLI 冒烟通过）。
2. **按主题搜索**：选 **xiaohongshu-mcp**（Apache-2.0，明确允许商用，最新 release v2.5.0 / 2026-08-13，有 darwin-arm64 二进制）。需专用账号扫码登录一次；`get_feed_detail` 必须携带 `feed_id + xsec_token` 且需登录态。**注意：该工具同时带发布/点赞/收藏/评论接口，接入时只开放读取类三件套**（search_feeds / get_feed_detail / user_profile）。
3. **MediaCrawler 维持排除**：许可证明确禁止商用。
4. **许可要点（本次新核实，比预研输入更准确）**：XHS-Downloader 是 GPL-3.0——内部使用不触发传染义务（不分发即可），但 README 免责声明附加了"未经书面授权不得用于商业宣传/推广/再授权"等条款；xiaohongshu-mcp 的 Apache-2.0 无附加限制。两者都不是小红书官方接口，**图片内容本身的授权与工具许可证是两回事**，素材默认按"仅供参考"管理。
5. **隔离架构已落地**：新建 `labs/xhs-material-lab/` 独立沙盒（素材库 sqlite + 抓取骨架 + 试点手册），零改动 OPV 代码、零飞书写入、零定时任务注册。
6. **试点前不需要做任何接入**。试点通过后，接入方式也只允许「只读供给适配器 + 默认关闭的实验开关」（§7），与现有 `config/experiments` 的 `generation_only_first` 模式同构。

## 2. 需求与预研输入的核对

预研输入（外部研究结论）基本准确，本次逐项核实后的**修正与补充**：

| 输入结论 | 核实结果 |
| --- | --- |
| XHS-Downloader：按链接获取、批量、去重 | ✅ 另有 API/MCP/库三种集成形态；下载记录存 `Volume/ExploreID.db` 实现去重；产物命名与目录结构可配置（`name_format`、`folder_mode`、`author_archive`），利于程序化入库 |
| XHS-Downloader 需注意旧链接失效 | ✅ 官方说明链接带日期信息、旧链可能被风控；另确认**解析需链接自带 xsec_token**（或 xhslink 短链）——所以"入库存原始完整链接 + 失败原因"的设计是必须的 |
| xiaohongshu-mcp：登录后搜索/详情/作者页 | ✅ 共 13 个工具，其中**读取类只有 4 个**（list_feeds/search_feeds/get_feed_detail/user_profile），其余是发布与互动类——预研输入未提示这层风险，**接入时必须白名单化** |
| xiaohongshu-mcp 详情需笔记 ID + xsec_token | ✅ 原文"两个参数缺一不可"，token 从搜索/Feed 列表获取，token 会过期，不能只存 ID |
| MediaCrawler 禁止商用 | ✅ 维持排除 |
| 是否存在官方通用接口 | 未发现。"全站搜索穿搭并下载"没有官方授权接口可用 |

## 3. 许可证与合规边界（决定能不能用）

| 工具 | 许可证 | 附加条款 | 对我们的含义 |
| --- | --- | --- | --- |
| XHS-Downloader | **GPL-3.0** | README 免责声明：不得侵犯知识产权；"未经作者或权利人书面授权，不得使用本项目进行任何商业宣传、推广或再授权" | 内部使用（不分发、不改造后对外发布）不触发 GPL 传染义务；通过**独立进程/独立 venv** 调用而非源码级并入我们的代码，保持弱耦合。若未来要深度绑定生产，建议按其公开接口自研最小实现替换 |
| xiaohongshu-mcp | **Apache-2.0** | README："基于学习的目的，禁止一切违法行为"；"一定不要出现引流、纯搬运的情况，属于官方重点打击对象"；"同一账号不允许在多个网页端登录" | 可商用。**必须用专用小号**（不复用日常账号/妙手登录态）；行为上控制频率、小批量、人工审核 |
| MediaCrawler | 自定义禁商用 | 明确禁止商业用途 | 排除，且不参考其代码实现 |

**图片内容授权 ≠ 工具许可证**。无论用哪个工具，下载成功都不等于取得商用授权。因此素材库默认 `authorization=reference_only`：用于提取搭配思路、场景、选题、AI 改图参考；直接发布他人原图走另行的人工授权确认。这与 OPV 既有红线一致——**带第三方水印的参考图不得流入发布链**（`packages/organic_photo_video/docs/OPV_REAL_GENERATION_E2E_TEST_PLAN_20260913.md`）。

## 4. 本机实测记录（2026-09-15）

环境：Mac mini（**主电脑/生产机**——OPV 飞书扫描器与发布调度器的 crontab + LaunchAgents 都在本机，这提高了隔离要求）。

| 验证项 | 结果 |
| --- | --- |
| 克隆 XHS-Downloader（浅克隆到实验室 third_party/） | ✅ |
| `uv sync --no-dev` 独立虚拟环境（自动拉 Python 3.12.13） | ✅ 一次通过 |
| `from source import XHS` 库导入 | ✅ |
| `main.py --help` CLI 启动 | ✅ 参数表正常渲染 |
| xiaohongshu-mcp darwin-arm64 release 资产 | ✅ 存在（login 工具 11.2MB + 主程序 15.6MB），本轮**未下载未运行**（首跑会拉约 150MB 无头浏览器，留到试点 B） |
| 真实抓取小红书 | ❌ 刻意未做：无登录态、且首轮真实访问按计划放进 20 条链接试点 |

## 5. 隔离设计（本次的核心要求）

### 5.1 已落地的隔离沙盒

新建 `labs/xhs-material-lab/`（仓库根新增目录，有 `structure_router_test/` 先例）：

- **独立运行环境**：XHS-Downloader 用自己的 uv venv（Python 3.12），系统 Python 3.9 与其它项目 venv 不受影响；
- **独立数据**：素材库 sqlite 在 `labs/xhs-material-lab/var/`，下载产物在 `labs/.../out/`，全部 gitignored，**不进** `~/.openclaw/shared/data/`（OPV 产物树与正式库所在地）；
- **独立凭据**：将来 xiaohongshu-mcp 登录态存实验室自己的 `third_party/` 下 data 目录；不复用 `~/.openclaw/browser/` 任何登录态，不碰妙手 9333/9222；
- **数据流单向**：采集程序 → 本地素材库 → （未来）只读适配器 → OPV；OPV 永远不反向写素材库；
- **无调度**：不注册任何 cron / LaunchAgent，一切手动小批量触发（生产机上加调度器必须走正式流程）；
- **不会被现有定时任务误拾取**：已核实 OPV 扫描器只扫**飞书任务表**并写 RDS，不扫文件系统；发布桥接只读写 `short_video_auto_publish.sqlite3`。实验室目录不在任何现有任务的扫描路径上。

### 5.2 明确不碰的红线（摸底核实后列出）

1. 正式飞书 workbench 表与运营表（token 硬编码于 `packages/organic_photo_video/scripts/run_feishu_tasks.py:29-32`、`services/operation_product_pack_source.py:18-19`）——不读不写；
2. OPV 冻结契约代码（task_intake / release_gate / main_schedule_bridge / workflow_v2 / rds_repository）与 RDS migrations——零改动；
3. crontab 中 `organic_photo_video workers` 两行与 `com.likeu.opv-*` LaunchAgents；
4. `~/.openclaw/shared/data/`、`~/.openclaw/browser/`、妙手 9333 约定；
5. `config/feishu_production_presets.json` 现有 preset（将来加实验入口也只新建独立 entry_group 或实验合同文件）。

## 6. 素材库数据模型（已建 schema）

按需求逐字段落地（`labs/xhs-material-lab/schema.sql`），要点：

- **整篇笔记入库**：`notes` 保留标题/正文/作者/主题/原始链接与 xsec_token；**`note_images` 保留组图顺序 `seq`**——穿搭参考的价值在组图的搭配结构与远近景顺序，不打散；
- **双保险去重**：`notes.note_id` 唯一（链接级）+ `note_images.sha256` 唯一（图片级，跨笔记查重）；XHS-Downloader 自带的下载记录库作为第三层；
- **状态机**：`pending_review / selected / rejected`（运营处置）× `pending / fetched / partial / failed`（抓取状态）+ 失败原因留档；
- **授权字段**：默认 `reference_only`，人工确认后才能改 `cleared_manual`；
- **`fetch_runs` 批次表**：直接支撑试点四指标（完整获取率、人工介入次数等）；
- 配套小工具 `scripts/material_lab.py`（init / add-links / status，纯标准库）与抓取骨架 `scripts/fetch_notes.py`（含连续 3 条失败即停批的人工介入规则）。

## 7. 与 OPV 的未来接入路径（试点通过后另行立项，本轮不做）

分层渐进，每一层都可独立叫停：

1. **现阶段（本轮）**：零接入。素材库是独立资产，运营用本地 `status` 盘点。
2. **参考供给层（建议的第一接入点）**：若试点证明素材适配率够高，写一个**只读适配器**把 `selected` 素材登记为 OPV 的风格参考图供给——参照现有 `asset_resolver.py` 只读外部 sqlite 的成熟模式；OPV 侧改动仅限新增文件 + 默认关闭的开关（同构于 `config/experiments/TH_BASELINE_6RUN_V1.json` 的 `generation_only_first` 实验合同模式；飞书入口若要加预设，用独立 `entry_group` 不进 production）。
3. **搜索任务层（更远）**：xiaohongshu-mcp 的搜索也由实验室进程承担，OPV 始终只面对素材库，采集工具可随时替换。

任何一层接入都保持：**采集进程可独立重启/重装，OPV 生成与发布不受其故障影响**。

## 8. 试点方案（按预研输入的口径，样本量≠平台安全额度）

**试点 A（先做）——按链接入库**：运营挑 ≤20 条笔记分享链接 → `material_lab.py add-links` → `fetch_notes.py` 分批抓（每批 ≤5，手动）→ 盘点。看四个数：
1. 图文完整获取比例（fetch_status=fetched / 总数）；
2. 图片完整率与顺序保真（note_images 抽查 + sha256 去重数）；
3. 单篇入库耗时；
4. 人工介入次数（fetch_runs.manual_interventions）。

**试点 B——按主题自动搜索**：3 个主题（如 旅游冬装 / 显高搭配 / 轻上装）× 每主题 ≤20 篇候选。xiaohongshu-mcp 专用小号登录一次，**只调读取三件套**，产出候选清单（标题/作者/链接/互动量），运营筛完把链接回流试点 A。点赞收藏只作辅助信号，选题匹配（账号风格/可搭配自家商品/衣服展示清楚/场景可复用）优先。

**判停规则**：登录失效或出现验证提示 → 立即停批人工处理，不自动重试（骨架脚本已内置连续 3 失败熔断）。素材适配率若低于约 30%，优先调搜索词与筛选标准，而不是加大抓取量。

## 8a. 试点 A 结果（2026-09-15 当日完成，全链路跑通）

**样本**：26 条（20 条匿名 explore 页新鲜链接 + 6 条公开渠道分享链）。样本自采结论：搜索引擎对小红书索引壁垒很高，公开渠道只能捞到零星分享链——**业务相关链接仍应由运营人工挑选**，匿名信息流只能当机制验证样本（主题不可控，20 条里混了 8 条视频）。

| 指标 | 结果 |
| --- | --- |
| 图文笔记完整获取 | **12/12 = 100%**（所有真实图文笔记全部完整：图片数与预期一致、顺序保留、raw 元数据齐全） |
| 视频笔记识别 | 8 条全部正确识别并归类"部分成功（试点只收图文）"，可选择性补抓封面 |
| 失败 | 6 条，全部带机器可读原因：公开渠道 token 已失效×1、app_share 上下文 token×1、死短链×2、作者主页分享链×1 |
| 图片完整性 | 28 张全部 sha256 入库，物理核验（分辨率/文件大小）通过，0 损坏，0 跨笔记重复 |
| 人工介入次数 | **0**（无登录墙、无验证码、无风控提示；熔断规则未触发） |
| 速度 | 每条约 2-4 秒；每批 5 条 20-30 秒（含进程启动） |

**试点中发现并已修复/沉淀的问题**：
1. 工具落盘结构是 `out/download/<笔记ID>/<笔记ID>_<序号>.jpeg`——文件映射与元数据提取（中文键名：作品标题/描述/类型/作者/发布时间/下载地址）已按真实结构实现，序号按数字解析保组图顺序（字典序会把 _10 排到 _2 前）。
2. **token 有上下文**：web 页 token（pc_search/pc_feed）对无 Cookie 抓取有效；`xsec_source=app_share` 的分享 token 对 web 端无效。`resolve-links` 子命令已沉淀（手动逐跳展开 xhslink，识别笔记链/主页分享链/死链三类）。
3. 短链二次跳转会掩盖真实目标（落地页再 302 回主页），必须到笔记/主页链接即停。
4. extract 失败与"视频跳过"都可能返回空载荷/列表，需分别归类 failed 与 partial。
5. 实图带作者水印（如 @某某 心理师）——印证素材必须 `reference_only`，水印图永不进发布链（OPV 既有红线）。

**结论**：无 Cookie 按链接抓图文入库完全可行，管线已可日常使用。运营补 20 条业务相关图文链接（泰女穿搭方向）重跑即是正式素材积累；试点 B（xiaohongshu-mcp 主题搜索）的前置条件不变。

## 8b. 试点 B 结果（2026-09-16，按主题自动搜索→自动入库全链路跑通）

账号：专用小号扫码登录一次（xiaohongshu-mcp v2.5.0，登录态持久化验证通过）；只用读取三件套，未触碰任何发布/点赞/收藏/评论接口。

| 环节 | 结果 |
| --- | --- |
| 站内搜索（3 主题 × note_type=图文） | 旅游冬装 20 / 显高搭配 16 / 轻上装 20，共 56 条候选；视频全被类型筛选挡掉 |
| 跨主题去重 | 显高搭配 4 条与旅游冬装撞题，note_id 唯一约束自动去重 |
| 搜索→自动入库（小批量抽 10 条验证） | **10/10 完整获取，108 张图片全部数字对齐**（含 18/17/16 张的多图穿搭合集），两批共约 80 秒 |
| 人工介入 | **0**（无验证码、无风控提示） |
| 待运营处置 | 46 条候选留在 pending_review，等运营勾选 |

**结论**："按主题自动搜索 → 候选入库 → 自动抓图打标"的完整闭环可行且当日验证通过。搜索 token 是 web 上下文，与试点 A 管线天然兼容。素材适配率（运营选用率）待运营审完 46 条候选后统计。

### 运营操作入口

- 盘点：`python3 labs/xhs-material-lab/scripts/material_lab.py status`
- 审核候选：候选清单含标题/作者/点赞/收藏（notes 表），勾选后置 `status='selected'`；淘汰置 `rejected`
- 追加主题搜索：`search_to_library.py <搜索结果json> <主题名>`；抓取：`fetch_notes.py --limit 5` 分批
- MCP 服务停止：杀掉 `xiaohongshu-mcp-darwin-arm64` 进程即可（重启后登录态仍在）

## 9. 风险清单

| 风险 | 对策 |
| --- | --- |
| 账号风控（搜索依赖登录态） | 专用小号、小批量手动、熔断停批、不自动重试；出现验证即人工处理 |
| xsec_token / 旧链接时效 | 入库即抓取，存完整原始链接与失败原因，不假定长期可回源 |
| 工具停维护 | XHS-Downloader 2.8（2026-09-12）、xiaohongshu-mcp v2.5.0（2026-08-13）均活跃；隔离架构保证可整体替换 |
| 误入发布链 | 素材存实验室目录（不在 OPV 产物树）；`reference_only` 默认值；水印素材永不发布（OPV 既有红线） |
| GPL/免责条款 | 独立进程弱耦合、不分发；深度绑产前重评或自研最小实现 |
| 生产机误操作 | 本轮零调度、零飞书、零 RDS；试点运行前 `ps`/`launchctl` 自查不影响现有 worker 即可（只读检查） |

## 10. 待决问题（需老板拍板）

1. **试点用哪个小号**做 xiaohongshu-mcp 登录（试点 B 才需要；建议新注册专用号并完成实名）。
2. 素材审核界面：试点期用本地 sqlite + 汇总清单即可；若跑顺后想进飞书，**新建独立表**，绝不写入现有 workbench 表。
3. 试点 A 的 20 条链接由运营提供后即可开跑（需明确授权"真实访问小红书"这一步，本轮预研刻意未做）。

---

### 附：本轮产出物

- `labs/xhs-material-lab/`（隔离沙盒：README 试点手册、schema.sql、material_lab.py、fetch_notes.py、third_party/XHS-Downloader 已装通）
- 本文档
- 参考已有能力：`skills/xiaohongshu-video-downloader/`（仅视频下载，含链接解析/xsec_token 过期经验，试点时复用其认知；与图片素材入库不重复）
