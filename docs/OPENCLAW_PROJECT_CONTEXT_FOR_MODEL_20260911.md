# OpenClaw 项目背景与能力地图（模型交接版）

> 快照日期：2026-09-11（Asia/Shanghai）  
> 适用机器：`/Users/likeu3` 这台 macOS 主电脑  
> 目标读者：需要理解、维护或继续开发当前 OpenClaw 系统的另一个模型  
> 安全说明：本文不包含 API Key、OAuth Token、Cookie、Feishu Secret、Webhook 或正式表格凭证。

## 1. 一句话定位

这里的“OpenClaw 项目”不是单一应用，而是四层组合：

1. **OpenClaw 平台本体**：本机安装的 CLI、Gateway、模型与插件系统。
2. **多 Agent 协调层**：`main` 负责前台协调，并把技术、测品、商业分析等任务路由给专门 Agent。
3. **业务工作区仓库**：跨境电商运营自动化代码，覆盖达人、选品、库存、原创/复刻内容、图文、混剪、口播、发布和妙手上架。
4. **主电脑生产运行态**：Feishu 凭证、正式 SQLite/RDS 数据、浏览器登录态、Cron、LaunchAgent、附件、缓存和日志。

最重要的边界是：**代码和通用配置在 Git 工作区，凭证、正式数据库、浏览器会话和调度状态只留在主电脑。**

## 2. 系统总览

```text
Feishu / 自然语言请求
        |
        v
OpenClaw Gateway + main Agent
        |
        +--> product_tester          测品、新品机会、冷启动诊断
        +--> business_strategist     商业分析、双周期复盘、经营洞察
        +--> dev-manager             开发、Bug、架构、OpenClaw 配置
        +--> laowang-assistant       轻咨询、日常整理
        |
        v
workspace/skills/*                   自然语言路由、安全边界、固定入口
        |
        +--> workspace/packages/*    可复用领域包
        +--> auto_mixcut/             混剪工程
        +--> miaoshou-auto-listing/   浏览器上架工程
        +--> vendor/hermes-agent/     Hermes 源码快照
        |
        v
Feishu Bitable / RDS / SQLite / OSS / 浏览器 / 视频与图片模型 / 发布平台
```

## 3. OpenClaw 平台本体与运行位置

| 层 | 当前事实 | 位置 |
|---|---|---|
| CLI | OpenClaw `2026.6.11` | `/Users/likeu3/.local/bin/openclaw` |
| npm 包 | 用户级安装；升级应改这里 | `/Users/likeu3/.local/lib/node_modules/openclaw` |
| 旧安装 | 历史 root 安装，通常不要编辑 | `/usr/local/lib/node_modules/openclaw` |
| 主配置 | 模型、Agent、Channel、Plugin、Gateway；包含敏感配置，不进 Git | `/Users/likeu3/.openclaw/openclaw.json` |
| Gateway | 本地 loopback，端口 `18789` | `ws://127.0.0.1:18789` |
| LaunchAgent | Gateway 常驻服务 | `/Users/likeu3/Library/LaunchAgents/ai.openclaw.gateway.plist` |
| 服务包装器 | 为 Gateway 加载受控环境 | `/Users/likeu3/.openclaw/service-env/ai.openclaw.gateway-env-wrapper.sh` |
| 工作区 Git 仓库 | 主要业务代码 | `/Users/likeu3/.openclaw/workspace` |
| Git 远端 | `quentinwang88-alt/openclaw-workspace` | `https://github.com/quentinwang88-alt/openclaw-workspace.git` |
| 日志 | Gateway 与维护日志 | `/Users/likeu3/Library/Logs/openclaw/`、`/Users/likeu3/.openclaw/logs/`、`/Users/likeu3/.openclaw/maintenance/logs/` |

### 当前模型与插件快照

- 全局默认模型：`openai/gpt-5.5`，默认 thinking 为 `high`。
- `main`：名称 `Lobster`，当前显式使用 `codex/gpt-5.6-sol`，thinking 为 `medium`。
- `dev-manager`、`business_strategist`、`product_tester`：当前主模型为 `openai/gpt-5.5`。
- `laowang-assistant`：当前主模型为 `deepseek-api/deepseek-v4-pro`。
- Feishu Channel 已配置 5 个账号别名：`default`、`laowang`、`xiaoce`、`xiaomei`、`xiaoshuai`。
- 已配置/启用的关键插件包括 Feishu、memory-core、reclaw、OpenAI、Codex、active-memory。

注意：历史升级运行手册里曾记录 `openai-codex/gpt-5.5`，但 2026-09-11 实际配置已经演进为上述值。处理当前问题时应优先读取实际 `openclaw.json` 的脱敏字段，不要照搬旧手册。

### 网络代理链

这台机器通常通过本地 HTTP-to-SOCKS 桥访问 GitHub、ChatGPT 和部分上游服务：

```text
OpenClaw / git / npm
  -> http://127.0.0.1:18080
  -> SOCKS5 127.0.0.1:10808
  -> upstream
```

- 桥接脚本：`/Users/likeu3/.openclaw/bin/socks_http_connect_bridge.js`
- 桥接错误日志：`/Users/likeu3/.openclaw/logs/local-http-socks-bridge.err.log`
- Feishu/Lark 域名通常在 `NO_PROXY` 中，避免绕代理。
- OpenClaw 升级可能覆盖 Codex Responses 传输与 env-proxy 兼容补丁；维护说明位于 Codex skill：`/Users/likeu3/.codex/skills/openclaw-upgrade-maintenance/`。

## 4. 多 Agent 结构

| Agent | 职责 | 工作区/定义位置 |
|---|---|---|
| `main` / Lobster / 龙虾哥 | 前台协调；达人 CRM、库存、补货、日常运营；按规则分发专业任务 | `/Users/likeu3/.openclaw/workspace` |
| `dev-manager` | 代码开发、Bug 排查、Skill 架构、OpenClaw 配置 | `/Users/likeu3/.openclaw/agents/dev-manager/workspace` |
| `product_tester` | 测品、新品机会、冷启动诊断、机会品报告、商品入库 | `/Users/likeu3/.openclaw/workspace-product_tester` |
| `business_strategist` | 商业数据分析、双周期对比、渠道和经营复盘 | `/Users/likeu3/.openclaw/workspace-business_strategist` |
| `laowang-assistant` | 轻咨询、开发草案、运营初筛、事务整理 | `/Users/likeu3/.openclaw/agents/laowang-assistant/workspace` |

路由规则的权威入口是：

- `/Users/likeu3/.openclaw/workspace/AGENTS.md`
- `/Users/likeu3/.openclaw/workspace/IDENTITY.md`
- `/Users/likeu3/.openclaw/workspace/SOUL.md`
- `/Users/likeu3/.openclaw/workspace/USER.md`

继续上一轮具体任务时，优先读轻量交接：`/Users/likeu3/.openclaw/workspace/runtime/current-handoff.md`。不要为了恢复单个任务盲扫全部历史 memory。

## 5. 业务工作区目录地图

```text
/Users/likeu3/.openclaw/workspace/
├── AGENTS.md / IDENTITY.md / SOUL.md / USER.md
│   └── Agent 行为、角色、路由和用户协作规则
├── skills/
│   └── 自然语言能力入口；每个 SKILL.md 定义触发条件、白名单命令和安全边界
├── packages/
│   ├── organic_photo_video/       TikTok 图文/静态视频生产域
│   ├── video_structure_router/    脚本结构候选与结构合同
│   ├── wig_success_replication/   墨西哥假发成功脚本复刻域
│   └── remake_video_execution/    长复刻分段执行域
├── auto_mixcut/                   TikTok Shop 商品智能混剪系统
├── miaoshou-auto-listing/         妙手 + TikTok 确定性上架执行器
├── vendor/hermes-agent/           Hermes Agent 的 Git-tracked vendored snapshot
├── scripts/                       双机接入、环境检查和少量工作区级工具
├── docs/                          跨域设计、交接和迁移文档
├── shared/                        仓库内共享代码/参考数据，不等同于正式数据库目录
├── runtime/ / outputs/ / tmp/     运行结果、试验产物和临时文件，通常不应提交
├── structure_router_test/         结构路由与脚本质量实验产物
└── memory/                        OpenClaw 会话连续性与工作记忆
```

根目录仍保留一批早期 Creator Grid / batch pipeline 脚本和历史说明。新任务优先走 `skills/` 和 `packages/` 中的正式入口，不要仅凭根目录文件名调用旧脚本。

## 6. 关键能力与项目位置

### 6.1 原创、复刻、生成、口播与发布主链

| 能力 | 主要入口 | 核心实现/下游 |
|---|---|---|
| 原创内容自然语言调度 | `skills/original-content-operator/` | 调用原创脚本与生产分流的白名单适配器 |
| 原创短/长视频脚本 | `skills/original-script-generator/` | 商品锚点、卖点、结构、人物、穿搭、场景、口播、首帧、Plan C；长视频核心在 `core/longform/` |
| 脚本总库送生产与分流 | `skills/script-run-manager-sync/` | 15 秒原创进短视频运行管理；20–45 秒原创进 Plan C；长复刻进分段执行器 |
| 长复刻分段执行 | `packages/remake_video_execution/` | 冻结人工稿、时间轴解析、≤15 秒分段、H3/桥帧/音频/字幕/回写状态 |
| 轻量养号复刻 | `skills/video-remake-lite/` | 读取 Feishu 待开始任务，分析视频并产出拆解、复刻卡、脚本、最终提示词 |
| 假发成功脚本复刻 | `skills/wig-success-script-replication/` + `packages/wig_success_replication/` | 成功母版、用途区分、人工选品、完整提示词、脚本池交接 |
| 结构方向路由 | `packages/video_structure_router/` | 只读 `sd_*`，筛选生产可用结构，输出结构合同并记录 `sr_*` 血缘 |
| 即梦视频生成 | `skills/jimeng-video-generator/` | 本机浏览器提交、资产认领、下载、Feishu 回写 |
| 轻量试穿视频 | `skills/lightweight-tryon-video/` | 人物/商品参考、试穿提示词、生成与任务回写 |
| 人工上传视频配口播 | `skills/manual-upload-voiceover/` | 唯一自然语言入口；只处理人工上传视频表 |
| 通用口播后处理 | `skills/run-manager-voiceover-postprocess/` | 画面分析、卖点、中央口播、TTS、混音、质检、Feishu 回写 |
| 短视频自动发布 | `skills/short-video-auto-publisher/` | 回收成片、脚本池、账号能力、未来 48h 排期、GeeLark/NeoBund/CreatOK、状态回写 |

关键边界：旧 S1–S4 `run_pipeline.py` 只维护历史数据，不能作为新的原创自然语言入口；“人工上传视频配口播”必须走 `manual-upload-voiceover`，不能误走原创脚本生成器。

### 6.2 图文、图片与混剪

| 能力 | 位置 | 说明 |
|---|---|---|
| TikTok 原生图文/静态视频 | `packages/organic_photo_video/` | Feishu 任务、RDS 状态机、配方/主题/账号、素材冻结、五图包、技术 QA、人工确认发布 |
| TikTok 商品图包 | `skills/tiktok-fashion-image-pack/` | 女装、发饰、墨西哥假发的商品事实提取、主图/场景图、标题与 QA 回写 |
| OpenAI 图片生成/编辑 | `skills/openai-image/` | Codex Responses 图片路径或旧 Images API；复用本机授权与代理 |
| 智能混剪 | `auto_mixcut/` + `skills/auto-mixcut-pipeline/` | 素材锚定、探测、水印、切片、AI 标注、素材角色、渲染计划、成片、质量门、Feishu 预览 |
| 复刻商品图刷新 | `skills/refresh-remake-product-images/` | 从权威原始脚本记录同步最新商品图到复刻相关表 |

### 6.3 选品、市场、VOC 与供应链

| 能力 | 位置 | 说明 |
|---|---|---|
| Hermes 市场洞察、方向卡、选品、1688 找货 | `skills/hermes-product-analysis/`、`skills/hermes-product-analysis-pipeline/` | 从标准化快照按国家、类目、日期执行；代码快照在 `vendor/hermes-agent/` |
| Hermes 市场洞察执行 | `skills/hermes-market-insight-pipeline/` | Feishu 榜单 → 配置/方向卡 → 执行 → 产物核验 |
| Hermes 能力审计 | `skills/hermes-market-direction-flow-audit/` | 区分代码/配置“支持”与是否已有真实产物 |
| FastMoss 选品 | `skills/fastmoss-selection-b/` | 选品方案 B；工作区脚本还有 `scripts/fastmoss_export*.py` |
| 发饰人工审品 | `skills/hair-style-review/` | 分析 Feishu 候选商品并产出审品结论 |
| 商品候选清洗 | `skills/product-candidate-enricher/` | 上架天数、中文名、受控子类等早期筛选字段 |
| VOC 内容洞察 | `skills/voc-insight/` | 从 RDS VOC 证据层生成痛点、形态洞察、视频证明点/辅助点 |
| VOC 产品机会 | `skills/voc-opportunity-insight/` | 原子证据、信号聚合、产品机会卡、规格风险与 Feishu 报告 |
| 1688 标题回填 | `skills/feishu-1688-title-fill/` | 复用登录 Chrome，安全批量回填采购链接标题 |

### 6.4 达人经营与运营分析

| 能力 | 位置 | 说明 |
|---|---|---|
| 达人 CRM / 视频评分 | `skills/creator-crm/` | 达人数据、视频评分、建联与相关运营流程；也是多个模型调用配置的复用来源 |
| 达人周报监控 | `skills/creator-monitoring-assistant/` | 导入周度 Excel、重建指标与标签、输出当前动作 |
| OpenClaw 监控编排 | `skills/creator-monitoring-openclaw/` | 从 Feishu 入站附件处理历史周回填，并只同步最终目标周 |
| 达人画像卡 | `skills/creator-profile-card/` | 主页截图、视频封面宫格、关系运营信息卡 |
| 每日运营日报巡检 | `skills/daily-report-inspection/` | 配饰/女装日报解析、异常与机会检查 |

### 6.5 库存、标题、Feishu 与文件工具

| 能力 | 位置 | 说明 |
|---|---|---|
| 库存查询 | `skills/inventory-query/` | BigSeller API 查询，支持 SKU 模糊匹配 |
| 库存预警 | `skills/inventory-alert/` | 库存阈值检查与提醒 |
| 妙手自动上架 | `miaoshou-auto-listing/` + `skills/miaoshou-auto-listing/` | 1688 采集、TikTok 认领、翻译、SKU 定价/库存、预检、发布和产品 ID 核验 |
| TikTok 标题改写 | `skills/tk-title-rewriter/` | 按类目改写为目标市场语言标题并回写 Feishu |
| Feishu 图片附件回填 | `skills/feishu-bitable-image-fill/` | URL/HTML/详情页图片 → Bitable 附件字段 |
| Feishu 内嵌表恢复 | `skills/feishu-embedded-bitable-introspection/` | 官方权限或自动化受阻时，从登录浏览器恢复表元数据和字段映射 |
| Feishu 文档 | `skills/feishu-doc-enhanced/`、`skills/feishu-doc-usage-guide/` | 文档读写和使用约束；创建后需 append 内容 |
| 文件处理 | `skills/file_handler/` | 入站文件识别和格式处理 |
| 小红书/TikTok 下载 | `skills/xiaohongshu-video-downloader/` | 下载帖子视频并用 ffprobe 校验 |

## 7. 生产数据、附件与运行态位置

这些目录是**主电脑生产状态**，不是普通源码：

| 内容 | 位置 | 注意事项 |
|---|---|---|
| 正式/半正式 SQLite 数据 | `/Users/likeu3/.openclaw/shared/data/` | 包含达人监控、电商、原创、长视频、复刻、发布、口播知识等数据库；不得提交或随意修改 |
| Feishu 入站附件 | `/Users/likeu3/.openclaw/media/inbound/` | Feishu 聊天上传文件落地位置 |
| 媒体出站/工具图片 | `/Users/likeu3/.openclaw/media/outbound/`、`media/tool-image-generation/` | 生成与发送侧运行产物 |
| Agent 会话 | `/Users/likeu3/.openclaw/agents/*/sessions/` | 运行上下文，不进仓库 |
| 凭证 | `/Users/likeu3/.openclaw/credentials/` | 严禁读取后外泄或提交 |
| Cron 历史迁移文件 | `/Users/likeu3/.openclaw/cron/*.migrated` | 2026.6 已迁移；不可把旧 JSON 当当前唯一事实源 |
| 维护健康检查 | `/Users/likeu3/.openclaw/maintenance/` | 会话维护脚本、状态和报告 |
| Jimeng 浏览器态 | `/Users/likeu3/.openclaw/jimeng-chrome-debug/` | 登录态和浏览器缓存，机器绑定 |
| 妙手运行态 | `workspace/miaoshou-auto-listing/runtime/` | 锁、采集回执、发布回执和批次摘要；用于防重复发布 |

共享数据库目前可见的重要文件包括：

- `creator_monitoring.sqlite3`
- `ecommerce.db`
- `original_script_generator.sqlite3`
- `original_script_batches.sqlite3`
- `original_batch.sqlite3`
- `longform_original_video.sqlite3`
- `remake_video_execution.sqlite3`
- `short_video_auto_publish.sqlite3`
- `voiceover_hook_knowledge.sqlite3`
- `tiktok_product_performance.sqlite3`

不要根据相似文件名随意选库。每条业务链应以对应 Skill/包中的 repository 或配置解析为准。

## 8. 自动化与外部副作用

历史迁移快照显示曾启用的 OpenClaw Cron 主要包括：原创脚本小时巡检、脚本送生产每 2 小时、短视频自动发布每 2 小时、发布日报、Memory Dreaming、复刻脚本同步。当前版本已把旧 Cron JSON 迁移到新的状态存储，实时清单应通过可访问 Gateway 的 `openclaw cron list --json` 获取。

高风险外部动作包括：

- Feishu Bitable/Doc 写入、创建字段、改状态和清勾选。
- 即梦、OpenAI、H3、CreatOK 等付费生成请求。
- GeeLark、NeoBund、TikTok 排期或发布。
- 妙手“确认发布”和 TikTok 商品上架。
- RDS migration、正式数据库修复或批量回填。
- Git push、OpenClaw 升级、LaunchAgent 重载。

原则：**“检查/看看”只授权只读；“执行/生成/发布/上架”才授权相应业务动作。** 具体授权口径以对应 `SKILL.md` 为准。

妙手尤其严格：只能使用专用 Chrome `9333`，与 NeoBund `9222` 分离；全局单锁、串行发布、不能猜测性重发，发布后必须取得 TikTok 产品 ID 或进入只读待核验状态。

## 9. 双机开发与 Git 约定

- 主电脑是唯一正式运行机：保留正式 Feishu 写回、网页登录态、Cron、LaunchAgent 和数据库。
- 第二台电脑默认只开发、阅读、局部测试，不接管生产。
- 第二台电脑入口：`scripts/bootstrap.sh`、`scripts/check_env.sh`、`scripts/doctor.sh`、`scripts/dev_start.sh`。
- 示例配置：仓库根 `.env.example`；正式 `.env`、`.env.local`、Token/Cookie 不进 Git。
- 详细说明：`README.md`、`SECOND_COMPUTER_SETUP.md`、`MACHINE_SPECIFIC_ITEMS.md`。
- GitHub 网络异常时使用本地 HTTP 代理桥；提交前检查 `.env`、auth JSON、SQLite、日志、输出物、图片和生成 JSON。

## 10. 2026-09-11 当前开发状态

- 分支：`main`，相对 `origin/main` **ahead 1**。
- 最近提交重点：原生图文生产、发布排期与授权恢复、脚本首帧交接、原创长视频、假发成功脚本复刻、轻量试穿、即梦参考审查。
- 工作树很脏：至少 91 个已跟踪文件发生修改，另有大量未跟踪运行/实验产物。
- 当前最活跃的开发区域：
  - `packages/organic_photo_video/`：TH/MX 原生图文、参考图识别、旅行/假发主题、Feishu 工作流。
  - `skills/original-script-generator/`：短/长视频、口播、人物和素材、远程续跑。
  - `skills/script-run-manager-sync/`：长视频/复刻生产路由。
  - `skills/short-video-auto-publisher/`：账号能力、BGM、排期、双渠道/CreatOK 发布。
  - `packages/remake_video_execution/`：新增长复刻分段执行域。

因此，另一个模型开始改代码前必须先运行 `git status --short --branch`，把现有改动视为用户工作，不能覆盖、重置或清理。

## 11. 新模型推荐阅读顺序

### 处理一般 OpenClaw 工作区任务

1. 本文。
2. `AGENTS.md`。
3. `IDENTITY.md`、`SOUL.md`、`USER.md`。
4. 用户请求直接命中的 `skills/<name>/SKILL.md`，必须完整阅读。
5. 对应领域包的 `README.md`、tests 和固定 adapter。
6. 若是续接具体任务，读 `runtime/current-handoff.md` 和该任务明确指向的交接文档。

### 处理代码开发

1. 先检查 Git 状态与目录内是否还有更深层 `AGENTS.md`。
2. 优先从 Skill 的白名单入口追到核心实现，不要从历史临时脚本猜主链。
3. 先跑相关小范围测试，再考虑真实服务、Feishu 或数据库。
4. 任何可能写正式表、花费额度或发布内容的命令都要重新判断授权。

### 处理 OpenClaw 本体升级/故障

1. 读取 `/Users/likeu3/.codex/skills/openclaw-upgrade-maintenance/SKILL.md` 及其 runbook。
2. 对照当前 `openclaw.json`、实际 dist bundle 名、LaunchAgent 和代理环境，不照抄历史文件名。
3. 验证模型调用、`main` Agent、Feishu Channel 三层都成功后，才能宣布修复完成。

## 12. 易踩坑与明确禁区

- 不输出或复制 `.env`、凭证目录、auth profile、Feishu Secret、Cookie、Token。
- 不把 `runtime/`、`outputs/`、`tmp/`、数据库、日志、浏览器 Profile 当源码提交。
- 不把 LaunchAgent 显示的历史 `Comment` 版本号当作实际 CLI 版本；应运行 `openclaw --version`。
- 不把 `cron/*.migrated` 当作当前实时调度事实，只能作为历史迁移快照。
- 不因沙箱内 loopback 返回 `EPERM`/abnormal closure 就直接判断 Gateway 已宕机；本次检查中 launchd 明确显示 Gateway 处于 `running`，实时网络健康需在正常本机上下文复核。
- 不直接执行根目录历史 pipeline 或旧 S1–S4 流程；先查当前 Skill。
- 不混用人工上传口播、原创视频、复刻视频、图文任务和短视频运营任务表。
- 不在妙手 `9333` 与 NeoBund `9222` 之间混用浏览器。
- 不自动修改“进入生产”“确认发布”“确认上架”等人工授权勾选。
- 不使用 `git reset --hard`、大范围删除或覆盖当前脏工作树。

## 13. 可作为后续维护入口的核心文件

- 工作区规则：`/Users/likeu3/.openclaw/workspace/AGENTS.md`
- 工作区总览：`/Users/likeu3/.openclaw/workspace/README.md`
- 机器专属边界：`/Users/likeu3/.openclaw/workspace/MACHINE_SPECIFIC_ITEMS.md`
- 第二台电脑接入：`/Users/likeu3/.openclaw/workspace/SECOND_COMPUTER_SETUP.md`
- OpenClaw 本机配置：`/Users/likeu3/.openclaw/openclaw.json`（敏感，不进 Git）
- OpenClaw Gateway：`/Users/likeu3/Library/LaunchAgents/ai.openclaw.gateway.plist`
- 业务 Skills：`/Users/likeu3/.openclaw/workspace/skills/`
- 领域 Packages：`/Users/likeu3/.openclaw/workspace/packages/`
- 共享正式数据：`/Users/likeu3/.openclaw/shared/data/`（敏感，不进 Git）
- OpenClaw 维护 Skill：`/Users/likeu3/.codex/skills/openclaw-upgrade-maintenance/`

---

如果后续系统结构发生变化，更新本文时应同时核对：CLI 版本、`openclaw.json` 脱敏结构、Agent workspace、技能目录、领域包、正式数据位置、Git 状态以及 `AGENTS.md` 的路由规则。
