# 原创内容生成（15 秒 + 20–45 秒长视频）交接文档

> 更新日期：2026-09-09  
> 用途：供新的开发模型接手原创脚本、原创视频和长视频 Plan C 的开发、测试与生产维护。  
> 本文记录当前代码事实。发生冲突时，以当前 workspace 源码、数据库状态和飞书实际字段为准。

## 1. 接手结论

当前原创内容已经形成两条正式生产分支：

1. **15 秒原创**：飞书运营任务生成一行一条的生产脚本；运营勾选“进入生产”后，脚本同步到现有短视频运行管理表，再由短视频执行链生成视频。
2. **20–45 秒原创长视频**：复用同一运营任务入口和生产脚本审核表，但使用独立的 Plan C 状态库、分段视频执行器、TTS、合并和成片回写，不进入短视频运行管理表。

两条分支共享商品事实、卖点治理、人物、穿搭、场景、结构选择和中央口播能力。它们不共享媒体状态机。不要把长视频改造成“多条 15 秒视频直接拼接”，也不要建立新的长视频运营表。

当前代码仍保留旧的“一行产品 + S1–S4 + 20 个变体”流程，只用于历史记录维护。新任务、自然语言入口和定时任务都不能调用旧 `run_pipeline.py`。

## 2. 代码与数据位置

### 2.1 开发源

- 原创脚本与长视频：`/Users/likeu3/.openclaw/workspace/skills/original-script-generator`
- 脚本送生产与长短视频分流：`/Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync`
- 短视频生成与发布：`/Users/likeu3/.openclaw/workspace/skills/short-video-auto-publisher`
- 图文内容工厂：`/Users/likeu3/.openclaw/workspace/packages/organic_photo_video`

只修改 workspace 开发源。`/Users/likeu3/.codex/skills/original-script-generator` 是安装态或软链接，不应直接编辑。

### 2.2 运行数据

- 原创脚本过程库：`/Users/likeu3/.openclaw/shared/data/original_script_generator.sqlite3`
- 长视频状态库：`/Users/likeu3/.openclaw/shared/data/longform_original_video.sqlite3`
- 长视频素材：`/Users/likeu3/.openclaw/shared/data/longform_original_video/<job_id>/`
- 共享运行数据根目录：`/Users/likeu3/.openclaw/shared/data/`

长视频 job 内的重要文件包括：

- `batches/<batch_id>/source_plan.json`：模型调用前冻结的来源计划；
- `frozen_references/manifest.json`：商品、人物和合成首帧的权限与 SHA256；
- `execution_report.json`：生产执行报告；
- `text_review/`：最终人工审阅包；
- SQLite 中的 `final_video_path`：最终成片位置。

运行数据库、缓存、日志和临时 JSON 不得写进 skill 代码目录，也不得提交 Git。

## 3. 系统边界

| 能力 | 负责模块 | 事实源/状态源 | 说明 |
| --- | --- | --- | --- |
| 运营建单 | 飞书“短视频运营任务表” | 飞书 | 一行一个产品批次 |
| 商品卖点 | 中央卖点库 | RDS `cc__*` | 规划前同步并冻结；RDS 不可用时不能读旧缓存冒充最新数据 |
| 原创计划与脚本 | `original-script-generator` | 本地 SQLite + 飞书结果 | 生成一行一条生产脚本 |
| 生产审核 | 飞书“原创视频生产脚本” | 飞书 | “进入生产”是唯一送生产勾选 |
| 15 秒媒体生产 | `script-run-manager-sync` + 短视频运行管理系统 | 短视频运行库/表 | 同步后由现有短视频链路负责 |
| 20–45 秒媒体生产 | Plan C | 长视频 SQLite | 独立分段、H3、TTS、合并 |
| 长视频发布交接 | `core/longform/main_schedule_bridge.py` | 主发布调度 SQLite | 当前只支持无挂车养号语义 |
| 原生图文 | `organic_photo_video` | OPV RDS + 主发布调度 | 与原创视频状态机分开，不要合并 |

## 4. 运营入口与人工维护信息

### 4.1 “短视频运营任务表”

运营创建原创任务时维护：

- `产品编码（需填写）`
- `产品图片（需填写）`
- `店铺ID（需填写）`
- `一级类目（需填写）`
- `产品类型（需填写）`
- `目标国家`
- `目标语言`
- `生成数量（需填写）`
- `视频规格（需填写）`
- `长视频场景模式（可选）`
- `测试阶段（可选，默认初测）`
- `任务状态（需填写，仅选择待执行）=待执行`

运营不维护 JSON、Prompt、随机种子、批次 ID、人物合同、穿搭合同和分段计划。

`视频规格（需填写）` 的分流规则：

- 留空或 `15秒原创`：走 15 秒原创；
- `20秒 / 25秒 / 30秒 / 35秒 / 40秒 / 45秒`：走原创长视频；
- 20–30 秒自动拆 2 段；
- 31–45 秒自动拆 3 段；
- 每段不超过 15 秒；
- 长视频单次 `生成数量` 最多 3 条。

`长视频场景模式（可选）` 支持自动、单场景和双场景。场景是软规划：没有明确第二使用情境时允许退回单场景，不应为换景编造地点。

### 4.2 “原创视频生产脚本”

程序为每条完成脚本创建一行。运营主要查看：

- 脚本标题、目标语言口播、中文口播；
- 完整生产脚本、视频生成提示词；
- 人物、穿搭、配饰、场景和结构摘要；
- 视频形态、分段计划、长视频任务 ID；
- 首帧与长视频成片；
- 处理状态、错误信息和同步结果。

人工动作只有：

- `生成首帧（需勾选）`：可选。未勾选时 15 秒任务继续使用商品原图；
- `进入生产`：确认脚本可以生成视频；
- `确认发布`：长视频成片确认进入主发布队列。

不要增加逐段确认、第二张审核表或每镜人工放行。

长视频的 K0 不使用 `生成首帧（需勾选）`。Plan C 在生产过程中生成并登记 K0，长视频执行器会把第一段 `start_frame_path` 一次性上传到同一行的 `长视频首帧（系统）`。已有附件时跳过上传；附件展示回写失败只写入同步提示，不把正常的 H3 远端任务或已完成成片标成失败。长视频成片继续回写本表，且不会创建短视频自动运行管理表记录。

## 5. 原创文本生成流程

正式自然语言入口固定为：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/original-script-generator/scripts/openclaw_original_script_task.py <action> [白名单参数]
```

常用操作：

```bash
# 只读检查，不调用模型、不写表
python3 scripts/openclaw_original_script_task.py check --limit 1

# 执行一行待执行任务；一行生成几条由飞书“生成数量”决定
python3 scripts/openclaw_original_script_task.py run --limit 1

# 精确执行一个产品；匹配多行时必须改用 record_id
python3 scripts/openclaw_original_script_task.py run --product-code <产品编码>

# 失败或部分完成任务断点续跑
python3 scripts/openclaw_original_script_task.py resume --record-id <rec...>

# 输入、卖点或配置变化后重新规划
python3 scripts/openclaw_original_script_task.py replan --record-id <rec...>

# 刷新共享穿搭与人物配置
python3 scripts/openclaw_original_script_task.py refresh-production-config
```

运行目录为：

```text
/Users/likeu3/.openclaw/workspace/skills/original-script-generator
```

正式流程当前使用：

- 主文本线路：`openai-codex/gpt-5.6-sol`（2026-09-09 由 gpt-5.5 切换）；
- 完整视觉蓝图：`gpt-5.6-sol/high`；
- 蓝图瞬时故障在有限重试后可回退 `gpt-5.6-terra/high`；
- 中央口播：固定通过中央口播能力生成目标语言表达；
- 人物、穿搭、场景、结构和卖点在 `PLAN_ONLY` 阶段冻结，`SCRIPT_ONLY` 不能重新选择。

核心原则：

- 一条 15 秒脚本只围绕一个核心卖点；
- 中央卖点决定讲什么，中央口播决定怎么说；
- 商品锚点决定商品事实，人物参考决定人物身份；
- 商品参考图中的模特、姿势、滤镜和背景没有人物权威；
- 画面和口播使用同一语义主线，但不做低价值的逐句逐镜硬对齐；
- 事实越权、错误语言、空结果、承载冲突才阻断；
- 钩子强弱、局部节奏和轻微场景偏差只记录，不启动无限修订。

## 6. 15 秒原创送生产

生产脚本经人工勾选 `进入生产` 后，使用唯一同步入口：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/scripts/openclaw_original_batch_sync.py check --limit 20
python3 /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/scripts/openclaw_original_batch_sync.py sync --limit 20
```

也可以按记录或产品执行：

```bash
python3 scripts/openclaw_original_batch_sync.py sync --record-id <rec...> --limit 1
python3 scripts/openclaw_original_batch_sync.py sync --product-code <产品编码>
```

同步器按 `视频形态（系统）` 统一分流：

- 15 秒原创 → 短视频运行管理表；
- 原创长视频 → Plan C；
- 20 秒以上复刻 → 复刻分段执行器。

15 秒同步只交接脚本、参考图、口播及执行合同，不在同步命令内直接发布。重复执行按脚本 ID 幂等；已提交或已排期任务不能被新内容覆盖。

首帧规则：

- 未勾选“生成首帧”：直接使用商品原图；
- 已勾选且首帧就绪：统一首帧替代原商品图成为主参考；
- 已勾选但首帧失败：只阻塞该脚本，保留“进入生产”供重试；
- 首帧不能重选卖点、人物、穿搭、场景或开场。

## 7. 20–45 秒原创长视频 Plan C

### 7.1 规划和脚本

新长视频任务与 `replan` 使用 `DIRECT_PRODUCT`：

1. 从当前运营任务附件解析商品身份；
2. 调用共享 `PLAN_ONLY` 分配卖点、结构、钩子、人物、穿搭和场景；
3. 在模型调用前冻结 `source_plan.json`；
4. 生成一个连续的 20–45 秒语义主线；
5. 按时长投影为 A/B 或 A/B/C 分段；
6. 输出一行一条的生产脚本并创建独立 `longform_job_id`。

长视频不依赖已有 15 秒脚本，也不应自动继承近期短脚本的商品颜色、穿搭、人物或场景。历史实验仍可显式传 `--source-script-id`，但这不是运营默认路径。

### 7.2 媒体生产

运营勾选长视频行的 `进入生产` 后，统一同步入口会先调用长视频执行器。可单独只读检查：

```bash
python3 scripts/run_feishu_longform_production_tasks.py --dry-run --limit 1
```

正式执行需要同时显式允许付费 H3 和外部 TTS：

```bash
python3 scripts/run_feishu_longform_production_tasks.py \
  --record-id <rec...> \
  --allow-real-submit \
  --allow-external-tts \
  --limit 1
```

单 job 的底层入口是：

```bash
python3 scripts/run_longform_original.py run-to-final \
  --job-id <JOB_ID> \
  --allow-real-submit \
  --allow-external-tts \
  --summary-only
```

正式顺序：

1. 冻结商品、人物和首帧参考；
2. 生成连续长口播并按 A/B/C 语义分段；
3. 生成或登记 K0 和需要的片段进入帧；
4. Edge TTS 做真实时长预检；
5. 只在明显过短或过长时做一次定向口播修订；
6. MiniMax H3 按片段提交、查询和下载；
7. 连续动作边界才使用上一段实际尾帧；其他边界使用独立进入帧；
8. FFmpeg 统一规格、去除片段原音轨并直接切镜；
9. 复用冻结 TTS 音频完成混音；
10. 成片写回飞书并清除“进入生产”。

远端 H3 尚未完成时，记录保持“生成中”，下一轮自动续跑。已经有 H3 `taskId` 的片段不得重复提交。

### 7.3 音频与 BGM

- H3 只生成静默表演画面，人物不对口型；
- TTS 在视频生成后叠加；
- 默认最终成片只嵌入口播；
- BGM 交给发布端按国家选择；
- 只有明确配置 `LONGFORM_BGM_PATH` 时才叠加本地 BGM；此时发布端不能再加第二条 BGM。

### 7.4 付费和幂等安全

- H3 真实提交必须有 `--allow-real-submit`；
- 外部 TTS 必须有 `--allow-external-tts`；
- MiniMax Key 只从当前进程环境或 `launchctl` 环境读取；
- 网络返回不确定时记录 `SUBMIT_UNCERTAIN`，禁止自动重试；
- 提交幂等指纹包含参考图片内容 SHA256、Prompt、模式、时长、比例和分辨率；
- `resume` 复用冻结来源；商品图、国家或语言变化必须 `replan`。

## 8. 长视频发布现状与限制

当前 `core/longform/main_schedule_bridge.py` 已能把 `FINAL_READY` 成片写入主发布调度库。运营可以：

- 在生成前同时勾选“进入生产”和“确认发布”，生成成功后直接入队；
- 先只生成，验收成片后再勾选“确认发布”，走 `publish_only` 入队。

当前实现有三个必须明确的限制：

1. 发布标题直接取 `口播_目标语言` 的第一句，最长 150 字符；
2. 发布登记固定为 `发布用途=养号`、`是否挂车=否`、`内容分支=非商品展示型`；
3. 必须有 `店铺ID` 和 `目标国家`，否则不能自动排班。

因此当前桥接只适合无挂车原创长视频验证。若要生产带货长视频，先扩展业务字段和路由合同，再允许发布。不能只把商品编码塞进现有桥接，因为当前代码会主动清空可挂车商品 ID。

## 9. 人物、穿搭与参考图

### 9.1 人物

人物由“原创人物模板库”统一维护。有效模板要求：

- 没有明确停用；
- 有可用人物参考图；
- 国家、类目、产品类型和展示方式兼容。

人物合同冻结稳定 `persona_id`、模板版本、身份锁、参考资产及结构快照。外貌、身体比例和妆发分别处理；人物参考图只控制身份，不控制裁切、拍摄距离、姿势或场景。

更新人物库后显式执行：

```bash
python3 scripts/openclaw_original_script_task.py refresh-personas
```

### 9.2 穿搭

原创和长视频共用结构化穿搭 Provider。选择优先级：

1. 产品专属模板；
2. 同类通用模板；
3. 内部轮廓兜底。

场景、人物和颜色只参与软排序，不增加低价值硬门槛。选定后冻结实际单品、配饰、领口、外层、颜色和完成效果；下游不能重选。

更新穿搭库后显式执行：

```bash
python3 scripts/openclaw_original_script_task.py refresh-outfits
```

一次刷新两类配置：

```bash
python3 scripts/openclaw_original_script_task.py refresh-production-config
```

## 10. 质量策略

只保留会影响事实、安全和生产执行的硬门槛：

- 商品事实或卖点越权；
- 目标语言错误或文本不可用；
- 商品、人物或穿戴状态物理冲突；
- 必要制作合同缺失；
- 长视频时长确实无法容纳；
- 媒体文件、哈希、远端任务或状态机异常。

以下问题只记录或交给人工审阅，不应继续堆规则、重试和模型阶段：

- 钩子力度一般；
- 轻微节奏差异；
- 场景偏好没有完全实现；
- 某个支持事实没有被口播使用；
- 局部口播没有精确落在对应镜头；
- 候选不足时的软重复。

画面和口播各自最多一次定向修订。口播问题不能触发已通过画面的重做。

## 11. 与图文内容工厂的关系

图文入口位于：

```text
/Users/likeu3/.openclaw/workspace/packages/organic_photo_video/scripts/run_feishu_scanner_locked.sh
```

当前 TH 旅行图文使用：

- Recipe：`PHOTO_TH_TRAVEL_OUTFIT_V2`，`recipe_version=6`；
- Planner Prompt：`opv-photo-travel-plan-v7`；
- Layout：`PHOTO_TRAVEL_CARD_V3`，version 3；
- 图片模型：`gpt-image-2.5-sunburst`；
- 媒体：5 张有序 JPEG 原生图文；
- 发布：CreatOK 原生图文，平台自动 BGM。

最新旅行图文优化已经将裤装与鞋履联合规划，并把 `styling_intent` 注入实际生图 Prompt。它属于图文流水线，不应复制到原创视频代码。原创视频需要相同商品/人物/穿搭资产时，应通过共享 Provider 和稳定 ID 复用，不共享 OPV 的任务状态机、Recipe 或图文布局。

## 12. 当前工作区状态

截至 2026-09-09，workspace 存在大量未提交修改和未跟踪产物，涉及：

- `organic_photo_video` 的旅行 V6/V7、Sunburst、发布和飞书流程；
- `original-script-generator` 的长视频生产、远端续跑和主排班桥接；
- `script-run-manager-sync` 的统一生产路由；
- `short-video-auto-publisher` 的 CreatOK、BGM 和原生图文支持。

接手后第一步必须运行：

```bash
git -C /Users/likeu3/.openclaw/workspace status --short
git -C /Users/likeu3/.openclaw/workspace diff --stat
```

禁止：

- `git reset --hard`、`git checkout -- .` 或批量恢复文件；
- 删除不属于本任务的未跟踪文件；
- 覆盖其他模型正在开发的修改；
- 直接修改安装镜像；
- 用旧文档覆盖当前源码行为。

修改前按文件查看 diff，使用小范围补丁。测试通过也不能自动提交 Git，除非用户明确要求。

## 13. 已验证情况

本交接整理时执行了以下离线测试：

```bash
# 长视频工作台、主排班桥接、远端续跑
python3 -m unittest \
  tests.test_longform_feishu_workbench \
  tests.test_longform_main_schedule_bridge \
  tests.test_longform_remote_resume -q
# 11 项通过

# 统一生产路由
python3 -m unittest tests.test_production_route -q
# 4 项通过

# TH 旅行图文规划、文案、排版和 Sunburst 适配
PYTHONPATH=.:tests python3 -m unittest \
  tests.test_photo_planning_upgrade \
  tests.test_photo_travel_semantics \
  tests.test_photo_package \
  tests.test_image_generator_channels -q
# 115 项通过
```

这些是离线回归，不代表已经完成一次新的长视频真实 H3、TTS、飞书回写和发布端到端验证。

较完整的长视频历史回归命令见：

```text
/Users/likeu3/.openclaw/workspace/skills/original-script-generator/docs/LONGFORM_CURRENT_TASK_V7_ACCEPTANCE_20260904.md
```

## 14. 当前最值得优先处理的问题

### P0：跑通一条新的真实长视频

用一条新建或明确 `replan` 的 20–30 秒任务验证：

1. 运营任务输入和商品附件；
2. `DIRECT_PRODUCT` 来源冻结；
3. 人物、穿搭、场景和卖点分配；
4. 生产脚本和连续长口播；
5. “进入生产”后的 H3 远端续跑；
6. Edge TTS、合并和飞书成片回写；
7. 人工观看最终视频；
8. 如需验证发布，再单独勾选“确认发布”。

不要用历史完成 job 验证新规划，因为 `resume` 会正确复用旧冻结来源。

### P0：确认长视频发布用途

在扩大生产前由业务决定：

- 只做养号无挂车：保留现有桥接；
- 需要带货：新增明确的发布用途、是否挂车和商品映射输入，并让主排班桥接按合同登记。

这项必须先于带货长视频自动发布，不能由模型根据是否有产品编码猜测。

### P1：完成真实样片后只修高价值问题

优先修：商品身份漂移、人物明显失真、穿搭与商品冲突、分段像多条短视频、口播时长不适配、远端幂等或状态恢复错误。

暂不增加：逐镜评分、动作数量配额、场景数量门槛、口播填满率死线、多轮自动润色和逐段人工确认。

## 15. 建议接手后的第一轮执行

新的模型先不要改代码，按以下顺序做一次基线：

```bash
cd /Users/likeu3/.openclaw/workspace/skills/original-script-generator

# 1. 查看当前待执行任务
python3 scripts/openclaw_original_script_task.py check --limit 1

# 2. 只查看进入生产的长视频行
python3 scripts/run_feishu_longform_production_tasks.py --dry-run --limit 1

# 3. 检查付费生产环境，不提交、不调用 TTS
python3 scripts/run_longform_original.py preflight --allow-real-submit
```

如果用户已经明确指定一条待执行记录，再运行脚本生成。生成后先汇报：

- 飞书 record ID、产品编码、批次 ID 和 longform job ID；
- 来源模式是否为 `DIRECT_PRODUCT`；
- 商品、人物和穿搭引用是否已冻结；
- 视频规格和分段结果；
- 目标语言口播与中文对照；
- 当前是否只完成文本，还是已经允许真实 H3/TTS。

真实 H3 和 TTS 会产生外部调用。只有用户已经明确要求执行该生产任务时才运行带两个 allow 参数的正式命令。遇到 `SUBMIT_UNCERTAIN` 不得重试，应先查询远端任务状态。

## 16. 验收标准

### 文本阶段

- 一行运营任务按飞书生成数量产出对应脚本；
- 每条脚本具有稳定 ID、人物、穿搭、场景、商品事实和目标语言口播；
- 15 秒与长视频在生产脚本表中被正确标记；
- 长视频是一条连续语义主线，后续段没有重新开钩子；
- 没有编造商品事实、卖点或使用情境；
- 失败可按 record ID 续跑，输入变化要求 replan。

### 媒体阶段

- 15 秒脚本只进入短视频运行管理表；
- 长视频只进入 Plan C；
- 每段最多 15 秒，状态和远端 task ID 可恢复；
- 人物、商品、穿搭和穿戴状态跨段稳定；
- TTS 使用冻结文本与音频，最终混音不重复合成；
- 最终成片写回同一生产脚本行；
- 未勾选“确认发布”时不进入发布队列。

### 发布阶段

- 店铺和国家路由明确；
- 当前无挂车长视频按养号语义进入主队列；
- 标题为目标语言且不为空；
- 重复巡检不会重复建发布任务；
- 带货长视频在发布合同扩展完成前不得沿用无挂车桥接。

## 17. 关键参考文件

- 当前原创规则：`/Users/likeu3/.openclaw/workspace/skills/original-script-generator/SKILL.md`
- 长视频设计与命令：`/Users/likeu3/.openclaw/workspace/skills/original-script-generator/docs/LONGFORM_PLAN_C_V1.md`
- 长视频 V7 验收：`/Users/likeu3/.openclaw/workspace/skills/original-script-generator/docs/LONGFORM_CURRENT_TASK_V7_ACCEPTANCE_20260904.md`
- 运营任务适配器：`/Users/likeu3/.openclaw/workspace/skills/original-script-generator/scripts/openclaw_original_script_task.py`
- 长视频 CLI：`/Users/likeu3/.openclaw/workspace/skills/original-script-generator/scripts/run_longform_original.py`
- 长视频飞书生产器：`/Users/likeu3/.openclaw/workspace/skills/original-script-generator/scripts/run_feishu_longform_production_tasks.py`
- 长视频主排班桥接：`/Users/likeu3/.openclaw/workspace/skills/original-script-generator/core/longform/main_schedule_bridge.py`
- 统一生产路由：`/Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/core/production_route.py`
- 送生产白名单入口：`/Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/scripts/openclaw_original_batch_sync.py`
- 图文生产入口：`/Users/likeu3/.openclaw/workspace/packages/organic_photo_video/scripts/run_feishu_scanner_locked.sh`

## 18. 可直接发给接手模型的指令

> 请先完整阅读 `/Users/likeu3/.openclaw/workspace/docs/ORIGINAL_CONTENT_SHORT_LONGFORM_HANDOFF_20260909.md`、原创 skill 的 `SKILL.md` 和长视频 `LONGFORM_PLAN_C_V1.md`。先检查 workspace 的 `git status` 和相关文件 diff，不要重置、回滚或覆盖现有未提交修改。先跑只读检查、预检和文档列出的定向测试，再选择一条用户明确指定的新长视频任务做端到端验证。新原创任务只能使用 `openclaw_original_script_task.py`；送生产只能使用 `openclaw_original_batch_sync.py`；禁止调用历史 `run_pipeline.py`。保留 15 秒和长视频的独立媒体状态机，不新增运营表、逐段确认或低价值 QA 门槛。真实 H3/TTS 只在用户已授权具体生产任务时执行；提交结果不确定时查询状态，不重复提交。完成后报告 record ID、产品编码、批次 ID、job ID、分段、冻结参考、模型/TTS/H3 状态、成片路径、飞书回写与是否进入发布队列。

## 19. OpenClaw 自然语言调度（已接入）

日常语言入口使用轻量 skill：

```text
/Users/likeu3/.openclaw/workspace/skills/original-content-operator/SKILL.md
```

OpenClaw 已识别为 `Ready`。它只负责区分两个阶段并调用现有白名单：

- “检查待执行原创任务”“跑一条原创长视频脚本” → `openclaw_original_script_task.py`；
- “检查进入生产的脚本”“生成勾选的原创视频”“继续长视频” → `openclaw_original_batch_sync.py`；
- “发布已确认长视频” → 仍走统一同步器，并且只处理飞书已经勾选“确认发布”的记录。

底层两个长篇 skill 继续作为开发和故障诊断资料，普通调度不再读取全部实现规则。2026-09-09 已通过两个真实 OpenClaw 只读语言冒烟：脚本队列检查正确命中脚本适配器；生产队列检查正确命中统一分流适配器。后者遇到现有定时同步锁时安全退出，没有抢锁、写表、生成媒体或自动重试。
