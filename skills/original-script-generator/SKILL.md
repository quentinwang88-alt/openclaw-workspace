---
name: original-script-generator
description: |
  原创短视频脚本自动生成 skill。读取飞书多维表格中由状态位驱动的任务，
  基于产品图片、目标国家、目标语言、产品类型和视频结构路由合同，自动生成 4 套原创短视频内容强策略卡、4 条经独立质检通过的正式脚本。S1-S4 仅作为兼容输出槽位，具体叙事与画面结构由每轮路由结果决定。
  每条脚本会先过独立质检，必要时自动修订，再生成最终视频提示词；默认先只生成母体脚本，只有当表格里勾选了 `生成变体` 时，才会在同轮或后续巡检中补跑 S1 / S2 / S3 / S4 的轻变体；如需只生成部分脚本的变体，可显式传 `--variant-script-index`。
  正式生产主流程只保留与 OpenClaw 主 agent 对齐的 `openai-codex/gpt-5.5` 主线路；阶段0完整脚本蓝图单独使用 `gpt-5.6-sol/high`，视觉适配仍使用主线路，中央口播命令固定使用 `gpt-5.6-sol/high`。
---

# Original Script Generator

## 目录与同步规范

这个 skill 后续统一按以下机制维护：

### 开发源

- `/Users/likeu3/.openclaw/workspace/skills/original-script-generator`

这里只允许长期开发和改代码。

### 安装态

- `/Users/likeu3/.codex/skills/original-script-generator`

这里只作为 OpenClaw / Codex 的共享安装镜像使用，不直接手改逻辑。

推荐同步方式：

```bash
mkdir -p ~/.codex/skills/original-script-generator
rsync -a ~/Desktop/skills/workspace/skills/original-script-generator/ ~/.codex/skills/original-script-generator/
```

### 运行数据

运行数据统一写入：

- `/Users/likeu3/.openclaw/shared/data/`

包括：

- sqlite 数据库
- 运行配置
- 内容 ID
- 中间阶段持久化结果

禁止把运行数据库、缓存、临时 JSON、日志随手写进 skill 代码目录。

### Git 边界

Git 只管理开发源目录：

- `skills/original-script-generator`

不管理：

- `~/.codex/skills/original-script-generator`
- `~/.openclaw/`
- 运行数据库
- 缓存和临时文件

### 第二台电脑

第二台电脑也应遵循同一机制：

`workspace 开发源 -> 同步到 ~/.codex/skills -> OpenClaw 使用 -> 运行数据写 ~/.openclaw/shared/data`

## 核心能力

当用户在飞书多维表格中把 `任务状态` 设为待执行状态后，这条流水线会：

1. 校验最小输入字段
2. 下载产品图片附件
3. 生成 `锚点卡_JSON`，只锁产品锚点
4. 从 RDS `sd_*` 结构资产中选择 4 个兼容结构方向，生成带证据等级与数据快照的结构合同；`S1 / S2 / S3 / S4` 只作为输出槽位
5. 生成 `四套策略_JSON` 字段中的 4 套内容强策略卡，并让每套策略服从同槽位结构合同
6. 回写 `Final_S1_JSON / Final_S2_JSON / Final_S3_JSON / Final_S4_JSON`
7. 为 4 套策略分别生成 `EXP_S1_JSON / EXP_S2_JSON / EXP_S3_JSON / EXP_S4_JSON`
8. 为 4 套策略分别生成正式脚本
9. 对每条脚本单独做独立质检，并追加统一结构合同校验
10. 只对通过质检的脚本生成最终视频提示词，并再次校验结构没有在模板转写中坍缩
11. 默认先只生成 S1 / S2 / S3 / S4 四条正式脚本；如果表格里一开始就勾选了 `生成变体`，则同轮继续生成 5 个轻变体并回写 `变体_S1_JSON ~ 变体_S4_JSON` 与 20 个可读变体字段；变体继承母体结构合同，不允许切换宏观结构
12. 更新 `输出摘要 / 输入哈希 / 最近执行时间 / 错误信息 / 执行日志 / 阶段耗时`
13. 原创任务启动时将 `脚本类型` 归一为 `原创脚本`；带有 `源复刻任务ID` 等权威复刻来源标记的记录不会进入原创任务队列

同时会把每次运行的中间过程落到本地 SQLite，便于按产品编码追溯。

### V23 完整脚本阶段 0（生活事件驱动与精简渲染）

正式流程之外保留一条不写飞书、不生成视频、不生成变体的文本实验路径。该路径会：

- 先按近期历史用量分配 `人物角色 × 场景母题 × 开场动作` 创意多样性合同；
- 创意多样性合同额外携带不参与硬校验的类目软适配档：`WORN_APPAREL / WORN_ACCESSORY / HAND_STATIC_ACCESSORY`。围巾、帽子、耳饰、项链和包优先真人或混合承载，从已经佩戴好的生活状态进入；戒指、手链和发饰优先手部或静物承载。结构合同仍是最终承载权威，软适配不得覆盖它；
- 内容论证包先确定 `primary_hook_id`，再生成带 `CREATIVE_DESIGN` 权威标记的完整脚本蓝图，使画面开场和中央口播从同一个钩子意图出发；
- 内容论证包 V9 以中央卖点库为内容权威：`VERIFIED` 卖点自动获得原创使用资格，`allowed_strength` 控制表达强度；画面语义匹配只记录 `proof_match_status=MATCHED/UNMATCHED` 并辅助选择细节，不得否决或降级卖点。未匹配时扣子、口袋等事实只能作为并列产品细节，禁止被写成卖点成立的原因；
- 蓝图沿用紧凑的 `retention_hook`，同时生成一件完整的 `event_design` 生活事件和 3 段 `macro_visual_passages`；开头只要求从人物原本就在进行的自然动作中途开始，不强制快速入画、停顿、抬眼或表演情绪；
- 由真实视频执行参考控制动作关系，由结构合同控制节奏；内容论证包 V4 明确分开“核心价值、用户顾虑、2–3 个可见证据”，不再把可见结构事实误当成完整卖点；
- 结构计划继续控制宏观 Beat、承载方式、连续性和开场机制；3 段事件画面由代码确定性投影到现有4至6个兼容槽位，不再调用独立视觉适配模型；相邻槽位只延续同一事件过程，同一画面可以同时支持多个卖点；
- `event_design` 是人物在该场景本来就要完成的一件普通事情；核心结果由事件过程证明，扣子、口袋、袖型等商品细节只需保持可见，不分配逐项指向、触摸或核对动作；`reaction_points` 可为空，不要求人物表演情绪；
- 原创流程只通过 `voiceover-argument-contract-v1` 决定讲什么，不再维护自己的泰语钩子措辞模板；自然称呼、观众指代（如“你们/谁正在……”）、钩子表面表达、语气词、跨镜语义段和泰语质检统一由中央口播引擎负责。中央口播 V33 会把称呼、观众指代、反应词和句尾语气词分开传入生成器，按候选的钩子角色择一自然使用；称呼不是默认开场，卖点库是语义授权而非中文逐字翻译来源。卖点模式允许零个可见事实，事实只作可选细节；这是软表达偏好，不新增失败门槛、重试或逐句硬匹配；
- 中央口播采用三段语义计划：自然抓人开场、连续卖点论证、轻收尾。计划由代码确定性生成，不再额外调用模型决定逐句落在哪个镜头；
- 画面只需要在整条视频中为口播事实提供证据。局部先说后拍、跨镜延续和顺序轻微不同只记录警告；保守时长上界超出也只警告，中心估时超过成片时长 20% 才按不可容纳阻断；只有编造事实、整片无证据、语言不可用、确实无法容纳或显式 `MUST_SILENT` 冲突才阻断；
- 支持只生成 3 个中央口播候选供人工选择；候选必须使用不同 ACTIVE 钩子原型，画面、人物和场景保持不变，未显式选择前不继续组装完整脚本；
- 人物、场景、妆发、完整穿搭和表演动机必须进入最终脚本的 `production_design`，不能只留在内部蓝图；静物和手部方向必须明确标出人物不出镜或仅手部出镜；
- 完整脚本继续保留4至6个内部结构槽位，同时由代码生成一份不增加模型调用的 `video_generation_brief`，只把人物场景、单一生活事件、3段宏观画面、商品锚点和连续口播交给视频模型，后台字段不得逐项转写成表演任务；
- 最终视频模型主输入只保留人物、场景、穿搭、一句话生活事件、三段宏观画面、一句动态执行重点和连续口播；开始/结果/收尾状态、结构槽位、claim 血缘、内部解释、重复镜头规则及质检信息继续保留在后台，不重复投喂视频模型；动态执行重点只提醒缩短开头准备动作、把主要观看时间留给核心商品结果，属于软提示，不新增阻断、重试或模型调用；
- 最终质检只保留 `opening_not_static / no_checklist_action` 两个文本计划软信号，并明确不能据此判断实际成片情绪或留存；字幕不在本轮范围内；
- 输出乱序盲审稿和独立答案表。机器规则通过只记为 `MACHINE_SCREENED`，不能替代泰语母语审核和内容人工审核。
- 完整脚本蓝图默认由 `gpt-5.6-sol/high` 生成；每份蓝图写入 `generation_provenance`，模型或推理强度不一致时不得复用旧蓝图缓存。蓝图 ID 也包含该溯源，因此旧视觉和口播缓存会随蓝图变化自动失效。

运行入口：

```bash
python3 scripts/run_reality_reference_stage0.py --product-code <产品编码> --directions 2
```

## 技术约束

- 任务队列与结果回写都只使用飞书多维表格
- 触发方式只使用单个状态位字段
- 用户最小输入只依赖：
  - `产品图片`
  - `产品编码`
  - `一级类目`
  - `目标国家`
  - `目标语言`
  - `产品类型`
  - `产品卖点说明`（可选）
  - `产品参数信息`（可选，若填写会优先并入 parameter_anchors，并参与输入哈希）
- 默认主线路模型配置对齐 OpenClaw 当前主 agent（`openai-codex/gpt-5.5`）
- 当前唯一生效线路：`primary`
- 支持通过命令行显式传入：
  - `--llm-route primary`
- 支持通过单独命令查看或写入 OpenClaw 默认线路：
  - `python3 skills/original-script-generator/set_llm_route.py`
  - `python3 skills/original-script-generator/set_llm_route.py primary`
  - `python3 skills/original-script-generator/切换脚本模型.py`
- 主线路支持环境变量覆盖：
  - `ORIGINAL_SCRIPT_PRIMARY_LLM_API_URL`
  - `ORIGINAL_SCRIPT_PRIMARY_LLM_MODEL`
  - `ORIGINAL_SCRIPT_PRIMARY_LLM_API_KEY`
- 阶段0蓝图专用模型支持：
  - `ORIGINAL_SCRIPT_BLUEPRINT_LLM_MODEL`，默认 `gpt-5.6-sol`
  - `ORIGINAL_SCRIPT_BLUEPRINT_REASONING_EFFORT`，默认 `high`
- 正式脚本与轻变体统一遵循：
  - 内部说明全部使用中文
  - 字幕/口播全部输出目标语言
  - 同时附中文对照，便于人工检查

## 默认状态值

待执行：

- `待执行-全流程`
- `待执行-重跑脚本`
- `待执行-重跑全流程`
- `待执行-脚本变体`
- `待执行-重跑脚本变体`

执行中：

- `执行中-输入校验`
- `执行中-锚点分析`
- `执行中-策略生成`
- `执行中-脚本生成`
- `执行中-脚本变体生成`

结束态：

- `已完成`
- `已完成-脚本变体`
- `失败-输入不完整`
- `失败-模型返回异常`
- `失败-JSON解析异常`
- `失败-回写异常`
- `失败-脚本变体输入缺失`
- `失败-脚本变体模型异常`
- `失败-脚本变体解析异常`
- `失败-脚本变体回写异常`

## 默认字段

必填输入字段：

- `产品图片`
- `产品编码`
- `一级类目`
- `目标国家`
- `目标语言`
- `产品类型`
- `任务状态`

可选补充字段：

- `产品卖点说明`
- `产品参数信息`

共享原始脚本表还包含一组由 `lightweight-tryon-video` 独立维护的轻量试穿入口：

- `轻量视频生成数量`：不生成 / 生成 1 个 / 生成 5 个
- `轻量视频状态`
- `轻量视频任务ID`
- `轻量视频错误信息`
- `轻量视频最近触发时间`

原创脚本流水线不得读取或改写这组字段，也不得把它们映射到 `任务状态` 或 `生成变体`；轻量任务由 `skills/lightweight-tryon-video` 独立巡检。

使用原则：

- 有人工说明就轻用，可作为设计灵感、好意头、轻寓意、送礼背景、卖点提醒、表达限制的优先参考
- 没有人工说明就不脑补，不得仅凭图片主动推断设计来源、寓意、宗教、民俗或功效含义
- 涉及寓意时，只能写成“设计灵感 / 好意头 / 轻寓意 / 祝福感”，不得扩写成招财、转运、保平安、开运、灵验、带来结果等强承诺

推荐约束：

- `一级类目` 固定为 `女装 / 配饰`
- `产品类型` 作为二级细分类目，例如 `上装 / 耳环 / 项链`

建议系统字段：

- `输入哈希`
- `最近执行时间`
- `错误信息`
- `执行日志`
- `阶段耗时`

建议中间字段：

- `锚点卡_JSON`
- `四套策略_JSON`
- `Final_S1_JSON`
- `Final_S2_JSON`
- `Final_S3_JSON`
- `Final_S4_JSON`
- `EXP_S1_JSON`
- `EXP_S2_JSON`
- `EXP_S3_JSON`
- `EXP_S4_JSON`
- `脚本_S1_质检_JSON`（可选）
- `脚本_S2_质检_JSON`（可选）
- `脚本_S3_质检_JSON`（可选）
- `脚本_S4_质检_JSON`（可选）
- `视频提示词_S1_JSON`（可选）
- `视频提示词_S2_JSON`（可选）
- `视频提示词_S3_JSON`（可选）
- `视频提示词_S4_JSON`（可选）
- `变体_S1_JSON`（可选）
- `变体_S2_JSON`（可选）
- `变体_S3_JSON`（可选）
- `变体_S4_JSON`（可选）

最终输出字段：

- `脚本_S1`
- `脚本_S2`
- `脚本_S3`
- `脚本_S4`
- `视频提示词_S1`（可选）
- `视频提示词_S2`（可选）
- `视频提示词_S3`（可选）
- `视频提示词_S4`（可选）
- `脚本1变体1`
- `脚本1变体2`
- `脚本1变体3`
- `脚本1变体4`
- `脚本1变体5`
- `脚本2变体1`
- `脚本2变体2`
- `脚本2变体3`
- `脚本2变体4`
- `脚本2变体5`
- `脚本3变体1`
- `脚本3变体2`
- `脚本3变体3`
- `脚本3变体4`
- `脚本3变体5`
- `脚本4变体1`
- `脚本4变体2`
- `脚本4变体3`
- `脚本4变体4`
- `脚本4变体5`
- `输出摘要`

## 当前架构

系统当前按以下职责运行：

1. P1 产品锚点卡
2. P2 内容强策略卡
3. P3 表达扩充计划
4. P4 正式脚本生成
5. P5 独立脚本质检
6. P6 脚本修订
7. P7 最终视频提示词生成
8. P8 轻变体生成

关键约束：

- 当前原创适配器固定输出 4 个技术槽位：`S1 / S2 / S3 / S4`；共享路由器本身支持任意 N 个方向
- S1-S4 不再代表固定视频结构；每次任务按 `叙事家族 × 视觉执行原型 × proof 表达` 动态绑定结构合同
- 历史 `PROMPT_ONLY` 结构的镜头数始终保持 `UNAVAILABLE`，不得从反推提示词编造镜头硬约束
- 当前原创 Schema 只支持 4-6 镜，因此会过滤明确不兼容的单镜到底或超多镜头视频簇
- 结构合同会先由代码编译成 `structure_execution_plan`，逐镜头明确 `structure_beat / carrier_mode / continuity_group / opening_mechanism`；有结构计划时不再套用统一六镜头模板
- 正式脚本先做基础归一化，再由代码按 `structure_execution_plan` 确定性重建 `shot_skeleton` 并注入 storyboard；模型返回旧字符串 skeleton、缺失 skeleton 或错误结构标签时都不能覆盖权威计划，归一化也不得删除显式结构字段
- `task_type / spoken_line_task` 只负责镜头与口播功能，不再被当作视频结构 Beat；结构 Beat 使用独立字段
- `MIXED` 承载会在执行计划中拆成商品静物、人物承载或手部承载的逐镜头组合，不允许只保留模糊标签
- 固定继承差异字段：`opening_mode / proof_mode / ending_mode / scene_subspace / visual_entry_mode / rhythm_signature / persona_state / action_entry_mode`
- 正式脚本阶段不再重做主卖点和方向分析
- 质检只抓 5 个关键问题：方向跑偏 / 开场无力或错误 / 口播太空 / 分镜太粗 / 产品关键锚点缺失
- 最终视频提示词只做干净转写，不再输出自检、自证和长解释

### 原创批次轻编排 V2

批次分配与单条脚本执行必须保持同一份冻结方向包。`PLAN_ONLY` 会为每个 item 固化结构合同、结构执行计划、真实执行卡、内容论证包、完整创意多样性合同、指定钩子和内容角度；`SCRIPT_ONLY` 不得重新选择人物、场景、承载或开场动作。旧 V1 item 没有 `frozen_direction_package_json`，必须重新规划，不能以兼容名义重新猜测。

15 秒原创批次中，中央卖点库的 `VERIFIED` 卖点直接令内容包获得 `original_15s_eligible=true`；承载匹配不改变卖点授权。中央 `visual_result` 标记为 `WEARER_REQUIRED`，若当前方向是纯静物或纯手部，则该“卖点 × 方向”在计划层记录为 `DEFERRED/WEARER_VISUAL_REQUIRED`，推荐转给真人或混合承载；`benefit` 保持 `FLEXIBLE`，不因静物方向被误伤。该处理不进入脚本质检或口播重试。只有完全没有授权卖点的事实观察候选才最多保留一份用于追踪和后续分流，不复制身份补数量，也不通过第三轮纯钩子变体隐藏补满请求；内容确实不足时仍返回 `PARTIAL_CONTENT_CAPACITY`，这属于正常部分计划而不是运行故障。

执行时只让 `gpt-5.6-sol/high` 生成一次完整蓝图；代码随后把三段连续生活事件确定性投影到结构槽位，不再调用旧视觉适配模型把画面写回商品检查清单。中央口播使用 `creative_full_single_v1` 一次生成完整表达：有卖点论证时建议 11-15 秒并可消费最多两个关联事实，普通事实观察允许 7-11 秒且默认只说一个代表事实；整段跨镜挂载，下游不得重新规划或逐句对齐。

批次执行只设三类硬阻断：完整蓝图/创意合同缺失；与承载方式相匹配的必要制作设定缺失；口播出现内容包未授权的商品效果。钩子强弱、首句与首镜局部错位、语气词数量和叙事锚点是否使用均不进入自动修订循环。

每个批次 item 使用一个 `stage_checkpoint_json` 保存蓝图原始结果、归一化蓝图、视觉计划和完整口播。任一阶段产出后立即落库；同一 `item_snapshot_hash`、冻结方向包和蓝图模型配置下，`--resume` 先复用检查点并重新执行当前校验，不从头调用模型。冻结输入或蓝图模型发生变化时整条检查点自动失效；视觉与口播各自再按依赖指纹判断是否复用。蓝图遇到瞬时容量、5xx、429、连接或超时错误时，先以 `gpt-5.6-sol/high` 发起两次带短退避的相同请求；仍失败才以同一冻结提示词兜底一次 `gpt-5.6-terra/high`。认证、JSON/schema、事实或校验错误不切换模型；长期失败仍交给 item 级 resume，避免无限重试。

批次计划会读取并预留既有 `creative_pattern_usage`，以 `人物角色 × 场景母题 × 开场动作` 的实际近期使用量选择下一组组合；相同请求由冻结批次保证幂等，新批次不得因为同一产品/结构而自动复用旧创意组合。创意历史只影响候选选择，不形成新的脚本失败门槛。

上装真人方向使用 10 套可执行生活事件组合轮换，不再只在办公室衣帽区、公寓玄关、服装收纳架和客厅窗边 4 套通勤模板中循环。新增组合覆盖电梯厅、书店出口、商场连廊、公寓取件、楼下等车和展览入口；这些场景与动作只属于 `CREATIVE_DESIGN`，不能作为卖点证据，也不新增质检规则或模型调用。

```bash
python3 scripts/run_original_batch.py \
  --product-code <产品编码> --count 2 --mode plan-only --seed 1

python3 scripts/run_original_batch.py \
  --product-code <产品编码> --mode script-only --batch-id <新V2批次ID> \
  --limit 2 --resume
```

#### 简化完整脚本旁路（文本灰度）

为避免人物、穿搭、场景、情绪和商品锚点在多层转换中逐步丢失，批次执行器额外提供显式 `simplified_v1`。它不替换现有 `legacy_v2`，也不接飞书、不生成视频：

1. `PLAN_ONLY` 在既有冻结方向包内额外固化 `simplified_creative_seed`，只包含商品事实、内容角度、指定钩子、宏观结构、生成前选定的承载方式和兼容的视觉参考；不新增数据库或服务。
2. `gpt-5.6-sol/high` 一次生成完整视觉脚本，同时交付人物外形、完整穿搭、具体场景、自然情绪变化、一个连续生活事件和 4-6 个分镜。宏观结构只控制观看顺序，不再确定性投影成统一镜头骨架。
3. 中央口播继续唯一负责泰语钩子、称呼/观众关系、语气词、事实选择和完整口语表达；视觉脚本只交付创作语境与全片证据，不再输出或消费 `spoken_claim_keys`。中央口播直接从内容论证包中选择全片已有证据的 1-2 个关联事实，视觉结构不得替口播选题，也不得因 `DETAIL_MACRO / visual_hook_type` 自动决定口播钩子。
4. 下游只做确定性装配，不得重写人物、场景、事件、分镜或整段口播。自动硬阻断只保留三类：输出不可用、商品事实/锚点越权或在全片完全无证据、承载方式明显冲突；风格强弱与局部对齐只供人工审阅。客观商品事实继续受授权边界约束，但称呼、观众关系、个人选择标准、个人偏好、自然衔接和轻收尾属于中央口播的表达自由，不要求 `claim_ref`，也不得被改写成普遍商品功效。
5. 对服饰类，先在内容事实与结构承载之间做一次归并：静物结构只要有纽扣、口袋、领型等可独立拍清的事实，就保留 `STATIC_PRODUCT` 并只向该方向提供兼容事实；只有当前可用事实全部依赖“上身/试穿/穿搭/腰线/版型”等真人证据时才切到 `PERSON_ON_CAMERA`。结构执行参考与最终承载不兼容时直接跳过，不让错误参考压平原创脚本。
6. 15 秒仍是视频时长，不是强制口播填满时长。有完整 `SELLING_ARGUMENT` 时口播建议 11-15 秒，可选择两个直接服务同一价值的事实；只有 `FACTUAL_OBSERVATION` 时默认只说一个代表事实并允许 7-11 秒，剩余时间交给画面、音乐或自然声，不得用参数清单或“容易忽略 / 值得留意 / 靠近看更吸引”等元话语补长度。

文本灰度执行：

```bash
python3 scripts/run_original_batch.py \
  --product-code <产品编码> --count 3 --mode plan-only --seed <新随机种子>

python3 scripts/run_original_batch.py \
  --product-code <产品编码> --mode script-only --batch-id <新批次ID> \
  --script-mode simplified_v1 --limit 3 --resume
```

默认仍为 `legacy_v2`。只有同一产品至少 3 条简化脚本在文本盲审中确认完整度、自然度和差异性后，才进入 1 条视频侦察；不得因启用旁路而直接扩大视频生成。

批次 CLI 每次同时输出 `batch_plan_report.json` 和 `batch_complete_scripts.md`。后者必须完整展示人物身份、外貌、妆发、说话人格、基础穿搭、商品角色、配饰、场景、光线、三段情绪、连续口播和逐镜商品锚点；静物方向应明确显示人物字段不适用，不能为了版面完整虚构人物。

### 阶段0：真实执行参考分支

为解决“结构合同不同，但模型仍把画面写回统一 AI 骨架”的问题，仓库内提供一条默认不接生产的文本实验分支：

1. 结构合同仍决定宏观 Beat、承载方式和连续性。
2. 只从 `VIDEO_INDEPENDENT` 指纹编译真实执行卡；`PROMPT_ONLY` 只能用于定位同源视频，不能冒充镜头事实。
3. 每个方向先生成一个紧凑的 `content_bundle_brief`：只有一个内容主线，但包含 2-3 个不重复、可验证的卖点原子；P2-Lite 仅保留为旧接口兼容投影，不再承担内容密度控制。
4. 模型先根据真实执行卡适配画面，必须携带 `execution_card_id`、`content_bundle_id`、`reference_spine_orders` 和每镜 `supported_claim_keys`；内容包中的每个卖点必须至少有一个真实可见支持镜头，不得补写源视频未观察到的场地、灯光和创作者身份。
5. 画面通过后再调用中央口播引擎：结构与真实执行卡约束“怎么拍”，原创 `voiceover-argument-contract-v1` 只交付核心价值、用户顾虑、证明主线、事实证据和既有创作上下文，中央口播 V32 决定“具体怎么说”。阶段0作业库仍保持隔离，但启动时自动加载版本化中央话术知识快照；每个候选最多检索 2 条明确授权的兼容样本，原文只用于学习观众关系、句子节奏、卖点衔接和收尾，不能继承样本商品事实、品牌、数据、功效、CTA 或逐句翻译，中文样本也不冒充泰语本地措辞证据。中央引擎会从既有 `event_context / core_result_moment / scene_moment` 确定性整理最多 3 个叙事锚点，生成时最多自然使用一个；锚点缺失或未使用只参与候选软排序，不阻断。15 秒默认选择 2 个最有区分度且可验证的事实，候选按钩子对应的完整修辞路径展开，差异必须延伸到中段或收尾，不能只换第一句。称呼、观众指代、轻反应词和句尾语气词按候选角色择一自然使用，不要求每条同时具备，也不作为硬阻断。事实安全、语言不可用、时长确实无法容纳和显式 `MUST_SILENT` 冲突继续硬阻断；钩子平、参数清单、叙事锚点未使用和抽象结论只记录软质量警告，不再自动循环润色。人工明确选择超长候选时，才允许一次既有压缩修订。口播不必逐句对齐首镜，只要整条视频存在事实证据；通用 CTA 和强制静默尾巴均不再要求。已批准兼容话术样本为空时必须输出 `UNAVAILABLE`，不得假装已经学习样本风格。
6. 真实性质检只阻断旧流水线动作链、抽象 AI 指令、事实编造、整片无证据、显式硬静默冲突、语言不可用、时长不可能和执行卡血缘丢失。局部镜头顺序、逐句落点、是否覆盖全部镜头只作提示。
7. 可用方向不足时允许只返回 1-3 条，不为凑满 S1-S4 编造参考。

该分支不写飞书、不生成视频、不生成变体，口播使用独立 SQLite 库：

```bash
python3 skills/original-script-generator/scripts/run_reality_reference_stage0.py \
  --product-code 1734482585843304442 \
  --directions 2 \
  --voiceover-model-command 'python3 /Users/likeu3/voiceover_copy_engine/scripts/codex_model_command.py'
```

只复用已通过的人物、场景和画面，生成 3 个不同钩子入口的口播候选：

```bash
python3 skills/original-script-generator/scripts/run_reality_reference_stage0.py \
  --product-code 1734257377321977850 \
  --directions 1 \
  --voiceover-candidates-only \
  --voiceover-candidate-count 3 \
  --blueprint-model gpt-5.6-sol \
  --blueprint-reasoning-effort high \
  --voiceover-model-command 'python3 /Users/likeu3/voiceover_copy_engine/scripts/codex_model_command.py'
```

输出额外包含 `voiceover_candidates.md`。人工选定后，去掉
`--voiceover-candidates-only`，保留 `--voiceover-candidate-count 3`，并增加
`--selected-voiceover-candidate-id VOC_1_PAIN_REFRAME` 继续组装同一方向；候选 ID 不存在时必须失败，不能悄悄回退第一稿。

阶段0还提供隔离的完整口播直创 A/B 入口。它只从既有阶段0结果提取已验证事实、创意上下文、叙事锚点、授权话术参考、`top_category / product_type / display_family / creative_product_profile` 和 V32 关系语言；绝不把旧候选文本发送给模型。类目字段只影响说话视角，不增加验证门槛。中央口播 `creative_full_script_v1` 一次生成需求切入、生活时刻切入和细节发现切入三条完整口播，生成前不经过 `copy_plan_v2`，生成后只做事实引用、语言字段和总时长检查，不自动修订：

```bash
python3 scripts/run_creative_full_voiceover_stage0.py \
  --source-result /path/to/stage0_result.json \
  --product-code <产品编码> \
  --output-dir /path/to/output
```

该入口默认只输出完整直创候选及乱序 A/B 盲审文件，不写飞书、不生成视频，也不改变正式生产默认模式。人工选定候选后，可复用候选结果继续装配完整脚本与最终视频提示词；整段口播会作为跨镜语义段原样挂载，下游不得重新规划或改写：

```bash
python3 scripts/run_creative_full_voiceover_stage0.py \
  --source-result /path/to/stage0_result.json \
  --candidate-result /path/to/creative_full_script_result.json \
  --selected-candidate-id FULL_B_LIVED_MOMENT \
  --output-dir /path/to/downstream_output
```

这条续跑路径同样不生成视频、不写飞书；旧 `copy_plan_v2` 模式继续保留兼容。

仅预览结构与真实执行卡，不调用模型和口播：

```bash
python3 skills/original-script-generator/scripts/run_reality_reference_stage0.py \
  --product-code 1734482585843304442 \
  --directions 2 \
  --preview-only
```

输出位于 `structure_router_test/reality_reference_stage0/`，包含结果 JSON、盲审文档、答案键和隔离口播数据库。正式主流程默认仍不启用这条分支；只有阶段0盲审通过后才允许接入生产开关。

预留开关如下，默认 `ORIGINAL_SCRIPT_REALITY_REFERENCE_ENABLED=0`：

- `ORIGINAL_SCRIPT_REALITY_REFERENCE_ENABLED`
- `ORIGINAL_SCRIPT_REALITY_REFERENCE_STRICT`
- `ORIGINAL_SCRIPT_VOICEOVER_AFTER_VISUAL`
- `ORIGINAL_SCRIPT_AUTHENTICITY_QC_ENABLED`

结构路由默认开启：

- `ORIGINAL_SCRIPT_STRUCTURE_ROUTER_ENABLED=0`：临时关闭并降级到旧流程
- `ORIGINAL_SCRIPT_STRUCTURE_CONTRACT_STRICT=1`：最终视频提示词出现结构硬违规时直接阻断；默认只在脚本质检阶段阻断，视频提示词阶段记录告警
- 路由读取 `LIKEU_AI_DATABASE_URL`，只读 `sd_*`；选择运行、方向合同和生产绑定写入 `sr_selection_run / sr_direction_assignment / sr_application_binding`

## 本地数据库

中间过程数据库默认保存在：

- `/Users/likeu3/.openclaw/shared/data/original_script_generator.sqlite3`

支持环境变量覆盖：

- `OPENCLAW_SHARED_DATA_DIR`
- `ORIGINAL_SCRIPT_GENERATOR_DB_PATH`

数据库会保存：

- 每次运行的 `record_id / 产品编码 / 输入哈希 / 状态 / 耗时`
- 每个阶段的 `prompt / 输入上下文 / 输出 JSON / 渲染文本 / 错误信息`

按产品编码查询：

```bash
python3 skills/original-script-generator/query_history.py --product-code "你的产品编码"
```

如需连同 prompt 和输出一起看：

```bash
python3 skills/original-script-generator/query_history.py --product-code "你的产品编码" --show-prompts --show-output
```

## 运行方式

```bash
python3 skills/original-script-generator/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx" --max-workers 1
```

查看或重写当前默认线路：

```bash
python3 skills/original-script-generator/set_llm_route.py
python3 skills/original-script-generator/set_llm_route.py primary
python3 skills/original-script-generator/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx" --limit 2
```

也可以直接用中文别名查看或切到主线路：

```bash
python3 skills/original-script-generator/切换脚本模型.py 主线
python3 skills/original-script-generator/切换脚本模型.py 主线路
python3 skills/original-script-generator/切换脚本模型.py 默认主线
```

先小批量验证：

```bash
python3 skills/original-script-generator/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx" --limit 5 --max-workers 1
```

脚本自身常驻轮询，每 1 小时检查一次待执行任务：

```bash
python3 skills/original-script-generator/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx" --llm-route primary --watch --poll-interval-seconds 3600 --max-workers 1
```

只查看待处理记录：

```bash
python3 skills/original-script-generator/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx" --dry-run
```

显式指定主线路执行：

```bash
python3 skills/original-script-generator/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx" --llm-route primary --limit 2
```

只重跑母版脚本和变体，不回到锚点卡 / 策略卡 / 表达计划：

```bash
python3 skills/original-script-generator/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx" --record-id "你的record_id" --force-rerun-script --llm-route primary
```

按任务编号只重跑指定脚本位，并自动续跑该脚本位的变体：

```bash
python3 skills/original-script-generator/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx" --task-no "003" --force-rerun-script --script-index 2 --script-index 4 --llm-route primary
```

按任务编号整条任务全流程重跑：

```bash
python3 skills/original-script-generator/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx" --task-no "003" --force-rerun-all --llm-route primary
```

中后段超时或失败后，按任务编号断点续跑，复用同输入哈希下已经成功的上游阶段：

```bash
python3 skills/original-script-generator/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx" --task-no "003" --force-rerun-all --resume-from-latest-success --llm-route primary
```

默认还会启用两类提速缓存：

- `ORIGINAL_SCRIPT_ENABLE_CONTRACT_REGISTRY=1`：SKU 级 P1 锚点卡 / `category_execution_contract` 缓存，key 包含产品图哈希、产品类型、卖点/参数和 schema version。
- `ORIGINAL_SCRIPT_TEMPLATE_VIDEO_PROMPT=1`：P7_VIDEO 默认由 P7 结构化脚本本地模板渲染，保留 gaze / 微反应 / 身体语言；模板校验失败时才回退 LLM。
- Q1 已启用三层分流：代码侧 L0 结构/时间/语言检查 + L1 契约/音频/人物表演检查先生成 `pre_qc_result`，再交给语义 Q1 兜底；语义 Q1 不允许推翻 high-confidence 硬违约。
- Q1 无需修改时只返回精简质检结果和空 `repaired_script`；只有确实修订时才回传完整脚本，避免大 JSON 重复输出导致超时或字段漂移。
- 结构合同校验优先读取显式结构字段；旧脚本关键词推断不能作为新结构合同的通过依据。
- 阶段复用顺序：同 input_hash 成功阶段优先，其次按 `stage_cache_key` 跨运行复用，最后才调用 LLM。
- P1 / P4 / P5 / P7 / Q1 已加入稳定 prompt 前缀，尽量让长规则区保持一致，动态 JSON 放在后续输入区。
- Q1 precheck 会先做极小范围本地修正：移除 forbidden_sfx 命中的音效 cue，补齐空泛/缺失的镜头级微反应；不会改商品 proof 主线。
- `ORIGINAL_SCRIPT_DEFER_VARIANTS_AFTER_MOTHER=1`：若勾选了生成变体，默认先落库母体脚本，后续巡检自动补齐变体；设为 `0` 可恢复同轮生成变体。

只重跑变体：

```bash
python3 skills/original-script-generator/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx" --record-id "你的record_id" --force-variants --llm-route primary
```

按任务编号只重跑某几个脚本位的变体：

```bash
python3 skills/original-script-generator/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx" --task-no "003" --force-variants --script-index 1 --script-index 3 --llm-route primary
```

查看当前 OpenClaw 默认线路：

```bash
python3 skills/original-script-generator/set_llm_route.py
```

或：

```bash
python3 skills/original-script-generator/切换脚本模型.py
```

## OpenClaw 定时任务

推荐将原创脚本的日常跑批固化为 OpenClaw 自动任务，默认方案如下：

- 每 `1` 小时检查一次
- 固定使用 `primary`
- 只处理飞书表格中状态为 `待开始 / 待执行` 的任务
- 并发数固定为 `1`
- 工作目录固定为：
  - `/Users/likeu3/.openclaw/workspace/skills/original-script-generator`

推荐自动任务执行内容：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/original-script-generator/run_pipeline.py --feishu-url "https://gcngopvfvo0q.feishu.cn/wiki/ZezEwZ7cKiUyeakdlI3cUuU1nRf?table=tblHRLMr9b3fvxBw&view=vewPpvR2oT" --llm-route primary --max-workers 1
```

推荐自动任务名称：

- `原创脚本小时巡检`

推荐自动任务说明：

- 每小时自动检查一次原创脚本流水线待执行队列
- 固定走 `primary`
- 默认并发 `1`
- 跑完后汇报成功数、失败数和失败主因
