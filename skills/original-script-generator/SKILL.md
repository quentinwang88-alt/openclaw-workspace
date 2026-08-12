---
name: original-script-generator
description: |
  原创短视频脚本自动生成 skill。读取飞书多维表格中由状态位驱动的任务，
  基于产品图片、目标国家、目标语言、产品类型和视频结构路由合同，自动生成 4 套原创短视频内容强策略卡、4 条经独立质检通过的正式脚本。S1-S4 仅作为兼容输出槽位，具体叙事与画面结构由每轮路由结果决定。
  每条脚本会先过独立质检，必要时自动修订，再生成最终视频提示词；默认先只生成母体脚本，只有当表格里勾选了 `生成变体` 时，才会在同轮或后续巡检中补跑 S1 / S2 / S3 / S4 的轻变体；如需只生成部分脚本的变体，可显式传 `--variant-script-index`。
  正式生产主流程只保留与 OpenClaw 主 agent 对齐的 `openai-codex/gpt-5.5` 主线路；阶段0完整脚本蓝图单独使用 `gpt-5.6-sol/high`，视觉适配仍使用主线路，中央口播命令固定使用 `gpt-5.6-sol/high`。
  OpenClaw 的自然语言入口：用户说“跑原创脚本”“执行短视频运营任务”“给产品 173... 生成原创脚本”“检查待执行原创任务”“续跑原创任务”“重新规划原创任务”时必须使用本 skill 的白名单任务适配器；任务数量始终从飞书运营任务表读取，不能把自然语言中的“10条脚本”误传为任务行数。
  不用于给已经上传的视频后配口播；“人工上传视频配口播/人工上传表口播”必须交给 manual-upload-voiceover，禁止调用本 skill 的 run_feishu_operation_tasks.py。
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

当用户在飞书多维表格中把 `任务状态（需填写，仅选择待执行）` 设为待执行状态后，这条流水线会：

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
- 创意多样性合同额外携带不参与硬校验的类目软适配档：`WORN_APPAREL / WORN_ACCESSORY / HAND_STATIC_ACCESSORY`。围巾、帽子、耳饰、项链、包、腕饰和发饰优先从已经佩戴或造型完成的生活状态进入；戒指保留手部/静物软偏好。结构合同仍是最终承载权威，软适配不得覆盖它；
- 内容论证包先确定 `primary_hook_id`，再生成带 `CREATIVE_DESIGN` 权威标记的完整脚本蓝图，使画面开场和中央口播从同一个钩子意图出发；
- 内容论证包以中央卖点目录为内容权威：飞书中已经人工确认的每个编号卖点都自动获得原创使用资格；中央概念映射只补充 `allowed_strength / claim_type / carrier` 标签，映射失败不得删除卖点。画面语义匹配只记录 `proof_match_status=MATCHED/UNMATCHED` 并辅助选择细节，不得否决或降级卖点。未匹配时扣子、口袋等事实只能作为并列产品细节，禁止被写成卖点成立的原因；
- 蓝图沿用紧凑的 `retention_hook`，同时生成一件完整的 `event_design` 生活事件和 3 段 `macro_visual_passages`；开头只要求从人物原本就在进行的自然动作中途开始，不强制快速入画、停顿、抬眼或表演情绪；
- 由真实视频执行参考控制动作关系，由结构合同控制节奏；内容论证包 V4 明确分开“核心价值、用户顾虑、2–3 个可见证据”，不再把可见结构事实误当成完整卖点；
- 结构计划继续控制宏观 Beat、承载方式、连续性和开场机制；3 段事件画面由代码确定性投影到现有4至6个兼容槽位，不再调用独立视觉适配模型；相邻槽位只延续同一事件过程，同一画面可以同时支持多个卖点；
- `event_design` 是人物在该场景本来就要完成的一件普通事情；核心结果由事件过程证明，扣子、口袋、袖型等商品细节只需保持可见，不分配逐项指向、触摸或核对动作；`reaction_points` 可为空，不要求人物表演情绪；
- 原创流程只通过 `voiceover-argument-contract-v1` 决定讲什么，不再维护自己的目标语言钩子措辞模板；自然称呼、观众指代（如“你们/谁正在……”）、钩子表面表达、语气词、跨镜语义段和本地语言表达统一由中央口播引擎负责。批次冻结的 `目标语言` 是唯一语言权威：泰语、越南语和马来语共用同一套钩子与卖点合同，但分别注入本地关系语言和语气表面；非泰语任务必须清除提示中的泰语示例，生成后进行轻量文字系统校验，错语种在脚本装配和写飞书前硬阻断，且目标国家、目标语言和类目进入口播缓存依赖，不能复用其他市场的旧口播。中央口播 V36 会把完整钩子原型的 `core_intent / attention_mechanisms / minimal_structure / relation_modes` 作为整段修辞路径传给生成器，而不是只传 `hook_id` 或只改第一句；批次层只按钩子兼容性给出一个可放弃的观众关系软偏好，每5条最多建议1条显式“姐妹们”式称呼，不强制实现。话术样本优先按同 `hook_id + 国家 + 类目` 读取；精确类目缺失时，可按同 `hook_id + 国家 + FEMALE_FASHION_WEARABLE` 表达族读取最多两条，只学习观众关系、句子节奏、衔接和信息密度，不得继承商品事实、材质、功效、用法、CTA或原句，且仍禁止跨钩子兜底。正式口播使用“一个核心卖点＋一个同主题信息＋轻收尾”的软密度目标，不引入第二卖点；钩子ID血缘与钩子表面实现分别记录，表面弱只是观察信号，不新增失败门槛、重试或自动润色；
- 中央口播采用三段语义计划：自然抓人开场、连续卖点论证、轻收尾。计划由代码确定性生成，不再额外调用模型决定逐句落在哪个镜头；
- 画面只需要在整条视频中为口播事实提供证据。局部先说后拍、跨镜延续和顺序轻微不同只记录警告；保守时长上界超出也只警告，中心估时超过成片时长 20% 才按不可容纳阻断；只有编造事实、整片无证据、语言不可用、确实无法容纳或显式 `MUST_SILENT` 冲突才阻断；
- 支持只生成 3 个中央口播候选供人工选择；候选必须使用不同 ACTIVE 钩子原型，画面、人物和场景保持不变，未显式选择前不继续组装完整脚本；
- 人物、场景、妆发、完整穿搭和表演动机必须进入最终脚本的 `production_design`，不能只留在内部蓝图；静物和手部方向必须明确标出人物不出镜或仅手部出镜；
- 完整脚本继续保留4至6个内部结构槽位，同时由代码生成一份不增加模型调用的 `video_generation_brief`。内部 `capture_rhythm_contract` 会把结构槽位确定性归并成2至3个真实手机拍摄单元，片段间直接剪切；人物、商品、穿搭、地点、时刻和手机保持连续，但不再把原生感等同为固定机位一镜到底。视频模型只接收人物场景、单一生活事件、拍摄单元、商品锚点和连续口播，后台字段不得逐项转写成表演任务；
- 最终视频模型主输入只保留人物、场景、穿搭、一句话生活事件、三段宏观画面、一句动态执行重点和连续口播；开始/结果/收尾状态、结构槽位、claim 血缘、内部解释、重复镜头规则及质检信息继续保留在后台，不重复投喂视频模型；动态执行重点只提醒缩短开头准备动作、把主要观看时间留给核心商品结果，属于软提示，不新增阻断、重试或模型调用；
- 最终质检只保留 `opening_not_static / no_checklist_action` 两个文本计划软信号，并明确不能据此判断实际成片情绪或留存；字幕不在本轮范围内；
- 输出乱序盲审稿和独立答案表。机器规则通过只记为 `MACHINE_SCREENED`，不能替代对应目标语言的母语审核和内容人工审核。
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

15 秒原创批次中，飞书人工确认卖点直接令内容包获得 `original_15s_eligible=true`；中央 `VERIFIED` 概念只作为可选治理标签。承载匹配不改变卖点授权。已映射的 `visual_result` 标记为 `WEARER_REQUIRED`，若当前方向是纯静物或纯手部，则该“卖点 × 方向”在计划层记录为 `DEFERRED/WEARER_VISUAL_REQUIRED`，推荐转给真人或混合承载；其余卖点保持 `FLEXIBLE`，不因概念未映射或静物方向被误伤。批次不再固定限制每个卖点最多两条，而是在有限的“结构 × 卖点”组合池中按当前使用次数最少优先分配；例如 12 条、5 个卖点应接近 `3/3/2/2/2`。只有组合池确实耗尽时才返回 `PARTIAL_CONTENT_CAPACITY`。

执行时只让 `gpt-5.6-sol/high` 生成一次完整蓝图；代码随后把三段连续生活事件确定性投影到结构槽位，不再调用旧视觉适配模型把画面写回商品检查清单。中央口播使用 `creative_full_single_v1` 一次生成完整表达：有卖点论证时建议 11-15 秒，只围绕一个已经拆分的主卖点展开，并至多消费一个直接相关的可见事实；普通事实观察允许 7-11 秒且默认只说一个代表事实。整段跨镜挂载，下游不得重新规划或逐句对齐。同一条人工卖点若被中央标准化为多个独立概念，规划目录必须将其拆成多个可轮换方向，不能把头发状态、静电、光泽等不同主题重新拼回同一条15秒口播。

批次执行只设三类硬阻断：完整蓝图/创意合同缺失；与承载方式相匹配的必要制作设定缺失；口播出现内容包未授权的商品效果。钩子强弱、首句与首镜局部错位、语气词数量和叙事锚点是否使用均不进入自动修订循环。

每个批次 item 使用一个 `stage_checkpoint_json` 保存蓝图原始结果、归一化蓝图、视觉计划和完整口播。任一阶段产出后立即落库；同一 `item_snapshot_hash`、冻结方向包和蓝图模型配置下，`--resume` 先复用检查点并重新执行当前校验，不从头调用模型。冻结输入或蓝图模型发生变化时整条检查点自动失效；视觉与口播各自再按依赖指纹判断是否复用。蓝图遇到瞬时容量、5xx、429、连接或超时错误时，先以 `gpt-5.6-sol/high` 发起两次带短退避的相同请求；仍失败才以同一冻结提示词兜底一次 `gpt-5.6-terra/high`。认证、JSON/schema、事实或校验错误不切换模型；长期失败仍交给 item 级 resume，避免无限重试。

批次计划会读取并预留既有 `creative_pattern_usage`，以 `人物角色 × 场景母题 × 开场动作` 的实际近期使用量选择下一组组合；相同请求由冻结批次保证幂等，新批次不得因为同一产品/结构而自动复用旧创意组合。创意历史只影响候选选择，不形成新的脚本失败门槛。

场景聚类是默认关闭的只读软参考：启用 `ORIGINAL_SCRIPT_SCENE_REFERENCE_ENABLED=1` 后，`PLAN_ONLY` 只读取 `sd_structure_scene_matrix / sd_scene_prototype`，将既有创意候选映射为场景族群，并仅在“精确结构×场景”存在至少 3 个样本且相对全局有正向提升时给予小幅排序偏好。选中的 `scene_reference_contract` 会冻结进 item 与 `simplified_creative_seed`；脚本阶段不再读取 RDS。场景执行卡按“精确结构场景 → 同宏观结构 → 场景族原型 → 既有策划场景母题”四级寻找参考，但最终可执行视觉配方只允许两种连贯来源：一条兼容的真实观察，或整张既有策划场景卡；聚类原型继续参与族群路由和代表样本定位，不再把聚合道具、光线或氛围拼接进策划地点。规划层额外冻结轻量 `scene_request_contract`，只携带标准产品类型、承载方式、场景意图、时段光线需要和拍摄关系；头巾遮阳等已有中央语义可以要求白天观察，并获得户外或半户外证明环境的正向软排序。未知商品类型标签仍可作为弱类目观察；当任务和历史样本的细分类目都已识别且明显不兼容时，不再伪装为同类真实样本，而是完整回退到一张连贯的策划场景卡。同国家候选中明确的场景族或昼夜冲突同样完整回退，不形成脚本失败。`WORK_BREAK / ON_THE_GO / OTHER` 等枚举只保留为 `situation_tags`，不能再写入审美锚点或最终视频提示词。`visual_scene_recipe` 只包含非空的空间关系、材质与色调、光线质感、生活痕迹；原始观察没有相应信息时明确留空，不调用模型补造。每批场景族与具体场景只做软轮换，不能形成固定配额、失败门槛或额外模型修订。

已就绪的 `simplified_v1` 批次如只需升级场景配方，可运行 `scripts/refresh_scene_recipe_batch.py --batch-id <批次ID>` 预演，再加 `--apply` 更新本地。该入口不调用模型，保持结构、卖点、钩子、口播、人物、穿搭、分镜和脚本ID不变，只刷新冻结场景参考、视觉执行合同与确定性视频提示词；显式增加 `--write-feishu` 才会按脚本ID原位更新飞书，且必须与 `--apply` 同时使用。执行前会自动备份原始 item。

创意组合在历史防重复之外增加卖点到场景的软适配分：拍照/探店优先有画面氛围的咖啡厅、精品空间、酒店或商业空间；显贵/复古优先质感室内；通勤优先办公室与交通过渡空间；身形/比例结果优先能清楚观察整体轮廓的背景。适配分不能覆盖同一完整组合的高额重复惩罚，也不形成场景硬门槛。静物方向可使用精品服装工作室或深木衣架的质感陈列环境；承载兼容只读取中央语义字段 `proof_subject / visual_dependency / compatible_carriers`，不从原始文案猜测。`ON_BODY_RESULT / SCENE_USAGE` 只进入真人或混合承载，`PRODUCT_DETAIL` 可进入真人、手部或静物，未知语义保持灵活。商品锚点与卖点同时明确颜色且无交集时，该组合在 PLAN_ONLY 标记 `DEFERRED/VARIANT_MISMATCH` 并改选其他卖点；任一侧颜色未知时不得阻断。

上装人物的表层设计通过统一穿搭选择层独立选择软穿搭合同，不再让穿搭档与场景候选序号绑定。原创流程以只读 Provider 消费轻视频穿搭模板的结构化字段：`适配产品编码`、适用商品类型、商品版型、内搭、下装、配饰、鞋履、风格与配置版本；搭配名称只作为运营展示元数据读取，不进入结构快照和模型提示，搭配核心描述、备注、适合颜色方向或禁止搭配等标题/正文内容仍不读取。`适配产品编码` 包含当前产品编码时为精确候选，包含 `*` 时为通用候选，留空则只供轻视频自身使用、不会进入原创流程。穿搭来源现按严格三级选择：产品专属模板 > 同类通用模板 > 内部轮廓兜底；只在最高可用层级内部按近期用量软轮换。产品只有一套专属模板时允许跨方向重复使用，不再因为“已用一次”切到内部轮廓。任何读取或匹配失败仍无声降级到 `INTERNAL_PROFILE`，不增加质检、模型调用、重试或硬失败。合同继续冻结 `source_type / template_id / template_version / structured_snapshot_hash`，旧 `surface_profile` 仅作兼容投影；批次报告记录来源与模板分布。可用 `ORIGINAL_SCRIPT_SHARED_OUTFIT_PROVIDER_ENABLED=0` 关闭共享 Provider，或用 `ORIGINAL_SCRIPT_OUTFIT_TEMPLATE_DB_PATH` 指向只读模板库。场景执行卡只写相对空间关系、至多两个背景锚点和一个生活痕迹；模型生成的无来源米/厘米精确距离会在归一化阶段确定性删除，道具不能变成必须执行的动作。

穿搭合同现统一升级为 `outfit-selection-v10-persona-affinity`。女装、丝巾、围巾、头巾、腕饰和发饰均优先读取同一份飞书共享穿搭表，再由类目内部候选无声兜底；共享模板与内部候选归一为同一合同，由一个不调用模型的选择器先确定最高可用来源层级，再按商品角色、已冻结展示方式、场景适配和近期用量软轮换。飞书穿搭表的 `适配场景族` 统一显示中文多选：`居家日常 / 咖啡/品质室内 / 街头/外出 / 镜前/试穿 / 办公/通勤 / 乘车/等候`，同步层读入 SQLite 时转换为内部稳定代码，旧英文值在迁移期仍可读取；新增的 `适配人物模板` 在飞书中只显示人物模板名称，同步层按人物库唯一名称解析并存为结构化 `preferred_persona_ids`；冻结合同继续使用稳定人物 ID，人物改名不改写历史血缘。原创规划不再先锁死场景后被动找穿搭，而是对每个场景候选匹配同来源层级的穿搭合同；匹配 `适配场景族` 时仅参与候选排序，产品专属模板按运营配置获得高于通用模板的软加分，允许其优先于一次场景族轮换；完整组合的高额重复惩罚仍会避免逐项照搬，未填写或没有兼容场景时正常回退，绝不形成失败、修订或额外模型调用。最终冻结 `outfit_scene_affinity_contract`，记录模板偏好、实际场景、`MATCHED / NO_PREFERENCE / FALLBACK` 和数据血缘，供批次报告与后续效果回填使用；该内部分数不发送给蓝图模型。现有女装模板使用 `target_role=TARGET_GARMENT`；丝巾/围巾使用 `SUPPORTING_OUTFIT_NECK`，头巾以及丝巾的头部/发尾展示使用 `SUPPORTING_OUTFIT_HEAD`，手链/手镯使用 `SUPPORTING_OUTFIT_WRIST`，抓夹等发饰使用 `SUPPORTING_OUTFIT_HAIR`，不能跨角色误用模板。表格中带“或/、”的上装、下装、鞋包候选会在规划时按种子确定性冻结一个选项；明确配饰中的 `A或B` 也按同一批次种子只冻结一个，明确并列的多个配饰继续共同保留。完整原值只保留在方向包供审计，蓝图模型只接收已经冻结的单一 `outfit_recipe`、`accessory_items` 及其紧凑投影，不再同时看到未选候选。连衣裙和连体裤会被确定性标准化为 `outfit_structure=ONE_PIECE` 与单一 `one_piece` 字段，上装/下装清空，避免模型把连体服装拆成裤装；配饰字段若填写具体帽子、包袋或首饰，会标准化为 `accessory_policy=SPECIFIED + accessory_items`，随冻结配方直接进入完整脚本、首帧和最终视频提示词。穿搭模板名称只用于批次报告和飞书显示，排除在创意 ID、脚本 ID、输入快照和缓存哈希之外；运营改名不会触发重生成。完整 `outfit_recipe / style_family / style_intensity / climate_profile / visibility_zones / demonstration_mode / preferred_persona_ids` 冻结进方向包；最终视频提示词额外确定性携带精简的穿搭配方、发型/领口/外层、配色/可见性/完成效果，不依赖模型在中间层自行记住。除商品露出和角色兼容外，这些均为软设计，不新增失败、修订或重试。

人物身份现通过 `persona-selection-v4-body-proportion` 与商品参考图分权。运营在独立的“原创人物模板库”维护人物参考图、适用国家、一级类目、产品类型、展示方式、身份、外貌、妆发和说话人格，并可用 `身形比例（可选）` 单独维护上/下身比例或头身比；显式执行 `refresh-personas` 后，这些字段会进入人物身份锁与首帧缓存指纹，不再只依赖商品参考图中的模特比例。固定同步器把中文运营字段转换成共享 `persona_templates` 的稳定内部合同，并将当前人物模板 ID 补入穿搭表的 `适配人物模板` 可选项。原创规划只把 `启用 + 人物参考图非空` 的人物视为已批准模板；草稿、停用、纯文字或误设为启用但缺参考图的记录不会被选中。真人方向先过滤国家、类目、标准产品类型、展示方式和拍摄关系，再优先选择当前穿搭关联且兼容的人物；未填写关联或关联人物不可用时，继续按近期用量和优先级安全回退，不让脚本失败。合同冻结 `persona_id / template_version / identity_lock / reference_asset_ids / reference_strategy / structured_snapshot_hash`，并额外冻结 `outfit_persona_affinity_contract`，记录实际穿搭、偏好人物、实际人物、`MATCHED / NO_PREFERENCE / FALLBACK / NOT_APPLICABLE` 与回退原因。随后完整脚本的人物身份、外貌、比例、妆发和说话人格由代码投影，不允许模型重选。商品参考图中的模特、滤镜、姿态和场景明确没有人物权威。静物与手部方向不分配人物。模板库或参考资产不可用时仅记录 `UNAVAILABLE` 并继续旧流程，不增加模型调用、修订或脚本失败。批次 JSON、批次 Markdown、完整生产脚本和飞书生产记录都显示实际采用的人物模板、穿搭模板名称、实际配饰、场景、穿搭×场景匹配状态以及人物×穿搭匹配状态；飞书同时保留稳定模板 ID 与关联 JSON 供审计。头巾、耳饰和发饰默认标记 `PERSONA_PRODUCT_COMPOSITE_REQUIRED`，丝巾、围巾和女装真人方向标记 `PERSONA_PRODUCT_COMPOSITE_PREFERRED`；统一首帧只有在批准人物参考资产和首帧准备器就绪后才可替换商品原始参考图，禁止把 `UNAVAILABLE` 当作已经完成首帧锁定。

人物模板不再要求运营重复切换“启用”。刷新后，只要模板没有明确设为“停用”且存在可用人物参考图，就直接进入兼容人物池；没有参考图的模板仍不进入视频人物选择。穿搭表的关联人物只负责优先级，关联人物不兼容或没有参考图时继续安全回退，不新增脚本失败门槛。

简化完整脚本会从冻结卖点生成一个 `opening_visual_job`，只规定前三秒优先让穿着结果、相关细节、使用场景或静物商品中的一种尽快成立。它是软生成指导，不要求前后对比、身体缺点特写、情绪反转、场景动作，不进入硬校验、自动修订或重试。

可穿戴类现统一使用 `visual-execution-contract-v4-wearable-saliency`，仍然属于同一份视觉执行合同，不增加模型调用、数据库字段或新质检层。V4 在商品锚点、穿搭、场景和 `opening_visual_job` 已确定后，编译软性的 `visual_saliency`：`exposure` 只要求人物脸部与商品处于自然清楚亮部、白平衡正常且不欠曝蒙灰；`separation` 只从已确认锚点判断深色、浅色、棕暖色、多图案或未知，并给出商品相对内搭和背景的明暗/冷暖边界；`opening_focus` 按女装、丝巾、头巾、围巾和静物承载选择首镜焦点与兼容景别，并服从已冻结佩戴状态和 `action_design`。女装新增显著性只影响视觉构图，不把场景事件传给中央口播。完整场景另确定性生成 `opening_scene_projection`：书店、货架、陈列墙等高密度场景只在首帧和第一拍摄单元保留一个地点锚点，密集书架、文字和道具退到侧边或远处；后续片段仍使用完整冻结场景。上述内容全部为 `SOFT_ONLY`，不阻断、不修订、不重试，也不能覆盖商品身份锁、佩戴连续性、穿搭轮廓、结构、卖点、钩子或场景来源。旧 V2/V3 合同继续可读，`ORIGINAL_SCRIPT_VISUAL_SALIENCY_V1_ENABLED=0` 可只关闭显著性扩展。

上装真人方向使用 12 套可执行创意组合轮换，不再只在办公室衣帽区、公寓玄关、服装收纳架和客厅窗边 4 套通勤模板中循环。候选覆盖电梯厅、书店出口、商场连廊、公寓取件、楼下等车、展览入口、咖啡厅窗边和精品酒店休息区；场景与其中已有的画面关系只属于 `CREATIVE_DESIGN`，不能作为卖点证据，也不新增动作要求、质检规则或模型调用。

### OpenClaw 自然语言命令路由

OpenClaw 接到以下明确执行指令时，必须调用唯一适配器，而不是临时拼接 shell、调用旧 `run_pipeline.py`，或自行创建/改写运营任务。

唯一入口：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/original-script-generator/scripts/openclaw_original_script_task.py <action> [白名单参数]
```

| 用户表达示例 | 固定动作 | 适配器调用 |
| --- | --- | --- |
| “检查待执行原创脚本任务”“看看原创脚本队列” | 只扫描，不生成脚本 | `check --limit 1` |
| “跑一条待执行原创脚本任务”“执行短视频运营任务” | 执行一行状态为`待执行`的任务 | `run --limit 1` |
| “给产品 1734482585843304442 跑原创脚本” | 精确定位该产品唯一的待执行任务后执行 | `run --product-code 1734482585843304442` |
| “只规划产品 173... 的原创任务，先不写脚本” | 生成冻结计划，不写生产脚本 | `plan --product-code 173...` |
| “续跑原创任务 recXXXX”“重试失败原创任务 recXXXX” | 只续跑失败/部分完成任务 | `resume --record-id recXXXX` |
| “刷新卖点后重新规划 recXXXX” | 同步卖点后新建规划批次 | `replan --record-id recXXXX` |
| “刷新穿搭模板” | 仅同步轻视频穿搭模板到本地共享库，不生成脚本 | `refresh-outfits` |
| “刷新原创人物模板” | 将原创人物模板库同步到本地共享库，不生成脚本 | `refresh-personas` |
| “检查勾选生成首帧的脚本” | 只预览用户已勾选的首帧任务 | `first-frame-check --limit 5` |
| “生成勾选脚本的首帧” | 生成或复用统一首帧并回写同一脚本行 | `first-frame-run --limit 5` |
| “重试首帧 recXXXX” | 忽略缓存重生成指定脚本的首帧 | `first-frame-retry --record-id recXXXX --limit 1` |
| “检查勾选进入生产的原创脚本” | 只预览将送入运行管理表的脚本 | 调用 `script-run-manager-sync` 的 `check` 入口 |
| “把勾选的原创脚本送生产”“同步产品 173... 的勾选脚本” | 将勾选脚本幂等写入运行管理表 | 调用 `script-run-manager-sync` 的 `sync` 入口 |

路由约束：

- 只提取 `record_id`（必须 `rec` 开头）、`product_code`（8–30位数字）和“最多处理几条**运营任务**”（1–5）。禁止把自然语言直接拼接进命令。
- 飞书任务行的 `生成数量` 才是一个产品要生成几条脚本（1–20）的唯一权威；“生成 10 条脚本”不能映射为 `--limit 10`。
- `run` 只读取并执行任务状态为`待执行`的行；`resume` 只接受`失败/部分完成`；`replan` 只接受`失败/部分完成/已完成`。没有候选时如实回复，不得把空白、已完成或其他状态偷偷改为`待执行`。
- 产品编码匹配到多行任务时必须要求 `rec` 记录 ID，不能猜测执行哪一行。
- 正式执行会读取产品、卖点和结构数据，调用已配置模型，并回写运营任务表和原创视频生产脚本表；**不生成视频、不发飞书消息**。`check`/`plan`不生成脚本内容，`check`不调用模型。
- 原创规划不会自动联网刷新穿搭模板。批次报告会记录共享模板库的 `last_refreshed_at / refresh_status`；默认超过 24 小时只输出软提醒并继续生成，可用 `ORIGINAL_SCRIPT_OUTFIT_TEMPLATE_STALE_HOURS` 调整阈值。需要刷新时显式执行 `refresh-outfits`，避免飞书可用性成为原创生成的硬依赖。
- 原创人物模板同样不会在每条脚本生成时自动联网拉取。运营补完或修改模板后显式执行 `refresh-personas`；同步只更新共享人物配置，不生成脚本、不改变运营任务状态。模板状态为“启用”但没有人物参考图时会以非激活状态落库并在表格标记“同步失败”，不会污染生产人物池。
- 执行结束必须汇报：任务记录 ID、产品编码、批次 ID、请求/计划/完成/失败数、人工确认/可用卖点数、脚本表新增/更新数，以及失败原因（如有）。
- “进入生产”是生产脚本表的唯一同步勾选；同步由 `script-run-manager-sync` 负责，原创脚本生成器不得直接写入短视频自动脚本运行管理表。

#### 用户可选统一首帧

`原创视频生产脚本` 以 `生成首帧（需勾选）` 作为唯一人工决策入口。程序不得因为
`PERSONA_PRODUCT_COMPOSITE_REQUIRED / PREFERRED` 自动替运营勾选，也不得把未生成首帧
变成所有脚本的生产门槛：

- 未勾选：继续使用原商品参考图送生产；
- 已勾选、首帧未就绪：只阻塞这一条脚本送生产；
- 已勾选、`首帧准备状态（系统）=已就绪/缓存复用`：使用 `统一首帧（系统）` 替换原商品参考图；
- 运营在同一脚本行预览首帧，满意后再勾选 `进入生产`；不新增第二张人工入口表。

首帧生成入口：

```bash
python3 scripts/run_first_frame_tasks.py --dry-run --limit 5
python3 scripts/run_first_frame_tasks.py --limit 5
python3 scripts/run_first_frame_tasks.py --record-id recXXXX --force --limit 1
```

首帧图片子进程默认最多运行 480 秒，超过后会被终止并以明确错误收口；可用
`ORIGINAL_FIRST_FRAME_IMAGE_TIMEOUT_SECONDS` 或 `--image-timeout-seconds` 调整。每次启动还会
把超过 900 秒仍为 `GENERATING` 的上次中断任务自动标记为失败并同步飞书，然后允许已勾选任务
正常重试；阈值可用 `ORIGINAL_FIRST_FRAME_STALE_AFTER_SECONDS` 或 `--stale-after-seconds` 调整。
生成调用开始前必须先写入脚本与资产绑定，确保进程中断后仍能定位对应飞书行。

首帧合同在完整脚本之后编译，只冻结已经选定的商品身份、人物、穿搭、场景和开场状态；
不得重新选择结构、卖点、钩子、口播或创意方向。图像提示词必须明确：商品参考图只控制商品，
人物参考图只控制人物，穿搭/场景/开场分别由冻结合同控制。缓存指纹不包含口播、钩子和卖点，
但包含商品身份、人物参考、人物比例、已解析穿搭结构、场景投影、视觉显著性、开场、模型与提示词版本。首帧提示词使用 `original-first-frame-prompt-v2-compact-scene`：高密度场景只保留地点身份与一个侧边/远处锚点，不把书架、货架、文字、收据和道具动作同时塞入主体背后；该投影只影响首帧与第一拍摄单元，不改完整脚本或后续片段场景。

#### 飞书生产工作台

任意 N 条的原创批次不再回写旧“一行产品 + S1-S4 + 20变体”表。生产入口和审核出口分别使用：

- `短视频运营任务表`：一行一个产品批次，以 `任务状态（需填写，仅选择待执行）=待执行` 触发；
- `原创视频生产脚本`：一行一条 `SCRIPT_READY` 脚本，主键使用 `complete_script_id`；
- 原始脚本表继续承担旧流程兼容，不新增批次字段。

运营任务表的 `一级类目（需填写）` 与 `产品类型（需填写）` 使用单选枚举。一级类目只开放 `女装 / 配饰`；产品类型直接复用 `config/product_type_config.json` 中已经通过类型守卫登记的标准显示名称。旧别名在读取时归一为标准值，未登记类型不允许静默兜底进入生产。

`测试阶段（可选，默认初测）` 使用 `初测 / 复测 / 终测 / 放大观察` 四个中文枚举，内部映射为 `INITIAL / RETEST / FINAL / SCALE_OBSERVE`。当前版本只将其用于批次身份、幂等和结果归档，暂不改变结构、钩子或卖点分配；留空按初测处理。

`任务状态（需填写，仅选择待执行）` 是任务触发开关。运营新建任务时只选择 `待执行`；`执行中-规划 / 执行中-脚本生成 / 已完成 / 部分完成 / 失败` 均由程序自动回写，人工不应手动选择。

新工作台字段可幂等创建：

```bash
python3 scripts/ensure_production_workbench_fields.py
```

执行待处理运营任务并把脚本写入审核表：

```bash
python3 scripts/run_feishu_operation_tasks.py --dry-run
python3 scripts/run_feishu_operation_tasks.py --limit 1
```

新工作台使用 `simplified_v1` 时，允许复用结构路由接入前的最近正式 `anchor_card`，但不复用或拼接 stage0 路由；本批次必须从当前 RDS 结构池重新选择结构，并重新读取中央卖点库。失败任务只能用显式记录 ID 续跑：

```bash
python3 scripts/run_feishu_operation_tasks.py \
  --record-id <飞书record_id> --resume-failed
```

每个新规划批次会先调用中央口播引擎自带的 `sync_feishu_product_claims.py --apply --product-code ...`，把飞书已确认卖点按编号幂等同步到中央 SQLite，再冻结卖点快照。每个已确认段落都是独立卖点；概念映射成功与否必须分别统计，但不得影响原创可用资格。同步后完全没有人工确认卖点时必须明确失败，不能把“计划0条”当作部分完成。需要让已有零计划或旧策略批次重新规划时使用：

```bash
python3 scripts/run_feishu_operation_tasks.py \
  --record-id <飞书record_id> --replan
```

任何任务失败都会写回飞书，且命令返回非零退出码，避免外层调度器把业务失败误判为成功。

完整脚本使用飞书多行文本分段格式，保留人物、外貌、妆发、穿搭、场景、自然状态、目标语言口播、中文对照和逐镜设计；视频生成提示词从 `video_generation_brief` 单独渲染，不包含中文翻译、claim ID、质检过程和结构选择解释。导出按脚本ID幂等更新，不能覆盖人工的审核状态、进入生产勾选、审核意见和同步结果。

生产提示词默认使用 `UGC_NATIVE_V1` 交接档。完整脚本仍保留人物、情绪、精确场景和详细分镜供人工审核；交给视频模型的提示词会把商品身份锁和商品负向约束放在最前，随后使用统一手机原生拍摄说明，并把逐镜机位压缩为必要景别。`product-identity-lock-v2` 只从已确认 `identity_anchors / visible_detail_anchors` 确定性编译：参考图是商品外观权威；已明确的单列可见扣与隐藏暗扣会分别表达，隐藏闭合件不得被转写成第二列可见纽扣；真正的双排扣或未知排布不得被擅自改成单排。渲染器会用 V2 重新编译旧视频简报中的 V1 商品锁，因此不需要重跑蓝图或口播。该过程不调用新模型，也不会根据未知属性编造负向约束。旧提示词可通过 `ORIGINAL_SCRIPT_VIDEO_PROMPT_PROFILE=legacy` 临时回退，默认值为 `ugc_native_v1`。

配饰通过可选 `category_execution_extension` 接入，不建立第二条生产流程。启用 `ORIGINAL_SCRIPT_ACCESSORY_PROFILE_ENABLED=1` 后，类型注册表明确识别的耳饰、发饰、腕饰和围巾会在 `PLAN_ONLY` 编译类目执行档案；其中 `围巾 / 秋冬围巾 / 丝巾 / 头巾` 先编译通用围巾档案，再按本条已分配卖点冻结为 `accessory-execution-profile-v5-scarf-action`，不得互相回退成同一个秋冬围巾模板。手链/手镯从已戴好的腕部结果开始，抓夹等发饰从已完成发型开始，并使用镜面或侧后方三分之四手机关系展示后脑区域；两类均禁止误用围巾的垂端、领口动作。旧 P1 类目合同只提供观察，产品类型注册表与类目扩展拥有分型和物理执行权威；除运营卖点明确授权外，不因类型名推断材质、功效、宗教或文化身份。

腕饰卖点的执行语义现统一在中央卖点适配器中轻量编译，不在脚本末端追加修补规则：运营卖点明确写有“单戴/叠戴/戴着/上腕”等佩戴结果时，优先使用手腕或人物承载并从已佩戴状态证明；明确写有“一滑就能套入/容易戴”等过程时，允许一次简单佩戴过程并以稳定佩戴结果结束，禁止摘下后再次佩戴。只讲切面、光泽或外观细节的卖点继续保持承载方式灵活。该语义仅决定如何呈现运营已确认的卖点，不改写卖点、不替运营判断卖点是否成立，也不影响女装、围巾、耳饰和发饰。

腕饰继续默认只允许一只目标商品。只有运营卖点在“叠戴”附近明确写出两个、三个或两三个等数量时，中央适配器才生成 `display_quantity_contract`，并冻结本条全片唯一的同款展示数量；该数量同时进入蓝图输入和最终商品身份锁，已授权同款不视为竞争性配饰，所有拍摄单元必须保持同一数量，不得从单戴切换到叠戴或中途增减。只写“可以叠戴”但没有明确数量时不自动复制商品，其他卖点也不能继承这项授权。

耳饰、腕饰和发饰现额外使用 `accessory-execution-profile-v4-small-prominence`，在既有类目执行档案内冻结软性的 `product_prominence_contract`，不扩展到女装或围巾四类。它只采用“一段商品主导近景＋一段人物/穿搭关系景”：耳饰使用半脸耳侧近景，腕饰使用手腕前臂近景，发饰使用后脑发饰区域近景；发饰镜面关系必须裁到头肩范围，全身镜面和中远景只能交代生活与穿搭，不能承担小商品结构证明。手部/静物方向则让商品与同一人物的手或商品本体成为主要观察区域，只保留少量真实环境。该合同同时进入蓝图和最终视频提示词，只调整兼容内容段的景别，不改变结构、佩戴连续性、动作主线、场景或穿搭，也不新增模型调用、硬质检、自动修订或重试。

多镜头节奏使用 `capture-rhythm-contract-v2-validated-boundaries`：视觉适配模型给出的拍摄单元边界只要数量正确、连续且没有跳号或交错，就直接保留；只有边界缺失或畸形时才按镜头数确定性兜底分组。程序不得把语义上属于同一拍摄单元的连续镜头再次机械切开，最终 `capture_unit_id / starts_new_take / transition_mode` 必须来自同一份分组结果。

围巾类扩展在任务开始时编译一次基础档案，并在每个计划项分配卖点后确定性冻结一种 `primary_demonstration_mode`：`NECK_WORN / HEAD_WORN / HAIR_TIE / BAG_ACCENT`。中央卖点概念只提供展示语义，运营确认原文继续决定讲什么；15 秒单条只执行一种主要用法，`supported_demonstration_modes` 只是授权范围，不得被口播或画面枚举成颈部、头发、包袋清单。对于已经选择 `HEAD_WORN / NECK_WORN / HAIR_TIE` 的使用场景类卖点，卖点适配器会在规划前直接编译 `WEARER_REQUIRED`，只匹配真人或混合结构，不再把遮阳、夏季佩戴或发型整理分给纯静物结构；材质光泽等商品细节主题仍保持灵活，不被一刀切为真人。`MULTI_USE` 会保留人工原始卖点血缘，但交给单条口播和视觉蓝图的是按本条 `primary_demonstration_mode` 收束后的一个用法，不在下游新增逐句拦截或修订循环。每条简化脚本还会从类目安全互动能力、结构 Beat 和既有 `action_grammar` 中确定性冻结一份 `action_design`，在 `RESULT_SHOW / DETAIL_SHOW / SIMPLE_WEAR_PROCESS / HANDHELD_PRODUCT` 中选择一条开始—核心动作—完成状态；场景生活动作只作辅助衔接，不与商品互动叠成动作清单。结构路由不再因为 `RESULT_FIRST_SIMPLE_ADJUSTMENT_ONLY` 就整体弱化 `USE_PROCESS`，但旧的完整包裹、缠绕和复杂系结仍由结构策略与全片物理交互边界禁止。动作命中只产生软提示，不新增模型调用、自动修订、重试或硬失败。场景偏好、三类独立生活场景池与穿搭模板只参与软排序，不新增硬质检。围巾类视频商品身份锁只消费参考图和已批准锚点中的颜色、图案、边框、流苏、形状和 Logo/文字，不从商品名称补写未知细节。结构合同仍决定真人/手部/静物承载，飞书中央卖点决定讲什么，钩子库和中央口播决定怎么说。没有匹配适配器的产品继续走公共轻量动作合同；历史批次需要 `--replan` 才能获得冻结动作设计。

配饰视频交接额外携带 `wear_state_contract` 与软性的 `hand_anatomy_guard`。`RESULT_SHOW / DETAIL_SHOW` 从已经完成的佩戴结果开始，只允许轻调；`SIMPLE_WEAR_PROCESS` 从松结或未完成状态开始，完成一个冻结步骤后再展示结果，视频提示词不得同时写“开场已经佩戴完成”和“重新系结”。手部提示最多出现同一人物的两只手并明确左右手属于同一人，用于降低第三只手与助手手臂风险，但不作为脚本失败门槛，也不宣称能完全消除视频模型的肢体生成误差。

穿戴类正式批次额外冻结拍摄关系：真人方向默认 `capture_mode=CREATOR_SELF_SHOT`，表示创作者本人面对自己的一台手机完成分享；手部和静物方向分别使用 `HANDS_PRODUCT_SHARE / STATIC_PRODUCT_RECORD`。`capture_mode` 是内部合同，不新增飞书人工字段。结构 Beat 继续控制同一生活时刻中的内容推进；公共 `NATIVE_MULTI_CLIP_V1` 拍摄节奏将4至6个结构段确定性归并成真人方向3个、手部/静物方向2个真实手机素材片段，片段间使用普通直接剪切或自然跳剪。同一人物、商品、穿搭、地点、时刻和手机保持连续，但允许在同一小片区域重新放置手机或补录商品细节；不得退回数字裁切、人物走近假装换景别，也不得扩写成摄影团队多机位。缺失拍摄单元时只做确定性归并并记录兜底，不新增模型修订、重试或失败门槛。环境变量 `ORIGINAL_SCRIPT_CAPTURE_RHYTHM_PROFILE=legacy_one_take` 可回退旧连续录制，`native_multiclip_v1` 可显式启用新版。每 5 条穿戴类脚本仍优先形成约 4 条创作者自拍 + 1 条手部/静物补充，结构继续从当前证据池轮换。

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
3. 中央口播继续唯一负责泰语钩子、称呼/观众关系、语气词、事实选择和完整口语表达；视觉脚本只交付创作语境与全片证据，不再输出或消费 `spoken_claim_keys`。卖点模式只向中央口播交付一个已拆分主卖点和至多一个直接相关的全片可见事实；事实观察模式默认选择一个代表事实。视觉结构不得替口播选题，也不得因 `DETAIL_MACRO / visual_hook_type` 自动决定口播钩子。
4. 下游只做确定性装配，不得重写人物、场景、事件、分镜或整段口播。自动硬阻断只保留三类：输出不可用、商品事实/锚点越权或在全片完全无证据、承载方式明显冲突；风格强弱与局部对齐只供人工审阅。客观商品事实继续受授权边界约束，但称呼、观众关系、个人选择标准、个人偏好、自然衔接和轻收尾属于中央口播的表达自由，不要求 `claim_ref`，也不得被改写成普遍商品功效。
5. 结构承载与卖点在 `PLAN_ONLY` 阶段一次性归并：只读取中央卖点语义中的 `visual_dependency / compatible_carriers`，不再根据“显瘦、上镜、穿上”等文案关键词猜承载。明确为 `WEARER_REQUIRED / HAND_REQUIRED / STATIC_REQUIRED` 的卖点只进入兼容结构；`FLEXIBLE` 或未知映射不删卖点。计划冻结后，蓝图、创意场景和脚本都必须沿用同一结构承载，禁止后置把静物或手部结构改写成真人结构。
6. 原始运营卖点文案只保留在冻结内容包并交给中央口播使用。视觉蓝图仅接收卖点 ID、`claim_type / claim_theme / allowed_strength`、承载语义和已归一的 `creative_core_value`；没有归一语义时不得从原始措辞推断人物出身、职业、地域、经济身份或特殊场景。该边界不影响中央口播继续使用人工确认卖点作为内容权威。
7. 15 秒仍是视频时长，不是强制口播填满时长。有完整 `SELLING_ARGUMENT` 时口播建议 11-15 秒，以一个主卖点为内容主线，至多补充一个同主题可见事实；只有 `FACTUAL_OBSERVATION` 时默认只说一个代表事实并允许 7-11 秒，剩余时间交给画面、音乐或自然声，不得用参数清单或“容易忽略 / 值得留意 / 靠近看更吸引”等元话语补长度。

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
- `ORIGINAL_SCRIPT_SCENE_REFERENCE_ENABLED=1`：启用上述场景聚类只读软参考；默认 `0`，不影响既有场景候选轮换
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

## 旧原创脚本表兼容边界

`run_pipeline.py` 与旧“一行产品 + S1-S4 + 变体”表只用于历史记录维护，**禁止**由 OpenClaw 自然语言命令、定时任务或新生产表触发。它不读取新`短视频运营任务表`，也不写入新`原创视频生产脚本`表。

新流程只有两条正式链路：

1. 原创脚本：`openclaw_original_script_task.py` 读取`短视频运营任务表`，写入`原创视频生产脚本`；
2. 脚本送生产：`script-run-manager-sync` 读取勾选`进入生产`的生产脚本，一条脚本对应一条`短视频自动脚本运行管理表`记录。

因此，不再创建或恢复名为“原创脚本小时巡检”的旧 `run_pipeline.py` 定时任务。
