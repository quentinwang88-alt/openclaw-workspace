---
name: original-script-generator
description: |
  原创短视频脚本自动生成 skill。读取飞书多维表格中由状态位驱动的任务，
  基于产品图片、目标国家、目标语言、产品类型，自动生成 4 套原创短视频内容强策略卡、4 条经独立质检通过的正式脚本，其中 S4 作为实验增强方向测试高惊艳首镜上限。
  每条脚本会先过独立质检，必要时自动修订，再生成最终视频提示词；默认先只生成母体脚本，只有当表格里勾选了 `生成变体` 时，才会在同轮或后续巡检中补跑 S1 / S2 / S3 / S4 的轻变体；如需只生成部分脚本的变体，可显式传 `--variant-script-index`。
  模型调用当前只保留与 OpenClaw 主 agent 对齐的 `openai-codex/gpt-5.4` 主线路；其它历史线路参数和外部模型调用均已废弃，不再使用。
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
4. 生成 `四套策略_JSON` 字段中的 4 套内容强策略卡（S1 / S2 / S3 / S4）
5. 回写 `Final_S1_JSON / Final_S2_JSON / Final_S3_JSON / Final_S4_JSON`
6. 为 4 套策略分别生成 `EXP_S1_JSON / EXP_S2_JSON / EXP_S3_JSON / EXP_S4_JSON`
7. 为 4 套策略分别生成正式脚本
8. 对每条脚本单独做独立质检，必要时自动修订
9. 只对通过质检的脚本生成最终视频提示词
10. 默认先只生成 S1 / S2 / S3 / S4 四条正式脚本；如果表格里一开始就勾选了 `生成变体`，则同轮继续生成 5 个轻变体并回写 `变体_S1_JSON ~ 变体_S4_JSON` 与 20 个可读变体字段；如果是母体完成后才补勾选，则由下一轮巡检自动触发脚本变体分支；如需只跑部分脚本的变体，可显式传 `--variant-script-index`
11. 更新 `输出摘要 / 输入哈希 / 最近执行时间 / 错误信息 / 执行日志 / 阶段耗时`

同时会把每次运行的中间过程落到本地 SQLite，便于按产品编码追溯。

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
- 默认主线路模型配置对齐 OpenClaw 当前主 agent（`openai-codex/gpt-5.4`）
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

- 固定输出 4 个方向：`S1 / S2 / S3 / S4`
- 固定继承差异字段：`opening_mode / proof_mode / ending_mode / scene_subspace / visual_entry_mode / rhythm_signature / persona_state / action_entry_mode`
- 正式脚本阶段不再重做主卖点和方向分析
- 质检只抓 5 个关键问题：方向跑偏 / 开场无力或错误 / 口播太空 / 分镜太粗 / 产品关键锚点缺失
- 最终视频提示词只做干净转写，不再输出自检、自证和长解释

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
