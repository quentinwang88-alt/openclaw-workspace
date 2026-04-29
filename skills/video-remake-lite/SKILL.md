---
name: video-remake-lite
description: |
  轻量级养号视频复刻 skill。读取飞书多维表格里状态为“待开始”的任务，
  将视频直接传给模型分析，按高保真轻量复刻三步生成高光DNA、轻微改写方案和最终固定分镜。
  全链路默认注入“养号高保真轻量复刻”总控约束，避免结果跑成原创或带货视频。
  支持通过飞书链接动态切表。
---

# Video Remake Lite

## 核心能力

当用户发来飞书多维表格链接时，运行这条流水线：

1. 读取表格里状态为 `待开始` 的记录
2. 从表格中提取 `视频`、`店铺ID`、`目标国家`、`目标语言`、`商品类型`、可选 `复刻模式`
3. 将视频直接传给模型分析
4. 顺序生成并回写：
   - `高光DNA提取结果`
   - `轻微改写复刻方案`
   - `最终固定分镜`
   - `负面限制词`
5. 任务完成后把状态改为 `已完成`
6. 如果存在 `同步状态` 字段，会写为 `待同步`

当前三步职责：

1. `高光DNA提取结果`
   判断素材是否适合养号复刻，锁定原视频高光、钩子、动作、情绪和节奏
2. `轻微改写复刻方案`
   只做轻微本地化和防判重改写，不重新创作、不输出多个方向
3. `最终固定分镜`
   输出唯一执行分镜和负面限制词，可直接进入后续短视频生成任务表

## 模型约束

本 skill 默认复用 `creator_crm` 视频评分链路的模型配置：

- API URL: `https://ark.cn-beijing.volces.com/api/coding/v3`
- 模型: `Doubao-Seed-2.0-pro`
- 超时: `120s`
- 重试: `2`
- 单步 `max_tokens`: `2500`

也就是说，这个 skill 的视频分析和文本生成都走同一套模型与参数口径。

## 默认字段

必需字段：

- `状态`
- `视频`
- `高光DNA提取结果`
- `轻微改写复刻方案`
- `最终固定分镜`

可选字段：

- `内容分支`，为空时默认 `非商品展示型`
- `店铺ID`，同步到原创脚本表后用于自动发布按账号店铺匹配
- `目标国家`
- `目标语言`
- `商品类型`
- `负面限制词`
- `同步状态`
- `同步到脚本ID`
- `错误信息`

也兼容英文字段名：

- `status`
- `source_video`
- `target_country`
- `target_language`
- `product_type`
- `replicate_mode`
- `analysis_result`
- `replicate_card`
- `localized_script`
- `final_execution_prompt`
- `run_log`

别名也支持自动识别，例如：

- `任务状态` / `执行状态`
- `视频素材` / `原视频`
- `产品类型` / `品类`
- `失败原因`

## OpenClaw 用法

推荐直接带飞书链接运行：

```bash
python3 skills/video-remake-lite/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx"
```

先小批量试跑：

```bash
python3 skills/video-remake-lite/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx" --limit 5
```

先看待处理任务：

```bash
python3 skills/video-remake-lite/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx" --dry-run
```

## 执行说明

- 三个生成步骤是单条记录内串行执行
- 每一步 prompt 前都会自动注入“养号高保真轻量复刻”全局总控约束
- 任一记录失败，不会阻塞下一条记录
- 如果表里存在 `错误信息` 或 `失败原因` 字段，会自动写回报错摘要
- 视频字段既支持公网 URL，也支持飞书附件；附件会先换成临时下载链接再传给模型
