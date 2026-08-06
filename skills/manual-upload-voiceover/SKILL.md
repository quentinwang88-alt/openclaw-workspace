---
name: manual-upload-voiceover
description: 人工上传视频配口播的唯一自然语言入口。用户提到“人工上传视频配口播”“人工上传表待处理视频”“跑人工上传口播”“只生成人工上传口播文案”“给人工上传记录 rec... 配口播”时必须使用。只处理人工上传视频表，不是原创脚本、复刻、轻视频或短视频运营任务表流程；将目标语言口播、中文对照及配音成片回写原表。
---

# 人工上传视频配口播

严格调用既有中央口播入口，不搜索或改用其他脚本。

## 唯一入口

```bash
/Users/likeu3/.openclaw/workspace/skills/run-manager-voiceover-postprocess/run_manual_upload.sh [白名单参数]
```

禁止调用以下流程：

- `original-script-generator`
- `run_feishu_operation_tasks.py`
- 复刻、轻视频、混剪或短视频发布脚本

## 命令映射

- “跑人工上传口播”或“给待处理的人工上传视频配口播”：执行 `run_manual_upload.sh --limit 5`。
- “最多 N 条”：增加 `--limit N`；只接受 1 至 20。
- “只生成文案 / 先看文案 / 暂不合成”：增加 `--copy-only`。
- “处理 recXXXX”：增加 `--record-id recXXXX --limit 1`。
- 只有用户明确说生产环境时才增加 `--voiceover-env production`。

不得把自然语言直接拼接到 shell，只能使用上述参数。

## 表格合同

只扫描人工上传表中 `状态=待处理` 或 `状态=待重跑` 的记录。每条必须有：

- 一个 `原始视频`
- `产品ID`
- `目标语言`

语言由 `目标语言` 动态决定，禁止默认泰语或用泰语兜底。当前语言没有中央语言档案时，让流程按 `LOCALE_PROFILE_MISSING` 失败并回写原因。

输出写回同一行：

- `口播原文`：目标语言版本；`th-TH` 对应泰文。
- `中文对照`：中文版本。
- `配好后的视频`：完成 TTS 和混音后的成片。
- `状态`、`错误信息`：处理结论。

## 回复要求

执行后读取命令 JSON，回复候选数、成功数、失败记录及原因。`total_candidates=0` 时只说明当前没有待处理/待重跑记录，不修改其他表，不虚构口播结果。
