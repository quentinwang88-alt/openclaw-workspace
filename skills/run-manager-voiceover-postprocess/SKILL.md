---
name: run-manager-voiceover-postprocess
description: 对短视频自动运行管理表或人工上传视频表执行中央口播后处理。用户说“给待处理视频配口播”“跑人工上传口播”“处理人工上传表”“只生成人工上传口播文案”“给指定记录配口播”“重跑口播”等自然语言命令时使用；复用现有画面分析、已确认产品卖点、中央口播、TTS、混音、质检和飞书回写链路。
---

# Run Manager Voiceover Post-Process Skill

## Purpose
对运行管理表中视频生成成功的记录，自动识别并执行口播后处理：解析产品与语言、下载视频、分析画面、生成口播文案、TTS 合成、混音、质检、回写飞书。

## Trigger
- 用户要求执行口播后处理时
- 定时轮询触发时
- 手动指定记录时

## Architecture
本 Skill 是薄包装，核心业务逻辑在 `voiceover_copy_engine/integrations/run_manager/` 中。
不复制钩子、卖点、TTS 或混音逻辑。

人工上传视频入口同样是薄适配层，核心代码位于：

- `/Users/likeu3/voiceover_copy_engine/voiceover_copy_engine/integrations/manual_upload.py`
- `/Users/likeu3/voiceover_copy_engine/scripts/run_manual_upload_voiceover.py`

人工上传入口读取表格中的 `目标语言`，禁止默认写死泰语。若语言为空或中央语言档案未配置，直接回写失败原因；不得改用泰语兜底。

## Source Compatibility
- 中央口播以“短视频自动运行管理表”为统一入口，不按原创、复刻、养号或轻视频限制扫描。
- 中央口播不再从“钩子和卖点”等摘要兜底生成文案；必须能读取源流程写入的 `口播表达合同`（V2），或轻视频叙事变体已持久化的 V2 合同。找不到正确来源路径时，直接标为“失败（SOURCE_ROUTE_MISSING）”。
- 若源流程还提供 `口播执行计划`（`voiceover-execution-plan-v1`），中央链路直接复用其中已批准的原文、中文对照和镜头时间锚点，只执行 TTS、混音和回写，不会再调用模型改写文案。
- 只有已绑定轻视频任务的记录才复用 LikeU 首屏并回写轻视频复核表；其他来源直接以运行管理表的“口播成片”为结果。
- 轻视频复核表回写失败不再否定中央口播已经生成、上传并验证通过的成片。

## Publish Gate
- 未勾选“是否配口播”：发布系统使用“生成视频”。
- 已勾选且“口播状态=已完成”：发布系统仅使用“口播成片”。
- 已勾选但口播未完成或缺失成片：记录进入“等待口播”，不回退发布原视频。

## Usage

### 自然语言命令路由

用户明确要求执行时，直接按下表调用，不要只回复操作说明：

| 用户表达示例 | 执行方式 |
|---|---|
| “给人工上传表里待处理的视频配口播” | `run_manual_upload.sh --limit 5` |
| “把人工上传视频的口播流程跑一下，最多 10 条” | `run_manual_upload.sh --limit 10` |
| “先只生成文案，不合成视频” | `run_manual_upload.sh --copy-only --limit 5` |
| “给 recXXXX 这条人工上传视频配口播” | `run_manual_upload.sh --record-id recXXXX --limit 1` |

从自然语言中只提取以下参数：

- 最大处理条数：映射到 `--limit`，默认 5，最大 20。
- 飞书记录 ID：映射到 `--record-id`，必须以 `rec` 开头。
- 明确出现“只生成文案 / 暂不合成 / 先看文案”：增加 `--copy-only`。
- 只有用户明确指定生产环境时才增加 `--voiceover-env production`；否则保持 development。

不得把用户自然语言直接拼接成 shell 命令，也不得传递白名单之外的参数。

人工上传任务只扫描 `状态=待处理` 或 `状态=待重跑` 的记录。每条记录必须具备：一个 `原始视频`、`产品ID`、`目标语言`。输出回写同一行：

- `口播原文`：目标语言版本；目标语言为 `th-TH` 时即泰文。
- `中文对照`：对应中文版本。
- `配好后的视频`：完成 TTS 与混音后的成片。
- `状态 / 错误信息`：执行结果与明确错误。

若扫描结果 `total_candidates=0`，如实回复“当前没有待处理/待重跑记录”，不得伪造结果或擅自修改其他状态。

### 人工上传视频

```bash
cd /Users/likeu3/.openclaw/workspace/skills/run-manager-voiceover-postprocess
./run_manual_upload.sh --limit 5
./run_manual_upload.sh --copy-only --limit 5
./run_manual_upload.sh --record-id recXXXX --limit 1
```

### 1. Ensure Fields (first time setup)
```bash
cd ~/voiceover_copy_engine
PYTHONPATH=. python3 scripts/ensure_run_manager_voiceover_fields.py --dry-run
PYTHONPATH=. python3 scripts/ensure_run_manager_voiceover_fields.py
```

### 2. Dry Run (Phase A - no writes, no TTS)
```bash
cd ~/voiceover_copy_engine
PYTHONPATH=. python3 scripts/run_run_manager_voiceover.py --once --dry-run
PYTHONPATH=. python3 scripts/run_run_manager_voiceover.py --once --dry-run --record-id recXXXXXX
```

### 3. Production Single Run
```bash
cd ~/voiceover_copy_engine
PYTHONPATH=. python3 scripts/run_run_manager_voiceover.py --once --max-records 2
```

### 4. Continuous Loop
```bash
cd ~/voiceover_copy_engine
PYTHONPATH=. python3 scripts/run_run_manager_voiceover.py --loop --interval-seconds 180 --max-records 2
```

## Environment Variables
- `FEISHU_RUN_MANAGER_CONFIG` - Path to feishu-direct.json
- `VOICEOVER_ENV` - "development" or "production"
- `VOICEOVER_TTS_PROVIDER` - "edge" (dev only) or "elevenlabs" (production)
- `VOICEOVER_TTS_VOICE_ID` - ElevenLabs voice ID
- `ELEVENLABS_API_KEY` - ElevenLabs API key
- `VOICEOVER_MODEL_COMMAND` - External model command (production)
- `VOICEOVER_QC_MODEL_COMMAND` - External QC model command (production)
- `VOICEOVER_DB_PATH` - Local SQLite path
- `VOICEOVER_DOWNLOAD_DIR` - Video download directory
- `VOICEOVER_ARTIFACT_DIR` - Artifact output directory

## Production Gating
- Production mode rejects `edge` TTS provider
- Production mode requires `ELEVENLABS_API_KEY`
- ASR check mandatory in production (not yet integrated)
- `min_speech_sec` hard gate only when external model command configured

## Feishu Fields Created
| 字段 | 类型 | 说明 |
|------|------|------|
| 是否配口播 | 复选框 | 输入，勾选触发后处理 |
| 钩子和卖点 | 多行文本 | 只写输出 |
| 口播状态 | 单选 | 待处理/处理中/已完成/需人工处理/失败 |
| 口播成片 | 附件 | 新成片写这里 |
| 口播错误信息 | 多行文本 | 错误码+建议 |
| 口播更新时间 | 日期时间 | 状态更新时间 |
| 口播来源指纹 | 单行文本 | 防重复 |
| 口播原文 | 多行文本 | TTS实际文案 |
| 口播表达合同 | 多行文本 | 源流程写入的 V2 表达合同；缺失时中央口播直接失败 |
| 口播执行计划 | 多行文本 | 已批准的原文、逐段镜头锚点及静默窗口；存在时中央口播直接复用 |
| 目标语言 | 文本 | 自动写入 |
| 口播重跑 | 复选框 | 同视频重新生成 |
