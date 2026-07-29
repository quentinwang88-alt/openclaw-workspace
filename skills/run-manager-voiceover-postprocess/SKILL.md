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
