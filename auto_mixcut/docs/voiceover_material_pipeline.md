# 统一素材库口播混剪实施说明

## 已实现的数据链路

- `outputs` 保留成片级口播快照：内容模式、口播变体、钩子、主卖点、口播正文、口播 OSS 对象和匹配版本。
- `output_material_usage` 按 `output_id + asset_id` 聚合成片素材使用情况，记录镜头次数、使用时长、角色、核心镜头数和首镜头。
- `scripts/reconcile_output_material_usage.py` 可按成片、批次或产品从 `output_segments` 幂等重建老数据，不要求一次性迁移全部历史素材。
- 轻视频的逐句时间切分、关键 Beat 识别和候选镜头评分已经抽到 `voiceover_visual_match_core.py`；轻视频继续调用该核心，混剪通过 RDS 适配器复用同一实现。
- 只有首镜和一个主卖点 Beat 使用硬证据匹配；普通 Beat 与结尾只做同产品软匹配，并保留原始镜头角色作为审计信息。
- `VoiceoverMixcutOrchestratorSkill` 只接收现有口播流程产出的 Beat、连续 TTS 时间轴和音频 OSS 对象，不生成或改写口播。
- `RenderSkill` 根据 `content_mode` 走两条音轨路径：`voiceover` 使用连续口播并关闭 BGM，`bgm` 保持原流程。

## 口播计划交接

现有口播/TTS 流程完成后，可以调用：

```bash
auto_mixcut prepare-voiceover-plan \
  --task-id TASK_ID \
  --batch-id BATCH_ID \
  --variant-no 1 \
  --voiceover-variant-id VOICEOVER_VARIANT_ID \
  --voiceover-object-id OSS_OBJECT_ID \
  --tts-timeline-json /absolute/path/tts_timeline.json \
  --beat-plan-json /absolute/path/beat_plan.json
```

只有首镜或核心卖点存在证据缺口时才会阻塞当前口播变体，并回写 `voiceover_status=blocked` 与 `narrative_failure_reason`。普通 Beat 缺少专属镜头不会阻塞，也不会自动创建补素材任务；素材不足时可非相邻复用，并在计划中记录 warning。

## 老数据和统计重建

```bash
python scripts/reconcile_output_material_usage.py --output-id OUTPUT_ID
python scripts/reconcile_output_material_usage.py --batch-id BATCH_ID
python scripts/reconcile_output_material_usage.py --product-id PRODUCT_ID
```

命令只重建成片素材使用快照，不下载视频、不重新渲染、不发布。

## 灰度开关

- 口播能力没有自动接管现有 BGM 任务；只有显式创建 `content_mode=voiceover` 的计划才启用。
- 跨国家/店铺共用同一产品素材，由 `MATERIAL_CANONICAL_PRODUCT_SELECTION_ENABLED=1` 启用；关闭时仍按执行产品 ID 取材，便于灰度回退。
- 帧抽样分三档：`fingerprint` 1 帧、默认 `representative` 3 帧、`evidence` 真实素材 6 帧/AI 素材 9 帧。
- 成片质检并发由 `AUTO_MIXCUT_QUALITY_CONCURRENCY` 控制，上限 3。
- 口播成片的素材唯一数不足只记 warning；分辨率、时长、音频、首镜和关键证据仍是硬质检。BGM 成片继续使用原有唯一素材硬门槛。
