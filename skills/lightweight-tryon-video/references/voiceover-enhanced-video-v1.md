# 口播增强型轻视频开发说明 V1

## 边界

- `video_jobs` 继续只承载 8–10 秒基础轻视频，保持视频优先、口播后配。
- `narrative_variants` 承载 18–24 秒增强型编排，不直接替代视频生成队列。
- 钩子必须来自现有钩子库；正式文案必须来自当前口播流程。
- 编排层可以拆 Beat，但不得改写正式口播。
- 轻视频不复制智能混剪打标逻辑，通过统一素材桥接复用。

## 数据对象

### content_strategies

保存钩子、主卖点、辅助卖点、画面重点、证据要求和抽样权重。策略可被多个执行变体重复引用。

### narrative_variants

保存一次具体执行的格式、时长、随机种子、口播请求/响应、Beat、TTS 时间轴和组装计划。`strategy_group_id` 可以重复，`variant_id` 必须唯一。

### media_assets

按 `product_id + file_sha256 + tag_taxonomy_version` 幂等。分别保存生成前 `expected_tags` 和生成后 `observed_tags`，以及 auto_mixcut 的资产、切片关联。

### supplement_shots

保留历史补充镜头和运营手工补拍的数据契约。默认口播重剪不再自动新增记录；首镜或核心卖点证据缺失时，只阻塞当前变体并交由人工决定是否使用兼容入口补拍。

## 抽样规则

抽样分两段：

1. 覆盖段优先让尚未充分测试的重要钩子和卖点出现；
2. 剩余数量按业务优先级、商品匹配、证据覆盖和后续效果权重带放回抽样。

同一钩子、卖点或策略不设置硬次数上限。只阻止意外的完全重复任务和重复文件处理。

## 口播协议

请求固定声明：

```json
{
  "policy": {
    "generator": "existing_voiceover_flow_only",
    "preserve_current_tone": true,
    "allow_downstream_rewrite": false,
    "hook_source": "existing_hook_library"
  }
}
```

轻视频桥接层直接复用 `/Users/likeu3/voiceover_copy_engine` 的 ACTIVE 钩子、证据优先文案和独立质检；无需维护另一份 hooks JSON。当前口播引擎已支持 10 秒及 18–24 秒档位。口播结果至少返回 `voiceover_text`，推荐同时返回 `beats`；没有 Beat 时适配器只按句界拆分，不改写原文。

## 素材入库顺序

```text
素材回传
→ 本地文件哈希登记
→ auto_mixcut 上传
→ 媒体探测
→ 水印检查
→ 镜头切分
→ 抽帧
→ AI 打标
→ observed_tags + qc_result
→ 合格素材供增强型混剪读取
```

默认 `source_reference` 模式直接使用原始脚本表的同一 SKU 商品参考图，对照视频抽帧完成视觉打标，不要求商品锚点卡。轻视频流程只复用上传、切片、抽帧和打标能力，不自动认领、伪造或确认锚点。兼容模式 `confirmed_anchor` 仍保留，选择该模式时锚点缺失会保持 `blocked`。

## 关键匹配规则

当前标准角色：

- `main_wear_upper`
- `fit_turn`
- `color_upper`
- `detail_sleeve`
- `detail_neckline`
- `detail_closure`
- `detail_waistline`
- `detail_fabric`
- `wear_hold_color`
- `scenario_pose`

首镜固定要求清楚展示产品；全条口播只选一个核心卖点 Beat，按主卖点和已有 `required_shot_roles` 推断其证据角色。普通 Beat 与结尾清空硬要求，使用同产品素材软匹配，原始角色继续保留在计划中供复核。兼容补拍入口的 Prompt 仍由模板编译，不自由生成。

## 状态

```text
waiting_assets
→ visual_roughcut_planned
→ waiting_voiceover
→ generating_voiceover
→ waiting_tts
→ matching_assets
→ voiceover_cut_planned / voiceover_key_evidence_missing
→ ready_to_mix
→ rendering
→ final_qc
→ review_ready
```

错误状态独立保留，重跑只失效下游结果。

## 飞书运营字段

原始脚本表新增：

- `视频组合策略`：基础轻视频 / 自动组合 / 口播增强
- `口播增强视频数量`：不生成 / 5 / 10 / 20 / 50
- `口播测试方向`：自动 / 版型 / 细节 / 多色 / 场景

这些字段由 `feishu ensure-source-schema` 幂等建立。飞书巡检检测到增强数量后，会直接读取当前口播系统的 ACTIVE 钩子并幂等创建变体；即使基础轻视频数量为“不生成”，也可单独创建增强型计划。

## 当前可执行链路

```text
原始脚本表增强数量
→ ACTIVE 钩子 + 商品卖点带放回抽样
→ 既有轻视频按哈希补登记
→ 原始商品参考图 + auto_mixcut 真实切片与视觉打标
→ 18–24 秒证据粗剪
→ 当前口播引擎按实际时间轴写作与质检
→ Edge / ElevenLabs 单次连续 TTS，读取词级时间边界
→ 首镜 + 一个核心卖点硬匹配，其余口播软匹配并按真实台词时间重排画面
→ 关键证据缺失时只停止当前变体，不自动补镜头
→ 连续口播音轨与重排画面合成
→ final_qc
```

粗剪只消费 `tag_status=completed` 且 `asset_status=ready` 的实际切片；生成前计划标签不得充当证据。3秒切片会按目标时长自适应组合，并在帧尾不足时补齐到精确目标时长。18 秒以上口播全文只请求一次 TTS，禁止逐句拼音频；增强型泰语默认采用自然偏慢的 `-18%` 语速，让 22 秒视频保持更高口播占用率。系统优先使用服务返回的词级边界对齐 Beat，服务缺失边界时才按句子权重回退估算。只有首镜和核心卖点匹配失败会形成 `evidence_gap` 并阻断当前变体；普通 Beat 素材不足时允许非相邻复用并记录 warning。TTS 超时必须回到当前口播流程精简，不由增强层改写或强制加速。最终混音默认 `voiceover_only_no_random_model_bgm`，BGM 需后续建立口播专用压低/闪避策略后再启用。

## 验收

- 原有8–10秒任务、提示词、飞书同步和后处理回归测试保持通过。
- 20–50条计划允许同一策略重复，但每个执行变体唯一。
- 正式口播全文保持原样，编排层不改写。
- 18 秒以上口播只有一个连续 TTS 请求，成片中段不得出现分块拼接造成的停顿。
- 首镜和一个核心卖点必须命中相应镜头角色；普通 Beat 不因缺少专属镜头阻塞，也不自动触发补镜头。
- 同一素材文件和标签体系版本只打标一次。
- 实际标签覆盖计划标签时，以实际标签为准。
