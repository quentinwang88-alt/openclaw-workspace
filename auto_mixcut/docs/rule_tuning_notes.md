# Rule Tuning Notes

## 2026-05-27 商品 1735243780969628725

测试对象：

- 类目：hair_accessories
- 市场：VN
- 素材：4 条 AI generated 商品视频，切出 20 个片段

## 人工反馈结果

### 严格排除版

批次：`BATCH_20260527081820_651EDE17`

- 5 条成片
- 4 条人工可发布
- 1 条需修改
- 策略：`needs_processing` 和 `medium risk` 片段不进入 render

### 本地语言字幕软降级版

批次：`BATCH_20260527085044_7C582CA8`

- 5 条成片
- 5 条人工可发布
- 策略：本地语言字幕/底部文字不一刀切排除，但降级使用

## 当前固化规则

如果 AI 判断原因主要是越南语字幕、当地语言字幕、底部文字或字幕需处理，且没有水印、平台 UI、账号信息、SKU 一致性不确定、商品漂移、错款、无关元素或严重遮挡，则不直接剔除，进入 `soft local-language subtitle issue`。

使用限制：

- 不进首镜。
- 每条视频最多使用 2 个软字幕片段。
- 核心镜头优先使用干净片段。
- 软字幕片段优先放在 scene / ending。
- 如果 clean pool 不足，可进入 detail / result，但不进入 hero。

## 模板表现

当前小样本统计：

- `GENERAL_BALANCED_15S`: 4/4 人工可发布
- `RESULT_FIRST_15S`: 3/4 人工可发布
- `DETAIL_HOOK_15S`: 2/2 人工可发布

样本量仍小，不调整模板池，只保留观察。
