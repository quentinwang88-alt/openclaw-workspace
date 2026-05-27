# AutoMixcut Feishu Tables

V1.0 needs 4 Feishu Bitable tables. Feishu is only the human workbench; long-term media files stay in local OSS/OSS storage and Feishu receives preview URLs.

## 1. 商品内容任务表

Purpose: task summary and status dashboard.

Fields:

| Field | Suggested Type |
| --- | --- |
| 商品ID | Text |
| 商品名称 | Text |
| 市场 | Single select |
| 类目 | Single select |
| 店铺 | Text |
| 优先级 | Single select |
| 任务类型 | Single select |
| 目标生成数量 | Number |
| 系统允许生成数量 | Number |
| 实际生成数量 | Number |
| 素材等级 | Single select |
| 素材状态 | Single select |
| 混剪状态 | Single select |
| 锚点状态 | Single select |
| 素材缺口说明 | Long text |
| 失败原因 | Long text |
| 最近成片预览 | URL |
| 人工备注 | Long text |

## 2. 商品锚点卡确认队列

Purpose: confirm the product anchor before material processing.

Fields:

| Field | Suggested Type |
| --- | --- |
| 商品ID | Text |
| 商品名称 | Text |
| 市场 | Single select |
| 类目 | Single select |
| 商品主图 | Attachment or URL |
| AI生成锚点卡 | Long text |
| 核心视觉点 | Long text |
| 不可错识别点 | Long text |
| 禁用错配项 | Long text |
| 适用核心镜头 | Multi select |
| 人工确认状态 | Single select: 待确认 / 已确认 / 需修改 |
| 人工修正内容 | Long text |
| 确认人 | User or Text |
| 确认时间 | Date time |
| 备注 | Long text |

## 3. 人工复核队列表

Purpose: review only exception segments, not all segments.

Fields:

| Field | Suggested Type |
| --- | --- |
| 片段ID | Text |
| 商品ID | Text |
| 市场 | Single select |
| 类目 | Single select |
| 片段预览链接 | URL |
| 封面图 | Attachment or URL |
| AI镜头用途 | Single select |
| AI商品可见度 | Single select |
| AI首镜强度 | Single select |
| AI可混剪判断 | Single select |
| AI风险等级 | Single select |
| AI置信度 | Single select |
| AI判断理由 | Long text |
| 商品匹配状态 | Single select |
| 有效镜位 | Multi select |
| 人工修正镜头用途 | Single select |
| 人工修正商品可见度 | Single select |
| 人工修正首镜强度 | Single select |
| 人工修正可混剪 | Single select |
| 人工修正风险等级 | Single select |
| 人工商品匹配判断 | Single select |
| 复核状态 | Single select: 待复核 / 已通过 / 已修正 / 废弃 |
| 备注 | Long text |

## 4. 成片质检表

Purpose: short-term preview of machine-QC-passed outputs.

Fields:

| Field | Suggested Type |
| --- | --- |
| 输出ID | Text |
| 商品ID | Text |
| 批次ID | Text |
| 变体编号 | Number |
| 模板ID | Text |
| 视频预览链接 | URL |
| 封面图 | Attachment or URL |
| 机器质检状态 | Single select |
| 人工质检状态 | Single select |
| 是否可发布 | Checkbox |
| 失败原因 | Long text |
| 飞书展示到期时间 | Date time |
| 备注 | Long text |

## Recommended Creation Order

1. 商品内容任务表
2. 商品锚点卡确认队列
3. 人工复核队列表
4. 成片质检表

After creation, record each table name, app token, and table ID in the OpenClaw Feishu config used by the AutoMixcut adapter.
