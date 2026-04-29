# Market Insight Output Design

## 总原则

市场洞察结果采用双轨输出：

1. 多维表格做主存储
2. 飞书文档做周报展示

原因：

- 多维表格适合阶段 2 直接读取、筛选、匹配
- 飞书文档适合运营和选品同学快速阅读结论
- 后续如果接入第三阶段数据回流，结构化主表更容易扩展

## 推荐输出结构

### A. 市场方向卡表

用途：

- 作为阶段 1 的核心结构化结果库
- 作为阶段 2 的直接输入源

建议一行一个 `MarketDirectionCard`

推荐字段如下：

| 字段名 | 类型建议 | 说明 |
| --- | --- | --- |
| `方向规范Key` | 文本 | 对应 `direction_canonical_key`，跨批次稳定主键 |
| `方向实例ID` | 文本 | 对应 `direction_instance_id`，单批次实例 ID |
| `批次日期` | 日期 | 对应 `batch_date` |
| `国家` | 单选/文本 | `VN` / `TH` 等 |
| `类目` | 单选 | `hair_accessory` / `light_tops` |
| `方向名称` | 文本 | 对应 `direction_name` |
| `主风格` | 单选/文本 | 对应 `style_main` |
| `产品形态/结果` | 单选/文本 | 对应 `product_form_or_result` |
| `核心价值点` | 多选/文本 | 对应 `top_value_points` |
| `核心元素` | 多选/文本 | 对应 `core_elements` |
| `核心场景` | 多选/文本 | 对应 `scene_tags` |
| `目标价格带` | 多选/文本 | 对应 `target_price_bands` |
| `热度等级` | 单选 | `high / medium / low` |
| `拥挤度等级` | 单选 | `high / medium / low` |
| `代表商品ID` | 多行文本 | 从 `representative_products` 里拆出 |
| `代表商品名称` | 多行文本 | 从 `representative_products` 里拆出 |
| `选品建议` | 多行文本 | 对应 `selection_advice` |
| `避坑提示` | 多行文本 | 对应 `avoid_notes` |
| `是否最新批次` | 复选框/单选 | 方便阶段 2 只读取最新方向 |

最低必备字段：

- `方向ID`
- `方向规范Key`
- `方向实例ID`
- `批次日期`
- `国家`
- `类目`
- `方向名称`
- `主风格`
- `产品形态/结果`
- `核心价值点`
- `核心元素`
- `目标价格带`
- `选品建议`
- `避坑提示`

### B. 市场商品打标快照表

用途：

- 给人工抽查单商品打标结果
- 方便回溯某个方向卡是由哪些样本聚合而来

这张表不是阶段 2 必须依赖的主表，建议作为辅助审计表保留。

推荐字段：

| 字段名 | 类型建议 | 说明 |
| --- | --- | --- |
| `批次日期` | 日期 | 对应 `batch_date` |
| `国家` | 单选/文本 | 对应 `country` |
| `类目` | 单选 | 对应 `category` |
| `商品ID` | 文本 | 对应 `product_id` |
| `商品名称` | 文本 | 对应 `product_name` |
| `店铺名称` | 文本 | 对应 `shop_name` |
| `商品图片` | 附件/链接 | 主图或图集链接 |
| `价格带` | 单选/文本 | 对应 `target_price_band` |
| `主风格` | 单选/文本 | 对应 `style_tag_main` |
| `次风格` | 多选/文本 | 对应 `style_tags_secondary` |
| `元素标签` | 多选/文本 | 对应 `element_tags` |
| `产品形态/结果` | 单选/文本 | 对应 `product_form_or_result` |
| `价值点` | 多选/文本 | 对应 `value_points` |
| `场景标签` | 多选/文本 | 对应 `scene_tags` |
| `热度分` | 数字 | 对应 `heat_score` |
| `热度等级` | 单选 | 对应 `heat_level` |
| `拥挤度分` | 数字 | 对应 `crowd_score` |
| `拥挤度等级` | 单选 | 对应 `crowd_level` |
| `方向优先级` | 单选 | 对应 `priority_level` |
| `短原因` | 文本 | 对应 `reason_short` |
| `有效样本` | 复选框/单选 | 对应 `is_valid_sample` |

### C. 市场洞察周报文档

用途：

- 给人读的固定周报
- 不承担阶段 2 程序读取职责

推荐每个 `国家 + 类目 + 批次日期` 一篇文档，固定结构如下：

1. 本期主流风格 Top 3
2. 本期核心购买动机 Top 3
3. 高热低卷方向
4. 高热高卷方向
5. 可测试方向
6. 暂不建议重压方向
7. VOC 摘要
8. 对阶段 2 选品的直接动作建议
9. 代表样本列表

文档适合强调：

- 本周优先补什么
- 少补什么
- 各方向为什么值得补

不适合承担：

- 程序化筛选
- 历史方向匹配
- 自动读取和联动

## 与阶段 2 的接口

阶段 2 推荐只读取“市场方向卡表”或其等价结构化产物。

能力边界说明：
- 方向卡只用于判断“哪个方向值得进入”，不用于替代单品评分。
- 单品层的材质、版型、价格带适配性，需要由后续单品评分系统承接。

阶段 2 读方向卡时至少应使用这些字段：

- `方向规范Key`
- `批次日期`
- `国家`
- `类目`
- `方向名称`
- `主风格`
- `产品形态/结果`
- `核心价值点`
- `核心元素`
- `目标价格带`
- `选品建议`
- `避坑提示`

阶段 2 候选品结果新增写回字段：

| 字段名 | 说明 |
| --- | --- |
| `匹配市场方向ID` | 对应 `matched_market_direction_id` |
| `匹配市场方向名称` | 对应 `matched_market_direction_name` |
| `匹配市场方向理由` | 对应 `matched_market_direction_reason` |

## 当前实现与飞书输出的关系

当前代码已经先把市场洞察结果稳定写到本地 artifacts：

- `market_insight_product_snapshot.json`
- `market_insight_product_tags.json`
- `market_direction_cards.json`
- `market_insight_report.json`
- `market_insight_report.md`
- `latest/<country>__<category>.json`

建议飞书落地顺序：

1. 先把 `market_direction_cards.json` 映射到“市场方向卡表”
2. 再把 `market_insight_report.md` 同步成飞书文档
3. 如需审计，再把 `market_insight_product_tags.json` 同步成“市场商品打标快照表”

仓库内可直接复用的字段模板：

- `configs/market_insight_output_templates/market_direction_card_table_fields.json`
- `configs/market_insight_output_templates/candidate_selection_market_direction_fields.json`

## V1 实施建议

如果只做最小可用版本，优先级如下：

1. 建立“市场方向卡表”
2. 生成“市场洞察周报文档”
3. 阶段 2 读取方向卡并写回匹配字段

不建议 v1 一开始就做：

- 太多中间表
- 复杂 BI 看板
- 用文档替代结构化主表
- 把所有洞察明细直接堆在候选品表里
