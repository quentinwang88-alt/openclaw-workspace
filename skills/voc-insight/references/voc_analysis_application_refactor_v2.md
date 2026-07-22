# VOC 分析与应用改造方案 V2

## 1. 改造目标

将现有 VOC 流程从“为 ADS/选品生成推荐”改造成可独立运行的“消费者需求与产品机会情报层”。

V2 负责回答：

1. 消费者在意哪些结果、属性和使用体验。
2. 现有产品在哪些方面存在未满足需求。
3. 哪些需求可转化为产品规格或产品概念。
4. 每个机会的证据范围、稳定性、矛盾点和局限是什么。
5. 相比上一批次，哪些机会新增、增强、减弱或消失。

V2 不负责：

- 给候选商品增加或扣减选品分。
- 输出“推荐测品、建议淘汰、待找货”。
- 创建 1688 找货任务。
- 自动写入 Hermes/其它选品系统的评分、动作和任务表。
- 把证据来源商品写成推荐商品。

一句话边界：

> VOC V2 输出“市场可能需要什么样的产品”，但不判断“当前哪个商品一定值得做”。

## 2. 现有模块处置

### 2.1 保留

| 现有能力 | 处置 |
|---|---|
| `fastmoss_voc_product_snapshot` | 保留为商品与批次上下文 |
| `fastmoss_voc_raw` | 保留为不可改写的原始证据 |
| 文本清洗、有效性判断 | 保留并版本化 |
| 商品形态识别 | 保留，输出识别依据和置信度 |
| 信号标签和风险标签 | 保留，升级为原子证据的一个字段 |
| `evidence_id` | 保留作为原始评论级追溯键 |
| 金标测试 | 保留，增加原子切分和机会可追溯测试 |
| 现有 ADS 相关脚本 | 保持不变，不作为 V2 默认入口 |

### 2.2 新增并行链路

新增独立入口：

```text
scripts/run_voc_opportunity.py
```

默认模式：

```text
mode = opportunity_reference
```

它只读取原始/富化证据并写入 V2 独立产物，不调用：

```text
build_product_recommendation
persist_product_recommendation
build_ads_hook_package
need_accio_lookup
selection score/action
```

### 2.3 暂时保留但不作为默认输出

```text
scripts/build_wigs_selection_report.py
fastmoss_voc_product_recommendation
recommended_usecases / not_for_usecases
```

这些属于旧链路兼容能力。V2 验证期间不删除，避免影响现有任务。

## 3. 目标流水线

```text
原始评论
  -> 数据质量与去重
  -> 评论原子切分
  -> 属性级观点/痛点证据
  -> 信号聚合
  -> 产品机会合成
  -> 规格与风险建议
  -> 批次变化
  -> 独立 VOC 机会报告/JSON/CSV
```

每一层必须可单独追溯，后一层不得覆盖前一层。

## 4. 核心数据模型

## 4.1 原子证据 AtomicEvidence

一条评论可以拆成多条原子证据。例如：

```text
“颜色比图片浅，但看起来很好看，抽绳固定得更牢。”
```

拆为：

1. `color_accuracy / negative / 颜色比图片浅`
2. `appearance / positive / 看起来好看`
3. `hold_stability / positive / 抽绳固定更牢`

字段契约：

```json
{
  "atomic_evidence_id": "{evidence_id}:seg:01",
  "evidence_id": "原始评论证据ID",
  "segment_index": 1,
  "market": "MX",
  "category_key": "wigs",
  "product_id": "仅作证据来源",
  "product_form": "clip_in_extension",
  "sample_pool": "natural_distribution|diagnostic_risk",
  "evidence_scope": "direct|category_proxy",
  "aspect_group": "appearance|color|material|install|hold|comfort|durability|value|fulfillment",
  "signal_tag": "color_match_issue",
  "polarity": "positive|negative|neutral|mixed",
  "severity": "low|medium|high|unknown",
  "opinion_target": "色号准确度",
  "opinion_text": "颜色比图片浅",
  "desired_outcome": "实物颜色与展示一致",
  "usage_scenario": "日常佩戴",
  "controllability": "product_spec|content_instruction|fulfillment|uncontrollable|unknown",
  "source_text": "原始西语片段",
  "extraction_method": "rule|llm|hybrid",
  "extractor_version": "...",
  "quality_flags": []
}
```

强制规则：

- 原文中没有的需求不得写成事实。
- `desired_outcome` 可以是归纳，但必须能回溯到 `source_text`。
- `product_id` 只表示证据载体，不能解释为推荐商品。
- 同一片段不能同时标为相互冲突的正向和负向，除非 `polarity=mixed` 并保留解释。
- `category_proxy` 证据不得进入商品自身口碑统计。

## 4.2 信号聚合 SignalSummary

按以下范围聚合：

```text
market + category_key + product_form + signal_tag + time_window
```

必须同时输出：

```text
evidence_count
product_count
batch_count
positive_count
negative_count
neutral_count
natural_sample_count
diagnostic_sample_count
direct_evidence_count
proxy_evidence_count
max_product_contribution
max_form_contribution
recent_evidence_count
evidence_refs
contradicting_evidence_refs
```

禁止只输出评论次数。商品覆盖、批次覆盖、单商品集中度和代理证据占比必须可见。

诊断型过采样的负面评论可以用于发现风险，但不得直接参与自然差评率计算。

## 4.3 产品机会卡 OpportunityCard

机会卡是 V2 的核心应用对象：

```json
{
  "opportunity_id": "稳定语义ID",
  "run_id": "...",
  "market": "MX",
  "category_key": "wigs",
  "product_forms": ["clip_in_extension"],
  "opportunity_type": "established_preference|pain_gap|feature_upgrade|usage_scenario|risk_only",
  "title_zh": "低光泽、自然衔接的卡扣接发",
  "user_job": "增加长度和发量，同时不显得像假发",
  "unmet_need": "自然感与低光泽仍存在不稳定表现",
  "opportunity_hypothesis": "低光泽纤维和更细化色号可能提升自然衔接体验",
  "must_have_specs": [],
  "optional_specs": [],
  "avoid_specs": [],
  "usage_scenarios": [],
  "supporting_signal_ids": [],
  "contradicting_signal_ids": [],
  "metrics": {},
  "confidence": "signal_only|emerging_opportunity|direction_candidate|stable_reference",
  "evidence_refs": [],
  "limitations": [],
  "status": "new|strengthened|stable|weakened|disappeared|first_observation",
  "generated_at": "ISO8601"
}
```

机会卡不得包含：

```text
recommended_product_id
product_potential_score
final_action
need_lookup
recommended_test
procurement/margin conclusion
```

## 4.4 规格与风险项 SpecRiskItem

用于形成长期复用的产品知识库：

```text
item_id
market/category/form
attribute
item_type = must_have | optional_upgrade | avoid | inspection_check
requirement_zh
consumer_reason
supporting_evidence_count/product_count/batch_count
evidence_refs
confidence
first_seen/last_seen
```

## 5. 机会类型定义

| 类型 | 解释 | 示例 |
|---|---|---|
| `established_preference` | 已有产品被稳定认可的必备属性 | 自然感、柔软、发量足 |
| `pain_gap` | 多商品重复出现且尚未解决的痛点 | 塑料光泽、色差、打结 |
| `feature_upgrade` | 正向需求与负向缺口结合形成的升级方向 | 更稳卡扣、低光泽纤维 |
| `usage_scenario` | 特定场景或人群形成的产品概念 | 婚礼快速造型、日常轻量佩戴 |
| `risk_only` | 当前只足以形成避坑或验货规则 | 鼓包、刘海需修剪 |

`risk_only` 不能被改写成正向机会标题；只有与明确期望结果和可控规格结合后，才能升级为 `feature_upgrade`。

## 6. 置信度与门槛

置信度表示“证据强度”，不表示商业成功概率。

| 置信度 | 初始建议门槛 | 允许输出 |
|---|---|---|
| `signal_only` | 少于3商品或少于10条原子证据 | 线索、风险观察 |
| `emerging_opportunity` | ≥3商品、≥10条证据 | 新兴机会卡 |
| `direction_candidate` | ≥5商品、≥20条证据、单商品≤30% | 产品方向参考 |
| `stable_reference` | ≥8商品、≥40条证据、≥2批次 | 稳定规格/方向参考 |

补充约束：

- `category_proxy` 证据单独计数，不能代替 direct evidence 门槛。
- `classic_only` 不阻止输出，但必须加入 `limitations`。
- 单商品贡献超过30%时最多为 `emerging_opportunity`。
- 只有诊断样本、没有自然分布样本时最多为 `emerging_opportunity`。
- 矛盾证据不可删除，必须进入 `contradicting_signal_ids`。
- 首次运行没有历史基线时，状态为 `first_observation`，不能写“增长”。

## 7. 机会合成逻辑

## 7.1 确定性部分

确定性程序负责：

1. 统计商品数、证据数、批次数和贡献集中度。
2. 区分 direct/proxy 和 natural/diagnostic。
3. 计算置信度。
4. 识别同属性的正向与负向信号组合。
5. 绑定全部 evidence refs。
6. 执行禁止字段和越权结论校验。

## 7.2 模型可参与部分

模型只允许：

- 将多个同属性信号归纳为用户任务、未满足需求和机会假设。
- 把消费者语言转成候选产品规格表达。
- 合并语义重复的机会卡。
- 生成简洁中文标题和摘要。

模型不得：

- 修改确定性统计。
- 提高置信度。
- 发明消费者没有提到的功能。
- 输出具体采购、销量、利润或成功概率结论。
- 删除与机会相反的证据。

模型输出后必须经过 claim validator：

```text
每个 user_job/unmet_need 都有 supporting_signal
每个 must_have/avoid spec 都有 evidence 或标记 hypothesis_only
所有数字等于确定性聚合值
所有 evidence_ref 都存在
禁止字段为空
```

## 8. 建议新增的独立数据表

为了与现有 ADS/选品表解耦，建议新增：

### voc_opportunity_run

一次 V2 分析运行的范围、参数、状态和质量摘要。

### voc_atomic_evidence

每条原始评论拆分后的原子证据。唯一键：

```text
atomic_evidence_id = {evidence_id}:seg:{index}
```

### voc_signal_summary

每个 run 下按形态/信号聚合的确定性统计。

### voc_opportunity_card

机会卡主表，JSON保存规格、矛盾信号、限制和指标。

### voc_opportunity_evidence_map

机会卡与原子证据的多对多映射，标明：

```text
supporting | contradicting | example
```

### voc_spec_risk_library

跨批次积累的产品规格、风险和验货规则。

### voc_opportunity_delta

同市场、类目、形态和稳定机会ID的批次变化。

所有表使用 `voc_` 独立命名，不写入选品评分表。

## 9. 应用输出

每次 V2 运行固定生成：

### 9.1 VOC机会总览

```text
{batch_id}_voc_opportunity_overview.md
```

内容：数据范围、主要需求、主要痛点、机会分布、限制和变化摘要。

### 9.2 机会卡JSON

```text
{batch_id}_voc_opportunity_cards.json
```

供其它系统只读引用。

### 9.3 机会卡CSV

```text
{batch_id}_voc_opportunity_cards.csv
```

供业务查看、筛选和飞书同步。

### 9.4 规格与风险库

```text
{batch_id}_voc_spec_risk_library.csv
```

### 9.5 原子证据明细

```text
{batch_id}_voc_atomic_evidence.csv
```

每张机会卡至少展示：

- 机会类型和置信度。
- 用户任务和未满足需求。
- 建议规格与规避项。
- 商品数、证据数、批次数和最大单商品贡献。
- 2–5条原始西语证据。
- 矛盾证据和数据限制。

报告不得出现默认商品排名。“商品ID”只能放在证据明细中，并标为“证据来源商品”。

## 10. 代码改造工作包

## WP0：冻结契约与并行入口（P0）

新增：

```text
references/voc_opportunity_output_contract_v2.json
references/voc_opportunity_thresholds_v1.json
scripts/run_voc_opportunity.py
```

任务：

1. 定义 AtomicEvidence、SignalSummary、OpportunityCard schema。
2. 新入口只读现有表，默认 dry-run。
3. 不修改 `run_voc_insight.py` 现有默认行为。
4. 增加 `--batch-id --market --category-key --output-dir --write`。

验收：

- 同一批次可同时运行旧流程和 V2，输出互不覆盖。
- V2 dry-run 不写任何 ADS、选品或商品推荐表。
- 缺少 batch/market/category 时拒绝运行。

## WP1：评论原子化（P0）

新增：

```text
core/atomic_evidence.py
references/voc_aspect_taxonomy_v1.json
tests/test_voc_atomic_evidence.py
```

改造：

- 复用 `enrich_wigs_voc.py` 的清洗、有效性、形态和信号规则。
- 将一评论一行扩展为一评论多原子片段。
- 增加 aspect、polarity、severity、desired_outcome、controllability。
- 原评论记录继续保留，不改变现有 `fastmoss_voc_enriched`。

验收：

- 一条包含正负混合观点的评论能拆出多个片段。
- “不亮、不会打结”等否定表达不会生成反向风险。
- 每个原子片段可回溯到原始评论和原文区间。
- 空评论、纯SKU和疑似错商品不生成正式原子证据。

## WP2：确定性聚合与置信度（P0）

新增：

```text
core/opportunity_aggregation.py
tests/test_voc_opportunity_thresholds.py
```

任务：

1. 按市场/类目/形态/信号聚合。
2. 计算 direct/proxy、natural/diagnostic、商品贡献和批次覆盖。
3. 实现四档置信度。
4. 输出支持与矛盾证据。

验收：

- 单一商品大量评论不能把机会提升到 `direction_candidate`。
- diagnostic 样本不能被计算成自然差评率。
- proxy evidence 不计入商品自身口碑。
- 输入顺序变化不影响聚合结果。

## WP3：机会卡与规格风险库（P0）

新增：

```text
core/opportunity_synthesis.py
references/category_opportunity_adapters.json
tests/test_voc_opportunity_claims.py
```

任务：

1. 先用确定性映射形成机会候选。
2. 允许模型做有限表达归纳。
3. 执行 claim validator。
4. 将机会拆成 must-have、optional、avoid、inspection check。

验收：

- 每个规格建议都有证据或 `hypothesis_only` 标记。
- 风险不会被单独包装成卖点。
- 不输出具体商品推荐、选品分或找货动作。
- 所有机会卡具有 evidence refs、limitations 和矛盾证据字段。

## WP4：独立报告与本地文件（P0）

新增：

```text
scripts/build_voc_opportunity_report.py
tests/test_voc_opportunity_report.py
```

任务：

1. 生成总览、机会卡、规格风险库和原子证据明细。
2. 将“优先验证商品”改成“证据来源商品”。
3. 默认按机会置信度和证据广度排序，不按具体商品排序。

验收：

- 报告脱离选品系统也能独立阅读。
- 每个结论可点击/查询到原始证据。
- 报告中不出现 `推荐测品/建议淘汰/找货`。
- classic-only、样本不足和代理证据限制在首页可见。

## WP5：独立落库与历史变化（P1）

新增：

```text
scripts/ensure_voc_opportunity_schema.py
core/opportunity_delta.py
tests/test_voc_opportunity_delta.py
```

任务：

1. 新建 V2 独立表。
2. 以稳定语义机会ID比较前后批次。
3. 输出 `new/strengthened/stable/weakened/disappeared`。
4. 没有同范围历史基线时输出 `first_observation`。

验收：

- 不同市场/类目/形态不得互相比较。
- 没有历史数据时不能伪造增长趋势。
- 重跑同一 run 使用 upsert，不生成重复机会卡。

## WP6：业务查看层（P1）

可新增独立飞书表“VOC产品机会池”，字段建议：

```text
机会ID
市场
类目
商品形态
机会类型
机会标题
用户任务
未满足需求
关键规格
规避项
商品数/证据数/批次数
置信度
变化状态
数据限制
原始证据
人工状态（新发现/持续观察/已验证/关闭）
```

该表不得包含选品分、找货状态和是否测品。其它系统只读订阅，不反向修改 V2 证据。

## 11. 测试与质量门

## 11.1 每批盲测

每个新市场/类目首批人工标注20–30条评论；后续每批盲抽10–20条，不把全部金标用于规则开发。

最低建议指标：

```text
评论有效性准确率 >= 95%
商品形态准确率 >= 90%
原子切分准确率 >= 85%
aspect准确率 >= 85%
polarity准确率 >= 90%
风险误作机会 = 0
机会卡无证据结论 = 0
证据引用不存在 = 0
禁止字段出现 = 0
```

## 11.2 业务质量指标

不以“选品成功率”直接评价 VOC，而跟踪：

```text
机会卡人工保留率
机会卡人工合并/推翻率
新增未知信号率
跨批次复现率
规格建议被验证率
风险预警命中率
单商品偏置率
代理证据占比
```

## 12. 墨西哥假发试运行

首个 V2 验证批次使用：

```text
FM_MX_WIGS_COMBINED_20260717
68条有效VOC / 17个商品 / classic-only
```

试运行至少应形成以下候选机会族，但最终置信度由程序按证据计算：

1. 低光泽、自然衔接。
2. 更准确的色号展示与匹配。
3. 更稳定的卡扣/抽绳固定结构。
4. 易佩戴、低鼓包的整顶假发。
5. 更耐梳理、不易打结掉发的纤维。

试运行必须明确：

- 这些商品ID只是证据来源，不是推荐采购对象。
- classic-only 是数据限制，不阻止机会发现。
- 首批没有历史趋势，全部状态为 `first_observation`。
- 旧选品式报告和 V2 机会报告同时保留，人工比较信息质量。

## 13. 推荐实施顺序

第一轮 MVP（必须完成）：

```text
WP0 契约与入口
-> WP1 原子证据
-> WP2 聚合与置信度
-> WP3 机会卡
-> WP4 本地报告
-> MX 假发回放验收
```

第二轮生产化：

```text
WP5 独立落库与趋势
-> WP6 独立业务查看层
-> 连续两个真实批次验证
```

生产切换条件：

1. MX假发回放质量门通过。
2. 至少两个真实批次没有无证据机会和风险反转。
3. V2报告可以不依赖旧商品推荐表独立生成。
4. V2写入范围与 ADS/选品表完全隔离。
