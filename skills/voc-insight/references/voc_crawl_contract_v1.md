# VOC 数据抓取规则与容错契约 V1

## 1. 目标与边界

本规范供独立 VOC 抓取程序执行，输出必须能被现有 `voc-insight` 分析流程直接读取。

抓取程序只负责：

1. 按指定市场、类目和批次选择商品。
2. 抓取商品快照、原始评论及评论元数据。
3. 标准化、去重、记录字段质量和抓取状态。
4. 在字段缺失或页面失败时安全降级，不编造字段。

抓取程序不得负责：

- 推断消费者洞察、卖点或选品结论。
- 把代理类目评论伪装成新品自身评论。
- 因为一个可选字段缺失而丢弃整条有效评论。
- 把验证码、解析失败或登录失效误记为“商品无评论”。

## 2. 批次与范围

每次运行必须固定唯一范围：

```text
batch_id + market + category_key
```

如果指定三级类目，还必须增加：

```text
category_scope_name
```

同一批次不得混入其它国家或类目。`batch_id` 建议格式：

```text
VOC__{MARKET}__{CATEGORY}__{YYYYMMDD_HHMMSS}
```

批次必须保存：抓取规则版本、源程序版本、开始/结束时间、目标配额、实际商品数、实际 VOC 数、状态分布和失败原因分布。

## 3. 采样规则

### 3.1 两类样本池必须分开

#### natural_distribution

用于判断真实需求普遍度，尽量保留来源页面的自然评论分布，不人为增加差评比例。

#### diagnostic_risk

用于发现材质、佩戴、耐用、色差、退货等风险，可主动抓取 1–3 星、问答或低分商品。

两类样本可以共同用于发现主题，但计算正负反馈占比时不得直接混合。每条记录必须保存 `sample_pool`；如果进行了过采样，应保存 `sampling_weight` 或明确标记 `biased_sample=true`。

### 3.2 选品分析建议配额

| 决策层级 | 最低商品数 | 最低有效 VOC | 说明 |
|---|---:|---:|---|
| 单商品线索 | 1 | 3 | 只代表该商品 |
| 商品形态小批验证 | 5 | 30 | 单商品贡献不高于 30% |
| 类目级选品 | 20 | 100 | 推荐目标 100–150 条 |
| 稳定类目方向 | 20 | 100 | 且至少连续两个批次复现 |

商品应尽量覆盖：

- `product_form`：卡扣接发、马尾接发、整顶假发、编发发束、局部发片等。
- `pool_type`：`classic`、`new`；不能识别时写 `unknown`。
- 销量层级：头部、中腰部、低销量/失败商品。
- 价格层级：低、中、高价格带。
- 上架阶段：新品、成长期、成熟品。

自然分布池建议每商品最多取 10 条评论，避免单一爆款支配类目结论。若评论不足则全部保留，不得复制补足。

### 3.3 new 池没有评论时

1. 商品快照照常保留。
2. 写入抓取状态 `no_visible_voc`，不能伪造空评论行。
3. 新品建议 7 天后重查；成熟商品建议 30 天后重查。
4. 如需使用同形态成熟竞品评论，必须另标：

```text
evidence_scope = category_proxy
proxy_for_product_id = 新品ID
source_product_id = 实际评论所属商品ID
```

代理证据只能用于方向/规格参考，不能用于该新品的商品级口碑判断。

## 4. 输出数据契约

## 4.1 商品快照：现有下游最低兼容字段

写入 `fastmoss_voc_product_snapshot` 时至少提供：

| 字段 | 必填级别 | 缺失处理 |
|---|---|---|
| `batch_id` | 强制 | 缺失则拒绝整批 |
| `market` | 强制 | 从任务范围填充；不得从评论猜测 |
| `category_key` | 强制 | 从任务范围填充 |
| `fastmoss_product_id` | 条件强制 | 缺失时可从稳定商品 URL 派生 `urlsha256:{hash}` |
| `pool_type` | 可降级 | 写 `unknown`，不得默认 `classic` |
| `product_title` | 可降级 | 写空字符串并记录 `missing_product_title` |
| `category_path` | 可降级 | 写空字符串 |
| `sales_metric_json` | 可降级 | 写 `{}`，单个指标不可用时保留其它指标 |

建议增强字段：

```text
source_name
source_product_url
category_scope_name
product_form_hint
price
currency
rating_avg
rating_count
sales_count
listing_date
main_image_url
sample_pool
crawl_status
field_quality_json
raw_payload_json
captured_at
```

## 4.2 原始 VOC：现有下游最低兼容字段

写入 `fastmoss_voc_raw` 时至少提供：

| 字段 | 必填级别 | 缺失处理 |
|---|---|---|
| `batch_id` | 强制 | 缺失则拒绝 |
| `fastmoss_product_id` | 强制 | 使用商品快照中的真实或派生 ID |
| `voc_text` | 强制 | 空白文本不写 VOC 表，转抓取状态 |
| `normalized_voc_text` | 可派生 | Unicode NFKC、去首尾空白、合并连续空白；保留原始语言 |
| `voc_text_hash` | 可派生 | `sha256(normalized_voc_text)` |
| `voc_rank` | 可派生 | 无来源排名时按页面顺序从 1 编号 |
| `pool_type` | 可降级 | 继承商品快照；未知写 `unknown` |

建议增强字段：

```text
source_review_id
source_review_url
rating
reviewed_at
variant_text
reviewer_hash
helpful_count
has_image
has_video
verified_purchase
sample_pool
sampling_weight
evidence_scope
proxy_for_product_id
language_hint
is_truncated
field_quality_json
raw_payload_json
captured_at
```

如果当前物理表暂时没有增强列，抓取程序应将增强字段写入 `raw_payload_json`、批次 `manifest_json` 或独立 sidecar 表；不得因为表结构尚未升级而停止输出最低兼容字段。

## 5. 唯一键、标准化与去重

### 5.1 商品主键

优先级：

```text
source_product_id
> 从稳定商品详情 URL 提取的商品 ID
> urlsha256(normalized_product_url)
```

如果商品 ID 和稳定 URL 均缺失，状态写 `missing_product_identity`，该商品进入隔离区，不能写正式 VOC。

### 5.2 评论主键

优先使用来源评论 ID：

```text
{source_name}:{source_review_id}
```

来源没有评论 ID 时，派生：

```text
sha256(source_name + product_id + normalized_voc_text + reviewed_at_or_empty)
```

### 5.3 去重层级

1. 批次内：同一商品相同评论 ID，只保留一条。
2. 跨批次：评论 ID 相同，更新 `last_seen_at`，不重复计为新 VOC。
3. 无评论 ID：同一商品 `normalized_voc_text` 相同视为重复。
4. 不同商品出现完全相同长评论时，保留原始记录但标记 `suspected_template_review=true`，分析层默认降权。
5. 不得仅凭短文本如“很好”“推荐”跨商品删除，因为可能是真实独立评论。

原始 `voc_text` 永远保留，标准化文本只用于哈希和检索，不能覆盖原文。

## 6. 字段级容错矩阵

| 缺失/异常 | 是否保留商品 | 是否保留 VOC | 降级值/动作 | 质量代码 |
|---|---|---|---|---|
| 商品标题缺失 | 是 | 是 | 标题空；后续形态分析降级 | `missing_product_title` |
| 商品 ID 缺失但 URL 稳定 | 是 | 是 | 派生 `urlsha256` ID | `derived_product_id` |
| 商品 ID 与 URL 都缺失 | 隔离 | 否 | 等待人工/上游修复 | `missing_product_identity` |
| pool_type 缺失 | 是 | 是 | `unknown` | `missing_pool_type` |
| 销量部分缺失 | 是 | 是 | `sales_metric_json` 只写可用值 | `partial_sales_metrics` |
| 销量全部缺失 | 是 | 是 | `{}` | `missing_sales_metrics` |
| 评论星级缺失 | 是 | 是 | `null` | `missing_rating` |
| 评论时间缺失 | 是 | 是 | `null`，不能填抓取时间冒充评论时间 | `missing_reviewed_at` |
| 评论 ID 缺失 | 是 | 是 | 派生内容指纹 | `derived_review_id` |
| 评论正文为空 | 是 | 否 | 不创建空 VOC 行 | `empty_review_text` |
| 评论疑似截断 | 是 | 是 | 保存可见原文并置 `is_truncated=true` | `truncated_review_text` |
| 语言无法识别 | 是 | 是 | `language_hint=unknown` | `unknown_language` |
| SKU/色号缺失 | 是 | 是 | `null` | `missing_variant` |
| 图片/视频缺失 | 是 | 是 | false 或 null | `missing_review_media` |
| 页面显示0评论 | 是 | 否 | 记录 `no_visible_voc` | `confirmed_zero_reviews` |
| 评论区未加载 | 是 | 否 | 视为抓取失败，不能记0评论 | `review_section_not_loaded` |

原则：身份字段决定“能否安全归属”；评论正文决定“能否成为 VOC”；其它字段缺失只影响质量等级，不应导致整条评论丢失。

## 7. 抓取状态与错误分类

每个商品必须恰好写入一个最终状态：

```text
success
success_partial
no_visible_voc
blocked_auth
blocked_captcha
rate_limited
transient_network_error
parser_changed
permanent_not_found
missing_product_identity
invalid_scope
```

状态判定：

- `success`：商品与评论核心字段完整。
- `success_partial`：至少一条有效评论已保存，但存在字段缺失、分页中断或部分评论失败。
- `no_visible_voc`：页面明确显示没有评论，且评论容器已正常加载。
- `parser_changed`：页面可访问，但预期节点大面积缺失；不能当成没有评论。
- `blocked_auth` / `blocked_captcha`：停止该域名继续轰炸式重试。
- `permanent_not_found`：确认商品404/下架；不能仅凭一次超时判定。

## 8. 重试与恢复规则

### 8.1 可以自动重试

适用于超时、连接重置、HTTP 429、HTTP 5xx：

```text
同轮最多 3 次
等待 30 秒、2 分钟、10 分钟
每次增加 0–30% 随机抖动
```

三次仍失败后，保存已成功抓到的数据，商品状态写对应失败原因，并进入延迟队列：首次延迟 6 小时，第二次延迟 24 小时。

### 8.2 不应盲目自动重试

- 登录失效、验证码：暂停同域任务，转人工认证。
- 解析器变化：达到阈值后打开熔断，不继续把整批写成0评论。
- 商品永久404：记录后跳过；可在7天后低频复核一次。
- 身份字段缺失：进入隔离区，不生成虚假 ID（稳定 URL 派生除外）。

### 8.3 断点续跑

1. 每个商品完成后立即持久化，不能整批结束后一次性写入。
2. 已成功评论使用唯一键 upsert，重跑不得重复。
3. 分页保存 `last_success_cursor` 或页码。
4. 重跑从最后成功游标继续。
5. 某商品失败不得回滚其它商品。

## 9. 空数据保护与熔断

满足任一条件时，批次标记 `warning` 并停止自动发布到分析层：

- 目标商品数大于 10，但 80% 以上商品都是 `no_visible_voc`。
- 连续 5 个商品出现 `review_section_not_loaded`。
- 同一页面模板 20% 以上商品出现核心节点缺失。
- 抓到 VOC，但 50% 以上正文为空、过短或是纯 SKU 文本。
- 实际市场/类目与任务范围不一致。
- 单一商品贡献超过类目 VOC 的 30%，且未完成降权或补样。

熔断时保留已经抓到的原始数据和完整错误证据，不删除批次。

## 10. 批次质量结果

批次结束必须输出：

```json
{
  "batch_id": "...",
  "policy_version": "voc_crawl_policy_v1",
  "target_products": 20,
  "attempted_products": 20,
  "successful_products": 15,
  "partial_products": 2,
  "no_visible_voc_products": 2,
  "failed_products": 1,
  "raw_voc_count": 120,
  "deduplicated_voc_count": 105,
  "core_field_complete_rate": 0.98,
  "optional_field_complete_rate": 0.76,
  "status_counts": {},
  "missing_field_counts": {},
  "retry_counts": {},
  "quality_status": "ok|warning|blocked",
  "quality_reasons": []
}
```

质量状态含义：

- `ok`：核心身份、范围和评论文本可靠，可进入分析。
- `warning`：数据可分析，但代表性、字段完整度或抓取覆盖不足。
- `blocked`：身份、范围、解析器或登录状态不可靠，不得进入正式分析。

## 11. 与现有分析程序的最小交付要求

抓取程序至少要稳定写入：

```text
fastmoss_voc_product_snapshot
fastmoss_voc_raw
```

并保证以下连接成立：

```text
snapshot.batch_id = raw.batch_id
snapshot.fastmoss_product_id = raw.fastmoss_product_id
```

下游现阶段依赖字段：

```text
snapshot: batch_id, fastmoss_product_id, pool_type, market,
          category_key, product_title
raw:      batch_id, fastmoss_product_id, voc_rank, voc_text, pool_type,
          normalized_voc_text, voc_text_hash
```

不得在抓取侧提前生成洞察标签；抓取侧只提供原始证据、元数据、质量状态和代理证据边界。
