---
name: product-candidate-enricher
description: |
  Enrich a Feishu product candidate table for early-stage merchandise review.
  This skill standardizes the listing-date display to year-month-day, computes
  listing days from the current date, translates product names into Chinese,
  and tags a controlled subcategory set for hair accessories. Use this when
  the user provides a Feishu bitable URL and wants the table refreshed for
  screening, naming cleanup, or lightweight product structuring.
---

# Product Candidate Enricher

这个 skill 面向商品候选池的结构化整理，当前默认服务于发饰方向的数据清洗。

默认表格：

- `https://gcngopvfvo0q.feishu.cn/wiki/CtGxwJpTEifSh5kIVtgcM2vCnLf?table=tblKhPn64Q266tRz&view=vewmWdRUHq`

默认处理动作：

1. 将 `预估商品上架时间` 的显示格式更新为 `yyyy-MM-dd`
2. 基于当前日期计算 `上架天数`
3. 使用 LLM 将 `商品名称` 翻译为中文并写入 `中文名称`
4. 使用 LLM 将 `子类目` 打成以下受控标签之一：
   `发夹 / 发簪 / 发带 / 发箍 / 其它`

## 默认字段

表格至少需要这些字段：

- `商品名称`
- `中文名称`
- `子类目`
- `预估商品上架时间`
- `上架天数`

## 运行方式

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/product-candidate-enricher/run_pipeline.py --dry-run --limit 5
```

正式执行：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/product-candidate-enricher/run_pipeline.py
```

并发打标：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/product-candidate-enricher/run_pipeline.py \
  --max-llm-workers 8
```

快速刷新上架天数：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/product-candidate-enricher/run_pipeline.py \
  --skip-llm \
  --overwrite-listing-days \
  --max-date-workers 48
```

只处理指定记录：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/product-candidate-enricher/run_pipeline.py \
  --record-id rec27al11vQ6Ou \
  --record-id rec27al11vQdpY
```

## 常用参数

- `--dry-run`：只预览，不回写飞书
- `--limit N`：限制处理记录数
- `--record-id XXX`：只处理指定 record_id
- `--skip-llm`：跳过中文翻译和子类目打标
- `--overwrite-chinese-name`：即使已有 `中文名称` 也重写
- `--overwrite-subcategory`：即使已有 `子类目` 也重写
- `--overwrite-listing-days`：强制重写 `上架天数`
- `--max-llm-workers N`：LLM 并发数，默认 `8`，建议 `6-10`
- `--max-date-workers N`：只刷上架天数时的并发数，默认 `48`
- `--skip-date-format-update`：跳过日期字段格式更新
- `--subcategories A,B,C`：覆盖默认子类目列表，便于后续扩充

## LLM 配置

默认使用：

- base URL：`https://yunwu.ai/v1`
- model：`gpt-4.1-nano`

支持环境变量覆盖：

- `PRODUCT_CANDIDATE_ENRICHER_LLM_BASE_URL`
- `PRODUCT_CANDIDATE_ENRICHER_LLM_API_KEY`
- `PRODUCT_CANDIDATE_ENRICHER_LLM_MODEL`
- `PRODUCT_CANDIDATE_ENRICHER_TIMEZONE`

## 运行规则

- 默认只在 `中文名称` 或 `子类目` 为空时调用 LLM
- LLM 打标默认使用 `8` 并发执行，可通过 `--max-llm-workers` 调整
- 只刷新 `上架天数` 时会自动切到高并发 + 批量回写路径
- 正式执行时采用“单条结果完成即单条回写”的方式，不再等待整表全部处理完才写飞书
- `上架天数` 默认按“今天 - 上架日期”计算
- 日期字段格式更新失败时，不阻断整批处理，只记录 warning
- 每次运行会在 skill 目录下的 `output/` 写一份摘要 JSON，便于复盘
