---
name: hair-style-review
description: |
  Independently analyze candidate hair-accessory products from a Feishu bitable.
  This skill scans records marked for candidate inclusion, judges the product
  style, decides whether the item is recommended, and writes the result back to
  the original table. Use it when the user wants a standalone style-review step
  that does not depend on script generation or later video workflows. It should
  also trigger for short natural-language commands such as “分析产品风格”,
  “分析备选商品风格”, “评估备选商品匹配度”, “判断产品是否推荐”, and
  “分析这个表格的产品风格：<飞书链接>”.
---

# Hair Style Review

这个 skill 用于独立执行“发饰风格匹配度分析”，不依赖脚本生成或后续视频流程。

推荐触发指令：

- `分析产品风格`
- `分析备选商品风格`
- `评估备选商品匹配度`
- `判断产品是否推荐`
- `分析这个表格的产品风格：<飞书链接>`

默认表格：

- `https://gcngopvfvo0q.feishu.cn/wiki/CtGxwJpTEifSh5kIVtgcM2vCnLf?table=tblKhPn64Q266tRz&view=vewmWdRUHq`

默认行为：

1. 扫描 `是否列入备选 = 是` 或 `是否纳入备选 = 是` 的记录
2. 如果表里没有备选字段，则扫描结果为空且输入可用的记录
3. 默认跳过已存在 `产品风格 / 是否推荐 / 详细原因` 的记录
4. 基于商品标题、商品图片、商品基础信息做风格分析
5. 回写 `产品风格 / 是否推荐 / 详细原因`
6. 如果表中已存在 `风格分析状态 / 风格分析时间 / 风格分析错误信息`，则一并更新

## 输入字段

支持以下别名：

- 标题：`商品标题` / `商品名称`
- 图片：`商品图片`
- 基础信息：`商品基础信息` / `备注`
- 备选标记：`是否纳入备选` / `是否列入备选`

同时支持这类链接：

- 标准 bitable/base 链接
- wiki 下直接 bitable 链接
- wiki 下 sheet 中嵌入 bitable block 的链接，例如 `...?sheet=...&table=...&view=...`

## 输出字段

- `产品风格`
- `是否推荐`
- `详细原因`

如果缺少这 3 个字段，skill 会尝试自动创建文本字段。

## 运行方式

批量扫描：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/hair-style-review/run_pipeline.py
```

只看不写：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/hair-style-review/run_pipeline.py --dry-run
```

单条或多条补跑：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/hair-style-review/run_pipeline.py \
  --record-id rec123 \
  --record-id rec456
```

强制重跑已有结果：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/hair-style-review/run_pipeline.py --force
```

## 运行规则

- 并发数固定为 `2`
- 优先使用图片做判断；图片缺失时允许只基于标题和基础信息分析
- 单条失败不影响整体
- 正式执行时按“单条完成即单条回写”方式写飞书
- 模型配置直接复用 `creator_crm/core/llm_analyzer.py`
