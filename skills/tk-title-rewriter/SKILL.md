---
name: tk-title-rewriter
description: |
  从飞书多维表格读取中文产品标题，按产品类目自动匹配本地 Prompt 模板，
  将标题改写为合规的 TikTok Shop 目标语言标题，并批量写回飞书。
  适用于用户给出飞书表格 URL，要求“优化 TK 标题”“按类目改写越南语/泰语标题”
  “把中文标题重写成 TikTok Shop 上架标题”的场景。
---

# TK Title Rewriter

这个 skill 面向商品标题优化场景，当前默认支持：

- `发夹` → 越南语标题
- `针织开衫` → 泰语标题

核心流程：

1. 解析飞书 URL 或 `base_token + table_id`
2. 自动识别 `原始标题字段`、`产品类目字段`、`输出字段`
3. 跳过空标题与已有 `TK标题` 的记录
4. 按类目精确匹配或别名匹配本地 `prompts/*.md`
5. 按类目分批调用 LLM 改写标题
6. 批量写回飞书，并输出成功 / 跳过 / 失败汇总

## 运行方式

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/tk-title-rewriter/run_pipeline.py \
  --feishu-url "https://gcngopvfvo0q.feishu.cn/wiki/XXX?table=tblYYY&view=vewZZZ"
```

如果想先看识别结果，不落表：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/tk-title-rewriter/run_pipeline.py \
  --feishu-url "https://gcngopvfvo0q.feishu.cn/wiki/XXX?table=tblYYY&view=vewZZZ" \
  --dry-run
```

如果标题字段、类目字段或输出字段不是默认名字，可以显式覆盖：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/tk-title-rewriter/run_pipeline.py \
  --feishu-url "https://gcngopvfvo0q.feishu.cn/base/XXX?table=tblYYY&view=vewZZZ" \
  --title-field "产品名称" \
  --category-field "品类名称" \
  --output-field "TK标题"
```

如果要强制重写已有标题：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/tk-title-rewriter/run_pipeline.py \
  --feishu-url "https://gcngopvfvo0q.feishu.cn/base/XXX?table=tblYYY&view=vewZZZ" \
  --overwrite
```

如果需要额外回写一个中文摘要字段：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/tk-title-rewriter/run_pipeline.py \
  --feishu-url "https://gcngopvfvo0q.feishu.cn/base/XXX?table=tblYYY&view=vewZZZ" \
  --cn-summary-field "优化后的标题（中文）"
```

## 默认字段匹配

原始标题字段优先尝试：

- `产品标题`
- `原始标题`
- `标题`
- `中文标题`
- `title`

产品类目字段优先尝试：

- `产品类目`
- `类目`
- `品类`
- `category`

输出字段默认：

- `TK标题`

## 运行规则

- LLM 每批最多处理 `20` 条
- 飞书每批最多写回 `500` 条
- 类目模板优先走“文件名精确匹配”，其次走 `aliases`
- 若输出字段不存在，会自动创建文本字段
- 若某个品类没有模板，会跳过并在汇总里列出来
- 若某批模型输出无法完整解析，只影响该批对应记录，不会中断整表其他记录
- 默认优先读取仓库环境中的 `LLM_API_URL / LLM_API_KEY / LLM_MODEL`，也支持单独传 `--llm-base-url / --llm-api-key / --llm-model`

## 模板目录

- `prompts/发夹.md`
- `prompts/针织开衫.md`

新增品类时，直接按现有模板结构复制一份到：

- `prompts/<品类名>.md`

要求：

- 文件名就是精确匹配类目名
- 文件头必须包含 `aliases`、`target_market`、`target_language`
- 第一个代码块中的全文会被当成 System Prompt
