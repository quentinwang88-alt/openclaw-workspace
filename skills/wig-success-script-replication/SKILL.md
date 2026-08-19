---
name: wig-success-script-replication
description: "Run and maintain the Mexico wig successful-script replication workflow from Feishu: one click validates inputs, builds and reviews or reuses a mother contract, activates it, then compiles self-contained video prompts for human-selected products. Use for requests such as ‘开始生成假发复刻’, ‘生成假发复刻提示词’, ‘检查待执行假发复刻任务’, retrying Feishu writeback, or preparing the three-table schema. Do not use for automatic product-fit scoring, video generation, publishing, or general video remakes."
---

# 假发成功脚本复刻

通过薄入口运行 V1.2：人工填资料并选品 → 一键生成 → 自动解析/审查或复用母版 → 自动启用 → 按累计目标补齐完整提示词。不进行产品匹配度评分。

## 运行规则

1. 读取 [references/operations.md](references/operations.md)，按其命令、状态和安全限制执行。
2. 自然语言请求只映射到 `scripts/openclaw_wig_replication_task.py` 公开的白名单动作；不把用户原文拼入 shell。
3. 修改飞书 Schema 时必须先运行 `ensure_feishu_schema.py --dry-run`，保留输出的 plan hash，再显式传给 `--apply --confirm-plan` 。未经 dry-run 不得执行。
4. 飞书“开始生成”代表用户明确授权本轮自动解析、审查、启用母版并生成；不再设置中途人工确认关口。
5. “每产品生成数”是同一母版版本与产品组合的累计目标，不是本轮新增数。同品默认目标 12 条，跨品默认目标 4 条；只生成缺失序号，目标不变不重复调用模型。
6. 每个产品第 1–3 条固定为高保真 H1/H2/H3；第 4 条起为一般复刻 G1–G5 循环，每条至少真实改变三个创意维度，同时保留冲突、前段揭晓、强反差、证明和 CTA 五个成功机制。
7. 人工选中即代表母版声明迁移获授权，不计算匹配分，也不逐项分析商品声明是否匹配。只服从人工明确填写的禁用说法，并检查素材完整性与提示词可执行性；不得输出“条件锁、当前禁止执行、等待核验”等内部控制语言。
8. 飞书写回失败时只重试 outbox，不重新调用模型。
9. 后台巡检只领取“开始生成”。任务行串行执行，单任务内最多并行处理 2 个产品；同一记录通过本机 worker 锁和 RDS named lock 防止重复执行。

## 能力边界

- 只保留三类模型任务：`MOTHER_ANALYZE`、`MOTHER_REVIEW`、`REPLICATION_COMPILE`。
- 三类模型任务全部固定为 `gpt-5.6-sol + high`；禁止弱模型静默降级。
- 不做 `PRODUCT_FIT_ASSESS`，不自动推荐或挑选产品，也不对母版商品声明执行二次确认或迁移分析。
- 不自动生成视频，不上传参考视频，不发布。
- 不直接写 RDS；所有领域状态、版本、幂等和 outbox 都由 `packages/wig_success_replication` 处理。

## 交付

完成后报告处理记录、母版状态、同品/跨品产品数、成功提示词数、失败数及可操作原因。不展示隐藏推理内容。
