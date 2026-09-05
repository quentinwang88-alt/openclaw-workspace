---
name: wig-success-script-replication
description: "Run and maintain Mexico wig successful-script replication from Feishu, with separate sales/nurture purposes and delivery to the video-script pool. Use for 开始生成假发复刻、检查待执行假发复刻任务、修订假发复刻脚本、假发脚本对照测试、writeback retries or replication/pool schema work. Do not use for product-fit scoring, video generation, publishing, or general video remakes."
---

# 假发成功脚本复刻

通过薄入口运行脚本总库 V1：人工选品、选用途 → 一键生成 → 复用或解析母版 → 按累计目标补齐完整提示词 → 自动进入“原创视频生产脚本”总库。不进行产品匹配度评分。

## 运行规则

1. 读取 [references/operations.md](references/operations.md)，按其命令、状态和安全限制执行。
2. 自然语言请求只映射到 `scripts/openclaw_wig_replication_task.py` 公开的白名单动作；不把用户原文拼入 shell。
3. 修改飞书 Schema 时必须先运行 `ensure_feishu_schema.py --dry-run`，保留输出的 plan hash，再显式传给 `--apply --confirm-plan` 。未经 dry-run 不得执行。
4. 飞书“开始生成”代表用户明确授权本轮复用或一次解析母版、自动启用并补生成；独立母版审查仅在明确测试时运行，不设中途人工确认关口。
   “验证说明”仅为人工选填参考，留空不拦截一键生成或分步母版处理，也不自动补造播放量、销量等成功依据。
5. “每产品生成数”是同一母版版本、产品与发布用途组合的累计目标，不是本轮新增数。同品默认12、跨品默认4；用途内只补缺失序号。带货与养号分别累计，切换用途不改写旧脚本。
6. 每个用途/产品第1–3条固定H1/H2/H3，第4条起G1–G5循环。两种用途都继承母版本来的机制，不统一强加商业五锚点。H1优先沿用原拍法，H2/H3轻变，一般复刻规划多维表达；不以文本相似度区间、证明动作数或变化维度数作为合格门槛。
7. 人工选中即代表母版声明迁移获授权，不计算匹配分，也不逐项分析商品声明是否匹配。只服从人工明确填写的禁用说法，并检查素材完整性与提示词可执行性；不得输出“条件锁、当前禁止执行、等待核验”等内部控制语言。
8. 飞书写回失败时只重试 outbox，不重新调用模型。
9. 后台巡检只领取“开始生成”。任务行串行执行，单任务内最多并行处理 2 个产品；同一记录通过本机 worker 锁和 RDS named lock 防止重复执行。
10. “发布用途”生成前必填带货/养号；“挂车设置”选填，带货默认挂车、养号默认不挂车，可明确覆盖。关联商品、用途和是否挂车是独立维度。
11. 新结果同时通过outbox进入提示词表及脚本总库。总库脚本ID稳定为`wsr_<prompt_id>`，重试不重生成、不覆盖人工改稿/挂车/审核状态。总库“进入生产”是唯一人工放行入口，系统不代勾选。
12. 历史未记录用途的数据迁移为“未分类”，不可猜测归类或自动补生成。明确母版/版本及用途后，用`migrate_legacy_purpose.py`先dry-run再按hash应用；该命令不导入旧脚本、不补写未经确认的历史图片快照。
13. 总库字段升级使用`script_pool_schema.py`先dry-run再按hash应用。人物图片与商品图片分别保留，口播原文不由同步器重写；无口播不强制配音。
14. 新生成必须把全部已选人物/商品图按同一有序`reference_manifest`传给编译与总库，不得静默截取前两张。原图和编译缩图分别记录SHA256；跨Base转存验证原图字节一致后只重绑定token，manifest身份不变。清单冻结在既有handoff JSON和总库binding metadata，不改历史记录。
15. 原始脚本保持完整，执行摘要分核心机制、基准拍法、可变表达；具体秒点和微动作不自动升级为核心。旧合同兼容投影标记`legacy_derived`，不能假称重新解析。规则检查只处理明显错误，不要求特定中文素材短语；需要模型修复时传失败正文并局部修复，最多一轮。`mechanical_passed`不代表语义质量或成片动作通过。
16. 普通“特殊要求”默认只影响本次编译；仅明确“母版要求：…”或`[母版要求]`分节进入母版级输入，其余仍保留为创作要求。补生成不覆盖旧稿；“修订某条”“对照测试”使用独立修订入口，详见运行手册。测试/修订不占累计序号，不自动送生产或发布。

## 能力边界

- 正常流程只运行必要的`MOTHER_ANALYZE`与`REPLICATION_COMPILE`；独立`MOTHER_REVIEW`仅测试，`REPLICATION_QA_REPAIR`仅在存在实际失败项时运行一轮。
- 模型任务保持 `gpt-5.6-sol + high`；不静默降级，不增加生产逐条模型质检。
- 不做 `PRODUCT_FIT_ASSESS`，不自动推荐或挑选产品，也不对母版商品声明执行二次确认或迁移分析。
- 不自动生成视频，不上传参考视频，不发布。
- 生产领域状态、版本、幂等与outbox由领域包处理。显式迁移仅使用已审查的hash确认脚本，不允许临时SQL猜测历史业务用途。

## 交付

完成后报告处理记录、母版状态、同品/跨品产品数、成功提示词数、失败数及可操作原因。不展示隐藏推理内容。
