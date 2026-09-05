# 运行手册

## 表与默认值

统一 Base 使用“成功脚本复刻表”现有 Base。Schema 的唯一定义在 `field_specs.py`：

- 成功脚本复刻表：15个运营字段（新增“发布用途”“挂车设置”）；
- 产品素材库：6 个运营字段；
- 复刻提示词表：5 个运营字段。

同产品默认累计目标12条，跨产品默认4条。每个母版版本、产品和发布用途组合的第1–3条为H1/H2/H3，第4条起G1–G5循环。两种用途都保留母版真实机制，不强行加入商品证明、购物CTA或口播。选品由人工判断，不做匹配评分。人工禁用说法仍执行。模型任务统一使用`gpt-5.6-sol + high`。

“每产品生成数”允许1–20整数，表示用途内累计目标。已有3条改6只补4–6；继续填6新增0且不调用模型。持久化序号以`(母版ID, 母版版本, 产品ID, 发布用途, sequence_no)`唯一。“发布用途”必填；“挂车设置”留空按用途默认，带货挂车、养号不挂车，可显式覆盖。

“人物脸部参考图”维护在成功脚本复刻表，不得与产品素材库的“产品图片”混用；生成前必须上传，画面只继承脸部和身份特征，不继承原图头发。

新生成使用`reference_manifest` V2：顺序先全部人物图、再全部商品图，不静默截取；每张保存原始字节SHA256与编译1024JPEG派生SHA256。显式超限或任一已选图片无法读取时返回具体错误，不把缺图任务当完整任务编译。新正文按“人物参考图组/商品参考图组”表达，不写死前两张、后两张；旧正文不批量改写。

母版默认一次分析完成结构整理；`execution_summary`分核心机制、基准拍法、可变表达，不把所有原稿秒点和微动作锁死。原稿不覆盖，不设输出体量目标。独立母版审查仅显式测试；未调用时记录`review_status=not_run`，不能报告“审查通过”。提示词快照保存执行摘要、版本、`mother_core_points`与`prompt_qa`，旧合同投影注明`legacy_derived`。规则检查通过不代表动作执行、人物一致或平台防判重已获验证。

运营只需填写资料并将“动作请求”设为“开始生成”。后台完成资料校验、母版复用或一次解析、自动启用、提示词编译、机械检查和飞书写回。普通“特殊要求”只进入编译，改变它不重复解析母版；明确永久要求可写单行“母版要求：…”或`[母版要求]`分节，使用`[本次要求]`切回创作要求。不新增飞书列。目标已满足时保留旧稿，不借修改要求覆盖历史。

最终提示词按统一设定、画面动作及结果、结尾声音表达；上传流程和实际图号由执行层统一补入，不因缺少“不上传参考视频”等固定短语调用模型重写。修复请求必须携带失败原稿及具体错误，最多一轮；纯素材说明由程序处理。写回失败只重试outbox，已有视频任务只继续抓取，不重提单。

“验证说明”为选填参考，留空不会阻塞带货或养号生成；已有内容原样传给母版处理，不自动补造成功依据。该调整不取消母版名称、完整脚本、来源产品及生成素材的必要校验。

## 首次检查

```bash
cd /Users/likeu3/.openclaw/workspace/skills/wig-success-script-replication
python3 scripts/check_runtime.py
python3 scripts/ensure_feishu_schema.py --dry-run
python3 scripts/apply_rds_migration.py
python3 scripts/install_launch_agent.py
```

`ensure_feishu_schema.py` 默认就是 dry-run。它只读取现状并打印变更计划和 plan hash。无网络的结构预览可使用 `--offline`。

## 应用 Schema

必须在同一份代码和同一 Base 上先 dry-run：

```bash
python3 scripts/ensure_feishu_schema.py --dry-run
python3 scripts/ensure_feishu_schema.py --apply --confirm-plan <上一步输出的SHA256>
```

第二条命令会重新读取现状；计划改变、hash 不匹配或没有本机 dry-run receipt 时立即停止。不删除表、字段、视图或记录，不操作原来分散 Base。

RDS 安装或升级同样先 dry-run，随后显式确认哈希。脚本按文件名顺序执行 `migrations/*.sql`；V1.2 会为历史提示词按创建时间回填累计序号，原有每产品前三条标记为 H1–H3，并建立序号唯一约束。它不会由 import 或后台任务自动运行：

```bash
python3 scripts/apply_rds_migration.py
python3 scripts/apply_rds_migration.py --apply --confirm-sha256 <上一步输出的SHA256>
```

自动巡检使用launchd，频率以已安装plist为准；2026-09-03本机核验为21600秒（6小时），本次升级保留该设置。不要为了部署重跑安装器而覆盖运营频率。worker有非阻塞文件锁，上一轮运行时本轮退出；单个任务最多并行2个产品。以下安装命令仅用于用户明确要求新装或改调度：

```bash
python3 scripts/install_launch_agent.py
python3 scripts/install_launch_agent.py --apply --confirm-sha256 <上一步输出的SHA256>
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.likeu3.wig-success-script-replication.plist
```

## 任务动作

OpenClaw 只调用白名单适配器：

```bash
python3 scripts/openclaw_wig_replication_task.py check --limit 5
python3 scripts/openclaw_wig_replication_task.py one-click --record-id recXXX
python3 scripts/openclaw_wig_replication_task.py process-mother --record-id recXXX
python3 scripts/openclaw_wig_replication_task.py confirm-mother --record-id recXXX
python3 scripts/openclaw_wig_replication_task.py generate --record-id recXXX
python3 scripts/openclaw_wig_replication_task.py retry-outbox --limit 50
```

`one-click` 是正式主入口，必须提供明确的 `rec...` 记录 ID。旧的分阶段命令只保留诊断兼容；`check` 是只读预览。

直接 runner 命令：

```bash
python3 scripts/run_pending_tasks.py --dry-run --limit 5
python3 scripts/run_pending_tasks.py --action one-click --record-id recXXX
python3 scripts/run_pending_tasks.py --action process-mother --record-id recXXX
python3 scripts/run_pending_tasks.py --action confirm-mother --record-id recXXX
python3 scripts/run_pending_tasks.py --action generate --record-id recXXX
python3 scripts/retry_feishu_outbox.py --dry-run --limit 50
python3 scripts/retry_feishu_outbox.py --limit 50
```

## 自然语言映射

- “检查待执行假发复刻任务” → `check`
- “开始生成假发复刻 recXXX” → `one-click --record-id recXXX`
- 飞书选择“开始生成” → 后台巡检自动执行 `one-click`
- “重试假发复刻飞书写回” → `retry-outbox`

自然语言中的数量不会被解释为任务行数。需要调整提示词累计目标时，应填写飞书“每产品生成数”；留空由领域包使用同品12、跨品4的默认目标。

## 明确修订与对照测试

“补生成”仍走累计入口；“修改已有脚本”走`revise`，“只改变指定内容做对照”走`compare`。不要为了测试复制新母版或提高累计目标。由agent把用户明确要求整理到UTF-8 instruction文件，必要时从该原稿冻结点生成changes JSON；不要求运营额外填写文件或新增表列。

```bash
python3 scripts/openclaw_wig_replication_task.py revise --prompt-id wsr_prompt_XXX --instruction-file /absolute/revision.txt --dry-run
python3 scripts/openclaw_wig_replication_task.py compare --prompt-id wsr_prompt_XXX --instruction-file /absolute/revision.txt --changes-file /absolute/changes.json --apply
python3 scripts/openclaw_wig_replication_task.py export-revision --artifact /absolute/revision_XXX.json --apply
```

- `revise/compare`默认预览，不调用模型、不写表；带`--snapshot`可用完整来源快照做离线预览，否则预览会明确`inputs_resolved=false`。`--apply`精确读取原脚本及原母版版本、核验全部参考图后编译一条，只保存隔离本地产物。同请求复用结果；不保存到累计序号表。
- changes是精确替换列表，例如`[{"point_id":"源记录中的实际ID","operation":"replace","replacement_requirement":"本次仍需达到的核心结果及获准拍法","reason":"用户明确的测试修改"}]`。不猜测ID，不把本次取消的拍法继续当作有效要求；未改变的核心点保持原文。原点与有效点分别冻结，不覆盖原母版。
- `export-revision --apply`把已完成产物作为新脚本写入现有总库，并立即只交付该条outbox。写回异常时重复导出同一产物，只补写，不重调模型，也不顺带处理其他任务。已导出的修订可继续修订，以上一版正文和有效要求为基准。
- 用户已明确要求“修订并写入”时，可以连续执行修订与导出，无需再加中途确认；只要求设计或预览时不使用`--apply`。这不是新的人工审核关口。
- 测试/修订记录带管理标题标识、默认不进入生产，不自动生成视频或发布。用户要求视频对照时，继续使用既有总库同步和视频生成入口，并限定这些测试记录；检查仅使用独立测试CLI，不恢复生产自动审核。
- 纯代码/提示词升级不自动改变旧母版版本或重置累计量。旧合同保留并标记兼容来源；显式`process-mother`才按新实现刷新母版，可能产生新版本，不应拿它代替单条修订。历史“验证说明”曾未参与指纹，单改它无法可靠触发旧母版重析，确需更新时使用这个显式刷新入口。

## 故障处理

- `check_runtime.py` 失败：先修复领域包导入、配置或数据库连接，不运行写操作。
- 模型失败：保留失败状态和错误摘要；不降级到其他模型。
- 飞书写回失败：运行 `retry_feishu_outbox.py`；不重跑母版分析或复刻编译。
- 写回与预览不初始化模型客户端，不依赖模型账号认证是否可用。局部修复失败时，已通过脚本仍持久化；续跑只补缺失序号。
- 单个跨品产品资料不足：记录该产品失败原因，其他产品继续。

## 视频脚本总库 V1

继续使用“原创视频生产脚本”表`tblIvHJ0nsn9WCwi`作为统一总库。新增用途、来源、是否挂车、人物参考图四字段；原字段/记录/视图保留。总库专用视图为全部脚本、墨西哥复刻、养号内容、待送生产。

新结果写回提示词表时同时登记总库及发布元数据。提示词表无需重复审核；只在总库检查并勾选“进入生产”。挂车调整独立于脚本正文；已提交/排期任务不原地覆盖。店铺及平台商品ID属于发布配置，内部S开头产品编码不能当平台挂车ID。墨西哥发布标题走现有西语标题准备流程，不使用中文管理标题；缺标题/店铺/挂车映射时不发布，但不妨碍脚本入库。

```bash
python3 scripts/script_pool_schema.py
python3 scripts/script_pool_schema.py --apply --confirm-plan <dry-run-hash>
python3 scripts/migrate_legacy_purpose.py
python3 scripts/migrate_legacy_purpose.py --mother-id <mother-id> --mother-version 2 --publish-purpose 养号 --cart-setting 不挂车
python3 scripts/migrate_legacy_purpose.py --mother-id <mother-id> --mother-version 2 --publish-purpose 养号 --cart-setting 不挂车 --apply --confirm-plan <dry-run-hash>
```

RDS003通过迁移台账识别已部署001/002，避免重复运行历史序号回填。旧数据用途设未分类、挂车未知；显式归类工具只改分类，不改正文、H/G序号、图片快照或outbox。旧36条结果需业务确认分类和历史图片来源后再单独导入，不自动以当前图片冒充历史生成时的图片。

新脚本人物图和商品图独立保存并按角色顺序传递；即梦全能参考最多12图，H3多参考最多9图。成功脚本复刻暂不走iMini单首帧路径或首尾帧模式，超限/不支持模式在付费前明确阻塞。角色说明只加在执行上下文，不改写总库正文。已明确选择并就绪的统一首帧可交接；该版本不会自动生成首帧。
