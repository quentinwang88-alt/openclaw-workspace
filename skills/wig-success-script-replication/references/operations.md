# 运行手册

## 表与默认值

统一 Base 使用“成功脚本复刻表”现有 Base。Schema 的唯一定义在 `field_specs.py`：

- 成功脚本复刻表：13 个运营字段（含“人物脸部参考图”和“每产品生成数”）；
- 产品素材库：6 个运营字段；
- 复刻提示词表：5 个运营字段。

同产品默认累计目标 12 条，跨产品默认累计目标 4 条。每个母版版本与产品组合的第 1–3 条固定为高保真 H1/H2/H3；第 4 条起按一般复刻 G1–G5 循环，每条至少改变三个创意维度。选品表示人工已判断可复刻并授权迁移母版声明；系统不评分，也不逐项分析声明是否适配。人工明确填写的“禁用说法”仍会执行。三类模型任务统一使用 `gpt-5.6-sol + high`。

“每产品生成数”允许填写 1–20 的整数，含义是累计目标总数。已有 3 条后改填 6，只补第 4–6 条；继续填 6 时新增 0 条且不调用模型。修改目标会形成新批次，但持久化序号以 `(母版ID, 母版版本, 产品ID, sequence_no)` 唯一，跨批次不会重复。

“人物脸部参考图”维护在成功脚本复刻表，不得与产品素材库的“产品图片”混用；生成前必须上传，画面只继承脸部和身份特征，不继承原图头发。

运营只需填写资料并将“动作请求”设为“开始生成”。后台自动完成资料校验、母版解析与独立审查、母版启用、提示词编译、质检和飞书写回；母版内容未变化时直接复用，不重复调用模型。

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

正式主机的自动巡检使用 launchd，每60秒唤醒一次；worker 本身有非阻塞文件锁，上一轮仍在运行时本轮立即退出。任务行串行处理以避免竞争共享 outbox，单个任务内最多并行处理 2 个产品：

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

## 故障处理

- `check_runtime.py` 失败：先修复领域包导入、配置或数据库连接，不运行写操作。
- 模型失败：保留失败状态和错误摘要；不降级到其他模型。
- 飞书写回失败：运行 `retry_feishu_outbox.py`；不重跑母版分析或复刻编译。
- 单个跨品产品资料不足：记录该产品失败原因，其他产品继续。
