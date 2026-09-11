---
name: original-content-operator
description: 用自然语言检查、生成、续跑和调度飞书中的原创内容任务，覆盖15秒原创和20–45秒原创长视频。用户说“检查待执行原创任务”“跑一条原创脚本”“生成一条原创长视频脚本”“检查进入生产的脚本”“生成勾选的原创视频”“继续长视频”或“发布已确认长视频”时使用。只调用现有白名单适配器，不创建任务、不修改进入生产或确认发布勾选。
---

# 原创内容调度

这是 OpenClaw 日常语言调度的轻量入口。普通检查和执行不需要读取两个底层 skill 的长篇实现说明；只有开发、故障诊断或合同变更时，才读取对应 `SKILL.md`。

## 判断阶段

- 用户提到“待执行任务、运营任务、生成脚本、规划、replan”：处理运营任务到生产脚本。
- 用户提到“进入生产、勾选脚本、生成视频、继续长视频、远端任务、成片、确认发布”：处理生产脚本到视频或发布队列。
- 用户只说“跑一条原创长视频”且没有提到勾选、成片或续跑：默认生成一行待执行运营任务的长视频脚本；视频规格仍以飞书字段为准。
- 用户说“从任务跑到成片”：先生成脚本；只有对应生产脚本已经勾选“进入生产”时才继续生成视频，不能代替用户勾选。

## 运营任务到生产脚本

只调用：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/original-script-generator/scripts/openclaw_original_script_task.py <action> [参数]
```

| 意图 | 调用 |
| --- | --- |
| 检查待执行原创任务 | `check --limit 1` |
| 跑一条原创任务或长视频脚本 | `run --limit 1` |
| 运行指定产品 | `run --product-code <产品编码>` |
| 只规划指定记录 | `plan --record-id <rec...>` |
| 续跑失败或部分完成记录 | `resume --record-id <rec...>` |
| 更新输入后重新规划 | `replan --record-id <rec...>` |
| 刷新人物和穿搭配置 | `refresh-production-config` |

`--limit` 表示最多处理几行运营任务，不表示生成几条脚本。每行的生成数量和视频规格只读飞书字段。产品匹配多行时停止并要求明确 record ID。

## 生产脚本到视频或发布队列

只调用：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/scripts/openclaw_original_batch_sync.py <check|sync> [参数]
```

| 意图 | 调用 |
| --- | --- |
| 检查进入生产的脚本 | `check --limit 20` |
| 生成所有已勾选脚本 | `sync --limit 20` |
| 生成或续跑指定记录 | `sync --record-id <rec...> --limit 1` |
| 生成指定产品的已勾选脚本 | `sync --product-code <产品编码>` |
| 发布已完成且已确认的长视频 | `sync --record-id <rec...> --limit 1` |

同步器拥有唯一分流权威：

- 15秒原创进入短视频运行管理表；
- 20–45秒原创长视频进入 Plan C；
- 20秒以上复刻进入复刻分段执行器。

不要直接调用内部 runner 绕过分流。`sync` 只处理已经勾选“进入生产”的脚本；发布只处理已经勾选“确认发布”的长视频。OpenClaw 不修改这两个勾选。

## 执行边界

- `check` 只读，不调用模型、不生成媒体、不写飞书。
- 用户明确要求“跑、生成、继续、送生产”时，可直接执行对应 `run` 或 `sync`。
- 长视频的“进入生产”勾选授权连续完成关键帧、H3、TTS 和合并，不增加逐段确认。
- 遇到 `SUBMIT_UNCERTAIN` 不自动重试，先查询已登记的远端 task ID。
- 当前长视频发布桥接只支持养号、无挂车；带货长视频不能使用该发布入口。
- 旧 S1–S4 `run_pipeline.py` 只维护历史记录，禁止用于新自然语言任务。

## 结果汇报

脚本阶段报告：record ID、产品编码、视频规格、批次 ID、计划/完成/失败数量和长视频 job ID。

生产阶段报告：生产脚本 record ID、实际分流、远端状态、是否 `FINAL_READY`、成片路径、飞书回写和是否进入发布队列。

只读检查明确说明未调用模型、未生成媒体、未写表。

## 深入资料

- 开发或诊断脚本生成：读取 `/Users/likeu3/.openclaw/workspace/skills/original-script-generator/SKILL.md`。
- 开发或诊断送生产：读取 `/Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/SKILL.md`。
- 全链路交接：读取 `/Users/likeu3/.openclaw/workspace/docs/ORIGINAL_CONTENT_SHORT_LONGFORM_HANDOFF_20260909.md`。
