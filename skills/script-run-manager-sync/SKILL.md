---
name: script-run-manager-sync
description: |
  将“原创视频脚本”飞书多维表格里勾选了“是否可同步”的记录，同步到“短视频自动脚本运行管理表”。
  每条脚本单独新增一条目标记录，默认把产品编码和脚本槽位写入任务名（例如 `产品编码.S1`、`产品编码.S1V1`）、把详细脚本写入提示词、把产品图片写入参考图。
  支持正式脚本和 20 个变体脚本，总计最多 24 条脚本；同步成功后自动取消源表勾选，并回写同步状态与同步时间。
  同时支持“人工短视频脚本库”：用户可在飞书勾选 `立即同步` 让 OpenClaw 在一分钟内触发单条同步，或在对话中说“立即同步人工脚本库”。
  对新“原创视频生产脚本”表，用户说“同步勾选原创脚本”“把勾选脚本送生产”“检查进入生产的脚本”“同步产品 173... 的原创生产脚本”时，必须使用 `scripts/openclaw_original_batch_sync.py`；只同步勾选`进入生产`的一行一条脚本。
  该表也作为视频脚本总库，兼容成功脚本复刻、视频复刻和人工编写。“把总库勾选脚本送生产”“同步墨西哥复刻脚本”仍使用同一白名单入口；用途和是否挂车独立读取。
---

# Script Run Manager Sync

## 视频脚本总库 V1

`original-batch`是兼容入口名称，不表示全部记录都是原创。新来源读取“脚本来源”；未填来源的旧原创继续兼容，`wsr_`前缀必须显式标注成功脚本复刻。总库仅处理勾选“进入生产”的行；检查/dry-run不建字段、不登记数据库、不改勾选。

- 用途、来源、是否挂车分别维护。养号默认不挂车，可明确选择是；原种草专线硬性不挂车约束保留。
- 复刻提示词及口播保持原文。存在口播使用`PRESERVE_SOURCE_COPY`；无口播不强补。中文脚本管理标题不是发布标题。
- 人物图与商品图按顺序一起交接，并保存角色索引合同。`check` 对未就绪首帧只报告等待；正式 `sync` 对同时勾选“进入生产”和“生成首帧”的记录先生成或复用首帧，成功后同轮继续同步，失败保留勾选。成功脚本复刻的统一首帧与人物、商品参考一起交接，避免静物开场丢失后段人物身份；原创参考图规则不变。
- WSR 已就绪首帧在所选未提交任务送生产前仍核对当前脚本与原图内容指纹；换图或改开场会失效，同图换 token 不会重新收费生成。未勾选首帧不触发此步骤，已提交或已排期任务不得借升级刷新首帧。
- WSR 图片交接使用人物合同 V2：每张图绑定实际运行表 token、原图 SHA256 与派生图 SHA256。原图哈希和有序角色决定 manifest 身份，转存 token 不决定内容身份；转存时用已下载字节校验，不按文件名猜复用。母版关键点随同一合同交接，旧任务缺少冻结点时明确 `final_prompt_only`，不伪造母版来源，不新增飞书人工字段。
- WSR显式修订/测试记录不占正式累计序号；已导入总库的修订从绑定快照读取冻结素材和原母版/有效检查点。仍只同步用户勾选进入生产的记录，不因为“测试”标签自动提交或发布；允许变化和版本留在合同，不加入视频正文。
- 先登记统一脚本元数据，再写运行表。`S...`内部产品编码写全球产品ID；挂车商品ID从登记映射取得，无挂车则清空平台商品ID。
- 未提交记录可幂等更新；已提交/已排期冲突保留勾选并报错，不重置状态或重复建任务。
- 选择器支持受限字母数字产品编码`S260...`；记录ID仍须`rec...`，不接受shell原文。

## 种草不挂车脚本

种草脚本使用独立源适配器 `core/seeding_batch_source.py`，不会复用原创生产脚本源表的业务字段。人工源表只需满足 `发布策略=种草不挂车`；适配器会在内部冻结 `脚本类型=种草脚本`、`发布用途=种草`、`是否挂车=否`、`内容分支=SEEDING_ORGANIC`。发布策略缺失或冲突都会阻断同步。商品编码只写入 `全球产品ID` 供内部分析，运行表的可挂车 `产品ID` 保持为空。

```bash
python3 scripts/openclaw_seeding_batch_sync.py check \
  --source-url '<种草视频生产脚本表URL>'

python3 scripts/openclaw_seeding_batch_sync.py sync \
  --source-url '<种草视频生产脚本表URL>' \
  --record-id recXXXXXXXX
```

## 核心能力

这个 skill 会：

1. 读取源表里勾选了 `是否可同步` 的记录
2. 从每条源记录里提取 `脚本方向一 ~ 脚本方向四`
3. 按“一条脚本 = 一条目标记录”拆分任务
4. 把 `产品编码` 写入目标表 `任务名`
5. 把脚本正文写入目标表 `提示词`
6. 把 `产品图片` 写入目标表 `参考图`
7. 默认从脚本主数据库/源表回查并写入 `脚本ID` 与 `店铺ID`
8. 自动把上游来源归一为运行表单选字段 `脚本类型`：原创脚本 / 短视频复刻脚本 / 养号脚本
9. 同步成功后回写源表 `同步状态` / `同步时间`，并自动取消 `是否可同步`

## 默认表格

- 源表：`原创视频脚本`
- 目标表：`短视频自动脚本运行管理表`

`run_pipeline.py` 已内置这两张表的 URL，通常不需要额外传参。

## 任务命名

- 目标表 `任务名` 默认写成 `产品编码.脚本槽位`
- 例如：`SYC001.S1`、`SYC001.S1V1`

## 触发方式

手动触发：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/run_pipeline.py --mode manual
```

定时触发：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/run_pipeline.py --mode scheduled
```

新的“一行一条脚本”原创生产表使用独立兼容入口：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/run_pipeline.py \
  --source-kind original-batch --dry-run

python3 /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/run_pipeline.py \
  --source-kind original-batch
```

该入口读取 `原创视频生产脚本` 中勾选了 `进入生产` 的行并按 `视频形态（系统）` 分流。`15秒原创` 一行对应一个短视频运行管理任务，使用上游 `脚本ID` 幂等同步，优先把 `短视频提示词`（迁移期兼容旧名 `视频生成提示词`）写入运行表 `提示词`，禁止回退同步 `完整生产脚本`。`长视频` 行由隔离的 Plan C 执行器处理关键帧、H3、TTS 与合并，原短视频同步器会明确跳过，不能误写短视频运行管理表。任一分支成功后才取消该行勾选；失败保留勾选供重试。

原创生产脚本的参考图由运营选择决定：未勾选 `生成首帧（需勾选）` 时始终沿用原商品参考图，
即使上游视觉参考模式为 `PERSONA_PRODUCT_COMPOSITE_REQUIRED/PREFERRED` 也不能自动阻塞；
勾选后只有 `统一首帧（系统）` 已存在且状态为 `已就绪/缓存复用` 才替换参考图。正式同步会先处理本轮已选择记录的首帧；生成失败只阻塞该脚本，并保留 `进入生产` 勾选供后续重试。首帧就绪而同步失败时只补同步，不重新生图。

### OpenClaw 原创生产脚本命令路由

唯一入口：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/scripts/openclaw_original_batch_sync.py <check|sync> [白名单参数]
```

| 用户表达示例 | 执行方式 |
| --- | --- |
| “检查勾选进入生产的原创脚本” | `check --limit 20`，只预览，不写表 |
| “同步勾选的原创脚本到运行管理表” | `sync --limit 20` |
| “同步产品 1734482585843304442 的勾选原创脚本” | `sync --product-code 1734482585843304442` |
| “把 recXXXX 这条原创生产脚本送生产” | `sync --record-id recXXXX --limit 1` |

只接受 `record_id`（`rec`开头）、`product_code`（8–30位数字）和 1–20 的脚本处理上限。不得把自然语言拼进 shell，也不得处理未勾选`进入生产`的记录。同步进程本身有文件锁；重复运行按`脚本ID`更新/跳过，不能重复创建运行任务。

OpenClaw 自动巡检沿用现有**命令型**任务、每六小时执行一次，不新增高频全表轮询：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/scripts/openclaw_original_batch_sync.py sync --limit 20
```

它先处理已勾选的长视频旁路任务，再处理短视频首帧与运行表同步。首帧复用同步器已读取的记录快照，不为每条再扫描源表；首帧成功后同轮交接，不等下一个六小时周期。用户说“执行勾选脚本”时可立即运行同一 `sync` 入口，不改变定时频率。`check` 不调用模型、不写表；`sync` 对长视频保留原图像、H3 与 TTS 链路，短视频分支只生成明确勾选的首帧并同步运行表，不直接生成视频或发布。

## 常用参数

- `--dry-run`：只预览将要创建/更新/跳过的任务，不落表
- `--limit N`：限制处理的源记录数
- `--product-code XXX`：只处理指定产品编码
- `--record-id XXX`：只处理指定源表 record_id
- `--source-feishu-url`：覆盖默认源表 URL
- `--target-feishu-url`：覆盖默认目标表 URL

## 同步规则

- 只有勾选了 `是否可同步` 的源记录才会被处理
- 同步非空的 `脚本方向一 ~ 脚本方向四` 以及 `脚本1变体1 ~ 脚本4变体5`
- 每个非空脚本都会新增一条目标记录
- 同步成功后自动取消源表 `是否可同步`
- 如果源表存在 `同步状态` / `同步时间` 字段，会自动回写结果
- `SEEDING_ORGANIC` 生产脚本优先使用源表 `首帧图` 作为参考图；没有首帧时才回退商品图。
- `SEEDING_ORGANIC` 自动写入 `是否配口播=true / 口播状态=待处理`，并保留已审核口播原文；执行计划携带必选生活方式 BGM 床，不允许下游把种草任务静默做成无口播或无音乐版本。V7 按脚本序号轮换 BGM 选择档，并携带“开场 medium → 口播 low → 揭示点 medium”的能量曲线以及商业转场音禁用列表，不增加飞书字段。
- `短视频复刻` 必须同时具备复刻流水线的来源证据（`源复刻任务ID`，或 `脚本来源=短视频复刻` 且 `发布用途=短视频复刻`）；只有一个孤立的 `脚本类型=短视频复刻` 时按原创脚本处理，避免复制行残留值造成错分

## 字段要求

源表需要以下字段：

- `产品编码`
- `产品图片`
- `是否可同步`
- `脚本方向一`
- `脚本方向二`
- `脚本方向三`
- `脚本方向四`
- `脚本1变体1 ~ 脚本4变体5`（可选但支持）
- `同步状态`（推荐）
- `同步时间`（推荐）

目标表需要以下字段：

- `任务名`
- `提示词`
- `参考图`
- `脚本ID`
- `店铺ID`
- `脚本类型`

`脚本类型` 是业务流水线分类，与运行表的生成平台 `渠道`（即梦 / iMini）相互独立。历史记录可先预览再幂等回填：

```bash
python3 skills/script-run-manager-sync/backfill_script_types.py
python3 skills/script-run-manager-sync/backfill_script_types.py --apply
```

如需同时纠正源表里“只有复刻类型、没有复刻来源证据”的历史误标，并覆盖重算运行表分类：

```bash
python3 skills/script-run-manager-sync/backfill_script_types.py --overwrite --repair-source-mislabels
python3 skills/script-run-manager-sync/backfill_script_types.py --overwrite --repair-source-mislabels --apply
```

如果目标表缺少 `店铺ID`，同步脚本会自动创建文本字段并在新同步记录中写入；对已经存在但 `店铺ID` 为空的待处理记录，也会在再次命中时补写。

## 人工脚本库立即同步

人工脚本库支持两种触发方式：

1. 勾选 `同步`：由两小时定时任务统一同步。
2. 勾选 `立即同步`：OpenClaw 在一分钟内接收请求，自动勾选 `同步` 并只处理这一条记录；执行后会自动取消 `立即同步`，最终结果写回 `状态` / `时间`。

也可以在 OpenClaw 对话中明确说“立即同步人工脚本库”或“同步人工脚本 ID <record_id>”，并执行：

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/script-run-manager-sync/run_pipeline.py \
  --mode manual --source-kind manual
```
