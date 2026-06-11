---
name: auto-mixcut-pipeline
description: |
  AI/真实素材混剪成片流水线 skill。用户说“跑混剪全流程”“让 OpenClaw 守护跑某个商品”“AI补素材回流后补齐成片”“补差额生成混剪视频”“不要重刷10条，只补缺口”时优先使用。
  负责调用 auto_mixcut 正式 CLI/RDS/OSS 流程，监控阻断性异常，按有效成片数补差额，不手工拼临时脚本，不默认全量重刷。
---

# Auto Mixcut Pipeline

## 适用场景

使用这个 skill 处理：

- 指定商品 ID，让 OpenClaw 自己跑混剪流程
- AI 补素材已经回流，需要继续补齐目标成片
- 检查某个商品为什么没有产满目标数量
- 守护式无人跑，只在阻断性问题时介入
- 同步混剪成片到飞书成片质检表

不用于：

- 即梦提单本身，使用 `jimeng-video-generator`
- 成片发布排期，使用 `short-video-auto-publisher`
- 原创短视频脚本生成，使用 `original-script-generator`

## 工作目录

所有命令默认在：

```bash
/Users/likeu3/.openclaw/workspace/auto_mixcut
```

生产环境必须带 RDS 和 Aliyun OSS，**包括只读查询/状态检查**：

```bash
env AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun ...
```

不要在生产流程里漏掉这两个环境变量，否则会误跑到本地 SQLite 或本地 OSS，表现为“商品不在系统里 / 无任务 / 无素材”。看到这种提示时，第一反应不是向用户要商品名、市场、类目，而是检查命令是否漏了正式 env 或是否没在 auto_mixcut 目录执行。

**硬规则：**

- 禁止用默认本地 SQLite 作为生产预检依据。
- 禁止用“先看这个产品在不在系统里”作为阻断理由，除非已经在正式 env 下查过 RDS，并且正式 guard 的飞书 bootstrap 也失败。
- 新商品如果只在飞书 `商品内容任务表` 里，正式 guard 会自动从飞书读取 `商品名称/市场/类目/店铺/目标生成数量` 并创建 RDS 任务；不要手工向用户重复索要这些字段。

视觉类 LLM 调用（素材打标、锚点匹配、一致性、终检）必须走 `Doubao Seed 2.0 Pro`：

- provider: `volcano_ark_coding`
- model: `doubao-seed-2-0-pro-260215`
- 不要兜底到 `chatgpt.com/backend-api/codex`；如果火山方舟不可用，应明确失败并提示检查 `VOLCANO_ARK_API_KEY`

运行 auto_mixcut 前如果环境里存在代理变量，优先清掉，避免所有 HTTPS 请求被错误转到本地服务：

```bash
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy \
  AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun ...
```

macOS Big Sur / 系统 Python 运行时如果出现 Aliyun OSS `SSLEOFError` 或 `LibreSSL 2.8.3` 相关提示，先确认：

```bash
python3 -c "import urllib3; print(urllib3.__version__)"
```

应为 `1.26.x`。不要因为这个错误绕过参考图包或改成不上传 OSS；参考图包仍是 AI 补素材的正式依赖。

## 默认守护入口

用户给商品 ID 并要求“自己跑”“守护跑”“补齐目标”时，优先调用守护器，而不是陪跑式逐步拆命令：

```bash
cd /Users/likeu3/.openclaw/workspace/auto_mixcut
env AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun \
  AUTO_MIXCUT_GUARD_SUBMIT_AI_PACKAGES=1 \
  AUTO_MIXCUT_GUARD_PROCESS_AI_RETURNS=1 \
  python3 scripts/run_mixcut_guard_loop.py \
    --product-id <商品ID> \
    --target <目标数量> \
    --max-passes 12 \
    --round-timeout 900 \
    --detach
```

如果用户没有显式给目标数量，可以不传 `--target`，守护器会读取 RDS/商品内容任务表当前任务目标。

`run_mixcut_guard_loop.py --detach` 是默认入口。不要默认用 `run_mixcut_guard.py` 单轮入口给用户陪跑；只有排查单步问题时才用单轮入口。

守护器会写回商品内容任务表：

- `守护状态`: `RUNNING / DONE / WAITING_AI_RETURN / READY_TO_CONTINUE / BLOCKED / ERROR`
- `下一步动作`: `WAIT_AI_SEGMENT_RETURN / RUN_GUARD_AGAIN / NEED_MATERIAL_UPLOAD / ...`
- `守护异常`
- `最近批次ID`

OpenClaw 正常情况下只需要看守护器返回和这些字段；只有 `BLOCKED / ERROR` 时再介入定位。

守护器语义：

- RDS 无任务时，先从飞书 `商品内容任务表` bootstrap；只有正式 env + 飞书也查不到或目标数量为空时，才提示用户补字段
- 目标已满时直接 `DONE`，不上传同步、不补跑下游、不新建批次
- 已有切片但存在缺失字段时，只做增量补处理，不调用全量 `batch`
- 只有完全没有切片的首次入池商品，才允许由守护器触发完整编排
- 同商品存在 `planning` 批次时，不再新建第二批，等待当前批次完成或先 abort 冗余批次

以下是底层口径，只有守护器异常或需要人工复核时才手动使用。

先判断该商品是否已经在 auto_mixcut 正式任务池里。

如果 RDS 已有 `products/content_tasks`，默认跑补差额入口：

```bash
env AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun \
python3 -m auto_mixcut.cli top-up --product-id <商品ID> --count <目标数量> --max-rounds 2
```

如果用户没有指定目标数量，先读取商品内容任务表/RDS 的目标数量；不要擅自改目标。

`top-up` 会按正式流程执行：

1. `check` 重新计算素材容量
2. 如果 readiness 返回 `AI补素材`，同步 Prompt Package 工作台
3. `render-plan` 生成缺口计划，默认 `fill_gap_only=True`
4. `render` 只渲染本轮缺口
5. `quality`
6. `final-video-qc`
7. `sync-feishu`

`top-up` 默认最多做 2 轮轻补：如果本轮产出里有 `draft_only`，且目标仍未满、素材池仍有容量，会继续补下一小轮；如果目标已满，会直接返回 `stop_reason=target_already_filled`，不做容量重估、不新建 batch。

返回结果必须优先看：

- `stop_reason`
- `final.target_variant_count`
- `final.effective_outputs`
- `final.target_remaining_variant_count`
- `final.material_pool_extra_capacity`
- `batch_ids`
- 每轮 `rounds[].planned_count / effective_count / draft_only_count`

## 从零完整流程

如果 `top-up` 返回 `TASK_NOT_FOUND`，说明不是补差额场景，而是新商品还没有进入 auto_mixcut 正式任务池。此时不要反复调用 `top-up`，也不要手工拼临时脚本；应切到“从零完整流程”。

从零完整流程需要先拿到商品基础信息：

```text
商品ID、商品名称、市场、类目、目标成片数量
```

如果用户已经给了这些信息，先创建任务：

```bash
env AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun \
python3 -m auto_mixcut.cli create-task \
  --product-id <商品ID> \
  --name "<商品名称>" \
  --market <市场> \
  --category <类目> \
  --count <目标数量>
```

然后处理素材入口：

- 如果素材已放在“商品素材上传表”，先跑正式导入：

```bash
env AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun \
python3 scripts/process_asset_uploads.py --product-id <商品ID>
```

- 如果是本地素材文件，使用正式上传入口逐个入库：

```bash
env AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun \
python3 -m auto_mixcut.cli upload \
  --product-id <商品ID> \
  --file <本地视频或图片路径> \
  --source-type self_shot \
  --source-trust-level high \
  --product-binding-type exact_sku
```

最后仍然回到守护入口，让守护器判断是首次完整编排还是补差额：

```bash
env AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun \
python3 scripts/run_mixcut_guard.py --product-id <商品ID> --target <目标数量>
```

底层 `batch` 会按正式流程走锚点、probe、水印、切片、抽帧、打标、有效角色、readiness、render-plan、render、quality、final-video-qc、同步飞书。它只应由守护器在“完全没有切片的首次入池商品”场景触发；不要对已有切片/已有成片的商品手动调用 `batch`。

如果用户只给了商品 ID，且 RDS/飞书任务池里查不到商品名称、市场、类目，就停下来要这四个字段；不要猜市场/类目创建任务，避免后续发错商品或错店铺。

## 补差额口径

有效成片只包括：

```text
passed
passed_with_warning
needs_review
publish_ready
```

不计入有效成片：

```text
draft_only
human_quality_status = rejected
render_status != rendered
```

补差额必须按：

```text
本轮应补数量 = target - 已有效成片数
```

例子：

- 目标 10，已有有效 3，只补 7
- 目标 10，已有有效 4，只补 6
- 目标 10，已有有效 7，只补 3
- 目标 10，已有有效 10，直接跳过，不新建渲染批次

飞书成片质检表可以展示 `draft_only`，但它只用于人工验收留痕，不算实际生成数量。

## 严禁默认全量重刷

不要默认使用：

```bash
python3 -m auto_mixcut.cli render-plan --product-id <商品ID> --full-refresh
```

只有用户明确要求“全部重剪/全部废掉重来/全量刷新”，才允许使用：

```bash
env AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun \
python3 -m auto_mixcut.cli render-plan --product-id <商品ID> --count <目标数量> --full-refresh --confirm-full-refresh
```

如果用户说“补缺口”“补齐目标”“AI素材回来了继续跑”，必须使用 `top-up` 或默认 `render-plan` 补差额模式，不能全量重刷。

## AI 补素材回流后的处理

AI 补素材是异步支线。素材回流后，不要自动重跑完整 10 条。

正确动作：

1. 用 `top-up` 重新计算素材容量
2. 只生成缺口 render plan
3. 只渲染缺口成片
4. 只同步新增成片到飞书
5. 保留已有成片、人工废弃状态、切片使用记录

如果素材仍不足，任务表/日志应保留类似说明：

```text
补差额: 目标=10; 已有效=7; 本轮计划=3; 跳过=0
```

如果同时存在 AI 素材缺口，也应保留 `AI补素材: ...` 说明，便于继续异步提单。

## 守护式运行原则

OpenClaw 守护跑时：

- 默认只监控阻断性异常，不边跑边临时改流程
- 不手工拼临时 Python 脚本替代正式 CLI
- 不绕过 RDS/OSS/飞书正式链路
- 不因为目标是 10 就强行凑满 10；素材容量不足时宁可少出，并写清楚原因
- 不因为目标数量已配置就重复同一个首镜硬凑；如果 `first_slot_candidates < target`，先按唯一首镜容量出片，并触发 `AI补素材: hero首镜N`
- 不覆盖已有成片和人工质检状态
- 不把 `draft_only` 当作有效成片

可以介入的阻断性问题：

- RDS/OSS/飞书连接失败
- 锚点缺失导致流程无法继续
- final QC 或外部模型调用长时间卡住
- render plan 生成 0 条且没有明确缺口说明
- 同一商品重复创建全量批次风险
- 多条成片首镜完全相同，且 readiness 已提示 `first slot uniqueness limited`

## 常用检查命令

查看当前产品有效成片与任务状态：

```bash
env AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun \
python3 -m auto_mixcut.cli check --product-id <商品ID>
```

只生成补差额计划，不渲染：

```bash
env AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun \
python3 -m auto_mixcut.cli render-plan --product-id <商品ID> --count <目标数量>
```

补差额完整执行：

```bash
env AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun \
python3 -m auto_mixcut.cli top-up --product-id <商品ID> --count <目标数量> --max-rounds 2
```

目标已满时可用同一命令做轻量验收；预期返回 `batch_id=""`、`batch_ids=[]`、`stop_reason=target_already_filled`。

同步某批次成片到飞书：

```bash
env AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun \
python3 -m auto_mixcut.cli sync-feishu --product-id <商品ID> --batch-id <批次ID>
```

拉取飞书人工质检结果：

```bash
env AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun \
python3 -m auto_mixcut.cli pull-output-qc --product-id <商品ID>
```

如果只是验证 render plan，验证后必须 abort 掉未渲染的测试批次：

```bash
env AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun \
python3 -m auto_mixcut.cli abort-batch --batch-id <批次ID> --reason validation_no_render
```

## 交接给其它 skill

即梦提单：

- 使用 `jimeng-video-generator`
- 参考图包由 `auto_mixcut/scripts/resolve_reference_image_pack.py` 从 RDS active 图包解析并下载
- 即梦回流后，再回到本 skill 跑 `top-up`

自动发布：

- 使用 `short-video-auto-publisher`
- 混剪成片需要带商品 ID、店铺 ID、素材/输出 ID
- 只发布人工确认可发布的混剪视频，不能发布 `draft_only` 或人工 `rejected`
