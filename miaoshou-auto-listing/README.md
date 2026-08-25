# 妙手自动上架 V1

这是一个确定性的 Playwright 页面执行器，当前覆盖：

```text
发布记录防重复 → 1688 采集 → 认领到 TikTok → 绑定目标店铺
→ 打开编辑页 → SKU 定价 → 逐 SKU 仓库库存 → SKU/包裹重量体积
→ 妙手文字翻译 → 尺码图识别/翻译 → 详情图翻译 → 主图翻译
→ 发布前总预检 → 保存发布 → 发布记录终态验证
```

V1 已接入精简的飞书人工任务表；仍不包含 AI Listing、订单或库存同步。图片翻译直接复用妙手，并显式取消默认的“不翻译商品上的文字”；图片视觉清理尚未进入默认工作流。服装缺少可信尺码图时返回异常，不生成或猜测尺码数据。

## 安全边界

- 默认是 dry-run，执行完 `PREFLIGHT` 后停止，不点击“保存并发布”。如果任务包含 `source_url`，dry-run 仍会执行采集、认领和页面编辑，因此应使用测试商品。
- 只有显式添加 `--execute` 才允许真实发布。
- 默认先查询发布成功记录；同一 `货源 ID + 店铺` 已发布时返回 `ALREADY_PUBLISHED`。只有任务显式设置 `allow_republish=true` 才允许有意重复发布。
- `--execute` 会先生成结构化预检快照；任一 SKU 的价格、库存、重量、尺码图或页面校验不通过，都不会打开发布确认弹窗。
- “发布任务已提交”不等于成功。执行器会交叉轮询发布记录和店铺产品五种状态，只有取得 TikTok 产品 ID 才返回成功。
- 批量发布采用 90 秒短核验；暂未取得产品 ID 时写为`待核验`并继续下一条，之后只能只读核验，绝不重复发布。
- 点击“确认发布”后会立即写入 `runtime/submissions/` 幂等回执；唯一键为“货源 ID + 店铺 + 国家”。即使飞书记录 ID 变化、程序重启或成功弹窗超时，也只能继续只读核验，不能重复发布。
- 采集认领后会写入 `runtime/acquisitions/`，并锁定“货源 + 店铺 + 妙手行标识”；线性任务重试会跳过采集、认领，也不会改选同货源的另一行。
- 页面加载、弹窗残留或虚拟 SKU 表暂时未就绪时，只重试当前步骤，不重跑整条任务；业务性异常（如确实缺少尺码图）不盲目重试。
- 人工中断会先把已领取的飞书任务写成终态；若发布请求已经提交，结果会明确标记“禁止自动重发、需继续验证”。
- 图片翻译必须出现“翻译结果预览”并完成“确认并保存”；详情图还必须保存批处理结果。任一环节失败都会在发布前中止。
- 图片不做 OCR 前置筛选，继续使用妙手全量翻译。自动化会复用已经打开的图片处理、翻译设置或结果预览弹窗，不在遮挡状态下重复点击入口。
- 商品描述字符数读取失败或超过 10000 时一律禁止发布；默认不会自动截断商品描述。
- 自动类目必须连续两次得到相同识别结果才进入后续步骤，避免店铺绑定后的异步加载造成误判。
- 不处理验证码或登录失效；检测到登录页返回 `LOGIN_EXPIRED`。
- 不使用截图找按钮，不使用坐标点击。
- 示例店铺名是占位符，未改成妙手真实展示名时会返回 `SHOP_BIND_FAILED`。

## 安装与验证

```bash
cd /Users/likeu3/.openclaw/workspace/miaoshou-auto-listing
python3 -m pip install -e .
python3 -m playwright install chromium
python3 -m unittest discover -s tests -v
```

本机已经安装 Python 依赖时，可先不安装项目，直接运行：

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 首次配置

1. 修改 `config/app.yaml` 的 `base_url` 与 `product_list_path`。
2. 修改 `config/shops.yaml` 中店铺的真实妙手展示名。
3. 复核 `config/pricing.yaml`，示例公式不是生产定价授权。
4. 启动妙手专用 Chrome（固定端口 `9333`，不与 NeoBund 的 `9222` 混用）：

```bash
./scripts/start_miaoshou_chrome.sh
```

首次在这个专用窗口登录妙手即可。该窗口只保留妙手页面；执行器连接前会校验端口内的页面，如果误连到 NeoBund 或其他浏览器实例会立即停止。

5. 根据真实页面校准 `config/selectors/*.yaml`。虚拟 SKU 表和弹层语义封装统一放在 `handlers/editor_dom.py`，不得在各 handler 中使用坐标或 `nth-child`。

## 运行

### OpenClaw 统一批量入口（推荐）

OpenClaw 不应直接循环调用底层 CLI，而应使用统一批量执行器。它会获取
`runtime/miaoshou_listing.lock` 全局锁、检查并启动妙手专用 Chrome 9333、
逐条串行领取飞书`待执行`记录。缺尺码图等明确发生在发布前的资料异常会
记录后跳过；登录、店铺、页面结构或发布异常会立即停止。批次汇总写入
`runtime/batches/`：

```bash
# 只读检查，不领取、不发布
.venv/bin/python scripts/run_openclaw_listing_batch.py --check-only

# 真实执行，默认单批最多 20 条
.venv/bin/python -u scripts/run_openclaw_listing_batch.py

# 明确限制本批数量
.venv/bin/python -u scripts/run_openclaw_listing_batch.py --max-items 5

# 只核验待核验记录，绝不发布
.venv/bin/python -u scripts/run_openclaw_listing_batch.py --verify-pending-only
```

`--max-items`只允许 1–50。批量执行器与选品工作台桥接器共用同一把锁，
因此两个 OpenClaw 会话不能同时操作妙手。异常不会自动改回`待执行`；
只有确认未进入发布阶段的资料异常允许继续下一条，系统性异常立即停止。

OpenClaw 自然语言入口由
`/Users/likeu3/.openclaw/workspace/skills/miaoshou-auto-listing/SKILL.md`
约束。只有“执行、跑一下、上架”等明确用语才授权发布；“检查、看看”只允许
运行`--check-only`。

飞书中的`qr.1688.com`商品短链接会在领取时解析为标准详情链接并回写，允许
原链接后带空格和货号备注；若短链接实际指向活动/推荐页且没有 offer ID，则
作为资料异常保留，不猜测商品。

复制并修改任务样例：

```bash
cp config/task.example.json runtime-task.json
```

安全 dry-run：

```bash
PYTHONPATH=src python3 -m miaoshou_auto_listing --task runtime-task.json
```

确认店铺、市场、商品 ID、定价规则都正确后，才运行真实发布：

```bash
PYTHONPATH=src python3 -m miaoshou_auto_listing --task runtime-task.json --execute
```

从飞书任务表领取并线性执行一条“待执行”任务：

```bash
PYTHONPATH=src python3 -m miaoshou_auto_listing --feishu-once
```

也可以按飞书记录 ID 精确执行一条：

```bash
PYTHONPATH=src python3 -m miaoshou_auto_listing \
  --feishu-record recXXXXXXXX
```

飞书只是输入和结果层，不参与浏览器工作流编排。每条任务在同一妙手标签页中按“采集 → 认领 → 编辑 → 翻译 → 预检 → 发布 → 验证”线性完成。状态为“待执行 → 执行中 → 成功/待核验/待补资料/异常”。不可逆发布回执独立保存在 `runtime/submissions/`，即使暂时核验不到结果也不会猜测性重发。批次摘要使用`confirmed_success`、`submitted_pending`、`needs_input`、`true_failed`区分确认成功、已提交待核验、资料问题和真实失败。

只读检查表格连接和待执行数量（不会领取或发布）：

```bash
PYTHONPATH=src python3 -m miaoshou_auto_listing --feishu-check
```

飞书模式只读取这 9 个字段：`采购链接`、`目标店铺`、`定价方式`、`定价`、`每SKU库存`、`尺码图`、`执行状态`、`执行结果`、`TikTok产品ID`。`尺码图`是可选附件：人工上传附件视为目标市场语言终稿，并在上传前核对其是否覆盖当前全部 SKU 尺码；缺少尺码或无法识别的链接写为`待补资料`。店铺别名与妙手真实店铺 ID、国家的对应关系维护在 `config/shops.yaml`。

`定价方式` 留空或选择 `固定售价` 时，`定价` 仍表示所有 SKU 的统一人民币售价，完全复用原有批量定价流程。选择 `采购价倍数` 时，`定价` 表示倍数；执行器使用妙手原生公式 `来源原价 × 倍数 + 0 - 0`，四舍五入并保留两位小数。倍数必须大于 `0` 且不超过 `100`，发布前会逐 SKU 核对采购价、预期售价与实际售价。历史字段 `定价（按采购价倍数）` 不参与执行，可从视图隐藏。

## 选品工作台联动

选品工作台仍是用户入口，现有人工上架表继续作为唯一执行队列。工作台勾选`是否测品`后进入找货；人工核对并补充`推荐货源链接`、采购价、`目标店铺`、`定价方式`、`定价`、`每SKU库存`后，直接勾选`确认上架`。`确认上架`是最终人工授权，桥接器不再要求把`找货状态`手工改成`已找到`；它会直接校验货源详情链接、采购价、店铺国家和定价参数，校验通过后只创建一条`待执行`任务，并把任务 ID、执行状态、结果和 TikTok 产品 ID 回写工作台。

默认只读检查：

```bash
.venv/bin/python scripts/run_selection_listing_bridge.py
```

创建缺失字段（幂等）并同步到队列，但不执行妙手发布：

```bash
.venv/bin/python scripts/run_selection_listing_bridge.py --ensure-fields --sync
```

只有明确需要无人值守发布时才可加`--execute-ready`。该开关按新建任务的精确飞书记录 ID 串行调用`--feishu-record`，不会使用“领取第一条待执行”的模糊入口。它与人工上品表批量执行器共用`runtime/miaoshou_listing.lock`，禁止同时运行：

```bash
.venv/bin/python scripts/run_selection_listing_bridge.py --sync --execute-ready
```

桥接唯一键由“选品记录 ID + 1688 offer ID + 店铺别名”生成。重复运行只做结果对账；待执行任务的参数变化会原位更新，执行中、成功或异常任务不会被覆盖。工作台刷新也会保留已经关联上架任务的记录。

任务中与幂等相关的字段：

```json
{
  "source_url": "https://detail.1688.com/offer/123.html",
  "allow_republish": false
}
```

自动断点续跑：

- 编辑阶段失败时，执行器会先保留故障截图，再尝试点击妙手“保存修改”。
- 优先尝试保存妙手草稿；若妙手因资料未完整而拒绝保存，则保留现有编辑页。下一次执行只有在页面仍能同时匹配同一货源 ID 和同一店铺时才从失败步骤恢复。
- 草稿未保存且原编辑页已关闭、刷新或商品不匹配时，会安全地从头执行。
- 检查点写入 `runtime/checkpoints/<task_id>.json`；任务输入变化、草稿未保存或检查点损坏时会安全地从头执行。发布回执不随检查点清理，跨任务仍会阻止相同货源、店铺和国家重复提交。
- 图片翻译结果独立保存在 `runtime/image_translations/`，记录原图、结果图 URL 指纹和目标语言。只有妙手草稿确认保存后才允许跨轮复用；更换尺码图或目标市场会自动使旧缓存失效。

仍可人工覆盖恢复步骤：

```bash
PYTHONPATH=src python3 -m miaoshou_auto_listing \
  --task runtime-task.json \
  --resume-from SET_WAREHOUSE \
  --execute
```

恢复时执行器会按目标步骤自动恢复“定位商品 → 店铺绑定 → 打开编辑页”等前置条件。显式 `--resume-from` 优先于本地检查点。若发布任务已经提交，应从 `VERIFY` 恢复，禁止从 `PUBLISH` 猜测性重提。每个步骤写入 `runtime/state/<task_id>.jsonl`；失败时截图和元数据写入 `runtime/artifacts/<task_id>/`。

## 发布前预检

成功结果中的 `preflight` 至少包含：

```text
店铺、市场、泰语标题、SKU 数量
每个 SKU 的 CNY 价格、平台换算价、库存、重量
包裹重量与长宽高、尺码图数量、页面校验错误
```

价格会在 `PREFLIGHT` 中重新写入，避免草稿重开后因实时汇率反算产生 CNY 99.97 / 99.83 等漂移。

## 真实页面联调门槛

在进入 10 商品验收前，先用一个不会造成业务损失的测试商品完成以下校准：

- 商品 ID 查询结果必须唯一，不能依赖列表第一行。
- SKU 表的采购价与售价输入框必须在同一行内解析。
- 店铺、仓库和国家必须按 ID/value 或精确文本选中。
- 翻译必须等待明确成功/失败信号。
- 发布必须在发布记录中同时确认“发布成功”和平台商品 ID。
- 同一货源再次执行必须返回 `ALREADY_PUBLISHED`，不能进入采集或编辑页。
- 人工制造一次选择器失效，确认返回 `PAGE_STRUCTURE_CHANGED` 并生成截图。
- 人工制造一次登录失效，确认返回 `LOGIN_EXPIRED`，且程序不会尝试自动登录。

本轮单品真实联调已通过，详见 `TEST_REPORT_2026-08-11_LIKEU_975683523984.md`。进入生产前仍需完成 10 商品验收；通过标准为至少 9 个成功、零重复发布，并且每个失败都带 `current_step`、固定 `error_code`、页面 URL 和截图。
