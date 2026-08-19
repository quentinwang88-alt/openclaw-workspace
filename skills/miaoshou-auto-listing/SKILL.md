---
name: miaoshou-auto-listing
description: Safely inspect or execute the Miaoshou automatic listing queue from the Feishu artificial-listing table or confirmed selection workbench. Use for requests such as “执行人工上品表待上架商品”, “跑一下自动上架”, “检查还有多少待上架”, or syncing confirmed products into the listing queue. Enforces the dedicated port 9333, one global lock, serial publishing, bounded batches, no automatic retry, and TikTok product-ID verification.
---

# 妙手自动上架

只使用项目内的确定性执行器：

```text
/Users/likeu3/.openclaw/workspace/miaoshou-auto-listing
```

## 判断授权

- 用户只说“检查、看看、还有多少条”时，只读检查，绝不领取或发布。
- 用户明确说“执行、跑一下、上架这些商品”时，视为授权发布表中已经标记为`待执行`的记录。
- 不把`异常`记录自动改回`待执行`，不猜测性重发。

## 人工上品表

只读检查：

```bash
cd /Users/likeu3/.openclaw/workspace/miaoshou-auto-listing
.venv/bin/python scripts/run_openclaw_listing_batch.py --check-only
```

执行一批：

```bash
cd /Users/likeu3/.openclaw/workspace/miaoshou-auto-listing
.venv/bin/python -u scripts/run_openclaw_listing_batch.py
```

默认最多执行 20 条。用户明确指定更小批量时添加`--max-items N`。不要设置超过 50。

只核验已经提交、尚未取得产品 ID 的记录（绝不点击发布）：

```bash
cd /Users/likeu3/.openclaw/workspace/miaoshou-auto-listing
.venv/bin/python -u scripts/run_openclaw_listing_batch.py --verify-pending-only
```

## 选品工作台联动

用户明确要求把已确认商品同步并上架时运行：

```bash
cd /Users/likeu3/.openclaw/workspace/miaoshou-auto-listing
.venv/bin/python -u scripts/run_selection_listing_bridge.py --sync --execute-ready
```

只要求同步、不发布时去掉`--execute-ready`。

## 强制安全规则

- 只连接妙手专用 Chrome 端口`9333`；永不使用 NeoBund 的`9222`。
- 只通过上述两个批量入口执行，不直接并行调用`--feishu-once`或`--feishu-record`。
- 保持串行提交。发布后先进行 90 秒短核验；若暂未取得 TikTok 产品 ID，状态写为`待核验`并继续下一条，整批末尾再做一次短核验。`待核验`只能执行 VERIFY，绝不能再次点击发布。
- 缺尺码图等明确的发布前资料异常可记录后跳过；登录、店铺、页面结构、发布或未知异常必须立即停止整批。
- 服装尺码图按以下顺序处理：飞书附件/已配置来源 → 妙手详情图 → 1688 源页面的结构化规格表。1688`包装信息/商品件重尺`只要明确包含`尺码`列及 SKU 尺码值（包括`均码+建议体重`，或按颜色/SKU列出的尺码与长宽高、体积、重量），可截图后复用妙手图片翻译并上传到尺码表；只有包装重量尺寸、没有尺码映射的仍应报资料异常。不得凭空生成尺寸数值。
- 飞书`尺码图`附件视为人工确认后的目标市场语言终稿，下载成功后直接上传，不再调用妙手图片翻译；从中文详情图、配置 URL 或 1688 结构化表自动提取的来源仍需走妙手翻译。
- 人工尺码图上传前必须与当前全部 SKU 尺码做覆盖核对；缺任一尺码写为`待补资料`，不进入发布。
- 1688 二维码短链必须先解析出明确 offer ID 并规范化为详情页链接；无法解析写为`待补资料`。
- 批次启动后必须先检查 9333 页面与妙手登录态；检查失败时不领取飞书记录。
- 若提示登录失效，让用户在 9333 专用窗口重新登录；不要尝试绕过验证码。
- 若提示另一个批次正在运行，不启动第二批，只告知用户已有任务在执行。
- “点击发布”不等于成功；只有飞书状态成功且存在 TikTok 产品 ID 才汇报成功。

## 汇报

读取命令末尾的批次摘要，并汇报：处理数、成功数、待核验数、待补资料数、真实异常数、剩余待执行数、每条 TikTok 产品 ID，以及首个真实异常原因。批次摘要同时保存在项目的`runtime/batches/`目录。
