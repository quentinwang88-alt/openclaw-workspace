---
name: jimeng-video-generator
description: 处理即梦视频自动化任务，尤其是“飞书多维表格里的待开始/待处理任务”闭环执行：提交到即梦、等待生成、在资产页按提示词认领结果、下载并回写飞书。
metadata:
  openclaw:
    emoji: "🎬"
    requires:
      bins: ["node"]
---

# 即梦视频生成器

默认把“执行表格里待开始的任务”理解为运行飞书直连流程，而不是只做单次生成。

## 多 OpenClaw 最小协同

当两台电脑读取同一张飞书表、但分别登录不同即梦账号时，当前 skill 使用一个最小字段来避免串单：

- 飞书新增字段：`执行归属`
- 字段值由真实执行方自动写入，格式是：
  - `<machineId>#<claimToken>`
  - 其中 `#` 前面的前缀就是机器标识

规则：

1. 提单前先自动认领，只认领 `执行归属` 为空或已经属于自己的记录。
2. 认领后会立刻回读确认；确认不是自己就放弃，不继续提交。
3. 提交前还会再确认一次归属，尽量避免两台机器对同一条记录重复提单。
4. 资产页下载只处理 `执行归属` 属于本机的记录，不会去对方即梦账号的资产页里认领对方提交的任务。
5. 如果你想把一条记录从 A 机器改给 B 机器，手动把飞书里的 `执行归属` 清空，再把状态改回 `待处理` 即可。

## 默认目标

当用户让你执行飞书表格里的待开始任务时，按这条主链路工作：

1. 读取 [feishu-direct.json](./feishu-direct.json)。
2. 连接已登录的调试 Chrome，并优先先跑 [preflight-feishu-direct.js](./preflight-feishu-direct.js) 做页面预检。
3. 优先通过 [run-feishu-direct.sh](./run-feishu-direct.sh) 启动主监控，不直接裸跑 `node`，这样能复用单实例锁和异常重试。
4. 持续监控生成队列。
5. 只在任务确实进入过生成队列、且当前生成队列归零后，才去资产页检查结果。
6. 在资产页必须进入视频详情页，读取“视频提示词”，优先按提示词中的 `【内容ID】` 与 trace 精确匹配；若缺少 `内容ID`，再回退到提示词指纹匹配后下载。
7. 下载成功后回写飞书附件、文件名、完成时间和结果状态。
8. 没有严重异常时继续自动处理下一条，不要频繁停下来问用户。

## 当前生产规则

- 一条多维表格记录只生成一次。
- 当前默认并发上限为 `8` 条未闭环任务；提交阶段会在每次点击生成后额外做一个短确认窗口，记录队列是否真实增长。
- “未闭环”指还处于 `submitted`、`rendering`、`downloaded` 或 `upload_failed`。
- 如果飞书记录显式勾选 `免参考图`，则允许在没有 `参考图` 附件时继续提交；未勾选时仍保持“无图即失败”的默认保护。
- 不能因为资产页里有旧视频就直接认领结果；默认会检查最多 `10` 个最新资产候选。
- 认领结果的最终依据是详情页提示词匹配，不是“最新一个视频”。
- 若提示词中存在 `【内容ID】`，则它是第一优先级主认领键；`prompt_hash / prompt_anchor` 仅作兜底。
- 多 inflight 下的结果认领按 trace 串行推进；即使同时有多条待认领，也不会整批跳过。
- 如果任务已观测进队、生成队列归零，但资产页连续多轮认领不到对应结果，会转入 `claim_failed` 低优先级补回写，不再阻塞后续新任务。
- 如果 trace 长时间停在 `submitting / submitted / rendering`，会被自动超时隔离，不再长期占用并发槽位。
- 如果页面报“你已取消生成，积分已返还”，但监测到生成队列增长，仍按成功提交处理，并把 trace 标记为已观测进队。
- 如果本地已经有按 `trace_id` 命名的下载文件，但 trace 还没到 `uploaded`，允许走恢复上传流程。
- 另有一个每 5 分钟运行一次的 `resume-only` 补回写 watcher，只负责续跑未闭环 trace，不新增提交。
- 只有出现严重异常时才中断新增任务并保留现场；普通等待、生成慢、轮询中都继续自动执行。

## 常用命令

主流程：

```bash
./skills/jimeng-video-generator/run-feishu-direct.sh /Users/likeu/.openclaw/workspace/skills/jimeng-video-generator/feishu-direct.json
```

人工立即补跑一轮（不受 `--scheduled` 时间窗限制，但仍受单实例锁保护）：

```bash
./skills/jimeng-video-generator/run-feishu-direct.sh /Users/likeu/.openclaw/workspace/skills/jimeng-video-generator/feishu-direct.json --oneshot
```

查询当前即梦主流程状态：

```bash
node skills/jimeng-video-generator/query-status.js
```

生成过去 24 小时的即梦提单/资产抓取日报预览（不发送）：

```bash
node skills/jimeng-video-generator/send-jimeng-daily-report.js --config skills/jimeng-video-generator/feishu-direct.json --dry-run
```

只看待处理记录，不真正提交：

```bash
node skills/jimeng-video-generator/feishu-direct-monitor.js --config skills/jimeng-video-generator/feishu-direct.json --dry-run
```

只做结果认领/补传：

```bash
node skills/jimeng-video-generator/result-uploader.js --config skills/jimeng-video-generator/feishu-direct.json
```

只续跑未闭环 trace，不新增提交：

```bash
PROCESS_LABEL=飞书结果补回写 ./skills/jimeng-video-generator/run-feishu-direct.sh /Users/likeu/.openclaw/workspace/skills/jimeng-video-generator/feishu-direct.json --resume-only
```

只处理单条 trace 的回写：

```bash
node skills/jimeng-video-generator/result-uploader.js --config skills/jimeng-video-generator/feishu-direct.json --trace-id <TRACE_ID>
```

页面预检：

```bash
node skills/jimeng-video-generator/preflight-feishu-direct.js --config skills/jimeng-video-generator/feishu-direct.json
```

## Prompt Package 视频片段提单

视频片段生成走 Prompt Package workbench，不再从飞书附件读取参考图。参考图来源顺序：

1. 记录里的 `参考图包ID`
2. `auto_mixcut` RDS 中 `product_id + market + sku_id` 对应的 active 图包
3. 如果没有 active 图包，应先由 `auto_mixcut` 从锚点卡商品主图导入 OSS 生成图包

运行入口：

```bash
./skills/jimeng-video-generator/run-segment-package.sh /Users/likeu3/.openclaw/workspace/skills/jimeng-video-generator/segment-package.json --one-shot
```

用户明确要求立即真实提单时，使用当前目录下的 worker，严格复用现有 pipeline，不要自己写临时提单脚本：

```bash
cd /Users/likeu3/.openclaw/workspace/skills/jimeng-video-generator
env IMINI_ALLOW_REAL_SUBMIT=1 node segment-package-worker.js --config segment-package.json --submit-only --limit=20 --no-ensure-schema
```

只处理单条 Prompt Package 记录时：

```bash
cd /Users/likeu3/.openclaw/workspace/skills/jimeng-video-generator
env IMINI_ALLOW_REAL_SUBMIT=1 node segment-package-worker.js --config segment-package.json --submit-only --record-id=<提示词包ID或record_id> --force-submit --no-ensure-schema
```

只预览可提单记录：

```bash
node skills/jimeng-video-generator/segment-package-worker.js --config skills/jimeng-video-generator/segment-package.json --submit-only --dry-run
```

只抓取并回流已生成资产，Prompt Package / imini 当前推荐直接使用 `result-uploader.js`，不要重新提单：

```bash
cd /Users/likeu3/.openclaw/workspace/skills/jimeng-video-generator
node result-uploader.js --config segment-package.json --channel imini --ignore-generating-count --limit 10
```

只抓取单条 trace：

```bash
cd /Users/likeu3/.openclaw/workspace/skills/jimeng-video-generator
node result-uploader.js --config segment-package.json --trace-id <TRACE_ID> --channel imini --ignore-generating-count --limit 1
```

夜间资产自动抓取 watcher：

```bash
cd /Users/likeu3/.openclaw/workspace/skills/jimeng-video-generator
./run-segment-package-asset-watch.sh segment-package.json --channel imini --limit 10 --first-delay-minutes 30 --interval-minutes 20 --max-empty 5
```

安装到 launchd 后，`com.likeu.jimeng-segment-asset-watch.plist` 会在每天 `22:00` 和 `00:00` 拉起 watcher；脚本内部有锁，同一时间只允许一个实例运行。

旧入口仍可用于 download-only，但 imini 定向回流优先用上面的 `result-uploader.js`：

```bash
node skills/jimeng-video-generator/segment-package-worker.js --config skills/jimeng-video-generator/segment-package.json --download-only
```

### Prompt Package / imini 当前规则

- 工作目录固定使用 `/Users/likeu3/.openclaw/workspace/skills/jimeng-video-generator`。
- 当前白天 `08:30-22:00` 不自动拉起提单任务，除非用户明确要求。夜间巡检可每 30 分钟检查一次，连续 4 次无待提单任务后停止。
- 参考图必须走 OSS 图包能力，不从飞书附件字段取图，也不要把参考图重新同步回飞书。
- OSS 参考图由 `auto_mixcut/scripts/resolve_reference_image_pack.py` 解析并下载到本地临时目录，worker 提交后会删除临时图。
- 即梦/imini 提单时必须保留表格模型设置；默认模型是 `Seedance 2.0`，不要擅自改成 `Seedance 2.0 Fast`。
- imini 当前走“图片转视频”：先生成首帧参考图，再上传首帧，随后填提示词、模型、比例、分辨率、时长。
- imini 首帧图不能出现可识别人脸。优先使用场景、环境、穿搭、动作开始前状态；商品锚点只做弱约束，避免跑款即可。
- 如果原提示词允许人物，可优先采用手机完整遮脸、镜前生活化构图；手机必须遮住整张脸，不得露出脸部皮肤轮廓。
- 首帧图生成后需要上传到飞书 `首帧参考图` 字段，便于人工检查。
- imini 提交前必须等待首帧图上传稳定、创建按钮可点击，再点击创建；提交前检必须确认模型、`9:16`、`480P`、`4s` 和提示词长度。
- OpenClaw 图片接口如果出现 SSRF 拦截，当前 worker 内应自动熔断，后续首帧直接走现有 `openai-image` fallback；不要改动 OpenClaw 调度入口。
- 提单成功后不要立刻全量扫资产。等用户确认视频已生成，或执行回流命令时，再按 `content_id / script_id` 去 imini 资产页匹配下载。
- 如果本地 trace 已经 `uploaded` 且有 `uploaded_file_token`，但飞书状态/附件不完整，优先用 `result-uploader.js` 补写，不要重复下载或重复提单。
- 遇到异常先查 `_state/submissions/<TRACE_ID>.json`、下载目录和现有代码日志，再介入；不要手写绕过 pipeline 的临时自动化。

### 夜间资产自动抓取机制

`segment-package-asset-watch.js` 只负责夜间资产回流，不新增提单：

1. `22:00` 后如果本夜有过 imini 提单任务，等最新一条提单后的 `30` 分钟自动执行资产扫描。
2. 如果 `22:00` 后没有提单任务，则 `00:00` 自动执行一次资产扫描，用于覆盖白天可能已经提交但还没回流的任务。
3. 扫描使用现有命令：`node result-uploader.js --config segment-package.json --channel imini --ignore-generating-count --limit 10`。
4. 每次扫描后，如果有 `uploaded` 或 `downloaded`，连续空扫计数清零；如果仍有未闭环 trace，`20` 分钟后继续扫。
5. 如果连续 `5` 次都没有抓到任何新资产，watcher 自动退出。
6. 如果当前监控窗口内没有未闭环 trace，watcher 自动退出。
7. `08:30-21:59` 属于白天手动窗口，watcher 不自动扫描资产页。

生成资产回流完成后，不要在即梦 skill 内重跑混剪整批；交回 `auto-mixcut-pipeline` 走补差额入口：

```bash
env AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun \
python3 -m auto_mixcut.cli top-up --product-id <商品ID> --count <目标数量>
```

该入口只按有效成片缺口补齐，不会默认重刷 10 条。

## 运行前检查

- Chrome 已用调试端口启动。
- 即梦账号已登录，且可打开视频生成页。
- 飞书配置完整，表格字段与 `feishu-direct.json` 一致。
- 如需双机同表运行，飞书里需要有 `执行归属` 字段。
- 表格里的新任务应是“待开始/待处理”，并且 `已提交次数 = 0`。

## 异常处理约定

- 如果只是生成尚未完成，继续等下一轮轮询。
- 如果结果未匹配到提示词，不要误绑旧视频；保持 trace 未闭环，等待下轮再查。
- 如果检测到多条待认领结果，不新增提交，先保持谨慎模式。
- 如果出现会污染数据的严重异常，停止主流程，保留 trace、下载文件和现场状态，等用户回来查看。

## 相关文件

- 主监控：[feishu-direct-monitor.js](./feishu-direct-monitor.js)
- 回写器：[result-uploader.js](./result-uploader.js)
- Prompt Package 视频片段 worker：[segment-package-worker.js](./segment-package-worker.js)
- Prompt Package 视频片段配置：[segment-package.json](./segment-package.json)
- 页面预检：[preflight-feishu-direct.js](./preflight-feishu-direct.js)
- 每日运行日报：[send-jimeng-daily-report.js](./send-jimeng-daily-report.js)
- 页面/下载基础能力：[folder-processor.js](./folder-processor.js)
- trace 状态管理：[trace-state.js](./trace-state.js)
