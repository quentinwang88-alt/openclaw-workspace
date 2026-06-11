# Jimeng Video Generator Migration Guide

这个目录是“飞书多维表格 -> 即梦生成视频 -> 资产页认领下载 -> 回写飞书”的完整 skill 包。

## 目录说明

- `SKILL.md`: skill 说明
- `feishu-direct-monitor.js`: 主流程监控
- `result-uploader.js`: 资产页认领与下载回写
- `query-status.js`: 状态查询
- `run-feishu-direct.sh`: 单实例入口
- `feishu-direct.json`: 当前主配置
- `com.likeu.jimeng-feishu-submit.plist`: 提单线定时配置
- `com.likeu.jimeng-feishu-download.plist`: 下载线定时配置
- `start-debug-chrome.sh`: 调试 Chrome 启动脚本
- `node_modules/`: 已带依赖，可直接运行

## 推荐迁移方式

1. 把整个 `jimeng-video-generator` 文件夹复制到目标 OpenClaw 工作区：
   - 目标建议路径：`~/.openclaw/workspace/skills/jimeng-video-generator`
2. 根据目标机器路径，检查并修改以下文件中的绝对路径：
   - `com.likeu.jimeng-feishu-submit.plist`
   - `com.likeu.jimeng-feishu-download.plist`
   - `feishu-direct.json`
3. 确认目标机器已经：
   - 安装并登录 Chrome
   - 即梦账号已登录
   - Chrome 调试端口可用（默认 `9222`）
   - OpenClaw 的飞书通道配置完整（`~/.openclaw/openclaw.json` 中有 `channels.feishu.appId/appSecret`）
4. 启动调试 Chrome：
   - `bash skills/jimeng-video-generator/start-debug-chrome.sh`
5. 手动验证一轮：
   - 查询状态：`node skills/jimeng-video-generator/query-status.js`
   - 手动补跑：`./skills/jimeng-video-generator/run-feishu-direct.sh ~/.openclaw/workspace/skills/jimeng-video-generator/feishu-direct.json --oneshot`

## 当前调度规则

- 当前是双线 one-shot，不常驻：
  - submit-only
  - download-only
- submit `launchd` 每 `10` 分钟拉起一次，但真实执行由 `--scheduled` 控制：
  - 北京时间 `23:30 - 次日 09:00`：每 `10` 分钟
  - 其它时间：每 `30` 分钟
- download 当前是每 `2` 小时 `:05` 触发一次
- 资产页扫描当前是头部优先模式，`deep` 模式已关闭。

## 当前关键行为

- 并发上限：`1`
- 明确“平台高峰限流”允许自动重试，阈值 `10`
- 其它提交异常优先转 `阻塞`，等待人工处理
- `resume-only` 默认禁用
- 状态查询优先读飞书，飞书不可达时回退到本地状态口径
- 支持双机同表最小归属机制：
  - 飞书字段 `执行归属`
  - 谁先真实认领并执行，这条记录就归谁
  - 下载只处理本机归属任务，避免跨即梦账号认领

## 注意事项

- 这个包里包含 `node_modules`，体积较大，但迁移后可直接运行。
- `launchd` 配置中的绝对路径如果不改，目标机器上不会正常触发。
- 运行时状态默认写到：
  - `~/Desktop/temp/jimeng-feishu-runtime`
  如果目标机器不希望写到桌面，请同步修改 `feishu-direct.json` 的 `runtimeRoot`。
- 如果是双机同表运行，请确认：
  - 飞书表里已新增 `执行归属`
  - 两台机器的 `machineId` 不同
    - 默认用各自 `hostname`
    - 也可以通过 `feishu-direct.json` 的 `machineId` 或环境变量 `OPENCLAW_MACHINE_ID` 显式指定
- 如果想把一条记录从 A 机器转给 B 机器，请先把飞书中的 `执行归属` 清空，再把状态改回 `待处理`。
