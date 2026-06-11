# 即梦多 OpenClaw 最新部署指南

这份说明对应当前最新版 `jimeng-video-generator` skill。

目标有两个：

- 把这套 skill 正确部署到另一台 Mac 上的 OpenClaw
- 保证多台 OpenClaw 共用一张飞书表时：
  - 不会把同一条数据重复提交到不同即梦账号
  - 不会去自己的资产页里找不属于自己提交的任务

## 这版 skill 的当前能力

- 提单线和下载线已经拆成两个独立进程
- 支持每天早上 9 点发送过去 24 小时的即梦提单/资产抓取日报
- `resume-only` 默认禁用，不会再每 2 分钟抢页面
- 两条线使用不同锁：
  - submit: `/tmp/jimeng-feishu-submit.lock`
  - download: `/tmp/jimeng-feishu-download.lock`
- 下载线默认头部优先扫描，`deep` 模式已关闭
- 下载线支持 `content_id / script_id / prompt_hash / prompt_anchor` 四层认领
- `timed_out` 中带真实进队信号的任务会继续尝试从资产页认领
- 上传参考图前会自动把 `.heic/.heif` 转成高质量 JPG 上传副本，不覆盖原图
- 提单时一旦检测到“积分不足 / 点数不足 / 余额不足”，会自动暂停后续提单，避免占住多个任务归属却无法生成

## 当前这台机器的真实部署信息

这台机器当前就是按下面这组配置在跑：

### 核心配置

- `machineId`: `龙虾妹`
- `runtimeRoot`: `~/Desktop/temp/jimeng-feishu-runtime`
- `cdpHost`: `127.0.0.1`
- `cdpPort`: `9222`
- `assetDeepScanEveryRuns`: `0`
- `claimStrategyOrder`: `["content_id", "script_id", "prompt_hash", "prompt_anchor"]`
- `submitPauseOnInsufficientCredits`: `true`

导出包根目录里的：

- `jimeng-video-generator/feishu-direct.json`

就是这台机器当前的真实参考配置。

注意：

- 它适合作为“当前机器配置参考”
- 不适合直接原样放到第二台机器运行
- 第二台机器至少要改：
  - `machineId`
  - `runtimeRoot`

### 本机 skill 目录

- `/Users/likeu/.openclaw/workspace/skills/jimeng-video-generator`

### 本机 submit 任务

- `launchd label`: `com.likeu.jimeng-feishu-submit`
- 执行参数：

```bash
./run-feishu-direct.sh /Users/likeu/.openclaw/workspace/skills/jimeng-video-generator/feishu-direct.json --oneshot --scheduled --submit-only
```

### 本机 download 任务

- `launchd label`: `com.likeu.jimeng-feishu-download`
- 执行参数：

```bash
./run-feishu-direct.sh /Users/likeu/.openclaw/workspace/skills/jimeng-video-generator/feishu-direct.json --oneshot --scheduled --download-only
```

### 本机日志位置

- submit:
  - `/Users/likeu/Library/Logs/jimeng-feishu-submit.log`
- download:
  - `/Users/likeu/Library/Logs/jimeng-feishu-download.log`
- daily report:
  - `/Users/likeu/Library/Logs/jimeng-feishu-daily-report.log`

## 两台机器必须满足的前提

1. 两台机器使用同一版 skill 代码
2. 两台机器登录不同的即梦账号
3. 两台机器共用同一张飞书表
4. 飞书表里已有字段 `执行归属`
5. 两台机器的 `machineId` 不同
6. 两台机器的 `runtimeRoot` 不同

## 第二台机器最少要改的配置

编辑：

- `feishu-direct.json`

至少确认这几项：

```json
{
  "machineId": "龙虾姐",
  "runtimeRoot": "~/Desktop/temp/jimeng-feishu-runtime-machine-b",
  "cdpPort": 9222,
  "assetDeepScanEveryRuns": 0,
  "claimStrategyOrder": ["content_id", "script_id", "prompt_hash", "prompt_anchor"],
  "submitPauseOnInsufficientCredits": true
}
```

说明：

- `machineId` 必须和当前机器不同
- `runtimeRoot` 必须是第二台机器自己的独立目录
- `assetDeepScanEveryRuns: 0` 表示不做 deep 扫描，只做头部优先扫描
- `submitPauseOnInsufficientCredits: true` 表示积分不足时暂停 submit 线，避免这一台机器继续占任务归属

## 推荐第二台机器怎么配

如果你没有别的特殊需要，建议第二台机器直接按这组思路配：

### 推荐命名

- 当前机器：
  - `machineId = 龙虾妹`
- 第二台机器：
  - `machineId = 龙虾姐`

### 推荐 runtimeRoot

- 当前机器：
  - `~/Desktop/temp/jimeng-feishu-runtime`
- 第二台机器：
  - `~/Desktop/temp/jimeng-feishu-runtime-machine-b`

不要两台机器共用同一个 `runtimeRoot`。

### 推荐第二台 `feishu-direct.json`

导出包里已经附带一份样例：

- `examples/feishu-direct.machine2.example.json`

你可以直接复制它覆盖第二台机器的 `feishu-direct.json`，再按第二台机器自己的实际路径改 `plist`。

## 为什么不会重复提交到不同即梦账号

这套多机流程防重复提交，靠的是“先认领、再回读、最后才提单”。

实际顺序是：

1. 两台机器都从飞书读取最新数据
2. 只有 `状态 = 待处理 / 部分提交` 且 `执行归属` 为空的任务，才会进入候选
3. 某一台机器先把：
   - `执行归属 = 自己的 machineId`
   - 同时状态写成 `处理中`
4. 程序立刻再回读一次飞书
5. 只有回读后仍然确认：
   - `执行归属` 还是自己
   才会真正进入即梦提单
6. 另一台机器回读发现归属不是自己，就会直接跳过

所以正常情况下：

- 同一条数据不会被两台机器同时提到两个不同即梦账号
- 哪台先拿到 `执行归属`，哪台才有资格继续提单

## 为什么不会去自己的资产页里找别台机器提交的任务

下载线现在不是全表盲扫，而是带归属过滤的。

实际规则是：

1. 下载线先构建待认领池
2. 只有满足下面任一条件的记录，才会由当前机器继续认领：
   - 飞书里的 `执行归属` 属于当前机器
   - 或本地 trace 的 `execution_machine_id` 属于当前机器
3. 然后才去资产页做匹配和下载

所以正常情况下：

- `龙虾妹` 只会去自己的资产页认 `执行归属 = 龙虾妹` 的任务
- `龙虾姐` 只会去自己的资产页认 `执行归属 = 龙虾姐` 的任务

这就是为什么：

- 不属于自己提交的任务，不会在自己的资产页里去找数据
- 也不会把另一台机器账号下的视频误回写到自己的任务上

## 路径需要同步修改的文件

如果第二台机器的用户名不是 `likeu`，或者 skill 放置路径不同，需要同步修改这些文件里的绝对路径：

- `feishu-direct.json`
- `com.likeu.jimeng-feishu-submit.plist`
- `com.likeu.jimeng-feishu-download.plist`

重点检查：

- `WorkingDirectory`
- `ProgramArguments`
- `StandardOutPath`
- `StandardErrorPath`

## 第二台机器部署步骤

1. 把整个 `jimeng-video-generator` 文件夹复制到：
   - `~/.openclaw/workspace/skills/jimeng-video-generator`
2. 按上面的要求修改：
   - `feishu-direct.json`
   - `com.likeu.jimeng-feishu-submit.plist`
   - `com.likeu.jimeng-feishu-download.plist`
   - 如果要启用日报，再确认 `com.likeu.jimeng-feishu-daily-report.plist`
3. 启动调试 Chrome：

```bash
bash ~/.openclaw/workspace/skills/jimeng-video-generator/start-debug-chrome.sh
```

4. 在 Chrome 里确认：
   - 已登录第二台机器自己的即梦账号
   - 能打开生成页
   - 能打开资产页

5. 建议手动打开两个标签页：
   - 一个停在即梦生成页
   - 一个停在即梦资产页

6. 手动验证配置：

```bash
node ~/.openclaw/workspace/skills/jimeng-video-generator/query-status.js
```

7. 手动验证提单线：

```bash
cd ~/.openclaw/workspace/skills/jimeng-video-generator
./run-feishu-direct.sh ~/.openclaw/workspace/skills/jimeng-video-generator/feishu-direct.json --oneshot --submit-only
```

8. 手动验证下载线：

```bash
cd ~/.openclaw/workspace/skills/jimeng-video-generator
./run-feishu-direct.sh ~/.openclaw/workspace/skills/jimeng-video-generator/feishu-direct.json --oneshot --download-only
```

9. 装载双线定时任务：

```bash
launchctl unload ~/Library/LaunchAgents/com.likeu.jimeng-feishu-direct.plist 2>/dev/null || true
launchctl unload ~/Library/LaunchAgents/com.likeu.jimeng-feishu-recovery.plist 2>/dev/null || true
cp ~/.openclaw/workspace/skills/jimeng-video-generator/com.likeu.jimeng-feishu-submit.plist ~/Library/LaunchAgents/
cp ~/.openclaw/workspace/skills/jimeng-video-generator/com.likeu.jimeng-feishu-download.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.likeu.jimeng-feishu-submit.plist
launchctl load ~/Library/LaunchAgents/com.likeu.jimeng-feishu-download.plist
```

10. 如果要启用每天 9 点的即梦日报，再追加：

```bash
cp ~/.openclaw/workspace/skills/jimeng-video-generator/com.likeu.jimeng-feishu-daily-report.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.likeu.jimeng-feishu-daily-report.plist
```

## 即梦日报说明

- 主脚本：
  - `send-jimeng-daily-report.js`
- 调度文件：
  - `com.likeu.jimeng-feishu-daily-report.plist`
- 默认统计窗口：
  - 过去 `24` 小时
- 默认通知配置：
  - `data/fastmoss-notify-config.json`
- 通知内容：
  - 发起了多少次提单尝试
  - 涉及多少条任务
  - 明确成功进队多少次
  - 最终失败多少次
  - 失败主因和数量
  - 成功写回多少条视频资产
  - 资产抓取失败多少条
  - 仍待认领多少条

## 第二台机器部署后，重点核对什么

至少核对下面这些配置没有串：

### 必须与当前机器相同的

- `appToken`
- `tableId`
- `viewId`
- 飞书字段映射
- `claimStrategyOrder`
- `assetDeepScanEveryRuns`
- `submitPauseOnInsufficientCredits`

### 必须与当前机器不同的

- `machineId`
- `runtimeRoot`
- 即梦账号登录态

### 允许相同的

- `cdpPort`

如果两台机器是两台不同 Mac，`cdpPort` 都用 `9222` 没问题。

## 当前调度规则

### 提单线

- `launchd` 每 10 分钟拉起一次
- 真实执行业务窗口：
  - 白天：每 `:00 / :30`
  - 夜间：每 `10` 分钟

### 下载线

- 每 2 小时 `:05` 触发一次
- 只要待认领池里有数据，就会真实进入资产页读取
- 当前不再做 `deep` 扫描，只做头部优先扫描

## 双机运行时必须注意

1. 两台机器的 `machineId` 不同
2. 两台机器登录不同即梦账号
3. 两台机器不要共用同一个 `runtimeRoot`
4. `执行归属` 不要手工乱改
5. 如果要转交任务给另一台机器：
   - 清空 `执行归属`
   - 把状态改回 `待处理`

## 建议的第二台机器标签页准备方式

为了让 submit/download 两条线更稳定，建议在第二台机器也手动先开好两个 tab：

- 一个停在即梦生成页
- 一个停在即梦资产页

然后分别手动跑一次：

- `submit-only`
- `download-only`

这样程序会把：

- submit 线绑定到自己的生成页 tab
- download 线绑定到自己的资产页 tab

避免两条线后面去抢同一个页面。

## 积分不足保护

当前最新版 skill 已经加入：

- submit 线遇到“积分不足 / 点数不足 / 余额不足 / 灵感值不足”时
- 会自动暂停后续提单
- 同时释放当前任务归属
- 下载线不受影响，仍可继续回写资产页已有结果

暂停状态会写入：

- `runtimeRoot/_state/submit-paused-insufficient-credits.json`

### 怎么恢复

人工确认积分恢复后，在目标机器执行：

```bash
cd ~/.openclaw/workspace/skills/jimeng-video-generator
npm run clear-submit-pause
```

或者：

```bash
node ~/.openclaw/workspace/skills/jimeng-video-generator/clear-submit-pause.js --config ~/.openclaw/workspace/skills/jimeng-video-generator/feishu-direct.json
```

恢复后，再等下一次自动 submit 轮次，或手动补跑一次 `submit-only`。

## 怎么判断另一台机器部署成功

至少看这几项：

1. `query-status.js` 能正常返回状态
2. `submit-only` 只在生成页动作，不会跑去资产页
3. `download-only` 只在资产页认领，不会新增提单
4. 飞书里的 `执行归属` 会写成第二台机器自己的 `machineId`
5. 下载回写只会认领属于第二台机器归属的记录
6. 两台机器同时开着时，同一条新任务只会有一台机器拿到 `执行归属`

## 推荐的首次验收方式

1. 两台机器都启动双线任务
2. 飞书里放 1 条全新的 `待处理`
3. 观察：
   - 哪台先写入 `执行归属`
   - 另一台是否跳过
   - 视频生成后是否由归属机器写回飞书

这套流程验过后，基本就能确认双机协同正常。
