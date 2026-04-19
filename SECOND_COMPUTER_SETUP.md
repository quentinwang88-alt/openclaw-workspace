# 第二台电脑接入说明

这份说明是给“**不想折腾环境、只想尽快继续开发同一个项目**”的人准备的。

## 目标

第二台电脑只做这些事：

- 看代码
- 改代码
- 跑局部验证
- 写文档
- 调试脚本

第二台电脑**默认不接管**这些事：

- 正式定时任务
- 正式飞书回写
- 正式网页登录态
- 正式批量执行
- 正式附件下载/上传

## 第一步：装基础工具

至少安装：

- `git`
- `python3`

可选但推荐：

- OpenClaw
- Codex / ChatGPT Desktop

## 第二步：拉取仓库

把仓库 clone 到你顺手的位置，例如：

```bash
git clone <你的仓库地址> openclaw-workspace
cd openclaw-workspace
```

## 第三步：跑初始化脚本

```bash
bash scripts/bootstrap.sh
```

这个脚本会做几件安全的事：

- 创建本机需要但不进仓库的目录
- 如果本地还没有 `.env`，就从 `.env.example` 复制一份
- 如果 `~/.openclaw/workspace` 还不存在，就建立一个兼容链接，减少旧脚本因为绝对路径报错

如果你本机已经有别的 `~/.openclaw/workspace`，脚本会提示，但不会强行覆盖。

## 第四步：填写本地 `.env`

打开仓库根目录的 `.env`。

最常见只需要关注这些：

- `THIS_MACHINE_ROLE=dev`
- `FEISHU_APP_TOKEN`
- `FEISHU_TABLE_ID`
- `LLM_API_KEY`
- `CATEGORY_API_KEY`

如果你还要调试 creator-monitoring，再补：

- `DATABASE_URL`
- `CREATOR_MONITORING_FEISHU_APP_TOKEN`
- `CREATOR_MONITORING_FEISHU_TABLE_ID`

如果你只是先读代码、改代码，不急着跑正式链路，这些可以先留空。

## 第五步：补本机私有配置

这几类文件不要从仓库里找，要在本机自己放：

1. `skills/creator-crm/config/api_config.json`
   说明：Kalodata Cookie 等本机私有配置
   做法：参考同目录下 `api_config.example.json`

2. `skills/inventory-query/config/api_config.json`
   说明：BigSeller token 等本机私有配置
   做法：参考同目录下 `api_config.example.json`

3. `skills/inventory-alert/config/alert_config.json`
   说明：Webhook / 飞书配置
   做法：参考同目录下 `alert_config.example.json`

如果你在第二台电脑只是做代码开发，不跑这些流程，可以先不补。

## 第六步：做自检

```bash
bash scripts/check_env.sh
```

想看更完整的排查结果：

```bash
bash scripts/doctor.sh
```

## 第七步：开始开发

推荐统一用这个方式启动开发命令：

```bash
bash scripts/dev_start.sh python3 skills/creator-crm/run_pipeline.py --dry-run --limit 1
```

这样做的好处：

- 会自动加载仓库 `.env`
- 会自动把仓库根目录加入 `PYTHONPATH`
- 对老脚本里残留的路径依赖更友好

## 常见问题

### 1. 为什么第二台电脑不直接跑正式任务？

因为正式运行还依赖这些主电脑专属内容：

- 浏览器登录态
- OpenClaw 本机配置
- 定时任务
- LaunchAgents
- SQLite 正式库
- 飞书附件收件目录

这些内容并不适合简单地直接跨机器同步。

### 2. 为什么还保留 `~/.openclaw/workspace` 兼容路径？

因为当前项目里仍有一批旧脚本写死了这个路径。  
这次整理已经把关键入口改成了仓库优先，但为了不破坏主电脑现有流程，也为了让第二台电脑更低门槛接入，保留这个兼容层最稳。

### 3. 如果 `bootstrap.sh` 提示不能创建兼容链接怎么办？

说明你的第二台电脑已经有自己的 `~/.openclaw/workspace`。

这时你有两个安全选项：

- 继续保留现状，只用 `bash scripts/dev_start.sh ...` 开发
- 人工确认后，自己决定是否调整原有 OpenClaw 工作区结构

## 需要人工确认的情况

- 第二台电脑是否也要安装 OpenClaw
- 第二台电脑是否需要调试网页自动化
- 第二台电脑是否需要读取主电脑的正式数据库快照
- 哪些本机私有 Token 真的有必要同步到第二台电脑
