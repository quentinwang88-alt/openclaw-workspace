# OpenClaw 双机开发工作区

这个仓库现在按“**双机开发、主电脑正式运行**”来整理：

- 主电脑：唯一正式运行机。继续保留正式 OpenClaw、正式网页登录态、正式飞书回写、正式定时任务。
- 第二台电脑：开发机。只负责看代码、改代码、调试、做局部验证，不默认接管正式任务。

## 这次整理后的原则

- 项目级内容尽量留在仓库里：`skills/`、`scripts/`、文档、项目规则、示例配置。
- 机器级内容继续留在本机：`~/.openclaw/`、浏览器登录态、LaunchAgents、crontab、SQLite 正式库、下载附件、缓存、日志。
- 敏感信息不进仓库：`.env`、Cookie、Token、App Secret、本机配置备份。

## 第二台电脑最短接入流程

1. 安装 `git` 和 `python3`
2. clone 这个仓库
3. 运行 `bash scripts/bootstrap.sh`
4. 打开本地 `.env`，按需要填写少量配置
5. 运行 `bash scripts/check_env.sh`
6. 需要时用 `bash scripts/dev_start.sh <命令>` 开始开发调试

详细步骤见 [SECOND_COMPUTER_SETUP.md](/Users/likeu3/.openclaw/workspace/SECOND_COMPUTER_SETUP.md)。

## 常用脚本

- `bash scripts/bootstrap.sh`
  初始化本地目录、生成本地 `.env`、尝试建立 `~/.openclaw/workspace` 兼容链接。
- `bash scripts/check_env.sh`
  用非程序员也能看懂的方式检查当前机器是否具备开发条件。
- `bash scripts/doctor.sh`
  输出“是否适合第二台电脑接入开发”、缺失项、机器专属项和风险项。
- `bash scripts/dev_start.sh python3 skills/creator-crm/run_pipeline.py --dry-run --limit 1`
  统一加载仓库 `.env` 后执行开发命令。

## 关键文档

- [SECOND_COMPUTER_SETUP.md](/Users/likeu3/.openclaw/workspace/SECOND_COMPUTER_SETUP.md)
  第二台电脑怎么接入。
- [MACHINE_SPECIFIC_ITEMS.md](/Users/likeu3/.openclaw/workspace/MACHINE_SPECIFIC_ITEMS.md)
  哪些内容必须继续留在主电脑本地。
- [AGENTS.md](/Users/likeu3/.openclaw/workspace/AGENTS.md)
  这个仓库的协作和操作规则。

## 当前已做的关键整理

- 加了仓库级路径/环境辅助层 `workspace_support.py`
- 把几个关键入口从“写死主电脑路径”改成“优先读仓库路径和 `.env`”
- 把 `creator-monitoring-openclaw` 默认指向仓库内的 `creator-monitoring-assistant`
- 补齐了 `.env.example`、接入文档、自检脚本、排障脚本
- 扩充了 `.gitignore`，把本机状态、数据库、缓存、日志、输出物、敏感配置排除在外

## 仍然要注意

- 这个仓库里还存在一批历史脚本和历史文档，里面仍有主电脑绝对路径或旧配置痕迹；它们不会影响这次整理后的最短开发接入路径，但在正式提交前仍建议再做一轮清理。
- 第二台电脑默认不要直接执行正式飞书回写、正式定时任务、正式网页控制。
