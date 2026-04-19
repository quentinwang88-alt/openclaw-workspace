#!/usr/bin/env bash
# 输出更完整的接入诊断结果：可接入性、机器专属项、风险项、遗留问题。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"
OPENCLAW_HOME="${OPENCLAW_HOME:-$HOME/.openclaw}"

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

printf '=== OpenClaw 项目接入诊断 ===\n'
printf '仓库：%s\n' "$REPO_ROOT"
printf '机器角色：%s\n\n' "${THIS_MACHINE_ROLE:-未设置}"

bash "$REPO_ROOT/scripts/check_env.sh"

printf '\n=== Git 状态 ===\n'
if git -C "$REPO_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  printf '仓库状态：已初始化 Git\n'
else
  printf '仓库状态：未初始化 Git\n'
fi

TRACKED_COUNT="$(git -C "$REPO_ROOT" ls-files 2>/dev/null | wc -l | tr -d ' ')"
printf '已纳入 Git 跟踪的文件数：%s\n' "${TRACKED_COUNT:-0}"

printf '\n=== 第二台电脑接入判断 ===\n'
if [ -f "$REPO_ROOT/.env.example" ] && [ -f "$REPO_ROOT/scripts/bootstrap.sh" ] && [ -f "$REPO_ROOT/scripts/check_env.sh" ]; then
  printf '结论：仓库层已经整理到“可以低成本接入开发”的状态。\n'
else
  printf '结论：仓库层还没整理完整，建议先补齐脚本和文档。\n'
fi

printf '\n=== 主电脑专属项 ===\n'
[ -f "$OPENCLAW_HOME/openclaw.json" ] && printf -- '- 存在主电脑 OpenClaw 本机配置：%s\n' "$OPENCLAW_HOME/openclaw.json"
[ -d "$OPENCLAW_HOME/browser" ] && printf -- '- 存在浏览器态目录：%s\n' "$OPENCLAW_HOME/browser"
[ -d "$OPENCLAW_HOME/media/inbound" ] && printf -- '- 存在飞书附件接收目录：%s\n' "$OPENCLAW_HOME/media/inbound"
[ -d "$OPENCLAW_HOME/shared/data" ] && printf -- '- 存在共享数据库目录：%s\n' "$OPENCLAW_HOME/shared/data"
[ -f "$HOME/Library/LaunchAgents/ai.openclaw.gateway.plist" ] && printf -- '- 存在 OpenClaw gateway LaunchAgent\n'
[ -f "$HOME/Library/LaunchAgents/com.likeu3.bigseller-token-receiver.plist" ] && printf -- '- 存在 BigSeller token receiver LaunchAgent\n'
if crontab -l 2>/dev/null | grep -q 'skills/inventory-alert'; then
  printf -- '- crontab 中存在 inventory-alert 正式定时任务\n'
fi
[ -f "$OPENCLAW_HOME/cron/jobs.json" ] && printf -- '- OpenClaw 本机 cron jobs 已存在：%s\n' "$OPENCLAW_HOME/cron/jobs.json"

printf '\n=== 风险项 ===\n'
ABS_COUNT="$(rg -n '/Users/likeu3|~/.openclaw/workspace' "$REPO_ROOT" \
  --glob '!output/**' \
  --glob '!cache/**' \
  --glob '!logs/**' \
  --glob '!skills/**/output/**' \
  --glob '!skills/**/logs/**' \
  --glob '!.git/**' \
  | wc -l | tr -d ' ')"
printf -- '- 仍检测到 %s 处主电脑绝对路径引用（多数为历史脚本/历史文档）\n' "$ABS_COUNT"

SECRET_LITERAL_COUNT="$(rg -n 'ES8dbWo9FaXmaVs6jA7cgMURnQe|tblk1IHpVAvv2nWc|CHvtbRRPRa8R0GsiRaNcwq1onRf|tblJA4PesVL0eicw|b5ee8fce-c898-49cf-8098-ece21150e04b|bdf9dd60-ffed-42da-b33c-46885373e005|sk-sp-c0ca4d62044f4c8cbce081dee1c13a89' "$REPO_ROOT" \
  --glob '!output/**' \
  --glob '!cache/**' \
  --glob '!logs/**' \
  --glob '!skills/**/output/**' \
  --glob '!skills/**/logs/**' \
  --glob '!.git/**' \
  | wc -l | tr -d ' ')"
printf -- '- 仍检测到 %s 处旧脚本/历史文档里的硬编码标识或旧密钥痕迹，正式提交前建议再清一轮\n' "$SECRET_LITERAL_COUNT"

printf '\n=== 还需要人工确认的事 ===\n'
printf -- '- 第二台电脑是否需要安装 OpenClaw 并调试网页自动化\n'
printf -- '- 第二台电脑是否真的需要拿到正式数据库快照\n'
printf -- '- 哪些历史脚本值得继续保留，哪些应当归档或删除\n'

printf '\n=== 建议下一步 ===\n'
printf -- '1. 先在第二台电脑跑 bash scripts/bootstrap.sh\n'
printf -- '2. 再跑 bash scripts/check_env.sh\n'
printf -- '3. 用 dry-run 验证关键入口，不要直接跑正式任务\n'
