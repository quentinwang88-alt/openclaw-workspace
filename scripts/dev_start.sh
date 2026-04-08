#!/usr/bin/env bash
# 统一开发入口：加载仓库 .env，再执行你指定的开发命令。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

export OPENCLAW_WORKSPACE_ROOT="${OPENCLAW_WORKSPACE_ROOT:-$REPO_ROOT}"
export PYTHONPATH="$REPO_ROOT${PYTHONPATH:+:$PYTHONPATH}"

if [ $# -eq 0 ]; then
  cat <<EOF
用法：
  bash scripts/dev_start.sh <你要执行的命令>

示例：
  bash scripts/dev_start.sh python3 skills/creator-crm/run_pipeline.py --dry-run --limit 1
  bash scripts/dev_start.sh python3 skills/creator-monitoring-assistant/run_pipeline.py --help
  bash scripts/dev_start.sh bash scripts/check_env.sh

提醒：
  第二台电脑默认只做开发和调试，不默认承担正式生产任务。
EOF
  exit 0
fi

exec "$@"
