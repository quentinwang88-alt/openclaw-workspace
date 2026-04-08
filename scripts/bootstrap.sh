#!/usr/bin/env bash
# 初始化本机的开发接入环境。
# 这个脚本只做安全、可重复执行的准备动作，不会覆盖主电脑现有正式配置。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OPENCLAW_HOME="${OPENCLAW_HOME:-$HOME/.openclaw}"
OPENCLAW_WORKSPACE_LINK="$OPENCLAW_HOME/workspace"

say() {
  printf '%s\n' "${1:-}"
}

mkdir -p "$OPENCLAW_HOME/shared/data"
mkdir -p "$OPENCLAW_HOME/media/inbound"
mkdir -p "$OPENCLAW_HOME/logs"
mkdir -p "$REPO_ROOT/logs"
mkdir -p "$REPO_ROOT/output"
mkdir -p "$REPO_ROOT/cache"
mkdir -p "$REPO_ROOT/screenshots"
mkdir -p "$REPO_ROOT/skills/creator-crm/output"
mkdir -p "$REPO_ROOT/skills/creator-crm/logs"
mkdir -p "$REPO_ROOT/skills/creator-crm/temp"
mkdir -p "$REPO_ROOT/skills/hair-style-review/output"
mkdir -p "$REPO_ROOT/skills/product-candidate-enricher/output"

if [ ! -f "$REPO_ROOT/.env" ]; then
  cp "$REPO_ROOT/.env.example" "$REPO_ROOT/.env"
  say "已生成本地 .env，请按需要填写。"
else
  say "本地 .env 已存在，保留原样。"
fi

REPO_REAL="$(cd "$REPO_ROOT" && pwd -P)"

if [ -L "$OPENCLAW_WORKSPACE_LINK" ] || [ -d "$OPENCLAW_WORKSPACE_LINK" ]; then
  if LINK_REAL="$(cd "$OPENCLAW_WORKSPACE_LINK" 2>/dev/null && pwd -P)"; then
    if [ "$LINK_REAL" = "$REPO_REAL" ]; then
      say "~/.openclaw/workspace 已指向当前仓库。"
    else
      say "提示：~/.openclaw/workspace 已存在且不指向当前仓库，已保守跳过。"
      say "      如需兼容旧脚本，请人工确认后再调整。"
    fi
  else
    say "提示：检测到 ~/.openclaw/workspace，但无法解析真实路径，已跳过。"
  fi
else
  mkdir -p "$OPENCLAW_HOME"
  ln -s "$REPO_ROOT" "$OPENCLAW_WORKSPACE_LINK"
  say "已创建兼容链接：~/.openclaw/workspace -> 当前仓库"
fi

say
say "开始环境自检："
bash "$REPO_ROOT/scripts/check_env.sh"
