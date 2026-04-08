#!/usr/bin/env bash
# 用尽量容易理解的方式检查当前机器是否具备“接入开发”的基本条件。

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

PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0

ok() {
  PASS_COUNT=$((PASS_COUNT + 1))
  printf '[通过] %s\n' "$1"
}

warn() {
  WARN_COUNT=$((WARN_COUNT + 1))
  printf '[提醒] %s\n' "$1"
}

fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  printf '[缺失] %s\n' "$1"
}

check_cmd() {
  local name="$1"
  if command -v "$name" >/dev/null 2>&1; then
    ok "已安装命令：$name"
  else
    fail "缺少命令：$name"
  fi
}

check_file() {
  local path="$1"
  local label="$2"
  if [ -e "$path" ]; then
    ok "$label 已存在"
  else
    fail "$label 不存在"
  fi
}

check_dir() {
  local path="$1"
  local label="$2"
  if [ -d "$path" ]; then
    ok "$label 已存在"
  else
    fail "$label 不存在"
  fi
}

check_optional_var() {
  local name="$1"
  local label="$2"
  local value="${!name:-}"
  if [ -n "$value" ] && [[ "$value" != YOUR_* ]] && [[ "$value" != *your-name* ]]; then
    ok "$label 已填写"
  else
    warn "$label 还没填写；只有运行相关流程时才需要"
  fi
}

printf '=== OpenClaw 项目环境检查 ===\n'
printf '仓库目录：%s\n\n' "$REPO_ROOT"

check_cmd git
check_cmd python3

if command -v openclaw >/dev/null 2>&1; then
  ok "已安装 openclaw"
else
  warn "未检测到 openclaw；如果第二台电脑只看代码和改代码，可以先不装"
fi

check_file "$REPO_ROOT/README.md" "README.md"
check_file "$REPO_ROOT/AGENTS.md" "AGENTS.md"
check_file "$REPO_ROOT/.env.example" ".env.example"
check_file "$REPO_ROOT/SECOND_COMPUTER_SETUP.md" "SECOND_COMPUTER_SETUP.md"
check_file "$REPO_ROOT/MACHINE_SPECIFIC_ITEMS.md" "MACHINE_SPECIFIC_ITEMS.md"
check_file "$REPO_ROOT/scripts/bootstrap.sh" "scripts/bootstrap.sh"
check_file "$REPO_ROOT/scripts/dev_start.sh" "scripts/dev_start.sh"
check_file "$REPO_ROOT/scripts/check_env.sh" "scripts/check_env.sh"
check_file "$REPO_ROOT/scripts/doctor.sh" "scripts/doctor.sh"
check_file "$REPO_ROOT/.codex/config.toml" ".codex/config.toml"

check_dir "$REPO_ROOT/skills" "skills 目录"
check_dir "$OPENCLAW_HOME/shared/data" "~/.openclaw/shared/data"
check_dir "$OPENCLAW_HOME/media/inbound" "~/.openclaw/media/inbound"

if [ -f "$ENV_FILE" ]; then
  ok "本地 .env 已存在"
else
  warn "本地 .env 不存在；可先运行 bash scripts/bootstrap.sh 自动生成"
fi

check_optional_var FEISHU_APP_TOKEN "Creator CRM 飞书 App Token"
check_optional_var FEISHU_TABLE_ID "Creator CRM 飞书 Table ID"
check_optional_var LLM_API_KEY "Creator CRM 主 LLM Key"
check_optional_var CATEGORY_API_KEY "Creator CRM 分类打标 LLM Key"
check_optional_var DATABASE_URL "Creator Monitoring 数据库地址"
check_optional_var CREATOR_MONITORING_FEISHU_APP_TOKEN "Creator Monitoring 飞书 App Token"
check_optional_var CREATOR_MONITORING_FEISHU_TABLE_ID "Creator Monitoring 飞书 Table ID"

if [ -f "$REPO_ROOT/skills/creator-crm/config/api_config.json" ]; then
  ok "skills/creator-crm/config/api_config.json 已存在"
else
  warn "缺少 skills/creator-crm/config/api_config.json；如果要跑 creator-crm，需要参考 example 补一个本机配置"
fi

if [ -f "$REPO_ROOT/skills/inventory-query/config/api_config.json" ]; then
  ok "skills/inventory-query/config/api_config.json 已存在"
else
  warn "缺少 skills/inventory-query/config/api_config.json；如果不调试库存查询，可以先不处理"
fi

if [ -f "$REPO_ROOT/skills/inventory-alert/config/alert_config.json" ]; then
  ok "skills/inventory-alert/config/alert_config.json 已存在"
else
  warn "缺少 skills/inventory-alert/config/alert_config.json；如果不调试库存预警，可以先不处理"
fi

printf '\n=== 检查结果 ===\n'
printf '通过：%s\n' "$PASS_COUNT"
printf '提醒：%s\n' "$WARN_COUNT"
printf '缺失：%s\n' "$FAIL_COUNT"

if [ "$FAIL_COUNT" -eq 0 ]; then
  printf '结论：这台机器已经具备“接入开发”的基础条件。\n'
else
  printf '结论：还缺少少量基础项，先补齐缺失项再继续。\n'
fi
