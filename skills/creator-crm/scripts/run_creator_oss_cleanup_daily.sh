#!/usr/bin/env bash
set -euo pipefail

SKILL_DIR="/Users/likeu3/.openclaw/workspace/skills/creator-crm"
LOG_DIR="$SKILL_DIR/logs"
mkdir -p "$LOG_DIR"

cd "$SKILL_DIR"

{
  echo "===== $(date '+%Y-%m-%d %H:%M:%S') creator-crm oss cleanup start ====="
  env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy \
    python3 "$SKILL_DIR/scripts/cleanup_creator_oss_assets.py" --limit "${CREATOR_CRM_CLEANUP_LIMIT:-500}"
  echo "===== $(date '+%Y-%m-%d %H:%M:%S') creator-crm oss cleanup done ====="
} >> "$LOG_DIR/creator_oss_cleanup.log" 2>&1
