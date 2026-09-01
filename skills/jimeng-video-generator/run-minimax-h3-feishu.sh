#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOCK_DIR="/tmp/openclaw-minimax-h3-feishu.lock"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "MiniMax H3 worker 已在运行，本轮跳过"
  exit 0
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

node "$SCRIPT_DIR/minimax-h3-feishu-worker.js" \
  --config "$SCRIPT_DIR/feishu-direct.json" \
  "$@"
