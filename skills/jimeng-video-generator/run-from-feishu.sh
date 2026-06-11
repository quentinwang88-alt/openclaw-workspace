#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_PATH="${1:-$SCRIPT_DIR/feishu-source.json}"
OUTPUT_DIR="${2:-$HOME/Desktop/jimeng}"

echo "📥 从飞书表格同步任务..."
node "$SCRIPT_DIR/feishu-sync-to-local.js" --config "$CONFIG_PATH" --output "$OUTPUT_DIR"

echo ""
echo "🎬 开始即梦智能监测..."
node "$SCRIPT_DIR/smart-monitor.js" "$OUTPUT_DIR"
