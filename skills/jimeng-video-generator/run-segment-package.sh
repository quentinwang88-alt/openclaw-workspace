#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_PATH="$SCRIPT_DIR/segment-package.json"
if [[ $# -gt 0 && "${1:-}" != --* ]]; then
  CONFIG_PATH="$1"
  shift
fi

cd "$SCRIPT_DIR"
exec node "$SCRIPT_DIR/segment-package-worker.js" --config "$CONFIG_PATH" "$@"
