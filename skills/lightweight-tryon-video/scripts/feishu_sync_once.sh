#!/bin/zsh
set -euo pipefail

SKILL_DIR="${0:A:h:h}"
LOCK_DIR="${TMPDIR:-/tmp}/light-tryon-feishu-sync.lock"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  exit 0
fi
trap 'rmdir "$LOCK_DIR"' EXIT INT TERM

python3 "$SKILL_DIR/scripts/run_pipeline.py" feishu "$@"
