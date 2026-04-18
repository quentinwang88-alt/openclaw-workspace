#!/bin/zsh
set -euo pipefail

WORKDIR="/Users/likeu3/.openclaw/workspace"
LOG_DIR="$WORKDIR/skills/script-run-manager-sync/output"

mkdir -p "$LOG_DIR"
cd "$WORKDIR"

printf '\n[%s] script-run-manager-sync scheduled run start\n' "$(date '+%Y-%m-%d %H:%M:%S')"
/usr/bin/python3 "$WORKDIR/skills/script-run-manager-sync/run_pipeline.py" --mode scheduled
printf '[%s] script-run-manager-sync scheduled run end\n' "$(date '+%Y-%m-%d %H:%M:%S')"
