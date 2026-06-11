#!/bin/zsh
set -euo pipefail

WORKDIR="/Users/likeu3/.openclaw/workspace"
LOG_DIR="$WORKDIR/skills/script-run-manager-sync/output"
LOCK_DIR="/tmp/com.likeu3.script-run-manager-sync.lock"
STAMP="$(date '+%Y-%m-%d %H:%M:%S')"
TMP_LOG="$(mktemp -t script-run-manager-sync.XXXXXX.log)"

mkdir -p "$LOG_DIR"
cd "$WORKDIR"

cleanup() {
  rm -f "$TMP_LOG"
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  printf '\n[%s] script-run-manager-sync skipped: previous run still active\n' "$STAMP"
  exit 0
fi

if /usr/bin/python3 "$WORKDIR/skills/script-run-manager-sync/run_pipeline.py" --mode scheduled >"$TMP_LOG" 2>&1; then
  if grep -q "待新增脚本数: 0" "$TMP_LOG"; then
    exit 0
  fi

  printf '\n[%s] script-run-manager-sync scheduled run start\n' "$STAMP"
  cat "$TMP_LOG"
  printf '[%s] script-run-manager-sync scheduled run end\n' "$(date '+%Y-%m-%d %H:%M:%S')"
  exit 0
fi

STATUS=$?
printf '\n[%s] script-run-manager-sync scheduled run failed (exit=%s)\n' "$STAMP" "$STATUS" >&2
cat "$TMP_LOG" >&2
exit "$STATUS"
