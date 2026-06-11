#!/bin/zsh
set -euo pipefail

WORKDIR="/Users/likeu3/.openclaw/workspace/skills/video-remake-lite"
LOG_DIR="$WORKDIR/output"
LOCK_DIR="/tmp/com.likeu3.video-remake-nurture-sync-to-script-table.lock"
STAMP="$(date '+%Y-%m-%d %H:%M:%S')"
TMP_LOG="$(mktemp -t video-remake-nurture-sync.XXXXXX.log)"

mkdir -p "$LOG_DIR"
cd "$WORKDIR"

cleanup() {
  rm -f "$TMP_LOG"
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  printf '\n[%s] video-remake nurture sync skipped: previous run still active\n' "$STAMP"
  exit 0
fi

if /usr/bin/python3 "$WORKDIR/sync_to_script_table.py" --mode scheduled --sync-profile nurture >"$TMP_LOG" 2>&1; then
  printf '\n[%s] video-remake nurture sync start\n' "$STAMP"
  cat "$TMP_LOG"
  printf '[%s] video-remake nurture sync end\n' "$(date '+%Y-%m-%d %H:%M:%S')"
  exit 0
fi

STATUS=$?
printf '\n[%s] video-remake nurture sync failed (exit=%s)\n' "$STAMP" "$STATUS" >&2
cat "$TMP_LOG" >&2
exit "$STATUS"
