#!/bin/zsh
set -euo pipefail

WORKDIR="/Users/likeu3/.openclaw/workspace/skills/short-video-auto-publisher"
LOG_DIR="$WORKDIR/output"
LOCK_DIR="/tmp/com.likeu3.short-video-product-publish-weekly-report.lock"
STAMP="$(date '+%Y-%m-%d %H:%M:%S')"
TMP_LOG="$(mktemp -t short-video-product-weekly.XXXXXX.log)"

mkdir -p "$LOG_DIR"
cd "$WORKDIR"

cleanup() {
  rm -f "$TMP_LOG"
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  printf '\n[%s] short-video product weekly report skipped: previous run still active\n' "$STAMP"
  exit 0
fi

if /usr/bin/python3 "$WORKDIR/run_pipeline.py" notify-product-publish-weekly --delivery openclaw-bot >"$TMP_LOG" 2>&1; then
  printf '\n[%s] short-video product weekly report start\n' "$STAMP"
  cat "$TMP_LOG"
  printf '[%s] short-video product weekly report end\n' "$(date '+%Y-%m-%d %H:%M:%S')"
  exit 0
fi

STATUS=$?
printf '\n[%s] short-video product weekly report failed (exit=%s)\n' "$STAMP" "$STATUS" >&2
cat "$TMP_LOG" >&2
exit "$STATUS"
