#!/bin/zsh
set -euo pipefail

WORKDIR="/Users/likeu3/.openclaw/workspace/skills/short-video-auto-publisher"
LOG_DIR="$WORKDIR/output"
LOCK_DIR="/tmp/com.likeu3.short-video-auto-publish.lock"
STAMP="$(date '+%Y-%m-%d %H:%M:%S')"
TMP_LOG="$(mktemp -t short-video-auto-publish.XXXXXX.log)"

mkdir -p "$LOG_DIR"
cd "$WORKDIR"

cleanup() {
  rm -f "$TMP_LOG"
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  printf '\n[%s] short-video auto publish skipped: previous run still active\n' "$STAMP"
  exit 0
fi

set +e
/usr/bin/python3 "$WORKDIR/run_pipeline.py" run-all --publish-mode geelark >"$TMP_LOG" 2>&1
STATUS=$?
set -e

if [ "$STATUS" -eq 0 ]; then
  printf '\n[%s] short-video auto publish start\n' "$STAMP"
  cat "$TMP_LOG"
  printf '[%s] short-video auto publish end\n' "$(date '+%Y-%m-%d %H:%M:%S')"
  exit 0
fi

printf '\n[%s] short-video auto publish failed (exit=%s)\n' "$STAMP" "$STATUS" >&2
cat "$TMP_LOG" >&2
exit "$STATUS"
