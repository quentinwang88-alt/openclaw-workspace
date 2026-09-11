#!/bin/zsh
set -euo pipefail

# launchd 默认 PATH 不含 /usr/local/bin，H3 网关需要 node；ffmpeg 在 ~/.local/bin。
export PATH="/Users/likeu3/.local/bin:/usr/local/bin:/opt/homebrew/bin:$PATH"

WORKDIR="/Users/likeu3/.openclaw/workspace"
RUNNER="$WORKDIR/skills/original-script-generator/scripts/run_feishu_longform_production_tasks.py"
LOG_DIR="$WORKDIR/skills/script-run-manager-sync/output"
STAMP="$(date '+%Y-%m-%d %H:%M:%S')"
TMP_LOG="$(mktemp -t original-longform.XXXXXX.log)"

mkdir -p "$LOG_DIR"
cleanup() {
  rm -f "$TMP_LOG"
}
trap cleanup EXIT

set +e
/usr/bin/python3 "$RUNNER" \
  --allow-real-submit \
  --allow-external-tts \
  --limit 1 \
  --max-wait-seconds 420 >"$TMP_LOG" 2>&1
STATUS=$?
set -e

if [ "$STATUS" -eq 0 ]; then
  if grep -q "待执行长视频生产/发布脚本: 0" "$TMP_LOG"; then
    exit 0
  fi
  printf '\n[%s] original longform patrol start\n' "$STAMP"
  sed -E 's/(api[_-]?key|token|authorization)[=:][^ ]+/\1=[REDACTED]/Ig' "$TMP_LOG"
  printf '[%s] original longform patrol end\n' "$(date '+%Y-%m-%d %H:%M:%S')"
  exit 0
fi

printf '\n[%s] original longform patrol failed (exit=%s)\n' "$STAMP" "$STATUS" >&2
sed -E 's/(api[_-]?key|token|authorization)[=:][^ ]+/\1=[REDACTED]/Ig' "$TMP_LOG" >&2
exit "$STATUS"
