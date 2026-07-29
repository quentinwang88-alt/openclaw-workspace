#!/bin/zsh
set -euo pipefail

SKILL_DIR="${0:A:h:h}"
LOCK_DIR="${TMPDIR:-/tmp}/light-tryon-feishu-sync.lock"
LOG_DIR="$SKILL_DIR/var"

# launchd opens the current log before invoking this wrapper. Renaming an
# oversized log here lets the next scheduled run start a fresh file while
# retaining one previous generation for diagnosis.
for log_file in "$LOG_DIR/run-manager-sync.log" "$LOG_DIR/run-manager-sync.error.log"; do
  if [[ -f "$log_file" ]] && (( $(wc -c < "$log_file") > 10485760 )); then
    mv -f "$log_file" "$log_file.1"
  fi
done

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  lock_pid=""
  if [[ -r "$LOCK_DIR/pid" ]]; then
    read -r lock_pid < "$LOCK_DIR/pid" || true
  fi
  if [[ "$lock_pid" == <-> ]] && kill -0 "$lock_pid" 2>/dev/null; then
    exit 0
  fi
  # A killed or crashed sync can leave the directory behind forever. Only
  # remove the expected PID marker, then reclaim the empty stale lock.
  rm -f "$LOCK_DIR/pid" 2>/dev/null || exit 0
  rmdir "$LOCK_DIR" 2>/dev/null || exit 0
  mkdir "$LOCK_DIR" 2>/dev/null || exit 0
fi
print -r -- "$$" > "$LOCK_DIR/pid"

cleanup_lock() {
  rm -f "$LOCK_DIR/pid" 2>/dev/null || true
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup_lock EXIT INT TERM HUP

python3 "$SKILL_DIR/scripts/run_pipeline.py" feishu "$@"
