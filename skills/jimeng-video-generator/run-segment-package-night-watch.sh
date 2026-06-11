#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_PATH="${1:-$SCRIPT_DIR/segment-package.json}"
LOCK_DIR="${SEGMENT_NIGHT_LOCK_DIR:-/tmp/jimeng-segment-night-watch.lock}"
PID_FILE="$LOCK_DIR/pid"
EMPTY_LIMIT="${SEGMENT_NIGHT_EMPTY_LIMIT:-4}"
INTERVAL_SECONDS="${SEGMENT_NIGHT_INTERVAL_SECONDS:-1800}"
DOWNLOAD_LIMIT="${SEGMENT_NIGHT_DOWNLOAD_LIMIT:-3}"
SUBMIT_LIMIT="${SEGMENT_NIGHT_SUBMIT_LIMIT:-1}"

cd "$SCRIPT_DIR"

minute_of_day() {
  local hour minute
  hour="$(date '+%H')"
  minute="$(date '+%M')"
  echo $((10#$hour * 60 + 10#$minute))
}

in_night_window() {
  local now
  now="$(minute_of_day)"
  # Night automation is allowed from 22:00 through 08:29.
  # Daytime 08:30-21:59 is manual-only.
  [[ "$now" -lt 510 || "$now" -ge 1320 ]]
}

cleanup_lock() {
  if [[ -d "$LOCK_DIR" && -f "$PID_FILE" ]]; then
    local owner
    owner="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ "$owner" == "$$" ]]; then
      rm -rf "$LOCK_DIR"
    fi
  fi
}

acquire_lock() {
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    printf '%s\n' "$$" > "$PID_FILE"
    return 0
  fi

  if [[ -f "$PID_FILE" ]]; then
    local existing
    existing="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$existing" ]] && kill -0 "$existing" 2>/dev/null; then
      echo "[$(date '+%F %T')] night watcher already running (PID=$existing), skip"
      return 1
    fi
  fi

  echo "[$(date '+%F %T')] removing stale night watcher lock"
  rm -rf "$LOCK_DIR"
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    printf '%s\n' "$$" > "$PID_FILE"
    return 0
  fi

  echo "[$(date '+%F %T')] failed to acquire night watcher lock"
  return 1
}

trap cleanup_lock EXIT
handle_stop() {
  echo "[$(date '+%F %T')] received stop signal"
  exit 0
}
trap handle_stop INT TERM

if ! in_night_window; then
  echo "[$(date '+%F %T')] daytime manual-only window, skip scheduled segment package watcher"
  exit 0
fi

if ! acquire_lock; then
  exit 0
fi

empty_count=0

while in_night_window; do
  echo "[$(date '+%F %T')] segment package night check starts (empty_count=$empty_count/$EMPTY_LIMIT)"

  echo "[$(date '+%F %T')] run download-only recovery first"
  if ! node "$SCRIPT_DIR/segment-package-worker.js" --config "$CONFIG_PATH" --download-only --limit="$DOWNLOAD_LIMIT"; then
    echo "[$(date '+%F %T')] download-only recovery failed, continue to submit dry-run"
  fi

  dry_log="$(mktemp -t jimeng-segment-dry.XXXXXX.log)"
  if node "$SCRIPT_DIR/segment-package-worker.js" --config "$CONFIG_PATH" --submit-only --dry-run --limit="$SUBMIT_LIMIT" > "$dry_log" 2>&1; then
    if grep -Fq '"recordId"' "$dry_log"; then
      empty_count=0
      echo "[$(date '+%F %T')] pending submit task found, run one scheduled submit"
      cat "$dry_log"
      if ! IMINI_ALLOW_REAL_SUBMIT=1 node "$SCRIPT_DIR/segment-package-worker.js" --config "$CONFIG_PATH" --submit-only --one-shot --limit="$SUBMIT_LIMIT"; then
        echo "[$(date '+%F %T')] scheduled submit failed; next loop will retry if still pending"
      fi
    else
      empty_count=$((empty_count + 1))
      echo "[$(date '+%F %T')] no pending submit task ($empty_count/$EMPTY_LIMIT)"
      cat "$dry_log"
      if [[ "$empty_count" -ge "$EMPTY_LIMIT" ]]; then
        echo "[$(date '+%F %T')] no pending submit task for $EMPTY_LIMIT consecutive checks, stop night watcher"
        rm -f "$dry_log"
        exit 0
      fi
    fi
  else
    echo "[$(date '+%F %T')] submit dry-run failed; not counting as empty"
    cat "$dry_log"
    empty_count=0
  fi
  rm -f "$dry_log"

  if ! in_night_window; then
    break
  fi

  echo "[$(date '+%F %T')] sleep ${INTERVAL_SECONDS}s before next night check"
  sleep "$INTERVAL_SECONDS"
done

echo "[$(date '+%F %T')] reached daytime manual-only window, stop night watcher"
