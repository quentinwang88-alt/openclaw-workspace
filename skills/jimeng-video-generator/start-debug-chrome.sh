#!/bin/bash

set -euo pipefail

PORT="${1:-9222}"
PROFILE_DIR="${HOME}/.openclaw/jimeng-chrome-debug"
EXTRA_ARGS=()
if [[ -n "${CHROME_PROXY_SERVER:-}" ]]; then
  EXTRA_ARGS+=(--proxy-server="${CHROME_PROXY_SERVER}")
fi

CHROME_ARGS=(
  --remote-debugging-port="${PORT}"
  --user-data-dir="${PROFILE_DIR}"
  --disable-background-timer-throttling
  --disable-renderer-backgrounding
  --disable-backgrounding-occluded-windows
)
if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
  CHROME_ARGS+=("${EXTRA_ARGS[@]}")
fi

open -na "Google Chrome" --args "${CHROME_ARGS[@]}"
