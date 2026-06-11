#!/bin/bash

set -euo pipefail

PORT="${1:-9222}"
PROFILE_DIR="${HOME}/.openclaw/jimeng-chrome-debug"

open -na "Google Chrome" --args \
  --remote-debugging-port="${PORT}" \
  --user-data-dir="${PROFILE_DIR}" \
  --disable-background-timer-throttling \
  --disable-renderer-backgrounding \
  --disable-backgrounding-occluded-windows
