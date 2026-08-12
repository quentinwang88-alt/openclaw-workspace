#!/bin/zsh
set -eu

MIAOSHOU_PORT="9333"
MIAOSHOU_PROFILE="/Users/likeu3/.openclaw/workspace/miaoshou-auto-listing/runtime/miaoshou_browser_profile"
MIAOSHOU_URL="https://erp.91miaoshou.com/tiktok/collect_box/items"

if curl -fsS "http://127.0.0.1:${MIAOSHOU_PORT}/json/version" >/dev/null 2>&1; then
  echo "Miaoshou Chrome is already listening on port ${MIAOSHOU_PORT}."
  exit 0
fi

mkdir -p "${MIAOSHOU_PROFILE}"
open -na "Google Chrome" --args \
  "--remote-debugging-port=${MIAOSHOU_PORT}" \
  "--user-data-dir=${MIAOSHOU_PROFILE}" \
  "--no-first-run" \
  "--no-default-browser-check" \
  "${MIAOSHOU_URL}"

for attempt in {1..30}; do
  if curl -fsS "http://127.0.0.1:${MIAOSHOU_PORT}/json/version" >/dev/null 2>&1; then
    echo "Miaoshou Chrome is ready on port ${MIAOSHOU_PORT}."
    exit 0
  fi
  sleep 1
done

echo "Miaoshou Chrome did not expose port ${MIAOSHOU_PORT} within 30 seconds." >&2
exit 1
