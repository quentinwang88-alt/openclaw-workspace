#!/usr/bin/env bash
set -euo pipefail

cd /Users/likeu3/.openclaw/workspace/auto_mixcut
export PATH="/Users/likeu3/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:${PATH:-}"

env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy \
  AUTO_MIXCUT_DB_PROVIDER=mysql \
  AUTO_MIXCUT_OSS_PROVIDER=aliyun \
  AUTO_MIXCUT_SCANNER_INTERVAL_SECONDS="${AUTO_MIXCUT_SCANNER_INTERVAL_SECONDS:-7200}" \
  AUTO_MIXCUT_SCANNER_MAX_PRODUCTS="${AUTO_MIXCUT_SCANNER_MAX_PRODUCTS:-2}" \
  AUTO_MIXCUT_SCANNER_MAX_WORKERS="${AUTO_MIXCUT_SCANNER_MAX_WORKERS:-2}" \
  python3 scripts/run_mixcut_task_scanner.py --loop "$@"
