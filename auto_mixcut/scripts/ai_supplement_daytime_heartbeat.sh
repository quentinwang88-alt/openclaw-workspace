#!/usr/bin/env bash
set -euo pipefail

cd /Users/likeu3/.openclaw/workspace/auto_mixcut

env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy \
  AUTO_MIXCUT_DB_PROVIDER=mysql \
  AUTO_MIXCUT_OSS_PROVIDER=aliyun \
  python3 scripts/run_ai_supplement_heartbeat.py --mode daytime "$@"
