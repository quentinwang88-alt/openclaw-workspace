#!/bin/zsh
set -eu

# cron 的默认 PATH 不含 /usr/local/bin，会导致 creatok 等本地 CLI 找不到。
export PATH="/usr/local/bin:$PATH"

package_root="/Users/likeu3/.openclaw/workspace/packages/organic_photo_video"
cd "$package_root"
exec /usr/bin/python3 -u scripts/run_feishu_scanner_lock.py "$@"
