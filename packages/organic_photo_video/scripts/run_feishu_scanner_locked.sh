#!/bin/zsh
set -eu

package_root="/Users/likeu3/.openclaw/workspace/packages/organic_photo_video"
cd "$package_root"
exec /usr/bin/python3 -u scripts/run_feishu_scanner_lock.py "$@"
