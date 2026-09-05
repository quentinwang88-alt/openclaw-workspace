#!/bin/zsh
set -eu

package_root="/Users/likeu3/.openclaw/workspace/packages/organic_photo_video"
lock_dir="/tmp/opv_publish_scheduler.lock"
if ! mkdir "$lock_dir" 2>/dev/null; then
  exit 0
fi
trap 'rmdir "$lock_dir"' EXIT
cd "$package_root"
/usr/bin/python3 -u scripts/run_publish_scheduler.py
