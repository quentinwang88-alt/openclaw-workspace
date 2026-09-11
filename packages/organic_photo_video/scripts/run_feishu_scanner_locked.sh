#!/bin/zsh
set -eu

# cron 的默认 PATH 不含 /usr/local/bin，会导致 creatok 等本地 CLI 找不到。
export PATH="/usr/local/bin:$PATH"
# 2026-09-11：codex 额度已重置，切回 codex 主力 + CreatoK 兜底（codex_fallback）。
# 额度再耗尽时会自动切 CreatoK，不会像独占通道那样全线失败。
# 想彻底独占 codex 或只用 CreatOK 时，把值改为 openai-image / creatok。
export OPV_PHOTO_CHANNEL="codex_fallback"
export OPENAI_CODEX_IMAGE_MODEL="gpt-image-2.5-sunburst"
# codex 图像流偶发慢响应：总闸门 240s，给慢响应留完成时间。
export OPENAI_IMAGE_TOTAL_TIMEOUT="240"
export OPENAI_IMAGE_FIRST_EVENT_TIMEOUT="120"

package_root="/Users/likeu3/.openclaw/workspace/packages/organic_photo_video"
cd "$package_root"
exec /usr/bin/python3 -u scripts/run_feishu_scanner_lock.py "$@"
