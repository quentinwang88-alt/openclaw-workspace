#!/bin/zsh
set -eu

# cron 的默认 PATH 不含 /usr/local/bin，会导致 creatok 等本地 CLI 找不到。
export PATH="/usr/local/bin:$PATH"
# 2026-09-13：生图通道改为 1route 主力（gpt-image-2.5-sunburst），
# codex 二线（额度/限流时接管），CreatOK 三线兜底。三线任一失败自动落下一线，
# 不会像独占通道那样全线失败。想看/改优先级只改这一行即可：
#   1route 独占 → "1route"；codex 主力 → "codex>1route>creatok"；
#   回旧行为 → "codex_fallback"（codex→creatok）或 "creatok_fallback"。
export OPV_PHOTO_CHANNEL="1route>codex>creatok"
export OPENAI_CODEX_IMAGE_MODEL="gpt-image-2.5-sunburst"
# codex 图像流偶发慢响应：总闸门 240s，给慢响应留完成时间。
export OPENAI_IMAGE_TOTAL_TIMEOUT="240"
export OPENAI_IMAGE_FIRST_EVENT_TIMEOUT="120"

package_root="/Users/likeu3/.openclaw/workspace/packages/organic_photo_video"
cd "$package_root"
exec /usr/bin/python3 -u scripts/run_feishu_scanner_lock.py "$@"
