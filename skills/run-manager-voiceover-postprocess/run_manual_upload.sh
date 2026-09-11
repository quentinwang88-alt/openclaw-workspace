#!/bin/bash
# Manual-upload voiceover wrapper for OpenClaw.
# Accepts only an allow-listed set of arguments and delegates to the central
# voiceover_copy_engine implementation.

set -euo pipefail

ENGINE_DIR="${VOICEOVER_COPY_ENGINE_ROOT:-/Users/likeu3/voiceover_copy_engine}"
ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --copy-only)
            ARGS+=("--copy-only")
            ;;
        --record-id)
            if [[ $# -lt 2 || ! "$2" =~ ^rec[A-Za-z0-9_-]+(,rec[A-Za-z0-9_-]+)*$ ]]; then
                echo "--record-id 必须是 rec 开头的飞书记录 ID；多个 ID 用英文逗号分隔" >&2
                exit 2
            fi
            ARGS+=("--record-id" "$2")
            shift
            ;;
        --limit)
            if [[ $# -lt 2 || ! "$2" =~ ^[1-9][0-9]*$ || "$2" -gt 20 ]]; then
                echo "--limit 必须是 1 到 20 的整数" >&2
                exit 2
            fi
            ARGS+=("--limit" "$2")
            shift
            ;;
        --voiceover-env)
            if [[ $# -lt 2 || ( "$2" != "development" && "$2" != "production" ) ]]; then
                echo "--voiceover-env 只能是 development 或 production" >&2
                exit 2
            fi
            ARGS+=("--voiceover-env" "$2")
            shift
            ;;
        *)
            echo "不支持的参数：$1" >&2
            exit 2
            ;;
    esac
    shift
done

cd "$ENGINE_DIR"
PYTHONPATH=".:${PYTHONPATH:-}" python3 scripts/run_manual_upload_voiceover.py "${ARGS[@]}"
