#!/bin/bash
# Run Manager Voiceover Post-Process - Single Run Wrapper
# Usage: ./run_once.sh [--dry-run] [--record-id recXXXX]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENGINE_DIR="${VOICEOVER_COPY_ENGINE_ROOT:-$HOME/voiceover_copy_engine}"

cd "$ENGINE_DIR"

ARGS=("--once")
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) ARGS+=("--dry-run") ;;
        --record-id) ARGS+=("--record-id" "$2"); shift ;;
        --max-records) ARGS+=("--max-records" "$2"); shift ;;
        --config) ARGS+=("--config" "$2"); shift ;;
    esac
    shift
done

PYTHONPATH=. python3 scripts/run_run_manager_voiceover.py "${ARGS[@]}"
