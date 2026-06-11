#!/bin/bash

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_PATH="${1:-$SCRIPT_DIR/feishu-direct.json}"
if [[ $# -gt 0 ]]; then
  shift
fi
EXTRA_ARG_COUNT=$#
if [[ $EXTRA_ARG_COUNT -gt 0 ]]; then
  EXTRA_ARGS=("$@")
else
  EXTRA_ARGS=()
fi
MAX_RESTARTS="${MAX_RESTARTS:-20}"
RESTART_DELAY_SECONDS="${RESTART_DELAY_SECONDS:-60}"
LOCK_DIR="${LOCK_DIR:-}"
PROCESS_LABEL="${PROCESS_LABEL:-}"
LOCK_PID_FILE="$LOCK_DIR/pid"
ALLOW_RESUME_ONLY="${ALLOW_RESUME_ONLY:-0}"
CIRCUIT_BREAKER_PATTERNS=(
  "飞书配置不完整"
  "飞书权限不足"
  "Forbidden"
  "connect ECONNREFUSED 127.0.0.1:9222"
  "browserURL: http://127.0.0.1:9222"
)

is_resume_only=0
is_one_shot=0
is_submit_only=0
is_download_only=0
if [[ $EXTRA_ARG_COUNT -gt 0 ]]; then
  for arg in "${EXTRA_ARGS[@]}"; do
    if [[ "$arg" == "--resume-only" ]]; then
      is_resume_only=1
    fi
    if [[ "$arg" == "--oneshot" ]]; then
      is_one_shot=1
    fi
    if [[ "$arg" == "--submit-only" ]]; then
      is_submit_only=1
    fi
    if [[ "$arg" == "--download-only" ]]; then
      is_download_only=1
    fi
  done
fi

if [[ -z "$LOCK_DIR" ]]; then
  if [[ $is_submit_only -eq 1 ]]; then
    LOCK_DIR="/tmp/jimeng-feishu-submit.lock"
  elif [[ $is_download_only -eq 1 ]]; then
    LOCK_DIR="/tmp/jimeng-feishu-download.lock"
  else
    LOCK_DIR="/tmp/jimeng-feishu-direct.lock"
  fi
fi

if [[ -z "$PROCESS_LABEL" ]]; then
  if [[ $is_submit_only -eq 1 ]]; then
    PROCESS_LABEL="飞书到即梦提单流程"
  elif [[ $is_download_only -eq 1 ]]; then
    PROCESS_LABEL="即梦资产下载回写流程"
  else
    PROCESS_LABEL="飞书到即梦流程"
  fi
fi

LOCK_PID_FILE="$LOCK_DIR/pid"

if [[ $is_resume_only -eq 1 && "$ALLOW_RESUME_ONLY" != "1" ]]; then
  echo "[$(date '+%F %T')] resume-only 已被默认禁用，跳过启动。若确需启用，请显式设置 ALLOW_RESUME_ONLY=1"
  exit 0
fi

if [[ $is_resume_only -eq 1 || $is_one_shot -eq 1 ]]; then
  MAX_RESTARTS="${MAX_RESTARTS:-1}"
  RESTART_DELAY_SECONDS="${RESTART_DELAY_SECONDS:-0}"
fi

stop_requested=0

handle_stop() {
  stop_requested=1
  echo "[$(date '+%F %T')] 收到停止信号，结束${PROCESS_LABEL}守护进程"
}

cleanup_lock() {
  if [[ -d "$LOCK_DIR" && -f "$LOCK_PID_FILE" ]]; then
    local owner_pid
    owner_pid="$(cat "$LOCK_PID_FILE" 2>/dev/null || true)"
    if [[ "$owner_pid" == "$$" ]]; then
      rm -rf "$LOCK_DIR"
    fi
  fi
}

acquire_lock() {
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    printf '%s\n' "$$" > "$LOCK_PID_FILE"
    return 0
  fi

  if [[ -f "$LOCK_PID_FILE" ]]; then
    local existing_pid
    existing_pid="$(cat "$LOCK_PID_FILE" 2>/dev/null || true)"
    if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" 2>/dev/null; then
      echo "[$(date '+%F %T')] 检测到已有${PROCESS_LABEL}正在运行（PID=${existing_pid}），本次不重复启动"
      return 1
    fi
  fi

  echo "[$(date '+%F %T')] 检测到${PROCESS_LABEL}陈旧锁，正在清理后重试"
  rm -rf "$LOCK_DIR"
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    printf '%s\n' "$$" > "$LOCK_PID_FILE"
    return 0
  fi

  echo "[$(date '+%F %T')] 无法获取${PROCESS_LABEL}锁，停止启动"
  return 1
}

trap 'handle_stop' INT TERM
trap 'cleanup_lock' EXIT

if ! acquire_lock; then
  exit 0
fi

detect_fatal_restart_error() {
  local log_file="$1"
  local pattern

  for pattern in "${CIRCUIT_BREAKER_PATTERNS[@]}"; do
    if grep -Fq "$pattern" "$log_file"; then
      echo "$pattern"
      return 0
    fi
  done

  return 1
}

attempt=1
while true; do
  echo "[$(date '+%F %T')] 启动${PROCESS_LABEL}（第 ${attempt} 次）"
  attempt_log="$(mktemp -t jimeng-feishu-attempt.XXXXXX.log)"
  if [[ $EXTRA_ARG_COUNT -gt 0 ]]; then
    node "$SCRIPT_DIR/feishu-direct-monitor.js" --config "$CONFIG_PATH" "${EXTRA_ARGS[@]}" > >(tee "$attempt_log") 2>&1
  else
    node "$SCRIPT_DIR/feishu-direct-monitor.js" --config "$CONFIG_PATH" > >(tee "$attempt_log") 2>&1
  fi
  exit_code=$?

  if [[ $stop_requested -eq 1 ]]; then
    rm -f "$attempt_log"
    exit 0
  fi

  if [[ $exit_code -eq 0 ]]; then
    echo "[$(date '+%F %T')] ${PROCESS_LABEL}正常结束"
    rm -f "$attempt_log"
    exit 0
  fi

  if [[ $is_resume_only -eq 1 || $is_one_shot -eq 1 ]]; then
    mode_label="resume-only"
    if [[ $is_one_shot -eq 1 && $is_resume_only -eq 0 ]]; then
      mode_label="oneshot"
    elif [[ $is_one_shot -eq 1 && $is_resume_only -eq 1 ]]; then
      mode_label="resume-only + oneshot"
    fi
    echo "[$(date '+%F %T')] ${PROCESS_LABEL}在 ${mode_label} 模式下异常退出（exit=${exit_code}），不自动重试"
    rm -f "$attempt_log"
    exit "$exit_code"
  fi

  fatal_reason="$(detect_fatal_restart_error "$attempt_log" || true)"
  if [[ -n "${fatal_reason:-}" ]]; then
    echo "[$(date '+%F %T')] 检测到${PROCESS_LABEL}确定性故障（${fatal_reason}），触发熔断并停止自动重试（exit=${exit_code}）"
    rm -f "$attempt_log"
    exit "$exit_code"
  fi

  if [[ $attempt -ge $MAX_RESTARTS ]]; then
    echo "[$(date '+%F %T')] ${PROCESS_LABEL}异常退出 ${attempt} 次，停止重试（exit=${exit_code}）"
    rm -f "$attempt_log"
    exit "$exit_code"
  fi

  echo "[$(date '+%F %T')] ${PROCESS_LABEL}异常退出（exit=${exit_code}），${RESTART_DELAY_SECONDS} 秒后自动重试"
  rm -f "$attempt_log"
  sleep "$RESTART_DELAY_SECONDS"
  attempt=$((attempt + 1))
done
