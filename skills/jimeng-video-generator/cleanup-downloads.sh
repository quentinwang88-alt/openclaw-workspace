#!/bin/bash

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_PATH="$SCRIPT_DIR/feishu-direct.json"
DRY_RUN=0

if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=1
fi

LOG_FILE="$HOME/Library/Logs/jimeng-feishu-cleanup.log"
LOCK_DIR="/tmp/jimeng-feishu-cleanup.lock"
LOCK_PID_FILE="$LOCK_DIR/pid"

# retention days for orphan files (no submission record)
ORPHAN_RETENTION=7

log() {
  echo "[$(date '+%F %T')] $*" | tee -a "$LOG_FILE"
}

get_retention() {
  case "$1" in
    uploaded|failed|blocked|timed_out|broken_state|claim_failed)
      echo 3 ;;
    upload_failed)
      # upload_failed may be retried by downstream workers, keep longer
      echo 7 ;;
    *)
      echo -1 ;;
  esac
}

is_protected() {
  case "$1" in
    submitted|rendering|observing|downloaded)
      return 0 ;;
    *)
      return 1 ;;
  esac
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
      log "已有清理进程正在运行（PID=${existing_pid}），本次跳过"
      return 1
    fi
  fi

  log "检测到陈旧锁，正在清理后重试"
  rm -rf "$LOCK_DIR"
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    printf '%s\n' "$$" > "$LOCK_PID_FILE"
    return 0
  fi

  log "无法获取锁，停止运行"
  return 1
}

trap 'cleanup_lock' EXIT

if [[ ! -f "$CONFIG_PATH" ]]; then
  log "ERROR: 配置文件不存在: $CONFIG_PATH"
  exit 1
fi

RUNTIME_ROOT=$(python3 -c "
import json, os, sys
with open('$CONFIG_PATH') as f:
    cfg = json.load(f)
root = cfg.get('runtimeRoot', '')
print(os.path.expanduser(root))
" 2>/dev/null)

if [[ -z "$RUNTIME_ROOT" ]]; then
  log "ERROR: 无法从配置文件 $CONFIG_PATH 读取 runtimeRoot"
  exit 1
fi

DOWNLOADS_DIR="$RUNTIME_ROOT/_state/downloads"
SUBMISSIONS_DIR="$RUNTIME_ROOT/_state/submissions"

if [[ ! -d "$DOWNLOADS_DIR" ]]; then
  log "下载目录不存在: $DOWNLOADS_DIR，无需清理"
  exit 0
fi

if ! acquire_lock; then
  exit 0
fi

log "========== cleanup start =========="
log "下载目录: $DOWNLOADS_DIR"
log "提交记录目录: $SUBMISSIONS_DIR"
[[ $DRY_RUN -eq 1 ]] && log "*** DRY-RUN 模式，不实际删除文件 ***"

deleted_count=0
skipped_count=0
error_count=0
deleted_size=0
would_delete_count=0
now=$(date +%s)

shopt -s nullglob
for file_path in "$DOWNLOADS_DIR"/*.mp4 "$DOWNLOADS_DIR"/*.MP4; do
  [[ -f "$file_path" ]] || continue

  filename=$(basename "$file_path")
  trace_id="${filename%.*}"

  file_mtime=$(stat -f %m "$file_path" 2>/dev/null)
  if [[ -z "$file_mtime" ]]; then
    log "WARN: 无法获取文件时间: $filename"
    error_count=$((error_count + 1))
    continue
  fi
  age_seconds=$((now - file_mtime))
  age_days=$((age_seconds / 86400))

  file_size_bytes=$(stat -f %z "$file_path" 2>/dev/null)
  file_size_mb=$(python3 -c "print(round(${file_size_bytes:-0} / 1048576.0, 1))" 2>/dev/null || echo "?")

  # lookup submission record
  submission_file="$SUBMISSIONS_DIR/${trace_id}.json"
  file_status="orphan"

  if [[ -f "$submission_file" ]]; then
    file_status=$(python3 <<PYEOF
import json, sys
try:
    with open('$submission_file') as f:
        rec = json.load(f)
    print(rec.get('status', 'unknown'))
except:
    print('broken_state')
PYEOF
)
  fi

  # determine retention
  if is_protected "$file_status"; then
    log "SKIPPED $file_status  $filename (${file_size_mb} MB, age=${age_days}d) — 进行中状态，受保护"
    skipped_count=$((skipped_count + 1))
    continue
  fi

  if [[ "$file_status" == "orphan" ]]; then
    retention_days=$ORPHAN_RETENTION
  else
    retention_days=$(get_retention "$file_status")
    # if we got -1, it's an unknown status we haven't classified
    if [[ $retention_days -lt 0 ]]; then
      retention_days=$ORPHAN_RETENTION
      file_status="unknown($file_status)"
    fi
  fi

  if [[ $age_days -ge $retention_days ]]; then
    if [[ $DRY_RUN -eq 1 ]]; then
      log "WOULD-DEL $file_status  $filename (${file_size_mb} MB, age=${age_days}d > ${retention_days}d)"
      would_delete_count=$((would_delete_count + 1))
    else
      rm -f "$file_path"
      if [[ $? -eq 0 ]]; then
        log "DELETED $file_status  $filename (${file_size_mb} MB, age=${age_days}d)"
        deleted_count=$((deleted_count + 1))
        deleted_size=$((deleted_size + file_size_bytes))
      else
        log "ERROR: 删除失败: $filename"
        error_count=$((error_count + 1))
      fi
    fi
  else
    log "KEPT    $file_status  $filename (${file_size_mb} MB, age=${age_days}d < ${retention_days}d)"
    skipped_count=$((skipped_count + 1))
  fi
done

deleted_size_mb=$(python3 -c "print(round(${deleted_size} / 1048576.0, 1))" 2>/dev/null || echo "?")
log "========== cleanup done =========="
if [[ $DRY_RUN -eq 1 ]]; then
  log "将会删除: ${would_delete_count} 个文件, 将跳过: ${skipped_count}, 错误: ${error_count}"
  log "*** 以上为 dry-run 结果，未实际删除任何文件 ***"
else
  log "已删除: ${deleted_count} 个文件 (${deleted_size_mb} MB), 跳过: ${skipped_count}, 错误: ${error_count}"
fi
