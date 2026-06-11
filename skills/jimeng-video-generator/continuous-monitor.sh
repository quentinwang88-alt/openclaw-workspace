#!/bin/bash

# 即梦视频生成 - 智能持续监测脚本
# 功能：检测完成状态 + 智能提交新任务（支持最大并发数配置）

set -e

export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
export NODE_OPTIONS="--max-old-space-size=4096"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${1:-$HOME/Desktop/jimeng}"
CHECK_INTERVAL=${2:-30}  # 默认30分钟检测一次

# 读取配置文件中的检测间隔
CONFIG_FILE="$SCRIPT_DIR/config.json"
if [ -f "$CONFIG_FILE" ]; then
  CONFIG_INTERVAL=$(node -e "console.log(require('$CONFIG_FILE').checkIntervalMinutes || 30)" 2>/dev/null || echo "30")
  if [ "$CONFIG_INTERVAL" != "undefined" ] && [ -n "$CONFIG_INTERVAL" ]; then
    CHECK_INTERVAL=$CONFIG_INTERVAL
  fi
fi

echo "========================================"
echo "🎬 即梦视频生成 - 智能持续监测"
echo "========================================"
echo "📂 数据目录: $DATA_DIR"
echo "⏱️  检测间隔: ${CHECK_INTERVAL} 分钟"
echo "📅 开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# 创建日志目录
LOG_DIR="$HOME/.openclaw/logs/jimeng"
mkdir -p "$LOG_DIR"

# 主循环
while true; do
  echo ""
  echo "========================================"
  echo "🔄 检测循环 - $(date '+%Y-%m-%d %H:%M:%S')"
  echo "========================================"
  
  # 运行智能检测脚本
  node "$SCRIPT_DIR/smart-monitor.js" "$DATA_DIR"
  EXIT_CODE=$?
  
  # 根据退出码判断状态
  # 0: 所有任务完成
  # 1: 还有任务在处理
  
  if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✅ 所有任务已完成！"
    echo "🏁 退出监测模式"
    break
  fi
  
  echo ""
  echo "💤 等待 ${CHECK_INTERVAL} 分钟后再次检测..."
  echo "   下次检测: $(date -v+${CHECK_INTERVAL}M '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -d "+${CHECK_INTERVAL} minutes" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo '计算失败')"
  
  sleep $((CHECK_INTERVAL * 60))
done

echo ""
echo "========================================"
echo "🏁 监测结束 - $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"