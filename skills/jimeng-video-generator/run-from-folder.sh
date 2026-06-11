#!/bin/bash

# 从文件夹读取配置并生成视频
# 用法: ./run-from-folder.sh <文件夹路径> [--watch]

set -e

# 设置环境变量
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
export NODE_OPTIONS="--max-old-space-size=4096"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -z "$1" ]; then
    echo "用法: ./run-from-folder.sh <文件夹路径> [--watch]"
    echo ""
    echo "文件夹结构:"
    echo "  <文件夹>/"
    echo "  ├── image.png        # 参考图片 (必需)"
    echo "  ├── prompt.txt       # 提示词 (必需)"
    echo "  └── config.json      # 可选配置 (模型、比例、时长)"
    echo ""
    echo "示例:"
    echo "  ./run-from-folder.sh /Users/likeu/Desktop/video-tasks"
    echo "  ./run-from-folder.sh /Users/likeu/Desktop/video-tasks --watch"
    exit 1
fi

FOLDER="$1"
WATCH_MODE=false

if [[ "$*" == *"--watch"* ]]; then
    WATCH_MODE=true
fi

node "$SCRIPT_DIR/folder-processor.js" "$FOLDER" $([ "$WATCH_MODE" = true ] && echo "--watch")