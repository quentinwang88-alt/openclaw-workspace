#!/bin/bash

# 即梦视频生成器运行脚本

cd "$(dirname "$0")"

# 检查参数
if [ -z "$1" ]; then
    echo "用法: ./run.sh --prompt '视频描述' [--image 图片路径] [--ratio 9:16] [--duration 15]"
    echo ""
    echo "示例:"
    echo "  ./run.sh --prompt '优雅女性展示护肤品质感'"
    echo "  ./run.sh --prompt '产品特写' --image ./product.png"
    echo "  ./run.sh --batch ./tasks.json"
    exit 1
fi

# 运行
node generate-video.js "$@"