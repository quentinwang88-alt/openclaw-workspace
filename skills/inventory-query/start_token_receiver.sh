#!/bin/bash
# BigSeller Token 接收服务启动脚本

cd "$(dirname "$0")"

echo "🚀 启动 BigSeller Token 接收服务..."
echo "📡 监听地址: http://localhost:8765"
echo "⏸️  按 Ctrl+C 停止服务"
echo ""

# 检查 Python 版本
if ! command -v python3 &> /dev/null; then
    echo "❌ 错误: 未找到 python3"
    echo "请先安装 Python 3"
    exit 1
fi

# 启动服务
python3 token_receiver.py
