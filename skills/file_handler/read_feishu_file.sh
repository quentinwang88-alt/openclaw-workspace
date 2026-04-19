#!/bin/bash
# 读取飞书文件工具脚本
# 用法: ./read_feishu_file.sh [文件路径]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 检查 Python 是否可用
if ! command -v python3 &> /dev/null; then
    echo "❌ 错误: 需要 Python3"
    exit 1
fi

# 检查依赖
if ! python3 -c "import pandas" 2>/dev/null; then
    echo "⚠️  正在安装依赖..."
    pip3 install pandas openpyxl -q
fi

# 运行 Python 脚本
if [ -z "$1" ]; then
    # 自动查找文件
    python3 "$SCRIPT_DIR/auto_file_handler.py"
else
    # 读取指定文件
    python3 "$SCRIPT_DIR/auto_file_handler.py" --file "$1"
fi
