#!/bin/bash

# 即梦视频生成定时监测脚本
# 扫描指定文件夹，自动处理新增任务

set -e

# 设置环境变量
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
export NODE_OPTIONS="--max-old-space-size=4096"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${1:-$HOME/Desktop/jimeng}"
FORCE="${2:-}"  # 如果传入 --force，则重新处理已完成的任务

echo "========================================"
echo "🎬 即梦视频生成 - 定时监测"
echo "========================================"
echo "📅 $(date '+%Y-%m-%d %H:%M:%S')"
echo "📂 数据目录: $DATA_DIR"
echo "🔄 强制重处理: ${FORCE:-否}"
echo ""

# 检查数据目录
if [ ! -d "$DATA_DIR" ]; then
    echo "❌ 数据目录不存在: $DATA_DIR"
    exit 1
fi

# 统计变量
TOTAL=0
PENDING=0
COMPLETED=0
SUBMITTED=0

# 扫描所有子文件夹
for folder in "$DATA_DIR"/*/; do
    # 跳过非目录
    [ -d "$folder" ] || continue
    
    # 获取文件夹名称
    folder_name=$(basename "$folder")
    
    # 跳过隐藏文件夹
    [[ "$folder_name" == .* ]] && continue
    
    TOTAL=$((TOTAL + 1))
    
    # 检查是否有 prompt.txt
    if [ ! -f "$folder/prompt.txt" ] && [ ! -f "$folder/prompt.md" ]; then
        echo "⏭️  $folder_name - 无提示词文件"
        continue
    fi
    
    # 检查是否有图片
    has_image=false
    for img_dir in "$folder" "$folder/图片" "$folder/产品主图" "$folder/images" "$folder/img"; do
        if [ -d "$img_dir" ]; then
            for ext in png jpg jpeg webp gif PNG JPG JPEG WEBP GIF; do
                if ls "$img_dir"/*.$ext 1>/dev/null 2>&1; then
                    has_image=true
                    break 2
                fi
            done
        fi
    done
    
    if [ "$has_image" = false ]; then
        echo "⏭️  $folder_name - 无图片文件"
        continue
    fi
    
    # 检查是否已完成
    if [ -f "$folder/.completed" ] && [ "$FORCE" != "--force" ]; then
        COMPLETED=$((COMPLETED + 1))
        echo "✅ $folder_name - 已完成"
        continue
    fi
    
    # 检查是否已提交（等待完成）
    if [ -f "$folder/.submitted" ] && [ "$FORCE" != "--force" ]; then
        SUBMITTED=$((SUBMITTED + 1))
        echo "⏳ $folder_name - 已提交，等待生成"
        continue
    fi
    
    # 需要处理
    PENDING=$((PENDING + 1))
    echo "⏳ $folder_name - 待处理"
    
    # 如果是强制模式，删除标记文件
    if [ "$FORCE" = "--force" ]; then
        rm -f "$folder/.completed" "$folder/.submitted"
    fi
    
    # 执行处理
    echo "   🚀 开始处理..."
    if node "$SCRIPT_DIR/folder-processor.js" "$folder" 2>&1 | grep -E "^(🎬|✅|❌|📊)" | head -5; then
        echo "   ✅ 处理完成"
    else
        echo "   ❌ 处理失败"
    fi
    echo ""
done

# 检测已提交的任务是否完成
echo "========================================"
echo "🔍 检测已提交任务的完成状态..."
echo "========================================"

node "$SCRIPT_DIR/check-completion.js" "$DATA_DIR" 2>&1 | head -30

# 输出统计
echo "========================================"
echo "📊 监测统计"
echo "========================================"
echo "总任务数: $TOTAL"
echo "已完成: $COMPLETED"
echo "已提交（等待完成）: $SUBMITTED"
echo "本次处理: $PENDING"
echo ""

if [ $PENDING -eq 0 ] && [ $SUBMITTED -eq 0 ]; then
    echo "✅ 没有待处理的任务"
elif [ $PENDING -eq 0 ]; then
    echo "⏳ 有 $SUBMITTED 个任务等待生成完成"
else
    echo "🎉 本次处理了 $PENDING 个任务"
fi