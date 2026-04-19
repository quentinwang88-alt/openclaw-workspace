#!/bin/bash
# 库存预警 Skill 快速设置脚本

echo "=== 库存预警 Skill 设置 ==="
echo ""

# 1. 检查依赖
echo "1. 检查依赖..."
if ! command -v python3 &> /dev/null; then
    echo "❌ 未找到 Python3，请先安装"
    exit 1
fi
echo "✓ Python3 已安装"

# 2. 安装 Python 依赖
echo ""
echo "2. 安装 Python 依赖..."
pip3 install -r requirements.txt
echo "✓ 依赖安装完成"

# 3. 配置文件
echo ""
echo "3. 配置文件..."
if [ ! -f "config/alert_config.json" ]; then
    cp config/alert_config.example.json config/alert_config.json
    echo "✓ 已创建配置文件: config/alert_config.json"
    echo ""
    echo "⚠️  请编辑 config/alert_config.json 配置飞书 Webhook URL"
else
    echo "✓ 配置文件已存在"
fi

# 4. 测试运行
echo ""
echo "4. 测试运行..."
python3 alert.py --no-notify
echo ""

# 5. 设置定时任务
echo ""
echo "5. 设置定时任务（可选）"
echo ""
echo "要设置每天上午 8:30 自动运行，请执行："
echo ""
echo "  crontab -e"
echo ""
echo "然后添加以下行："
echo ""
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "  30 8 * * * cd $SCRIPT_DIR && /usr/bin/python3 alert.py"
echo ""
echo "=== 设置完成 ==="
echo ""
echo "使用方法："
echo "  python3 alert.py              # 检查并发送通知"
echo "  python3 alert.py --no-notify  # 仅检查不发送通知"
