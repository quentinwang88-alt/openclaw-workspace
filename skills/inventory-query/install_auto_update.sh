#!/bin/bash
# Token 自动更新 - 一键安装脚本

set -e

echo "🚀 BigSeller Token 自动更新 - 安装向导"
echo "========================================"
echo ""

# 检查 Python
if ! command -v python3 &> /dev/null; then
    echo "❌ 未找到 Python 3，请先安装 Python"
    exit 1
fi

echo "✅ Python 已安装: $(python3 --version)"
echo ""

# 安装依赖
echo "📦 安装依赖..."
pip3 install Pillow 2>&1 | grep -v "already satisfied" || true
echo ""

# 生成图标
echo "🎨 生成浏览器扩展图标..."
cd browser-extension
python3 generate_icons.py
cd ..
echo ""

# 检查图标是否生成成功
if [ -f "browser-extension/icon16.png" ] && [ -f "browser-extension/icon48.png" ] && [ -f "browser-extension/icon128.png" ]; then
    echo "✅ 图标生成成功"
else
    echo "❌ 图标生成失败"
    exit 1
fi
echo ""

# 显示下一步操作
echo "✅ 安装准备完成！"
echo ""
echo "📋 下一步操作："
echo ""
echo "1️⃣  安装浏览器扩展："
echo "   Chrome/Edge: 打开 chrome://extensions/"
echo "   - 开启「开发者模式」"
echo "   - 点击「加载已解压的扩展程序」"
echo "   - 选择目录: $(pwd)/browser-extension"
echo ""
echo "2️⃣  启动自动接收服务："
echo "   cd $(pwd)"
echo "   python3 token_manager.py server"
echo ""
echo "3️⃣  测试同步："
echo "   - 浏览器访问 https://www.bigseller.pro"
echo "   - 点击扩展图标"
echo "   - 点击「立即同步」"
echo ""
echo "🎉 完成后，Token 将自动保持最新！"
echo ""
echo "📖 详细文档: TOKEN_AUTO_UPDATE_QUICKSTART.md"
