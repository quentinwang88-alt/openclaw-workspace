#!/bin/bash
# Codex 模式文生图测试脚本
# 用法: bash test_codex_mode.sh
# 请在 OpenClaw workspace 目录下运行

set -e
cd "$(dirname "$0")"

echo "=== Codex 模式文生图测试 ==="
echo ""

# 确保依赖
pip install httpx httpx-socks openai --break-system-packages -q 2>/dev/null || true

# 运行测试
OPENAI_IMAGE_API_MODE=codex \
OPENAI_IMAGE_OUTPUT_DIR="$HOME/.openclaw/workspace/runtime/image_outputs" \
PYTHONPATH="$HOME/.openclaw/workspace:$HOME/.openclaw/workspace/skills/openai-image" \
python3 run_pipeline.py \
  --input "$HOME/.openclaw/workspace/skills/openai-image/examples/sample_generate.json"

echo ""
echo "=== 测试完成 ==="
echo "检查输出目录: $HOME/.openclaw/workspace/runtime/image_outputs/"
ls -la "$HOME/.openclaw/workspace/runtime/image_outputs/" 2>/dev/null || true
