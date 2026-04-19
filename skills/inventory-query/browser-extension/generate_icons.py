#!/usr/bin/env python3
"""
生成浏览器扩展图标
"""

from PIL import Image, ImageDraw, ImageFont
from pathlib import Path


def create_icon(size: int, filename: str):
    """创建图标"""
    # 创建蓝色背景
    img = Image.new('RGB', (size, size), color='#1976d2')
    draw = ImageDraw.Draw(img)
    
    # 绘制字母 T (Token)
    font_size = int(size * 0.6)
    try:
        # macOS 字体
        font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", font_size)
    except:
        try:
            # Linux 字体
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
        except:
            # 默认字体
            font = ImageFont.load_default()
    
    text = "T"
    bbox = draw.textbbox((0, 0), text, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]
    
    position = ((size - text_width) // 2, (size - text_height) // 2 - bbox[1])
    draw.text(position, text, fill='white', font=font)
    
    # 保存
    output_path = Path(__file__).parent / filename
    img.save(output_path)
    print(f"✅ 已生成: {filename}")


def main():
    """生成所有尺寸的图标"""
    print("🎨 开始生成图标...")
    
    try:
        create_icon(16, 'icon16.png')
        create_icon(48, 'icon48.png')
        create_icon(128, 'icon128.png')
        print("\n✅ 所有图标生成完成！")
    except Exception as e:
        print(f"\n❌ 生成失败: {e}")
        print("\n请安装 Pillow: pip install Pillow")


if __name__ == "__main__":
    main()
