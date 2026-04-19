# 浏览器扩展图标说明

本目录需要三个尺寸的图标文件：

- `icon16.png` - 16x16 像素
- `icon48.png` - 48x48 像素  
- `icon128.png` - 128x128 像素

## 快速生成图标

你可以使用以下方法生成图标：

### 方法 1：在线生成
访问 https://www.favicon-generator.org/ 上传一张图片，自动生成多个尺寸

### 方法 2：使用 ImageMagick
```bash
# 安装 ImageMagick
brew install imagemagick  # macOS
# sudo apt install imagemagick  # Linux

# 从一张大图生成多个尺寸
convert source.png -resize 16x16 icon16.png
convert source.png -resize 48x48 icon48.png
convert source.png -resize 128x128 icon128.png
```

### 方法 3：使用 Python PIL
```python
from PIL import Image

img = Image.open('source.png')
img.resize((16, 16)).save('icon16.png')
img.resize((48, 48)).save('icon48.png')
img.resize((128, 128)).save('icon128.png')
```

## 临时方案

如果暂时没有图标，可以创建简单的纯色图标：

```python
from PIL import Image, ImageDraw, ImageFont

def create_icon(size, filename):
    img = Image.new('RGB', (size, size), color='#1976d2')
    draw = ImageDraw.Draw(img)
    
    # 绘制字母 T (Token)
    font_size = int(size * 0.6)
    try:
        font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", font_size)
    except:
        font = ImageFont.load_default()
    
    text = "T"
    bbox = draw.textbbox((0, 0), text, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]
    
    position = ((size - text_width) // 2, (size - text_height) // 2 - bbox[1])
    draw.text(position, text, fill='white', font=font)
    
    img.save(filename)

create_icon(16, 'icon16.png')
create_icon(48, 'icon48.png')
create_icon(128, 'icon128.png')
```

运行上述代码即可生成临时图标。
