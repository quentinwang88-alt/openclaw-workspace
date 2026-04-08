"""
增强版图片处理模块 - 支持在封面上添加数据标签

在视频封面上显示：
- 观看次数
- 成交金额（GMV）
"""

import os
import sys
from io import BytesIO
from typing import List, Tuple, Optional
from PIL import Image, ImageDraw, ImageFont

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.image_processor import ImageDownloader, GridCanvasGenerator
from core.kalodata_fetcher import KalodataVideo


class EnhancedGridGenerator(GridCanvasGenerator):
    """增强版宫格生成器 - 支持数据标签"""
    
    def __init__(
        self,
        cell_width: int = 300,
        cell_height: int = 400,
        quality: int = 85,
        show_stats: bool = True
    ):
        super().__init__(cell_width, cell_height, quality)
        self.show_stats = show_stats
        
        # 尝试加载字体
        self.font_large = self._load_font(size=20)
        self.font_small = self._load_font(size=16)
    
    def _load_font(self, size: int = 20) -> ImageFont.FreeTypeFont:
        """加载字体"""
        # 尝试加载系统字体
        font_paths = [
            "/System/Library/Fonts/PingFang.ttc",  # macOS 中文
            "/System/Library/Fonts/Helvetica.ttc",  # macOS 英文
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",  # Linux
            "C:\\Windows\\Fonts\\msyh.ttc",  # Windows 微软雅黑
            "C:\\Windows\\Fonts\\arial.ttf",  # Windows Arial
        ]
        
        for font_path in font_paths:
            try:
                if os.path.exists(font_path):
                    return ImageFont.truetype(font_path, size)
            except Exception:
                continue
        
        # 如果都失败，使用默认字体
        return ImageFont.load_default()
    
    def _format_number(self, num: int) -> str:
        """格式化数字（添加千分位）"""
        if num >= 1_000_000:
            return f"{num/1_000_000:.1f}M"
        elif num >= 1_000:
            return f"{num/1_000:.1f}K"
        else:
            return str(num)
    
    def _format_currency(self, amount: float) -> str:
        """格式化金额"""
        if amount >= 1_000_000:
            return f"¥{amount/1_000_000:.1f}M"
        elif amount >= 1_000:
            return f"¥{amount/1_000:.1f}K"
        else:
            return f"¥{amount:.0f}"
    
    def _add_stats_overlay(
        self,
        image: Image.Image,
        views: int,
        gmv: float
    ) -> Image.Image:
        """
        在图片上添加数据标签
        
        Args:
            image: 原始图片
            views: 观看次数
            gmv: 成交金额
        
        Returns:
            Image.Image: 添加标签后的图片
        """
        # 创建副本
        img = image.copy()
        draw = ImageDraw.Draw(img)
        
        width, height = img.size
        
        # 半透明黑色背景
        overlay = Image.new('RGBA', img.size, (0, 0, 0, 0))
        overlay_draw = ImageDraw.Draw(overlay)
        
        # 底部渐变遮罩
        gradient_height = 80
        for y in range(gradient_height):
            alpha = int(180 * (y / gradient_height))
            overlay_draw.rectangle(
                [(0, height - gradient_height + y), (width, height - gradient_height + y + 1)],
                fill=(0, 0, 0, alpha)
            )
        
        # 合并遮罩
        img = Image.alpha_composite(img.convert('RGBA'), overlay).convert('RGB')
        draw = ImageDraw.Draw(img)
        
        # 格式化数据
        views_text = f"👁 {self._format_number(views)}"
        gmv_text = self._format_currency(gmv)
        
        # 计算文本位置
        padding = 10
        bottom_y = height - padding - 20
        
        # 绘制观看次数（左下）
        draw.text(
            (padding, bottom_y - 25),
            views_text,
            fill=(255, 255, 255),
            font=self.font_small
        )
        
        # 绘制成交金额（左下，第二行）
        draw.text(
            (padding, bottom_y),
            gmv_text,
            fill=(255, 215, 0),  # 金色
            font=self.font_large
        )
        
        return img
    
    def create_canvas_with_stats(
        self,
        videos: List[KalodataVideo],
        max_images: int = 12
    ) -> Image.Image:
        """
        创建带数据标签的宫格画板
        
        Args:
            videos: Kalodata 视频列表
            max_images: 最大图片数量
        
        Returns:
            Image.Image: 拼接后的画板
        """
        if not videos:
            raise ValueError("视频列表为空")
        
        # 限制数量
        videos = videos[:max_images]
        
        # 下载图片
        cover_urls = [v.cover_url for v in videos]
        images = self.downloader.download_images_batch(cover_urls)
        
        if len(images) < len(videos):
            print(f"⚠️ 部分图片下载失败: {len(images)}/{len(videos)}")
        
        # 创建白色背景画布（3 列 x 4 行）
        canvas_width = self.cell_width * 3
        canvas_height = self.cell_height * 4
        canvas = Image.new('RGB', (canvas_width, canvas_height), color=(255, 255, 255))
        
        # 按 3x4 矩阵粘贴图片
        for idx, (img, video) in enumerate(zip(images, videos)):
            if idx >= 12:  # 最多 12 张
                break
            
            row = idx // 3  # 行号 (0-3)
            col = idx % 3   # 列号 (0-2)
            
            # Resize 图片
            img_resized = img.resize(
                (self.cell_width, self.cell_height),
                Image.Resampling.LANCZOS
            )
            
            # 添加数据标签
            if self.show_stats:
                img_resized = self._add_stats_overlay(
                    img_resized,
                    video.views,
                    video.gmv
                )
            
            # 计算粘贴位置
            x = col * self.cell_width
            y = row * self.cell_height
            
            # 粘贴到画布
            canvas.paste(img_resized, (x, y))
        
        return canvas
    
    def generate_grids_from_videos(
        self,
        videos: List[KalodataVideo],
        max_grids: int = 3
    ) -> List[Image.Image]:
        """
        从视频列表生成多张宫格（每张 3x6 = 18 个视频）
        
        Args:
            videos: Kalodata 视频列表
            max_grids: 最大宫格数量（默认 3 张）
        
        Returns:
            List[Image.Image]: 宫格列表
        """
        grids = []
        videos_per_grid = 18  # 3x6
        
        for i in range(max_grids):
            start_idx = i * videos_per_grid
            end_idx = start_idx + videos_per_grid
            
            video_chunk = videos[start_idx:end_idx]
            
            if not video_chunk:
                break
            
            try:
                # 创建 3x6 宫格
                grid = self._create_3x6_grid(video_chunk)
                grids.append(grid)
                print(f"✅ 宫格 {i+1}/{max_grids} 生成成功 ({len(video_chunk)} 个视频)")
            except Exception as e:
                print(f"⚠️ 宫格 {i+1} 生成失败: {e}")
                continue
        
        return grids
    
    def _create_3x6_grid(self, videos: List[KalodataVideo]) -> Image.Image:
        """
        创建 3x6 宫格
        
        Args:
            videos: 视频列表（最多 18 个）
        
        Returns:
            Image.Image: 3x6 宫格
        """
        # 下载图片
        cover_urls = [v.cover_url for v in videos]
        images = self.downloader.download_images_batch(cover_urls)
        
        if len(images) < len(videos):
            print(f"⚠️ 部分图片下载失败: {len(images)}/{len(videos)}")
        
        # 创建白色背景画布（3 列 x 6 行）
        canvas_width = self.cell_width * 3
        canvas_height = self.cell_height * 6
        canvas = Image.new('RGB', (canvas_width, canvas_height), color=(255, 255, 255))
        
        # 按 3x6 矩阵粘贴图片
        for idx, (img, video) in enumerate(zip(images, videos)):
            if idx >= 18:  # 最多 18 张
                break
            
            row = idx // 3  # 行号 (0-5)
            col = idx % 3   # 列号 (0-2)
            
            # Resize 图片
            img_resized = img.resize(
                (self.cell_width, self.cell_height),
                Image.Resampling.LANCZOS
            )
            
            # 添加数据标签
            if self.show_stats:
                img_resized = self._add_stats_overlay(
                    img_resized,
                    video.views,
                    video.gmv
                )
            
            # 计算粘贴位置
            x = col * self.cell_width
            y = row * self.cell_height
            
            # 粘贴到画布
            canvas.paste(img_resized, (x, y))
        
        return canvas


def generate_grids_from_kalodata(
    videos: List[KalodataVideo],
    output_dir: str,
    tk_handle: str,
    cell_size: Tuple[int, int] = (300, 400),
    show_stats: bool = True
) -> List[str]:
    """
    从 Kalodata 视频生成宫格并保存
    
    Args:
        videos: Kalodata 视频列表
        output_dir: 输出目录
        tk_handle: TikTok 账号
        cell_size: 单元格尺寸 (width, height)
        show_stats: 是否显示数据标签
    
    Returns:
        List[str]: 保存的文件路径列表
    """
    from pathlib import Path
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    generator = EnhancedGridGenerator(
        cell_width=cell_size[0],
        cell_height=cell_size[1],
        show_stats=show_stats
    )
    
    # 生成 3 张 3x6 宫格
    grids = generator.generate_grids_from_videos(videos, max_grids=3)
    
    # 保存文件
    file_paths = []
    for i, grid in enumerate(grids, 1):
        file_path = output_path / f"{tk_handle}_grid_{i}.png"
        grid.save(file_path, format='PNG', quality=85)
        file_paths.append(str(file_path))
        print(f"✅ 宫格 {i} 已保存: {file_path}")
    
    return file_paths


if __name__ == "__main__":
    print("=" * 80)
    print("🧪 增强版宫格生成器测试")
    print("=" * 80)
    
    # 创建测试数据
    from core.kalodata_fetcher import KalodataVideo
    
    test_videos = [
        KalodataVideo(
            video_id=f"test_{i}",
            cover_url="https://via.placeholder.com/300x400",
            views=10000 * (i + 1),
            gmv=5000.0 * (i + 1),
            description=f"测试视频 {i}"
        )
        for i in range(48)
    ]
    
    print(f"\n创建了 {len(test_videos)} 个测试视频")
    print("\n💡 提示: 实际使用时，请从 KalodataFetcher 获取真实数据")

