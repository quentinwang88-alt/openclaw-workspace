"""
图片处理模块 - 画板生成与图片处理

负责：
1. 下载视频封面图
2. 拼接为 3x4 宫格画板（支持播放量和成交金额文字叠加）
3. Base64 编码
"""

import os
import sys
import base64
import requests
from io import BytesIO
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image, ImageDraw, ImageFont

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.exceptions import ImageProcessingError, InsufficientDataError
from core.data_fetchers import VideoData


# ============================================================================
# 图片下载器
# ============================================================================

class ImageDownloader:
    """图片下载器（并发）"""
    
    def __init__(self, timeout: int = 15, max_workers: int = 9, max_retries: int = 2):
        self.timeout = timeout
        self.max_workers = max_workers
        self.max_retries = max_retries
    
    def download_image(self, url: str, retry_count: int = 0) -> Optional[Image.Image]:
        """
        下载单张图片
        
        Args:
            url: 图片 URL
            retry_count: 当前重试次数
            
        Returns:
            Optional[Image.Image]: PIL Image 对象，失败返回 None
        """
        try:
            resp = requests.get(url, timeout=self.timeout)
            resp.raise_for_status()
            
            img = Image.open(BytesIO(resp.content))
            
            # 转换为 RGB（防止 RGBA 或其他格式）
            if img.mode != 'RGB':
                img = img.convert('RGB')
            
            return img
            
        except requests.exceptions.Timeout:
            if retry_count < self.max_retries:
                print(f"    ⚠️ 图片下载超时，重试 ({retry_count + 1}/{self.max_retries}): {url[:50]}...")
                import time
                time.sleep(1)  # 等待1秒后重试
                return self.download_image(url, retry_count + 1)
            print(f"    ❌ 图片下载超时（已重试{self.max_retries}次）: {url[:50]}...")
            return None
            
        except requests.exceptions.RequestException as e:
            if retry_count < self.max_retries:
                print(f"    ⚠️ 图片下载失败，重试 ({retry_count + 1}/{self.max_retries}): {str(e)[:50]}")
                import time
                time.sleep(1)
                return self.download_image(url, retry_count + 1)
            print(f"    ❌ 图片下载失败: {str(e)[:100]}")
            return None
            
        except Exception as e:
            print(f"    ❌ 图片处理失败: {str(e)[:100]}")
            return None
    
    def download_images_batch(
        self,
        urls: List[str],
        overall_timeout: float = 120.0
    ) -> List[Tuple[int, Image.Image]]:
        """
        批量下载图片（并发，保持顺序）
        
        Args:
            urls: 图片 URL 列表
            overall_timeout: 整体超时时间（秒），防止无限等待
            
        Returns:
            List[Image.Image]: 成功下载的图片列表（保持原始顺序，失败的跳过）
        """
        import time
        start_time = time.time()
        results = [None] * len(urls)
        completed = 0
        
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 使用 enumerate 保持索引
                futures = {executor.submit(self.download_image, url): idx for idx, url in enumerate(urls)}
                
                for future in as_completed(futures):
                    # 检查整体超时
                    elapsed = time.time() - start_time
                    if elapsed > overall_timeout:
                        print(f"    ⚠️ 批量下载整体超时（{elapsed:.1f}秒），已取消剩余任务")
                        # 取消未完成的任务
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        break
                    
                    idx = futures[future]
                    try:
                        img = future.result(timeout=self.timeout + 5)  # 单个任务额外超时保护
                        if img:
                            results[idx] = img
                        completed += 1
                    except Exception as e:
                        completed += 1
                        continue
        except Exception as e:
            print(f"    ❌ 批量下载异常: {e}")
        
        # 过滤掉 None，保持顺序
        successful = [img for img in results if img is not None]
        elapsed = time.time() - start_time
        print(f"    📊 批量下载完成: {len(successful)}/{len(urls)} 张，耗时 {elapsed:.1f}秒")
        return successful


# ============================================================================
# 画板生成器
# ============================================================================

# ============================================================================
# 辅助函数
# ============================================================================

def _format_number(n: float) -> str:
    """格式化数字为易读形式（如 1.2M、35K）"""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.1f}K"
    else:
        return str(int(n))


def _get_font(size: int) -> ImageFont.ImageFont:
    """获取字体，优先使用系统字体，回退到默认字体"""
    font_candidates = [
        # macOS
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/Helvetica.ttc",
        "/Library/Fonts/Arial.ttf",
        # Linux
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        # Windows
        "C:/Windows/Fonts/arial.ttf",
    ]
    for path in font_candidates:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
    # 回退到 PIL 内置字体
    return ImageFont.load_default()


class GridCanvasGenerator:
    """3x4 宫格画板生成器（支持播放量和成交金额文字叠加）"""
    
    def __init__(
        self,
        cell_width: int = 300,
        cell_height: int = 400,
        quality: int = 85
    ):
        self.cell_width = cell_width
        self.cell_height = cell_height
        self.quality = quality
        self.downloader = ImageDownloader()
    
    def _draw_overlay_text(
        self,
        canvas: Image.Image,
        x: int,
        y: int,
        views: int = 0,
        revenue: float = 0.0
    ) -> None:
        """
        在单元格底部绘制半透明背景 + 播放量/成交金额文字
        
        Args:
            canvas: 目标画布（RGBA 模式）
            x: 单元格左上角 x 坐标
            y: 单元格左上角 y 坐标
            views: 播放量（0 表示不显示）
            revenue: 成交金额（0 表示不显示）
        """
        if views == 0 and revenue == 0.0:
            return
        
        draw = ImageDraw.Draw(canvas, 'RGBA')
        font_size = max(16, self.cell_height // 22)
        font = _get_font(font_size)
        
        # 构建文字行
        lines = []
        if views > 0:
            lines.append(f"▶ {_format_number(views)}")
        if revenue > 0.0:
            lines.append(f"$ {_format_number(revenue)}")
        
        if not lines:
            return
        
        line_height = font_size + 4
        bar_height = line_height * len(lines) + 8
        bar_y = y + self.cell_height - bar_height
        
        # 绘制半透明黑色背景条
        draw.rectangle(
            [x, bar_y, x + self.cell_width, y + self.cell_height],
            fill=(0, 0, 0, 160)
        )
        
        # 绘制文字
        for i, line in enumerate(lines):
            text_y = bar_y + 4 + i * line_height
            # 白色文字带黑色描边（增强可读性）
            for dx, dy in [(-1, -1), (1, -1), (-1, 1), (1, 1)]:
                draw.text((x + 8 + dx, text_y + dy), line, fill=(0, 0, 0, 200), font=font)
            draw.text((x + 8, text_y), line, fill=(255, 255, 255, 255), font=font)
    
    def create_canvas(
        self,
        images: List[Image.Image],
        max_images: int = 12,
        views_list: Optional[List[int]] = None,
        revenue_list: Optional[List[float]] = None
    ) -> Image.Image:
        """
        创建单个 3x4 宫格画板（根据实际图片数量动态调整高度）
        
        Args:
            images: PIL Image 列表（最多 12 张）
            max_images: 最大图片数量
            views_list: 每张图对应的播放量列表（可选）
            revenue_list: 每张图对应的成交金额列表（可选）
            
        Returns:
            Image.Image: 拼接后的画板
            
        Raises:
            ImageProcessingError: 图片处理失败
        """
        if not images:
            raise ImageProcessingError("图片列表为空")
        
        # 限制图片数量
        images = images[:max_images]
        n = len(images)
        
        # 计算实际需要的行数（向上取整）
        cols = 3
        rows = (n + cols - 1) // cols  # 向上取整
        
        # 创建 RGBA 画布（根据实际行数动态调整高度）
        canvas_width = self.cell_width * cols
        canvas_height = self.cell_height * rows
        canvas = Image.new('RGBA', (canvas_width, canvas_height), color=(255, 255, 255, 255))
        
        # 按 3xN 矩阵粘贴图片
        for idx, img in enumerate(images):
            if idx >= max_images:
                break
            
            row = idx // cols  # 行号
            col = idx % cols   # 列号 (0-2)
            
            # Resize 图片
            img_resized = img.resize(
                (self.cell_width, self.cell_height),
                Image.Resampling.LANCZOS
            )
            
            # 确保为 RGBA
            if img_resized.mode != 'RGBA':
                img_resized = img_resized.convert('RGBA')
            
            # 计算粘贴位置
            x = col * self.cell_width
            y = row * self.cell_height
            
            # 粘贴到画布
            canvas.paste(img_resized, (x, y))
            
            # 叠加播放量和成交金额文字
            views = views_list[idx] if views_list and idx < len(views_list) else 0
            revenue = revenue_list[idx] if revenue_list and idx < len(revenue_list) else 0.0
            self._draw_overlay_text(canvas, x, y, views=views, revenue=revenue)
        
        # 转换回 RGB（JPEG 不支持透明通道）
        return canvas.convert('RGB')
    
    def canvas_to_base64(self, canvas: Image.Image) -> str:
        """
        将画板转换为 Base64 编码
        
        Args:
            canvas: PIL Image 对象
            
        Returns:
            str: Base64 编码的 JPEG 字符串
        """
        buffer = BytesIO()
        canvas.save(buffer, format='JPEG', quality=self.quality)
        buffer.seek(0)
        
        return base64.b64encode(buffer.read()).decode('utf-8')
    
    def generate_from_urls(
        self,
        cover_urls: List[str],
        max_images: int = 12,
        views_list: Optional[List[int]] = None,
        revenue_list: Optional[List[float]] = None
    ) -> str:
        """
        从 URL 列表生成画板（下载 + 拼接 + Base64）
        
        Args:
            cover_urls: 封面 URL 列表
            max_images: 最大图片数量
            views_list: 每张图对应的播放量列表（可选）
            revenue_list: 每张图对应的成交金额列表（可选）
            
        Returns:
            str: Base64 编码的画板
            
        Raises:
            ImageProcessingError: 图片处理失败
            InsufficientDataError: 图片不足
        """
        # 下载图片
        images = self.downloader.download_images_batch(cover_urls[:max_images])
        
        if len(images) < 6:
            raise InsufficientDataError(
                required=6,
                actual=len(images),
                data_type="可用图片"
            )
        
        # 创建画板（传入文字叠加数据）
        canvas = self.create_canvas(
            images,
            max_images,
            views_list=views_list,
            revenue_list=revenue_list
        )
        
        # 转换为 Base64
        return self.canvas_to_base64(canvas)
    
    def generate_from_videos(
        self,
        videos: List[VideoData],
        max_images: int = 12
    ) -> str:
        """
        从视频数据生成画板（自动提取播放量和成交金额叠加到封面）
        
        Args:
            videos: VideoData 列表
            max_images: 最大图片数量
            
        Returns:
            str: Base64 编码的画板
            
        Raises:
            ImageProcessingError: 图片处理失败
            InsufficientDataError: 图片不足
        """
        valid_videos = [v for v in videos if v.cover_url]
        
        if not valid_videos:
            raise ImageProcessingError("视频列表中无封面 URL")
        
        cover_urls = [v.cover_url for v in valid_videos]
        views_list = [getattr(v, 'views', 0) for v in valid_videos]
        revenue_list = [v.revenue for v in valid_videos]
        
        return self.generate_from_urls(
            cover_urls,
            max_images,
            views_list=views_list,
            revenue_list=revenue_list
        )


# ============================================================================
# 多画板生成器
# ============================================================================

class MultiCanvasGenerator:
    """多画板生成器（支持最多 5 张 12 宫格画板）"""
    
    def __init__(
        self,
        cell_width: int = 300,
        cell_height: int = 400,
        quality: int = 85
    ):
        self.generator = GridCanvasGenerator(cell_width, cell_height, quality)
    
    def generate_canvases(
        self,
        videos: List[VideoData],
        max_canvases: int = 2,
        min_per_canvas: int = 6
    ) -> List[str]:
        """
        生成多张画板（每张最多 12 个视频）
        
        Args:
            videos: VideoData 列表
            max_canvases: 最大画板数量（默认 2 张，即 24 个视频）
            min_per_canvas: 每张画板最少需要的图片数（默认 6）
            
        Returns:
            List[str]: Base64 编码的画板列表
            
        Raises:
            InsufficientDataError: 视频不足以生成第一张画板
        """
        # 过滤出有封面的视频
        valid_videos = [v for v in videos if v.cover_url]
        
        # 至少需要能生成第一张画板
        if len(valid_videos) < min_per_canvas:
            raise InsufficientDataError(
                required=min_per_canvas,
                actual=len(valid_videos),
                data_type="有效视频（含封面）"
            )
        
        # 切分为 Chunks（每组最多 12 个），有多少生成多少
        chunks = []
        for i in range(0, len(valid_videos), 12):
            chunk = valid_videos[i:i+12]
            # 跳过图片不足的尾部 chunk
            if len(chunk) < min_per_canvas:
                print(f"⚠️ 第 {len(chunks)+1} 张画板图片不足 {min_per_canvas} 张（仅 {len(chunk)} 张），跳过")
                break
            chunks.append(chunk)
            
            if len(chunks) >= max_canvases:
                break
        
        if not chunks:
            raise InsufficientDataError(
                required=min_per_canvas,
                actual=len(valid_videos),
                data_type="有效视频（含封面）"
            )
        
        # 生成画板
        canvases = []
        for idx, chunk in enumerate(chunks):
            try:
                canvas_b64 = self.generator.generate_from_videos(chunk, max_images=12)
                canvases.append(canvas_b64)
                print(f"✅ 画板 {idx+1}/{len(chunks)} 生成成功（{len(chunk)} 张图）")
            except Exception as e:
                # 单个画板失败不影响其他
                print(f"⚠️ 画板 {idx+1} 生成失败: {str(e)}")
                continue
        
        if not canvases:
            raise ImageProcessingError("所有画板生成失败")
        
        return canvases
    
    def save_canvases_to_files(
        self,
        canvases: List[str],
        output_dir: str,
        prefix: str = "grid"
    ) -> List[str]:
        """
        将画板保存为文件
        
        Args:
            canvases: Base64 编码的画板列表
            output_dir: 输出目录
            prefix: 文件名前缀
            
        Returns:
            List[str]: 保存的文件路径列表
        """
        from pathlib import Path
        import base64
        from io import BytesIO
        from PIL import Image
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        file_paths = []
        
        for idx, canvas_b64 in enumerate(canvases):
            try:
                # 解码 Base64
                image_data = base64.b64decode(canvas_b64)
                image = Image.open(BytesIO(image_data))
                
                # 保存文件
                file_path = output_path / f"{prefix}_{idx+1}.jpg"
                image.save(file_path, format='JPEG', quality=85)
                
                file_paths.append(str(file_path))
                print(f"✅ 画板 {idx+1} 已保存: {file_path}")
                
            except Exception as e:
                print(f"⚠️ 画板 {idx+1} 保存失败: {str(e)}")
                continue
        
        return file_paths


# ============================================================================
# 便捷函数
# ============================================================================

def generate_grids_from_videos(
    videos: List[VideoData],
    max_canvases: int = 5,
    cell_size: Tuple[int, int] = (300, 400),
    quality: int = 85,
    save_to_dir: Optional[str] = None,
    file_prefix: str = "grid"
) -> List[str]:
    """
    便捷函数：从视频列表生成多张画板
    
    Args:
        videos: VideoData 列表
        max_canvases: 最大画板数量（默认 5 张）
        cell_size: 单元格尺寸 (width, height)
        quality: JPEG 质量 (0-100)
        save_to_dir: 保存目录（如果提供，则保存为文件）
        file_prefix: 文件名前缀
        
    Returns:
        List[str]: 如果 save_to_dir 为 None，返回 Base64 编码列表；否则返回文件路径列表
        
    Raises:
        InsufficientDataError: 视频不足
        ImageProcessingError: 图片处理失败
    """
    generator = MultiCanvasGenerator(
        cell_width=cell_size[0],
        cell_height=cell_size[1],
        quality=quality
    )
    
    canvases = generator.generate_canvases(videos, max_canvases)
    
    # 如果指定了保存目录，则保存为文件
    if save_to_dir:
        return generator.save_canvases_to_files(canvases, save_to_dir, file_prefix)
    
    return canvases
