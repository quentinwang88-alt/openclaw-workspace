"""
Skill 1 Enhanced: analyze_creator_vibe_kalodata - 基于 Kalodata + TikTok oEmbed 的深度商业打标

业务目标：
通过第三方数据平台获取达人 GMV 排名前 48 的视频，利用 oEmbed 补全封面，
在内存中拼接为 4 张"3x4 (12宫格)"的高清画板，最后通过一次多模态请求完成深度商业打标。

模块架构：
1. fetch_top_48_videos: 获取数据与封面并发补全
2. generate_four_12grids: 内存矩阵拼图
3. comprehensive_vision_analysis: 多图并发 LLM 推理
"""

import sys
import os
import json
import base64
import requests
from io import BytesIO
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image, ImageDraw, ImageFont
from collections import Counter

# 添加父目录到路径以支持导入
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.data_schema import (
    CreatorVibeAnalysis,
    ApparelStyle,
    AccessoryStyle,
    PreferredCategory,
)


# ============================================================================
# 数据结构定义
# ============================================================================

class VideoData:
    """视频数据结构"""
    def __init__(
        self,
        video_id: str,
        description: str,
        revenue: float,
        cover_url: Optional[str] = None
    ):
        self.video_id = video_id
        self.description = description
        self.revenue = revenue
        self.cover_url = cover_url
    
    def __repr__(self) -> str:
        return f"VideoData(id={self.video_id}, revenue={self.revenue}, has_cover={self.cover_url is not None})"


# ============================================================================
# 模块 1: fetch_top_48_videos (获取数据与封面并发补全)
# ============================================================================

def fetch_top_48_videos(
    tk_handle: str,
    kalodata_api_url: str,
    kalodata_api_key: str,
    max_workers: int = 5,
    timeout: int = 5
) -> List[VideoData]:
    """
    获取达人 GMV 排名前 48 的视频，并并发补全封面
    
    Args:
        tk_handle: TikTok 账号名（不带 @）
        kalodata_api_url: Kalodata API 端点
        kalodata_api_key: Kalodata API Key
        max_workers: 并发线程数（默认 5，避免被 TikTok 拦截）
        timeout: 单个请求超时时间（秒）
    
    Returns:
        List[VideoData]: 视频数据列表（最多 48 个）
    
    Raises:
        RuntimeError: 当 Kalodata API 调用失败时
    """
    print(f"📡 模块 1: 获取达人 @{tk_handle} 的 Top 48 视频")
    print("-" * 60)
    
    # 步骤 1: 请求 Kalodata API
    print("🔍 步骤 1/2: 调用 Kalodata API...")
    
    payload = {
        "creator_handle": tk_handle,
        "pageSize": 48,
        "sort": [{"field": "revenue", "type": "DESC"}]
    }
    
    headers = {
        "Authorization": f"Bearer {kalodata_api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(
            kalodata_api_url,
            json=payload,
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        
        # 提取视频列表
        videos_raw = data.get("data", {}).get("videos", [])
        
        if not videos_raw:
            raise RuntimeError("Kalodata API 返回空数据")
        
        print(f"✅ 成功获取 {len(videos_raw)} 个视频")
        
        # 构建 VideoData 对象
        videos = [
            VideoData(
                video_id=v.get("id"),
                description=v.get("description", ""),
                revenue=v.get("revenue", 0.0)
            )
            for v in videos_raw[:48]  # 确保最多 48 个
        ]
        
    except Exception as e:
        raise RuntimeError(f"Kalodata API 调用失败: {str(e)}")
    
    # 步骤 2: 并发补全封面（重点防风控）
    print(f"\n🖼️  步骤 2/2: 并发补全封面（max_workers={max_workers}）...")
    
    def fetch_cover_url(video: VideoData) -> VideoData:
        """
        通过 TikTok oEmbed API 获取视频封面
        
        Args:
            video: VideoData 对象
        
        Returns:
            VideoData: 更新了 cover_url 的对象
        """
        oembed_url = f"https://www.tiktok.com/oembed?url=https://www.tiktok.com/@{tk_handle}/video/{video.video_id}"
        
        try:
            resp = requests.get(oembed_url, timeout=timeout)
            resp.raise_for_status()
            oembed_data = resp.json()
            
            # 提取封面 URL
            cover_url = oembed_data.get("thumbnail_url")
            if cover_url:
                video.cover_url = cover_url
                return video
            else:
                print(f"⚠️  视频 {video.video_id} 无封面 URL")
                return video
                
        except Exception as e:
            print(f"⚠️  视频 {video.video_id} 获取封面失败: {str(e)}")
            return video
    
    # 并发执行
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_cover_url, video): video for video in videos}
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            if completed % 10 == 0:
                print(f"   进度: {completed}/{len(videos)}")
    
    # 统计成功率
    valid_videos = [v for v in videos if v.cover_url is not None]
    print(f"\n✅ 封面补全完成: {len(valid_videos)}/{len(videos)} 成功")
    
    if len(valid_videos) < 12:
        print(f"⚠️  警告: 有效视频不足 12 个，可能影响分析质量")
    
    return valid_videos


# ============================================================================
# 模块 2: generate_four_12grids (内存矩阵拼图)
# ============================================================================

def generate_four_12grids(
    videos: List[VideoData],
    grid_size: Tuple[int, int] = (300, 400),
    canvas_size: Tuple[int, int] = (900, 1600)
) -> List[str]:
    """
    将视频封面拼接为 4 张 3x4 (12宫格) 画板，返回 Base64 编码
    
    Args:
        videos: 视频数据列表（必须包含 cover_url）
        grid_size: 单个格子尺寸 (width, height)，默认 300x400
        canvas_size: 画布尺寸 (width, height)，默认 900x1600
    
    Returns:
        List[str]: Base64 编码的 JPEG 字符串列表（最多 4 张）
    
    Raises:
        ValueError: 当输入参数无效时
    """
    print(f"\n🎨 模块 2: 生成 3x4 矩阵画板")
    print("-" * 60)
    
    if not videos:
        raise ValueError("视频列表为空")
    
    # 过滤出有封面的视频
    valid_videos = [v for v in videos if v.cover_url]
    
    if not valid_videos:
        raise ValueError("没有有效的视频封面")
    
    print(f"📊 有效视频数: {len(valid_videos)}")
    
    # 步骤 1: 下载所有封面图到内存
    print("📥 步骤 1/3: 下载封面图到内存...")
    
    images: List[Image.Image] = []
    
    for i, video in enumerate(valid_videos):
        try:
            resp = requests.get(video.cover_url, timeout=10)
            resp.raise_for_status()
            img = Image.open(BytesIO(resp.content))
            
            # 转换为 RGB（防止 RGBA 或其他格式）
            if img.mode != 'RGB':
                img = img.convert('RGB')
            
            images.append(img)
            
            if (i + 1) % 12 == 0:
                print(f"   已下载: {i + 1}/{len(valid_videos)}")
                
        except Exception as e:
            print(f"⚠️  视频 {video.video_id} 下载封面失败: {str(e)}")
            continue
    
    print(f"✅ 成功下载 {len(images)} 张图片")
    
    # 步骤 2: 切分为最多 4 个 Chunk（每组最多 12 张）
    print("\n✂️  步骤 2/3: 切分为 Chunks...")
    
    chunks: List[List[Image.Image]] = []
    for i in range(0, len(images), 12):
        chunk = images[i:i+12]
        chunks.append(chunk)
        print(f"   Chunk {len(chunks)}: {len(chunk)} 张图片")
    
    # 步骤 3: 生成 3x4 矩阵画板
    print("\n🖼️  步骤 3/3: 生成画板...")
    
    canvas_list: List[str] = []
    
    for chunk_idx, chunk in enumerate(chunks):
        # 创建透明背景画布
        canvas = Image.new('RGB', canvas_size, color=(255, 255, 255))
        
        # 计算实际需要的行数（向上取整）
        num_images = len(chunk)
        rows_needed = (num_images + 2) // 3  # 每行 3 张，向上取整
        
        # 粘贴图片
        for idx, img in enumerate(chunk):
            # 计算位置（3 列 x 4 行）
            col = idx % 3
            row = idx // 3
            
            # 防止超出画布范围
            if row >= 4:
                print(f"⚠️  警告: Chunk {chunk_idx + 1} 图片数超过 12，已截断")
                break
            
            # Resize 图片
            img_resized = img.resize(grid_size, Image.Resampling.LANCZOS)
            
            # 计算粘贴位置
            x = col * grid_size[0]
            y = row * grid_size[1]
            
            # 粘贴到画布
            canvas.paste(img_resized, (x, y))
        
        # 转换为 Base64
        buffer = BytesIO()
        canvas.save(buffer, format='JPEG', quality=90)
        canvas_b64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
        
        canvas_list.append(canvas_b64)
        print(f"✅ 画板 {chunk_idx + 1}: {len(chunk)} 张图片 → Base64 ({len(canvas_b64)} 字符)")
    
    print(f"\n✅ 共生成 {len(canvas_list)} 张画板")
    
    return canvas_list


# ============================================================================
# 模块 3: comprehensive_vision_analysis (多图并发 LLM 推理)
# ============================================================================

def comprehensive_vision_analysis(
    tk_handle: str,
    videos: List[VideoData],
    canvas_list: List[str],
    vision_api_key: str,
    vision_api_url: str = "https://api.anthropic.com/v1/messages",
    model: str = "claude-3-5-sonnet-20241022"
) -> Dict[str, str]:
    """
    通过多图并发 LLM 推理完成深度商业打标
    
    Args:
        tk_handle: TikTok 账号名
        videos: 视频数据列表（用于提取文本标签）
        canvas_list: Base64 编码的画板列表
        vision_api_key: Vision API Key
        vision_api_url: Vision API 端点
        model: 模型名称
    
    Returns:
        Dict[str, str]: 分析结果字典，包含 ai_apparel_style, ai_accessory_style, 
                       preferred_category, analysis_reason
    
    Raises:
        RuntimeError: 当 Vision API 调用失败时
    """
    print(f"\n🤖 模块 3: 多图并发 LLM 推理")
    print("-" * 60)
    
    # 步骤 1: 提取高频文本标签
    print("📝 步骤 1/3: 提取视频描述高频词...")
    
    all_descriptions = " ".join([v.description for v in videos if v.description])
    
    # 简单的高频词提取（可以用更复杂的 NLP 方法）
    words = all_descriptions.lower().split()
    word_freq = Counter(words)
    top_keywords = [word for word, count in word_freq.most_common(20)]
    
    keywords_summary = ", ".join(top_keywords)
    print(f"   高频关键词: {keywords_summary[:100]}...")
    
    # 步骤 2: 构建 Vision LLM 请求
    print("\n🔧 步骤 2/3: 构建 Vision LLM 请求...")
    
    # 构建 content 数组（文本 + 多张图片）
    content = [
        {
            "type": "text",
            "text": f"""你是一个顶尖的电商操盘手。附件是达人 @{tk_handle} 历史变现能力最强的 48 个视频（分布在 {len(canvas_list)} 张画板上）。

**视频描述高频关键词**：
{keywords_summary}

**分析任务**：
请综合分析她高频带货的商品特征、出镜穿搭的长期调性，以及配饰（脖颈、耳部、手腕）的偏好款式。

**输出要求**：
严格返回 JSON 格式，包含以下字段（必须从受控词表中选择）：

1. **ai_apparel_style**（女装风格）：
   - Y2K_Spicy（Y2K 辣妹风）
   - Minimalist（极简风）
   - Sweet（甜美风）
   - Streetwear（街头风）
   - Elegant（优雅风）
   - Vacation（度假风）

2. **ai_accessory_style**（配饰风格）：
   - Dainty_Minimalist（精致极简）
   - Statement_Chunky（夸张粗犷）
   - Bling_Sparkle（闪耀亮片）
   - Vintage_Pearl（复古珍珠）
   - Boho_Colorful（波西米亚）
   - Sweet_Kawaii（甜美可爱）

3. **preferred_category**（品类偏好）：
   - Apparel_Top（上衣为主）
   - Apparel_Dress（连衣裙为主）
   - Accessories_Neck_Ear（项链耳饰为主）
   - Accessories_Bag_Hand（包包手饰为主）
   - Mixed_Fashion（混合搭配）

4. **analysis_reason**（分析理由）：
   简要说明你的判断依据（2-3句话，用于后续生成邀约信）

**输出格式**（必须是有效的 JSON）：
{{
  "ai_apparel_style": "选择的女装风格",
  "ai_accessory_style": "选择的配饰风格",
  "preferred_category": "选择的品类偏好",
  "analysis_reason": "你的判断理由"
}}"""
        }
    ]
    
    # 添加所有画板图片
    for idx, canvas_b64 in enumerate(canvas_list):
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": canvas_b64
            }
        })
    
    print(f"   已添加 {len(canvas_list)} 张画板图片")
    
    # 步骤 3: 调用 Vision API
    print("\n🚀 步骤 3/3: 调用 Vision API...")
    
    headers = {
        "x-api-key": vision_api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    
    payload = {
        "model": model,
        "max_tokens": 1024,
        "messages": [
            {
                "role": "user",
                "content": content
            }
        ]
    }
    
    try:
        response = requests.post(
            vision_api_url,
            json=payload,
            headers=headers,
            timeout=60
        )
        response.raise_for_status()
        result = response.json()
        
        # 提取响应文本
        response_text = result["content"][0]["text"]
        print(f"✅ Vision API 响应成功")
        print(f"   响应长度: {len(response_text)} 字符")
        
        # 解析 JSON
        # 尝试提取 JSON（可能被包裹在 markdown 代码块中）
        if "```json" in response_text:
            json_str = response_text.split("```json")[1].split("```")[0].strip()
        elif "```" in response_text:
            json_str = response_text.split("```")[1].split("```")[0].strip()
        else:
            json_str = response_text.strip()
        
        analysis_result = json.loads(json_str)
        
        print(f"\n📊 分析结果:")
        print(f"   女装风格: {analysis_result['ai_apparel_style']}")
        print(f"   配饰风格: {analysis_result['ai_accessory_style']}")
        print(f"   品类偏好: {analysis_result['preferred_category']}")
        print(f"   分析理由: {analysis_result['analysis_reason'][:100]}...")
        
        return analysis_result
        
    except Exception as e:
        raise RuntimeError(f"Vision API 调用失败: {str(e)}")


# ============================================================================
# 主流程：整合三个模块
# ============================================================================

def analyze_creator_vibe_kalodata(
    tk_handle: str,
    kalodata_api_url: str,
    kalodata_api_key: str,
    vision_api_key: str,
    vision_api_url: str = "https://api.anthropic.com/v1/messages",
    vision_model: str = "claude-3-5-sonnet-20241022"
) -> CreatorVibeAnalysis:
    """
    基于 Kalodata + TikTok oEmbed 的深度商业打标（完整流程）
    
    Args:
        tk_handle: TikTok 账号名（不带 @）
        kalodata_api_url: Kalodata API 端点
        kalodata_api_key: Kalodata API Key
        vision_api_key: Vision API Key
        vision_api_url: Vision API 端点
        vision_model: Vision 模型名称
    
    Returns:
        CreatorVibeAnalysis: 达人风格分析结果
    
    Raises:
        RuntimeError: 当任何模块调用失败时
    """
    print("=" * 60)
    print(f"🎯 达人深度商业打标 - @{tk_handle}")
    print("=" * 60)
    print()
    
    # 模块 1: 获取 Top 48 视频
    videos = fetch_top_48_videos(
        tk_handle=tk_handle,
        kalodata_api_url=kalodata_api_url,
        kalodata_api_key=kalodata_api_key
    )
    
    # 模块 2: 生成 4 张 12 宫格画板
    canvas_list = generate_four_12grids(videos)
    
    # 模块 3: 多图并发 LLM 推理
    analysis_result = comprehensive_vision_analysis(
        tk_handle=tk_handle,
        videos=videos,
        canvas_list=canvas_list,
        vision_api_key=vision_api_key,
        vision_api_url=vision_api_url,
        model=vision_model
    )
    
    # 构建最终结果
    print("\n✅ 步骤 4/4: 验证并构建结果...")
    
    try:
        result = CreatorVibeAnalysis(
            tk_handle=f"@{tk_handle}",
            ai_apparel_style=ApparelStyle(analysis_result["ai_apparel_style"]),
            ai_accessory_style=AccessoryStyle(analysis_result["ai_accessory_style"]),
            preferred_category=PreferredCategory(analysis_result["preferred_category"]),
            analysis_reason=analysis_result["analysis_reason"],
            bio="",  # Kalodata 可能不提供 bio
            followers_count=0  # Kalodata 可能不提供粉丝数
        )
        
        print("✅ 分析完成")
        print()
        return result
        
    except ValueError as e:
        raise ValueError(f"风格标签验证失败: {str(e)}")


# ============================================================================
# 测试与示例
# ============================================================================

def _test_analyze_creator_vibe_kalodata():
    """测试完整流程"""
    
    print("=" * 60)
    print("🚀 Skill 1 Enhanced: analyze_creator_vibe_kalodata 测试")
    print("=" * 60)
    print()
    
    # 配置（需要替换为实际的 API Key）
    KALODATA_API_URL = "https://api.kalodata.com/v1/creator/videos"
    KALODATA_API_KEY = os.getenv("KALODATA_API_KEY", "YOUR_KALODATA_API_KEY")
    VISION_API_KEY = os.getenv("ANTHROPIC_API_KEY", "YOUR_ANTHROPIC_API_KEY")
    
    # 测试达人
    test_handle = "sopxinh2"
    
    try:
        result = analyze_creator_vibe_kalodata(
            tk_handle=test_handle,
            kalodata_api_url=KALODATA_API_URL,
            kalodata_api_key=KALODATA_API_KEY,
            vision_api_key=VISION_API_KEY
        )
        
        print(f"✅ 分析成功")
        print(f"   TikTok: {result.tk_handle}")
        print(f"   女装风格: {result.ai_apparel_style}")
        print(f"   配饰风格: {result.ai_accessory_style}")
        print(f"   品类偏好: {result.preferred_category}")
        print(f"   分析理由: {result.analysis_reason}")
        print()
        
    except Exception as e:
        print(f"❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    _test_analyze_creator_vibe_kalodata()
    print("✅ 测试完成")
