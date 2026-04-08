"""
达人风格洞察 - 基于 GMV Top 48 视频的深度商业打标

业务目标：
通过 Kalodata 页面获取达人 GMV 排名前 48 的视频，利用 oEmbed 补全封面，
在内存中拼接为 4 张"3x4 (12宫格)"的高清画板，最后通过一次多模态请求完成深度商业打标。

模块：
1. fetch_top_48_videos_from_page - 从 Kalodata 页面获取数据与封面并发补全
2. generate_four_12grids - 内存矩阵拼图
3. comprehensive_vision_analysis - 多图并发 LLM 推理
"""

import os
import sys
import json
import base64
import re
from io import BytesIO
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image

# 添加父目录到路径以支持导入
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.data_schema import (
    ApparelStyle,
    AccessoryStyle,
    PreferredCategory,
    CreatorVibeAnalysis,
)


# ============================================================================
# 模块 1: fetch_top_48_videos_from_page (从 Kalodata 页面获取数据)
# ============================================================================

def fetch_top_48_videos_from_page(
    kalodata_url: str,
    browser_snapshot: Dict,
    timeout: int = 10
) -> Tuple[str, List[Dict[str, any]]]:
    """
    从 Kalodata 页面获取达人 GMV 排名前 48 的视频，并通过 oEmbed 补全封面
    
    Args:
        kalodata_url: Kalodata 达人详情页 URL
        browser_snapshot: 浏览器页面快照（包含视频数据）
        timeout: 请求超时时间（秒）
        
    Returns:
        (tk_handle, videos) 元组：
        - tk_handle: TikTok 账号名
        - videos: 视频列表，每个元素包含：
            - video_id: 视频 ID
            - description: 视频描述
            - revenue: GMV 收入
            - cover_url: 封面图 URL（通过 oEmbed 获取）
        
    Raises:
        RuntimeError: 当数据获取失败时
    """
    import requests
    
    print(f"📡 步骤 1/3: 从 Kalodata 页面获取 Top 48 GMV 视频...")
    
    # 1. 从页面快照中提取达人信息和视频数据
    # 注意：这里需要根据实际的页面结构来解析
    # 由于我们使用 browser 工具，可以通过 JavaScript 提取数据
    
    # 从 URL 中提取达人 ID
    creator_id_match = re.search(r'id=(\d+)', kalodata_url)
    if not creator_id_match:
        raise ValueError("无法从 URL 中提取达人 ID")
    
    creator_id = creator_id_match.group(1)
    
    # 从快照中提取 TikTok handle（需要从页面解析）
    # 这里假设快照中包含了达人的 TikTok handle
    tk_handle = browser_snapshot.get('tk_handle', '')
    if not tk_handle:
        raise ValueError("无法从页面获取 TikTok handle")
    
    # 从快照中提取视频列表
    videos_data = browser_snapshot.get('videos', [])
    if not videos_data:
        raise ValueError("无法从页面获取视频数据")
    
    print(f"✅ 成功获取达人 @{tk_handle} 的 {len(videos_data)} 个视频")
    
    # 2. 并发补全封面（防风控）
    print(f"🖼️  并发补全封面图（max_workers=5）...")
    
    def fetch_cover_url(video: Dict) -> Optional[Dict]:
        """通过 oEmbed 获取单个视频的封面 URL"""
        video_id = video.get("video_id")
        if not video_id:
            return None
        
        oembed_url = f"https://www.tiktok.com/oembed?url=https://www.tiktok.com/@{tk_handle}/video/{video_id}"
        
        try:
            resp = requests.get(oembed_url, timeout=5)
            resp.raise_for_status()
            oembed_data = resp.json()
            
            cover_url = oembed_data.get("thumbnail_url")
            if not cover_url:
                return None
            
            return {
                "video_id": video_id,
                "description": video.get("description", ""),
                "revenue": video.get("revenue", 0),
                "cover_url": cover_url
            }
            
        except Exception as e:
            print(f"⚠️  视频 {video_id} 封面获取失败: {str(e)}")
            return None
    
    # 并发执行（限制 5 个并发）
    valid_videos = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_cover_url, video): video for video in videos_data[:48]}
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                valid_videos.append(result)
    
    print(f"✅ 成功补全 {len(valid_videos)} 个视频封面")
    
    if len(valid_videos) < 12:
        raise RuntimeError(f"有效视频数量不足（仅 {len(valid_videos)} 个），无法生成画板")
    
    return tk_handle, valid_videos


def extract_videos_from_kalodata_page(page_html: str) -> Tuple[str, List[Dict]]:
    """
    从 Kalodata 页面 HTML 中提取视频数据
    
    这是一个辅助函数，用于解析页面中的视频数据
    实际使用时需要通过 browser 工具的 JavaScript 执行来获取数据
    """
    # 这里需要根据实际的页面结构来实现
    # 通常 Kalodata 会在页面中嵌入 JSON 数据
    pass


# ============================================================================
# 模块 2: generate_four_12grids (内存矩阵拼图)
# ============================================================================

def generate_four_12grids(
    videos: List[Dict[str, any]],
    cell_width: int = 300,
    cell_height: int = 400
) -> List[str]:
    """
    将视频封面拼接为最多 4 张 3x4 宫格画板，返回 Base64 编码
    
    Args:
        videos: 视频列表（包含 cover_url）
        cell_width: 单元格宽度（默认 300px）
        cell_height: 单元格高度（默认 400px）
        
    Returns:
        Base64 编码的 JPEG 字符串列表（最多 4 张）
        
    Raises:
        RuntimeError: 当图片下载或拼接失败时
    """
    import requests
    
    print(f"🎨 步骤 2/3: 生成 3x4 宫格画板...")
    
    # 1. 下载所有封面图到内存
    print(f"📥 下载 {len(videos)} 张封面图...")
    images = []
    
    for i, video in enumerate(videos):
        try:
            response = requests.get(video["cover_url"], timeout=10)
            response.raise_for_status()
            
            img = Image.open(BytesIO(response.content))
            img = img.convert("RGB")  # 确保是 RGB 模式
            img = img.resize((cell_width, cell_height), Image.Resampling.LANCZOS)
            images.append(img)
            
        except Exception as e:
            print(f"⚠️  封面 {i+1} 下载失败: {str(e)}")
            # 创建占位空白图
            placeholder = Image.new("RGB", (cell_width, cell_height), color=(240, 240, 240))
            images.append(placeholder)
    
    print(f"✅ 成功加载 {len(images)} 张图片")
    
    # 2. 切分为最多 4 个 Chunk（每组最多 12 张）
    chunks = [images[i:i+12] for i in range(0, len(images), 12)]
    chunks = chunks[:4]  # 最多 4 张画板
    
    print(f"📊 切分为 {len(chunks)} 个画板")
    
    # 3. 生成 3x4 矩阵画板
    canvas_width = cell_width * 3  # 900px
    canvas_height = cell_height * 4  # 1600px
    
    base64_canvases = []
    
    for chunk_idx, chunk in enumerate(chunks):
        # 创建白色背景画布
        canvas = Image.new("RGB", (canvas_width, canvas_height), color=(255, 255, 255))
        
        # 按 3列 x 4行 粘贴图片
        for idx, img in enumerate(chunk):
            row = idx // 3  # 行号 (0-3)
            col = idx % 3   # 列号 (0-2)
            
            x = col * cell_width
            y = row * cell_height
            
            canvas.paste(img, (x, y))
        
        # 转换为 Base64
        buffer = BytesIO()
        canvas.save(buffer, format="JPEG", quality=85)
        buffer.seek(0)
        
        base64_str = base64.b64encode(buffer.read()).decode("utf-8")
        base64_canvases.append(base64_str)
        
        print(f"✅ 画板 {chunk_idx + 1}: {len(chunk)} 张图片")
    
    print(f"✅ 成功生成 {len(base64_canvases)} 张画板")
    
    return base64_canvases


# ============================================================================
# 模块 3: comprehensive_vision_analysis (多图并发 LLM 推理)
# ============================================================================

VISION_ANALYSIS_PROMPT = """你是一个顶尖的电商操盘手和时尚风格分析师。

附件是该达人历史变现能力最强的 48 个视频（分布在 {num_canvases} 张画板上）。

**视频描述高频词**：
{high_freq_keywords}

**分析任务**：
请综合分析她高频带货的商品特征、出镜穿搭的长期调性，以及配饰（脖颈、耳部、手腕）的偏好款式。

**输出要求**：
1. **女装风格** (ai_apparel_style)：从以下选项中选择最匹配的一个
   - Y2K_Spicy（Y2K 辣妹风）
   - Minimalist（极简风）
   - Sweet（甜美风）
   - Streetwear（街头风）
   - Elegant（优雅风）
   - Vacation（度假风）

2. **配饰风格** (ai_accessory_style)：从以下选项中选择最匹配的一个
   - Dainty_Minimalist（精致极简）
   - Statement_Chunky（夸张粗犷）
   - Bling_Sparkle（闪耀亮片）
   - Vintage_Pearl（复古珍珠）
   - Boho_Colorful（波西米亚）
   - Sweet_Kawaii（甜美可爱）

3. **历史带货品类** (historical_product_category)：从以下选项中选择最匹配的一个
   - Apparel_Top（上衣为主）
   - Apparel_Dress（连衣裙为主）
   - Accessories_Neck_Ear（项链耳饰为主）
   - Accessories_Bag_Hand（包包手饰为主）
   - Mixed_Fashion（混合搭配）

**输出格式**（必须严格遵守 JSON 格式）：
{{
  "ai_apparel_style": "选择的女装风格",
  "ai_accessory_style": "选择的配饰风格",
  "historical_product_category": "选择的品类偏好",
  "analysis_reason": "简要说明你的判断理由（3-5句话，提及具体的视觉元素和商业特征）"
}}

**重要**：
- 必须从上述受控词表中选择，不能自创标签
- 输出必须是有效的 JSON 格式
- 重点关注高 GMV 视频中的商品特征和穿搭调性
"""


def comprehensive_vision_analysis(
    videos: List[Dict[str, any]],
    base64_canvases: List[str],
    vision_api_key: Optional[str] = None,
    vision_model: str = "claude-3-5-sonnet-20241022"
) -> Dict[str, str]:
    """
    通过多图并发 LLM 推理完成深度商业打标
    
    Args:
        videos: 视频列表（包含 description）
        base64_canvases: Base64 编码的画板列表
        vision_api_key: Vision API Key（可选，从环境变量读取）
        vision_model: Vision 模型名称
        
    Returns:
        分析结果字典，包含：
        - ai_apparel_style: 女装风格
        - ai_accessory_style: 配饰风格
        - historical_product_category: 历史带货品类
        - analysis_reason: 分析理由
        
    Raises:
        RuntimeError: 当 API 调用失败时
    """
    import requests
    
    print(f"🤖 步骤 3/3: 调用 Vision LLM 进行深度分析...")
    
    # 1. 提取高频关键词
    descriptions = [v.get("description", "") for v in videos]
    all_text = " ".join(descriptions)
    
    # 简单的高频词提取
    words = all_text.split()
    word_freq = {}
    for word in words:
        if len(word) > 2:  # 过滤短词
            word_freq[word] = word_freq.get(word, 0) + 1
    
    top_keywords = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:20]
    high_freq_keywords = ", ".join([f"{word}({count})" for word, count in top_keywords])
    
    print(f"📝 高频关键词: {high_freq_keywords[:100]}...")
    
    # 2. 构建 Vision API 请求
    api_key = vision_api_key or os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY 未设置")
    
    # 组装消息内容（多张图片 + 文本）
    content = [
        {
            "type": "text",
            "text": VISION_ANALYSIS_PROMPT.format(
                num_canvases=len(base64_canvases),
                high_freq_keywords=high_freq_keywords
            )
        }
    ]
    
    # 添加所有画板图片
    for i, canvas_b64 in enumerate(base64_canvases):
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": canvas_b64
            }
        })
    
    # 3. 调用 Claude Vision API
    api_url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    
    payload = {
        "model": vision_model,
        "max_tokens": 1024,
        "messages": [
            {
                "role": "user",
                "content": content
            }
        ]
    }
    
    try:
        print(f"🔄 调用 {vision_model}...")
        response = requests.post(
            api_url,
            headers=headers,
            json=payload,
            timeout=60
        )
        response.raise_for_status()
        
        result = response.json()
        assistant_message = result["content"][0]["text"]
        
        print(f"✅ LLM 响应成功")
        print(f"📄 原始响应: {assistant_message[:200]}...")
        
        # 4. 解析 JSON 响应
        if "```json" in assistant_message:
            json_str = assistant_message.split("```json")[1].split("```")[0].strip()
        elif "```" in assistant_message:
            json_str = assistant_message.split("```")[1].split("```")[0].strip()
        else:
            json_str = assistant_message.strip()
        
        analysis_result = json.loads(json_str)
        
        print(f"✅ 分析完成")
        print(f"   女装风格: {analysis_result['ai_apparel_style']}")
        print(f"   配饰风格: {analysis_result['ai_accessory_style']}")
        print(f"   历史品类: {analysis_result['historical_product_category']}")
        
        return analysis_result
        
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Vision API 调用失败: {str(e)}")
    except (json.JSONDecodeError, KeyError) as e:
        raise RuntimeError(f"响应解析失败: {str(e)}\n原始响应: {assistant_message}")


# ============================================================================
# 主流程：完整的达人风格洞察（通过 OpenClaw browser 工具）
# ============================================================================

def analyze_creator_from_kalodata(
    kalodata_url: str,
    vision_api_key: Optional[str] = None
) -> CreatorVibeAnalysis:
    """
    基于 Kalodata 页面的 GMV Top 48 视频深度商业打标
    
    注意：此函数需要在 OpenClaw 环境中使用 browser 工具
    
    Args:
        kalodata_url: Kalodata 达人详情页 URL
        vision_api_key: Vision API Key
        
    Returns:
        CreatorVibeAnalysis: 达人风格分析结果
        
    Raises:
        RuntimeError: 当任何步骤失败时
    """
    print("=" * 80)
    print(f"🎯 达人风格洞察 - 基于 Kalodata 页面")
    print("=" * 80)
    print()
    
    # 注意：这个函数需要从外部传入浏览器数据
    # 实际使用时，需要先用 browser 工具打开页面并提取数据
    raise NotImplementedError(
        "此函数需要在 OpenClaw 环境中配合 browser 工具使用。\n"
        "请参考 analyze_creator_gmv_workflow() 函数的实现。"
    )


# ============================================================================
# 简化的工作流（供 OpenClaw 调用）
# ============================================================================

def analyze_creator_gmv_workflow(
    tk_handle: str,
    videos_data: List[Dict],
    vision_api_key: Optional[str] = None
) -> CreatorVibeAnalysis:
    """
    简化的工作流：直接接收视频数据进行分析
    
    Args:
        tk_handle: TikTok 账号名
        videos_data: 视频数据列表，每个元素包含 video_id, description, revenue
        vision_api_key: Vision API Key
        
    Returns:
        CreatorVibeAnalysis: 达人风格分析结果
    """
    print("=" * 80)
    print(f"🎯 达人风格洞察 - @{tk_handle}")
    print("=" * 80)
    print()
    
    # 步骤 1: 补全封面
    print(f"📡 步骤 1/3: 补全 {len(videos_data)} 个视频的封面...")
    _, videos = fetch_top_48_videos_from_page(
        kalodata_url="",  # 不需要 URL
        browser_snapshot={"tk_handle": tk_handle, "videos": videos_data},
        timeout=10
    )
    print()
    
    # 步骤 2: 生成画板
    base64_canvases = generate_four_12grids(videos)
    print()
    
    # 步骤 3: Vision 分析
    analysis_result = comprehensive_vision_analysis(
        videos,
        base64_canvases,
        vision_api_key
    )
    print()
    
    # 步骤 4: 构建结果
    print("✅ 步骤 4/4: 验证并构建结果...")
    result = CreatorVibeAnalysis(
        tk_handle=f"@{tk_handle}",
        ai_apparel_style=ApparelStyle(analysis_result["ai_apparel_style"]),
        ai_accessory_style=AccessoryStyle(analysis_result["ai_accessory_style"]),
        preferred_category=PreferredCategory(analysis_result["historical_product_category"]),
        analysis_reason=analysis_result["analysis_reason"],
        bio="",
        followers_count=0
    )
    
    print("✅ 分析完成")
    print()
    print("=" * 80)
    print("📊 分析结果")
    print("=" * 80)
    print(f"TikTok: {result.tk_handle}")
    print(f"女装风格: {result.ai_apparel_style.value}")
    print(f"配饰风格: {result.ai_accessory_style.value}")
    print(f"历史品类: {result.preferred_category.value}")
    print(f"分析理由: {result.analysis_reason}")
    print("=" * 80)
    
    return result
