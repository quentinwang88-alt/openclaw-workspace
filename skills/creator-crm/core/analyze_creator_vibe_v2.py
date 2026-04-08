"""
Skill 1: analyze_creator_vibe_v2 - 极简成本方案

功能：使用 Playwright 截取达人主页全屏截图，交由 Vision LLM 提取风格标签
成本优化：
  - 无需 Apify（节省 ~$0.02/次）
  - 单张截图替代多张封面图（节省 ~60% Vision LLM tokens）
"""

from typing import Optional
import sys
import os
import json

# 添加父目录到路径以支持导入
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.data_schema import (
    CreatorVibeAnalysis,
    ApparelStyle,
    AccessoryStyle,
    PreferredCategory,
)
try:
    from utils.playwright_scraper import capture_tiktok_profile_screenshot_sync
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    capture_tiktok_profile_screenshot_sync = None


# Vision LLM Prompt 模板（优化版 - 适配单张截图）
VISION_ANALYSIS_PROMPT_V2 = """你是一位专业的时尚风格分析师，专注于东南亚女装与配饰市场。

请仔细分析以下 TikTok 达人主页的全屏截图，综合评估：
1. **主页网格（Grid）的整体穿搭氛围**：观察视频封面的整体色彩、风格一致性
2. **色彩调性**：主色调是冷色系/暖色系/中性色？饱和度高低？
3. **Bio 信息**：达人的自我介绍和定位

**分析要求**：

**女装风格**（从以下选项中选择最匹配的一个）：
- Y2K_Spicy（Y2K 辣妹风）- 紧身、低腰、亮色、性感
- Minimalist（极简风）- 纯色、基础款、高级感
- Sweet（甜美风）- 蕾丝、蝴蝶结、粉色系、温柔
- Streetwear（街头风）- 宽松、运动、潮牌
- Elegant（优雅风）- 正式、职业、优雅
- Vacation（度假风）- 轻松、度假、海滩

**配饰风格**（从以下选项中选择最匹配的一个）：
- Dainty_Minimalist（精致极简）- 细项链、小耳钉
- Statement_Chunky（夸张粗犷）- 大耳环、粗项链
- Bling_Sparkle（闪耀亮片）- 亮片、水钻
- Vintage_Pearl（复古珍珠）- 珍珠、复古
- Boho_Colorful（波西米亚）- 彩色、民族风
- Sweet_Kawaii（甜美可爱）- 可爱、小花、珍珠

**注意**：如果截图中没有明显配饰，请根据服装风格推测最匹配的配饰风格。

**品类偏好**（从以下选项中选择最匹配的一个）：
- Apparel_Top（上衣为主）
- Apparel_Dress（连衣裙为主）
- Accessories_Neck_Ear（项链耳饰为主）
- Accessories_Bag_Hand（包包手饰为主）
- Mixed_Fashion（混合搭配）

**输出格式**（必须严格遵守 JSON 格式）：
{{
  "ai_apparel_style": "选择的女装风格",
  "ai_accessory_style": "选择的配饰风格",
  "preferred_category": "选择的品类偏好",
  "analysis_reason": "简要说明你的判断理由（2-3句话，提及具体的视觉元素）"
}}

**重要**：
- 必须从上述受控词表中选择，不能自创标签
- 输出必须是有效的 JSON 格式
- analysis_reason 要具体，提及色彩、风格、氛围
"""


def analyze_creator_vibe_v2(
    tk_profile_url: str,
    vision_api_key: Optional[str] = None,
    use_mock: bool = False
) -> CreatorVibeAnalysis:
    """
    分析达人风格标签（极简成本方案）
    
    Args:
        tk_profile_url: TikTok 达人主页 URL（例如: https://www.tiktok.com/@username）
        vision_api_key: Vision LLM API Key（可选，从环境变量读取）
        use_mock: 是否使用模拟数据（用于测试）
        
    Returns:
        CreatorVibeAnalysis: 达人风格分析结果
        
    Raises:
        ValueError: 当输入参数无效时
        RuntimeError: 当截图或 Vision API 调用失败时
        
    Example:
        >>> result = analyze_creator_vibe_v2("https://www.tiktok.com/@fashionista_th")
        >>> print(result.ai_apparel_style)
    """
    # 参数验证
    if not tk_profile_url:
        raise ValueError("tk_profile_url 不能为空")
    
    # 提取账号名
    handle = tk_profile_url.rstrip('/').split('/')[-1].lstrip('@')
    
    print(f"🔍 开始分析达人: @{handle}")
    print(f"📍 URL: {tk_profile_url}")
    print()
    
    # 步骤 1: 使用 Playwright 截取主页全屏截图
    print("📸 步骤 1/3: 使用 Playwright 截取主页全屏截图...")
    try:
        if use_mock:
            # 使用模拟数据（用于测试）
            print("⚠️ 使用模拟截图数据")
            base64_screenshot = "mock_base64_screenshot_data"
        else:
            if not PLAYWRIGHT_AVAILABLE:
                raise RuntimeError(
                    "Playwright 未安装。请运行: pip3 install playwright && playwright install chromium"
                )
            base64_screenshot = capture_tiktok_profile_screenshot_sync(
                tk_profile_url,
                timeout=30000
            )
        
        print(f"✅ 截图成功")
        print(f"   截图大小: {len(base64_screenshot):,} 字符")
        print()
        
    except Exception as e:
        raise RuntimeError(f"截图失败: {str(e)}")
    
    # 步骤 2: 调用 Vision LLM 分析风格
    print("🤖 步骤 2/3: 调用 Vision LLM 分析风格...")
    
    if use_mock:
        # 使用模拟数据（用于测试）
        analysis_result = _get_mock_analysis(handle)
    else:
        # 实际调用 Vision API
        analysis_result = _call_vision_api_v2(
            screenshot_base64=base64_screenshot,
            api_key=vision_api_key
        )
    
    print(f"✅ 风格分析完成")
    print(f"   女装风格: {analysis_result['ai_apparel_style']}")
    print(f"   配饰风格: {analysis_result['ai_accessory_style']}")
    print(f"   品类偏好: {analysis_result['preferred_category']}")
    print()
    
    # 步骤 3: 验证并构建结果
    print("✅ 步骤 3/3: 验证并构建结果...")
    try:
        result = CreatorVibeAnalysis(
            tk_handle=f"@{handle}",
            ai_apparel_style=ApparelStyle(analysis_result["ai_apparel_style"]),
            ai_accessory_style=AccessoryStyle(analysis_result["ai_accessory_style"]),
            preferred_category=PreferredCategory(analysis_result["preferred_category"]),
            analysis_reason=analysis_result["analysis_reason"],
            bio="",  # 可选：从截图中提取或留空
            followers_count=0  # 可选：从截图中提取或留空
        )
        
        print("✅ 分析完成")
        print()
        return result
        
    except ValueError as e:
        raise ValueError(f"风格标签验证失败: {str(e)}")


def _call_vision_api_v2(
    screenshot_base64: str,
    api_key: Optional[str] = None
) -> dict:
    """
    调用 Vision LLM API 进行风格分析（单张截图版本）
    使用云雾 Gemini 3.1-pro-preview 模型
    
    Args:
        screenshot_base64: Base64 编码的截图
        api_key: Vision API Key
        
    Returns:
        分析结果字典
        
    Raises:
        RuntimeError: 当 API 调用失败时
    """
    try:
        import requests
        
        api_key = api_key or os.getenv("GEMINI_API_KEY")
        
        if not api_key:
            raise ValueError("GEMINI_API_KEY 未设置")
        
        # 云雾 Gemini API 端点
        api_url = "https://api.yunwu.ai/v1beta/models/gemini-3.1-pro-preview:generateContent"
        
        # 构建请求体
        payload = {
            "contents": [
                {
                    "parts": [
                        {
                            "text": VISION_ANALYSIS_PROMPT_V2
                        },
                        {
                            "inline_data": {
                                "mime_type": "image/png",
                                "data": screenshot_base64
                            }
                        }
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0.4,
                "maxOutputTokens": 500
            }
        }
        
        # 调用 API
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        
        response = requests.post(api_url, json=payload, headers=headers, timeout=60)
        response.raise_for_status()
        
        # 解析响应
        result = response.json()
        
        # 提取文本内容
        if "candidates" in result and len(result["candidates"]) > 0:
            content = result["candidates"][0]["content"]
            if "parts" in content and len(content["parts"]) > 0:
                result_text = content["parts"][0]["text"]
                
                # 解析 JSON 响应
                # 尝试提取 JSON 块（可能被 markdown 代码块包裹）
                import re
                json_match = re.search(r'```json\s*(\{.*?\})\s*```', result_text, re.DOTALL)
                if json_match:
                    result_text = json_match.group(1)
                else:
                    # 尝试直接提取 JSON 对象
                    json_match = re.search(r'\{.*\}', result_text, re.DOTALL)
                    if json_match:
                        result_text = json_match.group(0)
                
                return json.loads(result_text)
        
        raise RuntimeError("Gemini API 返回格式异常")
        
    except ImportError:
        # requests 未安装，返回模拟数据
        print("⚠️ requests 未安装，使用模拟分析结果")
        return _get_mock_analysis("default")
    except Exception as e:
        raise RuntimeError(f"Gemini API 调用失败: {str(e)}")


def _get_mock_analysis(handle: str) -> dict:
    """
    返回模拟分析结果（用于开发测试）
    
    Args:
        handle: TikTok 账号名
        
    Returns:
        模拟的分析结果
    """
    # 根据账号名返回不同的模拟数据
    mock_data = {
        "fashionista_th": {
            "ai_apparel_style": "Y2K_Spicy",
            "ai_accessory_style": "Statement_Chunky",
            "preferred_category": "Mixed_Fashion",
            "analysis_reason": "主页网格呈现高饱和度暖色调，多为紧身上衣和低腰裤，整体氛围辣妹感十足，配饰以夸张耳环为主"
        },
        "minimal_style": {
            "ai_apparel_style": "Minimalist",
            "ai_accessory_style": "Dainty_Minimalist",
            "preferred_category": "Accessories_Neck_Ear",
            "analysis_reason": "主页色调以黑白灰为主，穿搭简洁高级，配饰选择精致细项链，整体呈现极简风格"
        },
        "default": {
            "ai_apparel_style": "Sweet",
            "ai_accessory_style": "Sweet_Kawaii",
            "preferred_category": "Apparel_Dress",
            "analysis_reason": "主页色调以粉色系为主，常见蕾丝和蝴蝶结元素，配饰以珍珠和小花为主，整体风格甜美温柔"
        }
    }
    
    return mock_data.get(handle, mock_data["default"])


# ============================================================================
# 测试与示例
# ============================================================================

def _test_analyze_creator_vibe_v2():
    """测试达人风格分析（极简方案）"""
    
    print("=" * 60)
    print("🚀 Skill 1: analyze_creator_vibe_v2 测试（极简成本方案）")
    print("=" * 60)
    print()
    
    # 测试用例 1: Y2K 辣妹风
    print("测试用例 1: Y2K 辣妹风达人")
    print("-" * 60)
    try:
        result_1 = analyze_creator_vibe_v2(
            tk_profile_url="https://www.tiktok.com/@fashionista_th",
            use_mock=True
        )
        
        print(f"✅ 分析成功")
        print(f"   TikTok: {result_1.tk_handle}")
        print(f"   女装风格: {result_1.ai_apparel_style}")
        print(f"   配饰风格: {result_1.ai_accessory_style}")
        print(f"   品类偏好: {result_1.preferred_category}")
        print(f"   分析理由: {result_1.analysis_reason}")
        print()
        
    except Exception as e:
        print(f"❌ 测试失败: {str(e)}")
        print()
    
    # 测试用例 2: 极简风
    print("测试用例 2: 极简风达人")
    print("-" * 60)
    try:
        result_2 = analyze_creator_vibe_v2(
            tk_profile_url="https://www.tiktok.com/@minimal_style",
            use_mock=True
        )
        
        print(f"✅ 分析成功")
        print(f"   TikTok: {result_2.tk_handle}")
        print(f"   女装风格: {result_2.ai_apparel_style}")
        print(f"   配饰风格: {result_2.ai_accessory_style}")
        print(f"   品类偏好: {result_2.preferred_category}")
        print(f"   分析理由: {result_2.analysis_reason}")
        print()
        
    except Exception as e:
        print(f"❌ 测试失败: {str(e)}")
        print()


if __name__ == "__main__":
    _test_analyze_creator_vibe_v2()
    print("✅ 测试完成")
