"""
Vision 分析模块 - LLM 多模态分析

负责：
1. 提取视频描述高频词
2. 构建优化的 Vision Prompt
3. 调用 Vision API
4. 解析分析结果
"""

import os
import sys
import json
import requests
from typing import List, Dict, Optional
from collections import Counter

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.data_schema import (
    CreatorVibeAnalysis,
    ApparelStyle,
    AccessoryStyle,
    PreferredCategory
)
from config.exceptions import VisionAPIError, DataExtractionError
from core.data_fetchers import VideoData


# ============================================================================
# 优化后的 Prompt 模板
# ============================================================================

VISION_ANALYSIS_PROMPT_V2 = """你是电商时尚分析师。分析达人 @{tk_handle} 的 Top {num_videos} GMV 视频。

**分析重点**（按优先级）：
1. 达人出镜穿搭风格（70%权重）
2. 带货商品类型（20%权重）
3. 视频背景与调性（10%权重）

**高频关键词**：{keywords}

**输出 JSON**（必须从词表选择）：
{{
  "ai_apparel_style": "Y2K_Spicy|Minimalist|Sweet|Streetwear|Elegant|Vacation",
  "ai_accessory_style": "Dainty_Minimalist|Statement_Chunky|Bling_Sparkle|Vintage_Pearl|Boho_Colorful|Sweet_Kawaii",
  "preferred_category": "Apparel_Top|Apparel_Dress|Accessories_Neck_Ear|Accessories_Bag_Hand|Mixed_Fashion",
  "analysis_reason": "简要理由（2-3句）"
}}

**Few-shot 示例**：
输入：达人穿 Y2K 风格上衣，佩戴夸张耳环，高频词：crop top, statement jewelry
输出：{{"ai_apparel_style": "Y2K_Spicy", "ai_accessory_style": "Statement_Chunky", "preferred_category": "Mixed_Fashion", "analysis_reason": "达人主要穿搭为 Y2K 辣妹风格的短款上衣，搭配夸张的 statement 耳环，展现出年轻活力的时尚态度。"}}

**词表说明**：
- Y2K_Spicy: Y2K 辣妹风（短款上衣、低腰裤、亮色）
- Minimalist: 极简风（纯色、简洁线条、基础款）
- Sweet: 甜美风（蕾丝、蝴蝶结、粉色系）
- Streetwear: 街头风（宽松、运动、潮牌）
- Elegant: 优雅风（修身、职业、高级感）
- Vacation: 度假风（飘逸、印花、度假感）

- Dainty_Minimalist: 精致极简（细链、小耳钉）
- Statement_Chunky: 夸张粗犷（大耳环、粗链条）
- Bling_Sparkle: 闪耀亮片（水钻、亮片）
- Vintage_Pearl: 复古珍珠（珍珠、复古金属）
- Boho_Colorful: 波西米亚（彩色、民族风）
- Sweet_Kawaii: 甜美可爱（卡通、可爱元素）
"""


# ============================================================================
# 高频词提取器
# ============================================================================

class KeywordExtractor:
    """视频描述高频词提取器"""
    
    @staticmethod
    def extract_keywords(
        videos: List[VideoData],
        top_n: int = 20,
        min_word_length: int = 2
    ) -> str:
        """
        提取视频描述中的高频关键词
        
        Args:
            videos: VideoData 列表
            top_n: 返回前 N 个高频词
            min_word_length: 最小词长度
            
        Returns:
            str: 高频关键词字符串（格式：word1(count1), word2(count2), ...）
        """
        # 合并所有描述
        all_descriptions = " ".join([v.description for v in videos if v.description])
        
        # 简单的词频统计
        words = all_descriptions.split()
        word_freq = Counter()
        
        for word in words:
            if len(word) >= min_word_length:
                word_freq[word] += 1
        
        # 获取 Top N
        top_keywords = word_freq.most_common(top_n)
        
        # 格式化输出
        keywords_str = ", ".join([f"{word}({count})" for word, count in top_keywords])
        
        return keywords_str if keywords_str else "无明显高频词"


# ============================================================================
# Vision API 客户端
# ============================================================================

class VisionAPIClient:
    """Vision API 客户端（支持 Claude 和 Gemini）"""
    
    def __init__(
        self,
        api_key: str,
        api_url: str = "https://api.anthropic.com/v1/messages",
        model: str = "claude-3-5-sonnet-20241022",
        max_tokens: int = 1024,
        timeout: int = 60
    ):
        self.api_key = api_key
        self.api_url = api_url
        self.model = model
        self.max_tokens = max_tokens
        self.timeout = timeout
    
    def analyze(
        self,
        prompt: str,
        images_base64: List[str]
    ) -> Dict[str, str]:
        """
        调用 Vision API 进行分析
        
        Args:
            prompt: 分析提示词
            images_base64: Base64 编码的图片列表
            
        Returns:
            Dict[str, str]: 分析结果字典
            
        Raises:
            VisionAPIError: API 调用失败
        """
        # 构建 content 数组（文本 + 多张图片）
        content = [{"type": "text", "text": prompt}]
        
        # 添加所有图片
        for img_b64 in images_base64:
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/jpeg",
                    "data": img_b64
                }
            })
        
        # 构建请求
        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }
        
        payload = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "messages": [
                {
                    "role": "user",
                    "content": content
                }
            ]
        }
        
        # 调用 API
        try:
            response = requests.post(
                self.api_url,
                json=payload,
                headers=headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            
        except requests.exceptions.Timeout:
            raise VisionAPIError(408, "请求超时")
        except requests.exceptions.HTTPError as e:
            raise VisionAPIError(
                e.response.status_code,
                f"HTTP 错误: {str(e)}"
            )
        except requests.exceptions.RequestException as e:
            raise VisionAPIError(0, f"网络错误: {str(e)}")
        
        # 解析响应
        try:
            result = response.json()
            assistant_message = result["content"][0]["text"]
        except (json.JSONDecodeError, KeyError) as e:
            raise DataExtractionError(f"响应解析失败: {str(e)}")
        
        # 提取 JSON
        try:
            analysis_result = self._extract_json(assistant_message)
        except Exception as e:
            raise DataExtractionError(
                f"JSON 提取失败: {str(e)}\n原始响应: {assistant_message[:200]}..."
            )
        
        return analysis_result
    
    @staticmethod
    def _extract_json(text: str) -> Dict[str, str]:
        """
        从文本中提取 JSON
        
        Args:
            text: 包含 JSON 的文本
            
        Returns:
            Dict[str, str]: 解析后的 JSON 对象
            
        Raises:
            json.JSONDecodeError: JSON 解析失败
        """
        # 尝试多种提取方式
        if "```json" in text:
            json_str = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            json_str = text.split("```")[1].split("```")[0].strip()
        else:
            json_str = text.strip()
        
        return json.loads(json_str)


# ============================================================================
# Vision 分析器
# ============================================================================

class VisionAnalyzer:
    """Vision 分析器（整合关键词提取 + API 调用）"""
    
    def __init__(
        self,
        api_key: str,
        api_url: str = "https://api.anthropic.com/v1/messages",
        model: str = "claude-3-5-sonnet-20241022"
    ):
        self.keyword_extractor = KeywordExtractor()
        self.api_client = VisionAPIClient(api_key, api_url, model)
    
    def analyze(
        self,
        tk_handle: str,
        videos: List[VideoData],
        canvases_base64: List[str]
    ) -> CreatorVibeAnalysis:
        """
        执行完整的 Vision 分析
        
        Args:
            tk_handle: TikTok 账号名
            videos: VideoData 列表
            canvases_base64: Base64 编码的画板列表
            
        Returns:
            CreatorVibeAnalysis: 分析结果
            
        Raises:
            VisionAPIError: API 调用失败
            DataExtractionError: 数据解析失败
        """
        # 1. 提取高频关键词
        keywords = self.keyword_extractor.extract_keywords(videos)
        
        # 2. 构建 Prompt
        prompt = VISION_ANALYSIS_PROMPT_V2.format(
            tk_handle=tk_handle,
            num_videos=len(videos),
            keywords=keywords
        )
        
        # 3. 调用 Vision API
        analysis_result = self.api_client.analyze(prompt, canvases_base64)
        
        # 4. 验证并转换为 Pydantic 模型
        try:
            return CreatorVibeAnalysis(
                tk_handle=tk_handle,
                ai_apparel_style=ApparelStyle(analysis_result["ai_apparel_style"]),
                ai_accessory_style=AccessoryStyle(analysis_result["ai_accessory_style"]),
                preferred_category=PreferredCategory(analysis_result["preferred_category"]),
                analysis_reason=analysis_result["analysis_reason"]
            )
        except (KeyError, ValueError) as e:
            raise DataExtractionError(
                f"分析结果验证失败: {str(e)}\n原始结果: {analysis_result}"
            )


# ============================================================================
# 便捷函数
# ============================================================================

def analyze_creator_vision(
    tk_handle: str,
    videos: List[VideoData],
    canvases_base64: List[str],
    vision_api_key: str,
    vision_api_url: str = "https://api.anthropic.com/v1/messages",
    vision_model: str = "claude-3-5-sonnet-20241022"
) -> CreatorVibeAnalysis:
    """
    便捷函数：执行 Vision 分析
    
    Args:
        tk_handle: TikTok 账号名
        videos: VideoData 列表
        canvases_base64: Base64 编码的画板列表
        vision_api_key: Vision API Key
        vision_api_url: Vision API 端点
        vision_model: Vision 模型名称
        
    Returns:
        CreatorVibeAnalysis: 分析结果
        
    Raises:
        VisionAPIError: API 调用失败
        DataExtractionError: 数据解析失败
    """
    analyzer = VisionAnalyzer(vision_api_key, vision_api_url, vision_model)
    return analyzer.analyze(tk_handle, videos, canvases_base64)
