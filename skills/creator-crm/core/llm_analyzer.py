#!/usr/bin/env python3
"""
LLM 分析模块
用于视频质量评分和带货标签打标

特性:
- 支持多模型调用（通过 yunwu.ai API）
- 图片转 Base64 编码
- 限流和熔断保护
- 错误重试机制
"""

import os
import sys
import json
import time
import base64
import traceback
from pathlib import Path
from typing import Dict, Any, Optional, List
from abc import ABC, abstractmethod

import requests

# 添加路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.rate_limiter import (
    RateLimiter, RateLimiterConfig,
    CircuitBreaker, CircuitBreakerConfig
)
from core.model_runtime_config import (
    CATEGORY_API_KEY,
    CATEGORY_API_URL,
    CATEGORY_MODEL,
    DEFAULT_VISION_MODELS,
    DOUBAO_VISION_MODELS,
    GEMINI_VISION_MODELS,
    LLM_API_KEY,
    LLM_API_URL,
    LLM_MODEL,
    OPENAI_VISION_MODELS,
)


# ============================================================================
# 配置
# ============================================================================


# ============================================================================
# LLM 客户端
# ============================================================================

class LLMClient:
    """大模型 API 客户端（支持火山引擎 Doubao、OpenAI 和 Gemini 原生 API 格式）"""
    
    def __init__(
        self,
        api_url: str = LLM_API_URL,
        api_key: str = LLM_API_KEY,
        model: str = None,
        timeout: int = 120,
        fallback_models: List[str] = None,
        max_retries: int = 2
    ):
        self.api_url = api_url.rstrip('/')
        self.api_key = api_key
        self.model = model or LLM_MODEL
        self.timeout = timeout
        self.max_retries = max_retries
        # 回退模型列表
        self.fallback_models = fallback_models or DEFAULT_VISION_MODELS
        # 记录可用的模型（缓存）
        self._working_model: Optional[str] = None
    
    def _is_gemini_model(self, model: str) -> bool:
        """判断是否为 Gemini 模型"""
        return model.startswith("gemini")
    
    def _is_doubao_model(self, model: str) -> bool:
        """判断是否为火山引擎 doubao 模型"""
        return model.lower().startswith("doubao")
    
    def _call_with_retry(self, api_func, *args, **kwargs) -> Dict[str, Any]:
        """带重试的 API 调用"""
        last_error = None
        for attempt in range(self.max_retries + 1):
            try:
                return api_func(*args, **kwargs)
            except requests.exceptions.Timeout as e:
                last_error = e
                if attempt < self.max_retries:
                    wait_time = 2 ** attempt  # 指数退避
                    print(f"    ⚠️ API 调用超时，{wait_time}秒后重试 ({attempt + 1}/{self.max_retries})...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"API 调用超时（已重试{self.max_retries}次）: {e}")
            except requests.exceptions.RequestException as e:
                last_error = e
                if attempt < self.max_retries:
                    wait_time = 2 ** attempt
                    print(f"    ⚠️ 网络错误，{wait_time}秒后重试 ({attempt + 1}/{self.max_retries})...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"网络错误（已重试{self.max_retries}次）: {e}")
        raise last_error or Exception("未知错误")
    
    def chat_with_image(
        self,
        image_path: str,
        prompt: str,
        max_tokens: int = 2000
    ) -> Dict[str, Any]:
        """
        发送图片和提示词到 LLM（自动选择 API 格式）
        
        Args:
            image_path: 图片路径
            prompt: 提示词
            max_tokens: 最大输出 token 数
        
        Returns:
            LLM 响应结果
        """
        # 读取图片并转 Base64
        with open(image_path, 'rb') as f:
            image_data = f.read()
        
        image_base64 = base64.b64encode(image_data).decode('utf-8')
        
        # 获取图片 MIME 类型
        image_format = Path(image_path).suffix.lower().lstrip('.')
        if image_format == 'jpg':
            image_format = 'jpeg'
        mime_type = f"image/{image_format}"
        
        # 构建要尝试的模型列表（优先使用已知可用的模型）
        models_to_try = []
        if self._working_model:
            models_to_try.append(self._working_model)
        if self.model and self.model not in models_to_try:
            models_to_try.append(self.model)
        for m in self.fallback_models:
            if m not in models_to_try:
                models_to_try.append(m)
        
        # 尝试各个模型
        last_error = None
        for model in models_to_try:
            try:
                print(f"    🤖 尝试模型: {model}")
                
                # 根据模型类型选择 API 格式（带重试）
                if self._is_doubao_model(model):
                    result = self._call_with_retry(
                        self._call_doubao_api,
                        image_base64=image_base64,
                        mime_type=mime_type,
                        prompt=prompt,
                        model=model,
                        max_tokens=max_tokens
                    )
                elif self._is_gemini_model(model):
                    result = self._call_with_retry(
                        self._call_gemini_api,
                        image_base64=image_base64,
                        mime_type=mime_type,
                        prompt=prompt,
                        model=model,
                        max_tokens=max_tokens
                    )
                else:
                    result = self._call_with_retry(
                        self._call_openai_api,
                        image_base64=image_base64,
                        mime_type=mime_type,
                        prompt=prompt,
                        model=model,
                        max_tokens=max_tokens
                    )
                
                # 成功则缓存这个模型
                self._working_model = model
                print(f"    ✅ 模型 {model} 调用成功")
                return result
                
            except Exception as e:
                last_error = e
                error_msg = str(e)
                # 如果是模型不可用错误，尝试下一个模型
                if "No available channels" in error_msg or "No available channel" in error_msg or "not found" in error_msg.lower() or "502" in error_msg or "503" in error_msg or "超时" in error_msg or "timeout" in error_msg.lower():
                    print(f"    ⚠️ 模型 {model} 不可用或超时，尝试下一个...")
                    continue
                # 其他错误直接抛出
                raise e
        
        # 所有模型都不可用
        raise Exception(f"所有模型都不可用，最后错误: {last_error}")
    
    def _call_doubao_api(
        self,
        image_base64: str,
        mime_type: str,
        prompt: str,
        model: str,
        max_tokens: int
    ) -> Dict[str, Any]:
        """
        调用火山引擎 Doubao API
        
        API 格式：POST /api/v3/chat/completions (OpenAI 兼容)
        文档：https://www.volcengine.com/docs/82379/1298454
        """
        url = f"{self.api_url}/chat/completions"
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # 火山引擎 API 请求格式 (OpenAI 兼容)
        # 支持图片URL或base64，这里使用base64
        image_data_url = f"data:{mime_type};base64,{image_base64}"
        
        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": image_data_url
                            }
                        },
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ],
            "max_tokens": max_tokens
        }
        
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=self.timeout
        )
        
        if response.status_code != 200:
            raise Exception(f"Doubao API 调用失败: {response.status_code} - {response.text}")
        
        result = response.json()
        
        # 解析 OpenAI 兼容格式响应
        # {
        #   "choices": [{"message": {"content": "..."}}],
        #   "model": "...",
        #   "usage": {...}
        # }
        try:
            text_content = None
            
            # OpenAI 兼容格式 - choices 在根级别
            if 'choices' in result and len(result['choices']) > 0:
                text_content = result['choices'][0]['message']['content']
            
            # 旧格式 - output 是字典
            elif 'output' in result and isinstance(result['output'], dict):
                if 'text' in result['output']:
                    text_content = result['output']['text']
                elif 'choices' in result['output']:
                    text_content = result['output']['choices'][0]['message']['content']
                else:
                    raise Exception(f"未知的 output 字典格式: {result['output'].keys()}")
            
            else:
                raise Exception(f"未知的响应格式，顶层 keys: {result.keys()}")
            
            if not text_content:
                raise Exception(f"提取到的文本内容为空")
            
            if not text_content:
                raise Exception(f"提取到的文本内容为空")
            
            # 转换为统一格式（模拟 OpenAI 格式便于后续解析）
            return {
                'choices': [
                    {
                        'message': {
                            'content': text_content
                        }
                    }
                ]
            }
        except (KeyError, IndexError) as e:
            raise Exception(f"解析 Doubao 响应失败: {e}\n原始响应: {json.dumps(result, ensure_ascii=False)[:1000]}")
    
    def _call_gemini_api(
        self,
        image_base64: str,
        mime_type: str,
        prompt: str,
        model: str,
        max_tokens: int
    ) -> Dict[str, Any]:
        """
        调用 Gemini 原生 API
        
        API 格式：POST /v1beta/models/{model}:generateContent
        """
        url = f"{self.api_url}/v1beta/models/{model}:generateContent?key={self.api_key}"
        
        headers = {
            "Content-Type": "application/json"
        }
        
        # Gemini 原生 API 请求格式
        payload = {
            "contents": [
                {
                    "parts": [
                        {
                            "inline_data": {
                                "mime_type": mime_type,
                                "data": image_base64
                            }
                        },
                        {
                            "text": prompt
                        }
                    ]
                }
            ],
            "generationConfig": {
                "maxOutputTokens": max_tokens,
                "temperature": 0.3
            }
        }
        
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=self.timeout
        )
        
        if response.status_code != 200:
            raise Exception(f"Gemini API 调用失败: {response.status_code} - {response.text}")
        
        result = response.json()
        
        # 转换为统一格式（模拟 OpenAI 格式便于后续解析）
        try:
            text_content = result['candidates'][0]['content']['parts'][0]['text']
            return {
                'choices': [
                    {
                        'message': {
                            'content': text_content
                        }
                    }
                ]
            }
        except (KeyError, IndexError) as e:
            raise Exception(f"解析 Gemini 响应失败: {e}\n原始响应: {result}")
    
    def _call_openai_api(
        self,
        image_base64: str,
        mime_type: str,
        prompt: str,
        model: str,
        max_tokens: int
    ) -> Dict[str, Any]:
        """
        调用 OpenAI 兼容 API
        
        API 格式：POST /v1/chat/completions
        """
        url = f"{self.api_url}/v1/chat/completions"
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # OpenAI 兼容 API 请求格式
        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{mime_type};base64,{image_base64}"
                            }
                        },
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ],
            "max_tokens": max_tokens,
            "temperature": 0.3
        }
        
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=self.timeout
        )
        
        if response.status_code != 200:
            raise Exception(f"OpenAI API 调用失败: {response.status_code} - {response.text}")
        
        result = response.json()
        return result
    
    def parse_json_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """
        解析 LLM 响应中的 JSON
        
        Args:
            response: LLM API 响应
        
        Returns:
            解析后的 JSON 对象
        """
        try:
            content = response['choices'][0]['message']['content']
            
            # 尝试直接解析
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                pass
            
            # 尝试提取 JSON 块
            import re
            
            # 移除可能的 markdown 标记
            content = re.sub(r'^```json\s*', '', content)
            content = re.sub(r'\s*```$', '', content)
            content = content.strip()
            
            return json.loads(content)
            
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            raise Exception(f"解析 LLM 响应失败: {e}\n原始内容: {content if 'content' in dir() else 'N/A'}")
    
    def chat_with_multiple_images(
        self,
        image_paths: List[str],
        prompt: str,
        max_tokens: int = 3000
    ) -> Dict[str, Any]:
        """
        发送多张图片和提示词到 LLM（用于分析达人多宫格图）
        
        Args:
            image_paths: 图片路径列表（支持多张宫格图）
            prompt: 提示词
            max_tokens: 最大输出 token 数
        
        Returns:
            LLM 响应结果
        """
        # 读取所有图片并转 Base64
        images_data = []
        for image_path in image_paths:
            with open(image_path, 'rb') as f:
                image_data = f.read()
            
            image_base64 = base64.b64encode(image_data).decode('utf-8')
            
            # 获取图片 MIME 类型
            image_format = Path(image_path).suffix.lower().lstrip('.')
            if image_format == 'jpg':
                image_format = 'jpeg'
            mime_type = f"image/{image_format}"
            
            images_data.append({
                'base64': image_base64,
                'mime_type': mime_type,
                'path': image_path
            })
        
        print(f"    📷 准备发送 {len(images_data)} 张宫格图给 LLM...")
        
        # 构建要尝试的模型列表
        models_to_try = []
        if self._working_model:
            models_to_try.append(self._working_model)
        if self.model and self.model not in models_to_try:
            models_to_try.append(self.model)
        for m in self.fallback_models:
            if m not in models_to_try:
                models_to_try.append(m)
        
        # 尝试各个模型
        last_error = None
        for model in models_to_try:
            try:
                print(f"    🤖 尝试模型: {model}")
                
                # 根据模型类型选择 API 格式
                if self._is_doubao_model(model):
                    result = self._call_with_retry(
                        self._call_doubao_api_multi_images,
                        images_data=images_data,
                        prompt=prompt,
                        model=model,
                        max_tokens=max_tokens
                    )
                elif self._is_gemini_model(model):
                    result = self._call_with_retry(
                        self._call_gemini_api_multi_images,
                        images_data=images_data,
                        prompt=prompt,
                        model=model,
                        max_tokens=max_tokens
                    )
                else:
                    result = self._call_with_retry(
                        self._call_openai_api_multi_images,
                        images_data=images_data,
                        prompt=prompt,
                        model=model,
                        max_tokens=max_tokens
                    )
                
                # 成功则缓存这个模型
                self._working_model = model
                print(f"    ✅ 模型 {model} 调用成功")
                return result
                
            except Exception as e:
                last_error = e
                error_msg = str(e)
                if "No available channels" in error_msg or "No available channel" in error_msg or "not found" in error_msg.lower() or "502" in error_msg or "503" in error_msg or "超时" in error_msg or "timeout" in error_msg.lower():
                    print(f"    ⚠️ 模型 {model} 不可用或超时，尝试下一个...")
                    continue
                raise e
        
        raise Exception(f"所有模型都不可用，最后错误: {last_error}")
    
    def _call_doubao_api_multi_images(
        self,
        images_data: List[Dict],
        prompt: str,
        model: str,
        max_tokens: int
    ) -> Dict[str, Any]:
        """调用火山引擎 Doubao API（多图模式）"""
        url = f"{self.api_url}/chat/completions"
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # 构建多图消息内容
        content = []
        for img in images_data:
            content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:{img['mime_type']};base64,{img['base64']}"
                }
            })
        content.append({
            "type": "text",
            "text": prompt
        })
        
        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": content
                }
            ],
            "max_tokens": max_tokens
        }
        
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=self.timeout
        )
        
        if response.status_code != 200:
            raise Exception(f"Doubao API 调用失败: {response.status_code} - {response.text}")
        
        return response.json()
    
    def _call_gemini_api_multi_images(
        self,
        images_data: List[Dict],
        prompt: str,
        model: str,
        max_tokens: int
    ) -> Dict[str, Any]:
        """调用 Gemini API（多图模式）"""
        url = f"{self.api_url}/v1beta/models/{model}:generateContent"
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # 构建多图 parts
        parts = []
        for img in images_data:
            parts.append({
                "inline_data": {
                    "mime_type": img['mime_type'],
                    "data": img['base64']
                }
            })
        parts.append({"text": prompt})
        
        payload = {
            "contents": [{
                "parts": parts
            }],
            "generationConfig": {
                "maxOutputTokens": max_tokens,
                "temperature": 0.3
            }
        }
        
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=self.timeout
        )
        
        if response.status_code != 200:
            raise Exception(f"Gemini API 调用失败: {response.status_code} - {response.text}")
        
        return response.json()
    
    def _call_openai_api_multi_images(
        self,
        images_data: List[Dict],
        prompt: str,
        model: str,
        max_tokens: int
    ) -> Dict[str, Any]:
        """调用 OpenAI 兼容 API（多图模式）"""
        url = f"{self.api_url}/v1/chat/completions"
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # 构建多图消息内容
        content = []
        for img in images_data:
            content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:{img['mime_type']};base64,{img['base64']}"
                }
            })
        content.append({
            "type": "text",
            "text": prompt
        })
        
        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": content
                }
            ],
            "max_tokens": max_tokens,
            "temperature": 0.3
        }
        
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=self.timeout
        )
        
        if response.status_code != 200:
            raise Exception(f"OpenAI API 调用失败: {response.status_code} - {response.text}")
        
        return response.json()


# ============================================================================
# 视频评分 Agent
# ============================================================================

# 视频质量评分提示词
VIDEO_SCORING_PROMPT = """Task: 达人视频质量精细化评估 (25-Point Quality Matrix)
请像一位严苛且专业的品牌招商经理一样，对达人的内容质量进行 5 个维度的精细化评估。每个维度满分为 5 分。请严格按照以下"锚点规则"进行打分（你也可以根据实际情况给出 2 分或 4 分）：

⭐ 维度 1 (score_traffic): 基础流量水位
- 评判依据：提取文本数据中这批视频的平均或中位数播放量(Views)。
- [1分]：极度惨淡（绝大多数 < 1,000 播放）。
- [3分]：表现平稳，符合正常带货 KOC 水准（绝大多数在 5,000 - 20,000 播放之间）。
- [5分]：流量爆款制造机（绝大多数 > 100,000 播放）。

⭐ 维度 2 (score_presence): 真人出镜与表现力
- 评判依据：画板中达人真人出镜展示产品的比例与清晰度。
- [1分]：毫无个人 IP（纯商品摆拍、无人的风景切片、纯 PPT 幻灯片）。
- [3分]：有真人出镜，但大多只是局部展示（如只露手/脖颈），或出镜比例仅占一半左右。
- [5分]：强烈的个人 IP 属性（>80% 封面有清晰的真人全身/半身穿搭展示，且镜头感极强）。

⭐ 维度 3 (score_consistency): 视觉统一与主页美学
- 评判依据：画板整体看起来是否像一本精心策划的杂志。
- [1分]：杂货铺既视感（毫无排版可言，色调忽冷忽热，构图极其随意）。
- [3分]：有一定包装意识（部分使用了统一的花字，或某一两排风格一致，但整体仍有割裂感）。
- [5分]：高度专业统一（具有标志性的统一排版/字体、严格的色彩滤镜调性、完全一致的精美拍摄景别）。

⭐ 维度 4 (score_lighting): 画质与布光
- 评判依据：画面人物受光与分辨率。
- [1分]：画质恶劣（严重的逆光发黑、昏暗模糊、明显的低像素噪点）。
- [3分]：普通手机随手拍水平（光线自然但缺乏层次，清晰度尚可，无明显瑕疵）。
- [5分]：棚拍/专业级画质（主体受光极其均匀、色彩还原度极高、甚至有明显的轮廓光/眼神光打光痕迹，画面极度高清）。

⭐ 维度 5 (score_background): 置景与背景
- 评判依据：拍摄环境的整洁度与高级感。
- [1分]：拉低品牌调性（背景出现未经整理的杂乱床铺/杂物、廉价杂乱的货架等）。
- [3分]：普通日常（干净的纯色白墙、普通的自然街道，不减分但也不能为产品提供调性加成）。
- [5分]：极具场景美感（精心布置的 Aesthetic 房间/Studio、有高级感的咖啡厅/度假村，背景能极大提升产品的溢价感）。

【重要计算指令】：
1. 计算总分 (total_score)：将上述 5 个维度的得分相加 (满分 25 分)。公式：total_score = score_traffic + score_presence + score_consistency + score_lighting + score_background。
2. 计算综合星级 (final_star_rating)：将总分除以 5，保留一位小数 (例如：总分 21 分，则星级为 4.2)。

Output Format (严格的 JSON 输出)
请严格输出一个 JSON 对象。绝对不要包含任何 Markdown 标记（如 ```json）或其他解释性文字。必须严格按照以下顺序输出，确保你先进行详细的推理，再输出分数：
{
  "analysis_reason": "【第一步：先思考】请在这里用中文详细阐述你对这 5 个维度打分的具体依据，为什么给高分，为什么扣分。请务必在此完成总分和星级的计算推导。",
  "score_traffic": [1-5的整数],
  "score_presence": [1-5的整数],
  "score_consistency": [1-5的整数],
  "score_lighting": [1-5的整数],
  "score_background": [1-5的整数],
  "total_score": [1-25的整数],
  "final_star_rating": [1.0-5.0的浮点数]
}"""


# ============================================================================
# 合并 Agent：打分 + 风格打标（一次 LLM 调用，节省 token）
# ============================================================================

# 合并提示词：视频评分 + 达人风格打标
COMBINED_SCORING_VIBE_PROMPT = """Role: 跨境电商资深买手与达人商业潜能分析师

你需要完成两项分析任务：
1. **视频质量评分**：对达人内容质量进行 5 维度精细化评估
2. **达人风格打标**：分析达人外貌气质和视频调性

## 任务一：视频质量评分 (25-Point Quality Matrix)

请像一位严苛且专业的品牌招商经理一样，对达人的内容质量进行 5 个维度的精细化评估。每个维度满分为 5 分。

⭐ 维度 1 (score_traffic): 基础流量水位
- 评判依据：提取文本数据中这批视频的平均或中位数播放量(Views)。
- [1分]：极度惨淡（绝大多数 < 1,000 播放）。
- [3分]：表现平稳，符合正常带货 KOC 水准（绝大多数在 5,000 - 20,000 播放之间）。
- [5分]：流量爆款制造机（绝大多数 > 100,000 播放）。

⭐ 维度 2 (score_presence): 真人出镜与表现力
- 评判依据：画板中达人真人出镜展示产品的比例与清晰度。
- [1分]：毫无个人 IP（纯商品摆拍、无人的风景切片、纯 PPT 幻灯片）。
- [3分]：有真人出镜，但大多只是局部展示（如只露手/脖颈），或出镜比例仅占一半左右。
- [5分]：强烈的个人 IP 属性（>80% 封面有清晰的真人全身/半身穿搭展示，且镜头感极强）。

⭐ 维度 3 (score_consistency): 视觉统一与主页美学
- 评判依据：画板整体看起来是否像一本精心策划的杂志。
- [1分]：杂货铺既视感（毫无排版可言，色调忽冷忽热，构图极其随意）。
- [3分]：有一定包装意识（部分使用了统一的花字，或某一两排风格一致，但整体仍有割裂感）。
- [5分]：高度专业统一（具有标志性的统一排版/字体、严格的色彩滤镜调性、完全一致的精美拍摄景别）。

⭐ 维度 4 (score_lighting): 画质与布光
- 评判依据：画面人物受光与分辨率。
- [1分]：画质恶劣（严重的逆光发黑、昏暗模糊、明显的低像素噪点）。
- [3分]：普通手机随手拍水平（光线自然但缺乏层次，清晰度尚可，无明显瑕疵）。
- [5分]：棚拍/专业级画质（主体受光极其均匀、色彩还原度极高、甚至有明显的轮廓光/眼神光打光痕迹，画面极度高清）。

⭐ 维度 5 (score_background): 置景与背景
- 评判依据：拍摄环境的整洁度与高级感。
- [1分]：拉低品牌调性（背景出现未经整理的杂乱床铺/杂物、廉价杂乱的货架等）。
- [3分]：普通日常（干净的纯色白墙、普通的自然街道，不减分但也不能为产品提供调性加成）。
- [5分]：极具场景美感（精心布置的 Aesthetic 房间/Studio、有高级感的咖啡厅/度假村，背景能极大提升产品的溢价感）。

【评分计算指令】：
1. 计算总分 (total_score)：将上述 5 个维度的得分相加 (满分 25 分)。
2. 计算综合星级 (final_star_rating)：将总分除以 5，保留一位小数 (例如：总分 21 分，则星级为 4.2)。

---

## 任务二：达人风格打标

观察多宫格中达人的出镜外表（长相特点、妆容、发型）、当前穿搭、以及视频背景（置景调性），为该达人打上最符合的 1 个"核心调性标签"。

🎭 受控风格标签字典 (严格遵守)
请务必从以下 5 个宽泛的风格大类中选择 1 个：

1. Sweet_Girl (甜美邻家风)
视觉锚点：长相显幼态/甜美，笑容多；常穿浅色系、马卡龙色、碎花或带有蝴蝶结/蕾丝元素的衣服；妆容偏粉嫩、元气。
适配货盘：甜美系女装、Kawaii配饰、平价彩妆、可爱型生活好物。

2. Elegant_Lady (优雅轻熟风)
视觉锚点：气质成熟稳重、知性；穿搭有质感，常出现西装、真丝、修身连衣裙、纯色高级感服饰；妆容精致干净，背景通常较有格调。
适配货盘：中高客单价通勤女装、珍珠/法式配饰、抗老护肤品、高级香水。

3. Cool_Trendy (个性潮辣风)
视觉锚点：视觉冲击力强；常穿紧身露肤辣妹装、Oversize高街潮牌；可能带有挑眉、截断式眼妆、欧美浓妆、明显纹身或亮色染发。
适配货盘：Y2K/高街女装、夸张量感配饰、色彩大胆的彩妆、潮玩。

4. Clean_Minimalist (极简自然风)
视觉锚点：给人清爽、干净、不费力的感觉；常穿黑白灰、大地色系的基础款、纯色T恤/衬衫；伪素颜妆容，无夸张造型；背景通常非常干净。
适配货盘：基础款舒适女装、极简纤细配饰、天然植物护肤品、极简家居用品。

5. Everyday_Life (亲民接地气)
视觉锚点：外表非常普通的路人/宝妈既视感；穿搭极为随意（居家服、宽松大T恤）；基本不化妆或画很随意的淡妆；视频背景充满真实的生活气息。
适配货盘：9.9包邮大码/居家女装、廉价但实用的百货收纳、纸巾等日用消耗品、零食。

👁️ 风格分析指南：
- 看人优先于看货：即使一个甜美的女孩手里拿着一把电钻，她的整体风格依然是 Sweet_Girl。
- 宁宽不紧：如果达人风格介于两者之间，请选择包容性更强的那一个。

---

## Output Format (严格的 JSON 输出)

【最高指令】：你的所有输出必须仅仅是一个合法的 JSON 对象。绝对禁止输出任何前面的寒暄或后面的总结。绝对禁止包含 Markdown 代码块标记。请直接以 `{` 开头，并以 `}` 结尾。

{
  "scoring_analysis_reason": "【视频评分分析】详细阐述你对 5 个维度打分的具体依据，为什么给高分，为什么扣分。完成总分和星级的计算推导。",
  "score_traffic": [1-5的整数],
  "score_presence": [1-5的整数],
  "score_consistency": [1-5的整数],
  "score_lighting": [1-5的整数],
  "score_background": [1-5的整数],
  "total_score": [1-25的整数],
  "final_star_rating": [1.0-5.0的浮点数],
  "vibe_analysis_reason": "【风格分析】详细描述你观察到的达人长相特征、妆容浓淡、服装元素及背景调性，并解释为什么将她归入该风格。",
  "creator_vibe_tag": "填入 5 个风格标签中的 1 个（英文枚举值：Sweet_Girl / Elegant_Lady / Cool_Trendy / Clean_Minimalist / Everyday_Life）"
}"""


class CombinedScoringVibeAgent:
    """合并智能体：视频评分 + 达人风格打标（一次 LLM 调用，节省 token）
    
    将两个任务合并到一次 LLM 调用中，减少 token 消耗。
    输出包含评分和风格标签两部分结果。
    """
    
    # 有效的风格标签枚举
    VALID_VIBE_TAGS = [
        "Sweet_Girl",
        "Elegant_Lady", 
        "Cool_Trendy",
        "Clean_Minimalist",
        "Everyday_Life"
    ]
    
    def __init__(self):
        self.llm_client = LLMClient()
        
        # 保留熔断器：3次失败后熔断 120 秒
        self.circuit_breaker = CircuitBreaker(CircuitBreakerConfig(
            failure_threshold=3,
            success_threshold=2,
            timeout=120.0
        ))
        
        self.stats = {
            'processed': 0,
            'success': 0,
            'failed': 0,
            'total_time': 0.0
        }
    
    def execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行合并分析：视频评分 + 风格打标
        
        Args:
            payload: {
                'grid_paths': List[str],  # 宫格图路径列表
                'views_list': List[int],  # 播放量列表（可选）
                'tk_handle': str          # 达人账号
            }
        
        Returns:
            {
                'tk_handle': str,
                                # 评分结果
                'analysis_reason': str,
                'score_traffic': int,
                'score_presence': int,
                'score_consistency': int,
                'score_lighting': int,
                'score_background': int,
                'total_score': int,
                'final_star_rating': float,
                # 风格结果
                'vibe_reason': str,
                'vibe_tag': str
            }
        """
        start_time = time.time()
        
        try:
            tk_handle = payload['tk_handle']
            grid_paths = payload.get('grid_paths', [])
            
            if not grid_paths:
                raise Exception("缺少宫格图路径")
            
            # 使用所有宫格图进行分析
            print(f"  🎯 [CombinedScoringVibe] 分析达人: {tk_handle}")
            print(f"     使用 {len(grid_paths)} 张宫格图:")
            for i, gp in enumerate(grid_paths, 1):
                print(f"       [{i}] {Path(gp).name}")
            
            # 调用 LLM（多图模式）
            response = self.circuit_breaker.call(
                self._call_llm,
                grid_paths
            )
            
            # 解析 LLM 原始响应
            raw_content = response['choices'][0]['message']['content']
            
            # 使用正则提取 JSON
            import re
            match = re.search(r'\{.*\}', raw_content, re.DOTALL)
            
            if not match:
                raise Exception(f"未在模型输出中找到 JSON 括号结构")
            
            json_string = match.group(0)
            result = json.loads(json_string)
            
            # ============ 提取评分结果 ============
            # 辅助函数：安全转换为整数（处理字符串、浮点数等）
            def safe_int(val, default=3, min_val=1, max_val=5):
                """安全转换为整数，支持字符串、浮点数等类型"""
                if val is None:
                    return default
                try:
                    # 先转为浮点数，再转为整数（处理 "3.0" 这样的字符串）
                    int_val = int(float(val))
                    if min_val <= int_val <= max_val:
                        return int_val
                    return default
                except (ValueError, TypeError):
                    return default
            
            # 辅助函数：安全转换为浮点数
            def safe_float(val, default=3.0, min_val=1.0, max_val=5.0):
                """安全转换为浮点数，支持字符串等类型"""
                if val is None:
                    return default
                try:
                    float_val = float(val)
                    if min_val <= float_val <= max_val:
                        return round(float_val, 1)
                    return default
                except (ValueError, TypeError):
                    return default
            
            scoring_result = {
                'analysis_reason': result.get('scoring_analysis_reason', ''),
                'score_traffic': safe_int(result.get('score_traffic'), 3),
                'score_presence': safe_int(result.get('score_presence'), 3),
                'score_consistency': safe_int(result.get('score_consistency'), 3),
                'score_lighting': safe_int(result.get('score_lighting'), 3),
                'score_background': safe_int(result.get('score_background'), 3),
                'total_score': safe_int(result.get('total_score'), 15, min_val=5, max_val=25),
                'final_star_rating': safe_float(result.get('final_star_rating'), 3.0)
            }
            
            # 打印解析结果用于调试
            print(f"     📊 评分解析: traffic={scoring_result['score_traffic']}, presence={scoring_result['score_presence']}, consistency={scoring_result['score_consistency']}, lighting={scoring_result['score_lighting']}, background={scoring_result['score_background']}")
            
            # 重新计算总分和星级
            scoring_result['total_score'] = (
                scoring_result['score_traffic'] + 
                scoring_result['score_presence'] + 
                scoring_result['score_consistency'] + 
                scoring_result['score_lighting'] + 
                scoring_result['score_background']
            )
            scoring_result['final_star_rating'] = round(scoring_result['total_score'] / 5, 1)
            
            # ============ 提取风格结果 ============
            vibe_reason = result.get('vibe_analysis_reason', '')
            vibe_tag = result.get('creator_vibe_tag', 'Unknown')
            
            # 验证风格标签
            if vibe_tag not in self.VALID_VIBE_TAGS:
                print(f"     ⚠️ 无效的风格标签: {vibe_tag}，将设为 Unknown")
                vibe_tag = "Unknown"
            
            # ============ 合并输出 ============
            output = {
                'tk_handle': tk_handle,
                # 评分结果
                **scoring_result,
                # 风格结果
                'vibe_reason': vibe_reason,
                'vibe_tag': vibe_tag
            }
            
            # 统计
            self.stats['processed'] += 1
            self.stats['success'] += 1
            self.stats['total_time'] += time.time() - start_time
            
            print(f"     ✅ 分析完成: 评分 {scoring_result['final_star_rating']} 星, 风格 {vibe_tag}")
            
            return output
            
        except Exception as e:
            self.stats['processed'] += 1
            self.stats['failed'] += 1
            self.stats['total_time'] += time.time() - start_time
            print(f"     ❌ 分析失败: {e}")
            raise e
    
    def _call_llm(self, grid_paths: List[str]) -> Dict[str, Any]:
        """调用 LLM API（多图模式）"""
        return self.llm_client.chat_with_multiple_images(
            image_paths=grid_paths,
            prompt=COMBINED_SCORING_VIBE_PROMPT,
            max_tokens=2500
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self.stats.copy()
        if stats['processed'] > 0:
            stats['avg_time'] = stats['total_time'] / stats['processed']
            stats['success_rate'] = stats['success'] / stats['processed']
        return stats


class VideoScoringAgent:
    """视频质量评分智能体
    
    注意：LLM API 是正式付费接口，不需要限流。
    但保留熔断器用于异常恢复，防止连续失败时持续调用。
    """
    
    def __init__(self):
        self.llm_client = LLMClient()
        
        # 不再配置限流器 - LLM API 是正式付费接口，无需限流
        # self.rate_limiter = None
        
        # 保留熔断器：3次失败后熔断 120 秒，用于异常恢复
        self.circuit_breaker = CircuitBreaker(CircuitBreakerConfig(
            failure_threshold=3,
            success_threshold=2,
            timeout=120.0
        ))
        
        self.stats = {
            'processed': 0,
            'success': 0,
            'failed': 0,
            'total_time': 0.0
        }
    
    def execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行视频质量评分
        
        Args:
            payload: {
                'grid_paths': List[str],  # 宫格图路径列表
                'views_list': List[int],  # 播放量列表（可选）
                'tk_handle': str          # 达人账号
            }
        
        Returns:
            {
                'tk_handle': str,
                'analysis_reason': str,
                'score_traffic': int,
                'score_presence': int,
                'score_consistency': int,
                'score_lighting': int,
                'score_background': int,
                'total_score': int,
                'final_star_rating': float
            }
        """
        start_time = time.time()
        
        try:
            # 已移除限流检查 - LLM API 是正式付费接口，无需限流
            
            tk_handle = payload['tk_handle']
            grid_paths = payload.get('grid_paths', [])
            
            if not grid_paths:
                raise Exception("缺少宫格图路径")
            
            # 使用第一张宫格图进行评分
            grid_path = grid_paths[0]
            
            print(f"  🎯 [VideoScoring] 分析达人: {tk_handle}")
            print(f"     使用宫格图: {Path(grid_path).name}")
            
            # 调用 LLM
            response = self.circuit_breaker.call(
                self._call_llm,
                grid_path
            )
            
            # 解析结果
            result = self.llm_client.parse_json_response(response)
            
            # 验证必需字段
            required_fields = [
                'analysis_reason', 'score_traffic', 'score_presence',
                'score_consistency', 'score_lighting', 'score_background',
                'total_score', 'final_star_rating'
            ]
            
            for field in required_fields:
                if field not in result:
                    raise Exception(f"LLM 响应缺少字段: {field}")
            
            result['tk_handle'] = tk_handle
            
            # 统计
            self.stats['processed'] += 1
            self.stats['success'] += 1
            self.stats['total_time'] += time.time() - start_time
            
            print(f"     ✅ 评分完成: 总分 {result['total_score']}/25, 星级 {result['final_star_rating']}")
            
            return result
            
        except Exception as e:
            self.stats['processed'] += 1
            self.stats['failed'] += 1
            self.stats['total_time'] += time.time() - start_time
            print(f"     ❌ 评分失败: {e}")
            raise e
    
    def _call_llm(self, grid_path: str) -> Dict[str, Any]:
        """调用 LLM API"""
        return self.llm_client.chat_with_image(
            image_path=grid_path,
            prompt=VIDEO_SCORING_PROMPT,
            max_tokens=2000
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self.stats.copy()
        if stats['processed'] > 0:
            stats['avg_time'] = stats['total_time'] / stats['processed']
            stats['success_rate'] = stats['success'] / stats['processed']
        return stats


# ============================================================================
# 带货标签打标 Agent
# ============================================================================

# 带货标签打标提示词
CATEGORY_TAGGING_PROMPT = """Role: 东南亚 TikTok 资深选品专家与商业分析师

Background
你正在为一个大型跨境电商系统执行达人商业画像提取。我将为你提供该达人近期发布的若干视频封面图（已拼接为多宫格画板）。这些封面往往包含了达人强力推销的商品特征（如：手持展示、局部特写、醒目的花字标明产品等）。

Task
请利用你的视觉分析能力，穿透背景噪音，精准识别该达人最核心的带货商品大类及其对应的子类目。如果达人是混合带货，请最多挑选 2 个最核心的品类。

受控品类标签字典 (严格遵守)
你输出的大类和子类目，必须一字不差地从以下字典中选取：

1. 配饰大类 (Accessories)
首饰 (项链、耳环、戒指、手链等金属/珠宝材质饰品)
眼镜 (墨镜、防蓝光眼镜、平光镜)
假发与发饰 (全顶假发、假发片、大肠发圈、发夹)
帽子与围巾 (棒球帽、遮阳帽、冬季围巾)
配饰其它 (手表、腰带等不在上述细分中的配饰)

2. 服装大类 (Apparel)
外套与上衣 (T恤、衬衫、夹克、卫衣、毛衣)
裙装 (连衣裙、半身裙)
裤装 (牛仔裤、休闲裤、短裤)
内衣与睡衣 (文胸、内裤、居家睡衣套装)
服装其它 (不在上述细分中的服装)

3. 美妆 (Beauty)
彩妆与香水 (口红、粉底、眼影、香水)
护肤与个护 (洗面奶、面膜、身体乳、洗发水、防晒霜)

4. 数码 (Electronics)
手机与配件 (手机壳、充电宝、数据线、耳机)
生活小家电 (小风扇、电动牙刷、吹风机、桌面加湿器)

5. 穆斯林 (Muslim Fashion)
头巾/Hijab (穆斯林女性佩戴的各色头巾)
穆斯林长裙/长袍 (包裹全身的传统或改良版长袍/长裙)

6. 居家百货 (Home & Living)
日用消耗品 (纸巾、洗衣液、垃圾袋、清洁湿巾)
家居与收纳 (收纳盒、床品、地毯、水杯)

7. 运动户外 (Sports & Outdoors)
运动服饰 (瑜伽裤、运动内衣、速干衣)
健身与户外装备 (哑铃、跳绳、露营装备)

8. 箱包 (Bags)
女包 (单肩包、斜挎包、手提包)
箱包其它 (旅行箱、双肩包、男包)

9. 鞋靴 (Footwear)
休闲与运动鞋 (板鞋、老爹鞋、帆布鞋)
高跟与凉鞋 (高跟鞋、平底单鞋、凉鞋、拖鞋)

10. 其它 (Others)
食品与饮料 (零食、保健品、饮料)
其它未知商品 (完全无法归入以上任何类目的商品)

视觉判定锚点指南
- 优先寻找特写：如果画面中有商品被手持靠近镜头，或者画面大部分被某个商品占据，这通常是核心带货商品。
- 区分穿搭与售卖：如果达人穿着漂亮的裙子，但手里举着一瓶洗发水，其带货品类应标记为"美妆-护肤与个护"，而不是"服装-裙装"。
- 识别花字：注意封面上的泰文、越南文或英文包装，如果包装上出现 "Serum"、"Cream"，通常为美妆；出现 "Sale" 并指向衣服，则为服装。

Output Format (严格的 JSON 输出)
请绝对不要包含任何 Markdown 标记（如 ```json）或其他解释性文字。请严格按照以下格式输出你的分析结果：
{
"analysis_reason": "【第一步：先思考】请详细描述你在这些封面图中观察到了哪些具体商品（如：发现多张封面中达人手持展示带包装的面膜和身体乳），并说明你将其归类于特定大类和子类的理由。",
"primary_category_1": {
"main_category": "填入大类名称（如：美妆）",
"sub_category": "填入子类名称（如：护肤与个护）"
},
"primary_category_2": {
"main_category": "填入大类名称（若只有一个核心品类，请填入 None）",
"sub_category": "填入子类名称（若只有一个核心品类，请填入 None）"
}
}"""


# ============================================================================
# 达人风格打标 Agent（新增）
# ============================================================================

# 达人风格打标提示词
VIBE_TAGGING_PROMPT = """Role: 跨境电商资深买手与达人商业潜能分析师

Background
你正在为一个千万级出海电商平台（主营女装、配饰，兼顾美妆百货）进行达人库的"带货体质"分类。我将为你提供达人的近期视频封面多宫格。 风格是一个相对抽象的概念，我们的打标原则是**"宁宽不紧"**：主要目的是为了判断该达人的外貌气质和视频调性，最容易让粉丝掏钱购买哪种【设计风格】的产品。

Task
请观察多宫格中达人的出镜外表（长相特点、妆容、发型）、当前穿搭、以及视频背景（置景调性），为该达人打上最符合的 1 个"核心调性标签"。

🎭 受控风格标签字典 (严格遵守)
请务必从以下 5 个宽泛的风格大类中选择 1 个。请结合括号内的"视觉锚点"和"适配货盘"进行综合判断：

1. Sweet_Girl (甜美邻家风)
视觉锚点：长相显幼态/甜美，笑容多；常穿浅色系、马卡龙色、碎花或带有蝴蝶结/蕾丝元素的衣服（对应女装: Sweet）；妆容偏粉嫩、元气。
潜意识适配货盘：甜美系女装、Sweet_Kawaii (甜美俏皮) 配饰、平价彩妆、可爱型生活好物。

2. Elegant_Lady (优雅轻熟风)
视觉锚点：气质成熟稳重、知性；穿搭有质感，常出现西装、真丝、修身连衣裙、纯色高级感服饰（对应女装: Elegant）；妆容精致干净，背景通常较有格调（如高级公寓、精致咖啡厅）。
潜意识适配货盘：中高客单价通勤女装、Vintage_Pearl (复古珍珠/法式) 或 精致K金 配饰、抗老护肤品、高级香水。

3. Cool_Trendy (个性潮辣风)
视觉锚点：视觉冲击力强；常穿紧身露肤辣妹装、Oversize高街潮牌（对应女装: Y2K_Spicy, Streetwear）；可能带有挑眉、截断式眼妆、欧美浓妆、明显纹身或亮色染发。
潜意识适配货盘：Y2K/高街女装、Statement_Chunky (夸张量感金属) 配饰、色彩大胆的彩妆、潮玩。

4. Clean_Minimalist (极简自然风)
视觉锚点：给人清爽、干净、不费力的感觉；常穿黑白灰、大地色系的基础款（对应女装: Minimalist）、纯色T恤/衬衫；伪素颜妆容，无夸张造型；背景通常非常干净（大白墙、原木风）。
潜意识适配货盘：基础款舒适女装、Dainty_Minimalist (极简纤细) 配饰、天然植物护肤品、极简家居用品。

5. Everyday_Life (亲民接地气)
视觉锚点：外表非常普通的路人/宝妈既视感；穿搭极为随意（居家服、宽松大T恤）；基本不化妆或画很随意的淡妆；视频背景充满真实的生活气息（如普通的厨房、堆着杂物的卧室、超市）。
潜意识适配货盘：9.9包邮大码/居家女装、廉价但实用的百货收纳、纸巾等日用消耗品、零食。（注：此类达人通常不适合带高级首饰或名媛装）。

👁️ 分析步骤与防误判指南
1. 看人优先于看货：即使一个甜美的女孩手里拿着一把电钻，她的整体风格依然是 Sweet_Girl ，这意味着你找她带甜美的裙子依然会爆单。
2. 宁宽不紧：如果达人风格介于两者之间，请选择包容性更强的那一个（例如，既不特别辣也不特别甜的普通穿搭，可归入 Clean_Minimalist 或 Everyday_Life ）。

## Output Format (严格的 JSON 输出)
【最高指令】：你的所有输出必须仅仅是一个合法的 JSON 对象。
绝对禁止输出任何前面的寒暄（如"好的"、"分析如下"）或后面的总结。
绝对禁止包含 Markdown 代码块标记（如 ```json 和 ```）。
请直接以 `{` 开头，并以 `}` 结尾。必须严格按照以下格式：

{
  "vibe_analysis_reason": "【第一步：先思考】请详细描述你观察到的达人长相特征、妆容浓淡、服装元素及背景调性，并解释为什么将她归入该风格。",
  "creator_vibe_tag": "填入 5 个风格标签中的 1 个（英文枚举值）"
}"""


def extract_creator_vibe(llm_raw_response: str) -> Optional[Dict[str, str]]:
    """
    强力提取器：无视大模型的任何废话和 Markdown 标记，直接抠出 JSON 并解析
    
    Args:
        llm_raw_response: LLM 原始响应文本
        
    Returns:
        {'reason': str, 'vibe_tag': str} 或 None
    """
    import re
    
    try:
        # 使用正则表达式，寻找从第一个 { 到最后一个 } 之间的所有内容
        # re.DOTALL 允许正则匹配跨越多行
        match = re.search(r'\{.*\}', llm_raw_response, re.DOTALL)
        
        if not match:
            print("❌ 未在模型输出中找到 JSON 括号结构。")
            return None
            
        json_string = match.group(0)
        
        # 将纯文本解析为 Python 字典
        data = json.loads(json_string)
        
        # 安全提取字段（使用 .get 防止字段缺失导致报错）
        result = {
            "reason": data.get("vibe_analysis_reason", ""),
            "vibe_tag": data.get("creator_vibe_tag", "Unknown")
        }
        
        return result
        
    except json.JSONDecodeError as e:
        print(f"❌ JSON 格式损坏无法解析: {e}")
        print(f"原始文本是: {llm_raw_response}")
        return None
    except Exception as e:
        print(f"❌ 发生未知错误: {e}")
        return None


class VibeTaggingAgent:
    """达人风格打标智能体
    
    分析达人的外貌气质和视频调性，输出风格标签。
    支持传入多张宫格图进行分析。
    
    注意：LLM API 是正式付费接口，不需要限流。
    但保留熔断器用于异常恢复，防止连续失败时持续调用。
    """
    
    # 有效的风格标签枚举
    VALID_VIBE_TAGS = [
        "Sweet_Girl",
        "Elegant_Lady", 
        "Cool_Trendy",
        "Clean_Minimalist",
        "Everyday_Life"
    ]
    
    def __init__(self):
        self.llm_client = LLMClient()
        
        # 保留熔断器：3次失败后熔断 120 秒，用于异常恢复
        self.circuit_breaker = CircuitBreaker(CircuitBreakerConfig(
            failure_threshold=3,
            success_threshold=2,
            timeout=120.0
        ))
        
        self.stats = {
            'processed': 0,
            'success': 0,
            'failed': 0,
            'total_time': 0.0
        }
    
    def execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行达人风格打标
        
        Args:
            payload: {
                'grid_paths': List[str],  # 宫格图路径列表（传入所有宫格图）
                'tk_handle': str          # 达人账号
            }
        
        Returns:
            {
                'tk_handle': str,
                'vibe_reason': str,
                'vibe_tag': str
            }
        """
        start_time = time.time()
        
        try:
            tk_handle = payload['tk_handle']
            grid_paths = payload.get('grid_paths', [])
            
            if not grid_paths:
                raise Exception("缺少宫格图路径")
            
            # 使用所有宫格图进行风格分析（关键：传入所有宫格图）
            print(f"  🎭 [VibeTagging] 分析达人: {tk_handle}")
            print(f"     使用 {len(grid_paths)} 张宫格图进行分析")
            
            # 调用 LLM（使用第一张图，但提示词中告知这是多宫格的一部分）
            # 注意：当前 LLM 客户端设计为处理单张图片
            # 如果需要处理多张图片，需要修改调用方式
            # 这里我们使用第一张宫格图进行分析（已包含多张视频封面）
            grid_path = grid_paths[0]
            print(f"     主宫格图: {Path(grid_path).name}")
            
            # 调用 LLM
            response = self.circuit_breaker.call(
                self._call_llm,
                grid_path
            )
            
            # 解析 LLM 原始响应
            raw_content = response['choices'][0]['message']['content']
            
            # 使用强力提取器解析 JSON
            extracted = extract_creator_vibe(raw_content)
            
            if not extracted:
                raise Exception("无法从 LLM 响应中提取风格标签")
            
            vibe_reason = extracted.get('reason', '')
            vibe_tag = extracted.get('vibe_tag', 'Unknown')
            
            # 验证风格标签是否有效
            if vibe_tag not in self.VALID_VIBE_TAGS:
                print(f"     ⚠️ 无效的风格标签: {vibe_tag}，将设为 Unknown")
                vibe_tag = "Unknown"
            
            output = {
                'tk_handle': tk_handle,
                'vibe_reason': vibe_reason,
                'vibe_tag': vibe_tag
            }
            
            # 统计
            self.stats['processed'] += 1
            self.stats['success'] += 1
            self.stats['total_time'] += time.time() - start_time
            
            print(f"     ✅ 风格打标完成: {vibe_tag}")
            
            return output
            
        except Exception as e:
            self.stats['processed'] += 1
            self.stats['failed'] += 1
            self.stats['total_time'] += time.time() - start_time
            print(f"     ❌ 风格打标失败: {e}")
            raise e
    
    def _call_llm(self, grid_path: str) -> Dict[str, Any]:
        """调用 LLM API"""
        return self.llm_client.chat_with_image(
            image_path=grid_path,
            prompt=VIBE_TAGGING_PROMPT,
            max_tokens=1000
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self.stats.copy()
        if stats['processed'] > 0:
            stats['avg_time'] = stats['total_time'] / stats['processed']
            stats['success_rate'] = stats['success'] / stats['processed']
        return stats


# ============================================================================
# 带货标签打标 Agent
# ============================================================================

class CategoryTaggingAgent:
    """带货标签打标智能体
    
    使用独立的火山引擎 Responses API（doubao-seed-2-0-mini）
    API 格式：POST /api/v3/responses
    """
    
    def __init__(self):
        # 使用独立的 API 配置
        self.api_url = CATEGORY_API_URL
        self.api_key = CATEGORY_API_KEY
        self.model = CATEGORY_MODEL
        self.timeout = 120
        
        # 保留熔断器：3次失败后熔断 120 秒，用于异常恢复
        self.circuit_breaker = CircuitBreaker(CircuitBreakerConfig(
            failure_threshold=3,
            success_threshold=2,
            timeout=120.0
        ))
        
        self.stats = {
            'processed': 0,
            'success': 0,
            'failed': 0,
            'total_time': 0.0
        }
    
    def execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行带货标签打标
        
        Args:
            payload: {
                'grid_paths': List[str],  # 宫格图路径列表
                'tk_handle': str          # 达人账号
            }
        
        Returns:
            {
                'tk_handle': str,
                'analysis_reason': str,
                'main_category_1': str,
                'sub_category_1': str,
                'main_category_2': str or None,
                'sub_category_2': str or None
            }
        """
        start_time = time.time()
        
        try:
            tk_handle = payload['tk_handle']
            grid_paths = payload.get('grid_paths', [])
            
            if not grid_paths:
                raise Exception("缺少宫格图路径")
            
            # 使用所有宫格图进行打标
            print(f"  🏷️ [CategoryTagging] 分析达人: {tk_handle}")
            print(f"     使用 {len(grid_paths)} 张宫格图:")
            for i, gp in enumerate(grid_paths, 1):
                print(f"       [{i}] {Path(gp).name}")
            
            # 调用 LLM（多图模式，使用 responses API）
            response = self.circuit_breaker.call(
                self._call_llm,
                grid_paths
            )
            
            # 解析结果
            result = self._parse_response(response)
            
            # 检查返回类型，如果是列表则取第一个元素
            if isinstance(result, list):
                if len(result) > 0:
                    result = result[0]
                else:
                    raise Exception("LLM返回空列表")
            elif not isinstance(result, dict):
                raise Exception(f"LLM返回未知类型: {type(result)}")
            
            # 提取并验证品类
            analysis_reason = result.get('analysis_reason', '')
            
            # 主品类 1
            cat1 = result.get('primary_category_1', {})
            main_category_1 = cat1.get('main_category', '其它')
            sub_category_1 = cat1.get('sub_category', '其它未知商品')
            
            # 主品类 2
            cat2 = result.get('primary_category_2', {})
            main_category_2 = cat2.get('main_category')
            sub_category_2 = cat2.get('sub_category')
            
            # 处理 None 值
            if main_category_2 == 'None' or not main_category_2:
                main_category_2 = None
                sub_category_2 = None
            
            output = {
                'tk_handle': tk_handle,
                'analysis_reason': analysis_reason,
                'main_category_1': main_category_1,
                'sub_category_1': sub_category_1,
                'main_category_2': main_category_2,
                'sub_category_2': sub_category_2
            }
            
            # 统计
            self.stats['processed'] += 1
            self.stats['success'] += 1
            self.stats['total_time'] += time.time() - start_time
            
            print(f"     ✅ 打标完成: 主品类1={main_category_1}-{sub_category_1}, 主品类2={main_category_2}-{sub_category_2}")
            
            return output
            
        except Exception as e:
            self.stats['processed'] += 1
            self.stats['failed'] += 1
            self.stats['total_time'] += time.time() - start_time
            print(f"     ❌ 打标失败: {e}")
            raise e
    
    def _call_llm(self, grid_paths: List[str]) -> Dict[str, Any]:
        """
        调用火山引擎 Responses API（多图模式）
        
        API 格式：POST /api/v3/responses
        """
        url = f"{self.api_url}/responses"
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # 读取所有图片并转 Base64
        input_content = []
        for grid_path in grid_paths:
            with open(grid_path, 'rb') as f:
                image_data = f.read()
            
            image_base64 = base64.b64encode(image_data).decode('utf-8')
            
            # 获取图片 MIME 类型
            image_format = Path(grid_path).suffix.lower().lstrip('.')
            if image_format == 'jpg':
                image_format = 'jpeg'
            mime_type = f"image/{image_format}"
            
            # 构建 image_data_url
            image_data_url = f"data:{mime_type};base64,{image_base64}"
            
            input_content.append({
                "type": "input_image",
                "image_url": image_data_url
            })
        
        # 添加文本提示词
        input_content.append({
            "type": "input_text",
            "text": CATEGORY_TAGGING_PROMPT
        })
        
        payload = {
            "model": self.model,
            "input": [
                {
                    "role": "user",
                    "content": input_content
                }
            ]
        }
        
        print(f"    🤖 调用模型: {self.model} (Responses API)")
        
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=self.timeout
        )
        
        if response.status_code != 200:
            raise Exception(f"Responses API 调用失败: {response.status_code} - {response.text}")
        
        return response.json()
    
    def _parse_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """
        解析 Responses API 的响应
        
        响应格式（新版本）：
        {
            "output": [
                {"type": "reasoning", ...},
                {"type": "message", "content": [{"type": "output_text", "text": "..."}]}
            ]
        }
        
        或旧版本：
        {
            "output": {"text": "..."}
        }
        """
        try:
            text_content = None
            
            # Responses API 新格式：output 是列表
            if 'output' in response:
                output = response['output']
                
                if isinstance(output, list):
                    # 新格式：output 是列表，需要找到 message 类型的项
                    for item in output:
                        if item.get('type') == 'message':
                            content = item.get('content', [])
                            for c in content:
                                if c.get('type') == 'output_text':
                                    text_content = c.get('text', '')
                                    break
                            if text_content:
                                break
                    
                    if not text_content:
                        raise Exception(f"未在 output 列表中找到 message 类型的文本内容")
                        
                elif isinstance(output, dict):
                    # 旧格式：output 是字典
                    text_content = output.get('text', '')
                else:
                    raise Exception(f"output 类型未知: {type(output)}")
                    
            elif 'choices' in response:
                # 兼容 OpenAI 格式
                text_content = response['choices'][0]['message']['content']
            else:
                raise Exception(f"未知的响应格式，顶层 keys: {response.keys()}")
            
            if not text_content:
                raise Exception(f"提取到的文本内容为空")
            
            # 尝试解析 JSON
            import re
            
            # 尝试直接解析
            try:
                return json.loads(text_content)
            except json.JSONDecodeError:
                pass
            
            # 尝试提取 JSON 块
            match = re.search(r'\{.*\}', text_content, re.DOTALL)
            if match:
                return json.loads(match.group(0))
            
            # 移除 markdown 标记
            text_content = re.sub(r'^```json\s*', '', text_content)
            text_content = re.sub(r'\s*```$', '', text_content)
            text_content = text_content.strip()
            
            return json.loads(text_content)
            
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            raise Exception(f"解析 Responses API 响应失败: {e}\n原始响应: {json.dumps(response, ensure_ascii=False)[:1000]}")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self.stats.copy()
        if stats['processed'] > 0:
            stats['avg_time'] = stats['total_time'] / stats['processed']
            stats['success_rate'] = stats['success'] / stats['processed']
        return stats


# ============================================================================
# 飞书字段更新工具
# ============================================================================

class FeishuFieldUpdater:
    """飞书多维表格字段更新工具"""
    
    def __init__(self):
        self.access_token: Optional[str] = None
        self.token_expires_at: float = 0
    
    def _get_access_token(self) -> str:
        """获取飞书 access_token"""
        import requests
        
        # 检查 token 是否过期
        if self.access_token and time.time() < self.token_expires_at:
            return self.access_token
        
        # 获取新 token
        config_file = Path.home() / ".openclaw/openclaw.json"
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        app_id = config['channels']['feishu']['appId']
        app_secret = config['channels']['feishu']['appSecret']
        
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        response = requests.post(url, json={'app_id': app_id, 'app_secret': app_secret})
        result = response.json()
        
        if result.get('code') != 0:
            raise Exception(f"获取 access_token 失败: {result.get('msg')}")
        
        self.access_token = result['tenant_access_token']
        self.token_expires_at = time.time() + result.get('expire', 7200) - 300
        
        return self.access_token
    
    def update_record_fields(
        self,
        record_id: str,
        app_token: str,
        table_id: str,
        fields: Dict[str, Any]
    ) -> bool:
        """
        更新飞书记录的指定字段
        
        Args:
            record_id: 记录 ID
            app_token: 应用 Token
            table_id: 表格 ID
            fields: 要更新的字段字典
        
        Returns:
            是否成功
        """
        import requests
        
        access_token = self._get_access_token()
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        payload = {'fields': fields}
        
        print(f"     🔗 API URL: {url}")
        print(f"     📦 Payload: {json.dumps(fields, ensure_ascii=False)}")
        
        response = requests.put(url, json=payload, headers=headers)
        result = response.json()
        
        print(f"     📥 Response: {json.dumps(result, ensure_ascii=False)}")
        
        if result.get('code') != 0:
            raise Exception(f"更新记录失败: {result.get('msg')}")
        
        return True
    
    def update_scoring_result(
        self,
        record_id: str,
        app_token: str,
        table_id: str,
        scoring_result: Dict[str, Any]
    ) -> bool:
        """
        更新视频评分结果到飞书
        
        Args:
            record_id: 记录 ID
            app_token: 应用 Token
            table_id: 表格 ID
            scoring_result: 评分结果
        
        Returns:
            是否成功
        """
        # 获取评分值并确保是正确的类型
        final_rating = scoring_result.get('final_star_rating')
        
        # 飞书字段类型处理：
        # - 如果字段是数字类型，传入浮点数
        # - 如果字段是文本类型，传入字符串
        # 这里我们尝试两种方式：先尝试数字，失败则转为字符串
        if final_rating is not None:
            # 确保是浮点数
            try:
                final_rating = float(final_rating)
            except (ValueError, TypeError):
                final_rating = 0.0
        else:
            final_rating = 0.0
        
        fields = {
            '视频最终评分': final_rating,  # 数字类型
            '评分原因': scoring_result.get('analysis_reason', '')
        }
        
        print(f"  📤 [FeishuUpdater] 更新评分字段:")
        print(f"     record_id: {record_id}")
        print(f"     视频最终评分: {fields['视频最终评分']} (类型: {type(fields['视频最终评分']).__name__})")
        print(f"     评分原因: {fields['评分原因'][:100]}..." if fields['评分原因'] else "     评分原因: None")
        
        try:
            result = self.update_record_fields(record_id, app_token, table_id, fields)
            print(f"     ✅ 更新成功")
            return result
        except Exception as e:
            error_msg = str(e)
            # 如果是字段类型转换错误，尝试用字符串格式
            if 'TextFieldConvFail' in error_msg or 'field type' in error_msg.lower():
                print(f"     ⚠️ 数字类型写入失败，尝试字符串格式...")
                fields['视频最终评分'] = str(final_rating)
                result = self.update_record_fields(record_id, app_token, table_id, fields)
                print(f"     ✅ 字符串格式更新成功")
                return result
            else:
                raise e
    
    def update_category_result(
        self,
        record_id: str,
        app_token: str,
        table_id: str,
        category_result: Dict[str, Any]
    ) -> bool:
        """
        更新带货标签结果到飞书
        
        Args:
            record_id: 记录 ID
            app_token: 应用 Token
            table_id: 表格 ID
            category_result: 打标结果
        
        Returns:
            是否成功
        """
        fields = {
            '主大类': category_result.get('main_category_1'),
            '主子类': category_result.get('sub_category_1'),
            '打标理由': category_result.get('analysis_reason')
        }
        
        print(f"  📤 [FeishuUpdater] 更新标签字段:")
        print(f"     record_id: {record_id}")
        print(f"     主大类: {fields['主大类']}")
        print(f"     主子类: {fields['主子类']}")
        print(f"     打标理由: {fields['打标理由'][:100]}..." if fields['打标理由'] else "     打标理由: None")
        
        result = self.update_record_fields(record_id, app_token, table_id, fields)
        print(f"     ✅ 更新成功")
        return result
    
    def update_vibe_result(
        self,
        record_id: str,
        app_token: str,
        table_id: str,
        vibe_result: Dict[str, Any]
    ) -> bool:
        """
        更新达人风格标签结果到飞书
        
        Args:
            record_id: 记录 ID
            app_token: 应用 Token
            table_id: 表格 ID
            vibe_result: 风格打标结果
        
        Returns:
            是否成功
        """
        fields = {
            '达人风格标签': vibe_result.get('vibe_tag'),
            '风格打标理由': vibe_result.get('vibe_reason')
        }
        
        print(f"  📤 [FeishuUpdater] 更新风格标签字段:")
        print(f"     record_id: {record_id}")
        print(f"     达人风格标签: {fields['达人风格标签']}")
        print(f"     风格打标理由: {fields['风格打标理由'][:100]}..." if fields['风格打标理由'] else "     风格打标理由: None")
        
        result = self.update_record_fields(record_id, app_token, table_id, fields)
        print(f"     ✅ 更新成功")
        return result


# ============================================================================
# 任务状态检查器（用于解耦任务执行）
# ============================================================================

class TaskCompletionChecker:
    """
    任务完成状态检查器
    
    用于检查飞书记录中各项任务的完成情况，支持解耦执行。
    """
    
    def __init__(self):
        self.feishu_updater = FeishuFieldUpdater()
    
    def check_record_completion(
        self,
        record_id: str,
        app_token: str,
        table_id: str
    ) -> Dict[str, bool]:
        """
        检查单条记录的任务完成情况
        
        Returns:
            {
                'has_grid': bool,      # 是否有宫格图
                'has_score': bool,     # 是否有评分
                'has_category': bool,  # 是否有带货标签
                'has_vibe': bool       # 是否有风格标签
            }
        """
        import requests
        
        access_token = self.feishu_updater._get_access_token()
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.get(url, headers=headers)
        result = response.json()
        
        if result.get('code') != 0:
            raise Exception(f"获取记录失败: {result.get('msg')}")
        
        fields = result.get('data', {}).get('record', {}).get('fields', {})
        
        return {
            'has_grid': bool(fields.get('视频截图')),
            'has_score': fields.get('视频最终评分') is not None and fields.get('视频最终评分') != '',
            'has_category': bool(fields.get('主大类')),
            'has_vibe': bool(fields.get('达人风格标签'))
        }
    
    def get_incomplete_tasks(
        self,
        record_id: str,
        app_token: str,
        table_id: str
    ) -> List[str]:
        """
        获取未完成的任务列表
        
        Returns:
            未完成任务名称列表，如 ['scoring', 'category', 'vibe']
        """
        completion = self.check_record_completion(record_id, app_token, table_id)
        
        incomplete = []
        
        # 如果有宫格图，才检查后续任务
        if completion['has_grid']:
            if not completion['has_score']:
                incomplete.append('scoring')
            if not completion['has_category']:
                incomplete.append('category')
            if not completion['has_vibe']:
                incomplete.append('vibe')
        
        return incomplete


# ============================================================================
# 测试代码
# ============================================================================

if __name__ == "__main__":
    # 测试视频评分
    print("="*70)
    print("测试视频质量评分")
    print("="*70)
    
    # 查找测试图片
    grid_dir = Path(__file__).parent.parent / "output" / "grids"
    test_images = list(grid_dir.glob("*.png"))
    
    if test_images:
        test_image = str(test_images[0])
        print(f"使用测试图片: {test_image}")
        
        agent = VideoScoringAgent()
        
        try:
            result = agent.execute({
                'tk_handle': 'test_creator',
                'grid_paths': [test_image],
                'views_list': [10000, 15000, 8000]
            })
            
            print("\n评分结果:")
            print(json.dumps(result, ensure_ascii=False, indent=2))
        except Exception as e:
            print(f"测试失败: {e}")
    else:
        print("未找到测试图片")
    
    print("\n" + "="*70)
    print("测试带货标签打标")
    print("="*70)
    
    if test_images:
        agent = CategoryTaggingAgent()
        
        try:
            result = agent.execute({
                'tk_handle': 'test_creator',
                'grid_paths': [test_image]
            })
            
            print("\n打标结果:")
            print(json.dumps(result, ensure_ascii=False, indent=2))
        except Exception as e:
            print(f"测试失败: {e}")
    
    print("\n" + "="*70)
    print("测试达人风格打标")
    print("="*70)
    
    if test_images:
        vibe_agent = VibeTaggingAgent()
        
        try:
            result = vibe_agent.execute({
                'tk_handle': 'test_creator',
                'grid_paths': [test_image]
            })
            
            print("\n风格打标结果:")
            print(json.dumps(result, ensure_ascii=False, indent=2))
        except Exception as e:
            print(f"测试失败: {e}")
