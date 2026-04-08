"""
图片处理工具模块

提供图片下载、Base64 编码等功能
"""

import base64
import requests
from typing import Optional
from io import BytesIO


def download_and_encode_image(
    url: str,
    timeout: int = 10,
    max_retries: int = 3
) -> Optional[str]:
    """
    下载图片并转换为 Base64 编码字符串
    
    Args:
        url: 图片 URL
        timeout: 请求超时时间（秒）
        max_retries: 最大重试次数
        
    Returns:
        Base64 编码的图片字符串，失败返回 None
        
    Example:
        >>> base64_img = download_and_encode_image("https://example.com/image.jpg")
        >>> if base64_img:
        >>>     print(f"成功编码，长度: {len(base64_img)}")
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                     'AppleWebKit/537.36 (KHTML, like Gecko) '
                     'Chrome/120.0.0.0 Safari/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=timeout,
                stream=True
            )
            response.raise_for_status()
            
            # 读取图片内容
            image_bytes = BytesIO(response.content)
            
            # 转换为 Base64
            base64_encoded = base64.b64encode(image_bytes.getvalue()).decode('utf-8')
            
            return base64_encoded
            
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                continue
            print(f"⚠️ 图片下载超时（已重试 {max_retries} 次）: {url}")
            return None
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                continue
            print(f"⚠️ 图片下载失败: {url}, 错误: {str(e)}")
            return None
            
        except Exception as e:
            print(f"⚠️ 图片编码失败: {url}, 错误: {str(e)}")
            return None
    
    return None


def batch_download_and_encode(
    urls: list[str],
    min_success: int = 3,
    timeout: int = 10
) -> list[str]:
    """
    批量下载并编码图片，确保至少成功指定数量
    
    Args:
        urls: 图片 URL 列表
        min_success: 最少成功数量
        timeout: 单个请求超时时间
        
    Returns:
        成功编码的 Base64 字符串列表
        
    Raises:
        RuntimeError: 当成功数量少于 min_success 时
    """
    encoded_images = []
    
    for url in urls:
        encoded = download_and_encode_image(url, timeout=timeout)
        if encoded:
            encoded_images.append(encoded)
            
        # 达到最少成功数量即可提前返回
        if len(encoded_images) >= min_success:
            break
    
    if len(encoded_images) < min_success:
        raise RuntimeError(
            f"图片下载失败：需要至少 {min_success} 张，实际成功 {len(encoded_images)} 张"
        )
    
    return encoded_images


if __name__ == "__main__":
    # 测试图片下载与编码
    test_url = "https://picsum.photos/200/300"
    
    print("🧪 测试图片下载与 Base64 编码")
    print(f"测试 URL: {test_url}")
    print()
    
    result = download_and_encode_image(test_url)
    
    if result:
        print(f"✅ 成功编码")
        print(f"Base64 长度: {len(result)} 字符")
        print(f"前 100 字符: {result[:100]}...")
    else:
        print("❌ 编码失败")
