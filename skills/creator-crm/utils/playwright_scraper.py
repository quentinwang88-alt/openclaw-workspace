"""
Playwright 截图工具 - 极简成本方案

功能：使用无头浏览器截取 TikTok 达人主页全屏截图并转为 Base64
"""

import base64
import asyncio
from typing import Optional, Dict, Any
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout


async def capture_tiktok_profile_screenshot(
    tk_profile_url: str,
    timeout: int = 30000,
    viewport_width: int = 1280,
    viewport_height: int = 1024
) -> str:
    """
    使用 Playwright 截取 TikTok 达人主页全屏截图
    
    Args:
        tk_profile_url: TikTok 达人主页 URL (例如: https://www.tiktok.com/@username)
        timeout: 页面加载超时时间（毫秒）
        viewport_width: 视口宽度
        viewport_height: 视口高度
        
    Returns:
        Base64 编码的截图字符串
        
    Raises:
        RuntimeError: 当截图失败时
        
    Example:
        >>> url = "https://www.tiktok.com/@fashionista_th"
        >>> base64_img = await capture_tiktok_profile_screenshot(url)
        >>> print(f"截图长度: {len(base64_img)}")
    """
    try:
        async with async_playwright() as p:
            # 启动无头浏览器
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-setuid-sandbox'
                ]
            )
            
            # 创建浏览器上下文（模拟真实用户）
            context = await browser.new_context(
                viewport={'width': viewport_width, 'height': viewport_height},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/120.0.0.0 Safari/537.36'
            )
            
            # 创建新页面
            page = await context.new_page()
            
            # 导航到达人主页
            await page.goto(tk_profile_url, wait_until='networkidle', timeout=timeout)
            
            # 等待关键元素加载（达人主页的视频网格）
            try:
                # TikTok 主页的视频网格容器
                await page.wait_for_selector('[data-e2e="user-post-item"]', timeout=10000)
            except PlaywrightTimeout:
                # 如果找不到视频网格，可能是私密账号或无视频，继续截图
                print("⚠️ 未检测到视频网格，可能是私密账号或无视频")
            
            # 滚动页面以加载更多内容（可选）
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight / 2)')
            await asyncio.sleep(1)  # 等待图片加载
            
            # 截取全屏
            screenshot_bytes = await page.screenshot(full_page=True, type='png')
            
            # 关闭浏览器
            await browser.close()
            
            # 转换为 Base64
            base64_encoded = base64.b64encode(screenshot_bytes).decode('utf-8')
            
            return base64_encoded
            
    except PlaywrightTimeout:
        raise RuntimeError(f"页面加载超时: {tk_profile_url}")
    except Exception as e:
        raise RuntimeError(f"截图失败: {str(e)}")


def capture_tiktok_profile_screenshot_sync(
    tk_profile_url: str,
    timeout: int = 30000,
    viewport_width: int = 1280,
    viewport_height: int = 1024
) -> str:
    """
    同步版本的截图函数（方便在非异步环境中调用）
    
    Args:
        tk_profile_url: TikTok 达人主页 URL
        timeout: 页面加载超时时间（毫秒）
        viewport_width: 视口宽度
        viewport_height: 视口高度
        
    Returns:
        Base64 编码的截图字符串
    """
    return asyncio.run(
        capture_tiktok_profile_screenshot(
            tk_profile_url, timeout, viewport_width, viewport_height
        )
    )


async def extract_profile_bio(tk_profile_url: str, timeout: int = 30000) -> Dict[str, Any]:
    """
    提取达人主页的 Bio 信息（可选功能）
    
    Args:
        tk_profile_url: TikTok 达人主页 URL
        timeout: 页面加载超时时间（毫秒）
        
    Returns:
        包含 Bio 和粉丝数的字典
        
    Example:
        >>> url = "https://www.tiktok.com/@fashionista_th"
        >>> info = await extract_profile_bio(url)
        >>> print(info['bio'])
    """
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/120.0.0.0 Safari/537.36'
            )
            page = await context.new_page()
            
            await page.goto(tk_profile_url, wait_until='networkidle', timeout=timeout)
            
            # 提取 Bio（TikTok 的 Bio 选择器可能会变化）
            bio = ""
            try:
                bio_element = await page.query_selector('[data-e2e="user-bio"]')
                if bio_element:
                    bio = await bio_element.inner_text()
            except:
                pass
            
            # 提取粉丝数
            followers_count = 0
            try:
                followers_element = await page.query_selector('[data-e2e="followers-count"]')
                if followers_element:
                    followers_text = await followers_element.inner_text()
                    # 解析粉丝数（例如 "50.2K" -> 50200）
                    followers_count = _parse_follower_count(followers_text)
            except:
                pass
            
            await browser.close()
            
            return {
                "bio": bio,
                "followers_count": followers_count
            }
            
    except Exception as e:
        raise RuntimeError(f"提取 Bio 失败: {str(e)}")


def _parse_follower_count(text: str) -> int:
    """
    解析粉丝数文本（例如 "50.2K" -> 50200）
    
    Args:
        text: 粉丝数文本
        
    Returns:
        整数粉丝数
    """
    text = text.strip().upper()
    
    if 'M' in text:
        # 百万级别
        num = float(text.replace('M', ''))
        return int(num * 1_000_000)
    elif 'K' in text:
        # 千级别
        num = float(text.replace('K', ''))
        return int(num * 1_000)
    else:
        # 直接数字
        try:
            return int(text)
        except:
            return 0


# ============================================================================
# 测试代码
# ============================================================================

async def _test_screenshot():
    """测试截图功能"""
    print("🧪 测试 Playwright 截图功能")
    print()
    
    # 测试 URL（使用一个公开的 TikTok 账号）
    test_url = "https://www.tiktok.com/@khaby.lame"
    
    print(f"测试 URL: {test_url}")
    print("正在截图...")
    print()
    
    try:
        base64_img = await capture_tiktok_profile_screenshot(test_url)
        
        print(f"✅ 截图成功")
        print(f"Base64 长度: {len(base64_img):,} 字符")
        print(f"前 100 字符: {base64_img[:100]}...")
        print()
        
        # 可选：保存到本地文件用于验证
        import os
        output_dir = os.path.join(os.path.dirname(__file__), '..', 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        output_path = os.path.join(output_dir, 'test_screenshot.png')
        with open(output_path, 'wb') as f:
            f.write(base64.b64decode(base64_img))
        
        print(f"✅ 截图已保存到: {output_path}")
        
    except Exception as e:
        print(f"❌ 截图失败: {str(e)}")


if __name__ == "__main__":
    # 运行测试
    asyncio.run(_test_screenshot())
