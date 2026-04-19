#!/usr/bin/env python3
"""
测试 Token 管理功能
"""

import sys
from pathlib import Path

# 添加当前目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from token_manager import TokenManager


def test_token_status():
    """测试 Token 状态检查"""
    print("=" * 60)
    print("测试 1: 检查当前 Token 状态")
    print("=" * 60)
    
    manager = TokenManager()
    status = manager.get_current_token_status()
    
    if 'error' in status:
        print(f"❌ 错误: {status['error']}")
        return False
    
    print(f"✅ Token 状态:")
    print(f"   有效: {'是' if status.get('valid') else '否'}")
    print(f"   过期时间: {status['expires_at'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   剩余天数: {status['days_remaining']} 天")
    print(f"   需要刷新: {'是' if status.get('needs_refresh') else '否'}")
    print(f"   已过期: {'是' if status.get('expired') else '否'}")
    
    return True


def test_token_decode():
    """测试 Token 解析"""
    print("\n" + "=" * 60)
    print("测试 2: 解析 JWT Token")
    print("=" * 60)
    
    manager = TokenManager()
    
    # 从配置中读取 Token
    import json
    with open(manager.config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    import re
    cookie = config['api']['headers']['cookie']
    match = re.search(r'muc_token=([^;]+)', cookie)
    
    if not match:
        print("❌ 配置中未找到 muc_token")
        return False
    
    token = match.group(1)
    info = manager.decode_jwt_token(token)
    
    if not info:
        print("❌ Token 解析失败")
        return False
    
    print(f"✅ Token 信息:")
    print(f"   签发时间: {info.get('iat')}")
    print(f"   过期时间: {info.get('exp')}")
    
    # 解析 info 字段
    if 'info' in info:
        try:
            import json
            user_info = json.loads(info['info'])
            print(f"   用户 ID: {user_info.get('uid')}")
            print(f"   项目 ID: {user_info.get('puid')}")
        except:
            pass
    
    return True


def test_browser_extraction():
    """测试浏览器 Token 提取"""
    print("\n" + "=" * 60)
    print("测试 3: 从浏览器提取 Token")
    print("=" * 60)
    
    manager = TokenManager()
    
    # 尝试从 Chrome 提取
    print("尝试从 Chrome 浏览器提取...")
    token = manager.extract_token_from_browser('chrome')
    
    if token:
        print(f"✅ 成功提取 Token")
        print(f"   Token 长度: {len(token)} 字符")
        
        # 检查提取的 Token 状态
        status = manager.check_token_expiry(token)
        print(f"   过期时间: {status['expires_at'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   剩余天数: {status['days_remaining']} 天")
        return True
    else:
        print("⚠️  未能从浏览器提取 Token")
        print("   这是正常的，如果：")
        print("   - 未在 Chrome 中登录 BigSeller")
        print("   - 使用其他浏览器")
        print("   - browser-cookie3 未正确安装")
        return None  # None 表示跳过，不算失败


def main():
    """运行所有测试"""
    print("\n🧪 Token 管理功能测试\n")
    
    results = []
    
    # 测试 1: Token 状态
    results.append(("Token 状态检查", test_token_status()))
    
    # 测试 2: Token 解析
    results.append(("Token 解析", test_token_decode()))
    
    # 测试 3: 浏览器提取（可选）
    browser_result = test_browser_extraction()
    if browser_result is not None:
        results.append(("浏览器 Token 提取", browser_result))
    
    # 汇总结果
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{status} - {name}")
    
    print(f"\n总计: {passed}/{total} 测试通过")
    
    if passed == total:
        print("\n🎉 所有测试通过！Token 管理功能正常")
        return 0
    else:
        print("\n⚠️  部分测试失败，请检查配置")
        return 1


if __name__ == "__main__":
    sys.exit(main())
