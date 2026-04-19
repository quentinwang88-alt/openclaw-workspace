#!/usr/bin/env python3
"""
BigSeller 认证 Cookie 自动管理器
自动从浏览器提取最新的 BigSeller 认证信息
"""

import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Optional, Dict, List
from datetime import datetime
import base64

from profile_manager import InventoryProfileManager

try:
    import requests
except ImportError:
    requests = None

AUTH_COOKIE_ORDER = ("JSESSIONID", "muc_token", "language", "fingerPrint")


class TokenManager:
    """认证 Cookie 自动管理器"""
    
    def __init__(
        self,
        config_path: Optional[str] = None,
        profile: Optional[str] = None,
        config_dir: Optional[str] = None
    ):
        """初始化 Token 管理器"""
        if config_dir is None and config_path:
            resolved_path = Path(config_path)
            if resolved_path.parent.name == "profiles":
                config_dir = resolved_path.parent.parent
            else:
                config_dir = resolved_path.parent

        if config_dir is None:
            config_dir = Path(__file__).parent / "config"

        self.profile_manager = InventoryProfileManager(config_dir)
        self.profile = profile

        if config_path is None:
            config_path = self.profile_manager.get_config_path(profile)

        self.config_path = Path(config_path)

    def _load_current_config(self) -> Dict:
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _save_current_config(self, config: Dict):
        if self.profile:
            is_active = self.profile_manager.get_active_profile_name() == self.profile
            self.profile_manager.save_profile(self.profile, config, activate=is_active)
        else:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            self.profile_manager.sync_active_profile_from_legacy()

    def _build_backup_path(self) -> Path:
        backup_dir = self.config_path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        return backup_dir / (
            f"{self.config_path.stem}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
    
    @staticmethod
    def parse_cookie_string(cookie_string: str) -> Dict[str, str]:
        """将 Cookie 字符串解析为字典。"""
        cookies = {}

        if not cookie_string:
            return cookies

        for raw_part in cookie_string.split(';'):
            part = raw_part.strip()
            if not part or '=' not in part:
                continue

            name, value = part.split('=', 1)
            name = name.strip()
            value = value.strip()
            if name and value:
                cookies[name] = value

        return cookies

    @staticmethod
    def build_cookie_string(cookies: Dict[str, str]) -> str:
        """按稳定顺序构建 Cookie 字符串。"""
        ordered_names = []
        seen = set()

        for name in AUTH_COOKIE_ORDER:
            if cookies.get(name):
                ordered_names.append(name)
                seen.add(name)

        for name, value in cookies.items():
            if name in seen or not value:
                continue
            ordered_names.append(name)

        if not ordered_names:
            return ""

        return "; ".join(f"{name}={cookies[name]}" for name in ordered_names) + ";"

    @staticmethod
    def extract_warehouse_ids_from_inventory_response(response_data: Dict) -> List[int]:
        """从库存查询响应中提取当前账号对应的 warehouseIds。"""
        data = response_data.get('data') or {}
        qo = data.get('qo') or {}
        warehouse_ids = InventoryProfileManager.normalize_warehouse_ids(qo.get('warehouseIds'))
        if warehouse_ids:
            return warehouse_ids

        rows = (data.get('page') or {}).get('rows') or []
        row_ids = [
            row.get('warehouseId')
            for row in rows
            if isinstance(row, dict) and row.get('warehouseId') is not None
        ]
        return InventoryProfileManager.normalize_warehouse_ids(row_ids)

    def extract_auth_from_browser(self, browser: str = "chrome") -> Optional[Dict[str, object]]:
        """
        从浏览器提取 BigSeller 认证 Cookie。

        Args:
            browser: 浏览器类型 (chrome/firefox/safari/edge)

        Returns:
            包含完整 Cookie、muc_token 和 JSESSIONID 的字典，失败时返回 None
        """
        try:
            import browser_cookie3
        except ImportError:
            print("❌ 需要安装 browser_cookie3: pip install browser-cookie3")
            return None

        try:
            browser_map = {
                'chrome': browser_cookie3.chrome,
                'firefox': browser_cookie3.firefox,
                'safari': browser_cookie3.safari,
                'edge': browser_cookie3.edge,
            }

            if browser.lower() not in browser_map:
                print(f"❌ 不支持的浏览器: {browser}")
                print(f"支持的浏览器: {', '.join(browser_map.keys())}")
                return None

            cookie_jar = browser_map[browser.lower()](domain_name='bigseller.pro')
            cookie_values = {}

            for cookie in cookie_jar:
                if cookie.value:
                    cookie_values[cookie.name] = cookie.value

            if not cookie_values:
                print(f"❌ 在 {browser} 浏览器中未找到 BigSeller Cookie")
                print("请确保：")
                print("1. 已在浏览器中登录 BigSeller")
                print("2. 访问过 https://www.bigseller.pro")
                return None

            cookie_string = self.build_cookie_string(cookie_values)
            token = cookie_values.get('muc_token')
            jsessionid = cookie_values.get('JSESSIONID')

            print(f"✅ 从 {browser} 浏览器提取到 BigSeller 认证 Cookie")
            print(f"   muc_token: {'已找到' if token else '未找到'}")
            print(f"   JSESSIONID: {'已找到' if jsessionid else '未找到'}")

            return {
                'cookies': cookie_values,
                'cookie_string': cookie_string,
                'token': token,
                'jsessionid': jsessionid
            }

        except Exception as e:
            print(f"❌ 提取认证 Cookie 失败: {e}")
            return None

    def extract_token_from_browser(self, browser: str = "chrome") -> Optional[str]:
        """
        从浏览器提取 muc_token。
        保留这个旧接口以兼容现有脚本。
        
        Args:
            browser: 浏览器类型 (chrome/firefox/safari/edge)
        
        Returns:
            muc_token 字符串，如果失败返回 None
        """
        auth = self.extract_auth_from_browser(browser)
        if not auth:
            return None
        return auth.get('token')
    
    def decode_jwt_token(self, token: str) -> Optional[Dict]:
        """
        解析 JWT Token 获取信息
        
        Args:
            token: JWT Token 字符串
        
        Returns:
            Token 信息字典，包含过期时间等
        """
        try:
            # JWT 格式: header.payload.signature
            parts = token.split('.')
            if len(parts) != 3:
                return None
            
            # 解码 payload（第二部分）
            payload = parts[1]
            # 添加必要的 padding
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += '=' * padding
            
            decoded = base64.urlsafe_b64decode(payload)
            return json.loads(decoded)
        except Exception as e:
            print(f"❌ 解析 Token 失败: {e}")
            return None
    
    def check_token_expiry(self, token: str) -> Dict:
        """
        检查 Token 是否即将过期
        
        Returns:
            {
                'valid': bool,
                'expires_at': datetime,
                'days_remaining': int,
                'needs_refresh': bool
            }
        """
        info = self.decode_jwt_token(token)
        if not info or 'exp' not in info:
            return {
                'valid': False,
                'error': '无法解析 Token'
            }
        
        exp_timestamp = info['exp']
        expires_at = datetime.fromtimestamp(exp_timestamp)
        now = datetime.now()
        days_remaining = (expires_at - now).days
        
        return {
            'valid': expires_at > now,
            'expires_at': expires_at,
            'days_remaining': days_remaining,
            'needs_refresh': days_remaining < 3,  # 少于 3 天需要刷新
            'expired': expires_at <= now
        }
    
    def update_config_auth(
        self,
        new_token: Optional[str] = None,
        new_cookie: Optional[str] = None,
        cookie_updates: Optional[Dict[str, str]] = None,
        create_backup: bool = True
    ) -> bool:
        """
        更新配置文件中的认证 Cookie。

        Args:
            new_token: 新的 muc_token（兼容旧调用）
            new_cookie: 完整的 Cookie 字符串
            cookie_updates: 按 Cookie 名称更新的字段
            create_backup: 是否先创建备份

        Returns:
            是否更新成功
        """
        try:
            config = self._load_current_config()

            headers = config.setdefault('api', {}).setdefault('headers', {})
            current_cookie = headers.get('cookie', '')

            if create_backup:
                backup_path = self._build_backup_path()
                with open(backup_path, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=2, ensure_ascii=False)

            # 解析当前配置中的 cookie
            merged_values = self.parse_cookie_string(current_cookie)
            
            # 如果有新的完整 cookie，先合并它
            if new_cookie:
                new_cookie_values = self.parse_cookie_string(new_cookie)
                merged_values.update(new_cookie_values)
            
            # 应用单独的 cookie 更新（优先级最高）
            if cookie_updates:
                for name, value in cookie_updates.items():
                    if value:
                        merged_values[name] = value
            
            # 应用 token 更新
            if new_token:
                merged_values['muc_token'] = new_token
            
            if not merged_values:
                raise ValueError("没有可写入的 Cookie 信息")
            
            merged_cookie = self.build_cookie_string(merged_values)

            if not merged_cookie:
                raise ValueError("生成的 Cookie 为空")

            headers['cookie'] = merged_cookie

            self._save_current_config(config)

            merged_values = self.parse_cookie_string(merged_cookie)
            print("✅ 认证 Cookie 已更新到配置文件")
            print(f"   muc_token: {'已更新' if merged_values.get('muc_token') else '缺失'}")
            print(f"   JSESSIONID: {'已更新' if merged_values.get('JSESSIONID') else '缺失'}")
            return True

        except Exception as e:
            print(f"❌ 更新配置失败: {e}")
            return False

    def discover_warehouse_ids(self, timeout: Optional[int] = None) -> Dict:
        """
        调用 BigSeller 库存接口自动探测当前账号的默认 warehouseIds。
        会移除请求中的 warehouseIds 过滤条件，避免旧账号配置干扰新账号。
        """
        if requests is None:
            return {
                'success': False,
                'error': '需要安装 requests 库: pip install requests'
            }

        try:
            config = self._load_current_config()
            api_config = config.get('api', {})
            endpoints = api_config.get('endpoints', {})
            url = f"{api_config.get('base_url', '')}{endpoints.get('query', '')}"
            if not url:
                return {
                    'success': False,
                    'error': '配置缺少库存查询接口地址'
                }

            payload = deepcopy(api_config.get('payload_template', {}))
            payload['pageNo'] = 1
            payload['pageSize'] = 1
            payload['searchContent'] = ''
            payload.pop('warehouseIds', None)
            payload.pop('warehouseId', None)

            session = requests.Session()
            session.headers.update(api_config.get('headers', {}))
            http_proxy = os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy')
            https_proxy = os.environ.get('HTTPS_PROXY') or os.environ.get('https_proxy')
            if http_proxy or https_proxy:
                session.proxies = {
                    'http': http_proxy,
                    'https': https_proxy
                }

            request_timeout = timeout or api_config.get('timeout', 10)
            method = api_config.get('method', 'POST').upper()
            content_type = api_config.get('headers', {}).get('content-type', 'application/json')

            if method == 'POST':
                if 'application/x-www-form-urlencoded' in content_type:
                    response = session.post(url, data=payload, timeout=request_timeout)
                else:
                    response = session.post(url, json=payload, timeout=request_timeout)
            else:
                response = session.get(url, params=payload, timeout=request_timeout)

            response.raise_for_status()
            response_data = response.json()

            if response_data.get('code') != 0:
                return {
                    'success': False,
                    'error': response_data.get('msg') or f"API 返回错误（状态码: {response_data.get('code')})"
                }

            warehouse_ids = self.extract_warehouse_ids_from_inventory_response(response_data)
            if not warehouse_ids:
                return {
                    'success': False,
                    'error': '接口返回成功，但未解析到 warehouseIds'
                }

            rows = (response_data.get('data') or {}).get('page', {}).get('rows') or []
            sample_sku = rows[0].get('sku') if rows and isinstance(rows[0], dict) else None
            return {
                'success': True,
                'warehouse_ids': warehouse_ids,
                'sample_sku': sample_sku
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def sync_profile_warehouse_ids(self, profile_name: Optional[str] = None, timeout: Optional[int] = None) -> Dict:
        """自动探测并写回指定 profile 的 warehouseIds。"""
        target_profile = profile_name or self.profile or self.profile_manager.get_active_profile_name()
        if not target_profile:
            return {
                'success': False,
                'error': '未找到可同步的 profile'
            }

        discovery = self.discover_warehouse_ids(timeout=timeout)
        if not discovery.get('success'):
            return discovery

        current_config = self.profile_manager.load_profile_config(target_profile)
        current_ids = InventoryProfileManager.normalize_warehouse_ids(
            current_config.get('api', {}).get('payload_template', {}).get('warehouseIds')
        )
        new_ids = self.profile_manager.set_profile_warehouse_ids(target_profile, discovery['warehouse_ids'])
        return {
            'success': True,
            'profile': target_profile,
            'warehouse_ids': new_ids,
            'changed': new_ids != current_ids,
            'sample_sku': discovery.get('sample_sku')
        }

    def update_config_token(self, new_token: str) -> bool:
        """
        更新配置文件中的 Token
        
        Args:
            new_token: 新的 muc_token
        
        Returns:
            是否更新成功
        """
        return self.update_config_auth(new_token=new_token)
    
    def auto_refresh(self, browser: str = "chrome") -> bool:
        """
        自动刷新 Token（从浏览器提取并更新配置）
        
        Args:
            browser: 浏览器类型
        
        Returns:
            是否刷新成功
        """
        print("🔄 开始自动刷新认证 Cookie...")

        auth = self.extract_auth_from_browser(browser)
        if not auth:
            return False

        new_token = auth.get('token')
        if not new_token:
            print("❌ 浏览器中未找到 muc_token，无法完成刷新")
            return False

        status = self.check_token_expiry(new_token)
        if not status.get('valid'):
            print(f"❌ 提取的 Token 无效或已过期")
            return False

        print(f"✅ Token 有效期至: {status['expires_at'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   剩余天数: {status['days_remaining']} 天")

        if self.update_config_auth(
            new_token=new_token,
            new_cookie=auth.get('cookie_string'),
            cookie_updates=auth.get('cookies')
        ):
            print("✅ 认证 Cookie 自动刷新完成")
            warehouse_sync = self.sync_profile_warehouse_ids()
            if warehouse_sync.get('success'):
                changed_label = '已同步' if warehouse_sync.get('changed') else '已确认'
                print(
                    f"✅ warehouseIds {changed_label}: "
                    f"{warehouse_sync.get('warehouse_ids')}"
                )
            else:
                print(f"⚠️  自动同步 warehouseIds 失败: {warehouse_sync.get('error')}")
            return True

        return False
    
    def get_current_token_status(self) -> Dict:
        """获取当前配置中 Token 的状态"""
        try:
            config = self._load_current_config()
            
            cookie = config['api']['headers']['cookie']
            cookies = self.parse_cookie_string(cookie)
            token = cookies.get('muc_token')

            if not token:
                return {'error': '配置中未找到 muc_token'}

            status = self.check_token_expiry(token)
            status['jsessionid_present'] = bool(cookies.get('JSESSIONID'))
            status['cookie_names'] = list(cookies.keys())
            return status
            
        except Exception as e:
            return {'error': str(e)}


def main():
    """命令行工具"""
    import sys

    argv = sys.argv[1:]
    profile = None
    cleaned_args = []
    i = 0
    while i < len(argv):
        if argv[i] == "--profile":
            if i + 1 >= len(argv):
                print("❌ --profile 需要提供 profile 名称")
                sys.exit(1)
            profile = argv[i + 1]
            i += 2
            continue

        cleaned_args.append(argv[i])
        i += 1

    manager = TokenManager(profile=profile)
    
    if len(cleaned_args) < 1:
        print("Token 管理工具")
        print("\n用法:")
        print("  python token_manager.py status          # 查看当前 Token 状态")
        print("  python token_manager.py refresh [浏览器] # 自动刷新 Token")
        print("  python token_manager.py check [浏览器]   # 检查浏览器中的 Token")
        print("  python token_manager.py sync-warehouses  # 自动同步 warehouseIds")
        print("  python token_manager.py server [端口]    # 启动自动接收服务")
        print("  python token_manager.py status --profile 店铺A")
        print("\n支持的浏览器: chrome, firefox, safari, edge")
        print("默认浏览器: chrome")
        sys.exit(1)
    
    command = cleaned_args[0].lower()
    browser = cleaned_args[1] if len(cleaned_args) > 1 else "chrome"

    if profile:
        print(f"📁 当前 profile: {profile}")
    else:
        active = manager.profile_manager.get_active_profile_name()
        if active:
            print(f"📁 当前激活 profile: {active}")
    
    if command == "status":
        print("📊 当前 Token 状态:")
        status = manager.get_current_token_status()
        
        if 'error' in status:
            print(f"❌ {status['error']}")
        elif status.get('expired'):
            print(f"❌ Token 已过期")
            print(f"   过期时间: {status['expires_at'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   JSESSIONID: {'已配置' if status.get('jsessionid_present') else '缺失'}")
        elif status.get('needs_refresh'):
            print(f"⚠️  Token 即将过期，建议刷新")
            print(f"   过期时间: {status['expires_at'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   剩余天数: {status['days_remaining']} 天")
            print(f"   JSESSIONID: {'已配置' if status.get('jsessionid_present') else '缺失'}")
        else:
            print(f"✅ Token 有效")
            print(f"   过期时间: {status['expires_at'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   剩余天数: {status['days_remaining']} 天")
            print(f"   JSESSIONID: {'已配置' if status.get('jsessionid_present') else '缺失'}")
    
    elif command == "refresh":
        success = manager.auto_refresh(browser)
        sys.exit(0 if success else 1)

    elif command == "sync-warehouses":
        result = manager.sync_profile_warehouse_ids(profile_name=profile)
        if result.get('success'):
            changed_label = '已更新' if result.get('changed') else '已确认'
            print(f"✅ warehouseIds {changed_label}: {result.get('warehouse_ids')}")
            if result.get('sample_sku'):
                print(f"   示例 SKU: {result['sample_sku']}")
            sys.exit(0)

        print(f"❌ 同步 warehouseIds 失败: {result.get('error')}")
        sys.exit(1)
    
    elif command == "check":
        auth = manager.extract_auth_from_browser(browser)
        if auth and auth.get('token'):
            status = manager.check_token_expiry(auth['token'])
            print(f"\n📊 浏览器中的 Token 状态:")
            print(f"   过期时间: {status['expires_at'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   剩余天数: {status['days_remaining']} 天")
            print(f"   JSESSIONID: {'已找到' if auth.get('jsessionid') else '未找到'}")
    
    elif command == "server":
        # 启动自动接收服务
        from token_receiver import start_server
        port = int(cleaned_args[1]) if len(cleaned_args) > 1 else 8765
        print(f"\n🚀 启动 Token 自动接收服务...")
        print(f"📡 监听端口: {port}")
        print(f"💡 请安装并启用浏览器扩展")
        print(f"📂 扩展目录: {Path(__file__).parent / 'browser-extension'}")
        start_server('localhost', port)
    
    else:
        print(f"❌ 未知命令: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
