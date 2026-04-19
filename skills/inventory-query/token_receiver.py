#!/usr/bin/env python3
"""
Token 自动接收服务
接收来自浏览器扩展的 Token 更新请求
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional, Dict

from profile_manager import InventoryProfileManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TokenReceiverHandler(BaseHTTPRequestHandler):
    """Token 接收处理器"""
    
    config_path = Path(__file__).parent / "config" / "api_config.json"

    def do_GET(self):
        """处理 GET 请求"""
        if self.path == '/health':
            self.send_json_response(200, {
                'success': True,
                'message': 'Token receiver is running',
                'config_path': str(self.config_path),
                'timestamp': datetime.now().isoformat()
            })
        else:
            self.send_error(404, "Not Found")
    
    def do_POST(self):
        """处理 POST 请求"""
        if self.path == '/update-token':
            self.handle_token_update()
        else:
            self.send_error(404, "Not Found")
    
    def do_OPTIONS(self):
        """处理 CORS 预检请求"""
        self.send_response(200)
        self.send_cors_headers()
        self.end_headers()
    
    def send_cors_headers(self):
        """发送 CORS 头"""
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
    
    def handle_token_update(self):
        """处理 Token 更新请求"""
        try:
            # 读取请求体
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            token = data.get('token')
            full_cookie = data.get('cookie') or data.get('full_cookie')
            cookie_updates = data.get('cookies') or {}
            jsession_id = data.get('jsession_id')
            jsessionid = data.get('jsessionid')
            requested_profile = data.get('profile')
            source = data.get('source')

            if jsession_id and 'JSESSIONID' not in cookie_updates:
                cookie_updates['JSESSIONID'] = jsession_id

            if jsessionid and 'JSESSIONID' not in cookie_updates:
                cookie_updates['JSESSIONID'] = jsessionid

            if token and 'muc_token' not in cookie_updates:
                cookie_updates['muc_token'] = token

            if not token and not full_cookie and not cookie_updates:
                self.send_json_response(400, {
                    'success': False,
                    'error': '至少需要提供 token、cookie 或 cookies'
                })
                return

            # 健康检查不应污染真实配置
            if source == 'health-check':
                self.send_json_response(200, {
                    'success': True,
                    'message': 'Health check passed',
                    'timestamp': datetime.now().isoformat()
                })
                return

            profile_name = self.resolve_target_profile(
                requested_profile=requested_profile,
                token=token,
                full_cookie=full_cookie
            )
            
            success = self.update_config_auth(
                token=token,
                full_cookie=full_cookie,
                cookie_updates=cookie_updates or None,
                profile_name=profile_name
            )
            
            if success:
                warehouse_sync = self.sync_profile_runtime_settings(profile_name)
                logger.info("✅ 认证 Cookie 更新成功")
                response = {
                    'success': True,
                    'message': '认证 Cookie 更新成功',
                    'profile': profile_name,
                    'updated_fields': sorted(
                        [key for key in cookie_updates.keys() if key] +
                        (['cookie'] if full_cookie else [])
                    ),
                    'timestamp': datetime.now().isoformat()
                }
                if warehouse_sync.get('success'):
                    response['warehouse_ids'] = warehouse_sync.get('warehouse_ids')
                    response['warehouse_ids_changed'] = warehouse_sync.get('changed', False)
                elif warehouse_sync.get('error'):
                    response['warehouse_sync_error'] = warehouse_sync.get('error')
                self.send_json_response(200, response)
            else:
                logger.error("❌ 认证 Cookie 更新失败")
                self.send_json_response(500, {
                    'success': False,
                    'error': '更新配置文件失败'
                })
                
        except json.JSONDecodeError:
            self.send_json_response(400, {
                'success': False,
                'error': '无效的 JSON 数据'
            })
        except ValueError as e:
            self.send_json_response(409, {
                'success': False,
                'error': str(e)
            })
        except Exception as e:
            logger.error(f"❌ 处理请求失败: {e}")
            self.send_json_response(500, {
                'success': False,
                'error': str(e)
            })

    def resolve_target_profile(
        self,
        requested_profile: Optional[str] = None,
        token: Optional[str] = None,
        full_cookie: Optional[str] = None
    ) -> str:
        """解析本次同步应写入哪个 profile。"""
        manager = InventoryProfileManager(self.config_path.parent)

        if requested_profile:
            if not manager.profile_exists(requested_profile):
                raise ValueError(
                    f"profile 不存在: {requested_profile}。"
                    f"请先运行 python profile_manager.py create {requested_profile} --activate"
                )
            return manager.normalize_profile_name(requested_profile)

        matched_profile = manager.match_profile_for_auth(token=token, cookie_string=full_cookie)
        if matched_profile:
            return matched_profile

        active_profile = manager.get_active_profile_name() or "default"
        incoming_identity = manager.extract_identity_from_auth(token=token, cookie_string=full_cookie)
        active_identity = manager.get_profile_identity(active_profile)

        if manager.identities_differ(active_identity, incoming_identity):
            raise ValueError(
                "检测到你切到了一个未登记的新 BigSeller 账号。"
                "为避免覆盖当前店铺配置，请先运行 "
                "`python profile_manager.py create 新账号名 --activate`，"
                "再刷新 BigSeller 库存页并重新同步。"
            )

        return active_profile
    
    def update_config_auth(
        self,
        token: Optional[str] = None,
        full_cookie: Optional[str] = None,
        cookie_updates: Optional[Dict[str, str]] = None,
        profile_name: Optional[str] = None
    ) -> bool:
        """更新配置文件中的认证 Cookie。"""
        try:
            from token_manager import TokenManager

            manager = TokenManager(profile=profile_name)
            success = manager.update_config_auth(
                new_token=token,
                new_cookie=full_cookie,
                cookie_updates=cookie_updates,
                create_backup=True
            )

            if success:
                manager.profile_manager.activate_profile(profile_name or manager.profile_manager.get_active_profile_name() or "default")
                logger.info("📝 配置文件中的认证 Cookie 已更新")

            return success

        except Exception as e:
            logger.error(f"❌ 更新配置失败: {e}")
            return False

    def sync_profile_runtime_settings(self, profile_name: Optional[str] = None) -> Dict[str, object]:
        """同步认证后，自动探测并写回当前账号的 warehouseIds。"""
        try:
            from token_manager import TokenManager

            manager = TokenManager(profile=profile_name)
            result = manager.sync_profile_warehouse_ids(profile_name=profile_name)
            if result.get('success'):
                logger.info(
                    "📦 warehouseIds 已同步到 profile %s: %s",
                    result.get('profile'),
                    result.get('warehouse_ids')
                )
            else:
                logger.warning(
                    "⚠️ 自动同步 warehouseIds 失败（profile=%s）: %s",
                    profile_name,
                    result.get('error')
                )
            return result
        except Exception as e:
            logger.warning("⚠️ 自动同步 warehouseIds 时出现异常: %s", e)
            return {
                'success': False,
                'error': str(e)
            }

    def update_config_token(self, new_token: str, jsession_id: Optional[str] = None) -> bool:
        """兼容旧调用，保留 jsession_id 参数。"""
        cookie_updates = {}
        if jsession_id:
            cookie_updates['JSESSIONID'] = jsession_id
        return self.update_config_auth(token=new_token, cookie_updates=cookie_updates or None)
    
    def send_json_response(self, status_code: int, data: dict):
        """发送 JSON 响应"""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_cors_headers()
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))
    
    def log_message(self, format, *args):
        """自定义日志格式"""
        logger.info(f"{self.address_string()} - {format % args}")


def start_server(host: str = 'localhost', port: int = 8765):
    """启动 Token 接收服务"""
    server_address = (host, port)
    httpd = HTTPServer(server_address, TokenReceiverHandler)
    
    logger.info(f"🚀 Token 接收服务已启动")
    logger.info(f"📡 监听地址: http://{host}:{port}")
    logger.info(f"📝 配置文件: {TokenReceiverHandler.config_path}")
    logger.info(f"⏸️  按 Ctrl+C 停止服务")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("\n⏹️  服务已停止")
        httpd.shutdown()


def main():
    """命令行入口"""
    import sys
    
    host = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8765
    
    start_server(host, port)


if __name__ == "__main__":
    main()
