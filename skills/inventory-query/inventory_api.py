#!/usr/bin/env python3
"""
库存查询 API - OpenClaw Skill
高性能库存查询，基于 API 直接调用
支持自动 Token 刷新
"""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

try:
    import requests
except ImportError:
    raise ImportError("需要安装 requests 库: pip install requests")

from profile_manager import InventoryProfileManager


class InventoryAPI:
    """库存查询 API 客户端"""
    
    def __init__(
        self,
        config_path: Optional[str] = None,
        auto_refresh_token: bool = True,
        profile: Optional[str] = None
    ):
        """
        初始化 API 客户端
        
        Args:
            config_path: 配置文件路径
            auto_refresh_token: 是否自动刷新 Token（默认 True）
            profile: 指定使用的账号 profile；不传时使用当前激活账号
        """
        self.profile_manager = InventoryProfileManager(Path(__file__).parent / "config")
        self.profile = profile
        self.alias_map = self._load_alias_map()
        if config_path is None:
            config_path = self.profile_manager.get_config_path(profile)
        
        self.config_path = Path(config_path)
        self.config = self._load_config(self.config_path)
        self.session = requests.Session()
        self.last_query_time = 0
        self.auto_refresh_token = auto_refresh_token
        self._setup_session()
        
        # 检查 Token 状态
        if auto_refresh_token:
            self._check_and_refresh_token()
    
    def _load_alias_map(self) -> dict:
        """加载前台展示名到实际 SKU 的别名映射"""
        alias_path = Path(__file__).parent / 'title_sku_aliases.json'
        if not alias_path.exists():
            return {}
        try:
            with open(alias_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}

    def _load_config(self, config_path: Path) -> dict:
        """加载配置文件"""
        if not config_path.exists():
            raise FileNotFoundError(
                f"配置文件不存在: {config_path}\n"
                f"请复制 config/api_config.example.json 为 config/api_config.json 并配置"
            )
        
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _setup_session(self):
        """配置 Session"""
        self.session.headers.update(self.config['api']['headers'])
        self.timeout = self.config['api'].get('timeout', 10)
        
        # 配置代理（如果环境变量设置了代理）
        import os
        http_proxy = os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy')
        https_proxy = os.environ.get('HTTPS_PROXY') or os.environ.get('https_proxy')
        if http_proxy or https_proxy:
            self.session.proxies = {
                'http': http_proxy,
                'https': https_proxy
            }
    
    def _check_and_refresh_token(self):
        """检查并自动刷新 Token"""
        try:
            from token_manager import TokenManager
            
            manager = TokenManager(self.config_path)
            status = manager.get_current_token_status()
            
            if 'error' in status:
                print(f"⚠️  Token 状态检查失败: {status['error']}")
                return
            
            # Token 已过期，尝试自动刷新
            if status.get('expired'):
                print("❌ Token 已过期，尝试自动刷新...")
                if manager.auto_refresh():
                    # 重新加载配置
                    self.config = self._load_config(self.config_path)
                    self._setup_session()
                    print("✅ Token 自动刷新成功")
                else:
                    print("❌ Token 自动刷新失败，请手动更新")
                    print("   运行: python token_manager.py refresh")
            
            # Token 即将过期（少于 3 天），提醒刷新
            elif status.get('needs_refresh'):
                print(f"⚠️  Token 将在 {status['days_remaining']} 天后过期")
                print(f"   建议运行: python token_manager.py refresh")
            
        except ImportError:
            # token_manager 不可用，跳过自动刷新
            pass
        except Exception as e:
            print(f"⚠️  Token 检查失败: {e}")
    
    def _apply_rate_limit(self):
        """应用请求限流"""
        if not self.config.get('rate_limit', {}).get('enabled', False):
            return
        
        delay = self.config['rate_limit'].get('delay_between_queries', 0.5)
        current_time = time.time()
        time_since_last_query = current_time - self.last_query_time
        
        if time_since_last_query < delay:
            time.sleep(delay - time_since_last_query)
        
        self.last_query_time = time.time()
    
    def query_single(self, sku: str, country: Optional[str] = None, fuzzy: bool = True, max_pages: int = 10) -> dict:
        """
        查询单个 SKU 库存
        
        说明：
        1. 先按系统实际 SKU 编码字段 `sku` 查询
        2. 若查不到，再自动回退按前台展示名称字段 `title` 查询
        3. 典型场景：前台看到的是 `F-002`，系统实际库存编码是 `FSL001`
        
        Args:
            sku: SKU 代码或前台展示名称
            country: 国家代码（可选）
            fuzzy: 是否使用模糊匹配（默认 True）
            max_pages: 最大查询页数（默认 10，即最多查询 500 条记录）
        """
        if not country:
            country = self.config.get('default_country', 'thailand')
        
        api_config = self.config['api']
        url = api_config['base_url'] + api_config['endpoints']['query']
        
        # 先按系统库存编码 sku 查询
        primary_result = self._query_by_field(sku, country, fuzzy=fuzzy, max_pages=max_pages, match_field='sku')
        if 'error' not in primary_result or primary_result.get('error_type') != 'no_match':
            return primary_result

        # 如果存在本地别名映射，优先用映射后的真实 SKU 再查一次
        alias_sku = self.alias_map.get(sku)
        if alias_sku and alias_sku != sku:
            alias_result = self._query_by_field(alias_sku, country, fuzzy=False, max_pages=max_pages, match_field='sku')
            if 'error' not in alias_result:
                alias_result['fallback_used'] = True
                alias_result['fallback_type'] = 'alias_map'
                alias_result['query_input'] = sku
                alias_result['alias_sku'] = alias_sku
                return alias_result

        # 如果按 sku 没查到，再回退按前台展示名称 title 查询
        title_result = self._query_by_title(sku, country, fuzzy=fuzzy, max_pages=max_pages)
        if 'error' not in title_result:
            title_result['fallback_used'] = True
            title_result['fallback_type'] = 'title_query'
            title_result['query_input'] = sku
            return title_result

        primary_result['error'] = f"未找到匹配的 SKU、别名映射或前台名称（已搜索 {max_pages * 50} 条记录）"
        return primary_result
        
        # 默认返回未找到
        return {
            "sku": sku,
            "error": "未找到匹配的 SKU",
            "available": 0,
            "timestamp": datetime.now().isoformat()
        }
    
    def _build_payload(self, sku: str, country: str, page_no: int = 1) -> dict:
        """构建 POST 请求的 payload"""
        payload_template = self.config['api'].get('payload_template', {})
        payload = {}
        for key, value in payload_template.items():
            if value == "{sku}":
                payload[key] = sku
            elif value == "{country}":
                payload[key] = country
            elif key == "pageNo":
                payload[key] = page_no
            else:
                payload[key] = value
        return payload
    
    def _build_params(self, sku: str, country: str, page_no: int = 1) -> dict:
        """构建 GET 请求的参数"""
        params_template = self.config['api'].get('params_template', {})
        params = {}
        for key, value in params_template.items():
            if value == "{sku}":
                params[key] = sku
            elif value == "{country}":
                params[key] = country
            elif key == "pageNo":
                params[key] = page_no
            else:
                params[key] = value
        return params
    
    def _parse_response(self, response_data: dict, sku: str, fuzzy: bool = True, match_field: str = 'sku') -> dict:
        """
        解析 API 响应
        
        Args:
            response_data: API 响应数据
            sku: 查询的 SKU 或前台展示名称
            fuzzy: 是否使用模糊匹配（默认 True）
            match_field: 匹配字段，默认 sku；查不到时可回退到 title
        """
        response_format = self.config['api'].get('response_format', {})
        
        try:
            # 检查响应状态
            status_path = response_format.get('status_path', 'code')
            success_value = response_format.get('success_value', 0)
            
            status = self._get_nested_value(response_data, status_path)
            if status != success_value:
                error_msg = self._get_nested_value(
                    response_data,
                    response_format.get('error_path', 'message')
                )
                return {
                    "sku": sku,
                    "error": error_msg or f"API 返回错误（状态码: {status}）",
                    "available": 0,
                    "timestamp": datetime.now().isoformat()
                }
            
            # 提取数据
            data_path = response_format.get('data_path', 'data')
            
            # 特殊处理：如果 data_path 指向数组的第一个元素（如 data.page.rows.0）
            # 我们需要遍历整个数组找到匹配的 SKU
            if '.rows.' in data_path or data_path.endswith('.rows'):
                # 获取 rows 数组
                rows_path = data_path.rsplit('.', 1)[0] if data_path.endswith('.0') else data_path
                rows = self._get_nested_value(response_data, rows_path)
                
                if not rows or not isinstance(rows, list):
                    return {
                        "sku": sku,
                        "error": "未找到数据",
                        "error_type": "no_match",
                        "available": 0,
                        "timestamp": datetime.now().isoformat()
                    }
                
                # 查找匹配的记录
                matched_rows = []
                sku_lower = sku.lower()
                
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    
                    row_value = str(row.get(match_field, '') or '')
                    if fuzzy:
                        # 模糊匹配：包含查询字符串
                        if sku_lower in row_value.lower():
                            matched_rows.append(row)
                    else:
                        # 精确匹配
                        if row_value.lower() == sku_lower:
                            matched_rows.append(row)
                
                if not matched_rows:
                    return {
                        "sku": sku,
                        "error": f"未找到匹配的 SKU（返回了 {len(rows)} 条记录）",
                        "error_type": "no_match",
                        "available": 0,
                        "timestamp": datetime.now().isoformat()
                    }
                
                # 如果有多个匹配，返回所有匹配项的汇总
                if len(matched_rows) > 1:
                    fields = response_format.get('fields', {})
                    total_available = sum(
                        self._get_nested_value(row, fields.get('available', 'available')) or 0
                        for row in matched_rows
                    )
                    total_onhand = sum(
                        self._get_nested_value(row, fields.get('total', 'total')) or 0
                        for row in matched_rows
                    )
                    total_reserved = sum(
                        self._get_nested_value(row, fields.get('reserved', 'reserved')) or 0
                        for row in matched_rows
                    )
                    
                    # 收集所有匹配的 SKU 名称
                    matched_skus = [row.get('sku', '') for row in matched_rows]
                    matched_titles = [row.get('title', '') for row in matched_rows]
                    
                    return {
                        "sku": sku,
                        "available": total_available,
                        "total": total_onhand,
                        "reserved": total_reserved,
                        "status": "multiple_matches",
                        "matched_count": len(matched_rows),
                        "matched_skus": matched_skus,
                        "matched_titles": matched_titles,
                        "matched_by": match_field,
                        "timestamp": datetime.now().isoformat()
                    }
                
                # 单个匹配
                data = matched_rows[0]
            else:
                # 普通路径处理
                data = self._get_nested_value(response_data, data_path)
                
                if not data:
                    return {
                        "sku": sku,
                        "error": "未找到数据",
                        "error_type": "no_match",
                        "available": 0,
                        "timestamp": datetime.now().isoformat()
                    }
            
            # 提取库存字段
            fields = response_format.get('fields', {})
            result = {
                "sku": data.get('sku', sku),  # 使用实际返回的 SKU
                "title": data.get('title', ''),
                "available": self._get_nested_value(data, fields.get('available', 'available')) or 0,
                "total": self._get_nested_value(data, fields.get('total', 'total')) or 0,
                "reserved": self._get_nested_value(data, fields.get('reserved', 'reserved')) or 0,
                "status": self._get_nested_value(data, fields.get('status', 'status')) or "unknown",
                "matched_by": match_field,
                "timestamp": datetime.now().isoformat()
            }
            
            return result
            
        except Exception as e:
            return {
                "sku": sku,
                "error": f"解析响应失败: {e}",
                "available": 0,
                "timestamp": datetime.now().isoformat()
            }
    
    def _get_nested_value(self, data: dict, path: str):
        """获取嵌套字典的值，支持点号路径"""
        if not path:
            return data
        
        keys = path.split('.')
        value = data
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
            
            if value is None:
                return None
        
        return value
    
    def _query_by_field(self, keyword: str, country: Optional[str] = None, fuzzy: bool = True, max_pages: int = 10, match_field: str = 'sku') -> dict:
        """按指定字段查询。注意：后端接口会先按传入关键词过滤结果。"""
        if not country:
            country = self.config.get('default_country', 'thailand')

        api_config = self.config['api']
        url = api_config['base_url'] + api_config['endpoints']['query']

        for page_no in range(1, max_pages + 1):
            self._apply_rate_limit()
            try:
                if api_config['method'].upper() == 'POST':
                    payload = self._build_payload(keyword, country, page_no=page_no)
                    content_type = api_config['headers'].get('content-type', 'application/json')
                    if 'application/x-www-form-urlencoded' in content_type:
                        response = self.session.post(url, data=payload, timeout=self.timeout)
                    else:
                        response = self.session.post(url, json=payload, timeout=self.timeout)
                else:
                    params = self._build_params(keyword, country, page_no=page_no)
                    response = self.session.get(url, params=params, timeout=self.timeout)

                response.raise_for_status()
                response_data = response.json()

                if response_data.get('code') == 401006:
                    return {
                        "sku": keyword,
                        "error": "Token 已过期（错误代码：401006）",
                        "error_type": "auth_error",
                        "suggestion": "请运行: python token_manager.py refresh",
                        "available": 0,
                        "timestamp": datetime.now().isoformat()
                    }

                result = self._parse_response(response_data, keyword, fuzzy=fuzzy, match_field=match_field)
                if 'error' not in result or 'no_match' not in result.get('error_type', ''):
                    return result

                if page_no == max_pages:
                    return result
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    return {
                        "sku": keyword,
                        "error": "认证失败，Token 可能已过期",
                        "error_type": "auth_error",
                        "suggestion": "请运行: python token_manager.py refresh",
                        "available": 0,
                        "timestamp": datetime.now().isoformat()
                    }
                raise
            except Exception as e:
                label = '前台名称' if match_field == 'title' else 'SKU'
                return {
                    "sku": keyword,
                    "error": f"按{label}查询失败: {e}",
                    "available": 0,
                    "timestamp": datetime.now().isoformat()
                }

        return {
            "sku": keyword,
            "error": f"按字段 {match_field} 未找到匹配数据",
            "error_type": "no_match",
            "available": 0,
            "timestamp": datetime.now().isoformat()
        }

    def _query_by_title(self, sku: str, country: Optional[str] = None, fuzzy: bool = True, max_pages: int = 10) -> dict:
        """按前台展示名称 title 回退查询"""
        return self._query_by_field(sku, country=country, fuzzy=fuzzy, max_pages=max_pages, match_field='title')

    def query_multiple(self, sku_list: List[str], country: Optional[str] = None, fuzzy: bool = True) -> Dict[str, dict]:
        """
        查询多个 SKU 库存
        
        Args:
            sku_list: SKU 列表
            country: 国家代码（可选）
            fuzzy: 是否使用模糊匹配（默认 True）
        """
        results = {}
        
        for sku in sku_list:
            try:
                result = self.query_single(sku, country, fuzzy=fuzzy)
                results[sku] = result
            except Exception as e:
                results[sku] = {
                    "sku": sku,
                    "error": str(e),
                    "available": 0,
                    "timestamp": datetime.now().isoformat()
                }
        
        return results


# OpenClaw Skill 接口
def query_inventory(
    sku_list: List[str],
    country: Optional[str] = None,
    fuzzy: bool = True,
    profile: Optional[str] = None
) -> Dict[str, dict]:
    """
    查询库存信息
    
    Args:
        sku_list: SKU 列表
        country: 国家代码（可选，默认使用配置中的默认国家）
        fuzzy: 是否使用模糊匹配（默认 True）
               - True: 模糊匹配，查询 "wj" 会返回所有包含 "wj" 的 SKU
               - False: 精确匹配，只返回完全匹配的 SKU
        profile: 指定账号 profile；不传时使用当前激活账号
    
    Returns:
        字典，key 为 SKU，value 为库存信息
        
        当模糊匹配到多个 SKU 时，返回的数据包含：
        - available/total/reserved: 所有匹配 SKU 的总和
        - matched_count: 匹配到的 SKU 数量
        - matched_skus: 匹配到的所有 SKU 列表
        
    Example:
        >>> # 精确查询
        >>> results = query_inventory(["TH-DR-8801"], fuzzy=False)
        >>> print(results["TH-DR-8801"]["available"])
        150
        
        >>> # 模糊查询
        >>> results = query_inventory(["wj"], fuzzy=True)
        >>> print(f"找到 {results['wj']['matched_count']} 个匹配")
        >>> print(f"总库存: {results['wj']['available']}")
    """
    api = InventoryAPI(profile=profile)
    return api.query_multiple(sku_list, country, fuzzy=fuzzy)


if __name__ == "__main__":
    import sys
    
    # 测试代码
    print("库存查询 API 测试")
    print("=" * 50)
    
    try:
        # 从命令行参数获取 SKU，如果没有则使用默认值
        if len(sys.argv) > 1:
            test_skus = sys.argv[1:]
        else:
            test_skus = ["bu0010"]
            print("\n提示: 可以通过命令行参数指定 SKU，例如:")
            print("  python inventory_api.py wj bu0020 TH-DR-8801")
        
        print(f"\n测试查询 SKU: {test_skus}")
        
        results = query_inventory(test_skus)
        
        for sku, data in results.items():
            print(f"\nSKU: {sku}")
            if 'error' in data:
                print(f"  错误: {data['error']}")
            else:
                print(f"  可用库存: {data['available']}")
                print(f"  总库存: {data['total']}")
                print(f"  仓库锁定待发库存: {data['reserved']}")
                print(f"  状态: {data['status']}")
                if 'matched_count' in data:
                    print(f"  匹配数量: {data['matched_count']}")
                    print(f"  匹配的 SKU: {', '.join(data['matched_skus'])}")
        
        print("\n✓ 测试完成")
        
    except FileNotFoundError as e:
        print(f"\n✗ {e}")
        print("\n请先配置 API:")
        print("1. 复制 config/api_config.example.json 为 config/api_config.json")
        print("2. 按照 docs/API_DISCOVERY_GUIDE.md 找到 API 端点")
        print("3. 更新配置文件中的 API 信息")
    except Exception as e:
        print(f"\n✗ 错误: {e}")
        import traceback
        traceback.print_exc()
