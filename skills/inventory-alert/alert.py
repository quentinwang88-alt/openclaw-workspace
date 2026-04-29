#!/usr/bin/env python3
"""
库存预警 - OpenClaw Skill
监控库存预计可售天数，低于阈值时发送飞书通知
"""

import json
import re
import sys
import subprocess
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import parse_qs, urlparse

# 添加 inventory-query 到路径
sys.path.insert(0, str(Path(__file__).parent.parent / "inventory-query"))

try:
    from inventory_api import InventoryAPI
except ImportError:
    raise ImportError("需要先安装 inventory-query skill")

try:
    from profile_manager import InventoryProfileManager
except ImportError:
    InventoryProfileManager = None

try:
    import requests
except ImportError:
    raise ImportError("需要安装 requests 库: pip install requests")


class InventoryAlertAPI(InventoryAPI):
    """扩展库存查询 API，支持获取预计可售天数"""
    
    def _parse_response(self, response_data: dict, sku: str, fuzzy: bool = True) -> dict:
        """
        解析 API 响应，包含预计可售天数等字段
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
            
            # 处理 rows 数组
            if '.rows.' in data_path or data_path.endswith('.rows'):
                rows_path = data_path.rsplit('.', 1)[0] if data_path.endswith('.0') else data_path
                rows = self._get_nested_value(response_data, rows_path)
                
                if not rows or not isinstance(rows, list):
                    return {
                        "sku": sku,
                        "error": "未找到数据",
                        "available": 0,
                        "timestamp": datetime.now().isoformat()
                    }
                
                # 查找匹配的 SKU
                matched_rows = []
                sku_lower = sku.lower()
                
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    
                    row_sku = row.get('sku', '')
                    if fuzzy:
                        if sku_lower in row_sku.lower():
                            matched_rows.append(row)
                    else:
                        if row_sku.lower() == sku_lower:
                            matched_rows.append(row)
                
                if not matched_rows:
                    return {
                        "sku": sku,
                        "error": f"未找到匹配的 SKU（返回了 {len(rows)} 条记录）",
                        "error_type": "no_match",
                        "available": 0,
                        "timestamp": datetime.now().isoformat()
                    }
                
                # 返回所有匹配项（用于全量查询）
                results = []
                for row in matched_rows:
                    result = self._extract_inventory_data(row, response_format)
                    results.append(result)
                
                # 如果只有一个匹配，直接返回
                if len(results) == 1:
                    return results[0]
                
                # 多个匹配，返回列表
                return {
                    "sku": sku,
                    "matched_count": len(results),
                    "items": results,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                data = self._get_nested_value(response_data, data_path)
                if not data:
                    return {
                        "sku": sku,
                        "error": "未找到数据",
                        "available": 0,
                        "timestamp": datetime.now().isoformat()
                    }
                
                return self._extract_inventory_data(data, response_format)
            
        except Exception as e:
            return {
                "sku": sku,
                "error": f"解析响应失败: {e}",
                "available": 0,
                "timestamp": datetime.now().isoformat()
            }
    
    def _extract_inventory_data(self, data: dict, response_format: dict) -> dict:
        """从单条数据中提取库存信息"""
        fields = response_format.get('fields', {})
        
        result = {
            "sku": data.get('sku', ''),
            "title": data.get('title', ''),
            "image": data.get('image', ''),
            "available": self._get_nested_value(data, fields.get('available', 'available')) or 0,
            "total": self._get_nested_value(data, fields.get('total', 'total')) or 0,
            "reserved": self._get_nested_value(data, fields.get('reserved', 'reserved')) or 0,
            "status": self._get_nested_value(data, fields.get('status', 'status')) or "unknown",
            # 新增字段
            "avg_daily_sales": data.get('avgDailySales', 0) or 0,
            "purchase_sale_days": data.get('purchaseSaleDays', 0) or 0,
            "timestamp": datetime.now().isoformat()
        }
        
        return result
    
    def query_all_skus(self, country: Optional[str] = None, max_pages: int = 20) -> List[dict]:
        """
        查询所有 SKU 的库存信息
        
        Args:
            country: 国家代码（可选）
            max_pages: 最大查询页数（默认 20，即最多查询 1000 条记录）
        
        Returns:
            所有 SKU 的库存信息列表
        """
        if not country:
            country = self.config.get('default_country', 'thailand')
        
        api_config = self.config['api']
        url = api_config['base_url'] + api_config['endpoints']['query']
        
        all_items = []
        
        for page_no in range(1, max_pages + 1):
            self._apply_rate_limit()
            
            # 使用空字符串查询所有 SKU
            payload = self._build_payload("", country, page_no=page_no)
            
            content_type = api_config['headers'].get('content-type', 'application/json')
            if 'application/x-www-form-urlencoded' in content_type:
                response = self.session.post(url, data=payload, timeout=self.timeout)
            else:
                response = self.session.post(url, json=payload, timeout=self.timeout)
            
            response.raise_for_status()
            data = response.json()
            
            # 检查状态
            response_format = self.config['api'].get('response_format', {})
            status = self._get_nested_value(data, response_format.get('status_path', 'code'))
            if status != response_format.get('success_value', 0):
                error_msg = self._get_nested_value(data, response_format.get('error_path', 'msg'))
                print(f"  ⚠️ API 返回错误: code={status}, msg={error_msg}")
                break
            
            # 提取数据
            rows_path = response_format.get('data_path', 'data.page.rows')
            rows = self._get_nested_value(data, rows_path)
            
            if not rows or not isinstance(rows, list):
                break
            
            # 提取每一行的数据
            for row in rows:
                if isinstance(row, dict):
                    item = self._extract_inventory_data(row, response_format)
                    all_items.append(item)
            
            # 如果返回的数据少于 pageSize，说明已经是最后一页
            if len(rows) < payload.get('pageSize', 50):
                break
        
        return all_items


class InventoryAlert:
    """库存预警"""
    
    def __init__(self, config_path: Optional[str] = None, profile: Optional[str] = None):
        """初始化库存预警"""
        self.profile = profile or self._resolve_active_profile()
        if config_path is None:
            config_path = Path(__file__).parent / "config" / "alert_config.json"
        
        self.config = self._load_config(config_path, profile=self.profile)
        self.api = InventoryAlertAPI(profile=self.profile)

    def _resolve_active_profile(self) -> Optional[str]:
        """尽量复用当前激活的 inventory-query profile。"""
        if InventoryProfileManager is None:
            return None

        try:
            manager = InventoryProfileManager(
                Path(__file__).parent.parent / "inventory-query" / "config"
            )
            return manager.get_active_profile_name()
        except Exception:
            return None

    @staticmethod
    def _deep_merge_dicts(base: dict, override: dict) -> dict:
        """递归合并配置，后者覆盖前者。"""
        merged = deepcopy(base)
        for key, value in (override or {}).items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = InventoryAlert._deep_merge_dicts(merged[key], value)
            else:
                merged[key] = deepcopy(value)
        return merged

    def _load_config(self, config_path: Path, profile: Optional[str] = None) -> dict:
        """加载配置文件"""
        if not config_path.exists():
            # 使用默认配置
            return {
                "threshold_days": 10,
                "restock": {
                    "priority_strategy": "both",
                    "enable_abc_layering": True,
                    "purchase_days": 3,
                    "logistics_days": 12,
                    "safety_days": 2,
                    "safety_days_by_class": {
                        "A": 5,
                        "B": 3,
                        "C": 1,
                        "NEW": 3,
                    },
                    "abc_thresholds": {
                        "a_cumulative": 0.70,
                        "b_cumulative": 0.90,
                    },
                    "new_sku_age_days": 7,
                    "unknown_age_default_class": "B",
                },
                "feishu": {
                    "enabled": False,
                    "webhook_url": "",
                    "user_webhook_url": "",
                    "group_webhook_url": ""
                }
            }
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        profile_overrides = (config.get('profiles') or {}).get(profile, {}) if profile else {}
        if profile_overrides:
            config = self._deep_merge_dicts(config, profile_overrides)

        return config

    @staticmethod
    def _parse_feishu_bitable_url(url: str) -> Optional[Dict[str, Optional[str]]]:
        """解析飞书多维表格链接，支持 base/wiki/open-api 格式。"""
        if not url:
            return None

        cleaned = url.strip()

        match = re.search(r'/base/([a-zA-Z0-9]+)', cleaned)
        if match:
            parsed = urlparse(cleaned)
            params = parse_qs(parsed.query)
            table_id = params.get('table', [None])[0]
            if table_id:
                return {
                    'app_token': match.group(1),
                    'table_id': table_id,
                    'view_id': params.get('view', [None])[0],
                    'is_wiki': False,
                }

        match = re.search(r'/wiki/([a-zA-Z0-9]+)', cleaned)
        if match:
            parsed = urlparse(cleaned)
            params = parse_qs(parsed.query)
            table_id = params.get('table', [None])[0]
            if table_id:
                return {
                    'app_token': match.group(1),
                    'table_id': table_id,
                    'view_id': params.get('view', [None])[0],
                    'is_wiki': True,
                }

        match = re.search(r'/apps/([a-zA-Z0-9]+)/tables/([a-zA-Z0-9]+)', cleaned)
        if match:
            return {
                'app_token': match.group(1),
                'table_id': match.group(2),
                'view_id': None,
                'is_wiki': False,
            }

        match = re.match(r'^([a-zA-Z0-9]+)[/,]([a-zA-Z0-9]+)$', cleaned)
        if match:
            return {
                'app_token': match.group(1),
                'table_id': match.group(2),
                'view_id': None,
                'is_wiki': False,
            }

        return None

    def _resolve_wiki_bitable_app_token(self, wiki_token: str) -> Optional[str]:
        """将 wiki token 解析为底层 bitable app_token。"""
        access_token = self.get_tenant_access_token()
        if not access_token:
            return None

        try:
            response = requests.get(
                "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
                headers={"Authorization": f"Bearer {access_token}"},
                params={"token": wiki_token},
                timeout=30,
            )
            response.raise_for_status()
            result = response.json()
            if result.get('code') != 0:
                print(f"解析在途库存 wiki 链接失败: {result.get('msg')}")
                return None

            node = result.get('data', {}).get('node', {})
            if node.get('obj_type') != 'bitable':
                print(f"在途库存 wiki 节点不是 bitable: {node.get('obj_type')}")
                return None

            return node.get('obj_token')
        except Exception as e:
            print(f"解析在途库存 wiki 链接异常: {e}")
            return None

    def _resolve_in_transit_bitable_target(self) -> tuple:
        """优先从 URL 解析在途库存表；失败时回退到老配置字段。"""
        feishu_config = self.config.get('feishu', {})
        nested_config = feishu_config.get('in_transit_bitable', {})
        if not isinstance(nested_config, dict):
            nested_config = {}

        configured_url = (
            nested_config.get('url') or
            feishu_config.get('in_transit_bitable_url', '')
        )
        fallback_app_token = (
            nested_config.get('app_token') or
            feishu_config.get('in_transit_bitable_app_token', '')
        )
        fallback_table_id = (
            nested_config.get('table_id') or
            feishu_config.get('in_transit_bitable_table_id', '')
        )

        if configured_url:
            parsed = self._parse_feishu_bitable_url(configured_url)
            if parsed and parsed.get('table_id'):
                app_token = parsed.get('app_token')
                if parsed.get('is_wiki'):
                    resolved_token = self._resolve_wiki_bitable_app_token(app_token)
                    if resolved_token:
                        app_token = resolved_token

                if app_token:
                    return app_token, parsed['table_id']

                print(f"在途库存链接已解析到 table_id，但无法获取底层 app_token: {configured_url}")
            else:
                print(f"无法解析在途库存链接: {configured_url}")

        return fallback_app_token, fallback_table_id
    
    def check_alerts(self) -> List[dict]:
        """
        检查库存预警
        
        预警条件：
        1. 有销售记录（avg_daily_sales > 0）且库存为 0
        2. 有销售记录且预计可售天数在 0-threshold 之间（包括 0）
        
        注意：排除没有销售记录的产品，避免噪音
        
        Returns:
            需要预警的 SKU 列表，按库存和预计可售天数排序
        """
        threshold = self.config.get('threshold_days', 10)
        
        # 查询所有 SKU
        all_items = self.api.query_all_skus()
        
        # 筛选需要预警的 SKU
        alerts = []
        for item in all_items:
            if 'error' in item:
                continue
            
            available = item.get('available', 0)
            days = item.get('purchase_sale_days', 0)
            avg_sales = item.get('avg_daily_sales', 0)
            
            # 预警条件：
            # 1. 有销售记录（avg_sales > 0）且库存为 0
            # 2. 有销售记录且预计可售天数在 0-threshold 之间（包括 0）
            # 排除：没有销售记录的产品（避免噪音）
            if avg_sales > 0:
                if available == 0 or (days >= 0 and days < threshold):
                    alerts.append(item)
        
        # 按预计可售天数排序（库存为0的排在最前面）
        alerts.sort(key=lambda x: (x.get('available', 0) > 0, x.get('purchase_sale_days', 0)))
        
        return alerts
    
    def format_alert_message(self, alerts: List[dict]) -> str:
        """
        格式化预警消息
        
        Args:
            alerts: 预警列表
        
        Returns:
            格式化的消息文本
        """
        if not alerts:
            return "✅ 所有 SKU 库存充足，无需预警"
        
        lines = [
            f"⚠️ 库存预警通知",
            f"",
            f"发现 {len(alerts)} 个 SKU 预计可售天数低于 {self.config.get('threshold_days', 10)} 天：",
            f""
        ]
        
        for item in alerts:
            sku = item.get('sku', '')
            available = item.get('available', 0)
            days = item.get('purchase_sale_days', 0)
            avg_sales = item.get('avg_daily_sales', 0)
            
            lines.append(f"📦 SKU: {sku}")
            lines.append(f"   可用库存: {available}")
            lines.append(f"   预计可售: {days} 天")
            lines.append(f"   日均销量: {avg_sales:.2f}")
            lines.append("")
        
        lines.append(f"⏰ 检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return "\n".join(lines)
    
    def send_feishu_notification(self, message: str, webhook_url: str) -> bool:
        """
        发送飞书通知
        
        Args:
            message: 消息内容
            webhook_url: Webhook URL
        
        Returns:
            是否发送成功
        """
        if not webhook_url:
            return False
        
        payload = {
            "msg_type": "text",
            "content": {
                "text": message
            }
        }
        
        try:
            response = requests.post(webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            result = response.json()
            return result.get('code') == 0
        except Exception as e:
            print(f"发送飞书通知失败: {e}")
            return False
    
    def get_tenant_access_token(self) -> Optional[str]:
        """
        获取飞书 tenant_access_token
        
        Returns:
            access_token 或 None
        """
        feishu_config = self.config.get('feishu', {})
        app_id = feishu_config.get('app_id', '')
        app_secret = feishu_config.get('app_secret', '')
        
        if not app_id or not app_secret:
            return None
        
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": app_id,
            "app_secret": app_secret
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            result = response.json()
            if result.get('code') == 0:
                return result.get('tenant_access_token')
            else:
                print(f"获取 access_token 失败: {result.get('msg')}")
                return None
        except Exception as e:
            print(f"获取 access_token 异常: {e}")
            return None
    
    def load_in_transit_inventory(self, sku_title_map: Optional[Dict[str, str]] = None) -> Dict[str, int]:
        """
        从飞书表格加载在途库存数据
        
        支持功能：
        - 自动从飞书多维表格读取在途库存
        - 支持同一SKU多批次累加（如FSL001有400+200=600）
        - 支持SKU编码和SKU名称的容错匹配
        
        Args:
            sku_title_map: SKU编码 -> SKU名称的映射字典，用于容错匹配
        
        Returns:
            SKU -> 在途数量的字典，如果加载失败返回空字典
        """
        app_token, table_id = self._resolve_in_transit_bitable_target()
        
        if not app_token or not table_id:
            # 未配置在途库存表格，返回空字典
            return {}
        
        try:
            # 使用 API 直接调用
            access_token = self.get_tenant_access_token()
            if not access_token:
                print("无法获取 access_token")
                return {}
            
            url = f'https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records'
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') != 0:
                print(f"读取在途库存表格失败: {data.get('msg')}")
                return {}
            
            records = data.get('data', {}).get('items', [])
            
            # 第一轮：通过 SKU 编码精确匹配
            in_transit_map = {}
            for record in records:
                fields = record.get('fields', {})
                
                # 尝试多种字段名获取 SKU 编码
                sku = (fields.get('SKU编码', '') or
                       fields.get('sku编码', '') or
                       fields.get('sku_code', ''))
                
                # 尝试多种字段名获取数量
                quantity = (fields.get('在途数量', 0) or
                           fields.get('在途库存', 0) or
                           fields.get('quantity', 0) or
                           fields.get('数量', 0))
                
                # 确保 quantity 是数字类型
                try:
                    quantity = float(quantity) if quantity else 0
                except (ValueError, TypeError):
                    quantity = 0
                
                if sku and quantity > 0:
                    sku_key = sku.strip()
                    if sku_key in in_transit_map:
                        in_transit_map[sku_key] += int(quantity)
                    else:
                        in_transit_map[sku_key] = int(quantity)
            
            # 第二轮：容错匹配（如果提供了 sku_title_map）
            if sku_title_map:
                # 创建反向映射：title -> sku
                title_to_sku = {title.lower().strip(): sku for sku, title in sku_title_map.items()}
                
                # 同时创建 SKU 的模糊匹配映射（用于处理大小写、空格等差异）
                sku_fuzzy_map = {sku.lower().strip().replace('-', '').replace('_', ''): sku 
                                for sku in sku_title_map.keys()}
                
                for record in records:
                    fields = record.get('fields', {})
                    
                    # 获取在途数量
                    quantity = (fields.get('在途数量', 0) or
                               fields.get('在途库存', 0) or
                               fields.get('quantity', 0) or
                               fields.get('数量', 0))
                    
                    # 确保 quantity 是数字类型
                    try:
                        quantity = float(quantity) if quantity else 0
                    except (ValueError, TypeError):
                        quantity = 0
                    
                    if quantity <= 0:
                        continue
                    
                    matched_sku = None
                    
                    # 方法1：通过专门的 SKU 名称字段匹配
                    sku_name = (fields.get('SKU名称', '') or
                               fields.get('sku name', '') or
                               fields.get('sku_name', '') or
                               fields.get('产品名称', '') or
                               fields.get('Name', ''))
                    
                    if sku_name:
                        sku_name_lower = sku_name.lower().strip()
                        if sku_name_lower in title_to_sku:
                            matched_sku = title_to_sku[sku_name_lower]
                            # 累加在途库存（支持同一SKU多批次）
                            if matched_sku in in_transit_map:
                                in_transit_map[matched_sku] += int(quantity)
                                print(f"  ✓ 通过 SKU 名称字段 '{sku_name}' 匹配到 '{matched_sku}'，累加后: {in_transit_map[matched_sku]}")
                            else:
                                in_transit_map[matched_sku] = int(quantity)
                                print(f"  ✓ 通过 SKU 名称字段 '{sku_name}' 匹配到 '{matched_sku}'")
                            continue
                    
                    # 方法2：尝试将 "SKU" 或 "sku" 字段当作 title 来匹配
                    # 这是为了处理在途表格中 "SKU" 字段实际存的是产品名称的情况
                    potential_title = (fields.get('SKU', '') or
                                      fields.get('sku', ''))
                    
                    if potential_title and not matched_sku:
                        potential_title_lower = potential_title.lower().strip()
                        # 先尝试作为 title 精确匹配
                        if potential_title_lower in title_to_sku:
                            matched_sku = title_to_sku[potential_title_lower]
                            # 累加在途库存（支持同一SKU多批次）
                            if matched_sku in in_transit_map:
                                in_transit_map[matched_sku] += int(quantity)
                                print(f"  ✓ 通过 SKU 字段作为名称 '{potential_title}' 匹配到 '{matched_sku}'，累加后: {in_transit_map[matched_sku]}")
                            else:
                                in_transit_map[matched_sku] = int(quantity)
                                print(f"  ✓ 通过 SKU 字段作为名称 '{potential_title}' 匹配到 '{matched_sku}'")
                            continue
                        
                        # 如果精确匹配失败，尝试模糊匹配（当作 SKU 编码）
                        potential_sku_fuzzy = potential_title.lower().strip().replace('-', '').replace('_', '')
                        if potential_sku_fuzzy in sku_fuzzy_map:
                            matched_sku = sku_fuzzy_map[potential_sku_fuzzy]
                            # 累加在途库存（支持同一SKU多批次）
                            if matched_sku in in_transit_map:
                                in_transit_map[matched_sku] += int(quantity)
                                print(f"  ✓ 通过模糊匹配 '{potential_title}' 匹配到 '{matched_sku}'，累加后: {in_transit_map[matched_sku]}")
                            else:
                                in_transit_map[matched_sku] = int(quantity)
                                print(f"  ✓ 通过模糊匹配 '{potential_title}' 匹配到 '{matched_sku}'")
            
            print(f"✓ 已加载 {len(in_transit_map)} 个 SKU 的在途库存数据")
            return in_transit_map
            
        except Exception as e:
            print(f"加载在途库存数据失败: {e}")
            return {}
    
    def create_feishu_doc(self, alerts: List[dict]) -> Optional[str]:
        """
        将补货建议导出到飞书多维表格。

        这里直接复用主补货链路，避免在 inventory-alert 中维护第二套补货公式。
        
        Args:
            alerts: 预警列表（用于兼容性，实际不使用）
        
        Returns:
            表格 URL 或 None
        """
        feishu_config = self.config.get('feishu', {})
        app_token = feishu_config.get('bitable_app_token', '')
        table_id = feishu_config.get('bitable_table_id', '')
        
        if not app_token or not table_id:
            print("无法创建飞书表格：未配置 bitable_app_token 和 bitable_table_id")
            print("请在配置文件中添加：")
            print('  "bitable_app_token": "你的多维表格 app_token"')
            print('  "bitable_table_id": "你的数据表 table_id"')
            return None

        try:
            from export_restock_to_bitable import export_restock_to_bitable
        except ImportError as e:
            print(f"无法导入补货导出模块: {e}")
            return None

        result = export_restock_to_bitable(
            app_token=app_token,
            table_id=table_id,
            threshold_days=self.config.get('threshold_days', 10),
            profile=self.profile,
        )

        if result.get("success"):
            print(
                f"✓ 已更新飞书多维表格: {result.get('table_name', table_id)} "
                f"({result.get('record_count', 0)} 条记录)"
            )
            return result.get("table_url") or result.get("table_name")

        print(f"✗ 更新飞书多维表格失败: {result.get('error')}")
        return None
    

    def _save_spreadsheet_token(self, spreadsheet_token: str):
        """
        保存 spreadsheet_token 到配置文件
        
        Args:
            spreadsheet_token: 表格 token
        """
        try:
            config_path = Path(__file__).parent / "config" / "alert_config.json"
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                
                if 'feishu' not in config:
                    config['feishu'] = {}
                
                config['feishu']['spreadsheet_token'] = spreadsheet_token
                
                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=2, ensure_ascii=False)
                
                print(f"✓ 已保存表格 token 到配置文件")
        except Exception as e:
            print(f"保存表格 token 失败: {e}")
    
    def _write_sheet_data(self, access_token: str, spreadsheet_token: str, sheet_id: str, alerts: List[dict]):
        """
        写入表格数据
        
        Args:
            access_token: 访问令牌
            spreadsheet_token: 表格 token
            sheet_id: 工作表 ID
            alerts: 预警列表
        """
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_batch_update"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # 构建数据
        values = []
        
        # 标题行
        values.append([
            f"库存预警报告 - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        ])
        
        # 基本信息
        values.append([
            f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  |  预警数量: {len(alerts)} 个 SKU  |  预警阈值: {self.config.get('threshold_days', 10)} 天"
        ])
        
        # 空行
        values.append([])
        
        # 表头
        values.append([
            "SKU编码",
            "SKU名称",
            "SKU图片",
            "当前可用库存",
            "日均销量",
            "预计可售天数",
            "建议采购数量"
        ])
        
        # 数据行
        for item in alerts:
            sku = item.get('sku', '')
            title = item.get('title', '')
            image = item.get('image', '')
            available = item.get('available', 0)
            avg_sales = item.get('avg_daily_sales', 0)
            days = item.get('purchase_sale_days', 0)
            suggested_purchase = int(avg_sales * 15)
            
            values.append([
                sku,
                title,
                image if image else "-",
                available,
                round(avg_sales, 2),
                days,
                suggested_purchase
            ])
        
        # 构建请求
        payload = {
            "valueRange": {
                "range": f"{sheet_id}!A1:G{len(values)}",
                "values": values
            }
        }
        
        try:
            response = requests.put(url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            result = response.json()
            
            if result.get('code') != 0:
                print(f"写入表格数据失败: {result.get('msg')}")
            else:
                # 设置表头样式（加粗）
                self._format_sheet_header(access_token, spreadsheet_token, sheet_id)
                
        except Exception as e:
            print(f"写入表格数据失败: {e}")
            import traceback
            traceback.print_exc()
    
    def _format_sheet_header(self, access_token: str, spreadsheet_token: str, sheet_id: str):
        """
        格式化表头（加粗、背景色）
        
        Args:
            access_token: 访问令牌
            spreadsheet_token: 表格 token
            sheet_id: 工作表 ID
        """
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/style_batch_update"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "data": [
                {
                    "range": f"{sheet_id}!A1:A1",
                    "style": {
                        "font": {
                            "bold": True
                        },
                        "fontSize": 14
                    }
                },
                {
                    "range": f"{sheet_id}!A4:G4",
                    "style": {
                        "font": {
                            "bold": True
                        },
                        "backColor": {
                            "red": 0.9,
                            "green": 0.9,
                            "blue": 0.9
                        }
                    }
                }
            ]
        }
        
        try:
            response = requests.put(url, headers=headers, json=payload, timeout=10)
            response.raise_for_status()
        except Exception as e:
            print(f"格式化表头失败: {e}")
    
    def run(self, send_notification: bool = True) -> dict:
        """
        运行库存预警检查
        
        Args:
            send_notification: 是否发送通知（默认 True）
        
        Returns:
            检查结果
        """
        print("开始检查库存预警...")
        
        # 检查预警
        alerts = self.check_alerts()
        
        # 格式化消息
        message = self.format_alert_message(alerts)
        print(message)
        
        # 发送通知
        notifications_sent = []
        if send_notification and alerts:
            feishu_config = self.config.get('feishu', )
            
            if feishu_config.get('enabled', False):
                # 发送给用户
                user_webhook = feishu_config.get('user_webhook_url', '')
                if user_webhook:
                    if self.send_feishu_notification(message, user_webhook):
                        notifications_sent.append('user')
                        print("✓ 已发送通知给用户")
                
                # 发送给群组
                group_webhook = feishu_config.get('group_webhook_url', '')
                if group_webhook:
                    if self.send_feishu_notification(message, group_webhook):
                        notifications_sent.append('group')
                        print("✓ 已发送通知到群组")
                
                # 创建飞书文档
                doc_url = None
                if feishu_config.get('create_doc', False):
                    doc_url = self.create_feishu_doc(alerts)
                    if doc_url:
                        notifications_sent.append('doc')
        
        return {
            "timestamp": datetime.now().isoformat(),
            "alert_count": len(alerts),
            "alerts": alerts,
            "message": message,
            "notifications_sent": notifications_sent,
            "doc_url": doc_url if 'doc_url' in locals() else None
        }


# OpenClaw Skill 接口
def check_inventory_alerts(send_notification: bool = True, profile: Optional[str] = None) -> dict:
    """
    检查库存预警
    
    Args:
        send_notification: 是否发送飞书通知（默认 True）
        profile: 指定账号 profile；不传时使用当前激活账号
    
    Returns:
        检查结果，包含预警列表和通知状态
    
    Example:
        >>> # 检查并发送通知
        >>> result = check_inventory_alerts()
        >>> print(f"发现 {result['alert_count']} 个预警")
        
        >>> # 仅检查不发送通知
        >>> result = check_inventory_alerts(send_notification=False)
    """
    alert = InventoryAlert(profile=profile)
    return alert.run(send_notification=send_notification)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='库存预警检查')
    parser.add_argument('--no-notify', action='store_true', help='不发送通知')
    parser.add_argument('--profile', help='指定账号 profile')
    args = parser.parse_args()
    
    result = check_inventory_alerts(
        send_notification=not args.no_notify,
        profile=args.profile
    )
    
    print(f"\n检查完成，发现 {result['alert_count']} 个预警")
