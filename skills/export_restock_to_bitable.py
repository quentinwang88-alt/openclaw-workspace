#!/usr/bin/env python3
"""
库存补货建议导出到飞书多维表格
功能：
1. 从飞书多维表格读取最新在途库存数据
2. 调用 restock_skill_adapter 生成补货建议（自动累加在途库存）
3. 清空补货建议表格历史记录
4. 写入最新补货建议数据
5. 更新表格名称为当天日期

在途库存表格配置：
- App Token: TiykbignraDkSOshIKNcfZ9vnlg
- Table ID: tblbe4xbZQ56LS55
- URL: https://gcngopvfvo0q.feishu.cn/base/TiykbignraDkSOshIKNcfZ9vnlg

使用流程：
1. 确保在途库存表格已更新最新数据
2. 运行本脚本自动生成补货建议
3. 系统会自动读取在途数据并累加重复SKU的数量
"""

import sys
import json
import re
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Set

# 添加 skills 目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from restock_skill_adapter import generate_restock_report


LEGACY_FIELD_NAMES = {
    "Name",
    "SKU编码",
    "当前库存",
    "在途库存",
    "日均销量",
    "预计可售天数",
    "建议采购数量",
    "紧急程度",
}

RESTOCK_FIELD_SPECS = (
    {"name": "现货可售天数", "type": 2, "ui_type": "Number"},
    {"name": "含在途可售天数", "type": 2, "ui_type": "Number"},
    {"name": "现货优先级", "type": 1, "ui_type": "Text"},
    {"name": "实际优先级", "type": 1, "ui_type": "Text"},
    {"name": "风险标记", "type": 1, "ui_type": "Text"},
    {"name": "ABC分类", "type": 1, "ui_type": "Text"},
    {"name": "安全天数", "type": 2, "ui_type": "Number"},
    {"name": "补货周期", "type": 2, "ui_type": "Number"},
)


def get_feishu_token(app_id: str, app_secret: str) -> str:
    """获取飞书 access_token"""
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    response = requests.post(url, json={
        "app_id": app_id,
        "app_secret": app_secret
    })
    data = response.json()
    if data.get('code') != 0:
        raise Exception(f"获取 token 失败: {data.get('msg')}")
    return data['app_access_token']


def list_bitable_fields(access_token: str, app_token: str, table_id: str) -> List[Dict[str, Any]]:
    """读取多维表格字段定义。"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    headers = {"Authorization": f"Bearer {access_token}"}
    fields: List[Dict[str, Any]] = []
    page_token = None

    while True:
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token

        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        if data.get("code") != 0:
            raise Exception(f"获取表字段失败: {data.get('msg')}")

        fields.extend(data.get("data", {}).get("items", []))

        if not data.get("data", {}).get("has_more", False):
            break
        page_token = data.get("data", {}).get("page_token")

    return fields


def get_bitable_field_names(access_token: str, app_token: str, table_id: str) -> Set[str]:
    """读取多维表格已有字段，用于兼容新增列未建好的情况。"""
    field_names: Set[str] = set()
    for item in list_bitable_fields(access_token, app_token, table_id):
        field_name = item.get("field_name")
        if field_name:
            field_names.add(field_name)
    return field_names


def create_bitable_field(
    access_token: str,
    app_token: str,
    table_id: str,
    field_name: str,
    field_type: int,
    ui_type: str,
) -> Dict[str, Any]:
    """创建飞书多维表格字段。"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    headers = {"Authorization": f"Bearer {access_token}"}
    payload = {
        "field_name": field_name,
        "type": int(field_type),
        "ui_type": ui_type,
    }
    response = requests.post(url, headers=headers, json=payload)
    result = response.json()
    if result.get("code") != 0:
        raise Exception(f"创建字段失败: {field_name} - {result.get('msg')}")
    return result.get("data", {})


def ensure_bitable_fields(access_token: str, app_token: str, table_id: str) -> Dict[str, Any]:
    """补齐补货导出依赖的新增字段。"""
    existing_fields = get_bitable_field_names(access_token, app_token, table_id)
    created_fields: List[str] = []

    for spec in RESTOCK_FIELD_SPECS:
        field_name = str(spec["name"])
        if field_name in existing_fields:
            continue
        try:
            create_bitable_field(
                access_token=access_token,
                app_token=app_token,
                table_id=table_id,
                field_name=field_name,
                field_type=int(spec["type"]),
                ui_type=str(spec["ui_type"]),
            )
            created_fields.append(field_name)
            existing_fields.add(field_name)
            print(f"   ✓ 已创建字段: {field_name}")
        except Exception as exc:
            if "FieldNameDuplicated" in str(exc):
                existing_fields.add(field_name)
                continue
            raise

    return {
        "field_names": existing_fields,
        "created_fields": created_fields,
    }


def _get_priority_tuple(item: dict, key: str) -> tuple:
    """兼容 tuple/list 的优先级字段。"""
    value = item.get(key) or item.get("priority") or ("🟡 中", 3)
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        return (value[0], value[1])
    return ("🟡 中", 3)


def _filter_supported_fields(fields: Dict[str, Any], available_fields: Optional[Set[str]]) -> Dict[str, Any]:
    """只写入目标表已存在的字段；查不到结构时回退到旧字段集合。"""
    allowed_fields = available_fields or LEGACY_FIELD_NAMES
    return {
        key: value
        for key, value in fields.items()
        if key in allowed_fields and value is not None
    }


def _build_bitable_fields(item: dict) -> Dict[str, Any]:
    """构建补货建议表的字段。"""
    priority_map = {
        1: '🚨 紧急缺货',
        2: '⚠️ 即将缺货',
        3: '⏰ 库存预警',
        4: '✅ 库存充足'
    }

    effective_priority = _get_priority_tuple(item, "effective_priority")
    stock_priority = _get_priority_tuple(item, "stock_priority")
    effective_days = round(float(item.get('days_with_transit', item.get('purchase_sale_days', 0)) or 0), 1)
    stock_days = round(float(item.get('purchase_sale_days', 0) or 0), 1)

    return {
        "Name": item.get('title') or item['sku'],
        "SKU编码": item['sku'],
        "当前库存": item['available'],
        "在途库存": item.get('in_transit', 0),
        "日均销量": round(item['avg_daily_sales'], 2),
        "预计可售天数": effective_days,
        "现货可售天数": stock_days,
        "含在途可售天数": effective_days,
        "建议采购数量": item['suggested_qty'],
        "紧急程度": priority_map.get(effective_priority[1], '⏰ 库存预警'),
        "现货优先级": stock_priority[0],
        "实际优先级": effective_priority[0],
        "风险标记": item.get('risk_flag') or "",
        "ABC分类": item.get('sku_class') or "",
        "安全天数": item.get('safety_days_used'),
        "补货周期": item.get('lead_time_used'),
    }


def _extract_display_sort_parts(sku: str) -> tuple:
    """
    按展示规则拆分 SKU：
    1. 先按开头两个字母分组
    2. 同组内按后续数字升序
    3. 最后用完整 SKU 兜底，保证排序稳定
    """
    normalized = (sku or "").strip()
    match = re.match(r"^([A-Za-z]{2})(.*)$", normalized)
    if match:
        prefix = match.group(1).lower()
        remainder = match.group(2)
    else:
        prefix = normalized[:2].lower()
        remainder = normalized[2:]

    number_match = re.search(r"(\d+)", remainder)
    number_value = int(number_match.group(1)) if number_match else float("inf")
    return (prefix, number_value, normalized.lower())


def sort_restock_list_for_display(restock_list: List[dict]) -> List[dict]:
    """按 SKU 展示规则排序导出数据。"""
    return sorted(restock_list, key=lambda item: _extract_display_sort_parts(item.get("sku", "")))


def clear_bitable_records(access_token: str, app_token: str, table_id: str) -> bool:
    """清空多维表格所有记录"""
    print("🗑️ 正在清空历史记录...")
    
    # 先获取所有记录 ID
    list_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    all_record_ids = []
    page_token = None
    
    while True:
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        
        response = requests.get(list_url, headers=headers, params=params)
        data = response.json()
        
        if data.get('code') != 0:
            print(f"⚠️ 获取记录列表失败: {data.get('msg')}")
            return False
        
        records = data.get('data', {}).get('items', [])
        for record in records:
            all_record_ids.append(record['record_id'])
        
        if not data.get('data', {}).get('has_more', False):
            break
        page_token = data.get('data', {}).get('page_token')
    
    if not all_record_ids:
        print("  ℹ️ 表格为空，无需清空")
        return True
    
    # 批量删除记录
    batch_delete_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_delete"
    
    # 分批删除（每次最多 500 条）
    batch_size = 500
    total_deleted = 0
    
    for i in range(0, len(all_record_ids), batch_size):
        batch = all_record_ids[i:i+batch_size]
        response = requests.post(
            batch_delete_url,
            headers=headers,
            json={"records": batch}
        )
        result = response.json()
        
        if result.get('code') == 0:
            total_deleted += len(batch)
            print(f"  ✓ 已删除 {total_deleted}/{len(all_record_ids)} 条记录")
        else:
            print(f"  ⚠️ 删除失败: {result.get('msg')}")
            return False
    
    print(f"✅ 已清空 {total_deleted} 条历史记录")
    return True


def create_bitable_records(access_token: str, app_token: str, table_id: str,
                           restock_list: List[dict], available_fields: Optional[Set[str]] = None) -> bool:
    """批量创建补货建议记录"""
    print(f"📤 正在写入 {len(restock_list)} 条新记录...")
    
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # 分批创建（每次最多 500 条）
    batch_size = 500
    total_created = 0
    
    for i in range(0, len(restock_list), batch_size):
        batch = restock_list[i:i+batch_size]
        records = []
        
        for item in batch:
            fields = _filter_supported_fields(_build_bitable_fields(item), available_fields)
            record = {"fields": fields}
            records.append(record)
        
        response = requests.post(url, headers=headers, json={"records": records})
        result = response.json()
        
        if result.get('code') == 0:
            total_created += len(batch)
            print(f"  ✓ 已写入 {total_created}/{len(restock_list)} 条记录")
        else:
            print(f"  ❌ 写入失败: {result.get('msg')}")
            return False
    
    print(f"✅ 成功写入 {total_created} 条记录")
    return True


def update_bitable_name(access_token: str, app_token: str, table_id: str, 
                        new_name: str) -> bool:
    """更新多维表格名称"""
    print(f"📝 正在更新表格名称为: {new_name}")
    
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    try:
        response = requests.patch(url, headers=headers, json={"name": new_name})
        # 有些 API 返回的不是标准 JSON，需要特殊处理
        try:
            result = response.json()
        except json.JSONDecodeError:
            # 如果返回不是 JSON，检查状态码
            if response.status_code == 200:
                print(f"✅ 表格名称已更新")
                return True
            else:
                print(f"⚠️ 更新名称失败: HTTP {response.status_code}")
                return False
        
        if result.get('code') == 0:
            print(f"✅ 表格名称已更新")
            return True
        else:
            print(f"⚠️ 更新名称失败: {result.get('msg')}")
            return False
    except Exception as e:
        print(f"⚠️ 更新名称异常: {e}")
        return False


def export_restock_to_bitable(
    app_token: str = "ItCzbDvR3aIXuss5UeUcko1SnUg",
    table_id: str = "tblNIGOGvuDLGSit",
    purchase_cycle_days: int = 17,
    threshold_days: int = 10,
    profile: Optional[str] = None,
) -> Dict[str, Any]:
    """
    导出库存补货建议到飞书多维表格
    
    流程：
    1. 生成补货建议
    2. 清空历史记录
    3. 写入新数据
    4. 更新表格名称
    """
    print("=" * 60)
    print("📦 库存补货建议导出到飞书多维表格")
    print("=" * 60)
    
    # 1. 生成补货建议
    print("\n1️⃣ 生成补货建议报告...")
    result = generate_restock_report(
        purchase_cycle_days=purchase_cycle_days,
        threshold_days=threshold_days,
        output_format="json",
        profile=profile,
    )
    
    if not result.get("success"):
        return {
            "success": False,
            "error": f"生成报告失败: {result.get('error')}"
        }
    
    # 过滤掉日均销量为0的SKU（暂时不重点关注）
    restock_list = [
        item for item in result.get("restock_list", [])
        if item.get('avg_daily_sales', 0) > 0
    ]
    restock_list = sort_restock_list_for_display(restock_list)
    filtered_count = len(result.get("restock_list", [])) - len(restock_list)
    print(f"   ✓ 共 {len(restock_list)} 个 SKU 需要补货（已过滤 {filtered_count} 个日均销量为0的SKU）")
    
    # 读取飞书配置
    config_path = Path(__file__).parent / "inventory-alert" / "config" / "alert_config.json"
    if not config_path.exists():
        return {
            "success": False,
            "error": "飞书配置文件不存在"
        }
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    feishu_config = config.get('feishu', {})
    app_id = feishu_config.get('app_id')
    app_secret = feishu_config.get('app_secret')
    
    if not app_id or not app_secret:
        return {
            "success": False,
            "error": "飞书应用凭证未配置"
        }
    
    try:
        # 2. 获取飞书 token
        print("\n2️⃣ 获取飞书访问令牌...")
        access_token = get_feishu_token(app_id, app_secret)
        print("   ✓ 获取成功")

        print("\n2️⃣.5️⃣ 读取目标表字段...")
        field_setup = ensure_bitable_fields(access_token, app_token, table_id)
        available_fields = field_setup["field_names"]
        created_fields = field_setup["created_fields"]
        print(f"   ✓ 已识别 {len(available_fields)} 个字段")
        if created_fields:
            print(f"   ✓ 本次新增字段 {len(created_fields)} 个: {', '.join(created_fields)}")
        
        # 3. 清空历史记录
        print("\n3️⃣ 清空历史记录...")
        if not clear_bitable_records(access_token, app_token, table_id):
            return {
                "success": False,
                "error": "清空历史记录失败"
            }
        
        # 4. 写入新数据
        print("\n4️⃣ 写入最新补货建议...")
        if not create_bitable_records(access_token, app_token, table_id, restock_list, available_fields):
            return {
                "success": False,
                "error": "写入新数据失败"
            }
        
        # 5. 更新表格名称
        print("\n5️⃣ 更新表格名称...")
        today = datetime.now().strftime("%Y-%m-%d")
        new_name = f"库存补货建议 - {today}"
        update_bitable_name(access_token, app_token, table_id, new_name)
        
        print("\n" + "=" * 60)
        print("✅ 导出完成！")
        print("=" * 60)
        
        return {
            "success": True,
            "message": f"成功导出 {len(restock_list)} 条记录到飞书多维表格",
            "record_count": len(restock_list),
            "table_name": new_name,
            "table_url": f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}",
            "created_fields": created_fields,
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"导出失败: {str(e)}"
        }


if __name__ == "__main__":
    # 执行导出
    result = export_restock_to_bitable()
    
    if result.get("success"):
        print(f"\n✅ {result['message']}")
        print(f"   表格名称: {result['table_name']}")
    else:
        print(f"\n❌ {result['error']}")
