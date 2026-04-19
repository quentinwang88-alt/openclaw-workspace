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
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

# 添加 skills 目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from restock_skill_adapter import generate_restock_report


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
                           restock_list: List[dict]) -> bool:
    """批量创建补货建议记录"""
    print(f"📤 正在写入 {len(restock_list)} 条新记录...")
    
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # 映射优先级
    priority_map = {
        1: '🚨 紧急缺货',  # 🔴 极高 (0-3天)
        2: '⚠️ 即将缺货',  # 🟠 高 (4-7天)
        3: '⏰ 库存预警',  # 🟡 中 (8-10天)
        4: '✅ 库存充足'   # 🟢 低 (>10天)
    }
    
    # 分批创建（每次最多 500 条）
    batch_size = 500
    total_created = 0
    
    for i in range(0, len(restock_list), batch_size):
        batch = restock_list[i:i+batch_size]
        records = []
        
        for item in batch:
            record = {
                "fields": {
                    "Name": item['sku'],
                    "SKU编码": item['sku'],
                    "当前库存": item['available'],
                    "在途库存": item.get('in_transit', 0),
                    "日均销量": round(item['avg_daily_sales'], 2),
                    "预计可售天数": item['purchase_sale_days'],
                    "建议采购数量": item['suggested_qty'],
                    "紧急程度": priority_map.get(item['priority'][1], '⏰ 库存预警')
                }
            }
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
        response = requests.put(url, headers=headers, json={"name": new_name})
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
    threshold_days: int = 10
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
        output_format="json"
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
        
        # 3. 清空历史记录
        print("\n3️⃣ 清空历史记录...")
        if not clear_bitable_records(access_token, app_token, table_id):
            return {
                "success": False,
                "error": "清空历史记录失败"
            }
        
        # 4. 写入新数据
        print("\n4️⃣ 写入最新补货建议...")
        if not create_bitable_records(access_token, app_token, table_id, restock_list):
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
            "table_name": new_name
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
