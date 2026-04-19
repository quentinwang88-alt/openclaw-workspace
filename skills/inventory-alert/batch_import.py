#!/usr/bin/env python3
"""批量添加库存预警数据到飞书多维表格"""
import requests
import json
import time

# 配置
APP_TOKEN = "ItCzbDvR3aIXuss5UeUcko1SnUg"
TABLE_ID = "tblNIGOGvuDLGSit"
APP_ID = "cli_a920876864b9dcbd"
APP_SECRET = "w3Ln9WJ2c2jLfDkwhkbtFjUyo73KeGju"

# 获取 access_token
def get_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {
        "app_id": APP_ID,
        "app_secret": APP_SECRET
    }
    response = requests.post(url, json=payload)
    result = response.json()
    if result.get('code') == 0:
        return result['tenant_access_token']
    return None

# 添加记录
def add_record(access_token, fields):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {"fields": fields}
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        result = response.json()
        if result.get('code') == 0:
            return True
        else:
            print(f"  错误: {result.get('msg')}")
            return False
    except Exception as e:
        print(f"  异常: {e}")
        return False

# 所有预警数据
alerts_data = [
    # 紧急缺货 (0天)
    {"sku": "MWJ-PG", "available": 0, "avg_sales": 2.97, "days": 0, "suggested": 45},
    {"sku": "MWJ-BW", "available": 0, "avg_sales": 0.63, "days": 0, "suggested": 9},
    {"sku": "MLJZ001", "available": 0, "avg_sales": 1.37, "days": 0, "suggested": 21},
    {"sku": "w-009", "available": 0, "avg_sales": 0.86, "days": 0, "suggested": 13},
    {"sku": "XL001", "available": 0, "avg_sales": 0.47, "days": 0, "suggested": 7},
    {"sku": "sl0010", "available": 0, "avg_sales": 0.12, "days": 0, "suggested": 2},
    {"sku": "FL-KQ-L", "available": 0, "avg_sales": 0.01, "days": 0, "suggested": 1},
    {"sku": "FL-CA-S", "available": 0, "avg_sales": 0.03, "days": 0, "suggested": 1},
    {"sku": "FL-CA-M", "available": 0, "avg_sales": 0.01, "days": 0, "suggested": 1},
    {"sku": "LFM-BK", "available": 0, "avg_sales": 0.01, "days": 0, "suggested": 1},
    {"sku": "LFM-WT", "available": 0, "avg_sales": 0.15, "days": 0, "suggested": 2},
    {"sku": "LFG-MS", "available": 0, "avg_sales": 0.21, "days": 0, "suggested": 3},
    {"sku": "FL-KQ-2XL", "available": 0, "avg_sales": 0.01, "days": 0, "suggested": 1},
    {"sku": "bwj0011", "available": 1, "avg_sales": 3.84, "days": 0, "suggested": 58},
    {"sku": "JZ-G006", "available": 1, "avg_sales": 1.54, "days": 0, "suggested": 23},
    {"sku": "BWJ002", "available": 2, "avg_sales": 2.50, "days": 0, "suggested": 38},
    
    # 即将缺货 (1-3天)
    {"sku": "JZ-G009", "available": 1, "avg_sales": 0.66, "days": 1, "suggested": 10},
    {"sku": "Nbwj000", "available": 4, "avg_sales": 4.33, "days": 1, "suggested": 65},
    {"sku": "bgwj003", "available": 9, "avg_sales": 3.66, "days": 2, "suggested": 55},
    {"sku": "RWJ005", "available": 12, "avg_sales": 3.45, "days": 3, "suggested": 52},
    {"sku": "PU009", "available": 9, "avg_sales": 2.86, "days": 3, "suggested": 43},
    {"sku": "SL004", "available": 2, "avg_sales": 0.62, "days": 3, "suggested": 9},
    
    # 库存预警 (4-10天)
    {"sku": "sl009", "available": 4, "avg_sales": 0.93, "days": 4, "suggested": 14},
    {"sku": "sl008", "available": 1, "avg_sales": 0.24, "days": 4, "suggested": 4},
    {"sku": "xl002", "available": 16, "avg_sales": 3.79, "days": 4, "suggested": 57},
    {"sku": "Three-piece Set-White-Gold", "available": 2, "avg_sales": 0.40, "days": 5, "suggested": 6},
    {"sku": "W-008", "available": 21, "avg_sales": 3.91, "days": 5, "suggested": 59},
    {"sku": "PU005", "available": 22, "avg_sales": 3.50, "days": 6, "suggested": 53},
    {"sku": "PU0010", "available": 13, "avg_sales": 2.13, "days": 6, "suggested": 32},
    {"sku": "PU0012", "available": 9, "avg_sales": 1.21, "days": 7, "suggested": 18},
    {"sku": "PU007", "available": 56, "avg_sales": 6.84, "days": 8, "suggested": 103},
    {"sku": "pu003", "available": 72, "avg_sales": 7.83, "days": 9, "suggested": 117},
    {"sku": "PU0011", "available": 10, "avg_sales": 1.07, "days": 9, "suggested": 16},
]

def main():
    print("获取 access_token...")
    access_token = get_access_token()
    if not access_token:
        print("❌ 无法获取 access_token")
        return
    
    print(f"✓ 获取成功，开始添加 {len(alerts_data)} 条记录...\n")
    
    success_count = 0
    for item in alerts_data:
        # 确定紧急程度
        if item["days"] == 0:
            urgency = "🚨 紧急缺货"
        elif item["days"] <= 3:
            urgency = "⚠️ 即将缺货"
        else:
            urgency = "⏰ 库存预警"
        
        fields = {
            "SKU编码": item["sku"],
            "当前库存": item["available"],
            "日均销量": item["avg_sales"],
            "预计可售天数": item["days"],
            "建议采购数量": item["suggested"],
            "紧急程度": urgency
        }
        
        print(f"添加: {item['sku']} (建议采购 {item['suggested']} 件)...", end=" ")
        if add_record(access_token, fields):
            print("✓")
            success_count += 1
        else:
            print("✗")
        
        time.sleep(0.2)  # 避免频率限制
    
    print(f"\n完成！成功添加 {success_count}/{len(alerts_data)} 条记录")
    print(f"查看表格: https://gcngopvfvo0q.feishu.cn/base/{APP_TOKEN}")

if __name__ == "__main__":
    main()
