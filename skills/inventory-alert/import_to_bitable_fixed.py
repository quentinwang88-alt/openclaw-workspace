#!/usr/bin/env python3
"""批量导入库存预警数据到飞书多维表格（使用直接 API 调用）"""

import json
from pathlib import Path
from feishu_api import FeishuBitableAPI

# 加载配置
config_path = Path(__file__).parent / "config" / "alert_config.json"
with open(config_path) as f:
    config = json.load(f)

feishu_config = config.get("feishu", )
app_id = feishu_config.get("app_id")
app_secret = feishu_config.get("app_secret")

if not app_id or not app_secret:
    print("错误: 请在配置文件中设置 feishu.app_id 和 feishu.app_secret")
    exit(1)

# 创建 API 客户端
api = FeishuBitableAPI(app_id, app_secret)

# 多维表格配置
app_token = "ItCzbDvR3aIXuss5UeUcko1SnUg"
table_id = "tblNIGOGvuDLGSit"

# 预警数据（从刚才的输出整理）
alerts_data = [
    {"sku": "WWJ001", "available": 0, "avg_sales": 17.33, "days": 0, "suggested": 260},
    {"sku": "grwj029", "available": 0, "avg_sales": 20.60, "days": 0, "suggested": 309},
    {"sku": "FSL001", "available": 0, "avg_sales": 35.14, "days": 0, "suggested": 527},
    {"sku": "pwj014", "available": 0, "avg_sales": 3.35, "days": 0, "suggested": 50},
    {"sku": "pbwj013", "available": 0, "avg_sales": 3.72, "days": 0, "suggested": 56},
    {"sku": "MWJ-PG", "available": 0, "avg_sales": 2.97, "days": 0, "suggested": 45},
    {"sku": "MLJZ001", "available": 0, "avg_sales": 1.37, "days": 0, "suggested": 21},
    {"sku": "w-009", "available": 0, "avg_sales": 0.86, "days": 0, "suggested": 13},
    {"sku": "BWJ002", "available": 2, "avg_sales": 2.50, "days": 0, "suggested": 38},
    {"sku": "bwj0011", "available": 1, "avg_sales": 3.84, "days": 0, "suggested": 58},
    {"sku": "JZ-G006", "available": 1, "avg_sales": 1.54, "days": 0, "suggested": 23},
    {"sku": "SL002", "available": 3, "avg_sales": 2.44, "days": 1, "suggested": 37},
    {"sku": "JZ-G009", "available": 1, "avg_sales": 0.66, "days": 1, "suggested": 10},
    {"sku": "Nbwj000", "available": 5, "avg_sales": 4.33, "days": 1, "suggested": 65},
    {"sku": "bgwj003", "available": 9, "avg_sales": 3.66, "days": 2, "suggested": 55},
    {"sku": "RWJ005", "available": 12, "avg_sales": 3.45, "days": 3, "suggested": 52},
    {"sku": "PU009", "available": 9, "avg_sales": 2.86, "days": 3, "suggested": 43},
    {"sku": "SL004", "available": 2, "avg_sales": 0.62, "days": 3, "suggested": 9},
    {"sku": "xl002", "available": 16, "avg_sales": 3.79, "days": 4, "suggested": 57},
    {"sku": "PU008", "available": 35, "avg_sales": 6.87, "days": 5, "suggested": 103},
    {"sku": "W-008", "available": 21, "avg_sales": 3.91, "days": 5, "suggested": 59},
    {"sku": "SL001", "available": 331, "avg_sales": 54.07, "days": 6, "suggested": 811},
    {"sku": "PU005", "available": 23, "avg_sales": 3.50, "days": 6, "suggested": 53},
    {"sku": "PU0010", "available": 13, "avg_sales": 2.13, "days": 6, "suggested": 32},
    {"sku": "PU0012", "available": 9, "avg_sales": 1.21, "days": 7, "suggested": 18},
    {"sku": "PU007", "available": 56, "avg_sales": 6.84, "days": 8, "suggested": 103},
    {"sku": "pu003", "available": 72, "avg_sales": 7.83, "days": 9, "suggested": 117},
    {"sku": "PU0011", "available": 10, "avg_sales": 1.07, "days": 9, "suggested": 16},
]

print(f"准备导入 {len(alerts_data)} 条记录到飞书多维表格...")
print(f"表格地址: https://gcngopvfvo0q.feishu.cn/base/{app_token}")
print()

# 批量创建记录
success_count = 0
failed_count = 0

for item in alerts_data:
    # 确定紧急程度
    if item["days"] == 0:
        urgency = "🚨 紧急缺货"
    elif item["days"] <= 3:
        urgency = "⚠️ 即将缺货"
    else:
        urgency = "⏰ 库存预警"
    
    # 建议采购数量 = 日均销量 × (15 - 预计可售天数)
    suggested = int(item["avg_sales"] * (15 - item["days"]))
    
    fields = {
        "SKU编码": item["sku"],
        "SKU名称": item["sku"],  # API 中 title 字段与 sku 相同
        "当前库存": item["available"],
        "日均销量": item["avg_sales"],
        "预计可售天数": item["days"],
        "建议采购数量": suggested,
        "紧急程度": urgency
    }
    
    try:
        result = api.create_record(app_token, table_id, fields)
        print(f"✓ 添加记录: {item['sku']}")
        success_count += 1
    except Exception as e:
        print(f"✗ 失败: {item['sku']} - {e}")
        failed_count += 1

print()
print(f"导入完成！成功: {success_count}, 失败: {failed_count}")
print(f"查看表格: https://gcngopvfvo0q.feishu.cn/base/{app_token}")
