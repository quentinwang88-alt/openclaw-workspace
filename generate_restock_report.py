#!/usr/bin/env python3
"""生成补货建议报告"""

import json
import sys
from datetime import datetime
from pathlib import Path

# 添加 inventory-alert 到路径
sys.path.insert(0, str(Path(__file__).parent / "skills" / "inventory-alert"))

from alert import InventoryAlertAPI, InventoryAlert

def generate_report():
    """生成补货建议报告"""
    
    # 初始化
    api = InventoryAlertAPI()
    alert_system = InventoryAlert()
    
    print("正在查询所有 SKU 库存...")
    all_skus = api.query_all_skus()
    print(f"共查询到 {len(all_skus)} 个 SKU")
    
    # 加载在途库存
    sku_title_map = {item['sku']: item.get('title', item['sku']) for item in all_skus}
    in_transit = alert_system.load_in_transit_inventory(sku_title_map)
    print(f"已加载 {len(in_transit)} 个 SKU 的在途库存")
    
    # 计算补货建议
    PURCHASE_CYCLE_DAYS = 17  # 采购3天 + 物流12天 + 安全2天
    THRESHOLD_DAYS = 10
    
    restock_list = []
    
    for sku_data in all_skus:
        sku = sku_data['sku']
        available = sku_data.get('available', 0)
        avg_sales = sku_data.get('avg_daily_sales', 0)
        purchase_sale_days = sku_data.get('purchase_sale_days', 0)
        
        # 获取在途库存
        in_transit_qty = in_transit.get(sku, 0)
        
        # 计算含在途的可售天数
        total_available = available + in_transit_qty
        days_with_transit = total_available / avg_sales if avg_sales > 0 else 999
        
        # 只处理需要补货的SKU
        if purchase_sale_days <= THRESHOLD_DAYS:
            # 计算建议采购量
            target_qty = avg_sales * PURCHASE_CYCLE_DAYS
            suggested_qty = max(0, target_qty - total_available)
            
            restock_list.append({
                'sku': sku,
                'available': available,
                'in_transit': in_transit_qty,
                'avg_daily_sales': avg_sales,
                'purchase_sale_days': purchase_sale_days,
                'days_with_transit': days_with_transit,
                'suggested_qty': int(suggested_qty),
                'priority': get_priority(purchase_sale_days)
            })
    
    # 按可售天数排序
    restock_list.sort(key=lambda x: x['purchase_sale_days'])
    
    # 生成 Markdown 报告
    report = generate_markdown(restock_list, PURCHASE_CYCLE_DAYS, THRESHOLD_DAYS)
    
    # 保存报告
    output_dir = Path(__file__).parent / "skills" / "output"
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / f"补货建议_{datetime.now().strftime('%Y-%m-%d')}.md"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n✅ 报告已生成: {output_file}")
    return output_file

def get_priority(days):
    """获取优先级"""
    if days <= 3:
        return ('🔴 极高', 1)
    elif days <= 7:
        return ('🟠 高', 2)
    elif days <= 10:
        return ('🟡 中', 3)
    else:
        return ('🟢 低', 4)

def generate_markdown(restock_list, cycle_days, threshold_days):
    """生成 Markdown 报告"""
    
    now = datetime.now()
    
    md = f"""# 📦 补货建议报告

**生成时间**: {now.strftime('%Y-%m-%d %H:%M')}  
**补货周期**: {cycle_days}天（采购3天 + 物流12天 + 安全2天）  
**预警阈值**: {threshold_days}天可售

---

"""
    
    # 按优先级分组
    urgent = [x for x in restock_list if x['purchase_sale_days'] <= 3]
    high = [x for x in restock_list if 4 <= x['purchase_sale_days'] <= 7]
    medium = [x for x in restock_list if 8 <= x['purchase_sale_days'] <= 10]
    
    # 紧急补货
    if urgent:
        md += "## 🚨 紧急补货（0-3天可售）\n\n"
        for i, item in enumerate(urgent, 1):
            md += format_sku_item(i, item)
        md += "\n---\n\n"
    
    # 高优先级
    if high:
        md += "## ⚠️ 高优先级补货（4-7天可售）\n\n"
        for i, item in enumerate(high, len(urgent) + 1):
            md += format_sku_item(i, item)
        md += "\n---\n\n"
    
    # 中优先级
    if medium:
        md += "## 📋 中优先级补货（8-10天可售）\n\n"
        for i, item in enumerate(medium, len(urgent) + len(high) + 1):
            md += format_sku_item(i, item)
        md += "\n---\n\n"
    
    # 汇总
    md += "## 📊 补货汇总\n\n"
    md += "| 优先级 | SKU数量 | 建议采购总量 |\n"
    md += "|--------|---------|-------------|\n"
    
    if urgent:
        total_urgent = sum(x['suggested_qty'] for x in urgent)
        md += f"| 🔴 极高（0-3天） | {len(urgent)} | 约 {total_urgent:,} 件 |\n"
    
    if high:
        total_high = sum(x['suggested_qty'] for x in high)
        md += f"| 🟠 高（4-7天） | {len(high)} | 约 {total_high:,} 件 |\n"
    
    if medium:
        total_medium = sum(x['suggested_qty'] for x in medium)
        md += f"| 🟡 中（8-10天） | {len(medium)} | 约 {total_medium:,} 件 |\n"
    
    total_qty = sum(x['suggested_qty'] for x in restock_list)
    md += f"| **合计** | **{len(restock_list)}** | **约 {total_qty:,} 件** |\n\n"
    
    md += "---\n\n"
    md += f"**报告生成**: {now.strftime('%Y-%m-%d %H:%M')}  \n"
    md += "**数据来源**: BigSeller库存系统 + 飞书在途库存表\n"
    
    return md

def format_sku_item(index, item):
    """格式化单个SKU条目"""
    
    md = f"### {index}. {item['sku']}"
    
    # 标记有在途库存的SKU
    if item['in_transit'] > 0:
        md += " ⚡ 有在途库存"
    
    md += "\n"
    md += f"- **当前库存**: {item['available']} 件\n"
    md += f"- **在途库存**: {item['in_transit']} 件"
    
    if item['in_transit'] > 0:
        md += " （🚚 运输中）"
    
    md += "\n"
    md += f"- **日均销量**: {item['avg_daily_sales']:.2f} 件/天\n"
    md += f"- **预计可售**: {item['purchase_sale_days']} 天"
    
    if item['in_transit'] > 0:
        md += f"（含在途：{int(item['days_with_transit'])} 天）"
    
    md += "\n"
    md += f"- **建议采购**: **{item['suggested_qty']} 件**\n"
    md += f"- **优先级**: {item['priority'][0]}\n"
    
    if item['in_transit'] > 0:
        md += f"- **备注**: 在途{item['in_transit']}件预计到货后可售{int(item['days_with_transit'])}天"
        if item['suggested_qty'] > 10:
            md += "，仍需补货"
        md += "\n"
    
    md += "\n"
    
    return md

if __name__ == "__main__":
    generate_report()
