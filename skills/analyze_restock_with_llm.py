#!/usr/bin/env python3
"""
使用 LLM 分析补货建议数据并生成飞书文档报告
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


def _priority_days(item: dict) -> float:
    """优先使用新的主排序天数字段。"""
    return item.get('days_for_priority', item.get('days_with_transit', item.get('purchase_sale_days', 0)))


def analyze_with_llm(restock_data: Dict[str, Any]) -> str:
    """
    调用 LLM 分析补货建议数据
    
    使用 OpenClaw 的 sessions_spawn 或直接调用模型 API
    这里使用简单的分析逻辑生成报告
    """
    restock_list = restock_data.get('restock_list', [])
    
    if not restock_list:
        return "暂无需要补货的 SKU。"
    
    # 按优先级分组
    urgent = [x for x in restock_list if _priority_days(x) <= 3]
    high = [x for x in restock_list if 3 < _priority_days(x) <= 7]
    medium = [x for x in restock_list if 7 < _priority_days(x) <= 10]
    
    # 计算统计数据
    total_suggested_qty = sum(x['suggested_qty'] for x in restock_list)
    total_in_transit = sum(x.get('in_transit', 0) for x in restock_list)
    avg_daily_sales_total = sum(x['avg_daily_sales'] for x in restock_list)
    
    # 找出销量最高的 SKU
    top_sales = sorted(restock_list, key=lambda x: x['avg_daily_sales'], reverse=True)[:5]
    
    # 找出库存最紧急的 SKU
    most_urgent = sorted(restock_list, key=_priority_days)[:5]
    
    # 生成分析报告
    analysis = f"""## 📊 数据洞察

### 整体概况
- **需补货 SKU 总数**: {len(restock_list)} 个
- **建议采购总量**: {total_suggested_qty:,} 件
- **在途库存总量**: {total_in_transit:,} 件
- **日均销量合计**: {avg_daily_sales_total:.2f} 件/天

### 优先级分布
"""
    
    if urgent:
        urgent_qty = sum(x['suggested_qty'] for x in urgent)
        analysis += f"- 🔴 **极高优先级**: {len(urgent)} 个 SKU，建议采购 {urgent_qty:,} 件\n"
    
    if high:
        high_qty = sum(x['suggested_qty'] for x in high)
        analysis += f"- 🟠 **高优先级**: {len(high)} 个 SKU，建议采购 {high_qty:,} 件\n"
    
    if medium:
        medium_qty = sum(x['suggested_qty'] for x in medium)
        analysis += f"- 🟡 **中优先级**: {len(medium)} 个 SKU，建议采购 {medium_qty:,} 件\n"
    
    analysis += "\n### 🔥 销量 TOP 5 SKU\n\n"
    for i, item in enumerate(top_sales, 1):
        analysis += f"{i}. **{item['sku']}** - 日均销量 {item['avg_daily_sales']:.2f} 件，当前库存 {item['available']} 件\n"
    
    analysis += "\n### 🚨 最紧急补货 SKU\n\n"
    for i, item in enumerate(most_urgent, 1):
        status = "⚡ 有在途" if item.get('in_transit', 0) > 0 else ""
        analysis += (
            f"{i}. **{item['sku']}** - 实际可售 {item.get('days_with_transit', item['purchase_sale_days']):.1f} 天，"
            f"现货可售 {item['purchase_sale_days']:.1f} 天，建议采购 {item['suggested_qty']} 件 {status}\n"
        )
    
    # 生成建议
    analysis += "\n## 💡 运营建议\n\n"
    
    if urgent:
        analysis += "### 🚨 紧急行动项\n"
        analysis += "- 以下 SKU 库存极度紧张，建议 **立即安排补货**：\n"
        for item in urgent[:3]:
            analysis += (
                f"  - {item['sku']}: 实际可售 {item.get('days_with_transit', item['purchase_sale_days']):.1f} 天，"
                f"建议采购 {item['suggested_qty']} 件\n"
            )
        analysis += "\n"
    
    if total_in_transit > 0:
        analysis += f"### 🚚 在途库存提醒\n"
        analysis += f"- 当前有 {total_in_transit:,} 件商品在途运输中\n"
        analysis += "- 建议在途商品到货后优先分配给库存紧张的 SKU\n\n"
    
    # 计算平均可售天数
    avg_days = sum(_priority_days(x) for x in restock_list) / len(restock_list)
    if avg_days < 7:
        analysis += "### ⚠️ 整体库存健康度\n"
        analysis += f"- 平均可售天数仅 {avg_days:.1f} 天，整体库存偏低\n"
        analysis += "- 建议适当提高安全库存水平，避免断货风险\n\n"
    
    analysis += "### 📈 补货策略建议\n"
    analysis += "1. **优先处理极高优先级 SKU**，确保不断货\n"
    analysis += "2. **关注高销量 SKU**，这些 SKU 断货影响最大\n"
    analysis += "3. **合理规划在途库存**，避免过度补货造成积压\n"
    analysis += "4. **建议每周执行一次**补货建议检查，保持库存健康\n"
    
    return analysis


def create_feishu_doc(content: str, title: str = None) -> Dict[str, Any]:
    """
    创建飞书文档
    
    使用 feishu_doc 工具创建文档
    """
    if title is None:
        today = datetime.now().strftime("%Y-%m-%d")
        title = f"补货建议分析报告 - {today}"
    
    # 文档内容
    doc_content = f"""# 📦 补货建议分析报告

**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}

---

{content}

---

*报告由 AI 自动生成，数据来源于 BigSeller 库存系统*
"""
    
    return {
        "success": True,
        "title": title,
        "content": doc_content
    }


def main():
    """主函数"""
    print("=" * 60)
    print("📦 补货建议 LLM 分析报告生成")
    print("=" * 60)
    
    # 1. 生成补货建议数据
    print("\n1️⃣ 生成补货建议数据...")
    result = generate_restock_report(
        purchase_cycle_days=17,
        threshold_days=10,
        output_format="json"
    )
    
    if not result.get("success"):
        print(f"❌ 生成失败: {result.get('error')}")
        return
    
    # 过滤掉日均销量为0的SKU
    restock_list = [
        item for item in result.get("restock_list", [])
        if item.get('avg_daily_sales', 0) > 0
    ]
    
    print(f"   ✓ 共 {len(restock_list)} 个 SKU 需要补货")
    
    # 2. LLM 分析
    print("\n2️⃣ 执行数据分析和洞察...")
    analysis = analyze_with_llm({"restock_list": restock_list})
    print("   ✓ 分析完成")
    
    # 3. 生成完整报告
    print("\n3️⃣ 生成完整报告...")
    
    # 生成 Markdown 格式报告
    from restock_skill_adapter import format_markdown_report
    markdown_report = format_markdown_report({
        "restock_list": restock_list,
        "purchase_cycle_days": 17,
        "threshold_days": 10
    })
    
    # 合并分析和原始报告
    full_report = f"""{analysis}

---

{markdown_report}
"""
    
    # 4. 保存到文件
    today = datetime.now().strftime("%Y-%m-%d")
    output_path = Path(__file__).parent / f"restock_analysis_{today}.md"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(full_report)
    
    print(f"   ✓ 报告已保存: {output_path}")
    
    # 5. 输出摘要
    print("\n" + "=" * 60)
    print("📊 分析摘要")
    print("=" * 60)
    
    urgent = [x for x in restock_list if _priority_days(x) <= 3]
    high = [x for x in restock_list if 3 < _priority_days(x) <= 7]
    medium = [x for x in restock_list if 7 < _priority_days(x) <= 10]
    
    print(f"\n🔴 极高优先级: {len(urgent)} 个 SKU")
    print(f"🟠 高优先级: {len(high)} 个 SKU")
    print(f"🟡 中优先级: {len(medium)} 个 SKU")
    print(f"📦 总计需补货: {len(restock_list)} 个 SKU")
    
    total_qty = sum(x['suggested_qty'] for x in restock_list)
    print(f"📊 建议采购总量: {total_qty:,} 件")
    
    print(f"\n✅ 报告生成完成！")
    print(f"📄 文件路径: {output_path}")
    
    return full_report


if __name__ == "__main__":
    main()
