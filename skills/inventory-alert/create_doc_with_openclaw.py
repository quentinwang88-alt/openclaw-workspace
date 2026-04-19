#!/usr/bin/env python3
"""
使用 OpenClaw feishu_doc 工具创建库存预警文档
"""
import json
import subprocess
from datetime import datetime
from typing import List, Dict

def create_inventory_alert_doc(alerts: List[Dict]) -> str:
    """
    创建库存预警飞书文档
    
    Args:
        alerts: 预警列表
        
    Returns:
        文档 URL
    """
    if not alerts:
        return ""
    
    # 构建文档标题
    doc_title = f"库存预警报告 - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    
    # 构建 Markdown 内容
    content_lines = [
        f"# {doc_title}",
        "",
        f"**检查时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ",
        f"**预警数量**: {len(alerts)} 个 SKU  ",
        f"**预警阈值**: 10 天",
        "",
        "---",
        "",
        "## 📊 预警详情",
        ""
    ]
    
    # 按可售天数分组
    urgent = [a for a in alerts if a.get('purchase_sale_days', 0) == 0]
    critical = [a for a in alerts if 0 < a.get('purchase_sale_days', 0) <= 3]
    warning = [a for a in alerts if 3 < a.get('purchase_sale_days', 0) <= 10]
    
    if urgent:
        content_lines.extend([
            "### 🚨 紧急缺货（0天）",
            "",
            "| SKU编码 | 当前库存 | 日均销量 | 建议采购数量 |",
            "|---------|----------|----------|--------------|"
        ])
        for item in urgent:
            sku = item.get('sku', '')
            available = item.get('available', 0)
            avg_sales = item.get('avg_daily_sales', 0)
            suggested = int(avg_sales * 15)
            content_lines.append(f"| {sku} | {available} | {avg_sales:.2f} | **{suggested}** |")
        content_lines.append("")
    
    if critical:
        content_lines.extend([
            "### ⚠️ 即将缺货（1-3天）",
            "",
            "| SKU编码 | 当前库存 | 日均销量 | 预计可售 | 建议采购数量 |",
            "|---------|----------|----------|----------|--------------|"
        ])
        for item in critical:
            sku = item.get('sku', '')
            available = item.get('available', 0)
            avg_sales = item.get('avg_daily_sales', 0)
            days = item.get('purchase_sale_days', 0)
            suggested = int(avg_sales * 15)
            content_lines.append(f"| {sku} | {available} | {avg_sales:.2f} | {days}天 | **{suggested}** |")
        content_lines.append("")
    
    if warning:
        content_lines.extend([
            "### ⏰ 库存预警（4-10天）",
            "",
            "| SKU编码 | 当前库存 | 日均销量 | 预计可售 | 建议采购数量 |",
            "|---------|----------|----------|----------|--------------|"
        ])
        for item in warning:
            sku = item.get('sku', '')
            available = item.get('available', 0)
            avg_sales = item.get('avg_daily_sales', 0)
            days = item.get('purchase_sale_days', 0)
            suggested = int(avg_sales * 15)
            content_lines.append(f"| {sku} | {available} | {avg_sales:.2f} | {days}天 | {suggested} |")
        content_lines.append("")
    
    # 添加采购建议汇总
    content_lines.extend([
        "---",
        "",
        "## 📝 采购建议说明",
        "",
        "- **建议采购数量** = 日均销量 × 15天",
        "- 紧急缺货的 SKU 需要立即安排采购",
        "- 即将缺货的 SKU 建议在 1-2 天内完成采购",
        "- 库存预警的 SKU 可以在一周内安排采购",
        ""
    ])
    
    content = "\n".join(content_lines)
    
    # 使用 openclaw CLI 创建文档
    cmd = [
        "openclaw", "feishu-doc", "create",
        "--title", doc_title,
        "--content", content
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            # 解析输出获取 URL
            output = result.stdout
            if "https://feishu.cn/docx/" in output:
                # 提取 URL
                for line in output.split('\n'):
                    if "https://feishu.cn/docx/" in line:
                        url = line.strip().split()[-1]
                        return url
        else:
            print(f"创建文档失败: {result.stderr}")
    except Exception as e:
        print(f"创建文档异常: {e}")
    
    return ""

if __name__ == "__main__":
    # 测试
    test_alerts = [
        {
            "sku": "TEST001",
            "available": 0,
            "avg_daily_sales": 10.5,
            "purchase_sale_days": 0
        },
        {
            "sku": "TEST002",
            "available": 15,
            "avg_daily_sales": 5.2,
            "purchase_sale_days": 2
        }
    ]
    
    url = create_inventory_alert_doc(test_alerts)
    if url:
        print(f"✓ 文档创建成功: {url}")
    else:
        print("✗ 文档创建失败")
