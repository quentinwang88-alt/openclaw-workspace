#!/usr/bin/env python3
"""临时脚本：运行补货建议导出"""
import sys
from pathlib import Path

# 添加路径
sys.path.insert(0, str(Path(__file__).parent.parent / "inventory-query"))

from alert import InventoryAlert

if __name__ == "__main__":
    alert = InventoryAlert()
    
    print("=" * 60)
    print("开始计算补货建议（采购周期：5天）")
    print("=" * 60)
    
    # 调用导出功能
    result = alert.create_feishu_doc([])
    
    if result:
        print(f"\n✅ 成功！查看表格：{result}")
    else:
        print("\n❌ 导出失败")
