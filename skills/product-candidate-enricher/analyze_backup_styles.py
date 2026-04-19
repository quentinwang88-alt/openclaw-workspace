#!/usr/bin/env python3
"""分析备选商品风格分布"""

import sys
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent))

from core.feishu import FeishuBitableClient, parse_feishu_bitable_url, resolve_wiki_bitable_app_token

FEISHU_URL = "https://gcngopvfvo0q.feishu.cn/wiki/CtGxwJpTEifSh5kIVtgcM2vCnLf?table=tblKhPn64Q266tRz&view=vewmWdRUHq"

def main():
    # 解析URL
    info = parse_feishu_bitable_url(FEISHU_URL)
    app_token = resolve_wiki_bitable_app_token(info.app_token) if info.is_wiki else info.app_token

    client = FeishuBitableClient(app_token=app_token, table_id=info.table_id)

    # 获取所有记录
    print("📖 读取表格记录...")
    records = client.list_records(limit=None)
    print(f"✅ 共读取 {len(records)} 条记录")

    # 筛选备选商品
    backup_records = []
    for record in records:
        fields = record.fields
        # 检查是否列入备选
        is_backup = fields.get("是否列入备选")
        if is_backup == True or is_backup == "true" or is_backup == "是":
            backup_records.append({
                "record_id": record.record_id,
                "product_name": fields.get("中文名称", fields.get("商品名称", ""))[:50],
                "style": fields.get("产品风格", "未标记"),
                "subcategory": fields.get("子类目", "未分类"),
                "recommendation": fields.get("是否推荐", "未评估"),
                "reason": fields.get("详细原因", "")[:100],
                "sales_7d": fields.get("7天销量", 0),
                "price": fields.get("售价", ""),
            })

    print(f"\n📊 备选商品统计: {len(backup_records)} 条")

    if not backup_records:
        print("⚠️ 没有找到标记为备选的商品")
        return

    # 按风格统计
    style_counter = Counter([r["style"] for r in backup_records])
    print(f"\n🎨 风格分布:")
    for style, count in style_counter.most_common():
        print(f"   {style}: {count} 条 ({count/len(backup_records)*100:.1f}%)")

    # 按子类目统计
    subcategory_counter = Counter([r["subcategory"] for r in backup_records])
    print(f"\n📦 子类目分布:")
    for sub, count in subcategory_counter.most_common():
        print(f"   {sub}: {count} 条")

    # 按推荐状态统计
    rec_counter = Counter([r["recommendation"] for r in backup_records])
    print(f"\n✅ 推荐状态分布:")
    for rec, count in rec_counter.most_common():
        print(f"   {rec}: {count} 条")

    # Top 10 销量备选商品
    print(f"\n🔥 Top 10 销量备选商品:")
    top_sales = sorted(backup_records, key=lambda x: float(str(x["sales_7d"]).replace(",", "") or 0), reverse=True)[:10]
    for i, item in enumerate(top_sales, 1):
        print(f"   {i}. {item['product_name'][:30]}... | 风格: {item['style']} | 7天销量: {item['sales_7d']}")

    # 输出未标记风格的备选商品
    unstyled = [r for r in backup_records if r["style"] == "未标记" or not r["style"]]
    if unstyled:
        print(f"\n⚠️ 未标记风格的备选商品 ({len(unstyled)} 条):")
        for item in unstyled[:10]:
            print(f"   - {item['product_name'][:40]}...")

if __name__ == "__main__":
    main()
