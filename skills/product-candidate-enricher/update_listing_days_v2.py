#!/usr/bin/env python3
"""批量更新上架天数 - 使用批量API"""

import sys
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent))

from core.feishu import FeishuBitableClient, parse_feishu_bitable_url, resolve_wiki_bitable_app_token

FEISHU_URL = "https://gcngopvfvo0q.feishu.cn/wiki/CtGxwJpTEifSh5kIVtgcM2vCnLf?table=tblKhPn64Q266tRz&view=vewmWdRUHq"
FIELD_LISTING_TIME = "预估商品上架时间"
FIELD_LISTING_DAYS = "上架天数"

def compute_listing_days(listing_timestamp_ms, now):
    """计算上架天数"""
    if not listing_timestamp_ms:
        return None
    try:
        timestamp = float(listing_timestamp_ms)
        if timestamp > 1_000_000_000_000:
            timestamp /= 1000.0
        listing_dt = datetime.fromtimestamp(timestamp, tz=ZoneInfo("Asia/Shanghai"))
        return max(0, (now.date() - listing_dt.date()).days)
    except Exception:
        return None

def main():
    now = datetime.now(ZoneInfo("Asia/Shanghai"))

    # 解析URL
    info = parse_feishu_bitable_url(FEISHU_URL)
    app_token = resolve_wiki_bitable_app_token(info.app_token) if info.is_wiki else info.app_token

    client = FeishuBitableClient(app_token=app_token, table_id=info.table_id)

    # 获取所有记录
    print("📖 读取表格记录...")
    records = client.list_records(limit=None)
    print(f"✅ 共读取 {len(records)} 条记录")

    # 计算需要更新的记录
    updates = []
    for record in records:
        listing_time = record.fields.get(FIELD_LISTING_TIME)
        if listing_time:
            days = compute_listing_days(listing_time, now)
            if days is not None:
                updates.append({
                    "record_id": record.record_id,
                    "days": days
                })

    print(f"📊 需要更新 {len(updates)} 条记录的上架天数")

    # 批量更新 - 每批500条
    batch_size = 500
    total_updated = 0

    for i in range(0, len(updates), batch_size):
        batch = updates[i:i+batch_size]
        print(f"\n🔄 处理批次 {i//batch_size + 1}/{(len(updates)-1)//batch_size + 1} ({len(batch)} 条)...")

        # 构建批量更新数据
        batch_records = [
            {"record_id": item["record_id"], "fields": {FIELD_LISTING_DAYS: str(item["days"])}}
            for item in batch
        ]

        try:
            client.batch_update_records(batch_records)
            total_updated += len(batch)
            print(f"  ✅ 批次完成，累计更新 {total_updated} 条")
        except Exception as e:
            print(f"  ⚠️ 批次更新失败: {e}")
            # 尝试单条更新
            for item in batch:
                try:
                    client.update_record_fields(
                        record_id=item["record_id"],
                        fields={FIELD_LISTING_DAYS: str(item["days"])}
                    )
                    total_updated += 1
                except Exception as e2:
                    print(f"    ⚠️ 单条更新失败 {item['record_id']}: {e2}")

    print(f"\n🎉 全部完成！共更新 {total_updated} 条记录的上架天数")

    # 输出统计
    days_list = [u["days"] for u in updates]
    if days_list:
        print(f"\n📈 上架天数统计:")
        print(f"   最小: {min(days_list)} 天")
        print(f"   最大: {max(days_list)} 天")
        print(f"   平均: {sum(days_list)/len(days_list):.1f} 天")

if __name__ == "__main__":
    main()
