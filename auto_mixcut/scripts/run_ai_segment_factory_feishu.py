#!/usr/bin/env python3
"""轮询飞书 AI片段生成任务表，处理「待生成」状态的任务。"""
from __future__ import annotations

import sys
from pathlib import Path

WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402

sys.path.insert(0, str(WORKSPACE / "auto_mixcut"))
from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.skills.ai_segment_factory_skill import AISegmentFactorySkill  # noqa: E402
from auto_mixcut.skills.product_anchor_skill import ProductAnchorSkill  # noqa: E402
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill  # noqa: E402

FEISHU_URL = "https://gcngopvfvo0q.feishu.cn/wiki/WyNuwaTThiI3NDk7qyccflrunGe?table=tblVWMXmsAiA6DZV&view=vewCjgjw3s"

info = parse_feishu_bitable_url(FEISHU_URL)
app_token = resolve_wiki_bitable_app_token(info.app_token)
feishu = FeishuBitableClient(app_token=app_token, table_id=info.table_id)

records = feishu.list_records(page_size=100)
print(f"Found {len(records)} records in Feishu table")

pending = []
for rec in records:
    product_id = str(rec.fields.get("商品ID") or "").strip()
    status = str(rec.fields.get("状态") or "").strip()
    segment_type_raw = str(rec.fields.get("片段类型") or "").strip()
    count = int(rec.fields.get("生成数量") or 0)
    print(f"  {product_id}: status={status}, type={segment_type_raw}, count={count}")

    if status == "待生成" and product_id and segment_type_raw:
        pending.append((rec.record_id, product_id, segment_type_raw, count))

if not pending:
    print("\n没有待生成的任务。")
    sys.exit(0)

SEGMENT_TYPE_MAP = {
    "商品桌面展示": "product_display",
    "手拿商品": "handheld_product",
    "细节氛围": "detail_atmosphere",
    "佩戴/上身效果": "tryon_result",
    "镜前整理": "mirror_routine",
    "居家生活场景": "home_lifestyle",
    "出门前场景": "before_go_out",
    "季节/场景氛围": "seasonal_scene",
}

ctx = build_context()
rds = RDSRepositorySkill(ctx)
rds.init_db()

for feishu_record_id, product_id, segment_type_cn, count in pending:
    segment_type = SEGMENT_TYPE_MAP.get(segment_type_cn)
    if not segment_type:
        print(f"\nSKIP {product_id}: 未知片段类型 '{segment_type_cn}'")
        continue

    if count <= 0:
        count = 5

    print(f"\n=== Processing {product_id} ({segment_type_cn} x{count}) ===")

    # 确保本地有 product 且锚点已确认
    product = ctx.repo.get("products", "product_id", product_id)
    if not product:
        print(f"  Creating product in local DB...")
        rds.create_product_task(product_id, product_id, "VN", "hair_accessories", count)
        product = ctx.repo.get("products", "product_id", product_id) or {}

    if product.get("anchor_status") != "confirmed":
        print(f"  Drafting anchor (local DB anchor_status={product.get('anchor_status')})...")
        ProductAnchorSkill(ctx).draft_anchor(product_id)
        ProductAnchorSkill(ctx).confirm_anchor(product_id, "auto")

    # Verify anchor source
    anchor_data = ctx.repo.get("products", "product_id", product_id) or {}
    anchor_json = anchor_data.get("product_anchor_json") or {}
    anchor_version = anchor_data.get("anchor_version", "")
    anchor_keys = list(anchor_json.keys())[:5] if isinstance(anchor_json, dict) else []
    print(f"  Anchor: v={anchor_version}, keys={anchor_keys}")

    # 更新飞书状态为「锚点检查中」
    feishu.update_record_fields(feishu_record_id, {"状态": "锚点检查中"})

    res = AISegmentFactorySkill(ctx).run(
        product_id=product_id,
        segment_type=segment_type,
        requested_count=count,
    )

    d = res.to_dict()
    if res.success:
        grading = d["data"].get("grading", {})
        feishu.update_record_fields(feishu_record_id, {
            "状态": "已入库",
            "生成任务ID": d["data"]["job_id"],
            "实际生成数量": d["data"]["requested_count"],
            "入库片段数量": grading.get("total", 0),
            "A_core数量": grading.get("A_core", 0),
            "B_scene数量": grading.get("B_scene", 0),
            "C_reference数量": grading.get("C_reference", 0),
            "D_reject数量": grading.get("D_reject", 0),
            "核心可用率": f'{grading.get("core_rate", 0)}%',
            "场景可用率": f'{grading.get("scene_rate", 0)}%',
            "废弃率": f'{grading.get("reject_rate", 0)}%',
            "片段风险等级": d["data"].get("segment_type_risk", ""),
        })
        print(f"  OK: job={d['data']['job_id']}, A:{grading.get('A_core',0)} B:{grading.get('B_scene',0)} C:{grading.get('C_reference',0)} D:{grading.get('D_reject',0)}")
    else:
        error_msg = d.get("error", {}).get("message", "未知错误") if d.get("error") else "未知错误"
        feishu.update_record_fields(feishu_record_id, {
            "状态": "生成失败",
            "失败原因": error_msg,
        })
        print(f"  FAIL: {error_msg}")

print("\nDone.")
