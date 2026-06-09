"""
Seed2Pro LLM Tier Regression Validation
Runs full pipeline for product with isolated run_id, collects structured report.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
AUTO_MIXCUT = WORKSPACE / "auto_mixcut"
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))
sys.path.insert(0, str(AUTO_MIXCUT))

from core.bitable import FeishuBitableClient
from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.core.ids import new_id
from auto_mixcut.skills.product_anchor_skill import ProductAnchorSkill
from auto_mixcut.skills.media_probe_skill import MediaProbeSkill
from auto_mixcut.skills.watermark_detect_skill import WatermarkDetectSkill
from auto_mixcut.skills.segment_skill import SegmentSkill
from auto_mixcut.skills.frame_sample_skill import FrameSampleSkill
from auto_mixcut.skills.ai_tagging_skill import AITaggingSkill
from auto_mixcut.skills.ai_generated_consistency_skill import AIGeneratedConsistencySkill
from auto_mixcut.skills.effective_role_skill import EffectiveRoleSkill
from auto_mixcut.skills.readiness_check_skill import ReadinessCheckSkill
from auto_mixcut.skills.render_plan_skill import RenderPlanSkill
from auto_mixcut.skills.render_skill import RenderSkill
from auto_mixcut.skills.quality_gate_skill import QualityGateSkill

ORIGINAL_PRODUCT_ID = "1734199053312362490"
UPLOAD_APP_TOKEN = "TAHCb5acva0osEsP8bOcWabIndd"
UPLOAD_TABLE_ID = "tblBj3UCaBRicSKS"
TASK_APP_TOKEN = "X7s0bxMAsacG6FsZyFuc725On5e"
TASK_TABLE_ID = "tblIy2XkKc2144Pm"

MAX_LLM_CALLS_PER_STEP = 100
MAX_TOTAL_LLM_CALLS = 500
ABORT_ON_STORM_THRESHOLD = 10


def _count_llm(db_path: str) -> int:
    db = sqlite3.connect(str(db_path))
    n = db.execute("SELECT COUNT(*) FROM llm_call_logs").fetchone()[0]
    db.close()
    return n


def _get_max_log_id(db_path: str) -> int:
    db = sqlite3.connect(str(db_path))
    n = db.execute("SELECT COALESCE(MAX(id),0) FROM llm_call_logs").fetchone()[0]
    db.close()
    return n


def _check_storm(db_path: str, since: int) -> list:
    db = sqlite3.connect(str(db_path))
    rows = db.execute(
        "SELECT call_type, model_name, result_status, COUNT(*) as cnt FROM llm_call_logs WHERE id > ? GROUP BY call_type, model_name, result_status HAVING cnt > ?",
        (since, ABORT_ON_STORM_THRESHOLD),
    ).fetchall()
    db.close()
    return rows


def _safe_step(fn, label: str, db_path: str, last_log_id: int):
    before = _count_llm(db_path)
    started = time.time()
    result = fn()
    elapsed = time.time() - started
    after_id = _get_max_log_id(db_path)
    new_calls = after_id - last_log_id
    total = _count_llm(db_path)
    storms = _check_storm(db_path, last_log_id)

    status = "OK" if result.success else "FAIL"
    err_msg = ""
    if not result.success and result.error:
        err_msg = f"{result.error.code}: {result.error.message}"

    print(f"  [{status}] {label} | {elapsed:.1f}s | new_llm={new_calls} total={total}" + (f" | ERR: {err_msg}" if err_msg else ""))

    if new_calls > MAX_LLM_CALLS_PER_STEP:
        print(f"  ⚠️ ABORT: step made {new_calls} LLM calls")
        sys.exit(1)
    if total > MAX_TOTAL_LLM_CALLS:
        print(f"  ⚠️ ABORT: total {total} LLM calls")
        sys.exit(1)
    if storms:
        for row in storms:
            print(f"  ⚠️ STORM: {row[0]}/{row[1]}/{row[2]} = {row[3]}x")
        sys.exit(1)

    return result


def setup_product(ctx, run_product_id: str):
    product = ctx.repo.get("products", "product_id", run_product_id)
    if product:
        print(f"  Product already exists: {run_product_id}")
        return

    client = FeishuBitableClient(app_token=TASK_APP_TOKEN, table_id=TASK_TABLE_ID)
    records = client.list_records(limit=200)
    for r in records:
        f = r.fields
        if str(f.get("商品ID", "")) == ORIGINAL_PRODUCT_ID:
            name = str(f.get("商品名称", ""))
            market = str(f.get("市场", "TH"))
            category = str(f.get("类目", "womens_tops"))
            ctx.repo.insert("products", {
                "product_id": run_product_id,
                "product_name": name,
                "market": market,
                "category": category,
                "priority": "normal",
                "product_status": "active",
            })
            print(f"  Created product: {run_product_id} | {name[:40]} | {market} | {category}")
            break

    existing_assets = ctx.repo.list_where("assets", "product_id=?", (run_product_id,))
    if existing_assets:
        print(f"  Assets already exist: {len(existing_assets)}")
        return

    original_assets = ctx.repo.list_where("assets", "product_id=?", (ORIGINAL_PRODUCT_ID,))
    if original_assets:
        print(f"  Cloning {len(original_assets)} assets from original product...")
        for a in original_assets:
            ctx.repo.insert("assets", {
                "asset_id": new_id("ASSET"),
                "product_id": run_product_id,
                "source_type": a.get("source_type", ""),
                "source_trust_level": a.get("source_trust_level", "medium"),
                "product_binding_type": a.get("product_binding_type", "same_product"),
                "media_type": a.get("media_type", "video"),
                "original_oss_object_id": a.get("original_oss_object_id", ""),
                "file_status": a.get("file_status", ""),
                "probe_status": "",
                "asset_status": "downloaded",
            })
    else:
        print("  Downloading assets from Feishu...")
        upload_client = FeishuBitableClient(app_token=UPLOAD_APP_TOKEN, table_id=UPLOAD_TABLE_ID)
        records = upload_client.list_records(limit=200)
        downloaded = 0
        for r in records:
            f = r.fields
            if str(f.get("商品ID", "")) != ORIGINAL_PRODUCT_ID:
                continue
            file_val = f.get("素材文件")
            if not file_val or not isinstance(file_val, list) or not file_val:
                continue
            item = file_val[0]
            if not isinstance(item, dict) or not item.get("file_token"):
                continue

            source_type = str(f.get("素材来源 source_type", ""))
            fname = item.get("name", "video.mp4")

            try:
                raw_bytes, _, _, fsize = upload_client.download_attachment_bytes(item)
            except Exception as e:
                print(f"  WARN download failed for {fname}: {e}")
                continue

            suffix = fname.rsplit(".", 1)[-1] if "." in fname else "mp3"
            asset_id = new_id("ASSET")
            local_dir = ctx.settings.oss_root / "auto_mixcut" / "raw" / "TH" / "womens_tops" / run_product_id / asset_id
            local_dir.mkdir(parents=True, exist_ok=True)
            local_path = local_dir / fname
            local_path.write_bytes(raw_bytes)

            object_key = f"auto_mixcut/raw/TH/womens_tops/{run_product_id}/{asset_id}/{fname}"
            upload = ctx.oss.upload(local_path, object_key)
            oss_id = upload.data.get("object_id", "") if upload.success else ""
            if not oss_id:
                oss_id = new_id("OSS")
            ctx.repo.upsert("oss_objects", "object_id", {
                "object_id": oss_id,
                "object_key": object_key,
                "bucket": ctx.settings.bucket,
                "file_hash": upload.data.get("file_hash", "") if upload.success else "",
                "mime_type": f"video/{suffix}",
                "file_size": len(raw_bytes),
                "file_name": fname,
                "file_ext": suffix,
            })

            source_type_map = {"授权达人素材": "creator", "AI生成素材": "ai_generated", "竞品素材": "competitor", "抖音/搬运素材": "tiktok"}
            trust_map = {"creator": "high", "ai_generated": "medium", "competitor": "low", "tiktok": "low"}
            mapped = source_type_map.get(source_type, source_type)

            ctx.repo.insert("assets", {
                "asset_id": asset_id,
                "product_id": run_product_id,
                "source_type": mapped,
                "source_trust_level": trust_map.get(mapped, "medium"),
                "product_binding_type": "same_product",
                "media_type": "video",
                "original_oss_object_id": oss_id,
                "file_status": "downloaded",
                "probe_status": "",
                "asset_status": "downloaded",
            })
            downloaded += 1
        print(f"  Downloaded {downloaded} assets")

    ctx.repo.insert("content_tasks", {
        "task_id": new_id("TASK"),
        "product_id": run_product_id,
        "task_type": "mixcut",
        "requested_variant_count": 5,
        "task_status": "CREATED",
    })


def run_pipeline(ctx, run_product_id: str, db_path: str):
    total_before = _count_llm(db_path)
    print(f"  Starting pipeline | existing LLM calls: {total_before}")

    # Anchor
    anchor_skill = ProductAnchorSkill(ctx)
    product = ctx.repo.get("products", "product_id", run_product_id)
    if product.get("anchor_status") != "confirmed":
        lid = _get_max_log_id(db_path)
        _safe_step(lambda: anchor_skill.draft_anchor(run_product_id), "anchor_draft", db_path, lid)
        lid = _get_max_log_id(db_path)
        _safe_step(lambda: anchor_skill.confirm_anchor(run_product_id, "auto"), "anchor_confirm", db_path, lid)
    else:
        print("  [SKIP] anchor already confirmed")

    steps = [
        ("probe", lambda: MediaProbeSkill(ctx).probe_product(run_product_id)),
        ("watermark", lambda: WatermarkDetectSkill(ctx).check_product(run_product_id)),
        ("segment", lambda: SegmentSkill(ctx).segment_product(run_product_id)),
        ("frames", lambda: FrameSampleSkill(ctx).sample_product(run_product_id)),
        ("tag_submit", lambda: AITaggingSkill(ctx).submit_batch(run_product_id)),
        ("tag_poll", lambda: AITaggingSkill(ctx).poll_results(run_product_id)),
        ("consistency", lambda: AIGeneratedConsistencySkill(ctx).check_product(run_product_id)),
        ("effective_roles", lambda: EffectiveRoleSkill(ctx).compute_product(run_product_id)),
        ("readiness", lambda: ReadinessCheckSkill(ctx).check_product(run_product_id)),
        ("render_plan", lambda: RenderPlanSkill(ctx).create_plans(run_product_id)),
    ]

    batch_id = None
    for name, fn in steps:
        lid = _get_max_log_id(db_path)
        r = _safe_step(fn, name, db_path, lid)
        if not r.success:
            print(f"\n  Pipeline STOPPED at {name}")
            return None
        if name == "render_plan" and r.data:
            batch_id = r.data.get("batch_id")
            print(f"  batch_id: {batch_id}")

    if not batch_id:
        print("  No batch_id")
        return None

    lid = _get_max_log_id(db_path)
    r = _safe_step(lambda: RenderSkill(ctx).render_batch(batch_id), "render", db_path, lid)
    if not r.success:
        print(f"  Render FAILED")
        return None

    lid = _get_max_log_id(db_path)
    r = _safe_step(lambda: QualityGateSkill(ctx).check_batch(batch_id), "quality", db_path, lid)

    total_after = _count_llm(db_path)
    print(f"  Pipeline complete | total LLM calls this run: {total_after - total_before}")
    return batch_id


def collect_report(ctx, run_product_id: str, batch_id: str | None, db_path: str) -> dict:
    db = sqlite3.connect(db_path)
    report = {
        "product_id": ORIGINAL_PRODUCT_ID,
        "run_product_id": run_product_id,
        "run_time": datetime.utcnow().isoformat(timespec="seconds"),
        "asset_count": 0,
        "segment_count": 0,
        "tagged_segment_count": 0,
        "generated_output_count": 0,
        "passed_output_count": 0,
        "failed_output_count": 0,
        "template_distribution": {},
        "segment_role_distribution": {},
        "qc_fail_reasons": {},
        "bgm_distribution": {},
        "llm_route_summary": {},
        "human_review_segment_count": 0,
        "high_risk_segment_count": 0,
        "sample_outputs": [],
    }

    report["asset_count"] = db.execute("SELECT COUNT(*) FROM assets WHERE product_id=?", (run_product_id,)).fetchone()[0]
    report["segment_count"] = db.execute("SELECT COUNT(*) FROM segments WHERE product_id=?", (run_product_id,)).fetchone()[0]
    report["tagged_segment_count"] = db.execute("SELECT COUNT(DISTINCT segment_id) FROM segment_tags WHERE segment_id IN (SELECT segment_id FROM segments WHERE product_id=?)", (run_product_id,)).fetchone()[0]

    if batch_id:
        report["generated_output_count"] = db.execute("SELECT COUNT(*) FROM outputs WHERE batch_id=?", (batch_id,)).fetchone()[0]
        report["passed_output_count"] = db.execute("SELECT COUNT(*) FROM outputs WHERE batch_id=? AND machine_quality_status='passed'", (batch_id,)).fetchone()[0]
        report["failed_output_count"] = db.execute("SELECT COUNT(*) FROM outputs WHERE batch_id=? AND machine_quality_status!='passed'", (batch_id,)).fetchone()[0]

        template_rows = db.execute("SELECT template_id, COUNT(*) FROM render_plans WHERE batch_id=? GROUP BY template_id", (batch_id,)).fetchall()
        report["template_distribution"] = dict(template_rows)

    role_rows = db.execute("""
        SELECT t.primary_shot_role, COUNT(*) FROM segment_tags t
        JOIN segments s ON t.segment_id = s.segment_id
        WHERE s.product_id=? GROUP BY t.primary_shot_role
    """, (run_product_id,)).fetchall()
    report["segment_role_distribution"] = dict(role_rows)

    review_count = db.execute("""
        SELECT COUNT(*) FROM segment_tags t
        JOIN segments s ON t.segment_id = s.segment_id
        WHERE s.product_id=? AND t.needs_human_review=1
    """, (run_product_id,)).fetchone()[0]
    report["human_review_segment_count"] = review_count

    high_risk = db.execute("""
        SELECT COUNT(*) FROM segment_tags t
        JOIN segments s ON t.segment_id = s.segment_id
        WHERE s.product_id=? AND t.risk_level IN ('medium','high')
    """, (run_product_id,)).fetchone()[0]
    report["high_risk_segment_count"] = high_risk

    llm_rows = db.execute("""
        SELECT call_type, model_tier, model_name, result_status, COUNT(*), AVG(latency_ms), SUM(cache_hit)
        FROM llm_call_logs GROUP BY call_type, model_tier, model_name, result_status
    """).fetchall()
    for row in llm_rows:
        key = f"{row[0]}|{row[1]}|{row[2]}|{row[3]}"
        report["llm_route_summary"][key] = {"count": row[4], "avg_latency_ms": int(row[5] or 0), "cache_hits": row[6]}

    tag_detail = {}
    for field in ["primary_shot_role", "product_visibility", "hook_strength", "mixcut_usability", "risk_level", "confidence"]:
        rows = db.execute(f"""
            SELECT {field}, COUNT(*) FROM segment_tags t
            JOIN segments s ON t.segment_id = s.segment_id
            WHERE s.product_id=? GROUP BY {field}
        """, (run_product_id,)).fetchall()
        tag_detail[field] = dict(rows)
    report["tag_detail_distribution"] = tag_detail

    if batch_id:
        outputs = db.execute("SELECT output_id, variant_no, template_id, machine_quality_status, output_oss_object_id FROM outputs WHERE batch_id=?", (batch_id,)).fetchall()
        for o in outputs:
            report["sample_outputs"].append({
                "output_id": o[0],
                "variant_no": o[1],
                "template_id": o[2],
                "qc_status": o[3],
            })

    db.close()
    return report


def main():
    print("=" * 60)
    print("Seed2Pro Regression Validation")
    print("=" * 60)

    ctx = build_context()
    db_path = str(ctx.settings.db_path)
    run_product_id = open("/tmp/auto_mixcut_reg_run_id.txt").read().strip()

    print(f"\nOriginal product: {ORIGINAL_PRODUCT_ID}")
    print(f"Run product ID:   {run_product_id}")
    print(f"mock_llm: {ctx.settings.mock_llm}")

    if ctx.settings.mock_llm:
        print("FATAL: mock_llm is True! Cannot run real validation.")
        sys.exit(1)

    print("\n--- Step 1: Setup ---")
    setup_product(ctx, run_product_id)

    assets = ctx.repo.list_where("assets", "product_id=?", (run_product_id,))
    segments = ctx.repo.list_where("segments", "product_id=?", (run_product_id,))
    print(f"  Assets: {len(assets)} | Segments: {len(segments)}")

    print("\n--- Step 2: Run Pipeline ---")
    batch_id = run_pipeline(ctx, run_product_id, db_path)

    print("\n--- Step 3: Collect Report ---")
    report = collect_report(ctx, run_product_id, batch_id, db_path)

    report_path = AUTO_MIXCUT / f"reg_report_{run_product_id}.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"  Report saved: {report_path}")

    print("\n--- Summary ---")
    print(f"  Assets:        {report['asset_count']}")
    print(f"  Segments:      {report['segment_count']}")
    print(f"  Tagged:        {report['tagged_segment_count']}")
    print(f"  Outputs:       {report['generated_output_count']}")
    print(f"  QC Passed:     {report['passed_output_count']}")
    print(f"  QC Failed:     {report['failed_output_count']}")
    print(f"  Needs Review:  {report['human_review_segment_count']}")
    print(f"  High Risk:     {report['high_risk_segment_count']}")
    print(f"\n  LLM Routes:")
    for k, v in report["llm_route_summary"].items():
        print(f"    {k} → count={v['count']} avg_lat={v['avg_latency_ms']}ms cache={v['cache_hits']}")
    print(f"\n  Tag Distribution:")
    for field, dist in report.get("tag_detail_distribution", {}).items():
        print(f"    {field}: {dist}")

    segment_tagging_key = [k for k in report["llm_route_summary"] if k.startswith("segment_tagging_default|medium_vision|doubao-seed-2-0-pro")]
    if segment_tagging_key:
        print(f"\n  ✅ seed2pro confirmed in segment_tagging_default main path")
    else:
        print(f"\n  ❌ seed2pro NOT found in segment_tagging_default! Check config!")

    print("\nDone.")


if __name__ == "__main__":
    main()
