"""
Run full pipeline for product 1734199053312362490 with step-by-step monitoring.
Aborts on: infinite loops, excessive LLM calls, retry storms.
"""
from __future__ import annotations

import sys
import time
import sqlite3
from pathlib import Path

WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
AUTO_MIXCUT = WORKSPACE / "auto_mixcut"
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))
sys.path.insert(0, str(AUTO_MIXCUT))

from core.bitable import FeishuBitableClient  # noqa: E402
from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.agent.orchestrator import AutoMixcutOrchestratorAgent  # noqa: E402
from auto_mixcut.skills.product_anchor_skill import ProductAnchorSkill  # noqa: E402
from auto_mixcut.skills.media_probe_skill import MediaProbeSkill  # noqa: E402
from auto_mixcut.skills.watermark_detect_skill import WatermarkDetectSkill  # noqa: E402
from auto_mixcut.skills.segment_skill import SegmentSkill  # noqa: E402
from auto_mixcut.skills.frame_sample_skill import FrameSampleSkill  # noqa: E402
from auto_mixcut.skills.ai_tagging_skill import AITaggingSkill  # noqa: E402
from auto_mixcut.skills.ai_generated_consistency_skill import AIGeneratedConsistencySkill  # noqa: E402
from auto_mixcut.skills.effective_role_skill import EffectiveRoleSkill  # noqa: E402
from auto_mixcut.skills.readiness_check_skill import ReadinessCheckSkill  # noqa: E402
from auto_mixcut.skills.render_plan_skill import RenderPlanSkill  # noqa: E402
from auto_mixcut.skills.render_skill import RenderSkill  # noqa: E402
from auto_mixcut.skills.quality_gate_skill import QualityGateSkill  # noqa: E402

PRODUCT_ID = "1734199053312362490"
UPLOAD_APP_TOKEN = "TAHCb5acva0osEsP8bOcWabIndd"
UPLOAD_TABLE_ID = "tblBj3UCaBRicSKS"
MAX_LLM_CALLS_PER_STEP = 50
MAX_TOTAL_LLM_CALLS = 200


def _count_llm_calls(db_path: str) -> int:
    db = sqlite3.connect(str(db_path))
    n = db.execute("SELECT COUNT(*) FROM llm_call_logs").fetchone()[0]
    db.close()
    return n


def _count_llm_calls_since(db_path: str, since: int) -> int:
    db = sqlite3.connect(str(db_path))
    n = db.execute("SELECT COUNT(*) FROM llm_call_logs WHERE id > ?", (since,)).fetchone()[0]
    db.close()
    return n


def _check_retry_storm(db_path: str, since: int, threshold: int = 10) -> list:
    db = sqlite3.connect(str(db_path))
    rows = db.execute(
        "SELECT call_type, model_name, result_status, COUNT(*) as cnt FROM llm_call_logs WHERE id > ? GROUP BY call_type, model_name, result_status HAVING cnt > ?",
        (since, threshold),
    ).fetchall()
    db.close()
    return rows


def _get_max_log_id(db_path: str) -> int:
    db = sqlite3.connect(str(db_path))
    n = db.execute("SELECT COALESCE(MAX(id),0) FROM llm_call_logs").fetchone()[0]
    db.close()
    return n


def _safe(fn, label: str, db_path: str, max_log_id: int):
    before = _count_llm_calls(db_path)
    before_id = max_log_id
    print(f"\n{'='*60}")
    print(f"STEP: {label}")
    print(f"{'='*60}")
    started = time.time()

    result = fn()

    elapsed = time.time() - started
    after_id = _get_max_log_id(db_path)
    new_calls = after_id - before_id
    total_calls = _count_llm_calls(db_path)

    storms = _check_retry_storm(db_path, before_id, threshold=5)

    print(f"  result: {'OK' if result.success else 'FAIL'}")
    if not result.success and result.error:
        print(f"  error: {result.error.code} - {result.error.message}")
    print(f"  elapsed: {elapsed:.1f}s")
    print(f"  new LLM calls: {new_calls}")
    print(f"  total LLM calls: {total_calls}")

    if new_calls > MAX_LLM_CALLS_PER_STEP:
        print(f"  ⚠️ ABORT: step made {new_calls} LLM calls (limit {MAX_LLM_CALLS_PER_STEP})")
        sys.exit(1)
    if total_calls > MAX_TOTAL_LLM_CALLS:
        print(f"  ⚠️ ABORT: total {total_calls} LLM calls (limit {MAX_TOTAL_LLM_CALLS})")
        sys.exit(1)
    if storms:
        print(f"  ⚠️ RETRY STORM detected:")
        for row in storms:
            print(f"    {row[0]}/{row[1]}/{row[2]} = {row[3]} times")
        print(f"  ⚠️ ABORT: retry storm detected")
        sys.exit(1)

    if result.success and result.data:
        keys = list(result.data.keys()) if isinstance(result.data, dict) else []
        for k in keys[:5]:
            v = result.data[k]
            if isinstance(v, (str, int, float, bool)):
                print(f"  {k}: {v}")
            elif isinstance(v, list):
                print(f"  {k}: [{len(v)} items]")

    return result


def main():
    print(f"Pipeline run for product: {PRODUCT_ID}")
    ctx = build_context()
    db_path = str(ctx.settings.db_path)
    print(f"DB: {db_path}")
    print(f"mock_llm: {ctx.settings.mock_llm}")
    print(f"oss_root: {ctx.settings.oss_root}")

    total_llm_before = _count_llm_calls(db_path)
    print(f"Existing LLM call logs: {total_llm_before}")

    # === STEP 0: Create product if not exists ===
    product = ctx.repo.get("products", "product_id", PRODUCT_ID)
    if not product:
        print("\n--- Creating product from Feishu ---")
        client = FeishuBitableClient(app_token="X7s0bxMAsacG6FsZyFuc725On5e", table_id="tblIy2XkKc2144Pm")
        records = client.list_records(limit=200)
        for r in records:
            f = r.fields
            if str(f.get("商品ID", "")) == PRODUCT_ID:
                name = str(f.get("商品名称", ""))
                market = str(f.get("市场", "TH"))
                category = str(f.get("类目", "womens_tops"))
                ctx.repo.insert("products", {
                    "product_id": PRODUCT_ID,
                    "product_name": name,
                    "market": market,
                    "category": category,
                    "priority": "normal",
                    "product_status": "active",
                })
                print(f"  Created: {PRODUCT_ID} | {name[:40]} | {market} | {category}")
                break
        product = ctx.repo.get("products", "product_id", PRODUCT_ID)
        if not product:
            print("FATAL: Could not create product")
            sys.exit(1)

    # === STEP 0.5: Download assets from Feishu ===
    existing_assets = ctx.repo.list_where("assets", "product_id=?", (PRODUCT_ID,))
    if not existing_assets:
        print("\n--- Downloading video assets from Feishu ---")
        upload_client = FeishuBitableClient(app_token=UPLOAD_APP_TOKEN, table_id=UPLOAD_TABLE_ID)
        records = upload_client.list_records(limit=200)
        downloaded = 0
        for r in records:
            f = r.fields
            if str(f.get("商品ID", "")) != PRODUCT_ID:
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

            suffix = fname.rsplit(".", 1)[-1] if "." in fname else "mp4"
            from auto_mixcut.core.ids import new_id
            asset_id = new_id("ASSET")

            local_dir = ctx.settings.oss_root / "auto_mixcut" / "raw" / "TH" / "womens_tops" / PRODUCT_ID / asset_id
            local_dir.mkdir(parents=True, exist_ok=True)
            local_path = local_dir / fname
            local_path.write_bytes(raw_bytes)

            object_key = f"auto_mixcut/raw/TH/womens_tops/{PRODUCT_ID}/{asset_id}/{fname}"
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

            source_type_map = {
                "授权达人素材": "creator",
                "AI生成素材": "ai_generated",
                "竞品素材": "competitor",
                "抖音/搬运素材": "tiktok",
            }
            mapped_type = source_type_map.get(source_type, source_type)

            trust_map = {
                "creator": "high",
                "ai_generated": "medium",
                "competitor": "low",
                "tiktok": "low",
            }
            trust = trust_map.get(mapped_type, "medium")

            binding_type_map = {
                "当前商品同款": "same_product",
            }
            binding = str(f.get("商品绑定类型 product_binding_type", ""))
            mapped_binding = binding_type_map.get(binding, "same_product")

            ctx.repo.insert("assets", {
                "asset_id": asset_id,
                "product_id": PRODUCT_ID,
                "source_type": mapped_type,
                "source_trust_level": trust,
                "product_binding_type": mapped_binding,
                "media_type": "video",
                "original_oss_object_id": oss_id,
                "file_status": "downloaded",
                "probe_status": "",
                "asset_status": "downloaded",
            })
            downloaded += 1
            print(f"  ✓ {fname} ({fsize//1024}KB) as {mapped_type}")

        print(f"  Downloaded {downloaded} assets")
    else:
        print(f"\n  Assets already exist: {len(existing_assets)}")

    # === RUN PIPELINE STEPS ===
    last_log_id = _get_max_log_id(db_path)

    # Step 1: anchor_draft
    anchor_skill = ProductAnchorSkill(ctx)
    product = ctx.repo.get("products", "product_id", PRODUCT_ID)
    if product.get("anchor_status") != "confirmed":
        r = _safe(lambda: anchor_skill.draft_anchor(PRODUCT_ID), "anchor_draft", db_path, last_log_id)
        if not r.success:
            print(f"\nPipeline STOPPED at anchor_draft: {r.error.message if r.error else 'unknown'}")
            return
        # Auto-confirm
        r2 = _safe(lambda: anchor_skill.confirm_anchor(PRODUCT_ID, "auto"), "anchor_confirm", db_path, last_log_id)
        if not r2.success:
            print(f"\nPipeline STOPPED at anchor_confirm: {r2.error.message if r2.error else 'unknown'}")
            return
    else:
        print("\n[SKIP] anchor already confirmed")

    steps = [
        ("probe", lambda: MediaProbeSkill(ctx).probe_product(PRODUCT_ID)),
        ("watermark", lambda: WatermarkDetectSkill(ctx).check_product(PRODUCT_ID)),
        ("segment", lambda: SegmentSkill(ctx).segment_product(PRODUCT_ID)),
        ("frames", lambda: FrameSampleSkill(ctx).sample_product(PRODUCT_ID)),
        ("tag_submit", lambda: AITaggingSkill(ctx).submit_batch(PRODUCT_ID)),
        ("tag_poll", lambda: AITaggingSkill(ctx).poll_results(PRODUCT_ID)),
        ("consistency", lambda: AIGeneratedConsistencySkill(ctx).check_product(PRODUCT_ID)),
        ("effective_roles", lambda: EffectiveRoleSkill(ctx).compute_product(PRODUCT_ID)),
        ("readiness", lambda: ReadinessCheckSkill(ctx).check_product(PRODUCT_ID)),
        ("render_plan", lambda: RenderPlanSkill(ctx).create_plans(PRODUCT_ID)),
    ]

    batch_id = None
    for name, fn in steps:
        last_log_id = _get_max_log_id(db_path)
        r = _safe(fn, name, db_path, last_log_id)
        if not r.success:
            print(f"\nPipeline STOPPED at {name}: {r.error.message if r.error else 'unknown'}")
            return
        if name == "render_plan" and r.success and r.data:
            batch_id = r.data.get("batch_id")
            print(f"  batch_id: {batch_id}")

    if not batch_id:
        print("\nNo batch_id - cannot render")
        return

    # Render
    last_log_id = _get_max_log_id(db_path)
    r = _safe(lambda: RenderSkill(ctx).render_batch(batch_id), "render", db_path, last_log_id)
    if not r.success:
        print(f"\nPipeline STOPPED at render: {r.error.message if r.error else 'unknown'}")
        return

    # QC
    last_log_id = _get_max_log_id(db_path)
    r = _safe(lambda: QualityGateSkill(ctx).check_batch(batch_id), "quality", db_path, last_log_id)

    # Summary
    total_llm_after = _count_llm_calls(db_path)
    print(f"\n{'='*60}")
    print(f"PIPELINE COMPLETE")
    print(f"Total LLM calls this run: {total_llm_after - total_llm_before}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
