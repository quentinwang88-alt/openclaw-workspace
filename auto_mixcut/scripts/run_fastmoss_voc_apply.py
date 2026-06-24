#!/usr/bin/env python3
"""Apply Fastmoss VOC insights to product-level ADS selling-point candidates."""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.adapters.feishu import AutoMixcutFeishuClient, datetime_cell  # noqa: E402
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill  # noqa: E402
from auto_mixcut.skills.segment_prompt_factory_skill import SegmentPromptFactorySkill  # noqa: E402


EXPORT_BATCH_TABLE = "fastmoss_voc_export_batch"
INSIGHT_PACK_TABLE = "fastmoss_voc_insight_pack"
INSIGHT_TABLE = "fastmoss_voc_insight"
HOOK_TABLE = "fastmoss_voc_hook"
FLOW_ISSUE_TABLE = "fastmoss_voc_flow_issue"
RECOMMENDATION_TABLE = "fastmoss_voc_product_recommendation"
FORM_SUMMARY_TABLE = "fastmoss_voc_product_form_summary"
ENRICHED_TABLE = "fastmoss_voc_enriched"
CORE_INSIGHT_IDS = {"selling_appearance_cute_color", "selling_hold_quality"}

# taxonomy loader (shared with voc-insight)
_TAXONOMY_CACHE: Optional[Dict[str, Any]] = None

def _load_taxonomy() -> Dict[str, Any]:
    global _TAXONOMY_CACHE
    if _TAXONOMY_CACHE is not None:
        return _TAXONOMY_CACHE
    paths = [
        "/Users/likeu3/.openclaw/workspace/skills/voc-insight/references/video_proof_taxonomy.json",
    ]
    for p in paths:
        if os.path.exists(p):
            try:
                with open(p, encoding="utf-8") as f:
                    _TAXONOMY_CACHE = json.load(f)
                return _TAXONOMY_CACHE
            except Exception:
                pass
    _TAXONOMY_CACHE = {}
    return _TAXONOMY_CACHE

# keep old for backward compat, but prefer taxonomy
LEGACY_FALLBACK: Dict[str, Dict[str, Any]] = {
    "selling_appearance_cute_color": {
        "proof_archetype": "appearance_transform", "usage_lane": "video_hook", "video_fit_score": 85,
    },
    "selling_hold_quality": {
        "proof_archetype": "state_stability_proof", "usage_lane": "video_hook", "video_fit_score": 80,
    },
    "selling_value_quantity": {
        "proof_archetype": "copy_only", "usage_lane": "copy_only", "video_fit_score": 30,
    },
    "selling_fast_shipping": {
        "proof_archetype": "copy_only", "usage_lane": "copy_only", "video_fit_score": 20,
    },
}


CATEGORY_TO_MIXCUT = {
    "hair_clip": "hair_accessories",
    "hair_accessories": "hair_accessories",
    "earrings": "earrings",
    "bracelets": "bracelets",
    "necklace": "necklaces",
    "necklaces": "necklaces",
    "scarves_hats": "scarves_hats",
    "womens_top": "womens_outerwear",
    "womens_tops": "womens_outerwear",
    "womens_outerwear": "womens_outerwear",
}


HOOK_INTENTS = {
    "selling_appearance_cute_color": "tryon_result",
    "selling_hold_quality": "contrast_reveal",
    "selling_value_quantity": "product_clarity",
    "selling_fast_shipping": "atmosphere",
    "pain_fulfillment_completeness": "product_clarity",
}


SEGMENT_TYPE_BY_HOOK_INTENT = {
    "tryon_result": "tryon_result",
    "contrast_reveal": "tryon_result",
    "product_clarity": "product_display",
    "material_closeup": "detail_atmosphere",
    "atmosphere": "before_go_out",
}


CONFIDENCE_WEIGHT = {"high": 1.0, "medium": 0.75, "low": 0.45}


FORM_LABELS = {
    "bow_clip": "蝴蝶结发夹",
    "basic_hair_clip": "基础发夹/发卡",
    "duckbill_clip": "鸭嘴夹/侧夹",
    "floral_clip": "花朵发夹",
    "hair_pad_or_sticker": "发帖片/碎发贴",
    "character_clip": "卡通/角色发夹",
    "hair_comb_or_volumizer": "发梳/蓬松工具",
    "assorted_clip_set": "组合装发夹",
    "pearl_rhinestone_clip": "珍珠/水钻发夹",
    "unknown_hair_accessory": "未知发饰形态",
    "claw_clip": "鲨鱼夹/抓夹",
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--product-id", default="")
    parser.add_argument("--product-form", default="")
    parser.add_argument("--min-form-products", type=int, default=5)
    parser.add_argument("--min-form-voc", type=int, default=20)
    parser.add_argument("--market", default="")
    parser.add_argument("--mixcut-category", default="")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--allow-warning", action="store_true")
    parser.add_argument("--inspect", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--write-recommendation", action="store_true")
    parser.add_argument("--sync-feishu-task", action="store_true")
    parser.add_argument("--prompt-smoke", action="store_true")
    args = parser.parse_args()

    ctx = build_context()
    init = RDSRepositorySkill(ctx).init_db()
    if not init.success:
        print(json.dumps(init.to_dict(), ensure_ascii=False, indent=2, default=str), file=sys.stderr)
        return 1

    try:
        if args.inspect and not (args.dry_run or args.write_recommendation or args.prompt_smoke):
            result = inspect_batch(ctx, args.batch_id)
        else:
            result = apply_batch(
                ctx,
                batch_id=args.batch_id,
                product_id=args.product_id,
                product_form=args.product_form,
                min_form_products=args.min_form_products,
                min_form_voc=args.min_form_voc,
                market=args.market,
                mixcut_category=args.mixcut_category,
                limit=args.limit,
                allow_warning=args.allow_warning,
                dry_run=args.dry_run,
                write_recommendation=args.write_recommendation,
                sync_feishu_task=args.sync_feishu_task,
                prompt_smoke=args.prompt_smoke,
            )
    except RuntimeError as exc:
        result = {"success": False, "error": str(exc)}
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("success") else 1


def inspect_batch(ctx: Any, batch_id: str) -> Dict[str, Any]:
    summary = load_batch_summary(ctx, batch_id)
    return {"success": True, "mode": "inspect", **summary}


def apply_batch(
    ctx: Any,
    batch_id: str,
    product_id: str = "",
    product_form: str = "",
    min_form_products: int = 5,
    min_form_voc: int = 20,
    market: str = "",
    mixcut_category: str = "",
    limit: int = 10,
    allow_warning: bool = False,
    dry_run: bool = False,
    write_recommendation: bool = False,
    sync_feishu_task: bool = False,
    prompt_smoke: bool = False,
) -> Dict[str, Any]:
    summary = load_batch_summary(ctx, batch_id)
    form_summary = load_form_summary(ctx, batch_id, product_form) if product_form else {}
    gate = quality_gate(
        summary,
        allow_warning=allow_warning,
        product_form=product_form,
        form_summary=form_summary,
        min_form_products=min_form_products,
        min_form_voc=min_form_voc,
    )
    if not gate["passed"]:
        return {"success": False, "mode": "apply", "batch": summary, "product_form_summary": form_summary, "quality_gate": gate}

    products = load_target_products(
        ctx,
        summary,
        product_id=product_id,
        market=market,
        mixcut_category=mixcut_category,
        limit=limit,
    )
    products, skipped_products = filter_products_by_form(products, product_form)
    insights = load_insights(ctx, batch_id, product_form=product_form)
    if not insights:
        return {
            "success": False,
            "mode": "apply",
            "batch": summary,
            "product_form_summary": form_summary,
            "quality_gate": gate,
            "skipped_products": skipped_products,
            "error": "no_insights_for_batch",
        }

    if write_recommendation:
        ensure_recommendation_table(ctx)

    factory = SegmentPromptFactorySkill(ctx) if prompt_smoke else None
    applied: List[Dict[str, Any]] = []
    writes: List[Dict[str, Any]] = []
    feishu_syncs: List[Dict[str, Any]] = []
    for product in products:
        recommendation = build_product_recommendation(product, insights, summary, allow_warning=allow_warning, product_form_summary=form_summary)
        if prompt_smoke and factory:
            recommendation["prompt_smoke"] = build_prompt_smoke(factory, product, recommendation)
        if write_recommendation:
            write = write_product_recommendation(ctx, recommendation)
            writes.append(write)
        if sync_feishu_task:
            sync = sync_product_task_to_feishu(ctx, recommendation)
            feishu_syncs.append(sync)
        applied.append(recommendation)

    return {
        "success": all(item.get("success", True) for item in [*writes, *feishu_syncs]) if (writes or feishu_syncs) else True,
        "mode": "apply",
        "dry_run": bool(dry_run),
        "write_recommendation": bool(write_recommendation),
        "sync_feishu_task": bool(sync_feishu_task),
        "prompt_smoke": bool(prompt_smoke),
        "batch": summary,
        "product_form_summary": form_summary,
        "quality_gate": gate,
        "product_count": len(applied),
        "skipped_products": skipped_products,
        "products": applied,
        "writes": writes,
        "feishu_syncs": feishu_syncs,
    }


def load_batch_summary(ctx: Any, batch_id: str) -> Dict[str, Any]:
    with ctx.repo.connect() as conn:
        cur = conn.cursor()
        batch = fetch_one(cur, f"SELECT * FROM {EXPORT_BATCH_TABLE} WHERE batch_id=%s", (batch_id,))
        if not batch:
            raise RuntimeError(f"batch not found: {batch_id}")
        pack = fetch_one(
            cur,
            f"SELECT * FROM {INSIGHT_PACK_TABLE} WHERE batch_id=%s ORDER BY generated_at DESC LIMIT 1",
            (batch_id,),
        )
        issues = fetch_all(
            cur,
            f"SELECT issue_id, severity, title, detail FROM {FLOW_ISSUE_TABLE} WHERE batch_id=%s",
            (batch_id,),
        )
        counts: Dict[str, int] = {}
        for table in [
            "fastmoss_voc_product_snapshot",
            "fastmoss_voc_raw",
            "fastmoss_voc_no_visible_product",
            "fastmoss_voc_failed_product",
            INSIGHT_TABLE,
            HOOK_TABLE,
            "fastmoss_voc_insight_evidence_map",
        ]:
            try:
                row = fetch_one(cur, f"SELECT COUNT(*) AS c FROM {table} WHERE batch_id=%s", (batch_id,))
                counts[table] = int(row["c"]) if row else 0
            except Exception:
                counts[table] = -1

    category_key = str(batch.get("category_key") or "")
    return {
        "batch_id": batch_id,
        "market": batch.get("market"),
        "category_key": category_key,
        "mixcut_category": CATEGORY_TO_MIXCUT.get(category_key, category_key),
        "quality_status": (pack or {}).get("source_quality_status") or batch.get("quality_status"),
        "batch": compact_row(batch),
        "insight_pack": compact_row(pack or {}),
        "counts": counts,
        "issues": sorted(issues, key=lambda x: severity_rank(x.get("severity"))),
        "high_issue_count": sum(1 for item in issues if item.get("severity") == "high"),
    }


def quality_gate(
    summary: Dict[str, Any],
    allow_warning: bool,
    product_form: str = "",
    form_summary: Dict[str, Any] | None = None,
    min_form_products: int = 5,
    min_form_voc: int = 20,
) -> Dict[str, Any]:
    high_issues = [item for item in summary.get("issues", []) if item.get("severity") == "high"]
    quality_status = str(summary.get("quality_status") or "").lower()
    passed = True
    reasons: List[str] = []
    if high_issues and not allow_warning:
        passed = False
        reasons.append("high_flow_issues_present")
    if quality_status not in {"", "pass", "passed", "ok", "warning"} and not allow_warning:
        passed = False
        reasons.append(f"quality_status={quality_status}")
    if quality_status == "warning" and not allow_warning:
        passed = False
        reasons.append("quality_status=warning")
    form_summary = form_summary or {}
    if product_form:
        if not form_summary:
            passed = False
            reasons.append(f"product_form_not_found={product_form}")
        else:
            product_count = int(form_summary.get("product_count") or 0)
            voc_count = int(form_summary.get("voc_count") or 0)
            if product_count < min_form_products or voc_count < min_form_voc:
                passed = False
                reasons.append(
                    f"product_form_low_confidence={product_form}:{product_count}products/{voc_count}voc"
                )
    return {
        "passed": passed,
        "allow_warning": bool(allow_warning),
        "quality_status": summary.get("quality_status"),
        "high_issue_count": len(high_issues),
        "product_form": product_form,
        "form_product_count": int((form_summary or {}).get("product_count") or 0),
        "form_voc_count": int((form_summary or {}).get("voc_count") or 0),
        "reasons": reasons,
    }


def load_form_summary(ctx: Any, batch_id: str, product_form: str) -> Dict[str, Any]:
    form = str(product_form or "").strip()
    if not form:
        return {}
    with ctx.repo.connect() as conn:
        cur = conn.cursor()
        try:
            row = fetch_one(
                cur,
                f"SELECT * FROM {FORM_SUMMARY_TABLE} WHERE batch_id=%s AND product_form=%s LIMIT 1",
                (batch_id, form),
            )
        except Exception:
            return {}
    if not row:
        return {}
    data = compact_row(row)
    for key in [
        "sentiment_counts_json",
        "pack_type_counts_json",
        "style_tag_counts_json",
        "top_signal_tags_json",
        "product_ids_json",
        "product_examples_json",
        "raw_summary_json",
    ]:
        data[key.replace("_json", "")] = parse_json(row.get(key), [] if key.endswith("ids_json") or key.endswith("tags_json") or key.endswith("examples_json") else {})
    return data


def load_target_products(
    ctx: Any,
    summary: Dict[str, Any],
    product_id: str,
    market: str,
    mixcut_category: str,
    limit: int,
) -> List[Dict[str, Any]]:
    target_market = market or str(summary.get("market") or "")
    target_category = mixcut_category or str(summary.get("mixcut_category") or "")
    with ctx.repo.connect() as conn:
        cur = conn.cursor()
        if product_id:
            rows = fetch_all(cur, "SELECT * FROM products WHERE product_id=%s LIMIT 1", (product_id,))
        else:
            rows = fetch_all(
                cur,
                """
                SELECT *
                FROM products
                WHERE market=%s AND category=%s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (target_market, target_category, max(1, int(limit or 1))),
            )
    if product_id and not rows:
        raise RuntimeError(f"product not found: {product_id}")
    return rows


def filter_products_by_form(products: List[Dict[str, Any]], product_form: str) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    form = str(product_form or "").strip()
    if not form:
        return products, []
    matched: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for product in products:
        anchor = parse_json(product.get("product_anchor_json"), {})
        inferred, reason = infer_product_form(product, anchor)
        product = dict(product)
        product["_inferred_product_form"] = inferred
        product["_inferred_product_form_reason"] = reason
        if inferred == form:
            product["_matched_product_form"] = form
            matched.append(product)
        else:
            skipped.append(
                {
                    "product_id": product.get("product_id"),
                    "product_name": product.get("product_name"),
                    "target_product_form": form,
                    "inferred_product_form": inferred,
                    "reason": reason,
                }
            )
    return matched, skipped


def load_insights(ctx: Any, batch_id: str, product_form: str = "") -> List[Dict[str, Any]]:
    with ctx.repo.connect() as conn:
        cur = conn.cursor()
        rows = fetch_all(
            cur,
            f"""
            SELECT
              i.insight_pack_id,
              i.batch_id,
              i.insight_id,
              i.insight_type,
              i.title,
              i.local_title,
              i.summary,
              i.confidence,
              i.evidence_count,
              i.product_count,
              i.evidence_refs_json,
              i.raw_insight_json,
              h.hook_id,
              h.target_language,
              h.hook_text,
              h.evidence_examples_json
            FROM {INSIGHT_TABLE} i
            LEFT JOIN {HOOK_TABLE} h
              ON h.batch_id=i.batch_id AND h.insight_id=i.insight_id
            WHERE i.batch_id=%s
            """,
            (batch_id,),
        )
    form_metrics = load_form_insight_metrics(ctx, batch_id, product_form) if product_form else {}
    scoped_rows: List[Dict[str, Any]] = []
    for row in rows:
        if product_form:
            metrics = form_metrics.get(str(row.get("insight_id") or ""))
            if not metrics:
                continue
            row["evidence_scope"] = "product_form"
            row["product_form"] = product_form
            row["global_evidence_count"] = row.get("evidence_count")
            row["global_product_count"] = row.get("product_count")
            row["evidence_count"] = metrics["evidence_count"]
            row["product_count"] = metrics["product_count"]
            row["evidence_refs_json"] = json.dumps(metrics["evidence_refs"], ensure_ascii=False)
            row["evidence_examples_json"] = json.dumps(metrics["evidence_examples"], ensure_ascii=False)
        else:
            row["evidence_scope"] = "batch"
        row["evidence_refs"] = parse_json(row.get("evidence_refs_json"), [])
        row["evidence_examples"] = parse_json(row.get("evidence_examples_json"), [])
        scoped_rows.append(row)
    return sorted(
        scoped_rows,
        key=lambda r: (CONFIDENCE_WEIGHT.get(str(r.get("confidence") or ""), 0.3), int(r.get("evidence_count") or 0)),
        reverse=True,
    )


def load_form_insight_metrics(ctx: Any, batch_id: str, product_form: str) -> Dict[str, Dict[str, Any]]:
    with ctx.repo.connect() as conn:
        cur = conn.cursor()
        try:
            rows = fetch_all(
                cur,
                f"""
                SELECT
                  m.insight_id,
                  m.evidence_id,
                  m.fastmoss_product_id,
                  m.voc_text
                FROM fastmoss_voc_insight_evidence_map m
                JOIN {ENRICHED_TABLE} e
                  ON e.batch_id=m.batch_id
                 AND e.evidence_id=m.evidence_id
                WHERE m.batch_id=%s AND e.product_form=%s
                """,
                (batch_id, product_form),
            )
        except Exception:
            return {}
    metrics: Dict[str, Dict[str, Any]] = {}
    product_sets: Dict[str, set[str]] = {}
    for row in rows:
        insight_id = str(row.get("insight_id") or "")
        if not insight_id:
            continue
        item = metrics.setdefault(insight_id, {"evidence_count": 0, "product_count": 0, "evidence_refs": [], "evidence_examples": []})
        product_sets.setdefault(insight_id, set()).add(str(row.get("fastmoss_product_id") or ""))
        item["evidence_count"] += 1
        if row.get("evidence_id") and len(item["evidence_refs"]) < 20:
            item["evidence_refs"].append(row["evidence_id"])
        if row.get("voc_text") and len(item["evidence_examples"]) < 5:
            item["evidence_examples"].append(row["voc_text"])
    for insight_id, item in metrics.items():
        item["product_count"] = len(product_sets.get(insight_id) or set())
    return metrics


def build_product_recommendation(
    product: Dict[str, Any],
    insights: List[Dict[str, Any]],
    summary: Dict[str, Any],
    allow_warning: bool,
    product_form_summary: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    product_form_summary = product_form_summary or {}
    anchor = parse_json(product.get("product_anchor_json"), {})
    anchor_selling_points = extract_anchor_selling_points(anchor)
    flags = product_evidence_flags(product, anchor)
    target_form = str(product.get("_matched_product_form") or product_form_summary.get("product_form") or "")
    target_form_label = str(product_form_summary.get("product_form_label") or FORM_LABELS.get(target_form, target_form))
    form_confidence = form_confidence_label(product_form_summary)
    primary: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    risk_guards: List[Dict[str, Any]] = []
    auxiliary: List[Dict[str, Any]] = []

    for insight in insights:
        decision = decide_insight(insight, flags, anchor_selling_points, product_form=target_form)
        item = {
            "insight_id": insight.get("insight_id"),
            "insight_role": "product_core_selling_point"
            if insight.get("insight_id") in CORE_INSIGHT_IDS
            else "offer_selling_point"
            if insight.get("insight_id") == "selling_value_quantity"
            else "fulfillment_trust"
            if insight.get("insight_id") == "selling_fast_shipping"
            else "",
            "insight_type": insight.get("insight_type"),
            "title": insight.get("title"),
            "local_title": insight.get("local_title"),
            "summary": insight.get("summary"),
            "confidence": insight.get("confidence"),
            "evidence_count": insight.get("evidence_count"),
            "product_count": insight.get("product_count"),
            "evidence_scope": insight.get("evidence_scope"),
            "product_form": insight.get("product_form") or target_form,
            "global_evidence_count": insight.get("global_evidence_count"),
            "global_product_count": insight.get("global_product_count"),
            "hook_id": insight.get("hook_id"),
            "target_language": insight.get("target_language"),
            "source_hook_text": insight.get("hook_text"),
            "evidence_refs": insight.get("evidence_refs") or [],
            "evidence_examples": (insight.get("evidence_examples") or [])[:3],
            **decision,
        }
        if decision["decision"] in {"primary", "secondary"}:
            primary.append(item)
        elif decision["decision"] == "risk_guard":
            risk_guards.append(item)
        elif decision["decision"] == "auxiliary_only":
            auxiliary.append(item)
        else:
            skipped.append(item)

    form_suffix = f"__{target_form}" if target_form else ""
    recommendation_id = f"{summary['batch_id']}__{product['product_id']}{form_suffix}__ads_voc_reco_v0"
    lines = [
        f"{item.get('visual_proof_zh') or item['selling_point']} x{item['suggested_count']} | {item['hook_intent']} | VOC: {item['title']}"
        for item in primary
        if int(item.get("suggested_count") or 0) > 0
    ]
    return {
        "recommendation_id": recommendation_id,
        "product_id": product.get("product_id"),
        "product_name": product.get("product_name"),
        "market": product.get("market"),
        "category": product.get("category"),
        "batch_id": summary.get("batch_id"),
        "insight_pack_id": (summary.get("insight_pack") or {}).get("insight_pack_id"),
        "voc_category_key": summary.get("category_key"),
        "mixcut_category": summary.get("mixcut_category"),
        "product_form": target_form,
        "product_form_label": target_form_label,
        "product_form_summary": product_form_summary,
        "form_confidence": form_confidence,
        "inferred_product_form": product.get("_inferred_product_form") or "",
        "inferred_product_form_reason": product.get("_inferred_product_form_reason") or "",
        "quality_status": summary.get("quality_status"),
        "recommendation_status": "form_candidate" if target_form and not allow_warning else "form_smoke_candidate" if target_form else "smoke_candidate" if allow_warning else "candidate",
        "product_evidence_flags": flags,
        "primary_selling_points": primary,
        "auxiliary_points": auxiliary,
        "risk_guards": risk_guards,
        "skipped_insights": skipped,
        "recommendation_text": "\n".join(lines),
        "generated_at": datetime.utcnow().isoformat(timespec="seconds"),
    }


def decide_insight(
    insight: Dict[str, Any],
    flags: Dict[str, bool],
    anchor_selling_points: List[str],
    product_form: str = "",
) -> Dict[str, Any]:
    insight_id = str(insight.get("insight_id") or "")
    insight_type = str(insight.get("insight_type") or "")
    title = str(insight.get("title") or "")
    hook_intent = HOOK_INTENTS.get(insight_id, "product_clarity")
    visual_fields = visual_proof_fields(insight_id)

    if insight_type == "pain_point":
        return {
            "decision": "risk_guard",
            "suggested_count": 0,
            "hook_intent": hook_intent,
            "selling_point": "",
            "reason": "pain insight should be used as risk/negative guard, not as selling point",
            **visual_fields,
        }

    if insight_id == "selling_appearance_cute_color":
        if not flags.get("decorative_visual"):
            return skip_decision(hook_intent, "product lacks clear decorative/appearance evidence")
        return {
            "decision": "primary",
            "suggested_count": 4,
            "hook_intent": hook_intent,
            "selling_point": match_anchor_selling_point(anchor_selling_points, ["装饰", "焦点", "精致", "好看", "颜色", "造型", "珍珠", "亮钻"], form_default_selling_point(insight_id, product_form, title)),
            "reason": "VOC appearance signal matches visible product styling evidence",
            **visual_fields,
        }

    if insight_id == "selling_hold_quality":
        if not flags.get("hold_stability"):
            return skip_decision(hook_intent, "product lacks hold/fixation structure evidence")
        return {
            "decision": "secondary",
            "suggested_count": 2,
            "hook_intent": hook_intent,
            "selling_point": match_anchor_selling_point(anchor_selling_points, ["固定", "稳", "夹", "发髻", "完整", "利落", "抓夹"], form_default_selling_point(insight_id, product_form, title)),
            "reason": "VOC hold-quality signal matches hair-clip/claw structure evidence",
            **visual_fields,
        }

    if insight_id == "selling_value_quantity":
        if not flags.get("set_quantity"):
            return skip_decision(hook_intent, "quantity/value VOC is only valid for set or multi-pack products")
        return {
            "decision": "auxiliary_only",
            "suggested_count": 0,
            "hook_intent": hook_intent,
            "selling_point": "组合装数量感和性价比（仅作 offer 辅助信息）",
            "reason": "offer/value signal is not a standalone ADS product hook",
            **visual_fields,
        }

    if insight_id == "selling_fast_shipping":
        return {
            "decision": "auxiliary_only",
            "suggested_count": 0,
            "hook_intent": hook_intent,
            "selling_point": "发货快作为辅助信任点",
            "reason": "logistics signal is not a product-visual hook",
            **visual_fields,
        }

    if insight_type == "selling_point" and int(insight.get("evidence_count") or 0) >= 3:
        return {
            "decision": "secondary",
            "suggested_count": 1,
            "hook_intent": hook_intent,
            "selling_point": title,
            "reason": "generic selling insight with enough evidence",
            "usage_lane": "video_support",
            "video_fit_score": 55,
            "visual_proof_zh": "仅作为辅助观察，需人工补充具体可拍动作后再进入视频钩子",
            "required_action_zh": "",
            "proof_shot_list": [],
            "forbidden_claims": ["不要使用没有镜头证据的功能承诺"],
        }

    return skip_decision(hook_intent, "low evidence or unsupported insight")


def skip_decision(hook_intent: str, reason: str) -> Dict[str, Any]:
    return {
        "decision": "skip",
        "suggested_count": 0,
        "hook_intent": hook_intent,
        "selling_point": "",
        "reason": reason,
    }


def visual_proof_fields(insight_id: str) -> Dict[str, Any]:
    taxonomy = _load_taxonomy()
    signal_map = taxonomy.get("signal_map", {})
    # find by insight_id
    for tag, entry in signal_map.items():
        if entry.get("insight_id") == insight_id or ("selling_" + tag) == insight_id:
            return dict(entry)
    # legacy fallback
    legacy = LEGACY_FALLBACK.get(insight_id, {})
    if legacy:
        return dict(legacy)
    return {
        "proof_archetype": "scenario_use_proof",
        "usage_lane": "video_support",
        "video_fit_score": 55,
        "visual_goal_zh": "仅作为辅助观察，需人工补充具体可拍动作后再进入视频钩子",
        "visual_proof_zh": "仅作为辅助观察，需人工补充具体可拍动作后再进入视频钩子",
        "required_action_zh": "",
        "proof_shot_list": [],
        "forbidden_claims": ["不要使用没有镜头证据的功能承诺"],
    }


def build_prompt_smoke(factory: SegmentPromptFactorySkill, product: Dict[str, Any], recommendation: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates = [
        item
        for item in recommendation.get("primary_selling_points", [])
        if int(item.get("suggested_count") or 0) > 0
    ]
    if not candidates:
        return []
    anchor = parse_json(product.get("product_anchor_json"), {})
    proof_points = [item.get("visual_proof_zh") or item.get("selling_point") or "" for item in candidates]
    proof_actions = []
    for item in candidates:
        if item.get("required_action_zh"):
            proof_actions.append(f"VOC证明动作：{item.get('required_action_zh')}")
        for shot in item.get("proof_shot_list") or []:
            proof_actions.append(f"VOC证明镜头：{shot}")
    brief = {
        "product_id": product.get("product_id"),
        "category": product.get("category"),
        "product_subtype": product.get("product_name"),
        "primary_visual_result": "；".join(p for p in proof_points[:2] if p),
        "must_show": proof_actions[:6] + [f"VOC来源卖点：{item.get('selling_point')}" for item in candidates if item.get("selling_point")] + anchor_texts(anchor.get("display_anchors"), ("anchor", "constraint", "selling_point"))[:3],
        "must_not_show": anchor_texts(anchor.get("distortion_alerts"), ("anchor", "constraint", "text"))[:5],
        "hard_anchors": anchor_texts(anchor.get("hard_anchors"), ("anchor", "constraint", "text"))[:6],
        "display_anchors": anchor_texts(anchor.get("display_anchors"), ("anchor", "constraint", "text"))[:4],
        "key_visual_constraints": anchor_texts(anchor.get("key_visual_constraints"), ("constraint", "anchor", "text"))[:5],
        "forbidden_actions": anchor_texts(anchor.get("distortion_alerts"), ("anchor", "constraint", "text"))[:5],
        "safe_micro_actions": [a.replace("VOC证明动作：", "").replace("VOC证明镜头：", "") for a in proof_actions[:6]]
        or ["后脑佩戴结果近景", "手部轻夹入发髻区域", "同角度前后对比"],
    }
    results: List[Dict[str, Any]] = []
    for index, item in enumerate(candidates):
        hook_intent = str(item.get("hook_intent") or "product_clarity")
        slot = {
            "template_id": "VOC_ADS_HOOK_SMOKE",
            "slot_index": index,
            "slot_role": "hero",
            "segment_type": SEGMENT_TYPE_BY_HOOK_INTENT.get(hook_intent, "product_display"),
            "prompt_grade": "A",
            "is_hook": True,
            "hook_intent": hook_intent,
        }
        res = factory.build_package(brief, slot, persist=False)
        if res.success:
            package = res.data
            prompt = package.get("prompt") or {}
            results.append(
                {
                    "success": True,
                    "selling_point": item.get("selling_point"),
                    "hook_intent": hook_intent,
                    "segment_prompt_id": package.get("segment_prompt_id"),
                    "positive_head": str(prompt.get("positive") or "")[:360],
                    "motion_arc": prompt.get("motion_arc"),
                }
            )
        else:
            results.append(
                {
                    "success": False,
                    "selling_point": item.get("selling_point"),
                    "hook_intent": hook_intent,
                    "error": res.to_dict(),
                }
            )
    return results


def write_product_recommendation(ctx: Any, recommendation: Dict[str, Any]) -> Dict[str, Any]:
    now = datetime.utcnow().isoformat(timespec="seconds")
    row = {
        "recommendation_id": recommendation["recommendation_id"],
        "batch_id": recommendation["batch_id"],
        "insight_pack_id": recommendation.get("insight_pack_id") or "",
        "product_id": recommendation["product_id"],
        "market": recommendation.get("market") or "",
        "category_key": recommendation.get("voc_category_key") or "",
        "mixcut_category": recommendation.get("mixcut_category") or "",
        "quality_status": recommendation.get("quality_status") or "",
        "recommendation_status": recommendation.get("recommendation_status") or "candidate",
        "primary_selling_points_json": json.dumps(recommendation.get("primary_selling_points") or [], ensure_ascii=False),
        "risk_guards_json": json.dumps(recommendation.get("risk_guards") or [], ensure_ascii=False),
        "skipped_insights_json": json.dumps(recommendation.get("skipped_insights") or [], ensure_ascii=False),
        "source_payload_json": json.dumps(recommendation, ensure_ascii=False, default=str),
        "created_at": now,
        "updated_at": now,
    }
    res = ctx.repo.upsert(RECOMMENDATION_TABLE, "recommendation_id", row)
    return {"success": res.success, "recommendation_id": row["recommendation_id"], "result": res.to_dict() if not res.success else {"written": True}}


def sync_product_task_to_feishu(ctx: Any, recommendation: Dict[str, Any]) -> Dict[str, Any]:
    product_id = str(recommendation.get("product_id") or "")
    task = latest_task_with_feishu_record(ctx, product_id)
    if not task or not task.get("feishu_record_id"):
        return {"success": False, "product_id": product_id, "reason": "missing_product_task_feishu_record"}
    client = AutoMixcutFeishuClient("商品内容任务表")
    ensure_task_voc_fields(client)
    fields = feishu_task_fields(recommendation)
    client.update_record(str(task["feishu_record_id"]), fields)
    try:
        ctx.repo.update(
            RECOMMENDATION_TABLE,
            "recommendation_id",
            recommendation["recommendation_id"],
            {
                "recommendation_status": "synced_to_task_smoke"
                if str(recommendation.get("recommendation_status") or "").endswith("smoke_candidate")
                else "synced_to_task",
            },
        )
    except Exception:
        pass
    return {
        "success": True,
        "product_id": product_id,
        "task_id": task.get("task_id"),
        "feishu_record_id": task.get("feishu_record_id"),
        "updated_fields": sorted(fields),
    }


def latest_task_with_feishu_record(ctx: Any, product_id: str) -> Dict[str, Any] | None:
    with ctx.repo.connect() as conn:
        cur = conn.cursor()
        row = fetch_one(
            cur,
            """
            SELECT
              t.task_id,
              t.product_id,
              t.task_status,
              t.created_at,
              fs.feishu_record_id
            FROM content_tasks t
            LEFT JOIN feishu_sync_records fs
              ON fs.object_type=%s
             AND fs.object_id=t.task_id
             AND fs.feishu_table=%s
             AND fs.sync_status=%s
            WHERE t.product_id=%s
            ORDER BY t.created_at DESC
            LIMIT 1
            """,
            ("product_task", "商品内容任务表", "synced", product_id),
        )
    return row


def ensure_task_voc_fields(client: AutoMixcutFeishuClient) -> None:
    existing = {field.field_name for field in client.client.list_fields()}
    for field_name in [
        "AI推荐卖点",
        "VOC来源批次",
        "VOC质量状态",
        "VOC推荐状态",
        "VOC商品形态",
        "VOC形态样本量",
        "VOC形态置信度",
        "VOC风险提示",
        "VOC推荐更新时间",
        "VOC人工确认状态",
        "VOC人工确认卖点",
        "VOC目标钩子数",
        "VOC钩子包状态",
        "VOC钩子包ID",
        "VOC钩子候选数",
        "VOC钩子包摘要",
        "VOC钩子包更新时间",
    ]:
        if field_name not in existing:
            client.client.create_field(field_name, field_type=1, ui_type="Text")
            existing.add(field_name)


def feishu_task_fields(recommendation: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "AI推荐卖点": recommendation.get("recommendation_text") or "",
        "VOC来源批次": str(recommendation.get("batch_id") or ""),
        "VOC质量状态": str(recommendation.get("quality_status") or ""),
        "VOC推荐状态": str(recommendation.get("recommendation_status") or ""),
        "VOC商品形态": format_product_form(recommendation),
        "VOC形态样本量": format_form_sample(recommendation),
        "VOC形态置信度": str(recommendation.get("form_confidence") or ""),
        "VOC风险提示": format_voc_risk_note(recommendation),
        "VOC推荐更新时间": datetime.utcnow().isoformat(timespec="seconds"),
    }


def format_voc_risk_note(recommendation: Dict[str, Any]) -> str:
    parts: List[str] = []
    for item in recommendation.get("risk_guards") or []:
        title = str(item.get("title") or "").strip()
        reason = str(item.get("reason") or "").strip()
        if title:
            parts.append(f"风险: {title}" + (f"；{reason}" if reason else ""))
    for item in recommendation.get("skipped_insights") or []:
        title = str(item.get("title") or "").strip()
        reason = str(item.get("reason") or "").strip()
        if title:
            parts.append(f"跳过: {title}" + (f"；{reason}" if reason else ""))
    if recommendation.get("quality_status") == "warning":
        parts.append("样本状态: warning，仅作烟测/待复核候选")
    if recommendation.get("product_form"):
        parts.append(
            f"形态范围: {format_product_form(recommendation)}；{format_form_sample(recommendation)}；{recommendation.get('form_confidence') or ''}"
        )
    return "\n".join(parts[:8])


def format_product_form(recommendation: Dict[str, Any]) -> str:
    form = str(recommendation.get("product_form") or "")
    label = str(recommendation.get("product_form_label") or FORM_LABELS.get(form, ""))
    if form and label and label != form:
        return f"{form} / {label}"
    return form or label


def format_form_sample(recommendation: Dict[str, Any]) -> str:
    summary = recommendation.get("product_form_summary") or {}
    if not summary:
        return ""
    return f"{int(summary.get('product_count') or 0)}商品 / {int(summary.get('voc_count') or 0)}VOC"


def ensure_recommendation_table(ctx: Any) -> None:
    if getattr(ctx.repo, "dialect", "sqlite") == "mysql":
        statement = f"""
        CREATE TABLE IF NOT EXISTS {RECOMMENDATION_TABLE} (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          recommendation_id VARCHAR(256) NOT NULL UNIQUE,
          batch_id VARCHAR(128) NOT NULL,
          insight_pack_id VARCHAR(256),
          product_id VARCHAR(128) NOT NULL,
          market VARCHAR(32),
          category_key VARCHAR(128),
          mixcut_category VARCHAR(128),
          quality_status VARCHAR(64),
          recommendation_status VARCHAR(64),
          primary_selling_points_json JSON,
          risk_guards_json JSON,
          skipped_insights_json JSON,
          source_payload_json JSON,
          created_at DATETIME,
          updated_at DATETIME,
          INDEX idx_fm_voc_reco_batch (batch_id),
          INDEX idx_fm_voc_reco_product (product_id),
          INDEX idx_fm_voc_reco_market_category (market, mixcut_category)
        )
        """
    else:
        statement = f"""
        CREATE TABLE IF NOT EXISTS {RECOMMENDATION_TABLE} (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          recommendation_id TEXT NOT NULL UNIQUE,
          batch_id TEXT NOT NULL,
          insight_pack_id TEXT,
          product_id TEXT NOT NULL,
          market TEXT,
          category_key TEXT,
          mixcut_category TEXT,
          quality_status TEXT,
          recommendation_status TEXT,
          primary_selling_points_json TEXT,
          risk_guards_json TEXT,
          skipped_insights_json TEXT,
          source_payload_json TEXT,
          created_at TEXT,
          updated_at TEXT
        )
        """
    with ctx.repo.connect() as conn:
        cur = conn.cursor()
        cur.execute(statement)


def product_evidence_flags(product: Dict[str, Any], anchor: Dict[str, Any]) -> Dict[str, bool]:
    # Only use product-visible text values. JSON field names such as "set_relationship"
    # are metadata and must not trigger product evidence.
    haystack = " ".join([str(product.get("product_name") or ""), *flatten_text_values(anchor)]).lower()
    quantity_unit = bool(re.search(r"\b\d{1,3}\s*(pcs|ชิ้น|件|枚|个|入)\b", haystack))
    set_words = any_token(haystack, ["组合装", "套装", "多件", "多只", "multi-pack", "combo", "คละ", "หลายชิ้น", "เซ็ต"])
    return {
        "decorative_visual": any_token(haystack, ["装饰", "颜色", "造型", "精致", "珍珠", "亮钻", "蝴蝶结", "น่ารัก", "สวย", "สง่างาม"]),
        "hold_stability": any_token(haystack, ["抓夹", "固定", "夹住", "夹爪", "弹簧", "发髻", "后脑", "齿", "หนีบ", "กิ๊บ"]),
        "set_quantity": bool(quantity_unit or set_words),
    }


def infer_product_form(product: Dict[str, Any], anchor: Dict[str, Any]) -> tuple[str, str]:
    subtype = str(
        anchor.get("hair_accessory_subtype")
        or (anchor.get("category_execution_contract") or {}).get("product_subtype")
        or ""
    ).strip()
    if subtype in FORM_LABELS:
        return subtype, "anchor_subtype"
    haystack = " ".join([str(product.get("product_name") or ""), *flatten_text_values(anchor)]).lower()
    checks = [
        ("bow_clip", ["โบว์", "bow", "蝴蝶结", "蝴蝶", "ริบบิ้น"]),
        ("duckbill_clip", ["ปากเป็ด", "duckbill", "鸭嘴"]),
        ("claw_clip", ["ฉลาม", "กิ๊บฉลาม", "claw", "抓夹", "鲨鱼夹"]),
        ("floral_clip", ["ดอกไม้", "flower", "花朵", "กุหลาบ"]),
        ("hair_pad_or_sticker", ["แผ่นแปะผม", "碎发贴", "发帖片", "hair sticker"]),
        ("hair_comb_or_volumizer", ["volum", "蓬松", "หวี", "发梳", "รากผม"]),
        ("assorted_clip_set", ["เซ็ต", "คละ", "หลายชิ้น", "组合装", "套装", "多件"]),
        ("pearl_rhinestone_clip", ["มุก", "pearl", "珍珠", "水钻", "亮钻", "ไข่มุก"]),
        ("character_clip", ["sanrio", "การ์ตูน", "卡通", "角色"]),
    ]
    for form, tokens in checks:
        if any_token(haystack, tokens):
            return form, f"keyword:{form}"
    if any_token(haystack, ["กิ๊บ", "clip", "发夹", "发卡", "หนีบผม"]):
        return "basic_hair_clip", "generic_hair_clip_keyword"
    return "unknown_hair_accessory", "no_specific_form_signal"


def form_confidence_label(form_summary: Dict[str, Any]) -> str:
    if not form_summary:
        return ""
    products = int(form_summary.get("product_count") or 0)
    voc = int(form_summary.get("voc_count") or 0)
    if products >= 5 and voc >= 20:
        return "form_candidate"
    if products >= 3 and voc >= 10:
        return "low_confidence_form"
    return "observe_only"


def form_default_selling_point(insight_id: str, product_form: str, fallback: str) -> str:
    form = str(product_form or "")
    defaults = {
        "selling_appearance_cute_color": {
            "bow_clip": "蝴蝶结造型上头后有明显装饰焦点",
            "basic_hair_clip": "款式百搭，颜色/造型容易被认可",
            "duckbill_clip": "侧夹点缀后脸侧发型更有细节",
            "floral_clip": "花朵造型让发型更有甜美焦点",
            "pearl_rhinestone_clip": "珍珠/亮钻细节带来精致装饰感",
            "claw_clip": "抓夹上头后后脑造型更有装饰重点",
        },
        "selling_hold_quality": {
            "bow_clip": "夹上后发型更完整，质感/固定反馈不错",
            "basic_hair_clip": "夹住碎发或局部头发后更利落",
            "duckbill_clip": "侧边夹住小束头发，脸侧更干净",
            "floral_clip": "夹住局部头发同时增加花朵装饰感",
            "pearl_rhinestone_clip": "夹住局部头发同时保留精致细节",
            "claw_clip": "抓夹结构适合固定发髻区域头发",
        },
    }
    return defaults.get(insight_id, {}).get(form) or fallback


def extract_anchor_selling_points(anchor: Dict[str, Any]) -> List[str]:
    result: List[str] = []
    for item in anchor.get("candidate_primary_selling_points") or []:
        if isinstance(item, dict) and item.get("selling_point"):
            result.append(str(item["selling_point"]).strip())
    return dedupe(result)


def match_anchor_selling_point(anchor_selling_points: List[str], keywords: Iterable[str], fallback: str) -> str:
    for point in anchor_selling_points:
        if any(keyword in point for keyword in keywords):
            return point
    return anchor_selling_points[0] if anchor_selling_points else fallback


def anchor_texts(value: Any, keys: Iterable[str]) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            out.extend(anchor_texts(item, keys))
        return dedupe(out)
    if isinstance(value, dict):
        for key in keys:
            text = value.get(key)
            if text:
                return [str(text).strip()]
        return []
    text = str(value).strip()
    return [text] if text else []


def flatten_text_values(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, list):
        result: List[str] = []
        for item in value:
            result.extend(flatten_text_values(item))
        return result
    if isinstance(value, dict):
        result: List[str] = []
        for item in value.values():
            result.extend(flatten_text_values(item))
        return result
    if isinstance(value, (int, float)):
        return [str(value)]
    return []


def fetch_one(cur: Any, sql: str, params: Iterable[Any] = ()) -> Dict[str, Any] | None:
    cur.execute(sql, tuple(params))
    return cur.fetchone()


def fetch_all(cur: Any, sql: str, params: Iterable[Any] = ()) -> List[Dict[str, Any]]:
    cur.execute(sql, tuple(params))
    return list(cur.fetchall())


def parse_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return default


def compact_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: value
        for key, value in dict(row or {}).items()
        if key not in {"manifest_json", "run_report_json", "raw_pack_json", "quality_notes_json"}
    }


def severity_rank(severity: Any) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(str(severity or ""), 9)


def any_token(text: str, tokens: Iterable[str]) -> bool:
    return any(token.lower() in text for token in tokens)


def dedupe(values: Iterable[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
