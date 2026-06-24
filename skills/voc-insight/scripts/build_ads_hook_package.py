#!/usr/bin/env python3
"""Build product-scoped ADS hook candidates from VOC insight artifacts.

This script does not generate final mixcut videos. It merges:
- product-level VOC recommendations,
- latest category-level polished hook references,
- optional local product anchor cards,
- optional manual confirmation flags.

The output is a conservative candidate package. Without manual confirmation it is
marked needs_manual_confirmation unless --allow-unconfirmed-smoke is explicit.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

try:
    import pymysql
except ImportError:
    pymysql = None  # type: ignore


USECASE_ADS = "ads_mixcut"
CORE_ROLE = "product_core_selling_point"
CORE_INSIGHT_IDS = {"selling_appearance_cute_color", "selling_hold_quality"}
NON_CORE_IDS = {"selling_fast_shipping", "selling_value_quantity", "pain_slow_shipping", "pain_fulfillment_missing"}
VIDEO_USAGE_LANES = {"video_hook", "video_support"}
MIN_ADS_VIDEO_FIT_SCORE = 70
HOOK_INTENTS = {
    "selling_appearance_cute_color": "tryon_result",
    "selling_hold_quality": "contrast_reveal",
}

# taxonomy + adapter paths
_REFS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "references")

def load_video_proof_taxonomy() -> Dict[str, Any]:
    path = os.path.join(_REFS_DIR, "video_proof_taxonomy.json")
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def load_category_proof_adapters() -> Dict[str, Any]:
    path = os.path.join(_REFS_DIR, "category_proof_adapters.json")
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"adapters": []}

CATEGORY_TO_ADAPTER_KEY = {
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
    "bags": "bags",
}

def select_category_adapter(point: Dict[str, Any], anchor: Dict[str, Any],
                            product_form: str, mixcut_category: str,
                            category_key: str) -> Optional[Dict[str, Any]]:
    adapters_data = load_category_proof_adapters()
    adapters = adapters_data.get("adapters") or []
    archetype = point.get("proof_archetype") or ""
    if not archetype:
        return None
    raw_cat = (category_key or "").lower()
    adapter_cat = CATEGORY_TO_ADAPTER_KEY.get(raw_cat, raw_cat)
    candidates = []
    for a in adapters:
        if a.get("proof_archetype") != archetype:
            continue
        a_cat = (a.get("category") or "").lower()
        if a_cat == "*":
            score = 1
        elif a_cat == adapter_cat:
            score = 100
        else:
            continue
        candidates.append((score + (a.get("priority") or 0), a))
    candidates.sort(key=lambda x: -x[0])
    return candidates[0][1] if candidates else None

def compile_visual_proof(point: Dict[str, Any], adapter: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    result = {
        "proof_archetype": point.get("proof_archetype") or "",
        "voc_signal": point.get("voc_signal") or "",
        "usage_lane": point.get("usage_lane") or "video_support",
        "video_fit_score": int(point.get("video_fit_score") or 0),
        "required_beats": point.get("required_beats") or [],
        "visual_goal_zh": point.get("visual_goal_zh") or point.get("visual_proof_zh") or "",
        "forbidden_claims": point.get("forbidden_claims") or [],
        "copy_lane_reason": point.get("copy_lane_reason") or "",
        "visual_proof_zh": point.get("visual_goal_zh") or point.get("visual_proof_zh") or "",
        "required_action_zh": point.get("required_action_zh") or "",
        "proof_shot_list": point.get("proof_shot_list") or [],
    }
    if adapter:
        result["category_adapter"] = adapter.get("adapter_id") or ""
        result["required_action_zh"] = adapter.get("required_action_zh") or result["required_action_zh"]
        result["shot_plan"] = adapter.get("shot_plan") or []
        result["safe_micro_actions"] = adapter.get("safe_micro_actions") or []
        result["negative_constraints"] = adapter.get("negative_constraints") or []
        result["allowed_segment_types"] = adapter.get("allowed_segment_types") or []
    return result

# backward-compatible fallback loader (kept for old data without taxonomy fields)
def _legacy_fallback_proof(insight_id: str) -> Dict[str, Any]:
    taxonomy = load_video_proof_taxonomy()
    signal_map = taxonomy.get("signal_map", {})
    for tag, entry in signal_map.items():
        if entry.get("insight_id") == insight_id or ("selling_" + tag) == insight_id:
            return dict(entry)
    # old hardcoded fallback by insight_id pattern
    if insight_id == "selling_appearance_cute_color":
        return {"proof_archetype": "appearance_transform", "usage_lane": "video_hook", "video_fit_score": 85}
    elif insight_id == "selling_hold_quality":
        return {"proof_archetype": "state_stability_proof", "usage_lane": "video_hook", "video_fit_score": 80}
    elif insight_id in ("selling_fast_shipping", "selling_value_quantity"):
        return {"proof_archetype": "copy_only", "usage_lane": "copy_only", "video_fit_score": insight_id == "selling_value_quantity" and 30 or 20}
    return {"proof_archetype": "scenario_use_proof", "usage_lane": "video_support", "video_fit_score": 55}

PACKAGE_DDL = """CREATE TABLE IF NOT EXISTS voc_ads_hook_package (
  package_id VARCHAR(191) NOT NULL PRIMARY KEY,
  batch_id VARCHAR(191) NOT NULL,
  run_id VARCHAR(191) NULL,
  product_id VARCHAR(191) NOT NULL,
  product_form VARCHAR(191) NULL,
  usecase VARCHAR(64) NOT NULL,
  readiness_status VARCHAR(64) NOT NULL,
  manual_confirmation_status VARCHAR(64) NOT NULL,
  hook_candidate_count INT DEFAULT 0,
  requested_hook_count INT DEFAULT 0,
  payload_json JSON,
  created_at VARCHAR(64) NOT NULL,
  updated_at VARCHAR(64) NOT NULL,
  UNIQUE KEY uq_voc_ads_hook_package (batch_id, run_id, product_id, usecase),
  INDEX idx_voc_ads_hook_package_ready (batch_id, usecase, readiness_status)
) CHARACTER SET utf8mb4"""

TASK_CONFIRM_STATUS_FIELD = "VOC人工确认状态"
TASK_CONFIRM_POINTS_FIELD = "VOC人工确认卖点"
TASK_TARGET_COUNT_FIELD = "VOC目标钩子数"
TASK_PACKAGE_STATUS_FIELD = "VOC钩子包状态"
TASK_PACKAGE_ID_FIELD = "VOC钩子包ID"
TASK_PACKAGE_COUNT_FIELD = "VOC钩子候选数"
TASK_PACKAGE_SUMMARY_FIELD = "VOC钩子包摘要"
TASK_PACKAGE_UPDATED_FIELD = "VOC钩子包更新时间"
TASK_CONFIRMATION_FIELDS = [
    TASK_CONFIRM_STATUS_FIELD,
    TASK_CONFIRM_POINTS_FIELD,
    TASK_TARGET_COUNT_FIELD,
    TASK_PACKAGE_STATUS_FIELD,
    TASK_PACKAGE_ID_FIELD,
    TASK_PACKAGE_COUNT_FIELD,
    TASK_PACKAGE_SUMMARY_FIELD,
    TASK_PACKAGE_UPDATED_FIELD,
]
CONFIRMED_STATUS_VALUES = {"已确认", "确认", "通过", "approved", "confirmed", "yes", "true", "1"}
REJECTED_STATUS_VALUES = {"驳回", "不通过", "rejected", "reject", "no", "false", "0"}


def connect_db(url: Optional[str] = None):
    if pymysql is None:
        raise RuntimeError("pymysql not installed. Run: python3 -m pip install --user pymysql")
    url = (
        url
        or os.environ.get("VOC_INSIGHT_DATABASE_URL")
        or os.environ.get("LIKEU_AI_DATABASE_URL")
        or os.environ.get("HERMES_AGENT_DATABASE_URL")
        or ""
    )
    if not url:
        raise RuntimeError("No database URL. Set LIKEU_AI_DATABASE_URL or pass --database-url.")
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    return pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        charset=query.get("charset", ["utf8mb4"])[0],
        connect_timeout=20,
        read_timeout=60,
        write_timeout=60,
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def jload(value: Any, default: Any = None) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def dedupe(values: Iterable[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def cell_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ("text", "name", "link", "url"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
        return ""
    if isinstance(value, list):
        return " / ".join(item for item in (cell_text(v) for v in value) if item)
    return str(value).strip()


def normalize_count(value: Any) -> int:
    text = cell_text(value)
    if not text:
        return 0
    match = re.search(r"\d+", text)
    if not match:
        return 0
    return max(0, min(int(match.group(0)), 20))


def split_manual_values(values: Iterable[Any]) -> List[str]:
    parts: List[str] = []
    for value in values:
        text = cell_text(value)
        if not text:
            continue
        for part in re.split(r"[\n,，;；]+", text):
            cleaned = part.strip(" \t-:：")
            if cleaned:
                parts.append(cleaned)
    return dedupe(parts)


def extract_inline_count(text: str) -> Tuple[str, int]:
    raw = str(text or "").strip()
    count = 0
    patterns = [
        r"(?i)(?:x|×|\*)\s*(\d{1,2})",
        r"(\d{1,2})\s*(?:条|个|支|段|hooks?|pcs?)",
    ]
    cleaned = raw
    for pattern in patterns:
        match = re.search(pattern, cleaned)
        if match:
            count = max(count, max(0, min(int(match.group(1)), 20)))
            cleaned = re.sub(pattern, "", cleaned).strip(" \t-:：")
    return cleaned, count


def parse_confirmed_points(values: Iterable[Any]) -> Tuple[List[str], List[str], Dict[str, int]]:
    ids: List[str] = []
    texts: List[str] = []
    counts: Dict[str, int] = {}
    for part in split_manual_values(values):
        cleaned, count = extract_inline_count(part)
        matched_id = ""
        for insight_id in sorted(CORE_INSIGHT_IDS):
            if insight_id in cleaned:
                matched_id = insight_id
                ids.append(insight_id)
                if count:
                    counts[insight_id] = count
                cleaned = cleaned.replace(insight_id, "").strip(" \t-:：")
                break
        if cleaned:
            texts.append(cleaned)
            if count:
                counts[cleaned] = count
        elif count and matched_id:
            counts[matched_id] = count
    return dedupe(ids), dedupe(texts), counts


def confirmation_status_value(status: str) -> str:
    normalized = str(status or "").strip()
    lowered = normalized.lower()
    if normalized in CONFIRMED_STATUS_VALUES or lowered in CONFIRMED_STATUS_VALUES:
        return "confirmed"
    if normalized in REJECTED_STATUS_VALUES or lowered in REJECTED_STATUS_VALUES:
        return "rejected"
    return "pending"


def build_cli_confirmation(args: argparse.Namespace) -> Dict[str, Any]:
    ids, texts, counts = parse_confirmed_points([*(args.confirmed_insight_id or []), *(args.confirmed_selling_point or [])])
    target_count = int(args.target_hook_count or 0)
    return {
        "source": "cli" if (ids or texts or target_count) else "none",
        "raw_status": "confirmed" if (ids or texts) else "",
        "status": "confirmed" if (ids or texts) else "pending",
        "confirmed_insight_ids": ids,
        "confirmed_texts": texts,
        "counts_by_key": counts,
        "target_hook_count": max(0, min(target_count, 20)),
    }


def merge_confirmation(feishu: Dict[str, Any], cli: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(feishu or {})
    if not result:
        result = {
            "source": "none",
            "raw_status": "",
            "status": "pending",
            "confirmed_insight_ids": [],
            "confirmed_texts": [],
            "counts_by_key": {},
            "target_hook_count": 0,
        }
    if cli.get("source") != "none":
        result["source"] = "cli+{}".format(result.get("source") or "none") if result.get("source") not in {"", "none"} else "cli"
        if cli.get("status") == "confirmed":
            result["status"] = "confirmed"
            result["raw_status"] = result.get("raw_status") or "confirmed_by_cli"
        result["confirmed_insight_ids"] = dedupe([*(result.get("confirmed_insight_ids") or []), *(cli.get("confirmed_insight_ids") or [])])
        result["confirmed_texts"] = dedupe([*(result.get("confirmed_texts") or []), *(cli.get("confirmed_texts") or [])])
        counts = dict(result.get("counts_by_key") or {})
        counts.update(cli.get("counts_by_key") or {})
        result["counts_by_key"] = counts
        if cli.get("target_hook_count"):
            result["target_hook_count"] = cli["target_hook_count"]
    return result


def short_texts(value: Any, keys: Tuple[str, ...] = ("selling_point", "anchor", "constraint", "text")) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            out.extend(short_texts(item, keys))
        return dedupe(out)
    if isinstance(value, dict):
        for key in keys:
            text = str(value.get(key) or "").strip()
            if text:
                return [text]
    return []


def latest_category_run(conn, batch_id: str) -> Optional[str]:
    cur = conn.cursor()
    cur.execute(
        "SELECT a.run_id FROM voc_category_insight_artifact a "
        "JOIN voc_insight_run r ON r.run_id = a.run_id "
        "WHERE a.batch_id=%s AND r.scope='category' "
        "ORDER BY a.updated_at DESC LIMIT 1",
        (batch_id,),
    )
    row = cur.fetchone()
    return str(row["run_id"]) if row and row.get("run_id") else None


def load_category_reference_hooks(conn, batch_id: str, run_id: str, usecase: str) -> Dict[str, Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT polish_id, insight_id, insight_role, confidence, title_zh_polished,
               local_voice_polished, hooks_json, reason_zh, evidence_refs_json
        FROM voc_insight_polish
        WHERE batch_id=%s AND run_id=%s AND usecase=%s
          AND polish_status='completed'
          AND claim_validation_status='passed'
          AND hook_eligible=1
        ORDER BY updated_at DESC
        """,
        (batch_id, run_id, usecase),
    )
    refs: Dict[str, Dict[str, Any]] = {}
    for row in cur.fetchall():
        hooks = jload(row.get("hooks_json"), []) or []
        refs[str(row.get("insight_id") or "")] = {
            "polish_id": row.get("polish_id"),
            "insight_id": row.get("insight_id"),
            "insight_role": row.get("insight_role"),
            "confidence": row.get("confidence"),
            "title_zh_polished": row.get("title_zh_polished"),
            "local_voice_polished": row.get("local_voice_polished"),
            "hooks": [str(h).strip() for h in hooks if str(h).strip()],
            "reason_zh": row.get("reason_zh"),
            "evidence_refs": jload(row.get("evidence_refs_json"), []) or [],
        }
    return refs


def load_product_recommendations(conn, batch_id: str, product_id: str = "", limit: int = 20) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    params: List[Any] = [batch_id]
    where = "batch_id=%s"
    if product_id:
        where += " AND product_id=%s"
        params.append(product_id)
    cur.execute(
        f"""
        SELECT recommendation_id, batch_id, product_id, market, category_key, mixcut_category,
               quality_status, recommendation_status, primary_selling_points_json,
               risk_guards_json, skipped_insights_json, source_payload_json, updated_at
        FROM fastmoss_voc_product_recommendation
        WHERE {where}
        ORDER BY updated_at DESC
        LIMIT %s
        """,
        (*params, max(1, int(limit or 1))),
    )
    rows = cur.fetchall()
    picked: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        pid = str(row.get("product_id") or "")
        if not pid or pid in picked:
            continue
        row = dict(row)
        row["primary_selling_points"] = jload(row.pop("primary_selling_points_json"), []) or []
        row["risk_guards"] = jload(row.pop("risk_guards_json"), []) or []
        row["skipped_insights"] = jload(row.pop("skipped_insights_json"), []) or []
        row["source_payload"] = jload(row.pop("source_payload_json"), {}) or {}
        picked[pid] = row
    return list(picked.values())


def load_product_anchor(conn, product_id: str) -> Dict[str, Any]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT product_id, product_name, market, category, product_anchor_json,
               anchor_status, anchor_version, anchor_confirmed_at
        FROM products
        WHERE product_id=%s
        LIMIT 1
        """,
        (product_id,),
    )
    row = cur.fetchone()
    if not row:
        return {"product_id": product_id, "anchor_status": "missing", "anchor_summary": {}}
    anchor = jload(row.get("product_anchor_json"), {}) or {}
    anchor_summary = {
        "candidate_primary_selling_points": short_texts(anchor.get("candidate_primary_selling_points")),
        "hard_anchors": dedupe(
            short_texts(anchor.get("hard_anchors"), ("anchor", "constraint", "text"))
            + short_texts(anchor.get("core_visual_points"))
            + short_texts(anchor.get("structure_anchors"))
        )[:6],
        "display_anchors": dedupe(
            short_texts(anchor.get("display_anchors"), ("anchor", "constraint", "text"))
            + short_texts(anchor.get("core_visual_points"))
        )[:6],
        "key_visual_constraints": dedupe(
            short_texts(anchor.get("key_visual_constraints"), ("constraint", "anchor", "text"))
            + short_texts(anchor.get("must_not_change_points"))
        )[:6],
        "distortion_alerts": dedupe(
            short_texts(anchor.get("distortion_alerts"), ("anchor", "constraint", "text"))
            + short_texts(anchor.get("forbidden_mismatch"))
            + short_texts(anchor.get("forbidden_actions"))
        )[:6],
    }
    return {
        "product_id": row.get("product_id"),
        "product_name": row.get("product_name"),
        "market": row.get("market"),
        "category": row.get("category"),
        "anchor_status": row.get("anchor_status") or "unknown",
        "anchor_version": row.get("anchor_version"),
        "anchor_confirmed_at": row.get("anchor_confirmed_at"),
        "anchor_summary": anchor_summary,
    }


def latest_task_record(conn, product_id: str) -> Dict[str, Any]:
    cur = conn.cursor()
    cur.execute(
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
    return cur.fetchone() or {}


def feishu_task_client():
    root = Path(os.environ.get("AUTO_MIXCUT_ROOT", "/Users/likeu3/.openclaw/workspace/auto_mixcut"))
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from auto_mixcut.adapters.feishu import AutoMixcutFeishuClient  # type: ignore

    return AutoMixcutFeishuClient("商品内容任务表")


def ensure_feishu_task_fields(client: Any) -> None:
    existing = {field.field_name for field in client.client.list_fields()}
    for field_name in TASK_CONFIRMATION_FIELDS:
        if field_name not in existing:
            client.client.create_field(field_name, field_type=1, ui_type="Text")
            existing.add(field_name)


def load_feishu_confirmation(conn, product_id: str, ensure_fields: bool = False) -> Dict[str, Any]:
    task = latest_task_record(conn, product_id)
    record_id = str(task.get("feishu_record_id") or "")
    base = {
        "source": "none",
        "task_id": task.get("task_id"),
        "feishu_record_id": record_id,
        "raw_status": "",
        "status": "pending",
        "confirmed_insight_ids": [],
        "confirmed_texts": [],
        "counts_by_key": {},
        "target_hook_count": 0,
    }
    if not record_id.startswith("rec"):
        base["source"] = "rds_task_without_feishu_record" if task else "missing_task"
        return base
    client = feishu_task_client()
    if ensure_fields:
        ensure_feishu_task_fields(client)
    fields = client.get_record(record_id)
    raw_status = cell_text(fields.get(TASK_CONFIRM_STATUS_FIELD))
    ids, texts, counts = parse_confirmed_points([fields.get(TASK_CONFIRM_POINTS_FIELD)])
    status = confirmation_status_value(raw_status)
    if status == "pending" and (ids or texts):
        status = "confirmed"
    base.update(
        {
            "source": "feishu_task",
            "raw_status": raw_status,
            "status": status,
            "confirmed_insight_ids": ids,
            "confirmed_texts": texts,
            "counts_by_key": counts,
            "target_hook_count": normalize_count(fields.get(TASK_TARGET_COUNT_FIELD)),
        }
    )
    return base


def package_summary(package: Dict[str, Any]) -> str:
    lines = []
    for item in package.get("hook_candidates") or []:
        count = int(item.get("requested_hook_count") or 0)
        suffix = f" x{count}" if count else ""
        proof = item.get("visual_proof_zh") or item.get("product_selling_point")
        lines.append(f"{item.get('insight_id')}: {proof}{suffix}")
    if not lines:
        lines.append("; ".join(package.get("missing_reasons") or []) or "no hook candidates")
    return "\n".join(lines[:6])


def sync_package_status_to_feishu(conn, product_id: str, package: Dict[str, Any], ensure_fields: bool = False) -> Dict[str, Any]:
    task = latest_task_record(conn, product_id)
    record_id = str(task.get("feishu_record_id") or "")
    if not record_id.startswith("rec"):
        return {"success": False, "product_id": product_id, "reason": "missing_feishu_record"}
    client = feishu_task_client()
    if ensure_fields:
        ensure_feishu_task_fields(client)
    fields = {
        TASK_PACKAGE_STATUS_FIELD: package.get("readiness_status") or "",
        TASK_PACKAGE_ID_FIELD: package.get("package_id") or "",
        TASK_PACKAGE_COUNT_FIELD: str(len(package.get("hook_candidates") or [])),
        TASK_PACKAGE_SUMMARY_FIELD: package_summary(package),
        TASK_PACKAGE_UPDATED_FIELD: now_iso(),
    }
    client.update_record(record_id, fields)
    return {"success": True, "product_id": product_id, "feishu_record_id": record_id, "updated_fields": sorted(fields)}


def is_core_product_point(item: Dict[str, Any]) -> bool:
    insight_id = str(item.get("insight_id") or "")
    if insight_id in NON_CORE_IDS:
        return False
    role = str(item.get("insight_role") or "")
    if role:
        return role == CORE_ROLE
    return insight_id in CORE_INSIGHT_IDS


def visual_fields_for_point(item: Dict[str, Any]) -> Dict[str, Any]:
    insight_id = str(item.get("insight_id") or "")
    # prefer item's own taxonomy fields, then fallback
    proof_shots = item.get("proof_shot_list") or item.get("shot_plan")
    if isinstance(proof_shots, str):
        proof_shots = [p.strip() for p in re.split(r"[\n;；]+", proof_shots) if p.strip()]
    if not isinstance(proof_shots, list):
        proof_shots = []
    usage_lane = item.get("usage_lane")
    if not usage_lane:
        legacy = _legacy_fallback_proof(insight_id)
        usage_lane = legacy.get("usage_lane", "video_support")
    visual_goal = item.get("visual_goal_zh") or item.get("visual_proof_zh")
    if not visual_goal:
        legacy = _legacy_fallback_proof(insight_id)
        visual_goal = legacy.get("visual_goal_zh") or legacy.get("visual_proof_zh") or ""
    return {
        "voc_signal": item.get("voc_signal") or "",
        "proof_archetype": item.get("proof_archetype") or _legacy_fallback_proof(insight_id).get("proof_archetype") or "",
        "usage_lane": usage_lane,
        "video_fit_score": int(item.get("video_fit_score") or _legacy_fallback_proof(insight_id).get("video_fit_score") or 0),
        "visual_goal_zh": visual_goal,
        "visual_proof_zh": visual_goal,
        "required_beats": item.get("required_beats") or [],
        "forbidden_claims": item.get("forbidden_claims") or [],
        "copy_lane_reason": item.get("copy_lane_reason") or "",
        "required_action_zh": item.get("required_action_zh") or "",
        "proof_shot_list": [str(x).strip() for x in proof_shots if str(x).strip()],
        "shot_plan": item.get("shot_plan") or ([str(x).strip() for x in proof_shots if str(x).strip()]),
        "safe_micro_actions": item.get("safe_micro_actions") or [],
        "negative_constraints": item.get("negative_constraints") or [],
        "allowed_segment_types": item.get("allowed_segment_types") or [],
        "category_adapter": item.get("category_adapter") or "",
    }


def is_video_hook_point(point: Dict[str, Any]) -> bool:
    lane = str(point.get("usage_lane") or "").strip()
    archetype = str(point.get("proof_archetype") or "").strip()
    score = int(point.get("video_fit_score") or 0)
    if archetype in ("copy_only", "reject"):
        return False
    return lane in VIDEO_USAGE_LANES and score >= MIN_ADS_VIDEO_FIT_SCORE


def normalize_product_points(reco: Dict[str, Any]) -> List[Dict[str, Any]]:
    points = reco.get("primary_selling_points") or []
    out: List[Dict[str, Any]] = []
    for item in points:
        if not isinstance(item, dict):
            continue
        if str(item.get("decision") or "") not in {"primary", "secondary", ""}:
            continue
        if not is_core_product_point(item):
            continue
        insight_id = str(item.get("insight_id") or "")
        point = {
            "insight_id": insight_id,
            "insight_role": item.get("insight_role") or CORE_ROLE,
            "decision": item.get("decision") or "candidate",
            "selling_point": item.get("selling_point") or item.get("title"),
            "title": item.get("title"),
            "local_title": item.get("local_title"),
            "reason": item.get("reason"),
            "confidence": item.get("confidence"),
            "suggested_count": int(item.get("suggested_count") or 1),
            "scope": item.get("scope") or item.get("evidence_scope"),
            "scope_key": item.get("scope_key") or item.get("product_form"),
            "product_form": item.get("product_form"),
            "hook_intent": item.get("hook_intent") or HOOK_INTENTS.get(insight_id, "product_clarity"),
            "product_evidence_count": item.get("product_evidence_count"),
            "evidence_count": item.get("evidence_count"),
            "evidence_refs": item.get("evidence_refs") or [],
            "evidence_examples": item.get("evidence_examples") or [],
        }
        point.update(visual_fields_for_point(item))
        out.append(point)
    return out


def confirmation_status(
    core_points: List[Dict[str, Any]],
    manual_confirmation: Dict[str, Any],
    reco_status: str,
) -> str:
    normalized_ids = {str(x).strip() for x in (manual_confirmation.get("confirmed_insight_ids") or []) if str(x).strip()}
    normalized_texts = [str(x).strip() for x in (manual_confirmation.get("confirmed_texts") or []) if str(x).strip()]
    if any(str(core.get("insight_id") or "") in normalized_ids for core in core_points):
        return "confirmed_by_insight_id"
    for core in core_points:
        text = str(core.get("selling_point") or core.get("title") or "")
        if text and any(text in confirmed or confirmed in text for confirmed in normalized_texts):
            return "confirmed_by_text"
    if str(manual_confirmation.get("status") or "") == "confirmed":
        return "confirmed_by_status"
    if str(manual_confirmation.get("status") or "") == "rejected":
        return "rejected"
    if str(reco_status or "").lower() in {"confirmed", "manual_confirmed", "approved"}:
        return "confirmed_in_recommendation_status"
    return "pending"


def point_matches_confirmation(point: Dict[str, Any], manual_confirmation: Dict[str, Any]) -> bool:
    ids = {str(x).strip() for x in (manual_confirmation.get("confirmed_insight_ids") or []) if str(x).strip()}
    texts = [str(x).strip() for x in (manual_confirmation.get("confirmed_texts") or []) if str(x).strip()]
    if not ids and not texts:
        return True
    insight_id = str(point.get("insight_id") or "")
    if insight_id in ids:
        return True
    haystack = " | ".join(
        str(point.get(key) or "")
        for key in ("selling_point", "title", "local_title", "reason", "insight_id")
    )
    return any(text and (text in haystack or haystack in text) for text in texts)


def requested_count_for_point(point: Dict[str, Any], manual_confirmation: Dict[str, Any], candidate_count: int) -> int:
    counts = manual_confirmation.get("counts_by_key") or {}
    insight_id = str(point.get("insight_id") or "")
    if insight_id in counts:
        return int(counts[insight_id] or 0)
    for key, value in counts.items():
        text = str(key or "").strip()
        if not text:
            continue
        haystack = " | ".join(str(point.get(k) or "") for k in ("selling_point", "title", "local_title", "reason"))
        if text in haystack or haystack in text:
            return int(value or 0)
    target = int(manual_confirmation.get("target_hook_count") or 0)
    if target and candidate_count == 1:
        return target
    return max(1, int(point.get("suggested_count") or 1))


def build_package(
    batch_id: str,
    run_id: str,
    reco: Dict[str, Any],
    category_refs: Dict[str, Dict[str, Any]],
    anchor: Dict[str, Any],
    usecase: str,
    manual_confirmation: Dict[str, Any],
    allow_unconfirmed_smoke: bool,
) -> Dict[str, Any]:
    product_id = str(reco.get("product_id") or "")
    source_payload = reco.get("source_payload") or {}
    product_form = str(
        source_payload.get("product_form")
        or (source_payload.get("coverage") or {}).get("product_form")
        or ""
    )
    core_points = normalize_product_points(reco)
    video_points = [point for point in core_points if is_video_hook_point(point)]
    manual_status = confirmation_status(video_points or core_points, manual_confirmation, str(reco.get("recommendation_status") or ""))
    selected_points = [
        point
        for point in video_points
        if manual_status in {"pending", "rejected"} or point_matches_confirmation(point, manual_confirmation)
    ]
    candidates: List[Dict[str, Any]] = []
    mixcut_category = str(source_payload.get("mixcut_category") or source_payload.get("category") or "")
    category_key = str(reco.get("category_key") or source_payload.get("category_key") or "")
    for index, point in enumerate(selected_points):
        ref = category_refs.get(point["insight_id"]) or {}
        if not ref:
            continue
        requested_count = requested_count_for_point(point, manual_confirmation, len(selected_points))
        # compile adapter for this point
        adapter = select_category_adapter(point, anchor, product_form, mixcut_category, category_key)
        visual = compile_visual_proof(point, adapter)
        candidates.append(
            {
                "candidate_id": f"{product_id}__{point['insight_id']}__{index}",
                "insight_id": point["insight_id"],
                "insight_role": point["insight_role"],
                "hook_intent": point["hook_intent"],
                "requested_hook_count": requested_count,
                "product_selling_point": point["selling_point"],
                "voc_signal": visual["voc_signal"],
                "proof_archetype": visual["proof_archetype"],
                "usage_lane": visual["usage_lane"],
                "video_fit_score": visual["video_fit_score"],
                "required_beats": visual["required_beats"],
                "visual_goal_zh": visual["visual_goal_zh"],
                "visual_goal": visual["visual_goal_zh"],
                "visual_proof_zh": visual["visual_goal_zh"],
                "required_action_zh": visual["required_action_zh"],
                "proof_shot_list": visual.get("proof_shot_list") or [],
                "shot_plan": visual.get("shot_plan") or [],
                "safe_micro_actions": visual.get("safe_micro_actions") or [],
                "negative_constraints": visual.get("negative_constraints") or [],
                "allowed_segment_types": visual.get("allowed_segment_types") or [],
                "category_adapter": visual.get("category_adapter") or "",
                "copy_lane_reason": visual.get("copy_lane_reason") or "",
                "forbidden_claims": visual.get("forbidden_claims") or [],
                "product_reason": point["reason"],
                "product_evidence_count": point.get("product_evidence_count"),
                "product_evidence_refs": point.get("evidence_refs") or [],
                "category_reference_title": ref.get("title_zh_polished"),
                "category_reference_local_voice": ref.get("local_voice_polished"),
                "category_reference_hooks": ref.get("hooks") or [],
                "merge_instruction": "Use category hooks only as expression references; final prompt must show the product anchors and VOC visual proof action.",
            }
        )
    missing_reasons: List[str] = []
    if not core_points:
        missing_reasons.append("no_product_core_selling_point")
    if core_points and not video_points:
        missing_reasons.append("no_video_provable_voc_point")
    if core_points and not selected_points:
        missing_reasons.append("confirmed_visual_proof_not_matched")
    if core_points and not candidates:
        missing_reasons.append("no_matching_category_reference_hooks")
    if manual_status == "pending":
        missing_reasons.append("manual_confirmation_required")
    if manual_status == "rejected":
        missing_reasons.append("manual_confirmation_rejected")
    if missing_reasons:
        readiness = "smoke_ready_unconfirmed" if allow_unconfirmed_smoke and candidates else "needs_manual_confirmation"
        if any(reason in missing_reasons for reason in ("no_product_core_selling_point", "no_video_provable_voc_point", "confirmed_visual_proof_not_matched", "no_matching_category_reference_hooks", "manual_confirmation_rejected")):
            readiness = "blocked"
    else:
        readiness = "ready_for_hook_package"
    requested_total = int(manual_confirmation.get("target_hook_count") or 0)
    if not requested_total:
        requested_total = sum(int(item.get("requested_hook_count") or 0) for item in candidates)
    return {
        "package_id": f"{batch_id}__{run_id}__{product_id}__{usecase}",
        "batch_id": batch_id,
        "run_id": run_id,
        "product_id": product_id,
        "product_form": product_form,
        "usecase": usecase,
        "readiness_status": readiness,
        "manual_confirmation_status": manual_status,
        "requested_hook_count": requested_total,
        "missing_reasons": missing_reasons,
        "manual_confirmation": manual_confirmation,
        "recommendation": {
            "recommendation_id": reco.get("recommendation_id"),
            "recommendation_status": reco.get("recommendation_status"),
            "quality_status": reco.get("quality_status"),
            "updated_at": reco.get("updated_at"),
        },
        "product_anchor": anchor,
        "hook_candidates": candidates,
        "supporting_only": {
            "offer_and_fulfillment_signals_are_not_main_ads_hooks": True,
            "ads_hooks_require_visual_proof": True,
            "min_ads_video_fit_score": MIN_ADS_VIDEO_FIT_SCORE,
            "category_reference_hook_ids": sorted(category_refs.keys()),
        },
        "created_at": now_iso(),
    }


def persist_package(conn, package: Dict[str, Any]) -> None:
    cur = conn.cursor()
    ensure_package_table(cur)
    ts = now_iso()
    cur.execute(
        """
        INSERT INTO voc_ads_hook_package
        (package_id, batch_id, run_id, product_id, product_form, usecase,
         readiness_status, manual_confirmation_status, hook_candidate_count,
         requested_hook_count, payload_json, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          product_form=VALUES(product_form),
          readiness_status=VALUES(readiness_status),
          manual_confirmation_status=VALUES(manual_confirmation_status),
          hook_candidate_count=VALUES(hook_candidate_count),
          requested_hook_count=VALUES(requested_hook_count),
          payload_json=VALUES(payload_json),
          updated_at=VALUES(updated_at)
        """,
        (
            package["package_id"],
            package["batch_id"],
            package["run_id"],
            package["product_id"],
            package.get("product_form"),
            package["usecase"],
            package["readiness_status"],
            package["manual_confirmation_status"],
            len(package.get("hook_candidates") or []),
            int(package.get("requested_hook_count") or 0),
            json.dumps(package, ensure_ascii=False, default=str),
            ts,
            ts,
        ),
    )


def ensure_package_table(cur: Any) -> None:
    cur.execute(PACKAGE_DDL)
    cur.execute("SHOW COLUMNS FROM voc_ads_hook_package")
    existing = {str(row.get("Field") or "") for row in cur.fetchall()}
    if "requested_hook_count" not in existing:
        cur.execute("ALTER TABLE voc_ads_hook_package ADD COLUMN requested_hook_count INT DEFAULT 0 AFTER hook_candidate_count")


def run(args: argparse.Namespace) -> Dict[str, Any]:
    conn = connect_db(args.database_url)
    try:
        run_id = args.run_id or latest_category_run(conn, args.batch_id)
        if not run_id:
            raise RuntimeError(f"No category artifact run found for batch {args.batch_id}")
        category_refs = load_category_reference_hooks(conn, args.batch_id, run_id, args.usecase)
        recos = load_product_recommendations(conn, args.batch_id, args.product_id, args.limit)
        packages: List[Dict[str, Any]] = []
        feishu_syncs: List[Dict[str, Any]] = []
        cli_confirmation = build_cli_confirmation(args)
        for reco in recos:
            pid = str(reco.get("product_id") or "")
            anchor = load_product_anchor(conn, pid)
            feishu_confirmation = (
                load_feishu_confirmation(conn, pid, ensure_fields=bool(args.ensure_feishu_fields))
                if args.read_feishu_confirmation
                else {}
            )
            manual_confirmation = merge_confirmation(feishu_confirmation, cli_confirmation)
            packages.append(
                build_package(
                    args.batch_id,
                    run_id,
                    reco,
                    category_refs,
                    anchor,
                    args.usecase,
                    manual_confirmation,
                    bool(args.allow_unconfirmed_smoke),
                )
            )
        if args.write:
            for package in packages:
                persist_package(conn, package)
            conn.commit()
        if args.sync_feishu_status:
            if not args.write:
                raise RuntimeError("--sync-feishu-status requires --write")
            for package in packages:
                feishu_syncs.append(
                    sync_package_status_to_feishu(
                        conn,
                        str(package.get("product_id") or ""),
                        package,
                        ensure_fields=bool(args.ensure_feishu_fields),
                    )
                )
        return {
            "success": True,
            "batch_id": args.batch_id,
            "run_id": run_id,
            "usecase": args.usecase,
            "write": bool(args.write),
            "category_reference_hooks": [
                {
                    "insight_id": ref.get("insight_id"),
                    "title": ref.get("title_zh_polished"),
                    "hooks": ref.get("hooks"),
                }
                for ref in category_refs.values()
            ],
            "package_count": len(packages),
            "ready_count": sum(1 for p in packages if p.get("readiness_status") == "ready_for_hook_package"),
            "smoke_ready_count": sum(1 for p in packages if p.get("readiness_status") == "smoke_ready_unconfirmed"),
            "blocked_count": sum(1 for p in packages if p.get("readiness_status") == "blocked"),
            "needs_confirmation_count": sum(1 for p in packages if p.get("readiness_status") == "needs_manual_confirmation"),
            "feishu_syncs": feishu_syncs,
            "packages": packages,
        }
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build product-scoped ADS hook candidate packages from VOC insights.")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--product-id", default="")
    parser.add_argument("--run-id", default="", help="category artifact run_id; latest run is used if omitted")
    parser.add_argument("--usecase", default=USECASE_ADS)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--confirmed-insight-id", action="append", default=[])
    parser.add_argument("--confirmed-selling-point", action="append", default=[])
    parser.add_argument("--target-hook-count", type=int, default=0, help="fallback requested hook count; applies directly when one selling point is selected")
    parser.add_argument("--read-feishu-confirmation", action="store_true", help="read VOC manual confirmation fields from 商品内容任务表")
    parser.add_argument("--ensure-feishu-fields", action="store_true", help="create missing VOC confirmation/status fields in 商品内容任务表")
    parser.add_argument("--sync-feishu-status", action="store_true", help="write package status/id/summary back to 商品内容任务表; requires --write")
    parser.add_argument("--allow-unconfirmed-smoke", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--pretty", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = run(args)
    except Exception as exc:
        print(json.dumps({"success": False, "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2 if args.pretty else None, default=str))
    return 0 if result.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())
