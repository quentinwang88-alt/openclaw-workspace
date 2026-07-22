#!/usr/bin/env python3
"""voc-insight MVP — deterministic VOC insight generator.

Reads Fastmoss VOC evidence from RDS, produces form-level / category-level /
product-level insights with sample thresholds, guardrails and evidence refs.

Usage:
  python3 scripts/run_voc_insight.py --batch-id FM_TH_HAIRCLIP_20260622_165607 --scope form --product-form basic_hair_clip --dry-run
  python3 scripts/run_voc_insight.py --batch-id FM_TH_HAIRCLIP_20260622_165607 --scope category --write
  python3 scripts/run_voc_insight.py --batch-id FM_TH_HAIRCLIP_20260622_165607 --scope product --product-id 1729659517276948599 --usecase ads_mixcut --write
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

try:
    import pymysql
except ImportError:
    pymysql = None  # type: ignore


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

BATCH_LEVEL_TABLE = "fastmoss_voc_product_recommendation"
DET_RECO_SUFFIX = "voc_det_v1"

SCOPE_CATEGORY = "category"
SCOPE_FORM = "form"
SCOPE_PRODUCT = "product"

USECASE_ADS = "ads_mixcut"
USECASE_CONTENT = "content_copy"
USECASE_CREATOR = "creator_brief"
USECASE_SELECTION = "selection"
ALL_USECASES = [USECASE_ADS, USECASE_CONTENT, USECASE_CREATOR, USECASE_SELECTION]

# Signal tag -> static metadata. Titles/local voices align with upstream
# fastmoss_voc_insight rows so IDs stay cross-referenceable.
SIGNAL_META: Dict[str, Dict[str, str]] = {
    "appearance_cute_color": {
        "insight_type": "selling_point",
        "insight_role": "product_core_selling_point",
        "insight_id": "selling_appearance_cute_color",
        "title_zh": "款式好看、颜色/造型容易被认可",
        "local_voice": "สวย น่ารัก สีตรงใจ",
    },
    "hold_quality": {
        "insight_type": "selling_point",
        "insight_role": "product_core_selling_point",
        "insight_id": "selling_hold_quality",
        "title_zh": "夹得稳、质感/耐用性有正反馈",
        "local_voice": "ติดแน่น ใช้งานได้จริง",
    },
    "value_quantity": {
        "insight_type": "price_value",
        "insight_role": "offer_selling_point",
        "insight_id": "selling_value_quantity",
        "title_zh": "数量多、价格值、组合装有性价比",
        "local_voice": "ได้เยอะ คุ้มราคา",
    },
    "fast_shipping": {
        "insight_type": "selling_point",
        "insight_role": "fulfillment_trust",
        "insight_id": "selling_fast_shipping",
        "title_zh": "发货快可作为辅助信任点",
        "local_voice": "ส่งไว",
    },
    "slow_shipping": {
        "insight_type": "fulfillment_issue",
        "insight_role": "risk_guard",
        "insight_id": "pain_slow_shipping",
        "title_zh": "等待时间/物流体验会影响评价",
        "local_voice": "รอนาน ส่งช้า",
    },
    "fulfillment_missing": {
        "insight_type": "fulfillment_issue",
        "insight_role": "risk_guard",
        "insight_id": "pain_fulfillment_missing",
        "title_zh": "少发/漏发/履约不完整风险",
        "local_voice": "ได้ของไม่ครบ",
    },
}

# VOC 在视频链路里不再直接当“卖点文案”使用，而是先判定是否能被镜头证明。
# copy_only 的洞察仍可供商品卡/投放文案使用，但不会进入 ads_mixcut 主钩子池。
VISUAL_PROOF_META: Dict[str, Dict[str, Any]] = {}

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


_CATEGORY_SIGNAL_META_CACHE: Optional[Dict[str, Dict[str, str]]] = None


def load_category_signal_meta() -> Dict[str, Dict[str, str]]:
    """Load category signal metadata while preserving legacy signal behavior."""
    global _CATEGORY_SIGNAL_META_CACHE
    if _CATEGORY_SIGNAL_META_CACHE is not None:
        return _CATEGORY_SIGNAL_META_CACHE
    merged: Dict[str, Dict[str, str]] = {}
    for filename in ("wigs_voc_taxonomy_v1.json",):
        path = os.path.join(_REFS_DIR, filename)
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                payload = json.load(f)
            for tag, entry in (payload.get("signals") or {}).items():
                if not isinstance(entry, dict):
                    continue
                merged[str(tag)] = {
                    str(k): str(v) for k, v in entry.items()
                    if k in {"insight_type", "insight_role", "insight_id", "title_zh", "local_voice"}
                }
        except Exception:
            continue
    _CATEGORY_SIGNAL_META_CACHE = merged
    return merged


def signal_meta_for_tag(tag: str, agg: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    if tag in SIGNAL_META:
        return SIGNAL_META[tag]
    category_meta = load_category_signal_meta().get(tag)
    if category_meta:
        return category_meta
    sentiment_counts = dict((agg or {}).get("sentiment_counts") or {})
    negative = int(sentiment_counts.get("negative") or 0)
    positive = int(sentiment_counts.get("positive") or 0)
    if negative > positive:
        return {
            "insight_type": "pain_point",
            "insight_role": "risk_guard",
            "insight_id": "pain_" + tag,
            "title_zh": tag,
            "local_voice": "",
        }
    return {
        "insight_type": "selling_point",
        "insight_role": "product_core_selling_point",
        "insight_id": "selling_" + tag,
        "title_zh": tag,
        "local_voice": "",
    }

def proof_meta_for_signal(signal_tag: str, insight_type: str, insight_role: str) -> Dict[str, Any]:
    taxonomy = load_video_proof_taxonomy()
    signal_map = taxonomy.get("signal_map", {})
    entry = signal_map.get(signal_tag)
    if entry:
        return dict(entry)
    default = taxonomy.get("default_positive", {})
    if insight_type not in RISK_ONLY_TYPES and insight_role != "risk_guard":
        return dict(default) if default else {
            "voc_signal": "general_positive",
            "proof_archetype": "scenario_use_proof",
            "usage_lane": "video_support",
            "video_fit_score": 55,
            "required_beats": ["real_scene", "use_action", "result_state"],
            "visual_goal_zh": "在真实场景中展示商品有用或好看",
            "forbidden_claims": ["不要使用没有证据的功能承诺"],
        }
    return {
        "voc_signal": "risk_pain",
        "proof_archetype": "reject",
        "usage_lane": "reject",
        "video_fit_score": 0,
        "required_beats": [],
        "visual_goal_zh": "风险提示，不可包装成卖点",
        "forbidden_claims": ["不得转成推荐/强推/入手类表达"],
    }

# Insight types that must NEVER be sold; only used as risk guards.
RISK_ONLY_TYPES = {"fulfillment_issue", "pain_point", "risk_guard"}

# price_value / value_quantity is only valid for multi-pack / set products.
VALUE_PACK_TYPES = {"bulk_or_assorted_set"}

# Sample thresholds (per the V1 plan).
# form-level
FORM_OBSERVE_ONLY = "observe_only"          # <3 products OR <10 voc
FORM_PARTIAL = "partial_candidate"          # >=3 products AND >=10 voc
FORM_CANDIDATE = "form_candidate"           # >=5 products AND >=30 voc
FORM_ADS = "ads_candidate"                  # >=8 products AND >=60 voc
# category-level
CATEGORY_CANDIDATE = "category_candidate"
CATEGORY_ADS_CANDIDATE = "category_ads_candidate"

USECASES_BY_CONFIDENCE: Dict[str, List[str]] = {
    FORM_ADS: [USECASE_ADS, USECASE_CONTENT, USECASE_CREATOR, USECASE_SELECTION],
    FORM_CANDIDATE: [USECASE_CONTENT, USECASE_CREATOR, USECASE_ADS],
    FORM_PARTIAL: [USECASE_CREATOR, USECASE_CONTENT],
    CATEGORY_CANDIDATE: [USECASE_CONTENT, USECASE_CREATOR, USECASE_SELECTION],
    CATEGORY_ADS_CANDIDATE: [USECASE_ADS, USECASE_CONTENT, USECASE_CREATOR, USECASE_SELECTION],
    FORM_OBSERVE_ONLY: [],
}
NOT_FOR_USECASES: Dict[str, List[str]] = {
    FORM_OBSERVE_ONLY: ALL_USECASES,
    FORM_PARTIAL: [USECASE_ADS, USECASE_SELECTION],
}

# Min evidence rows for a (form, signal_tag) to become a non-trivial insight.
MIN_FORM_INSIGHT_EVIDENCE = 2
# Min forms a signal must cover (with >=1 evidence) to be promoted to category.
MIN_CATEGORY_FORMS = 3
MIN_CATEGORY_PRODUCTS = 10
MIN_CATEGORY_VOC = 30
MIN_CATEGORY_ADS_VOC = 80
MAX_CATEGORY_FORM_CONTRIB = 0.60
MAX_CATEGORY_PRODUCT_CONTRIB = 0.30


DDL_SQL = [
    """CREATE TABLE IF NOT EXISTS voc_insight_run (
  run_id VARCHAR(191) NOT NULL PRIMARY KEY,
  batch_id VARCHAR(191) NOT NULL,
  market VARCHAR(32) NOT NULL,
  category_key VARCHAR(191) NOT NULL,
  scope VARCHAR(32) NOT NULL,
  product_form VARCHAR(191) NULL,
  product_id VARCHAR(191) NULL,
  usecase VARCHAR(64) NULL,
  params_json LONGTEXT,
  status VARCHAR(64) NOT NULL,
  started_at VARCHAR(64) NOT NULL,
  finished_at VARCHAR(64) NULL,
  summary_json LONGTEXT,
  error_message LONGTEXT,
  created_at VARCHAR(64) NOT NULL,
  updated_at VARCHAR(64) NOT NULL,
  INDEX idx_voc_run_batch (batch_id)
) CHARACTER SET utf8mb4""",
    """CREATE TABLE IF NOT EXISTS voc_form_insight_artifact (
  artifact_id VARCHAR(191) NOT NULL PRIMARY KEY,
  run_id VARCHAR(191) NOT NULL,
  batch_id VARCHAR(191) NOT NULL,
  market VARCHAR(32) NOT NULL,
  category_key VARCHAR(191) NOT NULL,
  product_form VARCHAR(191) NOT NULL,
  confidence_level VARCHAR(64) NOT NULL,
  product_count INT DEFAULT 0,
  voc_count INT DEFAULT 0,
  insight_payload_json JSON,
  evidence_refs_json JSON,
  created_at VARCHAR(64) NOT NULL,
  updated_at VARCHAR(64) NOT NULL,
  UNIQUE KEY uq_voc_form_artifact (run_id, product_form),
  INDEX idx_voc_form_batch (batch_id, product_form)
) CHARACTER SET utf8mb4""",
    """CREATE TABLE IF NOT EXISTS voc_category_insight_artifact (
  artifact_id VARCHAR(191) NOT NULL PRIMARY KEY,
  run_id VARCHAR(191) NOT NULL,
  batch_id VARCHAR(191) NOT NULL,
  market VARCHAR(32) NOT NULL,
  category_key VARCHAR(191) NOT NULL,
  confidence_level VARCHAR(64) NOT NULL,
  covered_forms_json JSON,
  product_count INT DEFAULT 0,
  voc_count INT DEFAULT 0,
  insight_payload_json JSON,
  evidence_refs_json JSON,
  created_at VARCHAR(64) NOT NULL,
  updated_at VARCHAR(64) NOT NULL,
  UNIQUE KEY uq_voc_category_artifact (run_id)
) CHARACTER SET utf8mb4""",
]


# --------------------------------------------------------------------------- #
# DB helpers
# --------------------------------------------------------------------------- #

class DB:
    def __init__(self, conn):
        self.conn = conn

    @classmethod
    def connect(cls, url: Optional[str] = None) -> "DB":
        if pymysql is None:
            raise RuntimeError("pymysql not installed. Run: python3 -m pip install --user pymysql")
        url = url or os.environ.get("VOC_INSIGHT_DATABASE_URL") or os.environ.get("LIKEU_AI_DATABASE_URL") \
            or os.environ.get("HERMES_AGENT_DATABASE_URL") or ""
        if not url:
            raise RuntimeError("No database URL. Set LIKEU_AI_DATABASE_URL or pass --database-url.")
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        conn = pymysql.connect(
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
        return cls(conn)

    def cursor(self):
        return self.conn.cursor()

    def commit(self):
        self.conn.commit()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def ensure_tables(self) -> None:
        cur = self.conn.cursor()
        for stmt in DDL_SQL:
            cur.execute(stmt)
        self.conn.commit()


def jload(v: Any) -> Any:
    if v is None or v == "":
        return None
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v)
    except Exception:
        return None


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_mysql_dt() -> str:
    # fastmoss_voc_product_recommendation uses MySQL DATETIME columns.
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# --------------------------------------------------------------------------- #
# Loaders
# --------------------------------------------------------------------------- #

def load_batch_meta(db: DB, batch_id: str) -> Dict[str, Any]:
    """Derive batch metadata. export_batch may be empty; fall back to pack+snapshot+enriched."""
    cur = db.cursor()
    meta: Dict[str, Any] = {"batch_id": batch_id}
    cur.execute("SELECT * FROM fastmoss_voc_export_batch WHERE batch_id=%s", (batch_id,))
    row = cur.fetchone()
    if row:
        meta.update({k: row[k] for k in ("market", "category_key", "classic_product_count",
                                         "new_product_count", "product_count", "voc_count",
                                         "quality_status") if k in row})
    # insight_pack gives market/category if export_batch empty
    cur.execute("SELECT insight_pack_id, market, category_key, quality_json, flow_issue_count "
                "FROM fastmoss_voc_insight_pack WHERE batch_id=%s ORDER BY generated_at DESC LIMIT 1", (batch_id,))
    pack = cur.fetchone()
    if pack:
        meta.setdefault("market", pack.get("market"))
        meta.setdefault("category_key", pack.get("category_key"))
        meta["insight_pack_id"] = pack.get("insight_pack_id")
        meta["pack_quality_json"] = pack.get("quality_json")
    # pool_type distribution + totals from enriched
    cur.execute("SELECT pool_type, COUNT(*) voc, COUNT(DISTINCT fastmoss_product_id) products "
                "FROM fastmoss_voc_enriched WHERE batch_id=%s GROUP BY pool_type", (batch_id,))
    pools = cur.fetchall()
    meta["pool_types"] = sorted([p["pool_type"] for p in pools if p["pool_type"]])
    meta["classic_only"] = bool(pools) and all(p["pool_type"] == "classic" for p in pools)
    meta["enriched_voc_count"] = sum(p["voc"] for p in pools)
    meta["enriched_product_count"] = sum(p["products"] for p in pools)
    # snapshot-level totals (may include products without VOC)
    cur.execute("SELECT COUNT(DISTINCT fastmoss_product_id) products "
                "FROM fastmoss_voc_product_snapshot WHERE batch_id=%s", (batch_id,))
    snap = cur.fetchone()
    meta["snapshot_product_count"] = (snap or {}).get("products") or 0
    if "market" not in meta or not meta["market"]:
        cur.execute("SELECT market, category_key FROM fastmoss_voc_product_snapshot WHERE batch_id=%s LIMIT 1", (batch_id,))
        s = cur.fetchone()
        if s:
            meta.setdefault("market", s.get("market"))
            meta.setdefault("category_key", s.get("category_key"))
    return meta


def load_form_summaries(db: DB, batch_id: str) -> List[Dict[str, Any]]:
    cur = db.cursor()
    cur.execute(
        "SELECT product_form, product_form_label, product_count, voc_count, "
        "sentiment_counts_json, pack_type_counts_json, top_signal_tags_json, "
        "style_tag_counts_json, product_ids_json "
        "FROM fastmoss_voc_product_form_summary WHERE batch_id=%s ORDER BY voc_count DESC",
        (batch_id,),
    )
    rows = cur.fetchall()
    for r in rows:
        r["sentiment_counts"] = jload(r.pop("sentiment_counts_json")) or {}
        r["pack_type_counts"] = jload(r.pop("pack_type_counts_json")) or {}
        r["top_signal_tags"] = jload(r.pop("top_signal_tags_json")) or []
        r["style_tag_counts"] = jload(r.pop("style_tag_counts_json")) or {}
        r["product_ids"] = jload(r.pop("product_ids_json")) or []
    return rows


def load_enriched(db: DB, batch_id: str, product_form: Optional[str] = None,
                  product_id: Optional[str] = None) -> List[Dict[str, Any]]:
    cur = db.cursor()
    sql = ("SELECT evidence_id, fastmoss_product_id, product_form, product_form_label, "
           "product_pack_type, pool_type, sentiment, signal_tags_json, translation_zh_hint, "
           "voc_text, source_url FROM fastmoss_voc_enriched WHERE batch_id=%s")
    params: List[Any] = [batch_id]
    if product_form:
        sql += " AND product_form=%s"
        params.append(product_form)
    if product_id:
        sql += " AND fastmoss_product_id=%s"
        params.append(product_id)
    sql += " ORDER BY product_form, fastmoss_product_id, voc_rank"
    cur.execute(sql, params)
    rows = cur.fetchall()
    for r in rows:
        r["signal_tags"] = jload(r.pop("signal_tags_json")) or []
        if isinstance(r["signal_tags"], dict):
            r["signal_tags"] = list(r["signal_tags"].keys())
        r["signal_tags"] = [str(t) for t in r["signal_tags"]]
    return rows


def load_flow_issues(db: DB, batch_id: str) -> List[Dict[str, Any]]:
    cur = db.cursor()
    cur.execute("SELECT issue_id, severity, title, detail FROM fastmoss_voc_flow_issue WHERE batch_id=%s",
                (batch_id,))
    return cur.fetchall()


def load_product_snapshot(db: DB, batch_id: str, product_id: str) -> Optional[Dict[str, Any]]:
    cur = db.cursor()
    cur.execute("SELECT fastmoss_product_id, pool_type, market, category_key, product_title, "
                "sales_metric_json FROM fastmoss_voc_product_snapshot "
                "WHERE batch_id=%s AND fastmoss_product_id=%s LIMIT 1", (batch_id, product_id))
    row = cur.fetchone()
    if row:
        row["sales_metric"] = jload(row.pop("sales_metric_json"))
    return row


# --------------------------------------------------------------------------- #
# Sample thresholds
# --------------------------------------------------------------------------- #

def form_confidence(products: int, voc: int) -> str:
    if products < 3 or voc < 10:
        return FORM_OBSERVE_ONLY
    if products >= 8 and voc >= 60:
        return FORM_ADS
    if products >= 5 and voc >= 30:
        return FORM_CANDIDATE
    return FORM_PARTIAL


def category_confidence(form_covered: int, products: int, voc: int,
                        max_form_contrib: float, max_product_contrib: float,
                        classic_only: bool) -> Tuple[str, List[str]]:
    notes: List[str] = []
    if classic_only:
        notes.append("classic_only_evidence")
    if form_covered < MIN_CATEGORY_FORMS or products < MIN_CATEGORY_PRODUCTS or voc < MIN_CATEGORY_VOC:
        return FORM_OBSERVE_ONLY, notes
    if max_form_contrib > MAX_CATEGORY_FORM_CONTRIB:
        notes.append("single_form_bias")
    if max_product_contrib > MAX_CATEGORY_PRODUCT_CONTRIB:
        notes.append("single_product_bias")
    if max_form_contrib > MAX_CATEGORY_FORM_CONTRIB or max_product_contrib > MAX_CATEGORY_PRODUCT_CONTRIB:
        return FORM_PARTIAL, notes
    if voc >= MIN_CATEGORY_ADS_VOC:
        return CATEGORY_ADS_CANDIDATE, notes
    return CATEGORY_CANDIDATE, notes


def usecases_for(confidence: str, insight_type: str, insight_role: str) -> Tuple[List[str], List[str]]:
    if insight_type in RISK_ONLY_TYPES:
        return [], ALL_USECASES  # risk guards are not selling usecases
    if insight_role == "fulfillment_trust":
        rec = [u for u in USECASES_BY_CONFIDENCE.get(confidence, []) if u in (USECASE_CONTENT, USECASE_CREATOR)]
        return rec, [u for u in ALL_USECASES if u not in rec]
    if insight_role == "offer_selling_point":
        rec = [u for u in USECASES_BY_CONFIDENCE.get(confidence, []) if u in (USECASE_CONTENT, USECASE_CREATOR)]
        return rec, [u for u in ALL_USECASES if u not in rec]
    rec = list(USECASES_BY_CONFIDENCE.get(confidence, []))
    not_for = list(NOT_FOR_USECASES.get(confidence, []))
    return rec, not_for


def visual_proof_for(tag: str, insight_type: str, insight_role: str) -> Dict[str, Any]:
    meta = proof_meta_for_signal(tag, insight_type, insight_role)
    proof_archetype = meta.get("proof_archetype", "")
    if proof_archetype in ("copy_only", "reject"):
        meta["usage_lane"] = proof_archetype
    # new canonical fields
    result = {
        "voc_signal": meta.get("voc_signal", ""),
        "proof_archetype": proof_archetype,
        "usage_lane": meta.get("usage_lane", "video_support"),
        "video_fit_score": int(meta.get("video_fit_score") or 0),
        "required_beats": meta.get("required_beats") or [],
        "visual_goal_zh": meta.get("visual_goal_zh") or meta.get("visual_proof_zh") or "",
        "forbidden_claims": meta.get("forbidden_claims") or [],
        "copy_lane_reason": meta.get("copy_lane_reason") or "",
        # backward-compatible legacy fields
        "visual_proof_zh": meta.get("visual_goal_zh") or meta.get("visual_proof_zh") or "",
        "required_action_zh": meta.get("required_action_zh") or "",
        "proof_shot_list": meta.get("proof_shot_list") or meta.get("shot_plan") or [],
    }
    if proof_archetype in ("copy_only",):
        if not result["copy_lane_reason"]:
            result["copy_lane_reason"] = (
                "price value is not a visual proof" if insight_type == "price_value"
                else "fulfillment is not a visual proof"
            )
    return result


# --------------------------------------------------------------------------- #
# Aggregation
# --------------------------------------------------------------------------- #

def aggregate_signals(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Aggregate per signal_tag across the given enriched rows."""
    agg: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "evidence_count": 0, "products": set(), "evidence_refs": [],
        "evidence_examples": [], "sentiment_counts": Counter(),
        "pack_types": Counter(), "voc_texts": [],
    })
    for r in rows:
        for tag in r["signal_tags"]:
            a = agg[tag]
            a["evidence_count"] += 1
            a["products"].add(r["fastmoss_product_id"])
            a["evidence_refs"].append(r["evidence_id"])
            a["sentiment_counts"][r["sentiment"] or "unknown"] += 1
            a["pack_types"][r["product_pack_type"] or "unknown"] += 1
            a["voc_texts"].append((r["sentiment"] or "", r["voc_text"] or ""))
    # pick up to 3 examples, prefer positive
    for a in agg.values():
        texts = a["voc_texts"]
        texts.sort(key=lambda x: 0 if x[0] == "positive" else (1 if x[0] == "mixed" else 2))
        a["evidence_examples"] = [t[:160] for _, t in texts[:3]]
        del a["voc_texts"]
    return agg


def build_insight(tag: str, scope: str, scope_key: str, confidence: str,
                  agg: Dict[str, Any], extra_notes: Optional[List[str]] = None) -> Dict[str, Any]:
    meta = signal_meta_for_tag(tag, agg)
    insight_type = meta["insight_type"]
    insight_role = meta.get("insight_role", "product_core_selling_point")
    rec, not_for = usecases_for(confidence, insight_type, insight_role)
    notes = list(extra_notes or [])
    # pack-type guard note for price_value
    if insight_type == "price_value":
        pack_types = dict(agg["pack_types"])
        if not any(p in VALUE_PACK_TYPES for p in pack_types):
            notes.append("value_quantity_requires_set_or_multipack")
    if confidence == FORM_OBSERVE_ONLY and insight_type not in RISK_ONLY_TYPES:
        notes.append("low_sample_observe_only")
    visual_proof = visual_proof_for(tag, insight_type, insight_role)
    return {
        "insight_id": meta["insight_id"],
        "insight_type": insight_type,
        "insight_role": insight_role,
        "scope": scope,
        "scope_key": scope_key,
        "title_zh": meta["title_zh"],
        "local_voice": meta["local_voice"],
        "confidence": confidence,
        "product_count": len(agg["products"]),
        "evidence_count": agg["evidence_count"],
        "signal_tags": [tag],
        "recommended_usecases": rec,
        "not_for_usecases": not_for,
        "risk_notes": notes,
        "evidence_examples": agg["evidence_examples"],
        "evidence_refs": agg["evidence_refs"],
        "sentiment_counts": dict(agg["sentiment_counts"]),
        "pack_type_counts": dict(agg["pack_types"]),
        **visual_proof,
    }


def generate_form_insights(form_rows: List[Dict[str, Any]], form: str,
                           form_label: str, confidence: str,
                           classic_only: bool) -> List[Dict[str, Any]]:
    agg = aggregate_signals(form_rows)
    insights: List[Dict[str, Any]] = []
    notes = ["classic_only_evidence"] if classic_only else []
    # order by evidence_count desc
    for tag in sorted(agg.keys(), key=lambda t: agg[t]["evidence_count"], reverse=True):
        a = agg[tag]
        if a["evidence_count"] < MIN_FORM_INSIGHT_EVIDENCE:
            continue
        insights.append(build_insight(tag, SCOPE_FORM, form, confidence, a, notes))
    return insights


def generate_category_insights(form_to_rows: Dict[str, List[Dict[str, Any]]],
                               form_confidences: Dict[str, str],
                               category_key: str, classic_only: bool
                               ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    for rows in form_to_rows.values():
        all_rows.extend(rows)
    total_voc = len(all_rows)
    total_products = len({r["fastmoss_product_id"] for r in all_rows})
    # per-tag cross-form aggregation
    per_tag: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "evidence_count": 0, "products": set(), "evidence_refs": [],
        "evidence_examples": [], "sentiment_counts": Counter(),
        "pack_types": Counter(), "voc_texts": [], "forms": set(),
    })
    for form, rows in form_to_rows.items():
        agg = aggregate_signals(rows)
        for tag, a in agg.items():
            if a["evidence_count"] < MIN_FORM_INSIGHT_EVIDENCE:
                continue
            t = per_tag[tag]
            t["evidence_count"] += a["evidence_count"]
            t["products"].update(a["products"])
            t["evidence_refs"].extend(a["evidence_refs"])
            t["sentiment_counts"].update(a["sentiment_counts"])
            t["pack_types"].update(a["pack_types"])
            t["forms"].add(form)
            t["voc_texts"].extend([(s, txt) for s, txt in zip(
                [r["sentiment"] or "" for r in rows if tag in r["signal_tags"]],
                [r["voc_text"] or "" for r in rows if tag in r["signal_tags"]])][:3])
    # examples
    for t in per_tag.values():
        t["voc_texts"].sort(key=lambda x: 0 if x[0] == "positive" else (1 if x[0] == "mixed" else 2))
        t["evidence_examples"] = [txt[:160] for _, txt in t["voc_texts"][:3]]
        del t["voc_texts"]

    category_insights: List[Dict[str, Any]] = []
    covered_forms_set = set()
    for tag in sorted(per_tag.keys(), key=lambda k: per_tag[k]["evidence_count"], reverse=True):
        t = per_tag[tag]
        form_covered = len(t["forms"])
        # contribution of the biggest form
        max_form_voc = 0
        for form in t["forms"]:
            fv = sum(1 for r in form_to_rows[form] if tag in r["signal_tags"])
            max_form_voc = max(max_form_voc, fv)
        max_form_contrib = max_form_voc / t["evidence_count"] if t["evidence_count"] else 0
        # contribution of the biggest product
        prod_counter = Counter(r["fastmoss_product_id"] for r in all_rows if tag in r["signal_tags"])
        max_product_contrib = (prod_counter.most_common(1)[0][1] / t["evidence_count"]
                                if prod_counter and t["evidence_count"] else 0)
        conf, notes = category_confidence(form_covered, len(t["products"]), t["evidence_count"],
                                          max_form_contrib, max_product_contrib, classic_only)
        if conf == FORM_OBSERVE_ONLY:
            # still emit an observation but marked observe_only
            pass
        covered_forms_set.update(t["forms"])
        ins = build_insight(tag, SCOPE_CATEGORY, category_key, conf, t, notes)
        ins["covered_forms"] = sorted(t["forms"])
        ins["max_form_contrib"] = round(max_form_contrib, 3)
        ins["max_product_contrib"] = round(max_product_contrib, 3)
        category_insights.append(ins)
    summary = {
        "total_voc": total_voc,
        "total_products": total_products,
        "covered_forms": sorted(covered_forms_set),
        "classic_only": classic_only,
    }
    return category_insights, summary


# --------------------------------------------------------------------------- #
# Product recommendation
# --------------------------------------------------------------------------- #

def product_form_and_pack(rows: List[Dict[str, Any]]) -> Tuple[str, str]:
    form = Counter(r["product_form"] or "" for r in rows).most_common(1)
    pack = Counter(r["product_pack_type"] or "" for r in rows).most_common(1)
    return (form[0][0] if form else ""), (pack[0][0] if pack else "")


def apply_product_guardrails(insight: Dict[str, Any], product_pack: str,
                             product_form: str, product_rows: List[Dict[str, Any]],
                             usecase: str
                             ) -> Tuple[str, str]:
    """Return (decision, reason). decision in {primary, secondary, risk_guard, skip}."""
    itype = insight["insight_type"]
    role = insight.get("insight_role") or "product_core_selling_point"
    # form mismatch: a form-scoped insight must match the product form
    if insight["scope"] == SCOPE_FORM and insight["scope_key"] != product_form:
        return "skip", "form_mismatch: insight form={} != product form={}".format(
            insight["scope_key"], product_form)
    # low sample observe_only selling insights -> skip
    if insight["confidence"] == FORM_OBSERVE_ONLY and itype not in RISK_ONLY_TYPES:
        return "skip", "low_sample_observe_only: no strong conclusion"
    # risk-only types -> always risk_guard
    if itype in RISK_ONLY_TYPES:
        return "risk_guard", "risk_guard: {} is a risk/pain signal, not a selling point".format(itype)
    # Fulfillment trust is not a product selling point for ADS hooks.
    if role == "fulfillment_trust" and usecase == USECASE_ADS:
        return "skip", "fulfillment_trust_not_ads_product_hook: use as trust tag only"
    # price_value requires set/multipack
    if itype == "price_value" and product_pack not in VALUE_PACK_TYPES:
        return "skip", "value_quantity_requires_set_or_multipack: product_pack={}".format(product_pack or "unknown")
    # does this product have its own evidence for the signal tag?
    tag = insight["signal_tags"][0]
    product_ev = [r for r in product_rows if tag in r["signal_tags"]]
    if not product_ev:
        return "skip", "no_product_level_evidence: product has 0 VOC rows tagged {}".format(tag)
    return "secondary", "matched: {} product-level evidence rows".format(len(product_ev))


def build_product_recommendation(product_id: str, product_rows: List[Dict[str, Any]],
                                 form_insights: List[Dict[str, Any]],
                                 category_insights: List[Dict[str, Any]],
                                 usecase: str, form_confidence: str,
                                 category_confidence: str) -> Dict[str, Any]:
    product_form, product_pack = product_form_and_pack(product_rows)
    # candidate pool per insight_id: prefer a usable form-level insight (form not observe_only),
    # else fall back to the category-level insight, else keep the form observe_only observation.
    # This stops a thin form from masking a strong cross-form category conclusion.
    form_by_id = {ins["insight_id"]: ins for ins in form_insights}
    cat_by_id = {ins["insight_id"]: ins for ins in category_insights}
    candidates: List[Dict[str, Any]] = []
    for iid in list(form_by_id.keys()) + [i for i in cat_by_id if i not in form_by_id]:
        fins = form_by_id.get(iid)
        cins = cat_by_id.get(iid)
        if fins and fins["confidence"] != FORM_OBSERVE_ONLY:
            candidates.append(fins)
        elif cins:
            candidates.append(cins)
        elif fins:
            candidates.append(fins)

    primary: List[Dict[str, Any]] = []
    secondary: List[Dict[str, Any]] = []
    risk_guards: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    # rank selling candidates by product-level evidence count
    scored: List[Tuple[int, Dict[str, Any], str, str]] = []
    for ins in candidates:
        decision, reason = apply_product_guardrails(ins, product_pack, product_form, product_rows, usecase)
        if decision == "skip":
            skipped.append(_reco_entry(ins, decision, reason, product_form))
        elif decision == "risk_guard":
            risk_guards.append(_reco_entry(ins, decision, reason, product_form))
        else:
            tag = ins["signal_tags"][0]
            pev = sum(1 for r in product_rows if tag in r["signal_tags"])
            scored.append((pev, ins, decision, reason))
    # promote the top selling candidate to primary
    scored.sort(key=lambda x: x[0], reverse=True)
    primary_assigned = False
    for idx, (pev, ins, decision, reason) in enumerate(scored):
        primary_eligible = (ins.get("insight_role") or "product_core_selling_point") == "product_core_selling_point"
        if not primary_assigned and primary_eligible and pev > 0:
            decision = "primary"
            primary_assigned = True
        entry = _reco_entry(ins, decision, reason, product_form)
        entry["product_evidence_count"] = pev
        if decision == "primary":
            primary.append(entry)
        else:
            secondary.append(entry)

    has_primary = bool(primary)
    has_any = bool(primary or secondary or risk_guards)
    if has_primary:
        quality_status = "ok"
    elif has_any:
        quality_status = "warning"
    else:
        quality_status = "insufficient"

    coverage_source = "category"
    if form_confidence != FORM_OBSERVE_ONLY and any(i["scope"] == SCOPE_FORM for i in primary + secondary):
        coverage_source = "form" if not category_insights else "mixed"

    return {
        "product_id": product_id,
        "product_form": product_form,
        "product_pack_type": product_pack,
        "usecase": usecase,
        "quality_status": quality_status,
        "coverage": {
            "form_confidence": form_confidence,
            "category_confidence": category_confidence,
            "source": coverage_source,
        },
        "primary_selling_points": primary,
        "secondary_selling_points": secondary,
        "risk_guards": risk_guards,
        "skipped_insights": skipped,
    }


def _reco_entry(insight: Dict[str, Any], decision: str, reason: str, product_form: str) -> Dict[str, Any]:
    return {
        "insight_id": insight["insight_id"],
        "insight_type": insight["insight_type"],
        "insight_role": insight.get("insight_role", "product_core_selling_point"),
        "title": insight["title_zh"],
        "local_title": insight["local_voice"],
        "decision": decision,
        "reason": reason,
        "confidence": insight["confidence"],
        "scope": insight["scope"],
        "scope_key": insight["scope_key"],
        "product_form": product_form,
        "signal_tags": insight["signal_tags"],
        "evidence_count": insight["evidence_count"],
        "product_count": insight["product_count"],
        "evidence_refs": insight["evidence_refs"],
        "evidence_examples": insight["evidence_examples"][:3],
        "risk_notes": insight.get("risk_notes", []),
        "selling_point": insight["title_zh"] if decision in ("primary", "secondary") else "",
        "voc_signal": insight.get("voc_signal") or "",
        "proof_archetype": insight.get("proof_archetype") or "",
        "usage_lane": insight.get("usage_lane") or "video_support",
        "video_fit_score": int(insight.get("video_fit_score") or 0),
        "required_beats": insight.get("required_beats") or [],
        "visual_goal_zh": insight.get("visual_goal_zh") or insight.get("visual_proof_zh") or "",
        "visual_proof_zh": insight.get("visual_goal_zh") or insight.get("visual_proof_zh") or "",
        "required_action_zh": insight.get("required_action_zh") or "",
        "proof_shot_list": insight.get("proof_shot_list") or [],
        "copy_lane_reason": insight.get("copy_lane_reason") or "",
        "forbidden_claims": insight.get("forbidden_claims") or [],
    }


# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #

def persist_run(db: DB, run: Dict[str, Any]) -> None:
    cur = db.cursor()
    cur.execute(
        "INSERT INTO voc_insight_run (run_id, batch_id, market, category_key, scope, product_form, "
        "product_id, usecase, params_json, status, started_at, finished_at, summary_json, "
        "error_message, created_at, updated_at) "
        "VALUES (%(run_id)s, %(batch_id)s, %(market)s, %(category_key)s, %(scope)s, %(product_form)s, "
        "%(product_id)s, %(usecase)s, %(params_json)s, %(status)s, %(started_at)s, %(finished_at)s, "
        "%(summary_json)s, %(error_message)s, %(created_at)s, %(updated_at)s) "
        "ON DUPLICATE KEY UPDATE status=VALUES(status), finished_at=VALUES(finished_at), "
        "summary_json=VALUES(summary_json), error_message=VALUES(error_message), updated_at=VALUES(updated_at)",
        run,
    )


def persist_form_artifact(db: DB, run_id: str, batch_id: str, market: str,
                          category_key: str, form: str, confidence: str,
                          product_count: int, voc_count: int,
                          payload: List[Dict[str, Any]], evidence_refs: List[str]) -> None:
    artifact_id = "{}__form__{}".format(run_id, form)
    ts = now_iso()
    cur = db.cursor()
    cur.execute(
        "INSERT INTO voc_form_insight_artifact (artifact_id, run_id, batch_id, market, category_key, "
        "product_form, confidence_level, product_count, voc_count, insight_payload_json, "
        "evidence_refs_json, created_at, updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE confidence_level=VALUES(confidence_level), product_count=VALUES(product_count), "
        "voc_count=VALUES(voc_count), insight_payload_json=VALUES(insight_payload_json), "
        "evidence_refs_json=VALUES(evidence_refs_json), updated_at=VALUES(updated_at)",
        (artifact_id, run_id, batch_id, market, category_key, form, confidence,
         product_count, voc_count, json.dumps(payload, ensure_ascii=False),
         json.dumps(evidence_refs, ensure_ascii=False), ts, ts),
    )


def persist_category_artifact(db: DB, run_id: str, batch_id: str, market: str,
                              category_key: str, confidence: str, covered_forms: List[str],
                              product_count: int, voc_count: int,
                              payload: List[Dict[str, Any]], evidence_refs: List[str]) -> None:
    artifact_id = "{}__category".format(run_id)
    ts = now_iso()
    cur = db.cursor()
    cur.execute(
        "INSERT INTO voc_category_insight_artifact (artifact_id, run_id, batch_id, market, category_key, "
        "confidence_level, covered_forms_json, product_count, voc_count, insight_payload_json, "
        "evidence_refs_json, created_at, updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE confidence_level=VALUES(confidence_level), covered_forms_json=VALUES(covered_forms_json), "
        "product_count=VALUES(product_count), voc_count=VALUES(voc_count), "
        "insight_payload_json=VALUES(insight_payload_json), evidence_refs_json=VALUES(evidence_refs_json), "
        "updated_at=VALUES(updated_at)",
        (artifact_id, run_id, batch_id, market, category_key, confidence,
         json.dumps(covered_forms, ensure_ascii=False), product_count, voc_count,
         json.dumps(payload, ensure_ascii=False), json.dumps(evidence_refs, ensure_ascii=False), ts, ts),
    )


def persist_product_recommendation(db: DB, batch_id: str, insight_pack_id: Optional[str],
                                   market: str, category_key: str, reco: Dict[str, Any]) -> None:
    product_id = reco["product_id"]
    product_form = reco["product_form"] or "unknown"
    usecase = reco.get("usecase") or "general"
    recommendation_id = "{}__{}__{}__{}_{}".format(batch_id, product_id, product_form, usecase, DET_RECO_SUFFIX)
    ts = now_mysql_dt()
    primary = reco.get("primary_selling_points", [])
    secondary = reco.get("secondary_selling_points", [])
    risk_guards = reco.get("risk_guards", [])
    skipped = reco.get("skipped_insights", [])
    cur = db.cursor()
    cur.execute(
        "INSERT INTO fastmoss_voc_product_recommendation "
        "(recommendation_id, batch_id, insight_pack_id, product_id, market, category_key, "
        "quality_status, recommendation_status, primary_selling_points_json, risk_guards_json, "
        "skipped_insights_json, source_payload_json, created_at, updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE insight_pack_id=VALUES(insight_pack_id), market=VALUES(market), "
        "category_key=VALUES(category_key), quality_status=VALUES(quality_status), "
        "recommendation_status=VALUES(recommendation_status), primary_selling_points_json=VALUES(primary_selling_points_json), "
        "risk_guards_json=VALUES(risk_guards_json), skipped_insights_json=VALUES(skipped_insights_json), "
        "source_payload_json=VALUES(source_payload_json), updated_at=VALUES(updated_at)",
        (recommendation_id, batch_id, insight_pack_id, product_id, market, category_key,
         reco["quality_status"], "det_generated",
         json.dumps(primary + secondary, ensure_ascii=False),
         json.dumps(risk_guards, ensure_ascii=False),
         json.dumps(skipped, ensure_ascii=False),
         json.dumps(reco, ensure_ascii=False), ts, ts),
    )


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #

def run_pipeline(args: argparse.Namespace) -> Dict[str, Any]:
    scope = args.scope
    if scope is None:
        if args.product_id:
            scope = SCOPE_PRODUCT
        elif args.product_form:
            scope = SCOPE_FORM
        else:
            scope = SCOPE_CATEGORY

    started = now_iso()
    db = DB.connect(args.database_url)
    try:
        meta = load_batch_meta(db, args.batch_id)
        market = args.market or meta.get("market") or "UNKNOWN"
        category_key = args.category_key or meta.get("category_key") or "UNKNOWN"
        insight_pack_id = meta.get("insight_pack_id")
        classic_only = bool(meta.get("classic_only"))
        issues = load_flow_issues(db, args.batch_id)
        severe_issues = [i for i in issues if (i.get("severity") or "").lower() in ("high", "critical")]

        summaries = load_form_summaries(db, args.batch_id)
        form_confidences = {s["product_form"]: form_confidence(s["product_count"], s["voc_count"]) for s in summaries}
        # load enriched per form
        form_to_rows: Dict[str, List[Dict[str, Any]]] = {}
        for s in summaries:
            form_to_rows[s["product_form"]] = load_enriched(db, args.batch_id, product_form=s["product_form"])

        # ---- scope: form ----
        form_artifacts: List[Dict[str, Any]] = []
        if scope in (SCOPE_FORM, SCOPE_CATEGORY, SCOPE_PRODUCT):
            forms_to_build = [args.product_form] if scope == SCOPE_FORM and args.product_form else list(form_to_rows.keys())
            for form in forms_to_build:
                rows = form_to_rows.get(form) or load_enriched(db, args.batch_id, product_form=form)
                if not rows and form not in form_confidences:
                    # unknown form
                    continue
                s = next((x for x in summaries if x["product_form"] == form), None)
                pc = s["product_count"] if s else len({r["fastmoss_product_id"] for r in rows})
                vc = s["voc_count"] if s else len(rows)
                conf = form_confidences.get(form) or form_confidence(pc, vc)
                insights = generate_form_insights(rows, form,
                                                  (s or {}).get("product_form_label") or form,
                                                  conf, classic_only)
                ev_refs = []
                for ins in insights:
                    ev_refs.extend(ins["evidence_refs"])
                form_artifacts.append({
                    "product_form": form,
                    "confidence_level": conf,
                    "product_count": pc,
                    "voc_count": vc,
                    "insights": insights,
                    "evidence_refs": ev_refs,
                })

        # ---- scope: category ----
        category_artifact: Optional[Dict[str, Any]] = None
        if scope in (SCOPE_CATEGORY, SCOPE_PRODUCT):
            cat_insights, cat_summary = generate_category_insights(
                form_to_rows, form_confidences, category_key, classic_only)
            cat_ev_refs = []
            for ins in cat_insights:
                cat_ev_refs.extend(ins["evidence_refs"])
            # category confidence = best among category insights
            cat_conf = FORM_OBSERVE_ONLY
            for ins in cat_insights:
                if ins["confidence"] == CATEGORY_ADS_CANDIDATE:
                    cat_conf = CATEGORY_ADS_CANDIDATE
                    break
                if ins["confidence"] == CATEGORY_CANDIDATE:
                    cat_conf = CATEGORY_CANDIDATE
                    break
                if ins["confidence"] == FORM_PARTIAL and cat_conf == FORM_OBSERVE_ONLY:
                    cat_conf = FORM_PARTIAL
            category_artifact = {
                "confidence_level": cat_conf,
                "covered_forms": cat_summary["covered_forms"],
                "product_count": cat_summary["total_products"],
                "voc_count": cat_summary["total_voc"],
                "insights": cat_insights,
                "evidence_refs": cat_ev_refs,
                "classic_only": cat_summary["classic_only"],
            }

        # ---- scope: product ----
        product_reco: Optional[Dict[str, Any]] = None
        if scope == SCOPE_PRODUCT:
            if not args.product_id:
                raise RuntimeError("--product-id is required for --scope product")
            prod_rows = load_enriched(db, args.batch_id, product_id=args.product_id)
            if not prod_rows:
                # product may exist in snapshot but have no enriched VOC
                snap = load_product_snapshot(db, args.batch_id, args.product_id)
                if not snap:
                    raise RuntimeError("Product {} not found in batch {} (no snapshot, no VOC).".format(
                        args.product_id, args.batch_id))
                product_reco = {
                    "product_id": args.product_id,
                    "product_form": args.product_form or "",
                    "product_pack_type": "",
                    "usecase": args.usecase or "general",
                    "quality_status": "insufficient",
                    "coverage": {"form_confidence": FORM_OBSERVE_ONLY,
                                 "category_confidence": category_artifact["confidence_level"] if category_artifact else FORM_OBSERVE_ONLY,
                                 "source": "none"},
                    "primary_selling_points": [],
                    "secondary_selling_points": [],
                    "risk_guards": [],
                    "skipped_insights": [{
                        "insight_id": "no_voc",
                        "insight_type": "product_mismatch",
                        "title": "该商品在本批次无 VOC 证据",
                        "decision": "skip",
                        "reason": "no_enriched_voc: product has 0 enriched rows in this batch",
                        "confidence": FORM_OBSERVE_ONLY,
                        "scope": "product",
                        "scope_key": args.product_id,
                        "product_form": args.product_form or "",
                        "signal_tags": [],
                        "evidence_count": 0,
                        "product_count": 0,
                        "evidence_refs": [],
                        "evidence_examples": [],
                        "risk_notes": ["no_voc_evidence"],
                        "selling_point": "",
                    }],
                }
            else:
                pform, _ = product_form_and_pack(prod_rows)
                f_conf = form_confidences.get(pform, FORM_OBSERVE_ONLY)
                f_insights = []
                if pform in form_to_rows:
                    f_insights = next((a["insights"] for a in form_artifacts if a["product_form"] == pform), [])
                    if not f_insights:
                        rows = form_to_rows[pform]
                        s = next((x for x in summaries if x["product_form"] == pform), None)
                        conf = form_confidences.get(pform) or form_confidence(
                            s["product_count"] if s else len({r["fastmoss_product_id"] for r in rows}),
                            s["voc_count"] if s else len(rows))
                        f_insights = generate_form_insights(rows, pform,
                                                            (s or {}).get("product_form_label") or pform,
                                                            conf, classic_only)
                c_insights = category_artifact["insights"] if category_artifact else []
                c_conf = category_artifact["confidence_level"] if category_artifact else FORM_OBSERVE_ONLY
                product_reco = build_product_recommendation(
                    args.product_id, prod_rows, f_insights, c_insights,
                    args.usecase or "general", f_conf, c_conf)

        # ---- assemble result ----
        result: Dict[str, Any] = {
            "batch_id": args.batch_id,
            "market": market,
            "category_key": category_key,
            "scope": scope,
            "insight_pack_id": insight_pack_id,
            "batch_meta": {
                "pool_types": meta.get("pool_types", []),
                "classic_only": classic_only,
                "enriched_product_count": meta.get("enriched_product_count", 0),
                "enriched_voc_count": meta.get("enriched_voc_count", 0),
                "snapshot_product_count": meta.get("snapshot_product_count", 0),
                "severe_flow_issues": severe_issues,
            },
            "form_summaries": [{"product_form": s["product_form"], "label": s["product_form_label"],
                                "product_count": s["product_count"], "voc_count": s["voc_count"],
                                "confidence": form_confidences[s["product_form"]],
                                "top_signal_tags": s["top_signal_tags"][:5]} for s in summaries],
            "form_artifacts": form_artifacts if scope != SCOPE_FORM else [a for a in form_artifacts if a["product_form"] == args.product_form] if args.product_form else form_artifacts,
            "category_artifact": category_artifact,
            "product_recommendation": product_reco,
            "dry_run": not args.write,
        }

        # ---- persist ----
        run_id = "{}__{}__{}__{}".format(
            args.batch_id, scope,
            (args.product_form or args.product_id or "all"),
            started.replace(":", "").replace("-", ""))
        run_row = {
            "run_id": run_id, "batch_id": args.batch_id, "market": market,
            "category_key": category_key, "scope": scope,
            "product_form": args.product_form, "product_id": args.product_id,
            "usecase": args.usecase,
            "params_json": json.dumps(vars(args), ensure_ascii=False, default=str),
            "status": "running", "started_at": started, "finished_at": None,
            "summary_json": None, "error_message": None,
            "created_at": started, "updated_at": started,
        }
        summary = {
            "forms": len(form_artifacts),
            "category_insights": len(category_artifact["insights"]) if category_artifact else 0,
            "product_reco_quality": product_reco["quality_status"] if product_reco else None,
            "classic_only": classic_only,
            "severe_issues": len(severe_issues),
        }
        if args.write:
            db.ensure_tables()
            persist_run(db, run_row)
            if scope in (SCOPE_FORM, SCOPE_CATEGORY):
                for a in form_artifacts:
                    persist_form_artifact(db, run_id, args.batch_id, market, category_key,
                                          a["product_form"], a["confidence_level"], a["product_count"],
                                          a["voc_count"], a["insights"], a["evidence_refs"])
            if scope == SCOPE_CATEGORY and category_artifact:
                persist_category_artifact(db, run_id, args.batch_id, market, category_key,
                                          category_artifact["confidence_level"],
                                          category_artifact["covered_forms"],
                                          category_artifact["product_count"],
                                          category_artifact["voc_count"],
                                          category_artifact["insights"],
                                          category_artifact["evidence_refs"])
            if product_reco:
                persist_product_recommendation(db, args.batch_id, insight_pack_id, market,
                                               category_key, product_reco)
            run_row["status"] = "completed"
            run_row["finished_at"] = now_iso()
            run_row["summary_json"] = json.dumps(summary, ensure_ascii=False)
            persist_run(db, run_row)
            db.commit()
            result["run_id"] = run_id
            result["written"] = True
        else:
            result["run_id"] = run_id
            result["written"] = False
        result["summary"] = summary
        return result
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        raise
    finally:
        db.close()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="voc-insight deterministic MVP")
    p.add_argument("--batch-id", required=True, help="Fastmoss VOC batch id, e.g. FM_TH_HAIRCLIP_20260622_165607")
    p.add_argument("--market", default=None, help="e.g. TH (auto-detected if omitted)")
    p.add_argument("--category-key", default=None, help="e.g. hair_clip (auto-detected if omitted)")
    p.add_argument("--product-form", default=None, help="e.g. basic_hair_clip")
    p.add_argument("--scope", default=None, choices=[SCOPE_CATEGORY, SCOPE_FORM, SCOPE_PRODUCT],
                   help="category|form|product (inferred if omitted)")
    p.add_argument("--product-id", default=None, help="fastmoss_product_id for product scope")
    p.add_argument("--usecase", default=None, choices=ALL_USECASES, help="ads_mixcut|creator_brief|content_copy|selection")
    p.add_argument("--write", action="store_true", help="persist to RDS (creates tables, writes artifacts/reco)")
    p.add_argument("--sync-feishu-task", action="store_true", help="(stub) sync to Feishu task table")
    p.add_argument("--dry-run", action="store_true", help="no DB writes (default when --write absent)")
    p.add_argument("--database-url", default=None, help="override DB URL")
    p.add_argument("--pretty", action="store_true", help="pretty-print JSON output")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.scope == SCOPE_PRODUCT and not args.product_id:
        print("--product-id is required for --scope product", file=sys.stderr)
        return 2
    if args.scope == SCOPE_FORM and not args.product_form:
        print("--product-form is recommended for --scope form (will build all forms otherwise)", file=sys.stderr)
    try:
        result = run_pipeline(args)
    except Exception as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        return 1
    indent = 2 if args.pretty else None
    print(json.dumps(result, ensure_ascii=False, indent=indent, default=str))
    if args.sync_feishu_task:
        print("--sync-feishu-task is a stub in MVP; not implemented yet.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
