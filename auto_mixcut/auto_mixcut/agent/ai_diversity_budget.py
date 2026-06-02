from __future__ import annotations

from typing import Any

import yaml

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result
from auto_mixcut.skills.context import SkillContext


QUALITY_READY_STATUSES = {"publish_ready", "needs_review", "passed", "passed_with_warning"}
HUMAN_READY_STATUSES = {"approved", "published"}
REAL_SOURCE_TYPES = {"self_shot", "authorized_creator", "supplier"}
CORE_ROLES = {"hero", "detail", "result"}


class AIDiversityBudget:
    """Lightweight per-product AI ratio guardrail.

    pHash and completion decay are optional signals: they are used when data is
    available and skipped when missing.
    """

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx
        self.config = _budget_config(ctx)

    def evaluate(self, product_id: str) -> Result:
        ensured = _ensure_budget_tables(self.ctx)
        if not ensured.success:
            return ensured
        published_count = _published_like_output_count(self.ctx, product_id)
        base_phase = _phase_for_count(published_count, self.config)
        phash_signal = _phash_cluster_signal(self.ctx, product_id, self.config)
        completion_signal = _completion_decay_signal(self.ctx, product_id, self.config)
        phase = _apply_demotions(base_phase, [phash_signal, completion_signal])
        ai_ratio_cap = float((self.config.get("ai_ratio_cap") or {}).get(phase, 0.0))
        trusted_anchors = _trusted_real_anchor_count(self.ctx, product_id)
        retire_cfg = self.config.get("retire") or {}
        min_anchors = int(retire_cfg.get("min_trust_anchors_required") or 0)
        warning = None
        if phase == "mature" and trusted_anchors < min_anchors:
            warning = {
                "product_id": product_id,
                "reason": "diversity_exhausted_real_shortage",
                "trusted_real_anchor_count": trusted_anchors,
                "min_trust_anchors_required": min_anchors,
                "action": str(retire_cfg.get("on_exhausted") or "retire_candidate"),
            }
            _apply_retire_candidate(self.ctx, product_id, phase, ai_ratio_cap, trusted_anchors, warning)
        return Result.ok(
            {
                "product_id": product_id,
                "published_like_output_count": published_count,
                "base_phase": base_phase,
                "phase": phase,
                "ai_ratio_cap": ai_ratio_cap,
                "trusted_real_anchor_count": trusted_anchors,
                "phash_signal": phash_signal,
                "completion_signal": completion_signal,
                "warning": warning,
                "budget_json": {
                    "base_phase": base_phase,
                    "phase": phase,
                    "ai_ratio_cap": ai_ratio_cap,
                    "published_like_output_count": published_count,
                    "trusted_real_anchor_count": trusted_anchors,
                    "phash_signal": phash_signal,
                    "completion_signal": completion_signal,
                },
            }
        )


def _budget_config(ctx: SkillContext) -> dict[str, Any]:
    path = ctx.settings.root_dir / "config" / "ai_segment_factory.yaml"
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        data = {}
    budget = dict(data.get("diversity_budget") or {})
    budget.setdefault("thresholds", {"n1_cold_to_ramp": 30, "n2_ramp_to_mature": 80})
    budget.setdefault("ai_ratio_cap", {"cold": 0.8, "ramp": 0.5, "mature": 0.3})
    budget.setdefault("early_demote_signals", {"phash_cluster_threshold": 6, "completion_decay_ratio": 0.7, "completion_recent_k": 10, "completion_min_samples": 5})
    budget.setdefault("retire", {"min_trust_anchors_required": 1, "on_exhausted": "retire_candidate"})
    return budget


def _ensure_budget_tables(ctx: SkillContext) -> Result:
    if getattr(ctx.repo, "dialect", "sqlite") == "mysql":
        fingerprint_sql = """
            CREATE TABLE IF NOT EXISTS segment_visual_fingerprints (
              id BIGINT PRIMARY KEY AUTO_INCREMENT,
              fingerprint_id VARCHAR(128) NOT NULL UNIQUE,
              product_id VARCHAR(128),
              segment_id VARCHAR(128),
              source_type VARCHAR(64),
              phash VARCHAR(64),
              hash_method VARCHAR(64),
              frame_count INT,
              created_at DATETIME,
              updated_at DATETIME
            )
        """
        alerts_sql = """
            CREATE TABLE IF NOT EXISTS ai_diversity_alerts (
              id BIGINT PRIMARY KEY AUTO_INCREMENT,
              alert_id VARCHAR(128) NOT NULL UNIQUE,
              product_id VARCHAR(128) NOT NULL,
              reason VARCHAR(256),
              phase VARCHAR(32),
              ai_ratio_cap DECIMAL(5, 3),
              trusted_real_anchor_count INT,
              alert_json JSON,
              status VARCHAR(64),
              created_at DATETIME,
              updated_at DATETIME
            )
        """
        try:
            with ctx.repo.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(fingerprint_sql)
                    cur.execute(alerts_sql)
            return Result.ok()
        except Exception as exc:
            return Result.fail("AI_DIVERSITY_TABLE_FAILED", str(exc))
    try:
        with ctx.repo.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS segment_visual_fingerprints (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  fingerprint_id TEXT NOT NULL UNIQUE,
                  product_id TEXT,
                  segment_id TEXT,
                  source_type TEXT,
                  phash TEXT,
                  hash_method TEXT,
                  frame_count INTEGER,
                  created_at TEXT,
                  updated_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_diversity_alerts (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  alert_id TEXT NOT NULL UNIQUE,
                  product_id TEXT NOT NULL,
                  reason TEXT,
                  phase TEXT,
                  ai_ratio_cap REAL,
                  trusted_real_anchor_count INTEGER,
                  alert_json TEXT,
                  status TEXT,
                  created_at TEXT,
                  updated_at TEXT
                )
                """
            )
        return Result.ok()
    except Exception as exc:
        return Result.fail("AI_DIVERSITY_TABLE_FAILED", str(exc))


def _phase_for_count(count: int, config: dict[str, Any]) -> str:
    thresholds = config.get("thresholds") or {}
    n1 = int(thresholds.get("n1_cold_to_ramp") or 0)
    n2 = int(thresholds.get("n2_ramp_to_mature") or n1)
    if count <= n1:
        return "cold"
    if count <= n2:
        return "ramp"
    return "mature"


def _published_like_output_count(ctx: SkillContext, product_id: str) -> int:
    outputs = ctx.repo.list_where("outputs", "product_id=?", (product_id,))
    count = 0
    for output in outputs:
        machine = str(output.get("machine_quality_status") or "")
        human = str(output.get("human_quality_status") or "")
        if machine in QUALITY_READY_STATUSES or human in HUMAN_READY_STATUSES:
            count += 1
    return count


def _trusted_real_anchor_count(ctx: SkillContext, product_id: str) -> int:
    segments = ctx.repo.list_where("segments", "product_id=?", (product_id,))
    anchors = set()
    for segment in segments:
        if segment.get("source_type") not in REAL_SOURCE_TYPES:
            continue
        if segment.get("source_trust_level") not in {"high", "medium"}:
            continue
        if segment.get("product_match_status") not in {"trusted_by_source", "anchor_pass"}:
            continue
        if not CORE_ROLES.intersection(segment.get("effective_roles_json") or []):
            continue
        asset = ctx.repo.get("assets", "asset_id", segment.get("asset_id")) if segment.get("asset_id") else None
        source_identity = str((asset or {}).get("source_identity") or "").strip()
        scene_tag = str((asset or {}).get("scene_tag") or "").strip()
        if source_identity or scene_tag:
            anchors.add((source_identity or str(segment.get("asset_id")), scene_tag or "unknown_scene"))
        else:
            anchors.add((str(segment.get("asset_id")), "asset"))
    return len(anchors)


def _apply_demotions(base_phase: str, signals: list[dict[str, Any]]) -> str:
    order = ["cold", "ramp", "mature"]
    index = order.index(base_phase)
    demotions = sum(1 for signal in signals if signal.get("triggered"))
    return order[min(len(order) - 1, index + demotions)]


def _phash_cluster_signal(ctx: SkillContext, product_id: str, config: dict[str, Any]) -> dict[str, Any]:
    signal_cfg = config.get("early_demote_signals") or {}
    threshold = int(signal_cfg.get("phash_cluster_threshold") or 0)
    hashes = _ai_phashes(ctx, product_id)
    if threshold <= 0 or len(hashes) < 2:
        return {"available": False, "triggered": False, "reason": "insufficient_phash", "sample_count": len(hashes)}
    distances = []
    for idx, left in enumerate(hashes):
        for right in hashes[idx + 1:]:
            distances.append(_hamming(left, right))
    median = _median(distances)
    return {
        "available": True,
        "triggered": median < threshold,
        "median_hamming_distance": median,
        "threshold": threshold,
        "sample_count": len(hashes),
    }


def _completion_decay_signal(ctx: SkillContext, product_id: str, config: dict[str, Any]) -> dict[str, Any]:
    signal_cfg = config.get("early_demote_signals") or {}
    ratio_threshold = float(signal_cfg.get("completion_decay_ratio") or 0)
    recent_k = int(signal_cfg.get("completion_recent_k") or 10)
    min_samples = int(signal_cfg.get("completion_min_samples") or 1)
    rows = ctx.repo.list_where("outputs", "product_id=? AND avg_completion_rate IS NOT NULL ORDER BY COALESCE(published_at, updated_at, created_at) DESC", (product_id,))
    rates = [float(row.get("avg_completion_rate") or 0) for row in rows if row.get("avg_completion_rate") is not None]
    if ratio_threshold <= 0 or len(rates) < min_samples:
        return {"available": False, "triggered": False, "reason": "insufficient_completion", "sample_count": len(rates)}
    recent = rates[:recent_k]
    recent_avg = sum(recent) / len(recent)
    peak = max(rates)
    ratio = recent_avg / peak if peak > 0 else 1.0
    return {
        "available": True,
        "triggered": ratio < ratio_threshold,
        "recent_avg_completion_rate": recent_avg,
        "peak_completion_rate": peak,
        "decay_ratio": ratio,
        "threshold": ratio_threshold,
        "sample_count": len(rates),
    }


def _ai_phashes(ctx: SkillContext, product_id: str) -> list[str]:
    rows = ctx.repo.list_where("segment_visual_fingerprints", "product_id=? AND source_type='ai_generated' AND phash IS NOT NULL", (product_id,))
    hashes = [str(row.get("phash") or "").strip() for row in rows if str(row.get("phash") or "").strip()]
    if hashes:
        return hashes
    segments = ctx.repo.list_where("segments", "product_id=? AND source_type='ai_generated' AND visual_phash IS NOT NULL", (product_id,))
    return [str(row.get("visual_phash") or "").strip() for row in segments if str(row.get("visual_phash") or "").strip()]


def _hamming(left: str, right: str) -> int:
    left_bits = _hash_bits(left)
    right_bits = _hash_bits(right)
    if not left_bits or not right_bits:
        return 9999
    length = min(len(left_bits), len(right_bits))
    return sum(1 for idx in range(length) if left_bits[idx] != right_bits[idx]) + abs(len(left_bits) - len(right_bits))


def _hash_bits(value: str) -> str:
    text = str(value).strip().lower()
    if not text:
        return ""
    try:
        number = int(text, 16)
        return bin(number)[2:].zfill(len(text) * 4)
    except ValueError:
        return "".join(format(ord(ch), "08b") for ch in text)


def _median(values: list[int]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[mid])
    return (ordered[mid - 1] + ordered[mid]) / 2


def _apply_retire_candidate(ctx: SkillContext, product_id: str, phase: str, ai_ratio_cap: float, trusted_anchors: int, warning: dict[str, Any]) -> None:
    ctx.repo.update("products", "product_id", product_id, {"product_status": "retire_candidate"})
    alert_id = new_id("AIDIV")
    ctx.repo.upsert(
        "ai_diversity_alerts",
        "alert_id",
        {
            "alert_id": alert_id,
            "product_id": product_id,
            "reason": warning["reason"],
            "phase": phase,
            "ai_ratio_cap": ai_ratio_cap,
            "trusted_real_anchor_count": trusted_anchors,
            "alert_json": warning,
            "status": "open",
        },
    )
