from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from auto_mixcut.core.result import Result

from .context import SkillContext
from .material_usage_ledger_skill import ensure_material_usage_tables
from .usage_counter_skill import is_good_rendered_output


CORE_ROLES = {"hero", "detail", "result"}
BLOCKED = "duplicate_blocked"
REVIEW = "similarity_review"
PASS = "pass"


@dataclass(frozen=True)
class SimilarityDecision:
    decision: str
    similarity_level: str
    reason: dict[str, Any]
    compared_output_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "similarity_level": self.similarity_level,
            "reason": self.reason,
            "compared_output_id": self.compared_output_id,
        }


class OutputSimilaritySkill:
    """Structure-level duplicate gate for rendered mixcut outputs."""

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def check_output(self, output_id: str) -> Result:
        output = self.ctx.repo.get("outputs", "output_id", output_id)
        if not output:
            return Result.fail("OUTPUT_NOT_FOUND", "output not found", {"output_id": output_id})
        ensure = ensure_material_usage_tables(self.ctx)
        if not ensure.success:
            return ensure
        product_id = str(output.get("product_id") or "")
        current_slots = _slots(self.ctx, output_id)
        if not current_slots:
            return Result.ok({"output_id": output_id, "decision": PASS, "reason": "no_output_segments"})

        comparisons = []
        for previous in _previous_good_outputs(self.ctx, output):
            previous_slots = _slots(self.ctx, str(previous.get("output_id") or ""))
            if not previous_slots:
                continue
            metrics = _structure_metrics(output, current_slots, previous, previous_slots)
            decision = _decide(metrics)
            _persist_similarity(self.ctx, output, previous, metrics, decision)
            comparisons.append({"compared_output_id": previous.get("output_id"), **metrics, **decision.to_dict()})

        worst = _worst_decision(comparisons)
        return Result.ok({"output_id": output_id, "decision": worst.decision, "similarity_level": worst.similarity_level, "reason": worst.reason, "comparisons": comparisons})


def _previous_good_outputs(ctx: SkillContext, output: dict[str, Any]) -> list[dict[str, Any]]:
    product_id = str(output.get("product_id") or "")
    output_id = str(output.get("output_id") or "")
    current_created_at = str(output.get("created_at") or "")
    current_id = _int(output.get("id"))
    rows = ctx.repo.list_where("outputs", "product_id=? ORDER BY created_at, id", (product_id,))
    return [
        row
        for row in rows
        if str(row.get("output_id") or "") != output_id
        and is_good_rendered_output(row)
        and _is_earlier_output(row, current_created_at, current_id)
    ]


def _is_earlier_output(row: dict[str, Any], current_created_at: str, current_id: int) -> bool:
    row_created_at = str(row.get("created_at") or "")
    if current_created_at and row_created_at:
        if row_created_at != current_created_at:
            return row_created_at < current_created_at
        return _int(row.get("id")) < current_id
    if current_id > 0:
        return _int(row.get("id")) < current_id
    return True


def _slots(ctx: SkillContext, output_id: str) -> list[dict[str, Any]]:
    return ctx.repo.list_where("output_segments", "output_id=? ORDER BY slot_index", (output_id,))


def _structure_metrics(
    output: dict[str, Any],
    current_slots: list[dict[str, Any]],
    previous: dict[str, Any],
    previous_slots: list[dict[str, Any]],
) -> dict[str, Any]:
    current_segments = _ids(current_slots, "segment_id")
    previous_segments = _ids(previous_slots, "segment_id")
    current_assets = _ids(current_slots, "asset_id")
    previous_assets = _ids(previous_slots, "asset_id")
    current_core = _core_segment_ids(current_slots)
    previous_core = _core_segment_ids(previous_slots)
    first = current_slots[0] if current_slots else {}
    previous_first = previous_slots[0] if previous_slots else {}
    return {
        "same_first_segment": bool(first.get("segment_id") and first.get("segment_id") == previous_first.get("segment_id")),
        "same_first_asset": bool(first.get("asset_id") and first.get("asset_id") == previous_first.get("asset_id")),
        "same_template": bool(output.get("template_id") and output.get("template_id") == previous.get("template_id")),
        "segment_overlap_ratio": _overlap_ratio(current_segments, previous_segments),
        "core_segment_overlap_ratio": _overlap_ratio(current_core, previous_core),
        "asset_overlap_ratio": _overlap_ratio(current_assets, previous_assets),
        "core_overlap_count": len(set(current_core).intersection(previous_core)),
    }


def _decide(metrics: dict[str, Any]) -> SimilarityDecision:
    reasons: list[str] = []
    if metrics.get("same_first_segment"):
        reasons.append("same_first_segment")
    if float(metrics.get("segment_overlap_ratio") or 0) >= 0.6:
        reasons.append("segment_overlap_ratio>=0.6")
    if reasons:
        return SimilarityDecision(BLOCKED, "high", {"reasons": reasons, "metrics": metrics})

    review_reasons: list[str] = []
    if metrics.get("same_first_asset") and metrics.get("same_template"):
        review_reasons.append("same_first_asset_and_template")
    if 0.4 <= float(metrics.get("segment_overlap_ratio") or 0) < 0.6:
        review_reasons.append("segment_overlap_ratio>=0.4")
    if float(metrics.get("core_segment_overlap_ratio") or 0) >= 0.5:
        review_reasons.append("core_segment_overlap_ratio>=0.5")
    if metrics.get("same_template") and int(metrics.get("core_overlap_count") or 0) >= 2:
        review_reasons.append("same_template_core_overlap>=2")
    if review_reasons:
        return SimilarityDecision(REVIEW, "medium", {"reasons": review_reasons, "metrics": metrics})
    return SimilarityDecision(PASS, "low", {"reasons": [], "metrics": metrics})


def _persist_similarity(
    ctx: SkillContext,
    output: dict[str, Any],
    previous: dict[str, Any],
    metrics: dict[str, Any],
    decision: SimilarityDecision,
) -> None:
    output_id = str(output.get("output_id") or "")
    previous_id = str(previous.get("output_id") or "")
    if not output_id or not previous_id:
        return
    ctx.repo.upsert(
        "mixcut_output_similarity",
        "comparison_id",
        {
            "comparison_id": f"{output_id}:{previous_id}",
            "output_id": output_id,
            "compared_output_id": previous_id,
            "product_id": output.get("product_id"),
            "same_first_segment": 1 if metrics.get("same_first_segment") else 0,
            "same_first_asset": 1 if metrics.get("same_first_asset") else 0,
            "same_template": 1 if metrics.get("same_template") else 0,
            "segment_overlap_ratio": round(float(metrics.get("segment_overlap_ratio") or 0), 4),
            "core_segment_overlap_ratio": round(float(metrics.get("core_segment_overlap_ratio") or 0), 4),
            "asset_overlap_ratio": round(float(metrics.get("asset_overlap_ratio") or 0), 4),
            "similarity_level": decision.similarity_level,
            "decision": decision.decision,
            "reason_json": decision.reason,
            "updated_at": _now(),
        },
    )


def _worst_decision(comparisons: list[dict[str, Any]]) -> SimilarityDecision:
    rank = {PASS: 0, REVIEW: 1, BLOCKED: 2}
    if not comparisons:
        return SimilarityDecision(PASS, "low", {"reasons": []})
    worst = max(comparisons, key=lambda item: rank.get(str(item.get("decision") or PASS), 0))
    return SimilarityDecision(
        str(worst.get("decision") or PASS),
        str(worst.get("similarity_level") or "low"),
        worst.get("reason") or {},
        str(worst.get("compared_output_id") or ""),
    )


def _ids(slots: list[dict[str, Any]], key: str) -> list[str]:
    return [str(slot.get(key) or "") for slot in slots if str(slot.get(key) or "")]


def _core_segment_ids(slots: list[dict[str, Any]]) -> list[str]:
    return [str(slot.get("segment_id") or "") for slot in slots if str(slot.get("segment_id") or "") and str(slot.get("role_used") or "") in CORE_ROLES]


def _overlap_ratio(left: list[str], right: list[str]) -> float:
    if not left or not right:
        return 0.0
    left_set = set(left)
    right_set = set(right)
    denominator = max(1, min(len(left_set), len(right_set)))
    return len(left_set.intersection(right_set)) / denominator


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")
