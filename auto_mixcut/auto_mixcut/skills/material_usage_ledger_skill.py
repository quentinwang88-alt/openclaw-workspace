from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from auto_mixcut.core.result import Result
from auto_mixcut.domain.source_types import SourceType, TRUSTED_REAL_SOURCE_TYPES

from .context import SkillContext
from .material_policy_skill import PUBLISHED_RESULT_VALUES
from .usage_counter_skill import is_good_rendered_output, is_rejected_rendered_output


SEGMENT_TOTAL_CAP_DEFAULT = 2
SEGMENT_TOTAL_CAP_REAL = 3
FIRST_SEGMENT_CAP = 1
ASSET_TOTAL_CAP_DEFAULT = 4
ASSET_TOTAL_CAP_REAL = 6
FIRST_ASSET_CAP = 2


@dataclass(frozen=True)
class UsageLedgerDecision:
    allowed: bool
    block_reasons: tuple[str, ...]
    usage_risk_level: str
    segment_good_cap: int
    asset_good_cap: int
    segment_usage: dict[str, Any]
    asset_usage: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "block_reasons": list(self.block_reasons),
            "usage_risk_level": self.usage_risk_level,
            "segment_good_cap": self.segment_good_cap,
            "asset_good_cap": self.asset_good_cap,
            "segment_usage": self.segment_usage,
            "asset_usage": self.asset_usage,
        }


class MaterialUsageLedgerSkill:
    """Product-scoped material usage ledger for ADS mixcut reuse control."""

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def ensure_tables(self) -> Result:
        return ensure_material_usage_tables(self.ctx)

    def refresh_product(self, product_id: str) -> Result:
        product_id = str(product_id or "").strip()
        if not product_id:
            return Result.fail("PRODUCT_ID_REQUIRED", "product_id is required")
        ensured = self.ensure_tables()
        if not ensured.success:
            return ensured
        return refresh_product_usage_snapshot(self.ctx, product_id)

    def product_snapshot(self, product_id: str, *, refresh: bool = False) -> dict[str, dict[str, Any]]:
        product_id = str(product_id or "").strip()
        if refresh:
            self.refresh_product(product_id)
        else:
            self.ensure_tables()
        segment_rows = self.ctx.repo.list_where("mixcut_segment_usage_snapshot", "product_id=?", (product_id,))
        asset_rows = self.ctx.repo.list_where("mixcut_asset_usage_snapshot", "product_id=?", (product_id,))
        return {
            "segments": {str(row.get("segment_id") or ""): row for row in segment_rows},
            "assets": {str(row.get("asset_id") or ""): row for row in asset_rows},
        }

    def evaluate_segment(
        self,
        segment: dict[str, Any],
        *,
        segment_usage: dict[str, Any] | None = None,
        asset_usage: dict[str, Any] | None = None,
        slot_index: int = 0,
        role: str = "",
    ) -> dict[str, Any]:
        segment_usage = segment_usage or {}
        asset_usage = asset_usage or {}
        return evaluate_usage_ledger_policy(
            segment,
            segment_usage=segment_usage,
            asset_usage=asset_usage,
            slot_index=slot_index,
            role=role,
        ).to_dict()


def ensure_material_usage_tables(ctx: SkillContext) -> Result:
    mysql = getattr(ctx.repo, "dialect", "") == "mysql"
    statements = _mysql_statements() if mysql else _sqlite_statements()
    try:
        with ctx.repo.connect() as conn:
            if mysql:
                with conn.cursor() as cur:
                    for statement in statements:
                        cur.execute(statement)
            else:
                for statement in statements:
                    conn.execute(statement)
        return Result.ok({"tables": ["mixcut_segment_usage_snapshot", "mixcut_asset_usage_snapshot", "mixcut_output_similarity"]})
    except Exception as exc:
        return Result.fail("USAGE_LEDGER_TABLE_INIT_FAILED", str(exc))


def refresh_product_usage_snapshot(ctx: SkillContext, product_id: str) -> Result:
    product_id = str(product_id or "").strip()
    if not product_id:
        return Result.fail("PRODUCT_ID_REQUIRED", "product_id is required")
    ensure = ensure_material_usage_tables(ctx)
    if not ensure.success:
        return ensure

    segments = ctx.repo.list_where("segments", "product_id=?", (product_id,))
    outputs = ctx.repo.list_where("outputs", "product_id=?", (product_id,))
    output_by_id = {str(output.get("output_id") or ""): output for output in outputs}
    output_ids = [output_id for output_id in output_by_id if output_id]
    output_segments = _output_segments_for_outputs(ctx, output_ids)
    plans = ctx.repo.list_where("render_plans", "product_id=?", (product_id,))

    segment_counts: dict[str, Counter[str]] = {}
    asset_counts: dict[str, Counter[str]] = {}
    segment_meta: dict[str, dict[str, Any]] = {}
    asset_meta: dict[str, dict[str, Any]] = {}

    for segment in segments:
        segment_id = str(segment.get("segment_id") or "")
        asset_id = str(segment.get("asset_id") or "")
        if not segment_id:
            continue
        segment_counts.setdefault(segment_id, Counter())
        segment_meta[segment_id] = segment
        if asset_id:
            asset_counts.setdefault(asset_id, Counter())
            asset_meta.setdefault(asset_id, _asset_for_segment(ctx, segment))

    _accumulate_planned_counts(plans, segment_counts, asset_counts)
    _accumulate_output_counts(output_segments, output_by_id, segment_counts, asset_counts)

    now = _now()
    segment_rows = 0
    for segment_id, segment in segment_meta.items():
        asset_id = str(segment.get("asset_id") or "")
        counts = segment_counts.get(segment_id, Counter())
        asset_counts_for_segment = asset_counts.get(asset_id, Counter())
        decision = evaluate_usage_ledger_policy(segment, segment_usage=counts, asset_usage=asset_counts_for_segment)
        write = ctx.repo.upsert(
            "mixcut_segment_usage_snapshot",
            "segment_id",
            {
                "segment_id": segment_id,
                "product_id": product_id,
                "asset_id": asset_id,
                "source_type": segment.get("source_type"),
                "source_trust_level": segment.get("source_trust_level"),
                **_counter_row(counts),
                "ads_eligible": 1 if decision.allowed else 0,
                "ads_block_reason": ",".join(decision.block_reasons),
                "usage_risk_level": decision.usage_risk_level,
                "updated_at": now,
            },
        )
        if not write.success:
            return write
        segment_rows += 1

    asset_rows = 0
    for asset_id, counts in asset_counts.items():
        asset = asset_meta.get(asset_id) or {}
        decision = evaluate_usage_ledger_policy(asset, segment_usage={}, asset_usage=counts)
        write = ctx.repo.upsert(
            "mixcut_asset_usage_snapshot",
            "asset_id",
            {
                "asset_id": asset_id,
                "product_id": product_id,
                "source_type": asset.get("source_type"),
                "source_trust_level": asset.get("source_trust_level"),
                "segment_count": sum(1 for segment in segments if str(segment.get("asset_id") or "") == asset_id),
                **_counter_row(counts),
                "ads_eligible": 1 if decision.allowed else 0,
                "ads_block_reason": ",".join(decision.block_reasons),
                "usage_risk_level": decision.usage_risk_level,
                "updated_at": now,
            },
        )
        if not write.success:
            return write
        asset_rows += 1

    return Result.ok({"product_id": product_id, "segment_rows": segment_rows, "asset_rows": asset_rows})


def evaluate_usage_ledger_policy(
    segment: dict[str, Any],
    *,
    segment_usage: dict[str, Any] | Counter[str] | None = None,
    asset_usage: dict[str, Any] | Counter[str] | None = None,
    slot_index: int = 0,
    role: str = "",
) -> UsageLedgerDecision:
    segment_usage = dict(segment_usage or {})
    asset_usage = dict(asset_usage or {})
    source_type = str(segment.get("source_type") or asset_usage.get("source_type") or "").strip()
    first_slot = _is_first_slot(slot_index, role)
    segment_cap = _segment_good_cap(source_type)
    asset_cap = _asset_good_cap(source_type)
    reasons: list[str] = []

    if _int(segment_usage.get("published_output_count")) > 0:
        reasons.append("segment_already_published")
    if first_slot and _int(segment_usage.get("first_slot_published_count")) > 0:
        reasons.append("segment_first_slot_already_published")
    if source_type not in TRUSTED_REAL_SOURCE_TYPES and _int(asset_usage.get("published_output_count")) > 0:
        reasons.append("asset_already_published_low_trust")
    if first_slot and _int(asset_usage.get("first_slot_published_count")) > 0:
        reasons.append("asset_first_slot_already_published")
    if first_slot and _int(segment_usage.get("first_slot_good_count")) >= FIRST_SEGMENT_CAP:
        reasons.append("first_slot_segment_usage_cap")
    if _int(segment_usage.get("good_output_count")) >= segment_cap:
        reasons.append("segment_good_usage_cap")
    if first_slot and _int(asset_usage.get("first_slot_good_count")) >= FIRST_ASSET_CAP:
        reasons.append("first_slot_asset_usage_cap")
    if _int(asset_usage.get("good_output_count")) >= asset_cap:
        reasons.append("asset_good_usage_cap")

    risk = "low"
    if reasons:
        risk = "blocked"
    elif _int(segment_usage.get("good_output_count")) >= max(0, segment_cap - 1):
        risk = "medium"
    elif _int(asset_usage.get("good_output_count")) >= max(0, asset_cap - 1):
        risk = "medium"
    return UsageLedgerDecision(
        allowed=not reasons,
        block_reasons=tuple(reasons),
        usage_risk_level=risk,
        segment_good_cap=segment_cap,
        asset_good_cap=asset_cap,
        segment_usage=segment_usage,
        asset_usage=asset_usage,
    )


def _accumulate_planned_counts(
    plans: list[dict[str, Any]],
    segment_counts: dict[str, Counter[str]],
    asset_counts: dict[str, Counter[str]],
) -> None:
    for plan in plans:
        status = str(plan.get("render_status") or "").strip().lower()
        if status in {"aborted", "aborted_stale_planning", "failed", "render_timeout"}:
            continue
        for slot in ((plan.get("plan_json") or {}).get("segments") or []):
            segment_id = str(slot.get("segment_id") or "")
            asset_id = str(slot.get("asset_id") or "")
            if not segment_id:
                continue
            _inc(segment_counts, segment_id, "planned_count")
            if _is_first_slot(slot.get("slot") or 0, slot.get("role") or ""):
                _inc(segment_counts, segment_id, "first_slot_planned_count")
            if asset_id:
                _inc(asset_counts, asset_id, "planned_count")
                if _is_first_slot(slot.get("slot") or 0, slot.get("role") or ""):
                    _inc(asset_counts, asset_id, "first_slot_planned_count")


def _accumulate_output_counts(
    output_segments: list[dict[str, Any]],
    output_by_id: dict[str, dict[str, Any]],
    segment_counts: dict[str, Counter[str]],
    asset_counts: dict[str, Counter[str]],
) -> None:
    for row in output_segments:
        output = output_by_id.get(str(row.get("output_id") or "")) or {}
        segment_id = str(row.get("segment_id") or "")
        asset_id = str(row.get("asset_id") or "")
        if not segment_id:
            continue
        first_slot = _is_first_slot(row.get("slot_index") or 0, row.get("role_used") or "")
        rendered = str(output.get("render_status") or "") == "rendered"
        good = is_good_rendered_output(output)
        rejected = is_rejected_rendered_output(output)
        published = _is_published_output(output)
        draft = str(output.get("machine_quality_status") or "") in {"draft_only", "similarity_review", "duplicate_blocked", "failed"}

        for counts in _row_counters(segment_counts, asset_counts, segment_id, asset_id):
            if rendered:
                counts["rendered_count"] += 1
            if good:
                counts["good_output_count"] += 1
                if first_slot:
                    counts["first_slot_good_count"] += 1
            if draft:
                counts["draft_output_count"] += 1
            if rejected:
                counts["rejected_output_count"] += 1
            if published:
                counts["published_output_count"] += 1
                if first_slot:
                    counts["first_slot_published_count"] += 1


def _output_segments_for_outputs(ctx: SkillContext, output_ids: list[str]) -> list[dict[str, Any]]:
    if not output_ids:
        return []
    rows: list[dict[str, Any]] = []
    for chunk in _chunks(output_ids, 500):
        placeholders = ",".join(["?"] * len(chunk))
        rows.extend(ctx.repo.list_where("output_segments", f"output_id IN ({placeholders})", tuple(chunk)))
    return rows


def _row_counters(
    segment_counts: dict[str, Counter[str]],
    asset_counts: dict[str, Counter[str]],
    segment_id: str,
    asset_id: str,
) -> list[Counter[str]]:
    counters = [segment_counts.setdefault(segment_id, Counter())]
    if asset_id:
        counters.append(asset_counts.setdefault(asset_id, Counter()))
    return counters


def _counter_row(counts: dict[str, Any] | Counter[str]) -> dict[str, Any]:
    return {
        "planned_count": _int(counts.get("planned_count")),
        "rendered_count": _int(counts.get("rendered_count")),
        "good_output_count": _int(counts.get("good_output_count")),
        "draft_output_count": _int(counts.get("draft_output_count")),
        "rejected_output_count": _int(counts.get("rejected_output_count")),
        "first_slot_planned_count": _int(counts.get("first_slot_planned_count")),
        "first_slot_good_count": _int(counts.get("first_slot_good_count")),
        "first_slot_published_count": _int(counts.get("first_slot_published_count")),
        "published_output_count": _int(counts.get("published_output_count")),
    }


def _asset_for_segment(ctx: SkillContext, segment: dict[str, Any]) -> dict[str, Any]:
    asset_id = str(segment.get("asset_id") or "")
    if not asset_id:
        return {}
    return ctx.repo.get("assets", "asset_id", asset_id) or {
        "asset_id": asset_id,
        "product_id": segment.get("product_id"),
        "source_type": segment.get("source_type"),
        "source_trust_level": segment.get("source_trust_level"),
    }


def _segment_good_cap(source_type: str) -> int:
    return SEGMENT_TOTAL_CAP_REAL if source_type in TRUSTED_REAL_SOURCE_TYPES else SEGMENT_TOTAL_CAP_DEFAULT


def _asset_good_cap(source_type: str) -> int:
    if source_type in TRUSTED_REAL_SOURCE_TYPES:
        return ASSET_TOTAL_CAP_REAL
    if source_type == SourceType.AI_GENERATED:
        return ASSET_TOTAL_CAP_DEFAULT
    return ASSET_TOTAL_CAP_DEFAULT


def _is_published_output(output: dict[str, Any]) -> bool:
    if str(output.get("published_at") or "").strip():
        return True
    return str(output.get("publish_result") or "").strip().lower() in PUBLISHED_RESULT_VALUES


def _is_first_slot(slot_index: Any, role: Any) -> bool:
    return _int(slot_index) == 1 or str(role or "").strip().lower() == "hero"


def _inc(target: dict[str, Counter[str]], key: str, field: str) -> None:
    target.setdefault(key, Counter())[field] += 1


def _chunks(items: list[str], size: int):
    for idx in range(0, len(items), max(1, size)):
        yield items[idx : idx + size]


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def _sqlite_statements() -> list[str]:
    return [
        """
        CREATE TABLE IF NOT EXISTS mixcut_segment_usage_snapshot (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          segment_id TEXT NOT NULL UNIQUE,
          product_id TEXT NOT NULL,
          asset_id TEXT,
          source_type TEXT,
          source_trust_level TEXT,
          planned_count INTEGER DEFAULT 0,
          rendered_count INTEGER DEFAULT 0,
          good_output_count INTEGER DEFAULT 0,
          draft_output_count INTEGER DEFAULT 0,
          rejected_output_count INTEGER DEFAULT 0,
          first_slot_planned_count INTEGER DEFAULT 0,
          first_slot_good_count INTEGER DEFAULT 0,
          first_slot_published_count INTEGER DEFAULT 0,
          published_output_count INTEGER DEFAULT 0,
          ads_eligible INTEGER DEFAULT 1,
          ads_block_reason TEXT,
          usage_risk_level TEXT,
          created_at TEXT,
          updated_at TEXT
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_mixcut_segment_usage_product ON mixcut_segment_usage_snapshot(product_id)",
        "CREATE INDEX IF NOT EXISTS idx_mixcut_segment_usage_asset ON mixcut_segment_usage_snapshot(asset_id)",
        """
        CREATE TABLE IF NOT EXISTS mixcut_asset_usage_snapshot (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          asset_id TEXT NOT NULL UNIQUE,
          product_id TEXT NOT NULL,
          source_type TEXT,
          source_trust_level TEXT,
          segment_count INTEGER DEFAULT 0,
          planned_count INTEGER DEFAULT 0,
          rendered_count INTEGER DEFAULT 0,
          good_output_count INTEGER DEFAULT 0,
          draft_output_count INTEGER DEFAULT 0,
          rejected_output_count INTEGER DEFAULT 0,
          first_slot_planned_count INTEGER DEFAULT 0,
          first_slot_good_count INTEGER DEFAULT 0,
          first_slot_published_count INTEGER DEFAULT 0,
          published_output_count INTEGER DEFAULT 0,
          ads_eligible INTEGER DEFAULT 1,
          ads_block_reason TEXT,
          usage_risk_level TEXT,
          created_at TEXT,
          updated_at TEXT
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_mixcut_asset_usage_product ON mixcut_asset_usage_snapshot(product_id)",
        """
        CREATE TABLE IF NOT EXISTS mixcut_output_similarity (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          comparison_id TEXT NOT NULL UNIQUE,
          output_id TEXT NOT NULL,
          compared_output_id TEXT NOT NULL,
          product_id TEXT NOT NULL,
          same_first_segment INTEGER DEFAULT 0,
          same_first_asset INTEGER DEFAULT 0,
          same_template INTEGER DEFAULT 0,
          segment_overlap_ratio REAL DEFAULT 0,
          core_segment_overlap_ratio REAL DEFAULT 0,
          asset_overlap_ratio REAL DEFAULT 0,
          similarity_level TEXT,
          decision TEXT,
          reason_json TEXT,
          created_at TEXT,
          updated_at TEXT
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_mixcut_similarity_output ON mixcut_output_similarity(output_id)",
        "CREATE INDEX IF NOT EXISTS idx_mixcut_similarity_product ON mixcut_output_similarity(product_id)",
    ]


def _mysql_statements() -> list[str]:
    return [
        """
        CREATE TABLE IF NOT EXISTS mixcut_segment_usage_snapshot (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          segment_id VARCHAR(128) NOT NULL UNIQUE,
          product_id VARCHAR(128) NOT NULL,
          asset_id VARCHAR(128),
          source_type VARCHAR(64),
          source_trust_level VARCHAR(64),
          planned_count INT DEFAULT 0,
          rendered_count INT DEFAULT 0,
          good_output_count INT DEFAULT 0,
          draft_output_count INT DEFAULT 0,
          rejected_output_count INT DEFAULT 0,
          first_slot_planned_count INT DEFAULT 0,
          first_slot_good_count INT DEFAULT 0,
          first_slot_published_count INT DEFAULT 0,
          published_output_count INT DEFAULT 0,
          ads_eligible TINYINT DEFAULT 1,
          ads_block_reason TEXT,
          usage_risk_level VARCHAR(64),
          created_at DATETIME,
          updated_at DATETIME,
          KEY idx_mixcut_segment_usage_product (product_id),
          KEY idx_mixcut_segment_usage_asset (asset_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS mixcut_asset_usage_snapshot (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          asset_id VARCHAR(128) NOT NULL UNIQUE,
          product_id VARCHAR(128) NOT NULL,
          source_type VARCHAR(64),
          source_trust_level VARCHAR(64),
          segment_count INT DEFAULT 0,
          planned_count INT DEFAULT 0,
          rendered_count INT DEFAULT 0,
          good_output_count INT DEFAULT 0,
          draft_output_count INT DEFAULT 0,
          rejected_output_count INT DEFAULT 0,
          first_slot_planned_count INT DEFAULT 0,
          first_slot_good_count INT DEFAULT 0,
          first_slot_published_count INT DEFAULT 0,
          published_output_count INT DEFAULT 0,
          ads_eligible TINYINT DEFAULT 1,
          ads_block_reason TEXT,
          usage_risk_level VARCHAR(64),
          created_at DATETIME,
          updated_at DATETIME,
          KEY idx_mixcut_asset_usage_product (product_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS mixcut_output_similarity (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          comparison_id VARCHAR(256) NOT NULL UNIQUE,
          output_id VARCHAR(128) NOT NULL,
          compared_output_id VARCHAR(128) NOT NULL,
          product_id VARCHAR(128) NOT NULL,
          same_first_segment TINYINT DEFAULT 0,
          same_first_asset TINYINT DEFAULT 0,
          same_template TINYINT DEFAULT 0,
          segment_overlap_ratio DECIMAL(8, 4) DEFAULT 0,
          core_segment_overlap_ratio DECIMAL(8, 4) DEFAULT 0,
          asset_overlap_ratio DECIMAL(8, 4) DEFAULT 0,
          similarity_level VARCHAR(64),
          decision VARCHAR(64),
          reason_json JSON,
          created_at DATETIME,
          updated_at DATETIME,
          KEY idx_mixcut_similarity_output (output_id),
          KEY idx_mixcut_similarity_product (product_id)
        )
        """,
    ]
