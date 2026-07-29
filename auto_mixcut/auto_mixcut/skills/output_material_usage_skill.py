from __future__ import annotations

import hashlib
from collections import defaultdict

from auto_mixcut.core.result import Result

from .context import SkillContext


CORE_ROLES = {"hero", "result", "detail"}


class OutputMaterialUsageSkill:
    """Builds the business-level material usage snapshot for rendered outputs."""

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def refresh_output(self, output_id: str) -> Result:
        ensured = ensure_output_material_usage_table(self.ctx)
        if not ensured.success:
            return ensured
        output = self.ctx.repo.get("outputs", "output_id", output_id)
        if not output:
            return Result.fail("OUTPUT_NOT_FOUND", "output not found", {"output_id": output_id})
        slots = self.ctx.repo.list_where("output_segments", "output_id=? ORDER BY slot_index", (output_id,))
        grouped: dict[str, list[dict]] = defaultdict(list)
        for slot in slots:
            asset_id = str(slot.get("asset_id") or "").strip()
            if asset_id:
                grouped[asset_id].append(slot)
        task = _task_for_output(self.ctx, output)
        rows = [self._aggregate(output, task, asset_id, asset_slots) for asset_id, asset_slots in grouped.items()]
        write = self.ctx.repo.bulk_upsert("output_material_usage", "usage_id", rows)
        if not write.success:
            return write
        live_ids = {row["usage_id"] for row in rows}
        existing = self.ctx.repo.list_where("output_material_usage", "output_id=?", (output_id,))
        stale_ids = [row["usage_id"] for row in existing if row["usage_id"] not in live_ids]
        for usage_id in stale_ids:
            deleted = self.ctx.repo.delete_where("output_material_usage", "usage_id=?", (usage_id,))
            if not deleted.success:
                return deleted
        return Result.ok({"output_id": output_id, "material_count": len(rows), "rows": rows, "deleted_stale": len(stale_ids)})

    def refresh_batch(self, batch_id: str) -> Result:
        outputs = self.ctx.repo.list_where("outputs", "batch_id=?", (batch_id,))
        results = []
        for output in outputs:
            result = self.refresh_output(output["output_id"])
            if not result.success:
                return result
            results.append(result.data)
        return Result.ok({"batch_id": batch_id, "output_count": len(results), "results": results})

    def refresh_product(self, product_id: str) -> Result:
        outputs = self.ctx.repo.list_where("outputs", "product_id=?", (product_id,))
        results = []
        for output in outputs:
            result = self.refresh_output(output["output_id"])
            if not result.success:
                return result
            results.append(result.data)
        return Result.ok({"product_id": product_id, "output_count": len(results), "results": results})

    def _aggregate(self, output: dict, task: dict, asset_id: str, slots: list[dict]) -> dict:
        asset = self.ctx.repo.get("assets", "asset_id", asset_id) or {}
        roles = sorted({str(slot.get("role_used") or "") for slot in slots if slot.get("role_used")})
        core_slots = [slot for slot in slots if str(slot.get("role_used") or "") in CORE_ROLES]
        first_slots = [slot for slot in slots if int(slot.get("slot_index") or 0) == 1]
        used_duration_ms = sum(
            max(0, int(slot.get("end_ms_in_output") or 0) - int(slot.get("start_ms_in_output") or 0))
            for slot in slots
        )
        return {
            "usage_id": _usage_id(output["output_id"], asset_id),
            "output_id": output["output_id"],
            "batch_id": output.get("batch_id"),
            "product_id": output.get("product_id"),
            "target_language": output.get("target_language") or task.get("target_language"),
            "asset_id": asset_id,
            "source_system": asset.get("source_flow") or asset.get("source_type"),
            "source_record_id": asset.get("source_record_id"),
            "segment_count": len(slots),
            "used_duration_ms": used_duration_ms,
            "roles_json": roles,
            "core_segment_count": len(core_slots),
            "is_core_material": int(bool(core_slots)),
            "is_first_slot": int(bool(first_slots)),
            "first_slot_segment_id": first_slots[0].get("segment_id") if first_slots else None,
        }


def _usage_id(output_id: str, asset_id: str) -> str:
    digest = hashlib.sha1(f"{output_id}|{asset_id}".encode("utf-8")).hexdigest()[:24]
    return f"OMU_{digest}"


def _task_for_output(ctx: SkillContext, output: dict) -> dict:
    batches = ctx.repo.list_where("mixcut_batches", "batch_id=? LIMIT 1", (output.get("batch_id"),))
    if not batches or not batches[0].get("task_id"):
        return {}
    return ctx.repo.get("content_tasks", "task_id", batches[0]["task_id"]) or {}


def ensure_output_material_usage_table(ctx: SkillContext) -> Result:
    mysql = getattr(ctx.repo, "dialect", "sqlite") == "mysql"
    statement = (
        """
        CREATE TABLE IF NOT EXISTS output_material_usage (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          usage_id VARCHAR(128) NOT NULL UNIQUE,
          output_id VARCHAR(128) NOT NULL,
          batch_id VARCHAR(128),
          product_id VARCHAR(128),
          target_language VARCHAR(64),
          asset_id VARCHAR(128) NOT NULL,
          source_system VARCHAR(64),
          source_record_id VARCHAR(128),
          segment_count INT DEFAULT 0,
          used_duration_ms INT DEFAULT 0,
          roles_json JSON,
          core_segment_count INT DEFAULT 0,
          is_core_material TINYINT DEFAULT 0,
          is_first_slot TINYINT DEFAULT 0,
          first_slot_segment_id VARCHAR(128),
          created_at DATETIME,
          updated_at DATETIME,
          UNIQUE KEY uq_output_material (output_id, asset_id),
          KEY idx_output_material_output (output_id),
          KEY idx_output_material_product (product_id),
          KEY idx_output_material_asset (asset_id)
        )
        """
        if mysql
        else
        """
        CREATE TABLE IF NOT EXISTS output_material_usage (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          usage_id TEXT NOT NULL UNIQUE,
          output_id TEXT NOT NULL,
          batch_id TEXT,
          product_id TEXT,
          target_language TEXT,
          asset_id TEXT NOT NULL,
          source_system TEXT,
          source_record_id TEXT,
          segment_count INTEGER DEFAULT 0,
          used_duration_ms INTEGER DEFAULT 0,
          roles_json TEXT,
          core_segment_count INTEGER DEFAULT 0,
          is_core_material INTEGER DEFAULT 0,
          is_first_slot INTEGER DEFAULT 0,
          first_slot_segment_id TEXT,
          created_at TEXT,
          updated_at TEXT,
          UNIQUE(output_id, asset_id)
        )
        """
    )
    try:
        with ctx.repo.connect() as conn:
            if mysql:
                with conn.cursor() as cur:
                    cur.execute(statement)
            else:
                conn.execute(statement)
        return Result.ok()
    except Exception as exc:
        return Result.fail("OUTPUT_MATERIAL_USAGE_TABLE_FAILED", str(exc))
