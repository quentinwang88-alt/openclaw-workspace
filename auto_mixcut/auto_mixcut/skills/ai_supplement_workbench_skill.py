from __future__ import annotations

import importlib.util
from pathlib import Path
import re
from typing import Any

from auto_mixcut.core.result import Result

from .capacity_counter_skill import CapacityCounterSkill
from .context import SkillContext
from .feishu_review_skill import FeishuReviewSkill


class AISupplementWorkbenchSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def sync_for_product(self, product_id: str, max_packages: int = 6, gap_text: str = "") -> Result:
        task = _latest_task(self.ctx, product_id)
        if not task:
            return Result.fail("TASK_NOT_FOUND", "task not found", {"product_id": product_id})
        gap_text = _resolve_gap_text(self.ctx, task, gap_text)
        if "AI补素材" not in gap_text:
            data = {"product_id": product_id, "skipped": True, "reason": "no_ai_supplement_gap"}
            _update_task_ai_supplement(self.ctx, task, "skipped", 0, data)
            return Result.ok(data)
        if gap_text != str(task.get("blocked_reason") or ""):
            self.ctx.repo.update("content_tasks", "task_id", task["task_id"], {"blocked_reason": gap_text})
            task = _latest_task(self.ctx, product_id) or task
        if not self.ctx.settings.feishu_enabled:
            data = {"product_id": product_id, "skipped": True, "reason": "feishu_disabled"}
            _update_task_ai_supplement(self.ctx, task, "blocked", 0, data)
            return Result.ok(data)

        feishu = FeishuReviewSkill(self.ctx)
        anchor_sync = feishu.sync_anchor_queue(product_id)
        if not anchor_sync.success:
            return anchor_sync
        task_sync = feishu.sync_task(product_id)
        if not task_sync.success:
            return task_sync

        try:
            module = _load_workbench_module()
            result = module.sync_workbench(
                product_task_url=module.PRODUCT_TASK_URL,
                anchor_queue_url=module.ANCHOR_QUEUE_URL,
                prompt_workbench_url=module.PROMPT_WORKBENCH_URL,
                dry_run=False,
                product_id_filter=product_id,
                max_packages_per_product=max(1, max_packages),
                refresh_existing_prompts=False,
            )
        except Exception as exc:
            detail = {"product_id": product_id, "error": str(exc)}
            _update_task_ai_supplement(self.ctx, task, "failed", 0, detail)
            return Result.fail("AI_SUPPLEMENT_SYNC_FAILED", str(exc), {"product_id": product_id})

        created = result.get("created") or []
        skipped = result.get("skipped") or []
        failed = result.get("failed") or []
        existing_count = sum(1 for item in skipped if isinstance(item, dict) and item.get("reason") == "already_exists")
        if failed:
            status = "failed" if not created and not existing_count else "created"
        elif created or existing_count:
            status = "created"
        elif skipped:
            status = "blocked"
        else:
            status = "skipped"
        state_summary = _supplement_state_summary(gap_text, created, skipped, failed)
        data = {
            "product_id": product_id,
            "anchor_sync": anchor_sync.data,
            "task_sync": task_sync.data,
            "state_summary": state_summary,
            "workbench": result,
        }
        _update_task_ai_supplement(self.ctx, task, status, len(created) + existing_count, data)
        final_task_sync = feishu.sync_task(product_id)
        if final_task_sync.success:
            data["final_task_sync"] = final_task_sync.data
        else:
            data["final_task_sync_error"] = final_task_sync.to_dict()
        return Result.ok(data)


def _latest_task(ctx: SkillContext, product_id: str) -> dict | None:
    rows = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
    return rows[0] if rows else None


def _update_task_ai_supplement(ctx: SkillContext, task: dict, status: str, package_count: int, detail: dict) -> None:
    task_id = task.get("task_id")
    if not task_id:
        return
    patch = {
        "ai_supplement_status": status,
        "ai_supplement_package_count": package_count,
        "ai_supplement_detail_json": detail,
    }
    if status == "created":
        patch["task_status"] = "AI_SUPPLEMENT_CREATED"
        patch["pipeline_status"] = "WAITING_AI_RETURN"
        patch["next_action"] = "WAIT_AI_SEGMENT_RETURN"
        patch["last_error"] = ""
    elif status in {"blocked", "failed"}:
        patch["task_status"] = "AI_SUPPLEMENT_BLOCKED" if status == "blocked" else "AI_SUPPLEMENT_FAILED"
        patch["pipeline_status"] = "BLOCKED" if status == "blocked" else "ERROR"
        patch["next_action"] = "NEED_MORE_MATERIAL_OR_AI_SUPPLEMENT" if status == "blocked" else "CHECK_ERROR"
        patch["last_error"] = (detail or {}).get("error") or status
    ctx.repo.update("content_tasks", "task_id", task_id, patch)


def _resolve_gap_text(ctx: SkillContext, task: dict, explicit_gap_text: str = "") -> str:
    gap_text = str(explicit_gap_text or task.get("blocked_reason") or "")
    if "AI补素材" in gap_text or "ai补素材" in gap_text.lower():
        return gap_text
    inferred = _infer_capacity_gap_text(ctx, task)
    return inferred or gap_text


def _infer_capacity_gap_text(ctx: SkillContext, task: dict) -> str:
    product_id = str(task.get("product_id") or "")
    if product_id:
        refreshed = CapacityCounterSkill(ctx).refresh_product(product_id)
        if refreshed.success:
            task = _latest_task(ctx, product_id) or task

    target = int(task.get("requested_variant_count") or task.get("allowed_variant_count") or 0)
    actual = int(task.get("actual_variant_count") or 0)
    remaining = int(task.get("target_remaining_variant_count") or max(0, target - actual) or 0)
    extra_capacity = int(task.get("material_pool_extra_capacity") or 0)
    shortfall = max(0, remaining - extra_capacity)
    if shortfall <= 0:
        return ""

    need = min(max(shortfall, 1), 6)
    bottleneck = str(task.get("current_bottleneck") or task.get("capacity_note") or "")
    first_slot_remaining = int(task.get("first_slot_remaining_capacity") or 0)
    if "首镜" in bottleneck or first_slot_remaining <= 0:
        return f"AI补素材: hero首镜{min(max(need, 1), 6)}"

    hero = max(1, min(2, need))
    detail = max(1, min(2, need - 1)) if need >= 2 else 0
    result = 1 if need >= 3 else 0
    scene = 1 if need >= 4 else 0
    parts = [f"hero首镜{hero}"]
    if detail:
        parts.append(f"detail细节{detail}")
    if result:
        parts.append(f"result上身{result}")
    if scene:
        parts.append(f"scene场景{scene}")
    return "AI补素材: " + "; ".join(parts)


def _supplement_state_summary(gap_text: str, created: list, skipped: list, failed: list) -> dict[str, Any]:
    requested = _parse_requested_slots(gap_text)
    existing = [item for item in skipped if isinstance(item, dict) and item.get("reason") == "already_exists"]
    other_skipped = [item for item in skipped if not (isinstance(item, dict) and item.get("reason") == "already_exists")]
    created_count = len(created)
    existing_count = len(existing)
    failed_count = len(failed)
    return {
        "requested_slots": requested,
        "requested_total": sum(requested.values()),
        "created_count": created_count,
        "existing_count": existing_count,
        "available_task_package_count": created_count + existing_count,
        "failed_count": failed_count,
        "skipped_count": len(other_skipped),
        "state": "waiting_ai_return" if created_count or existing_count else ("failed" if failed_count else "blocked_or_skipped"),
        "next_trigger": "AI素材回流后只跑容量重算和top-up补差额",
    }


def _parse_requested_slots(text: str) -> dict[str, int]:
    aliases = {
        "hero": ["hero", "首镜"],
        "detail": ["detail", "细节"],
        "result": ["result", "上身"],
        "scene": ["scene", "场景"],
        "ending": ["ending", "结尾"],
    }
    result: dict[str, int] = {}
    for role, tokens in aliases.items():
        for token in tokens:
            match = re.search(rf"{re.escape(token)}\s*(?:[^\d;；,，]*)?(\d+)", text, re.IGNORECASE)
            if match:
                result[role] = max(result.get(role, 0), int(match.group(1)))
    return result


def _load_workbench_module() -> Any:
    path = Path(__file__).resolve().parents[2] / "scripts" / "sync_prompt_package_workbench_from_tasks.py"
    spec = importlib.util.spec_from_file_location("auto_mixcut_prompt_workbench_sync", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load prompt workbench sync script: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
