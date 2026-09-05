"""Read-only preflight checks before OPV image generation or publishing."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List

from domain.contracts import ContractViolationError, expected_plan_shot_count, is_multi_look_plan

from services.content_planner import normalize_storyboard_for_preset
from services.locale_quality import copy_locale_issues, visible_text_issues


@dataclass
class PreflightIssue:
    severity: str
    code: str
    message: str


@dataclass
class PreflightReport:
    task_id: str
    ready_for_generation: bool
    ready_for_publish: bool
    issues: List[PreflightIssue]
    facts: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "ready_for_generation": self.ready_for_generation,
            "ready_for_publish": self.ready_for_publish,
            "issues": [asdict(issue) for issue in self.issues],
            "facts": self.facts,
        }


class PreflightService:
    def __init__(self, repository, asset_reader):
        self._repository = repository
        self._assets = asset_reader

    def check_task(self, task_id: str) -> PreflightReport:
        issues: List[PreflightIssue] = []
        facts: Dict[str, Any] = {}
        task = self._repository.get_task(task_id)
        if task is None:
            return PreflightReport(
                task_id, False, False,
                [PreflightIssue("error", "task_missing", f"task {task_id} not found")],
                {},
            )
        account = self._repository.get_account_profile(task.account_id)
        if account is None:
            issues.append(PreflightIssue("error", "account_missing", task.account_id))
            return PreflightReport(task_id, False, False, issues, facts)
        facts["account"] = {"id": account.account_id, "status": account.status}
        preset = self._repository.get_render_preset(account.default_render_preset_id or "")
        theme = self._repository.get_theme(task.theme_id or "")
        pack = self._repository.get_market_pack(task.market_pack_id or "")
        if preset is None:
            issues.append(PreflightIssue("error", "preset_missing", "render preset not found"))
        elif preset.status != "active":
            issues.append(PreflightIssue("error", "preset_inactive", preset.render_preset_id))
        if theme is None:
            issues.append(PreflightIssue("error", "theme_missing", "theme not found"))
        if pack is None or pack.status != "active":
            issues.append(PreflightIssue("error", "market_pack_invalid", "market pack missing/inactive"))

        product = task.product_snapshot_json.get("product", {})
        refs = list(product.get("reference_images") or [])
        local_refs = [str(value) for value in refs if isinstance(value, str) and Path(value).is_file()]
        facts["product_reference_images"] = {"configured": len(refs), "local": len(local_refs)}
        if product.get("reference_pack_id"):
            facts["product_reference_pack"] = {
                "id": product.get("reference_pack_id"),
                "version": product.get("reference_pack_version"),
                "variant": product.get("variant_key"),
                "status": product.get("reference_status"),
                "selection_reason": product.get("selection_reason"),
                "visual_qa_policy": product.get("visual_qa_policy"),
            }
        if not local_refs:
            issues.append(PreflightIssue("error", "product_refs_missing_local", "no local product reference image"))
        elif len(local_refs) < 3:
            issues.append(PreflightIssue("warning", "product_refs_thin", f"only {len(local_refs)} local product references"))

        if theme is not None:
            category = str(product.get("category") or "").lower()
            categories = list(theme.product_match_rules_json.get("categories") or [])
            if category and category not in categories:
                issues.append(PreflightIssue("error", "theme_product_mismatch", f"{category} not matched by {theme.theme_id}"))
            if preset is not None:
                try:
                    plan = task.plan_json or {}
                    if is_multi_look_plan(plan):
                        expected_plan_shot_count(plan)
                        normalized = plan["shots"]
                        if sum(s["duration_ms"] for s in normalized) != 10000:
                            raise ValueError("multi_look frozen timeline must total 10000ms")
                    else:
                        normalized = normalize_storyboard_for_preset(
                            theme.default_storyboard_json.get("slots") or [], preset
                        )
                    facts["timeline"] = {
                        "target_ms": preset.target_duration_ms,
                        "durations_ms": [s["duration_ms"] for s in normalized],
                        "transitions": [s["transition_out"] for s in normalized],
                    }
                except Exception as exc:  # noqa: BLE001
                    issues.append(PreflightIssue("error", "timeline_invalid", str(exc)))

        try:
            selected_persona = ((task.plan_json or {}).get("persona") or {}).get("ref_id") or account.persona_ref_id
            persona = self._assets.get_persona(selected_persona or "")
            local_persona_refs = list(persona.get("local_reference_images") or [])
            facts["persona"] = {
                "id": selected_persona,
                "authority": ((persona.get("source") or {}).get("authority")),
                "status": persona.get("status"),
                "configured_refs": len(persona.get("reference_images") or []),
                "local_refs": len(local_persona_refs),
            }
            min_active = int((account.operating_rules_json or {}).get("persona_min_local_refs_active", 3))
            if account.status == "active" and len(local_persona_refs) < min_active:
                issues.append(PreflightIssue("error", "persona_refs_insufficient", f"active account needs {min_active} local persona refs"))
            elif not local_persona_refs:
                issues.append(PreflightIssue("warning", "persona_refs_not_local", "persona exists in original library but no local reference can be read by image generation"))
        except Exception as exc:  # noqa: BLE001
            issues.append(PreflightIssue("error", "persona_invalid", str(exc)))

        for kind, ref_id, getter in (
            ("look", (task.plan_json or {}).get("look", {}).get("ref_id") or (account.allowed_look_refs_json or [""])[0], self._assets.get_look),
            ("scene", (task.plan_json or {}).get("scene", {}).get("ref_id") or (account.core_scene_refs_json or account.allowed_scene_refs_json or [""])[0], self._assets.get_scene),
        ):
            try:
                getter(ref_id)
                facts[kind] = ref_id
            except Exception as exc:  # noqa: BLE001
                issues.append(PreflightIssue("error", f"{kind}_invalid", str(exc)))

        locale = task.target_locale
        copy_block = task.copy_json or (task.plan_json or {}).get("copy") or {}
        for issue in copy_locale_issues(copy_block, locale):
            issues.append(PreflightIssue("warning", "locale_impurity", issue))
        for index, shot in enumerate((task.plan_json or {}).get("shots") or []):
            for issue in visible_text_issues(shot.get("overlay_text"), locale, f"shots[{index}].overlay_text"):
                issues.append(PreflightIssue("warning", "locale_impurity", issue))

        shots = self._repository.list_shots(task_id)
        latest = self._latest(shots)
        try:
            count = expected_plan_shot_count(task.plan_json or {})
        except ContractViolationError as exc:
            issues.append(PreflightIssue("error", "shot_plan_invalid", str(exc)))
            count = 0
        if shots:
            facts["shots"] = {
                "rows": len(shots),
                "latest_slots": len(latest),
                "selected_slots": sum(1 for shot in latest if shot.is_selected),
            }
            if len(latest) != count or {s.slot_index for s in latest} != set(range(1, count + 1)):
                issues.append(PreflightIssue("error", "shot_set_incomplete", f"latest set has {len(latest)}/{count} slots"))
            if any(shot.image_url and not Path(shot.image_url).is_file() for shot in latest):
                issues.append(PreflightIssue("error", "shot_file_missing", "one or more selected shot files are missing"))
            if len(latest) == count and any(not shot.is_selected for shot in latest):
                issues.append(PreflightIssue("warning", "shot_selection_incomplete", f"latest P1-P{count} are not all selected"))

        publications = []
        list_by_task = getattr(self._repository, "list_publish_records_by_task", None)
        if list_by_task is not None:
            publications = list_by_task(task_id)
        facts["prior_publish_records"] = [
            {
                "publish_id": row.publish_id,
                "status": row.publish_status,
                "external_post_id": row.external_post_id,
                "platform_task_id": (row.platform_metadata_json or {}).get("platform_task_id")
                or (row.platform_metadata_json or {}).get("neobund_task_id"),
            }
            for row in publications
        ]
        if publications:
            issues.append(PreflightIssue("warning", "prior_publish_exists", "task already has publish history; never blind-resubmit"))

        generation_blockers = {
            "task_missing", "account_missing", "preset_missing", "preset_inactive",
            "theme_missing", "market_pack_invalid", "product_refs_missing_local",
            "theme_product_mismatch", "timeline_invalid", "persona_invalid",
            "persona_refs_insufficient", "look_invalid", "scene_invalid",
        }
        error_codes = {issue.code for issue in issues if issue.severity == "error"}
        ready_generation = not bool(error_codes & generation_blockers)
        ready_publish = ready_generation and bool(latest) and not any(
            issue.severity == "error" for issue in issues
        )
        return PreflightReport(task_id, ready_generation, ready_publish, issues, facts)

    @staticmethod
    def _latest(shots):
        by_slot = {}
        for shot in shots:
            current = by_slot.get(shot.slot_index)
            if current is None or shot.shot_version > current.shot_version:
                by_slot[shot.slot_index] = shot
        return [by_slot[index] for index in sorted(by_slot)]
