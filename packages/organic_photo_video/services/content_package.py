"""Content Package lifecycle (统一输出对象).

The package is the reusable output contract: publishing/rendering consumers
only touch READY/RENDERED packages and never need to understand generation.

Status machine (domain.statuses.PACKAGE_STATUS_TRANSITIONS):

    planning -> generating -> qa_review -> ready -> rendered
    (any non-terminal state may go invalid)

``advance_for_task`` is the single hook other flows call. It no-ops when the
task has no package, keeps same-status writes idempotent, and raises a domain
error for illegal transitions so callers cannot silently create split state.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from domain import statuses
from domain.models import ContentPackage, generate_prefixed_id, utc_now


class ContentPackageError(RuntimeError):
    pass


class ContentPackageService:
    def __init__(self, repository, clock=None):
        self._repository = repository
        self._clock = clock or utc_now

    def create_for_task(
        self,
        task,
        *,
        recipe_id: Optional[str] = None,
        plan: Optional[Dict[str, Any]] = None,
    ) -> ContentPackage:
        existing = self._repository.get_content_package_by_task(task.task_id)
        if existing:
            return existing
        plan = plan or task.plan_json or {}
        package = ContentPackage(
            content_package_id=generate_prefixed_id("opv_pkg"),
            task_id=task.task_id,
            recipe_id=recipe_id or ((plan.get("recipe") or {}).get("id")),
            theme_id=(plan.get("theme") or {}).get("id"),
            product_snapshot_id=str(
                ((plan.get("product_facts") or {}).get("product_id")) or ""
            ) or None,
            outfit_plan_id=(plan.get("outfit_plan") or {}).get("outfit_plan_id"),
            storyboard_version=(
                str(getattr(task, "storyboard_version", "") or "")
                or (
                    f"{(plan.get('recipe') or {}).get('id')}-v"
                    f"{(plan.get('recipe') or {}).get('version')}"
                    if (plan.get("recipe") or {}).get("id")
                    else None
                )
            ),
            generation_lineage_json={
                "created_at": self._clock().isoformat(timespec="seconds"),
                "planner": (plan.get("recipe") or {}).get("id")
                and "recipe-driven-v2"
                or "theme-driven-v1",
                "recipe_version": (plan.get("recipe") or {}).get("version"),
                "quality_profile_id": (
                    (plan.get("quality_contract") or {}).get("quality_profile_id")
                ),
                "render_profile_id": (
                    (plan.get("render_contract") or {}).get("render_profile_id")
                ),
                "persona_ref": (plan.get("persona") or {}).get("ref_id"),
                "look_ref": (plan.get("look") or {}).get("ref_id"),
            },
            status=statuses.PACKAGE_PLANNING,
        )
        creator = getattr(
            self._repository, "create_content_package_idempotent", None
        )
        if callable(creator):
            package, _created = creator(package)
        else:
            self._repository.insert_content_package(package)
        link = getattr(self._repository, "update_task_plan", None)
        if callable(link):
            link(task.task_id, content_package_id=package.content_package_id)
        return self._repository.get_content_package(package.content_package_id) or package

    def advance_for_task(
        self, task_id: str, to_status: str, **fields: Any
    ) -> Optional[ContentPackage]:
        task = self._repository.get_task(task_id)
        package_id = getattr(task, "content_package_id", None) if task else None
        if not package_id:
            return None
        package = self._repository.get_content_package(package_id)
        if package is None:
            return None
        if package.status == to_status:
            if fields:
                self._repository.update_content_package(package_id, **fields)
            return self._repository.get_content_package(package_id) or package
        try:
            statuses.ensure_transition(
                statuses.PACKAGE_STATUS_TRANSITIONS, package.status, to_status
            )
        except statuses.InvalidTransitionError:
            raise ContentPackageError(
                f"content package {package_id} cannot transition "
                f"{package.status!r} -> {to_status!r}"
            )
        self._repository.update_content_package(
            package_id, status=to_status, **fields
        )
        return self._repository.get_content_package(package_id) or package


def advance_package_for_task(
    repository, task_id: str, to_status: str, **fields: Any
) -> Optional[ContentPackage]:
    """Module-level convenience used by hero_first / render flows."""
    return ContentPackageService(repository).advance_for_task(
        task_id, to_status, **fields
    )
