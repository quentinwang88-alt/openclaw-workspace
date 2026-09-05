"""Unified entry: generate_product_image_story (产品驱动型图文内容).

    generate_product_image_story(
        repository=..., product_id=..., market="TH", language="th-TH",
        recipe_id="RECIPE_SCENE_SOLUTION_V1", theme_id=None, variant_count=1,
        product_snapshot={...},
    )

V1 scope: runs Intake (layer 0) -> Product Facts + Outfit Plan + Recipe-driven
Plan (layers 1-4) -> Content Package (PLANNING). Image generation, QA and
rendering are invoked through the existing services (hero_first / visual_qa /
render flow) and advance the same package.

Account resolution: the account profile matching (market, language) whose
status allows intake; explicit account_id overrides. Idempotency: the intake
key covers product+source+variant index+UTC date, so repeating the same call
on the same day returns the same tasks.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from services.task_intake import TaskIntakeService, TaskRequest


class StoryGenerationError(ValueError):
    pass


def resolve_account_for_market(
    repository, market: str, language: str, account_id: Optional[str] = None
) -> Any:
    country = str(market).strip().upper()
    locale = str(language).strip()
    if account_id:
        account = repository.get_account_profile(account_id)
        if account is None:
            raise StoryGenerationError(f"account {account_id} not found")
        if (
            account.target_country != country
            or account.default_locale != locale
            or account.status not in ("active", "testing")
        ):
            raise StoryGenerationError(
                f"account {account_id} is not intake-ready for {country}/{locale}"
            )
        return account
    candidates = [
        account
        for account in repository.list_account_profiles()
        if account.target_country == country
        and account.default_locale == locale
        and account.status in ("active", "testing")
    ]
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        raise StoryGenerationError(
            f"multiple intake-ready accounts match {country}/{locale}; pass account_id"
        )
    raise StoryGenerationError(
        f"no intake-ready account bound to {market}/{language}"
    )


def generate_product_image_story(
    repository,
    *,
    product_id: str,
    market: str,
    language: str,
    recipe_id: str,
    account_id: Optional[str] = None,
    theme_id: Optional[str] = None,
    hook_strategy: Optional[str] = None,
    variant_count: int = 1,
    product_snapshot: Optional[Dict[str, Any]] = None,
    operator: str = "story_api",
    asset_reader=None,
    source_type: str = "story_api",
    source_record_id_prefix: Optional[str] = None,
    feishu_record_id: Optional[str] = None,
    persona_ref: Optional[str] = None,
    look_ref: Optional[str] = None,
    scene_ref: Optional[str] = None,
    variant_index_offset: int = 0,
) -> List[Dict[str, Any]]:
    if variant_count < 1 or variant_count > 9:
        raise StoryGenerationError("variant_count must be 1..9")
    if not product_snapshot:
        raise StoryGenerationError(
            "product_snapshot with reference_images is required (Product "
            "Truth integration is a later phase)"
        )
    account = resolve_account_for_market(
        repository, market, language, account_id=account_id
    )
    intake = TaskIntakeService(repository)
    from services.content_planner import ContentPlannerService

    planner = ContentPlannerService(repository, asset_reader=asset_reader)

    results: List[Dict[str, Any]] = []
    for local_variant_index in range(1, variant_count + 1):
        variant_index = int(variant_index_offset) + local_variant_index
        snapshot = dict(product_snapshot)
        snapshot.setdefault("product_id", str(product_id))
        if persona_ref:
            snapshot["planned_persona_ref"] = persona_ref
        if look_ref:
            snapshot["planned_look_ref"] = look_ref
        if scene_ref:
            snapshot["planned_scene_ref"] = scene_ref
        source_record_id = (
            f"{source_record_id_prefix}:{local_variant_index}"
            if source_record_id_prefix
            else f"{product_id}:{recipe_id}:{theme_id or 'auto'}:"
                 f"{hook_strategy or 'auto'}:{variant_index}"
        )
        request = TaskRequest(
            account_id=account.account_id,
            product_id=str(product_id),
            product_snapshot=snapshot,
            theme_id=theme_id,
            source_type=source_type,
            source_record_id=source_record_id,
            feishu_record_id=feishu_record_id,
            idempotency_key=f"{source_type}:{source_record_id}",
            created_by=operator,
        )
        intake_result = intake.create_task(request)
        task = repository.get_task(intake_result.task.task_id)
        # Resume semantics: stages already completed in earlier runs are kept.
        if task.task_status in ("draft", "planned") or not task.plan_json:
            plan_result = planner.plan_task(
                intake_result.task.task_id,
                theme_id=theme_id,
                recipe_id=recipe_id,
                hook_strategy=hook_strategy,
                variant_index=variant_index,
                operator=operator,
            )
            plan = plan_result.plan
        else:
            plan = task.plan_json
        package = repository.get_content_package_by_task(
            intake_result.task.task_id
        )
        task = repository.get_task(intake_result.task.task_id) or task
        results.append(
            {
                "task_id": intake_result.task.task_id,
                "task_status": task.task_status,
                "created": intake_result.created,
                "content_package_id": (
                    package.content_package_id if package else None
                ),
                "package_status": package.status if package else None,
                "theme": (plan.get("theme") or {}).get("id"),
                "recipe": (plan.get("recipe") or {}).get("id"),
                "anchor_slot": plan.get("anchor_slot"),
                "hook_strategy": ((plan.get("recipe_execution") or {}).get(
                    "hook_strategy"
                )),
                "variation_plan": dict(plan.get("variation_plan") or {
                    "variant_index": variant_index,
                    "changed_axes": ["hook_strategy"],
                    "fixed_axes": ["product", "persona", "theme", "scene"],
                }),
                "outfit_plan_id": (plan.get("outfit_plan") or {}).get(
                    "outfit_plan_id"
                ),
                "shots": [
                    {
                        "slot_index": s["slot_index"],
                        "slot_role": s["slot_role"],
                        "narrative_function": s.get("narrative_function"),
                        "outfit_state_ref": s.get("outfit_state_ref"),
                    }
                    for s in plan.get("shots", [])
                ],
            }
        )
    return results
