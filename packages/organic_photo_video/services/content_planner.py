"""Content Planner (Stage B): turn an intake task into a validated
``opv-plan-v1`` plan and move the task to ``planned``.

Rules (MODEL_HANDOFF 5.2 / 8.1):

- Look priority is fixed: Successful Look > Look Template > AI Exploration.
  V1 resolves ``look_template`` refs from the account allow-list; the task
  snapshot may pin ``planned_look_ref`` / ``planned_scene_ref``, which must be
  inside the account's allowed refs (asset boundary).
- Core scenes are preferred (70% rule); the plan snapshots persona/look/scene
  content so historical tasks stay reproducible.
- Shot slots and durations come from the theme storyboard; the total must fit
  the render preset's target duration.
- ``plan()`` is idempotent: a planned task with an existing plan is returned
  as-is unless ``force=True``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from domain import contracts
from domain.models import (
    AccountProfile,
    ContentTask,
    MarketPack,
    RenderPreset,
    ThemeCatalog,
    generate_prefixed_id,
    utc_now,
)
from domain.statuses import (
    TASK_DRAFT,
    TASK_PLANNED,
    task_ensure_transition,
)


class ContentPlannerError(ValueError):
    pass


PLANNABLE_STATUSES = (TASK_DRAFT, TASK_PLANNED)


@dataclass
class PlanResult:
    task: ContentTask
    plan: Dict[str, Any]
    created: bool
    theme: Optional[ThemeCatalog]


class ContentPlannerService:
    def __init__(self, repository, asset_reader=None, clock=None):
        self._repository = repository
        self._assets = asset_reader
        self._clock = clock or utc_now

    # ------------------------------------------------------------------

    def plan_task(
        self,
        task_id: str,
        *,
        theme_id: Optional[str] = None,
        topic_text: Optional[str] = None,
        recipe_id: Optional[str] = None,
        hook_strategy: Optional[str] = None,
        variant_index: int = 1,
        force: bool = False,
        operator: str = "system",
    ) -> PlanResult:
        task = self._require_task(task_id)
        recipe = self._resolve_recipe(recipe_id, task)
        selected_hook = self._select_hook(recipe, hook_strategy, variant_index)

        existing_plan = task.plan_json or {}
        existing_recipe = (existing_plan.get("recipe") or {}).get("id")
        existing_hook = (existing_plan.get("recipe_execution") or {}).get(
            "hook_strategy"
        )
        existing_variant = int(
            (existing_plan.get("recipe_execution") or {}).get("variant_index") or 1
        )
        if (
            task.task_status == TASK_PLANNED
            and task.plan_json
            and not force
            and topic_text is None
            and (theme_id is None or theme_id == (existing_plan.get("theme") or {}).get("id"))
            and (recipe_id is None or recipe_id == existing_recipe)
            and (selected_hook is None or selected_hook == existing_hook)
            and int(variant_index) == existing_variant
        ):
            return PlanResult(
                task=task,
                plan=task.plan_json,
                created=False,
                theme=self._repository.get_theme(task.theme_id or ""),
            )

        if task.task_status not in PLANNABLE_STATUSES:
            raise ContentPlannerError(
                f"task {task_id} status {task.task_status!r} is not plannable"
            )

        account = self._repository.get_account_profile(task.account_id)
        if account is None:
            raise ContentPlannerError(f"account {task.account_id} not found")
        pack = self._repository.get_market_pack(
            task.market_pack_id or account.default_market_pack_id or ""
        )
        if pack is None:
            raise ContentPlannerError("task has no resolvable market pack")
        preset = self._repository.get_render_preset(
            account.default_render_preset_id or ""
        )
        if preset is None:
            raise ContentPlannerError(
                f"account {account.account_id} has no resolvable render preset"
            )

        theme = self._resolve_theme(task, theme_id)
        self._validate_recipe_theme(recipe, theme)
        render_profile, quality_profile = self._resolve_recipe_profiles(recipe)
        product = task.product_snapshot_json.get("product", {})
        topic = (
            topic_text
            or task.topic_text
            or self._topic_from_template(theme, product)
        )

        persona_ref, persona_snapshot = self._resolve_persona(account, product)
        persona_lock = self._freeze_persona_lock(persona_ref, persona_snapshot)
        look_ref, look_snapshot = self._resolve_look(account, product)
        scene_ref, scene_snapshot = self._resolve_scene(account, product)

        if recipe is not None:
            plan = self._build_recipe_plan(
                task=task,
                account=account,
                pack=pack,
                preset=preset,
                theme=theme,
                topic=topic,
                recipe=recipe,
                selected_hook=selected_hook,
                variant_index=variant_index,
                render_profile=render_profile,
                persona_ref=persona_ref,
                persona_snapshot=persona_snapshot,
                look_ref=look_ref,
                look_snapshot=look_snapshot,
                scene_ref=scene_ref,
                scene_snapshot=scene_snapshot,
                product=product,
            )
        else:
            plan = self._build_theme_plan(
                task=task,
                account=account,
                pack=pack,
                preset=preset,
                theme=theme,
                topic=topic,
                persona_ref=persona_ref,
                persona_snapshot=persona_snapshot,
                look_ref=look_ref,
                look_snapshot=look_snapshot,
                scene_ref=scene_ref,
                scene_snapshot=scene_snapshot,
                product=product,
            )
        plan["persona"]["lock"] = persona_lock
        plan["render_contract"] = {
            "preset_id": preset.render_preset_id,
            "target_duration_ms": preset.target_duration_ms,
            "fps": preset.fps,
            "transition_default": (preset.transition_rules_json or {}).get(
                "default", "cut"
            ),
        }
        if render_profile is not None:
            plan["render_contract"].update(
                {
                    "render_profile_id": render_profile.render_profile_id,
                    "render_profile_version": render_profile.profile_version,
                    "hook_window_ms": render_profile.hook_window_ms,
                    "allowed_motion": list(
                        (render_profile.motion_rules_json or {}).get("allowed") or []
                    ),
                    "allowed_transitions": list(
                        (render_profile.transition_rules_json or {}).get("allowed") or []
                    ),
                }
            )
        if quality_profile is not None:
            plan["quality_contract"] = {
                "quality_profile_id": quality_profile.quality_profile_id,
                "quality_profile_version": quality_profile.profile_version,
                "dimensions": dict(quality_profile.dimensions_json or {}),
                "decision_policy": "independent_dimensions",
                "min_overall_score": quality_profile.min_overall_score,
            }
        contracts.ensure_valid(
            contracts.validate_plan_json(plan), f"plan for {task_id}"
        )

        self._repository.update_task_plan(
            task_id,
            plan_json=plan,
            copy_json=plan["copy"],
            theme_id=theme.theme_id,
            topic_text=topic,
        )
        self._persist_recipe_extras(task_id, task, plan, recipe, account)
        if task.task_status == TASK_DRAFT:
            self._repository.transition_task(task_id, TASK_DRAFT, TASK_PLANNED)

        refreshed = self._repository.get_task(task_id)
        return PlanResult(
            task=refreshed or task,
            plan=plan,
            created=True,
            theme=theme,
        )

    def _require_task(self, task_id: str) -> ContentTask:
        task = self._repository.get_task(task_id)
        if task is None:
            raise ContentPlannerError(f"task {task_id} not found")
        return task

    def _resolve_theme(
        self, task, theme_id: Optional[str]
    ) -> ThemeCatalog:
        if theme_id:
            theme = self._repository.get_theme(theme_id)
            if theme is None:
                raise ContentPlannerError(f"theme {theme_id} not found")
            return theme
        if task.theme_id:
            theme = self._repository.get_theme(task.theme_id)
            if theme is None:
                raise ContentPlannerError(f"task theme {task.theme_id} not found")
            return theme
        product = task.product_snapshot_json.get("product", {})
        category = str(product.get("category") or "").strip().lower()
        if category:
            candidates = [
                t
                for t in self._repository.list_themes(status="active")
                if category in t.product_match_rules_json.get("categories", [])
                and task.target_country in t.applicable_markets_json
            ]
            if candidates:
                return candidates[0]
        raise ContentPlannerError(
            "no theme resolved: pass theme_id explicitly or set a product "
            "category matched by an active theme"
        )

    def _resolve_persona(self, account, product):
        allowed = list(
            (account.operating_rules_json or {}).get("allowed_persona_refs") or []
        )
        requested = str(product.get("planned_persona_ref") or "").strip()
        persona_ref = requested or account.persona_ref_id
        if not persona_ref:
            raise ContentPlannerError(
                f"account {account.account_id} has no persona bound"
            )
        if allowed and persona_ref not in allowed:
            raise ContentPlannerError(
                f"persona {persona_ref} is outside account allow-list"
            )
        snapshot: Dict[str, Any] = {}
        if self._assets is not None:
            snapshot = self._assets.get_persona(persona_ref)
        snapshot_persona_id = str(snapshot.get("persona_id") or "").strip()
        if snapshot_persona_id and snapshot_persona_id != persona_ref:
            raise ContentPlannerError(
                f"persona snapshot stable id mismatch: {snapshot_persona_id!r} "
                f"!= {persona_ref!r}"
            )
        return persona_ref, snapshot

    @staticmethod
    def _freeze_persona_lock(
        persona_ref: str, persona_snapshot: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Freeze the persona identity every generated image must keep."""
        canonical = json.dumps(
            persona_snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        return {
            "persona_id": persona_ref,
            "selection_scope": "TASK_FROZEN",
            "structured_snapshot_hash": hashlib.sha256(
                canonical.encode("utf-8")
            ).hexdigest(),
        }

    def _resolve_look(self, account, product):
        allowed = account.allowed_look_refs_json
        if not allowed:
            raise ContentPlannerError(
                f"account {account.account_id} allows no look refs"
            )
        requested = str(product.get("planned_look_ref") or "")
        if requested:
            if requested not in allowed:
                raise ContentPlannerError(
                    f"planned look {requested} is outside account allow-list"
                )
            look_ref = requested
        else:
            look_ref = allowed[0]
        snapshot: Dict[str, Any] = {}
        if self._assets is not None:
            snapshot = self._assets.get_look(look_ref)
        return look_ref, snapshot

    def _resolve_scene(self, account, product):
        core = account.core_scene_refs_json
        allowed = account.allowed_scene_refs_json
        if not core and not allowed:
            raise ContentPlannerError(
                f"account {account.account_id} allows no scene refs"
            )
        requested = str(product.get("planned_scene_ref") or "")
        if requested:
            if requested not in allowed and requested not in core:
                raise ContentPlannerError(
                    f"planned scene {requested} is outside account allow-list"
                )
            scene_ref = requested
        else:
            scene_ref = core[0] if core else allowed[0]
        snapshot: Dict[str, Any] = {}
        if self._assets is not None:
            snapshot = self._assets.get_scene(scene_ref)
        return scene_ref, snapshot

    def _resolve_recipe(self, recipe_id: Optional[str], task):
        resolved_id = recipe_id or getattr(task, "recipe_id", None)
        if not resolved_id:
            return None
        recipe = self._repository.get_content_recipe(resolved_id)
        if recipe is None:
            raise ContentPlannerError(f"recipe {resolved_id} not found")
        return recipe

    @staticmethod
    def _select_hook(recipe, requested: Optional[str], variant_index: int) -> Optional[str]:
        if recipe is None:
            return None
        hooks = list(recipe.hook_types_json or [])
        if not hooks:
            raise ContentPlannerError(f"recipe {recipe.recipe_id} has no hook types")
        if requested:
            if requested not in hooks:
                raise ContentPlannerError(
                    f"hook {requested!r} is not allowed by recipe {recipe.recipe_id}"
                )
            return requested
        index = max(1, int(variant_index)) - 1
        return hooks[index % len(hooks)]

    @staticmethod
    def _theme_topic_key(theme: ThemeCatalog) -> str:
        prefix = "THEME_"
        value = str(theme.theme_key or theme.theme_id).upper()
        if value.startswith(prefix):
            value = value[len(prefix):]
        parts = value.split("_")
        if parts and len(parts[0]) == 2:
            parts = parts[1:]
        return "_".join(parts).lower()

    def _validate_recipe_theme(self, recipe, theme: ThemeCatalog) -> None:
        if recipe is None or not recipe.suitable_topics_json:
            return
        topic_key = self._theme_topic_key(theme)
        if topic_key not in set(recipe.suitable_topics_json):
            raise ContentPlannerError(
                f"recipe {recipe.recipe_id} is not suitable for theme topic {topic_key!r}"
            )

    def _resolve_recipe_profiles(self, recipe):
        if recipe is None:
            return None, None
        render_profile = None
        quality_profile = None
        if recipe.render_profile_id:
            getter = getattr(self._repository, "get_render_profile", None)
            render_profile = getter(recipe.render_profile_id) if callable(getter) else None
            if render_profile is None or render_profile.status != "active":
                raise ContentPlannerError(
                    f"recipe render profile {recipe.render_profile_id} is missing/inactive"
                )
        if recipe.quality_profile_id:
            getter = getattr(self._repository, "get_quality_profile", None)
            quality_profile = getter(recipe.quality_profile_id) if callable(getter) else None
            if quality_profile is None or quality_profile.status != "active":
                raise ContentPlannerError(
                    f"recipe quality profile {recipe.quality_profile_id} is missing/inactive"
                )
        return render_profile, quality_profile

    def _persist_recipe_extras(self, task_id, task, plan, recipe, account) -> None:
        """Persist recipe/outfit/package extras; no-op for legacy plans."""
        if recipe is None:
            return
        update_fields: Dict[str, Any] = {
            "recipe_id": recipe.recipe_id,
            "recipe_version": recipe.recipe_version,
            "content_goal": recipe.content_goal,
            "hook_strategy": ((plan.get("recipe_execution") or {}).get(
                "hook_strategy"
            )),
            "storyboard_version": f"{recipe.recipe_key}-v{recipe.recipe_version}",
            "product_facts_json": plan.get("product_facts"),
            "outfit_plan_json": plan.get("outfit_plan"),
        }
        self._repository.update_task_plan(task_id, **update_fields)

        package_id = self._ensure_package(task, plan, recipe)
        if package_id:
            update_fields["content_package_id"] = package_id
            self._repository.update_task_plan(task_id, content_package_id=package_id)

    def _ensure_package(self, task, plan, recipe) -> Optional[str]:
        insert = getattr(self._repository, "insert_content_package", None)
        if not callable(insert):
            return None
        from services.content_package import ContentPackageService

        package = ContentPackageService(
            self._repository, clock=self._clock
        ).create_for_task(
            task,
            recipe_id=recipe.recipe_id,
            plan=plan,
        )
        return package.content_package_id

    def _build_recipe_plan(
        self,
        *,
        task,
        account,
        pack,
        preset,
        theme,
        topic,
        recipe,
        selected_hook,
        variant_index,
        render_profile,
        persona_ref,
        persona_snapshot,
        look_ref,
        look_snapshot,
        scene_ref,
        scene_snapshot,
        product,
    ) -> Dict[str, Any]:
        """Recipe-driven plan: narrative arc from the recipe, scenario from the
        theme, outfit plan and product facts as first-class layers."""
        from services.outfit_planner import (
            build_outfit_plan,
            build_outfit_states,
            build_product_facts,
        )

        product_facts = build_product_facts(product)
        outfit_plan = build_outfit_plan(
            outfit_plan_id=generate_prefixed_id("opv_outfit"),
            theme_id=theme.theme_id,
            product_facts=product_facts,
            market_pack={
                "climate_zone": pack.climate_zone,
                "target_country": pack.target_country,
                "target_locale": pack.target_locale,
            },
            persona=dict(persona_snapshot or account.persona_snapshot_json or {}),
        )
        outfit_states = build_outfit_states(
            outfit_plan, content_goal=recipe.content_goal
        )
        timeline = (preset.output_rules_json or {}).get("timeline_defaults_ms") or {}
        shots: List[Dict[str, Any]] = []
        asset_refs = [
            f"persona:{persona_ref}",
            f"look:{look_ref}",
            f"scene:{scene_ref}",
        ]
        product_refs = [
            f"product_image:{p}" for p in product.get("reference_images", [])
        ]
        rules = theme.content_plan_rules_json or {}
        for slot in recipe.story_structure_json:
            index = int(slot["slot_index"])
            outfit_state_ref = self._outfit_state_for_slot(
                recipe.content_goal, index
            )
            duration = int(
                timeline.get(str(index))
                or timeline.get(index)
                or preset.target_duration_ms // max(recipe.shot_count, 1)
            )
            shots.append(
                {
                    "slot_index": index,
                    "slot_role": slot.get("slot_role", "hero"),
                    "purpose": slot.get("purpose", ""),
                    "duration_ms": duration,
                    "motion_preset": self._recipe_motion(render_profile, slot),
                    "transition_out": self._recipe_transition(render_profile, preset, index),
                    "overlay_text": (
                        str(rules.get("overlay_text_hint") or "")
                        if index == 1
                        else ""
                    ),
                    "generation_prompt": self._shot_prompt(
                        slot, look_ref, scene_ref, product
                    ),
                    "source_refs": asset_refs + product_refs,
                    "narrative_function": slot.get("narrative_function"),
                    "product_focus": str(product_facts.get("category") or ""),
                    "camera_hint": slot.get("camera_hint") or "",
                    "outfit_state_ref": outfit_state_ref,
                    "overlay_spec": {
                        "text": str(rules.get("overlay_text_hint") or "")
                        if index == 1 else "",
                        "burn_in": False,
                    },
                    "transition_hint": self._recipe_transition(
                        render_profile, preset, index
                    ),
                    "continuity_constraints": [
                        "same_persona",
                        "same_product",
                        "same_scene_light",
                        "respect_outfit_state",
                    ],
                }
            )
        audio_policy = {
            "strategy": preset.audio_rules_json.get(
                "default_strategy", "platform_hot_bgm"
            ),
            "fallback": preset.audio_rules_json.get("fallback_strategy", "no_bgm"),
        }
        return {
            "schema_version": contracts.PLAN_SCHEMA_VERSION,
            "market_pack": {
                "id": pack.market_pack_id,
                "version": pack.pack_version,
                "country": pack.target_country,
                "locale": pack.target_locale,
            },
            "theme": {"id": theme.theme_id, "topic": topic},
            "persona": {"ref_id": persona_ref, "snapshot": persona_snapshot},
            "look": {
                "ref_id": look_ref,
                "source_type": "look_template",
                "snapshot": look_snapshot,
            },
            "scene": {"ref_id": scene_ref, "snapshot": scene_snapshot},
            "copy": self._build_recipe_copy(
                theme, topic, product, recipe, selected_hook
            ),
            "audio_policy": audio_policy,
            "shots": shots,
            "anchor_slot": recipe.anchor_slot,
            "recipe": {
                "id": recipe.recipe_id,
                "version": recipe.recipe_version,
                "content_goal": recipe.content_goal,
                "hook_strategy": selected_hook,
            },
            "recipe_execution": {
                "recipe_id": recipe.recipe_id,
                "hook_strategy": selected_hook,
                "anchor_slot": recipe.anchor_slot,
                "transform_mode": (
                    "single_frozen_outfit"
                    if recipe.content_goal == "scene_solution"
                    else "controlled_outfit_change"
                ),
                "variant_index": int(variant_index),
            },
            "outfit_plan": outfit_plan,
            "outfit_states": outfit_states,
            "product_facts": product_facts,
        }

    @staticmethod
    def _outfit_state_for_slot(content_goal: str, slot_index: int) -> str:
        if content_goal == "scene_solution":
            return "FINAL"
        if content_goal == "pain_point_solution":
            return "BASE" if slot_index == 2 else "FINAL"
        if content_goal == "visual_transform":
            if slot_index == 2:
                return "BASE"
            if slot_index == 3:
                return "ALT_1"
        return "FINAL"

    @staticmethod
    def _recipe_motion(render_profile, slot: Dict[str, Any]) -> str:
        allowed = list(
            ((render_profile.motion_rules_json if render_profile else {}) or {}).get(
                "allowed"
            ) or []
        )
        role_default = {
            "detail": "detail_zoom",
            "lifestyle": "light_pan",
        }.get(slot.get("slot_role"), "slow_push")
        return role_default if not allowed or role_default in allowed else allowed[0]

    @staticmethod
    def _recipe_transition(render_profile, preset, slot_index: int) -> str:
        if slot_index == 5:
            return "cut"
        profile_rules = (render_profile.transition_rules_json if render_profile else {}) or {}
        preset_rules = preset.transition_rules_json or {}
        profile_allowed = list(profile_rules.get("allowed") or [])
        preset_allowed = set(preset_rules.get("allowed_transitions") or ["cut"])
        allowed = [value for value in profile_allowed if value in preset_allowed]
        requested = str(profile_rules.get("default") or "cut")
        return requested if requested in allowed else (allowed[0] if allowed else "cut")

    def _build_recipe_copy(self, theme, topic, product, recipe, hook_strategy):
        copy = self._build_copy(theme, topic, product)
        copy["hook_strategy"] = hook_strategy
        copy["style_contract"] = dict(recipe.copy_style_json or {})
        copy["copy_generation_instruction"] = {
            "hook_strategy": hook_strategy,
            "title_tone": (recipe.copy_style_json or {}).get("title_tone"),
            "structure": (recipe.copy_style_json or {}).get("structure"),
            "target_market": (theme.applicable_markets_json or [None])[0],
            "human_review_required": True,
        }
        return copy

    def _build_theme_plan(
        self,
        *,
        task,
        account,
        pack,
        preset,
        theme,
        topic,
        persona_ref,
        persona_snapshot,
        look_ref,
        look_snapshot,
        scene_ref,
        scene_snapshot,
        product,
    ) -> Dict[str, Any]:
        return {
            "schema_version": contracts.PLAN_SCHEMA_VERSION,
            "market_pack": {
                "id": pack.market_pack_id,
                "version": pack.pack_version,
                "country": pack.target_country,
                "locale": pack.target_locale,
            },
            "theme": {"id": theme.theme_id, "topic": topic},
            "persona": {"ref_id": persona_ref, "snapshot": persona_snapshot},
            "look": {
                "ref_id": look_ref,
                "source_type": "look_template",
                "snapshot": look_snapshot,
            },
            "scene": {"ref_id": scene_ref, "snapshot": scene_snapshot},
            "copy": self._build_copy(theme, topic, product),
            "audio_policy": {
                "strategy": preset.audio_rules_json.get(
                    "default_strategy", "platform_hot_bgm"
                ),
                "fallback": preset.audio_rules_json.get("fallback_strategy", "no_bgm"),
            },
            "shots": self._build_shots(
                theme, account, preset, persona_ref, look_ref, scene_ref, product
            ),
        }

    def _topic_from_template(
        self, theme: ThemeCatalog, product: Dict[str, Any]
    ) -> str:
        template = theme.default_storyboard_json.get("topic_template") or ""
        if not template:
            raise ContentPlannerError(
                f"theme {theme.theme_id} has no topic_template and no topic given"
            )
        display = product.get("product_name") or product.get("product_id") or "product"
        return template.replace("{product}", str(display))

    def _build_copy(
        self, theme: ThemeCatalog, topic: str, product: Dict[str, Any]
    ) -> Dict[str, Any]:
        rules = theme.content_plan_rules_json
        hashtags = list(rules.get("hashtag_seed", []))
        caption_lines = [topic, str(product.get("product_name") or "").strip()]
        caption = "\n".join(line for line in caption_lines if line)
        return {
            "title": topic,
            "caption": caption,
            "hashtags": hashtags,
            "cover_text": str(rules.get("overlay_text_hint") or ""),
            "overlay_instruction": str(rules.get("overlay_instruction") or ""),
            "copy_status": "draft_needs_human_review",
        }

    def _build_shots(
        self,
        theme: ThemeCatalog,
        account: AccountProfile,
        preset: RenderPreset,
        persona_ref: str,
        look_ref: str,
        scene_ref: str,
        product: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        storyboard = normalize_storyboard_for_preset(
            theme.default_storyboard_json.get("slots") or [], preset
        )
        asset_refs = [
            f"persona:{persona_ref}",
            f"look:{look_ref}",
            f"scene:{scene_ref}",
        ]
        product_refs = [
            f"product_image:{path}"
            for path in product.get("reference_images", [])
        ]
        rules = theme.content_plan_rules_json
        allowed_transitions = set(
            preset.transition_rules_json.get("allowed_transitions") or []
        )
        preset_default_transition = preset.transition_rules_json.get(
            "default", "cut"
        )
        shots: List[Dict[str, Any]] = []
        for slot in storyboard:
            overlay = ""
            if slot.get("slot_index") == 1:
                overlay = str(rules.get("overlay_text_hint") or "")
            source_refs = asset_refs + product_refs
            transition_out = slot.get("transition_out", "short_dissolve")
            if allowed_transitions and transition_out not in allowed_transitions:
                transition_out = preset_default_transition
            shots.append(
                {
                    "slot_index": int(slot["slot_index"]),
                    "slot_role": slot["slot_role"],
                    "purpose": slot.get("purpose", ""),
                    "duration_ms": int(slot["duration_ms"]),
                    "motion_preset": slot.get("motion", "slow_push"),
                    "transition_out": transition_out,
                    "overlay_text": overlay,
                    "generation_prompt": self._shot_prompt(
                        slot, look_ref, scene_ref, product
                    ),
                    "source_refs": source_refs,
                }
            )
        return shots

    def _shot_prompt(
        self,
        slot: Dict[str, Any],
        look_ref: str,
        scene_ref: str,
        product: Dict[str, Any],
    ) -> str:
        """Structured generation hint; the real image prompt is composed by
        the Stage C generator using these anchors (never invented here)."""
        parts = [
            f"slot {slot.get('slot_index')} ({slot.get('slot_role')}): "
            + str(slot.get("purpose", "")),
            f"look ref {look_ref}",
            f"scene ref {scene_ref}",
            "product anchor: "
            + str(product.get("product_name") or product.get("product_id") or ""),
        ]
        return " | ".join(parts)


def normalize_storyboard_for_preset(
    slots: List[Dict[str, Any]], preset: RenderPreset
) -> List[Dict[str, Any]]:
    """Treat Theme durations/transitions as semantic weights, not render truth.

    The Render Preset owns the final duration and allowed transitions.  This
    keeps older 12.5-second themes compatible with the active 10-second preset
    without mutating their narrative structure.
    """
    if not slots:
        raise ContentPlannerError("theme storyboard has no slots")
    target_ms = int(preset.target_duration_ms)
    source_total = sum(max(1, int(slot.get("duration_ms") or 0)) for slot in slots)
    if source_total <= 0:
        raise ContentPlannerError("theme storyboard duration must be positive")

    normalized: List[int] = []
    allocated = 0
    for index, slot in enumerate(slots):
        if index == len(slots) - 1:
            value = target_ms - allocated
        else:
            weight = max(1, int(slot.get("duration_ms") or 0)) / source_total
            value = int(round(target_ms * weight / 100.0)) * 100
            value = max(500, value)
            allocated += value
        normalized.append(value)
    # Rounding/minimum protection can overshoot on pathological inputs. Put
    # the residual on the largest slot while preserving the 500ms contract.
    residual = target_ms - sum(normalized)
    if residual:
        largest = max(range(len(normalized)), key=normalized.__getitem__)
        normalized[largest] += residual
    if any(value < 500 for value in normalized):
        raise ContentPlannerError(
            f"render target {target_ms}ms cannot fit {len(slots)} slots at >=500ms"
        )

    transition_rules = preset.transition_rules_json or {}
    allowed = list(transition_rules.get("allowed_transitions") or ["cut"])
    default_transition = str(transition_rules.get("default") or allowed[0])
    output: List[Dict[str, Any]] = []
    for index, (slot, duration_ms) in enumerate(zip(slots, normalized)):
        item = dict(slot)
        item["duration_ms"] = duration_ms
        requested = str(slot.get("transition_out") or default_transition)
        item["transition_out"] = (
            "cut"
            if index == len(slots) - 1
            else requested if requested in allowed else default_transition
        )
        output.append(item)
    return output
