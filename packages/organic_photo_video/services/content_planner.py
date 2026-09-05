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
from copy import deepcopy
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
from services.batch_diversity_planner import shot_grammar_profile


class ContentPlannerError(ValueError):
    pass


PLANNABLE_STATUSES = (TASK_DRAFT, TASK_PLANNED)


def _presentation_profile(account) -> Dict[str, Any]:
    """Return the account-owned visual contract for a new plan.

    This is deliberately a generation direction, not a quality gate.  A
    profile is frozen into the task so later changes to the account settings do
    not mutate historical work.
    """
    rules = account.operating_rules_json or {}
    # Deployment receipt: a checked-in required profile must agree with RDS.
    # Comparing only the visual/selection contract avoids overwriting account
    # data and catches the previous local-only deployment before model calls.
    from pathlib import Path
    expected_path = Path(__file__).resolve().parents[1] / "config" / "accounts" / f"{account.account_id}.json"
    if expected_path.is_file():
        expected = json.loads(expected_path.read_text(encoding="utf-8")).get("operating_rules") or {}
        if expected.get("required_presentation_profile"):
            for field in ("required_presentation_profile", "presentation_profile", "look_selection_mode"):
                if rules.get(field) != expected.get(field):
                    raise ContentPlannerError(f"runtime account contract differs from deployed {field}; sync and read back before planning")
    raw = dict(rules.get("presentation_profile") or {})
    required = str(rules.get("required_presentation_profile") or "")
    if required and (raw.get("profile_id") != required or raw.get("background_mode") != "solid_color"):
        raise ContentPlannerError("required presentation profile is missing/mismatched; sync runtime account before generating")
    if not raw:
        return {}
    if str(raw.get("background_mode") or "") != "solid_color":
        return {}
    return {
        "profile_id": str(raw.get("profile_id") or "PURE_COLOR_FASHION_V1"),
        "revision": int(raw.get("revision") or 1),
        "item_count_policy": str(raw.get("item_count_policy") or "legacy_three"),
        "background_mode": "solid_color",
        "background_color": str(raw.get("background_color") or "#F6F5F2"),
        "background_alternates": list(raw.get("background_alternates") or []),
        "full_body_occupancy": str(raw.get("full_body_occupancy") or "85-92%"),
        "proportion_direction": str(
            raw.get("proportion_direction")
            or "自然修长时装比例，约7.6-8头身；不得机械拉伸"
        ),
    }


def apply_presentation_shots(plan: Dict[str, Any], profile: Dict[str, Any]) -> None:
    """Compile one visual contract, retaining complete bodies during rendering."""
    background = str(profile["background_color"])
    poses = {1: "自然正面站立，轻微重心偏移", 2: "正面站立，手臂不遮挡腰线",
             3: "三分之四侧身，轻微错步", 5: "侧后四十五度站立，轻微回看"}
    multi_look = (plan.get("recipe_execution") or {}).get("content_goal") == "multi_look"
    if multi_look:
        poses.update({4: "正面站立，腰线和鞋履完整", 5: "正面站立，轻微重心变化"})
    for shot in plan.get("shots") or []:
        index = int(shot["slot_index"])
        shot["render_background"] = background
        if shot.get("shot_kind") == "composite_board":
            shot["board_spec"]["background_color"] = background
            shot["board_spec"]["item_count_policy"] = profile.get("item_count_policy", "legacy_three")
            continue
        composition = dict(shot.get("composition_contract") or {})
        full = multi_look or index in {2, 3, 5} or (index == 1 and composition.get("framing") not in {
            "waist_up_product", "waist_up_detail", "face_closeup"})
        if full:
            composition = {"framing": "full_body", "camera_angle": "waist_height_level",
                "pose": poses.get(index, poses[2]), "prop_policy": "no_new_props",
                "instruction": f"完整全身；人物占画面高度约{profile['full_body_occupancy']}，头顶、鞋底留出安全空间；"
                    "相机腰部高度且保持足够距离，水平拍摄，腰线清楚、腿部自然修长；不得机械拉伸。",
                "forbidden": ["downward_camera", "cropped_limbs", "unplanned_props"]}
            shot.update(fit_mode="contain", motion_preset="static_hold")
        elif shot.get("slot_role") == "detail":
            composition = {"framing": "garment_neck_to_upper_thigh", "camera_angle": "garment_height_level",
                "pose": "relaxed_hands_clear_of_garment", "prop_policy": "no_new_props",
                "instruction": "从颈部至大腿上段展示商品，完整保留衣领、袖子、门襟、衣摆和腰线；衣服为主体，不拍美颜大头照。",
                "forbidden": ["face_dominant", "invented_product_details"]}
            shot.update(fit_mode="contain", motion_preset="static_hold")
        else:
            composition["instruction"] = "上半身商品构图，衣服占据主体，保留自然皮肤纹理；不得使用面部特写。"
            composition["pose"] = "relaxed_hands_clear_of_garment"
            shot["motion_preset"] = "static_hold"
        shot["composition_contract"] = composition
        shot["camera_hint"] = composition["instruction"]
        shot["purpose"] = "展示当前冻结穿搭状态；" + str(composition["pose"])
    if (plan.get("recipe_execution") or {}).get("content_goal") == "outfit_breakdown":
        shots = plan.get("shots") or []
        if len(shots) == 5 and sum(int(s["duration_ms"]) for s in shots) == 10000:
            for shot, duration in zip(sorted(shots, key=lambda s: s["slot_index"]), (2400, 1800, 2000, 1800, 2000)):
                shot["duration_ms"] = duration


_COMPOSITION_CONTRACTS: Dict[int, Dict[str, Any]] = {
    1: {
        "framing": "near_full",
        "camera_angle": "front_eye_level",
        "pose": "result_first_natural",
        "prop_policy": "only_from_scene_or_purpose",
        "instruction": "正面近全身，最终效果先行，商品与整体比例一眼可见",
        "forbidden": ["unplanned_props", "complex_props"],
    },
    2: {
        "framing": "full_body",
        "camera_angle": "front_eye_level",
        "pose": "neutral_before",
        "prop_policy": "only_from_scene_or_purpose",
        "instruction": "正面全身展示普通或改造前穿法，腰线和上下装关系必须完整可见",
        "forbidden": [
            "repeat_previous_composition",
            "repeat_previous_pose",
            "unplanned_props",
        ],
    },
    3: {
        "framing": "medium_full",
        "camera_angle": "three_quarter_dynamic",
        "pose": "scene_matched_action",
        "prop_policy": "only_from_scene_or_purpose",
        "instruction": "斜侧或动态中全身，动作只能来自当前主题、场景或本镜头目的",
        "forbidden": [
            "repeat_previous_composition",
            "repeat_previous_pose",
            "unplanned_props",
        ],
    },
    4: {
        "framing": "waist_up_detail",
        "camera_angle": "front_or_three_quarter_close",
        "pose": "show_confirmed_product_or_proportion_detail",
        "prop_policy": "no_new_props",
        "instruction": "腰部至头部或商品局部近景，突出已确认的衣长、腰线、领口或材质观感",
        "forbidden": [
            "full_body",
            "shoes",
            "large_floor_area",
            "invented_product_details",
            "repeat_previous_composition",
        ],
    },
    5: {
        "framing": "near_full",
        "camera_angle": "rear_three_quarter",
        "pose": "look_back_payoff",
        "prop_policy": "only_from_scene_or_purpose",
        "instruction": "侧后45度或背面回眸，以第二机位交付完整最终造型",
        "forbidden": [
            "front_pose",
            "repeat_previous_composition",
            "repeat_previous_pose",
            "unplanned_props",
        ],
    },
}


def _composition_contract(slot_index: int, *, content_goal: str = "") -> Dict[str, Any]:
    if content_goal == "multi_look":
        return {"framing": "full_body", "camera_angle": "waist_height_level",
                "pose": "front_natural_weight_shift", "prop_policy": "no_new_props",
                "instruction": "当前LOOK完整全身，正面或轻微三分之四侧身；相机腰部高度水平拍摄；头顶和鞋底完整，不做局部细节或背面收尾。",
                "forbidden": ["cropped_limbs", "face_closeup", "detail_crop", "back_only", "previous_look_clothing"]}
    # Outfit-breakdown P2 is an internal anchor for a FINAL look.  It must not
    # inherit the generic "before" posture merely because it occupies slot 2.
    if str(content_goal) == "outfit_breakdown" and int(slot_index) == 2:
        return {
            "framing": "full_body",
            "camera_angle": "waist_height_level",
            "pose": "final_look_natural_weight_shift",
            "prop_policy": "only_from_scene_or_purpose",
            "instruction": (
                "最终穿搭的完整全身展示；相机位于腰部高度、保持水平且有足够拍摄距离；"
                "不得俯拍，不得缩短腿部或躯干，腰线、裤脚和鞋履完整可见"
            ),
            "forbidden": ["before_outfit", "downward_camera", "cropped_limbs", "unplanned_props"],
        }
    if str(content_goal) == "outfit_breakdown" and int(slot_index) == 4:
        return {
            "framing": "garment_neck_to_upper_thigh",
            "camera_angle": "garment_height_level",
            "pose": "relaxed_hands_clear_of_garment",
            "prop_policy": "no_new_props",
            "instruction": (
                "商品证明镜头：从颈部至大腿上段取景，完整保留衣领、肩袖、门襟、衣摆和腰线；"
                "脸可以自然出画，以衣物为主体，不做自拍或美妆特写。"
                "仅展示商品参考能验证的结构，无细节参考时保持中景，不臆造微距纹理；"
                "保留自然肤质，脸部、手部和衣物清晰度一致，不磨皮。"
            ),
            "forbidden": ["face_dominant", "beauty_selfie", "skin_smoothing", "invented_product_details"],
        }
    raw = _COMPOSITION_CONTRACTS.get(int(slot_index), _COMPOSITION_CONTRACTS[1])
    return {**raw, "forbidden": list(raw.get("forbidden") or [])}


def _has_detail_reference(product: Dict[str, Any]) -> bool:
    roles = product.get("reference_roles") or {}
    return isinstance(roles, dict) and bool(roles.get("detail"))


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
        presentation_profile = _presentation_profile(account)
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
        product = deepcopy(task.product_snapshot_json.get("product", {}))
        topic = (
            topic_text
            or task.topic_text
            or self._topic_from_template(theme, product)
        )

        persona_ref, persona_snapshot = self._resolve_persona(account, product)
        persona_lock = self._freeze_persona_lock(persona_ref, persona_snapshot)
        scene_ref, scene_snapshot = self._resolve_scene(account, product)
        if recipe is not None and recipe.content_goal == "multi_look":
            sequence, selection = self._resolve_multi_looks(account, product, scene_ref, task)
            product["planned_look_sequence"] = sequence
            product["multi_look_selection"] = selection
            look_ref, look_snapshot = sequence[0]["look_ref"], sequence[0]["snapshot"]
            if not presentation_profile:
                presentation_profile = {"profile_id": "MULTI_LOOK_PURE_COLOR_V1", "revision": 1,
                    "background_mode": "solid_color", "background_color": "#F1F3F5", "background_alternates": [],
                    "full_body_occupancy": "85-92%", "item_count_policy": "dynamic_2_or_3",
                    "proportion_direction": "自然修长时装比例；不得机械拉伸身体或改变商品版型"}
        else:
            look_ref, look_snapshot = self._resolve_look(
                account, product, theme=theme, variant_index=variant_index
            )

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
        if recipe is not None and recipe.content_goal == "multi_look":
            topic = plan["copy"]["title"]
        if presentation_profile:
            plan["presentation_profile"] = presentation_profile
            # The generator receives recipe_execution through HeroFirst.  Keep
            # the same frozen contract for recipe and legacy theme plans.
            plan.setdefault("recipe_execution", {})["presentation_profile"] = (
                presentation_profile
            )
            if ((plan.get("recipe_execution") or {}).get("content_goal") in {"outfit_breakdown", "multi_look"}):
                # This editorial composition is intentionally not part of the
                # old four-layout rotation.  Variations happen inside the
                # reference composition, never by moving the person right or
                # turning the cover into a grid.
                plan["variation_plan"] = {
                    **dict(plan.get("variation_plan") or {}),
                    "layout_variant": "REF_LEFT_HERO",
                    "palette_variant": "REFERENCE_WHITE",
                }
                for shot in plan.get("shots") or []:
                    if shot.get("shot_kind") == "composite_board":
                        shot["board_spec"] = {
                            **dict(shot.get("board_spec") or {}),
                            "layout_id": "LAYOUT_OUTFIT_REFERENCE_LEFT_V1",
                            "layout_version": 2 if recipe is not None and recipe.content_goal == "multi_look" else 1,
                            "layout_variant": "REF_LEFT_HERO",
                            "palette_variant": "REFERENCE_WHITE",
                        }
            apply_presentation_shots(plan, presentation_profile)
        plan["persona"]["lock"] = persona_lock
        # Admit actual library-backed selections now, and recheck frozen files
        # immediately before generation. Legacy planner-only fixtures can still
        # construct plans without claiming they are generation-ready.
        if (persona_snapshot.get("source") or {}).get("authority"):
            from services.asset_readiness import require_generation_assets
            plan["asset_readiness"] = require_generation_assets(account, product, plan)
        plan["render_contract"] = {
            "preset_id": preset.render_preset_id,
            "target_duration_ms": preset.target_duration_ms,
            "fps": preset.fps,
            "transition_default": (preset.transition_rules_json or {}).get(
                "default", "cut"
            ),
        }
        if recipe is not None and recipe.content_goal == "multi_look":
            plan["render_contract"]["target_duration_ms"] = 6000
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
            if quality_profile.quality_profile_id == "QUALITY_OUTFIT_BREAKDOWN_V2":
                # New tasks opt in explicitly. Historical frozen contracts and
                # the original V1 profile retain their original decision policy.
                plan["quality_contract"].update({
                    "assessment_schema_version": 2,
                    "decision_policy": "hard_gates_only",
                })
        # Workflow V2 is introduced with outfit-breakdown as the pilot.  Older
        # recipes and historical plans keep their direct V1 generation path.
        if recipe is not None and (recipe.content_goal in {"outfit_breakdown", "multi_look"} or presentation_profile):
            plan["workflow_version"] = 2
        signature_axes = {
            **dict((plan.get("batch_diversity") or {}).get("axes") or {}),
            "product_id": str(product.get("product_id") or task.product_id),
            "reference_pack_id": str(product.get("reference_pack_id") or ""),
            "reference_pack_version": int(product.get("reference_pack_version") or 0),
            "variant_key": str(product.get("variant_key") or "default"),
            "persona_ref": str(persona_ref or ""),
            "look_ref": str(look_ref or ""),
            "scene_ref": str(scene_ref or ""),
            "theme_id": str(theme.theme_id),
            "recipe_id": str((plan.get("recipe") or {}).get("id") or ""),
            "hook_strategy": str(
                (plan.get("recipe_execution") or {}).get("hook_strategy") or ""
            ),
            "visible_silhouette": str(product.get("planned_visible_silhouette") or ""),
            "scene_zone": str(product.get("planned_scene_zone") or ""),
            "shot_grammar": str(product.get("planned_shot_grammar") or "LEGACY"),
            "layout_variant": str((plan.get("variation_plan") or {}).get("layout_variant") or ""),
            "copy_variant": str((plan.get("variation_plan") or {}).get("copy_variant") or ""),
            "palette_variant": str((plan.get("variation_plan") or {}).get("palette_variant") or ""),
            "presentation_profile": str(
                (plan.get("presentation_profile") or {}).get("profile_id") or ""
            ),
        }
        if recipe is not None and recipe.content_goal == "multi_look":
            from services.multi_look_planner import sequence_axes
            signature_axes.update(look_sequence=sequence_axes(plan["outfit_sequence"]), actual_look_count=plan["actual_shot_count"])
        plan["content_signature"] = {
            "sha256": hashlib.sha256(json.dumps(
                signature_axes, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")).hexdigest(),
            "axes": signature_axes,
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
            workflow_version=int(plan.get("workflow_version") or 1),
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

    def _resolve_look(self, account, product, *, theme=None, variant_index: int = 1):
        from services.look_selection import prepare_look_for_policy
        allowed = list(account.allowed_look_refs_json or [])
        rules = account.operating_rules_json or {}
        if rules.get("look_selection_mode") == "auto_library" and self._assets is not None:
            allowed = self._assets.list_look_ids()
        excluded = set(rules.get("excluded_look_refs") or rules.get("exclude_look_refs") or [])
        allowed = [ref for ref in allowed if ref not in excluded]
        if not allowed:
            raise ContentPlannerError(
                f"account {account.account_id} allows no look refs"
            )
        requested = str(product.get("planned_look_ref") or "").strip()
        if requested.upper() in {"AUTO", "AUTOMATIC", "自动匹配"}:
            requested = ""
        if requested:
            if requested not in allowed:
                raise ContentPlannerError(
                    f"planned look {requested} is outside account allow-list"
                )
            snapshot = self._assets.get_look(requested) if self._assets is not None else {}
            snapshot = prepare_look_for_policy(account, snapshot) if snapshot else snapshot
            if not self._look_matches_product(snapshot, product):
                raise ContentPlannerError(
                    f"planned look {requested} is incompatible with product {product.get('product_id') or ''}"
                )
            return requested, snapshot

        candidates = []
        for order, look_ref in enumerate(allowed):
            try:
                snapshot = self._assets.get_look(look_ref) if self._assets is not None else {}
                snapshot = prepare_look_for_policy(account, snapshot) if snapshot else snapshot
            except (KeyError, ValueError):
                continue
            if not self._look_matches_product(snapshot, product):
                continue
            candidates.append(
                (self._look_score(snapshot, product, theme), order, look_ref, snapshot)
            )
        if not candidates:
            raise ContentPlannerError(
                f"account {account.account_id} has no compatible look for product {product.get('product_id') or ''}"
            )
        candidates.sort(key=lambda item: (-item[0], item[1], item[2]))
        selected = candidates[(max(int(variant_index), 1) - 1) % len(candidates)]
        return selected[2], selected[3]

    def _resolve_multi_looks(self, account, product, scene_ref, task):
        from services.asset_compatibility import compatibility_errors
        from services.multi_look_planner import compatible_multi_look_candidates, history_look_usage, select_multi_look_sequence
        from services.outfit_planner import freeze_multi_look_state

        frozen = product.get("planned_look_sequence")
        if frozen is not None:
            if not isinstance(frozen, list) or not 1 <= len(frozen) <= 5:
                raise ContentPlannerError("frozen multi-look sequence must contain 1..5 entries")
            sequence, fingerprints = deepcopy(frozen), set()
            for index, entry in enumerate(sequence, start=1):
                snapshot = entry.get("snapshot") or {}
                if not snapshot.get("recipe") or entry.get("look_ref") != (snapshot.get("ref_id") or snapshot.get("look_id")):
                    raise ContentPlannerError("frozen multi-look snapshot/ref mismatch")
                if compatibility_errors(snapshot, product, scene_ref, pure_color=True):
                    raise ContentPlannerError("frozen multi-look snapshot is incompatible with target product")
                actual = freeze_multi_look_state(snapshot, index=index)["outfit_fingerprint"]
                if entry.get("fingerprint") != actual or actual in fingerprints:
                    raise ContentPlannerError("frozen multi-look fingerprint mismatch/duplicate")
                if entry.get("slot_index") != index or entry.get("state_id") != f"LOOK_{index:02d}":
                    raise ContentPlannerError("frozen multi-look sequence order changed")
                fingerprints.add(actual)
            return sequence, deepcopy(product.get("multi_look_selection") or {"actual_count": len(sequence), "requested_count": 5, "source": "frozen_batch"})
        if self._assets is None:
            raise ContentPlannerError("multi-look planning requires the existing template library")
        candidates, rejected = compatible_multi_look_candidates(account, self._assets, product, scene_ref)
        getter = getattr(self._repository, "list_recent_diversity_axes", None)
        record_id = str(task.feishu_record_id or task.source_record_id or "").split(":")[0]
        history = getter(account.account_id, exclude_source_record_id=record_id, limit=100) if callable(getter) else []
        try:
            sequence, selection = select_multi_look_sequence(candidates,
                first_ref=str(product.get("planned_look_ref") or ""), history=history_look_usage(history))
        except ValueError as exc:
            raise ContentPlannerError(str(exc)) from exc
        return sequence, {**selection, "rejections": rejected, "history_records_considered": len(history or [])}

    @staticmethod
    def _look_matches_product(snapshot: Dict[str, Any], product: Dict[str, Any]) -> bool:
        if not snapshot:
            return True
        from services.styling_normalizer import normalize_product_id
        product_id = normalize_product_id(product.get("product_id"))
        codes = {normalize_product_id(value) for value in snapshot.get("applicable_product_codes") or [] if normalize_product_id(value)}
        if codes and "*" not in codes and product_id not in codes:
            return False
        category = str(product.get("category") or "").strip().lower()
        types = {
            str(value).strip().lower()
            for value in (snapshot.get("compatibility") or {}).get("product_types") or []
            if str(value).strip()
        }
        if category and types and category not in types:
            return False
        return True

    @staticmethod
    def _look_score(snapshot: Dict[str, Any], product: Dict[str, Any], theme: Any) -> int:
        product_id = str(product.get("product_id") or "").strip()
        codes = {str(value).strip() for value in snapshot.get("applicable_product_codes") or []}
        score = 100 if product_id and product_id in codes else (25 if "*" in codes else 0)
        compatibility = snapshot.get("compatibility") or {}
        category = str(product.get("category") or "").strip().lower()
        if category and category in {
            str(value).strip().lower() for value in compatibility.get("product_types") or []
        }:
            score += 25
        theme_text = " ".join(
            str(value or "").lower()
            for value in (
                getattr(theme, "theme_id", ""), getattr(theme, "theme_key", ""),
                getattr(theme, "theme_name", ""),
            )
        )
        for marker in list(compatibility.get("demonstration_modes") or []) + list(compatibility.get("scene_families") or []):
            normalized = str(marker or "").strip().lower()
            if normalized and any(token in theme_text for token in normalized.split("_")):
                score += 40
                break
        recipe = snapshot.get("recipe") or {}
        score += min(sum(1 for value in recipe.values() if value) * 4, 24)
        score += min(max(int(snapshot.get("priority") or 0), 0) // 10, 10)
        return score

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
        scene_zone = str(product.get("planned_scene_zone") or "").strip()
        if scene_zone:
            snapshot = {**snapshot, "selected_zone": scene_zone}
        return scene_ref, snapshot

    def _alternate_look(self, account, product, look_ref, look_snapshot, content_goal):
        if content_goal != "visual_transform" or self._assets is None:
            return None
        from services.styling_normalizer import outfit_fingerprint
        from services.look_selection import prepare_look_for_policy
        rules = account.operating_rules_json or {}
        refs = (self._assets.list_look_ids() if rules.get("look_selection_mode") == "auto_library"
                else list(account.allowed_look_refs_json or []))
        excluded = set(rules.get("excluded_look_refs") or rules.get("exclude_look_refs") or [])
        requested = str(product.get("planned_alternate_look_ref") or "")
        if requested:
            refs = [ref for ref in refs if ref == requested]
        current_key = outfit_fingerprint(look_snapshot)
        for ref in refs:
            if ref == look_ref or ref in excluded:
                continue
            try:
                snapshot = prepare_look_for_policy(account, self._assets.get_look(ref))
            except (KeyError, ValueError):
                continue
            if account.status == "active" and snapshot.get("status") != "enabled":
                continue
            if self._look_matches_product(snapshot, product) and outfit_fingerprint(snapshot) != current_key:
                return snapshot
        return None

    @staticmethod
    def _grammar_shot(product: Dict[str, Any], slot_index: int) -> Dict[str, Any]:
        grammar_id = str(product.get("planned_shot_grammar") or "").strip()
        if not grammar_id:
            return {}
        profile = shot_grammar_profile(grammar_id)
        shots = list(profile.get("shots") or [])
        raw = shots[slot_index - 1] if 0 < slot_index <= len(shots) else {}
        return {"grammar_id": grammar_id, **dict(raw)}

    @classmethod
    def _apply_grammar_to_shot(
        cls, product: Dict[str, Any], slot: Dict[str, Any], *, content_goal: str = ""
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        index = int(slot.get("slot_index") or 1)
        grammar = cls._grammar_shot(product, index)
        effective = dict(slot)
        action = str(grammar.get("action") or "").strip()
        if action:
            effective["purpose"] = "；".join(
                value for value in (action, str(slot.get("purpose") or "").strip())
                if value
            )
        if grammar.get("camera_hint"):
            effective["camera_hint"] = grammar["camera_hint"]
        composition = _composition_contract(index, content_goal=content_goal)
        for key in ("framing", "camera_angle", "pose"):
            if grammar.get(key):
                composition[key] = grammar[key]
        if action or grammar.get("camera_hint"):
            composition["instruction"] = "；".join(
                value for value in (
                    action,
                    str(grammar.get("camera_hint") or "").strip(),
                ) if value
            )
        if content_goal == "multi_look":
            composition = _composition_contract(index, content_goal=content_goal)
            effective.update(camera_hint=composition["instruction"], purpose=slot.get("purpose", "完整穿搭"))
        elif content_goal == "outfit_breakdown" and index in {2, 4}:
            # Role constraints outrank generic variant camera/pose grammar.
            composition = _composition_contract(index, content_goal=content_goal)
            effective["camera_hint"] = composition["instruction"]
            if index == 4:
                effective["purpose"] = slot.get("purpose", "商品结构展示")
        return effective, composition

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
            build_multi_look_states,
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
            look=dict(look_snapshot or {}),
            content_goal=recipe.content_goal,
        )
        outfit_states = build_outfit_states(
            outfit_plan, content_goal=recipe.content_goal,
            alternate_look=self._alternate_look(account, product, look_ref, look_snapshot, recipe.content_goal),
        )
        multi_sequence = list(product.get("planned_look_sequence") or []) if recipe.content_goal == "multi_look" else []
        if multi_sequence:
            outfit_states = build_multi_look_states(outfit_plan, [entry["snapshot"] for entry in multi_sequence])
            for entry in multi_sequence:
                if outfit_states[entry["state_id"]]["outfit_fingerprint"] != entry["fingerprint"]:
                    raise ContentPlannerError("multi-look executable state differs from frozen template fingerprint")
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
        board_variation: Dict[str, Any] = {}
        if recipe.content_goal in {"outfit_breakdown", "multi_look"}:
            from services.board_layout import presentation_variant

            board_variation = presentation_variant(
                # All outputs from one Feishu row share the same base offset;
                # variant_index then rotates the axes without accidental hash
                # collisions caused by per-task source ids.
                record_key=str(
                    task.feishu_record_id or task.source_record_id or task.task_id
                ),
                variant_index=variant_index,
            )
            if product.get("planned_layout_variant"):
                board_variation["layout_variant"] = str(
                    product["planned_layout_variant"]
                )
        for slot in recipe.story_structure_json:
            index = int(slot["slot_index"])
            if multi_sequence and index > len(multi_sequence):
                continue
            shot_kind = str(slot.get("shot_kind") or "generated_photo")
            if shot_kind == "composite_board":
                effective_slot = dict(slot)
                composition_contract = {
                    "framing": "programmatic_board",
                    "camera_angle": "not_applicable",
                    "pose": "derived_from_anchor",
                    "prop_policy": "verified_assets_only",
                    "instruction": "按冻结 Layout 合成，不调用图片模型生成文字或商品卡",
                    "forbidden": ["fake_commerce_data", "platform_ui", "generated_text"],
                }
            else:
                effective_slot, composition_contract = self._apply_grammar_to_shot(
                    product, slot, content_goal=recipe.content_goal
                )
            outfit_state_ref = self._outfit_state_for_slot(
                recipe.content_goal, index
            )
            duration = int(
                timeline.get(str(index))
                or timeline.get(index)
                or preset.target_duration_ms // max(recipe.shot_count, 1)
            )
            motion_preset = self._recipe_motion(render_profile, effective_slot)
            if shot_kind == "composite_board":
                motion_preset = "static_hold"
            if slot.get("slot_role") == "detail" and not _has_detail_reference(product):
                motion_preset = "upper_body_focus"
            if recipe.content_goal == "outfit_breakdown" and index == 4:
                motion_preset = "slow_push"
            if multi_sequence:
                from services.multi_look_planner import multi_look_durations
                duration = multi_look_durations(len(multi_sequence))[index - 1]
                motion_preset = "static_hold"
            shot_payload = {
                    "slot_index": index,
                    "slot_role": slot.get("slot_role", "hero"),
                    "shot_kind": shot_kind,
                    "fit_mode": "contain" if shot_kind == "composite_board" else "cover",
                    "purpose": effective_slot.get("purpose", ""),
                    "duration_ms": duration,
                    "motion_preset": motion_preset,
                    "transition_out": self._recipe_transition(render_profile, preset, index),
                    "overlay_text": (
                        str(rules.get("overlay_text_hint") or "")
                        if index == 1
                        else ""
                    ),
                    "generation_prompt": self._shot_prompt(
                        effective_slot, look_ref, scene_ref, product
                    ),
                    "source_refs": asset_refs + product_refs,
                    "narrative_function": slot.get("narrative_function"),
                    "product_focus": str(product_facts.get("category") or ""),
                    "camera_hint": effective_slot.get("camera_hint") or "",
                    "composition_contract": composition_contract,
                    "shot_grammar": str(product.get("planned_shot_grammar") or "LEGACY"),
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
            if shot_kind == "composite_board":
                shot_payload["generation_prompt"] = ""
                shot_payload["overlay_text"] = ""
                shot_payload["overlay_spec"] = {"enabled": False, "burn_in": True}
                shot_payload["board_spec"] = {
                    "layout_id": "LAYOUT_OUTFIT_BREAKDOWN_V2",
                    "layout_version": 2,
                    "layout_variant": board_variation.get("layout_variant", "LEFT_HERO"),
                    "palette_variant": board_variation.get("palette_variant", "WARM_WHITE"),
                    "copy_variant": board_variation.get("copy_variant", selected_hook),
                    "source_person_slot": recipe.anchor_slot,
                    "fit_mode": "contain",
                    "commerce_policy": "verified_only",
                    "decomposition_required": True,
                    "item_contrast_policy": "soft_silhouette_v1",
                    "item_layout_policy": "compact_stack_v2",
                }
            if multi_sequence:
                entry = multi_sequence[index - 1]
                shot_payload.update(fit_mode="contain", source_look_ref=entry["look_ref"],
                    outfit_fingerprint=entry["fingerprint"],
                    source_refs=[f"persona:{persona_ref}", f"look:{entry['look_ref']}", f"scene:{scene_ref}"] + product_refs,
                    overlay_text="", overlay_spec={"enabled": False, "burn_in": False},
                    transition_out="cut", transition_hint="cut")
                if shot_kind == "composite_board":
                    shot_payload["board_spec"].update(copy_variant="multi_look", item_count_policy="dynamic_2_or_3",
                        source_person_slot=1, source_person_artifact="anchor_photo", source_outfit_state_ref="LOOK_01")
                else:
                    shot_payload["generation_prompt"] = self._shot_prompt(effective_slot, entry["look_ref"], scene_ref, product)
            shots.append(shot_payload)
        audio_policy = {
            "strategy": preset.audio_rules_json.get(
                "default_strategy", "platform_hot_bgm"
            ),
            "fallback": preset.audio_rules_json.get("fallback_strategy", "no_bgm"),
        }
        result = {
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
                theme, topic, product, recipe, selected_hook, account
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
                "content_goal": recipe.content_goal,
                "hook_strategy": selected_hook,
                "anchor_slot": recipe.anchor_slot,
                "transform_mode": (
                    "independent_frozen_looks" if multi_sequence else
                    "single_frozen_outfit"
                    if recipe.content_goal in {"scene_solution", "outfit_breakdown"}
                    else "controlled_outfit_change"
                ),
                "variant_index": int(variant_index),
                "locale": pack.target_locale,
            },
            "outfit_plan": outfit_plan,
            "outfit_states": outfit_states,
            "product_facts": product_facts,
            "batch_diversity": dict(product.get("batch_diversity") or {}),
            "shot_grammar": {
                "id": str(product.get("planned_shot_grammar") or "LEGACY"),
                "profile": shot_grammar_profile(
                    str(product.get("planned_shot_grammar") or "")
                ).get("name"),
            },
            "variation_plan": board_variation,
        }
        if multi_sequence:
            count = len(multi_sequence)
            result.update(actual_shot_count=count, outfit_sequence=deepcopy(multi_sequence),
                          multi_look_selection=deepcopy(product.get("multi_look_selection") or {}), workflow_version=2)
            result["recipe_execution"].update(actual_shot_count=count, shot_count_policy="available_distinct_looks",
                item_count_policy="dynamic_2_or_3", review_mode="technical_only")
            result["anchor_photo_spec"] = {
                **deepcopy(shots[0]), "shot_kind": "generated_photo", "fit_mode": "contain",
                "outfit_state_ref": "LOOK_01", "composition_contract": _composition_contract(1, content_goal="multi_look"),
                "generation_prompt": self._shot_prompt({"purpose": "Look A 完整全身独立原图", "camera_hint": "正面完整全身"}, look_ref, scene_ref, product),
                "purpose": "Look A 原图，仅供P1拆解，不作为第二页复用", "board_spec": {},
            }
            title = f"{count} ลุคจากไอเทมชิ้นเดียว"
            result["copy"].update(title=title, caption=title + "\n" + " · ".join(f"LOOK {i:02d}" for i in range(1, count + 1)),
                cover_text=title, actual_look_count=count, actual_page_count=count,
                overlay_instruction="按实际LOOK编号；不声称未生成的穿搭套数")
            result["copy"]["copy_generation_instruction"]["actual_look_count"] = count
            result["theme"]["topic"] = title
        return result

    @staticmethod
    def _outfit_state_for_slot(content_goal: str, slot_index: int) -> str:
        if content_goal == "multi_look":
            return f"LOOK_{slot_index:02d}"
        if content_goal in {"scene_solution", "outfit_breakdown"}:
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

    def _build_recipe_copy(
        self, theme, topic, product, recipe, hook_strategy, account
    ):
        human_review_required = bool(
            (account.operating_rules_json or {}).get(
                "human_review_required", True
            )
        )
        copy = self._build_copy(
            theme,
            topic,
            product,
            human_review_required=human_review_required,
        )
        copy["hook_strategy"] = hook_strategy
        copy["style_contract"] = dict(recipe.copy_style_json or {})
        copy["copy_generation_instruction"] = {
            "hook_strategy": hook_strategy,
            "title_tone": (recipe.copy_style_json or {}).get("title_tone"),
            "structure": (recipe.copy_style_json or {}).get("structure"),
            "target_market": (theme.applicable_markets_json or [None])[0],
            "human_review_required": human_review_required,
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
            "copy": self._build_copy(
                theme,
                topic,
                product,
                human_review_required=bool(
                    (account.operating_rules_json or {}).get(
                        "human_review_required", True
                    )
                ),
            ),
            "audio_policy": {
                "strategy": preset.audio_rules_json.get(
                    "default_strategy", "platform_hot_bgm"
                ),
                "fallback": preset.audio_rules_json.get("fallback_strategy", "no_bgm"),
            },
            "shots": self._build_shots(
                theme, account, preset, persona_ref, look_ref, scene_ref, product
            ),
            "batch_diversity": dict(product.get("batch_diversity") or {}),
            "shot_grammar": {
                "id": str(product.get("planned_shot_grammar") or "LEGACY"),
                "profile": shot_grammar_profile(
                    str(product.get("planned_shot_grammar") or "")
                ).get("name"),
            },
        }

    def _topic_from_template(
        self, theme: ThemeCatalog, product: Dict[str, Any]
    ) -> str:
        template = theme.default_storyboard_json.get("topic_template") or ""
        if not template:
            raise ContentPlannerError(
                f"theme {theme.theme_id} has no topic_template and no topic given"
            )
        display = self._safe_product_display_name(product)
        category = str(product.get("category") or "").strip().lower()
        if category == "outerwear" and ("เดรส" in template or "连衣裙" in template):
            template = "เสื้อตัวนอกตัวเดียว 3 ลุค: {product} แมทช์ได้หลายแบบ"
        topic = template.replace("{product}", display)
        suffix = str(product.get("planned_title_suffix") or "").strip()
        return f"{topic} · {suffix}" if suffix else topic

    @staticmethod
    def _safe_product_display_name(product: Dict[str, Any]) -> str:
        name = str(product.get("product_name") or "").strip()
        internal_placeholders = {
            "目标外套", "目标连衣裙", "目标上装", "目标下装", "目标商品",
        }
        if name and not name.isdigit() and name not in internal_placeholders:
            return name
        category = str(product.get("category") or "").strip().lower()
        return {
            "outerwear": "เสื้อตัวนอกตัวนี้",
            "dress": "เดรสตัวนี้",
            "top": "เสื้อตัวนี้",
            "bottom": "กางเกงตัวนี้",
        }.get(category, "ไอเทมชิ้นนี้")

    def _build_copy(
        self,
        theme: ThemeCatalog,
        topic: str,
        product: Dict[str, Any],
        *,
        human_review_required: bool = True,
    ) -> Dict[str, Any]:
        rules = theme.content_plan_rules_json
        hashtags = list(rules.get("hashtag_seed", []))
        caption_lines = [topic, self._safe_product_display_name(product)]
        caption = "\n".join(line for line in caption_lines if line)
        return {
            "title": topic,
            "caption": caption,
            "hashtags": hashtags,
            "cover_text": str(rules.get("overlay_text_hint") or ""),
            "overlay_instruction": str(rules.get("overlay_instruction") or ""),
            "copy_status": (
                "draft_needs_human_review"
                if human_review_required else "ready_for_auto_render"
            ),
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
            slot_index = int(slot["slot_index"])
            effective_slot, composition_contract = self._apply_grammar_to_shot(
                product, slot
            )
            overlay = ""
            if slot.get("slot_index") == 1:
                overlay = str(rules.get("overlay_text_hint") or "")
            source_refs = asset_refs + product_refs
            transition_out = slot.get("transition_out", "short_dissolve")
            if allowed_transitions and transition_out not in allowed_transitions:
                transition_out = preset_default_transition
            motion_preset = effective_slot.get("motion", "slow_push")
            if slot.get("slot_role") == "detail" and not _has_detail_reference(product):
                motion_preset = "upper_body_focus"
            shots.append(
                {
                    "slot_index": slot_index,
                    "slot_role": slot["slot_role"],
                    "purpose": effective_slot.get("purpose", ""),
                    "duration_ms": int(slot["duration_ms"]),
                    "motion_preset": motion_preset,
                    "transition_out": transition_out,
                    "overlay_text": overlay,
                    "generation_prompt": self._shot_prompt(
                        effective_slot, look_ref, scene_ref, product
                    ),
                    "source_refs": source_refs,
                    "composition_contract": composition_contract,
                    "camera_hint": effective_slot.get("camera_hint") or "",
                    "shot_grammar": str(product.get("planned_shot_grammar") or "LEGACY"),
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
            "product anchor: selected product reference images (visual truth)",
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
