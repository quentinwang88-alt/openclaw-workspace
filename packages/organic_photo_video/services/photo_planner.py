"""Plan the first native-photo MVP path from an enabled reusable asset set."""

from __future__ import annotations

import copy
import hashlib
from typing import Any, Dict, Mapping, Optional

from domain import statuses
from domain.contracts import ensure_valid, validate_plan_json
from domain.models import ContentRecipe
from domain.photo_contracts import validate_variables, validate_copy
from services.asset_set_service import AssetSetService
from services.photo_copy import resolve_photo_copy
from services.locale_quality import copy_locale_issues
from services.photo_content import freeze_content_card
from services.photo_recipe_contract import (
    category_binding_errors, is_supported_photo_recipe, is_v2_recipe,
    market_binding_errors, product_mode_errors,
)
from services.photo_wig_flow import recipe_is_mx_wig_choice
from services.content_package import ContentPackageService
from services.workflow_v2 import RevisionService


class PhotoPlannerError(ValueError):
    pass


class PhotoReusePlannerService:
    """Freeze recipe variables and reusable source photos into a V2 plan."""

    def __init__(self, repository: Any):
        self.repository = repository
        self.asset_sets = AssetSetService(repository)

    @staticmethod
    def _validate_variables(schema: Mapping[str, Any], values: Mapping[str, Any]) -> None:
        errors = validate_variables(schema, values)
        if errors:
            raise PhotoPlannerError("; ".join(errors))

    def plan_task(
        self, task_id: str, *, recipe_id: str, variables: Mapping[str, Any],
        copy_block: Mapping[str, Any], layout: Mapping[str, Any],
        asset_set_id: Optional[str] = None, operator: str = "operator",
        recipe_snapshot: Optional[Mapping[str, Any]] = None,
        asset_snapshot: Optional[Mapping[str, Any]] = None,
        execution_profile_id: str = "", copy_variant_id: str = "",
        content_card: Optional[Mapping[str, Any]] = None,
        theme_brief: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        task = self.repository.get_task(task_id)
        if task is None or task.task_status not in {statuses.TASK_DRAFT, statuses.TASK_PLANNED}:
            raise PhotoPlannerError("photo task must be draft or planned")
        if task.media_kind != "native_photo":
            raise PhotoPlannerError("photo planner requires media_kind=native_photo")
        recipe_probe = (ContentRecipe.from_row(recipe_snapshot) if recipe_snapshot
                        else self.repository.get_content_recipe(recipe_id))
        if recipe_probe is not None and recipe_is_mx_wig_choice(recipe_probe):
            # Explicit MX dispatch: the wig flow maps its own four-page plan;
            # every other task keeps the original five-slot logic below.
            from services.photo_wig_planner import plan_mx_wig_task
            return plan_mx_wig_task(
                self, task_id, recipe_id=recipe_id, variables=variables,
                copy_block=copy_block, layout=layout, asset_set_id=asset_set_id,
                operator=operator, recipe_snapshot=recipe_snapshot,
                asset_snapshot=asset_snapshot,
                execution_profile_id=execution_profile_id,
                copy_variant_id=copy_variant_id, content_card=content_card,
                theme_brief=theme_brief,
            )
        recipe = (ContentRecipe.from_row(recipe_snapshot) if recipe_snapshot
                  else self.repository.get_content_recipe(recipe_id))
        if recipe is None or recipe.status != "active":
            raise PhotoPlannerError("photo recipe is missing or inactive")
        if recipe.recipe_id != recipe_id:
            raise PhotoPlannerError("frozen recipe id does not match request")
        spec = dict(recipe.recipe_spec_json or {})
        if not is_supported_photo_recipe(spec):
            raise PhotoPlannerError("recipe is not a native-photo recipe")
        market_pack = None
        if is_v2_recipe(spec):
            # A country-agnostic recipe defers its market to the Market Pack, so
            # the pack must exist before anything else is validated.  Review fix
            # P0-1: a draft pack is not producible, and silence here would let a
            # recipe look market-independent when it is not.
            pack_id = str(getattr(task, "market_pack_id", "") or "")
            if not pack_id:
                raise PhotoPlannerError("MARKET_PACK_REQUIRED: 任务未绑定 Market Pack")
            market_pack = self.repository.get_market_pack(pack_id)
            if market_pack is None:
                raise PhotoPlannerError(
                    f"MARKET_PACK_REQUIRED: Market Pack 不存在：{pack_id}")
        binding_errors = category_binding_errors(
            spec, category_key=task.category_key, recipe_id=recipe.recipe_id)
        binding_errors += market_binding_errors(
            spec, market=task.target_country, locale=task.target_locale,
            market_pack=market_pack)
        binding_errors += product_mode_errors(
            spec, product_mode=task.product_mode)
        if binding_errors:
            raise PhotoPlannerError("; ".join(binding_errors))
        self._validate_variables(spec.get("variables_schema") or {}, variables)
        template_id = str(layout.get("template_id") or layout.get("layout_id") or "")
        template_version = int(layout.get("template_version") or layout.get("layout_version") or 0)
        # 视觉预设排版覆盖（2026-09-15 排版轮 C）：预设声明 structured_v1 且
        # 布局本身带 overlay_style=structured_v1 时，允许使用预设选择的模板
        # 替代配方默认模板；其余情况保持配方冻结校验逐字不变。
        preset_family = str(
            (((theme_brief or {}).get("visual_preset") or {}).get("layout") or {})
            .get("layout_family") or "")
        structured_override = (
            preset_family == "structured_v1"
            and str((layout.get("render_options") or {}).get("overlay_style") or "")
            == "structured_v1")
        if (template_id != spec.get("template_id")
                or template_version != int(spec.get("template_version") or 0)
        ) and not structured_override:
            raise PhotoPlannerError("layout does not match the version frozen by recipe")

        if not recipe_snapshot and not spec.get("content_card"):
            raise PhotoPlannerError("NEEDS_CONTENT: new requests require a source-qualified content card")
        requirements = spec.get("asset_requirements") or {}
        visual_tags = {key: variables[key] for key in spec.get("asset_match_keys", list(variables)) if key in variables}
        if asset_snapshot:
            asset_set = self.asset_sets.from_frozen(
                asset_snapshot, category_key=task.category_key, market=task.target_country,
                tags=visual_tags, requirements=requirements,
            )
            if asset_set.asset_set_id != asset_set_id:
                raise PhotoPlannerError("frozen asset set id does not match request")
        else:
            asset_set = self.asset_sets.select(
                category_key=task.category_key, market=task.target_country,
                content_key=task.idempotency_key, tags=visual_tags,
                requirements=requirements, asset_set_id=asset_set_id,
            )
        assets = self.asset_sets.ordered_assets(asset_set, roles=requirements.get("required_roles") or [], count=5)
        if len(recipe.story_structure_json) != 5:
            raise PhotoPlannerError("photo MVP recipe must contain five story slots")
        try:
            # 页数契约与冻结校验同源：单封面行 content_card.pages=4 → 4 条文案。
            card_pages = list((content_card or {}).get("pages") or [])
            expected_slides = len(card_pages) if card_pages else 5
            copy_block = resolve_photo_copy(copy_block, assets=assets, locale=task.target_locale,
                                            expected_slide_count=expected_slides)
        except ValueError as exc:
            raise PhotoPlannerError(str(exc)) from exc
        copy_errors = validate_copy(copy_block, expected_slide_count=expected_slides)
        copy_errors.extend(copy_locale_issues(copy_block, task.target_locale))
        if copy_errors:
            raise PhotoPlannerError("; ".join(copy_errors))
        title = copy_block["title"]
        caption = copy_block["caption"]
        hashtags = list(copy_block.get("hashtags") or [])
        slide_texts = list(copy_block.get("slide_texts") or [])
        if not caption or not all(isinstance(value, str) for value in hashtags):
            raise PhotoPlannerError("localized caption and string hashtags are required")
        if (len(slide_texts) != expected_slides
                or not all(isinstance(value, str) for value in slide_texts)):
            raise PhotoPlannerError(
                f"localized copy.slide_texts must contain {expected_slides} strings")

        layout_types = {
            "FULL_BLEED": "single", "DETAIL": "single",
            "CHOICE_DETAIL": "single", "SPLIT_TWO": "split_vertical",
            "GRID_FOUR": "grid_2x2", "TRIPTYCH": "triptych_3",
        }

        frozen_card = None
        if content_card or spec.get("content_card"):
            if layout.get("schema_version") != "opv-photo-layout-v2":
                raise PhotoPlannerError("content card requires executable layout v2")
            frozen_card = freeze_content_card(content_card or spec["content_card"], asset_set, spec.get("visual_rules") or {})
            if content_card and frozen_card != content_card:
                raise PhotoPlannerError("frozen content card changed")
        role_slots = {item["role"]: index for index, item in enumerate(assets, 1)}
        slides = []
        shots = []
        if frozen_card:
            # 卡片驱动：单封面等按 content_card 页数出页（页数与 slide_texts 同源）。
            for index, page in enumerate(frozen_card["pages"], 1):
                source_slots = [role_slots[role] for role in page.get("source_roles") or []
                                if role in role_slots]
                if any(value < 1 or value > len(assets) for value in source_slots):
                    raise PhotoPlannerError("card source_roles must reference available looks")
                source_ids = [assets[value - 1]["asset_id"] for value in source_slots]
                # 修复二：教程页级结构随页冻结（text=headline/body/kicker，
                # color_chips=配色示意色）——渲染器 v2 的输入源；旧任务的页
                # 没有这些键，slide 保持原样（字符串 overlay 路径不变）。
                page_text = page.get("text") if isinstance(page.get("text"), Mapping) else None
                slides.append({
                    "slot_index": index,
                    "slot_role": f"slide_{index}",
                    "source_kind": "reused_asset",
                    "source_refs": source_ids,
                    "source_slots": source_slots,
                    "overlay_text": slide_texts[index - 1] if index <= len(slide_texts) else "",
                    **({"page_text": {
                        key: str(page_text.get(key) or "")
                        for key in ("kicker", "headline", "body")}}
                       if page_text else {}),
                    **({"color_chips": [dict(chip) for chip in page.get("color_chips") or []]}
                       if page.get("color_chips") else {}),
                    "layout_snapshot": {
                        "template_id": template_id, "template_version": template_version,
                        "layout": str(page.get("layout") or "single"),
                        **({
                            "column_labels": list(page.get("column_labels") or []),
                        } if page.get("column_labels") else {}),
                    },
                })
        else:
            for index, story in enumerate(recipe.story_structure_json, 1):
                if int(story.get("slot_index") or index) != index:
                    raise PhotoPlannerError("recipe story slots must be in 1..5 order")
                page = frozen_card["pages"][index - 1] if frozen_card else None
                source_slots = ([role_slots[role] for role in page["source_roles"]] if page
                                else [int(value) for value in story.get("source_slots") or []])
                if any(value < 1 or value > len(assets) for value in source_slots):
                    raise PhotoPlannerError("recipe source_slots must reference slots 1..5")
                source_ids = ([assets[value - 1]["asset_id"] for value in source_slots]
                              if source_slots else [assets[index - 1]["asset_id"]])
                slides.append({
                    "slot_index": index,
                    "slot_role": str(story.get("role") or story.get("slot_role") or f"slide_{index}"),
                    "source_kind": "reused_asset",
                    "source_refs": source_ids,
                    "source_slots": source_slots,
                    "overlay_text": slide_texts[index - 1],
                    "layout_snapshot": {
                        "template_id": template_id, "template_version": template_version,
                        "layout_variant": str(story.get("layout_variant") or "DETAIL"),
                        "layout": page["layout"] if page else layout_types.get(
                            str(story.get("layout_variant") or "DETAIL"),
                            str(story.get("layout") or "single"),
                        ),
                        **({
                            "column_labels": list((page or {}).get("column_labels") or []),
                        } if (page or {}).get("column_labels") else {}),
                    },
                })
        for index, source in enumerate(assets, 1):
            shots.append({
                "slot_index": index,
                "slot_role": str(source.get("role") or f"source_{index}"),
                "shot_kind": "reused_asset", "asset_path": source["path"],
                "asset_sha256": source["sha256"], "source_asset_id": source["asset_id"],
            })

        pack = self.repository.get_market_pack(task.market_pack_id or "")
        if pack is None:
            raise PhotoPlannerError("task market pack is missing")
        product = None
        if task.product_mode != "NO_PRODUCT":
            raw = task.product_snapshot_json or {}
            snapshot = raw.get("product") if isinstance(raw.get("product"), dict) else raw
            product = {
                "id": str(snapshot.get("product_id") or task.product_id or ""),
                "snapshot": copy.deepcopy(snapshot),
            }
        plan = {
            "schema_version": "opv-photo-plan-v1", "workflow_version": 2,
            "media_kind": "native_photo", "category_key": task.category_key,
            "product_mode": task.product_mode,
            "market_pack": {
                "id": pack.market_pack_id, "version": pack.pack_version,
                "country": pack.target_country, "locale": pack.target_locale,
            },
            "recipe": {"id": recipe.recipe_id, "version": recipe.recipe_version},
            "template": {"id": template_id, "version": template_version},
            "variables": copy.deepcopy(dict(variables)), "cover_index": 1,
            "execution_profile_id": execution_profile_id, "copy_variant_id": copy_variant_id,
            "copy": {
                **copy.deepcopy(dict(copy_block)),
                "title": title, "caption": caption, "hashtags": hashtags,
                "slide_texts": slide_texts,
            },
            "product": product, "slides": slides, "shots": shots,
            "asset_set": {
                "id": asset_set.asset_set_id, "key": asset_set.asset_set_key,
                "version": asset_set.asset_set_version,
            },
            "production_policy": {"mode": "ASSET_REUSE", "ai_image_calls": 0},
        }
        if theme_brief:
            plan["theme_brief"] = copy.deepcopy(dict(theme_brief))
        if frozen_card:
            plan.update(source_binding="roles-v1", source_roles=[a["role"] for a in assets], content_card=frozen_card)
        ensure_valid(validate_plan_json(plan), "photo plan")
        storyboard_version = f"{recipe.recipe_id}-v{recipe.recipe_version}"
        if len(storyboard_version) > 32:
            # Both task/package columns are VARCHAR(32). Full identity remains
            # in recipe_id/recipe_version and the frozen plan, without truncation.
            storyboard_version = "photo-" + hashlib.sha256(storyboard_version.encode()).hexdigest()[:26]
        self.repository.update_task_plan(
            task_id, plan_json=plan, copy_json=plan["copy"], recipe_id=recipe.recipe_id,
            recipe_version=recipe.recipe_version, content_goal=recipe.content_goal,
            storyboard_version=storyboard_version,
            workflow_version=2,
        )
        if len(slides) != int(getattr(task, "requested_shot_count", 0) or 0):
            # 卡片驱动计划（如单封面 4 页）：可交付页数以冻结卡片为准。
            self.repository.update_task_requested_shot_count(
                task_id, requested_shot_count=len(slides))
        if task.task_status == statuses.TASK_DRAFT:
            task = self.repository.transition_task(task_id, statuses.TASK_DRAFT, statuses.TASK_PLANNED)
        else:
            task = self.repository.get_task(task_id)
        package = ContentPackageService(self.repository).create_for_task(
            task, recipe_id=recipe.recipe_id, plan=plan
        )
        revision = RevisionService(self.repository).ensure_working(
            self.repository.get_task(task_id), operator=operator
        )
        return {
            "task_id": task_id, "plan": plan,
            "content_package_id": package.content_package_id,
            "revision_id": revision.revision_id,
        }
