"""Create the assets required by the frozen outfit-breakdown board.

The service freezes the person from the recipe anchor, uses product/look
references as visual facts where available, generates missing isolated items
on a chroma-green background, then delegates all normalization and QC to
``cutout_assets``. Legacy boards require four roles; dynamic boards require a
person plus two or three real garments from the board's frozen outfit state.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

from services.cutout_assets import REQUIRED_ROLES, build_asset_manifest, transparent_reference_is_usable
from services.image_generator import _load_skill_service
from services.product_reference_resolver import select_product_references_for_slot


class OutfitDecompositionError(RuntimeError):
    pass


def _local_path(value: Any) -> str:
    if isinstance(value, str):
        return value if Path(value).is_file() else ""
    if isinstance(value, Mapping):
        candidate = str(
            value.get("cutout_image_path")
            or value.get("local_path")
            or value.get("path")
            or ""
        ).strip()
        return candidate if candidate and Path(candidate).is_file() else ""
    return ""


def _first_local(values: Sequence[Any]) -> str:
    return next((path for path in (_local_path(value) for value in values) if path), "")


def decomposition_ready(plan: Mapping[str, Any]) -> bool:
    manifest = dict(plan.get("decomposition_assets") or {})
    if manifest.get("status") != "ready":
        return False
    board = next((shot for shot in plan.get("shots") or [] if shot.get("shot_kind") == "composite_board"), {})
    expected_state = str(board.get("outfit_state_ref") or "FINAL")
    recorded_state = str(manifest.get("source_outfit_state_ref") or "FINAL")
    if board and expected_state != recorded_state:
        return False
    assets = dict(manifest.get("assets") or {})
    roles = manifest.get("required_roles") or REQUIRED_ROLES
    if manifest.get("item_count_policy") == "dynamic_2_or_3":
        items = list(manifest.get("item_roles") or [])
        if len(items) not in (2, 3) or not items or items[0] != "target_product":
            return False
        roles = ["person_cutout", *items]
    return all(_local_path(assets.get(role) or {}) for role in roles)


def _item_label(value: Any) -> str:
    if isinstance(value, Mapping):
        return " ".join(str(value.get(key) or "").strip() for key in
                        ("color", "type", "material", "description") if value.get(key)).strip()
    return str(value or "").strip()


def companion_items_for_state(plan: Mapping[str, Any], state_ref: str = "FINAL") -> list[tuple[str, str, Any]]:
    """Select existing garments in the requested frozen state, not global Look."""
    final = dict((plan.get("outfit_states") or {}).get(state_ref) or {})
    raw_refs = final.get("item_refs") or final.get("item_reference_images") or {}
    refs = dict(raw_refs) if isinstance(raw_refs, Mapping) else {
        str(item.get("role")): item for item in raw_refs
        if isinstance(item, Mapping) and item.get("role")
    } if isinstance(raw_refs, (list, tuple)) else {}
    absent = {"", "无", "none", "null", "不适用", "无需", "无独立下装"}
    output = []
    onepiece = _item_label(final.get("onepiece"))
    inner = _item_label(final.get("top_inner"))
    bottom = _item_label(final.get("bottom"))
    raw_bottom = final.get("bottom")
    if isinstance(raw_bottom, Mapping) and (
        raw_bottom.get("present") is False or str(raw_bottom.get("type") or "").strip().lower() in absent
    ):
        bottom = ""
    is_dress = bool(onepiece and onepiece.lower() not in absent) or any(
        token in inner.lower() for token in ("连衣裙", "连体", "dress", "jumpsuit")
    )
    if onepiece and onepiece.lower() not in absent:
        output.append(("onepiece", onepiece, refs.get("onepiece") or refs.get("top_inner")))
    elif inner.lower() not in absent:
        output.append(("top_inner", inner, refs.get("top_inner")))
    if not is_dress and bottom.lower() not in absent and "无独立下装" not in bottom:
        output.append(("bottom", bottom, refs.get("bottom")))
    if not output:
        raise OutfitDecompositionError(f"{state_ref} outfit has no real companion garment for decomposition")
    return output[:2]


def final_companion_items(plan: Mapping[str, Any]) -> list[tuple[str, str, Any]]:
    """Compatibility helper for historical FINAL-only consumers."""
    return companion_items_for_state(plan, "FINAL")


def compose_asset_prompt(role: str, label: str, *, has_item_reference: bool) -> str:
    role_names = {
        "target_product": "目标外套/目标商品",
        "top_inner": "内搭上衣",
        "bottom": "下装",
        "onepiece": "连衣裙/连体服",
    }
    if role == "person_cutout":
        return "\n".join(
            [
                "Use case: identity-preserve background extraction.",
                "Asset type: P1 outfit-breakdown person cutout.",
                "Primary request: use the supplied original person anchor photo and preserve exactly the same woman, face, hair, expression, full-body pose, clothing, bag and shoes.",
                "Change only the background to a uniform vivid green #00FF00. Keep the full body and all clothing edges; do not crop limbs.",
                "No restyling, no new person, no new clothing, no text, no watermark, no props, no second person.",
            ]
        )
    item_name = role_names.get(role, role)
    fact_line = (
        "附件中的对应单品是最高视觉事实，严格保留其颜色、版型、材质和结构。"
        if has_item_reference
        else "从附件中的完整穿搭人物准确还原该单品，不要改变颜色、版型或材质。"
    )
    return "\n".join(
        [
            f"生成一张独立的{item_name}商品抠图素材。",
            fact_line,
            f"穿搭配方描述：{label or '以参考图中实际穿着为准'}。",
            "只出现一件完整单品，正面平铺或隐形人台效果，居中，边缘完整，不得被裁切。",
            "背景必须是纯色高饱和亮绿色 #00FF00，四角和单品周围全部为同一绿色。",
            "不要人物、手、衣架、文字、商标、水印、价格、阴影道具、拼图或额外服饰。",
            "真实服装摄影质感，保留缝线、面料和廓形，只输出一张图片。",
        ]
    )


@dataclass
class GeneratedAsset:
    path: str
    provider: str = "openai-image"
    model: str = "gpt-image-2"


class OpenAIOutfitAssetGenerator:
    """Small adapter over the already approved openai-image runtime."""

    def __init__(self, service=None):
        self._service = service

    def _load_service(self):
        if self._service is None:
            self._service = _load_skill_service()()
        return self._service

    def generate(
        self,
        *,
        task_id: str,
        role: str,
        prompt: str,
        reference_paths: Sequence[str],
        output_dir: Path,
    ) -> GeneratedAsset:
        try:
            service = self._load_service()
            from core.schemas import ImageTaskRequest

            request = ImageTaskRequest.from_dict(
                {
                    "task_id": f"{task_id}_decompose_{role}",
                    "task_type": "opv_outfit_decomposition",
                    "target_field": role,
                    "mode": "edit" if reference_paths else "generate",
                    "prompt": prompt,
                    "input_image_paths": list(reference_paths),
                    "size": "1024x1024",
                    "quality": "high",
                    "output_format": "png",
                    "output_dir": str(output_dir),
                    "n": 1,
                    "metadata": {
                        "opv_task_id": task_id,
                        "asset_role": role,
                        "background_contract": "solid_chroma_green_00ff00",
                    },
                }
            )
            result = service.process_task(request)
        except Exception as exc:  # noqa: BLE001 - provider boundary
            raise OutfitDecompositionError(
                f"{role} generation failed: {type(exc).__name__}: {exc}"
            ) from exc
        paths = list(getattr(result, "output_image_paths", []) or [])
        if str(getattr(result, "status", "") or "") != "success" or not paths:
            raise OutfitDecompositionError(
                f"{role} generation failed: "
                f"{getattr(result, 'error_message', '') or 'no output image'}"
            )
        return GeneratedAsset(path=str(paths[0]))


class OutfitDecomposer:
    def __init__(self, repository, generator=None, output_root: Optional[Path] = None):
        self._repository = repository
        self._generator = generator or OpenAIOutfitAssetGenerator()
        self._output_root = (
            Path(output_root)
            if output_root
            else Path.home() / ".openclaw" / "shared" / "data" / "organic_photo_video"
        )

    def ensure(self, task, *, anchor_path: str):
        """Return a refreshed task with a complete persisted asset manifest."""
        plan = dict(task.plan_json or {})
        p1 = next((shot for shot in plan.get("shots") or [] if int(shot.get("slot_index") or 0) == 1), None)
        board_spec = dict((p1 or {}).get("board_spec") or {})
        state_ref = str((p1 or {}).get("outfit_state_ref") or "FINAL")
        content_goal = str((plan.get("recipe_execution") or {}).get("content_goal")
                           or (plan.get("recipe") or {}).get("content_goal") or "")
        profile = dict(plan.get("presentation_profile") or {})
        dynamic = profile.get("item_count_policy") == "dynamic_2_or_3" or content_goal == "multi_look"
        existing_manifest = dict(plan.get("decomposition_assets") or {})
        if decomposition_ready(plan):
            p1 = next(
                (shot for shot in plan.get("shots", []) if int(shot.get("slot_index") or 0) == 1),
                None,
            )
            board_manifest = dict(
                ((p1 or {}).get("board_spec") or {}).get("decomposition_assets") or {}
            )
            if p1 and not decomposition_ready(
                {"decomposition_assets": board_manifest}
            ):
                p1["board_spec"] = {
                    **dict(p1.get("board_spec") or {}),
                    "decomposition_assets": existing_manifest,
                }
                self._repository.update_task_plan(task.task_id, plan_json=plan)
                return self._repository.get_task(task.task_id) or task
            return task
        if not anchor_path or not Path(anchor_path).is_file():
            raise OutfitDecompositionError("person anchor is missing or unreadable")

        look = dict(((plan.get("look") or {}).get("snapshot") or {}))
        recipe = dict(look.get("recipe") or {})
        item_refs = dict(look.get("item_refs") or recipe.get("item_refs") or {}) if not dynamic else {}
        product = dict((task.product_snapshot_json or {}).get("product") or {})
        product_refs = select_product_references_for_slot(product, "hero")
        product_ref = _first_local(product_refs)
        if dynamic:
            product_ref = next((path for path in (_local_path(value) for value in product_refs)
                                if path and transparent_reference_is_usable(path)), product_ref)
        if not product_ref:
            raise OutfitDecompositionError("target product has no readable local reference")

        provider_dir = self._output_root / task.task_id / "decomposition_assets" / "provider"
        provider_dir.mkdir(parents=True, exist_ok=True)
        sources: Dict[str, str] = {}
        source_types: Dict[str, str] = {}
        anchor_slot = int(board_spec.get("source_person_slot") or plan.get("anchor_slot") or 2)
        list_shots = getattr(self._repository, "list_shots", None)
        task_shots = list_shots(task.task_id) if callable(list_shots) else []
        anchor_shot = max(
            (
                shot for shot in task_shots
                if int(shot.slot_index) == anchor_slot and str(shot.image_url or "") == anchor_path
            ),
            key=lambda shot: int(shot.shot_version),
            default=None,
        )
        source_metadata: Dict[str, Mapping[str, Any]] = {
            "person_cutout": {
                "source_slot": anchor_slot,
                "source_asset_id": str(getattr(anchor_shot, "shot_id", "") or ""),
                "source_sha256": str(getattr(anchor_shot, "image_sha256", "") or ""),
                "review_type": "technical_only",
                **({"outfit_state_ref": state_ref} if dynamic else {}),
            }
        }
        solid_anchor = str(
            (plan.get("presentation_profile") or {}).get("background_mode") or ""
        ) == "solid_color"

        roles = {
            "person_cutout": (f"P{anchor_slot} original anchor person", anchor_path, "anchor_slot"),
            "target_product": (str(product.get("product_name") or "目标商品"), product_ref, "product_reference"),
            "top_inner": (str(recipe.get("top_inner") or ""), _local_path(item_refs.get("top_inner") or {}), "look_item_ref"),
            "bottom": (str(recipe.get("bottom") or ""), _local_path(item_refs.get("bottom") or {}), "look_item_ref"),
        }
        if dynamic:
            roles = {role: roles[role] for role in ("person_cutout", "target_product")}
            for role, label, item_ref in companion_items_for_state(plan, state_ref):
                roles[role] = (label, _local_path(item_ref), "selected_outfit_item_ref")
        for role, (label, direct_ref, direct_type) in roles.items():
            if role != "person_cutout":
                source_metadata[role] = {"review_type": "technical_only", "label": label,
                                         "outfit_state_ref": state_ref if dynamic else ""}
                if dynamic:
                    selected_state = (plan.get("outfit_states") or {}).get(state_ref) or {}
                    localized = selected_state.get("item_labels_i18n") or selected_state.get("label_i18n") or {}
                    if isinstance(localized, Mapping) and isinstance(localized.get(role), Mapping):
                        source_metadata[role]["label_i18n"] = dict(localized[role])
            # Keep opaque same-background anchor pixels, including white
            # clothing. Never infer a person mask from pixel brightness.
            if role == "person_cutout" and solid_anchor:
                sources[role] = anchor_path
                source_types[role] = "anchor_pixels_same_background_region"
                continue
            if dynamic and direct_ref and role != "person_cutout" and transparent_reference_is_usable(direct_ref):
                sources[role] = direct_ref
                source_types[role] = f"reused_transparent_{direct_type}"
                continue
            visual_ref = direct_ref or anchor_path
            generated = self._generator.generate(
                task_id=task.task_id,
                role=role,
                prompt=compose_asset_prompt(
                    role, label, has_item_reference=bool(direct_ref)
                ),
                reference_paths=[visual_ref],
                output_dir=provider_dir,
            )
            sources[role] = generated.path
            source_types[role] = (
                "anchor_slot_isolated"
                if role == "person_cutout"
                else f"ai_isolated_from_{direct_type}" if direct_ref else "ai_derived_from_anchor"
            )

        normalized_dir = self._output_root / task.task_id / "decomposition_assets" / "normalized"
        try:
            manifest = build_asset_manifest(
                task_id=task.task_id,
                sources=sources,
                source_types=source_types,
                source_metadata=source_metadata,
                output_dir=normalized_dir,
                item_roles=[role for role in roles if role != "person_cutout"] if dynamic else None,
                anchor_background=str(profile.get("background_color") or "#FFFFFF") if solid_anchor else "",
            )
        except Exception as exc:  # normalization/QC boundary
            raise OutfitDecompositionError(f"cutout normalization failed: {exc}") from exc
        if dynamic:
            manifest["source_outfit_state_ref"] = state_ref
            # Keep the on-disk manifest consistent with the frozen plan copy.
            (normalized_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        plan["decomposition_assets"] = manifest
        p1 = next(
            (shot for shot in plan.get("shots", []) if int(shot.get("slot_index") or 0) == 1),
            None,
        )
        if not p1 or p1.get("shot_kind") != "composite_board":
            raise OutfitDecompositionError("P1 is not a composite outfit board")
        p1["board_spec"] = {
            **dict(p1.get("board_spec") or {}),
            # Old/frozen plans predate an explicit layout id.  Keep their V2
            # fallback while never overwriting a new reference-style layout.
            "layout_id": str(
                (p1.get("board_spec") or {}).get("layout_id")
                or "LAYOUT_OUTFIT_BREAKDOWN_V2"
            ),
            "layout_version": int(
                (p1.get("board_spec") or {}).get("layout_version") or 2
            ),
            "decomposition_required": True,
            "decomposition_assets": manifest,
            **({"item_count_policy": "dynamic_2_or_3"} if dynamic else {}),
            **({"background_color": str(profile.get("background_color") or "#FFFFFF")}
               if solid_anchor else {}),
        }
        self._repository.update_task_plan(task.task_id, plan_json=plan)
        refreshed = self._repository.get_task(task.task_id)
        if refreshed is None:
            raise OutfitDecompositionError("task disappeared after manifest persistence")
        return refreshed
