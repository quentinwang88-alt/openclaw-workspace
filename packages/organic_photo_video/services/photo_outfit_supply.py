"""Build TH native-photo source looks from product truth and the outfit library.

The service deliberately sits in front of the native-photo renderer.  A product
reference is reusable input; complete Look photos are outputs.  Existing
approved Look photos win, and only missing roles call the existing image
generator.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from services.image_generator import ShotGenerationRequest
from services.multi_look_planner import compatible_multi_look_candidates
from services.outfit_planner import freeze_multi_look_state


class PhotoOutfitSupplyError(ValueError):
    pass


def _bottom_axis(look: Mapping[str, Any]) -> str:
    recipe = dict(look.get("recipe") or {})
    value = " ".join(str(recipe.get(key) or "") for key in ("bottom", "onepiece", "dress"))
    lowered = value.lower()
    if any(token in lowered for token in ("短裤", "shorts", "裤", "pants", "jeans", "牛仔", "ショートパンツ")):
        return "pants"
    if any(token in lowered for token in ("裙", "skirt", "dress", "เดรส", "กระโปรง")):
        return "skirt"
    return "other"


def _look_signature(look: Mapping[str, Any]) -> str:
    payload = {
        key: str(look.get(key) or "")
        for key in ("role", "outerwear", "top_inner", "bottom", "shoes")
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


class PhotoOutfitSupplyService:
    """Resolve four role-bound full Looks, generating only unresolved roles."""

    def __init__(self, *, generator: Any, asset_reader: Any, root: Path):
        self.generator = generator
        self.asset_reader = asset_reader
        self.root = Path(root)

    @staticmethod
    def role_plan(recipe: Any) -> list[dict[str, str]]:
        spec = dict(getattr(recipe, "recipe_spec_json", {}) or {})
        raw = list((spec.get("outfit_supply") or {}).get("roles") or [])
        if not raw:
            raise PhotoOutfitSupplyError("Recipe 缺少 outfit_supply.roles，不能自动规划穿搭")
        result = []
        for item in raw:
            role, axis = str(item.get("role") or ""), str(item.get("bottom_axis") or "")
            if not role or axis not in {"pants", "skirt", "other"}:
                raise PhotoOutfitSupplyError("Recipe 的自动供图角色配置无效")
            result.append({"role": role, "bottom_axis": axis})
        return result

    def prepare(
        self, *, record_id: str, recipe: Any, product: Mapping[str, Any],
        account: Any, existing_sources: Sequence[Mapping[str, Any]] = (),
        variation: Mapping[str, Any] = None,
    ) -> dict[str, Any]:
        variation = dict(variation or {})
        roles = self.role_plan(recipe)
        planned_by_role = {
            str(item.get("role") or ""): dict(item)
            for item in variation.get("looks") or []
        }
        if planned_by_role and set(planned_by_role) != {item["role"] for item in roles}:
            raise PhotoOutfitSupplyError("内容计划中的穿搭角色与 Recipe 不一致")
        if not product.get("reference_images"):
            raise PhotoOutfitSupplyError("缺少可读取的商品参考图")
        output_dir = self.root / "outfit_supply" / self._safe(record_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = output_dir / "supply_manifest.json"
        input_hash = self._input_hash(recipe, product, variation)
        resumed: dict[str, dict[str, Any]] = {}
        if manifest_path.is_file():
            prior = json.loads(manifest_path.read_text(encoding="utf-8"))
            if prior.get("input_hash") != input_hash:
                raise PhotoOutfitSupplyError("该行商品参考图或 Recipe 已变化；请新建一行，避免混用旧补图")
            for source in prior.get("sources") or []:
                path = Path(str(source.get("path") or ""))
                if (path.is_file() and source.get("sha256")
                        and hashlib.sha256(path.read_bytes()).hexdigest() == source["sha256"]):
                    resumed[str(source.get("role") or "")] = dict(source)
        candidates, rejected = compatible_multi_look_candidates(
            account, self.asset_reader, dict(product), scene_ref="",
        )
        by_ref = {str(item.get("ref_id") or ""): item for item in candidates}
        used_refs: set[str] = set()
        assigned: dict[str, dict[str, Any]] = {}

        for role in roles:
            source = resumed.get(role["role"])
            look = by_ref.get(str((source or {}).get("look_ref") or ""))
            planned = planned_by_role.get(role["role"])
            signature_ok = (
                not planned or (
                    source is not None
                    and source.get("planned_look_signature") == _look_signature(planned)
                )
            )
            if source and look and _bottom_axis(look) == role["bottom_axis"] and signature_ok:
                assigned[role["role"]] = source
                used_refs.add(str(source["look_ref"]))

        # Reuse only photos whose frozen Look still exists and satisfies the
        # current role.  A random old product photo must not become a skirt or
        # shorts claim merely because four files are available.
        for role in roles:
            if role["role"] in assigned:
                continue
            for source in existing_sources:
                ref = str(source.get("look_ref") or "")
                look = by_ref.get(ref)
                planned = planned_by_role.get(role["role"])
                signature_ok = (
                    not planned or source.get("planned_look_signature") == _look_signature(planned)
                )
                if (not look or ref in used_refs or _bottom_axis(look) != role["bottom_axis"]
                        or not signature_ok):
                    continue
                source_path = Path(str(source.get("path") or "")).expanduser()
                expected_hash = str(source.get("sha256") or "")
                if (not source_path.is_file() or
                        (expected_hash and hashlib.sha256(source_path.read_bytes()).hexdigest() != expected_hash)):
                    # Historical video rows can point at rotated/cleaned media.
                    # Treat that source as unavailable and let the current
                    # product supply plan generate the missing role.
                    continue
                assigned[role["role"]] = {
                    **dict(source), "role": role["role"], "source_kind": "existing_outfit",
                    "look_ref": ref, "look_snapshot": look,
                }
                used_refs.add(ref)
                break

        available = [item for item in candidates if str(item.get("ref_id") or "") not in used_refs]
        missing_axes = [role["bottom_axis"] for role in roles if role["role"] not in assigned]
        for axis in set(missing_axes):
            needed = missing_axes.count(axis)
            found = sum(_bottom_axis(item) == axis for item in available)
            if found < needed:
                counts = {name: sum(_bottom_axis(item) == name for item in candidates)
                          for name in ("pants", "skirt")}
                raise PhotoOutfitSupplyError(
                    f"穿搭模板不足：还需要 {needed} 个 {axis}，可用于补图的只有 {found} 个；"
                    f"当前兼容裤装模板 {counts['pants']}、裙装模板 {counts['skirt']}"
                )
        generated_paths: list[str] = [str(item["path"]) for item in assigned.values()
                                      if item.get("source_kind") == "generated_outfit"]
        generated_count = 0
        persona_ref = str(getattr(account, "persona_ref_id", "") or "")
        if not persona_ref:
            raise PhotoOutfitSupplyError("生产账号没有绑定人物模板，不能补生成穿搭")
        persona = self.asset_reader.get_persona(persona_ref)

        for index, role in enumerate(roles, 1):
            if role["role"] in assigned:
                continue
            look = next((item for item in available if _bottom_axis(item) == role["bottom_axis"]), None)
            if look is None:
                raise PhotoOutfitSupplyError(f"穿搭模板分配异常：{role['role']}")
            available.remove(look)
            used_refs.add(str(look.get("ref_id") or ""))
            state = freeze_multi_look_state(look)
            planned = planned_by_role.get(role["role"])
            if planned:
                state.update({
                    "outerwear": planned["outerwear"],
                    "top_inner": planned["top_inner"],
                    "bottom": planned["bottom"],
                    "shoes": planned["shoes"],
                    "style_direction": "；".join(filter(None, [
                        str(variation.get("angle_zh") or ""),
                        str(variation.get("palette_zh") or ""),
                        str(variation.get("style_modifier") or ""),
                    ])),
                })
            planned_signature = _look_signature(planned) if planned else ""
            request = ShotGenerationRequest(
                task_id=f"photo_supply_{self._safe(record_id)}",
                slot_index=index, slot_role="full_look", shot_version=1,
                plan_shot={
                    "shot_kind": "generated_photo", "outfit_state_ref": role["role"],
                    "purpose": (
                        f"为原生图文生成 {role['role']} 完整穿搭；"
                        f"本篇方向：{variation.get('angle_zh') or '自动'}；"
                        f"配色：{variation.get('palette_zh') or '跟随商品'}"
                    ),
                    "camera_hint": "正面或轻微三分之四侧身完整全身，头顶和鞋底完整可见",
                    "composition_contract": {"framing": "full_body", "instruction": "完整全身穿搭，商品和上下装关系清晰"},
                },
                product=dict(product), persona_snapshot=persona, look_snapshot=look,
                scene_snapshot={
                    "name": str(variation.get("scene_zh") or "纯色穿搭背景"),
                    "prompt_core": str(variation.get("background_prompt") or (
                        "均匀浅色背景；只借鉴场景的色彩气氛，不增加复杂道具；"
                        + str(variation.get("scene_zh") or "")
                    )),
                },
                output_dir=str(output_dir), continuity_reference_images=generated_paths[:1],
                outfit_state=state,
                recipe_execution={
                    "content_goal": "multi_look", "transform_mode": "controlled_outfit_change",
                    "presentation_profile": {
                        "background_mode": "solid_color",
                        "background_color": str(variation.get("background_color") or "#F6F5F2"),
                    },
                    "locale": "th-TH",
                    "batch_variation": variation,
                },
                reference_roles=dict(product.get("reference_roles") or {}),
            )
            outcome = self.generator.generate_shot(request)
            if not outcome.ok or not outcome.image_path:
                raise PhotoOutfitSupplyError(
                    f"{role['role']} 补图失败：{outcome.error or '图片模型没有返回文件'}"
                )
            path = Path(outcome.image_path).resolve()
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            assigned[role["role"]] = {
                "role": role["role"], "path": str(path), "sha256": digest,
                "source_kind": "generated_outfit", "look_ref": str(look.get("ref_id") or ""),
                "look_snapshot": look, "generation_provider": outcome.provider,
                "generation_model": outcome.model, "generation_request_id": outcome.request_id,
                "display_label": {
                    "th-TH": str((planned or {}).get("display_label") or ""),
                    "zh-CN": str((planned or {}).get("display_label") or ""),
                },
                "outerwear_signature": str((planned or {}).get("outerwear") or ""),
                "planned_look_signature": planned_signature,
                "content_plan_item_id": str(variation.get("family_id") or ""),
                "planned_attributes": {
                    key: str((planned or {}).get(key) or "")
                    for key in ("outerwear", "top_inner", "bottom", "shoes")
                },
            }
            generated_paths.append(str(path))
            generated_count += 1
            self._save_manifest(
                manifest_path, input_hash=input_hash, record_id=record_id,
                sources=[assigned[key] for key in assigned], status="incomplete",
            )

        ordered = [assigned[item["role"]] for item in roles]
        self._save_manifest(
            manifest_path, input_hash=input_hash, record_id=record_id,
            sources=ordered, status="complete",
        )
        generated_total = sum(item.get("source_kind") == "generated_outfit" for item in ordered)
        return {
            "schema_version": "opv-photo-outfit-supply-v1", "record_id": record_id,
            "product_id": str(product.get("product_id") or ""),
            "sources": ordered, "required_count": len(roles),
            "reused_count": len(roles) - generated_total, "generated_count": generated_total,
            "generated_this_run": generated_count, "resumed_count": generated_total - generated_count,
            "rejected_template_count": len(rejected),
            "role_plan": roles, "supply_manifest": str(manifest_path),
        }

    @staticmethod
    def _input_hash(recipe: Any, product: Mapping[str, Any], variation=None) -> str:
        refs = []
        for value in product.get("reference_images") or []:
            path = Path(str(value)).expanduser()
            refs.append(hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else str(value))
        payload = {"recipe_id": recipe.recipe_id, "recipe_version": recipe.recipe_version,
                   "product_id": product.get("product_id"), "references": refs,
                   "variation": dict(variation or {})}
        return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    @staticmethod
    def _save_manifest(path: Path, *, input_hash: str, record_id: str,
                       sources: Sequence[Mapping[str, Any]], status: str) -> None:
        serializable = []
        for source in sources:
            serializable.append({key: value for key, value in source.items() if key != "look_snapshot"})
        payload = {"schema_version": "opv-photo-outfit-supply-v1", "record_id": record_id,
                   "input_hash": input_hash, "status": status, "sources": serializable}
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(path)

    @staticmethod
    def _safe(value: str) -> str:
        return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(value))[:120]
