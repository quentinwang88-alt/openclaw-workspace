"""Build deterministic, fully frozen native-photo requests without model calls."""
from __future__ import annotations

import copy
import hashlib
import json
from collections import Counter
from typing import Any, Mapping, Sequence

from domain.models import ContentRecipe, AssetSet
from domain.photo_contracts import validate_copy, validate_execution_profiles, validate_variables
from services.asset_set_service import AssetSetService, AssetSetError, validate_asset_set
from services.photo_copy import resolve_photo_copy
from services.photo_content import freeze_content_card
from services.photo_wig_flow import recipe_is_mx_wig_choice, request_is_mx_wig_choice


class PhotoRequestError(ValueError):
    pass


def fingerprint(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


def validate_frozen_request(request: Mapping[str, Any]) -> None:
    if request_is_mx_wig_choice(request):
        # Explicit MX dispatch: wig requests validate in the wig module with
        # four-page/hair-evidence semantics; every other request keeps the
        # original validation below unchanged.
        from services.photo_wig_planner import validate_mx_wig_frozen_request
        validate_mx_wig_frozen_request(request)
        return
    unsigned = {key: value for key, value in request.items() if key != "request_sha256"}
    if request.get("request_sha256") != fingerprint(unsigned):
        raise PhotoRequestError("frozen photo request fingerprint mismatch")
    if request.get("schema_version") not in {"opv-photo-request-v1", "opv-photo-request-v2"}:
        raise PhotoRequestError("unsupported frozen photo request schema")
    recipe = ContentRecipe.from_row(request["recipe_snapshot"])
    errors = validate_variables(recipe.recipe_spec_json.get("variables_schema") or {}, request.get("variables"))
    card_pages = list((request.get("content_card") or {}).get("pages") or [])
    expected_slides = len(card_pages) if card_pages else 5
    errors += validate_copy(
        request.get("copy"), expected_slide_count=expected_slides,
    )
    if errors:
        raise PhotoRequestError("; ".join(errors))
    if (recipe.recipe_id != request.get("recipe_id")
            or request.get("category_key") != recipe.recipe_spec_json.get("category_key")
            or request.get("market") not in recipe.recipe_spec_json.get("markets", [])
            or request.get("asset_set_id") != request["asset_snapshot"]["asset_set_id"]):
        raise PhotoRequestError("frozen photo request identity mismatch")

    if request.get("schema_version") == "opv-photo-request-v2":
        card = request.get("content_card")
        checked = freeze_content_card(card, AssetSet.from_row(request["asset_snapshot"]), recipe.recipe_spec_json.get("visual_rules") or {})
        if checked != card or request["layout_snapshot"].get("schema_version") != "opv-photo-layout-v2":
            raise PhotoRequestError("frozen content card or layout mismatch")


def apply_travel_single_cover(
    request: Mapping[str, Any], recommendation: Mapping[str, Any] = None,
) -> dict[str, Any]:
    """Make Look A the cover and remove its duplicate detail page."""
    result = copy.deepcopy(dict(request))
    if not str(result.get("recipe_id") or "").startswith("PHOTO_TH_TRAVEL"):
        return result
    recipe = ContentRecipe.from_row(result["recipe_snapshot"])
    asset_set = AssetSet.from_row(result["asset_snapshot"])
    available_roles = {
        str(item.get("role") or "")
        for item in asset_set.manifest_json.get("assets") or []
    }
    cover_role = "look_a"
    if cover_role not in available_roles:
        raise PhotoRequestError("旅行单图封面缺少 look_a..look_d 素材")

    card = copy.deepcopy(dict(result.get("content_card") or {}))
    pages = list(card.get("pages") or [])
    if card.get("travel_first_look_cover") is True and len(pages) == 4:
        return result
    if len(pages) != 5:
        raise PhotoRequestError("旅行单图封面需要完整五页内容卡")
    cover = {
        **dict(pages[0]),
        "purpose_zh": "旅行主题首套穿搭封面",
        "layout": "single",
        "source_roles": [cover_role],
    }
    # Original page 2 is Look A again. The cover now performs both jobs.
    card["pages"] = [cover] + [
        {**dict(page), "index": index}
        for index, page in enumerate(pages[2:], 2)
    ]
    card["travel_first_look_cover"] = True
    copy_block = copy.deepcopy(dict(result.get("copy") or {}))
    slide_texts = list(copy_block.get("slide_texts") or [])
    if len(slide_texts) != 5:
        raise PhotoRequestError("旅行首套封面需要完整五条原始排版文案")
    copy_block["slide_texts"] = [slide_texts[0]] + slide_texts[2:]
    copy_block["cover"] = copy_block["slide_texts"][0]
    result["copy"] = copy_block
    result["content_card"] = freeze_content_card(
        card, asset_set, recipe.recipe_spec_json.get("visual_rules") or {},
    )
    result["cover_selection"] = {
        "role": cover_role, "source": "fixed_first_look",
        "reason_zh": "第一套穿搭直接作为首图，不重复生成详情页",
    }
    result["request_sha256"] = fingerprint({
        key: value for key, value in result.items() if key != "request_sha256"
    })
    validate_frozen_request(result)
    return result


class PhotoRequestFactory:
    def __init__(self, repository: Any, *, layouts: Sequence[Mapping[str, Any]]):
        self.repository = repository
        self.assets = AssetSetService(repository)
        self.layouts = {str(item.get("layout_id") or item.get("template_id")): dict(item) for item in layouts}

    def build_batch(self, *, record_id: str, specs: Sequence[Any],
                    category_key: str, product_mode: str = "NO_PRODUCT",
                    overrides: Sequence[Mapping[str, Any]] = ()) -> list[dict[str, Any]]:
        if product_mode != "NO_PRODUCT":
            raise PhotoRequestError("automatic photo requests currently require NO_PRODUCT")
        if specs:
            probe = self.repository.get_content_recipe(specs[0].recipe_id)
            if probe is not None and recipe_is_mx_wig_choice(probe):
                # Explicit MX dispatch: the wig flow freezes its own requests
                # with the frozen hair plan; all other recipes keep the
                # original path below untouched.
                from services.photo_wig_planner import build_mx_wig_requests
                return build_mx_wig_requests(
                    self, record_id=record_id, specs=specs,
                    category_key=category_key, product_mode=product_mode,
                    overrides=overrides,
                )
        if overrides and len(overrides) != len(specs):
            raise PhotoRequestError("photo overrides must match the requested count")
        profile_use, asset_use, copy_use = Counter(), Counter(), Counter()
        output = []
        used = set()
        inventory = getattr(self.repository, "list_photo_content_signatures", None)
        reserved = set(inventory(exclude_record_id=record_id)) if callable(inventory) else set()
        for index, spec in enumerate(specs, 1):
            recipe = self.repository.get_content_recipe(spec.recipe_id)
            if recipe is None or recipe.status != "active":
                raise PhotoRequestError(f"active photo Recipe is not seeded: {spec.recipe_id}")
            recipe_spec = recipe.recipe_spec_json or {}
            errors = validate_execution_profiles(recipe_spec)
            if errors:
                raise PhotoRequestError(f"{spec.recipe_id}: " + "; ".join(errors))
            if (recipe_spec.get("category_key") != category_key or spec.market not in recipe_spec.get("markets", [])
                    or product_mode not in recipe_spec.get("product_modes", [])):
                raise PhotoRequestError("preset market/category/product mode does not match Recipe")
            layout = self.layouts.get(str(recipe_spec.get("template_id") or ""))
            if not layout or int(layout.get("layout_version") or layout.get("template_version") or 0) != recipe_spec.get("template_version"):
                raise PhotoRequestError("Recipe layout is missing or version mismatched")
            if layout.get("schema_version") != "opv-photo-layout-v2":
                raise PhotoRequestError("NEEDS_CONTENT: 新请求须使用可执行布局 v2 和已核验内容卡；旧冻结批次仍可续跑")
            override = dict(overrides[index - 1]) if overrides else {}
            allowed = {"profile_id", "variables", "copy", "asset_set_id", "asset_set_key"}
            if set(override) - allowed:
                raise PhotoRequestError("unsupported photo override fields: " + ", ".join(sorted(set(override) - allowed)))
            if any(key in override and not isinstance(override[key], Mapping) for key in ("variables", "copy")):
                raise PhotoRequestError("photo variables/copy overrides must be objects")
            if override.get("asset_set_id") and override.get("asset_set_key"):
                raise PhotoRequestError("choose asset_set_id or asset_set_key, not both")
            if override.get("copy"):
                raise PhotoRequestError(
                    "source-bound 图文不允许自由覆盖文案；请选择已审核文案版本，修改后需新建内容版本"
                )
            options = []
            content_errors = []
            available_signatures = set()
            requirements = recipe_spec.get("asset_requirements") or {}
            for profile in recipe_spec["execution_profiles"]:
                if override.get("profile_id") and override["profile_id"] != profile["profile_id"]:
                    continue
                variables = {**copy.deepcopy(profile["variables"]), **dict(override.get("variables") or {})}
                errors = validate_variables(recipe_spec.get("variables_schema") or {}, variables)
                if errors:
                    raise PhotoRequestError("; ".join(errors))
                keys = [override["asset_set_key"]] if override.get("asset_set_key") else profile["asset_set_keys"]
                if override.get("asset_set_id"):
                    pinned = self.repository.get_asset_set(str(override["asset_set_id"]))
                    keys = [pinned.asset_set_key] if pinned else ["__missing__"]
                required_tags = {
                    key: variables[key] for key in recipe_spec["asset_match_keys"] if key in variables
                }
                if override.get("asset_set_id"):
                    pinned = self.repository.get_asset_set(str(override["asset_set_id"]))
                    candidates = []
                    if pinned is not None:
                        try:
                            validate_asset_set(pinned, verify_files=True)
                            expected_tags = {
                                **required_tags, **dict(requirements.get("required_tags") or {})
                            }
                            if (pinned.status == "enabled" and pinned.category_key == category_key
                                    and pinned.market in {None, spec.market}
                                    and self.assets.tags_match(pinned.tags_json or {}, expected_tags)):
                                self.assets.validate_requirements(pinned, requirements)
                                candidates = [pinned]
                        except AssetSetError:
                            candidates = []
                else:
                    candidates = self.assets.candidates(
                        category_key=category_key, market=spec.market,
                        tags=required_tags, asset_set_keys=keys, requirements=requirements,
                    )
                for candidate in candidates:
                    if override.get("asset_set_id") and override["asset_set_id"] != candidate.asset_set_id:
                        continue
                    try:
                        card = freeze_content_card(profile.get("content_card") or recipe_spec.get("content_card"), candidate,
                                                   recipe_spec.get("visual_rules") or {})
                    except ValueError as exc:
                        content_errors.append(str(exc))
                        continue
                    signature = card["content_signature"]
                    available_signatures.add(signature)
                    if signature in used or signature in reserved:
                        continue
                    for variant in profile["copy_variants"]:
                        copy_block = {**copy.deepcopy(variant["copy"]), **dict(override.get("copy") or {})}
                        travel_contract = recipe_spec.get("travel_contract") or {}
                        extra_tokens = {}
                        if travel_contract:
                            destination = str((travel_contract.get("destination_labels_th") or {}).get(
                                str(variables.get("destination") or ""), ""))
                            temperature = str((travel_contract.get("temperature_labels_th") or {}).get(
                                str(variables.get("temperature_band") or ""), ""))
                            serialized = json.dumps(copy_block, ensure_ascii=False)
                            if "{destination}" in serialized and not destination:
                                raise PhotoRequestError("旅行文案缺少目的地泰语标签，无法冻结")
                            if "{temperature}" in serialized and not temperature:
                                raise PhotoRequestError("旅行文案缺少温度泰语标签，无法冻结")
                            extra_tokens = {"destination": destination, "temperature": temperature}
                        try:
                            copy_block = resolve_photo_copy(copy_block, assets=candidate.manifest_json["assets"], locale=spec.language, extra_tokens=extra_tokens)
                        except ValueError as exc:
                            raise PhotoRequestError(str(exc)) from exc
                        errors = validate_copy(copy_block)
                        if errors:
                            raise PhotoRequestError("; ".join(errors))
                        identity = (recipe.recipe_id, profile["profile_id"], candidate.asset_set_id, variant["copy_id"])
                        rank = (profile_use[identity[:2]], asset_use[candidate.asset_set_key],
                                copy_use[(recipe.recipe_id, fingerprint(copy_block))],
                                fingerprint([record_id, recipe.recipe_id, index, identity]))
                        options.append((rank, profile, candidate, variant, variables, copy_block, card))
            if not options:
                if available_signatures:
                    raise PhotoRequestError(f"NEEDS_CONTENT: 请求 {len(specs)} 篇，但仅找到 {len(used)} 份未占用的有效内容；同图同逻辑换标题不增加库存，整批未冻结")
                if content_errors:
                    raise PhotoRequestError("; ".join(sorted(set(content_errors))))
                raise AssetSetError(f"NEEDS_ASSET: {recipe.recipe_id} has no profile matching enabled assets/variables/roles")
            _, profile, asset_set, variant, variables, copy_block, card = min(options, key=lambda item: item[0])
            used.add(card["content_signature"])
            profile_use[(recipe.recipe_id, profile["profile_id"])] += 1
            asset_use[asset_set.asset_set_key] += 1
            copy_use[(recipe.recipe_id, fingerprint(copy_block))] += 1
            request = {
                "schema_version": "opv-photo-request-v2", "content_card": card, "account_id": spec.account_id,
                "market": spec.market, "locale": spec.language,
                "recipe_id": recipe.recipe_id, "category_key": category_key, "product_mode": product_mode,
                "profile_id": profile["profile_id"], "profile_label": profile.get("label") or profile["profile_id"],
                "copy_variant_id": variant["copy_id"], "variables": variables, "copy": copy_block,
                "asset_set_id": asset_set.asset_set_id, "asset_set_key": asset_set.asset_set_key,
                "asset_set_version": asset_set.asset_set_version,
                "asset_snapshot": self.assets.freeze(asset_set), "recipe_snapshot": recipe.to_row(),
                "layout_snapshot": copy.deepcopy(layout),
            }
            request["request_sha256"] = fingerprint(request)
            validate_frozen_request(request)
            output.append(request)
        return output

    @staticmethod
    def summary(requests: Sequence[Mapping[str, Any]]) -> str:
        entries = []
        for index, request in enumerate(requests, 1):
            card = request.get("content_card") or {}
            theme = request.get("theme_brief") or {}
            pages = "；".join(
                f"P{page.get('index')} {page.get('purpose_zh')}"
                for page in card.get("pages") or []
            )
            copy_status = str(
                request.get("copy", {}).get("language_review_status") or ""
            )
            language = {
                "pending_native_review": "泰语待人工确认",
                "production_copy_pack": "生产文案包",
                "DRAFT": "泰语草稿（未母语审校）",
                "MODEL_CHECKED": "泰语模型自查",
                "NATIVE_APPROVED": "泰语已母语审定",
            }.get(copy_status, "文案已配置")
            variation = dict(theme.get("batch_variation") or {})
            variation_text = ""
            if variation:
                variation_text = (
                    f"｜本篇方向：{variation.get('angle_zh') or variation.get('variation_id')}"
                    f"｜场景/配色：{variation.get('scene_zh') or '自动'} / "
                    f"{variation.get('palette_zh') or '自动'}"
                )
            entries.append(
                f"{index}. 主题：{theme.get('label_zh') or card.get('summary_zh') or request.get('profile_label', request['profile_id'])}"
                f"{variation_text}"
                f"｜用户问题：{card.get('question_zh') or '未说明'}"
                f"｜比较依据：{card.get('comparison_basis_zh') or '未说明'}"
                f"｜逐页：{pages or '未说明'}"
                f"｜素材：{request['asset_set_key']} V{request['asset_set_version']}"
                f"｜{language}"
            )
        return "\n".join(entries)
