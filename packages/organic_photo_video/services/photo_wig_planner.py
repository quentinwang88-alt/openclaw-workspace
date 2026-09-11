"""MX wig four-choice planner: theme, creative plan, frozen requests, 4-page map.

MX-only adapter module (``mx_wig_choice_v1``).  It owns:

- early MX theme resolution so a wig row never enters the TH theme parser;
- strict normalization of the model-produced creative plan (deterministic
  Spanish/copy contracts enforced here, no silent fallback);
- the wig content-card freezer (hair-shaped qualification evidence, never
  faked outerwear/bottom attributes);
- frozen-request construction and validation for the four-page contract;
- the production-plan mapper invoked from :mod:`services.photo_planner`.

Shared infrastructure (repository writes, revisions, packages, hashes) is
reused as-is; nothing here duplicates the underlying state machine.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from domain import statuses
from domain.contracts import (
    MX_WIG_CHOICE_FLOW,
    ensure_valid,
    validate_plan_json,
)
from domain.models import AssetSet, ContentRecipe
from domain.photo_contracts import validate_copy, validate_variables
from services.asset_set_service import AssetSetService, validate_asset_set
from services.locale_quality import copy_locale_issues
from services.photo_request_factory import PhotoRequestError, fingerprint
from services.photo_wig_flow import (
    MX_WIG_CATEGORY,
    MX_WIG_DEFAULT_THEME_LABEL_ZH,
    MX_WIG_LOGIC_KEY,
    MX_WIG_MARKET,
    MX_WIG_RECIPE_ID,
    MX_WIG_SOURCE_ROLES,
    MX_WIG_THEME_KEY,
    recipe_is_mx_wig_choice,
    request_is_mx_wig_choice,
)
from services.photo_wig_qa import check_wig_plan


class PhotoWigPlannerError(ValueError):
    pass


WIG_PLAN_STORE_SCHEMA_VERSION = "opv-photo-mx-wig-plan-v1"

# 旅行主题单元格明确不兼容：给出确定性的错误提示，而不是静默进入旅行逻辑。
_TRAVEL_THEME_MARKERS = ("旅行", "旅游", "cool_weather", "beach", "city_break")

MX_WIG_DEFAULT_THEME = {
    "theme_key": MX_WIG_THEME_KEY,
    "label_zh": MX_WIG_DEFAULT_THEME_LABEL_ZH,
    "visual_brief": (
        "同一人物四款周末发型选择；胸像/半身、自然光、简洁背景；"
        "只有发型按选项不同，服装妆容与摄影氛围保持一致"
    ),
    "market": MX_WIG_MARKET,
    "locale": "es-MX",
}


def resolve_mx_wig_theme(cell_text: str) -> Dict[str, Any]:
    """MX theme resolution — never routes into TH ``resolve_photo_theme``.

    Empty/自动 cells fall back to the default weekend-hair theme; any of the
    TH travel options raises an explicit incompatibility error instead of
    triggering travel logic.
    """
    value = str(cell_text or "").strip()
    if value in ("", "自动", "默认", MX_WIG_DEFAULT_THEME_LABEL_ZH,
                 "周末换个发型，你选哪款"):
        return dict(MX_WIG_DEFAULT_THEME)
    if any(marker in value for marker in _TRAVEL_THEME_MARKERS):
        raise PhotoWigPlannerError(
            f"当前 MX 发型预设不兼容旅行主题（{value}）；请清空图文主题或改用周末发型主题"
        )
    # MX 允许运营改写主题措辞：作为本主题的自定义表述接受，主题键不变。
    return {
        **dict(MX_WIG_DEFAULT_THEME),
        "label_zh": value,
        "custom_label": True,
    }


def reference_roles_from_plan(
    plan_item: Mapping[str, Any], persona_paths: Sequence[str],
    reference_paths: Sequence[str],
) -> Dict[str, List[str]]:
    """Map the plan's per-image uses onto supply reference role lists.

    Image indexes follow the planning order: persona identity images first,
    then the staged references in upload order.  Unlabelled uploads default to
    hair-inspiration (never clothing) uses.
    """
    ordered = [str(path) for path in persona_paths] + [str(path) for path in reference_paths]
    uses_map: Dict[int, List[str]] = {}
    for use in plan_item.get("reference_uses") or []:
        if not isinstance(use, Mapping) or use.get("image_index") is None:
            continue
        try:
            index = int(use.get("image_index"))
        except (TypeError, ValueError):
            continue
        uses_map[index] = [str(value) for value in use.get("uses") or []]
    role_keys = {
        "persona": "persona_identity_images",
        "hair_inspiration": "hair_inspiration_images",
        "environment": "environment_reference_images",
        "visual_style": "visual_style_reference_images",
    }
    roles: Dict[str, List[str]] = {key: [] for key in role_keys.values()}
    for index, path in enumerate(ordered, 1):
        uses = uses_map.get(index) or (
            ["persona"] if index <= len(persona_paths) else ["hair_inspiration"]
        )
        for use in uses:
            key = role_keys.get(use)
            if key and path not in roles[key]:
                roles[key].append(path)
    return roles


# ---------------------------------------------------------------------------
# Creative plan normalization
# ---------------------------------------------------------------------------

_OPTION_FIELDS = (
    "role", "label_es", "label_zh", "length", "length_zh", "texture",
    "texture_zh", "color_zh", "parting_zh", "silhouette_zh", "framing_zh",
    "bangs_zh", "note_zh",
)


def normalize_wig_choice_plan(
    raw: Any, *, count: int, choice_axis: str = "style",
) -> Tuple[Dict[str, Any], List[str]]:
    """Strictly normalize a model plan; returns (plan, errors)."""
    if not isinstance(raw, Mapping):
        return {}, ["计划输出必须是 JSON 对象"]
    errors: List[str] = []
    items_raw = raw.get("items")
    if not isinstance(items_raw, list) or len(items_raw) != count:
        return {}, [f"items 必须是恰好 {count} 篇的计划列表"]
    items: List[Dict[str, Any]] = []
    option_signature_sets: List[set] = []
    for index, item_raw in enumerate(items_raw, 1):
        if not isinstance(item_raw, Mapping):
            errors.append(f"items[{index}] 必须是对象")
            continue
        item = {
            "item_index": index,
            "topic_zh": str(item_raw.get("topic_zh") or "").strip(),
            "photography_direction_zh": str(
                item_raw.get("photography_direction_zh") or "").strip(),
            "wardrobe_direction_zh": str(
                item_raw.get("wardrobe_direction_zh") or "").strip(),
            "makeup_direction_zh": str(
                item_raw.get("makeup_direction_zh") or "").strip(),
            "scene_prompt_zh": str(
                item_raw.get("scene_prompt_zh") or "").strip(),
            "reference_uses": [
                dict(use) for use in item_raw.get("reference_uses") or []
                if isinstance(use, Mapping)
            ],
            "options": [
                {key: str(option.get(key) or "")
                 for key in _OPTION_FIELDS if option.get(key) is not None}
                for option in item_raw.get("options") or []
                if isinstance(option, Mapping)
            ],
            "copy": {
                "title": str((item_raw.get("copy") or {}).get("title") or "").strip(),
                "caption": str((item_raw.get("copy") or {}).get("caption") or "").strip(),
                "hashtags": [str(tag) for tag in (item_raw.get("copy") or {}).get("hashtags") or []],
                "slide_texts": [str(text) for text in (item_raw.get("copy") or {}).get("slide_texts") or []],
            },
        }
        item_errors = check_wig_plan(item)
        if item_errors:
            errors.extend(f"items[{index}]: {message}" for message in item_errors)
        items.append(item)
        option_signature_sets.append({
            tuple(option.get(key) or "" for key in (
                "length", "texture", "parting", "silhouette"))
            for option in item["options"]
        })
    if len({fingerprint(item["copy"]) for item in items}) != len(items):
        errors.append("多篇文案完全相同，缺少轮换差异")
    for index, (left, right) in enumerate(
            zip(option_signature_sets, option_signature_sets[1:]), 2):
        if left and left == right:
            errors.append(f"第 {index} 篇与上一篇的发型选项组合完全相同")
    if errors:
        return {}, errors
    plan = {
        "schema_version": WIG_PLAN_STORE_SCHEMA_VERSION,
        "theme_key": str(raw.get("theme_key") or MX_WIG_THEME_KEY),
        "locale": "es-MX",
        "choice_axis": str(raw.get("choice_axis") or choice_axis),
        "photography_direction_zh": str(raw.get("photography_direction_zh") or "").strip(),
        "items": items,
    }
    if plan["theme_key"] != MX_WIG_THEME_KEY:
        errors.append(f"theme_key 必须是 {MX_WIG_THEME_KEY}")
        return {}, errors
    return plan, []


def plan_from_items(items: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """Wrap deterministic items (tests/offline) into the frozen plan shape."""
    plan = {
        "schema_version": WIG_PLAN_STORE_SCHEMA_VERSION,
        "theme_key": MX_WIG_THEME_KEY,
        "locale": "es-MX",
        "choice_axis": "style",
        "photography_direction_zh": "",
        "items": [dict(item) for item in items],
    }
    errors: List[str] = []
    for index, item in enumerate(plan["items"], 1):
        item_errors = check_wig_plan(item)
        errors.extend(f"items[{index}]: {message}" for message in item_errors)
    if errors:
        raise PhotoWigPlannerError("；".join(errors))
    return plan


# ---------------------------------------------------------------------------
# Wig content-card freezer
# ---------------------------------------------------------------------------


def freeze_wig_content_card(
    card: Mapping[str, Any], asset_set: Any, visual_rules: Mapping[str, Any],
) -> dict:
    """Four-page wig variant of ``freeze_content_card``.

    Mirrors the shared contract shape (sources/content_signature/
    source_qualification_hash) but validates hair evidence.  Clothing rules
    are rejected here exactly as loudly as wig rules are rejected by the
    shared freezer — no rule set is ever silently ignored.
    """
    from services.photo_content import content_hash

    if not isinstance(card, Mapping):
        raise PhotoWigPlannerError("NEEDS_CONTENT: 缺少逐页内容卡")
    assets = asset_set.manifest_json.get("assets") or []
    for key in ("summary_zh", "audience_zh", "question_zh", "comparison_basis_zh", "logic_key"):
        if not isinstance(card.get(key), str) or not card[key].strip():
            raise PhotoWigPlannerError(f"NEEDS_CONTENT: content card requires {key}")
    if card.get("logic_key") != MX_WIG_LOGIC_KEY:
        raise PhotoWigPlannerError(
            f"NEEDS_CONTENT: wig card logic_key must be {MX_WIG_LOGIC_KEY}"
        )
    if not isinstance(card.get("asset_gaps"), list) or card["asset_gaps"]:
        raise PhotoWigPlannerError("NEEDS_ASSET: 内容卡的素材缺口必须明确且已补齐")
    pages = card.get("pages")
    if not isinstance(pages, list) or len(pages) != 4:
        raise PhotoWigPlannerError("NEEDS_CONTENT: wig content card requires 4 page responsibilities")
    by_role: Dict[str, Dict[str, Any]] = {}
    for asset in assets:
        role = str(asset.get("role") or "")
        if role in by_role:
            raise PhotoWigPlannerError("NEEDS_ASSET: ambiguous source role")
        by_role[role] = asset
    used = set()
    for index, page in enumerate(pages, 1):
        if page.get("index") != index or not page.get("purpose_zh"):
            raise PhotoWigPlannerError("NEEDS_CONTENT: each page needs ordered index and responsibility")
        roles = page.get("source_roles")
        count = {"single": 1, "split_vertical": 2, "grid_2x2": 4}.get(page.get("layout"))
        if (not isinstance(roles, list) or len(roles) != count or len(set(roles)) != len(roles)
                or any(role not in by_role for role in roles)):
            raise PhotoWigPlannerError("NEEDS_ASSET: page layout/source roles mismatch")
        used.update(roles)
    approval = asset_set.manifest_json.get("content_approval") or {}
    if (approval.get("schema_version") != "opv-source-qualification-v1"
            or not approval.get("reviewer")
            or card["logic_key"] not in approval.get("allowed_logic_keys", [])):
        raise PhotoWigPlannerError("NEEDS_CONTENT: source qualification does not permit this content logic")
    attributes = approval.get("attributes") or {}
    for role in used:
        asset = by_role[role]
        if approval.get("source_hashes", {}).get(asset["asset_id"]) != asset["sha256"]:
            raise PhotoWigPlannerError("NEEDS_CONTENT: source qualification hash is missing or stale")
        if not isinstance(attributes.get(asset["asset_id"]), Mapping):
            raise PhotoWigPlannerError("NEEDS_CONTENT: source role lacks verified attributes")
    props = {role: attributes[by_role[role]["asset_id"]] for role in used}
    supported = {
        "same_identity", "same_camera_scale", "distinct_hairstyles",
        "hairline_visible", "choice_labels",
    }
    clothing_rules = {"distinct_looks", "garment_relations", "layering_progression", "show_full_body"}
    for key, value in visual_rules.items():
        if value and key in clothing_rules:
            raise PhotoWigPlannerError(
                f"NEEDS_CONTENT: 假发任务不接受服装规则 {key}；请检查 Recipe 配置"
            )
        if value and key not in supported:
            raise PhotoWigPlannerError(
                f"NEEDS_CONTENT: unsupported visual rule must not be silently ignored ({key})"
            )
    if visual_rules.get("same_identity"):
        values = {props[role].get("identity_id") for role in used}
        if None in values or len(values) != 1:
            raise PhotoWigPlannerError("NEEDS_CONTENT: same_identity is not evidenced by the persona references")
    if visual_rules.get("same_camera_scale"):
        values = {props[role].get("camera_scale") for role in used}
        if None in values or len(values) != 1:
            raise PhotoWigPlannerError("NEEDS_CONTENT: same_camera_scale is not evidenced")
    required_distinct = int(visual_rules.get("distinct_hairstyles") or 0)
    if required_distinct:
        signatures = {
            tuple(str(props[role].get(key) or "") for key in (
                "length", "texture", "color", "parting", "silhouette"))
            for role in used
        }
        if len(signatures) < required_distinct:
            raise PhotoWigPlannerError(
                "NEEDS_CONTENT: not enough verified distinct hairstyles"
            )
    if visual_rules.get("hairline_visible") and not all(
            props[role].get("hairline_visible") is True for role in used):
        raise PhotoWigPlannerError("NEEDS_CONTENT: hairline visibility is not evidenced")
    if visual_rules.get("choice_labels"):
        labels = [str(props[role].get("label_es") or "") for role in sorted(used)]
        if any(not label for label in labels) or len(set(labels)) != len(labels):
            raise PhotoWigPlannerError("NEEDS_CONTENT: choice labels are missing or duplicated")
    frozen = copy.deepcopy(dict(card))
    frozen["sources"] = [
        {"role": role, "asset_id": by_role[role]["asset_id"], "sha256": by_role[role]["sha256"]}
        for role in sorted(used)
    ]
    frozen["content_signature"] = content_hash({
        "logic_key": card["logic_key"],
        "source_hashes": sorted({by_role[role]["sha256"] for role in used}),
    })
    frozen["source_qualification_hash"] = content_hash(approval)
    return frozen


# ---------------------------------------------------------------------------
# Frozen-request construction and validation
# ---------------------------------------------------------------------------

_MX_OVERRIDE_ALLOWED = {"asset_set_id", "asset_set_key", "wig_plan_item"}


def build_mx_wig_requests(
    factory: Any, *, record_id: str, specs: Sequence[Any],
    category_key: str, product_mode: str = "NO_PRODUCT",
    overrides: Sequence[Mapping[str, Any]] = (),
) -> List[Dict[str, Any]]:
    """PhotoRequestFactory MX branch: four-page frozen wig requests.

    Called from ``PhotoRequestFactory.build_batch`` when the resolved recipe
    carries ``execution_flow=mx_wig_choice_v1``.  Keeps the same inventory
    protections (NO_PRODUCT only, asset verification, cross-record content
    signature reservation) while sourcing copy from the frozen wig plan.
    """
    if product_mode != "NO_PRODUCT":
        raise PhotoRequestError("MX 假发四选一首版只支持 NO_PRODUCT；单品模式未开放")
    if overrides and len(overrides) != len(specs):
        raise PhotoRequestError("photo overrides must match the requested count")
    layouts = {str(item.get("layout_id") or item.get("template_id")): dict(item)
               for item in getattr(factory, "layouts", {}).values()}
    repository = factory.repository
    assets_service = factory.assets
    inventory = getattr(repository, "list_photo_content_signatures", None)
    reserved = set(inventory(exclude_record_id=record_id)) if callable(inventory) else set()
    used: set = set()
    output: List[Dict[str, Any]] = []
    for index, spec in enumerate(specs, 1):
        override = dict(overrides[index - 1]) if overrides else {}
        if set(override) - _MX_OVERRIDE_ALLOWED:
            raise PhotoRequestError(
                "unsupported MX photo override fields: "
                + ", ".join(sorted(set(override) - _MX_OVERRIDE_ALLOWED))
            )
        plan_item = override.get("wig_plan_item")
        if not isinstance(plan_item, Mapping) or not plan_item.get("options"):
            raise PhotoRequestError(
                f"第 {index} 篇缺少冻结的发型内容计划（wig_plan_item）；不能凭空生成四选一请求"
            )
        recipe = repository.get_content_recipe(spec.recipe_id)
        if recipe is None or recipe.status != "active":
            raise PhotoRequestError(f"active photo Recipe is not seeded: {spec.recipe_id}")
        if not recipe_is_mx_wig_choice(recipe):
            raise PhotoRequestError(
                f"{spec.recipe_id} 不是 mx_wig_choice_v1 Recipe，禁止进入假发请求构建"
            )
        recipe_spec = dict(recipe.recipe_spec_json or {})
        if (recipe_spec.get("category_key") != category_key
                or spec.market not in recipe_spec.get("markets", [])
                or product_mode not in recipe_spec.get("product_modes", [])):
            raise PhotoRequestError("preset market/category/product mode does not match Recipe")
        if spec.language != "es-MX":
            raise PhotoRequestError("MX 假发预设语言必须是 es-MX")
        layout = layouts.get(str(recipe_spec.get("template_id") or ""))
        if not layout or int(layout.get("layout_version") or layout.get("template_version") or 0) != recipe_spec.get("template_version"):
            raise PhotoRequestError("Recipe layout is missing or version mismatched")
        if layout.get("schema_version") != "opv-photo-layout-v2":
            raise PhotoRequestError("NEEDS_CONTENT: MX 请求须使用可执行布局 v2")
        plan_errors = check_wig_plan(dict(plan_item))
        if plan_errors:
            raise PhotoRequestError(
                f"第 {index} 篇冻结发型计划未通过校验：{'；'.join(plan_errors)}"
            )
        asset_set_id = str(override.get("asset_set_id") or "")
        pinned = repository.get_asset_set(asset_set_id) if asset_set_id else None
        if pinned is None:
            raise PhotoRequestError(f"NEEDS_ASSET: 第 {index} 篇的素材集不存在：{asset_set_id}")
        requirements = dict(recipe_spec.get("asset_requirements") or {})
        variables = dict((recipe_spec.get("execution_profiles") or [{}])[0].get("variables") or {})
        variable_errors = validate_variables(recipe_spec.get("variables_schema") or {}, variables)
        if variable_errors:
            raise PhotoRequestError("; ".join(variable_errors))
        try:
            validate_asset_set(pinned, verify_files=True)
            expected_tags = {
                **variables,
                **{key: value for key, value in variables.items()
                   if key in recipe_spec.get("asset_match_keys", [])},
                **dict(requirements.get("required_tags") or {}),
            }
            if (pinned.status != "enabled" or pinned.category_key != category_key
                    or pinned.market not in {None, spec.market}
                    or not assets_service.tags_match(pinned.tags_json or {}, expected_tags)):
                raise PhotoRequestError(f"NEEDS_ASSET: 第 {index} 篇素材集与预设不匹配")
            assets_service.validate_requirements(pinned, requirements)
        except PhotoRequestError:
            raise
        except Exception as exc:  # noqa: BLE001 - asset boundary
            raise PhotoRequestError(f"NEEDS_ASSET: 第 {index} 篇素材集不可用：{exc}") from exc
        roles_in_set = [str(item.get("role")) for item in pinned.manifest_json.get("assets") or []]
        if roles_in_set != list(MX_WIG_SOURCE_ROLES) or "cover_seed" in roles_in_set:
            raise PhotoRequestError(
                f"NEEDS_ASSET: 第 {index} 篇素材角色必须是 {list(MX_WIG_SOURCE_ROLES)}（无 cover_seed）"
            )
        card = freeze_wig_content_card(
            recipe_spec.get("content_card"), pinned, recipe_spec.get("visual_rules") or {},
        )
        signature = card["content_signature"]
        if signature in used or signature in reserved:
            raise PhotoRequestError(
                f"NEEDS_CONTENT: 第 {index} 篇的内容签名已被占用，同图不能重复冻结"
            )
        used.add(signature)
        plan_evidence_ok = _plan_matches_asset_set(plan_item, pinned)
        if not plan_evidence_ok:
            raise PhotoRequestError(
                f"第 {index} 篇的发型计划与素材集证据不一致（选项哈希不匹配）"
            )
        copy_block = copy.deepcopy(dict(plan_item["copy"]))
        copy_errors = validate_copy(copy_block, expected_slide_count=4)
        copy_errors.extend(copy_locale_issues(copy_block, "es-MX"))
        if copy_errors:
            raise PhotoRequestError("; ".join(copy_errors))
        request = {
            "schema_version": "opv-photo-request-v2",
            "execution_flow": MX_WIG_CHOICE_FLOW,
            "content_card": card,
            "account_id": spec.account_id,
            "market": spec.market, "locale": spec.language,
            "recipe_id": recipe.recipe_id, "category_key": category_key,
            "product_mode": product_mode,
            "profile_id": str((recipe_spec.get("execution_profiles") or [{}])[0].get(
                "profile_id") or "mx_weekend_hair_choice"),
            "profile_label": str((recipe_spec.get("execution_profiles") or [{}])[0].get(
                "label") or ""),
            "copy_variant_id": "wig_plan_bound",
            "variables": variables, "copy": copy_block,
            "asset_set_id": pinned.asset_set_id,
            "asset_set_key": pinned.asset_set_key,
            "asset_set_version": pinned.asset_set_version,
            "asset_snapshot": assets_service.freeze(pinned),
            "recipe_snapshot": recipe.to_row(),
            "layout_snapshot": copy.deepcopy(layout),
            "wig_plan_item": copy.deepcopy(dict(plan_item)),
        }
        request["request_sha256"] = fingerprint({
            key: value for key, value in request.items() if key != "request_sha256"
        })
        validate_mx_wig_frozen_request(request)
        output.append(request)
    return output


def _plan_matches_asset_set(plan_item: Mapping[str, Any], asset_set: Any) -> bool:
    """Every planned option must be evidenced inside the qualified asset set."""
    from services.photo_wig_qa import _HAIR_DISTINCT_KEYS  # noqa: PLC0415

    approval = (asset_set.manifest_json or {}).get("content_approval") or {}
    attributes = approval.get("attributes") or {}
    evidence = set()
    for asset in asset_set.manifest_json.get("assets") or []:
        attrs = attributes.get(asset.get("asset_id")) or {}
        evidence.add(tuple(str(attrs.get(key) or "") for key in _HAIR_DISTINCT_KEYS))
    planned = {
        tuple(str(option.get(key) or "") for key in _HAIR_DISTINCT_KEYS)
        for option in plan_item.get("options") or []
    }
    return planned == evidence and len(evidence) == 4


def validate_mx_wig_frozen_request(request: Mapping[str, Any]) -> None:
    """Full frozen-request validation for ``mx_wig_choice_v1``.

    Performs every shared check (fingerprint, schema, variables, copy, recipe
    identity, card/layout re-freeze) with MX semantics, plus persona-evidence
    and four-page conditions.  A request that merely carries a boolean marker
    can NOT pass: the five-field dispatch set is re-verified here.
    """
    if not request_is_mx_wig_choice(request):
        raise PhotoRequestError("frozen request is not a valid mx_wig_choice_v1 request")
    unsigned = {key: value for key, value in request.items() if key != "request_sha256"}
    if request.get("request_sha256") != fingerprint(unsigned):
        raise PhotoRequestError("frozen photo request fingerprint mismatch")
    if request.get("schema_version") != "opv-photo-request-v2":
        raise PhotoRequestError("unsupported frozen photo request schema")
    if request.get("product_mode") != "NO_PRODUCT":
        raise PhotoRequestError("mx_wig_choice_v1 首版只允许 NO_PRODUCT")
    if request.get("locale") != "es-MX":
        raise PhotoRequestError("mx_wig_choice_v1 语言必须是 es-MX")
    recipe = ContentRecipe.from_row(request["recipe_snapshot"])
    errors = validate_variables(
        recipe.recipe_spec_json.get("variables_schema") or {}, request.get("variables"))
    errors += validate_copy(request.get("copy"), expected_slide_count=4)
    errors += copy_locale_issues(dict(request.get("copy") or {}), "es-MX")
    if errors:
        raise PhotoRequestError("; ".join(errors))
    if (recipe.recipe_id != request.get("recipe_id")
            or request.get("category_key") != recipe.recipe_spec_json.get("category_key")
            or request.get("market") not in recipe.recipe_spec_json.get("markets", [])
            or request.get("asset_set_id") != request["asset_snapshot"]["asset_set_id"]):
        raise PhotoRequestError("frozen photo request identity mismatch")
    card = request.get("content_card")
    checked = freeze_wig_content_card(
        card, AssetSet.from_row(request["asset_snapshot"]),
        recipe.recipe_spec_json.get("visual_rules") or {},
    )
    if checked != card or request["layout_snapshot"].get("schema_version") != "opv-photo-layout-v2":
        raise PhotoRequestError("frozen content card or layout mismatch")
    plan_item = request.get("wig_plan_item")
    if not isinstance(plan_item, Mapping) or not plan_item.get("options"):
        raise PhotoRequestError("frozen request lacks the wig plan item evidence")
    asset_set = AssetSet.from_row(request["asset_snapshot"])
    if not _plan_matches_asset_set(plan_item, asset_set):
        raise PhotoRequestError("frozen wig plan does not match the qualified asset evidence")
    approval = (asset_set.manifest_json or {}).get("content_approval") or {}
    attributes = approval.get("attributes") or {}
    identity_ids = {
        str((attrs or {}).get("identity_id") or "")
        for attrs in attributes.values()
    }
    if len(identity_ids) != 1 or "" in identity_ids:
        raise PhotoRequestError("persona identity evidence missing or inconsistent across the four options")


def build_mx_theme_brief(plan_item: Mapping[str, Any], theme: Mapping[str, Any],
                         reference_mode: str, reference_count: int) -> Dict[str, Any]:
    return {
        "theme_key": theme.get("theme_key") or MX_WIG_THEME_KEY,
        "label_zh": theme.get("label_zh") or MX_WIG_DEFAULT_THEME_LABEL_ZH,
        "market": MX_WIG_MARKET,
        "locale": "es-MX",
        "reference_mode": reference_mode,
        "reference_count": reference_count,
        "wig_plan": copy.deepcopy(dict(plan_item)),
    }


# ---------------------------------------------------------------------------
# Production plan mapping (called from PhotoReusePlannerService)
# ---------------------------------------------------------------------------


def plan_mx_wig_task(
    service: Any, task_id: str, *, recipe_id: str,
    variables: Mapping[str, Any], copy_block: Mapping[str, Any],
    layout: Mapping[str, Any], asset_set_id: Optional[str] = None,
    operator: str = "operator",
    recipe_snapshot: Optional[Mapping[str, Any]] = None,
    asset_snapshot: Optional[Mapping[str, Any]] = None,
    execution_profile_id: str = "", copy_variant_id: str = "",
    content_card: Optional[Mapping[str, Any]] = None,
    theme_brief: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Four-page production plan; mirrors ``plan_task`` with MX contracts."""
    repository = service.repository
    task = repository.get_task(task_id)
    if task is None or task.task_status not in {statuses.TASK_DRAFT, statuses.TASK_PLANNED}:
        raise PhotoWigPlannerError("photo task must be draft or planned")
    if task.media_kind != "native_photo":
        raise PhotoWigPlannerError("photo planner requires media_kind=native_photo")
    recipe = (ContentRecipe.from_row(recipe_snapshot) if recipe_snapshot
              else repository.get_content_recipe(recipe_id))
    if recipe is None or recipe.status != "active":
        raise PhotoWigPlannerError("photo recipe is missing or inactive")
    if recipe.recipe_id != recipe_id:
        raise PhotoWigPlannerError("frozen recipe id does not match request")
    if not recipe_is_mx_wig_choice(recipe):
        raise PhotoWigPlannerError("recipe is not an mx_wig_choice_v1 recipe")
    spec = dict(recipe.recipe_spec_json or {})
    if spec.get("category_key") != task.category_key:
        raise PhotoWigPlannerError("recipe category does not match task")
    if task.target_country not in (spec.get("markets") or []):
        raise PhotoWigPlannerError("recipe does not support the task market")
    if task.product_mode not in (spec.get("product_modes") or []):
        raise PhotoWigPlannerError("recipe does not support the task product mode")
    if task.target_locale != "es-MX":
        raise PhotoWigPlannerError("mx_wig_choice_v1 任务语言必须是 es-MX")
    errors = validate_variables(spec.get("variables_schema") or {}, variables)
    if errors:
        raise PhotoWigPlannerError("; ".join(errors))
    template_id = str(layout.get("template_id") or layout.get("layout_id") or "")
    template_version = int(layout.get("template_version") or layout.get("layout_version") or 0)
    if (template_id != spec.get("template_id")
            or template_version != int(spec.get("template_version") or 0)):
        raise PhotoWigPlannerError("layout does not match the version frozen by recipe")
    if layout.get("schema_version") != "opv-photo-layout-v2":
        raise PhotoWigPlannerError("mx_wig_choice_v1 requires executable layout v2")
    requirements = spec.get("asset_requirements") or {}
    visual_tags = {key: variables[key] for key in spec.get("asset_match_keys", list(variables)) if key in variables}
    if asset_snapshot:
        asset_set = service.asset_sets.from_frozen(
            asset_snapshot, category_key=task.category_key, market=task.target_country,
            tags=visual_tags, requirements=requirements,
        )
        if asset_set.asset_set_id != asset_set_id:
            raise PhotoWigPlannerError("frozen asset set id does not match request")
    else:
        asset_set = service.asset_sets.select(
            category_key=task.category_key, market=task.target_country,
            content_key=task.idempotency_key, tags=visual_tags,
            requirements=requirements, asset_set_id=asset_set_id,
        )
    assets = service.asset_sets.ordered_assets(
        asset_set, roles=requirements.get("required_roles") or [], count=4)
    if len(assets) != 4:
        raise PhotoWigPlannerError("NEEDS_ASSET: mx_wig_choice_v1 requires exactly four sources")
    story = list(recipe.story_structure_json or [])
    if len(story) != 4:
        raise PhotoWigPlannerError("mx_wig_choice_v1 recipe must contain four story slots")
    copy_errors = validate_copy(copy_block, expected_slide_count=4)
    copy_errors.extend(copy_locale_issues(dict(copy_block), task.target_locale))
    if copy_errors:
        raise PhotoWigPlannerError("; ".join(copy_errors))
    slide_texts = [str(value) for value in copy_block.get("slide_texts") or []]
    frozen_card = None
    if content_card or spec.get("content_card"):
        frozen_card = freeze_wig_content_card(
            content_card or spec["content_card"], asset_set,
            spec.get("visual_rules") or {},
        )
        if content_card and frozen_card != content_card:
            raise PhotoWigPlannerError("frozen content card changed")
    role_slots = {str(item["role"]): index for index, item in enumerate(assets, 1)}
    slides = []
    shots = []
    for index, slot in enumerate(story, 1):
        if int(slot.get("slot_index") or index) != index:
            raise PhotoWigPlannerError("recipe story slots must be in 1..4 order")
        role = str(slot.get("role") or f"slide_{index}")
        page = frozen_card["pages"][index - 1] if frozen_card else None
        source_roles = list((page or {}).get("source_roles") or [role])
        if any(value not in role_slots for value in source_roles):
            raise PhotoWigPlannerError("page source role is not a qualified asset role")
        source_slots = [role_slots[value] for value in source_roles]
        slides.append({
            "slot_index": index,
            "slot_role": role,
            "source_kind": "reused_asset",
            "source_refs": [assets[value - 1]["asset_id"] for value in source_slots],
            "source_slots": source_slots,
            "overlay_text": slide_texts[index - 1],
            "layout_snapshot": {
                "template_id": template_id, "template_version": template_version,
                "layout_variant": str(slot.get("layout_variant") or "HAIR_OPTION"),
                "layout": (page or {}).get("layout") or "single",
            },
        })
    for index, source in enumerate(assets, 1):
        shots.append({
            "slot_index": index,
            "slot_role": str(source.get("role") or f"source_{index}"),
            "shot_kind": "reused_asset", "asset_path": source["path"],
            "asset_sha256": source["sha256"], "source_asset_id": source["asset_id"],
        })
    pack = repository.get_market_pack(task.market_pack_id or "")
    if pack is None:
        raise PhotoWigPlannerError("task market pack is missing")
    plan = {
        "schema_version": "opv-photo-plan-v1", "workflow_version": 2,
        "media_kind": "native_photo", "category_key": task.category_key,
        "product_mode": task.product_mode,
        "execution_flow": MX_WIG_CHOICE_FLOW,
        "market_pack": {
            "id": pack.market_pack_id, "version": pack.pack_version,
            "country": pack.target_country, "locale": pack.target_locale,
        },
        "recipe": {"id": recipe.recipe_id, "version": recipe.recipe_version},
        "template": {"id": template_id, "version": template_version},
        "variables": copy.deepcopy(dict(variables)), "cover_index": 1,
        "execution_profile_id": execution_profile_id,
        "copy_variant_id": copy_variant_id,
        "copy": {
            **copy.deepcopy(dict(copy_block)),
            "title": str(copy_block["title"]),
            "caption": str(copy_block["caption"]),
            "hashtags": list(copy_block.get("hashtags") or []),
            "slide_texts": slide_texts,
        },
        "product": None, "slides": slides, "shots": shots,
        "asset_set": {
            "id": asset_set.asset_set_id, "key": asset_set.asset_set_key,
            "version": asset_set.asset_set_version,
        },
        "production_policy": {"mode": "ASSET_REUSE", "ai_image_calls": 0},
    }
    if theme_brief:
        plan["theme_brief"] = copy.deepcopy(dict(theme_brief))
    if frozen_card:
        plan.update(source_binding="roles-v1",
                    source_roles=[item["role"] for item in assets],
                    content_card=frozen_card)
    ensure_valid(validate_plan_json(plan), "photo plan")
    storyboard_version = f"{recipe.recipe_id}-v{recipe.recipe_version}"
    import hashlib

    if len(storyboard_version) > 32:
        storyboard_version = "photo-" + hashlib.sha256(
            storyboard_version.encode()).hexdigest()[:26]
    repository.update_task_plan(
        task_id, plan_json=plan, copy_json=plan["copy"], recipe_id=recipe.recipe_id,
        recipe_version=recipe.recipe_version, content_goal=recipe.content_goal,
        storyboard_version=storyboard_version,
        workflow_version=2,
    )
    if task.task_status == statuses.TASK_DRAFT:
        repository.transition_task(task_id, statuses.TASK_DRAFT, statuses.TASK_PLANNED)
    task = repository.get_task(task_id)
    if task is None:
        raise PhotoWigPlannerError(f"planned task {task_id} disappeared")
    from services.content_package import ContentPackageService
    from services.workflow_v2 import RevisionService

    package = ContentPackageService(repository).create_for_task(
        task, recipe_id=recipe.recipe_id, plan=plan
    )
    revision = RevisionService(repository).ensure_working(
        repository.get_task(task_id), operator=operator
    )
    return {
        "task_id": task_id, "plan": plan,
        "content_package_id": package.content_package_id,
        "revision_id": revision.revision_id,
    }


def summarize_mx_wig_plan(wig_plan: Mapping[str, Any]) -> str:
    """Operator-facing summary for the Feishu 内容方案摘要 column."""
    lines = []
    for item in wig_plan.get("items") or []:
        options = "；".join(
            f"{option.get('role','').replace('hair_', '').upper()} {option.get('label_es')}"
            for option in item.get("options") or []
        )
        copy_block = item.get("copy") or {}
        lines.append(
            f"· {item.get('topic_zh') or ''}｜{options}｜标题：{copy_block.get('title')}"
        )
    return "MX 假发四选一计划：\n" + "\n".join(lines)
