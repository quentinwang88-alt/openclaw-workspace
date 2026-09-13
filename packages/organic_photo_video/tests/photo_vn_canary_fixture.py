#!/usr/bin/env python3
"""Phase 4 offline canary manifests for the VN scarf line (spec §7 Phase 4.5).

Phase 4 activates Vietnam *offline*: the Market Pack, the Vietnamese Locale /
Copy Packs, the destination weights, the persona binding and the disabled VN
presets.  Spec §14.2 then requires the phase to emit **one manifest per
reference mode** -- ``STYLE`` / ``STYLE + PRODUCT`` / ``COMPLETE_LOOK`` -- so a
reviewer can see, without generating a single image, what the VN scarf travel
line would plan.

Each manifest is a frozen, deterministic artefact with three parts:

* ``context``      -- the resolved execution snapshot (spec §4);
* ``content_plan`` -- the country-agnostic planner output bound to vi-VN;
* ``unresolved``   -- every binding that is declared but not yet delivered, so
  the manifest can never be mistaken for "ready to publish" (spec §9).

The builders here are pure: no Feishu, no RDS, no image generation, no network.
``build_all()`` is called both by the regeneration test (which keeps the shipped
JSON honest) and by ``python -m`` execution to rewrite them.
"""
from __future__ import annotations

import copy
import csv
import hashlib
import io
import json
import sys
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from config import loader  # noqa: E402
from services.photo_category_registry import (  # noqa: E402
    apply_target_product_to_look,
    build_product_qa_contract,
    get_photo_category_adapter,
)
from services.photo_content_planner import plan_th_choice_batch  # noqa: E402
from services.photo_execution_context import build_execution_context  # noqa: E402

CANARY_SCHEMA_VERSION = "opv-photo-canary-manifest-v1"

RECIPE_ID = "PHOTO_TRAVEL_OUTFIT_V3"
RECIPE_PATH = PACKAGE_ROOT / "config" / "recipes" / f"{RECIPE_ID}.json"
DESTINATION_PATH = PACKAGE_ROOT / "config" / "destinations" / "EAST_ASIA_COOL_V1.json"
LOCALE_PACK_PATH = PACKAGE_ROOT / "config" / "locales" / "LOCALE_VI_VN_V1.json"
MARKET_PACK_PATH = PACKAGE_ROOT / "config" / "market_packs" / "MP_VN_DEFAULT_v1.json"
ACCOUNT_PATH = PACKAGE_ROOT / "config" / "accounts" / "OPV_VN_TEST_001.json"
COPY_PACK_PATH = PACKAGE_ROOT / "config" / "copy_packs" / "VN_TRAVEL_OUTFIT_V1.tsv"

PLANNING_FLOW = "travel_two_step"
THEME_KEY = "COOL_WEATHER_TRAVEL"
MARKET_PACK_ID = "MP_VN_DEFAULT_V1"
LOCALE = "vi-VN"
LOCALE_PACK_ID = "LOCALE_VI_VN_V1"
COPY_PACK_ID = "VN_TRAVEL_OUTFIT_V1"
DESTINATION_CATALOG_ID = "EAST_ASIA_COOL_V1"
ACCOUNT_ID = "OPV_VN_TEST_001"

#: The synthetic scarf the STYLE+PRODUCT canary carries.  The id and pack id are
#: deliberately fixture-scoped: Phase 4 proves the *mechanism* (one scarf lands
#: in ``accessories`` for A-D); the real product pack arrives with the operator.
CANARY_PRODUCT = {
    "product_id": "VN_SCARF_CANARY_001",
    "product_name": "格纹羊毛围巾",
    "category": "scarf",
    "variant_key": "camel-check",
    "reference_pack_id": "PK_VN_SCARF_CANARY",
    "reference_pack_version": 1,
}

CANARIES = (
    {
        "canary_id": "VN_SCARF_STYLE",
        "canary_label_zh": "VN + scarf + STYLE（纯风格，无商品）",
        "reference_mode": "STYLE",
        "destination_id": "SEOUL_WINTER",
        "destination_key": "seoul",
        "temperature_band": "0_5c",
        "product": False,
        "spec_ref": "§8.4 canary 1",
    },
    {
        "canary_id": "VN_SCARF_STYLE_PRODUCT",
        "canary_label_zh": "VN + scarf + STYLE + PRODUCT（首尔，同一围巾进入 A-D）",
        "reference_mode": "STYLE",
        "destination_id": "SEOUL_WINTER",
        "destination_key": "seoul",
        "temperature_band": "0_5c",
        "product": True,
        "spec_ref": "§8.4 canary 2",
    },
    {
        "canary_id": "VN_SCARF_COMPLETE_LOOK",
        "canary_label_zh": "VN + scarf + COMPLETE_LOOK（上传四套完整穿搭，不生图）",
        "reference_mode": "COMPLETE_LOOK",
        "destination_id": "SEOUL_WINTER",
        "destination_key": "seoul",
        "temperature_band": "0_5c",
        "product": False,
        "spec_ref": "§8.4 canary 4",
    },
)
CANARIES_BY_ID = {item["canary_id"]: item for item in CANARIES}

PLAN_ITEM_LOOK_FIELDS = (
    "role", "travel_moment", "scene_prompt", "weather_logic",
    "outerwear", "top_inner", "bottom", "shoes",
    "outerwear_type", "bottom_type", "footwear_type",
)


def digest(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _locale_pack() -> dict:
    return loader.load_locale_pack_file(LOCALE_PACK_PATH)


def _destination(destination_id: str) -> dict:
    catalog = loader.load_destination_catalog_file(DESTINATION_PATH)
    return next(
        entry for entry in catalog["destinations"]
        if entry["destination_id"] == destination_id
    )


def _theme(pack: dict) -> dict:
    generic = pack["labels"]["generic"]
    return {
        "theme_key": THEME_KEY,
        "cta": generic["cta"],
        "visual_brief": "VN 围巾旅行穿搭：同一人物、同一目的地视觉体系、统一色彩基调",
    }


def _style_profile(spec: dict, destination_key: str, temperature_band: str) -> dict:
    """Synthesize the vision contract the STYLE path consumes.

    Production receives this from the vision model; a canary must be offline and
    deterministic, so the four looks are derived from the recipe's own travel
    contract (the same moments, the same first allowed footwear type per moment).
    """
    moments = list(spec["travel_contract"]["moments"])
    moments = moments[: int(spec["travel_contract"]["moments_per_post"])]
    all_footwear = list(spec["travel_contract"]["footwear_types"])
    looks = []
    for offset, (letter, moment) in enumerate(zip("abcd", moments)):
        allowed = list(moment["allowed_footwear_types"])
        if not allowed:
            # A LOW-mobility moment lists no positive requirement, only bans.
            forbidden = set(moment.get("forbidden_footwear_types") or [])
            allowed = [item for item in all_footwear if item not in forbidden]
        looks.append({
            "role": f"look_{letter}",
            "travel_moment": str(moment["key"]),
            "scene_prompt": str(moment["evidence_zh"]),
            "weather_logic": "室内外温度过渡",
            "outerwear": f"围巾旅行外套 {letter.upper()}",
            "top_inner": f"内搭 {letter.upper()}",
            "bottom": f"下装 {letter.upper()}",
            "shoes": f"鞋履 {letter.upper()}",
            "footwear_type": allowed[offset % len(allowed)],
            "outerwear_type": f"outerwear_{letter}",
            "bottom_type": f"bottom_{letter}",
        })
    return {
        "analysis_method": "doubao_seed_2_1",
        "planning_flow": PLANNING_FLOW,
        "presentation_type": "SCENE_MODEL",
        "palette": ["camel"],
        "temperature": "cool",
        "travel_variables": {
            "destination": destination_key,
            "temperature_band": temperature_band,
        },
        "aggregate": {"background": "东亚冷凉城市"},
        "recommended_sets": [{
            "content_angle_zh": "四场景旅行轻层搭",
            "scene_zh": "冷凉城市旅行",
            "palette_zh": "驼色、深蓝",
            "background_prompt": "",
            "style_modifier": "",
            "looks": looks,
            "copy": {},
        }],
    }


def _trim_item(item: dict) -> dict:
    trimmed = {
        "schema_version": item["schema_version"],
        "index": item["index"],
        "family_id": item["family_id"],
        "angle_zh": item["angle_zh"],
        "scene_zh": item["scene_zh"],
        "palette_zh": item["palette_zh"],
        "presentation_type": item.get("presentation_type", ""),
        "copy": copy.deepcopy(item["copy"]),
        "difference_axes": copy.deepcopy(item["difference_axes"]),
        "looks": [
            {key: look.get(key) for key in PLAN_ITEM_LOOK_FIELDS if key in look}
            for look in item.get("looks") or []
        ],
    }
    return trimmed


def _trim_plan(plan: dict) -> dict:
    return {
        "schema_version": plan["schema_version"],
        "policy_id": plan["policy_id"],
        "policy_version": plan["policy_version"],
        "theme_key": plan["theme_key"],
        "reference_mode": plan["reference_mode"],
        "count": plan["count"],
        "planning_flow": plan["planning_flow"],
        "required_roles": list(plan["required_roles"]),
        "plan_sha256": plan["plan_sha256"],
        "items": [_trim_item(item) for item in plan["items"]],
    }


def _copy_pack_review_statuses() -> list[str]:
    rows = csv.DictReader(
        io.StringIO(COPY_PACK_PATH.read_text(encoding="utf-8")), delimiter="\t"
    )
    return sorted({str(row.get("language_review_status") or "") for row in rows})


def _unresolved_bindings(spec: dict, market: dict, account: dict) -> list[dict]:
    """Every declared-but-undelivered binding, so the manifest is honest."""
    return [
        {
            "binding": "asset_set_keys",
            "declared": list(spec["execution_profiles"][0]["asset_set_keys"]),
            "delivered": False,
            "blocks": "real_image_generation",
            "note": (
                "V3 keeps the TH asset-set key so the Phase 2 TH equivalence stays "
                "byte-identical (test_photo_travel_v3_equivalence). A VN/market-neutral "
                "asset set is a Phase 5 blocker: without it VN cannot render."
            ),
        },
        {
            "binding": "copy_pack_language_review_status",
            "declared": _copy_pack_review_statuses(),
            "required": "NATIVE_APPROVED",
            "delivered": False,
            "blocks": "publishing",
            "note": "Vietnamese copy is DRAFT until a native speaker reviews it (spec §9).",
        },
        {
            "binding": "market_pack_status",
            "declared": [market["status"]],
            "required": "active",
            "delivered": False,
            "blocks": "publishing",
            "note": "MP_VN_DEFAULT_V1 ships as draft; activation is a Phase 5 gate.",
        },
        {
            "binding": "account_status",
            "declared": [account["status"]],
            "required": "active",
            "delivered": False,
            "blocks": "publishing",
            "note": (
                "OPV_VN_TEST_001 is paused and publishing_enabled=false until TikTok "
                "Content Posting photo capability is confirmed on the VN account."
            ),
        },
    ]


def _account_binding(account: dict) -> dict:
    rules = dict(account.get("operating_rules") or {})
    return {
        "account_id": account["account_id"],
        "status": account["status"],
        "target_country": account["target_country"],
        "default_locale": account["default_locale"],
        "persona_ref_id": account["persona_ref_id"],
        "allowed_persona_refs": list(rules.get("allowed_persona_refs") or []),
        "default_market_pack_id": account["default_market_pack_id"],
        "default_locale_pack_id": rules.get("default_locale_pack_id", ""),
        "default_render_preset_id": account["default_render_preset_id"],
        "publishing_enabled": bool(rules.get("publishing_enabled", False)),
    }


def _product_application(plan: dict, product: dict) -> dict:
    """Prove the same scarf lands in every planned look (spec §8.3).

    The content plan never sees a product; the scarf is frozen into the looks by
    the same resolver the generator path uses, so the canary shows the exact slot
    and label each of A-D would receive.
    """
    adapter = get_photo_category_adapter(str(product.get("category") or ""))
    application: dict[str, dict] = {}
    for item in plan["items"]:
        per_look: dict[str, dict] = {}
        for look in item.get("looks") or []:
            applied = apply_target_product_to_look(
                adapter=adapter, look=look, product_snapshot=product
            )
            per_look[str(look.get("role"))] = {
                "accessories": str(applied.get("accessories") or ""),
                "target_product": copy.deepcopy(applied.get("target_product") or {}),
            }
        application[str(item["family_id"])] = per_look
    return application


def _build_product_qa_contract(product: dict) -> dict:
    return build_product_qa_contract(
        adapter=get_photo_category_adapter(str(product.get("category") or "")),
        product_snapshot=product,
    )


def build_manifest(canary_id: str) -> dict:
    """Build one canary manifest from the shipped configs (deterministic)."""
    canary = CANARIES_BY_ID[canary_id]
    recipe = loader.load_content_recipe_file(RECIPE_PATH)
    spec = copy.deepcopy(recipe.recipe_spec_json)
    pack = _locale_pack()
    market = _read_json(MARKET_PACK_PATH)
    account = _read_json(ACCOUNT_PATH)
    adapter = get_photo_category_adapter("scarf")

    mode = canary["reference_mode"]
    product = dict(CANARY_PRODUCT) if canary["product"] else None
    if mode == "COMPLETE_LOOK":
        reference_hashes = [
            digest(f"phase4-canary:{canary_id}:look-{i}") for i in "abcd"
        ]
        style_profile = None
    else:
        reference_hashes = [digest(f"phase4-canary:{canary_id}:style-01")]
        style_profile = _style_profile(
            spec, canary["destination_key"], canary["temperature_band"]
        )

    # One record id per *mode* (not per canary) so STYLE and STYLE+PRODUCT differ
    # only by the product block: the product enters through the reference
    # context, never through the content plan.
    record_id = f"phase4-canary:{canary_id.removesuffix('_PRODUCT')}"

    context = build_execution_context(
        recipe_id=RECIPE_ID,
        recipe_version=int(recipe.recipe_version),
        planning_flow=PLANNING_FLOW,
        category_key=adapter.category_key,
        category_profile_id="SCARF_V1",
        category_profile_version=1,
        main_product_slot=adapter.main_product_slot,
        market_country="VN",
        market_pack_id=MARKET_PACK_ID,
        market_pack_version=int(market["pack_version"]),
        locale=LOCALE,
        locale_pack_id=LOCALE_PACK_ID,
        locale_pack_version=int(pack["locale_pack_version"]),
        copy_pack_id=COPY_PACK_ID,
        reference_mode=mode,
        input_fingerprint=digest(f"phase4-canary:{canary_id}:input"),
        reference_hashes=reference_hashes,
        destination=_destination(canary["destination_id"]),
        product=product,
        persona={
            "persona_ref_id": "FIXTURE_PERSONA_REF_VN_SCARF",
            "persona_pack_id": "FIXTURE_PERSONA_PACK_VN_SCARF_V1",
            "reference_hashes": [digest("phase4-canary:persona-ref-01")],
        },
    )

    plan = plan_th_choice_batch(
        record_id=record_id,
        recipe_id=RECIPE_ID,
        theme=_theme(pack),
        reference_mode=mode,
        count=1,
        style_profile=style_profile,
        travel_contract=spec["travel_contract"],
        copy_templates=spec["execution_profiles"][0]["copy_variants_by_locale"][LOCALE]["copy_variants"],
        recipe_spec=spec,
        variables={
            "destination": canary["destination_key"],
            "temperature_band": canary["temperature_band"],
        },
        locale_pack=pack,
    )

    manifest = {
        "schema_version": CANARY_SCHEMA_VERSION,
        "canary_id": canary_id,
        "canary_label_zh": canary["canary_label_zh"],
        "spec_ref": canary["spec_ref"],
        "reference_mode": mode,
        "destination_catalog_id": DESTINATION_CATALOG_ID,
        "context": context,
        "content_plan": _trim_plan(plan),
        "account_binding": _account_binding(account),
        "unresolved": _unresolved_bindings(spec, market, account),
    }
    if canary["product"]:
        manifest["product_application"] = _product_application(plan, CANARY_PRODUCT)
        manifest["product_qa_contract"] = _build_product_qa_contract(CANARY_PRODUCT)
    manifest["_fixture_note"] = (
        "Phase 4（VN 围巾跨市场）三种参考模式的离线 canary manifest。"
        "context/content_plan 由 shipped 配置确定性重建，由 "
        "tests/test_photo_vn_activation.py 的再生成测试钉住，不会漂移成与代码不符的文档。"
        "persona 为 fixture 占位标识；输入哈希由 fixture 文本派生，非真实图片哈希。"
        "unresolved 列出所有「已声明但尚未交付」的绑定，本 manifest 不代表可发布。"
    )
    return manifest


def build_all() -> dict[str, dict]:
    return {canary["canary_id"]: build_manifest(canary["canary_id"]) for canary in CANARIES}


def fixture_path(canary_id: str) -> Path:
    return TESTS_DIR / "fixtures" / f"PHASE4_CANARY_{canary_id}.json"


def dump(manifest: dict) -> str:
    return json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"


def write_all() -> list[Path]:
    written = []
    for canary_id, manifest in build_all().items():
        path = fixture_path(canary_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(dump(manifest), encoding="utf-8")
        written.append(path)
    return written


if __name__ == "__main__":
    for written in write_all():
        print(written)
