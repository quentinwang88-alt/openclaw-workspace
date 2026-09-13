"""Load and validate OPV versioned configuration files.

Shipped layout::

    config/
    ├── market_packs/    MP_*.json   (opv-market-pack-v1)
    ├── themes/          THEME_*.json(opv-theme-v1)
    ├── render_presets/  RP_*.json   (opv-render-preset-v1)
    └── examples/        account_profile_example.json (NOT seed data)

Every loader validates against ``domain.contracts`` before returning domain
models, so a malformed file fails fast at load time instead of at seed time.
"""

from __future__ import annotations

import json
import csv
import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from domain import contracts
from domain.models import (
    AccountProfile,
    ContentRecipe,
    MarketPack,
    QualityProfile,
    RenderProfile,
    RenderPreset,
    ThemeCatalog,
)

CONFIG_DIR = Path(__file__).resolve().parent
CATEGORY_DIR = CONFIG_DIR / "categories"
MARKET_PACK_DIR = CONFIG_DIR / "market_packs"
THEME_DIR = CONFIG_DIR / "themes"
RENDER_PRESET_DIR = CONFIG_DIR / "render_presets"
RECIPE_DIR = CONFIG_DIR / "recipes"
COPY_PACK_DIR = CONFIG_DIR / "copy_packs"
PROFILE_DIR = CONFIG_DIR / "profiles"
LAYOUT_DIR = CONFIG_DIR / "layouts"
VARIANT_POLICY_DIR = CONFIG_DIR / "variant_policies"
EXAMPLES_DIR = CONFIG_DIR / "examples"
# Country-agnostic layer (VN scarf cross-market, Phase 2).  Locale packs own
# every publish-language label; destination catalogs own semantic travel facts.
LOCALE_DIR = CONFIG_DIR / "locales"
DESTINATION_DIR = CONFIG_DIR / "destinations"

CATEGORY_PROFILE_SCHEMA = "opv-category-profile-v1"
PHOTO_LAYOUT_SCHEMA = "opv-photo-layout-v1"


class ConfigLoadError(ValueError):
    pass


def _load_json(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError as exc:
        raise ConfigLoadError(f"{path.name}: invalid JSON ({exc})") from exc
    if not isinstance(data, dict):
        raise ConfigLoadError(f"{path.name}: top level must be a JSON object")
    return data


def _load_validated(path: Path, validator, model_cls, label: str):
    payload = _load_json(path)
    errors = validator(payload)
    contracts.ensure_valid(errors, f"{label} {path.name}")
    return model_cls.from_payload(payload)


def validate_category_profile(payload: Mapping[str, Any]) -> List[str]:
    """Validate the small, config-only category profile used by photo recipes."""
    errors: List[str] = []
    if payload.get("schema_version") != CATEGORY_PROFILE_SCHEMA:
        errors.append(f"schema_version must be {CATEGORY_PROFILE_SCHEMA}")
    for key in ("category_key", "category_name", "status"):
        if not isinstance(payload.get(key), str) or not payload[key].strip():
            errors.append(f"{key} must be a non-empty string")
    if payload.get("status") not in {"draft", "active", "deprecated"}:
        errors.append("status must be draft, active, or deprecated")
    for key in ("interest_drivers", "visual_dimensions", "asset_requirements"):
        if not isinstance(payload.get(key), list) or not payload[key]:
            errors.append(f"{key} must be a non-empty list")
        elif any(not isinstance(item, str) or not item.strip() for item in payload[key]):
            errors.append(f"{key} entries must be non-empty strings")
    rules = payload.get("content_rules")
    if not isinstance(rules, dict) or not rules:
        errors.append("content_rules must be a non-empty object")
    return errors


def validate_photo_layout(payload: Mapping[str, Any]) -> List[str]:
    """Validate deterministic native-photo geometry without weakening board layouts."""
    errors: List[str] = []
    if payload.get("schema_version") == "opv-photo-layout-v2":
        from services.photo_package import normalize_photo_template
        try:
            normalized = normalize_photo_template(payload)
            if not normalized.get("template_id") or type(normalized.get("template_version")) is not int or normalized["template_version"] < 1:
                errors.append("photo layout requires id and positive version")
            if (normalized.get("width"), normalized.get("height")) != (1080, 1920):
                errors.append("photo layout v2 requires 1080x1920 canvas")
            if payload.get("status") not in {"draft", "active", "deprecated"}:
                errors.append("invalid photo layout status")
        except (ValueError, RuntimeError, KeyError, TypeError) as exc:
            errors.append(str(exc))
        return errors
    if payload.get("schema_version") != PHOTO_LAYOUT_SCHEMA:
        errors.append(f"schema_version must be {PHOTO_LAYOUT_SCHEMA}")
    for key in ("layout_id", "layout_kind", "status"):
        if not isinstance(payload.get(key), str) or not payload[key].strip():
            errors.append(f"{key} must be a non-empty string")
    if payload.get("layout_kind") not in {
        "SINGLE_LIGHT_TEXT", "COMPARISON", "CHOICE_GRID",
    }:
        errors.append("layout_kind must be SINGLE_LIGHT_TEXT, COMPARISON, or CHOICE_GRID")
    if payload.get("status") not in {"draft", "active", "deprecated"}:
        errors.append("status must be draft, active, or deprecated")
    version = payload.get("layout_version")
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        errors.append("layout_version must be an integer >= 1")

    canvas = payload.get("canvas")
    if not isinstance(canvas, dict):
        errors.append("canvas must be an object")
        width = height = 0
    else:
        width, height = canvas.get("width"), canvas.get("height")
        if not isinstance(width, int) or not isinstance(height, int) or width < 896 or height < 1593:
            errors.append("canvas must be at least 896x1593")
            width = height = 0
        elif abs((width / height) - (9 / 16)) > 0.02:
            errors.append("canvas must be 9:16 portrait")

    variants = payload.get("page_variants")
    if not isinstance(variants, dict) or not variants:
        errors.append("page_variants must be a non-empty object")
        return errors
    for variant_name, variant in variants.items():
        regions = variant.get("regions") if isinstance(variant, dict) else None
        if not isinstance(regions, list) or not regions:
            errors.append(f"page variant {variant_name} regions must be a non-empty list")
            continue
        ids = set()
        kinds = set()
        for region in regions:
            if not isinstance(region, dict):
                errors.append(f"page variant {variant_name} region must be an object")
                continue
            region_id = region.get("id")
            if not isinstance(region_id, str) or not region_id.strip():
                errors.append(f"page variant {variant_name} region id must be a non-empty string")
            elif region_id in ids:
                errors.append(f"page variant {variant_name} duplicates region id {region_id}")
            else:
                ids.add(region_id)
            kind = region.get("kind")
            if kind not in {"image", "text", "decor"}:
                errors.append(f"page variant {variant_name} region kind is invalid")
            else:
                kinds.add(kind)
            rect = region.get("rect")
            if not isinstance(rect, dict):
                errors.append(f"page variant {variant_name} region {region_id} rect must be an object")
                continue
            values = [rect.get(key) for key in ("x", "y", "width", "height")]
            if any(not isinstance(value, int) or isinstance(value, bool) for value in values):
                errors.append(f"page variant {variant_name} region {region_id} rect must use integers")
                continue
            x, y, region_width, region_height = values
            if x < 0 or y < 0 or region_width <= 0 or region_height <= 0:
                errors.append(f"page variant {variant_name} region {region_id} rect is invalid")
            elif width and height and (x + region_width > width or y + region_height > height):
                errors.append(f"page variant {variant_name} region {region_id} exceeds canvas")
        if "image" not in kinds or "text" not in kinds:
            errors.append(f"page variant {variant_name} needs image and text regions")
    return errors


def load_category_file(path: Path) -> Dict[str, Any]:
    payload = _load_json(Path(path))
    contracts.ensure_valid(validate_category_profile(payload), f"category profile {Path(path).name}")
    return payload


def load_categories(directory: Path = CATEGORY_DIR) -> List[Dict[str, Any]]:
    return [load_category_file(path) for path in sorted(directory.glob("*.json"))]


def load_locale_pack_file(path: Path) -> Dict[str, Any]:
    payload = _load_json(Path(path))
    contracts.ensure_valid(
        contracts.validate_locale_pack_payload(payload), f"locale pack {Path(path).name}"
    )
    return payload


def load_locale_packs(directory: Path = LOCALE_DIR) -> List[Dict[str, Any]]:
    return [load_locale_pack_file(path) for path in sorted(directory.glob("*.json"))]


def load_destination_catalog_file(path: Path) -> Dict[str, Any]:
    payload = _load_json(Path(path))
    contracts.ensure_valid(
        contracts.validate_destination_catalog_payload(payload),
        f"destination catalog {Path(path).name}",
    )
    return payload


def load_destination_catalogs(directory: Path = DESTINATION_DIR) -> List[Dict[str, Any]]:
    return [load_destination_catalog_file(path) for path in sorted(directory.glob("*.json"))]


def load_market_pack_file(path: Path) -> MarketPack:
    return _load_validated(
        Path(path), contracts.validate_market_pack_payload, MarketPack, "market pack"
    )


def load_theme_file(path: Path) -> ThemeCatalog:
    return _load_validated(Path(path), contracts.validate_theme_payload, ThemeCatalog, "theme")


def load_render_preset_file(path: Path) -> RenderPreset:
    return _load_validated(
        Path(path),
        contracts.validate_render_preset_payload,
        RenderPreset,
        "render preset",
    )


def load_content_recipe_file(path: Path) -> ContentRecipe:
    recipe_path = Path(path)
    payload = _load_json(recipe_path)
    spec = payload.get("recipe_spec") or payload.get("recipe_spec_json") or {}
    if isinstance(spec, dict):
        is_v2 = spec.get("schema_version") == contracts.PHOTO_RECIPE_V2_SCHEMA_VERSION
        locale_copy_packs = dict(spec.get("locale_copy_packs") or {}) if is_v2 else {}
        profiles = spec.get("execution_profiles") or []
        for profile in profiles:
            if not isinstance(profile, dict) or profile.get("copy_variants"):
                continue
            if is_v2:
                # A country-agnostic profile must not pin one locale's copy pack:
                # ``locale_copy_packs`` on the spec owns that binding.
                if str(profile.get("copy_pack_id") or "").strip():
                    continue
                variants_by_locale: Dict[str, Any] = {}
                for locale, locale_pack_id in locale_copy_packs.items():
                    pack_path = (
                        recipe_path.parent.parent / "copy_packs" / f"{locale_pack_id}.tsv"
                    )
                    variants = load_photo_copy_pack(
                        pack_path,
                        expected_recipe_id=str(payload.get("recipe_id") or ""),
                        expected_profile_id=str(profile.get("profile_id") or ""),
                    )
                    variants_by_locale[str(locale)] = {
                        "copy_pack_id": str(locale_pack_id),
                        "copy_variants": variants,
                    }
                if not variants_by_locale:
                    continue
                default_locale = sorted(variants_by_locale)[0]
                profile["copy_variants_by_locale"] = variants_by_locale
                profile["copy_variants"] = variants_by_locale[default_locale]["copy_variants"]
                continue
            pack_id = str(profile.get("copy_pack_id") or "").strip()
            if not pack_id:
                continue
            pack_path = recipe_path.parent.parent / "copy_packs" / f"{pack_id}.tsv"
            profile["copy_variants"] = load_photo_copy_pack(
                pack_path,
                expected_recipe_id=str(payload.get("recipe_id") or ""),
                expected_profile_id=str(profile.get("profile_id") or ""),
            )
    errors = contracts.validate_content_recipe_payload(payload)
    contracts.ensure_valid(errors, f"content recipe {recipe_path.name}")
    return ContentRecipe.from_payload(payload)


def _decode_copy_cell(value: str) -> str:
    return str(value or "").replace("\\n", "\n").strip()


def load_photo_copy_pack(
    path: Path, *, expected_recipe_id: str, expected_profile_id: str,
) -> List[Dict[str, Any]]:
    """Load an operator-friendly TSV copy pack and return recipe variants."""
    pack_path = Path(path)
    if not pack_path.is_file():
        raise ConfigLoadError(f"copy pack missing: {pack_path.name}")
    with pack_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    variants: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for line_no, row in enumerate(rows, 2):
        if str(row.get("status") or "active").strip().lower() != "active":
            continue
        recipe_id = str(row.get("recipe_id") or "").strip()
        profile_id = str(row.get("profile_id") or "").strip()
        if recipe_id != expected_recipe_id or profile_id != expected_profile_id:
            continue
        copy_id = str(row.get("copy_id") or "").strip()
        if not copy_id or copy_id in seen:
            raise ConfigLoadError(f"{pack_path.name}:{line_no}: copy_id must be unique")
        seen.add(copy_id)
        hashtags = [item.strip() for item in str(row.get("hashtags") or "").split("|") if item.strip()]
        copy_block = {
            "title": _decode_copy_cell(row.get("title") or ""),
            "caption": _decode_copy_cell(row.get("caption") or ""),
            "hashtags": hashtags,
            "slide_texts": [
                _decode_copy_cell(row.get(f"slide_{index}") or "")
                for index in range(1, 6)
            ],
            "language_review_status": str(
                row.get("language_review_status") or "production_copy_pack"
            ).strip(),
        }
        if copy_block["language_review_status"] == "NATIVE_APPROVED":
            from services.photo_copy_review import (
                PhotoCopyReviewError, validate_native_approval,
            )
            try:
                copy_block["language_review"] = validate_native_approval(row)
            except PhotoCopyReviewError as exc:
                raise ConfigLoadError(f"{pack_path.name}:{line_no}: {exc}") from exc
        from domain.photo_contracts import validate_copy
        copy_errors = validate_copy(copy_block, allow_placeholders=True)
        if copy_errors:
            raise ConfigLoadError(
                f"{pack_path.name}:{line_no}: " + "; ".join(copy_errors)
            )
        variants.append({"copy_id": copy_id, "copy": copy_block})
    if not variants:
        raise ConfigLoadError(
            f"{pack_path.name}: no active rows for {expected_recipe_id}/{expected_profile_id}"
        )
    return copy.deepcopy(variants)


def load_render_profile_file(path: Path) -> RenderProfile:
    return _load_validated(
        Path(path),
        contracts.validate_render_profile_payload,
        RenderProfile,
        "render profile",
    )


def load_quality_profile_file(path: Path) -> QualityProfile:
    return _load_validated(
        Path(path),
        contracts.validate_quality_profile_payload,
        QualityProfile,
        "quality profile",
    )


def load_content_recipes(directory: Path = RECIPE_DIR) -> List[ContentRecipe]:
    return [load_content_recipe_file(p) for p in sorted(directory.glob("*.json"))]


def load_render_profiles(directory: Path = PROFILE_DIR) -> List[RenderProfile]:
    return [load_render_profile_file(p) for p in sorted(directory.glob("IMAGE_*.json"))]


def load_quality_profiles(directory: Path = PROFILE_DIR) -> List[QualityProfile]:
    return [load_quality_profile_file(p) for p in sorted(directory.glob("QUALITY_*.json"))]


def load_board_layouts(directory: Path = LAYOUT_DIR) -> List[dict]:
    from services.board_layout import validate_board_layout

    output = []
    for path in sorted(directory.glob("*.json")):
        payload = _load_json(path)
        if payload.get("schema_version") in {PHOTO_LAYOUT_SCHEMA, "opv-photo-layout-v2"}:
            errors = validate_photo_layout(payload)
            label = "photo layout"
        else:
            errors = validate_board_layout(payload)
            label = "board layout"
        contracts.ensure_valid(errors, f"{label} {path.name}")
        output.append(payload)
    return output


def load_variant_policies(directory: Path = VARIANT_POLICY_DIR) -> List[dict]:
    from services.board_layout import validate_variant_policy

    output = []
    for path in sorted(directory.glob("*.json")):
        payload = _load_json(path)
        contracts.ensure_valid(validate_variant_policy(payload), f"variant policy {path.name}")
        output.append(payload)
    return output


def load_account_import_file(path: Path) -> AccountProfile:
    return _load_validated(
        Path(path),
        contracts.validate_account_profile_payload,
        AccountProfile,
        "account profile import",
    )


def load_market_packs(directory: Path = MARKET_PACK_DIR) -> List[MarketPack]:
    return [load_market_pack_file(p) for p in sorted(directory.glob("*.json"))]


def load_themes(directory: Path = THEME_DIR) -> List[ThemeCatalog]:
    return [load_theme_file(p) for p in sorted(directory.glob("*.json"))]


def load_render_presets(directory: Path = RENDER_PRESET_DIR) -> List[RenderPreset]:
    return [load_render_preset_file(p) for p in sorted(directory.glob("*.json"))]


@dataclass
class SeedBundle:
    categories: List[Dict[str, Any]] = field(default_factory=list)
    market_packs: List[MarketPack] = field(default_factory=list)
    themes: List[ThemeCatalog] = field(default_factory=list)
    render_presets: List[RenderPreset] = field(default_factory=list)
    content_recipes: List[ContentRecipe] = field(default_factory=list)
    render_profiles: List[RenderProfile] = field(default_factory=list)
    quality_profiles: List[QualityProfile] = field(default_factory=list)
    board_layouts: List[dict] = field(default_factory=list)
    variant_policies: List[dict] = field(default_factory=list)
    locale_packs: List[Dict[str, Any]] = field(default_factory=list)
    destination_catalogs: List[Dict[str, Any]] = field(default_factory=list)
    # The example account is documentation, never seed data; it is loaded for
    # contract tests only and must not be upserted by seed scripts.
    account_example: Optional[AccountProfile] = None

    def summary_lines(self) -> List[str]:
        lines = [
            f"categories={len(self.categories)}",
            f"market_packs={len(self.market_packs)}",
            f"themes={len(self.themes)}",
            f"render_presets={len(self.render_presets)}",
            f"account_example={'yes' if self.account_example else 'no'}",
            f"board_layouts={len(self.board_layouts)}",
            f"variant_policies={len(self.variant_policies)}",
            f"locale_packs={len(self.locale_packs)}",
            f"destination_catalogs={len(self.destination_catalogs)}",
        ]
        for category in self.categories:
            lines.append(f"category {category['category_key']} ({category['status']})")
        for pack in self.market_packs:
            lines.append(f"pack {pack.market_pack_id} v{pack.pack_version} ({pack.status})")
        for theme in self.themes:
            lines.append(f"theme {theme.theme_id} v{theme.theme_version} ({theme.status})")
        for preset in self.render_presets:
            lines.append(
                f"preset {preset.render_preset_id} v{preset.preset_version} ({preset.status})"
            )
        for recipe in self.content_recipes:
            lines.append(
                f"recipe {recipe.recipe_id} v{recipe.recipe_version} ({recipe.status})"
            )
        for profile in self.render_profiles:
            lines.append(f"render_profile {profile.render_profile_id} ({profile.status})")
        for profile in self.quality_profiles:
            lines.append(f"quality_profile {profile.quality_profile_id} ({profile.status})")
        return lines


def load_seed_bundle(config_dir: Path = CONFIG_DIR) -> SeedBundle:
    bundle = SeedBundle(
        categories=load_categories(config_dir / "categories"),
        market_packs=load_market_packs(config_dir / "market_packs"),
        themes=load_themes(config_dir / "themes"),
        render_presets=load_render_presets(config_dir / "render_presets"),
        content_recipes=load_content_recipes(config_dir / "recipes"),
        render_profiles=load_render_profiles(config_dir / "profiles"),
        quality_profiles=load_quality_profiles(config_dir / "profiles"),
        board_layouts=load_board_layouts(config_dir / "layouts"),
        variant_policies=load_variant_policies(config_dir / "variant_policies"),
        locale_packs=load_locale_packs(config_dir / "locales"),
        destination_catalogs=load_destination_catalogs(config_dir / "destinations"),
    )
    examples = config_dir / "examples"
    if examples.is_dir():
        example_files = sorted(examples.glob("*.json"))
        if example_files:
            bundle.account_example = load_account_import_file(example_files[0])
    return bundle
