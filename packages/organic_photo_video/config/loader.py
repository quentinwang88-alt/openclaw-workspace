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
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

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
MARKET_PACK_DIR = CONFIG_DIR / "market_packs"
THEME_DIR = CONFIG_DIR / "themes"
RENDER_PRESET_DIR = CONFIG_DIR / "render_presets"
RECIPE_DIR = CONFIG_DIR / "recipes"
PROFILE_DIR = CONFIG_DIR / "profiles"
LAYOUT_DIR = CONFIG_DIR / "layouts"
VARIANT_POLICY_DIR = CONFIG_DIR / "variant_policies"
EXAMPLES_DIR = CONFIG_DIR / "examples"


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
    return _load_validated(
        Path(path),
        contracts.validate_content_recipe_payload,
        ContentRecipe,
        "content recipe",
    )


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
        contracts.ensure_valid(validate_board_layout(payload), f"board layout {path.name}")
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
    market_packs: List[MarketPack] = field(default_factory=list)
    themes: List[ThemeCatalog] = field(default_factory=list)
    render_presets: List[RenderPreset] = field(default_factory=list)
    content_recipes: List[ContentRecipe] = field(default_factory=list)
    render_profiles: List[RenderProfile] = field(default_factory=list)
    quality_profiles: List[QualityProfile] = field(default_factory=list)
    board_layouts: List[dict] = field(default_factory=list)
    variant_policies: List[dict] = field(default_factory=list)
    # The example account is documentation, never seed data; it is loaded for
    # contract tests only and must not be upserted by seed scripts.
    account_example: Optional[AccountProfile] = None

    def summary_lines(self) -> List[str]:
        lines = [
            f"market_packs={len(self.market_packs)}",
            f"themes={len(self.themes)}",
            f"render_presets={len(self.render_presets)}",
            f"account_example={'yes' if self.account_example else 'no'}",
            f"board_layouts={len(self.board_layouts)}",
            f"variant_policies={len(self.variant_policies)}",
        ]
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
        market_packs=load_market_packs(config_dir / "market_packs"),
        themes=load_themes(config_dir / "themes"),
        render_presets=load_render_presets(config_dir / "render_presets"),
        content_recipes=load_content_recipes(config_dir / "recipes"),
        render_profiles=load_render_profiles(config_dir / "profiles"),
        quality_profiles=load_quality_profiles(config_dir / "profiles"),
        board_layouts=load_board_layouts(config_dir / "layouts"),
        variant_policies=load_variant_policies(config_dir / "variant_policies"),
    )
    examples = config_dir / "examples"
    if examples.is_dir():
        example_files = sorted(examples.glob("*.json"))
        if example_files:
            bundle.account_example = load_account_import_file(example_files[0])
    return bundle
