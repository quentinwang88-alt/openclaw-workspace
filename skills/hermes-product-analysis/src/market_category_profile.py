#!/usr/bin/env python3
"""Market x category profile loading and validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List

import yaml

from src.models import ConfigError


@dataclass(frozen=True)
class MarketCategoryProfile:
    market_id: str
    market_display_name: str
    category_id: str
    profile_version: str
    status: str = "ready"
    enabled: bool = True
    language: str = ""
    currency: str = ""
    timezone: str = ""
    local_context: List[str] = field(default_factory=list)
    profile_path: str = ""
    base_category_path: str = ""
    directions: Dict[str, Any] = field(default_factory=dict)
    product_anchor_schema: Dict[str, Any] = field(default_factory=dict)
    scoring: Dict[str, Any] = field(default_factory=dict)
    tag_dictionary: Dict[str, Any] = field(default_factory=dict)
    market_overrides: Dict[str, Any] = field(default_factory=dict)
    prompts: Dict[str, Any] = field(default_factory=dict)

    def canonical_direction_id(self, direction_slug: str) -> str:
        slug = str(direction_slug or "").strip()
        if not slug:
            raise ConfigError("direction_slug 不能为空")
        return f"{self.market_id}__{self.category_id}__{slug}"


class MarketCategoryProfileLoader(object):
    def __init__(self, skill_dir: Path):
        self.skill_dir = Path(skill_dir)

    def load_market_category_profile(self, market_id: str, category_id: str) -> MarketCategoryProfile:
        return _load_market_category_profile_cached(str(self.skill_dir), market_id, category_id)


@lru_cache(maxsize=32)
def _load_market_category_profile_cached(skill_dir_text: str, market_id: str, category_id: str) -> MarketCategoryProfile:
    skill_dir = Path(skill_dir_text)
    market_id = str(market_id or "").strip().upper()
    category_id = _normalize_category_id(category_id)
    if not market_id:
        raise ConfigError("market_id 不能为空")
    if not category_id:
        raise ConfigError("category_id 不能为空")

    market_registry = _read_yaml(skill_dir / "configs" / "market_registry.yaml")
    markets = dict(market_registry.get("markets") or {})
    if market_id not in markets:
        raise ConfigError("未知 market_id: {market_id}".format(market_id=market_id))
    market_payload = dict(markets[market_id] or {})

    profile_registry = _read_yaml(skill_dir / "configs" / "market_category_profiles.yaml")
    profiles = dict(profile_registry.get("market_category_profiles") or {})
    market_profiles = dict(profiles.get(market_id) or {})
    profile_entry = dict(market_profiles.get(category_id) or {})
    if not profile_entry:
        raise ConfigError("未注册 market/category profile: {market_id}/{category_id}".format(market_id=market_id, category_id=category_id))
    if not bool(profile_entry.get("enabled", False)):
        raise ConfigError("market/category profile 未启用: {market_id}/{category_id}".format(market_id=market_id, category_id=category_id))

    profile_path = _resolve_path(skill_dir, str(profile_entry.get("profile_path") or ""))
    profile_payload = _read_yaml(profile_path)
    if str(profile_payload.get("market_id") or "").upper() != market_id:
        raise ConfigError("{path}: profile.market_id 与注册表不一致".format(path=profile_path))
    if _normalize_category_id(profile_payload.get("category_id")) != category_id:
        raise ConfigError("{path}: profile.category_id 与注册表不一致".format(path=profile_path))

    base_category_path = _resolve_path(skill_dir, str(profile_payload.get("base_category_path") or ""))
    directions = _read_yaml(base_category_path / "directions.yaml")
    anchor_schema = _read_yaml(base_category_path / "product_anchor_schema.yaml")
    scoring = _read_yaml(base_category_path / "scoring.yaml")
    tag_dictionary = _read_optional_yaml(skill_dir, str(profile_payload.get("tag_dictionary_path") or ""))
    market_overrides = _read_optional_yaml(skill_dir, str(profile_payload.get("market_overrides_path") or ""))
    prompts = _read_optional_yaml(skill_dir, str(profile_payload.get("prompts_path") or ""))

    _validate_base_category(category_id, directions, anchor_schema, scoring)
    _validate_tag_dictionary(tag_dictionary, category_id)

    return MarketCategoryProfile(
        market_id=market_id,
        market_display_name=str(market_payload.get("display_name") or market_id),
        category_id=category_id,
        profile_version=str(profile_payload.get("profile_version") or f"{market_id}__{category_id}__v1"),
        status=str(profile_payload.get("status") or directions.get("status") or scoring.get("status") or "ready"),
        enabled=bool(profile_payload.get("enabled", True)),
        language=str(market_payload.get("language") or ""),
        currency=str(market_payload.get("currency") or ""),
        timezone=str(market_payload.get("default_timezone") or ""),
        local_context=list(market_payload.get("local_context") or []),
        profile_path=str(profile_path),
        base_category_path=str(base_category_path),
        directions=directions,
        product_anchor_schema=anchor_schema,
        scoring=scoring,
        tag_dictionary=tag_dictionary,
        market_overrides=market_overrides,
        prompts=prompts,
    )


def load_market_category_profile(market_id: str, category_id: str, skill_dir: Path | None = None) -> MarketCategoryProfile:
    root = Path(skill_dir or Path(__file__).resolve().parents[1])
    return _load_market_category_profile_cached(str(root), market_id, category_id)


def _normalize_category_id(value: Any) -> str:
    raw = str(value or "").strip()
    aliases = {
        "light_tops": "womens_tops",
        "轻上装": "womens_tops",
        "女装上装": "womens_tops",
        "发饰": "hair_accessory",
        "耳环": "earrings",
        "耳饰": "earrings",
    }
    return aliases.get(raw, raw)


def _resolve_path(skill_dir: Path, path_text: str) -> Path:
    if not path_text:
        raise ConfigError("配置路径不能为空")
    path = Path(path_text)
    if not path.is_absolute():
        path = skill_dir / path
    if not path.exists():
        raise ConfigError("配置路径不存在: {path}".format(path=path))
    return path


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ConfigError("配置文件不存在: {path}".format(path=path))
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ConfigError("{path}: YAML 根节点必须是对象".format(path=path))
    return payload


def _read_optional_yaml(skill_dir: Path, path_text: str) -> Dict[str, Any]:
    if not path_text:
        return {}
    return _read_yaml(_resolve_path(skill_dir, path_text))


def _validate_base_category(category_id: str, directions: Dict[str, Any], anchor_schema: Dict[str, Any], scoring: Dict[str, Any]) -> None:
    if _normalize_category_id(directions.get("category_id")) != category_id:
        raise ConfigError("directions.yaml category_id 与 profile 不一致: {category_id}".format(category_id=category_id))
    direction_map = dict(directions.get("directions") or {})
    if not direction_map:
        raise ConfigError("{category_id}: directions 不能为空".format(category_id=category_id))
    for slug, direction in direction_map.items():
        if category_id != "womens_tops" and category_id not in str(slug):
            raise ConfigError("{category_id}: direction slug 必须包含类目前缀: {slug}".format(category_id=category_id, slug=slug))
        if dict(direction or {}).get("status") == "not_ready":
            continue
        if not dict(direction or {}).get("direction_name"):
            raise ConfigError("{category_id}: direction 缺少 direction_name: {slug}".format(category_id=category_id, slug=slug))

    schema = dict(anchor_schema.get("product_anchor_schema") or {})
    if category_id == "earrings":
        for required in ("product_form", "wearing_type", "length_class"):
            if required not in schema:
                raise ConfigError("earrings product_anchor_schema 缺少字段: {field}".format(field=required))
        quality_weights = dict(scoring.get("product_quality_subscore_weights") or {})
        if sum(float(value or 0) for value in quality_weights.values()) != 35:
            raise ConfigError("earrings product_quality_subscore_weights 加和必须等于 35")
        penalty_application = dict(scoring.get("risk_penalty_application") or {})
        if penalty_application.get("apply_to") != "total_score":
            raise ConfigError("earrings risk_penalty_application.apply_to 必须为 total_score")
        if float(penalty_application.get("cap_per_product") or 0) > -1:
            raise ConfigError("earrings cap_per_product 必须是负向扣分")
        if not dict(scoring.get("risk_penalties") or {}):
            raise ConfigError("earrings risk_penalties 不能为空")
    if category_id == "womens_tops":
        statuses = {str((direction or {}).get("status") or "") for direction in direction_map.values()}
        if "not_ready" not in statuses:
            raise ConfigError("womens_tops 当前必须保留 not_ready placeholder")


def _validate_tag_dictionary(tag_dictionary: Dict[str, Any], category_id: str) -> None:
    for key in ("forms", "style_keywords", "scenes", "negative_keywords", "unique_keywords"):
        value = tag_dictionary.get(key)
        if not isinstance(value, dict):
            raise ConfigError("{category_id}: tag_dictionary.{key} 必须是对象".format(category_id=category_id, key=key))
        for lang_key in ("zh", "local", "en"):
            if lang_key not in value or not isinstance(value.get(lang_key), list):
                raise ConfigError("{category_id}: tag_dictionary.{key}.{lang_key} 必须是数组".format(category_id=category_id, key=key, lang_key=lang_key))
