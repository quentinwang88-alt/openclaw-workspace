#!/usr/bin/env python3
"""Load configurable market insight taxonomies."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

from src.models import ConfigError


class MarketInsightTaxonomyLoader(object):
    def __init__(self, taxonomy_dir: Path):
        self.taxonomy_dir = Path(taxonomy_dir)
        self._cache: Dict[str, Dict[str, object]] = {}

    def load(self, category: str) -> Dict[str, object]:
        normalized = str(category or "").strip()
        if not normalized:
            raise ConfigError("taxonomy category 不能为空")
        if normalized not in self._cache:
            file_path = self.taxonomy_dir / "{category}_v1.json".format(category=normalized)
            if not file_path.exists():
                raise ConfigError("未找到 taxonomy 文件: {path}".format(path=file_path))
            payload = json.loads(file_path.read_text(encoding="utf-8"))
            self._validate(payload, file_path)
            self._cache[normalized] = payload
        return self._cache[normalized]

    def _validate(self, payload, file_path: Path) -> None:
        if "value_points" not in payload and "buying_motives" in payload:
            payload["value_points"] = list(payload.get("buying_motives") or [])
        if "style_cluster" not in payload and "style_tag_main" in payload:
            payload["style_cluster"] = list(payload.get("style_tag_main") or [])
        if "style_tag_main" not in payload and "style_cluster" in payload:
            payload["style_tag_main"] = list(payload.get("style_cluster") or [])
        if "product_form" not in payload and "product_form_or_result" in payload:
            payload["product_form"] = list(payload.get("product_form_or_result") or [])
        if "product_form_or_result" not in payload and "product_form" in payload:
            payload["product_form_or_result"] = list(payload.get("product_form") or [])
        if "product_form" not in payload:
            payload["product_form"] = ["other"]
        if "product_form_or_result" not in payload:
            payload["product_form_or_result"] = list(payload.get("product_form") or ["other"])
        if "length_form" not in payload:
            payload["length_form"] = ["other"]
        if "direction_family_map" not in payload or not isinstance(payload.get("direction_family_map"), dict):
            payload["direction_family_map"] = {}

        for key in ("style_cluster", "product_form", "length_form", "element_tags", "value_points", "scene_tags"):
            value = payload.get(key)
            if not isinstance(value, list) or not value:
                raise ConfigError("{path}: taxonomy.{key} 必须是非空数组".format(path=file_path, key=key))
