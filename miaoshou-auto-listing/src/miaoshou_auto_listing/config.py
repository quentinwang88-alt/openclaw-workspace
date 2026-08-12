from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

from .models import AppConfig


CONFIG_FILES = {
    "markets": "markets.yaml",
    "shops": "shops.yaml",
    "pricing_rules": "pricing.yaml",
    "stock_rules": "stock.yaml",
    "warehouse": "warehouse.yaml",
    "logistics_profiles": "logistics.yaml",
    "retry": "retry.yaml",
    "feishu": "feishu.yaml",
}


def _read_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected a YAML mapping in {path}")
    return data


def load_config(config_dir: Path) -> AppConfig:
    config_dir = config_dir.resolve()
    app_data = _read_yaml(config_dir / "app.yaml")
    sections: Dict[str, Any] = {}
    for model_field, filename in CONFIG_FILES.items():
        data = _read_yaml(config_dir / filename)
        sections[model_field] = data.get(model_field, data)
    browser = dict(app_data.get("browser", {}))
    profile_dir = Path(browser["profile_dir"]).expanduser()
    if not profile_dir.is_absolute():
        profile_dir = (config_dir / profile_dir).resolve()
    browser["profile_dir"] = profile_dir
    return AppConfig(browser=browser, **sections)


def validate_task_config(task: Any, config: AppConfig) -> None:
    checks = (
        (task.market, config.markets, "market"),
        (task.target_shop, config.shops, "target_shop"),
        (task.pricing_rule_id, config.pricing_rules, "pricing_rule_id"),
        (task.market, config.warehouse, "warehouse market"),
    )
    for key, mapping, label in checks:
        if key not in mapping:
            raise ValueError(f"Unknown {label}: {key}")
    if task.category_group != "AUTO":
        for mapping, label in (
            (config.stock_rules, "stock category_group"),
            (config.logistics_profiles, "logistics category_group"),
        ):
            if task.category_group not in mapping:
                raise ValueError(f"Unknown {label}: {task.category_group}")
    shop_market = str(config.shops[task.target_shop].get("market", "")).upper()
    if shop_market and shop_market != task.market:
        raise ValueError(
            f"Shop {task.target_shop} belongs to {shop_market}, not {task.market}"
        )
