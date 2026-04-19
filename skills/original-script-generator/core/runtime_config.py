#!/usr/bin/env python3
"""
原创脚本生成器运行时配置。

用于在 OpenClaw 中持久化默认 LLM 线路。
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from core.llm_client import AUTO_ROUTE, PRIMARY_ROUTE, SUPPORTED_ROUTES


def _shared_data_dir() -> Path:
    root = os.environ.get("OPENCLAW_SHARED_DATA_DIR", str(Path.home() / ".openclaw" / "shared" / "data"))
    path = Path(root)
    path.mkdir(parents=True, exist_ok=True)
    return path


def default_runtime_config_path() -> Path:
    override = os.environ.get("ORIGINAL_SCRIPT_GENERATOR_CONFIG_PATH")
    if override:
        return Path(override)
    return _shared_data_dir() / "original_script_generator_config.json"


def _normalize_route(route: Optional[str]) -> str:
    normalized = str(route or AUTO_ROUTE).strip().lower()
    if normalized not in SUPPORTED_ROUTES:
        raise ValueError(f"不支持的 llm route: {route}")
    return normalized


def load_runtime_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    path = Path(config_path) if config_path else default_runtime_config_path()
    if not path.exists():
        return {"llm_route": PRIMARY_ROUTE}

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"llm_route": PRIMARY_ROUTE}

    route = payload.get("llm_route", AUTO_ROUTE)
    try:
        normalized_route = _normalize_route(route)
    except ValueError:
        normalized_route = AUTO_ROUTE

    return {
        "llm_route": normalized_route,
    }


def save_runtime_config(config: Dict[str, Any], config_path: Optional[Path] = None) -> Path:
    path = Path(config_path) if config_path else default_runtime_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = {
        "llm_route": _normalize_route(config.get("llm_route")),
    }
    path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def get_configured_llm_route(config_path: Optional[Path] = None) -> str:
    return load_runtime_config(config_path).get("llm_route", PRIMARY_ROUTE)


def set_configured_llm_route(route: str, config_path: Optional[Path] = None) -> Path:
    normalized_route = _normalize_route(route)
    return save_runtime_config({"llm_route": normalized_route}, config_path=config_path)


def resolve_llm_route(cli_route: Optional[str] = None, config_path: Optional[Path] = None) -> Tuple[str, str]:
    if cli_route:
        return _normalize_route(cli_route), "cli"
    return get_configured_llm_route(config_path=config_path), "config"
