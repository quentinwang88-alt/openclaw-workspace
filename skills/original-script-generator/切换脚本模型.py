#!/usr/bin/env python3
"""
中文别名入口：切换 original-script-generator 的默认 LLM 线路。
"""

import argparse
import sys
from pathlib import Path


SKILL_DIR = Path(__file__).parent.absolute()
sys.path.insert(0, str(SKILL_DIR))

from core.runtime_config import (  # noqa: E402
    default_runtime_config_path,
    get_configured_llm_route,
    set_configured_llm_route,
)


ROUTE_ALIASES = {
    "自动": "auto",
    "自动切换": "auto",
    "主线": "primary",
    "主线路": "primary",
    "默认主线": "primary",
    "备线": "backup",
    "备用": "backup",
    "备用线路": "backup",
    "Gemini": "gemini",
    "gemini": "gemini",
    "谷歌": "gemini",
    "谷歌线": "gemini",
    "Gemini线": "gemini",
    "Gemini线路": "gemini",
}


def _normalize_route(route_text: str) -> str:
    normalized = str(route_text or "").strip()
    if not normalized:
        raise ValueError("缺少线路参数")
    if normalized in ROUTE_ALIASES:
        return ROUTE_ALIASES[normalized]
    raise ValueError(f"不支持的中文线路别名: {route_text}")


def main() -> None:
    parser = argparse.ArgumentParser(description="切换原创脚本生成器默认模型线路")
    parser.add_argument("线路", nargs="?", help="可选：自动 / 主线 / 备线 / Gemini；不传则查看当前默认线路")
    args = parser.parse_args()

    config_path = default_runtime_config_path()

    if not args.线路:
        current = get_configured_llm_route(config_path=config_path)
        print(f"当前默认模型线路: {current}")
        print(f"配置文件: {config_path}")
        return

    route = _normalize_route(args.线路)
    set_configured_llm_route(route, config_path=config_path)
    print(f"已切换默认模型线路为: {args.线路} ({route})")
    print(f"配置文件: {config_path}")


if __name__ == "__main__":
    main()
