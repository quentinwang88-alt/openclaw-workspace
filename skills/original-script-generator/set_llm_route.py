#!/usr/bin/env python3
"""
设置 original-script-generator 的默认 LLM 线路。
"""

import argparse
import sys
from pathlib import Path


SKILL_DIR = Path(__file__).parent.absolute()
sys.path.insert(0, str(SKILL_DIR))

from core.llm_client import AUTO_ROUTE, BACKUP_ROUTE, GEMINI_ROUTE, PRIMARY_ROUTE  # noqa: E402
from core.runtime_config import (  # noqa: E402
    default_runtime_config_path,
    get_configured_llm_route,
    set_configured_llm_route,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="设置 original-script-generator 默认 LLM 线路")
    parser.add_argument(
        "route",
        nargs="?",
        choices=[AUTO_ROUTE, PRIMARY_ROUTE, BACKUP_ROUTE, GEMINI_ROUTE],
        help="要设置的默认线路：auto / primary / backup / gemini；不传则只查看当前默认值",
    )
    args = parser.parse_args()

    config_path = default_runtime_config_path()

    if not args.route:
        current = get_configured_llm_route(config_path=config_path)
        print(f"当前默认 LLM 线路: {current}")
        print(f"配置文件: {config_path}")
        return

    set_configured_llm_route(args.route, config_path=config_path)
    print(f"已切换默认 LLM 线路为: {args.route}")
    print(f"配置文件: {config_path}")


if __name__ == "__main__":
    main()
