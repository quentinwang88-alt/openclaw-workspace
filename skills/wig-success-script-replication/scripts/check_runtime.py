#!/usr/bin/env python3
"""Read-only runtime diagnostics for the wig replication domain package."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Iterable

from _runtime import PACKAGES_ROOT, RuntimeBridgeError, invoke, load_application, print_result


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check wig replication runtime")
    parser.add_argument(
        "--imports-only",
        action="store_true",
        help="only verify the package and application facade import",
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    package_spec = importlib.util.find_spec("wig_success_replication")
    report = {
        "packages_root": str(PACKAGES_ROOT),
        "package_found": package_spec is not None,
        "python": sys.executable,
        "mode": "read-only",
    }
    if package_spec is None:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 2
    try:
        app = load_application()
        report["application"] = f"{type(app).__module__}.{type(app).__name__}"
        if args.imports_only:
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 0
        details = invoke(app, ("check_runtime", "healthcheck", "diagnose"), read_only=True)
        report["checks"] = details
        print_result(report)
        if isinstance(details, dict) and details.get("ok") is False:
            return 1
        return 0
    except RuntimeBridgeError as exc:
        report["error"] = str(exc)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 2
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
