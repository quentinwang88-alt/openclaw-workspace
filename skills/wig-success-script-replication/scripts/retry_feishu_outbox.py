#!/usr/bin/env python3
"""Retry only persisted Feishu outbox rows; never rerun model work."""

from __future__ import annotations

import argparse
from typing import Iterable

from _runtime import RuntimeBridgeError, invoke, invoke_preview, load_application, print_result


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retry wig replication Feishu outbox")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    if not 1 <= args.limit <= 200:
        print("error: limit must be between 1 and 200")
        return 2
    try:
        app = load_application()
        if args.dry_run:
            result = invoke_preview(
                app,
                ("preview_feishu_outbox", "preview_outbox"),
                "retry_feishu_outbox",
                limit=args.limit,
                dry_run=True,
            )
        else:
            result = invoke(
                app,
                ("retry_feishu_outbox", "retry_outbox"),
                limit=args.limit,
            )
        print_result(result)
        return 0
    except RuntimeBridgeError as exc:
        print(f"error: {exc}")
        return 2
    except Exception as exc:
        print(f"outbox retry failed: {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
