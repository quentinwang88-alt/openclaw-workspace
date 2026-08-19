#!/usr/bin/env python3
"""Run V1 Lite row actions through the domain application facade."""

from __future__ import annotations

import argparse
import re
from typing import Iterable

from _runtime import RuntimeBridgeError, invoke, invoke_preview, load_application, print_result


RECORD_ID_RE = re.compile(r"^rec[A-Za-z0-9]+$")
ACTIONS = ("all", "one-click", "process-mother", "confirm-mother", "generate")


def _record_id(value: str) -> str:
    if not RECORD_ID_RE.fullmatch(value or ""):
        raise argparse.ArgumentTypeError("record id must start with rec and be alphanumeric")
    return value


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run pending wig replication actions")
    parser.add_argument("--action", choices=ACTIONS, default="all")
    parser.add_argument("--record-id", type=_record_id)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="list eligible rows and planned actions without writing Feishu/RDS",
    )
    return parser.parse_args(argv)


def _validate(args: argparse.Namespace) -> None:
    if not 1 <= args.limit <= 20:
        raise ValueError("limit must be between 1 and 20")
    if args.action in {"one-click", "process-mother", "confirm-mother", "generate"} and not args.record_id:
        raise ValueError(f"{args.action} requires --record-id")
    if args.dry_run and args.action == "confirm-mother":
        # A preview is still useful, but it never constitutes human confirmation.
        return


def _run(args: argparse.Namespace):
    app = load_application()
    common = {
        "action": args.action,
        "record_id": args.record_id,
        "limit": args.limit,
        "dry_run": args.dry_run,
    }
    if args.dry_run:
        return invoke_preview(
            app,
            ("preview_pending", "list_pending_tasks"),
            "run_pending",
            **common,
        )
    if args.action == "one-click":
        return invoke(app, ("one_click", "run_pending"), **common)
    if args.action == "process-mother":
        return invoke(app, ("process_mother", "process_mother_record", "run_pending"), **common)
    if args.action == "confirm-mother":
        return invoke(app, ("confirm_mother", "confirm_mother_record", "run_pending"), **common)
    if args.action == "generate":
        return invoke(app, ("generate_replication", "generate_for_record", "run_pending"), **common)
    return invoke(app, ("run_pending", "run_pending_tasks"), **common)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        _validate(args)
        print_result(_run(args))
        return 0
    except (ValueError, RuntimeBridgeError) as exc:
        print(f"error: {exc}")
        return 2
    except Exception as exc:  # domain errors are reported without a traceback by default
        print(f"task failed: {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
