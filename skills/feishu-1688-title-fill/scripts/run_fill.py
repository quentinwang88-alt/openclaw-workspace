#!/usr/bin/env python3
"""Run quiet batched Feishu 1688 title filling until completion or stall."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Tuple


SKILL_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_SKILLS = SKILL_ROOT.parent
FEISHU_HELPER_CANDIDATES = [
    WORKSPACE_SKILLS / "hair-style-review",
    Path("/Users/likeu3/.openclaw/workspace/skills/hair-style-review"),
]
WORKER_CANDIDATES = [
    WORKSPACE_SKILLS / "1688-title-fetcher" / "fill_feishu_1688_titles.py",
    Path("/Users/likeu3/.openclaw/workspace/skills/1688-title-fetcher/fill_feishu_1688_titles.py"),
]


def _install_feishu_helper() -> None:
    for candidate in FEISHU_HELPER_CANDIDATES:
        if candidate.exists():
            sys.path.insert(0, str(candidate))
            return
    raise RuntimeError("Cannot find Feishu helper skill at the expected local paths.")


def _resolve_worker_script() -> Path:
    for candidate in WORKER_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Cannot find fill_feishu_1688_titles.py in the expected OpenClaw skill paths.")


_install_feishu_helper()
WORKER_SCRIPT = _resolve_worker_script()

from core.feishu import FeishuBitableClient, parse_feishu_bitable_url, resolve_bitable_app_token  # type: ignore  # noqa: E402


UPDATED_RE = re.compile(r"^updated_records=(\d+)$", re.MULTILINE)


def extract_link_value(raw_value) -> str:
    if raw_value is None:
        return ""
    if isinstance(raw_value, str):
        return raw_value.strip()
    if isinstance(raw_value, dict):
        for key in ("link", "text", "url"):
            value = raw_value.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""
    if isinstance(raw_value, list):
        for item in raw_value:
            value = extract_link_value(item)
            if value:
                return value
    return ""


def build_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError(f"Cannot parse Feishu URL: {feishu_url}")
    app_token = resolve_bitable_app_token(info)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def count_rows(client: FeishuBitableClient, link_field: str, title_field: str) -> Tuple[int, int]:
    with_title = 0
    missing = 0
    for record in client.list_records():
        link = extract_link_value(record.fields.get(link_field))
        if "1688.com" not in link:
            continue
        title = str(record.fields.get(title_field) or "").strip()
        if title:
            with_title += 1
        else:
            missing += 1
    return with_title, missing


def parse_updated_records(output: str) -> int:
    match = UPDATED_RE.search(output)
    if not match:
        return 0
    return int(match.group(1))


def extend_args(cmd: list[str], flag: str, values: Iterable[str]) -> None:
    for value in values:
        cmd.extend([flag, value])


def main() -> int:
    parser = argparse.ArgumentParser(description="Run quiet batched Feishu 1688 title fill.")
    parser.add_argument("--feishu-url", required=True)
    parser.add_argument("--link-field", default="采购链接")
    parser.add_argument("--title-field", default="产品标题")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--max-batches", type=int)
    parser.add_argument("--sleep", type=float, default=0.5)
    parser.add_argument("--chrome-wait-seconds", type=int, default=30)
    parser.add_argument("--record-id", action="append", default=[])
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--worker-script", default=str(WORKER_SCRIPT))
    args = parser.parse_args()

    worker_script = Path(args.worker_script)
    if not worker_script.exists():
        raise FileNotFoundError(f"Worker script not found: {worker_script}")

    client = build_client(args.feishu_url)
    with_title, pending = count_rows(client, args.link_field, args.title_field)
    print(f"initial_with_title={with_title}")
    print(f"initial_pending={pending}")

    if pending == 0:
        print("No pending rows remain.")
        return 0

    total_updated = 0
    batch_no = 0

    while pending > 0:
        if args.max_batches is not None and batch_no >= args.max_batches:
            print("Reached max_batches before finishing.")
            break

        limit = min(max(args.batch_size, 1), pending)
        cmd = [
            "python3",
            str(worker_script),
            "--feishu-url",
            args.feishu_url,
            "--fetch-mode",
            "chrome-live",
            "--limit",
            str(limit),
            "--sleep",
            str(args.sleep),
            "--chrome-wait-seconds",
            str(args.chrome_wait_seconds),
            "--link-field",
            args.link_field,
            "--title-field",
            args.title_field,
        ]
        extend_args(cmd, "--record-id", args.record_id)
        if args.force:
            cmd.append("--force")
        if args.dry_run:
            cmd.append("--dry-run")

        result = subprocess.run(cmd, check=False, capture_output=True, text=True)
        stdout = result.stdout or ""
        stderr = result.stderr or ""
        if stdout:
            print(stdout.rstrip())
        if stderr:
            print(stderr.rstrip(), file=sys.stderr)
        if result.returncode != 0:
            return result.returncode

        updated = parse_updated_records(stdout)
        total_updated += updated
        batch_no += 1

        with_title, new_pending = count_rows(client, args.link_field, args.title_field)
        print(f"after_batch={batch_no}")
        print(f"current_with_title={with_title}")
        print(f"current_pending={new_pending}")

        if new_pending >= pending and updated == 0:
            print("No progress in the last batch. Stop and inspect Chrome login/captcha state.")
            return 1

        pending = new_pending
        if args.dry_run:
            break

    print(f"final_with_title={with_title}")
    print(f"final_pending={pending}")
    print(f"total_updated={total_updated}")
    return 0 if pending == 0 or args.dry_run else 1


if __name__ == "__main__":
    raise SystemExit(main())
