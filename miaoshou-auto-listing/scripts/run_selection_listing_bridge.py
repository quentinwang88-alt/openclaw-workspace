#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import json
import subprocess
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC = PROJECT_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from miaoshou_auto_listing.config import load_config  # noqa: E402
from miaoshou_auto_listing.services.feishu_task_table import FeishuTaskTable  # noqa: E402
from miaoshou_auto_listing.services.selection_workbench_bridge import (  # noqa: E402
    QUEUE_FIELD_SPECS,
    WORKBENCH_FIELD_SPECS,
    SelectionListingBridge,
    ensure_bridge_fields,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bridge confirmed selection-workbench rows into the listing queue"
    )
    parser.add_argument("--config-dir", type=Path, default=PROJECT_ROOT / "config")
    parser.add_argument(
        "--sync", action="store_true", help="Create/update queue rows and write back results"
    )
    parser.add_argument(
        "--ensure-fields", action="store_true", help="Create missing bridge fields"
    )
    parser.add_argument(
        "--execute-ready",
        action="store_true",
        help="After syncing, publish each newly ready queue row sequentially",
    )
    args = parser.parse_args()
    if args.execute_ready and not args.sync:
        raise SystemExit("--execute-ready requires --sync")

    config_dir = args.config_dir.resolve()
    config = load_config(config_dir)
    bridge_config = yaml.safe_load(
        (config_dir / "selection_bridge.yaml").read_text(encoding="utf-8")
    )["selection_bridge"]
    workbench = FeishuTaskTable(
        config, table_url=str(bridge_config["workbench_url"])
    )
    queue = FeishuTaskTable(config)

    # Share one browser-operation lock with the OpenClaw batch runner. Syncing
    # and publishing must never race against another Miaoshou session.
    lock_path = PROJECT_ROOT / "runtime" / "miaoshou_listing.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w", encoding="utf-8") as lock_handle:
        try:
            fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SystemExit("selection listing bridge is already running") from exc

        created_fields = {"workbench": [], "queue": []}
        if args.ensure_fields:
            created_fields["workbench"] = ensure_bridge_fields(
                workbench, WORKBENCH_FIELD_SPECS
            )
            created_fields["queue"] = ensure_bridge_fields(queue, QUEUE_FIELD_SPECS)

        report = SelectionListingBridge(config, workbench, queue).sync(
            dry_run=not args.sync
        )
        output = report.to_dict()
        output["mode"] = "sync" if args.sync else "dry_run"
        output["created_fields"] = created_fields
        print(json.dumps(output, ensure_ascii=False, indent=2))

        if args.execute_ready:
            for record_id in report.execute_record_ids:
                if record_id.startswith("dry-run:"):
                    continue
                subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "miaoshou_auto_listing.cli",
                        "--config-dir",
                        str(config_dir),
                        "--feishu-record",
                        record_id,
                    ],
                    cwd=PROJECT_ROOT,
                    check=True,
                )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
