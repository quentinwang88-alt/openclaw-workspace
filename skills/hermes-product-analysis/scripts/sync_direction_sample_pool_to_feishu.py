#!/usr/bin/env python3
"""Create/reuse and sync Market Insight direction sample pool table to Feishu."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_insight_sample_pool_feishu_sync import (  # noqa: E402
    DEFAULT_TABLE_NAME,
    sync_sample_pool_from_output_config,
)


DEFAULT_ARTIFACTS_ROOT = ROOT / "artifacts" / "market_insight"


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync direction top/new sample pool to Feishu.")
    parser.add_argument("--output-config", required=True)
    parser.add_argument("--artifacts-root", default=str(DEFAULT_ARTIFACTS_ROOT))
    parser.add_argument("--table-name", default=DEFAULT_TABLE_NAME)
    args = parser.parse_args()

    summary = sync_sample_pool_from_output_config(
        output_config_path=Path(args.output_config),
        artifacts_root=Path(args.artifacts_root),
        table_name=args.table_name,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
