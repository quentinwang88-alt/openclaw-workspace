#!/usr/bin/env python3
"""Sync latest market direction cards to Feishu."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_insight_feishu_sync import sync_from_output_config  # noqa: E402


DEFAULT_ARTIFACTS_ROOT = ROOT / "artifacts" / "market_insight"


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync latest market direction cards to Feishu.")
    parser.add_argument("--output-config", required=True)
    parser.add_argument("--artifacts-root", default=str(DEFAULT_ARTIFACTS_ROOT))
    parser.add_argument("--purge-target-scope", action="store_true")
    args = parser.parse_args()

    summary = sync_from_output_config(
        output_config_path=Path(args.output_config),
        artifacts_root=Path(args.artifacts_root),
        purge_target_scope=args.purge_target_scope,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
