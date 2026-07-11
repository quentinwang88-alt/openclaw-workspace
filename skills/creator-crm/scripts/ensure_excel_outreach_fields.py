#!/usr/bin/env python3
"""Ensure the final Excel-outreach Feishu table has concise outreach fields."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = SKILL_DIR.parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from workspace_support import load_repo_env

load_repo_env()

from scripts.run_excel_outreach_pipeline import ensure_final_fields, parse_feishu_url  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Ensure final Excel outreach fields.")
    parser.add_argument("--feishu-url", required=True)
    args = parser.parse_args()
    app_token, table_id = parse_feishu_url(args.feishu_url)
    ensure_final_fields(app_token, table_id)
    print("✅ 最终建联表字段已确认")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
