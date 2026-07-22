#!/usr/bin/env python3
"""Run standalone VOC opportunity analysis and publish the result to Feishu."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parents[1]
WORKSPACE = SKILL_DIR.parents[1]
VOC_DIR = WORKSPACE / "skills" / "voc-insight"
ANALYZE = VOC_DIR / "scripts" / "run_voc_opportunity.py"
SYNC = SKILL_DIR / "scripts" / "sync_voc_outputs_to_feishu.py"


def run_json(command):
    result = subprocess.run(command, check=True, text=True, capture_output=True)
    return json.loads(result.stdout)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze VOC opportunities and publish to Feishu")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input-json")
    source.add_argument("--batch-id")
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--output-dir", default=str(VOC_DIR / "output"))
    parser.add_argument("--bitable-url", default=None)
    parser.add_argument("--report-parent-wiki-url", default=None)
    parser.add_argument("--report-title", default="")
    parser.add_argument("--dry-run-feishu", action="store_true")
    args = parser.parse_args()

    analyze_cmd = [sys.executable, str(ANALYZE), "--output-dir", args.output_dir]
    if args.input_json:
        analyze_cmd.extend(["--input-json", args.input_json])
    else:
        analyze_cmd.extend(["--batch-id", args.batch_id])
        if args.database_url:
            analyze_cmd.extend(["--database-url", args.database_url])
    analyzed = run_json(analyze_cmd)
    if not analyzed.get("success"):
        raise SystemExit("VOC analysis failed quality gate")
    artifacts = analyzed.get("artifacts") or {}
    sync_cmd = [
        sys.executable, str(SYNC), "--result-json", artifacts["result_json"],
        "--report-markdown", artifacts["overview_report"], "--pretty",
    ]
    if args.bitable_url:
        sync_cmd.extend(["--bitable-url", args.bitable_url])
    if args.report_parent_wiki_url:
        sync_cmd.extend(["--report-parent-wiki-url", args.report_parent_wiki_url])
    if args.report_title:
        sync_cmd.extend(["--report-title", args.report_title])
    if args.dry_run_feishu:
        sync_cmd.append("--dry-run")
    published = run_json(sync_cmd)
    print(json.dumps({"success": True, "analysis": analyzed, "publication": published}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
