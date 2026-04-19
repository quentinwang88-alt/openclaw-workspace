#!/usr/bin/env python3
"""
按产品编码查询原创脚本生成流水线历史。
"""

import argparse
import json
import sys
from pathlib import Path


SKILL_DIR = Path(__file__).parent.absolute()
sys.path.insert(0, str(SKILL_DIR))

from core.storage import PipelineStorage, default_db_path  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="按产品编码查询原创脚本生成中间过程")
    parser.add_argument("--product-code", required=True, help="产品编码")
    parser.add_argument("--limit", type=int, default=5, help="最多展示多少次运行")
    parser.add_argument("--show-prompts", action="store_true", help="展示各阶段 prompt")
    parser.add_argument("--show-output", action="store_true", help="展示各阶段输出 JSON")
    args = parser.parse_args()

    storage = PipelineStorage()
    runs = storage.query_runs_by_product_code(args.product_code, limit=args.limit)

    print(f"DB_PATH: {default_db_path()}")
    print(f"PRODUCT_CODE: {args.product_code}")
    print(f"RUN_COUNT: {len(runs)}")
    if not runs:
        return

    for row in runs:
        print("\n" + "=" * 72)
        print(
            f"RUN_ID={row['run_id']} | RECORD_ID={row['record_id']} | "
            f"STATUS={row['runtime_status']} | STARTED={row['started_at']} | COMPLETED={row['completed_at'] or ''}"
        )
        print(f"INPUT_HASH={row['input_hash']}")
        print(f"COUNTRY={row['target_country']} | LANGUAGE={row['target_language']} | PRODUCT_TYPE={row['product_type']}")
        if row["error_message"]:
            print(f"ERROR={row['error_message']}")
        if row["stage_durations_json"]:
            print(f"STAGE_DURATIONS={row['stage_durations_json']}")
        if row["anchor_card_json"]:
            print("ANCHOR_CARD_JSON=available")
            if args.show_output:
                try:
                    print(json.dumps(json.loads(row["anchor_card_json"]), ensure_ascii=False, indent=2))
                except json.JSONDecodeError:
                    print(row["anchor_card_json"])
        if row["strategy_cards_json"]:
            print("STRATEGY_CARDS_JSON=available")
            if args.show_output:
                try:
                    print(json.dumps(json.loads(row["strategy_cards_json"]), ensure_ascii=False, indent=2))
                except json.JSONDecodeError:
                    print(row["strategy_cards_json"])

        stages = storage.query_stage_results(int(row["run_id"]))
        for stage in stages:
            print(f"- STAGE {stage['stage_order']}: {stage['stage_name']} [{stage['status']}] {stage['duration_seconds'] or ''}")
            if stage["error_message"]:
                print(f"  ERROR: {stage['error_message']}")
            if args.show_prompts and stage["prompt_text"]:
                print("  PROMPT:")
                print(stage["prompt_text"])
            if args.show_output and stage["output_json"]:
                print("  OUTPUT_JSON:")
                try:
                    print(json.dumps(json.loads(stage["output_json"]), ensure_ascii=False, indent=2))
                except json.JSONDecodeError:
                    print(stage["output_json"])


if __name__ == "__main__":
    main()
