#!/usr/bin/env python3
"""结构路由V2测试分析器 — 多产品、多层对比，不使用 task_type 推断结构 Beat。"""
import argparse
import json
import os
import sys
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

import pymysql

DB_URL = os.environ.get("ORIGINAL_SCRIPT_GENERATOR_DATABASE_URL", "")


def _connect():
    parsed = urlparse(DB_URL)
    query = parse_qs(parsed.query)
    database = parsed.path.lstrip("/")
    return pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=database,
        charset=query.get("charset", ["utf8mb4"])[0],
    )


def compress_beats(beats: List[str]) -> List[str]:
    """连续重复压缩，不全局去重。HOOK>PROOF>PROOF>PROOF→HOOK>PROOF"""
    compressed = []
    for b in beats:
        b = str(b or "").strip().upper()
        if not compressed or compressed[-1] != b:
            compressed.append(b)
    return compressed


def extract_contract(strategy: Optional[Dict]) -> Dict:
    if not isinstance(strategy, dict):
        return {}
    sc = strategy.get("structure_contract")
    if not isinstance(sc, dict):
        return {}
    return sc


def extract_plan_from_brief(brief: Optional[Dict]) -> Dict:
    if not isinstance(brief, dict):
        return {}
    plan = brief.get("structure_execution_plan")
    return plan if isinstance(plan, dict) else {}


def get_stage_result(cursor, run_id: int, stage: str) -> Optional[Dict]:
    cursor.execute(
        "SELECT output_json FROM osg__stage_results WHERE run_id=%s AND stage_name=%s AND status='success' ORDER BY stage_result_id DESC LIMIT 1",
        (run_id, stage),
    )
    row = cursor.fetchone()
    if row and row[0]:
        return json.loads(row[0])
    return None


def get_stage_context(cursor, run_id: int, stage: str) -> Optional[Dict]:
    cursor.execute(
        "SELECT input_context_json FROM osg__stage_results WHERE run_id=%s AND stage_name=%s AND status='success' ORDER BY stage_result_id DESC LIMIT 1",
        (run_id, stage),
    )
    row = cursor.fetchone()
    if row and row[0]:
        return json.loads(row[0])
    return None


def extract_structure_signature(
    plan: Dict, storyboard: Optional[List], shot_skeleton: Optional[List]
) -> Dict:
    """Extract structure signature from execution plan and actual fields."""
    shot_plan = [item for item in (plan.get("shot_plan") or []) if isinstance(item, dict)]

    # Planned beats
    planned_beats = [item.get("structure_beat", "") for item in shot_plan]

    # Actual beats from storyboard/skeleton
    script_beats = []
    script_carriers = []
    script_groups = []
    script_openings = []
    sources = storyboard or shot_skeleton or []
    for item in (sources if isinstance(sources, list) else []):
        if isinstance(item, dict):
            script_beats.append(item.get("structure_beat", "MISSING"))
            script_carriers.append(item.get("carrier_mode", "MISSING"))
            script_groups.append(item.get("continuity_group", "MISSING"))
            script_openings.append(item.get("opening_mechanism", "MISSING"))

    return {
        "planned_beat_sequence": planned_beats,
        "planned_beat_compressed": compress_beats(planned_beats),
        "script_beat_sequence": script_beats,
        "script_beat_compressed": compress_beats(script_beats),
        "script_carrier_sequence": script_carriers,
        "script_group_pattern": "-".join(
            str(g or "") for g in script_groups
        ),
        "first_opening": script_openings[0] if script_openings else "MISSING",
        "shot_count_planned": plan.get("shot_count", 0),
        "shot_count_actual": len(sources),
        "contract_applied": plan.get("contract_applied", False),
        "blocking_conflicts": plan.get("blocking_conflicts", []),
    }


def check_per_shot_consistency(
    plan: Dict, storyboard: Optional[List]
) -> Tuple[bool, List[str]]:
    violations = []
    shot_plan = [item for item in (plan.get("shot_plan") or []) if isinstance(item, dict)]
    sources = storyboard if isinstance(storyboard, list) else []
    if not plan.get("contract_applied") or not shot_plan:
        violations.append("structure_execution_plan missing or not applied")
    if len(sources) != len(shot_plan):
        violations.append(
            f"shot count mismatch: planned={len(shot_plan)}, actual={len(sources)}"
        )
    for idx, plan_shot in enumerate(shot_plan):
        if idx >= len(sources) or not isinstance(sources[idx], dict):
            violations.append(f"Shot {idx + 1}: missing or non-dict in output")
            continue
        actual = sources[idx]
        for field in ["structure_beat", "carrier_mode", "continuity_group", "opening_mechanism"]:
            expected = plan_shot.get(field, "")
            actual_val = actual.get(field, "MISSING")
            if str(expected) != str(actual_val):
                violations.append(
                    f"Shot {idx + 1} {field}: expected='{expected}', actual='{actual_val}'"
                )
    return len(violations) == 0, violations


def check_carrier_semantics(
    script_json: Dict, plan: Dict
) -> Tuple[bool, List[str]]:
    violations = []
    shot_plan = [item for item in (plan.get("shot_plan") or []) if isinstance(item, dict)]
    storyboard = script_json.get("storyboard") if isinstance(script_json.get("storyboard"), list) else []
    for idx, plan_shot in enumerate(shot_plan):
        carrier = plan_shot.get("carrier_mode", "")
        if idx >= len(storyboard):
            continue
        shot = storyboard[idx] if isinstance(storyboard[idx], dict) else {}
        content = str(shot.get("shot_content", shot.get("scene_description", ""))).lower()
        action = str(shot.get("person_action", "")).lower()
        if carrier == "STATIC_PRODUCT":
            if any(w in content for w in ["模特", "人物", "身穿", "穿戴", "上身"]):
                violations.append(f"Shot {idx + 1}: STATIC_PRODUCT but content has person terms")
            if action.strip():
                violations.append(f"Shot {idx + 1}: STATIC_PRODUCT but has person_action")
        if carrier == "HAND_ONLY":
            if any(w in content for w in ["全身", "全身镜", "模特"]):
                violations.append(f"Shot {idx + 1}: HAND_ONLY but content suggests full person")
    return len(violations) == 0, violations


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=int, nargs="+", required=True)
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print the final execution plan and rendered shots for failed directions.",
    )
    args = parser.parse_args()

    conn = _connect()
    cursor = conn.cursor()

    all_results = []

    for run_id in args.run_id:
        cursor.execute(
            "SELECT record_id, product_code, runtime_status, error_message, strategy_cards_json FROM osg__pipeline_runs WHERE run_id=%s",
            (run_id,),
        )
        row = cursor.fetchone()
        if not row:
            print(f"Run {run_id}: NOT FOUND", file=sys.stderr)
            continue
        record_id, product_code, status, error, strategy_json = row
        strategies = json.loads(strategy_json) if strategy_json else {}

        print(f"\n{'='*60}")
        print(f"Run {run_id} | Product {product_code} | Status: {status}")
        if error:
            print(f"  Error: {error[:200]}")

        # Strategy contracts
        strat_list = strategies.get("strategies", [])
        print(f"  Strategies: {len(strat_list)}")
        contracts = {}
        for i, s in enumerate(strat_list):
            if isinstance(s, dict):
                contract = extract_contract(s)
                contracts[i + 1] = contract

        # Check each direction
        direction_results = {}
        for slot_idx in range(1, 5):
            suffix = f"s{slot_idx}"

            # Final enriched artifacts are persisted in rendered-stage contexts.
            # Raw review/video stage output is the model/template response before
            # structure validation and provenance are attached.
            script_rendered_context = get_stage_context(cursor, run_id, f"script_{suffix}_rendered") or {}
            video_rendered_context = get_stage_context(cursor, run_id, f"video_prompt_{suffix}_rendered") or {}
            review = get_stage_result(cursor, run_id, f"script_review_{suffix}")
            script = get_stage_result(cursor, run_id, f"script_{suffix}")
            video = get_stage_result(cursor, run_id, f"video_prompt_{suffix}")

            if isinstance(script_rendered_context.get("script_json"), dict):
                final_script = script_rendered_context["script_json"]
            elif review:
                repaired = review.get("repaired_script")
                final_script = repaired if isinstance(repaired, dict) and repaired else script
            else:
                final_script = script

            if isinstance(video_rendered_context.get("video_prompt_json"), dict):
                video = video_rendered_context["video_prompt_json"]

            if not final_script:
                direction_results[suffix.upper()] = {"status": "NOT_GENERATED"}
                continue

            # Get plan from final script, then fall back to the authoritative brief.
            plan = final_script.get("structure_execution_plan", {})
            brief = get_stage_result(cursor, run_id, f"script_brief_{suffix}") or {}
            if not plan:
                plan = extract_plan_from_brief(brief)
            if slot_idx not in contracts:
                brief_contract = brief.get("structure_contract")
                if isinstance(brief_contract, dict):
                    contracts[slot_idx] = brief_contract

            storyboard = final_script.get("storyboard")
            skeleton = final_script.get("shot_skeleton")

            sig = extract_structure_signature(plan, storyboard, skeleton)
            consistent, violations = check_per_shot_consistency(plan, storyboard)

            # Check contract_validation
            scv = final_script.get("structure_contract_validation") or {}
            contract = contracts.get(slot_idx, {})
            if scv.get("valid") is not True:
                violations.append(
                    "structure_contract_validation.valid is not true: "
                    + json.dumps(scv, ensure_ascii=False)
                )

            video_consistent = False
            video_violations: List[str] = []
            if isinstance(video, dict):
                video_consistent, video_violations = check_per_shot_consistency(
                    plan,
                    video.get("shot_execution") if isinstance(video.get("shot_execution"), list) else [],
                )
                video_validation = video.get("structure_contract_validation") or {}
                if video_validation.get("valid") is not True:
                    video_violations.append(
                        "video structure_contract_validation.valid is not true: "
                        + json.dumps(video_validation, ensure_ascii=False)
                    )
                    video_consistent = False
            else:
                video_violations.append("video_prompt not generated")

            print(f"\n  {suffix.upper()}:")
            direction_passed = consistent and not violations and video_consistent and not video_violations
            print(f"    Status: {'PASSED' if direction_passed else 'FAILED'}")
            print(f"    Plan beats: {'>'.join(sig['planned_beat_compressed'])}")
            print(f"    Script beats: {'>'.join(sig['script_beat_compressed'])}")
            print(f"    Carriers: {'>'.join(sig['script_carrier_sequence'])}")
            print(f"    Contract valid: {scv.get('valid', 'MISSING')}")
            if violations:
                for v in violations:
                    print(f"    ⚠️ {v}")
            if video_violations:
                for v in video_violations:
                    print(f"    ⚠️ video: {v}")
            if args.verbose and not direction_passed:
                print("    Execution plan:")
                print(json.dumps(plan, ensure_ascii=False, indent=2))
                print("    Final storyboard:")
                print(json.dumps(storyboard, ensure_ascii=False, indent=2))
                if isinstance(video, dict):
                    print("    Final video shots:")
                    print(json.dumps(video.get("shot_execution"), ensure_ascii=False, indent=2))

            direction_results[suffix.upper()] = {
                "status": "PASSED" if direction_passed else "FAILED",
                "signature": sig,
                "violations": violations,
                "video_status": "PASSED" if video_consistent and not video_violations else "FAILED",
                "video_violations": video_violations,
            }

        all_results.append(
            {
                "run_id": run_id,
                "product_code": product_code,
                "tasks_status": status,
                "directions": direction_results,
            }
        )

    # Pairwise comparison per product
    for result in all_results:
        directions = result["directions"]
        keys = sorted(
            key for key, value in directions.items()
            if isinstance(value, dict) and isinstance(value.get("signature"), dict)
        )
        if len(keys) < 2:
            print(f"\n--- Pairwise for Run {result['run_id']}: UNAVAILABLE (fewer than 2 generated scripts) ---")
            continue
        print(f"\n--- Pairwise for Run {result['run_id']} ---")
        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                di = directions[keys[i]]
                dj = directions[keys[j]]
                si = di.get("signature", {})
                sj = dj.get("signature", {})
                core_diffs = sum(
                    1
                    for ax in [
                        "script_beat_compressed",
                        "script_carrier_sequence",
                        "script_group_pattern",
                        "first_opening",
                    ]
                    if str(si.get(ax)) != str(sj.get(ax))
                )
                status_icon = "✅" if core_diffs >= 2 else "⚠️"
                print(f"  {status_icon} {keys[i]} vs {keys[j]}: {core_diffs} core diffs")

    conn.close()
    print("\nAnalysis complete.")


if __name__ == "__main__":
    main()
