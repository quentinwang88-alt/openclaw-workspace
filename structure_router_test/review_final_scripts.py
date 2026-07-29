#!/usr/bin/env python3
"""Read final rendered scripts for the four-product structure-router review."""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, unquote, urlparse

import pymysql


FINAL_SOURCES = {
    "1736444730937804794": {"S1": 608, "S2": 608, "S3": 608, "S4": 609},
    "1734482585843304442": {"S1": 610, "S2": 610, "S3": 610, "S4": 610},
    "1736446411937318906": {"S1": 611, "S2": 611, "S3": 612, "S4": 614},
    "1734257377321977850": {"S1": 615, "S2": 615, "S3": 615, "S4": 615},
}


def connect():
    parsed = urlparse(os.environ["ORIGINAL_SCRIPT_GENERATOR_DATABASE_URL"])
    query = parse_qs(parsed.query)
    return pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        charset=query.get("charset", ["utf8mb4"])[0],
    )


def stage_context(cursor, run_id: int, stage_name: str) -> Optional[Dict[str, Any]]:
    cursor.execute(
        "SELECT input_context_json FROM osg__stage_results "
        "WHERE run_id=%s AND stage_name=%s AND status='success' "
        "ORDER BY stage_result_id DESC LIMIT 1",
        (run_id, stage_name),
    )
    row = cursor.fetchone()
    return json.loads(row[0]) if row and row[0] else None


def compact_script(script: Dict[str, Any], run_id: int, slot: str) -> Dict[str, Any]:
    plan = script.get("structure_execution_plan") if isinstance(script.get("structure_execution_plan"), dict) else {}
    shots = script.get("storyboard") if isinstance(script.get("storyboard"), list) else []
    return {
        "run_id": run_id,
        "slot": slot,
        "script_positioning": script.get("script_positioning"),
        "opening_design": script.get("opening_design"),
        "scene_seed": script.get("scene_seed"),
        "proof_path": script.get("proof_path"),
        "performance_strategy": script.get("performance_strategy"),
        "plan": {
            "beat_sequence": plan.get("beat_sequence"),
            "content_carrier": plan.get("content_carrier"),
            "continuity_mode": plan.get("continuity_mode"),
            "opening_mechanism": plan.get("opening_mechanism"),
            "shot_count": plan.get("shot_count"),
        },
        "shots": [
            {
                "shot_no": shot.get("shot_no"),
                "duration": shot.get("duration"),
                "structure_beat": shot.get("structure_beat"),
                "carrier_mode": shot.get("carrier_mode"),
                "continuity_group": shot.get("continuity_group"),
                "shot_content": shot.get("shot_content"),
                "shot_purpose": shot.get("shot_purpose"),
                "person_action": shot.get("person_action"),
                "voiceover": shot.get("voiceover_text_target_language"),
                "style_note": shot.get("style_note"),
                "anchor_reference": shot.get("anchor_reference"),
            }
            for shot in shots
            if isinstance(shot, dict)
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--product-code", choices=sorted(FINAL_SOURCES))
    group.add_argument("--all-summary", action="store_true")
    args = parser.parse_args()
    conn = connect()
    cursor = conn.cursor()
    if args.all_summary:
        token_counts = {
            "镜前/镜面": 0,
            "半步后退": 0,
            "低头确认": 0,
            "整理衣摆/下摆": 0,
            "转身/侧身": 0,
            "高腰下装": 0,
            "人台/衣模": 0,
            "咖啡场景": 0,
            "轻微连续变化模板句": 0,
            "提前给出轻决策信号元指令": 0,
        }
        patterns = {
            "镜前/镜面": ("镜前", "镜面", "全身镜", "落地镜"),
            "半步后退": ("半步后退",),
            "低头确认": ("低头",),
            "整理衣摆/下摆": ("整理衣摆", "抚平衣摆", "轻抚衣摆", "整理下摆", "顺过短款下摆"),
            "转身/侧身": ("转身", "侧身", "45度", "三分之四"),
            "高腰下装": ("高腰",),
            "人台/衣模": ("人台", "衣模"),
            "咖啡场景": ("咖啡",),
            "轻微连续变化模板句": ("商品通过机位/光线/角度形成轻微连续变化", "商品通过机位、光线和角度形成轻微连续变化"),
            "提前给出轻决策信号元指令": ("提前给出轻决策信号", "同时给出适合日常使用的轻判断"),
        }
        slot_signatures: Dict[str, set] = {slot: set() for slot in ("S1", "S2", "S3", "S4")}
        script_count = 0
        shot_count = 0
        for product_code, sources in FINAL_SOURCES.items():
            for slot, run_id in sources.items():
                context = stage_context(cursor, run_id, f"script_{slot.lower()}_rendered") or {}
                script = context.get("script_json") if isinstance(context.get("script_json"), dict) else {}
                compact = compact_script(script, run_id, slot)
                plan = compact["plan"]
                carriers = tuple(shot.get("carrier_mode") for shot in compact["shots"])
                slot_signatures[slot].add(
                    (
                        tuple(plan.get("beat_sequence") or []),
                        plan.get("continuity_mode"),
                        plan.get("shot_count"),
                        carriers,
                    )
                )
                script_count += 1
                shot_count += len(compact["shots"])
                text = json.dumps(compact, ensure_ascii=False)
                for label, tokens in patterns.items():
                    if any(token in text for token in tokens):
                        token_counts[label] += 1
        conn.close()
        print(
            json.dumps(
                {
                    "script_count": script_count,
                    "shot_count": shot_count,
                    "scripts_containing_motif": token_counts,
                    "unique_execution_signatures_per_slot_across_products": {
                        slot: len(signatures) for slot, signatures in slot_signatures.items()
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    result: Dict[str, Any] = {"product_code": args.product_code, "scripts": {}}
    for slot, run_id in FINAL_SOURCES[args.product_code].items():
        context = stage_context(cursor, run_id, f"script_{slot.lower()}_rendered") or {}
        script = context.get("script_json") if isinstance(context.get("script_json"), dict) else {}
        result["scripts"][slot] = compact_script(script, run_id, slot)
    conn.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
