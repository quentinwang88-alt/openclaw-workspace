#!/usr/bin/env python3
"""Read-only quality audit for persisted V1.2 replication prompts."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import production_runtime as runtime
from wig_success_replication.deterministic_qa import creative_history_snapshot, text_similarity
from wig_success_replication.repository import PromptRecord


BANNED = ("条件锁", "当前禁止执行", "等待核验", "等待事实核验", "仅供内部", "风险登记")
ANCHORS = {"conflict", "early_product_reveal", "strong_contrast", "proof", "cta"}


def _prompt(raw: dict[str, Any]) -> PromptRecord:
    signature = raw.get("creative_signature_json") or {}
    if isinstance(signature, str):
        signature = json.loads(signature)
    return PromptRecord(
        prompt_id=raw["prompt_id"], batch_id=raw["batch_id"], mother_id=raw["mother_id"],
        mother_version=int(raw["mother_version"]), product_id=raw["product_id"],
        variant_type=raw["variant_type"], variant_key=raw["variant_key"],
        mutation_key=raw["mutation_key"], change_summary=raw["change_summary"],
        full_prompt=raw["full_prompt"], prompt_hash=raw["prompt_hash"],
        sequence_no=int(raw["sequence_no"]), replication_mode=raw["replication_mode"],
        creative_route=raw["creative_route"], creative_signature=signature,
        planner_version=raw["planner_version"], review_status=raw["review_status"],
        feishu_record_id=raw.get("feishu_record_id") or "",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mother-id", required=True)
    parser.add_argument("--mother-version", type=int, required=True)
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    connection_factory = runtime._connection_factory(runtime._database_url())
    with connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """SELECT * FROM wsr_replication_prompt
               WHERE mother_id=%s AND mother_version=%s
               ORDER BY product_id,sequence_no""",
            (args.mother_id, args.mother_version),
        )
        rows = [_prompt(raw) for raw in cursor.fetchall()]

    grouped: dict[str, list[PromptRecord]] = defaultdict(list)
    for row in rows:
        grouped[row.product_id].append(row)
    products: dict[str, Any] = {}
    for product_id, prompts in grouped.items():
        h1 = next((row for row in prompts if row.sequence_no == 1), None)
        h1_voiceover = str(creative_history_snapshot(h1).get("voiceover_text") or "") if h1 else ""
        generated = [row for row in prompts if row.planner_version.startswith("v1.2")]
        checks = []
        for row in generated:
            signature = row.creative_signature
            voiceover = str(signature.get("voiceover_text") or "")
            checks.append({
                "sequence_no": row.sequence_no,
                "route": row.creative_route,
                "h1_voiceover_similarity": round(text_similarity(h1_voiceover, voiceover), 4) if h1_voiceover else None,
                "changed_dimensions": signature.get("changed_dimensions") or [],
                "proof_actions": signature.get("proof_actions") or [],
                "core_anchors_complete": set(signature.get("core_anchors") or []) == ANCHORS,
                "banned_phrases": [phrase for phrase in BANNED if phrase in row.full_prompt],
            })
        pairwise = []
        for index, left in enumerate(generated):
            for right in generated[index + 1:]:
                pairwise.append({
                    "left": left.sequence_no,
                    "right": right.sequence_no,
                    "voiceover_similarity": round(text_similarity(
                        str(left.creative_signature.get("voiceover_text") or ""),
                        str(right.creative_signature.get("voiceover_text") or ""),
                    ), 4),
                })
        products[product_id] = {
            "sequences": [row.sequence_no for row in prompts],
            "h1_voiceover_detected": bool(h1_voiceover),
            "checks": checks,
            "pairwise": pairwise,
            "all_gates_passed": all(
                len(set(item["changed_dimensions"])) >= 3
                and len(item["proof_actions"]) >= 2
                and item["core_anchors_complete"]
                and not item["banned_phrases"]
                and (item["h1_voiceover_similarity"] is None or 0.4 <= item["h1_voiceover_similarity"] <= 0.7)
                for item in checks
            ) and all(item["voiceover_similarity"] <= 0.75 for item in pairwise),
            "generated_prompts": [
                {
                    "sequence_no": row.sequence_no,
                    "route": row.creative_route,
                    "change_summary": row.change_summary,
                    "creative_signature": row.creative_signature,
                    "full_prompt": row.full_prompt,
                }
                for row in generated
            ],
        }
    report = {
        "created_at": datetime.now().astimezone().isoformat(),
        "mother_id": args.mother_id,
        "mother_version": args.mother_version,
        "products": products,
    }
    output = Path(args.output) if args.output else (
        Path(__file__).resolve().parents[1] / "artifacts"
        / f"v12-audit-{args.mother_version}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "output": str(output),
        "products": {
            key: {
                "sequences": value["sequences"],
                "all_gates_passed": value["all_gates_passed"],
                "checks": value["checks"],
                "pairwise": value["pairwise"],
            }
            for key, value in products.items()
        },
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
