#!/usr/bin/env python3
"""Run V1.2 against real Feishu/RDS inputs without external writes.

This evaluator intentionally uses an in-memory repository and an unlogged model
client. It reads the confirmed mother, product facts, images, and historical
prompts, generates only the missing slots up to the requested target, and saves
the evaluation report locally. It never updates Feishu or RDS.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import production_runtime as runtime
from wig_success_replication.deterministic_qa import (
    creative_history_snapshot,
    text_similarity,
)
from wig_success_replication.repository import InMemoryRepository, PromptRecord
from wig_success_replication.replication_service import ReplicationBatchService
from wig_success_replication.structured_llm import StructuredResponsesClient


def _historical_prompts(connection_factory: Any, mother_id: str, version: int) -> list[dict[str, Any]]:
    with connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """SELECT prompt_id,batch_id,mother_id,mother_version,product_id,
                      variant_type,variant_key,mutation_key,change_summary,full_prompt,
                      prompt_hash,review_status,feishu_record_id,created_at
               FROM wsr_replication_prompt
               WHERE mother_id=%s AND mother_version=%s
               ORDER BY product_id,created_at,prompt_id""",
            (mother_id, version),
        )
        return list(cursor.fetchall())


def _seed_legacy_prompt(repository: InMemoryRepository, raw: dict[str, Any], sequence_no: int) -> None:
    route = f"H{sequence_no}" if sequence_no <= 3 else f"LEGACY-{sequence_no}"
    mode = "high_fidelity" if sequence_no <= 3 else "general"
    variant_type = f"high_fidelity_h{sequence_no}" if sequence_no <= 3 else str(raw["variant_type"])
    row = PromptRecord(
        prompt_id=str(raw["prompt_id"]),
        batch_id=str(raw["batch_id"]),
        mother_id=str(raw["mother_id"]),
        mother_version=int(raw["mother_version"]),
        product_id=str(raw["product_id"]),
        variant_type=variant_type,
        variant_key=str(raw["variant_key"]),
        mutation_key=str(raw["mutation_key"]),
        change_summary=str(raw["change_summary"]),
        full_prompt=str(raw["full_prompt"]),
        prompt_hash=str(raw["prompt_hash"]),
        sequence_no=sequence_no,
        replication_mode=mode,
        creative_route=route,
        creative_signature={"legacy": True, "prompt_hash": str(raw["prompt_hash"])},
        planner_version="legacy-evaluation",
        review_status=str(raw.get("review_status") or "pending_review"),
        feishu_record_id=str(raw.get("feishu_record_id") or ""),
    )
    row.creative_signature = creative_history_snapshot(row)
    if not repository.save_prompt(row):
        raise RuntimeError(f"could not seed historical prompt {row.prompt_id}")


def _quality_report(repository: InMemoryRepository, mother_id: str, version: int, product_id: str) -> dict[str, Any]:
    rows = repository.list_prompts(mother_id, version, product_id)
    h1 = next((row for row in rows if row.sequence_no == 1), None)
    h1_voiceover = str((h1.creative_signature if h1 else {}).get("voiceover_text") or "")
    generated = [row for row in rows if row.planner_version == "v1.2-fidelity-general"]
    comparisons = []
    for row in generated:
        signature = row.creative_signature
        voiceover = str(signature.get("voiceover_text") or "")
        comparisons.append({
            "sequence_no": row.sequence_no,
            "mode": row.replication_mode,
            "route": row.creative_route,
            "h1_voiceover_similarity": round(text_similarity(h1_voiceover, voiceover), 4) if h1_voiceover else None,
            "changed_dimension_count": len(set(signature.get("changed_dimensions") or [])),
            "proof_action_count": len(signature.get("proof_actions") or []),
            "contains_internal_control_language": any(
                phrase in row.full_prompt
                for phrase in ("条件锁", "当前禁止执行", "等待核验", "等待事实核验", "仅供内部")
            ),
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
    return {
        "h1_voiceover_detected": bool(h1_voiceover),
        "generated_checks": comparisons,
        "generated_pairwise_voiceover_similarity": pairwise,
        "all_deterministic_gates_passed": all(
            item["changed_dimension_count"] >= 3
            and item["proof_action_count"] >= 2
            and not item["contains_internal_control_language"]
            and (
                item["h1_voiceover_similarity"] is None
                or 0.4 <= item["h1_voiceover_similarity"] <= 0.7
            )
            for item in comparisons
        ) and all(item["voiceover_similarity"] <= 0.75 for item in pairwise),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate V1.2 with real data and no external writes")
    parser.add_argument("--record-id", required=True)
    parser.add_argument("--target", type=int, default=6)
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    if not 4 <= args.target <= 20:
        raise ValueError("evaluation target must be between 4 and 20")

    connection_factory = runtime._connection_factory(runtime._database_url())
    live_repository = runtime.MySQLRepository(connection_factory)
    app_id, app_secret = runtime._feishu_credentials()
    feishu = runtime.FeishuClient(app_id, app_secret)
    mother_row = feishu.get_record(runtime.DEFAULT_APP_TOKEN, runtime.DEFAULT_MOTHER_TABLE, args.record_id)
    fields = mother_row.get("fields") or {}
    mother_id = runtime.deterministic_id("mx_wig_mother", args.record_id)
    mother = live_repository.latest_mother(mother_id)
    if mother is None or mother.status.value != "confirmed":
        raise RuntimeError("confirmed mother not found")

    selected_record_ids = runtime._relation_ids(fields.get("待复刻产品"))
    if not selected_record_ids:
        raise RuntimeError("待复刻产品 is empty")
    face_images = feishu.attachment_data_urls(fields.get("人物脸部参考图") or [], limit=2)
    if not face_images:
        raise RuntimeError("人物脸部参考图 is empty")

    isolated = InMemoryRepository()
    isolated.save_mother(mother)
    product_ids: list[str] = []
    product_images: dict[str, list[str]] = {}
    for record_id in selected_record_ids:
        row = feishu.get_record(runtime.DEFAULT_APP_TOKEN, runtime.DEFAULT_PRODUCT_TABLE, record_id)
        product_fields = row.get("fields") or {}
        product_id = runtime._text(product_fields.get("产品ID"))
        product = live_repository.latest_product_fact(product_id)
        if product is None or product.status.value != "confirmed":
            raise RuntimeError(f"confirmed product fact not found: {product_id}")
        isolated.save_product_fact(product)
        product_ids.append(product_id)
        product_images[product_id] = feishu.attachment_data_urls(product_fields.get("产品图片") or [])

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in _historical_prompts(connection_factory, mother_id, mother.version):
        grouped[str(raw["product_id"])].append(raw)
    for product_id in product_ids:
        for sequence_no, raw in enumerate(grouped.get(product_id, []), start=1):
            _seed_legacy_prompt(isolated, raw, sequence_no)

    llm = StructuredResponsesClient.from_openai_client(
        runtime._openai_client(), logger=None, max_retries=1
    )
    result = ReplicationBatchService(
        isolated, llm, max_product_workers=min(2, len(product_ids))
    ).generate(
        mother_id=mother_id,
        product_ids=product_ids,
        special_requirements=runtime._text(fields.get("特殊要求")),
        per_product_count=args.target,
        product_image_urls=product_images,
        face_reference_image_urls=face_images,
    )

    report = {
        "mode": "isolated-real-data-evaluation",
        "external_writes": False,
        "created_at": datetime.now().astimezone().isoformat(),
        "record_id": args.record_id,
        "mother_id": mother_id,
        "mother_version": mother.version,
        "target": args.target,
        "products": product_ids,
        "saved_prompts": result.saved_prompts,
        "failed_products": list(result.failed_products),
        "errors": list(result.errors),
        "quality": {
            product_id: _quality_report(isolated, mother_id, mother.version, product_id)
            for product_id in product_ids
        },
        "generated_prompts": [
            {
                "product_id": row.product_id,
                "sequence_no": row.sequence_no,
                "mode": row.replication_mode,
                "route": row.creative_route,
                "variant_type": row.variant_type,
                "change_summary": row.change_summary,
                "creative_signature": row.creative_signature,
                "full_prompt": row.full_prompt,
            }
            for row in isolated.prompts.values()
            if row.planner_version == "v1.2-fidelity-general"
        ],
    }
    output = Path(args.output) if args.output else (
        Path(__file__).resolve().parents[1]
        / "artifacts"
        / f"v12-real-eval-{args.record_id}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "output": str(output),
        "saved_prompts": result.saved_prompts,
        "failed_products": list(result.failed_products),
        "all_gates_passed": {
            product_id: values["all_deterministic_gates_passed"]
            for product_id, values in report["quality"].items()
        },
        "external_writes": False,
    }, ensure_ascii=False, indent=2))
    return 0 if not result.failed_products else 1


if __name__ == "__main__":
    raise SystemExit(main())
