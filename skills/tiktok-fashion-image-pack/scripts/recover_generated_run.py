#!/usr/bin/env python3
"""Recover a completed local image-pack run after Feishu upload/writeback failed."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List


SKILL_DIR = Path(__file__).resolve().parents[1]
CORE_DIR = SKILL_DIR / "core"
sys.path.insert(0, str(SKILL_DIR))
sys.path.insert(0, str(CORE_DIR))

from feishu import normalize_cell_value, resolve_client_from_url  # noqa: E402
from json_utils import dumps_pretty  # noqa: E402
from product_truth import analyze_product_truth, repair_multicolor_truth_from_sources  # noqa: E402
from prompt_builder import build_label, build_main_image_prompt  # noqa: E402
from qa import qa_generated_image, qa_scene_image  # noqa: E402
from scene_prompt_builder import build_scene_image_prompts, parse_scene_slots  # noqa: E402
from run_pipeline import (  # noqa: E402
    FIELD,
    STATUS_FAILED,
    STATUS_PROCESSING,
    STATUS_REVIEW,
    build_truth_update,
    combine_workflow_status,
    decide_final_status,
    decide_scene_status,
    download_scene_reference_images,
    download_source_images,
    join_list,
    normalize_generation_type,
    normalize_scene_preference,
    now_ms,
    safe_update,
    summarize_qa_results,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feishu-url", required=True)
    parser.add_argument("--record-id", required=True)
    parser.add_argument("--generated-dir", required=True)
    return parser.parse_args()


def select_generated_file(generated_dir: Path, marker: str) -> Path:
    candidates = sorted(generated_dir.glob(f"*_{marker}_*.png"), key=lambda path: path.stat().st_mtime)
    if not candidates:
        raise RuntimeError(f"未找到已生成图片: {marker}")
    return candidates[-1]


def main() -> int:
    args = parse_args()
    generated_dir = Path(args.generated_dir).expanduser().resolve()
    client, _ = resolve_client_from_url(args.feishu_url)
    record = next(
        (item for item in client.list_records(limit=None) if item.record_id == args.record_id),
        None,
    )
    if record is None:
        raise SystemExit(f"未找到飞书记录: {args.record_id}")

    safe_update(client, record.record_id, {
        FIELD["status"]: STATUS_PROCESSING,
        FIELD["final_status"]: "恢复本地已生成图片并重新上传",
        FIELD["last_run_at"]: now_ms(),
    })

    try:
        recovery_root = generated_dir.parent / "recovery"
        source_paths = download_source_images(client, record, recovery_root / "source")
        scene_reference_paths = download_scene_reference_images(client, record, recovery_root / "scene_reference")
        if not source_paths:
            raise RuntimeError("原始图片字段没有附件")

        country = normalize_cell_value(record.fields.get(FIELD["country"])) or "MX"
        category = normalize_cell_value(record.fields.get(FIELD["category"])) or "假发"
        notes = "\n".join(
            item for item in (
                normalize_cell_value(record.fields.get(FIELD["notes"])),
                normalize_cell_value(record.fields.get(FIELD["override"])),
            ) if item
        )
        truth = analyze_product_truth(
            image_paths=[str(path) for path in source_paths],
            country=country,
            category=category,
            notes=notes,
        )
        truth = repair_multicolor_truth_from_sources(truth, [str(path) for path in source_paths])

        brand_name = normalize_cell_value(record.fields.get(FIELD["brand_name"])) or "likeU"
        label_strategy = normalize_cell_value(record.fields.get(FIELD["label_strategy"])) or "likeU + 产品类型"
        label_name = normalize_cell_value(record.fields.get(FIELD["label_name"]))
        generation_type = normalize_generation_type(record.fields.get(FIELD["generation_type"]))
        main_prompt = build_main_image_prompt(
            product_truth=truth,
            brand_name=brand_name,
            label_strategy=label_strategy,
            label_override=label_name,
            country=country,
        )
        scene_prompts = build_scene_image_prompts(
            product_truth=truth,
            brand_name=brand_name,
            country=country,
            scene_slots=parse_scene_slots(record.fields.get(FIELD["scene_slots"])),
            scene_preference=normalize_scene_preference(record.fields.get(FIELD["scene_preference"])),
            has_scene_reference=bool(scene_reference_paths),
        )

        main_path = select_generated_file(generated_dir, "main")
        main_qa = qa_generated_image(
            source_image_paths=[str(path) for path in source_paths],
            generated_image_path=str(main_path),
            product_truth=truth,
        )
        main_attachment = client.upload_attachment(main_path)
        main_status, main_summary = decide_final_status(main_qa)

        scene_attachments: List[Dict[str, Any]] = []
        scene_details: List[Dict[str, Any]] = []
        scene_results: List[str] = []
        for scene in scene_prompts:
            scene_id = str(scene.get("image_id") or "").upper()
            scene_path = select_generated_file(generated_dir, scene_id)
            scene_qa = qa_scene_image(
                source_image_paths=[str(path) for path in source_paths],
                generated_image_path=str(scene_path),
                product_truth=truth,
                scene_role=str(scene.get("image_role") or scene_id),
            )
            attachment = client.upload_attachment(scene_path)
            scene_attachments.append(attachment)
            scene_results.append(str(scene_qa.get("result") or "需人工复核"))
            scene_details.append({
                "image_id": scene_id,
                "image_role": scene.get("image_role") or scene_id,
                "local_path": str(scene_path),
                "uploaded": True,
                "attempts": 1,
                "qa_result": scene_qa.get("result"),
                "qa_issues": scene_qa.get("issues", []),
                "qa_summary": scene_qa.get("summary", ""),
                "error": "",
                "recovered_after_upload_failure": True,
            })

        scene_qa_result = summarize_qa_results(scene_results)
        scene_qa_issues = "; ".join(
            f"{item['image_id']}:{', '.join(item.get('qa_issues') or []) or item.get('qa_summary', '')}"
            for item in scene_details
            if item.get("qa_result") not in {"通过", "轻微问题可用"} or item.get("qa_issues")
        )
        scene_result = {
            "attachments": scene_attachments,
            "details": scene_details,
            "qa_result": scene_qa_result,
            "qa_issues": scene_qa_issues,
        }
        scene_status, scene_summary = decide_scene_status(scene_result)
        final_status, final_summary = combine_workflow_status(
            generation_type=generation_type,
            main_status=main_status,
            main_summary=main_summary,
            scene_status=scene_status,
            scene_summary=scene_summary,
        )

        update = build_truth_update(truth)
        update.update({
            FIELD["brand_name"]: brand_name,
            FIELD["label_strategy"]: label_strategy,
            FIELD["label_name"]: label_name or build_label(
                brand_name=brand_name,
                product_type=str(truth.get("product_type_name_en") or "WIG"),
                strategy="仅产品类型",
            ),
            FIELD["truth_json"]: dumps_pretty(truth),
            FIELD["prompt"]: main_prompt,
            FIELD["scene_prompt"]: dumps_pretty(scene_prompts),
            FIELD["main_result"]: [main_attachment],
            FIELD["qa_result"]: main_qa["result"],
            FIELD["qa_issues"]: "; ".join(main_qa.get("issues", [])) or main_qa.get("summary", ""),
            FIELD["scene_result"]: scene_attachments,
            FIELD["scene_qa_result"]: scene_qa_result,
            FIELD["scene_qa_issues"]: scene_qa_issues,
            FIELD["scene_details"]: dumps_pretty(scene_details),
            FIELD["retry_count"]: 0,
            FIELD["status"]: final_status,
            FIELD["final_status"]: f"{final_summary}；已从本地生成结果恢复上传",
            FIELD["last_run_at"]: now_ms(),
        })
        if final_status == STATUS_REVIEW:
            update[FIELD["review_reason"]] = "; ".join(
                item for item in (
                    update.get(FIELD["qa_issues"], ""),
                    scene_qa_issues,
                    final_summary,
                ) if item
            )
        safe_update(client, record.record_id, update)
        print(dumps_pretty({
            "record_id": record.record_id,
            "status": final_status,
            "main_qa": main_qa,
            "scene_qa": scene_qa_result,
            "scene_qa_issues": scene_qa_issues,
            "uploaded": 1 + len(scene_attachments),
        }))
        return 0
    except Exception as exc:
        safe_update(client, record.record_id, {
            FIELD["status"]: STATUS_FAILED,
            FIELD["final_status"]: f"恢复上传失败: {exc}",
            FIELD["last_run_at"]: now_ms(),
        })
        raise


if __name__ == "__main__":
    raise SystemExit(main())
