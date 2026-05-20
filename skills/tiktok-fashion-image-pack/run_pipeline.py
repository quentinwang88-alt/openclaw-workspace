#!/usr/bin/env python3
"""Run the likeU TikTok fashion image-pack MVP from a Feishu bitable."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


SKILL_DIR = Path(__file__).resolve().parent
CORE_DIR = SKILL_DIR / "core"
WORKSPACE_DIR = SKILL_DIR.parents[1]
if str(CORE_DIR) not in sys.path:
    sys.path.insert(0, str(CORE_DIR))
if str(WORKSPACE_DIR) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_DIR))

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from feishu import (  # noqa: E402
    FeishuBitableClient,
    TableRecord,
    extract_attachments,
    normalize_cell_value,
    resolve_client_from_url,
)
from image_generator import generate_main_image  # noqa: E402
from json_utils import dumps_pretty  # noqa: E402
from product_truth import analyze_product_truth, heuristic_product_truth  # noqa: E402
from prompt_builder import build_label, build_main_image_prompt  # noqa: E402
from qa import qa_generated_image, skipped_qa  # noqa: E402


FIELD = {
    "task_id": "任务ID",
    "source_images": "原始图片",
    "country": "国家",
    "category": "类目",
    "generation_type": "生成图类型",
    "status": "生成状态",
    "notes": "备注",
    "override": "人工覆盖要求",
    "brand_name": "品牌名",
    "label_strategy": "主图小字策略",
    "label_name": "主图小字名称",
    "subtype": "商品子类",
    "main_color": "主色",
    "multicolor": "疑似多色",
    "sellable_colors": "售卖颜色",
    "material": "材质",
    "silhouette": "版型",
    "length": "衣长",
    "collar": "领型",
    "closure": "门襟",
    "pockets": "口袋结构",
    "sleeves": "袖型/袖口",
    "hem": "下摆结构",
    "selling_points": "核心卖点",
    "scenes": "适合场景",
    "customer": "目标人群",
    "must_preserve": "不可改动点",
    "must_not_add": "禁止添加元素",
    "template": "推荐首图模板",
    "detail_sequence": "推荐详情图顺序",
    "truth_json": "Product Truth JSON",
    "confidence": "AI识别置信度",
    "review_reason": "需复核原因",
    "main_result": "首图结果",
    "detail_result": "详情图结果",
    "prompt": "生成Prompt",
    "qa_result": "质检结果",
    "qa_issues": "质检问题",
    "retry_count": "重试次数",
    "last_run_at": "最后生成时间",
    "final_status": "最终状态",
}

STATUS_PENDING = "待生成"
STATUS_PROCESSING = "生成中"
STATUS_DONE = "已生成"
STATUS_REVIEW = "需人工复核"
STATUS_FAILED = "失败"
RESUMABLE_REVIEW_REASONS = {"skip-image-generation"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feishu-url", default=os.environ.get("LIKEU_IMAGE_PACK_FEISHU_URL", ""))
    parser.add_argument("--limit", type=int, default=1)
    parser.add_argument("--record-id", action="append", help="Only process selected Feishu record_id")
    parser.add_argument("--dry-run", action="store_true", help="Read and build prompts, but do not write or generate")
    parser.add_argument("--no-vision", action="store_true", help="Use heuristic product truth instead of a vision model")
    parser.add_argument("--skip-image-generation", action="store_true", help="Write/analyze prompt but do not call image generation")
    parser.add_argument("--skip-qa", action="store_true", help="Skip visual QA")
    parser.add_argument("--max-retries", type=int, default=1, help="Retries after QA rejects an image")
    parser.add_argument("--quality", default=os.environ.get("LIKEU_IMAGE_QUALITY", "medium"))
    parser.add_argument("--output-dir", default=str(SKILL_DIR / "output"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.feishu_url:
        raise SystemExit("请提供 --feishu-url 或设置 LIKEU_IMAGE_PACK_FEISHU_URL")

    client, view_id = resolve_client_from_url(args.feishu_url)
    validate_required_fields(client)
    records = select_records(client, view_id=view_id, limit=args.limit, record_ids=args.record_id)
    print(f"待处理记录: {len(records)}")

    summary = {"total": len(records), "done": 0, "review": 0, "failed": 0, "dry_run": args.dry_run}
    run_root = Path(args.output_dir).expanduser().resolve() / time.strftime("%Y%m%d_%H%M%S")

    for record in records:
        try:
            result = process_record(client, record, args=args, run_root=run_root)
            summary[result] += 1
        except Exception as exc:
            summary["failed"] += 1
            print(f"❌ {record.record_id}: {exc}")
            if not args.dry_run:
                safe_update(client, record.record_id, {
                    FIELD["status"]: STATUS_FAILED,
                    FIELD["final_status"]: f"失败: {exc}",
                    FIELD["last_run_at"]: now_ms(),
                })

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["failed"] == 0 else 1


def validate_required_fields(client: FeishuBitableClient) -> None:
    existing = {field.field_name for field in client.list_fields()}
    required = [FIELD["source_images"], FIELD["country"], FIELD["category"], FIELD["status"]]
    missing = [name for name in required if name not in existing]
    if missing:
        raise RuntimeError(f"飞书表缺少必需字段: {', '.join(missing)}")


def select_records(
    client: FeishuBitableClient,
    *,
    view_id: Optional[str],
    limit: Optional[int],
    record_ids: Optional[List[str]],
) -> List[TableRecord]:
    records = client.list_records(limit=None, view_id=view_id)
    selected_ids = set(record_ids or [])
    selected: List[TableRecord] = []
    for record in records:
        if selected_ids and record.record_id not in selected_ids:
            continue
        status = normalize_cell_value(record.fields.get(FIELD["status"]))
        if status == STATUS_PENDING or is_resumable_review_record(record):
            selected.append(record)
        if limit and len(selected) >= limit:
            break
    return selected


def is_resumable_review_record(record: TableRecord) -> bool:
    """Treat workflow-paused review records as runnable, not true manual QA."""
    status = normalize_cell_value(record.fields.get(FIELD["status"]))
    if status != STATUS_REVIEW:
        return False

    review_reason = normalize_cell_value(record.fields.get(FIELD["review_reason"]))
    final_status = normalize_cell_value(record.fields.get(FIELD["final_status"]))
    has_source = bool(extract_attachments(record.fields.get(FIELD["source_images"])))
    has_main_result = bool(extract_attachments(record.fields.get(FIELD["main_result"])))
    is_workflow_pause = (
        review_reason in RESUMABLE_REVIEW_REASONS
        or "skip-image-generation" in final_status
        or "跳过图片生成" in final_status
    )
    return is_workflow_pause and has_source and not has_main_result


def process_record(
    client: FeishuBitableClient,
    record: TableRecord,
    *,
    args: argparse.Namespace,
    run_root: Path,
) -> str:
    print(f"\n▶ 处理 {record.record_id}")
    if not args.dry_run:
        safe_update(client, record.record_id, {FIELD["status"]: STATUS_PROCESSING, FIELD["last_run_at"]: now_ms()})

    record_dir = run_root / record.record_id
    source_dir = record_dir / "source"
    output_dir = record_dir / "generated"
    source_paths = download_source_images(client, record, source_dir)
    if not source_paths:
        raise RuntimeError("原始图片字段没有附件")

    country = normalize_cell_value(record.fields.get(FIELD["country"])) or "TH"
    category = normalize_cell_value(record.fields.get(FIELD["category"])) or "女装上装/外套"
    notes = "\n".join(
        item for item in [
            normalize_cell_value(record.fields.get(FIELD["notes"])),
            normalize_cell_value(record.fields.get(FIELD["override"])),
        ] if item
    )

    if args.no_vision or args.dry_run:
        truth = heuristic_product_truth([str(path) for path in source_paths], category=category)
    else:
        truth = analyze_product_truth(
            image_paths=[str(path) for path in source_paths],
            country=country,
            category=category,
            notes=notes,
        )

    brand_name = normalize_cell_value(record.fields.get(FIELD["brand_name"])) or "likeU"
    label_strategy = normalize_cell_value(record.fields.get(FIELD["label_strategy"])) or "likeU + 产品类型"
    label_name = normalize_cell_value(record.fields.get(FIELD["label_name"]))
    prompt = build_main_image_prompt(
        product_truth=truth,
        brand_name=brand_name,
        label_strategy=label_strategy,
        label_override=label_name,
        country=country,
    )

    base_update = build_truth_update(truth)
    base_update.update({
        FIELD["brand_name"]: brand_name,
        FIELD["label_strategy"]: label_strategy,
        FIELD["label_name"]: label_name or build_label(
            brand_name=brand_name,
            product_type=str(truth.get("product_type_name_en") or "FASHION JACKET"),
            strategy="仅产品类型",
        ),
        FIELD["prompt"]: prompt,
        FIELD["truth_json"]: dumps_pretty(truth),
        FIELD["retry_count"]: 0,
        FIELD["last_run_at"]: now_ms(),
    })

    if args.dry_run:
        print("DRY RUN product_truth:")
        print(dumps_pretty(truth))
        print("DRY RUN prompt preview:")
        print(prompt[:1800])
        return "done"

    if args.skip_image_generation:
        base_update.update({
            FIELD["status"]: STATUS_REVIEW,
            FIELD["final_status"]: "已生成商品事实和 prompt，跳过图片生成",
            FIELD["review_reason"]: "skip-image-generation",
        })
        safe_update(client, record.record_id, base_update)
        return "review"

    generated_path = ""
    qa_result = skipped_qa("QA skipped")
    prompt_for_attempt = prompt
    max_attempts = max(1, int(args.max_retries) + 1)

    for attempt in range(max_attempts):
        generated_paths = generate_main_image(
            task_id=f"{record.record_id}_main_{attempt + 1}",
            prompt=prompt_for_attempt,
            input_image_path=str(source_paths[0]),
            input_image_paths=[str(path) for path in source_paths[:4]],
            output_dir=output_dir,
            quality=args.quality,
        )
        generated_path = generated_paths[0]
        if args.skip_qa:
            qa_result = skipped_qa("QA skipped by CLI")
            break
        qa_result = qa_generated_image(
            source_image_paths=[str(path) for path in source_paths],
            generated_image_path=generated_path,
            product_truth=truth,
        )
        if not qa_result["must_retry"]:
            break
        prompt_for_attempt = (
            f"{prompt}\n\n"
            "The previous generated image failed QA. Fix these issues strictly without changing product truth:\n"
            + "\n".join(f"- {item}" for item in qa_result.get("issues", []))
        )

    attachment = client.upload_attachment(Path(generated_path))
    final_status, final_summary = decide_final_status(qa_result)
    update = dict(base_update)
    update.update({
        FIELD["main_result"]: [attachment],
        FIELD["qa_result"]: qa_result["result"],
        FIELD["qa_issues"]: "; ".join(qa_result.get("issues", [])) or qa_result.get("summary", ""),
        FIELD["retry_count"]: max(0, min(max_attempts - 1, int(args.max_retries))),
        FIELD["status"]: final_status,
        FIELD["final_status"]: final_summary,
        FIELD["last_run_at"]: now_ms(),
    })
    if final_status == STATUS_REVIEW:
        update[FIELD["review_reason"]] = update[FIELD["qa_issues"]] or final_summary
    safe_update(client, record.record_id, update)
    print(f"✅ 写回 {record.record_id}: {final_summary}")
    return "done" if final_status == STATUS_DONE else "review"


def download_source_images(client: FeishuBitableClient, record: TableRecord, source_dir: Path) -> List[Path]:
    attachments = extract_attachments(record.fields.get(FIELD["source_images"]))
    paths: List[Path] = []
    for attachment in attachments[:4]:
        paths.append(client.download_attachment(attachment, source_dir))
    return paths


def build_truth_update(truth: Dict[str, Any]) -> Dict[str, Any]:
    return {
        FIELD["subtype"]: truth.get("subtype"),
        FIELD["main_color"]: truth.get("main_color"),
        FIELD["multicolor"]: bool(truth.get("is_probably_multicolor")),
        FIELD["sellable_colors"]: join_list(truth.get("sellable_colors_observed")),
        FIELD["material"]: truth.get("material"),
        FIELD["silhouette"]: truth.get("silhouette"),
        FIELD["length"]: truth.get("length"),
        FIELD["collar"]: truth.get("collar"),
        FIELD["closure"]: truth.get("closure"),
        FIELD["pockets"]: truth.get("pockets"),
        FIELD["sleeves"]: truth.get("sleeves"),
        FIELD["hem"]: truth.get("hem"),
        FIELD["selling_points"]: join_list(truth.get("core_selling_points")),
        FIELD["scenes"]: join_list(truth.get("recommended_scenes")),
        FIELD["customer"]: truth.get("target_customer"),
        FIELD["must_preserve"]: join_list(truth.get("must_preserve")),
        FIELD["must_not_add"]: join_list(truth.get("must_not_add")),
        FIELD["template"]: truth.get("main_image_template"),
        FIELD["detail_sequence"]: join_list(truth.get("detail_image_sequence")),
        FIELD["confidence"]: truth.get("confidence"),
        FIELD["review_reason"]: join_list(truth.get("review_reasons")),
    }


def decide_final_status(qa_result: Dict[str, Any]) -> (str, str):
    result = qa_result.get("result")
    if result in {"通过", "轻微问题可用", "未质检"}:
        return STATUS_DONE, f"首图生成完成；质检={result}"
    return STATUS_REVIEW, f"首图生成但需要复核；质检={result}"


def safe_update(client: FeishuBitableClient, record_id: str, fields: Dict[str, Any]) -> None:
    clean = {key: value for key, value in fields.items() if key and value is not None}
    client.update_record_fields(record_id, clean)


def join_list(value: Any) -> str:
    if isinstance(value, list):
        return "；".join(str(item).strip() for item in value if str(item).strip())
    return str(value or "").strip()


def now_ms() -> int:
    return int(time.time() * 1000)


if __name__ == "__main__":
    raise SystemExit(main())
