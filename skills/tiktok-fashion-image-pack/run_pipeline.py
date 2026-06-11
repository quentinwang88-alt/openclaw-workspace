#!/usr/bin/env python3
"""Run the likeU TikTok fashion image-pack MVP from a Feishu bitable."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


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
from image_generator import generate_main_image, generate_scene_image  # noqa: E402
from json_utils import dumps_pretty  # noqa: E402
from product_truth import analyze_product_truth, heuristic_product_truth, repair_multicolor_truth_from_sources  # noqa: E402
from prompt_builder import build_label, build_main_image_prompt  # noqa: E402
from qa import qa_generated_image, qa_scene_image, skipped_qa  # noqa: E402
from scene_prompt_builder import build_scene_image_prompts, parse_scene_slots  # noqa: E402
from title_optimizer import generate_title  # noqa: E402
from title_keywords import get_series_info, SUBTYPE_SERIES_MAP  # noqa: E402
from vision_client import VisionJSONClient  # noqa: E402
from circuit_breaker import CircuitBreakerOpen, ModelCircuitBreaker  # noqa: E402
from feedback_processor import (  # noqa: E402
    build_feedback_fix_prompt,
    find_scene_image_path,
    process_feedback_record,
)


FIELD = {
    "task_id": "任务ID",
    "source_images": "原始图片",
    "scene_reference_images": "原始场景参考图",
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
    "scene_result": "场景图结果",
    "prompt": "生成Prompt",
    "scene_prompt": "场景图Prompt",
    "qa_result": "质检结果",
    "qa_issues": "质检问题",
    "scene_qa_result": "场景图质检结果",
    "scene_qa_issues": "场景图质检问题",
    "scene_details": "场景图生成明细",
    "scene_preference": "场景偏好",
    "scene_slots": "场景图槽位",
    "retry_count": "重试次数",
    "last_run_at": "最后生成时间",
    "final_status": "最终状态",
    "title_status": "标题生成状态",
    "original_title": "原标题/供应商标题",
    "tk_title": "TK标题",
    "title_cn_summary": "标题中文摘要",
    "title_keywords": "标题关键词",
    "title_series": "标题系列",
    "title_series_code": "标题系列编码",
    "title_qa_result": "标题质检结果",
    "title_qa_issues": "标题质检问题",
    "title_prompt": "标题生成Prompt",
    "title_last_run": "标题生成时间",
    "title_human_req": "标题人工要求",
    "feedback_target_image": "反馈目标图",
    "feedback_issues": "图片反馈问题",
    "feedback_status": "反馈状态",
    "feedback_fix_result": "反馈修正结果",
    "feedback_fix_result_scene": "反馈修正结果_场景图",
    "feedback_fix_prompt": "反馈修正Prompt",
    "feedback_fix_method": "反馈处理方式",
    "feedback_qa_result": "反馈质检结果",
    "feedback_qa_issues": "反馈质检问题",
}

STATUS_PENDING = "待生成"
STATUS_PROCESSING = "生成中"
STATUS_DONE = "已生成"
STATUS_REVIEW = "需人工复核"
STATUS_FAILED = "失败"
TITLE_STATUS_PENDING = "待生成"
TITLE_STATUS_PROCESSING = "生成中"
TITLE_STATUS_DONE = "已生成"
TITLE_STATUS_REVIEW = "需人工复核"
TITLE_STATUS_FAILED = "失败"
TITLE_STATUS_PAUSED = "暂停"
FEEDBACK_STATUS_PENDING = "待修正"
FEEDBACK_STATUS_PROCESSING = "修正中"
FEEDBACK_STATUS_DONE = "已修正"
FEEDBACK_STATUS_REVIEW = "需人工复核"
FEEDBACK_FIX_METHOD_LOCAL = "局部修正"
FEEDBACK_FIX_METHOD_REGENERATE = "整图重生"
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
    parser.add_argument("--title-only", action="store_true", help="Only run title generation, skip image processing")
    parser.add_argument("--overwrite-title", action="store_true", help="Force regenerate title even if already done")
    parser.add_argument("--run-title", action="store_true", help="(已默认启用) 标题生成在图片 pipeline 后自动检测执行，无需显式加此参数")
    parser.add_argument("--skip-title", action="store_true", help="跳过标题生成（即使标题状态为待生成）")
    parser.add_argument("--feedback-only", action="store_true", help="Only process feedback fix records (反馈状态=待修正)")
    parser.add_argument("--disable-circuit-breaker", action="store_true", help="禁用模型错误熔断")
    parser.add_argument("--model-failure-threshold", type=int, default=3, help="连续模型调用失败多少次后停止本轮")
    parser.add_argument("--record-failure-threshold", type=int, default=2, help="连续记录失败多少条后停止本轮")
    parser.add_argument("--rate-limit-cooldown", type=int, default=120, help="遇到限流/服务繁忙时冷却秒数")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.circuit_breaker = ModelCircuitBreaker(
        model_failure_threshold=args.model_failure_threshold,
        record_failure_threshold=args.record_failure_threshold,
        rate_limit_cooldown_seconds=args.rate_limit_cooldown,
        enabled=not args.disable_circuit_breaker,
    )
    if not args.feishu_url:
        raise SystemExit("请提供 --feishu-url 或设置 LIKEU_IMAGE_PACK_FEISHU_URL")

    client, view_id = resolve_client_from_url(args.feishu_url)
    validate_required_fields(client)

    if args.feedback_only:
        return process_feedback_pipeline(client, view_id, args)

    title_only = args.title_only
    records = select_records(client, view_id=view_id, limit=args.limit, record_ids=args.record_id, title_only=title_only, overwrite_title=args.overwrite_title)
    print(f"待处理记录: {len(records)} (title_only={title_only})")

    summary = {"total": len(records), "done": 0, "review": 0, "failed": 0, "dry_run": args.dry_run}
    run_root = Path(args.output_dir).expanduser().resolve() / time.strftime("%Y%m%d_%H%M%S")

    for record in records:
        try:
            if title_only:
                result = process_title_record(client, record, args=args, run_root=run_root)
                summary["done" if result == "done" else "review"] += 1
            else:
                result = process_record(client, record, args=args, run_root=run_root)
                summary[result] += 1
            args.circuit_breaker.record_successful_record()
        except CircuitBreakerOpen as exc:
            print(f"⛔ 熔断停止: {exc}")
            break
        except Exception as exc:
            summary["failed"] += 1
            print(f"❌ {record.record_id}: {exc}")
            if not args.dry_run:
                safe_update(client, record.record_id, {
                    FIELD["status"]: STATUS_FAILED,
                    FIELD["final_status"]: f"失败: {exc}",
                    FIELD["last_run_at"]: now_ms(),
                })
            try:
                args.circuit_breaker.record_failed_record(exc)
            except CircuitBreakerOpen as breaker_exc:
                print(f"⛔ 熔断停止: {breaker_exc}")
                break

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
    title_only: bool = False,
    overwrite_title: bool = False,
) -> List[TableRecord]:
    records = client.list_records(limit=None, view_id=view_id)
    selected_ids = set(record_ids or [])
    has_explicit_ids = bool(selected_ids)
    limit = None if has_explicit_ids else limit
    selected: List[TableRecord] = []
    for record in records:
        if selected_ids and record.record_id not in selected_ids:
            continue
        if has_explicit_ids:
            selected.append(record)
            continue
        if title_only:
            if _is_title_candidate(record, overwrite=overwrite_title):
                selected.append(record)
        else:
            status = normalize_cell_value(record.fields.get(FIELD["status"]))
            if status == STATUS_PENDING or is_resumable_review_record(record):
                selected.append(record)
        if limit and len(selected) >= limit:
            break
    return selected


def _is_title_candidate(record: TableRecord, overwrite: bool = False) -> bool:
    title_status = normalize_cell_value(record.fields.get(FIELD["title_status"]))
    if title_status == TITLE_STATUS_PENDING:
        return True
    if title_status == TITLE_STATUS_PROCESSING:
        return True
    if title_status == TITLE_STATUS_DONE and overwrite:
        return True
    has_truth = bool(normalize_cell_value(record.fields.get(FIELD["truth_json"])))
    if title_status == TITLE_STATUS_REVIEW and has_truth:
        return True
    return False


def is_resumable_review_record(record: TableRecord) -> bool:
    """Treat workflow-paused review records as runnable, not true manual QA."""
    status = normalize_cell_value(record.fields.get(FIELD["status"]))
    if status != STATUS_REVIEW:
        return False

    review_reason = normalize_cell_value(record.fields.get(FIELD["review_reason"]))
    final_status = normalize_cell_value(record.fields.get(FIELD["final_status"]))
    has_source = bool(extract_attachments(record.fields.get(FIELD["source_images"])))
    has_main_result = bool(extract_attachments(record.fields.get(FIELD["main_result"])))
    has_scene_result = bool(extract_attachments(record.fields.get(FIELD["scene_result"])))
    run_main, run_scene = generation_targets(normalize_generation_type(record.fields.get(FIELD["generation_type"])))
    missing_requested_output = (run_main and not has_main_result) or (run_scene and not has_scene_result)
    is_workflow_pause = (
        review_reason in RESUMABLE_REVIEW_REASONS
        or "skip-image-generation" in final_status
        or "跳过图片生成" in final_status
    )
    return is_workflow_pause and has_source and missing_requested_output


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
    scene_reference_paths = download_scene_reference_images(client, record, record_dir / "scene_reference")

    country = normalize_cell_value(record.fields.get(FIELD["country"])) or "TH"
    category = normalize_cell_value(record.fields.get(FIELD["category"])) or "女装上装/外套"
    notes = "\n".join(
        item for item in [
            normalize_cell_value(record.fields.get(FIELD["notes"])),
            normalize_cell_value(record.fields.get(FIELD["override"])),
        ] if item
    )

    existing_truth_raw = normalize_cell_value(record.fields.get(FIELD["truth_json"]))
    review_reason = normalize_cell_value(record.fields.get(FIELD["review_reason"]))
    should_reuse_truth = bool(existing_truth_raw) and review_reason == "skip-image-generation"
    if should_reuse_truth:
        try:
            truth = json.loads(existing_truth_raw)
            print("  复用已有 Product Truth JSON（skip-image-generation 续跑）")
        except json.JSONDecodeError:
            should_reuse_truth = False

    if should_reuse_truth:
        pass
    elif args.no_vision or args.dry_run:
        truth = heuristic_product_truth([str(path) for path in source_paths], category=category)
    else:
        try:
            args.circuit_breaker.before_model_call()
            truth = analyze_product_truth(
                image_paths=[str(path) for path in source_paths],
                country=country,
                category=category,
                notes=notes,
            )
            args.circuit_breaker.record_model_success()
        except Exception as exc:
            args.circuit_breaker.record_model_failure(exc)
            raise
    truth = repair_multicolor_truth_from_sources(truth, [str(path) for path in source_paths])

    brand_name = normalize_cell_value(record.fields.get(FIELD["brand_name"])) or "likeU"
    label_strategy = normalize_cell_value(record.fields.get(FIELD["label_strategy"])) or "likeU + 产品类型"
    label_name = normalize_cell_value(record.fields.get(FIELD["label_name"]))
    generation_type = normalize_generation_type(record.fields.get(FIELD["generation_type"]))
    run_main, run_scene = generation_targets(generation_type)
    scene_slots = parse_scene_slots(record.fields.get(FIELD["scene_slots"]))
    scene_preference = normalize_scene_preference(record.fields.get(FIELD["scene_preference"]))
    prompt = ""
    scene_prompts: List[Dict[str, Any]] = []
    if run_main:
        prompt = build_main_image_prompt(
            product_truth=truth,
            brand_name=brand_name,
            label_strategy=label_strategy,
            label_override=label_name,
            country=country,
        )
    if run_scene:
        scene_prompts = build_scene_image_prompts(
            product_truth=truth,
            brand_name=brand_name,
            country=country,
            scene_slots=scene_slots,
            scene_preference=scene_preference,
            has_scene_reference=bool(scene_reference_paths),
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
        FIELD["truth_json"]: dumps_pretty(truth),
        FIELD["retry_count"]: 0,
        FIELD["last_run_at"]: now_ms(),
    })
    if run_main:
        base_update[FIELD["prompt"]] = prompt
    if run_scene:
        base_update[FIELD["scene_prompt"]] = dumps_pretty(scene_prompts)

    if args.dry_run:
        print("DRY RUN product_truth:")
        print(dumps_pretty(truth))
        if run_main:
            print("DRY RUN main prompt preview:")
            print(prompt[:1800])
        if run_scene:
            print("DRY RUN scene prompt preview:")
            print(scene_prompts[0]["prompt"][:1800])
        return "done"

    if args.skip_image_generation:
        base_update.update({
            FIELD["status"]: STATUS_REVIEW,
            FIELD["final_status"]: f"已生成商品事实和 prompt，跳过图片生成；类型={generation_type}",
            FIELD["review_reason"]: "skip-image-generation",
        })
        safe_update(client, record.record_id, base_update)
        return "review"

    update = dict(base_update)
    main_status = STATUS_DONE
    main_summary = "未生成首图"
    main_qa = skipped_qa("Main image not requested")
    retry_count = 0
    if run_main:
        main_attachment, main_qa, retry_count = process_main_image(
            client=client,
            record_id=record.record_id,
            prompt=prompt,
            source_paths=source_paths,
            output_dir=output_dir,
            product_truth=truth,
            args=args,
        )
        main_status, main_summary = decide_final_status(main_qa)
        update.update({
            FIELD["main_result"]: [main_attachment],
            FIELD["qa_result"]: main_qa["result"],
            FIELD["qa_issues"]: "; ".join(main_qa.get("issues", [])) or main_qa.get("summary", ""),
            FIELD["retry_count"]: retry_count,
        })

    scene_status = STATUS_DONE
    scene_summary = "未生成场景图"
    if run_scene:
        scene_result = process_scene_images(
            client=client,
            record_id=record.record_id,
            scene_prompts=scene_prompts,
            source_paths=source_paths,
            scene_reference_paths=scene_reference_paths,
            output_dir=output_dir,
            product_truth=truth,
            args=args,
        )
        scene_status, scene_summary = decide_scene_status(scene_result)
        update.update({
            FIELD["scene_result"]: scene_result["attachments"],
            FIELD["scene_qa_result"]: scene_result["qa_result"],
            FIELD["scene_qa_issues"]: scene_result["qa_issues"],
            FIELD["scene_details"]: dumps_pretty(scene_result["details"]),
        })

    final_status, final_summary = combine_workflow_status(
        generation_type=generation_type,
        main_status=main_status,
        main_summary=main_summary,
        scene_status=scene_status,
        scene_summary=scene_summary,
    )
    update.update({
        FIELD["status"]: final_status,
        FIELD["final_status"]: final_summary,
        FIELD["last_run_at"]: now_ms(),
    })
    if final_status == STATUS_REVIEW:
        update[FIELD["review_reason"]] = "; ".join(
            item for item in [
                update.get(FIELD["qa_issues"], ""),
                update.get(FIELD["scene_qa_issues"], ""),
                final_summary,
            ] if item
        )

    if not args.skip_title:
        title_result = _run_title_inline(
            client=client,
            record=record,
            product_truth=truth,
            original_title=normalize_cell_value(record.fields.get(FIELD["original_title"])) or "",
            human_req=normalize_cell_value(record.fields.get(FIELD["title_human_req"])) or "",
            args=args,
        )
        if title_result:
            update.update(title_result)

    safe_update(client, record.record_id, update)
    print(f"✅ 写回 {record.record_id}: {final_summary}")
    return "done" if final_status == STATUS_DONE else "review"


def _run_title_inline(
    *,
    client: FeishuBitableClient,
    record: TableRecord,
    product_truth: Dict[str, Any],
    original_title: str = "",
    human_req: str = "",
    args: argparse.Namespace,
) -> Optional[Dict[str, Any]]:
    title_status = normalize_cell_value(record.fields.get(FIELD["title_status"]))
    if title_status == TITLE_STATUS_DONE and not args.overwrite_title:
        return None
    if title_status == TITLE_STATUS_PAUSED:
        return None
    if not title_status and not args.overwrite_title:
        return None

    subtype = str(product_truth.get("subtype") or "unknown_womens_top")
    category = normalize_cell_value(record.fields.get(FIELD["category"])) or "女装上装/外套"
    country = normalize_cell_value(record.fields.get(FIELD["country"])) or "TH"
    try:
        args.circuit_breaker.before_model_call()
        result = generate_title(
            product_truth=product_truth,
            subtype=subtype,
            original_title=original_title,
            human_requirement=human_req,
            category=category,
            country=country,
        )
        args.circuit_breaker.record_model_success()
    except Exception as exc:
        args.circuit_breaker.record_model_failure(exc)
        raise
    series = get_series_info(subtype, category=category, country=country)
    series_code = _make_series_code(subtype, record.record_id, category=category, country=country)
    qa_r = result.get("qa_result", "")
    normalized = result.get("normalized_title", result["tk_title"])
    compliance_risk = result.get("compliance_risk", False)

    if compliance_risk:
        title_status = TITLE_STATUS_REVIEW
    elif qa_r in ("通过", "轻微问题可用"):
        title_status = TITLE_STATUS_DONE
    else:
        title_status = TITLE_STATUS_REVIEW

    title_update = {
        FIELD["tk_title"]: normalized,
        FIELD["title_cn_summary"]: result.get("cn_summary", ""),
        FIELD["title_keywords"]: result.get("keywords_used", ""),
        FIELD["title_series"]: series["series_name_cn"],
        FIELD["title_series_code"]: series_code,
        FIELD["title_qa_result"]: qa_r,
        FIELD["title_qa_issues"]: "; ".join(result.get("qa_issues", [])) or result.get("qa_summary", ""),
        FIELD["title_prompt"]: result.get("prompt", ""),
        FIELD["title_last_run"]: now_ms(),
        FIELD["title_status"]: title_status,
    }
    print(f"  📝 标题已生成: {normalized[:60]}... (QA: {qa_r})")
    return title_update


def normalize_generation_type(raw_value: Any) -> str:
    value = normalize_cell_value(raw_value) or "只首图"
    aliases = {
        "主图": "只首图",
        "首图": "只首图",
        "场景图": "只场景图",
        "补场景图": "只场景图",
        "首图+场景": "首图+场景图",
        "主图+场景图": "首图+场景图",
        "全套": "全套图包",
        "图包": "全套图包",
    }
    return aliases.get(value, value)


def generation_targets(generation_type: str) -> Tuple[bool, bool]:
    if generation_type == "只场景图":
        return False, True
    if generation_type in {"首图+场景图", "全套图包"}:
        return True, True
    if generation_type == "首图+详情图":
        return True, False
    return True, False


def normalize_scene_preference(raw_value: Any) -> str:
    value = normalize_cell_value(raw_value)
    if not value or value == "自动匹配":
        return ""
    return value


def process_main_image(
    *,
    client: FeishuBitableClient,
    record_id: str,
    prompt: str,
    source_paths: List[Path],
    output_dir: Path,
    product_truth: Dict[str, Any],
    args: argparse.Namespace,
) -> Tuple[Dict[str, Any], Dict[str, Any], int]:
    generated_path = ""
    qa_result = skipped_qa("QA skipped")
    prompt_for_attempt = prompt
    max_attempts = max(1, int(args.max_retries) + 1)
    attempts_used = 0

    for attempt in range(max_attempts):
        attempts_used = attempt + 1
        try:
            args.circuit_breaker.before_model_call()
            generated_paths = generate_main_image(
                task_id=f"{record_id}_main_{attempt + 1}",
                prompt=prompt_for_attempt,
                input_image_path=str(source_paths[0]),
                input_image_paths=[str(path) for path in source_paths[:4]],
                output_dir=output_dir,
                quality=args.quality,
            )
            args.circuit_breaker.record_model_success()
        except Exception as exc:
            args.circuit_breaker.record_model_failure(exc)
            raise
        generated_path = generated_paths[0]
        if args.skip_qa:
            qa_result = skipped_qa("QA skipped by CLI")
            break
        try:
            qa_result = qa_generated_image(
                source_image_paths=[str(path) for path in source_paths],
                generated_image_path=generated_path,
                product_truth=product_truth,
            )
        except Exception as exc:
            args.circuit_breaker.record_model_failure(exc)
            raise
        if not qa_result["must_retry"]:
            break
        prompt_for_attempt = (
            f"{prompt}\n\n"
            "The previous generated image failed QA. Fix these issues strictly without changing product truth:\n"
            + "\n".join(f"- {item}" for item in qa_result.get("issues", []))
        )

    attachment = client.upload_attachment(Path(generated_path))
    return attachment, qa_result, max(0, attempts_used - 1)


def process_scene_images(
    *,
    client: FeishuBitableClient,
    record_id: str,
    scene_prompts: List[Dict[str, Any]],
    source_paths: List[Path],
    scene_reference_paths: List[Path],
    output_dir: Path,
    product_truth: Dict[str, Any],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    attachments: List[Dict[str, Any]] = []
    details: List[Dict[str, Any]] = []
    accepted_results: List[str] = []
    max_attempts = max(1, int(args.max_retries) + 1)

    for scene in scene_prompts:
        scene_id = str(scene.get("image_id") or "S").upper()
        prompt = str(scene.get("prompt") or "")
        role = str(scene.get("image_role") or scene_id)
        prompt_for_attempt = prompt
        generated_path = ""
        qa_result = skipped_qa("QA skipped")
        attempts_used = 0
        error_message = ""

        for attempt in range(max_attempts):
            attempts_used = attempt + 1
            try:
                args.circuit_breaker.before_model_call()
                generated_paths = generate_scene_image(
                    task_id=f"{record_id}_{scene_id}_{attempt + 1}",
                    prompt=prompt_for_attempt,
                    input_image_path=str(source_paths[0]),
                    input_image_paths=[str(path) for path in [*source_paths[:4], *scene_reference_paths[:4]]],
                    output_dir=output_dir,
                    quality=args.quality,
                )
                args.circuit_breaker.record_model_success()
                generated_path = generated_paths[0]
                if args.skip_qa:
                    qa_result = skipped_qa("QA skipped by CLI")
                    break
                qa_result = qa_scene_image(
                    source_image_paths=[str(path) for path in source_paths],
                    generated_image_path=generated_path,
                    product_truth=product_truth,
                    scene_role=role,
                )
                if not qa_result["must_retry"]:
                    break
                prompt_for_attempt = (
                    f"{prompt}\n\n"
                    "The previous generated scene image failed QA. Fix these issues strictly without changing product truth:\n"
                    + "\n".join(f"- {item}" for item in qa_result.get("issues", []))
                )
            except CircuitBreakerOpen:
                raise
            except Exception as exc:
                args.circuit_breaker.record_model_failure(exc)
                error_message = str(exc)
                qa_result = {
                    "result": "需人工复核",
                    "score": 0.0,
                    "issues": [error_message],
                    "must_retry": False,
                    "summary": f"{scene_id} generation failed",
                }
                break

        attachment: Optional[Dict[str, Any]] = None
        if generated_path:
            attachment = client.upload_attachment(Path(generated_path))
            attachments.append(attachment)
        accepted_results.append(str(qa_result.get("result") or "需人工复核"))
        details.append({
            "image_id": scene_id,
            "image_role": role,
            "local_path": generated_path,
            "uploaded": bool(attachment),
            "attempts": attempts_used,
            "qa_result": qa_result.get("result"),
            "qa_issues": qa_result.get("issues", []),
            "qa_summary": qa_result.get("summary", ""),
            "error": error_message,
        })

    qa_issue_text = "; ".join(
        f"{item['image_id']}:{', '.join(item.get('qa_issues') or []) or item.get('qa_summary', '')}"
        for item in details
        if item.get("qa_result") not in {"通过", "轻微问题可用", "未质检"}
        or item.get("qa_issues")
    )
    return {
        "attachments": attachments,
        "details": details,
        "qa_result": summarize_qa_results(accepted_results),
        "qa_issues": qa_issue_text,
    }


def summarize_qa_results(results: List[str]) -> str:
    if not results:
        return "需人工复核"
    if any(result == "不通过" for result in results):
        return "不通过"
    if any(result == "需人工复核" for result in results):
        return "需人工复核"
    if any(result == "轻微问题可用" for result in results):
        return "轻微问题可用"
    if all(result == "未质检" for result in results):
        return "未质检"
    return "通过"


def decide_scene_status(scene_result: Dict[str, Any]) -> Tuple[str, str]:
    result = scene_result.get("qa_result")
    attachments = scene_result.get("attachments") or []
    details = scene_result.get("details") or []
    requested_count = len(details)
    generated_count = len(attachments)
    if generated_count == requested_count and result in {"通过", "轻微问题可用", "未质检"}:
        return STATUS_DONE, f"场景图生成完成 {generated_count}/{requested_count}；质检={result}"
    if generated_count:
        return STATUS_REVIEW, f"场景图部分完成 {generated_count}/{requested_count}；质检={result}"
    return STATUS_REVIEW, f"场景图未成功生成；质检={result}"


def combine_workflow_status(
    *,
    generation_type: str,
    main_status: str,
    main_summary: str,
    scene_status: str,
    scene_summary: str,
) -> Tuple[str, str]:
    summaries = []
    if generation_type != "只场景图":
        summaries.append(main_summary)
    if generation_type in {"只场景图", "首图+场景图", "全套图包"}:
        summaries.append(scene_summary)
    status = STATUS_REVIEW if STATUS_REVIEW in {main_status, scene_status} else STATUS_DONE
    return status, "；".join(item for item in summaries if item)


def process_title_record(
    client: FeishuBitableClient,
    record: TableRecord,
    *,
    args: argparse.Namespace,
    run_root: Path,
) -> str:
    print(f"\n📝 标题生成 {record.record_id}")
    original_title = normalize_cell_value(record.fields.get(FIELD["original_title"]))
    if not original_title:
        original_title = normalize_cell_value(record.fields.get(FIELD["brand_name"])) or ""
    human_req = normalize_cell_value(record.fields.get(FIELD["title_human_req"])) or ""
    raw_category = normalize_cell_value(record.fields.get(FIELD["category"]))
    raw_country = normalize_cell_value(record.fields.get(FIELD["country"]))
    category, country = infer_title_context_from_text(
        original_title=original_title,
        category=raw_category,
        country=raw_country,
    )

    product_truth_raw = normalize_cell_value(record.fields.get(FIELD["truth_json"]))
    fallback_truth_used = False
    if not product_truth_raw:
        if not original_title:
            print(f"  ⚠️ 缺少 Product Truth JSON 且无原标题，跳过")
            return "failed"
        product_truth = build_title_fallback_truth(
            original_title=original_title,
            category=category,
            country=country,
        )
        fallback_truth_used = True
        print(f"  ℹ️ 缺少 Product Truth JSON，已基于原标题生成轻量商品事实: {category}/{country}/{product_truth.get('subtype')}")
    else:
        try:
            product_truth = json.loads(product_truth_raw)
        except json.JSONDecodeError:
            if not original_title:
                print(f"  ❌ Product Truth JSON 解析失败，且无原标题可兜底")
                return "failed"
            product_truth = build_title_fallback_truth(
                original_title=original_title,
                category=category,
                country=country,
            )
            fallback_truth_used = True
            print(f"  ⚠️ Product Truth JSON 解析失败，已基于原标题生成轻量商品事实: {category}/{country}/{product_truth.get('subtype')}")

    title_status = normalize_cell_value(record.fields.get(FIELD["title_status"]))
    if title_status == TITLE_STATUS_DONE and not args.overwrite_title:
        print(f"  标题已生成，跳过（使用 --overwrite-title 强制重新生成）")
        return "done"
    if title_status == TITLE_STATUS_PROCESSING:
        print(f"  检测到残留的生成中状态，恢复执行...")

    if not args.dry_run:
        safe_update(client, record.record_id, {
            FIELD["title_status"]: TITLE_STATUS_PROCESSING,
            FIELD["title_last_run"]: now_ms(),
        })

    subtype = str(product_truth.get("subtype") or "unknown_womens_top")

    result = generate_title(
        product_truth=product_truth,
        subtype=subtype,
        original_title=original_title,
        human_requirement=human_req,
        category=category,
        country=country,
    )

    series = get_series_info(subtype, category=category, country=country)
    series_name = series["series_name_cn"]
    series_code = _make_series_code(subtype, record.record_id, category=category, country=country)
    normalized = result.get("normalized_title", result["tk_title"])
    compliance_risk = result.get("compliance_risk", False)

    update = {
        FIELD["tk_title"]: normalized,
        FIELD["title_cn_summary"]: result.get("cn_summary", ""),
        FIELD["title_keywords"]: result.get("keywords_used", ""),
        FIELD["title_series"]: series_name,
        FIELD["title_series_code"]: series_code,
        FIELD["title_qa_result"]: result.get("qa_result", ""),
        FIELD["title_qa_issues"]: "; ".join(result.get("qa_issues", [])) or result.get("qa_summary", ""),
        FIELD["title_prompt"]: result.get("prompt", ""),
        FIELD["title_last_run"]: now_ms(),
    }
    if fallback_truth_used:
        update[FIELD["truth_json"]] = dumps_pretty(product_truth)
        update[FIELD["category"]] = category
        update[FIELD["country"]] = country
        update.update(build_truth_update(product_truth))

    qa_result = result.get("qa_result", "")
    if compliance_risk:
        update[FIELD["title_status"]] = TITLE_STATUS_REVIEW
        final_status = "标题需人工复核（材质/功能不合规）"
    elif qa_result in ("通过", "轻微问题可用"):
        update[FIELD["title_status"]] = TITLE_STATUS_DONE
        final_status = "标题已生成"
    elif qa_result == "不通过":
        update[FIELD["title_status"]] = TITLE_STATUS_REVIEW
        final_status = "标题需人工复核（质检不通过）"
    else:
        update[FIELD["title_status"]] = TITLE_STATUS_REVIEW
        final_status = "标题需人工复核"

    if args.dry_run:
        print("DRY RUN title result:")
        print(f"  TK标题(原始): {result['tk_title']}")
        print(f"  TK标题(修正): {normalized}")
        print(f"  系列: {series_name} / {series_code}")
        print(f"  质检: {qa_result}")
        return "done"

    safe_update(client, record.record_id, update)
    print(f"  ✅ 标题写入: {final_status}")
    print(f"  TK标题: {normalized[:80]}...")
    return "done" if qa_result in ("通过", "轻微问题可用") else "review"


def infer_title_context_from_text(
    *,
    original_title: str,
    category: str = "",
    country: str = "",
) -> Tuple[str, str]:
    resolved_country = (country or "").strip().upper()
    text = original_title or ""
    if not resolved_country:
        if re.search(r"[\u0E00-\u0E7F]", text):
            resolved_country = "TH"
        elif re.search(r"[À-ỹĐđ]", text) or any(token in text.lower() for token in ("kẹp", "băng đô", "dây buộc", "scrunchie")):
            resolved_country = "VN"
        else:
            resolved_country = "TH"

    resolved_category = (category or "").strip()
    if not resolved_category:
        lower = text.lower()
        hair_terms = (
            "กิ๊บ", "ที่คาดผม", "ยางมัดผม", "ยางรัดผม", "โบว์ติดผม",
            "เครื่องประดับผม", "อุปกรณ์ตกแต่งผม", "kẹp tóc", "kẹp càng cua",
            "kẹp mái", "băng đô", "dây buộc tóc", "thun cột tóc", "nơ tóc",
            "scrunchie", "scrunchies", "发夹", "发箍", "发圈", "头饰",
        )
        resolved_category = "发饰" if any(term in lower or term in text for term in hair_terms) else "女装上装/外套"

    return resolved_category, resolved_country


def build_title_fallback_truth(
    *,
    original_title: str,
    category: str,
    country: str,
) -> Dict[str, Any]:
    truth = heuristic_product_truth([original_title or "title_only"], category=category)
    if str(truth.get("category") or "").strip().lower() == "hair_accessory":
        enrich_hair_accessory_truth_from_title(truth, original_title, country)
    truth["source_image_type"] = "title_only"
    truth["confidence"] = min(float(truth.get("confidence") or 0.35), 0.45)
    reasons = truth.get("review_reasons") if isinstance(truth.get("review_reasons"), list) else []
    reasons = [str(item).strip() for item in reasons if str(item).strip()]
    reasons.append("title-only fallback; no Product Truth JSON or source image")
    truth["review_reasons"] = []
    for item in reasons:
        if item not in truth["review_reasons"]:
            truth["review_reasons"].append(item)
    truth["target_customer"] = truth.get("target_customer") or ("Thai shoppers" if country == "TH" else "Vietnam shoppers")
    return truth


def enrich_hair_accessory_truth_from_title(truth: Dict[str, Any], original_title: str, country: str) -> None:
    """Extract lightweight differentiators from marketplace titles for title-only hair accessory tasks."""
    text = original_title or ""
    lower = text.lower()
    subtype = str(truth.get("subtype") or "")

    if subtype == "unknown_hair_accessory":
        truth["subtype"] = infer_hair_subtype_from_title(text)
        subtype = str(truth.get("subtype") or "")
        truth["product_type_name_en"] = {
            "claw_clip": "CLAW CLIP",
            "hair_clip": "HAIR CLIP",
            "hair_bow": "HAIR BOW",
            "headband": "HEADBAND",
            "scrunchie": "SCRUNCHIE",
            "hair_tie": "HAIR TIE",
            "hair_pin": "HAIR PIN",
        }.get(subtype, "HAIR ACCESSORY")

    colors = extract_hair_title_terms(text, HAIR_TITLE_COLOR_TERMS)
    patterns = extract_hair_title_terms(text, HAIR_TITLE_PATTERN_TERMS)
    shapes = extract_hair_title_terms(text, HAIR_TITLE_SHAPE_TERMS)
    structures = extract_hair_title_terms(text, HAIR_TITLE_STRUCTURE_TERMS)
    use_cases = extract_hair_title_terms(text, HAIR_TITLE_USE_TERMS)
    style_terms = extract_hair_title_terms(text, HAIR_TITLE_STYLE_TERMS)

    if colors:
        truth["main_color"] = colors[0]
        truth["sellable_colors_observed"] = colors[:3]
        truth["is_probably_multicolor"] = len(colors[:3]) > 1
    if patterns or shapes:
        truth["decorative_elements"] = ", ".join([*patterns, *shapes])
    elif structures:
        truth["decorative_elements"] = truth.get("decorative_elements") or "unknown"

    if structures:
        truth["grip_structure"] = ", ".join(structures)
    if any(term in lower or term in text for term in ("ใหญ่", "ขนาดใหญ่", "big", "large", "to", "cỡ lớn")):
        truth["size_scale"] = "large"
    elif any(term in lower or term in text for term in ("เล็ก", "ขนาดเล็ก", "small", "mini", "cỡ nhỏ")):
        truth["size_scale"] = "small"

    selling_points = []
    for item in [*colors[:1], *patterns[:1], *shapes[:1], *structures[:2], *use_cases[:2], *style_terms[:1]]:
        if item and item not in selling_points:
            selling_points.append(item)
    if selling_points:
        truth["core_selling_points"] = selling_points

    preserve = truth.get("must_preserve") if isinstance(truth.get("must_preserve"), list) else []
    for item in [*colors[:2], *patterns[:2], *shapes[:2], *structures[:2]]:
        if item and item not in preserve:
            preserve.append(item)
    if preserve:
        truth["must_preserve"] = preserve

    avoid_generic = [
        "avoid generic repeated title if original title contains color, pattern, shape, or structure",
        "preserve at least one title-only differentiator in TK title",
    ]
    reasons = truth.get("review_reasons") if isinstance(truth.get("review_reasons"), list) else []
    for item in avoid_generic:
        if item not in reasons:
            reasons.append(item)
    truth["review_reasons"] = reasons


def infer_hair_subtype_from_title(text: str) -> str:
    lower = (text or "").lower()
    if any(term in lower or term in text for term in ("ทรงฉลาม", "กิ๊บหนีบ", "claw", "càng cua", "鲨鱼夹")):
        return "claw_clip"
    if any(term in lower or term in text for term in ("โบว์", "bow", "nơ", "蝴蝶结")):
        return "hair_bow"
    if any(term in lower or term in text for term in ("ที่คาดผม", "headband", "băng đô", "发箍")):
        return "headband"
    if any(term in lower or term in text for term in ("scrunchie", "ยางมัดผม", "dây buộc tóc", "大肠")):
        return "scrunchie"
    if any(term in lower or term in text for term in ("ยางรัดผม", "hair tie", "thun cột tóc", "发圈")):
        return "hair_tie"
    if any(term in lower or term in text for term in ("กิ๊บเป๊าะแป๊ะ", "kẹp mái", "pin", "边夹")):
        return "hair_pin"
    if any(term in lower or term in text for term in ("กิ๊บ", "kẹp tóc", "发夹")):
        return "hair_clip"
    return "unknown_hair_accessory"


def extract_hair_title_terms(text: str, terms: Tuple[str, ...]) -> List[str]:
    lower = (text or "").lower()
    found: List[str] = []
    for term in sorted(terms, key=len, reverse=True):
        if term.lower() in lower or term in text:
            if term not in found and not any(term in existing for existing in found):
                found.append(term)
    return found


HAIR_TITLE_COLOR_TERMS = (
    "สีดำ", "สีขาว", "สีครีม", "สีเบจ", "สีน้ำตาล", "สีชมพู", "สีแดง", "สีเหลือง",
    "สีเขียว", "สีฟ้า", "สีม่วง", "สีเงิน", "สีทอง", "สีมัสตาร์ด", "เหลืองเขียวมัสตาร์ด",
    "ดำ", "ขาว", "ครีม", "เบจ", "น้ำตาล", "ชมพู", "แดง", "เหลือง", "เขียว", "มัสตาร์ด",
    "đen", "trắng", "kem", "be", "nâu", "hồng", "đỏ", "vàng", "xanh", "tím",
)
HAIR_TITLE_PATTERN_TERMS = (
    "ลายจุด", "ลายดอก", "ลายหินอ่อน", "ลายเสือ", "ลายตาราง", "สีพื้น",
    "chấm bi", "hoa", "vân đá", "da báo", "kẻ caro", "màu trơn",
)
HAIR_TITLE_SHAPE_TERMS = (
    "ทรงหัวใจ", "รูปหัวใจ", "ทรงฉลาม", "โบว์ใหญ่", "โบว์เล็ก", "ขอบสูง", "มินิ",
    "hình tim", "nơ to", "nơ nhỏ", "mini",
)
HAIR_TITLE_STRUCTURE_TERMS = (
    "แม่เหล็ก", "แบบแม่เหล็ก", "ฟันหนีบ", "ฟันหนีบแข็งแรง", "กิ๊บเป๊าะแป๊ะ",
    "อะคริลิก", "พลาสติก", "ผ้าซาติน", "ผ้ากำมะหยี่", "ประดับมุก", "ประดับคริสตัล",
    "nam châm", "răng kẹp", "nhựa acrylic", "nhựa", "vải satin", "nhung", "đính ngọc trai", "đính đá",
)
HAIR_TITLE_USE_TERMS = (
    "รวบผม", "จัดทรงผม", "ติดหน้าม้า", "ผมด้านข้าง", "มวยผม", "หางม้า", "ผมหนา",
    "búi tóc", "tóc mái", "tóc dày", "buộc tóc", "giữ tóc",
)
HAIR_TITLE_STYLE_TERMS = (
    "สไตล์เกาหลี", "มินิมอล", "ลุคหวาน", "น่ารัก", "สดใส", "หรูหรา", "เรียบง่าย",
    "Hàn Quốc", "tối giản", "xinh xắn", "tiểu thư", "sang nhẹ",
)


def _make_series_code(
    subtype: str,
    record_id: str = "",
    *,
    category: str = "女装上装/外套",
    country: str = "TH",
) -> str:
    series = get_series_info(subtype, category=category, country=country)
    prefix = series.get("series_code_prefix") or "U"
    suffix = abs(hash(record_id)) % 900 + 100 if record_id else 200
    return f"{prefix}{suffix}"


def download_source_images(client: FeishuBitableClient, record: TableRecord, source_dir: Path) -> List[Path]:
    attachments = extract_attachments(record.fields.get(FIELD["source_images"]))
    paths: List[Path] = []
    for attachment in attachments[:4]:
        paths.append(client.download_attachment(attachment, source_dir))
    return paths


def download_scene_reference_images(client: FeishuBitableClient, record: TableRecord, output_dir: Path) -> List[Path]:
    attachments = extract_attachments(record.fields.get(FIELD["scene_reference_images"]))
    paths: List[Path] = []
    for attachment in attachments[:4]:
        paths.append(client.download_attachment(attachment, output_dir))
    return paths


def build_truth_update(truth: Dict[str, Any]) -> Dict[str, Any]:
    if str(truth.get("category") or "").strip().lower() in {"hair_accessory", "hair_accessories", "发饰"}:
        return {
            FIELD["subtype"]: truth.get("subtype"),
            FIELD["main_color"]: truth.get("main_color"),
            FIELD["multicolor"]: bool(truth.get("is_probably_multicolor")),
            FIELD["sellable_colors"]: join_list(truth.get("sellable_colors_observed")),
            FIELD["material"]: truth.get("material"),
            FIELD["silhouette"]: truth.get("size_scale"),
            FIELD["length"]: truth.get("wearing_position"),
            FIELD["collar"]: truth.get("decorative_elements"),
            FIELD["closure"]: truth.get("grip_structure"),
            FIELD["pockets"]: truth.get("pack_count"),
            FIELD["sleeves"]: "",
            FIELD["hem"]: "",
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


def process_feedback_pipeline(
    client: FeishuBitableClient,
    view_id: Optional[str],
    args: argparse.Namespace,
) -> int:
    records = select_feedback_records(client, view_id=view_id, limit=args.limit, record_ids=args.record_id)
    print(f"待处理反馈记录: {len(records)}")

    summary = {"total": len(records), "done": 0, "review": 0, "failed": 0, "dry_run": args.dry_run}
    run_root = Path(args.output_dir).expanduser().resolve() / time.strftime("%Y%m%d_%H%M%S")

    for record in records:
        try:
            result = process_feedback_record(client, record, args=args, run_root=run_root)
            summary[result] += 1
            args.circuit_breaker.record_successful_record()
        except CircuitBreakerOpen as exc:
            print(f"⛔ 熔断停止: {exc}")
            break
        except Exception as exc:
            summary["failed"] += 1
            print(f"❌ {record.record_id}: {exc}")
            if not args.dry_run:
                safe_update(client, record.record_id, {
                    FIELD["feedback_status"]: FEEDBACK_STATUS_REVIEW,
                    FIELD["last_run_at"]: now_ms(),
                })
            try:
                args.circuit_breaker.record_failed_record(exc)
            except CircuitBreakerOpen as breaker_exc:
                print(f"⛔ 熔断停止: {breaker_exc}")
                break

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["failed"] == 0 else 1


def select_feedback_records(
    client: FeishuBitableClient,
    *,
    view_id: Optional[str],
    limit: Optional[int],
    record_ids: Optional[List[str]],
) -> List[TableRecord]:
    records = client.list_records(limit=None, view_id=view_id)
    selected_ids = set(record_ids or [])
    has_explicit_ids = bool(selected_ids)
    limit = None if has_explicit_ids else limit
    selected: List[TableRecord] = []
    for record in records:
        if selected_ids and record.record_id not in selected_ids:
            continue
        if has_explicit_ids:
            selected.append(record)
            continue
        feedback_status = normalize_cell_value(record.fields.get(FIELD["feedback_status"]))
        if feedback_status == FEEDBACK_STATUS_PENDING:
            selected.append(record)
        if limit and len(selected) >= limit:
            break
    return selected


if __name__ == "__main__":
    raise SystemExit(main())
