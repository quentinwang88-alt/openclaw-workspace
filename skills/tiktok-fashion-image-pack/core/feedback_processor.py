#!/usr/bin/env python3
"""Feedback-driven image fix processor for likeU TikTok Shop product images."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

SKILL_DIR = Path(__file__).resolve().parents[1]
CORE_DIR = SKILL_DIR / "core"
WORKSPACE_DIR = SKILL_DIR.parents[1]
if str(CORE_DIR) not in sys.path:
    sys.path.insert(0, str(CORE_DIR))
if str(WORKSPACE_DIR) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_DIR))

from feishu import FeishuBitableClient, TableRecord, extract_attachments, normalize_cell_value
from image_generator import generate_fix_image
from feedback_qa import qa_feedback_fix, format_qa_issues
from circuit_breaker import CircuitBreakerOpen


FIELD = {
    "source_images": "原始图片",
    "scene_reference_images": "原始场景参考图",
    "main_result": "首图结果",
    "scene_result": "场景图结果",
    "scene_details": "场景图生成明细",
    "truth_json": "Product Truth JSON",
    "prompt": "生成Prompt",
    "feedback_target_image": "反馈目标图",
    "feedback_issues": "图片反馈问题",
    "feedback_status": "反馈状态",
    "feedback_fix_result": "反馈修正结果",
    "feedback_fix_result_scene": "反馈修正结果_场景图",
    "feedback_fix_prompt": "反馈修正Prompt",
    "feedback_fix_method": "反馈处理方式",
    "feedback_qa_result": "反馈质检结果",
    "feedback_qa_issues": "反馈质检问题",
    "last_run_at": "最后生成时间",
}

FEEDBACK_STATUS_PENDING = "待修正"
FEEDBACK_STATUS_PROCESSING = "修正中"
FEEDBACK_STATUS_DONE = "已修正"
FEEDBACK_STATUS_REVIEW = "需人工复核"
FIX_METHOD_LOCAL = "局部修正"
FIX_METHOD_REGENERATE = "整图重生"
FIX_METHOD_STRUCTURE = "结构优先重生"

STRUCTURE_KEYWORDS = (
    "扣子",
    "扣钮",
    "纽扣",
    "门襟",
    "口袋",
    "领型",
    "领口",
    "衣领",
    "袖口",
    "下摆",
    "拉链",
    "腰带",
)

PROMPT_TEMPLATE_PATH = SKILL_DIR / "prompts" / "feedback_fix.md"


def build_feedback_fix_prompt(
    *,
    issues: str,
    fix_method: str = "",
    target: str = "",
    product_truth: Dict[str, Any],
    product_ref_count: int = 1,
    scene_ref_count: int = 0,
) -> str:
    template = PROMPT_TEMPLATE_PATH.read_text(encoding="utf-8")
    product_ref_label = _image_range_label(2, product_ref_count)
    scene_ref_label = _image_range_label(2 + product_ref_count, scene_ref_count) if scene_ref_count else ""
    scene_ref_note = (
        f"- **{scene_ref_label}（原始场景参考图）**：供应商或原始场景图，"
        "只用于辅助确认真实穿着结构、扣子/口袋位置、开合方式和材质呈现\n"
        if scene_ref_count
        else ""
    )

    if fix_method == FIX_METHOD_STRUCTURE:
        method_context = (
            "\n## 修正模式：结构优先重生\n\n"
            "本次采用「结构优先重生」模式。优先保证反馈点名的产品结构准确、清晰、可验证，"
            "包括扣子/扣钮数量、单侧位置、口袋位置、领型、袖口、下摆等。"
            "为了让结构清楚呈现，可以调整手臂遮挡、衣襟开合、局部姿态、裁切和角度，"
            "但不得改变原始商品图和原始场景参考图中的真实产品结构。\n\n"
            "结构证明构图要求：半身正面或微侧正面，衣服主体占画面约 70%，"
            "双手和头发不要遮挡被反馈点名的结构；背景保持简单真实，"
            "模特可裁脸、侧脸或弱露脸，不要让人脸成为画面重点。"
        )
    elif fix_method == FIX_METHOD_REGENERATE:
        method_context = (
            "\n## 修正模式：整图重生\n\n"
            "本次采用「整图重生」模式。在上述反馈问题被修正的前提下，"
            "构图、姿态、光线和场景氛围可以根据修正需要调整，"
            "但产品细节仍以原始商品图（{{product_ref_label}}）、原始场景参考图和商品识别信息为最高依据。"
        )
    else:
        method_context = (
            "\n## 修正模式：局部修正\n\n"
            "本次采用「局部修正」模式。在上述反馈问题被修正的前提下，"
            "未被反馈点名的画面元素、构图、姿态、光线和场景氛围尽量保持图片 1 的现有状态。"
        )

    method_context = method_context.replace("{{product_ref_label}}", product_ref_label)
    target_note = f"\n\n本次修正目标图：{target}" if target else ""

    truth_json = json.dumps(product_truth, ensure_ascii=False, indent=2)

    return (
        template.replace("{{product_ref_label}}", product_ref_label)
        .replace("{{scene_ref_note}}", scene_ref_note.rstrip())
        .replace("{{fix_method_context}}", method_context)
        .replace("{{issues}}", issues + target_note)
        .replace("{{product_truth_json}}", truth_json)
    )


def _image_range_label(start: int, count: int) -> str:
    if count <= 0:
        return ""
    end = start + count - 1
    return f"图片 {start}" if start == end else f"图片 {start}-{end}"


def is_structure_feedback(issues: str) -> bool:
    return any(keyword in issues for keyword in STRUCTURE_KEYWORDS)


def resolve_feedback_fix_method(requested_method: str, issues: str) -> str:
    if is_structure_feedback(issues):
        return FIX_METHOD_STRUCTURE
    if requested_method == FIX_METHOD_REGENERATE:
        return FIX_METHOD_REGENERATE
    return FIX_METHOD_LOCAL


def expand_feedback_issues(issues: str) -> str:
    """Expand short operator feedback into stricter image-generation acceptance rules."""
    issue_text = issues.strip()
    additions: List[str] = []

    if re.search(r"(扣子|扣钮|纽扣)", issue_text):
        additions.append(
            "扣子/扣钮必须按反馈中的数量和位置精确呈现，清晰可数，不多不少；"
            "袖口扣、装饰扣、背景物体不计入正面门襟扣子。"
        )
        additions.append(
            "如果当前角度导致数量看不清，可以调整衣襟开合、手臂遮挡、局部姿态或裁切，"
            "让扣子/扣钮完整露出。"
        )
    if re.search(r"[0-9一二三四五六七八九十]+颗", issue_text):
        additions.append("反馈中出现的具体颗数是硬性验收标准，最终图中不能多一颗或少一颗。")
    if "右边" in issue_text or "左边" in issue_text or "左右" in issue_text:
        additions.append(
            "左右位置必须以成图画面中观众看到的方向为准，并对照原始参考图；"
            "不要镜像到相反一侧。"
        )
    if "口袋" in issue_text:
        additions.append(
            "口袋必须清晰可见，位置、数量、翻盖/贴袋结构参考原始商品图和原始场景参考图；"
            "不要被手臂、袖子、衣褶或裁切遮挡到无法确认。"
        )
    if re.search(r"(领型|领口|衣领|袖口|下摆|门襟|拉链|腰带)", issue_text):
        additions.append(
            "被反馈点名的结构必须完整露出并可验证，可以采用半身正面或微侧正面结构证明构图。"
        )

    if not additions:
        return issue_text

    deduped: List[str] = []
    for item in additions:
        if item not in deduped:
            deduped.append(item)

    return issue_text + "\n\n## 系统扩写验收标准\n" + "\n".join(f"- {item}" for item in deduped)


def parse_feedback_targets(raw_value: Any) -> List[str]:
    """Parse 反馈目标图 field into a list of normalized target labels."""
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return [normalize_cell_value(v) for v in raw_value if normalize_cell_value(v)]
    value = normalize_cell_value(raw_value)
    if not value:
        return []
    return [t.strip() for t in value.split(",") if t.strip()]


def find_scene_image_path(
    client: FeishuBitableClient,
    record: TableRecord,
    target_slot: str,
    output_dir: Path,
) -> Optional[str]:
    """Find and download a specific scene image by slot label (S1-S6).

    Uses 场景图生成明细 to locate the correct attachment in 场景图结果.
    """
    details_raw = normalize_cell_value(record.fields.get(FIELD["scene_details"]))
    if not details_raw:
        return None

    try:
        details = json.loads(details_raw)
        if not isinstance(details, list):
            return None
    except json.JSONDecodeError:
        return None

    target_upper = target_slot.strip().upper()
    for idx, detail in enumerate(details):
        slot_id = str(detail.get("image_id") or "").strip().upper()
        if slot_id != target_upper:
            continue
        if not detail.get("uploaded"):
            continue

        scene_attachments = extract_attachments(record.fields.get(FIELD["scene_result"]))
        if idx >= len(scene_attachments):
            return None

        attachment = scene_attachments[idx]
        try:
            path = client.download_attachment(attachment, output_dir)
            return str(path)
        except Exception:
            return None

    return None


def process_feedback_record(
    client: FeishuBitableClient,
    record: TableRecord,
    *,
    args: Any,
    run_root: Path,
) -> str:
    print(f"\n🔧 反馈修正 {record.record_id}")

    targets = parse_feedback_targets(record.fields.get(FIELD["feedback_target_image"]))
    if not targets:
        print("  ⚠️ 未指定反馈目标图，跳过")
        return "failed"

    issues = normalize_cell_value(record.fields.get(FIELD["feedback_issues"]))
    if not issues:
        print("  ⚠️ 未填写图片反馈问题，跳过")
        return "failed"

    requested_fix_method = normalize_cell_value(record.fields.get(FIELD["feedback_fix_method"])) or FIX_METHOD_LOCAL
    expanded_issues = expand_feedback_issues(issues)
    fix_method = resolve_feedback_fix_method(requested_fix_method, expanded_issues)
    if fix_method != requested_fix_method:
        print(f"  修正模式自动调整: {requested_fix_method} -> {fix_method}")

    truth_raw = normalize_cell_value(record.fields.get(FIELD["truth_json"]))
    try:
        product_truth = json.loads(truth_raw) if truth_raw else {}
    except json.JSONDecodeError:
        product_truth = {}

    # Download source and scene reference images
    source_dir = run_root / record.record_id / "feedback_source"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_paths = _download_source_images(client, record, source_dir)
    if not source_paths:
        print("  ⚠️ 缺少原始商品图，跳过")
        return "failed"
    scene_ref_paths = _download_scene_reference_images(client, record, source_dir)
    reference_paths = [*source_paths, *scene_ref_paths]

    print(f"  参考图: 原始商品图 {len(source_paths)} 张，原始场景参考图 {len(scene_ref_paths)} 张")

    generated_dir = run_root / record.record_id / "feedback_generated"
    generated_dir.mkdir(parents=True, exist_ok=True)
    output_dir = run_root / record.record_id / "feedback_output"
    output_dir.mkdir(parents=True, exist_ok=True)

    if not args.dry_run:
        safe_update_feedback(client, record.record_id, {
            FIELD["feedback_status"]: FEEDBACK_STATUS_PROCESSING,
        })

    # Separate targets
    main_targets = [t for t in targets if t == "首图"]
    scene_targets = [t for t in targets if t.upper().startswith("S")]

    fix_results_main: List[Dict[str, Any]] = []
    fix_results_scene: List[Dict[str, Any]] = []
    all_qa_results: List[Dict[str, Any]] = []
    all_prompts: List[str] = []
    all_qa_issues: List[str] = []
    has_failure = False

    # Process main image fixes
    for target in main_targets:
        try:
            main_attachments = extract_attachments(record.fields.get(FIELD["main_result"]))
            if not main_attachments:
                print(f"  ⚠️ 无首图结果，跳过 {target}")
                continue

            fix_target_path = str(client.download_attachment(main_attachments[0], generated_dir))
            fix_prompt = build_feedback_fix_prompt(
                issues=expanded_issues,
                fix_method=fix_method,
                target=target,
                product_truth=product_truth,
                product_ref_count=len(source_paths),
                scene_ref_count=len(scene_ref_paths),
            )

            if args.dry_run:
                print(f"DRY RUN {target} fix prompt:\n{fix_prompt[:1800]}")
                continue

            fixed_path, fb_qa, prompt_history = generate_feedback_fix_with_retries(
                args=args,
                task_id=f"{record.record_id}_fb_main",
                base_prompt=fix_prompt,
                fix_target_path=fix_target_path,
                reference_paths=[str(p) for p in reference_paths],
                product_reference_paths=[str(p) for p in source_paths],
                scene_reference_paths=[str(p) for p in scene_ref_paths],
                issues=expanded_issues,
                fix_method=fix_method,
                output_dir=output_dir,
            )

            all_qa_results.append(fb_qa)
            all_qa_issues.append(f"首图 QA: {fb_qa.get('result')} | {fb_qa.get('summary', '')}")

            attachment = client.upload_attachment(Path(fixed_path))
            fix_results_main.append(attachment)
            all_prompts.append(f"=== 首图修正 ===\n" + "\n\n".join(prompt_history))
            print(f"  ✅ 首图修正完成")
        except Exception as exc:
            has_failure = True
            all_qa_issues.append(f"首图修正失败: {exc}")
            print(f"  ❌ 首图修正失败: {exc}")

    # Process scene image fixes
    for target in scene_targets:
        try:
            scene_path = find_scene_image_path(client, record, target, generated_dir)
            if not scene_path:
                print(f"  ⚠️ 未找到 {target} 对应的场景图，跳过")
                continue

            fix_prompt = build_feedback_fix_prompt(
                issues=expanded_issues,
                fix_method=fix_method,
                target=target,
                product_truth=product_truth,
                product_ref_count=len(source_paths),
                scene_ref_count=len(scene_ref_paths),
            )

            if args.dry_run:
                print(f"DRY RUN {target} fix prompt:\n{fix_prompt[:1800]}")
                continue

            fixed_path, fb_qa, prompt_history = generate_feedback_fix_with_retries(
                args=args,
                task_id=f"{record.record_id}_fb_{target}",
                base_prompt=fix_prompt,
                fix_target_path=scene_path,
                reference_paths=[str(p) for p in reference_paths],
                product_reference_paths=[str(p) for p in source_paths],
                scene_reference_paths=[str(p) for p in scene_ref_paths],
                issues=expanded_issues,
                fix_method=fix_method,
                output_dir=output_dir,
            )

            all_qa_results.append(fb_qa)
            all_qa_issues.append(f"{target} QA: {fb_qa.get('result')} | {fb_qa.get('summary', '')}")

            attachment = client.upload_attachment(Path(fixed_path))
            fix_results_scene.append(attachment)
            all_prompts.append(f"=== {target} 修正 ===\n" + "\n\n".join(prompt_history))
            print(f"  ✅ {target} 修正完成")
        except Exception as exc:
            has_failure = True
            all_qa_issues.append(f"{target}修正失败: {exc}")
            print(f"  ❌ {target} 修正失败: {exc}")

    if args.dry_run:
        return "done"

    # Determine final status based on generation failures AND QA results
    formatted_qa_issues = "; ".join(all_qa_issues) if all_qa_issues else ""
    detailed_qa_issues = _aggregate_feedback_qa_issues(all_qa_results)

    if has_failure and not fix_results_main and not fix_results_scene:
        final_status = FEEDBACK_STATUS_REVIEW
        qa_result = "不通过"
    elif has_failure:
        final_status = FEEDBACK_STATUS_REVIEW
        qa_result = "部分通过"
    elif _has_qa_failures(all_qa_results):
        final_status = FEEDBACK_STATUS_REVIEW
        qa_result = "不通过"
    elif _has_qa_partial(all_qa_results):
        final_status = FEEDBACK_STATUS_REVIEW
        qa_result = "部分通过"
    else:
        final_status = FEEDBACK_STATUS_DONE
        qa_result = "通过"

    update: Dict[str, Any] = {
        FIELD["feedback_status"]: final_status,
        FIELD["feedback_fix_prompt"]: "\n\n".join(all_prompts),
        FIELD["feedback_qa_result"]: qa_result,
        FIELD["feedback_qa_issues"]: detailed_qa_issues or formatted_qa_issues,
    }
    if fix_results_main:
        update[FIELD["feedback_fix_result"]] = fix_results_main
    if fix_results_scene:
        update[FIELD["feedback_fix_result_scene"]] = fix_results_scene

    safe_update_feedback(client, record.record_id, update)
    print(f"✅ 反馈修正写回 {record.record_id}: {final_status}")
    return "review" if final_status == FEEDBACK_STATUS_REVIEW else "done"


def generate_feedback_fix_with_retries(
    *,
    args: Any,
    task_id: str,
    base_prompt: str,
    fix_target_path: str,
    reference_paths: List[str],
    product_reference_paths: List[str],
    scene_reference_paths: List[str],
    issues: str,
    fix_method: str,
    output_dir: Path,
) -> tuple[str, Dict[str, Any], List[str]]:
    current_target_path = fix_target_path
    latest_fixed_path = fix_target_path
    last_qa: Dict[str, Any] = {"result": "未质检", "score": 1.0, "items": [], "summary": "QA skipped"}
    prompt_history: List[str] = []
    max_attempts = max(int(getattr(args, "max_retries", 0) or 0), 0) + 1

    for attempt in range(max_attempts):
        prompt = base_prompt if attempt == 0 else build_retry_fix_prompt(base_prompt, last_qa, attempt)
        prompt_history.append(f"--- attempt {attempt + 1} ---\n{prompt}")
        attempt_task_id = task_id if attempt == 0 else f"{task_id}_retry{attempt}"
        try:
            args.circuit_breaker.before_model_call()
            fixed_paths = generate_fix_image(
                task_id=attempt_task_id,
                prompt=prompt,
                fix_target_path=current_target_path,
                reference_paths=reference_paths,
                output_dir=output_dir,
                quality=args.quality,
            )
            args.circuit_breaker.record_model_success()
        except CircuitBreakerOpen:
            raise
        except Exception as exc:
            args.circuit_breaker.record_model_failure(exc)
            raise
        fixed_path = fixed_paths[0]
        latest_fixed_path = fixed_path

        if args.skip_qa:
            return fixed_path, last_qa, prompt_history

        try:
            last_qa = qa_feedback_fix(
                fix_image_path=fixed_path,
                previous_image_path=current_target_path,
                product_reference_paths=product_reference_paths,
                scene_reference_paths=scene_reference_paths,
                issues=issues,
                fix_method=fix_method,
            )
        except Exception as qa_exc:
            last_qa = {"result": "需人工复核", "score": 0.0, "items": [], "summary": str(qa_exc)}

        if str(last_qa.get("result") or "") == "通过":
            return fixed_path, last_qa, prompt_history

        if attempt < max_attempts - 1:
            print(f"  ↻ QA未通过，自动重试 {attempt + 1}/{max_attempts - 1}: {last_qa.get('summary', '')}")
            current_target_path = fixed_path

    return latest_fixed_path, last_qa, prompt_history


def build_retry_fix_prompt(base_prompt: str, qa_result: Dict[str, Any], attempt: int) -> str:
    qa_issues = format_qa_issues(qa_result) or str(qa_result.get("summary") or "")
    return (
        base_prompt
        + "\n\n## 上一轮反馈质检未通过，本轮必须针对性修正\n\n"
        + f"重试轮次：{attempt}\n"
        + f"上一轮失败原因：{qa_issues}\n\n"
        + "强制要求：\n"
        + "- 如果失败原因是数量不精确，必须删除或补齐多余/缺失的扣子，只保留反馈指定数量。\n"
        + "- 如果失败原因是结构被遮挡，必须调整手臂、衣襟、姿态或裁切，让结构完整露出。\n"
        + "- 如果失败原因是左右位置错误，必须按成图画面中的观众左右方向修正，不要镜像。\n"
        + "- 不要为了重试而新增未售卖配件或改变未被反馈点名的颜色、材质和版型。"
    )


def _has_qa_failures(qa_results: List[Dict[str, Any]]) -> bool:
    return any(str(r.get("result", "")) == "不通过" for r in qa_results)


def _has_qa_partial(qa_results: List[Dict[str, Any]]) -> bool:
    return any(str(r.get("result", "")) == "部分通过" for r in qa_results)


def _aggregate_feedback_qa_issues(qa_results: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for r in qa_results:
        formatted = format_qa_issues(r)
        if formatted:
            parts.append(formatted)
    return "; ".join(parts) if parts else ""


def safe_update_feedback(client: FeishuBitableClient, record_id: str, fields: Dict[str, Any]) -> None:
    clean = {key: value for key, value in fields.items() if key and value is not None}
    client.update_record_fields(record_id, clean)


def _download_source_images(client: FeishuBitableClient, record: TableRecord, source_dir: Path) -> List[Path]:
    attachments = extract_attachments(record.fields.get(FIELD["source_images"]))
    paths: List[Path] = []
    for attachment in attachments[:4]:
        paths.append(client.download_attachment(attachment, source_dir))
    return paths


def _download_scene_reference_images(client: FeishuBitableClient, record: TableRecord, source_dir: Path) -> List[Path]:
    attachments = extract_attachments(record.fields.get(FIELD["scene_reference_images"]))
    paths: List[Path] = []
    for attachment in attachments[:4]:
        paths.append(client.download_attachment(attachment, source_dir))
    return paths
