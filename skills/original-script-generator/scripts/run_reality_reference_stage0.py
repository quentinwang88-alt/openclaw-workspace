#!/usr/bin/env python3
"""Run the no-Feishu, text-only reality-reference stage-0 experiment."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


SKILL_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = SKILL_ROOT.parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.llm_client import OriginalScriptLLMClient  # noqa: E402
from core.complete_script_v3 import (  # noqa: E402
    attach_field_consumers,
    build_creative_diversity_contract,
    creative_usage_row,
    validate_complete_blueprint,
    validate_complete_script,
    video_prompt_projection,
)
from core.reality_reference import (  # noqa: E402
    assemble_reality_script,
    build_reality_direction_packages,
    project_event_blueprint_to_visual_plan,
    validate_visual_adaptation,
    validate_voiceover_plan,
    validate_voiceover_visual_grounding,
)
from core.reality_reference_prompts import (  # noqa: E402
    build_complete_script_blueprint_prompt,
    build_visual_adaptation_prompt,
)
from core.reality_voiceover_bridge import (  # noqa: E402
    run_central_voiceover,
    run_central_voiceover_candidates,
)
from core.storage import PipelineStorage  # noqa: E402


DEFAULT_PRODUCTS = ["1734482585843304442", "1736446411937318906"]
BLUEPRINT_LLM_DEFAULT_MODEL = os.environ.get(
    "ORIGINAL_SCRIPT_BLUEPRINT_LLM_MODEL", "gpt-5.6-sol"
)
BLUEPRINT_LLM_DEFAULT_REASONING_EFFORT = os.environ.get(
    "ORIGINAL_SCRIPT_BLUEPRINT_REASONING_EFFORT", "high"
)


def _json_load(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _latest_product_context(storage: PipelineStorage, product_code: str) -> Dict[str, Any]:
    # Repeated isolated experiments can create many stage0 audit rows. Search
    # far enough back to reach the last real production source run.
    runs = storage.query_runs_by_product_code(product_code, limit=200)
    if not runs:
        raise RuntimeError(f"找不到产品 {product_code} 的原创脚本历史运行")
    for row in runs:
        record_id = str(row["record_id"] or "")
        # Stage-0 runs deliberately copy the source stages for reproducibility.
        # They must never become the source of a later stage-0 run, otherwise
        # record ids grow as stage0-reality:stage0-reality:... and failed
        # experiments can silently become the new baseline.
        if record_id.startswith("stage0-reality:"):
            continue
        anchor = storage.get_latest_stage_output_json(record_id, "anchor_card", product_code)
        route = storage.get_latest_stage_output_json(record_id, "structure_route", product_code)
        if anchor and route:
            raw_record_fields = _json_load(row["raw_record_fields_json"])
            selling_point_catalog: List[Dict[str, Any]] = []
            for index in range(1, 5):
                brief = storage.get_latest_stage_output_json(
                    record_id, f"script_brief_s{index}", product_code
                ) or {}
                final_strategy = (
                    brief.get("final_strategy")
                    if isinstance(brief.get("final_strategy"), dict)
                    else {}
                )
                if str(final_strategy.get("primary_selling_point") or "").strip():
                    selling_point_catalog.append(
                        {
                            **final_strategy,
                            "source_slot": f"S{index}",
                            "source_stage": f"script_brief_s{index}",
                        }
                    )
            baselines = {
                f"S{index}": storage.get_latest_stage_output_json(record_id, f"script_s{index}", product_code) or {}
                for index in range(1, 5)
            }
            return {
                "source_run_id": int(row["run_id"]),
                "record_id": record_id,
                "product_code": product_code,
                "top_category": str(row["top_category"] or ""),
                "target_country": str(row["target_country"] or ""),
                "target_language": str(row["target_language"] or ""),
                "product_type": str(row["product_type"] or ""),
                "anchor_card": anchor,
                "product_selling_note": str(
                    raw_record_fields.get("产品卖点说明")
                    or raw_record_fields.get("product_selling_note")
                    or ""
                ).strip(),
                "selling_point_catalog": selling_point_catalog,
                "structure_selection": route,
                "baselines": baselines,
            }
    raise RuntimeError(f"产品 {product_code} 没有同时包含 anchor_card 与 structure_route 的运行")


def _record_stage(
    storage: PipelineStorage,
    *,
    run_id: int,
    context: Dict[str, Any],
    stage_name: str,
    stage_order: int,
    output: Dict[str, Any],
    prompt: str = "",
    duration: float = 0.0,
) -> None:
    storage.record_stage_result(
        run_id=run_id,
        record_id=context["record_id"],
        product_code=context["product_code"],
        stage_name=stage_name,
        stage_order=stage_order,
        status="success",
        prompt_text=prompt or f"deterministic:{stage_name}",
        input_context={
            "stage0": True,
            "source_run_id": context["source_run_id"],
            "product_code": context["product_code"],
        },
        image_paths=[],
        output_json=output,
        duration_seconds=duration,
    )


def _record_failed_stage(
    storage: PipelineStorage,
    *,
    run_id: int,
    context: Dict[str, Any],
    stage_name: str,
    stage_order: int,
    error: Exception,
    prompt: str = "",
    diagnostic_output: Optional[Dict[str, Any]] = None,
    duration: float = 0.0,
) -> None:
    """Persist enough failed-stage evidence for a later audit.

    Stage-0 is an experiment, so a rejected parsed candidate is useful evidence
    and must not disappear behind the final validator message.
    """

    storage.record_stage_result(
        run_id=run_id,
        record_id=context["record_id"],
        product_code=context["product_code"],
        stage_name=stage_name,
        stage_order=stage_order,
        status="failed",
        prompt_text=prompt or f"deterministic:{stage_name}",
        input_context={
            "stage0": True,
            "source_run_id": context["source_run_id"],
            "product_code": context["product_code"],
        },
        image_paths=[],
        output_json=diagnostic_output or {},
        duration_seconds=duration,
        error_message=str(error)[:4000],
    )


def _visual_validator(direction: Dict[str, Any]):
    def validate(payload: Any) -> None:
        if not isinstance(payload, dict):
            raise ValueError("视觉适配结果必须是对象")
        result = validate_visual_adaptation(
            payload,
            execution_plan=direction["structure_execution_plan"],
            execution_reference=direction["execution_reference"],
            content_bundle_brief=direction.get("content_bundle_brief", {}),
            creative_blueprint=direction.get("creative_blueprint", {}),
            creative_diversity_contract=direction.get("creative_diversity_contract", {}),
        )
        if not result["valid"]:
            raise ValueError("；".join(result["issues"][:8]))

    return validate


def _blueprint_model_provenance(
    client: Optional[OriginalScriptLLMClient],
) -> Dict[str, Any]:
    return {
        "stage": "complete_script_blueprint",
        "route": str(getattr(client, "route", "primary") or "primary"),
        "model": str(getattr(client, "primary_model", "") or ""),
        "reasoning_effort": str(
            getattr(client, "primary_reasoning_effort", "") or ""
        ),
    }


def _blueprint_cache_matches_model(
    blueprint: Any,
    expected_provenance: Dict[str, Any],
) -> bool:
    if not isinstance(blueprint, dict):
        return False
    actual = blueprint.get("generation_provenance")
    if not isinstance(actual, dict):
        return False
    return all(
        str(actual.get(field) or "") == str(expected_provenance.get(field) or "")
        for field in ("stage", "route", "model", "reasoning_effort")
    )


def _normalize_blueprint(
    payload: Dict[str, Any],
    contract: Dict[str, Any],
    generation_provenance: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    normalized = dict(payload)
    # These values are allocated by code before generation.  The model may
    # elaborate around them, but it is not the authority that names them.
    # Canonicalising them here avoids treating harmless paraphrases as drift
    # while preserving strict identity checks in validate_complete_blueprint.
    if contract.get("viewer_relationship"):
        normalized["viewer_relationship"] = contract["viewer_relationship"]
    scene = dict(normalized.get("scene")) if isinstance(normalized.get("scene"), dict) else {}
    if contract.get("scene_motif"):
        scene["location"] = contract["scene_motif"]
    normalized["scene"] = scene
    normalized["diversity_contract_id"] = contract.get("contract_id", "")
    normalized["presentation_mode"] = contract.get("required_presentation_mode", "UNAVAILABLE")
    normalized["authority"] = "CREATIVE_DESIGN"
    if generation_provenance:
        normalized["generation_provenance"] = dict(generation_provenance)
    normalized = attach_field_consumers(normalized)
    material = json.dumps(normalized, ensure_ascii=False, sort_keys=True, default=str)
    normalized["creative_blueprint_id"] = "CBP_" + hashlib.sha256(
        material.encode("utf-8")
    ).hexdigest()[:24].upper()
    return normalized


def _blueprint_validator(
    contract: Dict[str, Any],
    diagnostics: Optional[Dict[str, Any]] = None,
    generation_provenance: Optional[Dict[str, Any]] = None,
):
    def validate(payload: Any) -> None:
        if not isinstance(payload, dict):
            raise ValueError("完整脚本蓝图必须是对象")
        normalized = _normalize_blueprint(
            payload,
            contract,
            generation_provenance=generation_provenance,
        )
        result = validate_complete_blueprint(normalized, contract)
        if diagnostics is not None:
            diagnostics.clear()
            diagnostics.update(
                {
                    "raw_candidate": payload,
                    "normalized_candidate": normalized,
                    "validation": result,
                }
            )
        if not result["valid"]:
            raise ValueError("；".join(result["issues"][:10]))

    return validate


def _render_storyboard(script: Dict[str, Any]) -> str:
    rows: List[str] = []
    blueprint = script.get("creative_blueprint") if isinstance(script.get("creative_blueprint"), dict) else {}
    production = script.get("production_design") if isinstance(script.get("production_design"), dict) else {}
    compact_brief = (
        script.get("video_generation_brief")
        if isinstance(script.get("video_generation_brief"), dict)
        else {}
    )
    if compact_brief:
        character = compact_brief.get("character") if isinstance(compact_brief.get("character"), dict) else {}
        scene = compact_brief.get("scene") if isinstance(compact_brief.get("scene"), dict) else {}
        macro_passages = [
            item for item in compact_brief.get("macro_visual_passages", []) if isinstance(item, dict)
        ]
        macro_rows = [
            (
                f"  {index}. {item.get('visible_process', '')}；"
                f"{item.get('observable_action', '')}；{item.get('camera_observation', '')}"
            )
            for index, item in enumerate(macro_passages, 1)
        ]
        rows.extend(
            [
                "【视频模型主输入｜优先复制本段】",
                f"- 人物：{character.get('identity', '')}；{character.get('appearance', '')}；{character.get('hair_makeup', '')}",
                f"- 场景：{scene.get('location', '')}；{scene.get('moment', '')}；{scene.get('lighting', '')}；{scene.get('background', '')}",
                f"- 穿搭：{compact_brief.get('outfit', '')}",
                f"- 生活事件：{compact_brief.get('natural_behavior_mainline', '')}",
                "- 三段画面：" + ("\n" + "\n".join(macro_rows) if macro_rows else ""),
                f"- 执行重点：{compact_brief.get('render_focus', '')}",
                f"- 连续口播：{compact_brief.get('continuous_voiceover', '')}",
                "",
                "【制作设定与内部证据｜不要逐项改写成表演任务】",
            ]
        )
    if blueprint:
        persona = blueprint.get("persona") if isinstance(blueprint.get("persona"), dict) else {}
        scene = blueprint.get("scene") if isinstance(blueprint.get("scene"), dict) else {}
        character = production.get("character_setting") if isinstance(production.get("character_setting"), dict) else {}
        scene_setting = production.get("scene_setting") if isinstance(production.get("scene_setting"), dict) else {}
        outfit = production.get("outfit_setting") if isinstance(production.get("outfit_setting"), dict) else {}
        performance = production.get("performance_setting") if isinstance(production.get("performance_setting"), dict) else {}
        rows.extend(
            [
                f"- 创作命题：{blueprint.get('creative_thesis', '')}",
                f"- 拍摄动机：{blueprint.get('creator_motivation', '')}",
                f"- 承载方式：{production.get('presentation_mode', '')}；{character.get('note', '')}",
                f"- 人物身份：{character.get('identity') or persona.get('identity', '')}；年龄感：{character.get('age_presence') or persona.get('age_presence', '')}",
                f"- 外形妆发：{character.get('appearance') or persona.get('appearance', '')}；{character.get('hair_makeup') or persona.get('hair_makeup', '')}",
                f"- 完整穿搭：{outfit.get('styling') or persona.get('styling', '')}；{outfit.get('visibility_note', '')}",
                f"- 场景时刻：{scene_setting.get('location') or scene.get('location', '')}；{scene_setting.get('moment') or scene.get('moment', '')}",
                f"- 光线背景：{scene_setting.get('lighting') or scene.get('lighting', '')}；{scene_setting.get('background') or scene.get('background', '')}",
                f"- 机位：{scene_setting.get('camera_setup') or scene.get('camera_setup', '')}",
                f"- 表演进入：{performance.get('entry_state', '')}",
                f"- 行为动机：{performance.get('behavior_motivation', '')}",
                f"- 收尾状态：{performance.get('ending_state', '')}",
                f"- 说话人格：{character.get('speaking_personality') or persona.get('speaking_personality', '')}；表演强度：{character.get('performance_intensity') or persona.get('performance_intensity', '')}",
                f"- 连续口播：{script.get('continuous_voiceover', {}).get('target_language', '')}",
                f"- 中文对照：{script.get('continuous_voiceover', {}).get('chinese_translation', '')}",
                "",
            ]
        )
    if compact_brief:
        rows.extend(["", "【内部六镜结构槽位｜仅供血缘、节奏和事实审核】"])
    for shot in script.get("storyboard", []) or []:
        if not isinstance(shot, dict):
            continue
        line = str(shot.get("voiceover_text_target_language") or "").strip()
        if not line:
            line = (
                "（承接前一语义段的连续口播）"
                if str(shot.get("audio_actual") or "").strip() == "VOICEOVER_CONTINUATION"
                else "（静默）"
            )
        zh = str(shot.get("voiceover_text_zh") or "").strip()
        rows.append(
            f"- 镜头{shot.get('shot_no')}｜{shot.get('duration')}｜{shot.get('carrier_mode')}｜{shot.get('structure_beat')}\n"
            f"  - 画面：{shot.get('shot_content', '')}\n"
            f"  - 动作：{shot.get('observable_action', '')}\n"
            f"  - 表演：{shot.get('gaze_and_reaction', '')}\n"
            f"  - 声音：{shot.get('audio_actual', '')}\n"
            f"  - 口播：{line}" + (f"（{zh}）" if zh else "")
        )
    return "\n".join(rows)


def _pair_order(product_code: str, slot: str) -> bool:
    digest = hashlib.sha256(f"{product_code}:{slot}:blind-v1".encode("utf-8")).hexdigest()
    return int(digest[:2], 16) % 2 == 0


def _build_blind_markdown(results: List[Dict[str, Any]]) -> tuple[str, Dict[str, Any]]:
    lines = [
        "# 原创脚本真实执行参考阶段0盲测包",
        "",
        "每组请先独立阅读A/B，再按1-5分评估：真实用户感、动作可观察性、AI/广告感、商品证明自然度、与其它脚本重复度、手机可拍性。最后必须二选一。",
    ]
    answer_key: Dict[str, Any] = {}
    pair_no = 0
    for product in results:
        for direction in product.get("directions", []):
            if not direction.get("script"):
                continue
            pair_no += 1
            slot = direction["output_slot"]
            baseline = product.get("baselines", {}).get(slot, {})
            new_script = direction["script"]
            new_first = _pair_order(product["product_code"], slot)
            script_a = new_script if new_first else baseline
            script_b = baseline if new_first else new_script
            answer_key[f"pair_{pair_no}"] = {
                "product_code": product["product_code"],
                "source_slot": slot,
                "A": "new_reality_reference" if new_first else "baseline",
                "B": "baseline" if new_first else "new_reality_reference",
                "execution_card_id": direction.get("execution_card_id"),
            }
            lines.extend(
                [
                    "",
                    f"## 对比组 {pair_no}",
                    "",
                    "### 脚本 A",
                    "",
                    _render_storyboard(script_a),
                    "",
                    "### 脚本 B",
                    "",
                    _render_storyboard(script_b),
                    "",
                    "评分：A____ / B____；强制选择：A / B；理由：________________",
                ]
            )
    return "\n".join(lines) + "\n", answer_key


def _build_voiceover_candidates_markdown(results: List[Dict[str, Any]]) -> str:
    lines = [
        "# 中央口播候选",
        "",
        "只比较怎么说；人物、场景和画面保持不变。"
        "`READY_FOR_SELECTION` 可直接入选；`LONG_WARNING` 只是不自动入选，不是整批失败。",
        "",
    ]
    for product in results:
        for direction in product.get("directions", []) or []:
            candidates = direction.get("voiceover_candidates") or []
            if not candidates:
                continue
            lines.extend(
                [
                    f"## 产品 {product.get('product_code')}｜{direction.get('output_slot')}",
                    "",
                    f"- 核心价值：{direction.get('content_bundle_brief', {}).get('value_proposition', {}).get('text', '')}",
                    f"- 用户顾虑：{direction.get('content_bundle_brief', {}).get('audience_tension', {}).get('text', '')}",
                    "",
                ]
            )
            for candidate in candidates:
                readiness = (
                    candidate.get("selection_readiness")
                    if isinstance(candidate.get("selection_readiness"), dict)
                    else {}
                )
                delivery = (
                    (candidate.get("expression_contract") or {})
                    .get("hook_surface_contract", {})
                    .get("opening_delivery", {})
                    if isinstance(candidate.get("expression_contract"), dict)
                    else {}
                )
                target = " ".join(
                    str(item.get("voiceover_text_target_language") or "").strip()
                    for item in candidate.get("lines", [])
                    if str(item.get("voiceover_text_target_language") or "").strip()
                )
                chinese = " ".join(
                    str(item.get("voiceover_text_zh") or "").strip()
                    for item in candidate.get("lines", [])
                    if str(item.get("voiceover_text_zh") or "").strip()
                )
                lines.extend(
                    [
                        f"### {candidate.get('candidate_id')}｜{candidate.get('hook_id')}",
                        "",
                        f"- 开口方式：{delivery.get('mode', '')}｜收尾倾向：{delivery.get('closing_move', '')}",
                        f"- 入选状态：{readiness.get('status', 'UNAVAILABLE')}｜中心估时：{readiness.get('estimated_sec', '')}",
                        f"- 泰语：{target}",
                        f"- 中文：{chinese}",
                        "",
                    ]
                )
    return "\n".join(lines) + "\n"


def _candidate_is_auto_selectable(candidate: Dict[str, Any]) -> bool:
    readiness = candidate.get("selection_readiness")
    if not isinstance(readiness, dict):
        # Backward-compatible snapshots predate the selection field.
        return True
    return bool(readiness.get("auto_selectable", True))


def _first_auto_selectable_candidate(
    candidates: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    return next(
        (item for item in candidates if _candidate_is_auto_selectable(item)),
        None,
    )


def _build_batch_payload(
    *,
    preview_only: bool,
    skip_voiceover: bool,
    requested_products: List[str],
    results: List[Dict[str, Any]],
    product_errors: List[Dict[str, Any]],
) -> Dict[str, Any]:
    completed = sum(1 for item in results if item.get("status") == "COMPLETED")
    partial = sum(1 for item in results if item.get("status") == "PARTIAL")
    failed = sum(1 for item in results if item.get("status") == "FAILED")
    reference_insufficient = sum(
        1 for item in results if item.get("status") == "REFERENCE_INSUFFICIENT"
    )
    return {
        "schema_version": "reality-reference-stage0-result-v2",
        "batch_status": (
            "COMPLETED"
            if len(results) == len(requested_products)
            and partial == 0
            and failed == 0
            and reference_insufficient == 0
            else "PARTIAL"
        ),
        "preview_only": preview_only,
        "skip_voiceover": skip_voiceover,
        "requested_product_count": len(requested_products),
        "processed_product_count": len(results),
        "completed_product_count": completed,
        "partial_product_count": partial,
        "failed_product_count": failed,
        "reference_insufficient_product_count": reference_insufficient,
        "product_errors": product_errors,
        "products": results,
    }


def _write_partial_batch_result(
    output_dir: Path,
    *,
    preview_only: bool,
    skip_voiceover: bool,
    requested_products: List[str],
    results: List[Dict[str, Any]],
    product_errors: List[Dict[str, Any]],
) -> Dict[str, Any]:
    payload = _build_batch_payload(
        preview_only=preview_only,
        skip_voiceover=skip_voiceover,
        requested_products=requested_products,
        results=results,
        product_errors=product_errors,
    )
    (output_dir / "stage0_result.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return payload


def _cached_voiceover_candidates(
    snapshot: Dict[str, Any],
    *,
    product_code: str,
    direction_result: Dict[str, Any],
    selected_candidate_id: str,
) -> List[Dict[str, Any]]:
    """Return an immutable approved candidate set for the same visual contract."""

    if not selected_candidate_id or not isinstance(snapshot, dict):
        return []
    current_bundle_id = str(
        (direction_result.get("content_bundle_brief") or {}).get("content_bundle_id") or ""
    )
    current_blueprint_id = str(
        (direction_result.get("creative_blueprint") or {}).get("creative_blueprint_id") or ""
    )
    for product in snapshot.get("products") or []:
        if not isinstance(product, dict) or str(product.get("product_code") or "") != product_code:
            continue
        for cached_direction in product.get("directions") or []:
            if not isinstance(cached_direction, dict):
                continue
            cached_bundle_id = str(
                (cached_direction.get("content_bundle_brief") or {}).get("content_bundle_id") or ""
            )
            cached_blueprint_id = str(
                (cached_direction.get("creative_blueprint") or {}).get("creative_blueprint_id") or ""
            )
            same_contract = all(
                (
                    str(cached_direction.get("output_slot") or "")
                    == str(direction_result.get("output_slot") or ""),
                    str(cached_direction.get("execution_card_id") or "")
                    == str(direction_result.get("execution_card_id") or ""),
                    cached_bundle_id == current_bundle_id,
                    cached_blueprint_id == current_blueprint_id,
                )
            )
            candidates = [
                item
                for item in cached_direction.get("voiceover_candidates") or []
                if isinstance(item, dict)
            ]
            if same_contract and any(
                str(item.get("candidate_id") or "") == selected_candidate_id
                for item in candidates
            ):
                return json.loads(json.dumps(candidates, ensure_ascii=False))
    return []


def run_product(
    storage: PipelineStorage,
    creative_storage: PipelineStorage,
    llm: Optional[OriginalScriptLLMClient],
    *,
    product_code: str,
    directions: int,
    preview_only: bool,
    skip_voiceover: bool,
    voiceover_root: str,
    voiceover_db_path: str,
    blueprint_llm: Optional[OriginalScriptLLMClient] = None,
    voiceover_model_command: str = "",
    voiceover_qc_model_command: str = "",
    recent_execution_card_ids: Optional[List[str]] = None,
    recent_source_video_ids: Optional[List[str]] = None,
    voiceover_candidates_only: bool = False,
    voiceover_candidate_count: int = 1,
    selected_voiceover_candidate_id: str = "",
    voiceover_candidate_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    context = _latest_product_context(storage, product_code)
    active_blueprint_llm = blueprint_llm or llm
    stage0_run_id = storage.create_run(
        record_id=f"stage0-reality:{context['record_id']}",
        product_code=product_code,
        input_hash=hashlib.sha256(
            f"{context['source_run_id']}:{context['structure_selection'].get('data_snapshot_hash')}".encode("utf-8")
        ).hexdigest(),
        context={
            **context,
            "request_status": "阶段0-真实执行参考",
        },
        raw_record_fields={"stage0": True, "source_run_id": context["source_run_id"]},
    )
    package_start = time.time()
    packages = build_reality_direction_packages(
        context["structure_selection"],
        anchor_card=context["anchor_card"],
        product_type=context["product_type"],
        top_category=context["top_category"],
        direction_limit=directions,
        strict=True,
        recent_execution_card_ids=recent_execution_card_ids,
        recent_source_video_ids=recent_source_video_ids,
        selling_point_catalog=context.get("selling_point_catalog", []),
        product_selling_note=context.get("product_selling_note", ""),
    )
    _record_stage(
        storage,
        run_id=stage0_run_id,
        context=context,
        stage_name="reality_direction_package",
        stage_order=810,
        output=packages,
        duration=time.time() - package_start,
    )
    result = {
        "product_code": product_code,
        "source_run_id": context["source_run_id"],
        "stage0_run_id": stage0_run_id,
        "record_id": context["record_id"],
        "target_country": context["target_country"],
        "target_language": context["target_language"],
        "top_category": context["top_category"],
        "product_type": context["product_type"],
        "display_family": str(
            (
                context.get("anchor_card", {}).get("category_execution_contract")
                if isinstance(
                    context.get("anchor_card", {}).get("category_execution_contract"), dict
                )
                else {}
            ).get("display_family") or ""
        ).strip(),
        "package_status": packages["status"],
        "selected_count": packages["selected_count"],
        "model_configuration": {
            "complete_script_blueprint": _blueprint_model_provenance(active_blueprint_llm),
            "visual_adaptation": {
                "stage": "event_blueprint_projection",
                "source": "DETERMINISTIC_NO_MODEL_CALL",
            },
        },
        "directions": [],
        "baselines": context["baselines"],
    }
    for direction_index, direction in enumerate(packages.get("directions", []), 1):
        slot = str(direction.get("output_slot") or f"S{direction_index}")
        usage_id = ""
        direction_started = time.time()
        try:
            slot = str(direction.get("output_slot") or f"S{direction_index}")
            reference = direction["execution_reference"]
            recent_patterns = creative_storage.list_recent_creative_patterns(
                country=context["target_country"],
                category=context["top_category"],
                limit=120,
            )
            diversity_contract = build_creative_diversity_contract(
                product_code=product_code,
                country=context["target_country"],
                category=context["top_category"],
                product_type=context["product_type"],
                direction=direction,
                recent_usage=recent_patterns,
            )
            direction["creative_diversity_contract"] = diversity_contract
            usage = creative_usage_row(
                contract=diversity_contract,
                product_code=product_code,
                direction=direction,
                source_run_id=stage0_run_id,
            )
            usage_id = ""
            if not preview_only:
                usage_id = creative_storage.reserve_creative_pattern(usage)
            _record_stage(
                storage,
                run_id=stage0_run_id,
                context=context,
                stage_name=f"creative_diversity_{slot.lower()}",
                stage_order=825 + direction_index,
                output={**diversity_contract, "usage_id": usage_id},
            )
            _record_stage(
                storage,
                run_id=stage0_run_id,
                context=context,
                stage_name=f"execution_reference_{slot.lower()}",
                stage_order=820 + direction_index,
                output=direction["reference_selection"],
            )
            _record_stage(
                storage,
                run_id=stage0_run_id,
                context=context,
                stage_name=f"p2_lite_{slot.lower()}",
                stage_order=830 + direction_index,
                output=direction["p2_lite"],
            )
            _record_stage(
                storage,
                run_id=stage0_run_id,
                context=context,
                stage_name=f"content_bundle_{slot.lower()}",
                stage_order=834 + direction_index,
                output=direction.get("content_bundle_brief", {}),
            )
            direction_result: Dict[str, Any] = {
                "output_slot": slot,
                "direction_assignment_id": direction.get("direction_assignment_id"),
                "cluster_id": direction.get("cluster_id"),
                "cluster_version": direction.get("cluster_version"),
                "execution_card_id": reference.get("execution_card_id"),
                "source_profile_id": reference.get("source_profile_id"),
                "source_video_id": reference.get("source_video_id"),
                "reference_score": reference.get("selection_score"),
                "p2_lite": direction["p2_lite"],
                "content_bundle_brief": direction.get("content_bundle_brief", {}),
                "creative_diversity_contract": diversity_contract,
                "creative_usage_id": usage_id,
            }
            if preview_only:
                result["directions"].append(direction_result)
                continue
            assert llm is not None
            assert active_blueprint_llm is not None
            blueprint_provenance = _blueprint_model_provenance(active_blueprint_llm)
            blueprint_prompt = build_complete_script_blueprint_prompt(
                target_country=context["target_country"],
                product_type=context["product_type"],
                direction=direction,
            )
            blueprint_start = time.time()
            blueprint_stage_name = f"complete_script_blueprint_v24_carrier_{slot.lower()}"
            cached_blueprint = storage.get_latest_stage_output_json(
                context["record_id"], blueprint_stage_name, product_code
            )
            cached_blueprint_validation = (
                validate_complete_blueprint(cached_blueprint, diversity_contract)
                if isinstance(cached_blueprint, dict)
                and cached_blueprint
                and _blueprint_cache_matches_model(
                    cached_blueprint,
                    blueprint_provenance,
                )
                else {"valid": False}
            )
            if cached_blueprint_validation.get("valid"):
                creative_blueprint = cached_blueprint
                blueprint_validation = cached_blueprint_validation
                print(f"  ♻️ 复用已通过的{blueprint_stage_name}，不重复调用模型")
            else:
                blueprint_diagnostics: Dict[str, Any] = {}
                try:
                    raw_blueprint = active_blueprint_llm.call_json(
                        blueprint_prompt,
                        max_tokens=4200,
                        max_attempts=3,
                        validator=_blueprint_validator(
                            diversity_contract,
                            blueprint_diagnostics,
                            blueprint_provenance,
                        ),
                    )
                except Exception as exc:
                    _record_failed_stage(
                        storage,
                        run_id=stage0_run_id,
                        context=context,
                        stage_name=blueprint_stage_name,
                        stage_order=838 + direction_index,
                        error=exc,
                        prompt=blueprint_prompt,
                        diagnostic_output=blueprint_diagnostics,
                        duration=time.time() - blueprint_start,
                    )
                    raise
                creative_blueprint = _normalize_blueprint(
                    raw_blueprint,
                    diversity_contract,
                    generation_provenance=blueprint_provenance,
                )
                blueprint_validation = validate_complete_blueprint(
                    creative_blueprint, diversity_contract
                )
            if not blueprint_validation["valid"]:
                raise ValueError("完整脚本蓝图校验失败：" + "；".join(blueprint_validation["issues"]))
            direction["creative_blueprint"] = creative_blueprint
            direction["video_prompt_blueprint"] = video_prompt_projection(creative_blueprint)
            _record_stage(
                storage,
                run_id=stage0_run_id,
                context=context,
                stage_name=blueprint_stage_name,
                stage_order=838 + direction_index,
                output={**creative_blueprint, "validation": blueprint_validation},
                prompt=blueprint_prompt,
                duration=time.time() - blueprint_start,
            )
            direction_result["creative_blueprint"] = creative_blueprint
            visual_start = time.time()
            visual_stage_name = f"event_projection_v2_carrier_{slot.lower()}"
            visual_plan = project_event_blueprint_to_visual_plan(direction=direction)
            visual_validation = validate_visual_adaptation(
                visual_plan,
                execution_plan=direction["structure_execution_plan"],
                execution_reference=reference,
                content_bundle_brief=direction.get("content_bundle_brief", {}),
                creative_blueprint=creative_blueprint,
                creative_diversity_contract=diversity_contract,
            )
            _record_stage(
                storage,
                run_id=stage0_run_id,
                context=context,
                stage_name=visual_stage_name,
                stage_order=840 + direction_index,
                output={**visual_plan, "validation": visual_validation},
                prompt="deterministic:event_blueprint_to_visual_plan",
                duration=time.time() - visual_start,
            )
            if not visual_validation["valid"]:
                raise ValueError(
                    "事件蓝图确定性投影校验失败："
                    + "；".join(visual_validation["issues"])
                )
            if skip_voiceover:
                voiceover_plan = {
                    "voiceover_plan_schema_version": "visual-first-voiceover-v1",
                    "source": "SKIPPED_FOR_VISUAL_REVIEW",
                    "lines": [],
                    "silent_shots": list(range(1, len(visual_plan.get("shots", [])) + 1)),
                }
            else:
                voiceover_start = time.time()
                voiceover_kwargs = {
                    "product_code": product_code,
                    "target_country": context["target_country"],
                    "target_language": context["target_language"],
                    "direction": direction,
                    "visual_plan": visual_plan,
                    "voiceover_root": voiceover_root,
                    "voiceover_db_path": voiceover_db_path,
                    "model_command": voiceover_model_command,
                    "qc_model_command": voiceover_qc_model_command,
                }
                voiceover_candidates = _cached_voiceover_candidates(
                    voiceover_candidate_snapshot or {},
                    product_code=product_code,
                    direction_result=direction_result,
                    selected_candidate_id=selected_voiceover_candidate_id,
                )
                if voiceover_candidates:
                    print(
                        f"  ♻️ 复用已选中央口播候选 {selected_voiceover_candidate_id}，"
                        "不重新生成三稿"
                    )
                else:
                    voiceover_candidates = run_central_voiceover_candidates(
                        candidate_count=max(1, min(3, int(voiceover_candidate_count))),
                        **voiceover_kwargs,
                    )
                selected_candidate = next(
                    (
                        item
                        for item in voiceover_candidates
                        if str(item.get("candidate_id") or "")
                        == str(selected_voiceover_candidate_id or "")
                    ),
                    None,
                )
                if selected_voiceover_candidate_id and selected_candidate is None:
                    available_ids = [
                        str(item.get("candidate_id") or "")
                        for item in voiceover_candidates
                    ]
                    raise ValueError(
                        "指定的口播候选不存在："
                        f"{selected_voiceover_candidate_id}；当前候选={','.join(available_ids)}"
                    )
                if (
                    selected_candidate is not None
                    and not _candidate_is_auto_selectable(selected_candidate)
                ):
                    # Human selection is allowed to request one existing
                    # compression pass.  It runs only here, never while the
                    # three candidates are first explored.
                    print(
                        "  ⏱️ 已选候选中心估时超过15秒，"
                        "按现有一次软修订机制压缩后再组装"
                    )
                    compressed_candidate = run_central_voiceover(
                        **voiceover_kwargs,
                        candidate_hook_id=str(
                            selected_candidate.get("selected_hook_id")
                            or selected_candidate.get("hook_id")
                            or ""
                        ),
                        candidate_id=str(selected_candidate.get("candidate_id") or ""),
                        force_duration_compression=True,
                    )
                    if not _candidate_is_auto_selectable(compressed_candidate):
                        readiness = compressed_candidate.get("selection_readiness") or {}
                        raise ValueError(
                            "已选口播候选在一次压缩后仍超过15秒；"
                            f"中心估时={readiness.get('estimated_sec')}秒，请选择其它候选"
                        )
                    voiceover_candidates = [
                        (
                            compressed_candidate
                            if str(item.get("candidate_id") or "")
                            == str(compressed_candidate.get("candidate_id") or "")
                            else item
                        )
                        for item in voiceover_candidates
                    ]
                    selected_candidate = compressed_candidate
                if selected_candidate is not None:
                    voiceover_plan = selected_candidate
                elif voiceover_candidates_only:
                    voiceover_plan = voiceover_candidates[0]
                else:
                    voiceover_plan = _first_auto_selectable_candidate(voiceover_candidates)
                    if voiceover_plan is None:
                        raise ValueError(
                            "全部中央口播候选中心估时超过15秒，"
                            "未自动入选；请人工选择后触发一次压缩。"
                        )
                voiceover_validation = validate_voiceover_plan(
                    voiceover_plan, len(visual_plan.get("shots", []))
                )
                first_shot = (
                    visual_plan.get("shots", [])[0]
                    if isinstance(visual_plan.get("shots"), list) and visual_plan.get("shots")
                    else {}
                )
                grounding_validation = validate_voiceover_visual_grounding(
                    voiceover_plan,
                    primary_observation=str(direction["p2_lite"].get("primary_observation") or ""),
                    first_shot_content=(
                        " ".join(
                            value
                            for value in (
                                str(first_shot.get("shot_content") or ""),
                                str(first_shot.get("observable_action") or ""),
                                *[
                                    str(atom.get("fact_text") or "")
                                    for atom in direction.get("content_bundle_brief", {}).get("claim_atoms", [])
                                    if isinstance(atom, dict)
                                    and str(atom.get("claim_key") or "")
                                    in first_shot.get("supported_claim_keys", [])
                                ],
                            )
                            if value
                        )
                        if isinstance(first_shot, dict)
                        else ""
                    ),
                )
                conflict_policy = (
                    voiceover_plan.get("expression_contract", {}).get("conflict_policy", {})
                    if isinstance(voiceover_plan.get("expression_contract"), dict)
                    else {}
                )
                voiceover_priority = bool(
                    conflict_policy.get("voiceover_priority_on_soft_alignment_conflict")
                )
                if not grounding_validation["valid"] and not voiceover_priority:
                    raise ValueError("中央口播视觉落地失败：" + "；".join(grounding_validation["issues"]))
                if not grounding_validation["valid"] and voiceover_priority:
                    voiceover_plan.setdefault("warnings", []).append(
                        {
                            "code": "LOCAL_OPENING_GROUNDING_WARNING",
                            "message": "；".join(grounding_validation["issues"]),
                            "blocking": False,
                        }
                    )
                if not voiceover_validation["valid"]:
                    raise ValueError("中央口播后置校验失败：" + "；".join(voiceover_validation["issues"]))
                voiceover_plan["validation"] = {
                    **voiceover_validation,
                    "visual_grounding": grounding_validation,
                }
                direction_result["voiceover_argument_contract"] = (
                    voiceover_plan.get("expression_contract", {}).get("argument_contract", {})
                )
                direction_result["voiceover_candidates"] = voiceover_candidates
                direction_result["selected_voiceover_candidate_id"] = (
                    str(voiceover_plan.get("candidate_id") or "")
                    if selected_candidate is not None or not voiceover_candidates_only
                    else ""
                )
                direction_result["selection_status"] = (
                    "SELECTED"
                    if selected_candidate is not None or not voiceover_candidates_only
                    else "WAITING_FOR_HUMAN_SELECTION"
                )
                _record_stage(
                    storage,
                    run_id=stage0_run_id,
                    context=context,
                    stage_name=f"voiceover_{slot.lower()}",
                    stage_order=850 + direction_index,
                    output=voiceover_plan,
                    duration=time.time() - voiceover_start,
                )
                if voiceover_candidates_only and selected_candidate is None:
                    direction_result["visual_plan"] = visual_plan
                    direction_result["voiceover_plan"] = voiceover_plan
                    direction_result["status"] = "WAITING_FOR_HUMAN_SELECTION"
                    result["directions"].append(direction_result)
                    continue
            script = assemble_reality_script(
                direction=direction,
                visual_plan=visual_plan,
                voiceover_plan=voiceover_plan,
            )
            complete_quality = validate_complete_script(script)
            script["quality_result"] = complete_quality
            if usage_id:
                creative_storage.update_creative_pattern_status(
                    usage_id,
                    "MACHINE_SCREENED" if complete_quality["valid"] else "RELEASED",
                    metadata={
                        "contract_id": diversity_contract.get("contract_id"),
                        "quality_result": complete_quality,
                    },
                )
            _record_stage(
                storage,
                run_id=stage0_run_id,
                context=context,
                stage_name=f"authenticity_review_{slot.lower()}",
                stage_order=860 + direction_index,
                output=script["authenticity_review"],
            )
            direction_result.update(
                {
                    "visual_plan": visual_plan,
                    "voiceover_plan": voiceover_plan,
                    "script": script,
                    "carrier_integrity": (
                        script.get("video_generation_brief", {}).get("carrier_integrity", {})
                        if isinstance(script.get("video_generation_brief"), dict)
                        else {}
                    ),
                    "quality_result": complete_quality,
                }
            )
            result["directions"].append(direction_result)
        except Exception as exc:
            diagnostic_persistence_errors: List[str] = []
            if usage_id:
                try:
                    creative_storage.update_creative_pattern_status(
                        usage_id,
                        "RELEASED",
                        metadata={
                            "contract_id": direction.get("creative_diversity_contract", {}).get("contract_id", ""),
                            "failure": str(exc)[:4000],
                        },
                    )
                except Exception as cleanup_exc:
                    diagnostic_persistence_errors.append(f"creative_usage_release: {cleanup_exc}")
            failure = {
                "output_slot": slot,
                "direction_assignment_id": direction.get("direction_assignment_id"),
                "cluster_id": direction.get("cluster_id"),
                "cluster_version": direction.get("cluster_version"),
                "execution_card_id": direction.get("execution_reference", {}).get("execution_card_id"),
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:4000],
            }
            if diagnostic_persistence_errors:
                failure["diagnostic_persistence_errors"] = diagnostic_persistence_errors
            result.setdefault("direction_errors", []).append(failure)
            try:
                _record_failed_stage(
                    storage,
                    run_id=stage0_run_id,
                    context=context,
                    stage_name=f"direction_failure_{slot.lower()}",
                    stage_order=890 + direction_index,
                    error=exc,
                    diagnostic_output=failure,
                    duration=time.time() - direction_started,
                )
            except Exception as persistence_exc:
                failure.setdefault("diagnostic_persistence_errors", []).append(
                    f"rds_failed_stage: {persistence_exc}"
                )
            print(f"  ⚠️ {slot} 失败，继续下一方向：{exc}")
            continue
    result["completed_count"] = len(result["directions"])
    result["failed_count"] = len(result.get("direction_errors", []))
    if int(result.get("selected_count") or 0) == 0:
        result["status"] = "REFERENCE_INSUFFICIENT"
        runtime_status = "阶段0-参考不足"
    elif result["failed_count"] and result["completed_count"]:
        result["status"] = "PARTIAL"
        runtime_status = "阶段0-部分完成"
    elif result["failed_count"]:
        result["status"] = "FAILED"
        runtime_status = "阶段0-方向全部失败"
    else:
        result["status"] = "COMPLETED"
        runtime_status = "阶段0-预览完成" if preview_only else "阶段0-文本完成"
    storage.update_run_status(
        stage0_run_id,
        runtime_status=runtime_status,
        error_message="；".join(
            str(item.get("error_message") or "")
            for item in result.get("direction_errors", [])
            if item.get("error_message")
        )[:4000],
        stage_durations={},
        completed=True,
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="原创脚本真实执行参考阶段0（不写飞书）")
    parser.add_argument("--product-code", action="append", dest="product_codes")
    parser.add_argument("--directions", type=int, default=2)
    parser.add_argument("--preview-only", action="store_true", help="只检索执行卡，不调用模型和口播引擎")
    parser.add_argument("--skip-voiceover", action="store_true", help="只验证视觉脚本，全部镜头保持静默")
    parser.add_argument("--voiceover-root", default="/Users/likeu3/voiceover_copy_engine")
    parser.add_argument("--voiceover-db-path", default="", help="阶段0口播隔离库；默认写入output-dir")
    parser.add_argument("--voiceover-model-command", default="", help="中央口播正式模型命令；留空使用确定性回归适配器")
    parser.add_argument(
        "--require-model-voiceover",
        action="store_true",
        help="质量测试必须使用正式口播模型；未配置模型命令时直接停止",
    )
    parser.add_argument("--voiceover-qc-model-command", default="", help="中央口播独立质检模型命令")
    parser.add_argument(
        "--voiceover-candidates-only",
        action="store_true",
        help="只生成中央口播候选，不组装完整脚本",
    )
    parser.add_argument("--voiceover-candidate-count", type=int, default=1)
    parser.add_argument("--selected-voiceover-candidate-id", default="")
    parser.add_argument(
        "--blueprint-model",
        default=BLUEPRINT_LLM_DEFAULT_MODEL,
        help="完整脚本蓝图专用模型；默认gpt-5.6-sol",
    )
    parser.add_argument(
        "--blueprint-reasoning-effort",
        default=BLUEPRINT_LLM_DEFAULT_REASONING_EFFORT,
        help="完整脚本蓝图推理强度；默认high",
    )
    parser.add_argument(
        "--output-dir",
        default=str(WORKSPACE_ROOT / "structure_router_test" / "reality_reference_stage0"),
    )
    args = parser.parse_args()
    if args.require_model_voiceover and not str(args.voiceover_model_command or "").strip():
        parser.error("本次是口播质量测试，必须显式传入 --voiceover-model-command")
    products = args.product_codes or DEFAULT_PRODUCTS
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    candidate_snapshot_path = output_dir / "voiceover_candidate_snapshot.json"
    voiceover_candidate_snapshot: Dict[str, Any] = {}
    if args.selected_voiceover_candidate_id and candidate_snapshot_path.exists():
        try:
            loaded_snapshot = json.loads(candidate_snapshot_path.read_text(encoding="utf-8"))
            if isinstance(loaded_snapshot, dict):
                voiceover_candidate_snapshot = loaded_snapshot
        except (OSError, json.JSONDecodeError):
            voiceover_candidate_snapshot = {}
    voiceover_db_path = str(
        Path(args.voiceover_db_path).expanduser().resolve()
        if args.voiceover_db_path
        else output_dir / "voiceover_stage0.sqlite3"
    )
    storage = PipelineStorage()
    # Stage-0 creative history is intentionally isolated from the production
    # RDS schema.  It can be promoted later after the text experiment passes.
    creative_storage = PipelineStorage(
        output_dir / "creative_pattern_stage0.sqlite3",
        database_url="sqlite",
    )
    llm = None if args.preview_only else OriginalScriptLLMClient(route="primary", timeout=240)
    blueprint_llm = (
        None
        if args.preview_only
        else OriginalScriptLLMClient(
            route="primary",
            primary_model=args.blueprint_model,
            primary_reasoning_effort=args.blueprint_reasoning_effort,
            timeout=240,
        )
    )
    if blueprint_llm is not None:
        print(
            "🧠 完整脚本蓝图模型: "
            f"{blueprint_llm.primary_model} / {blueprint_llm.primary_reasoning_effort}"
        )
    results: List[Dict[str, Any]] = []
    product_errors: List[Dict[str, Any]] = []
    for product_code in products:
        print(f"\n🧩 阶段0产品: {product_code}")
        try:
            result = run_product(
                storage,
                creative_storage,
                llm,
                product_code=product_code,
                directions=max(1, min(4, args.directions)),
                preview_only=bool(args.preview_only),
                skip_voiceover=bool(args.skip_voiceover),
                voiceover_root=args.voiceover_root,
                voiceover_db_path=voiceover_db_path,
                blueprint_llm=blueprint_llm,
                voiceover_model_command=args.voiceover_model_command,
                voiceover_qc_model_command=args.voiceover_qc_model_command,
                # References are unique inside one product package. Across
                # products they may be reused, otherwise earlier SKUs can
                # exhaust the small independent-video reference pool.
                recent_execution_card_ids=[],
                recent_source_video_ids=[],
                voiceover_candidates_only=bool(args.voiceover_candidates_only),
                voiceover_candidate_count=max(1, min(3, int(args.voiceover_candidate_count))),
                selected_voiceover_candidate_id=str(args.selected_voiceover_candidate_id or ""),
                voiceover_candidate_snapshot=voiceover_candidate_snapshot,
            )
        except Exception as exc:
            # A stage-0 failure is still a terminal experiment result.  Leaving
            # it as "started" makes operational monitoring and later audits lie.
            failed_run_id: Optional[int] = None
            for row in storage.query_runs_by_product_code(product_code, limit=20):
                if not str(row["record_id"] or "").startswith("stage0-reality:"):
                    continue
                if str(row["runtime_status"] or "") != "started":
                    continue
                failed_run_id = int(row["run_id"])
                storage.update_run_status(
                    failed_run_id,
                    runtime_status="阶段0-失败",
                    stage_durations={},
                    error_message=str(exc)[:2000],
                    completed=True,
                )
                creative_storage.release_creative_patterns_for_run(failed_run_id)
                break
            failure = {
                "product_code": product_code,
                "stage0_run_id": failed_run_id,
                "status": "FAILED",
                "selected_count": 0,
                "completed_count": 0,
                "failed_count": 1,
                "directions": [],
                "direction_errors": [],
                "product_error": {
                    "error_type": type(exc).__name__,
                    "error_message": str(exc)[:4000],
                },
                "baselines": {},
            }
            results.append(failure)
            product_errors.append(failure["product_error"] | {"product_code": product_code})
            print(f"  ❌ 产品失败，继续下一产品：{exc}")
            _write_partial_batch_result(
                output_dir,
                preview_only=bool(args.preview_only),
                skip_voiceover=bool(args.skip_voiceover),
                requested_products=list(products),
                results=results,
                product_errors=product_errors,
            )
            continue
        results.append(result)
        print(f"  ✅ 参考方向 {result['selected_count']} 条，stage0_run_id={result['stage0_run_id']}")
        _write_partial_batch_result(
            output_dir,
            preview_only=bool(args.preview_only),
            skip_voiceover=bool(args.skip_voiceover),
            requested_products=list(products),
            results=results,
            product_errors=product_errors,
        )
    payload = _write_partial_batch_result(
        output_dir,
        preview_only=bool(args.preview_only),
        skip_voiceover=bool(args.skip_voiceover),
        requested_products=list(products),
        results=results,
        product_errors=product_errors,
    )
    result_path = output_dir / "stage0_result.json"
    if not args.preview_only:
        (output_dir / "voiceover_candidates.md").write_text(
            _build_voiceover_candidates_markdown(results), encoding="utf-8"
        )
        if any(
            direction.get("voiceover_candidates")
            for product in results
            for direction in product.get("directions", [])
            if isinstance(direction, dict)
        ):
            candidate_snapshot_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        blind_markdown, answer_key = _build_blind_markdown(results)
        (output_dir / "blind_review.md").write_text(blind_markdown, encoding="utf-8")
        (output_dir / "blind_answer_key.json").write_text(
            json.dumps(answer_key, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    print(f"\n结果：{result_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
