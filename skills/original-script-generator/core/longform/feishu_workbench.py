"""Feishu task-table adapter for the isolated 20–45 second workflow.

The operator launches a long-form batch from the existing product-level task
table. New batches use the current task images and a fresh shared asset plan.
Resume consumes a frozen source snapshot, not recent scripts for that SKU.
Only text contracts and voiceover are created here.  Paid media generation is
still gated by the existing ``进入生产`` checkbox on each exported script row.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

from core.longform.assets import freeze_reference_assets
from core.longform.contracts import stable_id
from core.longform.keyframes import build_keyframe_contracts
from core.longform.model import generate_master_contract
from core.longform.planner import compile_longform_plan
from core.longform.storage import DEFAULT_ASSET_ROOT, LongformStorage
from core.longform.voiceover import DEFAULT_MODEL_COMMAND, run_longform_voiceover
from core.original_batch_storage import BatchStorage
from core.production_script_renderer import _actual_outfit_accessories, _outfit_scene_match_label
from core.production_script_feishu import (
    PRODUCTION_SCRIPT_FIELD_NAMES,
    projection_to_feishu_fields,
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _json(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        loaded = json.loads(str(value or "{}"))
    except (TypeError, json.JSONDecodeError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


SOURCE_POLICY_VERSION = "longform-current-task-source-v3-relaxed-visibility"


def task_input_snapshot(task: Mapping[str, Any]) -> Dict[str, Any]:
    """Only authoritative task fields; no expiring download URLs or status."""
    return {
        **{key: task.get(key) for key in (
            "product_code", "target_country", "target_language", "top_category",
            "product_type", "duration_seconds", "longform_scene_mode",
            "requested_count", "random_seed",
        )},
        "product_images": [
            {key: item.get(key) for key in ("file_token", "name", "size", "sha256")
             if item.get(key) is not None}
            for item in task.get("product_images") or [] if isinstance(item, Mapping)
        ],
    }


def _source_snapshot_path(batch_id: str, asset_root: str | Path) -> Path:
    if not batch_id or any(not (c.isalnum() or c == "_") for c in batch_id):
        raise ValueError("非法长视频批次ID")
    return Path(asset_root) / "batches" / batch_id / "source_plan.json"


def load_longform_source_snapshot(
    batch_id: str, task: Mapping[str, Any], *, asset_root: str | Path = DEFAULT_ASSET_ROOT,
) -> list[Dict[str, Any]]:
    path = _source_snapshot_path(batch_id, asset_root)
    if not path.exists():
        return []
    snapshot = json.loads(path.read_text(encoding="utf-8"))
    if snapshot.get("task_input") != task_input_snapshot(task):
        raise RuntimeError("LONGFORM_INPUT_CHANGED: 任务图片或内容配置已变化，请replan；不能续跑旧商品版本")
    sources = snapshot.get("sources")
    if not isinstance(sources, list) or not sources:
        raise RuntimeError("长视频冻结来源损坏，不能用历史脚本静默替换")
    return sources


def _freeze_source_snapshot(batch_id: str, task: Mapping[str, Any],
                            sources: Sequence[Mapping[str, Any]], asset_root: str | Path) -> None:
    path = _source_snapshot_path(batch_id, asset_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({
        "schema_version": SOURCE_POLICY_VERSION,
        "task_input": task_input_snapshot(task), "sources": list(sources),
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    temporary.replace(path)


def longform_batch_id(record_id: str, task: Mapping[str, Any], *, replan: bool = False) -> str:
    material = {
        "record_id": record_id,
        "task_id": _text(task.get("task_id")),
        "product_code": _text(task.get("product_code")),
        "video_spec": _text(task.get("video_spec")),
        "duration_seconds": int(float(task.get("duration_seconds") or 0)),
        "scene_mode": _text(task.get("longform_scene_mode") or "auto"),
        "requested_count": int(task.get("requested_count") or 0),
        "random_seed": int(task.get("random_seed") or 0),
        "replan": bool(replan),
        "previous_batch_id": _text(task.get("batch_id")) if replan else "",
        "task_input": task_input_snapshot(task),
        "schema_version": SOURCE_POLICY_VERSION,
    }
    return "LFB_" + hashlib.sha256(
        json.dumps(material, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:20].upper()


def _job_id(batch_id: str, item_index: int, source_reference_id: str) -> str:
    return stable_id(
        "LFJ_",
        {
            "batch_id": batch_id,
            "item_index": int(item_index),
            "source_reference_id": source_reference_id,
            "schema_version": "longform-feishu-workbench-v1",
        },
    )


def _render_complete_script(master: Mapping[str, Any], plan: Mapping[str, Any],
                            voiceover: Mapping[str, Any]) -> str:
    world = _json(master.get("production_world"))
    semantic = _json(master.get("semantic_spine"))
    bundle = _json(master.get("longform_argument_bundle"))
    primary = _json(bundle.get("primary_argument"))
    lines = [
        f"完整长视频脚本｜{master.get('product_code', '')}",
        f"目标时长：{plan.get('target_duration_seconds', '')}秒",
        f"分段：{' + '.join(str(item.get('duration_seconds')) + '秒' for item in plan.get('segments') or [])}",
        "",
        "【人物与制作世界】",
        f"人物：{_text(_json(world.get('character')).get('identity') or world.get('person_state'))}",
        f"穿搭：{_text(world.get('outfit') or _json(world.get('outfit_prompt_projection')).get('frozen_outfit'))}",
        f"穿搭模板：{_text(_json(world.get('outfit_contract')).get('template_display_name'))}（{_text(_json(world.get('outfit_contract')).get('template_id'))}）",
        f"穿搭匹配：{_text(_json(_json(world.get('outfit_selection_report')).get('color_affinity')).get('match_status')) or '未限定'}；兼容候选{_json(world.get('outfit_selection_report')).get('eligible_count', '未知')}套",
        f"拍摄关系：{_text(world.get('capture_mode') or world.get('camera'))}",
        "",
        "【内容主线】",
        f"核心处境：{_text(semantic.get('primary_narrative_context'))}",
        f"核心购买理由：{_text(semantic.get('core_buying_reason') or primary.get('text'))}",
    ]
    for item in bundle.get("supporting_arguments") or []:
        if isinstance(item, Mapping) and _text(item.get("text")):
            lines.append(f"支持价值：{_text(item.get('text'))}")
    lines.extend([
        "", "【目标语言口播】", _text(voiceover.get("target_text")),
        "", "【中文口播】", _text(voiceover.get("chinese_translation")),
        "", "【场景与分段】",
    ])
    scene_by_id = {
        _text(item.get("scene_id")): item
        for item in plan.get("scene_blocks") or [] if isinstance(item, Mapping)
    }
    for segment in plan.get("segments") or []:
        if not isinstance(segment, Mapping):
            continue
        scene = _json(scene_by_id.get(_text(segment.get("scene_id"))))
        lines.extend([
            "",
            f"片段{segment.get('segment_id')}｜{segment.get('duration_seconds')}秒｜{segment.get('segment_visual_role', '')}",
            f"场景：{_text(scene.get('location'))}｜{_text(scene.get('moment'))}｜{_text(scene.get('lighting'))}",
        ])
        for unit in segment.get("execution_units") or segment.get("capture_units") or []:
            if not isinstance(unit, Mapping):
                continue
            lines.append(
                f"- {unit.get('execution_priority', '')}/{unit.get('beat', '')}｜"
                f"{_text(unit.get('camera'))}｜{_text(unit.get('visual_content'))}"
            )
    return "\n".join(lines).strip()


def _render_video_prompt(plan: Mapping[str, Any]) -> str:
    lines = [
        f"同一条{int(plan.get('target_duration_seconds') or 0)}秒长视频，按以下片段生成后直接剪辑合并。",
        "全片保持同一人物、同一商品、同一冻结穿搭和同一个开场钩子；后续片段不得重新介绍商品。",
    ]
    for segment in plan.get("segments") or []:
        if not isinstance(segment, Mapping):
            continue
        lines.extend([
            "",
            f"【片段{segment.get('segment_id')}｜{segment.get('duration_seconds')}秒】",
            _text(segment.get("video_prompt")),
        ])
    return "\n".join(lines).strip()


def _projection(job_id: str, batch_id: str, item_index: int,
                master: Mapping[str, Any], plan: Mapping[str, Any],
                voiceover: Mapping[str, Any]) -> Dict[str, Any]:
    world = _json(master.get("production_world"))
    persona = _json(world.get("persona_contract"))
    outfit = _json(world.get("outfit_contract"))
    semantic = _json(master.get("semantic_spine"))
    bundle = _json(master.get("longform_argument_bundle"))
    primary = _json(bundle.get("primary_argument"))
    scenes = [
        _text(item.get("location"))
        for item in plan.get("scene_blocks") or []
        if isinstance(item, Mapping) and _text(item.get("location"))
    ]
    segment_plan = "+".join(
        str(int(item.get("duration_seconds") or 0))
        for item in plan.get("segments") or [] if isinstance(item, Mapping)
    )
    duration = int(plan.get("target_duration_seconds") or master.get("target_duration_seconds") or 0)
    source = _json(master.get("source_lineage"))
    return {
        "script_id": stable_id("LFSCRIPT_", {"job_id": job_id}),
        "product_code": _text(master.get("product_code")),
        "batch_id": batch_id,
        "batch_item_id": job_id,
        "item_index": int(item_index),
        "script_title": f"{duration}秒长视频｜{_text(semantic.get('primary_narrative_context') or primary.get('text'))}"[:120],
        "duration_seconds": duration,
        "video_format": "长视频",
        "segment_plan": segment_plan,
        "longform_job_id": job_id,
        "longform_status": "待审核",
        "top_category": _text(source.get("top_category")),
        "product_type": _text(source.get("product_type")),
        "target_country": _text(master.get("target_country")),
        "target_language": _text(master.get("target_language")),
        "core_selling_point": _text(primary.get("text") or semantic.get("core_buying_reason")),
        "hook_type": _text(semantic.get("hook_id") or master.get("requested_hook_id")),
        "macro_family": "LONGFORM_CONTINUOUS_ARGUMENT",
        "carrier_mode": _text(world.get("carrier_mode") or world.get("presentation_mode")),
        "creative_signature": stable_id("LFCREATIVE_", {"job_id": job_id, "scenes": scenes}),
        "scene_summary": " → ".join(scenes),
        "target_voiceover": _text(voiceover.get("target_text")),
        "chinese_voiceover": _text(voiceover.get("chinese_translation")),
        "complete_script": _render_complete_script(master, plan, voiceover),
        "video_prompt": _render_video_prompt(plan),
        "script_type": "原创脚本",
        "persona_id": _text(persona.get("persona_id")),
        "persona_name": _text(persona.get("template_display_name") or persona.get("persona_name")),
        "persona_contract_json": json.dumps(persona, ensure_ascii=False),
        "outfit_template_id": _text(outfit.get("template_id")),
        "outfit_template_name": _text(outfit.get("template_display_name")),
        "outfit_selection_report": _json(world.get("outfit_selection_report")),
        "outfit_accessories": _actual_outfit_accessories(outfit, {}),
        "outfit_scene_match": _outfit_scene_match_label(_json(world.get("outfit_scene_affinity_contract"))),
        "outfit_persona_match": _text(_json(world.get("outfit_persona_affinity_contract")).get("match_status")),
        "outfit_scene_contract_json": json.dumps(_json(world.get("outfit_scene_affinity_contract")), ensure_ascii=False),
        "outfit_persona_contract_json": json.dumps(_json(world.get("outfit_persona_affinity_contract")), ensure_ascii=False),
        "input_snapshot_hash": stable_id("LFINPUT_", {"master": master, "plan": plan}),
        "model_version": "/".join(_text(_json(master.get("generation_provenance")).get(k))
                                   for k in ("model", "reasoning_effort")) + " + central-voiceover",
    }


def build_longform_text_batch(
    *,
    record_id: str,
    task: Mapping[str, Any],
    batch_id: str,
    source_storage: BatchStorage | None = None,
    longform_storage: LongformStorage | None = None,
    asset_root: str | Path = DEFAULT_ASSET_ROOT,
    blueprint_model: str = "gpt-5.6-sol",
    blueprint_reasoning: str = "high",
    voiceover_model_command: str = DEFAULT_MODEL_COMMAND,
    include_voiceover: bool = True,
    direct_sources: Sequence[Mapping[str, Any]] = (),
) -> Dict[str, Any]:
    """Create or reuse all text-ready long-form jobs for one Feishu task."""

    longform_storage = longform_storage or LongformStorage()
    longform_storage.ensure_schema()
    product_code = _text(task.get("product_code"))
    # source_storage is retained in the API for legacy callers, but recent
    # short scripts no longer own the current task's images or creative plan.
    resumed_rows = {
        int(_json(row.get("master_contract_json")).get("workbench_request", {}).get("item_index") or 0): row
        for row in longform_storage.list_batch_jobs(batch_id)
    }
    frozen_sources = load_longform_source_snapshot(batch_id, task, asset_root=asset_root)
    for resumed in resumed_rows.values():
        recorded_input = _json(_json(resumed.get("master_contract_json")).get("workbench_request")).get("task_input")
        if recorded_input is None and frozen_sources:
            continue  # Validated batch snapshot is sufficient evidence.
        if recorded_input != task_input_snapshot(task):
            raise RuntimeError(
                "LONGFORM_INPUT_CHANGED_OR_UNVERIFIABLE: 旧任务缺当前图片快照或输入已变化，请replan；不能沿用旧商品再展示新图"
            )
    direct_sources = [dict(item) for item in direct_sources if isinstance(item, Mapping)]
    if frozen_sources:
        direct_sources = frozen_sources
    elif direct_sources:
        _freeze_source_snapshot(batch_id, task, direct_sources, asset_root)
    if not resumed_rows and not direct_sources:
        raise RuntimeError(
            "长视频新任务需要当前产品冻结计划；不能按产品编码复用历史短脚本"
        )

    requested = int(task.get("requested_count") or 1)
    duration = int(float(task.get("duration_seconds") or 0))
    scene_mode = _text(task.get("longform_scene_mode") or "auto")
    projections = []
    job_ids = []
    failures = []
    for item_index in range(1, requested + 1):
        # No modulo recycling: a shortage is an explicit partial batch, not a
        # second copy of an existing frozen direction.
        direct_entry = direct_sources[item_index - 1] if item_index <= len(direct_sources) else {}
        resumed_row = resumed_rows.get(item_index)
        source_reference_id = _text(
            direct_entry.get("source_reference_id")
            or direct_entry.get("source_plan_item_id")
            or f"DIRECT_PRODUCT_{product_code}_{item_index}"
        )
        job_id = _text((resumed_row or {}).get("job_id")) or _job_id(batch_id, item_index, source_reference_id)
        try:
            row = resumed_row or longform_storage.get_job(job_id)
            if row is None:
                source = dict(direct_entry.get("source") or {})
                if not source:
                    raise RuntimeError("DIRECT_PRODUCT_PLAN 本槽位无可用冻结方向，不复制其他槽位")
                source_materials = (
                    source,
                    direct_entry.get("product_context") or {},
                    direct_entry.get("frozen_package") or {},
                    {"persona_reference_assets": direct_entry.get("persona_reference_assets") or []},
                )
                source["top_category"] = _text(task.get("top_category"))
                source["product_type"] = _text(task.get("product_type"))
                source["scene_mode"] = scene_mode
                source["workbench_request"] = {
                    "schema_version": SOURCE_POLICY_VERSION,
                    "record_id": record_id,
                    "batch_id": batch_id,
                    "item_index": item_index,
                    "requested_count": requested,
                    "random_seed": int(task.get("random_seed") or 0),
                    "task_input": task_input_snapshot(task),
                    "product_input_hash": _text((direct_entry.get("product_context") or {}).get("input_hash")),
                }
                lineage = _json(source.get("source_lineage"))
                lineage.update({
                    "operation_record_id": record_id,
                    "longform_batch_id": batch_id,
                    "item_index": item_index,
                    "source_batch_id": _text(direct_entry.get("source_plan_batch_id")),
                    "source_batch_item_id": _text(direct_entry.get("source_plan_item_id")),
                    "source_plan_item_id": _text(
                        direct_entry.get("source_plan_item_id")
                    ),
                    "source_plan_batch_id": _text(
                        direct_entry.get("source_plan_batch_id")
                    ),
                    "source_selection_mode": "DIRECT_PRODUCT",
                    "source_policy_version": SOURCE_POLICY_VERSION,
                    "top_category": _text(task.get("top_category")),
                    "product_type": _text(task.get("product_type")),
                })
                source["source_lineage"] = lineage
                persona_lock = _json(_json(source.get("production_world")).get("persona_contract"))
                frozen_assets = freeze_reference_assets(
                    job_id=job_id, asset_root=asset_root,
                    source_script_id="",  # Current sources only; no historical composite lookup.
                    materials=source_materials, persona_lock=persona_lock,
                )
                if task.get("product_images") and not any(
                    item.get("role") == "PRODUCT_REFERENCE"
                    for item in frozen_assets.get("assets") or []
                ):
                    raise RuntimeError("CURRENT_PRODUCT_REFERENCE_UNAVAILABLE: 当前任务原图未能冻结，不使用历史合成图替代")
                master = generate_master_contract(
                    source, model=blueprint_model,
                    reasoning_effort=blueprint_reasoning,
                )
                master.setdefault("generation_provenance", {
                    "model": blueprint_model, "reasoning_effort": blueprint_reasoning,
                })
                plan = compile_longform_plan(master)
                master["frozen_reference_assets"] = frozen_assets
                keyframes = build_keyframe_contracts(master, plan)
                longform_storage.save_plan(job_id, master, plan, keyframes)
                row = longform_storage.get_job(job_id)
            master = _json(row.get("master_contract_json"))
            plan = _json(row.get("plan_json"))
            voiceover = _json(row.get("voiceover_json"))
            if include_voiceover and not _text(voiceover.get("target_text")):
                voiceover = run_longform_voiceover(
                    master, plan, model_command=voiceover_model_command,
                )
                longform_storage.update_job(
                    job_id, "VOICEOVER_READY",
                    voiceover_json=json.dumps(voiceover, ensure_ascii=False),
                )
            if include_voiceover and not _text(voiceover.get("target_text")):
                raise RuntimeError("长视频口播尚未就绪")
            if include_voiceover:
                projections.append(
                    _projection(job_id, batch_id, item_index, master, plan, voiceover)
                )
                usage_id = _text((direct_entry.get("frozen_package") or {}).get("creative_usage_id"))
                if usage_id:
                    # The shared planner already reserved this identity. Update
                    # it rather than inserting a second usage on every resume.
                    try:
                        from core.storage import PipelineStorage
                        PipelineStorage(database_url="sqlite").update_creative_pattern_status(
                            usage_id, "MACHINE_SCREENED",
                        )
                    except Exception as exc:
                        print(f"长视频创意使用状态未更新（不影响已完成脚本）：{str(exc)[:160]}")
            job_ids.append(job_id)
        except Exception as exc:  # item isolation; caller decides batch status.
            failures.append({"item_index": item_index, "job_id": job_id, "error": str(exc)})
    return {
        "batch_id": batch_id,
        "product_code": product_code,
        "requested_count": requested,
        "planned_count": len(job_ids),
        "ready_count": len(projections),
        "failed_count": len(failures),
        "job_ids": job_ids,
        "projections": projections,
        "failures": failures,
    }


def export_longform_projections(
    *, target_client: Any, projections: Sequence[Mapping[str, Any]],
    product_images: Sequence[Dict[str, Any]] = (), store_id: str = "",
) -> Dict[str, int]:
    """Idempotently upsert text-ready long-form rows by long-form job id."""

    existing = target_client.list_records(page_size=500)
    job_field = PRODUCTION_SCRIPT_FIELD_NAMES["longform_job_id"]
    by_job = {
        _text(record.fields.get(job_field)): record
        for record in existing if _text(record.fields.get(job_field))
    }
    workflow_names = {
        PRODUCTION_SCRIPT_FIELD_NAMES[key]
        for key in (
            "processing_status", "production_enabled", "review_note", "sync_result",
            "sync_time", "run_task_id", "longform_status", "longform_video",
            "longform_error", "first_frame_requested", "composite_first_frame",
            "first_frame_status",
        )
    }
    creates = []
    updated = 0
    for projection in projections:
        job_id = _text(projection.get("longform_job_id"))
        current = by_job.get(job_id)
        fields = projection_to_feishu_fields(
            dict(projection), product_images=product_images, store_id=store_id,
            include_workflow_defaults=current is None,
        )
        if current is None:
            fields[PRODUCTION_SCRIPT_FIELD_NAMES["first_frame_status"]] = "不适用"
            creates.append({"fields": fields})
        else:
            target_client.update_record_fields(
                current.record_id,
                {key: value for key, value in fields.items() if key not in workflow_names},
            )
            updated += 1
    created = target_client.batch_create_records(creates) if creates else []
    return {"created": len(created), "updated": updated, "skipped": 0}
