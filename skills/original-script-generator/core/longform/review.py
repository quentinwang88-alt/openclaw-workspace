from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


def _json(value: Any) -> dict:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        return json.loads(str(value or "{}"))
    except json.JSONDecodeError:
        return {}


def export_review_bundle(row: Mapping[str, Any], output_dir: str | Path) -> dict:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    master = _json(row.get("master_contract_json"))
    plan = _json(row.get("plan_json"))
    keyframes = _json(row.get("keyframe_package_json"))
    voiceover = _json(row.get("voiceover_json"))
    payloads = {
        "master_contract.json": master,
        "longform_plan.json": plan,
        "keyframe_package.json": keyframes,
        "voiceover.json": voiceover,
    }
    for name, payload in payloads.items():
        (root / name).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
        )

    lines = [
        f"# 长视频文本灰度｜{master.get('product_code', '')}", "",
        f"- Job：`{row.get('job_id', '')}`", f"- 状态：`{row.get('status', '')}`",
        f"- 目标时长：{plan.get('target_duration_seconds', '')} 秒", "",
        "## 语义主线", "",
        str((_json(master.get("semantic_spine"))).get("core_buying_reason") or ""), "",
        "## 长视频卖点容量", "",
    ]
    bundle = _json(master.get("longform_argument_bundle"))
    primary = _json(bundle.get("primary_argument"))
    lines.extend([
        f"- 主卖点 `{primary.get('argument_id', '')}`：{primary.get('text', '')}",
    ])
    for item in bundle.get("supporting_arguments") or []:
        lines.append(
            f"- 支撑 `{item.get('argument_id', '')}` · `{item.get('argument_role', '')}`：{item.get('text', '')}"
        )
    lines.extend([
        "", "## 场景块", "",
    ])
    for scene in plan.get("scene_blocks") or []:
        lines.append(
            f"- `{scene.get('scene_id', '')}` · {','.join(scene.get('segment_ids') or [])}："
            f"{scene.get('location', '')}｜{scene.get('narrative_role', '')}"
        )
    progression = _json(plan.get("scene_progression_contract"))
    report = _json(plan.get("visual_progression_report"))
    lines.extend([
        "", "## 视觉推进", "",
        f"- 场景偏好/实际：{progression.get('preferred_scene_count', '')} / {progression.get('resolved_scene_count', '')}",
        f"- 状态：`{report.get('status', '')}`",
        f"- 商品细节片段：{','.join(report.get('product_detail_segment_ids') or []) or '无'}",
    ])
    for warning in report.get("warnings") or []:
        lines.append(f"- 软提示：`{warning}`")
    lines.extend([
        "",
        "## 完整口播（中文）", "", str(voiceover.get("chinese_translation") or ""), "",
        "## 完整口播（目标语言）", "", str(voiceover.get("target_text") or ""), "",
        "## 口播写作输入审计", "",
    ])
    writer_audit = _json(voiceover.get("_writer_input_audit"))
    resource_snapshot = _json(voiceover.get("central_resource_snapshot"))
    reference_audit = _json(resource_snapshot.get("approved_style_reference_audit"))
    native_audit = _json(resource_snapshot.get("native_rhetoric_audit"))
    lines.extend([
        f"- 写作策略：`{writer_audit.get('policy_version', '')}`",
        f"- 模型输入哈希：`{writer_audit.get('writer_input_hash', '')}`",
        f"- 选中参考：{', '.join(writer_audit.get('selected_reference_ids') or []) or '无'}",
        f"- 人工修辞参考数：{reference_audit.get('selected_count', 0)}",
        f"- 本土原句状态：`{native_audit.get('status', '')}`",
        f"- 排除的拍摄观察数：{writer_audit.get('excluded_description_count', 0)}",
        "",
        "## 分段", "",
    ])
    for segment in plan.get("segments") or []:
        lines.extend([
            f"### 片段 {segment.get('segment_id')}｜{segment.get('duration_seconds')} 秒", "",
            f"- 视觉职责：`{segment.get('segment_visual_role', '')}`",
        ])
        for unit in segment.get("execution_units") or segment.get("capture_units") or []:
            lines.extend([
                f"- `{unit.get('unit_id')}` · `{unit.get('execution_priority', '')}` · `{unit.get('beat')}`：{unit.get('visual_content', '')}",
                f"  - 信息增量：{unit.get('information_gain', '')}",
            ])
        lines.append("")
    bridge_contract = _json(plan.get("bridge_contract"))
    boundaries = bridge_contract.get("boundaries") or []
    if not boundaries and bridge_contract.get("planned_state"):
        boundaries = [{
            "from_segment": "A", "to_segment": "B",
            "planned_state": bridge_contract.get("planned_state"),
        }]
    lines.extend(["## 桥接状态", ""])
    for boundary in boundaries:
        lines.extend([
            f"### {boundary.get('from_segment', '')}→{boundary.get('to_segment', '')}",
            f"- 边界：`{boundary.get('boundary_mode', 'CONTINUOUS')}`",
            "", "```json",
            json.dumps(boundary.get("planned_state") or {}, ensure_ascii=False, indent=2),
            "```", "",
        ])
    review_path = root / "human_review.md"
    review_path.write_text("\n".join(lines), encoding="utf-8")
    return {"output_dir": str(root), "review_file": str(review_path), "files": list(payloads)}
