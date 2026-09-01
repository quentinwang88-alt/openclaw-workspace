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
    lines.extend([
        "",
        "## 完整口播（中文）", "", str(voiceover.get("chinese_translation") or ""), "",
        "## 完整口播（目标语言）", "", str(voiceover.get("target_text") or ""), "",
        "## 分段", "",
    ])
    for segment in plan.get("segments") or []:
        lines.extend([
            f"### 片段 {segment.get('segment_id')}｜{segment.get('duration_seconds')} 秒", "",
        ])
        for unit in segment.get("capture_units") or []:
            lines.extend([
                f"- `{unit.get('unit_id')}` · `{unit.get('beat')}`：{unit.get('visual_content', '')}",
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
