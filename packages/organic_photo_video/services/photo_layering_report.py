"""Human-readable summaries for deterministic temperature-layering QA."""
from __future__ import annotations

from typing import Any, Mapping


FAILURE_LABELS_ZH = {
    "UNKNOWN_BAND": "温度档无法确认",
    "LAYER_COUNT_MISMATCH": "可见层数不符",
    "LAYER_ORDER_MISMATCH": "层栈不是逐层包含",
    "NOT_STACKABLE": "层间不可叠",
    "BASE_INCONSISTENT": "基础层漂移",
    "FORBIDDEN_ITEM_PRESENT": "出现禁用单品",
    "ITEM_OUTSIDE_ALLOWLIST": "单品不在允许枚举",
    "CLAIM_MISMATCH": "温度话术与画面不符",
    "INSUFFICIENT_EVIDENCE": "画面证据不足",
    "PERSON_DISASTER": "人物存在灾难级问题",
    "LAYER_SOURCE_IDENTITY_MISMATCH": "三态人物不一致",
    "LAYER_SOURCE_CAMERA_MISMATCH": "三态机位不一致",
    "QA_SCHEMA_INCOMPLETE": "质检结构不完整",
}


def layering_qa_report(qa: Mapping[str, Any]) -> dict[str, Any]:
    roles = []
    all_codes: list[str] = []
    for page in qa.get("roles") or []:
        codes = [str(value) for value in page.get("failure_codes") or [] if str(value)]
        all_codes.extend(code for code in codes if code not in all_codes)
        roles.append({
            "role": str(page.get("role") or ""),
            "passed": page.get("passed") is True,
            "visible_layer_count": int(page.get("visible_layer_count") or 0),
            "visible_layer_stack": [str(value) for value in page.get("visible_layer_stack") or []],
            "identity_id": str(page.get("identity_id") or ""),
            "camera_signature": str(page.get("camera_signature") or ""),
            "failure_codes": codes,
            "failure_labels_zh": [FAILURE_LABELS_ZH.get(code, code) for code in codes],
            "repair_instruction": str(page.get("repair_instruction") or ""),
        })
    return {
        "passed": qa.get("passed") is True,
        "profile_binding": dict(qa.get("profile_binding") or {}),
        "summary": dict(qa.get("summary") or {}),
        "failure_codes": all_codes,
        "failure_labels_zh": [FAILURE_LABELS_ZH.get(code, code) for code in all_codes],
        "roles": roles,
        "notes": str(qa.get("notes") or ""),
    }


def layering_qa_note_zh(qa: Mapping[str, Any], *, limit: int = 180) -> str:
    report = layering_qa_report(qa)
    state = "通过" if report["passed"] else "未通过"
    role_text = "；".join(
        f"{item['role']}={item['visible_layer_count']}层"
        + ("" if item["passed"] else "（" + "、".join(item["failure_labels_zh"]) + "）")
        for item in report["roles"]
    )
    value = f"温度分层 QA {state}：{role_text}"
    return value[:max(1, int(limit))]


def layering_qa_markdown(qa: Mapping[str, Any]) -> str:
    report = layering_qa_report(qa)
    binding = report["profile_binding"]
    lines = [
        "# 温度分层 Canary QA",
        "",
        f"- 结论：{'通过' if report['passed'] else '未通过'}",
        f"- 温度档：{binding.get('band_key') or 'unknown'}",
        f"- 体感：{binding.get('thermal_sensitivity') or 'unknown'}",
        f"- 场景：{binding.get('scene') or 'unknown'}",
        "",
        "| 角色 | 层数 | 可见层栈 | 人物 | 机位 | 结论 |",
        "| --- | ---: | --- | --- | --- | --- |",
    ]
    for item in report["roles"]:
        verdict = "通过" if item["passed"] else "未通过：" + "、".join(item["failure_labels_zh"])
        lines.append(
            f"| {item['role']} | {item['visible_layer_count']} | "
            f"{' → '.join(item['visible_layer_stack'])} | {item['identity_id']} | "
            f"{item['camera_signature']} | {verdict} |"
        )
    if report["notes"]:
        lines.extend(["", "## 质检备注", "", report["notes"]])
    repairs = [
        f"- {item['role']}：{item['repair_instruction']}"
        for item in report["roles"] if item["repair_instruction"]
    ]
    if repairs:
        lines.extend(["", "## 修复建议", "", *repairs])
    return "\n".join(lines) + "\n"
