#!/usr/bin/env python3
"""
对原创脚本历史样本做轻量压力测试。

当前聚焦：
- 15 秒硬节点规则触发分布
- review 耗时 / 通过率 / 修正幅度
- 方向重叠类本地规则的命中情况

默认从 original-script-generator 的 SQLite 持久化库里抽取最近样本。
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


SKILL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_DIR))

from core.business_rules import validate_script_direction_separation, validate_script_time_nodes  # noqa: E402
from core.constants import SCRIPT_ROLE_DEFAULTS_BY_STRATEGY  # noqa: E402
from core.storage import default_db_path  # noqa: E402


REVIEW_STAGE_PATTERN = re.compile(r"script_review_s(\d+)")
STRATEGY_ORDER = {"S1": 1, "S2": 2, "S3": 3, "S4": 4}


def _parse_json(value: Any) -> Dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _stage_script_index(stage_name: str) -> Optional[int]:
    match = REVIEW_STAGE_PATTERN.search(str(stage_name or ""))
    return int(match.group(1)) if match else None


def _canonical_review_key(row: sqlite3.Row) -> Optional[Tuple[int, int]]:
    index = _stage_script_index(str(row["stage_name"] or ""))
    if index is None:
        return None
    return int(row["run_id"]), index


def _extract_script_bundle(row: sqlite3.Row) -> Optional[Dict[str, Any]]:
    input_context = _parse_json(row["input_context_json"])
    review_json = _parse_json(row["output_json"])
    final_strategy = input_context.get("final_strategy") if isinstance(input_context.get("final_strategy"), dict) else {}
    original_script = input_context.get("script_json") if isinstance(input_context.get("script_json"), dict) else {}
    repaired_script = review_json.get("repaired_script") if isinstance(review_json.get("repaired_script"), dict) else {}
    script_json = repaired_script or original_script
    if not final_strategy or not script_json:
        return None
    strategy_id = str(final_strategy.get("strategy_id", "") or "").strip().upper()
    final_strategy = dict(final_strategy)
    script_role = str(final_strategy.get("script_role", "") or "").strip()
    if not script_role:
        script_role = SCRIPT_ROLE_DEFAULTS_BY_STRATEGY.get(strategy_id, "unknown")
        if script_role != "unknown":
            final_strategy["script_role"] = script_role

    return {
        "run_id": int(row["run_id"]),
        "record_id": str(row["record_id"] or "").strip(),
        "product_code": str(row["product_code"] or "").strip(),
        "stage_name": str(row["stage_name"] or "").strip(),
        "stage_result_id": int(row["stage_result_id"]),
        "duration_seconds": float(row["duration_seconds"] or 0.0),
        "created_at": str(row["created_at"] or "").strip(),
        "final_strategy": final_strategy,
        "script_json": script_json,
        "original_script": original_script,
        "review_json": review_json,
        "strategy_id": strategy_id,
        "script_role": script_role,
        "product_type": str(input_context.get("product_type", "") or "").strip(),
        "passed_review": bool(review_json.get("pass")),
    }


def _count_changed_shots(original_script: Dict[str, Any], repaired_script: Dict[str, Any]) -> int:
    original_storyboard = original_script.get("storyboard") if isinstance(original_script.get("storyboard"), list) else []
    repaired_storyboard = repaired_script.get("storyboard") if isinstance(repaired_script.get("storyboard"), list) else []
    max_len = max(len(original_storyboard), len(repaired_storyboard))
    changed = 0
    tracked_fields = (
        "duration",
        "shot_content",
        "shot_purpose",
        "voiceover_text_target_language",
        "spoken_line_task",
        "person_action",
        "style_note",
        "anchor_reference",
        "task_type",
    )
    for index in range(max_len):
        left = original_storyboard[index] if index < len(original_storyboard) and isinstance(original_storyboard[index], dict) else {}
        right = repaired_storyboard[index] if index < len(repaired_storyboard) and isinstance(repaired_storyboard[index], dict) else {}
        if any(str(left.get(field, "") or "").strip() != str(right.get(field, "") or "").strip() for field in tracked_fields):
            changed += 1
    return changed


def _categorize_rule_message(message: str) -> str:
    text = str(message or "").strip()
    if not text:
        return "unknown"
    if "未检测到 hook" in text:
        return "timing_hook_missing"
    if "hook 完成时间" in text:
        return "timing_hook_late"
    if "未检测到 proof" in text:
        return "timing_proof_missing"
    if "核心 proof 起始时间" in text:
        return "timing_proof_window_miss"
    if "未检测到 decision" in text:
        return "timing_decision_missing"
    if "decision 信号" in text:
        return "timing_decision_late"
    if text.startswith("S4 "):
        return "direction_s4_separation"
    if "同构" in text or "结构上过于同构" in text:
        return "direction_overlap"
    if text.startswith("ai_shot_risk:forbidden"):
        return "ai_shot_risk_forbidden"
    if text.startswith("ai_shot_risk:high"):
        return "ai_shot_risk_high"
    if text.startswith("hair_accessory:"):
        return "hair_accessory_" + (text.split(":", 1)[1] or "other")
    if text.startswith("audio_layer:"):
        return "audio_layer_" + (text.split(":", 1)[1] or "other")
    return "other"


def _flatten_text(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(_flatten_text(item) for item in value.values())
    if isinstance(value, list):
        return " ".join(_flatten_text(item) for item in value)
    return str(value or "")


def _is_hair_accessory_sample(product_type: str, script_json: Dict[str, Any]) -> bool:
    text = f"{product_type} {_flatten_text(script_json)}"
    return any(token in text for token in ("发饰", "发夹", "抓夹", "边夹", "刘海夹", "香蕉夹", "竖夹", "鲨鱼夹", "发箍", "发圈"))


def _is_ear_accessory_sample(product_type: str, script_json: Dict[str, Any]) -> bool:
    text = f"{product_type} {_flatten_text(script_json)}"
    return any(token in text for token in ("耳线", "耳环", "耳饰", "耳钉", "耳夹", "耳坠"))


def _audit_ai_shot_risk(script_json: Dict[str, Any]) -> List[str]:
    issues: List[str] = []
    storyboard = script_json.get("storyboard") if isinstance(script_json.get("storyboard"), list) else []
    for shot in storyboard:
        if not isinstance(shot, dict):
            continue
        risk = str(shot.get("ai_shot_risk", "") or "").strip()
        if risk == "forbidden":
            issues.append("ai_shot_risk:forbidden")
        elif risk == "high":
            issues.append("ai_shot_risk:high")
    return issues


def _audit_hair_accessory_rules(product_type: str, script_json: Dict[str, Any]) -> List[str]:
    if not _is_hair_accessory_sample(product_type, script_json):
        return []
    storyboard = script_json.get("storyboard") if isinstance(script_json.get("storyboard"), list) else []
    first_shot = storyboard[0] if storyboard and isinstance(storyboard[0], dict) else {}
    first_text = _flatten_text(first_shot)
    all_text = _flatten_text(script_json)
    issues: List[str] = []
    if not any(token in first_text for token in ("夹好", "已夹", "上头", "发型结果", "固定结果", "侧后", "后脑")):
        issues.append("hair_accessory:first_result_missing")
    if not any(token in all_text for token in ("横夹", "竖夹", "侧边", "后脑", "半扎", "散发整理", "装饰点缀", "固定头发")):
        issues.append("hair_accessory:wearing_relation_unclear")
    if any(token in all_text for token in ("完整夹发过程", "反复调整发夹", "大幅甩头", "复杂盘发")):
        issues.append("hair_accessory:risky_process_dependency")
    if not any(token in all_text for token in ("固定", "夹住", "收住", "发型更完整", "更整齐", "位置稳定")):
        issues.append("hair_accessory:fixation_proof_missing")
    return issues


def _audit_audio_layer(product_type: str, script_json: Dict[str, Any]) -> List[str]:
    audio_layer = script_json.get("audio_layer") if isinstance(script_json.get("audio_layer"), dict) else {}
    if not audio_layer:
        return ["audio_layer:missing"]
    issues: List[str] = []
    bgm_energy = str(audio_layer.get("bgm_energy", "") or "").strip()
    if bgm_energy not in {"low", "medium"}:
        issues.append("audio_layer:invalid_bgm_energy")
    if str(audio_layer.get("voiceover_priority", "") or "").strip() != "high":
        issues.append("audio_layer:voiceover_priority_not_high")
    cues = audio_layer.get("sfx_cues") if isinstance(audio_layer.get("sfx_cues"), list) else []
    if len(cues) > 3:
        issues.append("audio_layer:sfx_overdense")
    cue_text = _flatten_text(cues)
    if _is_hair_accessory_sample(product_type, script_json) and cues and not any(token in cue_text for token in ("soft_click", "hair_rustle")):
        issues.append("audio_layer:hair_accessory_fixed_sfx_missing")
    if _is_ear_accessory_sample(product_type, script_json) and any(token in cue_text for token in ("bling", "夸张", "连续闪光", "metal_crash")):
        issues.append("audio_layer:ear_accessory_over_bling")
    if any(token in _flatten_text(audio_layer) for token in ("盖住口播", "强鼓点", "游戏音效")) and bgm_energy == "medium":
        issues.append("audio_layer:possible_voiceover_masking")
    return issues


def _load_latest_review_rows(
    conn: sqlite3.Connection,
    *,
    record_id: str = "",
    product_code: str = "",
    run_ids: Optional[Iterable[int]] = None,
) -> List[sqlite3.Row]:
    conditions = [
        "status = 'success'",
        "stage_name LIKE 'script_review_s%'",
        "output_json IS NOT NULL",
        "input_context_json IS NOT NULL",
    ]
    params: List[Any] = []
    if record_id:
        conditions.append("record_id = ?")
        params.append(record_id)
    if product_code:
        conditions.append("product_code = ?")
        params.append(product_code)
    if run_ids:
        run_ids = list(run_ids)
        placeholders = ",".join("?" for _ in run_ids)
        conditions.append(f"run_id IN ({placeholders})")
        params.extend(run_ids)

    query = f"""
        SELECT stage_result_id, run_id, record_id, product_code, stage_name, duration_seconds, output_json, input_context_json, created_at
        FROM stage_results
        WHERE {' AND '.join(conditions)}
        ORDER BY stage_result_id DESC
    """
    rows = conn.execute(query, params).fetchall()

    deduped: Dict[Tuple[int, int], sqlite3.Row] = {}
    for row in rows:
        key = _canonical_review_key(row)
        if key is None or key in deduped:
            continue
        deduped[key] = row
    return sorted(deduped.values(), key=lambda item: int(item["stage_result_id"]), reverse=True)


def run_pressure_test(
    db_path: Path,
    limit: int,
    record_id: str = "",
    product_code: str = "",
) -> Dict[str, Any]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        latest_rows = _load_latest_review_rows(conn, record_id=record_id, product_code=product_code)
        selected_rows = latest_rows[:limit]
        if not selected_rows:
            return {
                "db_path": str(db_path),
                "sample_count": 0,
                "samples": [],
                "trigger_distribution": {},
            }

        run_ids = sorted({int(row["run_id"]) for row in selected_rows})
        sibling_rows = _load_latest_review_rows(conn, run_ids=run_ids)

        run_script_map: Dict[int, Dict[str, Dict[str, Any]]] = defaultdict(dict)
        for row in sibling_rows:
            bundle = _extract_script_bundle(row)
            if not bundle:
                continue
            strategy_id = bundle["strategy_id"]
            if strategy_id:
                run_script_map[bundle["run_id"]][strategy_id] = bundle["script_json"]

        samples: List[Dict[str, Any]] = []
        trigger_counter: Counter[str] = Counter()
        role_counter: Counter[str] = Counter()
        review_pass_counter: Counter[str] = Counter()
        trigger_duration_values: Dict[str, List[float]] = defaultdict(list)
        duration_values: List[float] = []
        changed_shot_values: List[int] = []
        repaired_values: List[int] = []

        for row in selected_rows:
            bundle = _extract_script_bundle(row)
            if not bundle:
                continue

            final_strategy = bundle["final_strategy"]
            strategy_id = bundle["strategy_id"]
            role = bundle["script_role"]
            warnings, violations = validate_script_time_nodes(final_strategy, bundle["script_json"])

            sibling_scripts = run_script_map.get(bundle["run_id"], {})
            current_order = STRATEGY_ORDER.get(strategy_id, 99)
            existing_scripts = {
                item_strategy_id: script_json
                for item_strategy_id, script_json in sibling_scripts.items()
                if STRATEGY_ORDER.get(item_strategy_id, 99) < current_order
            }
            direction_issue = validate_script_direction_separation(
                final_strategy=final_strategy,
                script_json=bundle["script_json"],
                existing_scripts=existing_scripts,
            )

            changed_shots = _count_changed_shots(bundle["original_script"], bundle["script_json"])
            repaired = bundle["original_script"] != bundle["script_json"]

            duration_values.append(bundle["duration_seconds"])
            changed_shot_values.append(changed_shots)
            if repaired:
                repaired_values.append(changed_shots)
            role_counter[role] += 1
            review_pass_counter["pass" if bundle["passed_review"] else "fail"] += 1

            all_rule_messages = list(warnings) + list(violations)
            if direction_issue:
                all_rule_messages.append(direction_issue)
            local_rule_issues: List[str] = []
            local_rule_issues.extend(_audit_ai_shot_risk(bundle["script_json"]))
            local_rule_issues.extend(_audit_hair_accessory_rules(bundle["product_type"], bundle["script_json"]))
            local_rule_issues.extend(_audit_audio_layer(bundle["product_type"], bundle["script_json"]))
            all_rule_messages.extend(local_rule_issues)
            for message in all_rule_messages:
                category = _categorize_rule_message(message)
                trigger_counter[category] += 1
                trigger_duration_values[category].append(bundle["duration_seconds"])

            samples.append(
                {
                    "run_id": bundle["run_id"],
                    "record_id": bundle["record_id"],
                    "product_code": bundle["product_code"],
                    "stage_name": bundle["stage_name"],
                    "created_at": bundle["created_at"],
                    "strategy_id": strategy_id,
                    "script_role": role,
                    "passed_review": bundle["passed_review"],
                    "review_duration_seconds": round(bundle["duration_seconds"], 3),
                    "changed_shots": changed_shots,
                    "repaired": repaired,
                    "timing_warnings": warnings,
                    "timing_violations": violations,
                    "direction_issue": direction_issue or "",
                    "local_rule_issues": local_rule_issues,
                }
            )

        sample_count = len(samples)
        repaired_count = sum(1 for item in samples if item["repaired"])
        pass_count = sum(1 for item in samples if item["passed_review"])
        return {
            "db_path": str(db_path),
            "sample_count": sample_count,
            "avg_review_duration_seconds": round(sum(duration_values) / sample_count, 3) if sample_count else 0.0,
            "review_pass_rate": round(pass_count / sample_count, 4) if sample_count else 0.0,
            "repair_hit_rate": round(repaired_count / sample_count, 4) if sample_count else 0.0,
            "avg_changed_shots_per_sample": round(sum(changed_shot_values) / sample_count, 3) if sample_count else 0.0,
            "avg_changed_shots_when_repaired": round(sum(repaired_values) / len(repaired_values), 3) if repaired_values else 0.0,
            "role_distribution": dict(role_counter),
            "review_distribution": dict(review_pass_counter),
            "trigger_distribution": dict(trigger_counter.most_common()),
            "trigger_avg_duration_seconds": {
                key: round(sum(values) / len(values), 3)
                for key, values in sorted(trigger_duration_values.items(), key=lambda item: sum(item[1]) / len(item[1]), reverse=True)
                if values
            },
            "samples": samples,
        }
    finally:
        conn.close()


def _render_text_report(result: Dict[str, Any]) -> str:
    if not result.get("sample_count"):
        return f"未找到可用样本，数据库: {result.get('db_path', '')}"

    lines = [
        f"数据库: {result.get('db_path', '')}",
        f"样本数: {result.get('sample_count', 0)}",
        f"平均 review 耗时: {result.get('avg_review_duration_seconds', 0.0)}s",
        f"review 通过率: {result.get('review_pass_rate', 0.0):.2%}",
        f"修正命中率: {result.get('repair_hit_rate', 0.0):.2%}",
        f"平均改动镜头数: {result.get('avg_changed_shots_per_sample', 0.0)}",
        f"修正样本平均改动镜头数: {result.get('avg_changed_shots_when_repaired', 0.0)}",
        f"角色分布: {json.dumps(result.get('role_distribution', {}), ensure_ascii=False)}",
        f"review 分布: {json.dumps(result.get('review_distribution', {}), ensure_ascii=False)}",
        "规则触发分布:",
    ]
    for key, value in (result.get("trigger_distribution") or {}).items():
        avg_duration = (result.get("trigger_avg_duration_seconds") or {}).get(key)
        suffix = f" | avg_review={avg_duration}s" if avg_duration is not None else ""
        lines.append(f"- {key}: {value}{suffix}")

    lines.append("样本明细:")
    for item in result.get("samples", []) or []:
        issues: List[str] = []
        issues.extend(item.get("timing_warnings") or [])
        issues.extend(item.get("timing_violations") or [])
        if item.get("direction_issue"):
            issues.append(item["direction_issue"])
        issues.extend(item.get("local_rule_issues") or [])
        issue_text = "；".join(issues[:3]) if issues else "无"
        lines.append(
            "- "
            + f"run={item.get('run_id')} {item.get('strategy_id')} {item.get('script_role')} "
            + f"pass={item.get('passed_review')} repaired={item.get('repaired')} "
            + f"changed_shots={item.get('changed_shots')} issues={issue_text}"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="对原创脚本历史样本做轻量压力测试")
    parser.add_argument("--db-path", default=str(default_db_path()), help="SQLite 数据库路径")
    parser.add_argument("--limit", type=int, default=10, help="抽样数量，默认 10")
    parser.add_argument("--record-id", default="", help="按 record_id 过滤")
    parser.add_argument("--product-code", default="", help="按 product_code 过滤")
    parser.add_argument("--json", action="store_true", help="输出 JSON")
    args = parser.parse_args()

    result = run_pressure_test(
        db_path=Path(args.db_path),
        limit=max(1, int(args.limit)),
        record_id=str(args.record_id or "").strip(),
        product_code=str(args.product_code or "").strip(),
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    print(_render_text_report(result))


if __name__ == "__main__":
    main()
