"""Contract validators shared by all downstream production flows."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List

from .models import ContractValidationResult


PROCESS_TOKENS = ("使用", "佩戴", "戴上", "穿上", "夹上", "固定", "操作", "整理", "展开", "过程")
# Only keep tokens that unambiguously describe a real person as the visual carrier.
# Fashion copy routinely uses terms such as ``上身比例`` / ``上身版型`` and
# still-life direction can mention ``镜前`` or a profile angle without showing a
# person.  Treating those words as hard evidence made valid STATIC_PRODUCT
# contracts fail.
PERSON_TOKENS = ("人物", "真人", "模特", "女生", "男生", "佩戴者", "穿着者", "使用者")
HAND_TOKENS = ("手", "手持", "指尖", "拿起", "翻转")
SUPPORTED_BEATS = {"HOOK", "PROOF", "USE_PROCESS", "ENDING"}
SUPPORTED_CARRIERS = {"HAND_ONLY", "STATIC_PRODUCT", "WEARER_ACTIVE"}


def _hard(contract: Dict[str, Any]) -> Dict[str, Any]:
    return contract.get("hard_constraints") if isinstance(contract.get("hard_constraints"), dict) else {}


def _text(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(_text(item) for item in value.values())
    if isinstance(value, list):
        return " ".join(_text(item) for item in value)
    return str(value or "")


def _person_evidence_text(value: Any) -> str:
    """Read only fields that describe what is visibly present in the shot.

    Creative rationale fields such as ``shot_purpose`` and ``style_note`` can
    legitimately say "avoid relying on a person's figure" while the rendered
    shot is a pure still life.  Those instructions are not visual evidence and
    must not trigger a hard carrier violation.
    """
    if isinstance(value, list):
        return " ".join(_person_evidence_text(item) for item in value)
    if not isinstance(value, dict):
        return _text(value)
    fields = (
        "shot_content",
        "scene_description",
        "visual_description",
        "person_action",
        "subject_action",
        "wearer_action",
        "body_language",
        "product_interaction",
        "action",
        "composition",
    )
    parts = [_text(value.get(field)) for field in fields if value.get(field) is not None]
    performance = value.get("performance")
    if isinstance(performance, dict):
        parts.append(_text(performance))
    return " ".join(parts)


def _contains_positive_person_signal(value: Any) -> bool:
    text = _person_evidence_text(value)
    for phrase in (
        "无人物",
        "没有人物",
        "不出现人物",
        "不得出现人物",
        "无真人",
        "没有真人",
        "不出现真人",
        "不得出现真人",
        "无模特",
        "没有模特",
        "不出现模特",
        "不得出现模特",
        "无使用者",
        "不出现使用者",
        "无人出镜",
        "不出镜",
    ):
        text = text.replace(phrase, "")
    return any(token in text for token in PERSON_TOKENS)


def _collapse_consecutive(values: List[str]) -> List[str]:
    collapsed: List[str] = []
    for value in values:
        if not collapsed or collapsed[-1] != value:
            collapsed.append(value)
    return collapsed


def _observed_beats(shots: List[Dict[str, Any]]) -> tuple[List[str], str]:
    if not shots:
        return [], "NONE"
    explicit = [str(shot.get("structure_beat") or "").strip().upper() for shot in shots]
    if any(explicit):
        if any(beat not in SUPPORTED_BEATS for beat in explicit):
            return explicit, "EXPLICIT_INVALID"
        return _collapse_consecutive(explicit), "EXPLICIT"
    beats: List[str] = ["HOOK"]
    for index, shot in enumerate(shots[1:], 1):
        task = str(shot.get("spoken_line_task") or shot.get("task_type") or "").lower()
        shot_text = _text(shot)
        if any(token in shot_text for token in PROCESS_TOKENS):
            beats.append("USE_PROCESS")
        elif "proof" in task:
            beats.append("PROOF")
    last_task = str(shots[-1].get("spoken_line_task") or shots[-1].get("task_type") or "").lower()
    if "decision" in last_task or "bridge" in last_task:
        beats.append("ENDING")
    return beats, "TEXT_HEURISTIC"


def _is_subsequence(required: List[str], observed: List[str]) -> bool:
    if not required:
        return True
    position = 0
    for beat in observed:
        if position < len(required) and beat == required[position]:
            position += 1
    return position == len(required)


def _validate_common(shots: List[Dict[str, Any]], contract: Dict[str, Any]) -> ContractValidationResult:
    hard = _hard(contract)
    blocking: List[str] = []
    warnings: List[str] = []
    observed_beats, beat_authority = _observed_beats(shots)
    if beat_authority in {"NONE", "TEXT_HEURISTIC"} and hard.get("beat_sequence") != "UNAVAILABLE":
        blocking.append("结构合同存在，但镜头缺少显式 structure_beat，不能用文案关键词代替结构证据")
    if beat_authority == "EXPLICIT_INVALID":
        blocking.append("镜头包含不受支持的 structure_beat")
    required_beats = hard.get("required_beats")
    if isinstance(required_beats, list):
        for beat in required_beats:
            if beat in {"HOOK", "PROOF", "USE_PROCESS", "ENDING"} and beat not in observed_beats:
                blocking.append(f"结构合同要求 {beat}，当前镜头未形成可确认的对应段落")
    beat_sequence = hard.get("beat_sequence")
    if isinstance(beat_sequence, list):
        normalized_sequence = [str(item) for item in beat_sequence if str(item) in {"HOOK", "PROOF", "USE_PROCESS", "ENDING"}]
        if normalized_sequence and not _is_subsequence(normalized_sequence, observed_beats):
            blocking.append(
                f"结构合同要求 Beat 顺序 {'>'.join(normalized_sequence)}，"
                f"当前可确认顺序为 {'>'.join(observed_beats) or 'UNAVAILABLE'}"
            )

    shot_count = hard.get("shot_count")
    if isinstance(shot_count, dict) and shot_count.get("authority") == "VIDEO_MEASURED":
        minimum = shot_count.get("min")
        maximum = shot_count.get("max")
        if minimum is not None and len(shots) < int(minimum):
            blocking.append(f"结构合同实测镜头下限为 {minimum}，当前只有 {len(shots)} 镜")
        if maximum is not None and len(shots) > int(maximum):
            blocking.append(f"结构合同实测镜头上限为 {maximum}，当前有 {len(shots)} 镜")

    full_text = _text(shots)
    carrier = hard.get("content_carrier")
    explicit_carriers = [str(shot.get("carrier_mode") or "").strip().upper() for shot in shots]
    has_explicit_carriers = bool(explicit_carriers) and all(explicit_carriers)
    if carrier in {"WEARER_ACTIVE", "HAND_ONLY", "STATIC_PRODUCT"}:
        if not has_explicit_carriers:
            blocking.append(f"结构合同指定 {carrier}，但镜头缺少显式 carrier_mode")
        elif any(value != carrier for value in explicit_carriers):
            blocking.append(f"结构合同指定 {carrier}，当前存在其它 carrier_mode")
    elif carrier == "MIXED":
        distinct = {value for value in explicit_carriers if value in SUPPORTED_CARRIERS}
        if len(distinct) < 2:
            blocking.append("结构合同指定 MIXED，但当前没有形成至少两种明确画面承载")

    if carrier == "WEARER_ACTIVE" and not _contains_positive_person_signal(shots):
        warnings.append("显式承载为 WEARER_ACTIVE，但画面文本未明确穿戴者/使用者")
    elif carrier == "HAND_ONLY" and not any(token in full_text for token in HAND_TOKENS):
        warnings.append("显式承载为 HAND_ONLY，但画面文本未明确手部承载")
    elif carrier == "STATIC_PRODUCT" and _contains_positive_person_signal(shots):
        blocking.append("结构合同指定 STATIC_PRODUCT，但画面文本出现明显人物承载")

    for index, shot in enumerate(shots, 1):
        if str(shot.get("carrier_mode") or "").strip().upper() == "STATIC_PRODUCT" and _contains_positive_person_signal(shot):
            blocking.append(f"第 {index} 镜指定 STATIC_PRODUCT，但画面文本出现明显人物承载")

    continuity = hard.get("continuity_mode")
    continuity_groups = [str(shot.get("continuity_group") or "").strip() for shot in shots]
    explicit_groups = {value for value in continuity_groups if value}
    if continuity == "SINGLE_SHOT" and len(shots) != 1:
        blocking.append(f"结构合同指定 SINGLE_SHOT，当前拆成 {len(shots)} 个镜头")
    if continuity == "MULTI_CUT" and len(explicit_groups) < 2:
        blocking.append("结构合同指定 MULTI_CUT，但 continuity_group 没有形成多组切镜")
    if continuity == "CONTINUOUS_LOW_CUT":
        if not explicit_groups:
            blocking.append("结构合同指定 CONTINUOUS_LOW_CUT，但镜头缺少 continuity_group")
        elif len(explicit_groups) > 2:
            blocking.append("结构合同指定 CONTINUOUS_LOW_CUT，但当前场景连续组过多")

    visual_hook_type = hard.get("visual_hook_type")
    if isinstance(visual_hook_type, str) and visual_hook_type not in {"", "UNAVAILABLE"}:
        observed_opening = str((shots[0] if shots else {}).get("opening_mechanism") or "").strip().upper()
        if not observed_opening:
            blocking.append(f"结构合同指定 {visual_hook_type}，但首镜缺少 opening_mechanism")
        elif observed_opening != visual_hook_type:
            blocking.append(f"结构合同指定 {visual_hook_type}，当前首镜为 {observed_opening}")

    return ContractValidationResult(
        valid=not blocking,
        blocking_issues=blocking,
        warnings=warnings,
        observed={
            "shot_count": len(shots),
            "observed_beats": observed_beats,
            "beat_authority": beat_authority,
            "carrier_modes": explicit_carriers,
            "continuity_groups": continuity_groups,
            "opening_mechanism": str((shots[0] if shots else {}).get("opening_mechanism") or ""),
            "carrier_text_signals": {
                "person": _contains_positive_person_signal(shots),
                "hand": any(token in full_text for token in HAND_TOKENS),
                "process": any(token in full_text for token in PROCESS_TOKENS),
            },
        },
    )


def validate_script_against_contract(script_json: Dict[str, Any], contract: Dict[str, Any]) -> ContractValidationResult:
    shots = script_json.get("storyboard") if isinstance(script_json.get("storyboard"), list) else []
    return _validate_common([item for item in shots if isinstance(item, dict)], contract)


def validate_video_prompt_against_contract(prompt_json: Dict[str, Any], contract: Dict[str, Any]) -> ContractValidationResult:
    shots = prompt_json.get("shot_execution") if isinstance(prompt_json.get("shot_execution"), list) else []
    return _validate_common([item for item in shots if isinstance(item, dict)], contract)


def validate_direction_diversity(assignments: Iterable[Dict[str, Any]]) -> ContractValidationResult:
    values = list(assignments)
    blocking: List[str] = []
    warnings: List[str] = []
    families = set()
    visual_keys = set()
    for assignment in values:
        contract = assignment.get("structure_contract") if isinstance(assignment, dict) else {}
        identity = contract.get("direction_identity") if isinstance(contract, dict) else {}
        if isinstance(identity, dict):
            families.add(str(identity.get("macro_family_key") or "UNAVAILABLE"))
            visual_keys.add(str(identity.get("visual_archetype_key") or "UNAVAILABLE"))
    if len(values) >= 2 and len(families - {"UNAVAILABLE"}) < 2:
        warnings.append("结构方向未覆盖至少两个叙事家族")
    if len(visual_keys - {"UNAVAILABLE"}) < len(values):
        warnings.append("至少两个方向共享同一视觉执行原型")
    return ContractValidationResult(
        valid=not blocking,
        blocking_issues=blocking,
        warnings=warnings,
        observed={"direction_count": len(values), "family_count": len(families), "visual_key_count": len(visual_keys)},
    )
