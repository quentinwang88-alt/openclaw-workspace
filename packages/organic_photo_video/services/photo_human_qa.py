"""Human-presentation QA: normalize model observations, recompute pass/fail.

The vision model only reports observed facts (scores, head tilt, gaze, pose
family, AI-face signs). This module owns the deterministic verdict: score
thresholds, hard-fail conditions, and group repetition rules. A model-side
``passed`` flag is never trusted — in fact the observation prompt does not
even ask for one.

Standard level additionally re-checks AI-face labels for corroboration
(``_corroborated_ai_face_signs``): a lone ``PLASTIC_SKIN`` is recorded as
evidence instead of triggering a paid regeneration round. Strict level keeps
the original, uncorroborated regime.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY_ID = "TH_CREATOR_REALISM_V1"

SCHEMA_VERSION = "opv-photo-human-presentation-qa-v1"

SCORE_THRESHOLDS = {
    "face_realism": 80,
    "head_posture": 85,
    "body_posture": 80,
    "gesture_naturalness": 75,
    "expression_naturalness": 75,
    "creator_photo_feel": 80,
}

HARD_FAIL_AI_FACE_SIGNS = {"PLASTIC_SKIN", "DOLL_EYES", "FACE_GEOMETRY_ARTIFACT"}

# 标准档（OPV_PHOTO_QA_LEVEL 默认值）下，**孤证**的 PLASTIC_SKIN 不判死，降级为质量提示。
#
# 依据（2026-09-14 越南围巾搭配线 real-gen 实测）：
#   * 同一张图两次结论相反 —— 首关判 `ai_face_signs=[]` 通过，同一 sha256 的文件
#     在随后的整组复核被判 PLASTIC_SKIN；
#   * 同一次调用的 4 张里两张判塑料、两张判干净，把图调出来肉眼比对，被否的与
#     判干净的皮肤质感接近，差别主要在脸的大小与角度（正脸大特写 vs 低头小脸）。
# 一次孤证误判的代价是整组重做（4 张生成图 + 一轮付费重跑），所以要求佐证。
#
# 「有佐证」= 满足任一：
#   a) 同时给出另一个硬失败 AI 脸特征（DOLL_EYES / FACE_GEOMETRY_ARTIFACT）；
#   b) face_realism 低于标准档阈值（80）。
# 佐证不足时该标签只进 quality_warnings（code=PLASTIC_SKIN_UNCONFIRMED），证据仍保留。
# strict 档完全不受影响：任何硬失败 AI 脸特征照旧判死。
PLASTIC_SKIN_CORROBORATION_FACE_REALISM = SCORE_THRESHOLDS["face_realism"]

HEAD_TILT_LEVELS = {"NONE", "MINOR", "OBVIOUS"}
GAZE_VALUES = {"CAMERA", "FORWARD", "SIDE", "DOWN"}
POSE_FAMILIES = {
    "RELAXED_STAND", "WALKING_CANDID", "SCENE_INTERACTION",
    "TURN_BACK", "STATIC_MANNEQUIN",
}
EXPRESSIONS = {"NEUTRAL", "SOFT_SMILE", "CANDID", "FIXED_BEAUTY_SMILE"}

DEFAULT_GROUP_RULES = {
    "min_pose_families": 3,
    "min_gaze_directions": 2,
    "max_static_camera_smile": 1,
    "max_same_direction_minor_tilt": 1,
    "max_fixed_beauty_smile_roles": 1,
}

STATIC_CAMERA_SMILE = ("RELAXED_STAND", "CAMERA", "SOFT_SMILE")


class HumanPresentationQAError(ValueError):
    pass


def load_human_presentation_policy(policy_id: str = DEFAULT_POLICY_ID) -> Dict[str, Any]:
    """Load a human photography contract from config; unknown id fails loudly."""
    path = PACKAGE_ROOT / "config" / "photo_human_presentation" / f"{policy_id}.json"
    if not path.is_file():
        raise HumanPresentationQAError(f"人物摄影合同不存在：{policy_id}")
    return json.loads(path.read_text(encoding="utf-8"))


def _score(value: Any) -> float:
    try:
        return max(0.0, min(100.0, float(value)))
    except (TypeError, ValueError):
        return 0.0


def _enum(value: Any, allowed: Optional[set]) -> str:
    text = str(value or "").strip().upper()
    return text if not allowed or text in allowed else ""


def _flag(value: Any) -> bool:
    return bool(value) if isinstance(value, bool) else str(value or "").strip().lower() in {
        "true", "yes", "1",
    }


def normalize_human_presentation_review(
    raw: Mapping[str, Any], role_order: Sequence[str] = None,
) -> Dict[str, Any]:
    """Coerce raw model output into the observation schema; raise when unusable."""
    if isinstance(raw, list):
        raw = {"roles": raw}
    if not isinstance(raw, Mapping):
        raise HumanPresentationQAError("人物表现观察返回结构无效")
    raw_roles = raw.get("roles")
    if not isinstance(raw_roles, list) or not raw_roles:
        raise HumanPresentationQAError("人物表现观察缺少 roles 数组")
    roles: List[Dict[str, Any]] = []
    for index, entry in enumerate(raw_roles):
        if not isinstance(entry, Mapping):
            raise HumanPresentationQAError("人物表现观察 roles 元素无效")
        role = str(entry.get("role") or "")
        if role_order:
            allowed = [str(value) for value in role_order]
            folded = {value.strip().lower(): value for value in allowed}
            role = folded.get(role.strip().lower(), role)
            if role not in allowed:
                # 模型改写了 role 名时按图片顺序归位（图片顺序由程序控制）。
                role = allowed[index] if index < len(allowed) else ""
        if role_order and not role:
            continue
        if role_order and role not in role_order:
            continue
        raw_scores = entry.get("scores") or {}
        if not isinstance(raw_scores, Mapping):
            raise HumanPresentationQAError(f"{role} 人物表现评分结构无效")
        scores = {key: _score(raw_scores.get(key)) for key in SCORE_THRESHOLDS}
        raw_obs = entry.get("observations") or {}
        if not isinstance(raw_obs, Mapping):
            raise HumanPresentationQAError(f"{role} 人物表现观察结构无效")
        ai_face_signs = [
            str(value).strip().upper()
            for value in (raw_obs.get("ai_face_signs") or [])
            if str(value).strip()
        ]
        observations = {
            "head_tilt": _enum(raw_obs.get("head_tilt"), HEAD_TILT_LEVELS),
            "head_tilt_direction": _enum(
                raw_obs.get("head_tilt_direction"), {"NONE", "LEFT", "RIGHT"}
            ),
            "gaze": _enum(raw_obs.get("gaze"), GAZE_VALUES),
            "pose_family": _enum(raw_obs.get("pose_family"), POSE_FAMILIES),
            "expression": _enum(raw_obs.get("expression"), EXPRESSIONS),
            "ai_face_signs": ai_face_signs,
            "limb_structure_implausible": _flag(raw_obs.get("limb_structure_implausible")),
            "identity_drift": _flag(raw_obs.get("identity_drift")),
        }
        roles.append({
            "role": role,
            "scores": scores,
            "observations": observations,
            "issues": [str(value) for value in entry.get("issues") or []],
            "repair_instruction": str(entry.get("repair_instruction") or ""),
        })
    if role_order and [item["role"] for item in roles] != [str(v) for v in role_order]:
        raise HumanPresentationQAError("人物表现观察没有覆盖全部角色")
    return {
        "schema_version": SCHEMA_VERSION,
        "roles": roles,
        "model_notes": str(raw.get("notes") or ""),
    }


def evaluate_human_presentation(
    review: Mapping[str, Any], *, policy: Mapping[str, Any] = None,
    pose_contracts: Mapping[str, Mapping[str, Any]] = None,
    group_rules: bool = True, role_order: Sequence[str] = None,
    level: str = "",
) -> Dict[str, Any]:
    """Deterministic verdict over normalized observations; never trust the model.

    ``level`` (default from OPV_PHOTO_QA_LEVEL, "standard"): catastrophic-only —
    fail solely on hard disasters (OBVIOUS head tilt, mannequin pose, AI-face
    signs, implausible limbs); scores and group-diversity rules are recorded
    as evidence, not gates. ``strict`` keeps the full threshold regime.
    """
    normalized = (
        review if review.get("schema_version") == SCHEMA_VERSION
        else normalize_human_presentation_review(review, role_order)
    )
    level = (level or os.environ.get("OPV_PHOTO_QA_LEVEL", "standard")).strip().lower()
    strict = level == "strict"
    contract = dict(policy or load_human_presentation_policy())
    group = {**DEFAULT_GROUP_RULES, **dict(contract.get("group") or {})}
    pose_contracts = dict(pose_contracts or {})
    failed_roles: List[str] = []
    role_verdicts: List[Dict[str, Any]] = []
    quality_warnings: List[Dict[str, str]] = []
    for item in normalized["roles"]:
        issues, repair_parts = _role_issues(item, strict=strict)
        passed = not issues
        if not passed:
            failed_roles.append(item["role"])
        instruction = item.get("repair_instruction") or ""
        if not passed and not instruction:
            instruction = _synthetic_repair(item, pose_contracts.get(item["role"]) or {})
        elif not passed:
            instruction = _merge_instruction(
                instruction, pose_contracts.get(item["role"]) or {}
            )
        quality_warnings.extend(
            _role_warnings(item, pose_contracts.get(item["role"]) or {}, strict=strict)
        )
        role_verdicts.append({
            "role": item["role"], "passed": passed, "scores": dict(item["scores"]),
            "observations": dict(item["observations"]), "issues": issues,
            "repair_instruction": instruction,
        })
    group_result, group_issues = (
        _group_verdicts(normalized["roles"], group)
        if group_rules and strict
        else _identical_pose_check(normalized["roles"])
    )
    if group_issues:
        identical_role = str(group_result.get("identical_pose_role") or "")
        failed_roles = failed_roles or (
            [identical_role] if identical_role
            else [item["role"] for item in _roles_in_group_issues(
                normalized["roles"], group_issues
            )]
        )
    if not failed_roles and not group_issues and len(quality_warnings) >= 3:
        # 多样性偏弱的软提示（如四张仅两类动作）记为整组 warning。
        distinct = {
            str(item["observations"].get("pose_family") or "")
            for item in normalized["roles"]
        } - {""}
        if 1 < len(distinct) < 3:
            quality_warnings.append({
                "role": "group", "code": "POSE_DIVERSITY_LOW",
                "message": f"四张仅 {len(distinct)} 类动作，建议后续轮换动作族",
            })
    return {
        "schema_version": SCHEMA_VERSION,
        "qa_level": "strict" if strict else "standard",
        "policy_id": str(contract.get("policy_id") or DEFAULT_POLICY_ID),
        "passed": not failed_roles and not group_issues,
        "roles": role_verdicts,
        "failed_roles": failed_roles,
        "quality_warnings": quality_warnings,
        "group": group_result,
        "group_issues": group_issues,
        "model_notes": normalized.get("model_notes") or "",
    }


def _corroborated_ai_face_signs(
    signs: Sequence[str], scores: Mapping[str, Any],
) -> List[str]:
    """Standard level: keep only the AI-face labels that carry corroboration.

    A lone ``PLASTIC_SKIN`` with a healthy ``face_realism`` score is the one
    label the model is known to issue inconsistently, so it needs a second
    signal before it may cost a whole regeneration round.  Every other hard
    label, and any ``PLASTIC_SKIN`` next to one of them, passes through
    untouched.  See ``PLASTIC_SKIN_CORROBORATION_FACE_REALISM`` for evidence.
    """
    remaining = [sign for sign in signs if sign != "PLASTIC_SKIN"]
    if remaining:
        return list(signs)
    if _score(scores.get("face_realism")) < PLASTIC_SKIN_CORROBORATION_FACE_REALISM:
        return list(signs)
    return []


def _role_warnings(item: Mapping[str, Any],
                   pose_contract: Mapping[str, Any],
                   *, strict: bool = False) -> List[Dict[str, str]]:
    """Non-blocking observations worth recording for human review."""
    role = str(item.get("role") or "")
    observations = dict(item.get("observations") or {})
    scores = dict(item.get("scores") or {})
    warnings: List[Dict[str, str]] = []
    raw_signs = sorted(set(observations.get("ai_face_signs") or []) & HARD_FAIL_AI_FACE_SIGNS)
    if not strict and raw_signs and not _corroborated_ai_face_signs(raw_signs, scores):
        warnings.append({
            "role": role, "code": "PLASTIC_SKIN_UNCONFIRMED",
            "message": (
                f"仅一处『塑料皮肤』标签且 face_realism "
                f"{_score(scores.get('face_realism')):.0f} 未低于 "
                f"{PLASTIC_SKIN_CORROBORATION_FACE_REALISM}，按孤证处理不触发重生；"
                "证据保留，请人工复核"
            ),
        })
    if observations.get("head_tilt") == "MINOR":
        warnings.append({
            "role": role, "code": "HEAD_TILT_MINOR",
            "message": f"轻微歪头（{observations.get('head_tilt_direction') or '?'}）",
        })
    expected_family = str(pose_contract.get("pose_family") or "").strip()
    observed_family = str(observations.get("pose_family") or "").strip()
    if expected_family and observed_family and expected_family != observed_family:
        warnings.append({
            "role": role, "code": "POSE_PARTIAL",
            "message": f"动作与计划存在偏差（计划 {expected_family}，实际 {observed_family}）",
        })
    expected_gaze = str(pose_contract.get("gaze") or "").strip()
    observed_gaze = str(observations.get("gaze") or "").strip()
    if expected_gaze and observed_gaze and expected_gaze != observed_gaze:
        warnings.append({
            "role": role, "code": "GAZE_MISMATCH",
            "message": f"视线方向与计划不一致（计划 {expected_gaze}，实际 {observed_gaze}）",
        })
    if observations.get("identity_drift"):
        warnings.append({
            "role": role, "code": "IDENTITY_NOTE",
            "message": "面部与人物参考存在漂移（AI 链路常态，仅记录）",
        })
    feel = scores.get("creator_photo_feel")
    if isinstance(feel, (int, float)) and 60 <= feel < 80:
        warnings.append({
            "role": role, "code": "FEEL_MEDIOCRE",
            "message": f"creator photo feel 分数一般（{feel:.0f}）",
        })
    return warnings


def _role_issues(item: Mapping[str, Any], *, strict: bool = False) -> tuple[List[str], List[str]]:
    issues: List[str] = []
    repair_parts: List[str] = []
    scores = dict(item.get("scores") or {})
    if strict:
        for key, threshold in SCORE_THRESHOLDS.items():
            if scores.get(key, 0.0) < threshold:
                issues.append(f"{key}={scores.get(key, 0.0):.0f}<{threshold}")
    observations = dict(item.get("observations") or {})
    if observations.get("head_tilt") == "OBVIOUS":
        issues.append("head_tilt=OBVIOUS")
        repair_parts.append("头部明显倾斜")
    if observations.get("pose_family") == "STATIC_MANNEQUIN":
        issues.append("pose_family=STATIC_MANNEQUIN")
        repair_parts.append("姿势像静态人台")
    ai_signs = sorted(set(observations.get("ai_face_signs") or []) & HARD_FAIL_AI_FACE_SIGNS)
    if ai_signs and not strict:
        ai_signs = _corroborated_ai_face_signs(ai_signs, scores)
    if ai_signs:
        issues.append("ai_face_signs=" + ",".join(ai_signs))
        repair_parts.append("AI 脸特征（" + "、".join(ai_signs) + "）")
    if observations.get("limb_structure_implausible"):
        issues.append("limb_structure_implausible")
        repair_parts.append("肢体结构不合理")
    # identity_drift 只记录观察：AI persona + AI 生成的链路下，人脸身份
    # 漂移是模型能力的常态而非缺陷；用户关注的是歪头/假笑/AI 感（已有
    # 专门维度覆盖），不作为硬失败触发付费重生。
    return issues, repair_parts


def _identical_pose_check(roles: List[Mapping[str, Any]]) -> tuple[Dict[str, Any], List[str]]:
    """Standard level's only group gate: all four pages share one pose family.

    Mild "fewer than three action types" stays a recorded warning; only a
    near-identical quartet justifies regenerating the single most repeated
    page once (handled by the caller's targeted repair).
    """
    poses = [str(item["observations"].get("pose_family") or "") for item in roles]
    distinct = {pose for pose in poses if pose}
    identical = len(distinct) == 1 and len(poses) >= 4
    group = {
        "enabled": True,
        "reason": "identical_pose_gate",
        "pose_families": sorted(distinct),
    }
    if identical:
        # Attribute to the last page of the repeated family (keep A as anchor).
        role = str(roles[-1].get("role") or "")
        group["identical_pose_role"] = role
        return group, [f"四张动作几乎完全相同（全部 {poses[0]}），定向重生 {role} 一次"]
    return group, []


def _group_verdicts(roles: List[Mapping[str, Any]], group: Mapping[str, Any]) -> tuple[Dict[str, Any], List[str]]:
    issues: List[str] = []
    pose_families = {
        item["observations"].get("pose_family") for item in roles
        if item["observations"].get("pose_family")
    }
    gazes = {
        item["observations"].get("gaze") for item in roles
        if item["observations"].get("gaze")
    }
    static_smiles = [
        item["role"] for item in roles
        if (item["observations"].get("pose_family"),
            item["observations"].get("gaze"),
            item["observations"].get("expression")) == STATIC_CAMERA_SMILE
    ]
    fixed_smiles = [
        item["role"] for item in roles
        if item["observations"].get("expression") == "FIXED_BEAUTY_SMILE"
    ]
    tilt_directions: Dict[str, List[str]] = {}
    for item in roles:
        observations = item["observations"]
        if observations.get("head_tilt") == "MINOR" and observations.get("head_tilt_direction"):
            tilt_directions.setdefault(observations["head_tilt_direction"], []).append(item["role"])
    if len(pose_families) < int(group.get("min_pose_families", 3)):
        issues.append(
            f"组内动作族仅 {len(pose_families)} 类（{sorted(pose_families)}），"
            f"要求至少 {group.get('min_pose_families', 3)} 类"
        )
    if len(gazes) < int(group.get("min_gaze_directions", 2)):
        issues.append(f"组内视线方向仅 {len(gazes)} 类（{sorted(gazes)}），要求至少 {group.get('min_gaze_directions', 2)} 类")
    if len(static_smiles) > int(group.get("max_static_camera_smile", 1)):
        issues.append(f"正面站立看镜头微笑的静态照超过 {group.get('max_static_camera_smile', 1)} 张：{static_smiles}")
    if len(fixed_smiles) > int(group.get("max_fixed_beauty_smile_roles", 1)):
        issues.append(f"固定美颜式微笑超过 {group.get('max_fixed_beauty_smile_roles', 1)} 张：{fixed_smiles}")
    for direction, roles_in_direction in sorted(tilt_directions.items()):
        if len(roles_in_direction) > int(group.get("max_same_direction_minor_tilt", 1)):
            issues.append(
                f"{len(roles_in_direction)} 张同向（{direction}）轻微歪头：{roles_in_direction}"
            )
    return {
        "enabled": True,
        "pose_families": sorted(pose_families),
        "gaze_directions": sorted(gazes),
        "static_camera_smile_roles": static_smiles,
        "fixed_beauty_smile_roles": fixed_smiles,
        "minor_tilt_directions": {key: value for key, value in tilt_directions.items()},
    }, issues


def _roles_in_group_issues(roles: List[Mapping[str, Any]], group_issues: Sequence[str]) -> List[Mapping[str, Any]]:
    """Whole-group failures re-do every role unless attribution improves later."""
    return roles


def _synthetic_repair(item: Mapping[str, Any], pose_contract: Mapping[str, Any]) -> str:
    observations = dict(item.get("observations") or {})
    parts: List[str] = []
    tilt = observations.get("head_tilt")
    direction = observations.get("head_tilt_direction") or ""
    if tilt in {"MINOR", "OBVIOUS"}:
        parts.append(f"上一版人物头部向{ '左' if direction == 'LEFT' else '右' if direction == 'RIGHT' else '一侧' }倾斜")
    if observations.get("pose_family") == "STATIC_MANNEQUIN":
        parts.append("双臂僵直、姿势像人台")
    if "PLASTIC_SKIN" in (observations.get("ai_face_signs") or []):
        parts.append("皮肤过度磨皮")
    if "DOLL_EYES" in (observations.get("ai_face_signs") or []):
        parts.append("眼睛过大像玻璃眼")
    if observations.get("expression") == "FIXED_BEAUTY_SMILE":
        parts.append("表情为固定微笑")
    if observations.get("identity_drift"):
        parts.append("人物身份与参考图不一致")
    requirement = "；".join(
        str(pose_contract.get(key) or "")
        for key in ("action_zh", "gaze_zh", "head_zh", "body_zh")
        if pose_contract.get(key)
    )
    head = "；".join(filter(None, parts)) or "人物表现未达标准"
    return f"{head}。保持同一人物身份、穿搭和场景；{requirement or '按动作合同重新执行自然动作'}。" if requirement else f"{head}。保持同一人物身份、穿搭和场景，按动作合同重新执行自然动作。"


def _merge_instruction(instruction: str, pose_contract: Mapping[str, Any]) -> str:
    requirement = "；".join(
        str(pose_contract.get(key) or "")
        for key in ("action_zh", "gaze_zh", "head_zh", "body_zh")
        if pose_contract.get(key)
    )
    if requirement and requirement not in instruction:
        return f"{instruction.rstrip('。')}。本角色动作要求：{requirement}。"
    return instruction
