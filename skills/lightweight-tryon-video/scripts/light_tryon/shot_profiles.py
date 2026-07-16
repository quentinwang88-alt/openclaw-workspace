from __future__ import annotations

from typing import Any


FULL_FIXED = "SHOT_FULL_FIXED"
UPPER_FIXED = "SHOT_UPPER_FIXED"
UPPER_THREE_QUARTER = "SHOT_UPPER_THREE_QUARTER"
UPPER_PUSH_IN = "SHOT_UPPER_PUSH_IN"


SHOT_PROFILES: dict[str, dict[str, Any]] = {
    FULL_FIXED: {
        "shot_profile_id": FULL_FIXED,
        "shot_profile_name": "全身固定",
        "shot_type": "full_body",
        "camera_motion": "fixed",
        "framing_type": "vertical_centered",
        "camera_height": "chest_level",
        "camera_angle": "front_flat",
        "subject_position": "center_display_zone",
        "subject_scale": "35%-45%",
        "movement_boundary": "small",
        "prompt_core": "竖屏9:16，胸口高度正面平视，全身完整入镜；单镜头固定机位，镜头位置、焦段和构图全程不动。",
        "consistency_prompt": "人物头顶到双脚持续完整可见，固定正面机位和全身构图。",
        "prompt_negative": "不要镜头推进、缩放、摇移、切镜，不要裁掉头顶或双脚。",
    },
    UPPER_FIXED: {
        "shot_profile_id": UPPER_FIXED,
        "shot_profile_name": "上半身固定",
        "shot_type": "upper_body",
        "camera_motion": "fixed",
        "framing_type": "vertical_upper_body",
        "camera_height": "chest_level",
        "camera_angle": "front_flat",
        "subject_position": "center_display_zone",
        "subject_scale": "45%-55%",
        "movement_boundary": "very_small",
        "prompt_core": "竖屏9:16，胸口高度正面平视，上半身至胯部构图；单镜头固定机位，完整保留上装领口、肩线、袖型、门襟和下摆。",
        "consistency_prompt": "镜头位置、焦段和上半身构图全程固定，上装关键结构持续完整可见。",
        "prompt_negative": "不要大头近景，不要镜头移动，不要裁掉上装下摆、袖口或门襟。",
    },
    UPPER_THREE_QUARTER: {
        "shot_profile_id": UPPER_THREE_QUARTER,
        "shot_profile_name": "上半身至大腿固定",
        "shot_type": "upper_three_quarter",
        "camera_motion": "fixed",
        "framing_type": "vertical_head_to_mid_thigh",
        "camera_height": "chest_level",
        "camera_angle": "front_flat",
        "subject_position": "center_display_zone",
        "subject_scale": "50%-60%",
        "movement_boundary": "very_small",
        "prompt_core": "竖屏9:16，胸口高度正面平视，从头顶构图至大腿中段；单镜头固定机位，上装占人物展示区域约60%，完整保留领口、肩线、袖型、门襟、口袋和下摆，下装只露腰头、胯部和大腿上半截。",
        "consistency_prompt": "镜头位置、焦段和头顶至大腿中段构图全程固定，上装始终是画面绝对主体。",
        "prompt_negative": "不要全身构图，不要出现膝盖、完整裤腿、脚部或鞋子，不要裁掉上装下摆、袖口或门襟。",
    },
    UPPER_PUSH_IN: {
        "shot_profile_id": UPPER_PUSH_IN,
        "shot_profile_name": "上半身缓慢推近",
        "shot_type": "upper_body",
        "camera_motion": "push_in",
        "framing_type": "vertical_slow_push_in",
        "camera_height": "chest_level",
        "camera_angle": "front_flat",
        "subject_position": "center_display_zone",
        "subject_scale": "40%-55%",
        "movement_boundary": "very_small",
        "prompt_core": "竖屏9:16，胸口高度正面平视，镜头从大半身沿正面光轴极慢、匀速、平稳推近至上半身，不变焦、不摇移、不改变角度；上装领口、肩线、袖型、门襟和下摆始终完整可见。",
        "consistency_prompt": "只允许一次沿光轴的极慢平稳推近，推近过程中人物比例和上装完整性稳定。",
        "prompt_negative": "不要突然加速、后退、横移、摇移、变焦、切镜或推到大头近景，不要裁掉上装关键结构。",
    },
}


LEGACY_SCENE_PROFILE = {
    "SCENE_A_001": FULL_FIXED,
    "SCENE_B_001": UPPER_FIXED,
    "SCENE_C_001": FULL_FIXED,
    "SCENE_D_001": UPPER_FIXED,
    "SCENE_E_001": UPPER_PUSH_IN,
}


def get_shot_profile(profile_id: str | None, scene: dict[str, Any] | None = None) -> dict[str, Any]:
    resolved = str(profile_id or "").strip()
    if not resolved and scene:
        resolved = LEGACY_SCENE_PROFILE.get(str(scene.get("scene_id") or ""), "")
    if not resolved and scene:
        scene_type = str(scene.get("scene_type") or "")
        if scene_type in {"upper_body_fixed", "half_body_detail"}:
            resolved = UPPER_FIXED
        elif scene_type == "slow_push_in" or scene.get("camera_motion") == "push_in":
            resolved = UPPER_PUSH_IN
    resolved = resolved or FULL_FIXED
    if resolved not in SHOT_PROFILES:
        raise ValueError(f"未知镜头策略: {resolved}")
    return dict(SHOT_PROFILES[resolved])


def _configured_sequence(shot_plan: dict[str, Any], count: int) -> list[str]:
    if count == 1:
        sequence = list(shot_plan.get("single_sequence") or [])
    elif count == 5:
        sequence = list(shot_plan.get("five_sequence") or [])
    else:
        cycle = list(shot_plan.get("fallback_cycle") or shot_plan.get("five_sequence") or shot_plan.get("single_sequence") or [])
        sequence = [cycle[index % len(cycle)] for index in range(count)] if cycle else []
    if len(sequence) != count:
        raise ValueError(f"镜头方案 {shot_plan.get('shot_plan_id') or ''} 未配置 {count} 条有效镜头序列")
    for profile_id in sequence:
        get_shot_profile(str(profile_id))
    return [str(item) for item in sequence]


def shot_profile_sequence(category: str, count: int, shot_plan: dict[str, Any] | None = None) -> list[str]:
    if shot_plan:
        return _configured_sequence(shot_plan, count)
    normalized = str(category or "").strip().lower()
    outerwear_categories = {"outerwear", "外套"}
    upper_categories = {
        "top", "tshirt", "tank_top", "knit_top", "shirt", "outerwear",
        "上装", "上衣", "外套", "t恤", "背心", "吊带", "针织", "针织衫", "衬衫",
    }
    if normalized in outerwear_categories:
        if count == 1:
            return [UPPER_FIXED]
        if count == 5:
            return [UPPER_FIXED, UPPER_FIXED, UPPER_FIXED, UPPER_THREE_QUARTER, UPPER_PUSH_IN]
    if normalized in upper_categories:
        if count == 1:
            return [FULL_FIXED]
        if count == 5:
            return [FULL_FIXED, FULL_FIXED, UPPER_FIXED, UPPER_FIXED, UPPER_PUSH_IN]
    # 非标准条数保持稳定、细节、轻动态的循环；镜头策略不再要求运营重复建环境。
    cycle = [FULL_FIXED, FULL_FIXED, UPPER_FIXED, UPPER_PUSH_IN]
    return [cycle[index % len(cycle)] for index in range(count)]
