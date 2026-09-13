"""Generate four role-bound Looks from style references and a frozen theme."""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from services.image_generator import ShotGenerationRequest
from services.photo_category_registry import (
    WOMENSWEAR_V1,
    adapter_for_product_category,
    product_owns_slot,
)
from services.photo_theme import style_look_specs


def _product_adapter(product: Mapping[str, Any] | None):
    """按商品类目取适配器（Phase 3）。

    未被任何适配器认领的类目（wig、空类目）回退 ``WOMENSWEAR_V1``：这张表历史
    上服务所有商品，回退才能保证这些商品的输出与 Phase 3 之前逐字一致。
    """
    token = str((product or {}).get("category") or "").lower()
    return adapter_for_product_category(token) or WOMENSWEAR_V1


def _product_targets_slot(product: Mapping[str, Any] | None, slot: str) -> bool:
    """商品类目**本身**是否就是占据该槽位的那件商品。

    口径改由 Category Adapter 提供（Phase 1 等价搬迁）。注意这与规划侧的
    "类目→槽位"映射不同：``dress`` 在规划侧写进 outerwear 槽位，但这里的
    原条件是"商品类目就是 outerwear"，所以连衣裙**不**触发覆盖。这个差异
    被原样保留，未做"统一"。

    这里保留本文件原有的 ``lower()`` 归一（**不** strip），带空格的类目串
    行为与搬迁前一致。
    """
    token = str((product or {}).get("category") or "").lower()
    return product_owns_slot(_product_adapter(product), slot, token)


def _target_slot_override(product: Mapping[str, Any] | None) -> str:
    """商品应写入的 **额外** 冻结穿搭槽位；无则返回空串。

    只认适配器声明的 ``main_product_slot``，且必须是该类目**自己拥有**的槽位。
    这样 ``outerwear``（womenswear 的主槽位，已由上方既有分支处理）与
    ``top``/``dress`` 等映射到别的槽位的类目都不会走到这里 —— 只有像 scarf
    这样主槽位为 ``accessories`` 的类目才会新增一个键，TH V2 输出不变。
    """
    adapter = _product_adapter(product)
    slot = str(adapter.main_product_slot or "")
    if not slot or slot == "outerwear":
        return ""
    return slot if _product_targets_slot(product, slot) else ""


MAX_GROUP_REPAIR_ATTEMPTS = 1
REPAIR_NOTE_MARK = "；质检修正："

# Four clear shot intents for the human-scene path. Pseudo-precise camera
# numbers (exact degrees / focal length / horizon / height ratio) were removed
# 2026-09-07: image models cannot execute them reliably and QA cannot verify
# them. Only checkable requirements survive: full body with shoes visible,
# and visibly different action across the four looks.
POSE_CONTRACTS = {
    "look_a": {
        "pose_family": "RELAXED_STAND",
        "gaze": "CAMERA",
        "head_posture": "LEVEL",
        "action_zh": "轻松站立或轻靠栏杆等场景支撑物，一只脚略向前，重心落在另一条腿上",
        "gaze_zh": "看镜头附近，眼神放松",
        "head_zh": "头颈自然水平，不向左右肩膀倾斜",
        "body_zh": "肩膀放松下压，手臂自然垂放微弯",
        "camera_zh": "手机随手拍视角，构图自然即可，不必居中",
        "camera_hint": "完整全身自然站姿，头顶和鞋底完整入镜",
    },
    "look_b": {
        "pose_family": "WALKING_CANDID",
        "gaze": "FORWARD",
        "head_posture": "LEVEL",
        "action_zh": "自然行走，能看出明确的迈步感，手臂随步伐自然摆动",
        "gaze_zh": "看向前方行进方向，不与镜头对视",
        "head_zh": "头颈顺着行走方向保持水平",
        "body_zh": "步幅自然，肩髋随步伐轻微转动",
        "camera_zh": "跟拍视角，人物在画面中行进",
        "camera_hint": "行走中的自然全身抓拍，有迈步感，头顶和鞋底完整入镜",
    },
    "look_c": {
        "pose_family": "SCENE_INTERACTION",
        "gaze": "SIDE",
        "head_posture": "LEVEL",
        "action_zh": "与场景或自身配饰互动：整理袖口、单手插兜或轻触场景中已有物体，手部动作合理自然",
        "gaze_zh": "看手部动作或望向侧面",
        "head_zh": "保持水平，不歪头",
        "body_zh": "上身随动作轻微转动，重心偏向一侧",
        "camera_zh": "从侧方抓拍互动瞬间，可借场景元素做前景层次",
        "camera_hint": "与场景自然互动的全身瞬间，手部动作合理，头顶和鞋底完整入镜",
    },
    "look_d": {
        "pose_family": "TURN_BACK",
        "gaze": "CAMERA",
        "head_posture": "LEVEL",
        "action_zh": "回身或侧身观景，行走过后被叫住般回望，朝向与 A/B 明显不同",
        "gaze_zh": "回望镜头附近",
        "head_zh": "转头但不向肩膀侧倾",
        "body_zh": "肩线与髋部朝向明显不同，姿态放松",
        "camera_zh": "从行进方向侧后方或侧面拍摄",
        "camera_hint": "回身或侧身观景的全身瞬间，与站立/行走方向明显不同，头顶和鞋底完整入镜",
    },
}

# 手势细节变体：同一动作族内按篇确定性轮换，避免每篇动作雷同。
POSE_GESTURE_VARIANTS = {
    "look_a": [
        "一只手轻插外套口袋，另一只手自然垂放微弯",
        "一只手把被风吹起的头发轻别到耳后",
        "双手在身前自然交叠，手指放松",
    ],
    "look_b": [
        "双手轻插外套口袋慢走，肩部放松",
        "一只手拎着包带随步伐轻轻晃动",
        "手臂摆幅稍大，步子带一点轻快节奏",
    ],
    "look_c": [
        "低头整理外套袖口，指尖捏住袖边",
        "单手调整背包肩带，另一只手轻扶栏杆",
        "单手插兜，另一只手轻触身侧的栏杆或墙面",
    ],
    "look_d": [
        "回身时一只手轻轻拨动被带起的头发",
        "回身时手轻轻拎起外套下摆",
        "回身时手臂自然后摆保持平衡",
    ],
}


def pose_gesture_detail(role: str, seed: str) -> str:
    """Deterministic gesture variant per piece (hash of record/family/role)."""
    import zlib

    variants = POSE_GESTURE_VARIANTS.get(role) or []
    if not variants:
        return ""
    return variants[zlib.crc32(str(seed).encode("utf-8")) % len(variants)]

# Legacy fallback hints for paths without a pose contract (travel keeps its
# own scene system this round).
POSE_HINTS = {
    "look_a": "自然正面站立，一只手轻放身侧",
    "look_b": "轻微三分之四侧身，重心自然落在一侧",
    "look_c": "小幅迈步或自然转身，手臂放松",
    "look_d": "正面放松站立，双手姿态与前页不同",
}

TRAVEL_CAMERA_HINTS = {
    "airport_departure": "航站楼内自然光，随身行李入画，完整全身站姿，头顶和鞋底留安全边距",
    "old_town_walk": "老城街道与建筑立面为背景，步行中的自然全身构图",
    "cafe_visit": "咖啡店座位或露台环境，持杯或落座姿态，完整穿搭清晰可见",
    "evening_stroll": "傍晚暖色光线与街灯氛围，完整全身漫步姿态",
    "shopping_day": "商场橱窗与街道环境，自然站姿完整全身",
    "photo_spot": "标志性建筑前取景拍照姿态，完整全身构图",
    "night_walk": "夜晚灯串暖光环境，完整全身自然站姿",
}


class PhotoStyleReferenceError(ValueError):
    pass


def _with_repair_note(purpose: str, note: str) -> str:
    """Attach a QA repair note exactly once, never stacking duplicates."""
    base = str(purpose or "").split(REPAIR_NOTE_MARK, 1)[0]
    return f"{base}{REPAIR_NOTE_MARK}{note}" if note else base


def _with_background_anchor(scene_prompt: str, *, features: Sequence[Any],
                            enabled: bool) -> str:
    """2026-09-08 背景继承放松：不再在生成端追加背景特征注入——
    每页以冻结的 scene_prompt 为最终画面依据，参考图只负责旅行氛围。
    函数保留以兼容旧调用签名。"""
    return str(scene_prompt or "")


def _emit_progress(progress: Any, event: str, **data: Any) -> None:
    if progress is None:
        return
    try:
        progress(event, **data)
    except Exception:  # noqa: BLE001 - progress writes must never break generation
        pass


class PhotoStyleReferenceSupplyService:
    def __init__(self, *, generator: Any, root: Path, vision_service: Any = None):
        self.generator = generator
        self.root = Path(root)
        self.vision_service = vision_service

    def prepare(
        self, *, record_id: str, reference_paths: Sequence[str],
        theme: Mapping[str, Any], account: Any, persona: Mapping[str, Any],
        variation: Mapping[str, Any] = None, progress: Any = None,
        product: Mapping[str, Any] = None,
        locale: str = "th-TH",
    ) -> dict[str, Any]:
        variation = dict(variation or {})
        product = dict(product or {})
        paths = [str(Path(value).expanduser().resolve()) for value in reference_paths]
        if not paths or any(not Path(value).is_file() for value in paths):
            raise PhotoStyleReferenceError("风格参考图缺失或不可读取")
        looks = style_look_specs(theme, variation)
        role_order = [str(look["role"]) for look in looks]
        output_dir = self.root / "style_reference_supply" / self._safe(record_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = output_dir / "supply_manifest.json"
        input_hash = self._input_hash(paths, theme, looks, account, variation, product)
        completed: dict[str, dict[str, Any]] = {}
        prior_status = ""
        prior_group_alignment = None
        group_repair_attempts = 0
        attempt_history: list[dict[str, Any]] = []
        if manifest_path.is_file():
            prior = json.loads(manifest_path.read_text(encoding="utf-8"))
            if prior.get("input_hash") != input_hash:
                raise PhotoStyleReferenceError("参考图或主题已变化；请新建任务，避免混用旧生成结果")
            prior_status = str(prior.get("status") or "")
            prior_group_alignment = prior.get("group_alignment")
            prior_group_human = prior.get("group_human_presentation_qa")
            group_repair_attempts = int(prior.get("group_repair_attempts") or 0)
            attempt_history = [dict(item) for item in prior.get("attempt_history") or []]
            for item in prior.get("sources") or []:
                path = Path(str(item.get("path") or ""))
                if (path.is_file() and item.get("sha256")
                        and hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"]):
                    completed[str(item.get("role") or "")] = dict(item)
        if prior_status == "group_failed":
            raise PhotoStyleReferenceError(
                "整组参考一致性重做次数已用尽；请新建飞书任务，历史证据见 "
                f"{manifest_path}"
            )
        if prior_status == "complete" and all(role in completed for role in role_order):
            ordered = [completed[role] for role in role_order]
            return {
                "schema_version": "opv-photo-style-reference-supply-v1",
                "record_id": record_id, "reference_mode": "STYLE",
                "theme_brief": {**dict(theme), "variation": variation},
                "sources": ordered, "generated_count": len(ordered),
                "generated_this_run": 0, "supply_manifest": str(manifest_path),
                "group_alignment": prior_group_alignment,
                "group_human_presentation_qa": prior_group_human,
                "group_consistency_qa": prior.get("group_consistency_qa"),
                "persona_pack_id": str(prior.get("persona_pack_id") or ""),
                "group_repair_attempts": group_repair_attempts,
                "repair_rounds": len(attempt_history),
                "repaired_roles_this_run": [],
            }
        generated_this_run = 0
        # Roles invalidated by a previous repair round but not yet redone.
        repaired_roles_this_run: list[str] = [
            str(role) for role in ((attempt_history[-1].get("failed_roles") or [])
                                   if attempt_history else [])
            if role in role_order and role not in completed
        ]
        # 每张最多重生一次：attempt_history.retired 是历史重生记录。
        regenerated_roles: set[str] = {
            str(item.get("role") or "")
            for entry in attempt_history
            for item in entry.get("retired") or []
        }
        repair_notes: dict[str, str] = dict(
            attempt_history[-1].get("repair_notes") or {}
        ) if attempt_history else {}
        style_profile = dict(variation.get("style_profile") or {})
        planning_flow = str(
            variation.get("planning_flow") or style_profile.get("planning_flow") or ""
        )
        # 分层图文线（温度分层 / 冷热切换）共享同一套三态生成机制：外层锚点、
        # 反向生成顺序、LAYER_PROGRESSION 参考用途、禁并行预取。差异只在各自的
        # 语义质检模块，因此统一由 registry 谓词判定，避免散落的 flow 字符串比较。
        from services.photo_flow_registry import (
            is_layered_progression_flow, is_thermal_transition_flow,
        )
        thermal_transition_planned = is_thermal_transition_flow(planning_flow)
        layered_planned = is_layered_progression_flow(planning_flow)
        presentation_type = str(
            variation.get("presentation_type") or style_profile.get("presentation_type")
            or "MODEL_FULL_BODY"
        )
        flat_lay = presentation_type == "FLAT_LAY"
        scene_model = presentation_type == "SCENE_MODEL"
        travel_planned = all(str(look.get("travel_moment") or "") for look in looks)
        human_scene = scene_model and not travel_planned and bool(persona)
        # 旅行路径同样绑定人物包身份（2026-09-07 修复：旅行 look_a 曾无人物
        # 参考生成，随机人物经身份锚点污染整组）。
        persona_bound = scene_model and bool(persona) and not flat_lay
        persona_pack_id = ""
        identity_paths: list[str] = []
        human_contract: dict[str, Any] = {}
        color_grading_plan = dict(style_profile.get("color_grading_plan") or {})
        qa_reference_paths = list(dict.fromkeys(
            [str(value) for value in product.get("reference_images") or [] if Path(str(value)).is_file()]
            + list(paths)
        ))
        if persona_bound:
            from services.persona_pack import (
                build_persona_pack, evaluate_persona_pack, select_identity_references,
            )
            pack = build_persona_pack(persona)
            evaluation = evaluate_persona_pack(pack)
            if not evaluation["ready"]:
                raise PhotoStyleReferenceError(
                    "真人场景人物包未通过预检（"
                    + str(getattr(account, "persona_ref_id", "")
                          or persona.get("persona_id") or "未绑定人物")
                    + "）："
                    + "；".join(item["message"] for item in evaluation["issues"])
                    + "。人物包是系统资产，需先在素材库补齐角色化参考并通过人工验收"
                )
            persona_pack_id = str(pack.get("persona_pack_id") or "")
            identity_paths = [str(item["local_path"])
                              for item in select_identity_references(persona)]
            if human_scene:
                from services.photo_human_qa import load_human_presentation_policy
                human_contract = load_human_presentation_policy()
        while True:
            # Human-scene identity comes from the approved persona pack only;
            # a previously generated page is never used as an identity anchor
            # (head-tilt/expression pollution — TH persona realism handoff 6.4).
            identity_anchor = "" if (flat_lay or (persona_bound and not layered_planned)) else next(
                (str(completed[role]["path"]) for role in role_order if role in completed), ""
            )
            # 参考图签名配色（全组共享）：生成端颜色不得偏移出色系。
            reference_palette = [
                str(value) for value in (style_profile.get("palette") or []) if value
            ][:6]

            def build_request(index: int, look: Mapping[str, Any]) -> ShotGenerationRequest:
                role = str(look["role"])
                per_reference = list(style_profile.get("per_reference") or [])
                typed = any(item.get("reference_uses") for item in per_reference
                            if isinstance(item, Mapping))
                selected_outfit = {
                    int(value) for value in look.get("outfit_reference_indices") or []
                    if str(value).isdigit()
                }
                selected_style_paths = []
                for position, path in enumerate(paths, 1):
                    item = next((dict(value) for value in per_reference
                                 if isinstance(value, Mapping)
                                 and int(value.get("index") or 0) == position), {})
                    uses = {str(value) for value in item.get("reference_uses") or []}
                    include = (not typed or "ENVIRONMENT" in uses
                               or "VISUAL_STYLE" in uses
                               or (layered_planned and "LAYER_PROGRESSION" in uses)
                               or ("OUTFIT" in uses
                                   and (not selected_outfit or position in selected_outfit)))
                    if include:
                        selected_style_paths.append(path)
                references = list(dict.fromkeys(
                    selected_style_paths
                    + (identity_paths if persona_bound else [])
                    + ([identity_anchor] if identity_anchor else [])
                ))
                travel_moment = str(look.get("travel_moment") or "")
                travel_scene_prompt = str(look.get("scene_prompt") or "")
                if flat_lay:
                    camera_hint = "正上方俯拍，衣物平铺成一套完整搭配，保持参考图的留白与层次"
                    composition = {
                        "framing": "flat_lay",
                        "instruction": "正上方完整平铺展示外套、内搭、下装和鞋履；无人物、无人台、无衣架",
                        "camera_angle": "top_down",
                        "pose": "garment_flat_lay",
                        "forbidden": ["人物", "人脸", "手脚", "模特", "人台", "试衣间界面", "商品编号", "截图黑边"],
                        "prop_policy": "no_new_props",
                    }
                else:
                    pose_contract = (
                        POSE_CONTRACTS.get(role)
                        if human_scene and not travel_moment and not layered_planned else None
                    )
                    camera_hint = (
                        TRAVEL_CAMERA_HINTS.get(travel_moment)
                        or (pose_contract or {}).get("camera_hint")
                        or POSE_HINTS.get(role, "自然完整全身站姿")
                    )
                    composition = {
                        "framing": "full_body",
                        "instruction": (
                            f"生活场景中的完整全身穿搭，场景要求：{travel_scene_prompt}；"
                            "人物动作自然，头顶和鞋底留安全边距，服装层次清晰"
                            if travel_scene_prompt else
                            "生活场景中的完整全身穿搭，人物动作自然，头顶和鞋底留安全边距，服装层次清晰"
                            if scene_model else
                            "完整全身穿搭，头顶和鞋底留安全边距，服装层次清晰"
                        ),
                        "camera_angle": "eye_level",
                        "pose": (pose_contract or {}).get("action_zh")
                        or POSE_HINTS.get(role, "natural_standing"),
                        "forbidden": ["手机遮脸", "试衣间界面", "商品编号", "截图黑边"],
                        "prop_policy": "no_new_props",
                    }
                    if travel_moment and role == "look_a":
                        composition["instruction"] += (
                            "；本页兼作图文首图，画面上方保留自然、干净的短标题区域，"
                            "人物和穿搭主体保持完整"
                        )
                        composition["cover_overlay_safe_area"] = "top"
                    if pose_contract:
                        gesture = pose_gesture_detail(
                            role, f"{record_id}|{variation.get('family_id')}|{role}"
                        )
                        composition["pose_contract"] = {
                            key: pose_contract[key]
                            for key in ("pose_family", "gaze", "head_posture",
                                        "action_zh", "gaze_zh", "head_zh",
                                        "body_zh", "camera_zh")
                            if pose_contract.get(key)
                        }
                        if gesture:
                            composition["pose_contract"]["gesture_detail_zh"] = gesture
                plan_shot = {
                    "slot_index": index, "slot_role": role,
                    "purpose": (
                        f"{theme['label_zh']}：生成 {role} 完整穿搭（travel_moment={travel_moment}）；"
                        f"本篇方向：{variation.get('angle_zh') or theme['label_zh']}"
                        if travel_moment else
                        f"{theme['label_zh']}：生成 {role} 完整穿搭；"
                        f"本篇方向：{variation.get('angle_zh') or theme['label_zh']}"
                    ),
                    "camera_hint": camera_hint,
                    "composition_contract": composition,
                }
                repair_note = repair_notes.get(role, "")
                if repair_note:
                    plan_shot["purpose"] = _with_repair_note(plan_shot["purpose"], repair_note)
                outfit_state = {
                    "top_inner": look["top_inner"], "bottom": look["bottom"],
                    "shoes": look["shoes"], "style_direction": "；".join(filter(None, [
                        str(theme["visual_brief"]), str(variation.get("style_modifier") or ""),
                        (
                            "穿搭比例与下装鞋履衔接："
                            + str(look.get("styling_intent") or "")
                            if str(look.get("styling_intent") or "").strip() else ""
                        ),
                        (
                            "同一人物身份、同一目的地视觉体系、统一色彩基调；"
                            "各 Look 场景互相独立，按各自 travel_moment 呈现"
                            if travel_moment else ""
                        ),
                        (
                            "全组摄影调色延续参考图气质；当套服装颜色严格执行冻结描述。"
                            "参考图配色仅作为审美方向（"
                            + "、".join(reference_palette[:4])
                            + "），不得把四套配套单品强行收敛到同一色域"
                            if reference_palette else ""
                        ),
                    ])),
                    "outerwear": (
                        "指定商品，以商品参考图的颜色、版型和结构为准"
                        if _product_targets_slot(product, "outerwear")
                        else look["outerwear"]
                    ),
                }
                target_slot = _target_slot_override(product)
                if target_slot:
                    # Phase 3：主槽位不是 outerwear 的类目（scarf → accessories）
                    # 把商品冻结在该槽位。声明 outerwear 主槽位的类目（womenswear）
                    # 永远走不到这里，故 TH V2 输出不变。
                    outfit_state[target_slot] = "指定商品，以商品参考图的颜色、版型和结构为准"
                return ShotGenerationRequest(
                    task_id=f"photo_style_{self._safe(record_id)}",
                    slot_index=index, slot_role="full_look",
                    shot_version=1 + group_repair_attempts,
                    plan_shot=plan_shot,
                    product=product, persona_snapshot={} if flat_lay else dict(persona),
                    look_snapshot={"recipe": dict(look)},
                    scene_snapshot={
                        "name": (
                            f"{travel_moment}｜{variation.get('scene_zh') or '旅行场景'}"
                            if travel_moment else
                            str(variation.get("scene_zh") or (
                                "参考图生活场景" if scene_model else "纯色穿搭背景"
                            ))
                        ),
                        "prompt_core": (
                            _with_background_anchor(
                                travel_scene_prompt
                                or str(variation.get("background_prompt") or (
                                    "自然生活环境，空间和光线忠实延续参考图氛围"
                                    if scene_model else "均匀暖米色纯色背景"
                                )),
                                features=style_profile.get("background_features") or [],
                                enabled=bool(travel_moment),
                            )
                        ),
                    },
                    output_dir=str(output_dir), continuity_reference_images=references,
                    outfit_state=outfit_state,
                    recipe_execution={
                        "content_goal": (
                            "layering_progression" if layered_planned else "multi_look"
                        ),
                        "reference_mode": "STYLE",
                        "transform_mode": (
                            "layering_reference_reduction"
                            if layered_planned else "style_reference_variation"
                        ),
                        "theme_brief": {**dict(theme), "variation": variation},
                        "presentation_profile": {
                            "presentation_type": presentation_type,
                            "background_mode": (
                                "reference_surface" if flat_lay else
                                "creator_environment" if scene_model else "solid_color"
                            ),
                            "background_color": str(variation.get("background_color") or "#E9DFD0"),
                            "full_body_occupancy": "78-88%",
                            "reference_style_profile": style_profile,
                        },
                        # 发布语言随任务走（review 修复 P0-2）：此前写死 th-TH，
                        # 会让 VN 任务的生成请求带着泰语语言标记。默认值保持
                        # th-TH，故既有 TH V2 调用方不传时输出逐字不变。
                        "locale": str(locale or "th-TH"),
                        **(
                            {"human_presentation_contract": human_contract}
                            if human_contract else {}
                        ),
                        **(
                            {"color_grading_plan": color_grading_plan}
                            if color_grading_plan else {}
                        ),
                    },
                    reference_roles={
                        "style_reference_images": list(selected_style_paths),
                        "environment_reference_images": [
                            paths[int(value) - 1]
                            for value in (style_profile.get("environment_reference") or {}).get("indices") or []
                            if str(value).isdigit() and 0 < int(value) <= len(paths)
                        ],
                        "visual_style_reference_images": [
                            paths[int(value) - 1]
                            for value in (style_profile.get("visual_style_reference") or {}).get("indices") or []
                            if str(value).isdigit() and 0 < int(value) <= len(paths)
                        ],
                        "outfit_reference_images": [
                            paths[value - 1] for value in sorted(selected_outfit)
                            if 0 < value <= len(paths)
                        ],
                        "identity_anchor": identity_anchor,
                        **({"layer_progression_anchor": identity_anchor}
                           if layered_planned and identity_anchor else {}),
                        **({"product_identity_images": list(product.get("reference_images") or [])}
                           if product else {}),
                        **(
                            {"persona_identity_images": list(identity_paths)}
                            if identity_paths else {}
                        ),
                    },
                )

            generation_order = [
                str(value) for value in variation.get("generation_order") or []
            ]
            by_generation_role = {str(item.get("role") or ""): item for item in looks}
            generation_looks = (
                [by_generation_role[role] for role in generation_order]
                if (layered_planned and generation_order
                    and set(generation_order) == set(by_generation_role))
                else list(looks)
            )
            # 并行预取：无生成图锚点的路径（人物场景/平铺）B/C/D 与 Look A
            # 门禁并行生成，整组墙钟时间约减半。门禁彻底失败时预取结果
            # 直接丢弃（不入 manifest，不进链路，仅损失生图费）。
            parallelizable = ((flat_lay or human_scene) and len(looks) > 2
                              and not layered_planned)
            prefetch: dict[str, Any] = {}
            executor = None
            if parallelizable:
                from concurrent.futures import ThreadPoolExecutor
                executor = ThreadPoolExecutor(max_workers=2)
                for index, look in enumerate(generation_looks, 1):
                    role = str(look["role"])
                    if index == 1 or role in completed:
                        continue
                    prefetch[role] = executor.submit(
                        self.generator.generate_shot, build_request(index, look)
                    )

            def generate_with_prefetch(index: int, look: Mapping[str, Any]) -> Any:
                role = str(look["role"])
                future = prefetch.pop(role, None)
                if future is not None:
                    return future.result()
                return self.generator.generate_shot(build_request(index, look))

            for index, look in enumerate(generation_looks, 1):
                role = str(look["role"])
                planned_look_signature = self._look_signature(look)
                if role in completed:
                    continue
                request = build_request(index, look)
                outcome = generate_with_prefetch(index, look)
                if not outcome.ok or not outcome.image_path:
                    raise PhotoStyleReferenceError(
                        f"{role} 风格参考生图失败：{outcome.error or '图片模型没有返回文件'}"
                    )
                output = Path(outcome.image_path).resolve()
                alignment = None
                if index == 1 and style_profile:
                    if style_profile.get("analysis_method") == "doubao_seed_2_1":
                        reviewer = self._vision_reviewer()
                        alignment = reviewer.review_alignment(
                            reference_paths=qa_reference_paths, generated_paths=[str(output)],
                            contract=style_profile, scope="FIRST_LOOK_A",
                        )
                        if not alignment["passed"]:
                            retry_shot = dict(request.plan_shot)
                            retry_shot["purpose"] = _with_repair_note(
                                retry_shot.get("purpose"),
                                "首次视觉质检未通过：" + str(alignment.get("notes") or ""),
                            )
                            retry = replace(request, shot_version=request.shot_version + 1,
                                            plan_shot=retry_shot)
                            outcome = self.generator.generate_shot(retry)
                            if not outcome.ok or not outcome.image_path:
                                raise PhotoStyleReferenceError(
                                    f"{role} 风格参考生图重试失败：{outcome.error or '图片模型没有返回文件'}"
                                )
                            output = Path(outcome.image_path).resolve()
                            alignment = reviewer.review_alignment(
                                reference_paths=qa_reference_paths, generated_paths=[str(output)],
                                contract=style_profile, scope="FIRST_LOOK_A_RETRY",
                            )
                            if not alignment["passed"]:
                                raise PhotoStyleReferenceError(
                                    "首张参考一致性检查两次未通过：" + str(alignment.get("notes") or "")
                                )
                    else:
                        from services.photo_style_reference_analyzer import validate_style_alignment
                        try:
                            validate_style_alignment(style_profile, str(output))
                        except ValueError as exc:
                            raise PhotoStyleReferenceError(f"首张参考一致性检查失败：{exc}") from exc
                human_gate = None
                if index == 1 and human_scene:
                    reviewer = self._vision_reviewer()
                    if hasattr(reviewer, "review_human_presentation"):
                        gate_contract = {role: dict((request.plan_shot.get("composition_contract") or {}).get("pose_contract") or {})}
                        human_gate = self._review_human(
                            reviewer, image_paths=[str(output)], role_order=[role],
                            persona_reference_paths=identity_paths,
                            pose_contracts=gate_contract, group_rules=False,
                        )
                        if not human_gate["passed"]:
                            instruction = "；".join(
                                item.get("repair_instruction") or ""
                                for item in human_gate["roles"]
                            ).strip("；")
                            retry_shot = dict(request.plan_shot)
                            retry_shot["purpose"] = _with_repair_note(
                                retry_shot.get("purpose"),
                                "人物表现质检未通过：" + (instruction or "人物真实感不足"),
                            )
                            retry = replace(request, shot_version=request.shot_version + 1,
                                            plan_shot=retry_shot)
                            outcome = self.generator.generate_shot(retry)
                            if not outcome.ok or not outcome.image_path:
                                raise PhotoStyleReferenceError(
                                    f"{role} 人物表现修复重生失败：{outcome.error or '图片模型没有返回文件'}"
                                )
                            output = Path(outcome.image_path).resolve()
                            regenerated_roles.add(role)
                            human_gate = self._review_human(
                                reviewer, image_paths=[str(output)], role_order=[role],
                                persona_reference_paths=identity_paths,
                                pose_contracts=gate_contract, group_rules=False,
                            )
                            if not human_gate["passed"]:
                                raise PhotoStyleReferenceError(
                                    "首张人物表现检查两次未通过，停止生成后续角色防止坏锚点污染整组："
                                    + "；".join(
                                        "、".join(filter(None, [
                                            item["role"], *item["issues"],
                                            item["repair_instruction"],
                                        ]))
                                        for item in human_gate["roles"]
                                    )
                                )
                digest = hashlib.sha256(output.read_bytes()).hexdigest()
                completed[role] = {
                    "role": role, "path": str(output), "sha256": digest,
                    "source_kind": "style_reference_generated",
                    "display_label": {"th-TH": look["display_label"], "zh-CN": look["display_label"]},
                    "outerwear_signature": str(look["outerwear"]),
                    "planned_look_signature": planned_look_signature,
                    "content_plan_item_id": str(variation.get("family_id") or ""),
                    "planned_attributes": {
                        key: str(look.get(key) or "")
                        for key in ("outerwear", "top_inner", "bottom", "shoes")
                    },
                    "generation_provider": outcome.provider, "generation_model": outcome.model,
                    "generation_request_id": outcome.request_id,
                    "first_look_alignment": alignment,
                    **({
                        "persona_pack_id": persona_pack_id,
                        **({
                            "pose_contract": dict(
                                (request.plan_shot.get("composition_contract") or {}).get("pose_contract") or {}
                            )
                        } if human_scene else {}),
                        **({"human_gate_qa": human_gate} if human_gate is not None else {}),
                    } if persona_bound else {}),
                }
                if layered_planned:
                    identity_anchor = str(output)
                elif not flat_lay and not persona_bound:
                    identity_anchor = identity_anchor or str(output)
                generated_this_run += 1
                self._save(manifest_path, input_hash, record_id, theme, paths, completed,
                           "incomplete", group_repair_attempts=group_repair_attempts,
                           attempt_history=attempt_history, persona_pack_id=persona_pack_id)
                _emit_progress(progress, "asset_generated", role=role,
                               done=len(completed), total=len(looks))
            if executor is not None:
                executor.shutdown(wait=True)
                executor = None
            ordered = [completed[role] for role in role_order]
            group_alignment = None
            group_human_qa = None
            group_consistency_qa = None
            reviewer = self._vision_reviewer()
            failed_roles: list[str] = []
            has_attribution = False
            consistency_failed = False
            # 程序化跨图一致性：免费、确定性，失败直接短路省掉视觉调用。
            if human_scene and color_grading_plan:
                from services.photo_color_consistency import evaluate_group_consistency
                _emit_progress(progress, "consistency_started")
                program_consistency = evaluate_group_consistency(
                    [str(item["path"]) for item in ordered]
                )
                group_consistency_qa = {"program": program_consistency}
                if not program_consistency["passed"]:
                    # 旅行/生活场景天然存在白天、室内和夜景差异。默认 standard
                    # 仅记录整图色彩统计，避免用背景色差触发付费重生；strict
                    # 仍保留旧门禁供专项调试。
                    consistency_failed = (
                        os.environ.get("OPV_PHOTO_QA_LEVEL", "standard").strip().lower()
                        == "strict"
                    )
                    index_roles = list(role_order)
                    failed_roles = [
                        index_roles[index]
                        for index in program_consistency["failed_roles"]
                        if index < len(index_roles)
                    ]
                    if consistency_failed:
                        for role in failed_roles:
                            repair_notes[role] = (
                                "本张肤色/色调偏离全组基准（"
                                + "；".join(program_consistency["issues"][:2])
                                + "）；严格对齐人物参考图的肤色与全组统一色调重新生成。"
                            )
                        _emit_progress(progress, "consistency_failed",
                                       roles=list(failed_roles))
                    else:
                        failed_roles = []
                        _emit_progress(
                            progress, "consistency_hint",
                            notes="；".join(program_consistency["issues"][:2]),
                        )
                if consistency_failed and group_repair_attempts >= MAX_GROUP_REPAIR_ATTEMPTS:
                    self._save(manifest_path, input_hash, record_id, theme, paths, completed,
                              "group_failed", group_alignment=None,
                              group_human_presentation_qa=None,
                              group_consistency_qa=group_consistency_qa,
                              group_repair_attempts=group_repair_attempts,
                              attempt_history=attempt_history, persona_pack_id=persona_pack_id)
                    raise PhotoStyleReferenceError(
                        "跨图一致性检查未通过且重做次数已用尽（"
                        f"{group_repair_attempts}/{MAX_GROUP_REPAIR_ATTEMPTS}）："
                        + "；".join(group_consistency_qa["program"]["issues"])
                        + f"；QA 证据保留于 {manifest_path}，请新建飞书任务"
                    )
                if consistency_failed:
                    repair_round = {
                        "round": group_repair_attempts + 1,
                        "failed_roles": list(failed_roles),
                        "alignment": None,
                        "consistency_alignment": group_consistency_qa,
                        "repair_notes": dict(repair_notes),
                        "retired": [],
                        "invalidated_at": datetime.now(timezone.utc).isoformat(),
                    }
                    for role in failed_roles:
                        item = completed.pop(role, None)
                        if item:
                            # 完整原始条目入 retired：恢复时保留全部生成元数据。
                            repair_round["retired"].append(dict(item))
                    attempt_history.append(repair_round)
                    group_repair_attempts += 1
                    repaired_roles_this_run.extend(failed_roles)
                    self._save(manifest_path, input_hash, record_id, theme, paths, completed,
                              "group_repair_pending", group_alignment=None,
                              group_human_presentation_qa=None,
                              group_consistency_qa=group_consistency_qa,
                              group_repair_attempts=group_repair_attempts,
                              attempt_history=attempt_history, persona_pack_id=persona_pack_id)
                    _emit_progress(progress, "repair_scheduled", roles=list(failed_roles),
                                   reason="color_consistency",
                                   notes="；".join(group_consistency_qa["program"]["issues"][:2]))
                    continue
            if (layered_planned or style_profile.get("analysis_method") == "doubao_seed_2_1") and not consistency_failed:
                _emit_progress(progress, "qa_started")
                if thermal_transition_planned:
                    if not hasattr(reviewer, "review_thermal_transition_pages"):
                        raise PhotoStyleReferenceError(
                            "冷热切换供图缺少 review_thermal_transition_pages，禁止无语义质检进入资产集"
                        )
                    semantic = reviewer.review_thermal_transition_pages(
                        reference_paths=qa_reference_paths,
                        look_plans=looks,
                        image_paths=[str(item["path"]) for item in ordered],
                        thermal_transition_contract=(
                            style_profile.get("thermal_transition_contract") or {}
                        ),
                        profile_binding=variation.get("profile_binding") or {},
                    )
                    from services.photo_thermal_transition_qa import (
                        failed_roles_from_thermal_qa,
                        thermal_qa_as_alignment,
                    )
                    group_alignment = thermal_qa_as_alignment(semantic)
                    repair_notes = {
                        str(item.get("role")): str(item.get("repair_instruction") or "")
                        for item in semantic.get("roles") or []
                    }
                    if not semantic["passed"]:
                        failed_roles = failed_roles_from_thermal_qa(
                            semantic, role_order
                        )
                elif layered_planned:
                    if not hasattr(reviewer, "review_layering_pages"):
                        raise PhotoStyleReferenceError(
                            "温度分层供图缺少 review_layering_pages，禁止无语义质检进入资产集"
                        )
                    semantic = reviewer.review_layering_pages(
                        reference_paths=qa_reference_paths,
                        look_plans=looks,
                        image_paths=[str(item["path"]) for item in ordered],
                        layering_contract=style_profile.get("layering_contract") or {},
                        profile_binding=variation.get("profile_binding") or {},
                    )
                    from services.photo_layering_qa import (
                        failed_roles_from_layering_qa,
                        layering_qa_as_alignment,
                    )
                    group_alignment = layering_qa_as_alignment(semantic)
                    repair_notes = {
                        str(item.get("role")): str(item.get("repair_instruction") or "")
                        for item in semantic.get("roles") or []
                    }
                    if not semantic["passed"]:
                        failed_roles = failed_roles_from_layering_qa(
                            semantic, role_order
                        )
                elif travel_planned and hasattr(reviewer, "review_travel_pages"):
                    semantic = reviewer.review_travel_pages(
                        reference_paths=qa_reference_paths, look_plans=looks,
                        product_reference_paths=[
                            str(value) for value in product.get("reference_images") or []
                            if Path(str(value)).is_file()
                        ],
                        style_reference_paths=paths,
                        product_context={
                            key: product.get(key)
                            for key in ("product_id", "product_name", "category",
                                        "reference_pack_id", "reference_pack_version")
                            if product.get(key) not in (None, "")
                        },
                        travel_place=str(
                            (style_profile.get("travel_topic") or {}).get("place") or ""
                        ),
                        image_paths=[str(item["path"]) for item in ordered],
                        travel_contract=style_profile.get("travel_contract") or {},
                        persona_based=bool(persona),
                    )
                    from services.photo_travel_qa import (
                        failed_roles_from_travel_qa, travel_qa_as_alignment,
                    )
                    group_alignment = travel_qa_as_alignment(semantic)
                    repair_notes = {
                        str(item.get("role")): str(item.get("repair_instruction") or "")
                        for item in semantic.get("roles") or []
                    }
                    if not semantic["passed"]:
                        failed_roles = failed_roles_from_travel_qa(semantic, role_order)
                else:
                    group_alignment = reviewer.review_alignment(
                        reference_paths=qa_reference_paths,
                        generated_paths=[str(item["path"]) for item in ordered],
                        contract=style_profile, scope="FULL_LOOK_GROUP",
                        generated_roles=role_order,
                        persona_based=bool(persona),
                    )
                    if not group_alignment["passed"]:
                        failed_roles, has_attribution = self._failed_roles(
                            group_alignment, role_order)
                if human_scene and hasattr(reviewer, "review_human_presentation"):
                    _emit_progress(progress, "human_qa_started")
                    group_human_qa = self._review_human(
                        reviewer,
                        image_paths=[str(item["path"]) for item in ordered],
                        role_order=list(role_order),
                        persona_reference_paths=identity_paths,
                        pose_contracts={
                            role: dict(completed[role].get("pose_contract") or {})
                            for role in role_order
                        },
                        group_rules=not layered_planned,
                    )
                    if not group_human_qa["passed"]:
                        for item in group_human_qa["roles"]:
                            if not item["passed"] and item["repair_instruction"]:
                                repair_notes[str(item["role"])] = item["repair_instruction"]
                        role_failures = {
                            str(item["role"]) for item in group_human_qa["roles"]
                            if not item["passed"]
                        }
                        # 组级重复度失败无法归因到单张时整组重做。
                        human_failed = [
                            role for role in role_order
                            if role in role_failures or group_human_qa["group_issues"]
                        ]
                        failed_roles = list(dict.fromkeys(failed_roles + human_failed))
                # 肤色视觉 QA 默认关闭（省一次模型调用；肤色差异已降级为
                # 提示证据）。OPV_PHOTO_SKIN_QA=on 可重新启用。
                skin_qa_enabled = os.environ.get(
                    "OPV_PHOTO_SKIN_QA", "off").strip().lower() == "on"
                if (skin_qa_enabled and human_scene and color_grading_plan
                        and hasattr(reviewer, "review_group_consistency")):
                    from services.photo_color_consistency import (
                        evaluate_visual_consistency,
                    )
                    _emit_progress(progress, "consistency_visual_started")
                    role_paths = {
                        role: str(completed[role]["path"]) for role in role_order
                    }
                    raw_consistency = reviewer.review_group_consistency(
                        image_paths=[role_paths[role] for role in role_order],
                        role_order=list(role_order),
                        persona_reference_paths=identity_paths,
                        color_grading_plan=color_grading_plan,
                    )
                    visual_verdict = evaluate_visual_consistency(
                        raw_consistency, role_order, role_paths=role_paths,
                    )
                    group_consistency_qa = {
                        **(group_consistency_qa or {}),
                        "visual": visual_verdict,
                    }
                    # 肤色/调色的跨图微差是生成端已知方差，重生无法收敛。
                    # 降级为质量提示，不再触发付费重生；需要专项调试时可用
                    # OPV_PHOTO_QA_LEVEL=strict 恢复程序化色彩门禁。
                    if not visual_verdict["passed"]:
                        drift_notes = [
                            f"{role}：{(visual_verdict['roles'].get(role) or {}).get('drift_note_zh')}"
                            for role in visual_verdict["failed_roles"]
                        ]
                        human_review_hint = (
                            "肤色/色调跨图存在轻微不一致（"
                            + "；".join(drift_notes[:2])
                            + "），已保留证据请人工复核；如需统一可在发布前做整体调色。"
                        )
                        _emit_progress(progress, "consistency_visual_hint",
                                       notes=human_review_hint[:120])
                style_failed = group_alignment is None or not group_alignment["passed"]
                human_failed_flag = group_human_qa is not None and not group_human_qa["passed"]
                # 归因语义：模型给了 per_look → 只重生归因到的角色；
                # 完全无归因 → 保守整组重做（failed_roles 填全角色）。
                if style_failed and not has_attribution and not failed_roles:
                    failed_roles = list(role_order)
                style_needs_redo = style_failed or human_failed_flag
                if style_needs_redo or human_failed_flag:
                    if group_repair_attempts >= MAX_GROUP_REPAIR_ATTEMPTS:
                        self._save(manifest_path, input_hash, record_id, theme, paths, completed,
                                  "group_failed", group_alignment=group_alignment,
                                  group_human_presentation_qa=group_human_qa,
                                  group_consistency_qa=group_consistency_qa,
                                  group_repair_attempts=group_repair_attempts,
                                  attempt_history=attempt_history, persona_pack_id=persona_pack_id)
                        raise PhotoStyleReferenceError(
                            "整组质检未通过且重做次数已用尽（"
                            f"{group_repair_attempts}/{MAX_GROUP_REPAIR_ATTEMPTS}）："
                            + str((group_alignment or {}).get("notes") or "")
                            + ("；人物表现：" + "；".join(group_human_qa["group_issues"])
                               if human_failed_flag else "")
                            + ("；跨图一致性：" + "；".join(group_consistency_qa["program"]["issues"])
                               if consistency_failed else "")
                            + f"；QA 证据保留于 {manifest_path}，请新建飞书任务"
                        )
                    # 每张最多重生一次：已重生过的角色不再进入修复轮；
                    # 剔除后仍有失败角色时任务直接失败（费用上限保护）。
                    overrun = [r for r in failed_roles if r in regenerated_roles]
                    if overrun and len(overrun) == len(failed_roles):
                        self._save(manifest_path, input_hash, record_id, theme, paths, completed,
                                  "group_failed", group_alignment=group_alignment,
                                  group_human_presentation_qa=group_human_qa,
                                  group_consistency_qa=group_consistency_qa,
                                  group_repair_attempts=group_repair_attempts,
                                  attempt_history=attempt_history, persona_pack_id=persona_pack_id)
                        raise PhotoStyleReferenceError(
                            "以下图片已重生过一次仍未达标，按每张最多重生一次规则终止："
                            + "、".join(overrun)
                            + f"；QA 证据保留于 {manifest_path}，请新建飞书任务"
                        )
                    repair_round = {
                        "round": group_repair_attempts + 1,
                        "failed_roles": list(failed_roles),
                        "alignment": dict(group_alignment) if group_alignment else None,
                        **({"human_alignment": group_human_qa}
                           if group_human_qa is not None else {}),
                        **({"consistency_alignment": group_consistency_qa}
                           if group_consistency_qa is not None else {}),
                        "repair_notes": dict(repair_notes),
                        "retired": [],
                        "invalidated_at": datetime.now(timezone.utc).isoformat(),
                    }
                    for role in failed_roles:
                        item = completed.pop(role, None)
                        if item:
                            # 完整原始条目入 retired：恢复时保留全部生成元数据。
                            repair_round["retired"].append(dict(item))
                    attempt_history.append(repair_round)
                    group_repair_attempts += 1
                    repaired_roles_this_run.extend(failed_roles)
                    self._save(manifest_path, input_hash, record_id, theme, paths, completed,
                              "group_repair_pending", group_alignment=group_alignment,
                              group_human_presentation_qa=group_human_qa,
                              group_consistency_qa=group_consistency_qa,
                              group_repair_attempts=group_repair_attempts,
                              attempt_history=attempt_history, persona_pack_id=persona_pack_id)
                    _emit_progress(progress, "repair_scheduled", roles=list(failed_roles),
                                   reason="human_presentation" if human_failed_flag else "style_alignment",
                                   notes="；".join(group_human_qa["group_issues"][:2]) if human_failed_flag else "")
                    continue
            for role in role_order:
                finding = next(
                    (item for item in (group_human_qa or {}).get("roles") or []
                     if item.get("role") == role), None,
                ) if group_human_qa else None
                if finding and role in completed:
                    completed[role]["human_presentation_qa"] = dict(finding)
            quality = self._quality_summary(
                group_alignment, group_human_qa, group_consistency_qa, attempt_history,
            )
            self._save(manifest_path, input_hash, record_id, theme, paths, completed, "complete",
                       group_alignment=group_alignment,
                       group_human_presentation_qa=group_human_qa,
                       group_consistency_qa=group_consistency_qa,
                       group_repair_attempts=group_repair_attempts,
                       attempt_history=attempt_history, persona_pack_id=persona_pack_id,
                       quality=quality)
            return {
                "schema_version": "opv-photo-style-reference-supply-v1",
                "record_id": record_id, "reference_mode": "STYLE",
                "theme_brief": {**dict(theme), "variation": variation},
                "sources": ordered, "generated_count": len(ordered),
                "generated_this_run": generated_this_run,
                "supply_manifest": str(manifest_path),
                "group_alignment": group_alignment,
                "group_human_presentation_qa": group_human_qa,
                "group_consistency_qa": group_consistency_qa,
                "persona_pack_id": persona_pack_id,
                "group_repair_attempts": group_repair_attempts,
                "repair_rounds": len(attempt_history),
                "repaired_roles_this_run": repaired_roles_this_run,
                "quality": quality,
            }

    @staticmethod
    def _quality_summary(group_alignment, group_human_qa, group_consistency_qa,
                         attempt_history) -> dict:
        """Non-blocking quality record: warnings stay evidence, never gates."""
        warnings: list[dict[str, str]] = []
        for item in (group_human_qa or {}).get("quality_warnings") or []:
            warnings.append({
                "role": str(item.get("role") or ""),
                "code": str(item.get("code") or ""),
                "message": str(item.get("message") or ""),
            })
        for item in (group_alignment or {}).get("quality_warnings") or []:
            warnings.append({
                "role": str(item.get("role") or "group"),
                "code": str(item.get("code") or "TRAVEL_QA_NOTE"),
                "message": str(item.get("message") or ""),
            })
        program = (group_consistency_qa or {}).get("program") or {}
        for issue in program.get("issues") or []:
            warnings.append({
                "role": "group", "code": "COLOR_CONSISTENCY",
                "message": f"全图调色一致性：{issue}",
            })
        visual = (group_consistency_qa or {}).get("visual") or {}
        for role, finding in (visual.get("roles") or {}).items():
            if not finding.get("passed"):
                drift = finding.get("drift_note_zh") or ""
                warnings.append({
                    "role": str(role), "code": "SKIN_TONE_NOTE",
                    "message": f"肤色/调色轻微不一致：{drift}" if drift else "肤色/调色轻微不一致",
                })
        repair_count = sum(len(entry.get("retired") or []) for entry in attempt_history)
        style_passed = bool((group_alignment or {}).get("passed", True))
        human_passed = bool((group_human_qa or {}).get("passed", True))
        color_passed = (
            bool(program.get("passed", True))
            if os.environ.get("OPV_PHOTO_QA_LEVEL", "standard").strip().lower()
            == "strict" else True
        )
        passed = style_passed and human_passed and color_passed
        summary_parts = [f"{w['role']}：{w['message']}" for w in warnings[:3]]
        summary = ("质量提示（不影响发布）：" + "；".join(summary_parts)) if warnings else ""
        return {
            "quality_gate": "passed" if passed else "failed",
            "quality_warnings": warnings,
            "repair_count": int(repair_count),
            "publish_ready": passed,
            "quality_summary_zh": summary,
        }

    def regenerate_roles(self, *, item_dir: Path, roles: Sequence[str],
                         reason: str) -> dict:
        """手动指定重生：把指定角色从 completed 摘除（原始条目进 retired
        留痕），重勾执行时断点续跑只重生这些角色。不触发自动重试循环。"""
        item_dir = Path(item_dir)
        manifest_path = item_dir / "supply_manifest.json"
        if not manifest_path.is_file():
            raise PhotoStyleReferenceError(f"找不到供给清单：{manifest_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        wanted = {str(r).strip().lower() for r in roles}
        unknown = wanted - {str(item.get("role") or "") for item in manifest.get("sources") or []}
        if unknown:
            raise PhotoStyleReferenceError(f"素材里没有这些角色：{sorted(unknown)}")
        repair_round = {
            "round": int(manifest.get("group_repair_attempts") or 0) + 1,
            "trigger": "operator_manual",
            "reason": reason,
            "failed_roles": sorted(wanted),
            "alignment": None,
            "repair_notes": {role: f"运营手动要求重生：{reason}" for role in wanted},
            "retired": [],
            "invalidated_at": datetime.now(timezone.utc).isoformat(),
        }
        new_sources = []
        for item in manifest.get("sources") or []:
            role = str(item.get("role") or "")
            if role in wanted:
                repair_round["retired"].append({
                    "role": role, "path": str(item.get("path") or ""),
                    "sha256": str(item.get("sha256") or ""),
                    "planned_look_signature": str(item.get("planned_look_signature") or ""),
                })
            else:
                new_sources.append(item)
        manifest["sources"] = new_sources
        manifest.setdefault("attempt_history", []).append(repair_round)
        # 手动重生不消耗自动修复预算（trigger=operator_manual 区分）。
        manifest["status"] = "group_repair_pending"
        temporary = manifest_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(manifest_path)
        return {"roles": sorted(wanted), "retired": repair_round["retired"],
                "manifest": str(manifest_path)}

    def restore_retired_sources(self, item_dir: Path, *, reason: str,
                                recheck_result: str = None,
                                roles: Sequence[str] = None) -> dict:
        """误判恢复：把 retired 的原始素材记录找回并重新纳入工作清单。

        保留原始图片路径/哈希与全部生成元数据；恢复原因、原失败原因与
        重新检查结果写入 manifest 的 recovery_log（与最终通过结果同时保留，
        检查失败不会悄悄消失）。
        """
        import shutil as _shutil

        item_dir = Path(item_dir)
        manifest_path = item_dir / "supply_manifest.json"
        if not manifest_path.is_file():
            raise PhotoStyleReferenceError(f"找不到供给清单：{manifest_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        roles = {str(r) for r in (roles or [])}
        restored, skipped = [], []
        for entry in reversed(manifest.get("attempt_history") or []):
            for item in reversed(entry.get("retired") or []):
                role = str(item.get("role") or "")
                if roles and role not in roles:
                    continue
                if role in restored:
                    continue
                path = Path(str(item.get("path") or ""))
                expected = str(item.get("sha256") or "")
                if (not path.is_file() or (expected and hashlib.sha256(
                        path.read_bytes()).hexdigest() != expected)):
                    skipped.append({"role": role, "path": str(path),
                                    "reason": "文件缺失或哈希不匹配"})
                    continue
                restored.append({**item, "role": role})
        if not restored:
            raise PhotoStyleReferenceError(f"没有可恢复的素材记录：{skipped or 'retired 为空'}")
        current = {str(item.get("role") or "") for item in manifest.get("sources") or []}
        manifest["sources"] = [
            dict(item) for item in (manifest.get("sources") or [])
            if str(item.get("role") or "") not in {r["role"] for r in restored}
        ] + restored
        manifest["status"] = "incomplete"
        manifest.setdefault("recovery_log", []).append({
            "restored_at": datetime.now(timezone.utc).isoformat(),
            "reason": reason,
            "original_failure": [
                {"round": entry.get("round"),
                 "failed_roles": entry.get("failed_roles") or [],
                 "notes": str((entry.get("alignment") or {}).get("notes") or "")[:300]}
                for entry in manifest.get("attempt_history") or []
            ],
            "roles_restored": sorted(r["role"] for r in restored),
            "recheck_result": recheck_result,
            "skipped": skipped,
        })
        temporary = manifest_path.with_suffix(".recovery.tmp")
        temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                             encoding="utf-8")
        temporary.replace(manifest_path)
        return {"restored_roles": sorted(r["role"] for r in restored),
                "skipped": skipped, "manifest": str(manifest_path)}

    def _review_human(self, reviewer, *, image_paths, role_order,
                      persona_reference_paths, pose_contracts, group_rules):
        """Run the observation review and recompute the verdict programmatically."""
        from services.photo_human_qa import evaluate_human_presentation
        raw = reviewer.review_human_presentation(
            image_paths=image_paths, role_order=list(role_order),
            persona_reference_paths=list(persona_reference_paths or []),
            pose_contracts=dict(pose_contracts or {}),
        )
        return evaluate_human_presentation(
            raw, pose_contracts=dict(pose_contracts or {}),
            group_rules=group_rules, role_order=role_order,
        )

    @staticmethod
    def _failed_roles(alignment: Mapping[str, Any],
                      role_order: Sequence[str]) -> list[str]:
        findings = {
            str(item.get("role") or ""): item
            for item in alignment.get("role_findings") or []
            if isinstance(item, Mapping)
        }
        FAILURE_HINTS = ("场景", "退化", "平铺", "棚拍", "畸形", "多人",
                         "水印", "Logo", "肢体", "重叠", "错配", "缺失")
        failed = []
        detail_notes = []
        attributed = False
        for role in role_order:
            finding = findings.get(role)
            if not finding:
                continue
            attributed = True
            if finding.get("missing_major_garment"):
                failed.append(role)
                continue
            if finding.get("passed") is False:
                issues = [str(v) for v in finding.get("issues") or []]
                blocking = any(
                    hint in issue for issue in issues for hint in FAILURE_HINTS)
                if blocking:
                    failed.append(role)
                else:
                    # 单品级细节（内搭层数/配饰/鞋型/颜色微差）降为提示。
                    detail_notes.extend(f"{role}：{issue}" for issue in issues)
        if detail_notes:
            alignment = dict(alignment)
            alignment["detail_level_notes"] = detail_notes
        # 穿搭主体缺失是硬门禁：模型整体 passed 也按角色失败处理。
        missing = [
            role for role in role_order
            if role in findings and findings[role].get("missing_major_garment")
        ]
        failed = list(dict.fromkeys(failed + missing))
        # 无归因（模型没给 per_look）时整组重做，避免重复检查同一组图。
        return failed, attributed or bool(failed)

    def _vision_reviewer(self):
        if self.vision_service is None:
            from services.photo_reference_vision import PhotoReferenceVisionService
            self.vision_service = PhotoReferenceVisionService(root=self.root)
        return self.vision_service

    @classmethod
    def input_fingerprint(cls, paths, theme, variation, account, product=None) -> str:
        """重拍前置校验：当前输入能否复现原供给清单的 input_hash。"""
        looks = style_look_specs(theme, variation)
        resolved = [str(Path(value).expanduser().resolve()) for value in paths]
        return cls._input_hash(resolved, theme, looks, account, variation, product)

    def verify_and_rebaseline_identity(self, *, item_dir: Path, paths, theme, account,
                                       variation, persona=None, product=None) -> None:
        """重拍前逐组件身份校验，通过后重定 input_hash 基线。

        冻结内容计划可能在供给生成之后被补写（如旅行 cover_selection 兜底），
        组合 hash 因此无法直接复现；这里改为核对真正影响生成的身份组件：
        参考图文件哈希、主题字典、各角色穿搭规格签名、人物包 ID。全部一致
        才允许按当前输入重写 input_hash，供 prepare 断点续跑。
        """
        import re as _re
        item_dir = Path(item_dir)
        manifest_path = item_dir / "supply_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        resolved = [str(Path(value).expanduser().resolve()) for value in paths]
        current_refs = [hashlib.sha256(Path(value).read_bytes()).hexdigest()
                        for value in resolved]
        raw_record_id = str(manifest.get("record_id") or "")
        analysis_id = _re.sub(r"_item_\d+$", "", raw_record_id)
        analysis_path = self.root / "reference_contracts" / analysis_id / "reference_analysis.json"
        if analysis_path.is_file():
            analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
            recorded = [str(value) for value in analysis.get("reference_hashes") or []]
            if recorded and recorded != current_refs:
                raise PhotoStyleReferenceError(
                    "参考图与原生成不一致（文件哈希不匹配）；不能按新参考重拍旧素材")
        stored_theme = dict(manifest.get("theme_brief") or {})
        if stored_theme and dict(theme) != stored_theme:
            original_label = str(stored_theme.get("label_zh") or "原主题")
            raise PhotoStyleReferenceError(
                f"图文主题已从「{original_label}」改为「{theme.get('label_zh') or '当前所选'}」，"
                "不能按新主题重拍旧素材；请把该行图文主题改回原值后重拍，"
                "或新建一行按新主题整组重做")
        looks = style_look_specs(theme, variation)
        by_role = {str(item.get("role") or ""): item for item in manifest.get("sources") or []}
        for look in looks:
            source = by_role.get(str(look.get("role") or ""))
            if not source:
                continue
            recorded_signature = str(source.get("planned_look_signature") or "")
            if recorded_signature and recorded_signature != self._look_signature(look):
                raise PhotoStyleReferenceError(
                    f"{look.get('role')} 穿搭规格与原生成不一致；请勿修改内容计划后重拍")
        stored_pack_id = str(manifest.get("persona_pack_id") or "")
        if stored_pack_id and persona:
            from services.persona_pack import build_persona_pack
            if str(build_persona_pack(persona).get("persona_pack_id") or "") != stored_pack_id:
                raise PhotoStyleReferenceError("账号绑定的人物模板与原生成不一致；不能换人重拍")
        manifest["input_hash"] = self._input_hash(
            resolved, theme, looks, account, variation, product)
        temporary = manifest_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                             encoding="utf-8")
        temporary.replace(manifest_path)

    @staticmethod
    def _input_hash(paths, theme, looks, account, variation, product=None) -> str:
        payload = {
            "reference_hashes": [hashlib.sha256(Path(path).read_bytes()).hexdigest() for path in paths],
            "theme": dict(theme), "looks": looks, "variation": dict(variation or {}),
            "persona_ref_id": str(getattr(account, "persona_ref_id", "") or ""),
            "product_identity": {
                key: dict(product or {}).get(key)
                for key in ("product_id", "reference_pack_id", "reference_pack_version",
                            "asset_fingerprint")
            },
        }
        return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()).hexdigest()

    @staticmethod
    def _look_signature(look: Mapping[str, Any]) -> str:
        payload = {
            key: str(look.get(key) or "")
            for key in ("role", "outerwear", "top_inner", "bottom", "shoes")
        }
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _save(path, input_hash, record_id, theme, references, completed, status,
              group_alignment=None, group_human_presentation_qa=None,
              group_consistency_qa=None, group_repair_attempts=0,
              attempt_history=None, persona_pack_id="", quality=None):
        payload = {
            "schema_version": "opv-photo-style-reference-supply-v1",
            "input_hash": input_hash, "record_id": record_id, "status": status,
            "theme_brief": dict(theme), "style_reference_paths": list(references),
            "sources": list(completed.values()),
            "group_alignment": group_alignment,
            "group_human_presentation_qa": group_human_presentation_qa,
            "group_consistency_qa": group_consistency_qa,
            "persona_pack_id": str(persona_pack_id or ""),
            "group_repair_attempts": int(group_repair_attempts or 0),
            "attempt_history": list(attempt_history or []),
            "quality": quality,
        }
        temporary = Path(path).with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(path)

    @staticmethod
    def _safe(value: str) -> str:
        return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(value))[:120]
