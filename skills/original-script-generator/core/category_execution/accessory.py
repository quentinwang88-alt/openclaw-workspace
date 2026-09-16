"""Accessory execution adapter for hair clips, earrings and scarves.

The adapter deliberately reuses the existing type registry and AI shot-risk
registry.  It does not select claims, structures, scenes, hooks or dialogue.
"""

from __future__ import annotations

import copy
import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from core.product_type_resolution import normalize_product_type

from .base import CategoryExecutionAdapter

# The pairing vocabulary is single-sourced with the mixed-display template so the
# script validator and the blueprint guidance can never disagree about which
# words an unauthorised script may not contain.
from core.accessory_mixed_templates import (  # noqa: E402
    PAIRING_OUTPUT_TERMS as _PAIR_OUTPUT_TERMS,
    SINGLE_OUTPUT_TERMS as _SINGLE_OUTPUT_TERMS,
)


_RISK_REGISTRY_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "ai_shot_risk_registry.json"
)

_HAIR_TYPES = {
    "hair_accessory_generic",
    "claw_clip",
    "hair_clip",
    "headband",
    "scrunchie",
    "hair_tie",
    "ribbon",
    "hair_pin",
}
_WRIST_TYPES = {"bracelet", "bangle", "slim_bangle"}
_FINGER_TYPES = {"ring"}
_SCARF_TYPES = {"scarf", "winter_scarf", "silk_scarf", "headscarf"}
_SUPPORTED_TYPES = {
    "earring", *_SCARF_TYPES, *_HAIR_TYPES, *_WRIST_TYPES, *_FINGER_TYPES,
}
_SMALL_PROMINENCE_TYPES = {
    "earring", *_HAIR_TYPES, *_WRIST_TYPES, *_FINGER_TYPES,
}
_SMALL_MOTION_TYPES = {"earring", *_HAIR_TYPES}

SMALL_ACCESSORY_MOTION_ENV = "ORIGINAL_SCRIPT_SMALL_ACCESSORY_MOTION_V2_ENABLED"
SCARF_MULTICLIP_ENV = "ORIGINAL_SCRIPT_SCARF_MULTICLIP_V1_ENABLED"
SMALL_ACCESSORY_MULTICLIP_ENV = (
    "ORIGINAL_SCRIPT_SMALL_ACCESSORY_MULTICLIP_V1_ENABLED"
)

SCARF_MULTICLIP_PROFILE = "SCARF_MULTICLIP_V1"
SMALL_ACCESSORY_MULTICLIP_PROFILE = "SMALL_ACCESSORY_MULTICLIP_V1"

_PAIR_TERMS = ("一对", "成对", "一副", "双耳", "两只")
_SINGLE_TERMS = ("单只", "单个", "单耳", "单边")

# ---------------------------------------------------------------------------
# NO_FACE framing vocabulary
# ---------------------------------------------------------------------------
# The accessory profiles were authored for a wearer-on-camera video, so their
# small-product framing language names the face ("半脸耳侧近景").  That wording
# is correct for PERSON_ON_CAMERA and for every other wearer profile, and must
# stay byte-identical for them.
#
# When a *frozen mixed-display contract* declares ``face_policy = NO_FACE`` the
# same framing has to be expressed without any face reference.  Otherwise the
# final video prompt still asks the model for a half-face shot while the
# storyboard, the per-shot ``face_policy`` and the contract all forbid it --
# the template says NO_FACE but the prompt says 半脸.
#
# Selection is driven exclusively by the frozen contract travelling inside the
# category extension, so an extension without one keeps the historical strings
# verbatim, and already-frozen plan packages can be repaired by re-running only
# the script stage.
NO_FACE_POLICY = "NO_FACE"

_NO_FACE_EAR_CLOSE = "耳侧与耳垂近景"
_NO_FACE_EAR_RELATION = "耳侧与颈侧近景"

# Terms that must never survive into a prompt that forbids faces.
#
# "头肩" (head-and-shoulders) belongs here even though it does not literally
# say "face": the mixed template's EAR zone lists
# ``forbidden_framing = [眼睛入画, 鼻子入画, 嘴部入画, 正面全脸, 镜面反射露脸]``
# and its ``allowed_framing`` has no head-and-shoulders option at all.  A
# head-and-shoulders shot puts eyes, nose and mouth in frame, so offering one as
# the fallback "context" framing contradicts the contract that is supposed to
# guarantee the face-free result.
_FACE_TERMS = ("半脸", "侧脸", "正脸", "全脸", "镜面头肩", "头肩", "自拍")

# Ordered longest-first: each entry has to be applied before any of its own
# prefixes, otherwise "半脸耳侧近景" would be eaten by the bare "半脸" rule.
_NO_FACE_TEXT_REPLACEMENTS = (
    ("镜面头肩结果与半脸耳侧近景", "同机位重拍结果与" + _NO_FACE_EAR_RELATION),
    ("镜面头肩结果", "同机位重拍结果"),
    ("镜面头肩", "同机位重拍"),
    # Non-mirror head-and-shoulders wording.  These run after the mirror rules
    # above have already consumed "镜面头肩".
    #
    # "头肩" has to go because a head-and-shoulders shot puts eyes, nose and
    # mouth in frame, which the EAR zone forbids.  But it must not be removed
    # blindly: the HAIR zone's authorised framing is "后脑与侧后方", so
    # "头肩侧后方近景" is a *back* view that never shows the face and has to keep
    # its direction.  Most specific patterns therefore come first, and the bare
    # "头肩" rule stays last as the catch-all.
    ("回到头肩或上半身交代人物与穿搭", "回到颈部与肩线关系景交代人物与穿搭"),
    ("头肩侧后方", "侧后方"),
    ("头肩或上半身", "上半身"),
    ("头肩关系内", "颈肩关系内"),
    ("头肩构图内", "颈肩构图内"),
    ("头肩关系", "颈肩关系"),
    ("头肩", "颈肩"),
    # "自拍" is in ``_FACE_TERMS`` but used to have no rule here, which made the
    # scrubber a silent no-op for it: ``_has_face_term`` reported True while
    # ``_no_face_safe_text`` handed the text back untouched.
    #
    # Where it comes from: the frozen capture mode is
    # ``CREATOR_SELF_SHOT`` -- *one person, one phone, several takes cut
    # together*.  That names who owns the camera, not where the camera points.
    # A model reading the token name paraphrases it back into "自拍" and writes
    # it into the storyboard ("人物在同一自拍范围内轻微调整位置"), which quietly
    # re-imports the face the NO_FACE contract exists to exclude.
    #
    # Most specific first, then the bare term as the catch-all -- same ordering
    # discipline as the head-and-shoulders block above.  The replacements keep
    # the *recording-organisation* meaning and drop only the framing claim.
    ("自然自拍范围", "自然同机位取景范围"),
    ("同一自拍范围", "同一取景范围"),
    ("自拍范围内", "同机位取景范围内"),
    ("普通自拍或固定近距离记录", "固定近距离记录"),
    ("自拍", "自持手机固定录制"),
    ("半脸与耳侧近景", _NO_FACE_EAR_CLOSE),
    ("半脸耳侧近景", _NO_FACE_EAR_RELATION),
    ("半脸耳侧", "耳侧"),
    ("耳侧和半脸", "耳侧"),
    ("耳侧与半脸", "耳侧"),
    ("半脸", ""),
    ("侧脸", "耳侧"),
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def mixed_contract_face_policy(extension: Any) -> str:
    """Face policy declared by the frozen mixed-display contract, if present."""

    source = extension if isinstance(extension, Mapping) else {}
    contract = source.get("mixed_template_contract")
    if not isinstance(contract, Mapping) or not contract:
        return ""
    return _text(contract.get("face_policy")).upper()


def _is_no_face(face_policy: Any) -> bool:
    return _text(face_policy).upper() == NO_FACE_POLICY


def _face_pick(no_face: bool, face_text: str, neutral_text: str) -> str:
    """Return the neutral wording only when NO_FACE is genuinely in force."""

    return neutral_text if no_face else face_text


def _has_face_term(value: Any) -> bool:
    text = _text(value)
    return any(term in text for term in _FACE_TERMS)


def _no_face_safe_text(value: Any) -> str:
    """Rewrite authored framing prose so it never names the face."""

    text = _text(value)
    for source, target in _NO_FACE_TEXT_REPLACEMENTS:
        if source in text:
            text = text.replace(source, target)
    return text


def _no_face_deep_clean(value: Any) -> Any:
    """Remove every face reference from a finished adapter projection.

    One guarantee at the adapter boundary instead of a per-field rewrite: once
    the frozen contract declares ``NO_FACE``, nothing this adapter emits may
    describe a face.  Scattering individual rewrites is how ``头肩关系`` stayed
    in ``EAR_RELATION_TO_CLOSE_RETURN`` long after the guidance prose had been
    fixed -- the guidance fields were patched one by one and the interaction
    keywords were simply missed.

    Strings are rewritten; string entries of a list are additionally dropped
    when they still name the face, which mirrors the rule already applied to
    ``optional_simple_interactions``.  Non-string values are preserved, so this
    can never remove a capability or a contract object.
    """

    if isinstance(value, str):
        return _no_face_safe_text(value)
    if isinstance(value, Mapping):
        return {key: _no_face_deep_clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        cleaned: List[Any] = []
        for item in value:
            if isinstance(item, str):
                text = _no_face_safe_text(item)
                if text and not _has_face_term(text):
                    cleaned.append(text)
                continue
            cleaned.append(_no_face_deep_clean(item))
        return cleaned
    return value


def _dedupe(values: Iterable[Any], limit: int = 8) -> List[str]:
    result: List[str] = []
    for value in values:
        text = _text(value)
        if text and text not in result:
            result.append(text)
        if len(result) >= limit:
            break
    return result


def _mixed_template_blueprint_guidance(extension: Mapping[str, Any]) -> str:
    """Per-shot guidance for the authored mixed-display template, if present.

    Returns an empty string when the extension carries no template, which keeps
    every legacy accessory path on its existing guidance.
    """

    if not isinstance(extension, Mapping):
        return ""
    contract = extension.get("mixed_template_contract")
    if not isinstance(contract, Mapping) or not contract:
        return ""
    try:
        from core.accessory_mixed_templates import (
            PART_EVIDENCE_KEY,
            render_mixed_blueprint_guidance,
        )
    except Exception:  # noqa: BLE001 - never break an existing path
        return ""
    profile = (
        extension.get("profile") if isinstance(extension.get("profile"), Mapping) else {}
    )
    identity_authority = (
        profile.get("identity_authority")
        if isinstance(profile.get("identity_authority"), Mapping)
        else {}
    )
    # Part-gated action candidates are only issued when the frozen evidence
    # confirms the part.  Without evidence nothing is issued, which is the
    # conservative default: an unconfirmed part must not be displayed.
    evidence = extension.get(PART_EVIDENCE_KEY)
    lines = render_mixed_blueprint_guidance(
        contract,
        identity_authority=identity_authority,
        evidence=evidence if isinstance(evidence, Mapping) else None,
    )
    return "\n".join(line for line in lines if _text(line))


def _small_accessory_motion_enabled() -> bool:
    value = _text(os.environ.get(SMALL_ACCESSORY_MOTION_ENV, "1")).lower()
    return value not in {"0", "false", "no", "off"}


def _category_multiclip_enabled(env_name: str) -> bool:
    """Return the isolated category rollout switch.

    The rollout defaults on only for an already-matched accessory extension.
    Women's apparel never creates this extension and therefore cannot enter
    either branch.  Each family still has its own one-variable rollback.
    """

    value = _text(os.environ.get(env_name, "1")).lower()
    return value not in {"0", "false", "no", "off"}


def _category_capture_roles(
    *, product_subtype: str, unit_count: int, action_mode: str
) -> List[str]:
    if product_subtype in _SCARF_TYPES:
        middle = (
            "SIMPLE_WEAR_PROCESS_OR_DETAIL"
            if action_mode == "SIMPLE_WEAR_PROCESS"
            else "DETAIL_OR_WORN_RELATION"
        )
        if unit_count == 3:
            return ["WORN_OR_PRODUCT_OPENING", middle, "WORN_OR_CONTEXT_RESULT"]
        if unit_count == 4:
            return [
                "WORN_OR_PRODUCT_OPENING",
                "DETAIL_PROOF",
                middle,
                "WORN_OR_CONTEXT_RESULT",
            ]
        return [
            "WORN_OR_PRODUCT_OPENING",
            "DETAIL_PROOF",
            middle,
            *(["DISTINCT_WORN_RELATION"] * max(0, unit_count - 4)),
            "WORN_OR_CONTEXT_RESULT",
        ]
    if unit_count == 3:
        return [
            "PRODUCT_RESULT_CLOSE",
            "PRODUCT_OR_PERSON_RELATION",
            "PRODUCT_VISIBLE_RESULT",
        ]
    if unit_count == 4:
        return [
            "PRODUCT_RESULT_CLOSE",
            "PRODUCT_DETAIL_RELATION",
            "PRODUCT_OR_PERSON_RELATION",
            "PRODUCT_VISIBLE_RESULT",
        ]
    return [
        "PRODUCT_RESULT_CLOSE",
        "PRODUCT_DETAIL_RELATION",
        *(["DISTINCT_PRODUCT_RELATION"] * max(0, unit_count - 3)),
        "PRODUCT_VISIBLE_RESULT",
    ]


def _category_capture_framing(
    *, product_subtype: str, unit_count: int, action_mode: str
) -> List[str]:
    if product_subtype in _SCARF_TYPES:
        process_or_relation = (
            "独立补录一次已经冻结的简单佩戴步骤，完成后保持同一结果；不得从已经佩戴完成倒退为重新系结"
            if action_mode == "SIMPLE_WEAR_PROCESS"
            else "独立补录一个新的商品细节或佩戴关系，改变真实观看信息而不是数字裁切"
        )
        values = [
            "独立录制已经搭配完成的围巾结果或商品主体开场，让商品与当前佩戴区域清楚可辨",
            "独立补录图案、边缘、垂坠或与领口/头发的一个已确认关系",
            process_or_relation,
            "独立补录商品与人物穿搭或当前生活场景的清楚结果；不擅自增加远景收尾",
        ]
    else:
        values = [
            "独立录制小商品已经佩戴完成的主体近景，让商品第一眼清楚可辨",
            "独立补录商品结构、佩戴落点或与人体局部的清楚关系，不用数字裁切假装换镜头",
            "独立补录人物、穿搭或自然使用中的一种新关系；中远景不得承担小商品结构证明",
            "在当前结构最后一个兼容片段中保持商品清楚可辨，不额外补无信息远景收尾",
        ]
    if unit_count == 3:
        # Three visible clips still need a complete opening-middle-result arc.
        # Truncating the four-clip template would drop the final product result
        # and make the third role disagree with its framing instruction.
        return [values[0], values[2], values[-1]]
    if unit_count <= len(values):
        return values[:unit_count]
    return [
        *values[:-1],
        *(
            ["独立补录另一项兼容证明或观看关系，不重复上一段核心动作"]
            * (unit_count - len(values))
        ),
        values[-1],
    ]


def _small_accessory_motion_capture(
    *, unit_count: int, terminal_guidance: str
) -> tuple[List[str], List[str]]:
    """Project the existing small-accessory motion arc once, before blueprint."""

    if unit_count == 3:
        roles = [
            "PRODUCT_RESULT_CLOSE",
            "NATURAL_MOTION_RELATION",
            "PRODUCT_REACQUISITION",
        ]
    else:
        middle_roles = ["NATURAL_MOTION_RELATION"]
        if unit_count >= 4:
            middle_roles.append("PRODUCT_DETAIL_RELATION")
        if unit_count >= 5:
            middle_roles.extend(
                ["CONTEXT_RELATION"] * (unit_count - 4)
            )
        roles = [
            "PRODUCT_RESULT_CLOSE",
            *middle_roles,
            "PRODUCT_REACQUISITION",
        ]

    framing: List[str] = []
    for index, role in enumerate(roles):
        if index == 0:
            framing.append(
                "独立录制商品已经佩戴完成的结果近景，让小商品第一眼清楚可辨"
            )
        elif index == len(roles) - 1:
            framing.append(
                terminal_guidance or "同一地点补录商品结果近景，自然完成收束"
            )
        elif role == "NATURAL_MOTION_RELATION":
            framing.append(
                "同一地点重新放置手机，录制一次连续的上半身或拍摄关系变化，"
                "不用重复摆头支撑整段"
            )
        elif role == "PRODUCT_DETAIL_RELATION":
            framing.append(
                "同一地点补录商品佩戴细节或与人物的清晰位置关系；"
                "沿用本段原有可见事件，不重复上一段核心动作"
            )
        else:
            framing.append(
                "同一地点补录商品与当前生活状态的自然关系；"
                "商品仍清楚可辨，不重复核心动作或退到远景"
            )
    return roles, framing


def _small_accessory_performance_arc(
    kind: str, *, face_policy: str = ""
) -> List[Dict[str, str]]:
    no_face = _is_no_face(face_policy)
    if kind == "HAIR_ACCESSORY":
        return [
            {
                "unit_role": "PRODUCT_RESULT_CLOSE",
                "gaze_target": "先观察镜中或侧后方的发饰位置",
                "micro_reaction": "保持轻专注，像在确认当天已经完成的发型",
                "movement_guidance": (
                    "发饰从第一帧已经清楚可见，人物的肩部和头部正处在一个很小的自然角度变化中，"
                    "不先静止等待再开始"
                ),
            },
            {
                "unit_role": "NATURAL_MOTION_RELATION",
                "gaze_target": "动作完成时短暂与自己的手机建立视线联系",
                "micro_reaction": "确认取景后神情稍微放松，不做夸张惊喜",
            },
            {
                "unit_role": "PRODUCT_REACQUISITION",
                "gaze_target": "回到镜中整体或发饰结果确认点",
                "micro_reaction": "以轻确认自然结束，不做广告式定格",
                "movement_guidance": (
                    "通过一次轻微肩部或头部角度变化，或同一部手机的简单重新构图，"
                    "把发饰带回侧后方近景；动作延续到片段末尾，只在最后一瞬自然收住，不重新夹发"
                ),
            },
        ]
    return [
        {
            "unit_role": "PRODUCT_RESULT_CLOSE",
            "gaze_target": _face_pick(
                no_face,
                "先观察镜中耳侧或当前耳饰位置",
                "先观察耳侧或当前耳饰位置",
            ),
            "micro_reaction": _face_pick(
                no_face,
                "保持轻专注，让半脸与耳侧关系自然成立",
                "保持轻专注，让耳饰与耳侧关系自然成立",
            ),
            "movement_guidance": _face_pick(
                no_face,
                (
                    "耳饰从第一帧已经清楚可见，人物的肩部和侧脸正处在一个很小的自然角度变化中，"
                    "不先静止等待再开始"
                ),
                (
                    "耳饰从第一帧已经清楚可见，人物的肩部和耳侧正处在一个很小的自然角度变化中，"
                    "不先静止等待再开始"
                ),
            ),
        },
        {
            "unit_role": "NATURAL_MOTION_RELATION",
            "gaze_target": "角度变化后短暂看向自己的手机",
            "micro_reaction": "神情稍微放松，像朋友间确认佩戴效果",
        },
        {
            "unit_role": "PRODUCT_REACQUISITION",
            "gaze_target": _face_pick(
                no_face,
                "回到耳侧或镜中整体确认点",
                "回到耳侧整体确认点",
            ),
            "micro_reaction": "轻确认后自然收住，不触碰耳饰",
            "movement_guidance": _face_pick(
                no_face,
                (
                    "通过一次轻微上半身角度变化，或同一部手机的简单重新构图，"
                    "把耳饰带回半脸耳侧近景；动作延续到片段末尾，只在最后一瞬自然收住，不触碰耳饰"
                ),
                (
                    "通过一次轻微上半身角度变化，或同一部手机的简单重新构图，"
                    f"把耳饰带回{_NO_FACE_EAR_RELATION}；动作延续到片段末尾，"
                    "只在最后一瞬自然收住，不触碰耳饰"
                ),
            ),
        },
    ]


def _small_accessory_motion_fields(
    kind: str, *, face_policy: str = ""
) -> Dict[str, Any]:
    return {
        "schema_version": "action-design-v2-continuous-motion",
        "motion_scope": "ONE_CONTINUOUS_CHANGE",
        "motion_duration_preference": "3_TO_6_SECONDS",
        "performance_arc": _small_accessory_performance_arc(
            kind, face_policy=face_policy
        ),
    }


def _anchor_texts(anchor_card: Dict[str, Any]) -> List[str]:
    """Read only hard identity anchors as relationship authority.

    ``display_anchors`` may contain a model-authored presentation suggestion
    such as "手持展示成对耳饰".  That is not proof of the sold unit, so it must
    never authorize PAIR/SINGLE semantics.
    """

    values: List[str] = []
    for item in anchor_card.get("hard_anchors") or []:
        if isinstance(item, dict):
            values.append(
                item.get("anchor")
                or item.get("anchor_text")
                or item.get("name")
                or item.get("value")
            )
        else:
            values.append(item)
    return _dedupe(values, limit=30)


def _earring_pairing_authority(anchor_card: Dict[str, Any]) -> Dict[str, Any]:
    anchors = _anchor_texts(anchor_card)
    joined = "；".join(anchors)
    has_pair = any(term in joined for term in _PAIR_TERMS)
    has_single = any(term in joined for term in _SINGLE_TERMS)
    if has_pair and not has_single:
        mode = "PAIR"
    elif has_single and not has_pair:
        mode = "SINGLE"
    else:
        mode = "UNAVAILABLE"
    return {
        "pairing_mode": mode,
        "authority_source": "APPROVED_ANCHORS" if mode != "UNAVAILABLE" else "UNAVAILABLE",
        "must_not_assume": (
            [] if mode != "UNAVAILABLE" else ["耳饰为单只还是成对"]
        ),
    }


def _resolve_product_prominence(
    profile: Dict[str, Any], *, presentation_mode: str, face_policy: str = ""
) -> Dict[str, Any]:
    """Project one small-product viewing scale without creating a QC gate."""

    contract = dict(profile.get("product_prominence") or {})
    if not contract:
        return {}
    no_face = _is_no_face(face_policy)
    mode = _text(presentation_mode).upper()
    motion_kind = _text(contract.get("motion_projection_kind")).upper()
    # Historical frozen extensions predate ``motion_projection_kind``.  Infer
    # only from the registered subtype so old SCRIPT_READY rows can receive
    # the new soft render projection without rerunning blueprint or voiceover.
    if not motion_kind:
        product_subtype = _text(profile.get("product_subtype"))
        if product_subtype in _HAIR_TYPES:
            motion_kind = "HAIR_ACCESSORY"
        elif product_subtype == "earring":
            motion_kind = "EAR_ACCESSORY"
    if no_face and motion_kind == "EAR_ACCESSORY":
        # The authored earring scale names the face.  Keep the same viewing
        # intent -- the ear and the product remain the observation target --
        # but strip every face reference from the descriptive prose.
        for key in ("opening_guidance", "context_guidance"):
            if _text(contract.get(key)):
                contract[key] = _no_face_safe_text(contract[key])
    wearer = mode in {
        "PERSON_ON_CAMERA", "MIXED", "WEARER_ACTIVE", "WEARER_PASSIVE",
    }
    if (
        wearer
        and motion_kind in {"HAIR_ACCESSORY", "EAR_ACCESSORY"}
        and _small_accessory_motion_enabled()
    ):
        if motion_kind == "HAIR_ACCESSORY":
            ending_framing = "后脑发饰区域或头肩侧后方近景"
            ending_guidance = (
                "最后一段在同一地点补录后脑发饰区域或头肩侧后方近景，"
                "让发饰、佩戴位置、相对大小和发束关系重新清楚可辨"
            )
        elif no_face:
            ending_framing = _NO_FACE_EAR_RELATION
            ending_guidance = (
                f"最后一段在同一地点补录{_NO_FACE_EAR_RELATION}，"
                "让耳饰本体、佩戴落点和相对长度重新清楚可辨"
            )
        else:
            ending_framing = "半脸耳侧近景"
            ending_guidance = (
                "最后一段在同一地点补录半脸耳侧近景，"
                "让耳饰本体、佩戴落点和相对长度重新清楚可辨"
            )
        contract.update({
            "schema_version": "small-accessory-prominence-v2-motion-return",
            "sequence_policy": "PRODUCT_OPENING_TO_MOTION_TO_PRODUCT_RETURN",
            "capture_arc": [
                "PRODUCT_RESULT_CLOSE",
                "NATURAL_MOTION_RELATION",
                "PRODUCT_REACQUISITION",
            ],
            "terminal_visibility": {
                "category": motion_kind,
                "ending_framing": ending_framing,
                "ending_guidance": ending_guidance,
                "renderer_authority": "SOFT_DETERMINISTIC_PROJECTION",
                "hard_qc": False,
                "may_trigger_retry": False,
            },
            "performance_arc": _small_accessory_performance_arc(
                motion_kind, face_policy=face_policy
            ),
        })
    if mode in {"HAND_ONLY", "HANDS_ONLY"}:
        contract.update({
            "primary_observation_unit": "PRODUCT_AND_SAME_PERSON_HANDS",
            "primary_framing": "PRODUCT_DOMINANT_HAND_CLOSE",
            "context_framing": "SMALL_REAL_ENVIRONMENT",
            "opening_guidance": (
                "首个核心展示段让商品和同一人物的手成为画面主要观察区域，"
                "只保留少量真实环境，不使用远景证明小商品"
            ),
            "context_guidance": (
                "后续可稍微带到桌面或日常环境，但商品仍保持清楚可辨"
            ),
        })
    elif mode == "STATIC_PRODUCT":
        contract.update({
            "primary_observation_unit": "PRODUCT_ONLY",
            "primary_framing": "PRODUCT_DOMINANT_CLOSE",
            "context_framing": "SMALL_REAL_ENVIRONMENT",
            "opening_guidance": (
                "首个核心展示段让商品本体成为画面主要观察区域，"
                "只保留少量真实环境，不使用远景证明小商品"
            ),
            "context_guidance": (
                "后续可稍微带到摆放关系，但商品不退到难以辨认的位置"
            ),
        })
    contract.update({
        "sequence_policy": contract.get("sequence_policy")
        or "ONE_PRODUCT_DOMINANT_VIEW_THEN_CONTEXT_VIEW",
        "opening_timing": "FIRST_CORE_DISPLAY_SEGMENT_PREFER_0_TO_3S",
        "authority": "SOFT_CATEGORY_COMPOSITION",
        "hard_required": False,
        "may_trigger_retry": False,
        "may_override_structure_or_wear_state": False,
    })
    return contract


@lru_cache(maxsize=1)
def _risk_registry() -> Dict[str, Any]:
    try:
        data = json.loads(_RISK_REGISTRY_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _profile_definition(canonical_type: str) -> Dict[str, Any]:
    if canonical_type == "earring":
        return {
            "display_family": "EAR_ACCESSORY",
            "wearing_zone": "EAR",
            "required_result_view": "ALREADY_WORN_EAR_VISIBLE",
            "supporting_style_context": "FACE_AND_OUTFIT_RELATION",
            "product_prominence": {
                "scope": "SMALL_WORN_ACCESSORY",
                "motion_projection_kind": "EAR_ACCESSORY",
                "primary_observation_unit": "EAR_AND_HALF_FACE",
                "primary_framing": "EAR_HALF_FACE_CLOSE",
                "context_framing": "HEAD_SHOULDER_OR_UPPER_BODY",
                "opening_guidance": (
                    "首个核心展示段优先使用半脸与耳侧近景，让耳饰本体、佩戴落点和相对长度成为主要观察对象"
                ),
                "context_guidance": (
                    "后续可回到头肩或上半身交代人物与穿搭；中远景只作关系景，不承担耳饰结构证明"
                ),
            },
            "risk_registry_key": "ear_accessory",
            "identity_priority": [
                "pair_or_single",
                "attachment_type",
                "component_order",
                "relative_length",
                "material_color",
            ],
        }
    if canonical_type in _HAIR_TYPES:
        return {
            "display_family": "HAIR_ACCESSORY",
            "wearing_zone": "HAIR",
            "required_result_view": "ALREADY_STYLED_HAIR_RESULT",
            "supporting_style_context": "DAILY_HAIRSTYLE",
            "product_prominence": {
                "scope": "SMALL_WORN_ACCESSORY",
                "motion_projection_kind": "HAIR_ACCESSORY",
                "primary_observation_unit": "HAIR_ACCESSORY_AND_HAIR_REGION",
                "primary_framing": "HAIR_REGION_CLOSE",
                "context_framing": "HEAD_SHOULDER_REAR_OR_MIRROR",
                "opening_guidance": (
                    "首个核心展示段优先使用后脑发饰区域近景，让发饰本体、夹持位置、相对大小和发束关系成为主要观察对象"
                ),
                "context_guidance": (
                    "后续可回到头肩侧后方或镜面关系景；镜面必须裁到头肩范围，全身镜面和中远景不能承担发饰证明"
                ),
            },
            "process_policy": "RESULT_FIRST_NO_FULL_STYLING_PROCESS",
            "preferred_carriers": ["WEARER_ACTIVE", "MIXED", "HAND_ONLY"],
            "compatible_proof_subjects": [
                "ON_BODY_RESULT", "SCENE_USAGE", "PRODUCT_DETAIL",
                "GENERAL_EXPRESSION",
            ],
            "outfit_context": {
                "target_role": "SUPPORTING_OUTFIT_HAIR",
                "visibility_zones": ["HEAD", "HAIR", "SHOULDER", "UPPER_BODY"],
                "occlusion_avoid": ["帽子", "头巾", "遮住商品的浓密发束", "竞争性头部配饰"],
                "style_family_preferences": ["DAILY_HAIR_RESULT", "CITY_HAIR_ACCENT"],
                "visibility_requirement": "至少一段从侧后方或镜面关系清楚看到发饰位置、相对大小、发型结果与上半身穿搭",
            },
            "scene_preferences": [
                "VANITY_TRYON", "HOME_ROUTINE", "CAFE_DINING", "STREET_OUTING",
            ],
            "capture_relationship": (
                "后脑区域商品的基础关系优先使用镜面反射，或侧后方三分之四手机视角；"
                "同一地点的独立细节片段与头肩关系片段可以直接剪切，"
                "不要求人物正对前置镜头同时展示后脑"
            ),
            "risk_registry_key": "hair_accessory",
            "identity_priority": [
                "accessory_subtype",
                "overall_shape",
                "ornament_layout",
                "relative_size",
                "placement_zone",
            ],
        }
    if canonical_type in _FINGER_TYPES:
        return {
            "display_family": "FINGER_ACCESSORY",
            "wearing_zone": "FINGER_HAND",
            "required_result_view": "ALREADY_WORN_FINGER_RESULT",
            "supporting_style_context": "HAND_AND_SIMPLE_UPPER_BODY_RELATION",
            "product_prominence": {
                "scope": "SMALL_WORN_ACCESSORY",
                "primary_observation_unit": "RING_FINGER_AND_HAND",
                "primary_framing": "RING_HAND_CLOSE",
                "context_framing": "HAND_FOREARM_OR_UPPER_BODY",
                "opening_guidance": (
                    "首个核心展示段优先使用手指与手部近景，让戒指本体、戒面、开口或指间佩戴位置成为主要观察对象"
                ),
                "context_guidance": (
                    "后续可带到手、前臂或简洁上半身关系；中远景只作生活关系，不承担戒指结构证明"
                ),
            },
            "process_policy": "ONE_GOVERNED_ACTION_THEN_STABLE_WORN_RESULT",
            "preferred_carriers": ["HAND_ONLY", "MIXED", "WEARER_ACTIVE"],
            "compatible_proof_subjects": [
                "ON_BODY_RESULT", "SCENE_USAGE", "PRODUCT_DETAIL",
                "GENERAL_EXPRESSION",
            ],
            "outfit_context": {
                "target_role": "SUPPORTING_HAND_STYLING",
                "visibility_zones": ["FINGER", "HAND", "FOREARM", "UPPER_BODY"],
                "occlusion_avoid": ["竞争性戒指", "抢主体的手链或腕表", "遮挡手部的宽大袖口"],
                "style_family_preferences": ["CLEAN_HAND_DETAIL", "DAILY_RING_ACCENT"],
                "visibility_requirement": "至少一段清楚看到戒指本体、手指佩戴位置和相对尺寸；人物上半身入镜时采用简洁上衣与干净手部造型",
            },
            "scene_preferences": [
                "HOME_ROUTINE", "CAFE_DINING", "OFFICE_WORKBREAK", "STREET_OUTING",
            ],
            "capture_relationship": (
                "以同一人物的手部近景为主要证明；如带入上半身，只作自然生活关系，"
                "不得用远景代替戒指本体证明"
            ),
            "risk_registry_key": "general_accessory",
            "identity_priority": [
                "overall_shape", "ring_face", "opening_or_closed_band",
                "ornament_layout", "material_color",
            ],
        }
    if canonical_type in _WRIST_TYPES:
        return {
            "display_family": "WRIST_ACCESSORY",
            "wearing_zone": "WRIST_FOREARM",
            "required_result_view": "ALREADY_WORN_WRIST_RESULT",
            "supporting_style_context": "WRIST_AND_OUTFIT_RELATION",
            "product_prominence": {
                "scope": "SMALL_WORN_ACCESSORY",
                "primary_observation_unit": "WRIST_AND_FOREARM",
                "primary_framing": "WRIST_FOREARM_CLOSE",
                "context_framing": "UPPER_BODY_OR_NATURAL_USE",
                "opening_guidance": (
                    "首个核心展示段优先使用手腕与前臂近景，让腕饰本体、佩戴位置和相对宽度成为主要观察对象"
                ),
                "context_guidance": (
                    "后续可回到上半身或拿包等自然使用关系；中全景只作穿搭关系，不承担腕饰结构证明"
                ),
            },
            "process_policy": "RESULT_FIRST_NO_FULL_WEAR_PROCESS",
            "preferred_carriers": ["WEARER_ACTIVE", "MIXED", "HAND_ONLY"],
            "compatible_proof_subjects": [
                "ON_BODY_RESULT", "SCENE_USAGE", "PRODUCT_DETAIL",
                "GENERAL_EXPRESSION",
            ],
            "outfit_context": {
                "target_role": "SUPPORTING_OUTFIT_WRIST",
                "visibility_zones": ["WRIST", "FOREARM", "UPPER_BODY"],
                "occlusion_avoid": ["遮住腕部的长袖口", "叠戴手表", "竞争性手链或手镯"],
                "style_family_preferences": ["DAILY_WRIST_ACCENT", "CITY_WRIST_DETAIL"],
                "visibility_requirement": "至少一段清楚看到腕饰已经佩戴在手腕上的比例、位置及与袖口或整套穿搭的关系",
            },
            "scene_preferences": [
                "HOME_ROUTINE", "CAFE_DINING", "OFFICE_WORKBREAK", "STREET_OUTING",
            ],
            "capture_relationship": (
                "手机保持普通自拍或固定近距离记录关系，腕部自然进入画面；"
                "不使用第三人商业跟拍或悬空产品广告镜头"
            ),
            "risk_registry_key": "general_accessory",
            "identity_priority": [
                "accessory_subtype",
                "overall_shape",
                "relative_width",
                "ornament_layout",
                "material_color",
            ],
        }
    scarf_profiles = {
        "winter_scarf": {
            "display_family": "WORN_ACCESSORY",
            "wearing_zone": "NECK_SHOULDER",
            "required_result_view": "ALREADY_WORN_UPPER_BODY_RESULT",
            "supporting_style_context": "OUTERWEAR_OR_KNIT_LAYERING",
            "process_policy": "RESULT_FIRST_SIMPLE_ADJUSTMENT_ONLY",
            "preferred_carriers": ["WEARER_ACTIVE", "MIXED"],
            "compatible_proof_subjects": [
                "ON_BODY_RESULT", "SCENE_USAGE", "PRODUCT_DETAIL",
                "GENERAL_EXPRESSION",
            ],
            "outfit_context": {
                "target_role": "SUPPORTING_OUTFIT_NECK",
                "visibility_zones": ["NECK", "SHOULDER", "UPPER_BODY"],
                "occlusion_avoid": ["夸张大翻领", "遮住围巾主体的头发或包带"],
                "style_family_preferences": ["WINTER_DAILY_LAYERING", "COMMUTE_OUTER_LAYER"],
                "visibility_requirement": "至少一段清楚看到围巾与脖颈、肩部及上半身穿搭关系",
            },
            "scene_preferences": [
                "HOME_ROUTINE", "OFFICE_WORKBREAK", "STREET_OUTING", "CAR_TRANSIT",
            ],
            "risk_registry_key": "winter_scarf",
            "identity_priority": [
                "main_color", "pattern", "fringe", "overall_shape", "drape_relation"
            ],
        },
        "silk_scarf": {
            "display_family": "LIGHT_NECK_ACCESSORY",
            "wearing_zone": "NECK_UPPER_BODY",
            "required_result_view": "ALREADY_STYLED_NECK_RESULT",
            "supporting_style_context": "LIGHT_OUTFIT_ACCENT",
            "process_policy": "RESULT_FIRST_SIMPLE_ADJUSTMENT_ONLY",
            "preferred_carriers": ["WEARER_ACTIVE", "MIXED"],
            "compatible_proof_subjects": [
                "ON_BODY_RESULT", "SCENE_USAGE", "PRODUCT_DETAIL",
                "GENERAL_EXPRESSION",
            ],
            "outfit_context": {
                "target_role": "SUPPORTING_OUTFIT_NECK",
                "visibility_zones": ["NECK", "SHOULDER", "UPPER_BODY"],
                "occlusion_avoid": ["复杂领型", "项链", "遮住丝巾的头发"],
                "style_family_preferences": ["TH_WARM_CITY_FEMININE", "TH_WARM_CASUAL_CHIC"],
                "visibility_requirement": "至少一段清楚看到丝巾与领口、脸部附近及上半身关系",
            },
            "scene_preferences": [
                "CAFE_DINING", "OFFICE_WORKBREAK", "HOME_ROUTINE", "STREET_OUTING",
            ],
            "risk_registry_key": "silk_scarf",
            "identity_priority": [
                "main_color", "pattern", "overall_shape", "neckline_relation"
            ],
        },
        "headscarf": {
            "display_family": "HEAD_SCARF_ACCESSORY",
            "wearing_zone": "HEAD_HAIR",
            "required_result_view": "ALREADY_STYLED_HEAD_RESULT",
            "supporting_style_context": "DAILY_HEAD_STYLE",
            # USE_PROCESS is a coarse structure beat: it may be a harmless
            # already-worn adjustment, not necessarily a wrapping tutorial.
            # Keep it as a soft preference and let the physical interaction
            # boundary forbid full wrapping/tying specifically.
            "process_policy": "RESULT_FIRST_SIMPLE_ADJUSTMENT_ONLY",
            "preferred_carriers": ["WEARER_ACTIVE", "MIXED"],
            "compatible_proof_subjects": [
                "ON_BODY_RESULT", "SCENE_USAGE", "PRODUCT_DETAIL",
                "GENERAL_EXPRESSION",
            ],
            "outfit_context": {
                "target_role": "SUPPORTING_OUTFIT_HEAD",
                "visibility_zones": ["HEAD", "HAIR", "UPPER_BODY"],
                "occlusion_avoid": ["帽子", "夸张耳饰", "与头巾竞争的复杂头部配饰"],
                "style_family_preferences": ["Y2K_BOLD_FEMININE", "STREET_FEMININE", "RESORT_CHIC"],
                "visibility_requirement": "至少一段清楚看到头巾位置、轮廓、头发状态与穿搭关系",
            },
            "scene_preferences": [
                "HOME_ROUTINE", "VANITY_TRYON", "CAFE_DINING", "STREET_OUTING",
            ],
            "risk_registry_key": "headscarf",
            "identity_priority": [
                "main_color", "pattern", "overall_shape", "placement_zone"
            ],
        },
        "scarf": {
            "display_family": "WORN_ACCESSORY",
            "wearing_zone": "NECK_SHOULDER",
            "required_result_view": "ALREADY_WORN_UPPER_BODY_RESULT",
            "supporting_style_context": "OUTFIT_LAYERING",
            "process_policy": "RESULT_FIRST_SIMPLE_ADJUSTMENT_ONLY",
            "preferred_carriers": ["WEARER_ACTIVE", "MIXED"],
            "compatible_proof_subjects": [
                "ON_BODY_RESULT", "SCENE_USAGE", "PRODUCT_DETAIL",
                "GENERAL_EXPRESSION",
            ],
            "outfit_context": {
                "target_role": "SUPPORTING_OUTFIT_NECK",
                "visibility_zones": ["NECK", "SHOULDER", "UPPER_BODY"],
                "occlusion_avoid": ["遮住商品主体的衣领、头发或包带"],
                "style_family_preferences": ["DAILY_NECK_ACCENT", "LIGHT_DAILY_LAYERING"],
                "visibility_requirement": "至少一段清楚看到商品与肩颈、上半身穿搭关系",
            },
            "scene_preferences": ["HOME_ROUTINE", "OFFICE_WORKBREAK", "STREET_OUTING"],
            "risk_registry_key": "scarf",
            "identity_priority": [
                "main_color", "pattern", "fringe", "overall_shape", "drape_relation"
            ],
        },
    }
    return scarf_profiles[canonical_type]


_RESULT_LABELS = {
    "ALREADY_WORN_EAR_VISIBLE": "从已经佩戴好的状态开始，至少有一段清楚看到耳饰与耳部、脸部或整套穿搭的关系",
    "ALREADY_STYLED_HAIR_RESULT": "真人方向从已经固定好的发型状态开始，至少有一段清楚看到发饰位置、相对大小和发型结果",
    "ALREADY_WORN_WRIST_RESULT": "真人方向从已经佩戴好的腕部状态开始，至少有一段清楚看到腕饰与手腕、袖口或整套穿搭的比例关系",
    "ALREADY_WORN_FINGER_RESULT": "从戒指已经位于手指或正在完成一次冻结动作的状态开始，至少有一段清楚看到戒指本体、佩戴位置和手部关系",
    "ALREADY_WORN_UPPER_BODY_RESULT": "从已经围好或披好的状态开始，至少有一段清楚看到围巾与脖颈、肩部及上半身穿搭的关系",
    "ALREADY_STYLED_NECK_RESULT": "从已经搭配好的状态开始，至少有一段清楚看到丝巾与颈部、领口及上半身穿搭的关系",
    "ALREADY_STYLED_HEAD_RESULT": "从已经完成的头部造型开始，至少有一段清楚看到头巾位置、轮廓及与头发和穿搭的关系",
    "ALREADY_STYLED_BAG_ACCENT_RESULT": "从已经搭配好的包袋点缀状态开始，至少有一段清楚看到商品与包袋及整体造型的关系",
    "PRODUCT_DETAIL_ONLY": "当前承载只展示商品外观与结构，不声称必须依靠真人佩戴才能证明的效果",
}

_RELATION_LABELS = {
    "EAR": "商品已经正确佩戴在耳部，耳侧保持清楚可见",
    "HAIR": "商品已经固定在头发或发型的正确位置",
    "WRIST_FOREARM": "商品已经正确佩戴在手腕位置，腕部和前臂关系保持清楚可见",
    "FINGER_HAND": "商品位于同一人物的手指或手部近景中，戒指本体与佩戴位置保持清楚可见",
    "NECK_SHOULDER": "商品已经自然位于脖颈和肩部区域",
    "NECK_UPPER_BODY": "商品已经自然搭配在颈部和上半身领口区域",
    "HEAD_HAIR": "商品已经位于头部或头发的日常造型位置",
    "BAG_ACCESSORY": "商品已经作为点缀固定在日常包袋上",
}

_DEMONSTRATION_PROFILES = {
    "EAR_WORN": {
        "wearing_zone": "EAR",
        "required_result_view": "ALREADY_WORN_EAR_VISIBLE",
        "supporting_style_context": "FACE_AND_OUTFIT_RELATION",
        "optional_simple_interactions": [
            "让肩部、上半身和侧脸一起完成一次连续角度变化，耳侧始终无遮挡",
            "镜面头肩结果与半脸耳侧近景分成两段手机素材直接剪切",
        ],
    },
    "HAIR_WORN": {
        "wearing_zone": "HAIR",
        "required_result_view": "ALREADY_STYLED_HAIR_RESULT",
        "supporting_style_context": "DAILY_HAIRSTYLE",
        "optional_simple_interactions": [
            "从已经夹好的发型开始，让肩部、上半身和头部一起完成一次连续转向",
            "把镜面头肩结果与侧后方发饰近景分成两段手机素材直接剪切",
            "在头肩构图内自然改变一次重心，随后补录发饰结果近景",
        ],
    },
    "WRIST_WORN": {
        "wearing_zone": "WRIST_FOREARM",
        "required_result_view": "ALREADY_WORN_WRIST_RESULT",
        "supporting_style_context": "WRIST_AND_OUTFIT_RELATION",
        "optional_simple_interactions": [
            "腕饰已经戴好，前臂自然落在桌面或包带旁",
            "手腕只做一次很小的自然转动，让整体轮廓看清",
            "用一个日常拿取动作带出腕饰与袖口的关系，不重新佩戴",
        ],
    },
    "FINGER_WORN": {
        "wearing_zone": "FINGER_HAND",
        "required_result_view": "ALREADY_WORN_FINGER_RESULT",
        "supporting_style_context": "HAND_AND_SIMPLE_UPPER_BODY_RELATION",
        "optional_simple_interactions": [
            "戒指已经戴好，手部只做一次自然角度变化",
            "手部在拿杯、翻页或拿包时自然进入近景，戒指仍然清楚",
            "先用同一人物的手托住商品看清，再进入稳定佩戴结果",
        ],
    },
    "NECK_WORN": {
        "wearing_zone": "NECK_UPPER_BODY",
        "required_result_view": "ALREADY_STYLED_NECK_RESULT",
        "supporting_style_context": "LIGHT_OUTFIT_ACCENT",
        "optional_simple_interactions": [
            "轻托一次已经系好的垂端",
            "拨开一次遮挡商品的头发",
            "自然转向让领口与商品关系更清楚",
        ],
    },
    "HEAD_WORN": {
        "wearing_zone": "HEAD_HAIR",
        "required_result_view": "ALREADY_STYLED_HEAD_RESULT",
        "supporting_style_context": "DAILY_HEAD_STYLE",
        "optional_simple_interactions": [
            "轻扶一次已经固定好的边缘",
            "自然侧脸让图案与佩戴位置更清楚",
            "拨开一次遮挡商品的脸侧头发",
        ],
    },
    "HAIR_TIE": {
        "wearing_zone": "HEAD_HAIR",
        "required_result_view": "ALREADY_STYLED_HAIR_RESULT",
        "supporting_style_context": "DAILY_HAIRSTYLE",
        "optional_simple_interactions": [
            "轻托一次已经系好的马尾或发尾",
            "自然侧头让结点与垂端进入镜头",
        ],
    },
    "BAG_ACCENT": {
        "wearing_zone": "BAG_ACCESSORY",
        "required_result_view": "ALREADY_STYLED_BAG_ACCENT_RESULT",
        "supporting_style_context": "DAILY_BAG_ACCENT",
        "optional_simple_interactions": [
            "正常拿起一次已经搭配好的包袋",
            "轻扶一次已经固定好的结点或垂端",
        ],
    },
}


_COMMON_WEARER_INTERACTION_CAPABILITIES = [
    {
        "interaction_id": "WORN_RESULT_ANGLE",
        "primary_action_mode": "RESULT_SHOW",
        "start_state": "商品已经完成佩戴并保持正确位置",
        "core_action": "人物用一次自然角度变化展示商品与穿搭的关系",
        "end_state": "回到清楚可见的完整佩戴结果",
        "risk_tier": "LOW",
        "action_keywords": ["角度", "展示", "佩戴结果"],
    },
    {
        "interaction_id": "WORN_DETAIL_TO_PHONE",
        "primary_action_mode": "DETAIL_SHOW",
        "start_state": "商品已经完成佩戴并保持正确位置",
        "core_action": "只把一个已确认的可见细节自然带近手机看清",
        "end_state": "细节看清后回到完整佩戴关系",
        "risk_tier": "LOW",
        "action_keywords": ["细节", "带近", "看清"],
    },
]


_SCARF_PROCESS_CAPABILITIES = {
    "silk_scarf": {
        "NECK_WORN": {
            "interaction_id": "SILK_SCARF_PRELOOPED_SIMPLE_KNOT",
            "primary_action_mode": "SIMPLE_WEAR_PROCESS",
            "start_state": "丝巾已经绕过颈部并形成一个松结",
            "core_action": "把短端穿过已有松结并轻轻收好，不重做完整围法",
            "end_state": "结点完成后停留展示领口与上半身搭配结果",
            "risk_tier": "MEDIUM",
            "action_keywords": ["穿过", "松结", "收好"],
        },
        "HAIR_TIE": {
            "interaction_id": "SILK_SCARF_HAIR_TIE_FINISH",
            "primary_action_mode": "SIMPLE_WEAR_PROCESS",
            "start_state": "商品已经绕住发束并形成稳定结点",
            "core_action": "只把一端收进已有结点并整理一次垂端",
            "end_state": "保持已经完成的发尾点缀结果",
            "risk_tier": "MEDIUM",
            "action_keywords": ["收进", "结点", "垂端"],
        },
        "BAG_ACCENT": {
            "interaction_id": "SILK_SCARF_BAG_KNOT_FINISH",
            "primary_action_mode": "SIMPLE_WEAR_PROCESS",
            "start_state": "商品已经绕过包柄并形成松结",
            "core_action": "把短端穿过已有松结并轻轻收好",
            "end_state": "拿起包袋展示已经完成的点缀结果",
            "risk_tier": "MEDIUM",
            "action_keywords": ["包柄", "穿过", "收好"],
        },
    },
    "headscarf": {
        "HEAD_WORN": {
            "interaction_id": "HEADSCARF_EDGE_TUCK_FINISH",
            "primary_action_mode": "SIMPLE_WEAR_PROCESS",
            "start_state": "头巾主体已经完成佩戴并保持稳定轮廓",
            "core_action": "只把耳侧一小段松边收进已有造型，不重新包裹或缠绕",
            "end_state": "收好后展示额前边缘、图案和侧面结果",
            "risk_tier": "MEDIUM",
            "action_keywords": ["松边", "收进", "侧面结果"],
        },
    },
    "winter_scarf": {
        "NECK_WORN": {
            "interaction_id": "WINTER_SCARF_SHOULDER_DRAPE",
            "primary_action_mode": "SIMPLE_WEAR_PROCESS",
            "start_state": "围巾已经自然位于颈肩区域",
            "core_action": "把一侧垂端自然搭回肩侧并顺手整理一次",
            "end_state": "保持肩颈与外层穿搭关系完整可见",
            "risk_tier": "MEDIUM",
            "action_keywords": ["垂端", "肩侧", "整理"],
        },
    },
    "scarf": {
        "NECK_WORN": {
            "interaction_id": "SCARF_SIMPLE_DRAPE_FINISH",
            "primary_action_mode": "SIMPLE_WEAR_PROCESS",
            "start_state": "商品已经处于稳定的肩颈搭配状态",
            "core_action": "只调整一侧垂落关系并自然放回",
            "end_state": "保留商品与肩颈、上半身的完整关系",
            "risk_tier": "MEDIUM",
            "action_keywords": ["调整", "垂落", "放回"],
        },
    },
}


def _interaction_capabilities(
    *,
    product_subtype: str,
    demonstration_mode: str,
    presentation_mode: str,
    preferred_action_mode: str = "",
    face_policy: str = "",
) -> list[dict[str, Any]]:
    no_face = _is_no_face(face_policy)
    mode = _text(presentation_mode).upper()
    preferred_mode = _text(preferred_action_mode).upper()
    wrist_process = {
        "interaction_id": "WRIST_SIMPLE_PUT_ON",
        "primary_action_mode": "SIMPLE_WEAR_PROCESS",
        "start_state": "腕饰保持参考图中的完整形态，位于同一人物另一只手或腕部入口旁",
        "core_action": "只完成一次自然套入或戴入动作，不重复摘下和重新佩戴",
        "end_state": "腕饰稳定佩戴在手腕上，整体轮廓与腕部关系清楚",
        "risk_tier": "MEDIUM",
        "action_keywords": ["套入", "一次佩戴", "腕部结果"],
    }
    ring_adjust_then_wear = {
        "interaction_id": "RING_ADJUST_THEN_WEAR",
        "primary_action_mode": "ADJUST_THEN_WEAR",
        "start_state": "同一人物用另一只手稳定托住戒指，开口或可调节位置清楚可见",
        "core_action": "只完成一次很小的尺寸调节并戴到一根手指上，不反复掰动或摘戴",
        "end_state": "戒指稳定留在手指上，戒面、开口与指间比例清楚可见",
        "risk_tier": "MEDIUM",
        "action_keywords": ["一次小幅调节", "戴到手指", "稳定结果"],
    }
    ring_handheld = {
        "interaction_id": "RING_HANDHELD_MATERIAL_TO_WORN",
        "primary_action_mode": "HANDHELD_PRODUCT",
        "start_state": "同一人物用指腹自然托住戒指，商品完整形态清楚可见",
        "core_action": "在手部近景中只做一次很小的角度变化看清金属或戒面，再直接进入佩戴结果",
        "end_state": "戒指稳定佩戴在一根手指上，商品与手部关系清楚",
        "risk_tier": "MEDIUM",
        "action_keywords": ["手持近景", "一次角度变化", "佩戴结果"],
    }
    ring_result = {
        "interaction_id": "RING_WORN_RESULT_ANGLE",
        "primary_action_mode": "RESULT_SHOW",
        "start_state": "戒指已经稳定佩戴在一根手指上，手部自然放松",
        "core_action": "手部只做一次自然角度变化，让戒面、指间位置和整体效果看清",
        "end_state": "回到戒指无遮挡且手部造型干净的佩戴结果",
        "risk_tier": "LOW",
        "action_keywords": ["手部角度", "戒面", "佩戴结果"],
    }
    ring_scene_use = {
        "interaction_id": "RING_SCENE_USE_RELATION",
        "primary_action_mode": "SCENE_USE",
        "start_state": "戒指已经戴好，人物处于当前真实生活场景",
        "core_action": "通过一次拿杯、翻页、打字或拿包中的自然动作让佩戴手进入近景",
        "end_state": "动作结束时戒指本体仍清楚，场景只提供使用关系而不抢主体",
        "risk_tier": "LOW",
        "action_keywords": ["生活动作", "手部近景", "场景关系"],
    }
    if mode in {"STATIC_PRODUCT"}:
        return []
    if mode in {"HAND_ONLY", "HANDS_ONLY"}:
        if product_subtype in _FINGER_TYPES:
            capabilities = [ring_result, ring_handheld]
            if preferred_mode == "ADJUST_THEN_WEAR":
                capabilities.insert(0, ring_adjust_then_wear)
            return capabilities
        if product_subtype in _WRIST_TYPES and preferred_mode == "SIMPLE_WEAR_PROCESS":
            return [wrist_process]
        if product_subtype in _WRIST_TYPES or product_subtype in _HAIR_TYPES:
            return [
                {
                    "interaction_id": "SMALL_ACCESSORY_STABLE_DETAIL",
                    "primary_action_mode": "HANDHELD_PRODUCT",
                    "start_state": "商品保持与参考图一致的完整形态",
                    "core_action": "只用一只手或双手边缘稳定托住商品，让一个已确认结构看清",
                    "end_state": "商品保持原结构并稳定停住",
                    "risk_tier": "LOW",
                    "action_keywords": ["稳定托住", "结构", "停住"],
                },
            ]
        return [
            {
                "interaction_id": "HANDHELD_PARTIAL_UNFOLD",
                "primary_action_mode": "HANDHELD_PRODUCT",
                "start_state": "商品自然放在当前展示面上",
                "core_action": "双手只展开一个局部，让已确认图案或边缘进入画面",
                "end_state": "局部看清后自然放回或稳定停住",
                "risk_tier": "LOW",
                "action_keywords": ["展开", "图案", "边缘"],
            },
            {
                "interaction_id": "HANDHELD_DETAIL_TO_PHONE",
                "primary_action_mode": "DETAIL_SHOW",
                "start_state": "商品保持当前形状与图案布局",
                "core_action": "双手把一个已确认细节拿近手机看清",
                "end_state": "看清后放回原来的展示位置",
                "risk_tier": "LOW",
                "action_keywords": ["拿近", "细节", "放回"],
            },
        ]
    if product_subtype in _HAIR_TYPES and _small_accessory_motion_enabled():
        return [
            {
                "interaction_id": "HAIR_BODY_ARC_REVEAL",
                "primary_action_mode": "RESULT_SHOW",
                "start_state": "发饰已经固定完成并从第一帧可见，人物正处在很小的自然角度变化中",
                "core_action": "肩部、上半身和头部作为整体完成一次连续转向，头部自然跟随，不单独反复摆动",
                "end_state": "通过轻微角度变化回到商品无遮挡的发型结果，只在最后一瞬自然收住",
                "risk_tier": "LOW",
                "action_keywords": ["上半身连续转向", "侧后方", "发型结果"],
                **_small_accessory_motion_fields("HAIR_ACCESSORY"),
            },
            {
                "interaction_id": "HAIR_MIRROR_TO_REAR_CUT",
                "primary_action_mode": "RESULT_SHOW",
                "start_state": "发饰已经固定完成并从第一帧可见，人物在普通镜面距离内保持轻微自然状态变化",
                "core_action": "先录一段镜面头肩结果，再重新放置同一部手机补录侧后方发饰近景，两段直接剪切",
                "end_state": "重新构图回到发饰位置、相对大小和发束关系清楚的近景，只在最后一瞬收住",
                "risk_tier": "LOW",
                "action_keywords": ["镜面头肩", "侧后方近景", "直接剪切"],
                **_small_accessory_motion_fields("HAIR_ACCESSORY"),
            },
            {
                "interaction_id": "HAIR_WEIGHT_SHIFT_REFRAME",
                "primary_action_mode": "RESULT_SHOW",
                "start_state": "发饰已经固定完成并从第一帧可见，人物在头肩或上半身构图内轻微调整自然重心",
                "core_action": "人物在头肩关系内自然改变一次重心或轻微侧移，下一段重新放置手机补录发饰结果",
                "end_state": "通过重新构图回到发饰无遮挡的头肩侧后方近景，只在最后一瞬收住",
                "risk_tier": "LOW",
                "action_keywords": ["自然重心变化", "重新构图", "发饰近景"],
                **_small_accessory_motion_fields("HAIR_ACCESSORY"),
            },
        ]
    if product_subtype in _HAIR_TYPES:
        return [
            {
                "interaction_id": "HAIR_RESULT_REAR_THREE_QUARTER",
                "primary_action_mode": "RESULT_SHOW",
                "start_state": "发饰已经固定完成，发型和商品位置保持稳定",
                "core_action": "人物只做一次轻微侧头或回头，让侧后方三分之四角度看清发饰与发型结果",
                "end_state": "停在商品无遮挡的已完成发型结果",
                "risk_tier": "LOW",
                "action_keywords": ["侧后方", "轻微回头", "发型结果"],
            },
            {
                "interaction_id": "HAIR_RESULT_MIRROR_CONFIRM",
                "primary_action_mode": "RESULT_SHOW",
                "start_state": "发饰已经固定完成，人物与镜面保持普通生活距离",
                "core_action": "通过镜面只确认一次已经完成的后脑或侧后方佩戴结果",
                "end_state": "发饰位置、相对大小和发束关系保持清楚",
                "risk_tier": "LOW",
                "action_keywords": ["镜面", "后脑", "固定结果"],
            },
        ]
    if product_subtype == "earring" and _small_accessory_motion_enabled():
        return [
            {
                "interaction_id": "EAR_FACE_ARC_REVEAL",
                "primary_action_mode": "RESULT_SHOW",
                "start_state": _face_pick(
                    no_face,
                    "耳饰已经佩戴完成并从第一帧可见，耳侧和半脸正在发生很小的自然角度变化",
                    "耳饰已经佩戴完成并从第一帧可见，耳侧正在发生很小的自然角度变化",
                ),
                "core_action": _face_pick(
                    no_face,
                    "肩部、上半身和侧脸一起完成一次连续角度变化，耳饰始终留在清楚亮部",
                    "肩部、上半身和耳侧一起完成一次连续角度变化，耳饰始终留在清楚亮部",
                ),
                "end_state": _face_pick(
                    no_face,
                    "通过轻微角度变化回到耳饰、耳侧和半脸关系清楚的结果，只在最后一瞬收住",
                    "通过轻微角度变化回到耳饰与耳侧关系清楚的结果，只在最后一瞬收住",
                ),
                "risk_tier": "LOW",
                "action_keywords": _face_pick(
                    no_face,
                    ["侧脸连续变化", "半脸耳侧", "佩戴结果"],
                    ["耳侧连续变化", "耳侧近景", "佩戴结果"],
                ),
                **_small_accessory_motion_fields(
                    "EAR_ACCESSORY", face_policy=face_policy
                ),
            },
            {
                "interaction_id": "EAR_MIRROR_TO_PHONE",
                "primary_action_mode": "RESULT_SHOW",
                "start_state": _face_pick(
                    no_face,
                    "耳饰已经佩戴完成并从第一帧可见，人物在镜中观察时保持轻微自然状态变化",
                    "耳饰已经佩戴完成并从第一帧可见，人物保持轻微自然状态变化",
                ),
                "core_action": _face_pick(
                    no_face,
                    "镜面头肩结果与半脸耳侧近景分成两段手机素材，人物只短暂看向自己的手机",
                    "同机位重拍结果与"
                    + _NO_FACE_EAR_RELATION
                    + "分成两段手机素材，人物只短暂看向自己的手机",
                ),
                "end_state": _face_pick(
                    no_face,
                    "重新构图回到耳饰无遮挡的半脸耳侧结果，只在最后一瞬收住",
                    "重新构图回到耳饰无遮挡的耳侧结果，只在最后一瞬收住",
                ),
                "risk_tier": "LOW",
                "action_keywords": _face_pick(
                    no_face,
                    ["镜面", "手机视线", "耳侧近景"],
                    ["同机位重拍", "手机视线", "耳侧近景"],
                ),
                **_small_accessory_motion_fields(
                    "EAR_ACCESSORY", face_policy=face_policy
                ),
            },
            {
                "interaction_id": "EAR_RELATION_TO_CLOSE_RETURN",
                "primary_action_mode": "RESULT_SHOW",
                "start_state": "耳饰已经佩戴完成并从第一帧可见，人物在头肩关系内保持轻微自然状态变化",
                "core_action": _face_pick(
                    no_face,
                    "人物完成一次自然上半身角度变化，下一段重新放置手机回到半脸耳侧近景",
                    "人物完成一次自然上半身角度变化，"
                    f"下一段重新放置手机回到{_NO_FACE_EAR_RELATION}",
                ),
                "end_state": "通过重新构图让耳饰本体、佩戴落点和相对长度重新清楚可辨，只在最后一瞬收住",
                "risk_tier": "LOW",
                "action_keywords": ["头肩关系", "上半身变化", "耳侧回收"],
                **_small_accessory_motion_fields(
                    "EAR_ACCESSORY", face_policy=face_policy
                ),
            },
        ]
    if product_subtype in _WRIST_TYPES:
        capabilities = [
            {
                "interaction_id": "WRIST_RESULT_NATURAL_TURN",
                "primary_action_mode": "RESULT_SHOW",
                "start_state": "腕饰已经佩戴完成，手腕自然放松",
                "core_action": "前臂保持稳定，手腕只做一次小幅自然转动看清商品整体轮廓",
                "end_state": "回到腕饰、手腕与袖口关系清楚的佩戴结果",
                "risk_tier": "LOW",
                "action_keywords": ["手腕", "自然转动", "佩戴结果"],
            },
            {
                "interaction_id": "WRIST_RESULT_LIFESTYLE_REVEAL",
                "primary_action_mode": "RESULT_SHOW",
                "start_state": "腕饰已经戴好并保持正确位置",
                "core_action": "用一次自然拿包、拿杯或翻页动作让腕部进入画面，不触碰或重新佩戴商品",
                "end_state": "动作结束时腕饰主体仍清楚可见",
                "risk_tier": "LOW",
                "action_keywords": ["日常动作", "腕部", "清楚可见"],
            },
        ]
        if preferred_mode == "SIMPLE_WEAR_PROCESS":
            capabilities.append(wrist_process)
        return capabilities
    if product_subtype in _FINGER_TYPES:
        capabilities = [ring_result, ring_handheld, ring_scene_use]
        if preferred_mode == "ADJUST_THEN_WEAR":
            capabilities.insert(0, ring_adjust_then_wear)
        return capabilities
    capabilities = [dict(item) for item in _COMMON_WEARER_INTERACTION_CAPABILITIES]
    process = (
        _SCARF_PROCESS_CAPABILITIES.get(_text(product_subtype), {}).get(
            _text(demonstration_mode).upper()
        )
    )
    if process:
        capabilities.append(dict(process))
    return capabilities


def _wear_state_contract(selected_action: Dict[str, Any]) -> Dict[str, Any]:
    mode = _text(selected_action.get("primary_action_mode")).upper()
    if mode == "SIMPLE_WEAR_PROCESS":
        return {
            "schema_version": "wear-state-contract-v1",
            "initial_state": "IN_PROGRESS",
            "initial_state_zh": _text(selected_action.get("start_state")),
            "result_state": "STYLED_RESULT",
            "result_state_zh": _text(selected_action.get("end_state")),
            "continuity_rule_zh": "开场保持未完成状态，只完成冻结的一个简单步骤；不得先展示完整佩戴结果后再重新系结",
        }
    return {
        "schema_version": "wear-state-contract-v1",
        "initial_state": "ALREADY_STYLED",
        "initial_state_zh": _text(selected_action.get("start_state")),
        "result_state": "ALREADY_STYLED",
        "result_state_zh": _text(selected_action.get("end_state")),
        "continuity_rule_zh": "从已经完成的佩戴结果开始，只展示或轻调一次；不得重新系结或重做佩戴过程",
    }


def _hand_anatomy_guard(carrier_execution: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "schema_version": "hand-anatomy-guard-v1",
        "max_visible_hands": 2,
        "hand_owner": "SINGLE_PERSON",
        "guidance_zh": (
            "画面中最多出现同一人物自然生长的两只手；双手参与时左右手各自完成一个连续角色，"
            "不出现第三只手、助手手臂、重复手掌或额外手指结构"
        ),
        "hard_required": False,
        "presentation_mode": _text(carrier_execution.get("presentation_mode")),
    }

_IDENTITY_LABELS = {
    "pair_or_single": "单只或成对关系",
    "attachment_type": "佩戴连接结构",
    "component_order": "部件数量与排列顺序",
    "relative_length": "相对长度和比例",
    "material_color": "材质观感和颜色",
    "accessory_subtype": "发饰具体类型",
    "overall_shape": "整体形状",
    "ornament_layout": "装饰排列",
    "relative_size": "与头发的相对大小",
    "relative_width": "与手腕的相对宽度",
    "placement_zone": "佩戴位置",
    "main_color": "主色",
    "pattern": "图案",
    "fringe": "流苏或边缘结构",
    "drape_relation": "肩颈垂落关系",
    "neckline_relation": "与领口的搭配关系",
}


def _pairing_guidance(authority: Dict[str, Any]) -> str:
    mode = _text(authority.get("pairing_mode")).upper()
    if mode == "PAIR":
        return "授权锚点明确为成对耳饰；保持成对关系，不改写成单只商品"
    if mode == "SINGLE":
        return "授权锚点明确为单只耳饰；保持单只关系，不改写成一对商品"
    return "授权锚点未说明单只或成对；只写‘耳饰/耳侧’，不得补写一对、双耳、两只、单只或单耳"


def _identity_labels(values: Iterable[Any], limit: int = 4) -> List[str]:
    return [
        _IDENTITY_LABELS.get(value, value)
        for value in _dedupe(values, limit=limit)
    ]


class AccessoryExecutionAdapter(CategoryExecutionAdapter):
    domain = "ACCESSORY"

    def supports(self, *, product_type: str, top_category: str) -> bool:
        resolved = normalize_product_type(product_type, top_category)
        return (
            resolved.recognized_by_registry
            and resolved.canonical_type in _SUPPORTED_TYPES
        )

    def compile_profile(
        self,
        *,
        product_type: str,
        top_category: str,
        anchor_card: Dict[str, Any],
    ) -> Dict[str, Any]:
        resolved = normalize_product_type(product_type, top_category)
        if not (
            resolved.recognized_by_registry
            and resolved.canonical_type in _SUPPORTED_TYPES
        ):
            return {}
        definition = _profile_definition(resolved.canonical_type)
        risk = _risk_registry().get(definition["risk_registry_key"])
        risk = risk if isinstance(risk, dict) else {}
        interaction_boundary = list(risk.get("forbidden") or [])
        if resolved.canonical_type in _SCARF_TYPES:
            # The existing type registry already owns the scarf-specific
            # boundaries; reuse them instead of introducing a second rule set.
            interaction_boundary = [
                *resolved.forbidden_terms,
                *interaction_boundary,
            ]
        if resolved.canonical_type == "headscarf":
            # Four compact slots are enough, but the physical AI-video risk
            # must not be pushed out by the three identity safeguards.
            interaction_boundary = [
                "禁止完整包裹、缠绕或复杂系结教程",
                *interaction_boundary,
            ]
        profile = {
            "display_family": definition["display_family"],
            "product_subtype": resolved.canonical_type,
            "wearing_zone": definition["wearing_zone"],
            "required_result_view": definition["required_result_view"],
            "supporting_style_context": definition["supporting_style_context"],
            "interaction_boundary": _dedupe(interaction_boundary, limit=4),
            "identity_priority": list(definition["identity_priority"]),
        }
        if definition.get("product_prominence"):
            profile["product_prominence"] = dict(
                definition["product_prominence"]
            )
        # Optional execution semantics are owned by each category profile.  The
        # previous scarf-only projection left hair accessories with scarf
        # defaults and kept wrist accessories outside the extension entirely.
        for key in ("process_policy", "capture_relationship"):
            if definition.get(key):
                profile[key] = definition[key]
        for key in (
            "preferred_carriers",
            "compatible_proof_subjects",
            "scene_preferences",
        ):
            if definition.get(key):
                profile[key] = list(definition[key])
        if definition.get("outfit_context"):
            profile["outfit_context"] = dict(definition["outfit_context"])
        if resolved.canonical_type == "earring":
            profile["identity_authority"] = _earring_pairing_authority(anchor_card)
        return {
            "domain": self.domain,
            "schema_version": (
                "accessory-execution-profile-v3-scarf"
                if resolved.canonical_type in _SCARF_TYPES
                else "accessory-execution-profile-v5-small-motion-return"
                if (
                    resolved.canonical_type in _SMALL_MOTION_TYPES
                    and _small_accessory_motion_enabled()
                )
                else "accessory-execution-profile-v4-small-prominence"
                if resolved.canonical_type in _SMALL_PROMINENCE_TYPES
                else "accessory-execution-profile-v2"
            ),
            "product_type_source": {
                "canonical_type": resolved.canonical_type,
                "canonical_family": resolved.canonical_family,
                "canonical_slot": resolved.canonical_slot,
                "display_type": resolved.display_type,
                "recognized_by_registry": True,
            },
            "profile": profile,
            "authority": {
                "carrier": "STRUCTURE_CONTRACT",
                "content": "CENTRAL_SELLING_ARGUMENT",
                "product_appearance": "REFERENCE_IMAGE_AND_APPROVED_ANCHORS",
                "physical_execution": "CATEGORY_EXTENSION",
            },
        }

    def resolve_carrier_execution(
        self,
        extension: Dict[str, Any],
        *,
        presentation_mode: str,
    ) -> Dict[str, Any]:
        profile = extension.get("profile") if isinstance(extension.get("profile"), dict) else {}
        # The frozen mixed-display contract, when present, owns the face policy
        # for the whole script.  Reading it here keeps every downstream framing
        # string consistent with the per-shot ``face_policy`` stamped onto the
        # capture units, including for plan packages frozen before this change.
        face_policy = mixed_contract_face_policy(extension)
        no_face = _is_no_face(face_policy)
        mode = _text(presentation_mode).upper()
        wearer = mode in {
            "PERSON_ON_CAMERA", "MIXED", "WEARER_ACTIVE", "WEARER_PASSIVE",
        }
        required_view = (
            _text(profile.get("required_result_view"))
            if wearer
            else "PRODUCT_DETAIL_ONLY"
        )
        product_subtype = _text(profile.get("product_subtype"))
        demonstration_mode = _text(profile.get("primary_demonstration_mode")).upper()
        if not demonstration_mode:
            if product_subtype == "headscarf":
                demonstration_mode = "HEAD_WORN"
            elif product_subtype in _HAIR_TYPES:
                demonstration_mode = "HAIR_WORN"
            elif product_subtype in _WRIST_TYPES:
                demonstration_mode = "WRIST_WORN"
            elif product_subtype in _FINGER_TYPES:
                demonstration_mode = "FINGER_WORN"
            elif product_subtype == "earring":
                demonstration_mode = "EAR_WORN"
            else:
                demonstration_mode = "NECK_WORN"
        demonstration_profile = _DEMONSTRATION_PROFILES.get(demonstration_mode, {})
        product_prominence = _resolve_product_prominence(
            profile, presentation_mode=mode, face_policy=face_policy
        )
        optional_interactions = (
            list(
                profile.get("optional_simple_interactions")
                or demonstration_profile.get("optional_simple_interactions")
                or []
            )
            if wearer
            else [
                "自然展开一次商品",
                "拿近手机看清图案后放回原位",
            ]
            if mode in {"HAND_ONLY", "HANDS_ONLY"}
            else []
        )
        if no_face:
            # These suggestions are authored prose.  Rewrite them first and then
            # drop anything that still names the face, so an unauthored entry
            # can never smuggle a half-face shot past the frozen contract.
            neutral = [_no_face_safe_text(value) for value in optional_interactions]
            optional_interactions = [
                value for value in neutral if value and not _has_face_term(value)
            ]
        carrier_execution = {
            "presentation_mode": mode,
            "wearing_zone": _text(profile.get("wearing_zone")),
            "required_view": required_view,
            "required_view_guidance_zh": _RESULT_LABELS.get(required_view, ""),
            "product_relation_zh": _RELATION_LABELS.get(
                _text(profile.get("wearing_zone")), ""
            ),
            "interaction_boundary": list(profile.get("interaction_boundary") or []),
            "identity_priority": list(profile.get("identity_priority") or []),
            "identity_authority": dict(profile.get("identity_authority") or {}),
            "process_policy": _text(profile.get("process_policy")),
            "preferred_carriers": list(profile.get("preferred_carriers") or []),
            "compatible_proof_subjects": list(
                profile.get("compatible_proof_subjects") or []
            ),
            "outfit_context": dict(profile.get("outfit_context") or {}),
            "scene_preferences": list(profile.get("scene_preferences") or []),
            "capture_relationship": _text(profile.get("capture_relationship")),
            "product_prominence_contract": product_prominence,
            "claim_boundary": (
                "允许展示并表达已经佩戴后的关系；不要求完整佩戴过程"
                if wearer
                else "只展示商品外观与结构；不把静物或手持画面写成佩戴结果证明"
            ),
            "primary_demonstration_mode": _text(
                profile.get("primary_demonstration_mode") or demonstration_mode
            ),
            "preferred_action_mode": _text(profile.get("preferred_action_mode")),
            "proof_action_intent": _text(profile.get("proof_action_intent")),
            "required_proof_relation": _text(
                profile.get("required_proof_relation")
            ),
            "demonstration_policy": _text(profile.get("demonstration_policy")),
            "optional_simple_interactions": optional_interactions,
            "interaction_capabilities": _interaction_capabilities(
                product_subtype=_text(profile.get("product_subtype")),
                demonstration_mode=demonstration_mode,
                presentation_mode=mode,
                preferred_action_mode=_text(profile.get("preferred_action_mode")),
                face_policy=face_policy,
            ),
        }
        if no_face:
            # Boundary guarantee: every code-owned field of this projection is
            # face-free, including ones a future author forgets to route
            # through ``_face_pick``.
            carrier_execution = _no_face_deep_clean(carrier_execution)
        return carrier_execution

    def resolve_argument_execution(
        self,
        extension: Dict[str, Any],
        *,
        selling_argument: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Freeze one visible usage for one short script.

        The operator argument may authorize several possible uses, but a
        15-second script should demonstrate one of them rather than becoming a
        neck/hair/bag checklist.  This is a contract conversion, not a new
        creative model call.
        """

        result = copy.deepcopy(extension)
        profile = result.get("profile") if isinstance(result.get("profile"), dict) else {}
        product_subtype = _text(profile.get("product_subtype"))
        if product_subtype in {*_WRIST_TYPES, *_FINGER_TYPES, *_HAIR_TYPES, "earring"}:
            preferred_action_mode = _text(
                selling_argument.get("preferred_action_mode")
            ).upper()
            if preferred_action_mode in {
                "RESULT_SHOW", "SIMPLE_WEAR_PROCESS", "ADJUST_THEN_WEAR",
                "HANDHELD_PRODUCT", "SCENE_USE",
            }:
                profile["preferred_action_mode"] = preferred_action_mode
                profile["argument_theme"] = _text(
                    selling_argument.get("argument_theme")
                )
                profile["proof_action_intent"] = _text(
                    selling_argument.get("proof_action_intent")
                )
                profile["required_proof_relation"] = _text(
                    selling_argument.get("required_proof_relation")
                )
                result["profile"] = profile
            return result
        if product_subtype not in _SCARF_TYPES:
            return result
        mode = _text(selling_argument.get("primary_demonstration_mode")).upper()
        if mode not in _DEMONSTRATION_PROFILES:
            mode = "HEAD_WORN" if _text(profile.get("product_subtype")) == "headscarf" else "NECK_WORN"
        profile.update(_DEMONSTRATION_PROFILES[mode])
        profile.update({
            "primary_demonstration_mode": mode,
            "supported_demonstration_modes": list(
                selling_argument.get("supported_demonstration_modes") or [mode]
            ),
            "demonstration_policy": "ONE_PRIMARY_MODE_PER_15S",
            "argument_theme": _text(selling_argument.get("argument_theme")),
            "evidence_mode": _text(selling_argument.get("evidence_mode")),
        })
        result["profile"] = profile
        result["schema_version"] = "accessory-execution-profile-v5-scarf-action"
        return result

    def build_blueprint_guidance(
        self,
        extension: Dict[str, Any],
        *,
        carrier_execution: Dict[str, Any],
    ) -> str:
        # Authored mixed-display template: it owns the physical choreography for
        # the whole clip (handheld + static + face-free worn modules), so the
        # legacy single-carrier advice below -- which assumes one framing for the
        # entire video, e.g. a half-face ear close-up -- must not be emitted.
        mixed_guidance = _mixed_template_blueprint_guidance(extension)
        if mixed_guidance:
            return mixed_guidance

        profile = extension.get("profile") if isinstance(extension.get("profile"), dict) else {}
        lines = [
            carrier_execution.get("product_relation_zh"),
            carrier_execution.get("required_view_guidance_zh"),
            carrier_execution.get("claim_boundary"),
        ]
        prominence = (
            carrier_execution.get("product_prominence_contract")
            if isinstance(
                carrier_execution.get("product_prominence_contract"), dict
            )
            else {}
        )
        if prominence:
            prominence_parts = [
                _text(prominence.get("opening_guidance")),
                _text(prominence.get("context_guidance")),
            ]
            terminal = (
                prominence.get("terminal_visibility")
                if isinstance(prominence.get("terminal_visibility"), dict)
                else {}
            )
            if _text(terminal.get("ending_guidance")):
                prominence_parts.append(_text(terminal.get("ending_guidance")))
            lines.append(
                "小商品观察尺度："
                + "；".join(part for part in prominence_parts if part)
                + "。这只调整兼容内容段的景别，不改变结构、佩戴状态或动作主线"
            )
        boundaries = _dedupe(carrier_execution.get("interaction_boundary") or [], limit=3)
        if boundaries:
            lines.append("避免：" + "；".join(boundaries))
        selected_action = (
            carrier_execution.get("selected_action_design")
            if isinstance(carrier_execution.get("selected_action_design"), dict)
            else {}
        )
        if selected_action:
            lines.append(
                "本条核心商品互动："
                + _text(selected_action.get("start_state"))
                + "；"
                + _text(selected_action.get("core_action"))
                + "；"
                + _text(selected_action.get("end_state"))
                + "。只执行这一段连续动作，不扩写成完整佩戴教程"
            )
        else:
            optional_interactions = _dedupe(
                carrier_execution.get("optional_simple_interactions") or [],
                limit=3,
            )
            if optional_interactions:
                lines.append(
                    "可选自然轻互动：最多自然采用其中一个，也可以不用；"
                    + "；".join(optional_interactions)
                    + "。这不是镜头清单，不得扩写成完整佩戴教程"
                )
        primary_mode = _text(profile.get("primary_demonstration_mode"))
        if primary_mode:
            lines.append(
                "本条15秒只执行一种主要展示方式："
                + primary_mode
                + "；其他可用方式不在画面中逐项枚举"
            )
        priorities = _identity_labels(profile.get("identity_priority") or [], limit=4)
        if priorities:
            lines.append("商品身份优先保持：" + "、".join(priorities))
        outfit = profile.get("outfit_context") if isinstance(profile.get("outfit_context"), dict) else {}
        if outfit:
            lines.append(
                "穿搭可见关系："
                + _text(outfit.get("visibility_requirement"))
            )
        capture_relationship = _text(profile.get("capture_relationship"))
        if capture_relationship:
            lines.append("拍摄位置关系：" + capture_relationship)
        identity_authority = (
            profile.get("identity_authority")
            if isinstance(profile.get("identity_authority"), dict)
            else {}
        )
        if identity_authority:
            lines.append(_pairing_guidance(identity_authority))
        return "\n".join(_dedupe(lines, limit=10 if prominence else 8))

    def build_video_brief(
        self,
        extension: Dict[str, Any],
        *,
        carrier_execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        profile = extension.get("profile") if isinstance(extension.get("profile"), dict) else {}
        selected_action = dict(
            carrier_execution.get("selected_action_design") or {}
        )
        wear_state = _wear_state_contract(selected_action)
        required_result = _text(carrier_execution.get("required_view_guidance_zh"))
        if wear_state.get("initial_state") == "IN_PROGRESS" and required_result:
            required_result = "完成上述简单动作后，" + re.sub(
                r"^从已经[^，。；]*[，。；]?",
                "",
                required_result,
            )
        return {
            "schema_version": (
                "accessory-video-handoff-v5-state-aware"
                if _text(profile.get("product_subtype")) in _SCARF_TYPES
                else "accessory-video-handoff-v5-small-motion-return"
                if (
                    _text(profile.get("product_subtype")) in _SMALL_MOTION_TYPES
                    and _small_accessory_motion_enabled()
                )
                else "accessory-video-handoff-v4-small-prominence"
                if _text(profile.get("product_subtype")) in _SMALL_PROMINENCE_TYPES
                else "accessory-video-handoff-v2"
            ),
            "product_relation": _text(carrier_execution.get("product_relation_zh")),
            "required_visible_result": required_result,
            "interaction_limit": _dedupe(
                carrier_execution.get("interaction_boundary") or [], limit=3
            ),
            "identity_focus": _identity_labels(
                profile.get("identity_priority") or [], limit=4
            ),
            "claim_boundary": _text(carrier_execution.get("claim_boundary")),
            "identity_authority": dict(profile.get("identity_authority") or {}),
            "identity_authority_guidance": (
                _pairing_guidance(profile.get("identity_authority") or {})
                if profile.get("identity_authority")
                else ""
            ),
            "outfit_context": dict(profile.get("outfit_context") or {}),
            "capture_relationship": _text(
                carrier_execution.get("capture_relationship")
                or profile.get("capture_relationship")
            ),
            "product_prominence_contract": dict(
                carrier_execution.get("product_prominence_contract") or {}
            ),
            "process_policy": _text(profile.get("process_policy")),
            "optional_simple_interactions": _dedupe(
                carrier_execution.get("optional_simple_interactions") or [],
                limit=3,
            ),
            "selected_action_design": selected_action,
            "wear_state_contract": wear_state,
            "hand_anatomy_guard": _hand_anatomy_guard(carrier_execution),
        }

    def project_capture_rhythm(
        self,
        extension: Dict[str, Any],
        *,
        carrier_execution: Dict[str, Any],
        capture_contract: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Add category filming relations without touching structure order.

        The shared compiler continues to own visible-clip count, macro beats,
        edit boundaries and phone feasibility.  This projection only supplies
        physical category roles and per-clip framing.  It is deliberately
        impossible for women's apparel to enter because no ACCESSORY extension
        exists for apparel.
        """

        result = copy.deepcopy(capture_contract)
        profile = (
            extension.get("profile")
            if isinstance(extension.get("profile"), dict)
            else {}
        )
        product_subtype = _text(profile.get("product_subtype"))
        if product_subtype in _SCARF_TYPES:
            env_name = SCARF_MULTICLIP_ENV
            rollout_profile = SCARF_MULTICLIP_PROFILE
            preferred_visible_clips = 4
        elif product_subtype in _SMALL_PROMINENCE_TYPES:
            env_name = SMALL_ACCESSORY_MULTICLIP_ENV
            rollout_profile = SMALL_ACCESSORY_MULTICLIP_PROFILE
            preferred_visible_clips = 3
        else:
            return result
        if not _category_multiclip_enabled(env_name):
            return result
        if _text(result.get("profile")).upper() == "LEGACY_ONE_TAKE":
            return result

        unit_count = int(result.get("capture_unit_count") or 0)
        if unit_count < 3:
            # The category layer cannot invent cuts when the public compiler
            # has explicitly selected a legacy/insufficient contract.
            return result
        selected_action = (
            carrier_execution.get("selected_action_design")
            if isinstance(
                carrier_execution.get("selected_action_design"), dict
            )
            else {}
        )
        action_mode = _text(
            selected_action.get("primary_action_mode")
            or carrier_execution.get("preferred_action_mode")
        ).upper()
        prominence = (
            carrier_execution.get("product_prominence_contract")
            if isinstance(
                carrier_execution.get("product_prominence_contract"), dict
            )
            else {}
        )
        motion_return = (
            product_subtype in _SMALL_PROMINENCE_TYPES
            and _text(prominence.get("sequence_policy")).upper()
            == "PRODUCT_OPENING_TO_MOTION_TO_PRODUCT_RETURN"
        )
        if motion_return:
            terminal = (
                prominence.get("terminal_visibility")
                if isinstance(prominence.get("terminal_visibility"), dict)
                else {}
            )
            category_roles, framing = _small_accessory_motion_capture(
                unit_count=unit_count,
                terminal_guidance=_text(terminal.get("ending_guidance")),
            )
        else:
            category_roles = _category_capture_roles(
                product_subtype=product_subtype,
                unit_count=unit_count,
                action_mode=action_mode,
            )
            framing = _category_capture_framing(
                product_subtype=product_subtype,
                unit_count=unit_count,
                action_mode=action_mode,
            )
        result.update({
            "category_projection": rollout_profile,
            "category_unit_roles": category_roles,
            # ``unit_roles`` owns only the physical display relation.  The
            # shared ``structure_unit_roles`` remains untouched and continues
            # to own HOOK/PROOF/USE/ENDING order.
            "unit_roles": category_roles,
            "framing_guidance_by_unit": framing,
            "category_rollout_contract": {
                "profile": rollout_profile,
                "product_subtype": product_subtype,
                "policy_version": "category-multiclip-rollout-v1",
                "preferred_visible_clips": preferred_visible_clips,
                "planned_visible_clips": unit_count,
                "structure_order_unchanged": True,
                "female_apparel_unchanged": True,
                "hard_required": False,
                "may_trigger_retry": False,
                "rollback_env": env_name,
            },
        })
        if motion_return:
            result["capture_grammar"] = (
                "PRODUCT_OPENING_TO_MOTION_TO_PRODUCT_RETURN"
            )
        richness = (
            dict(result.get("shot_richness_contract"))
            if isinstance(result.get("shot_richness_contract"), dict)
            else {}
        )
        richness.update({
            "category_profile": rollout_profile,
            "category_preferred_visible_clips": preferred_visible_clips,
            "category_planned_visible_clips": unit_count,
            "category_difference_policy": (
                "SCARF_RESULT_DETAIL_RELATION_WITHOUT_STATE_REGRESSION"
                if product_subtype in _SCARF_TYPES
                else "SMALL_PRODUCT_OPEN_MOTION_RETURN"
                if motion_return
                else "SMALL_PRODUCT_CLOSE_RELATION_VISIBLE_RESULT"
            ),
        })
        result["shot_richness_contract"] = richness
        return result

    def validate_identity(
        self,
        extension: Dict[str, Any],
        *,
        script: Dict[str, Any],
    ) -> List[str]:
        profile = (
            extension.get("profile")
            if isinstance(extension.get("profile"), dict)
            else {}
        )
        if _text(profile.get("product_subtype")) != "earring":
            return []
        authority = (
            profile.get("identity_authority")
            if isinstance(profile.get("identity_authority"), dict)
            else {}
        )
        mode = _text(authority.get("pairing_mode")).upper() or "UNAVAILABLE"

        # Check authored content only.  Do not scan the execution brief because
        # its own guard sentence intentionally contains the forbidden terms.
        concept = script.get("script_concept") or {}
        raw_production = script.get("production_design") or {}
        production = {
            key: value
            for key, value in raw_production.items()
            if key != "accessory_execution"
        }
        usage = script.get("product_usage") or {}
        voice = script.get("continuous_voiceover") or {}
        content_scope = {
            "script_concept": concept,
            "production_design": production,
            "product_usage": usage,
            "storyboard": script.get("storyboard") or [],
            "voiceover_translation": voice.get("chinese_translation"),
            "voiceover_realization_zh": voice.get("selling_argument_realization_zh"),
        }
        content = json.dumps(content_scope, ensure_ascii=False)
        pair_hits = [term for term in _PAIR_OUTPUT_TERMS if term in content]
        single_hits = [term for term in _SINGLE_OUTPUT_TERMS if term in content]
        if mode == "UNAVAILABLE" and (pair_hits or single_hits):
            return [
                "PRODUCT_IDENTITY_CONFLICT：授权锚点未说明耳饰单双关系，"
                "脚本不得推断=" + "、".join(_dedupe([*pair_hits, *single_hits]))
            ]
        if mode == "PAIR" and single_hits:
            return [
                "PRODUCT_IDENTITY_CONFLICT：授权为成对耳饰，脚本却写成单只="
                + "、".join(_dedupe(single_hits))
            ]
        if mode == "SINGLE" and pair_hits:
            return [
                "PRODUCT_IDENTITY_CONFLICT：授权为单只耳饰，脚本却写成成对="
                + "、".join(_dedupe(pair_hits))
            ]
        return []
