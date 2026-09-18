"""Compact Feishu workbench adapter for OPV image-story production.

Feishu is the operator surface; RDS remains the source of truth.  The adapter
uses the Feishu record id as the external idempotency namespace and never
publishes content.
"""

from __future__ import annotations

import hashlib
import json
import mimetypes
import os
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from domain.models import ProductionBatch
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from services.asset_resolver import LightTryonAssetReader
from services.batch_diversity_planner import BatchDiversityPlanner
from services.content_story import generate_product_image_story
from services.hero_first import HeroFirstProducer
from services.image_generator import build_default_photo_generator
from services.photo_wig_flow import (
    MX_WIG_CHOICE_FLOW,
    batch_is_mx_wig_choice,
    recipe_is_mx_wig_choice,
)
from services.product_reference_resolver import (
    ProductReferenceResolutionError,
    ProductReferenceResolver,
)
from services.video_render_flow import VideoRenderFlow
from services.video_renderer import FFmpegStillRenderer
from services.feishu_v2 import (
    FeishuV2Mixin, FIELD_REVIEW_MODE, FIELD_REVIEW_STAGE, FIELD_RETRY_REVIEW,
    FIELD_REVIEW_TOKEN, MODE_AUTO, MODE_HUMAN,
)
from services.workflow_v2 import workflow_v2_enabled
from services.production_batch import BatchLeaseBusy, ProjectionPendingError


SOURCE_TYPE = "feishu_opv"
FIELD_PRODUCT = "产品编码"
FIELD_STORE = "店铺"
FIELD_PRESET = "生产预设"
FIELD_EXECUTE = "执行"
FIELD_PROGRESS = "进度"
FIELD_OUTPUT = "预览/成片"
FIELD_REVIEW = "审核"
FIELD_NOTES = "备注"
FIELD_QUANTITY = "生成篇数"
FIELD_QUANTITY_LEGACY = "生成数量"
FIELD_CONFIRM_PUBLISH = "确认发布"
FIELD_PHOTO_REQUEST = "图文任务JSON"  # optional legacy override; never required
FIELD_PHOTO_SUMMARY = "内容方案摘要"
FIELD_FULL_COPY_ZH = "完整文案（中文）"  # 最终成片文案的中文全文（按套、按页）
FIELD_PHOTO_INPUT = "完整穿搭素材（可选）"
FIELD_PHOTO_INPUT_LEGACY = "图文参考图"
FIELD_PRODUCT_REFERENCE = "商品参考图（可选）"
FIELD_REFERENCE = "参考图（可选）"
FIELD_REFERENCE_TYPE = "参考图类型"
FIELD_CONTENT_THEME = "图文主题"
FIELD_CONTENT_REQUIREMENT = "内容要求（可选）"
FIELD_TARGET_ACCOUNT = "目标账号（可选）"
FIELD_VISUAL_PRESET = "视觉预设（可选）"
FIELD_TRAVEL_PLACE = "旅行地点（可选）"
FIELD_TRAVEL_COUNTRY = "旅行国家"
# 自动供稿来源标记（与 services/auto_photo_supply.py 同值）：外部 reference_only
# 行必须有执行合同，付费前反查（Phase 1 来源分流）。
FIELD_SOURCE_TAG = "来源标记"
FIELD_TEMPERATURE_BAND = "温度档"
FIELD_THERMAL_SENSITIVITY = "体感倾向"
FIELD_TEMPERATURE_SCENE = "温度穿搭场景"
FIELD_TRANSITION_SCENE = "冷热切换场景"
FIELD_TRANSITION_SENSITIVITY = "体感"
FIELD_DRESS_CODE = "着装要求"
FIELD_LAYER_BASE_REFERENCE = "基础层图"
FIELD_LAYER_MID_REFERENCE = "中间层图"
FIELD_LAYER_OUTER_REFERENCE = "外层图"
FIELD_FAILURE_REASON = "图文生成失败的原因"
FIELD_RETAKE_LOOK = "重拍 Look（可选）"
FIELD_MUSIC_MODE = "配乐方式"
FIELD_PHOTO_ASSET_STATUS = "素材状态"

TEMPERATURE_BAND_OPTIONS = ("15°C 左右", "10°C 左右", "5°C 左右", "0°C 左右")
THERMAL_SENSITIVITY_OPTIONS = ("怕冷", "正常体感", "怕热")
TEMPERATURE_SCENE_OPTIONS = ("通勤", "室内", "户外")
# Daily hot→cold transition line.  These tuples are the single source of truth
# for both the shipped Bitable schema and ``resolve_thermal_transition_variables``
# — the accepted operator values must never drift from the column options.
TRANSITION_SCENE_OPTIONS = (
    "室外热→BTS→办公室空调", "室外热→商场→影院", "校园室外→教室",
)
DRESS_CODE_OPTIONS = ("办公室", "校园", "周末")

PROGRESS_PENDING = "待执行"
PROGRESS_RUNNING = "生成中"
PROGRESS_PLANNING = "规划中"
PROGRESS_PREPARING_ASSETS = "准备素材"
PROGRESS_QA = "质检中"
PROGRESS_REPAIR = "待修复"
PROGRESS_RETRYABLE = "失败可重试"
PROGRESS_REVIEW = "待审核"
PROGRESS_DONE = "已完成"
PROGRESS_ACTION = "需处理"
PROGRESS_QUEUED = "待排班"
PROGRESS_SCHEDULED = "已排期"
PROGRESS_SUBMITTING = "提交中"
PROGRESS_PUBLISHING = "发布中"
PROGRESS_PUBLISHED = "已发布"
PROGRESS_PUBLISH_FAILED = "发布失败"

# 主发布队列表示「已占用、不可覆盖」的状态。投影与返工守卫必须共用此集合：
# 遗漏任一状态都会把在途任务误判成「未排班」（曾把「提交中」显示成「待排班」）。
MAIN_QUEUE_SUBMITTING_STATUSES = {PROGRESS_SUBMITTING, "提交结果不明"}
MAIN_QUEUE_OCCUPIED_STATUSES = {
    "待排期", PROGRESS_QUEUED, *MAIN_QUEUE_SUBMITTING_STATUSES,
    PROGRESS_SCHEDULED, PROGRESS_PUBLISHING, PROGRESS_PUBLISHED,
}

IN_FLIGHT_PROGRESS = {
    PROGRESS_RUNNING, PROGRESS_PLANNING, PROGRESS_PREPARING_ASSETS,
    PROGRESS_QA, PROGRESS_REPAIR,
}

REVIEW_PENDING = "待审核"
REVIEW_APPROVED = "通过"
REVIEW_NOT_REQUIRED = "无需审核"
REVIEW_REDO_ALL = "整组重做"
REVIEW_SCHEDULE = "排期发布"

# 工作台兜底异常处理器把这类异常显式标注成「系统内部错误」。它们几乎都代表代码缺陷
# 而不是业务拒绝，且 str(exc) 常常只是一个裸 repr —— 例如 KeyError 只会给出
# 'source_record_id'，运营看到这一行无法判断是业务拦截、缺填字段还是系统故障
# （真实案例：recvuMrl4BEn0U 的失败原因整列就是这个字符串）。
# 有业务语义的拒绝请抛带人话消息的领域异常（如 FeishuWorkflowError），不要落进这里。
OPAQUE_INTERNAL_ERRORS = (KeyError, TypeError, AttributeError, IndexError, AssertionError)


def parse_retake_roles(raw: Any) -> list[str]:
    """解析“重拍 Look（可选）”列：C / C,D / look_c 均可，中英逗号均可；非法 token 忽略。"""
    tokens = str(raw or "").replace("，", ",").replace("、", ",").split(",")
    roles = set()
    for token in tokens:
        name = token.strip().lower()
        if name in ("a", "b", "c", "d"):
            roles.add("look_" + name)
        elif name in ("look_a", "look_b", "look_c", "look_d"):
            roles.add(name)
    return sorted(roles)


class FeishuWorkflowError(RuntimeError):
    pass


def resolve_temperature_variables(fields: Mapping[str, Any]) -> dict[str, str]:
    band = text_value(fields.get(FIELD_TEMPERATURE_BAND))
    sensitivity = text_value(fields.get(FIELD_THERMAL_SENSITIVITY))
    scene = text_value(fields.get(FIELD_TEMPERATURE_SCENE))
    band_key = {
        "15°C 左右": "t15", "15°C": "t15", "t15": "t15",
        "10°C 左右": "t10", "10°C": "t10", "t10": "t10",
        "5°C 左右": "t5", "5°C": "t5", "t5": "t5",
        "0°C 左右": "t0", "0°C": "t0", "t0": "t0",
    }.get(band, "")
    sensitivity_key = {
        "怕冷": "feels_cold", "feels_cold": "feels_cold",
        "正常体感": "normal", "normal": "normal",
        "怕热": "feels_warm", "feels_warm": "feels_warm",
    }.get(sensitivity, "")
    scene_key = {
        "通勤": "Commute", "Commute": "Commute",
        "室内": "Indoor", "Indoor": "Indoor",
        "户外": "Outdoor", "Outdoor": "Outdoor",
    }.get(scene, "")
    missing = [label for label, value in (
        (FIELD_TEMPERATURE_BAND, band_key),
        (FIELD_THERMAL_SENSITIVITY, sensitivity_key),
        (FIELD_TEMPERATURE_SCENE, scene_key),
    ) if not value]
    if missing:
        raise FeishuWorkflowError("温度穿搭必须填写：" + "、".join(missing))
    return {
        "band_key": band_key,
        "thermal_sensitivity": sensitivity_key,
        "scene": scene_key,
        "style_series": "minimal_city",
    }


TRANSITION_SCENE_KEYS = {
    "室外热→BTS→办公室空调": "outdoor_bts_office",
    "outdoor_bts_office": "outdoor_bts_office",
    "室外热→商场→影院": "outdoor_mall_cinema",
    "outdoor_mall_cinema": "outdoor_mall_cinema",
    "校园室外→教室": "campus_outdoor_classroom",
    "campus_outdoor_classroom": "campus_outdoor_classroom",
}
DRESS_CODE_KEYS = {
    "办公室": "office", "office": "office",
    "校园": "campus", "campus": "campus",
    "周末": "weekend", "weekend": "weekend",
}

# The shipped column options and the accepted operator values are two views of
# one list.  Drift would let an operator pick an option the resolver rejects,
# so it is checked at import time instead of at 3am in production.
assert set(TRANSITION_SCENE_OPTIONS) <= set(TRANSITION_SCENE_KEYS), (
    "冷热切换场景字段选项与解析映射不一致"
)
assert set(DRESS_CODE_OPTIONS) <= set(DRESS_CODE_KEYS), (
    "着装要求字段选项与解析映射不一致"
)
assert set(THERMAL_SENSITIVITY_OPTIONS) <= {
    "怕冷", "正常体感", "怕热",
}, "体感字段选项与解析映射不一致"

# Travel temperature enhancement.  ``thermal_sensitivity`` is an *optional*
# variable on the travel recipe and only the TEMPERATURE travel theme consumes
# it: the operator picks it through the existing 体感倾向 column, and an empty
# column falls back to the default declared by the recipe's variables_schema.
# Every other travel theme never reads this field, so their planning prompt
# stays byte-identical (see ``build_travel_topic``).
TRAVEL_THERMAL_THEME_TYPE = "TEMPERATURE"
TRAVEL_THERMAL_SENSITIVITY_VARIABLE = "thermal_sensitivity"
TRAVEL_THERMAL_SENSITIVITY_LABELS = {
    "怕冷": "feels_cold", "feels_cold": "feels_cold",
    "正常体感": "normal", "normal": "normal",
    "怕热": "feels_warm", "feels_warm": "feels_warm",
}
TRAVEL_THERMAL_SENSITIVITY_FALLBACK = "normal"
assert set(TRAVEL_THERMAL_SENSITIVITY_LABELS) >= set(
    THERMAL_SENSITIVITY_OPTIONS
), "体感倾向字段选项必须都能解析为旅行体感枚举"


def resolve_thermal_transition_variables(fields: Mapping[str, Any]) -> dict[str, str]:
    """Freeze one daily hot→cold transition profile from operator fields."""
    scene = text_value(fields.get(FIELD_TRANSITION_SCENE))
    sensitivity = text_value(fields.get(FIELD_TRANSITION_SENSITIVITY))
    dress_code = text_value(fields.get(FIELD_DRESS_CODE))
    scene_key = TRANSITION_SCENE_KEYS.get(scene, "")
    sensitivity_key = {
        "怕冷": "feels_cold", "feels_cold": "feels_cold",
        "正常体感": "normal", "normal": "normal",
        "怕热": "feels_warm", "feels_warm": "feels_warm",
    }.get(sensitivity, "")
    dress_code_key = DRESS_CODE_KEYS.get(dress_code, "")
    missing = [label for label, value in (
        (FIELD_TRANSITION_SCENE, scene_key),
        (FIELD_TRANSITION_SENSITIVITY, sensitivity_key),
        (FIELD_DRESS_CODE, dress_code_key),
    ) if not value]
    if missing:
        raise FeishuWorkflowError("冷热切换必须填写：" + "、".join(missing))
    return {
        "transition_key": scene_key,
        "thermal_sensitivity": sensitivity_key,
        "dress_code": dress_code_key,
        "style_series": "minimal_city",
        "temperature_label_mode": "QUALITATIVE",
    }


def resolve_travel_thermal_sensitivity(
    fields: Mapping[str, Any],
    variables_schema: Mapping[str, Any] | None = None,
) -> str:
    """Resolve the optional travel ``thermal_sensitivity`` for one task.

    The allowed values and the default both come from the recipe's own
    ``variables_schema`` so the operator surface, the recipe contract and the
    planner can never drift apart.  An empty column means "operator did not
    specify", which must behave like the legacy tasks: default to ``normal``.
    """
    rule = dict((variables_schema or {}).get(
        TRAVEL_THERMAL_SENSITIVITY_VARIABLE) or {})
    allowed = [str(value) for value in rule.get("values") or []]
    default = str(rule.get("default") or TRAVEL_THERMAL_SENSITIVITY_FALLBACK)
    raw = text_value(fields.get(FIELD_THERMAL_SENSITIVITY))
    if not raw:
        return default
    resolved = TRAVEL_THERMAL_SENSITIVITY_LABELS.get(raw, "")
    if not resolved or (allowed and resolved not in allowed):
        raise FeishuWorkflowError(
            f"{FIELD_THERMAL_SENSITIVITY}只能填写：怕冷、正常体感、怕热"
        )
    return resolved


#: 常见旅行地点 → 国家 的最小映射与统一解析（Phase 3）见 photo_travel_qa。
from services.photo_travel_qa import resolve_travel_destination  # noqa: E402


def _external_contract_store():
    """外部供稿合同存储（进程级缓存；与消费侧台账同库）。"""
    global _EXTERNAL_CONTRACT_STORE
    if _EXTERNAL_CONTRACT_STORE is None:
        from services.external_supply_contract import ExternalSupplyContractStore
        _EXTERNAL_CONTRACT_STORE = ExternalSupplyContractStore()
    return _EXTERNAL_CONTRACT_STORE


_EXTERNAL_CONTRACT_STORE = None


def assert_external_supply_wiring(
    *, source_tag: str, record_id: str, reference_mode: str,
    product_id: str, store: Any = None,
) -> "Optional[dict]":
    """外部自动供稿行的付费前门禁（Phase 1 来源分流）。

    - 来源标记是 auto_supply 的行必须有执行合同（record_id 反查）；
    - authorization 必须是 reference_only；
    - 禁止 COMPLETE_LOOK（原图直用成片底图）；
    - 行产品编码与合同一致。
    返回合同 dict；非外部行返回 None。任何违规抛 FeishuWorkflowError。
    """
    if not str(source_tag or "").startswith("auto_supply|"):
        return None
    from services.photo_reference import REFERENCE_MODE_COMPLETE_LOOK
    contract = None
    try:
        contract = (store or _external_contract_store()).find_by_record(record_id)
    except Exception:  # noqa: BLE001 - 合同库不可读也必须中止，不能放行
        contract = None
    if contract is None:
        raise FeishuWorkflowError(
            "AUTO_SUPPLY_WIRING：自动外部供稿行缺少执行合同"
            "（external_supply_contracts 无此 record_id），付费前中止；"
            "请通过自动供稿流程重建该任务，勿手工复用旧行")
    if str(contract.get("authorization") or "") != "reference_only":
        raise FeishuWorkflowError(
            "AUTO_SUPPLY_WIRING：外部供稿合同授权异常（"
            f"{contract.get('authorization')!r}）")
    if reference_mode == REFERENCE_MODE_COMPLETE_LOOK:
        raise FeishuWorkflowError(
            "AUTO_SUPPLY_WIRING：外部 reference_only 参考禁止进入"
            "完整穿搭（COMPLETE_LOOK）原图直用路径")
    contract_product = str(((contract.get("product") or {}).get("code")) or "")
    if contract_product and product_id and contract_product != product_id:
        raise FeishuWorkflowError(
            "AUTO_SUPPLY_WIRING：行产品编码与执行合同不一致"
            f"（{product_id} ≠ {contract_product}）")
    return contract


def build_travel_topic(
    *, theme: Mapping[str, Any], travel_place: str,
    travel_variables: Mapping[str, Any], content_requirement: str = "",
    fields: Mapping[str, Any] | None = None,
    recipe_spec: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Freeze the topic-linked travel brief for one task.

    Only the TEMPERATURE theme adds the resolved ``thermal_sensitivity`` and the
    theme's own adjustment guidance.  All five other travel themes return the
    exact brief they returned before, so their planning prompt cannot change.
    """
    if not travel_place:
        raise FeishuWorkflowError(
            "当前选择的是具体旅行主题，请填写旅行地点；"
            "如不需要具体目的地，请改选“凉爽旅行”"
        )
    theme_type = str(theme.get("travel_theme_type") or "")
    band = str(travel_variables.get("temperature_band") or "")
    topic: dict[str, Any] = {
        "theme_type": theme_type,
        "theme_version": int(theme.get("travel_theme_version") or 1),
        "theme_label_zh": str(theme.get("label_zh") or ""),
        "planning_focus": str(theme.get("visual_brief") or ""),
        "topic_patterns": list(theme.get("topic_patterns") or []),
        "body_copy_focus": str(theme.get("body_copy_focus") or ""),
        "cta_patterns": list(theme.get("cta_patterns") or []),
        "place": travel_place,
        "temperature_band": band,
        "temperature_context": {"value": band, "source": "execution_profile"},
        "thai_fallback": {
            "title": str(theme.get("title") or ""),
            "cover": str(theme.get("cover") or ""),
            "caption": str(theme.get("caption") or ""),
            "hashtags": list(theme.get("hashtags") or []),
            "cta": str(theme.get("cta") or ""),
        },
        "content_requirement": content_requirement,
    }
    if theme_type == TRAVEL_THERMAL_THEME_TYPE:
        topic[TRAVEL_THERMAL_SENSITIVITY_VARIABLE] = (
            resolve_travel_thermal_sensitivity(
                fields or {},
                (recipe_spec or {}).get("variables_schema") or {},
            )
        )
        topic["thermal_sensitivity_planning"] = dict(
            theme.get("thermal_sensitivity_planning") or {}
        )
    return topic


def layering_approval_attributes(qa: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Translate deterministic layering observations into frozen source evidence."""
    output: dict[str, dict[str, Any]] = {}
    for page in qa.get("roles") or []:
        role = str(page.get("role") or "")
        if not role:
            continue
        stack = [str(value) for value in page.get("visible_layer_stack") or []]
        output[role] = {
            "identity_id": str(page.get("identity_id") or ""),
            "camera_signature": str(page.get("camera_signature") or ""),
            # The content-card contract compares a stable scale enum, while
            # semantic QA keeps the more precise camera signature separately.
            "camera_scale": "FULL_BODY",
            "full_body": page.get("full_body") is True,
            "upper_layers": stack,
            "visible_layer_count": int(page.get("visible_layer_count") or 0),
            "observed_item_types": list(page.get("observed_item_types") or []),
            "stackability": dict(page.get("stackability") or {}),
        }
    return output


class _RecordFlock:
    """Non-blocking per-record advisory lock (kernel-owned, death-safe).

    Scanner slots run in parallel; this guarantees two workers never process
    the same row. ``with _RecordFlock(rid) as locked:`` — locked is False when
    another worker owns the record.
    """

    def __init__(self, record_id: str, directory: str = "/tmp"):
        import fcntl

        self._fcntl = fcntl
        self.path = f"{directory}/opv_record_{record_id}.flock"
        self.fd: Optional[int] = None

    def __enter__(self) -> bool:
        self.fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            self._fcntl.flock(self.fd, self._fcntl.LOCK_EX | self._fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(self.fd)
            self.fd = None
        return self.fd is not None

    def __exit__(self, *args) -> bool:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        return False


def dependent_redo_slots(plan: Dict[str, Any], requested_slots: Iterable[int]) -> List[int]:
    """Expand redo scope when a derived board's continuity anchor changes."""
    requested = sorted({int(value) for value in requested_slots})
    execution = plan.get("recipe_execution") or {}
    anchor = int(plan.get("anchor_slot") or 1)
    if execution.get("content_goal") == "outfit_breakdown" and anchor in requested:
        return list(range(1, 6))
    return requested


def text_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                parts.append(str(item.get("text") or item.get("name") or ""))
            else:
                parts.append(str(item))
        return "".join(parts).strip()
    return str(value).strip()


def task_quantity(fields: Mapping[str, Any]) -> int:
    """Prefer the operator-friendly name while keeping historical rows readable."""
    value = fields.get(FIELD_QUANTITY)
    if value in (None, ""):
        value = fields.get(FIELD_QUANTITY_LEGACY)
    return quantity_value(value)


# 素材状态把行标成「已确认/已匹配」时，首跑路径会跳过素材段、直接复用已冻结的
# 素材集。可复用的前提是**真的有一个 asset_set_id 可用**：要么来自冻结批次
# （此时根本不走首跑路径），要么来自「图文任务JSON」里随人工确认写入的 pin。
# 两者都没有却仍然跳过，只会得到空 overrides ⇒ 落到内容池回退路径
# ⇒ ``NEEDS_CONTENT: 仅找到 0 份未占用的有效内容``。
# 真实成因见 2026-09-14 恢复死角排查：素材审核确认把 素材状态 写进飞书，
# 却只把 asset_set_id 留在内存里，那次 _generate 一失败，该行就再也跑不起来。
SETTLED_ASSET_STATUSES = ("已确认，正在生成", "已匹配可用素材")


def confirmed_asset_set_pins(fields: Mapping[str, Any]) -> list[str]:
    """「图文任务JSON」里随人工确认冻结下来的 asset_set_id pin（可多个）。"""
    raw = text_value((fields or {}).get(FIELD_PHOTO_REQUEST))
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, dict):
        return []
    items = payload.get("items") if "items" in payload else [payload]
    if not isinstance(items, list):
        return []
    return [str(item["asset_set_id"]) for item in items
            if isinstance(item, dict) and item.get("asset_set_id")]


def asset_supply_must_run(asset_status: str, fields: Mapping[str, Any]) -> bool:
    """首跑路径是否必须（重新）派生素材集，而不是直接复用。"""
    return (asset_status not in SETTLED_ASSET_STATUSES
            or not confirmed_asset_set_pins(fields))


def retake_theme(table_theme: Optional[Mapping[str, Any]],
                 supply_manifest: Mapping[str, Any]) -> Optional[dict[str, Any]]:
    """重拍要用的主题：优先沿用首跑冻结的那一个。

    首跑的无主题 STYLE 行用的是 ``effective_theme``（中性主题），表格里仍是空的。
    重拍若重读表格就会拿到 ``None``，而 ``_input_hash`` 会执行 ``dict(theme)``
    ⇒ ``TypeError: 'NoneType' object is not iterable``（2026-09-14 复现）。

    恢复源是**供给清单里的 ``theme_brief``**：``PhotoStyleReferenceSupplyService._save``
    往那里写的是首跑传入的那个纯主题对象（``dict(theme)``），也正是一次
    ``_input_hash`` 的输入。冻结请求里的 ``theme_brief`` 是另一个东西——它包了
    ``reference_mode`` / ``batch_variation`` / ``variation``，拿它当主题参与原
    hash 只会永远对不上，因此这里刻意不读请求。

    表格填了真实不同的主题时不覆盖：交给既有身份检查照原样拒绝，避免用「消除
    异常」的名义放过真的换题。
    """
    frozen = (supply_manifest or {}).get("theme_brief")
    if not isinstance(frozen, Mapping) or not frozen:
        # 旧清单没有该字段：沿用历史兼容路径（表格值直通）。
        return dict(table_theme) if isinstance(table_theme, Mapping) else None
    if table_theme is None:
        return dict(frozen)
    if dict(table_theme) == dict(frozen):
        return dict(frozen)
    # 运营真的改了主题：把表格值原样交给身份检查，它会报「主题与原生成不一致」。
    return dict(table_theme)


def theme_is_optional(*, locale_pack: Optional[Mapping[str, Any]],
                      layering_flow: bool, thermal_transition_flow: bool,
                      planning_flow: str) -> bool:
    """本流程是否允许运营不选「图文主题」（主题缺省规则，唯一归属）。

    只有「发布文案已经不依赖主题」的流程才豁免：声明了 ``locale_copy_packs`` 的
    国家无关配方由 Locale Pack 拥有发布文案与家族文案，主题只影响家族排序；而
    VN 围巾线的运营输入本来就只有「参考图类型 / 参考图 / 产品编码」三项，没有
    「图文主题」。v1 配方（靠 theme 携带内联泰语文案）与旅行 / 分层（温度分层、
    冷热切换）流程保持原样拦截。

    2026-09-14 起从这里统一回答，工作流与入口清单共用同一实现，避免出现第二份
    「哪些线可以空主题」的口径。
    """
    return bool(
        locale_pack is not None
        and not layering_flow
        and not thermal_transition_flow
        and planning_flow != "travel_two_step"
    )


def is_topic_travel_recipe(recipe_id: str, repository: Any = None) -> bool:
    """Whether this Recipe publishes topic-linked travel copy.

    Phase 2 made travel Recipes country-agnostic, so the old ``PHOTO_TH_TRAVEL``
    prefix stopped identifying them: a VN travel Recipe was silently skipped by
    the publishing gate (review P1-6).  The predicate is the contract itself,
    with the legacy prefix retained so historical rows keep exact behaviour and
    a stub without a repository still resolves.
    """
    if not recipe_id:
        return False
    if recipe_id.startswith("PHOTO_TH_TRAVEL"):
        return True
    if repository is None:
        return False
    recipe = repository.get_content_recipe(recipe_id)
    spec = dict(getattr(recipe, "recipe_spec_json", None) or {})
    if spec.get("travel_contract"):
        return True
    from services.photo_content_planner import get_planning_flow
    return get_planning_flow(recipe_id) == "travel_two_step"


@dataclass(frozen=True)
class PresetTask:
    account_id: str
    market: str
    language: str
    recipe_id: str
    theme_id: str
    hook_strategy: str
    persona_ref: str
    look_ref: str
    scene_ref: str


#: 目录/视图用的展示分组。**只控制目录与视图**，不改变启用语义——启用与否仍由
#: 预设的 ``status`` 决定（见 ``ProductionPresetCatalog._require_enabled``）。
#: production=运营日常入口；trial=已配置待验收；legacy=保留的历史入口。
PRESET_ENTRY_GROUPS = ("production", "trial", "legacy")


def preset_entry_group(raw: Mapping[str, Any]) -> str:
    """解析一条预设的展示分组：显式 ``entry_group`` 优先，否则按入口性质推导。

    推导规则（与 config/feishu_production_presets.json 里写入的值一致）：
    ``status != active`` ⇒ trial；``media_kind == native_photo`` ⇒ production；
    其余（历史视频入口）⇒ legacy。写成推导是为了让**将来新增**的预设即使漏写
    ``entry_group`` 也能落进正确分组，而不是悄悄掉出目录。
    """
    declared = str(raw.get("entry_group") or "").strip()
    if declared:
        if declared not in PRESET_ENTRY_GROUPS:
            raise FeishuWorkflowError(
                "未知的预设展示分组：" + declared + "；可选："
                + "、".join(PRESET_ENTRY_GROUPS)
            )
        return declared
    if str(raw.get("status", "active")) != "active":
        return "trial"
    if raw.get("media_kind") == "native_photo":
        return "production"
    return "legacy"


class ProductionPresetCatalog:
    def __init__(self, path: Optional[Path] = None):
        default = Path(__file__).resolve().parents[1] / "config" / "feishu_production_presets.json"
        self.path = Path(path) if path else default
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        self.overlay_profile_id = str(payload.get("default_overlay_profile_id") or "")
        self._raw = {item["name"]: item for item in payload.get("presets", [])}
        if not self._raw:
            raise FeishuWorkflowError("no Feishu production presets configured")

    @property
    def names(self) -> List[str]:
        return [name for name, raw in self._raw.items() if raw.get("status", "active") == "active"]

    def entry_group(self, name: str) -> str:
        """该预设落在目录的哪一组（常用 / 试验 / 历史）。"""
        return preset_entry_group(self.metadata(name))

    def entries(self, group: Optional[str] = None) -> List[Dict[str, Any]]:
        """只读目录视图：把预设按展示分组列出来。

        纯展示用途 —— 不参与生成、发布或任何校验决策。启用语义只由 ``status``
        承担：``production`` 组里 status=disabled 的预设同样 resolve 不了。
        """
        if group is not None and group not in PRESET_ENTRY_GROUPS:
            raise FeishuWorkflowError(
                "未知的预设展示分组：" + str(group) + "；可选："
                + "、".join(PRESET_ENTRY_GROUPS)
            )
        rows: List[Dict[str, Any]] = []
        for name, raw in self._raw.items():
            current = preset_entry_group(raw)
            if group is not None and current != group:
                continue
            tasks = list(raw.get("tasks") or [])
            rows.append({
                "name": name,
                "entry_group": current,
                "status": str(raw.get("status", "active")),
                "enabled": str(raw.get("status", "active")) == "active",
                "media_kind": str(raw.get("media_kind") or ""),
                "routing_policy": str(raw.get("routing_policy") or ""),
                "markets": sorted({str(item.get("market") or "")
                                   for item in tasks if item.get("market")}),
                "recipe_ids": sorted({str(item.get("recipe_id") or "")
                                      for item in tasks if item.get("recipe_id")}),
                # deterministic_one 预设（如「随机养号组合」）本身没有 tasks，
                # 只指向候选项；目录里一并显示，避免看成空入口。
                "tasks_from": list(raw.get("tasks_from") or []),
            })
        return rows

    def _require_enabled(self, name: str) -> None:
        raw = self.metadata(name)
        if raw.get("status", "active") != "active":
            raise FeishuWorkflowError("NEEDS_CONTENT: 该生产预设已停用，缺少通过内容验收的方案：" + name)

    def metadata(self, name: str) -> Dict[str, Any]:
        raw = self._raw.get(name)
        if raw is None:
            raise FeishuWorkflowError(f"未知生产预设：{name}")
        return dict(raw)

    def is_native_photo(self, name: str) -> bool:
        return self.metadata(name).get("media_kind") == "native_photo"

    def resolve(self, name: str, record_id: str) -> List[PresetTask]:
        self._require_enabled(name)
        raw = self._raw.get(name)
        if raw is None:
            raise FeishuWorkflowError(f"未知生产预设：{name}")
        if raw.get("selection") == "deterministic_one":
            candidates = list(raw.get("tasks_from") or [])
            if not candidates:
                raise FeishuWorkflowError(f"预设没有候选项：{name}")
            digest = hashlib.sha256(record_id.encode("utf-8")).digest()
            return self.resolve(candidates[digest[0] % len(candidates)], record_id)
        tasks = [PresetTask(**item) for item in raw.get("tasks", [])]
        if not tasks:
            raise FeishuWorkflowError(f"预设没有生产任务：{name}")
        return tasks

    def resolve_batch(
        self, name: str, record_id: str, quantity: int
    ) -> List[PresetTask]:
        """Expand one operator row to exactly ``quantity`` deterministic units."""
        self._require_enabled(name)
        if quantity < 1 or quantity > 9:
            raise FeishuWorkflowError("生成篇数必须是 1 到 9")
        raw = self._raw.get(name)
        if raw is None:
            raise FeishuWorkflowError(f"未知生产预设：{name}")
        if raw.get("selection") == "deterministic_one":
            candidates = list(raw.get("tasks_from") or [])
            if not candidates:
                raise FeishuWorkflowError(f"预设没有候选项：{name}")
            result: List[PresetTask] = []
            for index in range(quantity):
                digest = hashlib.sha256(
                    f"{record_id}:{index + 1}".encode("utf-8")
                ).digest()
                selected = candidates[digest[0] % len(candidates)]
                result.append(self.resolve(selected, f"{record_id}:{index + 1}")[0])
            return self._diversify_repeated_recipes(result)
        tasks = [PresetTask(**item) for item in raw.get("tasks", [])]
        if not tasks:
            raise FeishuWorkflowError(f"预设没有生产任务：{name}")
        return self._diversify_repeated_recipes(
            [tasks[index % len(tasks)] for index in range(quantity)]
        )

    def preview(self, name: str, record_id: str, quantity: int) -> Dict[str, Any]:
        """Cheap routing preview; actual library counts are only known after planning."""
        from services.multi_look_planner import MULTI_LOOK_RECIPE_ID
        specs = self.resolve_batch(name, record_id, quantity)
        return {"preset": name, "routing_policy": self._raw[name].get("routing_policy", "explicit_preset"),
                "count_status": "requested_not_yet_resolved",
                "note": "目标套数，不是实际套数；生成前按兼容穿搭库冻结并展示实际页数与时长。",
                "videos": [{"recipe_id": spec.recipe_id,
                            "requested_look_count": 5 if spec.recipe_id == MULTI_LOOK_RECIPE_ID else None,
                            "target_duration_ms": 6000 if spec.recipe_id == MULTI_LOOK_RECIPE_ID else None}
                           for spec in specs]}

    @staticmethod
    def _diversify_repeated_recipes(tasks: List[PresetTask]) -> List[PresetTask]:
        """Keep the preset's first intent; rotate hooks on later repetitions."""
        seen: Dict[str, int] = {}
        result = []
        for task in tasks:
            occurrence = seen.get(task.recipe_id, 0)
            seen[task.recipe_id] = occurrence + 1
            result.append(
                task if occurrence == 0 else replace(task, hook_strategy="")
            )
        return result


def quantity_value(value: Any) -> int:
    if value in (None, ""):
        return 1
    try:
        result = int(float(value))
    except (TypeError, ValueError) as exc:
        raise FeishuWorkflowError("生成篇数必须是 1 到 9 的整数") from exc
    if result < 1 or result > 9 or float(value) != result:
        raise FeishuWorkflowError("生成篇数必须是 1 到 9 的整数")
    return result


class FeishuTaskWorkflow(FeishuV2Mixin):
    def __init__(
        self,
        repository,
        client,
        *,
        catalog: Optional[ProductionPresetCatalog] = None,
        generator=None,
        renderer=None,
        publish_scheduler=None,
        product_reference_resolver=None,
        visual_qa_adapter=None,
        output_root=None,
        asset_readiness_gate=None,
        photo_reference_vision=None,
        publish_account_resolver=None,
    ):
        self.repository = repository
        self.client = client
        self.catalog = catalog or ProductionPresetCatalog()
        self.generator = generator or build_default_photo_generator()
        self.renderer = renderer or FFmpegStillRenderer()
        self.publish_scheduler = publish_scheduler
        self.visual_qa_adapter = visual_qa_adapter
        self.output_root = output_root
        self.asset_readiness_gate = asset_readiness_gate
        self.photo_reference_vision = photo_reference_vision
        self.publish_account_resolver = publish_account_resolver
        self._run_lease = None
        self._photo_quality_summaries: Dict[str, str] = {}
        self.product_reference_resolver = (
            product_reference_resolver or ProductReferenceResolver(repository)
        )

    def _resolve_target_account(self, record) -> "Optional[Any]":
        """解析行级「目标账号（可选）」；显式填写但解析失败必须报错。

        返回 ``PublishAccountBinding``；未填写时返回 ``None``（旧任务原路径，
        不允许任何静默回退店铺公共池的逻辑出现在这里）。
        """
        handle = text_value(record.fields.get(FIELD_TARGET_ACCOUNT))
        if not handle:
            return None
        from services.publish_account_profile import (
            PublishAccountProfileError, build_default_resolver,
        )
        resolver = self.publish_account_resolver or build_default_resolver()
        self.publish_account_resolver = resolver
        try:
            return resolver.resolve(handle)
        except PublishAccountProfileError as exc:
            raise FeishuWorkflowError(str(exc)) from exc

    @staticmethod
    def _complete_look_attachments(fields: Dict[str, Any]) -> List[Dict[str, Any]]:
        return list(fields.get(FIELD_PHOTO_INPUT) or fields.get(FIELD_PHOTO_INPUT_LEGACY) or [])

    @staticmethod
    def _layering_role_attachments(
        fields: Dict[str, Any], *, label: str = "温度穿搭",
    ) -> List[Dict[str, Any]]:
        ordered = []
        for field_name in (
            FIELD_LAYER_BASE_REFERENCE,
            FIELD_LAYER_MID_REFERENCE,
            FIELD_LAYER_OUTER_REFERENCE,
        ):
            values = list(fields.get(field_name) or [])
            if len(values) != 1:
                raise FeishuWorkflowError(
                    f"{label} COMPLETE_LOOK 要求“{field_name}”恰好一张"
                )
            ordered.append(values[0])
        return ordered

    @staticmethod
    def _is_replannable_photo_error(exc: Exception) -> bool:
        """Only pre-batch local planning/supply conflicts may start a clean attempt."""
        message = str(exc)
        return any(marker in message for marker in (
            "参考图或内容要求已变化",
            "参考分析或旅行变量已变化",
            "主题、预设、参考模式或生成数量已变化",
            "参考图或主题已变化",
            "主题、人物或参考图已变化",
            "整组参考一致性重做次数已用尽",
        ))

    @staticmethod
    def _archive_photo_planning_state(
        root: Path,
        record_id: str,
        *,
        reason: str,
        include_reference: bool = True,
        include_content_plan: bool = True,
        supply_item_ids: Optional[Iterable[str]] = None,
    ) -> Optional[Path]:
        """Move stale pre-batch state aside so a row can be safely replanned once."""
        safe_record_id = "".join(
            char if char.isalnum() or char in "-_" else "_" for char in record_id
        )
        sources: list[tuple[str, Path]] = []
        if include_reference:
            sources.append(("reference_contracts", root / "reference_contracts" / record_id))
        if include_content_plan:
            sources.append(("content_plans", root / "content_plans" / record_id))
        if supply_item_ids is None:
            supply_paths = sorted((root / "style_reference_supply").glob(f"{record_id}_item_*"))
        else:
            supply_paths = [root / "style_reference_supply" / value for value in supply_item_ids]
        sources.extend(("style_reference_supply", path) for path in supply_paths)
        sources = [(group, path) for group, path in sources if path.exists()]
        if not sources:
            return None

        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        archive = root / "replan_archive" / f"{safe_record_id}_{stamp}"
        moved: list[str] = []
        for group, source in sources:
            destination = archive / group / source.name
            destination.parent.mkdir(parents=True, exist_ok=True)
            source.rename(destination)
            moved.append(str(destination.relative_to(archive)))
        (archive / "replan.json").write_text(json.dumps({
            "record_id": record_id,
            "archived_at": datetime.now(timezone.utc).isoformat(),
            "reason": reason,
            "moved": moved,
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        return archive

    @staticmethod
    def _archive_frozen_plan_copy(
        root: Path, record_id: str, *, payload: Mapping[str, Any], reason: str,
    ) -> Optional[Path]:
        """Keep a **copy** of the frozen plan before its copy fields are rebuilt.

        ``_archive_photo_planning_state`` *moves* state aside, which is right for
        a real replan but wrong here: the paid images must stay in place and the
        row must keep running.  So this only preserves the old plan (the Thai
        original, in the 2026-09-14 case) under the same ``replan_archive``
        convention for audit, and touches nothing else.
        """
        if not payload:
            return None
        safe_record_id = "".join(
            char if char.isalnum() or char in "-_" else "_" for char in record_id
        )
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        archive = root / "replan_archive" / f"{safe_record_id}_{stamp}"
        target = archive / "content_plans" / safe_record_id / "plan.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(dict(payload), ensure_ascii=False, indent=2), encoding="utf-8")
        (archive / "copy_repair.json").write_text(json.dumps({
            "record_id": record_id,
            "archived_at": datetime.now(timezone.utc).isoformat(),
            "reason": reason,
            "note": "只归档旧计划副本（文案语言错的那一版）；"
                    "content_plan 与 style_reference_supply 原地保留，图片生成器不会被调用",
            "copied": [str(target.relative_to(archive))],
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        return archive

    def scan(
        self,
        *,
        dry_run: bool = False,
        record_id: str = "",
        resume_running: bool = False,
    ) -> Dict[str, Any]:
        records = (
            [self.client.get_record(record_id)]
            if record_id
            else self.client.list_records(page_size=500)
        )
        report = {"scanned": len(records), "eligible": 0, "processed": [], "errors": []}
        for record in records:
            fields = record.fields or {}
            # Complete a committed photo projection before interpreting any
            # retained command. This includes terminal-review writeback failure.
            pending_batch = self._batch(record.record_id) if not dry_run else None
            if (pending_batch and pending_batch.manifest_json.get("media_kind") == "native_photo"
                    and pending_batch.pending_fields_json):
                try:
                    self._claim_batch(pending_batch)
                    pending = pending_batch.pending_fields_json
                    self.client.update_record_fields(record.record_id, pending)
                    self.repository.acknowledge_batch_projection(pending_batch.batch_id, pending)
                    report["processed"].append({"record_id": record.record_id, "action": "recover_projection"})
                except BatchLeaseBusy as exc:
                    report["processed"].append({"record_id": record.record_id, "action": "leased_skip", "reason": str(exc)})
                except Exception as exc:
                    report["errors"].append({"record_id": record.record_id, "error": f"恢复工作台失败：{exc}"})
                finally:
                    if self._run_lease:
                        self._run_lease.close()
                        self._run_lease = None
                continue
            action = self._action(fields)
            v2_tasks = (
                self._v2_tasks(record.record_id)
                if action in {"approve", "retry_review"} else []
            )
            all_photo = bool(v2_tasks) and all(
                str(getattr(task, "media_kind", "video") or "video") == "native_photo"
                for task in v2_tasks
            )
            if (action == "approve" and not v2_tasks
                    and text_value(fields.get(FIELD_PHOTO_ASSET_STATUS)) == "待内容审核"):
                action = "approve_photo_assets"
            if action in {"approve", "retry_review"} and v2_tasks and not (
                action == "approve" and all_photo
            ):
                # A retained legacy review command is not permission to start
                # fresh media work under the new technical-only workflow.
                action = "generate" if bool(fields.get(FIELD_EXECUTE)) else ""
            if action == "schedule" and not bool(fields.get(FIELD_CONFIRM_PUBLISH)) and self._v2_tasks(record.record_id):
                action = ""  # V2 requires the dedicated explicit publish checkbox.
            if not dry_run and not action:
                try:
                    batch = self._batch(record.record_id)
                    if batch and batch.pending_fields_json:
                        pending = batch.pending_fields_json
                        self.client.update_record_fields(record.record_id, pending)
                        self.repository.acknowledge_batch_projection(batch.batch_id, pending)
                        report["processed"].append({"record_id": record.record_id, "action": "recover_projection"})
                        continue
                    if batch and batch.batch_status == "running" and batch.lease_until and text_value(fields.get(FIELD_PROGRESS)) == PROGRESS_RUNNING:
                        from datetime import datetime
                        if batch.lease_until <= datetime.utcnow():
                            self._write_fields(record.record_id, {FIELD_PROGRESS: PROGRESS_ACTION,
                                FIELD_EXECUTE: False, FIELD_REVIEW: REVIEW_PENDING,
                                FIELD_NOTES: "上次运行租约已过期；在途生成结果未知，未自动重跑。请核对 RDS 素材后显式续跑或返工。"})
                            continue
                except Exception as exc:
                    report["errors"].append({"record_id": record.record_id, "error": f"恢复工作台失败：{exc}"})
                    continue
            if (
                resume_running
                and record_id
                and text_value(fields.get(FIELD_PROGRESS)) == PROGRESS_RUNNING
            ):
                action = "generate"
            if not action:
                if (
                    callable(getattr(self.publish_scheduler, "get_task_state", None))
                    and text_value(fields.get(FIELD_PROGRESS))
                    in {PROGRESS_QUEUED, PROGRESS_SCHEDULED, PROGRESS_PUBLISHING,
                        PROGRESS_PUBLISH_FAILED}
                ):
                    # 发布失败的行也要继续投影：底层重试恢复后，陈旧的失败
                    # 显示必须能自愈回真实状态（已排期/已发布）。
                    synced = self.sync_publication_status(record.record_id, current_fields=fields)
                    if synced.get("updated"):
                        report["processed"].append(synced)
                continue
            report["eligible"] += 1
            if dry_run:
                result = {"record_id": record.record_id, "action": action}
                if action == "generate" and callable(getattr(self.catalog, "preview", None)):
                    try:
                        batch = self._batch(record.record_id)
                        if batch:
                            result["production_preview"] = {"source": "frozen_batch",
                                "note": "续跑沿用冻结清单，不按当前预设重新选配方。",
                                "recipe_ids": [entry["spec"]["recipe_id"] for entry in batch.manifest_json["entries"]]}
                        else:
                            existing = (self._tasks(record.record_id) if callable(getattr(
                                self.repository, "list_tasks_by_source_prefix", None)) else [])
                            if existing:
                                result["production_preview"] = {"source": "existing_tasks",
                                    "note": self._production_plan_note(existing)}
                            else:
                                result["production_preview"] = self.catalog.preview(
                                    text_value(fields.get(FIELD_PRESET)), record.record_id,
                                    task_quantity(fields))
                    except Exception as exc:
                        result["preview_error"] = str(exc)
                report["processed"].append(result)
                continue
            try:
                with _RecordFlock(record.record_id) as record_locked:
                    if not record_locked:
                        # Another scanner slot owns this row; it will project
                        # the final fields when it finishes.
                        report["processed"].append({
                            "record_id": record.record_id,
                            "action": "record_locked_skip",
                        })
                        continue
                    batch = self._batch(record.record_id)
                    if batch:
                        self._claim_batch(batch)
                    if action == "generate":
                        result = self._generate(record)
                    elif action == "auto_render":
                        result = self._approve_and_render(
                            record, approval_mode="operator_auto"
                        )
                    elif action == "approve":
                        result = self._approve_and_render(record)
                    elif action == "approve_photo_assets":
                        result = self._approve_staged_photo_assets(record)
                    elif action == "retry_review":
                        tasks = self._v2_tasks(record.record_id)
                        if not tasks:
                            raise FeishuWorkflowError("重试审核仅适用于 V2 分阶段任务")
                        result = self._advance_v2(record, tasks, automatic=self._review_mode(record) == MODE_AUTO, resume_generation=False)
                    elif action == "confirm_publish":
                        result = self._confirm_photo_publish(record)
                    elif action == "schedule":
                        result = self._schedule_publish(record)
                    else:
                        result = self._redo(record, action)
                    report["processed"].append(result)
            except BatchLeaseBusy as exc:
                # A losing worker must not consume commands or overwrite the
                # active owner's workbench projection.
                report["processed"].append({"record_id": record.record_id, "action": "leased_skip", "reason": str(exc)})
            except ProjectionPendingError as exc:
                report["errors"].append({"record_id": record.record_id, "error": str(exc), "projection_pending": True})
            except Exception as exc:  # noqa: BLE001 - isolate workbench rows
                message = str(exc).strip()[:900] or exc.__class__.__name__
                if isinstance(exc, OPAQUE_INTERNAL_ERRORS):
                    # 裸 KeyError/TypeError 的 str() 只是一个字段名，运营无从下手；
                    # 显式标注为系统内部错误，避免被误读成业务拒绝。业务异常消息逐字不变。
                    message = f"系统内部错误（{exc.__class__.__name__}）：{message}"
                failure_fields = {
                    FIELD_EXECUTE: False,
                    FIELD_PROGRESS: PROGRESS_ACTION,
                    FIELD_NOTES: message,
                    FIELD_FAILURE_REASON: message,
                    FIELD_RETRY_REVIEW: False,
                    FIELD_REVIEW: REVIEW_PENDING,
                }
                try:
                    prior_progress = text_value(
                        self.client.get_record(record.record_id).fields.get(FIELD_PROGRESS))
                except Exception:
                    prior_progress = ""
                if action == "generate" and (
                        prior_progress in IN_FLIGHT_PROGRESS
                        or prior_progress.startswith("素材生成 ")):
                    # Paid work already started; resume reuses completed assets
                    # instead of regenerating them, so mark it retryable.
                    failure_fields[FIELD_PROGRESS] = PROGRESS_RETRYABLE
                    failure_fields[FIELD_NOTES] = (
                        message + "；已生成素材已保留，重新勾选执行将从断点续跑，不重复生成已完成角色"
                    )
                if action in {"schedule", "confirm_publish"}:
                    failure_fields[FIELD_CONFIRM_PUBLISH] = False
                try:
                    current_batch = self._batch(record.record_id)
                    if self._v2_tasks(record.record_id) or (
                        current_batch and int(current_batch.manifest_json.get("workflow_version") or 1) >= 2
                    ):
                        is_photo = current_batch and current_batch.manifest_json.get("media_kind") == "native_photo"
                        failure_fields.update({FIELD_REVIEW: REVIEW_NOT_REQUIRED,
                                               FIELD_REVIEW_MODE: None})
                    self._write_fields(record.record_id, failure_fields)
                except Exception as projection_error:
                    message += f"；飞书回写失败，RDS 状态保留：{projection_error}"
                report["errors"].append({"record_id": record.record_id, "error": message})
            finally:
                if self._run_lease:
                    try:
                        self._run_lease.close()
                    except Exception as exc:
                        report["errors"].append({"record_id": record.record_id, "error": f"租约释放失败：{exc}"})
                    self._run_lease = None
        return report

    def _batch(self, record_id):
        reader = getattr(self.repository, "get_production_batch", None)
        return reader(record_id) if callable(reader) else None

    def _claim_batch(self, batch):
        if self._run_lease:
            self._run_lease.check()
            return
        from services.production_batch import BatchLease
        self._run_lease = BatchLease(self.repository, batch.batch_id)

    def _write_fields(self, record_id, fields):
        if self._run_lease:
            self._run_lease.check()
        batch = self._batch(record_id)
        if batch:
            self.repository.queue_batch_projection(batch.batch_id, fields)
        try:
            self.client.update_record_fields(record_id, fields)
        except Exception as exc:
            if batch:
                raise ProjectionPendingError(f"RDS 状态已保留，飞书投影等待重放：{exc}") from exc
            raise
        if batch:
            self.repository.acknowledge_batch_projection(batch.batch_id, fields)

    def _assert_batch_complete(self, record_id, tasks):
        batch = self._batch(record_id)
        if batch and (len(tasks) != batch.expected_count or len({t.source_record_id for t in tasks}) != batch.expected_count):
            raise FeishuWorkflowError(f"批次只建成 {len(tasks)}/{batch.expected_count} 条，勾选执行补齐冻结清单后再审核或发布")
        if batch and batch.manifest_json.get("media_kind") == "native_photo":
            entries = self._photo_batch_entries(batch)
            if {entry["source_record_id"] for entry in entries} != {task.source_record_id for task in tasks}:
                raise FeishuWorkflowError("图文任务与冻结批次清单不一致，禁止放行")

    @staticmethod
    def _action(fields: Dict[str, Any]) -> str:
        review = text_value(fields.get(FIELD_REVIEW))
        progress = text_value(fields.get(FIELD_PROGRESS))
        if bool(fields.get(FIELD_CONFIRM_PUBLISH)) and progress not in {
            PROGRESS_QUEUED, PROGRESS_SCHEDULED, PROGRESS_PUBLISHING, PROGRESS_PUBLISHED,
        }:
            return "confirm_publish"
        if review == REVIEW_SCHEDULE and progress not in {
            PROGRESS_QUEUED, PROGRESS_SCHEDULED, PROGRESS_PUBLISHING, PROGRESS_PUBLISHED,
        }:
            return "schedule"
        if review == REVIEW_APPROVED and progress != PROGRESS_DONE:
            return "approve"
        if review == REVIEW_REDO_ALL:
            return "redo_all"
        if review == "重做成片":
            return "redo_render"
        if review.startswith("重做P") and review[3:].isdigit():
            return f"redo_{int(review[3:])}"
        if bool(fields.get(FIELD_RETRY_REVIEW)):
            return "retry_review"
        if bool(fields.get(FIELD_EXECUTE)) and parse_retake_roles(
                fields.get(FIELD_RETAKE_LOOK)) and progress not in {
            PROGRESS_QUEUED, PROGRESS_SCHEDULED, PROGRESS_PUBLISHING, PROGRESS_PUBLISHED,
        }:
            # 运营手动重拍：已完成的技术成品也允许按重拍 Look 重生指定素材。
            return "generate"
        if bool(fields.get(FIELD_EXECUTE)) and progress not in {
            PROGRESS_DONE, PROGRESS_QUEUED, PROGRESS_SCHEDULED, PROGRESS_PUBLISHING,
            PROGRESS_PUBLISHED,
        }:
            return "generate"
        return ""

    def _generate(self, record) -> Dict[str, Any]:
        batch = self._batch(record.record_id)
        existing = self._tasks(record.record_id)
        from services.release_gate import require_photo_content_allowed
        for task in existing:
            require_photo_content_allowed(self.repository, task)
        if (existing and batch is None
                and all(workflow_v2_enabled(task) for task in existing)
                and all(str(getattr(task, "media_kind", "video") or "video") != "native_photo"
                        for task in existing)):
            if len(existing) != task_quantity(record.fields):
                raise FeishuWorkflowError("历史批次缺少冻结清单且任务数量不足；请人工核对，不能按当前配置猜测补单")
            return self._advance_v2(record, existing)
        preset_name = batch.manifest_json["preset"] if batch else text_value(record.fields.get(FIELD_PRESET))
        if not preset_name:
            raise FeishuWorkflowError("请选择生产预设")
        quantity = batch.expected_count if batch else task_quantity(record.fields)
        if ((batch and batch.manifest_json.get("media_kind") == "native_photo")
                or (batch is None and self.catalog.is_native_photo(preset_name))):
            return self._generate_native_photo(record, preset_name, quantity, existing, batch=batch)
        if text_value(record.fields.get(FIELD_TARGET_ACCOUNT)):
            # 本轮目标账号只接入原生图文；视频线填写了目标账号必须显式报错，
            # 不允许静默忽略导致运营误以为内容已绑定账号。
            raise FeishuWorkflowError(
                "目标账号（可选）当前只支持原生图文预设；视频任务请清空该字段"
            )
        if existing and batch is None:
            if len(existing) != task_quantity(record.fields):
                raise FeishuWorkflowError("历史批次缺少冻结清单且任务数量不足；请人工核对，不能按当前配置猜测补单")
            if all(workflow_v2_enabled(task) for task in existing):
                return self._advance_v2(record, existing)
            if any(workflow_v2_enabled(task) for task in existing):
                raise FeishuWorkflowError("历史混合 V1/V2 批次需要人工拆分，不能遗漏子任务")
        product_id = batch.manifest_json["product_id"] if batch else text_value(record.fields.get(FIELD_PRODUCT))
        if not product_id:
            raise FeishuWorkflowError("请填写产品编码")
        specs = ([PresetTask(**entry["spec"]) for entry in batch.manifest_json["entries"]]
                 if batch else self.catalog.resolve_batch(preset_name, record.record_id, quantity))
        allowed_source_ids = {
            f"{record.record_id}:{index}:{spec.recipe_id}:1"
            for index, spec in enumerate(specs, start=1)
        }
        unexpected = [
            task.source_record_id
            for task in self._tasks(record.record_id)
            if task.source_record_id not in allowed_source_ids
        ]
        if unexpected:
            raise FeishuWorkflowError(
                "该记录已经按其他生产预设建过任务；为避免混组，请新建一行再选择新预设"
            )
        self._write_fields(record.record_id, {
            FIELD_EXECUTE: False,
            FIELD_PROGRESS: PROGRESS_RUNNING,
            FIELD_REVIEW: REVIEW_NOT_REQUIRED,
            FIELD_NOTES: "",
            **({FIELD_REVIEW_MODE: None} if not existing or all(workflow_v2_enabled(task) for task in existing)
               or (batch and int(batch.manifest_json.get("workflow_version") or 1) >= 2) else {}),
        })
        task_ids: List[str] = []
        products: List[Dict[str, Any]] = []
        asset_reader = LightTryonAssetReader()
        if batch is None:
            for index, spec in enumerate(specs, start=1):
                try:
                    products.append(self.product_reference_resolver.resolve_snapshot(
                        product_id, selection_key=f"{record.record_id}:{index}", account_id=spec.account_id,
                    ))
                except ProductReferenceResolutionError as exc:
                    raise FeishuWorkflowError(str(exc)) from exc
            assignments = BatchDiversityPlanner(self.repository, asset_reader).plan(
                record_id=record.record_id, specs=specs, products=products,
            )
            if not existing:
                from services.workflow_v2 import canonical_hash
                batch = self.repository.create_production_batch_idempotent(ProductionBatch(
                    batch_id="opv_batch_" + canonical_hash(record.record_id)[:32],
                    source_record_id=record.record_id, expected_count=quantity,
                    manifest_json={"workflow_version": 2, "product_id": product_id, "preset": preset_name,
                                   "entries": [{"spec": asdict(a.spec), "product": a.product_snapshot} for a in assignments]},
                ))
            else:
                # Historical V1 rows retain their old behavior and do not
                # receive a retroactive V2 plan or batch interpretation.
                entries = [{"spec": asdict(a.spec), "product": a.product_snapshot} for a in assignments]
        if batch:
            self._claim_batch(batch)
            product_id = batch.manifest_json["product_id"]
            entries = batch.manifest_json["entries"]
            if len(entries) != batch.expected_count:
                raise FeishuWorkflowError("冻结批次清单数量损坏，禁止创建或放行")
        products = [entry["product"] for entry in entries]
        producer = HeroFirstProducer(self.repository, self.generator, output_root=self.output_root,
                                     asset_readiness_gate=self.asset_readiness_gate)
        for index, entry in enumerate(entries, start=1):
            if self._run_lease:
                self._run_lease.check()
            spec, product = PresetTask(**entry["spec"]), entry["product"]
            prefix = f"{record.record_id}:{index}:{spec.recipe_id}"
            found = [t for t in self._tasks(record.record_id) if t.source_record_id == prefix + ":1" and t.plan_json and t.task_status != "draft"]
            rows = [{"task_id": t.task_id} for t in found] or generate_product_image_story(
                self.repository,
                product_id=product_id,
                market=spec.market,
                language=spec.language,
                recipe_id=spec.recipe_id,
                account_id=spec.account_id,
                theme_id=spec.theme_id,
                hook_strategy=spec.hook_strategy,
                product_snapshot=dict(product),
                operator="feishu_opv_workbench",
                asset_reader=asset_reader,
                source_type=SOURCE_TYPE,
                source_record_id_prefix=prefix,
                feishu_record_id=record.record_id,
                persona_ref=spec.persona_ref,
                look_ref=spec.look_ref,
                scene_ref=spec.scene_ref,
                variant_index_offset=index - 1,
            )
            for row in rows:
                task_id = row["task_id"]
                task = self.repository.get_task(task_id)
                if batch and not workflow_v2_enabled(task):
                    if task.task_status != "planned" or task.active_revision_id:
                        raise FeishuWorkflowError("冻结 V2 批次中发现已运行的 V1 子任务，需人工核对")
                    self.repository.update_task_plan(task_id, plan_json={**task.plan_json, "workflow_version": 2})
                    from services.workflow_v2 import RevisionService
                    RevisionService(self.repository).ensure_working(self.repository.get_task(task_id))
                    task = self.repository.get_task(task_id)
                task_ids.append(task_id)
        # Show actual frozen counts, not the preset's promised maximum, before
        # any costly image call. This is informational, never a new approval gate.
        self._write_fields(record.record_id, {FIELD_NOTES: self._production_plan_note(
            [self.repository.get_task(task_id) for task_id in task_ids])})
        # Materialize the whole frozen batch before starting costly media.
        for task_id in task_ids:
            if self._run_lease:
                self._run_lease.check()
            task = self.repository.get_task(task_id)
            if workflow_v2_enabled(task):
                continue
            if task and task.task_status in {"planned", "hero_generating", "image_generating", "failed"}:
                producer.produce(task_id)
            self._recover_incomplete_review(task_id, producer)
        # Workflow V2 intentionally stops after the anchor.  It is a normal
        # review state, not a failed auto-render and must never be handed to
        # the group-render loop below.
        v2_tasks = [self.repository.get_task(tid) for tid in task_ids if workflow_v2_enabled(self.repository.get_task(tid))]
        if v2_tasks:
            if len(v2_tasks) != len(task_ids):
                raise FeishuWorkflowError("同一记录不能混合 V1 自动成片和 V2 分阶段审核任务")
            return self._advance_v2(record, v2_tasks, automatic=self._review_mode(record) == MODE_AUTO)
        reference_note = self._reference_notes(products)
        rendered = self._render_tasks(
            task_ids,
            approval_mode="operator_auto",
            notes=f"飞书自动生产；{reference_note}",
        )
        attachments = self._upload_files(rendered, parent_type="bitable_file")
        self._write_fields(record.record_id, {
            FIELD_PROGRESS: PROGRESS_DONE,
            FIELD_OUTPUT: attachments,
            FIELD_REVIEW: REVIEW_NOT_REQUIRED,
            FIELD_NOTES: (
                f"已自动生成 {len(rendered)} 条成片；{reference_note}。"
                "仅执行必要的媒体技术检查，未进行人工审美审核；未发布。"
            ),
        })
        return {
            "record_id": record.record_id,
            "action": "generate_and_render",
            "task_ids": task_ids,
            "videos": rendered,
        }

    def _generate_native_photo(self, record, preset_name: str, quantity: int, existing, *, batch=None) -> Dict[str, Any]:
        """Freeze the entire row before task creation; retry only its frozen entries."""
        if batch and batch.batch_status == "cancelled":
            raise FeishuWorkflowError("该图文批次已取消；请新增一行重新发起，历史记录保持只读")
        if batch is None and not existing and self._mx_wig_recipe_for_preset(preset_name) is not None:
            # Explicit MX dispatch (mx_wig_choice_v1): only brand-new rows on
            # the MX wig preset enter the wig flow here.  Frozen MX batches
            # fall through to the shared production tail below, where frozen
            # request validation and the planner dispatch back into the wig
            # module; historical TH/MX V1 rows never enter the wig code.
            return self._generate_mx_wig_photo(record, preset_name, quantity)
        from config.loader import load_board_layouts
        from services.photo_package import NativePhotoProductionFlow
        from services.photo_planner import PhotoReusePlannerService
        from services.photo_request_factory import (
            PhotoRequestFactory, apply_travel_single_cover, fingerprint,
            validate_frozen_request,
        )
        from services.task_intake import TaskIntakeService, TaskRequest

        style_product: dict[str, Any] = {}

        if batch is None:
            if existing:
                raise FeishuWorkflowError("历史图文缺少冻结批次；请先核对原任务，不能按当前预设自动补单")
            overrides = []
            raw = text_value(record.fields.get(FIELD_PHOTO_REQUEST))
            if raw:
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise FeishuWorkflowError("图文任务JSON不是有效 JSON") from exc
                if not isinstance(payload, dict):
                    raise FeishuWorkflowError("图文任务JSON必须是对象")
                overrides = payload.get("items") if "items" in payload else [payload]
                if (not isinstance(overrides, list) or len(overrides) != quantity
                        or any(not isinstance(item, dict) for item in overrides)):
                    raise FeishuWorkflowError("图文任务JSON.items 数量必须与生成篇数一致")
            specs = self.catalog.resolve_batch(preset_name, record.record_id, quantity)
            if any(not spec.account_id for spec in specs):
                raise FeishuWorkflowError("该图文预设尚未绑定 OPV 生产账号")
            preset = self.catalog.metadata(preset_name)

            def _agnostic_asset_binding(recipe_obj: Any) -> dict[str, str]:
                """国家无关配方（V3）的素材集类别与市场来源。

                《VN_SCARF_CROSS_MARKET_IMPLEMENTATION_SPEC》§通用 Recipe 删除项
                明确要求 V3 配方不再固定 ``markets`` / ``category_key``：市场由
                Market Pack 拥有、类别由预设（行级声明）提供。因此配方未声明这两项
                时，由本函数把预设类别与该篇市场传给素材集入库；声明齐全的旧配方
                返回空 dict，调用参数与行为逐字不变。
                """
                spec_json = dict(getattr(recipe_obj, "recipe_spec_json", {}) or {})
                if spec_json.get("category_key") and spec_json.get("markets"):
                    return {}
                return {
                    "category_key": str(preset.get("category_key") or ""),
                    "market": str(specs[0].market or ""),
                }

            product_id = text_value(record.fields.get(FIELD_PRODUCT))
            unified_attachments = list(record.fields.get(FIELD_REFERENCE) or [])
            legacy_complete = self._complete_look_attachments(record.fields)
            legacy_product = list(record.fields.get(FIELD_PRODUCT_REFERENCE) or [])
            recipe_ids = {spec.recipe_id for spec in specs}
            recipe_for_input = (
                self.repository.get_content_recipe(specs[0].recipe_id)
                if len(recipe_ids) == 1 else None
            )
            roles = list((((recipe_for_input.recipe_spec_json or {}).get("asset_requirements") or {}).get("required_roles") or [])) if recipe_for_input else []
            # 发布语言与语言标签由 Locale Pack 拥有（review 修复 P0-2）。只有
            # 配方自己声明了 ``locale_copy_packs``（国家无关配方 v2）时才绑定
            # pack；v1 配方继续读内联泰语表，TH V2 输出逐字不变。
            recipe_locale_packs = dict(
                ((recipe_for_input.recipe_spec_json or {}) if recipe_for_input else {})
                .get("locale_copy_packs") or {}
            )
            locale_pack = None
            # 发布语言由 Locale Pack 拥有（review 修复 P0-2），主题文案覆盖
            # （``build_theme_copy``）也必须知道本任务的语言：否则非 TH 行会从
            # 主题内联文案拿到泰语 title/CTA/hashtags 兜底（2026-09-14 VN 围巾线）。
            # v1 配方不带 language，留空即沿用 TH 默认行为。
            publish_locale = str(getattr(specs[0], "language", "") or "")
            if recipe_locale_packs:
                from config.loader import resolve_locale_pack
                if not publish_locale:
                    raise FeishuWorkflowError("国家无关图文配方缺少发布语言")
                locale_pack = resolve_locale_pack(publish_locale)
                if locale_pack is None:
                    raise FeishuWorkflowError(
                        "找不到发布语言对应的 Locale Pack：" + publish_locale)
            from services.photo_content_planner import get_planning_flow
            planning_flow = get_planning_flow(recipe_for_input.recipe_id) if recipe_for_input else ""
            from services.photo_flow_registry import (
                is_layered_progression_flow, is_thermal_transition_flow,
            )
            # 分层图文（温度分层 / 冷热切换）共享同一套三态素材、生成与质检机制，
            # 差别只在业务变量与合同；因此统一由 registry 谓词路由，不再逐处写
            # flow 字符串比较。
            thermal_transition_flow = is_thermal_transition_flow(planning_flow)
            layering_flow = is_layered_progression_flow(planning_flow)
            flow_label = "冷热切换" if thermal_transition_flow else "温度分层"
            temperature_variables: dict[str, str] = {}
            if layering_flow:
                if quantity != 1:
                    raise FeishuWorkflowError(f"{flow_label}首版每条记录只允许生成一篇")
                if product_id:
                    raise FeishuWorkflowError(f"{flow_label}不支持商品参考模式")
                temperature_variables = (
                    resolve_thermal_transition_variables(record.fields)
                    if thermal_transition_flow
                    else resolve_temperature_variables(record.fields)
                )
            from services.photo_theme import resolve_photo_theme, build_theme_copy
            from services.photo_reference import (
                REFERENCE_MODE_COMPLETE_LOOK, REFERENCE_MODE_PRODUCT, REFERENCE_MODE_STYLE,
            )
            try:
                theme = resolve_photo_theme(text_value(record.fields.get(FIELD_CONTENT_THEME)))
            except ValueError as exc:
                raise FeishuWorkflowError(str(exc)) from exc
            # ---- 目标账号（可选）：账号默认主题/定位/表达/视觉基准的统一解析 ----
            # 本篇配置优先级（按项）：任务显式选择 → 账号默认 → 原有行为。
            # 只有填写了目标账号的行才会新增任何冻结键；旧行为逐字不变。
            account_binding = self._resolve_target_account(record)
            theme_source = "task" if theme is not None else ""
            if theme is None and account_binding is not None:
                default_theme = str(
                    account_binding.profile.get("default_theme") or "").strip()
                if default_theme:
                    try:
                        theme = resolve_photo_theme(default_theme)
                    except ValueError as exc:
                        raise FeishuWorkflowError(
                            f"账号 {account_binding.account_id} 的默认主题无效：{exc}"
                        ) from exc
                    theme_source = "account_default"
            if account_binding is not None:
                market = str(specs[0].market or "").strip().upper()
                binding_market = str(account_binding.target_country or "").strip().upper()
                if binding_market and market and binding_market != market:
                    raise FeishuWorkflowError(
                        f"目标账号 {account_binding.account_id} 属于 "
                        f"{binding_market} 市场，与预设 {preset_name}（{market}）不一致"
                    )
                row_store = text_value(record.fields.get(FIELD_STORE))
                if row_store and row_store != account_binding.store_id:
                    raise FeishuWorkflowError(
                        f"目标账号 {account_binding.account_id} 属于店铺 "
                        f"{account_binding.store_id}，与任务选择的店铺 {row_store} 不一致；"
                        "请修正店铺或目标账号，不能自动借用其他店铺"
                    )
            if theme is not None and theme.get("native_multiway"):
                # 指定商品身份优先用产品编码；商品参考包（PRODUCT 模式）同样成立。
                if not product_id and not legacy_product:
                    raise FeishuWorkflowError(
                        "原生图文主题“一衣多穿”需要填写产品编码（同商品多搭配）；"
                        "旧图片视频一衣多穿请继续使用「TH｜一衣多穿｜轻文字」预设"
                    )
            # 内容表达：任务补充要求里显式写「实用指南/搭配灵感」时覆盖账号默认。
            expression_mode = ""
            expression_source = ""
            if account_binding is not None:
                expression_mode = str(
                    account_binding.profile.get("expression_mode") or "").strip()
                if expression_mode:
                    expression_source = "account_default"
            requirement_text = text_value(record.fields.get(FIELD_CONTENT_REQUIREMENT))
            if "实用指南" in requirement_text:
                expression_mode, expression_source = "PRACTICAL_GUIDE", "task"
            elif "搭配灵感" in requirement_text:
                expression_mode, expression_source = "STYLE_INSPIRATION", "task"
            account_brief: Optional[dict[str, Any]] = None
            if account_binding is not None:
                account_brief = {
                    "target_publish_account_id": account_binding.account_id,
                    "target_account_name": account_binding.account_name,
                    "account_store_id": account_binding.store_id,
                    "theme_source": theme_source,
                    "expression_mode": expression_mode,
                    "expression_source": expression_source,
                    "positioning": str(
                        account_binding.profile.get("positioning") or ""),
                    "visual_baseline": str(
                        account_binding.profile.get("visual_baseline") or ""),
                    "profile_fingerprint": account_binding.fingerprint,
                    "profile_source": account_binding.source,
                }
            # ---- 视觉预设（Phase 2）：本篇选择 → 账号默认 → 入口默认 ----
            # 入口默认只对账号绑定行生效；无绑定旧行不新增键，冻结请求不变。
            from services.visual_preset import (
                VisualPresetError, entry_default_preset_id, resolve_preset_snapshot,
            )
            try:
                visual_preset_snapshot = resolve_preset_snapshot(
                    task_value=text_value(record.fields.get(FIELD_VISUAL_PRESET)),
                    account_default=(
                        account_binding.profile.get("default_visual_preset")
                        if account_binding is not None else ""),
                    entry_preset_id=entry_default_preset_id(
                        str(preset.get("routing_policy") or "")),
                    entry_default_allowed=account_binding is not None,
                )
            except VisualPresetError as exc:
                raise FeishuWorkflowError(str(exc)) from exc
            if layering_flow:
                expected_theme = "THERMAL_TRANSITION" if thermal_transition_flow else "TEMPERATURE_DRESSING"
                if str((theme or {}).get("theme_key") or "") != expected_theme:
                    theme_label = "冷热切换" if thermal_transition_flow else "温度穿搭"
                    raise FeishuWorkflowError(
                        f"{flow_label}必须选择图文主题“{theme_label}”"
                    )
            reference_mode = ""
            reference_attachments = []
            product_context: dict[str, Any] = {}
            selected_reference_type = text_value(record.fields.get(FIELD_REFERENCE_TYPE))
            if layering_flow:
                role_field_values = sum((
                    list(record.fields.get(name) or []) for name in (
                        FIELD_LAYER_BASE_REFERENCE,
                        FIELD_LAYER_MID_REFERENCE,
                        FIELD_LAYER_OUTER_REFERENCE,
                    )
                ), [])
                if selected_reference_type == "完整穿搭":
                    if unified_attachments or legacy_complete:
                        raise FeishuWorkflowError(
                            f"{flow_label} COMPLETE_LOOK 请只填写基础层图/中间层图/外层图，不要混用通用参考图字段"
                        )
                    reference_mode = REFERENCE_MODE_COMPLETE_LOOK
                    reference_attachments = self._layering_role_attachments(
                        record.fields, label=flow_label,
                    )
                elif selected_reference_type == "风格参考":
                    if thermal_transition_flow:
                        raise FeishuWorkflowError(
                            "冷热切换 Phase 1 仅开放“完整穿搭”三张素材，尚未开放风格参考"
                        )
                    if role_field_values or legacy_complete:
                        raise FeishuWorkflowError(
                            "温度分层 STYLE 请只填写“参考图（可选）”，不要混用三张完整穿搭角色图"
                        )
                    if not unified_attachments:
                        raise FeishuWorkflowError("温度分层 STYLE 至少需要一张风格参考图")
                    reference_mode = REFERENCE_MODE_STYLE
                    reference_attachments = unified_attachments
                else:
                    supported = "“完整穿搭”" if thermal_transition_flow else "“完整穿搭”或“风格参考”"
                    raise FeishuWorkflowError(
                        f"{flow_label}必须显式选择{supported}，不支持自动判断/商品参考"
                    )
            else:
                # 非分层（旅行/通用）参考解析统一走公共门面；行为与旧内联分支逐字等价。
                # 分层流禁止商品编码（上方已校验），故商品快照与上下文恒为空。
                from services.photo_reference_context import resolve_photo_reference_context
                try:
                    reference_context = resolve_photo_reference_context(
                        selected_type=selected_reference_type,
                        unified_attachments=unified_attachments,
                        legacy_complete=legacy_complete,
                        legacy_product=legacy_product,
                        product_id=product_id,
                        required_roles=roles,
                        quantity=quantity,
                        account_id=specs[0].account_id,
                        record_id=record.record_id,
                        product_reference_resolver=self.product_reference_resolver,
                    )
                except ValueError as exc:
                    raise FeishuWorkflowError(str(exc)) from exc
                except ProductReferenceResolutionError as exc:
                    raise FeishuWorkflowError(
                        f"指定商品 {product_id} 缺少可用商品参考包：{exc}"
                    ) from exc
                reference_mode = reference_context.reference_mode
                reference_attachments = list(reference_context.reference_attachments)
                style_product = reference_context.product_snapshot
                product_context = reference_context.product_context
            # ---- 外部自动供稿门禁（Phase 1 来源分流）：付费生成前反查合同 ----
            source_tag = text_value(record.fields.get(FIELD_SOURCE_TAG))
            external_auto_supply = str(source_tag or "").startswith("auto_supply|")
            external_contract = assert_external_supply_wiring(
                source_tag=source_tag,
                record_id=record.record_id,
                reference_mode=reference_mode,
                product_id=text_value(record.fields.get(FIELD_PRODUCT)),
            )
            if (external_contract is not None and not reference_attachments
                    and reference_mode != "COMPLETE_LOOK"):
                # 方案 §6.5：外部合同零图行（narrative_only / 无可用细节页的
                # outfit_only）按 STYLE 零参考执行——规划器按内容计划规划、
                # 人物包出人物，参考只以文字化摘要进入内容要求。
                # 带指定商品时同样走 STYLE（STYLE+product 有商品快照路径）；
                # 旅行预设不支持 PRODUCT，外部行不得落入。
                from services.photo_reference import REFERENCE_MODE_STYLE
                reference_mode = REFERENCE_MODE_STYLE
            requires_product_supply = bool(
                recipe_for_input and (recipe_for_input.recipe_spec_json or {}).get("outfit_supply")
            )
            asset_status = text_value(record.fields.get(FIELD_PHOTO_ASSET_STATUS))
            if (requires_product_supply and not reference_attachments and not product_id
                    and asset_status not in {"已确认，正在生成", "已匹配可用素材"}
                    and not external_auto_supply):
                # 外部自动供稿零图行（narrative_only / 无细节页 outfit_only）
                # 由执行合同承载输入需求（方案 §6.5），不适用此手工输入校验
                raise FeishuWorkflowError(
                    "该图文预设需要填写产品编码或上传参考图"
                )
            from services.photo_batch_variation import plan_batch_variations
            from services.photo_content_planner import (
                ADDITIVE_CONTRACT_KEYS, LegacyPlanLocaleUpgrade, PhotoContentPlanStore,
                get_planning_flow, plan_th_choice_batch, recipe_has_planning_policy,
            )
            variation_theme = theme or {
                "theme_key": "AUTO", "label_zh": "自动差异化穿搭",
                "visual_brief": "保持同一商品或参考风格，变化场景、配色和穿搭组合",
            }
            staging_root = (Path(self.output_root) if self.output_root else
                            Path.home() / ".openclaw/shared/data/organic_photo_video")
            style_reference_paths: list[str] = []
            style_profile: dict[str, Any] = {}
            content_requirement = text_value(record.fields.get(FIELD_CONTENT_REQUIREMENT))
            travel_place = text_value(record.fields.get(FIELD_TRAVEL_PLACE))
            # Phase 1.2（§1.2）：自动行行字段缺地点时，从绑定冻结合同补齐。
            # 优先级：行显式字段 > 冻结合同 > 无（不能凭空发明）。
            if not travel_place and external_contract is not None:
                contract_dest = dict(external_contract.get("destination") or {})
                contract_place = str(contract_dest.get("place") or "").strip()
                if contract_place:
                    travel_place = contract_place
            # 统一旅行目的地（Phase 3）：国家 + 地点合并解析，冲突规划前报错；
            # 空值表示未指定——生成不得凭空标注真实国家/景点。
            try:
                travel_destination = resolve_travel_destination(
                    country=text_value(record.fields.get(FIELD_TRAVEL_COUNTRY)),
                    place=travel_place,
                )
            except ValueError as exc:
                raise FeishuWorkflowError(str(exc)) from exc
            travel_country = travel_destination.get("country") or ""
            travel_contract: dict[str, Any] = {}
            travel_variables: dict[str, Any] = {}
            travel_copy_templates = None
            travel_topic: dict[str, Any] = {}
            planning_flow = get_planning_flow(recipe_for_input.recipe_id) if recipe_for_input else ""
            # 主题是否必填：唯一归属在 ``theme_is_optional``（见其 docstring）。
            theme_optional = theme_is_optional(
                locale_pack=locale_pack, layering_flow=layering_flow,
                thermal_transition_flow=thermal_transition_flow,
                planning_flow=planning_flow,
            )
            theme_supplied = theme is not None
            # ``effective_theme`` 才是下游真正使用的主题：运营未选主题且本流程允许时，
            # 给一个中性主题（生成提示需要 label_zh，发布文案不依赖它），theme_key 留空
            # 表示「运营未选」。这样 ``theme`` 本身仍为 None，用于判定"是否用主题文案
            # 覆盖发布文案"（见下方 ``if theme_supplied``）。
            effective_theme = theme
            if theme is None and theme_optional:
                effective_theme = {**dict(variation_theme), "theme_key": ""}
            # 档位与文案都必须按「本任务的市场 + 发布语言」解析：国家无关配方的一
            # 个档位可以只服务某些市场，也可以只提供某些语言的已审核文案。
            from domain.photo_contracts import select_execution_profile
            from services.photo_locale import profile_copy_variants
            layering_copy_templates = None
            if layering_flow:
                recipe_spec_input = recipe_for_input.recipe_spec_json or {}
                # 分层图文按各自合同的主变量匹配 execution_profile：温度分层用
                # 温度档，冷热切换用切换场景。
                profile_variable_key = (
                    "transition_key" if thermal_transition_flow else "band_key"
                )
                profile_variable_value = str(
                    temperature_variables.get(profile_variable_key) or ""
                )
                profile_for_band = next(
                    (item for item in recipe_spec_input.get("execution_profiles") or []
                     if str((item.get("variables") or {}).get(profile_variable_key) or "")
                     == profile_variable_value),
                    None,
                )
                if not isinstance(profile_for_band, Mapping):
                    raise FeishuWorkflowError(
                        f"{flow_label}尚未配置 {profile_variable_value} profile"
                    )
                layering_copy_templates = profile_copy_variants(
                    profile_for_band, locale=publish_locale,
                )
            if planning_flow == "travel_two_step":
                recipe_spec_input = recipe_for_input.recipe_spec_json or {}
                travel_contract = dict(recipe_spec_input.get("travel_contract") or {})
                profiles_input = list(recipe_spec_input.get("execution_profiles") or [])
                # 档位按**本任务的市场**选，而不是取 profiles[0]。国家无关配方的
                # 档位可以声明 ``markets``：PHOTO_TRAVEL_OUTFIT_V3 的两档变量逐字
                # 相同、只有 asset_set_keys 不同（泰国档 / 越南档），取第一档会把
                # VN 任务绑到泰国的素材命名空间与泰语文案包上。未声明 markets 的
                # 档位对所有市场开放 ⇒ 既有 TH/MX 线路逐字不变。
                travel_profile = select_execution_profile(
                    profiles_input, market=str(specs[0].market or ""),
                ) or {}
                travel_variables = dict(travel_profile.get("variables") or {})
                # 文案语言跟着国家走：读**本次发布语言**的文案包，而不是
                # ``copy_variants``（loader 把它固定成按字母序第一个语言 = 泰语）。
                travel_copy_templates = profile_copy_variants(
                    travel_profile, locale=publish_locale,
                ) or None
                if theme and theme.get("travel_theme_type"):
                    travel_topic = build_travel_topic(
                        theme=theme, travel_place=travel_place,
                        travel_variables=travel_variables,
                        content_requirement=content_requirement,
                        fields=record.fields, recipe_spec=recipe_spec_input,
                    )
                    # 账号定位进入选题冻结：只有配置了目标账号的行才带这些键，
                    # travel_topic 本身会进内容计划 input_contract（hash 覆盖）。
                    if expression_mode:
                        travel_topic["expression_mode"] = expression_mode
                    if travel_country:
                        # 旅行国家进入选题冻结（additive：未填国家的历史行不变）
                        travel_topic["travel_country"] = travel_country
                    if account_brief and account_brief.get("positioning"):
                        travel_topic["account_positioning"] = str(
                            account_brief["positioning"])
                elif theme and theme.get("native_multiway"):
                    # 原生一衣多穿进入正式文案路径（2026-09-15 断点 A3）：
                    # 与六类旅行主题共用主题联动分支（模型生成完整发布文案），
                    # 地点可选（填了就进文案语境，不填不强制）；不再回落通用
                    # 四选一投票模板。legacy 视频一衣多穿不受影响。
                    travel_topic = {
                        "theme_type": "NATIVE_MULTIWAY",
                        "theme_version": 1,
                        "theme_label_zh": str(theme.get("label_zh") or ""),
                        "planning_focus": str(theme.get("visual_brief") or ""),
                        "place": travel_place,
                        "temperature_band": str(travel_variables.get("temperature_band") or ""),
                        "temperature_context": {
                            "value": str(travel_variables.get("temperature_band") or ""),
                            "source": "execution_profile",
                        },
                        "thai_fallback": {
                            "title": str(theme.get("title") or ""),
                            "cover": str(theme.get("cover") or ""),
                            "caption": str(theme.get("caption") or ""),
                            "hashtags": list(theme.get("hashtags") or []),
                            "cta": str(theme.get("cta") or ""),
                        },
                        "content_requirement": content_requirement,
                    }
                    if travel_country:
                        travel_topic["travel_country"] = travel_country
                    if expression_mode:
                        travel_topic["expression_mode"] = expression_mode
                    if account_brief and account_brief.get("positioning"):
                        travel_topic["account_positioning"] = str(
                            account_brief["positioning"])
            if reference_mode == REFERENCE_MODE_STYLE and recipe_for_input:
                if effective_theme is None:
                    raise FeishuWorkflowError("风格参考模式需要选择图文主题")
                from services.photo_asset_supply import PhotoAssetSupplyService
                from services.photo_reference_vision import PhotoReferenceVisionService
                style_reference_paths = PhotoAssetSupplyService(
                    self.client, root=staging_root,
                ).stage_reference_images(
                    record_id=record.record_id, attachments=reference_attachments,
                    reference_kind="style",
                    # 方案 §6.5：外部合同零页行无参考可下载
                    allow_empty=bool(external_contract is not None),
                )
                reference_vision = self.photo_reference_vision or PhotoReferenceVisionService(
                    root=staging_root
                )
                self.photo_reference_vision = reference_vision
                if planning_flow == "travel_two_step":
                    # 固定背景（Phase 3）：预设 background.mode=fixed 时旅行规划
                    # 改用固定背景合同；快照随 style_profile 供供给与 QA 识别。
                    fixed_background_payload = None
                    if visual_preset_snapshot is not None:
                        # 直接读冻结快照的有效配置；旧快照缺 background 键时
                        # 用显示名解析一次（窄兼容，不读最新文件覆盖新任务）。
                        _bg = dict(visual_preset_snapshot.get("background") or {})
                        if not _bg:
                            from services.visual_preset import resolve_visual_preset
                            _bg = dict(resolve_visual_preset(
                                visual_preset_snapshot.get("name"))["background"])
                        if str(_bg.get("mode") or "") == "fixed":
                            fixed_background_payload = _bg
                    self._write_fields(record.record_id, {FIELD_PROGRESS: PROGRESS_PLANNING})
                    def _plan_travel_reference():
                        analysis = reference_vision.analyze_reference(
                            record_id=record.record_id, paths=style_reference_paths,
                            allow_empty=not style_reference_paths,
                            theme=theme or variation_theme,
                            category_key=str((recipe_for_input.recipe_spec_json or {}).get("category_key") or preset.get("category_key") or ""),
                            content_requirement=content_requirement,
                            account_visual_baseline=str(
                                (account_brief or {}).get("visual_baseline") or ""),
                        )
                        plan = reference_vision.plan_travel_content(
                            record_id=record.record_id, analysis=analysis,
                            travel_contract=travel_contract, variables=travel_variables,
                            content_requirement=content_requirement, count=quantity,
                            travel_topic=travel_topic or None,
                            product_context=product_context,
                            reference_paths=style_reference_paths,
                            # B3：外部合同的 adoption 决定未标注参考图的
                            # 用途（不再一律三用途全补；手工行保持旧行为）
                            untagged_uses=(
                                [u for u in {
                                    "outfit_only": ["OUTFIT"],
                                    "visual_only": ["VISUAL_STYLE"],
                                    "narrative_only": [],
                                    "overall": ["OUTFIT"]}.get(
                                        str((external_contract or {}).get("adoption")
                                            or ""), ())
                                 # 固定背景屏蔽 ENVIRONMENT（§5.2）：
                                 # 相交入口会同步剥掉模型标注的该用途
                                 if not (fixed_background_payload
                                         and u == "ENVIRONMENT")]
                                if external_contract is not None else ()),
                            product_reference_paths=list(
                                style_product.get("reference_images") or []
                            ),
                            locale_pack=locale_pack,
                            account_positioning=str(
                                (account_brief or {}).get("positioning") or ""),
                            expression_mode=expression_mode,
                            account_visual_baseline=str(
                                (account_brief or {}).get("visual_baseline") or ""),
                            fixed_background=fixed_background_payload,
                        )
                        return analysis, plan

                    try:
                        reference_analysis, travel_plan = _plan_travel_reference()
                    except Exception as exc:
                        if not self._is_replannable_photo_error(exc):
                            raise
                        self._archive_photo_planning_state(
                            staging_root, record.record_id, reason=str(exc),
                        )
                        reference_analysis, travel_plan = _plan_travel_reference()
                    style_profile = reference_vision.build_travel_style_profile(
                        reference_analysis, travel_plan, count=quantity,
                    )
                    if visual_preset_snapshot is not None:
                        style_profile["visual_preset"] = dict(visual_preset_snapshot)
                    if travel_topic:
                        style_profile["travel_topic"] = dict(travel_topic)
                else:
                    style_profile = reference_vision.analyze(
                        record_id=record.record_id, paths=style_reference_paths,
                        theme=effective_theme or variation_theme,
                        category_key=str((recipe_for_input.recipe_spec_json or {}).get("category_key") or preset.get("category_key") or ""),
                        content_requirement=content_requirement, count=quantity,
                        product_context=product_context,
                        planning_flow=planning_flow, required_roles=roles,
                    )
                if product_context:
                    style_profile["product_context"] = dict(product_context)
            content_plan = None
            if (recipe_for_input
                    and recipe_has_planning_policy(recipe_for_input.recipe_id)
                    and effective_theme is not None
                    and reference_mode in {
                        REFERENCE_MODE_COMPLETE_LOOK, REFERENCE_MODE_PRODUCT,
                        REFERENCE_MODE_STYLE,
                    }):
                input_contract = {
                    "recipe_id": recipe_for_input.recipe_id,
                    "recipe_version": recipe_for_input.recipe_version,
                    "theme_key": str(effective_theme.get("theme_key") or ""),
                    "reference_mode": reference_mode,
                    "quantity": quantity,
                    "product_id": product_id,
                    "reference_tokens": [
                        str(item.get("file_token") or item.get("name") or "")
                        for item in reference_attachments if isinstance(item, Mapping)
                    ],
                    "style_profile": style_profile,
                    "content_requirement": content_requirement,
                    "travel_topic": travel_topic or {},
                    "travel_place": travel_place,
                    "temperature_variables": temperature_variables,
                }
                if recipe_locale_packs:
                    # 发布语言决定计划内容（文案模板来自语言文案包）⇒ 语言必须进
                    # 冻结契约。否则同一行改语言会静默复用旧语言的计划：契约比对
                    # 发现不了，VN 行会带着泰语文案一路走到发布契约才炸，而图片
                    # 已经付过费。只有声明了 ``locale_copy_packs`` 的国家无关配方
                    # 才带该键，v1 配方（泰语内联文案、与语言无关）契约逐字不变。
                    input_contract["publish_locale"] = publish_locale
                plan_store = PhotoContentPlanStore(staging_root)

                def _build_content_plan():
                    return plan_th_choice_batch(
                        record_id=record.record_id,
                        recipe_id=recipe_for_input.recipe_id,
                        theme=effective_theme, reference_mode=reference_mode, count=quantity,
                        style_profile=style_profile,
                        travel_contract=travel_contract or None,
                        copy_templates=(
                            layering_copy_templates
                            if planning_flow == "layering_two_step"
                            else travel_copy_templates
                        ),
                        required_roles=roles,
                        recipe_spec=recipe_for_input.recipe_spec_json or {},
                        variables=temperature_variables,
                        # Locale Pack 必须传下去：``_family_plan`` / ``_vision_plan``
                        # 都靠它取发布文案，漏传会回落到已被剥离的内联泰语字段
                        # （得到空串），再由主题文案兜成泰语（2026-09-14）。
                        locale_pack=locale_pack,
                    )

                def _load_content_plan(repair_copy=None):
                    return plan_store.load_or_create(
                        record_id=record.record_id, input_contract=input_contract,
                        create=_build_content_plan,
                        # 只有声明了语言文案包的配方才可能出现新增的 publish_locale：
                        # 老计划缺这个键时允许走「冻结文案自证语言」的窄兼容，
                        # v1 泰语配方（契约逐字不含该键）完全不受影响。
                        tolerate_additive_keys=(
                            ADDITIVE_CONTRACT_KEYS if recipe_locale_packs else ()
                        ),
                        repair_copy=repair_copy,
                    )

                # 只有本行的旧计划是「文案语言错」时才会有内容：它是供给层证明
                # 「只改了文案」的对照物（清单从不保存 variation）。
                former_variations: list[dict[str, Any]] = []
                try:
                    content_plan = _load_content_plan()
                except LegacyPlanLocaleUpgrade as exc:
                    # 旧计划的文案语言是错的，但源图仍可用。检测这一趟**不写任何
                    # 东西**：先把旧计划归档一份副本留证，再走第二趟只替换文案字段
                    # （``adopt_repaired_copy`` 会断言除文案外的画面字段逐字不变，
                    # 不一致就拒绝）。全程不搬 content_plan、不搬
                    # style_reference_supply —— 已付费素材原地复用，图片生成器
                    # 调用次数为 0。证不出语言一致（reason="unverified"）或画面
                    # 会变（reason="visual_changed"）时直接抛，什么都不动。
                    if exc.reason != "copy_language":
                        raise
                    former_payload = plan_store.read_frozen(record.record_id)
                    former_variations = list(
                        (former_payload.get("plan") or {}).get("items") or [])
                    self._archive_frozen_plan_copy(
                        staging_root, record.record_id, payload=former_payload,
                        reason=str(exc))
                    content_plan = _load_content_plan(repair_copy=_build_content_plan)
                except Exception as exc:
                    if not self._is_replannable_photo_error(exc):
                        raise
                    self._archive_photo_planning_state(
                        staging_root, record.record_id, reason=str(exc),
                        include_reference=False,
                    )
                    content_plan = _load_content_plan()
                variations = list(content_plan["items"])
            else:
                variations = plan_batch_variations(
                    record_id=record.record_id, theme=variation_theme, count=quantity,
                )
            if reference_mode == REFERENCE_MODE_COMPLETE_LOOK and content_plan is None:
                variations = [
                    {
                        "variation_id": f"complete_look_set_{index}", "index": index,
                        "theme_key": str(variation_theme.get("theme_key") or ""),
                        "angle_zh": f"完整穿搭素材第 {index} 组",
                        "scene_zh": "沿用上传素材", "palette_zh": "沿用上传素材",
                        "style_modifier": "不得改写上传的完整穿搭事实",
                    }
                    for index in range(1, quantity + 1)
                ]
            # 发布契约预检（2026-09-13）：必须在**任何**付费素材生成之前完成。
            # 全部变体文案此刻已定稿（含 COMPLETE_LOOK 合成项），而下方各分支
            # 会真调视觉模型并逐张计费——文案不合规要在这里就拦下。
            self._assert_planned_copy_contract(variations)
            pinned_asset_set_ids: list[str] = []
            prepared_source_groups: list[list[dict[str, Any]]] = []
            if (reference_mode == REFERENCE_MODE_COMPLETE_LOOK and recipe_for_input
                    and asset_supply_must_run(asset_status, record.fields)):
                if external_auto_supply:
                    # 兜底（正常在外层门禁已拦）：外部参考绝不做原图直用登记
                    raise FeishuWorkflowError(
                        "AUTO_SUPPLY_WIRING：外部 reference_only 参考禁止登记为"
                        "完整穿搭资产（原图直用）")
                recipe = recipe_for_input
                from services.photo_asset_supply import PhotoAssetSupplyService
                asset_supply = PhotoAssetSupplyService(self.client, root=staging_root)
                for index in range(quantity):
                    item_id = f"{record.record_id}_item_{index + 1}"
                    begin, end = index * len(roles), (index + 1) * len(roles)
                    variation = variations[index]
                    profile_binding = dict(variation.get("profile_binding") or {})
                    staged = asset_supply.stage(
                        record_id=item_id, attachments=reference_attachments[begin:end],
                        required_roles=roles,
                        metadata={
                            "theme_key": str((theme or {}).get("theme_key") or ""),
                            "reference_mode": reference_mode,
                            "batch_variation": variation,
                            "profile_binding": profile_binding,
                        },
                    )
                    prepared_source_groups.append(list(staged["files"]))
                    approval_attributes = None
                    approval_evidence = None
                    reviewer = "feishu_operator_execution"
                    reviewer_type = "technical"
                    if layering_flow:
                        from services.photo_reference_vision import PhotoReferenceVisionService
                        reference_vision = self.photo_reference_vision or PhotoReferenceVisionService(
                            root=staging_root
                        )
                        self.photo_reference_vision = reference_vision
                        self._write_fields(record.record_id, {FIELD_PROGRESS: PROGRESS_QA})
                        if thermal_transition_flow:
                            approval_evidence = reference_vision.review_thermal_transition_pages(
                                reference_paths=(),
                                look_plans=list(variation.get("looks") or []),
                                image_paths=[str(item["path"]) for item in staged["files"]],
                                thermal_transition_contract=dict(
                                    (recipe.recipe_spec_json or {}).get(
                                        "thermal_transition_contract"
                                    ) or {}
                                ),
                                profile_binding=profile_binding,
                            )
                            from services.photo_thermal_transition_qa import (
                                thermal_transition_qa_note_zh,
                            )
                            qa_note = thermal_transition_qa_note_zh(approval_evidence)
                            semantic_reviewer = "system_thermal_transition_semantic_qa"
                        else:
                            approval_evidence = reference_vision.review_layering_pages(
                                reference_paths=(),
                                look_plans=list(variation.get("looks") or []),
                                image_paths=[str(item["path"]) for item in staged["files"]],
                                layering_contract=dict(
                                    (recipe.recipe_spec_json or {}).get("layering_contract") or {}
                                ),
                                profile_binding=profile_binding,
                            )
                            from services.photo_layering_report import layering_qa_note_zh
                            qa_note = layering_qa_note_zh(approval_evidence)
                            semantic_reviewer = "system_layering_semantic_qa"
                        self._write_fields(record.record_id, {
                            FIELD_NOTES: qa_note,
                        })
                        if not approval_evidence.get("passed"):
                            failure_codes = sorted({
                                str(code)
                                for page in approval_evidence.get("roles") or []
                                for code in page.get("failure_codes") or []
                                if str(code)
                            })
                            raise FeishuWorkflowError(
                                "LAYER_SOURCE_INCONSISTENT：完整穿搭三态未通过分层证据校验（"
                                + "、".join(failure_codes or ["证据不足"])
                                + "）"
                            )
                        approval_attributes = layering_approval_attributes(approval_evidence)
                        reviewer = semantic_reviewer
                        reviewer_type = "model"
                    saved = asset_supply.qualify(
                        record_id=item_id, recipe=recipe, repository=self.repository,
                        reviewer=reviewer, reviewer_type=reviewer_type,
                        source="feishu_complete_look_input",
                        profile_binding=profile_binding,
                        approval_attributes=approval_attributes,
                        approval_evidence=approval_evidence,
                        **_agnostic_asset_binding(recipe),
                    )
                    pinned_asset_set_ids.append(saved.asset_set_id)
            if (reference_mode == REFERENCE_MODE_STYLE and recipe_for_input
                    and asset_supply_must_run(asset_status, record.fields)):
                if effective_theme is None:
                    raise FeishuWorkflowError("风格参考模式需要选择图文主题")
                recipe = recipe_for_input
                from services.photo_asset_supply import PhotoAssetSupplyService
                from services.photo_style_reference_supply import PhotoStyleReferenceSupplyService
                asset_supply = PhotoAssetSupplyService(self.client, root=staging_root)
                # 重拍 Look 只在已有冻结批次的行生效（见 _retake_photo_supply_roles）；
                # 首跑路径忽略该字段，避免误填导致首跑失败。
                paths = style_reference_paths
                account = self.repository.get_account_profile(specs[0].account_id)
                if account is None:
                    raise FeishuWorkflowError("图文生产账号不存在")
                if style_profile.get("presentation_type") == "FLAT_LAY":
                    persona = {}
                else:
                    if not getattr(account, "persona_ref_id", None):
                        raise FeishuWorkflowError("真人参考模式需要图文生产账号绑定人物模板")
                    persona = LightTryonAssetReader().get_persona(account.persona_ref_id)
                for index, variation in enumerate(variations, 1):
                    item_id = f"{record.record_id}_item_{index}"
                    self._write_fields(record.record_id, {
                        FIELD_PROGRESS: PROGRESS_PREPARING_ASSETS,
                    })
                    asset_counter = {"done": 0}
                    total_assets = len(variations) * len(roles)

                    def _asset_progress(event: str, **data: Any) -> None:
                        if event == "asset_generated":
                            asset_counter["done"] += 1
                            self._write_fields(record.record_id, {
                                FIELD_PROGRESS: f"素材生成 {asset_counter['done']}/{total_assets}",
                            })
                        elif event == "qa_started":
                            self._write_fields(record.record_id, {FIELD_PROGRESS: PROGRESS_QA})
                        elif event == "human_qa_started":
                            self._write_fields(record.record_id, {
                                FIELD_PROGRESS: "人物表现质检中",
                                FIELD_REVIEW_STAGE: "人物表现质检",
                            })
                        elif event == "repair_scheduled":
                            note = str(data.get("notes") or "")
                            self._write_fields(record.record_id, {
                                FIELD_PROGRESS: PROGRESS_REPAIR,
                                **({
                                    FIELD_NOTES: (
                                        f"人物质检修复 {len(data.get('roles') or [])} 张：{note}"[:180]
                                        if data.get("reason") == "human_presentation"
                                        else f"风格质检修复 {len(data.get('roles') or [])} 张"[:180]
                                    )
                                } if note or data.get("reason") else {}),
                            })

                    supply_service = PhotoStyleReferenceSupplyService(
                        generator=self.generator, root=staging_root,
                        vision_service=self.photo_reference_vision,
                    )

                    def _prepare_style_sources():
                        return supply_service.prepare(
                            record_id=item_id, reference_paths=paths,
                            allow_empty_references=not paths,
                            theme=effective_theme or variation_theme,
                            account=account, persona=persona, variation=variation,
                            progress=_asset_progress, product=style_product,
                            locale=str(getattr(specs[0], "language", "") or "th-TH"),
                            # 本行刚从「文案语言错」就地重建过时，给出旧计划的同一条目：
                            # 供给侧据此证明这次差异只在文案，直接复用已付费图片。
                            copy_repaired_from=(
                                former_variations[index - 1]
                                if index <= len(former_variations) else None
                            ),
                        )

                    try:
                        prepared = _prepare_style_sources()
                    except Exception as exc:
                        if not self._is_replannable_photo_error(exc):
                            raise
                        self._archive_photo_planning_state(
                            staging_root, record.record_id, reason=str(exc),
                            include_reference=False, include_content_plan=False,
                            supply_item_ids=[item_id],
                        )
                        prepared = _prepare_style_sources()
                    if recipe.recipe_id.startswith("PHOTO_TH_TRAVEL"):
                        variation["cover_selection"] = {
                            "role": "look_a",
                            "source": "fixed_first_look",
                            "reason_zh": "第一套穿搭直接承担首图，不另选并重复一张素材",
                        }
                    from services.photo_content_check import validate_prepared_sources
                    validate_prepared_sources(
                        variation, prepared["sources"], required_roles=roles,
                    )
                    quality_summary = str(
                        (prepared.get("quality") or {}).get("quality_summary_zh") or ""
                    )
                    if quality_summary:
                        # 非阻塞质量提示：并入最终完成摘要展示，不影响生成与发布。
                        self._photo_quality_summaries[record.record_id] = quality_summary
                    prepared_source_groups.append(list(prepared["sources"]))
                    asset_supply.stage_existing(
                        record_id=item_id, sources=prepared["sources"], required_roles=roles,
                        metadata={"theme_key": str((effective_theme or {}).get("theme_key") or ""),
                                  "reference_mode": reference_mode,
                                  "product_id": str(style_product.get("product_id") or ""),
                                  "product_reference_pack_id": str(
                                      style_product.get("reference_pack_id") or ""
                                  ),
                                  "batch_variation": variation},
                    )
                    style_approval_evidence = dict(
                        (prepared.get("group_alignment") or {}).get("layering_qa") or {}
                    ) if layering_flow else None
                    saved = asset_supply.qualify(
                        record_id=item_id, recipe=recipe, repository=self.repository,
                        reviewer="system_style_reference_generation", reviewer_type="technical",
                        source="feishu_style_reference_generated",
                        profile_binding=dict(variation.get("profile_binding") or {}),
                        **_agnostic_asset_binding(recipe),
                        approval_attributes=(
                            layering_approval_attributes(style_approval_evidence)
                            if style_approval_evidence else None
                        ),
                        approval_evidence=(
                            style_approval_evidence or dict(prepared.get("quality") or {})
                        ),
                    )
                    pinned_asset_set_ids.append(saved.asset_set_id)
            if (reference_mode == REFERENCE_MODE_PRODUCT and recipe_for_input
                    and asset_supply_must_run(asset_status, record.fields)):
                recipe = recipe_for_input
                if recipe is None or recipe.status != "active":
                    raise FeishuWorkflowError("图文 Recipe 不存在或已停用")
                from services.photo_asset_supply import PhotoAssetSupplyService
                from services.photo_outfit_supply import PhotoOutfitSupplyService
                asset_supply = PhotoAssetSupplyService(self.client, root=staging_root)
                if reference_attachments:
                    references = asset_supply.stage_product_references(
                        record_id=record.record_id, attachments=reference_attachments,
                    )
                    effective_product_id = product_id or f"FEISHU_PHOTO_{record.record_id}"
                    variant_key = f"photo_{record.record_id}"[:96]
                    self.product_reference_resolver.build_pack(
                        product_id=effective_product_id, product_name=effective_product_id,
                        category="outerwear", references=references, variant_key=variant_key,
                        is_default=not bool(product_id), source_type="feishu_photo_product_input",
                        source_ref=record.record_id, persist=True,
                    )
                    product = self.product_reference_resolver.resolve_snapshot(
                        effective_product_id, selection_key=record.record_id,
                        variant_key=variant_key, account_id=specs[0].account_id,
                    )
                    product_id = effective_product_id
                else:
                    product = self.product_reference_resolver.resolve_snapshot(
                        product_id, selection_key=record.record_id,
                        account_id=specs[0].account_id,
                    )
                source_reader = getattr(self.repository, "list_reusable_outfit_sources", None)
                existing_sources = (source_reader(product_id, limit=30)
                                    if callable(source_reader) else [])
                account = self.repository.get_account_profile(specs[0].account_id)
                if account is None:
                    raise FeishuWorkflowError("图文生产账号不存在")
                remaining_existing_sources = list(existing_sources)
                for index, variation in enumerate(variations, 1):
                    item_id = f"{record.record_id}_item_{index}"
                    prepared = PhotoOutfitSupplyService(
                        generator=self.generator, asset_reader=LightTryonAssetReader(), root=staging_root,
                    ).prepare(
                        record_id=item_id, recipe=recipe, product=product,
                        account=account,
                        existing_sources=remaining_existing_sources,
                        variation=variation,
                    )
                    if variation.get("looks"):
                        from services.photo_content_check import validate_prepared_sources
                        validate_prepared_sources(variation, prepared["sources"])
                    prepared_source_groups.append(list(prepared["sources"]))
                    reused_refs = {
                        str(item.get("look_ref") or "")
                        for item in prepared["sources"]
                        if item.get("source_kind") == "existing_outfit"
                    }
                    if reused_refs:
                        remaining_existing_sources = [
                            item for item in remaining_existing_sources
                            if str(item.get("look_ref") or "") not in reused_refs
                        ]
                    item_roles = [item["role"] for item in prepared["role_plan"]]
                    asset_supply.stage_existing(
                        record_id=item_id, sources=prepared["sources"], required_roles=item_roles,
                        metadata={"theme_key": str(variation_theme.get("theme_key") or ""),
                                  "reference_mode": reference_mode,
                                  "batch_variation": variation},
                    )
                    saved = asset_supply.qualify(
                        record_id=item_id, recipe=recipe, repository=self.repository,
                        reviewer="system_product_outfit_generation", reviewer_type="technical",
                        source="feishu_product_outfit_generated",
                        **_agnostic_asset_binding(recipe),
                    )
                    pinned_asset_set_ids.append(saved.asset_set_id)
            if len(prepared_source_groups) > 1:
                from services.photo_content_check import validate_batch_sources
                validate_batch_sources(
                    prepared_source_groups, required_roles=roles,
                    planning_flow=planning_flow,
                )
            if recipe_for_input and recipe_for_input.recipe_id.startswith("PHOTO_TH_TRAVEL"):
                for variation in variations:
                    variation["cover_selection"] = {
                        "role": "look_a",
                        "source": "fixed_first_look",
                        "reason_zh": "第一套穿搭直接承担首图，不另选并重复一张素材",
                    }
                if content_plan is not None:
                    content_plan["plan_sha256"] = fingerprint({
                        key: value for key, value in content_plan.items()
                        if key != "plan_sha256"
                    })
            if pinned_asset_set_ids:
                if len(pinned_asset_set_ids) != quantity:
                    raise FeishuWorkflowError("批次素材集数量与生成篇数不一致")
                base_overrides = list(overrides or [{} for _ in pinned_asset_set_ids])
                overrides = []
                for item, asset_set_id, variation in zip(
                        base_overrides, pinned_asset_set_ids, variations):
                    binding = dict(variation.get("profile_binding") or {})
                    override = {**dict(item), "asset_set_id": asset_set_id}
                    if binding:
                        override.setdefault("profile_id", str(binding.get("profile_id") or ""))
                        override["variables"] = {
                            **dict(binding.get("variables") or {}),
                            **dict(override.get("variables") or {}),
                        }
                    overrides.append(override)
            try:
                requests = PhotoRequestFactory(self.repository, layouts=load_board_layouts()).build_batch(
                    record_id=record.record_id, specs=specs,
                    category_key=str(preset.get("category_key") or ""),
                    product_mode=str(preset.get("default_product_mode") or "NO_PRODUCT"),
                    overrides=overrides,
                )
                if style_product:
                    for request in requests:
                        request["product_id"] = str(style_product.get("product_id") or product_id)
                        request["product_snapshot"] = dict(style_product)
                        request["request_sha256"] = fingerprint({
                            key: value for key, value in request.items()
                            if key != "request_sha256"
                        })
                        validate_frozen_request(request)
                if theme or reference_mode:
                    for index, request in enumerate(requests):
                        frozen_manifest = request["asset_snapshot"].get("manifest_json") or {}
                        if isinstance(frozen_manifest, str):
                            frozen_manifest = json.loads(frozen_manifest)
                        if theme_supplied:
                            request["copy"] = build_theme_copy(
                                theme, frozen_manifest.get("assets") or [],
                                variations[index],
                                locale=publish_locale or "th-TH",
                                locale_pack=locale_pack,
                                variation_index=index + 1,
                                expression_mode=expression_mode,
                            )
                        request["theme_brief"] = {
                            **dict(variation_theme), "reference_mode": reference_mode,
                            "reference_count": len(reference_attachments),
                            "batch_variation": variations[index],
                            **({
                                "travel_theme_type": str(theme.get("travel_theme_type") or ""),
                                "travel_theme_version": int(theme.get("travel_theme_version") or 1),
                                "place": str(travel_topic.get("place") or travel_place or ""),
                                "topic_zh": str(variations[index].get("topic_zh") or ""),
                                "temperature_context": dict(travel_topic.get("temperature_context") or {}),
                                **({"travel_country": str(travel_topic.get("travel_country") or travel_country or "")}
                                   if (travel_topic.get("travel_country") or travel_country) else {}),
                            } if theme and theme.get("travel_theme_type") else {}),
                        }
                        if account_brief is not None:
                            # 本篇实际使用的账号配置快照：主题来源、表达模式、
                            # 视觉基准与目标账号一起冻结，续跑/重拍只读冻结值。
                            request["theme_brief"]["account"] = dict(account_brief)
                            request["target_publish_account_id"] = str(
                                account_brief["target_publish_account_id"])
                        if visual_preset_snapshot is not None:
                            # 视觉预设快照（Phase 2）：身份/版本/背景方式/指纹
                            # 进冻结请求；排版与背景执行在 Phase 3 消费。
                            request["theme_brief"]["visual_preset"] = dict(
                                visual_preset_snapshot)
                        if visual_preset_snapshot is not None:
                            # 排版接线（2026-09-15 排版轮 B2）：预设声明
                            # structured_v1 时按背景类型替换布局模板；未知
                            # family 显式报错，不静默沿用旧 PHOTO_TRAVEL_CARD。
                            _layout_family = str(
                                (visual_preset_snapshot.get("layout") or {})
                                .get("layout_family") or "")
                            if _layout_family == "structured_v1":
                                from services.visual_preset import structured_layout_for
                                _layout_id = structured_layout_for(
                                    (visual_preset_snapshot.get("background") or {})
                                    .get("kind"))
                                request["layout_snapshot"] = json.loads(
                                    (Path(__file__).resolve().parents[1]
                                     / "config" / "layouts" / f"{_layout_id}.json")
                                    .read_text(encoding="utf-8"))
                            elif _layout_family:
                                raise FeishuWorkflowError(
                                    f"尚未支持的排版 family：{_layout_family}")
                        request["request_sha256"] = fingerprint({
                            key: value for key, value in request.items() if key != "request_sha256"
                        })
                        validate_frozen_request(request)
                # Theme copy is authored as cover + A/B/C/D. For travel output,
                # Look A itself becomes the cover, so remove its duplicate detail
                # page only after all theme/product mutations are frozen.
                requests = [apply_travel_single_cover(request) for request in requests]
            except Exception as exc:
                from services.asset_set_service import AssetSetError
                attachments = self._complete_look_attachments(record.fields)
                if not attachments and (isinstance(exc, AssetSetError) or "NEEDS_CONTENT" in str(exc)):
                    product_id = text_value(record.fields.get(FIELD_PRODUCT))
                    source_reader = getattr(self.repository, "list_reusable_outfit_sources", None)
                    recipe = self.repository.get_content_recipe(specs[0].recipe_id) if len(specs) == 1 else None
                    roles = list((((recipe.recipe_spec_json or {}).get("asset_requirements") or {}).get("required_roles") or [])) if recipe else []
                    sources = (source_reader(product_id, limit=max(4, len(roles)))
                               if product_id and roles and callable(source_reader) else [])
                    if len(sources) >= len(roles) and roles:
                        from services.photo_asset_supply import PhotoAssetSupplyService
                        staging_root = (Path(self.output_root) if self.output_root else
                                        Path.home() / ".openclaw/shared/data/organic_photo_video")
                        staged = PhotoAssetSupplyService(self.client, root=staging_root).stage_existing(
                            record_id=record.record_id, sources=sources, required_roles=roles,
                        )
                        summary = (
                            f"已从产品 {product_id} 的旧穿搭任务选出 {len(roles)} 套不同 Look，"
                            "按 A/B/C/D 暂存。请确认图片满足当前主题后，将审核改为“通过”；"
                            "确认前不会入库、生成或发布。"
                        )
                        self._write_fields(record.record_id, {
                            FIELD_EXECUTE: False, FIELD_PROGRESS: PROGRESS_ACTION,
                            FIELD_REVIEW: REVIEW_NOT_REQUIRED, FIELD_REVIEW_STAGE: "素材已准备",
                            FIELD_PHOTO_ASSET_STATUS: "待内容审核", FIELD_PHOTO_SUMMARY: summary,
                            FIELD_NOTES: "来源为旧穿搭任务的已选中、技术通过图片；勾选确认发布后继续生成并入队。",
                        })
                        return {"record_id": record.record_id, "action": "stage_existing_outfit_assets",
                                "staging_manifest": staged["manifest_path"], "photo_count": len(staged["files"])}
                    raise FeishuWorkflowError(
                        f"{exc}；可从现有穿搭模板流程生成完整造型图后，"
                        "在“图文参考图”按 A/B/C/D 顺序上传。系统不会要求运营填写 JSON。"
                    ) from exc
                raise
            manifest = {
                "schema_version": "opv-photo-batch-v1", "workflow_version": 2,
                "media_kind": "native_photo", "preset": preset_name,
                "entries": [{"spec": asdict(spec), "request": request,
                             "source_record_id": f"{record.record_id}:{index}:{spec.recipe_id}:1"}
                            for index, (spec, request) in enumerate(zip(specs, requests), 1)],
            }
            if content_plan is not None:
                manifest["content_plan"] = content_plan
            manifest["manifest_sha256"] = fingerprint(manifest)
            batch = self.repository.create_production_batch_idempotent(ProductionBatch(
                batch_id="opv_batch_" + fingerprint(record.record_id)[:32],
                source_record_id=record.record_id, expected_count=quantity,
                manifest_json=manifest,
            ))
        return self._produce_native_photo_entries(record, batch)

    def _produce_native_photo_entries(self, record, batch) -> Dict[str, Any]:
        """Shared frozen-batch production tail (first run and every resume).

        Behavior-preserving extraction: the per-entry verify → intake → plan
        → produce → export → upload loop is identical for TH and MX; all MX
        semantics arrive via the frozen request (validate dispatch) and the
        planner dispatch inside ``plan_task``.
        """
        from services.photo_planner import PhotoReusePlannerService
        from services.photo_package import NativePhotoProductionFlow
        from services.photo_request_factory import PhotoRequestFactory, validate_frozen_request
        from services.task_intake import TaskIntakeService, TaskRequest
        self._claim_batch(batch)
        entries = self._photo_batch_entries(batch)
        expected_sources = {entry["source_record_id"] for entry in entries}
        if any(task.source_record_id not in expected_sources for task in self._tasks(record.record_id)):
            raise FeishuWorkflowError("该行存在冻结批次以外的任务，禁止混组")
        requests = [entry["request"] for entry in entries]
        summary = PhotoRequestFactory.summary(requests)
        # 本篇实际使用的账号配置（运营核对入口，不新增确认步骤）：目标账号、
        # 主题来源、表达模式与视觉基准都来自冻结快照。
        frozen_account = dict(
            ((requests[0].get("theme_brief") or {}).get("account") or {})
            if requests else {},
        )
        if frozen_account.get("target_publish_account_id"):
            expression_labels = {
                "STYLE_INSPIRATION": "搭配灵感", "PRACTICAL_GUIDE": "实用指南",
            }
            preset_source_labels = {
                "task": "任务选择", "account_default": "账号默认",
                "entry_default": "入口默认",
            }
            theme_label = str(
                (requests[0].get("theme_brief") or {}).get("label_zh") or "")
            preset_snapshot = dict(
                (requests[0].get("theme_brief") or {}).get("visual_preset") or {})
            if preset_snapshot:
                preset_segment = (
                    "；视觉预设 {name}（{mode}，来源：{source}）".format(
                        name=preset_snapshot.get("name"),
                        mode=preset_snapshot.get("background_mode"),
                        source=preset_source_labels.get(
                            str(preset_snapshot.get("source") or ""), "入口默认"),
                    ))
            else:
                preset_segment = ""
            # 旅行目的地采用值与来源（Phase 3）：表格自动填入的国家也让运营可见，
            # 避免被误认为无效默认项。
            dest_country = text_value(record.fields.get(FIELD_TRAVEL_COUNTRY))
            dest_place = text_value(record.fields.get(FIELD_TRAVEL_PLACE))
            destination_parts = [x for x in (dest_country, dest_place) if x]
            destination_segment = (
                "；旅行目的地 {}（来源：任务字段）".format("、".join(destination_parts))
                if destination_parts else "")
            # 外部自动供稿行的采用方式与参考来源（Phase 2 可追溯）
            external_segment = ""
            if str(text_value(record.fields.get(FIELD_SOURCE_TAG)) or "").startswith("auto_supply|"):
                try:
                    _contract = _external_contract_store().find_by_record(record.record_id)
                except Exception:  # noqa: BLE001
                    _contract = None
                if _contract:
                    external_segment = (
                        "；外部参考 采用 {adoption}｜主参考 {note}｜策略 {version}".format(
                            adoption=_contract.get("adoption") or "—",
                            note=_contract.get("main_note_id") or "—",
                            version=_contract.get("policy_version") or "—"))
            summary = (
                "本篇配置：目标账号 {account}（店铺 {store}）；主题 {theme}"
                "（来源：{source}）；表达 {expression}；视觉基准 {baseline}"
                "{preset_segment}{destination_segment}{external_segment}\n{summary}"
            ).format(
                account=frozen_account.get("target_publish_account_id"),
                store=frozen_account.get("account_store_id") or "未记录",
                theme=theme_label or "未选",
                source={
                    "task": "任务选择", "account_default": "账号默认",
                }.get(str(frozen_account.get("theme_source") or ""), "原预设"),
                expression=expression_labels.get(
                    str(frozen_account.get("expression_mode") or ""), "默认"),
                baseline=str(frozen_account.get("visual_baseline") or "无"),
                preset_segment=preset_segment,
                destination_segment=destination_segment,
                external_segment=external_segment,
                summary=summary,
            )
        frozen_content_plan = (batch.manifest_json or {}).get("content_plan")
        if frozen_content_plan:
            from services.photo_content_planner import summarize_batch_plan
            summary += "\n具体内容计划：\n" + summarize_batch_plan(frozen_content_plan)
        output_root = (Path(self.output_root) if self.output_root else
                       Path.home() / ".openclaw/shared/data/organic_photo_video/photo_packages")
        producer = HeroFirstProducer(
            self.repository, self.generator, output_root=output_root,
            asset_readiness_gate=self.asset_readiness_gate, technical_only=True,
        )
        retake_roles = parse_retake_roles(text_value(record.fields.get(FIELD_RETAKE_LOOK)))
        if retake_roles:
            # staging_root 只在首跑分支里定义；重拍发生在批次复用路径，需要自取。
            retake_staging_root = (Path(self.output_root) if self.output_root else
                                   Path.home() / ".openclaw/shared/data/organic_photo_video")
            if batch_is_mx_wig_choice(batch.manifest_json or {}):
                # Explicit MX dispatch: frozen wig batches retake through the
                # wig supply adapter; the look_x tokens map to hair_x there.
                self._retake_mx_wig_roles(
                    record, batch, entries, retake_roles, retake_staging_root, producer)
            else:
                self._retake_photo_supply_roles(
                    record, batch, entries, retake_roles, retake_staging_root, producer)
        self._write_fields(record.record_id, {
            FIELD_EXECUTE: False, FIELD_PROGRESS: PROGRESS_RUNNING,
            FIELD_REVIEW: REVIEW_PENDING, FIELD_NOTES: "", FIELD_PHOTO_SUMMARY: summary,
            FIELD_PHOTO_ASSET_STATUS: "已匹配可用素材",
        })
        task_ids, paths, failures = [], [], []
        final_copies: List[tuple] = []
        # 提速（2026-09-15）：中文翻译随成片产出即时启动，与后续任务的生图/
        # QA 及附件上传并行；仍按文案指纹缓存，失败只降级为占位提示。
        from concurrent.futures import ThreadPoolExecutor
        from services.copy_translation import (
            CopyTranslationError, CopyTranslationService,
        )
        _zh_root = (Path(self.output_root).parent if self.output_root
                    else Path.home() / ".openclaw/shared/data/organic_photo_video")

        def _translate_one(copy_block, locale):
            try:
                return ("ok", CopyTranslationService(root=_zh_root).translate(
                    copy_block, source_locale=locale))
            except CopyTranslationError as exc:
                return ("error", str(exc))
            except Exception as exc:  # 翻译链路异常不得影响成片交付
                return ("error", str(exc))

        _zh_executor = ThreadPoolExecutor(max_workers=1)
        _zh_futures: List = []
        for entry in entries:
            if self._run_lease:
                self._run_lease.check()
            item = entry["request"]
            validate_frozen_request(item)
            source_record_id = entry["source_record_id"]
            try:
                found = [task for task in self._tasks(record.record_id) if task.source_record_id == source_record_id]
                if len(found) > 1:
                    raise FeishuWorkflowError("冻结条目存在重复任务，禁止继续")
                task = found[0] if found else TaskIntakeService(self.repository).create_task(TaskRequest(
                    account_id=item["account_id"],
                    product_id=(str(item.get("product_id") or "") or None),
                    product_snapshot=dict(item.get("product_snapshot") or {}),
                    media_kind="native_photo", category_key=item["category_key"],
                    product_mode=item["product_mode"], source_type=SOURCE_TYPE,
                    source_record_id=source_record_id, feishu_record_id=record.record_id,
                    requested_shot_count=(
                        len((item.get("content_card") or {}).get("pages") or []) or 5
                    ), created_by="feishu_opv_photo", idempotency_key=source_record_id,
                    target_publish_account_id=str(
                        item.get("target_publish_account_id") or ""),
                )).task
                if task.target_country != item["market"] or task.target_locale != item["locale"]:
                    raise FeishuWorkflowError("生产账号市场/语言已变化，与冻结批次不一致")
                if task.task_status == "draft":
                    PhotoReusePlannerService(self.repository).plan_task(
                        task.task_id, recipe_id=item["recipe_id"], variables=item["variables"],
                        copy_block=item["copy"], layout=item["layout_snapshot"],
                        asset_set_id=item["asset_set_id"], recipe_snapshot=item["recipe_snapshot"],
                        asset_snapshot=item["asset_snapshot"], execution_profile_id=item["profile_id"],
                        copy_variant_id=item["copy_variant_id"], content_card=item.get("content_card"),
                        theme_brief=item.get("theme_brief"), operator="feishu_opv_photo",
                    )
                flow = NativePhotoProductionFlow(
                    self.repository, producer, output_root=output_root,
                    vision_service=getattr(self, "photo_reference_vision", None),
                )
                result = flow.prepare(task.task_id, template=item["layout_snapshot"])
                manifest = result.get("photo_manifest") or {}
                expected_pages = int(getattr(task, "requested_shot_count", 0) or 5)
                if len(manifest.get("slides") or []) != expected_pages:
                    raise FeishuWorkflowError(f"图文任务 {task.task_id} 没有完整成品页")
                task_ids.append(task.task_id)
                paths.extend(str(slide["path"]) for slide in manifest["slides"])
                final_copy = dict(manifest.get("copy") or {})
                if final_copy:
                    locale = str(item.get("locale") or "th-TH")
                    final_copies.append((final_copy, locale))
                    _zh_futures.append(_zh_executor.submit(
                        _translate_one, final_copy, locale))
            except Exception as exc:
                failures.append(f"{source_record_id}: {exc}")
        attachments = self._upload_files(paths, parent_type="bitable_image") if paths else []
        completion_fields = {
            FIELD_PROGRESS: PROGRESS_ACTION if failures else PROGRESS_DONE,
            FIELD_OUTPUT: attachments, FIELD_REVIEW: REVIEW_NOT_REQUIRED,
            FIELD_REVIEW_STAGE: "处理中" if failures else "技术完成", FIELD_PHOTO_SUMMARY: summary,
            FIELD_FAILURE_REASON: None,
            FIELD_NOTES: (f"已完成 {len(task_ids)}/{batch.expected_count} 篇原生图文，共 {len(paths)} 张；"
                          + ("勾选执行后沿用冻结方案补齐。" + "；".join(failures) if failures
                             else "技术检查已通过；勾选确认发布后冻结当前成品并进入发布队列。")
                          + "｜" + self._photo_quality_summaries.get(record.record_id, "")
                          )[:1500],
        }
        # 完整中文文案（最终成片包口径）：翻译已在循环内并行完成/在途，
        # 这里与上传汇合；失败只降级为占位提示，不影响成片与发布。
        full_copy_zh = ""
        try:
            from services.copy_translation import compose_full_copy_zh
            parts = []
            for future in _zh_futures:
                status, payload = future.result()
                if status == "ok":
                    parts.append(compose_full_copy_zh(
                        payload, set_index=len(parts) + 1,
                        total_sets=len(_zh_futures)))
                else:
                    parts.append(f"（第 {len(parts) + 1} 套中文翻译待补跑：{payload}；"
                                 "重新执行该行或运行 scripts/backfill_copy_zh.py 可补齐）")
            if parts:
                full_copy_zh = "\n\n".join(parts)
        finally:
            _zh_executor.shutdown(wait=True)
        if full_copy_zh:
            completion_fields[FIELD_FULL_COPY_ZH] = full_copy_zh
        self._write_fields(record.record_id, completion_fields)
        return {"record_id": record.record_id, "action": "generate_native_photo",
                "task_ids": task_ids, "photo_count": len(paths), "failures": failures}

    def _compose_full_copy_zh(self, final_copies: List[tuple]) -> str:
        if not final_copies:
            return ""
        from services.copy_translation import (
            CopyTranslationError, CopyTranslationService, compose_full_copy_zh,
        )
        root = (Path(self.output_root).parent if self.output_root
                else Path.home() / ".openclaw/shared/data/organic_photo_video")
        service = CopyTranslationService(root=root)
        parts: List[str] = []
        try:
            for index, (copy_block, locale) in enumerate(final_copies, 1):
                translation = service.translate(copy_block, source_locale=locale)
                parts.append(compose_full_copy_zh(
                    translation, set_index=index, total_sets=len(final_copies)))
            return "\n\n".join(parts)
        except CopyTranslationError as exc:
            if parts:
                return "\n\n".join(parts) + f"\n\n（其余套中文翻译待补跑：{exc}）"
            return (f"（中文翻译待补跑：{exc}；不影响成片与发布；"
                    "重新执行该行或运行 scripts/backfill_copy_zh.py 可补齐）")
        except Exception as exc:  # 翻译链路任何异常都不得影响成片交付
            return f"（中文翻译待补跑：{exc}；不影响成片与发布）"

    def _mx_wig_recipe_for_preset(self, preset_name: str):
        """Return the wig recipe only when the preset's tasks point at the
        exact ``mx_wig_choice_v1`` recipe; everything else returns None."""
        try:
            metadata = self.catalog.metadata(preset_name)
        except FeishuWorkflowError:
            return None
        if metadata.get("media_kind") != "native_photo":
            return None
        recipe_ids = sorted({
            str(task.get("recipe_id") or "")
            for task in metadata.get("tasks") or []
        })
        for recipe_id in recipe_ids:
            try:
                recipe = self.repository.get_content_recipe(recipe_id)
            except Exception:  # noqa: BLE001 - probe must never raise
                recipe = None
            if recipe_is_mx_wig_choice(recipe):
                return recipe
        return None

    def _archive_mx_wig_planning_state(self, staging_root: Path, record_id: str,
                                       reason: str) -> None:
        """Move stale wig planning state aside so a row can be replanned once."""
        supply_paths = sorted(
            (staging_root / "wig_choice_supply").glob(f"{record_id}_item_*")
        ) if (staging_root / "wig_choice_supply").exists() else []
        self._archive_photo_planning_state(
            staging_root, record_id, reason=reason,
            include_reference=True, include_content_plan=True,
            supply_item_ids=[],
        )
        if not supply_paths:
            return
        from datetime import datetime as _dt
        stamp = _dt.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        archive = staging_root / "replan_archive" / f"{record_id}_{stamp}"
        moved = []
        for source in supply_paths:
            destination = archive / "wig_choice_supply" / source.name
            destination.parent.mkdir(parents=True, exist_ok=True)
            source.rename(destination)
            moved.append(str(destination.relative_to(archive)))
        (archive / "replan.json").write_text(json.dumps({
            "record_id": record_id, "flow": MX_WIG_CHOICE_FLOW,
            "archived_at": _dt.now(timezone.utc).isoformat(),
            "reason": reason, "moved": moved,
        }, ensure_ascii=False, indent=2), encoding="utf-8")

    def _generate_mx_wig_photo(self, record, preset_name: str,
                               quantity: int) -> Dict[str, Any]:
        """``mx_wig_choice_v1`` first run: plan → generate → freeze → produce.

        全程不进入 TH 主题解析/服装供给：主题在 MX 模块解析，参考图按发型
        灵感/环境/风格处理，产品编码在付费生成前显式拒绝并保留原值。
        """
        if text_value(record.fields.get(FIELD_TARGET_ACCOUNT)):
            raise FeishuWorkflowError(
                "MX 假发线暂不支持目标账号定位；目标账号只用于 TH 原生图文任务"
            )
        from config.loader import load_board_layouts
        from domain.models import ProductionBatch
        from dataclasses import asdict
        from services.photo_asset_supply import PhotoAssetSupplyService
        from services.photo_reference_vision import PhotoReferenceVisionService
        from services.photo_request_factory import (
            PhotoRequestFactory, fingerprint,
        )
        from services.photo_wig_planner import (
            reference_roles_from_plan, resolve_mx_wig_theme,
            summarize_mx_wig_plan,
        )
        from services.photo_wig_qa import WigGroupQaReviewer, check_wig_batch_sources
        from services.photo_wig_supply import PhotoWigSupplyService

        specs = self.catalog.resolve_batch(preset_name, record.record_id, quantity)
        if any(not spec.account_id for spec in specs):
            raise FeishuWorkflowError("该图文预设尚未绑定 OPV 生产账号")
        preset = self.catalog.metadata(preset_name)
        product_id = text_value(record.fields.get(FIELD_PRODUCT))
        if product_id:
            raise FeishuWorkflowError(
                "本预设生成四款发型灵感；单款商品模式暂未开放，"
                "请清空产品编码或使用后续单品预设。（已保留你填写的产品编码，未自动清除）"
            )
        unified_attachments = list(record.fields.get(FIELD_REFERENCE) or [])
        legacy_complete = self._complete_look_attachments(record.fields)
        reference_attachments = unified_attachments or legacy_complete
        theme = resolve_mx_wig_theme(text_value(record.fields.get(FIELD_CONTENT_THEME)))
        content_requirement = text_value(record.fields.get(FIELD_CONTENT_REQUIREMENT))
        staging_root = (Path(self.output_root) if self.output_root else
                        Path.home() / ".openclaw/shared/data/organic_photo_video")
        account = self.repository.get_account_profile(specs[0].account_id)
        if account is None:
            raise FeishuWorkflowError("图文生产账号不存在")
        if not getattr(account, "persona_ref_id", None):
            raise FeishuWorkflowError("MX 假发预设需要生产账号绑定人物主参考（OPV_MX_PHOTO_001 → MX_WIG_CAST_A_001）")
        persona = LightTryonAssetReader().get_persona(account.persona_ref_id)
        persona_paths = [
            str(item.get("local_path"))
            for item in persona.get("reference_items") or []
            if item.get("approved", True) and item.get("role") in {"FACE_FRONT_NEUTRAL", "FACE_THREE_QUARTER"}
        ]
        if not persona_paths:
            raise FeishuWorkflowError("绑定人物缺少已批准的脸部主参考，无法锁定身份")

        self._write_fields(record.record_id, {FIELD_PROGRESS: PROGRESS_PLANNING})
        reference_paths: list[str] = []
        if reference_attachments:
            reference_paths = PhotoAssetSupplyService(
                self.client, root=staging_root,
            ).stage_reference_images(
                record_id=record.record_id, attachments=reference_attachments,
                reference_kind="style",
            )
        reference_vision = self.photo_reference_vision or PhotoReferenceVisionService(
            root=staging_root
        )
        self.photo_reference_vision = reference_vision

        def _plan_wig_content():
            return reference_vision.plan_wig_choice_content(
                record_id=record.record_id, persona_paths=persona_paths,
                reference_paths=reference_paths,
                content_requirement=content_requirement, count=quantity,
                theme_label_zh=str(theme.get("label_zh") or ""),
            )

        try:
            wig_plan = _plan_wig_content()
        except Exception as exc:
            if not self._is_replannable_photo_error(exc):
                raise
            self._archive_mx_wig_planning_state(staging_root, record.record_id, reason=str(exc))
            wig_plan = _plan_wig_content()
        reviewer = WigGroupQaReviewer(reference_vision)
        for plan_item in wig_plan["items"]:
            copy_review = reviewer.review_copy(plan_item=plan_item)
            if not copy_review.get("passed"):
                raise FeishuWorkflowError(
                    "西语文案审校未通过："
                    + "；".join(copy_review.get("issues") or ["语义不合格"])
                    + "。请调整内容要求后重新执行。"
                )
        pinned_asset_set_ids: list[str] = []
        prepared_source_groups: list[list[dict[str, Any]]] = []
        supply = PhotoWigSupplyService(
            generator=self.generator, root=staging_root, qa_reviewer=reviewer,
        )
        total_assets = len(wig_plan["items"]) * 4
        asset_counter = {"done": 0}

        def _asset_progress(event: str, **data: Any) -> None:
            if event == "asset_generated":
                asset_counter["done"] += 1
                self._write_fields(record.record_id, {
                    FIELD_PROGRESS: f"素材生成 {asset_counter['done']}/{total_assets}",
                })
            elif event == "qa_started":
                self._write_fields(record.record_id, {FIELD_PROGRESS: PROGRESS_QA})
            elif event == "repair_scheduled":
                self._write_fields(record.record_id, {
                    FIELD_PROGRESS: PROGRESS_REPAIR,
                    FIELD_NOTES: f"假发组级质检修复 {'、'.join(data.get('roles') or [])}"[:180],
                })

        for index, plan_item in enumerate(wig_plan["items"], 1):
            item_id = f"{record.record_id}_item_{index}"
            self._write_fields(record.record_id, {FIELD_PROGRESS: PROGRESS_PREPARING_ASSETS})
            prepared = supply.prepare(
                record_id=item_id, plan_item=plan_item, persona=persona,
                reference_paths=reference_paths,
                reference_roles=reference_roles_from_plan(
                    plan_item, persona_paths, reference_paths),
                content_requirement=content_requirement,
                progress=_asset_progress,
            )
            prepared_source_groups.append(list(prepared["sources"]))
            asset_set = supply.register_asset_set(
                repository=self.repository, record_id=item_id,
                sources=prepared["sources"], plan_item=plan_item, persona=persona,
            )
            pinned_asset_set_ids.append(asset_set.asset_set_id)
        check_wig_batch_sources(prepared_source_groups)
        overrides = [
            {"asset_set_id": asset_set_id, "wig_plan_item": plan_item}
            for asset_set_id, plan_item in zip(pinned_asset_set_ids, wig_plan["items"])
        ]
        requests = PhotoRequestFactory(
            self.repository, layouts=load_board_layouts(),
        ).build_batch(
            record_id=record.record_id, specs=specs,
            category_key=str(preset.get("category_key") or ""),
            product_mode=str(preset.get("default_product_mode") or "NO_PRODUCT"),
            overrides=overrides,
        )
        summary = PhotoRequestFactory.summary(requests) + "\n" + summarize_mx_wig_plan(wig_plan)
        manifest = {
            "schema_version": "opv-photo-batch-v1", "workflow_version": 2,
            "media_kind": "native_photo", "preset": preset_name,
            "execution_flow": MX_WIG_CHOICE_FLOW,
            "theme_key": str(theme.get("theme_key") or ""),
            "wig_plan": wig_plan,
            "entries": [{"spec": asdict(spec), "request": request,
                         "source_record_id": f"{record.record_id}:{index}:{spec.recipe_id}:1"}
                        for index, (spec, request) in enumerate(zip(specs, requests), 1)],
        }
        manifest["manifest_sha256"] = fingerprint(manifest)
        batch = self.repository.create_production_batch_idempotent(ProductionBatch(
            batch_id="opv_batch_" + fingerprint(record.record_id)[:32],
            source_record_id=record.record_id, expected_count=quantity,
            manifest_json=manifest,
        ))
        return self._produce_native_photo_entries(record, batch)

    def _retake_mx_wig_roles(self, record, batch, entries, retake_roles,
                             staging_root, producer) -> None:
        """运营手动重拍（MX）：只重生指定发型素材，再按新素材换绑对应页面。

        ``重拍 Look`` 列继续由运营填 A/B/C/D；``parse_retake_roles`` 返回的
        look_x 在本 MX 适配层内映射为 hair_x，不修改全局 parser。重拍沿用
        revision/rework 流程只替换对应资产，发布冻结版本不可原地覆盖。
        """
        from domain import statuses
        from services.photo_reference_vision import PhotoReferenceVisionService
        from services.photo_request_factory import fingerprint
        from services.photo_wig_qa import WigGroupQaReviewer
        from services.photo_wig_supply import PhotoWigSupplyService
        from services.release_gate import assert_main_queue_rework_allowed
        from services.workflow_v2 import ReworkService

        hair_roles = [f"hair_{str(role).split('_', 1)[1].lower()}" for role in retake_roles]
        roles_label = "、".join(role.split("_")[1].upper() for role in retake_roles)
        self._write_fields(record.record_id, {
            FIELD_PROGRESS: PROGRESS_RUNNING,
            FIELD_NOTES: f"重拍发型 {roles_label}：正在重生指定素材…",
        })
        vision = self.photo_reference_vision or PhotoReferenceVisionService(root=staging_root)
        self.photo_reference_vision = vision
        supply = PhotoWigSupplyService(
            generator=self.generator, root=staging_root,
            qa_reviewer=WigGroupQaReviewer(vision),
        )
        wig_plan = (batch.manifest_json or {}).get("wig_plan") or {}
        content_items = list(wig_plan.get("items") or [])
        task_by_source = {
            task.source_record_id: task for task in self._tasks(record.record_id)
        }
        default_slots = {"hair_a": 1, "hair_b": 2, "hair_c": 3, "hair_d": 4}
        for index, entry in enumerate(entries, 1):
            item_id = f"{record.record_id}_item_{index}"
            manifest = supply.load_manifest(item_id)
            if manifest is None:
                raise FeishuWorkflowError(
                    f"第 {index} 篇没有假发供给清单；只有 mx_wig_choice_v1 生成的行支持重拍发型")
            plan_item = content_items[index - 1] if index <= len(content_items) else None
            if plan_item is None:
                raise FeishuWorkflowError(f"第 {index} 篇缺少冻结发型计划，无法重拍")
            account = self.repository.get_account_profile(entry["request"]["account_id"])
            if account is None:
                raise FeishuWorkflowError("图文生产账号不存在")
            persona = LightTryonAssetReader().get_persona(account.persona_ref_id)
            reference_paths = [str(value) for value in manifest.get("reference_paths") or []]
            missing = [value for value in reference_paths if not Path(value).is_file()]
            if missing:
                raise FeishuWorkflowError("参考原图缺失，无法按原参考重拍：" + "、".join(missing))
            expected_hash = str(manifest.get("input_hash") or "")
            if expected_hash and supply.input_hash(
                    plan_item=plan_item, persona=persona,
                    reference_paths=reference_paths,
                    content_requirement=str(manifest.get("content_requirement") or ""),
                    channel_params=dict(manifest.get("channel_params") or {})) != expected_hash:
                raise FeishuWorkflowError(
                    f"第 {index} 篇的人物/参考图/计划与冻结供给不一致，已停止重拍")
            to_retire = [role for role in hair_roles if role in default_slots]
            supply.regenerate_roles(item_id, to_retire, reason="运营手动重拍")
            self._write_fields(record.record_id, {
                FIELD_PROGRESS: f"素材重拍 {index}/{len(entries)} 篇",
            })
            prepared = supply.prepare(
                record_id=item_id, plan_item=plan_item, persona=persona,
                reference_paths=reference_paths,
                reference_roles=dict(manifest.get("reference_roles") or {}),
                content_requirement=str(manifest.get("content_requirement") or ""),
                channel_params=dict(manifest.get("channel_params") or {}),
            )
            new_sources = {
                str(item.get("role")): item for item in prepared.get("sources") or []
            }
            # manifest 是重生成本地快照：其中的 sources 即重拍前的旧源。
            old_sources = {
                str(item.get("role")): item
                for item in manifest.get("sources") or []
            }
            # 生成文件名是确定性的（同名覆盖），必须比内容哈希而不是路径。
            changed_roles = sorted(
                role for role in to_retire
                if str((new_sources.get(role) or {}).get("sha256") or "")
                != str((old_sources.get(role) or {}).get("sha256") or "")
            )
            if not changed_roles:
                raise FeishuWorkflowError(
                    f"第 {index} 篇重拍后素材内容没有变化（生成通道可能返回了相同结果）；请重新勾选执行重试")
            task = task_by_source.get(entry["source_record_id"])
            if task is None:
                raise FeishuWorkflowError(f"第 {index} 篇任务不存在，无法换绑重拍素材")
            assert_main_queue_rework_allowed(task.task_id)
            current = self.repository.get_task(task.task_id) or task
            if current.task_status == statuses.TASK_PHOTO_READY:
                current = self.repository.transition_task(
                    task.task_id, statuses.TASK_PHOTO_READY, statuses.TASK_PHOTO_PACKAGING)
            if current.task_status != statuses.TASK_PHOTO_PACKAGING:
                raise FeishuWorkflowError(
                    f"第 {index} 篇任务状态为 {current.task_status}；"
                    "仅已完成图文支持重拍发型，已进入发布队列的请先处理排程")
            self.repository.transition_task(
                task.task_id, statuses.TASK_PHOTO_PACKAGING, statuses.TASK_IMAGE_REVIEW)
            revision = self.repository.get_task_revision(current.active_revision_id)
            if revision is None:
                raise FeishuWorkflowError(f"第 {index} 篇任务缺少活动修订版，无法重拍")
            plan_shots = ((revision.plan_snapshot_json or {}).get("plan") or {}).get("shots") or []
            patch: dict[int, dict[str, str]] = {}
            for role in changed_roles:
                new_item = new_sources[role]
                prior_path = str((old_sources.get(role) or {}).get("path") or "")
                slot = next(
                    (int(shot.get("slot_index") or 0) for shot in plan_shots
                     if prior_path and shot.get("asset_path") == prior_path),
                    0,
                ) or default_slots.get(role, 0)
                if not slot:
                    raise FeishuWorkflowError(
                        f"第 {index} 篇冻结计划里找不到 {role} 对应页面，已停止换绑")
                patch[slot] = {
                    "asset_path": str(new_item.get("path")),
                    "asset_sha256": str(new_item.get("sha256")),
                }
            ReworkService(self.repository).begin(
                task.task_id, expected_revision_id=revision.revision_id,
                expected_lock_version=revision.lock_version,
                scope="photo_source", plan_patch=patch,
                reason=f"运营手动重拍发型 {roles_label}",
                idempotency_key=fingerprint({
                    "record": record.record_id, "revision": revision.revision_id,
                    "flow": MX_WIG_CHOICE_FLOW, "roles": to_retire,
                }),
                operator="feishu_operator_retake",
            )
            report = producer.produce(task.task_id)
            if report.task_status not in {
                statuses.TASK_IMAGE_REVIEW, statuses.TASK_PHOTO_PACKAGING,
                statuses.TASK_PHOTO_READY,
            }:
                raise FeishuWorkflowError(
                    f"第 {index} 篇重拍后任务状态异常：{report.task_status}")
        self._write_fields(record.record_id, {FIELD_RETAKE_LOOK: ""})

    def _assert_planned_copy_contract(self, variations: Sequence[Mapping[str, Any]]) -> None:
        """Normalize then reject a planned copy that breaks the publish contract.

        Runs before the paid asset stage.  Freeze-time validation
        (``validate_frozen_request``) happens *after* four billed generations, so
        a 90-UTF-16 title overflow used to cost a complete row's image budget
        (2026-09-13: TH 旅行线实测，title 91 单元）。 Machine-authored copy is
        clamped here so a resumed run carrying a previously frozen copy also
        becomes publishable; anything normalization cannot fix (hashtag shape,
        an oversized hashtag block) still fails loudly — but for free.
        """
        from domain.photo_contracts import (
            normalize_publish_copy, planned_copy_contract_errors,
        )
        for index, variation in enumerate(variations, 1):
            copy_block = variation.get("copy")
            if not isinstance(copy_block, Mapping) or not copy_block:
                continue
            normalized = normalize_publish_copy(copy_block)
            if normalized != copy_block:
                variation["copy"] = normalized
            errors = planned_copy_contract_errors(normalized)
            if errors:
                raise FeishuWorkflowError(
                    f"第 {index} 篇文案不符合发布契约（未开始付费生图）："
                    + "；".join(sorted(set(errors))))

    def _retake_photo_supply_roles(self, record, batch, entries, retake_roles,
                                   staging_root, producer) -> None:
        """运营手动重拍：只重生指定 look 素材，再按新素材重建受影响页面。

        供给清单断点续跑保证其余 look 不重复生成、不重复计费；页面通过
        V2 photo_source 返工修订换绑新文件，未重拍槽位沿用已选素材。
        仅支持风格参考生成、且尚未进入发布管线的已完成图文。
        """
        from domain import statuses
        from services.photo_style_reference_supply import PhotoStyleReferenceSupplyService
        from services.photo_theme import resolve_photo_theme
        from services.photo_request_factory import fingerprint
        from services.release_gate import assert_main_queue_rework_allowed
        from services.workflow_v2 import ReworkService
        roles_label = "、".join(role.split("_")[1].upper() for role in retake_roles)
        self._write_fields(record.record_id, {
            FIELD_PROGRESS: PROGRESS_RUNNING,
            FIELD_NOTES: f"重拍 Look {roles_label}：正在重生指定素材…",
        })
        supply_service = PhotoStyleReferenceSupplyService(
            generator=self.generator, root=staging_root,
            vision_service=self.photo_reference_vision,
        )
        # 表格主题只在「真实改题」时才有权威性（见 ``retake_theme``）：无主题首跑
        # 行的表格列一直是空的，重拍必须从供给清单恢复首跑真正用的那个。
        table_theme = resolve_photo_theme(text_value(record.fields.get(FIELD_CONTENT_THEME)))
        content_items = list(
            ((batch.manifest_json or {}).get("content_plan") or {}).get("items") or [])
        task_by_source = {
            task.source_record_id: task for task in self._tasks(record.record_id)
        }
        for index, entry in enumerate(entries, 1):
            item_request = entry["request"]
            item_id = f"{record.record_id}_item_{index}"
            item_dir = staging_root / "style_reference_supply" / item_id
            manifest_path = item_dir / "supply_manifest.json"
            if not manifest_path.is_file():
                raise FeishuWorkflowError(
                    f"第 {index} 篇没有素材供给清单；只有风格参考模式生成的行支持重拍 Look")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            item_theme = retake_theme(table_theme, manifest)
            old_sources = {
                str(item.get("role") or ""): item for item in manifest.get("sources") or []
            }
            # 角色可能已被此前的质检修复摘除（sources 里暂时没有）——这正是
            # 断点续跑要重生的情况，不能当作填写错误拦截。
            # 注意循环变量不能叫 entry：外层 entry 是批次条目，遮蔽会在
            # 后面的 task_by_source 取值处炸 KeyError。
            retired_sources = {}
            for attempt in manifest.get("attempt_history") or []:
                for item in attempt.get("retired") or []:
                    role = str(item.get("role") or "")
                    if role and role not in retired_sources:
                        retired_sources[role] = item
            reference_paths = [str(value) for value in manifest.get("style_reference_paths") or []]
            missing = [value for value in reference_paths if not Path(value).is_file()]
            if missing:
                raise FeishuWorkflowError(
                    "风格参考原图缺失，无法按原参考重拍：" + "、".join(missing))
            variation = content_items[index - 1] if index <= len(content_items) else {}
            account = self.repository.get_account_profile(item_request["account_id"])
            if account is None:
                raise FeishuWorkflowError("图文生产账号不存在")
            persona = {}
            if getattr(account, "persona_ref_id", None):
                persona = LightTryonAssetReader().get_persona(account.persona_ref_id)
            product = {}
            if item_request.get("product_id"):
                product = self.product_reference_resolver.resolve_snapshot(
                    item_request["product_id"], selection_key=record.record_id,
                    account_id=item_request["account_id"],
                )
            expected_hash = str(manifest.get("input_hash") or "")
            if expected_hash and supply_service.input_fingerprint(
                    reference_paths, item_theme, variation, account, product) != expected_hash:
                # 冻结内容计划可能在供给之后被补写（如旅行 cover_selection 兜底），
                # 组合 hash 无法直接复现；逐组件核对参考图/主题/穿搭/人物后重定基线。
                supply_service.verify_and_rebaseline_identity(
                    item_dir=item_dir, paths=reference_paths, theme=item_theme,
                    account=account, variation=variation, persona=persona, product=product)
            # 幂等：上次重拍中断时目标角色可能已被摘除，直接续跑重生即可。
            to_retire = [role for role in retake_roles if role in old_sources]
            if to_retire:
                supply_service.regenerate_roles(
                    item_dir=item_dir, roles=to_retire, reason="运营手动重拍")
            self._write_fields(record.record_id, {
                FIELD_PROGRESS: f"素材重拍 {index}/{len(entries)} 篇",
            })
            prepared = supply_service.prepare(
                record_id=item_id, reference_paths=reference_paths, theme=item_theme,
                account=account, persona=persona, variation=variation, product=product,
                # 发布语言走冻结请求自己记下的那一个（首跑按 ``specs[0].language``
                # 写入），重拍不重读预设，避免与首跑不一致。
                locale=str(item_request.get("locale") or "th-TH"),
            )
            new_sources = {
                str(item.get("role") or ""): item for item in prepared.get("sources") or []
            }
            # 生成文件名是确定性的（同名覆盖），必须比内容哈希而不是路径。
            changed_roles = sorted(
                role for role, item in new_sources.items()
                if str(item.get("sha256") or "") != str(
                    (old_sources.get(role) or {}).get("sha256") or "")
            )
            if not changed_roles:
                raise FeishuWorkflowError(
                    f"第 {index} 篇重拍后素材内容没有变化（生成通道可能返回了相同结果）；请重新勾选执行重试")
            task = task_by_source.get(entry["source_record_id"])
            if task is None:
                raise FeishuWorkflowError(f"第 {index} 篇任务不存在，无法换绑重拍素材")
            assert_main_queue_rework_allowed(task.task_id)
            current = self.repository.get_task(task.task_id) or task
            if current.task_status == statuses.TASK_PHOTO_READY:
                current = self.repository.transition_task(
                    task.task_id, statuses.TASK_PHOTO_READY, statuses.TASK_PHOTO_PACKAGING)
            if current.task_status != statuses.TASK_PHOTO_PACKAGING:
                raise FeishuWorkflowError(
                    f"第 {index} 篇任务状态为 {current.task_status}；"
                    "仅已完成图文支持重拍 Look，已进入发布队列的请先处理排程")
            self.repository.transition_task(
                task.task_id, statuses.TASK_PHOTO_PACKAGING, statuses.TASK_IMAGE_REVIEW)
            revision = self.repository.get_task_revision(current.active_revision_id)
            if revision is None:
                raise FeishuWorkflowError(f"第 {index} 篇任务缺少活动修订版，无法重拍")
            plan_shots = ((revision.plan_snapshot_json or {}).get("plan") or {}).get("shots") or []
            patch: dict[int, dict[str, str]] = {}
            default_slots = {"look_a": 1, "look_b": 2, "look_c": 3, "look_d": 4}
            for role in changed_roles:
                new_item = new_sources[role]
                prior_path = (str((old_sources.get(role) or {}).get("path") or "")
                              or str((retired_sources.get(role) or {}).get("path") or ""))
                slot = next(
                    (int(shot.get("slot_index") or 0) for shot in plan_shots
                     if prior_path and shot.get("asset_path") == prior_path),
                    0,
                ) or default_slots.get(role, 0)
                if not slot:
                    raise FeishuWorkflowError(
                        f"第 {index} 篇冻结计划里找不到 {role} 对应页面，已停止换绑")
                patch[slot] = {
                    "asset_path": str(new_item.get("path")),
                    "asset_sha256": str(new_item.get("sha256")),
                }
            ReworkService(self.repository).begin(
                task.task_id, expected_revision_id=revision.revision_id,
                expected_lock_version=revision.lock_version,
                scope="photo_source", plan_patch=patch,
                reason=f"运营手动重拍 {roles_label}",
                idempotency_key=fingerprint({
                    "record": record.record_id, "revision": revision.revision_id,
                    "roles": retake_roles,
                }),
                operator="feishu_operator_retake",
            )
            report = producer.produce(task.task_id)
            if report.task_status not in {
                statuses.TASK_IMAGE_REVIEW, statuses.TASK_PHOTO_PACKAGING,
                statuses.TASK_PHOTO_READY,
            }:
                raise FeishuWorkflowError(
                    f"第 {index} 篇重拍后任务状态异常：{report.task_status}")
        self._write_fields(record.record_id, {FIELD_RETAKE_LOOK: ""})

    def _approve_staged_photo_assets(self, record) -> Dict[str, Any]:
        """Turn an ordered upload into reusable inventory, then produce its package."""
        if self._batch(record.record_id) or self._tasks(record.record_id):
            raise FeishuWorkflowError("已有冻结任务时不能改用暂存素材")
        preset_name = text_value(record.fields.get(FIELD_PRESET))
        preset = self.catalog.metadata(preset_name)
        if preset.get("media_kind") != "native_photo":
            raise FeishuWorkflowError("暂存素材审核只适用于原生图文预设")
        if task_quantity(record.fields) != 1:
            raise FeishuWorkflowError("人工上传素材首版一次只审核并生成 1 篇")
        spec = self.catalog.resolve_batch(preset_name, record.record_id, 1)[0]
        recipe = self.repository.get_content_recipe(spec.recipe_id)
        if recipe is None or recipe.status != "active":
            raise FeishuWorkflowError("图文 Recipe 不存在或已停用")
        from services.photo_asset_supply import PhotoAssetSupplyService
        staging_root = (Path(self.output_root) if self.output_root else
                        Path.home() / ".openclaw/shared/data/organic_photo_video")
        saved = PhotoAssetSupplyService(self.client, root=staging_root).qualify(
            record_id=record.record_id, recipe=recipe, repository=self.repository,
        )
        # Pin this run to the exact human-confirmed upload without asking the
        # operator to maintain the compatibility JSON field. The pin is written
        # to Feishu as well: keeping it only in memory made the row
        # unrecoverable whenever _generate failed after the status write below
        # (素材状态 already says "已确认" ⇒ the next run skips the asset section,
        # yet nothing pins an asset set ⇒ NEEDS_CONTENT from the content pool).
        pinned_payload = json.dumps({"asset_set_id": saved.asset_set_id})
        self._write_fields(record.record_id, {
            FIELD_REVIEW: REVIEW_NOT_REQUIRED, FIELD_PHOTO_ASSET_STATUS: "已确认，正在生成",
            FIELD_PHOTO_REQUEST: pinned_payload,
            FIELD_NOTES: f"已按确认发布冻结素材集 {saved.asset_set_id} V{saved.asset_set_version}；开始生成原生图文。",
        })
        record.fields[FIELD_PHOTO_ASSET_STATUS] = "已确认，正在生成"
        record.fields[FIELD_REVIEW] = REVIEW_PENDING
        record.fields[FIELD_PHOTO_REQUEST] = pinned_payload
        result = self._generate(record)
        result["qualified_asset_set_id"] = saved.asset_set_id
        return result

    @staticmethod
    def _photo_batch_entries(batch):
        from services.photo_request_factory import fingerprint, validate_frozen_request
        manifest = batch.manifest_json
        unsigned = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
        if (manifest.get("schema_version") != "opv-photo-batch-v1"
                or manifest.get("manifest_sha256") != fingerprint(unsigned)):
            raise FeishuWorkflowError("冻结图文批次指纹损坏，禁止继续")
        entries = manifest.get("entries") or []
        if not entries or len(entries) != batch.expected_count:
            raise FeishuWorkflowError("冻结批次清单数量损坏，禁止创建或放行")
        sources = []
        for index, entry in enumerate(entries, 1):
            request = entry["request"]
            validate_frozen_request(request)
            expected = f"{batch.source_record_id}:{index}:{request['recipe_id']}:1"
            if entry.get("source_record_id") != expected:
                raise FeishuWorkflowError("冻结图文批次条目身份损坏")
            sources.append(expected)
        if len(set(sources)) != batch.expected_count:
            raise FeishuWorkflowError("冻结图文批次包含重复条目")
        return entries

    @staticmethod
    def _production_plan_note(tasks) -> str:
        entries = []
        for index, task in enumerate(tasks, 1):
            plan = task.plan_json or {}
            if str(getattr(task, "media_kind", "video") or "video") == "native_photo":
                entries.append(f"第{index}条：{len(plan.get('slides') or [])}张原生图文/无视频渲染")
                continue
            shots = plan.get("shots") or []
            sequence = plan.get("outfit_sequence") or []
            # BASE/FINAL may be different wearing states of the same outfit.
            fingerprints = {state.get("outfit_fingerprint") for state in (plan.get("outfit_states") or {}).values()
                            if state.get("outfit_fingerprint")}
            looks = len(sequence) or len(fingerprints) or None
            seconds = sum(int(s.get("duration_ms") or 0) for s in shots) / 1000
            count = f"{looks}套穿搭" if looks is not None else "穿搭套数未标注"
            entries.append(f"第{index}条：{count}/{len(shots)}页/{seconds:g}秒")
        if tasks and all(str(getattr(task, "media_kind", "video")) == "native_photo" for task in tasks):
            return "生成前冻结计划：" + "；".join(entries) + "。有效内容库存不足时整批停止，不减少篇数。"
        return "生成前冻结计划：" + "；".join(entries) + "。按实际兼容模板编排；数量不足会减少页数，不用同套换角度冒充多套。"

    @staticmethod
    def _reference_note(product: Dict[str, Any]) -> str:
        variant = str(product.get("variant_key") or "default")
        version = int(product.get("reference_pack_version") or 1)
        count = len(product.get("reference_images") or [])
        status = str(product.get("reference_status") or "limited")
        suffix = "，缺少部分角度时细节镜头使用保守模式" if status == "limited" else ""
        return f"商品参考包 {variant}/V{version}，{count} 张图，状态 {status}{suffix}"

    @classmethod
    def _reference_notes(cls, products: List[Dict[str, Any]]) -> str:
        unique: List[Dict[str, Any]] = []
        seen = set()
        for product in products:
            key = (product.get("reference_pack_id"), product.get("reference_pack_version"))
            if key not in seen:
                seen.add(key)
                unique.append(product)
        return "；".join(cls._reference_note(product) for product in unique)

    def _approve_and_render(
        self, record, *, approval_mode: str = "human"
    ) -> Dict[str, Any]:
        tasks = self._tasks(record.record_id)
        if not tasks:
            raise FeishuWorkflowError("找不到该飞书记录对应的 RDS 任务")
        self._assert_batch_complete(record.record_id, tasks)
        if all(
            str(getattr(task, "media_kind", "video") or "video") == "native_photo"
            for task in tasks
        ):
            return self._approve_photo_packages(record, tasks)
        if any(workflow_v2_enabled(task) for task in tasks):
            if not all(workflow_v2_enabled(task) for task in tasks):
                raise FeishuWorkflowError("混合工作流版本需分开处理")
            if approval_mode != "human":
                return self._project_v2(record, tasks)
            return self._advance_v2(record, tasks, approve=True)
        self._write_fields(record.record_id, {FIELD_PROGRESS: PROGRESS_RUNNING})
        note = text_value(record.fields.get(FIELD_NOTES))
        render_paths = self._render_tasks(
            [task.task_id for task in tasks],
            approval_mode=approval_mode,
            notes=(
                note or "飞书人工审核通过"
                if approval_mode == "human"
                else "历史待审核任务按新版流程自动续跑"
            ),
        )
        attachments = self._upload_files(render_paths, parent_type="bitable_file")
        auto = approval_mode == "operator_auto"
        self._write_fields(record.record_id, {
            FIELD_PROGRESS: PROGRESS_DONE,
            FIELD_OUTPUT: attachments,
            FIELD_REVIEW: REVIEW_NOT_REQUIRED if auto else REVIEW_APPROVED,
            FIELD_NOTES: (
                f"已完成 {len(render_paths)} 条自动成片；仅执行媒体技术检查；未发布。"
                if auto else f"已完成 {len(render_paths)} 条人工确认成片；未发布。"
            ),
        })
        return {
            "record_id": record.record_id,
            "action": "auto_render" if auto else "approve",
            "videos": render_paths,
        }

    def _require_travel_copy_release_allowed(self, tasks) -> None:
        """Validate the actual frozen travel package selected for publishing.

        Topic-linked travel copy is generated per task, so a static copy-pack
        status cannot prove that the final title and four overlays are safe.
        The publish checkbox binds the operator decision to this exact package;
        this gate checks its topic binding, locale purity and placeholders.
        """
        from domain.photo_contracts import placeholder_errors
        from services.locale_quality import copy_locale_issues

        repository = getattr(self, "repository", None)
        for task in tasks:
            recipe_id = str(getattr(task, "recipe_id", "") or "")
            if not is_topic_travel_recipe(recipe_id, repository):
                continue
            package = self.repository.get_content_package(
                str(getattr(task, "content_package_id", "") or "")
            )
            revision = self.repository.get_task_revision(
                str(getattr(task, "active_revision_id", "") or "")
            )
            manifest = dict(getattr(package, "photo_manifest_json", None) or {})
            copy_block = dict(manifest.get("copy") or {})
            theme_brief = dict(manifest.get("theme_brief") or {})
            frozen_theme = dict(
                ((revision.plan_snapshot_json or {}).get("plan") or {}).get("theme_brief") or {}
            ) if revision else {}
            slide_texts = list(copy_block.get("slide_texts") or [])
            expected_pages = int(getattr(task, "requested_shot_count", 0) or 5)
            if package is None or revision is None or len(slide_texts) != expected_pages or not all(
                str(value or "").strip() for value in slide_texts
            ):
                raise FeishuWorkflowError(
                    f"旅行图文缺少完整的最终 {expected_pages} 页文案"
                )
            if (not theme_brief.get("travel_theme_type")
                    or theme_brief.get("travel_theme_type") != frozen_theme.get("travel_theme_type")
                    or theme_brief.get("theme_key") != frozen_theme.get("theme_key")):
                raise FeishuWorkflowError("旅行图文的主题与最终发布包没有正确绑定")
            issues = placeholder_errors(copy_block)
            issues.extend(copy_locale_issues(
                copy_block, str(getattr(task, "target_locale", "") or "th-TH")))
            if issues:
                raise FeishuWorkflowError(
                    "旅行图文最终发布文案未通过发布检查：" + "；".join(issues)
                )

    def _approve_photo_packages(
        self, record, tasks, *, approval_mode: str = "content_review"
    ) -> Dict[str, Any]:
        from services.workflow_v2 import PhotoPackageReviewService
        from services.release_gate import require_photo_content_allowed
        if approval_mode == "publish_confirmation":
            self._require_travel_copy_release_allowed(tasks)
        review_ids = []
        # Do not partially approve a batch that still has missing/failed media.
        for task in tasks:
            require_photo_content_allowed(self.repository, task)
            if task.task_status == "photo_ready":
                if not self._v2_released(task):
                    raise FeishuWorkflowError("已验收图文的冻结 release 已失效")
            elif task.task_status != "photo_packaging":
                raise FeishuWorkflowError("图文批次尚未全部完成，请先勾选执行补齐")
        for task in tasks:
            if self._run_lease:
                self._run_lease.check()
            if task.task_status == "photo_ready":
                continue  # Previous review committed; retry only its projection.
            compact = approval_mode == "publish_confirmation"
            review = PhotoPackageReviewService(self.repository).record(
                task.task_id, decision="passed",
                dimensions=(
                    {"publish_confirmation": True, "technical_package": True}
                    if compact else {
                        "operator_preview": True,
                        "content_alignment": True,
                        "language_confirmed": True,
                    }
                ),
                evidence=(
                    {
                        "authorization": "feishu_confirm_publish_checkbox",
                        "visual_review_performed": False,
                    }
                    if compact else None
                ),
                reviewer_type="human",
                reviewer=(
                    "feishu_publish_confirmation" if compact
                    else "feishu_human_operator"
                ),
            )
            review_ids.append(review.review_id)
        self._write_fields(record.record_id, {
            FIELD_PROGRESS: PROGRESS_DONE,
            FIELD_REVIEW: REVIEW_NOT_REQUIRED if approval_mode == "publish_confirmation" else REVIEW_APPROVED,
            FIELD_REVIEW_STAGE: "技术完成" if approval_mode == "publish_confirmation" else "已验收",
            FIELD_CONFIRM_PUBLISH: approval_mode == "publish_confirmation",
            FIELD_NOTES: (
                f"已按确认发布冻结 {len(tasks)} 篇原生图文，正在进入主排班池。"
                if approval_mode == "publish_confirmation"
                else f"已人工验收 {len(tasks)} 篇原生图文；勾选确认发布后进入主排班池。"
            ),
        })
        return {
            "record_id": record.record_id, "action": "approve_native_photo",
            "task_ids": [task.task_id for task in tasks], "review_ids": review_ids,
        }

    def _confirm_photo_publish(self, record) -> Dict[str, Any]:
        """Use 确认发布 as the only operator gate for native-photo content."""
        preset_name = text_value(record.fields.get(FIELD_PRESET))
        tasks = self._tasks(record.record_id)
        photo_flow = bool(tasks) and all(
            str(getattr(task, "media_kind", "video") or "video") == "native_photo"
            for task in tasks
        )
        if not tasks and preset_name and self.catalog.is_native_photo(preset_name):
            generated = self._generate(record)
            if generated.get("action") in {
                "stage_photo_assets", "stage_existing_outfit_assets",
                "prepare_product_outfit_assets",
            }:
                record.fields[FIELD_PHOTO_ASSET_STATUS] = "待内容审核"
                generated = self._approve_staged_photo_assets(record)
            tasks = self._tasks(record.record_id)
            photo_flow = bool(tasks) and all(
                str(getattr(task, "media_kind", "video") or "video") == "native_photo"
                for task in tasks
            )
        elif not tasks:
            raise FeishuWorkflowError("找不到该飞书记录对应的 RDS 任务")
        if not photo_flow:
            # Existing video behavior remains unchanged.
            if text_value(record.fields.get(FIELD_PROGRESS)) != PROGRESS_DONE:
                raise FeishuWorkflowError("视频尚未完成技术生产，不能确认发布")
            return self._schedule_publish(record)

        self._assert_batch_complete(record.record_id, tasks)
        if any(task.task_status == "photo_packaging" for task in tasks):
            self._approve_photo_packages(
                record, tasks, approval_mode="publish_confirmation"
            )
            tasks = self._tasks(record.record_id)
        if any(task.task_status != "photo_ready" for task in tasks):
            raise FeishuWorkflowError("图文尚未完成技术生产，不能确认发布")
        return self._schedule_publish(record)

    def _render_tasks(
        self,
        task_ids: Iterable[str],
        *,
        approval_mode: str,
        notes: str,
    ) -> List[str]:
        flow = VideoRenderFlow(self.repository, self.renderer)
        render_paths: List[str] = []
        for task_id in task_ids:
            current = self.repository.get_task(task_id)
            if current is None:
                raise FeishuWorkflowError(f"找不到 RDS 任务：{task_id}")
            if current.task_status == "image_review":
                current = flow.approve_group(
                    task_id,
                    reviewer=(
                        "feishu_operator_auto_render"
                        if approval_mode == "operator_auto"
                        else "feishu_human_review"
                    ),
                    notes=notes,
                    approval_mode=approval_mode,
                )
            if current.task_status == "rendering":
                rendered = flow.render(
                    task_id,
                    overlay_profile_id=self.catalog.overlay_profile_id or None,
                )
            elif current.task_status == "video_review":
                rendered = self.repository.latest_render(task_id)
            else:
                raise FeishuWorkflowError(
                    f"任务 {task_id} 当前状态 {current.task_status}，不能生成成片"
                )
            if not rendered or rendered.qc_status != "passed" or not rendered.output_url:
                raise FeishuWorkflowError(f"任务 {task_id} 视频媒体质检未通过")
            render_paths.append(rendered.output_url)
        return render_paths

    def _schedule_publish(self, record) -> Dict[str, Any]:
        if self.publish_scheduler is None:
            raise FeishuWorkflowError("自动排班服务未接入")
        tasks = self._tasks(record.record_id)
        if not tasks:
            raise FeishuWorkflowError("找不到该飞书记录对应的 RDS 任务")
        self._assert_batch_complete(record.record_id, tasks)
        slots = []
        confirmed_by_checkbox = bool(record.fields.get(FIELD_CONFIRM_PUBLISH))
        photo_only = all(
            str(getattr(task, "media_kind", "video") or "video") == "native_photo"
            for task in tasks
        )
        selected_store_id = text_value(record.fields.get(FIELD_STORE))
        if photo_only and not selected_store_id:
            raise FeishuWorkflowError("原生图文确认发布前必须选择店铺")
        # 目标账号贯穿投递：任务冻结了目标账号时，本行选择的店铺必须与该账号
        # 所属店铺一致；不一致直接报错，不允许借店铺公共池绕过账号绑定。
        target_accounts = {
            str(getattr(task, "target_publish_account_id", "") or "")
            for task in tasks
            if str(getattr(task, "target_publish_account_id", "") or "")
        }
        if photo_only and target_accounts:
            if len(target_accounts) > 1:
                raise FeishuWorkflowError("同一行任务冻结了多个目标账号，禁止混组发布")
            target_account_id = next(iter(target_accounts))
            binding = self._resolve_target_account(record)
            if binding is None or binding.account_id != target_account_id:
                raise FeishuWorkflowError(
                    f"任务冻结的目标账号 {target_account_id} 与当前行「目标账号」不一致；"
                    "转账号需走修订/重排，不能在发布时改绑"
                )
            if selected_store_id != binding.store_id:
                raise FeishuWorkflowError(
                    f"目标账号 {target_account_id} 属于店铺 {binding.store_id}，"
                    f"请把发布店铺改为 {binding.store_id}；内容只会投递到该账号"
                )
        # Preflight every task before enqueuing any member of this row.
        for task in tasks:
            if workflow_v2_enabled(task) and not self._v2_released(task):
                raise FeishuWorkflowError("当前 V2 版本尚未通过终审，不能确认发布")
        enqueue_main = callable(getattr(self.publish_scheduler, "enqueue_task", None))
        for task in tasks:
            if enqueue_main:
                enqueue_kwargs = {"feishu_record_id": record.record_id}
                if photo_only:
                    enqueue_kwargs["store_id"] = selected_store_id
                slots.append(self.publish_scheduler.enqueue_task(task.task_id, **enqueue_kwargs))
            else:
                slot = self.publish_scheduler.queue_task(
                    task.task_id,
                    operator=(
                        "feishu_confirm_publish_v2"
                        if confirmed_by_checkbox else "feishu_legacy_review_schedule"
                    ),
                )
                slots.append({
                    "task_id": task.task_id,
                    "local_time": slot.account_local_time.isoformat(timespec="minutes"),
                    "timezone": slot.account_timezone,
                })
        if enqueue_main:
            progress = PROGRESS_QUEUED
            note = (f"已进入主排班池，共 {len(slots)} 篇原生图文；"
                    f"发布店铺 {selected_store_id}；按店铺养号配额分配具备图文直发能力的账号和时间，不挂商品；"
                    "CreatOK 原生图文由 TikTok 自动添加推荐音乐，不承诺具体曲目。"
                    if photo_only else
                    f"已进入短视频主排班池，共 {len(slots)} 条。"
                    "按店铺养号配额自动分配账号和时间，非带货/不挂车；"
                    "在未来 48 小时排期窗口内提前选择当地 NeoBund BGM 并创建定时任务。")
            action = "enqueue_main_schedule"
        else:
            progress = PROGRESS_SCHEDULED
            summary = "；".join(
                f"{item['local_time']} ({item['timezone']})" for item in slots
            )
            note = f"已进入自动发布队列：{summary}。发布前 120 分钟内选择国家热点 BGM。"
            action = "schedule"
        self._write_fields(record.record_id, {
            FIELD_PROGRESS: progress,
            FIELD_CONFIRM_PUBLISH: False,
            # 本次排期成功即代表上一次失败（本函数内的「必须选择店铺」「尚未通过终审」等）
            # 已被解决；不清掉会让已发布的行长期挂着失败原因（曾出现 7 行已发布却显示
            # 「原生图文确认发布前必须选择店铺」）。
            FIELD_FAILURE_REASON: None,
            FIELD_MUSIC_MODE: (
                "TikTok 平台自动推荐" if photo_only and enqueue_main
                else "发布窗口自动选曲"
            ),
            FIELD_NOTES: note,
        })
        return {"record_id": record.record_id, "action": action, "slots": slots}

    def sync_publication_status(
        self, record_id: str, *, current_fields: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Project RDS publish truth back to the compact Feishu row."""
        tasks = self._tasks(record_id)
        if not tasks:
            return {"record_id": record_id, "updated": False}
        state_reader = getattr(self.publish_scheduler, "get_task_state", None)
        if callable(state_reader):
            states = [state_reader(task.task_id) for task in tasks]
            progress, note = self._publication_projection(states)
            current = current_fields or self.client.get_record(record_id).fields or {}
            if text_value(current.get(FIELD_PROGRESS)) == progress and text_value(current.get(FIELD_NOTES)) == note:
                return {"record_id": record_id, "updated": False, "progress": progress}
            self._write_fields(record_id, {
                FIELD_PROGRESS: progress,
                FIELD_NOTES: note,
            })
            return {"record_id": record_id, "updated": True, "progress": progress}
        states = [task.task_status for task in tasks]
        records = []
        for task in tasks:
            render = self.repository.get_render(task.selected_render_id or "")
            record = (
                self.repository.get_publish_record_by_render(render.render_id)
                if render else None
            )
            if record:
                records.append(record)
        if all(state == "published" for state in states):
            progress = PROGRESS_PUBLISHED
        elif any(state == "failed" for state in states):
            progress = PROGRESS_PUBLISH_FAILED
        elif any(state == "publishing" for state in states):
            progress = PROGRESS_SCHEDULED
        elif records:
            progress = PROGRESS_SCHEDULED
        else:
            progress = PROGRESS_QUEUED
        details = []
        for publish in records:
            audio = (publish.platform_metadata_json or {}).get("audio") or {}
            selected = audio.get("selected") or {}
            local_time = ((publish.platform_metadata_json or {}).get("schedule") or {}).get(
                "account_local_time"
            )
            item = str(local_time or publish.planned_publish_at or "")
            if selected.get("title"):
                item += f"｜BGM: {selected['title']}"
            if publish.external_post_url:
                item += f"｜{publish.external_post_url}"
            details.append(item)
        self._write_fields(record_id, {
            FIELD_PROGRESS: progress,
            FIELD_NOTES: "；".join(details)[:1800],
        })
        return {"record_id": record_id, "updated": True, "progress": progress}

    @staticmethod
    def _publication_projection(states: List[Dict[str, Any]]) -> tuple[str, str]:
        """Return a truthful batch projection, including partial failures."""
        statuses = [str(item.get("status") or "") for item in states]
        errors = [str(item.get("error_message") or "").strip() for item in states]
        stopped = [
            index for index, error in enumerate(errors)
            if "已停止自动重试" in error or "超过自动重试上限" in error
        ]
        retrying = [index for index, error in enumerate(errors) if "等待自动重试" in error]
        failed = [index for index, status in enumerate(statuses) if status == "发布失败"]
        # 「提交中」= 已占位但结果未确认，禁止重发。它既不是「待排班」也不是
        # 确定的失败，必须单独呈现，否则整批会被误报成「待排班」而掩盖在途任务。
        submitting = [
            index for index, status in enumerate(statuses)
            if status in MAIN_QUEUE_SUBMITTING_STATUSES
        ]
        needs_action = sorted(set(stopped + failed))

        if statuses and all(status == "已发布" for status in statuses):
            progress = PROGRESS_PUBLISHED
        elif needs_action:
            progress = PROGRESS_PUBLISH_FAILED
        elif any(status == "发布中" for status in statuses):
            progress = PROGRESS_PUBLISHING
        elif submitting:
            progress = PROGRESS_SUBMITTING
        elif any(status == "已排期" for status in statuses):
            progress = PROGRESS_SCHEDULED
        else:
            progress = PROGRESS_QUEUED

        counts = {
            "已发布": statuses.count("已发布"),
            "已排期": statuses.count("已排期"),
            "提交中": len(submitting),
            "发布中": statuses.count("发布中"),
            "待排班": sum(status in {"", "待排班"} for status in statuses),
            "重试中": len(retrying),
            "需处理": len(needs_action),
        }
        summary = "，".join(
            f"{label}{count}" for label, count in counts.items() if count
        ) or "待排班0"
        attention = set(needs_action) | set(retrying) | set(submitting)
        details = []
        for index, item in enumerate(states):
            values = [
                str(item.get("task_id") or "").strip() if index in attention else "",
                str(item.get("account_name") or "").strip(),
                str(item.get("planned_publish_at") or "").strip(),
                f"BGM: {item.get('bgm_title')}" if item.get("bgm_title") else "",
            ]
            if index in attention:
                values.append(errors[index][:180])
            detail = "｜".join(value for value in values if value)
            if detail:
                details.append(detail)
        note = f"批次状态：共{len(states)}，{summary}。"
        if details:
            note += "；".join(details)
        return progress, note[:1800]

    def _redo(self, record, action: str) -> Dict[str, Any]:
        tasks = self._tasks(record.record_id)
        if not tasks:
            raise FeishuWorkflowError("找不到该飞书记录对应的 RDS 任务")
        if any(workflow_v2_enabled(task) for task in tasks):
            if not all(workflow_v2_enabled(task) for task in tasks):
                raise FeishuWorkflowError("混合工作流版本需分开返工")
            return self._redo_v2(record, tasks, action)
        if action == "redo_render":
            raise FeishuWorkflowError("重做成片仅适用于 V2 分阶段流程")
        slots = list(range(1, 6)) if action == "redo_all" else [int(action.split("_")[1])]
        self._write_fields(record.record_id, {FIELD_PROGRESS: PROGRESS_RUNNING})
        producer = HeroFirstProducer(self.repository, self.generator)
        effective_slots = set(slots)
        for task in tasks:
            current = self.repository.get_task(task.task_id) or task
            if current.task_status != "image_review":
                raise FeishuWorkflowError(
                    f"任务 {task.task_id} 已不在图片审核阶段，不能重做图片"
                )
            self.repository.transition_task(task.task_id, "image_review", "image_generating")
            try:
                anchor = int((current.plan_json or {}).get("anchor_slot") or 1)
                # P1 is derived from P2 and P3-P5 share P2 continuity. A new
                # anchor invalidates the complete visible group.
                task_slots = dependent_redo_slots(current.plan_json or {}, slots)
                effective_slots.update(task_slots)
                ordered = sorted(task_slots, key=lambda value: value != anchor)
                for slot in ordered:
                    result = producer.regenerate_slot(task.task_id, slot)
                    if result.status != "generated":
                        raise FeishuWorkflowError(
                            f"任务 {task.task_id} 的 P{slot} 重做失败：{result.error}"
                        )
            finally:
                refreshed = self.repository.get_task(task.task_id)
                if refreshed and refreshed.task_status == "image_generating":
                    self.repository.transition_task(
                        task.task_id, "image_generating", "image_review"
                    )
        attachments = self._upload_previews([task.task_id for task in tasks])
        self._write_fields(record.record_id, {
            FIELD_PROGRESS: PROGRESS_REVIEW,
            FIELD_OUTPUT: attachments,
            FIELD_REVIEW: REVIEW_PENDING,
            FIELD_NOTES: f"已重做 {'P' + str(slots[0]) if len(effective_slots) == 1 else '整组'}，请重新审核。",
        })
        return {
            "record_id": record.record_id,
            "action": action,
            "slots": sorted(effective_slots),
        }

    def _tasks(self, record_id: str):
        return self.repository.list_tasks_by_source_prefix(SOURCE_TYPE, f"{record_id}:")

    def _recover_incomplete_review(self, task_id: str, producer) -> None:
        """Retry only incomplete slots once while keeping the row resumable."""
        task = self.repository.get_task(task_id)
        if not task or task.task_status != "image_review":
            return
        shots = HeroFirstProducer._latest_per_slot(self.repository.list_shots(task_id))
        incomplete = [
            shot.slot_index
            for shot in shots
            if shot.shot_status != "generated" or shot.qa_status != "passed" or not shot.image_url
        ]
        if not incomplete:
            return
        self.repository.transition_task(task_id, "image_review", "image_generating")
        try:
            anchor = int((task.plan_json or {}).get("anchor_slot") or 1)
            for slot in sorted(incomplete, key=lambda value: value != anchor):
                result = producer.regenerate_slot(task_id, slot)
                if result.status != "generated":
                    raise FeishuWorkflowError(
                        f"任务 {task_id} 的 P{slot} 自动补跑失败：{result.error}"
                    )
        finally:
            refreshed = self.repository.get_task(task_id)
            if refreshed and refreshed.task_status == "image_generating":
                self.repository.transition_task(task_id, "image_generating", "image_review")

    def _upload_previews(self, task_ids: Iterable[str]) -> List[Dict[str, str]]:
        paths: List[str] = []
        for task_id in task_ids:
            shots = HeroFirstProducer._latest_per_slot(self.repository.list_shots(task_id))
            if len(shots) != 5 or any(not shot.image_url for shot in shots):
                raise FeishuWorkflowError(f"任务 {task_id} 没有完整的 5 张预览图")
            paths.extend(str(shot.image_url) for shot in shots)
        return self._upload_files(paths, parent_type="bitable_image")

    def _upload_files(self, paths: Iterable[str], *, parent_type: str) -> List[Dict[str, str]]:
        uploaded: List[Dict[str, str]] = []
        for raw in paths:
            path = Path(raw)
            if not path.is_file():
                raise FeishuWorkflowError(f"输出文件不存在：{path}")
            content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            item = self.client.upload_attachment(
                path.read_bytes(), path.name, content_type, path.stat().st_size,
                parent_type=parent_type,
            )
            uploaded.append({"file_token": item["file_token"]})
        return uploaded
