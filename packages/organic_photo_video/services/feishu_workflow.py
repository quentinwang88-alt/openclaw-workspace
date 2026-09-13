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
from typing import Any, Dict, Iterable, List, Mapping, Optional

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
FIELD_PHOTO_INPUT = "完整穿搭素材（可选）"
FIELD_PHOTO_INPUT_LEGACY = "图文参考图"
FIELD_PRODUCT_REFERENCE = "商品参考图（可选）"
FIELD_REFERENCE = "参考图（可选）"
FIELD_REFERENCE_TYPE = "参考图类型"
FIELD_CONTENT_THEME = "图文主题"
FIELD_CONTENT_REQUIREMENT = "内容要求（可选）"
FIELD_TRAVEL_PLACE = "旅行地点（可选）"
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
        self._run_lease = None
        self._photo_quality_summaries: Dict[str, str] = {}
        self.product_reference_resolver = (
            product_reference_resolver or ProductReferenceResolver(repository)
        )

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
                resolve_reference_mode,
            )
            try:
                theme = resolve_photo_theme(text_value(record.fields.get(FIELD_CONTENT_THEME)))
            except ValueError as exc:
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
            elif unified_attachments:
                reference_attachments = unified_attachments
                try:
                    reference_mode = resolve_reference_mode(
                        selected_type=selected_reference_type,
                        attachments=reference_attachments, product_id=product_id,
                        required_role_count=len(roles), requested_count=quantity,
                        required_roles=roles,
                    )
                except ValueError as exc:
                    raise FeishuWorkflowError(str(exc)) from exc
            elif legacy_complete:
                reference_mode, reference_attachments = REFERENCE_MODE_COMPLETE_LOOK, legacy_complete
            elif legacy_product or product_id:
                reference_mode, reference_attachments = REFERENCE_MODE_PRODUCT, legacy_product
            # Explicit STYLE + product code is a supported combination: the
            # product pack remains the identity lock, while uploaded images are
            # inspiration only and must never be written into that pack.
            if reference_mode == REFERENCE_MODE_STYLE and product_id:
                try:
                    style_product = self.product_reference_resolver.resolve_snapshot(
                        product_id, selection_key=record.record_id,
                        account_id=specs[0].account_id,
                    )
                except ProductReferenceResolutionError as exc:
                    raise FeishuWorkflowError(
                        f"指定商品 {product_id} 缺少可用商品参考包：{exc}"
                    ) from exc
            product_context = ({
                key: style_product.get(key)
                for key in ("product_id", "product_name", "category", "variant_key",
                            "reference_pack_id", "reference_pack_version")
                if style_product.get(key) not in (None, "")
            } if style_product else {})
            requires_product_supply = bool(
                recipe_for_input and (recipe_for_input.recipe_spec_json or {}).get("outfit_supply")
            )
            asset_status = text_value(record.fields.get(FIELD_PHOTO_ASSET_STATUS))
            if (requires_product_supply and not reference_attachments and not product_id
                    and asset_status not in {"已确认，正在生成", "已匹配可用素材"}):
                raise FeishuWorkflowError(
                    "该图文预设需要填写产品编码或上传参考图"
                )
            from services.photo_batch_variation import plan_batch_variations
            from services.photo_content_planner import (
                PhotoContentPlanStore, get_planning_flow, plan_th_choice_batch,
                recipe_has_planning_policy,
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
            travel_contract: dict[str, Any] = {}
            travel_variables: dict[str, Any] = {}
            travel_copy_templates = None
            travel_topic: dict[str, Any] = {}
            planning_flow = get_planning_flow(recipe_for_input.recipe_id) if recipe_for_input else ""
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
                layering_copy_templates = list(profile_for_band.get("copy_variants") or [])
            if planning_flow == "travel_two_step":
                recipe_spec_input = recipe_for_input.recipe_spec_json or {}
                travel_contract = dict(recipe_spec_input.get("travel_contract") or {})
                profiles_input = list(recipe_spec_input.get("execution_profiles") or [])
                travel_variables = dict((profiles_input[0].get("variables") or {})
                                        if profiles_input else {})
                travel_copy_templates = list(
                    (profiles_input[0].get("copy_variants") or [])
                    if profiles_input else []
                ) or None
                if theme and theme.get("travel_theme_type"):
                    travel_topic = build_travel_topic(
                        theme=theme, travel_place=travel_place,
                        travel_variables=travel_variables,
                        content_requirement=content_requirement,
                        fields=record.fields, recipe_spec=recipe_spec_input,
                    )
            if reference_mode == REFERENCE_MODE_STYLE and recipe_for_input:
                if theme is None:
                    raise FeishuWorkflowError("风格参考模式需要选择图文主题")
                from services.photo_asset_supply import PhotoAssetSupplyService
                from services.photo_reference_vision import PhotoReferenceVisionService
                style_reference_paths = PhotoAssetSupplyService(
                    self.client, root=staging_root,
                ).stage_reference_images(
                    record_id=record.record_id, attachments=reference_attachments,
                    reference_kind="style",
                )
                reference_vision = self.photo_reference_vision or PhotoReferenceVisionService(
                    root=staging_root
                )
                self.photo_reference_vision = reference_vision
                if planning_flow == "travel_two_step":
                    self._write_fields(record.record_id, {FIELD_PROGRESS: PROGRESS_PLANNING})
                    def _plan_travel_reference():
                        analysis = reference_vision.analyze_reference(
                            record_id=record.record_id, paths=style_reference_paths,
                            theme=theme or variation_theme,
                            category_key=str((recipe_for_input.recipe_spec_json or {}).get("category_key") or ""),
                            content_requirement=content_requirement,
                        )
                        plan = reference_vision.plan_travel_content(
                            record_id=record.record_id, analysis=analysis,
                            travel_contract=travel_contract, variables=travel_variables,
                            content_requirement=content_requirement, count=quantity,
                            travel_topic=travel_topic or None,
                            product_context=product_context,
                            reference_paths=style_reference_paths,
                            product_reference_paths=list(
                                style_product.get("reference_images") or []
                            ),
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
                    if travel_topic:
                        style_profile["travel_topic"] = dict(travel_topic)
                else:
                    style_profile = reference_vision.analyze(
                        record_id=record.record_id, paths=style_reference_paths,
                        theme=theme or variation_theme,
                        category_key=str((recipe_for_input.recipe_spec_json or {}).get("category_key") or ""),
                        content_requirement=content_requirement, count=quantity,
                        product_context=product_context,
                        planning_flow=planning_flow, required_roles=roles,
                    )
                if product_context:
                    style_profile["product_context"] = dict(product_context)
            content_plan = None
            if (recipe_for_input
                    and recipe_has_planning_policy(recipe_for_input.recipe_id)
                    and theme is not None
                    and reference_mode in {
                        REFERENCE_MODE_COMPLETE_LOOK, REFERENCE_MODE_PRODUCT,
                        REFERENCE_MODE_STYLE,
                    }):
                input_contract = {
                    "recipe_id": recipe_for_input.recipe_id,
                    "recipe_version": recipe_for_input.recipe_version,
                    "theme_key": theme["theme_key"],
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
                plan_store = PhotoContentPlanStore(staging_root)

                def _load_content_plan():
                    return plan_store.load_or_create(
                        record_id=record.record_id, input_contract=input_contract,
                        create=lambda: plan_th_choice_batch(
                            record_id=record.record_id,
                            recipe_id=recipe_for_input.recipe_id,
                            theme=theme, reference_mode=reference_mode, count=quantity,
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
                        ),
                    )

                try:
                    content_plan = _load_content_plan()
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
            pinned_asset_set_ids: list[str] = []
            prepared_source_groups: list[list[dict[str, Any]]] = []
            if (reference_mode == REFERENCE_MODE_COMPLETE_LOOK and recipe_for_input
                    and asset_status not in {
                        "已确认，正在生成", "已匹配可用素材",
                    }):
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
                    )
                    pinned_asset_set_ids.append(saved.asset_set_id)
            if (reference_mode == REFERENCE_MODE_STYLE and recipe_for_input
                    and asset_status not in {"已确认，正在生成", "已匹配可用素材"}):
                if theme is None:
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
                            record_id=item_id, reference_paths=paths, theme=theme,
                            account=account, persona=persona, variation=variation,
                            progress=_asset_progress, product=style_product,
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
                        metadata={"theme_key": theme["theme_key"],
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
                    and asset_status not in {
                        "已确认，正在生成", "已匹配可用素材",
                    }):
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
                        if theme:
                            request["copy"] = build_theme_copy(
                                theme, frozen_manifest.get("assets") or [], variations[index]
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
                            } if theme and theme.get("travel_theme_type") else {}),
                        }
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
            except Exception as exc:
                failures.append(f"{source_record_id}: {exc}")
        attachments = self._upload_files(paths, parent_type="bitable_image") if paths else []
        self._write_fields(record.record_id, {
            FIELD_PROGRESS: PROGRESS_ACTION if failures else PROGRESS_DONE,
            FIELD_OUTPUT: attachments, FIELD_REVIEW: REVIEW_NOT_REQUIRED,
            FIELD_REVIEW_STAGE: "处理中" if failures else "技术完成", FIELD_PHOTO_SUMMARY: summary,
            FIELD_FAILURE_REASON: None,
            FIELD_NOTES: (f"已完成 {len(task_ids)}/{batch.expected_count} 篇原生图文，共 {len(paths)} 张；"
                          + ("勾选执行后沿用冻结方案补齐。" + "；".join(failures) if failures
                             else "技术检查已通过；勾选确认发布后冻结当前成品并进入发布队列。")
                          + "｜" + self._photo_quality_summaries.get(record.record_id, "")
                          )[:1500],
        })
        return {"record_id": record.record_id, "action": "generate_native_photo",
                "task_ids": task_ids, "photo_count": len(paths), "failures": failures}

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
        theme = resolve_photo_theme(text_value(record.fields.get(FIELD_CONTENT_THEME)))
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
                    reference_paths, theme, variation, account, product) != expected_hash:
                # 冻结内容计划可能在供给之后被补写（如旅行 cover_selection 兜底），
                # 组合 hash 无法直接复现；逐组件核对参考图/主题/穿搭/人物后重定基线。
                supply_service.verify_and_rebaseline_identity(
                    item_dir=item_dir, paths=reference_paths, theme=theme,
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
                record_id=item_id, reference_paths=reference_paths, theme=theme,
                account=account, persona=persona, variation=variation, product=product,
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
        self._write_fields(record.record_id, {
            FIELD_REVIEW: REVIEW_NOT_REQUIRED, FIELD_PHOTO_ASSET_STATUS: "已确认，正在生成",
            FIELD_NOTES: f"已按确认发布冻结素材集 {saved.asset_set_id} V{saved.asset_set_version}；开始生成原生图文。",
        })
        record.fields[FIELD_PHOTO_ASSET_STATUS] = "已确认，正在生成"
        record.fields[FIELD_REVIEW] = REVIEW_PENDING
        # Pin this run to the exact human-confirmed upload without asking the
        # operator to maintain the compatibility JSON field.
        record.fields[FIELD_PHOTO_REQUEST] = json.dumps({"asset_set_id": saved.asset_set_id})
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

        for task in tasks:
            recipe_id = str(getattr(task, "recipe_id", "") or "")
            if not recipe_id.startswith("PHOTO_TH_TRAVEL"):
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
            issues.extend(copy_locale_issues(copy_block, "th-TH"))
            if issues:
                raise FeishuWorkflowError(
                    "旅行图文最终泰语文案未通过发布检查：" + "；".join(issues)
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
