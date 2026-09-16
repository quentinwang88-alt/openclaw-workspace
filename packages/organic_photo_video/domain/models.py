"""OPV domain models mapped one-to-one onto the ``opv_*`` RDS tables.

Conventions
-----------
- All datetimes are naive UTC (``DATETIME(6)`` columns store UTC).
- ``*_json`` attributes hold Python dicts/lists in memory; :meth:`to_row`
  serializes them to JSON strings for MySQL, :meth:`from_row` parses them back.
- ``DECIMAL`` columns become ``float`` and ``TINYINT(1)`` columns become
  ``bool`` in :meth:`from_row`.
- ``created_at`` / ``updated_at`` are DB-managed defaults and are excluded
  from :meth:`to_row`.
- Config/import payloads (versioned JSON files) use non-suffixed keys such as
  ``visual_rules``; :meth:`from_payload` accepts both spellings.
"""

from __future__ import annotations

import dataclasses
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Type, TypeVar

TModel = TypeVar("TModel")


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------

def utc_now() -> datetime:
    """Naive UTC now (matches DATETIME(6) columns)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def generate_prefixed_id(prefix: str, moment: Optional[datetime] = None) -> str:
    """Human-sortable id like ``opv_task_20260830_a1b2c3d4e5f6`` (<= 64 chars)."""
    stamp = (moment or utc_now()).strftime("%Y%m%d")
    return f"{prefix}_{stamp}_{uuid.uuid4().hex[:12]}"


def dump_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def load_json_value(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    return json.loads(value)


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def to_bool(value: Any) -> bool:
    if value is None:
        return False
    return bool(value)


def _field_names(model_cls: Type[Any]) -> List[str]:
    return [f.name for f in dataclasses.fields(model_cls)]


def _from_payload(model_cls: Type[TModel], payload: Mapping[str, Any]) -> TModel:
    """Build a model from a config payload.

    Accepts both column-style keys (``visual_rules_json``) and file-style keys
    (``visual_rules``). ``schema_version`` is a contract marker consumed by the
    validators and is not a model field. Unknown keys raise instead of being
    silently dropped.
    """
    known = set(_field_names(model_cls))
    init: Dict[str, Any] = {}
    for key, value in payload.items():
        if key == "schema_version":
            continue
        if key.startswith("_"):
            continue  # documentation markers like _example_note
        if key in known:
            attr = key
        elif f"{key}_json" in known:
            attr = f"{key}_json"
        else:
            raise ValueError(f"unknown field {key!r} for {model_cls.__name__} payload")
        init[attr] = value
    missing_required = [
        f.name
        for f in dataclasses.fields(model_cls)
        if f.default is dataclasses.MISSING
        and f.default_factory is dataclasses.MISSING
        and f.name not in init
    ]
    if missing_required:
        raise ValueError(
            f"{model_cls.__name__} payload missing required fields: {missing_required}"
        )
    return model_cls(**init)


# --------------------------------------------------------------------------
# Reference data
# --------------------------------------------------------------------------

@dataclass
class MarketPack:
    market_pack_id: str
    pack_key: str
    target_country: str
    target_locale: str
    pack_name: str
    pack_version: int = 1
    climate_zone: Optional[str] = None
    season_key: str = "all_season"
    status: str = "draft"
    visual_rules_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    copy_rules_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    topic_rules_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    safety_rules_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "MarketPack":
        return _from_payload(cls, payload)

    def to_row(self) -> Dict[str, Any]:
        return {
            "market_pack_id": self.market_pack_id,
            "pack_key": self.pack_key,
            "pack_version": self.pack_version,
            "target_country": self.target_country,
            "target_locale": self.target_locale,
            "climate_zone": self.climate_zone,
            "season_key": self.season_key,
            "pack_name": self.pack_name,
            "status": self.status,
            "visual_rules_json": dump_json(self.visual_rules_json),
            "copy_rules_json": dump_json(self.copy_rules_json),
            "topic_rules_json": dump_json(self.topic_rules_json),
            "safety_rules_json": dump_json(self.safety_rules_json),
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "MarketPack":
        return cls(
            market_pack_id=row["market_pack_id"],
            pack_key=row["pack_key"],
            pack_version=int(row["pack_version"]),
            target_country=row["target_country"],
            target_locale=row["target_locale"],
            pack_name=row["pack_name"],
            climate_zone=row.get("climate_zone"),
            season_key=row.get("season_key") or "all_season",
            status=row.get("status") or "draft",
            visual_rules_json=load_json_value(row.get("visual_rules_json"), {}),
            copy_rules_json=load_json_value(row.get("copy_rules_json"), {}),
            topic_rules_json=load_json_value(row.get("topic_rules_json"), {}),
            safety_rules_json=load_json_value(row.get("safety_rules_json"), {}),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


@dataclass
class ThemeCatalog:
    theme_id: str
    theme_key: str
    theme_name: str
    theme_version: int = 1
    description: Optional[str] = None
    status: str = "draft"
    applicable_markets_json: List[str] = dataclasses.field(default_factory=list)
    product_match_rules_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    content_plan_rules_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    default_storyboard_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "ThemeCatalog":
        return _from_payload(cls, payload)

    def to_row(self) -> Dict[str, Any]:
        return {
            "theme_id": self.theme_id,
            "theme_key": self.theme_key,
            "theme_version": self.theme_version,
            "theme_name": self.theme_name,
            "description": self.description,
            "status": self.status,
            "applicable_markets_json": dump_json(self.applicable_markets_json),
            "product_match_rules_json": dump_json(self.product_match_rules_json),
            "content_plan_rules_json": dump_json(self.content_plan_rules_json),
            "default_storyboard_json": dump_json(self.default_storyboard_json),
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "ThemeCatalog":
        return cls(
            theme_id=row["theme_id"],
            theme_key=row["theme_key"],
            theme_version=int(row["theme_version"]),
            theme_name=row["theme_name"],
            description=row.get("description"),
            status=row.get("status") or "draft",
            applicable_markets_json=load_json_value(
                row.get("applicable_markets_json"), []
            ),
            product_match_rules_json=load_json_value(
                row.get("product_match_rules_json"), {}
            ),
            content_plan_rules_json=load_json_value(
                row.get("content_plan_rules_json"), {}
            ),
            default_storyboard_json=load_json_value(
                row.get("default_storyboard_json"), {}
            ),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


@dataclass
class RenderPreset:
    render_preset_id: str
    preset_key: str
    preset_name: str
    preset_version: int = 1
    status: str = "draft"
    width_px: int = 1080
    height_px: int = 1920
    fps: int = 30
    target_duration_ms: int = 12500
    codec: str = "h264"
    render_mode: str = "still_slideshow"
    motion_rules_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    transition_rules_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    text_overlay_rules_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    audio_rules_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    output_rules_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "RenderPreset":
        return _from_payload(cls, payload)

    def to_row(self) -> Dict[str, Any]:
        return {
            "render_preset_id": self.render_preset_id,
            "preset_key": self.preset_key,
            "preset_version": self.preset_version,
            "preset_name": self.preset_name,
            "status": self.status,
            "width_px": self.width_px,
            "height_px": self.height_px,
            "fps": self.fps,
            "target_duration_ms": self.target_duration_ms,
            "codec": self.codec,
            "render_mode": self.render_mode,
            "motion_rules_json": dump_json(self.motion_rules_json),
            "transition_rules_json": dump_json(self.transition_rules_json),
            "text_overlay_rules_json": dump_json(self.text_overlay_rules_json),
            "audio_rules_json": dump_json(self.audio_rules_json),
            "output_rules_json": dump_json(self.output_rules_json),
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "RenderPreset":
        return cls(
            render_preset_id=row["render_preset_id"],
            preset_key=row["preset_key"],
            preset_version=int(row["preset_version"]),
            preset_name=row["preset_name"],
            status=row.get("status") or "draft",
            width_px=int(row["width_px"]),
            height_px=int(row["height_px"]),
            fps=int(row["fps"]),
            target_duration_ms=int(row["target_duration_ms"]),
            codec=row.get("codec") or "h264",
            render_mode=row.get("render_mode") or "still_slideshow",
            motion_rules_json=load_json_value(row.get("motion_rules_json"), {}),
            transition_rules_json=load_json_value(row.get("transition_rules_json"), {}),
            text_overlay_rules_json=load_json_value(
                row.get("text_overlay_rules_json"), {}
            ),
            audio_rules_json=load_json_value(row.get("audio_rules_json"), {}),
            output_rules_json=load_json_value(row.get("output_rules_json"), {}),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


@dataclass
class AccountProfile:
    account_id: str
    account_code: str
    account_name: str
    target_country: str
    default_locale: str
    timezone: str
    platform: str = "tiktok"
    status: str = "testing"
    persona_ref_id: Optional[str] = None
    persona_snapshot_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    visual_identity_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    allowed_style_refs_json: List[str] = dataclasses.field(default_factory=list)
    allowed_look_refs_json: List[str] = dataclasses.field(default_factory=list)
    allowed_scene_refs_json: List[str] = dataclasses.field(default_factory=list)
    core_scene_refs_json: List[str] = dataclasses.field(default_factory=list)
    default_market_pack_id: Optional[str] = None
    default_render_preset_id: Optional[str] = None
    operating_rules_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "AccountProfile":
        return _from_payload(cls, payload)

    def to_row(self) -> Dict[str, Any]:
        return {
            "account_id": self.account_id,
            "account_code": self.account_code,
            "account_name": self.account_name,
            "platform": self.platform,
            "target_country": self.target_country,
            "default_locale": self.default_locale,
            "timezone": self.timezone,
            "status": self.status,
            "persona_ref_id": self.persona_ref_id,
            "persona_snapshot_json": dump_json(self.persona_snapshot_json),
            "visual_identity_json": dump_json(self.visual_identity_json),
            "allowed_style_refs_json": dump_json(self.allowed_style_refs_json),
            "allowed_look_refs_json": dump_json(self.allowed_look_refs_json),
            "allowed_scene_refs_json": dump_json(self.allowed_scene_refs_json),
            "core_scene_refs_json": dump_json(self.core_scene_refs_json),
            "default_market_pack_id": self.default_market_pack_id,
            "default_render_preset_id": self.default_render_preset_id,
            "operating_rules_json": dump_json(self.operating_rules_json),
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "AccountProfile":
        return cls(
            account_id=row["account_id"],
            account_code=row["account_code"],
            account_name=row["account_name"],
            platform=row.get("platform") or "tiktok",
            target_country=row["target_country"],
            default_locale=row["default_locale"],
            timezone=row["timezone"],
            status=row.get("status") or "testing",
            persona_ref_id=row.get("persona_ref_id"),
            persona_snapshot_json=load_json_value(row.get("persona_snapshot_json"), {}),
            visual_identity_json=load_json_value(row.get("visual_identity_json"), {}),
            allowed_style_refs_json=load_json_value(
                row.get("allowed_style_refs_json"), []
            ),
            allowed_look_refs_json=load_json_value(row.get("allowed_look_refs_json"), []),
            allowed_scene_refs_json=load_json_value(
                row.get("allowed_scene_refs_json"), []
            ),
            core_scene_refs_json=load_json_value(row.get("core_scene_refs_json"), []),
            default_market_pack_id=row.get("default_market_pack_id"),
            default_render_preset_id=row.get("default_render_preset_id"),
            operating_rules_json=load_json_value(row.get("operating_rules_json"), {}),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


@dataclass
class ProductReferencePack:
    """Operator-owned, versioned product images used as appearance authority."""

    pack_id: str
    product_id: str
    variant_key: str = "default"
    pack_version: int = 1
    product_name: Optional[str] = None
    category: Optional[str] = None
    status: str = "limited"
    is_default: bool = False
    assets_json: List[Dict[str, Any]] = dataclasses.field(default_factory=list)
    asset_fingerprint: str = ""
    source_type: Optional[str] = None
    source_ref: Optional[str] = None
    selection_reason: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_row(self) -> Dict[str, Any]:
        return {
            "pack_id": self.pack_id,
            "product_id": self.product_id,
            "variant_key": self.variant_key,
            "pack_version": self.pack_version,
            "product_name": self.product_name,
            "category": self.category,
            "status": self.status,
            "is_default": 1 if self.is_default else 0,
            "assets_json": dump_json(self.assets_json),
            "asset_fingerprint": self.asset_fingerprint,
            "source_type": self.source_type,
            "source_ref": self.source_ref,
            "selection_reason": self.selection_reason,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "ProductReferencePack":
        return cls(
            pack_id=row["pack_id"],
            product_id=row["product_id"],
            variant_key=row.get("variant_key") or "default",
            pack_version=int(row.get("pack_version") or 1),
            product_name=row.get("product_name"),
            category=row.get("category"),
            status=row.get("status") or "limited",
            is_default=to_bool(row.get("is_default")),
            assets_json=load_json_value(row.get("assets_json"), []),
            asset_fingerprint=row.get("asset_fingerprint") or "",
            source_type=row.get("source_type"),
            source_ref=row.get("source_ref"),
            selection_reason=row.get("selection_reason"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


@dataclass
class AssetSet:
    """A versioned manifest of source photos that may be reused together.

    The files remain in the existing media storage.  This row only freezes
    their roles, hashes, identity/pair relationships, and business tags so a
    content revision can select them deterministically.
    """

    asset_set_id: str
    asset_set_key: str
    category_key: str
    manifest_json: Dict[str, Any]
    asset_set_version: int = 1
    market: Optional[str] = None
    status: str = "draft"
    tags_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_row(self) -> Dict[str, Any]:
        return {
            "asset_set_id": self.asset_set_id,
            "asset_set_key": self.asset_set_key,
            "asset_set_version": self.asset_set_version,
            "category_key": self.category_key,
            "market": self.market,
            "status": self.status,
            "tags_json": dump_json(self.tags_json),
            "manifest_json": dump_json(self.manifest_json),
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "AssetSet":
        return cls(
            asset_set_id=row["asset_set_id"],
            asset_set_key=row["asset_set_key"],
            asset_set_version=int(row.get("asset_set_version") or 1),
            category_key=row["category_key"],
            market=row.get("market"),
            status=row.get("status") or "draft",
            tags_json=load_json_value(row.get("tags_json"), {}),
            manifest_json=load_json_value(row.get("manifest_json"), {}),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


# --------------------------------------------------------------------------
# Business flow
# --------------------------------------------------------------------------

@dataclass
class ContentTask:
    task_id: str
    idempotency_key: str
    account_id: str
    product_id: Optional[str]
    target_country: str
    target_locale: str
    product_snapshot_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    # ``video`` preserves the historical flow.  Native photo tasks opt in
    # explicitly and may omit a product when ``product_mode=NO_PRODUCT``.
    media_kind: str = "video"
    category_key: Optional[str] = None
    product_mode: Optional[str] = None
    source_type: str = "manual"
    source_record_id: Optional[str] = None
    market_pack_id: Optional[str] = None
    theme_id: Optional[str] = None
    topic_text: Optional[str] = None
    task_status: str = "draft"
    current_stage: str = "intake"
    priority: str = "normal"
    requested_shot_count: int = 5
    retry_count: int = 0
    max_retry_count: int = 2
    plan_json: Optional[Dict[str, Any]] = None
    copy_json: Optional[Dict[str, Any]] = None
    group_qa_json: Optional[Dict[str, Any]] = None
    selected_render_id: Optional[str] = None
    recipe_id: Optional[str] = None
    recipe_version: Optional[int] = None
    content_goal: Optional[str] = None
    hook_strategy: Optional[str] = None
    outfit_plan_json: Optional[Dict[str, Any]] = None
    storyboard_version: Optional[str] = None
    content_package_id: Optional[str] = None
    product_facts_json: Optional[Dict[str, Any]] = None
    # Workflow V2 keeps a mutable working revision and a separately frozen
    # released revision.  Null values deliberately preserve historical rows.
    workflow_version: int = 1
    active_revision_id: Optional[str] = None
    released_revision_id: Optional[str] = None
    row_version: int = 1
    # 真实投递账号（TikTok handle）。``account_id`` 仍是内部 OPV 生产资料
    # 外键；该列只表示发布归属，空值＝沿用店铺公共池的旧行为。
    target_publish_account_id: str = ""
    failure_code: Optional[str] = None
    failure_detail: Optional[str] = None
    feishu_record_id: Optional[str] = None
    created_by: str = "manual"
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_row(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "idempotency_key": self.idempotency_key,
            "source_type": self.source_type,
            "source_record_id": self.source_record_id,
            "account_id": self.account_id,
            "product_id": self.product_id,
            "product_snapshot_json": dump_json(self.product_snapshot_json),
            "media_kind": self.media_kind,
            "category_key": self.category_key,
            "product_mode": self.product_mode,
            "target_country": self.target_country,
            "target_locale": self.target_locale,
            "market_pack_id": self.market_pack_id,
            "theme_id": self.theme_id,
            "topic_text": self.topic_text,
            "task_status": self.task_status,
            "current_stage": self.current_stage,
            "priority": self.priority,
            "requested_shot_count": self.requested_shot_count,
            "retry_count": self.retry_count,
            "max_retry_count": self.max_retry_count,
            "plan_json": dump_json(self.plan_json) if self.plan_json is not None else None,
            "copy_json": dump_json(self.copy_json) if self.copy_json is not None else None,
            "group_qa_json": (
                dump_json(self.group_qa_json) if self.group_qa_json is not None else None
            ),
            "selected_render_id": self.selected_render_id,
            "recipe_id": self.recipe_id,
            "recipe_version": self.recipe_version,
            "content_goal": self.content_goal,
            "hook_strategy": self.hook_strategy,
            "outfit_plan_json": (
                dump_json(self.outfit_plan_json)
                if self.outfit_plan_json is not None
                else None
            ),
            "storyboard_version": self.storyboard_version,
            "content_package_id": self.content_package_id,
            "product_facts_json": (
                dump_json(self.product_facts_json)
                if self.product_facts_json is not None
                else None
            ),
            "workflow_version": self.workflow_version,
            "active_revision_id": self.active_revision_id,
            "released_revision_id": self.released_revision_id,
            "row_version": self.row_version,
            "target_publish_account_id": self.target_publish_account_id,
            "failure_code": self.failure_code,
            "failure_detail": self.failure_detail,
            "feishu_record_id": self.feishu_record_id,
            "created_by": self.created_by,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "ContentTask":
        return cls(
            task_id=row["task_id"],
            idempotency_key=row["idempotency_key"],
            source_type=row.get("source_type") or "manual",
            source_record_id=row.get("source_record_id"),
            account_id=row["account_id"],
            product_id=row.get("product_id"),
            product_snapshot_json=load_json_value(row.get("product_snapshot_json"), {}),
            media_kind=row.get("media_kind") or "video",
            category_key=row.get("category_key"),
            product_mode=row.get("product_mode"),
            target_country=row["target_country"],
            target_locale=row["target_locale"],
            market_pack_id=row.get("market_pack_id"),
            theme_id=row.get("theme_id"),
            topic_text=row.get("topic_text"),
            task_status=row.get("task_status") or "draft",
            current_stage=row.get("current_stage") or "intake",
            priority=row.get("priority") or "normal",
            requested_shot_count=int(row.get("requested_shot_count") or 5),
            retry_count=int(row.get("retry_count") or 0),
            max_retry_count=int(row.get("max_retry_count") or 2),
            plan_json=load_json_value(row.get("plan_json")),
            copy_json=load_json_value(row.get("copy_json")),
            group_qa_json=load_json_value(row.get("group_qa_json")),
            selected_render_id=row.get("selected_render_id"),
            recipe_id=row.get("recipe_id"),
            recipe_version=(
                int(row["recipe_version"]) if row.get("recipe_version") is not None else None
            ),
            content_goal=row.get("content_goal"),
            hook_strategy=row.get("hook_strategy"),
            outfit_plan_json=load_json_value(row.get("outfit_plan_json")),
            storyboard_version=row.get("storyboard_version"),
            content_package_id=row.get("content_package_id"),
            product_facts_json=load_json_value(row.get("product_facts_json")),
            workflow_version=int(row.get("workflow_version") or 1),
            active_revision_id=row.get("active_revision_id"),
            released_revision_id=row.get("released_revision_id"),
            row_version=int(row.get("row_version") or 1),
            target_publish_account_id=str(row.get("target_publish_account_id") or ""),
            failure_code=row.get("failure_code"),
            failure_detail=row.get("failure_detail"),
            feishu_record_id=row.get("feishu_record_id"),
            created_by=row.get("created_by") or "manual",
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


@dataclass
class TaskRevision:
    """Frozen planning inputs plus the working selection for one production pass.

    ``asset_manifest_json`` is intentionally the authoritative selection
    manifest for a revision.  Existing shot/render tables remain the physical
    artifact history, so a second asset graph is not introduced prematurely.
    """

    revision_id: str
    task_id: str
    revision_no: int
    plan_snapshot_json: Dict[str, Any]
    input_snapshot_hash: str
    parent_revision_id: Optional[str] = None
    asset_manifest_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    selection_hash: str = ""
    rework_spec_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    revision_status: str = "working"
    lock_version: int = 1
    created_by: str = "system"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_row(self) -> Dict[str, Any]:
        return {
            "revision_id": self.revision_id,
            "task_id": self.task_id,
            "revision_no": self.revision_no,
            "parent_revision_id": self.parent_revision_id,
            "plan_snapshot_json": dump_json(self.plan_snapshot_json),
            "input_snapshot_hash": self.input_snapshot_hash,
            "asset_manifest_json": dump_json(self.asset_manifest_json),
            "selection_hash": self.selection_hash,
            "rework_spec_json": dump_json(self.rework_spec_json),
            "revision_status": self.revision_status,
            "lock_version": self.lock_version,
            "created_by": self.created_by,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "TaskRevision":
        return cls(
            revision_id=row["revision_id"], task_id=row["task_id"],
            revision_no=int(row["revision_no"]),
            parent_revision_id=row.get("parent_revision_id"),
            plan_snapshot_json=load_json_value(row.get("plan_snapshot_json"), {}),
            input_snapshot_hash=row.get("input_snapshot_hash") or "",
            asset_manifest_json=load_json_value(row.get("asset_manifest_json"), {}),
            selection_hash=row.get("selection_hash") or "",
            rework_spec_json=load_json_value(row.get("rework_spec_json"), {}),
            revision_status=row.get("revision_status") or "working",
            lock_version=int(row.get("lock_version") or 1),
            created_by=row.get("created_by") or "system",
            created_at=row.get("created_at"), updated_at=row.get("updated_at"),
        )


@dataclass
class QualityReview:
    """Append-only, scoped review bound to the exact reviewed inputs."""

    review_id: str
    revision_id: str
    scope: str
    target_id: str
    input_fingerprint: str
    quality_profile_id: str
    quality_profile_version: int
    decision: str
    reviewer_type: str
    dimensions_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    reason_codes_json: List[str] = dataclasses.field(default_factory=list)
    evidence_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    reviewer: str = "system"
    created_at: Optional[datetime] = None

    def to_row(self) -> Dict[str, Any]:
        return {
            "review_id": self.review_id, "revision_id": self.revision_id,
            "scope": self.scope, "target_id": self.target_id,
            "input_fingerprint": self.input_fingerprint,
            "quality_profile_id": self.quality_profile_id,
            "quality_profile_version": self.quality_profile_version,
            "decision": self.decision, "reviewer_type": self.reviewer_type,
            "dimensions_json": dump_json(self.dimensions_json),
            "reason_codes_json": dump_json(self.reason_codes_json),
            "evidence_json": dump_json(self.evidence_json), "reviewer": self.reviewer,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "QualityReview":
        return cls(
            review_id=row["review_id"], revision_id=row["revision_id"],
            scope=row["scope"], target_id=row["target_id"],
            input_fingerprint=row["input_fingerprint"],
            quality_profile_id=row.get("quality_profile_id") or "",
            quality_profile_version=int(row.get("quality_profile_version") or 1),
            decision=row.get("decision") or "failed",
            reviewer_type=row.get("reviewer_type") or "system",
            dimensions_json=load_json_value(row.get("dimensions_json"), {}),
            reason_codes_json=load_json_value(row.get("reason_codes_json"), []),
            evidence_json=load_json_value(row.get("evidence_json"), {}),
            reviewer=row.get("reviewer") or "system", created_at=row.get("created_at"),
        )


@dataclass
class ContentShot:
    shot_id: str
    task_id: str
    slot_index: int
    slot_role: str
    duration_ms: Optional[int]
    shot_version: int = 1
    shot_status: str = "planned"
    narrative_purpose: Optional[str] = None
    motion_preset: str = "slow_push"
    transition_in: str = "cut"
    transition_out: str = "cut"
    overlay_text: Optional[str] = None
    generation_prompt: Optional[str] = None
    negative_prompt: Optional[str] = None
    source_refs_json: List[Any] = dataclasses.field(default_factory=list)
    generation_provider: Optional[str] = None
    generation_model: Optional[str] = None
    generation_request_id: Optional[str] = None
    image_oss_object_id: Optional[str] = None
    image_url: Optional[str] = None
    image_sha256: Optional[str] = None
    image_width: Optional[int] = None
    image_height: Optional[int] = None
    is_selected: bool = False
    qa_status: str = "pending"
    qa_json: Optional[Dict[str, Any]] = None
    narrative_function: Optional[str] = None
    product_focus: Optional[str] = None
    overlay_spec_json: Optional[Dict[str, Any]] = None
    transition_hint: Optional[str] = None
    continuity_constraints_json: List[Any] = dataclasses.field(default_factory=list)
    outfit_state_ref: Optional[str] = None
    origin_revision_id: Optional[str] = None
    input_fingerprint: str = ""
    failure_detail: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_row(self) -> Dict[str, Any]:
        return {
            "shot_id": self.shot_id,
            "task_id": self.task_id,
            "slot_index": self.slot_index,
            "slot_role": self.slot_role,
            "shot_version": self.shot_version,
            "shot_status": self.shot_status,
            "narrative_purpose": self.narrative_purpose,
            "duration_ms": self.duration_ms,
            "motion_preset": self.motion_preset,
            "transition_in": self.transition_in,
            "transition_out": self.transition_out,
            "overlay_text": self.overlay_text,
            "generation_prompt": self.generation_prompt,
            "negative_prompt": self.negative_prompt,
            "source_refs_json": dump_json(self.source_refs_json),
            "generation_provider": self.generation_provider,
            "generation_model": self.generation_model,
            "generation_request_id": self.generation_request_id,
            "image_oss_object_id": self.image_oss_object_id,
            "image_url": self.image_url,
            "image_sha256": self.image_sha256,
            "image_width": self.image_width,
            "image_height": self.image_height,
            "is_selected": 1 if self.is_selected else 0,
            "qa_status": self.qa_status,
            "qa_json": dump_json(self.qa_json) if self.qa_json is not None else None,
            "narrative_function": self.narrative_function,
            "product_focus": self.product_focus,
            "overlay_spec_json": (
                dump_json(self.overlay_spec_json)
                if self.overlay_spec_json is not None
                else None
            ),
            "transition_hint": self.transition_hint,
            "continuity_constraints_json": dump_json(self.continuity_constraints_json),
            "outfit_state_ref": self.outfit_state_ref,
            "origin_revision_id": self.origin_revision_id,
            "input_fingerprint": self.input_fingerprint,
            "failure_detail": self.failure_detail,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "ContentShot":
        return cls(
            shot_id=row["shot_id"],
            task_id=row["task_id"],
            slot_index=int(row["slot_index"]),
            slot_role=row["slot_role"],
            duration_ms=(
                int(row["duration_ms"])
                if row.get("duration_ms") is not None
                else None
            ),
            shot_version=int(row.get("shot_version") or 1),
            shot_status=row.get("shot_status") or "planned",
            narrative_purpose=row.get("narrative_purpose"),
            motion_preset=row.get("motion_preset") or "slow_push",
            transition_in=row.get("transition_in") or "cut",
            transition_out=row.get("transition_out") or "cut",
            overlay_text=row.get("overlay_text"),
            generation_prompt=row.get("generation_prompt"),
            negative_prompt=row.get("negative_prompt"),
            source_refs_json=load_json_value(row.get("source_refs_json"), []),
            generation_provider=row.get("generation_provider"),
            generation_model=row.get("generation_model"),
            generation_request_id=row.get("generation_request_id"),
            image_oss_object_id=row.get("image_oss_object_id"),
            image_url=row.get("image_url"),
            image_sha256=row.get("image_sha256"),
            image_width=(
                int(row["image_width"]) if row.get("image_width") is not None else None
            ),
            image_height=(
                int(row["image_height"]) if row.get("image_height") is not None else None
            ),
            is_selected=to_bool(row.get("is_selected")),
            qa_status=row.get("qa_status") or "pending",
            qa_json=load_json_value(row.get("qa_json")),
            narrative_function=row.get("narrative_function"),
            product_focus=row.get("product_focus"),
            overlay_spec_json=load_json_value(row.get("overlay_spec_json")),
            transition_hint=row.get("transition_hint"),
            continuity_constraints_json=load_json_value(
                row.get("continuity_constraints_json"), []
            ),
            outfit_state_ref=row.get("outfit_state_ref"),
            origin_revision_id=row.get("origin_revision_id"),
            input_fingerprint=row.get("input_fingerprint") or "",
            failure_detail=row.get("failure_detail"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


@dataclass
class VideoRender:
    render_id: str
    task_id: str
    render_preset_id: str
    copy_snapshot_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    shot_selection_json: List[Any] = dataclasses.field(default_factory=list)
    timeline_json: List[Any] = dataclasses.field(default_factory=list)
    render_version: int = 1
    render_mode: str = "still_slideshow"
    render_engine: str = "ffmpeg"
    render_status: str = "queued"
    bgm_ref_id: Optional[str] = None
    bgm_oss_object_id: Optional[str] = None
    duration_ms: Optional[int] = None
    output_oss_object_id: Optional[str] = None
    output_url: Optional[str] = None
    output_sha256: Optional[str] = None
    output_metadata_json: Optional[Dict[str, Any]] = None
    qc_status: str = "pending"
    qc_json: Optional[Dict[str, Any]] = None
    publish_ready: bool = False
    origin_revision_id: Optional[str] = None
    input_fingerprint: str = ""
    failure_detail: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_row(self) -> Dict[str, Any]:
        return {
            "render_id": self.render_id,
            "task_id": self.task_id,
            "render_version": self.render_version,
            "render_preset_id": self.render_preset_id,
            "render_mode": self.render_mode,
            "render_engine": self.render_engine,
            "render_status": self.render_status,
            "shot_selection_json": dump_json(self.shot_selection_json),
            "timeline_json": dump_json(self.timeline_json),
            "copy_snapshot_json": dump_json(self.copy_snapshot_json),
            "bgm_ref_id": self.bgm_ref_id,
            "bgm_oss_object_id": self.bgm_oss_object_id,
            "duration_ms": self.duration_ms,
            "output_oss_object_id": self.output_oss_object_id,
            "output_url": self.output_url,
            "output_sha256": self.output_sha256,
            "output_metadata_json": (
                dump_json(self.output_metadata_json)
                if self.output_metadata_json is not None
                else None
            ),
            "qc_status": self.qc_status,
            "qc_json": dump_json(self.qc_json) if self.qc_json is not None else None,
            "publish_ready": 1 if self.publish_ready else 0,
            "origin_revision_id": self.origin_revision_id,
            "input_fingerprint": self.input_fingerprint,
            "failure_detail": self.failure_detail,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "VideoRender":
        return cls(
            render_id=row["render_id"],
            task_id=row["task_id"],
            render_version=int(row.get("render_version") or 1),
            render_preset_id=row["render_preset_id"],
            render_mode=row.get("render_mode") or "still_slideshow",
            render_engine=row.get("render_engine") or "ffmpeg",
            render_status=row.get("render_status") or "queued",
            shot_selection_json=load_json_value(row.get("shot_selection_json"), []),
            timeline_json=load_json_value(row.get("timeline_json"), []),
            copy_snapshot_json=load_json_value(row.get("copy_snapshot_json"), {}),
            bgm_ref_id=row.get("bgm_ref_id"),
            bgm_oss_object_id=row.get("bgm_oss_object_id"),
            duration_ms=(
                int(row["duration_ms"]) if row.get("duration_ms") is not None else None
            ),
            output_oss_object_id=row.get("output_oss_object_id"),
            output_url=row.get("output_url"),
            output_sha256=row.get("output_sha256"),
            output_metadata_json=load_json_value(row.get("output_metadata_json")),
            qc_status=row.get("qc_status") or "pending",
            qc_json=load_json_value(row.get("qc_json")),
            publish_ready=to_bool(row.get("publish_ready")),
            origin_revision_id=row.get("origin_revision_id"),
            input_fingerprint=row.get("input_fingerprint") or "",
            failure_detail=row.get("failure_detail"),
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


@dataclass
class PublishRecord:
    publish_id: str
    task_id: str
    render_id: Optional[str]
    account_id: str
    caption_snapshot_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    media_kind: str = "video"
    content_package_id: Optional[str] = None
    revision_id: Optional[str] = None
    main_slot_id: Optional[int] = None
    publisher_account_id: Optional[str] = None
    publish_channel: Optional[str] = None
    provider_task_id: Optional[str] = None
    publish_key: Optional[str] = None
    release_manifest_json: Optional[Dict[str, Any]] = None
    platform: str = "tiktok"
    publish_status: str = "ready"
    publish_mode: str = "manual"
    external_post_id: Optional[str] = None
    external_post_url: Optional[str] = None
    cover_shot_id: Optional[str] = None
    operator_name: Optional[str] = None
    planned_publish_at: Optional[datetime] = None
    submitted_at: Optional[datetime] = None
    published_at: Optional[datetime] = None
    platform_metadata_json: Optional[Dict[str, Any]] = None
    failure_detail: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_row(self) -> Dict[str, Any]:
        return {
            "publish_id": self.publish_id,
            "task_id": self.task_id,
            "render_id": self.render_id,
            "account_id": self.account_id,
            "media_kind": self.media_kind,
            "content_package_id": self.content_package_id,
            "revision_id": self.revision_id,
            "main_slot_id": self.main_slot_id,
            "publisher_account_id": self.publisher_account_id,
            "publish_channel": self.publish_channel,
            "provider_task_id": self.provider_task_id,
            "publish_key": self.publish_key,
            "release_manifest_json": (
                dump_json(self.release_manifest_json)
                if self.release_manifest_json is not None
                else None
            ),
            "platform": self.platform,
            "publish_status": self.publish_status,
            "publish_mode": self.publish_mode,
            "external_post_id": self.external_post_id,
            "external_post_url": self.external_post_url,
            "caption_snapshot_json": dump_json(self.caption_snapshot_json),
            "cover_shot_id": self.cover_shot_id,
            "operator_name": self.operator_name,
            "planned_publish_at": self.planned_publish_at,
            "submitted_at": self.submitted_at,
            "published_at": self.published_at,
            "platform_metadata_json": (
                dump_json(self.platform_metadata_json)
                if self.platform_metadata_json is not None
                else None
            ),
            "failure_detail": self.failure_detail,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "PublishRecord":
        return cls(
            publish_id=row["publish_id"],
            task_id=row["task_id"],
            render_id=row.get("render_id"),
            account_id=row["account_id"],
            media_kind=row.get("media_kind") or "video",
            content_package_id=row.get("content_package_id"),
            revision_id=row.get("revision_id"),
            main_slot_id=(
                int(row["main_slot_id"])
                if row.get("main_slot_id") is not None
                else None
            ),
            publisher_account_id=row.get("publisher_account_id"),
            publish_channel=row.get("publish_channel"),
            provider_task_id=row.get("provider_task_id"),
            publish_key=row.get("publish_key"),
            release_manifest_json=load_json_value(row.get("release_manifest_json")),
            platform=row.get("platform") or "tiktok",
            publish_status=row.get("publish_status") or "ready",
            publish_mode=row.get("publish_mode") or "manual",
            external_post_id=row.get("external_post_id"),
            external_post_url=row.get("external_post_url"),
            caption_snapshot_json=load_json_value(row.get("caption_snapshot_json"), {}),
            cover_shot_id=row.get("cover_shot_id"),
            operator_name=row.get("operator_name"),
            planned_publish_at=row.get("planned_publish_at"),
            submitted_at=row.get("submitted_at"),
            published_at=row.get("published_at"),
            platform_metadata_json=load_json_value(row.get("platform_metadata_json")),
            failure_detail=row.get("failure_detail"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


@dataclass
class MetricSnapshot:
    publish_id: str
    captured_at: datetime
    metric_id: Optional[int] = None
    captured_after_hours: int = 24
    source_type: str = "manual"
    view_count: int = 0
    like_count: int = 0
    comment_count: int = 0
    share_count: int = 0
    save_count: int = 0
    profile_visit_count: int = 0
    follower_gain_count: int = 0
    avg_watch_time_ms: Optional[int] = None
    completion_rate: Optional[float] = None
    engagement_rate: Optional[float] = None
    raw_metrics_json: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_row(self) -> Dict[str, Any]:
        row: Dict[str, Any] = {
            "publish_id": self.publish_id,
            "captured_after_hours": self.captured_after_hours,
            "captured_at": self.captured_at,
            "source_type": self.source_type,
            "view_count": self.view_count,
            "like_count": self.like_count,
            "comment_count": self.comment_count,
            "share_count": self.share_count,
            "save_count": self.save_count,
            "profile_visit_count": self.profile_visit_count,
            "follower_gain_count": self.follower_gain_count,
            "avg_watch_time_ms": self.avg_watch_time_ms,
            "completion_rate": self.completion_rate,
            "engagement_rate": self.engagement_rate,
            "raw_metrics_json": (
                dump_json(self.raw_metrics_json)
                if self.raw_metrics_json is not None
                else None
            ),
        }
        if self.metric_id is not None:
            row["metric_id"] = self.metric_id
        return row

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "MetricSnapshot":
        return cls(
            metric_id=int(row["metric_id"]) if row.get("metric_id") is not None else None,
            publish_id=row["publish_id"],
            captured_after_hours=int(row.get("captured_after_hours") or 24),
            captured_at=row["captured_at"],
            source_type=row.get("source_type") or "manual",
            view_count=int(row.get("view_count") or 0),
            like_count=int(row.get("like_count") or 0),
            comment_count=int(row.get("comment_count") or 0),
            share_count=int(row.get("share_count") or 0),
            save_count=int(row.get("save_count") or 0),
            profile_visit_count=int(row.get("profile_visit_count") or 0),
            follower_gain_count=int(row.get("follower_gain_count") or 0),
            avg_watch_time_ms=(
                int(row["avg_watch_time_ms"])
                if row.get("avg_watch_time_ms") is not None
                else None
            ),
            completion_rate=to_float(row.get("completion_rate")),
            engagement_rate=to_float(row.get("engagement_rate")),
            raw_metrics_json=load_json_value(row.get("raw_metrics_json")),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


@dataclass
class LookFeedback:
    feedback_id: str
    task_id: str
    account_id: str
    product_id: str
    feedback_type: str
    decision: str
    reason_codes_json: List[str] = dataclasses.field(default_factory=list)
    score_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    look_ref_id: Optional[str] = None
    evidence_json: Optional[Dict[str, Any]] = None
    promotion_status: str = "not_requested"
    successful_look_ref_id: Optional[str] = None
    reviewer: Optional[str] = None
    notes: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_row(self) -> Dict[str, Any]:
        return {
            "feedback_id": self.feedback_id,
            "task_id": self.task_id,
            "account_id": self.account_id,
            "product_id": self.product_id,
            "look_ref_id": self.look_ref_id,
            "feedback_type": self.feedback_type,
            "decision": self.decision,
            "reason_codes_json": dump_json(self.reason_codes_json),
            "score_json": dump_json(self.score_json),
            "evidence_json": (
                dump_json(self.evidence_json) if self.evidence_json is not None else None
            ),
            "promotion_status": self.promotion_status,
            "successful_look_ref_id": self.successful_look_ref_id,
            "reviewer": self.reviewer,
            "notes": self.notes,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "LookFeedback":
        return cls(
            feedback_id=row["feedback_id"],
            task_id=row["task_id"],
            account_id=row["account_id"],
            product_id=row["product_id"],
            look_ref_id=row.get("look_ref_id"),
            feedback_type=row["feedback_type"],
            decision=row["decision"],
            reason_codes_json=load_json_value(row.get("reason_codes_json"), []),
            score_json=load_json_value(row.get("score_json"), {}),
            evidence_json=load_json_value(row.get("evidence_json")),
            promotion_status=row.get("promotion_status") or "not_requested",
            successful_look_ref_id=row.get("successful_look_ref_id"),
            reviewer=row.get("reviewer"),
            notes=row.get("notes"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


@dataclass
class FeishuOutbox:
    outbox_id: str
    aggregate_type: str
    aggregate_id: str
    operation: str
    payload_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    target_app_token: Optional[str] = None
    target_table_id: Optional[str] = None
    target_record_id: Optional[str] = None
    status: str = "holding"
    attempts: int = 0
    not_before: Optional[datetime] = None
    last_error: Optional[str] = None
    synced_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_row(self) -> Dict[str, Any]:
        return {
            "outbox_id": self.outbox_id,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "operation": self.operation,
            "target_app_token": self.target_app_token,
            "target_table_id": self.target_table_id,
            "target_record_id": self.target_record_id,
            "payload_json": dump_json(self.payload_json),
            "status": self.status,
            "attempts": self.attempts,
            "not_before": self.not_before,
            "last_error": self.last_error,
            "synced_at": self.synced_at,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "FeishuOutbox":
        return cls(
            outbox_id=row["outbox_id"],
            aggregate_type=row["aggregate_type"],
            aggregate_id=row["aggregate_id"],
            operation=row["operation"],
            target_app_token=row.get("target_app_token"),
            target_table_id=row.get("target_table_id"),
            target_record_id=row.get("target_record_id"),
            payload_json=load_json_value(row.get("payload_json"), {}),
            status=row.get("status") or "holding",
            attempts=int(row.get("attempts") or 0),
            not_before=row.get("not_before"),
            last_error=row.get("last_error"),
            synced_at=row.get("synced_at"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


# --------------------------------------------------------------------------
# Capability upgrade: recipe / profiles / content package
# --------------------------------------------------------------------------

@dataclass
class ContentRecipe:
    recipe_id: str
    recipe_key: str
    content_goal: str
    hook_types_json: List[str] = dataclasses.field(default_factory=list)
    story_structure_json: List[Any] = dataclasses.field(default_factory=list)
    copy_style_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    suitable_topics_json: List[str] = dataclasses.field(default_factory=list)
    recipe_spec_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    recipe_version: int = 1
    anchor_slot: int = 1
    shot_count: int = 5
    render_profile_id: Optional[str] = None
    quality_profile_id: Optional[str] = None
    status: str = "draft"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "ContentRecipe":
        return _from_payload(cls, payload)

    def to_row(self) -> Dict[str, Any]:
        return {
            "recipe_id": self.recipe_id,
            "recipe_key": self.recipe_key,
            "recipe_version": self.recipe_version,
            "content_goal": self.content_goal,
            "anchor_slot": self.anchor_slot,
            "shot_count": self.shot_count,
            "hook_types_json": dump_json(self.hook_types_json),
            "story_structure_json": dump_json(self.story_structure_json),
            "copy_style_json": dump_json(self.copy_style_json),
            "render_profile_id": self.render_profile_id,
            "quality_profile_id": self.quality_profile_id,
            "suitable_topics_json": dump_json(self.suitable_topics_json),
            "recipe_spec_json": dump_json(self.recipe_spec_json),
            "status": self.status,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "ContentRecipe":
        return cls(
            recipe_id=row["recipe_id"],
            recipe_key=row["recipe_key"],
            recipe_version=int(row["recipe_version"]),
            content_goal=row["content_goal"],
            anchor_slot=int(row.get("anchor_slot") or 1),
            shot_count=int(row.get("shot_count") or 5),
            hook_types_json=load_json_value(row.get("hook_types_json"), []),
            story_structure_json=load_json_value(row.get("story_structure_json"), []),
            copy_style_json=load_json_value(row.get("copy_style_json"), {}),
            render_profile_id=row.get("render_profile_id"),
            quality_profile_id=row.get("quality_profile_id"),
            suitable_topics_json=load_json_value(row.get("suitable_topics_json"), []),
            recipe_spec_json=load_json_value(row.get("recipe_spec_json"), {}),
            status=row.get("status") or "draft",
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


@dataclass
class RenderProfile:
    render_profile_id: str
    profile_key: str
    motion_rules_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    transition_rules_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    profile_version: int = 1
    aspect_ratio: str = "9:16"
    duration_min_ms: int = 10000
    duration_max_ms: int = 15000
    fps: int = 30
    codec: str = "h264"
    hook_window_ms: int = 2000
    status: str = "draft"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "RenderProfile":
        return _from_payload(cls, payload)

    def to_row(self) -> Dict[str, Any]:
        return {
            "render_profile_id": self.render_profile_id,
            "profile_key": self.profile_key,
            "profile_version": self.profile_version,
            "aspect_ratio": self.aspect_ratio,
            "duration_min_ms": self.duration_min_ms,
            "duration_max_ms": self.duration_max_ms,
            "fps": self.fps,
            "codec": self.codec,
            "hook_window_ms": self.hook_window_ms,
            "motion_rules_json": dump_json(self.motion_rules_json),
            "transition_rules_json": dump_json(self.transition_rules_json),
            "status": self.status,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "RenderProfile":
        return cls(
            render_profile_id=row["render_profile_id"],
            profile_key=row["profile_key"],
            profile_version=int(row["profile_version"]),
            aspect_ratio=row.get("aspect_ratio") or "9:16",
            duration_min_ms=int(row.get("duration_min_ms") or 10000),
            duration_max_ms=int(row.get("duration_max_ms") or 15000),
            fps=int(row.get("fps") or 30),
            codec=row.get("codec") or "h264",
            hook_window_ms=int(row.get("hook_window_ms") or 2000),
            motion_rules_json=load_json_value(row.get("motion_rules_json"), {}),
            transition_rules_json=load_json_value(row.get("transition_rules_json"), {}),
            status=row.get("status") or "draft",
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


@dataclass
class QualityProfile:
    quality_profile_id: str
    profile_key: str
    dimensions_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    profile_version: int = 1
    min_overall_score: Optional[float] = None
    status: str = "draft"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "QualityProfile":
        return _from_payload(cls, payload)

    def to_row(self) -> Dict[str, Any]:
        return {
            "quality_profile_id": self.quality_profile_id,
            "profile_key": self.profile_key,
            "profile_version": self.profile_version,
            "dimensions_json": dump_json(self.dimensions_json),
            "min_overall_score": self.min_overall_score,
            "status": self.status,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "QualityProfile":
        return cls(
            quality_profile_id=row["quality_profile_id"],
            profile_key=row["profile_key"],
            profile_version=int(row["profile_version"]),
            dimensions_json=load_json_value(row.get("dimensions_json"), {}),
            min_overall_score=to_float(row.get("min_overall_score")),
            status=row.get("status") or "draft",
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


@dataclass
class ContentPackage:
    content_package_id: str
    task_id: str
    selected_image_ids_json: List[Any] = dataclasses.field(default_factory=list)
    hashtags_json: List[str] = dataclasses.field(default_factory=list)
    render_ids_json: List[Any] = dataclasses.field(default_factory=list)
    generation_lineage_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    photo_manifest_json: Optional[Dict[str, Any]] = None
    product_snapshot_id: Optional[str] = None
    recipe_id: Optional[str] = None
    theme_id: Optional[str] = None
    outfit_plan_id: Optional[str] = None
    storyboard_version: Optional[str] = None
    cover_image_id: Optional[str] = None
    cover_title: Optional[str] = None
    caption: Optional[str] = None
    qa_summary_json: Optional[Dict[str, Any]] = None
    content_fingerprint: Optional[str] = None
    status: str = "planning"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_row(self) -> Dict[str, Any]:
        return {
            "content_package_id": self.content_package_id,
            "task_id": self.task_id,
            "product_snapshot_id": self.product_snapshot_id,
            "recipe_id": self.recipe_id,
            "theme_id": self.theme_id,
            "outfit_plan_id": self.outfit_plan_id,
            "storyboard_version": self.storyboard_version,
            "selected_image_ids_json": dump_json(self.selected_image_ids_json),
            "cover_image_id": self.cover_image_id,
            "cover_title": self.cover_title,
            "caption": self.caption,
            "hashtags_json": dump_json(self.hashtags_json),
            "render_ids_json": dump_json(self.render_ids_json),
            "qa_summary_json": (
                dump_json(self.qa_summary_json)
                if self.qa_summary_json is not None
                else None
            ),
            "content_fingerprint": self.content_fingerprint,
            "generation_lineage_json": dump_json(self.generation_lineage_json),
            "photo_manifest_json": (
                dump_json(self.photo_manifest_json)
                if self.photo_manifest_json is not None
                else None
            ),
            "status": self.status,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "ContentPackage":
        return cls(
            content_package_id=row["content_package_id"],
            task_id=row["task_id"],
            product_snapshot_id=row.get("product_snapshot_id"),
            recipe_id=row.get("recipe_id"),
            theme_id=row.get("theme_id"),
            outfit_plan_id=row.get("outfit_plan_id"),
            storyboard_version=row.get("storyboard_version"),
            selected_image_ids_json=load_json_value(
                row.get("selected_image_ids_json"), []
            ),
            cover_image_id=row.get("cover_image_id"),
            cover_title=row.get("cover_title"),
            caption=row.get("caption"),
            hashtags_json=load_json_value(row.get("hashtags_json"), []),
            render_ids_json=load_json_value(row.get("render_ids_json"), []),
            qa_summary_json=load_json_value(row.get("qa_summary_json")),
            content_fingerprint=row.get("content_fingerprint"),
            generation_lineage_json=load_json_value(
                row.get("generation_lineage_json"), {}
            ),
            photo_manifest_json=load_json_value(row.get("photo_manifest_json")),
            status=row.get("status") or "planning",
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


@dataclass
class ProductionBatch:
    """Frozen workbench batch intent plus recoverable run/projection metadata."""

    batch_id: str
    source_record_id: str
    expected_count: int
    manifest_json: Dict[str, Any] = dataclasses.field(default_factory=dict)
    source_type: str = "feishu_opv"
    pending_fields_json: Optional[Dict[str, Any]] = None
    batch_status: str = "planned"
    lock_version: int = 1
    run_owner: Optional[str] = None
    lease_until: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_row(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id, "source_type": self.source_type,
            "source_record_id": self.source_record_id, "expected_count": self.expected_count,
            "manifest_json": dump_json(self.manifest_json),
            "pending_fields_json": dump_json(self.pending_fields_json) if self.pending_fields_json is not None else None,
            "batch_status": self.batch_status, "lock_version": self.lock_version,
            "run_owner": self.run_owner, "lease_until": self.lease_until,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "ProductionBatch":
        return cls(
            batch_id=row["batch_id"], source_record_id=row["source_record_id"],
            expected_count=int(row["expected_count"]), source_type=row.get("source_type") or "feishu_opv",
            manifest_json=load_json_value(row.get("manifest_json"), {}),
            pending_fields_json=load_json_value(row.get("pending_fields_json")),
            batch_status=row.get("batch_status") or "planned", lock_version=int(row.get("lock_version") or 1),
            run_owner=row.get("run_owner"), lease_until=row.get("lease_until"),
            created_at=row.get("created_at"), updated_at=row.get("updated_at"),
        )
