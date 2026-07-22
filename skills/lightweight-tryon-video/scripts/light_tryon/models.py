from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ProductInput:
    product_id: str
    product_name: str
    market: str = "TH"
    language: str = "th"
    category: str = "top"
    sub_category: str = ""
    product_title: str = ""
    product_images: list[str] = field(default_factory=list)
    core_selling_points: list[str] = field(default_factory=list)
    recommended_scene_pool: list[str] = field(default_factory=list)
    recommended_action_pool: list[str] = field(default_factory=list)
    recommended_styling_pool: list[str] = field(default_factory=list)
    subtitle_angle_pool: list[str] = field(default_factory=list)
    target_publish_count: int = 4
    status: str = "ready"
    notes: str = ""
    enable_light_video: bool = True
    default_persona_id: str = ""
    account_id: str = ""
    generation_priority: str = "medium"
    light_video_status: str = "pending"
    light_video_notes: str = ""
    source_script_record_id: str = ""
    source_product_code: str = ""
    shot_plan_id: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProductInput":
        required = ["product_id", "product_name"]
        missing = [key for key in required if not str(data.get(key, "")).strip()]
        if missing:
            raise ValueError(f"商品缺少必填字段: {', '.join(missing)}")
        count = int(data.get("target_publish_count") or 4)
        if count < 1 or count > 100:
            raise ValueError("target_publish_count 必须在 1-100 之间")
        from .utils import normalized_list

        return cls(
            product_id=str(data["product_id"]).strip(),
            product_name=str(data["product_name"]).strip(),
            market=str(data.get("market") or "TH").strip().upper(),
            language=str(data.get("language") or "th").strip().lower(),
            category=str(data.get("category") or "top").strip(),
            sub_category=str(data.get("sub_category") or "").strip(),
            product_title=str(data.get("product_title") or data.get("product_name") or "").strip(),
            product_images=normalized_list(data.get("product_images")),
            core_selling_points=normalized_list(data.get("core_selling_points")),
            recommended_scene_pool=normalized_list(data.get("recommended_scene_pool")),
            recommended_action_pool=normalized_list(data.get("recommended_action_pool")),
            recommended_styling_pool=normalized_list(data.get("recommended_styling_pool")),
            subtitle_angle_pool=normalized_list(data.get("subtitle_angle_pool")),
            target_publish_count=count,
            status=str(data.get("status") or "ready").strip(),
            notes=str(data.get("notes") or "").strip(),
            enable_light_video=str(data.get("enable_light_video", True)).strip().lower() not in {"0", "false", "no", "off", "否"},
            default_persona_id=str(data.get("default_persona_id") or "").strip(),
            account_id=str(data.get("account_id") or "").strip(),
            generation_priority=str(data.get("generation_priority") or "medium").strip().lower(),
            light_video_status=str(data.get("light_video_status") or "pending").strip(),
            light_video_notes=str(data.get("light_video_notes") or "").strip(),
            source_script_record_id=str(data.get("source_script_record_id") or "").strip(),
            source_product_code=str(data.get("source_product_code") or "").strip(),
            shot_plan_id=str(data.get("shot_plan_id") or "").strip(),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "product_id": self.product_id,
            "product_name": self.product_name,
            "market": self.market,
            "language": self.language,
            "category": self.category,
            "sub_category": self.sub_category,
            "product_title": self.product_title,
            "product_images": self.product_images,
            "core_selling_points": self.core_selling_points,
            "recommended_scene_pool": self.recommended_scene_pool,
            "recommended_action_pool": self.recommended_action_pool,
            "recommended_styling_pool": self.recommended_styling_pool,
            "subtitle_angle_pool": self.subtitle_angle_pool,
            "target_publish_count": self.target_publish_count,
            "status": self.status,
            "notes": self.notes,
            "enable_light_video": self.enable_light_video,
            "default_persona_id": self.default_persona_id,
            "account_id": self.account_id,
            "generation_priority": self.generation_priority,
            "light_video_status": self.light_video_status,
            "light_video_notes": self.light_video_notes,
            "source_script_record_id": self.source_script_record_id,
            "source_product_code": self.source_product_code,
            "shot_plan_id": self.shot_plan_id,
        }


@dataclass(frozen=True)
class PlannedJob:
    job_id: str
    product_id: str
    market: str
    language: str
    persona_id: str
    scene_id: str
    shot_profile_id: str
    shot_plan_id: str
    action_id: str
    styling_id: str
    subtitle_id: str
    duration_seconds: int
    variant_no: int
    publish_priority: int
    plan_version: str = "v1"
    account_id: str = ""
    source_script_record_id: str = ""
    visual_plan_id: str = ""
    outfit_image_path: str = ""
    outfit_image_url: str = ""
    outfit_image_version: str = ""
    legacy_job: bool = False

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


@dataclass(frozen=True)
class ContentStrategy:
    """A reusable hook/selling-point strategy; repetition is intentionally allowed."""

    strategy_group_id: str
    product_id: str
    hook_id: str
    hook_name: str
    hook_type: str
    primary_selling_point: str
    secondary_selling_points: list[str] = field(default_factory=list)
    visual_focus: str = ""
    required_evidence: list[str] = field(default_factory=list)
    selection_weight: float = 1.0
    plan_version: str = "narrative-v1"
    status: str = "active"
    source_payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


@dataclass(frozen=True)
class NarrativeVariant:
    """One executable variant under a strategy group.

    ``basic_8_10`` remains video-first. ``enhanced_18_24`` is planned from the
    hook/selling-point combination and then aligned to the existing voiceover.
    """

    variant_id: str
    strategy_group_id: str
    product_id: str
    format_type: str
    target_duration_seconds: int
    variant_no: int
    execution_seed: str
    plan_version: str = "narrative-v1"
    workflow_state: str = "planned"
    source_job_id: str = ""
    production_batch_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()
