"""Single ownership decision for rows in the shared production script pool."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping


class ProductionRoute(str, Enum):
    ORIGINAL_LONGFORM = "ORIGINAL_LONGFORM"
    REMAKE_SEGMENTED = "REMAKE_SEGMENTED"
    SHORT_VIDEO_RUN_MANAGER = "SHORT_VIDEO_RUN_MANAGER"
    INVALID = "INVALID"


@dataclass(frozen=True)
class RouteDecision:
    route: ProductionRoute
    reason: str
    conflict: str = ""


REMAKE_SOURCES = {"视频复刻", "成功脚本复刻"}


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return "".join(_text(item) for item in value)
    if isinstance(value, dict):
        return str(value.get("text") or value.get("name") or "").strip()
    return str(value).strip()


def _duration(value: Any) -> int:
    try:
        return int(float(_text(value) or 15))
    except (TypeError, ValueError):
        return 15


def classify_production_route(
    fields: Mapping[str, Any], mapping: Mapping[str, str | None]
) -> RouteDecision:
    def value(name: str) -> str:
        field = mapping.get(name)
        return _text(fields.get(field)) if field else ""

    source = value("script_source")
    video_format = value("video_format")
    duration = _duration(fields.get(mapping.get("video_duration"))) if mapping.get("video_duration") else 15
    is_remake = source in REMAKE_SOURCES
    if is_remake and duration > 15:
        conflict = ""
        if video_format and video_format not in {"长视频", "分段视频"}:
            conflict = f"视频形态={video_format} 与 {duration} 秒复刻执行要求冲突"
        return RouteDecision(
            ProductionRoute.REMAKE_SEGMENTED,
            f"脚本来源={source} 且视频时长={duration}s，需要复刻分段执行器",
            conflict,
        )
    if video_format == "长视频":
        if is_remake:
            return RouteDecision(
                ProductionRoute.REMAKE_SEGMENTED,
                f"脚本来源={source} 且显式标记长视频",
            )
        return RouteDecision(ProductionRoute.ORIGINAL_LONGFORM, "显式标记原创长视频")
    return RouteDecision(ProductionRoute.SHORT_VIDEO_RUN_MANAGER, f"单段执行，视频时长={duration}s")

