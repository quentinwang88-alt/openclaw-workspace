"""Compatibility wrapper around the preset-bound photo content planner."""
from __future__ import annotations

from typing import Any, Mapping
from services.photo_content_planner import plan_th_choice_batch


_LEGACY_AXES = (
    ("warm_neutral", "暖中性色层搭", "城市日常", "暖棕、奶油白"),
    ("dark_clean", "深色利落穿搭", "通勤街道", "黑、灰、深蓝"),
    ("denim_mix", "牛仔休闲组合", "周末街区", "靛蓝、棕、奶油白"),
    ("soft_earth", "柔和大地色", "自然光咖啡店", "燕麦、橄榄绿、可可棕"),
    ("monochrome", "黑白灰极简", "写字楼", "黑、白、银灰"),
    ("sporty_layer", "运动休闲层搭", "机场步行", "海军蓝、灰、酒红"),
    ("feminine_layer", "柔和精致层搭", "书店咖啡", "奶油、淡粉、可可"),
    ("travel_layer", "旅行轻层搭", "机场到市区", "浅驼、深蓝、奶油白"),
    ("color_point", "低饱和亮点色", "自然光街角", "芥末黄、森林绿、酒红"),
)


def plan_batch_variations(*, record_id: str, theme: Mapping[str, Any], count: int) -> list[dict[str, Any]]:
    # Preserve old preset/qualified-asset tasks that do not opt into a theme.
    # Reference-driven TH V3 runs use the concrete planner in feishu_workflow.
    if str(theme.get("theme_key") or "") == "AUTO":
        start = sum(record_id.encode("utf-8")) % len(_LEGACY_AXES)
        result = []
        for index in range(1, count + 1):
            key, angle, scene, palette = _LEGACY_AXES[(start + index - 1) % len(_LEGACY_AXES)]
            result.append({
                "variation_id": key, "family_id": key, "index": index,
                "theme_key": "AUTO", "angle_zh": angle,
                "scene_zh": scene, "palette_zh": palette,
                "style_modifier": f"本篇使用{palette}，保持{angle}",
            })
        return result
    return plan_th_choice_batch(
        record_id=record_id, recipe_id="PHOTO_TH_PICK_YOUR_LOOK_V3",
        theme=theme, reference_mode="STYLE", count=count,
    )["items"]
