"""共享视觉预设（2026-09-15 轮 Phase 2）。

一个视觉预设合并此前零散的背景方式/摄影基准/排版选择，是小型版本化配置：
- ``background.mode``：fixed（固定背景）｜scene（目的地场景）；
- ``photography_baseline_zh``：光线/色调/景别倾向（账号风格文字仍可叠加微调）；
- ``layout``：排版方案引用（Phase 3 新排版器消费；v1 先冻结引用）；
- ``version``：历史任务按冻结快照恢复。

解析优先级（按项）：本篇任务选择 → 账号默认 → 生产入口默认。同一预设可被
多账号使用；预设不携带任何账号身份，生成差异不允许按账号名分支。
"""
from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

PRESET_SCHEMA_VERSION = "opv-visual-preset-v1"
BACKGROUND_MODES = ("fixed", "scene")
BACKGROUND_KINDS = ("solid", "indoor", "destination")

_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "visual_presets"


class VisualPresetError(RuntimeError):
    pass


@lru_cache(maxsize=1)
def visual_presets() -> Dict[str, Dict[str, Any]]:
    """按 preset_id 索引的全部视觉预设（含 name 别名解析用）。"""
    presets: Dict[str, Dict[str, Any]] = {}
    if not _CONFIG_DIR.is_dir():
        return presets
    for path in sorted(_CONFIG_DIR.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        try:
            preset = normalize_visual_preset(payload)
        except VisualPresetError:
            continue
        presets[str(preset["preset_id"])] = preset
    return presets


def normalize_visual_preset(payload: Dict[str, Any]) -> Dict[str, Any]:
    if str(payload.get("schema_version") or "") != PRESET_SCHEMA_VERSION:
        raise VisualPresetError("视觉预设 schema 版本不受支持")
    preset_id = str(payload.get("preset_id") or "").strip()
    name = str(payload.get("name") or "").strip()
    if not preset_id or not name:
        raise VisualPresetError("视觉预设缺少 preset_id/name")
    background = dict(payload.get("background") or {})
    mode = str(background.get("mode") or "")
    if mode not in BACKGROUND_MODES:
        raise VisualPresetError(f"视觉预设 {preset_id} 的 background.mode 无效")
    kind = str(background.get("kind") or "").strip()
    if not kind:
        # 兼容缺省：fixed 默认 indoor、scene 默认 destination；不靠中文描述猜。
        kind = "indoor" if mode == "fixed" else "destination"
    if kind not in BACKGROUND_KINDS:
        raise VisualPresetError(f"视觉预设 {preset_id} 的 background.kind 无效：{kind}")
    if mode == "scene" and kind != "destination":
        raise VisualPresetError(f"视觉预设 {preset_id}：scene 模式 kind 必须是 destination")
    if mode == "fixed" and kind == "destination":
        raise VisualPresetError(f"视觉预设 {preset_id}：fixed 模式 kind 不得是 destination")
    background["kind"] = kind
    preset = {
        "schema_version": PRESET_SCHEMA_VERSION,
        "preset_id": preset_id,
        "name": name,
        "applicable": dict(payload.get("applicable") or {}),
        "background": background,
        "photography_baseline_zh": str(
            payload.get("photography_baseline_zh") or "").strip(),
        "layout": dict(payload.get("layout") or {}),
        "fallback_zh": str(payload.get("fallback_zh") or "").strip(),
        "version": int(payload.get("version") or 1),
    }
    return preset


def resolve_visual_preset(value: Any) -> Dict[str, Any]:
    """按 preset_id 或显示名解析；不存在时报错（不静默回退）。"""
    text = str(value or "").strip()
    if not text:
        raise VisualPresetError("视觉预设为空")
    presets = visual_presets()
    if text in presets:
        return dict(presets[text])
    for preset in presets.values():
        if text == str(preset.get("name") or ""):
            return dict(preset)
    raise VisualPresetError(
        f"未知的视觉预设：{text}；可选：" + "、".join(
            f'{p["name"]}({p["preset_id"]})' for p in presets.values()))


def visual_preset_fingerprint(preset: Dict[str, Any]) -> str:
    canonical = json.dumps(preset, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def visual_preset_options() -> List[str]:
    """任务表「视觉预设（可选）」下拉选项（显示名，确定性排序）。"""
    return sorted(str(p["name"]) for p in visual_presets().values())


def entry_default_preset_id(routing_policy: str = "") -> str:
    """生产入口默认：旅行线=旅行场景；商品供给线=纯色解析；其余=清爽室内。"""
    if routing_policy == "native_photo_product_supply_v1":
        return "VP_SOLID_COLOR_V1"
    if routing_policy == "native_photo_style_plan_v1":
        return "VP_TRAVEL_SCENE_V1"
    return "VP_CLEAN_INDOOR_V1"


def preset_snapshot(preset: Dict[str, Any], *, source: str) -> Dict[str, Any]:
    """冻结进 theme_brief 的**有效配置**快照（2026-09-15 七样审查后扩充）。

    除身份/版本/指纹外，携带规范化后的背景（mode/kind/固定场景描述）、摄影
    基准与排版引用——下游（供给/生图/QA/渲染）直接消费冻结值，不再用显示名
    解析最新配置文件；旧快照缺这些键时按旧行为执行（窄兼容）。
    """
    return {
        "preset_id": preset["preset_id"],
        "name": preset["name"],
        "version": preset["version"],
        "background_mode": preset["background"].get("mode"),
        "source": source,
        "fingerprint": visual_preset_fingerprint(preset),
        "background": {
            key: preset["background"][key]
            for key in ("mode", "kind", "fixed_scene_zh", "stability_zh")
            if preset["background"].get(key)
        },
        "photography_baseline_zh": preset["photography_baseline_zh"],
        "layout": dict(preset["layout"]),
    }


def resolve_preset_snapshot(*, task_value: Any, account_default: Any,
                            entry_preset_id: str,
                            entry_default_allowed: bool = False) -> Optional[Dict[str, Any]]:
    """按项解析视觉预设并产出快照；三层都缺省时返回 None（旧行为）。

    ``entry_default_allowed``：入口默认只在账号绑定行生效——无绑定的旧行
    保持无视觉预设键，冻结请求逐字不变。
    """
    if str(task_value or "").strip():
        return preset_snapshot(resolve_visual_preset(task_value), source="task")
    if str(account_default or "").strip():
        return preset_snapshot(resolve_visual_preset(account_default),
                               source="account_default")
    if entry_default_allowed and str(entry_preset_id or "").strip():
        return preset_snapshot(resolve_visual_preset(entry_preset_id),
                               source="entry_default")
    return None


#: structured_v1 家族按背景类型选择呈现：纯色/室内＝直接排字，旅行＝留白+柔和底。
STRUCTURED_LAYOUT_BY_BACKGROUND_KIND = {
    "solid": "PHOTO_STRUCTURED_CLEAN_V1",
    "indoor": "PHOTO_STRUCTURED_CLEAN_V1",
    "destination": "PHOTO_STRUCTURED_SCENE_V1",
}


def structured_layout_for(background_kind: Any) -> str:
    layout_id = STRUCTURED_LAYOUT_BY_BACKGROUND_KIND.get(str(background_kind or ""))
    if not layout_id:
        raise VisualPresetError(
            "structured_v1 排版尚未支持该背景类型：" + str(background_kind))
    return layout_id
