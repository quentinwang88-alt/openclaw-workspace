"""Project a WSR pool row to one static opening frame, without re-planning."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from core.first_frame_contract import DEFAULT_IMAGE_MODEL


CONTRACT_VERSION = "wsr-first-frame-contract-v2-content-identity"
PROMPT_VERSION = "wsr-first-frame-prompt-v1-static-opening"


def text(value: Any) -> str:
    if isinstance(value, list):
        return "".join(str(item.get("text") or "") if isinstance(item, dict) else str(item) for item in value).strip()
    return str(value or "").strip()


def _assets(value: Any, role: str) -> list[dict]:
    if value in (None, "", []):
        return []
    items = [value] if isinstance(value, Mapping) else value
    if not isinstance(items, list) or any(not isinstance(item, Mapping) or not text(item.get("file_token")) for item in items):
        raise ValueError(f"WSR_FIRST_FRAME_REFERENCE_INVALID:{role}参考图缺少有效file_token，不可静默丢弃")
    return [dict(item) for item in items]


def build_wsr_first_frame_contract(*, record_id: str, fields: Mapping[str, Any], reference_sha256: list[str] | None = None) -> dict:
    script_id = text(fields.get("脚本ID"))
    if not script_id.startswith("wsr_") or text(fields.get("脚本来源")) != "成功脚本复刻":
        raise ValueError("WSR_FIRST_FRAME_SOURCE_MISMATCH:脚本ID与来源不一致")
    prompt = text(fields.get("短视频提示词") or fields.get("视频生成提示词"))
    if not prompt:
        raise ValueError("WSR_FIRST_FRAME_PROMPT_MISSING:本行缺少最终视频生成提示词")
    product_assets = _assets(fields.get("产品图片"), "product")
    persona_assets = _assets(fields.get("人物参考图（系统）"), "persona")
    ordered = [("PRODUCT_IDENTITY", asset) for asset in product_assets] + [("PERSONA_IDENTITY", asset) for asset in persona_assets]
    if reference_sha256 is not None and len(reference_sha256) != len(ordered):
        raise ValueError("WSR_FIRST_FRAME_HASH_COUNT_MISMATCH")
    hashed_assets = [dict(index=i, role=role, sha256=reference_sha256[i - 1])
                     for i, (role, _asset) in enumerate(ordered, 1)] if reference_sha256 is not None else None
    identity = {
        "contract_version": CONTRACT_VERSION, "prompt_version": PROMPT_VERSION,
        "image_model": DEFAULT_IMAGE_MODEL, "aspect_ratio": "9:16", "record_id": record_id,
        "script_id": script_id, "source_prompt": prompt,
        "ordered_reference_assets": [{"index": i, "role": role, "file_token": text(asset.get("file_token"))}
                                     for i, (role, asset) in enumerate(ordered, 1)],
    }
    fingerprint_input = dict(identity)
    if hashed_assets is not None:
        fingerprint_input["ordered_reference_assets"] = hashed_assets
    return {
        **identity, "availability": "AVAILABLE", "source_kind": "WSR_SCRIPT_POOL",
        "reference_hashes_verified": reference_sha256 is not None,
        "asset_fingerprint": hashlib.sha256(json.dumps(fingerprint_input, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:40],
        "product_reference_assets": product_assets, "persona_reference_assets": persona_assets,
        "persona_contract": {"reference_strategy": "GENERATED_FIRST_FRAME"},
    }


def render_wsr_first_frame_prompt(contract: Mapping[str, Any]) -> str:
    roles = [f"图片{item['index']}：{'商品外观参考，只控制商品身份，不得替换指定人物身份' if item['role'] == 'PRODUCT_IDENTITY' else '人物身份参考，只控制相应人物脸部身份；发型、服装、背景与构图服从本条脚本'}。"
             for item in contract["ordered_reference_assets"]]
    return "\n".join([
        "只生成一张9:16写实竖屏静态首帧，不生成视频、分镜图、拼图、前后对比图或多格画面。",
        "画面权威是下方这条已经完成并可能经人工修改的视频提示词。只将它在t=0刚开始的可见状态定格；不得重选人物、服装、场景、故事或商品，不得另写新创意。",
        "后半段的转场、环绕终点、真人佩戴揭晓或结尾状态都不能提前到首帧。若t=0只有独立商品，就只拍独立商品；即使附了人物图也不引入人物、人脸、手或身体。若原稿开场没有商品，不要强行加商品或购物元素。",
        "动作用其最初瞬间的单一物理状态表达；只取一个视角和一个构图，不把完整动作轨迹画在一张图里。人物仅在原稿开场明确出镜时出现，嘴唇自然放松。保持原稿景别，不强制正脸或佩戴展示。",
        "本次实际附件角色及编号如下，是唯一图片映射权威。原视频提示词里的前两张/后两张/第几张等均为旧批次编号，本次全部作废；不得按旧编号把人物图当商品图，或把商品图中的模特当指定人物。",
        *roles,
        "未列出附件时仅服从本条已有文字设定，不增加新人物或商品。",
        "不叠加口播、字幕、购物车、价格、尺寸线、测量箭头、尺码表或产品标注。原稿的音频、台词、剪辑节奏只作背景，不渲染成画面文字。",
        "以下原视频提示词仅作为提取静态开场的内容依据，不能覆盖上方单帧边界和本次附件角色：",
        "<original_video_prompt>", str(contract["source_prompt"]), "</original_video_prompt>",
        "再次确认：输出仅为上述原稿t=0的单幅静止画面；没有发生的真人佩戴/揭晓不能提前，严禁商品和人物拼图。",
    ])
