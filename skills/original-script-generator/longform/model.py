from __future__ import annotations

import json
from typing import Any, Dict, Mapping

from core.llm_client import OriginalScriptLLMClient

from .contracts import (
    LONGFORM_SCHEMA_VERSION,
    recommended_capture_unit_range,
    segment_count_for_duration,
    validate_master_contract,
)


def build_master_script_prompt(source: Mapping[str, Any]) -> str:
    duration = int(source.get("target_duration_seconds") or 0)
    segment_count = segment_count_for_duration(duration)
    unit_min, unit_max = recommended_capture_unit_range(duration)
    segment_ids = [chr(ord("A") + index) for index in range(segment_count)]
    scene_mode = str(source.get("scene_mode") or "single").lower()
    scene_instruction = {
        "single": "全片保持一个场景，scene_blocks只输出一个。",
        "multi": "优先输出两个有关联但可明确区分的生活场景；第二场景必须是新的生活地点或新的真实使用时刻，不能只是同一房间内移动一步、换机位或改景别。场景切换只能发生一次且只能在片段边界，禁止切走后再切回。若第二场景不自然，可退回一个。",
        "auto": "根据卖点与动作容量判断使用一个或两个场景；只有第二场景带来新的使用语义或可见信息时才增加。",
    }.get(scene_mode, "全片保持一个场景。")
    return f"""你是长视频原创脚本作者。为同一条 {duration} 秒竖屏达人分享视频设计一个完整主脚本，后续会用{segment_count}个最长15秒的视频片段生成并直接切镜合并。

关键原则：
1. 这是一个作品，不是{segment_count}条短视频。全片只允许一个开场钩子；后续片段继续论证，禁止重新介绍商品。
2. 保持 product_identity_lock、人物和穿搭不变。{scene_instruction}
2.1 longform_argument_bundle 是完整内容容量：primary_argument 定主线，supporting_arguments 负责把中段发展成原因、使用价值或风格回报。按其中 content_capacity 使用2-4个不同价值，不逐条朗读、不虚构，也不能只剩主卖点反复改写。
3. 建议输出{unit_min}至{unit_max}个按序 capture_units。每个单元必须有新的可见信息，使用 HOOK / PROOF / USE_PROCESS / CONTEXT / ENDING 等输入结构允许的Beat；避免一镜到底，也不要为凑镜头增加无意义动作。
4. 每个拍摄单元都应有可自然停住的结束状态；如果该单元成为分段边界，下一段必须能够从同一人物、商品状态、姿势、机位、场景、穿搭和光线继续。可以在 observable_end_state 或 end_state 中表达，不要求为此增加摆拍。
5. 同场景的后续片段延续上一段实际尾帧；跨场景片段从独立场景进入帧开始。两种情况都不得重新穿戴、重新系结或重复钩子。
6. 真实感优先于广告精修：普通手机机位、自然直切、动作克制但持续有信息增量。商品结构、数量和锚点严格服从 product_identity_lock。
7. 口播由中央口播引擎统一生成，本步骤不写逐句口播，不做句镜绑定。

只返回JSON。保留输入的所有顶层权威字段，并新增 scene_blocks 与 capture_units。片段ID固定为{segment_ids}。scene_blocks最多2个，每个包含 scene_id、location、moment、lighting、background、narrative_role、segment_ids；一个片段只能属于一个场景。每个场景的segment_ids必须连续，全片最多出现一次场景切换，例如A在场景1、B/C在场景2，或A/B在场景1、C在场景2；禁止A/C同场景而B为另一场景。每个capture_unit至少包含 unit_id、segment_id、scene_id、beat、visual_content、information_gain、camera、character_action、product_anchors_visible；可自然停住时补充 observable_end_state 或 end_state。

冻结输入：
{json.dumps(dict(source), ensure_ascii=False, indent=2)}"""


def generate_master_contract(source: Mapping[str, Any], *, model: str = "gpt-5.6-sol",
                             reasoning_effort: str = "high") -> Dict[str, Any]:
    """One optional model call; frozen authority is restored after generation."""

    client = OriginalScriptLLMClient(
        route="primary", primary_model=model,
        primary_reasoning_effort=reasoning_effort, timeout=300, max_retries=0,
    )
    raw = client.call_json(
        build_master_script_prompt(source), max_tokens=9000, max_attempts=1,
        repair_json_on_failure=False,
    )
    merged = dict(raw)
    for key in (
        "product_code", "target_country", "target_language", "target_duration_seconds",
        "product_identity_lock", "product_truth", "production_world", "semantic_spine",
        "verified_facts", "relationship_language", "approved_style_references",
        "native_rhetoric_contract", "requested_hook_id",
        "approved_supporting_arguments",
        "longform_argument_bundle", "scene_mode",
    ):
        if key in source:
            merged[key] = source[key]
    merged["schema_version"] = LONGFORM_SCHEMA_VERSION
    return validate_master_contract(merged)
