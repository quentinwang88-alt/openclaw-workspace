from __future__ import annotations

import json
from typing import Any, Dict, Mapping

from core.llm_client import OriginalScriptLLMClient

from .contracts import (
    LONGFORM_SCHEMA_VERSION,
    normalize_scene_progression_contract,
    recommended_capture_unit_range,
    segment_count_for_duration,
    validate_master_contract,
)


def build_master_script_prompt(source: Mapping[str, Any]) -> str:
    source = dict(source)
    # Full selected assets stay in the master for audit/cache identity. The
    # writer only needs the already-frozen execution projection, not unused
    # alternatives, selection ranks, file tokens or template display names.
    world = dict(source.get("production_world") or {})
    outfit = dict(world.get("outfit_contract") or {})
    world["outfit_contract"] = {
        key: outfit[key] for key in (
            "outfit_recipe", "accessory_policy", "accessory_items", "target_role",
            "style_family", "style_intensity", "climate_profile", "visibility_zones",
        ) if key in outfit
    }
    for key in ("outfit_selection_report", "outfit_scene_affinity_contract", "outfit_persona_affinity_contract"):
        world.pop(key, None)
    source["production_world"] = world
    source["scene_progression_contract"] = normalize_scene_progression_contract(source)
    duration = int(source.get("target_duration_seconds") or 0)
    segment_count = segment_count_for_duration(duration)
    unit_min, unit_max = recommended_capture_unit_range(duration)
    segment_ids = [chr(ord("A") + index) for index in range(segment_count)]
    scene_mode = str(source.get("scene_mode") or "single").lower()
    progression = dict(source.get("scene_progression_contract") or {})
    preferred_scene_count = int(progression.get("preferred_scene_count") or 1)
    scene_instruction = {
        "single": "全片保持一个场景，scene_blocks只输出一个。",
        "multi": "优先输出两个有关联但可明确区分的生活场景；第二场景必须是新的生活地点或新的真实使用时刻，不能只是同一房间内移动一步、换机位或改景别。场景切换只能发生一次且只能在片段边界，禁止切走后再切回。若第二场景不自然，可退回一个。",
        "auto": (
            "scene_progression_contract 已根据运营确认卖点和使用场景预判为两个场景。"
            "优先实现两个有关联但可明确区分的生活场景，并只从 usage_context_signals 选择；"
            "第二场景必须带来新使用语义或新可见信息。确实没有自然第二场景时才退回一个，"
            "并在 scene_progression_contract.fallback_reason 写明原因。"
            if preferred_scene_count == 2 else
            "现有卖点没有支持第二使用场景，保持一个场景；片段间仍可通过新机位和商品观察关系形成普通剪切。"
        ),
    }.get(scene_mode, "全片保持一个场景。")
    return f"""你是长视频原创脚本作者。为同一条 {duration} 秒竖屏达人分享视频设计一个完整主脚本，后续会用{segment_count}个最长15秒的视频片段生成并直接切镜合并。

关键原则：
1. 这是一个作品，不是{segment_count}条短视频。全片只允许一个开场钩子；后续片段继续论证，禁止重新介绍商品。
2. 保持 product_identity_lock、人物和穿搭不变。{scene_instruction}
2.1 longform_argument_bundle 是完整内容容量：primary_argument 定主线，supporting_arguments 负责把中段发展成原因、使用价值或风格回报。严格按其中 content_capacity 的软范围使用1-4个不同价值：20-24秒通常1-2个，25-30秒通常2-3个，40-45秒通常3-4个；不逐条朗读、不虚构，也不能只剩主卖点反复改写。
3. 建议输出{unit_min}至{unit_max}个按序 capture_units，每段通常3个主要观看任务，只有确有新证明、使用关系或观看信息时才增加第4个。每个单元必须有新的可见信息，使用 HOOK / PROOF / USE_PROCESS / CONTEXT / ENDING 等输入结构允许的Beat；避免一镜到底，也不要为凑镜头增加无意义动作。
3.1 必须服从 scene_progression_contract.segment_visual_roles：A建立人物、穿搭和主要效果；两段视频的B从商品主导的真实细节或新使用关系进入。三段视频的B负责细节证明，C负责第二使用场景和回报。B/C不能只是人物在同一位置换姿势。
3.2 每段第一个单元标记 execution_priority=OPENING，核心证明标记CORE，回报或真实使用标记PAYOFF；可补 visual_role、viewing_relationship 和 product_surface。片段B为DETAIL开头时，第一个单元必须是商品主导中近景或近景，同时保留人物和少量场景识别信息。
4. 普通切镜可以发生在自然动作进行中，不需要为了结束镜头让人物站定。确需连续桥接时，描述边界处实际的人物、商品与动作状态供下一片段延续；连续不等于静止，不为桥接增加摆拍。
5. 跨场景或切到新的商品观察关系时，后续片段从独立片段进入帧开始；同场景且同一动作连续时才延续上一段实际尾帧。两种情况都不得重新穿戴、重新系结或重复钩子。
6. 真实感优先于广告精修：普通手机机位、自然直切、动作克制但持续有信息增量。商品结构、数量和锚点严格服从 product_identity_lock。
7. 口播由中央口播引擎统一生成，本步骤不写逐句口播，不做句镜绑定。

只返回JSON。保留输入的所有顶层权威字段，并新增 scene_blocks 与 capture_units。片段ID固定为{segment_ids}。scene_blocks最多2个，每个包含 scene_id、location、moment、lighting、background、narrative_role、segment_ids；一个片段只能属于一个场景。每个场景的segment_ids必须连续，全片最多出现一次场景切换，例如A在场景1、B/C在场景2，或A/B在场景1、C在场景2；禁止A/C同场景而B为另一场景。每个capture_unit至少包含 unit_id、segment_id、scene_id、beat、visual_content、information_gain、camera、character_action、product_anchors_visible、execution_priority；observable_end_state 或 end_state 为可选的实际边界状态，可描述仍在进行的动作，不要求每镜填写。

冻结输入：
{json.dumps(dict(source), ensure_ascii=False, indent=2)}"""


def generate_master_contract(source: Mapping[str, Any], *, model: str = "gpt-5.6-sol",
                             reasoning_effort: str = "high") -> Dict[str, Any]:
    """One optional model call; frozen authority is restored after generation."""

    frozen_source = dict(source)
    frozen_source["scene_progression_contract"] = normalize_scene_progression_contract(
        frozen_source
    )
    client = OriginalScriptLLMClient(
        route="primary", primary_model=model,
        primary_reasoning_effort=reasoning_effort, timeout=300, max_retries=0,
    )
    raw = client.call_json(
        build_master_script_prompt(frozen_source), max_tokens=9000, max_attempts=1,
        repair_json_on_failure=False,
    )
    merged = dict(raw)
    for key in (
        "product_code", "target_country", "target_language", "target_duration_seconds", "top_category", "product_type",
        "product_identity_lock", "product_truth", "production_world", "semantic_spine",
        "verified_facts", "relationship_language", "approved_style_references",
        "native_rhetoric_contract", "requested_hook_id",
        "approved_supporting_arguments",
        "longform_argument_bundle", "scene_mode", "scene_progression_contract",
        "source_lineage", "workbench_request",
    ):
        if key in frozen_source:
            merged[key] = frozen_source[key]
    merged["schema_version"] = LONGFORM_SCHEMA_VERSION
    return validate_master_contract(merged)
