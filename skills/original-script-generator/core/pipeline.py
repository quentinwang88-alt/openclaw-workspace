#!/usr/bin/env python3
"""
原创脚本自动生成流水线。
"""

import hashlib
import json
import os
import re
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from core.bitable import (
    FeishuAPIError,
    FeishuBitableClient,
    TaskRecord,
    build_update_payload,
    extract_attachments,
    normalize_cell_value,
)
from core.autopublish_metadata_sync import default_metadata_db_path, sync_record_to_auto_publish_db
from core.business_rules import (
    is_sea_market,
    preferred_sea_scene_order,
    validate_script_direction_separation,
    validate_script_time_nodes,
    validate_strategy_distribution,
)
from core.business_rules import is_home_share_scene
from core.json_parser import JSONParseError, parse_json_text
from core.json_parser import (
    validate_anchor_card_payload,
    validate_expression_plan_payload,
    validate_opening_strategy_payload,
    validate_persona_style_emotion_pack_payload,
    validate_product_type_guard_payload,
    validate_review_payload,
    validate_script_payload,
    validate_strategy_payload,
    validate_variant_payload,
    validate_video_prompt_payload,
)
from core.llm_client import OriginalScriptLLMClient
from core.prompt_contract_builder import build_prompt_contract, build_prompt_contract_payload
from core.prompts import (
    build_anchor_card_prompt,
    build_expression_plan_prompt,
    build_final_strategy_prompt,
    build_final_video_prompt_prompt,
    build_opening_strategy_prompt,
    build_product_type_guard_prompt,
    build_script_review_prompt,
    build_script_prompt,
    build_strategy_prompt,
    build_styling_plan_prompt,
    build_variant_prompt,
)
from core.product_type_resolution import resolve_product_context
from core.script_brief_builder import build_script_brief
from core.script_ids import build_script_id_from_context
from core.script_renderer import (
    build_summary,
    compress_final_video_prompt_payload,
    render_anchor_card,
    render_failed_script,
    render_skipped_video_prompt,
    render_script,
    render_strategy_progress_preview,
    render_variant_script,
    render_video_prompt,
)
from core.script_type_validator import validate_generated_text
from core.storage import PipelineStorage


STATUS_PENDING_ALL = "待执行-全流程"
STATUS_PENDING_RERUN_SCRIPT = "待执行-重跑脚本"
STATUS_PENDING_RERUN_ALL = "待执行-重跑全流程"
STATUS_PENDING_VARIANTS = "待执行-脚本变体"
STATUS_PENDING_RERUN_VARIANTS = "待执行-重跑脚本变体"
STATUS_PENDING_LEGACY = "待开始"

STATUS_RUNNING_VALIDATE = "执行中-输入校验"
STATUS_RUNNING_ANCHOR = "执行中-锚点分析"
STATUS_RUNNING_STRATEGY = "执行中-策略生成"
STATUS_RUNNING_SCRIPT = "执行中-脚本生成"
STATUS_RUNNING_VARIANTS = "执行中-脚本变体生成"
STATUS_RUNNING_LEGACY = "进行中"

STATUS_DONE = "已完成"
STATUS_DONE_VARIANTS = "已完成-脚本变体"
STATUS_DONE_WITH_QC_WARNINGS = "已完成-含质检失败脚本"
STATUS_FAILED_INPUT = "失败-输入不完整"
STATUS_FAILED_MODEL = "失败-模型返回异常"
STATUS_FAILED_JSON = "失败-JSON解析异常"
STATUS_FAILED_WRITE = "失败-回写异常"
STATUS_FAILED_VARIANT_INPUT = "失败-脚本变体输入缺失"
STATUS_FAILED_VARIANT_MODEL = "失败-脚本变体模型异常"
STATUS_FAILED_VARIANT_JSON = "失败-脚本变体解析异常"
STATUS_FAILED_VARIANT_WRITE = "失败-脚本变体回写异常"
STATUS_FAILED_INTERRUPTED = "失败-任务被中断"
STATUS_FAILED_LEGACY = "任务失败"

PENDING_STATUSES = {
    STATUS_PENDING_ALL,
    STATUS_PENDING_RERUN_SCRIPT,
    STATUS_PENDING_RERUN_ALL,
    STATUS_PENDING_VARIANTS,
    STATUS_PENDING_RERUN_VARIANTS,
    STATUS_PENDING_LEGACY,
}

CLOTHING_PRODUCT_TYPES = {
    "上装",
    "下装",
    "裤子",
    "裙子",
    "半裙",
    "连衣裙",
    "套装",
    "外套",
    "衬衫",
    "T恤",
    "背心",
    "毛衣",
    "卫衣",
    "牛仔裤",
    "短裤",
    "长裤",
}

ACCESSORY_PRODUCT_TYPES = {
    "耳环",
    "耳饰",
    "项链",
    "戒指",
    "手圈",
    "手环",
    "手链",
    "手镯",
    "手串",
    "饰品",
    "首饰",
    "配饰",
    "配件",
    "包包",
    "手提包",
    "单肩包",
    "斜挎包",
    "腋下包",
    "托特包",
    "水桶包",
    "双肩包",
    "钱包",
    "卡包",
    "帽子",
    "棒球帽",
    "渔夫帽",
    "草帽",
    "围巾",
    "丝巾",
    "披肩",
    "腰带",
    "皮带",
    "墨镜",
    "太阳镜",
    "眼镜",
    "平光镜",
    "发饰",
    "发夹",
    "发箍",
    "发圈",
    "头绳",
    "头箍",
    "胸针",
    "手表",
    "脚链",
    "袜子",
}

ACCESSORY_PRODUCT_TYPE_KEYWORDS = {
    "手圈",
    "手环",
    "手链",
    "手镯",
    "手串",
    "饰品",
    "首饰",
    "配饰",
    "bracelet",
    "bangle",
    "cuff",
    "ring",
    "necklace",
    "pendant",
    "anklet",
    "brooch",
}

HAIR_CLIP_INCLUDE_KEYWORDS = {
    "发夹",
    "抓夹",
    "边夹",
    "刘海夹",
    "香蕉夹",
    "竖夹",
    "半扎夹",
    "盘发夹",
    "功能型发夹",
    "装饰发夹",
    "鲨鱼夹",
}

HAIR_ACCESSORY_INCLUDE_KEYWORDS = {
    "发饰",
    "发夹",
    "抓夹",
    "边夹",
    "刘海夹",
    "鲨鱼夹",
    "香蕉夹",
    "发箍",
    "发圈",
    "发带",
    "头绳",
    "头箍",
}

HAIR_CLIP_EXCLUDE_KEYWORDS = {
    "发箍",
    "发圈",
    "发带",
    "头巾",
    "头绳",
}

FUNCTIONAL_CLIP_KEYWORDS = {
    "抓夹",
    "鲨鱼夹",
    "香蕉夹",
    "竖夹",
    "半扎夹",
    "盘发夹",
    "固定",
    "整理",
    "恢复",
    "成型",
    "摘盔",
    "头盔",
    "通勤",
    "盘发",
    "半扎",
    "后脑",
}

DECORATIVE_CLIP_KEYWORDS = {
    "边夹",
    "刘海夹",
    "装饰夹",
    "装饰发夹",
    "侧边",
    "刘海",
    "碎发",
    "点缀",
    "女生感",
    "精致感",
    "蝴蝶结",
}

ALLOWED_PRODUCT_TYPES = {"服装", "配饰"} | CLOTHING_PRODUCT_TYPES | ACCESSORY_PRODUCT_TYPES
ALLOWED_TOP_CATEGORIES = {"女装", "配饰"}

STAGE_ORDER = {
    "product_type_guard": 0,
    "anchor_card": 1,
    "opening_strategy": 2,
    "persona_style_emotion_pack": 3,
    "strategy_candidates": 4,
    "strategy_cards": 5,
    "expression_s1": 6,
    "expression_s2": 7,
    "expression_s3": 8,
    "expression_s4": 9,
    "script_brief_s1": 10,
    "script_s1": 11,
    "script_review_s1": 12,
    "video_prompt_s1": 13,
    "script_brief_s2": 14,
    "script_s2": 15,
    "script_review_s2": 16,
    "video_prompt_s2": 17,
    "script_brief_s3": 18,
    "script_s3": 19,
    "script_review_s3": 20,
    "video_prompt_s3": 21,
    "script_brief_s4": 22,
    "script_s4": 23,
    "script_review_s4": 24,
    "video_prompt_s4": 25,
    "variant_s1": 31,
    "variant_s2": 32,
    "variant_s3": 33,
    "variant_s4": 34,
}

VARIANT_BATCHES = [["V1"], ["V2"], ["V3"], ["V4"], ["V5"]]

VARIANT_STRENGTH_LIGHT = "light"
VARIANT_STRENGTH_MEDIUM = "medium"
VARIANT_STRENGTH_HEAVY = "heavy"

VARIANT_FOCUS_OPTIONS = {
    "opening",
    "proof",
    "ending",
    "scene",
    "rhythm",
    "persona",
    "action",
    "outfit",
    "emotion",
}

DIRECTION_ALLOWED_POOLS = {
    "S1": {
        "script_role": ["cognitive_reframing"],
        "opening_mode": ["轻顾虑冲突型", "轻判断型"],
        "proof_mode": ["顾虑化解型", "结果证明型"],
        "ending_mode": ["顾虑化解收尾", "轻安利收尾"],
    },
    "S2": {
        "script_role": ["aura_enhancement"],
        "opening_mode": ["轻顾虑冲突型", "轻判断型", "结果先给型"],
        "proof_mode": ["结果证明型", "搭配成立型", "细节证明型"],
        "ending_mode": ["适合谁收尾", "结果感收尾", "场景代入收尾"],
    },
    "S3": {
        "script_role": ["risk_resolution"],
        "opening_mode": ["轻判断型", "结果先给型"],
        "proof_mode": ["顾虑化解型", "结果证明型"],
        "ending_mode": ["适合谁收尾", "顾虑化解收尾", "轻安利收尾"],
    },
    "S4": {
        "script_role": ["result_delivery"],
        "opening_mode": ["高惊艳首镜型"],
        "proof_mode": ["结果证明型", "细节证明型"],
        "ending_mode": ["结果感收尾", "顾虑化解收尾"],
        "extra_boundary": [
            "首镜更具结果感和 hero shot 气质，但仍保留自然分享语境",
            "首镜后必须尽快进入 proof，不得空钩子",
            "不得滑向广告片",
        ],
    },
}

DEFAULT_VARIANT_PROFILES = {
    "S1": [
        {"variant_id": "V1", "variant_no": 1, "variant_strength": "light", "variant_focus": "opening"},
        {"variant_id": "V2", "variant_no": 2, "variant_strength": "light", "variant_focus": "proof"},
        {"variant_id": "V3", "variant_no": 3, "variant_strength": "light", "variant_focus": "ending"},
        {"variant_id": "V4", "variant_no": 4, "variant_strength": "medium", "variant_focus": "scene"},
        {"variant_id": "V5", "variant_no": 5, "variant_strength": "medium", "variant_focus": "persona"},
    ],
    "S2": [
        {"variant_id": "V1", "variant_no": 1, "variant_strength": "light", "variant_focus": "opening"},
        {"variant_id": "V2", "variant_no": 2, "variant_strength": "light", "variant_focus": "proof"},
        {"variant_id": "V3", "variant_no": 3, "variant_strength": "light", "variant_focus": "scene"},
        {"variant_id": "V4", "variant_no": 4, "variant_strength": "medium", "variant_focus": "outfit"},
        {"variant_id": "V5", "variant_no": 5, "variant_strength": "medium", "variant_focus": "emotion"},
    ],
    "S3": [
        {"variant_id": "V1", "variant_no": 1, "variant_strength": "light", "variant_focus": "proof"},
        {"variant_id": "V2", "variant_no": 2, "variant_strength": "light", "variant_focus": "ending"},
        {"variant_id": "V3", "variant_no": 3, "variant_strength": "light", "variant_focus": "action"},
        {"variant_id": "V4", "variant_no": 4, "variant_strength": "medium", "variant_focus": "persona"},
        {"variant_id": "V5", "variant_no": 5, "variant_strength": "medium", "variant_focus": "scene"},
    ],
    "S4": [
        {"variant_id": "V1", "variant_no": 1, "variant_strength": "light", "variant_focus": "opening"},
        {"variant_id": "V2", "variant_no": 2, "variant_strength": "light", "variant_focus": "proof"},
        {"variant_id": "V3", "variant_no": 3, "variant_strength": "light", "variant_focus": "ending"},
        {"variant_id": "V4", "variant_no": 4, "variant_strength": "light", "variant_focus": "scene"},
        {"variant_id": "V5", "variant_no": 5, "variant_strength": "light", "variant_focus": "emotion"},
    ],
}

VARIANT_GROUPS = [
    {
        "script_index": 1,
        "script_json_field": "script_s1_json",
        "variant_json_field": "variant_s1_json",
        "final_field": "final_s1_json",
        "exp_field": "exp_s1_json",
        "fallback_stage_name": "script_s1",
        "render_fields": [
            "script_1_variant_1",
            "script_1_variant_2",
            "script_1_variant_3",
            "script_1_variant_4",
            "script_1_variant_5",
        ],
    },
    {
        "script_index": 2,
        "script_json_field": "script_s2_json",
        "variant_json_field": "variant_s2_json",
        "final_field": "final_s2_json",
        "exp_field": "exp_s2_json",
        "fallback_stage_name": "script_s2",
        "render_fields": [
            "script_2_variant_1",
            "script_2_variant_2",
            "script_2_variant_3",
            "script_2_variant_4",
            "script_2_variant_5",
        ],
    },
    {
        "script_index": 3,
        "script_json_field": "script_s3_json",
        "variant_json_field": "variant_s3_json",
        "final_field": "final_s3_json",
        "exp_field": "exp_s3_json",
        "fallback_stage_name": "script_s3",
        "render_fields": [
            "script_3_variant_1",
            "script_3_variant_2",
            "script_3_variant_3",
            "script_3_variant_4",
            "script_3_variant_5",
        ],
    },
    {
        "script_index": 4,
        "script_json_field": "script_s4_json",
        "variant_json_field": "variant_s4_json",
        "final_field": "final_s4_json",
        "exp_field": "exp_s4_json",
        "fallback_stage_name": "script_s4",
        "render_fields": [
            "script_4_variant_1",
            "script_4_variant_2",
            "script_4_variant_3",
            "script_4_variant_4",
            "script_4_variant_5",
        ],
    },
]

DEFAULT_VARIANT_SCRIPT_INDEXES = {1, 2, 3, 4}

VARIANT_STAGE_LOOKUP = {
    "anchor_card_json": "anchor_card",
    "opening_strategy_json": "opening_strategy",
    "styling_plan_json": "persona_style_emotion_pack",
    "final_s1_json": "strategy_cards",
    "final_s2_json": "strategy_cards",
    "final_s3_json": "strategy_cards",
    "final_s4_json": "strategy_cards",
    "exp_s1_json": "expression_s1",
    "exp_s2_json": "expression_s2",
    "exp_s3_json": "expression_s3",
    "exp_s4_json": "expression_s4",
}


class ValidationError(Exception):
    pass


class ModelStageError(Exception):
    pass


class JsonStageError(Exception):
    pass


class OriginalScriptPipeline:
    def __init__(
        self,
        client: FeishuBitableClient,
        mapping: Dict[str, Optional[str]],
        variant_script_indexes: Optional[List[int]] = None,
        script_rerun_indexes: Optional[List[int]] = None,
        llm_route: str = "auto",
        llm_route_order: Optional[List[str]] = None,
    ):
        self.client = client
        self.mapping = mapping
        self.storage = PipelineStorage()
        self.stats = {"total": 0, "success": 0, "failed": 0}
        self.variant_script_indexes: Optional[set] = set(variant_script_indexes or []) or None
        self.script_rerun_indexes: Optional[set] = set(script_rerun_indexes or []) or None
        self.llm_route = llm_route
        self.llm_route_order = list(llm_route_order or []) or None
        self.auto_publish_metadata_db_path = str(default_metadata_db_path())

    def process_records(
        self,
        records: List[TaskRecord],
        dry_run: bool = False,
        max_workers: int = 1,
    ) -> Dict[str, int]:
        self.stats["total"] = len(records)

        if dry_run:
            print("🔍 Dry run 待处理任务：")
            for index, record in enumerate(records, 1):
                context = self._build_context(record)
                print(
                    f"  {index}. record_id={record.record_id} | "
                    f"国家={context['target_country'] or '未提供'} | "
                    f"语言={context['target_language'] or '未提供'} | "
                    f"产品类型={context['product_type'] or '未提供'} | "
                    f"状态={context['request_status']}"
                )
            return self.stats

        worker_count = max(1, min(max_workers, 3, len(records)))
        print(f"🚦 记录级并发数: {worker_count}")

        if worker_count == 1:
            for index, record in enumerate(records, 1):
                print(f"\n{'=' * 72}")
                print(f"🧩 处理任务 {index}/{len(records)}: {record.record_id}")
                print(f"{'=' * 72}")
                try:
                    ok = self._process_single_record(record)
                    if ok:
                        self.stats["success"] += 1
                    else:
                        self.stats["failed"] += 1
                except Exception as exc:
                    self.stats["failed"] += 1
                    print(f"❌ 当前记录失败: {exc}")
            return self.stats

        future_to_record: Dict[Any, TaskRecord] = {}
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            for index, record in enumerate(records, 1):
                print(f"🧩 已入队任务 {index}/{len(records)}: {record.record_id}")
                future_to_record[executor.submit(self._process_single_record, record)] = record

            for future in as_completed(future_to_record):
                record = future_to_record[future]
                try:
                    ok = future.result()
                    if ok:
                        self.stats["success"] += 1
                        print(f"✅ 并发任务完成: {record.record_id}")
                    else:
                        self.stats["failed"] += 1
                        print(f"❌ 并发任务失败: {record.record_id}")
                except Exception as exc:
                    self.stats["failed"] += 1
                    print(f"❌ 并发任务失败: {record.record_id} | {exc}")

        return self.stats

    def _process_single_record(self, record: TaskRecord) -> bool:
        logs: List[str] = []
        stage_durations: Dict[str, float] = {}
        context = self._build_context(record)
        request_status = context["request_status"]
        is_variant_request = request_status in {STATUS_PENDING_VARIANTS, STATUS_PENDING_RERUN_VARIANTS}
        variant_phase_active = is_variant_request
        record_id = record.record_id
        run_id: Optional[int] = None
        llm_client = OriginalScriptLLMClient(route=self.llm_route, route_order=self.llm_route_order)

        print(f"\n{'=' * 72}")
        print(f"🧩 开始处理: {record_id} | 产品编码={context['product_code'] or '未提供'}")
        print(f"{'=' * 72}")

        try:
            self._write_update(
                record_id,
                logs,
                stage_durations,
                status=self._runtime_status(
                    STATUS_RUNNING_VARIANTS if is_variant_request else STATUS_RUNNING_VALIDATE
                ),
                extra_values={
                    "error_message": "",
                },
            )

            attachments = extract_attachments(record.fields.get(self.mapping["product_images"]))

            normalized_product_type = self._normalize_product_type(
                context["product_type"],
                context.get("top_category", ""),
            )
            context["top_category"] = self._normalize_top_category(
                context.get("top_category", ""),
                normalized_product_type,
            )
            context.update(self._build_hair_clip_context(context))

            if is_variant_request:
                self._validate_variant_inputs(record, context)
                self._validate_variant_output_fields()
            else:
                self._validate_inputs(context, attachments)

            input_hash = self._build_input_hash(attachments, context)
            image_paths: List[str] = []
            if not is_variant_request:
                image_paths = self._download_images(record_id, attachments, logs)
            run_id = self.storage.create_run(
                record_id=record_id,
                product_code=context["product_code"],
                input_hash=input_hash,
                context=context,
                raw_record_fields=record.fields,
            )
            self.storage.update_run_status(
                run_id,
                runtime_status=self._runtime_status(
                    STATUS_RUNNING_VARIANTS if is_variant_request else STATUS_RUNNING_VALIDATE
                ),
                stage_durations=stage_durations,
            )

            context.update(self._normalize_type_guard_payload_for_context(context))
            logs.append(self._summarize_type_guard(context.get("type_guard", {})))

            if is_variant_request:
                self._process_variant_only_record(
                    record=record,
                    context=context,
                    record_id=record_id,
                    run_id=run_id,
                    logs=logs,
                    stage_durations=stage_durations,
                    llm_client=llm_client,
                )
                return True

            full_flow = request_status != STATUS_PENDING_RERUN_SCRIPT
            if not full_flow:
                full_flow = not self._can_rerun_script_only(record, context["product_code"])
                if full_flow:
                    logs.append("重跑脚本所需中间结果缺失，自动回退全流程")

            anchor_card: Dict[str, Any]
            opening_strategy_payload: Dict[str, Any]
            persona_style_emotion_pack: Dict[str, Any]
            strategy_candidates: Dict[str, Any]
            strategy_cards: Dict[str, Any]
            final_s1: Dict[str, Any]
            final_s2: Dict[str, Any]
            final_s3: Dict[str, Any]
            final_s4: Dict[str, Any]
            exp_s1: Dict[str, Any]
            exp_s2: Dict[str, Any]
            exp_s3: Dict[str, Any]
            exp_s4: Dict[str, Any]

            allow_resume_stages = request_status != STATUS_PENDING_RERUN_ALL

            type_guard_payload = (
                self._load_resume_stage_output(
                    record_id=record_id,
                    product_code=context["product_code"],
                    input_hash=input_hash,
                    stage_name="product_type_guard",
                    validator=validate_product_type_guard_payload,
                )
                if allow_resume_stages
                else None
            )
            if type_guard_payload:
                logs.append("复用同输入哈希的产品类型视觉守卫")
            else:
                try:
                    type_guard_payload = self._run_stage(
                        "product_type_guard",
                        build_product_type_guard_prompt(
                            context["product_type"],
                            context.get("top_category", ""),
                        ),
                        run_id=run_id,
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_context={
                            **context,
                            "table_product_type": context.get("product_type", ""),
                            "business_category": context.get("top_category", ""),
                        },
                        stage_durations=stage_durations,
                        image_paths=image_paths,
                        llm_client=llm_client,
                        validator=validate_product_type_guard_payload,
                        max_tokens=1400,
                    )
                except (JsonStageError, ModelStageError) as exc:
                    logs.append(f"产品类型视觉守卫失败，已回退表格优先守卫：{exc}")
                    type_guard_payload = {}
            context.update(self._normalize_type_guard_payload_for_context(context, type_guard_payload))
            logs.append(self._summarize_type_guard(context.get("type_guard", {})))

            if full_flow:
                anchor_card = (
                    self._load_resume_stage_output(
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_hash=input_hash,
                        stage_name="anchor_card",
                        validator=validate_anchor_card_payload,
                    )
                    if allow_resume_stages
                    else None
                )
                if anchor_card:
                    logs.append("复用同输入哈希的产品锚点卡")
                else:
                    self._write_update(
                        record_id,
                        logs,
                        stage_durations,
                        status=self._runtime_status(STATUS_RUNNING_ANCHOR),
                        extra_values={"input_hash": input_hash},
                    )

                    anchor_card = self._run_stage(
                        "anchor_card",
                        build_anchor_card_prompt(
                            context["target_country"],
                            context["target_language"],
                            context["product_type"],
                            context.get("product_selling_note", ""),
                            hair_clip_mode=bool(context.get("hair_clip_mode")),
                            type_guard_json=context.get("type_guard"),
                        ),
                        run_id=run_id,
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_context=context,
                        stage_durations=stage_durations,
                        image_paths=image_paths,
                        llm_client=llm_client,
                        validator=validate_anchor_card_payload,
                    )
                context.update(self._build_hair_clip_context(context, anchor_card))
                if run_id is not None:
                    self.storage.update_run_artifacts(run_id, anchor_card=anchor_card)

                opening_strategy_payload = (
                    self._load_resume_stage_output(
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_hash=input_hash,
                        stage_name="opening_strategy",
                        validator=validate_opening_strategy_payload,
                    )
                    if allow_resume_stages
                    else None
                )
                if opening_strategy_payload:
                    logs.append("复用同输入哈希的首镜吸引策略")
                else:
                    opening_strategy_payload = self._run_stage(
                        "opening_strategy",
                        build_opening_strategy_prompt(
                            context["target_country"],
                            context["target_language"],
                            context["product_type"],
                            anchor_card,
                            product_selling_note=context.get("product_selling_note", ""),
                            type_guard_json=context.get("type_guard"),
                        ),
                        run_id=run_id,
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_context={
                            **context,
                            "anchor_card": anchor_card,
                        },
                        stage_durations=stage_durations,
                        llm_client=llm_client,
                        validator=validate_opening_strategy_payload,
                    )

                persona_style_emotion_pack = (
                    self._load_resume_stage_output(
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_hash=input_hash,
                        stage_name="persona_style_emotion_pack",
                        validator=validate_persona_style_emotion_pack_payload,
                    )
                    if allow_resume_stages
                    else None
                )
                if persona_style_emotion_pack:
                    logs.append("复用同输入哈希的人物/穿搭/情绪强化包")
                else:
                    persona_style_emotion_pack = self._run_stage(
                        "persona_style_emotion_pack",
                        build_styling_plan_prompt(
                            context["target_country"],
                            context["target_language"],
                            context["product_type"],
                            anchor_card,
                            product_selling_note=context.get("product_selling_note", ""),
                            type_guard_json=context.get("type_guard"),
                        ),
                        run_id=run_id,
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_context={
                            **context,
                            "anchor_card": anchor_card,
                        },
                        stage_durations=stage_durations,
                        llm_client=llm_client,
                        validator=validate_persona_style_emotion_pack_payload,
                    )

                strategy_candidates = (
                    self._load_resume_stage_output(
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_hash=input_hash,
                        stage_name="strategy_candidates",
                        validator=validate_strategy_payload,
                    )
                    if allow_resume_stages
                    else None
                )
                if strategy_candidates:
                    logs.append("复用同输入哈希的四方向内容策略匹配")
                else:
                    self._write_update(
                        record_id,
                        logs,
                        stage_durations,
                        status=self._runtime_status(STATUS_RUNNING_STRATEGY),
                        extra_values={
                            "anchor_card_json": self._dump_json(anchor_card),
                            "opening_strategy_json": self._dump_json(opening_strategy_payload),
                            "styling_plan_json": self._dump_json(persona_style_emotion_pack),
                        },
                    )

                    strategy_candidates = self._run_stage(
                        "strategy_candidates",
                        build_strategy_prompt(
                            context["target_country"],
                            context["target_language"],
                            context["product_type"],
                            anchor_card,
                            opening_strategy_payload,
                            persona_style_emotion_pack,
                            product_selling_note=context.get("product_selling_note", ""),
                            hair_accessory_mode=bool(context.get("hair_accessory_mode")),
                            hair_clip_mode=bool(context.get("hair_clip_mode")),
                            clip_expression_mode=str(context.get("clip_expression_mode", "") or ""),
                            type_guard_json=context.get("type_guard"),
                        ),
                        run_id=run_id,
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_context={
                            **context,
                            "anchor_card": anchor_card,
                            "opening_strategy_payload": opening_strategy_payload,
                            "persona_style_emotion_pack": persona_style_emotion_pack,
                        },
                        stage_durations=stage_durations,
                        llm_client=llm_client,
                        validator=validate_strategy_payload,
                        max_tokens=6200,
                    )

                strategy_cards = (
                    self._load_resume_stage_output(
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_hash=input_hash,
                        stage_name="strategy_cards",
                        validator=validate_strategy_payload,
                    )
                    if allow_resume_stages
                    else None
                )
                if strategy_cards:
                    logs.append("复用同输入哈希的四方向语义级定稿")
                else:
                    strategy_cards = self._run_strategy_stage_with_empty_retry(
                        prompt=build_final_strategy_prompt(
                            context["target_country"],
                            context["target_language"],
                            context["product_type"],
                            anchor_card,
                            product_selling_note=context.get("product_selling_note", ""),
                            opening_strategies_json=opening_strategy_payload,
                            styling_plans_json=persona_style_emotion_pack,
                            strategies_json=strategy_candidates,
                            hair_accessory_mode=bool(context.get("hair_accessory_mode")),
                            hair_clip_mode=bool(context.get("hair_clip_mode")),
                            clip_expression_mode=str(context.get("clip_expression_mode", "") or ""),
                            type_guard_json=context.get("type_guard"),
                        ),
                        run_id=run_id,
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_context={
                            **context,
                            "anchor_card": anchor_card,
                            "opening_strategy_payload": opening_strategy_payload,
                            "persona_style_emotion_pack": persona_style_emotion_pack,
                            "strategy_candidates": strategy_candidates,
                        },
                        stage_durations=stage_durations,
                        llm_client=llm_client,
                    )
                    strategy_cards = self._repair_strategy_cards_if_needed(
                        context=context,
                        anchor_card=anchor_card,
                        opening_strategy_payload=opening_strategy_payload,
                        persona_style_emotion_pack=persona_style_emotion_pack,
                        strategy_candidates=strategy_candidates,
                        strategy_cards=strategy_cards,
                        run_id=run_id,
                        record_id=record_id,
                        stage_durations=stage_durations,
                        llm_client=llm_client,
                    )
                if run_id is not None:
                    self.storage.update_run_artifacts(run_id, strategy_cards=strategy_cards)

                final_s1 = self._find_strategy(strategy_cards, "S1")
                final_s2 = self._find_strategy(strategy_cards, "S2")
                final_s3 = self._find_strategy(strategy_cards, "S3")
                final_s4 = self._find_strategy(strategy_cards, "S4")

                self._write_partial_fields(
                    record_id,
                    {
                        "three_strategies_json": self._dump_json(strategy_cards),
                        "opening_strategy_json": self._dump_json(opening_strategy_payload),
                        "styling_plan_json": self._dump_json(persona_style_emotion_pack),
                        "final_s1_json": self._dump_json(final_s1),
                        "final_s2_json": self._dump_json(final_s2),
                        "final_s3_json": self._dump_json(final_s3),
                        "final_s4_json": self._dump_json(final_s4),
                    },
                )
                self._write_strategy_progress_preview(
                    record_id,
                    anchor_card,
                    {
                        1: final_s1,
                        2: final_s2,
                        3: final_s3,
                        4: final_s4,
                    },
                )

                exp_s1 = (
                    self._load_resume_stage_output(
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_hash=input_hash,
                        stage_name="expression_s1",
                    )
                    if allow_resume_stages
                    else None
                )
                if exp_s1:
                    logs.append("复用同输入哈希的表达计划 S1")
                else:
                    exp_s1 = self._run_stage(
                        "expression_s1",
                        build_expression_plan_prompt(
                            context["target_country"],
                            context["target_language"],
                            context["product_type"],
                            anchor_card,
                            final_s1,
                            product_selling_note=context.get("product_selling_note", ""),
                            persona_style_emotion_pack_json=persona_style_emotion_pack,
                            type_guard_json=context.get("type_guard"),
                        ),
                        run_id=run_id,
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_context={
                            **context,
                            "anchor_card": anchor_card,
                            "final_strategy": final_s1,
                        },
                        stage_durations=stage_durations,
                        llm_client=llm_client,
                        validator=validate_expression_plan_payload,
                    )
                self._write_partial_fields(record_id, {"exp_s1_json": self._dump_json(exp_s1)})
                exp_s2 = (
                    self._load_resume_stage_output(
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_hash=input_hash,
                        stage_name="expression_s2",
                    )
                    if allow_resume_stages
                    else None
                )
                if exp_s2:
                    logs.append("复用同输入哈希的表达计划 S2")
                else:
                    exp_s2 = self._run_stage(
                        "expression_s2",
                        build_expression_plan_prompt(
                            context["target_country"],
                            context["target_language"],
                            context["product_type"],
                            anchor_card,
                            final_s2,
                            product_selling_note=context.get("product_selling_note", ""),
                            persona_style_emotion_pack_json=persona_style_emotion_pack,
                            type_guard_json=context.get("type_guard"),
                        ),
                        run_id=run_id,
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_context={
                            **context,
                            "anchor_card": anchor_card,
                            "final_strategy": final_s2,
                        },
                        stage_durations=stage_durations,
                        llm_client=llm_client,
                        validator=validate_expression_plan_payload,
                    )
                self._write_partial_fields(record_id, {"exp_s2_json": self._dump_json(exp_s2)})
                exp_s3 = (
                    self._load_resume_stage_output(
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_hash=input_hash,
                        stage_name="expression_s3",
                    )
                    if allow_resume_stages
                    else None
                )
                if exp_s3:
                    logs.append("复用同输入哈希的表达计划 S3")
                else:
                    exp_s3 = self._run_stage(
                        "expression_s3",
                        build_expression_plan_prompt(
                            context["target_country"],
                            context["target_language"],
                            context["product_type"],
                            anchor_card,
                            final_s3,
                            product_selling_note=context.get("product_selling_note", ""),
                            persona_style_emotion_pack_json=persona_style_emotion_pack,
                            type_guard_json=context.get("type_guard"),
                        ),
                        run_id=run_id,
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_context={
                            **context,
                            "anchor_card": anchor_card,
                            "final_strategy": final_s3,
                        },
                        stage_durations=stage_durations,
                        llm_client=llm_client,
                        validator=validate_expression_plan_payload,
                    )
                self._write_partial_fields(record_id, {"exp_s3_json": self._dump_json(exp_s3)})
                exp_s4 = (
                    self._load_resume_stage_output(
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_hash=input_hash,
                        stage_name="expression_s4",
                    )
                    if allow_resume_stages
                    else None
                )
                if exp_s4:
                    logs.append("复用同输入哈希的表达计划 S4")
                else:
                    exp_s4 = self._run_stage(
                        "expression_s4",
                        build_expression_plan_prompt(
                            context["target_country"],
                            context["target_language"],
                            context["product_type"],
                            anchor_card,
                            final_s4,
                            product_selling_note=context.get("product_selling_note", ""),
                            persona_style_emotion_pack_json=persona_style_emotion_pack,
                            type_guard_json=context.get("type_guard"),
                        ),
                        run_id=run_id,
                        record_id=record_id,
                        product_code=context["product_code"],
                        input_context={
                            **context,
                            "anchor_card": anchor_card,
                            "final_strategy": final_s4,
                        },
                        stage_durations=stage_durations,
                        llm_client=llm_client,
                        validator=validate_expression_plan_payload,
                    )
                self._write_partial_fields(record_id, {"exp_s4_json": self._dump_json(exp_s4)})
                if run_id is not None:
                    self.storage.update_run_artifacts(
                        run_id,
                        expression_plans={
                            "exp_s1_json": exp_s1,
                            "exp_s2_json": exp_s2,
                            "exp_s3_json": exp_s3,
                            "exp_s4_json": exp_s4,
                        },
                    )

                self._write_update(
                    record_id,
                    logs,
                    stage_durations,
                    status=self._runtime_status(STATUS_RUNNING_SCRIPT),
                    extra_values={
                        "three_strategies_json": self._dump_json(strategy_cards),
                        "opening_strategy_json": self._dump_json(opening_strategy_payload),
                        "styling_plan_json": self._dump_json(persona_style_emotion_pack),
                        "final_s1_json": self._dump_json(final_s1),
                        "final_s2_json": self._dump_json(final_s2),
                        "final_s3_json": self._dump_json(final_s3),
                        "final_s4_json": self._dump_json(final_s4),
                        "exp_s1_json": self._dump_json(exp_s1),
                        "exp_s2_json": self._dump_json(exp_s2),
                        "exp_s3_json": self._dump_json(exp_s3),
                        "exp_s4_json": self._dump_json(exp_s4),
                    },
                )
            else:
                anchor_card = self._load_variant_context_json(
                    record=record,
                    logical_name="anchor_card_json",
                    fallback_stage_name=VARIANT_STAGE_LOOKUP["anchor_card_json"],
                    product_code=context["product_code"],
                )
                context.update(self._build_hair_clip_context(context, anchor_card))
                opening_strategy_payload = self._load_variant_context_json(
                    record=record,
                    logical_name="opening_strategy_json",
                    fallback_stage_name=VARIANT_STAGE_LOOKUP["opening_strategy_json"],
                    product_code=context["product_code"],
                )
                persona_style_emotion_pack = self._load_variant_context_json(
                    record=record,
                    logical_name="styling_plan_json",
                    fallback_stage_name=VARIANT_STAGE_LOOKUP["styling_plan_json"],
                    product_code=context["product_code"],
                )
                final_s1 = self._load_variant_context_json(
                    record=record,
                    logical_name="final_s1_json",
                    fallback_stage_name=VARIANT_STAGE_LOOKUP["final_s1_json"],
                    product_code=context["product_code"],
                )
                final_s2 = self._load_variant_context_json(
                    record=record,
                    logical_name="final_s2_json",
                    fallback_stage_name=VARIANT_STAGE_LOOKUP["final_s2_json"],
                    product_code=context["product_code"],
                )
                final_s3 = self._load_variant_context_json(
                    record=record,
                    logical_name="final_s3_json",
                    fallback_stage_name=VARIANT_STAGE_LOOKUP["final_s3_json"],
                    product_code=context["product_code"],
                )
                final_s4 = self._load_variant_context_json(
                    record=record,
                    logical_name="final_s4_json",
                    fallback_stage_name=VARIANT_STAGE_LOOKUP["final_s4_json"],
                    product_code=context["product_code"],
                )
                exp_s1 = self._load_variant_context_json(
                    record=record,
                    logical_name="exp_s1_json",
                    fallback_stage_name=VARIANT_STAGE_LOOKUP["exp_s1_json"],
                    product_code=context["product_code"],
                )
                exp_s2 = self._load_variant_context_json(
                    record=record,
                    logical_name="exp_s2_json",
                    fallback_stage_name=VARIANT_STAGE_LOOKUP["exp_s2_json"],
                    product_code=context["product_code"],
                )
                exp_s3 = self._load_variant_context_json(
                    record=record,
                    logical_name="exp_s3_json",
                    fallback_stage_name=VARIANT_STAGE_LOOKUP["exp_s3_json"],
                    product_code=context["product_code"],
                )
                exp_s4 = self._load_variant_context_json(
                    record=record,
                    logical_name="exp_s4_json",
                    fallback_stage_name=VARIANT_STAGE_LOOKUP["exp_s4_json"],
                    product_code=context["product_code"],
                )
                self._write_update(
                    record_id,
                    logs,
                    stage_durations,
                    status=self._runtime_status(STATUS_RUNNING_SCRIPT),
                    extra_values={
                        "input_hash": input_hash,
                        "opening_strategy_json": self._dump_json(opening_strategy_payload),
                        "styling_plan_json": self._dump_json(persona_style_emotion_pack),
                    },
                )

            script_indexes_to_generate = set(DEFAULT_VARIANT_SCRIPT_INDEXES) if full_flow else self._selected_script_indexes_for_rerun()
            strategies_by_index = {1: final_s1, 2: final_s2, 3: final_s3, 4: final_s4}
            expressions_by_index = {1: exp_s1, 2: exp_s2, 3: exp_s3, 4: exp_s4}
            script_bundles_by_index: Dict[int, Dict[str, Any]] = {}

            for script_index in DEFAULT_VARIANT_SCRIPT_INDEXES:
                if script_index not in script_indexes_to_generate:
                    script_bundles_by_index[script_index] = self._load_existing_script_bundle(
                        record=record,
                        context=context,
                        script_index=script_index,
                    )
                    continue

                existing_scripts = {
                    f"S{prev_index}": script_bundles_by_index[prev_index]["script_json"]
                    for prev_index in range(1, script_index)
                    if prev_index in script_bundles_by_index
                }
                script_bundle = self._generate_passed_script_bundle(
                    script_index=script_index,
                    context=context,
                    anchor_card=anchor_card,
                    opening_strategy_payload=opening_strategy_payload,
                    persona_style_emotion_pack=persona_style_emotion_pack,
                    final_strategy=strategies_by_index[script_index],
                    expression_plan=expressions_by_index[script_index],
                    normalized_product_type=normalized_product_type,
                    run_id=run_id,
                    record_id=record_id,
                    stage_durations=stage_durations,
                    image_paths=image_paths,
                    llm_client=llm_client,
                    existing_scripts=existing_scripts,
                )
                script_bundles_by_index[script_index] = script_bundle
                self._write_partial_fields(
                    record_id,
                    self._build_script_bundle_output_values(
                        script_index,
                        script_bundle,
                        anchor_card,
                        final_s1,
                        final_s2,
                        final_s3,
                        final_s4,
                    ),
                )

            all_script_bundles = [script_bundles_by_index[index] for index in DEFAULT_VARIANT_SCRIPT_INDEXES]
            main_output_values = {
                "output_summary": build_summary(anchor_card, final_s1, final_s2, final_s3, final_s4),
                "last_run_at": self._now_string(),
                "error_message": "",
                "input_hash": input_hash,
            }
            for index, bundle in script_bundles_by_index.items():
                main_output_values[f"script_s{index}_json"] = self._dump_json(bundle["script_json"])
                main_output_values[f"review_s{index}_json"] = self._dump_json(bundle["review_json"])
                main_output_values[f"script_s{index}"] = bundle["rendered_script"]
                main_output_values[f"video_prompt_s{index}_json"] = self._dump_json(bundle["video_prompt_json"])
                main_output_values[f"video_prompt_s{index}"] = bundle["rendered_video_prompt"]
            if run_id is not None:
                self.storage.update_run_artifacts(
                    run_id,
                    content_ids=self._collect_script_bundle_content_ids(all_script_bundles),
                )
            qc_failed_indexes = [
                index
                for index, bundle in enumerate(all_script_bundles, start=1)
                if not bool(bundle.get("passed_review", True))
            ]
            qc_pass_indexes = [
                index
                for index, bundle in enumerate(all_script_bundles, start=1)
                if bool(bundle.get("passed_review", True))
            ]
            if qc_failed_indexes:
                warning_text = "；".join(
                    f"脚本{index}质检失败，已保留最后一版：{bundle.get('failure_reason', '')}"
                    for index, bundle in enumerate(all_script_bundles, start=1)
                    if not bool(bundle.get("passed_review", True))
                )
                main_output_values["error_message"] = warning_text
                if not qc_pass_indexes:
                    logs.append(f"存在质检失败脚本，且无可继续生成变体的通过脚本：{warning_text}")
                    self._write_update(
                        record_id,
                        logs,
                        stage_durations,
                        status=self._runtime_status(STATUS_DONE_WITH_QC_WARNINGS),
                        extra_values=main_output_values,
                    )
                    if run_id is not None:
                        self.storage.update_run_status(
                            run_id,
                            runtime_status=self._runtime_status(STATUS_DONE_WITH_QC_WARNINGS),
                            error_message=warning_text,
                            stage_durations=stage_durations,
                            completed=True,
                        )
                    logs.append("任务完成（含质检失败脚本落库）")
                    self._sync_auto_publish_metadata(record_id, logs)
                    self._flush_log_fields(record_id, logs, stage_durations)
                    print("  ✅ 当前记录完成（含质检失败脚本落库）")
                    return True

                pass_index_label = "、".join(f"脚本{index}" for index in qc_pass_indexes)
                fail_index_label = "、".join(f"脚本{index}" for index in qc_failed_indexes)
                logs.append(
                    f"存在质检失败脚本，跳过 {fail_index_label} 的变体生成，继续生成 {pass_index_label} 的变体：{warning_text}"
                )
            else:
                logs.append("主体脚本生成完成，开始自动生成脚本变体")

            base_variant_indexes = set(script_indexes_to_generate)
            if qc_failed_indexes:
                selected_variant_indexes = base_variant_indexes & set(qc_pass_indexes)
            else:
                selected_variant_indexes = base_variant_indexes
            self._validate_variant_output_fields(selected_script_indexes=selected_variant_indexes)
            self._write_update(
                record_id,
                logs,
                stage_durations,
                status=self._runtime_status(STATUS_RUNNING_VARIANTS),
                extra_values=main_output_values,
            )
            variant_phase_active = True
            self._process_variants_with_context(
                context=context,
                record_id=record_id,
                run_id=run_id,
                logs=logs,
                stage_durations=stage_durations,
                llm_client=llm_client,
                anchor_card=anchor_card,
                persona_style_emotion_pack=persona_style_emotion_pack,
                final_contexts={
                    "final_s1_json": final_s1,
                    "final_s2_json": final_s2,
                    "final_s3_json": final_s3,
                    "final_s4_json": final_s4,
                },
                expression_contexts={
                    "exp_s1_json": exp_s1,
                    "exp_s2_json": exp_s2,
                    "exp_s3_json": exp_s3,
                    "exp_s4_json": exp_s4,
                },
                script_contexts={
                    "script_s1_json": all_script_bundles[0]["script_json"],
                    "script_s2_json": all_script_bundles[1]["script_json"],
                    "script_s3_json": all_script_bundles[2]["script_json"],
                    "script_s4_json": all_script_bundles[3]["script_json"],
                },
                final_extra_values=main_output_values,
                selected_script_indexes=selected_variant_indexes,
                final_runtime_status=(
                    STATUS_DONE_WITH_QC_WARNINGS if qc_failed_indexes else STATUS_DONE_VARIANTS
                ),
                final_error_message=warning_text if qc_failed_indexes else "",
            )
            if qc_failed_indexes:
                logs.append("任务完成（仅为通过质检脚本生成变体）")
            else:
                logs.append("任务完成")
            self._sync_auto_publish_metadata(record_id, logs)
            self._flush_log_fields(record_id, logs, stage_durations)
            print("  ✅ 当前记录完成")
            return True

        except ValidationError as exc:
            logs.append(f"输入校验失败：{exc}")
            failed_status = STATUS_FAILED_VARIANT_INPUT if variant_phase_active else STATUS_FAILED_INPUT
            self._mark_failed(record_id, failed_status, str(exc), logs, stage_durations)
            if run_id is not None:
                self.storage.update_run_status(
                    run_id,
                    runtime_status=self._runtime_status(failed_status),
                    error_message=str(exc),
                    stage_durations=stage_durations,
                    completed=True,
                )
            return False
        except JsonStageError as exc:
            logs.append(f"JSON 解析失败：{exc}")
            failed_status = STATUS_FAILED_VARIANT_JSON if variant_phase_active else STATUS_FAILED_JSON
            self._mark_failed(record_id, failed_status, str(exc), logs, stage_durations)
            if run_id is not None:
                self.storage.update_run_status(
                    run_id,
                    runtime_status=self._runtime_status(failed_status),
                    error_message=str(exc),
                    stage_durations=stage_durations,
                    completed=True,
                )
            return False
        except FeishuAPIError as exc:
            logs.append(f"飞书回写失败：{exc}")
            failed_status = STATUS_FAILED_VARIANT_WRITE if variant_phase_active else STATUS_FAILED_WRITE
            self._mark_failed(record_id, failed_status, str(exc), logs, stage_durations)
            if run_id is not None:
                self.storage.update_run_status(
                    run_id,
                    runtime_status=self._runtime_status(failed_status),
                    error_message=str(exc),
                    stage_durations=stage_durations,
                    completed=True,
                )
            return False
        except ModelStageError as exc:
            logs.append(f"模型阶段失败：{exc}")
            failed_status = STATUS_FAILED_VARIANT_MODEL if variant_phase_active else STATUS_FAILED_MODEL
            self._mark_failed(record_id, failed_status, str(exc), logs, stage_durations)
            if run_id is not None:
                self.storage.update_run_status(
                    run_id,
                    runtime_status=self._runtime_status(failed_status),
                    error_message=str(exc),
                    stage_durations=stage_durations,
                    completed=True,
                )
            return False
        except KeyboardInterrupt:
            interruption_message = "任务被手动中断"
            logs.append(interruption_message)
            self.stats["failed"] += 1
            self._mark_failed(record_id, STATUS_FAILED_INTERRUPTED, interruption_message, logs, stage_durations)
            if run_id is not None:
                self.storage.update_run_status(
                    run_id,
                    runtime_status=self._runtime_status(STATUS_FAILED_INTERRUPTED),
                    error_message=interruption_message,
                    stage_durations=stage_durations,
                    completed=True,
                )
            self._flush_log_fields(record_id, logs, stage_durations)
            print(f"  ⏹️ 当前记录中断: {interruption_message}")
            raise
        except Exception as exc:
            logs.append(f"未知错误：{exc}")
            failed_status = STATUS_FAILED_VARIANT_MODEL if variant_phase_active else STATUS_FAILED_MODEL
            self._mark_failed(record_id, failed_status, str(exc), logs, stage_durations)
            if run_id is not None:
                self.storage.update_run_status(
                    run_id,
                    runtime_status=self._runtime_status(failed_status),
                    error_message=str(exc),
                    stage_durations=stage_durations,
                    completed=True,
                )
            return False

    def _build_context(self, record: TaskRecord) -> Dict[str, str]:
        fields = record.fields
        return {
            "task_no": normalize_cell_value(fields.get(self.mapping.get("task_no"))),
            "product_code": normalize_cell_value(fields.get(self.mapping.get("product_code"))),
            "product_id": normalize_cell_value(fields.get(self.mapping.get("product_id"))),
            "parent_slot_1": normalize_cell_value(fields.get(self.mapping.get("parent_slot_1"))),
            "parent_slot_2": normalize_cell_value(fields.get(self.mapping.get("parent_slot_2"))),
            "parent_slot_3": normalize_cell_value(fields.get(self.mapping.get("parent_slot_3"))),
            "parent_slot_4": normalize_cell_value(fields.get(self.mapping.get("parent_slot_4"))),
            "top_category": normalize_cell_value(fields.get(self.mapping.get("top_category"))),
            "target_country": normalize_cell_value(fields.get(self.mapping.get("target_country"))),
            "target_language": normalize_cell_value(fields.get(self.mapping.get("target_language"))),
            "product_type": normalize_cell_value(fields.get(self.mapping.get("product_type"))),
            "product_selling_note": normalize_cell_value(fields.get(self.mapping.get("product_selling_note"))),
            "request_status": normalize_cell_value(fields.get(self.mapping.get("status"))),
            "llm_route": self.llm_route,
        }

    @staticmethod
    def _hair_clip_text_blob(*parts: Any) -> str:
        chunks: List[str] = []
        for part in parts:
            if isinstance(part, dict):
                chunks.append(json.dumps(part, ensure_ascii=False))
            elif isinstance(part, list):
                chunks.append(" ".join(str(item or "") for item in part))
            else:
                chunks.append(str(part or ""))
        return " ".join(chunks).lower()

    def _should_enable_hair_clip_mode(
        self,
        product_type: str,
        top_category: str = "",
        anchor_card: Optional[Dict[str, Any]] = None,
        product_selling_note: str = "",
    ) -> bool:
        text = self._hair_clip_text_blob(product_type, top_category, product_selling_note, anchor_card or {})
        if any(token in text for token in HAIR_CLIP_EXCLUDE_KEYWORDS):
            return False
        return any(token in text for token in HAIR_CLIP_INCLUDE_KEYWORDS)

    def _should_enable_hair_accessory_mode(
        self,
        product_type: str,
        top_category: str = "",
        anchor_card: Optional[Dict[str, Any]] = None,
        product_selling_note: str = "",
    ) -> bool:
        text = self._hair_clip_text_blob(product_type, top_category, product_selling_note, anchor_card or {})
        return any(token in text for token in HAIR_ACCESSORY_INCLUDE_KEYWORDS)

    def _detect_clip_expression_mode(
        self,
        product_type: str,
        anchor_card: Optional[Dict[str, Any]] = None,
        product_selling_note: str = "",
    ) -> str:
        text = self._hair_clip_text_blob(product_type, product_selling_note, anchor_card or {})
        if any(token in text for token in FUNCTIONAL_CLIP_KEYWORDS):
            return "functional"
        if any(token in text for token in DECORATIVE_CLIP_KEYWORDS):
            return "decorative"
        return "functional" if "夹" in text else ""

    @staticmethod
    def _extract_hair_clip_anchor_values(anchor_card: Dict[str, Any]) -> Dict[str, List[str]]:
        clip = anchor_card.get("hair_clip_anchors")
        if isinstance(clip, dict):
            return {
                "wear_position_anchor": [str(x) for x in (clip.get("wear_position_anchor") or []) if str(x).strip()],
                "action_anchor": [str(x) for x in (clip.get("action_anchor") or []) if str(x).strip()],
                "result_anchor": [str(x) for x in (clip.get("result_anchor") or []) if str(x).strip()],
                "stability_anchor": [str(x) for x in (clip.get("stability_anchor") or []) if str(x).strip()],
                "scene_fit_anchor": [str(x) for x in (clip.get("scene_fit_anchor") or []) if str(x).strip()],
            }
        structure_anchors = [str(x) for x in (anchor_card.get("structure_anchors") or []) if str(x).strip()]
        operation_anchors = [str(x) for x in (anchor_card.get("operation_anchors") or []) if str(x).strip()]
        fixation_result_anchors = [str(x) for x in (anchor_card.get("fixation_result_anchors") or []) if str(x).strip()]
        before_after_result_anchors = [str(x) for x in (anchor_card.get("before_after_result_anchors") or []) if str(x).strip()]
        scene_usage_anchors = [str(x) for x in (anchor_card.get("scene_usage_anchors") or []) if str(x).strip()]
        return {
            "wear_position_anchor": structure_anchors,
            "action_anchor": operation_anchors,
            "result_anchor": before_after_result_anchors,
            "stability_anchor": fixation_result_anchors,
            "scene_fit_anchor": scene_usage_anchors,
        }

    def _build_hair_clip_context(
        self,
        context: Dict[str, Any],
        anchor_card: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        hair_clip_mode = self._should_enable_hair_clip_mode(
            context.get("product_type", ""),
            context.get("top_category", ""),
            anchor_card=anchor_card,
            product_selling_note=context.get("product_selling_note", ""),
        )
        clip_expression_mode = (
            self._detect_clip_expression_mode(
                context.get("product_type", ""),
                anchor_card=anchor_card,
                product_selling_note=context.get("product_selling_note", ""),
            )
            if hair_clip_mode
            else ""
        )
        anchors = self._extract_hair_clip_anchor_values(anchor_card or {})
        return {
            "hair_accessory_mode": self._should_enable_hair_accessory_mode(
                context.get("product_type", ""),
                context.get("top_category", ""),
                anchor_card=anchor_card,
                product_selling_note=context.get("product_selling_note", ""),
            ),
            "hair_clip_mode": hair_clip_mode,
            "clip_expression_mode": clip_expression_mode,
            **anchors,
        }

    @staticmethod
    def _normalize_type_guard_payload_for_context(
        context: Dict[str, Any],
        vision_payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload = vision_payload or {}
        resolved = resolve_product_context(
            raw_product_type=context.get("product_type", ""),
            business_category=context.get("top_category", ""),
            vision_type=str(payload.get("vision_type", "") or "").strip() or None,
            vision_family=str(payload.get("vision_family", "") or "").strip() or None,
            vision_slot=str(payload.get("vision_slot", "") or "").strip() or None,
            vision_confidence=payload.get("vision_confidence"),
        )
        prompt_contract = build_prompt_contract(resolved)
        prompt_payload = build_prompt_contract_payload(resolved)
        type_guard = resolved.to_dict()
        type_guard["prompt_contract"] = prompt_contract
        type_guard["prompt_payload"] = prompt_payload
        type_guard["visible_evidence"] = [
            str(item or "").strip()
            for item in (payload.get("visible_evidence") or [])
            if str(item or "").strip()
        ] if isinstance(payload.get("visible_evidence"), list) else []
        type_guard["risk_note"] = str(payload.get("risk_note", "") or "").strip()
        return {
            "type_guard": type_guard,
            "resolved_product_family": str(type_guard.get("canonical_family", "") or "").strip(),
            "resolved_product_slot": str(type_guard.get("canonical_slot", "") or "").strip(),
            "resolved_product_type": str(type_guard.get("display_type", "") or "").strip(),
            "type_conflict_level": str(type_guard.get("conflict_level", "") or "").strip(),
            "type_conflict_reason": str(type_guard.get("conflict_reason", "") or "").strip(),
            "type_review_required": bool(type_guard.get("review_required")),
        }

    @staticmethod
    def _summarize_type_guard(type_guard: Dict[str, Any]) -> str:
        if not isinstance(type_guard, dict) or not type_guard:
            return "产品类型守卫：未启用"
        table_type = str(type_guard.get("raw_product_type", "") or "").strip() or "未填写"
        display_type = str(type_guard.get("display_type", "") or "").strip() or table_type
        family = str(type_guard.get("canonical_family", "") or "").strip() or "unknown"
        slot = str(type_guard.get("canonical_slot", "") or "").strip() or "unknown"
        vision_type = str(type_guard.get("vision_type", "") or "").strip() or "unknown"
        conflict_level = str(type_guard.get("conflict_level", "") or "").strip() or "none"
        confidence = type_guard.get("vision_confidence")
        confidence_text = ""
        if isinstance(confidence, (int, float)):
            confidence_text = f"{float(confidence):.2f}"
        elif str(confidence or "").strip():
            confidence_text = str(confidence).strip()
        parts = [
            f"产品类型守卫：表格={table_type}",
            f"最终={display_type}",
            f"族类/部位={family}/{slot}",
            f"视觉={vision_type}",
            f"冲突={conflict_level}",
        ]
        if confidence_text:
            parts.append(f"视觉置信度={confidence_text}")
        conflict_reason = str(type_guard.get("conflict_reason", "") or "").strip()
        if conflict_reason:
            parts.append(f"原因={conflict_reason}")
        return " | ".join(parts)

    @staticmethod
    def _evaluate_type_guard_validation(
        context: Dict[str, Any],
        script_json: Dict[str, Any],
    ) -> Optional[Any]:
        type_guard = context.get("type_guard")
        if not isinstance(type_guard, dict) or not type_guard:
            return None
        resolved = resolve_product_context(
            raw_product_type=str(type_guard.get("raw_product_type", "") or context.get("product_type", "")).strip(),
            business_category=str(type_guard.get("business_category", "") or context.get("top_category", "")).strip(),
            vision_type=str(type_guard.get("vision_type", "") or "").strip() or None,
            vision_family=str(type_guard.get("vision_family", "") or "").strip() or None,
            vision_slot=str(type_guard.get("vision_slot", "") or "").strip() or None,
            vision_confidence=type_guard.get("vision_confidence"),
        )
        return validate_generated_text(render_script(script_json), resolved)

    @staticmethod
    def _merge_unique_issue_texts(values: List[str], additions: List[str]) -> List[str]:
        merged = list(values)
        for item in additions:
            text = str(item or "").strip()
            if text and text not in merged:
                merged.append(text)
        return merged

    def _augment_review_with_type_guard_feedback(
        self,
        review_json: Dict[str, Any],
        context: Dict[str, Any],
        script_json: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], List[str]]:
        validation = self._evaluate_type_guard_validation(context, script_json)
        if validation is None:
            return review_json, []

        review_json = dict(review_json)
        if validation.warnings:
            existing_minor = self._review_issue_texts(review_json, "minor_issues")
            review_json["minor_issues"] = self._merge_unique_issue_texts(existing_minor, validation.warnings)
        return review_json, [str(item or "").strip() for item in validation.violations if str(item or "").strip()]

    def _augment_review_with_timing_feedback(
        self,
        review_json: Dict[str, Any],
        final_strategy: Dict[str, Any],
        script_json: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], List[str]]:
        warnings, violations = validate_script_time_nodes(final_strategy, script_json)
        if not warnings and not violations:
            return review_json, []

        review_json = dict(review_json)
        if warnings:
            existing_minor = self._review_issue_texts(review_json, "minor_issues")
            review_json["minor_issues"] = self._merge_unique_issue_texts(existing_minor, warnings)
        return review_json, [str(item or "").strip() for item in violations if str(item or "").strip()]

    def _validate_inputs(self, context: Dict[str, str], attachments: List[Dict[str, Any]]) -> None:
        missing = []
        if not attachments:
            missing.append("产品图片")
        if not context["target_country"]:
            missing.append("目标国家")
        if not context["target_language"]:
            missing.append("目标语言")
        if not context["product_type"]:
            missing.append("产品类型")
        if missing:
            raise ValidationError(f"缺少必填字段: {', '.join(missing)}")

        normalized_product_type = self._normalize_product_type(
            context["product_type"],
            context.get("top_category", ""),
        )
        if context["product_type"] not in ALLOWED_PRODUCT_TYPES and normalized_product_type not in {"服装", "配饰"}:
            raise ValidationError(
                f"产品类型必须为 {', '.join(sorted(ALLOWED_PRODUCT_TYPES))}，当前值为: {context['product_type']}"
            )
        if context.get("top_category") and context["top_category"] not in ALLOWED_TOP_CATEGORIES:
            raise ValidationError(
                f"一级类目必须为 {', '.join(sorted(ALLOWED_TOP_CATEGORIES))}，当前值为: {context['top_category']}"
            )

    def _build_input_hash(self, attachments: List[Dict[str, Any]], context: Dict[str, str]) -> str:
        attachment_tokens = [item.get("file_token", "") for item in attachments]
        source = json.dumps(
            {
                "attachments": attachment_tokens,
                "top_category": context.get("top_category", ""),
                "target_country": context["target_country"],
                "target_language": context["target_language"],
                "product_type": context["product_type"],
                "product_selling_note": context.get("product_selling_note", ""),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha256(source.encode("utf-8")).hexdigest()

    def _download_images(
        self,
        record_id: str,
        attachments: List[Dict[str, Any]],
        logs: List[str],
        max_images: int = 4,
    ) -> List[str]:
        temp_dir = Path(tempfile.gettempdir()) / "original_script_generator" / record_id
        temp_dir.mkdir(parents=True, exist_ok=True)

        image_paths = []
        for index, attachment in enumerate(attachments[:max_images], 1):
            path = self.client.download_attachment(attachment, temp_dir)
            image_paths.append(str(path))
            logs.append(f"下载图片 {index}: {path.name}")
        if not image_paths:
            raise ValidationError("产品图片为空或下载失败")
        return image_paths

    def _run_stage(
        self,
        stage_name: str,
        prompt: str,
        run_id: Optional[int],
        record_id: str,
        product_code: str,
        input_context: Dict[str, Any],
        stage_durations: Dict[str, float],
        image_paths: Optional[List[str]] = None,
        max_tokens: int = 3200,
        llm_client: Optional[OriginalScriptLLMClient] = None,
        validator: Optional[Any] = None,
    ) -> Dict[str, Any]:
        print(f"  ▶️ 阶段开始: {stage_name}")
        start = time.time()
        try:
            active_llm_client = llm_client or OriginalScriptLLMClient(route=self.llm_route, route_order=self.llm_route_order)
            result = active_llm_client.call_json(
                prompt,
                image_paths=image_paths or [],
                max_tokens=max_tokens,
                validator=validator,
            )
            if not isinstance(result, dict):
                raise JsonStageError(f"{stage_name} 返回结果不是 JSON 对象")
            elapsed = time.time() - start
            stage_durations[stage_name] = round(elapsed, 3)
            if run_id is not None:
                self.storage.record_stage_result(
                    run_id=run_id,
                    record_id=record_id,
                    product_code=product_code,
                    stage_name=stage_name,
                    stage_order=self._resolve_stage_order(stage_name),
                    status="success",
                    prompt_text=prompt,
                    input_context=input_context,
                    image_paths=image_paths or [],
                    output_json=result,
                    duration_seconds=round(elapsed, 3),
                )
            print(f"  ✅ 阶段完成: {stage_name} ({elapsed:.1f}s)")
            return result
        except JSONParseError as exc:
            if run_id is not None:
                self.storage.record_stage_result(
                    run_id=run_id,
                    record_id=record_id,
                    product_code=product_code,
                    stage_name=stage_name,
                    stage_order=self._resolve_stage_order(stage_name),
                    status="json_error",
                    prompt_text=prompt,
                    input_context=input_context,
                    image_paths=image_paths or [],
                    duration_seconds=round(time.time() - start, 3),
                    error_message=str(exc),
                )
            raise JsonStageError(f"{stage_name}: {exc}")
        except JsonStageError:
            raise
        except Exception as exc:
            stage_durations[stage_name] = round(time.time() - start, 3)
            if run_id is not None:
                self.storage.record_stage_result(
                    run_id=run_id,
                    record_id=record_id,
                    product_code=product_code,
                    stage_name=stage_name,
                    stage_order=self._resolve_stage_order(stage_name),
                    status="model_error",
                    prompt_text=prompt,
                    input_context=input_context,
                    image_paths=image_paths or [],
                    duration_seconds=round(time.time() - start, 3),
                    error_message=str(exc),
                )
            raise ModelStageError(f"{stage_name}: {exc}")

    def _run_script_stage_with_language_retry(
        self,
        stage_name: str,
        prompt: str,
        run_id: Optional[int],
        record_id: str,
        product_code: str,
        input_context: Dict[str, Any],
        stage_durations: Dict[str, float],
        image_paths: Optional[List[str]],
        llm_client: OriginalScriptLLMClient,
    ) -> Dict[str, Any]:
        try:
            result = self._run_stage(
                stage_name=stage_name,
                prompt=prompt,
                run_id=run_id,
                record_id=record_id,
                product_code=product_code,
                input_context=input_context,
                stage_durations=stage_durations,
                image_paths=image_paths,
                llm_client=llm_client,
                validator=validate_script_payload,
                max_tokens=5200,
            )
            self._ensure_script_spoken_structure(result, stage_name)
            return result
        except JsonStageError as exc:
            repair_instruction = (
                "脚本修复指令：请严格补齐 storyboard 的核心执行字段。"
                "每个 storyboard 镜头必须保留 shot_no、duration、shot_content、shot_purpose、"
                "subtitle_text_target_language、subtitle_text_zh、voiceover_text_target_language、"
                "voiceover_text_zh、spoken_line_task、person_action、style_note、anchor_reference、task_type。"
                "同时补齐 opening_design、full_15s_flow、execution_constraints、negative_constraints。"
                "全片必须覆盖 hook、proof、decision 三类任务。"
                "默认使用 4-6 个镜头推进，单镜头尽量控制在 1-3 秒，避免三个大镜头机械拼接。"
                f"上次失败原因：{exc}"
            )
            result = self._run_stage(
                stage_name=stage_name,
                prompt=f"{prompt}\n\n{repair_instruction}",
                run_id=run_id,
                record_id=record_id,
                product_code=product_code,
                input_context={**input_context, "repair_reason": str(exc)},
                stage_durations=stage_durations,
                image_paths=image_paths,
                llm_client=llm_client,
                validator=validate_script_payload,
                max_tokens=5200,
            )
            self._ensure_script_spoken_structure(result, stage_name)
            return result

    def _run_strategy_stage_with_empty_retry(
        self,
        prompt: str,
        run_id: Optional[int],
        record_id: str,
        product_code: str,
        input_context: Dict[str, Any],
        stage_durations: Dict[str, float],
        llm_client: OriginalScriptLLMClient,
    ) -> Dict[str, Any]:
        try:
            return self._run_stage(
                stage_name="strategy_cards",
                prompt=prompt,
                run_id=run_id,
                record_id=record_id,
                product_code=product_code,
                input_context=input_context,
                stage_durations=stage_durations,
                llm_client=llm_client,
                validator=validate_strategy_payload,
                max_tokens=6200,
            )
        except JsonStageError as exc:
            print(f"  ⚠️ 内容强策略卡 JSON 异常，触发一次自动重试: {exc}")
            retry_instruction = (
                "补充要求：上次返回的策略卡不是可解析的合法 JSON。"
                "请直接重新输出完整 4 套内容强策略卡。"
                "不要在 JSON 前写说明，不要输出“开始生成”“下面是结果”之类前缀。"
                "不要省略结尾括号，不要截断，必须返回完整合法 JSON。"
                "每个字段值尽量短，不要写长句，不要重复账号/国家适配解释。"
                "forbidden_patterns 和 realism_principles 保持为短数组。"
            )
            return self._run_stage(
                stage_name="strategy_cards",
                prompt=f"{prompt}\n\n{retry_instruction}",
                run_id=run_id,
                record_id=record_id,
                product_code=product_code,
                input_context={
                    **input_context,
                    "empty_retry_reason": str(exc),
                },
                stage_durations=stage_durations,
                llm_client=llm_client,
                validator=validate_strategy_payload,
                max_tokens=6200,
            )

    @staticmethod
    def _resolve_stage_order(stage_name: str) -> int:
        if stage_name in STAGE_ORDER:
            return STAGE_ORDER[stage_name]
        for prefix, order in STAGE_ORDER.items():
            if stage_name.startswith(prefix):
                return order
        return 99

    def _find_strategy(self, strategy_cards: Dict[str, Any], strategy_id: str) -> Dict[str, Any]:
        for item in strategy_cards.get("strategies", []) or []:
            item_strategy_id = str(item.get("strategy_id", "") or "").strip()
            item_final_id = str(item.get("final_strategy_id", "") or "").strip()
            if item_strategy_id == strategy_id or item_final_id == f"Final_{strategy_id}":
                return item
        raise JsonStageError(f"未找到 {strategy_id}")

    def _repair_strategy_cards_if_needed(
        self,
        context: Dict[str, Any],
        anchor_card: Dict[str, Any],
        opening_strategy_payload: Dict[str, Any],
        persona_style_emotion_pack: Dict[str, Any],
        strategy_candidates: Dict[str, Any],
        strategy_cards: Dict[str, Any],
        run_id: Optional[int],
        record_id: str,
        stage_durations: Dict[str, float],
        llm_client: OriginalScriptLLMClient,
    ) -> Dict[str, Any]:
        validation_error = validate_strategy_distribution(
            context["target_country"],
            strategy_cards.get("strategies", []) or [],
        )
        if not validation_error:
            validation_error = self._validate_hair_clip_strategy_distribution(
                context=context,
                strategies=strategy_cards.get("strategies", []) or [],
            )
        if not validation_error:
            return strategy_cards

        print(f"  ⚠️ 内容强策略卡校验未通过，触发策略修复重试: {validation_error}")
        repair_instruction = (
            "请重做 4 套内容强策略卡，并严格满足差异化约束。"
            "4 条 script_role 必须完整覆盖 cognitive_reframing、result_delivery、risk_resolution、aura_enhancement。"
            "至少保证 3 种 proof_mode、3 种 ending_mode、3 种 visual_entry_mode、"
            "2 种 persona_state、3 种 action_entry_mode。"
            "同时拉开 dominant_user_question、proof_thesis、decision_thesis 三组语义字段，"
            "不要只是同一句轻改写。"
            "每条都要明确 primary_focus；secondary_focus 可为空，但若为空，proof 仍要更集中服务 primary_focus。"
            "同时至少拉开 2 种 styling_completion_tag、2 种 persona_visual_tone、2 种 emotion_arc_tag，"
            "并确保 styling_key_anchor 不要 4 条完全相同。"
            "S4 必须与 S1 在首镜逻辑上明确拉开。"
            "如果是东南亚市场，请至少保留 2 套家中自然分享场景，并覆盖至少 2 种 scene_subspace。"
            "东南亚市场下的 S4 必须明确落在家中自然分享语境，"
            "opening_strategy、opening_first_shot、risk_note 不要出现 棚拍、商拍、广告片、studio、campaign、hero shot、大片感 这类词。"
            "S4 的惊艳感只能来自真实近景结果、自然动作、商品聚焦，不能写成珠宝广告或商业大片表达。"
            f"本次失败原因：{validation_error}"
        )
        repaired_cards = self._run_strategy_stage_with_empty_retry(
            prompt=build_final_strategy_prompt(
                context["target_country"],
                context["target_language"],
                context["product_type"],
                anchor_card,
                product_selling_note=context.get("product_selling_note", ""),
                opening_strategies_json=opening_strategy_payload,
                styling_plans_json=persona_style_emotion_pack,
                strategies_json=strategy_candidates,
                repair_instruction=repair_instruction,
                hair_accessory_mode=bool(context.get("hair_accessory_mode")),
                hair_clip_mode=bool(context.get("hair_clip_mode")),
                clip_expression_mode=str(context.get("clip_expression_mode", "") or ""),
                type_guard_json=context.get("type_guard"),
            ),
            run_id=run_id,
            record_id=record_id,
            product_code=context["product_code"],
            input_context={
                **context,
                "anchor_card": anchor_card,
                "opening_strategy_payload": opening_strategy_payload,
                "persona_style_emotion_pack": persona_style_emotion_pack,
                "strategy_candidates": strategy_candidates,
                "repair_instruction": repair_instruction,
            },
            stage_durations=stage_durations,
            llm_client=llm_client,
        )

        repaired_validation_error = validate_strategy_distribution(
            context["target_country"],
            repaired_cards.get("strategies", []) or [],
        )
        if not repaired_validation_error:
            repaired_validation_error = self._validate_hair_clip_strategy_distribution(
                context=context,
                strategies=repaired_cards.get("strategies", []) or [],
            )
        if repaired_validation_error:
            raise ModelStageError(f"strategy_cards 校验失败: {repaired_validation_error}")
        return repaired_cards

    def _record_script_brief_stage(
        self,
        stage_name: str,
        script_brief: Dict[str, Any],
        run_id: Optional[int],
        record_id: str,
        product_code: str,
        input_context: Dict[str, Any],
        stage_durations: Dict[str, float],
    ) -> None:
        stage_durations[stage_name] = 0.0
        if run_id is None:
            return
        self.storage.record_stage_result(
            run_id=run_id,
            record_id=record_id,
            product_code=product_code,
            stage_name=stage_name,
            stage_order=self._resolve_stage_order(stage_name),
            status="success",
            prompt_text="script_brief_builder",
            input_context=input_context,
            image_paths=[],
            output_json=script_brief,
            duration_seconds=0.0,
        )

    @staticmethod
    def _review_flag_is_pass(review_json: Dict[str, Any]) -> bool:
        if "pass" in review_json:
            return bool(review_json.get("pass"))
        return str(review_json.get("result", "") or "").strip().upper() == "PASS"

    @staticmethod
    def _review_issue_texts(review_json: Dict[str, Any], field_name: str) -> List[str]:
        values = review_json.get(field_name)
        if not isinstance(values, list):
            return []
        return [str(item or "").strip() for item in values if str(item or "").strip()]

    @staticmethod
    def _extract_repaired_script(review_json: Dict[str, Any], fallback_script: Dict[str, Any]) -> Dict[str, Any]:
        repaired_script = review_json.get("repaired_script")
        if isinstance(repaired_script, dict):
            return repaired_script
        return fallback_script

    def _blocking_script_issue_after_review(
        self,
        context: Dict[str, Any],
        final_strategy: Dict[str, Any],
        script_json: Dict[str, Any],
        existing_scripts: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Optional[str]:
        try:
            validate_script_payload(script_json)
            self._ensure_script_spoken_structure(script_json, str(final_strategy.get("strategy_id", "") or "script"))
        except (JSONParseError, JsonStageError) as exc:
            return str(exc)
        storyboard = script_json.get("storyboard") if isinstance(script_json.get("storyboard"), list) else []
        shots = [shot for shot in storyboard if isinstance(shot, dict)]
        if len(shots) < 4 or len(shots) > 6:
            return "镜头数量未控制在 4-6 镜内"
        first_shot = shots[0] if shots else {}
        first_anchor = str(first_shot.get("anchor_reference", "") or "").strip()
        if not first_anchor:
            return "首镜没有清楚承接商品锚点"

        overlap_issue = validate_script_direction_separation(
            final_strategy=final_strategy,
            script_json=script_json,
            existing_scripts=existing_scripts or {},
        )
        if overlap_issue:
            return overlap_issue

        if context.get("hair_accessory_mode"):
            all_text = " ".join(
                " ".join(
                    [
                        str(shot.get("shot_content", "") or ""),
                        str(shot.get("person_action", "") or ""),
                        str(shot.get("anchor_reference", "") or ""),
                        str(shot.get("voiceover_text_zh", "") or ""),
                        str(shot.get("voiceover_text_target_language", "") or ""),
                    ]
                )
                for shot in shots
            )
            action_tokens = ["夹", "戴", "整理", "固定", "扎", "盘", "绕", "扣", "上头", "顺手"]
            result_tokens = ["变化", "整齐", "利落", "干净", "完整", "固定", "稳", "更顺", "更清爽", "更服帖"]
            if not any(token in all_text for token in action_tokens):
                return "发饰脚本缺少操作过程"
            if not any(token in all_text for token in result_tokens):
                return "发饰脚本缺少变化或固定结果"

        return None

    def _promote_light_review_if_safe(
        self,
        review_json: Dict[str, Any],
        script_json: Dict[str, Any],
        blocking_reason: Optional[str],
    ) -> Tuple[Dict[str, Any], bool, str]:
        if blocking_reason:
            review_json = dict(review_json)
            review_json.setdefault("major_issues", [])
            if blocking_reason not in review_json["major_issues"]:
                review_json["major_issues"] = list(review_json["major_issues"]) + [blocking_reason]
            review_json["pass"] = False
            review_json["repaired_script"] = script_json
            return review_json, False, blocking_reason

        if self._review_flag_is_pass(review_json):
            return review_json, True, ""

        review_json = dict(review_json)
        major_issues = self._review_issue_texts(review_json, "major_issues")
        minor_issues = self._review_issue_texts(review_json, "minor_issues")
        merged_minor = minor_issues + [issue for issue in major_issues if issue not in minor_issues]
        review_json["pass"] = True
        review_json["major_issues"] = []
        review_json["minor_issues"] = merged_minor
        review_json["repaired_script"] = script_json
        return review_json, True, ""

    @staticmethod
    def _should_retry_s4_directional_failure(
        final_strategy: Dict[str, Any],
        failure_reason: str,
    ) -> bool:
        strategy_id = str(final_strategy.get("strategy_id", "") or "").strip().upper()
        if strategy_id != "S4":
            return False
        reason = str(failure_reason or "").strip()
        if not reason:
            return False
        return any(
            token in reason
            for token in (
                "S4 前3镜仍然是连续局部拆解",
                "S4 前段 proof 过早滑向细节拆解",
            )
        )

    def _build_s4_directional_retry_instruction(
        self,
        context: Dict[str, Any],
        script_json: Dict[str, Any],
        failure_reason: str,
    ) -> str:
        storyboard = script_json.get("storyboard") if isinstance(script_json.get("storyboard"), list) else []
        shot_summaries: List[str] = []
        for shot in storyboard[:3]:
            if not isinstance(shot, dict):
                continue
            shot_no = str(shot.get("shot_no", "") or "?")
            shot_content = str(shot.get("shot_content", "") or "").strip()
            task_type = str(shot.get("task_type", "") or "").strip()
            if shot_content:
                shot_summaries.append(f"镜头{shot_no}（{task_type or '未标注'}）：{shot_content}")

        shot_summary_block = "\n".join(shot_summaries) if shot_summaries else "(空)"
        accessory_retry_rule = ""
        if str(context.get("product_type", "") or "").strip() in ACCESSORY_PRODUCT_TYPES:
            accessory_retry_rule = (
                "\n7. 耳饰/发饰类第二镜优先展示佩戴后的侧脸、上脸或整体结果承接，"
                "不要继续用超近景只拆花型、结构或材质。"
            )

        return (
            "这是一次 S4 高惊艳首镜方向的定向修订，不是重写主卖点。\n"
            f"上次失败原因：{failure_reason}\n"
            "当前脚本前3镜摘要：\n"
            f"{shot_summary_block}\n"
            "修订要求：\n"
            "1. 保持当前商品锚点、主卖点、目标国家/语言、结尾收束方向不变。\n"
            "2. 首镜仍可保留当前最强结果感或 before/after 机制，但必须重排前3镜。\n"
            "3. 镜头2必须改成结果承接镜：优先拍佩戴/上身后的侧脸、上脸、半身或整体结果，不能继续做纯局部细节特写。\n"
            "4. 镜头3再进入结构 proof、动作 proof 或顾虑解除 proof，用动作或结果把首镜接实。\n"
            "5. 前3镜里最多只允许 1 个纯局部细节镜头，不允许连续 2-3 个局部拆解镜头。\n"
            "6. 如果需要讲结构、材质、工艺，请放到镜头3或更后面，不要占用镜头2。"
            f"{accessory_retry_rule}\n"
            "8. 输出仍必须是完整合法 JSON，并保持 4-6 镜结构。"
        )

    def _review_script_bundle(
        self,
        review_stage_name: str,
        context: Dict[str, Any],
        anchor_card: Dict[str, Any],
        persona_style_emotion_pack: Dict[str, Any],
        final_strategy: Dict[str, Any],
        expression_plan: Dict[str, Any],
        script_json: Dict[str, Any],
        run_id: Optional[int],
        record_id: str,
        stage_durations: Dict[str, float],
        llm_client: OriginalScriptLLMClient,
        existing_scripts: Optional[Dict[str, Dict[str, Any]]] = None,
        input_context_extra: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, Any], bool, str]:
        review_input_context = {
            **context,
            "anchor_card": anchor_card,
            "final_strategy": final_strategy,
            "expression_plan": expression_plan,
            "persona_style_emotion_pack": persona_style_emotion_pack,
            "script_json": script_json,
            "existing_scripts": existing_scripts or {},
        }
        if input_context_extra:
            review_input_context.update(input_context_extra)

        review_json = self._run_stage(
            review_stage_name,
            build_script_review_prompt(
                context["target_country"],
                context["product_type"],
                anchor_card,
                final_strategy,
                expression_plan,
                persona_style_emotion_pack,
                script_json,
                type_guard_json=context.get("type_guard"),
            ),
            run_id=run_id,
            record_id=record_id,
            product_code=context["product_code"],
            input_context=review_input_context,
            stage_durations=stage_durations,
            llm_client=llm_client,
            validator=validate_review_payload,
        )

        script_json = self._extract_repaired_script(review_json, script_json)
        review_json, type_guard_violations = self._augment_review_with_type_guard_feedback(
            review_json=review_json,
            context=context,
            script_json=script_json,
        )
        review_json, timing_violations = self._augment_review_with_timing_feedback(
            review_json=review_json,
            final_strategy=final_strategy,
            script_json=script_json,
        )
        blocking_reason = self._blocking_script_issue_after_review(
            context=context,
            final_strategy=final_strategy,
            script_json=script_json,
            existing_scripts=existing_scripts,
        )
        local_violations = type_guard_violations + timing_violations
        if not blocking_reason and local_violations:
            blocking_reason = "；".join(local_violations[:3])
        review_json, passed_review, failure_reason = self._promote_light_review_if_safe(
            review_json=review_json,
            script_json=script_json,
            blocking_reason=blocking_reason,
        )
        return review_json, script_json, passed_review, failure_reason

    def _generate_passed_script_bundle(
        self,
        script_index: int,
        context: Dict[str, Any],
        anchor_card: Dict[str, Any],
        opening_strategy_payload: Dict[str, Any],
        persona_style_emotion_pack: Dict[str, Any],
        final_strategy: Dict[str, Any],
        expression_plan: Dict[str, Any],
        normalized_product_type: str,
        run_id: Optional[int],
        record_id: str,
        stage_durations: Dict[str, float],
        image_paths: Optional[List[str]],
        llm_client: OriginalScriptLLMClient,
        existing_scripts: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        brief_stage_name = f"script_brief_s{script_index}"
        stage_name = f"script_s{script_index}"
        review_stage_name = f"script_review_s{script_index}"
        video_stage_name = f"video_prompt_s{script_index}"

        script_brief = build_script_brief(
            product_type=context["product_type"],
            anchor_card=anchor_card,
            opening_strategies=opening_strategy_payload,
            persona_style_emotion_pack=persona_style_emotion_pack,
            final_strategy=final_strategy,
            expression_plan=expression_plan,
            existing_scripts=existing_scripts,
            type_guard_json=context.get("type_guard"),
        )
        self._record_script_brief_stage(
            stage_name=brief_stage_name,
            script_brief=script_brief,
            run_id=run_id,
            record_id=record_id,
            product_code=context["product_code"],
            input_context={
                **context,
                "anchor_card": anchor_card,
                "opening_strategy_payload": opening_strategy_payload,
                "persona_style_emotion_pack": persona_style_emotion_pack,
                "final_strategy": final_strategy,
                "expression_plan": expression_plan,
            },
            stage_durations=stage_durations,
        )

        script_json = self._run_script_stage_with_language_retry(
            stage_name=stage_name,
            prompt=build_script_prompt(
                context["target_country"],
                context["target_language"],
                context["product_type"],
                script_brief,
                product_selling_note=context.get("product_selling_note", ""),
                existing_script_jsons=existing_scripts,
                hair_accessory_mode=bool(context.get("hair_accessory_mode")),
                hair_clip_mode=bool(context.get("hair_clip_mode")),
                clip_expression_mode=str(context.get("clip_expression_mode", "") or ""),
                type_guard_json=context.get("type_guard"),
            ),
            run_id=run_id,
            record_id=record_id,
            product_code=context["product_code"],
            input_context={
                **context,
                "script_brief": script_brief,
            },
            stage_durations=stage_durations,
            image_paths=image_paths,
            llm_client=llm_client,
        )

        review_json, script_json, passed_review, failure_reason = self._review_script_bundle(
            review_stage_name=review_stage_name,
            context=context,
            anchor_card=anchor_card,
            persona_style_emotion_pack=persona_style_emotion_pack,
            final_strategy=final_strategy,
            expression_plan=expression_plan,
            script_json=script_json,
            run_id=run_id,
            record_id=record_id,
            stage_durations=stage_durations,
            llm_client=llm_client,
            existing_scripts=existing_scripts,
        )

        if not passed_review and self._should_retry_s4_directional_failure(final_strategy, failure_reason):
            retry_stage_name = f"{stage_name}_retry1"
            retry_review_stage_name = f"{review_stage_name}_retry1"
            retry_instruction = self._build_s4_directional_retry_instruction(
                context=context,
                script_json=script_json,
                failure_reason=failure_reason,
            )
            retry_input_context = {
                **context,
                "script_brief": script_brief,
                "retry_attempt": 1,
                "retry_reason": failure_reason,
                "retry_type": "s4_directional",
            }
            script_json = self._run_script_stage_with_language_retry(
                stage_name=retry_stage_name,
                prompt=build_script_prompt(
                    context["target_country"],
                    context["target_language"],
                    context["product_type"],
                    script_brief,
                    product_selling_note=context.get("product_selling_note", ""),
                    existing_script_jsons=existing_scripts,
                    current_script_json=script_json,
                    repair_instruction=retry_instruction,
                    hair_accessory_mode=bool(context.get("hair_accessory_mode")),
                    hair_clip_mode=bool(context.get("hair_clip_mode")),
                    clip_expression_mode=str(context.get("clip_expression_mode", "") or ""),
                    type_guard_json=context.get("type_guard"),
                ),
                run_id=run_id,
                record_id=record_id,
                product_code=context["product_code"],
                input_context=retry_input_context,
                stage_durations=stage_durations,
                image_paths=image_paths,
                llm_client=llm_client,
            )
            review_json, script_json, passed_review, failure_reason = self._review_script_bundle(
                review_stage_name=retry_review_stage_name,
                context=context,
                anchor_card=anchor_card,
                persona_style_emotion_pack=persona_style_emotion_pack,
                final_strategy=final_strategy,
                expression_plan=expression_plan,
                script_json=script_json,
                run_id=run_id,
                record_id=record_id,
                stage_durations=stage_durations,
                llm_client=llm_client,
                existing_scripts=existing_scripts,
                input_context_extra={
                    "retry_attempt": 1,
                    "retry_reason": failure_reason,
                    "retry_type": "s4_directional",
                },
            )

        if not passed_review:
            failure_reason = failure_reason or self._summarize_review_failure(review_json)
            self._ensure_script_content_id(
                script_json,
                context=context,
                script_index=script_index,
                record_id=record_id,
            )
            rendered_script = render_failed_script(render_script(script_json), review_json)
            rendered_video_prompt = render_skipped_video_prompt(failure_reason)

            self._record_rendered_artifact(
                run_id=run_id,
                record_id=record_id,
                product_code=context["product_code"],
                stage_name=f"{stage_name}_rendered",
                prompt_text=f"render_script({stage_name})",
                input_context={"script_json": script_json, "review_json": review_json, "failed_qc": True},
                rendered_text=rendered_script,
            )
            self._record_rendered_artifact(
                run_id=run_id,
                record_id=record_id,
                product_code=context["product_code"],
                stage_name=f"{video_stage_name}_rendered",
                prompt_text=f"render_video_prompt_skipped({video_stage_name})",
                input_context={"review_json": review_json, "skipped_reason": failure_reason},
                rendered_text=rendered_video_prompt,
            )
            return {
                "script_json": script_json,
                "review_json": review_json,
                "video_prompt_json": {
                    "status": "SKIPPED_DUE_TO_QC_FAIL",
                    "reason": failure_reason,
                },
                "rendered_script": rendered_script,
                "rendered_video_prompt": rendered_video_prompt,
                "passed_review": False,
                "failure_reason": failure_reason,
            }

        video_prompt_start = time.time()
        try:
            video_prompt_json = self._run_stage(
                video_stage_name,
                build_final_video_prompt_prompt(
                    context["target_country"],
                    context["target_language"],
                    context["product_type"],
                    anchor_card,
                    final_strategy,
                    script_json,
                    type_guard_json=context.get("type_guard"),
                ),
                run_id=run_id,
                record_id=record_id,
                product_code=context["product_code"],
                input_context={
                    **context,
                    "anchor_card": anchor_card,
                    "final_strategy": final_strategy,
                    "script_json": script_json,
                },
                stage_durations=stage_durations,
                llm_client=llm_client,
                validator=validate_video_prompt_payload,
            )
        except JsonStageError as exc:
            video_prompt_json = self._build_video_prompt_fallback_from_script(script_json)
            validate_video_prompt_payload(video_prompt_json)
            elapsed = round(time.time() - video_prompt_start, 3)
            stage_durations[video_stage_name] = elapsed
            if run_id is not None:
                self.storage.record_stage_result(
                    run_id=run_id,
                    record_id=record_id,
                    product_code=context["product_code"],
                    stage_name=video_stage_name,
                    stage_order=self._resolve_stage_order(video_stage_name),
                    status="success",
                    prompt_text=f"fallback_from_script({video_stage_name})",
                    input_context={
                        **context,
                        "anchor_card": anchor_card,
                        "final_strategy": final_strategy,
                        "script_json": script_json,
                        "fallback_reason": str(exc),
                    },
                    image_paths=[],
                    output_json=video_prompt_json,
                    duration_seconds=elapsed,
                )

        self._ensure_script_content_id(
            script_json,
            context=context,
            script_index=script_index,
            record_id=record_id,
            video_prompt_json=video_prompt_json,
        )
        video_prompt_json = compress_final_video_prompt_payload(video_prompt_json)
        validate_video_prompt_payload(video_prompt_json)
        rendered_script = render_script(script_json)
        rendered_video_prompt = render_video_prompt(video_prompt_json)

        self._record_rendered_artifact(
            run_id=run_id,
            record_id=record_id,
            product_code=context["product_code"],
            stage_name=f"{stage_name}_rendered",
            prompt_text=f"render_script({stage_name})",
            input_context={"script_json": script_json},
            rendered_text=rendered_script,
        )
        self._record_rendered_artifact(
            run_id=run_id,
            record_id=record_id,
            product_code=context["product_code"],
            stage_name=f"{video_stage_name}_rendered",
            prompt_text=f"render_video_prompt({video_stage_name})",
            input_context={"video_prompt_json": video_prompt_json},
            rendered_text=rendered_video_prompt,
        )

        return {
            "script_json": script_json,
            "review_json": review_json,
            "video_prompt_json": video_prompt_json,
            "rendered_script": rendered_script,
            "rendered_video_prompt": rendered_video_prompt,
            "passed_review": True,
            "failure_reason": "",
        }

    def _merge_direction_overlap_review(
        self,
        final_strategy: Dict[str, Any],
        script_json: Dict[str, Any],
        review_json: Dict[str, Any],
        existing_scripts: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        overlap_issue = validate_script_direction_separation(
            final_strategy=final_strategy,
            script_json=script_json,
            existing_scripts=existing_scripts or {},
        )
        if not overlap_issue:
            return review_json

        if str(review_json.get("result", "")).strip() == "FAIL":
            issues = review_json.get("issues")
            if isinstance(issues, list) and len(issues) < 3:
                issues.append(
                    {
                        "category": "方向跑偏",
                        "location": "整条脚本结构",
                        "message": overlap_issue,
                    }
                )
            rewrite = review_json.get("rewrite")
            if isinstance(rewrite, list) and len(rewrite) < 4:
                rewrite.append("重写开场触发和前段 proof 顺序，拉开与其他方向脚本的结构差异。")
            return review_json

        return {
            "result": "FAIL",
            "issues": [
                {
                    "category": "方向跑偏",
                    "location": "整条脚本结构",
                    "message": overlap_issue,
                }
            ],
            "rewrite": [
                "保持当前卖点和产品锚点不变。",
                "重写开场触发动作，不要复用已生成方向的进入方式。",
                "重写前段 proof 展开顺序，避免与其他方向同构。",
                "结尾收法也要保留当前方向语气，不要写成同一路径。",
            ],
        }

    @staticmethod
    def _contains_any_token(text: str, tokens: List[str]) -> bool:
        normalized = str(text or "")
        return any(str(token or "").strip() and str(token) in normalized for token in tokens)

    @staticmethod
    def _shot_text(shot: Dict[str, Any]) -> str:
        return " ".join(
            [
                str(shot.get("shot_content", "") or ""),
                str(shot.get("person_action", "") or ""),
                str(shot.get("anchor_reference", "") or ""),
                str(shot.get("scene_function", "") or ""),
                str(shot.get("styling_base_role", "") or ""),
                str(shot.get("voiceover_text_target_language", "") or ""),
            ]
        )

    def _build_hair_clip_review_patch(
        self,
        context: Dict[str, Any],
        anchor_card: Dict[str, Any],
        final_strategy: Dict[str, Any],
        script_json: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if not context.get("hair_clip_mode"):
            return None
        strategy_id = str(final_strategy.get("strategy_id", "") or "").strip().upper()
        if strategy_id not in {"S3", "S4"}:
            return None

        clip_mode = str(context.get("clip_expression_mode", "") or "").strip()
        storyboard = script_json.get("storyboard") if isinstance(script_json.get("storyboard"), list) else []
        shots = [shot for shot in storyboard if isinstance(shot, dict)]
        if not shots:
            return None

        first_two = shots[:2]
        second_shot = shots[1] if len(shots) > 1 and isinstance(shots[1], dict) else {}
        first_two_text = " ".join(
            str(
                " ".join(
                    [
                        str(shot.get("shot_content", "") or ""),
                        str(shot.get("person_action", "") or ""),
                        str(shot.get("anchor_reference", "") or ""),
                        str(shot.get("scene_function", "") or ""),
                    ]
                )
            )
            for shot in first_two
        )
        all_text = " ".join(
            str(
                " ".join(
                    [
                        str(shot.get("shot_content", "") or ""),
                        str(shot.get("person_action", "") or ""),
                        str(shot.get("anchor_reference", "") or ""),
                        str(shot.get("scene_function", "") or ""),
                        str(shot.get("styling_base_role", "") or ""),
                        str(shot.get("voiceover_text_target_language", "") or ""),
                    ]
                )
            )
            for shot in shots
        )
        anchors = self._extract_hair_clip_anchor_values(anchor_card)
        result_tokens = anchors["result_anchor"] + ["整齐", "更稳", "更快", "恢复", "利落", "头型", "干净", "点缀", "女生感", "精致感"]
        action_tokens = anchors["action_anchor"] + ["夹上", "固定", "盘发", "捞发", "整理", "夹刘海", "夹侧边", "摘盔", "摘头盔", "半扎"]
        operational_action_tokens = ["夹上", "固定", "扣上", "盘发", "捞发", "整理", "夹刘海", "夹侧边", "摘盔", "摘头盔", "补夹", "挽发"]
        stability_tokens = anchors["stability_anchor"] + ["不掉", "不散", "稳", "固定", "厚发", "头盔友好", "不移位", "快速", "2秒", "10秒"]
        local_result_tokens = ["脸边", "刘海", "侧边", "碎发", "更干净", "更整齐", "点缀", "女生感", "精致感"] + anchors["result_anchor"]
        material_tokens = ["材质", "缎面", "光泽", "褶皱", "高级感", "质感", "细节"]
        helmet_tokens = ["头盔", "摘盔", "摘头盔", "骑摩托", "骑车", "helmet", "moto"]
        arrival_place_tokens = ["楼下", "门口", "停车点", "电梯口", "入口", "洗手间", "办公楼", "校园", "咖啡店", "公寓", "宿舍"]
        arrival_action_tokens = ["刚到", "到达", "刚停车", "停好车", "手里拿着头盔", "手拿头盔", "摘盔后", "摘头盔后"]
        commute_state_tokens = ["通勤包", "上班", "上学", "出门", "通勤穿搭", "工作日"]
        natural_light_tokens = ["自然光", "户外", "半户外", "门口光", "楼下自然光"]
        indoor_static_tokens = ["卧室", "床边", "梳妆台", "室内", "衣柜前", "房间里"]

        issues: List[Dict[str, str]] = []
        rewrite: List[str] = []

        if clip_mode == "functional":
            if not self._contains_any_token(first_two_text, result_tokens):
                issues.append(
                    {
                        "category": "开场无力或错误",
                        "location": "前2-4秒分镜",
                        "message": "functional 发夹未先给出夹上后的结果或整理后的变化，仍然像旧类目先讲质感/氛围。",
                    }
                )
                rewrite.append("把前2-4秒改成先看到夹上后的结果，再进入动作证明，不要先拍材质或氛围。")
            if not self._contains_any_token(first_two_text, action_tokens):
                issues.append(
                    {
                        "category": "产品关键锚点缺失",
                        "location": "前2-4秒分镜",
                        "message": "functional 发夹前段缺少夹上/固定/盘发/整理等动作证明，结果没有被动作接实。",
                    }
                )
                rewrite.append("在前2-4秒内补一个明确的夹上/固定/盘发/整理动作镜头。")
            if not self._contains_any_token(all_text, stability_tokens):
                issues.append(
                    {
                        "category": "分镜太粗",
                        "location": "全片 proof 段",
                        "message": "functional 发夹缺少稳定性或效率证明，未把“不掉/不散/快速恢复”等顾虑解除拍出来。",
                    }
                )
                rewrite.append("增加一个稳定性或效率证明镜头，例如不掉、不散、快速恢复、厚发也能夹。")
            if strategy_id == "S4" and self._contains_any_token(first_two_text, material_tokens) and not self._contains_any_token(first_two_text, action_tokens):
                issues.append(
                    {
                        "category": "方向跑偏",
                        "location": "前段 proof",
                        "message": "functional 发夹的 S4 首镜后过早滑向材质/细节展示，没有立刻用动作把结果接实。",
                    }
                )
                rewrite.append("S4 首镜后立刻接动作证明，再补材质/风格，不要先做细节拆解。")
            if strategy_id == "S4" and second_shot:
                second_text = self._shot_text(second_shot)
                if self._contains_any_token(second_text, material_tokens) and not self._contains_any_token(second_text, operational_action_tokens):
                    issues.append(
                        {
                            "category": "方向跑偏",
                            "location": "镜头2",
                            "message": "functional 发夹的 S4 第二镜仍在延长材质/光泽展示，没有立刻进入夹上/固定/整理等操作动作证明。",
                        }
                    )
                    rewrite.append("把第二镜改成明确的夹上/固定/整理动作证明，不要继续延长材质或光泽镜头。")
        elif clip_mode == "decorative":
            if not self._contains_any_token(first_two_text, local_result_tokens):
                issues.append(
                    {
                        "category": "开场无力或错误",
                        "location": "前2-4秒分镜",
                        "message": "decorative 发夹未先给出局部整理/点缀后的变化，首镜结果不够明确。",
                    }
                )
                rewrite.append("把首镜改成先看到脸边/刘海/侧边整理后的局部结果。")
            if not self._contains_any_token(first_two_text, action_tokens):
                issues.append(
                    {
                        "category": "分镜太粗",
                        "location": "前2-4秒分镜",
                        "message": "decorative 发夹缺少轻动作整理，局部变化没有被动作证明。",
                    }
                )
                rewrite.append("在前2-4秒补一个轻动作整理镜头，例如夹刘海、夹侧边碎发或轻整理脸边。")
            if self._contains_any_token(all_text, ["儿童", "礼物风", "童趣", "可爱到爆"]) and not self._contains_any_token(all_text, local_result_tokens):
                issues.append(
                    {
                        "category": "方向跑偏",
                        "location": "整条脚本风格",
                        "message": "decorative 发夹滑向儿童礼物风或空泛精致风，没有把局部整理/点缀结果落地。",
                    }
                )
                rewrite.append("收回儿童礼物风表达，改成局部整理或点缀效果的真实结果。")

        helmet_shots = [
            shot for shot in shots
            if self._contains_any_token(
                self._shot_text(shot),
                helmet_tokens,
            )
        ]
        if helmet_shots:
            evidence_count = 0
            if self._contains_any_token(all_text, arrival_place_tokens):
                evidence_count += 1
            if self._contains_any_token(all_text, arrival_action_tokens):
                evidence_count += 1
            if self._contains_any_token(all_text, commute_state_tokens):
                evidence_count += 1
            if self._contains_any_token(all_text, natural_light_tokens):
                evidence_count += 1
            if self._contains_any_token(first_two_text, action_tokens):
                evidence_count += 1

            if evidence_count < 2:
                issues.append(
                    {
                        "category": "分镜太粗",
                        "location": "头盔相关场景",
                        "message": "头盔场景缺少真实通勤到达证据，看起来更像抽象功能场景或摆拍场景。",
                    }
                )
                rewrite.append("把头盔场景改成更真实的到达后节点，补楼下/门口/停车点/电梯口等环境证据，并让整理动作紧跟其后。")

            if len(helmet_shots) > 2:
                issues.append(
                    {
                        "category": "方向跑偏",
                        "location": "头盔相关镜头分配",
                        "message": "头盔元素镜头过多，头盔开始抢走发夹本身的注意力。",
                    }
                )
                rewrite.append("把头盔元素控制在 1-2 个镜头内，其余镜头回到发夹动作、整理结果和恢复后状态。")

            if self._contains_any_token(all_text, indoor_static_tokens) and not self._contains_any_token(all_text, arrival_place_tokens + arrival_action_tokens):
                issues.append(
                    {
                        "category": "方向跑偏",
                        "location": "头盔场景真实性",
                        "message": "头盔场景更像室内静态摆拍，没有落在真实户外或半户外通勤到达节点。",
                    }
                )
                rewrite.append("不要在卧室/梳妆台等室内静态戴盔摆拍，改到楼下、门口、停车点、电梯口或洗手间镜前等真实到达节点。")

        if not issues:
            return None
        return {
            "result": "FAIL",
            "issues": issues[:3],
            "rewrite": rewrite[:4] or ["保持主卖点不变，重写前2-4秒，让结果和动作更早出现。"] ,
        }

    def _validate_hair_clip_strategy_distribution(
        self,
        context: Dict[str, Any],
        strategies: List[Dict[str, Any]],
    ) -> Optional[str]:
        if not context.get("hair_clip_mode") or len(strategies) < 2:
            return None

        clip_mode = str(context.get("clip_expression_mode", "") or "").strip()
        helmet_tokens = ["头盔", "骑摩托", "骑车", "摘盔", "摘头盔", "停车", "helmet", "moto"]
        arrival_tokens = ["到达", "刚到", "楼下", "门口", "入口", "停车点", "电梯口", "洗手间", "镜前", "刚停车", "手里拿着头盔", "手拿头盔"]
        indoor_static_tokens = ["卧室", "床边", "衣柜", "梳妆台", "室内", "房间里"]

        helmet_indexes: List[int] = []
        for index, strategy in enumerate(strategies, start=1):
            text = self._hair_clip_text_blob(
                strategy.get("scene_subspace", ""),
                strategy.get("scene_function", ""),
                strategy.get("opening_strategy", ""),
                strategy.get("opening_first_shot", ""),
                strategy.get("core_proof_method", ""),
                strategy.get("risk_note", ""),
            )
            if any(token in text for token in helmet_tokens):
                helmet_indexes.append(index)
                if clip_mode == "decorative":
                    return "decorative 发夹不应把头盔场景作为默认主场景，应优先回到到达后轻整理或日常局部整理"
                if any(token in text for token in indoor_static_tokens) and not any(token in text for token in arrival_tokens):
                    return "头盔场景应优先落在真实通勤到达节点，不要写成纯室内静态戴盔摆拍"

        if len(helmet_indexes) > 1:
            return f"发夹类多脚本里头盔场景最多只允许 1 条，当前命中了 {len(helmet_indexes)} 条"
        return None

    def _merge_hair_clip_review(
        self,
        context: Dict[str, Any],
        anchor_card: Dict[str, Any],
        final_strategy: Dict[str, Any],
        script_json: Dict[str, Any],
        review_json: Dict[str, Any],
    ) -> Dict[str, Any]:
        local_review = self._build_hair_clip_review_patch(context, anchor_card, final_strategy, script_json)
        if not local_review:
            return review_json
        if str(review_json.get("result", "")).strip() == "FAIL":
            issues = review_json.get("issues")
            if isinstance(issues, list):
                for issue in local_review.get("issues", []):
                    if len(issues) >= 3:
                        break
                    issues.append(issue)
            rewrite = review_json.get("rewrite")
            if isinstance(rewrite, list):
                for item in local_review.get("rewrite", []):
                    if len(rewrite) >= 4:
                        break
                    rewrite.append(item)
            return review_json
        return local_review

    @staticmethod
    def _summarize_review_failure(review_json: Dict[str, Any]) -> str:
        if "major_issues" in review_json:
            major_issues = [str(item or "").strip() for item in (review_json.get("major_issues") or []) if str(item or "").strip()]
            if major_issues:
                return "；".join(major_issues[:3])
        issues = review_json.get("issues", []) or []
        if not isinstance(issues, list) or not issues:
            return "脚本达到最大修订次数后仍未通过质检"
        parts: List[str] = []
        for issue in issues[:3]:
            if not isinstance(issue, dict):
                continue
            category = str(issue.get("category", "") or "").strip()
            location = str(issue.get("location", "") or "").strip()
            message = str(issue.get("message", "") or "").strip()
            fragment = " ".join(part for part in [category, location, message] if part)
            if fragment:
                parts.append(fragment)
        if not parts:
            return "脚本达到最大修订次数后仍未通过质检"
        return "；".join(parts)

    def _record_rendered_artifact(
        self,
        run_id: Optional[int],
        record_id: str,
        product_code: str,
        stage_name: str,
        prompt_text: str,
        input_context: Dict[str, Any],
        rendered_text: str,
    ) -> None:
        if run_id is None:
            return
        self.storage.record_stage_result(
            run_id=run_id,
            record_id=record_id,
            product_code=product_code,
            stage_name=stage_name,
            stage_order=self._resolve_stage_order(stage_name),
            status="success",
            prompt_text=prompt_text,
            input_context=input_context,
            image_paths=[],
            rendered_text=rendered_text,
        )

    def _process_variant_only_record(
        self,
        record: TaskRecord,
        context: Dict[str, Any],
        record_id: str,
        run_id: Optional[int],
        logs: List[str],
        stage_durations: Dict[str, float],
        llm_client: OriginalScriptLLMClient,
    ) -> None:
        self._write_update(
            record_id,
            logs,
            stage_durations,
            status=self._runtime_status(STATUS_RUNNING_VARIANTS),
        )

        anchor_card = self._load_variant_context_json(
            record=record,
            logical_name="anchor_card_json",
            fallback_stage_name=VARIANT_STAGE_LOOKUP["anchor_card_json"],
            product_code=context["product_code"],
        )
        try:
            persona_style_emotion_pack = self._load_variant_context_json(
                record=record,
                logical_name="styling_plan_json",
                fallback_stage_name=VARIANT_STAGE_LOOKUP["styling_plan_json"],
                product_code=context["product_code"],
            )
        except Exception:
            persona_style_emotion_pack = {}
        final_contexts: Dict[str, Dict[str, Any]] = {}
        expression_contexts: Dict[str, Dict[str, Any]] = {}
        script_contexts: Dict[str, Dict[str, Any]] = {}
        selected_groups = self._selected_variant_groups()

        for group in selected_groups:
            final_contexts[group["final_field"]] = self._load_variant_context_json(
                record=record,
                logical_name=group["final_field"],
                fallback_stage_name=VARIANT_STAGE_LOOKUP[group["final_field"]],
                product_code=context["product_code"],
            )
            expression_contexts[group["exp_field"]] = self._load_variant_context_json(
                record=record,
                logical_name=group["exp_field"],
                fallback_stage_name=VARIANT_STAGE_LOOKUP[group["exp_field"]],
                product_code=context["product_code"],
            )
            script_contexts[group["script_json_field"]] = self._load_script_json_for_variants(
                record=record,
                logical_name=group["script_json_field"],
                fallback_stage_name=group["fallback_stage_name"],
                product_code=context["product_code"],
            )

        self._process_variants_with_context(
            context=context,
            record_id=record_id,
            run_id=run_id,
            logs=logs,
            stage_durations=stage_durations,
            llm_client=llm_client,
            anchor_card=anchor_card,
            persona_style_emotion_pack=persona_style_emotion_pack,
            final_contexts=final_contexts,
            expression_contexts=expression_contexts,
            script_contexts=script_contexts,
        )
        print("  ✅ 脚本变体生成完成")

    def _process_variants_with_context(
        self,
        context: Dict[str, Any],
        record_id: str,
        run_id: Optional[int],
        logs: List[str],
        stage_durations: Dict[str, float],
        llm_client: OriginalScriptLLMClient,
        anchor_card: Dict[str, Any],
        persona_style_emotion_pack: Dict[str, Any],
        final_contexts: Dict[str, Dict[str, Any]],
        expression_contexts: Dict[str, Dict[str, Any]],
        script_contexts: Dict[str, Dict[str, Any]],
        final_extra_values: Optional[Dict[str, Any]] = None,
        selected_script_indexes: Optional[set] = None,
        final_runtime_status: Optional[str] = None,
        final_error_message: str = "",
    ) -> None:
        self._validate_variant_output_fields(selected_script_indexes=selected_script_indexes)
        selected_groups = self._selected_variant_groups(selected_script_indexes=selected_script_indexes)
        update_values: Dict[str, Any] = {}
        content_id_values: Dict[str, str] = {}
        completed_variant_count = 0
        total_variant_count = len(selected_groups) * sum(len(batch) for batch in VARIANT_BATCHES)

        for group in selected_groups:
            variants_payload = self._generate_variants_for_script(
                script_index=group["script_index"],
                context=context,
                target_country=context["target_country"],
                target_language=context["target_language"],
                product_type=context["product_type"],
                anchor_card_json=anchor_card,
                final_strategy_json=final_contexts[group["final_field"]],
                expression_plan_json=expression_contexts[group["exp_field"]],
                persona_style_emotion_pack_json=persona_style_emotion_pack,
                original_script_json=script_contexts[group["script_json_field"]],
                product_selling_note=context.get("product_selling_note", ""),
                run_id=run_id,
                record_id=record_id,
                product_code=context["product_code"],
                stage_durations=stage_durations,
                llm_client=llm_client,
                on_variant_generated=lambda variant, all_variants, current_group=group: self._write_variant_progress(
                    record_id=record_id,
                    logs=logs,
                    stage_durations=stage_durations,
                    group=current_group,
                    variant=variant,
                    all_variants=all_variants,
                    completed_count=completed_variant_count + len(all_variants),
                    total_count=total_variant_count,
                ),
            )
            completed_variant_count += len(variants_payload.get("variants", []) or [])
            update_values[group["variant_json_field"]] = self._dump_json(variants_payload)
            content_id_values.update(
                self._collect_variant_content_ids(
                    variants_payload=variants_payload,
                    script_index=group["script_index"],
                )
            )
            rendered_variants = [
                render_variant_script(variant)
                for variant in (variants_payload.get("variants", []) or [])
            ]
            for field_name, rendered_text in zip(group["render_fields"], rendered_variants):
                update_values[field_name] = rendered_text

        final_payload = {
            **update_values,
            "last_run_at": self._now_string(),
            "error_message": final_error_message or "",
        }
        if final_extra_values:
            final_payload.update(final_extra_values)
        if final_error_message:
            final_payload["error_message"] = final_error_message

        resolved_final_status = final_runtime_status or STATUS_DONE_VARIANTS
        self._write_update(
            record_id,
            logs,
            stage_durations,
            status=self._runtime_status(resolved_final_status),
            extra_values=final_payload,
        )
        if run_id is not None:
            self.storage.update_run_artifacts(run_id, content_ids=content_id_values)
            self.storage.update_run_status(
                run_id,
                runtime_status=self._runtime_status(resolved_final_status),
                error_message=final_error_message or None,
                stage_durations=stage_durations,
                completed=True,
            )
        logs.append("脚本变体生成完成")
        self._flush_log_fields(record_id, logs, stage_durations)

    # 兼容业务文档里的函数命名，内部继续复用现有主流程。
    def process_script_variants(
        self,
        record: TaskRecord,
        llm_client: Optional[OriginalScriptLLMClient] = None,
    ) -> bool:
        context = self._build_context(record)
        self._validate_variant_inputs(record, context)
        self._validate_variant_output_fields()
        self._process_variant_only_record(
            record=record,
            context=context,
            record_id=record.record_id,
            run_id=None,
            logs=[],
            stage_durations={},
            llm_client=llm_client or OriginalScriptLLMClient(route=self.llm_route, route_order=self.llm_route_order),
        )
        return True

    def generate_variants_for_script(
        self,
        script_index: int,
        target_country: str,
        target_language: str,
        product_type: str,
        anchor_card_json: Dict[str, Any],
        final_strategy_json: Dict[str, Any],
        expression_plan_json: Dict[str, Any],
        original_script_json: Dict[str, Any],
        persona_style_emotion_pack_json: Optional[Dict[str, Any]] = None,
        product_selling_note: str = "",
        llm_client: Optional[OriginalScriptLLMClient] = None,
    ) -> Dict[str, Any]:
        return self._generate_variants_for_script(
            script_index=script_index,
            context={
                "product_code": "",
                "target_country": target_country,
                "target_language": target_language,
                "product_type": product_type,
                "top_category": "",
                "product_selling_note": product_selling_note,
            },
            target_country=target_country,
            target_language=target_language,
            product_type=product_type,
            anchor_card_json=anchor_card_json,
            final_strategy_json=final_strategy_json,
            expression_plan_json=expression_plan_json,
            persona_style_emotion_pack_json=persona_style_emotion_pack_json or {},
            original_script_json=original_script_json,
            product_selling_note=product_selling_note,
            run_id=None,
            record_id="manual_preview",
            product_code="",
            stage_durations={},
            llm_client=llm_client or OriginalScriptLLMClient(route=self.llm_route, route_order=self.llm_route_order),
        )

    def _write_variant_progress(
        self,
        record_id: str,
        logs: List[str],
        stage_durations: Dict[str, float],
        group: Dict[str, Any],
        variant: Dict[str, Any],
        all_variants: List[Dict[str, Any]],
        completed_count: int,
        total_count: int,
    ) -> None:
        variant_id = str(variant.get("variant_id", "")).strip().upper()
        if not variant_id.startswith("V"):
            return
        try:
            variant_index = int(variant_id[1:]) - 1
        except ValueError:
            return
        if variant_index < 0 or variant_index >= len(group["render_fields"]):
            return

        render_field = group["render_fields"][variant_index]
        progress_fields: Dict[str, Any] = {
            render_field: render_variant_script(variant),
            "error_message": "",
        }
        if group.get("variant_json_field"):
            progress_fields[group["variant_json_field"]] = self._dump_json(
                {
                    "variant_count": len(all_variants),
                    "variants": all_variants,
                }
            )

        payload = build_update_payload(
            self.mapping,
            {
                "status": self._runtime_status(STATUS_RUNNING_VARIANTS),
                **progress_fields,
            },
        )
        if payload:
            self.client.update_record_fields(record_id, payload)

        logs.append(
            f"已回写脚本{group['script_index']}变体{variant_index + 1}（{completed_count}/{total_count}）"
        )
        self._flush_log_fields(record_id, logs, stage_durations)

    def _ensure_script_content_id(
        self,
        script_json: Dict[str, Any],
        context: Dict[str, Any],
        script_index: int,
        record_id: str,
        video_prompt_json: Optional[Dict[str, Any]] = None,
    ) -> str:
        content_id = str(script_json.get("content_id", "") or "").strip()
        unified_id = build_script_id_from_context(context, script_index=script_index, variant_no=None, record_id=record_id)
        if content_id != unified_id:
            content_id = unified_id
            script_json["content_id"] = content_id
        if isinstance(video_prompt_json, dict):
            current_prompt_id = str(video_prompt_json.get("content_id", "") or "").strip()
            if current_prompt_id != unified_id:
                video_prompt_json["content_id"] = content_id
        return content_id

    def _ensure_variant_content_id(
        self,
        variant: Dict[str, Any],
        context: Dict[str, Any],
        script_index: int,
        record_id: str,
    ) -> str:
        content_id = str(variant.get("content_id", "") or "").strip()
        raw_variant_no = variant.get("variant_no")
        variant_no = raw_variant_no if isinstance(raw_variant_no, int) else None
        if variant_no is None:
            variant_id = str(variant.get("variant_id", "") or "").strip().upper()
            if variant_id.startswith("V") and variant_id[1:].isdigit():
                variant_no = int(variant_id[1:])
        unified_id = build_script_id_from_context(context, script_index=script_index, variant_no=variant_no, record_id=record_id)
        if content_id != unified_id:
            content_id = unified_id
            variant["content_id"] = content_id
        final_prompt = variant.get("final_video_script_prompt")
        if isinstance(final_prompt, dict):
            current_prompt_id = str(final_prompt.get("content_id", "") or "").strip()
            if current_prompt_id != unified_id:
                final_prompt["content_id"] = content_id
        return content_id

    @staticmethod
    def _collect_script_bundle_content_ids(script_bundles: List[Dict[str, Any]]) -> Dict[str, str]:
        content_ids: Dict[str, str] = {}
        for index, bundle in enumerate(script_bundles, start=1):
            script_json = bundle.get("script_json") or {}
            video_prompt_json = bundle.get("video_prompt_json") or {}
            script_id = str(script_json.get("content_id", "") or "").strip()
            prompt_id = str(video_prompt_json.get("content_id", "") or "").strip()
            if script_id:
                content_ids[f"script_s{index}"] = script_id
            if prompt_id:
                content_ids[f"video_prompt_s{index}"] = prompt_id
        return content_ids

    @staticmethod
    def _collect_variant_content_ids(variants_payload: Dict[str, Any], script_index: int) -> Dict[str, str]:
        content_ids: Dict[str, str] = {}
        for variant in variants_payload.get("variants", []) or []:
            if not isinstance(variant, dict):
                continue
            variant_id = str(variant.get("variant_id", "") or "").strip().upper()
            content_id = str(variant.get("content_id", "") or "").strip()
            if variant_id and content_id:
                content_ids[f"script_{script_index}_{variant_id.lower()}"] = content_id
        return content_ids

    @staticmethod
    def _source_script_id(script_index: int) -> str:
        return f"script_s{script_index}"

    def _build_direction_allowed_pool(self, final_strategy_json: Dict[str, Any]) -> Dict[str, Any]:
        strategy_id = str(final_strategy_json.get("strategy_id", "") or "").strip().upper()
        return dict(DIRECTION_ALLOWED_POOLS.get(strategy_id, {}))

    def _build_variant_profiles(self, strategy_id: str) -> List[Dict[str, Any]]:
        normalized = str(strategy_id or "").strip().upper()
        profiles = [dict(item) for item in DEFAULT_VARIANT_PROFILES.get(normalized, DEFAULT_VARIANT_PROFILES["S1"])]

        heavy_enabled = os.environ.get("ORIGINAL_SCRIPT_ENABLE_HEAVY_VARIANTS", "").strip().lower() in {"1", "true", "yes", "on"}
        heavy_s4_enabled = os.environ.get("ORIGINAL_SCRIPT_ENABLE_HEAVY_S4_VARIANTS", "").strip().lower() in {"1", "true", "yes", "on"}

        if normalized in {"S1", "S2", "S3"} and heavy_enabled and profiles:
            profiles[-1]["variant_strength"] = VARIANT_STRENGTH_HEAVY
            profiles[-1]["variant_focus"] = "rhythm"
        if normalized == "S4" and heavy_s4_enabled and profiles:
            profiles[-1]["variant_strength"] = VARIANT_STRENGTH_HEAVY
            profiles[-1]["variant_focus"] = "opening"

        return profiles

    def _build_variant_layers(
        self,
        target_country: str,
        product_type: str,
        final_strategy_json: Dict[str, Any],
        original_script_json: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        constraints = original_script_json.get("execution_constraints", {}) or {}
        storyboard = original_script_json.get("storyboard", []) or []
        scene_subspace = str(final_strategy_json.get("scene_subspace", "") or "").strip()
        scene_function = str(final_strategy_json.get("scene_function", "") or "").strip()
        styling_logic = str(final_strategy_json.get("styling_base_logic", "") or "").strip()
        styling_constraints = "；".join(final_strategy_json.get("styling_base_constraints", []) or [])
        persona_state = str(final_strategy_json.get("persona_state", "") or "").strip()
        persona_polish = str(final_strategy_json.get("persona_polish_level", "") or "").strip()
        persona_role = str(final_strategy_json.get("persona_presence_role", "") or "").strip()
        scene_domain_base = "家中自然分享" if is_sea_market(target_country) else "日常生活分享"
        if "镜" in scene_subspace:
            scene_domain_base = "出门前 / 对镜整理空间"

        person_layer = {
            "person_identity_base": f"{target_country or '本地'}日常分享女生",
            "person_style_base": f"{persona_state} / {persona_polish}".strip(" /"),
            "appearance_boundary": str(constraints.get("person_constraints", "") or "真实、不网红化、不过度精致"),
            "body_presentation_boundary": "正常身材、真实比例、不极端展示",
            "camera_relationship": persona_role or "轻分享、非主播感、非强模特感",
        }
        outfit_layer = {
            "outfit_core_formula": styling_logic or f"{product_type} + 日常基础搭配",
            "product_role_in_outfit": str(final_strategy_json.get("product_dominance_rule", "") or "商品始终是主角"),
            "silhouette_boundary": str(constraints.get("styling_constraints", "") or styling_constraints or "保持真实日常轮廓，不做极端廓形"),
            "pairing_boundary": styling_constraints or "不得让配搭抢掉商品主角",
            "color_mood_boundary": "干净、清爽、不杂乱、不高饱和乱堆",
        }
        scene_layer = {
            "scene_domain_base": scene_domain_base,
            "scene_subspace": scene_subspace,
            "scene_function_moment": scene_function or "结果确认 / 轻分享时刻",
            "light_boundary": "自然光 / 柔和室内光 / 真实生活光线",
            "prop_boundary": "只允许镜子、桌边物品、包、衣柜等生活道具，不要复杂商业陈设",
        }
        emotion_layer = {
            "emotion_base": f"{final_strategy_json.get('opening_emotion', '')} -> {final_strategy_json.get('middle_emotion', '')} -> {final_strategy_json.get('ending_emotion', '')}",
            "emotion_curve": f"开头 {final_strategy_json.get('opening_emotion', '')} / 中段 {final_strategy_json.get('middle_emotion', '')} / 结尾 {final_strategy_json.get('ending_emotion', '')}",
            "emotion_intensity_boundary": "只允许轻流动，不允许夸张表演、强主播感、完整情绪剧本",
            "delivery_boundary": "不喊卖、不强表演、不强销售感、不强模特感",
        }
        if storyboard:
            first_state = str((storyboard[0] or {}).get("person_state", "") or "").strip()
            if first_state:
                person_layer["person_style_base"] = first_state
        return {
            "person_variant_layer": person_layer,
            "outfit_variant_layer": outfit_layer,
            "scene_variant_layer": scene_layer,
            "emotion_variant_layer": emotion_layer,
        }

    def _validate_variant_batch_semantics(
        self,
        payload: Dict[str, Any],
        expected_profiles: List[Dict[str, Any]],
        source_script_id: str,
        source_strategy_id: str,
        final_strategy_json: Dict[str, Any],
        target_country: str,
    ) -> None:
        variants = payload.get("variants", []) or []
        expected_by_id = {str(item["variant_id"]): item for item in expected_profiles}
        source_strategy_name = str(final_strategy_json.get("strategy_name", "") or "").strip()
        source_primary = str(final_strategy_json.get("primary_selling_point", "") or "").strip()
        source_direction = str(final_strategy_json.get("strategy_id", "") or "").strip()
        for variant in variants:
            variant_id = str(variant.get("variant_id", "") or "").strip()
            expected = expected_by_id.get(variant_id, {})
            internal_state = variant.get("internal_variant_state") or {}
            final_prompt = variant.get("final_video_script_prompt") or {}
            if is_sea_market(target_country):
                self._normalize_variant_sea_scene(variant)
            if str(variant.get("source_script_id", "") or "").strip() != source_script_id:
                raise JsonStageError(f"{variant_id}: source_script_id 与正式脚本不一致")
            if str(variant.get("source_strategy_id", "") or "").strip() != source_strategy_id:
                raise JsonStageError(f"{variant_id}: source_strategy_id 与正式策略不一致")
            normalized_strategy_id = self._normalize_variant_strategy_id(
                str(variant.get("strategy_id", "") or "").strip(),
                source_direction=source_direction,
                source_strategy_id=source_strategy_id,
                variant_id=variant_id,
            )
            if normalized_strategy_id != source_direction:
                raise JsonStageError(f"{variant_id}: strategy_id 漂移，疑似跨方向")
            variant["strategy_id"] = source_direction
            if str(variant.get("strategy_name", "") or "").strip() != source_strategy_name:
                variant["strategy_name"] = source_strategy_name
            if str(variant.get("primary_selling_point", "") or "").strip() != source_primary:
                variant["primary_selling_point"] = source_primary
            if expected:
                if str(variant.get("variant_strength", "") or "").strip() != str(expected.get("variant_strength", "")):
                    raise JsonStageError(f"{variant_id}: variant_strength 不符合当前批次权限")
                if str(variant.get("variant_focus", "") or "").strip() != str(expected.get("variant_focus", "")):
                    raise JsonStageError(f"{variant_id}: variant_focus 不符合当前批次设置")

            if not isinstance(internal_state, dict):
                internal_state = {}

            joined_text = self._collect_variant_text(variant)
            production_text = self._collect_variant_production_text(final_prompt)
            lowered = production_text.lower()
            if self._contains_unnegated_risk(
                production_text,
                ["廉价电商", "假高级"],
            ):
                raise JsonStageError(f"{variant_id}: 广告化风险过高")
            if self._contains_unnegated_risk(
                lowered,
                ["studio", "campaign", "runway", "lookbook"],
            ):
                raise JsonStageError(f"{variant_id}: 出现非生活语境的广告化表达")
            person_layer = internal_state.get("person_variant_layer", {}) or {}
            outfit_layer = internal_state.get("outfit_variant_layer", {}) or {}
            scene_layer = internal_state.get("scene_variant_layer", {}) or {}
            emotion_layer = internal_state.get("emotion_variant_layer", {}) or {}
            scene_hint_text = " ".join(
                [
                    production_text,
                    str((final_prompt.get("video_setup") or {}).get("scene_final", "") or ""),
                    str(scene_layer.get("scene_domain_base", "") or "") if isinstance(scene_layer, dict) else "",
                    str(scene_layer.get("scene_function_moment", "") or "") if isinstance(scene_layer, dict) else "",
                ]
            )
            home_scene_ok = is_home_share_scene(scene_hint_text)
            if (
                self._contains_unnegated_risk(production_text, ["广告片", "棚拍", "商拍"])
                and not home_scene_ok
            ):
                raise JsonStageError(f"{variant_id}: 广告化风险过高")
            if is_sea_market(target_country) and not home_scene_ok:
                raise JsonStageError(f"{variant_id}: 破坏东南亚家中自然分享语境")
            if self._contains_unnegated_risk(
                str(person_layer.get("camera_relationship", "") or ""),
                ["主播", "导购", "强模特", "销售"],
            ):
                raise JsonStageError(f"{variant_id}: 人物脱离原目标人群边界")
            if self._contains_unnegated_risk(
                str(outfit_layer.get("product_role_in_outfit", "") or ""),
                ["配角", "弱化商品", "退居次要", "商品不是主角"],
            ):
                raise JsonStageError(f"{variant_id}: 穿搭削弱了商品主角色")
            if self._contains_unnegated_risk(
                str(scene_layer.get("scene_domain_base", "") or ""),
                ["棚拍", "商业片场", "广告棚"],
            ):
                raise JsonStageError(f"{variant_id}: 场景脱离原生活语境")
            if self._contains_unnegated_risk(
                str(emotion_layer.get("delivery_boundary", "") or ""),
                ["强表演", "强销售", "喊卖"],
            ):
                raise JsonStageError(f"{variant_id}: 情绪边界失控")

            changed_structure_fields = internal_state.get("changed_structure_fields", []) or []
            changed_feeling_layers = internal_state.get("changed_feeling_layers", []) or []
            proof_blueprint = internal_state.get("proof_blueprint", []) or []
            consistency_checks = internal_state.get("consistency_checks", {}) or {}

            if isinstance(changed_feeling_layers, list):
                normalized_layers = []
                alias_map = {
                    "persona": "person",
                    "人物": "person",
                    "styling": "outfit",
                    "穿搭": "outfit",
                    "搭配": "outfit",
                    "场景": "scene",
                    "情绪": "emotion",
                    "mood": "emotion",
                }
                for item in changed_feeling_layers:
                    text = str(item or "").strip()
                    normalized_layers.append(alias_map.get(text, alias_map.get(text.lower(), text.lower())))
                deduped_layers = []
                for item in normalized_layers:
                    if item and item not in deduped_layers:
                        deduped_layers.append(item)
                changed_feeling_layers = deduped_layers

            if isinstance(consistency_checks, dict):
                normalized_checks = {}
                for key, value in consistency_checks.items():
                    mapped = {
                        "persona_manifestation": "person_manifestation",
                        "styling_manifestation": "outfit_manifestation",
                        "mood_manifestation": "emotion_manifestation",
                    }.get(str(key or "").strip(), str(key or "").strip())
                    normalized_checks[mapped] = value
                consistency_checks = normalized_checks

            if str(variant.get("variant_strength", "") or "").strip() == VARIANT_STRENGTH_LIGHT:
                if len(changed_feeling_layers) > 2:
                    changed_feeling_layers = changed_feeling_layers[:2]
                    internal_state["changed_feeling_layers"] = changed_feeling_layers
                if len(changed_structure_fields) > 2:
                    changed_structure_fields = changed_structure_fields[:2]
                    internal_state["changed_structure_fields"] = changed_structure_fields

            if str(variant.get("variant_strength", "") or "").strip() == VARIANT_STRENGTH_MEDIUM:
                if changed_structure_fields and len(changed_structure_fields) < 2:
                    raise JsonStageError(f"{variant_id}: medium 变体的结构字段变化不足 2 个")
                if changed_feeling_layers and len(changed_feeling_layers) < 2:
                    raise JsonStageError(f"{variant_id}: medium 变体的视频感受层变化不足 2 类")

            if proof_blueprint and not any(
                str(item.get("concern_relieved", "") or "").strip() for item in proof_blueprint if isinstance(item, dict)
            ):
                raise JsonStageError(f"{variant_id}: proof 缺少顾虑解除")

            layer_to_check_key = {
                "person": "person_manifestation",
                "outfit": "outfit_manifestation",
                "scene": "scene_manifestation",
                "emotion": "emotion_manifestation",
            }
            variant_strength = str(variant.get("variant_strength", "") or "").strip()
            for layer in changed_feeling_layers:
                key = layer_to_check_key.get(str(layer).strip())
                if not key:
                    continue
                declared = str(consistency_checks.get(key, "") or "").strip() if consistency_checks else ""
                manifested = declared or self._infer_variant_layer_manifestation(final_prompt, str(layer).strip())
                if not manifested and variant_strength in {VARIANT_STRENGTH_MEDIUM, VARIANT_STRENGTH_HEAVY}:
                    raise JsonStageError(f"{variant_id}: {layer} 变化未在正文中体现")

            shot_execution = final_prompt.get("shot_execution", []) or []
            if not shot_execution:
                raise JsonStageError(f"{variant_id}: final_video_script_prompt 缺少分镜执行")
            if not any(str(item.get("product_focus", "") or "").strip() for item in shot_execution if isinstance(item, dict)):
                raise JsonStageError(f"{variant_id}: 产品关键锚点缺失")

    @staticmethod
    def _normalize_variant_strategy_id(
        raw_strategy_id: str,
        *,
        source_direction: str,
        source_strategy_id: str,
        variant_id: str,
    ) -> str:
        candidate = str(raw_strategy_id or "").strip()
        direction = str(source_direction or "").strip()
        strategy = str(source_strategy_id or "").strip()
        if not candidate or not direction:
            return candidate

        candidate_upper = candidate.upper()
        direction_upper = direction.upper()
        strategy_upper = strategy.upper()
        variant_upper = str(variant_id or "").strip().upper()

        if candidate_upper == direction_upper:
            return direction
        if strategy_upper and candidate_upper == strategy_upper:
            return direction
        if strategy_upper.startswith("FINAL_") and strategy_upper[6:] == candidate_upper:
            return direction
        if candidate_upper.startswith("FINAL_") and candidate_upper[6:] == direction_upper:
            return direction

        candidate_bases = [candidate_upper]
        if variant_upper:
            suffixes = (f"_{variant_upper}", f"-{variant_upper}", variant_upper)
            for suffix in suffixes:
                if candidate_upper.endswith(suffix) and len(candidate_upper) > len(suffix):
                    candidate_bases.append(candidate_upper[: -len(suffix)])

        for base in candidate_bases:
            if base == direction_upper:
                return direction
            if strategy_upper and base == strategy_upper:
                return direction
            if base.startswith("FINAL_") and base[6:] == direction_upper:
                return direction

        return candidate

    @staticmethod
    def _is_generic_variant_scene_text(text: str) -> bool:
        normalized = str(text or "").strip().lower()
        if not normalized:
            return True
        generic_tokens = (
            "生活化真实场景",
            "真实场景",
            "日常场景",
            "生活化场景",
            "生活化真实",
            "真实生活场景",
            "原生生活场景",
            "原生真实场景",
            "原生自然场景",
            "natural real-life scene",
            "real life scene",
            "daily scene",
            "lifestyle scene",
        )
        return any(token in normalized for token in generic_tokens)

    @staticmethod
    def _sea_scene_from_variant_id(variant_id: str) -> str:
        mapping = {
            "V1": "家中镜前/玄关镜前",
            "V2": "家中衣柜/穿衣区",
            "V3": "家中梳妆台/桌边",
            "V4": "家中窗边自然光",
            "V5": "家中床边/坐姿分享",
        }
        return mapping.get(str(variant_id or "").strip().upper(), preferred_sea_scene_order()[-1])

    @classmethod
    def _normalize_variant_sea_scene(cls, variant: Dict[str, Any]) -> None:
        if not isinstance(variant, dict):
            return
        final_prompt = variant.get("final_video_script_prompt") or {}
        if not isinstance(final_prompt, dict):
            return
        video_setup = final_prompt.get("video_setup") or {}
        if not isinstance(video_setup, dict):
            video_setup = {}
            final_prompt["video_setup"] = video_setup

        internal_state = variant.get("internal_variant_state") or {}
        if not isinstance(internal_state, dict):
            internal_state = {}
            variant["internal_variant_state"] = internal_state
        scene_layer = internal_state.get("scene_variant_layer") or {}
        if not isinstance(scene_layer, dict):
            scene_layer = {}
            internal_state["scene_variant_layer"] = scene_layer

        current_scene = str(video_setup.get("scene_final", "") or "").strip()
        scene_candidates = [
            current_scene,
            str(scene_layer.get("scene_subspace", "") or "").strip(),
            str(scene_layer.get("scene_function_moment", "") or "").strip(),
            str(scene_layer.get("scene_domain_base", "") or "").strip(),
        ]
        if any(is_home_share_scene(text) for text in scene_candidates if text) and not cls._is_generic_variant_scene_text(current_scene):
            return

        explicit_scene = ""
        for candidate in scene_candidates[1:]:
            if candidate and is_home_share_scene(candidate):
                explicit_scene = candidate
                break
        if not explicit_scene:
            explicit_scene = cls._sea_scene_from_variant_id(str(variant.get("variant_id", "") or "").strip())

        video_setup["scene_final"] = explicit_scene
        scene_layer.setdefault("scene_domain_base", "家中自然分享")
        if not str(scene_layer.get("scene_subspace", "") or "").strip():
            scene_layer["scene_subspace"] = explicit_scene
        if not str(scene_layer.get("scene_function_moment", "") or "").strip():
            scene_layer["scene_function_moment"] = f"{explicit_scene}里的真实顺手整理语境"

    @staticmethod
    def _collect_variant_text(variant: Dict[str, Any]) -> str:
        text_parts: List[str] = []
        final_prompt = variant.get("final_video_script_prompt") or {}
        internal_state = variant.get("internal_variant_state") or {}
        for key in ("video_setup",):
            value = final_prompt.get(key) or {}
            if isinstance(value, dict):
                text_parts.extend(str(item) for item in value.values() if item)
        for key in ("style_boundaries", "negative_constraints"):
            value = final_prompt.get(key) or []
            if isinstance(value, list):
                text_parts.extend(str(item) for item in value if item)
        for shot in final_prompt.get("shot_execution", []) or []:
            if isinstance(shot, dict):
                text_parts.extend(
                    str(shot.get(key, ""))
                    for key in (
                        "visual",
                        "person_action",
                        "product_focus",
                        "voiceover",
                        "subtitle",
                    )
                    if shot.get(key)
                )
        for key in (
            "variant_name",
            "main_adjustment",
            "test_goal",
            "variant_change_summary",
            "main_change",
            "secondary_change",
            "difference_summary",
        ):
            value = internal_state.get(key)
            if value:
                text_parts.append(str(value))
        for key in ("person_variant_layer", "outfit_variant_layer", "scene_variant_layer", "emotion_variant_layer"):
            value = internal_state.get(key) or {}
            if isinstance(value, dict):
                text_parts.extend(str(item) for item in value.values() if item)
        consistency_checks = internal_state.get("consistency_checks") or {}
        if isinstance(consistency_checks, dict):
            text_parts.extend(str(item) for item in consistency_checks.values() if item)
        proof_blueprint = internal_state.get("proof_blueprint") or []
        if isinstance(proof_blueprint, list):
            for item in proof_blueprint:
                if isinstance(item, dict):
                    text_parts.extend(str(value) for value in item.values() if value)
        return "\n".join(part for part in text_parts if part)

    @staticmethod
    def _collect_variant_production_text(final_prompt: Dict[str, Any]) -> str:
        text_parts: List[str] = []
        video_setup = final_prompt.get("video_setup") or {}
        if isinstance(video_setup, dict):
            text_parts.extend(str(item) for item in video_setup.values() if item)
        for key in ("style_boundaries", "negative_constraints"):
            value = final_prompt.get(key) or []
            if isinstance(value, list):
                text_parts.extend(str(item) for item in value if item)
        for shot in final_prompt.get("shot_execution", []) or []:
            if isinstance(shot, dict):
                text_parts.extend(
                    str(shot.get(field, ""))
                    for field in ("visual", "person_action", "product_focus", "voiceover", "subtitle")
                    if shot.get(field)
                )
        return "\n".join(part for part in text_parts if part)

    @staticmethod
    def _infer_variant_layer_manifestation(final_prompt: Dict[str, Any], layer: str) -> str:
        video_setup = final_prompt.get("video_setup") or {}
        shots = final_prompt.get("shot_execution", []) or []
        if layer == "person":
            values = []
            if isinstance(video_setup, dict):
                values.append(str(video_setup.get("person_final", "") or ""))
            values.extend(str(item.get("person_action", "") or "") for item in shots if isinstance(item, dict))
            return " / ".join([v for v in values if v.strip()][:3]).strip()
        if layer == "outfit":
            values = []
            if isinstance(video_setup, dict):
                values.append(str(video_setup.get("outfit_final", "") or ""))
            values.extend(str(item.get("product_focus", "") or "") for item in shots if isinstance(item, dict))
            return " / ".join([v for v in values if v.strip()][:3]).strip()
        if layer == "scene":
            values = []
            if isinstance(video_setup, dict):
                values.append(str(video_setup.get("scene_final", "") or ""))
            values.extend(str(item.get("visual", "") or "") for item in shots if isinstance(item, dict))
            return " / ".join([v for v in values if v.strip()][:3]).strip()
        if layer == "emotion":
            values = []
            if isinstance(video_setup, dict):
                values.append(str(video_setup.get("emotion_final", "") or ""))
            values.extend(str(item.get("voiceover", "") or "") for item in shots if isinstance(item, dict))
            return " / ".join([v for v in values if v.strip()][:3]).strip()
        return ""

    @staticmethod
    def _contains_unnegated_risk(text: str, keywords: List[str]) -> bool:
        normalized = str(text or "")
        negation_markers = (
            "不",
            "非",
            "无",
            "别",
            "勿",
            "避免",
            "不要",
            "不能",
            "不得",
            "禁止",
            "no ",
            "not ",
            "avoid",
            "without",
            "dilarang",
            "jangan",
            "tanpa",
            "bukan",
            "tak",
            "tidak",
            "không",
            "khong",
            "đừng",
            "không được",
            "khong duoc",
            "禁止使用",
            "不得使用",
        )
        for keyword in keywords:
            start = 0
            while True:
                index = normalized.find(keyword, start)
                if index < 0:
                    break
                prefix = normalized[max(0, index - 16):index].lower()
                if not any(marker in prefix for marker in negation_markers):
                    return True
                start = index + len(keyword)
        return False

    def _generate_variants_for_script(
        self,
        script_index: int,
        context: Optional[Dict[str, Any]],
        target_country: str,
        target_language: str,
        product_type: str,
        anchor_card_json: Dict[str, Any],
        final_strategy_json: Dict[str, Any],
        expression_plan_json: Dict[str, Any],
        persona_style_emotion_pack_json: Dict[str, Any],
        original_script_json: Dict[str, Any],
        product_selling_note: str,
        run_id: Optional[int],
        record_id: str,
        product_code: str,
        stage_durations: Dict[str, float],
        llm_client: OriginalScriptLLMClient,
        on_variant_generated: Optional[Any] = None,
        script_level_repair_instruction: str = "",
    ) -> Dict[str, Any]:
        content_id_context = context or {
            "product_code": product_code,
            "target_country": target_country,
            "target_language": target_language,
            "product_type": product_type,
            "top_category": "",
            "product_selling_note": product_selling_note,
        }
        source_script_id = self._source_script_id(script_index)
        source_strategy_id = str(
            final_strategy_json.get("final_strategy_id")
            or final_strategy_json.get("strategy_id")
            or ""
        ).strip()
        strategy_id = str(final_strategy_json.get("strategy_id", "") or "").strip().upper()
        direction_allowed_pool = self._build_direction_allowed_pool(final_strategy_json)
        variant_layers = self._build_variant_layers(
            target_country=target_country,
            product_type=product_type,
            final_strategy_json=final_strategy_json,
            original_script_json=original_script_json,
        )
        variant_profiles = self._build_variant_profiles(strategy_id)
        variants: List[Dict[str, Any]] = []
        for batch_index, variant_ids in enumerate(VARIANT_BATCHES, 1):
            batch_profiles = [
                profile for profile in variant_profiles if str(profile.get("variant_id")) in set(variant_ids)
            ]
            stage_name = f"variant_s{script_index}_batch_{batch_index}"
            validator = lambda data, expected_variant_ids=variant_ids: validate_variant_payload(  # noqa: E731
                data,
                expected_count=len(expected_variant_ids),
                expected_variant_ids=expected_variant_ids,
            )
            prompt = build_variant_prompt(
                target_country=target_country,
                target_language=target_language,
                product_type=product_type,
                anchor_card_json=anchor_card_json,
                final_strategy_json=final_strategy_json,
                expression_plan_json=expression_plan_json,
                persona_style_emotion_pack_json=persona_style_emotion_pack_json,
                original_script_json=original_script_json,
                source_script_id=source_script_id,
                source_strategy_id=source_strategy_id,
                direction_allowed_pool_json=direction_allowed_pool,
                person_variant_layer_json=variant_layers["person_variant_layer"],
                outfit_variant_layer_json=variant_layers["outfit_variant_layer"],
                scene_variant_layer_json=variant_layers["scene_variant_layer"],
                emotion_variant_layer_json=variant_layers["emotion_variant_layer"],
                variant_plan_json=batch_profiles,
                product_selling_note=product_selling_note,
                repair_instruction=script_level_repair_instruction,
                variant_ids=variant_ids,
                type_guard_json=content_id_context.get("type_guard"),
            )
            try:
                batch_payload = self._run_stage(
                    stage_name,
                    prompt,
                    run_id=run_id,
                    record_id=record_id,
                    product_code=product_code,
                    input_context={
                        "target_country": target_country,
                        "target_language": target_language,
                        "product_type": product_type,
                        "product_selling_note": product_selling_note,
                        "anchor_card": anchor_card_json,
                        "final_strategy": final_strategy_json,
                        "expression_plan": expression_plan_json,
                        "original_script": original_script_json,
                        "source_script_id": source_script_id,
                        "source_strategy_id": source_strategy_id,
                        "direction_allowed_pool": direction_allowed_pool,
                        "variant_layers": variant_layers,
                        "variant_plan": batch_profiles,
                        "variant_ids": variant_ids,
                    },
                    stage_durations=stage_durations,
                    llm_client=llm_client,
                    validator=validator,
                    max_tokens=5200,
                )
                self._validate_variant_batch_semantics(
                    batch_payload,
                    expected_profiles=batch_profiles,
                    source_script_id=source_script_id,
                    source_strategy_id=source_strategy_id,
                    final_strategy_json=final_strategy_json,
                    target_country=target_country,
                )
            except JsonStageError as exc:
                repair_instruction = (
                    "请严格补齐 variants 数组中的必填字段。"
                    "每个变体都要包含 variant_id、variant_no、variant_strength、variant_focus、"
                    "source_script_id、source_strategy_id、strategy_id、strategy_name、primary_selling_point、"
                    "final_video_script_prompt。"
                    "在 debug_mode=true 时，还必须返回 internal_variant_state，且其中要包含 inherited_core_items、"
                    "changed_structure_fields、changed_feeling_layers、main_change、secondary_change、difference_summary、"
                    "coverage、proof_blueprint、person_variant_layer、outfit_variant_layer、scene_variant_layer、"
                    "emotion_variant_layer、consistency_checks。"
                    "final_video_script_prompt 只保留视频整体设定、分镜执行、统一风格边界、负向限制。"
                    "必须严格继承 source_script_id、source_strategy_id、strategy_id、strategy_name、primary_selling_point，"
                    f"其中 source_script_id 必须等于 {source_script_id}，"
                    f"source_strategy_id 必须等于 {source_strategy_id}，"
                    f"strategy_id 必须严格等于 {strategy_id}，"
                    "不要把 strategy_id 写成 source_strategy_id，不要写 Final_S1/Final_S2/Final_S3/Final_S4，"
                    "也不要写 S1_V1/S4_V2 这类变体化命名。"
                    "不得跨方向，不得改主卖点，不得破坏生活语境。"
                    "请明显缩短字段文本，避免长段落；final_video_script_prompt 默认使用 4-6 个镜头，"
                    "单镜头尽量控制在 1-3 秒。字幕字段允许为空字符串。"
                    f"本次只返回 {', '.join(variant_ids)}。"
                )
                try:
                    batch_payload = self._run_stage(
                        stage_name,
                        build_variant_prompt(
                            target_country=target_country,
                            target_language=target_language,
                            product_type=product_type,
                            anchor_card_json=anchor_card_json,
                            final_strategy_json=final_strategy_json,
                            expression_plan_json=expression_plan_json,
                            persona_style_emotion_pack_json=persona_style_emotion_pack_json,
                            original_script_json=original_script_json,
                            source_script_id=source_script_id,
                            source_strategy_id=source_strategy_id,
                            direction_allowed_pool_json=direction_allowed_pool,
                            person_variant_layer_json=variant_layers["person_variant_layer"],
                            outfit_variant_layer_json=variant_layers["outfit_variant_layer"],
                            scene_variant_layer_json=variant_layers["scene_variant_layer"],
                            emotion_variant_layer_json=variant_layers["emotion_variant_layer"],
                            variant_plan_json=batch_profiles,
                            product_selling_note=product_selling_note,
                            repair_instruction=f"{repair_instruction}\n上次失败原因：{exc}",
                            variant_ids=variant_ids,
                            type_guard_json=content_id_context.get("type_guard"),
                        ),
                        run_id=run_id,
                        record_id=record_id,
                        product_code=product_code,
                        input_context={
                            "target_country": target_country,
                            "target_language": target_language,
                            "product_type": product_type,
                            "product_selling_note": product_selling_note,
                            "anchor_card": anchor_card_json,
                            "final_strategy": final_strategy_json,
                            "expression_plan": expression_plan_json,
                            "original_script": original_script_json,
                            "source_script_id": source_script_id,
                            "source_strategy_id": source_strategy_id,
                            "direction_allowed_pool": direction_allowed_pool,
                            "variant_layers": variant_layers,
                            "variant_plan": batch_profiles,
                            "variant_ids": variant_ids,
                            "repair_reason": str(exc),
                        },
                        stage_durations=stage_durations,
                        llm_client=llm_client,
                        validator=validator,
                        max_tokens=5200,
                    )
                    self._validate_variant_batch_semantics(
                        batch_payload,
                        expected_profiles=batch_profiles,
                        source_script_id=source_script_id,
                        source_strategy_id=source_strategy_id,
                        final_strategy_json=final_strategy_json,
                        target_country=target_country,
                    )
                except JsonStageError as retry_exc:
                    # 优先保可执行变体正文；debug/internal 层不稳时，自动退化为仅保留生产层输出。
                    batch_payload = self._run_stage(
                        stage_name,
                        build_variant_prompt(
                            target_country=target_country,
                            target_language=target_language,
                            product_type=product_type,
                            anchor_card_json=anchor_card_json,
                            final_strategy_json=final_strategy_json,
                            expression_plan_json=expression_plan_json,
                            persona_style_emotion_pack_json=persona_style_emotion_pack_json,
                            original_script_json=original_script_json,
                            source_script_id=source_script_id,
                            source_strategy_id=source_strategy_id,
                            direction_allowed_pool_json=direction_allowed_pool,
                            person_variant_layer_json=variant_layers["person_variant_layer"],
                            outfit_variant_layer_json=variant_layers["outfit_variant_layer"],
                            scene_variant_layer_json=variant_layers["scene_variant_layer"],
                            emotion_variant_layer_json=variant_layers["emotion_variant_layer"],
                            variant_plan_json=batch_profiles,
                            product_selling_note=product_selling_note,
                            repair_instruction=(
                                "最后一次降级修复：允许省略 internal_variant_state，"
                                "只要 variants 的 final_video_script_prompt 完整、可执行、"
                                "不跨方向、不改主卖点、不丢关键锚点即可。"
                                f"其中 source_script_id 必须等于 {source_script_id}，"
                                f"source_strategy_id 必须等于 {source_strategy_id}，"
                                f"strategy_id 必须严格等于 {strategy_id}。"
                                "绝对不要输出解释文字，只返回合法 JSON。"
                                f"\n前两次失败原因：{exc} / {retry_exc}"
                            ),
                            variant_ids=variant_ids,
                            type_guard_json=content_id_context.get("type_guard"),
                        ),
                        run_id=run_id,
                        record_id=record_id,
                        product_code=product_code,
                        input_context={
                            "target_country": target_country,
                            "target_language": target_language,
                            "product_type": product_type,
                            "product_selling_note": product_selling_note,
                            "anchor_card": anchor_card_json,
                            "final_strategy": final_strategy_json,
                            "expression_plan": expression_plan_json,
                            "original_script": original_script_json,
                            "source_script_id": source_script_id,
                            "source_strategy_id": source_strategy_id,
                            "direction_allowed_pool": direction_allowed_pool,
                            "variant_layers": variant_layers,
                            "variant_plan": batch_profiles,
                            "variant_ids": variant_ids,
                            "repair_reason": f"{exc}; {retry_exc}",
                        },
                        stage_durations=stage_durations,
                        llm_client=llm_client,
                        validator=validator,
                        max_tokens=5200,
                    )
                    self._validate_variant_batch_semantics(
                        batch_payload,
                        expected_profiles=batch_profiles,
                        source_script_id=source_script_id,
                        source_strategy_id=source_strategy_id,
                        final_strategy_json=final_strategy_json,
                        target_country=target_country,
                    )

            batch_variants = batch_payload.get("variants", []) or []
            for variant in batch_variants:
                if isinstance(variant, dict):
                    self._ensure_variant_content_id(
                        variant,
                        context=content_id_context,
                        script_index=script_index,
                        record_id=record_id,
                    )
            variants.extend(batch_variants)
            if on_variant_generated:
                for variant in batch_variants:
                    on_variant_generated(variant, list(variants))

        final_payload = {"variant_count": len(variants), "variants": variants}
        validate_variant_payload(final_payload, expected_count=len(variants))
        self._ensure_variant_spoken_structure(final_payload, script_index)
        self._validate_variant_batch_semantics(
            final_payload,
            expected_profiles=variant_profiles,
            source_script_id=source_script_id,
            source_strategy_id=source_strategy_id,
            final_strategy_json=final_strategy_json,
            target_country=target_country,
        )
        if is_sea_market(target_country):
            home_count = self._count_variant_home_share_scripts(variants)
            if home_count < 3:
                if script_level_repair_instruction:
                    raise JsonStageError(
                        f"脚本{script_index}变体家中自然分享场景数量不足，当前 {home_count} 个，至少需要 3 个"
                    )
                repair_instruction = (
                    "请重做本条正式脚本的全部轻变体，确保 5 个变体里至少 3 个保留达人家中自然分享场景。"
                    "优先把 V1、V2、V3 固定在家中自然分享场景，并通过卧室镜前、衣柜/穿衣区、梳妆台/桌边、"
                    "床边、客厅自然走动区域、窗边自然光区域、玄关镜前这些子场景拉开差异。"
                    "每条 home-share 变体的 final_video_script_prompt.video_setup.scene_final 都必须直接写出明确家中子场景，"
                    "不要只写“生活化真实场景”“真实场景”“日常场景”这类泛标签。"
                    "不要牺牲商品主线，也不要把家中场景都拍成同一个机位。"
                )
                return self._generate_variants_for_script(
                    script_index=script_index,
                    context=content_id_context,
                    target_country=target_country,
                    target_language=target_language,
                    product_type=product_type,
                    anchor_card_json=anchor_card_json,
                    final_strategy_json=final_strategy_json,
                    expression_plan_json=expression_plan_json,
                    persona_style_emotion_pack_json=persona_style_emotion_pack_json,
                    original_script_json=original_script_json,
                    product_selling_note=product_selling_note,
                    run_id=run_id,
                    record_id=record_id,
                    product_code=product_code,
                    stage_durations=stage_durations,
                    llm_client=llm_client,
                    on_variant_generated=on_variant_generated,
                    script_level_repair_instruction=repair_instruction,
                )
        return final_payload

    def _ensure_script_spoken_structure(self, script_json: Dict[str, Any], stage_name: str) -> None:
        self._ensure_spoken_task_coverage(
            storyboard=script_json.get("storyboard", []) or [],
            label=stage_name,
            require_hook=False,
        )

    def _ensure_variant_spoken_structure(self, payload: Dict[str, Any], script_index: int) -> None:
        variants = payload.get("variants", []) or []
        for variant in variants:
            variant_id = str(variant.get("variant_id", "") or "")
            internal_state = variant.get("internal_variant_state") or {}
            coverage_values = internal_state.get("coverage", []) or []
            coverage = {str(item).strip() for item in coverage_values if str(item).strip()}
            if coverage != {"hook", "proof", "decision"}:
                inferred = self._infer_variant_coverage(variant.get("final_video_script_prompt") or {})
                coverage.update(inferred)
                if isinstance(internal_state, dict):
                    internal_state["coverage"] = sorted(coverage) if coverage else ["hook", "proof", "decision"]
            label = f"variant_s{script_index}_{variant_id}"
            if "hook" not in coverage:
                raise JsonStageError(f"{label}: 缺少 hook 覆盖")
            if "proof" not in coverage:
                raise JsonStageError(f"{label}: 中段承接不足，缺少 proof")
            if "decision" not in coverage:
                raise JsonStageError(f"{label}: 决策收束不足，缺少 decision")

    @staticmethod
    def _infer_variant_coverage(final_prompt: Dict[str, Any]) -> set:
        coverage = set()
        shots = final_prompt.get("shot_execution", []) or []
        if not isinstance(shots, list) or not shots:
            return coverage

        first_shot = shots[0] if isinstance(shots[0], dict) else {}
        last_shot = shots[-1] if isinstance(shots[-1], dict) else {}
        middle_shots = [shot for shot in shots[1:-1] if isinstance(shot, dict)]

        def _has_signal(shot: Dict[str, Any], keys: List[str]) -> bool:
            return any(str(shot.get(key, "") or "").strip() for key in keys)

        if _has_signal(first_shot, ["visual", "product_focus", "voiceover"]):
            coverage.add("hook")
        if any(_has_signal(shot, ["visual", "product_focus", "voiceover"]) for shot in middle_shots):
            coverage.add("proof")
        if _has_signal(last_shot, ["voiceover", "product_focus", "visual"]):
            coverage.add("decision")

        if len(shots) >= 4:
            coverage.update({"hook", "proof", "decision"})
        return coverage

    @staticmethod
    def _build_video_prompt_fallback_from_script(script_json: Dict[str, Any]) -> Dict[str, Any]:
        positioning = script_json.get("script_positioning", {}) or {}
        constraints = script_json.get("execution_constraints", {}) or {}
        storyboard = script_json.get("storyboard", []) or []

        title = str(positioning.get("script_title", "") or "").strip()
        direction = str(positioning.get("direction_type", "") or "").strip()
        focus = str(positioning.get("core_primary_selling_point", "") or "").strip()
        setup_parts = [part for part in [title, direction, focus] if part]
        video_setup = "；".join(setup_parts) if setup_parts else "原生自然短视频脚本"

        boundary_parts: List[str] = []
        for key in (
            "visual_style",
            "person_constraints",
            "styling_constraints",
            "tone_completion_constraints",
            "scene_constraints",
            "emotion_progression_constraints",
            "camera_focus",
            "product_priority_rule",
            "realism_principle",
            "product_priority_principle",
        ):
            value = str(constraints.get(key, "") or "").strip()
            if value:
                boundary_parts.append(value)
        for item in (script_json.get("negative_constraints", []) or [])[:4]:
            text = str(item or "").strip()
            if text:
                boundary_parts.append(text)
        execution_boundary = "；".join(boundary_parts) if boundary_parts else "原生自然，商品保持画面主角"
        normalized_boundary = re.sub(r"\s+", "", execution_boundary)

        shot_execution: List[Dict[str, Any]] = []
        for index, shot in enumerate(storyboard[:6], 1):
            if not isinstance(shot, dict):
                continue
            style_note = str(shot.get("style_note", "") or "").strip()
            if style_note and re.sub(r"\s+", "", style_note) in normalized_boundary:
                style_note = ""
            shot_execution.append(
                {
                    "shot_no": int(shot.get("shot_no")) if isinstance(shot.get("shot_no"), int) else index,
                    "duration": str(shot.get("duration", "") or "").strip(),
                    "shot_content": str(shot.get("shot_content", "") or "").strip(),
                    "voiceover_text_target_language": str(shot.get("voiceover_text_target_language", "") or "").strip(),
                    "voiceover_text_zh": str(shot.get("voiceover_text_zh", "") or "").strip(),
                    "spoken_line_task": str(shot.get("spoken_line_task", "") or "").strip(),
                    "person_action": str(shot.get("person_action", "") or "").strip(),
                    "style_note": style_note,
                }
            )

        return {
            "video_setup": video_setup,
            "shot_execution": shot_execution,
            "execution_boundary": execution_boundary,
        }

    def _ensure_spoken_task_coverage(
        self,
        storyboard: List[Dict[str, Any]],
        label: str,
        require_hook: bool,
    ) -> None:
        coverage = self._collect_spoken_task_coverage(storyboard)
        if require_hook and "hook" not in coverage:
            raise JsonStageError(f"{label}: 缺少 spoken_line_task=hook")
        if "proof" not in coverage:
            raise JsonStageError(f"{label}: 中段承接不足，缺少 proof 或 proof+decision")
        if "decision" not in coverage:
            raise JsonStageError(f"{label}: 决策收束不足，缺少 decision 或 proof+decision")

    @staticmethod
    def _collect_spoken_task_coverage(storyboard: List[Dict[str, Any]]) -> set:
        coverage = set()
        for item in storyboard:
            if not isinstance(item, dict):
                continue
            task = str(item.get("spoken_line_task", "") or "").strip()
            if task == "proof+decision":
                coverage.add("proof")
                coverage.add("decision")
            elif task:
                coverage.add(task)
        return coverage

    def _validate_variant_inputs(self, record: TaskRecord, context: Dict[str, Any]) -> None:
        missing_fields: List[str] = []
        product_code = context.get("product_code", "")

        required_json_fields = ["anchor_card_json"]
        for group in self._selected_variant_groups():
            required_json_fields.extend(
                [group["final_field"], group["exp_field"]]
            )

        for logical_name in required_json_fields:
            if not self._has_json_field(record.fields, logical_name) and not self._has_stage_fallback(
                record_id=record.record_id,
                product_code=product_code,
                logical_name=logical_name,
            ):
                missing_fields.append(self.mapping.get(logical_name) or logical_name)

        for group in self._selected_variant_groups():
            json_logical_name = group["script_json_field"]
            logical_name = f"script_s{group['script_index']}"
            has_text = bool(normalize_cell_value(record.fields.get(self.mapping.get(logical_name))))
            has_json = self._has_json_field(record.fields, json_logical_name)
            has_fallback = self._has_stage_fallback(
                record_id=record.record_id,
                product_code=product_code,
                logical_name=json_logical_name,
            )
            if not has_text and not has_json and not has_fallback:
                missing_fields.append(
                    f"{self.mapping.get(logical_name) or logical_name} / {self.mapping.get(json_logical_name) or json_logical_name}"
                )

        if missing_fields:
            raise ValidationError(f"脚本变体依赖缺失: {', '.join(missing_fields)}")

    def _validate_variant_output_fields(self, selected_script_indexes: Optional[set] = None) -> None:
        missing_fields: List[str] = []
        for group in self._selected_variant_groups(selected_script_indexes=selected_script_indexes):
            for logical_name in group["render_fields"]:
                if not self.mapping.get(logical_name):
                    missing_fields.append(logical_name)
        if missing_fields:
            raise ValidationError(f"表格缺少脚本变体回写字段: {', '.join(missing_fields)}")

    def _selected_variant_groups(self, selected_script_indexes: Optional[set] = None) -> List[Dict[str, Any]]:
        selected_indexes = (
            set(self.variant_script_indexes)
            if self.variant_script_indexes
            else set(DEFAULT_VARIANT_SCRIPT_INDEXES)
        )
        if selected_script_indexes is not None:
            selected_indexes &= set(selected_script_indexes)
        return [
            group
            for group in VARIANT_GROUPS
            if int(group.get("script_index", 0)) in selected_indexes
        ]

    def _has_stage_fallback(
        self,
        record_id: str,
        product_code: str,
        logical_name: str,
    ) -> bool:
        fallback_stage_name = VARIANT_STAGE_LOOKUP.get(logical_name)
        if logical_name.startswith("script_s") and logical_name.endswith("_json"):
            fallback_stage_name = logical_name.replace("_json", "")
        if not fallback_stage_name:
            return False
        stage_output = self.storage.get_latest_stage_output_json(
            record_id=record_id,
            product_code=product_code,
            stage_name=fallback_stage_name,
        )
        return bool(stage_output and isinstance(stage_output, dict))

    def _load_resume_stage_output(
        self,
        record_id: str,
        product_code: str,
        input_hash: str,
        stage_name: str,
        validator: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        if not input_hash:
            return None
        stage_output = self.storage.get_latest_stage_output_json_for_input(
            record_id=record_id,
            product_code=product_code,
            input_hash=input_hash,
            stage_name=stage_name,
        )
        if stage_output and isinstance(stage_output, dict):
            if validator is not None:
                try:
                    validator(stage_output)
                except Exception:
                    return None
            return stage_output
        return None

    def _count_variant_home_share_scripts(self, variants: List[Dict[str, Any]]) -> int:
        count = 0
        for variant in variants:
            if isinstance(variant, dict):
                self._normalize_variant_sea_scene(variant)
            final_prompt = variant.get("final_video_script_prompt", {}) or {}
            video_setup = final_prompt.get("video_setup", {}) or {}
            internal_state = variant.get("internal_variant_state", {}) or {}
            scene_layer = internal_state.get("scene_variant_layer", {}) or {} if isinstance(internal_state, dict) else {}
            scene_candidates = [
                str(" ".join(str(v) for v in video_setup.values()) or ""),
                str(scene_layer.get("scene_subspace", "") or "") if isinstance(scene_layer, dict) else "",
                str(scene_layer.get("scene_function_moment", "") or "") if isinstance(scene_layer, dict) else "",
                str(scene_layer.get("scene_domain_base", "") or "") if isinstance(scene_layer, dict) else "",
            ]
            for shot in final_prompt.get("shot_execution", []) or []:
                if isinstance(shot, dict):
                    scene_candidates.append(str(shot.get("visual", "") or ""))
                    scene_candidates.append(str(shot.get("person_action", "") or ""))
            if any(is_home_share_scene(text) for text in scene_candidates if text):
                count += 1
        return count

    def _load_script_json_for_variants(
        self,
        record: TaskRecord,
        logical_name: str,
        fallback_stage_name: str,
        product_code: str,
    ) -> Dict[str, Any]:
        field_name = self.mapping.get(logical_name)
        raw_value = record.fields.get(field_name) if field_name else None
        if raw_value:
            try:
                value = self._load_json_field(record.fields, logical_name)
                if isinstance(value, dict):
                    return value
            except Exception:
                pass

        stage_output = self.storage.get_latest_stage_output_json(
            record_id=record.record_id,
            product_code=product_code,
            stage_name=fallback_stage_name,
        )
        if stage_output and isinstance(stage_output, dict):
            return stage_output

        raise ValidationError(
            f"缺少正式脚本 JSON 上下文: {field_name or logical_name} / fallback {fallback_stage_name}"
        )

    def _load_variant_context_json(
        self,
        record: TaskRecord,
        logical_name: str,
        fallback_stage_name: str,
        product_code: str,
    ) -> Dict[str, Any]:
        field_name = self.mapping.get(logical_name)
        raw_value = record.fields.get(field_name) if field_name else None
        if raw_value:
            try:
                value = self._load_json_field(record.fields, logical_name)
                if isinstance(value, dict):
                    return value
            except Exception:
                pass

        stage_output = self.storage.get_latest_stage_output_json(
            record_id=record.record_id,
            product_code=product_code,
            stage_name=fallback_stage_name,
        )
        if not stage_output or not isinstance(stage_output, dict):
            raise ValidationError(
                f"缺少变体生成依赖: {field_name or logical_name} / fallback {fallback_stage_name}"
            )

        if logical_name == "anchor_card_json":
            return stage_output

        if logical_name.startswith("final_s"):
            strategy_id = logical_name.replace("final_", "").replace("_json", "").upper()
            return self._find_strategy(stage_output, strategy_id)

        if logical_name.startswith("exp_s"):
            return stage_output

        return stage_output

    def _selected_script_indexes_for_rerun(self) -> set:
        return set(self.script_rerun_indexes or DEFAULT_VARIANT_SCRIPT_INDEXES)

    @staticmethod
    def _review_failure_reason(review_json: Dict[str, Any]) -> str:
        if "major_issues" in review_json:
            parts = [str(item or "").strip() for item in (review_json.get("major_issues") or []) if str(item or "").strip()]
            if parts:
                return "；".join(parts[:3])
        issues = review_json.get("issues", []) or []
        parts: List[str] = []
        for issue in issues[:3]:
            if not isinstance(issue, dict):
                continue
            category = str(issue.get("category", "") or "").strip()
            location = str(issue.get("location", "") or "").strip()
            message = str(issue.get("message", "") or "").strip()
            part = " ".join(item for item in [category, location, message] if item).strip()
            if part:
                parts.append(part)
        return "；".join(parts)

    def _load_existing_script_bundle(
        self,
        record: TaskRecord,
        context: Dict[str, Any],
        script_index: int,
    ) -> Dict[str, Any]:
        product_code = context["product_code"]
        script_json = self._load_script_json_for_variants(
            record=record,
            logical_name=f"script_s{script_index}_json",
            fallback_stage_name=f"script_s{script_index}",
            product_code=product_code,
        )
        review_json = self._load_variant_context_json(
            record=record,
            logical_name=f"review_s{script_index}_json",
            fallback_stage_name=f"script_review_s{script_index}",
            product_code=product_code,
        )
        passed_review = self._review_flag_is_pass(review_json)
        failure_reason = "" if passed_review else self._review_failure_reason(review_json)

        if passed_review:
            try:
                video_prompt_json = self._load_variant_context_json(
                    record=record,
                    logical_name=f"video_prompt_s{script_index}_json",
                    fallback_stage_name=f"video_prompt_s{script_index}",
                    product_code=product_code,
                )
            except Exception:
                video_prompt_json = self._build_video_prompt_fallback_from_script(script_json)
        else:
            video_prompt_json = {
                "status": "SKIPPED_DUE_TO_QC_FAIL",
                "reason": failure_reason or "脚本未通过质检",
            }

        self._ensure_script_content_id(
            script_json,
            context=context,
            script_index=script_index,
            record_id=record.record_id,
            video_prompt_json=video_prompt_json if passed_review else None,
        )
        rendered_script = render_script(script_json)
        if not passed_review:
            rendered_script = render_failed_script(rendered_script, review_json)
            rendered_video_prompt = render_skipped_video_prompt(failure_reason or "脚本未通过质检")
        else:
            rendered_video_prompt = render_video_prompt(video_prompt_json)

        return {
            "script_json": script_json,
            "review_json": review_json,
            "video_prompt_json": video_prompt_json,
            "rendered_script": rendered_script,
            "rendered_video_prompt": rendered_video_prompt,
            "passed_review": passed_review,
            "failure_reason": failure_reason,
        }

    def _dump_json(self, value: Dict[str, Any]) -> str:
        return json.dumps(value, ensure_ascii=False, indent=2)

    def _write_partial_fields(
        self,
        record_id: str,
        extra_values: Dict[str, Any],
    ) -> None:
        payload = build_update_payload(self.mapping, extra_values)
        if payload:
            self.client.update_record_fields(record_id, payload)

    def _write_strategy_progress_preview(
        self,
        record_id: str,
        anchor_card: Dict[str, Any],
        strategies_by_index: Dict[int, Dict[str, Any]],
    ) -> None:
        if self.mapping.get("anchor_card_json") or self.mapping.get("final_s1_json"):
            return

        preview_values: Dict[str, Any] = {}
        for script_index, strategy in strategies_by_index.items():
            logical_name = f"script_s{script_index}"
            if not self.mapping.get(logical_name):
                continue
            preview_values[logical_name] = render_strategy_progress_preview(
                anchor_card,
                strategy,
                include_anchor_card=script_index == 1,
            )
        self._write_partial_fields(record_id, preview_values)

    def _build_script_bundle_output_values(
        self,
        script_index: int,
        script_bundle: Dict[str, Any],
        anchor_card: Dict[str, Any],
        final_s1: Dict[str, Any],
        final_s2: Dict[str, Any],
        final_s3: Dict[str, Any],
        final_s4: Dict[str, Any],
    ) -> Dict[str, Any]:
        values = {
            f"script_s{script_index}_json": self._dump_json(script_bundle["script_json"]),
            f"review_s{script_index}_json": self._dump_json(script_bundle["review_json"]),
            f"script_s{script_index}": script_bundle["rendered_script"],
            f"video_prompt_s{script_index}_json": self._dump_json(script_bundle["video_prompt_json"]),
            f"video_prompt_s{script_index}": script_bundle["rendered_video_prompt"],
            "output_summary": build_summary(anchor_card, final_s1, final_s2, final_s3, final_s4),
            "last_run_at": self._now_string(),
            "error_message": "",
        }
        return values

    def _write_update(
        self,
        record_id: str,
        logs: List[str],
        stage_durations: Dict[str, float],
        status: str,
        extra_values: Optional[Dict[str, Any]] = None,
    ) -> None:
        updates = {"status": status}
        if extra_values:
            updates.update(extra_values)
        payload = build_update_payload(self.mapping, updates)
        self.client.update_record_fields(record_id, payload)
        logs.append(f"状态更新：{status}")
        self._flush_log_fields(record_id, logs, stage_durations)

    def _flush_log_fields(
        self,
        record_id: str,
        logs: List[str],
        stage_durations: Dict[str, float],
    ) -> None:
        payload = build_update_payload(
            self.mapping,
            {
                "execution_log": "\n".join(logs[-30:]),
                "stage_durations": json.dumps(stage_durations, ensure_ascii=False, indent=2),
            },
        )
        if payload:
            self.client.update_record_fields(record_id, payload)

    def _mark_failed(
        self,
        record_id: str,
        status: str,
        error_message: str,
        logs: List[str],
        stage_durations: Dict[str, float],
    ) -> None:
        try:
            self._write_update(
                record_id,
                logs,
                stage_durations,
                status=self._runtime_status(status),
                extra_values={"error_message": error_message[:2000]},
            )
        except Exception as exc:
            print(f"⚠️ 回写失败状态也失败了: {exc}")

    def _sync_auto_publish_metadata(self, record_id: str, logs: List[str]) -> None:
        try:
            written = sync_record_to_auto_publish_db(
                client=self.client,
                record_id=record_id,
                metadata_db_path=self.auto_publish_metadata_db_path,
            )
            logs.append(f"自动发布主数据已同步: {written} 条")
        except Exception as exc:
            logs.append(f"自动发布主数据同步失败: {exc}")

    def _runtime_status(self, preferred_status: str) -> str:
        status_field_name = self.mapping.get("status")
        if status_field_name == "任务状态":
            if preferred_status in {
                STATUS_PENDING_VARIANTS,
                STATUS_PENDING_RERUN_VARIANTS,
                STATUS_RUNNING_VARIANTS,
                STATUS_DONE_VARIANTS,
                STATUS_DONE_WITH_QC_WARNINGS,
                STATUS_FAILED_VARIANT_INPUT,
                STATUS_FAILED_VARIANT_MODEL,
                STATUS_FAILED_VARIANT_JSON,
                STATUS_FAILED_VARIANT_WRITE,
                STATUS_FAILED_INTERRUPTED,
            }:
                return preferred_status
            if preferred_status == STATUS_DONE:
                return STATUS_DONE
            if preferred_status in {
                STATUS_FAILED_INPUT,
                STATUS_FAILED_MODEL,
                STATUS_FAILED_JSON,
                STATUS_FAILED_WRITE,
                STATUS_FAILED_INTERRUPTED,
            }:
                return STATUS_FAILED_LEGACY
            return STATUS_RUNNING_LEGACY
        return preferred_status

    @staticmethod
    def _normalize_product_type(product_type: str, top_category: str = "") -> str:
        text = str(product_type or "").strip()
        lowered = text.lower()
        if product_type in CLOTHING_PRODUCT_TYPES:
            return "服装"
        if product_type in ACCESSORY_PRODUCT_TYPES:
            return "配饰"
        if any(keyword in text for keyword in ACCESSORY_PRODUCT_TYPE_KEYWORDS if not keyword.isascii()):
            return "配饰"
        if any(keyword in lowered for keyword in ACCESSORY_PRODUCT_TYPE_KEYWORDS if keyword.isascii()):
            return "配饰"
        if top_category == "配饰" and product_type:
            return "配饰"
        return product_type

    @staticmethod
    def _normalize_top_category(top_category: str, normalized_product_type: str) -> str:
        if top_category in ALLOWED_TOP_CATEGORIES:
            return top_category
        if normalized_product_type == "配饰":
            return "配饰"
        return "女装"

    def _can_rerun_script_only(self, record: TaskRecord, product_code: str) -> bool:
        needed = [
            "anchor_card_json",
            "opening_strategy_json",
            "styling_plan_json",
            "final_s1_json",
            "final_s2_json",
            "final_s3_json",
            "final_s4_json",
            "exp_s1_json",
            "exp_s2_json",
            "exp_s3_json",
            "exp_s4_json",
        ]
        return all(
            self._has_json_field(record.fields, logical_name)
            or self._has_stage_fallback(record_id=record.record_id, product_code=product_code, logical_name=logical_name)
            for logical_name in needed
        )

    def _has_json_field(self, fields: Dict[str, Any], logical_name: str) -> bool:
        field_name = self.mapping.get(logical_name)
        raw_value = fields.get(field_name) if field_name else None
        if not raw_value:
            return False
        try:
            self._load_json_field(fields, logical_name)
            return True
        except Exception:
            return False

    def _load_json_field(self, fields: Dict[str, Any], logical_name: str) -> Dict[str, Any]:
        field_name = self.mapping.get(logical_name)
        if not field_name:
            raise ValidationError(f"表格缺少字段: {logical_name}")
        raw_value = fields.get(field_name)
        if isinstance(raw_value, dict):
            return raw_value
        if not raw_value:
            raise ValidationError(f"字段为空: {field_name}")
        if isinstance(raw_value, str):
            value = parse_json_text(raw_value)
            if not isinstance(value, dict):
                raise JsonStageError(f"字段 {field_name} 不是 JSON 对象")
            return value
        raise JsonStageError(f"无法解析字段 {field_name}")

    @staticmethod
    def _now_string() -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")


def validate_required_fields(mapping: Dict[str, Optional[str]]) -> None:
    required = {
        "status": "状态字段",
        "product_images": "产品图片字段",
        "target_country": "目标国家字段",
        "target_language": "目标语言字段",
        "product_type": "产品类型字段",
        "script_s1": "脚本_S1 字段",
        "script_s2": "脚本_S2 字段",
        "script_s3": "脚本_S3 字段",
        "script_s4": "脚本_S4 字段",
    }
    missing = [label for key, label in required.items() if not mapping.get(key)]
    if missing:
        raise ValidationError(f"表格缺少必需字段: {', '.join(missing)}")


def load_pending_records(
    client: FeishuBitableClient,
    mapping: Dict[str, Optional[str]],
    limit: Optional[int] = None,
) -> List[TaskRecord]:
    records = client.list_records(page_size=100)
    status_field = mapping["status"]
    pending = [
        record
        for record in records
        if normalize_cell_value(record.fields.get(status_field)) in PENDING_STATUSES
    ]
    if limit:
        pending = pending[:limit]
    return pending


def load_selected_records(
    client: FeishuBitableClient,
    mapping: Dict[str, Optional[str]],
    product_code: Optional[str] = None,
    record_id: Optional[str] = None,
    task_nos: Optional[List[str]] = None,
    force_status: Optional[str] = None,
) -> List[TaskRecord]:
    records = client.list_records(page_size=100)
    selected: List[TaskRecord] = []
    task_no_set = {str(item or "").strip() for item in (task_nos or []) if str(item or "").strip()}

    for record in records:
        matched = False
        if record_id and record.record_id == record_id:
            matched = True

        if product_code:
            current_product_code = normalize_cell_value(
                record.fields.get(mapping.get("product_code"))
            )
            if current_product_code == product_code:
                matched = True

        if task_no_set:
            current_task_no = normalize_cell_value(
                record.fields.get(mapping.get("task_no"))
            )
            if current_task_no in task_no_set:
                matched = True

        if not matched:
            continue

        if force_status and mapping.get("status"):
            fields = dict(record.fields)
            fields[mapping["status"]] = force_status
            selected.append(TaskRecord(record_id=record.record_id, fields=fields))
        else:
            selected.append(record)

    return selected
