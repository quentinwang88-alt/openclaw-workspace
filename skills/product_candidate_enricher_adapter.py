#!/usr/bin/env python3
"""OpenClaw adapter for the product candidate enricher skill."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


SKILLS_PATH = Path(__file__).parent
if str(SKILLS_PATH) not in sys.path:
    sys.path.insert(0, str(SKILLS_PATH))

from base_skill import BaseSkill, register_skill


SKILL_DIR = SKILLS_PATH / "product-candidate-enricher"
MODULE_PATH = SKILL_DIR / "run_pipeline.py"
RUNNER_AVAILABLE = False
RUNNER_IMPORT_ERROR: Optional[str] = None
run_candidate_enrichment = None

try:
    if str(SKILL_DIR) not in sys.path:
        sys.path.insert(0, str(SKILL_DIR))
    spec = importlib.util.spec_from_file_location(
        "product_candidate_enricher_run_pipeline",
        MODULE_PATH,
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"无法从 {MODULE_PATH} 加载模块")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    run_candidate_enrichment = module.run_candidate_enrichment
    RUNNER_AVAILABLE = True
except Exception as exc:
    RUNNER_IMPORT_ERROR = str(exc)


@register_skill
class ProductCandidateEnricherAdapter(BaseSkill):
    @property
    def name(self) -> str:
        return "enrich_product_candidates"

    @property
    def description(self) -> str:
        return (
            "整理飞书商品候选池。"
            "会将预估商品上架时间调整为年-月-日显示格式，计算上架天数，"
            "把商品名称翻译为中文，并对子类目做受控打标。"
        )

    @property
    def json_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "feishu_url": {
                    "type": "string",
                    "description": "要处理的飞书多维表格链接。不传时使用 skill 内置默认链接。",
                },
                "limit": {
                    "type": "integer",
                    "description": "最多处理多少条记录，用于小批量验证。",
                },
                "record_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "只处理指定 record_id 列表。",
                },
                "dry_run": {
                    "type": "boolean",
                    "description": "是否只预览不回写飞书。建议首次执行时先设为 true。",
                },
                "skip_llm": {
                    "type": "boolean",
                    "description": "是否跳过中文翻译和子类目打标，只做日期相关处理。",
                },
                "overwrite_chinese_name": {
                    "type": "boolean",
                    "description": "即使中文名称已有值，也强制重写。",
                },
                "overwrite_subcategory": {
                    "type": "boolean",
                    "description": "即使子类目已有值，也强制重写。",
                },
                "overwrite_listing_days": {
                    "type": "boolean",
                    "description": "是否强制重写上架天数字段。",
                },
                "max_llm_workers": {
                    "type": "integer",
                    "description": "LLM 并发数。默认 8，建议使用 6 到 10。",
                },
                "max_date_workers": {
                    "type": "integer",
                    "description": "只刷新上架天数时的并发数。默认 48，适合快速批量刷新日期结果。",
                },
                "skip_date_format_update": {
                    "type": "boolean",
                    "description": "是否跳过日期字段显示格式更新。",
                },
                "subcategories": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "覆盖默认子类目列表，便于后续扩充。",
                },
            },
            "required": [],
        }

    def execute(self, **kwargs) -> Dict[str, Any]:
        if not RUNNER_AVAILABLE or run_candidate_enrichment is None:
            return {
                "success": False,
                "error": f"skill 加载失败: {RUNNER_IMPORT_ERROR}",
                "data": None,
            }

        record_ids = kwargs.get("record_ids")
        if isinstance(record_ids, str):
            record_ids = [record_ids]

        subcategories = kwargs.get("subcategories")
        if isinstance(subcategories, str):
            subcategories = [item.strip() for item in subcategories.split(",") if item.strip()]

        return run_candidate_enrichment(
            feishu_url=kwargs.get("feishu_url") or module.DEFAULT_FEISHU_URL,
            limit=kwargs.get("limit"),
            record_ids=record_ids,
            dry_run=kwargs.get("dry_run", False),
            skip_llm=kwargs.get("skip_llm", False),
            skip_date_format_update=kwargs.get("skip_date_format_update", False),
            overwrite_chinese_name=kwargs.get("overwrite_chinese_name", False),
            overwrite_subcategory=kwargs.get("overwrite_subcategory", False),
            overwrite_listing_days=kwargs.get("overwrite_listing_days", False),
            max_llm_workers=kwargs.get("max_llm_workers", 8),
            max_date_workers=kwargs.get("max_date_workers", 48),
            subcategories=subcategories,
        )
