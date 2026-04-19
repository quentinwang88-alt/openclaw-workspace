#!/usr/bin/env python3
"""OpenClaw adapter for the standalone hair style review skill."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any, Dict, Optional


SKILLS_PATH = Path(__file__).parent
if str(SKILLS_PATH) not in sys.path:
    sys.path.insert(0, str(SKILLS_PATH))

from base_skill import BaseSkill, register_skill


SKILL_DIR = SKILLS_PATH / "hair-style-review"
MODULE_PATH = SKILL_DIR / "run_pipeline.py"
RUNNER_AVAILABLE = False
RUNNER_IMPORT_ERROR: Optional[str] = None
run_style_analysis_job = None
DEFAULT_FEISHU_URL = None

try:
    if str(SKILL_DIR) not in sys.path:
        sys.path.insert(0, str(SKILL_DIR))
    spec = importlib.util.spec_from_file_location("hair_style_review_run_pipeline", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法从 {MODULE_PATH} 加载模块")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    run_style_analysis_job = module.run_style_analysis_job
    DEFAULT_FEISHU_URL = module.DEFAULT_FEISHU_URL
    RUNNER_AVAILABLE = True
except Exception as exc:
    RUNNER_IMPORT_ERROR = str(exc)


@register_skill
class HairStyleReviewAdapter(BaseSkill):
    @property
    def name(self) -> str:
        return "review_hair_style_candidates"

    @property
    def description(self) -> str:
        return (
            "用于“分析产品风格”“分析备选商品风格”“评估备选商品匹配度”“判断产品是否推荐”等简短指令。"
            "也支持“分析这个表格的产品风格：<飞书链接>”这类带链接指令。"
            "独立执行发饰商品风格分析；有备选字段时只处理被纳入备选的记录，没有备选字段时处理结果为空的记录，回写产品风格、是否推荐和详细原因。"
        )

    @property
    def json_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "feishu_url": {
                    "type": "string",
                    "description": "要处理的飞书多维表格链接。不传时使用默认候选商品表。",
                },
                "record_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "只处理指定 record_id 列表。",
                },
                "limit": {
                    "type": "integer",
                    "description": "限制本次最多处理多少条记录。",
                },
                "dry_run": {
                    "type": "boolean",
                    "description": "是否只预览分析结果，不回写飞书。",
                },
                "force": {
                    "type": "boolean",
                    "description": "是否忽略已有结果，强制重跑。",
                },
            },
            "required": [],
        }

    def execute(self, **kwargs) -> Dict[str, Any]:
        if not RUNNER_AVAILABLE or run_style_analysis_job is None:
            return {
                "success": False,
                "error": f"skill 加载失败: {RUNNER_IMPORT_ERROR}",
                "data": None,
            }
        record_ids = kwargs.get("record_ids")
        if isinstance(record_ids, str):
            record_ids = [record_ids]
        return run_style_analysis_job(
            feishu_url=kwargs.get("feishu_url") or DEFAULT_FEISHU_URL,
            record_ids=record_ids,
            dry_run=kwargs.get("dry_run", False),
            force=kwargs.get("force", False),
            limit=kwargs.get("limit"),
        )
