from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from auto_mixcut.core.result import Result

from .context import SkillContext

ORIGINAL_SCRIPT_DB = Path.home() / ".openclaw" / "shared" / "data" / "original_script_generator.sqlite3"


class ProductAnchorSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def draft_anchor(self, product_id: str) -> Result:
        product = self.ctx.repo.get("products", "product_id", product_id)
        if not product:
            return Result.fail("PRODUCT_NOT_FOUND", "product not found", {"product_id": product_id})

        existing_anchor = _load_anchor_from_original_db(product_id)
        if existing_anchor:
            res = self.ctx.repo.update(
                "products", "product_id", product_id,
                {"product_anchor_json": existing_anchor, "anchor_status": "drafted", "anchor_version": "p1_from_original"},
            )
            if not res.success:
                return res
            self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "ANCHOR_DRAFTED"})
            return Result.ok({"product_id": product_id, "source": "original_script_generator", "product_anchor_json": existing_anchor})

        if not self.ctx.settings.mock_llm:
            return self._draft_via_router(product)

        anchor = _build_default_anchor(product)
        res = self.ctx.repo.update(
            "products", "product_id", product_id,
            {"product_anchor_json": anchor, "anchor_status": "drafted", "anchor_version": "v1.0"},
        )
        if not res.success:
            return res
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "ANCHOR_DRAFTED"})
        return Result.ok({"product_id": product_id, "source": "mock", "product_anchor_json": anchor})

    def _draft_via_router(self, product: Dict[str, Any]) -> Result:
        from .llm_router_skill import LLMRouterSkill
        from .llm_prompts import normalize_product_anchor

        router = LLMRouterSkill(self.ctx)
        product_id = product.get("product_id", "")
        product_name = product.get("product_name", "")
        category = product.get("category", "")
        market = product.get("market", "")

        call = router.call(
            "product_anchor_generation",
            {
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "market": market,
                "prompt_version": "v1.0",
                "image_count": 0,
            },
            product_id=product_id,
        )

        if not call.success:
            anchor = _build_default_anchor(product)
            anchor["drafted_by"] = "mock_fallback"
            anchor["fallback_reason"] = call.error.message if call.error else "router call failed"
        else:
            try:
                response = call.data.get("response", {})
                anchor = normalize_product_anchor(response, category, product_name)
            except Exception:
                anchor = _build_default_anchor(product)
                anchor["drafted_by"] = "mock_fallback"
                anchor["fallback_reason"] = "normalization failed"

        res = self.ctx.repo.update(
            "products", "product_id", product_id,
            {"product_anchor_json": anchor, "anchor_status": "drafted", "anchor_version": "v1.0"},
        )
        if not res.success:
            return res
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "ANCHOR_DRAFTED"})
        return Result.ok({"product_id": product_id, "source": "llm_router", "product_anchor_json": anchor})

    def confirm_anchor(self, product_id: str, reviewer: str = "mock_reviewer") -> Result:
        product = self.ctx.repo.get("products", "product_id", product_id)
        if not product or not product.get("product_anchor_json"):
            return Result.fail("ANCHOR_NOT_DRAFTED", "anchor draft is required before confirmation", {"product_id": product_id})
        res = self.ctx.repo.update(
            "products",
            "product_id",
            product_id,
            {
                "anchor_status": "confirmed",
                "anchor_confirmed_at": datetime.utcnow().isoformat(timespec="seconds"),
                "anchor_confirmed_by": reviewer,
            },
        )
        if not res.success:
            return res
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "ANCHOR_CONFIRMED"})
        return Result.ok({"product_id": product_id, "anchor_status": "confirmed"})

    def require_confirmed(self, product_id: str) -> Result:
        product = self.ctx.repo.get("products", "product_id", product_id)
        if product and product.get("anchor_status") == "confirmed":
            return Result.ok({"product_id": product_id})
        return Result.fail("ANCHOR_REQUIRED", "confirmed product anchor is required before material processing", {"product_id": product_id})


def _load_anchor_from_original_db(product_id: str) -> Optional[Dict[str, Any]]:
    if not ORIGINAL_SCRIPT_DB.exists():
        return None
    try:
        conn = sqlite3.connect(str(ORIGINAL_SCRIPT_DB))
        row = conn.execute(
            "SELECT output_json FROM stage_results WHERE stage_name='anchor_card' AND product_code=? AND status='success' ORDER BY stage_result_id DESC LIMIT 1",
            (product_id,),
        ).fetchone()
        conn.close()
        if row:
            return json.loads(row[0])
    except Exception:
        pass
    return None


def _build_default_anchor(product: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "category": product.get("category"),
        "product_subtype": product.get("product_name") or product.get("product_id"),
        "core_visual_points": ["商品主体形状清楚", "主要材质和颜色可见", "能看出佩戴或使用场景"],
        "must_not_change_points": ["商品类型不能变", "核心外观特征必须可见", "不能变成其他类目商品"],
        "forbidden_mismatch": ["其他类目商品", "无关配饰", "画面中没有商品", "款式明显不一致"],
        "strict_roles": ["hero", "detail", "result"],
        "allowed_scene_usage": True,
        "drafted_by": "mock_tier2_vision_zh",
    }
