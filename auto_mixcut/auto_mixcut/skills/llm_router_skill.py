from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any, Dict

from auto_mixcut.adapters.vision_json import VisionJSONClient
from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext


class LLMRouterSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx
        self._vision_client: VisionJSONClient | None = None

    def call(self, call_type: str, payload: Dict[str, Any], product_id: str = "", segment_id: str = "", asset_id: str = "") -> Result:
        route = dict(ROUTES.get(call_type, {"model_tier": "medium_vision", "model_name": "mock-medium-vision"}))
        if not self.ctx.settings.mock_llm and "vision" in route["model_tier"]:
            route["model_name"] = os.environ.get("AUTO_MIXCUT_VISION_MODEL", "gpt-5.5")
        self._ensure_cache_table()
        input_hash = self._input_hash(call_type, payload, product_id, segment_id, asset_id, route)
        cache_hit = False
        started = time.time()
        try:
            cached = None if self.ctx.settings.mock_llm else self._read_cache(input_hash)
            if cached is not None:
                response = cached
                cache_hit = True
            else:
                response = self._mock(call_type, payload) if self.ctx.settings.mock_llm else self._real(call_type, payload, product_id, segment_id, asset_id)
                if not self.ctx.settings.mock_llm:
                    self._write_cache(input_hash, call_type, response, payload, product_id, segment_id, asset_id, route)
            status = "success"
            error_message = ""
        except Exception as exc:
            response = {"error": str(exc)}
            status = "failed"
            error_message = str(exc)
        latency_ms = int((time.time() - started) * 1000)
        self.ctx.repo.upsert(
            "llm_calls",
            "call_id",
            {
                "call_id": new_id("LLM"),
                "product_id": product_id,
                "asset_id": asset_id,
                "segment_id": segment_id,
                "call_type": call_type,
                "model_tier": route["model_tier"],
                "model_name": route["model_name"],
                "prompt_version": payload.get("prompt_version", "v1.0"),
                "input_hash": input_hash,
                "cache_hit": int(cache_hit),
                "token_input": 0,
                "token_output": 0,
                "image_count": payload.get("image_count", 0),
                "estimated_cost": 0,
                "latency_ms": latency_ms,
                "result_status": status,
            },
        )
        if status != "success":
            return Result.fail("LLM_CALL_FAILED", error_message, {"call_type": call_type, "segment_id": segment_id})
        return Result.ok({"route": route, "response": response, "cache_hit": cache_hit})

    def _ensure_cache_table(self) -> None:
        with self.ctx.repo.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS llm_cache (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  cache_key TEXT NOT NULL UNIQUE,
                  call_type TEXT,
                  product_id TEXT,
                  asset_id TEXT,
                  segment_id TEXT,
                  model_tier TEXT,
                  model_name TEXT,
                  prompt_version TEXT,
                  input_hash TEXT,
                  response_json TEXT,
                  created_at TEXT,
                  updated_at TEXT
                )
                """
            )

    def _input_hash(self, call_type: str, payload: Dict[str, Any], product_id: str, segment_id: str, asset_id: str, route: Dict[str, str]) -> str:
        material = {
            "call_type": call_type,
            "payload": payload,
            "product_id": product_id,
            "segment_id": segment_id,
            "asset_id": asset_id,
            "model_tier": route["model_tier"],
            "model_name": route["model_name"],
            "prompt_version": payload.get("prompt_version", "v1.0"),
            "product_anchor": _product_anchor(self.ctx, product_id),
            "frame_hashes": _frame_hashes(self.ctx, segment_id),
            "image_path_hashes": _image_path_hashes(payload.get("image_paths") or []),
        }
        return hashlib.sha256(json.dumps(material, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")).hexdigest()

    def _read_cache(self, input_hash: str) -> Dict[str, Any] | None:
        row = self.ctx.repo.get("llm_cache", "cache_key", input_hash)
        if not row:
            return None
        data = row.get("response_json")
        if isinstance(data, dict):
            return data
        if isinstance(data, str):
            return json.loads(data)
        return None

    def _write_cache(self, input_hash: str, call_type: str, response: Dict[str, Any], payload: Dict[str, Any], product_id: str, segment_id: str, asset_id: str, route: Dict[str, str]) -> None:
        self.ctx.repo.upsert(
            "llm_cache",
            "cache_key",
            {
                "cache_key": input_hash,
                "call_type": call_type,
                "product_id": product_id,
                "asset_id": asset_id,
                "segment_id": segment_id,
                "model_tier": route["model_tier"],
                "model_name": route["model_name"],
                "prompt_version": payload.get("prompt_version", "v1.0"),
                "input_hash": input_hash,
                "response_json": response,
            },
        )

    def _real(self, call_type: str, payload: Dict[str, Any], product_id: str, segment_id: str, asset_id: str) -> Dict[str, Any]:
        if call_type == "segment_tagging_default":
            segment = self.ctx.repo.get("segments", "segment_id", segment_id) or {}
            product = self.ctx.repo.get("products", "product_id", product_id) or {}
            asset = self.ctx.repo.get("assets", "asset_id", asset_id) or {}
            image_paths = _frame_paths(self.ctx, segment_id, max_count=6)
            if not image_paths:
                raise RuntimeError(f"no sampled frames for segment {segment_id}")
            prompt = _segment_tagging_prompt(product, asset, segment)
            data = self._client().call_json(prompt, image_paths, max_output_tokens=1600)
            return _normalize_segment_tag(data)
        if call_type == "ai_generated_consistency_check":
            image_paths = _frame_paths(self.ctx, segment_id, max_count=9)
            if not image_paths:
                raise RuntimeError(f"no sampled frames for segment {segment_id}")
            data = self._client().call_json(_consistency_prompt(), image_paths, max_output_tokens=900)
            return _normalize_consistency(data)
        if call_type == "watermark_detection":
            image_paths = payload.get("image_paths") or []
            data = self._client().call_json(_watermark_prompt(), image_paths, max_output_tokens=700)
            return data if isinstance(data, dict) else {"has_watermark": "unknown", "confidence": "low"}
        if call_type == "ai_anchor_check":
            segment = self.ctx.repo.get("segments", "segment_id", segment_id) or {}
            product = self.ctx.repo.get("products", "product_id", product_id) or {}
            image_paths = _frame_paths(self.ctx, segment_id, max_count=6)
            if not image_paths:
                raise RuntimeError(f"no sampled frames for segment {segment_id}")
            prompt = _ai_anchor_check_prompt(product, segment)
            data = self._client().call_json(prompt, image_paths, max_output_tokens=1200)
            return _normalize_anchor_check(data)
        if call_type == "segment_prompt_refinement":
            anchor_json = str(payload.get("anchor_json") or "{}")
            segment_type = str(payload.get("segment_type") or "")
            segment_type_cn = str(payload.get("segment_type_cn") or "")
            category = str(payload.get("category") or "")
            data = self._client().call_json(_segment_prompt_refinement_prompt(anchor_json, segment_type, segment_type_cn, category), [], max_output_tokens=800)
            return _normalize_prompt_refinement(data)
        return self._mock(call_type, payload)

    def _client(self) -> VisionJSONClient:
        if self._vision_client is None:
            self._vision_client = VisionJSONClient()
        return self._vision_client

    def _mock(self, call_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if call_type == "segment_tagging_default":
            index = int(payload.get("index", 0))
            roles = ["hero", "detail", "result", "scene", "ending"]
            role = roles[index % len(roles)]
            secondary = {
                "hero": ["detail"],
                "detail": [],
                "result": [],
                "scene": ["ending"],
                "ending": [],
            }[role]
            return {
                "primary_shot_role": role,
                "secondary_roles": secondary,
                "product_visibility": "high" if role in {"hero", "detail", "result"} else "medium",
                "hook_strength": "strong" if role in {"hero", "result"} else "medium",
                "mixcut_usability": "yes",
                "risk_level": "low",
                "confidence": "high",
                "needs_human_review": False,
                "reason": f"mock tag selected {role} from sampled frames",
            }
        if call_type == "ai_generated_consistency_check":
            return {"frame_consistency_score": 92, "frame_consistency_status": "pass", "frame_consistency_reason": "mock frames remain visually consistent"}
        if call_type == "watermark_detection":
            return {"has_watermark": "no", "confidence": "medium"}
        if call_type == "ai_anchor_check":
            segment_type = str(payload.get("segment_type") or "")
            if segment_type in {"detail_atmosphere", "tryon_result"}:
                return {
                    "anchor_match_level": "soft_pass",
                    "product_category_correct": True,
                    "core_visual_points_status": {"product_shape": "likely", "key_features": "unclear"},
                    "forbidden_mismatch_detected": False,
                    "distortion_risk": "medium",
                    "allowed_core_roles": [],
                    "allowed_soft_roles": ["scene", "ending"],
                    "needs_human_review": False,
                    "reason": "AI生成商品类目一致，但细节不够清晰",
                }
            return {
                "anchor_match_level": "strict_pass",
                "product_category_correct": True,
                "core_visual_points_status": {},
                "forbidden_mismatch_detected": False,
                "distortion_risk": "low",
                "allowed_core_roles": ["hero", "detail", "result"],
                "allowed_soft_roles": ["scene", "ending"],
                "needs_human_review": False,
                "reason": "AI生成素材关键视觉点通过",
            }
        if call_type == "segment_prompt_generation":
            return {"prompt": "mock generated prompt", "prompt_version": "v1.0"}
        if call_type == "segment_prompt_refinement":
            segment_type = str(payload.get("segment_type") or "home_lifestyle")
            segment_type_cn = str(payload.get("segment_type_cn") or "居家生活场景")
            return {
                "visual_description": f"一段3秒竖屏短视频，展现{segment_type_cn}氛围，商品在画面中自然呈现。",
                "key_anchor_points": ["商品主体形状和材质可见", "佩戴/使用场景自然"],
                "scene_description": f"适合{segment_type_cn}的日常场景，自然光，真实生活感。",
                "forbidden_items": ["字幕", "文字", "logo", "水印", "广告感", "多镜头"],
            }
        return {"ok": True}


ROUTES = {
    "product_anchor_generation": {"model_tier": "medium_vision", "model_name": "mock-medium-vision"},
    "watermark_detection": {"model_tier": "medium_vision", "model_name": "mock-medium-vision"},
    "segment_tagging_default": {"model_tier": "medium_vision", "model_name": "mock-medium-vision"},
    "product_anchor_check": {"model_tier": "medium_vision", "model_name": "mock-medium-vision"},
    "ai_anchor_check": {"model_tier": "medium_vision", "model_name": "mock-medium-vision"},
    "ai_generated_consistency_check": {"model_tier": "medium_vision", "model_name": "mock-medium-vision"},
    "segment_prompt_generation": {"model_tier": "medium_vision", "model_name": "mock-medium-vision"},
    "segment_prompt_refinement": {"model_tier": "medium_text", "model_name": "gpt-5.5"},
    "risk_escalation": {"model_tier": "high_vision", "model_name": "mock-high-vision"},
    "final_video_qc": {"model_tier": "medium_vision", "model_name": "mock-medium-vision"},
    "golden_benchmark": {"model_tier": "medium_vision", "model_name": "mock-medium-vision"},
}


def _frame_paths(ctx: SkillContext, segment_id: str, max_count: int) -> list[str]:
    rows = ctx.repo.list_where("segment_frames", "segment_id=? ORDER BY id DESC", (segment_id,))
    selected = list(reversed(rows[:max_count]))
    paths = []
    for row in selected:
        obj = ctx.repo.get("oss_objects", "object_id", row.get("oss_object_id"))
        if not obj:
            continue
        path = ctx.settings.oss_root / obj["object_key"]
        if path.exists() and path.stat().st_size > 1024:
            paths.append(str(path))
    return paths


def _product_anchor(ctx: SkillContext, product_id: str) -> Any:
    product = ctx.repo.get("products", "product_id", product_id) or {}
    return product.get("product_anchor_json") or {}


def _frame_hashes(ctx: SkillContext, segment_id: str) -> list[str]:
    rows = ctx.repo.list_where("segment_frames", "segment_id=? ORDER BY id DESC", (segment_id,))
    hashes = []
    for row in reversed(rows[:10]):
        obj = ctx.repo.get("oss_objects", "object_id", row.get("oss_object_id"))
        if obj:
            hashes.append(str(obj.get("file_hash") or obj.get("object_key") or ""))
    return hashes


def _image_path_hashes(paths: list[str]) -> list[str]:
    hashes = []
    for path in paths:
        try:
            with open(path, "rb") as fh:
                hashes.append(hashlib.sha256(fh.read()).hexdigest())
        except OSError:
            hashes.append(str(path))
    return hashes


def _segment_tagging_prompt(product: dict, asset: dict, segment: dict) -> str:
    anchor = product.get("product_anchor_json") or {}
    return f"""
请根据连续抽帧判断这个 TikTok Shop 商品短视频片段的混剪用途。

商品信息：
- 商品ID：{product.get('product_id')}
- 商品名称：{product.get('product_name')}
- 市场：{product.get('market')}
- 类目：{product.get('category')}
- 商品锚点：{json.dumps(anchor, ensure_ascii=False)}

素材信息：
- source_type：{asset.get('source_type')}
- source_trust_level：{asset.get('source_trust_level')}
- product_binding_type：{asset.get('product_binding_type')}
- segment_id：{segment.get('segment_id')}

请只返回 JSON，不要 markdown，不要解释。字段和值必须严格使用下面枚举：
{{
  "primary_shot_role": "hero|detail|result|scene|ending|unusable",
  "secondary_roles": ["hero|detail|result|scene|ending"],
  "product_visibility": "high|medium|low",
  "hook_strength": "strong|medium|weak",
  "mixcut_usability": "yes|needs_processing|no",
  "risk_level": "low|medium|high",
  "confidence": "high|medium|low",
  "needs_human_review": true|false,
  "reason": "中文，简短说明判断依据"
}}

判断标准：
- hero：商品主体清楚、首屏能吸引人，适合开头。
- detail：商品材质、结构、局部细节清楚。
- result：佩戴/使用后效果清楚。
- scene：氛围、生活方式、背景场景，商品可以不是强主体。
- ending：适合收尾、定格、轻氛围。
- unusable：黑屏、严重模糊、商品不可见、明显错品、风险内容、水印/UI遮挡严重。
- 如果商品与锚点不确定、AI生成漂移、画面含平台水印/账号UI/明显搬运痕迹，应提高 risk_level 或 needs_human_review。
""".strip()


def _normalize_segment_tag(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("segment tag response is not object")
    roles = {"hero", "detail", "result", "scene", "ending", "unusable"}
    vis = {"high", "medium", "low"}
    hooks = {"strong", "medium", "weak"}
    usability = {"yes", "needs_processing", "no"}
    risk = {"low", "medium", "high"}
    conf = {"high", "medium", "low"}
    primary = _enum(data.get("primary_shot_role"), roles, "unusable")
    secondary = [_enum(v, roles - {"unusable"}, "") for v in (data.get("secondary_roles") or [])]
    secondary = [v for v in secondary if v]
    return {
        "primary_shot_role": primary,
        "secondary_roles": secondary[:3],
        "product_visibility": _enum(data.get("product_visibility"), vis, "low"),
        "hook_strength": _enum(data.get("hook_strength"), hooks, "weak"),
        "mixcut_usability": _enum(data.get("mixcut_usability"), usability, "needs_processing"),
        "risk_level": _enum(data.get("risk_level"), risk, "medium"),
        "confidence": _enum(data.get("confidence"), conf, "low"),
        "needs_human_review": bool(data.get("needs_human_review")),
        "reason": str(data.get("reason") or "").strip()[:500],
    }


def _consistency_prompt() -> str:
    return """
请检查这些连续帧中的商品是否跨帧保持一致，重点看商品形状、结构、关键装饰、数量、材质是否漂移。
只返回 JSON：
{
  "frame_consistency_score": 0-100,
  "frame_consistency_status": "pass|uncertain|fail",
  "frame_consistency_reason": "中文简短原因"
}
""".strip()


def _normalize_consistency(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("consistency response is not object")
    score = float(data.get("frame_consistency_score") or 0)
    status = _enum(data.get("frame_consistency_status"), {"pass", "uncertain", "fail"}, "uncertain")
    return {"frame_consistency_score": max(0, min(100, score)), "frame_consistency_status": status, "frame_consistency_reason": str(data.get("frame_consistency_reason") or "")[:500]}


def _watermark_prompt() -> str:
    return "请判断图片是否包含 TikTok/Douyin logo、平台 UI、用户ID、账号名或明显水印。只返回 JSON：{\"has_watermark\":\"yes|no|unknown\",\"confidence\":\"high|medium|low\",\"watermark_type\":\"TikTok|Douyin|platform_ui|user_id|other|none\",\"reason\":\"中文简短原因\"}"


def _enum(value: Any, allowed: set[str], default: str) -> str:
    text = str(value or "").strip()
    return text if text in allowed else default


def _ai_anchor_check_prompt(product: dict, segment: dict) -> str:
    anchor = product.get("product_anchor_json") or {}
    return f"""
请根据连续抽帧，判断这个 AI 生成的商品片段是否可以用于混剪。

商品信息：
- 商品ID：{product.get('product_id')}
- 商品名称：{product.get('product_name')}
- 类目：{product.get('category')}

商品锚点：
{json.dumps(anchor, ensure_ascii=False)}

片段信息：
- 片段类型：{segment.get('segment_type') or 'unknown'}
- source_type：{segment.get('source_type')}

请只返回 JSON，不要 markdown，不要解释：
{{
  "anchor_match_level": "strict_pass|soft_pass|uncertain|fail",
  "product_category_correct": true|false,
  "core_visual_points_status": {{}},
  "forbidden_mismatch_detected": true|false,
  "forbidden_mismatch_reason": "如有就不匹配，简述原因；否则null",
  "distortion_risk": "low|medium|high",
  "allowed_core_roles": ["hero|detail|result"],
  "allowed_soft_roles": ["scene|ending"],
  "needs_human_review": true|false,
  "reason": "中文简短说明判断依据"
}}

判定标准：
- strict_pass：商品类别正确，关键识别点清楚，不违反 forbidden_mismatch，跨帧一致，可承担 hero/detail/result。
- soft_pass：商品方向大体正确，但细节不足以承担强商品展示，只能用于 scene/ending。
- uncertain：模型不确定，普通商品默认降级 scene/ending，高优先级商品需人工复核。
- fail：商品明显错了（类目错误、结构错误、核心视觉点消失），不能进入混剪。

注意：AI 生成素材容易在细节处失真，请重点关注商品形状、结构、关键装饰是否漂移。
""".strip()


def _normalize_anchor_check(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("anchor check response is not object")
    levels = {"strict_pass", "soft_pass", "uncertain", "fail"}
    roles = {"hero", "detail", "result"}
    soft_roles = {"scene", "ending"}
    core = data.get("allowed_core_roles") or []
    soft = data.get("allowed_soft_roles") or []
    return {
        "anchor_match_level": _enum(data.get("anchor_match_level"), levels, "uncertain"),
        "product_category_correct": bool(data.get("product_category_correct")),
        "core_visual_points_status": data.get("core_visual_points_status") or {},
        "forbidden_mismatch_detected": bool(data.get("forbidden_mismatch_detected")),
        "forbidden_mismatch_reason": str(data.get("forbidden_mismatch_reason") or "") if data.get("forbidden_mismatch_reason") else None,
        "distortion_risk": _enum(data.get("distortion_risk"), {"low", "medium", "high"}, "medium"),
        "allowed_core_roles": [r for r in core if r in roles],
        "allowed_soft_roles": [r for r in soft if r in soft_roles],
        "needs_human_review": bool(data.get("needs_human_review")),
        "reason": str(data.get("reason") or "").strip()[:500],
    }


def _segment_prompt_refinement_prompt(anchor_json: str, segment_type: str, segment_type_cn: str, category: str) -> str:
    return f"""你是一个 TikTok Shop 商品视频 prompt 提炼助手。
请从商品锚点中提取与「{segment_type_cn}」({segment_type}) 片段类型最相关的视觉信息，生成精炼的视频生成 prompt 组件。

商品类目：{category}

商品锚点：
{anchor_json}

请只返回 JSON，不要 markdown，不要解释：
{{
  "visual_description": "一段 3 秒竖屏视频的英文描述，聚焦该片段类型需要的画面内容。如果此片段类型需要在画面中展示商品，必须包含商品关键视觉特征。120 词以内。",
  "key_anchor_points": ["3-5 个用于该片段的关键锚点要求，中文简短描述"],
  "scene_description": "该片段类型的场景和光线描述，英文。30 词以内。",
  "forbidden_items": ["必须禁止出现的元素，如字幕、水印、logo、广告感等，中文"]
}}

提炼原则：
- 如果片段类型是 product_display / handheld_product / detail_atmosphere / tryon_result：必须强调商品核心视觉点（结构、材质、颜色、关键装饰）
- 如果片段类型是 mirror_routine / home_lifestyle / before_go_out / seasonal_scene：强调氛围和生活感，商品可以自然出现但不强制
- 必须根据 category_execution_contract 中的 forbidden_actions 给出禁止项
- visual_description 必须包含 TikTok UGC 风格、9:16 竖屏、单镜头、2-5 秒这些硬约束
""".strip()


def _normalize_prompt_refinement(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("prompt refinement response is not object")
    return {
        "visual_description": str(data.get("visual_description") or "").strip()[:800],
        "key_anchor_points": (data.get("key_anchor_points") or [])[:5],
        "scene_description": str(data.get("scene_description") or "").strip()[:200],
        "forbidden_items": (data.get("forbidden_items") or [])[:10],
    }
