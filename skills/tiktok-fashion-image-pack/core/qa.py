#!/usr/bin/env python3
"""Visual QA for generated likeU product main images."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from vision_client import VisionJSONClient


def build_visual_qa_prompt(product_truth: Dict[str, Any]) -> str:
    return f"""
你是 TikTok Shop 女装主图质检员。你会看到两张图：
1. 供应商原图/商品事实参考
2. AI 生成主图

请检查生成图是否可作为商品主图上线。只输出合法 JSON。

Product Truth:
{product_truth}

输出字段：
{{
  "result": "通过|轻微问题可用|不通过|需人工复核",
  "score": 0.0,
  "issues": [],
  "must_retry": false,
  "summary": ""
}}

质检重点：
- 商品颜色是否明显偏差。
- 材质是否变了，例如麂皮变皮衣、PU皮变棉布。
- 领型、门襟、扣子/拉链、口袋数量和位置是否变了。
- 袖口、下摆、衣长、版型是否变了。
- 是否新增帽子、毛领、腰带、刺绣、额外口袋等原图没有的元素。
- 是否出现明显不售卖配件，让买家误以为附带。
- 模特手脸是否异常，商品是否被遮挡。
- 是否有价格、促销、大段文案或廉价感。

判定标准：
- 关键结构/材质错误：不通过，must_retry=true。
- 轻微背景或调色问题但商品真实可信：轻微问题可用。
- 原图本身不清楚，无法判断：需人工复核。
""".strip()


def qa_generated_image(
    *,
    source_image_paths: List[str],
    generated_image_path: str,
    product_truth: Dict[str, Any],
    client: Optional[VisionJSONClient] = None,
) -> Dict[str, Any]:
    vision = client or VisionJSONClient()
    images = source_image_paths[:1] + [generated_image_path]
    result = vision.call_json(build_visual_qa_prompt(product_truth), image_paths=images, max_output_tokens=1800)
    if not isinstance(result, dict):
        raise ValueError("QA response must be a JSON object")
    return normalize_qa_result(result)


def normalize_qa_result(raw: Dict[str, Any]) -> Dict[str, Any]:
    result = str(raw.get("result") or "").strip()
    if result not in {"通过", "轻微问题可用", "不通过", "需人工复核"}:
        result = "需人工复核"
    try:
        score = float(raw.get("score"))
    except (TypeError, ValueError):
        score = 0.0
    issues = raw.get("issues")
    if isinstance(issues, list):
        normalized_issues = [str(item).strip() for item in issues if str(item).strip()]
    else:
        normalized_issues = [str(issues).strip()] if str(issues or "").strip() else []
    return {
        "result": result,
        "score": max(0.0, min(1.0, score)),
        "issues": normalized_issues,
        "must_retry": bool(raw.get("must_retry")) or result == "不通过",
        "summary": str(raw.get("summary") or "").strip(),
    }


def skipped_qa(reason: str = "QA skipped") -> Dict[str, Any]:
    return {
        "result": "未质检",
        "score": 0.0,
        "issues": [reason],
        "must_retry": False,
        "summary": reason,
    }
