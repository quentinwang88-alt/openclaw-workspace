#!/usr/bin/env python3
"""Visual QA for generated likeU product main images."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from vision_client import VisionJSONClient


def build_visual_qa_prompt(product_truth: Dict[str, Any]) -> str:
    if is_hair_accessory(product_truth):
        return build_hair_accessory_visual_qa_prompt(product_truth)
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


def build_hair_accessory_visual_qa_prompt(product_truth: Dict[str, Any]) -> str:
    return f"""
你是 TikTok Shop 发饰主图质检员。你会看到两张图：
1. 供应商原图/商品事实参考
2. AI 生成主图

请检查生成图是否可作为发饰商品主图上线。只输出合法 JSON。

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
- 发饰颜色是否明显偏差。
- 材质是否变了，例如缎面变塑料、亚克力变金属、珍珠/水钻元素被乱加。
- 尺寸比例是否可信，是否被夸张放大或缩小。
- 固定结构是否变了，例如鲨鱼夹齿、边夹、发箍、弹力发圈、蝴蝶结夹结构。
- 装饰元素是否变了，例如新增/减少蝴蝶结、珍珠、水钻、花朵、图案。
- 是否编造套装数量、额外颜色或额外件数。
- 是否出现不售卖的耳环、项链、化妆品等，让买家误以为附带。
- 人脸/手/头发是否明显 AI，发饰是否被遮挡或不容易看清。
- 是否有价格、促销、大段文案或廉价感。

判定标准：
- 关键颜色/材质/结构/数量错误：不通过，must_retry=true。
- 轻微背景或调色问题但发饰真实可信：轻微问题可用。
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


def build_scene_qa_prompt(product_truth: Dict[str, Any], scene_role: str) -> str:
    if is_hair_accessory(product_truth):
        return build_hair_accessory_scene_qa_prompt(product_truth, scene_role)
    return f"""
你是 TikTok Shop 女装场景图质检员。你会看到两类图：
1. 供应商原图/商品事实参考
2. AI 生成场景图

请检查生成图是否可作为商品场景图上线。场景可以更生活化，但商品事实不能变。只输出合法 JSON。

Scene role: {scene_role}
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
- 商品颜色、材质、衣长、版型、领型、门襟、口袋、袖口、下摆是否和原图一致。
- 是否新增帽子、毛领、腰带、刺绣、额外口袋、拉链/扣子等原图没有的结构。
- 如果是纯产品图生成的试穿图，人物是否过度完美、太像 AI、美颜过重；优先接受弱露脸/半身/侧身的自然试穿。
- 即使原图有真人，S1/S2/S5/S6 也不应生成过于完美的正脸甜笑模特照；弱脸、低头、侧脸、手机遮脸、局部裁切通常更真实。
- 场景道具是否让买家误以为随商品附赠。
- 商品是否占画面主体，是否被手、包、杯子、头发等遮挡关键结构。
- 是否出现价格、促销、大段文案或廉价感。

判定标准：
- 关键结构/材质/颜色错误：不通过，must_retry=true。
- 人物略 AI 但商品准确、可上线：轻微问题可用，并说明问题。
- 人脸明显像 AI 精修模特、正脸过度完美且削弱商品真实感：需人工复核；如果同时商品结构也漂移，则不通过。
- 场景不错但商品被遮挡明显：需人工复核或不通过。
""".strip()


def build_hair_accessory_scene_qa_prompt(product_truth: Dict[str, Any], scene_role: str) -> str:
    return f"""
你是 TikTok Shop 发饰场景图质检员。你会看到两类图：
1. 供应商原图/商品事实参考
2. AI 生成场景图

请检查生成图是否可作为发饰商品场景图上线。场景可以更生活化，但商品事实不能变。只输出合法 JSON。

Scene role: {scene_role}
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
- 发饰颜色、材质、大小比例、佩戴位置、固定结构、装饰元素是否和原图一致。
- 是否新增未观察到的珍珠、水钻、蝴蝶结、花朵、logo、IP 图案、额外颜色或额外件数。
- 多色场景是否只使用观察到的颜色，且不同颜色仍是同一款结构。
- 如果是纯产品图生成的佩戴图，人物是否过度完美、太像 AI；优先接受弱露脸、侧后脑、手持、局部头发近景。
- 耳环、项链、化妆品、梳子等道具是否让买家误以为随商品附赠。
- 发饰是否占画面主体，是否被头发、手、背景遮挡到无法识别。
- 是否出现价格、促销、大段文案或廉价感。

判定标准：
- 关键结构/材质/颜色/数量错误：不通过，must_retry=true。
- 人物略 AI 但发饰准确、可上线：轻微问题可用，并说明问题。
- 人脸明显像 AI 精修模特、正脸过度完美且削弱商品真实感：需人工复核；如果同时发饰事实也漂移，则不通过。
- 场景不错但发饰被遮挡明显：需人工复核或不通过。
""".strip()


def qa_scene_image(
    *,
    source_image_paths: List[str],
    generated_image_path: str,
    product_truth: Dict[str, Any],
    scene_role: str,
    client: Optional[VisionJSONClient] = None,
) -> Dict[str, Any]:
    vision = client or VisionJSONClient()
    images = source_image_paths[:2] + [generated_image_path]
    result = vision.call_json(
        build_scene_qa_prompt(product_truth, scene_role),
        image_paths=images,
        max_output_tokens=1800,
    )
    if not isinstance(result, dict):
        raise ValueError("Scene QA response must be a JSON object")
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


def is_hair_accessory(product_truth: Dict[str, Any]) -> bool:
    return str(product_truth.get("category") or "").strip().lower() in {"hair_accessory", "hair_accessories", "发饰"}
