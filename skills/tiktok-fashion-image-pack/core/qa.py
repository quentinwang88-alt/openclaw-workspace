#!/usr/bin/env python3
"""Visual QA for generated likeU product main images."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from vision_client import VisionJSONClient


def build_visual_qa_prompt(product_truth: Dict[str, Any]) -> str:
    if is_wig(product_truth):
        return build_wig_visual_qa_prompt(product_truth)
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


def build_wig_visual_qa_prompt(product_truth: Dict[str, Any]) -> str:
    product_form = str(product_truth.get("product_form") or "full_wig").strip().lower()
    required_layout = {
        "full_wig": "必须同时有左侧真人正面/3/4佩戴效果和右侧从头顶到发尾的完整背面效果，不能缺少任一视角。",
        "ponytail_piece": "必须同时展示真实马尾佩戴效果和从基座到发尾的完整独立产品；不得生成整顶假发、lace或虚构梳齿/抽绳。",
        "clip_in_extension": "必须同时展示接发后的长度/发量效果和真实接发片；不得生成整顶假发，片数、发夹和发片宽度只能来自原图。",
        "hair_topper": "必须同时展示头顶覆盖佩戴效果和完整发顶片；不得扩展成整顶假发，底网、卡扣和覆盖面积只能来自原图。",
        "unknown": "产品形态未确认时必须以完整商品实物证明为主，不得擅自呈现为整顶假发、马尾、接发片或发顶片。",
    }.get(product_form, "必须按 Product Truth 中确认的产品形态展示佩戴效果和完整商品证明。")
    return f"""
你是墨西哥市场假发/接发产品首图质检员。你会看到供应商原图和 AI 生成首图。
请判断生成图是否符合对应产品形态的首图要求，并保持商品事实。只输出合法 JSON。

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

强制检查：
- 产品形态要求：{required_layout}
- 真人应为自然的成年墨西哥/拉美女性商业美妆形象：健康暖肤色、真实皮肤纹理、清晰眉睫、克制暖调眼妆、暖裸色或玫瑰棕唇色；不能是塑料脸、夸张网红滤镜或欧美奢华棚拍。
- 产品或背面必须从基座/头顶到发尾完整可见，并清楚展示长度、层次、渐变位置和卷度；发尾不得裁切。
- 背景应为干净白色或柔和奶油色，不能是复杂海报。
- 不得出现任何文字、品牌名、Logo、边框、水印、价格、图标或促销元素。
- 主色、发根色、渐变/挑染位置、长度、层次、刘海、分缝、卷度节奏、发尾轮廓必须与原图一致。
- 不得把合成纤维表现成真人发，不得虚构 lace、baby hair、密度、耐热能力、帽网结构或赠品。
- 两个视角必须是同一款同一颜色，佩戴效果与商品证明的发根、渐变、长度、卷度、结构和件数不能互相矛盾。
- 发丝不能出现融化、重复纹理、断裂、左右镜像错位或不自然粘连。

判定：
- 缺少该产品形态要求的任一关键视角、裁切发尾、有文字/Logo/边框、错误产品形态/颜色/卷度/长度/结构/件数：不通过，must_retry=true。
- 原始图片不足以确认发际线或帽网时，不应仅因未展示这些结构而失败；若生成图主动虚构则不通过。
- 仅有轻微自然阴影或不影响商品事实的清洁痕迹：轻微问题可用。
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
    if is_wig(product_truth):
        return build_wig_scene_qa_prompt(product_truth, scene_role)
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


def build_wig_scene_qa_prompt(product_truth: Dict[str, Any], scene_role: str) -> str:
    return f"""
你是 TikTok Shop 墨西哥站假发详情/场景图质检员。你会看到供应商原图和一张 W 槽生成图。
只输出合法 JSON。

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
- 先确认 product_form。整顶假发、马尾、Clip-in接发片和发顶片不得互相转换；unknown 不得擅自套用整顶假发结构。
- 主色、发根色、渐变/挑染位置、长度、密度外观、层次、刘海、分缝、卷度和发尾是否一致。
- W2 是否按产品形态完整展示整顶背面或独立产品全貌；W3 是否只展示有原图依据的发际线/lace/分缝或基座/发片；W4 是否只展示有依据的帽网、梳齿、抽绳、发夹、卡扣和调节带。
- 缺少 hairline/cap/attachment source 时，W3/W4 是否正确使用真实可见细节或侧视图替代，不能虚构结构。
- 是否暗示真人发、耐热、lace 尺寸、密度、帽围、赠品或套装数量等未证实信息。
- 模特是否真实自然，是否出现塑料皮肤、错误发际线、发丝穿脸/穿肩、重复发束或幻想发型。
- 墨西哥平台图包不得出现文字、Logo、边框、水印、价格或宣传图形。

产品形态、件数、关键颜色、长度、卷度、发际线、帽网/固定结构或材质错误：不通过，must_retry=true。
原图缺少关键结构，无法判断且生成图也未虚构：需人工复核或轻微问题可用。
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


def is_wig(product_truth: Dict[str, Any]) -> bool:
    return str(product_truth.get("category") or "").strip().lower() in {"wig", "wigs", "假发", "假髮", "peluca", "pelucas"}
