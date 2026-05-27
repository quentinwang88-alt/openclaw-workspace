#!/usr/bin/env python3
"""Feedback-specific QA: checks whether generated fix images actually resolved the human feedback issues."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

SKILL_DIR = Path(__file__).resolve().parents[1]
CORE_DIR = SKILL_DIR / "core"
if str(CORE_DIR) not in sys.path:
    sys.path.insert(0, str(CORE_DIR))

from vision_client import VisionJSONClient


QA_PROMPT = """你是商品图修正质检员。对照以下人工反馈，检查修正后的图片是否成功解决了每一条问题。

输入图片角色：
- 图片 1：修正后的图片，本次质检对象
- 图片 2：上一版生成图，用于判断未被反馈点名的画面是否被过度改动
- 图片 3 起：原始商品图和原始场景参考图，用于核对产品真实细节

人工反馈（必须修正的问题）：
{issues}

修正模式：{fix_method}

请逐条检查并给出结论。

## 检查要点

1. 逐条对照反馈中的每一个问题点，确认图片中是否已被修正
2. 检查图片 1 的材质、版型、扣子、口袋、领型、袖口、下摆是否与图片 3 起的原始参考一致
3. 如果反馈提到"参考原图"、"参考原始场景图"或"以原图为准"，必须对照图片 3 起核验
4. 如果反馈提到禁止事项（如"不要新增包包"），检查是否有违反
5. 如果反馈提到具体数值（如"4颗扣子"、"五颗扣钮"），检查数量和分布位置是否正确
6. 如果反馈提到左右结构，必须对照图片 3 起判断是否镜像或位置反了
7. 如果反馈提到扣子、口袋、领型、袖口、下摆等结构问题，图片 1 中该结构必须清晰可见；如果被手、袖子、头发、角度遮住导致无法确认，应判为"部分通过"或"不通过"
8. 如果反馈未提到颜色，颜色应优先保持图片 2 的当前售卖颜色；不要因图片 3 起的主推色不同而单独判颜色失败
9. 未被反馈点名的产品细节不应相对图片 2 出现明显变化

只看修正后图片无法确认的细节，不要轻易判"通过"，应标记为"部分通过"或"不通过"并说明原因。

## 输出格式

用 JSON 格式输出：

```json
{{
  "result": "通过" 或 "不通过" 或 "部分通过",
  "score": 0.0 到 1.0 之间的分数,
    "items": [
    {{
      "issue": "反馈中的具体问题描述",
      "category": "结构未修正" 或 "结构被遮挡" 或 "数量不精确" 或 "疑似镜像错误" 或 "颜色跑偏" 或 "风格可用但细节需人工看" 或 "其它",
      "status": "已修正" 或 "未修正" 或 "部分修正",
      "detail": "简短的检查结论"
    }}
  ],
  "summary": "整体质检结论，中文描述"
}}
```

- 如果所有问题都被修正 → result: "通过", score: 1.0
- 如果大部分问题被修正但有少量残留 → result: "部分通过", score: 0.6-0.8
- 如果主要问题未修正 → result: "不通过", score: < 0.5
"""


def qa_feedback_fix(
    *,
    fix_image_path: str,
    previous_image_path: Optional[str] = None,
    product_reference_paths: Optional[List[str]] = None,
    scene_reference_paths: Optional[List[str]] = None,
    issues: str,
    fix_method: str = "局部修正",
    vision_client: Optional[VisionJSONClient] = None,
) -> Dict[str, Any]:
    """Run vision-based QA to check if feedback issues were resolved.

    Returns a dict with: result, score, items, summary
    """
    client = vision_client or VisionJSONClient()
    prompt = QA_PROMPT.format(issues=issues, fix_method=fix_method)
    image_paths = [fix_image_path]
    if previous_image_path:
        image_paths.append(previous_image_path)
    image_paths.extend(product_reference_paths or [])
    image_paths.extend(scene_reference_paths or [])

    try:
        result = client.call_json(
            prompt=prompt,
            image_paths=image_paths,
            max_output_tokens=2000,
        )
        if not isinstance(result, dict):
            return _fallback_qa(issues)
        return {
            "result": str(result.get("result") or "需人工复核"),
            "score": float(result.get("score") or 0.5),
            "items": result.get("items") or [],
            "summary": str(result.get("summary") or ""),
        }
    except Exception as exc:
        return {
            "result": "需人工复核",
            "score": 0.0,
            "items": [],
            "summary": f"反馈质检调用失败: {exc}",
        }


def _fallback_qa(issues: str) -> Dict[str, Any]:
    return {
        "result": "需人工复核",
        "score": 0.5,
        "items": [],
        "summary": "反馈质检返回格式异常，需人工确认",
    }


def format_qa_issues(qa_result: Dict[str, Any]) -> str:
    """Format QA issues into a readable string for Feishu writeback."""
    parts: List[str] = []
    for item in qa_result.get("items") or []:
        status = item.get("status", "")
        if status in ("未修正", "部分修正"):
            issue = item.get("issue", "")
            detail = item.get("detail", "")
            category = item.get("category") or classify_feedback_qa_issue(issue, detail)
            parts.append(f"[{status}][{category}] {issue} | {detail}")
    return "; ".join(parts) if parts else ""


def classify_feedback_qa_issue(issue: str, detail: str) -> str:
    text = f"{issue} {detail}"
    if any(word in text for word in ("多一颗", "少一颗", "数量", "4颗", "五颗", "6颗", "5颗")):
        return "数量不精确"
    if any(word in text for word in ("遮挡", "看不清", "无法确认", "不够清晰")):
        return "结构被遮挡"
    if any(word in text for word in ("镜像", "相反", "左侧", "右侧", "左右")):
        return "疑似镜像错误"
    if any(word in text for word in ("扣子", "扣钮", "口袋", "领型", "袖口", "下摆", "门襟")):
        return "结构未修正"
    if any(word in text for word in ("颜色", "色差", "偏色")):
        return "颜色跑偏"
    if any(word in text for word in ("细节", "人工", "确认")):
        return "风格可用但细节需人工看"
    return "其它"
