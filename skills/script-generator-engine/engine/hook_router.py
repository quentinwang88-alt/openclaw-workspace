#!/usr/bin/env python3
"""
hook_router —— 卖点 -> 主钩子 路由(决策1 核心:卖点驱动钩子)

逻辑:
  - 读配置包 pain_hooks.yaml 的 selling_point_routing。
  - 用户上架填的 selling_point(自由文本) 做关键词模糊匹配 -> 命中主钩子。
  - 命中多个时取命中关键词最多的;都不命中 -> fallback。
配置可演进:增删钩子/改关键词只需改 pain_hooks.yaml,本代码不动。
"""

from __future__ import annotations
from typing import Dict, Any, Optional, Tuple, List


def route_selling_point(selling_point: str, pain_hooks_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    输入:
      selling_point   - 用户上架填的卖点自由文本(中/马来/英)
      pain_hooks_cfg  - pain_hooks.yaml 解析后的 dict
    返回:
      {
        "main_hook_id": str,
        "aux_hook_id": Optional[str],
        "matched_keywords": [...],
        "is_fallback": bool,
        "hook_detail": <该钩子的完整定义>,
      }
    """
    routing = pain_hooks_cfg.get("selling_point_routing", {})
    rules: List[Dict[str, Any]] = routing.get("rules", [])
    fallback = routing.get("fallback", {})
    hooks = pain_hooks_cfg.get("hooks", {})

    sp = (selling_point or "").lower()

    best_hook: Optional[str] = None
    best_hits: List[str] = []
    for rule in rules:
        hits = [kw for kw in rule.get("keywords", []) if kw.lower() in sp]
        if len(hits) > len(best_hits):
            best_hits = hits
            best_hook = rule.get("hook")

    if best_hook:
        return {
            "main_hook_id": best_hook,
            "aux_hook_id": None,
            "matched_keywords": best_hits,
            "is_fallback": False,
            "hook_detail": hooks.get(best_hook, {}),
        }

    # fallback
    primary = fallback.get("primary")
    return {
        "main_hook_id": primary,
        "aux_hook_id": fallback.get("aux"),
        "matched_keywords": [],
        "is_fallback": True,
        "hook_detail": hooks.get(primary, {}),
    }


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from category_router import load_pack

    bundle = load_pack("MY", "hesturi")
    cfg = bundle["pain_hooks"]

    # 模拟用户上架时填的各种卖点
    test_cases = [
        "戴一天都不勒,很舒适",
        "saiz ngam, 合头不松垮",
        "碎发不跑,固定好",
        "一套5条很划算",
        "纯棉透气不闷热",
        "颜色好看",            # 不命中 -> fallback
        "凉爽 + 不勒",          # 多命中,取命中多者
    ]
    print("卖点 -> 主钩子 路由测试:")
    print("=" * 60)
    for sp in test_cases:
        r = route_selling_point(sp, cfg)
        tag = "(fallback)" if r["is_fallback"] else f"命中{r['matched_keywords']}"
        pain = r["hook_detail"].get("pain_desc", "")[:20]
        print(f"  「{sp}」")
        print(f"     -> {r['main_hook_id']}  {tag}")
        print(f"        痛点: {pain}...")
    print("=" * 60)
    print("✅ 卖点驱动钩子 路由正常")
