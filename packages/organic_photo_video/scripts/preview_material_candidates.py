#!/usr/bin/env python3
"""素材候选预览（Phase 1）：查看素材与缓存状态，按主题预演程序初筛。

用法（包根目录）：
  python3 scripts/preview_material_candidates.py                # 素材总览
  python3 scripts/preview_material_candidates.py --theme 一衣多穿 --product-category 开衫
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.material_adapter import ProductBrief, narrow_candidates  # noqa: E402
from services.material_analysis import ANALYSIS_VERSION, MaterialLedger  # noqa: E402
from services.material_source import MaterialSource  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--theme", default="")
    ap.add_argument("--product-category", default="")
    ap.add_argument("--limit", type=int, default=8)
    ap.add_argument("--db", default="")
    ap.add_argument("--images-root", default="")
    ap.add_argument("--ledger", default="")
    ap.add_argument("--model", default="")
    args = ap.parse_args()

    source = MaterialSource(args.db or None, args.images_root or None)
    ledger = MaterialLedger(args.ledger or None)
    model = args.model or os.environ.get("OPV_PHOTO_VISION_MODEL", "").strip() or "preview"
    packages = source.list_packages(require_complete=True)
    print(f"完整素材：{len(packages)} 篇")

    analyses = {}
    for package in packages:
        cached = ledger.get_cached_analysis(
            package.version_fingerprint, model, ANALYSIS_VERSION)
        if cached is not None:
            analyses[package.note_id] = cached
    print(f"已有分析缓存：{len(analyses)} 篇（model={model}）")

    product = ProductBrief("preview", args.product_category) if args.product_category else None
    recent = ledger.recent_note_ids()
    candidates = narrow_candidates(
        packages, analyses, theme=args.theme, product=product,
        recent_note_ids=recent, limit=args.limit)

    if not candidates:
        print("（无可用候选：先跑 run_material_analysis.py 生成分析缓存）")
    else:
        print(f"\n初筛候选（主题={args.theme or '—'}，商品={args.product_category or '—'}）：")
        for cand in candidates:
            print(f"  {cand.score:5.2f}  {cand.note_id}  {cand.title[:32]}")
            for reason in cand.reasons[:3]:
                print(f"         · {reason}")
    ledger.close()


if __name__ == "__main__":
    main()
