#!/usr/bin/env python3
"""素材分析执行脚本（Phase 1）：对未分析素材跑 Doubao 分析并写缓存。

用法（包根目录）：
  python3 scripts/run_material_analysis.py --limit 5
  python3 scripts/run_material_analysis.py --note-id <id> --force

环境变量与生产视觉链共用：OPV_PHOTO_VISION_MODEL / _API_URL / _API_KEY。
成本（调用/图片/Token/耗时）记入消费侧台账 analysis_call_log。
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.material_analysis import MaterialAnalyzer, MaterialLedger  # noqa: E402
from services.material_source import MaterialSource  # noqa: E402


def build_client():
    from services.photo_reference_vision import _DoubaoVisionClient

    api_url = os.environ.get("OPV_PHOTO_VISION_API_URL", "").strip()
    api_key = os.environ.get("OPV_PHOTO_VISION_API_KEY", "").strip()
    model = os.environ.get("OPV_PHOTO_VISION_MODEL", "").strip()
    missing = [n for n, v in (
        ("OPV_PHOTO_VISION_API_URL", api_url),
        ("OPV_PHOTO_VISION_API_KEY", api_key),
        ("OPV_PHOTO_VISION_MODEL", model),
    ) if not v]
    if missing:
        sys.exit(f"缺少环境变量：{', '.join(missing)}（与生产视觉链共用配置）")
    return _DoubaoVisionClient(api_url=api_url, api_key=api_key, model=model), model


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=5, help="本轮最多分析篇数")
    ap.add_argument("--note-id", default="", help="只分析指定笔记（可配 --force）")
    ap.add_argument("--force", action="store_true", help="忽略缓存重析")
    ap.add_argument("--batch-size", type=int, default=6)
    ap.add_argument("--db", default="")
    ap.add_argument("--images-root", default="")
    ap.add_argument("--ledger", default="")
    args = ap.parse_args()

    client, model = build_client()
    source = MaterialSource(args.db or None, args.images_root or None)
    ledger = MaterialLedger(args.ledger or None)
    analyzer = MaterialAnalyzer(
        source, ledger, client, model=model, batch_size=args.batch_size)

    if args.note_id:
        outcomes = [analyzer.analyze_note(args.note_id, force=args.force)]
    else:
        outcomes = analyzer.analyze_pending(limit=args.limit, force=args.force)

    ok = fail = cached = 0
    for outcome in outcomes:
        if outcome.error:
            fail += 1
            print(f"  ✗ {outcome.note_id}: {outcome.error}")
        elif outcome.cached:
            cached += 1
            print(f"  = {outcome.note_id}（命中缓存）")
        else:
            ok += 1
            result = outcome.result or {}
            print(f"  ✓ {outcome.note_id}｜{result.get('set_structure')}"
                  f"｜可消费={result.get('consumable')}"
                  f"｜{str(result.get('note_topic') or '')[:36]}")
    print(f"\n本轮：新分析 {ok}，缓存命中 {cached}，失败 {fail}")
    print(f"台账累计成本：{ledger.cost_summary()}")
    ledger.close()


if __name__ == "__main__":
    main()
