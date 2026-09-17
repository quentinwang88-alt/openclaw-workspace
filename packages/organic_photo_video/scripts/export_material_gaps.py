#!/usr/bin/env python3
"""导出素材需求清单（消费收敛方案 §7：缺口是异步采集需求）。

合并两类信号：
1. material_gaps 台账（近窗口内 no_material / no_thermal_match 等原因 +
   detail 里的主题/品类线索）；
2. 池子存量：按查询族主题统计 v3 可用候选数，低于 min_usable_pool 的族
   进入需求（库存足够即停止扩量，不无限采集）。

输出 JSON（采集脚本消费）：
  [{"family": "...", "queries": [...], "reason": "...", "usable_now": N}]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
LAB_CONFIG = WORKSPACE_ROOT / "labs" / "xhs-material-lab" / "config" / "query_families.json"
for value in (str(WORKSPACE_ROOT), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()


def load_families(path: Path = LAB_CONFIG):
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return payload.get("families") or []


def count_usable_by_theme(ledger, source, model: str, analysis_version: str):
    """按 lab 主题统计 v3 缓存且「至少一个用途可用」的候选数。"""
    counts: dict = {}
    for package in source.list_packages():
        analysis = ledger.get_cached_analysis(
            package.version_fingerprint, model, analysis_version)
        if not analysis:
            continue
        usability = analysis.get("purpose_usability")
        if isinstance(usability, dict) and usability:
            flags = [bool((usability.get(n) or {}).get("usable"))
                     for n in ("outfit", "visual", "narrative")
                     if isinstance(usability.get(n), dict)]
            if flags and not any(flags):
                continue
        elif not analysis.get("consumable", False):
            continue
        theme = str(package.theme or "").strip()
        counts[theme] = counts.get(theme, 0) + 1
    return counts


def recent_gap_hints(ledger, *, hours: int = 72):
    """近窗口 gap 的 detail 文本（主题/品类线索从文本里抽，不新增表）。"""
    cutoff_expr = f"datetime('now','-{int(hours)} hours')"
    rows = ledger._conn.execute(
        "SELECT reason, detail, created_at FROM material_gaps"
        " WHERE created_at >= " + cutoff_expr +
        " ORDER BY rowid DESC LIMIT 100").fetchall()
    return [(str(r["reason"] or ""), str(r["detail"] or "")) for r in rows]


_GAP_KEYWORD_FAMILIES = [
    ("一衣多穿", "same_item_multiway"),
    ("多搭", "same_item_multiway"),
    ("旅行", "travel_layering"),
    ("旅游", "travel_layering"),
    ("配色", "color_ratio"),
    ("颜色", "color_ratio"),
    ("鞋", "shoes_pairing"),
    ("围巾", "scarf_pairing"),
]


def classify_gap(reason: str, detail: str):
    text = f"{reason} {detail}"
    for keyword, family in _GAP_KEYWORD_FAMILIES:
        if keyword in text:
            return family
    return ""


def build_demand(families, usable_by_theme, gap_rows):
    demands = []
    for family in families:
        fid = str(family.get("family_id") or "")
        themes = [str(t) for t in family.get("themes") or []]
        usable_now = sum(usable_by_theme.get(t, 0) for t in themes)
        threshold = int(family.get("min_usable_pool") or 0)
        gap_hits = sum(
            1 for reason, detail in gap_rows
            if classify_gap(reason, detail) == fid)
        queries = [str(q) for q in family.get("base_queries") or []]
        # 缺口 detail 里的商品品类/颜色词补检索词（§7：由实际商品形态补词）
        for reason, detail in gap_rows:
            if classify_gap(reason, detail) != fid:
                continue
            for token in ("开衫", "棉服", "羽绒", "外套", "半裙", "阔腿裤", "针织", "风衣"):
                if token in detail and f"{token}搭配" not in queries:
                    queries.append(f"{token}搭配")
                    break
        need_pool = usable_now < threshold
        need_gap = gap_hits > 0 and usable_now < max(threshold, gap_hits * 2)
        if not (need_pool or need_gap):
            continue
        reason_text = []
        if need_pool:
            reason_text.append(
                f"池子存量 {usable_now} < 目标 {threshold}")
        if need_gap:
            reason_text.append(f"近 {len(gap_rows)} 条缺口中 {gap_hits} 条指向本族")
        demands.append({
            "family": fid,
            "label": family.get("label"),
            "queries": queries,
            "usable_now": usable_now,
            "min_usable_pool": threshold,
            "gap_hits": gap_hits,
            "reason": "；".join(reason_text),
        })
    return demands


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default="Doubao-Seed-2.1-turbo")
    parser.add_argument("--gap-hours", type=int, default=72)
    parser.add_argument("--out", default="")
    args = parser.parse_args()

    from services.material_analysis import ANALYSIS_VERSION, MaterialLedger
    from services.material_source import MaterialSource

    families = load_families()
    ledger = MaterialLedger()
    usable = count_usable_by_theme(ledger, MaterialSource(), args.model, ANALYSIS_VERSION)
    gaps = recent_gap_hints(ledger, hours=args.gap_hours)
    ledger.close()
    demands = build_demand(families, usable, gaps)
    payload = {
        "schema_version": "opv-material-demand-list-v1",
        "generated_at": __import__("datetime").datetime.now().isoformat(timespec="seconds"),
        "usable_by_theme": usable,
        "demands": demands,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
        print(f"写出 {args.out}：{len(demands)} 个需求族")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
