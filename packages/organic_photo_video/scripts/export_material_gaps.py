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


def _covers_purposes(analysis, purposes, mode: str) -> bool:
    """需求用途校验（方案 C2）：mode=any 可替代 / all 需同时具备；
    v2 旧缓存未知用途退回 consumable。"""
    usability = analysis.get("purpose_usability")
    if not (isinstance(usability, dict) and usability):
        return bool(analysis.get("consumable", False))
    flags = [bool((usability.get(n) or {}).get("usable"))
             for n in (purposes or [])
             if isinstance(usability.get(n), dict)]
    if not flags:
        return False
    return all(flags) if mode == "all" else any(flags)


def count_usable_by_family(families, ledger, source, model: str,
                           analysis_version: str, lab_db: str = ""):
    """按查询族统计可用候选（方案 C2）。

    归属以 query_hits.family 为主、notes.theme 兼容别名兜底（label 仅
    展示）；用途按族配置（purposes+purposes_mode）校验；同件多搭族区分
    真同件多搭（set_structure=same_item_multiway）与可适配候选。
    计数按 note_id 去重（v3 缓存键本身按版本指纹）。
    """
    import sqlite3
    family_ids = {str(f.get("family_id")) for f in families}
    hits_by_fid: dict = {fid: set() for fid in family_ids}
    if lab_db:
        try:
            conn = sqlite3.connect(f"file:{lab_db}?mode=ro", uri=True)
            for fid, note_id in conn.execute(
                    "SELECT DISTINCT family, note_id FROM query_hits"):
                if fid in hits_by_fid:
                    hits_by_fid[fid].add(str(note_id))
            conn.close()
        except Exception:  # noqa: BLE001 - lab 库缺失时退回主题别名
            pass
    theme_alias: dict = {}
    for f in families:
        for theme in f.get("themes") or []:
            theme_alias[str(theme)] = str(f.get("family_id"))

    analyses_by_note = {}
    for package in source.list_packages():
        analysis = ledger.get_cached_analysis(
            package.version_fingerprint, model, analysis_version)
        if analysis:
            analyses_by_note[package.note_id] = analysis
            fid = theme_alias.get(str(package.theme or "").strip())
            if fid:
                hits_by_fid[fid].add(package.note_id)

    counts: dict = {}
    for f in families:
        fid = str(f.get("family_id"))
        purposes = [str(x) for x in f.get("purposes") or []]
        mode = str(f.get("purposes_mode") or "any")
        usable = true_mw = 0
        for note_id in hits_by_fid.get(fid, ()):
            analysis = analyses_by_note.get(note_id)
            if not analysis or not _covers_purposes(analysis, purposes, mode):
                continue
            usable += 1
            if (str(analysis.get("set_structure") or "")
                    == "same_item_multiway"):
                true_mw += 1
        counts[fid] = {
            "usable": usable,
            "true_multiway": true_mw,
            "adaptable": max(0, usable - true_mw),
        }
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
        raw = usable_by_theme.get(fid, 0)
        if isinstance(raw, dict):
            usable_now = int(raw.get("usable") or 0)
        else:
            usable_now = int(raw)
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


def _infer_family(demand: dict) -> str:
    """台账需求→查询族推断（§6.3：语义资格由分析决定，此处仅归属）。"""
    text = " ".join(str(demand.get(k) or "") for k in
                    ("theme_direction", "product_form"))
    if any(k in text for k in ("围巾", "丝巾")):
        return "scarf_pairing"
    if any(k in text for k in ("鞋",)):
        return "shoes_pairing"
    if any(k in text for k in ("配色", "颜色", "色系")):
        return "color_ratio"
    if any(k in text for k in ("一衣多穿", "多搭")):
        return "same_item_multiway"
    if any(k in text for k in ("旅行", "旅游", "穿脱", "层次")):
        return "travel_layering"
    return ""


def compose_demand_queries(demand: dict) -> list:
    """方案 C3：目的地/商品词组合查询（2 定向＋1 通用）。

    词只取明确字段（国家/城市/主题方向/商品形态），不补未知季节；
    无目的地时只用通用词；命中词是检索来源而非拍摄地证明。
    """
    theme = str(demand.get("theme_direction") or "").strip()
    country = str(demand.get("destination_country") or "").strip()
    form = str(demand.get("product_form") or "").strip()
    theme_kw = theme if theme and theme != "自由选题" else "穿搭"
    queries = []
    if country:
        queries.append(f"{country}{theme_kw}穿搭")
        if form:
            queries.append(f"{country}{form}搭配")
    elif form:
        queries.append(f"{form}搭配")
    queries.append(f"{theme_kw}穿搭灵感")     # 通用兜底词
    return queries[:3]


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
    from services.material_source import DEFAULT_LIBRARY_DB
    usable = count_usable_by_family(
        families, ledger, MaterialSource(), args.model, ANALYSIS_VERSION,
        lab_db=str(DEFAULT_LIBRARY_DB))
    gaps = recent_gap_hints(ledger, hours=args.gap_hours)
    demands = build_demand(families, usable, gaps)
    # 方案 §6.1：统一 demands 合同——族需求带 demand_key；台账需求按
    # family 推断归属、需求消退（所属族库存达标即视为已满足不导出）
    for item in demands:
        item.setdefault("demand_key", f"family:{item.get('family')}")
    fam_usable = {fid: (v.get("usable") if isinstance(v, dict) else v)
                  for fid, v in (usable or {}).items()}
    ledger_demands = []
    try:
        for item in ledger.list_demands(within_days=14):
            item = dict(item)
            fid = _infer_family(item)
            threshold = next(
                (int(f.get("min_usable_pool") or 0)
                 for f in families if f.get("family_id") == fid), 0)
            if fid and int(fam_usable.get(fid) or 0) >= threshold:
                continue     # 需求消退：该族库存已达标
            item["family"] = fid or ""
            item["demand_key"] = f"ledger:{item.get('theme_direction')}|"                                  f"{item.get('destination_country')}"
            item["queries"] = compose_demand_queries(item)
            ledger_demands.append(item)
    except Exception:  # noqa: BLE001 - 台账需求读取失败不阻塞族需求
        ledger_demands = []
    ledger.close()
    unified = demands + ledger_demands
    payload = {
        "schema_version": "opv-material-demand-list-v2",
        "generated_at": __import__("datetime").datetime.now().isoformat(timespec="seconds"),
        "usable_by_theme": usable,
        "demands": unified,
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
