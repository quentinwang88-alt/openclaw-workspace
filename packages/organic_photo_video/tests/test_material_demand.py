"""需求驱动采集（消费收敛方案 §7）——需求清单语义与入库/query_hits。"""
from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
LAB_ROOT = PACKAGE_ROOT.parents[1] / "labs" / "xhs-material-lab"
sys.path.insert(0, str(PACKAGE_ROOT))
sys.path.insert(0, str(PACKAGE_ROOT / "scripts"))
sys.path.insert(0, str(LAB_ROOT / "scripts"))

from export_material_gaps import build_demand, classify_gap  # noqa: E402

NOTES_DDL = """
CREATE TABLE notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    note_id TEXT UNIQUE, xsec_token TEXT, source_url TEXT, theme TEXT,
    origin TEXT, title TEXT, author_id TEXT, author_nickname TEXT,
    like_count INTEGER, collected_count INTEGER, status TEXT, fetch_status TEXT
);
"""

FAMILIES = [
    {"family_id": "same_item_multiway", "label": "同件多搭",
     "themes": ["一衣多穿"], "base_queries": ["一衣多穿"],
     "purposes": ["outfit"], "min_usable_pool": 6},
    {"family_id": "travel_layering", "label": "旅行穿脱/层次",
     "themes": ["旅行穿搭"], "base_queries": ["旅行穿搭"],
     "purposes": ["outfit", "narrative"], "purposes_mode": "any",
     "min_usable_pool": 8},
    {"family_id": "scarf_pairing", "label": "围巾搭配", "themes": [],
     "base_queries": ["围巾搭配"], "purposes": ["outfit"],
     "min_usable_pool": 4},
]


class DemandListTest(unittest.TestCase):
    def test_pool_threshold_gates_expansion(self):
        # 库存足够的族不扩量（§7：库存足够即停止扩量）；
        # 存量 0 的族（一衣多穿/围巾）入选，存量足的旅行族被排除
        demands = build_demand(
            FAMILIES, {"travel_layering": 10}, [])
        self.assertEqual(
            [d["family"] for d in demands],
            ["same_item_multiway", "scarf_pairing"])
        scarf = next(d for d in demands if d["family"] == "scarf_pairing")
        self.assertEqual(scarf["usable_now"], 0)
        self.assertIn("池子存量", scarf["reason"])

    def test_gap_classification_and_query_augment(self):
        # 缺口按关键词归族；商品品类词从缺口 detail 补进检索词
        gaps = [("no_material", "无可用候选：主题 一衣多穿，缺 开衫 多搭参考")]
        demands = build_demand(FAMILIES, {}, gaps)
        item = next(d for d in demands if d["family"] == "same_item_multiway")
        self.assertIn("开衫搭配", item["queries"])
        self.assertEqual(item["gap_hits"], 1)
        self.assertEqual(classify_gap("no_material", "主题 旅行穿搭"),
                         "travel_layering")
        self.assertEqual(classify_gap("no_material", "无可用候选素材"), "")


class CollectImportTest(unittest.TestCase):
    def test_import_feeds_and_query_hits(self):
        from collect_by_demand import import_feeds

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(NOTES_DDL)
        feeds = [
            {"id": "a" * 24, "xsecToken": "t1", "noteCard": {
                "type": "normal", "displayTitle": "围巾",
                "user": {"userId": "u", "nickname": "n"},
                "interactInfo": {"likedCount": "1.2万", "collectedCount": 300}}}
            for _ in range(2)
        ] + [
            {"id": "b" * 24, "xsecToken": "t2", "noteCard": {"type": "video"}},
        ]
        added, dup, skipped = import_feeds(conn, feeds, "围巾搭配", 10)
        self.assertEqual((added, dup, skipped), (1, 1, 1))
        row = conn.execute(
            "SELECT like_count, collected_count, theme, status FROM notes"
            " WHERE note_id=?", ("a" * 24,)).fetchone()
        self.assertEqual(row["like_count"], 12000)   # 「1.2万」解析
        self.assertEqual(row["theme"], "围巾搭配")
        self.assertEqual(row["status"], "pending_review")


if __name__ == "__main__":
    unittest.main()


class DemandLedgerTest(unittest.TestCase):
    """方案 C1/C3：需求进台账（相同需求合并）+ 目的地组合查询词。"""

    def test_record_and_merge_demand(self):
        from services.material_analysis import MaterialLedger
        with TemporaryDirectory() as tmp:
            ledger = MaterialLedger(str(Path(tmp) / "d.sqlite3"))
            demand = {"theme_direction": "旅行穿脱/层次", "purposes": ["outfit"],
                      "product_form": "短外套", "destination_country": "日本",
                      "destination_use": "environment_inspiration"}
            ledger.record_demand(demand)
            ledger.record_demand(demand)          # 相同需求合并计数
            other = dict(demand, destination_country="韩国")
            ledger.record_demand(other)           # 不同目的地=不同需求
            rows = ledger.list_demands()
            self.assertEqual(len(rows), 2)
            top = max(rows, key=lambda r: r["hit_count"])
            self.assertEqual(top["hit_count"], 2)
            self.assertEqual(top["destination_country"], "日本")

    def test_compose_demand_queries(self):
        from export_material_gaps import compose_demand_queries
        q = compose_demand_queries({"theme_direction": "配色", "product_form": "开衫",
                                    "destination_country": ""})
        self.assertIn("开衫搭配", q)               # 无目的地只用商品词+通用
        q2 = compose_demand_queries({"theme_direction": "旅行穿脱/层次",
                                     "product_form": "短外套",
                                     "destination_country": "日本"})
        self.assertEqual(len(q2), 3)               # 2 定向+1 通用
        self.assertTrue(q2[0].startswith("日本"))


class FamilyInventoryTest(unittest.TestCase):
    """方案 C2：按族+需求用途计数，真同件多搭与可适配分开。"""

    def test_covers_purposes_mode(self):
        from export_material_gaps import _covers_purposes
        a_any = {"purpose_usability": {"outfit": {"usable": True},
                                       "narrative": {"usable": False}}}
        a_none = {"purpose_usability": {"outfit": {"usable": False},
                                        "narrative": {"usable": False}}}
        # any：一个可用即满足；all：必须同时
        self.assertTrue(_covers_purposes(a_any, ["outfit", "narrative"], "any"))
        self.assertFalse(_covers_purposes(a_any, ["outfit", "narrative"], "all"))
        self.assertFalse(_covers_purposes(a_none, ["outfit"], "any"))
        # v2 旧缓存退回 consumable
        self.assertTrue(_covers_purposes({"consumable": True}, ["outfit"], "all"))

    def test_count_usable_by_family_split_multiway(self):
        from export_material_gaps import count_usable_by_family
        with TemporaryDirectory() as tmp:
            from services.material_analysis import ANALYSIS_VERSION, MaterialLedger
            from tests.test_material_phase1 import LabFixture, _analysis_payload
            lab = LabFixture(Path(tmp))
            lab.add_note(note_id="m" * 24, images=2, theme="一衣多穿",
                         title="真一衣多穿")
            ledger = MaterialLedger(str(Path(tmp) / "l.sqlite3"))
            pkg = lab.source().get("m" * 24)
            ledger.put_cached_analysis(
                fingerprint=pkg.version_fingerprint, model="m",
                analysis_version=ANALYSIS_VERSION, note_id="m" * 24,
                result=_analysis_payload(structure="same_item_multiway"),
                image_count=2, calls=1, prompt_tokens=1, completion_tokens=1,
                duration_ms=1)
            ledger2 = MaterialLedger(str(Path(tmp) / "l.sqlite3"))
            counts = count_usable_by_family(
                FAMILIES, ledger2, lab.source(), "m", ANALYSIS_VERSION)
            mw = counts["same_item_multiway"]
            self.assertEqual(mw["usable"], 1)
            self.assertEqual(mw["true_multiway"], 1)   # 真结构
            self.assertEqual(mw["adaptable"], 0)
            # 主题别名归属：notes.theme 不在 themes 里则不归属
            self.assertEqual(counts["travel_layering"]["usable"], 0)


class UnifiedDemandsTest(unittest.TestCase):
    """方案 §6.1/§6.3：统一 demands 合同、需求消退、搜索冷却。"""

    def test_export_unifies_and_fades(self):
        from services.material_analysis import MaterialLedger
        with TemporaryDirectory() as tmp:
            ledger = MaterialLedger(str(Path(tmp) / "u.sqlite3"))
            # 台账需求：旅行穿脱/层次+日本（旅行族库存达标→消退）
            ledger.record_demand({"theme_direction": "旅行穿脱/层次",
                                  "purposes": ["outfit"], "product_form": "",
                                  "destination_country": "日本",
                                  "destination_use": "environment_inspiration"})
            # 台账需求：围巾（围巾族库存 0→导出，带 demand_key/queries）
            ledger.record_demand({"theme_direction": "围巾搭配",
                                  "purposes": ["outfit"], "product_form": "",
                                  "destination_country": "",
                                  "destination_use": ""})
            ledger.close()
            import export_material_gaps as E
            families = [
                {"family_id": "travel_layering", "themes": ["旅行穿搭"],
                 "base_queries": ["x"], "purposes": ["outfit"],
                 "min_usable_pool": 8},
                {"family_id": "scarf_pairing", "themes": ["围巾搭配"],
                 "base_queries": ["y"], "purposes": ["outfit"],
                 "min_usable_pool": 4}]
            usable = {"travel_layering": {"usable": 18}, "scarf_pairing": {"usable": 0}}
            fam_demands = E.build_demand(families, usable, [])
            for item in fam_demands:
                item.setdefault("demand_key", f"family:{item.get('family')}")
            # 复刻 main 的合并+消退路径
            fam_usable = {fid: (v.get("usable") if isinstance(v, dict) else v)
                          for fid, v in usable.items()}
            out = []
            led = MaterialLedger(str(Path(tmp) / "u.sqlite3"))
            for item in led.list_demands(within_days=14):
                item = dict(item)
                fid = E._infer_family(item)
                threshold = next(
                    (int(f.get("min_usable_pool") or 0)
                     for f in families if f.get("family_id") == fid), 0)
                if fid and int(fam_usable.get(fid) or 0) >= threshold:
                    continue
                item["family"] = fid or ""
                item["demand_key"] = "ledger:x"
                item["queries"] = E.compose_demand_queries(item)
                out.append(item)
            led.close()
            themes = [x.get("theme_direction") for x in out]
            self.assertNotIn("旅行穿脱/层次", themes)   # 消退
            self.assertIn("围巾搭配", themes)           # 未满足→导出
            self.assertTrue(out[0].get("demand_key"))
            self.assertTrue(E._infer_family(
                {"theme_direction": "东京配色层次"}) == "color_ratio")

    def test_collector_cooldown_skips_recent_demand(self):
        import sqlite3
        import collect_by_demand as C
        with TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "lab.sqlite3")
            conn = sqlite3.connect(db)
            conn.executescript(C.QUERY_HITS_SCHEMA
                               + """
CREATE TABLE notes (id INTEGER PRIMARY KEY AUTOINCREMENT,
  note_id TEXT UNIQUE, xsec_token TEXT, source_url TEXT, theme TEXT,
  origin TEXT, title TEXT, author_id TEXT, author_nickname TEXT,
  like_count INTEGER, collected_count INTEGER, status TEXT,
  fetch_status TEXT);
""")
            conn.execute(
                "INSERT INTO search_attempts (demand_key, query, state,"
                " result_count) VALUES ('ledger:scarf','围巾搭配','done',0)")
            conn.commit()
            calls = []
            C.mcp_search = lambda q, f, timeout=60: calls.append(q) or []
            import types
            args = types.SimpleNamespace(
                demand={"demands": [{"family": "scarf_pairing",
                                     "demand_key": "ledger:scarf",
                                     "queries": ["围巾搭配"]}]},
                db=db, families=str(Path(__file__).resolve().parents[3] /
                                    "labs/xhs-material-lab/config/query_families.json"),
                limit_per_query=5, out_root=str(Path(tmp)))
            C.main.__wrapped__ if hasattr(C.main, "__wrapped__") else None
            # 直接调用 main 会走 argparse——用 sys.argv 注入
            import sys as _sys
            old_argv = _sys.argv
            _sys.argv = ["collect_by_demand.py", "--demand", "/dev/null"]
            (Path(tmp) / "d.json").write_text(
                '{"demands": [{"family": "scarf_pairing", "demand_key":'
                ' "ledger:scarf", "queries": ["围巾搭配"]}]}', encoding="utf-8")
            _sys.argv = ["collect_by_demand.py", "--demand",
                         str(Path(tmp) / "d.json"), "--db", db,
                         "--limit-per-query", "5",
                         "--out-root", str(Path(tmp)),
                         "--families", str(
                             Path(__file__).resolve().parents[3] /
                             "labs/xhs-material-lab/config/query_families.json")]
            try:
                C.main()
            finally:
                _sys.argv = old_argv
            self.assertEqual(calls, [])      # 冷却命中：不发起搜索
