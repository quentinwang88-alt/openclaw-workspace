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
     "min_usable_pool": 6},
    {"family_id": "travel_layering", "label": "旅行穿脱/层次",
     "themes": ["旅行穿搭"], "base_queries": ["旅行穿搭"],
     "min_usable_pool": 8},
    {"family_id": "scarf_pairing", "label": "围巾搭配", "themes": [],
     "base_queries": ["围巾搭配"], "min_usable_pool": 4},
]


class DemandListTest(unittest.TestCase):
    def test_pool_threshold_gates_expansion(self):
        # 库存足够的族不扩量（§7：库存足够即停止扩量）；
        # 存量 0 的族（一衣多穿/围巾）入选，存量足的旅行族被排除
        demands = build_demand(
            FAMILIES, {"旅行穿搭": 10}, [])
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
