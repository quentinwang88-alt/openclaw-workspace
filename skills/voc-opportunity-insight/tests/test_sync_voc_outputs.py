from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parents[1]
SCRIPT_PATH = SKILL_DIR / "scripts" / "sync_voc_outputs_to_feishu.py"
RESULT_PATH = (
    SKILL_DIR.parent
    / "voc-insight"
    / "output"
    / "FM_MX_WIGS_COMBINED_20260717_voc_opportunity_result.json"
)

SPEC = importlib.util.spec_from_file_location("voc_feishu_sync", SCRIPT_PATH)
assert SPEC and SPEC.loader
SYNC = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SYNC)


class SyncVocOutputsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.payload = json.loads(RESULT_PATH.read_text(encoding="utf-8"))
        cls.rows = SYNC.build_table_rows(cls.payload, "https://example.test/report")

    def test_expected_row_counts_and_unique_keys(self) -> None:
        expected = {"机会卡": 6, "信号聚合": 80, "规格风险": 48, "原子证据": 182}
        actual = {}
        keys = []
        for row in self.rows:
            actual[row["数据类型"]] = actual.get(row["数据类型"], 0) + 1
            keys.append(row["唯一键"])
        self.assertEqual(expected, actual)
        self.assertEqual(316, len(self.rows))
        self.assertEqual(len(keys), len(set(keys)))

    def test_signal_keys_are_stable_for_same_batch(self) -> None:
        changed = dict(self.payload)
        changed["run_id"] = "a-different-run-id"
        original_keys = {row["唯一键"] for row in self.rows if row["数据类型"] == "信号聚合"}
        changed_keys = {
            row["唯一键"]
            for row in SYNC.build_table_rows(changed, "https://example.test/report")
            if row["数据类型"] == "信号聚合"
        }
        self.assertEqual(original_keys, changed_keys)
        self.assertTrue(all(self.payload["batch_id"] in key for key in original_keys))

    def test_business_labels_are_chinese_and_prioritized(self) -> None:
        opportunity = next(row for row in self.rows if row["数据类型"] == "机会卡")
        signal = next(row for row in self.rows if row["数据类型"] == "信号聚合")
        evidence = next(row for row in self.rows if row["数据类型"] == "原子证据")
        self.assertTrue(opportunity["置信度"].startswith("P"))
        self.assertNotIn("feature_upgrade", opportunity.get("机会类型", ""))
        self.assertNotIn("braiding_hair", opportunity.get("产品形态", ""))
        self.assertNotIn("appearance_style", signal["文本"])
        self.assertIn(evidence["正负向"], {"正向", "负向", "正负混合", "中性"})

    def test_layered_table_contract(self) -> None:
        self.assertEqual(
            {"02_规格与风险", "03_信号分析", "04_原始证据", "99_后台全量"},
            set(SYNC.LAYER_TABLE_SCHEMAS),
        )
        self.assertNotIn("唯一键", SYNC.VIEW_VISIBLE_FIELDS["01_机会总览"])
        self.assertIn("原始VOC", SYNC.LAYER_TABLE_SCHEMAS["04_原始证据"])
        expected = {"02_规格与风险": 48, "03_信号分析": 80, "04_原始证据": 182, "99_后台全量": 316}
        self.assertEqual(expected, {
            name: len(SYNC.build_layer_rows(self.rows, name)) for name in SYNC.LAYER_TABLE_SCHEMAS
        })

    def test_fingerprint_ignores_operational_fields(self) -> None:
        fields = {"唯一键": "x", "报告链接": "u", "更新时间": "t"}
        base = SYNC.fingerprint(fields)
        fields["更新时间"] = "t2"
        fields["内容指纹"] = "old"
        self.assertEqual(base, SYNC.fingerprint(fields))
        fields["报告链接"] = "u2"
        self.assertNotEqual(base, SYNC.fingerprint(fields))

    def test_markdown_and_urls(self) -> None:
        blocks = SYNC.markdown_children("# 标题\n\n- 条目\n\n| A | B |\n|---|---|\n| 1 | 2 |")
        self.assertEqual([3, 12, 2, 2], [block["block_type"] for block in blocks])
        wiki_token, table_id = SYNC.parse_bitable_url(SYNC.DEFAULT_BITABLE_URL)
        self.assertEqual("DMIYwxge7iV8sAktvjDcZx84nzQ", wiki_token)
        self.assertEqual("tbl3wdy3DTHdjHlH", table_id)
        self.assertEqual("vewFYyTCLh", SYNC.parse_view_id(SYNC.DEFAULT_BITABLE_URL))


if __name__ == "__main__":
    unittest.main()
