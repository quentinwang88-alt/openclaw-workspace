"""Phase 1 素材消费模块测试：material_source / material_analysis / material_adapter。

不发任何网络请求（Doubao 客户端用 mock）；素材库与台账均为临时文件。
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from PIL import Image

from services.material_adapter import (
    ProductBrief,
    narrow_candidates,
    select_reference,
)
from services.material_analysis import MaterialAnalyzer, MaterialLedger
from services.material_source import MaterialSource

LAB_SCHEMA = """
CREATE TABLE notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    note_id TEXT UNIQUE,
    source_url TEXT,
    origin TEXT DEFAULT 'search',
    theme TEXT,
    title TEXT,
    description TEXT DEFAULT '',
    author_nickname TEXT DEFAULT '',
    like_count INTEGER,
    collected_count INTEGER,
    status TEXT DEFAULT 'pending_review',
    authorization TEXT DEFAULT 'reference_only',
    fetch_status TEXT DEFAULT 'fetched',
    image_count INTEGER DEFAULT 0
);
CREATE TABLE note_images (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    note_pk INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    file_path TEXT,
    sha256 TEXT,
    UNIQUE (note_pk, seq)
);
"""


def _tiny_jpeg(path: Path, color=(200, 180, 170)) -> bytes:
    with Image.new("RGB", (64, 96), color) as im:
        im.save(path, "JPEG", quality=80)
    return path.read_bytes()


class LabFixture:
    def __init__(self, root: Path):
        self.root = root
        self.db_path = root / "library.sqlite3"
        self.images_root = root / "out"
        self.images_root.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.executescript(LAB_SCHEMA)
        self._counter = 0

    def add_note(self, *, note_id: str, images: int = 2, theme: str = "旅游冬装",
                 title: str = "测试笔记", status: str = "pending_review",
                 fetch_status: str = "fetched", like: int = 100) -> None:
        self._counter += 1
        pk = self._counter
        self.conn.execute(
            "INSERT INTO notes (note_id, source_url, theme, title, status,"
            " fetch_status, image_count, like_count) VALUES (?,?,?,?,?,?,?,?)",
            (note_id, f"https://xhs/{note_id}", theme, title, status,
             fetch_status, images, like),
        )
        folder = self.images_root / "download" / note_id
        folder.mkdir(parents=True, exist_ok=True)
        for seq in range(1, images + 1):
            p = folder / f"{note_id}_{seq}.jpeg"
            data = _tiny_jpeg(p, color=(150 + seq * 10, 150, 150))
            import hashlib
            digest = hashlib.sha256(data).hexdigest()
            self.conn.execute(
                "INSERT INTO note_images (note_pk, seq, file_path, sha256) VALUES (?,?,?,?)",
                (pk, seq, str(p.relative_to(self.images_root)), digest),
            )
        self.conn.commit()

    def source(self) -> MaterialSource:
        return MaterialSource(str(self.db_path), str(self.images_root))

    def drop_image_file(self, note_id: str, seq: int) -> None:
        (self.images_root / "download" / note_id / f"{note_id}_{seq}.jpeg").unlink()


class MockClient:
    """按调用次序返回预置响应；记录调用以便断言。"""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def chat_with_multiple_images(self, paths, prompt, max_tokens):
        self.calls.append({"paths": list(paths), "prompt": prompt})
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return {
            "choices": [{"message": {"content": json.dumps(item, ensure_ascii=False)}}],
            "usage": {"prompt_tokens": 111, "completion_tokens": 22},
        }


def _analysis_payload(note_topic="冬季旅游穿搭", structure="same_item_multiway",
                      consumable=True, pages=2,
                      shoot_style="street_snap", photography_quality="good",
                      outfit_usable=True, visual_usable=True, narrative_usable=True):
    return {
        "note_topic": note_topic,
        "core_items": [{"item": "短款棉服", "role": "核心"}],
        "outfit_relations": "上短下高",
        "set_structure": structure,
        "page_roles": [
            {"seq": i, "role": "cover" if i == 1 else "full_outfit"}
            for i in range(1, pages + 1)
        ],
        "pages": [
            {"seq": i, "outfit_summary": f"第{i}页搭配：短款棉服+直筒裤+短靴",
             "outfit_set_id": i, "variation": ""}
            for i in range(1, pages + 1)
        ],
        "palette": ["奶白", "浅蓝"],
        "photography": "街拍",
        "background": "城市街道",
        "shoot_style": shoot_style,
        "photography_quality": photography_quality,
        "purpose_usability": {
            "outfit": {"usable": outfit_usable, "reason": "搭配清楚"},
            "visual": {"usable": visual_usable, "reason": "色调可借鉴"},
            "narrative": {"usable": narrative_usable, "reason": "递进清晰"},
        },
        "consumable": consumable,
        "consumable_reason": "穿搭清晰",
    }


class MaterialSourceTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.lab = LabFixture(Path(self._tmp.name))

    def tearDown(self):
        self._tmp.cleanup()

    def test_filters_and_order(self):
        self.lab.add_note(note_id="a" * 24, images=3, status="selected")
        self.lab.add_note(note_id="b" * 24, status="rejected")
        self.lab.add_note(note_id="c" * 24, fetch_status="partial")
        source = self.lab.source()
        packages = source.list_packages()
        self.assertEqual([p.note_id for p in packages], ["a" * 24])
        self.assertEqual([img.seq for img in packages[0].images], [1, 2, 3])
        self.assertTrue(packages[0].complete)
        self.assertEqual(packages[0].authorization, "reference_only")

    def test_missing_file_breaks_completeness(self):
        self.lab.add_note(note_id="d" * 24, images=2)
        self.lab.drop_image_file("d" * 24, 2)
        source = self.lab.source()
        packages = source.list_packages(require_complete=True)
        self.assertEqual(packages, [])
        package = source.get("d" * 24)
        self.assertIsNotNone(package)
        self.assertFalse(package.complete)

    def test_fingerprint_tracks_image_set(self):
        self.lab.add_note(note_id="e" * 24, images=2)
        source = self.lab.source()
        first = source.get("e" * 24).version_fingerprint
        # 指纹基于库内容（note_id + 有序 sha256）：重复读取稳定
        self.assertEqual(first, source.get("e" * 24).version_fingerprint)
        # 不同素材集合指纹不同
        self.lab.add_note(note_id="7" * 24, images=3)
        self.assertNotEqual(first, source.get("7" * 24).version_fingerprint)
        # 文件丢失不改变指纹（该笔记转为不可用而非新版本）
        self.lab.drop_image_file("e" * 24, 1)
        self.assertEqual(first, source.get("e" * 24).version_fingerprint)


class MaterialAnalyzerTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.lab = LabFixture(self.root)
        self.ledger = MaterialLedger(str(self.root / "ledger.sqlite3"))

    def tearDown(self):
        self.ledger.close()
        self._tmp.cleanup()

    def _analyzer(self, client, batch_size=6):
        return MaterialAnalyzer(
            self.lab.source(), self.ledger, client, model="mock-model",
            batch_size=batch_size, max_image_edge=64)

    def test_batches_merge_and_cache(self):
        self.lab.add_note(note_id="f" * 24, images=5)
        client = MockClient([
            _analysis_payload(pages=2), _analysis_payload(pages=2),
            _analysis_payload(pages=1),
        ])
        analyzer = self._analyzer(client, batch_size=2)
        outcome = analyzer.analyze_note("f" * 24)
        self.assertIsNone(outcome.error)
        self.assertEqual(len(client.calls), 3)  # 5 张图 ÷ 每批 2 = 3 批
        seqs = [r["seq"] for r in outcome.result["page_roles"]]
        self.assertEqual(seqs, [1, 2, 3, 4, 5])  # 分批合并保序

        again = analyzer.analyze_note("f" * 24)
        self.assertTrue(again.cached)
        self.assertEqual(len(client.calls), 3)  # 命中缓存不再调用
        cost = self.ledger.cost_summary()
        self.assertEqual(cost["calls"], 3)
        self.assertEqual(cost["images"], 5)

    def test_failure_records_gap(self):
        self.lab.add_note(note_id="9" * 24, images=1)
        client = MockClient([RuntimeError("boom"), RuntimeError("boom")])
        analyzer = self._analyzer(client)
        outcome = analyzer.analyze_note("9" * 24)
        self.assertIsNotNone(outcome.error)
        rows = self.ledger._conn.execute(
            "SELECT reason FROM material_gaps").fetchall()
        self.assertTrue(any(r["reason"] == "analyze_failed" for r in rows))

    def test_failure_cooldown_prevents_paid_retry(self):
        # 分析失败后 24h 冷却：analyze_pending 不再重试（force 可越过）
        self.lab.add_note(note_id="6" * 24, images=1)
        failing = MockClient([RuntimeError("boom"), RuntimeError("boom")])
        analyzer = self._analyzer(failing)
        outcome = analyzer.analyze_note("6" * 24)
        self.assertIsNotNone(outcome.error)
        fresh = MockClient([])  # 第二轮：客户端不应再被调用
        analyzer2 = MaterialAnalyzer(
            self.lab.source(), self.ledger, fresh, model="mock-model",
            batch_size=6, max_image_edge=64)
        outcomes = analyzer2.analyze_pending(limit=5)
        self.assertEqual(fresh.calls, [])
        self.assertEqual(outcomes, [])   # 冷却命中：待办为空
        forced = MockClient([_analysis_payload(pages=1)])
        analyzer3 = MaterialAnalyzer(
            self.lab.source(), self.ledger, forced, model="mock-model",
            batch_size=6, max_image_edge=64)
        outcome = analyzer3.analyze_note("6" * 24, force=True)
        self.assertIsNone(outcome.error)   # force 越过冷却

    def test_incomplete_note_rejected_without_call(self):
        self.lab.add_note(note_id="8" * 24, fetch_status="partial")
        client = MockClient([])
        analyzer = self._analyzer(client)
        outcome = analyzer.analyze_note("8" * 24)
        self.assertIsNotNone(outcome.error)
        self.assertEqual(len(client.calls), 0)


class MaterialAdapterTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.lab = LabFixture(Path(self._tmp.name))

    def tearDown(self):
        self._tmp.cleanup()

    def _packages(self, note_ids):
        source = self.lab.source()
        return {p.note_id: p for p in source.list_packages(include_rejected=True)}

    def test_narrow_prefers_structure_and_product(self):
        self.lab.add_note(note_id="1" * 24, title="一衣多穿", images=2)
        self.lab.add_note(note_id="2" * 24, title="独立合集", images=2, like=50000)
        packages = self.lab.source().list_packages()
        analyses = {
            "1" * 24: _analysis_payload(structure="same_item_multiway"),
            "2" * 24: _analysis_payload(structure="independent_collection"),
        }
        result = narrow_candidates(
            packages, analyses, theme="一衣多穿",
            product=ProductBrief("P1", "短款棉服"))
        self.assertEqual(result[0].note_id, "1" * 24)

        recent = narrow_candidates(
            packages, analyses, theme="一衣多穿",
            product=ProductBrief("P1", "短款棉服"),
            recent_note_ids=["1" * 24])
        self.assertEqual(recent[0].note_id, "2" * 24)  # 近期使用降权生效

    def test_unconsumable_excluded(self):
        packages = self.lab.source().list_packages()
        analyses = {"1" * 24: _analysis_payload(consumable=False)}
        result = narrow_candidates(packages, analyses, theme="")
        self.assertEqual(result, [])

    def test_purpose_usability_gate(self):
        # v3 按用途可用性（附B-2 取代审美硬排除）：
        # - 摄影差但搭配清楚（outfit 可用）→ 仍在候选（降权，可文字化借鉴）
        # - 三用途全不可用 → 排除
        self.lab.add_note(note_id="1" * 24, images=2)
        self.lab.add_note(note_id="2" * 24, images=2)
        self.lab.add_note(note_id="3" * 24, images=2)
        packages = self.lab.source().list_packages()
        analyses = {
            "1" * 24: _analysis_payload(shoot_style="mirror_selfie",
                                        photography_quality="poor",
                                        visual_usable=False),
            "2" * 24: _analysis_payload(outfit_usable=False,
                                        visual_usable=False,
                                        narrative_usable=False),
            "3" * 24: _analysis_payload(shoot_style="studio",
                                        photography_quality="good"),
        }
        result = narrow_candidates(packages, analyses, theme="")
        ids = [c.note_id for c in result]
        self.assertIn("1" * 24, ids)      # 摄影差但 outfit 可用 → 保留
        self.assertNotIn("2" * 24, ids)   # 全用途不可用 → 排除
        self.assertIn("3" * 24, ids)
        poor = next(c for c in result if c.note_id == "1" * 24)
        self.assertTrue(any("摄影质量差" in r for r in poor.reasons))
        self.assertLess(poor.score, next(
            c.score for c in result if c.note_id == "3" * 24))

    def test_select_reference_validates_main(self):
        self.lab.add_note(note_id="1" * 24, title="候选", images=2)
        packages = self.lab.source().list_packages()
        analyses = {"1" * 24: _analysis_payload()}
        candidates = narrow_candidates(packages, analyses, theme="一衣多穿")
        ok = MockClient([{
            "main_note_id": "1" * 24,
            "supplement_note_ids": ["2" * 24],   # 不在候选内 → 应被过滤
            "adoption": "outfit_only",
            "rationale": "借鉴搭配",
            "rejected": [],
        }])
        result = select_reference(ok, candidates, theme="一衣多穿",
                                  product=ProductBrief("P1", "短款棉服"))
        self.assertEqual(result.main_note_id, "1" * 24)
        self.assertEqual(result.supplement_note_ids, [])
        self.assertEqual(result.adoption, "outfit_only")

        bad = MockClient([{"main_note_id": "zzz-not-exist", "adoption": "overall"}])
        self.assertIsNone(
            select_reference(bad, candidates, theme="一衣多穿"))

    def test_select_reference_page_validation(self):
        # 页级选材校验：虚拟页码剔除、同页去重、narrative_only 强制清空
        self.lab.add_note(note_id="1" * 24, title="候选", images=3)
        packages = self.lab.source().list_packages()
        analyses = {"1" * 24: _analysis_payload(pages=3)}
        candidates = narrow_candidates(packages, analyses, theme="")
        ok = MockClient([{
            "main_note_id": "1" * 24, "adoption": "outfit_only",
            "pages": [{"seq": 2, "purpose": "outfit_detail"},
                      {"seq": 99, "purpose": "full_outfit"},   # 不存在的页
                      {"seq": 2, "purpose": "visual_tone"}],   # 重复页
            "rationale": "细节页", "rejected": [],
        }])
        result = select_reference(ok, candidates, theme="")
        self.assertEqual([p["seq"] for p in result.pages], [2])
        self.assertEqual(result.pages[0]["note_id"], "1" * 24)

        narrative = MockClient([{
            "main_note_id": "1" * 24, "adoption": "narrative_only",
            "pages": [{"seq": 1, "purpose": "full_outfit"}],  # 模型违规带页
            "rationale": "只借结构", "rejected": [],
        }])
        result2 = select_reference(narrative, candidates, theme="")
        self.assertEqual(result2.pages, [])

    def test_thermal_utilities(self):
        from services.material_adapter import (
            parse_thermal_band, thermal_overlap, thermal_window,
            title_season_tag, theme_thermal_band)
        self.assertEqual(parse_thermal_band("气温为15-22°C"), (15, 22))
        self.assertEqual(parse_thermal_band("15–22度"), (15, 22))
        self.assertIsNone(parse_thermal_band("无温度"))
        self.assertEqual(thermal_window("浅蓝色短款蓬松外套", "outerwear"), (5, 12))
        self.assertEqual(thermal_window("奶白针织开衫"), (12, 20))
        self.assertIsNone(thermal_window("饰品"))
        self.assertTrue(thermal_overlap((12, 20), (15, 22)))
        self.assertFalse(thermal_overlap((5, 12), (15, 22)))
        self.assertEqual(title_season_tag("秋冬旅行穿搭 保暖又出片"), "cold")
        self.assertEqual(title_season_tag("海岛夏日度假穿搭"), "warm")
        self.assertEqual(title_season_tag("秋日城市穿搭"), "cool")
        self.assertEqual(title_season_tag("随便看看"), "")
        self.assertEqual(theme_thermal_band("凉爽旅行"), (15, 22))
        self.assertIsNone(theme_thermal_band("自动"))

    def test_season_penalty_under_temperature_band(self):
        # 温度带 15-22°C 下，秋冬厚装主导的笔记被强降权（不硬排除——
        # 冷笔记里可能仍有薄款搭配页，交给终选页级约束把关）
        self.lab.add_note(note_id="c" * 24, images=2, title="秋冬旅行穿搭 保暖又出片")
        self.lab.add_note(note_id="e" * 24, images=2, title="城市漫步日常")
        packages = self.lab.source().list_packages()
        analyses = {
            "c" * 24: _analysis_payload(),
            "e" * 24: _analysis_payload(),
        }
        result = narrow_candidates(
            packages, analyses, theme="", temperature_band=(15, 22))
        scores = {c.note_id: c.score for c in result}
        self.assertGreater(scores["e" * 24], scores["c" * 24])
        cold = next(c for c in result if c.note_id == "c" * 24)
        self.assertTrue(any("温度带不符" in r for r in cold.reasons))

    def test_empty_candidates_returns_none(self):
        self.assertIsNone(select_reference(MockClient([]), [], theme="四选一"))


    def test_b3_purpose_compatibility_enforced(self):
        # B3：用途与可用性强校验——摄影不可用→visual_tone 页被丢、
        # visual_only 确定性降级 outfit_only；不默认 overall
        from services.material_adapter import enforce_purpose_compatibility
        from services.material_adapter import SelectionResult
        base = _analysis_payload(visual_usable=False, outfit_usable=True)
        sel = SelectionResult(main_note_id="1" * 24, adoption="visual_only",
                              pages=[{"note_id": "1" * 24, "seq": 1, "purpose": "visual_tone"},
                                     {"note_id": "1" * 24, "seq": 2, "purpose": "outfit_detail"}])
        out = enforce_purpose_compatibility(sel, base)
        self.assertEqual(out.adoption, "outfit_only")          # 降级不 overall
        self.assertEqual([p_["purpose"] for p_ in out.pages], ["outfit_detail"])
        # 全不可用 → narrative_only 零图
        none_ok = _analysis_payload(outfit_usable=False, visual_usable=False,
                                    narrative_usable=True)
        out2 = enforce_purpose_compatibility(
            SelectionResult(main_note_id="1" * 24, adoption="outfit_only",
                            pages=[{"note_id": "1" * 24, "seq": 1, "purpose": "outfit_detail"}]),
            none_ok)
        self.assertEqual(out2.adoption, "narrative_only")
        self.assertEqual(out2.pages, [])
        # v2 旧缓存（用途未知）不拦截
        legacy = dict(base); legacy.pop("purpose_usability")
        out3 = enforce_purpose_compatibility(sel, legacy)
        self.assertEqual(out3.adoption, "visual_only")


if __name__ == "__main__":
    unittest.main()
