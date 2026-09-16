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
                      consumable=True, pages=2):
    return {
        "note_topic": note_topic,
        "core_items": [{"item": "短款棉服", "role": "核心"}],
        "outfit_relations": "上短下高",
        "set_structure": structure,
        "page_roles": [
            {"seq": i, "role": "cover" if i == 1 else "full_outfit"}
            for i in range(1, pages + 1)
        ],
        "palette": ["奶白", "浅蓝"],
        "photography": "街拍",
        "background": "城市街道",
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

    def test_empty_candidates_returns_none(self):
        self.assertIsNone(select_reference(MockClient([]), [], theme="四选一"))


if __name__ == "__main__":
    unittest.main()
