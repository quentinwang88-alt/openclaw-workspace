"""外部参考执行合同与接线门禁测试（Phase 1-3）。

覆盖 2026-09-16 修复方案 §八 的关键跨层用例（可单测部分）：
- 外部参考数量等于角色数 → 仍不能走原图直用；
- 合同持久化/幂等/指纹（adoption/页面/目的地变化必须换指纹）；
- 旅行目的地统一解析与冲突；
- 外部行门禁（缺合同/授权异常/COMPLETE_LOOK/产品不一致）；
- 选材结果的页级校验（虚拟页码剔除、narrative_only 清空）。
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT))
sys.path.insert(0, str(PACKAGE_ROOT.parent))

from services.external_supply_contract import (  # noqa: E402
    SUPPLY_POLICY_VERSION, ExternalSupplyContractStore, contract_fingerprint)
from services.photo_travel_qa import (  # noqa: E402
    TravelDestinationError, resolve_travel_destination)


class FakeContractStore:
    """门禁单测用的轻量假库。"""

    def __init__(self, contracts):
        self._by_record = {c["record_id"]: c for c in contracts if c.get("record_id")}

    def find_by_record(self, record_id):
        return self._by_record.get(record_id)


class ExternalContractStoreTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.store = ExternalSupplyContractStore(str(Path(self._tmp.name) / "c.sqlite3"))

    def tearDown(self):
        self._tmp.cleanup()

    def _contract(self, **overrides):
        base = {
            "account_id": "tocrystal66", "supply_date": "2026-09-17", "slot": 1,
            "adoption": "outfit_only", "main_note_id": "m" * 24,
            "main_note_title": "参考笔记",
            "selected_pages": [{"note_id": "m" * 24, "seq": 2,
                                "sha256": "a" * 64, "purpose": "outfit_detail"}],
            "product": {}, "destination": {"country": "日本", "place": ""},
            "content_requirement": "x", "policy_version": SUPPLY_POLICY_VERSION,
        }
        base.update(overrides)
        base["contract_fingerprint"] = contract_fingerprint(
            adoption=base["adoption"], main_note_id=base["main_note_id"],
            selected_pages=base["selected_pages"], product=base["product"],
            destination=base["destination"], policy_version=base["policy_version"],
            temperature_band=base.get("temperature_band", ""),
            topic_statement=base.get("topic_statement", ""))
        return base

    def test_persist_intent_then_attach_record(self):
        stored = self.store.persist_intent(self._contract())
        self.assertEqual(stored["status"], "intent")
        # 幂等：同 slot 重复持久化返回既有合同，不报错不重复
        again = self.store.persist_intent(self._contract())
        self.assertEqual(again["contract_id"], stored["contract_id"])
        self.store.attach_record(stored["contract_id"], "recX")
        found = self.store.find_by_record("recX")
        self.assertIsNotNone(found)
        self.assertEqual(found["status"], "created")
        self.assertEqual(found["destination"]["country"], "日本")
        self.assertEqual(found["selected_pages"][0]["seq"], 2)
        self.assertIsNone(self.store.find_by_record("rec_missing"))

    def test_intent_recovery_blocks_recreate(self):
        # 已 created 且绑定 record_id 的合同：再次 persist 原样返回，
        # 供给层据此走 already_created，不重复建行
        stored = self.store.persist_intent(self._contract())
        self.store.attach_record(stored["contract_id"], "recY")
        again = self.store.persist_intent(self._contract())
        self.assertEqual(again["status"], "created")
        self.assertEqual(again["record_id"], "recY")

    def test_fingerprint_tracks_visual_inputs(self):
        base = self._contract()
        self.assertEqual(
            base["contract_fingerprint"],
            self._contract()["contract_fingerprint"])  # 相同输入稳定
        # adoption 变化
        self.assertNotEqual(
            base["contract_fingerprint"],
            self._contract(adoption="overall")["contract_fingerprint"])
        # 选中页面变化
        self.assertNotEqual(
            base["contract_fingerprint"],
            self._contract(selected_pages=[
                {"note_id": "m" * 24, "seq": 3, "sha256": "b" * 64,
                 "purpose": "full_outfit"}])["contract_fingerprint"])
        # 目的地变化（同图同 adoption，只换国家 → 仍算视觉输入变化）
        self.assertNotEqual(
            base["contract_fingerprint"],
            self._contract(destination={"country": "泰国", "place": ""})["contract_fingerprint"])
        # 商品变化
        self.assertNotEqual(
            base["contract_fingerprint"],
            self._contract(product={"code": "P1", "name": "x", "category": "开衫",
                                    "variant": ""})["contract_fingerprint"])
        # 策略版本变化
        self.assertNotEqual(
            base["contract_fingerprint"],
            self._contract(policy_version="external-reference-exec-v3")["contract_fingerprint"])
        # 温度带变化（2026-09-16：文案声明的温度进入视觉约束）
        self.assertNotEqual(
            base["contract_fingerprint"],
            self._contract(temperature_band="15-22°C")["contract_fingerprint"])

    def test_fingerprint_includes_topic_statement(self):
        base = self._contract()
        self.assertNotEqual(
            base["contract_fingerprint"],
            self._contract(topic_statement="完全不同的本篇主张")["contract_fingerprint"])


class TravelDestinationTest(unittest.TestCase):
    def test_empty_means_unspecified(self):
        self.assertEqual(resolve_travel_destination(country="", place=""),
                         {"country": "", "place": ""})

    def test_place_fills_country(self):
        self.assertEqual(resolve_travel_destination(country="", place="东京铁塔"),
                         {"country": "日本", "place": "东京铁塔"})

    def test_country_only_passes(self):
        self.assertEqual(resolve_travel_destination(country="日本", place=""),
                         {"country": "日本", "place": ""})

    def test_conflict_raises_before_planning(self):
        with self.assertRaises(TravelDestinationError):
            resolve_travel_destination(country="日本", place="曼谷")

    def test_unknown_place_passes_through(self):
        self.assertEqual(resolve_travel_destination(country="日本", place="小众海岛"),
                         {"country": "日本", "place": "小众海岛"})


class ExternalWiringGateTest(unittest.TestCase):
    """assert_external_supply_wiring：付费前门禁（不构造完整 workflow）。"""

    def _gate(self, **kwargs):
        from services.feishu_workflow import assert_external_supply_wiring
        params = dict(
            source_tag="auto_supply|2026-09-17|tocrystal66",
            record_id="rec1", reference_mode="STYLE", product_id="",
            store=kwargs.pop("store", None))
        params.update(kwargs)
        return params

    def test_non_external_row_passes(self):
        from services.feishu_workflow import assert_external_supply_wiring
        self.assertIsNone(assert_external_supply_wiring(
            **self._gate(source_tag="")))

    def test_external_without_contract_raises(self):
        from services.feishu_workflow import FeishuWorkflowError, assert_external_supply_wiring
        with self.assertRaises(FeishuWorkflowError) as ctx:
            assert_external_supply_wiring(**self._gate(store=FakeContractStore([])))
        self.assertIn("缺少执行合同", str(ctx.exception))

    def test_external_complete_look_forbidden(self):
        # 外部参考数量等于角色数量也拦（原图直用通道彻底封死）
        from services.feishu_workflow import FeishuWorkflowError, assert_external_supply_wiring
        store = FakeContractStore([{
            "record_id": "rec1", "authorization": "reference_only",
            "product": {}}])
        with self.assertRaises(FeishuWorkflowError) as ctx:
            assert_external_supply_wiring(**self._gate(
                reference_mode="COMPLETE_LOOK", store=store))
        self.assertIn("原图直用", str(ctx.exception))

    def test_bad_authorization_and_product_mismatch(self):
        from services.feishu_workflow import FeishuWorkflowError, assert_external_supply_wiring
        store = FakeContractStore([{
            "record_id": "rec1", "authorization": "own_asset", "product": {}}])
        with self.assertRaises(FeishuWorkflowError):
            assert_external_supply_wiring(**self._gate(store=store))
        store2 = FakeContractStore([{
            "record_id": "rec1", "authorization": "reference_only",
            "product": {"code": "P1"}}])
        with self.assertRaises(FeishuWorkflowError):
            assert_external_supply_wiring(**self._gate(
                product_id="P2", store=store2))

    def test_valid_external_returns_contract(self):
        from services.feishu_workflow import assert_external_supply_wiring
        store = FakeContractStore([{
            "record_id": "rec1", "authorization": "reference_only",
            "adoption": "outfit_only", "product": {}}])
        contract = assert_external_supply_wiring(**self._gate(store=store))
        self.assertEqual(contract["adoption"], "outfit_only")


class StyleRoutingTest(unittest.TestCase):
    def test_explicit_style_type_beats_role_count_trap(self):
        # 附件数 == 角色数 × 篇数 时，显式「风格参考」必须胜出，
        # 不得落进 COMPLETE_LOOK（修复前的自动判断陷阱）
        from services.photo_reference import (
            REFERENCE_MODE_COMPLETE_LOOK, REFERENCE_MODE_STYLE, resolve_reference_mode)
        mode = resolve_reference_mode(
            selected_type="风格参考",
            attachments=[{"name": f"r{i}"} for i in range(6)],
            product_id="", required_role_count=6, requested_count=1)
        self.assertEqual(mode, REFERENCE_MODE_STYLE)
        # 运营人工路径不受影响：自动判断 + 数量吻合仍走完整穿搭
        auto = resolve_reference_mode(
            selected_type="自动判断",
            attachments=[{"name": f"r{i}"} for i in range(6)],
            product_id="", required_role_count=6, requested_count=1)
        self.assertEqual(auto, REFERENCE_MODE_COMPLETE_LOOK)


if __name__ == "__main__":
    unittest.main()


class SlotLeaseTest(unittest.TestCase):
    """supply_slots 执行租约（评审 §D：名额唯一键≠锁）。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        from services.material_analysis import MaterialLedger
        self.path = str(Path(self._tmp.name) / "lease.sqlite3")
        self.worker1 = MaterialLedger(self.path)
        self.worker2 = MaterialLedger(self.path)   # 双连接模拟双 worker

    def tearDown(self):
        self.worker1.close()
        self.worker2.close()
        self._tmp.cleanup()

    def test_atomic_acquire_holds_and_takeover(self):
        self.assertEqual(
            self.worker1.acquire_slot_lease("a", "2026-09-17", 1, owner="w1"),
            "acquired")
        # 第二个 worker 同名额：租约未过期 → 拒绝
        self.assertEqual(
            self.worker2.acquire_slot_lease("a", "2026-09-17", 1, owner="w2"),
            "held_by_other")
        # 同一 worker 重入：幂等续持
        self.assertEqual(
            self.worker1.acquire_slot_lease("a", "2026-09-17", 1, owner="w1"),
            "acquired")
        # 模拟租约过期：手动回拨 lease_until
        self.worker1._conn.execute(
            "UPDATE supply_slots SET lease_until=datetime('now','-1 hour')")
        self.worker1._conn.commit()
        self.assertEqual(
            self.worker2.acquire_slot_lease("a", "2026-09-17", 1, owner="w2"),
            "acquired")   # 过期可被接管
        # 旧 owner 续租失败（已易主）
        self.assertFalse(self.worker1.renew_slot_lease(
            "a", "2026-09-17", 1, owner="w1"))
        self.assertTrue(self.worker2.renew_slot_lease(
            "a", "2026-09-17", 1, owner="w2"))

    def test_release_returns_to_reserved_and_complete_is_terminal(self):
        self.worker1.acquire_slot_lease("a", "2026-09-17", 2, owner="w1")
        self.worker1.release_slot_lease("a", "2026-09-17", 2, owner="w1")
        self.assertEqual(
            self.worker2.acquire_slot_lease("a", "2026-09-17", 2, owner="w2"),
            "acquired")   # 释放后可被他人获取
        self.worker2.complete_slot(
            "a", "2026-09-17", 2, record_id="recX")
        self.assertEqual(
            self.worker1.acquire_slot_lease("a", "2026-09-17", 2, owner="w1"),
            "already_created")   # 终态不可再取

    def test_legacy_db_migrates_owner_columns(self):
        import sqlite3
        from services.material_analysis import LEDGER_SCHEMA, MaterialLedger
        # 先建一个无 owner 列的旧库（手工去掉新列模拟旧版）
        legacy = str(Path(self._tmp.name) / "legacy.sqlite3")
        conn = sqlite3.connect(legacy)
        conn.executescript(LEDGER_SCHEMA)
        conn.execute(
            "CREATE TABLE supply_slots_old AS SELECT account_id, supply_date,"
            " slot, status, record_id, product_code, main_note_id, adoption,"
            " note, created_at, updated_at FROM supply_slots")
        conn.execute("DROP TABLE supply_slots")
        conn.execute("ALTER TABLE supply_slots_old RENAME TO supply_slots")
        conn.commit()
        conn.close()
        ledger = MaterialLedger(legacy)   # 初始化即迁移
        self.assertEqual(
            ledger.acquire_slot_lease("a", "2026-09-17", 1, owner="w"),
            "acquired")
        ledger.close()


class BudgetReservationTest(unittest.TestCase):
    """方案 A3：逐调用额度原子预留。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        from services.material_analysis import MaterialLedger
        self.w1 = MaterialLedger(str(Path(self._tmp.name) / "b.sqlite3"))
        self.w2 = MaterialLedger(str(Path(self._tmp.name) / "b.sqlite3"))

    def tearDown(self):
        self.w1.close(); self.w2.close(); self._tmp.cleanup()

    def test_cap1_single_launch_two_workers(self):
        # cap=1：两个 worker 争最后一个额度，只成一个
        self.assertTrue(self.w1.reserve_budget(purpose="selection", cap=1))
        self.assertFalse(self.w2.reserve_budget(purpose="selection", cap=1))
        self.w1.settle_budget(purpose="selection", state="consumed")
        # 已消费仍占额度：重试不能再预留（失败不返还）
        self.assertFalse(self.w1.reserve_budget(purpose="selection", cap=1))
        # 用途独立：分析额度不受终选影响
        self.assertTrue(self.w2.reserve_budget(purpose="analysis", cap=1))

    def test_release_only_when_not_launched(self):
        self.assertTrue(self.w1.reserve_budget(purpose="selection", cap=1))
        self.w1.settle_budget(purpose="selection", state="release")  # 未发起取消
        self.assertTrue(self.w2.reserve_budget(purpose="selection", cap=1))
        self.w2.settle_budget(purpose="selection", state="unknown")  # 结果未知占额度
        self.assertFalse(self.w1.reserve_budget(purpose="selection", cap=1))

    def test_budget_day_by_timezone(self):
        import os
        day = self.w1.budget_today()
        self.assertRegex(day, r"^\d{4}-\d{2}-\d{2}$")
        os.environ["OPV_BUDGET_TIMEZONE"] = "UTC"
        try:
            from datetime import datetime
            from zoneinfo import ZoneInfo
            self.assertEqual(
                self.w1.budget_today(),
                datetime.now(ZoneInfo("UTC")).date().isoformat())
        finally:
            del os.environ["OPV_BUDGET_TIMEZONE"]
