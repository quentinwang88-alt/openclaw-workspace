"""Phase 2 自动供稿测试：供给策略解析 + 供稿执行器（飞书客户端/模型全 mock）。"""
from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import field
from pathlib import Path

from services.auto_photo_supply import AutoPhotoSupply
from services.material_analysis import ANALYSIS_VERSION, MaterialLedger
from services.material_source import MaterialSource
from services.publish_account_profile import (
    SUPPLY_AUTOMATION_PRODUCE,
    PublishAccountBinding,
    normalize_profile_payload,
)

from tests.test_material_phase1 import LabFixture, _analysis_payload


class FixedSelectionClient:
    """不限次数的选材假客户端：总是选中给定主参考（可指定 adoption/页引用）。"""

    def __init__(self, main_note_id, adoption="overall", pages=None):
        self.main_note_id = main_note_id
        self.adoption = adoption
        self.pages = pages
        self.calls = 0
        self.last_prompt = ""

    def chat_with_multiple_images(self, paths, prompt, max_tokens):
        self.calls += 1
        self.last_prompt = prompt
        return {
            "choices": [{"message": {"content": json.dumps({
                "main_note_id": self.main_note_id,
                "supplement_note_ids": [],
                "adoption": self.adoption,
                "pages": self.pages or [],
                "rationale": "结构完整",
                "rejected": [],
            }, ensure_ascii=False)}}],
            "usage": {"prompt_tokens": 50, "completion_tokens": 10},
        }


class FakeTaskTableClient:
    """飞书任务表假客户端：记录建行/上传，支持预置已存在行。"""

    def __init__(self, existing=None):
        self.rows = list(existing or [])   # [{"record_id":..., "fields": {...}}]
        self.created = []
        self.uploads = []

    def list_records(self, page_size=500):
        class _Rec:
            def __init__(self, row):
                self.record_id = row.get("record_id", "")
                self.fields = row.get("fields", {})
        return [_Rec(row) for row in self.rows]

    def batch_create_records(self, records):
        self.created.extend(records)
        rid = f"rec_auto_{len(self.created)}"
        self.rows.append({"record_id": rid, "fields": dict(records[0]["fields"])})
        return [rid]

    def upload_attachment(self, content, name, content_type, size=None):
        self.uploads.append(name)
        return {"file_token": f"tok_{name}", "name": name,
                "size": size or len(content), "type": content_type}


def make_binding(account_id="tocrystal66", *, name="泰国女装1", profile_extra=None):
    profile = normalize_profile_payload({
        "default_theme": "旅行穿搭",
        "photo_supply_policy": {
            "content_strategy": "定位优先",
            "product_mode": "不指定商品",
            "automation": "自动生产",
            "daily_limit": 2,
            "preset": "图文｜TH｜旅行穿搭",
            **(profile_extra or {}),
        },
    })
    return PublishAccountBinding(
        account_id=account_id, account_name=name, store_id="THFZ01",
        target_country="TH", profile=profile, source="test")


def make_ledger_with_analysis(root: Path, source: MaterialSource, note_ids,
                              model="mock-model", name="ledger.sqlite3",
                              pages=3):
    ledger = MaterialLedger(str(root / name))
    for note_id in note_ids:
        package = source.get(note_id)
        ledger.put_cached_analysis(
            fingerprint=package.version_fingerprint, model=model,
            analysis_version=ANALYSIS_VERSION,
            note_id=note_id, result=_analysis_payload(pages=pages), image_count=2,
            calls=1, prompt_tokens=100, completion_tokens=20, duration_ms=10)
    return ledger


class SupplyPolicyTest(unittest.TestCase):
    def test_unconfigured_profile_unchanged(self):
        before = normalize_profile_payload({"default_theme": "旅行穿搭"})
        self.assertNotIn("photo_supply_policy", before)
        self.assertNotIn("photo_supply_policy", json.dumps(before, sort_keys=True))

    def test_configured_policy_normalized(self):
        profile = normalize_profile_payload({"photo_supply_policy": {
            "content_strategy": "参考优先", "product_mode": "使用指定商品",
            "product_codes": "P1，P2, P3", "automation": "自动生产并发布",
            "daily_limit": "3", "preset": "图文｜TH｜四选一穿搭",
        }})
        policy = profile["photo_supply_policy"]
        self.assertEqual(policy["content_strategy"], "reference_first")
        self.assertEqual(policy["product_mode"], "specified")
        self.assertEqual(policy["product_codes"], ["P1", "P2", "P3"])
        self.assertEqual(policy["automation"], "produce_publish")
        self.assertEqual(policy["daily_limit"], 3)
        # 浮点/带小数字符串安全取整（2026-09-16 review P1.2）
        float_policy = normalize_profile_payload({"photo_supply_policy": {
            "automation": "自动生产", "daily_limit": 2.0}})
        self.assertEqual(float_policy["photo_supply_policy"]["daily_limit"], 2)
        str_policy = normalize_profile_payload({"photo_supply_policy": {
            "automation": "自动生产", "daily_limit": "4.0"}})
        self.assertEqual(str_policy["photo_supply_policy"]["daily_limit"], 4)

    def test_binding_supply_policy_default_off(self):
        binding = PublishAccountBinding(
            account_id="a", account_name="a", store_id="s", target_country="TH")
        self.assertEqual(binding.supply_policy["automation"], "off")


class AutoPhotoSupplyTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.lab = LabFixture(self.root)
        self.lab.add_note(note_id="m" * 24, images=3, title="旅行穿搭笔记")
        self.lab.add_note(note_id="n" * 24, images=2, title="另一篇")
        self.note_ids = ["m" * 24, "n" * 24]

    def tearDown(self):
        self._tmp.cleanup()

    def _supply(self, client, ledger, vision=None, apply=True, product_resolver=None):
        from services.external_supply_contract import ExternalSupplyContractStore
        return AutoPhotoSupply(
            client=client, source=self.lab.source(), ledger=ledger,
            vision_client=vision, model="mock-model", today="2026-09-16",
            contract_store=ExternalSupplyContractStore(
                str(self.root / "contracts.sqlite3")),
            product_snapshot_resolver=product_resolver)

    def _vision_ok(self):
        return FixedSelectionClient(
            "m" * 24, pages=[{"seq": 1, "purpose": "full_outfit"},
                             {"seq": 2, "purpose": "outfit_detail"},
                             {"seq": 3, "purpose": "outfit_detail"}])

    @staticmethod
    def _fake_product_resolver(catalog=None):
        snapshots = catalog or {}

        def resolve(code):
            if code not in snapshots:
                raise RuntimeError(f"商品 {code} 无参考包")
            return snapshots[code]
        return resolve

    def test_creates_rows_with_contract_fields(self):
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        results = self._supply(client, ledger, vision=self._vision_ok()).run(
            [make_binding()], apply=True)
        self.assertEqual(results[0].status, "supplied")
        self.assertEqual(len(client.created), 2)   # daily_limit=2
        fields = client.created[0]["fields"]
        self.assertEqual(fields["生产预设"], "图文｜TH｜旅行穿搭")
        self.assertTrue(fields["执行"])
        self.assertEqual(fields["生成篇数"], 1)
        self.assertEqual(fields["目标账号（可选）"], "tocrystal66")
        self.assertEqual(fields["图文主题"], "旅行穿搭")
        self.assertEqual(fields["来源标记"], "auto_supply|2026-09-16|tocrystal66")
        self.assertTrue(fields["备注"].startswith("自动供稿"))
        # 来源分流（Phase 1）：外部参考走「参考图 + 风格参考」，
        # 绝不写「完整穿搭素材」字段（原图直用通道已封死）
        self.assertNotIn("完整穿搭素材（可选）", fields)
        self.assertEqual(fields["参考图类型"], "风格参考")
        self.assertEqual(len(fields["参考图（可选）"]), 3)
        self.assertGreater(len(client.uploads), 0)
        self.assertIn("采用方式 overall", fields["内容要求（可选）"])
        slots = ledger.slots_for("tocrystal66", "2026-09-16")
        self.assertEqual(len(slots), 2)
        self.assertTrue(all(s["status"] == "created" for s in slots))

    def test_external_contract_persisted_and_attached(self):
        from services.external_supply_contract import ExternalSupplyContractStore
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        self._supply(client, ledger, vision=self._vision_ok()).run(
            [make_binding()], apply=True)
        store = ExternalSupplyContractStore(str(self.root / "contracts.sqlite3"))
        contract = store.find_by_record(client.rows[0]["record_id"])
        self.assertIsNotNone(contract)
        self.assertEqual(contract["status"], "created")
        self.assertEqual(contract["adoption"], "overall")
        self.assertEqual(contract["authorization"], "reference_only")
        self.assertEqual(len(contract["selected_pages"]), 3)
        self.assertTrue(all(p.get("sha256") for p in contract["selected_pages"]))
        self.assertTrue(contract["contract_fingerprint"])

    def test_page_level_selection_limits_uploads(self):
        # 页级选材：只上传选材结果指定的页面，不再固定取前 N 张
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        vision = FixedSelectionClient(
            "m" * 24, pages=[{"seq": 2, "purpose": "outfit_detail"}])
        self._supply(client, ledger, vision=vision).run(
            [make_binding(profile_extra={"daily_limit": 1})], apply=True)
        fields = client.created[0]["fields"]
        self.assertEqual(len(fields["参考图（可选）"]), 1)
        self.assertIn("m" * 24 + "_2", client.uploads[0])

    def test_narrative_only_sends_no_original_images(self):
        # narrative_only：只借鉴结构，不发任何第三方原图作为生图参考
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        vision = FixedSelectionClient(
            "m" * 24, adoption="narrative_only",
            pages=[{"seq": 1, "purpose": "full_outfit"}])  # 模型违规带页 → 必须被清空
        self._supply(client, ledger, vision=vision).run(
            [make_binding(profile_extra={"daily_limit": 1})], apply=True)
        fields = client.created[0]["fields"]
        self.assertNotIn("参考图（可选）", fields)
        self.assertEqual(client.uploads, [])
        self.assertIn("narrative_only", fields["内容要求（可选）"])

    def test_topic_comes_from_selection_main_note(self):
        # 修复串配：选题/摘要来自模型终选主参考（n 篇），而不是初筛第一（m 篇）
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        vision = FixedSelectionClient("n" * 24, pages=[{"seq": 1, "purpose": "full_outfit"}])
        self._supply(client, ledger, vision=vision).run(
            [make_binding(profile_extra={"daily_limit": 1})], apply=True)
        requirement = client.created[0]["fields"]["内容要求（可选）"]
        self.assertIn("冬季旅游穿搭", requirement)   # _analysis_payload 的 note_topic

    def test_destination_writes_requirement_and_contract(self):
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        binding = make_binding(profile_extra={"daily_limit": 1})
        binding.profile["travel_country"] = "日本"
        self._supply(client, ledger, vision=self._vision_ok()).run(
            [binding], apply=True)
        requirement = client.created[0]["fields"]["内容要求（可选）"]
        self.assertIn("旅行目的地设定：日本", requirement)
        self.assertIn("不照搬参考图拍摄地", requirement)

    def test_specified_product_uses_real_summary(self):
        # 指定商品：复用商品解析器读真实品类/名称；资料缺失明确暂停，不建行
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        ok_resolver = self._fake_product_resolver({
            "P1": {"category": "开衫", "product_name": "奶白针织开衫",
                   "variant_key": "宽松款"}})
        self._supply(client, ledger, vision=self._vision_ok(),
                     product_resolver=ok_resolver).run(
            [make_binding(profile_extra={
                "product_mode": "使用指定商品", "product_codes": "P1",
                "daily_limit": 1})], apply=True)
        fields = client.created[0]["fields"]
        self.assertEqual(fields["产品编码"], "P1")
        self.assertIn("奶白针织开衫", fields["内容要求（可选）"])

        miss_client = FakeTaskTableClient()
        results = self._supply(miss_client, make_ledger_with_analysis(
            self.root, self.lab.source(), self.note_ids, name="ledger_miss.sqlite3"),
            vision=self._vision_ok(),
            product_resolver=self._fake_product_resolver({})).run(
            [make_binding(profile_extra={
                "product_mode": "使用指定商品", "product_codes": "P9",
                "daily_limit": 1})], apply=True)
        self.assertEqual(results[0].slots[0].status, "error")
        self.assertIn("商品资料缺失", results[0].slots[0].detail)
        self.assertEqual(miss_client.created, [])

    def test_marker_survives_notes_being_cleared(self):
        # 工作流接手行后会清空/覆写备注（feishu_workflow 启动时 FIELD_NOTES=""
        # 2026-09-16 review P1）；幂等标记在「来源标记」列，必须存续。
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        self._supply(client, ledger, vision=self._vision_ok()).run(
            [make_binding()], apply=True)
        self.assertEqual(len(client.created), 2)
        for row in client.rows:  # 模拟扫描器清空备注
            row["fields"]["备注"] = ""
        vision = self._vision_ok()
        results = self._supply(client, ledger, vision=vision).run(
            [make_binding()], apply=True)
        self.assertEqual(len(client.created), 2)   # 不重复建行
        self.assertEqual(vision.calls, 0)
        self.assertEqual(results[0].status, "limit_reached")

    def test_rerun_idempotent(self):
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        supply = self._supply(client, ledger, vision=self._vision_ok())
        supply.run([make_binding()], apply=True)
        first = len(client.created)
        # 第二轮：飞书对账发现已建 2 行（limit=2）→ 不再建行、不调模型
        vision = self._vision_ok()
        results = self._supply(client, ledger, vision=vision).run(
            [make_binding()], apply=True)
        self.assertEqual(len(client.created), first)
        self.assertEqual(results[0].status, "limit_reached")
        self.assertEqual(vision.calls, 0)

    def test_ledger_reconciliation_after_crash(self):
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        supply = self._supply(client, ledger, vision=self._vision_ok())
        supply.run([make_binding()], apply=True)
        # 模拟崩溃：台账 slot1 退回 reserved（飞书行已存在）
        ledger._conn.execute(
            "UPDATE supply_slots SET status='reserved', record_id=NULL"
            " WHERE slot=1")
        ledger._conn.commit()
        vision = self._vision_ok()
        self._supply(client, ledger, vision=vision).run(
            [make_binding()], apply=True)
        # 对账：飞书已有 2 行 → 不会重复建行
        self.assertEqual(len(client.created), 2)
        self.assertEqual(vision.calls, 0)

    def test_off_and_no_preset_accounts(self):
        ledger = MaterialLedger(str(self.root / "ledger2.sqlite3"))
        client = FakeTaskTableClient()
        off = PublishAccountBinding(
            account_id="x", account_name="x", store_id="s", target_country="TH")
        no_preset = make_binding(profile_extra={"preset": "", "daily_limit": 1})
        results = self._supply(client, ledger).run([off, no_preset], apply=True)
        self.assertEqual(results[0].status, "skipped_off")
        self.assertEqual(results[1].status, "skipped_no_preset")
        self.assertEqual(client.created, [])

    def test_reference_first_writes_topic_requirement(self):
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        binding = make_binding(profile_extra={
            "content_strategy": "参考优先", "daily_limit": 1})
        self._supply(client, ledger, vision=self._vision_ok()).run(
            [binding], apply=True)
        fields = client.created[0]["fields"]
        # STYLE 执行路径必须携带主题：参考优先也写图文主题（账号默认），
        # 选题摘要与参考图同源（模型终选主参考）
        self.assertEqual(fields["图文主题"], "旅行穿搭")
        self.assertTrue(
            fields["内容要求（可选）"].startswith("参考优先选题："))

    def test_product_rotation_thermal_gate(self):
        # 热学门禁（2026-09-16）：主题「旅行穿搭」→ 本篇温度带 15-22°C；
        # 棉服（5-12°C）失配被跳过，轮换顺延到下一个应季商品。
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        binding = make_binding(profile_extra={
            "product_mode": "使用指定商品", "product_codes": "P1,P2,P3",
            "daily_limit": 2})
        resolver = self._fake_product_resolver({
            "P1": {"category": "开衫", "product_name": "A", "variant_key": "v1"},
            "P2": {"category": "棉服", "product_name": "加厚棉服B", "variant_key": "v2"},
            "P3": {"category": "长袖针织", "product_name": "C", "variant_key": "v3"}})
        vision = self._vision_ok()
        self._supply(client, ledger, vision=vision,
                     product_resolver=resolver).run([binding], apply=True)
        codes = [row["fields"].get("产品编码") for row in client.created]
        self.assertEqual(codes, ["P1", "P3"])   # P2 棉服被热学门禁跳过
        # 温度带约束进入内容要求、行字段与终选 prompt（三处同源）
        self.assertIn("温度带 15–22°C", client.created[0]["fields"]["内容要求（可选）"])
        self.assertEqual(client.created[0]["fields"]["温度档"], "15°C 左右")
        self.assertIn("温度带约束", vision.last_prompt)

    def test_product_all_thermal_mismatch_skips_row(self):
        # 全部商品失配：记缺口不建行，不静默降级为自由搭配
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        binding = make_binding(profile_extra={
            "product_mode": "使用指定商品", "product_codes": "P1",
            "daily_limit": 1})
        resolver = self._fake_product_resolver({
            "P1": {"category": "棉服", "product_name": "蓬松棉服", "variant_key": ""}})
        results = self._supply(client, ledger, vision=self._vision_ok(),
                               product_resolver=resolver).run([binding], apply=True)
        self.assertEqual(results[0].slots[0].status, "no_material")
        self.assertIn("温度带", results[0].slots[0].detail)
        self.assertEqual(client.created, [])
        gaps = ledger._conn.execute(
            "SELECT reason FROM material_gaps").fetchall()
        self.assertTrue(any(r["reason"] == "no_thermal_match" for r in gaps))

    def test_no_material_records_gap(self):
        ledger = MaterialLedger(str(self.root / "ledger3.sqlite3"))  # 无分析缓存
        client = FakeTaskTableClient()
        results = self._supply(client, ledger, vision=FixedSelectionClient("m" * 24)).run(
            [make_binding()], apply=True)
        self.assertEqual(results[0].status, "no_material")
        self.assertEqual(client.created, [])
        gaps = ledger._conn.execute(
            "SELECT reason FROM material_gaps").fetchall()
        self.assertTrue(any(r["reason"] == "no_material" for r in gaps))

    def test_dry_run_makes_no_writes(self):
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        results = self._supply(client, ledger, vision=None).run(
            [make_binding()], apply=False)
        self.assertEqual(client.created, [])
        self.assertEqual(client.uploads, [])
        self.assertEqual(results[0].slots[0].status, "dry_run")
        slots = ledger.slots_for("tocrystal66", "2026-09-16")
        self.assertEqual(slots, [])


if __name__ == "__main__":
    unittest.main()
