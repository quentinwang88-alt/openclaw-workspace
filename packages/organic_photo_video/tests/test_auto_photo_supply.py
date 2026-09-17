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
                              pages=3, note_topic="冬季旅游穿搭"):
    ledger = MaterialLedger(str(root / name))
    for note_id in note_ids:
        package = source.get(note_id)
        ledger.put_cached_analysis(
            fingerprint=package.version_fingerprint, model=model,
            analysis_version=ANALYSIS_VERSION,
            note_id=note_id,
            result=_analysis_payload(pages=pages, note_topic=note_topic),
            image_count=2,
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
        self.assertEqual(fields["来源标记"],
                         "auto_supply|2026-09-16|tocrystal66|slot1")
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

    def test_reference_first_without_theme_derives_theme(self):
        # 参考优先 + 无默认主题：主题从选材结果推导（travel_two_step 等
        # 流程强制图文主题），并标注推导来源；温度带随推导主题生效
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        binding = make_binding(profile_extra={
            "content_strategy": "参考优先", "daily_limit": 1})
        binding.profile["default_theme"] = ""
        self._supply(client, ledger, vision=self._vision_ok()).run(
            [binding], apply=True)
        fields = client.created[0]["fields"]
        self.assertEqual(fields["图文主题"], "凉爽旅行")   # 推导主题
        self.assertIn("旅行选题：主题=凉爽旅行",
                      fields["内容要求（可选）"])
        self.assertIn("温度带 15–22°C", fields["内容要求（可选）"])
        self.assertEqual(fields["温度档"], "15°C 左右")

    def test_positioning_first_without_theme_derives_within_positioning(self):
        # B2：定位优先缺主题不再报错——按选题性质在定位内提炼
        # （旅行 fixture → 凉爽旅行；配色 fixture 走通用结构，同 B2 规则）
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        binding = make_binding(profile_extra={"daily_limit": 1})
        binding.profile["default_theme"] = ""
        results = self._supply(client, ledger, vision=self._vision_ok()).run(
            [binding], apply=True)
        slot = results[0].slots[0]
        self.assertEqual(slot.status, "created")
        fields = client.created[0]["fields"]
        self.assertEqual(fields["图文主题"], "凉爽旅行")   # 定位内提炼（旅行向定位+旅行选题）
        self.assertEqual(fields["生产预设"], "图文｜TH｜旅行穿搭")

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


    def test_frozen_intent_recovery_reuses_selection(self):
        # 评审 §D「冻结 A、执行 B」：崩溃后重跑必须复用冻结合同的选材输入，
        # 不重新调用模型（即使本轮模型会选另一篇）
        from services.external_supply_contract import ExternalSupplyContractStore
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        store = ExternalSupplyContractStore(str(self.root / "contracts.sqlite3"))
        # 手工冻结 intent：主参考=n（第二篇），页=seq1，adoption=outfit_only
        store.persist_intent({
            "account_id": "tocrystal66", "supply_date": "2026-09-16", "slot": 1,
            "adoption": "outfit_only", "main_note_id": "n" * 24,
            "main_note_title": "另一篇",
            "selected_pages": [{"note_id": "n" * 24, "seq": 1, "sha256": "x",
                                "purpose": "outfit_detail"}],
            "product": {}, "destination": {}, "temperature_band": "",
            "content_requirement": "冻结要求", "policy_version": "p",
            "contract_fingerprint": "frozen"})
        client = FakeTaskTableClient()
        vision = FixedSelectionClient(   # 本轮模型会选 m —— 不得生效
            "m" * 24, pages=[{"seq": 1, "purpose": "full_outfit"}])
        results = self._supply(client, ledger, vision=vision).run(
            [make_binding(profile_extra={"daily_limit": 1})], apply=True)
        slot = results[0].slots[0]
        self.assertEqual(slot.status, "created")
        self.assertEqual(slot.main_note_id, "n" * 24)   # 冻结的主参考
        self.assertEqual(slot.adoption, "outfit_only")
        self.assertEqual(vision.calls, 0)               # 未重新选材
        fields = client.created[0]["fields"]
        self.assertIn("n" * 24, client.uploads[0])      # 上传冻结页
        contract = store.find_by_record(slot.record_id)
        self.assertEqual(contract["adoption"], "outfit_only")

    def test_unrecoverable_frozen_intent_superseded(self):
        # 冻结主参考已不在候选（如被拒）：覆盖 intent 用新选材，不留「冻结A执行B」
        from services.external_supply_contract import ExternalSupplyContractStore
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        store = ExternalSupplyContractStore(str(self.root / "contracts.sqlite3"))
        store.persist_intent({
            "account_id": "tocrystal66", "supply_date": "2026-09-16", "slot": 1,
            "adoption": "overall", "main_note_id": "gone" * 8,
            "main_note_title": "已拒绝",
            "selected_pages": [], "product": {}, "destination": {},
            "temperature_band": "", "content_requirement": "旧要求",
            "policy_version": "p", "contract_fingerprint": "old"})
        client = FakeTaskTableClient()
        vision = FixedSelectionClient(
            "m" * 24, pages=[{"seq": 1, "purpose": "full_outfit"}])
        results = self._supply(client, ledger, vision=vision).run(
            [make_binding(profile_extra={"daily_limit": 1})], apply=True)
        slot = results[0].slots[0]
        self.assertEqual(slot.status, "created")
        self.assertEqual(slot.main_note_id, "m" * 24)   # 新选材生效
        contract = store.find_by_record(slot.record_id)
        self.assertEqual(contract["main_note_id"], "m" * 24)
        self.assertEqual(contract["status"], "created")

    def test_contract_persists_brief_and_topic(self):
        # 方案 §4/§6.4：effective_brief 快照与 topic_statement 冻结进合同
        from services.external_supply_contract import ExternalSupplyContractStore
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        self._supply(client, ledger, vision=self._vision_ok()).run(
            [make_binding(profile_extra={"daily_limit": 1})], apply=True)
        store = ExternalSupplyContractStore(str(self.root / "contracts.sqlite3"))
        contract = store.find_by_record(client.rows[0]["record_id"])
        brief = contract.get("effective_brief") or {}
        self.assertEqual(brief.get("account_id"), "tocrystal66")
        self.assertEqual(brief.get("content_strategy"), "positioning_first")
        self.assertEqual(brief.get("theme", {}).get("value"), "旅行穿搭")
        # B1：温度带必须带来源（主题预设的公开含义），不自动发明
        band = brief.get("temperature_band") or {}
        self.assertEqual(band.get("value"), "15-22°C")
        self.assertIn("主题预设", band.get("source") or "")
        self.assertTrue(contract.get("topic_statement"))

    def test_inventory_gate_caps_by_gap(self):
        # 库存门：缺口约束新增量（受 daily_limit 封顶）；显式 target=4 时
        # 存量 4 → inventory_full 不补
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        existing = [{"record_id": f"inv{i}", "fields": {
            "目标账号（可选）": "tocrystal66", "进度": "已完成"}} for i in range(3)]
        client = FakeTaskTableClient(existing=existing)
        results = self._supply(client, ledger, vision=self._vision_ok()).run(
            [make_binding()], apply=True)
        self.assertEqual(results[0].status, "supplied")
        self.assertEqual(len(client.created), 2)   # 缺口 9 → daily_limit=2 封顶

        full_client = FakeTaskTableClient(existing=[
            {"record_id": f"f{i}", "fields": {
                "目标账号（可选）": "tocrystal66", "进度": "待排班"}}
            for i in range(4)])
        full_binding = make_binding(profile_extra={
            "daily_limit": 2, "target_inventory": 4})
        results2 = self._supply(full_client, ledger, vision=self._vision_ok()).run(
            [full_binding], apply=True)
        self.assertEqual(results2[0].status, "inventory_full")
        self.assertEqual(full_client.created, [])

    def test_inventory_zero_states_ignored(self):
        # 已发布/需处理不计库存；该场景可正常建行
        ledger = make_ledger_with_analysis(
            self.root, self.lab.source(), self.note_ids, name="ledger_mixed.sqlite3")
        mixed = FakeTaskTableClient(existing=[
            {"record_id": "m1", "fields": {
                "目标账号（可选）": "tocrystal66", "进度": "已发布"}},
            {"record_id": "m2", "fields": {
                "目标账号（可选）": "tocrystal66", "进度": "需处理"}}])
        results = self._supply(mixed, ledger, vision=self._vision_ok()).run(
            [make_binding(profile_extra={"daily_limit": 1,
                                          "target_inventory": 4})], apply=True)
        self.assertEqual(results[0].status, "supplied")
        self.assertEqual(len(mixed.created), 1)

    def test_budget_exhausted_skips_account(self):
        import os
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        for _ in range(2):
            ledger.log_supply_call(purpose="analysis", model="m")
        os.environ["OPV_SUPPLY_DAILY_CALL_CAP"] = "2"
        try:
            client = FakeTaskTableClient()
            results = self._supply(client, ledger, vision=self._vision_ok()).run(
                [make_binding()], apply=True)
            self.assertEqual(results[0].status, "budget_exhausted")
            self.assertEqual(client.created, [])
        finally:
            del os.environ["OPV_SUPPLY_DAILY_CALL_CAP"]

    def test_budget_exhausted_skips_account(self):
        import os
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        for _ in range(2):
            ledger.log_supply_call(purpose="analysis", model="m")
        os.environ["OPV_SUPPLY_DAILY_CALL_CAP"] = "2"
        try:
            client = FakeTaskTableClient()
            results = self._supply(client, ledger, vision=self._vision_ok()).run(
                [make_binding()], apply=True)
            self.assertEqual(results[0].status, "budget_exhausted")
            self.assertEqual(client.created, [])
        finally:
            del os.environ["OPV_SUPPLY_DAILY_CALL_CAP"]

    def test_target_inventory_normalized(self):
        profile = normalize_profile_payload({"photo_supply_policy": {
            "automation": "自动生产", "target_inventory": "6"}})
        self.assertEqual(profile["photo_supply_policy"]["target_inventory"], 6)


    def test_a2_attach_failure_reconciled_next_run(self):
        # 方案 A2：行已建但 attach_record 失败（合同留 intent）→ 下轮 run()
        # 按标记 slot 段唯一匹配补绑，不重选不重建
        from services.external_supply_contract import ExternalSupplyContractStore
        store = ExternalSupplyContractStore(str(self.root / "contracts.sqlite3"))
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        # 第一次跑：劫持 attach_record 模拟绑定失败
        client = FakeTaskTableClient()
        broken = ExternalSupplyContractStore(str(self.root / "contracts.sqlite3"))
        orig_attach = broken.attach_record
        broken.attach_record = lambda cid, rid: (_ for _ in ()).throw(
            RuntimeError("attach 模拟失败"))
        supply1 = AutoPhotoSupply(
            client=client, source=self.lab.source(), ledger=ledger,
            vision_client=self._vision_ok(), model="mock-model",
            today="2026-09-16", contract_store=broken)
        supply1.run([make_binding(profile_extra={"daily_limit": 1})], apply=True)
        row = client.rows[-1]
        marker = row["fields"]["来源标记"]
        self.assertIn("slot1", marker)          # 标记带 slot 段
        self.assertEqual(broken.get(
            "tocrystal66|2026-09-16|1")["status"], "submitting")  # 已发未绑
        # 第二次跑（新 store，attach 正常）：对账回绑该行，不再新建
        client2 = FakeTaskTableClient(existing=[row])
        ledger2 = make_ledger_with_analysis(
            self.root, self.lab.source(), self.note_ids, name="ledger_r2.sqlite3")
        supply2 = AutoPhotoSupply(
            client=client2, source=self.lab.source(), ledger=ledger2,
            vision_client=FixedSelectionClient("m" * 24), model="mock-model",
            today="2026-09-16", contract_store=store)
        results = supply2.run([make_binding(profile_extra={"daily_limit": 1})], apply=True)
        self.assertEqual(len(client2.created), 0)         # 未重建
        contract = store.get("tocrystal66|2026-09-16|1")
        self.assertEqual(contract["status"], "created")   # 已补绑
        self.assertEqual(contract["record_id"], row["record_id"])
        self.assertEqual(results[0].status, "limit_reached")

    def test_a71_response_unknown_submitting_not_recreated(self):
        # 方案 §7.1：空响应→submitting 保持未知；查空后仍不重建、不重选
        from services.external_supply_contract import ExternalSupplyContractStore
        store = ExternalSupplyContractStore(str(self.root / "contracts.sqlite3"))
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)

        class EmptyCreateClient(FakeTaskTableClient):
            def batch_create_records(self, records):
                return []                      # 模拟响应未知/为空

        supply1 = AutoPhotoSupply(
            client=EmptyCreateClient(), source=self.lab.source(), ledger=ledger,
            vision_client=self._vision_ok(), model="mock-model",
            today="2026-09-16", contract_store=store)
        results = supply1.run([make_binding(profile_extra={"daily_limit": 1})], apply=True)
        slot = results[0].slots[0]
        self.assertEqual(slot.status, "submit_unknown")
        contract = store.get("tocrystal66|2026-09-16|1")
        self.assertEqual(contract["status"], "submitting")
        self.assertEqual(contract["main_note_id"], "m" * 24)   # 冻结输入在
        # 下轮（正常客户端但行确实不存在）：仍 submitting，不重建不重选
        client2 = FakeTaskTableClient()
        ledger2 = make_ledger_with_analysis(
            self.root, self.lab.source(), self.note_ids, name="ledger_r3.sqlite3")
        vision = FixedSelectionClient("n" * 24)
        supply2 = AutoPhotoSupply(
            client=client2, source=self.lab.source(), ledger=ledger2,
            vision_client=vision, model="mock-model",
            today="2026-09-16", contract_store=store)
        results2 = supply2.run([make_binding(profile_extra={"daily_limit": 1})], apply=True)
        self.assertEqual(len(client2.created), 0)             # 不重建
        self.assertEqual(vision.calls, 0)                      # 不重选
        self.assertEqual(store.get("tocrystal66|2026-09-16|1")["status"],
                         "submitting")

    def test_a71_submitting_row_found_binds_cross_day(self):
        # 方案 §7.1：行实际已创建（响应丢失）——跨日对账按合同自身日期补绑
        from services.external_supply_contract import ExternalSupplyContractStore
        store = ExternalSupplyContractStore(str(self.root / "contracts.sqlite3"))
        store.persist_intent({
            "account_id": "tocrystal66", "supply_date": "2026-09-15", "slot": 1,
            "adoption": "overall", "main_note_id": "m" * 24, "main_note_title": "t",
            "selected_pages": [], "product": {}, "destination": {},
            "temperature_band": "", "content_requirement": "x",
            "policy_version": "p", "contract_fingerprint": "f"})
        store.mark_submitting("tocrystal66|2026-09-15|1")
        # 行是昨天的标记（今天的扫描也必须看到它）
        yesterday_row = {"record_id": "rec_yd", "fields": {
            "来源标记": "auto_supply|2026-09-15|tocrystal66|slot1",
            "目标账号（可选）": "tocrystal66", "进度": "已完成"}}
        client = FakeTaskTableClient(existing=[yesterday_row])
        ledger = make_ledger_with_analysis(
            self.root, self.lab.source(), self.note_ids, name="ledger_xd.sqlite3")
        AutoPhotoSupply(
            client=client, source=self.lab.source(), ledger=ledger,
            vision_client=self._vision_ok(), model="mock-model",
            today="2026-09-16", contract_store=store).run(
            [make_binding(profile_extra={"daily_limit": 1})], apply=True)
        contract = store.get("tocrystal66|2026-09-15|1")
        self.assertEqual(contract["status"], "created")
        self.assertEqual(contract["record_id"], "rec_yd")

    def test_a2_marker_ambiguous_records_gap(self):
        # 同 slot 匹配多行：记异常不猜不删（gap reason=marker_ambiguous）
        from services.external_supply_contract import ExternalSupplyContractStore
        store = ExternalSupplyContractStore(str(self.root / "contracts.sqlite3"))
        store.persist_intent({
            "account_id": "tocrystal66", "supply_date": "2026-09-16", "slot": 1,
            "adoption": "overall", "main_note_id": "m" * 24, "main_note_title": "t",
            "selected_pages": [], "product": {}, "destination": {},
            "temperature_band": "", "content_requirement": "x",
            "policy_version": "p", "contract_fingerprint": "f"})
        dup_rows = [
            {"record_id": f"dup{i}", "fields": {
                "来源标记": "auto_supply|2026-09-16|tocrystal66|slot1",
                "目标账号（可选）": "tocrystal66", "进度": "已完成"}}
            for i in range(2)]
        client = FakeTaskTableClient(existing=dup_rows)
        ledger = make_ledger_with_analysis(
            self.root, self.lab.source(), self.note_ids, name="ledger_r4.sqlite3")
        AutoPhotoSupply(
            client=client, source=self.lab.source(), ledger=ledger,
            vision_client=self._vision_ok(), model="mock-model",
            today="2026-09-16", contract_store=store).run(
            [make_binding(profile_extra={"daily_limit": 1})], apply=True)
        gaps = ledger._conn.execute(
            "SELECT reason FROM material_gaps").fetchall()
        self.assertTrue(any(r["reason"] == "marker_ambiguous" for r in gaps))
        self.assertEqual(store.get("tocrystal66|2026-09-16|1")["status"], "intent")

    def test_a4_inventory_by_pieces(self):
        # 方案 A4 验收：素材生成1/4 篇数1 + 生成中 篇数3 = 库存4；
        # 已发布篇数3=0；空进度+执行=排队计篇数；未识别进度→库存未知
        P = AutoPhotoSupply._inventory_pieces
        f = lambda qty=None, **kw: {**({"生成篇数": qty} if qty else {}), **kw}
        self.assertEqual(P(f(), "素材生成 1/4", True), (1, True))
        self.assertEqual(P(f(3), "生成中", True), (3, True))
        self.assertEqual(P(f(3), "已发布", True), (0, True))
        self.assertEqual(P(f(2), "需处理", True), (0, True))
        self.assertEqual(P(f(2), "", True), (2, True))      # 待执行排队
        self.assertEqual(P(f(2), "", False), (0, True))     # 空置行不计
        self.assertEqual(P(f(), "奇怪的中间态", True), (0, False))  # 未知
        # 整表折算：跨日不清零（扫描无日期过滤）+ 篇数求和
        ledger = MaterialLedger(str(self.root / "ledger_a4.sqlite3"))
        client = FakeTaskTableClient(existing=[
            {"record_id": "a1", "fields": {"目标账号（可选）": "tocrystal66",
             "进度": "素材生成 1/4", "生成篇数": 1}},
            {"record_id": "a2", "fields": {"目标账号（可选）": "tocrystal66",
             "进度": "生成中", "生成篇数": 3}},
            {"record_id": "a3", "fields": {"目标账号（可选）": "tocrystal66",
             "进度": "已发布", "生成篇数": 2}},
        ])
        supply = AutoPhotoSupply(client=client, source=self.lab.source(),
                                 ledger=ledger, model="m", today="2026-09-16")
        inventory = supply._scan_task_rows()[1]
        self.assertEqual(inventory.get("tocrystal66"), 4)

    def test_b2_topic_statement_generalized(self):
        # 参考原题「42套」不得照搬：source_topic 保留原题，主张泛化
        from services.auto_photo_supply import _generalize_reference_topic
        self.assertEqual(_generalize_reference_topic("4⃣2️⃣套|韩剧女主穿搭"),
                         "4⃣2️⃣套|韩剧女主穿搭")  # emoji 数字不动
        self.assertIn("多套", _generalize_reference_topic("秋冬18套穿搭合集"))
        self.assertIn("组图", _generalize_reference_topic("入秋15图日常"))
        ledger = make_ledger_with_analysis(
            self.root, self.lab.source(), self.note_ids, name="ledger_b2.sqlite3")
        client = FakeTaskTableClient()
        self._supply(client, ledger, vision=self._vision_ok()).run(
            [make_binding(profile_extra={"daily_limit": 1})], apply=True)
        from services.external_supply_contract import ExternalSupplyContractStore
        contract = ExternalSupplyContractStore(
            str(self.root / "contracts.sqlite3")).find_by_record(
                client.rows[0]["record_id"])
        brief = contract.get("effective_brief") or {}
        self.assertIn("冬季旅游穿搭", brief.get("source_topic") or "")  # 原题保留
        self.assertNotIn("42", contract.get("topic_statement") or "")


    def test_b2_generic_structure_for_nontravel_topic(self):
        # B2：配色选题（无旅行词）→ 切通用预设+非旅行主题+无温度
        ledger = make_ledger_with_analysis(
            self.root, self.lab.source(), self.note_ids,
            name="ledger_b2g.sqlite3", note_topic="灰蓝配色穿搭的层次感")
        client = FakeTaskTableClient()
        binding = make_binding(profile_extra={
            "content_strategy": "参考优先", "daily_limit": 1})
        binding.profile["default_theme"] = ""
        self._supply(client, ledger, vision=self._vision_ok()).run(
            [binding], apply=True)
        fields = client.created[0]["fields"]
        self.assertEqual(fields["生产预设"], "图文｜TH｜四选一穿搭")  # 通用结构
        self.assertEqual(fields["图文主题"], "日常通勤")              # 结构性映射
        self.assertNotIn("温度档", fields)                            # 不发明温度
        self.assertNotIn("温度带", fields["内容要求（可选）"])
        self.assertIn("灰蓝配色穿搭的层次感", fields["内容要求（可选）"])  # 素材主张
        from services.external_supply_contract import ExternalSupplyContractStore
        contract = ExternalSupplyContractStore(
            str(self.root / "contracts.sqlite3")).find_by_record(
                client.rows[0]["record_id"])
        brief = contract.get("effective_brief") or {}
        self.assertEqual(brief["output"]["preset"], "图文｜TH｜四选一穿搭")
        self.assertEqual(brief["temperature_band"].get("value"), "")

    def test_b2_travel_topic_keeps_travel_preset(self):
        # 旅行选题（fixture 默认 冬季旅游穿搭）→ 原路径不变
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        binding = make_binding(profile_extra={
            "content_strategy": "参考优先", "daily_limit": 1})
        binding.profile["default_theme"] = ""
        self._supply(client, ledger, vision=self._vision_ok()).run(
            [binding], apply=True)
        fields = client.created[0]["fields"]
        self.assertEqual(fields["生产预设"], "图文｜TH｜旅行穿搭")
        self.assertEqual(fields["图文主题"], "凉爽旅行")
        self.assertEqual(fields["温度档"], "15°C 左右")


    def test_c4_zero_candidates_triggers_limited_refill(self):
        # 方案 C4：0 候选先补分析再判缺口（不再提前 return）
        calls = {"n": 0}
        root, lab, note_ids = self.root, self.lab, self.note_ids

        class RefillAnalyzer:
            def analyze_pending(self, limit=5):
                calls["n"] += 1
                calls["limit"] = limit
                # 模拟补分析后池子可用（写独立 ledger，不影响本供给台账）
                make_ledger_with_analysis(
                    root, lab.source(), note_ids,
                    name="ledger_c4refill.sqlite3")
                return []

        ledger = MaterialLedger(str(self.root / "ledger_c4.sqlite3"))  # 无缓存
        client = FakeTaskTableClient()
        supply = AutoPhotoSupply(
            client=client, source=self.lab.source(), ledger=ledger,
            vision_client=self._vision_ok(), analyzer=RefillAnalyzer(),
            model="mock-model", today="2026-09-16",
            contract_store=__import__(
                "services.external_supply_contract", fromlist=["x"]
            ).ExternalSupplyContractStore(str(self.root / "contracts.sqlite3")))
        results = supply.run([make_binding()], apply=True)
        # 注意：RefillAnalyzer 写的是另一个 ledger，本 ledger 仍无候选 →
        # 断言补分析被触发且缺口照记
        self.assertEqual(calls["n"], 1)
        self.assertEqual(calls["limit"], 5)
        self.assertEqual(results[0].status, "no_material",
                         msg=f"slots={results[0].slots} detail={results[0].detail}")
        self.assertEqual(results[0].slots[0].status, "no_material")

    def test_b2_destination_rotation_frozen_in_contract(self):
        # §3.2/§3.4：定位优先旅行主题→账号范围程序轮换进合同；
        # 重试（同合同恢复）不重新选址
        from services.external_supply_contract import ExternalSupplyContractStore
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        binding = make_binding(profile_extra={"daily_limit": 1})
        binding.profile["travel_destinations"] = [
            {"id": "东京", "country": "日本", "city": "东京", "label": "东京"},
            {"id": "首尔", "country": "韩国", "city": "首尔", "label": "首尔"}]
        self._supply(client, ledger, vision=self._vision_ok()).run(
            [binding], apply=True)
        fields = client.created[0]["fields"]
        self.assertEqual(fields["生产预设"], "图文｜TH｜旅行穿搭")   # 旅行 fixture
        contract = ExternalSupplyContractStore(
            str(self.root / "contracts.sqlite3")).find_by_record(
                client.rows[0]["record_id"])
        dest = contract.get("destination") or {}
        brief = contract.get("effective_brief") or {}
        self.assertIn(dest.get("place"), ("东京", "首尔"))          # 范围内轮换
        self.assertEqual(brief.get("destination_source"), "账号范围轮换")
        self.assertIn(dest.get("place"), fields.get("内容要求（可选）", ""))

    def test_b2_no_destination_no_invention(self):
        # 范围未配置：不发明地点（§2.1 留空原行为）
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        self._supply(client, ledger, vision=self._vision_ok()).run(
            [make_binding(profile_extra={"daily_limit": 1})], apply=True)
        from services.external_supply_contract import ExternalSupplyContractStore
        contract = ExternalSupplyContractStore(
            str(self.root / "contracts.sqlite3")).find_by_record(
                client.rows[0]["record_id"])
        self.assertFalse((contract.get("destination") or {}).get("country"))

    def test_d_matrix_explicit_destination_overrides_rotation(self):
        # 矩阵§9-2：本篇明确目的地（账号单值 travel_country）覆盖范围轮换；
        # 同名额重试（冻结合同）不换地点
        from services.external_supply_contract import ExternalSupplyContractStore
        ledger = make_ledger_with_analysis(self.root, self.lab.source(), self.note_ids)
        client = FakeTaskTableClient()
        binding = make_binding(profile_extra={"daily_limit": 1})
        binding.profile["travel_country"] = "首尔"          # 本篇/账号明确值
        binding.profile["travel_destinations"] = [
            {"id": "东京", "country": "日本", "city": "东京", "label": "东京"}]
        self._supply(client, ledger, vision=self._vision_ok()).run(
            [binding], apply=True)
        contract = ExternalSupplyContractStore(
            str(self.root / "contracts.sqlite3")).find_by_record(
                client.rows[0]["record_id"])
        dest = contract.get("destination") or {}
        brief = contract.get("effective_brief") or {}
        self.assertEqual(dest.get("country"), "韩国")       # 明确值优先
        self.assertEqual(brief.get("destination_source"), "账号单值")


if __name__ == "__main__":
    unittest.main()
