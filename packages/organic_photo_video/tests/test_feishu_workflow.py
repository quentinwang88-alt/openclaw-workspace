#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services.feishu_workflow import (  # noqa: E402
    FIELD_EXECUTE,
    FIELD_CONFIRM_PUBLISH,
    FIELD_PHOTO_REQUEST,
    FIELD_PROGRESS,
    FIELD_RETAKE_LOOK,
    FIELD_REVIEW,
    PROGRESS_DONE,
    PROGRESS_REVIEW,
    PROGRESS_SCHEDULED,
    FeishuTaskWorkflow, FeishuWorkflowError,
    ProductionPresetCatalog,
    dependent_redo_slots,
    parse_retake_roles,
    quantity_value,
    text_value,
)


class CatalogTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "presets.json"
        task = {
            "account_id": "A", "market": "TH", "language": "th-TH",
            "recipe_id": "R", "theme_id": "T", "hook_strategy": "H",
            "persona_ref": "P", "look_ref": "L", "scene_ref": "S",
        }
        self.path.write_text(json.dumps({
            "default_overlay_profile_id": "OVERLAY_LIGHT_V1",
            "presets": [
                {"name": "固定", "tasks": [task]},
                {"name": "随机", "selection": "deterministic_one", "tasks_from": ["固定"]},
            ],
        }), encoding="utf-8")

    def tearDown(self):
        self.temp.cleanup()

    def test_fixed_and_deterministic_presets(self):
        catalog = ProductionPresetCatalog(self.path)
        self.assertEqual(catalog.names, ["固定", "随机"])
        self.assertEqual(catalog.resolve("固定", "rec1"), catalog.resolve("随机", "rec1"))
        self.assertEqual(catalog.overlay_profile_id, "OVERLAY_LIGHT_V1")
        self.assertEqual(len(catalog.resolve_batch("固定", "rec1", 3)), 3)
        self.assertEqual(len(catalog.resolve_batch("随机", "rec1", 3)), 3)
        fixed_batch = catalog.resolve_batch("固定", "rec1", 3)
        self.assertEqual(fixed_batch[0].hook_strategy, "H")
        self.assertEqual(fixed_batch[1].hook_strategy, "")

    def test_unknown_preset_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "未知生产预设"):
            ProductionPresetCatalog(self.path).resolve("不存在", "rec1")

    def test_random_entry_always_uses_current_multi_look_route(self):
        catalog = ProductionPresetCatalog()
        for record in ("rec1", "rec2", "recvufx3c1c27d"):
            specs = catalog.resolve_batch("TH｜随机养号组合｜轻文字", record, 9)
            self.assertEqual({spec.recipe_id for spec in specs}, {"RECIPE_MULTI_LOOK_V1"})
            self.assertTrue(all(not spec.look_ref for spec in specs))
        preview = catalog.preview("TH｜随机养号组合｜轻文字", "rec1", 2)
        self.assertEqual(preview["routing_policy"], "multi_look_6s_v2")
        self.assertEqual(preview["count_status"], "requested_not_yet_resolved")
        self.assertEqual(preview["videos"][0]["target_duration_ms"], 6000)
        self.assertEqual(preview["videos"][0]["requested_look_count"], 5)

    def test_production_note_reports_actual_frozen_counts_and_timing(self):
        from types import SimpleNamespace
        plan = {"outfit_sequence": [{}, {}, {}],
                "shots": [{"duration_ms": ms} for ms in (1600, 2200, 2200)]}
        note = FeishuTaskWorkflow._production_plan_note([SimpleNamespace(plan_json=plan)])
        self.assertIn("3套穿搭/3页/6秒", note)
        self.assertNotIn("5套", note)

    def test_shipped_catalog_has_automatic_look_preset(self):
        catalog = ProductionPresetCatalog(
            PACKAGE_ROOT / "config" / "feishu_production_presets.json"
        )
        batch = catalog.resolve_batch("TH｜自动匹配穿搭组合｜轻文字", "rec1", 3)
        self.assertEqual(len(batch), 3)
        self.assertTrue(all(spec.look_ref == "" for spec in batch))

    def test_shipped_catalog_retires_outfit_breakdown_preset(self):
        """2026-09-15 选项精简：0 行使用的拆解首图预设已停用（隐藏）。

        停用语义：不可再被新行解析（NEEDS_CONTENT），且不出现在目录
        ``names``（ensure 不再播种该下拉选项）；历史行/冻结批次不受影响，
        将来需要时把 ``status`` 改回 active 并重跑字段脚本即可恢复。
        """
        catalog = ProductionPresetCatalog(
            PACKAGE_ROOT / "config" / "feishu_production_presets.json"
        )
        self.assertNotIn("TH｜穿搭拆解首图｜均衡变体", catalog.names)
        with self.assertRaises(FeishuWorkflowError) as ctx:
            catalog.resolve_batch("TH｜穿搭拆解首图｜均衡变体", "rec1", 4)
        self.assertIn("已停用", str(ctx.exception))
        self.assertEqual(
            catalog.metadata("TH｜穿搭拆解首图｜均衡变体").get("status"),
            "disabled",
        )

    def test_shipped_catalog_exposes_native_photo_presets(self):
        path = PACKAGE_ROOT / "config" / "feishu_production_presets.json"
        catalog = ProductionPresetCatalog(path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        photo_presets = [
            item for item in payload["presets"]
            if item.get("media_kind") == "native_photo"
        ]
        # 2026-09-13: the retired temperature-layering preset was dropped with
        # its recipe, leaving the disabled daily hot→cold transition preset as
        # the eighth native-photo entry.
        # 2026-09-13 (VN scarf Phase 4): +2 disabled VN scarf presets (travel +
        # daily matching) = 10 entries, and the scarf category joins the set.
        # 2026-09-15 (选项精简): 图文小个子 + MX 三个未用发型线转 disabled
        # （0 行使用；恢复=改回 active 重跑字段脚本）。当前启用中的原生图文
        # 预设 = TH 旅行 / TH 四选一 / MX 四选一发型。
        self.assertEqual(len(photo_presets), 10)
        self.assertEqual(
            {item["category_key"] for item in photo_presets},
            {"womenswear", "wig", "scarf"},
        )
        expected_routes = {
            "图文｜TH｜四选一穿搭": (
                "native_photo_product_supply_v1", "REUSE_THEN_GENERATE_MISSING",
            ),
            "图文｜TH｜旅行穿搭": (
                "native_photo_style_plan_v1", "REUSE_THEN_GENERATE_MISSING",
            ),
            "图文｜VN｜围巾旅行": (
                "native_photo_style_plan_v1", "REUSE_THEN_GENERATE_MISSING",
            ),
            "图文｜VN｜围巾搭配四选一": (
                "native_photo_product_supply_v1", "REUSE_THEN_GENERATE_MISSING",
            ),
        }
        recipe_ids = set()
        for item in photo_presets:
            expected_route, expected_assets = expected_routes.get(
                item["name"], ("native_photo_v1", "ASSET_REUSE"),
            )
            self.assertEqual(item["routing_policy"], expected_route)
            self.assertEqual(item["default_product_mode"], "NO_PRODUCT")
            self.assertEqual(item["default_asset_mode"], expected_assets)
            if item.get("status") == "disabled":
                self.assertNotIn(item["name"], catalog.names)
                with self.assertRaisesRegex(FeishuWorkflowError, "NEEDS_CONTENT"):
                    catalog.resolve(item["name"], "photo-config-test")
                continue
            resolved = catalog.resolve(item["name"], "photo-config-test")
            self.assertEqual(len(resolved), 1)
            self.assertTrue(catalog.is_native_photo(item["name"]))
            self.assertEqual(catalog.metadata(item["name"])["routing_policy"], expected_route)
            recipe_ids.add(resolved[0].recipe_id)
        self.assertEqual(len(recipe_ids), 3)
        self.assertIn("PHOTO_TH_TRAVEL_OUTFIT_V2", recipe_ids)

    def test_photo_plan_note_never_reports_video_duration(self):
        from types import SimpleNamespace
        note = FeishuTaskWorkflow._production_plan_note([
            SimpleNamespace(media_kind="native_photo", plan_json={"slides": [{}, {}, {}, {}, {}]})
        ])
        self.assertIn("5张原生图文", note)
        self.assertNotIn("秒", note)


class FieldParsingTest(unittest.TestCase):
    def test_photo_request_field_name_is_stable(self):
        self.assertEqual(FIELD_PHOTO_REQUEST, "图文任务JSON")

    def test_text_value_accepts_feishu_rich_text(self):
        self.assertEqual(text_value([{"text": "173"}, {"text": "42"}]), "17342")
        self.assertEqual(text_value(None), "")

    def test_quantity_defaults_and_validates(self):
        self.assertEqual(quantity_value(None), 1)
        self.assertEqual(quantity_value(3.0), 3)
        with self.assertRaisesRegex(RuntimeError, "1 到 9"):
            quantity_value(10)

    def test_action_prioritizes_review_and_ignores_completed(self):
        self.assertEqual(
            FeishuTaskWorkflow._action({FIELD_EXECUTE: True}), "generate"
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({FIELD_EXECUTE: True, FIELD_REVIEW: "重做P3"}),
            "redo_3",
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({FIELD_REVIEW: "通过"}), "approve"
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({
                FIELD_REVIEW: "待审核", FIELD_PROGRESS: PROGRESS_REVIEW,
            }),
            "",
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({FIELD_REVIEW: "通过", FIELD_PROGRESS: PROGRESS_DONE}),
            "",
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({FIELD_REVIEW: "排期发布", FIELD_PROGRESS: PROGRESS_DONE}),
            "schedule",
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({
                FIELD_CONFIRM_PUBLISH: True, FIELD_PROGRESS: PROGRESS_DONE,
            }),
            "confirm_publish",
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({
                FIELD_EXECUTE: True, FIELD_PROGRESS: PROGRESS_DONE,
            }),
            "",
        )

    def test_redoing_outfit_anchor_invalidates_the_complete_group(self):
        plan = {
            "anchor_slot": 2,
            "recipe_execution": {"content_goal": "outfit_breakdown"},
        }
        self.assertEqual(dependent_redo_slots(plan, [2]), [1, 2, 3, 4, 5])
        self.assertEqual(dependent_redo_slots(plan, [1]), [1])
        self.assertEqual(dependent_redo_slots({"anchor_slot": 2}, [2]), [2])


class PublicationProjectionTest(unittest.TestCase):
    def test_stopped_retry_makes_partial_batch_visible(self):
        progress, note = FeishuTaskWorkflow._publication_projection([
            {"status": "已排期", "account_name": "账号A", "planned_publish_at": "2026-09-05 13:25:00"},
            {"status": "待排班", "error_message": "已停止自动重试（3/3）：上传失败"},
        ])
        self.assertEqual(progress, "发布失败")
        self.assertIn("需处理1", note)
        self.assertIn("已停止自动重试", note)

    def test_retrying_is_counted_without_being_reported_as_failure(self):
        progress, note = FeishuTaskWorkflow._publication_projection([
            {"status": "待排班", "error_message": "等待自动重试（1/3，下次 2026-09-05 10:00:00）：网络失败"},
        ])
        self.assertEqual(progress, "待排班")
        self.assertIn("重试中1", note)

    def test_only_complete_batch_is_published(self):
        progress, note = FeishuTaskWorkflow._publication_projection([
            {"status": "已发布", "account_name": "账号A"},
            {"status": "已发布", "account_name": "账号B"},
        ])
        self.assertEqual(progress, "已发布")
        self.assertIn("已发布2", note)

    def test_submitting_is_never_reported_as_pending_schedule(self):
        """「提交中」是在途占位，不是待排班；曾整批被误报成待排班。"""
        progress, note = FeishuTaskWorkflow._publication_projection([
            {"status": "提交中", "account_name": "泰国养号视频-2",
             "planned_publish_at": "2026-09-12 15:00:00",
             "error_message": "提交结果不明；仅对账，禁止自动重发：本地命令解析失败"},
        ])
        self.assertEqual(progress, "提交中")
        self.assertIn("提交中1", note)
        self.assertNotIn("待排班1", note)

    def test_submitting_outranks_scheduled_in_mixed_batch(self):
        progress, note = FeishuTaskWorkflow._publication_projection([
            {"status": "已排期", "account_name": "账号A"},
            {"status": "提交中", "account_name": "账号B"},
        ])
        self.assertEqual(progress, "提交中")
        self.assertIn("已排期1", note)
        self.assertIn("提交中1", note)

    def test_confirmed_failure_still_outranks_submitting(self):
        progress, _ = FeishuTaskWorkflow._publication_projection([
            {"status": "提交中"},
            {"status": "发布失败"},
        ])
        self.assertEqual(progress, "发布失败")


class ScanTest(unittest.TestCase):
    def test_dry_run_existing_frozen_batch_keeps_legacy_recipe(self):
        from types import SimpleNamespace
        class Repository:
            def get_production_batch(self, record_id):
                return SimpleNamespace(manifest_json={"entries": [{"spec": {"recipe_id": "RECIPE_SCENE_SOLUTION_V1"}}]})
        class Client:
            def list_records(self, page_size=500):
                return [SimpleNamespace(record_id="legacy", fields={FIELD_EXECUTE: True})]
        runner = FeishuTaskWorkflow(repository=Repository(), client=Client(),
            generator=object(), renderer=object())
        report = runner.scan(dry_run=True)
        preview = report["processed"][0]["production_preview"]
        self.assertEqual(preview["source"], "frozen_batch")
        self.assertEqual(preview["recipe_ids"], ["RECIPE_SCENE_SOLUTION_V1"])

    class Record:
        record_id = "rec1"
        fields = {FIELD_EXECUTE: False}

    class Client:
        def list_records(self, page_size=500):
            assert page_size == 500
            return [ScanTest.Record()]

    def test_blank_scan_is_noop(self):
        runner = FeishuTaskWorkflow(
            repository=object(), client=self.Client(),
            catalog=object(), generator=object(), renderer=object(),
        )
        report = runner.scan(dry_run=True)
        self.assertEqual(report["scanned"], 1)
        self.assertEqual(report["eligible"], 0)

    def test_explicit_resume_recovers_only_selected_running_record(self):
        class RunningRecord:
            record_id = "rec_running"
            fields = {FIELD_EXECUTE: False, FIELD_PROGRESS: "生成中"}

        class Client:
            def get_record(self, record_id):
                self.record_id = record_id
                return RunningRecord()

        client = Client()
        runner = FeishuTaskWorkflow(
            repository=object(), client=client,
            catalog=object(), generator=object(), renderer=object(),
        )
        report = runner.scan(
            dry_run=True, record_id="rec_running", resume_running=True
        )
        self.assertEqual(report["eligible"], 1)
        self.assertEqual(report["processed"][0]["action"], "generate")
        self.assertEqual(client.record_id, "rec_running")


class PlannedCopyContractTest(unittest.TestCase):
    """不合规的机器文案必须在**任何付费素材生成之前**拦下。"""

    OVER_LIMIT_TITLE = (
        "วัดโทไดจิ ฤดูใบไม้ร่วง ใส่ยังไงถ่ายรูปสวย? "
        "4 ลุคฝรั่งเศสวินเทจ กางเกง หรือ กระโปรง เลือกได้"
    )

    def setUp(self):
        self.runner = FeishuTaskWorkflow(
            repository=object(), client=object(),
            catalog=object(), generator=object(), renderer=object(),
        )

    @staticmethod
    def _units(value):
        return len(str(value).encode("utf-16-le")) // 2

    def test_over_limit_model_title_is_clamped_in_place_before_paid_work(self):
        variations = [{"copy": {
            "title": self.OVER_LIMIT_TITLE, "caption": "สั้น", "hashtags": ["#a"],
        }}]
        self.runner._assert_planned_copy_contract(variations)
        self.assertLessEqual(self._units(variations[0]["copy"]["title"]), 90)
        self.assertEqual(variations[0]["copy"]["caption"], "สั้น")

    def test_compliant_or_copyless_variations_are_left_alone(self):
        variations = [
            {"copy": {"title": "สั้น", "caption": "c", "hashtags": ["#a"]}},
            {"angle_zh": "完全穿搭不产出模型文案"},
            {},
        ]
        self.runner._assert_planned_copy_contract(variations)
        self.assertEqual(variations[0]["copy"]["title"], "สั้น")

    def test_unclampable_copy_fails_loudly_but_for_free(self):
        variations = [{"copy": {"title": "t", "caption": "c", "hashtags": ["no-dash"]}}]
        with self.assertRaisesRegex(FeishuWorkflowError, "未开始付费生图"):
            self.runner._assert_planned_copy_contract(variations)
        with self.assertRaisesRegex(FeishuWorkflowError, "未开始付费生图"):
            self.runner._assert_planned_copy_contract([{"copy": {"title": ""}}])


class RetakeRolesTest(unittest.TestCase):
    def test_parse_accepts_letters_full_names_and_cjk_commas(self):
        self.assertEqual(parse_retake_roles("C"), ["look_c"])
        self.assertEqual(parse_retake_roles("C，D"), ["look_c", "look_d"])
        self.assertEqual(parse_retake_roles("A、d"), ["look_a", "look_d"])
        self.assertEqual(parse_retake_roles("look_b"), ["look_b"])
        self.assertEqual(parse_retake_roles(" E "), [])
        self.assertEqual(parse_retake_roles(None), [])
        self.assertEqual(parse_retake_roles(""), [])

    def test_action_allows_retake_from_completed_but_not_from_publish_queue(self):
        self.assertEqual(
            FeishuTaskWorkflow._action({
                FIELD_EXECUTE: True, FIELD_RETAKE_LOOK: "C", FIELD_PROGRESS: PROGRESS_DONE,
            }),
            "generate",
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({
                FIELD_EXECUTE: True, FIELD_RETAKE_LOOK: "C", FIELD_PROGRESS: PROGRESS_SCHEDULED,
            }),
            "",
        )
        self.assertEqual(
            FeishuTaskWorkflow._action({
                FIELD_EXECUTE: True, FIELD_PROGRESS: PROGRESS_DONE,
            }),
            "",
        )


class FakeRetakeTask:
    def __init__(self, task_id="opv_task_1", status="photo_packaging"):
        import types
        self.task_id = task_id
        self.task_status = status
        self.workflow_version = 2
        self.active_revision_id = "revision_1"
        self.row_version = 1
        self.source_record_id = "rec1:1:RECIPE:1"
        self.media_kind = "native_photo"
        self.plan_json = {}


class RetakeWorkflowTest(unittest.TestCase):
    """手动重拍 Look：供给重生 + photo_source 返工修订换绑页面素材。"""

    def setUp(self):
        import types
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.supply_dir = self.root / "style_reference_supply" / "rec1_item_1"
        self.supply_dir.mkdir(parents=True)
        self.old_image = self.supply_dir / "old_c.png"
        self.old_image.write_bytes(b"old-c")
        self.new_image = self.supply_dir / "new_c.png"
        self.new_image.write_bytes(b"brand-new-c")
        self.reference = self.root / "reference.jpg"
        self.reference.write_bytes(b"reference")
        import hashlib
        old_sha = hashlib.sha256(self.old_image.read_bytes()).hexdigest()
        new_sha = hashlib.sha256(self.new_image.read_bytes()).hexdigest()
        self.manifest = {
            "input_hash": "hash123",
            "style_reference_paths": [str(self.reference)],
            "sources": [
                {"role": "look_a", "path": str(self.root / "a.png"), "sha256": "aa"},
                {"role": "look_b", "path": str(self.root / "b.png"), "sha256": "bb"},
                {"role": "look_c", "path": str(self.old_image), "sha256": old_sha},
                {"role": "look_d", "path": str(self.root / "d.png"), "sha256": "dd"},
            ],
        }
        (self.supply_dir / "supply_manifest.json").write_text(
            json.dumps(self.manifest, ensure_ascii=False), encoding="utf-8")
        shots = [
            {"slot_index": 1, "shot_kind": "reused_asset",
             "asset_path": str(self.root / "a.png"), "asset_sha256": "aa"},
            {"slot_index": 2, "shot_kind": "reused_asset",
             "asset_path": str(self.root / "b.png"), "asset_sha256": "bb"},
            {"slot_index": 3, "shot_kind": "reused_asset",
             "asset_path": str(self.old_image), "asset_sha256": old_sha},
            {"slot_index": 4, "shot_kind": "reused_asset",
             "asset_path": str(self.root / "d.png"), "asset_sha256": "dd"},
        ]
        self.plan = {"shots": shots, "workflow_version": 2}
        selected = {f"shot:{i}": f"shot_{i}" for i in range(1, 5)}
        selected.update({f"slide:{i}": f"slide_{i}" for i in range(1, 6)})
        candidates = {f"shot_{i}": {"asset_id": f"shot_{i}", "path": f"/tmp/{i}.png",
                                    "sha256": f"sha{i}"} for i in range(1, 5)}
        self.revision = types.SimpleNamespace(
            revision_id="revision_1", task_id="opv_task_1", lock_version=3,
            revision_status="working",
            plan_snapshot_json={"plan": json.loads(json.dumps(self.plan))},
            asset_manifest_json={"selected": selected, "candidates": candidates},
            parent_revision_id=None, rework_spec_json={},
        )
        self.task = FakeRetakeTask()

        class Repository:
            def __init__(self, outer):
                self.outer = outer

            def get_production_batch(self, record_id):
                return None

            def list_tasks_by_source_prefix(self, source_type, prefix):
                assert prefix.startswith("rec1")
                return [self.outer.task]

            def get_account_profile(self, account_id):
                import types as t
                return t.SimpleNamespace(persona_ref_id=None, account_id=account_id)

            def get_task(self, task_id):
                return self.outer.task

            def transition_task(self, task_id, from_status, to_status, **kwargs):
                assert self.outer.task.task_status == from_status
                self.outer.task.task_status = to_status
                return self.outer.task

            def get_task_revision(self, revision_id):
                return self.outer.revision

            def list_task_revisions(self, task_id):
                return [self.outer.revision]

            def create_revision_and_activate(self, revision, *, expected_task_row_version,
                                             transition_to=None):
                assert self.outer.task.row_version == expected_task_row_version
                self.outer.revision = revision
                self.outer.task.active_revision_id = revision.revision_id
                self.outer.task.plan_json = json.loads(json.dumps(
                    revision.plan_snapshot_json["plan"]))
                self.outer.task.row_version += 1
                if transition_to:
                    self.outer.task.task_status = transition_to

        class Client:
            def __init__(self):
                self.writes = []

            def update_record_fields(self, record_id, fields):
                self.writes.append(dict(fields))

        self.repository = Repository(self)
        self.client = Client()
        import types as types_mod
        self.entries = [{"request": {"account_id": "acc1"}, "source_record_id": self.task.source_record_id}]
        self.batch = types_mod.SimpleNamespace(
            manifest_json={"content_plan": {"items": [{}]}})

    def build_workflow(self):
        from unittest import mock
        runner = FeishuTaskWorkflow(
            repository=self.repository, client=self.client,
            generator=mock.Mock(), renderer=mock.Mock(),
            product_reference_resolver=mock.Mock(),
        )
        return runner

    def fake_supply(self, prepared_sources):
        from unittest import mock
        service = mock.Mock()
        service.input_fingerprint.return_value = self.manifest["input_hash"]
        service.regenerate_roles.return_value = {"roles": ["look_c"]}
        service.prepare.return_value = {"sources": prepared_sources}
        return service

    def test_retake_rebinds_only_changed_roles_and_clears_field(self):
        """真实场景：重生图同名覆盖原路径，只有内容哈希变化，也必须换绑。"""
        from unittest import mock
        runner = self.build_workflow()
        prepared = [
            {"role": "look_a", "path": str(self.root / "a.png"), "sha256": "aa"},
            {"role": "look_b", "path": str(self.root / "b.png"), "sha256": "bb"},
            {"role": "look_c", "path": str(self.old_image), "sha256": "cc-new"},
            {"role": "look_d", "path": str(self.root / "d.png"), "sha256": "dd"},
        ]
        supply = self.fake_supply(prepared)
        producer = mock.Mock()
        import types as types_mod
        def fake_produce(task_id):
            self.task.task_status = "image_review"
            return types_mod.SimpleNamespace(task_status="image_review")
        producer.produce.side_effect = fake_produce
        with mock.patch("services.release_gate.assert_main_queue_rework_allowed"), \
                mock.patch("services.photo_style_reference_supply."
                           "PhotoStyleReferenceSupplyService", return_value=supply):
            runner._retake_photo_supply_roles(
                types_mod.SimpleNamespace(record_id="rec1", fields={}), self.batch,
                self.entries, ["look_c"], self.root, producer)
        # 只摘除 look_c；prepare 按原参考路径断点续跑。
        self.assertEqual(supply.regenerate_roles.call_args.kwargs["roles"], ["look_c"])
        self.assertEqual(supply.prepare.call_args.kwargs["reference_paths"],
                         [str(self.reference)])
        self.assertEqual(producer.produce.call_count, 1)
        self.assertEqual(self.task.task_status, "image_review")
        self.assertIn({FIELD_RETAKE_LOOK: ""}, self.client.writes)
        # 同名覆盖：路径不变、哈希换绑为新内容，其余槽位不动。
        new_plan = self.task.plan_json
        self.assertEqual(new_plan["shots"][2]["asset_path"], str(self.old_image))
        self.assertEqual(new_plan["shots"][2]["asset_sha256"], "cc-new")
        self.assertEqual(new_plan["shots"][0]["asset_path"], str(self.root / "a.png"))
        self.assertEqual(new_plan["shots"][0]["asset_sha256"], "aa")

    def test_retake_rebaselines_post_supply_variation_drift(self):
        """供给后补写的字段（cover_selection 等）导致组合 hash 漂移时，
        走组件级身份校验 + 重定基线，然后照常摘除重拍。"""
        from unittest import mock
        runner = self.build_workflow()
        prepared = [
            {"role": "look_a", "path": str(self.root / "a.png"), "sha256": "aa"},
            {"role": "look_b", "path": str(self.root / "b.png"), "sha256": "bb"},
            {"role": "look_c", "path": str(self.new_image), "sha256": "cc-new"},
            {"role": "look_d", "path": str(self.root / "d.png"), "sha256": "dd"},
        ]
        supply = self.fake_supply(prepared)
        supply.input_fingerprint.return_value = "drifted-hash"
        producer = mock.Mock()
        import types as types_mod
        def fake_produce(task_id):
            self.task.task_status = "image_review"
            return types_mod.SimpleNamespace(task_status="image_review")
        producer.produce.side_effect = fake_produce
        with mock.patch("services.release_gate.assert_main_queue_rework_allowed"), \
                mock.patch("services.photo_style_reference_supply."
                           "PhotoStyleReferenceSupplyService", return_value=supply):
            runner._retake_photo_supply_roles(
                types_mod.SimpleNamespace(record_id="rec1", fields={}), self.batch,
                self.entries, ["look_c"], self.root, producer)
        supply.verify_and_rebaseline_identity.assert_called_once()
        self.assertEqual(
            supply.verify_and_rebaseline_identity.call_args.kwargs["variation"],
            self.batch.manifest_json["content_plan"]["items"][0])
        self.assertEqual(supply.regenerate_roles.call_args.kwargs["roles"], ["look_c"])
        self.assertEqual(self.task.plan_json["shots"][2]["asset_sha256"], "cc-new")

    def test_retake_resumes_role_retired_by_earlier_repair(self):
        """角色已被此前质检/重拍摘除（sources 里缺失）时应直接续跑重生，
        不能当作“素材里没有该角色”拦截。"""
        from unittest import mock
        runner = self.build_workflow()
        # 模拟 look_b 已被摘除：sources 只剩 a/c/d
        manifest = json.loads(json.dumps(self.manifest))
        manifest["sources"] = [s for s in manifest["sources"] if s["role"] != "look_b"]
        (self.supply_dir / "supply_manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
        prepared = [
            {"role": "look_a", "path": str(self.root / "a.png"), "sha256": "aa"},
            {"role": "look_b", "path": str(self.root / "b.png"), "sha256": "bb-new"},
            {"role": "look_c", "path": str(self.root / "c.png"), "sha256": "cc"},
            {"role": "look_d", "path": str(self.root / "d.png"), "sha256": "dd"},
        ]
        supply = self.fake_supply(prepared)
        producer = mock.Mock()
        import types as types_mod
        def fake_produce(task_id):
            self.task.task_status = "image_review"
            return types_mod.SimpleNamespace(task_status="image_review")
        producer.produce.side_effect = fake_produce
        with mock.patch("services.release_gate.assert_main_queue_rework_allowed"), \
                mock.patch("services.photo_style_reference_supply."
                           "PhotoStyleReferenceSupplyService", return_value=supply):
            runner._retake_photo_supply_roles(
                types_mod.SimpleNamespace(record_id="rec1", fields={}), self.batch,
                self.entries, ["look_b"], self.root, producer)
        supply.regenerate_roles.assert_not_called()  # 无可摘除，直接续跑
        supply.prepare.assert_called_once()
        slot2 = self.task.plan_json["shots"][1]
        self.assertEqual(slot2["asset_sha256"], "bb-new")

    def test_retake_survives_attempt_history_entry_variable_shadowing(self):
        """回归：清单带 attempt_history 时内层循环曾遮蔽外层批次条目 entry，
        导致后续 task_by_source 取值 KeyError('source_record_id')。"""
        from unittest import mock
        runner = self.build_workflow()
        manifest = json.loads(json.dumps(self.manifest))
        manifest["attempt_history"] = [{
            "round": 1, "trigger": "operator_manual", "reason": "上一轮重拍",
            "failed_roles": ["look_c"], "retired": [
                {"role": "look_c", "path": str(self.old_image), "sha256": "previous"}],
        }]
        (self.supply_dir / "supply_manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
        prepared = [
            {"role": "look_a", "path": str(self.root / "a.png"), "sha256": "aa"},
            {"role": "look_b", "path": str(self.root / "b.png"), "sha256": "bb"},
            {"role": "look_c", "path": str(self.old_image), "sha256": "cc-new"},
            {"role": "look_d", "path": str(self.root / "d.png"), "sha256": "dd"},
        ]
        supply = self.fake_supply(prepared)
        producer = mock.Mock()
        import types as types_mod
        def fake_produce(task_id):
            self.task.task_status = "image_review"
            return types_mod.SimpleNamespace(task_status="image_review")
        producer.produce.side_effect = fake_produce
        with mock.patch("services.release_gate.assert_main_queue_rework_allowed"), \
                mock.patch("services.photo_style_reference_supply."
                           "PhotoStyleReferenceSupplyService", return_value=supply):
            runner._retake_photo_supply_roles(
                types_mod.SimpleNamespace(record_id="rec1", fields={}), self.batch,
                self.entries, ["look_c"], self.root, producer)
        self.assertEqual(self.task.plan_json["shots"][2]["asset_sha256"], "cc-new")

    def test_retake_rejects_rows_without_supply_manifest(self):
        from unittest import mock
        runner = self.build_workflow()
        self.supply_dir.joinpath("supply_manifest.json").unlink()
        with mock.patch("services.release_gate.assert_main_queue_rework_allowed"):
            with self.assertRaisesRegex(FeishuWorkflowError, "供给清单"):
                from types import SimpleNamespace
                runner._retake_photo_supply_roles(
                    SimpleNamespace(record_id="rec1", fields={}), self.batch, self.entries,
                    ["look_c"], self.root, mock.Mock())


class PhotoSourceReworkTest(unittest.TestCase):
    """ReworkService photo_source：换绑 reused_asset 槽位到新素材文件。"""

    def setUp(self):
        import types
        from domain.models import TaskRevision
        from services.workflow_v2 import canonical_hash
        self.TaskRevision = TaskRevision
        self.canonical_hash = canonical_hash
        shots = [
            {"slot_index": index, "shot_kind": "reused_asset",
             "asset_path": f"/tmp/look_{index}.png", "asset_sha256": f"sha{index}"}
            for index in range(1, 5)
        ]
        selected = {f"shot:{index}": f"shot_{index}" for index in range(1, 5)}
        selected.update({f"slide:{index}": f"slide_{index}" for index in range(1, 6)})
        snapshot = {"plan": {"shots": shots, "workflow_version": 2}}
        self.revision = types.SimpleNamespace(
            revision_id="revision_1", task_id="opv_task_1", lock_version=1,
            revision_status="working", plan_snapshot_json=snapshot,
            asset_manifest_json={"selected": selected, "candidates": {
                f"shot_{index}": {"asset_id": f"shot_{index}", "path": f"/tmp/{index}.png",
                                  "sha256": f"sha{index}"} for index in range(1, 5)}},
            parent_revision_id=None, rework_spec_json={}, revision_no=1,
            created_by="fixture", input_snapshot_hash="hash", selection_hash="hash",
        )
        self.task = types.SimpleNamespace(
            task_id="opv_task_1", task_status="image_review", workflow_version=2,
            active_revision_id="revision_1", row_version=1, plan_json={},
        )
        self.created = []

        class Repository:
            def __init__(self, outer):
                self.outer = outer
                self.outer.revisions = {"revision_1": outer.revision}

            def get_task(self, task_id):
                return self.outer.task

            def get_task_revision(self, revision_id):
                return self.outer.revisions.get(revision_id)

            def list_task_revisions(self, task_id):
                return list(self.outer.revisions.values())

            def create_revision_and_activate(self, revision, *, expected_task_row_version,
                                             transition_to=None):
                self.outer.created.append(revision)
                self.outer.revisions[revision.revision_id] = revision
                self.outer.task.active_revision_id = revision.revision_id
                self.outer.task.plan_json = revision.plan_snapshot_json["plan"]
                if transition_to:
                    self.outer.task.task_status = transition_to
                return self.outer.task

        self.repository = Repository(self)

    def test_photo_source_rework_patches_plan_and_pops_slot_selections(self):
        from services.workflow_v2 import ReworkService
        revision = ReworkService(self.repository, publication_guard=lambda task_id: None).begin(
            "opv_task_1", expected_revision_id="revision_1", expected_lock_version=1,
            scope="photo_source",
            plan_patch={3: {"asset_path": "/tmp/new_c.png", "asset_sha256": "new"}},
            reason="运营手动重拍 C", idempotency_key="retake-1",
            operator="fixture",
        )
        self.assertEqual(self.task.task_status, "rework_pending")
        plan = revision.plan_snapshot_json["plan"]
        self.assertEqual(plan["shots"][2]["asset_path"], "/tmp/new_c.png")
        self.assertEqual(plan["shots"][2]["asset_sha256"], "new")
        self.assertEqual(plan["shots"][0]["asset_path"], "/tmp/look_1.png")
        selected = revision.asset_manifest_json["selected"]
        self.assertNotIn("shot:3", selected)
        self.assertNotIn("slide:3", selected)
        self.assertIn("shot:1", selected)
        self.assertIn("shot:4", selected)
        self.assertEqual(revision.rework_spec_json["resume_stage"], "image_generating")
        self.assertEqual(revision.rework_spec_json["scope"], "photo_source")

    def test_photo_source_rework_rejects_unknown_slot_and_missing_hash(self):
        from services.workflow_v2 import ReworkService, WorkflowV2Error
        service = ReworkService(self.repository, publication_guard=lambda task_id: None)
        with self.assertRaises(WorkflowV2Error):
            service.begin("opv_task_1", expected_revision_id="revision_1",
                expected_lock_version=1, scope="photo_source",
                plan_patch={9: {"asset_path": "/x.png", "asset_sha256": "x"}},
                reason="bad slot", idempotency_key="retake-2")
        with self.assertRaises(WorkflowV2Error):
            service.begin("opv_task_1", expected_revision_id="revision_1",
                expected_lock_version=1, scope="photo_source",
                plan_patch={3: {"asset_path": "/tmp/new_c.png"}},
                reason="missing sha", idempotency_key="retake-3")


class PublishFailedResyncTest(unittest.TestCase):
    """发布失败行在底层恢复后必须能自愈回真实状态，不允许陈旧失败永挂。"""

    def test_scan_refreshes_stale_publish_failed_row(self):
        from types import SimpleNamespace
        task = SimpleNamespace(task_id="opv_task_1")
        writes = []

        class Repository:
            def get_production_batch(self, record_id):
                return None

            def list_tasks_by_source_prefix(self, source_type, prefix):
                return [task]

        class Client:
            def list_records(self, page_size=500):
                return [SimpleNamespace(record_id="rec1", fields={
                    FIELD_PROGRESS: "发布失败", "备注": "旧失败",
                })]

            def update_record_fields(self, record_id, fields):
                writes.append((record_id, dict(fields)))

        class Scheduler:
            def get_task_state(self, task_id):
                return {"task_id": task_id, "status": "已排期",
                        "account_name": "账号A",
                        "planned_publish_at": "2026-09-10 15:00:00",
                        "error_message": ""}

        runner = FeishuTaskWorkflow(
            repository=Repository(), client=Client(), generator=object(),
            renderer=object(), publish_scheduler=Scheduler())
        report = runner.scan()
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0][0], "rec1")
        self.assertEqual(writes[0][1].get("进度"), "已排期")
        self.assertTrue(any(item.get("updated") for item in report["processed"]))


class RetakeWiringTest(unittest.TestCase):
    """回归：批次复用路径里 staging_root 未定义曾导致重拍直接 UnboundLocalError。"""

    def test_generate_native_photo_passes_existing_staging_root_to_retake(self):
        from types import SimpleNamespace
        from unittest import mock
        task = FakeRetakeTask()
        batch = SimpleNamespace(
            batch_id="batch1", batch_status="running", expected_count=1,
            source_record_id="rec1",
            manifest_json={"preset": "PHOTO_TH_TRAVEL_V1", "media_kind": "native_photo",
                           "entries": [{"spec": {}, "request": {},
                                        "source_record_id": task.source_record_id}]})

        class Repository:
            def get_production_batch(self, record_id):
                return batch

            def list_tasks_by_source_prefix(self, source_type, prefix):
                return [task]

        class Client:
            def list_records(self, page_size=500):
                return []

            def update_record_fields(self, record_id, fields):
                pass

        runner = FeishuTaskWorkflow(repository=Repository(), client=Client(),
                                    generator=object(), renderer=object())
        runner._run_lease = mock.Mock()
        entries = [{"spec": {}, "request": {"profile_id": "fixture", "copy": {},
                                            "theme_brief": {}, "content_card": {},
                                            "asset_set_key": "set", "asset_set_version": 1},
                    "source_record_id": task.source_record_id}]
        runner._photo_batch_entries = mock.Mock(return_value=entries)
        captured = {}

        class RetakeCalled(Exception):
            pass

        def fake_retake(record, batch_arg, entries_arg, roles, staging_root, producer):
            captured["staging_root"] = staging_root
            captured["roles"] = roles
            raise RetakeCalled()

        runner._retake_photo_supply_roles = fake_retake
        record = SimpleNamespace(record_id="rec1", fields={FIELD_RETAKE_LOOK: "C"})
        with mock.patch("services.release_gate.require_photo_content_allowed"):
            with self.assertRaises(RetakeCalled):
                runner._generate(record)
        self.assertEqual(captured["roles"], ["look_c"])
        self.assertTrue(Path(captured["staging_root"]).exists())


if __name__ == "__main__":
    unittest.main()
