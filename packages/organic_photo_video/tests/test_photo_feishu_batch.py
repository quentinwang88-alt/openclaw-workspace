from __future__ import annotations

import copy
import hashlib
import io
import json
import tempfile
import unittest
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from PIL import Image

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services.feishu_workflow import (
    FeishuTaskWorkflow, FIELD_FAILURE_REASON, FIELD_PHOTO_SUMMARY, FIELD_STORE,
)
from services.image_generator import GenerationOutcome
from services.photo_request_factory import fingerprint
from test_photo_planner import PlannerRepo


class BatchRepo(PlannerRepo):
    def __init__(self, root):
        super().__init__(root)
        from photo_content_fixture import qualify
        self.asset_sets = qualify(self, root, count=3)
        self.tasks, self.batch, self.plans, self.produced = {}, None, {}, []

    def get_task(self, identity):
        return self.tasks.get(identity)

    def get_production_batch(self, identity):
        return self.batch if self.batch and self.batch.source_record_id == identity else None

    def create_production_batch_idempotent(self, batch):
        self.batch = self.batch or copy.deepcopy(batch)
        return self.batch

    def upsert_asset_set(self, asset_set):
        self.asset_sets.append(copy.deepcopy(asset_set))

    def list_tasks_by_source_prefix(self, _source, prefix):
        return [t for t in self.tasks.values() if t.source_record_id.startswith(prefix)]

    def claim_batch_run(self, identity, **kwargs):
        if self.batch.run_owner:
            return False
        self.batch.run_owner = kwargs["owner"]
        return True

    def heartbeat_batch_run(self, identity, **kwargs):
        return self.batch.run_owner == kwargs["owner"]

    def finish_batch_run(self, identity, **kwargs):
        self.batch.run_owner = None

    def queue_batch_projection(self, identity, fields):
        self.batch.pending_fields_json = copy.deepcopy(fields)

    def acknowledge_batch_projection(self, identity, fields):
        if self.batch.pending_fields_json == fields:
            self.batch.pending_fields_json = None


class Client:
    def __init__(self):
        self.fields = {"生产预设": "图文｜TH｜穿搭四选一", "生成数量": 3, "执行": True,
                       "素材状态": "已匹配可用素材", FIELD_STORE: "THFZ01"}
        self.fail_terminal = False

    def get_record(self, identity):
        return SimpleNamespace(record_id=identity, fields=copy.deepcopy(self.fields))

    def update_record_fields(self, identity, fields):
        if self.fail_terminal and fields.get("确认发布") is True:
            raise RuntimeError("Feishu unavailable")
        self.fields.update(copy.deepcopy(fields))

    def download_attachment_bytes(self, attachment):
        stream = io.BytesIO()
        seed = int(attachment["file_token"])
        Image.new("RGB", (120, 180), (50 + seed, 80, 100)).save(stream, format="PNG")
        content = stream.getvalue()
        return content, f"{seed}.png", "image/png", len(content)


class ReferenceVision:
    def analyze(self, **kwargs):
        count = kwargs["count"]
        sets = []
        for index in range(1, count + 1):
            sets.append({
                "content_angle_zh": f"参考穿搭第 {index} 组",
                "scene_zh": f"城市生活场景 {index}", "palette_zh": f"暖色组 {index}",
                "background_prompt": f"自然生活场景 {index}", "style_modifier": "秋季自然穿搭",
                "looks": [{
                    "role": f"look_{letter}", "display_label": f"ลุค {letter.upper()}",
                    "outerwear": f"外套 {index}{letter}", "top_inner": f"内搭 {index}{letter}",
                    "bottom": f"下装 {index}{letter}", "shoes": f"鞋 {index}{letter}",
                    "outerwear_type": f"outer-{index}-{letter}",
                    "bottom_type": f"bottom-{index}-{letter}",
                } for letter in "abcd"],
                "copy": {"title": f"4 ลุค {index}", "cover": f"เลือกหนึ่งลุค {index}",
                         "caption": f"A B C หรือ D {index}", "cta": "คุณชอบลุคไหน?"},
            })
        return {
            "analysis_method": "doubao_seed_2_1", "presentation_type": "MODEL_FULL_BODY",
            "palette": ["camel", "cream"], "temperature": "warm", "aggregate": {},
            "recommended_sets": sets,
        }

    def review_alignment(self, **kwargs):
        return {"passed": True, "notes": "通过"}


class PhotoBatchTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = BatchRepo(Path(self.temp.name))
        self.client = Client()
        self.workflow = FeishuTaskWorkflow(self.repo, self.client, generator=object(), renderer=object(),
                                          output_root=Path(self.temp.name),
                                          photo_reference_vision=ReferenceVision())
        self.client.fields["生产预设"] = next(name for name in self.workflow.catalog.names
            if self.workflow.catalog.is_native_photo(name)
            and self.workflow.catalog.resolve(name, "rec")[0].recipe_id == self.repo.recipe.recipe_id)
        self.workflow._upload_files = lambda paths, **kwargs: [{"file_token": path} for path in paths]
        self.workflow._v2_released = lambda task: bool(task.released_revision_id)
        self.fail_create_index = None
        self.fail_produce_index = None
        self.review_fail_index = None
        self.review_calls = []
        self.addCleanup(patch.stopall)
        patch("services.task_intake.TaskIntakeService.create_task", side_effect=self.create).start()
        patch("services.photo_planner.PhotoReusePlannerService.plan_task", side_effect=self.plan).start()
        patch("services.feishu_workflow.HeroFirstProducer", return_value=object()).start()
        patch("services.photo_package.NativePhotoProductionFlow.prepare", side_effect=self.prepare).start()
        patch("services.workflow_v2.PhotoPackageReviewService.record", side_effect=self.review).start()

    def create(self, request):
        # The complete batch must exist before the first task write.
        assert self.repo.batch is not None and self.repo.batch.expected_count >= 1
        index = int(request.source_record_id.split(":")[1])
        if index == self.fail_create_index:
            raise RuntimeError("intake interrupted")
        identity = f"task-{index}"
        if identity not in self.repo.tasks:
            self.repo.tasks[identity] = SimpleNamespace(task_id=identity, source_record_id=request.source_record_id,
                task_status="draft", target_country="TH", target_locale="th-TH", media_kind="native_photo",
                workflow_version=2, released_revision_id=None, active_revision_id=f"rev-{index}",
                plan_json={}, content_package_id=f"pack-{index}")
        return SimpleNamespace(task=self.repo.tasks[identity])

    def plan(self, identity, **kwargs):
        self.repo.plans[identity] = copy.deepcopy(kwargs)
        self.repo.tasks[identity].task_status = "planned"

    def prepare(self, identity, **kwargs):
        task = self.repo.tasks[identity]
        if task.task_status == "planned":
            if identity == f"task-{self.fail_produce_index}":
                raise RuntimeError("packaging interrupted")
            self.repo.produced.append(identity)
            task.task_status = "photo_packaging"
        return {"photo_manifest": {"slides": [{"path": self.repo.asset_set.manifest_json["assets"][i % 4]["path"]} for i in range(5)]}}

    def review(self, identity, **kwargs):
        self.review_calls.append(identity)
        if identity == f"task-{self.review_fail_index}":
            raise RuntimeError("review interrupted")
        task = self.repo.tasks[identity]
        task.task_status = "photo_ready"
        task.released_revision_id = task.active_revision_id
        return SimpleNamespace(review_id=f"review-{identity}")

    def scan(self):
        return self.workflow.scan(record_id="rec")

    def test_no_json_builds_frozen_batch_and_preview_summary(self):
        report = self.scan()
        self.assertEqual(report["errors"], [])
        self.assertEqual(len(self.repo.tasks), 3)
        self.assertEqual(self.client.fields["审核阶段"], "技术完成")
        self.assertEqual(self.client.fields["审核"], "无需审核")
        self.assertIn("V1", self.client.fields[FIELD_PHOTO_SUMMARY])
        self.assertEqual(len({e["request"]["content_card"]["content_signature"] for e in self.repo.batch.manifest_json["entries"]}), 3)
        self.assertTrue(all(t.task_status == "photo_packaging" for t in self.repo.tasks.values()))

    def test_uploaded_complete_looks_need_only_confirm_publish(self):
        self.client.fields["生成数量"] = 1
        self.client.fields["素材状态"] = ""
        self.client.fields["图文参考图"] = [
            {"file_token": str(index)} for index in range(1, 5)
        ]
        produced = self.scan()
        self.assertEqual(produced["processed"][0]["action"], "generate_native_photo")
        self.assertEqual(self.client.fields["素材状态"], "已匹配可用素材")
        self.assertIsNotNone(self.repo.batch)
        self.workflow.publish_scheduler = SimpleNamespace(
            enqueue_task=lambda task_id, **kwargs: {"task_id": task_id}
        )
        self.client.fields["确认发布"] = True
        scheduled = self.scan()
        self.assertFalse(scheduled["errors"])
        self.assertEqual(scheduled["processed"][0]["action"], "enqueue_main_schedule")
        self.assertEqual(len(self.repo.tasks), 1)

    def test_product_code_automatically_prepares_and_packages_four_looks(self):
        self.client.fields["生成数量"] = 1
        self.client.fields["素材状态"] = ""
        self.client.fields["产品编码"] = "P-OUTER-1"
        sources = []
        for index, role in enumerate(("look_a", "look_b", "look_c", "look_d")):
            path = Path(self.temp.name) / f"auto-{index}.png"
            Image.new("RGB", (120, 180), (70 + index, 80, 90)).save(path)
            sources.append({"role": role, "path": str(path), "look_ref": f"LOOK-{index}",
                            "source_kind": "generated_outfit"})
        self.workflow.product_reference_resolver.resolve_snapshot = lambda *args, **kwargs: {
            "product_id": "P-OUTER-1", "category": "outerwear",
            "reference_images": [sources[0]["path"]],
        }
        self.repo.get_account_profile = lambda _identity: SimpleNamespace(persona_ref_id="PERSONA_TH")
        with patch("services.photo_outfit_supply.PhotoOutfitSupplyService.prepare",
                   return_value={"sources": sources, "role_plan": [{"role": x["role"]} for x in sources],
                                 "reused_count": 1, "generated_count": 3}):
            result = self.scan()
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["processed"][0]["action"], "generate_native_photo")
        self.assertEqual(self.client.fields["素材状态"], "已匹配可用素材")
        self.assertEqual(len(self.client.fields["预览/成片"]), 5)
        self.assertIsNotNone(self.repo.batch)

    def test_product_reference_quantity_three_builds_independent_asset_sets(self):
        self.client.fields.update({
            "生成数量": 3, "素材状态": "", "产品编码": "P-OUTER-1",
            "图文主题": "秋季穿搭", "参考图类型": "商品参考",
        })
        self.workflow.product_reference_resolver.resolve_snapshot = lambda *args, **kwargs: {
            "product_id": "P-OUTER-1", "category": "outerwear",
            "reference_images": [str(Path(self.temp.name) / "product.png")],
        }
        Image.new("RGB", (120, 180), "navy").save(Path(self.temp.name) / "product.png")
        self.repo.get_account_profile = lambda _identity: SimpleNamespace(persona_ref_id="PERSONA_TH")
        calls = []

        def prepare(**kwargs):
            calls.append(copy.deepcopy(kwargs.get("variation") or {}))
            sources = []
            for offset, role in enumerate(("look_a", "look_b", "look_c", "look_d")):
                path = Path(self.temp.name) / f"product-{len(calls)}-{offset}.png"
                Image.new("RGB", (120, 180), (40 * len(calls), 30 + offset, 90)).save(path)
                sources.append({
                    "role": role, "path": str(path), "look_ref": f"LOOK-{offset}",
                    "source_kind": "generated_outfit",
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "planned_look_signature": hashlib.sha256(json.dumps({
                        key: str((kwargs["variation"]["looks"][offset]).get(key) or "")
                        for key in ("role", "outerwear", "top_inner", "bottom", "shoes")
                    }, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest(),
                })
            return {"sources": sources, "role_plan": [{"role": x["role"]} for x in sources]}

        with patch("services.photo_outfit_supply.PhotoOutfitSupplyService.prepare",
                   side_effect=prepare):
            result = self.scan()
        self.assertEqual(result["errors"], [])
        self.assertEqual(len(calls), 3)
        self.assertEqual(len({item["variation_id"] for item in calls}), 3)
        entries = self.repo.batch.manifest_json["entries"]
        self.assertEqual(len({entry["request"]["asset_set_id"] for entry in entries}), 3)

    def test_product_driven_preset_rejects_empty_input_instead_of_using_demo_assets(self):
        self.client.fields["生成数量"] = 1
        self.client.fields["素材状态"] = ""
        result = self.scan()
        self.assertEqual(result["processed"], [])
        self.assertIn("需要填写产品编码或上传参考图", result["errors"][0]["error"])
        self.assertIsNone(self.repo.batch)

    def test_unified_style_references_generate_theme_bound_package_without_asset_review(self):
        class Generator:
            calls = []

            def generate_shot(inner_self, request):
                inner_self.calls.append(request)
                path = Path(request.output_dir) / f"style-{request.slot_index}.png"
                Image.new("RGB", (120, 180), (30 * request.slot_index, 90, 100)).save(path)
                return GenerationOutcome(ok=True, image_path=str(path), request_id=str(request.slot_index))

        generator = Generator()
        self.workflow.generator = generator
        self.client.fields.update({
            "生成数量": 1, "素材状态": "", "图文主题": "秋季穿搭",
            "参考图类型": "风格参考",
            "参考图（可选）": [{"file_token": "1"}, {"file_token": "2"}],
        })
        self.repo.get_account_profile = lambda _identity: SimpleNamespace(persona_ref_id="PERSONA_TH")
        with patch("services.feishu_workflow.LightTryonAssetReader.get_persona",
                   return_value={"persona_id": "PERSONA_TH", "local_reference_images": []}):
            result = self.scan()
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["processed"][0]["action"], "generate_native_photo")
        self.assertEqual(len(generator.calls), 4)
        request = self.repo.batch.manifest_json["entries"][0]["request"]
        self.assertEqual(request["theme_brief"]["theme_key"], "AUTUMN_OUTFIT")
        self.assertEqual(request["theme_brief"]["reference_mode"], "STYLE")
        self.assertTrue(request["copy"]["title"].startswith("4 ลุค"))
        self.assertEqual(
            request["copy"]["slide_texts"][0],
            request["theme_brief"]["batch_variation"]["copy"]["cover"],
        )

    def test_style_reference_quantity_three_freezes_three_distinct_posts(self):
        class Generator:
            def __init__(inner_self):
                inner_self.calls = []

            def generate_shot(inner_self, request):
                inner_self.calls.append(request)
                path = Path(request.output_dir) / f"style-{request.slot_index}.png"
                seed = len(inner_self.calls)
                Image.new("RGB", (120, 180), (20 + seed, 70, 110)).save(path)
                return GenerationOutcome(ok=True, image_path=str(path), request_id=str(seed))

        generator = Generator()
        self.workflow.generator = generator
        self.client.fields.update({
            "生成数量": 3, "素材状态": "", "图文主题": "秋季穿搭",
            "参考图类型": "风格参考",
            "参考图（可选）": [{"file_token": "1"}, {"file_token": "2"}],
        })
        self.repo.get_account_profile = lambda _identity: SimpleNamespace(persona_ref_id="PERSONA_TH")
        with patch("services.feishu_workflow.LightTryonAssetReader.get_persona",
                   return_value={"persona_id": "PERSONA_TH", "local_reference_images": []}):
            result = self.scan()
        self.assertEqual(result["errors"], [])
        self.assertEqual(len(generator.calls), 12)
        self.assertEqual(len(self.repo.tasks), 3)
        self.assertEqual(len(self.client.fields["预览/成片"]), 15)
        entries = self.repo.batch.manifest_json["entries"]
        self.assertEqual(len({entry["request"]["asset_set_id"] for entry in entries}), 3)
        variations = [entry["request"]["theme_brief"]["batch_variation"] for entry in entries]
        self.assertEqual(len({item["variation_id"] for item in variations}), 3)
        self.assertEqual(len({item["scene_zh"] for item in variations}), 3)
        self.assertEqual(len({item["copy"]["cover"] for item in variations}), 3)
        self.assertTrue(all(len(item["looks"]) == 4 for item in variations))
        self.assertIn("具体内容计划", self.client.fields[FIELD_PHOTO_SUMMARY])
        self.assertEqual(self.repo.batch.manifest_json["content_plan"]["count"], 3)

    def test_complete_look_quantity_two_splits_eight_images_per_post(self):
        self.client.fields.update({
            "生成数量": 2, "素材状态": "", "参考图类型": "完整穿搭",
            "参考图（可选）": [{"file_token": str(index)} for index in range(1, 9)],
        })
        result = self.scan()
        self.assertEqual(result["errors"], [])
        entries = self.repo.batch.manifest_json["entries"]
        self.assertEqual(len(entries), 2)
        self.assertNotEqual(entries[0]["request"]["asset_set_id"],
                            entries[1]["request"]["asset_set_id"])
        import json
        manifests = [entry["request"]["asset_snapshot"]["manifest_json"] for entry in entries]
        manifests = [json.loads(item) if isinstance(item, str) else item for item in manifests]
        first_hashes = {item["sha256"] for item in manifests[0]["assets"]}
        second_hashes = {item["sha256"] for item in manifests[1]["assets"]}
        self.assertTrue(first_hashes.isdisjoint(second_hashes))

    def test_partial_intake_resumes_original_three_after_config_and_row_changes(self):
        self.fail_create_index = 2
        first = self.scan()
        self.assertTrue(first["processed"][0]["failures"])
        self.assertEqual(set(self.repo.tasks), {"task-1", "task-3"})
        frozen = copy.deepcopy(self.repo.batch.manifest_json)
        with self.assertRaisesRegex(RuntimeError, "2/3"):
            self.workflow._assert_batch_complete("rec", list(self.repo.tasks.values()))
        self.client.fields.update({"生产预设": "deleted", "生成数量": 9, "图文任务JSON": "not json", "执行": True})
        self.repo.recipe.recipe_spec_json = {}
        self.repo.asset_set.status = "disabled"
        self.fail_create_index = None
        second = self.scan()
        self.assertEqual(second["errors"], [])
        self.assertEqual(self.repo.batch.manifest_json, frozen)
        self.assertEqual(set(self.repo.tasks), {"task-1", "task-2", "task-3"})
        self.assertEqual(self.repo.produced, ["task-1", "task-3", "task-2"])
        self.assertEqual(self.repo.plans["task-2"]["recipe_snapshot"], frozen["entries"][1]["request"]["recipe_snapshot"])

    def test_partial_packaging_does_not_regenerate_completed_items(self):
        self.fail_produce_index = 2
        self.scan()
        self.assertEqual(len(self.repo.tasks), 3)
        self.client.fields["确认发布"] = True
        rejected = self.scan()
        self.assertTrue(rejected["errors"])
        self.assertEqual(self.review_calls, [])
        self.client.fields["执行"] = True
        self.fail_produce_index = None
        self.scan()
        self.assertEqual(self.repo.produced, ["task-1", "task-3", "task-2"])

    def test_review_retry_skips_already_released_task(self):
        self.scan()
        self.workflow.publish_scheduler = SimpleNamespace(
            enqueue_task=lambda task_id, **kwargs: {"task_id": task_id}
        )
        self.client.fields["确认发布"] = True
        self.review_fail_index = 2
        self.assertTrue(self.scan()["errors"])
        self.assertEqual(self.repo.tasks["task-1"].task_status, "photo_ready")
        self.client.fields["确认发布"] = True
        self.review_fail_index = None
        self.assertEqual(self.scan()["errors"], [])
        self.assertEqual(self.review_calls, ["task-1", "task-2", "task-2", "task-3"])
        self.assertEqual(self.client.fields["进度"], "待排班")
        self.assertFalse(self.client.fields["确认发布"])

    def test_confirm_publish_is_the_only_photo_release_gate(self):
        self.scan()

        class Scheduler:
            def __init__(inner):
                inner.calls = []

            def enqueue_task(inner, task_id, **kwargs):
                inner.calls.append((task_id, kwargs))
                return {"task_id": task_id, "status": "待排班"}

        scheduler = Scheduler()
        self.workflow.publish_scheduler = scheduler
        self.client.fields.update({"审核": "待审核", "确认发布": True})
        report = self.scan()
        self.assertEqual(report["errors"], [])
        self.assertEqual(report["processed"][0]["action"], "enqueue_main_schedule")
        self.assertEqual(len(self.review_calls), 3)
        self.assertEqual(len(scheduler.calls), 3)
        self.assertEqual(self.client.fields["审核"], "无需审核")
        self.assertFalse(self.client.fields["确认发布"])
        self.assertTrue(all(call[1]["store_id"] == "THFZ01" for call in scheduler.calls))

    def test_photo_confirm_publish_requires_store(self):
        self.scan()
        self.workflow.publish_scheduler = SimpleNamespace(
            enqueue_task=lambda task_id, **kwargs: {"task_id": task_id}
        )
        self.client.fields.pop(FIELD_STORE)
        self.client.fields["确认发布"] = True
        report = self.scan()
        self.assertTrue(report["errors"])
        self.assertIn("必须选择店铺", report["errors"][0]["error"])
        # 补上店铺重试成功后，残留的失败原因必须自愈，否则已发布的行会长期挂着
        # 这条文案（线上曾出现 7 行「已发布」仍显示「确认发布前必须选择店铺」）。
        self.assertIn("必须选择店铺", self.client.fields[FIELD_FAILURE_REASON])
        self.client.fields[FIELD_STORE] = "THFZ01"
        self.client.fields["确认发布"] = True
        retried = self.scan()
        self.assertEqual(retried["errors"], [])
        self.assertEqual(self.client.fields["进度"], "待排班")
        self.assertIsNone(self.client.fields.get(FIELD_FAILURE_REASON))

    def test_committed_review_projection_replays_without_new_review(self):
        self.scan()
        self.workflow.publish_scheduler = SimpleNamespace(
            enqueue_task=lambda task_id, **kwargs: {"task_id": task_id}
        )
        self.client.fields["确认发布"] = True
        self.client.fail_terminal = True
        self.assertTrue(self.scan()["errors"])
        self.assertTrue(self.repo.batch.pending_fields_json)
        self.client.fail_terminal = False
        recovered = self.scan()
        self.assertEqual(recovered["processed"][0]["action"], "recover_projection")
        self.assertEqual(len(self.review_calls), 3)
        self.assertEqual(self.client.fields["进度"], "已完成")

    def test_tampered_batch_and_unexpected_task_block_release(self):
        self.scan()
        self.repo.tasks["task-3"].source_record_id = "rec:4:other:1"
        with self.assertRaisesRegex(RuntimeError, "不一致"):
            self.workflow._assert_batch_complete("rec", list(self.repo.tasks.values()))
        self.repo.batch.manifest_json["entries"][0]["request"]["copy"]["caption"] = "tampered"
        self.client.fields["确认发布"] = True
        self.assertTrue(self.scan()["errors"])
        self.assertEqual(self.review_calls, [])

    def test_legacy_json_cannot_override_source_bound_copy(self):
        self.client.fields["图文任务JSON"] = '{"items":[{"copy":{"caption":"one"}},{"copy":{"caption":"two"}},{"copy":{"caption":"three"}}]}'
        report = self.scan()
        self.assertTrue(report["errors"])
        self.assertIn("不允许自由覆盖文案", report["errors"][0]["error"])
        # 有业务语义的拒绝必须逐字呈现，不能被兜底处理器加上系统错误前缀。
        self.assertNotIn("系统内部错误", self.client.fields[FIELD_FAILURE_REASON])
        self.assertIsNone(self.repo.batch)

    def test_confirmed_staged_assets_persist_the_asset_set_pin(self):
        """人工确认素材时必须把 asset_set_id 一并写进「图文任务JSON」。

        审核确认写下的「素材状态=已确认，正在生成」会先落地；若 asset_set_id 只
        留在内存里，那一次 _generate 中途失败后该行就永久卡死：重跑既跳过素材段
        （素材状态已确认）又没有 pin 可复用 ⇒ 落到内容池报 NEEDS_CONTENT。
        """
        from services.photo_asset_supply import PhotoAssetSupplyService
        PhotoAssetSupplyService(self.client, root=Path(self.temp.name)).stage(
            record_id="rec",
            attachments=[{"file_token": str(index)} for index in range(1, 5)],
            required_roles=["look_a", "look_b", "look_c", "look_d"],
        )
        self.client.fields.update({
            "生成数量": 1, "素材状态": "待内容审核", "审核": "通过", "执行": False,
        })
        report = self.scan()
        self.assertEqual(report["errors"], [])
        self.assertEqual(report["processed"][0]["action"], "generate_native_photo")
        pin = json.loads(self.client.fields["图文任务JSON"])
        self.assertEqual(pin["asset_set_id"],
                         report["processed"][0]["qualified_asset_set_id"])
        self.assertTrue(pin["asset_set_id"])

    def test_settled_asset_status_without_a_pin_rebuilds_the_supply(self):
        """素材状态「已确认/已匹配」但没有 pin 也没有批次时，素材段必须照常跑。

        跳过素材段只会得到空 overrides ⇒ 落到内容池回退路径 ⇒ NEEDS_CONTENT。
        """
        from services.photo_asset_supply import PhotoAssetSupplyService
        calls: list[str] = []
        original = PhotoAssetSupplyService.qualify

        def spy(inner_self, **kwargs):
            calls.append(str(kwargs.get("record_id") or ""))
            return original(inner_self, **kwargs)

        self.client.fields.update({
            "生成数量": 1, "素材状态": "已确认，正在生成",
            "图文参考图": [{"file_token": str(index)} for index in range(1, 5)],
        })
        with patch.object(PhotoAssetSupplyService, "qualify", spy):
            produced = self.scan()
        self.assertEqual(produced["errors"], [])
        self.assertEqual(produced["processed"][0]["action"], "generate_native_photo")
        self.assertTrue(calls, "素材段被跳过了：该状态下行既无批次也无 pin")
        self.assertIsNotNone(self.repo.batch)

    def test_persisted_pin_keeps_the_asset_section_skipped(self):
        """有 pin 的行仍按原语义跳过素材段，直接复用已冻结素材集。"""
        from services.photo_asset_supply import PhotoAssetSupplyService
        calls: list[str] = []
        original = PhotoAssetSupplyService.qualify

        def spy(inner_self, **kwargs):
            calls.append(str(kwargs.get("record_id") or ""))
            return original(inner_self, **kwargs)

        self.client.fields.update({
            "生成数量": 1, "素材状态": "已匹配可用素材",
            "图文任务JSON": json.dumps({"asset_set_id": "aset-1"}),
        })
        with patch.object(PhotoAssetSupplyService, "qualify", spy):
            produced = self.scan()
        self.assertEqual(produced["errors"], [])
        self.assertEqual(calls, [])
        request = self.repo.batch.manifest_json["entries"][0]["request"]
        self.assertEqual(request["asset_snapshot"]["asset_set_id"], "aset-1")

    def test_theme_overridden_copy_is_bound_and_freezeable(self):
        """真实入口链路：有主题 → 素材 → build_batch → copy 覆盖 → 封版校验。

        这是 2026-09-14 表格 143–147 行被堵死的现场：运营填了「图文主题」，
        冻结后 `build_theme_copy` 会把已解析的 request copy 整个换掉。若这一步
        不绑定素材标签，字面 `{{label_x}}` 就会进请求，被
        `validate_frozen_request` 拒掉整行，而失败发生在四张图已付费之后。
        """
        from services.photo_request_factory import validate_frozen_request
        self.client.fields.update({
            "生成数量": 1, "素材状态": "",
            "图文主题": "凉爽旅行",
            "图文参考图": [{"file_token": str(index)} for index in range(1, 5)],
        })
        report = self.scan()
        self.assertEqual(report["errors"], [])
        self.assertEqual(report["processed"][0]["action"], "generate_native_photo")
        request = self.repo.batch.manifest_json["entries"][0]["request"]
        copy = request["copy"]
        blob = json.dumps(copy, ensure_ascii=False)
        self.assertNotIn("{{", blob)
        self.assertNotIn("}}", blob)
        # 标签绑定到冻结素材的 display_label，而不是字面模板或空串。
        self.assertTrue(all(
            "·" in slide for slide in copy["slide_texts"][1:4]), copy["slide_texts"])
        for slide in copy["slide_texts"][1:4]:
            self.assertNotRegex(slide, r"\{\{label_[a-d]\}\}")
        validate_frozen_request(request)
        self.assertTrue(copy["title"] and copy["caption"] and copy["hashtags"])

    def test_unexpected_internal_error_is_labelled_with_its_type(self):
        # 真实案例 recvuMrl4BEn0U：兜底处理器把 KeyError 的裸 repr 原样写进
        # 「图文生成失败的原因」，运营整列只看到 'source_record_id'，
        # 无法判断是业务拦截、缺填字段还是系统故障。未预期异常必须自带类型标注。
        with patch.object(FeishuTaskWorkflow, "_generate",
                          side_effect=KeyError("source_record_id")):
            report = self.scan()
        self.assertTrue(report["errors"])
        self.assertEqual(self.client.fields[FIELD_FAILURE_REASON],
                         "系统内部错误（KeyError）：'source_record_id'")

    def test_batch_lease_loser_does_not_consume_execute(self):
        self.scan()
        self.repo.batch.run_owner = "other-worker"
        self.client.fields["确认发布"] = True
        result = self.scan()
        self.assertEqual(result["processed"][0]["action"], "leased_skip")
        self.assertTrue(self.client.fields["确认发布"])

    def test_pre_batch_replan_archives_only_local_planning_state(self):
        root = Path(self.temp.name)
        paths = [
            root / "reference_contracts" / "rec",
            root / "content_plans" / "rec",
            root / "style_reference_supply" / "rec_item_1",
        ]
        for path in paths:
            path.mkdir(parents=True)
            (path / "manifest.json").write_text("{}", encoding="utf-8")

        archive = self.workflow._archive_photo_planning_state(
            root, "rec", reason="参考图或内容要求已变化",
        )

        self.assertIsNotNone(archive)
        self.assertTrue((archive / "replan.json").exists())
        self.assertTrue((archive / "reference_contracts" / "rec" / "manifest.json").exists())
        self.assertTrue((archive / "content_plans" / "rec" / "manifest.json").exists())
        self.assertTrue((archive / "style_reference_supply" / "rec_item_1" / "manifest.json").exists())
        self.assertTrue(all(not path.exists() for path in paths))
        self.assertIsNone(self.repo.batch, "replan helper must not create or mutate a DB batch")

    def test_only_known_pre_batch_failures_are_replannable(self):
        self.assertTrue(self.workflow._is_replannable_photo_error(
            RuntimeError("参考分析或旅行变量已变化；请新建任务")))
        self.assertTrue(self.workflow._is_replannable_photo_error(
            RuntimeError("整组参考一致性重做次数已用尽")))
        self.assertFalse(self.workflow._is_replannable_photo_error(
            RuntimeError("模型鉴权失败")))


if __name__ == "__main__":
    unittest.main()
