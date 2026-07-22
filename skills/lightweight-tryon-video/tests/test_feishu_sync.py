from __future__ import annotations

import tempfile
import unittest
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

SKILL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_DIR / "scripts"))

from light_tryon.database import LightTryonDB
from light_tryon.feishu_mappings import TABLE_MAPPINGS
from light_tryon.feishu_sync import (
    cleanup_review_duplicates,
    ensure_schema,
    initialize_template_records,
    pull_manual_reviews,
    pull_visual_plan_reviews,
    pull_templates,
    push_reviews,
    push_visual_plans,
)
from light_tryon.models import ProductInput
from light_tryon.planner import plan_product
from light_tryon.prompting import PROMPT_BUILDER_VERSION, build_prompt
from light_tryon.review_video_processing import process_review_videos
from light_tryon.run_manager_sync import (
    ensure_run_manager_schema,
    pull_generation_preferences,
    pull_run_manager_results,
    resolve_run_manager_identity,
    sync_jobs_to_run_manager,
)
from light_tryon.source_script_sync import ensure_source_schema, find_source_records, process_source_requests, set_source_request
from light_tryon.visual_plans import confirm_outfit_image, orchestrate_visual_plans


class FakeClient:
    def __init__(self, records=None, fields=None):
        self.records = [SimpleNamespace(record_id=item["record_id"], fields=dict(item.get("fields") or {})) for item in (records or [])]
        self.fields = fields or [SimpleNamespace(field_id="fld_text", field_name="文本", field_type=1, ui_type="Text", property=None)]
        self.views = [{"view_id": "view_1", "view_name": "表格"}]
        self.table_name = "数据表"
        self.uploads = []

    def list_records(self, page_size=500):
        return self.records

    def list_fields(self):
        return self.fields

    def list_views(self):
        return self.views

    def update_field(self, field_id, spec):
        for item in self.fields:
            if item.field_id == field_id:
                item.field_name = spec["name"]
                item.field_type = spec.get("type", item.field_type)
                item.ui_type = spec.get("ui_type", item.ui_type)
                item.property = spec.get("property", item.property)

    def create_field(self, field_name, field_type=1, ui_type="Text", property=None):
        self.fields.append(SimpleNamespace(field_id=f"fld_{len(self.fields)}", field_name=field_name, field_type=field_type, ui_type=ui_type, property=property))

    def rename_table(self, name):
        self.table_name = name

    def rename_view(self, view_id, name):
        for view in self.views:
            if view["view_id"] == view_id:
                view["view_name"] = name

    def create_view(self, name):
        self.views.append({"view_id": f"view_{len(self.views) + 1}", "view_name": name})
        return self.views[-1]["view_id"]

    def update_record_fields(self, record_id, fields):
        for record in self.records:
            if record.record_id == record_id:
                record.fields.update(fields)
                return
        raise KeyError(record_id)

    def batch_create_records(self, records):
        ids = []
        for item in records:
            record_id = f"rec_{len(self.records) + 1}"
            self.records.append(SimpleNamespace(record_id=record_id, fields=dict(item.get("fields") or {})))
            ids.append(record_id)
        return ids

    def batch_delete_records(self, record_ids):
        wanted = set(record_ids)
        before = len(self.records)
        self.records = [record for record in self.records if record.record_id not in wanted]
        return before - len(self.records)

    def download_attachment_bytes(self, attachment):
        name = str(attachment.get("name") or "image.jpg")
        content_type = "video/mp4" if name.lower().endswith(".mp4") else "image/jpeg"
        content = b"fake-video" if content_type == "video/mp4" else b"fake-image"
        return content, name, content_type, len(content)

    def upload_attachment(self, content, file_name, content_type, size=None, parent_type="bitable_image"):
        attachment = {
            "file_token": f"uploaded_{len(self.uploads) + 1}", "name": file_name,
            "type": content_type, "size": size or len(content), "parent_type": parent_type,
        }
        self.uploads.append(attachment)
        return attachment


def template_clients():
    minimal = {
        "persona": {"视觉身份ID": "PERSONA_001", "视觉身份名称": "固定人设", "启用状态": "启用", "人设核心描述": "固定自然女生"},
        "scene": {"场景模板ID": "SCENE_A_001", "场景名称": "主场景", "启用状态": "停用", "核心场景描述": "固定卧室"},
        "action": {"动作模板ID": "ACT_001", "动作名称": "站立", "启用状态": "启用", "动作核心描述": "自然站立"},
        "styling": {"搭配模板ID": "STYLE_001", "搭配名称": "通勤", "启用状态": "启用", "搭配核心描述": "简洁搭配"},
        "subtitle": {"字幕模板ID": "SUB_TH_001", "字幕模板名称": "泰语", "启用状态": "启用", "字幕语言": "泰语", "开场字幕": "สวย"},
    }
    # 字幕表没有 prompt_core；同步层必须以字幕文本作为合法内容。
    clients = {role: FakeClient([{"record_id": f"rec_{role}", "fields": fields}]) for role, fields in minimal.items()}
    return clients


class FeishuSyncTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = LightTryonDB(Path(self.tmp.name) / "test.sqlite3")
        self.db.init_schema()
        self.db.seed_templates(SKILL_DIR / "assets" / "default_templates.json")

    def tearDown(self):
        self.tmp.cleanup()

    def test_mappings_have_unique_names_and_backends(self):
        for role, mapping in TABLE_MAPPINGS.items():
            names = [item["name"] for item in mapping.fields]
            backends = [item["backend"] for item in mapping.fields]
            self.assertEqual(len(names), len(set(names)), role)
            self.assertEqual(len(backends), len(set(backends)), role)
            self.assertEqual(names[0], mapping.primary_field)

    def test_schema_ensure_is_idempotent(self):
        clients = {role: FakeClient() for role in TABLE_MAPPINGS}
        dry = ensure_schema(clients, dry_run=True)
        self.assertTrue(dry["dry_run"])
        self.assertTrue(all(len(client.fields) == 1 for client in clients.values()))
        real = ensure_schema(clients)
        self.assertFalse(real["dry_run"])
        for role, client in clients.items():
            self.assertEqual(client.table_name, TABLE_MAPPINGS[role].title)
            self.assertEqual(len(client.fields), len(TABLE_MAPPINGS[role].fields))
        second = ensure_schema(clients)
        self.assertTrue(all(not any(a["operation"] == "create_field" for a in item["actions"]) for item in second["tables"].values()))

    def test_schema_ensure_adds_new_select_options_without_dropping_existing(self):
        clients = {role: FakeClient() for role in TABLE_MAPPINGS}
        ensure_schema(clients)
        scene_type = next(item for item in clients["scene"].fields if item.field_name == "场景类型")
        scene_type.property = {"options": [{"id": "opt_old", "name": "主场景全身", "color": 1}]}
        report = ensure_schema(clients)
        names = [item["name"] for item in scene_type.property["options"]]
        self.assertIn("主场景全身", names)
        self.assertIn("高亮上半身固定", names)
        self.assertIn("缓慢推近", names)
        action = next(
            item for item in report["tables"]["scene"]["actions"]
            if item["operation"] == "add_field_options" and item["field"] == "场景类型"
        )
        self.assertIn("缓慢推近", action["options"])

    def test_review_duration_field_is_renamed_without_duplicate(self):
        legacy = SimpleNamespace(field_id="fld_duration", field_name="目标时长", field_type=2, ui_type="Number", property=None)
        review = FakeClient(fields=[legacy])
        report = ensure_schema({"review": review})
        names = [item.field_name for item in review.fields]
        self.assertIn("视频时长", names)
        self.assertNotIn("目标时长", names)
        self.assertEqual(names.count("视频时长"), 1)
        self.assertTrue(any(item["operation"] == "rename_field" for item in report["tables"]["review"]["actions"]))

    def test_review_duplicate_cleanup_keeps_bound_record_and_backfills_actual_job_time(self):
        product = ProductInput.from_dict({"product_id": "SKU_DUP", "product_name": "去重测试", "market": "TH", "language": "th", "category": "top"})
        self.db.upsert_product(product)
        job = plan_product(self.db, "SKU_DUP", count=1)[0]
        self.db.create_jobs([job])
        payload = build_prompt(self.db.get_job_context(job.job_id))
        self.db.update_prompt(job.job_id, payload, PROMPT_BUILDER_VERSION)
        review = FakeClient([
            {"record_id": "rec_keep", "fields": {"视频任务ID": job.job_id}},
            {"record_id": "rec_delete", "fields": {"视频任务ID": job.job_id}},
        ])
        self.db.update_review_sync(job.job_id, "rec_keep", "synced")
        dry = cleanup_review_duplicates(self.db, review, dry_run=True)
        self.assertEqual(dry["planned_delete"], 1)
        self.assertEqual(len(review.records), 2)
        result = cleanup_review_duplicates(self.db, review)
        self.assertEqual(result["deleted"], 1)
        self.assertEqual([row.record_id for row in review.records], ["rec_keep"])
        push_reviews(self.db, review, job_ids=[job.job_id])
        self.assertIsInstance(review.records[0].fields["脚本创建时间"], int)

    def test_review_created_time_field_is_renamed_for_clarity(self):
        created = SimpleNamespace(field_id="fld_created", field_name="创建时间", field_type=1001, ui_type="CreatedTime", property=None)
        review = FakeClient(fields=[created])
        ensure_schema({"review": review})
        names = [item.field_name for item in review.fields]
        self.assertIn("飞书记录创建时间", names)
        self.assertIn("脚本创建时间", names)
        self.assertNotIn("创建时间", names)

    def test_run_manager_sync_preserves_exact_generation_contract_and_returns_video(self):
        source = FakeClient([{
            "record_id": "rec_source_queue",
            "fields": {
                "每方案视频数量": "每方案 1 个", "产品图片": [{"file_token": "product", "name": "product.jpg"}],
                "产品编码": "QUEUE_SKU", "一级类目": "女装", "目标国家": "泰国", "目标语言": "泰语", "产品类型": "上装",
                "店铺ID": "SHOP_TH_1",
            },
        }])
        process_source_requests(self.db, source)
        plan_id = self.db.get_source_request("rec_source_queue")["visual_plan_ids"][0]
        outfit = Path(self.tmp.name) / "queue-confirmed.jpg"
        outfit.write_bytes(b"confirmed-outfit")
        confirm_outfit_image(self.db, plan_id, image_path=str(outfit))
        process_source_requests(self.db, source)
        job = self.db.list_jobs(source_script_record_id="rec_source_queue")[0]
        review = FakeClient([{"record_id": "rec_review_queue", "fields": {}}])
        push_reviews(self.db, review, job_ids=[job["job_id"]])
        review.records[0].fields.update({
            "生成渠道": "iMini", "生成模型": "Seedance 2.0 VIP", "视频时长": 10,
        })
        preferences = pull_generation_preferences(self.db, review, job_ids=[job["job_id"]])
        self.assertEqual(preferences["updated"], 1)
        run = FakeClient(fields=[])
        ensure_run_manager_schema(run)
        queued = sync_jobs_to_run_manager(self.db, review, run, job_ids=[job["job_id"]])
        self.assertEqual(queued["created"], 1)
        fields = run.records[0].fields
        self.assertEqual(fields["脚本ID"], job["job_id"])
        self.assertEqual(fields["商品ID"], "QUEUE_SKU")
        self.assertEqual(fields["店铺ID"], "SHOP_TH_1")
        self.assertEqual(fields["模型"], "Seedance 2.0 VIP")
        self.assertEqual(fields["视频时长"], 10)
        self.assertEqual(fields["分辨率"], "720P")
        self.assertEqual(fields["渠道"], "iMini")
        self.assertEqual(fields["免参考图"], "否")
        self.assertEqual(fields["首帧策略"], "直接使用原始脚本参考图")
        self.assertEqual(run.uploads[0]["parent_type"], "bitable_image")
        self.assertIn("product", run.uploads[0]["name"])
        self.assertNotEqual(run.uploads[0]["name"], outfit.name)
        repeated = sync_jobs_to_run_manager(self.db, review, run, job_ids=[job["job_id"]])
        self.assertEqual(repeated["skipped"], 1)
        previous_fingerprint = fields["来源指纹"]
        product = self.db.get_product(job["product_id"])
        product["account_id"] = "SHOP_TH_2"
        self.db.upsert_product(product)
        identity_update = sync_jobs_to_run_manager(self.db, review, run, job_ids=[job["job_id"]])
        self.assertEqual(identity_update["updated"], 1)
        self.assertEqual(fields["店铺ID"], "SHOP_TH_2")
        self.assertNotEqual(fields["来源指纹"], previous_fingerprint)
        run.records[0].fields.update({
            "状态": "已完成", "结果回传状态": "uploaded", "最新追踪ID": "trace-queue",
            "生成视频": [{"file_token": "video-output", "name": "output.mp4", "type": "video/mp4", "size": 10}],
        })
        returned = pull_run_manager_results(self.db, review, run, job_ids=[job["job_id"]])
        self.assertEqual(returned["returned"], 1)
        self.assertEqual(review.records[0].fields["生成状态"], "生成成功")
        self.assertEqual(review.records[0].fields["队列同步状态"], "已回流")
        self.assertEqual(review.uploads[-1]["parent_type"], "bitable_file")
        stored = self.db.get_job(job["job_id"])
        self.assertEqual(stored["generation_model"], "Seedance 2.0 VIP")
        self.assertEqual(stored["duration_seconds"], 10)
        self.assertEqual(stored["run_manager_sync_status"], "returned")
        self.assertEqual(stored["run_manager_result_source_token"], "video-output")
        # 飞书附件在后处理/重传后可能不再携带 source_* 扩展信息；独立的来源 token
        # 仍应确保同一运行表结果不会被重复回流和再次渲染。
        raw_without_source_metadata = [
            {key: value for key, value in item.items() if not key.startswith("source_")}
            for item in stored["raw_video_attachments"]
        ]
        self.db.update_review_video_processing(
            job["job_id"], status="success", source_hash="postprocess-hash",
            raw_attachments=raw_without_source_metadata,
        )
        repeated_result = pull_run_manager_results(self.db, review, run, job_ids=[job["job_id"]])
        self.assertEqual(repeated_result["returned"], 0)
        self.assertEqual(repeated_result["skipped"], 1)

    def test_source_identity_is_required_and_submitted_job_is_never_reset(self):
        source = FakeClient([{
            "record_id": "rec_source_identity_guard",
            "fields": {
                "每方案视频数量": "每方案 1 个",
                "产品图片": [{"file_token": "product", "name": "product.jpg"}],
                "产品编码": "IDENTITY_SKU",
                "一级类目": "女装", "目标国家": "泰国", "目标语言": "泰语", "产品类型": "上装",
            },
        }])
        process_source_requests(self.db, source)
        plan_id = self.db.get_source_request("rec_source_identity_guard")["visual_plan_ids"][0]
        outfit = Path(self.tmp.name) / "identity-confirmed.jpg"
        outfit.write_bytes(b"confirmed-outfit")
        confirm_outfit_image(self.db, plan_id, image_path=str(outfit))
        process_source_requests(self.db, source)
        job = self.db.list_jobs(source_script_record_id="rec_source_identity_guard")[0]
        review = FakeClient([{"record_id": "rec_review_identity", "fields": {}}])
        push_reviews(self.db, review, job_ids=[job["job_id"]])
        review.records[0].fields["生成渠道"] = "即梦"
        pull_generation_preferences(self.db, review, job_ids=[job["job_id"]])
        run = FakeClient(fields=[])
        missing_store = sync_jobs_to_run_manager(self.db, review, run, job_ids=[job["job_id"]])
        self.assertEqual(missing_store["failed"], 1)
        self.assertIn("缺少店铺ID", missing_store["items"][0]["error"])

        product = self.db.get_product(job["product_id"])
        product["account_id"] = "SHOP_TH_GUARD"
        self.db.upsert_product(product)
        queued = sync_jobs_to_run_manager(self.db, review, run, job_ids=[job["job_id"]])
        self.assertEqual(queued["created"], 1)
        run.records[0].fields.update({
            "商品ID": "OSG_rec_source_identity_guard", "店铺ID": "myps01", "来源指纹": "stale",
            "状态": "已提交", "执行归属": "worker", "已提交次数": 1, "最新追踪ID": "trace-active",
        })
        protected = sync_jobs_to_run_manager(self.db, review, run, job_ids=[job["job_id"]])
        self.assertEqual(protected["blocked"], 1)
        self.assertEqual(run.records[0].fields["状态"], "已提交")
        self.assertEqual(run.records[0].fields["执行归属"], "worker")
        self.assertEqual(run.records[0].fields["最新追踪ID"], "trace-active")

        # A historical duplicate must not stop every other task. The locally
        # bound record remains authoritative and neither active row is reset.
        run.records.append(SimpleNamespace(
            record_id="rec_historical_duplicate",
            fields={
                **run.records[0].fields,
                "状态": "已提交", "最新追踪ID": "trace-historical",
            },
        ))
        duplicate_safe = sync_jobs_to_run_manager(self.db, review, run, job_ids=[job["job_id"]])
        self.assertEqual(duplicate_safe["blocked"], 1)
        self.assertEqual(run.records[0].fields["最新追踪ID"], "trace-active")
        self.assertEqual(run.records[1].fields["最新追踪ID"], "trace-historical")
        run.records[1].fields.update({
            "状态": "已完成", "结果回传状态": "uploaded",
            "生成视频": [{"file_token": "historical-video", "name": "historical.mp4"}],
        })
        historical_result = pull_run_manager_results(self.db, review, run, job_ids=[job["job_id"]])
        self.assertEqual(historical_result["returned"], 1)
        self.assertEqual(self.db.get_job(job["job_id"])["run_manager_record_id"], "rec_historical_duplicate")

    def test_run_manager_identity_rejects_internal_source_key_and_keeps_legacy_fallback(self):
        with self.assertRaisesRegex(ValueError, "内部商品键"):
            resolve_run_manager_identity({
                "product_id": "OSG_rec_bad",
                "source_script_record_id": "rec_bad",
                "source_product_code": "OSG_rec_bad",
                "account_id": "SHOP_TH_1",
            }, "myps01")
        self.assertEqual(
            resolve_run_manager_identity({"product_id": "LEGACY_SKU"}, "LEGACY_STORE"),
            ("LEGACY_SKU", "LEGACY_STORE"),
        )

    def test_template_pull_updates_disabled_and_then_skips(self):
        clients = template_clients()
        first = pull_templates(self.db, clients)
        self.assertEqual(first["failed"], 0)
        self.assertEqual(self.db.get_template("scene", "SCENE_A_001")["status"], "disabled")
        second = pull_templates(self.db, clients)
        self.assertGreaterEqual(second["skipped"], 5)

    def test_shot_plan_round_trip_preserves_order_and_duplicates(self):
        client = FakeClient()
        ensure_schema({"shot_plan": client})
        initialize_template_records(self.db, {"shot_plan": client}, roles=["shot_plan"])
        original = self.db.get_template("shot_plan", "SHOTPLAN_OUTERWEAR_FOCUS")["five_sequence"]
        pull_templates(self.db, {"shot_plan": client}, roles=["shot_plan"])
        restored = self.db.get_template("shot_plan", "SHOTPLAN_OUTERWEAR_FOCUS")["five_sequence"]
        self.assertEqual(restored, original)
        self.assertEqual(restored.count("SHOT_UPPER_FIXED"), 2)
        self.assertEqual(restored.count("SHOT_UPPER_THREE_QUARTER"), 2)

    def test_review_round_trip_and_regeneration_are_idempotent(self):
        product = ProductInput.from_dict({"product_id": "SKU_1", "product_name": "测试上衣", "market": "TH", "language": "th", "category": "top"})
        self.db.upsert_product(product)
        jobs = plan_product(self.db, "SKU_1", count=1)
        self.db.create_jobs(jobs)
        job_id = jobs[0].job_id
        payload = build_prompt(self.db.get_job_context(job_id))
        self.db.update_prompt(job_id, payload, PROMPT_BUILDER_VERSION)
        review = FakeClient([{"record_id": "rec_blank", "fields": {}}])
        pushed = push_reviews(self.db, review)
        self.assertEqual(pushed["created"], 1)
        self.assertEqual(review.records[0].fields["完整Prompt"], payload["display_prompt"])
        self.assertNotIn("template_snapshots", review.records[0].fields["完整Prompt"])
        review.records[0].fields.update({
            "人工复核状态": "打回", "人工复核原因": "动作不自然", "是否需要补生成": "是",
            "补生成策略": "更换动作", "发布状态": "暂停发布", "运营备注": "重跑一次",
        })
        pulled = pull_manual_reviews(self.db, review)
        self.assertEqual(pulled["processed"], 1)
        self.assertEqual(pulled["regenerated"], 1)
        child_id = review.records[0].fields["补生成任务ID"]
        self.assertEqual(self.db.get_job(child_id)["parent_job_id"], job_id)
        repeated = pull_manual_reviews(self.db, review)
        self.assertEqual(repeated["processed"], 0)
        self.assertEqual(repeated["regenerated"], 0)
        # 系统回推不能覆盖运营人工字段。
        push_reviews(self.db, review)
        self.assertEqual(review.records[0].fields["人工复核原因"], "动作不自然")

    def test_review_initial_video_is_postprocessed_and_uploaded_idempotently(self):
        product = ProductInput.from_dict({
            "product_id": "SKU_VIDEO", "product_name": "测试外套", "market": "TH",
            "language": "th", "category": "outerwear",
        })
        self.db.upsert_product(product)
        job = plan_product(self.db, "SKU_VIDEO", count=1)[0]
        self.db.create_jobs([job])
        payload = build_prompt(self.db.get_job_context(job.job_id))
        payload["subtitle_plan"] = {"cues": [{"start": 1.0, "end": 2.0, "text": "สวย"}]}
        payload["brand_plan"] = {"enabled": True, "display_name": "LikeU"}
        self.db.update_prompt(job.job_id, payload, PROMPT_BUILDER_VERSION)
        review = FakeClient([{
            "record_id": "rec_video",
            "fields": {
                "视频任务ID": job.job_id,
                "初始成片": [{"file_token": "raw_1", "name": "initial.mp4", "size": 10, "type": "video/mp4"}],
            },
        }])

        calls = []

        def fake_renderer(input_video, output_video, subtitle_plan, brand_plan, **kwargs):
            calls.append((input_video, subtitle_plan, brand_plan))
            Path(output_video).write_bytes(b"rendered-video")
            Path(kwargs["cover_output"]).write_bytes(b"cover")
            return {"output_video_path": str(output_video), "output_cover_path": str(kwargs["cover_output"])}

        first = process_review_videos(self.db, review, renderer=fake_renderer)
        self.assertEqual(first["processed"], 1)
        self.assertEqual(first["failed"], 0)
        self.assertEqual(review.records[0].fields["最终视频"][0]["file_token"], "uploaded_1")
        self.assertEqual(review.uploads[0]["parent_type"], "bitable_file")
        stored = self.db.get_job(job.job_id)
        self.assertEqual(stored["review_video_process_status"], "success")
        self.assertTrue(Path(stored["raw_video_path"]).is_file())
        self.assertTrue(Path(stored["output_video_path"]).is_file())
        self.assertEqual(len(calls), 1)

        repeated = process_review_videos(self.db, review, renderer=fake_renderer)
        self.assertEqual(repeated["processed"], 0)
        self.assertEqual(repeated["skipped"], 1)
        self.assertEqual(len(calls), 1)

    def test_source_schema_reuses_legacy_dropdown(self):
        fields = [SimpleNamespace(field_id="fld_legacy", field_name="是否跑轻模型", field_type=3, ui_type="SingleSelect", property={})]
        source = FakeClient(fields=fields)
        dry = ensure_source_schema(source, dry_run=True)
        self.assertEqual(dry["actions"][0]["operation"], "migrate_field")
        ensure_source_schema(source, db=self.db)
        names = [item.field_name for item in source.fields]
        self.assertNotIn("是否跑轻模型", names)
        self.assertIn("每方案视频数量", names)
        self.assertEqual(len([name for name in names if name == "每方案视频数量"]), 1)
        self.assertIn("轻量视频场景", names)
        self.assertIn("轻量视频搭配", names)
        scene_field = next(item for item in source.fields if item.field_name == "轻量视频场景")
        styling_field = next(item for item in source.fields if item.field_name == "轻量视频搭配")
        self.assertEqual(scene_field.field_type, 4)
        self.assertEqual(styling_field.field_type, 4)
        scene_options = [item["name"] for item in scene_field.property["options"]]
        self.assertIn("现代简约卧室", scene_options)
        self.assertIn("明亮现代咖啡店", scene_options)

    def test_source_trigger_one_to_five_is_idempotent(self):
        source = FakeClient([{
            "record_id": "rec_source_1",
            "fields": {
                "每方案视频数量": "每方案 1 个",
                "产品图片": [{"file_token": "token_a", "name": "a.jpg"}],
                "产品编码": "DUPLICATE_SKU",
                "一级类目": "女装",
                "目标国家": "泰国",
                "目标语言": "泰语",
                "产品类型": "上装",
                "店铺ID": "SHOP_TH_1",
            },
        }])
        first = process_source_requests(self.db, source)
        self.assertEqual(first["created_visual_plans"], 1)
        self.assertEqual(first["created_jobs"], 0)
        self.assertEqual(source.records[0].fields["轻量视频状态"], "待首帧生成")
        repeated = process_source_requests(self.db, source)
        self.assertEqual(repeated["created_jobs"], 0)
        self.assertEqual(repeated["skipped"], 1)
        plan_id = self.db.get_source_request("rec_source_1")["visual_plan_ids"][0]
        outfit = Path(self.tmp.name) / "confirmed.jpg"
        outfit.write_bytes(b"confirmed")
        confirm_outfit_image(self.db, plan_id, image_path=str(outfit))
        confirmed = process_source_requests(self.db, source)
        self.assertEqual(confirmed["created_jobs"], 1)
        source.records[0].fields["每方案视频数量"] = "每方案 5 个"
        expanded = process_source_requests(self.db, source)
        self.assertEqual(expanded["created_jobs"], 4)
        self.assertEqual(len(self.db.list_jobs(source_script_record_id="rec_source_1")), 5)
        source.records[0].fields["每方案视频数量"] = "每方案 1 个"
        reduced = process_source_requests(self.db, source)
        self.assertEqual(reduced["created_jobs"], 0)
        self.assertEqual(len(self.db.list_jobs(source_script_record_id="rec_source_1")), 5)
        source.records[0].fields["每方案视频数量"] = "不生成"
        disabled = process_source_requests(self.db, source)
        self.assertEqual(disabled["disabled"], 1)
        self.assertEqual(len(self.db.list_jobs(source_script_record_id="rec_source_1")), 5)

    def test_source_enhanced_only_request_plans_from_existing_hook_library(self):
        source = FakeClient([{
            "record_id": "rec_source_enhanced",
            "fields": {
                "每方案视频数量": "不生成",
                "视频组合策略": "口播增强",
                "口播增强视频数量": "生成 5 个",
                "口播测试方向": ["版型", "细节"],
                "产品图片": [{"file_token": "token_enhanced", "name": "enhanced.jpg"}],
                "产品编码": "ENHANCED_SKU",
                "产品名称": "短款外套",
                "产品卖点说明": ["短款版型", "金属拉链细节"],
                "一级类目": "女装",
                "目标国家": "泰国",
                "目标语言": "泰语",
                "产品类型": "外套",
            },
        }])
        hooks = [
            {"hook_id": "GENERAL_PRODUCT_SHARE", "hook_name": "通用轻分享型"},
            {"hook_id": "DETAIL_SURPRISE", "hook_name": "细节惊喜型"},
        ]
        with patch("light_tryon.source_script_sync.load_active_voiceover_hooks", return_value=hooks), patch(
            "light_tryon.source_script_sync.load_product_voiceover_usage",
            return_value={"recent_count": 0},
        ):
            first = process_source_requests(self.db, source)
            second = process_source_requests(self.db, source)
        self.assertEqual(first["created_narrative_variants"], 5)
        self.assertEqual(second["created_narrative_variants"], 0)
        self.assertEqual(second["existing_narrative_variants"], 5)
        product_id = self.db.get_source_request("rec_source_enhanced")["product_id"]
        variants = self.db.list_narrative_variants(product_id)
        self.assertEqual(len(variants), 5)
        self.assertEqual(source.records[0].fields["预计视频总数"], 5)
        self.assertEqual(source.records[0].fields["轻量视频状态"], "待编排")

    def test_source_scene_change_creates_new_active_config_without_mixing_old_jobs(self):
        source = FakeClient([{
            "record_id": "rec_source_scene",
            "fields": {
                "每方案视频数量": "每方案 5 个",
                "轻量视频场景": ["现代简约卧室"],
                "轻量视频搭配": ["日常上衣加直筒牛仔裤"],
                "产品图片": [{"file_token": "token_scene", "name": "scene.jpg"}],
                "产品编码": "SCENE_SKU", "一级类目": "女装", "目标国家": "泰国",
                "目标语言": "泰语", "产品类型": "上装",
            },
        }])
        first = process_source_requests(self.db, source)
        self.assertEqual(first["created_visual_plans"], 1)
        first_plan = self.db.get_source_request("rec_source_scene")["visual_plan_ids"][0]
        outfit = Path(self.tmp.name) / "scene-confirmed.jpg"
        outfit.write_bytes(b"confirmed")
        confirm_outfit_image(self.db, first_plan, image_path=str(outfit))
        generated = process_source_requests(self.db, source)
        self.assertEqual(generated["created_jobs"], 5)
        first_ids = {job["job_id"] for job in self.db.list_jobs(source_script_record_id="rec_source_scene")}
        source.records[0].fields["轻量视频场景"] = ["明亮现代咖啡店"]
        second = process_source_requests(self.db, source)
        self.assertEqual(second["created_visual_plans"], 1)
        self.assertEqual(second["created_jobs"], 0)
        active = self.db.list_visual_plans(source_record_id="rec_source_scene", plan_status="active")
        self.assertEqual([row["scene_id"] for row in active], ["ENV_CAFE_001"])
        self.assertEqual(len(self.db.list_jobs(source_script_record_id="rec_source_scene")), 5)
        self.assertEqual(len(first_ids), 5)

    def test_same_product_code_uses_independent_source_identity(self):
        base_fields = {
            "每方案视频数量": "每方案1个",
            "产品图片": [{"file_token": "token_same", "name": "same.jpg"}],
            "产品编码": "SAME_SKU",
            "一级类目": "女装",
            "目标国家": "泰国",
            "目标语言": "泰语",
            "产品类型": "上装",
        }
        source = FakeClient([
            {"record_id": "rec_a", "fields": dict(base_fields)},
            {"record_id": "rec_b", "fields": dict(base_fields)},
        ])
        result = process_source_requests(self.db, source)
        self.assertEqual(result["created_visual_plans"], 2)
        self.assertEqual(result["created_jobs"], 0)
        self.assertNotEqual(self.db.get_source_request("rec_a")["product_id"], self.db.get_source_request("rec_b")["product_id"])

    def test_source_trigger_rejects_missing_image(self):
        source = FakeClient([{
            "record_id": "rec_no_image",
            "fields": {
                "每方案视频数量": "每方案 1 个", "产品编码": "SKU_NO_IMAGE", "一级类目": "女装",
                "目标国家": "泰国", "目标语言": "泰语", "产品类型": "上装",
            },
        }])
        result = process_source_requests(self.db, source)
        self.assertEqual(result["failed"], 1)
        self.assertIn("产品图片为空", source.records[0].fields["轻量视频错误信息"])

    def test_source_trigger_does_not_fallback_to_wrong_subtitle_language(self):
        source = FakeClient([{
            "record_id": "rec_vi",
            "fields": {
                "每方案视频数量": "每方案 1 个",
                "产品图片": [{"file_token": "token_vi", "name": "vi.jpg"}],
                "产品编码": "SKU_VI", "一级类目": "女装", "目标国家": "越南",
                "目标语言": "越南语", "产品类型": "上装",
            },
        }])
        result = process_source_requests(self.db, source)
        self.assertEqual(result["created_visual_plans"], 1)
        plan_id = self.db.get_source_request("rec_vi")["visual_plan_ids"][0]
        outfit = Path(self.tmp.name) / "vi-confirmed.jpg"
        outfit.write_bytes(b"confirmed")
        confirm_outfit_image(self.db, plan_id, image_path=str(outfit))
        failed = process_source_requests(self.db, source)
        self.assertEqual(failed["failed"], 1)
        self.assertIn("字幕模板", source.records[0].fields["轻量视频错误信息"])

    def test_find_and_set_source_request_by_product_id(self):
        source = FakeClient([{
            "record_id": "rec_product_id",
            "fields": {"产品ID": "173", "产品编码": "SKU173", "每方案视频数量": ""},
        }])
        matches = find_source_records(source, "173")
        self.assertEqual([item["record_id"] for item in matches], ["rec_product_id"])
        result = set_source_request(source, "173", 1)
        self.assertEqual(result["record_id"], "rec_product_id")
        self.assertEqual(source.records[0].fields["每方案视频数量"], "每方案 1 个")

    def test_visual_plan_feishu_confirmation_creates_gated_jobs(self):
        product = ProductInput.from_dict({
            "product_id": "VP_SKU", "product_name": "视觉方案上衣", "market": "TH", "language": "th",
            "category": "top", "product_images": ["https://example.com/product.jpg"],
        })
        self.db.upsert_product(product)
        plan = orchestrate_visual_plans(
            self.db, "VP_SKU", source_record_id="rec_vp", scene_values=["现代简约卧室"],
            styling_values=["日常上衣加直筒牛仔裤"], per_plan_video_count=5,
        )[0]
        visual_table = FakeClient([{"record_id": "rec_blank", "fields": {}}])
        pushed = push_visual_plans(self.db, visual_table)
        self.assertEqual(pushed["created"], 1)
        self.assertEqual(visual_table.records[0].fields["视觉方案ID"], plan["visual_plan_id"])
        self.assertEqual(self.db.list_jobs(source_script_record_id="rec_vp"), [])
        visual_table.records[0].fields.update({
            "穿搭图状态": "已确认",
            "产品穿搭图": [{"file_token": "outfit_token", "name": "confirmed.jpg"}],
        })
        pulled = pull_visual_plan_reviews(self.db, visual_table)
        self.assertEqual(pulled["confirmed"], 1)
        self.assertEqual(pulled["created_jobs"], 5)
        self.assertEqual(len(self.db.list_jobs(source_script_record_id="rec_vp")), 5)
        confirmed_plan = self.db.get_visual_plan(plan["visual_plan_id"])
        self.assertTrue(Path(confirmed_plan["outfit_image_path"]).is_file())

    def test_scene_reference_attachment_is_cached_during_template_pull(self):
        scene_client = FakeClient([{
            "record_id": "rec_scene_ref",
            "fields": {
                "场景模板ID": "SCENE_A_001", "场景名称": "现代简约卧室", "启用状态": "启用",
                "核心场景描述": "固定卧室", "场景参考图": [{"file_token": "scene_token", "name": "scene.jpg"}],
            },
        }])
        result = pull_templates(self.db, {"scene": scene_client}, roles=["scene"])
        self.assertEqual(result["failed"], 0)
        references = self.db.get_template("scene", "SCENE_A_001")["reference_images"]
        self.assertEqual(len(references), 1)
        self.assertTrue(Path(references[0]).is_file())

    def test_persona_brand_logo_is_cached_during_template_pull(self):
        persona_client = FakeClient([{
            "record_id": "rec_persona_brand",
            "fields": {
                "视觉身份ID": "PERSONA_001", "视觉身份名称": "固定人设", "启用状态": "启用",
                "人设核心描述": "固定自然女生", "品牌叠加状态": "启用",
                "品牌展示名称": "TEST STUDIO", "品牌视觉预设": "奶油衬线", "品牌主色": "奶油白",
                "店铺Logo": [{"file_token": "brand_logo_token", "name": "logo.png"}],
            },
        }])
        result = pull_templates(self.db, {"persona": persona_client}, roles=["persona"])
        self.assertEqual(result["failed"], 0)
        persona = self.db.get_template("persona", "PERSONA_001")
        self.assertEqual(persona["brand_overlay_enabled"], "enabled")
        self.assertEqual(persona["brand_display_name"], "TEST STUDIO")
        self.assertEqual(persona["brand_style_preset"], "cream_serif")
        self.assertEqual(len(persona["brand_logo_images"]), 1)
        self.assertTrue(Path(persona["brand_logo_images"][0]).is_file())

    def test_legacy_standard_environment_reference_is_cached_during_template_pull(self):
        scene_client = FakeClient([{
            "record_id": "rec_scene_legacy_ref",
            "fields": {
                "场景模板ID": "SCENE_A_001", "场景名称": "室内INS奶油风", "启用状态": "启用",
                "核心场景描述": "奶油风试穿空间",
                "标准环境参考图": [{"file_token": "legacy_scene_token", "name": "legacy-scene.jpg"}],
            },
        }])
        result = pull_templates(self.db, {"scene": scene_client}, roles=["scene"])
        self.assertEqual(result["failed"], 0)
        references = self.db.get_template("scene", "SCENE_A_001")["reference_images"]
        self.assertEqual(len(references), 1)
        self.assertTrue(Path(references[0]).is_file())


if __name__ == "__main__":
    unittest.main()
