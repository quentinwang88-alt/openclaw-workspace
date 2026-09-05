from __future__ import annotations

import copy
import sys
import unittest
from tempfile import TemporaryDirectory
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT.parents[1] / "packages" / "wig_success_replication"))

from script_pool_export import ScriptPoolWriter, build_pool_fields, content_digest, register_metadata
from script_pool_schema import build_pool_plan, FIELDS, EXTENSIONS, VIEWS, view_property
from wig_success_replication.repository import InMemoryRepository
from wig_success_replication.reference_manifest import build_reference_manifest, bytes_sha256


def sample(purpose="带货", cart=True, identity="prompt_a"):
    return {"prompt_id": identity, "mother_id": "m1", "mother_version": 2,
            "product_id": "S260724014590", "sequence_no": 4, "replication_mode": "general",
            "creative_route": "G1", "full_prompt": "人物参考图只参考脸部。Hola México.",
            "voiceover_text": "Hola México.", "publish_purpose": purpose, "cart_enabled": cart,
            "handoff_context": {"mother_name": "测试母版",
                                "product_images": [{"file_token": "product"}],
                                "persona_images": [{"file_token": "person"}]}}


class FakeFeishu:
    def __init__(self):
        self.rows = {}
        self.uploaded = []
        self.scans = 0
        self.ambiguous = False

    def list_records(self, app, table, page_size=500):
        assert page_size == 500
        self.scans += 1
        return list(self.rows.values())

    def get_record(self, app, table, record):
        return self.rows[record]

    def copy_attachment_to_base(self, attachment, app):
        self.uploaded.append(attachment["file_token"])
        return {"file_token": "copied_" + attachment["file_token"]}

    def create_record(self, app, table, fields):
        row = {"record_id": f"rec{len(self.rows)+1}", "fields": copy.deepcopy(fields)}
        self.rows[row["record_id"]] = row
        if self.ambiguous:
            self.ambiguous = False
            raise TimeoutError("server accepted, response lost")
        return {"record": row}


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.repo = InMemoryRepository()
        self.client = FakeFeishu()
        self.registered = []
        self.writer = ScriptPoolWriter(
            self.client, self.repo,
            metadata_registrar=lambda fields, payload: self.registered.append(fields) or {"status": "ok"},
            pool_app_token="pool",
        )

    def test_all_four_combinations_keep_content_and_do_not_start_production(self):
        for purpose in ("带货", "养号"):
            for cart in (True, False):
                payload = sample(purpose, cart)
                fields = build_pool_fields(payload, [], [])
                self.assertEqual(fields["发布用途"], purpose)
                self.assertEqual(fields["是否挂车"], "是" if cart else "否")
                self.assertEqual(fields["视频生成提示词"], payload["full_prompt"])
                self.assertFalse(fields["进入生产"])
                self.assertNotIn("平台商品ID", fields)

    def test_metadata_first_registration_after_feishu_retry_then_preserves_mapping(self):
        payload = sample()
        payload["handoff_context"]["platform_product_id"] = "1234567890123456789"
        fields = {**build_pool_fields(payload, [], []), "_record_id": "rec1", "_first_pool_delivery": False}
        with TemporaryDirectory() as temp:
            path = str(Path(temp) / "publish.sqlite3")
            first = register_metadata(fields, payload, path)
            self.assertEqual(first["platform_product_id"], "1234567890123456789")
            from app.db import AutoPublishDB
            db = AutoPublishDB(Path(path))
            with db._connect() as connection:
                connection.execute("UPDATE script_pool_bindings SET platform_product_id=?", ("2234567890123456789",))
            retried = register_metadata(fields, payload, path)
            self.assertEqual(retried["platform_product_id"], "2234567890123456789")
            self.assertEqual(db.get_script_metadata(fields["脚本ID"])["content_branch"], "DIRECT_RESPONSE")

    def test_nurture_metadata_stays_nurture_without_fabricated_publish_title(self):
        payload = sample("养号", False)
        fields = {**build_pool_fields(payload, [], []), "_record_id": "rec2"}
        with TemporaryDirectory() as temp:
            path = str(Path(temp) / "publish.sqlite3")
            registered = register_metadata(fields, payload, path)
            from app.db import AutoPublishDB
            metadata = AutoPublishDB(Path(path)).get_script_metadata(fields["脚本ID"])
            self.assertEqual(metadata["content_branch"], "NURTURE")
            self.assertEqual(metadata["cart_enabled"], "否")
            self.assertFalse(metadata["short_video_title"])
            self.assertEqual(registered["platform_product_id"], "")

    def test_repeat_preserves_human_decision_cart_and_prompt(self):
        payload = sample()
        self.writer.apply("upsert_script_pool", payload)
        fields = self.client.rows["rec1"]["fields"]
        fields.update({"是否挂车": "否", "处理状态": "不采用", "视频生成提示词": "人工改稿"})
        self.writer.apply("upsert_script_pool", payload)
        self.assertEqual(len(self.client.rows), 1)
        self.assertEqual(self.registered[-1]["视频生成提示词"], "人工改稿")
        self.assertEqual(self.registered[-1]["是否挂车"], "否")
        self.assertEqual(self.client.uploaded, ["product", "person"])

    def test_ambiguous_create_reconciles_stable_id(self):
        self.client.ambiguous = True
        with self.assertRaises(TimeoutError):
            self.writer.apply("upsert_script_pool", sample())
        self.writer.apply("upsert_script_pool", sample())
        self.assertEqual(len(self.client.rows), 1)
        self.assertEqual(self.client.scans, 2)
        self.assertEqual(len(self.client.uploaded), 2)

    def test_metadata_failure_does_not_duplicate_or_upload_again(self):
        original = self.writer.metadata_registrar
        self.writer.metadata_registrar = lambda *args: (_ for _ in ()).throw(RuntimeError("database busy"))
        with self.assertRaises(RuntimeError):
            self.writer.apply("upsert_script_pool", sample())
        self.writer.metadata_registrar = original
        self.writer.apply("upsert_script_pool", sample())
        self.assertEqual(len(self.client.rows), 1)
        self.assertEqual(len(self.client.uploaded), 2)

    def test_source_revision_conflict_does_not_overwrite(self):
        self.writer.apply("upsert_script_pool", sample())
        changed = sample()
        changed["full_prompt"] = "different source revision"
        with self.assertRaisesRegex(RuntimeError, "source content changed"):
            self.writer.apply("upsert_script_pool", changed)
        self.assertEqual(self.client.rows["rec1"]["fields"]["视频生成提示词"], sample()["full_prompt"])

    def test_missing_snapshot_rejected_and_shared_images_reused(self):
        missing = sample()
        del missing["handoff_context"]
        with self.assertRaisesRegex(ValueError, "frozen handoff"):
            self.writer.apply("upsert_script_pool", missing)
        self.writer.apply("upsert_script_pool", sample())
        self.writer.apply("upsert_script_pool", sample(identity="prompt_b"))
        self.assertEqual(len(self.client.rows), 2)
        self.assertEqual(len(self.client.uploaded), 2)

    def test_no_product_and_no_voiceover_nurture_is_not_fabricated(self):
        payload = sample("养号", False)
        payload.update(product_id="", voiceover_text="")
        fields = build_pool_fields(payload, [], [])
        self.assertEqual(fields["产品编码"], "")
        self.assertEqual(fields["口播_目标语言"], "")

    def test_cart_override_does_not_change_source_content_digest(self):
        a = sample()
        b = sample(cart=False)
        self.assertEqual(content_digest(a), content_digest(b))

    def test_manifest_transfer_preserves_hash_but_rebinds_pool_tokens(self):
        payload = sample()
        assets = [{"index": 1, "role": "person_identity", "file_token": "person", "original_sha256": bytes_sha256(b"person")},
                  {"index": 2, "role": "product", "file_token": "product", "original_sha256": bytes_sha256(b"product")}]
        manifest = build_reference_manifest(assets)
        payload["handoff_context"]["reference_manifest"] = manifest
        original = self.client.copy_attachment_to_base
        def verified_copy(attachment, app):
            return {**original(attachment, app), "original_sha256": attachment["original_sha256"]}
        self.client.copy_attachment_to_base = verified_copy
        self.writer.apply("upsert_script_pool", payload)
        binding = self.repo.get_script_pool_binding(payload["prompt_id"])
        transferred = binding.metadata["reference_manifest"]
        self.assertEqual(transferred["manifest_id"], manifest["manifest_id"])
        self.assertEqual([a["file_token"] for a in transferred["reference_assets"]], ["copied_person", "copied_product"])
        self.assertNotIn("original_sha256", self.client.rows["rec1"]["fields"]["产品图片"][0])
        self.writer.apply("upsert_script_pool", payload)
        self.assertEqual(len(self.client.uploaded), 2)

    def test_missing_transfer_hash_does_not_create_pool_row(self):
        payload = sample()
        payload["handoff_context"]["reference_manifest"] = build_reference_manifest([
            {"index": 1, "role": "person_identity", "file_token": "person", "original_sha256": bytes_sha256(b"person")},
            {"index": 2, "role": "product", "file_token": "product", "original_sha256": bytes_sha256(b"product")},
        ])
        with self.assertRaisesRegex(ValueError, "verify original SHA256"):
            self.writer.apply("upsert_script_pool", payload)
        self.assertEqual(self.client.rows, {})

    def test_revision_delivery_freezes_context_without_a_cumulative_slot(self):
        payload = sample(identity="revision_123")
        payload["sequence_no"] = 0
        payload["handoff_context"].update(revision_kind="test", parent_prompt_id="prompt_parent",
            mother_core_points=[{"point_id": "mask", "requirement": "原拍法"}],
            effective_checkpoints=[{"point_id": "mask", "requirement": "新拍法"}])
        self.writer.apply("upsert_script_pool", payload)
        self.writer.apply("upsert_script_pool", payload)
        self.assertEqual(len(self.client.rows), 1)
        self.assertEqual(self.repo.prompts, {}, "修订不占用累计生成序号")
        fields = self.client.rows["rec1"]["fields"]
        self.assertTrue(fields["脚本标题"].startswith("对照测试｜"))
        self.assertFalse(fields["进入生产"])
        binding = self.repo.get_script_pool_binding("revision_123")
        frozen = binding.metadata["frozen_handoff_context"]
        self.assertEqual(frozen["mother_id"], "m1")
        self.assertEqual(frozen["mother_version"], 2)
        self.assertEqual(frozen["effective_checkpoints"], payload["handoff_context"]["effective_checkpoints"])


class PoolSchemaTests(unittest.TestCase):
    def state(self):
        fields = [{"field_name": n, "field_id": f"f{i}", "type": t,
                   "property": {"options": [{"name": o, "id": f"o{i}{j}"} for j, o in enumerate(opts)]}}
                  for i, (n, t, opts, _) in enumerate(FIELDS)]
        fields += [{"field_name": n, "field_id": n, "type": 3,
                    "property": {"options": [{"name": o, "id": o} for o in opts]}}
                   for n, opts in EXTENSIONS.items()]
        fields += [{"field_name": "目标国家", "field_id": "country", "type": 1},
                   {"field_name": "处理状态", "field_id": "state", "type": 3,
                    "property": {"options": [{"name": o, "id": o} for o in ["待审核", "已选用"]]}}]
        return {"fields": fields, "views": [{"view_id": "operator", "view_name": "原运营视图"}]}

    def test_only_new_views_are_configured_and_second_plan_empty(self):
        state = self.state()
        actions = build_pool_plan(state)
        self.assertFalse(any(a.get("name") == "原运营视图" for a in actions))
        self.assertEqual(sum(a["op"] == "create-view" for a in actions), 4)
        state["views"] += [{"view_name": n, "view_id": n, "property": view_property(n, state["fields"])} for n in VIEWS]
        self.assertEqual(build_pool_plan(state), [])

    def test_server_decorated_properties_and_two_status_or_filter(self):
        state = self.state()
        for name in VIEWS:
            prop = view_property(name, state["fields"])
            prop["filter_info"]["condition_omitted"] = None
            for condition in prop["filter_info"]["conditions"]:
                condition.update({"condition_id": "server_id", "field_type": 3})
            state["views"].append({"view_name": name, "property": prop})
        self.assertEqual(build_pool_plan(state), [])
        ready = view_property("总库·待送生产", state["fields"])["filter_info"]
        self.assertEqual(ready["conjunction"], "or")
        self.assertEqual(len(ready["conditions"]), 2)


if __name__ == "__main__":
    unittest.main()
