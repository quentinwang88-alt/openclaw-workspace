import hashlib
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, MagicMock, patch

from core.reference_manifest_handoff import bind_transferred_wsr_references, build_reference_manifest, load_frozen_wsr_context
from run_pipeline import transfer_reference_images


class ReferenceManifestHandoffTest(unittest.TestCase):
    def setup_transfer(self, *, raw=b"original", actual=b"compressed"):
        original, derived = hashlib.sha256(raw).hexdigest(), hashlib.sha256(actual).hexdigest()
        source = {"file_token": "src-token", "name": "same.jpg"}
        task = SimpleNamespace(script_source="成功脚本复刻", source_record_id="recSource",
            reference_images=[source], persona_contract=json.dumps({"reference_assets": [{"index": 1, "role": "product"}]}))
        manifest = build_reference_manifest([{"index": 1, "role": "product", "file_token": "src-token",
            "original_sha256": original, "derived_sha256": derived, "transform": "compiler-jpeg1024-v1"}])
        source_client, target_client = Mock(), Mock()
        source_client.download_attachment_bytes.return_value = (actual, "same.jpg", "image/jpeg", len(actual))
        target_client.upload_attachment.return_value = {"file_token": "target-token", "name": "same.jpg"}
        cache = {}
        refs = transfer_reference_images(source_client, target_client, [source], cache)
        return task, manifest, cache, {"参考图": refs, "提示词": "人工正文不改"}, source_client, target_client

    def test_raw_identity_and_transferred_hash_are_separate(self):
        task, manifest, cache, fields, _, _ = self.setup_transfer()
        result = bind_transferred_wsr_references(task, fields, {"reference_images": "参考图", "persona_contract": "人物模板合同"}, cache,
            {"reference_manifest": manifest, "mother_id": "m1", "mother_version": 2,
             "mother_core_points": [{"point_id": "p1", "requirement": "实体遮镜", "evidence_source": "mother"}], "provenance": "frozen_mother"})
        contract = json.loads(result["人物模板合同"])
        self.assertEqual(contract["schema_version"], "2")
        self.assertEqual(contract["reference_manifest"]["manifest_id"], manifest["manifest_id"])
        asset = contract["reference_assets"][0]
        self.assertEqual(asset["file_token"], "target-token")
        self.assertNotEqual(asset["original_sha256"], asset["derived_sha256"])
        self.assertEqual(contract["mother_core_points"][0]["point_id"], "p1")
        self.assertEqual(result["提示词"], "人工正文不改")
        self.assertFalse(any(key.startswith("_") for key in result["参考图"][0]))

    def test_reuploaded_same_bytes_keep_manifest_id(self):
        task, manifest, cache, fields, _, _ = self.setup_transfer()
        task.reference_images[0]["file_token"] = "replacement-token"
        cache["replacement-token"] = cache.pop("src-token")
        result = bind_transferred_wsr_references(task, fields, {"reference_images": "参考图", "persona_contract": "人物模板合同"}, cache,
                                                {"reference_manifest": manifest})
        self.assertEqual(json.loads(result["人物模板合同"])["reference_manifest"]["manifest_id"], manifest["manifest_id"])

    def test_same_name_changed_bytes_new_identity_and_corruption_rejected(self):
        task, manifest, cache, fields, _, _ = self.setup_transfer()
        cache["src-token"]["_transfer_sha256"] = "a" * 64
        with self.assertRaisesRegex(ValueError, "BYTES_CHANGED"):
            bind_transferred_wsr_references(task, fields, {"reference_images": "参考图", "persona_contract": "人物模板合同"}, cache,
                                             {"reference_manifest": manifest})
        task.reference_images[0]["file_token"] = "new-source"
        cache["new-source"] = cache.pop("src-token")
        result = bind_transferred_wsr_references(task, fields, {"reference_images": "参考图", "persona_contract": "人物模板合同"}, cache,
                                                {"reference_manifest": manifest})
        self.assertNotEqual(json.loads(result["人物模板合同"])["reference_manifest"]["manifest_id"], manifest["manifest_id"])

    def test_legacy_uses_verified_bytes_not_filename_or_assumed_mother(self):
        task, _, cache, fields, _, _ = self.setup_transfer()
        result = bind_transferred_wsr_references(task, fields, {"reference_images": "参考图", "persona_contract": "人物模板合同"}, cache, {})
        contract = json.loads(result["人物模板合同"])
        self.assertEqual(contract["mother_core_provenance"], "final_prompt_only")
        self.assertEqual(contract["mother_core_points"], [])
        self.assertEqual(contract["reference_assets"][0]["original_sha256"], cache["src-token"]["_transfer_sha256"])
        with self.assertRaisesRegex(ValueError, "HASH_MISSING"):
            bind_transferred_wsr_references(task, fields, {"reference_images": "参考图", "persona_contract": "人物模板合同"}, {}, {})

    def test_transfer_cache_reuses_token_not_name_and_never_uploads_metadata(self):
        task, _, cache, fields, source, target = self.setup_transfer()
        self.assertEqual(transfer_reference_images(source, target, task.reference_images, cache), fields["参考图"])
        self.assertEqual(target.upload_attachment.call_count, 1)
        task.reference_images[0]["file_token"] = "same-name-new-token"
        transfer_reference_images(source, target, task.reference_images, cache)
        self.assertEqual(target.upload_attachment.call_count, 2)

    def test_revision_binding_resolves_without_a_cumulative_prompt_row(self):
        frozen = {"mother_id": "m1", "mother_version": 2,
                  "mother_core_points": [{"point_id": "mask", "requirement": "旧拍法"}],
                  "effective_checkpoints": [{"point_id": "mask", "requirement": "新拍法"}],
                  "revision_kind": "test", "parent_prompt_id": "prompt_parent"}
        cursor, connection = MagicMock(), MagicMock()
        cursor.fetchone.side_effect = [None, {"metadata_json": json.dumps({
            "frozen_handoff_context": frozen, "reference_manifest": {"manifest_id": "frozen"}})}]
        connection.cursor.return_value.__enter__.return_value = cursor
        with patch.dict(os.environ, {"WIG_REPLICATION_DATABASE_URL": "mysql://fake:fake@offline/test"}), \
             patch("workspace_support.load_repo_env"), patch("pymysql.connect", return_value=connection):
            context = load_frozen_wsr_context("wsr_revision_123")
        self.assertEqual(cursor.execute.call_count, 2)
        self.assertEqual(cursor.execute.call_args.args[1], ("revision_123", "wsr_revision_123"))
        self.assertEqual(context["effective_checkpoints"], frozen["effective_checkpoints"])
        self.assertEqual(context["mother_version"], 2)
        self.assertEqual(context["reference_manifest"]["manifest_id"], "frozen")
        connection.close.assert_called_once()

    def test_revision_checkpoints_survive_run_table_handoff_without_prompt_changes(self):
        task, manifest, cache, fields, _, _ = self.setup_transfer()
        frozen = [{"point_id": "mask", "requirement": "双掌全遮镜"}]
        effective = [{"point_id": "mask", "requirement": "单掌全遮镜"}]
        changes = [{"point_id": "mask", "operation": "replace", "replacement_requirement": "单掌全遮镜", "reason": "测试"}]
        context = {"reference_manifest": manifest, "mother_core_points": frozen,
                   "frozen_mother_core_points": frozen, "effective_checkpoints": effective,
                   "allowed_changes": changes, "revision_kind": "test"}
        result = bind_transferred_wsr_references(task, fields,
            {"reference_images": "参考图", "persona_contract": "人物模板合同"}, cache, context)
        contract = json.loads(result["人物模板合同"])
        self.assertEqual(contract["effective_checkpoints"], effective)
        self.assertEqual(contract["mother_core_points"], frozen)
        self.assertEqual(contract["allowed_changes"], changes)
        self.assertEqual(result["提示词"], fields["提示词"])


if __name__ == "__main__":
    unittest.main()
