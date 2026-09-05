import json
from contextlib import ExitStack
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from core.longform.feishu_workbench import (
    build_longform_text_batch, load_longform_source_snapshot, longform_batch_id,
)
from core.longform.storage import LongformStorage
from core.longform.assets import freeze_reference_assets
from scripts.run_feishu_operation_tasks import _build_direct_longform_sources


def task():
    return dict(product_code="1737141103233042426", requested_count=2,
                duration_seconds=30, target_country="泰国", target_language="泰语",
                top_category="女装", product_type="外套", random_seed=9,
                longform_scene_mode="auto", product_images=[{"file_token": "WHITE"}])


def direct(index):
    return {
        "source_reference_id": f"PLAN_ITEM_{index}", "source_plan_item_id": f"PLAN_ITEM_{index}",
        "source_plan_batch_id": "PLAN_CURRENT", "product_context": {"input_hash": "WHITE_HASH"},
        "source": {
            "product_code": task()["product_code"], "target_country": "泰国", "target_language": "泰语",
            "target_duration_seconds": 30, "product_identity_lock": {"must_preserve": ["白色按扣外套"]},
            "production_world": {"outfit_contract": {"template_id": f"OUTFIT_{index}"}},
            "semantic_spine": {"hook_id": "DETAIL_SURPRISE"},
        },
    }


PLAN = {"target_duration_seconds": 30, "scene_blocks": [], "segments": [
    {"segment_id": "A", "duration_seconds": 15, "generation_mode": "first_frame", "video_prompt": "A"},
    {"segment_id": "B", "duration_seconds": 15, "generation_mode": "first_frame", "video_prompt": "B"},
]}


class CurrentTaskSourcesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.enterContext = stack.enter_context
        self.storage = LongformStorage(Path(self.tmp.name) / "lf.sqlite")
        self.model = self.enterContext(patch(
            "core.longform.feishu_workbench.generate_master_contract", side_effect=lambda source, **kw: dict(source)))
        self.enterContext(patch("core.longform.feishu_workbench.compile_longform_plan", return_value=PLAN))
        self.freeze = self.enterContext(patch("core.longform.feishu_workbench.freeze_reference_assets", return_value={"assets": [{"role": "PRODUCT_REFERENCE"}]}))
        self.enterContext(patch("core.longform.feishu_workbench.build_keyframe_contracts", return_value={"K0": {}}))
        self.voice = self.enterContext(patch("core.longform.feishu_workbench.run_longform_voiceover", return_value={
            "target_text": "ทดสอบ", "chinese_translation": "测试"}))

    def run_batch(self, sources=(), request=None):
        history = Mock()
        history.list_ready_script_results_for_product.side_effect = AssertionError("must not discover old scripts")
        return build_longform_text_batch(record_id="REC", task=request or task(), batch_id="LFB_TEST",
            source_storage=history, longform_storage=self.storage, asset_root=self.tmp.name,
            direct_sources=sources)

    def test_current_images_and_fresh_outfits_win_even_with_history(self):
        result = self.run_batch([direct(1), direct(2)])
        self.assertEqual(2, result["ready_count"])
        masters = [json.loads(self.storage.get_job(job)["master_contract_json"]) for job in result["job_ids"]]
        self.assertEqual(["OUTFIT_1", "OUTFIT_2"], [m["production_world"]["outfit_contract"]["template_id"] for m in masters])
        self.assertTrue(all(m["product_identity_lock"]["must_preserve"] == ["白色按扣外套"] for m in masters))
        self.assertTrue(all(c.kwargs["source_script_id"] == "" for c in self.freeze.call_args_list))
        self.assertEqual(2, len(load_longform_source_snapshot("LFB_TEST", task(), asset_root=self.tmp.name)))

    def test_resume_preserves_jobs_and_resources_without_new_model_calls(self):
        first = self.run_batch([direct(1), direct(2)])
        second = self.run_batch([direct(91), direct(92)])
        self.assertEqual(first["job_ids"], second["job_ids"])
        self.assertEqual(2, self.model.call_count)
        self.assertEqual(2, self.voice.call_count)

    def test_first_model_failure_keeps_source_plan_for_retry(self):
        self.model.side_effect = RuntimeError("temporary unavailable")
        first = self.run_batch([direct(1), direct(2)])
        self.assertEqual(2, first["failed_count"])
        self.model.side_effect = lambda source, **kw: dict(source)
        result = self.run_batch()
        self.assertEqual(2, result["ready_count"])
        self.assertEqual("OUTFIT_1", self.model.call_args_list[-2].args[0]["production_world"]["outfit_contract"]["template_id"])

    def test_changed_images_or_market_require_new_batch_not_resume(self):
        self.run_batch([direct(1), direct(2)])
        for changed in ({"product_images": [{"file_token": "BLUE"}]}, {"target_language": "西班牙语"}):
            new_task = {**task(), **changed}
            with self.assertRaisesRegex(RuntimeError, "LONGFORM_INPUT_CHANGED"):
                self.run_batch(request=new_task)
            self.assertNotEqual(longform_batch_id("REC", task()), longform_batch_id("REC", new_task))

    def test_insufficient_directions_do_not_clone_existing_item(self):
        result = self.run_batch([direct(1)])
        self.assertEqual(1, result["ready_count"])
        self.assertEqual(1, result["failed_count"])
        self.assertEqual(1, self.model.call_count)

    def test_new_batch_without_current_plan_does_not_fallback_to_history(self):
        with self.assertRaisesRegex(RuntimeError, "当前产品冻结计划"):
            self.run_batch()

    def test_missing_current_raw_image_stops_before_script_model(self):
        self.freeze.return_value = {"assets": [{"role": "COMPOSITE_FIRST_FRAME"}]}
        result = self.run_batch([direct(1), direct(2)])
        self.assertEqual(2, result["failed_count"])
        self.model.assert_not_called()
        self.assertIn("CURRENT_PRODUCT_REFERENCE_UNAVAILABLE", result["failures"][0]["error"])

    def test_mutated_reference_bytes_cannot_keep_old_anchor_identity(self):
        image = Path(self.tmp.name) / "product.jpg"
        image.write_bytes(b"changed-image")
        with self.assertRaisesRegex(ValueError, "REFERENCE_CONTENT_CHANGED"):
            freeze_reference_assets(job_id="LFJ_CHANGED", asset_root=self.tmp.name,
                materials=[{"role": "PRODUCT_REFERENCE", "local_path": str(image), "sha256": "old-sha"}])

    def test_missing_manifest_cannot_bypass_job_input_identity(self):
        result = self.run_batch([direct(1), direct(2)])
        path = Path(self.tmp.name) / "batches/LFB_TEST/source_plan.json"
        path.unlink()
        # New frozen jobs have enough input provenance even without the file.
        self.assertEqual(result["job_ids"], self.run_batch()["job_ids"])
        with self.assertRaisesRegex(RuntimeError, "UNVERIFIABLE"):
            self.run_batch(request={**task(), "product_images": [{"file_token": "BLUE"}]})
        with self.storage.connect() as conn:
            for job in result["job_ids"]:
                row = self.storage.get_job(job)
                master = json.loads(row["master_contract_json"])
                master["workbench_request"].pop("task_input")
                conn.execute("UPDATE longform_job SET master_contract_json=? WHERE job_id=?", (json.dumps(master), job))
        with self.assertRaisesRegex(RuntimeError, "UNVERIFIABLE"):
            self.run_batch()

    def test_direct_builder_does_not_read_old_anchor_when_task_has_images(self):
        context = {"product_code": task()["product_code"], "input_hash": "WHITE", "anchor_card": {}}
        item = Mock(batch_item_id="NEW_ITEM", frozen_direction_package_json="{}")
        batch = Mock(batch_id="NEW_PLAN")
        request = {**task(), "test_phase": "INITIAL"}
        with patch("scripts.run_feishu_operation_tasks.build_operation_product_context", return_value=context) as bootstrap, \
             patch("scripts.run_feishu_operation_tasks._load_or_bootstrap_operation_context", side_effect=AssertionError("old anchor")), \
             patch("scripts.run_feishu_operation_tasks.run_plan_only", return_value=(batch, [item], {})) as planner, \
             patch("scripts.run_feishu_operation_tasks.source_from_product_plan", return_value={"new": True}), \
             patch("scripts.run_feishu_operation_tasks._cache_longform_persona_references", return_value=[]):
            result = _build_direct_longform_sources(operation_client=Mock(), task=request, record_id="REC",
                output_dir=Path(self.tmp.name), voiceover_root="", voiceover_db_path="", replan=False,
                longform_batch_id_value="LFB_NEW")
        self.assertTrue(bootstrap.call_args.kwargs["require_current_references"])
        self.assertTrue(planner.call_args.kwargs["product_context_override"]["longform_outfit_color_matching"])
        self.assertEqual("NEW_ITEM", result[0]["source_plan_item_id"])


if __name__ == "__main__":
    unittest.main()
