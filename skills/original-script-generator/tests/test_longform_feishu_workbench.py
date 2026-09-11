import tempfile
import unittest
import json
from pathlib import Path
from unittest import mock

from core.bitable import TaskRecord
from core.longform.feishu_workbench import (
    build_longform_text_batch,
    export_longform_projections,
    longform_batch_id,
)
from core.longform.production_runner import (
    ensure_unattended_keyframes,
    run_longform_job_to_final,
)
from core.longform.source_adapter import source_from_complete_script, source_from_product_plan
from core.longform.storage import LongformStorage


class LongformFeishuWorkbenchTest(unittest.TestCase):
    def test_historical_script_projection_remains_available(self):
        script = {
            "complete_script_id": "SCSCRIPT_OLD",
            "product_code": "P001",
            "semantic_context": {
                "primary_narrative_context": "旅行去降温地区",
                "core_buying_reason": "旅行带一件就够",
            },
            "video_generation_brief": {
                "product_truth": {"canonical_product_type": "outerwear"},
                "product_identity_lock": {"must_preserve": ["单排按扣"]},
                "production_design": {
                    "character": {"identity": "旅行分享者"},
                    "scene": {"location": "酒店房间行李箱旁"},
                },
                "verified_facts": [{"claim_key": "C1", "fact_text": "短款版型"}],
            },
        }
        bundle = {
            "primary_argument": {"argument_id": "A1", "text": "旅行带一件就够"},
            "supporting_arguments": [], "visual_facts": [], "content_capacity": {},
        }
        with mock.patch(
            "core.longform.source_adapter._build_argument_bundle",
            return_value=bundle,
        ):
            source = source_from_complete_script(script, duration_seconds=25)

        self.assertEqual(
            "READ_ONLY_15S_AUTHORITY_PROJECTION",
            source["source_lineage"]["source_mode"],
        )
        self.assertEqual("SCSCRIPT_OLD", source["source_lineage"]["source_script_id"])
        self.assertEqual(25, source["target_duration_seconds"])

    def test_direct_product_plan_projects_existing_planner_authority(self):
        product_context = {
            "product_code": "NEW001",
            "target_country": "泰国",
            "target_language": "泰语",
            "product_type": "outerwear",
        }
        frozen_package = {
            "requested_hook_id": "AUDIENCE_NEED_CALLOUT",
            "structure_contract": {
                "hard_constraints": {"content_carrier": "PERSON_ON_CAMERA"},
            },
            "content_bundle_brief": {
                "selling_argument": {
                    "argument_id": "ARG1",
                    "primary_selling_point": "旅行带一件就够",
                },
            },
            "semantic_spine_contract": {
                "script_thesis": {
                    "primary_narrative_context": "去降温地区旅行",
                    "core_buying_reason": "一件适配多个旅行时刻",
                },
            },
            "simplified_creative_seed": {
                "product_truth": {
                    "product_code": "NEW001",
                    "canonical_product_type": "outerwear",
                    "product_identity": "短款外套",
                    "identity_anchors": ["单排按扣", "短款版型"],
                    "visible_detail_anchors": ["圆领"],
                    "approved_claims": [
                        {"claim_key": "C1", "fact_text": "短款版型"},
                    ],
                },
                "creative_direction": {
                    "preferred_presentation": "PERSON_ON_CAMERA",
                    "capture_mode": "SELF_RECORDED",
                },
                "diversity_context": {
                    "persona_selection_contract": {
                        "persona_id": "P1",
                        "persona_name": "泰国旅行分享者",
                        "script_projection": {"identity": "准备旅行的泰国女生"},
                    },
                    "outfit_selection_contract": {
                        "template_id": "O1",
                        "template_display_name": "旅行轻装",
                        "outfit_recipe": {"top": "白色吊带", "bottom": "高腰长裤"},
                    },
                    "scene_reference": {
                        "execution_card": {
                            "space": {"location": "酒店房间行李箱旁"},
                            "lighting": "窗边自然光",
                            "visual_scene_recipe": {
                                "lived_in_detail": "半开的行李箱和随手放下的登机牌",
                            },
                        },
                    },
                },
            },
        }
        bundle = {
            "primary_argument": {"argument_id": "ARG1", "text": "旅行带一件就够"},
            "supporting_arguments": [],
            "visual_facts": [{"claim_key": "C1", "fact_text": "短款版型"}],
            "content_capacity": {},
        }
        with mock.patch(
            "core.longform.source_adapter._build_argument_bundle",
            return_value=bundle,
        ):
            source = source_from_product_plan(
                product_context,
                frozen_package,
                duration_seconds=30,
                source_plan_item_id="ITEM1",
            )

        self.assertEqual("DIRECT_PRODUCT_PLAN", source["source_lineage"]["source_mode"])
        self.assertEqual("去降温地区旅行", source["semantic_spine"]["primary_narrative_context"])
        self.assertEqual("酒店房间行李箱旁", source["production_world"]["scene"])
        self.assertEqual("P1", source["production_world"]["persona_contract"]["persona_id"])
        self.assertIn("单排按扣", source["product_identity_lock"]["must_preserve"])

    def test_batch_can_start_without_historical_short_script(self):
        class EmptyShortScriptStorage:
            def ensure_schema(self):
                return None

            def list_ready_script_results_for_product(self, *_args, **_kwargs):
                return []

        direct_source = {
            "product_code": "NEW001",
            "target_country": "泰国",
            "target_language": "泰语",
            "target_duration_seconds": 20,
            "product_identity_lock": {"must_preserve": ["短款版型"]},
            "product_truth": {"canonical_product_type": "outerwear"},
            "production_world": {
                "character": {"identity": "泰国旅行分享者"},
                "persona_contract": {"persona_id": "P1", "persona_name": "旅行分享者"},
                "outfit_contract": {"template_id": "O1", "template_display_name": "旅行轻装"},
                "scene": "酒店房间行李箱旁",
                "carrier_mode": "PERSON_ON_CAMERA",
            },
            "semantic_spine": {
                "hook_id": "AUDIENCE_NEED_CALLOUT",
                "primary_narrative_context": "去降温地区旅行",
                "core_buying_reason": "旅行带一件就够",
            },
            "longform_argument_bundle": {
                "primary_argument": {"argument_id": "A1", "text": "旅行带一件就够"},
                "supporting_arguments": [],
            },
            "source_lineage": {"source_mode": "DIRECT_PRODUCT_PLAN"},
        }
        plan = {
            "target_duration_seconds": 20,
            "segments": [{
                "segment_id": "A", "duration_seconds": 10,
                "generation_mode": "first_frame", "video_prompt": "A",
                "execution_units": [], "scene_id": "S1",
            }, {
                "segment_id": "B", "duration_seconds": 10,
                "generation_mode": "first_frame", "video_prompt": "B",
                "execution_units": [], "scene_id": "S1",
            }],
            "scene_blocks": [{"scene_id": "S1", "location": "酒店房间行李箱旁"}],
        }
        voiceover = {"target_text": "泰语口播", "chinese_translation": "中文口播"}
        with tempfile.TemporaryDirectory() as directory:
            storage = LongformStorage(Path(directory) / "longform.sqlite3")
            with mock.patch(
                "core.longform.feishu_workbench.generate_master_contract",
                side_effect=lambda source, **_kwargs: dict(source),
            ), mock.patch(
                "core.longform.feishu_workbench.compile_longform_plan",
                return_value=plan,
            ), mock.patch(
                "core.longform.feishu_workbench.freeze_reference_assets",
                return_value={"assets": []},
            ), mock.patch(
                "core.longform.feishu_workbench.build_keyframe_contracts",
                return_value={"K0": {"prompt": "首帧"}},
            ), mock.patch(
                "core.longform.feishu_workbench.run_longform_voiceover",
                return_value=voiceover,
            ):
                result = build_longform_text_batch(
                    record_id="rec-new",
                    task={
                        "product_code": "NEW001", "requested_count": 1,
                        "duration_seconds": 20, "longform_scene_mode": "auto",
                    },
                    batch_id="LFB_DIRECT",
                    source_storage=EmptyShortScriptStorage(),
                    longform_storage=storage,
                    asset_root=directory,
                    direct_sources=[{
                        "source_reference_id": "PLAN_ITEM_1",
                        "source_plan_item_id": "PLAN_ITEM_1",
                        "source_plan_batch_id": "PLAN_BATCH_1",
                        "source": direct_source,
                    }],
                )
            row = storage.get_job(result["job_ids"][0])
            master = json.loads(row["master_contract_json"])

        self.assertEqual(1, result["ready_count"])
        self.assertEqual("DIRECT_PRODUCT", master["source_lineage"]["source_selection_mode"])
        self.assertEqual("PLAN_ITEM_1", master["source_lineage"]["source_plan_item_id"])

    def test_batch_identity_includes_duration_and_replan_generation(self):
        task = {
            "task_id": "TASK1",
            "product_code": "1737141103233042426",
            "video_spec": "30秒",
            "duration_seconds": 30,
            "longform_scene_mode": "auto",
            "requested_count": 2,
            "random_seed": 7,
            "batch_id": "OLD",
        }
        first = longform_batch_id("recABC", task)
        self.assertEqual(first, longform_batch_id("recABC", task))
        self.assertNotEqual(first, longform_batch_id("recABC", task, replan=True))
        self.assertTrue(first.startswith("LFB_"))

    def test_export_creates_one_human_review_row_per_job(self):
        class FakeClient:
            def __init__(self):
                self.created = []

            def list_records(self, *, page_size):
                self.page_size = page_size
                return []

            def batch_create_records(self, records):
                self.created.extend(records)
                return ["rec-new"] * len(records)

            def update_record_fields(self, *_args):
                raise AssertionError("unexpected update")

        client = FakeClient()
        result = export_longform_projections(
            target_client=client,
            projections=[{
                "script_id": "LFSCRIPT_1",
                "product_code": "1737141103233042426",
                "batch_id": "LFB_1",
                "batch_item_id": "LFJ_1",
                "item_index": 1,
                "script_title": "30秒长视频",
                "duration_seconds": 30,
                "video_format": "长视频",
                "segment_plan": "15+15",
                "longform_job_id": "LFJ_1",
                "longform_status": "待审核",
                "complete_script": "完整脚本",
                "video_prompt": "片段A\n片段B",
            }],
        )
        self.assertEqual({"created": 1, "updated": 0, "skipped": 0}, result)
        fields = client.created[0]["fields"]
        self.assertEqual("长视频", fields["视频形态（系统）"])
        self.assertEqual("15+15", fields["分段计划（系统）"])
        self.assertEqual("不适用", fields["首帧准备状态（系统）"])
        self.assertNotIn("长视频首帧（系统）", fields)
        self.assertFalse(fields["进入生产"])

    def test_unattended_keyframes_generate_continuous_plan_without_human_pause(self):
        class FakeStorage:
            def get_job(self, _job_id):
                return {
                    "plan_json": '{"bridge_contract":{"boundaries":[{"boundary_mode":"CONTINUOUS"}]}}',
                    "keyframe_package_json": (
                        '{"K0":{"prompt":"k0"},"K1_PLANNED":{"prompt":"k1"},'
                        '"frozen_reference_assets":{"assets":[]}}'
                    ),
                    "segments": [{"segment_id": "A", "status": "PLANNED"},
                                 {"segment_id": "B", "status": "PLANNED"}],
                }

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def fake_generate(**kwargs):
                output = Path(kwargs["output_dir"]) / f"{kwargs['asset_id']}.png"
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(b"png")
                return output

            with mock.patch(
                "core.longform.production_runner._generate_image",
                side_effect=fake_generate,
            ) as generator, mock.patch(
                "core.longform.production_runner._register_initial_keyframes",
                return_value={"status": "KEYFRAMES_READY"},
            ) as register:
                result = ensure_unattended_keyframes(
                    FakeStorage(), "LFJ_TEST", asset_root=root,
                )

        self.assertEqual("KEYFRAMES_READY", result["status"])
        self.assertEqual(2, generator.call_count)
        planned_values = register.call_args.args[5]
        self.assertEqual(1, len(planned_values))
        self.assertTrue(planned_values[0].startswith("K1="))

    def test_production_preflight_fails_before_keyframe_generation(self):
        class FakeStorage:
            def ensure_schema(self):
                return None

            def get_job(self, _job_id):
                return {"segments": [{"segment_id": "A", "status": "PLANNED"}]}

        with mock.patch(
            "core.longform.production_runner.H3Gateway.preflight",
            return_value={"ready": False, "problems": ["missing h3 credential"]},
        ), mock.patch(
            "core.longform.production_runner.ensure_unattended_keyframes"
        ) as keyframes:
            with self.assertRaisesRegex(RuntimeError, "missing h3 credential"):
                run_longform_job_to_final(
                    "LFJ_PREFLIGHT",
                    storage=FakeStorage(),
                    allow_real_submit=True,
                    allow_external_tts=True,
                )

        keyframes.assert_not_called()


if __name__ == "__main__":
    unittest.main()
