import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from core.longform.audio import (
    _resolve_bgm_path, choose_narration_rate, finalize_with_voiceover,
    synthesize_segment_preflight, voiceover_text_hash,
)
from core.longform.assets import freeze_reference_assets
from core.longform.contracts import (
    LongformContractError, normalize_scene_progression_contract,
    validate_master_contract,
)
from core.longform.keyframes import build_keyframe_contracts
from core.longform.h3_gateway import H3Gateway
from core.longform.model import build_master_script_prompt
from core.longform.media import merge_segments, select_bridge_candidate
from core.longform.planner import (
    _execution_unit_signature, _execution_units, compile_longform_plan,
    plan_segment_durations, split_duration,
)
from core.longform.storage import LongformStorage
from core.longform.source_adapter import _build_argument_bundle
from core.longform.voiceover import (
    _estimated_spoken_seconds, _section_duration_fit,
    build_longform_voiceover_payload, calibrate_longform_voiceover_with_edge,
)
from scripts.run_longform_original import (
    _k0_reference_paths, _register_initial_keyframes, _registered_h3_frames,
)


def fixture(duration=27):
    units = []
    unit_count = 8 if duration <= 30 else (11 if duration <= 40 else 12)
    beats = ["HOOK"] + ["PROOF"] * (unit_count - 2) + ["ENDING"]
    if unit_count >= 6:
        beats[3] = "USE_PROCESS"
        beats[-3] = "CONTEXT"
    for index, beat in enumerate(beats, 1):
        unit = {
            "unit_id": f"CU{index:02d}",
            "beat": beat,
            "visual_content": f"第{index}个可见内容",
            "information_gain": f"新增信息{index}",
            "camera": "普通手机中近景",
            "character_action": "自然继续当前分享",
            "product_anchors_visible": ["目标商品锚点"],
        }
        if index == 4:
            unit["observable_end_state"] = "人物停在自然半身姿态，商品正面可见"
            unit["end_state"] = {
                "person_state": "同一人物自然站立",
                "product_wear_state": "商品保持已穿戴完成状态",
                "pose_or_action_state": "手自然落下，面向手机",
                "camera_state": "普通手机半身平视",
                "scene_state": "同一客厅窗边",
                "outfit_state": "同一完整穿搭",
                "lighting_state": "同一白天自然光",
            }
        units.append(unit)
    return {
        "product_code": "P001",
        "target_country": "泰国",
        "target_language": "泰语",
        "target_duration_seconds": duration,
        "product_identity_lock": {"must_preserve": ["目标结构"], "must_not_change": ["数量"]},
        "product_truth": {"canonical_product_type": "outerwear"},
        "production_world": {
            "person_state": "同一泰国女性创作者",
            "product_wear_state": "商品已经穿好",
            "scene": "同一客厅窗边",
            "outfit": "同一完整穿搭",
            "lighting": "白天自然光",
            "camera": "普通手机半身平视",
            "carrier_mode": "PERSON_ON_CAMERA",
        },
        "semantic_spine": {
            "hook_id": "AUDIENCE_NEED_CALLOUT",
            "primary_narrative_context": "旅行去降温地区",
            "core_buying_reason": "一件外套覆盖旅行中的多个穿着时刻",
            "selling_argument": {"argument_id": "ARG1", "text": "旅行带一件就好"},
        },
        "verified_facts": [{"claim_key": "C1", "fact_text": "短款版型"}],
        "capture_units": units,
    }


class LongformOriginalTest(unittest.TestCase):
    def setUp(self):
        resolver = mock.patch("core.longform.voiceover.resolve_longform_voiceover_resources", return_value={
            "hook_guidance": {}, "relationship_language": {},
            "approved_style_references": [], "native_rhetoric_contract": {},
        })
        resolver.start()
        self.addCleanup(resolver.stop)

    def test_duration_split(self):
        self.assertEqual(split_duration(20), (10, 10))
        self.assertEqual(plan_segment_durations(21), [11, 10])
        self.assertEqual(plan_segment_durations(24), [12, 12])
        self.assertEqual(split_duration(25), (13, 12))
        self.assertEqual(split_duration(30), (15, 15))
        self.assertEqual(plan_segment_durations(40), [14, 13, 13])
        self.assertEqual(plan_segment_durations(45), [15, 15, 15])
        with self.assertRaises(ValueError):
            split_duration(40)
        with self.assertRaises(ValueError):
            plan_segment_durations(19)

    def test_twenty_second_contract_uses_compact_two_segment_capacity(self):
        master = validate_master_contract(fixture(20))
        plan = compile_longform_plan(master)
        self.assertEqual(
            master["longform_argument_bundle"]["content_capacity"]
            ["recommended_effective_argument_range"],
            [1, 2],
        )
        self.assertEqual(
            [item["duration_seconds"] for item in plan["segments"]], [10, 10]
        )
        self.assertEqual(len(plan["segments"]), 2)
        payload = build_longform_voiceover_payload(master, plan)
        self.assertEqual(
            [item["target_spoken_seconds_range"] for item in payload["semantic_sections"]],
            [[9.0, 9.55], [9.0, 9.55]],
        )

    def test_compile_is_one_story_with_bridge(self):
        master = validate_master_contract(fixture())
        plan = compile_longform_plan(master)
        self.assertEqual([item["duration_seconds"] for item in plan["segments"]], [14, 13])
        self.assertEqual(plan["segments"][0]["generation_mode"], "first_frame")
        self.assertEqual(plan["segments"][1]["generation_mode"], "first_frame")
        self.assertNotIn("HOOK", [item["beat"] for item in plan["segments"][1]["capture_units"]])
        self.assertIn("不得重新开场", plan["segments"][1]["video_prompt"])
        self.assertNotIn("information_gain", plan["segments"][0]["video_prompt"])
        self.assertNotIn("visual_saliency", plan["segments"][0]["video_prompt"])
        for segment in plan["segments"]:
            self.assertIn(
                "延续参考画面中的同一件商品，不重新设计、增加或删除商品可见结构",
                segment["video_prompt"],
            )

    def test_postdub_prompt_removes_speaking_cues_and_freezes_quiet_mouth(self):
        value = fixture(25)
        value["capture_units"][0]["character_action"] = (
            "正对自己的手机镜头自然交流，只向前一步并站稳"
        )
        plan = compile_longform_plan(validate_master_contract(value))
        prompt = plan["segments"][0]["video_prompt"]
        self.assertNotIn("正对自己的手机镜头自然交流", prompt)
        self.assertIn("自然看向自己的手机镜头", prompt)
        self.assertIn("声音路由为后期画外旁白", prompt)
        self.assertIn("人物始终不说话", prompt)
        self.assertIn("嘴唇自然闭合或放松", prompt)
        self.assertIn("不要让人物在画面中说话或做口型", prompt)
        self.assertNotIn("每个拍摄单元必须实际出现", plan["segments"][0]["video_prompt"])
        self.assertEqual(len(plan["segments"][0]["execution_units"]), 3)
        self.assertEqual(len(plan["segments"][1]["execution_units"]), 3)
        self.assertIn("本段唯一视觉职责", plan["segments"][1]["video_prompt"])
        self.assertNotIn("最后约0.5秒尽量让商品主体自然清楚", plan["segments"][0]["video_prompt"])
        self.assertNotIn("最后约0.5秒尽量让商品主体自然清楚", plan["segments"][1]["video_prompt"])
        self.assertFalse(
            plan["bridge_contract"]["soft_end_state_preference"]["hard_validation"]
        )
        self.assertEqual(
            plan["bridge_contract"]["soft_end_state_preference"]["failure_policy"],
            "CONTINUE_WITH_BEST_AVAILABLE_FRAME",
        )
        self.assertLess(len(plan["segments"][0]["video_prompt"]), 4000)
        self.assertEqual(plan["merge_contract"]["transition"], "HARD_CUT")

    def test_single_scene_detail_chapter_uses_setup_entry(self):
        master = validate_master_contract(fixture())
        plan = compile_longform_plan(master)
        package = build_keyframe_contracts(master, plan)
        self.assertIn("商品身份", package["K0"]["prompt"])
        self.assertIn("SB_ENTRY", package)
        self.assertEqual(package["SB_ENTRY"]["role"], "SETUP_ENTRY")
        self.assertEqual(
            package["SB_ENTRY"]["generation_strategy"],
            "NEW_SETUP_WITH_FROZEN_PERSON_PRODUCT_OUTFIT",
        )
        self.assertFalse(package["SB_ENTRY"]["human_confirmation_required"])
        self.assertIn("新手机机位和商品观察关系", package["SB_ENTRY"]["prompt"])

    def test_three_segment_plan_combines_setup_cut_and_one_continuous_bridge(self):
        master = validate_master_contract(fixture(45))
        plan = compile_longform_plan(master)
        self.assertEqual(
            [item["duration_seconds"] for item in plan["segments"]],
            [15, 15, 15],
        )
        self.assertEqual(
            [item["generation_mode"] for item in plan["segments"]],
            ["first_frame", "first_last", "first_frame"],
        )
        self.assertEqual([len(item["capture_units"]) for item in plan["segments"]], [4, 4, 4])
        self.assertEqual(len(plan["bridge_contract"]["boundaries"]), 2)
        package = build_keyframe_contracts(master, plan)
        self.assertIn("SB_ENTRY", package)
        self.assertIn("K2_PLANNED", package)
        self.assertIn("K2_ACTUAL", package)
        self.assertEqual(
            package["K2_PLANNED"]["reference_policy"]["primary"],
            "GENERATED_SB_ENTRY",
        )
        self.assertIn("上一张已生成的SB_ENTRY", package["K2_PLANNED"]["prompt"])

    def test_two_scene_plan_uses_hard_cut_and_scene_entry_without_tail_bridge(self):
        value = fixture()
        value["scene_mode"] = "multi"
        value["scene_blocks"] = [
            {"scene_id": "SCENE_HOME", "location": "旅行公寓行李箱旁", "segment_ids": ["A"],
             "narrative_role": "PREPARATION"},
            {"scene_id": "SCENE_OUT", "location": "旅店外有自然人流的街角", "segment_ids": ["B"],
             "narrative_role": "REAL_USAGE"},
        ]
        for index, unit in enumerate(value["capture_units"]):
            unit["segment_id"] = "A" if index < 4 else "B"
        master = validate_master_contract(value)
        plan = compile_longform_plan(master)
        boundary = plan["bridge_contract"]["boundaries"][0]
        self.assertEqual(boundary["boundary_mode"], "DISCONTINUOUS_CUT")
        self.assertFalse(boundary["tail_extraction_required"])
        self.assertEqual(plan["segments"][0]["generation_mode"], "first_frame")
        self.assertEqual(plan["segments"][1]["frame_contract"]["start_source"], "SCENE_ENTRY_GENERATED")
        self.assertIn("新的生活场景", plan["segments"][1]["video_prompt"])
        package = build_keyframe_contracts(master, plan)
        self.assertIn("SB_ENTRY", package)
        self.assertNotIn("K1_PLANNED", package)
        self.assertEqual(package["SB_ENTRY"]["role"], "SCENE_ENTRY")
        self.assertFalse(package["SB_ENTRY"]["human_confirmation_required"])

    def test_noncontiguous_scene_return_collapses_without_extra_switch(self):
        value = fixture(45)
        value["scene_mode"] = "multi"
        value["scene_blocks"] = [
            {"scene_id": "S1", "location": "卧室", "segment_ids": ["A", "C"]},
            {"scene_id": "S2", "location": "同卧室另一机位", "segment_ids": ["B"]},
        ]
        for index, unit in enumerate(value["capture_units"]):
            unit["segment_id"] = ("A", "B", "C")[min(index // 4, 2)]
        plan = compile_longform_plan(validate_master_contract(value))
        self.assertEqual(len(plan["scene_blocks"]), 1)
        self.assertEqual(
            plan["scene_blocks"][0]["resolution_status"],
            "NONCONTIGUOUS_SCENE_SEQUENCE_COLLAPSED_TO_SINGLE",
        )
        self.assertEqual(
            [item["boundary_mode"] for item in plan["bridge_contract"]["boundaries"]],
            ["DISCONTINUOUS_CUT", "CONTINUOUS"],
        )

    def test_auto_scene_progression_prefers_two_scenes_from_usage_value(self):
        value = fixture(30)
        value["scene_mode"] = "auto"
        value["longform_argument_bundle"] = {
            "primary_argument": {"argument_id": "A", "text": "高领挡风"},
            "supporting_arguments": [{
                "argument_id": "B", "text": "去冷的地方旅行时能穿",
                "argument_role": "USAGE_VALUE",
            }],
        }
        progression = normalize_scene_progression_contract(value)
        self.assertEqual(progression["preferred_scene_count"], 2)
        self.assertEqual(progression["segment_visual_roles"]["B"], "DETAIL_AND_REAL_USE")
        prompt = build_master_script_prompt(value)
        self.assertIn("优先实现两个有关联但可明确区分的生活场景", prompt)
        self.assertIn("商品主导中近景或近景", prompt)

    def test_auto_scene_progression_does_not_split_on_primary_context_alone(self):
        value = fixture(30)
        value["scene_mode"] = "auto"
        value["semantic_spine"]["source_semantic_spine_contract"] = {
            "product_market_context": {
                "primary_usage_world": {"text": "日常穿搭", "semantic_tags": ["DAILY"]},
                "secondary_usage_worlds": [],
            },
        }
        progression = normalize_scene_progression_contract(value)
        self.assertEqual(progression["preferred_scene_count"], 1)

    def test_catalog_bundle_selects_multiple_distinct_operator_values(self):
        catalog = {
            "status": "AVAILABLE", "snapshot_hash": "HASH", "catalog": [
                {"value_id": "V1", "source_argument_id": "S1", "primary_selling_point": "小个子比例利落",
                 "claim_type": "visual_result", "operator_priority": "core"},
                {"value_id": "V2", "source_argument_id": "S2", "primary_selling_point": "去冷的地方旅行可穿",
                 "claim_type": "scenario", "operator_priority": "core"},
                {"value_id": "V3", "source_argument_id": "S3", "primary_selling_point": "里面可以自然叠穿",
                 "claim_type": "benefit", "operator_priority": "core"},
                {"value_id": "V4", "source_argument_id": "S4", "primary_selling_point": "旅行拍照容易搭配",
                 "claim_type": "benefit", "operator_priority": "normal"},
            ],
        }
        semantic = {
            "source_argument": {"source_argument_id": "S1"},
            "product_market_context": {"source_argument_ids": ["S2", "S1", "S3", "S4"]},
        }
        with mock.patch(
            "core.longform.source_adapter.load_verified_selling_point_catalog",
            return_value=catalog,
        ):
            bundle = _build_argument_bundle("P1", "外套", 45, semantic, {}, [])
        self.assertEqual(bundle["primary_argument"]["argument_id"], "V1")
        self.assertEqual(len(bundle["supporting_arguments"]), 3)
        self.assertIn("USAGE_VALUE", [item["argument_role"] for item in bundle["supporting_arguments"]])
        self.assertEqual(
            bundle["content_capacity"]["recommended_effective_argument_range"], [3, 4]
        )

    def test_longform_keyframes_remove_unbound_reference_edits_and_outfit_override(self):
        value = fixture()
        value["production_world"].update({
            "persona_contract": {
                "persona_name": "泰国自然分享女生",
                "script_projection": {
                    "identity": "旅行穿搭者",
                    "appearance": (
                        "泰国年轻女性，把模特妆容换成淡妆，其余发型身材不变。"
                        "注意还原参考图的肤色以及皮肤自然纹理"
                    ),
                    "hair_makeup": "自然长发",
                },
            },
            "outfit_prompt_projection": {
                "frozen_outfit": "白色修身短款背心；黑色短裙",
            },
            "visual_saliency": {
                "separation": {
                    "outfit_guidance": "商品相邻的内搭使用中等或较深的纯色建立边界",
                    "background_guidance": "商品与背景保持清楚边界",
                },
            },
            "scene_contract": {"location": "机场候机区", "lighting": "自然侧光"},
        })
        master = validate_master_contract(value)
        package = build_keyframe_contracts(master, compile_longform_plan(master))
        for key in ("K0", "SB_ENTRY"):
            prompt = package[key]["prompt"]
            self.assertIn("白色修身短款背心", prompt)
            self.assertIn("冻结穿搭保持不变", prompt)
            self.assertNotIn("使用中等或较深", prompt)
            self.assertNotIn("其余发型身材不变", prompt)
            self.assertNotIn("还原参考图", prompt)

    def test_paid_segment_frames_must_be_registered_in_order(self):
        with tempfile.TemporaryDirectory() as directory:
            k0 = Path(directory) / "k0.jpg"
            k1 = Path(directory) / "k1.jpg"
            actual = Path(directory) / "actual.jpg"
            for path in (k0, k1, actual):
                path.write_bytes(path.name.encode("utf-8"))
            row = {
                "segments": [
                    {
                        "segment_id": "A", "status": "PLANNED",
                        "generation_mode": "first_last",
                        "start_frame_path": str(k0), "end_frame_path": str(k1),
                    },
                    {
                        "segment_id": "B", "status": "PLANNED",
                        "generation_mode": "first_frame",
                        "start_frame_path": str(actual), "end_frame_path": "",
                    },
                ]
            }
            with self.assertRaises(SystemExit):
                _registered_h3_frames(row, "A")
            row["segments"][0]["status"] = "KEYFRAMES_READY"
            self.assertEqual(_registered_h3_frames(row, "A"), (str(k0), str(k1)))
            with self.assertRaises(SystemExit):
                _registered_h3_frames(row, "B")
            row["segments"][1]["status"] = "BRIDGE_READY"
            self.assertEqual(_registered_h3_frames(row, "B"), (str(actual), ""))

    def test_paid_three_segment_frames_include_second_planned_bridge(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = {name: Path(directory) / f"{name}.jpg" for name in ("k0", "k1", "k2", "a1", "a2")}
            for path in paths.values():
                path.write_bytes(path.name.encode("utf-8"))
            row = {
                "segments": [
                    {"segment_id": "A", "status": "KEYFRAMES_READY", "generation_mode": "first_last",
                     "start_frame_path": str(paths["k0"]), "end_frame_path": str(paths["k1"])},
                    {"segment_id": "B", "status": "BRIDGE_READY", "generation_mode": "first_last",
                     "start_frame_path": str(paths["a1"]), "end_frame_path": str(paths["k2"])},
                    {"segment_id": "C", "status": "BRIDGE_READY", "generation_mode": "first_frame",
                     "start_frame_path": str(paths["a2"]), "end_frame_path": ""},
                ]
            }
            self.assertEqual(_registered_h3_frames(row, "B"), (str(paths["a1"]), str(paths["k2"])))
            self.assertEqual(_registered_h3_frames(row, "C"), (str(paths["a2"]), ""))

    def test_register_three_segment_planned_keyframes_once(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = LongformStorage(Path(directory) / "longform.sqlite3")
            storage.ensure_schema()
            master = validate_master_contract(fixture(45))
            plan = compile_longform_plan(master)
            storage.save_plan("J45", master, plan, build_keyframe_contracts(master, plan))
            paths = {name: Path(directory) / f"{name}.jpg" for name in ("k0", "k1", "k2", "entry")}
            for path in paths.values():
                path.write_bytes(path.name.encode("utf-8"))
            row = storage.get_job("J45")
            result = _register_initial_keyframes(
                storage, row, plan, "J45", str(paths["k0"]),
                [f"K2={paths['k2']}"], str(paths["k1"]),
                [f"B={paths['entry']}"],
            )
            self.assertEqual(result["status"], "KEYFRAMES_READY")
            registered = storage.get_job("J45")["segments"]
            self.assertEqual(registered[0]["status"], "KEYFRAMES_READY")
            self.assertEqual(registered[1]["status"], "BRIDGE_READY")
            self.assertEqual(registered[1]["end_frame_path"], str(paths["k2"].resolve()))

    def test_register_multiscene_uses_scene_entry_and_requires_no_planned_bridge(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = LongformStorage(Path(directory) / "longform.sqlite3")
            storage.ensure_schema()
            value = fixture()
            value["scene_mode"] = "multi"
            value["scene_blocks"] = [
                {"scene_id": "S1", "location": "室内", "segment_ids": ["A"]},
                {"scene_id": "S2", "location": "街边", "segment_ids": ["B"]},
            ]
            master = validate_master_contract(value)
            plan = compile_longform_plan(master)
            frames = build_keyframe_contracts(master, plan)
            storage.save_plan("JM", master, plan, frames)
            k0 = Path(directory) / "k0.jpg"
            entry = Path(directory) / "entry.jpg"
            k0.write_bytes(b"k0")
            entry.write_bytes(b"entry")
            row = storage.get_job("JM")
            _register_initial_keyframes(
                storage, row, plan, "JM", str(k0), [], "", [f"B={entry}"],
            )
            registered = storage.get_job("JM")["segments"]
            self.assertEqual(registered[0]["status"], "KEYFRAMES_READY")
            self.assertEqual(registered[0]["end_frame_path"], "")
            self.assertEqual(registered[1]["status"], "BRIDGE_READY")
            self.assertEqual(registered[1]["start_frame_path"], str(entry.resolve()))

    def test_voiceover_is_whole_video_not_shot_locked(self):
        master = validate_master_contract(fixture())
        plan = compile_longform_plan(master)
        payload = build_longform_voiceover_payload(master, plan)
        self.assertTrue(payload["speech_policy"]["one_continuous_thought"])
        self.assertTrue(payload["speech_policy"]["no_sentence_to_shot_lock"])
        self.assertEqual(len(payload["semantic_sections"]), 2)
        self.assertTrue(payload["speech_policy"]["no_rehook_after_first_segment"])
        self.assertEqual(payload["speech_policy"]["tts_layout"], "bounded-semantic-continuation-v1")
        self.assertIn("longform_argument_bundle", payload)
        self.assertEqual(
            payload["semantic_sections"][0]["target_spoken_seconds_range"], [12.6, 13.4]
        )

    def test_segment_duration_fit_detects_total_fit_but_local_overflow(self):
        master = validate_master_contract(fixture())
        plan = compile_longform_plan(master)
        payload = build_longform_voiceover_payload(master, plan)
        report = _section_duration_fit({
            "semantic_sections": [
                {"segment_id": "A", "target_text": "ก" * 170},
                {"segment_id": "B", "target_text": "ก" * 170},
            ],
        }, payload)
        self.assertEqual(report[0]["status"], "FIT")
        self.assertEqual(report[1]["status"], "OUT_OF_RANGE")

    def test_segment_duration_fit_accepts_small_local_drift(self):
        payload = {
            "target_language": "泰语",
            "semantic_sections": [{
                "segment_id": "A",
                "duration_seconds": 13,
                "target_spoken_seconds_range": [10.7, 11.7],
            }],
        }
        report = _section_duration_fit({
            "semantic_sections": [{"segment_id": "A", "target_text": "ก" * 128}],
        }, payload)
        self.assertEqual(report[0]["status"], "SOFT_FIT")
        self.assertEqual(report[0]["soft_acceptable_seconds_range"], [9.8, 12.7])

    def test_voiceover_duration_estimate_uses_target_locale_rate(self):
        self.assertEqual(_estimated_spoken_seconds("ก" * 130, "泰语"), 10.0)

    def test_edge_preflight_measures_and_freezes_each_segment_audio(self):
        with tempfile.TemporaryDirectory() as directory:
            def fake_synthesize(_text, output_path, **_kwargs):
                Path(output_path).write_bytes(b"audio")

            with mock.patch(
                "core.longform.audio._synthesize_edge", side_effect=fake_synthesize,
            ), mock.patch(
                "core.longform.audio.audio_duration_seconds", side_effect=[12.2, 10.9],
            ):
                report = synthesize_segment_preflight(
                    [
                        {"segment_id": "A", "target_text": "หนึ่ง"},
                        {"segment_id": "B", "target_text": "สอง"},
                    ],
                    [
                        {"segment_id": "A", "duration_seconds": 13},
                        {"segment_id": "B", "duration_seconds": 12},
                    ],
                    directory,
                )
            self.assertTrue(report["all_target_fit"])
            self.assertTrue(all(Path(item["audio_path"]).is_file() for item in report["sections"]))

    def test_actual_edge_duration_allows_only_one_targeted_revision(self):
        master = validate_master_contract(fixture(25))
        plan = compile_longform_plan(master)
        original = {
            "target_text": "ฉบับเดิม", "chinese_translation": "原稿",
            "estimated_seconds": 20,
            "semantic_sections": [
                {"segment_id": "A", "target_text": "สั้นหนึ่ง"},
                {"segment_id": "B", "target_text": "สั้นสอง"},
            ],
        }
        short = {
            "schema_version": "longform-edge-tts-preflight-v1",
            "voice_id": "th-TH-PremwadeeNeural",
            "sections": [
                {"segment_id": "A", "coverage_ratio": .60, "status": "TOO_SHORT"},
                {"segment_id": "B", "coverage_ratio": .62, "status": "TOO_SHORT"},
            ],
            "actual_tts_seconds_total": 15, "all_acceptable": False,
        }
        fit = {
            "schema_version": "longform-edge-tts-preflight-v1",
            "voice_id": "th-TH-PremwadeeNeural",
            "sections": [
                {"segment_id": "A", "coverage_ratio": .92, "status": "TARGET_FIT"},
                {"segment_id": "B", "coverage_ratio": .93, "status": "TARGET_FIT"},
            ],
            "actual_tts_seconds_total": 23.1, "all_acceptable": True,
        }
        revised = {
            "target_text": "ฉบับใหม่", "chinese_translation": "新稿",
            "semantic_sections": [
                {"segment_id": "A", "target_text": "ยาวขึ้นหนึ่ง"},
                {"segment_id": "B", "target_text": "ยาวขึ้นสอง"},
            ],
        }
        with mock.patch(
            "core.longform.voiceover.synthesize_segment_preflight",
            side_effect=[short, fit],
        ), mock.patch(
            "core.longform.voiceover._invoke_voiceover_model", return_value=revised,
        ) as invoke:
            result = calibrate_longform_voiceover_with_edge(
                master, plan, original, "/tmp/unused-longform-test",
            )
        self.assertEqual(invoke.call_count, 1)
        self.assertEqual(result["target_text"], "ฉบับใหม่")
        self.assertTrue(result["tts_preflight"]["revision_selected"])
        self.assertEqual(result["duration_fit"]["method"], "EDGE_TTS_EFFECTIVE_SPEECH_V2")

    def test_plan_time_reference_freeze_is_local_and_hashed(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "product.png"
            source.write_bytes(b"product-reference")
            missing_db = Path(directory) / "missing.sqlite3"
            with mock.patch("core.longform.assets.default_db_path", return_value=missing_db):
                manifest = freeze_reference_assets(
                    job_id="JOB1", asset_root=directory,
                    materials=({"product_reference_assets": [{"local_path": str(source)}]},),
                )
            self.assertEqual(manifest["status"], "AVAILABLE")
            self.assertEqual(manifest["assets"][0]["role"], "PRODUCT_REFERENCE")
            self.assertTrue(Path(manifest["assets"][0]["local_path"]).is_file())
            self.assertEqual(len(manifest["assets"][0]["sha256"]), 64)
            self.assertEqual(manifest["reference_mode"], "RAW_PRODUCT")
            self.assertEqual(
                manifest["assets"][0]["authority"], "PRODUCT_APPEARANCE_AUTHORITY"
            )

    def test_reference_freeze_records_stable_persona_id_and_reference_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            product = root / "product.png"
            persona = root / "persona.png"
            product.write_bytes(b"product")
            persona.write_bytes(b"persona")
            missing_db = root / "missing.sqlite3"
            with mock.patch("core.longform.assets.default_db_path", return_value=missing_db):
                manifest = freeze_reference_assets(
                    job_id="JOB_PERSONA_LOCK",
                    asset_root=root,
                    materials=({
                        "product_reference_assets": [{"local_path": str(product)}],
                        "persona_reference_assets": [{"local_path": str(persona)}],
                    },),
                    persona_lock={
                        "persona_id": "TH_APPAREL_SELECTED_01_001",
                        "structured_snapshot_hash": "s" * 64,
                    },
                )
            lock = manifest["persona_lock"]
            self.assertEqual(lock["status"], "FROZEN")
            self.assertEqual(lock["persona_id"], "TH_APPAREL_SELECTED_01_001")
            self.assertEqual(lock["structured_snapshot_hash"], "s" * 64)
            self.assertEqual(len(lock["reference_asset_sha256s"]), 1)

    def test_k0_prefers_raw_product_and_uses_composite_only_as_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            product = root / "product.jpg"
            persona = root / "persona.jpg"
            composite = root / "composite.jpg"
            for path, body in ((product, b"p"), (persona, b"u"), (composite, b"c")):
                path.write_bytes(body)
            keyframes = {"frozen_reference_assets": {"assets": [
                {"role": "COMPOSITE_FIRST_FRAME", "local_path": str(composite)},
                {"role": "PERSONA_REFERENCE", "local_path": str(persona)},
                {"role": "PRODUCT_REFERENCE", "local_path": str(product)},
            ]}}
            self.assertEqual(
                _k0_reference_paths(keyframes),
                [str(product.resolve()), str(persona.resolve())],
            )
            keyframes["frozen_reference_assets"]["assets"] = [
                {"role": "COMPOSITE_FIRST_FRAME", "local_path": str(composite)},
                {"role": "PERSONA_REFERENCE", "local_path": str(persona)},
            ]
            self.assertEqual(
                _k0_reference_paths(keyframes),
                [str(composite.resolve()), str(persona.resolve())],
            )

    def test_reference_scanner_does_not_treat_product_code_as_image_authority(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            generic = root / "generic.jpg"
            product = root / "product.jpg"
            generic.write_bytes(b"generic")
            product.write_bytes(b"product")
            missing_db = root / "missing.sqlite3"
            with mock.patch("core.longform.assets.default_db_path", return_value=missing_db):
                manifest = freeze_reference_assets(
                    job_id="JOB_ROLE", asset_root=root,
                    materials=({
                        "product_code": "P001",
                        "misc": {"local_path": str(generic)},
                        "product_reference_assets": [{"local_path": str(product)}],
                    },),
                )
            roles = {Path(item["local_path"]).read_bytes(): item["role"] for item in manifest["assets"]}
            self.assertEqual(roles[b"product"], "PRODUCT_REFERENCE")
            self.assertEqual(roles[b"generic"], "SUPPORTING_REFERENCE")

    def test_cross_segment_action_signature_is_only_a_soft_selection_penalty(self):
        units = [
            {"unit_id": "U1", "visual_content": "全身站稳", "camera": "全身中远景"},
            {"unit_id": "U2", "visual_content": "指尖轻扶立领", "camera": "半身中景"},
            {"unit_id": "U3", "visual_content": "人物自然走两步", "camera": "全身中远景"},
            {"unit_id": "U4", "visual_content": "背面停住", "camera": "半身中景"},
        ]
        used = {_execution_unit_signature(units[1])}
        selected = _execution_units(
            units, segment_role="CONTEXT_AND_WEAR_RESULT", detail_focus=[],
            incoming_boundary_mode="DISCONTINUOUS_CUT", used_signatures=used,
        )
        self.assertEqual([item["unit_id"] for item in selected], ["U1", "U3", "U4"])

    def test_h3_segment_modes_and_frames(self):
        master = validate_master_contract(fixture())
        plan = compile_longform_plan(master)
        with tempfile.TemporaryDirectory() as directory:
            k0 = Path(directory) / "k0.jpg"
            k1 = Path(directory) / "k1.jpg"
            k0.write_bytes(b"k0")
            k1.write_bytes(b"k1")
            request_a = H3Gateway.build_segment_request(
                plan["segments"][0], start_frame=str(k0), end_frame=str(k1)
            )
            request_b = H3Gateway.build_segment_request(
                plan["segments"][1], start_frame=str(k1)
            )
            self.assertEqual(request_a["mode"], "first_frame")
            self.assertEqual(len(request_a["imagePaths"]), 1)
            self.assertEqual(request_b["mode"], "first_frame")
            self.assertEqual(len(request_b["imagePaths"]), 1)
            self.assertEqual(request_a["duration"], 14)
            self.assertEqual(request_b["duration"], 13)

    def test_h3_duration_payloads_for_all_supported_buckets(self):
        with tempfile.TemporaryDirectory() as directory:
            start = Path(directory) / "start.jpg"
            end = Path(directory) / "end.jpg"
            start.write_bytes(b"start")
            end.write_bytes(b"end")
            for duration, expected in ((20, [10, 10]), (25, [13, 12]), (30, [15, 15]),
                                       (40, [14, 13, 13]), (45, [15, 15, 15])):
                plan = compile_longform_plan(validate_master_contract(fixture(duration)))
                actual = []
                for segment in plan["segments"]:
                    request = H3Gateway.build_segment_request(
                        segment, start_frame=str(start),
                        end_frame=str(end) if segment["generation_mode"] == "first_last" else "",
                    )
                    actual.append(request["duration"])
                self.assertEqual(actual, expected)

    def test_only_one_hook(self):
        value = fixture()
        value["capture_units"][4]["beat"] = "HOOK"
        with self.assertRaises(LongformContractError):
            validate_master_contract(value)

    def test_isolated_storage(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = LongformStorage(Path(directory) / "longform.sqlite3")
            storage.ensure_schema()
            master = validate_master_contract(fixture())
            plan = compile_longform_plan(master)
            frames = build_keyframe_contracts(master, plan)
            storage.save_plan("J1", master, plan, frames)
            row = storage.get_job("J1")
            self.assertEqual(row["status"], "PLANNED")
            self.assertEqual(len(row["segments"]), 2)
            self.assertEqual(row["execution_report_json"], "{}")

            master_45 = validate_master_contract(fixture(45))
            plan_45 = compile_longform_plan(master_45)
            storage.save_plan("J45", master_45, plan_45, build_keyframe_contracts(master_45, plan_45))
            self.assertEqual(len(storage.get_job("J45")["segments"]), 3)

    def test_narration_rate_uses_small_fixed_presets(self):
        self.assertEqual(choose_narration_rate(21.5, 27.9), 0)
        self.assertEqual(choose_narration_rate(25.0, 27.9), 0)
        self.assertEqual(choose_narration_rate(28.0, 27.9), 8)

    def test_longform_does_not_burn_default_bgm_when_platform_owns_music(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(_resolve_bgm_path())

    def test_finalization_requires_explicit_external_tts_authority(self):
        with self.assertRaises(ValueError):
            finalize_with_voiceover(
                "/missing/video.mp4", {"target_text": "ทดสอบ"}, "/tmp/missing.mp4",
                allow_external_tts=False,
            )

    def test_segmented_finalization_reuses_preflight_audio_and_applies_light_bgm(self):
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            self.skipTest("ffmpeg not installed")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            video = root / "video.mp4"
            bgm = root / "bgm.m4a"
            subprocess.run([
                ffmpeg, "-y", "-loglevel", "error", "-f", "lavfi", "-i",
                "color=c=black:s=108x192:d=8:r=30", "-an", str(video),
            ], check=True)
            subprocess.run([
                ffmpeg, "-y", "-loglevel", "error", "-f", "lavfi", "-i",
                "sine=frequency=220:duration=8", "-c:a", "aac", str(bgm),
            ], check=True)
            sections = []
            reports = []
            for segment_id, frequency in (("A", 440), ("B", 520)):
                target_text = f"text-{segment_id}"
                audio = root / f"preflight-{segment_id}.mp3"
                subprocess.run([
                    ffmpeg, "-y", "-loglevel", "error", "-f", "lavfi", "-i",
                    f"sine=frequency={frequency}:duration=3.7", "-q:a", "5", str(audio),
                ], check=True)
                sections.append({"segment_id": segment_id, "target_text": target_text})
                reports.append({
                    "segment_id": segment_id,
                    "text_sha256": voiceover_text_hash(target_text),
                    "audio_path": str(audio),
                })
            output = root / "final.mp4"
            result = finalize_with_voiceover(
                video,
                {
                    "target_text": "text-A text-B",
                    "semantic_sections": sections,
                    "tts_preflight": {
                        "voice_id": "th-TH-PremwadeeNeural",
                        "sections": reports,
                    },
                },
                output,
                allow_external_tts=True,
                segment_plan=[
                    {"segment_id": "A", "duration_seconds": 4},
                    {"segment_id": "B", "duration_seconds": 4},
                ],
                bgm_path=bgm,
            )
            self.assertTrue(output.is_file())
            self.assertEqual(result["bgm_policy"], "LIGHT_BED_APPLIED")
            self.assertTrue(all(item["preflight_audio_reused"] for item in result["sections"]))
            self.assertTrue(all(item["selected_rate_percent"] == 0 for item in result["sections"]))
            self.assertTrue(all(item["opening_delay_ms"] < 350 for item in result["sections"]))

    def test_voiceover_payload_uses_neutral_closure_language_when_unverified(self):
        master = validate_master_contract(fixture())
        plan = compile_longform_plan(master)
        payload = build_longform_voiceover_payload(master, plan)
        self.assertEqual(
            payload["closure_language_contract"]["mode"],
            "NEUTRAL_CLOSURE_LANGUAGE",
        )
        master["product_identity_lock"]["visible_closure_contract"] = {
            "status": "AVAILABLE", "visible_description": "正面采用拉链"
        }
        payload = build_longform_voiceover_payload(master, plan)
        self.assertEqual(payload["closure_language_contract"]["mode"], "ZIPPER_VERIFIED")

    def test_bridge_selector_returns_registered_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            try:
                from PIL import Image
            except ImportError:
                self.skipTest("Pillow not installed")
            paths = []
            for index, value in enumerate((90, 100, 100, 220), start=1):
                path = Path(directory) / f"bridge_{index:02d}.jpg"
                Image.new("L", (180, 320), value).save(path)
                paths.append(path)
            selected = select_bridge_candidate(paths)
            self.assertIn(Path(selected["selected"]), paths)
            self.assertEqual(
                selected["failure_policy"], "CONTINUE_WITH_BEST_AVAILABLE_FRAME"
            )
            self.assertEqual(
                selected["product_readability"], "NOT_SEMANTICALLY_EVALUATED"
            )
            self.assertIn(
                selected["method"],
                {"CLARITY_LOW_MOTION_V1", "DETERMINISTIC_SECOND_FRAME"},
            )

    def test_merge_accepts_three_registered_segments(self):
        ffmpeg = shutil.which("ffmpeg")
        ffprobe = shutil.which("ffprobe")
        if not ffmpeg or not ffprobe:
            self.skipTest("ffmpeg/ffprobe not installed")
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for index, color in enumerate(("red", "green", "blue"), 1):
                path = Path(directory) / f"segment_{index}.mp4"
                subprocess.run(
                    [ffmpeg, "-y", "-loglevel", "error", "-f", "lavfi", "-i",
                     f"color=c={color}:s=108x192:d=0.2:r=30", "-an", str(path)],
                    check=True,
                )
                paths.append(path)
            output = Path(directory) / "merged.mp4"
            merge_segments(paths, output, width=108, height=192)
            self.assertTrue(output.is_file())
            duration = subprocess.run(
                [ffprobe, "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=nw=1:nk=1", str(output)],
                capture_output=True, text=True, check=True,
            )
            self.assertGreater(float(duration.stdout.strip()), 0.4)

    def test_h3_preflight_accepts_standard_environment_without_exposing_value(self):
        gateway = H3Gateway()
        with mock.patch.dict(
            os.environ, {"METASO_MINIMAX_API_KEY": "test-secret"}, clear=False,
        ):
            result = gateway.preflight(require_api_key=True)
        self.assertTrue(result["credential_present"])
        self.assertNotIn("test-secret", json.dumps(result))


if __name__ == "__main__":
    unittest.main()
