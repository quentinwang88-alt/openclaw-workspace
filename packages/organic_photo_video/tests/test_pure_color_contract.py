"""No model/network calls: profile compilation and render framing regression."""
import sys
import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services.content_planner import _presentation_profile, apply_presentation_shots, ContentPlannerError
from services.video_renderer import TimelineSlot, build_filter_graph
from services.image_generator import ShotGenerationRequest, compose_shot_prompt, OpenAIImageGenerator


class PureColorContractTest(unittest.TestCase):
    def test_completely_unsynced_runtime_account_is_rejected(self):
        account = SimpleNamespace(account_id="OPV_TH_TEST_001", operating_rules_json={})
        with self.assertRaisesRegex(ContentPlannerError, "runtime account contract differs"):
            _presentation_profile(account)

    def test_companion_reference_path_aliases_reach_generator(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "reference.png"
            path.touch()
            request = ShotGenerationRequest("task", 2, "full_look", 1, {}, {}, {}, {}, {}, folder)
            for field in ("local_path", "path", "cutout_image_path"):
                request.outfit_state = {"item_refs": {"top_inner": {field: str(path)}}}
                self.assertIn(str(path), OpenAIImageGenerator._reference_paths(request))

    def test_missing_required_profile_stops_before_generation(self):
        account = SimpleNamespace(account_id="UNCONFIGURED_TEST", operating_rules_json={
            "required_presentation_profile": "PURE_V2"})
        with self.assertRaises(ContentPlannerError):
            _presentation_profile(account)

    def test_full_body_slots_override_scene_grammar_and_do_not_crop(self):
        profile = {"background_color": "#F1F3F5", "full_body_occupancy": "85-92%", "item_count_policy": "dynamic_2_or_3"}
        plan = {"recipe_execution": {"content_goal": "outfit_breakdown"}, "shots": [
            {"slot_index": i, "slot_role": "detail" if i == 4 else "full_look", "duration_ms": ms,
             "shot_kind": "composite_board" if i == 1 else "generated_photo", "board_spec": {},
             "camera_hint": "俯拍咖啡馆", "purpose": "坐在床上", "composition_contract": {}}
            for i, ms in enumerate([1400, 1800, 2000, 1800, 3000], 1)]}
        apply_presentation_shots(plan, profile)
        self.assertEqual([s["duration_ms"] for s in plan["shots"]], [2400, 1800, 2000, 1800, 2000])
        self.assertEqual(plan["shots"][0]["board_spec"]["background_color"], "#F1F3F5")
        for shot in plan["shots"][1:]:
            self.assertEqual(shot["fit_mode"], "contain")
            self.assertEqual(shot["motion_preset"], "static_hold")
            self.assertNotIn("咖啡", shot["camera_hint"])
            self.assertNotIn("床", shot["purpose"])
        self.assertEqual(plan["shots"][3]["composition_contract"]["framing"], "garment_neck_to_upper_thigh")

    def test_contain_uses_group_color_and_static_zoom(self):
        slot = TimelineSlot(1, "s", 1, "/tmp/test.png", "static_hold", "cut", 2400, 2400,
                            fit_mode="contain", render_background="#F1F3F5")
        graph = build_filter_graph([slot])
        self.assertIn("color=0xF1F3F5", graph)
        self.assertIn("zoompan=z='1.0'", graph)
        self.assertNotIn("crop=", graph)

    def test_two_looks_do_not_forbid_the_requested_outfit_change(self):
        request = ShotGenerationRequest("task", 2, "full_look", 1, {"slot_index": 2}, {}, {}, {}, {}, "/tmp",
            outfit_state={"top_inner": "白色T恤", "bottom": {"type": "蓝色牛仔裤"}, "change_permissions": []},
            recipe_execution={"transform_mode": "controlled_outfit_change"})
        prompt = compose_shot_prompt(request)
        self.assertIn("按本镜搭配替换配套单品", prompt)
        self.assertNotIn("允许变化项：无", prompt)


if __name__ == "__main__":
    unittest.main()
