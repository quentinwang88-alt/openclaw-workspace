from pathlib import Path
import sys
import unittest

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services.text_overlay import (
    OVERLAY_PROFILE_LIGHT_V1,
    TextOverlayError,
    apply_overlay_profile,
    validate_overlay_text,
)
from services.video_renderer import TimelineSlot, build_ass_document, build_filter_graph


class TextOverlayTest(unittest.TestCase):
    def test_recipe_profile_assigns_only_approved_slots(self):
        plan = {"shots": [{"slot_index": i} for i in range(1, 6)]}
        output = apply_overlay_profile(
            plan,
            recipe_id="RECIPE_VISUAL_TRANSFORM_V1",
            locale="th-TH",
            profile_id=OVERLAY_PROFILE_LIGHT_V1,
        )
        self.assertEqual(output["overlay_profile_id"], OVERLAY_PROFILE_LIGHT_V1)
        self.assertTrue(output["shots"][0]["overlay_text"])
        self.assertTrue(output["shots"][1]["overlay_text"].startswith("LOOK 1"))
        self.assertEqual(output["shots"][3]["overlay_text"], "")
        self.assertTrue(output["shots"][4]["overlay_text"])
        self.assertEqual(output["shots"][0]["overlay_spec"]["layout_policy"], "reserved_header_v2")

    def test_overlay_rejects_cjk_and_unknown_locale(self):
        with self.assertRaises(TextOverlayError):
            validate_overlay_text("中文标题", locale="th-TH")
        with self.assertRaises(TextOverlayError):
            apply_overlay_profile(
                {"shots": []},
                recipe_id="RECIPE_SCENE_SOLUTION_V1",
                locale="es-MX",
                profile_id=OVERLAY_PROFILE_LIGHT_V1,
            )

    def test_outfit_board_recipe_does_not_duplicate_video_overlay(self):
        plan = {"shots": [{"slot_index": i} for i in range(1, 6)]}
        output = apply_overlay_profile(
            plan,
            recipe_id="RECIPE_OUTFIT_BREAKDOWN_V1",
            locale="th-TH",
            profile_id=OVERLAY_PROFILE_LIGHT_V1,
        )
        self.assertTrue(all(shot["overlay_text"] == "" for shot in output["shots"]))

    def test_ass_document_has_safe_styles_and_slot_timing(self):
        slots = [
            TimelineSlot(
                slot_index=1,
                shot_id="s1",
                shot_version=1,
                image_path="/tmp/p1.png",
                motion_preset="slow_push",
                transition_out="cut",
                planned_ms=2000,
                effective_ms=2000,
                start_ms=0,
                overlay_text="ลุคคาเฟ่ แต่งตามง่าย",
                overlay_spec={"template_id": "HOOK_TITLE_V1"},
            ),
            TimelineSlot(
                slot_index=2,
                shot_id="s2",
                shot_version=1,
                image_path="/tmp/p2.png",
                motion_preset="slow_push",
                transition_out="cut",
                planned_ms=2000,
                effective_ms=2000,
                start_ms=2000,
            ),
        ]
        document = build_ass_document(slots)
        self.assertIn("PlayResX: 1080", document)
        self.assertIn("Style: Hook,Thonburi,66", document)
        self.assertIn("0:00:00.00,0:00:02.00,Hook", document)
        self.assertIn("ลุคคาเฟ่ แต่งตามง่าย", document)

    def test_reserved_header_keeps_title_outside_photo_and_legacy_unchanged(self):
        slot = TimelineSlot(slot_index=1, shot_id="s1", shot_version=1,
            image_path="/tmp/p1.png", motion_preset="static_hold", transition_out="cut",
            planned_ms=1600, effective_ms=1600, overlay_text="LOOK 1",
            overlay_spec={"template_id": "HOOK_TITLE_V1", "layout_policy": "reserved_header_v2"})
        graph = build_filter_graph([slot])
        self.assertIn("scale=1080:1680:force_original_aspect_ratio=decrease", graph)
        self.assertIn("pad=1080:1920:(ow-iw)/2:240", graph)
        self.assertGreater(graph.index("scale=1080:1680"), graph.index("zoompan="))
        self.assertIn("0:00:00.00,0:00:01.60,SafeHook", build_ass_document([slot]))
        slot.overlay_spec = {"template_id": "HOOK_TITLE_V1"}
        self.assertNotIn("scale=1080:1680", build_filter_graph([slot]))
        self.assertIn("0:00:00.00,0:00:01.60,Hook", build_ass_document([slot]))
        slot.overlay_text = ""
        slot.overlay_spec["layout_policy"] = "reserved_header_v2"
        self.assertNotIn("scale=1080:1680", build_filter_graph([slot]))

    def _render_states(self, base, final):
        return apply_overlay_profile(
            {"outfit_states": {"BASE": base, "FINAL": final, "ALT_1": final},
             "shots": [{"slot_index": n, "outfit_state_ref": ref} for n, ref in
                       ((1, "FINAL"), (2, "BASE"), (3, "ALT_1"), (4, "FINAL"), (5, "FINAL"))]},
            recipe_id="RECIPE_VISUAL_TRANSFORM_V1", locale="th-TH",
            profile_id=OVERLAY_PROFILE_LIGHT_V1,
        )["shots"]

    def test_copy_uses_actual_shot_state_and_no_guessed_white(self):
        shots = self._render_states(
            {"bottom": {"type": "蓝色牛仔裤"}},
            {"bottom": {"type": "格纹百褶短裙"}},
        )
        self.assertEqual(shots[1]["overlay_text"], "LOOK 1 · กางเกง")
        self.assertEqual(shots[2]["overlay_text"], "LOOK 2 · กระโปรง")
        self.assertNotIn("ขาว", " ".join(s["overlay_text"] for s in shots))

    def test_dress_detected_from_inner_and_unknown_is_not_guessed(self):
        shots = self._render_states(
            {"top_inner": "针织连衣裙", "bottom": {"type": "无独立下装"}},
            {"top_inner": "其他", "bottom": {"type": "未知"}},
        )
        self.assertEqual(shots[1]["overlay_text"], "LOOK 1 · เดรส")
        self.assertEqual(shots[2]["overlay_text"], "LOOK 2")

    def test_same_look_does_not_claim_two_looks(self):
        state = {"top_inner": "背心", "bottom": {"type": "长裤"}}
        shots = self._render_states(state, state)
        text = " ".join(s["overlay_text"] for s in shots)
        self.assertNotIn("2 ลุค", text)
        self.assertNotIn("LOOK 2", text)

    def test_scene_copy_has_no_invented_cafe_or_added_bag(self):
        output = apply_overlay_profile(
            {"outfit_states": {"FINAL": {"bag": "none"}},
             "shots": [{"slot_index": n} for n in range(1, 6)]},
            recipe_id="RECIPE_SCENE_SOLUTION_V1", locale="th-TH",
            profile_id=OVERLAY_PROFILE_LIGHT_V1,
        )
        text = " ".join(s["overlay_text"] for s in output["shots"])
        self.assertNotIn("คาเฟ่", text)
        self.assertNotIn("เพิ่มกระเป๋า", text)


if __name__ == "__main__":
    unittest.main()
