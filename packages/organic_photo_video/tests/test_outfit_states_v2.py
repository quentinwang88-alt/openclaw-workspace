from pathlib import Path
import sys
import unittest

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services.outfit_planner import build_outfit_plan, build_outfit_states


def make_plan(recipe, look_id="STYLE_B"):
    return build_outfit_plan(
        outfit_plan_id="test", theme_id="THEME_TH_TRAVEL_DEPARTURE_V1",
        product_facts={"product_name": "棉服", "category": "outerwear", "facts": {"color": "brown"}},
        look={"look_id": look_id, "recipe": recipe,
              "recipe_field_sources": {"bag": {"source": "outfit_recipe"}}},
    )


class OutfitStatesV2Test(unittest.TestCase):
    def test_complete_template_overrides_rules_and_preserves_sources(self):
        recipe = {
            "top_inner": "黑白条纹针织衫", "bottom": "格纹高腰百褶短裙",
            "bottom_type": "短裙", "bottom_color": "棕色格纹", "bottom_fit": "百褶",
            "footwear": "棕色玛丽珍", "socks": "白色腿套", "bag": "黑色小方包",
            "accessories": "none", "color_palette": ["brown", "black"],
            "overall_style": "复古", "wear_mode": "敞开穿", "tucking": "前摆微塞",
            "item_reference_images": {"bottom": ["/tmp/skirt.png"]},
        }
        plan = make_plan(recipe)
        self.assertEqual(plan["bag"], "黑色小方包")
        self.assertEqual(plan["socks"], "白色腿套")
        self.assertEqual(plan["shoes"], "棕色玛丽珍")
        self.assertEqual(plan["bottom"]["type"], recipe["bottom"])
        self.assertEqual(plan["bottom"]["color"], recipe["bottom_color"])
        self.assertEqual(plan["bottom"]["fit"], "百褶")
        self.assertEqual(plan["accessories"], "none")
        self.assertEqual(plan["color_palette"], ["brown", "black"])
        self.assertEqual(plan["source_recipe"], recipe)
        self.assertEqual(plan["field_sources"]["bag"], {"source": "outfit_recipe"})
        state = build_outfit_states(plan, content_goal="outfit_breakdown")["FINAL"]
        self.assertEqual(state["source_look_id"], "STYLE_B")
        self.assertEqual(state["item_reference_images"], recipe["item_reference_images"])
        self.assertEqual(state["target_wear_mode"], "敞开穿")

    def test_only_missing_fields_fall_back(self):
        plan = make_plan({"bottom": "蓝色牛仔裤", "bag": "none"})
        self.assertEqual(plan["bag"], "none")
        self.assertNotEqual(plan["bottom"]["color"], "白色")
        self.assertTrue(plan["shoes"])
        self.assertEqual(plan["field_sources"]["shoes"], "rule.shoes")

    def test_two_library_looks_no_rule_generated_third(self):
        plan = make_plan({"top_inner": "针织衫", "bottom": "格纹短裙", "bag": "none"})
        alt = {"look_id": "STYLE_A", "recipe": {"top_inner": "条纹衫", "bottom": "蓝色牛仔裤", "footwear": "黑色乐福鞋", "bag": "棕色小包"}}
        states = build_outfit_states(plan, content_goal="visual_transform", alternate_look=alt)
        self.assertEqual(states["BASE"]["source_look_id"], "STYLE_A")
        self.assertEqual(states["BASE"]["top_inner"], "条纹衫")
        self.assertEqual(states["BASE"]["shoes"], "黑色乐福鞋")
        self.assertEqual(states["BASE"]["bag"], "棕色小包")
        self.assertEqual(states["ALT_1"], states["FINAL"])
        self.assertEqual(states["FINAL"]["bottom"]["type"], "格纹短裙")
        self.assertFalse(states["FINAL"]["same_look"])

    def test_no_alternate_soft_falls_back_without_inventing_clothes(self):
        plan = make_plan({"top_inner": "针织连衣裙", "bottom": "无独立下装"})
        states = build_outfit_states(plan, content_goal="visual_transform")
        self.assertTrue(states["FINAL"]["same_look"])
        self.assertEqual(states["BASE"], states["FINAL"])
        self.assertEqual(states["ALT_1"], states["FINAL"])

    def test_pain_point_only_changes_wearing_not_clothes_or_body(self):
        plan = make_plan({"top_inner": "条纹衫", "bottom": "灰色阔腿裤", "footwear": "棕色靴子", "bag": "none"})
        states = build_outfit_states(plan, content_goal="pain_point_solution")
        for field in ("top_inner", "bottom", "shoes", "socks", "bag", "accessories", "source_look_id", "outfit_fingerprint"):
            self.assertEqual(states["BASE"][field], states["FINAL"][field])
        self.assertEqual(states["BASE"]["change_permissions"], ["target_wear_mode", "tucking"])
        self.assertIn("保持人物真实身材", states["BASE"]["target_wear_mode"])

    def test_alternate_same_recipe_different_id_does_not_count_as_new_look(self):
        recipe = {"top_inner": "条纹衫", "bottom": "蓝色牛仔裤"}
        plan = make_plan(recipe)
        states = build_outfit_states(plan, content_goal="visual_transform", alternate_look={"ref_id": "DUPLICATE", "recipe": recipe})
        self.assertTrue(states["FINAL"]["same_look"])
        self.assertEqual(states["BASE"], states["FINAL"])

    def test_reader_ref_id_and_top_level_item_refs_are_preserved(self):
        plan = make_plan({"top_inner": "针织衫", "bottom": "格纹短裙"})
        states = build_outfit_states(plan, content_goal="visual_transform", alternate_look={
            "ref_id": "STYLE_0017", "recipe": {"top_inner": "条纹衫", "bottom": "蓝色牛仔裤", "footwear": "运动鞋", "socks": "白袜"},
            "item_refs": {"top_inner": ["/tmp/striped.png"]},
        })
        self.assertEqual(states["BASE"]["source_look_id"], "STYLE_0017")
        self.assertEqual(states["BASE"]["item_refs"], {"top_inner": ["/tmp/striped.png"]})
        self.assertEqual(states["BASE"]["socks"], "白袜")

    def test_explicit_alternatives_are_frozen_once_not_per_page(self):
        recipe = {"top_inner": "白色 修身短款背心或\n修身短袖T恤", "bottom": "牛仔裤",
                  "footwear": "棕色乐福鞋或者白色运动鞋", "accessories": ["项链", "耳环"]}
        plan = make_plan(recipe)
        self.assertEqual(plan["top_inner"], "白色 修身短款背心")
        self.assertEqual(plan["shoes"], "棕色乐福鞋")
        self.assertEqual(plan["accessories"], ["项链", "耳环"])
        self.assertEqual(plan["source_recipe"], recipe)
        self.assertEqual(plan["field_sources"]["top_inner"]["choice"]["original"], recipe["top_inner"])
        states = build_outfit_states(plan, content_goal="pain_point_solution")
        self.assertEqual(states["BASE"]["top_inner"], states["FINAL"]["top_inner"])
        self.assertNotIn("或", states["FINAL"]["shoes"])


if __name__ == "__main__":
    unittest.main()
