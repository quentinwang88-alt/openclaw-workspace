import unittest

from core.longform.source_adapter import _compact_outfit
from core.longform.planner import _compact_world
from core.simplified_complete_script import _outfit_prompt_projection


class LongformOutfitProjectionTests(unittest.TestCase):
    def test_new_scene_does_not_inherit_old_room_background(self):
        world = _compact_world({"production_world": {"scene_contract": {
            "location": "卧室", "background": "卧室衣柜", "moment": "起床时",
        }}}, {"location": "酒店大堂", "background": "", "moment": "抵达时"})
        self.assertEqual("酒店大堂", world["same_scene"])
        self.assertNotIn("卧室", str(world))

    def test_audit_keeps_version_and_render_uses_resolved_recipe(self):
        outfit = {
            "template_id": "STYLE_X", "template_version": "V7",
            "structured_snapshot_hash": "abc", "accessory_items": ["小方包"],
            "outfit_recipe": {"top": "白色T恤", "bottom": "水洗蓝阔腿牛仔裤", "bag": "小方包"},
            "source_outfit_recipe": {"top": "白色T恤或黑色背心"},
            "neckline_direction": "领口留出外套前襟", "palette_relation": "内搭和商品保持明暗边界",
            "finish_direction": "保留牛仔水洗纹理", "inner_requirements": "内搭平整",
        }
        frozen = _compact_outfit(outfit)
        self.assertEqual("V7", frozen["template_version"])
        self.assertEqual("abc", frozen["structured_snapshot_hash"])
        self.assertEqual(["小方包"], frozen["accessory_items"])
        world = _compact_world({"production_world": {
            "outfit_contract": frozen, "outfit_prompt_projection": _outfit_prompt_projection(frozen),
        }})
        self.assertIn("水洗蓝阔腿牛仔裤", world["frozen_outfit"])
        self.assertIn("内搭平整", world["frozen_outfit"])
        self.assertIn("小方包", world["frozen_outfit"])
        self.assertIn("前襟", world["outfit_detail"])
        self.assertIn("牛仔水洗纹理", world["outfit_finish"])
        self.assertNotIn("或黑色背心", str(world))
        self.assertNotIn("structured_snapshot_hash", world)
