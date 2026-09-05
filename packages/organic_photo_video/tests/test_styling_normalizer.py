import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from services.asset_resolver import LightTryonAssetReader
from services.styling_normalizer import normalize_product_id, normalize_styling_recipe, outfit_fingerprint, outfit_visual_features


class StylingNormalizerTests(unittest.TestCase):
    def test_code_artifacts_do_not_change_real_sku_characters(self):
        self.assertEqual(normalize_product_id(" 1737141103233042426\ufffc\u200b "), "1737141103233042426")
        self.assertEqual(normalize_product_id("SKU-01_A"), "SKU-01_A")
        self.assertNotEqual(normalize_product_id("1737-1411"), "17371411")

    def test_legacy_shoe_and_bottom_fields_are_extracted_without_coat_or_brand(self):
        row = {"styling_name": "卡其色棉服", "inner_type": "圆领针织衫", "inner_color": "米白色",
               "bottom_type": "牛仔裤", "bottom_color": '["深蓝色"]', "bottom_fit": "高腰阔腿",
               "prompt_core": "外套：深棕色羽绒服，品牌ABC。\n鞋履：脚穿一双深棕色麂皮运动鞋，鞋侧白色经典三条杠标志。"}
        recipe, sources = normalize_styling_recipe(row)
        self.assertEqual(recipe["bottom"], "深蓝色 高腰阔腿 牛仔裤")
        self.assertEqual(recipe["footwear"], "深棕色 麂皮 运动鞋")
        self.assertNotIn("卡其", json.dumps(recipe, ensure_ascii=False))
        self.assertNotIn("三条杠", json.dumps(recipe, ensure_ascii=False))
        self.assertNotIn("ABC", json.dumps(recipe))
        self.assertEqual(sources["footwear"]["source"], "prompt_core_labeled_allowlist")
        self.assertEqual(sources["bottom_fit"]["columns"], ["bottom_fit"])

    def test_json_fit_array_is_human_readable(self):
        recipe, _ = normalize_styling_recipe({"bottom_type": "牛仔裤", "bottom_color": '["白色"]', "bottom_fit": '["直筒","宽松"]'})
        self.assertEqual(recipe["bottom_fit"], "直筒、宽松")
        self.assertEqual(recipe["bottom"], "白色 直筒、宽松 牛仔裤")

    def test_shoe_sock_labels_and_no_unlabeled_outer_scarf_import(self):
        recipe, _ = normalize_styling_recipe({"prompt_core": "外套与围巾：浅蓝外套与灰色围巾。\n鞋袜配饰：脚踩一双棕色毛绒厚底雪地靴/棉鞋，搭配白色的堆堆袜/腿套。"})
        self.assertEqual(recipe["footwear"], "棕色 毛绒 厚底 雪地靴")
        self.assertEqual(recipe["socks"], "白色 堆堆袜")
        self.assertNotIn("accessories", recipe)
        self.assertEqual(recipe["target_outer"], "目标商品本身")

    def test_explicit_recipe_precedes_columns_and_safe_extraction(self):
        recipe, sources = normalize_styling_recipe(
            {"bottom_type": "牛仔裤", "bottom_color": '["蓝色"]', "bottom_fit": "阔腿", "prompt_core": "鞋履：白色运动鞋"},
            {"bottom": "黑色短裙", "footwear": "黑色玛丽珍", "bag": "none"},
        )
        self.assertEqual(recipe["bottom"], "黑色短裙")
        self.assertNotIn("bottom_color", recipe)
        self.assertEqual(recipe["footwear"], "黑色玛丽珍")
        self.assertEqual(sources["bag"]["source"], "outfit_recipe")

    def test_white_denim_uses_bottom_colour_not_blue_or_inner_colour(self):
        self.assertEqual(outfit_visual_features({"top_inner": "蓝色背心", "bottom": "白色高腰阔腿牛仔裤"})["silhouette"], "WHITE_WIDE_PANTS")
        self.assertEqual(outfit_visual_features({"top_inner": "白色背心", "bottom": "深蓝直筒牛仔裤"})["silhouette"], "BLUE_STRAIGHT_PANTS")

    def test_fingerprint_ignores_template_name_but_keeps_visible_items(self):
        one = {"name": "A", "recipe": {"top_inner": "白背心", "bottom": "蓝色直筒裤", "footwear": "白鞋", "bag": "黑包"}}
        two = {"name": "B", "recipe": {**one["recipe"], "bag": "红包"}}
        self.assertEqual(outfit_fingerprint(one, include_accessories=False), outfit_fingerprint(two, include_accessories=False))
        self.assertNotEqual(outfit_fingerprint(one), outfit_fingerprint(two))
        self.assertNotEqual(outfit_fingerprint({"onepiece": "白色针织连衣裙"}), outfit_fingerprint({"onepiece": "黑色针织连衣裙"}))
        state = {"top_inner": "白背心", "bottom": {"type": "蓝色直筒裤", "color": "按选中 Look 单品描述，不额外改色"}, "shoes": "白鞋", "bag": "黑包"}
        self.assertEqual(outfit_fingerprint(one), outfit_fingerprint(state))

    def test_reader_preserves_raw_codes_and_normalization_provenance(self):
        row = dict.fromkeys(("styling_id", "styling_name", "status", "vibe_tag", "prompt_core", "outfit_recipe", "silhouette_key", "applicable_product_codes", "applicable_product_type", "product_fit", "supported_demonstration_modes", "scene_families", "style_intensity", "climate_profile", "priority", "inner_type", "inner_color", "bottom_type", "bottom_color", "bottom_fit", "accessory_level", "footwear_visibility", "base_outfit_direction", "target_role", "preferred_persona_ids", "feishu_record_id", "sync_status", "last_synced_at"), "")
        row.update(styling_id="STYLE_0017", status="enabled", applicable_product_codes='["1737141103233042426\\ufffc"]', bottom_fit="高腰阔腿", bottom_type="牛仔裤")
        reader = LightTryonAssetReader()
        reader._fetch = lambda *args: row
        result = reader.get_look("STYLE_0017")
        self.assertEqual(result["applicable_product_codes"], ["1737141103233042426"])
        self.assertEqual(result["compatibility"]["raw_product_codes"], ["1737141103233042426\ufffc"])
        self.assertEqual(result["source"]["normalization_notes"], ["product_code_formatting_artifacts_removed"])

    def test_readonly_library_discovery_respects_readable_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "assets.sqlite3"
            with sqlite3.connect(path) as conn:
                conn.execute("CREATE TABLE styling_templates(styling_id TEXT,status TEXT)")
                conn.executemany("INSERT INTO styling_templates VALUES(?,?)", [("A", "enabled"), ("B", "testing"), ("C", "disabled")])
            reader = LightTryonAssetReader(path)
            self.assertEqual(reader.list_look_ids(), ["A", "B"])
            self.assertEqual(reader.list_look_ids(["enabled"]), ["A"])
            reader.get_look = lambda ref: {"ref_id": ref}
            self.assertEqual(reader.list_looks(), [{"ref_id": "A"}, {"ref_id": "B"}])


if __name__ == "__main__":
    unittest.main()
