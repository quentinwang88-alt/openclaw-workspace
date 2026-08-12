from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.complete_script_v3 import build_creative_diversity_contract
from core.complete_script_v3 import _outfit_scene_affinity_contract
from core.complete_script_v3 import _stable_id as creative_stable_id
from core.original_batch_allocator import _stable_id as allocator_stable_id
from core.original_batch_executor import (
    _stable_hash as executor_stable_hash,
    _stable_id as executor_stable_id,
)
from core.outfit_template_provider import (
    _normalized_bottom_fit,
    get_outfit_template_provider_snapshot,
    load_structured_outfit_templates,
    without_outfit_display_metadata,
)
from core.outfit_selection import freeze_outfit_recipe
from core.simplified_complete_script import _stable_id as simplified_stable_id


def _direction() -> dict:
    return {
        "direction_assignment_id": "SRA_OUTFIT_PROVIDER",
        "output_slot": "S1",
        "cluster_id": 8,
        "execution_reference": {"content_carrier": "WEARER_ACTIVE"},
        "structure_execution_plan": {"macro_family_key": "HOOK>PROOF"},
    }


class OutfitTemplateProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "outfit.sqlite3"
        conn = sqlite3.connect(self.path)
        conn.executescript(
            """
            CREATE TABLE styling_templates (
                styling_id TEXT PRIMARY KEY,
                styling_name TEXT NOT NULL,
                status TEXT NOT NULL,
                applicable_product_codes TEXT NOT NULL DEFAULT '[]',
                applicable_product_type TEXT NOT NULL DEFAULT '[]',
                product_fit TEXT NOT NULL DEFAULT '[]',
                bottom_type TEXT,
                bottom_color TEXT,
                bottom_fit TEXT,
                inner_type TEXT,
                inner_color TEXT,
                inner_requirements TEXT,
                accessory_level TEXT,
                footwear_visibility TEXT,
                vibe_tag TEXT,
                config_version TEXT,
                priority INTEGER NOT NULL DEFAULT 0,
                prompt_core TEXT,
                notes TEXT,
                preferred_persona_ids TEXT NOT NULL DEFAULT '[]'
            );
            """
        )
        rows = [
            (
                "STYLE_EXACT", "TITLE_MUST_NOT_LEAK", "enabled", json.dumps(["SKU_1"]),
                json.dumps(["outerwear"]), json.dumps(["短款"]), "midi_skirt",
                json.dumps(["黑色"]), json.dumps(["高腰"]), "纯色背心", "白色",
                "不遮挡外套门襟", "minimal", "optional", json.dumps(["日常干净"]),
                "STYLE_V9", 90, "PROMPT_MUST_NOT_LEAK", "NOTE_MUST_NOT_LEAK",
                json.dumps(["TH_APPAREL_CAFE_001"]),
            ),
            (
                "STYLE_GENERIC", "GENERIC_TITLE", "enabled", json.dumps(["*"]),
                json.dumps(["outerwear"]), json.dumps(["不限"]), "straight_jeans",
                json.dumps(["蓝色牛仔"]), json.dumps(["直筒"]), "基础T恤", "白色",
                "", "none", "not_required", json.dumps(["日常干净"]),
                "STYLE_V2", 10, "GENERIC_PROMPT", "", json.dumps([]),
            ),
            (
                "STYLE_PRIVATE", "PRIVATE_TITLE", "enabled", json.dumps([]),
                json.dumps(["outerwear"]), json.dumps(["不限"]), "wide_leg_pants",
                json.dumps(["米色"]), json.dumps(["高腰"]), "", "", "", "none",
                "optional", json.dumps(["轻通勤"]), "STYLE_V1", 100, "PRIVATE_PROMPT", "",
                json.dumps([]),
            ),
        ]
        conn.executemany(
            "INSERT INTO styling_templates VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        conn.commit()
        conn.close()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_exact_and_wildcard_are_visible_but_blank_is_private(self) -> None:
        candidates = load_structured_outfit_templates(
            product_code="SKU_1", product_type="外套", db_path=self.path,
        )
        self.assertEqual(["STYLE_EXACT", "STYLE_GENERIC"], [item["template_id"] for item in candidates])
        exact = candidates[0]
        self.assertEqual("EXACT_PRODUCT_CODE", exact["match_scope"])
        serialized = json.dumps(exact, ensure_ascii=False)
        self.assertEqual("TITLE_MUST_NOT_LEAK", exact["template_display_name"])
        self.assertNotIn("PROMPT_MUST_NOT_LEAK", serialized)
        self.assertIn("白色纯色背心", exact["base_outfit_direction"])
        self.assertIn("黑色", exact["base_outfit_direction"])
        self.assertIn("styling_name", exact["ignored_unstructured_fields"])
        self.assertEqual(
            ["TH_APPAREL_CAFE_001"], exact["preferred_persona_ids"]
        )
        self.assertEqual("TARGET_GARMENT", exact["target_role"])
        self.assertEqual("白色纯色背心", exact["outfit_recipe"]["top"])
        self.assertIn("简洁半裙", exact["outfit_recipe"]["bottom"])

    def test_recipe_alternatives_are_frozen_before_prompt_generation(self) -> None:
        source = {
            "accessory_policy": "SPECIFIED",
            "accessory_items": ["小号肩包或细金属耳环"],
            "outfit_recipe": {
                "top": "合身吊带或短袖T恤",
                "bottom": "阔腿裤或牛仔短裤",
                "footwear": "凉鞋、平底鞋或运动鞋",
                "other_accessories": "小号肩包或细金属耳环",
            }
        }
        first = freeze_outfit_recipe(source, seed=41)
        second = freeze_outfit_recipe(source, seed=41)
        self.assertEqual(first["outfit_recipe"], second["outfit_recipe"])
        self.assertNotIn("或", first["outfit_recipe"]["top"])
        self.assertNotIn("或", first["outfit_recipe"]["bottom"])
        self.assertNotIn("、", first["outfit_recipe"]["footwear"])
        self.assertEqual(1, len(first["accessory_items"]))
        self.assertNotIn("或", first["accessory_items"][0])
        self.assertEqual(
            first["accessory_items"][0],
            first["outfit_recipe"]["other_accessories"],
        )
        self.assertEqual(
            ["小号肩包或细金属耳环"], first["source_accessory_items"]
        )
        self.assertEqual(source["outfit_recipe"], first["source_outfit_recipe"])

    def test_template_display_name_is_excluded_from_all_generation_hashes(self) -> None:
        first = {
            "outfit_selection_contract": {
                "template_id": "STYLE_1",
                "template_display_name": "旧名称",
                "outfit_recipe": {"top": "白色吊带"},
            }
        }
        renamed = json.loads(json.dumps(first, ensure_ascii=False))
        renamed["outfit_selection_contract"]["template_display_name"] = "新名称"
        self.assertEqual(
            without_outfit_display_metadata(first),
            without_outfit_display_metadata(renamed),
        )
        for builder in (
            creative_stable_id,
            allocator_stable_id,
            simplified_stable_id,
            executor_stable_id,
        ):
            self.assertEqual(builder("ID_", first), builder("ID_", renamed))
        self.assertEqual(
            executor_stable_hash(first), executor_stable_hash(renamed)
        )

    def test_one_piece_and_specific_accessory_are_structured(self) -> None:
        conn = sqlite3.connect(self.path)
        conn.execute(
            "INSERT INTO styling_templates VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "STYLE_DRESS", "DRESS", "enabled", json.dumps(["SKU_DRESS"]),
                json.dumps(["outerwear"]), json.dumps(["不限"]), "连衣裙",
                json.dumps([]), json.dumps([]), "", "深蓝条纹波点",
                "", "深咖色鸭舌帽带字母设计", "optional", json.dumps(["城市休闲"]),
                "STYLE_V1", 30, "", "", json.dumps([]),
            ),
        )
        conn.commit()
        conn.close()
        candidate = load_structured_outfit_templates(
            product_code="SKU_DRESS", product_type="外套", db_path=self.path,
        )[0]
        self.assertEqual("ONE_PIECE", candidate["outfit_structure"])
        self.assertEqual("深蓝条纹波点连衣裙", candidate["outfit_recipe"]["one_piece"])
        self.assertEqual("", candidate["outfit_recipe"]["top"])
        self.assertEqual("", candidate["outfit_recipe"]["bottom"])
        self.assertEqual("SPECIFIED", candidate["accessory_policy"])
        self.assertEqual(
            ["深咖色鸭舌帽带字母设计"], candidate["accessory_items"]
        )

    def test_obvious_bottom_fit_conflicts_keep_one_value_per_axis(self) -> None:
        resolved, warnings = _normalized_bottom_fit(
            "牛仔裤", ["高腰", "低腰", "阔腿", "修身"]
        )
        self.assertEqual(["高腰", "阔腿"], resolved)
        self.assertEqual(2, len(warnings))

    def test_original_allocator_keeps_exact_template_after_first_batch_use(self) -> None:
        with patch.dict(os.environ, {"ORIGINAL_SCRIPT_OUTFIT_TEMPLATE_DB_PATH": str(self.path)}):
            contract = build_creative_diversity_contract(
                product_code="SKU_1",
                country="泰国",
                category="女装",
                product_type="外套",
                direction=_direction(),
                recent_usage=[],
            )
            repeated = build_creative_diversity_contract(
                product_code="SKU_1",
                country="泰国",
                category="女装",
                product_type="外套",
                direction=_direction(),
                recent_usage=[{
                    "_batch_reserved": True,
                    "outfit_selection_contract": contract["outfit_selection_contract"],
                }],
            )
        outfit = contract["outfit_selection_contract"]
        self.assertEqual("LIGHTWEIGHT_TEMPLATE", outfit["source_type"])
        self.assertEqual("STYLE_EXACT", outfit["template_id"])
        self.assertEqual(
            "EXACT_PRODUCT_TEMPLATE", outfit["source_tier"]
        )
        self.assertEqual(
            "STYLE_EXACT",
            repeated["outfit_selection_contract"]["template_id"],
        )
        self.assertEqual("TARGET_GARMENT", outfit["target_role"])
        self.assertEqual("outfit-selection-v10-persona-affinity", outfit["contract_version"])
        self.assertFalse(outfit["hard_required"])

    def test_outfit_scene_affinity_is_advisory_and_explicit(self) -> None:
        matched = _outfit_scene_affinity_contract(
            {
                "template_id": "STYLE_CAFE",
                "template_version": "V1",
                "source_type": "LIGHTWEIGHT_TEMPLATE",
                "scene_families": ["CAFE_DINING", "STREET_OUTING"],
            },
            "CAFE_DINING",
        )
        fallback = _outfit_scene_affinity_contract(
            {
                "template_id": "STYLE_CAFE",
                "scene_families": ["CAFE_DINING"],
            },
            "HOME_ROUTINE",
        )
        self.assertEqual("MATCHED", matched["match_status"])
        self.assertGreater(matched["ranking_bonus"], 0)
        self.assertEqual("SOFT_PREFERENCE", matched["authority"])
        self.assertFalse(matched["hard_required"])
        self.assertEqual("FALLBACK", fallback["match_status"])
        self.assertEqual(0, fallback["ranking_bonus"])
        self.assertTrue(fallback["fallback_reason"])

    def test_exact_product_scene_preference_gets_a_larger_soft_bonus(self) -> None:
        generic = _outfit_scene_affinity_contract(
            {
                "template_id": "STYLE_GENERIC",
                "source_tier": "GENERIC_SHARED_TEMPLATE",
                "scene_families": ["CAFE_DINING"],
            },
            "CAFE_DINING",
        )
        exact = _outfit_scene_affinity_contract(
            {
                "template_id": "STYLE_EXACT",
                "source_tier": "EXACT_PRODUCT_TEMPLATE",
                "match_scope": "EXACT_PRODUCT_CODE",
                "scene_families": ["CAFE_DINING"],
            },
            "CAFE_DINING",
        )
        self.assertGreater(exact["ranking_bonus"], generic["ranking_bonus"])
        self.assertFalse(exact["hard_required"])

    def test_joint_ranking_prefers_outfit_compatible_scene_without_a_gate(self) -> None:
        candidates = [
            {
                "moment_family_id": "HOME",
                "persona_role": "日常分享者",
                "viewer_relationship": "像朋友分享",
                "scene_motif": "客厅窗边的单色背景区域",
                "opening_action": "商品已经穿好，人物自然进入画面",
                "action_grammar": "建立整体→观察细节→回到整体",
                "visual_tone": "自然记录",
            },
            {
                "moment_family_id": "CAFE",
                "persona_role": "日常分享者",
                "viewer_relationship": "像朋友分享",
                "scene_motif": "咖啡厅靠窗的质感座位区域",
                "opening_action": "商品已经穿好，人物自然进入画面",
                "action_grammar": "建立整体→观察细节→回到整体",
                "visual_tone": "自然记录",
            },
        ]
        outfit = {
            "source_type": "LIGHTWEIGHT_TEMPLATE",
            "source_tier": "EXACT_PRODUCT_TEMPLATE",
            "template_id": "STYLE_CAFE",
            "template_version": "V1",
            "scene_families": ["CAFE_DINING"],
            "silhouette_key": "CITY_CAFE",
            "style_family": "CITY_CAFE",
            "hair_direction": "自然发型",
            "base_outfit_direction": "完整日常穿搭",
            "hard_required": False,
        }
        with patch(
            "core.complete_script_v3._creative_combinations",
            return_value=candidates,
        ), patch(
            "core.complete_script_v3._select_outfit_contract",
            return_value=(outfit, 0, 0),
        ):
            contract = build_creative_diversity_contract(
                product_code="SKU_JOINT",
                country="泰国",
                category="",
                product_type="外套",
                direction=_direction(),
                recent_usage=[],
            )
        self.assertEqual("CAFE_DINING", contract["scene_family_key"])
        self.assertEqual(
            "MATCHED",
            contract["outfit_scene_affinity_contract"]["match_status"],
        )
        self.assertFalse(
            contract["outfit_scene_affinity_contract"]["hard_required"]
        )

    def test_missing_product_code_column_degrades_to_empty(self) -> None:
        broken = Path(self.tmp.name) / "broken.sqlite3"
        conn = sqlite3.connect(broken)
        conn.execute("CREATE TABLE styling_templates (styling_id TEXT, status TEXT)")
        conn.commit()
        conn.close()
        self.assertEqual(
            [],
            load_structured_outfit_templates(
                product_code="SKU_1", product_type="外套", db_path=broken,
            ),
        )

    def test_provider_snapshot_is_unknown_for_legacy_schema_without_sync_time(self) -> None:
        snapshot = get_outfit_template_provider_snapshot(db_path=self.path)
        self.assertEqual("UNKNOWN", snapshot["refresh_status"])
        self.assertEqual(3, snapshot["template_count"])
        self.assertEqual(3, snapshot["enabled_template_count"])
        self.assertEqual([], snapshot["soft_warnings"])

    def test_stale_snapshot_only_emits_a_soft_warning(self) -> None:
        stale = Path(self.tmp.name) / "stale.sqlite3"
        conn = sqlite3.connect(stale)
        conn.execute(
            "CREATE TABLE styling_templates (status TEXT, updated_at TEXT, last_synced_at TEXT)"
        )
        conn.execute(
            "INSERT INTO styling_templates VALUES (?,?,?)",
            ("enabled", "2000-01-01T00:00:00+08:00", "2000-01-01T00:00:00+08:00"),
        )
        conn.commit()
        conn.close()
        with patch.dict(os.environ, {"ORIGINAL_SCRIPT_OUTFIT_TEMPLATE_STALE_HOURS": "1"}):
            snapshot = get_outfit_template_provider_snapshot(db_path=stale)
        self.assertEqual("STALE", snapshot["refresh_status"])
        self.assertTrue(snapshot["soft_warnings"])

    def test_accessory_role_columns_are_consumed_when_present(self) -> None:
        accessory = Path(self.tmp.name) / "accessory.sqlite3"
        conn = sqlite3.connect(accessory)
        conn.executescript(
            """
            CREATE TABLE styling_templates (
                styling_id TEXT PRIMARY KEY,
                styling_name TEXT NOT NULL,
                status TEXT NOT NULL,
                applicable_product_codes TEXT NOT NULL DEFAULT '[]',
                applicable_product_type TEXT NOT NULL DEFAULT '[]',
                product_fit TEXT NOT NULL DEFAULT '[]',
                bottom_type TEXT,
                bottom_color TEXT,
                bottom_fit TEXT,
                inner_type TEXT,
                inner_color TEXT,
                inner_requirements TEXT,
                accessory_level TEXT,
                footwear_visibility TEXT,
                vibe_tag TEXT,
                config_version TEXT,
                priority INTEGER NOT NULL DEFAULT 0,
                target_role TEXT,
                supported_demonstration_modes TEXT,
                scene_families TEXT,
                style_intensity TEXT,
                climate_profile TEXT,
                outfit_recipe TEXT,
                base_outfit_direction TEXT,
                hair_direction TEXT,
                neckline_direction TEXT,
                outer_layer_direction TEXT,
                palette_relation TEXT,
                visibility_zones TEXT,
                visibility_requirement TEXT,
                finish_direction TEXT,
                prompt_core TEXT,
                notes TEXT
            );
            """
        )
        conn.execute(
            "INSERT INTO styling_templates VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "STYLE_SCARF", "TITLE_MUST_NOT_LEAK", "enabled", json.dumps(["*"]),
                json.dumps(["silk_scarf"]), json.dumps(["不限"]), "wide_leg_pants",
                json.dumps([]), json.dumps([]), "吊带", "", "", "minimal",
                "optional", json.dumps(["TH_WARM_CITY_FEMININE"]), "STYLE_ACC_V1", 100,
                "SUPPORTING_OUTFIT_NECK", json.dumps(["NECK_WORN", "HAIR_TIE"]),
                json.dumps(["CAFE_DINING"]), "FASHION_FORWARD", "TH_WARM",
                json.dumps({"top": "合身纯色吊带", "bottom": "高腰阔腿裤"}),
                "合身纯色吊带配高腰阔腿裤", "头发放到肩后",
                "开放领口和丝巾保持清楚边界", "无外层",
                "用中性色衬托丝巾", json.dumps(["NECK", "UPPER_BODY"]),
                "丝巾图案、边缘和领口关系清楚",
                "真实暖天气出门造型", "PROMPT_MUST_NOT_LEAK", "",
            ),
        )
        conn.commit()
        conn.close()

        candidates = load_structured_outfit_templates(
            product_code="SKU_2", product_type="丝巾", db_path=accessory,
        )
        self.assertEqual(1, len(candidates))
        candidate = candidates[0]
        self.assertEqual("SUPPORTING_OUTFIT_NECK", candidate["target_role"])
        self.assertEqual(["NECK_WORN", "HAIR_TIE"], candidate["supported_demonstration_modes"])
        self.assertEqual("FASHION_FORWARD", candidate["style_intensity"])
        self.assertEqual("合身纯色吊带", candidate["outfit_recipe"]["top"])
        self.assertEqual(["NECK", "UPPER_BODY"], candidate["visibility_zones"])
        self.assertEqual("TITLE_MUST_NOT_LEAK", candidate["template_display_name"])
        self.assertNotIn("PROMPT_MUST_NOT_LEAK", json.dumps(candidate, ensure_ascii=False))

        with patch.dict(
            os.environ,
            {"ORIGINAL_SCRIPT_OUTFIT_TEMPLATE_DB_PATH": str(accessory)},
        ):
            contract = build_creative_diversity_contract(
                product_code="SKU_2",
                country="泰国",
                category="配饰",
                product_type="丝巾",
                direction=_direction(),
                recent_usage=[],
            )
        selected = contract["outfit_selection_contract"]
        self.assertEqual("LIGHTWEIGHT_TEMPLATE", selected["source_type"])
        self.assertEqual("STYLE_SCARF", selected["template_id"])
        self.assertEqual("用中性色衬托丝巾", selected["palette_relation"])
