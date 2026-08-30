import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.product_selling_argument_adapter import (
    _accessory_operator_execution_semantics,
    _prefer_segmented_operator_sources,
    _requires_respectful_reframe,
    _scarf_execution_semantics,
    _small_accessory_claim_action_semantics,
    compatible_structure_carriers,
    load_verified_selling_point_catalog,
    normalized_carrier_requirement,
)


class ProductSellingArgumentAdapterTest(unittest.TestCase):
    def test_v2_arguments_supersede_old_generations_and_keep_latest_source(self):
        rows = [
            {
                "claim_source_id": "WHOLE",
                "source_ref": "feishu-product-claims:rec1",
                "source_created_at": "2026-01-01",
            },
            {
                "claim_source_id": "SEG1",
                "source_ref": "feishu-product-claims:rec1#segment-1",
                "source_created_at": "2026-01-02",
            },
            {
                "claim_source_id": "V2_OLD",
                "source_ref": "feishu-product-claims:rec1#argument-v2-1",
                "source_created_at": "2026-01-03",
            },
            {
                "claim_source_id": "V2_NEW",
                "source_ref": "feishu-product-claims:rec1#argument-v2-1",
                "source_created_at": "2026-01-04",
            },
            {
                "claim_source_id": "V2_NEW",
                "source_ref": "feishu-product-claims:rec1#argument-v2-1",
                "source_created_at": "2026-01-04",
                "claim_id": "C2",
            },
            {
                "claim_source_id": "V2_2",
                "source_ref": "feishu-product-claims:rec1#argument-v2-2",
                "source_created_at": "2026-01-03",
            },
        ]

        selected = _prefer_segmented_operator_sources(rows)

        self.assertEqual(
            {"V2_NEW", "V2_2"},
            {row["claim_source_id"] for row in selected},
        )
        self.assertEqual(3, len(selected))

    def test_small_accessory_claims_compile_to_visible_action_intents(self):
        adjustable = _small_accessory_claim_action_semantics(
            "戒指", "开口设计可以调节大小，粗细手指都方便"
        )
        self.assertEqual("SIZE_ADJUSTMENT", adjustable["proof_action_intent"])
        self.assertEqual("ADJUST_THEN_WEAR", adjustable["preferred_action_mode"])
        self.assertNotIn("STATIC_PRODUCT", adjustable["compatible_carriers"])

        handheld = _small_accessory_claim_action_semantics(
            "发夹", "金属材质拿在手里有一点分量"
        )
        self.assertEqual(
            "HANDHELD_MATERIAL_FEEL", handheld["proof_action_intent"]
        )
        self.assertEqual("HANDHELD_PRODUCT", handheld["preferred_action_mode"])

        scene = _small_accessory_claim_action_semantics(
            "戒指", "上班通勤和周末聚会都可以戴"
        )
        self.assertEqual("SCENE_USAGE", scene["proof_action_intent"])
        self.assertEqual("WEARER_REQUIRED", scene["visual_dependency"])

        unknown = _small_accessory_claim_action_semantics(
            "戒指", "戒面有一圈几何纹理"
        )
        self.assertEqual({}, unknown)

    def test_wrist_operator_wording_only_controls_execution_semantics(self):
        stacked = _accessory_operator_execution_semantics(
            "手镯", "2毫米细圈，单戴秀气，两个叠戴更有层次"
        )
        self.assertEqual("HAND_REQUIRED", stacked["visual_dependency"])
        self.assertEqual("RESULT_SHOW", stacked["preferred_action_mode"])
        self.assertNotIn("STATIC_PRODUCT", stacked["compatible_carriers"])
        self.assertEqual(
            {
                "status": "AUTHORIZED",
                "mode": "SAME_SKU_STACK",
                "min_display_count": 2,
                "max_display_count": 2,
                "required_display_count": 2,
                "continuity": "SAME_COUNT_THROUGHOUT_VIDEO",
                "authority": "EXPLICIT_OPERATOR_QUANTITY",
            },
            stacked["display_quantity_contract"],
        )

        ranged = _accessory_operator_execution_semantics(
            "手镯", "2毫米细圈，单戴秀气，两三个叠戴更有层次"
        )
        self.assertEqual(
            3, ranged["display_quantity_contract"]["required_display_count"]
        )

        uncounted = _accessory_operator_execution_semantics(
            "手镯", "这个细圈可以叠戴，风格更丰富"
        )
        self.assertNotIn("display_quantity_contract", uncounted)

        process = _accessory_operator_execution_semantics(
            "手镯", "62毫米圈口，一滑就进去了，佩戴很方便"
        )
        self.assertEqual("SIMPLE_WEAR_PROCESS", process["preferred_action_mode"])
        self.assertEqual(
            ["HAND_ONLY", "HANDS_ONLY", "MIXED"],
            process["compatible_carriers"],
        )

        detail = _accessory_operator_execution_semantics(
            "手镯", "表面有特殊切割，在自然光下有细小反光"
        )
        self.assertEqual({}, detail)

    def test_respectful_reframe_is_conditional_not_blanket_cooling(self):
        self.assertTrue(_requires_respectful_reframe("像农村妇女的土气穿搭"))
        self.assertTrue(_requires_respectful_reframe("和我一样虚荣心强的姐妹"))
        self.assertFalse(_requires_respectful_reframe("空调房、降温环境外搭"))
        self.assertFalse(_requires_respectful_reframe("遮肉显瘦，版型好"))

    def test_carrier_requirement_uses_structured_semantics_only(self):
        argument = {
            "operator_expression": "穿上很显瘦",
            "visual_dependency": "WEARER_REQUIRED",
        }
        self.assertEqual("WEARER_REQUIRED", normalized_carrier_requirement(argument))
        self.assertEqual(
            ["WEARER_ACTIVE", "MIXED"],
            compatible_structure_carriers(argument),
        )
        self.assertEqual(
            "FLEXIBLE",
            normalized_carrier_requirement({"operator_expression": "穿上很显瘦"}),
        )
    def test_all_confirmed_feishu_segments_remain_available_without_concept_mapping(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "voiceover.sqlite"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """CREATE TABLE product_claim_sources (
                        claim_source_id TEXT, product_id TEXT, raw_text TEXT,
                        source_type TEXT, source_ref TEXT,
                        operator_priority TEXT, created_at TEXT
                    )"""
                )
                conn.execute(
                    """CREATE TABLE product_claims (
                        product_id TEXT, verification_status TEXT, claim_id TEXT,
                        claim_source_id TEXT, concept_id TEXT, source_span TEXT,
                        canonical_claim_zh TEXT, claim_type TEXT, claim_theme TEXT,
                        evidence_requirement TEXT, allowed_strength TEXT,
                        operator_priority TEXT, updated_at TEXT, created_at TEXT,
                        normalizer_confidence REAL
                    )"""
                )
                source_rows = []
                claim_rows = []
                for index in range(1, 6):
                    source_rows.append((
                        f"S{index}", "P1", f"{index}、人工确认卖点{index}",
                        "operator_input", f"feishu-product-claims:rec#segment-{index}",
                        "core", str(index),
                    ))
                    mapped = index in {2, 3}
                    claim_rows.append((
                        "P1", "VERIFIED" if mapped else "UNRESOLVED", f"C{index}",
                        f"S{index}", "SHARED_STYLE" if mapped else None,
                        f"{index}、人工确认卖点{index}",
                        "风格表达" if mapped else f"人工确认卖点{index}",
                        "benefit" if mapped else "feature", "style",
                        "source_plus_video" if mapped else "source_only",
                        "factual" if mapped else "soft_only", "core", "", str(index),
                        0.93 if mapped else 0.0,
                    ))
                conn.executemany(
                    "INSERT INTO product_claim_sources VALUES (?, ?, ?, ?, ?, ?, ?)",
                    source_rows,
                )
                conn.executemany(
                    "INSERT INTO product_claims VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    claim_rows,
                )
            with patch.dict("os.environ", {"ORIGINAL_SCRIPT_CLAIMS_DB_PATH": str(db_path)}):
                result = load_verified_selling_point_catalog("P1")

            self.assertEqual(result["status"], "AVAILABLE")
            self.assertEqual(result["confirmed_argument_count"], 5)
            self.assertEqual(result["available_argument_count"], 5)
            self.assertEqual(result["mapped_argument_count"], 2)
            self.assertEqual(result["unmapped_argument_count"], 3)
            self.assertEqual(
                [item["source_argument_id"] for item in result["catalog"]],
                ["S1", "S2", "S3", "S4", "S5"],
            )
            self.assertEqual(
                [item["verification_status"] for item in result["catalog"]],
                ["OPERATOR_CONFIRMED"] * 5,
            )
            self.assertEqual(
                [item["mapping_status"] for item in result["catalog"]],
                ["UNMAPPED", "MAPPED", "MAPPED", "UNMAPPED", "UNMAPPED"],
            )
            self.assertEqual(result["catalog"][0]["source_claim_ids"], [])

    def test_segmented_operator_sources_replace_legacy_whole_cell_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "voiceover.sqlite"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """CREATE TABLE product_claim_sources (
                        claim_source_id TEXT, product_id TEXT, raw_text TEXT,
                        source_type TEXT, source_ref TEXT,
                        operator_priority TEXT, created_at TEXT
                    )"""
                )
                conn.execute(
                    """CREATE TABLE product_claims (
                        product_id TEXT, verification_status TEXT, claim_id TEXT,
                        claim_source_id TEXT, concept_id TEXT, source_span TEXT,
                        canonical_claim_zh TEXT, claim_type TEXT, claim_theme TEXT,
                        evidence_requirement TEXT, allowed_strength TEXT,
                        operator_priority TEXT, updated_at TEXT, created_at TEXT,
                        normalizer_confidence REAL
                    )"""
                )
                conn.executemany(
                    "INSERT INTO product_claim_sources VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            "OLD", "P1", "1、旅行办公室休闲都能穿\n2、版型遮肉",
                            "operator_input", "feishu-product-claims:rec1",
                            "core", "1",
                        ),
                        (
                            "TRAVEL", "P1", "1、旅行只带一件，办公室、通勤和休闲都能穿",
                            "operator_input", "feishu-product-claims:rec1#segment-1",
                            "core", "2",
                        ),
                        (
                            "FIT", "P1", "2、版型遮肉",
                            "operator_input", "feishu-product-claims:rec1#segment-2",
                            "core", "2",
                        ),
                    ],
                )
                conn.executemany(
                    "INSERT INTO product_claims VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        ("P1", "VERIFIED", "O1", "OLD", "CCP_MULTI_SCENE", "旅行办公室休闲", "适配多种日常场景", "scenario", "scenario", "source_plus_video", "soft_only", "core", "", "1", 0.9),
                        ("P1", "VERIFIED", "O2", "OLD", "CCP_BODY_SKIMMING", "版型遮肉", "版型对身形有视觉包容感", "visual_result", "fit", "video_positive", "soft_only", "core", "", "2", 0.9),
                        ("P1", "VERIFIED", "T1", "TRAVEL", "CCP_DAILY_SCENE", "办公室、通勤和休闲", "适合日常穿搭场景", "scenario", "scenario", "source_plus_video", "soft_only", "core", "", "3", 0.9),
                        ("P1", "VERIFIED", "T2", "TRAVEL", "CCP_MULTI_SCENE", "旅行只带一件", "适配多种日常场景", "scenario", "scenario", "source_plus_video", "soft_only", "core", "", "4", 0.9),
                        ("P1", "VERIFIED", "F1", "FIT", "CCP_BODY_SKIMMING", "版型遮肉", "版型对身形有视觉包容感", "visual_result", "fit", "video_positive", "soft_only", "core", "", "5", 0.9),
                    ],
                )
            with patch.dict("os.environ", {"ORIGINAL_SCRIPT_CLAIMS_DB_PATH": str(db_path)}):
                result = load_verified_selling_point_catalog("P1")

            source_ids = [item["source_argument_id"] for item in result["catalog"]]
            self.assertNotIn("OLD", source_ids)
            self.assertEqual(source_ids, ["TRAVEL", "TRAVEL", "FIT"])
            travel = result["catalog"][0]
            self.assertEqual(travel["operator_expression"], "适合日常穿搭场景")
            self.assertEqual(
                travel["source_operator_expression"],
                "旅行只带一件，办公室、通勤和休闲都能穿",
            )
            self.assertEqual(travel["source_scope_concept_count"], 2)

    def test_only_verified_benefits_and_results_become_arguments(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "voiceover.sqlite"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """CREATE TABLE product_claim_sources (
                        claim_source_id TEXT, source_type TEXT, source_ref TEXT
                    )"""
                )
                conn.execute(
                    """CREATE TABLE product_claims (
                        product_id TEXT, verification_status TEXT, claim_id TEXT,
                        claim_source_id TEXT, concept_id TEXT, source_span TEXT,
                        canonical_claim_zh TEXT, claim_type TEXT, claim_theme TEXT,
                        evidence_requirement TEXT, allowed_strength TEXT,
                        operator_priority TEXT, updated_at TEXT,
                        created_at TEXT
                    )"""
                )
                conn.executemany(
                    "INSERT INTO product_claim_sources VALUES (?, ?, ?)",
                    [
                        ("S1", "operator_input", "feishu:1"),
                        ("S2", "official_spec", "system:2"),
                        ("S3", "official_spec", "system:3"),
                        ("S4", "official_spec", "system:4"),
                    ],
                )
                conn.executemany(
                    "INSERT INTO product_claims VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        ("P1", "VERIFIED", "B1", "S1", "C1", "4、空调房、降温时可以穿", "适合作为降温环境的外搭", "benefit", "function", "source_plus_video", "soft_only", "normal", "", "1"),
                        ("P1", "VERIFIED", "R1", "S2", "C2", "显腿长", "腿部线条视觉更修长", "visual_result", "fit", "source_plus_video", "moderate", "core", "", "2"),
                        ("P1", "VERIFIED", "F1", "S3", "C3", "口袋", "带有口袋结构", "feature", "function", "video_positive", "factual", "normal", "", "3"),
                        ("P1", "PROPOSED", "B2", "S4", "C4", "百搭", "不需要复杂搭配", "benefit", "style", "source_plus_video", "soft_only", "normal", "", "4"),
                    ],
                )
            with patch.dict("os.environ", {"ORIGINAL_SCRIPT_CLAIMS_DB_PATH": str(db_path)}):
                result = load_verified_selling_point_catalog("P1")
            self.assertEqual(result["status"], "AVAILABLE")
            self.assertEqual([item["value_id"] for item in result["catalog"]], ["CENTRAL_B1", "CENTRAL_R1"])
            benefit, visual_result = result["catalog"]
            self.assertEqual(benefit["primary_selling_point"], "空调房、降温时可以穿")
            self.assertEqual(benefit["canonical_selling_point"], "适合作为降温环境的外搭")
            self.assertEqual(benefit["source_type"], "operator_input")
            self.assertEqual(benefit["verification_status"], "VERIFIED")
            self.assertEqual(benefit["evidence_requirement"], "source_plus_video")
            self.assertEqual(benefit["visual_dependency"], "FLEXIBLE")
            self.assertEqual(benefit["compatible_carriers"], [])
            self.assertEqual(visual_result["visual_dependency"], "WEARER_REQUIRED")
            self.assertEqual(visual_result["compatible_carriers"], ["WEARER_ACTIVE", "MIXED"])
            self.assertEqual(result["evidence_claims"][0]["claim_id"], "F1")

    def test_scarf_concepts_become_single_usage_execution_semantics(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "voiceover.sqlite"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """CREATE TABLE product_claim_sources (
                        claim_source_id TEXT, product_id TEXT, raw_text TEXT,
                        source_type TEXT, source_ref TEXT,
                        operator_priority TEXT, created_at TEXT
                    )"""
                )
                conn.execute(
                    """CREATE TABLE product_claims (
                        product_id TEXT, verification_status TEXT, claim_id TEXT,
                        claim_source_id TEXT, concept_id TEXT, source_span TEXT,
                        canonical_claim_zh TEXT, claim_type TEXT, claim_theme TEXT,
                        evidence_requirement TEXT, allowed_strength TEXT,
                        operator_priority TEXT, updated_at TEXT, created_at TEXT,
                        normalizer_confidence REAL
                    )"""
                )
                conn.execute(
                    "INSERT INTO product_claim_sources VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        "S1", "P-SCARF", "头发容易扁塌，适合局部点缀",
                        "operator_input", "feishu-product-claims:rec#segment-1",
                        "core", "1",
                    ),
                )
                conn.executemany(
                    "INSERT INTO product_claims VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        ("P-SCARF", "VERIFIED", "C1", "S1", "CCP_SCARF_HAIR_RESCUE", "头发容易扁塌", "快速完成头部造型", "benefit", "scarf_hair_use", "source_plus_video", "moderate", "core", "", "1", 0.93),
                        ("P-SCARF", "VERIFIED", "C2", "S1", "CCP_SCARF_MULTI_USE", "适合局部点缀", "多种点缀用途", "benefit", "scarf_multi_use", "source_plus_video", "factual", "core", "", "2", 0.93),
                    ],
                )
            with patch.dict("os.environ", {"ORIGINAL_SCRIPT_CLAIMS_DB_PATH": str(db_path)}):
                result = load_verified_selling_point_catalog(
                    "P-SCARF", product_type="丝巾"
                )

            argument = result["catalog"][0]
            self.assertEqual("HAIR_RESCUE", argument["argument_theme"])
            self.assertEqual("HAIR_TIE", argument["primary_demonstration_mode"])
            self.assertEqual("ONE_PRIMARY_MODE_PER_15S", argument["demonstration_policy"])
            self.assertIn("NECK_WORN", argument["supported_demonstration_modes"])
            self.assertEqual("CENTRAL_CONCEPT", argument["hook_tension_authority"])
            self.assertEqual("PAIN_REFRAME", argument["preferred_hook_ids"][0])

            with patch.dict("os.environ", {"ORIGINAL_SCRIPT_CLAIMS_DB_PATH": str(db_path)}):
                head_result = load_verified_selling_point_catalog(
                    "P-SCARF", product_type="头巾"
                )
            self.assertEqual(
                "HEAD_WORN",
                head_result["catalog"][0]["primary_demonstration_mode"],
            )

    def test_scarf_worn_usage_semantics_control_carrier_and_multi_use_scope(self):
        sun_shade = _scarf_execution_semantics(
            "头巾", ["CCP_HEADSCARF_SUN_SHADE"]
        )
        self.assertEqual("HEAD_WORN", sun_shade["primary_demonstration_mode"])
        self.assertEqual("WEARER_REQUIRED", sun_shade["visual_dependency"])
        self.assertNotIn("STATIC_PRODUCT", sun_shade["compatible_carriers"])

        summer_comfort = _scarf_execution_semantics(
            "丝巾", ["CCP_SCARF_SUMMER_COMFORT"]
        )
        self.assertEqual("NECK_WORN", summer_comfort["primary_demonstration_mode"])
        self.assertEqual("WEARER_REQUIRED", summer_comfort["visual_dependency"])

        multi_use = _scarf_execution_semantics(
            "丝巾", ["CCP_SCARF_MULTI_USE"]
        )
        self.assertEqual(
            "PRIMARY_DEMONSTRATION_MODE_ONLY",
            multi_use["voiceover_scope_policy"],
        )
        self.assertIn("颈部点缀", multi_use["voiceover_core_value"])
        self.assertNotIn("头发", multi_use["voiceover_core_value"])
        self.assertNotIn("包袋", multi_use["voiceover_core_value"])

        product_detail = _scarf_execution_semantics(
            "丝巾", ["CCP_SCARF_SURFACE_GLOSS"]
        )
        self.assertNotEqual(
            "WEARER_REQUIRED", product_detail.get("visual_dependency")
        )

    def test_operator_claim_wins_duplicate_concept(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "voiceover.sqlite"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "CREATE TABLE product_claim_sources (claim_source_id TEXT, source_type TEXT, source_ref TEXT)"
                )
                conn.execute(
                    """CREATE TABLE product_claims (
                        product_id TEXT, verification_status TEXT, claim_id TEXT,
                        claim_source_id TEXT, concept_id TEXT, source_span TEXT,
                        canonical_claim_zh TEXT, claim_type TEXT, claim_theme TEXT,
                        evidence_requirement TEXT, allowed_strength TEXT,
                        operator_priority TEXT, updated_at TEXT, created_at TEXT
                    )"""
                )
                conn.executemany(
                    "INSERT INTO product_claim_sources VALUES (?, ?, ?)",
                    [("OFF", "official_spec", "old"), ("OP", "operator_input", "feishu")],
                )
                conn.executemany(
                    "INSERT INTO product_claims VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        ("P1", "VERIFIED", "OLD", "OFF", "C1", "百搭", "便于日常搭配", "benefit", "style", "source_plus_video", "factual", "normal", "", "1"),
                        ("P1", "VERIFIED", "NEW", "OP", "C1", "1、旅行带一件，上班和休闲都能穿", "便于日常搭配", "benefit", "style", "source_plus_video", "factual", "core", "", "2"),
                    ],
                )
            with patch.dict("os.environ", {"ORIGINAL_SCRIPT_CLAIMS_DB_PATH": str(db_path)}):
                result = load_verified_selling_point_catalog("P1")
            self.assertEqual(len(result["catalog"]), 1)
            self.assertEqual(result["catalog"][0]["value_id"], "CENTRAL_NEW")
            self.assertEqual(
                result["catalog"][0]["primary_selling_point"],
                "旅行带一件，上班和休闲都能穿",
            )

    def test_multi_concept_operator_segment_splits_into_narrow_arguments(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "voiceover.sqlite"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """CREATE TABLE product_claim_sources (
                        claim_source_id TEXT, product_id TEXT, raw_text TEXT,
                        source_type TEXT, source_ref TEXT,
                        operator_priority TEXT, created_at TEXT
                    )"""
                )
                conn.execute(
                    """CREATE TABLE product_claims (
                        product_id TEXT, verification_status TEXT, claim_id TEXT,
                        claim_source_id TEXT, concept_id TEXT, source_span TEXT,
                        canonical_claim_zh TEXT, claim_type TEXT, claim_theme TEXT,
                        evidence_requirement TEXT, allowed_strength TEXT,
                        operator_priority TEXT, updated_at TEXT, created_at TEXT,
                        normalizer_confidence REAL
                    )"""
                )
                conn.execute(
                    "INSERT INTO product_claim_sources VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        "S4", "P-HEAD",
                        "4.面料有光泽度阳光下很漂亮 夏天头发出油佩戴不容易有静电 能一秒出门 拯救没洗头",
                        "operator_input", "feishu-product-claims:rec#segment-4",
                        "normal", "4",
                    ),
                )
                conn.executemany(
                    "INSERT INTO product_claims VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        ("P-HEAD", "VERIFIED", "C_HAIR", "S4", "CCP_SCARF_HAIR_RESCUE", "4.原文", "可用于整理头发状态并快速完成头部造型", "benefit", "scarf_hair_use", "source_plus_video", "soft_only", "normal", "", "1", 0.92),
                        ("P-HEAD", "VERIFIED", "C_STATIC", "S4", "CCP_SCARF_LOW_STATIC", "4.原文", "佩戴时不易产生明显静电困扰", "benefit", "scarf_comfort", "source_plus_video", "soft_only", "normal", "", "2", 0.91),
                        ("P-HEAD", "VERIFIED", "C_GLOSS", "S4", "CCP_SCARF_SURFACE_GLOSS", "4.原文", "表面在自然光下呈现可见光泽", "feature", "scarf_material", "source_plus_video", "soft_only", "normal", "", "3", 0.90),
                    ],
                )
            with patch.dict("os.environ", {"ORIGINAL_SCRIPT_CLAIMS_DB_PATH": str(db_path)}):
                result = load_verified_selling_point_catalog(
                    "P-HEAD", product_type="头巾"
                )

            self.assertEqual(
                [
                    "可用于整理头发状态并快速完成头部造型",
                    "佩戴时不易产生明显静电困扰",
                ],
                [item["operator_expression"] for item in result["catalog"]],
            )
            self.assertEqual(
                ["OPERATOR_S4_C_HAIR", "OPERATOR_S4_C_STATIC"],
                [item["value_id"] for item in result["catalog"]],
            )
            self.assertNotIn(
                "阳光下很漂亮",
                " ".join(item["operator_expression"] for item in result["catalog"]),
            )


if __name__ == "__main__":
    unittest.main()
