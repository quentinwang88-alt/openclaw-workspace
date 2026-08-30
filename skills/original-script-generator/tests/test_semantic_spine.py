import unittest

from core.semantic_spine import (
    build_context_bridge,
    build_product_market_context,
    build_script_semantic_spine,
    classify_scene_relation,
    semantic_trace,
)


class SemanticSpineTest(unittest.TestCase):
    def setUp(self):
        self.catalog = [
            {
                "argument_id": "ARG_TRAVEL",
                "source_argument_id": "SRC_TRAVEL",
                "claim_type": "scenario",
                "operator_priority": "core",
                "source_operator_expression": (
                    "旅行的时候带一件就好（百搭），办公室、通勤、休闲一件都可以hold住"
                ),
                "operator_expression": "适合日常穿搭场景",
                "core_value": "适合多种场景",
            },
            {
                "argument_id": "ARG_COLD",
                "source_argument_id": "SRC_COLD",
                "claim_type": "scenario",
                "source_operator_expression": "空调房、降温、夜晚或冬季出行可作为外搭",
                "operator_expression": "降温环境外搭",
            },
        ]
        self.market = build_product_market_context(
            product_code="1736444730937804794",
            target_country="TH",
            target_language="th",
            product_type="外套",
            selling_point_catalog=self.catalog,
        )

    def test_market_context_preserves_travel_and_compiles_cool_destination(self):
        primary = self.market["primary_usage_world"]
        self.assertEqual(primary["kind"], "TRAVEL_TO_COOLER_DESTINATION")
        self.assertEqual(primary["text"], "前往气温较低地区旅行")
        self.assertIn("TRAVEL", primary["semantic_tags"])
        self.assertIn("COLD_CLIMATE", primary["semantic_tags"])

    def test_script_spine_keeps_raw_operator_text_and_primary_clause(self):
        spine = build_script_semantic_spine(
            product_code="1736444730937804794",
            content_bundle={"selling_argument": self.catalog[0]},
            market_context=self.market,
        )
        self.assertEqual(
            spine["source_argument"]["raw_text"],
            self.catalog[0]["source_operator_expression"],
        )
        thesis = spine["script_thesis"]
        self.assertEqual(thesis["primary_narrative_context"], "前往气温较低地区旅行")
        self.assertEqual(thesis["selected_source_span"], "旅行的时候带一件就好（百搭）")
        self.assertEqual(thesis["core_buying_reason"], "一件商品适配多种穿搭或使用场景")

    def test_scene_relation_rejects_office_and_prefers_travel(self):
        spine = build_script_semantic_spine(
            product_code="1736444730937804794",
            content_bundle={"selling_argument": self.catalog[0]},
            market_context=self.market,
        )
        office = classify_scene_relation(
            spine,
            {"scene_family_key": "OFFICE_WORKBREAK", "scene_motif": "办公室收纳区"},
        )
        travel = classify_scene_relation(
            spine,
            {"scene_family_key": "TRAVEL_PREP", "scene_motif": "卧室行李箱旁整理出发穿搭"},
        )
        self.assertEqual(office["relation"], "CONFLICTS")
        self.assertEqual(travel["relation"], "SUPPORTS")

    def test_context_bridge_anchors_scene_only_when_visual_context_supports(self):
        spine = build_script_semantic_spine(
            product_code="1736444730937804794",
            content_bundle={"selling_argument": self.catalog[0]},
            market_context=self.market,
        )
        travel_bridge = build_context_bridge(
            spine,
            {
                "scene_family_key": "TRAVEL_PREP",
                "scene_motif": "卧室行李箱旁整理出发穿搭",
                "persona_role": "出发前整理行李的旅行者",
            },
        )
        neutral_bridge = build_context_bridge(
            spine,
            {"scene_family_key": "GENERIC_INDOOR", "scene_motif": "普通室内窗边"},
        )
        self.assertEqual(travel_bridge["voiceover_context_mode"], "SCENE_ANCHORED")
        self.assertEqual(neutral_bridge["voiceover_context_mode"], "SITUATION_ANCHORED")
        trace = semantic_trace(
            spine,
            travel_bridge,
            voiceover_context_mode="SCENE_ANCHORED",
        )
        self.assertEqual(trace["drift_status"], "NO_EXPLICIT_DRIFT")
        self.assertFalse(trace["is_blocking"])


if __name__ == "__main__":
    unittest.main()
