import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_insight_models import MarketDirectionCard, VOCLightSummary  # noqa: E402
from src.market_insight_report_generator import (  # noqa: E402
    MarketInsightReportGenerator,
    MarketInsightDirectionCopyRenderer,
)


def build_card(
    style_cluster: str,
    direction_family: str,
    direction_tier: str,
    item_count: int,
    sales_median: float,
    video_density: float,
    creator_density: float,
    top_forms=None,
    category: str = "hair_accessory",
):
    top_forms = top_forms or ["抓夹", "发夹"]
    return MarketDirectionCard(
        direction_canonical_key="VN__{category}__{name}".format(category=category, name=style_cluster),
        direction_instance_id="2026-04-21__VN__{category}__{name}".format(category=category, name=style_cluster),
        batch_date="2026-04-21",
        country="VN",
        category=category,
        direction_name=style_cluster,
        style_cluster=style_cluster,
        direction_family=direction_family,
        direction_item_count=item_count,
        direction_sales_median_7d=sales_median,
        direction_video_density_avg=video_density,
        direction_creator_density_avg=creator_density,
        direction_tier=direction_tier,
        confidence_sample_score=2,
        confidence_consistency_score=2,
        confidence_completeness_score=2,
        decision_confidence="high",
        confidence_reason_tags=[],
        top_forms=top_forms,
        form_distribution={"抓夹": 0.7, "发夹": 0.3},
        form_distribution_by_count={"抓夹": 0.7, "发夹": 0.3},
        form_distribution_by_sales={"抓夹": 0.85, "发夹": 0.15},
        core_elements=["低饱和纯色", "布艺"],
        scene_tags=["出门", "上班"],
        target_price_bands=["5-10 RMB", "10-15 RMB"],
        heat_level="medium",
        crowd_level="low",
        top_value_points=["提升精致度", "快速整理头发"],
        default_content_route_preference="neutral",
        representative_products=[{"product_id": "p1", "product_name": style_cluster}],
        priority_level="medium",
        selection_advice="测试",
        avoid_notes="避坑",
        confidence=0.8,
        product_count=item_count,
        average_heat_score=55.0,
        average_crowd_score=12.0,
        direction_key="VN__{category}__{name}".format(category=category, name=style_cluster),
    )


class MarketInsightReportGeneratorTest(unittest.TestCase):
    def test_generate_report_uses_fixed_five_layer_structure(self):
        generator = MarketInsightReportGenerator(skill_dir=ROOT)
        cards = [
            build_card("韩系轻通勤型", "审美风格型", "priority", 18, 420.0, 0.25, 0.12),
            build_card("甜感装饰型", "审美风格型", "crowded", 24, 210.0, 1.2, 0.8),
            build_card("基础通勤型", "日常场景型", "balanced", 16, 280.0, 0.4, 0.2),
            build_card("盘发效率型", "功能结果型", "crowded", 17, 380.0, 0.18, 0.1, top_forms=["盘发工具", "抓夹"]),
        ]

        payload, markdown, meta = generator.generate_report(
            cards=cards,
            voc_summary=VOCLightSummary(voc_status="skipped"),
            country="VN",
            category="hair_accessory",
            batch_date="2026-04-21",
            use_llm=False,
        )

        self.assertEqual(payload["report_version"], "v3.1_action_closed_loop")
        self.assertIn("business_summary", payload)
        self.assertTrue(payload["direction_actions"])
        self.assertTrue(payload["data_supplement_actions"][0]["cannot_be_only_action"])
        self.assertEqual(payload["market_regime_assessment"]["regime_code"], "strong_entry_window")
        self.assertEqual(payload["decision_summary"]["enter"]["display_names"], ["韩系轻通勤型", "基础通勤型", "盘发效率型"])
        self.assertEqual(payload["decision_summary"]["watch"]["display_names"], [])
        self.assertEqual(payload["decision_summary"]["avoid"]["display_names"], ["甜感装饰型"])
        self.assertTrue(isinstance(payload["watch_direction_table"], list))
        self.assertEqual(payload["opportunity_direction_cards"][0]["recommended_form"], "抓夹")
        self.assertEqual(payload["opportunity_direction_cards"][0]["suggested_test_count"], "8-12 款")
        self.assertEqual(payload["opportunity_direction_cards"][2]["suggested_test_count"], "4-6 款")
        self.assertTrue(payload["direction_matrix"]["table_lines"][0].startswith("| 大类 \\ 层级"))
        self.assertIn("### 审美风格型", payload["direction_matrix"]["display_lines"])
        self.assertTrue(payload["reverse_signals"]["hidden_risks"])
        self.assertTrue(payload["cross_system_recommendations"]["content_route_recommendations"])
        self.assertNotIn("盘发效率型", [item["direction_name"] for item in payload["reverse_signals"]["hidden_opportunities"]])
        self.assertEqual(
            [item["direction_name"] for item in payload["reverse_signals"]["hidden_risks"]],
            ["甜感装饰型"],
        )
        self.assertTrue(payload["cross_system_recommendations"]["content_route_recommendations"])
        self.assertEqual(meta["used_llm"], False)
        self.assertEqual(meta["fallback_count"], 3)
        self.assertIn("## 0. 批次结论", markdown)
        self.assertIn("## 1. 本批执行清单", markdown)
        self.assertIn("### 必须做", markdown)
        self.assertIn("### 可选做", markdown)
        self.assertIn("### 暂缓", markdown)
        self.assertIn("## 2. 关键依据", markdown)
        self.assertIn("## 3. 任务参数", markdown)
        self.assertIn("## 4. 附录索引", markdown)
        self.assertIn("补数据当作唯一动作", markdown)

    def test_generate_report_builds_watch_direction_table(self):
        generator = MarketInsightReportGenerator(skill_dir=ROOT)
        cards = [
            build_card("韩系轻通勤型", "审美风格型", "balanced", 18, 320.0, 0.78, 0.35),
            build_card("基础通勤型", "日常场景型", "balanced", 12, 290.0, 0.66, 0.21),
            build_card("发圈套组型", "形态专用型", "low_sample", 6, 270.0, 0.22, 0.18),
        ]

        payload, markdown, meta = generator.generate_report(
            cards=cards,
            voc_summary=VOCLightSummary(voc_status="skipped"),
            country="VN",
            category="hair_accessory",
            batch_date="2026-04-21",
            use_llm=False,
        )

        watch_rows = payload["watch_direction_table"]
        self.assertIn("发圈套组型", [row["direction_name"] for row in watch_rows])
        self.assertTrue(all(row["observe_reason"] for row in watch_rows))
        self.assertTrue(all(row["block_condition"] for row in watch_rows))
        self.assertIn("## 1. 本批执行清单", markdown)
        self.assertIn("发圈套组型", markdown)
        self.assertIn("### 暂缓", markdown)
        self.assertNotIn("样本数增至 10+", markdown)
        self.assertNotIn("视频密度降到 0.5", markdown)

    def test_v131_action_wording_has_no_hidden_candidate_conflict_or_internal_fields(self):
        generator = MarketInsightReportGenerator(skill_dir=ROOT)
        cards = [
            build_card("头盔友好整理型", "功能结果型", "low_sample", 1, 262.0, 0.0153, 0.01, top_forms=["抓夹"]),
            build_card("盘发效率型", "功能结果型", "crowded", 17, 380.0, 0.1049, 0.07, top_forms=["盘发工具", "抓夹"]),
            build_card("甜感装饰型", "审美风格型", "crowded", 146, 272.0, 0.45, 0.51),
            build_card("发箍修饰型", "形态专用型", "low_sample", 7, 1192.0, 0.08, 0.05, top_forms=["发箍"]),
        ]
        cards[1].new_product_entry_signal = {
            "type": "old_product_dominated",
            "confidence": "high",
            "rationale": "销量主要由老品贡献。",
        }

        payload, markdown, _ = generator.generate_report(
            cards=cards,
            voc_summary=VOCLightSummary(voc_status="skipped"),
            country="VN",
            category="hair_accessory",
            batch_date="2026-04-21",
            use_llm=False,
        )

        self.assertIn("## 0. 批次结论", markdown)
        self.assertIn("## 1. 本批执行清单", markdown)
        self.assertIn("头盔友好整理型", markdown)
        self.assertNotIn("适合作为低成本验证方向", markdown)
        self.assertNotIn("study_top_capacity_limited", markdown)
        self.assertNotIn("tested_sku_with_sales_count", markdown)
        self.assertNotIn("content_ctr", markdown)
        self.assertIn("不做方向级批量铺货", markdown)
        observations = "\n".join(payload["direction_matrix"]["observations"])
        self.assertNotIn("可立即进入", observations)

    def test_generate_report_classifies_medium_density_crowded_supply_overhang_as_avoid(self):
        generator = MarketInsightReportGenerator(skill_dir=ROOT)
        cards = [
            build_card("甜感装饰型", "审美风格型", "crowded", 146, 272.0, 0.45, 0.51),
            build_card("大体量气质型", "审美风格型", "crowded", 42, 301.0, 0.82, 0.49),
            build_card("盘发效率型", "功能结果型", "crowded", 17, 380.0, 0.10, 0.11, top_forms=["盘发工具", "抓夹"]),
            build_card("少女礼物感型", "审美风格型", "balanced", 13, 287.0, 0.32, 0.34),
        ]

        payload, markdown, meta = generator.generate_report(
            cards=cards,
            voc_summary=VOCLightSummary(voc_status="skipped"),
            country="VN",
            category="hair_accessory",
            batch_date="2026-04-21",
            use_llm=False,
        )

        self.assertEqual(payload["decision_summary"]["enter"]["display_names"], ["少女礼物感型", "盘发效率型"])
        self.assertEqual(payload["decision_summary"]["watch"]["display_names"], ["大体量气质型"])
        self.assertEqual(payload["decision_summary"]["avoid"]["display_names"], ["甜感装饰型"])
        self.assertEqual(
            [item["direction_name"] for item in payload["reverse_signals"]["hidden_risks"]],
            ["甜感装饰型", "大体量气质型"],
        )
        self.assertNotIn("盘发效率型", [item["direction_name"] for item in payload["reverse_signals"]["hidden_opportunities"]])
        self.assertIn("盘发效率型", payload["consistency_warnings"][0])
        self.assertEqual(payload["consistency_errors"], [])
        self.assertFalse(meta["report_publish_blocked"])
        self.assertIn("## 4. 附录索引", markdown)

    def test_generate_report_marks_mature_saturated_regime(self):
        generator = MarketInsightReportGenerator(skill_dir=ROOT)
        cards = [
            build_card("甜感装饰型", "审美风格型", "crowded", 146, 272.0, 0.45, 0.51),
            build_card("大体量气质型", "审美风格型", "crowded", 42, 319.5, 1.3907, 0.6935),
            build_card("发圈套组型", "形态专用型", "low_sample", 5, 227.0, 0.1337, 0.1097, top_forms=["发圈套组"]),
            build_card("少女礼物感型", "审美风格型", "balanced", 13, 287.0, 0.3151, 0.3447),
            build_card("盘发效率型", "功能结果型", "crowded", 17, 380.0, 0.1049, 0.07, top_forms=["盘发工具", "抓夹"]),
            build_card("头盔友好整理型", "功能结果型", "low_sample", 1, 262.0, 0.0153, 0.01, top_forms=["抓夹"]),
        ]

        payload, markdown, _ = generator.generate_report(
            cards=cards,
            voc_summary=VOCLightSummary(voc_status="skipped"),
            country="VN",
            category="hair_accessory",
            batch_date="2026-04-21",
            use_llm=False,
        )

        self.assertEqual(payload["market_regime_assessment"]["regime_code"], "mature_supply_structural")
        self.assertEqual(payload["market_regime_assessment"]["regime_label"], "成熟供给盘下的结构性验证期")
        self.assertNotIn("暗线机会只有 1 个", payload["market_regime_assessment"]["regime_reason"])
        self.assertIn("## 0. 批次结论", markdown)
        self.assertNotIn("本批次建议避开的方向", markdown)

    def test_render_llm_copy_retries_failed_direction_once(self):
        generator = MarketInsightReportGenerator(skill_dir=ROOT)
        cards = [
            build_card("少女礼物感型", "审美风格型", "balanced", 13, 287.0, 0.3151, 0.3447),
            build_card("盘发效率型", "功能结果型", "crowded", 17, 380.0, 0.1049, 0.1159, top_forms=["盘发工具", "抓夹"]),
        ]
        card_payloads = [generator._card_payload(card) for card in cards]
        family_groups = generator._group_by_family(card_payloads)
        enriched_cards = [generator._enrich_card(card, family_groups) for card in card_payloads]
        enter_cards = [card for card in enriched_cards if card["summary_bucket"] == "enter"]

        calls = {}

        def flaky_render(context):
            style = context["style_cluster"]
            calls[style] = calls.get(style, 0) + 1
            if style == "少女礼物感型" and calls[style] == 1:
                raise RuntimeError("transient render failure")
            return {
                "rationale_one_line": "{style} 文案".format(style=style),
                "intra_family_comparison": "{style} 对比".format(style=style),
                "risk_note": "{style} 风险".format(style=style),
            }

        generator.renderer.render = flaky_render

        rendered, meta = generator._render_llm_copy(enter_cards, use_llm=True)

        self.assertEqual(meta["fallback_count"], 0)
        self.assertEqual(meta["requested_direction_count"], 2)
        self.assertEqual(calls["少女礼物感型"], 2)
        self.assertEqual(calls["盘发效率型"], 1)
        self.assertFalse(meta["error_details"])
        self.assertEqual(rendered[enter_cards[0]["direction_canonical_key"]]["rationale_one_line"], "少女礼物感型 文案")

    def test_renderer_shortens_over_limit_fields_instead_of_failing(self):
        class _Completed(object):
            def __init__(self, stdout):
                self.stdout = stdout
                self.stderr = ""
                self.returncode = 0

        long_rationale = "该方向当前商品数17、销量位次1/2、视频密度0.1049、盘发工具销售占比0.6451，适合作为本轮优先进入方向。"
        payload = {
            "rationale_one_line": long_rationale,
            "intra_family_comparison": "在功能结果型内销量位次1/2，但视频位次和创作者位次均为2/2，说明成交表现更集中，内容供给相对更少。",
            "risk_note": "如果后续内容只停留在快速整理头发这一层，新增题材仍可能与现有盘发工具内容重合，影响后续持续放量效率。",
        }

        def fake_run(*args, **kwargs):
            return _Completed(json.dumps(payload, ensure_ascii=False))

        renderer = MarketInsightDirectionCopyRenderer(
            prompt_path=ROOT / "prompts" / "market_insight_direction_report_prompt_v1.txt",
            hermes_bin=ROOT / "run_pipeline.py",
            timeout_seconds=30,
            command_runner=fake_run,
        )
        result = renderer.render({"style_cluster": "盘发效率型"})

        self.assertLessEqual(len(result["rationale_one_line"]), 50)
        self.assertLessEqual(len(result["intra_family_comparison"]), 80)
        self.assertLessEqual(len(result["risk_note"]), 60)
        self.assertTrue(result["rationale_one_line"])

    def test_renderer_normalizes_risk_note_and_avoids_half_sentence(self):
        class _Completed(object):
            def __init__(self, stdout):
                self.stdout = stdout
                self.stderr = ""
                self.returncode = 0

        payload = {
            "rationale_one_line": "该方向视频密度0.3151、销量位次2/4，具备相对更稳的进入窗口。",
            "intra_family_comparison": "在审美风格型内，该方向视频密度最低，但销量和达人位次都不算绝对领先，说明仍要依赖更稳定的内容承接。",
            "risk_note": "当前路径为复刻优先，若复刻内容未能有效覆盖送礼、拍照/约会、提升精致度等核心价值点，需求边界可能难以验证。且销售主要集中在少数样本上，后续放大仍需谨慎。",
        }

        def fake_run(*args, **kwargs):
            return _Completed(json.dumps(payload, ensure_ascii=False))

        renderer = MarketInsightDirectionCopyRenderer(
            prompt_path=ROOT / "prompts" / "market_insight_direction_report_prompt_v1.txt",
            hermes_bin=ROOT / "run_pipeline.py",
            timeout_seconds=30,
            command_runner=fake_run,
        )
        result = renderer.render({"style_cluster": "少女礼物感型"})

        self.assertNotIn("当前路径为复刻优先", result["risk_note"])
        self.assertLessEqual(len(result["risk_note"]), 60)
        self.assertTrue(result["risk_note"].endswith(("。", "！", "？", "!", "?")))
        self.assertNotIn("且销售主要集中", result["risk_note"])

    def test_generate_report_uses_light_top_family_order_and_weight_advice(self):
        generator = MarketInsightReportGenerator(skill_dir=ROOT)
        cards = [
            build_card("薄针织开衫", "穿着诉求型", "crowded", 118, 272.0, 1.6982, 2.0515, top_forms=["开衫"], category="light_tops"),
            build_card("学院轻甜型", "风格气质型", "balanced", 57, 287.0, 0.5623, 0.3211, top_forms=["套头"], category="light_tops"),
            build_card("简洁轻熟型", "风格气质型", "crowded", 77, 260.0, 2.2419, 1.5155, top_forms=["衬衫"], category="light_tops"),
            build_card("显比例短上衣", "穿着结果型", "crowded", 51, 301.0, 1.3907, 0.6935, top_forms=["套头"], category="light_tops"),
            build_card("other", "other", "low_sample", 3, 120.0, 0.12, 0.08, top_forms=["other"], category="light_tops"),
        ]

        payload, markdown, _ = generator.generate_report(
            cards=cards,
            voc_summary=VOCLightSummary(voc_status="skipped"),
            country="TH",
            category="light_tops",
            batch_date="2026-04-22",
            use_llm=False,
        )

        self.assertEqual(payload["family_order"], ["穿着诉求型", "穿着结果型", "风格气质型", "other"])
        self.assertIn("### 穿着诉求型", payload["direction_matrix"]["display_lines"])
        self.assertIn("### 穿着结果型", payload["direction_matrix"]["display_lines"])
        self.assertIn("### 风格气质型", payload["direction_matrix"]["display_lines"])
        self.assertNotIn("### 审美风格型", payload["direction_matrix"]["display_lines"])
        weight_items = payload["cross_system_recommendations"]["scoring_weight_recommendations"]
        family_to_suggestion = {item["direction_family"]: item["suggestion"] for item in weight_items}
        self.assertIn("场景诉求清晰度", family_to_suggestion["穿着诉求型"])
        self.assertIn("上身结果可感知度", family_to_suggestion["穿着结果型"])
        self.assertIn("风格识别度", family_to_suggestion["风格气质型"])
        self.assertIn("## 1. 本批执行清单", markdown)
        self.assertIn("薄针织开衫", markdown)
        self.assertIn("学院轻甜型", markdown)
        self.assertIn("简洁轻熟型", markdown)

    def test_generate_report_blocks_publication_when_enter_and_hidden_risk_conflict_exist(self):
        generator = MarketInsightReportGenerator(skill_dir=ROOT)
        cards = [
            build_card("甜感装饰型", "审美风格型", "crowded", 20, 790.0, 0.0627, 0.09),
            build_card("韩系轻通勤型", "审美风格型", "crowded", 64, 250.0, 0.3846, 0.22),
        ]

        original_find_hidden_risks = generator._find_hidden_risks
        try:
            generator._find_hidden_risks = lambda cards: cards[:1]
            payload, _, meta = generator.generate_report(
                cards=cards,
                voc_summary=VOCLightSummary(voc_status="skipped"),
                country="TH",
                category="hair_accessory",
                batch_date="2026-04-23",
                use_llm=False,
            )
        finally:
            generator._find_hidden_risks = original_find_hidden_risks

        self.assertTrue(meta["report_publish_blocked"])
        self.assertEqual(meta["consistency_error_count"], 1)
        self.assertTrue(payload["consistency_errors"])
        self.assertIn("甜感装饰型", payload["consistency_errors"][0])

    def test_generate_report_keeps_crowded_low_density_opportunity_publishable(self):
        generator = MarketInsightReportGenerator(skill_dir=ROOT)
        cards = [
            build_card("盘发效率型", "功能结果型", "crowded", 17, 380.0, 0.1049, 0.07, top_forms=["盘发工具", "抓夹"]),
            build_card("甜感装饰型", "审美风格型", "crowded", 146, 272.0, 0.45, 0.51),
            build_card("少女礼物感型", "审美风格型", "balanced", 13, 287.0, 0.3151, 0.3447),
        ]

        payload, _, meta = generator.generate_report(
            cards=cards,
            voc_summary=VOCLightSummary(voc_status="skipped"),
            country="VN",
            category="hair_accessory",
            batch_date="2026-04-21",
            use_llm=False,
        )

        self.assertIn("盘发效率型", payload["decision_summary"]["enter"]["display_names"])
        self.assertFalse(meta["report_publish_blocked"])
        self.assertEqual(payload["consistency_errors"], [])

    def test_report_renders_action_override_for_old_product_content_gap(self):
        generator = MarketInsightReportGenerator(skill_dir=ROOT)
        card = build_card("盘发效率型", "功能结果型", "crowded", 18, 380.0, 0.1, 0.1, top_forms=["盘发工具", "抓夹"])
        card.product_age_structure = {
            "valid_age_sample_count": 18,
            "missing_age_rate": 0.0,
            "age_confidence": "high",
            "old_180d_sales_share": 0.912,
            "new_90d_sales_share": 0.047,
        }
        card.new_product_entry_signal = {
            "type": "old_product_dominated",
            "confidence": "high",
            "rationale": "销量主要由老品贡献。",
        }

        _, markdown, _ = generator.generate_report(
            cards=[card],
            voc_summary=VOCLightSummary(voc_status="skipped"),
            country="VN",
            category="hair_accessory",
            batch_date="2026-04-21",
            use_llm=False,
        )

        self.assertIn("老品替代核验", markdown)
        self.assertIn("老品占位偏强", markdown)
        self.assertIn("新产品替代", markdown) if "新产品替代" in markdown else self.assertIn("新品替代", markdown)


if __name__ == "__main__":
    unittest.main()
