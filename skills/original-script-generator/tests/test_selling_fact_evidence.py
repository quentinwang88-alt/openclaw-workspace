"""C1（方案 §4）：每个所选卖点保留原文／来源／商品图片版本／事实类型／适配结论。

方案要的不是"能不能用"，而是"用哪个口径"：只由运营意图支撑的体验句可以降级成
可见结果描述，但不能原样当成实测；外观看起来轻盈不等于重量轻。这里的每条断言都
对着方案里的一句话，避免把词表当成能力清单。
"""

from __future__ import annotations

import os
import unittest
from unittest import mock

from core.reality_reference import _select_value_proposition
from core.selling_fact_evidence import (
    BLOCKING_VERDICTS,
    EXPERIENCE_CURRENT_OBSERVATION,
    EXPERIENCE_NONE,
    EXPERIENCE_OPERATOR_CONFIRMED_HISTORY,
    FACT_EVIDENCE_TIERS,
    PART_STATE_ABSENT,
    PART_STATE_UNKNOWN,
    PART_STATE_VERIFIED,
    SELLING_FACT_EVIDENCE_ENV,
    SELLING_FACT_EVIDENCE_SCHEMA,
    TIER_AESTHETIC,
    TIER_APPEARANCE,
    TIER_EXPERIENCE,
    TIER_PERFORMANCE,
    TIER_STYLING,
    VERDICT_ADAPTED,
    VERDICT_CEILING,
    VERDICT_CONFLICT,
    VERDICT_NEEDS_SOURCE,
    VERDICT_UNKNOWN_PART,
    classify_fact_evidence,
    product_image_version,
    record_selected_argument,
    selling_fact_evidence_enabled,
    strip_forbidden_wording,
)


def _argument(text: str, **extra):
    argument = {
        "source_operator_expression": text,
        "claim_type": "benefit",
        "authorization_source": "FEISHU_OPERATOR_CONFIRMED",
        "source": "FEISHU_OPERATOR_CONFIRMED_ARGUMENT",
        "source_ref": "feishu-product-claims:rec#argument-v2-1",
        "source_claim_ids": ["PCL_TEST"],
        "source_argument_id": "PCS_TEST",
        "mapping_status": "UNMAPPED",
        "verification_status": "OPERATOR_CONFIRMED",
    }
    argument.update(extra)
    return argument


def _parts(state: str):
    return {"has_pendant": {"state": state, "source": "REGISTRY"}}


class FactTierTest(unittest.TestCase):
    """方案 C1 的五条事实类型，一条一档。"""

    def test_experience_wording_is_the_experience_tier(self):
        evidence = classify_fact_evidence(_argument("随手一夹就能搞定，省步骤"))
        self.assertEqual(evidence["tier"], TIER_EXPERIENCE)
        self.assertEqual(evidence["requires"], "CONFIRMED_EXPERIENCE")
        self.assertIn("随手一夹", evidence["tier_basis"]["matched"]["experience"])

    def test_performance_wording_is_the_performance_tier(self):
        evidence = classify_fact_evidence(_argument("戴一整天也不勒，很轻"))
        self.assertEqual(evidence["tier"], TIER_PERFORMANCE)
        self.assertEqual(evidence["requires"], "SPEC_OR_TEST_SOURCE")

    def test_material_wording_is_the_appearance_tier(self):
        evidence = classify_fact_evidence(_argument("双层纱质蝴蝶造型"))
        self.assertEqual(evidence["tier"], TIER_APPEARANCE)
        self.assertIn("纱", evidence["tier_basis"]["matched"]["material"])

    def test_a_holding_power_claim_is_a_performance_claim(self):
        # 真实目录原句（抓夹批次 OPERATOR_PCS_6D90B9CBA7FA4864）。它读起来像"适合
        # 什么发质"，实质是夹持力主张；漏掉它会掉进兜底的审美档而被放行为喜好。
        evidence = classify_fact_evidence(_argument("发量多也能稳稳夹住"))
        self.assertEqual(evidence["tier"], TIER_PERFORMANCE)
        self.assertEqual(evidence["requires"], "SPEC_OR_TEST_SOURCE")
        self.assertIn("稳稳", evidence["tier_basis"]["matched"]["performance"])

    def test_scene_wording_is_the_styling_tier(self):
        evidence = classify_fact_evidence(_argument("适合日常通勤搭配"))
        self.assertEqual(evidence["tier"], TIER_STYLING)

    def test_preference_wording_is_the_aesthetic_tier(self):
        evidence = classify_fact_evidence(_argument("我喜欢这种温柔氛围"))
        self.assertEqual(evidence["tier"], TIER_AESTHETIC)

    def test_a_sentence_touching_several_tiers_reports_the_riskiest(self):
        # "双层纱质蝴蝶造型，超唯美" 同时是外观、审美，还带材质词；口径必须由最严
        # 的那档决定，否则材质断言会借审美档溜过去。
        evidence = classify_fact_evidence(_argument("双层纱质蝴蝶造型，超唯美"))
        self.assertEqual(evidence["tier"], TIER_APPEARANCE)
        self.assertEqual(evidence["tiers"][0], TIER_APPEARANCE)
        self.assertIn(TIER_AESTHETIC, evidence["tiers"])
        self.assertIn("唯美", evidence["tier_basis"]["matched"]["aesthetic"])

    def test_a_scenario_claim_stays_styling_even_without_scene_words(self):
        # 归一化后的措辞可能已经丢掉场景词，但卖点本身是一条搭配建议。
        evidence = classify_fact_evidence(
            _argument("适合日常穿搭场景", claim_type="scenario")
        )
        self.assertEqual(evidence["tier"], TIER_STYLING)

    def test_the_five_tiers_are_the_documented_ones(self):
        self.assertEqual(
            set(FACT_EVIDENCE_TIERS),
            {
                TIER_EXPERIENCE,
                TIER_PERFORMANCE,
                TIER_APPEARANCE,
                TIER_STYLING,
                TIER_AESTHETIC,
            },
        )


class ExperienceAuthorityTest(unittest.TestCase):
    """方案：人工运营输入提供意图，不自动构成真实体验记录。"""

    def test_operator_input_is_not_an_experience_record(self):
        record = record_selected_argument(
            _argument("我试了一整天"), product_type="抓夹", top_category="配饰"
        )
        self.assertEqual(record["adaptation"]["experience_authority"], EXPERIENCE_NONE)

    def test_a_declared_authority_inside_the_existing_domain_is_kept(self):
        for authority in (
            EXPERIENCE_CURRENT_OBSERVATION,
            EXPERIENCE_OPERATOR_CONFIRMED_HISTORY,
        ):
            record = record_selected_argument(
                _argument("我试了一整天", experience_authority=authority),
                product_type="抓夹",
                top_category="配饰",
            )
            self.assertEqual(
                record["adaptation"]["experience_authority"], authority
            )

    def test_an_unknown_authority_value_is_not_trusted(self):
        record = record_selected_argument(
            _argument("我试了一整天", experience_authority="LOOKS_FINE_TO_ME"),
            product_type="抓夹",
            top_category="配饰",
        )
        self.assertEqual(record["adaptation"]["experience_authority"], EXPERIENCE_NONE)

    def test_an_authorised_experience_keeps_its_strength(self):
        record = record_selected_argument(
            _argument(
                "我试了一整天都戴着",
                experience_authority=EXPERIENCE_OPERATOR_CONFIRMED_HISTORY,
            ),
            product_type="抓夹",
            top_category="配饰",
        )
        self.assertEqual(record["adaptation"]["verdict"], VERDICT_ADAPTED)
        self.assertEqual(
            record["adaptation"]["experience_authority"],
            EXPERIENCE_OPERATOR_CONFIRMED_HISTORY,
        )
        # 放行的是"有人真的记过这件事"，不外推成商品性能。
        self.assertIn("不外推", record["adaptation"]["allowed_wording"])

    def test_an_unsourced_experience_claim_names_the_forbidden_words(self):
        record = record_selected_argument(
            _argument("无需繁琐步骤，随手一夹就能打造高颅顶或蓬松丸子头"),
            product_type="抓夹",
            top_category="配饰",
        )
        adaptation = record["adaptation"]
        self.assertEqual(adaptation["verdict"], VERDICT_NEEDS_SOURCE)
        self.assertTrue(adaptation["eligible"])
        for term in ("随手一夹", "一夹就", "无需繁琐"):
            self.assertIn(term, adaptation["forbidden_wording"])
        self.assertNotIn("丸子头", adaptation["forbidden_wording"])


class AdaptationVerdictTest(unittest.TestCase):
    def test_material_needs_a_source_even_without_an_image_version(self):
        record = record_selected_argument(
            _argument("雪纺花朵抓夹，仙气感十足"),
            product_type="抓夹",
            top_category="配饰",
        )
        adaptation = record["adaptation"]
        self.assertEqual(adaptation["verdict"], VERDICT_NEEDS_SOURCE)
        self.assertIn("雪纺", adaptation["forbidden_wording"])
        self.assertEqual(record["product_image_version"]["status"], "UNKNOWN")

    def test_a_product_image_does_not_turn_a_material_claim_into_a_fact(self):
        # 方案把"材质"与性能、佩戴体验并列，要求"相应来源"。商品图是外观来源，
        # 只能说明看起来如何，证明不了纤维成分 —— 所以有图也不放开材质断言。
        assets = [
            {
                "role": "PRODUCT_REFERENCE",
                "file_token": "FT_1",
                "sha256": "a" * 64,
                "authority": "OPERATION_TASK_PRODUCT_IMAGES",
            }
        ]
        record = record_selected_argument(
            _argument("雪纺花朵抓夹，仙气感十足"),
            product_reference_assets=assets,
            product_type="抓夹",
            top_category="配饰",
        )
        self.assertEqual(record["adaptation"]["verdict"], VERDICT_NEEDS_SOURCE)
        self.assertIn("雪纺", record["adaptation"]["forbidden_wording"])
        self.assertEqual(record["product_image_version"]["status"], "AVAILABLE")

    def test_an_appearance_only_claim_opens_up_once_the_image_version_is_known(self):
        # 图片支撑得住的是造型、层次与排列，这一档有图才真正放开。
        assets = [
            {
                "role": "PRODUCT_REFERENCE",
                "file_token": "FT_1",
                "sha256": "a" * 64,
                "authority": "OPERATION_TASK_PRODUCT_IMAGES",
            }
        ]
        record = record_selected_argument(
            _argument("双层蝴蝶造型，轮廓清晰"),
            product_reference_assets=assets,
            product_type="抓夹",
            top_category="配饰",
        )
        self.assertEqual(record["adaptation"]["verdict"], VERDICT_ADAPTED)
        self.assertEqual(record["adaptation"]["forbidden_wording"], [])

    def test_stacked_scenes_without_the_authorisation_are_kept_to_one(self):
        record = record_selected_argument(
            _argument(
                "百搭，适合日常约会、度假拍照、婚礼伴娘发型",
                claim_type="scenario",
                multi_scenario_authorized=False,
            ),
            product_type="抓夹",
            top_category="配饰",
        )
        adaptation = record["adaptation"]
        self.assertEqual(adaptation["verdict"], VERDICT_CEILING)
        conflict = adaptation["conflicts"][0]
        self.assertEqual(conflict["kind"], "MULTI_SCENARIO_UNAUTHORIZED")
        self.assertEqual(conflict["allowed_scenarios"], 1)
        self.assertEqual(
            conflict["stacked_scenes"],
            ["日常", "约会", "度假", "拍照", "婚礼", "伴娘"],
        )
        # 单个场景词是对的，错的只是并列；把它们整体禁掉会误杀可用的搭配建议。
        self.assertEqual(adaptation["forbidden_wording"], [])

    def test_an_authorised_multi_scenario_claim_is_not_downgraded(self):
        record = record_selected_argument(
            _argument(
                "适合日常约会、度假拍照",
                claim_type="scenario",
                multi_scenario_authorized=True,
            ),
            product_type="抓夹",
            top_category="配饰",
        )
        self.assertEqual(record["adaptation"]["verdict"], VERDICT_ADAPTED)

    def test_aesthetic_is_kept_as_a_first_person_preference(self):
        record = record_selected_argument(
            _argument("我喜欢这种温柔的层次感"), product_type="抓夹", top_category="配饰"
        )
        self.assertEqual(record["adaptation"]["verdict"], VERDICT_ADAPTED)
        self.assertIn("第一人称", record["adaptation"]["allowed_wording"])

    def test_stacked_hairstyles_without_the_authorisation_are_kept_to_one(self):
        # 真实目录原句（抓夹批次 OPERATOR_PCS_24827A1BD22E447B）。场景判据看不见
        # 造型词，但"适合三种发型"同样在承诺已经演示过三个结果，而一次成片只有一个
        # 冻结造型。
        record = record_selected_argument(
            _argument("适合半扎发、法式盘发、高马尾等多种发型"),
            product_type="抓夹",
            top_category="配饰",
        )
        adaptation = record["adaptation"]
        self.assertEqual(adaptation["verdict"], VERDICT_CEILING)
        conflict = adaptation["conflicts"][0]
        self.assertEqual(conflict["kind"], "MULTI_LOOK_UNAUTHORIZED")
        self.assertEqual(conflict["allowed_looks"], 1)
        self.assertEqual(
            conflict["stacked_looks"], ["半扎", "盘发", "马尾"]
        )
        # 单个造型词是对的；禁掉整类词会让"丸子头"这种可用结果也写不出来。
        self.assertEqual(adaptation["forbidden_wording"], [])
        self.assertIn("一个", adaptation["allowed_wording"])

    def test_a_single_hairstyle_is_not_a_stack(self):
        record = record_selected_argument(
            _argument("适合蓬松丸子头"), product_type="发夹", top_category="配饰"
        )
        self.assertEqual(record["adaptation"]["verdict"], VERDICT_ADAPTED)

    def test_an_authorised_multi_look_claim_is_not_downgraded(self):
        record = record_selected_argument(
            _argument("可盘发、可马尾、可半扎发", multi_look_authorized=True),
            product_type="发夹",
            top_category="配饰",
        )
        self.assertEqual(record["adaptation"]["verdict"], VERDICT_ADAPTED)

    def test_both_stacks_are_reported_when_both_are_present(self):
        record = record_selected_argument(
            _argument("通勤可半扎发，度假可马尾"),
            product_type="发夹",
            top_category="配饰",
        )
        kinds = {item["kind"] for item in record["adaptation"]["conflicts"]}
        self.assertEqual(
            kinds, {"MULTI_SCENARIO_UNAUTHORIZED", "MULTI_LOOK_UNAUTHORIZED"}
        )
        # 两条堆叠同时收窄时，允许的写法要把两个"只保留一个"都说出来。
        self.assertIn("场景", record["adaptation"]["allowed_wording"])
        self.assertIn("造型", record["adaptation"]["allowed_wording"])


class PartDependencyTest(unittest.TestCase):
    """方案：未知部件仅排除依赖该部件的主题；明确冲突需解决。"""

    def test_a_contradicted_part_blocks_the_candidate(self):
        evidence = classify_fact_evidence(
            _argument("带吊坠的水滴造型"),
            parts_evidence=_parts(PART_STATE_ABSENT),
        )
        from core.selling_fact_evidence import adaptation_verdict

        adaptation = adaptation_verdict(_argument("带吊坠的水滴造型"), evidence)
        self.assertEqual(adaptation["verdict"], VERDICT_CONFLICT)
        self.assertFalse(adaptation["eligible"])
        self.assertIn("吊坠", adaptation["reason"])
        self.assertIn(VERDICT_CONFLICT, BLOCKING_VERDICTS)

    def test_denying_a_part_is_not_asserting_it(self):
        evidence = classify_fact_evidence(
            _argument("素链，无吊坠"), parts_evidence=_parts(PART_STATE_ABSENT)
        )
        from core.selling_fact_evidence import adaptation_verdict

        adaptation = adaptation_verdict(_argument("素链，无吊坠"), evidence)
        self.assertNotEqual(adaptation["verdict"], VERDICT_CONFLICT)
        self.assertTrue(adaptation["eligible"])
        # 但"命名了部件"这件事要如实记下来，方便人工复核。
        self.assertFalse(evidence["mentioned_parts"][0]["asserted"])

    def test_an_unknown_part_alone_does_not_block(self):
        # 关键词命中只能说明"提到了这个部件"，不足以断言"这个主题依赖它"。
        record = record_selected_argument(
            _argument("前面缀着一枚小吊坠"),
            product_type="耳饰",
            top_category="配饰",
        )
        self.assertEqual(
            record["fact_type"]["mentioned_parts"][0]["state"], PART_STATE_UNKNOWN
        )
        self.assertTrue(record["adaptation"]["eligible"])

    def test_a_declared_required_part_that_is_unconfirmed_blocks(self):
        record = record_selected_argument(
            _argument("垂坠水滴造型", required_parts=["has_pendant"]),
            product_type="耳饰",
            top_category="配饰",
        )
        adaptation = record["adaptation"]
        self.assertEqual(adaptation["verdict"], VERDICT_UNKNOWN_PART)
        self.assertFalse(adaptation["eligible"])
        # 报告是给人读的，所以先说部件名；part_key 仍留在结构里供机器核对。
        self.assertIn("吊坠", adaptation["reason"])
        self.assertEqual(adaptation["conflicts"][0]["part_key"], "has_pendant")
        self.assertEqual(adaptation["conflicts"][0]["part_state"], PART_STATE_UNKNOWN)

    def test_a_declared_required_part_that_is_verified_does_not_block(self):
        record = record_selected_argument(
            _argument("垂坠水滴造型", required_parts=["has_pendant"]),
            product_type="耳饰",
            top_category="配饰",
            parts_evidence=_parts(PART_STATE_VERIFIED),
        )
        self.assertTrue(record["adaptation"]["eligible"])
        self.assertNotEqual(record["adaptation"]["verdict"], VERDICT_UNKNOWN_PART)

    def test_an_unresolvable_product_type_claims_nothing_about_parts(self):
        record = record_selected_argument(
            _argument("垂坠水滴造型", required_parts=["has_pendant"]),
            product_type="",
            top_category="",
        )
        self.assertFalse(record["fact_type"]["part_evidence_available"])
        # 没有部件证据时仍不下结论：声明了依赖就如实报告，但不据此宣称"未确认"。
        self.assertTrue(record["adaptation"]["eligible"])
        self.assertEqual(record["adaptation"]["verdict"], VERDICT_NEEDS_SOURCE)

    def test_subtypes_without_gated_actions_report_no_parts(self):
        # 抓夹/发夹在物理注册表里没有依赖部件的可选动作，这里必须什么都不报，
        # 而不是拿一个空表当成"部件都不存在"。
        record = record_selected_argument(
            _argument("双层纱质蝴蝶造型"), product_type="抓夹", top_category="配饰"
        )
        self.assertFalse(record["fact_type"]["part_evidence_available"])
        self.assertEqual(record["fact_type"]["mentioned_parts"], [])


class ProductImageVersionTest(unittest.TestCase):
    def test_no_asset_records_unknown_rather_than_a_version(self):
        record = product_image_version([])
        self.assertEqual(record["status"], "UNKNOWN")
        self.assertEqual(record["reason"], "NO_PRODUCT_REFERENCE_ASSET")
        self.assertEqual(record["record_id"], "")
        self.assertEqual(record["sha256"], [])

    def test_the_record_id_is_stable_and_content_bound(self):
        first = product_image_version(
            [{"role": "PRODUCT_REFERENCE", "file_token": "FT_1", "sha256": "a" * 64}]
        )
        again = product_image_version(
            [{"role": "PRODUCT_REFERENCE", "file_token": "FT_1", "sha256": "a" * 64}]
        )
        other = product_image_version(
            [{"role": "PRODUCT_REFERENCE", "file_token": "FT_1", "sha256": "b" * 64}]
        )
        self.assertEqual(first["status"], "AVAILABLE")
        self.assertEqual(first["record_id"], again["record_id"])
        self.assertNotEqual(first["record_id"], other["record_id"])
        self.assertTrue(first["record_id"].startswith("PIV_"))

    def test_non_product_assets_are_not_counted_as_the_product_image(self):
        record = product_image_version(
            [
                {"role": "PERSONA_REFERENCE", "file_token": "FT_P", "sha256": "c" * 64},
                {"role": "PRODUCT_REFERENCE", "file_token": "FT_1", "sha256": "a" * 64},
            ]
        )
        self.assertEqual(record["count"], 1)
        self.assertEqual(record["file_tokens"], ["FT_1"])


class ForbiddenWordingStripTest(unittest.TestCase):
    """只删不换：被禁的是断言所在的词组，不是那个孤零零的字。"""

    def test_a_single_character_material_takes_its_qualifier_with_it(self):
        # 实测原文：双层纱质蝴蝶造型，超唯美 —— 被禁的断言是"纱质"。
        self.assertEqual(
            strip_forbidden_wording("双层纱质蝴蝶造型，超唯美", ["纱"]),
            "双层蝴蝶造型，超唯美",
        )

    def test_a_whole_phrase_is_removed_as_is(self):
        self.assertEqual(strip_forbidden_wording("雪纺花朵抓夹", ["雪纺"]), "花朵抓夹")

    def test_nothing_is_substituted(self):
        out = strip_forbidden_wording("双层纱质蝴蝶造型", ["纱"])
        self.assertNotIn("纱", out)
        # 只删：结果里不会出现原文没有的字。
        self.assertTrue(set(out) <= set("双层纱质蝴蝶造型"))

    def test_a_sentence_made_only_of_banned_wording_yields_nothing(self):
        # 返回空表示没有可用口径，调用方据此记缺口，而不是发一个残句。
        self.assertEqual(strip_forbidden_wording("雪纺", ["雪纺"]), "")

    def test_punctuation_left_behind_by_a_deletion_is_cleaned(self):
        self.assertEqual(strip_forbidden_wording("纱质，超唯美", ["纱"]), "超唯美")

    def test_an_empty_text_stays_empty(self):
        self.assertEqual(strip_forbidden_wording("", ["纱"]), "")
        self.assertEqual(strip_forbidden_wording(None, ["纱"]), "")

    def test_no_terms_means_no_change(self):
        self.assertEqual(strip_forbidden_wording("双层纱质蝴蝶造型", []), "双层纱质蝴蝶造型")


class SelectedArgumentRecordTest(unittest.TestCase):
    """C1 的五要素：原文、来源、商品图片版本、事实类型、适配结论。"""

    def setUp(self):
        self.record = record_selected_argument(
            _argument(
                "百搭，适合日常约会、度假拍照、婚礼伴娘发型",
                claim_type="scenario",
                operator_expression="适合日常穿搭场景",
            ),
            product_type="抓夹",
            top_category="配饰",
        )

    def test_the_record_carries_all_five_required_elements(self):
        for key in (
            "source_text",
            "source",
            "product_image_version",
            "fact_type",
            "adaptation",
        ):
            self.assertIn(key, self.record)
        self.assertEqual(self.record["schema_version"], SELLING_FACT_EVIDENCE_SCHEMA)

    def test_the_reviewed_operator_sentence_is_the_recorded_original(self):
        self.assertEqual(
            self.record["source_text"], "百搭，适合日常约会、度假拍照、婚礼伴娘发型"
        )
        self.assertEqual(self.record["source_text_field"], "source_operator_expression")

    def test_provenance_points_back_at_concrete_fields(self):
        source = self.record["source"]
        self.assertEqual(source["source_ref"], "feishu-product-claims:rec#argument-v2-1")
        self.assertEqual(source["source_claim_ids"], ["PCL_TEST"])
        self.assertEqual(source["source_argument_id"], "PCS_TEST")
        self.assertEqual(source["authority"], "FEISHU_OPERATOR_CONFIRMED")

    def test_the_fact_type_names_the_requirement_not_just_a_label(self):
        self.assertEqual(self.record["fact_type"]["tier"], TIER_STYLING)
        self.assertEqual(self.record["fact_type"]["requires"], "COMPATIBLE_PLANNED_LOOK")
        self.assertEqual(self.record["fact_type"]["tier_label_zh"], "搭配建议")

    def test_the_adaptation_says_what_wording_is_permitted(self):
        adaptation = self.record["adaptation"]
        self.assertTrue(adaptation["allowed_wording"])
        self.assertTrue(adaptation["reason"])
        self.assertIn(adaptation["verdict"], (
            VERDICT_ADAPTED,
            VERDICT_CEILING,
            VERDICT_NEEDS_SOURCE,
            VERDICT_UNKNOWN_PART,
            VERDICT_CONFLICT,
        ))


class PlanningFilterTest(unittest.TestCase):
    """``eligible`` 只在开关打开时影响选点，默认关等于零行为变化。"""

    BLOCKING = {
        "value_id": "BLOCKING",
        "argument_kind": "SELLING_ARGUMENT",
        "script_role": "benefit_delivery",
        "primary_selling_point": "垂坠水滴造型，显气质",
        "claim_type": "benefit",
        "source": "FEISHU_OPERATOR_CONFIRMED_ARGUMENT",
        "required_parts": ["has_pendant"],
    }
    OTHER = {
        "value_id": "OTHER",
        "argument_kind": "SELLING_ARGUMENT",
        "script_role": "benefit_delivery",
        "primary_selling_point": "素圈造型，干净利落",
        "claim_type": "benefit",
        "source": "FEISHU_OPERATOR_CONFIRMED_ARGUMENT",
    }

    def _select(self):
        value, _tension = _select_value_proposition(
            {},
            {"content_carrier": "", "proof_mechanisms": []},
            selling_point_catalog=[dict(self.BLOCKING), dict(self.OTHER)],
            product_reference_assets=[],
            product_type="耳饰",
            top_category="配饰",
        )
        return value

    def test_the_switch_defaults_to_off(self):
        self.assertFalse(selling_fact_evidence_enabled())

    def test_with_the_switch_off_a_blocking_candidate_still_wins_on_score(self):
        with mock.patch.dict(os.environ, {SELLING_FACT_EVIDENCE_ENV: "0"}):
            self.assertEqual(self._select().get("value_id"), "BLOCKING")

    def test_with_the_switch_on_a_blocking_candidate_is_not_planned(self):
        with mock.patch.dict(os.environ, {SELLING_FACT_EVIDENCE_ENV: "1"}):
            self.assertEqual(self._select().get("value_id"), "OTHER")

    def test_a_needs_source_candidate_is_recorded_not_filtered(self):
        catalog = [
            {
                "value_id": "EXPERIENCE",
                "argument_kind": "SELLING_ARGUMENT",
                "script_role": "benefit_delivery",
                "primary_selling_point": "随手一夹就能搞定，省步骤",
                "claim_type": "benefit",
                "source": "FEISHU_OPERATOR_CONFIRMED_ARGUMENT",
            }
        ]
        with mock.patch.dict(os.environ, {SELLING_FACT_EVIDENCE_ENV: "1"}):
            value, _tension = _select_value_proposition(
                {},
                {"content_carrier": "", "proof_mechanisms": []},
                selling_point_catalog=catalog,
                product_reference_assets=[],
                product_type="抓夹",
                top_category="配饰",
            )
        # NEEDS_SOURCE 不是阻塞结论：口径降级，但方向不被砍掉。
        self.assertEqual(value.get("value_id"), "EXPERIENCE")


if __name__ == "__main__":
    unittest.main()
