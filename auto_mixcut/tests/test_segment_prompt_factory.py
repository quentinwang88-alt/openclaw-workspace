from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.segment_prompt_factory_skill import (
    SegmentPromptFactorySkill,
    _choose_perturbation,
    _load_factory_config,
    _load_prompt_variables_config,
    _post_assembly_validate,
    _post_validation_config,
)


class SegmentPromptFactoryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        os.environ["AUTO_MIXCUT_ROOT"] = str(Path(__file__).resolve().parents[1])
        os.environ["AUTO_MIXCUT_DB_PROVIDER"] = "sqlite"
        os.environ["AUTO_MIXCUT_DB"] = str(root / "db.sqlite")
        os.environ["AUTO_MIXCUT_OSS_ROOT"] = str(root / "oss")
        os.environ["AUTO_MIXCUT_OSS_PROVIDER"] = "local"
        os.environ["AUTO_MIXCUT_TEMP_ROOT"] = str(root / "tmp")
        os.environ["AUTO_MIXCUT_MOCK_FFMPEG"] = "1"
        os.environ["AUTO_MIXCUT_MOCK_LLM"] = "1"
        os.environ["ORIGINAL_SCRIPT_GENERATOR_DB_PATH"] = str(root / "missing_original.sqlite3")
        self.ctx = build_context()

    def tearDown(self):
        self.tmp.cleanup()

    def test_grade_a_controls_generation_policy_without_repeating_anchors(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(_brief(), _slot(ai_gen_grade="A", person_framing="ai_full_face"))
        self.assertTrue(res.success, res.to_dict())
        package = res.data
        positive = package["prompt"]["positive"]
        negative = package["prompt"]["negative"]

        self.assertEqual(package["duration_sec"], 4)
        self.assertRegex(package["segment_script_id"], r"^SPK-[A-F0-9]{8}$")
        self.assertEqual(package["person_framing"], "ai_local")
        self.assertEqual(package["gen_policy"]["num_variants"], 4)
        self.assertTrue(package["gen_policy"]["lock_character_ref"])
        self.assertIn("产品清晰为主体，对焦在产品上", positive)
        self.assertEqual(positive.count("珍珠蝴蝶结轮廓"), 1)
        self.assertIn("不要切镜", negative)
        self.assertIn("不要水印", negative)
        self.assertIn("不要字幕", negative)
        self.assertIn("不要文字", negative)
        self.assertEqual(package["anchor_ref"]["hard_anchors"], ["珍珠蝴蝶结轮廓", "奶油白缎面"])

        saved = self.ctx.repo.get("segment_prompt_packages", "segment_prompt_id", package["segment_prompt_id"])
        self.assertIsNotNone(saved)
        self.assertEqual(saved["package_status"], "created")
        self.assertEqual(saved["prompt_grade"], "A")
        self.assertEqual(saved["segment_script_id"], package["segment_script_id"])

    def test_reference_image_pack_metadata_is_saved(self):
        slot = _slot(ai_gen_grade="A")
        slot.update({"sku_id": "CREAM", "reference_image_pack_id": "REFPACK_VN_P001_CREAM_V1", "reference_image_version": 1})
        res = SegmentPromptFactorySkill(self.ctx).build_package(_brief(), slot)
        self.assertTrue(res.success, res.to_dict())

        saved = self.ctx.repo.get("segment_prompt_packages", "segment_prompt_id", res.data["segment_prompt_id"])
        self.assertIsNotNone(saved)
        self.assertEqual(saved["sku_id"], "CREAM")
        self.assertEqual(saved["reference_image_pack_id"], "REFPACK_VN_P001_CREAM_V1")
        self.assertEqual(saved["reference_image_version"], 1)

    def test_same_segment_type_keeps_prompt_detail_independent_from_grade(self):
        grade_a = SegmentPromptFactorySkill(self.ctx).build_package(_brief(), _slot(ai_gen_grade="A"), persist=False)
        grade_b = SegmentPromptFactorySkill(self.ctx).build_package(_brief(), _slot(ai_gen_grade="B"), persist=False)
        self.assertTrue(grade_a.success, grade_a.to_dict())
        self.assertTrue(grade_b.success, grade_b.to_dict())

        a_positive = grade_a.data["prompt"]["positive"]
        b_positive = grade_b.data["prompt"]["positive"]
        self.assertIn("产品清晰为主体，对焦在产品上", a_positive)
        self.assertNotIn("产品清晰为主体，对焦在产品上", b_positive)
        self.assertIn("带有珍珠蝴蝶结轮廓、奶油白缎面的发饰特写", a_positive)
        self.assertIn("带有珍珠蝴蝶结轮廓、奶油白缎面的发饰特写", b_positive)
        self.assertEqual(a_positive.count("珍珠蝴蝶结轮廓"), 1)
        self.assertEqual(b_positive.count("珍珠蝴蝶结轮廓"), 1)

    def test_category_is_normalized_but_raw_category_is_preserved(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(_outerwear_brief(category="womens_top"), _slot(ai_gen_grade="A"))
        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["raw_category"], "womens_top")
        self.assertEqual(res.data["category"], "womens_outerwear")

    def test_womens_outerwear_allows_face_policy(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(_outerwear_brief(), _slot(ai_gen_grade="B", person_framing="ai_full_face"))
        self.assertTrue(res.success, res.to_dict())
        prompt_text = "；".join(res.data["prompt"].values())
        self.assertNotIn("不要正脸", prompt_text)
        self.assertNotIn("正脸全身", prompt_text)

    def test_outerwear_negative_l1_is_complete_and_l2_is_limited(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(_outerwear_brief(), _slot(ai_gen_grade="A", segment_type="product_display"), persist=False)
        self.assertTrue(res.success, res.to_dict())
        negative = res.data["prompt"]["negative"]

        for redline in ["不要切镜", "不要转场", "不要分屏", "不要水印", "不要字幕", "不要文字", "不要竞品logo", "商品变形", "错品类", "全身正面导致版型失真", "前后帧衣服不一致"]:
            self.assertIn(redline, negative)
        self.assertNotIn("正脸全身", negative)
        self.assertIn("长时间只拍局部不拍轮廓", negative)
        self.assertIn("缺少至少一个完整上身结果镜", negative)
        self.assertIn("人物动作只是摆拍商品无动机", negative)

        l2_items = [
            "错误廓形",
            "变形衣形",
            "错误袖长",
            "错误衣长",
            "塑料假面料",
            "纸片薄面料",
            "融化的缝线",
            "不对称衣领",
            "缺失胸前双翻盖口袋结构",
        ]
        self.assertLessEqual(sum(1 for item in l2_items if item in negative), 6)

    def test_batch_packages_have_unique_perturbations(self):
        res = SegmentPromptFactorySkill(self.ctx).build_packages(_brief(), _slot(ai_gen_grade="A"), count=4, persist=False)
        self.assertTrue(res.success, res.to_dict())
        seeds = [
            tuple(item["gen_policy"]["perturbation_seed_group"][key] for key in ["camera_motion", "time_light", "composition", "color_tone", "props_env", "micro_arc"])
            for item in res.data["packages"]
        ]
        self.assertEqual(len(seeds), len(set(seeds)))

    def test_missing_hard_anchors_fails(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(_brief(hard_anchors=[]), _slot())
        self.assertFalse(res.success)
        self.assertEqual(res.error.code, "HARD_ANCHORS_REQUIRED")

    def test_post_validation_replaces_enum_and_dedups_anchor(self):
        package = _manual_package(
            positive="ai_local展示米白色短款版型，米白色短款版型再次出现，柔和自然光",
            negative="不要切镜；不要转场；不要分屏；不要水印；不要字幕；不要文字；不要竞品logo；商品变形；错品类；全身正面导致版型失真；前后帧衣服不一致",
            motion_arc="手扶衣摆，然后轻微侧转，然后衣形停留",
        )
        res = _post_assembly_validate(self.ctx, package)
        self.assertTrue(res.success, res.to_dict())
        positive = res.data["prompt"]["positive"]
        self.assertNotIn("ai_local", positive)
        self.assertIn("背影或侧面", positive)
        self.assertEqual(positive.count("米白色短款版型"), 1)

    def test_post_validation_blocks_unmapped_enum(self):
        package = _manual_package(
            positive="A_core商品展示",
            negative="不要切镜；不要转场；不要分屏；不要水印；不要字幕；不要文字；不要竞品logo；商品变形；错品类；全身正面导致版型失真；前后帧衣服不一致",
            motion_arc="手扶衣摆，然后轻微侧转，然后衣形停留",
        )
        res = _post_assembly_validate(self.ctx, package)
        self.assertFalse(res.success)
        self.assertEqual(res.error.code, "ENUM_LEAK")

    def test_post_validation_fixes_product_display_wearing_motion(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(_outerwear_brief(), _slot(ai_gen_grade="A", segment_type="product_display"), persist=False)
        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["prompt"]["motion_arc"], "手扶衣摆，然后轻微侧转，然后衣形停留")
        self.assertIn("fixed_motion_arc", {item["code"] for item in res.data["prompt_validation_warnings"]})

    def test_post_validation_reorders_l1_and_resolves_scene_conflict(self):
        package = _manual_package(
            positive="居家柔光里展示女装外套，街角自然移动，柔和自然光",
            negative="错误衣长；不要水印；不要切镜；不要转场；不要分屏；不要字幕；不要文字；不要竞品logo；商品变形；错品类；前后帧衣服不一致；全身正面导致版型失真",
            motion_arc="手扶衣摆，然后轻微侧转，然后衣形停留",
            segment_type="home_lifestyle",
        )
        res = _post_assembly_validate(self.ctx, package)
        self.assertTrue(res.success, res.to_dict())
        self.assertNotIn("街角", res.data["prompt"]["positive"])
        self.assertTrue(res.data["prompt"]["negative"].startswith("不要切镜；不要转场；不要分屏；不要水印"))
        self.assertIn("scene_space_conflict_resolved", {item["code"] for item in res.data["prompt_validation_warnings"]})

    def test_sampling_keeps_scene_and_light_in_same_space(self):
        pools = (_load_prompt_variables_config(self.ctx).get("variable_pools") or {})
        config = _load_factory_config(self.ctx)
        values = _choose_perturbation(_outerwear_brief()["material_anchor_brief"], _slot(segment_type="mirror_routine"), pools, set(), config)
        combined = values["time_light"] + values["props_env"]
        self.assertFalse(any(token in combined for token in ["街角", "户外冷风街景", "街边店铺", "窗边", "商场走廊"]))
        self.assertTrue(any(token in combined for token in ["玄关", "卧室", "试衣镜前", "居家", "室内", "衣柜旁", "舒适房间"]))

    def test_any_segment_resolves_space_once(self):
        pools = (_load_prompt_variables_config(self.ctx).get("variable_pools") or {})
        config = _load_factory_config(self.ctx)
        values = _choose_perturbation(_outerwear_brief()["material_anchor_brief"], _slot(segment_type="product_display"), pools, set(), config)
        combined = values["time_light"] + values["props_env"]
        indoor = any(token in combined for token in ["玄关", "卧室", "试衣镜前", "居家", "室内", "衣柜旁", "舒适房间"])
        outdoor = any(token in combined for token in ["街角", "户外冷风街景", "街边店铺", "窗边", "商场走廊", "咖啡店门口"])
        self.assertNotEqual(indoor, outdoor, values)

    def test_persona_layer_has_five_dimensions_and_segment_beat(self):
        product_display = SegmentPromptFactorySkill(self.ctx).build_package(_outerwear_brief(), _slot(ai_gen_grade="A", segment_type="product_display"), persist=False)
        tryon = SegmentPromptFactorySkill(self.ctx).build_package(_outerwear_brief(), _slot(ai_gen_grade="A", segment_type="tryon_result"), persist=False)
        self.assertTrue(product_display.success, product_display.to_dict())
        self.assertTrue(tryon.success, tryon.to_dict())

        pd_positive = product_display.data["prompt"]["positive"]
        tr_positive = tryon.data["prompt"]["positive"]
        for token in ["人物画像：", "年龄段=", "性别=", "肤色=", "发型发色=", "穿搭风格=", "单beat表演："]:
            self.assertIn(token, pd_positive)
        self.assertIn("专注端详", pd_positive)
        self.assertIn("看向商品", pd_positive)
        self.assertIn("眼神聚焦商品", pd_positive)
        self.assertIn("满意微笑", tr_positive)
        self.assertIn("可看镜头", tr_positive)
        self.assertIn("眼神变亮轻点头", tr_positive)
        self.assertIn("完整上身结果镜", tr_positive)
        self.assertNotIn("不要正脸", pd_positive + tr_positive)
        self.assertNotIn("局部裁切", pd_positive + tr_positive)
        self.assertNotIn("避免直视镜头", pd_positive + tr_positive)
        self.assertNotIn("自然看向旁边", pd_positive + tr_positive)
        self.assertNotIn("手部小幅整理商品", pd_positive + tr_positive)

    def test_batch_persona_participates_in_perturbation(self):
        res = SegmentPromptFactorySkill(self.ctx).build_packages(_outerwear_brief(), _slot(ai_gen_grade="A", segment_type="product_display"), count=7, persist=False)
        self.assertTrue(res.success, res.to_dict())
        personas = []
        for package in res.data["packages"]:
            positive = package["prompt"]["positive"]
            segment = next(part for part in positive.split("；") if part.startswith("人物画像："))
            personas.append(segment)
        self.assertGreater(len(set(personas)), 1)

    def test_confirm_beat_gaze_encourages_camera(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(_outerwear_brief(), _slot(ai_gen_grade="C", segment_type="mirror_routine"), persist=False)
        self.assertTrue(res.success, res.to_dict())
        positive = res.data["prompt"]["positive"]
        self.assertIn("看镜头确认", positive)
        self.assertNotIn("避免直视镜头", positive)

    def test_earrings_risk_negative_allows_face(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(_brief(category="earrings"), _slot(ai_gen_grade="A", segment_type="product_display"), persist=False)
        self.assertTrue(res.success, res.to_dict())
        prompt_text = "；".join(res.data["prompt"].values())
        self.assertIn("戴耳环动作", res.data["prompt"]["negative"])
        self.assertIn("手指进入耳部", res.data["prompt"]["negative"])
        self.assertIn("只拍近景细节不拍佩戴结果", res.data["prompt"]["negative"])
        self.assertNotIn("不要正脸", prompt_text)
        self.assertIn("人物画像：", res.data["prompt"]["positive"])

    def test_original_persona_asset_is_reused_and_saved(self):
        db_path = Path(self.tmp.name) / "original.sqlite3"
        _create_original_persona_db(db_path, product_id="VN_OUTER_PROMPT_001")
        os.environ["ORIGINAL_SCRIPT_GENERATOR_DB_PATH"] = str(db_path)

        res = SegmentPromptFactorySkill(self.ctx).build_package(_outerwear_brief(), _slot(ai_gen_grade="A", segment_type="tryon_result"))
        self.assertTrue(res.success, res.to_dict())
        package = res.data
        positive = package["prompt"]["positive"]
        negative = package["prompt"]["negative"]
        persona_context = package["persona_context"]

        self.assertEqual(persona_context["source"], "original_script_generator")
        self.assertEqual(persona_context["source_scope"], "product")
        self.assertRegex(positive, r"人物画像：年龄段=[^，；]+，性别=[^，；]+，肤色=[^，；]+，发型发色=[^，；]+，穿搭风格=[^，；]+")
        self.assertNotIn("来源=原创脚本", positive)
        self.assertNotIn("人物状态=R3", positive)
        self.assertNotIn("外观锚点=", positive)
        self.assertNotIn("穿搭规则=", positive)
        self.assertIn("可看镜头", positive)
        self.assertIn("满意微笑", positive)
        self.assertIn("眼神变亮轻点头", positive)
        self.assertIn("不要像棚拍模特一样僵硬", negative)

        saved = self.ctx.repo.get("segment_prompt_packages", "segment_prompt_id", package["segment_prompt_id"])
        self.assertEqual((saved["prompt_package_json"] or {})["persona_context"]["source"], "original_script_generator")

    def test_original_persona_asset_falls_back_to_category(self):
        db_path = Path(self.tmp.name) / "original_category.sqlite3"
        _create_original_persona_db(db_path, product_id="OTHER_OUTER_PRODUCT", product_type="上装", top_category="女装")
        os.environ["ORIGINAL_SCRIPT_GENERATOR_DB_PATH"] = str(db_path)

        res = SegmentPromptFactorySkill(self.ctx).build_package(_outerwear_brief(), _slot(ai_gen_grade="A", segment_type="product_display"), persist=False)
        self.assertTrue(res.success, res.to_dict())
        persona_context = res.data["persona_context"]
        self.assertEqual(persona_context["source"], "original_script_generator")
        self.assertEqual(persona_context["source_scope"], "category")
        self.assertEqual(persona_context["source_product_id"], "OTHER_OUTER_PRODUCT")

    def test_post_validation_blocks_origin_text_regression(self):
        cases = [
            "人物画像：来源=原创脚本，人物状态=R3 轻判断型；单beat表演：可看镜头，满意微笑，眼神变亮，半步后退，轻整理后确认",
            "人物画像：年龄段=轻熟，性别=女，肤色=偏白皙，发型发色=黑长直，穿搭风格=简约通勤；单beat表演：0-3s观察，满意微笑，眼神变亮，半步后退，轻整理后确认",
            "人物画像：年龄段=轻熟，性别=女，肤色=偏白皙，发型发色=黑长直，穿搭风格=棕色皮质拉链夹克；单beat表演：可看镜头，满意微笑，眼神变亮，半步后退，轻整理后确认",
        ]
        for positive in cases:
            with self.subTest(positive=positive):
                package = _manual_package(
                    positive=positive,
                    negative="不要切镜；不要转场；不要分屏；不要水印；不要字幕；不要文字；不要竞品logo；商品变形；错品类；全身正面导致版型失真；前后帧衣服不一致",
                    motion_arc="手扶衣摆，然后轻微侧转，然后衣形停留",
                )
                res = _post_assembly_validate(self.ctx, package)
                self.assertFalse(res.success)
                self.assertEqual(res.error.code, "REVERTED_TO_ORIGIN_TEXT")

    def test_product_only_outerwear_still_skips_person_layer(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(
            _outerwear_brief(),
            _slot(ai_gen_grade="B", person_framing="product_only", segment_type="product_still"),
            persist=False,
        )
        self.assertTrue(res.success, res.to_dict())
        package = res.data
        positive = package["prompt"]["positive"]
        negative = package["prompt"]["negative"]

        self.assertEqual(package["person_framing"], "product_only")
        self.assertEqual(package["segment_type"], "product_still")
        self.assertFalse(package["gen_policy"]["lock_character_ref"])
        self.assertIn("外套挂拍展示完整廓形", positive)
        self.assertIn("无人", positive)
        self.assertNotIn("人物画像：", positive)
        self.assertNotIn("单beat表演：", positive)
        self.assertNotIn("手部小幅整理商品", positive)
        self.assertIn("不要人物", negative)
        self.assertIn("不要人手", negative)

    def test_product_only_unboxing_allows_hands_but_not_person_layer(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(
            _outerwear_brief(),
            _slot(ai_gen_grade="A", person_framing="product_only", segment_type="unboxing"),
            persist=False,
        )
        self.assertTrue(res.success, res.to_dict())
        positive = res.data["prompt"]["positive"]
        negative = res.data["prompt"]["negative"]

        self.assertIn("只出手部与包装", positive)
        self.assertIn("拆包装取出外套", positive)
        self.assertNotIn("人物画像：", positive)
        self.assertNotIn("单beat表演：", positive)
        self.assertIn("不要面部", negative)
        self.assertIn("不要身体", negative)
        self.assertNotIn("不要人手", negative)

    def test_product_only_c6_blocks_person_words(self):
        package = _manual_package(
            positive="产品静物特写,稳定构图,干净背景,柔和自然光,无人物,对焦在产品质感与做工；人物画像：年龄段=轻熟；外套挂拍展示完整廓形,衣长/罗纹立领/面料垂感清晰,衣架或立体挂展,无人",
            negative="不要切镜；不要转场；不要分屏；不要水印；不要字幕；不要文字；不要竞品logo；商品变形；错品类；前后帧衣服不一致；全身正面导致版型失真",
            motion_arc="缓慢推近",
            segment_type="product_still",
        )
        package["person_framing"] = "product_only"
        res = _post_assembly_validate(self.ctx, package)

        self.assertFalse(res.success)
        self.assertEqual(res.error.code, "PRODUCT_ONLY_PERSON_LEAK")

    def test_earrings_uses_category_pool_without_outerwear_motion_words(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(
            _earrings_brief(),
            _slot(ai_gen_grade="A", segment_type="product_display"),
            persist=False,
        )
        self.assertTrue(res.success, res.to_dict())
        package = res.data
        prompt_text = "；".join(package["prompt"].values())

        self.assertEqual(package["category"], "earrings")
        self.assertNotIn("衣摆", prompt_text)
        self.assertNotIn("衣形", prompt_text)
        self.assertNotIn("扣上耳朵", prompt_text)
        self.assertNotIn("指尖拿耳饰", prompt_text)
        perturbation = package["gen_policy"]["perturbation_seed_group"]
        earrings_pool = (_load_prompt_variables_config(self.ctx).get("variable_pools") or {})["earrings"]
        self.assertIn(perturbation["camera_motion"], earrings_pool["camera_motion"])
        self.assertIn(perturbation["micro_arc"], earrings_pool["micro_arc"])

    def test_earrings_forbidden_segment_type_is_blocked(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(
            _earrings_brief(),
            _slot(ai_gen_grade="A", segment_type="before_go_out"),
            persist=False,
        )

        self.assertFalse(res.success)
        self.assertEqual(res.error.code, "SEGMENT_TYPE_FORBIDDEN_BY_CATEGORY")

    def test_earrings_product_only_uses_shared_pool_and_product_form(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(
            _earrings_brief(),
            _slot(ai_gen_grade="A", person_framing="product_only", segment_type="product_still"),
            persist=False,
        )
        self.assertTrue(res.success, res.to_dict())
        package = res.data
        positive = package["prompt"]["positive"]
        perturbation = package["gen_policy"]["perturbation_seed_group"]
        shared_pool = (_load_prompt_variables_config(self.ctx).get("variable_pools") or {})["product_only_shared"]

        self.assertIn("蓝色四瓣花朵造型耳饰", positive)
        self.assertNotIn("佩戴在耳垂外侧", positive)
        self.assertNotIn("随转头", positive)
        self.assertNotIn("卧室床头灯", positive)
        self.assertIn(perturbation["camera_motion"], shared_pool["camera_motion"])
        self.assertIn(perturbation["props_env"], shared_pool["props_env"])

    def test_bracelet_inferred_from_generic_category_uses_wrist_prompt(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(
            _bracelets_brief(category="general"),
            _slot(ai_gen_grade="A", segment_type="mirror_routine"),
            persist=False,
        )
        self.assertTrue(res.success, res.to_dict())
        package = res.data
        prompt_text = "；".join(package["prompt"].values())

        self.assertEqual(package["raw_category"], "general")
        self.assertEqual(package["category"], "bracelets")
        self.assertIn("手链", prompt_text)
        self.assertIn("手腕", prompt_text)
        self.assertNotIn("女装外套", prompt_text)
        self.assertNotIn("衣摆", prompt_text)
        self.assertNotIn("衣领", prompt_text)
        self.assertNotIn("耳垂", prompt_text)
        self.assertNotIn("耳侧", prompt_text)
        perturbation = package["gen_policy"]["perturbation_seed_group"]
        bracelet_pool = (_load_prompt_variables_config(self.ctx).get("variable_pools") or {})["bracelets"]
        self.assertIn(perturbation["camera_motion"], bracelet_pool["camera_motion"])
        self.assertIn(perturbation["micro_arc"], bracelet_pool["micro_arc"])

    def test_bracelet_product_only_uses_product_form_without_wrist_wearing(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(
            _bracelets_brief(),
            _slot(ai_gen_grade="A", person_framing="product_only", segment_type="product_still"),
            persist=False,
        )
        self.assertTrue(res.success, res.to_dict())
        positive = res.data["prompt"]["positive"]

        self.assertEqual(res.data["category"], "bracelets")
        self.assertIn("手链托盘陈列", positive)
        self.assertNotIn("人物画像：", positive)
        self.assertNotIn("单beat表演：", positive)
        self.assertNotIn("手腕近景", positive)
        self.assertNotIn("耳饰", positive)
        self.assertNotIn("外套", positive)

    def test_product_only_blocks_wear_effect_leak(self):
        package = _manual_package(
            positive="产品静物特写,稳定构图,干净背景,柔和自然光,无人物,对焦在产品质感与做工；耳饰悬挂或托盘陈列,金属光泽/流苏细节微距,无人无手；随转头轻微跟拍",
            negative="不要切镜；不要转场；不要分屏；不要水印；不要字幕；不要文字；不要竞品logo；商品变形；错品类；不要人物；不要人手；不要面部；不要身体；不要穿戴动作",
            motion_arc="缓慢推近",
            segment_type="product_still",
        )
        package["category"] = "earrings"
        package["person_framing"] = "product_only"
        res = _post_assembly_validate(self.ctx, package)

        self.assertFalse(res.success)
        self.assertEqual(res.error.code, "PRODUCT_ONLY_WEAR_EFFECT_LEAK")


def _brief(category: str = "hair_accessories", hard_anchors: list[str] | None = None) -> dict:
    return {
        "material_anchor_brief": {
            "product_id": "VN_HAIR_PROMPT_001",
            "display_family": "发饰",
            "product_subtype": "珍珠蝴蝶结发夹",
            "category": category,
            "primary_visual_result": "发夹清楚固定在头发上",
            "must_show": ["发夹在头发上"],
            "must_not_show": ["不要文字贴纸"],
            "hard_anchors": ["珍珠蝴蝶结轮廓", "奶油白缎面"] if hard_anchors is None else hard_anchors,
            "display_anchors": ["温柔发型效果"],
            "key_visual_constraints": ["发饰必须稳定可见"],
            "safe_micro_actions": ["轻轻夹上头发"],
            "forbidden_actions": ["不要竞品logo", "不要字幕文字"],
            "ai_shot_risk_profile": {"consistency_risk": "medium", "deformation_risk": "medium"},
        },
        "ai_local_human_brief": {
            "enabled": True,
            "gaze_options": ["视线略微向下"],
            "micro_behavior_options": ["单手轻轻调整发夹"],
            "body_language_options": ["侧后方局部入镜"],
            "forbidden_performance": ["夸张表演"],
        },
    }


def _outerwear_brief(category: str = "womens_outerwear") -> dict:
    data = _brief(category=category, hard_anchors=["米白色短款版型", "罗纹立领"])
    brief = data["material_anchor_brief"]
    brief.update(
        {
            "product_id": "VN_OUTER_PROMPT_001",
            "display_family": "女装外套",
            "product_subtype": "米白色短款夹克外套",
            "primary_visual_result": "短款外套版型清楚成型",
            "must_show": ["衣长至腰腹位置", "罗纹立领"],
            "key_visual_constraints": ["不要完整正脸全身", "衣服前后帧保持一致"],
            "forbidden_actions": ["正脸全身(AI禁止)", "前后帧衣服不一致", "错配衣长"],
        }
    )
    return data


def _earrings_brief() -> dict:
    data = _brief(category="earrings", hard_anchors=["蓝色四瓣花朵造型耳饰", "短款贴耳比例"])
    brief = data["material_anchor_brief"]
    brief.update(
        {
            "product_id": "VN_EARRING_PROMPT_001",
            "display_family": "耳饰",
            "product_subtype": "蓝色四瓣花朵造型耳饰",
            "product_form": ["蓝色四瓣花朵造型耳饰", "短款贴耳款式"],
            "wear_effect": ["佩戴在耳垂外侧，短款贴耳比例的佩戴效果"],
            "primary_visual_result": "耳饰已戴好后在侧面清楚可见",
            "must_show": ["蓝色四瓣花朵", "短款贴耳比例"],
            "key_visual_constraints": ["不要改成蝴蝶或星星", "不要生成长流苏"],
            "forbidden_actions": ["戴耳环动作", "手指进入耳部", "手触耳饰调整", "大幅转头"],
        }
    )
    return data


def _bracelets_brief(category: str = "bracelets") -> dict:
    data = _brief(category=category, hard_anchors=["金色四瓣花吊坠手链", "细链结构"])
    brief = data["material_anchor_brief"]
    brief.update(
        {
            "product_id": "TH_BRACELET_PROMPT_001",
            "display_family": "สร้อยข้อมือ",
            "product_subtype": "สร้อยข้อมือดอกไม้四瓣花手链",
            "product_form": ["金色四瓣花吊坠手链", "细链结构"],
            "wear_effect": ["佩戴在手腕上，吊坠靠近手背侧的佩戴效果"],
            "primary_visual_result": "手链在手腕上清楚可见",
            "must_show": ["四瓣花吊坠", "细链结构"],
            "key_visual_constraints": ["不要改成项链或耳饰", "不要生成多余吊坠"],
            "forbidden_actions": ["复杂扣戴过程", "大幅甩手", "拍成项链或耳饰"],
        }
    )
    return data


def _slot(ai_gen_grade: str = "A", person_framing: str = "ai_local", segment_type: str = "product_display") -> dict:
    return {
        "template_id": "RESULT_FIRST_15S",
        "slot_index": 0,
        "slot_role": "hero",
        "hook_intent": "product_clarity",
        "ai_gen_grade": ai_gen_grade,
        "segment_type": segment_type,
        "person_framing": person_framing,
        "duration_sec": 4,
    }


def _manual_package(positive: str, negative: str, motion_arc: str, segment_type: str = "product_display") -> dict:
    return {
        "segment_prompt_id": "manual-post-validate",
        "segment_script_id": "SPK-MANUAL01",
        "product_id": "VN_OUTER_PROMPT_001",
        "category": "womens_outerwear",
        "segment_type": segment_type,
        "prompt": {"positive": positive, "negative": negative, "motion_arc": motion_arc},
        "anchor_ref": {"hard_anchors": ["米白色短款版型", "罗纹立领"], "forbidden_actions": []},
    }


def _create_original_persona_db(path: Path, product_id: str, product_type: str = "外套", top_category: str = "女装") -> None:
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE pipeline_runs (
          run_id INTEGER PRIMARY KEY,
          record_id TEXT,
          product_code TEXT,
          product_type TEXT,
          top_category TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stage_results (
          stage_result_id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id INTEGER,
          record_id TEXT,
          product_code TEXT,
          stage_name TEXT,
          stage_order INTEGER,
          status TEXT,
          output_json TEXT,
          created_at TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO pipeline_runs (run_id, record_id, product_code, product_type, top_category) VALUES (1, 'rec-test', ?, ?, ?)",
        (product_id, product_type, top_category),
    )
    payload = {
        "persona_state": "R3 轻判断型",
        "appearance_anchor": "真实顺眼的东南亚日常女性气质，五官自然，人物服务于外套上身效果。",
        "hairstyle_rule": "黑长直或低扎发，发型低干扰。",
        "clothing_rule": "低饱和内搭，突出外套版型。",
        "emotion_arc_tag": "观察 → 满意 → 确认",
        "human_performance_contract": {
            "expression_arc": ["先观察外套轮廓", "看到整体轮廓后眼神变亮", "最后满意确认"],
            "gaze_plan": ["mirror", "camera", "mirror_full_result"],
            "micro_reaction_beats": ["轻看衣摆", "微微点头", "满意确认"],
            "body_language_beats": ["轻微侧身", "半步后退看整体", "自然站定"],
            "product_interaction_beats": ["轻扶衣摆", "轻整理衣领", "最后轻调整"],
            "forbidden_performance": ["不要像棚拍模特一样僵硬"],
        },
        "anti_template_warnings": ["不要全程固定微笑"],
    }
    conn.execute(
        """
        INSERT INTO stage_results (run_id, record_id, product_code, stage_name, stage_order, status, output_json, created_at)
        VALUES (1, 'rec-test', ?, 'persona_style_emotion_pack', 3, 'success', ?, '2026-06-03T00:00:00')
        """,
        (product_id, json.dumps(payload, ensure_ascii=False)),
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    unittest.main()
