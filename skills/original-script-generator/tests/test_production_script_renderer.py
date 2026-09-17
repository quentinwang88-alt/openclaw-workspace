import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from core import production_script_renderer as renderer
from core.production_script_renderer import (
    SCRIPT_RENDERER_VERSION,
    _apply_small_accessory_capture_projection,
    _capture_rhythm_contract,
    _preserve_category_capture_projection,
    _ResultProxy,
    attach_render_validation,
    build_production_projection,
    load_item_result,
    render_complete_production_script,
    render_stage0_video_generation_prompt,
    render_validation_block_reason,
    render_validation_blocks_delivery,
    render_video_generation_prompt,
)


class ProductionScriptRendererTest(unittest.TestCase):
    def setUp(self):
        script = {
            "complete_script_id": "SCSCRIPT_1",
            "script_concept": {"macro_structure": ["HOOK", "PROOF"]},
            "production_design": {
                "presentation_mode": "PERSON_ON_CAMERA",
                "capture_mode": "CREATOR_SELF_SHOT",
                "character": {
                    "identity": "看展穿搭者",
                    "appearance": "二十多岁泰国女性",
                    "hair_makeup": "自然妆发",
                    "speaking_personality": "朋友式分享",
                },
                "outfit": {
                    "base_outfit": "白色内搭和高腰裤",
                    "product_role": "短款外套",
                    "accessories": "单肩包",
                },
                "scene": {
                    "location": "展览入口白墙走廊",
                    "moment": "周末午后",
                    "lighting": "窗边自然光和普通顶灯混合",
                    "background": "导览牌旁保留一段墙面和入口处的普通动线",
                    "phone_placement": "手机放在入口旁矮台上",
                    "subject_position": "人物离手机约两步，在导览牌一侧自然停留",
                    "background_depth": "前景有矮台边缘，背景只保留入口与白墙纵深",
                    "lived_in_trace": "随手放在矮台上的帆布包",
                },
                "emotion": {
                    "starting_state": "自然走入",
                    "natural_change": "停下看展签",
                    "ending_state": "继续前行",
                },
            },
            "product_usage": {"identity_anchors_preserved": ["米白短款外套"]},
            "continuous_voiceover": {
                "hook_id": "USER_ADVOCACY_STANCE",
                "target_text": "สาวๆ ตัวนี้ใส่แล้วสัดส่วนดีค่ะ",
                "chinese_translation": "姐妹们，这件穿上比例很好。",
            },
            "storyboard": [
                {
                    "shot_no": 1,
                    "time_range": "0-3s",
                    "narrative_role": "HOOK",
                    "visual_content": "人物走入白墙走廊",
                    "character_action": "自然前行",
                    "natural_emotion": "专注",
                    "camera": "固定全身景",
                    "product_anchors_visible": ["米白短款外套"],
                    "supported_claim_keys": ["CLM_1"],
                }
            ],
            "video_generation_brief": {
                "production_design": {},
                "storyboard": [],
                "instruction": "保持连续事件",
            },
            "generation_provenance": {"model": "gpt-5.6-sol", "reasoning_effort": "high"},
        }
        self.item = SimpleNamespace(
            result_json=json.dumps({"script": script}, ensure_ascii=False),
            content_bundle_json=json.dumps(
                {"selling_argument": {"core_value": "腿部线条视觉更修长"}},
                ensure_ascii=False,
            ),
            batch_item_id="OCI_1",
            item_index=1,
            product_code="P1",
            macro_family_key="HOOK>PROOF",
            carrier_mode="WEARER_ACTIVE",
            actual_hook_id="USER_ADVOCACY_STANCE",
            requested_hook_id="USER_ADVOCACY_STANCE",
            visual_signature="人|展览|走入|前行",
            cluster_id=2,
            cluster_version="v1",
            selection_run_id="SR_1",
            direction_assignment_id="SRA_1",
            content_bundle_id="CBR_1",
            creative_contract_id="CDV_1",
            item_snapshot_hash="SN_1",
        )
        self.batch = SimpleNamespace(
            batch_id="OCB_1",
            product_code="P1",
            target_country="泰国",
            target_language="泰语",
            top_category="女装",
            product_type="外套",
            duration_seconds=15,
        )

    def test_complete_script_keeps_production_details(self):
        text = render_complete_production_script(item=self.item, duration_seconds=15)
        for marker in ("【人物设定】", "【完整穿搭】", "【场景设定】", "【人物状态】", "【连续口播】", "【分镜01"):
            self.assertIn(marker, text)
        self.assertIn("姐妹们", text)
        self.assertIn("手机位置：手机放在入口旁矮台上", text)
        self.assertIn("生活痕迹：随手放在矮台上的帆布包", text)

    def test_ugc_prompt_keeps_scene_execution_detail_without_commercial_rewrite(self):
        text = render_video_generation_prompt(item=self.item, duration_seconds=15)
        self.assertIn("手机位置：手机放在入口旁矮台上", text)
        self.assertIn("窗边自然光和普通顶灯混合", text)
        self.assertIn("随手放在矮台上的帆布包", text)

    def test_video_prompt_keeps_whole_video_semantic_context(self):
        result = json.loads(self.item.result_json)
        script = result["script"]
        script["video_generation_brief"].update({
            "render_profile": "UGC_NATIVE_V2_MULTICLIP",
            "production_design": script["production_design"],
            "storyboard": script["storyboard"],
            "voiceover": script["continuous_voiceover"],
            "semantic_context": {
                "primary_narrative_context": "前往气温较低地区旅行",
                "core_buying_reason": "一件商品适配多种穿搭或使用场景",
            },
        })
        self.item.result_json = json.dumps(result, ensure_ascii=False)
        text = render_video_generation_prompt(item=self.item, duration_seconds=15)
        self.assertIn("【整片语义主线｜不做逐句逐镜绑定】", text)
        self.assertIn("主消费情境：前往气温较低地区旅行", text)
        self.assertIn("核心购买理由：一件商品适配多种穿搭或使用场景", text)

    def test_direct_share_prompt_removes_overlapping_behavior_controls(self):
        result = json.loads(self.item.result_json)
        script = result["script"]
        base = script["storyboard"][0]
        script["storyboard"] = [
            {
                **base,
                "shot_no": index,
                "time_range": time_range,
                "visual_content": visual,
                "character_action": "面对自己的手机自然分享",
                "natural_emotion": "轻满意",
            }
            for index, (time_range, visual) in enumerate(
                [
                    ("0-4s", "人物已经穿好外套直接分享"),
                    ("4-9s", "在身状态下看清外套细节"),
                    ("9-15s", "人物与整套穿搭保持清楚"),
                ],
                1,
            )
        ]
        profile = {
            "enabled": True,
            "recording_mode": "CREATOR_DIRECT_SHARE",
            "capture_preset": "WORN_DIRECT_SHARE",
            "planned_visible_clip_count": 3,
        }
        contract = {
            "schema_version": "capture-rhythm-contract-v5-structure-visible-clips",
            "profile": "NATIVE_MULTI_CLIP_V1",
            "capture_unit_count": 3,
            "capture_setup_mode": "FIXED_PHONE_MULTI_CLIP",
            "capture_grammar": "WORN_DIRECT_SHARE",
            "creator_recording_profile": profile,
            "macro_structure": ["HOOK", "PROOF"],
            "structure_unit_roles": ["HOOK", "PROOF", "PROOF"],
            "edit_style": "NATIVE_HARD_CUT",
        }
        script["video_generation_brief"] = {
            "schema_version": "production-video-brief-v11-semantic-context",
            "render_profile": "UGC_NATIVE_V2_MULTICLIP",
            "capture_mode": "CREATOR_SELF_SHOT",
            "creator_recording_profile": profile,
            "recording_context": {
                "recording_motivation": "主动分享外套",
                "camera_relationship": "固定手机",
            },
            "capture_rhythm_contract": contract,
            "production_design": script["production_design"],
            "storyboard": script["storyboard"],
            "product_truth": {
                "product_identity": "米白短款外套",
                "identity_anchors": ["米白短款外套"],
            },
            "voiceover": script["continuous_voiceover"],
            "visual_execution_contract": {
                "schema_version": "visual-execution-contract-v4-wearable-saliency",
                "opening_scene_projection": {
                    "location_identity": "展览入口",
                    "opening_background_anchor": "导览牌",
                },
            },
        }
        self.item.result_json = json.dumps(result, ensure_ascii=False)

        text = render_video_generation_prompt(item=self.item, duration_seconds=15)

        self.assertIn("【拍摄方式｜达人直接分享】", text)
        self.assertIn("人物从开头已经穿好商品并全程保持穿着", text)
        self.assertIn("【人物、穿搭与生活场景】", text)
        self.assertNotIn("【达人主动分享关系】", text)
        self.assertNotIn("本段相对上一段的新信息", text)
        self.assertNotIn("【首帧/第一拍摄单元场景投影", text)
        self.assertNotIn("自然反应：", text)
        self.assertNotIn("本条分享动机：", text)

    def test_ugc_prompt_separates_persona_and_product_reference_authority(self):
        result = json.loads(self.item.result_json)
        script = result["script"]
        script["video_generation_brief"] = {
            "production_design": script["production_design"],
            "storyboard": script["storyboard"],
            "product_truth": {"identity_anchors": ["米白短款外套"]},
            "voiceover": script["continuous_voiceover"],
            "persona_selection_contract": {
                "availability": "AVAILABLE",
                "persona_id": "TH_PERSONA_001",
                "persona_name": "泰国咖啡店自然分享女生",
                "reference_strategy": "PERSONA_PRODUCT_COMPOSITE_PREFERRED",
                "script_projection": {
                    "identity": "泰国日常创作者",
                    "appearance": "自然未精修肤质",
                    "hair_makeup": "黑色长发和轻妆",
                },
            },
            "outfit_selection_contract": {
                "template_id": "STYLE_CAFE_001",
                "template_display_name": "都市咖啡通勤穿搭",
                "accessory_items": ["棕色小号肩包", "细金属耳环"],
                "preferred_persona_ids": ["TH_PERSONA_001"],
            },
            "outfit_scene_affinity_contract": {
                "policy_version": "outfit-scene-affinity-v2-exact-soft-boost",
                "template_id": "STYLE_CAFE_001",
                "selected_scene_family": "CAFE_DINING",
                "match_status": "MATCHED",
                "ranking_bonus": 30,
                "authority": "SOFT_PREFERENCE",
                "hard_required": False,
            },
            "outfit_persona_affinity_contract": {
                "policy_version": "outfit-persona-affinity-v1-soft",
                "outfit_template_id": "STYLE_CAFE_001",
                "selected_persona_id": "TH_PERSONA_001",
                "match_status": "MATCHED",
                "authority": "SOFT_PREFERENCE",
                "hard_required": False,
            },
        }
        self.item.result_json = json.dumps(result, ensure_ascii=False)
        text = render_video_generation_prompt(item=self.item, duration_seconds=15)
        self.assertIn("【人物身份锁｜与商品参考分权】", text)
        self.assertIn("TH_PERSONA_001", text)
        self.assertIn("商品参考图只决定商品外观", text)
        projection = build_production_projection(batch=self.batch, item=self.item)
        self.assertEqual(projection["persona_id"], "TH_PERSONA_001")
        self.assertEqual(
            projection["persona_name"], "泰国咖啡店自然分享女生"
        )
        self.assertEqual(projection["outfit_template_id"], "STYLE_CAFE_001")
        self.assertEqual(projection["outfit_template_name"], "都市咖啡通勤穿搭")
        self.assertEqual(projection["outfit_accessories"], "棕色小号肩包；细金属耳环")
        self.assertEqual(projection["outfit_scene_match"], "已匹配")
        self.assertIn(
            "outfit-scene-affinity-v2-exact-soft-boost",
            projection["outfit_scene_contract_json"],
        )
        self.assertEqual(projection["outfit_persona_match"], "MATCHED")
        self.assertIn(
            "outfit-persona-affinity-v1-soft",
            projection["outfit_persona_contract_json"],
        )
        self.assertEqual(
            projection["reference_strategy"],
            "PERSONA_PRODUCT_COMPOSITE_PREFERRED",
        )
        complete = render_complete_production_script(
            item=self.item, duration_seconds=15
        )
        self.assertIn("人物模板：泰国咖啡店自然分享女生", complete)
        self.assertIn("穿搭模板：都市咖啡通勤穿搭", complete)
        self.assertIn("穿搭×场景：已匹配", complete)
        self.assertIn("配饰道具：棕色小号肩包；细金属耳环", complete)
        self.assertIn("人物×穿搭：MATCHED", complete)

    def test_video_prompt_excludes_internal_lineage_and_translation(self):
        text = render_video_generation_prompt(item=self.item, duration_seconds=15)
        self.assertIn("สาวๆ", text)
        self.assertNotIn("CLM_1", text)
        self.assertNotIn("姐妹们", text)

    def test_ugc_prompt_prioritizes_product_identity_and_removes_polished_execution(self):
        result = json.loads(self.item.result_json)
        script = result["script"]
        script["video_generation_brief"] = {
            "production_design": script["production_design"],
            "storyboard": [{
                **script["storyboard"][0],
                "visual_content": "人物走入白墙走廊，背景车辆保持静置虚化",
                "camera": "正面固定全身景，稳定器轻微后移",
            }],
            "product_truth": {
                "product_identity": "棕色短款翻领上装",
                "identity_anchors": [
                    "翻领结构与竖向前襟门襟",
                    "正面四颗圆形纹理扣，左右袖口各一颗扣子",
                ],
                "visible_detail_anchors": ["翻领与前襟扣子近景"],
            },
            "voiceover": script["continuous_voiceover"],
        }
        self.item.result_json = json.dumps(result, ensure_ascii=False)

        text = render_video_generation_prompt(item=self.item, duration_seconds=15)

        self.assertIn("【商品身份锁｜最高优先级】", text)
        self.assertIn("正面四颗圆形纹理扣", text)
        self.assertIn("禁止将参考图中的前襟扣子改成双排扣", text)
        self.assertIn("【拍摄方式｜UGC_NATIVE_V1】", text)
        self.assertNotIn("人物状态：", text)
        self.assertNotIn("稳定器轻微后移", text)
        self.assertNotIn("背景车辆保持静置虚化", text)
        self.assertNotIn("虚化", text.split("【人物、穿搭与生活场景】", 1)[1])
        self.assertIn("商品一致性优先于场景美感和镜头效果", text)

    def test_old_scarf_brief_rebuilds_accessory_identity_lock_on_render(self):
        result = json.loads(self.item.result_json)
        script = result["script"]
        script["video_generation_brief"] = {
            "production_design": script["production_design"],
            "storyboard": script["storyboard"],
            "product_truth": {
                "product_identity": "深蓝条纹波点丝巾",
                "identity_anchors": ["深蓝底色", "条纹与波点图案"],
                "visible_detail_anchors": ["方形轮廓与图案布局"],
            },
            "product_identity_lock": {
                "compiler_version": "product-identity-lock-v2",
                "must_preserve": ["深蓝条纹波点丝巾"],
                "negative_constraints": ["禁止改变衣长、领型、前襟和袖口"],
            },
            "category_execution_extension": {
                "domain": "ACCESSORY",
                "profile": {
                    "product_subtype": "silk_scarf",
                    "wearing_zone": "NECK_UPPER_BODY",
                },
            },
            "accessory_execution_brief": {"product_relation": "已经搭配在颈部"},
            "voiceover": script["continuous_voiceover"],
        }
        self.item.result_json = json.dumps(result, ensure_ascii=False)

        text = render_video_generation_prompt(item=self.item, duration_seconds=15)

        self.assertIn("禁止改变参考图和已批准锚点中的颜色、图案布局", text)
        self.assertNotIn("衣长、领型、前襟和袖口", text)
        self.assertIn("可选轻互动：最多自然采用一个，也可以不用", text)

    def test_frozen_action_design_is_rendered_once_as_the_action_mainline(self):
        result = json.loads(self.item.result_json)
        script = result["script"]
        action = {
            "primary_action_mode": "SIMPLE_WEAR_PROCESS",
            "start_state": "丝巾已经绕过颈部并形成一个松结",
            "core_action": "把短端穿过已有松结并轻轻收好",
            "end_state": "停留展示领口与上半身搭配结果",
            "supporting_scene_action": "拿起桌边小包→自然准备离开",
        }
        script["video_generation_brief"] = {
            "production_design": script["production_design"],
            "storyboard": script["storyboard"],
            "product_truth": {
                "canonical_product_type": "silk_scarf",
                "product_identity": "深蓝条纹波点丝巾",
                "identity_anchors": ["深蓝底色", "条纹与波点图案"],
            },
            "category_execution_extension": {
                "domain": "ACCESSORY",
                "profile": {"product_subtype": "silk_scarf"},
            },
            "accessory_execution_brief": {
                "product_relation": "已经搭配在颈部",
                "selected_action_design": action,
                "optional_simple_interactions": ["轻托垂端", "拨开头发"],
                "wear_state_contract": {
                    "initial_state": "IN_PROGRESS",
                    "continuity_rule_zh": "开场保持未完成状态，只完成冻结的一个简单步骤；不得先展示完整佩戴结果后再重新系结",
                },
                "hand_anatomy_guard": {
                    "guidance_zh": "画面中最多出现同一人物自然生长的两只手，不出现第三只手或助手手臂",
                },
            },
            "action_design": action,
            "voiceover": script["continuous_voiceover"],
        }
        self.item.result_json = json.dumps(result, ensure_ascii=False)

        text = render_video_generation_prompt(item=self.item, duration_seconds=15)

        self.assertIn("【本条动作主线｜只执行这一条】", text)
        self.assertIn("把短端穿过已有松结并轻轻收好", text)
        self.assertIn("辅助生活衔接：拿起桌边小包→自然准备离开", text)
        self.assertNotIn("可选轻互动：", text)
        self.assertNotIn("商品关系：已经搭配在颈部", text)
        self.assertIn("佩戴连续性：开场保持未完成状态", text)
        self.assertIn("不出现第三只手或助手手臂", text)

    def test_hidden_snaps_render_as_one_visible_row_and_product_negative_is_up_front(self):
        result = json.loads(self.item.result_json)
        script = result["script"]
        ambiguous_anchor = "前襟为按扣与暗扣结构，左侧可见五颗圆形扣，右侧对应五颗暗扣"
        script["production_design"]["outfit"]["product_role"] = (
            "米白外套作为外层，左侧可见五颗圆形扣，右侧对应五颗暗扣"
        )
        script["video_generation_brief"] = {
            "production_design": script["production_design"],
            "storyboard": [{
                **script["storyboard"][0],
                "visual_content": "人物靠近手机，让左侧五颗圆形扣与右侧对应五颗暗扣同时可见",
                "character_action": "双手短暂靠近两侧扣位后自然放下",
                "product_anchors_visible": [ambiguous_anchor],
            }],
            "product_truth": {
                "product_identity": "米白色短款外套",
                "identity_anchors": [ambiguous_anchor],
                "visible_detail_anchors": ["前襟扣位细节"],
            },
            "voiceover": script["continuous_voiceover"],
        }
        self.item.result_json = json.dumps(result, ensure_ascii=False)

        text = render_video_generation_prompt(item=self.item, duration_seconds=15)

        self.assertIn("前襟只允许一列五颗可见扣子", text)
        self.assertIn("另一侧暗扣属于隐藏闭合件，不得显示为第二列可见纽扣", text)
        self.assertIn("禁止把隐藏暗扣画成外露纽扣", text)
        self.assertNotIn("左侧可见五颗圆形扣，右侧对应五颗暗扣", text)
        self.assertNotIn("左侧五颗圆形扣与右侧对应五颗暗扣同时可见", text)
        self.assertNotIn("两侧扣位", text)
        self.assertLess(text.index("【商品负向约束】"), text.index("【拍摄方式｜UGC_NATIVE_V1】"))

    def test_creator_self_shot_prompt_uses_one_phone_relationship_not_director_cameras(self):
        result = json.loads(self.item.result_json)
        script = result["script"]
        script["storyboard"] = [
            {
                **script["storyboard"][0],
                "shot_no": 1,
                "time_range": "0-3s",
                "narrative_role": "HOOK",
                "visual_content": "人物站在电梯厅，固定中长景看向电梯门",
                "camera": "正面固定中长景，稳定器后移",
            },
            {
                **script["storyboard"][0],
                "shot_no": 2,
                "time_range": "3-8s",
                "narrative_role": "PROOF",
                "visual_content": "切至侧前方约四十五度，人物展示外套",
                "camera": "侧前方近景，浅景深推近",
            },
        ]
        script["video_generation_brief"] = {
            "schema_version": "production-video-brief-v3-creator-capture",
            "render_profile": "UGC_NATIVE_V1",
            "capture_mode": "CREATOR_SELF_SHOT",
            "production_design": script["production_design"],
            "storyboard": script["storyboard"],
            "product_truth": {
                "product_identity": "米白短款外套",
                "identity_anchors": ["米白短款外套"],
            },
            "voiceover": script["continuous_voiceover"],
        }
        self.item.result_json = json.dumps(result, ensure_ascii=False)

        text = render_video_generation_prompt(item=self.item, duration_seconds=15)

        self.assertIn("【拍摄关系｜CREATOR_SELF_SHOT】", text)
        self.assertIn("创作者本人使用手机前置镜头", text)
        self.assertIn("主要看向自己的手机镜头说话", text)
        self.assertIn("【连续内容段01", text)
        self.assertNotIn("手机机位：", text)
        self.assertNotIn("稳定器后移", text)
        self.assertNotIn("侧前方约四十五度", text)
        self.assertNotIn("浅景深推近", text)

    def test_native_multiclip_prompt_renders_real_capture_units_and_direct_cuts(self):
        result = json.loads(self.item.result_json)
        script = result["script"]
        base = script["storyboard"][0]
        script["storyboard"] = [
            {
                **base,
                "shot_no": index,
                "time_range": time_range,
                "narrative_role": role,
                "visual_content": visual,
                "character_action": action,
            }
            for index, (time_range, role, visual, action) in enumerate(
                [
                    ("0-2.5s", "HOOK", "人物穿好外套看向手机", "开始分享"),
                    ("2.5-6s", "PROOF", "外套前襟和衣长清楚", "自然站立"),
                    ("6-10s", "PROOF", "人物侧身展示版型", "轻微转身"),
                    ("10-15s", "ENDING", "人物拿包准备离开", "拿起随身包"),
                ],
                1,
            )
        ]
        script["capture_rhythm_contract"] = {
            "schema_version": "capture-rhythm-contract-v1",
            "profile": "NATIVE_MULTI_CLIP_V1",
            "capture_unit_count": 3,
            "capture_grammar": "OPENING_TO_PROOF_TO_CONTEXT",
            "edit_style": "NATIVE_HARD_CUT",
        }
        script["video_generation_brief"] = {
            "schema_version": "production-video-brief-v7-native-multiclip",
            "render_profile": "UGC_NATIVE_V2_MULTICLIP",
            "capture_mode": "CREATOR_SELF_SHOT",
            "capture_rhythm_contract": script["capture_rhythm_contract"],
            "production_design": script["production_design"],
            "storyboard": script["storyboard"],
            "product_truth": {
                "product_identity": "米白短款外套",
                "identity_anchors": ["米白短款外套"],
            },
            "voiceover": script["continuous_voiceover"],
        }
        self.item.result_json = json.dumps(result, ensure_ascii=False)

        text = render_video_generation_prompt(item=self.item, duration_seconds=15)

        self.assertIn("【拍摄方式｜UGC_NATIVE_V2_MULTICLIP】", text)
        self.assertEqual(3, text.count("【拍摄片段"))
        self.assertEqual(2, text.count("【直接剪切｜开始另一段独立手机素材】"))
        self.assertIn("不锁死手机位置与景别", text)
        self.assertNotIn("结构只控制内容推进，不代表切换摄影机位", text)
        self.assertNotIn("保持同一创作者、商品、穿搭、场景和手机视角", text)

    def test_explicit_public_multiclip_rebuild_preserves_category_projection(self):
        embedded = {
            "schema_version": "capture-rhythm-contract-v5-structure-visible-clips",
            "profile": "NATIVE_MULTI_CLIP_V1",
            "capture_unit_count": 4,
            "macro_structure": ["HOOK", "PROOF", "PROOF", "ENDING"],
            "structure_unit_roles": ["HOOK", "PROOF", "PROOF", "ENDING"],
            "category_projection": "SCARF_MULTICLIP_V1",
            "category_unit_roles": [
                "WORN_OR_PRODUCT_OPENING",
                "DETAIL_PROOF",
                "DETAIL_OR_WORN_RELATION",
                "WORN_OR_CONTEXT_RESULT",
            ],
            "unit_roles": [
                "WORN_OR_PRODUCT_OPENING",
                "DETAIL_PROOF",
                "DETAIL_OR_WORN_RELATION",
                "WORN_OR_CONTEXT_RESULT",
            ],
            "framing_guidance_by_unit": ["围巾画面1", "围巾画面2", "围巾画面3", "围巾画面4"],
            "category_rollout_contract": {
                "profile": "SCARF_MULTICLIP_V1",
                "female_apparel_unchanged": True,
            },
            "shot_richness_contract": {
                "category_profile": "SCARF_MULTICLIP_V1",
                "category_preferred_visible_clips": 4,
            },
        }
        brief = {"capture_rhythm_contract": embedded}
        storyboard = [
            {"narrative_role": role}
            for role in ("HOOK", "PROOF", "PROOF", "ENDING")
        ]
        with patch.dict(
            os.environ,
            {"ORIGINAL_SCRIPT_CAPTURE_RHYTHM_PROFILE": "native_multiclip_v1"},
            clear=False,
        ):
            rebuilt = _capture_rhythm_contract(
                brief,
                {},
                capture_mode="CREATOR_SELF_SHOT",
                storyboard=storyboard,
            )
        self.assertEqual("SCARF_MULTICLIP_V1", rebuilt["category_projection"])
        self.assertEqual(embedded["unit_roles"], rebuilt["unit_roles"])
        self.assertEqual(
            "SCARF_MULTICLIP_V1",
            rebuilt["shot_richness_contract"]["category_profile"],
        )

    def test_public_rebuild_drops_stale_category_projection_on_count_change(self):
        embedded = {
            "capture_unit_count": 4,
            "category_projection": "SCARF_MULTICLIP_V1",
            "category_unit_roles": ["A", "B", "C", "D"],
            "unit_roles": ["A", "B", "C", "D"],
            "framing_guidance_by_unit": ["1", "2", "3", "4"],
            "category_rollout_contract": {"profile": "SCARF_MULTICLIP_V1"},
            "shot_richness_contract": {
                "category_profile": "SCARF_MULTICLIP_V1"
            },
        }
        rebuilt = {
            "capture_unit_count": 3,
            "unit_roles": ["HOOK", "PROOF", "ENDING"],
            "shot_richness_contract": {"planned_visible_clips": 3},
        }
        result = _preserve_category_capture_projection(rebuilt, embedded)
        self.assertNotIn("category_rollout_contract", result)
        self.assertNotIn("category_projection", result)
        self.assertEqual(["HOOK", "PROOF", "ENDING"], result["unit_roles"])

    def test_stage0_prompt_keeps_real_clip_framing_and_phone_relationship(self):
        macro_passages = [
            {
                "visible_process": "人物已经穿好外套，完整造型从首帧清楚建立。",
                "observable_action": "人物站在座位旁自然开始分享。",
                "camera_observation": "固定手机从正面偏45度观察头部至膝上的完整造型。",
            },
            {
                "visible_process": "肩部至腰胯侧前方近景看清宽松袖型和口袋。",
                "observable_action": "人物维持自然说话状态，手臂放松。",
                "camera_observation": "侧前方近景观察外套结构。",
            },
            {
                "visible_process": "人物坐入窗边座位，以隔桌关系呈现整套穿搭。",
                "observable_action": "人物自然完成分享。",
                "camera_observation": "手机固定在对面桌边，以朋友交谈距离观察三分之二身。",
            },
        ]
        unique_clips = [
            {
                "shot_content": "完整造型入画",
                "observable_action": "自然开始分享",
                "framing": "头部至膝上的中全景",
                "recording_relation": "手机固定在窗台旁，人物正面偏45度面对手机",
                "anchor_reference": "外套完整轮廓",
            },
            {
                "shot_content": "外套结构近景",
                "observable_action": "手臂自然放松",
                "framing": "肩部至腰胯的侧前方近景",
                "recording_relation": "沿用同一位置独立重录，人物从片段开始站得更近",
                "anchor_reference": "口袋和短款下摆",
            },
            {
                "shot_content": "隔桌关系呈现穿搭",
                "observable_action": "坐下自然完成分享",
                "framing": "普通手机三分之二身景别",
                "recording_relation": "手机移到对面桌边固定，形成隔桌交谈关系",
                "anchor_reference": "外套与基础穿搭关系",
            },
        ]
        storyboard = []
        units = []
        shot_no = 1
        for unit_index, clip in enumerate(unique_clips, 1):
            unit_id = f"CU_{unit_index:02d}"
            numbers = []
            for duplicate_index in range(2):
                numbers.append(shot_no)
                storyboard.append(
                    {
                        **clip,
                        "shot_no": shot_no,
                        "duration": f"{shot_no - 1}-{shot_no}s",
                        "capture_unit_id": unit_id,
                        "structure_role": "HOOK" if unit_index == 1 else "PROOF",
                        "starts_new_take": duplicate_index == 0,
                        "gaze_and_reaction": "自然看向自己的手机",
                    }
                )
                shot_no += 1
            units.append(
                {
                    "capture_unit_id": unit_id,
                    "shot_numbers": numbers,
                    "structure_role": "HOOK" if unit_index == 1 else "PROOF",
                    "framing_guidance": "通用兜底构图",
                }
            )
        script = {
            "creative_blueprint": {"macro_visual_passages": macro_passages},
            "continuous_voiceover": {"target_language": "ข้อความภาษาไทย"},
            "storyboard": storyboard,
            "capture_units": units,
            "video_generation_brief": {
                "production_design": {
                    "character_setting": {
                        "identity": "曼谷日常穿搭创作者",
                        "appearance": "自然肤质",
                        "hair_makeup": "轻妆长发",
                    },
                    "outfit_setting": {"styling": "白色内搭、牛仔裤和黑色短外套"},
                    "scene_setting": {
                        "location": "咖啡厅窗边",
                        "moment": "白天短暂停留",
                        "lighting": "现场自然光",
                        "background": "座位和过道纵深",
                    },
                },
                "storyboard": storyboard,
                "capture_units": units,
                "capture_rhythm_contract": {"camera_setup_count": 2},
                "product_identity_lock": {
                    "must_preserve": ["黑色短外套", "五颗前襟扣"],
                    "must_not_change": ["禁止双排扣"],
                },
                "voiceover": {"target_language": "ข้อความภาษาไทย"},
            },
        }

        text = render_stage0_video_generation_prompt(script=script)

        self.assertEqual(3, text.count("【拍摄片段"))
        self.assertEqual(2, text.count("【直接剪切｜开始另一段独立手机素材】"))
        self.assertIn("【拍摄片段01｜0-2s｜HOOK】", text)
        self.assertIn("头部至膝上的中全景", text)
        self.assertIn("肩部至腰胯的侧前方近景", text)
        self.assertIn("手机移到对面桌边固定", text)
        self.assertIn("隔桌关系", text)
        self.assertIn("ข้อความภาษาไทย", text)
        self.assertNotIn("supported_claim_keys", text)
        self.assertNotIn("【内部结构槽位", text)
        self.assertNotIn("通用兜底构图", text)

    def test_v3_two_clip_contract_is_upgraded_without_dropping_five_routed_beats(self):
        result = json.loads(self.item.result_json)
        script = result["script"]
        base = script["storyboard"][0]
        script["storyboard"] = [
            {
                **base,
                "shot_no": index,
                "time_range": time_range,
                "narrative_role": role,
                "visual_content": visual,
                "character_action": action,
            }
            for index, (time_range, role, visual, action) in enumerate(
                [
                    ("0-2s", "HOOK", "人物穿好外套进入近景", "看向手机开始分享"),
                    ("2-5s", "PROOF", "外套前襟和衣长清楚", "手指轻触前襟"),
                    ("5-8s", "USE_PROCESS", "人物侧身展示版型", "自然侧身半步"),
                    ("8-11s", "PROOF", "人物看屏幕确认轮廓", "轻微整理包带"),
                    ("11-15s", "ENDING", "商品结果回到清楚半身景", "拿起随身包准备离开"),
                ],
                1,
            )
        ]
        old_contract = {
            "schema_version": "capture-rhythm-contract-v3-scene-feasible-reference",
            "profile": "NATIVE_MULTI_CLIP_V1",
            "capture_unit_count": 2,
            "capture_setup_mode": "ONE_PUBLIC_PHONE_POSITION_PLUS_HANDHELD_CUTAWAY",
            "capture_grammar": "OPENING_TO_CONTEXT",
            "edit_style": "NATIVE_HARD_CUT",
        }
        script["capture_rhythm_contract"] = old_contract
        script["video_generation_brief"] = {
            "schema_version": "production-video-brief-v8-opening-projection",
            "render_profile": "UGC_NATIVE_V2_MULTICLIP",
            "capture_mode": "CREATOR_SELF_SHOT",
            "capture_rhythm_contract": old_contract,
            "production_design": script["production_design"],
            "storyboard": script["storyboard"],
            "product_truth": {
                "product_identity": "米白短款外套",
                "identity_anchors": ["米白短款外套"],
            },
            "voiceover": script["continuous_voiceover"],
        }
        self.item.result_json = json.dumps(result, ensure_ascii=False)

        text = render_video_generation_prompt(item=self.item, duration_seconds=15)

        self.assertEqual(5, text.count("【拍摄片段"))
        self.assertEqual(4, text.count("【直接剪切｜开始另一段独立手机素材】"))
        self.assertIn("实际剪辑目标：5个独立可见片段", text)
        self.assertIn("拿起随身包准备离开", text)
        self.assertEqual(5, text.count("本段相对上一段的新信息："))
        projection = build_production_projection(
            batch=self.batch,
            item=self.item,
        )
        self.assertEqual(
            "capture-rhythm-contract-v5-structure-visible-clips",
            projection["capture_rhythm_schema"],
        )
        self.assertEqual(5, projection["visible_clip_count"])
        self.assertEqual(2, projection["camera_setup_count"])
        self.assertEqual("PRESERVED", projection["shot_richness_status"])
        self.assertEqual("PRESERVED", projection["structure_preservation_status"])
        self.assertEqual(
            ["HOOK", "PROOF", "USE_PROCESS", "PROOF", "ENDING"],
            projection["compiled_function_sequence"],
        )

    def test_v4_flattened_contract_is_upgraded_from_allocated_macro_structure(self):
        result = json.loads(self.item.result_json)
        script = result["script"]
        base = script["storyboard"][0]
        script["allocated_direction"] = {
            "macro_structure": ["HOOK", "PROOF", "USE_PROCESS"]
        }
        script["storyboard"] = [
            {
                **base,
                "shot_no": index,
                "time_range": time_range,
                "narrative_role": role,
                "visual_content": f"画面{index}",
                "character_action": f"动作{index}",
                "capture_unit_id": f"CU_{index:02d}",
                "starts_new_take": True,
            }
            for index, (time_range, role) in enumerate(
                [
                    ("0-3s", "HOOK"),
                    ("3-6s", "PROOF"),
                    ("6-10s", "USE"),
                    ("10-15s", "ENDING"),
                ],
                1,
            )
        ]
        old_contract = {
            "schema_version": "capture-rhythm-contract-v4-shot-richness",
            "profile": "NATIVE_MULTI_CLIP_V1",
            "capture_unit_count": 4,
            "capture_setup_mode": "FIXED_PHONE_MULTI_CLIP",
            "shot_richness_contract": {
                "planned_visible_clips": 4,
                "minimum_visible_clips": 3,
            },
        }
        script["capture_rhythm_contract"] = old_contract
        script["video_generation_brief"] = {
            "schema_version": "production-video-brief-v9-shot-richness",
            "render_profile": "UGC_NATIVE_V2_MULTICLIP",
            "capture_mode": "CREATOR_SELF_SHOT",
            "capture_rhythm_contract": old_contract,
            "production_design": script["production_design"],
            "storyboard": script["storyboard"],
            "product_truth": {
                "product_identity": "米白短款外套",
                "identity_anchors": ["米白短款外套"],
            },
            "voiceover": script["continuous_voiceover"],
        }
        self.item.result_json = json.dumps(result, ensure_ascii=False)

        text = render_video_generation_prompt(item=self.item, duration_seconds=15)
        projection = build_production_projection(batch=self.batch, item=self.item)

        self.assertIn("【拍摄片段04｜10-15s｜USE_PROCESS】", text)
        self.assertNotIn("【拍摄片段04｜10-15s｜ENDING】", text)
        self.assertEqual(
            ["HOOK", "PROOF", "PROOF", "USE_PROCESS"],
            projection["compiled_function_sequence"],
        )
        self.assertEqual("PRESERVED", projection["structure_preservation_status"])

    def test_four_clip_small_accessory_owns_core_motion_only_once(self):
        units = [
            {"capture_unit_id": f"CU_{index:02d}"}
            for index in range(1, 5)
        ]
        projected = _apply_small_accessory_capture_projection(
            units,
            accessory_brief={
                "product_prominence_contract": {
                    "sequence_policy": (
                        "PRODUCT_OPENING_TO_MOTION_TO_PRODUCT_RETURN"
                    )
                }
            },
        )

        roles = [unit["unit_role"] for unit in projected]
        self.assertEqual(
            [
                "PRODUCT_RESULT_CLOSE",
                "NATURAL_MOTION_RELATION",
                "PRODUCT_DETAIL_RELATION",
                "PRODUCT_REACQUISITION",
            ],
            roles,
        )
        self.assertEqual(1, roles.count("NATURAL_MOTION_RELATION"))
        self.assertIn("不重复上一段核心动作", projected[2]["framing_guidance"])

    def test_small_accessory_prompt_projects_motion_and_product_visible_ending(self):
        result = json.loads(self.item.result_json)
        script = result["script"]
        base = script["storyboard"][0]
        script["storyboard"] = [
            {
                **base,
                "shot_no": index,
                "time_range": time_range,
                "narrative_role": role,
                "visual_content": visual,
                "character_action": action,
                "natural_emotion": reaction,
            }
            for index, (time_range, role, visual, action, reaction) in enumerate(
                [
                    ("0-3s", "HOOK", "发夹已经佩戴完成", "看向手机", "刚注意到效果"),
                    ("3-9s", "PROOF", "人物侧身展示发型", "轻微转头", "自然满意"),
                    ("9-15s", "ENDING", "人物走向门口", "拿包离开", "准备出门"),
                ],
                1,
            )
        ]
        script["capture_rhythm_contract"] = {
            "schema_version": "capture-rhythm-contract-v1",
            "profile": "NATIVE_MULTI_CLIP_V1",
            "capture_unit_count": 3,
            "capture_grammar": "OPENING_TO_PROOF_TO_CONTEXT",
            "edit_style": "NATIVE_HARD_CUT",
        }
        category_extension = {
            "domain": "ACCESSORY",
            "schema_version": "accessory-execution-profile-v2",
            "product_type_source": {
                "canonical_type": "claw_clip",
                "display_type": "抓夹",
            },
            "profile": {
                "product_subtype": "claw_clip",
                "wearing_zone": "HAIR",
                "required_result_view": "ALREADY_STYLED_HAIR_RESULT",
            },
        }
        script["video_generation_brief"] = {
            "schema_version": "production-video-brief-v7-native-multiclip",
            "render_profile": "UGC_NATIVE_V2_MULTICLIP",
            "capture_mode": "CREATOR_SELF_SHOT",
            "capture_rhythm_contract": script["capture_rhythm_contract"],
            "production_design": script["production_design"],
            "storyboard": script["storyboard"],
            "action_design": {
                "start_state": "发饰已经佩戴完成",
                "core_action": "完成一次自然上半身角度变化",
                "end_state": "回到发饰无遮挡的侧后方近景",
                "supporting_scene_action": "拿包离开",
                "motion_scope": "ONE_CONTINUOUS_CHANGE",
            },
            "category_execution_extension": category_extension,
            "accessory_execution_brief": {
                "schema_version": "accessory-video-handoff-v4-small-prominence",
                "product_prominence_contract": {
                    "primary_framing": "HAIR_REGION_CLOSE"
                },
            },
            "product_truth": {
                "product_identity": "棕色抓夹",
                "identity_anchors": ["棕色抓夹"],
            },
            "voiceover": script["continuous_voiceover"],
        }
        self.item.result_json = json.dumps(result, ensure_ascii=False)

        text = render_video_generation_prompt(item=self.item, duration_seconds=15)

        self.assertIn("PRODUCT_RESULT_CLOSE", text)
        self.assertIn("NATURAL_MOTION_RELATION", text)
        self.assertIn("PRODUCT_REACQUISITION", text)
        self.assertIn("视线关系：", text)
        self.assertIn("自然反应：", text)
        self.assertIn("发饰", text)
        self.assertIn("人物动作：完成一次自然上半身角度变化", text)
        self.assertIn("不先静止等待再开始", text)
        self.assertIn("动作延续到片段末尾，只在最后一瞬自然收住", text)
        self.assertIn("最后一段不离场，结尾服从商品回收近景", text)
        self.assertNotIn("人物走向门口；", text)

    def test_legacy_profile_remains_available_for_rollback(self):
        with patch.dict("os.environ", {"ORIGINAL_SCRIPT_VIDEO_PROMPT_PROFILE": "legacy"}):
            text = render_video_generation_prompt(item=self.item, duration_seconds=15)
        self.assertIn("【人物与穿搭】", text)
        self.assertIn("人物状态：", text)
        self.assertNotIn("【拍摄方式｜UGC_NATIVE_V1】", text)

    def test_capture_rhythm_environment_can_upgrade_an_old_stored_script(self):
        with patch.dict(
            "os.environ",
            {"ORIGINAL_SCRIPT_CAPTURE_RHYTHM_PROFILE": "native_multiclip_v1"},
        ):
            text = render_video_generation_prompt(item=self.item, duration_seconds=15)
        self.assertIn("【拍摄方式｜UGC_NATIVE_V2_MULTICLIP】", text)
        self.assertIn("【拍摄节奏｜NATIVE_MULTI_CLIP_V1】", text)

    def test_visual_execution_v2_keeps_native_texture_and_styling_completion(self):
        result = json.loads(self.item.result_json)
        script = result["script"]
        script["production_design"]["life_event"] = {
            "continuous_event": "在同一窗边座位拿起随身包准备离开"
        }
        contract = {
            "schema_version": "visual-execution-contract-v2",
            "visual_finish_profile": "NATIVE_STYLED",
            "styling_context": {
                "finish_direction": "城市休闲造型完整但保留真实穿着质感",
                "supporting_elements": "一只日常肩包",
                "grooming_direction": "自然有气色的妆发",
            },
            "scene_context": {
                "situation_tags": ["WORK_BREAK"],
                "aesthetic_anchors": ["暖木与窗边自然侧光"],
                "visual_scene_recipe": {
                    "space_relationship": "咖啡厅靠窗座位与局部桌面纵深",
                    "material_palette": "暖木桌面与透明玻璃杯",
                    "lighting_texture": "柔和窗边自然光",
                    "lived_in_detail": "桌边随手放下的小包",
                },
                "instruction": "同一真实场景来源，不混拼布景",
            },
        }
        script["video_generation_brief"]["visual_execution_contract"] = contract
        script["video_generation_brief"]["production_design"] = script["production_design"]
        self.item.result_json = json.dumps(result, ensure_ascii=False)

        text = render_video_generation_prompt(item=self.item, duration_seconds=15)

        self.assertIn("【视觉完成度｜NATIVE_STYLED】", text)
        self.assertIn("城市休闲造型完整", text)
        self.assertIn("暖木与窗边自然侧光", text)
        self.assertIn("场景来源配方（软参考）", text)
        self.assertIn("材质与色调：暖木桌面与透明玻璃杯", text)
        self.assertNotIn("WORK_BREAK", text)
        self.assertIn("连续生活事件：在同一窗边座位拿起随身包准备离开", text)
        self.assertIn("人物造型、真实场景和手机原生质感同时完整成立", text)
        self.assertNotIn("商品一致性优先于人物美感", text)

    def test_visual_execution_v3_renders_compact_saliency_guidance(self):
        result = json.loads(self.item.result_json)
        script = result["script"]
        contract = {
            "schema_version": "visual-execution-contract-v3-saliency",
            "visual_finish_profile": "NATIVE_STYLED",
            "styling_context": {"finish_direction": "真实但完成度清楚的出门造型"},
            "scene_context": {"visual_scene_recipe": {}},
            "visual_saliency": {
                "exposure": {
                    "profile": "BRIGHT_NATIVE",
                    "guidance": "人物脸部与商品处于画面主要亮部，画面不欠曝、不蒙灰。",
                },
                "separation": {
                    "outfit_guidance": "内搭与深色商品保持清楚明度边界。",
                    "background_guidance": "背景与商品保持明暗分离。",
                },
                "opening_focus": {
                    "guidance": "首镜让丝巾成为第一色彩焦点。",
                    "natural_change": "人物轻微转向主要亮部，不重新系结。",
                },
            },
        }
        script["video_generation_brief"] = {
            "production_design": script["production_design"],
            "storyboard": script["storyboard"],
            "product_truth": {
                "canonical_product_type": "silk_scarf",
                "identity_anchors": ["深蓝底色与方形轮廓"],
            },
            "visual_execution_contract": contract,
            "voiceover": script["continuous_voiceover"],
        }
        self.item.result_json = json.dumps(result, ensure_ascii=False)

        text = render_video_generation_prompt(item=self.item, duration_seconds=15)

        self.assertIn("【画面优先级｜明亮原生与商品分离】", text)
        self.assertIn("画面不欠曝、不蒙灰", text)
        self.assertIn("内搭与深色商品保持清楚明度边界", text)
        self.assertIn("首镜让丝巾成为第一色彩焦点", text)
        self.assertIn("不重新系结", text)
        self.assertIn("参考图的整体曝光、滤镜、背景色调", text)
        self.assertEqual(text.count("【画面优先级｜明亮原生与商品分离】"), 1)

    def test_projection_contains_readable_and_lineage_fields(self):
        row = build_production_projection(batch=self.batch, item=self.item)
        self.assertEqual(row["script_id"], "SCSCRIPT_1")
        self.assertEqual(row["core_selling_point"], "腿部线条视觉更修长")
        self.assertEqual(row["cluster_id"], 2)
        self.assertIn("【人物设定】", row["complete_script"])

    def test_projection_creative_signature_includes_frozen_action_signature(self):
        result = json.loads(self.item.result_json)
        result["script"]["video_generation_brief"]["action_design"] = {
            "action_signature": "ACT_TEST123",
            "primary_action_mode": "DETAIL_SHOW",
        }
        self.item.result_json = json.dumps(result, ensure_ascii=False)

        row = build_production_projection(batch=self.batch, item=self.item)

        self.assertEqual(
            row["creative_signature"],
            "人|展览|走入|前行|ACT_TEST123",
        )

    def test_respectful_argument_uses_safe_voiceover_summary_everywhere(self):
        result = json.loads(self.item.result_json)
        result["script"]["continuous_voiceover"][
            "selling_argument_realization_zh"
        ] = "穿上后整体更精致、更有气质"
        self.item.content_bundle_json = json.dumps(
            {
                "selling_argument": {
                    "core_value": "农村妇女穿上这件衣服都能拥有富婆的气质",
                    "expression_policy": "SEMANTIC_AUTHORITY_NOT_VERBATIM",
                    "respectful_reframe_required": True,
                }
            },
            ensure_ascii=False,
        )
        self.item.result_json = json.dumps(result, ensure_ascii=False)

        row = build_production_projection(batch=self.batch, item=self.item)

        self.assertEqual(row["core_selling_point"], "穿上后整体更精致、更有气质")
        self.assertIn("穿上后整体更精致、更有气质", row["script_title"])
        self.assertNotIn("农村妇女", row["script_title"])
        self.assertNotIn("农村妇女", row["complete_script"])

    def test_legacy_sensitive_argument_never_falls_back_to_raw_wording(self):
        self.item.content_bundle_json = json.dumps(
            {
                "selling_argument": {
                    "core_value": "农村妇女穿上这件衣服都能拥有富婆的气质",
                    "expression_policy": "SEMANTIC_AUTHORITY_NOT_VERBATIM",
                    "respectful_reframe_required": True,
                }
            },
            ensure_ascii=False,
        )
        row = build_production_projection(batch=self.batch, item=self.item)

        self.assertEqual(row["core_selling_point"], "已按自然口播表达")
        self.assertNotIn("农村妇女", row["script_title"])
        self.assertNotIn("农村妇女", row["complete_script"])


class SemanticMainlinePromptTest(unittest.TestCase):
    """「整片语义主线」两行各自判定：缩窄后只剩一行时不能整块丢掉。

    合同把并列场景收窄到实际冻结的那一个后，"主消费情境"可能被清空而"核心购买
    理由"仍有内容（反之亦然）。用前者当整块的门，会把后者一起带走 —— 素材的
    收窄就变成了素材的消失。
    """

    def _script(self, semantic_context):
        storyboard = [
            {
                "shot_no": index,
                "duration": f"{index - 1}-{index}s",
                "capture_unit_id": f"CU_{index:02d}",
                "structure_role": "HOOK" if index == 1 else "PROOF",
                "starts_new_take": True,
                "visual_content": "人物自然看向镜头",
                "framing_guidance": "胸部以上的近景",
            }
            for index in (1, 2)
        ]
        units = [
            {
                "capture_unit_id": f"CU_{index:02d}",
                "shot_numbers": [index],
                "structure_role": "HOOK" if index == 1 else "PROOF",
                "framing_guidance": "胸部以上的近景",
            }
            for index in (1, 2)
        ]
        return {
            "creative_blueprint": {"macro_visual_passages": []},
            "continuous_voiceover": {"target_language": "马来语"},
            "storyboard": storyboard,
            "capture_units": units,
            "video_generation_brief": {
                "production_design": {"capture_mode": "CREATOR_SELF_SHOT"},
                "semantic_context": semantic_context,
            },
        }

    def test_both_lines_render_when_present(self):
        text = render_stage0_video_generation_prompt(
            script=self._script(
                {"primary_narrative_context": "上班", "core_buying_reason": "上班"}
            )
        )
        self.assertIn("【整片语义主线｜不做逐句逐镜绑定】", text)
        self.assertIn("主消费情境：上班", text)
        self.assertIn("核心购买理由：上班", text)

    def test_the_reason_survives_an_empty_context_line(self):
        text = render_stage0_video_generation_prompt(
            script=self._script(
                {"primary_narrative_context": "", "core_buying_reason": "上班"}
            )
        )
        self.assertIn("【整片语义主线｜不做逐句逐镜绑定】", text)
        self.assertIn("核心购买理由：上班", text)
        self.assertNotIn("主消费情境：", text)

    def test_the_context_survives_an_empty_reason_line(self):
        text = render_stage0_video_generation_prompt(
            script=self._script(
                {"primary_narrative_context": "上班", "core_buying_reason": ""}
            )
        )
        self.assertIn("主消费情境：上班", text)
        self.assertNotIn("核心购买理由：", text)

    def test_the_block_is_absent_when_both_lines_are_empty(self):
        text = render_stage0_video_generation_prompt(
            script=self._script(
                {"primary_narrative_context": "", "core_buying_reason": ""}
            )
        )
        self.assertNotIn("【整片语义主线", text)


class FaceFreeConstraintTest(unittest.TestCase):
    """The mixed template's NO_FACE guarantee must be stated in the prompt.

    The per-shot framing prose implies it, but without an explicit constraint a
    video model is free to fall back to the accessory genre's habitual wearer
    close-up -- the exact failure this profile exists to prevent.
    """

    NO_FACE_CONTRACT = {
        "execution_profile": "ACCESSORY_MIXED_TEMPLATE_V1",
        "face_policy": "NO_FACE",
        "capture_units": [
            {
                "unit_id": "CU_01",
                "module": "WORN_DETAIL",
                "carrier_mode": "WEARER_ACTIVE",
                "face_policy": "NO_FACE",
                "forbidden_framing": [
                    "眼睛入画",
                    "鼻子入画",
                    "嘴部入画",
                    "正面全脸",
                    "镜面反射露脸",
                ],
                "allowed_framing": ["耳廓与耳垂近景", "耳侧与颈侧关系", "少量下颌边缘"],
            }
        ],
    }

    def _item(self, contract):
        script = {
            "production_design": {
                "presentation_mode": "PERSON_ON_CAMERA",
                "capture_mode": "CREATOR_SELF_SHOT",
            },
            "video_generation_brief": {
                "render_profile": "ugc_native_v1",
                "category_execution_extension": {"mixed_template_contract": contract},
                "storyboard": [
                    {
                        "shot_no": 1,
                        "time_range": "0-3s",
                        "visual_content": "耳饰已经佩戴，耳廓与耳垂局部近景",
                        "character_action": "保持自然小幅呼吸",
                        "camera": "耳廓与耳垂局部近景，固定机位",
                    }
                ],
                "capture_units": [
                    {
                        "capture_unit_id": "CU_01",
                        "shot_numbers": [1],
                        "time_range": "0-3s",
                    }
                ],
            },
        }
        return SimpleNamespace(
            result_json=json.dumps({"script": script}, ensure_ascii=False),
            content_bundle_json="",
        )

    def test_no_face_contract_is_stated_in_the_prompt(self):
        rendered = render_video_generation_prompt(
            item=self._item(self.NO_FACE_CONTRACT), duration_seconds=15
        )
        self.assertIn("【全片不露脸｜硬约束】", rendered)
        self.assertIn("正面全脸", rendered)
        self.assertIn("耳廓与耳垂近景", rendered)

    def test_absent_contract_adds_no_constraint(self):
        rendered = render_video_generation_prompt(
            item=self._item({}), duration_seconds=15
        )
        self.assertNotIn("【全片不露脸｜硬约束】", rendered)

    def test_non_no_face_policy_adds_no_constraint(self):
        contract = dict(self.NO_FACE_CONTRACT, face_policy="FACE_ALLOWED")
        rendered = render_video_generation_prompt(
            item=self._item(contract), duration_seconds=15
        )
        self.assertNotIn("【全片不露脸｜硬约束】", rendered)


class RenderValidationDeliveryTest(unittest.TestCase):
    """开发包 A：执行校验的交付语义。

    规则本身是结构的（提示词与冻结合同逐镜比对），所以"确定的冲突"必须在
    最终交接处被拦下；但拦截**不得**走 ``SCRIPT_FAILED`` —— 那是 ``resume``
    会重试的状态，会把一个可离线修复的渲染缺陷变成一次付费重生成。
    """

    def test_only_fail_blocks_delivery(self):
        for status in ("PASS", "NOT_APPLICABLE", "AUDIT_ERROR", "", None, "FAILED"):
            self.assertFalse(
                render_validation_blocks_delivery({"status": status}),
                f"{status!r} 不应拦交付",
            )
        self.assertFalse(render_validation_blocks_delivery(None))
        self.assertFalse(render_validation_blocks_delivery({}))
        self.assertTrue(render_validation_blocks_delivery({"status": "FAIL"}))

    def test_block_reason_names_the_shot_and_source(self):
        reason = render_validation_block_reason(
            {
                "status": "FAIL",
                "issues": [
                    {
                        "shot_id": 2,
                        "module": "HANDHELD_PRODUCT",
                        "code": "MIXED_EXECUTION_ACTION_BOUNDARY",
                        "source": "RENDERER",
                    }
                ],
            }
        )
        self.assertIn("镜2", reason)
        self.assertIn("HANDHELD_PRODUCT", reason)
        self.assertIn("MIXED_EXECUTION_ACTION_BOUNDARY", reason)
        self.assertIn("RENDERER", reason)

    def test_block_reason_is_empty_for_non_conflict_verdicts(self):
        """相似性不确定（NEEDS_REVIEW）不是执行冲突，不得混用同一措辞。"""

        self.assertEqual("", render_validation_block_reason({"status": "NEEDS_REVIEW"}))
        self.assertEqual("", render_validation_block_reason({"status": "PASS"}))
        self.assertEqual("", render_validation_block_reason(None))

    def test_result_proxy_audits_the_pending_result(self):
        """审计必须针对**即将落盘**的稿，而不是 item 上残留的旧结果。"""

        stale = SimpleNamespace(
            result_json=json.dumps({"stale": True}), script_id="SCRIPT_1"
        )
        proxy = _ResultProxy(stale, {"status": "SUCCESS", "script": {}})

        self.assertEqual({"status": "SUCCESS", "script": {}}, load_item_result(proxy))
        # 其余属性透传，无需维护一份会失效的字段清单
        self.assertEqual("SCRIPT_1", proxy.script_id)
        # 缺失属性仍走 getattr 默认值（renderer 大量使用该形式）
        self.assertEqual("", getattr(proxy, "frozen_direction_package_json", ""))
        # 原对象未被改动
        self.assertEqual({"stale": True}, load_item_result(stale))

    def test_attach_skips_non_mixed_and_failed_results(self):
        item = SimpleNamespace(result_json="", script_id="SCRIPT_1")
        # 非混合模式：返回空，旧流程的 result_json 逐字不变
        self.assertEqual(
            {},
            attach_render_validation(
                item=item,
                result={"status": "SUCCESS", "script": {}},
                duration_seconds=15,
            ),
        )
        # 未成功的结果不审
        self.assertEqual(
            {},
            attach_render_validation(
                item=item, result={"status": "FAILED"}, duration_seconds=15
            ),
        )
        self.assertEqual(
            {},
            attach_render_validation(
                item=item, result={"status": "SUCCESS"}, duration_seconds=15
            ),
        )

    def test_attach_reports_audit_error_instead_of_raising(self):
        """记账失败不得把一条已完成的脚本变成失败稿。"""

        item = SimpleNamespace(result_json="", script_id="SCRIPT_1")
        with patch.object(
            renderer,
            "_frozen_mixed_contract_for_item",
            side_effect=RuntimeError("boom"),
        ):
            self.assertEqual(
                {},
                attach_render_validation(
                    item=item,
                    result={"status": "SUCCESS", "script": {}},
                    duration_seconds=15,
                ),
            )

        with patch.object(
            renderer,
            "_frozen_mixed_contract_for_item",
            return_value={"execution_profile": "ACCESSORY_MIXED_TEMPLATE"},
        ), patch.object(
            renderer,
            "render_video_generation_prompt_checked",
            side_effect=RuntimeError("boom"),
        ):
            payload = attach_render_validation(
                item=item,
                result={"status": "SUCCESS", "script": {}},
                duration_seconds=15,
            )
        validation = payload["render_validation"]
        self.assertEqual("AUDIT_ERROR", validation["status"])
        self.assertIn("boom", validation["reason"])
        # 关键：AUDIT_ERROR 不拦交付
        self.assertFalse(render_validation_blocks_delivery(validation))

    def test_attach_binds_the_validation_to_the_delivered_prompt(self):
        item = SimpleNamespace(result_json="", script_id="SCRIPT_1")
        with patch.object(
            renderer,
            "_frozen_mixed_contract_for_item",
            return_value={"execution_profile": "ACCESSORY_MIXED_TEMPLATE"},
        ), patch.object(
            renderer,
            "render_video_generation_prompt_checked",
            return_value={
                "text": "prompt-body",
                "report": SimpleNamespace(ok=True, over_limit=True),
                "render_validation": {
                    "status": "PASS",
                    "prompt_hash": "hash-1",
                    "renderer_version": SCRIPT_RENDERER_VERSION,
                },
            },
        ):
            payload = attach_render_validation(
                item=item,
                result={"status": "SUCCESS", "script": {}},
                duration_seconds=15,
            )
        validation = payload["render_validation"]
        self.assertEqual("PASS", validation["status"])
        self.assertEqual("hash-1", validation["prompt_hash"])
        self.assertEqual(SCRIPT_RENDERER_VERSION, validation["renderer_version"])
        self.assertEqual(len("prompt-body"), validation["prompt_chars"])
        # 超内部预算只报告，不据此判失败、不截断
        self.assertTrue(validation["over_limit"])
        self.assertTrue(validation["compaction_ok"])


if __name__ == "__main__":
    unittest.main()
