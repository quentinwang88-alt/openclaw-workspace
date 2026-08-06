import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from core.production_script_renderer import (
    build_production_projection,
    render_complete_production_script,
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

    def test_legacy_profile_remains_available_for_rollback(self):
        with patch.dict("os.environ", {"ORIGINAL_SCRIPT_VIDEO_PROMPT_PROFILE": "legacy"}):
            text = render_video_generation_prompt(item=self.item, duration_seconds=15)
        self.assertIn("【人物与穿搭】", text)
        self.assertIn("人物状态：", text)
        self.assertNotIn("【拍摄方式｜UGC_NATIVE_V1】", text)

    def test_projection_contains_readable_and_lineage_fields(self):
        row = build_production_projection(batch=self.batch, item=self.item)
        self.assertEqual(row["script_id"], "SCSCRIPT_1")
        self.assertEqual(row["core_selling_point"], "腿部线条视觉更修长")
        self.assertEqual(row["cluster_id"], 2)
        self.assertIn("【人物设定】", row["complete_script"])

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


if __name__ == "__main__":
    unittest.main()
