import tempfile
import unittest
import sqlite3
from pathlib import Path
from unittest.mock import patch

from core.first_frame_contract import (
    build_first_frame_contract,
    render_first_frame_prompt,
)
from scripts.run_first_frame_tasks import _download_references
from core.first_frame_storage import FirstFrameStorage


def _script():
    persona = {
        "availability": "AVAILABLE",
        "persona_id": "P1",
        "persona_name": "泰国自然分享女生",
        "template_version": "V2",
        "reference_images": [{"file_token": "person_ref"}],
        "reference_strategy": "PERSONA_PRODUCT_COMPOSITE_PREFERRED",
        "identity_lock": {"body_proportion_text": "上身4下身6，头身比约1:7.2"},
        "script_projection": {
            "identity": "泰国年轻女性",
            "appearance": "自然肤质",
            "hair_makeup": "自然长发、淡妆",
        },
    }
    return {
        "production_design": {},
        "allocated_direction": {"opening_visual_job": {"job": "SHOW_RESULT"}},
        "video_generation_brief": {
            "production_design": {
                "presentation_mode": "PERSON_ON_CAMERA",
                "capture_mode": "CREATOR_SELF_SHOT",
                "scene": {
                    "location": "书店出口",
                    "lighting": "普通明亮室内光",
                    "background": "密集书架、图书陈列台和出口文字牌",
                    "lived_in_trace": "桌边收据和待归还书籍",
                },
            },
            "persona_selection_contract": persona,
            "outfit_selection_contract": {
                "template_id": "STYLE_1",
                "template_version": "V3",
                "target_role": "TARGET_GARMENT",
                "outfit_recipe": {"top": "白色背心", "bottom": "直筒牛仔裤"},
            },
            "outfit_prompt_projection": {"frozen_outfit": "白色背心；直筒牛仔裤"},
            "product_truth": {
                "identity_anchors": ["棕色短款外套", "单排金色圆扣"],
                "display_quantity_contract": {},
            },
            "product_identity_lock": {
                "must_preserve": ["棕色短款外套", "单排金色圆扣"],
                "must_not_change": ["禁止改成双排扣"],
            },
            "visual_execution_contract": {
                "schema_version": "visual-execution-contract-v4-wearable-saliency",
                "visual_saliency": {
                    "exposure": {"guidance": "脸部与外套在主要亮部"},
                    "separation": {
                        "outfit_guidance": "内搭与外套保持清楚边界",
                        "background_guidance": "书架只放侧边或远处",
                    },
                    "opening_focus": {"guidance": "目标外套先成为第一视觉焦点"},
                },
            },
            "storyboard": [{
                "visual_content": "人物穿好外套面对手机",
                "character_action": "正准备离开",
                "natural_emotion": "平静自然",
                "camera": "手机固定中景",
                "product_anchors_visible": ["单排金色圆扣"],
            }],
        },
    }


class FirstFrameContractTest(unittest.TestCase):
    def test_local_persona_reference_needs_no_feishu_download(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "persona.png"
            source.write_bytes(b"persona-image")

            class NoDownloadClient:
                def download_attachment_bytes(self, _asset):
                    raise AssertionError("local reference should not call Feishu")

            paths = _download_references(
                NoDownloadClient(),
                [{"local_path": str(source), "name": "persona.png"}],
                Path(tmp),
                cache_dir=Path(tmp) / "cache",
            )
            self.assertEqual(len(paths), 1)
            self.assertEqual(Path(paths[0]).read_bytes(), b"persona-image")

    def test_stage0_setting_aliases_are_preserved(self):
        script = {
            "video_generation_brief": {
                "production_design": {
                    "presentation_mode": "PERSON_ON_CAMERA",
                    "character_setting": {
                        "identity": "曼谷年轻通勤女性",
                        "appearance": "自然真实",
                        "hair_makeup": "自然棕色长发",
                        "age_presence": "自然成年女性比例",
                    },
                    "scene_setting": {
                        "location": "咖啡厅窗边",
                        "moment": "下午",
                        "lighting": "自然光",
                        "background": "普通座椅",
                    },
                    "outfit_setting": {
                        "styling": "白色T恤、浅蓝牛仔裤与近黑短外搭",
                        "visibility_note": "完整穿搭清楚可见",
                    },
                },
                "product_identity_lock": {"must_preserve": ["近黑短外搭"]},
                "storyboard": [
                    {
                        "shot_content": "外搭平整放在长凳上，全段无人无手",
                        "observable_action": "商品保持静止",
                        "framing": "固定竖屏近景",
                        "carrier_mode": "STATIC_PRODUCT",
                        "anchor_reference": "圆领与前襟",
                    }
                ],
            }
        }
        contract = build_first_frame_contract(
            script_id="SCSCRIPT_STAGE0_744_S1",
            product_code="1734257377321977850",
            product_images=[{"file_token": "product"}],
            script=script,
        )
        self.assertEqual(contract["availability"], "AVAILABLE")
        self.assertEqual(contract["scene_contract"]["location"], "咖啡厅窗边")
        self.assertEqual(
            contract["outfit_prompt_projection"]["frozen_outfit"],
            "白色T恤、浅蓝牛仔裤与近黑短外搭",
        )
        self.assertEqual(
            contract["persona_contract"]["reference_strategy"],
            "FROZEN_SCRIPT_TEXT_ONLY",
        )
        self.assertEqual(
            contract["body_proportion_authority"]["guidance"],
            "自然成年女性比例",
        )
        self.assertEqual(
            contract["opening_contract"]["visual_content"],
            "外搭平整放在长凳上，全段无人无手",
        )
        self.assertEqual(contract["opening_contract"]["carrier_mode"], "STATIC_PRODUCT")
        prompt = render_first_frame_prompt(contract)
        self.assertIn("首帧承载方式：纯商品静物", prompt)
        self.assertIn("不得出现人物、脸、身体、穿搭、手或手臂", prompt)

    def test_contract_and_prompt_keep_reference_authorities_separate(self):
        contract = build_first_frame_contract(
            script_id="S1", product_code="1730000000000000000",
            product_images=[{"file_token": "product_ref"}], script=_script(),
        )
        self.assertEqual(contract["availability"], "AVAILABLE")
        self.assertEqual(
            contract["reference_order"], ["PRODUCT_IDENTITY", "PERSONA_IDENTITY"]
        )
        prompt = render_first_frame_prompt(contract)
        self.assertIn("商品参考图只决定目标商品", prompt)
        self.assertIn("人物参考图只决定同一人物", prompt)
        self.assertIn("禁止改成双排扣", prompt)
        self.assertIn("白色背心；直筒牛仔裤", prompt)
        self.assertIn("书店出口", prompt)
        self.assertIn("上身4下身6", prompt)
        self.assertIn("人物参考图不控制头身比", prompt)
        self.assertIn("至少覆盖头部至膝部", prompt)
        self.assertIn("不要尺寸数字、尺寸线、测量箭头、尺码表、规格标签", prompt)
        self.assertIn("参考图的裁切、镜头距离与头部画面占比不代表身体比例", prompt)
        self.assertIn("书架、货架、文字和陈列只放画面侧边或远处", prompt)
        self.assertNotIn("桌边收据和待归还书籍", prompt)

    def test_legacy_dress_recipe_is_rendered_as_one_piece(self):
        script = _script()
        script["video_generation_brief"]["outfit_selection_contract"] = {
            "template_id": "STYLE_DRESS",
            "outfit_recipe": {
                "top": "深蓝条纹波点连衣裙",
                "bottom": "深蓝色",
                "other_accessories": "深咖色鸭舌帽",
            },
        }
        contract = build_first_frame_contract(
            script_id="D1", product_code="1730000000000000000",
            product_images=[{"file_token": "product_ref"}], script=script,
        )
        self.assertEqual("ONE_PIECE", contract["outfit_contract"]["outfit_structure"])
        prompt = render_first_frame_prompt(contract)
        self.assertIn("连体单品：深蓝条纹波点连衣裙", prompt)
        self.assertNotIn("下装：深蓝色", prompt)

    def test_fingerprint_excludes_script_id_but_changes_with_opening(self):
        one = build_first_frame_contract(
            script_id="S1", product_code="1730000000000000000",
            product_images=[{"file_token": "product_ref"}], script=_script(),
        )
        two = build_first_frame_contract(
            script_id="S2", product_code="1730000000000000000",
            product_images=[{"file_token": "product_ref"}], script=_script(),
        )
        self.assertEqual(one["asset_fingerprint"], two["asset_fingerprint"])
        changed = _script()
        changed["video_generation_brief"]["storyboard"][0]["visual_content"] = "人物耳侧近景"
        three = build_first_frame_contract(
            script_id="S3", product_code="1730000000000000000",
            product_images=[{"file_token": "product_ref"}], script=changed,
        )
        self.assertNotEqual(one["asset_fingerprint"], three["asset_fingerprint"])

    def test_person_first_frame_freezes_before_active_speech(self):
        script = _script()
        script["video_generation_brief"]["storyboard"][0][
            "character_action"
        ] = "人物面对手机自然开口分享"
        contract = build_first_frame_contract(
            script_id="S_SPEAK", product_code="1730000000000000000",
            product_images=[{"file_token": "product_ref"}], script=script,
        )
        prompt = render_first_frame_prompt(contract)
        self.assertIn("人物面对手机，刚准备开始分享", prompt)
        self.assertIn("嘴唇自然放松或仅轻微分开", prompt)
        self.assertNotIn("人物面对手机自然开口分享", prompt)

    def test_person_direction_without_reference_is_unavailable(self):
        script = _script()
        script["video_generation_brief"]["persona_selection_contract"]["availability"] = "UNAVAILABLE"
        contract = build_first_frame_contract(
            script_id="S1", product_code="1730000000000000000",
            product_images=[{"file_token": "product_ref"}], script=script,
        )
        self.assertEqual(contract["availability"], "PERSONA_REFERENCE_UNAVAILABLE")

    def test_unbound_frozen_character_uses_text_design_without_persona_reference(self):
        script = _script()
        brief = script["video_generation_brief"]
        brief.pop("persona_selection_contract")
        brief["production_design"]["character"] = {
            "identity": "准备出门的泰国年轻女性创作者",
            "appearance": "自然健康肤色，身形匀称",
            "hair_makeup": "黑色中长发低马尾，轻薄日常妆",
            "speaking_personality": "轻松直接",
        }
        contract = build_first_frame_contract(
            script_id="S_TEXT_PERSONA", product_code="1730000000000000000",
            product_images=[{"file_token": "product_ref"}], script=script,
        )
        self.assertEqual(contract["availability"], "AVAILABLE")
        self.assertEqual(
            contract["persona_contract"]["reference_strategy"],
            "FROZEN_SCRIPT_TEXT_ONLY",
        )
        prompt = render_first_frame_prompt(contract)
        self.assertIn("准备出门的泰国年轻女性创作者", prompt)
        self.assertIn("黑色中长发低马尾", prompt)
        self.assertIn("未绑定人物参考图", prompt)
        self.assertIn("不得从商品参考图复制模特的脸", prompt)

    def test_missing_body_proportion_uses_natural_adult_fallback(self):
        script = _script()
        script["video_generation_brief"]["persona_selection_contract"][
            "identity_lock"
        ]["body_proportion_text"] = ""
        contract = build_first_frame_contract(
            script_id="S_FALLBACK", product_code="1730000000000000000",
            product_images=[{"file_token": "product_ref"}], script=script,
        )
        self.assertEqual(
            "NATURAL_ADULT_FALLBACK",
            contract["body_proportion_authority"]["status"],
        )
        prompt = render_first_frame_prompt(contract)
        self.assertIn("采用自然写实的成年人物比例", prompt)
        self.assertNotIn("服从人物模板参考", prompt)

    def test_old_script_backfills_proportion_for_same_persona_only(self):
        script = _script()
        script["video_generation_brief"]["persona_selection_contract"][
            "identity_lock"
        ]["body_proportion_text"] = ""
        provider = {
            "provider_version": "provider-test",
            "templates": [
                {"persona_id": "OTHER", "body_proportion_text": "错误人物比例"},
                {"persona_id": "P1", "body_proportion_text": "上身4下身6，头身比约1:7.2"},
            ],
        }
        with patch(
            "core.persona_template_provider.load_persona_templates",
            return_value=provider,
        ):
            contract = build_first_frame_contract(
                script_id="S_BACKFILL", product_code="1730000000000000000",
                product_images=[{"file_token": "product_ref"}], script=script,
            )
        self.assertEqual(
            "LATEST_SAME_PERSONA_BACKFILL",
            contract["persona_contract"]["body_proportion_source"],
        )
        self.assertEqual(
            "上身4下身6，头身比约1:7.2",
            contract["persona_contract"]["identity_lock"]["body_proportion_text"],
        )

    def test_asset_cache_and_binding(self):
        with tempfile.TemporaryDirectory() as temp:
            storage = FirstFrameStorage(Path(temp) / "test.sqlite3")
            storage.upsert_asset(
                fingerprint="fp", asset_id="A1", status="READY",
                feishu_attachment_json='{"file_token":"x"}',
            )
            storage.bind(
                script_id="S1", source_record_id="rec1", fingerprint="fp",
                asset_id="A1", status="READY",
            )
            self.assertEqual(storage.get_ready_asset("fp")["asset_id"], "A1")

    def test_stale_generating_asset_is_closed_with_binding(self):
        with tempfile.TemporaryDirectory() as temp:
            storage = FirstFrameStorage(Path(temp) / "test.sqlite3")
            storage.upsert_asset(
                fingerprint="fp", asset_id="A1", status="GENERATING",
            )
            storage.bind(
                script_id="S1", source_record_id="rec1", fingerprint="fp",
                asset_id="A1", status="GENERATING",
            )
            with sqlite3.connect(str(storage.db_path)) as conn:
                conn.execute(
                    "UPDATE original_first_frame_asset SET updated_at='2000-01-01 00:00:00'"
                )
            recovered = storage.recover_stale_generating(stale_after_seconds=900)
            self.assertEqual(recovered[0]["source_record_id"], "rec1")
            with sqlite3.connect(str(storage.db_path)) as conn:
                asset_status = conn.execute(
                    "SELECT status FROM original_first_frame_asset WHERE asset_fingerprint='fp'"
                ).fetchone()[0]
                binding_status = conn.execute(
                    "SELECT status FROM original_first_frame_binding WHERE script_id='S1'"
                ).fetchone()[0]
            self.assertEqual(asset_status, "FAILED")
            self.assertEqual(binding_status, "FAILED")


if __name__ == "__main__":
    unittest.main()
