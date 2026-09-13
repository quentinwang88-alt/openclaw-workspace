from __future__ import annotations

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path

from PIL import Image, ImageChops

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from config.loader import load_board_layouts
from domain.photo_contracts import (
    clamp_utf16, normalize_publish_copy, planned_copy_contract_errors, validate_copy,
)
from services.asset_set_service import validate_asset_set, AssetSetError
from services.photo_copy import resolve_photo_copy
from services.photo_package import _draw_choice_badges, _draw_overlay, normalize_photo_template, PhotoPackageError
from services.photo_planner import PhotoReusePlannerService, PhotoPlannerError
from services.photo_request_factory import PhotoRequestError, fingerprint, validate_frozen_request
from scripts.import_photo_asset_set import normalize
from scripts.preflight_native_photo import preflight
import test_photo_request_factory as factory_tests
from test_photo_planner import PlannerRepo


class CopyLabelTest(unittest.TestCase):
    def setUp(self):
        self.copy = {"title": "เลือก {{label_a}}", "caption": "ชอบ {{label_d}}?",
            "hashtags": ["#OOTD"], "slide_texts": ["เลือกหนึ่งลุค", "A · {{label_a}}",
            "B · {{label_b}}", "C · {{label_c}}", "D · {{label_d}}\nคุณเลือกอะไร?"]}

    def test_locale_selection_role_order_and_missing_label_fallback(self):
        assets = [{"role": "look_b", "display_label": {"th-TH": "สีเทา", "es-MX": "Gris"}},
                  {"role": "look_a", "display_label": {"th-TH": "น้ำตาล"}},
                  {"role": "look_c", "display_label": {"en-US": "Wrong locale"}}]
        result = resolve_photo_copy(self.copy, assets=assets, locale="th-TH")
        self.assertEqual(result["title"], "เลือก น้ำตาล")
        self.assertEqual(result["slide_texts"][2:4], ["B · สีเทา", "C · ลุค C"])
        self.assertIn("ลุค D", result["caption"])
        self.assertIn("{{label_a}}", self.copy["title"])
        self.assertFalse(validate_copy(result))

    def test_unknown_and_residual_placeholders_block(self):
        self.assertFalse(validate_copy(self.copy, allow_placeholders=True))
        self.assertTrue(validate_copy(self.copy))
        for value in ("{{unknown}}", "{{ label_a }}", "{{label_a", "bad }}"):
            bad = {**self.copy, "title": value}
            self.assertTrue(validate_copy(bad, allow_placeholders=True))
            with self.assertRaisesRegex(ValueError, "placeholder"):
                resolve_photo_copy(bad, assets=[], locale="th-TH")
        with self.assertRaisesRegex(ValueError, "placeholder"):
            resolve_photo_copy(self.copy, assets=[{"role": "look_a", "display_label": "{{unknown}}"}], locale="th-TH")
        with self.assertRaisesRegex(PhotoPackageError, "placeholder"):
            _draw_overlay(Image.new("RGB", (1080, 1920)), "{{label_a}}", {})

    def test_import_preserves_labels_and_planner_resolves_advanced_cli_copy(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = PlannerRepo(Path(tmp))
            raw = {"schema_version": "opv-asset-set-v1", "asset_set_id": "test", "asset_set_key": "test",
                "category_key": "womenswear", "market": "TH", "status": "enabled",
                "assets": copy.deepcopy(repo.asset_set.manifest_json["assets"])}
            raw["assets"][1]["display_label"] = {"th-TH": "น้ำตาล"}
            path = Path(tmp) / "assets.json"; path.write_text(json.dumps(raw))
            normalized = normalize(path)
            self.assertEqual(normalized.manifest_json["assets"][1]["display_label"], {"th-TH": "น้ำตาล"})
            repo.asset_set.manifest_json = normalized.manifest_json
            layout = next(x for x in load_board_layouts() if x["layout_id"] == "PHOTO_CHOICE_GRID_V1")
            kwargs = dict(recipe_id=repo.recipe.recipe_id, variables={"choice_axis": "outerwear", "scene": "Photo", "style": "korean_clean"},
                layout=layout, asset_set_id=repo.asset_set.asset_set_id, recipe_snapshot=repo.recipe.to_row(), asset_snapshot=repo.asset_set.to_row())
            with self.assertRaisesRegex(PhotoPlannerError, "placeholder"):
                PhotoReusePlannerService(repo).plan_task(repo.task.task_id, copy_block={**self.copy, "caption": "{{unknown}}"}, **kwargs)
            self.assertIsNone(repo.revision)
            result = PhotoReusePlannerService(repo).plan_task(repo.task.task_id, copy_block=self.copy, **kwargs)
            self.assertEqual(result["plan"]["slides"][1]["overlay_text"], "A · น้ำตาล")
            repo.asset_set.manifest_json["assets"][1]["display_label"] = {"th-TH": ["bad"]}
            with self.assertRaisesRegex(AssetSetError, "display_label"):
                validate_asset_set(repo.asset_set)


class FrozenLabelTest(unittest.TestCase):
    def test_factory_resolves_before_freezing_and_does_not_reread_changed_labels(self):
        fixture = factory_tests.FactoryTest(); fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.repo.asset_set.manifest_json["assets"][0]["display_label"] = {
            "th-TH": "น้ำตาล", "zh-CN": "棕色造型",
        }
        request = fixture.build()[0]
        self.assertEqual(request["copy"]["slide_texts"][1], "A · น้ำตาล")
        self.assertEqual(request["copy"]["language_review_status"], "pending_native_review")
        self.assertIn("泰语待人工确认", fixture.factory.summary([request]))
        fixture.repo.asset_set.manifest_json["assets"][0]["display_label"] = {
            "th-TH": "changed", "zh-CN": "已改变",
        }
        result = PhotoReusePlannerService(fixture.repo).plan_task(fixture.repo.task.task_id,
            recipe_id=request["recipe_id"], variables=request["variables"], copy_block=request["copy"],
            layout=request["layout_snapshot"], asset_set_id=request["asset_set_id"],
            recipe_snapshot=request["recipe_snapshot"], asset_snapshot=request["asset_snapshot"])
        self.assertEqual(result["plan"]["copy"]["slide_texts"][1], "A · น้ำตาล")
        self.assertEqual(result["plan"]["copy"]["language_review_status"], "pending_native_review")
        request["copy"]["caption"] = "{{label_a}}"
        request["request_sha256"] = fingerprint({k: v for k, v in request.items() if k != "request_sha256"})
        with self.assertRaisesRegex(PhotoRequestError, "placeholder"):
            validate_frozen_request(request)


class PublishCopyContractTest(unittest.TestCase):
    """发布契约（TikTok 标题 90 UTF-16 上限）在机器文案入口的夹取行为。"""

    # 2026-09-13 线上实测：这条 91 单元的模型标题让 TH 旅行线一行在**付费生图
    # 之后**整行失败——只超 1 个字符，是最难发现的那种形态。
    MODEL_TITLE = (
        "วัดโทไดจิ ฤดูใบไม้ร่วง ใส่ยังไงถ่ายรูปสวย? "
        "4 ลุคฝรั่งเศสวินเทจ กางเกง หรือ กระโปรง เลือกได้"
    )

    @staticmethod
    def _units(value):
        return len(str(value).encode("utf-16-le")) // 2

    def test_clamp_trims_only_what_overflows(self):
        self.assertEqual(clamp_utf16("a" * 90, 90), "a" * 90)   # 恰好等于上限不动
        self.assertEqual(clamp_utf16("สั้น", 90), "สั้น")         # 未超不动
        self.assertEqual(self._units(clamp_utf16("a" * 91, 90)), 90)
        self.assertEqual(clamp_utf16("a" * 200, 10), "a" * 10)  # 无空格则硬切

    def test_clamp_backs_up_to_a_word_boundary(self):
        clipped = clamp_utf16("hello world this is a fairly long sentence", 20)
        self.assertEqual(clipped, "hello world this")           # 不回退到半个词
        self.assertLessEqual(self._units(clipped), 20)

    def test_clamp_never_splits_a_surrogate_pair(self):
        # 每个 emoji 占 2 个 UTF-16 单元，按字符度量才不会留下孤立高位代理。
        self.assertEqual(clamp_utf16("😀" * 60, 90), "😀" * 45)

    def test_the_real_production_overflow_becomes_publishable(self):
        normalized = normalize_publish_copy({"title": self.MODEL_TITLE})
        self.assertEqual(self._units(normalized["title"]), 82)
        self.assertTrue(self.MODEL_TITLE.startswith(normalized["title"]))

    def test_normalize_keeps_every_other_key_and_only_touches_the_title(self):
        copy_block = {"title": self.MODEL_TITLE, "caption": "สั้น", "hashtags": ["#a"],
                      "slide_texts": ["x"] * 5, "place_localized": "วัดโทไดจิ"}
        normalized = normalize_publish_copy(copy_block)
        self.assertEqual(sorted(normalized), sorted(copy_block))
        self.assertEqual(normalized["caption"], "สั้น")
        self.assertEqual(normalized["slide_texts"], ["x"] * 5)
        self.assertFalse(validate_copy(normalized))

    def test_caption_absorbs_the_cut_when_hashtags_push_past_the_description_limit(self):
        hashtags = ["#" + "ก" * 60, "#" + "ข" * 60]
        normalized = normalize_publish_copy({
            "title": "สั้น", "caption": "ค" * 5000, "hashtags": hashtags,
        })
        self.assertEqual(normalized["hashtags"], hashtags)       # hashtags 不动
        self.assertLessEqual(
            self._units(normalized["caption"]) + self._units(" ".join(hashtags)) + 1, 4000)

    def test_human_authored_copy_is_still_rejected_not_silently_clamped(self):
        # 配置侧（拷贝包 / 审核模板）超限必须继续报错，不能被静默夹掉。
        over = {"title": "a" * 91, "caption": "c", "hashtags": ["#a"],
                "slide_texts": ["x"] * 5}
        self.assertTrue(validate_copy(over))

    def test_planned_copy_check_tolerates_fields_the_theme_still_supplies(self):
        # 规划阶段的文案本来就不完整：主题会兜底 caption / hashtags，slide_texts
        # 要等素材到齐才拼。付费前预检必须容忍这些缺口，否则正常行会被误拦。
        self.assertFalse(planned_copy_contract_errors({"title": "สั้น"}))
        self.assertFalse(planned_copy_contract_errors({}))
        self.assertFalse(planned_copy_contract_errors(
            {"title": "สั้น", "caption": "c", "hashtags": ["#a"]}))

    def test_planned_copy_check_still_catches_the_publish_limits(self):
        self.assertTrue(planned_copy_contract_errors({"title": "a" * 91}))
        self.assertFalse(planned_copy_contract_errors({"title": "a" * 90}))
        self.assertTrue(planned_copy_contract_errors({"title": "   "}))
        self.assertTrue(planned_copy_contract_errors({"hashtags": ["no-dash"]}))
        self.assertTrue(planned_copy_contract_errors({"caption": ""}))

    def test_planned_copy_check_measures_caption_plus_hashtags(self):
        tags = ["#" + "ก" * 60, "#" + "ข" * 60]
        self.assertTrue(planned_copy_contract_errors(
            {"title": "สั้น", "caption": "ค" * 5000, "hashtags": tags}))
        self.assertFalse(planned_copy_contract_errors(
            {"title": "สั้น", "caption": "สั้น", "hashtags": tags}))


class ChoiceBadgeTest(unittest.TestCase):
    def test_only_enabled_choice_cover_has_four_visible_badges(self):
        raw = next(x for x in load_board_layouts() if x["layout_id"] == "PHOTO_CHOICE_GRID_V1")
        template = normalize_photo_template(raw)
        base = Image.new("RGB", (1080, 1920), "#888888")
        for changed, index, layout, expected in (({}, 1, "grid_2x2", True), ({}, 2, "grid_2x2", False),
                ({}, 1, "single", False), ({"template_id": "OTHER"}, 1, "grid_2x2", False),
                ({"choice_badges": False}, 1, "grid_2x2", False)):
            image = base.copy()
            _draw_choice_badges(image, {**template, **changed}, index=index, cover_index=1, layout=layout)
            diff = ImageChops.difference(image, base)
            self.assertEqual(bool(diff.getbbox()), expected)
            if expected:
                patches = []
                for box in ((0, 0, 540, 960), (540, 0, 1080, 960), (0, 960, 540, 1920), (540, 960, 1080, 1920)):
                    patch = diff.crop(box)
                    self.assertIsNotNone(patch.getbbox())
                    patches.append(patch.crop(patch.getbbox()).tobytes())
                self.assertEqual(len(set(patches)), 4)  # Different A/B/C/D glyphs.

    def test_only_new_truthful_sample_is_ready_and_retired_acceptance_stays_blocked(self):
        report = preflight(PACKAGE_ROOT / "config", verify_files=True)
        self.assertFalse(report["errors"], report["errors"])
        expected = {"PHOTO_TH_PICK_YOUR_LOOK_V1", "PHOTO_TH_TEMPERATURE_DRESSING_V1", "PHOTO_TH_TRAVEL_OUTFIT_V1"}
        ready = {r["recipe_id"]: r["ready_profile_count"] for r in report["recipes"] if r["recipe_id"] in expected}
        self.assertEqual(ready, {name: 0 for name in expected})
        self.assertEqual(next(r for r in report["recipes"] if r["recipe_id"] == "PHOTO_TH_PICK_YOUR_LOOK_V3")["ready_profile_count"], 1)
        self.assertEqual(next(r for r in report["recipes"] if r["recipe_id"] == "PHOTO_TH_PICK_YOUR_LOOK_V2")["ready_profile_count"], 0)
        # Travel V2 has no static assets but accepts per-task product/style
        # inputs and dynamically freezes their generated asset set.
        self.assertEqual(next(r for r in report["recipes"] if r["recipe_id"] == "PHOTO_TH_TRAVEL_OUTFIT_V2")["ready_profile_count"], 0)
        self.assertNotIn("PHOTO_TH_TRAVEL_OUTFIT_V2", report["needs_asset"])
        self.assertIn("PHOTO_TH_TRAVEL_OUTFIT_V2", report["dynamic_input_required"])
        travel = next(r for r in report["recipes"] if r["recipe_id"] == "PHOTO_TH_TRAVEL_OUTFIT_V2")
        self.assertEqual(travel["profiles"][0]["status"], "DYNAMIC_INPUT_REQUIRED")
        # 2026-09-13: the retired temperature-layering recipe is gone; the
        # daily thermal-transition line is its dynamic-input replacement.
        self.assertNotIn(
            "PHOTO_TH_TEMPERATURE_DRESSING_V2",
            {r["recipe_id"] for r in report["recipes"]},
        )
        transition = next(
            r for r in report["recipes"]
            if r["recipe_id"] == "PHOTO_TH_THERMAL_TRANSITION_V1"
        )
        self.assertEqual(transition["ready_profile_count"], 0)
        self.assertEqual(
            [item["profile_id"] for item in transition["profiles"]],
            ["thermal_outdoor_bts_office_normal_office"],
        )
        self.assertEqual(
            transition["profiles"][0]["status"], "DYNAMIC_INPUT_REQUIRED"
        )

    def test_preflight_can_target_dynamic_travel_production(self):
        report = preflight(
            PACKAGE_ROOT / "config", verify_files=True,
            recipe_ids=["PHOTO_TH_TRAVEL_OUTFIT_V2"],
        )
        self.assertEqual(report["errors"], [])
        self.assertEqual(report["needs_asset"], [])
        self.assertEqual(report["dynamic_input_required"], [
            "PHOTO_TH_TRAVEL_OUTFIT_V2"
        ])
        self.assertEqual(report["recipe_count"], 1)


if __name__ == "__main__":
    unittest.main()
