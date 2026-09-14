"""Publish language follows the market — never the loader's alphabetical default.

2026-09-14 真跑（VN 围巾旅行线 recvvcgvz0LsIh）暴露：``config.loader`` 把配方的
``locale_copy_packs`` 展开成 per-profile ``copy_variants_by_locale``，同时把
``sorted(...)[0]``（只要带泰语就一定是 ``th-TH``）写进 ``copy_variants``；而发布链路
读的是 ``copy_variants``，于是 VN 行拿到**泰语**文案模板体，``{destination}`` /
``{temperature}`` 却已被越南语标签填好——最终在封版契约里报
"title contains Thai characters for locale vi-VN"，而图片已经付过费。

本文件钉住三条：
1. ``select_execution_profile`` 按市场选档（未声明 markets 的档位对所有市场开放）；
2. ``profile_copy_variants`` 按发布语言取文案包，缺语言时响亮失败；
3. 真实配方（PHOTO_TRAVEL_OUTFIT_V3）下 VN 档 × vi-VN 产出无泰语、TH 档 × th-TH
   与 ``copy_variants`` 逐字相同（TH 逐字不变）。
"""
import copy
import unittest
from pathlib import Path

from config import loader
from domain.photo_contracts import select_execution_profile
from services.photo_locale import (
    PhotoLocaleError, profile_copy_variants, travel_copy_template,
)
from services.photo_request_factory import PhotoRequestError, _localized_variants

import re

PKG_ROOT = Path(__file__).resolve().parents[1]
TRAVEL_RECIPE = PKG_ROOT / "config" / "recipes" / "PHOTO_TRAVEL_OUTFIT_V3.json"
MATCHING_RECIPE = PKG_ROOT / "config" / "recipes" / "PHOTO_MATCHING_CHOICE_V3.json"
TH_V2_RECIPE = PKG_ROOT / "config" / "recipes" / "PHOTO_TH_TRAVEL_OUTFIT_V2.json"
THAI = re.compile(r"[\u0e00-\u0e7f]")


def _thai_fields(copy_block):
    """Every published field that still carries Thai script."""
    hits = []
    for field in ("title", "caption", "cover", "cover_text", "cta"):
        value = str(copy_block.get(field) or "")
        if THAI.search(value):
            hits.append(field)
    for key in ("hashtags", "slide_texts"):
        for index, value in enumerate(copy_block.get(key) or []):
            if THAI.search(str(value)):
                hits.append(f"{key}[{index}]")
    return hits


class SelectExecutionProfileTest(unittest.TestCase):
    def test_undecided_markets_stay_open_to_every_market(self):
        profiles = [{"profile_id": "only", "asset_set_keys": ["K"], "variables": {}}]
        for market in ("TH", "VN", "MX", ""):
            with self.subTest(market=market):
                self.assertEqual(
                    select_execution_profile(profiles, market=market)["profile_id"],
                    "only",
                )

    def test_declared_markets_narrow_the_selection(self):
        profiles = [
            {"profile_id": "th", "markets": ["TH"]},
            {"profile_id": "vn", "markets": ["VN"]},
        ]
        self.assertEqual(select_execution_profile(profiles, market="TH")["profile_id"], "th")
        self.assertEqual(select_execution_profile(profiles, market="VN")["profile_id"], "vn")
        self.assertIsNone(select_execution_profile(profiles, market="MX"))

    def test_explicit_profile_id_wins_and_unknown_id_is_none(self):
        profiles = [
            {"profile_id": "th", "markets": ["TH"]},
            {"profile_id": "vn", "markets": ["VN"]},
        ]
        self.assertEqual(
            select_execution_profile(profiles, market="VN", profile_id="th")["profile_id"],
            "th",
        )
        self.assertIsNone(
            select_execution_profile(profiles, market="VN", profile_id="missing")
        )


class ProfileCopyVariantsTest(unittest.TestCase):
    @staticmethod
    def _profile():
        th = {"copy_id": "th_v1", "copy": {"title": "ไอเดียแต่งตัวเที่ยว{destination}"}}
        vn = {"copy_id": "vn_v1", "copy": {"title": "Gợi ý phối đồ du lịch {destination}"}}
        return {
            "profile_id": "travel_scene_four_looks",
            "copy_variants": [th],
            "copy_variants_by_locale": {
                "th-TH": {"copy_pack_id": "TH_TRAVEL_OUTFIT_V3", "copy_variants": [th]},
                "vi-VN": {"copy_pack_id": "VN_TRAVEL_OUTFIT_V1", "copy_variants": [vn]},
            },
        }

    def test_picks_the_requested_language(self):
        profile = self._profile()
        self.assertEqual(
            [item["copy_id"] for item in profile_copy_variants(profile, locale="vi-VN")],
            ["vn_v1"],
        )
        self.assertEqual(
            [item["copy_id"] for item in profile_copy_variants(profile, locale="th-TH")],
            ["th_v1"],
        )
        # ``copy_variants`` 是 loader 写的字母序默认（带泰语时一定是 th-TH）：
        # 不带语言的任务仍读它，TH 逐字不变。
        self.assertEqual(
            [item["copy_id"] for item in profile_copy_variants(profile, locale="")],
            ["th_v1"],
        )

    def test_v1_profiles_without_a_locale_map_keep_the_legacy_default(self):
        profile = {"profile_id": "v1", "copy_variants": [{"copy_id": "only"}]}
        self.assertEqual(
            [item["copy_id"] for item in profile_copy_variants(profile, locale="vi-VN")],
            ["only"],
        )

    def test_missing_language_fails_loudly_instead_of_serving_thai(self):
        profile = self._profile()
        profile["copy_variants_by_locale"].pop("vi-VN")
        with self.assertRaises(PhotoLocaleError) as ctx:
            profile_copy_variants(profile, locale="vi-VN")
        self.assertIn("vi-VN", str(ctx.exception))

    def test_results_are_copies_not_references(self):
        profile = self._profile()
        taken = profile_copy_variants(profile, locale="vi-VN")
        taken[0]["copy"]["title"] = "mutated"
        self.assertEqual(
            profile["copy_variants_by_locale"]["vi-VN"]["copy_variants"][0]["copy"]["title"],
            "Gợi ý phối đồ du lịch {destination}",
        )

    def test_request_factory_keeps_its_own_error_type(self):
        profile = self._profile()
        profile["copy_variants_by_locale"].pop("vi-VN")
        with self.assertRaises(PhotoRequestError):
            _localized_variants(profile, "vi-VN")


class RealRecipeLanguageFollowsMarketTest(unittest.TestCase):
    """The shipped VN travel recipe must never hand Thai copy to a VN task."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.recipe = loader.load_content_recipe_file(TRAVEL_RECIPE)
        cls.spec = cls.recipe.recipe_spec_json
        cls.profiles = {item["profile_id"]: item for item in cls.spec["execution_profiles"]}

    def test_market_selects_the_matching_profile(self):
        self.assertEqual(
            select_execution_profile(self.spec["execution_profiles"], market="VN")["profile_id"],
            "travel_scene_four_looks_vn",
        )
        self.assertEqual(
            select_execution_profile(self.spec["execution_profiles"], market="TH")["profile_id"],
            "travel_scene_four_looks",
        )

    def test_vn_task_gets_vietnamese_copy(self):
        profile = self.profiles["travel_scene_four_looks_vn"]
        variants = profile_copy_variants(profile, locale="vi-VN")
        self.assertTrue(variants)
        for variant in variants:
            with self.subTest(copy_id=variant["copy_id"]):
                self.assertEqual(_thai_fields(variant["copy"]), [])

    def test_th_task_is_byte_identical_to_the_legacy_default(self):
        profile = self.profiles["travel_scene_four_looks"]
        self.assertEqual(
            profile_copy_variants(profile, locale="th-TH"), profile["copy_variants"],
        )
        # 字母序默认仍是泰语 ⇒ 「不传语言的调用方」行为不变。
        self.assertTrue(_thai_fields(profile["copy_variants"][0]["copy"]))

    def test_publishing_branch_of_a_vn_plan_is_thai_free(self):
        """The plan item's copy is what ``build_theme_copy`` freezes.

        It prefers ``variation['copy']`` over the locale fallback, so a Thai
        template body would reach the frozen request untouched.
        """
        profile = self.profiles["travel_scene_four_looks_vn"]
        locale_pack = loader.load_locale_pack_file(
            PKG_ROOT / "config" / "locales" / "LOCALE_VI_VN_V1.json"
        )
        from services.photo_copy import fill_travel_copy_tokens

        variables = dict(profile["variables"])
        for variant in profile_copy_variants(profile, locale="vi-VN"):
            template = variant["copy"]
            filled = copy.deepcopy(template)
            for field in ("title", "caption"):
                filled[field] = fill_travel_copy_tokens(
                    str(template.get(field) or ""),
                    travel_contract=self.spec["travel_contract"],
                    variables=variables, locale_pack=locale_pack,
                )
            filled["slide_texts"] = [
                fill_travel_copy_tokens(
                    str(value), travel_contract=self.spec["travel_contract"],
                    variables=variables, locale_pack=locale_pack,
                )
                for value in template["slide_texts"]
            ]
            with self.subTest(copy_id=variant["copy_id"]):
                self.assertEqual(_thai_fields(filled), [])
                # 越南语标签真的被填进去了（不是被清空）
                self.assertIn("Thành phố se lạnh", filled["title"])

    def test_matching_recipe_profiles_keep_no_locale_map_behaviour(self):
        """搭配线的文案归 Locale Pack 的 ``family_copy``，档位不带语言映射。"""
        recipe = loader.load_content_recipe_file(MATCHING_RECIPE)
        for profile in recipe.recipe_spec_json["execution_profiles"]:
            locale_map = profile.get("copy_variants_by_locale")
            if not locale_map:
                self.assertEqual(
                    profile_copy_variants(profile, locale="vi-VN"),
                    [dict(item) for item in profile.get("copy_variants") or []],
                )

    def test_v1_thai_recipe_is_unaffected(self):
        recipe = loader.load_content_recipe_file(TH_V2_RECIPE)
        profile = recipe.recipe_spec_json["execution_profiles"][0]
        self.assertFalse(recipe.recipe_spec_json.get("locale_copy_packs"))
        self.assertFalse(profile.get("copy_variants_by_locale"))
        for variant in profile_copy_variants(profile, locale="th-TH"):
            with self.subTest(copy_id=variant["copy_id"]):
                self.assertTrue(_thai_fields(variant["copy"]))

    def test_degraded_template_is_locale_owned(self):
        """模型选题文案不合格时退化的模板同样不得是泰语。"""
        locale_pack = loader.load_locale_pack_file(
            PKG_ROOT / "config" / "locales" / "LOCALE_VI_VN_V1.json"
        )
        degraded = travel_copy_template(locale_pack, 1, topic_zh="主题")
        self.assertEqual(_thai_fields(degraded), [])
        self.assertIsNone(travel_copy_template(None, 1, topic_zh="主题"))


if __name__ == "__main__":
    unittest.main()
