#!/usr/bin/env python3
"""VN scarf Phase 5: the travel line binds a VN asset pool per market.

The Phase 2 contract pins ``PHOTO_TRAVEL_OUTFIT_V3.execution_profiles[0]`` to the
TH binding (``TH_WOMENSWEAR_CHOICE``) so the country-agnostic rewrite stays
byte-equivalent to V2.  Phase 5 therefore does **not** touch profile[0]; it adds a
*second* execution profile (``travel_scene_four_looks_vn``) bound to
``VN_SCARF_CHOICE``.  The factory already walks every profile and keeps the
candidates each one can actually serve, so a VN row now freezes the scarf pack
with no per-request override while profile[0] keeps serving TH.

These tests pin the properties that make that safe:

* the two profiles differ **only** in their asset binding — variables and copy
  stay identical, so the travel plan is shared (no creative drift);
* a VN request resolves ``VN_SCARF_CHOICE`` natively, a TH request still
  resolves ``TH_WOMENSWEAR_CHOICE`` and never leaks into the scarf pack;
* a profile may serve a subset of the recipe's locales (only ``vi-VN`` here)
  without breaking recipe load — the loader no longer demands rows for every
  declared locale from every profile.
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from PIL import Image

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
for _path in (PACKAGE_ROOT, TESTS_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from config import loader  # noqa: E402
from domain.models import AssetSet  # noqa: E402
from services.asset_set_service import AssetSetService  # noqa: E402
from services.photo_request_factory import PhotoRequestFactory  # noqa: E402

from test_photo_v3_production_path import (  # noqa: E402
    SCARF_ASSET_SET_KEY, VnScarfProductionRepo, _sha256,
)

CONFIG_DIR = PACKAGE_ROOT / "config"
TRAVEL_RECIPE_PATH = CONFIG_DIR / "recipes" / "PHOTO_TRAVEL_OUTFIT_V3.json"
TRAVEL_RECIPE_ID = "PHOTO_TRAVEL_OUTFIT_V3"
TH_PROFILE_ID = "travel_scene_four_looks"
VN_PROFILE_ID = "travel_scene_four_looks_vn"
TH_ASSET_SET_KEY = "TH_WOMENSWEAR_CHOICE"
TH_PACK_ID = "MP_TH_DEFAULT_V1"
TH_ACCOUNT_ID = "OPV_TH_TEST_001"
TH_LOCALE = "th-TH"


def _build_th_womenswear_asset_set(root: Path) -> AssetSet:
    """A TH womenswear travel pack: four distinct looks, travel logic key.

    Mirrors the structure the scarf pack uses so the same ``content_card``
    freezes, but it is scoped to ``womenswear``/``TH`` — the point of the TH
    control is that this pack must remain the only TH candidate.
    """
    assets = []
    approval = {
        "schema_version": "opv-source-qualification-v1",
        "reviewer": "test_photo_travel_vn_profile",
        "allowed_logic_keys": ["travel_scene_outfit_choice"],
        "source_hashes": {},
        "attributes": {},
    }
    for index, letter in enumerate("abcd"):
        path = root / f"th-look-{letter}.jpg"
        Image.new("RGB", (80, 120), (40, index * 40 + 20, 90)).save(path)
        asset_id = f"th-look-{letter}"
        assets.append({
            "asset_id": asset_id,
            "path": str(path),
            "sha256": _sha256(path),
            "role": f"look_{letter}",
            "display_label": {"th-TH": f"ลุค {letter.upper()}", "zh-CN": f"造型 {letter.upper()}"},
        })
        approval["source_hashes"][asset_id] = _sha256(path)
        approval["attributes"][asset_id] = {
            "outerwear_id": f"th-coat-{letter}",
            "bottom_id": f"th-bottom-{index}",
        }
    return AssetSet(
        asset_set_id="aset-th-womenswear-1",
        asset_set_key=TH_ASSET_SET_KEY,
        category_key="womenswear",
        market="TH",
        asset_set_version=1,
        status="enabled",
        manifest_json={"assets": assets, "content_approval": approval},
        tags_json={
            "choice_axis": ["pants_or_skirt"],
            "use_cases": ["travel_outfit_choice"],
            "scene": ["Photo", "Cafe", "Street"],
            "style": ["french_vintage", "minimal"],
        },
    )


class _TwoMarketRepo(VnScarfProductionRepo):
    """The VN scarf repo plus a TH womenswear pack, pack and account."""

    def __init__(self, root: Path, **kwargs):
        super().__init__(root, recipe_path=TRAVEL_RECIPE_PATH, **kwargs)
        self.th_set = _build_th_womenswear_asset_set(root)
        self.th_pack = SimpleNamespace(
            market_pack_id=TH_PACK_ID, status="active",
            target_country="TH", target_locale=TH_LOCALE,
        )
        self.th_account = SimpleNamespace(
            account_id=TH_ACCOUNT_ID, default_market_pack_id=TH_PACK_ID,
        )

    def get_market_pack(self, market_pack_id):
        if market_pack_id == self.th_pack.market_pack_id:
            return self.th_pack
        return super().get_market_pack(market_pack_id)

    def get_account_profile(self, account_id):
        if account_id == self.th_account.account_id:
            return self.th_account
        return super().get_account_profile(account_id)

    def get_asset_set(self, asset_set_id):
        if asset_set_id == self.th_set.asset_set_id:
            return self.th_set
        return super().get_asset_set(asset_set_id)

    def list_asset_sets(self, **_kwargs):
        return [self.asset_set, self.th_set]


class TravelVnProfileConfigTest(unittest.TestCase):
    """The recipe ships two profiles that differ only in their asset binding."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.spec = loader.load_content_recipe_file(TRAVEL_RECIPE_PATH).recipe_spec_json

    def _by_id(self):
        return {p["profile_id"]: p for p in self.spec["execution_profiles"]}

    def test_two_profiles_bound_to_two_markets(self):
        profiles = self._by_id()
        self.assertEqual(sorted(profiles), [TH_PROFILE_ID, VN_PROFILE_ID])
        # profile[0] is the frozen TH binding the V2-equivalence contract compares.
        self.assertEqual(profiles[TH_PROFILE_ID]["asset_set_keys"], [TH_ASSET_SET_KEY])
        self.assertEqual(profiles[VN_PROFILE_ID]["asset_set_keys"], [SCARF_ASSET_SET_KEY])

    def test_profiles_share_variables_so_the_travel_plan_is_identical(self):
        profiles = self._by_id()
        self.assertEqual(
            profiles[TH_PROFILE_ID]["variables"], profiles[VN_PROFILE_ID]["variables"],
        )

    def test_the_two_profiles_share_the_same_vietnamese_copy(self):
        """Drift guard: the profiles differ only in the asset binding."""
        profiles = self._by_id()
        th_vn = profiles[TH_PROFILE_ID]["copy_variants_by_locale"]["vi-VN"]
        vn_vn = profiles[VN_PROFILE_ID]["copy_variants_by_locale"]["vi-VN"]
        self.assertEqual(th_vn, vn_vn)

    def test_a_profile_may_serve_only_its_own_locale(self):
        """The VN profile carries no Thai copy and still loads cleanly."""
        profiles = self._by_id()
        vn_locales = sorted(profiles[VN_PROFILE_ID]["copy_variants_by_locale"])
        self.assertEqual(vn_locales, ["vi-VN"])
        # The legacy default is the first locale the profile actually serves.
        self.assertEqual(
            profiles[VN_PROFILE_ID]["copy_variants"],
            profiles[VN_PROFILE_ID]["copy_variants_by_locale"]["vi-VN"]["copy_variants"],
        )
        # profile[0] is unchanged: it still carries both locales, TH default.
        th_locales = sorted(profiles[TH_PROFILE_ID]["copy_variants_by_locale"])
        self.assertEqual(th_locales, ["th-TH", "vi-VN"])
        self.assertEqual(
            profiles[TH_PROFILE_ID]["copy_variants"],
            profiles[TH_PROFILE_ID]["copy_variants_by_locale"]["th-TH"]["copy_variants"],
        )


class TravelVnProfileFactoryTest(unittest.TestCase):
    """A VN row freezes the scarf pack; a TH row never leaves womenswear."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = _TwoMarketRepo(Path(self.temp.name))
        self.factory = PhotoRequestFactory(
            self.repo, layouts=loader.load_board_layouts())

    def _spec(self, *, market, language, account_id):
        return SimpleNamespace(
            recipe_id=TRAVEL_RECIPE_ID, market=market, language=language,
            account_id=account_id,
        )

    def test_vn_request_resolves_the_scarf_pack_natively(self):
        request = self.factory.build_batch(
            record_id="vn-travel-record",
            specs=[self._spec(market="VN", language="vi-VN", account_id="OPV_VN_TEST_001")],
            category_key="scarf",
        )[0]
        self.assertEqual(request["asset_set_key"], SCARF_ASSET_SET_KEY)
        self.assertEqual(request["profile_id"], VN_PROFILE_ID)
        self.assertEqual(request["variables"]["destination"], "generic_cool_city")

    def test_th_request_still_resolves_the_womenswear_pack(self):
        request = self.factory.build_batch(
            record_id="th-travel-record",
            specs=[self._spec(market="TH", language=TH_LOCALE, account_id=TH_ACCOUNT_ID)],
            category_key="womenswear",
        )[0]
        self.assertEqual(request["asset_set_key"], TH_ASSET_SET_KEY)
        self.assertEqual(request["profile_id"], TH_PROFILE_ID)

    def test_the_scarf_pack_is_not_a_th_candidate(self):
        """Cross-market leak check: the VN pack cannot back a TH womenswear row."""
        spec = loader.load_content_recipe_file(TRAVEL_RECIPE_PATH).recipe_spec_json
        requirements = spec["asset_requirements"]
        tags = {"choice_axis": "pants_or_skirt"}
        service = AssetSetService(self.repo)
        th = service.candidates(
            category_key="womenswear", market="TH", tags=tags,
            asset_set_keys=[TH_ASSET_SET_KEY], requirements=requirements)
        self.assertEqual([c.asset_set_key for c in th], [TH_ASSET_SET_KEY])
        # Asking for the scarf key under TH/womenswear finds nothing — the same
        # market+category filter that made the unfixed travel line fail loudly.
        leak = service.candidates(
            category_key="womenswear", market="TH", tags=tags,
            asset_set_keys=[SCARF_ASSET_SET_KEY], requirements=requirements)
        self.assertEqual(leak, [])


if __name__ == "__main__":
    unittest.main()
