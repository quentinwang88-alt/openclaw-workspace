"""Phase 1 equivalence tests for the Category Adapter registry.

These tests are deliberately written against *legacy copies* of the tables and
expressions that used to live inline in the pipeline.  They are not "new
behaviour" tests: they exist so that a later cleanup cannot silently change
what the pipeline produces for TH V2.

If one of these fails, do not re-baseline it.  Read the diff and decide whether
the change to the pipeline was intended (Phase 3 may intend some of them).
"""
import unittest
from pathlib import Path

from services.photo_category_registry import (
    DEFAULT_ROLE_PRIORITY,
    TARGET_PRODUCT_FIELDS,
    WOMENSWEAR_V1,
    UnknownPhotoCategoryError,
    adapter_for_product_category,
    apply_target_product_to_look,
    build_product_qa_contract,
    get_photo_category_adapter,
    product_owns_slot,
    registered_category_keys,
    resolve_product_display_label,
    resolve_product_slot,
    role_priority_for_slot,
)

ROLES = ("look_a", "look_b", "look_c", "look_d")

# --- legacy copies -------------------------------------------------------
# Verbatim from photo_reference_vision._normalize_travel_plan (pre-Phase-1).
LEGACY_TARGET_FIELDS = {
    "outerwear": ("outerwear", "外套"),
    "top": ("top_inner", "上装"),
    "bottom": ("bottom", "下装"),
    "dress": ("outerwear", "连衣裙"),
    "shoes": ("shoes", "鞋履"),
    "shoe": ("shoes", "鞋履"),
    "footwear": ("shoes", "鞋履"),
}

# Verbatim from image_generator.compose_shot_prompt (pre-Phase-1).
LEGACY_PRODUCT_LABELS = {
    "outerwear": "目标外套",
    "dress": "目标连衣裙",
    "top": "目标上装",
    "bottom": "目标下装",
}
LEGACY_PRODUCT_LABEL_DEFAULT = "目标商品"

# Verbatim from product_reference_resolver.select_product_references_for_slot.
LEGACY_ROLE_ORDER = {
    "hero": ("front", "side", "back", "lifestyle"),
    "full_look": ("front", "back", "side"),
    "lifestyle": ("front", "lifestyle", "side"),
    "detail": ("front", "detail"),
    "second_angle": ("front", "back", "side", "lifestyle"),
}
LEGACY_ROLE_FALLBACK = ("front", "side", "back", "lifestyle", "detail")


def legacy_target_product_block(look, product):
    """The pre-Phase-1 planning-side block, reimplemented verbatim."""
    normalized = dict(look)
    product = dict(product or {})
    product_id = str(product.get("product_id") or "")
    if product_id:
        category = str(product.get("category") or "").strip().lower()
        normalized["target_product"] = {
            key: product.get(key)
            for key in ("product_id", "product_name", "category",
                        "reference_pack_id", "reference_pack_version")
            if product.get(key) not in (None, "")
        }
        target = LEGACY_TARGET_FIELDS.get(category)
        if target:
            normalized[target[0]] = f"指定商品{target[1]}（以商品参考图为准）"
    return normalized


def legacy_style_supply_condition(product):
    """The pre-Phase-1 generation-side condition, reimplemented verbatim."""
    return bool(product) and str(product.get("category") or "").lower() == "outerwear"


class AdapterRegistryTest(unittest.TestCase):
    def test_womenswear_is_registered_and_resolvable(self):
        # 2026-09-13 (VN scarf Phase 3): SCARF_V1 joined the registry. This is a
        # deliberate, reviewed expansion — see tests/test_photo_scarf_category.py
        # for the scarf contract itself.
        self.assertEqual(registered_category_keys(), ("scarf", "womenswear"))
        adapter = get_photo_category_adapter("womenswear")
        self.assertIs(adapter, WOMENSWEAR_V1)
        # lookup is tolerant of padding/case, matching the registry convention
        self.assertIs(get_photo_category_adapter(" WOMENSWEAR "), WOMENSWEAR_V1)

    def test_unknown_category_raises(self):
        # "scarf" used to live here; Phase 3 registers it, so the guards are now
        # the genuinely unregistered tokens. "shoes" is a *product* category,
        # not a category_key, and must not resolve either.
        for token in ("wig", "shoes", "", "nope"):
            with self.assertRaises(UnknownPhotoCategoryError):
                get_photo_category_adapter(token)

    def test_womenswear_declares_the_recipe_contract(self):
        self.assertEqual(WOMENSWEAR_V1.category_key, "womenswear")
        self.assertEqual(WOMENSWEAR_V1.main_product_slot, "outerwear")
        self.assertEqual(WOMENSWEAR_V1.required_product_roles, ROLES)
        self.assertEqual(
            WOMENSWEAR_V1.accepted_product_categories,
            ("outerwear", "top", "bottom", "dress", "shoes", "shoe", "footwear"),
        )
        self.assertTrue(WOMENSWEAR_V1.identity_attributes)
        self.assertIn("dominant_color", WOMENSWEAR_V1.identity_attributes)
        self.assertIn("wearable_styling", WOMENSWEAR_V1.capabilities)

    def test_adapter_for_product_category_indexes_accepted_categories(self):
        self.assertIs(adapter_for_product_category("dress"), WOMENSWEAR_V1)
        self.assertIs(adapter_for_product_category("FOOTWEAR"), WOMENSWEAR_V1)
        # wig is a real product category in this repo but has no adapter yet
        self.assertIsNone(adapter_for_product_category("wig"))
        self.assertIsNone(adapter_for_product_category(""))
        self.assertIsNone(adapter_for_product_category(None))


class LegacyTableEquivalenceTest(unittest.TestCase):
    """The adapter tables must still be byte-identical to the inline ones."""

    def test_product_slot_table_matches_legacy(self):
        self.assertEqual(dict(WOMENSWEAR_V1.product_slot_by_category),
                         LEGACY_TARGET_FIELDS)

    def test_display_label_table_matches_legacy(self):
        self.assertEqual(dict(WOMENSWEAR_V1.product_display_label_by_category),
                         LEGACY_PRODUCT_LABELS)
        self.assertEqual(WOMENSWEAR_V1.default_product_display_label_zh,
                         LEGACY_PRODUCT_LABEL_DEFAULT)

    def test_role_priority_table_matches_legacy(self):
        self.assertEqual(dict(WOMENSWEAR_V1.product_reference_priority_by_slot),
                         LEGACY_ROLE_ORDER)
        self.assertEqual(DEFAULT_ROLE_PRIORITY, LEGACY_ROLE_FALLBACK)

    def test_target_product_fields_match_legacy(self):
        self.assertEqual(TARGET_PRODUCT_FIELDS, (
            "product_id", "product_name", "category",
            "reference_pack_id", "reference_pack_version",
        ))


class ResolveProductSlotTest(unittest.TestCase):
    def test_matches_legacy_lookup_for_every_key(self):
        for token, expected in LEGACY_TARGET_FIELDS.items():
            self.assertEqual(resolve_product_slot(WOMENSWEAR_V1, token), expected)

    def test_none_adapter_has_no_slot(self):
        self.assertIsNone(resolve_product_slot(None, "dress"))

    def test_lookup_does_not_normalize(self):
        # the caller owns normalization; a padded token must miss here
        self.assertIsNone(resolve_product_slot(WOMENSWEAR_V1, " outerwear "))
        self.assertIsNone(resolve_product_slot(WOMENSWEAR_V1, "Dress"))


class ResolveDisplayLabelTest(unittest.TestCase):
    def test_matches_legacy_map_and_default(self):
        for token in ("outerwear", "dress", "top", "bottom"):
            self.assertEqual(resolve_product_display_label(token),
                             LEGACY_PRODUCT_LABELS[token])
        # legacy peculiarity: shoes aliases exist in the planning table but not
        # here, so they fall back to the default label. Pinned, not "fixed".
        for token in ("shoes", "shoe", "footwear", "wig", ""):
            self.assertEqual(resolve_product_display_label(token),
                             LEGACY_PRODUCT_LABEL_DEFAULT)


class RolePriorityTest(unittest.TestCase):
    def test_matches_legacy_role_order(self):
        for role, expected in LEGACY_ROLE_ORDER.items():
            self.assertEqual(role_priority_for_slot(WOMENSWEAR_V1, role), expected)

    def test_unknown_role_uses_shared_fallback(self):
        for role in ("fifth_angle", "", "HERO"):
            self.assertEqual(role_priority_for_slot(WOMENSWEAR_V1, role),
                             LEGACY_ROLE_FALLBACK)

    def test_no_adapter_uses_shared_fallback(self):
        self.assertEqual(role_priority_for_slot(None, "hero"), LEGACY_ROLE_FALLBACK)


class ProductOwnsSlotTest(unittest.TestCase):
    """``product_owns_slot`` is NOT the same question as ``resolve_product_slot``.

    Planning writes ``dress`` into the outerwear slot; generation only lets an
    ``outerwear`` product take that slot over. Phase 1 keeps both behaviours.
    """

    def test_only_outerwear_owns_the_outerwear_slot(self):
        self.assertTrue(product_owns_slot(WOMENSWEAR_V1, "outerwear", "outerwear"))
        self.assertFalse(product_owns_slot(WOMENSWEAR_V1, "outerwear", "dress"))
        self.assertFalse(product_owns_slot(WOMENSWEAR_V1, "outerwear", "top"))

    def test_dress_still_maps_to_the_outerwear_slot_when_planning(self):
        self.assertEqual(resolve_product_slot(WOMENSWEAR_V1, "dress"),
                         ("outerwear", "连衣裙"))

    def test_unknown_slot_or_adapter_is_false(self):
        self.assertFalse(product_owns_slot(WOMENSWEAR_V1, "accessories", "scarf"))
        self.assertFalse(product_owns_slot(None, "outerwear", "outerwear"))

    def test_caller_owns_normalization(self):
        self.assertFalse(product_owns_slot(WOMENSWEAR_V1, "outerwear", " Outerwear"))


class ApplyTargetProductTest(unittest.TestCase):
    def look(self):
        return {"role": "look_a", "outerwear": "奶白羊毛大衣",
                "top_inner": "白衬衫", "bottom": "直筒牛仔", "shoes": "平底短靴"}

    def test_no_product_leaves_look_untouched(self):
        for product in (None, {}, {"product_id": ""}, {"product_id": None}):
            self.assertEqual(apply_target_product_to_look(
                adapter=WOMENSWEAR_V1, look=self.look(), product_snapshot=product),
                self.look())

    def test_every_category_matches_the_legacy_block(self):
        cases = [
            {"product_id": "P1", "product_name": "大衣", "category": "outerwear",
             "reference_pack_id": "PK1", "reference_pack_version": 3},
            {"product_id": "P2", "category": "top"},
            {"product_id": "P3", "category": "bottom"},
            {"product_id": "P4", "category": "dress"},
            {"product_id": "P5", "category": "shoes"},
            {"product_id": "P6", "category": "shoe"},
            {"product_id": "P7", "category": "footwear"},
            {"product_id": "P8", "category": "wig"},
            {"product_id": "P9", "category": ""},
            {"product_id": "PA", "category": "  DRESS  "},
        ]
        for product in cases:
            with self.subTest(category=product.get("category")):
                self.assertEqual(
                    apply_target_product_to_look(
                        adapter=WOMENSWEAR_V1, look=self.look(),
                        product_snapshot=product),
                    legacy_target_product_block(self.look(), product),
                )

    def test_unmapped_category_still_freezes_target_product(self):
        result = apply_target_product_to_look(
            adapter=WOMENSWEAR_V1, look=self.look(),
            product_snapshot={"product_id": "P8", "category": "wig"},
        )
        self.assertEqual(result["target_product"], {"product_id": "P8",
                                                    "category": "wig"})
        self.assertEqual(result["outerwear"], self.look()["outerwear"])

    def test_dress_occupies_the_outerwear_slot(self):
        result = apply_target_product_to_look(
            adapter=WOMENSWEAR_V1, look=self.look(),
            product_snapshot={"product_id": "P4", "category": "dress"},
        )
        self.assertEqual(result["outerwear"], "指定商品连衣裙（以商品参考图为准）")

    def test_blank_product_fields_are_omitted(self):
        result = apply_target_product_to_look(
            adapter=WOMENSWEAR_V1, look=self.look(),
            product_snapshot={"product_id": "P1", "product_name": "",
                              "category": "top"},
        )
        self.assertEqual(result["target_product"],
                         {"product_id": "P1", "category": "top"})

    def test_adapter_none_self_resolves_from_product_category(self):
        product = {"product_id": "P5", "category": "shoes"}
        self.assertEqual(
            apply_target_product_to_look(adapter=None, look=self.look(),
                                         product_snapshot=product),
            legacy_target_product_block(self.look(), product),
        )

    def test_input_look_is_not_mutated(self):
        original = self.look()
        apply_target_product_to_look(
            adapter=WOMENSWEAR_V1, look=original,
            product_snapshot={"product_id": "P1", "category": "top"},
        )
        self.assertEqual(original, self.look())


class SiteEquivalenceTest(unittest.TestCase):
    """The two real call sites must still behave exactly as they did."""

    def test_style_supply_condition_matches_legacy(self):
        from services.photo_style_reference_supply import _product_targets_slot

        cases = [
            None, {}, {"category": "outerwear"}, {"category": "OUTERWEAR"},
            {"category": " outerwear "}, {"category": "dress"},
            {"category": "shoes"}, {"category": "wig"}, {"category": ""},
            {"category": None}, {"category": "top"},
        ]
        for product in cases:
            with self.subTest(product=product):
                self.assertEqual(_product_targets_slot(product, "outerwear"),
                                 legacy_style_supply_condition(product))

    def test_style_supply_ignores_case_but_not_padding(self):
        from services.photo_style_reference_supply import _product_targets_slot

        self.assertTrue(_product_targets_slot({"category": "Outerwear"}, "outerwear"))
        self.assertFalse(_product_targets_slot({"category": " outerwear "}, "outerwear"))

    def test_dress_does_not_take_over_the_outerwear_slot(self):
        """Regression guard: the first Phase-1 attempt got this wrong."""
        from services.photo_style_reference_supply import _product_targets_slot

        self.assertFalse(_product_targets_slot({"category": "dress"}, "outerwear"))
        self.assertTrue(_product_targets_slot({"category": "outerwear"}, "outerwear"))

    def test_vision_site_delegates_to_the_registry(self):
        import inspect

        from services import photo_reference_vision

        source = inspect.getsource(
            photo_reference_vision.PhotoReferenceVisionService._normalize_travel_plan
        )
        self.assertIn("apply_target_product_to_look", source)
        self.assertNotIn("target_fields", source)

    def test_resolver_delegates_to_the_registry(self):
        import inspect

        from services import product_reference_resolver

        source = inspect.getsource(
            product_reference_resolver.select_product_references_for_slot
        )
        self.assertIn("role_priority_for_slot", source)
        self.assertNotIn('"second_angle"', source)

    def test_generator_delegates_to_the_registry(self):
        import inspect

        from services import image_generator

        source = inspect.getsource(image_generator.compose_shot_prompt)
        self.assertIn("resolve_product_display_label", source)
        self.assertNotIn("目标外套", source)


class ProductQaContractTest(unittest.TestCase):
    def test_contract_describes_the_identity_lock(self):
        product = {"product_id": "P1", "category": "outerwear",
                   "reference_pack_id": "PK1", "reference_pack_version": 3}
        contract = build_product_qa_contract(adapter=WOMENSWEAR_V1,
                                             product_snapshot=product)
        self.assertEqual(contract["schema_version"], "opv-photo-product-qa-v1")
        self.assertEqual(contract["category_key"], "womenswear")
        self.assertEqual(contract["product_id"], "P1")
        self.assertEqual(contract["main_product_slot"], "outerwear")
        self.assertEqual(contract["required_product_roles"], list(ROLES))
        self.assertEqual(contract["reference_pack_id"], "PK1")
        self.assertEqual(contract["reference_pack_version"], 3)
        self.assertIn("颜色", contract["must_keep"])
        self.assertIn("替换成相似款", contract["forbidden"])

    def test_contract_tolerates_missing_product(self):
        contract = build_product_qa_contract(adapter=WOMENSWEAR_V1)
        self.assertEqual(contract["product_id"], "")
        self.assertEqual(contract["identity_attributes"],
                         list(WOMENSWEAR_V1.identity_attributes))


class ModulePurityTest(unittest.TestCase):
    def test_registry_module_has_no_service_dependencies(self):
        source = Path("services/photo_category_registry.py").read_text(
            encoding="utf-8")
        self.assertNotIn("import requests", source)
        self.assertNotIn("from services.", source)
        self.assertNotIn("open(", source)


if __name__ == "__main__":
    unittest.main()
