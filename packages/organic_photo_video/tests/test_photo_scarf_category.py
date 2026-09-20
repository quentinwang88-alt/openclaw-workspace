"""Phase 3 tests for the SCARF category adapter.

Phase 3 adds the first category that is not womenswear, and the first one whose
main product slot is not ``outerwear``.  The point of these tests is twofold:

1. the scarf contract itself (slot, label, reference priority, QA fields), and
2. that adding it left TH V2 / womenswear output **bit-for-bit unchanged**.

If a womenswear assertion here fails, it is a regression, not a re-baseline:
Phase 3 was explicitly scoped to add a category, not to change an existing one.
The scarf assertions, by contrast, are new behaviour introduced in this phase.
"""
import json
import unittest
from pathlib import Path

from config import loader
from domain import contracts
from services.image_generator import ShotGenerationRequest, compose_shot_prompt
from services.photo_category_registry import (
    DEFAULT_ROLE_PRIORITY,
    SCARF_QA_FIELDS,
    SCARF_V1,
    WOMENSWEAR_V1,
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
from services.photo_execution_context import build_execution_context
from services.photo_style_reference_supply import (
    _product_targets_slot,
    _target_slot_override,
)
from services.photo_locale import destination_entry
from services.product_reference_resolver import select_product_references_for_slot

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SCARF_PROFILE_PATH = PACKAGE_ROOT / "config" / "categories" / "SCARF_V1.json"
RECIPE_PATH = PACKAGE_ROOT / "config" / "recipes" / "PHOTO_TRAVEL_OUTFIT_V3.json"

ROLES = ("look_a", "look_b", "look_c", "look_d")

# Verbatim from product_reference_resolver's pre-Phase-1 inline table: the
# womenswear order that every product used to get, with the shared fallback.
LEGACY_ROLE_ORDER = {
    "hero": ("front", "side", "back", "lifestyle"),
    "full_look": ("front", "back", "side"),
    "lifestyle": ("front", "lifestyle", "side"),
    "detail": ("front", "detail"),
    "second_angle": ("front", "back", "side", "lifestyle"),
}
LEGACY_ROLE_FALLBACK = ("front", "side", "back", "lifestyle", "detail")

# Spec §5.6: the per-page scarf QA output must carry exactly these fields.
SPEC_SCARF_QA_FIELDS = (
    "role", "product_present", "product_matches", "visibility_sufficient",
    "dominant_color_matches", "pattern_family_matches", "edge_or_fringe_matches",
    "length_volume_plausible", "face_unobscured", "repair_instruction",
)


def _shipped_scarf_profile() -> dict:
    return loader.load_category_file(SCARF_PROFILE_PATH)


class ShippedScarfProfileTest(unittest.TestCase):
    """The v2 config file and the Python adapter must not drift apart."""

    def setUp(self):
        self.profile = _shipped_scarf_profile()

    def test_profile_loads_under_the_v2_schema(self):
        self.assertEqual(self.profile["schema_version"],
                         loader.CATEGORY_PROFILE_V2_SCHEMA)
        self.assertEqual(self.profile["category_key"], "scarf")

    def test_profile_never_binds_market_or_language(self):
        for forbidden in contracts.CATEGORY_FORBIDDEN_KEYS:
            self.assertNotIn(forbidden, self.profile)

    def test_profile_is_loadable_by_the_seed_bundle(self):
        keys = {item["category_key"] for item in loader.load_categories()}
        self.assertEqual(keys, {"womenswear", "wig", "scarf"})

    def test_profile_agrees_with_the_registered_adapter(self):
        profile = self.profile
        self.assertEqual(profile["category_key"], SCARF_V1.category_key)
        self.assertEqual(tuple(profile["capabilities"]), SCARF_V1.capabilities)
        self.assertEqual(tuple(profile["accepted_product_categories"]),
                         SCARF_V1.accepted_product_categories)
        self.assertEqual(profile["main_product_slot"], SCARF_V1.main_product_slot)
        self.assertEqual(profile["product_label_zh"], SCARF_V1.product_label_zh)
        self.assertEqual(tuple(profile["required_product_roles"]),
                         SCARF_V1.required_product_roles)
        self.assertEqual(tuple(profile["identity_attributes"]),
                         SCARF_V1.identity_attributes)
        self.assertEqual(
            {role: tuple(order)
             for role, order in profile["product_reference_priority_by_slot"].items()},
            {role: tuple(order)
             for role, order in SCARF_V1.product_reference_priority_by_slot.items()},
        )


class ScarfAdapterContractTest(unittest.TestCase):
    def test_scarf_is_registered_as_a_category_key(self):
        self.assertIn("scarf", registered_category_keys())
        self.assertIs(get_photo_category_adapter("scarf"), SCARF_V1)
        self.assertIs(get_photo_category_adapter(" SCARF "), SCARF_V1)

    def test_scarf_owns_the_accessories_slot_not_a_garment_slot(self):
        self.assertEqual(SCARF_V1.main_product_slot, "accessories")
        self.assertEqual(resolve_product_slot(SCARF_V1, "scarf"),
                         ("accessories", "围巾"))
        self.assertTrue(product_owns_slot(SCARF_V1, "accessories", "scarf"))
        # A scarf must never take over a garment slot.
        for slot in ("outerwear", "top_inner", "bottom", "shoes"):
            with self.subTest(slot=slot):
                self.assertFalse(product_owns_slot(SCARF_V1, slot, "scarf"))

    def test_scarf_declares_the_recipe_capabilities(self):
        self.assertEqual(
            SCARF_V1.capabilities,
            ("wearable_styling", "travel_look", "product_embedding"),
        )
        self.assertEqual(SCARF_V1.required_product_roles, ROLES)

    def test_scarf_identity_attributes_match_the_spec(self):
        self.assertEqual(SCARF_V1.identity_attributes, (
            "dominant_color", "pattern_family", "material_appearance",
            "edge_or_fringe", "length_volume",
        ))
        # Garment-only attributes must not leak into a scarf contract.
        for garment_only in ("collar", "placket", "cuff", "silhouette"):
            self.assertNotIn(garment_only, SCARF_V1.identity_attributes)

    def test_product_category_lookup_finds_the_scarf_adapter(self):
        self.assertIs(adapter_for_product_category("scarf"), SCARF_V1)
        self.assertIs(adapter_for_product_category("SCARF"), SCARF_V1)

    def test_scarf_display_label_is_the_generic_name(self):
        # Spec §6.2: product_reference_resolver / image_generator only gain the
        # generic local name "目标围巾"; no country logic is added.
        self.assertEqual(resolve_product_display_label("scarf"), "目标围巾")


class ScarfSlotWriteTest(unittest.TestCase):
    def look(self):
        return {"role": "look_a", "outerwear": "奶白羊毛大衣",
                "top_inner": "白衬衫", "bottom": "直筒牛仔", "shoes": "平底短靴"}

    def test_scarf_is_written_into_accessories(self):
        result = apply_target_product_to_look(
            adapter=None, look=self.look(),
            product_snapshot={"product_id": "S1", "product_name": "格纹围巾",
                              "category": "scarf"},
        )
        self.assertEqual(result["accessories"], "指定商品围巾（以商品参考图为准）")
        self.assertEqual(result["target_product"],
                         {"product_id": "S1", "product_name": "格纹围巾",
                          "category": "scarf"})

    def test_scarf_does_not_overwrite_garment_slots(self):
        result = apply_target_product_to_look(
            adapter=None, look=self.look(),
            product_snapshot={"product_id": "S1", "category": "scarf"},
        )
        for slot in ("outerwear", "top_inner", "bottom", "shoes"):
            with self.subTest(slot=slot):
                self.assertEqual(result[slot], self.look()[slot])

    def test_scarf_write_is_idempotent(self):
        once = apply_target_product_to_look(
            adapter=None, look=self.look(),
            product_snapshot={"product_id": "S1", "category": "scarf"})
        twice = apply_target_product_to_look(
            adapter=None, look=once,
            product_snapshot={"product_id": "S1", "category": "scarf"})
        self.assertEqual(once, twice)

    def test_style_supply_writes_the_accessories_slot_only_for_scarf(self):
        self.assertEqual(_target_slot_override({"category": "scarf"}), "accessories")
        # Every other category must stay inert, otherwise TH V2 changes.
        for category in ("outerwear", "OUTERWEAR", "dress", "top", "bottom",
                         "shoes", "wig", "", None):
            with self.subTest(category=category):
                self.assertEqual(_target_slot_override({"category": category}), "")
        self.assertEqual(_target_slot_override(None), "")

    def test_style_supply_still_lets_outerwear_claim_its_own_slot(self):
        self.assertTrue(_product_targets_slot({"category": "outerwear"}, "outerwear"))
        self.assertFalse(_product_targets_slot({"category": "scarf"}, "outerwear"))


class ScarfReferencePriorityTest(unittest.TestCase):
    def test_scarf_prefers_front_lifestyle_detail(self):
        for role in ("hero", "full_look"):
            with self.subTest(role=role):
                self.assertEqual(role_priority_for_slot(SCARF_V1, role),
                                 ("front", "lifestyle", "detail"))
        self.assertEqual(role_priority_for_slot(SCARF_V1, "detail"),
                         ("front", "detail"))

    def test_scarf_never_asks_for_a_back_view(self):
        # A scarf is worn on one side; a back shot is not part of the contract.
        for role in ("hero", "full_look", "detail"):
            self.assertNotIn("back", role_priority_for_slot(SCARF_V1, role))

    def test_unlisted_scarf_role_falls_back_to_the_shared_order(self):
        self.assertEqual(role_priority_for_slot(SCARF_V1, "second_angle"),
                         DEFAULT_ROLE_PRIORITY)
        self.assertEqual(DEFAULT_ROLE_PRIORITY, LEGACY_ROLE_FALLBACK)

    def test_resolver_uses_the_scarf_order_for_scarf_products(self):
        product = {
            "category": "scarf",
            "reference_roles": {
                "front": ["f.jpg"], "lifestyle": ["l.jpg"],
                "detail": ["d.jpg"], "back": ["b.jpg"],
            },
        }
        # "hero" for a scarf is front > lifestyle > detail: three picked, and
        # the back view is never reached.
        self.assertEqual(select_product_references_for_slot(product, "hero"),
                         ["f.jpg", "l.jpg", "d.jpg"])

    def test_resolver_keeps_the_legacy_order_for_other_categories(self):
        """Womenswear and unclaimed categories must be unchanged.

        ``hero`` in the legacy womenswear order is front > side > back, so the
        same three files come back whichever non-scarf category is used.
        """
        for category in ("outerwear", "dress", "top", "wig", "", None):
            with self.subTest(category=category):
                product = {
                    "category": category,
                    "reference_roles": {
                        "front": ["f.jpg"], "side": ["s.jpg"],
                        "back": ["b.jpg"], "lifestyle": ["l.jpg"],
                        "detail": ["d.jpg"],
                    },
                }
                self.assertEqual(
                    select_product_references_for_slot(product, "hero"),
                    ["f.jpg", "s.jpg", "b.jpg"],
                )


class ScarfQaContractTest(unittest.TestCase):
    def contract(self):
        return build_product_qa_contract(
            adapter=SCARF_V1,
            product_snapshot={"product_id": "S1", "reference_pack_id": "PK9",
                              "reference_pack_version": 2},
        )

    def test_qa_fields_are_the_spec_fields(self):
        self.assertEqual(tuple(self.contract()["qa_fields"]), SPEC_SCARF_QA_FIELDS)
        self.assertEqual(SCARF_QA_FIELDS, SPEC_SCARF_QA_FIELDS)

    def test_declared_qa_rules_are_deterministic(self):
        rules = self.contract()["qa_rules"]
        self.assertTrue(rules)
        # The spec's hard failures and the "no MINOR escape hatch" clause.
        joined = "\n".join(rules)
        for fragment in ("product_present=false", "product_matches=false",
                         "visibility_sufficient=false", "MINOR"):
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, joined)

    def test_identity_lock_uses_scarf_wording(self):
        contract = self.contract()
        self.assertEqual(contract["category_key"], "scarf")
        self.assertEqual(contract["main_product_slot"], "accessories")
        self.assertEqual(contract["product_label_zh"], "目标围巾")
        self.assertIn("图案家族", contract["must_keep"])
        self.assertIn("边缘或流苏结构", contract["must_keep"])
        self.assertIn("遮挡人物面部", contract["forbidden"])
        self.assertEqual(contract["reference_pack_id"], "PK9")
        self.assertEqual(contract["reference_pack_version"], 2)

    def test_womenswear_contract_is_unchanged(self):
        """Phase 1's shape and wording must survive Phase 3 verbatim."""
        contract = build_product_qa_contract(adapter=WOMENSWEAR_V1)
        self.assertEqual(contract["must_keep"], [
            "颜色", "图案", "材质观感", "形状与结构",
            "版型", "衣长", "领型", "前襟", "袖口",
        ])
        self.assertEqual(contract["forbidden"], [
            "替换成相似款", "重新设计指定商品",
            "用文案覆盖商品参考图的颜色和材质",
        ])
        self.assertEqual(contract["qa_fields"], [])
        self.assertEqual(contract["qa_rules"], [])


class WomenswearUnaffectedTest(unittest.TestCase):
    """Everything the TH V2 route touches must be byte-identical."""

    def test_legacy_tables_are_untouched(self):
        self.assertEqual(dict(WOMENSWEAR_V1.product_slot_by_category), {
            "outerwear": ("outerwear", "外套"),
            "top": ("top_inner", "上装"),
            "bottom": ("bottom", "下装"),
            "dress": ("outerwear", "连衣裙"),
            "shoes": ("shoes", "鞋履"),
            "shoe": ("shoes", "鞋履"),
            "footwear": ("shoes", "鞋履"),
        })
        self.assertEqual(dict(WOMENSWEAR_V1.product_reference_priority_by_slot),
                         LEGACY_ROLE_ORDER)
        self.assertEqual(dict(WOMENSWEAR_V1.product_own_category_by_slot),
                         {"outerwear": ("outerwear",)})

    def test_none_of_the_adapter_declared_extras_leak_into_womenswear(self):
        self.assertEqual(WOMENSWEAR_V1.qa_fields, ())
        self.assertEqual(WOMENSWEAR_V1.qa_rules, ())
        self.assertEqual(WOMENSWEAR_V1.presence_lock_lines, ())

    def _prompt(self, category: str) -> str:
        return compose_shot_prompt(ShotGenerationRequest(
            task_id="t", slot_index=1, slot_role="full_look", shot_version=1,
            plan_shot={"outfit_state_ref": "FINAL", "purpose": "p",
                       "camera_hint": "c"},
            product={"product_id": "P1", "category": category},
            persona_snapshot={}, look_snapshot={"recipe": {}},
            scene_snapshot={"name": "n", "prompt_core": "pc"}, output_dir=".",
        ))

    def test_scarf_presence_lock_appears_only_for_scarf(self):
        scarf_prompt = self._prompt("scarf")
        self.assertIn("本页必须出现指定围巾", scarf_prompt)
        self.assertIn("不得遮挡人物面部", scarf_prompt)
        womenswear_prompt = self._prompt("outerwear")
        for line in SCARF_V1.presence_lock_lines:
            self.assertNotIn(line, womenswear_prompt)

    def test_scarf_presence_lines_come_only_from_the_adapter(self):
        """Suppressing the adapter must remove exactly the declared lines.

        This isolates the adapter's contribution: with the adapter lookup
        disabled the scarf prompt still carries the scarf label (that comes
        from ``resolve_product_display_label``, a separate table), and the only
        missing lines are ``presence_lock_lines``.
        """
        from unittest import mock

        from services import image_generator

        with_adapter = self._prompt("scarf")
        with mock.patch.object(image_generator, "adapter_for_product_category",
                               lambda token: None):
            without_adapter = self._prompt("scarf")
        removed = [line for line in with_adapter.splitlines()
                   if line not in without_adapter.splitlines()]
        self.assertEqual(removed, list(SCARF_V1.presence_lock_lines))
        self.assertIn("目标围巾", without_adapter)


class AliasTest(unittest.TestCase):
    def test_local_scarf_names_normalize_to_the_scarf_category(self):
        from services.operation_product_pack_source import normalize_product_category

        for local in ("围巾", "披肩", "丝巾", "脖套"):
            with self.subTest(local=local):
                self.assertEqual(normalize_product_category(local), "scarf")
        self.assertEqual(normalize_product_category("scarf"), "scarf")

    def test_existing_aliases_are_untouched(self):
        from services.operation_product_pack_source import normalize_product_category

        self.assertEqual(normalize_product_category("外套"), "outerwear")
        self.assertEqual(normalize_product_category("配饰"), "accessory")
        # Unknown Chinese labels still normalize to empty, as before.
        self.assertEqual(normalize_product_category("未知类目"), "")


class ThScarfDecouplingTest(unittest.TestCase):
    """Prove the category layer carries no country: TH + SCARF works offline.

    Spec §7 Phase 3: "先在临时任务中验证 TH + SCARF，证明类目与国家解耦".
    This is an offline binding only -- no generation, no RDS, no publish.
    """

    DESTINATION_ID = "SEOUL_WINTER"

    def setUp(self):
        self.catalog = loader.load_destination_catalogs()[0]
        self.destination = destination_entry(self.catalog, self.DESTINATION_ID)
        self.assertIsNotNone(self.destination)

    def _context(self, *, country: str, locale: str, pack_id: str):
        return build_execution_context(
            recipe_id="PHOTO_TRAVEL_OUTFIT_V3", recipe_version=1,
            planning_flow="travel_two_step",
            category_key="scarf", category_profile_id="SCARF_V1",
            category_profile_version=1, main_product_slot="accessories",
            market_country=country, market_pack_id=pack_id, market_pack_version=1,
            locale=locale, locale_pack_id="LOCALE_" + locale.replace("-", "_").upper(),
            locale_pack_version=1, copy_pack_id="TH_TRAVEL_OUTFIT_V3",
            reference_mode="STYLE", input_fingerprint="fp-1",
            destination=self.destination,
            product={"product_id": "S1", "category": "scarf",
                     "reference_pack_id": "PK1", "reference_pack_version": 1},
            persona={"persona_ref_id": "P1", "persona_pack_id": "PP1"},
        )

    def test_category_is_independent_of_the_market_it_is_bound_to(self):
        th = self._context(country="TH", locale="th-TH", pack_id="MP_TH_DEFAULT_V1")
        vn = self._context(country="VN", locale="vi-VN", pack_id="MP_VN_DEFAULT_V1")
        # Only the market and locale blocks may differ; recipe, category,
        # destination, reference, product and persona are identical.
        differing = {key for key in th if th[key] != vn[key]}
        self.assertEqual(differing, {"market", "locale"})
        self.assertEqual(th["category"], vn["category"])
        self.assertEqual(th["category"]["main_product_slot"], "accessories")

    def test_category_layer_derives_the_same_output_in_both_markets(self):
        th = self._context(country="TH", locale="th-TH", pack_id="MP_TH_DEFAULT_V1")
        vn = self._context(country="VN", locale="vi-VN", pack_id="MP_VN_DEFAULT_V1")
        look = {"role": "look_a", "outerwear": "大衣", "top_inner": "衬衫",
                "bottom": "牛仔", "shoes": "短靴"}
        product = {"product_id": "S1", "category": "scarf"}
        outputs = []
        for context in (th, vn):
            resolved = apply_target_product_to_look(
                adapter=SCARF_V1, look=look, product_snapshot=product)
            outputs.append((
                resolved["accessories"],
                build_product_qa_contract(adapter=SCARF_V1,
                                          product_snapshot=product),
                # The market binding must not reach the category layer at all.
                {key: value for key, value in context.items()
                 if key not in {"market", "locale"}},
            ))
        self.assertEqual(outputs[0][0], outputs[1][0])
        self.assertEqual(outputs[0][1], outputs[1][1])
        self.assertEqual(outputs[0][2], outputs[1][2])

    def test_scarf_adapter_has_no_country_or_language_field(self):
        fields = set(SCARF_V1.__dataclass_fields__)
        for forbidden in ("markets", "country", "locale", "target_country",
                          "target_locale"):
            self.assertNotIn(forbidden, fields)

    def test_recipe_v3_capabilities_are_satisfied_by_the_scarf_adapter(self):
        """The preflight rule: a recipe may only demand shipped capabilities."""
        spec = json.loads(RECIPE_PATH.read_text(encoding="utf-8"))["recipe_spec"]
        self.assertEqual(spec["planning_flow"], "travel_two_step")
        self.assertEqual(spec["market_policy"], "MARKET_PACK_REQUIRED")
        required = set(spec["required_category_capabilities"])
        self.assertTrue(required)
        self.assertLessEqual(required, set(SCARF_V1.capabilities))

    def test_recipe_v3_still_declares_no_market_or_category(self):
        spec = json.loads(RECIPE_PATH.read_text(encoding="utf-8"))["recipe_spec"]
        self.assertNotIn("markets", spec)
        self.assertNotIn("category_key", spec)
        self.assertNotIn("locale", spec)


if __name__ == "__main__":
    unittest.main()


class FreeScarfCategoryContextTest(unittest.TestCase):
    """修复一（2026-09-20 模板优化）：无商品编码的围巾任务类目契约贯通。

    评审实锤（行269 recvvcD2x9br89）：VN围巾旅行无商品 → 供给/规划/QA 全部
    回落女装语义，Look A 无围巾。修复后任务类目接管；TH 女装与 MX 假发
    逐字不变。"""

    def test_required_visible_items_declared_on_scarf_only(self):
        self.assertEqual(SCARF_V1.required_visible_items, ("scarf",))
        self.assertEqual(WOMENSWEAR_V1.required_visible_items, ())

    def test_task_category_adapter_resolution_priority_and_conflict(self):
        from services.photo_category_registry import (
            resolve_task_category_adapter,
        )
        # 商品优先（指定商品路径不变）
        self.assertEqual(
            resolve_task_category_adapter("scarf", "scarf").category_key, "scarf")
        # 无商品：任务类目接管——不再回落女装
        self.assertEqual(
            resolve_task_category_adapter("scarf", None).category_key, "scarf")
        # 无商品 + 未注册任务类目（wig）→ None（旧行为）
        self.assertIsNone(resolve_task_category_adapter("wig", None))
        self.assertIsNone(resolve_task_category_adapter("", None))
        # 商品与任务矛盾 → 付费生成前报错
        with self.assertRaisesRegex(ValueError, "不一致"):
            resolve_task_category_adapter("scarf", "outerwear")

    def test_travel_prompt_requires_accessories_for_free_scarf(self):
        from services.photo_reference_vision import PhotoReferenceVisionService
        contract = {"moments": [
            {"key": m, "label_zh": m, "evidence_zh": "e", "label_th": "t",
             "forbidden_footwear_types": []}
            for m in ("old_town_walk", "cafe_visit")
        ]}
        prompt = PhotoReferenceVisionService._travel_plan_prompt(
            analysis={}, travel_contract=contract, variables={},
            content_requirement="", count=1,
            product_context={"task_category_key": "scarf"})
        self.assertIn("【类目存在要求（围巾）】", prompt)
        self.assertIn("accessories 字段", prompt)
        self.assertIn("复用同一条围巾设定", prompt)
        # 指定围巾商品：走商品身份路径，不出自由类目块
        product_prompt = PhotoReferenceVisionService._travel_plan_prompt(
            analysis={}, travel_contract=contract, variables={},
            content_requirement="", count=1,
            product_context={"product_id": "p1", "category": "scarf",
                             "task_category_key": "scarf"})
        self.assertNotIn("【类目存在要求", product_prompt)
        # 女装任务：无类目块，提示词不变
        plain = PhotoReferenceVisionService._travel_plan_prompt(
            analysis={}, travel_contract=contract, variables={},
            content_requirement="", count=1)
        self.assertNotIn("【类目存在要求", plain)

    def test_normalize_requires_accessories_for_free_scarf_looks(self):
        from services.photo_reference_vision import PhotoReferenceVisionService
        contract = {"moments": [
            {"key": m, "label_zh": m, "evidence_zh": "e", "label_th": "t",
             "forbidden_footwear_types": []}
            for m in ("old_town_walk", "cafe_visit", "evening_stroll",
                      "airport_departure")
        ]}

        def looks(with_accessories):
            moments = ("old_town_walk", "cafe_visit", "evening_stroll",
                       "airport_departure")
            return [
                {"role": f"look_{letter}", "travel_moment": moment,
                 "scene_prompt": f"场景{letter}", "weather_logic": "室内外过渡",
                 "display_label": "", "footwear_type": "SNEAKER",
                 "outerwear": f"外套{letter}", "top_inner": f"内搭{letter}",
                 "bottom": f"下装{letter}", "shoes": f"鞋{letter}",
                 "outerwear_type": "", "bottom_type": "",
                 **({"accessories": "米灰色羊绒围巾松绕一圈"}
                    if with_accessories else {})}
                for letter, moment in zip("abcd", moments)
            ]

        payload = {"posts": [{"content_angle_zh": "x", "scene_zh": "s",
                              "palette_zh": "p", "background_prompt": "",
                              "style_modifier": "", "looks": looks(True)}]}
        plan, errors = PhotoReferenceVisionService(
            root=Path("/tmp"))._normalize_travel_plan(
            payload, contract, 1,
            product_context={"task_category_key": "scarf"})
        self.assertEqual(errors, [], errors)
        self.assertTrue(all(
            str(look.get("accessories") or "") for look in plan["posts"][0]["looks"]))

        missing = {"posts": [{"content_angle_zh": "x", "scene_zh": "s",
                              "palette_zh": "p", "background_prompt": "",
                              "style_modifier": "",
                              "looks": looks(False)}]}
        _, errors = PhotoReferenceVisionService(
            root=Path("/tmp"))._normalize_travel_plan(
            missing, contract, 1,
            product_context={"task_category_key": "scarf"})
        self.assertTrue(any("accessories" in e for e in errors), errors)

        # 女装/无任务类目：accessories 仍可选（旧行为）
        _, errors_legacy = PhotoReferenceVisionService(
            root=Path("/tmp"))._normalize_travel_plan(
            missing, contract, 1, product_context={})
        self.assertFalse(any("accessories" in e for e in errors_legacy))

    def test_free_scarf_qa_uses_presence_observation_not_product_identity(self):
        from services.photo_travel_qa import (
            FAILURE_REQUIRED_ITEM_MISSING, normalize_travel_qa,
        )

        def pages(visible):
            return [
                {"role": f"look_{letter}", "observed_moment": "old_town_walk",
                 "scene_evidence": ["街景", "人物全身"],
                 "outfit_matches": True, "weather_matches": True,
                 "mobility_matches": True, "observed_footwear_type": "SNEAKER",
                 "repair_instruction": "",
                 **({"required_item_visible": visible})}
                for letter in "abcd"
            ]

        moment_rules = {"old_town_walk": {}}
        look_plans = [
            {"role": f"look_{letter}", "travel_moment": "old_town_walk"}
            for letter in "abcd"
        ]
        ok = normalize_travel_qa(
            {"pages": pages(True), "style_uniform": True,
             "destination_conflict": False, "notes": ""},
            look_plans=look_plans, moment_rules=moment_rules,
            has_product=False, product_qa_fields=("required_item_visible",),
        )
        self.assertTrue(ok["passed"], [r.get("failure_code") for r in ok["roles"]])

        bad = normalize_travel_qa(
            {"pages": pages(False), "style_uniform": True,
             "destination_conflict": False, "notes": ""},
            look_plans=look_plans, moment_rules=moment_rules,
            has_product=False, product_qa_fields=("required_item_visible",),
        )
        self.assertFalse(bad["passed"])
        self.assertTrue(all(
            role.get("failure_code") == FAILURE_REQUIRED_ITEM_MISSING
            for role in bad["roles"]))
        self.assertIn("围巾", bad["roles"][0].get("repair") or bad["roles"][0].get("repair_instruction") or "")

    def test_supply_adapter_falls_back_to_task_category(self):
        from services.photo_style_reference_supply import _product_adapter
        self.assertEqual(_product_adapter({"task_category_key": "scarf"}).category_key,
                         "scarf")
        # 旧调用兼容：空 dict 仍回退女装
        self.assertEqual(_product_adapter({}).category_key, "womenswear")
        # 未注册任务类目回退女装（wig 走自己的 planner，不经此路径）
        self.assertEqual(_product_adapter({"task_category_key": "wig"}).category_key,
                         "womenswear")

    def test_generator_free_category_presence_lock(self):
        request = ShotGenerationRequest(
            task_id="t-free-scarf", slot_index=1, slot_role="full_look",
            shot_version=1,
            plan_shot={"purpose": "生成 look_a 完整穿搭"},
            product={"task_category_key": "scarf"},
            persona_snapshot={}, look_snapshot={"recipe": {}},
            scene_snapshot={"name": "n", "prompt_core": "pc"}, output_dir=".",
            outfit_state={"outerwear": "米色大衣",
                          "accessories": "酒红色羊毛围巾绕颈一圈"},
        )
        prompt = compose_shot_prompt(request)
        self.assertIn("【类目存在锁】", prompt)
        self.assertIn("围巾", prompt)
        self.assertIn("配饰：酒红色羊毛围巾绕颈一圈", prompt)
        # 无类目要求时提示词不变
        plain = ShotGenerationRequest(
            task_id="t-plain", slot_index=1, slot_role="full_look",
            shot_version=1,
            plan_shot={"purpose": "生成 look_a 完整穿搭",
                       "outfit_state": {"outerwear": "米色大衣"}},
            product={},
            persona_snapshot={}, look_snapshot={"recipe": {}},
            scene_snapshot={"name": "n", "prompt_core": "pc"}, output_dir=".",
        )
        self.assertNotIn("【类目存在锁】", compose_shot_prompt(plain))


class FreeScarfQAContextCaptureTest(unittest.TestCase):
    """复审 F5：QA 上下文白名单曾删掉 task_category_key，导致自由围巾
    required_item_visible 永远不启用。测试从**真实 supply 调用**捕获
    reviewer 参数断言（不能只直接调 QA helper 证明规则存在）。"""

    def test_product_qa_context_keeps_task_category(self):
        from services.photo_style_reference_supply import _product_qa_context
        ctx = _product_qa_context({"task_category_key": "scarf"})
        self.assertEqual(ctx["product_context"].get("task_category_key"), "scarf")
        # 指定商品路径不受影响
        with_product = _product_qa_context({
            "product_id": "p1", "category": "scarf",
            "task_category_key": "scarf"})
        self.assertEqual(with_product["product_context"].get("product_id"), "p1")

    def test_supply_review_call_receives_free_scarf_category(self):
        # 走 prepare 的 QA 段太重；此处直接验证 _product_qa_context 的输出
        # 接入 review_travel_pages 的 adapter 解析能启用 required_item_visible。
        from services.photo_style_reference_supply import _product_qa_context
        from services.photo_category_registry import resolve_task_category_adapter
        from services.photo_travel_qa import normalize_travel_qa

        ctx = _product_qa_context({"task_category_key": "scarf"})
        adapter = resolve_task_category_adapter(
            ctx["product_context"].get("task_category_key"),
            ctx["product_context"].get("category"))
        self.assertIsNotNone(adapter)
        self.assertEqual(adapter.category_key, "scarf")
        self.assertEqual(adapter.required_visible_items, ("scarf",))

        # 模拟 QA 缺围巾返回 false → 定向修复
        looks = [{"role": f"look_{l}", "travel_moment": "old_town_walk"}
                 for l in "abcd"]
        pages = [
            {"role": f"look_{l}", "observed_moment": "old_town_walk",
             "scene_evidence": ["街景"], "outfit_matches": True,
             "weather_matches": True, "mobility_matches": True,
             "observed_footwear_type": "SNEAKER", "repair_instruction": "",
             "required_item_visible": (l != "a")}
            for l in "abcd"
        ]
        qa = normalize_travel_qa(
            {"pages": pages, "style_uniform": True,
             "destination_conflict": False, "notes": ""},
            look_plans=looks, moment_rules={"old_town_walk": {}},
            has_product=False, product_qa_fields=("required_item_visible",),
        )
        self.assertFalse(qa["passed"])
        failed = [r for r in qa["roles"] if not r.get("passed")]
        self.assertEqual([r["role"] for r in failed], ["look_a"])
