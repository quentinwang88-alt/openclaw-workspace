"""Daily hot→cold thermal-transition flow: contract, canary and routing."""
from __future__ import annotations

import copy
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace

from PIL import Image

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from config.loader import load_content_recipes
from services.feishu_workflow import (
    FIELD_DRESS_CODE, FIELD_TRANSITION_SCENE, FIELD_TRANSITION_SENSITIVITY,
    FeishuWorkflowError, layering_approval_attributes,
    resolve_thermal_transition_variables,
)
from services.photo_asset_supply import PhotoAssetSupplyError, PhotoAssetSupplyService
from services.photo_content import freeze_content_card
from services.photo_content_planner import load_planning_policy
from services.photo_package import NativePhotoProductionFlow, PhotoPackageError
from services.photo_theme import resolve_photo_theme
from services.photo_thermal_transition_contract import (
    BASE_OUTFIT_DRIFT, CONTEXT_ORDER_MISMATCH, ITEM_SLOT_DUPLICATED,
    TEMPERATURE_VALUE_UNSOURCED, THERMAL_TRANSITION_FLOW,
    TRANSITION_NOT_VISIBLE, UNKNOWN_THERMAL_CONTEXT,
    ThermalTransitionContractError, validate_thermal_transition_plan,
)
from services.photo_thermal_transition_flow import (
    PhotoThermalTransitionFlowError, build_thermal_transition_content_plan,
)
from scripts.run_thermal_transition_canary import (
    CANARY_VARIABLES, ROLE_ORDER, build_canary_plan, run_canary,
)


RECIPE_ID = "PHOTO_TH_THERMAL_TRANSITION_V1"
THEME_LABEL = "冷热切换"

CONTEXTS = {"base": "outdoor_hot", "mid": "transit_cool", "outer": "office_cold"}
STACKS = {
    "base": ["transition_core_base"],
    "mid": ["transition_core_base", "transition_light_shirt"],
    "outer": [
        "transition_core_base", "transition_light_shirt",
        "transition_light_blazer",
    ],
}
ITEMS = {
    "base": ["breathable_top", "trousers", "loafer"],
    "mid": ["breathable_top", "cotton_shirt", "trousers", "loafer"],
    "outer": [
        "breathable_top", "cotton_shirt", "light_blazer", "trousers", "loafer",
    ],
}


def recipe():
    return next(
        item for item in load_content_recipes() if item.recipe_id == RECIPE_ID
    )


def offline_observation():
    """A reviewed offline observation; never used as a business asset proof."""
    return {
        "thermal_index": 0,
        "notes": "同人同机位，离线人工观察样本",
        "pages": [{
            "role": role,
            "observed_thermal_context": CONTEXTS[role],
            "visible_layer_count": len(STACKS[role]),
            "visible_layer_stack": list(STACKS[role]),
            "observed_item_types": list(ITEMS[role]),
            "layer_evidence": ["可见层次清楚"],
            "identity_id": "person_1",
            "camera_signature": "camera_1",
            "full_body": True,
            "collar_compatible": True,
            "sleeve_conflict": False,
            "hem_conflict": False,
            "temperature_claim_matches": True,
            "temperature_claim_text": "",
            "observed_base_signature": "core_base",
            "observed_bottom_signature": "bottom_main",
            "observed_shoes_signature": "shoes_main",
            "added_garment_visible": True,
            "person_flags": {
                "face_or_limb_deformity": False, "obvious_unnatural_tilt": False,
            },
            "repair_instruction": "",
        } for role in ROLE_ORDER],
    }


def build_plan(variables=None):
    loaded = recipe()
    spec = dict(loaded.recipe_spec_json or {})
    return loaded, spec, build_thermal_transition_content_plan(
        record_id="thermal-canary", recipe_id=RECIPE_ID, recipe_spec=spec,
        policy=load_planning_policy(RECIPE_ID),
        theme=resolve_photo_theme(THEME_LABEL), reference_mode="COMPLETE_LOOK",
        variables=dict(variables or CANARY_VARIABLES),
    )


class _Client:
    @staticmethod
    def download_attachment_bytes(attachment):
        value = int(attachment["file_token"])
        stream = io.BytesIO()
        Image.new("RGB", (120, 180), (60 + value, 90, 120)).save(stream, "PNG")
        data = stream.getvalue()
        return data, f"{value}.png", "image/png", len(data)


class _Repo:
    def __init__(self):
        self.assets = []

    def list_asset_sets(self, **_kwargs):
        return list(self.assets)

    def get_asset_set(self, identity):
        return next(
            (item for item in self.assets if item.asset_set_id == identity), None
        )

    def upsert_asset_set(self, item):
        self.assets.append(item)


class ThermalTransitionFlowTest(unittest.TestCase):
    def test_operator_fields_freeze_one_explicit_transition_profile(self):
        self.assertEqual(resolve_thermal_transition_variables({
            FIELD_TRANSITION_SCENE: "室外热→BTS→办公室空调",
            FIELD_TRANSITION_SENSITIVITY: "正常体感",
            FIELD_DRESS_CODE: "办公室",
        }), {
            "transition_key": "outdoor_bts_office",
            "thermal_sensitivity": "normal",
            "dress_code": "office",
            "style_series": "minimal_city",
            "temperature_label_mode": "QUALITATIVE",
        })
        with self.assertRaisesRegex(FeishuWorkflowError, "冷热切换必须填写"):
            resolve_thermal_transition_variables({
                FIELD_TRANSITION_SCENE: "室外热→商场→影院",
                FIELD_TRANSITION_SENSITIVITY: "",
                FIELD_DRESS_CODE: "校园",
            })

    def test_plan_freezes_roles_copy_and_thermal_contexts(self):
        _loaded, spec, plan = build_plan()
        post = plan["items"][0]
        self.assertEqual(plan["flow"], THERMAL_TRANSITION_FLOW)
        self.assertEqual(plan["count"], 1)
        self.assertEqual(post["required_roles"], list(ROLE_ORDER))
        self.assertEqual(post["presentation_order"], list(ROLE_ORDER))
        self.assertEqual(post["generation_order"], list(reversed(ROLE_ORDER)))
        self.assertEqual(
            [look["role"] for look in post["looks"]], list(ROLE_ORDER)
        )
        self.assertEqual(
            [look["thermal_context"] for look in post["looks"]],
            [CONTEXTS[role] for role in ROLE_ORDER],
        )
        self.assertEqual(
            [look["expected_visible_layer_count"] for look in post["looks"]],
            [1, 2, 3],
        )
        self.assertEqual(post["profile_binding"]["transition_key"], "outdoor_bts_office")
        self.assertEqual(
            post["profile_binding"]["core_base_garment_id"], "transition_core_base"
        )
        self.assertEqual(
            post["reference_uses_consumed"], ["LAYER_PROGRESSION", "OUTFIT"]
        )
        # Every contract token must already be resolved and localized.
        for field in ("title", "caption", "cta"):
            self.assertNotIn("{", str(post["copy"].get(field) or ""))
        self.assertTrue(all(
            "{" not in text for text in post["copy"]["slide_texts"]
        ))
        self.assertEqual(post["copy"]["language_review_status"], "DRAFT")
        # The shared structural QA contract is derived, not duplicated.
        self.assertEqual(
            post["style_profile"]["layering_contract"]["source_roles"],
            list(ROLE_ORDER),
        )
        self.assertEqual(
            post["style_profile"]["layering_contract"]["bands"][0]["key"],
            "outdoor_bts_office",
        )
        self.assertEqual(
            spec["thermal_transition_contract"]["schema_version"],
            "opv-photo-thermal-transition-contract-v1",
        )

    def test_phase_one_canary_rejects_unimplemented_combinations(self):
        loaded = recipe()
        policy = load_planning_policy(RECIPE_ID)
        base = dict(CANARY_VARIABLES)
        closed = {
            "transition_key": {"outdoor_mall_cinema", "campus_outdoor_classroom"},
            "thermal_sensitivity": {"feels_cold", "feels_warm"},
            "dress_code": {"campus", "weekend"},
            "style_series": {"soft_casual"},
            "temperature_label_mode": {"EXPLICIT_INPUT"},
        }
        for key, offenders in closed.items():
            for offender in offenders:
                with self.subTest(variable=key, value=offender):
                    with self.assertRaisesRegex(
                        PhotoThermalTransitionFlowError, "Phase 1 canary"
                    ):
                        build_thermal_transition_content_plan(
                            record_id="thermal-closed", recipe_id=RECIPE_ID,
                            recipe_spec=loaded.recipe_spec_json, policy=policy,
                            theme=resolve_photo_theme(THEME_LABEL),
                            reference_mode="COMPLETE_LOOK",
                            variables={**base, key: offender},
                        )

    def test_style_reference_mode_is_closed_in_phase_one(self):
        loaded = recipe()
        with self.assertRaisesRegex(PhotoThermalTransitionFlowError, "不支持参考模式"):
            build_thermal_transition_content_plan(
                record_id="thermal-style", recipe_id=RECIPE_ID,
                recipe_spec=loaded.recipe_spec_json,
                policy=load_planning_policy(RECIPE_ID),
                theme=resolve_photo_theme(THEME_LABEL), reference_mode="STYLE",
                variables=dict(CANARY_VARIABLES),
            )

    def test_theme_must_match_the_planning_policy(self):
        loaded = recipe()
        with self.assertRaisesRegex(PhotoThermalTransitionFlowError, "主题"):
            build_thermal_transition_content_plan(
                record_id="thermal-theme", recipe_id=RECIPE_ID,
                recipe_spec=loaded.recipe_spec_json,
                policy=load_planning_policy(RECIPE_ID),
                theme=resolve_photo_theme("温度穿搭"), reference_mode="COMPLETE_LOOK",
                variables=dict(CANARY_VARIABLES),
            )

    def test_plan_validator_rejects_broken_frozen_plans(self):
        _loaded, spec, plan = build_plan()
        contract = spec["thermal_transition_contract"]

        def rejected(mutate):
            broken = copy.deepcopy(plan)
            mutate(broken)
            with self.assertRaises(ThermalTransitionContractError):
                validate_thermal_transition_plan(
                    broken, thermal_transition_contract=contract
                )

        rejected(lambda value: value["items"][0]["looks"][1].update(
            thermal_context="sauna_hot"))
        rejected(lambda value: value["items"][0]["looks"].__setitem__(
            1, {**value["items"][0]["looks"][1], "thermal_context": "office_cold"}))
        rejected(lambda value: value["items"][0]["looks"][1].update(
            expected_visible_layer_count=3))
        rejected(lambda value: value["items"][0].update(
            generation_order=list(ROLE_ORDER)))
        rejected(lambda value: value["items"][0].update(
            required_roles=["mid", "base", "outer"]))
        rejected(lambda value: value["items"][0]["looks"][2]["items"].append(
            {"slot": "bottom", "garment_id": "other", "item_type": "jeans"}))
        rejected(lambda value: value["items"][0]["looks"][2]["items"].append(
            {"slot": "extra", "garment_id": "puffer", "item_type": "puffer_jacket"}))
        rejected(lambda value: value["items"][0].update(copy={
            **value["items"][0]["copy"], "title": "32°C ถึง 18°C"}))
        # The unmodified plan must still validate.
        validate_thermal_transition_plan(plan, thermal_transition_contract=contract)

    def test_duplicate_slot_items_are_named_explicitly(self):
        _loaded, spec, plan = build_plan()
        broken = copy.deepcopy(plan)
        broken["items"][0]["looks"][2]["items"].append(
            {"slot": "shoes", "garment_id": "other_shoes", "item_type": "sneaker"}
        )
        with self.assertRaisesRegex(ThermalTransitionContractError, ITEM_SLOT_DUPLICATED):
            validate_thermal_transition_plan(
                broken, thermal_transition_contract=spec["thermal_transition_contract"]
            )

    def test_failure_codes_are_routable(self):
        for code in (
            UNKNOWN_THERMAL_CONTEXT, CONTEXT_ORDER_MISMATCH,
            TEMPERATURE_VALUE_UNSOURCED, BASE_OUTFIT_DRIFT,
            TRANSITION_NOT_VISIBLE, ITEM_SLOT_DUPLICATED,
        ):
            self.assertRegex(code, r"^[A-Z][A-Z_]+$")

    def test_asset_supply_freezes_transition_tags_and_content_card(self):
        loaded, _spec, plan = build_plan()
        post = plan["items"][0]
        with tempfile.TemporaryDirectory() as tmp:
            supply = PhotoAssetSupplyService(_Client(), root=Path(tmp))
            supply.stage(
                record_id="thermal-assets", required_roles=list(ROLE_ORDER),
                attachments=[{"file_token": str(index)} for index in range(1, 4)],
            )
            repo = _Repo()
            evidence = _thermal_qa_payload()
            saved = supply.qualify(
                record_id="thermal-assets", recipe=loaded, repository=repo,
                reviewer="system_thermal_transition_semantic_qa",
                reviewer_type="model",
                source="feishu_complete_look_input",
                profile_binding=post["profile_binding"],
                approval_attributes=layering_approval_attributes(evidence),
                approval_evidence=evidence,
            )
            self.assertEqual(
                saved.tags_json["use_cases"], ["thermal_transition"]
            )
            self.assertEqual(
                saved.manifest_json["profile_binding"]["profile_id"],
                post["profile_binding"]["profile_id"],
            )
            frozen = freeze_content_card(
                loaded.recipe_spec_json["content_card"], saved,
                loaded.recipe_spec_json["visual_rules"],
            )
            self.assertEqual(frozen["layering_roles"], list(ROLE_ORDER))
            self.assertEqual(
                [page["layout"] for page in frozen["pages"]],
                ["triptych_3", "single", "single", "single", "triptych_3"],
            )
            self.assertEqual(
                [page["source_roles"] for page in frozen["pages"]],
                [list(ROLE_ORDER), ["base"], ["mid"], ["outer"], list(ROLE_ORDER)],
            )
            self.assertEqual(
                frozen["pages"][0]["column_labels"],
                ["ข้างนอกร้อน", "รถไฟฟ้า/ห้างเย็น", "ออฟฟิศแอร์แรง"],
            )
            # The same source photos must not be re-registered as another family.
            supply.stage(
                record_id="thermal-assets-other", required_roles=list(ROLE_ORDER),
                attachments=[{"file_token": str(index)} for index in range(1, 4)],
            )
            with self.assertRaisesRegex(PhotoAssetSupplyError, "不得复用相同源图"):
                supply.qualify(
                    record_id="thermal-assets-other", recipe=loaded,
                    repository=repo,
                    profile_binding={
                        **post["profile_binding"], "family_key": "another_family",
                    },
                    approval_attributes=layering_approval_attributes(evidence),
                    approval_evidence=evidence,
                )

    def test_final_page_qa_routes_thermal_transition_and_blocks_on_failure(self):
        seen = {}

        class Vision:
            def review_layering_final_pages(self, **kwargs):
                seen.update(kwargs)
                return {
                    "schema_version": "opv-photo-layering-final-qa-v1",
                    "passed": False,
                    "pages": [{
                        "index": 1, "text_readable": True,
                        "text_matches_expected": True, "text_clipped": True,
                        "text_garbled": False, "subject_obscured": False,
                        "issues": ["封面文字溢出底板"],
                    }],
                }

        class Repo:
            def __init__(self):
                self.saved = None

            def update_content_package(self, package_id, **values):
                self.saved = (package_id, values)

        repo = Repo()
        flow = NativePhotoProductionFlow(
            repo, None, output_root=Path("/tmp"), vision_service=Vision()
        )
        manifest = {
            "content_package_id": "pkg-thermal",
            "copy": {"slide_texts": ["a", "b", "c", "d", "e"]},
            "slides": [{"path": f"/tmp/page-{index}.jpg"} for index in range(5)],
        }
        with self.assertRaisesRegex(PhotoPackageError, "冷热切换最终页面 QA 未通过"):
            flow._run_final_page_qa(SimpleNamespace(recipe_id=RECIPE_ID), manifest)
        self.assertEqual(
            seen["role_order"],
            ["hook", "state_base", "state_mid", "state_outer", "cta"],
        )
        self.assertIn("thermal_transition_final_page_qa", manifest)
        self.assertEqual(repo.saved[0], "pkg-thermal")

    def test_local_canary_renders_five_pages_and_stays_unreleased(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = []
            for index, color in enumerate(
                ((232, 229, 223), (214, 205, 190), (188, 174, 158)), 1
            ):
                path = root / f"source-{index}.png"
                Image.new("RGB", (540, 960), color).save(path)
                paths.append(path)
            result = run_canary(
                base=paths[0], mid=paths[1], outer=paths[2],
                output_dir=root / "canary", observation=offline_observation(),
            )
            self.assertEqual(result["status"], "RENDERED")
            self.assertEqual(result["failed_roles"], [])
            self.assertEqual(
                result["thermal_contexts"]["expected"],
                [CONTEXTS[role] for role in ROLE_ORDER],
            )
            self.assertEqual(
                [slide["layout"] for slide in result["slides"]],
                ["triptych_3", "single", "single", "single", "triptych_3"],
            )
            self.assertEqual(
                [slide["source_roles"] for slide in result["slides"]],
                [list(ROLE_ORDER), ["base"], ["mid"], ["outer"], list(ROLE_ORDER)],
            )
            self.assertTrue(all(
                Path(slide["path"]).is_file() for slide in result["slides"]
            ))
            self.assertFalse(
                result["release_ready"], "DRAFT 泰语不得误报可发布"
            )
            markdown = Path(result["qa_report_path"]).read_text(encoding="utf-8")
            self.assertIn("冷热切换 Canary QA", markdown)
            self.assertIn("outdoor_hot", markdown)

    def test_canary_rejects_identical_source_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = root / "a.png"
            Image.new("RGB", (540, 960), (200, 200, 200)).save(first)
            # A byte-identical copy under a different name is still a duplicate.
            duplicate = root / "b.png"
            duplicate.write_bytes(first.read_bytes())
            outer = root / "c.png"
            Image.new("RGB", (540, 960), (10, 10, 10)).save(outer)
            with self.assertRaisesRegex(ValueError, "三张内容不同"):
                run_canary(
                    base=first, mid=duplicate, outer=outer,
                    output_dir=root / "canary",
                    observation=offline_observation(),
                )

    def test_canary_builder_only_opens_the_single_canary_profile(self):
        _recipe, spec, plan = build_canary_plan()
        self.assertEqual(len(spec["execution_profiles"]), 1)
        self.assertEqual(
            spec["execution_profiles"][0]["variables"]["transition_key"],
            "outdoor_bts_office",
        )
        self.assertEqual(
            plan["items"][0]["profile_binding"]["transition_key"],
            "outdoor_bts_office",
        )


def _thermal_qa_payload():
    """Minimal passing evidence for asset qualification (schema-compatible)."""
    return {
        "schema_version": "opv-photo-layering-semantic-qa-v1",
        "passed": True,
        "roles": [{
            "role": role, "passed": True, "failure_codes": [],
            "identity_id": "person-1", "camera_signature": "camera-1",
            "full_body": True, "visible_layer_count": len(STACKS[role]),
            "visible_layer_stack": list(STACKS[role]),
            "observed_item_types": ["breathable_top"],
            "stackability": {
                "collar_compatible": True, "sleeve_conflict": False,
                "hem_conflict": False,
            },
        } for role in ROLE_ORDER],
    }


if __name__ == "__main__":
    unittest.main()
