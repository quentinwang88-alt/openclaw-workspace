#!/usr/bin/env python3

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from domain.models import AccountProfile
from services.batch_diversity_planner import BatchDiversityPlanner
from services.content_planner import ContentPlannerService
from services.feishu_workflow import PresetTask


class FakeRepo:
    def __init__(self):
        self.account = AccountProfile(
            account_id="A", account_code="a", account_name="a",
            target_country="TH", default_locale="th-TH", timezone="Asia/Bangkok",
            status="testing", persona_ref_id="P1",
            allowed_look_refs_json=[f"L{index}" for index in range(1, 7)],
            allowed_scene_refs_json=["SCENE_A_001", "ENV_CAFE_001", "ENV_AIRPORT_DEPART_001"],
            core_scene_refs_json=["SCENE_A_001"],
            operating_rules_json={"allowed_persona_refs": ["P1", "P2", "P3"]},
        )
        self.recipes = {
            f"R{index}": SimpleNamespace(
                hook_types_json=["final_effect", "curiosity", "contrast"]
            )
            for index in range(1, 4)
        }

    def get_account_profile(self, account_id):
        return self.account

    def get_content_recipe(self, recipe_id):
        return self.recipes[recipe_id]


class FakeAssets:
    silhouettes = [
        "WHITE_WIDE_PANTS", "BLUE_DISTRESSED_DENIM", "DARK_STRAIGHT_PANTS",
        "BLACK_SHORT_BOTTOM", "SOFT_SKIRT", "TRAVEL_CASUAL",
    ]

    def get_look(self, ref_id):
        index = int(ref_id[1:]) - 1
        return {
            "ref_id": ref_id,
            "silhouette_key": self.silhouettes[index],
            "applicable_product_codes": ["*"],
            "compatibility": {"product_types": ["outerwear"]},
            "recipe": {"bottom": self.silhouettes[index]},
        }


def specs():
    # Mirrors the shipped three-recipe preset: bedroom / cafe / bedroom.
    scene_refs = ["SCENE_A_001", "ENV_CAFE_001", "SCENE_A_001"]
    return [
        PresetTask(
            account_id="A", market="TH", language="th-TH",
            recipe_id=f"R{(index % 3) + 1}", theme_id=f"T{(index % 3) + 1}",
            hook_strategy="final_effect", persona_ref="P1", look_ref="",
            scene_ref=scene_refs[index % 3],
        )
        for index in range(9)
    ]


def products():
    return [
        {
            "product_id": "1737141103233042426",
            "product_name": "短款外套",
            "category": "outerwear",
            "reference_pack_id": "PACK_A" if index % 2 == 0 else "PACK_B",
            "reference_pack_version": 1,
            "reference_images": ["/tmp/ref.jpg"],
        }
        for index in range(9)
    ]


class BatchDiversityPlannerTest(unittest.TestCase):
    def test_no_compatible_look_never_falls_back_to_model_invention(self):
        repo = FakeRepo()
        repo.account.allowed_look_refs_json = []
        with self.assertRaisesRegex(ValueError, "穿搭模板"):
            BatchDiversityPlanner(repo, FakeAssets()).plan(record_id="empty", specs=specs()[:1], products=products()[:1])

    def test_scene_family_restriction_is_enforced_in_batch_assignment(self):
        from dataclasses import replace
        class AirportAssets(FakeAssets):
            def get_look(self, ref):
                result = super().get_look(ref)
                result["compatibility"]["scene_families"] = ["airport_departure_travel"]
                return result
        planner = BatchDiversityPlanner(FakeRepo(), AirportAssets())
        with self.assertRaisesRegex(ValueError, "兼容"):
            planner.plan(record_id="x", specs=specs()[:1], products=products()[:1])
        batch = planner.plan(record_id="x", specs=[replace(s, scene_ref="") for s in specs()], products=products())
        self.assertEqual({a.spec.scene_ref for a in batch}, {"ENV_AIRPORT_DEPART_001"})

    def test_recent_history_avoids_previous_combination_and_is_stable(self):
        repo = FakeRepo()
        first = BatchDiversityPlanner(repo, FakeAssets()).plan(record_id="same-offset", specs=specs()[:1], products=products()[:1])
        repo.list_recent_diversity_axes = lambda account_id, **kwargs: [first[0].metadata["axes"]]
        second = BatchDiversityPlanner(repo, FakeAssets()).plan(record_id="same-offset", specs=specs()[:1], products=products()[:1])
        self.assertNotEqual(first[0].metadata["combination_key"], second[0].metadata["combination_key"])
        self.assertEqual(second[0].metadata["history_records_considered"], 1)
        self.assertEqual(second[0].metadata["historical_repeat_count"], 0)

    def test_unreadable_rotating_persona_is_removed_before_planning(self):
        class Personas(FakeAssets):
            def get_persona(self, ref):
                return {"status": "testing", "local_reference_images": []}
        with self.assertRaisesRegex(ValueError, "人物池"):
            BatchDiversityPlanner(FakeRepo(), Personas()).plan(record_id="x", specs=specs()[:1], products=products()[:1])

    def test_nine_item_acceptance_plan_is_unique_balanced_and_stable(self):
        planner = BatchDiversityPlanner(FakeRepo(), FakeAssets())
        first = planner.plan(record_id="rec-batch-9", specs=specs(), products=products())
        retry = planner.plan(record_id="rec-batch-9", specs=specs(), products=products())
        self.assertEqual(
            [row.metadata for row in first], [row.metadata for row in retry]
        )
        keys = [row.metadata["combination_key"] for row in first]
        self.assertEqual(len(set(keys)), 9)
        self.assertTrue(all(not row.metadata["fallback_used"] for row in first))
        grammar_ids = [row.metadata["axes"]["shot_grammar"] for row in first]
        self.assertEqual({value: grammar_ids.count(value) for value in set(grammar_ids)}, {
            "G1": 3, "G2": 3, "G3": 3,
        })
        personas = [row.metadata["axes"]["persona_ref"] for row in first]
        self.assertEqual(sorted(personas.count(value) for value in set(personas)), [3, 3, 3])
        self.assertGreaterEqual(
            len({row.metadata["axes"]["silhouette_key"] for row in first}), 4
        )
        self.assertGreaterEqual(
            len({
                (row.metadata["axes"]["scene_ref"], row.metadata["axes"]["scene_zone"])
                for row in first
            }),
            5,
        )
        self.assertTrue(all(
            assignment.spec.scene_ref == original.scene_ref
            for assignment, original in zip(first, specs())
        ))
        pack_ids = [row.metadata["axes"]["reference_pack_id"] for row in first]
        self.assertEqual(sorted(pack_ids.count(value) for value in set(pack_ids)), [4, 5])
        title_keys = [
            f"{row.spec.theme_id}:{row.product_snapshot['planned_title_suffix']}"
            for row in first
        ]
        self.assertEqual(len(set(title_keys)), 9)
        templates = {
            "T1": "สาวตัวเล็กต้องรู้: {product}",
            "T2": "ลุคนัดคาเฟ่: {product}",
            "T3": "เดรสตัวเดียว 3 ลุค: {product}",
        }
        content_planner = ContentPlannerService(repository=None)
        titles = [
            content_planner._topic_from_template(
                SimpleNamespace(default_storyboard_json={
                    "topic_template": templates[row.spec.theme_id]
                }),
                row.product_snapshot,
            )
            for row in first
        ]
        self.assertEqual(len(set(titles)), 9)
        self.assertTrue(all("1737141103233042426" not in title for title in titles))
        self.assertTrue(all("เดรส" not in title for title in titles))

    def test_missing_asset_variety_degrades_without_blocking(self):
        repo = FakeRepo()
        repo.account.allowed_look_refs_json = ["L1"]
        repo.account.allowed_scene_refs_json = ["SCENE_A_001"]
        repo.account.operating_rules_json = {"allowed_persona_refs": ["P1"]}
        repo.recipes["R1"].hook_types_json = ["final_effect"]
        small_specs = [
            PresetTask(
                account_id="A", market="TH", language="th-TH",
                recipe_id="R1", theme_id="T1", hook_strategy="final_effect",
                persona_ref="P1", look_ref="L1", scene_ref="SCENE_A_001",
            )
            for _ in range(9)
        ]
        small_products = [{**products()[0], "reference_pack_id": "PACK_A"} for _ in range(9)]
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "small.json"
            config_path.write_text(json.dumps({
                "profile_id": "SMALL",
                "shot_grammars": {"G1": {"shots": [], "title_suffixes_th": ["a"]}},
                "scene_zones": {"SCENE_A_001": ["主区域"]},
            }), encoding="utf-8")
            planner = BatchDiversityPlanner(repo, FakeAssets(), config_path=config_path)
            batch = planner.plan(
                record_id="small-pool", specs=small_specs, products=small_products
            )
            retry = planner.plan(
                record_id="small-pool", specs=small_specs, products=small_products
            )
        self.assertEqual(len(batch), 9)
        self.assertTrue(any(row.metadata["fallback_used"] for row in batch))
        self.assertEqual(
            [row.metadata["combination_key"] for row in batch],
            [row.metadata["combination_key"] for row in retry],
        )


if __name__ == "__main__":
    unittest.main()
