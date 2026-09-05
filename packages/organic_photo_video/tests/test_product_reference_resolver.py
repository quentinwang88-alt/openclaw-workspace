#!/usr/bin/env python3

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from domain.models import ProductReferencePack
from services.product_reference_resolver import (
    ProductReferenceResolutionError,
    ProductReferenceResolver,
    has_detail_reference,
    select_product_references_for_slot,
)
from services.operation_product_pack_source import OperationProductPackCandidate
from services.image_generator import ShotGenerationRequest, compose_shot_prompt
from services.outfit_planner import build_outfit_plan, build_product_facts


class FakeRepo:
    def __init__(self):
        self.packs = []
        self.candidates = []

    def list_product_reference_packs(self, product_id):
        return [row for row in self.packs if row.product_id == product_id]

    def upsert_product_reference_pack(self, pack):
        self.packs.append(pack)

    def list_product_snapshot_candidates(self, product_id, limit=20):
        return list(self.candidates)[:limit]


class ProductReferenceResolverTest(unittest.TestCase):
    def test_history_prefers_less_used_pack_and_freezes_history_per_batch(self):
        first = self.resolver.build_pack(product_id="P1", product_name="coat", category="outerwear", references=[self.paths[0]], variant_key="a", persist=True)
        second = self.resolver.build_pack(product_id="P1", product_name="coat", category="outerwear", references=[self.paths[1]], variant_key="b", persist=True)
        history = [{"reference_pack_id": first.pack_id}] * 10
        self.repo.list_recent_diversity_axes = lambda *args, **kwargs: history
        snapshot = self.resolver.resolve_snapshot("P1", selection_key="batch:1", account_id="A", allow_history_bootstrap=False)
        self.assertEqual(snapshot["reference_pack_id"], second.pack_id)
        selected = [self.resolver.resolve_snapshot("P1", selection_key=f"batch:{i}", account_id="A", allow_history_bootstrap=False)["reference_pack_id"] for i in range(1, 10)]
        self.assertEqual([selected.count(first.pack_id), selected.count(second.pack_id)], [4, 5])
        history[:] = [{"reference_pack_id": second.pack_id}] * 20
        retry = self.resolver.resolve_snapshot("P1", selection_key="batch:1", account_id="A", allow_history_bootstrap=False)
        self.assertEqual(snapshot, retry)

    def test_modified_pack_file_requires_reimport(self):
        self.resolver.build_pack(product_id="P1", product_name="coat", category="outerwear", references=[self.paths[0]], variant_key="a", persist=True)
        Path(self.paths[0]).write_bytes(b"changed")
        with self.assertRaisesRegex(ProductReferenceResolutionError, "重新入库"):
            self.resolver.resolve_snapshot("P1", allow_history_bootstrap=False)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.paths = []
        for index in range(1, 5):
            path = self.root / f"ref_{index}.jpg"
            path.write_bytes(f"image-{index}".encode())
            self.paths.append(str(path))
        self.repo = FakeRepo()
        self.resolver = ProductReferenceResolver(self.repo)

    def tearDown(self):
        self.temp.cleanup()

    def test_operator_pack_has_roles_and_no_score(self):
        pack = self.resolver.build_pack(
            product_id="P1", product_name="coat", category="outerwear",
            references=self.paths[:3], explicit_roles=["back", "front", "lifestyle"],
            variant_key="light_blue", is_default=True,
        )
        self.assertEqual(pack.status, "limited")
        self.assertEqual([row["role"] for row in pack.assets_json], [
            "back", "front", "lifestyle",
        ])
        self.assertNotIn("quality_score", pack.to_row())

    def test_full_role_set_is_ready_and_deduplicates_bytes(self):
        duplicate = self.root / "duplicate.jpg"
        duplicate.write_bytes(Path(self.paths[0]).read_bytes())
        pack = self.resolver.build_pack(
            product_id="P1", product_name="coat", category="outerwear",
            references=self.paths + [str(duplicate)],
            explicit_roles=["front", "back", "lifestyle", "detail", "detail"],
        )
        self.assertEqual(pack.status, "ready")
        self.assertEqual(len(pack.assets_json), 4)

    def test_default_pack_wins_and_ambiguous_multi_pack_blocks(self):
        self.repo.packs = [
            ProductReferencePack("a", "P1", variant_key="blue", status="ready"),
            ProductReferencePack("b", "P1", variant_key="black", status="limited"),
        ]
        with self.assertRaisesRegex(ProductReferenceResolutionError, "没有默认组"):
            self.resolver.resolve_snapshot("P1", allow_history_bootstrap=False)
        self.repo.packs[1].is_default = True
        self.repo.packs[1].assets_json = [{
            "local_path": self.paths[0], "role": "front", "usable": True,
        }]
        result = self.resolver.resolve_snapshot("P1", allow_history_bootstrap=False)
        self.assertEqual(result["variant_key"], "black")

    def test_batch_key_rotates_stably_and_latest_version_wins(self):
        def pack(pack_id, variant, version, image):
            return ProductReferencePack(
                pack_id, "P1", variant_key=variant, pack_version=version,
                status="ready", assets_json=[{
                    "local_path": image, "role": "front", "usable": True,
                }],
            )
        self.repo.packs = [
            pack("blue-v1", "blue", 1, self.paths[0]),
            pack("blue-v2", "blue", 2, self.paths[1]),
            pack("black-v1", "black", 1, self.paths[2]),
        ]
        first = self.resolver.resolve_snapshot(
            "P1", selection_key="rec:1", allow_history_bootstrap=False
        )
        retry = self.resolver.resolve_snapshot(
            "P1", selection_key="rec:1", allow_history_bootstrap=False
        )
        self.assertEqual(first["reference_pack_id"], retry["reference_pack_id"])
        self.assertNotEqual(first["reference_pack_id"], "blue-v1")
        explicit = self.resolver.resolve_snapshot(
            "P1", variant_key="blue", allow_history_bootstrap=False
        )
        self.assertEqual(explicit["reference_pack_id"], "blue-v2")

    def test_snapshot_freezes_pack_and_slot_reference_roles(self):
        pack = self.resolver.build_pack(
            product_id="P1", product_name="coat", category="outerwear",
            references=self.paths, explicit_roles=["front", "back", "lifestyle", "detail"],
            variant_key="blue", is_default=True, persist=True,
        )
        product = self.resolver.resolve_snapshot("P1", allow_history_bootstrap=False)
        self.assertEqual(product["reference_pack_id"], pack.pack_id)
        self.assertEqual(product["visual_qa_policy"], "operator_preview_only")
        self.assertEqual(
            select_product_references_for_slot(product, "detail"),
            [str(Path(self.paths[0]).resolve()), str(Path(self.paths[3]).resolve())],
        )
        self.assertTrue(has_detail_reference(product))

    def test_history_bootstrap_is_a_persisted_limited_pack(self):
        self.repo.candidates = [{
            "task_id": "old-task",
            "product": {
                "product_name": "coat", "category": "outerwear",
                "reference_images": self.paths[:2],
            },
        }]
        product = self.resolver.resolve_snapshot("P1")
        self.assertEqual(product["reference_status"], "limited")
        self.assertEqual(len(self.repo.packs), 1)
        self.assertEqual(self.repo.packs[0].source_type, "opv_history_bootstrap")

    def test_operation_rows_supplement_manual_pack_and_dedupe_by_content(self):
        manual = self.resolver.build_pack(
            product_id="P1", product_name="coat", category="outerwear",
            references=self.paths[:2], variant_key="manual-blue",
            is_default=True, persist=True,
        )

        class Source:
            calls = 0

            def list_candidates(inner_self, product_id):
                inner_self.calls += 1
                return [
                    OperationProductPackCandidate(
                        "rec-duplicate", product_id, self.paths[:2]
                    ),
                    OperationProductPackCandidate(
                        "rec-distinct", product_id, self.paths[2:]
                    ),
                ]

        source = Source()
        resolver = ProductReferenceResolver(self.repo, candidate_source=source)
        resolver.resolve_snapshot("P1", selection_key="batch:1")
        resolver.resolve_snapshot("P1", selection_key="batch:2")
        self.assertEqual(source.calls, 1)
        self.assertEqual(len(self.repo.packs), 2)
        imported = next(row for row in self.repo.packs if row.pack_id != manual.pack_id)
        self.assertEqual(imported.source_type, "feishu_short_video_operation")
        self.assertEqual(imported.source_ref, "rec-distinct")

    def test_consecutive_batch_slots_cycle_through_all_packs(self):
        for index, path in enumerate(self.paths[:3], start=1):
            self.resolver.build_pack(
                product_id="P1", product_name="coat", category="outerwear",
                references=[path], variant_key=f"v{index}", persist=True,
            )
        selected = [
            self.resolver.resolve_snapshot(
                "P1", selection_key=f"record-1:{index}",
                allow_history_bootstrap=False,
            )["reference_pack_id"]
            for index in range(1, 7)
        ]
        self.assertEqual(len(set(selected[:3])), 3)
        self.assertEqual(selected[:3], selected[3:])

    def test_operation_pack_inherits_category_but_not_sibling_variant_name(self):
        authoritative = self.resolver.build_pack(
            product_id="1737141103233042426", product_name="浅蓝色短款蓬松外套",
            category="outerwear", references=[self.paths[0]],
            variant_key="manual", is_default=True, persist=True,
        )
        imported = self.resolver.build_pack(
            product_id="1737141103233042426",
            product_name="浅蓝色短款蓬松外套", category="",
            references=[self.paths[1]], variant_key="operation_rec",
            source_type="feishu_short_video_operation", persist=True,
        )
        result = self.resolver.resolve_snapshot(
            "1737141103233042426", variant_key=imported.variant_key,
            allow_history_bootstrap=False,
        )
        self.assertEqual(result["product_name"], "目标外套")
        self.assertEqual(result["category"], "outerwear")
        result["color"] = "brown"  # explicit fact owned by the selected pack
        facts = build_product_facts(result)
        outfit = build_outfit_plan(
            outfit_plan_id="brown-conflict",
            theme_id="THEME_TH_TRAVEL_DEPARTURE_V1",
            product_facts=facts,
        )
        self.assertEqual(outfit["color_palette"][0], "brown")
        prompt = compose_shot_prompt(ShotGenerationRequest(
            task_id="brown-conflict", slot_index=1, slot_role="hero",
            shot_version=1,
            plan_shot={"slot_index": 1, "slot_role": "hero", "purpose": "展示"},
            product=result,
            product_facts=facts,
            persona_snapshot={"name": "persona"},
            look_snapshot={
                "prompt_core": "wear light blue outerwear",
                "recipe": {
                    "target_outer": "浅蓝色短款外套", "bottom": "白色长裤",
                },
            },
            scene_snapshot={"name": "bright room"}, output_dir="/tmp",
        ))
        self.assertIn("color=brown", prompt)
        self.assertNotIn("浅蓝", prompt)
        self.assertNotIn("light blue", prompt.lower())
        blue = self.resolver.resolve_snapshot(
            "1737141103233042426", variant_key=authoritative.variant_key,
            allow_history_bootstrap=False,
        )
        self.assertEqual(blue["product_name"], "浅蓝色短款蓬松外套")


if __name__ == "__main__":
    unittest.main()
