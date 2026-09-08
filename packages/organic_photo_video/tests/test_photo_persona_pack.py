"""Persona Pack readiness tests (TH persona realism handoff Phase 1)."""

import tempfile
import unittest
from pathlib import Path

from PIL import Image

from services.asset_readiness import check_generation_assets
from services.persona_pack import (
    build_persona_pack,
    evaluate_persona_pack,
    normalize_reference_items,
    ordered_reference_items,
)
from types import SimpleNamespace


def make_persona_files(folder, count=4, roles=None):
    roles = roles or (
        ["FACE_FRONT_NEUTRAL", "FACE_THREE_QUARTER", "BODY_FULL_NEUTRAL", "BODY_FULL_MOTION"][:count]
    )
    paths = []
    for index, role in enumerate(roles):
        path = Path(folder) / f"{role.lower() or 'ref'}_{index}.png"
        Image.new("RGB", (40, 60), (100, 110, 120)).save(path)
        paths.append((str(path), role))
    return paths


class PersonaPackNormalizationTest(unittest.TestCase):
    def test_legacy_reference_json_without_role_stays_readable(self):
        with tempfile.TemporaryDirectory() as folder:
            paths = make_persona_files(folder, roles=["", "", "", ""])
            snapshot = {"reference_images": [
                {"local_path": path}  # 旧数据没有 role/approved 字段
                for path, _ in paths
            ]}
            items = normalize_reference_items(snapshot)
            self.assertEqual(len(items), 4)
            self.assertTrue(all(item["role"] == "" for item in items))
            self.assertTrue(all(item["approved"] for item in items))
            self.assertTrue(all(Path(item["local_path"]).is_file() for item in items))

    def test_single_closeup_selfie_is_not_ready_for_human_scenes(self):
        with tempfile.TemporaryDirectory() as folder:
            paths = make_persona_files(folder, roles=[""])
            snapshot = {"reference_images": [{"local_path": paths[0][0]}]}
            evaluation = evaluate_persona_pack(build_persona_pack(snapshot))
            self.assertFalse(evaluation["ready"])
            codes = {item["code"] for item in evaluation["issues"]}
            self.assertIn("persona_pack_insufficient_refs", codes)
            self.assertIn("persona_pack_face_missing", codes)
            self.assertIn("persona_pack_full_body_missing", codes)

    def test_full_pack_is_ready(self):
        with tempfile.TemporaryDirectory() as folder:
            paths = make_persona_files(folder)
            snapshot = {"persona_id": "TH_APPAREL_REAL_01_001", "reference_items": [
                {"local_path": path, "role": role, "approved": True}
                for path, role in paths
            ]}
            evaluation = evaluate_persona_pack(build_persona_pack(snapshot))
            self.assertTrue(evaluation["ready"], evaluation["issues"])
            self.assertEqual(evaluation["persona_pack_id"], "TH_APPAREL_REAL_01_001")
            self.assertEqual(len(evaluation["roles"]), 4)

    def test_face_only_pack_still_missing_full_body(self):
        with tempfile.TemporaryDirectory() as folder:
            paths = make_persona_files(folder, roles=[
                "FACE_FRONT_NEUTRAL", "FACE_THREE_QUARTER",
            ])
            snapshot = {"reference_images": [
                {"local_path": path, "role": role} for path, role in paths
            ]}
            evaluation = evaluate_persona_pack(build_persona_pack(snapshot))
            self.assertFalse(evaluation["ready"])
            self.assertIn("persona_pack_full_body_missing",
                          {item["code"] for item in evaluation["issues"]})

    def test_disapproved_and_missing_files_are_excluded(self):
        with tempfile.TemporaryDirectory() as folder:
            paths = make_persona_files(folder)
            snapshot = {"reference_items": [
                {"local_path": paths[0][0], "role": paths[0][1], "approved": False},
                {"local_path": "/nonexistent/missing.png", "role": "FACE_THREE_QUARTER"},
                {"local_path": paths[2][0], "role": paths[2][1]},
                {"local_path": paths[3][0], "role": paths[3][1]},
            ]}
            pack = build_persona_pack(snapshot)
            self.assertEqual(pack["approved_count"], 2)

    def test_identity_order_puts_face_evidence_first(self):
        with tempfile.TemporaryDirectory() as folder:
            paths = make_persona_files(folder)
            snapshot = {"reference_items": [
                {"local_path": path, "role": role}
                for path, role in reversed(paths)
            ]}
            ordered = ordered_reference_items(snapshot)
            self.assertEqual(
                [item["role"] for item in ordered],
                ["FACE_FRONT_NEUTRAL", "FACE_THREE_QUARTER",
                 "BODY_FULL_NEUTRAL", "BODY_FULL_MOTION"],
            )


class PersonaPackReadinessGateTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.account = SimpleNamespace(
            status="testing", persona_ref_id="TH_APPAREL_REAL_01_001",
            operating_rules_json={},
        )
        self.plan = {
            "presentation_type": "SCENE_MODEL",
            "persona": {"ref_id": "ACTUAL", "snapshot": {"status": "testing"}},
            "look": {"snapshot": {"status": "enabled", "recipe": {
                "top_inner": "white tee", "bottom": "jeans", "footwear": "sneakers",
            }}},
            "scene": {"ref_id": "S", "snapshot": {"status": "enabled", "prompt_core": "street"}},
            "shots": [],
        }
        self.product = {"product_id": "SKU", "category": "outerwear",
                        "reference_images": [], "reference_roles": {}}

    def test_scene_model_plan_blocks_single_selfie_persona(self):
        with tempfile.TemporaryDirectory() as folder:
            paths = make_persona_files(folder, roles=[""])
            self.plan["persona"]["snapshot"]["reference_images"] = [
                {"local_path": paths[0][0]}
            ]
            report = check_generation_assets(self.account, self.product, self.plan)
            codes = {item["code"] for item in report["issues"]}
            self.assertIn("persona_pack_full_body_missing", codes)
            self.assertFalse(report["ready"])

    def test_scene_model_plan_passes_with_role_complete_pack(self):
        with tempfile.TemporaryDirectory() as folder:
            paths = make_persona_files(folder)
            product_ref = Path(folder) / "product.png"
            Image.new("RGB", (40, 60), (90, 90, 90)).save(product_ref)
            self.product["reference_images"] = [str(product_ref)]
            self.product["reference_roles"] = {"front": [str(product_ref)]}
            self.plan["persona"]["snapshot"]["reference_items"] = [
                {"local_path": path, "role": role} for path, role in paths
            ]
            self.plan["persona"]["snapshot"]["local_reference_images"] = [
                path for path, _ in paths
            ]
            report = check_generation_assets(self.account, self.product, self.plan)
            codes = {item["code"] for item in report["issues"]}
            self.assertNotIn("persona_pack_full_body_missing", codes)
            self.assertNotIn("persona_pack_face_missing", codes)
            self.assertTrue(report["ready"], report["issues"])

    def test_non_scene_plan_keeps_legacy_persona_rules(self):
        with tempfile.TemporaryDirectory() as folder:
            paths = make_persona_files(folder, roles=[""])
            self.plan["presentation_type"] = "MODEL_FULL_BODY"
            self.plan["persona"]["snapshot"]["reference_images"] = [
                {"local_path": paths[0][0]}
            ]
            self.plan["persona"]["snapshot"]["local_reference_images"] = [paths[0][0]]
            report = check_generation_assets(self.account, self.product, self.plan)
            codes = {item["code"] for item in report["issues"]}
            self.assertNotIn("persona_pack_full_body_missing", codes)


if __name__ == "__main__":
    unittest.main()
