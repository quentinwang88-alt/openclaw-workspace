from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from PIL import Image

from config.loader import load_content_recipes
from services.image_generator import GenerationOutcome
from services.photo_content_planner import plan_th_choice_batch
from services.photo_outfit_supply import PhotoOutfitSupplyService
from services.photo_theme import resolve_photo_theme


def look(ref, bottom):
    return {"ref_id": ref, "name": ref, "recipe": {"top_inner": "white top", "bottom": bottom,
            "footwear": "sneakers"}, "status": "enabled"}


class Reader:
    def get_persona(self, ref):
        return {"ref_id": ref, "name": "TH creator", "prompt_core": "young Thai fashion creator"}


class Generator:
    def __init__(self, root):
        self.root, self.calls = Path(root), []

    def generate_shot(self, request):
        self.calls.append(request)
        path = self.root / f"generated-{request.slot_index}.png"
        Image.new("RGB", (180, 320), (80 + request.slot_index, 90, 100)).save(path)
        return GenerationOutcome(ok=True, image_path=str(path), request_id=f"req-{request.slot_index}",
                                 width=180, height=320)


class FailOnceGenerator(Generator):
    def generate_shot(self, request):
        if len(self.calls) == 1:
            self.calls.append(request)
            return GenerationOutcome(ok=False, error="temporary failure")
        return super().generate_shot(request)


class PhotoOutfitSupplyTest(unittest.TestCase):
    def test_planned_product_run_executes_exact_companion_outfits(self):
        recipe = next(item for item in load_content_recipes()
                      if item.recipe_id == "PHOTO_TH_PICK_YOUR_LOOK_V3")
        candidates = [look("PANTS_1", "pants"), look("PANTS_2", "jeans"),
                      look("SKIRT_1", "pleated skirt"), look("SKIRT_2", "long skirt")]
        variation = plan_th_choice_batch(
            record_id="planned-product", recipe_id=recipe.recipe_id,
            theme=resolve_photo_theme("秋季穿搭"), reference_mode="PRODUCT", count=1,
        )["items"][0]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            product_path = root / "product.png"
            Image.new("RGB", (180, 320), "navy").save(product_path)
            old_path = root / "old.png"
            Image.new("RGB", (180, 320), "gray").save(old_path)
            generator = Generator(root)
            service = PhotoOutfitSupplyService(
                generator=generator, asset_reader=Reader(), root=root,
            )
            account = SimpleNamespace(persona_ref_id="PERSONA_TH", allowed_look_refs_json=[])
            with patch("services.photo_outfit_supply.compatible_multi_look_candidates",
                       return_value=(candidates, {})):
                result = service.prepare(
                    record_id="planned", recipe=recipe,
                    product={"product_id": "P1", "category": "outerwear",
                             "reference_images": [str(product_path)]},
                    account=account,
                    existing_sources=[{"look_ref": "PANTS_1", "path": str(old_path)}],
                    variation=variation,
                )
        self.assertEqual(result["reused_count"], 0)
        self.assertEqual(len(generator.calls), 4)
        for request, planned in zip(generator.calls, variation["looks"]):
            self.assertEqual(request.outfit_state["bottom"], planned["bottom"])
            self.assertEqual(request.outfit_state["shoes"], planned["shoes"])
            self.assertEqual(request.outfit_state["outerwear"], planned["outerwear"])

    def test_reuses_matching_looks_and_generates_only_missing_roles(self):
        recipe = next(item for item in load_content_recipes()
                      if item.recipe_id == "PHOTO_TH_PICK_YOUR_LOOK_V3")
        candidates = [look("SHORT_1", "denim shorts"), look("SHORT_2", "black shorts"),
                      look("SKIRT_1", "pleated skirt"), look("SKIRT_2", "long skirt")]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            existing = []
            for ref in ("SHORT_1", "SKIRT_1"):
                path = root / f"{ref}.png"
                Image.new("RGB", (180, 320), "gray").save(path)
                existing.append({"task_id": ref, "look_ref": ref, "path": str(path)})
            generator = Generator(root)
            service = PhotoOutfitSupplyService(generator=generator, asset_reader=Reader(), root=root)
            account = SimpleNamespace(persona_ref_id="PERSONA_TH", allowed_look_refs_json=[])
            with patch("services.photo_outfit_supply.compatible_multi_look_candidates",
                       return_value=(candidates, {})):
                result = service.prepare(
                    record_id="rec", recipe=recipe,
                    product={"product_id": "P1", "category": "outerwear",
                             "reference_images": [str(existing[0]["path"])]},
                    account=account, existing_sources=existing,
                )
            self.assertEqual(result["reused_count"], 2)
            self.assertEqual(result["generated_count"], 2)
            self.assertEqual([item["role"] for item in result["sources"]],
                             ["look_a", "look_b", "look_c", "look_d"])
            self.assertEqual(len(generator.calls), 2)
            self.assertEqual({item["source_kind"] for item in result["sources"]},
                             {"existing_outfit", "generated_outfit"})

    def test_missing_required_outfit_template_fails_before_partial_generation(self):
        recipe = next(item for item in load_content_recipes()
                      if item.recipe_id == "PHOTO_TH_PICK_YOUR_LOOK_V3")
        candidates = [look("SHORT_1", "denim shorts"), look("SHORT_2", "black shorts")]
        with tempfile.TemporaryDirectory() as tmp:
            generator = Generator(tmp)
            service = PhotoOutfitSupplyService(generator=generator, asset_reader=Reader(), root=Path(tmp))
            account = SimpleNamespace(persona_ref_id="PERSONA_TH", allowed_look_refs_json=[])
            with patch("services.photo_outfit_supply.compatible_multi_look_candidates",
                       return_value=(candidates, {})):
                with self.assertRaisesRegex(ValueError, "裙装模板 0"):
                    service.prepare(
                        record_id="rec", recipe=recipe,
                        product={"product_id": "P1", "category": "outerwear",
                                 "reference_images": ["product.png"]},
                        account=account,
                    )
            self.assertEqual(len(generator.calls), 0)

    def test_retry_reuses_successful_generated_roles_from_manifest(self):
        recipe = next(item for item in load_content_recipes()
                      if item.recipe_id == "PHOTO_TH_PICK_YOUR_LOOK_V3")
        candidates = [look("SHORT_1", "denim shorts"), look("SHORT_2", "black shorts"),
                      look("SKIRT_1", "pleated skirt"), look("SKIRT_2", "long skirt")]
        with tempfile.TemporaryDirectory() as tmp:
            account = SimpleNamespace(persona_ref_id="PERSONA_TH", allowed_look_refs_json=[])
            first = FailOnceGenerator(tmp)
            service = PhotoOutfitSupplyService(generator=first, asset_reader=Reader(), root=Path(tmp))
            product_path = Path(tmp) / "product.png"
            Image.new("RGB", (180, 320), "navy").save(product_path)
            product = {"product_id": "P1", "category": "outerwear",
                       "reference_images": [str(product_path)]}
            with patch("services.photo_outfit_supply.compatible_multi_look_candidates",
                       return_value=(candidates, {})):
                with self.assertRaisesRegex(ValueError, "补图失败"):
                    service.prepare(record_id="rec", recipe=recipe, product=product, account=account)
            self.assertTrue((Path(tmp) / "outfit_supply" / "rec" / "supply_manifest.json").is_file())
            second = Generator(tmp)
            service = PhotoOutfitSupplyService(generator=second, asset_reader=Reader(), root=Path(tmp))
            with patch("services.photo_outfit_supply.compatible_multi_look_candidates",
                       return_value=(candidates, {})):
                result = service.prepare(record_id="rec", recipe=recipe, product=product, account=account)
            self.assertEqual(result["resumed_count"], 1)
            self.assertEqual(result["generated_this_run"], 3)
            self.assertEqual(len(second.calls), 3)


if __name__ == "__main__":
    unittest.main()
