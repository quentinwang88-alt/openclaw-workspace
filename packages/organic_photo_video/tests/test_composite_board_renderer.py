from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from PIL import Image, ImageDraw, ImageChops

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services.composite_board_renderer import (  # noqa: E402
    CompositeBoardRenderer,
    _near_white_background_to_alpha,
    _paste_with_shadow,
)
from services.image_generator import GenerationOutcome, ShotGenerationRequest  # noqa: E402
from services.shot_producer_router import ShotProducerRouter  # noqa: E402


def make_image(path: Path, size, color, *, white_background=False):
    image = Image.new("RGB", size, "white" if white_background else color)
    if white_background:
        draw = ImageDraw.Draw(image)
        draw.rounded_rectangle((size[0] // 4, 20, size[0] * 3 // 4, size[1] - 20),
                               radius=24, fill=color)
    image.save(path)


class CompositeBoardRendererTest(unittest.TestCase):
    def test_light_item_contrast_does_not_recolour_garment_or_background(self):
        garment = Image.new("RGBA", (100, 120), (0, 0, 0, 0))
        ImageDraw.Draw(garment).rectangle((20, 20, 80, 100), fill="white")
        box = {"x": 30, "y": 30, "width": 100, "height": 120, "shadow": False}
        legacy = Image.new("RGBA", (200, 200), "#F1F3F5")
        current = legacy.copy()
        _paste_with_shadow(legacy, garment, box)
        _paste_with_shadow(current, garment, box, enhance_contrast=True)
        self.assertEqual(current.getpixel((80, 80)), (255, 255, 255, 255))
        self.assertEqual(current.getpixel((0, 0)), legacy.getpixel((0, 0)))
        self.assertLess(current.getpixel((48, 80))[0], legacy.getpixel((48, 80))[0])

    def test_compact_stack_policies_render_without_changing_hero(self):
        with tempfile.TemporaryDirectory() as tmp:
            request = self.reference_request(Path(tmp))
            request.plan_shot["board_spec"].update(item_contrast_policy="soft_silhouette_v1",
                                                  item_layout_policy="compact_stack_v2")
            result = CompositeBoardRenderer().generate_shot(request)
            self.assertTrue(result.ok, result.error)
            self.assertEqual(result.raw["item_layout_policy"], "compact_stack_v2")
            self.assertEqual(result.raw["item_contrast_policy"], "soft_silhouette_v1")
            self.assertEqual(result.raw["item_count"], 3)

    def test_multi_look_board_binds_look01_and_slot1_original_photo(self):
        with tempfile.TemporaryDirectory() as tmp:
            request = self.reference_request(Path(tmp))
            request.plan_shot["outfit_state_ref"] = "LOOK_01"
            request.plan_shot["board_spec"]["source_person_slot"] = 1
            request.outfit_state = {"bottom": "裙子", "label_i18n": {"bottom": {"th-TH": "กระโปรงลายตาราง"}}}
            manifest = request.plan_shot["board_spec"]["decomposition_assets"]
            manifest.update(item_count_policy="dynamic_2_or_3", item_roles=["target_product", "top_inner", "bottom"],
                            source_outfit_state_ref="LOOK_01")
            for asset in manifest["assets"].values():
                asset["outfit_state_ref"] = "LOOK_01"
                asset["source_slot"] = 1
            manifest["assets"]["person_cutout"]["source_asset_id"] = "p1_original_candidate"
            renderer = CompositeBoardRenderer()
            with patch.object(renderer, "_draw_copy", wraps=renderer._draw_copy) as draw:
                result = renderer.generate_shot(request)
                self.assertTrue(result.ok, result.error)
                items = draw.call_args.args[3]
                self.assertEqual(items[-1]["label_i18n"], {"th-TH": "กระโปรงลายตาราง"})
            self.assertEqual(result.raw["source_outfit_state_ref"], "LOOK_01")
            self.assertEqual(result.raw["source_person_asset_id"], "p1_original_candidate")
            manifest["assets"]["bottom"]["outfit_state_ref"] = "LOOK_02"
            result = renderer.generate_shot(request)
            self.assertFalse(result.ok)
            self.assertIn("belongs to LOOK_02", result.error)

    def test_multi_look_does_not_render_final_manifest_as_look01(self):
        with tempfile.TemporaryDirectory() as tmp:
            request = self.reference_request(Path(tmp))
            request.plan_shot["outfit_state_ref"] = "LOOK_01"
            request.plan_shot["board_spec"]["decomposition_assets"]["item_count_policy"] = "dynamic_2_or_3"
            result = CompositeBoardRenderer().generate_shot(request)
            self.assertFalse(result.ok)
            self.assertIn("outfit state mismatch", result.error)

    def test_dynamic_two_item_board_uses_profile_background_and_keeps_white_opaque(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            request = self.reference_request(root)
            manifest = request.plan_shot["board_spec"]["decomposition_assets"]
            manifest.update({"item_count_policy": "dynamic_2_or_3", "item_roles": ["target_product", "onepiece"]})
            manifest["assets"]["onepiece"] = manifest["assets"].pop("top_inner")
            manifest["assets"].pop("bottom")
            person = manifest["assets"]["person_cutout"]
            person["composition_mode"] = "same_background_region"
            image = Image.new("RGBA", (400, 1000), "#F1F3F5")
            ImageDraw.Draw(image).rectangle((100, 200, 300, 900), fill="white")
            image.save(person["path"])
            request.recipe_execution["presentation_profile"] = {"background_mode": "solid_color", "background_color": "#F1F3F5"}
            result = CompositeBoardRenderer().generate_shot(request)
            self.assertTrue(result.ok, result.error)
            self.assertEqual(result.raw["item_count"], 2)
            self.assertEqual(result.raw["background_color"], "#F1F3F5")
            with Image.open(result.image_path) as output:
                self.assertEqual(output.getpixel((0, 0)), (241, 243, 245))
                self.assertEqual(output.getpixel((365, 900)), (255, 255, 255))

    def reference_request(self, root):
        request = self.request(root)
        assets = {}
        colors = {"person_cutout": (230, 40, 60), "target_product": (30, 60, 220),
                  "top_inner": (40, 180, 80), "bottom": (220, 120, 30)}
        for role, color in colors.items():
            source = Image.new("RGBA", (500, 1000), (0, 0, 0, 0))
            bounds = (160, 40, 340, 960) if role == "person_cutout" else (50, 100, 450, 900)
            ImageDraw.Draw(source).rectangle(bounds, fill=(*color, 255))
            path = root / f"{role}.png"
            source.save(path)
            assets[role] = {"path": str(path), "source_slot": 2}
        request.plan_shot["board_spec"] = {
            "layout_id": "LAYOUT_OUTFIT_REFERENCE_LEFT_V1", "layout_variant": "REF_LEFT_HERO",
            "palette_variant": "REFERENCE_WHITE", "source_person_slot": 2,
            "decomposition_assets": {"assets": assets},
        }
        return request

    def test_reference_collage_is_white_textless_large_and_non_stretched(self):
        with tempfile.TemporaryDirectory() as tmp:
            request = self.reference_request(Path(tmp))
            # Old task palettes cannot silently turn the reference white gray.
            request.plan_shot["board_spec"]["palette_variant"] = "COOL_WHITE"
            with patch("services.composite_board_renderer.board_copy", side_effect=AssertionError("no copy required")), \
                 patch("services.composite_board_renderer.subprocess.run", side_effect=AssertionError("no text subprocess")):
                result = CompositeBoardRenderer().generate_shot(request)
            self.assertTrue(result.ok, result.error)
            self.assertEqual((result.width, result.height), (1080, 1920))
            self.assertEqual(result.raw["layout_id"], "LAYOUT_OUTFIT_REFERENCE_LEFT_V1")
            self.assertEqual(result.raw["background_color"], "#FFFFFF")
            self.assertFalse(result.raw["text_rendered"])
            self.assertFalse(result.raw["commerce_data_rendered"])
            self.assertEqual(result.raw["item_count"], 3)
            with Image.open(result.image_path) as output:
                self.assertEqual(output.getpixel((0, 0)), (255, 255, 255))
                red, green, blue = output.split()
                mask = ImageChops.multiply(red.point(lambda v: 255 if v == 230 else 0),
                    green.point(lambda v: 255 if v == 40 else 0))
                mask = ImageChops.multiply(mask, blue.point(lambda v: 255 if v == 60 else 0))
                left, top, right, bottom = mask.getbbox()
                self.assertGreater((bottom - top) / output.height, .9)
                self.assertLess((left + right) / 2, output.width / 2)
                self.assertAlmostEqual((right - left) / (bottom - top), 181 / 921, delta=.005)

    def test_reference_layout_defaults_do_not_rotate_into_legacy_grid(self):
        with tempfile.TemporaryDirectory() as tmp:
            request = self.reference_request(Path(tmp))
            request.plan_shot["board_spec"].pop("layout_variant")
            request.plan_shot["board_spec"].pop("palette_variant")
            result = CompositeBoardRenderer().generate_shot(request)
            self.assertTrue(result.ok, result.error)
            self.assertEqual(result.raw["layout_variant"], "REF_LEFT_HERO")
            self.assertEqual(result.raw["palette_variant"], "REFERENCE_WHITE")

    def test_reference_layout_still_requires_real_decomposition_assets(self):
        with tempfile.TemporaryDirectory() as tmp:
            request = self.reference_request(Path(tmp))
            request.plan_shot["board_spec"]["decomposition_assets"]["assets"].pop("top_inner")
            result = CompositeBoardRenderer().generate_shot(request)
            self.assertFalse(result.ok)
            self.assertIn("top_inner", result.error)

    def test_reference_layout_rejects_empty_person_cutout(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            request = self.reference_request(root)
            Image.new("RGBA", (500, 1000)).save(root / "person_cutout.png")
            result = CompositeBoardRenderer().generate_shot(request)
            self.assertFalse(result.ok)
            self.assertIn("visible pixels", result.error)

    def test_background_removal_keeps_enclosed_white_garment(self):
        image = Image.new("RGB", (80, 100), "white")
        draw = ImageDraw.Draw(image)
        draw.ellipse((15, 10, 65, 95), fill="#222222")
        draw.rectangle((30, 35, 50, 70), fill="white")
        cutout = _near_white_background_to_alpha(image)
        self.assertEqual(cutout.getpixel((0, 0))[3], 0)
        self.assertEqual(cutout.getpixel((40, 50))[3], 255)

    def request(self, root: Path, *, continuity=True):
        anchor = root / "anchor.png"
        product = root / "product.png"
        bottom = root / "bottom.png"
        make_image(anchor, (720, 1280), "#8899AA")
        make_image(product, (500, 700), "#222222", white_background=True)
        make_image(bottom, (500, 700), "#555555", white_background=True)
        return ShotGenerationRequest(
            task_id="task-1", slot_index=1, slot_role="hero", shot_version=1,
            plan_shot={
                "shot_kind": "composite_board",
                "board_spec": {
                    "layout_variant": "RIGHT_HERO",
                    "copy_variant": "three_piece_formula",
                    "palette_variant": "COOL_WHITE",
                },
            },
            product={
                "reference_images": [str(product)],
                "title_i18n": {"th-TH": "เสื้อแจ็กเก็ตสีดำ"},
            },
            persona_snapshot={},
            look_snapshot={
                "recipe": {"bottom": "กระโปรงสั้น"},
                "item_refs": {
                    "bottom": {"local_path": str(bottom), "label_i18n": {"th-TH": "กระโปรงสั้น"}},
                },
            },
            scene_snapshot={}, output_dir=str(root),
            continuity_reference_images=[str(anchor)] if continuity else [],
            recipe_execution={"locale": "th-TH", "hook_strategy": "outfit_formula"},
        )

    def test_renders_916_board_from_real_assets(self):
        with tempfile.TemporaryDirectory() as tmp:
            request = self.request(Path(tmp))
            result = CompositeBoardRenderer().generate_shot(request)
            self.assertTrue(result.ok, result.error)
            self.assertEqual((result.width, result.height), (1080, 1920))
            self.assertEqual(result.provider, "programmatic-board")
            self.assertEqual(result.raw["layout_variant"], "RIGHT_HERO")
            self.assertEqual(result.raw["copy_variant"], "three_piece_formula")
            self.assertFalse(result.raw["commerce_data_rendered"])
            self.assertTrue(Path(result.image_path).is_file())

    def test_missing_anchor_fails_without_calling_photo_model(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = CompositeBoardRenderer().generate_shot(
                self.request(Path(tmp), continuity=False)
            )
            self.assertFalse(result.ok)
            self.assertIn("anchor", result.error)

    def test_router_keeps_generated_photos_on_existing_generator(self):
        class FakePhoto:
            def generate_shot(self, request):
                return GenerationOutcome(ok=True, provider="photo")

        class FakeBoard:
            def generate_shot(self, request):
                return GenerationOutcome(ok=True, provider="board")

        router = ShotProducerRouter(FakePhoto(), FakeBoard())
        with tempfile.TemporaryDirectory() as tmp:
            request = self.request(Path(tmp))
            self.assertEqual(router.generate_shot(request).provider, "board")
            request.plan_shot = {"shot_kind": "generated_photo"}
            self.assertEqual(router.generate_shot(request).provider, "photo")


if __name__ == "__main__":
    unittest.main()
