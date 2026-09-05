from pathlib import Path
import sys
import tempfile
import unittest

from PIL import Image, ImageDraw

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services.cutout_assets import (  # noqa: E402
    build_asset_manifest,
    chroma_green_to_alpha,
    edge_background_to_alpha,
    normalize_cutout,
    normalize_anchor_region,
    refresh_manifest_anchor_pixels,
    CutoutAssetError,
)


class CutoutAssetsTest(unittest.TestCase):
    def test_board_only_refresh_preserves_old_manifest_and_all_garment_assets(self):
        from copy import deepcopy
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            image = Image.new("RGB", (360, 640), "#F1F3F5")
            ImageDraw.Draw(image).rectangle((90, 40, 270, 610), fill="#335577")
            image.save(root / "anchor.png")
            old = {"status": "ready", "assets": {
                "person_cutout": {"path": str(root / "old/person_cutout_normalized.png"), "sha256": "old-person", "source_slot": 2},
                "target_product": {"path": "real-product.png", "sha256": "product"},
                "top_inner": {"path": "real-inner.png", "sha256": "inner"},
                "bottom": {"path": "real-bottom.png", "sha256": "bottom"},
            }}
            before = deepcopy(old)
            result = refresh_manifest_anchor_pixels(old, anchor_path=root / "anchor.png", output_dir=root / "new", background="#F1F3F5")
            self.assertEqual(old, before)
            for role in ("target_product", "top_inner", "bottom"):
                self.assertEqual(result["assets"][role], before["assets"][role])
            self.assertEqual(result["assets"]["person_cutout"]["source_slot"], 2)
            self.assertTrue((root / "new/manifest.json").is_file())
            with self.assertRaises(CutoutAssetError):
                refresh_manifest_anchor_pixels(old, anchor_path=root / "anchor.png", output_dir=root / "old", background="#F1F3F5")

    def test_adaptive_anchor_matte_handles_noisy_gradient_and_preserves_white_interior(self):
        import numpy as np
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rng = np.random.default_rng(4)
            array = np.zeros((640, 360, 3), dtype=np.float32)
            array[:] = 236 + np.linspace(0, 6, 640)[:, None, None]
            array += rng.normal(0, .8, array.shape)
            image = Image.fromarray(np.uint8(np.clip(array, 0, 255)))
            draw = ImageDraw.Draw(image)
            draw.ellipse((130, 30, 225, 160), fill="#49372a")
            draw.rounded_rectangle((90, 130, 270, 340), radius=20, fill="#bfd1dc")
            draw.rectangle((150, 140, 205, 335), fill="white")
            draw.rectangle((110, 330, 250, 600), fill="#44474a")
            image.save(root / "anchor.png")
            qa = normalize_anchor_region(root / "anchor.png", root / "matte.png", background="#F1F3F5")
            self.assertEqual(qa["composition_mode"], "adaptive_background_matte")
            self.assertLess(qa["width"], 240)
            left, top, _, _ = qa["source_crop"]
            with Image.open(root / "matte.png") as matte:
                self.assertEqual(matte.getpixel((180 - left, 250 - top)), (255, 255, 255, 255))
                self.assertEqual(matte.getchannel("A").getextrema(), (0, 255))
                self.assertGreater(matte.height, 550)

    def test_same_background_anchor_keeps_edge_connected_white_clothing_opaque(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, output = Path(tmp) / "anchor.png", Path(tmp) / "region.png"
            image = Image.new("RGB", (400, 640), "white")
            draw = ImageDraw.Draw(image)
            draw.rectangle((130, 30, 270, 150), fill="#342a20")
            # The white jacket touches white backdrop. A brightness floodfill
            # would erase it, whereas the region must retain every opaque pixel.
            draw.rectangle((110, 150, 290, 600), fill="white")
            image.save(source)
            qa = normalize_anchor_region(source, output, background="#FFFFFF")
            with Image.open(output) as result:
                self.assertEqual(result.getchannel("A").getextrema(), (255, 255))
                self.assertEqual(result.height, 640)
                left = qa["source_crop"][0]
                self.assertEqual(result.getpixel((200 - left, 300)), (255, 255, 255, 255))
                self.assertEqual(result.convert("RGB").tobytes(), image.crop(tuple(qa["source_crop"])).tobytes())

    def test_dynamic_manifest_accepts_two_items_without_bottom(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sources = {}
            for role in ("person_cutout", "target_product", "onepiece"):
                path = root / f"{role}.png"
                image = Image.new("RGBA", (400, 500), (0, 0, 0, 0))
                ImageDraw.Draw(image).rectangle((80, 60, 320, 440), fill="white")
                image.save(path)
                sources[role] = str(path)
            manifest = build_asset_manifest(task_id="dress", sources=sources,
                output_dir=root / "normalized", item_roles=["target_product", "onepiece"])
            self.assertEqual(manifest["schema_version"], "opv-outfit-decomposition-v2")
            self.assertEqual(manifest["item_roles"], ["target_product", "onepiece"])
            self.assertNotIn("bottom", manifest["assets"])

    def test_chroma_key_preserves_white_garment(self):
        image = Image.new("RGB", (300, 400), "#00ff00")
        ImageDraw.Draw(image).rectangle((80, 40, 220, 360), fill="white")
        result = chroma_green_to_alpha(image)
        self.assertEqual(result.getpixel((0, 0))[3], 0)
        self.assertEqual(result.getpixel((150, 200))[3], 255)

    def test_chroma_key_removes_low_dominance_green_spill(self):
        image = Image.new("RGB", (300, 400), "#00ff00")
        ImageDraw.Draw(image).rectangle((80, 40, 220, 360), fill=(80, 100, 90))
        result = chroma_green_to_alpha(image)
        red, green, blue, alpha = result.getpixel((150, 200))
        self.assertEqual(green, max(red, blue))
        self.assertEqual(alpha, 255)

    def test_checkerboard_background_is_removed_but_white_clothing_remains(self):
        image = Image.new("RGB", (300, 400), "#eeeeee")
        draw = ImageDraw.Draw(image)
        for y in range(0, 400, 20):
            for x in range(0, 300, 20):
                if (x // 20 + y // 20) % 2:
                    draw.rectangle((x, y, x + 19, y + 19), fill="#fafafa")
        draw.ellipse((80, 40, 220, 360), fill="#222222")
        draw.rectangle((120, 150, 180, 260), fill="white")
        result = edge_background_to_alpha(image)
        self.assertEqual(result.getpixel((0, 0))[3], 0)
        self.assertEqual(result.getpixel((150, 200))[3], 255)

    def test_manifest_requires_and_normalizes_all_four_roles(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sources = {}
            for role in ("person_cutout", "target_product", "top_inner", "bottom"):
                path = root / f"{role}.png"
                image = Image.new("RGBA", (400, 500), (0, 0, 0, 0))
                ImageDraw.Draw(image).rectangle((80, 60, 320, 440), fill="#6688AA")
                image.save(path)
                sources[role] = str(path)
            manifest = build_asset_manifest(
                task_id="t1", sources=sources, output_dir=root / "normalized"
            )
            self.assertEqual(manifest["status"], "ready")
            self.assertEqual(set(manifest["assets"]), set(sources))
            self.assertTrue(all(
                item["qa_status"] == "passed"
                for item in manifest["assets"].values()
            ))


if __name__ == "__main__":
    unittest.main()
