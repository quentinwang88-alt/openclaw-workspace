"""The three-column ``triptych_3`` layout used by the daily transition card."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

from PIL import Image

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from config.loader import load_board_layouts
from services.photo_package import (
    PhotoPackageError, _compose, normalize_photo_template,
)


LAYOUT_ID = "PHOTO_THERMAL_ROUTE_V1"
COLORS = ((214, 60, 60), (60, 176, 90), (58, 92, 210))
LABELS = ["ข้างนอกร้อน", "รถไฟฟ้า/ห้างเย็น", "ออฟฟิศแอร์แรง"]


def _template():
    layout = next(
        item for item in load_board_layouts() if item.get("layout_id") == LAYOUT_ID
    )
    return normalize_photo_template(layout)


def _sources(root: Path, colors=COLORS, size=(540, 960)):
    paths = []
    for index, color in enumerate(colors, 1):
        path = root / f"source-{index}.png"
        Image.new("RGB", size, color).save(path)
        paths.append(str(path))
    return paths


class ThermalTriptychLayoutTest(unittest.TestCase):
    def test_shipped_layout_is_executable_v2(self):
        layout = next(
            item for item in load_board_layouts() if item.get("layout_id") == LAYOUT_ID
        )
        self.assertEqual(layout["schema_version"], "opv-photo-layout-v2")
        self.assertEqual(layout["layout_kind"], "THERMAL_ROUTE")
        template = normalize_photo_template(layout)
        self.assertEqual(template["template_id"], LAYOUT_ID)
        self.assertEqual((template["width"], template["height"]), (1080, 1920))

    def test_columns_keep_frozen_role_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            template = _template()
            paths = _sources(Path(tmp))
            image = _compose(
                paths, layout="triptych_3", width=template["width"],
                height=template["height"], background=template["background"],
                template=template, column_labels=LABELS,
            )
            width, height = image.size
            gap = max(0, round(min(width, height) * 0.012))
            cell = (width - gap * 2) // 3
            pixels = image.load()
            sampled = [
                pixels[index * (cell + gap) + cell // 2, height // 2]
                for index in range(3)
            ]
            self.assertEqual(sampled, list(COLORS))

    def test_exactly_three_sources_are_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            template = _template()
            paths = _sources(Path(tmp))
            with self.assertRaisesRegex(PhotoPackageError, "exactly three"):
                _compose(
                    paths[:2], layout="triptych_3", width=1080, height=1920,
                    background="#FFFFFF", template=template,
                )
            with self.assertRaisesRegex(PhotoPackageError, "exactly three"):
                _compose(
                    paths + [paths[0]], layout="triptych_3", width=1080, height=1920,
                    background="#FFFFFF", template=template,
                )

    def test_labels_stay_in_the_bottom_safe_strip_of_their_own_column(self):
        with tempfile.TemporaryDirectory() as tmp:
            template = _template()
            paths = _sources(Path(tmp))
            image = _compose(
                paths, layout="triptych_3", width=1080, height=1920,
                background=template["background"], template=template,
                column_labels=LABELS,
            )
            width, height = image.size
            gap = max(0, round(min(width, height) * 0.012))
            cell = (width - gap * 2) // 3
            pixels = image.load()
            bottom_y = height - 30
            middle_y = height // 3
            for index, color in enumerate(COLORS):
                left = index * (cell + gap)
                # The mid/lower body of each column stays clean photo: labels
                # can never climb into a face or the headline block.
                self.assertEqual(pixels[left + cell // 2, middle_y], color)
                self.assertNotEqual(pixels[left + cell // 2, bottom_y], color)
            # Column boundaries keep a visible gap, so labels inside one column
            # cannot bleed into the next.
            for index in range(2):
                x = (index + 1) * cell + index * gap + gap // 2
                self.assertEqual(pixels[x, height // 2], tuple(int(
                    template["background"][offset:offset + 2], 16
                ) for offset in (1, 3, 5)))

    def test_overlong_label_is_rejected_instead_of_overflowing(self):
        with tempfile.TemporaryDirectory() as tmp:
            template = _template()
            paths = _sources(Path(tmp))
            with self.assertRaisesRegex(PhotoPackageError, "does not fit its column"):
                _compose(
                    paths, layout="triptych_3", width=1080, height=1920,
                    background=template["background"], template=template,
                    column_labels=["ข้างนอก", "x" * 200, "ออฟฟิศ"],
                )

    def test_missing_labels_are_allowed(self):
        with tempfile.TemporaryDirectory() as tmp:
            template = _template()
            paths = _sources(Path(tmp))
            image = _compose(
                paths, layout="triptych_3", width=1080, height=1920,
                background=template["background"], template=template,
            )
            self.assertEqual(image.size, (1080, 1920))

    def test_existing_layouts_are_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = _sources(root, COLORS[:2], size=(540, 960))
            single = _compose(
                paths[:1], layout="single", width=1080, height=1920,
                background="#FFFFFF",
            )
            self.assertEqual(single.size, (1080, 1920))
            self.assertEqual(single.load()[540, 960], COLORS[0])
            split = _compose(
                paths, layout="split_vertical", width=1080, height=1920,
                background="#FFFFFF",
            )
            self.assertEqual(split.size, (1080, 1920))
            gap = max(0, round(min(1080, 1920) * 0.012))
            cell = (1080 - gap) // 2
            pixels = split.load()
            self.assertEqual(pixels[cell // 2, 960], COLORS[0])
            self.assertEqual(pixels[cell + gap + cell // 2, 960], COLORS[1])
            with self.assertRaisesRegex(PhotoPackageError, "unsupported photo layout"):
                _compose(
                    paths, layout="triptych_4", width=1080, height=1920,
                    background="#FFFFFF", template=_template(),
                )

    def test_rendered_bytes_are_stable_and_distinct_per_page(self):
        """Two renders of identical inputs must be byte-identical (no jitter)."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            template = _template()
            paths = _sources(root)
            digests = []
            for run in range(2):
                image = _compose(
                    paths, layout="triptych_3", width=1080, height=1920,
                    background=template["background"], template=template,
                    column_labels=LABELS,
                )
                out = root / f"render-{run}.jpg"
                image.save(out, "JPEG", quality=int(template["jpeg_quality"]))
                digests.append(hashlib.sha256(out.read_bytes()).hexdigest())
            self.assertEqual(digests[0], digests[1])

    def test_recipe_card_pages_match_the_layout_contract(self):
        from config.loader import load_content_recipes

        recipe = next(
            item for item in load_content_recipes()
            if item.recipe_id == "PHOTO_TH_THERMAL_TRANSITION_V1"
        )
        card = recipe.recipe_spec_json["content_card"]
        self.assertEqual(
            [page["layout"] for page in card["pages"]],
            ["triptych_3", "single", "single", "single", "triptych_3"],
        )
        self.assertEqual(
            [page["source_roles"] for page in card["pages"]],
            [["base", "mid", "outer"], ["base"], ["mid"], ["outer"],
             ["base", "mid", "outer"]],
        )
        self.assertEqual(
            card["layering_roles"], ["base", "mid", "outer"]
        )
        self.assertEqual(
            json.dumps(card["pages"][0]["column_labels"], ensure_ascii=False),
            json.dumps(LABELS, ensure_ascii=False),
        )


if __name__ == "__main__":
    unittest.main()
