from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace

from PIL import Image, ImageDraw

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain.models import ContentTask  # noqa: E402
from services.outfit_decomposer import (  # noqa: E402
    GeneratedAsset,
    OutfitDecomposer,
    OutfitDecompositionError,
    compose_asset_prompt,
    decomposition_ready,
    final_companion_items,
    companion_items_for_state,
)


class Repo:
    def __init__(self, task):
        self.task = task

    def get_task(self, task_id):
        return self.task if task_id == self.task.task_id else None

    def update_task_plan(self, task_id, *, plan_json=None, **_fields):
        self.task.plan_json = plan_json


class Generator:
    def __init__(self, root: Path):
        self.root = root
        self.calls = []

    def generate(self, **kwargs):
        self.calls.append(kwargs)
        path = self.root / f"{kwargs['role']}.png"
        image = Image.new("RGB", (512, 512), "#00ff00")
        ImageDraw.Draw(image).rectangle((110, 70, 400, 450), fill="#dfe8f0")
        image.save(path)
        return GeneratedAsset(path=str(path))


def make_source(path: Path, *, green=False):
    image = Image.new("RGB", (512, 640), "#00ff00" if green else "#f8f8f8")
    ImageDraw.Draw(image).rectangle((120, 60, 390, 590), fill="#6688aa")
    image.save(path)


class OutfitDecomposerTest(unittest.TestCase):
    def test_multi_look_cover_uses_look01_and_original_slot1_person_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            anchor, product = root / "p1_original.png", root / "product.png"
            make_source(anchor)
            make_source(product, green=True)
            task = ContentTask(task_id="multi", idempotency_key="c" * 64,
                account_id="account", product_id="product", target_country="TH", target_locale="th-TH",
                plan_json={"anchor_slot": 1, "recipe_execution": {"content_goal": "multi_look"},
                    "presentation_profile": {"background_mode": "solid_color", "background_color": "#F1F3F5"},
                    "look": {"snapshot": {"recipe": {"top_inner": "wrong old tank", "bottom": "wrong old pants"}}},
                    "outfit_states": {
                        "FINAL": {"top_inner": "wrong FINAL top", "bottom": {"type": "wrong FINAL pants"}},
                        "LOOK_01": {"top_inner": "条纹针织衫", "bottom": {"type": "格纹短裙"},
                                    "label_i18n": {"bottom": {"th-TH": "กระโปรงลายตาราง"}}},
                        "LOOK_02": {"top_inner": "wrong second dress", "bottom": {}},
                    },
                    "shots": [{"slot_index": 1, "shot_kind": "composite_board", "outfit_state_ref": "LOOK_01",
                               "board_spec": {"layout_id": "LAYOUT_OUTFIT_REFERENCE_LEFT_V1", "source_person_slot": 1}}]},
                product_snapshot_json={"product": {"reference_images": [str(product)]}})
            repo = Repo(task)
            repo.list_shots = lambda _: [SimpleNamespace(slot_index=1, image_url=str(anchor), shot_version=1,
                                                        shot_id="p1_original_candidate", image_sha256="original_digest")]
            generator = Generator(root)
            service = OutfitDecomposer(repo, generator=generator, output_root=root / "output")
            result = service.ensure(task, anchor_path=str(anchor))
            manifest = result.plan_json["decomposition_assets"]
            self.assertEqual(manifest["source_outfit_state_ref"], "LOOK_01")
            self.assertEqual(manifest["item_roles"], ["target_product", "top_inner", "bottom"])
            self.assertEqual(manifest["assets"]["bottom"]["label"], "格纹短裙")
            self.assertEqual(manifest["assets"]["bottom"]["label_i18n"], {"th-TH": "กระโปรงลายตาราง"})
            self.assertEqual(manifest["assets"]["person_cutout"]["source_slot"], 1)
            self.assertEqual(manifest["assets"]["person_cutout"]["source_asset_id"], "p1_original_candidate")
            self.assertEqual(manifest["assets"]["person_cutout"]["source_sha256"], "original_digest")
            self.assertTrue(all(asset["outfit_state_ref"] == "LOOK_01" for asset in manifest["assets"].values()))
            self.assertNotIn("wrong", str(generator.calls))
            self.assertNotIn("person_cutout", [call["role"] for call in generator.calls])
            calls = len(generator.calls)
            service.ensure(result, anchor_path=str(anchor))
            self.assertEqual(len(generator.calls), calls)
            result.plan_json["shots"][0]["outfit_state_ref"] = "LOOK_02"
            self.assertFalse(decomposition_ready(result.plan_json))

    def test_multi_look_missing_selected_state_does_not_fall_back_to_final(self):
        plan = {"outfit_states": {"FINAL": {"top_inner": "white tank", "bottom": {"type": "pants"}}}}
        with self.assertRaisesRegex(OutfitDecompositionError, "LOOK_01 outfit has no real companion"):
            companion_items_for_state(plan, "LOOK_01")

    def test_final_dress_is_one_garment_not_fabricated_bottom(self):
        plan = {"outfit_states": {"FINAL": {"top_inner": "米白色针织连衣裙", "bottom": {"type": "无独立下装"}}}}
        self.assertEqual(final_companion_items(plan), [("top_inner", "米白色针织连衣裙", None)])

    def test_dynamic_preserves_anchor_and_final_items_and_reuses_transparent_product(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            anchor, product = root / "anchor.png", root / "product.png"
            make_source(anchor)
            image = Image.new("RGBA", (512, 640), (0, 0, 0, 0))
            ImageDraw.Draw(image).rectangle((110, 60, 400, 590), fill="#8899aa")
            image.save(product)
            for final, expected in [
                ({"top_inner": "条纹T恤", "bottom": {"type": "阔腿牛仔裤", "color": "蓝色"}}, ["target_product", "top_inner", "bottom"]),
                ({"top_inner": "针织连衣裙", "bottom": {}}, ["target_product", "top_inner"]),
            ]:
                with self.subTest(final=final):
                    task = ContentTask(task_id="dynamic", idempotency_key="a" * 64,
                        account_id="account", product_id="product", target_country="TH", target_locale="th-TH",
                        plan_json={"presentation_profile": {"background_mode": "solid_color", "background_color": "#F1F3F5", "item_count_policy": "dynamic_2_or_3"},
                            "look": {"snapshot": {"recipe": {"top_inner": "wrong old top", "bottom": "wrong old skirt"}}},
                            "outfit_states": {"FINAL": final},
                            "shots": [{"slot_index": 1, "shot_kind": "composite_board", "board_spec": {"layout_id": "LAYOUT_OUTFIT_REFERENCE_LEFT_V1"}}]},
                        product_snapshot_json={"product": {"reference_images": [str(product)]}})
                    generator = Generator(root)
                    result = OutfitDecomposer(Repo(task), generator=generator, output_root=root / "output").ensure(task, anchor_path=str(anchor))
                    manifest = result.plan_json["decomposition_assets"]
                    self.assertTrue(decomposition_ready(result.plan_json))
                    self.assertEqual(manifest["item_roles"], expected)
                    self.assertEqual([call["role"] for call in generator.calls], expected[1:])
                    self.assertEqual(manifest["assets"]["person_cutout"]["composition_mode"], "adaptive_background_matte")
                    self.assertEqual(manifest["assets"]["target_product"]["source_type"], "reused_transparent_product_reference")
                    self.assertNotIn("wrong old", str(generator.calls))
                    self.assertEqual(result.plan_json["shots"][0]["board_spec"]["background_color"], "#F1F3F5")

    def test_generates_missing_assets_and_persists_complete_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            anchor = root / "anchor.png"
            product = root / "product.png"
            top_ref = root / "top.png"
            for path in (anchor, product, top_ref):
                make_source(path, green=path != anchor)
            task = ContentTask(
                task_id="task_1",
                idempotency_key="a" * 64,
                account_id="account",
                product_id="product",
                target_country="TH",
                target_locale="th-TH",
                plan_json={
                    "look": {
                        "snapshot": {
                            "recipe": {"top_inner": "white tank", "bottom": "white pants"},
                            "item_refs": {"top_inner": {"local_path": str(top_ref)}},
                        }
                    },
                    "shots": [{
                        "slot_index": 1,
                        "shot_kind": "composite_board",
                        "board_spec": {"decomposition_required": True},
                    }],
                },
                product_snapshot_json={
                    "product": {
                        "product_name": "blue jacket",
                        "reference_images": [str(product)],
                    }
                },
            )
            repo = Repo(task)
            generator = Generator(root)
            refreshed = OutfitDecomposer(
                repo, generator=generator, output_root=root / "output"
            ).ensure(task, anchor_path=str(anchor))

            self.assertTrue(decomposition_ready(refreshed.plan_json))
            manifest = refreshed.plan_json["decomposition_assets"]
            self.assertEqual(set(manifest["assets"]), {
                "person_cutout", "target_product", "top_inner", "bottom"
            })
            self.assertEqual(len(generator.calls), 4)
            person_call = next(call for call in generator.calls if call["role"] == "person_cutout")
            top_call = next(call for call in generator.calls if call["role"] == "top_inner")
            bottom_call = next(call for call in generator.calls if call["role"] == "bottom")
            self.assertEqual(top_call["reference_paths"], [str(top_ref)])
            self.assertEqual(person_call["reference_paths"], [str(anchor)])
            self.assertEqual(bottom_call["reference_paths"], [str(anchor)])
            self.assertEqual(
                manifest["assets"]["person_cutout"]["source_type"], "anchor_slot_isolated"
            )
            self.assertEqual(
                manifest["assets"]["top_inner"]["source_type"],
                "ai_isolated_from_look_item_ref",
            )
            self.assertEqual(
                manifest["assets"]["person_cutout"]["source_slot"], 2
            )
            self.assertEqual(
                refreshed.plan_json["shots"][0]["board_spec"]["layout_id"],
                "LAYOUT_OUTFIT_BREAKDOWN_V2",
            )

    def test_prompt_requires_one_item_and_chroma_green(self):
        prompt = compose_asset_prompt("bottom", "white pants", has_item_reference=False)
        self.assertIn("只出现一件完整单品", prompt)
        self.assertIn("#00FF00", prompt)
        self.assertIn("完整穿搭人物", prompt)


if __name__ == "__main__":
    unittest.main()
