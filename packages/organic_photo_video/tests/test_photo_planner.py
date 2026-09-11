from __future__ import annotations

import copy
import hashlib
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from PIL import Image

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from config.loader import load_board_layouts, load_content_recipes  # noqa: E402
from domain.models import AssetSet  # noqa: E402
from services.photo_planner import PhotoReusePlannerService, PhotoPlannerError  # noqa: E402


class PlannerRepo:
    def __init__(self, root: Path):
        self.task = SimpleNamespace(
            task_id="photo-plan-task", task_status="draft", media_kind="native_photo",
            category_key="womenswear", target_country="TH", target_locale="th-TH",
            product_mode="NO_PRODUCT", product_id=None, product_snapshot_json={},
            market_pack_id="MP_TH_DEFAULT_V1", requested_shot_count=5,
            idempotency_key="stable-key", row_version=1, plan_json={}, copy_json={},
            content_package_id=None, storyboard_version=None, workflow_version=1,
            active_revision_id=None,
        )
        self.recipe = next(item for item in load_content_recipes()
                           if item.recipe_id == "PHOTO_TH_PICK_YOUR_LOOK_V1")
        self.recipe.status = "active"  # Frozen legacy behavior fixture.
        assets = []
        for index in range(1, 6):
            path = root / f"look-{index}.jpg"
            Image.new("RGB", (80, 120), (index * 30, 50, 90)).save(path)
            assets.append({
                "asset_id": f"look-{index}", "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "role": ["cover_seed", "look_a", "look_b", "look_c", "look_d"][index - 1],
            })
        self.asset_set = AssetSet(
            asset_set_id="aset-1", asset_set_key="TH_LOOKS", category_key="womenswear",
            market="TH", status="enabled", manifest_json={"assets": assets},
            tags_json={"choice_axis": ["outerwear", "style", "color"],
                       "scene": ["Photo", "Cafe", "Shopping"],
                       "style": ["korean_clean", "minimal", "casual"], "use_cases": ["pick_your_look"]},
        )
        self.pack = SimpleNamespace(
            market_pack_id="MP_TH_DEFAULT_V1", pack_version=1,
            target_country="TH", target_locale="th-TH",
        )
        self.package = None
        self.revision = None

    def get_task(self, task_id):
        return self.task if task_id == self.task.task_id else None

    def get_content_recipe(self, recipe_id):
        return self.recipe if recipe_id == self.recipe.recipe_id else None

    def get_asset_set(self, asset_set_id):
        return self.asset_set if asset_set_id == self.asset_set.asset_set_id else None

    def list_asset_sets(self, **_kwargs):
        return [self.asset_set]

    def get_market_pack(self, pack_id):
        return self.pack if pack_id == self.pack.market_pack_id else None

    def update_task_plan(self, task_id, **fields):
        assert task_id == self.task.task_id
        for key, value in fields.items():
            setattr(self.task, key, copy.deepcopy(value))

    def transition_task(self, task_id, source, target, **_kwargs):
        assert task_id == self.task.task_id and self.task.task_status == source
        self.task.task_status = target
        return self.task

    def get_content_package_by_task(self, task_id):
        return self.package if self.package and task_id == self.task.task_id else None

    def insert_content_package(self, package):
        self.package = package

    def get_content_package(self, package_id):
        return self.package if self.package and package_id == self.package.content_package_id else None

    def list_task_revisions(self, _task_id):
        return [] if self.revision is None else [self.revision]

    def create_revision_and_activate(self, revision, **_kwargs):
        self.revision = revision
        self.task.active_revision_id = revision.revision_id
        self.task.workflow_version = 2
        self.task.row_version += 1
        return self.task

    def get_task_revision(self, revision_id):
        return self.revision if self.revision and revision_id == self.revision.revision_id else None


class PhotoPlannerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.repo = PlannerRepo(Path(self.temp.name))
        self.layout = next(item for item in load_board_layouts()
                           if item.get("layout_id") == "PHOTO_CHOICE_GRID_V1")
        self.variables = {
            "choice_axis": "outerwear", "scene": "Photo", "style": "korean_clean",
        }
        self.copy = {
            "title": "เลือกหนึ่งลุค", "caption": "วันนี้ชอบลุคไหน",
            "hashtags": ["#OOTD"], "slide_texts": ["เลือก A B C D", "A", "B", "C", "D"],
        }

    def tearDown(self):
        self.temp.cleanup()

    def test_choice_recipe_freezes_grid_sources_copy_and_zero_ai_policy(self):
        result = PhotoReusePlannerService(self.repo).plan_task(
            self.repo.task.task_id, recipe_id=self.repo.recipe.recipe_id,
            variables=self.variables, copy_block=self.copy, layout=self.layout,
            asset_set_id=self.repo.asset_set.asset_set_id,
            recipe_snapshot=self.repo.recipe.to_row(), asset_snapshot=self.repo.asset_set.to_row(), operator="alice",
        )
        plan = result["plan"]
        self.assertEqual(plan["slides"][0]["layout_snapshot"]["layout"], "grid_2x2")
        self.assertEqual(plan["slides"][0]["source_slots"], [2, 3, 4, 5])
        self.assertEqual(plan["slides"][0]["overlay_text"], "เลือก A B C D")
        self.assertEqual(plan["production_policy"], {"mode": "ASSET_REUSE", "ai_image_calls": 0})
        self.assertEqual(self.repo.task.task_status, "planned")
        self.assertIsNotNone(self.repo.task.active_revision_id)

    def test_requires_all_five_localized_slide_texts(self):
        bad = {**self.copy, "slide_texts": ["only one"]}
        with self.assertRaisesRegex(PhotoPlannerError, "contain 5 strings"):
            PhotoReusePlannerService(self.repo).plan_task(
                self.repo.task.task_id, recipe_id=self.repo.recipe.recipe_id,
                variables=self.variables, copy_block=bad, layout=self.layout,
                asset_set_id=self.repo.asset_set.asset_set_id,
            recipe_snapshot=self.repo.recipe.to_row(), asset_snapshot=self.repo.asset_set.to_row(),
            )

    def test_long_recipe_identity_fits_both_storyboard_columns_without_losing_attribution(self):
        recipe_id = "PHOTO_TH_TEMPERATURE_DRESSING_V1"
        self.repo.recipe.recipe_id = recipe_id
        result = PhotoReusePlannerService(self.repo).plan_task(
            self.repo.task.task_id, recipe_id=recipe_id, variables=self.variables,
            copy_block=self.copy, layout=self.layout, asset_set_id=self.repo.asset_set.asset_set_id,
            recipe_snapshot=self.repo.recipe.to_row(), asset_snapshot=self.repo.asset_set.to_row(),
        )
        self.assertEqual(len(self.repo.task.storyboard_version), 32)
        self.assertEqual(self.repo.package.storyboard_version, self.repo.task.storyboard_version)
        self.assertEqual(result["plan"]["recipe"], {"id": recipe_id, "version": self.repo.recipe.recipe_version})
        self.assertEqual(self.repo.task.recipe_id, recipe_id)


if __name__ == "__main__":
    unittest.main()
