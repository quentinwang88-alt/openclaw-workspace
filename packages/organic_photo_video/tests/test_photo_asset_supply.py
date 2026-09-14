from __future__ import annotations

import io
import copy
import tempfile
import unittest
from pathlib import Path

from PIL import Image

from services.photo_asset_supply import PhotoAssetSupplyError, PhotoAssetSupplyService
from config.loader import load_content_recipes


class Client:
    @staticmethod
    def _content(seed=0):
        stream = io.BytesIO()
        Image.new("RGB", (120, 180), (90 + seed, 100, 110)).save(stream, format="PNG")
        return stream.getvalue()

    def download_attachment_bytes(self, attachment):
        content = self._content(int(attachment.get("file_token") or 0))
        return content, attachment.get("name", "look.png"), "image/png", len(content)


class PhotoAssetSupplyTest(unittest.TestCase):
    def test_operator_can_stage_ordered_complete_looks_without_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = PhotoAssetSupplyService(Client(), root=Path(tmp))
            result = service.stage(
                record_id="rec/unsafe", required_roles=["look_a", "look_b", "look_c", "look_d"],
                attachments=[{"file_token": str(i), "name": f"{i}.png"} for i in range(4)],
            )
            self.assertEqual(result["status"], "pending_content_review")
            self.assertEqual([item["role"] for item in result["files"]], ["look_a", "look_b", "look_c", "look_d"])
            self.assertTrue(all(Path(item["path"]).is_file() for item in result["files"]))
            self.assertTrue(Path(result["manifest_path"]).is_file())

    def test_incomplete_upload_is_not_silently_promoted(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = PhotoAssetSupplyService(Client(), root=Path(tmp))
            with self.assertRaisesRegex(PhotoAssetSupplyError, "需要 4 张"):
                service.stage(
                    record_id="rec", required_roles=["look_a", "look_b", "look_c", "look_d"],
                    attachments=[{"file_token": "1", "name": "1.png"}],
                )

    def test_human_confirmation_promotes_exact_bytes_with_recipe_relation(self):
        class Repo:
            def __init__(self): self.items = []
            def list_asset_sets(self, **kwargs): return list(self.items)
            def get_asset_set(self, identity): return next((x for x in self.items if x.asset_set_id == identity), None)
            def upsert_asset_set(self, item): self.items.append(item)
        recipe = next(item for item in load_content_recipes()
                      if item.recipe_id == "PHOTO_TH_PICK_YOUR_LOOK_V3")
        recipe = copy.deepcopy(recipe)
        recipe.recipe_spec_json["visual_rules"]["garment_relations"] = [{
            "relation": "same_outerwear_different_bottom",
            "roles": ["look_b", "look_c"],
        }]
        with tempfile.TemporaryDirectory() as tmp:
            service = PhotoAssetSupplyService(Client(), root=Path(tmp))
            service.stage(
                record_id="rec", required_roles=["look_a", "look_b", "look_c", "look_d"],
                attachments=[{"file_token": str(i), "name": f"{i}.png"} for i in range(4)],
            )
            repo = Repo()
            saved = service.qualify(record_id="rec", recipe=recipe, repository=repo)
            self.assertEqual(saved.status, "enabled")
            self.assertEqual(saved.tags_json["choice_axis"], "pants_or_skirt")
            attrs = saved.manifest_json["content_approval"]["attributes"]
            by_role = {item["role"]: item["asset_id"] for item in saved.manifest_json["assets"]}
            self.assertEqual(attrs[by_role["look_b"]]["outerwear_id"],
                             attrs[by_role["look_c"]]["outerwear_id"])
            self.assertNotEqual(attrs[by_role["look_b"]]["bottom_id"],
                                attrs[by_role["look_c"]]["bottom_id"])
            self.assertEqual(service.qualify(record_id="rec", recipe=recipe, repository=repo).asset_set_id,
                             saved.asset_set_id)

    def test_qualify_is_content_idempotent_when_versions_drift(self):
        """重试同一记录时 staging manifest 被重写为 pending，但内容相同必须
        复用已登记的 asset set；期间其他记录推高版本号也不得触发不可变冲突
        （生产回归：recvuwy4SLaaxj 重试撞 ASSET_..._UPLOAD_<hash> 版本漂移）。"""
        class Repo:
            def __init__(self):
                self.items = []
                self.version_drift = 0

            def list_asset_sets(self, **kwargs):
                return list(self.items)

            def get_asset_set(self, identity):
                return next((x for x in self.items if x.asset_set_id == identity), None)

            def upsert_asset_set(self, item):
                self.items.append(item)

        recipe = next(item for item in load_content_recipes()
                      if item.recipe_id == "PHOTO_TH_PICK_YOUR_LOOK_V3")
        with tempfile.TemporaryDirectory() as tmp:
            service = PhotoAssetSupplyService(Client(), root=Path(tmp))
            service.stage(
                record_id="rec-retry", required_roles=["look_a", "look_b", "look_c", "look_d"],
                attachments=[{"file_token": str(i), "name": f"{i}.png"} for i in range(4)],
            )
            repo = Repo()
            first = service.qualify(record_id="rec-retry", recipe=recipe, repository=repo)
            # 另一条记录登记了更高版本，模拟版本漂移。
            repo.version_drift = 99
            # 重跑 stage_existing（重写 staging manifest 为 pending）后再次 qualify。
            sources = [
                {"task_id": f"task-{i}", "path": first.manifest_json["assets"][i]["path"]}
                for i in range(4)
            ]
            service.stage_existing(
                record_id="rec-retry", sources=sources,
                required_roles=["look_a", "look_b", "look_c", "look_d"],
            )
            second = service.qualify(record_id="rec-retry", recipe=recipe, repository=repo)
            self.assertEqual(second.asset_set_id, first.asset_set_id)
            self.assertEqual(second.asset_set_version, first.asset_set_version)

    def test_agnostic_recipe_takes_category_and_market_from_caller(self):
        """国家无关配方（V3）不声明 category_key/markets：调用方必须能补上，
        否则素材集造不出来，流程会在付费生成之后才失败（生产回归：
        recvv9wN5IeRLg 四套 look 已生成、整组质检通过，却卡在
        『asset set category_key is required』）。"""
        class Repo:
            def __init__(self): self.items = []
            def list_asset_sets(self, **kwargs): return list(self.items)
            def get_asset_set(self, identity):
                return next((x for x in self.items if x.asset_set_id == identity), None)
            def upsert_asset_set(self, item): self.items.append(item)

        recipe = next(item for item in load_content_recipes()
                      if item.recipe_id == "PHOTO_MATCHING_CHOICE_V3")
        spec = recipe.recipe_spec_json
        self.assertFalse(spec.get("category_key"))
        self.assertFalse(spec.get("markets"))
        with tempfile.TemporaryDirectory() as tmp:
            service = PhotoAssetSupplyService(Client(), root=Path(tmp))
            service.stage(
                record_id="rec-vn", required_roles=["look_a", "look_b", "look_c", "look_d"],
                attachments=[{"file_token": str(i), "name": f"{i}.png"} for i in range(4)],
            )
            repo = Repo()
            saved = service.qualify(
                record_id="rec-vn", recipe=recipe, repository=repo,
                category_key="scarf", market="VN",
            )
            self.assertEqual(saved.category_key, "scarf")
            self.assertEqual(saved.market, "VN")
            self.assertEqual(saved.asset_set_key, "VN_SCARF_CHOICE")
            self.assertEqual(saved.status, "enabled")
            self.assertTrue(saved.asset_set_id.startswith("ASSET_VN_SCARF_UPLOAD_"))

    def test_agnostic_recipe_without_category_anywhere_fails_loudly(self):
        class Repo:
            def __init__(self): self.items = []
            def list_asset_sets(self, **kwargs): return list(self.items)
            def get_asset_set(self, identity): return None
            def upsert_asset_set(self, item): self.items.append(item)

        recipe = next(item for item in load_content_recipes()
                      if item.recipe_id == "PHOTO_MATCHING_CHOICE_V3")
        with tempfile.TemporaryDirectory() as tmp:
            service = PhotoAssetSupplyService(Client(), root=Path(tmp))
            service.stage(
                record_id="rec-vn-2", required_roles=["look_a", "look_b", "look_c", "look_d"],
                attachments=[{"file_token": str(i), "name": f"{i}.png"} for i in range(4)],
            )
            with self.assertRaisesRegex(PhotoAssetSupplyError, "素材集缺少类别"):
                service.qualify(record_id="rec-vn-2", recipe=recipe, repository=Repo())

    def test_legacy_recipe_ignores_empty_caller_binding(self):
        """旧配方（TH_V3 声明了 category_key/markets）仍以配方为准：
        调用方传空值时不得把类别抹成空串。"""
        class Repo:
            def __init__(self): self.items = []
            def list_asset_sets(self, **kwargs): return list(self.items)
            def get_asset_set(self, identity):
                return next((x for x in self.items if x.asset_set_id == identity), None)
            def upsert_asset_set(self, item): self.items.append(item)

        recipe = next(item for item in load_content_recipes()
                      if item.recipe_id == "PHOTO_TH_PICK_YOUR_LOOK_V3")
        with tempfile.TemporaryDirectory() as tmp:
            service = PhotoAssetSupplyService(Client(), root=Path(tmp))
            service.stage(
                record_id="rec-th", required_roles=["look_a", "look_b", "look_c", "look_d"],
                attachments=[{"file_token": str(i), "name": f"{i}.png"} for i in range(4)],
            )
            saved = service.qualify(
                record_id="rec-th", recipe=recipe, repository=Repo(),
                category_key="", market="",
            )
            self.assertEqual(saved.category_key, "womenswear")
            self.assertEqual(saved.market, "TH")

    def test_existing_outfit_sources_are_staged_without_copy_or_model_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sources = []
            for index in range(4):
                path = root / f"look-{index}.png"
                path.write_bytes(Client._content(index))
                sources.append({"task_id": f"task-{index}", "look_ref": f"LOOK-{index}", "path": str(path)})
            service = PhotoAssetSupplyService(Client(), root=root / "output")
            result = service.stage_existing(
                record_id="rec", sources=sources,
                required_roles=["look_a", "look_b", "look_c", "look_d"],
            )
            self.assertEqual(result["source"], "existing_outfit_tasks")
            self.assertEqual([item["source_look_ref"] for item in result["files"]],
                             ["LOOK-0", "LOOK-1", "LOOK-2", "LOOK-3"])
