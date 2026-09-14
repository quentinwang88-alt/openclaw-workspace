"""按市场收敛 execution profile —— 2026-09-14 3C 真跑暴露的断点回归。

背景（证据链见 docs/OPV_VN_SCARF_3C_*）：

``PHOTO_TRAVEL_OUTFIT_V3`` 有两个 **变量完全相同、只有 asset_set_keys 不同** 的
档位：``travel_scene_four_looks``（``TH_WOMENSWEAR_CHOICE``）与
``travel_scene_four_looks_vn``（``VN_SCARF_CHOICE``）。而自动风格参考供给
（STYLE）调用 ``qualify`` 时不指名 profile，旧实现回落到 ``profiles[0]``——
于是 **VN 请求把素材集登记进了泰国档的命名空间**。撞上
``uq_opv_asset_set_version``（``asset_set_key`` + ``asset_set_version`` 唯一）后，
INSERT 静默改写泰国侧的历史停用行、保留它原来的 ``asset_set_id``，本次算出的
content-addressed id 从不落地 ⇒ 下游 pin 成悬空引用 ⇒ 报 ``NEEDS_ASSET``，
而 4 张 look 已经付过费。

修复：档位可选声明 ``markets``；``build_batch`` 与 ``qualify`` 都按本次请求的
市场选档，两个入口给出同一答案。未声明 ``markets`` 的档位对所有市场开放，
因此既有 TH/MX 配方逐字不变（本文件最后一条用例钉住这一点）。
"""
from __future__ import annotations

import copy
import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from PIL import Image

from config import loader
from domain.photo_contracts import validate_execution_profiles
from services.asset_set_service import AssetSetError
from services.photo_asset_supply import PhotoAssetSupplyService

TRAVEL_RECIPE_PATH = (
    Path(__file__).resolve().parents[1] / "config" / "recipes" / "PHOTO_TRAVEL_OUTFIT_V3.json"
)


class Client:
    """最小附件下载替身：按 file_token 造不同颜色的 PNG。"""

    @staticmethod
    def _content(seed=0):
        stream = io.BytesIO()
        Image.new("RGB", (120, 180), (90 + seed, 100, 110)).save(stream, format="PNG")
        return stream.getvalue()

    def download_attachment_bytes(self, attachment):
        content = self._content(int(attachment.get("file_token") or 0))
        return content, attachment.get("name", "look.png"), "image/png", len(content)


class QuietRepo:
    """只实现 qualify 需要的三件事；不模拟任何真实存储。"""

    def __init__(self):
        self.items = []

    def list_asset_sets(self, **kwargs):
        return list(self.items)

    def get_asset_set(self, identity):
        return next((x for x in self.items if x.asset_set_id == identity), None)

    def upsert_asset_set(self, item):
        self.items.append(item)


class HijackingRepo(QuietRepo):
    """模拟仓储层写入后回读不到本次登记的 id（写入被改道到别的行）。

    「槽位被占」本身现在由 ``RdsRepository.upsert_asset_set`` 的真实唯一键语义
    处理（冲突 ⇒ 回滚 + 按索引口径重取版本 + 有界重试），覆盖在
    ``tests/test_rds_repository.py::AssetSetRegistrationTest``。这里钉的是上一层
    的契约：写入没有落到本次 id 时，``qualify`` 绝不能把悬空 id 记进 staging。
    """

    def get_asset_set(self, identity):
        return None


class ExecutionProfileMarketBindingTest(unittest.TestCase):
    def setUp(self):
        self.recipe = copy.deepcopy(loader.load_content_recipe_file(TRAVEL_RECIPE_PATH))
        self.spec = self.recipe.recipe_spec_json
        self.profiles = {item["profile_id"]: item for item in self.spec["execution_profiles"]}

    def _stage(self, service, record_id):
        service.stage(
            record_id=record_id, required_roles=["look_a", "look_b", "look_c", "look_d"],
            attachments=[{"file_token": str(i), "name": f"{i}.png"} for i in range(4)],
        )

    def test_travel_recipe_pins_each_market_to_its_own_asset_set_key(self):
        self.assertEqual(self.profiles["travel_scene_four_looks"]["markets"], ["TH"])
        self.assertEqual(self.profiles["travel_scene_four_looks_vn"]["markets"], ["VN"])
        self.assertEqual(
            self.profiles["travel_scene_four_looks"]["asset_set_keys"],
            ["TH_WOMENSWEAR_CHOICE"],
        )
        self.assertEqual(
            self.profiles["travel_scene_four_looks_vn"]["asset_set_keys"],
            ["VN_SCARF_CHOICE"],
        )

    def test_qualify_binds_the_profile_of_the_requested_market(self):
        """VN 请求 + 空 binding ⇒ 必须落在越南档的键上，不能回落 profiles[0]。"""
        with tempfile.TemporaryDirectory() as tmp:
            service = PhotoAssetSupplyService(Client(), root=Path(tmp))
            self._stage(service, "rec-vn-market")
            saved = service.qualify(
                record_id="rec-vn-market", recipe=self.recipe, repository=QuietRepo(),
                category_key="scarf", market="VN",
            )
            self.assertEqual(saved.asset_set_key, "VN_SCARF_CHOICE")
            self.assertEqual(saved.asset_set_id[:13], "ASSET_VN_SCAR")

    def test_qualify_binds_the_th_profile_for_a_th_request(self):
        """同一份配方，泰国请求走泰国档——按市场而不是按顺序选档。"""
        with tempfile.TemporaryDirectory() as tmp:
            service = PhotoAssetSupplyService(Client(), root=Path(tmp))
            self._stage(service, "rec-th-market")
            saved = service.qualify(
                record_id="rec-th-market", recipe=self.recipe, repository=QuietRepo(),
                category_key="womenswear", market="TH",
            )
            self.assertEqual(saved.asset_set_key, "TH_WOMENSWEAR_CHOICE")
            self.assertEqual(saved.market, "TH")

    def test_explicit_profile_binding_still_wins(self):
        """调用方指了 profile 就照办，不被市场回落覆盖。"""
        with tempfile.TemporaryDirectory() as tmp:
            service = PhotoAssetSupplyService(Client(), root=Path(tmp))
            self._stage(service, "rec-explicit")
            saved = service.qualify(
                record_id="rec-explicit", recipe=self.recipe, repository=QuietRepo(),
                profile_binding={
                    "profile_id": "travel_scene_four_looks_vn",
                    "asset_set_key": "VN_SCARF_CHOICE",
                },
                category_key="scarf", market="VN",
            )
            self.assertEqual(saved.asset_set_key, "VN_SCARF_CHOICE")

    def test_write_that_cannot_be_read_back_fails_loudly_instead_of_pinning_a_ghost_id(self):
        """写入回读不到时必须响亮失败：悬空 pin 会在冻结阶段表现为 NEEDS_ASSET，
        而图已经付过费（本次 3C 真跑就是这么损失一次的）。"""
        class ClientCountingDownloads(Client):
            downloads = 0

            def download_attachment_bytes(self, attachment):
                ClientCountingDownloads.downloads += 1
                return super().download_attachment_bytes(attachment)

        with tempfile.TemporaryDirectory() as tmp:
            service = PhotoAssetSupplyService(ClientCountingDownloads(), root=Path(tmp))
            self._stage(service, "rec-slot-taken")
            downloads_after_stage = ClientCountingDownloads.downloads
            with self.assertRaisesRegex(AssetSetError, "回读不到记录"):
                service.qualify(
                    record_id="rec-slot-taken", recipe=self.recipe,
                    repository=HijackingRepo(), category_key="scarf", market="VN",
                )
            # 登记失败不得重新生图/重新下载源图，暂存清单也不能被标成 qualified。
            self.assertEqual(ClientCountingDownloads.downloads, downloads_after_stage)
            self.assertNotEqual(service.load_staged("rec-slot-taken").get("status"), "qualified")

    def test_profiles_without_markets_stay_open_to_every_market(self):
        """既有一切配方都不声明 markets ⇒ 校验零错误、选档仍是 profiles[0]。

        这条钉住「TH/MX 逐字不变」：不声明就该对所有市场开放。
        """
        for recipe in loader.load_content_recipes():
            errors = validate_execution_profiles(
                recipe.recipe_spec_json, require_profiles=False,
            )
            self.assertEqual(errors, [], recipe.recipe_id)
        spec = copy.deepcopy(self.spec)
        for profile in spec["execution_profiles"]:
            profile.pop("markets", None)
        self.assertEqual(validate_execution_profiles(spec), [])
        with tempfile.TemporaryDirectory() as tmp:
            service = PhotoAssetSupplyService(Client(), root=Path(tmp))
            self._stage(service, "rec-no-markets")
            saved = service.qualify(
                record_id="rec-no-markets",
                recipe=SimpleNamespace(recipe_spec_json=spec, status="active"),
                repository=QuietRepo(), category_key="scarf", market="VN",
            )
            # 没有 markets 声明时保持旧行为：profiles[0]（此处即泰国档）。
            self.assertEqual(saved.asset_set_key, "TH_WOMENSWEAR_CHOICE")

    def test_malformed_markets_is_rejected_by_the_validator(self):
        spec = copy.deepcopy(self.spec)
        spec["execution_profiles"][0]["markets"] = []
        self.assertTrue(any("markets" in error for error in validate_execution_profiles(spec)))
        spec["execution_profiles"][0]["markets"] = ["VN", "VN"]
        self.assertTrue(any("markets" in error for error in validate_execution_profiles(spec)))


if __name__ == "__main__":
    unittest.main()
