"""产品图引用必须能被"从数据库读上下文"的那条路径拿到（C1 的图片版本）。

``load_product_context`` 的权威来自 pipeline 数据库，那里没有图片列表；已审核的
产品图只由 operation bootstrap 落缓存。两条路径不一致时，从数据库读出来的产品会被
当成"没有图"，于是每一个外观断言都被降档 —— 哪怕图就躺在缓存里。这里锁住那段读取，
以及它"取不到就返回空、绝不拖垮加载"的容错。
"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.operation_product_bootstrap import load_cached_product_reference_assets


def _write_cache(root: Path, name: str, *, product_code: str, anchor_card, assets) -> Path:
    path = root / f"{name}.json"
    path.write_text(
        json.dumps(
            {
                "cache_key": name,
                "product_code": product_code,
                "schema_version": "operation-new-sku-anchor-v2-product-cache",
                "anchor_card": anchor_card,
                "product_reference_assets": assets,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


class CachedProductReferenceAssetsTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)
        self.image = self.root / "image.png"
        self.image.write_bytes(b"png")
        patcher = patch.dict(
            os.environ, {"ORIGINAL_SCRIPT_ANCHOR_CACHE_ROOT": str(self.root)}
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _asset(self, token="FT_1"):
        return {
            "role": "PRODUCT_REFERENCE",
            "file_token": token,
            "local_path": str(self.image),
            "sha256": "a" * 64,
            "authority": "OPERATION_TASK_PRODUCT_IMAGES",
        }

    def test_a_matching_cache_entry_is_returned(self):
        _write_cache(
            self.root, "k1", product_code="P1", anchor_card={"a": 1}, assets=[self._asset()]
        )
        assets = load_cached_product_reference_assets("P1", anchor_card={"a": 1})
        self.assertEqual([item["file_token"] for item in assets], ["FT_1"])

    def test_another_products_cache_is_never_borrowed(self):
        _write_cache(
            self.root, "k1", product_code="P2", anchor_card={"a": 1}, assets=[self._asset()]
        )
        self.assertEqual(
            load_cached_product_reference_assets("P1", anchor_card={"a": 1}), []
        )

    def test_a_descriptor_whose_file_is_gone_is_dropped(self):
        missing = dict(self._asset(), local_path=str(self.root / "gone.png"))
        _write_cache(
            self.root, "k1", product_code="P1", anchor_card={"a": 1}, assets=[missing]
        )
        self.assertEqual(
            load_cached_product_reference_assets("P1", anchor_card={"a": 1}), []
        )

    def test_the_entry_matching_the_anchor_card_in_hand_wins(self):
        # 同一 SKU 重新审核过图时，缓存里会同时留着新旧两份；绝不能拿旧的顶新。
        old = self.root / "old.png"
        old.write_bytes(b"old")
        _write_cache(
            self.root,
            "a_old",
            product_code="P1",
            anchor_card={"v": 1},
            assets=[dict(self._asset("FT_OLD"), local_path=str(old))],
        )
        _write_cache(
            self.root,
            "b_new",
            product_code="P1",
            anchor_card={"v": 2},
            assets=[self._asset("FT_NEW")],
        )
        assets = load_cached_product_reference_assets("P1", anchor_card={"v": 2})
        self.assertEqual([item["file_token"] for item in assets], ["FT_NEW"])

    def test_an_empty_product_code_resolves_to_nothing(self):
        _write_cache(
            self.root, "k1", product_code="P1", anchor_card={"a": 1}, assets=[self._asset()]
        )
        self.assertEqual(load_cached_product_reference_assets(""), [])

    def test_a_missing_cache_directory_resolves_to_nothing(self):
        with patch.dict(
            os.environ, {"ORIGINAL_SCRIPT_ANCHOR_CACHE_ROOT": str(self.root / "nope")}
        ):
            self.assertEqual(load_cached_product_reference_assets("P1"), [])

    def test_a_corrupt_cache_file_is_skipped_rather_than_raised(self):
        (self.root / "broken.json").write_text("{not json", encoding="utf-8")
        _write_cache(
            self.root, "k1", product_code="P1", anchor_card={"a": 1}, assets=[self._asset()]
        )
        assets = load_cached_product_reference_assets("P1", anchor_card={"a": 1})
        self.assertEqual([item["file_token"] for item in assets], ["FT_1"])


class ContextLoaderAttachesThemTest(unittest.TestCase):
    """``load_product_context`` 带不上图时只能退化为空，不能把加载打挂。"""

    def test_a_broken_cache_never_fails_the_load(self):
        from core.original_batch_executor import _resolve_cached_reference_assets

        with patch(
            "core.operation_product_bootstrap.load_cached_product_reference_assets",
            side_effect=OSError("cache exploded"),
        ):
            self.assertEqual(_resolve_cached_reference_assets("P1", {}), [])
