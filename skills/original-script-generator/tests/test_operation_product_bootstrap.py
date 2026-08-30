import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from core.operation_product_bootstrap import build_operation_product_context


class OperationProductBootstrapTest(unittest.TestCase):
    def test_builds_new_sku_context_from_operation_images_and_central_catalog(self):
        with tempfile.TemporaryDirectory() as tmp:
            image_path = Path(tmp) / "image.png"
            image_path.write_bytes(b"test")
            operation_client = MagicMock()
            operation_client.download_attachment.return_value = image_path
            llm = MagicMock()
            llm.call_json.return_value = {
                "product_positioning_one_liner": "深蓝图案长幅丝巾",
                "hard_anchors": ["深蓝底色", "长幅形态"],
                "display_anchors": ["领口佩戴结果"],
                "parameter_anchors": [],
                "key_visual_constraints": [],
            }
            snapshot = {
                "status": "AVAILABLE",
                "catalog": [{"value_id": "ARG_1"}],
                "confirmed_argument_count": 1,
                "available_argument_count": 1,
                "mapped_argument_count": 0,
                "unmapped_argument_count": 1,
                "snapshot_hash": "SNAP_1",
            }
            task = {
                "product_code": "P_NEW",
                "product_images": [{"file_token": "FILE_1", "name": "image.png"}],
                "target_country": "泰国",
                "target_language": "泰语",
                "top_category": "配饰",
                "product_type": "丝巾",
            }
            with patch.dict(
                "os.environ",
                {"ORIGINAL_SCRIPT_ANCHOR_CACHE_ROOT": str(Path(tmp) / "shared-cache")},
            ), patch(
                "core.operation_product_bootstrap.load_verified_selling_point_catalog",
                return_value=snapshot,
            ):
                context = build_operation_product_context(
                    operation_client=operation_client,
                    task=task,
                    record_id="REC_1",
                    output_dir=Path(tmp),
                    voiceover_root="/tmp/voiceover",
                    llm_client=llm,
                )

            self.assertEqual(context["source_run_id"], "OPERATION_NEW_SKU_BOOTSTRAP")
            self.assertEqual(context["product_type"], "丝巾")
            self.assertEqual(context["anchor_card"]["anchor_authority"], "OPERATION_TASK_PRODUCT_IMAGES")
            self.assertEqual(context["selling_point_catalog"], [{"value_id": "ARG_1"}])
            self.assertEqual(context["structure_route"]["status"], "REBUILD_REQUIRED")
            self.assertEqual(len(context["product_reference_assets"]), 1)
            self.assertTrue(Path(context["product_reference_assets"][0]["local_path"]).is_file())
            operation_client.download_attachment.assert_called_once()
            llm.call_json.assert_called_once()

    def test_same_sku_and_images_reuse_anchor_across_operation_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            image_path = root / "image.png"
            image_path.write_bytes(b"test")
            operation_client = MagicMock()
            operation_client.download_attachment.return_value = image_path
            llm = MagicMock()
            llm.call_json.return_value = {
                "product_positioning_one_liner": "浅绿图案头巾",
                "hard_anchors": ["浅绿色", "方巾形态"],
                "display_anchors": ["头部佩戴结果"],
                "parameter_anchors": [],
                "key_visual_constraints": [],
            }
            task = {
                "product_code": "P_CACHE",
                "product_images": [{"file_token": "FILE_CACHE", "name": "image.png"}],
                "target_country": "泰国",
                "target_language": "泰语",
                "top_category": "配饰",
                "product_type": "头巾",
            }
            snapshot = {"status": "AVAILABLE", "catalog": [], "snapshot_hash": "S"}
            with patch.dict(
                "os.environ",
                {"ORIGINAL_SCRIPT_ANCHOR_CACHE_ROOT": str(root / "shared-cache")},
            ), patch(
                "core.operation_product_bootstrap.load_verified_selling_point_catalog",
                return_value=snapshot,
            ), patch(
                "core.operation_product_bootstrap.validate_anchor_card_payload",
                return_value=None,
            ):
                build_operation_product_context(
                    operation_client=operation_client,
                    task=task,
                    record_id="REC_A",
                    output_dir=root / "run-a",
                    voiceover_root="/tmp/voiceover",
                    llm_client=llm,
                )
                build_operation_product_context(
                    operation_client=operation_client,
                    task=task,
                    record_id="REC_B",
                    output_dir=root / "run-b",
                    voiceover_root="/tmp/voiceover",
                    llm_client=llm,
                )

            operation_client.download_attachment.assert_called_once()
            llm.call_json.assert_called_once()


if __name__ == "__main__":
    unittest.main()
