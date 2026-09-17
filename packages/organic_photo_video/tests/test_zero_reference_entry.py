"""零外部图入口（方案 §6.5）——适配点守卫语义。

完整链路（外部合同零页行 → STYLE 零参考 → 规划器+人物包生成）由
真实样片验收覆盖；此处锁定三个入口的放行/拒绝边界：
- 手工模式空附件仍拒绝（不全局取消旧校验）；
- allow_empty 只对显式开启者放行。
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT))


class ZeroReferenceEntryTest(unittest.TestCase):
    def test_stage_reference_images_empty_gate(self):
        from services.photo_asset_supply import (
            PhotoAssetSupplyError, PhotoAssetSupplyService)

        class _Client:
            pass

        service = PhotoAssetSupplyService(_Client(), root=Path(tempfile.mkdtemp()))
        with self.assertRaises(PhotoAssetSupplyError):
            service.stage_reference_images(
                record_id="r", attachments=[], reference_kind="style")
        self.assertEqual(
            service.stage_reference_images(
                record_id="r", attachments=[], reference_kind="style",
                allow_empty=True),
            [])

    def test_analyze_reference_empty_gate(self):
        from services.photo_reference_vision import (
            PhotoReferenceVisionError, PhotoReferenceVisionService)

        service = PhotoReferenceVisionService(root=Path(tempfile.mkdtemp()))
        with self.assertRaises(PhotoReferenceVisionError):
            service.analyze_reference(
                record_id="r", paths=[], theme={"theme_key": "t"},
                category_key="tops")
        analysis = service.analyze_reference(
            record_id="r", paths=[], theme={"theme_key": "t"},
            category_key="tops", content_requirement="文字化搭配要点",
            allow_empty=True)
        self.assertEqual(analysis["per_reference"], [])
        self.assertEqual(analysis["reference_count"], 0)
        self.assertEqual(analysis["content_requirement"], "文字化搭配要点")

    def test_prepare_empty_gate(self):
        from services.photo_style_reference_supply import (
            PhotoStyleReferenceError, PhotoStyleReferenceSupplyService)

        service = PhotoStyleReferenceSupplyService(
            generator=object(), root=Path(tempfile.mkdtemp()))
        # allow_empty=False：空路径仍拒绝（手工参考模式行为不变）
        with self.assertRaises(PhotoStyleReferenceError):
            service.prepare(
                record_id="r", reference_paths=[], theme={"theme_key": "t"},
                account=object(), persona={})
        # allow_empty=True：越过路径校验（后续规划为真实链路，此处只验入口
        # 不因缺参考图立即抛错——用最小桩走到 looks 规划）
        try:
            service.prepare(
                record_id="r", reference_paths=[], theme={"theme_key": "t"},
                account=object(), persona={}, allow_empty_references=True)
        except PhotoStyleReferenceError as exc:
            self.assertNotIn("风格参考图缺失", str(exc))  # 不是入口拒绝
        except Exception:
            pass  # 桩对象在更深处失败可接受：入口已放行


if __name__ == "__main__":
    unittest.main()
