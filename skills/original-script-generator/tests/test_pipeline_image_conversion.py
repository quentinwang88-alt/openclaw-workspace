#!/usr/bin/env python3
"""模型图片格式归一回归测试。"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.pipeline import OriginalScriptPipeline, ValidationError  # noqa: E402


class PipelineImageConversionTest(unittest.TestCase):
    def test_supported_png_is_reused_without_conversion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            image_path = Path(tmp_dir) / "image.png"
            image_path.write_bytes(b"png")

            with mock.patch("core.pipeline.subprocess.run") as run_mock:
                normalized = OriginalScriptPipeline._ensure_model_supported_image(
                    image_path,
                    {"type": "image/png"},
                )

            self.assertEqual(normalized, image_path)
            run_mock.assert_not_called()

    def test_heic_is_converted_to_jpeg_before_model_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            image_path = Path(tmp_dir) / "sample.heic"
            image_path.write_bytes(b"heic")

            def fake_run(cmd, check, capture_output, text):  # type: ignore[no-untyped-def]
                Path(cmd[-1]).write_bytes(b"jpeg")
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with mock.patch("core.pipeline.subprocess.run", side_effect=fake_run) as run_mock:
                normalized = OriginalScriptPipeline._ensure_model_supported_image(
                    image_path,
                    {"type": "image/heic"},
                )

            self.assertEqual(normalized.name, "sample.converted.jpg")
            self.assertTrue(normalized.exists())
            run_mock.assert_called_once()

    def test_failed_heic_conversion_raises_validation_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            image_path = Path(tmp_dir) / "sample.heic"
            image_path.write_bytes(b"heic")

            with mock.patch(
                "core.pipeline.subprocess.run",
                side_effect=subprocess.CalledProcessError(1, ["sips"], stderr="bad image"),
            ):
                with self.assertRaises(ValidationError) as ctx:
                    OriginalScriptPipeline._ensure_model_supported_image(
                        image_path,
                        {"type": "image/heic"},
                    )

            self.assertIn("图片格式转换失败", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
