"""CreatOK channel adapter + fallback wiring tests (no real CLI/credits)."""

import json
import struct
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services.image_generator import (  # noqa: E402
    CreatokImageGenerator,
    FallbackShotGenerator,
    GenerationOutcome,
    OpenAIImageGenerator,
    ShotGenerationRequest,
    build_default_photo_generator,
)


def fake_png(path: Path, width: int = 1088, height: int = 1920) -> Path:
    path.write_bytes(
        b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\rIHDR"
        + struct.pack(">II", width, height) + b"\x08\x06\x00\x00\x00" + b"\x00" * 8
    )
    return path


def make_request(output_dir: str) -> ShotGenerationRequest:
    return ShotGenerationRequest(
        task_id="task_probe",
        slot_index=1,
        slot_role="hero",
        shot_version=1,
        plan_shot={"slot_index": 1, "slot_role": "hero", "purpose": "cover"},
        product={"category": "dress"},
        persona_snapshot={"name": "persona"},
        look_snapshot={"recipe": {"top_inner": "白衬衫"}},
        scene_snapshot={"name": "scene"},
        output_dir=output_dir,
    )


def ok_envelope(task_id: str = "tk_1") -> str:
    return json.dumps({
        "ok": True, "cli_version": "test", "command": "image generate",
        "task_id": task_id, "status": "succeeded",
        "data": {"result": {"images": [{"url": "https://img.example/x.jpg?sign=1"}]}},
        "artifacts": {}, "error": None,
    })


class CreatokImageGeneratorTest(unittest.TestCase):
    def setUp(self):
        self.generator = CreatokImageGenerator(poll_timeout=60)

    def test_missing_api_key_fails_fast_without_cli_call(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            "os.environ", {}, clear=False
        ):
            import os
            os.environ.pop("CREATOK_API_KEY", None)
            with patch("services.image_generator.subprocess.run") as run_mock:
                outcome = self.generator.generate_shot(make_request(tmp))
        self.assertFalse(outcome.ok)
        self.assertIn("CREATOK_API_KEY", outcome.error)
        run_mock.assert_not_called()

    def test_success_downloads_image_and_passes_qc(self):
        with tempfile.TemporaryDirectory() as tmp:
            request = make_request(tmp)
            image_path = fake_png(Path(tmp) / "expected.jpg")

            def fake_download(url, target):
                fake_png(target)
                return target

            with patch.dict("os.environ", {"CREATOK_API_KEY": "k"}), \
                 patch("services.image_generator.subprocess.run") as run_mock, \
                 patch("services.image_generator.CreatokImageGenerator._download",
                       staticmethod(fake_download)):
                run_mock.return_value = subprocess.CompletedProcess(
                    [], 0, stdout=ok_envelope(), stderr=""
                )
                outcome = self.generator.generate_shot(request)
            self.assertTrue(outcome.ok, outcome.error)
            self.assertEqual(outcome.provider, "creatok")
            self.assertEqual(outcome.model, "gpt-image-2-official")
            self.assertEqual((outcome.width, outcome.height), (1088, 1920))
            command = run_mock.call_args[0][0]
            options = json.loads(command[command.index("--options") + 1])
            self.assertEqual(options["model"], "gpt-image-2-official")
            self.assertEqual(options["resolution"], "1K")
            self.assertEqual(options["quality"], "low")
            self.assertEqual(options["aspect_ratio"], "9:16")
            self.assertNotIn("--ref", command)
            self.assertTrue(Path(outcome.image_path).exists())
            self.assertTrue((Path(tmp) / "creatok_task_probe_P1_v1" / "envelope.json").exists())

    def test_reference_images_passed_through_ref_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            reference = fake_png(Path(tmp) / "ref.png")
            request = make_request(tmp)
            request.continuity_reference_images = [str(reference)]

            def fake_download(url, target):
                fake_png(target)
                return target

            with patch.dict("os.environ", {"CREATOK_API_KEY": "k"}), \
                 patch("services.image_generator.subprocess.run") as run_mock, \
                 patch("services.image_generator.CreatokImageGenerator._download",
                       staticmethod(fake_download)):
                run_mock.return_value = subprocess.CompletedProcess(
                    [], 0, stdout=ok_envelope(), stderr=""
                )
                outcome = self.generator.generate_shot(request)
            self.assertTrue(outcome.ok, outcome.error)
            command = run_mock.call_args[0][0]
            refs = command[command.index("--ref") + 1].split(",")
            self.assertEqual(refs, [str(reference)])
            self.assertEqual(outcome.raw["reference_count"], 1)

    def test_cli_error_envelope_maps_to_failed_outcome(self):
        with tempfile.TemporaryDirectory() as tmp:
            failure = json.dumps({
                "ok": False, "error": {"kind": "server", "message": "boom"},
            })
            with patch.dict("os.environ", {"CREATOK_API_KEY": "k"}), \
                 patch("services.image_generator.subprocess.run") as run_mock:
                run_mock.return_value = subprocess.CompletedProcess(
                    [], 1, stdout=failure, stderr="stack"
                )
                outcome = self.generator.generate_shot(make_request(tmp))
        self.assertFalse(outcome.ok)
        self.assertIn("creatok server: boom", outcome.error)

    def test_non_json_cli_output_maps_to_failed_outcome(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"CREATOK_API_KEY": "k"}), \
                 patch("services.image_generator.subprocess.run") as run_mock:
                run_mock.return_value = subprocess.CompletedProcess(
                    [], 0, stdout="not json", stderr=""
                )
                outcome = self.generator.generate_shot(make_request(tmp))
        self.assertFalse(outcome.ok)
        self.assertIn("non-JSON", outcome.error)

    def test_quality_omitted_when_disabled(self):
        generator = CreatokImageGenerator(quality="")
        options = generator._options()
        self.assertNotIn("quality", options)


class FallbackShotGeneratorTest(unittest.TestCase):
    def test_primary_success_skips_fallback(self):
        primary = _FakeGenerator(ok=True, provider="creatok")
        fallback = _FakeGenerator(ok=True, provider="openai-image")
        with tempfile.TemporaryDirectory() as tmp:
            outcome = FallbackShotGenerator(primary, fallback).generate_shot(
                make_request(tmp)
            )
        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.raw["channel"], "primary")
        self.assertEqual(fallback.calls, 0)

    def test_primary_failure_hands_off_to_fallback(self):
        primary = _FakeGenerator(ok=False, error="creatok server: boom")
        fallback = _FakeGenerator(ok=True, provider="openai-image")
        with tempfile.TemporaryDirectory() as tmp:
            outcome = FallbackShotGenerator(primary, fallback).generate_shot(
                make_request(tmp)
            )
        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.provider, "openai-image")
        self.assertEqual(outcome.raw["channel"], "fallback")
        self.assertIn("creatok server: boom", outcome.raw["primary_error"])
        self.assertEqual(primary.calls, 1)
        self.assertEqual(fallback.calls, 1)

    def test_primary_exception_still_reaches_fallback(self):
        def explode(self_request):
            raise RuntimeError("cli exploded")

        primary = _FakeGenerator(ok=False)
        primary.generate_shot = explode
        fallback = _FakeGenerator(ok=True, provider="openai-image")
        with tempfile.TemporaryDirectory() as tmp:
            outcome = FallbackShotGenerator(primary, fallback).generate_shot(
                make_request(tmp)
            )
        self.assertTrue(outcome.ok)
        self.assertIn("cli exploded", outcome.raw["primary_error"])

    def test_double_failure_reports_both_channels(self):
        primary = _FakeGenerator(ok=False, error="creatok down")
        fallback = _FakeGenerator(ok=False, error="codex down")
        with tempfile.TemporaryDirectory() as tmp:
            outcome = FallbackShotGenerator(primary, fallback).generate_shot(
                make_request(tmp)
            )
        self.assertFalse(outcome.ok)
        self.assertIn("creatok down", outcome.error)
        self.assertIn("codex down", outcome.error)


class _FakeGenerator:
    def __init__(self, ok, provider="fake", error=""):
        self.ok = ok
        self.provider = provider
        self.error = error
        self.calls = 0

    def generate_shot(self, request):
        self.calls += 1
        return GenerationOutcome(ok=self.ok, provider=self.provider, error=self.error)


class DefaultBuilderTest(unittest.TestCase):
    def test_default_is_creatok_with_codex_fallback(self):
        with patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("OPV_PHOTO_CHANNEL", None)
            generator = build_default_photo_generator()
        self.assertIsInstance(generator, FallbackShotGenerator)
        self.assertIsInstance(generator.primary, CreatokImageGenerator)
        self.assertIsInstance(generator.fallback, OpenAIImageGenerator)

    def test_channel_override_pins_legacy_channel(self):
        with patch.dict("os.environ", {"OPV_PHOTO_CHANNEL": "openai-image"}):
            generator = build_default_photo_generator()
        self.assertIsInstance(generator, OpenAIImageGenerator)

    def test_channel_override_drops_fallback(self):
        with patch.dict("os.environ", {"OPV_PHOTO_CHANNEL": "creatok"}):
            generator = build_default_photo_generator()
        self.assertIsInstance(generator, CreatokImageGenerator)


if __name__ == "__main__":
    unittest.main()
