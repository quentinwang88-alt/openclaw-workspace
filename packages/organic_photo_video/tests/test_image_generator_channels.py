"""Channel adapter + chain wiring tests (no real CLI/credits/network)."""

import base64
import io
import json
import struct
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from PIL import Image

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services.image_generator import (  # noqa: E402
    ChannelChainShotGenerator,
    CreatokImageGenerator,
    FallbackShotGenerator,
    GenerationOutcome,
    OneRouteImageGenerator,
    OpenAIImageGenerator,
    ShotGenerationRequest,
    _ChannelHealth,
    build_default_photo_generator,
    check_portrait_916,
    classify_error_kind,
    parse_channel_chain,
    prepare_sunburst_primary_reference,
    reset_channel_health,
    sticky_channel_preference,
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


class OpenAIImageGeneratorTest(unittest.TestCase):
    @staticmethod
    def _schemas_module():
        module = ModuleType("core.schemas")
        module.ImageTaskRequest = SimpleNamespace(from_dict=lambda payload: payload)
        return module

    def test_sunburst_primary_reference_is_contained_on_cached_9x16_canvas(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "narrow-person.png"
            Image.new("RGB", (300, 900), (180, 90, 45)).save(source)

            first = prepare_sunburst_primary_reference(str(source), tmp)
            second = prepare_sunburst_primary_reference(str(source), tmp)

            self.assertEqual(first, second)
            with Image.open(source) as original:
                self.assertEqual(original.size, (300, 900))
            with Image.open(first) as canvas:
                self.assertEqual(canvas.size, (1080, 1920))
                self.assertEqual(canvas.mode, "RGB")
                self.assertEqual(canvas.getpixel((0, 0)), (242, 242, 242))
                self.assertEqual(canvas.getpixel((540, 960)), (180, 90, 45))

    def test_sunburst_request_replaces_only_the_primary_reference(self):
        with tempfile.TemporaryDirectory() as tmp:
            primary = Path(tmp) / "square-product.png"
            secondary = Path(tmp) / "persona.png"
            Image.new("RGB", (640, 640), (240, 240, 240)).save(primary)
            Image.new("RGB", (900, 1600), (210, 180, 160)).save(secondary)
            output = fake_png(Path(tmp) / "sunburst-output.png", 941, 1672)
            captured = {}

            def process_task(payload):
                captured.update(payload)
                return SimpleNamespace(
                    status="success",
                    output_image_paths=[str(output)],
                    error_message="",
                    task_id="task_probe_P1_v1",
                    model="gpt-image-2.5-sunburst",
                )

            service = SimpleNamespace(
                settings=SimpleNamespace(effective_model="gpt-image-2.5-sunburst"),
                process_task=process_task,
            )
            request = make_request(tmp)
            request.product = {"reference_images": [str(primary)]}
            request.persona_snapshot = {"local_reference_images": [str(secondary)]}

            with patch.dict(sys.modules, {"core.schemas": self._schemas_module()}):
                outcome = OpenAIImageGenerator(service=service).generate_shot(request)

            normalized = captured["input_image_paths"][0]
            self.assertNotEqual(normalized, str(primary))
            self.assertEqual(captured["input_image_paths"][1], str(secondary))
            with Image.open(normalized) as canvas:
                self.assertEqual(canvas.size, (1080, 1920))
            self.assertIn("light-gray 9:16 composition canvas", captured["prompt"])
            self.assertTrue(outcome.raw["sunburst_canvas_normalized"])
            self.assertTrue(outcome.ok, outcome.error)

    def test_other_models_keep_the_original_primary_reference(self):
        with tempfile.TemporaryDirectory() as tmp:
            primary = Path(tmp) / "square-product.png"
            Image.new("RGB", (640, 640), (240, 240, 240)).save(primary)
            output = fake_png(Path(tmp) / "flare-output.png", 941, 1672)
            captured = {}

            def process_task(payload):
                captured.update(payload)
                return SimpleNamespace(
                    status="success",
                    output_image_paths=[str(output)],
                    error_message="",
                    task_id="task_probe_P1_v1",
                    model="gpt-image-2.5-flare",
                )

            service = SimpleNamespace(
                settings=SimpleNamespace(effective_model="gpt-image-2.5-flare"),
                process_task=process_task,
            )
            request = make_request(tmp)
            request.product = {"reference_images": [str(primary)]}

            with patch.dict(sys.modules, {"core.schemas": self._schemas_module()}):
                outcome = OpenAIImageGenerator(service=service).generate_shot(request)

            self.assertEqual(captured["input_image_paths"][0], str(primary))
            self.assertNotIn("sunburst_canvas_normalized", outcome.raw)
            self.assertTrue(outcome.ok, outcome.error)

    def test_records_the_model_resolved_by_the_shared_service(self):
        with tempfile.TemporaryDirectory() as tmp:
            image_path = fake_png(Path(tmp) / "sunburst.png")
            result = SimpleNamespace(
                status="success",
                output_image_paths=[str(image_path)],
                error_message="",
                task_id="task_probe_P1_v1",
                model="gpt-image-2.5-sunburst",
            )
            service = SimpleNamespace(
                settings=SimpleNamespace(effective_model="gpt-image-2.5-sunburst"),
                process_task=lambda _request: result,
            )

            with patch.dict(sys.modules, {"core.schemas": self._schemas_module()}):
                outcome = OpenAIImageGenerator(service=service).generate_shot(
                    make_request(tmp)
                )

        self.assertTrue(outcome.ok, outcome.error)
        self.assertEqual(outcome.model, "gpt-image-2.5-sunburst")

    def test_records_resolved_model_when_generation_fails(self):
        service = SimpleNamespace(
            settings=SimpleNamespace(effective_model="gpt-image-2.5-sunburst"),
            process_task=lambda _request: SimpleNamespace(
                status="failed",
                output_image_paths=[],
                error_message="model unavailable",
                task_id="task_probe_P1_v1",
                model="gpt-image-2.5-sunburst",
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(sys.modules, {"core.schemas": self._schemas_module()}):
                outcome = OpenAIImageGenerator(service=service).generate_shot(
                    make_request(tmp)
                )

        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.model, "gpt-image-2.5-sunburst")


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
    def __init__(self, ok, provider="fake", error="", channel_name="", error_kind=""):
        self.ok = ok
        self.provider = provider
        self.error = error
        self.error_kind = error_kind
        self.calls = 0
        if channel_name:
            self.channel_name = channel_name

    def generate_shot(self, request):
        self.calls += 1
        return GenerationOutcome(
            ok=self.ok, provider=self.provider, error=self.error,
            error_kind=self.error_kind, image_path="/tmp/fake.png" if self.ok else None,
        )


class DefaultBuilderTest(unittest.TestCase):
    def test_default_is_the_1route_codex_creatok_chain(self):
        """默认优先级：1route → codex → creatok。"""
        with patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("OPV_PHOTO_CHANNEL", None)
            generator = build_default_photo_generator()
        self.assertIsInstance(generator, ChannelChainShotGenerator)
        self.assertEqual(generator.channel_chain, ["1route", "codex", "creatok"])
        self.assertIsInstance(generator.channels[0], OneRouteImageGenerator)
        self.assertIsInstance(generator.channels[1], OpenAIImageGenerator)
        self.assertIsInstance(generator.channels[2], CreatokImageGenerator)

    def test_explicit_chain_expression_is_respected(self):
        with patch.dict("os.environ", {"OPV_PHOTO_CHANNEL": "creatok>codex>1route"}):
            generator = build_default_photo_generator()
        self.assertEqual(generator.channel_chain, ["creatok", "codex", "1route"])

    def test_legacy_creatok_fallback_keeps_its_pairing(self):
        with patch.dict("os.environ", {"OPV_PHOTO_CHANNEL": "creatok_fallback"}):
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

    def test_codex_fallback_flips_direction(self):
        """codex 主力 + CreatoK 兜底：额度耗尽时自动切通道，不裸奔。"""
        with patch.dict("os.environ", {"OPV_PHOTO_CHANNEL": "codex_fallback"}):
            generator = build_default_photo_generator()
        self.assertIsInstance(generator, FallbackShotGenerator)
        self.assertIsInstance(generator.primary, OpenAIImageGenerator)
        self.assertIsInstance(generator.fallback, CreatokImageGenerator)

    def test_oneroute_only_chain_has_no_fallback(self):
        with patch.dict("os.environ", {"OPV_PHOTO_CHANNEL": "1route"}):
            generator = build_default_photo_generator()
        self.assertIsInstance(generator, OneRouteImageGenerator)

    def test_unknown_tokens_degrade_to_the_rest_of_the_chain(self):
        self.assertEqual(parse_channel_chain("1route>bogus>creatok"), ["1route", "creatok"])
        self.assertEqual(parse_channel_chain("nonsense"), ["1route", "codex", "creatok"])


class OneRouteImageGeneratorTest(unittest.TestCase):
    """1route 适配器：OpenAI 兼容 HTTP，不触网。"""

    API_KEY = "test-key-1route"

    def setUp(self):
        reset_channel_health()
        self.generator = OneRouteImageGenerator(
            api_key=self.API_KEY, base_url="https://image-api.1route.dev"
        )

    @staticmethod
    def _png_b64(width: int = 1088, height: int = 1920) -> str:
        buffer = io.BytesIO()
        Image.new("RGB", (width, height), (200, 180, 160)).save(buffer, format="PNG")
        return base64.b64encode(buffer.getvalue()).decode("ascii")

    def _opener(self, *results):
        """Return a fake urlopen that yields/raises each queued result in order."""
        queue = list(results)
        calls = []

        def opener(request, timeout=None):
            calls.append(request)
            item = queue.pop(0) if queue else {"data": [{"b64_json": self._png_b64()}]}
            if isinstance(item, BaseException):
                raise item
            body = json.dumps(item).encode("utf-8")

            class _Response:
                def read(self_inner):
                    return body

                def __enter__(self_inner):
                    return self_inner

                def __exit__(self_inner, *exc):
                    return False

            return _Response()

        opener.calls = calls
        return opener

    def test_generates_without_references_and_passes_qc(self):
        opener = self._opener()
        generator = OneRouteImageGenerator(
            api_key=self.API_KEY, base_url="https://image-api.1route.dev", opener=opener
        )
        with tempfile.TemporaryDirectory() as tmp:
            outcome = generator.generate_shot(make_request(tmp))
            self.assertTrue(outcome.ok, outcome.error)
            self.assertEqual(outcome.provider, "1route")
            self.assertEqual(outcome.model, "gpt-image-2.5-sunburst")
            self.assertEqual((outcome.width, outcome.height), (1088, 1920))
            self.assertTrue(Path(outcome.image_path).exists())

        request = opener.calls[0]
        self.assertTrue(request.full_url.endswith("/v1/images/generations"))
        self.assertEqual(request.get_header("Authorization"), f"Bearer {self.API_KEY}")
        payload = json.loads(request.data.decode("utf-8"))
        self.assertEqual(payload["model"], "gpt-image-2.5-sunburst")
        # Must be a true 9:16 request: the relay honours `size` literally, so the
        # OpenAI-standard 1024x1536 (=2:3) made the channel fail its own gate.
        self.assertEqual(payload["size"], "1088x1920")
        self.assertEqual(payload["quality"], "high")
        self.assertEqual(payload["n"], 1)
        self.assertNotIn("image", payload)

    def test_requested_size_itself_clears_the_9_16_gate(self):
        """请求尺寸必须自身满足 9:16 闸门，否则该通道永远过不了自己的质检。

        中转站字面遵守 ``size``（不像 codex 会重解释后仍返回 9:16），所以请求
        OpenAI 标准的 1024x1536 会原样拿回 2:3 图，被 ``check_portrait_916`` 拒掉。
        """
        generator = OneRouteImageGenerator(api_key=self.API_KEY)
        width, height = (int(part) for part in generator.size.split("x"))
        self.assertTrue(check_portrait_916((width, height)), generator.size)

    def test_size_env_override_is_honoured(self):
        with patch.dict("os.environ", {"OPV_ONEROUTE_IMAGE_SIZE": "1024x1792"}):
            generator = OneRouteImageGenerator(api_key=self.API_KEY)
        self.assertEqual(generator.size, "1024x1792")

    def test_missing_api_key_fails_fast_without_http(self):
        # The key is resolved at construction time, so the env override has to
        # wrap the constructor too (ambient .env may already carry a real key).
        opener = self._opener()
        with patch.dict("os.environ", {"OPV_ONEROUTE_API_KEY": "", "ONEROUTE_API_KEY": ""}), \
             tempfile.TemporaryDirectory() as tmp:
            generator = OneRouteImageGenerator(
                api_key="", base_url="https://image-api.1route.dev", opener=opener
            )
            outcome = generator.generate_shot(make_request(tmp))
        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.error_kind, "config")
        self.assertEqual(opener.calls, [])

    def test_references_use_multipart_edits_with_local_files(self):
        reference = Path(tempfile.mkdtemp()) / "product.png"
        Image.new("RGB", (640, 640), (240, 240, 240)).save(reference)
        opener = self._opener()
        generator = OneRouteImageGenerator(
            api_key=self.API_KEY, base_url="https://image-api.1route.dev", opener=opener,
            model="gpt-image-2",
        )
        with tempfile.TemporaryDirectory() as tmp:
            request = make_request(tmp)
            request.product = {"reference_images": [str(reference)]}
            outcome = generator.generate_shot(request)
        self.assertTrue(outcome.ok, outcome.error)
        posted = opener.calls[0]
        self.assertTrue(posted.full_url.endswith("/v1/images/edits"))
        self.assertIn("multipart/form-data; boundary=", posted.get_header("Content-type"))
        body = posted.data
        self.assertIn(b'name="model"', body)
        self.assertIn(b"gpt-image-2", body)
        self.assertIn(b'name="image[]"', body)
        self.assertIn(b"product.png", body)
        self.assertEqual(outcome.raw["reference_count"], 1)
        self.assertEqual(outcome.raw["edit_mode"], "multipart")

    def test_json_edit_mode_sends_data_url_references(self):
        reference = Path(tempfile.mkdtemp()) / "product.png"
        Image.new("RGB", (640, 640), (240, 240, 240)).save(reference)
        opener = self._opener()
        generator = OneRouteImageGenerator(
            api_key=self.API_KEY, base_url="https://image-api.1route.dev", opener=opener,
            edit_mode="json", model="gpt-image-2",
        )
        with tempfile.TemporaryDirectory() as tmp:
            request = make_request(tmp)
            request.product = {"reference_images": [str(reference)]}
            outcome = generator.generate_shot(request)
        self.assertTrue(outcome.ok, outcome.error)
        posted = opener.calls[0]
        self.assertTrue(posted.full_url.endswith("/v1/images/generations"))
        payload = json.loads(posted.data.decode("utf-8"))
        self.assertEqual(len(payload["image"]), 1)
        self.assertTrue(payload["image"][0].startswith("data:image/png;base64,"))

    def test_sunburst_normalizes_primary_reference_canvas(self):
        reference = Path(tempfile.mkdtemp()) / "narrow.png"
        Image.new("RGB", (300, 900), (180, 90, 45)).save(reference)
        opener = self._opener()
        generator = OneRouteImageGenerator(
            api_key=self.API_KEY, base_url="https://image-api.1route.dev", opener=opener
        )
        with tempfile.TemporaryDirectory() as tmp:
            request = make_request(tmp)
            request.product = {"reference_images": [str(reference)]}
            outcome = generator.generate_shot(request)
            self.assertTrue(outcome.ok, outcome.error)
            self.assertTrue(outcome.raw["sunburst_canvas_normalized"])
            canvas = Path(outcome.raw["primary_reference_canvas"])
            self.assertTrue(canvas.is_file())
            with Image.open(canvas) as opened:
                self.assertEqual(opened.size, (1080, 1920))
        self.assertIn(
            b"light-gray 9:16 composition canvas", opener.calls[0].data
        )

    def test_model_ladder_falls_back_to_the_secondary_model(self):
        """默认模型失败 → 备选模型 gpt-image-2 重试。"""
        opener = self._opener(
            {"error": {"message": "model unavailable"}},
            {"data": [{"b64_json": self._png_b64()}]},
        )
        generator = OneRouteImageGenerator(
            api_key=self.API_KEY, base_url="https://image-api.1route.dev", opener=opener
        )
        with tempfile.TemporaryDirectory() as tmp:
            outcome = generator.generate_shot(make_request(tmp))
        self.assertEqual(generator.model_ladder(), ["gpt-image-2.5-sunburst", "gpt-image-2"])
        # First attempt has no payload → ladder continues; second succeeds.
        self.assertEqual(len(opener.calls), 2)
        self.assertTrue(outcome.ok, outcome.error)
        self.assertEqual(outcome.model, "gpt-image-2")
        self.assertEqual(len(outcome.raw["model_attempts"]), 1)

    def test_http_error_is_classified_and_does_not_raise(self):
        import urllib.error

        error = urllib.error.HTTPError(
            "https://image-api.1route.dev/v1/images/generations", 401,
            "Unauthorized", {}, io.BytesIO(b'{"error":"invalid key"}'),
        )
        opener = self._opener(error)
        generator = OneRouteImageGenerator(
            api_key=self.API_KEY, base_url="https://image-api.1route.dev", opener=opener
        )
        with tempfile.TemporaryDirectory() as tmp:
            outcome = generator.generate_shot(make_request(tmp))
        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.error_kind, "auth")
        # A rejected credential is model-independent → no second model attempt.
        self.assertEqual(len(opener.calls), 1)

    def test_non_portrait_output_fails_quality_gate(self):
        opener = self._opener({"data": [{"b64_json": self._png_b64(1024, 1024)}]})
        generator = OneRouteImageGenerator(
            api_key=self.API_KEY, base_url="https://image-api.1route.dev", opener=opener,
            model="gpt-image-2",
        )
        with tempfile.TemporaryDirectory() as tmp:
            outcome = generator.generate_shot(make_request(tmp))
        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.error_kind, "quality")
        self.assertIn("not 9:16 portrait", outcome.error)

    def test_url_payload_is_downloaded(self):
        opener = self._opener({"data": [{"url": "https://img.example/x.png"}]})
        generator = OneRouteImageGenerator(
            api_key=self.API_KEY, base_url="https://image-api.1route.dev", opener=opener
        )
        downloaded = {}

        def fake_download(url, target):
            downloaded["url"] = url
            Image.new("RGB", (1088, 1920), (10, 20, 30)).save(target, format="PNG")
            return target

        with patch.object(OneRouteImageGenerator, "_download", staticmethod(fake_download)), \
             tempfile.TemporaryDirectory() as tmp:
            outcome = generator.generate_shot(make_request(tmp))
        self.assertTrue(outcome.ok, outcome.error)
        self.assertEqual(downloaded["url"], "https://img.example/x.png")


class ChannelChainShotGeneratorTest(unittest.TestCase):
    def setUp(self):
        reset_channel_health()

    @staticmethod
    def _chain(*generators, threshold=3):
        for generator in generators:
            generator.channel_name = generator.provider
        return ChannelChainShotGenerator(
            list(generators),
            labels=[g.provider for g in generators],
            health=_ChannelHealth(threshold=threshold, cooldown=300.0),
            sticky=False,
        )

    def test_priority_order_stops_at_the_first_success(self):
        first = _FakeGenerator(ok=True, provider="1route")
        second = _FakeGenerator(ok=True, provider="codex")
        third = _FakeGenerator(ok=True, provider="creatok")
        with tempfile.TemporaryDirectory() as tmp:
            outcome = self._chain(first, second, third).generate_shot(make_request(tmp))
        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.provider, "1route")
        self.assertEqual(outcome.raw["channel"], "1route")
        self.assertEqual(outcome.raw["channel_index"], 0)
        self.assertEqual(outcome.raw["channel_chain"], ["1route", "codex", "creatok"])
        self.assertEqual((second.calls, third.calls), (0, 0))

    def test_chain_advances_until_a_channel_succeeds(self):
        first = _FakeGenerator(ok=False, provider="1route", error="1route http 503: busy")
        second = _FakeGenerator(ok=False, provider="codex", error="codex quota", error_kind="rate_limit")
        third = _FakeGenerator(ok=True, provider="creatok")
        with tempfile.TemporaryDirectory() as tmp:
            outcome = self._chain(first, second, third).generate_shot(make_request(tmp))
        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.provider, "creatok")
        self.assertEqual(len(outcome.raw["attempts"]), 2)
        self.assertEqual(outcome.raw["attempts"][1]["error_kind"], "rate_limit")

    def test_total_failure_reports_the_full_trace(self):
        first = _FakeGenerator(ok=False, provider="1route", error="1route down")
        second = _FakeGenerator(ok=False, provider="codex", error="codex down")
        third = _FakeGenerator(ok=False, provider="creatok", error="creatok down")
        with tempfile.TemporaryDirectory() as tmp:
            outcome = self._chain(first, second, third).generate_shot(make_request(tmp))
        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.error_kind, "exhausted")
        for expected in ("1route down", "codex down", "creatok down"):
            self.assertIn(expected, outcome.error)
        self.assertEqual(len(outcome.raw["attempts"]), 3)

    def test_channel_exception_is_contained_inside_the_chain(self):
        def explode(_request):
            raise RuntimeError("transport exploded")

        broken = _FakeGenerator(ok=True, provider="1route")
        broken.generate_shot = explode
        fallback = _FakeGenerator(ok=True, provider="codex")
        with tempfile.TemporaryDirectory() as tmp:
            outcome = self._chain(broken, fallback).generate_shot(make_request(tmp))
        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.provider, "codex")
        self.assertIn("transport exploded", outcome.raw["attempts"][0]["error"])

    def test_breaker_stops_paying_the_timeout_on_every_shot(self):
        failing = _FakeGenerator(ok=False, provider="1route", error="1route timeout")
        failing.channel_name = "1route"
        healthy = _FakeGenerator(ok=True, provider="codex")
        healthy.channel_name = "codex"
        chain = ChannelChainShotGenerator(
            [failing, healthy], labels=["1route", "codex"],
            health=_ChannelHealth(threshold=2, cooldown=300.0), sticky=False,
        )
        with tempfile.TemporaryDirectory() as tmp:
            for _ in range(5):
                outcome = chain.generate_shot(make_request(tmp))
        self.assertTrue(outcome.ok)
        # Two probe attempts, then the breaker short-circuits the dead channel.
        self.assertEqual(failing.calls, 2)
        self.assertEqual(outcome.raw["attempts"][0]["skipped"], "breaker_open")
        self.assertTrue(chain.health.report()["1route"]["open"])

    def test_auth_failure_trips_the_breaker_immediately(self):
        failing = _FakeGenerator(
            ok=False, provider="1route", error="1route http 401: bad key",
            error_kind="auth",
        )
        failing.channel_name = "1route"
        healthy = _FakeGenerator(ok=True, provider="codex")
        healthy.channel_name = "codex"
        chain = ChannelChainShotGenerator(
            [failing, healthy], labels=["1route", "codex"],
            health=_ChannelHealth(threshold=3, cooldown=300.0),
        )
        with tempfile.TemporaryDirectory() as tmp:
            for _ in range(3):
                chain.generate_shot(make_request(tmp))
        self.assertEqual(failing.calls, 1)

    def test_success_resets_the_failure_counter(self):
        flaky = _FakeGenerator(ok=False, provider="1route", error="1route 503 busy")
        flaky.channel_name = "1route"
        chain = ChannelChainShotGenerator(
            [flaky], labels=["1route"],
            health=_ChannelHealth(threshold=2, cooldown=300.0),
        )
        with tempfile.TemporaryDirectory() as tmp:
            chain.generate_shot(make_request(tmp))
            flaky.ok = True
            chain.generate_shot(make_request(tmp))
        self.assertFalse(chain.health.report().get("1route", {}).get("open", False))
        self.assertNotIn("1route", chain.health.report())

    def test_empty_chain_is_rejected(self):
        with self.assertRaises(ValueError):
            ChannelChainShotGenerator([])


class ChannelStickinessTest(unittest.TestCase):
    """一组图只用一个模型：中途换通道正是 09-11 混模型色差的来源。"""

    def setUp(self):
        reset_channel_health()

    @staticmethod
    def _chain(*generators, sticky=True):
        for generator in generators:
            generator.channel_name = generator.provider
        labels = [g.provider for g in generators]
        return ChannelChainShotGenerator(
            list(generators), labels=labels,
            health=_ChannelHealth(threshold=5, cooldown=300.0), sticky=sticky,
        )

    def test_group_reuses_the_first_winning_channel(self):
        first = _FakeGenerator(ok=True, provider="1route")
        second = _FakeGenerator(ok=True, provider="codex")
        chain = self._chain(first, second)
        with tempfile.TemporaryDirectory() as tmp:
            first_shot = chain.generate_shot(make_request(tmp))
            self.assertEqual(second.calls, 0)
            first.ok = False
            second_shot = chain.generate_shot(make_request(tmp))
        self.assertEqual(first_shot.provider, "1route")
        self.assertEqual(second_shot.provider, "codex")
        # 钉住的 1route 被再次尝试（先试它），失败后才解钉换 codex。
        self.assertEqual(first.calls, 2)
        self.assertEqual(second_shot.raw["channel_preferred"], "")

    def test_pinned_channel_wins_over_a_recovered_priority_channel(self):
        """P1 落到 codex 后即使 1route 恢复，P2 也不会换模型（组内统一）。"""
        first = _FakeGenerator(ok=False, provider="1route", error="1route down")
        second = _FakeGenerator(ok=True, provider="codex")
        chain = self._chain(first, second)
        with tempfile.TemporaryDirectory() as tmp:
            first_shot = chain.generate_shot(make_request(tmp))
            first.ok = True
            second_shot = chain.generate_shot(make_request(tmp))
        self.assertEqual(first_shot.provider, "codex")
        self.assertEqual(second_shot.provider, "codex")
        self.assertEqual(second_shot.raw["channel_preferred"], "codex")

    def test_sticky_is_disabled_with_strict_priority(self):
        first = _FakeGenerator(ok=True, provider="1route")
        second = _FakeGenerator(ok=True, provider="codex")
        chain = self._chain(first, second, sticky=False)
        with tempfile.TemporaryDirectory() as tmp:
            chain.generate_shot(make_request(tmp))
            outcome = chain.generate_shot(make_request(tmp))
        self.assertEqual(outcome.provider, "1route")
        self.assertEqual(outcome.raw["channel_preferred"], "")

    def test_a_different_task_is_not_pinned(self):
        first = _FakeGenerator(ok=True, provider="1route")
        second = _FakeGenerator(ok=True, provider="codex")
        chain = self._chain(first, second)
        with tempfile.TemporaryDirectory() as tmp:
            chain.generate_shot(make_request(tmp))
            other = make_request(tmp)
            other.task_id = "task_other"
            first.ok = False
            outcome = chain.generate_shot(other)
        self.assertEqual(outcome.provider, "codex")

    def test_broken_pinned_channel_is_released_for_the_next_shot(self):
        first = _FakeGenerator(ok=True, provider="1route")
        healthy = _FakeGenerator(ok=True, provider="codex")
        third = _FakeGenerator(ok=True, provider="creatok")
        for generator in (first, healthy, third):
            generator.channel_name = generator.provider
        chain = ChannelChainShotGenerator(
            [first, healthy, third], labels=["1route", "codex", "creatok"],
            health=_ChannelHealth(threshold=5, cooldown=300.0),
        )
        with tempfile.TemporaryDirectory() as tmp:
            chain.generate_shot(make_request(tmp))           # 钉住 1route
            first.ok = False
            chain.generate_shot(make_request(tmp))           # 1route 失败 → 解钉，codex 顶上
            second_outcome = chain.generate_shot(make_request(tmp))
        self.assertEqual(chain.labels[second_outcome.raw["channel_index"]], "codex")
        self.assertEqual(third.calls, 0)

    def test_sticky_preference_is_reported(self):
        first = _FakeGenerator(ok=True, provider="1route")
        chain = self._chain(first)
        with tempfile.TemporaryDirectory() as tmp:
            chain.generate_shot(make_request(tmp))
            chain.generate_shot(make_request(tmp))
        self.assertEqual(sticky_channel_preference(), {"task_probe": "1route"})


class ErrorClassificationTest(unittest.TestCase):
    def test_error_kinds(self):
        self.assertEqual(classify_error_kind("OPV_ONEROUTE_API_KEY not configured"), "config")
        self.assertEqual(classify_error_kind("1route http 401: bad"), "auth")
        self.assertEqual(classify_error_kind("429 rate limit"), "rate_limit")
        self.assertEqual(classify_error_kind("usage_limit_reached"), "rate_limit")
        self.assertEqual(classify_error_kind("ConnectTimeout: timeout"), "timeout")
        self.assertEqual(classify_error_kind("content policy violation"), "content")
        self.assertEqual(classify_error_kind("image is not 9:16 portrait: (1, 1)"), "quality")
        self.assertEqual(classify_error_kind("weird failure"), "unknown")


if __name__ == "__main__":
    unittest.main()
