import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
from types import SimpleNamespace

from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.hermes_analyzer import HermesAnalyzer  # noqa: E402


class HermesAnalyzerImageTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp(prefix="hermes_analyzer_test_"))
        self.analyzer = HermesAnalyzer(skill_dir=ROOT, hermes_bin="/bin/echo", llm_backend="hermes")

    def _make_image(self, name: str, color):
        path = self.temp_dir / name
        image = Image.new("RGB", (240, 180), color=color)
        image.save(path)
        image.close()
        return path

    def test_materialize_analysis_image_builds_contact_sheet_for_multiple_images(self):
        image_one = self._make_image("one.jpg", (255, 0, 0))
        image_two = self._make_image("two.jpg", (0, 0, 255))

        result_path, image_count = self.analyzer._materialize_analysis_image([str(image_one), str(image_two)])

        self.assertEqual(image_count, 2)
        self.assertTrue(result_path.exists())
        self.assertNotEqual(result_path, image_one)
        self.assertNotEqual(result_path, image_two)

        composed = Image.open(result_path)
        self.assertGreater(composed.width, 1000)
        self.assertGreater(composed.height, 700)
        composed.close()

    def test_run_prompt_mentions_multi_image_contact_sheet(self):
        captured = {}
        image_one = self._make_image("one.jpg", (255, 0, 0))
        image_two = self._make_image("two.jpg", (0, 255, 0))

        def fake_runner(command, **kwargs):
            captured["command"] = command
            captured["kwargs"] = kwargs
            return SimpleNamespace(
                returncode=0,
                stdout='{"predicted_category":"发饰","confidence":"high","reason":"图片显示为发饰"}',
                stderr="",
            )

        analyzer = HermesAnalyzer(
            skill_dir=ROOT,
            hermes_bin="/bin/echo",
            command_runner=fake_runner,
            llm_backend="hermes",
        )
        analyzer._run_prompt(
            prompt_name="category_identification_prompt_v2.txt",
            payload={"images": [str(image_one), str(image_two)]},
            product_images=[str(image_one), str(image_two)],
        )

        query = captured["command"][captured["command"].index("-q") + 1]
        image_path = Path(captured["command"][captured["command"].index("--image") + 1])
        self.assertIn("多图拼板", query)
        self.assertTrue(image_path.exists())
        self.assertNotEqual(image_path, image_one)

    def test_run_prompt_can_use_openclaw_registry_provider(self):
        captured = {}
        image_one = self._make_image("one.jpg", (255, 0, 0))
        models_config = self.temp_dir / "models.json"
        models_config.write_text(
            """
{
  "providers": {
    "mock-api": {
      "baseUrl": "https://example.invalid/v1",
      "apiKey": "test-key",
      "api": "openai-completions",
      "models": [
        {"id": "vision-model", "input": ["text", "image"]}
      ]
    }
  }
}
""".strip(),
            encoding="utf-8",
        )

        class FakeResponse:
            ok = True
            text = '{"ok": true}'

            def json(self):
                return {
                    "choices": [
                        {
                            "message": {
                                "content": '{"predicted_category":"发饰","confidence":"high","reason":"图片显示为发饰"}'
                            }
                        }
                    ]
                }

        def fake_post(url, **kwargs):
            captured["url"] = url
            captured["kwargs"] = kwargs
            return FakeResponse()

        analyzer = HermesAnalyzer(
            skill_dir=ROOT,
            llm_backend="openclaw",
            openclaw_model="mock-api/vision-model",
            openclaw_models_config=str(models_config),
            request_post=fake_post,
        )
        payload = analyzer._run_prompt(
            prompt_name="category_identification_prompt_v2.txt",
            payload={"images": [str(image_one)]},
            product_images=[str(image_one)],
        )

        self.assertEqual(payload["predicted_category"], "发饰")
        self.assertTrue(captured["url"].endswith("/chat/completions"))
        message = captured["kwargs"]["json"]["messages"][0]["content"]
        self.assertEqual(message[0]["type"], "text")
        self.assertEqual(message[1]["type"], "image_url")

    def test_run_prompt_uses_codex_responses_direct_image_input(self):
        captured = {}
        image_one = self._make_image("one.jpg", (255, 0, 0))
        models_config = self.temp_dir / "models-codex.json"
        models_config.write_text(
            """
{
  "providers": {
    "openai-codex": {
      "baseUrl": "https://chatgpt.com/backend-api",
      "api": "openai-codex-responses",
      "models": [
        {"id": "gpt-5.5", "input": ["text", "image"]}
      ]
    }
  }
}
""".strip(),
            encoding="utf-8",
        )

        class FakeEvent:
            def __init__(self, event_type, delta="", text=""):
                self.type = event_type
                self.delta = delta
                self.text = text

        class FakeFinalResponse:
            output_text = ""

            def model_dump(self, mode="json"):
                return {}

        class FakeStream:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def __iter__(self):
                return iter([FakeEvent("response.output_text.delta", '{"predicted_category":"发饰","confidence":"high","reason":"图片显示为发饰"}')])

            def get_final_response(self):
                return FakeFinalResponse()

        class FakeResponses:
            def stream(self, **kwargs):
                captured["stream_kwargs"] = kwargs
                return FakeStream()

        class FakeOpenAI:
            def __init__(self, **kwargs):
                captured["client_kwargs"] = kwargs
                self.responses = FakeResponses()

        with patch("src.hermes_analyzer.OpenAI", FakeOpenAI):
            analyzer = HermesAnalyzer(
                skill_dir=ROOT,
                llm_backend="openclaw",
                openclaw_model="openai-codex/gpt-5.5",
                openclaw_models_config=str(models_config),
            )
            analyzer._extract_codex_access_token = lambda: "test-token"
            payload = analyzer._run_prompt(
                prompt_name="category_identification_prompt_v2.txt",
                payload={"images": [str(image_one)]},
                product_images=[str(image_one)],
            )

        self.assertEqual(payload["predicted_category"], "发饰")
        self.assertTrue(captured["client_kwargs"]["base_url"].endswith("/codex"))
        content = captured["stream_kwargs"]["input"][0]["content"]
        self.assertEqual(content[0]["type"], "input_text")
        self.assertEqual(content[1]["type"], "input_image")
        self.assertTrue(content[1]["image_url"].startswith("data:image/jpeg;base64,"))

    def test_codex_direct_image_input_requires_access_token(self):
        image_one = self._make_image("one.jpg", (255, 0, 0))
        models_config = self.temp_dir / "models-codex-block.json"
        models_config.write_text(
            """
{
  "providers": {
    "openai-codex": {
      "baseUrl": "https://chatgpt.com/backend-api",
      "api": "openai-codex-responses",
      "models": [
        {"id": "gpt-5.5", "input": ["text", "image"]}
      ]
    }
  }
}
""".strip(),
            encoding="utf-8",
        )
        analyzer = HermesAnalyzer(
            skill_dir=ROOT,
            llm_backend="openclaw",
            openclaw_model="openai-codex/gpt-5.5",
            openclaw_models_config=str(models_config),
        )
        analyzer._extract_codex_access_token = lambda: ""

        with self.assertRaises(Exception) as ctx:
            analyzer._run_prompt(
                prompt_name="category_identification_prompt_v2.txt",
                payload={"images": [str(image_one)]},
                product_images=[str(image_one)],
            )

        self.assertIn("access token", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
