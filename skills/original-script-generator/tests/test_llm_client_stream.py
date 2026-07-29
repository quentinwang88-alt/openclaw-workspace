#!/usr/bin/env python3

import unittest
import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from core.llm_client import OriginalScriptLLMClient


class _FakeStream:
    def __init__(self, events):
        self.events = list(events)
        self.closed = False
        self.final_response_requested = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        self.closed = True

    def __iter__(self):
        return iter(self.events)

    def get_final_response(self):
        self.final_response_requested = True
        raise AssertionError("get_final_response should not be called after output_text.done")


class _FakeResponses:
    def __init__(self, stream):
        self._stream = stream

    def stream(self, **kwargs):
        return self._stream


class _FakeClient:
    def __init__(self, stream):
        self.responses = _FakeResponses(stream)


class LLMClientStreamTests(unittest.TestCase):
    def test_supports_per_instance_model_and_reasoning_override(self) -> None:
        client = OriginalScriptLLMClient(
            primary_api_key="token",
            primary_model="gpt-5.6-sol",
            primary_reasoning_effort="high",
        )

        self.assertEqual("gpt-5.6-sol", client.primary_model)
        self.assertEqual("high", client.primary_reasoning_effort)

    def test_stream_returns_on_output_text_done_without_waiting_final_response(self) -> None:
        stream = _FakeStream(
            [
                SimpleNamespace(type="response.output_text.delta", delta='{"ok"'),
                SimpleNamespace(type="response.output_text.delta", delta=": true}"),
                SimpleNamespace(type="response.output_text.done", text='{"ok": true}'),
            ]
        )
        client = OriginalScriptLLMClient(primary_api_key="token")
        client._primary_client = _FakeClient(stream)

        response = client._call_primary("return json", image_paths=[], max_tokens=100)

        self.assertEqual(response["choices"][0]["message"]["content"], '{"ok": true}')
        self.assertTrue(stream.closed)
        self.assertFalse(stream.final_response_requested)

    def test_resolves_openclaw_explicit_proxy_for_codex_backend(self) -> None:
        payload = {
            "models": {
                "providers": {
                    "openai-codex": {
                        "request": {
                            "allowPrivateNetwork": True,
                            "proxy": {
                                "mode": "explicit-proxy",
                                "url": "http://127.0.0.1:18080",
                            },
                        }
                    }
                }
            }
        }
        with patch("core.llm_client._safe_read_json", return_value=payload):
            client = OriginalScriptLLMClient(primary_api_key="token")

        self.assertEqual(client.primary_proxy_url, "http://127.0.0.1:18080")

    def test_single_attempt_can_disable_final_json_repair_call(self) -> None:
        client = OriginalScriptLLMClient(primary_api_key="token")
        client._call_raw = MagicMock(return_value={})
        client._extract_text = MagicMock(return_value="not valid json")
        client._repair_json_output = MagicMock(return_value='{"ok": true}')

        with self.assertRaises(Exception):
            client.call_json(
                "return json",
                max_attempts=1,
                repair_json_on_failure=False,
            )

        self.assertEqual(client._call_raw.call_count, 1)
        client._repair_json_output.assert_not_called()

    def test_cli_transport_returns_final_agent_message(self) -> None:
        client = OriginalScriptLLMClient(
            primary_api_key="token",
            primary_model="gpt-5.6-sol",
            primary_reasoning_effort="high",
        )
        result = SimpleNamespace(
            returncode=0,
            stderr="",
            stdout=(
                '{"type":"thread.started"}\n'
                '{"type":"item.completed","item":{"type":"agent_message","text":"{\\"ok\\":true}"}}\n'
                '{"type":"turn.completed"}\n'
            ),
        )
        with patch.dict(os.environ, {"ORIGINAL_SCRIPT_USE_CODEX_CLI": "1"}), patch(
            "core.llm_client.subprocess.run", return_value=result
        ) as run:
            response = client._call_primary("return json", image_paths=[], max_tokens=100)

        self.assertEqual(response["choices"][0]["message"]["content"], '{"ok":true}')
        command = run.call_args.args[0]
        self.assertIn("gpt-5.6-sol", command)
        self.assertIn("model_reasoning_effort=high", command)


if __name__ == "__main__":
    unittest.main()
