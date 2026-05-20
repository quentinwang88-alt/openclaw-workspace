#!/usr/bin/env python3

import unittest
from types import SimpleNamespace

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


if __name__ == "__main__":
    unittest.main()
