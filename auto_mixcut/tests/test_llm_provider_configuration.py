from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from auto_mixcut.adapters.llm_provider import OpenAICompatibleProvider
from auto_mixcut.skills.llm_router_skill import _safe_provider_error


class LLMProviderConfigurationTest(unittest.TestCase):
    def test_openai_compatible_provider_disables_sdk_retries(self) -> None:
        config = {
            "type": "openai_compatible",
            "base_url": "https://example.invalid/api/coding/v3",
            "api_key_env": "TEST_ARK_KEY",
            "default_timeout": 180,
        }
        with patch.dict(os.environ, {"TEST_ARK_KEY": "secret-value"}, clear=False):
            with patch("openai.OpenAI") as client:
                OpenAICompatibleProvider(config)._get_client()
        self.assertEqual(client.call_args.kwargs["max_retries"], 0)
        self.assertEqual(client.call_args.kwargs["base_url"], config["base_url"])

    def test_provider_error_is_actionable_but_redacts_ark_credentials(self) -> None:
        message = _safe_provider_error(
            RuntimeError("InvalidSubscription using ark-example-secret-value at coding endpoint")
        )
        self.assertIn("InvalidSubscription", message)
        self.assertNotIn("ark-example-secret-value", message)
        self.assertIn("ark-***", message)


if __name__ == "__main__":
    unittest.main()
