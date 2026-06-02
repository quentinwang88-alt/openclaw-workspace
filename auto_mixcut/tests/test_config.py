from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from auto_mixcut.core.config import Settings


class SettingsTest(unittest.TestCase):
    def test_legacy_aliyun_config_keys_and_aliyun_public_base_are_supported(self):
        old_env = dict(os.environ)
        try:
            for key in [
                "AUTO_MIXCUT_OSS_PROVIDER",
                "ALIYUN_OSS_BUCKET",
                "ALIYUN_OSS_ENDPOINT",
                "ALIYUN_OSS_ACCESS_KEY_ID",
                "ALIYUN_OSS_ACCESS_KEY_SECRET",
                "AUTO_MIXCUT_ALIYUN_OSS_PUBLIC_BASE_URL",
                "AUTO_MIXCUT_PREVIEW_BASE_URL",
            ]:
                os.environ.pop(key, None)
            os.environ["AUTO_MIXCUT_ALIYUN_OSS_PUBLIC_BASE_URL"] = "https://preview.example.com"
            with tempfile.TemporaryDirectory() as tmp:
                config_path = Path(tmp) / "config.json"
                config_path.write_text(
                    json.dumps(
                        {
                            "oss_provider": "aliyun",
                            "bucket": "bucket-a",
                            "aliyun_oss_endpoint": "https://oss.example.com",
                            "aliyun_oss_access_key_id": "legacy-id",
                            "aliyun_oss_access_key_secret": "legacy-secret",
                        }
                    ),
                    encoding="utf-8",
                )
                settings = Settings.load(str(config_path))
            self.assertEqual(settings.aliyun_access_key_id, "legacy-id")
            self.assertEqual(settings.aliyun_access_key_secret, "legacy-secret")
            self.assertEqual(settings.aliyun_public_base_url, "https://preview.example.com")
        finally:
            os.environ.clear()
            os.environ.update(old_env)


if __name__ == "__main__":
    unittest.main()
