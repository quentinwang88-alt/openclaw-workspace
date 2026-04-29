import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.config import _load_local_env  # noqa: E402


class ConfigEnvTest(unittest.TestCase):
    def test_load_local_env_preserves_inner_quotes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            env_path = root / ".env.local"
            env_path.write_text(
                'FASTMOSS_B_HERMES_COMMAND=python3 demo.py --profile "{profile}" --batch-id "{batch_id}"\n',
                encoding="utf-8",
            )
            app_dir = root / "app"
            app_dir.mkdir(parents=True, exist_ok=True)
            fake_config = app_dir / "config.py"
            fake_config.write_text("# stub", encoding="utf-8")
            with patch("app.config.Path.resolve", return_value=fake_config):
                with patch.dict("app.config.os.environ", {}, clear=True):
                    _load_local_env()
                    self.assertEqual(
                        __import__("app.config", fromlist=["os"]).os.environ["FASTMOSS_B_HERMES_COMMAND"],
                        'python3 demo.py --profile "{profile}" --batch-id "{batch_id}"',
                    )


if __name__ == "__main__":
    unittest.main()
