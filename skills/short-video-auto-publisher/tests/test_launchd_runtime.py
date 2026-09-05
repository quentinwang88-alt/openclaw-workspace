import importlib.util
import fcntl
import os
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('publisher_launchd', Path(__file__).resolve().parents[1] / 'scripts/run_launchd_task.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class LaunchdRuntimeTest(unittest.TestCase):
    def test_minimal_launchd_path_includes_media_tools(self):
        with patch.dict(os.environ, {'PATH': '/usr/bin:/bin', 'CUSTOM_FLAG': 'keep'}, clear=True):
            env = module.runtime_environment()
        self.assertIn(str(Path.home() / '.local/bin'), env['PATH'].split(os.pathsep))
        self.assertIn('/usr/bin', env['PATH'].split(os.pathsep))
        self.assertEqual(env['CUSTOM_FLAG'], 'keep')

    def test_process_lock_is_exclusive_and_recovers_when_handle_closes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / 'publisher.flock'
            first = module.acquire_process_lock(path)
            self.assertIsNotNone(first)
            self.assertIsNone(module.acquire_process_lock(path))
            first.close()
            second = module.acquire_process_lock(path)
            self.assertIsNotNone(second)
            fcntl.flock(second.fileno(), fcntl.LOCK_UN)
            second.close()
