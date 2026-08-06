from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "openclaw_original_batch_sync.py"
)
SPEC = importlib.util.spec_from_file_location("openclaw_original_batch_sync", SCRIPT)
assert SPEC and SPEC.loader
adapter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adapter)


class OpenClawOriginalBatchSyncTest(unittest.TestCase):
    def test_check_is_read_only_and_uses_new_source_kind(self):
        command = adapter.build_sync_command(action="check", limit=20)
        self.assertIn("--dry-run", command)
        self.assertEqual(command[command.index("--source-kind") + 1], "original-batch")

    def test_sync_one_record_uses_a_typed_selector(self):
        command = adapter.build_sync_command(action="sync", record_id="recAbc123", limit=1)
        self.assertEqual(command[command.index("--record-id") + 1], "recAbc123")
        self.assertNotIn("--dry-run", command)

    def test_product_code_selector_is_supported(self):
        command = adapter.build_sync_command(
            action="sync", product_code="1734482585843304442", limit=5
        )
        self.assertEqual(command[command.index("--product-code") + 1], "1734482585843304442")

    def test_rejects_invalid_identifiers_and_limit(self):
        with self.assertRaisesRegex(ValueError, "record_id"):
            adapter.build_sync_command(action="sync", record_id="wrong")
        with self.assertRaisesRegex(ValueError, "最多同步"):
            adapter.build_sync_command(action="sync", limit=21)


if __name__ == "__main__":
    unittest.main()
