from pathlib import Path
import sys
import unittest


PACKAGE_ROOT = Path(__file__).resolve().parent.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from scripts import apply_rds_migration


class MigrationInstallerTest(unittest.TestCase):
    def test_each_new_migration_is_single_statement_and_has_own_digest(self):
        sources = apply_rds_migration.migration_sources()
        by_name = {}
        for path, statement in apply_rds_migration.executable_statements(sources):
            by_name.setdefault(path.name, []).append(statement)
        self.assertEqual(len(by_name["003_enforce_package_and_outfit_state.sql"]), 1)
        self.assertEqual(len(by_name["004_unique_content_package_per_task.sql"]), 1)
        self.assertEqual(len(by_name["005_publish_schedule_queue.sql"]), 1)
        self.assertEqual(len(by_name["006_product_reference_pack.sql"]), 1)
        self.assertEqual(len(by_name["009_native_photo_content.sql"]), 6)
        digests = {
            path.name: apply_rds_migration.source_digest(path.name, sql)
            for path, sql in sources
        }
        self.assertEqual(len(set(digests.values())), len(digests))
        self.assertTrue(all(len(value) == 64 for value in digests.values()))

    def test_native_photo_migration_is_additive_and_keeps_video_defaults(self):
        sources = dict(
            (path.name, sql) for path, sql in apply_rds_migration.migration_sources()
        )
        sql = sources["009_native_photo_content.sql"]
        self.assertIn("MODIFY COLUMN product_id VARCHAR(191) NULL", sql)
        self.assertIn("MODIFY COLUMN duration_ms INT NULL", sql)
        self.assertIn("MODIFY COLUMN render_id VARCHAR(64) NULL", sql)
        self.assertIn("media_kind VARCHAR(24) NOT NULL DEFAULT 'video'", sql)
        self.assertIn("UNIQUE KEY uq_opv_publish_key (publish_key)", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS opv_asset_set", sql)


if __name__ == "__main__":
    unittest.main()
