#!/usr/bin/env python3
"""Read-only readiness check for OPV schema upgrades and reference data."""

from __future__ import annotations

import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
for value in (str(PACKAGE_ROOT), str(PACKAGE_ROOT.parents[1])):
    if value not in sys.path:
        sys.path.insert(0, value)

from apply_rds_migration import connect, database_url, source_digest  # noqa: E402


def main() -> int:
    with connect(database_url()) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema=DATABASE() AND table_name='_opv_schema_migrations'"
        )
        ledger_exists = bool(cursor.fetchone()[0])
        applied = {}
        if ledger_exists:
            cursor.execute(
                "SELECT migration_name, sha256 FROM _opv_schema_migrations "
                "ORDER BY migration_name"
            )
            applied = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema=DATABASE() AND table_name='opv_content_shot' "
            "AND column_name='outfit_state_ref'"
        )
        outfit_state_column = bool(cursor.fetchone()[0])

        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.statistics "
            "WHERE table_schema=DATABASE() AND table_name='opv_content_package' "
            "AND index_name='uq_opv_content_package_task' AND non_unique=0"
        )
        package_unique_index = bool(cursor.fetchone()[0])

        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema=DATABASE() AND table_name='opv_publish_record' "
            "AND column_name IN ('planned_publish_at','submitted_at')"
        )
        publish_schedule_columns = int(cursor.fetchone()[0]) == 2
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.statistics "
            "WHERE table_schema=DATABASE() AND table_name='opv_publish_record' "
            "AND index_name='idx_opv_publish_due'"
        )
        publish_due_index = bool(cursor.fetchone()[0])

        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema=DATABASE() "
            "AND table_name='opv_product_reference_pack'"
        )
        product_reference_pack_table = bool(cursor.fetchone()[0])
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() "
            "AND table_name IN ('opv_task_revision','opv_quality_review','opv_production_batch')"
        )
        workflow_tables = int(cursor.fetchone()[0]) == 3
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() "
            "AND table_name='opv_production_batch' AND column_name IN "
            "('manifest_json','expected_count','pending_fields_json','run_owner','lease_until')"
        )
        batch_recovery_columns = int(cursor.fetchone()[0]) == 5
        upgrade_checksums = all(
            applied.get(path.name) == source_digest(path.name, path.read_text(encoding="utf-8"))
            for path in (PACKAGE_ROOT / "migrations").glob("00[78]*.sql")
        )

        cursor.execute(
            "SELECT task_id, COUNT(*) AS package_count "
            "FROM opv_content_package GROUP BY task_id HAVING COUNT(*) > 1 "
            "ORDER BY package_count DESC, task_id LIMIT 20"
        )
        duplicate_packages = list(cursor.fetchall())

        cursor.execute(
            "SELECT recipe_id, status FROM opv_content_recipe ORDER BY recipe_id"
        )
        recipes = list(cursor.fetchall())
        cursor.execute(
            "SELECT render_profile_id, status FROM opv_render_profile "
            "ORDER BY render_profile_id"
        )
        render_profiles = list(cursor.fetchall())
        cursor.execute(
            "SELECT quality_profile_id, status FROM opv_quality_profile "
            "ORDER BY quality_profile_id"
        )
        quality_profiles = list(cursor.fetchall())

    print(f"ledger_exists={int(ledger_exists)}")
    for name, digest in applied.items():
        print(f"migration={name} sha256={digest}")
    print(f"outfit_state_column={int(outfit_state_column)}")
    print(f"package_unique_index={int(package_unique_index)}")
    print(f"publish_schedule_columns={int(publish_schedule_columns)}")
    print(f"publish_due_index={int(publish_due_index)}")
    print(f"product_reference_pack_table={int(product_reference_pack_table)}")
    print(f"workflow_v2_tables={int(workflow_tables)}")
    print(f"batch_recovery_columns={int(batch_recovery_columns)}")
    print(f"workflow_upgrade_checksums={int(upgrade_checksums)}")
    print(f"duplicate_package_task_count={len(duplicate_packages)}")
    for task_id, count in duplicate_packages:
        print(f"duplicate_package task_id={task_id} count={count}")
    print("recipes=" + ",".join(f"{value}:{status}" for value, status in recipes))
    print(
        "render_profiles="
        + ",".join(f"{value}:{status}" for value, status in render_profiles)
    )
    print(
        "quality_profiles="
        + ",".join(f"{value}:{status}" for value, status in quality_profiles)
    )
    ready = (
        not duplicate_packages
        and outfit_state_column
        and package_unique_index
        and publish_schedule_columns
        and publish_due_index
        and product_reference_pack_table
        and workflow_tables
        and batch_recovery_columns
        and upgrade_checksums
    )
    print(f"ready_for_003_008={int(ready)}")
    print("scope=schema_only_not_content_or_production_acceptance")
    return 0 if ready else 2


if __name__ == "__main__":
    raise SystemExit(main())
