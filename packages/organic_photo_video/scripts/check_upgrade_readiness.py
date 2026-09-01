#!/usr/bin/env python3
"""Read-only readiness check for OPV migrations 003/004 and reference data."""

from __future__ import annotations

import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
for value in (str(PACKAGE_ROOT), str(PACKAGE_ROOT.parents[1])):
    if value not in sys.path:
        sys.path.insert(0, value)

from apply_rds_migration import connect, database_url  # noqa: E402


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
    ready = not duplicate_packages
    print(f"ready_for_003_004={int(ready)}")
    return 0 if ready else 2


if __name__ == "__main__":
    raise SystemExit(main())
