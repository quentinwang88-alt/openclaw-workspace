#!/usr/bin/env python3
"""Validate and import one OPV account profile JSON into opv_account_profile.

The payload must satisfy the ``opv-account-profile-v1`` contract
(domain/contracts.py). Importing the shipped example file is refused unless
``--allow-example`` is passed, because examples are documentation, not
production accounts.

Dry-run by default; ``--apply`` performs the upsert. Requires
ORGANIC_PHOTO_VIDEO_DATABASE_URL (falls back to LIKEU_AI_DATABASE_URL).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
for path in (str(WORKSPACE_ROOT), str(PACKAGE_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from config.loader import EXAMPLES_DIR, load_account_import_file  # noqa: E402
from repositories.rds_repository import RdsRepository  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("account_json", help="path to the account profile JSON")
    parser.add_argument("--apply", action="store_true", help="actually upsert into RDS")
    parser.add_argument(
        "--allow-example",
        action="store_true",
        help="permit importing the shipped example file (documentation only)",
    )
    args = parser.parse_args()

    path = Path(args.account_json).resolve()
    if not path.is_file():
        print(f"file not found: {path}", file=sys.stderr)
        return 2
    if path.parent == EXAMPLES_DIR.resolve() and not args.allow_example:
        print(
            "refusing to import the shipped example account without --allow-example; "
            "copy the structure into a real account file first",
            file=sys.stderr,
        )
        return 2

    account = load_account_import_file(path)
    print("mode=" + ("apply" if args.apply else "dry-run"))
    print(f"account_id={account.account_id}")
    print(f"account_code={account.account_code}")
    print(f"status={account.status}")
    print(f"market={account.target_country}/{account.default_locale} tz={account.timezone}")
    print(f"default_market_pack_id={account.default_market_pack_id}")
    print(f"default_render_preset_id={account.default_render_preset_id}")
    print(f"persona_ref_id={account.persona_ref_id}")
    print(
        "refs: "
        f"looks={len(account.allowed_look_refs_json)} "
        f"scenes={len(account.allowed_scene_refs_json)} "
        f"core_scenes={len(account.core_scene_refs_json)} "
        f"styles={len(account.allowed_style_refs_json)}"
    )
    if account.status == "active" and not account.allowed_look_refs_json:
        print("warning: active account without look refs will fail intake", file=sys.stderr)

    if not args.apply:
        print("no_database_writes=1")
        return 0

    repository = RdsRepository.from_env()
    repository.upsert_account_profile(account)
    print("upserted_account=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
