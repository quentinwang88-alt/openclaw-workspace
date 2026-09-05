#!/usr/bin/env python3
"""Merge only owned styling rules into an existing account; dry-run by default."""
import argparse
from dataclasses import replace
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT.parents[1]):
    sys.path.insert(0, str(path))
from workspace_support import load_repo_env
from repositories.rds_repository import RdsRepository

OWNED = ("look_selection_mode", "exclude_look_refs", "required_presentation_profile", "item_count_policy", "presentation_profile")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("account_json", type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup", type=Path)
    args = parser.parse_args()
    desired = json.loads(args.account_json.read_text(encoding="utf-8"))
    load_repo_env()
    repo = RdsRepository.from_env()
    current = repo.get_account_profile(desired["account_id"])
    if current is None:
        raise ValueError("cannot merge styling settings into a missing account")
    original_rules = dict(current.operating_rules_json or {})
    desired_rules = desired.get("operating_rules") or {}
    if any(key not in desired_rules for key in OWNED):
        raise ValueError("deployment file must specify every owned styling field")
    merged = {**original_rules, **{key: desired_rules[key] for key in OWNED}}
    changed = [key for key in OWNED if original_rules.get(key) != merged.get(key)]
    print(json.dumps({"account_id": current.account_id, "mode": "apply" if args.apply else "dry_run",
                      "changed_fields": changed, "preserve_bgm_and_publish_rules": True}, ensure_ascii=False), flush=True)
    if not args.apply:
        return 0
    if args.backup is None:
        raise ValueError("--backup is required for apply")
    if not args.backup.exists():
        args.backup.parent.mkdir(parents=True, exist_ok=True)
        args.backup.write_text(json.dumps({"account_id": current.account_id, "operating_rules": original_rules},
                                         ensure_ascii=False, indent=2), encoding="utf-8")
    if changed:
        repo.upsert_account_profile(replace(current, operating_rules_json=merged))
    actual = repo.get_account_profile(current.account_id)
    if actual.operating_rules_json != merged or actual.status != current.status:
        raise ValueError("runtime profile read-back does not match intended merge")
    for field in ("allowed_look_refs_json", "allowed_persona_refs_json", "visual_identity_json", "default_render_preset_id"):
        if hasattr(current, field) and getattr(actual, field) != getattr(current, field):
            raise ValueError(f"unexpected non-owned account change: {field}")
    print("runtime_readback=matched; unrelated_account_settings=preserved", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
