#!/usr/bin/env python3
"""Run creator-monitoring backfills and a final Feishu refresh."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workspace_support import (
    get_media_inbound_dir,
    get_shared_sqlite_url,
    load_repo_env,
)

load_repo_env()

DEFAULT_PROJECT_ROOT = Path(
    os.environ.get(
        "CREATOR_MONITORING_PROJECT_ROOT",
        str(REPO_ROOT / "skills" / "creator-monitoring-assistant"),
    )
).expanduser()
DEFAULT_DATABASE_URL = os.environ.get("DATABASE_URL", get_shared_sqlite_url("creator_monitoring"))
DEFAULT_INBOUND_DIR = Path(
    os.environ.get("OPENCLAW_MEDIA_INBOUND_DIR", str(get_media_inbound_dir()))
).expanduser()
DEFAULT_FEISHU_APP_TOKEN = os.environ.get(
    "CREATOR_MONITORING_FEISHU_APP_TOKEN",
    os.environ.get("FEISHU_APP_TOKEN", ""),
)
DEFAULT_FEISHU_TABLE_ID = os.environ.get(
    "CREATOR_MONITORING_FEISHU_TABLE_ID",
    os.environ.get("FEISHU_TABLE_ID", ""),
)

STORE_TOKEN_ALIASES = {
    "泰国服装1店": {"泰国", "服装", "1店", "TH"},
    "泰国女装1店": {"泰国", "女装", "1店", "TH"},
    "泰国配饰1店": {"泰国", "配饰", "1店", "TH"},
    "马来西亚配饰1店": {"马来", "马来西亚", "配饰", "1店", "MY"},
}


def parse_week_file(value: str) -> Tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("Expected WEEK=/absolute/path.xlsx")
    week, path = value.split("=", 1)
    week = week.strip()
    path = path.strip()
    if not week or not path:
        raise argparse.ArgumentTypeError("Expected WEEK=/absolute/path.xlsx")
    return week, path


def tokenize(text: Optional[str]) -> Set[str]:
    if not text:
        return set()
    normalized = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", " ", text)
    parts = [part.strip().lower() for part in normalized.split() if part.strip()]
    tokens: Set[str] = set(parts)
    if text in STORE_TOKEN_ALIASES:
        tokens.update(token.lower() for token in STORE_TOKEN_ALIASES[text])
    return tokens


def extension_of(path: str) -> str:
    return os.path.splitext(path)[1].lower()


def score_inbound_candidate(path: Path, requested_name: str, store: str, now_ts: float) -> float:
    basename = path.name
    basename_lower = basename.lower()
    requested_lower = requested_name.strip().lower()
    score = 0.0

    if basename_lower == requested_lower:
        score += 120
    elif requested_lower and requested_lower in basename_lower:
        score += 70
    elif extension_of(basename_lower) == extension_of(requested_lower):
        score += 10

    overlap = (tokenize(requested_name) | tokenize(store)) & tokenize(basename)
    score += len(overlap) * 12

    age_seconds = max(0.0, now_ts - path.stat().st_mtime)
    if age_seconds <= 300:
        score += 35
    elif age_seconds <= 1800:
        score += 20
    elif age_seconds <= 7200:
        score += 8

    return score


def resolve_inbound_file(requested: str, store: str, inbound_dir: Path, window_minutes: int) -> Path:
    if not inbound_dir.exists():
        raise SystemExit(f"Inbound directory does not exist: {inbound_dir}")
    now_ts = time.time()
    window_seconds = max(1, window_minutes) * 60
    candidates = []
    for path in inbound_dir.iterdir():
        if not path.is_file():
            continue
        if now_ts - path.stat().st_mtime > window_seconds:
            continue
        score = score_inbound_candidate(path, requested_name=requested, store=store, now_ts=now_ts)
        if score > 0:
            candidates.append((score, path.stat().st_mtime, path))
    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    if not candidates:
        raise SystemExit(
            f"Could not resolve inbound file '{requested}' in {inbound_dir} within {window_minutes} minutes"
        )
    return candidates[0][2].resolve()


def sort_week_key(stat_week: str) -> Tuple[int, int]:
    raw = stat_week.strip().upper().replace("W", "-").replace("_", "-")
    parts = [part for part in raw.split("-") if part]
    if len(parts) != 2:
        raise ValueError(f"Invalid stat week: {stat_week}")
    return int(parts[0]), int(parts[1])


def build_command(
    project_root: Path,
    stat_week: str,
    source_file_path: str,
    platform: str,
    country: str,
    store: str,
) -> List[str]:
    return [
        sys.executable,
        str(project_root / "run_pipeline.py"),
        "--stat-week",
        stat_week,
        "--source-file-path",
        source_file_path,
        "--platform",
        platform,
        "--country",
        country,
        "--store",
        store,
    ]


def ensure_project_imports(project_root: Path) -> None:
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)


def database_has_sync_week(project_root: Path, database_url: str, stat_week: str, store: str) -> bool:
    ensure_project_imports(project_root)
    from app.db import Database

    database = Database(database_url)
    row = database.fetchone(
        """
        SELECT COUNT(*) AS cnt
        FROM creator_monitoring_result
        WHERE stat_week = :stat_week
          AND store = :store
        """,
        {"stat_week": stat_week, "store": store},
    )
    return bool(row and int(row["cnt"]) > 0)


def sync_from_database(
    project_root: Path,
    database_url: str,
    stat_week: str,
    store: str,
) -> Dict[str, object]:
    ensure_project_imports(project_root)
    from app.db import Database
    from app.services.feishu_sync import sync_current_action_table_to_feishu

    database = Database(database_url)
    return sync_current_action_table_to_feishu(stat_week=stat_week, store=store, db=database)


def build_sync_from_db_preview(
    project_root: Path,
    database_url: str,
    stat_week: str,
    store: str,
) -> str:
    return (
        "sync_current_action_table_to_feishu"
        f" project_root={project_root}"
        f" database_url={database_url}"
        f" stat_week={stat_week}"
        f" store={store}"
    )


def build_repair_command(
    project_root: Path,
    database_url: str,
    stat_week: str,
    store: str,
    feishu_app_token: str,
    feishu_table_id: str,
    dry_run: bool,
) -> List[str]:
    command = [
        sys.executable,
        str(project_root / "scripts" / "repair_feishu_current_action_table.py"),
        "--stat-week",
        stat_week,
        "--store",
        store,
        "--database-url",
        database_url,
    ]
    if feishu_app_token:
        command.extend(["--app-token", feishu_app_token])
    if feishu_table_id:
        command.extend(["--table-id", feishu_table_id])
    if dry_run:
        command.append("--dry-run")
    return command


def run_step(command: List[str], env: Dict[str, str], cwd: Path, dry_run: bool) -> None:
    print(" ".join(command))
    if dry_run:
        return
    subprocess.run(command, cwd=str(cwd), env=env, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill creator-monitoring weeks and refresh Feishu once.")
    parser.add_argument("--week-file", action="append", type=parse_week_file)
    parser.add_argument("--sync-week", required=True, help="The final week to sync to Feishu.")
    parser.add_argument("--sync-file", help="Excel file for --sync-week if not already present in --week-file.")
    parser.add_argument("--platform", default="tiktok")
    parser.add_argument("--country", default="th")
    parser.add_argument("--store", required=True)
    parser.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    parser.add_argument("--database-url", default=DEFAULT_DATABASE_URL)
    parser.add_argument("--inbound-dir", default=str(DEFAULT_INBOUND_DIR))
    parser.add_argument("--feishu-app-token", default=DEFAULT_FEISHU_APP_TOKEN)
    parser.add_argument("--feishu-table-id", default=DEFAULT_FEISHU_TABLE_ID)
    parser.add_argument("--window-minutes", type=int, default=180)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    project_root = Path(args.project_root).expanduser().resolve()
    inbound_dir = Path(args.inbound_dir).expanduser().resolve()
    if not project_root.exists():
        raise SystemExit(f"Project root does not exist: {project_root}")

    week_files: Dict[str, str] = {}
    for week, value in args.week_file or []:
        candidate = Path(value).expanduser()
        if candidate.exists():
            week_files[week] = str(candidate.resolve())
        else:
            week_files[week] = str(
                resolve_inbound_file(
                    requested=value,
                    store=args.store,
                    inbound_dir=inbound_dir,
                    window_minutes=args.window_minutes,
                )
            )

    sync_from_db_only = False
    if args.sync_week not in week_files:
        if args.sync_file:
            sync_candidate = Path(args.sync_file).expanduser()
            if sync_candidate.exists():
                week_files[args.sync_week] = str(sync_candidate.resolve())
            else:
                week_files[args.sync_week] = str(
                    resolve_inbound_file(
                        requested=args.sync_file,
                        store=args.store,
                        inbound_dir=inbound_dir,
                        window_minutes=args.window_minutes,
                    )
                )
        elif database_has_sync_week(project_root, args.database_url, args.sync_week, args.store):
            sync_from_db_only = True
        else:
            raise SystemExit(
                "--sync-week is not in --week-file, no --sync-file was provided, "
                "and the target week was not found in the database"
            )

    for week, path in week_files.items():
        if not Path(path).exists():
            raise SystemExit(f"Excel file not found for {week}: {path}")

    ordered_weeks = sorted(week_files.keys(), key=sort_week_key)
    env = os.environ.copy()
    env["DATABASE_URL"] = args.database_url
    if args.feishu_app_token:
        env["FEISHU_APP_TOKEN"] = args.feishu_app_token
    if args.feishu_table_id:
        env["FEISHU_TABLE_ID"] = args.feishu_table_id

    print("Backfill order:", ", ".join(ordered_weeks))
    print("Final Feishu sync week:", args.sync_week)
    print("Final sync source:", "database" if sync_from_db_only else "excel")
    print("Inbound directory:", inbound_dir)
    for week in ordered_weeks:
        print(f"{week}: {week_files[week]}")

    for week in ordered_weeks:
        env["FEISHU_ENABLE_SYNC"] = "false"
        command = build_command(
            project_root=project_root,
            stat_week=week,
            source_file_path=week_files[week],
            platform=args.platform,
            country=args.country,
            store=args.store,
        )
        print(f"\n[history] {week}")
        run_step(command, env=env, cwd=project_root, dry_run=args.dry_run)

    env["FEISHU_ENABLE_SYNC"] = "true"
    if sync_from_db_only:
        print(f"\n[final-sync-from-db] {args.sync_week}")
        print(build_sync_from_db_preview(project_root, args.database_url, args.sync_week, args.store))
        if not args.dry_run:
            os.environ.update(env)
            print(sync_from_database(project_root, args.database_url, args.sync_week, args.store))
    else:
        sync_command = build_command(
            project_root=project_root,
            stat_week=args.sync_week,
            source_file_path=week_files[args.sync_week],
            platform=args.platform,
            country=args.country,
            store=args.store,
        )
        print(f"\n[final-sync] {args.sync_week}")
        run_step(sync_command, env=env, cwd=project_root, dry_run=args.dry_run)

    repair_command = build_repair_command(
        project_root=project_root,
        database_url=args.database_url,
        stat_week=args.sync_week,
        store=args.store,
        feishu_app_token=args.feishu_app_token,
        feishu_table_id=args.feishu_table_id,
        dry_run=args.dry_run,
    )
    print(f"\n[post-repair] {args.sync_week}")
    run_step(repair_command, env=env, cwd=project_root, dry_run=args.dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()
