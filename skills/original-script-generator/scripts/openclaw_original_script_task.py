#!/usr/bin/env python3
"""Safe OpenClaw command adapter for the row-based original-script workflow.

This module deliberately exposes a small, typed command surface.  Natural
language is interpreted by OpenClaw's skill instructions; this adapter then
only receives validated record/product identifiers and fixed action names.
It never creates a task row or changes a row into ``待执行`` by itself.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable


SKILL_ROOT = Path(__file__).resolve().parents[1]
RUNNER = SKILL_ROOT / "scripts" / "run_feishu_operation_tasks.py"
LIGHTWEIGHT_ROOT = SKILL_ROOT.parent / "lightweight-tryon-video"
OUTFIT_REFRESH_RUNNER = LIGHTWEIGHT_ROOT / "scripts" / "run_pipeline.py"
OUTFIT_REFRESH_DB = LIGHTWEIGHT_ROOT / "var" / "light_tryon.sqlite3"
OUTFIT_REFRESH_CONFIG = LIGHTWEIGHT_ROOT / "config" / "feishu_tables.json"
PERSONA_REFRESH_RUNNER = SKILL_ROOT / "scripts" / "ensure_persona_template_workbench.py"
FIRST_FRAME_RUNNER = SKILL_ROOT / "scripts" / "run_first_frame_tasks.py"
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.production_script_feishu import operation_record_values  # noqa: E402
from scripts.run_feishu_operation_tasks import _client  # noqa: E402


DEFAULT_OPERATION_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "RJxHw0uAkiJPkSkXvMvcq3hXn5B?table=tblr8C7uvGIPBQar&view=vewWEsmd4q"
)
RECORD_ID_RE = re.compile(r"^rec[A-Za-z0-9]+$")
PRODUCT_CODE_RE = re.compile(r"^\d{8,30}$")
ACTION_STATUSES = {
    "check": {"待执行"},
    "run": {"待执行"},
    "plan": {"待执行"},
    "resume": {"失败", "部分完成"},
    "replan": {"失败", "部分完成", "已完成"},
    "export-ready": {"失败", "部分完成", "已完成", "执行中-脚本生成", "执行中-规划"},
}
REFRESH_ACTIONS = {
    "refresh-outfits", "refresh-personas", "refresh-production-config",
}
FIRST_FRAME_ACTIONS = {"first-frame-check", "first-frame-run", "first-frame-retry"}
ALL_ACTIONS = {*ACTION_STATUSES, *REFRESH_ACTIONS, *FIRST_FRAME_ACTIONS}


def _validate_record_id(value: str) -> str:
    if not RECORD_ID_RE.fullmatch(value or ""):
        raise ValueError("record_id 必须是 rec 开头的飞书记录 ID")
    return value


def _validate_product_code(value: str) -> str:
    if not PRODUCT_CODE_RE.fullmatch(value or ""):
        raise ValueError("product_code 必须是 8 至 30 位数字产品编码")
    return value


def _operation_client():
    return _client(DEFAULT_OPERATION_URL)


def resolve_record_id_for_product(*, product_code: str, action: str) -> str:
    """Resolve one eligible task row without writing any Feishu field."""
    product_code = _validate_product_code(product_code)
    allowed_statuses = ACTION_STATUSES[action]
    matches: list[str] = []
    for record in _operation_client().list_records(page_size=500):
        task = operation_record_values(record)
        if str(task.get("product_code") or "").strip() != product_code:
            continue
        if task.get("status") in allowed_statuses:
            matches.append(record.record_id)

    if not matches:
        statuses = " / ".join(sorted(allowed_statuses))
        raise ValueError(
            f"产品 {product_code} 没有状态为 {statuses} 的原创脚本运营任务；"
            "请先在短视频运营任务表创建并设置正确状态。"
        )
    if len(matches) > 1:
        raise ValueError(
            f"产品 {product_code} 匹配到多条可执行任务：{', '.join(matches)}。"
            "请改用明确的 rec 记录 ID，避免执行错批次。"
        )
    return matches[0]


def build_runner_command(
    *,
    action: str,
    record_id: str | None = None,
    limit: int | None = None,
) -> list[str]:
    """Build the fixed runner invocation; no user text becomes shell text."""
    if action not in ALL_ACTIONS:
        raise ValueError(f"未知 action: {action}")
    if action == "refresh-production-config":
        raise ValueError(
            "refresh-production-config 是组合动作，请通过本适配器 main 执行"
        )
    if action == "refresh-outfits":
        if record_id or limit is not None:
            raise ValueError("refresh-outfits 不接受任务记录、产品编码或任务数量")
        return [
            sys.executable,
            str(OUTFIT_REFRESH_RUNNER),
            "--db",
            str(OUTFIT_REFRESH_DB),
            "feishu",
            "--config",
            str(OUTFIT_REFRESH_CONFIG),
            "pull-templates",
            "--role",
            "styling",
        ]
    if action == "refresh-personas":
        if record_id or limit is not None:
            raise ValueError("refresh-personas 不接受任务记录、产品编码或任务数量")
        return [
            sys.executable,
            str(PERSONA_REFRESH_RUNNER),
            "--pull-to-db",
            "--no-seed",
            "--db-path",
            str(OUTFIT_REFRESH_DB),
        ]
    if action in FIRST_FRAME_ACTIONS:
        if limit is not None and not 1 <= limit <= 20:
            raise ValueError("一次最多处理 20 条用户已勾选的首帧任务")
        command = [sys.executable, str(FIRST_FRAME_RUNNER)]
        if record_id:
            command.extend(["--record-id", _validate_record_id(record_id)])
        if action == "first-frame-check":
            command.append("--dry-run")
        elif action == "first-frame-retry":
            command.append("--force")
        if limit is not None:
            command.extend(["--limit", str(limit)])
        return command
    if record_id:
        record_id = _validate_record_id(record_id)
    if limit is not None and not 1 <= limit <= 5:
        raise ValueError("一次最多处理 5 条运营任务；单条任务内的脚本数量由飞书的生成数量决定")
    if action in {"plan", "resume", "replan"} and not record_id:
        raise ValueError(f"{action} 必须指定 record_id 或 product_code")

    command = [sys.executable, str(RUNNER)]
    if record_id:
        command.extend(["--record-id", record_id])
    if action == "check":
        command.append("--dry-run")
    elif action == "plan":
        command.append("--plan-only")
    elif action == "resume":
        command.append("--resume-failed")
    elif action == "replan":
        command.append("--replan")
    elif action == "export-ready":
        command.append("--export-ready-only")

    if limit is not None:
        command.extend(["--limit", str(limit)])
    return command


def build_refresh_commands(action: str) -> list[list[str]]:
    """Return fixed refresh subprocesses without accepting task input."""

    if action == "refresh-production-config":
        return [
            build_runner_command(action="refresh-outfits"),
            build_runner_command(action="refresh-personas"),
        ]
    if action in {"refresh-outfits", "refresh-personas"}:
        return [build_runner_command(action=action)]
    raise ValueError(f"非刷新动作: {action}")


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="OpenClaw 原创脚本任务适配器（只转发固定白名单命令）"
    )
    parser.add_argument("action", choices=sorted(ALL_ACTIONS))
    selector = parser.add_mutually_exclusive_group()
    selector.add_argument("--record-id")
    selector.add_argument("--product-code")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="运营任务动作处理1-5行；首帧动作处理1-20条脚本行",
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.action in REFRESH_ACTIONS and (
            args.record_id or args.product_code or args.limit is not None
        ):
            raise ValueError("刷新模板是独立动作，不接受任务选择参数")
        if args.action == "refresh-production-config":
            commands = build_refresh_commands(args.action)
            environment = os.environ.copy()
            for index, refresh_command in enumerate(commands, start=1):
                print(
                    f"配置刷新 {index}/{len(commands)}: "
                    + Path(refresh_command[1]).name
                )
                completed = subprocess.run(
                    refresh_command,
                    cwd=str(SKILL_ROOT),
                    env=environment,
                    check=False,
                )
                if completed.returncode:
                    return completed.returncode
            from core.outfit_template_provider import (
                get_outfit_template_provider_snapshot,
            )
            from core.persona_template_provider import load_persona_templates

            outfit = get_outfit_template_provider_snapshot()
            persona = load_persona_templates()
            print(json.dumps({
                "status": "REFRESHED",
                "outfit": {
                    "last_refreshed_at": outfit.get("last_refreshed_at"),
                    "template_count": outfit.get("template_count"),
                    "enabled_template_count": outfit.get(
                        "enabled_template_count"
                    ),
                    "soft_warnings": outfit.get("soft_warnings") or [],
                },
                "persona": {
                    "last_refreshed_at": persona.get("last_refreshed_at"),
                    "enabled_count": persona.get("enabled_count"),
                    "approved_asset_count": persona.get(
                        "approved_asset_count"
                    ),
                    "text_quality_warning_count": persona.get(
                        "text_quality_warning_count"
                    ),
                    "soft_warnings": persona.get("soft_warnings") or [],
                },
            }, ensure_ascii=False, indent=2))
            return 0
        record_id = args.record_id
        if record_id:
            record_id = _validate_record_id(record_id)
        if args.product_code and args.action not in FIRST_FRAME_ACTIONS:
            record_id = resolve_record_id_for_product(
                product_code=args.product_code,
                action=args.action,
            )
            print(f"已定位运营任务: {record_id} | 产品: {args.product_code}")
        if args.action in {"plan", "resume", "replan"} and not record_id:
            raise ValueError(f"{args.action} 必须指定 --record-id 或 --product-code")
        limit = args.limit
        if args.action not in REFRESH_ACTIONS and not record_id and limit is None:
            limit = 5 if args.action in FIRST_FRAME_ACTIONS else 1
        command = build_runner_command(
            action=args.action,
            record_id=record_id,
            limit=limit,
        )
        if args.product_code and args.action in FIRST_FRAME_ACTIONS:
            command.extend(["--product-code", _validate_product_code(args.product_code)])
    except ValueError as exc:
        print(f"参数错误: {exc}", file=sys.stderr)
        return 2

    environment = os.environ.copy()
    # This is the current formal workflow's read-only scene-reference switch.
    environment["ORIGINAL_SCRIPT_SCENE_REFERENCE_ENABLED"] = "1"
    print(f"OpenClaw 原创任务动作: {args.action}")
    completed = subprocess.run(command, cwd=str(SKILL_ROOT), env=environment, check=False)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
