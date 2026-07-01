#!/usr/bin/env python3
"""ADS 无人值守混剪编排脚本.

扫描产品当前素材池和成片，生成补充计划、写 Prompt Package、回流检查并触发 guard 渲染。
真实提单默认交给 run_ai_supplement_heartbeat.py / segment-package-worker 消费飞书表格。

Usage:
  python3 scripts/run_ads_mixcut_unattended.py \\
    --product-id 1729659517276948599 --target-count 30 --use-voc-hooks --dry-run --json

正式执行需要 --write；默认不会在本脚本里直接提交即梦/Imini。
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import math
import os
import signal
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, unquote, urlparse

try:
    import pymysql
except ImportError:
    pymysql = None  # type: ignore

SKILL_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# auto_mixcut is at workspace/auto_mixcut → one level up to workspace
_WORKSPACE_ROOT = os.path.dirname(SKILL_ROOT)
_ENV_PATH = os.path.join(_WORKSPACE_ROOT, ".env")
JIMENG_SKILL_ROOT = os.path.join(_WORKSPACE_ROOT, "skills", "jimeng-video-generator")
SEGMENT_PACKAGE_CONFIG = "segment-package.json"
if SKILL_ROOT not in sys.path:
    sys.path.insert(0, SKILL_ROOT)

from auto_mixcut.config.factory_config import factory_config  # noqa: E402
from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.skills.ai_supplement_gateway_skill import summarize_package_rows  # noqa: E402
from auto_mixcut.skills.ai_supplement_workbench_skill import AISupplementWorkbenchSkill  # noqa: E402
from auto_mixcut.skills.capacity_counter_skill import CapacityCounterSkill  # noqa: E402
from auto_mixcut.skills.feishu_review_skill import sync_product_task_best_effort  # noqa: E402
from auto_mixcut.skills.mixcut_state_machine_skill import decide_factory_state  # noqa: E402
if os.path.exists(_ENV_PATH):
    for _line in open(_ENV_PATH, encoding="utf-8"):
        _line = _line.strip()
        if not _line or _line.startswith("#") or "=" not in _line:
            continue
        _k, _v = _line.split("=", 1)
        _k, _v = _k.strip(), _v.strip()
        if len(_v) >= 2 and _v[0] == _v[-1] and _v[0] in "'\"":
            _v = _v[1:-1]
        if _k and _k not in os.environ:
            os.environ[_k] = _v

# ── RDS DDL ──
RUN_TABLE_DDL = """CREATE TABLE IF NOT EXISTS ads_mixcut_unattended_run (
  run_id VARCHAR(191) NOT NULL PRIMARY KEY,
  product_id VARCHAR(128) NOT NULL,
  target_count INT DEFAULT 30,
  use_voc_hooks TINYINT DEFAULT 1,
  short_ads_mode TINYINT DEFAULT 1,
  max_new_hook_segments INT DEFAULT 6,
  max_new_support_segments INT DEFAULT 12,
  status VARCHAR(64) NOT NULL,
  current_step VARCHAR(64) NULL,
  existing_output_count INT DEFAULT 0,
  planned_new_hook_segments INT DEFAULT 0,
  planned_new_support_segments INT DEFAULT 0,
  rendered_output_count INT DEFAULT 0,
  payload_json JSON,
  error_message TEXT,
  created_at VARCHAR(64) NOT NULL,
  updated_at VARCHAR(64) NOT NULL,
  INDEX idx_ads_run_product (product_id)
) CHARACTER SET utf8mb4"""

CORE_ROLES = {"hero", "result", "detail", "scene", "ending"}
HOOK_ROLES = {"hero"}
VOC_ADS_HOOK_PACKAGE_TEMPLATE_ID = "VOC_ADS_HOOK_PACKAGE"
ADS_FAST_TEMPLATE_ID = "AD_FAST_HOOK_8S"
DEFAULT_ADS_VOC_QUOTA_RATIO = 0.2
DEFAULT_ADS_VOC_SEGMENT_OUTPUT_CAP = 2
DEFAULT_MAX_RENDERS_PER_PASS = 4
DEFAULT_AI_PACKAGE_STOCK_RATIO = 0.5
DEFAULT_AI_PACKAGE_STOCK_MIN = 6
GOAL_MODE_ABSOLUTE_TARGET = "absolute_target"
GOAL_MODE_INCREMENTAL_ADD = "incremental_add"
GOAL_MODE_FACTORY_TIER = "factory_tier"
GOAL_MODES = {GOAL_MODE_ABSOLUTE_TARGET, GOAL_MODE_INCREMENTAL_ADD, GOAL_MODE_FACTORY_TIER}
FACTORY_TIERS = {20, 40, 60, 80}
GOOD_MACHINE_OUTPUT_STATUSES = {"passed", "passed_with_warning", "needs_review", "publish_ready"}
REJECTED_HUMAN_OUTPUT_STATUSES = {"rejected", "discard", "不可发布", "废弃", "不要", "不使用"}
FAILED_SEGMENT_STATUSES = {
    "qc_failed",
    "frame_sample_failed",
    "frame_sample_timeout",
    "fingerprint_failed",
    "tag_failed",
    "effective_role_failed",
    "ai_stage_failed",
    "unusable",
}
STAGE_KEYS = [
    "prepare_voc_hooks",
    "ai_supplement",
    "submit_hook_packages",
    "import_returns",
    "wait_returns",
    "segment_qc",
    "render",
    "final_qc",
]
def is_truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "是"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)) or default)
    except ValueError:
        return default


def connect_db(url: Optional[str] = None):
    if pymysql is None:
        raise RuntimeError("pymysql not installed")
    url = url or os.environ.get("AUTO_MIXCUT_DATABASE_URL") or os.environ.get("LIKEU_AI_DATABASE_URL") or ""
    if not url:
        raise RuntimeError("No database URL")
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    return pymysql.connect(
        host=parsed.hostname, port=parsed.port or 3306,
        user=unquote(parsed.username or ""), password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        charset=query.get("charset", ["utf8mb4"])[0],
        connect_timeout=_env_int("AUTO_MIXCUT_ADS_DB_CONNECT_TIMEOUT", 10),
        read_timeout=_env_int("AUTO_MIXCUT_ADS_DB_READ_TIMEOUT", 20),
        write_timeout=_env_int("AUTO_MIXCUT_ADS_DB_WRITE_TIMEOUT", 20),
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def trace_step(event: str, **payload: Any) -> None:
    path = os.environ.get("AUTO_MIXCUT_ADS_TRACE_FILE", "").strip()
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"ts": now_iso(), "event": event, **payload}, ensure_ascii=False, default=str) + "\n")
    except Exception:
        return


def jload(v: Any) -> Any:
    if v is None or v == "":
        return None
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v)
    except Exception:
        return None


def command_result(
    cmd: List[str],
    cwd: str,
    env: Optional[Dict[str, str]] = None,
    timeout_minutes: int = 60,
) -> Dict[str, Any]:
    started = now_iso()
    merged_env = dict(os.environ)
    if env:
        merged_env.update(env)
    try:
        proc = subprocess.run(
            cmd,
            cwd=cwd,
            env=merged_env,
            text=True,
            capture_output=True,
            timeout=max(1, int(timeout_minutes or 1)) * 60,
        )
        status = "completed" if proc.returncode == 0 else "failed"
        return {
            "status": status,
            "returncode": proc.returncode,
            "cmd": cmd,
            "cwd": cwd,
            "started_at": started,
            "finished_at": now_iso(),
            "stdout_tail": (proc.stdout or "")[-4000:],
            "stderr_tail": (proc.stderr or "")[-4000:],
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "status": "failed",
            "returncode": None,
            "cmd": cmd,
            "cwd": cwd,
            "started_at": started,
            "finished_at": now_iso(),
            "error": f"timeout after {timeout_minutes} minutes",
            "stdout_tail": (exc.stdout or "")[-4000:] if isinstance(exc.stdout, str) else "",
            "stderr_tail": (exc.stderr or "")[-4000:] if isinstance(exc.stderr, str) else "",
        }


def _env_truthy(name: str) -> bool:
    return str(os.environ.get(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def submit_channel_label(channel: str) -> str:
    return "Imini" if str(channel or "").strip().lower() == "imini" else "即梦"


def stage_status(result: Dict[str, Any]) -> tuple[str, str]:
    current_step = "plan"
    status = ((result.get("plan") or {}).get("status") or "planned")
    for key in STAGE_KEYS:
        if key not in result:
            continue
        current_step = key
        item = result.get(key) or {}
        item_status = str(item.get("status") or "")
        if item_status in {"failed", "blocked", "timeout"}:
            status = f"{key}_{item_status}"
        elif item_status == "completed":
            status = key
    final = result.get("final_inspect") or {}
    if int(final.get("good_outputs") or 0) >= int(result.get("target_count") or 0):
        status = "complete"
        current_step = "complete"
    elif final:
        packages = final.get("prompt_packages") or {}
        if int(packages.get("failed") or 0) > 0 and result.get("scoped_prompt_ids"):
            status = "prompt_package_failed"
            current_step = "wait_returns"
        elif current_step in {"render", "final_qc"}:
            quantity_goal = final.get("quantity_goal") or {}
            new_good_outputs = _to_int(quantity_goal.get("new_good_outputs"), 0)
            status = "in_progress" if new_good_outputs > 0 else "incomplete"
    return status, current_step


def prepared_prompt_ids(result: Dict[str, Any]) -> List[str]:
    prepare = result.get("prepare_voc_hooks") or {}
    payload = prepare.get("result") or {}
    ids: List[str] = []
    for item in payload.get("created") or []:
        prompt_id = str(item.get("segment_prompt_id") or "").strip()
        if prompt_id:
            ids.append(prompt_id)
    return ids


def normalize_goal_args(args: argparse.Namespace) -> argparse.Namespace:
    add_count = _to_int(getattr(args, "add_count", 0), 0)
    factory_tier = _to_int(getattr(args, "factory_tier", 0), 0)
    if add_count > 0:
        args.goal_mode = GOAL_MODE_INCREMENTAL_ADD
        args.target_count = add_count
    if factory_tier > 0:
        args.goal_mode = GOAL_MODE_FACTORY_TIER
        args.target_count = factory_tier
    if getattr(args, "goal_mode", "") not in GOAL_MODES:
        args.goal_mode = GOAL_MODE_ABSOLUTE_TARGET
    args.target_count = max(0, _to_int(getattr(args, "target_count", 0), 0))
    args.add_count = max(0, add_count)
    if args.goal_mode == GOAL_MODE_FACTORY_TIER and factory_tier <= 0 and args.target_count in FACTORY_TIERS:
        factory_tier = args.target_count
    args.factory_tier = factory_tier if factory_tier in FACTORY_TIERS else 0
    return args


def build_quantity_goal(
    out: Dict[str, Any],
    target_count: int,
    goal_mode: str = GOAL_MODE_ABSOLUTE_TARGET,
    add_count: int = 0,
    factory_tier: int = 0,
) -> Dict[str, Any]:
    start_good = max(0, _to_int(out.get("good_outputs"), 0))
    requested = max(0, _to_int(target_count, 0))
    mode = goal_mode if goal_mode in GOAL_MODES else GOAL_MODE_ABSOLUTE_TARGET

    if mode == GOAL_MODE_INCREMENTAL_ADD:
        desired_new = max(0, _to_int(add_count, 0) or requested)
        target_good = start_good + desired_new
    elif mode == GOAL_MODE_FACTORY_TIER:
        tier = _to_int(factory_tier, 0) or requested
        target_good = tier if tier in FACTORY_TIERS else requested
        desired_new = max(0, target_good - start_good)
    else:
        target_good = requested
        desired_new = max(0, target_good - start_good)

    return update_quantity_goal_progress(
        {
            "goal_mode": mode,
            "requested_target_count": requested,
            "start_strict_good_count": start_good,
            "target_strict_good_count": max(0, target_good),
            "desired_new_good_count": desired_new,
            "factory_tier": target_good if mode == GOAL_MODE_FACTORY_TIER else 0,
        },
        start_good,
    )


def update_quantity_goal_progress(goal: Dict[str, Any], current_good: int) -> Dict[str, Any]:
    target_good = max(0, _to_int(goal.get("target_strict_good_count"), 0))
    start_good = max(0, _to_int(goal.get("start_strict_good_count"), 0))
    current_good = max(0, _to_int(current_good, 0))
    return {
        **goal,
        "current_strict_good_count": current_good,
        "new_good_outputs": max(0, current_good - start_good),
        "remaining_to_target": max(0, target_good - current_good),
    }


def render_batch_limit(value: Any) -> int:
    limit = _to_int(value, DEFAULT_MAX_RENDERS_PER_PASS)
    return max(0, limit)


# ── Step 1: inspect ──

def load_task(conn, product_id: str) -> Optional[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM content_tasks WHERE product_id=%s ORDER BY updated_at DESC LIMIT 1",
        (product_id,),
    )
    return cur.fetchone()


def load_segment_summary(conn, product_id: str) -> Dict[str, Any]:
    cur = conn.cursor()
    ph = _sql_placeholder(conn)
    cur.execute(
        "SELECT s.segment_id, s.effective_roles_json, s.slot_role, s.is_image_generated, s.prompt_package_id, "
        "s.segment_type, s.segment_status, s.source_type, s.product_mismatch_suspect, s.product_mismatch_reason, "
        "p.template_id AS prompt_template_id "
        "FROM segments s "
        "LEFT JOIN segment_prompt_packages p ON p.segment_prompt_id=s.prompt_package_id "
        f"WHERE s.product_id={ph}",
        (product_id,),
    )
    rows = [_row_dict(row) for row in cur.fetchall()]
    latest_tags = load_latest_segment_tags(conn, [str(r.get("segment_id") or "") for r in rows])
    by_role: Dict[str, int] = Counter()
    by_core_role: Dict[str, int] = Counter()
    by_status: Dict[str, int] = Counter()
    hook_count = 0
    voc_total = 0
    voc_usable = 0
    voc_unusable = 0
    voc_mismatch_suspect = 0
    mismatch_suspect = 0
    segments: List[Dict] = []
    for r in rows:
        tag = latest_tags.get(str(r.get("segment_id") or ""), {})
        status = str(r.get("segment_status") or "").strip()
        source_type = str(r.get("source_type") or "").strip()
        is_voc_segment = str(r.get("prompt_template_id") or "") == VOC_ADS_HOOK_PACKAGE_TEMPLATE_ID
        if is_voc_segment:
            voc_total += 1
        by_status[status or "unknown"] += 1
        if status in FAILED_SEGMENT_STATUSES:
            if is_voc_segment:
                voc_unusable += 1
            continue
        if source_type == "ai_generated" and status != "qc_passed":
            if is_voc_segment:
                voc_unusable += 1
            continue
        if is_truthy_flag(r.get("product_mismatch_suspect")):
            mismatch_suspect += 1
            if is_voc_segment:
                voc_unusable += 1
                voc_mismatch_suspect += 1
            continue
        roles = jload(r.get("effective_roles_json")) or []
        if not isinstance(roles, list):
            roles = []
        role_set = {str(role) for role in roles if str(role or "").strip()}
        slot = r.get("slot_role") or ""
        hook_type = tag.get("hook_visual_type") or "none"
        primary = tag.get("primary_shot_role") or slot
        if primary:
            role_set.add(str(primary))
        is_hook = (
            "hero" in role_set
            and hook_type
            and hook_type not in ("none", "")
            and (tag.get("hook_strength") or "") in {"strong", "medium"}
        )
        for role in role_set:
            by_role[role] += 1
            if role in CORE_ROLES:
                by_core_role[role] += 1
        if is_hook:
            hook_count += 1
        if is_voc_segment:
            voc_usable += 1
        segments.append({
            "segment_id": r.get("segment_id"),
            "effective_roles": sorted(role_set),
            "primary_shot_role": primary,
            "segment_status": status,
            "source_type": source_type,
            "prompt_package_id": r.get("prompt_package_id"),
            "prompt_template_id": r.get("prompt_template_id"),
            "is_voc_ads_hook_package": is_voc_segment,
            "is_hook": is_hook,
            "hook_visual_type": hook_type,
            "hook_strength": tag.get("hook_strength"),
            "is_image_generated": bool(r.get("is_image_generated")),
        })
    return {
        "raw_total": len(rows),
        "total": len(segments),
        "by_status": dict(by_status),
        "by_role": dict(by_role),
        "by_core_role": dict(by_core_role),
        "hook_segments": hook_count,
        "voc_segments": {
            "total": voc_total,
            "usable": voc_usable,
            "unusable": voc_unusable,
            "mismatch_suspect": voc_mismatch_suspect,
        },
        "product_mismatch_suspect_segments": mismatch_suspect,
        "segments": segments,
    }


def load_latest_segment_tags(conn, segment_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    ids = [str(segment_id or "").strip() for segment_id in segment_ids if str(segment_id or "").strip()]
    if not ids:
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    cur = conn.cursor()
    ph = _sql_placeholder(conn)
    for chunk in _chunks(ids, 500):
        placeholders = ",".join([ph] * len(chunk))
        cur.execute(
            "SELECT t.segment_id, t.primary_shot_role, t.hook_visual_type, t.hook_strength "
            "FROM segment_tags t "
            "JOIN ("
            "  SELECT segment_id, MAX(id) AS id FROM segment_tags "
            f"  WHERE segment_id IN ({placeholders}) GROUP BY segment_id"
            ") latest ON latest.id=t.id",
            tuple(chunk),
        )
        for row in (_row_dict(item) for item in cur.fetchall()):
            result[str(row.get("segment_id") or "")] = row
    return result


def _row_dict(row: Any) -> Dict[str, Any]:
    if isinstance(row, dict):
        return row
    try:
        return dict(row)
    except Exception:
        return {}


def _sql_placeholder(conn) -> str:
    return "?" if conn.__class__.__module__.startswith("sqlite3") else "%s"


def _chunks(items: List[str], size: int):
    for idx in range(0, len(items), max(1, size)):
        yield items[idx : idx + size]


def load_output_summary(conn, product_id: str) -> Dict[str, Any]:
    cur = conn.cursor()
    good_statuses = sorted(GOOD_MACHINE_OUTPUT_STATUSES)
    rejected_statuses = sorted(REJECTED_HUMAN_OUTPUT_STATUSES)
    failed_statuses = sorted(FAILED_SEGMENT_STATUSES)
    failed_placeholders = ",".join(["%s"] * len(failed_statuses))
    cur.execute(
        "SELECT o.output_id, o.template_id, o.render_status, o.machine_quality_status, o.human_quality_status, o.duration_ms, "
        f"SUM(CASE WHEN s.segment_status IN ({failed_placeholders}) THEN 1 ELSE 0 END) failed_segment_count, "
        "SUM(CASE WHEN s.segment_id IS NULL AND os.segment_id IS NOT NULL THEN 1 ELSE 0 END) missing_segment_count, "
        "SUM(CASE WHEN p.template_id='VOC_ADS_HOOK_PACKAGE' THEN 1 ELSE 0 END) voc_segment_count "
        "FROM outputs o "
        "LEFT JOIN output_segments os ON os.output_id=o.output_id "
        "LEFT JOIN segments s ON s.segment_id=os.segment_id "
        "LEFT JOIN segment_prompt_packages p ON p.segment_prompt_id=s.prompt_package_id "
        "WHERE o.product_id=%s "
        "GROUP BY o.output_id, o.template_id, o.render_status, o.machine_quality_status, o.human_quality_status, o.duration_ms",
        (*failed_statuses, product_id),
    )
    rows = cur.fetchall()
    good_set = set(good_statuses)
    rejected_set = set(rejected_statuses)
    total = len(rows)
    rendered = 0
    base_good = 0
    strict_good = 0
    ads_total = 0
    ads_rendered = 0
    ads_base_good = 0
    ads_strict_good = 0
    outputs_with_failed_segments = 0
    outputs_missing_segments = 0
    good_excluded_by_failed_segments = 0
    outputs_with_voc_segments = 0
    base_good_outputs_with_voc_segments = 0
    strict_good_outputs_with_voc_segments = 0
    voc_rendered_output_count = 0
    voc_draft_only_output_count = 0
    voc_failed_segment_output_count = 0
    strict_durations: List[int] = []
    for row in rows:
        is_rendered = row.get("render_status") == "rendered"
        is_ads_output = str(row.get("template_id") or "").upper().startswith("AD_FAST")
        if is_ads_output:
            ads_total += 1
        if is_rendered:
            rendered += 1
            if is_ads_output:
                ads_rendered += 1
        is_base_good = (
            is_rendered
            and row.get("machine_quality_status") in good_set
            and str(row.get("human_quality_status") or "") not in rejected_set
        )
        failed_count = int(row.get("failed_segment_count") or 0)
        missing_count = int(row.get("missing_segment_count") or 0)
        voc_count = int(row.get("voc_segment_count") or 0)
        has_segment_issue = failed_count > 0 or missing_count > 0
        if failed_count > 0:
            outputs_with_failed_segments += 1
        if missing_count > 0:
            outputs_missing_segments += 1
        if voc_count > 0:
            outputs_with_voc_segments += 1
            if is_rendered:
                voc_rendered_output_count += 1
            if str(row.get("machine_quality_status") or "") == "draft_only":
                voc_draft_only_output_count += 1
            if has_segment_issue:
                voc_failed_segment_output_count += 1
        if is_base_good:
            base_good += 1
            if is_ads_output:
                ads_base_good += 1
            if voc_count > 0:
                base_good_outputs_with_voc_segments += 1
            if has_segment_issue:
                good_excluded_by_failed_segments += 1
            else:
                strict_good += 1
                if is_ads_output:
                    ads_strict_good += 1
                if voc_count > 0:
                    strict_good_outputs_with_voc_segments += 1
                if is_ads_output:
                    strict_durations.append(int(row.get("duration_ms") or 0))
    return {
        "total_outputs": total,
        "good_outputs": ads_strict_good,
        "base_good_outputs": ads_base_good,
        "rendered_outputs": ads_rendered,
        "all_good_outputs": strict_good,
        "all_base_good_outputs": base_good,
        "all_rendered_outputs": rendered,
        "ads_total_outputs": ads_total,
        "good_outputs_excluded_by_failed_segments": good_excluded_by_failed_segments,
        "outputs_with_failed_segments": outputs_with_failed_segments,
        "outputs_missing_segments": outputs_missing_segments,
        "outputs_with_voc_segments": outputs_with_voc_segments,
        "good_outputs_with_voc_segments": strict_good_outputs_with_voc_segments,
        "base_good_outputs_with_voc_segments": base_good_outputs_with_voc_segments,
        "strict_good_outputs_with_voc_segments": strict_good_outputs_with_voc_segments,
        "voc_strict_good_output_count": strict_good_outputs_with_voc_segments,
        "voc_rendered_output_count": voc_rendered_output_count,
        "voc_draft_only_output_count": voc_draft_only_output_count,
        "voc_failed_segment_output_count": voc_failed_segment_output_count,
        "avg_duration_ms": int(sum(strict_durations) / len(strict_durations)) if strict_durations else 0,
        "min_duration_ms": min(strict_durations) if strict_durations else 0,
        "max_duration_ms": max(strict_durations) if strict_durations else 0,
    }


def load_voc_hook_package(conn, product_id: str) -> Optional[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        "SELECT package_id, readiness_status, manual_confirmation_status, hook_candidate_count, payload_json "
        "FROM voc_ads_hook_package WHERE product_id=%s AND usecase='ads_mixcut' "
        "ORDER BY CASE "
        "WHEN readiness_status='ready_for_hook_package' AND manual_confirmation_status LIKE 'confirmed%%' THEN 0 "
        "WHEN readiness_status='smoke_ready_unconfirmed' THEN 1 "
        "ELSE 2 END, updated_at DESC LIMIT 1",
        (product_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    payload = jload(row.get("payload_json")) or {}
    return {
        "package_id": row["package_id"],
        "readiness_status": row["readiness_status"],
        "manual_confirmation_status": row["manual_confirmation_status"],
        "hook_candidate_count": row.get("hook_candidate_count") or 0,
        "confirmed": str(row["manual_confirmation_status"] or "").startswith("confirmed")
        and row["readiness_status"] == "ready_for_hook_package",
        "candidates": payload.get("hook_candidates") or [],
    }


def diagnose_voc_gap(conn, product_id: str) -> Dict[str, Any]:
    cur = conn.cursor()
    result: Dict[str, Any] = {"product_id": product_id}
    checks = [
        ("raw_voc_count", "SELECT COUNT(*) c FROM fastmoss_voc_raw WHERE fastmoss_product_id=%s"),
        ("enriched_voc_count", "SELECT COUNT(*) c FROM fastmoss_voc_enriched WHERE fastmoss_product_id=%s"),
        ("snapshot_count", "SELECT COUNT(*) c FROM fastmoss_voc_product_snapshot WHERE fastmoss_product_id=%s"),
        ("recommendation_count", "SELECT COUNT(*) c FROM fastmoss_voc_product_recommendation WHERE product_id=%s"),
        ("ads_hook_package_count", "SELECT COUNT(*) c FROM voc_ads_hook_package WHERE product_id=%s AND usecase='ads_mixcut'"),
    ]
    for key, sql in checks:
        try:
            cur.execute(sql, (product_id,))
            row = cur.fetchone() or {}
            result[key] = int(row.get("c") or 0)
        except Exception as exc:
            result[key] = None
            result.setdefault("diagnostic_errors", []).append({"check": key, "error": str(exc)})

    evidence_count = int(result.get("raw_voc_count") or 0) + int(result.get("enriched_voc_count") or 0) + int(result.get("snapshot_count") or 0)
    reco_count = int(result.get("recommendation_count") or 0)
    package_count = int(result.get("ads_hook_package_count") or 0)
    if evidence_count <= 0 and reco_count <= 0:
        result["missing_reason"] = "product_not_in_voc_capture_pool"
        result["next_action"] = "run FastMoss VOC capture for this product, or build a manually confirmed category-transfer VOC hook package"
    elif reco_count <= 0:
        result["missing_reason"] = "product_voc_recommendation_missing"
        result["next_action"] = "run voc-insight product scope for this product, then build ADS hook package"
    elif package_count <= 0:
        result["missing_reason"] = "ads_hook_package_missing"
        result["next_action"] = "run build_ads_hook_package.py and confirm one or more VOC selling points"
    else:
        result["missing_reason"] = "ads_hook_package_not_ready_or_unconfirmed"
        result["next_action"] = "check voc_ads_hook_package readiness_status/manual_confirmation_status"
    return result


def load_prompt_package_summary(conn, product_id: str, prompt_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    cur = conn.cursor()
    if prompt_ids:
        placeholders = ",".join(["%s"] * len(prompt_ids))
        cur.execute(
            "SELECT segment_prompt_id, package_status, feishu_record_id, external_provider, external_job_id, "
            "generated_asset_id, generated_segment_id, failure_reason, result_sync_status, submit_channel, created_at, updated_at "
            f"FROM segment_prompt_packages WHERE product_id=%s AND segment_prompt_id IN ({placeholders})",
            (product_id, *prompt_ids),
        )
    else:
        cur.execute(
            "SELECT segment_prompt_id, package_status, feishu_record_id, external_provider, external_job_id, "
            "generated_asset_id, generated_segment_id, failure_reason, result_sync_status, submit_channel, created_at, updated_at "
            "FROM segment_prompt_packages WHERE product_id=%s",
            (product_id,),
        )
    rows = cur.fetchall()
    return summarize_package_rows(rows)


def voc_participation_summary(use_voc_hooks: bool, voc: Optional[Dict[str, Any]], voc_gap: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not use_voc_hooks:
        return {"mode": "voc_disabled", "participates": False, "reason": "voc_hooks_disabled"}
    if voc and voc.get("confirmed"):
        return {
            "mode": "product_voc",
            "participates": True,
            "package_id": voc.get("package_id"),
            "candidate_count": int(voc.get("hook_candidate_count") or len(voc.get("candidates") or [])),
        }
    if voc:
        return {
            "mode": "voc_unconfirmed_ignored",
            "participates": False,
            "package_id": voc.get("package_id"),
            "readiness": voc.get("readiness_status"),
            "manual_confirmation_status": voc.get("manual_confirmation_status"),
            "reason": "voc_package_not_confirmed_ignored",
            "next_action": "regular ADS_FAST continues; confirm VOC only for experimental dedicated hook packages",
        }
    reason = (voc_gap or {}).get("missing_reason") or "voc_package_missing"
    mode = "category_voc_transfer_needed" if reason == "product_not_in_voc_capture_pool" else "voc_missing"
    return {
        "mode": mode,
        "participates": False,
        "reason": reason,
        "next_action": (voc_gap or {}).get("next_action") or "build and confirm ADS VOC hook package",
    }


def bottleneck_summary(task: Optional[Dict[str, Any]], seg: Dict[str, Any], remaining: int) -> Dict[str, Any]:
    task = task or {}
    current = str(task.get("current_bottleneck") or "")
    note = str(task.get("capacity_note") or task.get("blocked_reason") or "")
    first_slot_capacity = _to_int(task.get("first_slot_remaining_capacity"), 0)
    material_extra = _to_int(task.get("material_pool_extra_capacity"), 0)
    role = ""
    action = "none"
    if remaining <= 0:
        role = ""
        action = "target_met"
    elif first_slot_capacity <= 0 and ("首镜" in note or "first_slot" in note or "复用模式" in current or "first slot" in note.lower()):
        role = "hero"
        action = "submit_or_generate_hero_first_slot"
    elif material_extra <= 0:
        role = "mixed_support"
        action = "generate_support_segments"
    else:
        role = "render_capacity"
        action = "render_guard"
    return {
        "role": role,
        "action": action,
        "current_bottleneck": current,
        "capacity_note": note,
        "first_slot_remaining_capacity": first_slot_capacity,
        "material_pool_extra_capacity": material_extra,
        "hero_segments": int((seg.get("by_core_role") or {}).get("hero") or 0),
    }


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value if value is not None else default)
    except (TypeError, ValueError):
        return default


def _ads_voc_output_quota(target: int, use_voc_hooks: bool, voc_confirmed: bool, voc_candidates: int) -> int:
    if not use_voc_hooks or not voc_confirmed or voc_candidates <= 0:
        return 0
    target_count = max(0, int(target or 0))
    if target_count <= 0:
        return 0
    return min(target_count, max(1, math.ceil(target_count * _ads_voc_quota_ratio())))


def _ads_voc_renderable_quota(target: int, use_voc_hooks: bool, voc_confirmed: bool, voc_candidates: int, voc_usable_segments: int) -> int:
    desired = _ads_voc_output_quota(target, use_voc_hooks, voc_confirmed, voc_candidates)
    if desired <= 0 or voc_usable_segments <= 0:
        return 0
    return min(desired, voc_usable_segments * _ads_voc_segment_output_cap())


def _ads_voc_segment_gap(desired_output_quota: int, filled_outputs: int, usable_segments: int) -> int:
    output_gap = max(0, int(desired_output_quota or 0) - int(filled_outputs or 0))
    if output_gap <= 0:
        return 0
    needed_segments = math.ceil(output_gap / _ads_voc_segment_output_cap())
    return max(0, needed_segments - max(0, int(usable_segments or 0)))


def _ads_voc_quota_ratio() -> float:
    try:
        value = float(os.environ.get("AUTO_MIXCUT_ADS_VOC_PROOF_QUOTA_RATIO", DEFAULT_ADS_VOC_QUOTA_RATIO))
    except (TypeError, ValueError):
        value = DEFAULT_ADS_VOC_QUOTA_RATIO
    return min(1.0, max(0.0, value))


def _ads_voc_segment_output_cap() -> int:
    try:
        value = int(os.environ.get("AUTO_MIXCUT_ADS_VOC_SEGMENT_OUTPUT_CAP", DEFAULT_ADS_VOC_SEGMENT_OUTPUT_CAP))
    except (TypeError, ValueError):
        value = DEFAULT_ADS_VOC_SEGMENT_OUTPUT_CAP
    return max(1, value)


def _voc_quota_status(
    use_voc_hooks: bool,
    voc_confirmed: bool,
    desired_quota: int,
    renderable_quota: int,
    filled_outputs: int,
    usable_segments: int,
) -> str:
    if not use_voc_hooks:
        return "disabled"
    if not voc_confirmed:
        return "package_missing_or_unconfirmed"
    if desired_quota <= 0:
        return "not_applicable"
    if filled_outputs >= desired_quota:
        return "filled"
    if usable_segments <= 0:
        return "needs_voc_segment_generation"
    if renderable_quota <= filled_outputs:
        return "needs_more_voc_segments"
    return "needs_voc_proof_render"


# ── Step 2: plan ──

def plan_ads_mixcut(
    product_id: str,
    task: Optional[Dict[str, Any]],
    seg: Dict[str, Any],
    out: Dict[str, Any],
    voc: Optional[Dict[str, Any]],
    voc_gap: Optional[Dict[str, Any]],
    target: int,
    use_voc_hooks: bool,
    max_hook: int,
    max_support: int,
    quantity_goal: Optional[Dict[str, Any]] = None,
    max_renders_per_pass: int = DEFAULT_MAX_RENDERS_PER_PASS,
) -> Dict[str, Any]:
    quantity_goal = quantity_goal or build_quantity_goal(out, target, GOAL_MODE_ABSOLUTE_TARGET)
    existing_good = max(0, _to_int(out.get("good_outputs"), 0))
    target = max(0, _to_int(quantity_goal.get("target_strict_good_count"), target))
    remaining = max(0, target - existing_good)
    pass_limit = render_batch_limit(max_renders_per_pass)
    blockers: List[str] = []
    if task is None:
        blockers.append("content_task_missing")

    hero_count = seg["by_core_role"].get("hero", 0)
    result_count = seg["by_core_role"].get("result", 0)
    detail_count = seg["by_core_role"].get("detail", 0)
    scene_count = seg["by_core_role"].get("scene", 0)
    ending_count = seg["by_core_role"].get("ending", 0)

    # hook gap
    hook_gap = 0
    blocked_hook_gap = 0
    voc_candidates = 0
    voc_confirmed = False
    voc_segment_counts = seg.get("voc_segments") or {}
    voc_usable_segments = int(voc_segment_counts.get("usable") or 0)
    voc_filled_outputs = int(out.get("strict_good_outputs_with_voc_segments") or 0)
    if use_voc_hooks and voc and voc.get("candidates") and voc.get("confirmed", False):
        voc_candidates = len(voc["candidates"])
        voc_confirmed = True
        desired_hook_gap = max(0, min(max_hook, 6) - seg["hook_segments"])
        hook_gap = desired_hook_gap
    else:
        hook_gap = max(0, 3 - hero_count)

    voc_desired_output_quota = _ads_voc_output_quota(target, use_voc_hooks, voc_confirmed, voc_candidates)
    voc_renderable_output_quota = _ads_voc_renderable_quota(
        target,
        use_voc_hooks,
        voc_confirmed,
        voc_candidates,
        voc_usable_segments,
    )
    voc_quota_remaining = max(0, voc_desired_output_quota - voc_filled_outputs)
    voc_segment_gap = _ads_voc_segment_gap(voc_desired_output_quota, voc_filled_outputs, voc_usable_segments)
    if voc_confirmed and voc_segment_gap > 0:
        hook_gap = max(hook_gap, min(max_hook, voc_segment_gap))

    # support gap: need at least 2 of {result, detail, scene} with some depth
    support_gap = 0
    if result_count < 3:
        support_gap += 3 - result_count
    if detail_count < 2:
        support_gap += 2 - detail_count
    if scene_count < 1:
        support_gap += 1 - scene_count
    support_gap = min(support_gap, max_support)
    bottleneck = bottleneck_summary(task, seg, remaining)
    material_extra_capacity = _to_int(bottleneck.get("material_pool_extra_capacity"), 0)
    capacity_exhausted = remaining > 0 and material_extra_capacity <= 0

    # render plan
    planned_renders = 0
    guard_target_count = existing_good
    min_assets = hero_count + result_count + detail_count
    if remaining > 0 and min_assets >= 3 and not capacity_exhausted:
        planned_renders = min(remaining, pass_limit) if pass_limit > 0 else remaining
        guard_target_count = existing_good + planned_renders

    if remaining <= 0:
        status = "ready"
    elif task is None:
        status = "missing_task"
    elif capacity_exhausted:
        status = "needs_prep"
    elif hook_gap > 0 or support_gap > 0:
        status = "needs_prep"
    else:
        status = "can_render"

    plan = {
        "product_id": (task or {}).get("product_id") or product_id,
        "task_type": (task or {}).get("task_type") or "",
        "task_status": (task or {}).get("task_status") or "",
        "existing_good_outputs": existing_good,
        "existing_total_outputs": out["total_outputs"],
        "target_count": target,
        "factory_target_count": target,
        "current_effective_count": existing_good,
        "quantity_goal": update_quantity_goal_progress(quantity_goal, existing_good),
        "remaining_to_target": remaining,
        "remaining_to_factory_target": remaining,
        "asset_pool": {
            "raw_total_segments": seg.get("raw_total", seg["total"]),
            "total_usable_segments": seg["total"],
            "hero": hero_count,
            "result": result_count,
            "detail": detail_count,
            "scene": scene_count,
            "ending": ending_count,
            "hook_segments": seg["hook_segments"],
            "voc_usable_segments": voc_usable_segments,
        },
        "flow_summary": {
            "strict_good_outputs": existing_good,
            "base_good_outputs": out.get("base_good_outputs", existing_good),
            "good_outputs_excluded_by_failed_segments": out.get("good_outputs_excluded_by_failed_segments", 0),
            "target_met": remaining <= 0,
            "voc_participation": voc_participation_summary(use_voc_hooks, voc, voc_gap),
            "voc_output_usage": {
                "voc_total_segment_count": int(voc_segment_counts.get("total") or 0),
                "voc_usable_segment_count": voc_usable_segments,
                "voc_unusable_segment_count": int(voc_segment_counts.get("unusable") or 0),
                "voc_mismatch_suspect_segment_count": int(voc_segment_counts.get("mismatch_suspect") or 0),
                "outputs_with_voc_segments": out.get("outputs_with_voc_segments", 0),
                "voc_rendered_output_count": out.get("voc_rendered_output_count", 0),
                "voc_draft_only_output_count": out.get("voc_draft_only_output_count", 0),
                "voc_failed_segment_output_count": out.get("voc_failed_segment_output_count", 0),
                "base_good_outputs_with_voc_segments": out.get("base_good_outputs_with_voc_segments", 0),
                "strict_good_outputs_with_voc_segments": out.get("strict_good_outputs_with_voc_segments", 0),
                "voc_desired_output_quota": voc_desired_output_quota,
                "voc_renderable_output_quota": voc_renderable_output_quota,
                "voc_quota_filled": voc_filled_outputs,
                "voc_quota_remaining": voc_quota_remaining,
                "voc_segment_gap": voc_segment_gap,
                "voc_quota_status": _voc_quota_status(
                    use_voc_hooks,
                    voc_confirmed,
                    voc_desired_output_quota,
                    voc_renderable_output_quota,
                    voc_filled_outputs,
                    voc_usable_segments,
                ),
            },
            "bottleneck": bottleneck,
        },
        "voc_hook_package": {
            "found": voc is not None,
            "package_id": voc["package_id"] if voc else None,
            "readiness": voc["readiness_status"] if voc else None,
            "confirmed": voc_confirmed,
            "candidate_count": voc_candidates,
            "missing_diagnosis": voc_gap if voc is None else None,
        } if use_voc_hooks else None,
        "gap": {
            "new_hook_segments_planned": hook_gap,
            "new_hook_segments_blocked_by_confirmation": blocked_hook_gap,
            "new_support_segments_planned": support_gap,
        },
        "render": {
            "planned_renders": planned_renders,
            "max_renders_per_pass": pass_limit,
            "guard_target_count": guard_target_count,
            "pass_target_count": guard_target_count,
            "final_target_count": target,
            "factory_target_count": target,
            "remaining_to_factory_target": remaining,
            "available_templates": "AD_FAST_HOOK_8S, AD_FAST_HOOK_10S, AD_FAST_PROOF_12S",
        },
        "status": status,
        "blockers": blockers,
    }

    warnings = []
    if task is None:
        warnings.append("no content task found; create/sync an ADS mixcut task before unattended execution")
    if remaining <= 0:
        plan["note"] = "target already met, no work needed"
    if use_voc_hooks and voc is None:
        if voc_gap and voc_gap.get("missing_reason"):
            warnings.append(f"no VOC hook package found: {voc_gap.get('missing_reason')}; {voc_gap.get('next_action')}")
        else:
            warnings.append("no VOC hook package found; run build_ads_hook_package.py first")
    elif use_voc_hooks and voc and not voc_confirmed:
        warnings.append("VOC hook package exists but is not confirmed; ignoring it and continuing regular ADS_FAST")
    elif use_voc_hooks and voc_confirmed and voc_quota_remaining > 0:
        if voc_usable_segments <= 0:
            warnings.append("VOC hook package confirmed but no usable VOC segments have returned yet; prompt package generation is needed")
        else:
            warnings.append("VOC hook package confirmed but strict good outputs have not met VOC proof quota yet; render planning will prefer VOC proof segments")
    if warnings:
        plan["warnings"] = warnings

    return plan


# ── persist ──

def persist_run(conn, run_id: str, result: Dict[str, Any], args: argparse.Namespace) -> None:
    cur = conn.cursor()
    cur.execute(RUN_TABLE_DDL)
    ts = now_iso()
    plan = result.get("plan") or {}
    gap = plan.get("gap") or {}
    status, current_step = stage_status(result)
    final = result.get("final_inspect") or {}
    cur.execute(
        "INSERT INTO ads_mixcut_unattended_run "
        "(run_id, product_id, target_count, use_voc_hooks, short_ads_mode, "
        "max_new_hook_segments, max_new_support_segments, "
        "status, current_step, existing_output_count, "
        "planned_new_hook_segments, planned_new_support_segments, "
        "rendered_output_count, payload_json, created_at, updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (run_id, args.product_id, _to_int(result.get("target_count"), args.target_count), 1 if args.use_voc_hooks else 0,
         1 if args.short_ads_mode else 0,
         args.max_new_hook_segments, args.max_new_support_segments,
         status, current_step,
         plan.get("existing_good_outputs", 0),
         gap.get("new_hook_segments_planned", 0),
         gap.get("new_support_segments_planned", 0),
         final.get("good_outputs", 0), json.dumps(result, ensure_ascii=False, default=str),
         ts, ts),
    )
    conn.commit()


def sync_content_task_goal(result: Dict[str, Any], args: argparse.Namespace) -> Dict[str, Any]:
    target = _to_int(result.get("target_count"), _to_int(getattr(args, "target_count", 0), 0))
    product_id = str(getattr(args, "product_id", "") or "").strip()
    if not product_id or target <= 0:
        return {"status": "skipped", "reason": "missing_product_or_target"}
    ctx = build_context()
    rows = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC LIMIT 1", (product_id,))
    if not rows:
        return {"status": "skipped", "reason": "content_task_missing"}
    task = rows[0]
    final = result.get("final_inspect") or {}
    quantity_goal = final.get("quantity_goal") or result.get("quantity_goal") or {}
    good_outputs = _to_int(final.get("good_outputs"), 0)
    remaining = _to_int(quantity_goal.get("remaining_to_target"), max(0, target - good_outputs))
    render = result.get("render") or {}
    render_status = str(render.get("status") or "")
    render_error = str(render.get("error") or "")
    ai_supplement = result.get("ai_supplement") or {}
    ai_status = str(ai_supplement.get("status") or "").strip().lower()
    latest_rows = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC LIMIT 1", (product_id,))
    latest_task = latest_rows[0] if latest_rows else task
    bottleneck = (((result.get("plan") or {}).get("flow_summary") or {}).get("bottleneck") or {})
    decision = decide_factory_state(
        latest_task,
        facts={
            "target_count": target,
            "actual_count": good_outputs,
            "remaining_count": remaining,
            "render_status": render_status,
            "ai_status": ai_status,
            "material_pool_extra_capacity": bottleneck.get("material_pool_extra_capacity"),
            "first_slot_remaining_capacity": bottleneck.get("first_slot_remaining_capacity"),
        },
    )
    pipeline_status = decision.pipeline_status
    next_action = decision.next_action
    task_status = decision.task_status
    if decision.is_done:
        last_error = ""
    elif render_status == "failed":
        last_error = render_error or "render_failed"
    elif ai_status in {"blocked", "failed"}:
        last_error = str(latest_task.get("last_error") or ai_supplement.get("reason") or decision.stable_reason)
    else:
        last_error = str(latest_task.get("last_error") or "") if decision.is_blocked or decision.is_error else ""
    patch = {
        "requested_variant_count": target,
        "task_status": task_status,
        "pipeline_status": pipeline_status,
        "next_action": next_action,
        "last_error": last_error,
    }
    write = ctx.repo.update("content_tasks", "task_id", task["task_id"], patch)
    if not write.success:
        return {"status": "failed", "reason": "content_task_update_failed", "error": write.to_dict()}
    capacity = CapacityCounterSkill(ctx).refresh_product(product_id)
    if str(os.environ.get("AUTO_MIXCUT_SKIP_INLINE_FEISHU_SYNC") or "").strip().lower() in {"1", "true", "yes"}:
        sync = {"status": "skipped", "reason": "inline_feishu_sync_disabled"}
    else:
        sync = sync_product_task_best_effort(ctx, product_id)
    return {
        "status": "synced",
        "target_count": target,
        "factory_target_count": target,
        "good_outputs": good_outputs,
        "remaining_to_target": remaining,
        "remaining_to_factory_target": remaining,
        "pipeline_status": pipeline_status,
        "next_action": next_action,
        "capacity": capacity.to_dict(),
        "feishu_sync": sync,
    }


# ── Step 3: prepare VOC hook prompt packages ──

def prepare_voc_hooks(plan: Dict[str, Any], args: argparse.Namespace) -> Dict[str, Any]:
    if not args.use_voc_hooks:
        return {"status": "skipped", "reason": "voc_hooks_disabled"}
    if not args.prepare_voc_hooks:
        return {"status": "not_requested"}
    blockers = list(plan.get("blockers") or [])
    if "content_task_missing" in blockers:
        return {"status": "blocked", "reason": "content_task_missing"}
    voc_info = plan.get("voc_hook_package") or {}
    if not voc_info.get("found"):
        return {"status": "skipped", "reason": "voc_hook_package_missing_optional"}
    if not voc_info.get("confirmed"):
        return {"status": "skipped", "reason": "voc_hook_package_not_confirmed_ignored"}
    planned_hooks = int((plan.get("gap") or {}).get("new_hook_segments_planned") or 0)
    if planned_hooks <= 0:
        return {"status": "skipped", "reason": "no_hook_gap"}

    os.environ.setdefault("AUTO_MIXCUT_DB_PROVIDER", "mysql")
    if args.database_url:
        os.environ["AUTO_MIXCUT_DATABASE_URL"] = args.database_url

    from scripts.sync_prompt_package_workbench_from_tasks import (  # noqa: WPS433
        ANCHOR_QUEUE_URL,
        PRODUCT_TASK_URL,
        PROMPT_WORKBENCH_URL,
        sync_workbench,
    )

    product_task_url = args.product_task_url or PRODUCT_TASK_URL
    anchor_queue_url = args.anchor_queue_url or ANCHOR_QUEUE_URL
    prompt_workbench_url = args.prompt_workbench_url or PROMPT_WORKBENCH_URL
    dry_run = not args.write_prompt_workbench
    result = sync_workbench(
        product_task_url=product_task_url,
        anchor_queue_url=anchor_queue_url,
        prompt_workbench_url=prompt_workbench_url,
        dry_run=dry_run,
        product_id_filter=args.product_id,
        max_packages_per_product=min(max(1, args.max_new_hook_segments), max(1, planned_hooks)),
        refresh_existing_prompts=args.refresh_existing_prompts,
        use_voc_ads_package=True,
        submit_channel=args.submit_channel,
    )
    return {
        "status": "completed" if not result.get("failed") else "failed",
        "dry_run": dry_run,
        "write_prompt_workbench": bool(args.write_prompt_workbench),
        "planned_hook_segments": planned_hooks,
        "created_count": len(result.get("created") or []),
        "skipped_count": len(result.get("skipped") or []),
        "failed_count": len(result.get("failed") or []),
        "result": result,
    }


def prepare_ai_supplement(plan: Dict[str, Any], args: argparse.Namespace) -> Dict[str, Any]:
    remaining = _to_int(plan.get("remaining_to_target"), 0)
    render_plan = plan.get("render") or {}
    planned_renders = _to_int(render_plan.get("planned_renders"), 0)
    if remaining <= 0:
        return {"status": "skipped", "reason": "target_already_met"}
    if planned_renders > 0:
        return {"status": "skipped", "reason": "render_capacity_available", "planned_renders": planned_renders}
    bottleneck = ((plan.get("flow_summary") or {}).get("bottleneck") or {})
    if _to_int(bottleneck.get("material_pool_extra_capacity"), 0) > 0:
        return {"status": "skipped", "reason": "material_capacity_available"}
    gap_text = _ads_ai_supplement_gap_text(plan)
    if not gap_text:
        return {"status": "skipped", "reason": "no_ai_supplement_gap"}
    package_stock_target = _ads_ai_supplement_package_stock_target(plan)
    if not args.write:
        return {"status": "dry_run", "gap_text": gap_text, "package_stock_target": package_stock_target}
    ctx = build_context()
    result = AISupplementWorkbenchSkill(ctx).sync_for_product(
        args.product_id,
        max_packages=package_stock_target or DEFAULT_AI_PACKAGE_STOCK_MIN,
        gap_text=gap_text,
    )
    if not result.success:
        return {"status": "failed", "gap_text": gap_text, "result": result.to_dict()}
    payload = result.data or {}
    status = "created"
    if payload.get("skipped"):
        reason = str(payload.get("reason") or "")
        status = "needs_submit_retry" if reason in {"created_or_recoverable_packages_need_submit", "recoverable_failed_packages_need_retry"} else "blocked"
    return {"status": status, "gap_text": gap_text, "package_stock_target": package_stock_target, "result": result.to_dict()}


def _ads_ai_supplement_gap_text(plan: Dict[str, Any]) -> str:
    remaining = _to_int(plan.get("remaining_to_target"), 0)
    bottleneck = ((plan.get("flow_summary") or {}).get("bottleneck") or {})
    material_extra = _to_int(bottleneck.get("material_pool_extra_capacity"), 0)
    shortfall = max(0, remaining - material_extra)
    if shortfall <= 0:
        return ""
    need = _ads_ai_supplement_package_stock_target(plan)
    text = str(bottleneck.get("current_bottleneck") or bottleneck.get("capacity_note") or "")
    first_slot_capacity = _to_int(bottleneck.get("first_slot_remaining_capacity"), 0)
    hero_priority = "首镜" in text or "first_slot" in text or first_slot_capacity <= 0
    counts = _ads_ai_supplement_role_counts(need, hero_priority=hero_priority)
    labels = {
        "hero": "hero首镜",
        "detail": "detail细节",
        "result": "result上身",
        "scene": "scene场景",
    }
    parts = [f"{labels[role]}{amount}" for role, amount in counts.items() if amount > 0]
    return "AI补素材: " + "; ".join(parts)


def _ads_ai_supplement_package_stock_target(plan: Dict[str, Any]) -> int:
    remaining = _to_int(plan.get("remaining_to_target"), 0)
    bottleneck = ((plan.get("flow_summary") or {}).get("bottleneck") or {})
    material_extra = _to_int(bottleneck.get("material_pool_extra_capacity"), 0)
    shortfall = max(0, remaining - material_extra)
    if shortfall <= 0:
        return 0
    ratio = _float_env("AUTO_MIXCUT_AI_PACKAGE_STOCK_RATIO", DEFAULT_AI_PACKAGE_STOCK_RATIO)
    min_stock = max(1, _to_int(os.environ.get("AUTO_MIXCUT_AI_PACKAGE_STOCK_MIN"), DEFAULT_AI_PACKAGE_STOCK_MIN))
    target = _to_int(plan.get("factory_target_count") or plan.get("target_count"), 0)
    cap = _ads_ai_supplement_package_stock_cap(target)
    return max(1, min(shortfall, cap, max(min_stock, math.ceil(shortfall * ratio))))


def _ads_ai_supplement_package_stock_cap(target_count: int) -> int:
    env_cap = _to_int(os.environ.get("AUTO_MIXCUT_AI_PACKAGE_STOCK_CAP"), 0)
    if env_cap > 0:
        return env_cap
    target_count = _to_int(target_count, 0)
    if target_count <= 20:
        return 8
    if target_count <= 40:
        return 12
    return 16


def _ads_ai_supplement_role_counts(total: int, hero_priority: bool = False) -> Dict[str, int]:
    total = max(1, _to_int(total, 1))
    weights = {"hero": 0.55, "detail": 0.25, "result": 0.15, "scene": 0.05} if hero_priority else {"hero": 0.4, "detail": 0.3, "result": 0.2, "scene": 0.1}
    counts = {"hero": 1, "detail": 0, "result": 0, "scene": 0}
    if total >= 2:
        counts["detail"] = 1
    if total >= 3:
        counts["result"] = 1
    if total >= 6:
        counts["scene"] = 1
    remaining = total - sum(counts.values())
    while remaining > 0:
        role = max(weights, key=lambda item: (weights[item] * total - counts[item], weights[item]))
        counts[role] += 1
        remaining -= 1
    return counts


def _float_env(name: str, default: float) -> float:
    try:
        return max(0.1, float(os.environ.get(name, str(default)) or default))
    except ValueError:
        return default


def submit_hook_packages(plan: Dict[str, Any], args: argparse.Namespace, prompt_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    if not args.submit_hook_packages:
        return {"status": "not_requested"}
    if not direct_submit_enabled(args):
        return {
            "status": "blocked",
            "reason": "direct_submit_disabled",
            "next_action": "submit prompt packages via run_ai_supplement_heartbeat.py / segment-package-worker from Feishu workbench",
        }
    if not args.write:
        return {"status": "blocked", "reason": "submit_requires_write"}
    if args.submit_channel == "imini" and not args.allow_imini_real_submit:
        return {"status": "blocked", "reason": "imini_real_submit_requires_explicit_allow"}
    planned_hooks = int((plan.get("gap") or {}).get("new_hook_segments_planned") or 0)
    limit = int(args.submit_limit or planned_hooks or args.max_new_hook_segments or 1)
    channel = submit_channel_label(args.submit_channel)
    env = {}
    if args.submit_channel == "imini":
        env["IMINI_ALLOW_REAL_SUBMIT"] = "1"
    scoped_ids = [item for item in (prompt_ids or []) if item]
    if scoped_ids:
        results = []
        for prompt_id in scoped_ids[: max(1, limit)]:
            cmd = [
                "node",
                "segment-package-worker.js",
                "--config",
                SEGMENT_PACKAGE_CONFIG,
                "--submit-only",
                f"--product-id={args.product_id}",
                f"--record-id={prompt_id}",
                "--limit=1",
                f"--channel={channel}",
            ]
            results.append(command_result(cmd, JIMENG_SKILL_ROOT, env=env, timeout_minutes=args.submit_timeout_minutes))
        failed = [item for item in results if item.get("status") != "completed"]
        return {
            "status": "failed" if failed else "completed",
            "submit_channel": args.submit_channel,
            "scope": "prompt_ids",
            "prompt_ids": scoped_ids[: max(1, limit)],
            "success_count": len(results) - len(failed),
            "failed_count": len(failed),
            "results": results,
        }

    cmd = [
        "node",
        "segment-package-worker.js",
        "--config",
        SEGMENT_PACKAGE_CONFIG,
        "--submit-only",
        f"--product-id={args.product_id}",
        f"--limit={max(1, limit)}",
        f"--channel={channel}",
    ]
    result = command_result(cmd, JIMENG_SKILL_ROOT, env=env, timeout_minutes=args.submit_timeout_minutes)
    return {
        **result,
        "submit_channel": args.submit_channel,
        "scope": "product",
        "planned_limit": max(1, limit),
    }


def direct_submit_enabled(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "allow_direct_submit", False) or factory_config().allow_direct_submit)


def import_returns(args: argparse.Namespace, dry_run: bool = False, force: bool = False) -> Dict[str, Any]:
    if not args.import_returns and not dry_run and not force:
        return {"status": "not_requested"}
    cmd = [
        sys.executable,
        "scripts/import_prompt_package_returns.py",
        "--product-id",
        args.product_id,
    ]
    if dry_run or not args.write:
        cmd.append("--dry-run")
    return command_result(
        cmd,
        SKILL_ROOT,
        env={"AUTO_MIXCUT_DB_PROVIDER": "mysql", "AUTO_MIXCUT_OSS_PROVIDER": "aliyun"},
        timeout_minutes=args.import_timeout_minutes,
    )


def wait_for_returns(conn, args: argparse.Namespace, prompt_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    if not args.wait_returns:
        return {"status": "not_requested"}
    if not args.write:
        return {"status": "blocked", "reason": "wait_returns_requires_write"}
    scoped_ids = [item for item in (prompt_ids or []) if item]
    if not scoped_ids:
        return {"status": "skipped", "reason": "no_prompt_ids_to_wait_for"}
    deadline = time.monotonic() + max(0, int(args.wait_timeout_minutes or 0)) * 60
    poll_seconds = max(5, int(float(args.poll_interval_minutes or 1) * 60))
    attempts: List[Dict[str, Any]] = []
    scoped_ids = [item for item in (prompt_ids or []) if item]
    last_summary = load_prompt_package_summary(conn, args.product_id, scoped_ids or None)

    while True:
        upload_cmd = [
            "node",
            "result-uploader.js",
            "--config",
            SEGMENT_PACKAGE_CONFIG,
            "--channel",
            args.submit_channel,
            "--ignore-generating-count",
            "--limit",
            str(max(1, int(args.return_scan_limit or 10))),
        ]
        upload = command_result(upload_cmd, JIMENG_SKILL_ROOT, timeout_minutes=args.return_scan_timeout_minutes)
        imported = import_returns(args, dry_run=False, force=True)
        last_summary = load_prompt_package_summary(conn, args.product_id, scoped_ids or None)
        attempts.append({
            "attempt": len(attempts) + 1,
            "upload": upload,
            "import": imported,
            "prompt_packages": last_summary,
        })

        if last_summary["pending"] <= 0:
            if scoped_ids and last_summary["failed"] > 0:
                return {
                    "status": "failed",
                    "attempt_count": len(attempts),
                    "scope": "prompt_ids",
                    "prompt_ids": scoped_ids,
                    "prompt_packages": last_summary,
                    "attempts": attempts,
                    "reason": "scoped_prompt_package_failed",
                }
            return {
                "status": "completed",
                "attempt_count": len(attempts),
                "scope": "prompt_ids" if scoped_ids else "product",
                "prompt_ids": scoped_ids,
                "prompt_packages": last_summary,
                "attempts": attempts,
            }
        if time.monotonic() >= deadline:
            return {
                "status": "timeout",
                "attempt_count": len(attempts),
                "scope": "prompt_ids" if scoped_ids else "product",
                "prompt_ids": scoped_ids,
                "prompt_packages": last_summary,
                "attempts": attempts,
                "reason": "return_timeout",
            }
        time.sleep(poll_seconds)


def run_segment_qc(args: argparse.Namespace) -> Dict[str, Any]:
    if not args.run_segment_qc:
        return {"status": "not_requested"}
    if not args.write:
        return {"status": "blocked", "reason": "segment_qc_requires_write"}
    os.environ.setdefault("AUTO_MIXCUT_DB_PROVIDER", "mysql")
    if args.database_url:
        os.environ["AUTO_MIXCUT_DATABASE_URL"] = args.database_url
    from auto_mixcut.core.bootstrap import build_context  # noqa: WPS433
    from auto_mixcut.skills.ai_generation_qc_skill import _basic_qc  # noqa: WPS433
    from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill  # noqa: WPS433

    trace_step("segment_qc_context_start", product_id=args.product_id)
    ctx = build_context()
    trace_step("segment_qc_context_done", product_id=args.product_id)
    if not _env_truthy("AUTO_MIXCUT_ADS_FAST_MODE") and not _env_truthy("AUTO_MIXCUT_SKIP_ADS_SEGMENT_QC_INIT_DB"):
        trace_step("segment_qc_init_db_start", product_id=args.product_id)
        with _operation_timeout(_ads_inspect_timeout_seconds()):
            init = RDSRepositorySkill(ctx).init_db()
        trace_step("segment_qc_init_db_done", product_id=args.product_id, success=init.success)
        if not init.success:
            return {"status": "failed", "reason": "rds_init_failed", "result": init.to_dict()}
    else:
        trace_step("segment_qc_init_db_skipped", product_id=args.product_id)
    trace_step("segment_qc_list_start", product_id=args.product_id)
    segments = ctx.repo.list_where("segments", "product_id=? AND source_type='ai_generated'", (args.product_id,))
    trace_step("segment_qc_list_done", product_id=args.product_id, segments=len(segments))
    checked = passed = failed = skipped = 0
    results = []
    skip_statuses = {"qc_passed", "qc_failed", "frame_sample_failed", "frame_sample_timeout", "fingerprint_failed", "tag_failed"}
    for segment in segments:
        segment_id = str(segment.get("segment_id") or "")
        status = str(segment.get("segment_status") or "")
        if status in skip_statuses or status in FAILED_SEGMENT_STATUSES:
            skipped += 1
            results.append({"segment_id": segment_id, "skipped": True, "reason": status or "already_checked"})
            continue
        ok, issues = _basic_qc(segment)
        next_status = "qc_passed" if ok else "qc_failed"
        ctx.repo.update("segments", "segment_id", segment_id, {"segment_status": next_status})
        checked += 1
        passed += 1 if ok else 0
        failed += 0 if ok else 1
        results.append({"segment_id": segment_id, "segment_status": next_status, "issues": issues})
    return {
        "status": "completed",
        "checked": checked,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "results": results[:50],
    }


def run_render_guard(args: argparse.Namespace, plan: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not args.render:
        return {"status": "not_requested"}
    if not args.write:
        return {"status": "blocked", "reason": "render_requires_write"}
    render_plan = (plan or {}).get("render") or {}
    if _to_int(render_plan.get("planned_renders"), 0) <= 0:
        return {"status": "skipped", "reason": "no_planned_renders", "plan_status": (plan or {}).get("status")}
    guard_target = _to_int(render_plan.get("guard_target_count"), _to_int(getattr(args, "target_count", 0), 0))
    final_target = _to_int(render_plan.get("final_target_count"), _to_int(getattr(args, "target_count", 0), 0))
    cmd = [
        sys.executable,
        "scripts/run_mixcut_guard.py",
        "--product-id",
        args.product_id,
        "--target",
        str(guard_target),
        "--max-rounds",
        str(max(1, int(args.guard_max_rounds or 3))),
        "--template-id",
        ADS_FAST_TEMPLATE_ID,
        "--skip-upload-sync",
    ]
    env = {
        "AUTO_MIXCUT_DB_PROVIDER": "mysql",
        "AUTO_MIXCUT_OSS_PROVIDER": "aliyun",
        "AUTO_MIXCUT_ADS_FAST_MODE": "1",
        "AUTO_MIXCUT_SKIP_INLINE_FEISHU_SYNC": "1",
        "AUTO_MIXCUT_SKIP_GUARD_INIT_DB": "1",
        "AUTO_MIXCUT_SKIP_RENDER_RUNTIME_SCHEMA": "1",
        "AUTO_MIXCUT_STALE_PLANNING_BATCH_MINUTES": "10",
        "AUTO_MIXCUT_RENDER_BATCH_TIMEOUT": "720",
        "AUTO_MIXCUT_FFMPEG_TIMEOUT_SEC": "240",
        "AUTO_MIXCUT_DB_READ_TIMEOUT": "15",
        "AUTO_MIXCUT_DB_WRITE_TIMEOUT": "15",
        "AUTO_MIXCUT_TOP_UP_SKIP_CAPACITY_REFRESH": "1",
    }
    if _env_truthy("AUTO_MIXCUT_RENDER_GUARD_SUBPROCESS"):
        result = command_result(
            cmd,
            SKILL_ROOT,
            env=env,
            timeout_minutes=args.render_timeout_minutes,
        )
    else:
        result = run_render_guard_inline(
            args,
            guard_target=guard_target,
            env=env,
            started_at=now_iso(),
        )
    result["guard_target_count"] = guard_target
    result["pass_target_count"] = guard_target
    result["final_target_count"] = final_target
    result["factory_target_count"] = final_target
    result["planned_renders"] = _to_int(render_plan.get("planned_renders"), 0)
    result["max_renders_per_pass"] = _to_int(render_plan.get("max_renders_per_pass"), 0)
    return result


def run_render_guard_inline(
    args: argparse.Namespace,
    guard_target: int,
    env: Dict[str, str],
    started_at: str,
) -> Dict[str, Any]:
    old_env = {key: os.environ.get(key) for key in env}
    os.environ.update(env)
    try:
        from scripts.run_mixcut_guard import run_guard_pass  # noqa: WPS433

        trace_step("inline_guard_build_context_start", product_id=args.product_id, guard_target=guard_target)
        ctx = build_context()
        trace_step("inline_guard_build_context_done", product_id=args.product_id, guard_target=guard_target)
        with _operation_timeout(max(1, int(getattr(args, "render_timeout_minutes", 1) or 1)) * 60):
            trace_step("inline_guard_run_start", product_id=args.product_id, guard_target=guard_target)
            res = run_guard_pass(
                ctx,
                product_id=args.product_id,
                target=guard_target,
                max_rounds=max(1, int(args.guard_max_rounds or 3)),
                template_id=ADS_FAST_TEMPLATE_ID,
                process_uploads=False,
            )
            trace_step("inline_guard_run_done", product_id=args.product_id, guard_target=guard_target, success=res.success)
        payload = res.to_dict()
        return {
            "status": "completed" if res.success else "failed",
            "returncode": 0 if res.success else 1,
            "cmd": ["inline", "run_guard_pass"],
            "cwd": SKILL_ROOT,
            "started_at": started_at,
            "finished_at": now_iso(),
            "stdout_tail": json.dumps(payload, ensure_ascii=False, default=str)[-4000:],
            "stderr_tail": "",
        }
    except TimeoutError as exc:
        return {
            "status": "failed",
            "returncode": None,
            "cmd": ["inline", "run_guard_pass"],
            "cwd": SKILL_ROOT,
            "started_at": started_at,
            "finished_at": now_iso(),
            "error": str(exc),
            "stdout_tail": "",
            "stderr_tail": "",
        }
    except Exception as exc:
        return {
            "status": "failed",
            "returncode": None,
            "cmd": ["inline", "run_guard_pass"],
            "cwd": SKILL_ROOT,
            "started_at": started_at,
            "finished_at": now_iso(),
            "error": str(exc),
            "stdout_tail": "",
            "stderr_tail": "",
        }
    finally:
        for key, value in old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@contextmanager
def _operation_timeout(seconds: int):
    if not hasattr(signal, "SIGALRM"):
        yield
        return
    previous_handler = signal.getsignal(signal.SIGALRM)

    def _raise_timeout(signum, frame):
        raise TimeoutError(f"operation timed out after {seconds}s")

    signal.signal(signal.SIGALRM, _raise_timeout)
    signal.alarm(max(1, int(seconds)))
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


def run_final_qc(args: argparse.Namespace, render_result: Dict[str, Any]) -> Dict[str, Any]:
    if not args.run_final_qc:
        return {"status": "not_requested"}
    if render_result and render_result.get("status") == "completed":
        return {"status": "completed", "reason": "covered_by_run_mixcut_guard"}
    return {"status": "skipped", "reason": "render_not_completed"}


# ── main ──

def run(args: argparse.Namespace) -> Dict[str, Any]:
    trace_step("run_start", product_id=args.product_id, target_count=args.target_count, full_run=bool(getattr(args, "full_run", False)))
    trace_step("connect_db_start", product_id=args.product_id)
    conn = connect_db(args.database_url)
    trace_step("connect_db_done", product_id=args.product_id)
    try:
        # step 1: inspect
        trace_step("inspect_start", product_id=args.product_id)
        with _operation_timeout(_ads_inspect_timeout_seconds()):
            task = load_task(conn, args.product_id)
            seg = load_segment_summary(conn, args.product_id)
            out = load_output_summary(conn, args.product_id)
            voc = load_voc_hook_package(conn, args.product_id) if args.use_voc_hooks else None
            voc_gap = diagnose_voc_gap(conn, args.product_id) if args.use_voc_hooks and voc is None else {}
        trace_step("inspect_done", product_id=args.product_id, segments=seg.get("total"), good_outputs=out.get("good_outputs"), voc=bool(voc))
        quantity_goal = build_quantity_goal(
            out,
            args.target_count,
            getattr(args, "goal_mode", GOAL_MODE_ABSOLUTE_TARGET),
            getattr(args, "add_count", 0),
            getattr(args, "factory_tier", 0),
        )

        # step 2: plan
        trace_step("plan_start", product_id=args.product_id)
        plan = plan_ads_mixcut(
            args.product_id, task, seg, out, voc,
            voc_gap,
            args.target_count, args.use_voc_hooks,
            args.max_new_hook_segments, args.max_new_support_segments,
            quantity_goal=quantity_goal,
            max_renders_per_pass=args.max_renders_per_pass,
        )
        trace_step("plan_done", product_id=args.product_id, status=plan.get("status"), remaining=plan.get("remaining_to_target"), planned_renders=(plan.get("render") or {}).get("planned_renders"))

        run_id = "ads_unattended__{}__{}".format(args.product_id, now_iso().replace(":", "").replace("-", ""))
        result = {
            "run_id": run_id,
            "dry_run": not args.write,
            "product_id": args.product_id,
            "goal_mode": quantity_goal["goal_mode"],
            "requested_target_count": quantity_goal["requested_target_count"],
            "target_count": quantity_goal["target_strict_good_count"],
            "factory_target_count": quantity_goal["target_strict_good_count"],
            "quantity_goal": quantity_goal,
            "use_voc_hooks": args.use_voc_hooks,
            "inspect": {
                "task_exists": task is not None,
                "task_type": (task or {}).get("task_type") or "",
                "segment_summary": {k: v for k, v in seg.items() if k != "segments"},
                "output_summary": out,
                "voc_hook_package_found": voc is not None,
                "voc_hook_package_gap": voc_gap or None,
            },
            "plan": plan,
        }

        # early exit: target already met
        if plan.get("remaining_to_target", 0) <= 0:
            result["skipped_full_run"] = True
            result["skip_reason"] = "target_already_met"
            if args.write:
                persist_run(conn, run_id, result, args)
            return result

        if args.prepare_voc_hooks:
            trace_step("prepare_voc_hooks_start", product_id=args.product_id)
            result["prepare_voc_hooks"] = prepare_voc_hooks(plan, args)
            trace_step("prepare_voc_hooks_done", product_id=args.product_id, status=(result["prepare_voc_hooks"] or {}).get("status"))

        scoped_prompt_ids = prepared_prompt_ids(result)
        ai_supplement_needed = (
            _to_int(plan.get("remaining_to_target"), 0) > 0
            and _to_int((plan.get("render") or {}).get("planned_renders"), 0) <= 0
            and str(plan.get("status") or "") == "needs_prep"
            and not scoped_prompt_ids
        )
        if ai_supplement_needed:
            trace_step("ai_supplement_start", product_id=args.product_id)
            result["ai_supplement"] = prepare_ai_supplement(plan, args)
            trace_step("ai_supplement_done", product_id=args.product_id, status=(result["ai_supplement"] or {}).get("status"))

        if scoped_prompt_ids:
            result["scoped_prompt_ids"] = scoped_prompt_ids
            if not args.submit_hook_packages:
                result["prompt_package_submit_handoff"] = {
                    "status": "deferred_to_worker",
                    "reason": "direct_submit_disabled",
                    "prompt_ids": scoped_prompt_ids,
                    "next_action": "run_ai_supplement_heartbeat.py or the prompt-package worker will submit ready Feishu rows",
                }

        if args.submit_hook_packages:
            hook_gap = (plan.get("gap") or {}).get("new_hook_segments_planned", 0)
            if hook_gap <= 0 or not scoped_prompt_ids:
                result["submit_hook_packages"] = {"status": "skipped", "reason": "no_hook_gap" if hook_gap <= 0 else "no_prompt_ids"}
            else:
                result["submit_hook_packages"] = submit_hook_packages(plan, args, scoped_prompt_ids)

        if args.import_returns:
            trace_step("import_returns_start", product_id=args.product_id)
            result["import_returns"] = import_returns(args)
            trace_step("import_returns_done", product_id=args.product_id, status=(result["import_returns"] or {}).get("status"))

        if args.wait_returns:
            result["wait_returns"] = wait_for_returns(conn, args, scoped_prompt_ids)

        if args.run_segment_qc:
            trace_step("segment_qc_start", product_id=args.product_id)
            result["segment_qc"] = run_segment_qc(args)
            trace_step("segment_qc_done", product_id=args.product_id, status=(result["segment_qc"] or {}).get("status"))

        render_result: Dict[str, Any] = {}
        if args.render:
            trace_step("render_guard_start", product_id=args.product_id)
            render_result = run_render_guard(args, plan)
            result["render"] = render_result
            trace_step("render_guard_done", product_id=args.product_id, status=render_result.get("status"), error=render_result.get("error"))

        if args.run_final_qc:
            trace_step("final_qc_start", product_id=args.product_id)
            result["final_qc"] = run_final_qc(args, render_result)
            trace_step("final_qc_done", product_id=args.product_id, status=(result["final_qc"] or {}).get("status"))

        trace_step("final_inspect_start", product_id=args.product_id)
        with _operation_timeout(_ads_inspect_timeout_seconds()):
            final_out = load_output_summary(conn, args.product_id)
            final_seg = load_segment_summary(conn, args.product_id)
            final_packages = load_prompt_package_summary(conn, args.product_id)
        trace_step("final_inspect_done", product_id=args.product_id, good_outputs=final_out.get("good_outputs"), segments=final_seg.get("total"))
        final_quantity_goal = update_quantity_goal_progress(quantity_goal, final_out["good_outputs"])
        result["final_inspect"] = {
            "good_outputs": final_out["good_outputs"],
            "total_outputs": final_out["total_outputs"],
            "remaining_to_target": final_quantity_goal["remaining_to_target"],
            "quantity_goal": final_quantity_goal,
            "usable_segments": final_seg["total"],
            "prompt_packages": final_packages,
        }

        if args.write:
            trace_step("task_goal_sync_start", product_id=args.product_id)
            result["task_goal_sync"] = sync_content_task_goal(result, args)
            trace_step("task_goal_sync_done", product_id=args.product_id, status=(result["task_goal_sync"] or {}).get("status"))
            persist_run(conn, run_id, result, args)
            result["written"] = True

        trace_step("run_done", product_id=args.product_id, status=stage_status(result)[0])
        return result
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _ads_inspect_timeout_seconds() -> int:
    try:
        return max(10, int(os.environ.get("AUTO_MIXCUT_ADS_INSPECT_TIMEOUT", "120") or "120"))
    except ValueError:
        return 120


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="ADS 无人值守混剪编排")
    p.add_argument("--product-id", required=True)
    p.add_argument("--target-count", type=int, default=30, help="absolute target by default; with --goal-mode incremental_add it means new outputs to add")
    p.add_argument("--goal-mode", choices=sorted(GOAL_MODES), default=GOAL_MODE_ABSOLUTE_TARGET)
    p.add_argument("--add-count", type=int, default=0, help="shortcut for --goal-mode incremental_add --target-count N")
    p.add_argument("--factory-tier", type=int, choices=sorted(FACTORY_TIERS), default=0, help="shortcut for --goal-mode factory_tier using 20/40/60/80")
    p.add_argument("--max-renders-per-pass", type=int, default=DEFAULT_MAX_RENDERS_PER_PASS, help="cap one unattended render pass; 0 means no cap")
    p.add_argument("--use-voc-hooks", action="store_true", default=True)
    p.add_argument("--no-voc-hooks", dest="use_voc_hooks", action="store_false")
    p.add_argument("--max-new-hook-segments", type=int, default=6)
    p.add_argument("--max-new-support-segments", type=int, default=12)
    p.add_argument("--short-ads-mode", action="store_true", default=True)
    p.add_argument("--prepare-voc-hooks", action="store_true", help="dry-run or write VOC hook prompt packages into prompt workbench")
    p.add_argument("--write-prompt-workbench", action="store_true", help="actually create/refresh prompt workbench rows; requires --prepare-voc-hooks and --write")
    p.add_argument("--refresh-existing-prompts", action="store_true")
    p.add_argument("--submit-hook-packages", action="store_true", help="submit ready prompt package rows to the selected generation channel")
    p.add_argument("--submit-channel", choices=["jimeng", "imini"], default="jimeng")
    p.add_argument("--submit-limit", type=int, default=0)
    p.add_argument("--submit-timeout-minutes", type=int, default=90)
    p.add_argument("--allow-imini-real-submit", action="store_true")
    p.add_argument("--allow-direct-submit", action="store_true", help="escape hatch: let this ADS script directly call segment-package-worker")
    p.add_argument("--import-returns", action="store_true", help="sync prompt package submission/return status from Feishu into RDS")
    p.add_argument("--import-timeout-minutes", type=int, default=20)
    p.add_argument("--wait-returns", action="store_true", help="poll generation result uploader and import returns until pending prompt packages clear or timeout")
    p.add_argument("--wait-timeout-minutes", type=int, default=90)
    p.add_argument("--poll-interval-minutes", type=float, default=5)
    p.add_argument("--return-scan-limit", type=int, default=10)
    p.add_argument("--return-scan-timeout-minutes", type=int, default=45)
    p.add_argument("--run-segment-qc", action="store_true")
    p.add_argument("--render", action="store_true")
    p.add_argument("--guard-max-rounds", type=int, default=2)
    p.add_argument("--render-timeout-minutes", type=int, default=120)
    p.add_argument("--run-final-qc", action="store_true")
    p.add_argument("--full-run", action="store_true", help="prepare prompt packages, import existing returns, QC, render, and final QC in one guarded run")
    p.add_argument("--product-task-url", default=None)
    p.add_argument("--anchor-queue-url", default=None)
    p.add_argument("--prompt-workbench-url", default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--write", action="store_true", help="写入 run 表")
    p.add_argument("--json", dest="pretty", action="store_false", default=True)
    p.add_argument("--database-url", default=None)
    p.set_defaults(use_voc_hooks=True, short_ads_mode=True)
    return p


def result_has_failed_step(value: Any) -> bool:
    if isinstance(value, dict):
        status = str(value.get("status") or "").strip().lower()
        if status in {"failed", "blocked", "timeout"}:
            return True
        return any(result_has_failed_step(item) for item in value.values())
    if isinstance(value, list):
        return any(result_has_failed_step(item) for item in value)
    return False


def result_target_met(result: Dict[str, Any]) -> bool:
    final = result.get("final_inspect") or {}
    if final:
        return _to_int(final.get("remaining_to_target"), 1) <= 0
    plan = result.get("plan") or {}
    return _to_int(plan.get("remaining_to_target"), 1) <= 0


def apply_full_run_defaults(args: argparse.Namespace) -> argparse.Namespace:
    if not args.full_run:
        return args
    args.prepare_voc_hooks = True
    args.write_prompt_workbench = True
    args.import_returns = True
    if direct_submit_enabled(args):
        args.submit_hook_packages = True
        args.wait_returns = True
    else:
        args.submit_hook_packages = False
        args.wait_returns = False
    args.run_segment_qc = True
    args.render = True
    args.run_final_qc = True
    return args


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    normalize_goal_args(args)
    apply_full_run_defaults(args)
    if args.dry_run and args.write:
        print("ERROR: --dry-run and --write are mutually exclusive", file=sys.stderr)
        return 2
    if args.full_run and not args.write:
        print("ERROR: --full-run requires --write", file=sys.stderr)
        return 2
    if args.write_prompt_workbench and not args.prepare_voc_hooks:
        print("ERROR: --write-prompt-workbench requires --prepare-voc-hooks", file=sys.stderr)
        return 2
    if args.write_prompt_workbench and not args.write:
        print("ERROR: --write-prompt-workbench requires --write", file=sys.stderr)
        return 2
    try:
        result = run(args)
    except Exception as e:
        print("ERROR: {}".format(e), file=sys.stderr)
        return 1
    indent = 2 if args.pretty else None
    print(json.dumps(result, ensure_ascii=False, indent=indent, default=str))
    return 0 if result_target_met(result) else 1 if result_has_failed_step(result) else 0


if __name__ == "__main__":
    raise SystemExit(main())
