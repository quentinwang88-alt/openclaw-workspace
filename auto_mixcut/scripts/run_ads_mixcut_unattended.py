#!/usr/bin/env python3
"""ADS 无人值守混剪编排脚本 — V1 dry-run / plan only.

扫描产品当前素材池和成片，生成补充计划，不实际提交或渲染。

Usage:
  python3 scripts/run_ads_mixcut_unattended.py \\
    --product-id 1729659517276948599 --target-count 30 --use-voc-hooks --dry-run --json

正式执行才允许 --write（V1 先不做，dry-run 只出 plan）。
"""
from __future__ import annotations

import argparse
import json
import os
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
    "submit_hook_packages",
    "import_returns",
    "wait_returns",
    "segment_qc",
    "render",
    "final_qc",
]
PROMPT_PENDING_STATUSES = {"submitted", "generating", "已提单", "生成中"}
PROMPT_IMPORTED_STATUSES = {"imported", "consumed", "fulfilled", "质检中", "质检通过", "returned", "已回流", "已生成"}
PROMPT_FAILED_STATUSES = {"failed", "质检废弃", "失败"}
RECOVERABLE_PROMPT_FAILURE_TOKENS = [
    "高峰期",
    "暂时无法提交更多任务",
    "无法提交更多任务",
    "请等待其他任务完成",
    "platform_limited",
    "retry_pending",
    "队列已满",
    "提示词输入失败",
    "prompt_input_failed",
]


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
        connect_timeout=20, read_timeout=60, write_timeout=60, autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
            status = "incomplete"
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
    cur.execute(
        "SELECT s.segment_id, s.effective_roles_json, s.slot_role, s.is_image_generated, s.prompt_package_id, "
        "s.segment_type, s.segment_status, s.source_type, t.primary_shot_role, t.hook_visual_type, t.hook_strength "
        "FROM segments s "
        "LEFT JOIN segment_tags t ON t.id = ("
        "  SELECT MAX(t2.id) FROM segment_tags t2 WHERE t2.segment_id=s.segment_id"
        ") "
        "WHERE s.product_id=%s",
        (product_id,),
    )
    rows = cur.fetchall()
    by_role: Dict[str, int] = Counter()
    by_core_role: Dict[str, int] = Counter()
    by_status: Dict[str, int] = Counter()
    hook_count = 0
    segments: List[Dict] = []
    for r in rows:
        status = str(r.get("segment_status") or "").strip()
        source_type = str(r.get("source_type") or "").strip()
        by_status[status or "unknown"] += 1
        if status in FAILED_SEGMENT_STATUSES:
            continue
        if source_type == "ai_generated" and status != "qc_passed":
            continue
        roles = jload(r.get("effective_roles_json")) or []
        if not isinstance(roles, list):
            roles = []
        role_set = {str(role) for role in roles if str(role or "").strip()}
        slot = r.get("slot_role") or ""
        hook_type = r.get("hook_visual_type") or "none"
        primary = r.get("primary_shot_role") or slot
        if primary:
            role_set.add(str(primary))
        is_hook = (
            "hero" in role_set
            and hook_type
            and hook_type not in ("none", "")
            and (r.get("hook_strength") or "") in {"strong", "medium"}
        )
        for role in role_set:
            by_role[role] += 1
            if role in CORE_ROLES:
                by_core_role[role] += 1
        if is_hook:
            hook_count += 1
        segments.append({
            "segment_id": r.get("segment_id"),
            "effective_roles": sorted(role_set),
            "primary_shot_role": primary,
            "segment_status": status,
            "source_type": source_type,
            "is_hook": is_hook,
            "hook_visual_type": hook_type,
            "hook_strength": r.get("hook_strength"),
            "is_image_generated": bool(r.get("is_image_generated")),
        })
    return {
        "raw_total": len(rows),
        "total": len(segments),
        "by_status": dict(by_status),
        "by_role": dict(by_role),
        "by_core_role": dict(by_core_role),
        "hook_segments": hook_count,
        "segments": segments,
    }


def load_output_summary(conn, product_id: str) -> Dict[str, Any]:
    cur = conn.cursor()
    good_statuses = sorted(GOOD_MACHINE_OUTPUT_STATUSES)
    rejected_statuses = sorted(REJECTED_HUMAN_OUTPUT_STATUSES)
    failed_statuses = sorted(FAILED_SEGMENT_STATUSES)
    failed_placeholders = ",".join(["%s"] * len(failed_statuses))
    cur.execute(
        "SELECT o.output_id, o.render_status, o.machine_quality_status, o.human_quality_status, o.duration_ms, "
        f"SUM(CASE WHEN s.segment_status IN ({failed_placeholders}) THEN 1 ELSE 0 END) failed_segment_count, "
        "SUM(CASE WHEN s.segment_id IS NULL AND os.segment_id IS NOT NULL THEN 1 ELSE 0 END) missing_segment_count, "
        "SUM(CASE WHEN p.template_id='VOC_ADS_HOOK_PACKAGE' THEN 1 ELSE 0 END) voc_segment_count "
        "FROM outputs o "
        "LEFT JOIN output_segments os ON os.output_id=o.output_id "
        "LEFT JOIN segments s ON s.segment_id=os.segment_id "
        "LEFT JOIN segment_prompt_packages p ON p.segment_prompt_id=s.prompt_package_id "
        "WHERE o.product_id=%s "
        "GROUP BY o.output_id, o.render_status, o.machine_quality_status, o.human_quality_status, o.duration_ms",
        (*failed_statuses, product_id),
    )
    rows = cur.fetchall()
    good_set = set(good_statuses)
    rejected_set = set(rejected_statuses)
    total = len(rows)
    rendered = 0
    base_good = 0
    strict_good = 0
    outputs_with_failed_segments = 0
    outputs_missing_segments = 0
    good_excluded_by_failed_segments = 0
    outputs_with_voc_segments = 0
    base_good_outputs_with_voc_segments = 0
    strict_good_outputs_with_voc_segments = 0
    strict_durations: List[int] = []
    for row in rows:
        is_rendered = row.get("render_status") == "rendered"
        if is_rendered:
            rendered += 1
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
        if is_base_good:
            base_good += 1
            if voc_count > 0:
                base_good_outputs_with_voc_segments += 1
            if has_segment_issue:
                good_excluded_by_failed_segments += 1
            else:
                strict_good += 1
                if voc_count > 0:
                    strict_good_outputs_with_voc_segments += 1
                strict_durations.append(int(row.get("duration_ms") or 0))
    return {
        "total_outputs": total,
        "good_outputs": strict_good,
        "base_good_outputs": base_good,
        "rendered_outputs": rendered,
        "good_outputs_excluded_by_failed_segments": good_excluded_by_failed_segments,
        "outputs_with_failed_segments": outputs_with_failed_segments,
        "outputs_missing_segments": outputs_missing_segments,
        "outputs_with_voc_segments": outputs_with_voc_segments,
        "good_outputs_with_voc_segments": strict_good_outputs_with_voc_segments,
        "base_good_outputs_with_voc_segments": base_good_outputs_with_voc_segments,
        "strict_good_outputs_with_voc_segments": strict_good_outputs_with_voc_segments,
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
            "SELECT segment_prompt_id, package_status, external_provider, external_job_id, "
            "generated_asset_id, generated_segment_id, failure_reason "
            f"FROM segment_prompt_packages WHERE product_id=%s AND segment_prompt_id IN ({placeholders})",
            (product_id, *prompt_ids),
        )
    else:
        cur.execute(
            "SELECT segment_prompt_id, package_status, external_provider, external_job_id, "
            "generated_asset_id, generated_segment_id, failure_reason "
            "FROM segment_prompt_packages WHERE product_id=%s",
            (product_id,),
        )
    rows = cur.fetchall()
    by_status: Dict[str, int] = Counter()
    pending = imported = failed = recoverable_failed = submitted = generated = consumed = 0
    for row in rows:
        status = str(row.get("package_status") or "").strip()
        failure_reason = str(row.get("failure_reason") or "").strip()
        by_status[status or "unknown"] += 1
        if status in PROMPT_PENDING_STATUSES:
            pending += 1
        if status == "submitted":
            submitted += 1
        if status in PROMPT_FAILED_STATUSES:
            if _recoverable_prompt_failure(failure_reason):
                recoverable_failed += 1
            else:
                failed += 1
        if status in PROMPT_IMPORTED_STATUSES:
            imported += 1
        if status == "consumed":
            consumed += 1
        if row.get("generated_asset_id") or row.get("generated_segment_id"):
            generated += 1
    return {
        "total": len(rows),
        "pending": pending,
        "submitted": submitted,
        "imported": imported,
        "generated": generated,
        "consumed": consumed,
        "failed": failed,
        "recoverable_failed": recoverable_failed,
        "by_status": dict(by_status),
    }


def _recoverable_prompt_failure(text: str) -> bool:
    lower = str(text or "").lower()
    return any(str(token).lower() in lower for token in RECOVERABLE_PROMPT_FAILURE_TOKENS)


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
            "mode": "voc_blocked",
            "participates": False,
            "package_id": voc.get("package_id"),
            "readiness": voc.get("readiness_status"),
            "manual_confirmation_status": voc.get("manual_confirmation_status"),
            "reason": "voc_package_not_confirmed",
            "next_action": "confirm one or more VOC selling points before ADS hook package consumption",
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
) -> Dict[str, Any]:
    existing_good = out["good_outputs"]
    remaining = max(0, target - existing_good)
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
    if use_voc_hooks and voc and voc.get("candidates"):
        voc_candidates = len(voc["candidates"])
        voc_confirmed = voc.get("confirmed", False)
        desired_hook_gap = max(0, min(max_hook, 6) - seg["hook_segments"])
        if voc_confirmed:
            hook_gap = desired_hook_gap
        else:
            blocked_hook_gap = desired_hook_gap
    else:
        hook_gap = max(0, 3 - hero_count)

    # support gap: need at least 2 of {result, detail, scene} with some depth
    support_gap = 0
    if result_count < 3:
        support_gap += 3 - result_count
    if detail_count < 2:
        support_gap += 2 - detail_count
    if scene_count < 1:
        support_gap += 1 - scene_count
    support_gap = min(support_gap, max_support)

    # render plan
    planned_renders = 0
    min_assets = hero_count + result_count + detail_count
    if remaining > 0 and min_assets >= 3:
        planned_renders = remaining

    if remaining <= 0:
        status = "ready"
    elif task is None:
        status = "missing_task"
    elif use_voc_hooks and voc and voc.get("candidates") and not voc_confirmed and blocked_hook_gap > 0:
        status = "needs_manual_confirmation"
        blockers.append("voc_manual_confirmation_required")
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
        "remaining_to_target": remaining,
        "asset_pool": {
            "raw_total_segments": seg.get("raw_total", seg["total"]),
            "total_usable_segments": seg["total"],
            "hero": hero_count,
            "result": result_count,
            "detail": detail_count,
            "scene": scene_count,
            "ending": ending_count,
            "hook_segments": seg["hook_segments"],
        },
        "flow_summary": {
            "strict_good_outputs": existing_good,
            "base_good_outputs": out.get("base_good_outputs", existing_good),
            "good_outputs_excluded_by_failed_segments": out.get("good_outputs_excluded_by_failed_segments", 0),
            "target_met": remaining <= 0,
            "voc_participation": voc_participation_summary(use_voc_hooks, voc, voc_gap),
            "voc_output_usage": {
                "outputs_with_voc_segments": out.get("outputs_with_voc_segments", 0),
                "base_good_outputs_with_voc_segments": out.get("base_good_outputs_with_voc_segments", 0),
                "strict_good_outputs_with_voc_segments": out.get("strict_good_outputs_with_voc_segments", 0),
            },
            "bottleneck": bottleneck_summary(task, seg, remaining),
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
        warnings.append("VOC hook package exists but is NOT confirmed; dry-run-only until manual confirmation")
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
        (run_id, args.product_id, args.target_count, 1 if args.use_voc_hooks else 0,
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
        return {"status": "blocked", "reason": "voc_hook_package_missing"}
    if not voc_info.get("confirmed"):
        return {"status": "blocked", "reason": "voc_hook_package_not_confirmed"}
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


def submit_hook_packages(plan: Dict[str, Any], args: argparse.Namespace, prompt_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    if not args.submit_hook_packages:
        return {"status": "not_requested"}
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

    ctx = build_context()
    init = RDSRepositorySkill(ctx).init_db()
    if not init.success:
        return {"status": "failed", "reason": "rds_init_failed", "result": init.to_dict()}
    segments = ctx.repo.list_where("segments", "product_id=? AND source_type='ai_generated'", (args.product_id,))
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


def run_render_guard(args: argparse.Namespace) -> Dict[str, Any]:
    if not args.render:
        return {"status": "not_requested"}
    if not args.write:
        return {"status": "blocked", "reason": "render_requires_write"}
    cmd = [
        sys.executable,
        "scripts/run_mixcut_guard.py",
        "--product-id",
        args.product_id,
        "--target",
        str(args.target_count),
        "--max-rounds",
        str(max(1, int(args.guard_max_rounds or 3))),
    ]
    env = {"AUTO_MIXCUT_DB_PROVIDER": "mysql", "AUTO_MIXCUT_OSS_PROVIDER": "aliyun"}
    if os.environ.get("AUTO_MIXCUT_ADS_FAST_MODE", "").strip().lower() in {"1", "true", "yes", "on"}:
        env["AUTO_MIXCUT_ADS_FAST_MODE"] = "1"
    return command_result(
        cmd,
        SKILL_ROOT,
        env=env,
        timeout_minutes=args.render_timeout_minutes,
    )


def run_final_qc(args: argparse.Namespace, render_result: Dict[str, Any]) -> Dict[str, Any]:
    if not args.run_final_qc:
        return {"status": "not_requested"}
    if render_result and render_result.get("status") == "completed":
        return {"status": "completed", "reason": "covered_by_run_mixcut_guard"}
    return {"status": "skipped", "reason": "render_not_completed"}


# ── main ──

def run(args: argparse.Namespace) -> Dict[str, Any]:
    conn = connect_db(args.database_url)
    try:
        # step 1: inspect
        task = load_task(conn, args.product_id)
        seg = load_segment_summary(conn, args.product_id)
        out = load_output_summary(conn, args.product_id)
        voc = load_voc_hook_package(conn, args.product_id) if args.use_voc_hooks else None
        voc_gap = diagnose_voc_gap(conn, args.product_id) if args.use_voc_hooks and voc is None else {}

        # step 2: plan
        plan = plan_ads_mixcut(
            args.product_id, task, seg, out, voc,
            voc_gap,
            args.target_count, args.use_voc_hooks,
            args.max_new_hook_segments, args.max_new_support_segments,
        )

        run_id = "ads_unattended__{}__{}".format(args.product_id, now_iso().replace(":", "").replace("-", ""))
        result = {
            "run_id": run_id,
            "dry_run": not args.write,
            "product_id": args.product_id,
            "target_count": args.target_count,
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
            result["prepare_voc_hooks"] = prepare_voc_hooks(plan, args)

        scoped_prompt_ids = prepared_prompt_ids(result)
        if scoped_prompt_ids:
            result["scoped_prompt_ids"] = scoped_prompt_ids

        if args.submit_hook_packages:
            hook_gap = (plan.get("gap") or {}).get("new_hook_segments_planned", 0)
            if hook_gap <= 0 or not scoped_prompt_ids:
                result["submit_hook_packages"] = {"status": "skipped", "reason": "no_hook_gap" if hook_gap <= 0 else "no_prompt_ids"}
            else:
                result["submit_hook_packages"] = submit_hook_packages(plan, args, scoped_prompt_ids)

        if args.import_returns:
            result["import_returns"] = import_returns(args)

        if args.wait_returns:
            result["wait_returns"] = wait_for_returns(conn, args, scoped_prompt_ids)

        if args.run_segment_qc:
            result["segment_qc"] = run_segment_qc(args)

        render_result: Dict[str, Any] = {}
        if args.render:
            render_result = run_render_guard(args)
            result["render"] = render_result

        if args.run_final_qc:
            result["final_qc"] = run_final_qc(args, render_result)

        final_out = load_output_summary(conn, args.product_id)
        final_seg = load_segment_summary(conn, args.product_id)
        final_packages = load_prompt_package_summary(conn, args.product_id)
        result["final_inspect"] = {
            "good_outputs": final_out["good_outputs"],
            "total_outputs": final_out["total_outputs"],
            "remaining_to_target": max(0, args.target_count - final_out["good_outputs"]),
            "usable_segments": final_seg["total"],
            "prompt_packages": final_packages,
        }

        if args.write:
            persist_run(conn, run_id, result, args)
            result["written"] = True

        return result
    finally:
        try:
            conn.close()
        except Exception:
            pass


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="ADS 无人值守混剪编排")
    p.add_argument("--product-id", required=True)
    p.add_argument("--target-count", type=int, default=30)
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
    p.add_argument("--full-run", action="store_true", help="prepare, submit, wait/import, QC, render, and final QC in one guarded run")
    p.add_argument("--product-task-url", default=None)
    p.add_argument("--anchor-queue-url", default=None)
    p.add_argument("--prompt-workbench-url", default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--write", action="store_true", help="写入 run 表")
    p.add_argument("--json", dest="pretty", action="store_false", default=True)
    p.add_argument("--database-url", default=None)
    p.set_defaults(use_voc_hooks=True, short_ads_mode=True)
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.full_run:
        args.prepare_voc_hooks = True
        args.write_prompt_workbench = True
        args.submit_hook_packages = True
        args.import_returns = True
        args.wait_returns = True
        args.run_segment_qc = True
        args.render = True
        args.run_final_qc = True
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
