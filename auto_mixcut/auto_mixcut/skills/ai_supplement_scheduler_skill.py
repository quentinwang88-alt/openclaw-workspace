from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
import sys
from typing import Any
from zoneinfo import ZoneInfo

import requests

from auto_mixcut.core.result import Result

from .ai_supplement_gateway_skill import AISupplementGatewaySkill, submit_budget_from_state
from .context import SkillContext


APPROVAL_PENDING_STATUSES = {"approval_requested", "needs_submit_retry", "created", "approved"}


def daytime_approval_required(ctx: SkillContext, product_id: str) -> bool:
    if _truthy(os.environ.get("AUTO_MIXCUT_AI_SUPPLEMENT_FORCE_SUBMIT")):
        return False
    if not _truthy(os.environ.get("AUTO_MIXCUT_AI_SUPPLEMENT_DAY_APPROVAL", "1")):
        return False
    task = _latest_task(ctx, product_id) or {}
    if str(task.get("ai_supplement_status") or "") == "approved":
        return False
    return _is_daytime()


def request_daytime_approval(ctx: SkillContext, product_id: str, budget: dict[str, Any], command: list[str] | None = None) -> dict[str, Any]:
    task = _latest_task(ctx, product_id)
    if not task:
        return {"status": "failed", "reason": "task_not_found", "product_id": product_id}
    product = ctx.repo.get("products", "product_id", product_id) or {}
    detail = _detail(task)
    today = _now().date().isoformat()
    already_notified = (
        detail.get("approval_requested_date") == today
        and str(task.get("ai_supplement_status") or "") == "approval_requested"
    )
    approval_command = _approval_command(product_id)
    message = _approval_message(product, task, budget, approval_command)
    notify_result: dict[str, Any] = {"skipped": True, "reason": "already_notified_today"} if already_notified else _send_notification(message)
    patch_detail = {
        **detail,
        "state": "approval_requested",
        "approval_requested_at": _now().isoformat(timespec="seconds"),
        "approval_requested_date": today,
        "approval_command": approval_command,
        "submit_command": " ".join(command or []),
        "budget": budget,
        "notification": notify_result,
    }
    ctx.repo.update(
        "content_tasks",
        "task_id",
        task["task_id"],
        {
            "task_status": "AI_SUPPLEMENT_CREATED",
            "pipeline_status": "WAITING_AI_RETURN",
            "next_action": "WAIT_AI_SUPPLEMENT_APPROVAL",
            "last_error": "daytime_ai_supplement_approval_required",
            "ai_supplement_status": "approval_requested",
            "ai_supplement_package_count": int(budget.get("ai_submit_inflight_count") or task.get("ai_supplement_package_count") or 0),
            "ai_supplement_detail_json": patch_detail,
        },
    )
    return {
        "status": "approval_requested",
        "already_notified": already_notified,
        "notification": notify_result,
        "approval_command": approval_command,
        "message": message,
    }


def queue_daytime_approval(ctx: SkillContext, product_id: str, budget: dict[str, Any], command: list[str] | None = None) -> dict[str, Any]:
    task = _latest_task(ctx, product_id)
    if not task:
        return {"status": "failed", "reason": "task_not_found", "product_id": product_id}
    detail = {
        **_detail(task),
        "state": "approval_requested",
        "approval_queued_at": _now().isoformat(timespec="seconds"),
        "approval_requested_date": _now().date().isoformat(),
        "approval_queued_slot": _approval_slot(),
        "approval_command": _approval_command(product_id),
        "submit_command": " ".join(command or []),
        "budget": budget,
        "notification": {"skipped": True, "reason": "queued_for_12_18_batch"},
    }
    ctx.repo.update(
        "content_tasks",
        "task_id",
        task["task_id"],
        {
            "task_status": "AI_SUPPLEMENT_CREATED",
            "pipeline_status": "WAITING_AI_RETURN",
            "next_action": "WAIT_AI_SUPPLEMENT_APPROVAL",
            "last_error": "daytime_ai_supplement_approval_required",
            "ai_supplement_status": "approval_requested",
            "ai_supplement_package_count": int(budget.get("ready_to_submit_count") or budget.get("ai_submit_inflight_count") or task.get("ai_supplement_package_count") or 0),
            "ai_supplement_detail_json": detail,
        },
    )
    return {
        "status": "approval_queued",
        "reason": "queued_for_12_18_batch",
        "product_id": product_id,
        "approval_command": _approval_command(product_id),
    }


def request_daytime_batch_approval(ctx: SkillContext, items: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = [item for item in items if int(item.get("remaining_count") or 0) > 0 and int(_budget_from_item(item).get("submit_limit") or 0) > 0]
    if not candidates:
        return {"status": "skipped", "reason": "no_pending_products"}
    slot = _approval_slot()
    already_notified = True
    rows: list[dict[str, Any]] = []
    for item in candidates:
        product_id = str(item.get("product_id") or "")
        task = _latest_task(ctx, product_id)
        if not task:
            continue
        detail = _detail(task)
        budget = _budget_from_item(item)
        rows.append({"item": item, "task": task, "detail": detail, "budget": budget})
        if detail.get("approval_requested_slot") != slot or str(task.get("ai_supplement_status") or "") != "approval_requested":
            already_notified = False
    if not rows:
        return {"status": "skipped", "reason": "no_valid_tasks"}

    message = _batch_approval_message([row["item"] | {"budget": row["budget"]} for row in rows])
    notify_result: dict[str, Any] = {"skipped": True, "reason": "already_notified_this_slot"} if already_notified else _send_notification(message)
    now = _now().isoformat(timespec="seconds")
    for row in rows:
        task = row["task"]
        item = row["item"]
        budget = row["budget"]
        detail = {
            **row["detail"],
            "state": "approval_requested",
            "approval_requested_at": now,
            "approval_requested_date": _now().date().isoformat(),
            "approval_requested_slot": slot,
            "approval_command": _approval_all_command(),
            "submit_command": " ".join(_submit_command(str(item.get("product_id") or ""), budget)),
            "budget": budget,
            "notification": notify_result,
        }
        ctx.repo.update(
            "content_tasks",
            "task_id",
            task["task_id"],
            {
                "task_status": "AI_SUPPLEMENT_CREATED",
                "pipeline_status": "WAITING_AI_RETURN",
                "next_action": "WAIT_AI_SUPPLEMENT_APPROVAL",
                "last_error": "daytime_ai_supplement_approval_required",
                "ai_supplement_status": "approval_requested",
                "ai_supplement_package_count": int(budget.get("ready_to_submit_count") or budget.get("ai_submit_inflight_count") or task.get("ai_supplement_package_count") or 0),
                "ai_supplement_detail_json": detail,
            },
        )
    return {
        "status": "approval_requested",
        "already_notified": already_notified,
        "slot": slot,
        "product_count": len(rows),
        "notification": notify_result,
        "approval_command": _approval_all_command(),
        "message": message,
    }


def approve_product(ctx: SkillContext, product_id: str) -> Result:
    task = _latest_task(ctx, product_id)
    if not task:
        return Result.fail("TASK_NOT_FOUND", "task not found", {"product_id": product_id})
    detail = _detail(task)
    detail.update({"state": "approved", "approved_at": _now().isoformat(timespec="seconds")})
    res = ctx.repo.update(
        "content_tasks",
        "task_id",
        task["task_id"],
        {
            "ai_supplement_status": "approved",
            "ai_supplement_detail_json": detail,
            "pipeline_status": "WAITING_AI_RETURN",
            "next_action": "RUN_AI_SEGMENT_WORKER",
            "last_error": "",
        },
    )
    return res if not res.success else Result.ok({"product_id": product_id, "ai_supplement_status": "approved"})


def approve_pending_products(ctx: SkillContext) -> Result:
    approved = []
    for item in pending_ai_supplement_products(ctx, include_waiting_return=False):
        if str(item.get("ai_supplement_status") or "") not in {"approval_requested", "needs_submit_retry", "created"}:
            continue
        product_id = str(item.get("product_id") or "")
        res = approve_product(ctx, product_id)
        approved.append({"product_id": product_id, **res.to_dict()})
    return Result.ok({"approved_count": len([item for item in approved if item.get("success")]), "results": approved})


def pending_ai_supplement_products(ctx: SkillContext, include_waiting_return: bool = True) -> list[dict[str, Any]]:
    tasks = ctx.repo.list_where("content_tasks", "requested_variant_count>0 ORDER BY id DESC", ())
    seen: set[str] = set()
    pending: list[dict[str, Any]] = []
    for task in tasks:
        product_id = str(task.get("product_id") or "")
        if not product_id or product_id in seen:
            continue
        seen.add(product_id)
        target = int(task.get("requested_variant_count") or 0)
        actual = int(task.get("actual_variant_count") or 0)
        remaining = int(task.get("target_remaining_variant_count") or max(0, target - actual))
        if remaining <= 0:
            continue
        package_state = AISupplementGatewaySkill(ctx).package_state(product_id)
        ai_status = str(task.get("ai_supplement_status") or "")
        next_action = str(task.get("next_action") or "")
        pipeline_status = str(task.get("pipeline_status") or "")
        needs_submit = (
            ai_status in APPROVAL_PENDING_STATUSES
            or next_action in {"RUN_AI_SEGMENT_WORKER", "WAIT_AI_SUPPLEMENT_APPROVAL"}
            or package_state["ready_to_submit_count"] > 0
        )
        waiting_return = include_waiting_return and (
            pipeline_status == "WAITING_AI_RETURN"
            or next_action == "WAIT_AI_SEGMENT_RETURN"
            or package_state["inflight_count"] > 0
        )
        if not needs_submit and not waiting_return:
            continue
        product = ctx.repo.get("products", "product_id", product_id) or {}
        pending.append(
            {
                "product_id": product_id,
                "task_id": task.get("task_id"),
                "product_name": product.get("product_name") or product_id,
                "market": product.get("market") or "",
                "category": product.get("category") or "",
                "target_count": target,
                "remaining_count": remaining,
                "pipeline_status": pipeline_status,
                "next_action": next_action,
                "ai_supplement_status": ai_status,
                **package_state,
            }
        )
    return pending


def should_submit_now(ctx: SkillContext, product_id: str, mode: str) -> bool:
    mode = str(mode or "").strip().lower()
    if mode in {"nightly", "night", "review"}:
        return True
    if mode in {"approved", "approve"}:
        return True
    if not daytime_approval_required(ctx, product_id):
        return True
    return False


def _approval_message(product: dict[str, Any], task: dict[str, Any], budget: dict[str, Any], approval_command: str) -> str:
    product_id = str(task.get("product_id") or product.get("product_id") or "")
    product_name = str(product.get("product_name") or product_id)
    market = str(product.get("market") or "")
    category = str(product.get("category") or "")
    target = int(task.get("requested_variant_count") or 0)
    actual = int(task.get("actual_variant_count") or 0)
    remaining = int(task.get("target_remaining_variant_count") or max(0, target - actual))
    submit_limit = int(budget.get("submit_limit") or budget.get("needed_after_inflight") or 0)
    inflight = int(budget.get("ai_submit_inflight_count") or 0)
    return "\n".join(
        [
            "混剪 AI 补素材需要确认",
            f"商品：{product_id}",
            f"名称：{product_name[:80]}",
            f"市场/类目：{market} / {category}",
            f"目标/已有效/缺口：{target} / {actual} / {remaining}",
            f"建议本轮补素材：{submit_limit} 个（已在途/已导入包：{inflight}）",
            "",
            "同意后让 OpenClaw 执行：",
            approval_command,
            "",
            "如果不同意，可以不处理；23:00 后心跳会把当天待补素材统一 review 并自动跑。",
        ]
    )


def _batch_approval_message(items: list[dict[str, Any]]) -> str:
    total_submit = sum(int((item.get("budget") or {}).get("submit_limit") or 0) for item in items)
    lines = [
        "混剪 AI 补素材汇总确认",
        f"待确认商品：{len(items)} 个",
        f"建议补素材：{total_submit} 个",
        "",
    ]
    for index, item in enumerate(items, start=1):
        budget = item.get("budget") or {}
        product_name = str(item.get("product_name") or item.get("product_id") or "")
        lines.extend(
            [
                f"{index}. {item.get('product_id')}",
                f"   {str(product_name)[:70]}",
                f"   市场/类目：{item.get('market') or '-'} / {item.get('category') or '-'}",
                f"   目标缺口：{item.get('remaining_count')}；本轮建议补：{budget.get('submit_limit')}；待提单包：{item.get('ready_to_submit_count')}；在途：{item.get('inflight_count')}",
            ]
        )
    lines.extend(
        [
            "",
            "全部同意后让 OpenClaw 执行：",
            _approval_all_command(),
            "",
            "不同意可以不处理；23:00 后心跳会统一 review 当天待补素材并自动跑。",
        ]
    )
    return "\n".join(lines)


def _send_notification(message: str) -> dict[str, Any]:
    if _truthy(os.environ.get("AUTO_MIXCUT_AI_SUPPLEMENT_NOTIFY_DRY_RUN")):
        print(message)
        return {"dry_run": True}
    webhook = _first_env("AUTO_MIXCUT_FEISHU_WEBHOOK_URL", "SHORT_VIDEO_AUTO_PUBLISH_FEISHU_WEBHOOK_URL", "FEISHU_WEBHOOK_URL", "LARK_WEBHOOK_URL")
    if webhook:
        response = requests.post(webhook, json={"msg_type": "text", "content": {"text": message}}, timeout=30)
        payload = _safe_json(response)
        if response.status_code >= 400 or (isinstance(payload, dict) and payload.get("code") not in (None, 0)):
            return {"sent": False, "channel": "webhook", "error": payload, "status_code": response.status_code}
        return {"sent": True, "channel": "webhook", "result": payload}
    chat_id = _first_env("AUTO_MIXCUT_FEISHU_CHAT_ID", "SHORT_VIDEO_AUTO_PUBLISH_FEISHU_CHAT_ID", "FEISHU_CHAT_ID", "LARK_CHAT_ID")
    if chat_id:
        try:
            publisher_path = Path("/Users/likeu3/.openclaw/workspace/skills/short-video-auto-publisher")
            if str(publisher_path) not in sys.path:
                sys.path.insert(0, str(publisher_path))
            from app.notifications import send_openclaw_feishu_text  # type: ignore

            result = send_openclaw_feishu_text(
                openclaw_config_path=Path.home() / ".openclaw" / "openclaw.json",
                chat_id=chat_id,
                text=message,
                account=os.environ.get("AUTO_MIXCUT_OPENCLAW_FEISHU_ACCOUNT", ""),
            )
            return {"sent": True, "channel": "openclaw_feishu", "result": result}
        except Exception as exc:
            return {"sent": False, "channel": "openclaw_feishu", "error": str(exc)}
    try:
        daily_report_path = Path("/Users/likeu3/.openclaw/workspace/skills/daily-report-inspection")
        if str(daily_report_path) not in sys.path:
            sys.path.insert(0, str(daily_report_path))
        from feishu_sender import FeishuSender  # type: ignore

        result = FeishuSender().send_text_message(message)
        if isinstance(result, dict) and result.get("code") not in (None, 0):
            return {"sent": False, "channel": "daily_report_feishu_sender", "error": result}
        return {"sent": True, "channel": "daily_report_feishu_sender", "result": result}
    except Exception as exc:
        fallback_error = str(exc)
    print(message)
    return {"sent": False, "channel": "stdout", "reason": "notification_channel_not_configured", "fallback_error": fallback_error}


def _approval_command(product_id: str) -> str:
    return (
        "cd /Users/likeu3/.openclaw/workspace/auto_mixcut && "
        "env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy "
        "AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun "
        f"python3 scripts/run_ai_supplement_heartbeat.py --approve-product {product_id} --run-now"
    )


def _approval_all_command() -> str:
    return (
        "cd /Users/likeu3/.openclaw/workspace/auto_mixcut && "
        "env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy "
        "AUTO_MIXCUT_DB_PROVIDER=mysql AUTO_MIXCUT_OSS_PROVIDER=aliyun "
        "python3 scripts/run_ai_supplement_heartbeat.py --approve-all --run-now"
    )


def _submit_command(product_id: str, budget: dict[str, Any]) -> list[str]:
    limit = max(1, int(budget.get("submit_limit") or budget.get("ready_to_submit_count") or budget.get("remaining_count") or 1))
    needed = max(1, int(budget.get("target_remaining") or budget.get("remaining_count") or limit))
    return [
        "node",
        "/Users/likeu3/.openclaw/workspace/skills/jimeng-video-generator/segment-package-worker.js",
        "--submit-only",
        "--one-shot",
        f"--product-id={product_id}",
        f"--limit={limit}",
        f"--max-submit-needed={needed}",
    ]


def _budget_from_item(item: dict[str, Any]) -> dict[str, int]:
    remaining = max(1, int(item.get("remaining_count") or 1))
    return submit_budget_from_state(remaining, item)


def _approval_slot() -> str:
    now = _now()
    hour = 12 if now.hour < 18 else 18
    return f"{now.date().isoformat()}T{hour:02d}"


def _latest_task(ctx: SkillContext, product_id: str) -> dict[str, Any] | None:
    rows = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
    return rows[0] if rows else None


def _detail(task: dict[str, Any]) -> dict[str, Any]:
    value = task.get("ai_supplement_detail_json") or {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {"raw": value[:1000]}
    return {}


def _now() -> datetime:
    return datetime.now(ZoneInfo(os.environ.get("AUTO_MIXCUT_LOCAL_TZ", "Asia/Shanghai")))


def _is_daytime() -> bool:
    now = _now()
    day_start = _int_env("AUTO_MIXCUT_AI_SUPPLEMENT_DAY_START_HOUR", 9)
    night_start = _int_env("AUTO_MIXCUT_AI_SUPPLEMENT_NIGHT_START_HOUR", 23)
    return day_start <= now.hour < night_start


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)) or default)
    except ValueError:
        return default


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _first_env(*names: str) -> str:
    for name in names:
        value = str(os.environ.get(name, "") or "").strip()
        if value:
            return value
    return ""


def _safe_json(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return {"raw": response.text[:500]}
