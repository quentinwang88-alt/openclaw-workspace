#!/usr/bin/env python3
"""飞书群发布日报通知。"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
import json
from pathlib import Path
from typing import Any, Dict, List

import requests


def default_summary_date() -> str:
    return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def default_queue_date() -> str:
    return date.today().strftime("%Y-%m-%d")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _compact_reason(reason: str, limit: int = 80) -> str:
    text = " ".join(_text(reason).split())
    if not text:
        return "未返回失败原因"
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def format_daily_publish_summary(summary: Dict[str, Any], *, max_failure_lines: int = 20) -> str:
    failures: List[Dict[str, Any]] = list(summary.get("failures") or [])
    accounts: List[Dict[str, Any]] = list(summary.get("accounts") or [])

    lines = [
        f"短视频自动发布日报 - {summary.get('date')}",
        f"总任务：{int(summary.get('total') or 0)}",
        f"发布成功：{int(summary.get('published') or 0)}",
        f"发布失败：{int(summary.get('failed') or 0)}",
        f"仍在排期：{int(summary.get('scheduled') or 0)}",
        f"待排期空槽：{int(summary.get('pending') or 0)}",
        f"已取消：{int(summary.get('cancelled') or 0)}",
    ]

    successful_accounts = [
        account
        for account in accounts
        if int(account.get("published") or 0) > 0
    ]
    if successful_accounts:
        store_success: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"published": 0, "accounts": []})
        for account in successful_accounts:
            store_id = _text(account.get("store_id")) or "未知店铺"
            published_count = int(account.get("published") or 0)
            store_success[store_id]["published"] += published_count
            store_success[store_id]["accounts"].append(
                f"{_text(account.get('account_name')) or _text(account.get('account_id'))} {published_count}"
            )

        lines.append("")
        lines.append("发布成功店铺清单：")
        for store_id, payload in sorted(
            store_success.items(),
            key=lambda item: (-int(item[1]["published"] or 0), item[0]),
        ):
            account_text = "；".join(payload["accounts"])
            lines.append(f"- {store_id}：成功 {int(payload['published'] or 0)}（{account_text}）")

    failed_accounts = [
        account
        for account in accounts
        if int(account.get("failed") or 0) > 0
    ]
    if failed_accounts:
        lines.append("")
        lines.append("失败主要环境：")
        for account in failed_accounts[:8]:
            lines.append(
                "- "
                f"{_text(account.get('store_id'))} / {_text(account.get('account_name'))}"
                f"（{_text(account.get('account_id'))}）："
                f"失败 {int(account.get('failed') or 0)}，成功 {int(account.get('published') or 0)}"
            )

    if failures:
        reason_counter = Counter(_compact_reason(row.get("error_message"), limit=60) for row in failures)
        lines.append("")
        lines.append("失败原因 Top：")
        for reason, count in reason_counter.most_common(5):
            lines.append(f"- {reason}：{count}")

        lines.append("")
        lines.append("失败明细：")
        for row in failures[:max_failure_lines]:
            scheduled_for = _text(row.get("scheduled_for"))
            time_text = scheduled_for[11:16] if len(scheduled_for) >= 16 else scheduled_for
            lines.append(
                "- "
                f"{time_text} "
                f"{_text(row.get('store_id'))}/{_text(row.get('account_name'))} "
                f"脚本ID={_text(row.get('script_id')) or '-'} "
                f"产品ID={_text(row.get('product_id')) or '-'} "
                f"任务ID={_text(row.get('publish_task_id')) or '-'} "
                f"原因={_compact_reason(row.get('error_message'))}"
            )
        remaining = len(failures) - max_failure_lines
        if remaining > 0:
            lines.append(f"- 还有 {remaining} 条失败明细未展开，请看发布追踪表。")
    else:
        lines.append("")
        lines.append("昨天没有发布失败任务。")

    return "\n".join(lines)


def _compact_title(title: str, limit: int = 42) -> str:
    text = " ".join(_text(title).split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def format_manual_publish_queue(rows: List[Dict[str, Any]], *, queue_date: str, max_items: int = 80) -> str:
    lines = [
        f"今日人工发布清单 - {queue_date}",
        f"待人工关注：{len(rows)} 条",
        "说明：按计划发布时间手动在 GeeLark 发布；商品ID为空/养号视频则不挂商品。",
    ]
    if not rows:
        lines.append("")
        lines.append("今天暂无需要人工处理的排期视频。")
        return "\n".join(lines)

    current_time = ""
    for index, row in enumerate(rows[:max_items], start=1):
        scheduled_for = _text(row.get("scheduled_for"))
        time_text = scheduled_for[11:16] if len(scheduled_for) >= 16 else scheduled_for
        if time_text != current_time:
            current_time = time_text
            lines.append("")
            lines.append(f"{time_text}")

        status = _text(row.get("schedule_status")) or "-"
        store_id = _text(row.get("store_id")) or "-"
        account_name = _text(row.get("account_name")) or "-"
        account_id = _text(row.get("account_id")) or "-"
        script_id = _text(row.get("script_id")) or "-"
        product_id = _text(row.get("product_id")) or "-"
        title = _compact_title(row.get("short_video_title"))
        video_path = _text(row.get("local_file_path")) or _text(row.get("video_source_value")) or "-"
        task_id = _text(row.get("publish_task_id")) or "-"
        error = _compact_reason(row.get("slot_error_message") or row.get("asset_error_message"), limit=60)
        reason_text = "" if error == "未返回失败原因" and status == "已排期" else f"\n  异常：{error}"
        lines.append(
            f"{index}. {store_id} / {account_name}（{account_id}）\n"
            f"  状态：{status}；脚本ID：{script_id}；商品ID：{product_id}\n"
            f"  标题：{title or '-'}\n"
            f"  视频：{video_path}\n"
            f"  GeeLark任务ID：{task_id}{reason_text}"
        )

    remaining = len(rows) - max_items
    if remaining > 0:
        lines.append("")
        lines.append(f"还有 {remaining} 条未展开，请查看发布追踪表。")
    return "\n".join(lines)


def format_product_publish_weekly_report(
    rows: List[Dict[str, Any]],
    *,
    periods: Dict[str, str],
    table_url: str = "",
    max_items: int = 100,
) -> str:
    if not rows:
        return f"产品发布周报 - {periods.get('this_week_label', '')}\n本周暂无产品发布。"
    store_map: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"this_week": 0, "last_week": 0, "this_month": 0, "products": []}
    )
    for row in rows:
        store_id = _text(row.get("store_id")) or "未知"
        product_id = _text(row.get("product_id")) or "-"
        tw = int(row.get("this_week_published") or 0)
        lw = int(row.get("last_week_published") or 0)
        tm = int(row.get("current_month_published") or 0)
        store_map[store_id]["this_week"] += tw
        store_map[store_id]["last_week"] += lw
        store_map[store_id]["this_month"] += tm
        if tw > 0:
            store_map[store_id]["products"].append(f"  {product_id}: {tw}")
    total_tw = sum(int(row.get("this_week_published") or 0) for row in rows)
    total_lw = sum(int(row.get("last_week_published") or 0) for row in rows)
    total_tm = sum(int(row.get("current_month_published") or 0) for row in rows)
    lines = [
        f"产品发布周报 - {periods.get('this_week_label', '')}",
        f"本周已发布：{total_tw} 条  |  上周：{total_lw} 条  |  本月：{total_tm} 条",
    ]
    for store_id, payload in sorted(
        store_map.items(),
        key=lambda item: -item[1]["this_week"],
    ):
        count = payload["this_week"]
        name = store_id
        lines.append("")
        lines.append(f"{name}：本周 {count}（上周 {payload['last_week']}，本月 {payload['this_month']}）")
        for product in payload["products"][:max_items]:
            lines.append(product)
    if table_url:
        lines.append("")
        lines.append(f"详细数据：{table_url}")
    return "\n".join(lines)


def send_feishu_webhook_text(webhook_url: str, text: str) -> Dict[str, Any]:
    url = _text(webhook_url)
    if not url:
        raise ValueError("缺少飞书群机器人 webhook URL")
    response = requests.post(
        url,
        json={"msg_type": "text", "content": {"text": text}},
        timeout=30,
    )
    try:
        payload = response.json()
    except ValueError:
        payload = {"raw": response.text}
    if response.status_code >= 400:
        raise RuntimeError(f"飞书 webhook 发送失败 HTTP {response.status_code}: {payload}")
    if isinstance(payload, dict) and payload.get("code") not in (None, 0):
        raise RuntimeError(f"飞书 webhook 发送失败: {payload}")
    return payload if isinstance(payload, dict) else {"result": payload}


def _load_openclaw_feishu_app(config_path: str | Path, account: str = "") -> Dict[str, str]:
    path = Path(config_path).expanduser()
    config = json.loads(path.read_text(encoding="utf-8"))
    feishu = config.get("channels", {}).get("feishu", {})
    account_key = _text(account)
    if account_key:
        app_config = feishu.get("accounts", {}).get(account_key, {})
    else:
        app_config = feishu
    app_id = _text(app_config.get("appId"))
    app_secret = _text(app_config.get("appSecret"))
    if not app_id or not app_secret:
        raise ValueError(f"OpenClaw 飞书账号配置不完整: {account_key or 'main'}")
    return {
        "app_id": app_id,
        "app_secret": app_secret,
        "domain": _text(feishu.get("domain")) or "feishu",
        "account": account_key or "main",
    }


def send_openclaw_feishu_text(
    *,
    openclaw_config_path: str | Path,
    chat_id: str,
    text: str,
    account: str = "",
) -> Dict[str, Any]:
    app = _load_openclaw_feishu_app(openclaw_config_path, account=account)
    token_resp = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": app["app_id"], "app_secret": app["app_secret"]},
        timeout=30,
    )
    token_payload = token_resp.json()
    if token_payload.get("code") != 0:
        raise RuntimeError(f"获取 OpenClaw 飞书 token 失败: {token_payload}")
    token = token_payload["tenant_access_token"]

    message_resp = requests.post(
        "https://open.feishu.cn/open-apis/im/v1/messages",
        params={"receive_id_type": "chat_id"},
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "receive_id": _text(chat_id),
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False),
        },
        timeout=30,
    )
    try:
        payload = message_resp.json()
    except ValueError:
        payload = {"raw": message_resp.text}
    if message_resp.status_code >= 400:
        raise RuntimeError(f"OpenClaw 飞书消息发送失败 HTTP {message_resp.status_code}: {payload}")
    if isinstance(payload, dict) and payload.get("code") not in (None, 0):
        raise RuntimeError(f"OpenClaw 飞书消息发送失败: {payload}")
    return payload if isinstance(payload, dict) else {"result": payload}
