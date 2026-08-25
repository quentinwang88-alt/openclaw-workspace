#!/usr/bin/env python3
"""Hash-confirmed Feishu label backfill for migrated H1-H3 prompt rows."""

from __future__ import annotations

import argparse
import hashlib
import json
from typing import Any

import production_runtime as runtime


LABELS = {
    1: "高保真·H1基准",
    2: "高保真·H2外壳轻变",
    3: "高保真·H3钩子轻变",
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mother-id", required=True)
    parser.add_argument("--mother-version", type=int, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-plan", default="")
    args = parser.parse_args()

    connection_factory = runtime._connection_factory(runtime._database_url())
    with connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """SELECT product_id,sequence_no,full_prompt
               FROM wsr_replication_prompt
               WHERE mother_id=%s AND mother_version=%s AND sequence_no BETWEEN 1 AND 3
               ORDER BY product_id,sequence_no""",
            (args.mother_id, args.mother_version),
        )
        migrated = list(cursor.fetchall())

    app_id, app_secret = runtime._feishu_credentials()
    feishu = runtime.FeishuClient(app_id, app_secret)
    rows = feishu.list_records(runtime.DEFAULT_APP_TOKEN, runtime.DEFAULT_PROMPT_TABLE)
    index: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        fields = row.get("fields") or {}
        key = (
            runtime._text(fields.get("复刻产品")),
            runtime._text(fields.get("完整提示词")),
        )
        index.setdefault(key, []).append(row)

    actions: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []
    for prompt in migrated:
        key = (str(prompt["product_id"]), str(prompt["full_prompt"]))
        matches = index.get(key) or []
        if len(matches) != 1:
            unmatched.append({
                "product_id": prompt["product_id"],
                "sequence_no": int(prompt["sequence_no"]),
                "match_count": len(matches),
            })
            continue
        row = matches[0]
        current = runtime._text((row.get("fields") or {}).get("版本类型"))
        target = LABELS[int(prompt["sequence_no"])]
        if current != target:
            actions.append({
                "record_id": row["record_id"],
                "product_id": prompt["product_id"],
                "sequence_no": int(prompt["sequence_no"]),
                "from": current,
                "to": target,
            })

    digest = hashlib.sha256(
        json.dumps(actions, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    print(json.dumps({
        "mode": "apply" if args.apply else "dry-run",
        "plan_hash": digest,
        "action_count": len(actions),
        "actions": actions,
        "unmatched": unmatched,
    }, ensure_ascii=False, indent=2))
    if unmatched:
        raise RuntimeError("not every migrated prompt matched exactly one Feishu row")
    if not args.apply:
        return 0
    if args.confirm_plan != digest:
        raise RuntimeError("--confirm-plan must exactly match the dry-run hash")
    for action in actions:
        feishu.update_record(
            runtime.DEFAULT_APP_TOKEN,
            runtime.DEFAULT_PROMPT_TABLE,
            action["record_id"],
            {"版本类型": action["to"]},
        )
    print(f"applied={len(actions)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
