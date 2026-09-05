#!/usr/bin/env python3
"""Explicit, hash-confirmed historical purpose classification; never calls a model.

No arguments: read-only inventory. A scoped dry-run requires mother ID/version,
purpose and cart setting. Applying that exact plan updates classification only,
inside one transaction and under the production task's named lock.

This does NOT reconstruct historical asset snapshots, enqueue export, update
Feishu, or enter production. After classification, use an explicit historical
export to reconstruct and label the available legacy asset context.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
from typing import Any


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
PACKAGE_ROOT = WORKSPACE_ROOT / "packages" / "wig_success_replication"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from wig_success_replication.publishing import resolve_publish_settings  # noqa: E402


UNKNOWN = {None, "", "未分类"}
NEXT_STEP = "归类不会自动补图片快照或导出；之后须显式执行 historical export，并标记 legacy_reconstructed。"


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def build_plan(mother: dict[str, Any] | None, batches: list[dict[str, Any]],
               prompts: list[dict[str, Any]], *, publish_purpose: str,
               cart_setting: Any) -> dict[str, Any]:
    purpose, cart = resolve_publish_settings(publish_purpose, cart_setting)
    if not mother:
        raise ValueError("母版版本不存在")
    scope = {"mother_id": mother["mother_id"], "mother_version": int(mother["version"])}
    batch_ids = {row["batch_id"] for row in batches}
    actions: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []
    for table, key, rows in (
        ("wsr_replication_batch", "batch_id", batches),
        ("wsr_replication_prompt", "prompt_id", prompts),
    ):
        for row in sorted(rows, key=lambda value: value[key]):
            if row["mother_id"] != scope["mother_id"] or int(row["mother_version"]) != scope["mother_version"]:
                raise ValueError("历史记录越出明确母版版本范围，拒绝更新")
            if table == "wsr_replication_prompt" and row["batch_id"] not in batch_ids:
                raise ValueError(f"历史结果 {row[key]} 与所属批次母版范围不一致")
            old_purpose = row.get("publish_purpose")
            old_cart = row.get("cart_enabled")
            # Do not repair or overwrite previously classified data here.
            if old_purpose not in UNKNOWN:
                if old_purpose != purpose or old_cart is None or bool(old_cart) != cart:
                    raise ValueError(f"已分类冲突：{table}/{row[key]}，请先人工核对；本次不写任何记录")
            elif old_cart is not None and bool(old_cart) != cart:
                raise ValueError(f"已有挂车设置冲突：{table}/{row[key]}，本次不写任何记录")
            snapshot = {"table": table, "id": row[key], "content_hash": digest(row)}
            snapshots.append(snapshot)
            if old_purpose in UNKNOWN:
                actions.append({**snapshot, "from_purpose": old_purpose, "from_cart": old_cart,
                                "to_purpose": purpose, "to_cart": cart})
    # Classification merges unknown rows into the chosen cumulative namespace.
    # Detect collisions with already-classified rows during dry-run rather
    # than discovering them halfway through an UPDATE transaction.
    sequences: set[tuple[str, int]] = set()
    content: set[tuple[str, str]] = set()
    for row in prompts:
        sequence_key = (row["product_id"], int(row["sequence_no"]))
        if sequence_key in sequences:
            raise ValueError(f"归类后累计序号冲突：产品 {sequence_key[0]} / 第 {sequence_key[1]} 条；未写入")
        sequences.add(sequence_key)
        if row.get("prompt_hash"):
            content_key = (row["product_id"], row["prompt_hash"])
            if content_key in content:
                raise ValueError(f"归类后内容哈希冲突：产品 {row['product_id']}；未写入")
            content.add(content_key)
    result = {
        "scope": scope,
        "mother_name": mother.get("name") or "",
        "mother_snapshot_hash": digest(mother),
        "publish_purpose": purpose,
        "cart_enabled": cart,
        "prompt_count": len(prompts),
        "batch_count": len(batches),
        "zero_result_batch_ids": sorted(batch_ids - {row["batch_id"] for row in prompts}),
        "snapshots": snapshots,
        "actions": actions,
        "action_count": len(actions),
        "next_step": NEXT_STEP,
    }
    return {**result, "plan_hash": digest(result)}


def read_scope(cursor: Any, mother_id: str, mother_version: int, *, lock: bool = False):
    suffix = " FOR UPDATE" if lock else ""
    cursor.execute("SELECT * FROM wsr_mother_version WHERE mother_id=%s AND version=%s" + suffix,
                   (mother_id, mother_version))
    mother = cursor.fetchone()
    cursor.execute("SELECT * FROM wsr_replication_batch WHERE mother_id=%s AND mother_version=%s ORDER BY batch_id" + suffix,
                   (mother_id, mother_version))
    batches = list(cursor.fetchall())
    cursor.execute("SELECT * FROM wsr_replication_prompt WHERE mother_id=%s AND mother_version=%s ORDER BY prompt_id" + suffix,
                   (mother_id, mother_version))
    return mother, batches, list(cursor.fetchall())


def inventory(cursor: Any) -> dict[str, Any]:
    cursor.execute(
        """SELECT p.mother_id,p.mother_version,p.product_id,m.name AS mother_name,
                  COUNT(*) AS prompt_count
           FROM wsr_replication_prompt p LEFT JOIN wsr_mother_version m
           ON m.mother_id=p.mother_id AND m.version=p.mother_version
           WHERE p.publish_purpose IS NULL OR p.publish_purpose IN ('','未分类')
           GROUP BY p.mother_id,p.mother_version,p.product_id,m.name
           ORDER BY p.mother_id,p.mother_version,p.product_id"""
    )
    grouped = {}
    for row in cursor.fetchall():
        key = (row["mother_id"], int(row["mother_version"]), row["product_id"])
        grouped[key] = {"mother_id": key[0], "mother_version": key[1], "product_id": key[2],
                        "mother_name": row.get("mother_name") or "", "unclassified_prompt_count": int(row["prompt_count"]),
                        "unclassified_batch_ids": [], "zero_result_batch_ids": []}
    cursor.execute(
        """SELECT b.mother_id,b.mother_version,b.batch_id,b.product_ids_json,m.name AS mother_name,
                  (SELECT COUNT(*) FROM wsr_replication_prompt p WHERE p.batch_id=b.batch_id) AS prompt_count
           FROM wsr_replication_batch b LEFT JOIN wsr_mother_version m
           ON m.mother_id=b.mother_id AND m.version=b.mother_version
           WHERE b.publish_purpose IS NULL OR b.publish_purpose IN ('','未分类')
           ORDER BY b.mother_id,b.mother_version,b.batch_id"""
    )
    for row in cursor.fetchall():
        products = row["product_ids_json"]
        if isinstance(products, str):
            products = json.loads(products)
        for product_id in products or [""]:
            key = (row["mother_id"], int(row["mother_version"]), str(product_id))
            target = grouped.setdefault(key, {"mother_id": key[0], "mother_version": key[1], "product_id": key[2],
                                             "mother_name": row.get("mother_name") or "", "unclassified_prompt_count": 0,
                                             "unclassified_batch_ids": [], "zero_result_batch_ids": []})
            target["unclassified_batch_ids"].append(row["batch_id"])
            if not int(row["prompt_count"]):
                target["zero_result_batch_ids"].append(row["batch_id"])
    return {"mode": "read-only-inventory", "groups": [grouped[key] for key in sorted(grouped)],
            "unclassified_prompt_count": sum(row["unclassified_prompt_count"] for row in grouped.values()),
            "next_step": NEXT_STEP}


def classify(connection: Any, *, mother_id: str, mother_version: int,
             publish_purpose: str, cart_setting: Any, apply: bool = False,
             confirm_plan: str = "") -> dict[str, Any]:
    if not mother_id.strip() or mother_version < 1:
        raise ValueError("必须指定有效母版 ID 与版本")
    resolve_publish_settings(publish_purpose, cart_setting)
    if apply and (len(confirm_plan) != 64 or any(c not in "0123456789abcdef" for c in confirm_plan)):
        raise ValueError("--confirm-plan 必须为 dry-run 输出的完整 SHA256")
    acquired = False
    lock_name = ""
    try:
        with connection.cursor() as cursor:
            if apply:
                # Share the production runner's record-level lock, so a task
                # cannot create unclassified rows during this atomic migration.
                cursor.execute("SELECT feishu_record_id FROM wsr_mother_version WHERE mother_id=%s AND version=%s",
                               (mother_id, mother_version))
                mother = cursor.fetchone()
                if not mother:
                    raise ValueError("母版版本不存在")
                lock_name = f"wsr:{mother.get('feishu_record_id') or mother_id}"[:64]
                cursor.execute("SELECT GET_LOCK(%s, 0) AS acquired", (lock_name,))
                acquired = bool((cursor.fetchone() or {}).get("acquired"))
                if not acquired:
                    raise RuntimeError("母版任务正在执行，未写入；待任务结束后重新 dry-run")
                connection.begin()
            mother, batches, prompts = read_scope(cursor, mother_id, mother_version, lock=apply)
            plan = build_plan(mother, batches, prompts, publish_purpose=publish_purpose, cart_setting=cart_setting)
            if not apply:
                return {"mode": "dry-run", **plan}
            if confirm_plan != plan["plan_hash"]:
                raise ValueError("计划已变化或确认 hash 不匹配；未写入，请重新 dry-run")
            for action in plan["actions"]:
                # Table/key are selected from our fixed allowlist, never CLI.
                table = action["table"]
                key = "prompt_id" if table == "wsr_replication_prompt" else "batch_id"
                cursor.execute(
                    f"UPDATE {table} SET publish_purpose=%s,cart_enabled=%s "
                    f"WHERE {key}=%s AND mother_id=%s AND mother_version=%s "
                    "AND (publish_purpose IS NULL OR publish_purpose IN ('','未分类'))",
                    (plan["publish_purpose"], plan["cart_enabled"], action["id"], mother_id, mother_version),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError(f"记录 {action['id']} 已变化，事务全部回滚")
            connection.commit()
            return {"mode": "applied", **plan, "updated_records": len(plan["actions"])}
    except BaseException:
        if apply:
            connection.rollback()
        raise
    finally:
        if acquired:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT RELEASE_LOCK(%s) AS released", (lock_name,))
            except Exception:
                # The owning connection is closed by main. Preserve the real
                # transaction outcome instead of masking it with cleanup I/O.
                pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mother-id")
    parser.add_argument("--mother-version", type=int)
    parser.add_argument("--publish-purpose", choices=["带货", "养号"])
    parser.add_argument("--cart-setting", choices=["按用途默认", "挂车", "不挂车", "是", "否"])
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--dry-run", action="store_true")
    modes.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-plan", default="")
    args = parser.parse_args(argv)
    supplied = [args.mother_id, args.mother_version, args.publish_purpose, args.cart_setting]
    scoped = any(value is not None for value in supplied)
    if scoped and not all(value is not None for value in supplied):
        parser.error("归类必须同时提供 --mother-id、--mother-version、--publish-purpose、--cart-setting")
    if scoped and (not args.mother_id.strip() or args.mother_version < 1):
        parser.error("母版 ID 不能为空，版本必须为正整数")
    if args.apply and not scoped:
        parser.error("--apply 必须指定明确母版版本及发布设置")
    if args.confirm_plan and not args.apply:
        parser.error("--confirm-plan 只能与 --apply 同时使用")
    # Lazy import reuses only the configured DictCursor connection factory.
    # Do not build the application, runner, OpenAI client or Feishu client.
    import production_runtime as runtime

    with runtime._connection_factory(runtime._database_url())() as connection:
        if scoped:
            result = classify(connection, mother_id=args.mother_id, mother_version=args.mother_version,
                              publish_purpose=args.publish_purpose, cart_setting=args.cart_setting,
                              apply=args.apply, confirm_plan=args.confirm_plan)
        else:
            with connection.cursor() as cursor:
                result = inventory(cursor)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
