#!/usr/bin/env python3
"""Safely repair Feishu display projections for an already-generated batch.

This is deliberately a projection-only maintenance command.  It never calls a
model and never regenerates the visual script, voiceover, allocation, or video
prompt.  Each supplied summary is stored alongside the already-approved
central voiceover, then only the three human-facing Feishu fields are updated.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402
from core.original_batch_storage import BatchStorage  # noqa: E402
from core.production_script_feishu import PRODUCTION_SCRIPT_FIELD_NAMES  # noqa: E402
from core.production_script_renderer import build_production_projection  # noqa: E402


DEFAULT_SCRIPT_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "KsX7w8Y8ZiJfnsk2Mtvc7xLun1f?table=tblIvHJ0nsn9WCwi&view=vewKfXc8lj"
)


def _client(url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(url)
    if not info:
        raise ValueError(f"无法解析飞书链接: {url}")
    app_token = resolve_wiki_bitable_app_token(info.app_token) if "/wiki/" in url else info.app_token
    return FeishuBitableClient(app_token, info.table_id)


def _parse_summaries(values: list[str]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for value in values:
        item_id, separator, summary = value.partition("=")
        if not separator or not item_id.strip() or not summary.strip():
            raise ValueError("--summary 格式必须是 BATCH_ITEM_ID=安全中文卖点摘要")
        if item_id.strip() in result:
            raise ValueError(f"重复的 batch item: {item_id.strip()}")
        result[item_id.strip()] = summary.strip()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="只回填历史批次的安全卖点展示字段")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--summary", action="append", required=True)
    parser.add_argument("--script-url", default=DEFAULT_SCRIPT_URL)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    summaries = _parse_summaries(args.summary)
    storage = BatchStorage()
    batch = storage.get_batch(args.batch_id)
    if not batch:
        raise ValueError(f"批次不存在: {args.batch_id}")
    items = {item.batch_item_id: item for item in storage.get_items(args.batch_id)}
    missing = sorted(set(summaries) - set(items))
    if missing:
        raise ValueError("批次中不存在 item: " + ", ".join(missing))

    changed = []
    for item_id, summary in summaries.items():
        item = items[item_id]
        bundle = json.loads(item.content_bundle_json or "{}")
        argument = bundle.get("selling_argument") if isinstance(bundle.get("selling_argument"), dict) else {}
        if not (
            argument.get("respectful_reframe_required")
            or str(argument.get("expression_policy") or "") == "SEMANTIC_AUTHORITY_NOT_VERBATIM"
        ):
            raise ValueError(f"{item_id} 不是需要安全展示回填的卖点")
        result = json.loads(item.result_json or "{}")
        script = result.get("script") if isinstance(result.get("script"), dict) else {}
        voice = script.get("continuous_voiceover") if isinstance(script.get("continuous_voiceover"), dict) else {}
        if not voice.get("target_text") or not voice.get("chinese_translation"):
            raise ValueError(f"{item_id} 缺少已完成的中央口播，拒绝编造摘要")
        voice["selling_argument_realization_zh"] = summary
        script["continuous_voiceover"] = voice
        result["script"] = script
        item.result_json = json.dumps(result, ensure_ascii=False, sort_keys=True)
        changed.append(item)

    if args.dry_run:
        for item in changed:
            projection = build_production_projection(batch=batch, item=item)
            print(json.dumps({
                "batch_item_id": item.batch_item_id,
                "script_id": projection["script_id"],
                "script_title": projection["script_title"],
                "core_selling_point": projection["core_selling_point"],
            }, ensure_ascii=False))
        return 0

    # Persist the local authoritative script result before projecting it.
    for item in changed:
        storage.update_item_status(
            item.batch_item_id,
            item.status,
            result_json=item.result_json,
        )

    client = _client(args.script_url)
    field_names = PRODUCTION_SCRIPT_FIELD_NAMES
    by_script_id = {
        str(record.fields.get(field_names["script_id"]) or "").strip(): record
        for record in client.list_records(page_size=100)
    }
    updated = 0
    for item in changed:
        projection = build_production_projection(batch=batch, item=item)
        script_id = str(projection["script_id"] or "").strip()
        record = by_script_id.get(script_id)
        if not record:
            raise ValueError(f"飞书中找不到已导出的脚本: {script_id}")
        client.update_record_fields(record.record_id, {
            field_names["script_title"]: projection["script_title"],
            field_names["core_selling_point"]: projection["core_selling_point"],
            field_names["complete_script"]: projection["complete_script"],
        })
        updated += 1
    print(json.dumps({"batch_id": args.batch_id, "updated": updated}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
