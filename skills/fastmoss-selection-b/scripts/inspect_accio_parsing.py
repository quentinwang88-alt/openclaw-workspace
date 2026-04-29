#!/usr/bin/env python3
"""检查 Accio 群消息的解析覆盖率。"""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.accio import extract_message_text, parse_accio_response_from_messages  # noqa: E402
from app.config import get_settings  # noqa: E402
from app.pipeline import FastMossPipeline  # noqa: E402
from app.utils import safe_text  # noqa: E402


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: inspect_accio_parsing.py <batch_id>")
    batch_id = sys.argv[1].strip()
    settings = get_settings()
    pipeline = FastMossPipeline.from_settings(settings)
    batch = pipeline.db.get_batch(batch_id) or {}
    chat_id = safe_text(batch.get("accio_chat_id"))
    if not chat_id:
        raise RuntimeError("批次缺少 accio_chat_id")
    valid_work_ids = {
        safe_text(row.get("work_id"))
        for row in pipeline.db.list_selection_records(batch_id)
        if safe_text(row.get("work_id"))
    }
    messages = pipeline.messenger.list_chat_messages(chat_id, max_pages=10)
    parsed = parse_accio_response_from_messages(messages, batch_id, valid_work_ids=valid_work_ids)

    items = []
    for message in sorted(messages, key=lambda item: int(str(item.get("create_time") or "0") or "0"), reverse=True)[:20]:
        text = extract_message_text(message)
        items.append(
            {
                "message_id": message.get("message_id"),
                "create_time": message.get("create_time"),
                "sender": message.get("sender"),
                "text_preview": text[:400],
                "contains_batch_id": batch_id in text,
                "work_id_hits": text.count(batch_id + "_"),
            }
        )

    print(
        json.dumps(
            {
                "batch_id": batch_id,
                "chat_id": chat_id,
                "valid_work_id_count": len(valid_work_ids),
                "parsed_item_count": len(parsed.items) if parsed else 0,
                "parsed_message_id": parsed.message_id if parsed else "",
                "parsed_work_ids": sorted(parsed.items.keys())[:50] if parsed else [],
                "messages": items,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
