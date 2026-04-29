#!/usr/bin/env python3
"""发送一次 Accio @ 测试消息，并观察是否收到机器人回复。"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.feishu import FeishuIMClient  # noqa: E402
from app.accio import extract_message_text  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="测试 Accio 机器人 @ 触发")
    parser.add_argument("--chat-id", required=True)
    parser.add_argument("--mention-id", required=True)
    parser.add_argument("--display-name", default="ACCIO选品专员")
    parser.add_argument("--wait-seconds", type=int, default=20)
    args = parser.parse_args()

    client = FeishuIMClient()
    sent_at = time.time()
    prompt = "权限联调测试，请忽略。如果你收到了这条 @ 消息，请简短回复“收到”。"
    client.send_text_with_mention(
        args.chat_id,
        args.mention_id,
        args.display_name,
        prompt,
    )
    time.sleep(max(args.wait_seconds, 1))
    messages = client.list_chat_messages(args.chat_id, since_timestamp=sent_at - 1, max_pages=3)
    summarized = []
    for item in messages[:10]:
        summarized.append(
            {
                "message_id": item.get("message_id"),
                "msg_type": item.get("msg_type"),
                "sender": item.get("sender"),
                "text": extract_message_text(item)[:300],
                "create_time": item.get("create_time"),
            }
        )
    print(
        json.dumps(
            {
                "chat_id": args.chat_id,
                "mention_id": args.mention_id,
                "wait_seconds": args.wait_seconds,
                "messages": summarized,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
