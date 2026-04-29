#!/usr/bin/env python3
"""Append the latest market-insight report markdown into an existing Feishu doc."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.feishu import FeishuDocClient  # noqa: E402


def _extract_doc_token(doc_url: str) -> str:
    parsed = urlparse(str(doc_url or "").strip())
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 2 and parts[0] == "docx":
        return parts[1]
    raise SystemExit("无法从文档链接解析 doc token")


def main() -> int:
    parser = argparse.ArgumentParser(description="Append latest market insight report markdown into an existing Feishu doc.")
    parser.add_argument("--doc-url", required=True)
    parser.add_argument("--country", default="VN")
    parser.add_argument("--category", default="hair_accessory")
    parser.add_argument("--artifacts-root", default=str(ROOT / "artifacts" / "market_insight"))
    args = parser.parse_args()

    latest_index_path = Path(args.artifacts_root) / "latest" / "{country}__{category}.json".format(
        country=args.country,
        category=args.category,
    )
    latest_payload = json.loads(latest_index_path.read_text(encoding="utf-8"))
    report_md_path = Path(str(latest_payload.get("report_md_path") or "").strip())
    if not report_md_path.exists():
        raise SystemExit("最新报告 Markdown 不存在: {path}".format(path=report_md_path))

    report_markdown = report_md_path.read_text(encoding="utf-8").strip()
    title = "市场洞察报告-{country}-{category}-{batch_date}".format(
        country=args.country,
        category=args.category,
        batch_date=str(latest_payload.get("batch_date") or "").replace("-", ""),
    )
    content = "{title}\n\n{body}\n".format(title=title, body=report_markdown)

    doc_token = _extract_doc_token(args.doc_url)
    client = FeishuDocClient()
    append_result = client.append_markdown(document_id=doc_token, content=content)
    print(
        json.dumps(
            {
                "doc_token": doc_token,
                "doc_url": args.doc_url,
                "report_md_path": str(report_md_path),
                "line_count": int(append_result.get("line_count") or 0),
                "batch_count": int(append_result.get("batch_count") or 0),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
