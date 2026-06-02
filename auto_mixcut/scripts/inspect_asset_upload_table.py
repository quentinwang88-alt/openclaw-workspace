#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402


DEFAULT_URL = "https://gcngopvfvo0q.feishu.cn/wiki/KhzowIkkbi4Di6kOQRDcvd1NnYe?table=tblBj3UCaBRicSKS&view=vewcSyAc8S"


def client_from_url(url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(url)
    if not info:
        raise RuntimeError(f"无法解析飞书 URL: {url}")
    app_token = resolve_wiki_bitable_app_token(info.app_token) if "/wiki/" in info.original_url else info.app_token
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--product-id", required=True)
    args = parser.parse_args()

    client = client_from_url(args.url)
    fields = [{"name": f.field_name, "type": f.field_type, "ui_type": f.ui_type} for f in client.list_fields()]
    matches = []
    for record in client.list_records(page_size=500):
        values = record.fields
        if str(values.get("商品ID") or values.get("产品ID") or values.get("产品编码") or "").strip() == args.product_id:
            item = {"record_id": record.record_id, "fields": values}
            matches.append(item)
    print(json.dumps({"fields": fields, "matches": matches, "match_count": len(matches)}, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
