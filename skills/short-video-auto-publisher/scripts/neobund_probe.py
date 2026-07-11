#!/usr/bin/env python3
"""NeoBund connectivity probe for short-video auto publish."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Dict


SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from app.neobund_publish import NeoBundPublishAdapter  # noqa: E402


DEFAULT_CONFIG_PATH = Path("/Users/likeu3/.openclaw/shared/data/short_video_auto_publisher_config.json")


def load_config(path: str | Path) -> Dict[str, Any]:
    config_path = Path(path).expanduser()
    if not config_path.exists():
        return {}
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def resolve_value(args: argparse.Namespace, config: Dict[str, Any], arg_name: str, config_keys: tuple[str, ...]) -> str:
    explicit = str(getattr(args, arg_name, "") or "").strip()
    if explicit:
        return explicit
    for key in config_keys:
        value = str(config.get(key, "") or "").strip()
        if value:
            return value
    return ""


def compact_account(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "authId": item.get("authId") or item.get("id"),
        "username": item.get("username") or item.get("creatorUsername"),
        "remark": item.get("remark"),
        "userType": item.get("userType"),
        "registerRegion": item.get("registerRegion"),
        "quotaStatus": item.get("quotaStatus"),
    }


def compact_product(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": item.get("id") or item.get("ttProductId"),
        "title": item.get("title") or item.get("productTitle"),
        "status": item.get("status"),
        "shopId": item.get("shopId"),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NeoBund 登录态、账号、商品、上传探针")
    parser.add_argument("--config-path", default=str(DEFAULT_CONFIG_PATH), help="本地配置文件路径")
    parser.add_argument("--base-url", default="https://www.neobund.ai/np", help="NeoBund API Base URL")
    parser.add_argument("--access-token", default="", help="NeoBund access_token；为空时读配置 neobund_access_token")
    parser.add_argument("--cookie", default="", help="NeoBund Cookie header；为空时读配置 neobund_cookie")
    parser.add_argument("--account-id", default="", help="NeoBund authId；为空时只探测账号列表")
    parser.add_argument("--product-id", default="", help="TikTok Shop 商品 ID，用于商品列表过滤")
    parser.add_argument("--list-products", action="store_true", help="查询指定账号下商品")
    parser.add_argument("--video-path", default="", help="本地视频路径；配合 --upload-only 只上传不发布")
    parser.add_argument("--upload-only", action="store_true", help="只上传视频并登记文件，不创建发布任务")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = load_config(args.config_path)
    access_token = resolve_value(args, config, "access_token", ("neobund_access_token", "neobund_token", "neobund_api_token"))
    cookie = resolve_value(args, config, "cookie", ("neobund_cookie", "neobund_cookie_header"))
    account_map = config.get("neobund_account_id_map") or config.get("neobund_auth_id_map") or {}
    if not isinstance(account_map, dict):
        account_map = {}

    adapter = NeoBundPublishAdapter(
        base_url=args.base_url,
        access_token=access_token,
        cookie=cookie,
        account_id_map=account_map,
    )

    accounts_payload = adapter.list_accounts()
    records = accounts_payload.get("records", []) if isinstance(accounts_payload, dict) else []
    print(json.dumps({"step": "accounts", "count": len(records), "records": [compact_account(item) for item in records[:20] if isinstance(item, dict)]}, ensure_ascii=False, indent=2))

    auth_id = str(args.account_id or "").strip()
    if args.list_products:
        if not auth_id:
            raise SystemExit("--list-products 需要 --account-id")
        products_payload = adapter.list_products(auth_id=auth_id, product_id=args.product_id)
        products = products_payload.get("records", []) if isinstance(products_payload, dict) else []
        print(json.dumps({"step": "products", "count": len(products), "records": [compact_product(item) for item in products[:20] if isinstance(item, dict)]}, ensure_ascii=False, indent=2))

    if args.upload_only:
        if not args.video_path:
            raise SystemExit("--upload-only 需要 --video-path")
        upload_result = adapter.upload_video(args.video_path)
        print(json.dumps({"step": "upload", "fileId": upload_result.file_id, "key": upload_result.key, "bucketName": upload_result.bucket_name, "url": upload_result.url}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
