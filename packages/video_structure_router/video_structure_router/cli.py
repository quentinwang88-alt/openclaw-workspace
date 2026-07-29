"""Small diagnostic CLI for the structure router."""

from __future__ import annotations

import argparse
import json

from .models import RouteRequest
from .service import StructureRouterService
from .storage import RouterStorage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="视频结构路由诊断工具")
    parser.add_argument("--ensure-schema", action="store_true", help="创建/校验 sr_* 表")
    parser.add_argument("--request-id", default="manual-preview")
    parser.add_argument("--product-code", default="PREVIEW")
    parser.add_argument("--country", default="TH")
    parser.add_argument("--category", default="配饰")
    parser.add_argument("--product-type", default="发饰")
    parser.add_argument("--count", type=int, default=4)
    parser.add_argument("--duration", type=float, default=15.0)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--write", action="store_true", help="把选择结果写入 sr_*；默认只读预览")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    storage = RouterStorage()
    if args.ensure_schema:
        storage.ensure_schema()
    request = RouteRequest(
        request_id=args.request_id,
        consumer_flow="ORIGINAL_SCRIPT",
        product_code=args.product_code,
        target_country=args.country,
        category=args.category,
        product_type=args.product_type,
        direction_count=max(1, args.count),
        duration_seconds=args.duration,
        random_seed=args.seed,
        capabilities={
            "allowed_carriers": ["HAND_ONLY", "STATIC_PRODUCT", "MIXED", "WEARER_ACTIVE"],
            "allowed_continuity_modes": ["MULTI_CUT", "CONTINUOUS_LOW_CUT"],
            "min_shots": 4,
            "max_shots": 6,
        },
    )
    service = StructureRouterService(storage=storage if args.write else None)
    print(json.dumps(service.select(request).to_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
