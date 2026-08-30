#!/usr/bin/env python3
"""Local entry point for the isolated organic-seeding script branch."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Sequence

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.content_branches.organic_seeding.pipeline import OrganicSeedingPipeline  # noqa: E402


def _load_json(path: str) -> Dict[str, Any]:
    payload = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("输入文件必须是JSON对象")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="生成种草类不挂车短视频脚本")
    parser.add_argument("--input", required=True, help="产品事实与可选主题JSON")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--duration-seconds", type=float, default=15)
    parser.add_argument("--request-id", default="")
    parser.add_argument("--image", action="append", default=[])
    parser.add_argument("--output", default="organic_seeding_result.json")
    parser.add_argument(
        "--preview-only",
        action="store_true",
        help="不调用模型，仅验证规划、隔离存储、质检和发布策略合同",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    payload = _load_json(args.input)
    product_context = payload.get("product_context") or payload
    themes = payload.get("themes") or []
    structures = payload.get("structure_contracts") or []
    product_code = str(product_context.get("product_code") or "").strip()
    request_id = args.request_id or f"LOCAL_SEEDING_{product_code}_{args.count}"
    result = OrganicSeedingPipeline().run(
        request_id=request_id,
        product_context=product_context,
        count=args.count,
        theme_inputs=themes,
        image_paths=args.image,
        duration_seconds=args.duration_seconds,
        preview_only=args.preview_only,
        structure_contracts=structures,
    )
    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "status": result["status"],
                "run_id": result["run_id"],
                "ready_count": result["ready_count"],
                "failed_count": result["failed_count"],
                "output": str(output),
                "publish_policy": result["publish_policy"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if result["status"] in {"COMPLETED", "PARTIAL"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
