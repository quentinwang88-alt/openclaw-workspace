"""Bootstrap authoritative product context for a brand-new operation-table SKU.

The row-based production flow must not require a legacy original-script run.
For a new SKU, the operation row's product images are the appearance authority;
the central voiceover catalog remains the selling-argument authority.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from core.category_execution import reconcile_anchor_category_contract
from core.json_parser import validate_anchor_card_payload
from core.llm_client import OriginalScriptLLMClient
from core.product_selling_argument_adapter import load_verified_selling_point_catalog
from core.prompts import build_anchor_card_prompt


def _text(value: Any) -> str:
    return str(value or "").strip()


def _stable_hash(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _anchor_cache_root() -> Path:
    configured = _text(os.environ.get("ORIGINAL_SCRIPT_ANCHOR_CACHE_ROOT"))
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".openclaw" / "shared" / "data" / "original_product_anchor_cache"


def _reference_cache_root() -> Path:
    configured = _text(os.environ.get("ORIGINAL_SCRIPT_PRODUCT_REFERENCE_CACHE_ROOT"))
    if configured:
        return Path(configured).expanduser()
    return _anchor_cache_root().parent / "original_product_reference_cache"


def _asset_descriptor(path: str | Path, *, attachment: Mapping[str, Any]) -> Dict[str, Any]:
    source = Path(path).expanduser().resolve()
    return {
        "role": "PRODUCT_REFERENCE",
        "local_path": str(source),
        "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "file_token": _text(attachment.get("file_token")),
        "name": _text(attachment.get("name") or attachment.get("file_name")),
        "authority": "OPERATION_TASK_PRODUCT_IMAGES",
    }


def build_operation_product_context(
    *,
    operation_client: Any,
    task: Mapping[str, Any],
    record_id: str,
    output_dir: Path,
    voiceover_root: str,
    llm_client: Optional[OriginalScriptLLMClient] = None,
) -> Dict[str, Any]:
    """Create the minimum P1 context needed by PLAN_ONLY for a new SKU.

    This is a one-model-call product-anchor bootstrap. It does not generate a
    strategy, selling point, hook, scene or script, and it never treats product
    names as visual evidence.
    """

    attachments = list(task.get("product_images") or [])
    if not attachments:
        raise RuntimeError("NEW_SKU_ANCHOR_UNAVAILABLE: 运营任务没有产品图片")

    anchor_cache_key = _stable_hash(
        {
            "product_code": task.get("product_code"),
            "product_type": task.get("product_type"),
            "top_category": task.get("top_category"),
            "attachment_tokens": [
                _text(item.get("file_token"))
                for item in attachments
                if isinstance(item, dict)
            ],
            "schema": "operation-new-sku-anchor-v2-product-cache",
        }
    )
    # Cache by SKU + image identity rather than operation row.  Repeated
    # batches for the same product reuse the same audited appearance anchor and
    # do not pay for another image download/model pass.
    cache_path = _anchor_cache_root() / f"{anchor_cache_key}.json"
    audit_cache_path = Path(output_dir) / "operation_anchor_card.json"
    cached = {}
    if cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            cached = {}
    anchor_card = (
        dict(cached.get("anchor_card") or {})
        if cached.get("cache_key") == anchor_cache_key
        else {}
    )
    if anchor_card:
        validate_anchor_card_payload(anchor_card)
        product_reference_assets = [
            dict(item) for item in cached.get("product_reference_assets") or []
            if isinstance(item, Mapping)
            and Path(_text(item.get("local_path"))).expanduser().is_file()
        ]
    else:
        image_root = (
            _reference_cache_root() / _text(task.get("product_code")) / anchor_cache_key
        )
        image_root.mkdir(parents=True, exist_ok=True)
        image_paths = []
        product_reference_assets = []
        for index, attachment in enumerate(attachments[:4], start=1):
            downloaded = operation_client.download_attachment(
                attachment, image_root / f"image_{index}"
            )
            image_paths.append(str(downloaded))
            product_reference_assets.append(
                _asset_descriptor(downloaded, attachment=attachment)
            )

        client = llm_client or OriginalScriptLLMClient(route="primary")
        anchor_card = client.call_json(
            build_anchor_card_prompt(
                _text(task.get("target_country") or "泰国"),
                _text(task.get("target_language") or "泰语"),
                _text(task.get("product_type")),
                "",
                "",
            ),
            image_paths=image_paths,
            max_tokens=5200,
            validator=validate_anchor_card_payload,
        )
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps(
                {
                    "cache_key": anchor_cache_key,
                    "product_code": _text(task.get("product_code")),
                    "schema_version": "operation-new-sku-anchor-v2-product-cache",
                    "anchor_card": anchor_card,
                    "product_reference_assets": product_reference_assets,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    audit_cache_path.parent.mkdir(parents=True, exist_ok=True)
    audit_cache_path.write_text(
        json.dumps(
            {
                "cache_key": anchor_cache_key,
                "shared_cache_path": str(cache_path),
                "anchor_card": anchor_card,
                "product_reference_assets": product_reference_assets,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    anchor_card = reconcile_anchor_category_contract(
        anchor_card,
        product_type=_text(task.get("product_type")),
        top_category=_text(task.get("top_category")),
    )
    anchor_card.update(
        {
            "product_code": _text(task.get("product_code")),
            "target_country": _text(task.get("target_country") or "泰国"),
            "target_language": _text(task.get("target_language") or "泰语"),
            "product_type": _text(task.get("product_type")),
            "top_category": _text(task.get("top_category")),
            "anchor_authority": "OPERATION_TASK_PRODUCT_IMAGES",
        }
    )

    central_snapshot = load_verified_selling_point_catalog(
        _text(task.get("product_code")),
        voiceover_root=voiceover_root,
        product_type=_text(task.get("product_type")),
    )
    central_catalog = list(central_snapshot.get("catalog") or [])
    input_material = {
        "record_id": record_id,
        "product_code": task.get("product_code"),
        "product_type": task.get("product_type"),
        "top_category": task.get("top_category"),
        "target_country": task.get("target_country"),
        "target_language": task.get("target_language"),
        "attachment_tokens": [
            _text(item.get("file_token")) for item in attachments if isinstance(item, dict)
        ],
        "anchor_card": anchor_card,
        "product_reference_asset_fingerprints": [
            {
                "role": item.get("role"), "sha256": item.get("sha256"),
                "file_token": item.get("file_token"),
            }
            for item in product_reference_assets
        ],
        "selling_snapshot_hash": central_snapshot.get("snapshot_hash"),
    }
    return {
        "source_run_id": "OPERATION_NEW_SKU_BOOTSTRAP",
        "source_record_id": record_id,
        "input_hash": _stable_hash(input_material),
        "product_code": _text(task.get("product_code")),
        "target_country": _text(task.get("target_country") or "泰国"),
        "target_language": _text(task.get("target_language") or "泰语"),
        "product_type": _text(task.get("product_type")),
        "top_category": _text(task.get("top_category")),
        "anchor_card": anchor_card,
        "product_reference_assets": product_reference_assets,
        "structure_route": {
            "status": "REBUILD_REQUIRED",
            "request": {},
            "assignments": [],
            "source": "operation_new_sku_anchor_bootstrap",
        },
        "selling_point_catalog": central_catalog,
        "selling_point_catalog_snapshot": central_snapshot,
        "selling_point_catalog_sources": {
            "central_confirmed_count": int(
                central_snapshot.get("confirmed_argument_count") or 0
            ),
            "central_available_count": int(
                central_snapshot.get("available_argument_count") or len(central_catalog)
            ),
            "central_mapped_count": int(
                central_snapshot.get("mapped_argument_count") or 0
            ),
            "central_unmapped_count": int(
                central_snapshot.get("unmapped_argument_count") or 0
            ),
            "legacy_strategy_count": 0,
            "selected_source": "central_operator",
        },
        "product_selling_note": "",
    }
