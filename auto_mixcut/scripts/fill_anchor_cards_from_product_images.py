#!/usr/bin/env python3
"""Fill Feishu product anchor cards from original-script anchors or product images."""
from __future__ import annotations

import argparse
import json
import mimetypes
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List


WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.core.config import Settings  # noqa: E402
from auto_mixcut.skills.llm_prompts import normalize_product_anchor  # noqa: E402
from auto_mixcut.skills.llm_router_skill import LLMRouterSkill  # noqa: E402
from auto_mixcut.skills.product_anchor_skill import _load_anchor_from_original_db  # noqa: E402


ANCHOR_QUEUE_URL = "https://gcngopvfvo0q.feishu.cn/wiki/V35wwjDLYiMFeTkiVFPc7SM5nvd?table=tbl2QRHwF7g9CmaF&view=vewv752AHQ"
ANCHOR_CARD_FIELD = "AI生成锚点卡"
CORE_FIELD = "核心视觉点"
MUST_NOT_CHANGE_FIELD = "不可错识别点"
FORBIDDEN_FIELD = "禁用错配项"
STRICT_ROLES_FIELD = "适用核心镜头"
CONFIRM_STATUS_FIELD = "人工确认状态"
NOTE_FIELD = "备注"
IMAGE_FIELD = "商品主图"


def resolve_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise RuntimeError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def fill_anchor_cards(
    anchor_queue_url: str,
    product_id_filter: str = "",
    dry_run: bool = False,
    generate_in_dry_run: bool = False,
    force: bool = False,
    limit: int | None = None,
    max_images: int = 3,
) -> Dict[str, Any]:
    client = resolve_client(anchor_queue_url)
    field_defs = {field.field_name: field for field in client.list_fields()}
    records = client.list_records(page_size=100, limit=limit)
    updated: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []

    for record in records:
        fields = record.fields or {}
        product_id = _text(fields.get("商品ID"))
        if product_id_filter and product_id != product_id_filter:
            continue
        if not product_id:
            skipped.append({"record_id": record.record_id, "reason": "missing_product_id"})
            continue

        status = _text(fields.get(CONFIRM_STATUS_FIELD))
        if status in {"已确认", "确认通过", "confirmed"} and not force:
            skipped.append({"product_id": product_id, "record_id": record.record_id, "reason": "already_confirmed"})
            continue

        existing_anchor = _jsonish(fields.get(ANCHOR_CARD_FIELD))
        if existing_anchor and not force and status not in {"需修改"}:
            skipped.append({"product_id": product_id, "record_id": record.record_id, "reason": "anchor_exists", "anchor_status": status})
            continue

        anchor = _load_anchor_from_original_db(product_id)
        source = "original_script_generator" if anchor else ""
        if not anchor:
            images = _attachments(fields.get(IMAGE_FIELD))
            if not images:
                skipped.append({"product_id": product_id, "record_id": record.record_id, "reason": "no_product_image"})
                continue
            if dry_run and not generate_in_dry_run:
                updated.append(
                    {
                        "product_id": product_id,
                        "record_id": record.record_id,
                        "action": "would_generate_from_images",
                        "image_count": min(len(images), max_images),
                    }
                )
                continue
            try:
                anchor = _generate_anchor_from_images(client, fields, images[:max_images])
                source = anchor.get("anchor_source") or "product_image_generation"
            except Exception as exc:
                failed.append({"product_id": product_id, "record_id": record.record_id, "reason": "vision_generation_failed", "error": str(exc)})
                continue

        updates = _build_updates(anchor, fields, source, field_defs)
        if dry_run:
            updated.append({"product_id": product_id, "record_id": record.record_id, "action": "would_update", "source": source, "updates": _preview_updates(updates)})
            continue
        try:
            client.update_record_fields(record.record_id, updates)
            updated.append({"product_id": product_id, "record_id": record.record_id, "source": source})
        except Exception as exc:
            failed.append({"product_id": product_id, "record_id": record.record_id, "reason": "feishu_update_failed", "error": str(exc)})

    return {"updated": updated, "skipped": skipped, "failed": failed}


def _generate_anchor_from_images(client: FeishuBitableClient, fields: Dict[str, Any], attachments: List[Dict[str, Any]]) -> Dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="auto_mixcut_anchor_") as tmp_dir:
        image_paths = _download_images(client, attachments, Path(tmp_dir))
        if not image_paths:
            raise RuntimeError("no downloadable product images")
        payload = {
            "product_id": _text(fields.get("商品ID")),
            "product_name": _text(fields.get("商品名称")),
            "market": _text(fields.get("市场")),
            "category": _text(fields.get("归一类目")) or _text(fields.get("类目")),
            "image_count": len(image_paths),
        }
        settings = Settings.load()
        router_ctx = _build_router_context(settings)
        call = LLMRouterSkill(router_ctx).call(
            "product_anchor_generation",
            {**payload, "image_paths": image_paths, "prompt_version": "v1.0"},
            product_id=payload["product_id"],
        )
        if not call.success:
            raise RuntimeError(call.error.message if call.error else "product anchor generation failed")
        response = call.data.get("response") or {}
        route = call.data.get("route") or {}
        anchor = normalize_product_anchor(response, payload["category"], payload["product_name"])
        anchor["anchor_source"] = f"product_image_generation:{route.get('model_tier') or 'medium_vision'}:{route.get('model_name') or 'unknown'}"
        return anchor


def _build_router_context(settings: Settings) -> Any:
    if settings.db_provider in {"mysql", "rds"}:
        ctx = build_context()
        ensure = getattr(ctx.repo, "ensure_llm_router_tables", None)
        if callable(ensure):
            ensure()
        return ctx
    return SimpleNamespace(settings=settings, repo=_InMemoryRouterRepo())


class _InMemoryRouterRepo:
    """Minimal repo for LLMRouterSkill so Feishu巡检 does not persist local state."""

    def __init__(self) -> None:
        self.rows: Dict[str, List[Dict[str, Any]]] = {}

    def get(self, table: str, key: str, value: Any) -> Dict[str, Any] | None:
        for row in self.rows.get(table, []):
            if row.get(key) == value:
                return dict(row)
        return None

    def list_where(self, table: str, where: str = "1=1", params: tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
        return []

    def upsert(self, table: str, key: str, row: Dict[str, Any]) -> Any:
        rows = self.rows.setdefault(table, [])
        for index, existing in enumerate(rows):
            if existing.get(key) == row.get(key):
                rows[index] = {**existing, **row}
                return SimpleNamespace(success=True, data=row)
        rows.append(dict(row))
        return SimpleNamespace(success=True, data=row)

    def insert(self, table: str, row: Dict[str, Any]) -> Any:
        self.rows.setdefault(table, []).append(dict(row))
        return SimpleNamespace(success=True, data=row)


def _download_images(client: FeishuBitableClient, attachments: List[Dict[str, Any]], target_dir: Path) -> List[str]:
    paths: List[str] = []
    for index, attachment in enumerate(attachments, 1):
        content, file_name, content_type, _size = client.download_attachment_bytes(attachment)
        suffix = Path(str(file_name or "")).suffix
        if not suffix:
            suffix = mimetypes.guess_extension(str(content_type or "")) or ".jpg"
        path = target_dir / f"product_{index}{suffix}"
        path.write_bytes(content)
        paths.append(str(path))
    return paths


def _build_updates(anchor: Dict[str, Any], fields: Dict[str, Any], source: str, field_defs: Dict[str, Any]) -> Dict[str, Any]:
    updates = {
        ANCHOR_CARD_FIELD: json.dumps(anchor, ensure_ascii=False, indent=2),
        CORE_FIELD: _join(anchor.get("core_visual_points")),
        MUST_NOT_CHANGE_FIELD: _join(anchor.get("must_not_change_points")),
        FORBIDDEN_FIELD: _join(anchor.get("forbidden_mismatch")),
        STRICT_ROLES_FIELD: _field_value(STRICT_ROLES_FIELD, anchor.get("strict_roles"), field_defs),
        CONFIRM_STATUS_FIELD: "待确认",
        NOTE_FIELD: _append_note(_text(fields.get(NOTE_FIELD)), source),
    }
    return {key: value for key, value in updates.items() if key in field_defs and value not in (None, "")}


def _field_value(field_name: str, value: Any, field_defs: Dict[str, Any]) -> Any:
    field = field_defs.get(field_name)
    ui_type = str(getattr(field, "ui_type", "") or "")
    if ui_type == "MultiSelect":
        allowed = {str(item.get("name") or "") for item in ((getattr(field, "property", None) or {}).get("options") or [])}
        values = _listish(value)
        return [item for item in values if not allowed or item in allowed]
    if ui_type == "SingleSelect":
        text = _text(value)
        allowed = {str(item.get("name") or "") for item in ((getattr(field, "property", None) or {}).get("options") or [])}
        return text if not allowed or text in allowed else ""
    return _join(value)


def _append_note(existing: str, source: str) -> str:
    line = f"锚点巡检自动补全：{source or 'unknown'}"
    if not existing:
        return line
    if line in existing:
        return existing
    return f"{existing}\n{line}"


def _preview_updates(updates: Dict[str, Any]) -> Dict[str, Any]:
    preview = dict(updates)
    if preview.get(ANCHOR_CARD_FIELD):
        preview[ANCHOR_CARD_FIELD] = str(preview[ANCHOR_CARD_FIELD])[:240] + "..."
    return preview


def _attachments(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict) and item.get("file_token")]
    if isinstance(value, dict) and value.get("file_token"):
        return [value]
    return []


def _jsonish(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    text = _text(value)
    if text.startswith("{"):
        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _join(value: Any) -> str:
    return "\n".join(_listish(value))


def _listish(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        items: List[str] = []
        for item in value:
            items.extend(_listish(item))
        return [item for item in items if item]
    if isinstance(value, dict):
        return [_text(value)] if _text(value) else []
    text = str(value or "").strip()
    if not text:
        return []
    for sep in ["\n", "；", ";", "、", ","]:
        if sep in text:
            return [item.strip("- ").strip() for item in text.split(sep) if item.strip("- ").strip()]
    return [text]


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value", "link", "url"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
        return ""
    if isinstance(value, list):
        return ",".join(item for item in (_text(item) for item in value) if item).strip()
    return str(value).strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--anchor-queue-url", default=ANCHOR_QUEUE_URL)
    parser.add_argument("--product-id", default="")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--max-images", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--generate-in-dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    result = fill_anchor_cards(
        args.anchor_queue_url,
        product_id_filter=args.product_id,
        dry_run=args.dry_run,
        generate_in_dry_run=args.generate_in_dry_run,
        force=args.force,
        limit=args.limit,
        max_images=max(1, min(args.max_images, 5)),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
