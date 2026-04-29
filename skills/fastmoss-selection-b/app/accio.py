#!/usr/bin/env python3
"""Accio 请求导出与结果解析。"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlsplit

import pandas as pd

from app.models import AccioResponse
from app.utils import extract_json_candidates, normalize_bool, parse_percent, safe_text


def build_accio_image_file_name(item: Dict[str, Any]) -> str:
    product_id = safe_text(item.get("product_id")) or safe_text(item.get("work_id")) or "product_image"
    suffix = Path(urlsplit(safe_text(item.get("product_image"))).path).suffix or ".jpg"
    return "{product_id}{suffix}".format(product_id=product_id, suffix=suffix)


def build_accio_request_rows(selection_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for item in selection_rows:
        rows.append(
            {
                "work_id": item.get("work_id", ""),
                "product_id": item.get("product_id", ""),
                "商品名称": item.get("product_name", ""),
                "TikTok商品落地页地址": item.get("product_url", ""),
                "商品图片": item.get("product_image", ""),
                "图片文件名": build_accio_image_file_name(item),
                "7天成交均价_rmb": item.get("avg_price_7d_rmb"),
                "售价中位_rmb": item.get("price_mid_rmb"),
                "规则通过原因": item.get("rule_pass_reason", ""),
            }
        )
    return rows


def export_accio_request(rows: List[Dict[str, Any]], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    if path.suffix.lower() == ".csv":
        frame.to_csv(path, index=False)
        return path
    if path.suffix.lower() != ".xlsx":
        path = path.with_suffix(".xlsx")
    frame.to_excel(path, index=False)
    return path


def export_accio_image_bundle(
    selection_rows: List[Dict[str, Any]],
    path: Path,
    image_downloader: Any,
) -> Optional[Path]:
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        manifest = []
        for item in selection_rows:
            image_url = safe_text(item.get("product_image"))
            if not image_url:
                continue
            image_file_name = build_accio_image_file_name(item)
            try:
                content, _, _, _ = image_downloader(image_url, item)
            except Exception:
                continue
            archive.writestr(image_file_name, content)
            manifest.append(
                {
                    "work_id": safe_text(item.get("work_id")),
                    "product_id": safe_text(item.get("product_id")),
                    "image_file_name": image_file_name,
                    "image_url": image_url,
                }
            )
            written += 1
        if manifest:
            archive.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
    if written == 0:
        path.unlink(missing_ok=True)
        return None
    return path


def build_accio_message(batch_id: str, item_count: int) -> str:
    return (
        "FastMoss shortlist 请求\n"
        "batch_id: {batch_id}\n"
        "数量: {count}\n"
        "附件说明：\n"
        "1. `accio_request.xlsx` 是精简请求表\n"
        "2. `accio_images.zip` 是商品图片包，`图片文件名` 字段与 zip 内文件一一对应\n"
        "请对每个 work_id 只返回 1 个最合适的 1688 货源结果。\n"
        "要求：\n"
        "1. `procurement_price_rmb` 返回你最终推荐的单个采购价（人民币）\n"
        "2. `source_url` 返回对应 1688 采购链接\n"
        "3. 如有补充说明写到 `note`\n"
        "4. 请用 JSON code block 返回，格式示例：\n"
        "```json\n"
        "{{\n"
        '  "batch_id": "{batch_id}",\n'
        '  "items": [\n'
        "    {{\n"
        '      "work_id": "xxx",\n'
        '      "source_url": "https://detail.1688.com/offer/xxx.html",\n'
        '      "procurement_price_rmb": 12.8,\n'
        '      "procurement_price_range": "11.5-13.5",\n'
        '      "confidence": 0.86,\n'
        '      "abnormal_low_price": false,\n'
        '      "note": "起订量 2 件，颜色可选"\n'
        "    }}\n"
        "  ]\n"
        "}}\n"
        "```"
    ).format(batch_id=batch_id, count=item_count)


def _first_present(item: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = item.get(key)
        if value not in (None, ""):
            return value
    return None


def build_accio_workspace_note(source_url: Any, note: Any) -> str:
    source_text = safe_text(source_url)
    note_text = safe_text(note)
    if source_text and note_text:
        return "{source}\n{note}".format(source=source_text, note=note_text)
    return source_text or note_text


def _flatten_post_content(payload: Any) -> str:
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        tag = safe_text(payload.get("tag")).lower()
        if tag == "a":
            return safe_text(payload.get("href")) or safe_text(payload.get("text"))
        if tag in {"text", "code_block"}:
            return safe_text(payload.get("text"))
        if "content" in payload:
            return _flatten_post_content(payload.get("content"))
        pieces = []
        for key, value in payload.items():
            if key in {"tag", "style", "language"}:
                continue
            pieces.append(_flatten_post_content(value))
        return "\n".join(piece for piece in pieces if piece)
    if isinstance(payload, list):
        if payload and all(isinstance(item, (dict, str)) for item in payload):
            pieces = []
            for item in payload:
                piece = _flatten_post_content(item)
                if piece:
                    pieces.append(piece)
            return "".join(pieces)
        pieces = []
        for item in payload:
            pieces.append(_flatten_post_content(item))
        return "\n".join(piece for piece in pieces if piece)
    return ""


def extract_message_text(message: Dict[str, Any]) -> str:
    content = message.get("body", {}).get("content")
    if content is None:
        content = message.get("content")
    if isinstance(content, str):
        try:
            parsed = json.loads(content)
        except ValueError:
            return content
        if isinstance(parsed, dict) and "text" in parsed:
            return safe_text(parsed.get("text"))
        return _flatten_post_content(parsed)
    return _flatten_post_content(content)


def _normalize_accio_item(item: Dict[str, Any]) -> Dict[str, Any]:
    confidence = _first_present(
        item,
        "confidence",
        "match_confidence",
        "置信度",
    )
    if isinstance(confidence, str):
        confidence = parse_percent(confidence)
    elif isinstance(confidence, (int, float)):
        confidence = float(confidence) if float(confidence) <= 1 else float(confidence) / 100.0
    source_url = _first_present(
        item,
        "source_url",
        "purchase_url",
        "procurement_url",
        "1688_url",
        "url",
        "link",
        "推荐货源链接",
    )
    procurement_price_rmb = _first_present(
        item,
        "procurement_price_rmb",
        "best_procurement_price_rmb",
        "推荐采购价_rmb",
        "采购价_rmb",
        "price_rmb",
        "price",
    )
    procurement_price_range = _first_present(
        item,
        "procurement_price_range",
        "price_range",
        "采购价区间",
    )
    abnormal_low_price = _first_present(
        item,
        "abnormal_low_price",
        "low_price_flag",
        "异常低价标记",
    )
    note = _first_present(
        item,
        "note",
        "accio_note",
        "备注",
        "comment",
    )
    return {
        "work_id": safe_text(item.get("work_id")),
        "accio_source_url": safe_text(source_url),
        "procurement_price_rmb": procurement_price_rmb,
        "procurement_price_range": safe_text(procurement_price_range),
        "match_confidence": confidence,
        "abnormal_low_price": 1 if normalize_bool(abnormal_low_price) else 0,
        "accio_note": safe_text(note),
    }


def _work_id_allowed(work_id: str, batch_id: str, valid_work_id_set: set[str]) -> bool:
    if not work_id:
        return False
    if valid_work_id_set:
        return work_id in valid_work_id_set
    return work_id.startswith("{batch_id}_".format(batch_id=batch_id))


def _extract_items_from_payload(
    payload: Any,
    batch_id: str,
    valid_work_id_set: set[str],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    normalized = {}
    raw_items = {}

    if isinstance(payload, dict) and safe_text(payload.get("batch_id")) == batch_id and isinstance(payload.get("items"), list):
        for item in payload.get("items", []):
            if not isinstance(item, dict):
                continue
            normalized_item = _normalize_accio_item(item)
            work_id = normalized_item["work_id"]
            if not _work_id_allowed(work_id, batch_id, valid_work_id_set):
                continue
            normalized[work_id] = normalized_item
            raw_items[work_id] = dict(item)
        return normalized, raw_items

    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            normalized_item = _normalize_accio_item(item)
            work_id = normalized_item["work_id"]
            if not _work_id_allowed(work_id, batch_id, valid_work_id_set):
                continue
            normalized[work_id] = normalized_item
            raw_items[work_id] = dict(item)
        return normalized, raw_items

    if isinstance(payload, dict) and "work_id" in payload:
        normalized_item = _normalize_accio_item(payload)
        work_id = normalized_item["work_id"]
        if _work_id_allowed(work_id, batch_id, valid_work_id_set):
            normalized[work_id] = normalized_item
            raw_items[work_id] = dict(payload)
    return normalized, raw_items


def parse_accio_response_from_messages(
    messages: List[Dict[str, Any]],
    batch_id: str,
    valid_work_ids: Optional[Iterable[str]] = None,
) -> Optional[AccioResponse]:
    valid_work_id_set = {safe_text(work_id) for work_id in (valid_work_ids or []) if safe_text(work_id)}
    ordered_messages = sorted(
        messages,
        key=lambda item: int(str(item.get("create_time") or "0") or "0"),
    )
    ordered_texts = []
    merged_items = {}
    merged_raw_items = {}
    last_message_id = ""

    for message in ordered_messages:
        text = extract_message_text(message)
        ordered_texts.append(text)
        if safe_text(message.get("message_id")):
            last_message_id = safe_text(message.get("message_id"))
        for block in extract_json_candidates(text):
            try:
                payload = json.loads(block)
            except ValueError:
                continue
            normalized, raw_items = _extract_items_from_payload(payload, batch_id, valid_work_id_set)
            if not normalized:
                continue
            merged_items.update(normalized)
            merged_raw_items.update(raw_items)
    combined_text = "\n".join([text for text in ordered_texts if text])
    for block in extract_json_candidates(combined_text):
        try:
            payload = json.loads(block)
        except ValueError:
            continue
        normalized, raw_items = _extract_items_from_payload(payload, batch_id, valid_work_id_set)
        if not normalized:
            continue
        merged_items.update(normalized)
        merged_raw_items.update(raw_items)
    if merged_items:
        return AccioResponse(
            batch_id=batch_id,
            items=merged_items,
            message_id=last_message_id or "combined_messages",
            raw_payload={"batch_id": batch_id, "items": list(merged_raw_items.values())},
        )
    return None
