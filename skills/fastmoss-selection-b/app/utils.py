#!/usr/bin/env python3
"""通用工具。"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from datetime import date, datetime, timezone
from json import JSONDecoder
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote, unquote, urlsplit, urlunsplit


NUMBER_RE = re.compile(r"-?\d+(?:,\d{3})*(?:\.\d+)?")
JSON_CODE_BLOCK_RE = re.compile(r"```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```", re.IGNORECASE | re.DOTALL)


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def sha256_bytes(content: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(content)
    return digest.hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value != 0
    text = safe_text(value).lower()
    return text in {"1", "true", "yes", "y", "on", "是", "已启用", "启用"}


def _coerce_numeric_string(value: str) -> str:
    return value.replace(",", "").strip()


def safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = _coerce_numeric_string(safe_text(value))
    if not text:
        return None
    if text.startswith("(") and text.endswith(")"):
        text = "-{body}".format(body=text[1:-1])
    try:
        return float(text)
    except ValueError:
        match = NUMBER_RE.search(text)
        if not match:
            return None
        return float(match.group(0).replace(",", ""))


def safe_int(value: Any) -> Optional[int]:
    numeric = safe_float(value)
    if numeric is None:
        return None
    return int(round(numeric))


def parse_percent(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        if abs(numeric) > 1:
            return numeric / 100.0
        return numeric
    text = safe_text(value)
    if not text:
        return None
    numeric = safe_float(text)
    if numeric is None:
        return None
    if "%" in text or abs(numeric) > 1:
        return numeric / 100.0
    return numeric


def parse_price_range(value: Any) -> Tuple[str, Optional[float], Optional[float], Optional[float]]:
    raw = safe_text(value)
    if not raw:
        return "", None, None, None
    matches = [float(item.replace(",", "")) for item in NUMBER_RE.findall(raw)]
    if not matches:
        return raw, None, None, None
    if len(matches) == 1:
        low = high = matches[0]
    else:
        low = min(matches[0], matches[1])
        high = max(matches[0], matches[1])
    median = round((low + high) / 2.0, 4)
    return raw, low, high, median


def to_rmb(amount_local: Optional[float], fx_rate_to_rmb: Optional[float]) -> Optional[float]:
    if amount_local is None or not fx_rate_to_rmb:
        return None
    if fx_rate_to_rmb == 0:
        return None
    return round(amount_local / fx_rate_to_rmb, 4)


def parse_datetime_value(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime()
        except Exception:
            pass
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        if numeric > 10_000_000_000:
            return datetime.fromtimestamp(numeric / 1000.0, tz=timezone.utc).replace(tzinfo=None)
        if numeric > 1_000_000_000:
            return datetime.fromtimestamp(numeric, tz=timezone.utc).replace(tzinfo=None)
    text = safe_text(value)
    if not text:
        return None
    if text.isdigit():
        numeric = int(text)
        if len(text) >= 13:
            return datetime.fromtimestamp(numeric / 1000.0, tz=timezone.utc).replace(tzinfo=None)
        if len(text) >= 10:
            return datetime.fromtimestamp(numeric, tz=timezone.utc).replace(tzinfo=None)
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y.%m.%d",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def to_feishu_datetime_millis(value: Any) -> Any:
    parsed = parse_datetime_value(value)
    if not parsed:
        return value
    return int(parsed.replace(tzinfo=timezone.utc).timestamp() * 1000)


def sanitize_url_for_feishu(value: Any) -> str:
    text = safe_text(value)
    if not text:
        return ""
    parsed = urlsplit(text)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    normalized_path = quote(unquote(parsed.path), safe="/-._~")
    normalized_query = quote(unquote(parsed.query), safe="=&%-._~")
    normalized_fragment = quote(unquote(parsed.fragment), safe="-._~")
    return urlunsplit((parsed.scheme, parsed.netloc, normalized_path, normalized_query, normalized_fragment))


def build_feishu_url_cell(value: Any, text: Optional[str] = None) -> Any:
    url = sanitize_url_for_feishu(value)
    if not url:
        return ""
    label = safe_text(text) or url
    return {"text": label, "link": url, "type": "url"}


def calc_listing_days(snapshot_time: Optional[datetime], listing_time: Optional[datetime]) -> Optional[int]:
    if snapshot_time is None or listing_time is None:
        return None
    diff = snapshot_time.date() - listing_time.date()
    return max(diff.days, 0)


def extract_product_id(product_url: Any, explicit_product_id: Any = None) -> str:
    explicit_text = safe_text(explicit_product_id)
    if explicit_text:
        return explicit_text

    url = safe_text(product_url)
    if not url:
        return ""

    for pattern in (
        r"(?:product|item|goods|sku)[=/](\d{5,})",
        r"[?&](?:product_id|item_id|goods_id|sku_id|id)=(\d{5,})",
        r"/(\d{8,})(?:[/?#]|$)",
    ):
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    fallback = re.findall(r"\d{8,}", url)
    return fallback[-1] if fallback else ""


def clamp(value: int, lower: int, upper: int) -> int:
    return max(lower, min(value, upper))


def coerce_attachment_list(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        return [value]
    text = safe_text(value)
    if not text:
        return []
    try:
        payload = json.loads(text)
    except ValueError:
        return []
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def extract_json_code_blocks(text: str) -> List[str]:
    blocks = [match.group(1).strip() for match in JSON_CODE_BLOCK_RE.finditer(text or "")]
    stripped = (text or "").strip()
    if not blocks and stripped.startswith(("{", "[")) and stripped.endswith(("}", "]")):
        blocks.append(stripped)
    return blocks


def extract_json_candidates(text: str) -> List[str]:
    candidates = []
    seen = set()
    for block in extract_json_code_blocks(text):
        if block not in seen:
            seen.add(block)
            candidates.append(block)

    decoder = JSONDecoder()
    raw_text = text or ""
    for index, char in enumerate(raw_text):
        if char not in "{[":
            continue
        try:
            _, end = decoder.raw_decode(raw_text[index:])
        except ValueError:
            continue
        candidate = raw_text[index : index + end].strip()
        if candidate and candidate not in seen:
            seen.add(candidate)
            candidates.append(candidate)
    return candidates


def write_csv_rows(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def first_number(value: Any) -> Optional[float]:
    numeric = safe_float(value)
    if numeric is None:
        return None
    return round(numeric, 4)


def build_work_id(batch_id: str, product_id: str) -> str:
    return "{batch_id}_{product_id}".format(batch_id=batch_id, product_id=product_id)


def build_followup_id(work_id: str) -> str:
    return "fup_{work_id}".format(work_id=work_id)


def coerce_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]
