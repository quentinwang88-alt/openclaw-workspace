from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def json_loads(value: Any, default: Any = None) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (list, dict, int, float, bool)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def stable_hash(*parts: Any, length: int = 12) -> str:
    raw = "\x1f".join(json_dumps(part) if isinstance(part, (dict, list)) else str(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def safe_slug(value: str, max_length: int = 42) -> str:
    text = re.sub(r"[^A-Za-z0-9_-]+", "_", str(value or "").strip()).strip("_")
    return (text or "product")[:max_length]


def normalized_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        parsed = json_loads(value, None)
        if isinstance(parsed, list):
            value = parsed
        else:
            value = [part.strip() for part in re.split(r"[,，;；\n]", value) if part.strip()]
    if not isinstance(value, Iterable) or isinstance(value, (bytes, dict)):
        value = [value]
    result: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text and text not in result:
            result.append(text)
    return result


def read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def writeable_parent(path: str | Path) -> Path:
    parent = Path(path).expanduser().resolve().parent
    parent.mkdir(parents=True, exist_ok=True)
    return parent


def category_matches(category: str, allowed: Any) -> bool:
    values = {item.lower() for item in normalized_list(allowed)}
    if not values or "*" in values:
        return True
    normalized = str(category or "").strip().lower()
    if normalized in values:
        return True
    aliases = {
        "上装": "top",
        "上衣": "top",
        "t恤": "tshirt",
        "短袖": "tshirt",
        "背心": "tank_top",
        "吊带": "tank_top",
        "针织衫": "knit_top",
        "针织": "knit_top",
        "衬衫": "shirt",
        "外套": "outerwear",
        "套装": "set",
        "连衣裙": "dress",
        "连体裤": "jumpsuit",
        "家居服": "homewear",
        "裤装": "pants",
    }
    alias = aliases.get(normalized)
    return bool(alias and alias in values)
