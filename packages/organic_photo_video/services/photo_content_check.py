"""Low-cost structural checks around planned native-photo batches."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Sequence


class PhotoContentCheckError(ValueError):
    pass


def validate_prepared_sources(
    plan_item: Mapping[str, Any], sources: Sequence[Mapping[str, Any]],
) -> None:
    roles = [str(item.get("role") or "") for item in sources]
    if roles != ["look_a", "look_b", "look_c", "look_d"]:
        raise PhotoContentCheckError("生成结果没有按 A/B/C/D 返回四套穿搭")
    hashes = [str(item.get("sha256") or "") for item in sources]
    if any(not value for value in hashes) or len(set(hashes)) != 4:
        raise PhotoContentCheckError("同一篇中存在空图片或完全重复图片")
    planned = list(plan_item.get("looks") or [])
    if not planned:
        return
    expected = {
        str(item.get("role") or ""): str(item.get("planned_look_signature") or "")
        for item in sources
    }
    for look in planned:
        role = str(look.get("role") or "")
        expected_signature = hashlib.sha256(json.dumps({
            key: str(look.get(key) or "")
            for key in ("role", "outerwear", "top_inner", "bottom", "shoes")
        }, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
        if expected.get(role) != expected_signature:
            raise PhotoContentCheckError(f"{role} 生成结果与冻结内容计划不一致")


def validate_batch_sources(source_groups: Sequence[Sequence[Mapping[str, Any]]]) -> None:
    signatures: set[tuple[str, ...]] = set()
    for index, sources in enumerate(source_groups, 1):
        signature = tuple(str(item.get("sha256") or "") for item in sources)
        if signature in signatures:
            raise PhotoContentCheckError(f"第 {index} 篇与前一篇使用了完全相同的四张素材")
        signatures.add(signature)
