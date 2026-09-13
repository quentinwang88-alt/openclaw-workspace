"""Low-cost structural checks around planned native-photo batches."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Sequence

from services.photo_flow_registry import (
    PhotoFlowRegistryError, flow_contract_from, resolve_required_roles,
    validate_ordered_roles,
)


class PhotoContentCheckError(ValueError):
    pass


def validate_prepared_sources(
    plan_item: Mapping[str, Any], sources: Sequence[Mapping[str, Any]],
    *, required_roles: Sequence[str] = (),
) -> None:
    try:
        planning_flow, item_roles = flow_contract_from(plan_item)
        expected_roles = resolve_required_roles(
            planning_flow=planning_flow,
            required_roles=required_roles or item_roles,
        )
        validate_ordered_roles(
            sources, planning_flow=planning_flow, required_roles=expected_roles,
        )
    except PhotoFlowRegistryError as exc:
        raise PhotoContentCheckError("生成结果" + str(exc)) from exc
    hashes = [str(item.get("sha256") or "") for item in sources]
    if (len(hashes) != len(expected_roles) or any(not value for value in hashes)
            or len(set(hashes)) != len(expected_roles)):
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


def validate_batch_sources(
    source_groups: Sequence[Sequence[Mapping[str, Any]]],
    *, required_roles: Sequence[str] = (), planning_flow: str = "",
) -> None:
    try:
        expected_roles = resolve_required_roles(
            planning_flow=planning_flow, required_roles=required_roles,
        )
    except PhotoFlowRegistryError as exc:
        raise PhotoContentCheckError(str(exc)) from exc
    signatures: set[tuple[str, ...]] = set()
    for index, sources in enumerate(source_groups, 1):
        try:
            validate_ordered_roles(
                sources, planning_flow=planning_flow,
                required_roles=expected_roles,
            )
        except PhotoFlowRegistryError as exc:
            raise PhotoContentCheckError(f"第 {index} 篇{exc}") from exc
        signature = tuple(str(item.get("sha256") or "") for item in sources)
        if signature in signatures:
            raise PhotoContentCheckError(
                f"第 {index} 篇与前一篇使用了完全相同的一组素材"
            )
        signatures.add(signature)
