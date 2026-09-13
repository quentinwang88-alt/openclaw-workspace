"""Small source-bound content contract, independent of captions/profile names."""
from __future__ import annotations

import copy
import hashlib
import json
import re
from typing import Any, Mapping


LABEL_PLACEHOLDER = re.compile(r"\{\{label_([a-d])\}\}")


def content_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _resolve_content_labels(value: Any, assets: list[Mapping[str, Any]]) -> Any:
    """Bind generic Recipe prose to the exact source photos being frozen."""
    by_role = {str(asset.get("role") or ""): asset for asset in assets}
    labels = {}
    for letter in "abcd":
        asset = by_role.get(f"look_{letter}") or {}
        raw = asset.get("content_label") or asset.get("display_label")
        if isinstance(raw, Mapping):
            raw = raw.get("zh-CN") or raw.get("zh")
        labels[letter] = raw.strip() if isinstance(raw, str) else f"造型 {letter.upper()}"

    def resolve(item: Any) -> Any:
        if isinstance(item, str):
            def replace(match: re.Match[str]) -> str:
                label = labels.get(match.group(1), "")
                return label
            return LABEL_PLACEHOLDER.sub(replace, item)
        if isinstance(item, Mapping):
            return {key: resolve(child) for key, child in item.items()}
        if isinstance(item, list):
            return [resolve(child) for child in item]
        return copy.deepcopy(item)

    return resolve(value)


def freeze_content_card(card: Mapping[str, Any], asset_set: Any, visual_rules: Mapping[str, Any]) -> dict:
    if not isinstance(card, Mapping):
        raise ValueError("NEEDS_CONTENT: 缺少逐页内容卡，不能仅凭 profile 名称生产")
    assets = asset_set.manifest_json.get("assets") or []
    card = _resolve_content_labels(card, assets)
    for key in ("summary_zh", "audience_zh", "question_zh", "comparison_basis_zh", "logic_key"):
        if not isinstance(card.get(key), str) or not card[key].strip():
            raise ValueError(f"NEEDS_CONTENT: content card requires {key}")
    if not isinstance(card.get("asset_gaps"), list) or card["asset_gaps"]:
        raise ValueError("NEEDS_ASSET: 内容卡的素材缺口必须明确且已补齐")
    pages = card.get("pages")
    expected_pages = 4 if card.get("travel_first_look_cover") is True else 5
    if not isinstance(pages, list) or len(pages) != expected_pages:
        raise ValueError(
            f"NEEDS_CONTENT: content card requires {expected_pages} page responsibilities"
        )
    by_role = {}
    for asset in assets:
        role = asset.get("role")
        if role in by_role:
            raise ValueError("NEEDS_ASSET: ambiguous source role")
        by_role[role] = asset
    used = set()
    for index, page in enumerate(pages, 1):
        if page.get("index") != index or not page.get("purpose_zh"):
            raise ValueError("NEEDS_CONTENT: each page needs ordered index and responsibility")
        roles = page.get("source_roles")
        count = {"single": 1, "split_vertical": 2, "grid_2x2": 4, "triptych_3": 3}.get(page.get("layout"))
        if (not isinstance(roles, list) or len(roles) != count or len(set(roles)) != len(roles)
                or any(role not in by_role for role in roles)):
            raise ValueError("NEEDS_ASSET: page layout/source roles mismatch")
        used.update(roles)
    approval = asset_set.manifest_json.get("content_approval") or {}
    if (approval.get("schema_version") != "opv-source-qualification-v1"
            or not approval.get("reviewer") or card["logic_key"] not in approval.get("allowed_logic_keys", [])):
        raise ValueError("NEEDS_CONTENT: source qualification does not permit this content logic")
    attributes = approval.get("attributes") or {}
    for role in used:
        asset = by_role[role]
        if approval.get("source_hashes", {}).get(asset["asset_id"]) != asset["sha256"]:
            raise ValueError("NEEDS_CONTENT: source qualification hash is missing or stale")
        if not isinstance(attributes.get(asset["asset_id"]), Mapping):
            raise ValueError("NEEDS_CONTENT: source role lacks verified attributes")
    props = {role: attributes[by_role[role]["asset_id"]] for role in used}
    supported = {"distinct_looks", "garment_relations", "layering_progression", "same_identity", "same_camera_scale", "show_full_body"}
    if any(value and key not in supported for key, value in visual_rules.items()):
        raise ValueError("NEEDS_CONTENT: unsupported visual rule must not be silently ignored")
    if visual_rules.get("distinct_looks"):
        looks = {(p.get("outerwear_id"), p.get("bottom_id")) for p in props.values()}
        if any(None in value for value in looks) or len(looks) < int(visual_rules["distinct_looks"]):
            raise ValueError("NEEDS_CONTENT: not enough verified distinct outfit combinations")
    for key, attr in (("same_identity", "identity_id"), ("same_camera_scale", "camera_scale")):
        if visual_rules.get(key):
            values = {p.get(attr) for p in props.values()}
            if None in values or len(values) != 1:
                raise ValueError(f"NEEDS_CONTENT: {key} is not evidenced")
    if visual_rules.get("show_full_body") and not all(p.get("full_body") is True for p in props.values()):
        raise ValueError("NEEDS_CONTENT: full-body framing is not evidenced")
    for relation in visual_rules.get("garment_relations") or []:
        roles = relation.get("roles") or []
        if relation.get("relation") != "same_outerwear_different_bottom" or len(roles) != 2 or any(r not in props for r in roles):
            raise ValueError("NEEDS_CONTENT: unsupported garment relation")
        left, right = [props[r] for r in roles]
        if (not left.get("outerwear_id") or left.get("outerwear_id") != right.get("outerwear_id")
                or not left.get("bottom_id") or not right.get("bottom_id") or left["bottom_id"] == right["bottom_id"]):
            raise ValueError("NEEDS_CONTENT: same outerwear / different bottom relation is not evidenced")
    if visual_rules.get("layering_progression"):
        roles = card.get("layering_roles") or []
        if len(roles) < 3 or any(r not in props for r in roles):
            raise ValueError("NEEDS_CONTENT: layering progression needs explicit base/mid/outer roles")
        layers = [set(props[r].get("upper_layers") or []) for r in roles]
        if not layers[0] or not all(a < b for a, b in zip(layers, layers[1:])):
            raise ValueError("NEEDS_CONTENT: changing bottoms cannot satisfy layering progression")
    frozen = copy.deepcopy(dict(card))
    frozen["sources"] = [{"role": role, "asset_id": by_role[role]["asset_id"], "sha256": by_role[role]["sha256"]} for role in sorted(used)]
    # Deliberately exclude title, copy_id, profile_id, market words and page order.
    frozen["content_signature"] = content_hash({"logic_key": card["logic_key"], "source_hashes": sorted({by_role[r]["sha256"] for r in used})})
    frozen["source_qualification_hash"] = content_hash(approval)
    return frozen
