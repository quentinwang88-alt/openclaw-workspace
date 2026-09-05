"""Canonical hashes and deterministic identifiers used for idempotency."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Iterable


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_hash(*parts: Any) -> str:
    payload = canonical_json(parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def source_hash(source_script: str, source_product_fact: Any) -> str:
    """Version a mother when either the immutable script or source facts change."""
    return stable_hash(source_script.strip(), source_product_fact)


def product_fact_hash(product_id: str, image_refs: Iterable[str], notes: str) -> str:
    return stable_hash(product_id.strip(), sorted(str(item).strip() for item in image_refs), notes.strip())


def normalize_prompt(prompt: str) -> str:
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in prompt.strip().splitlines()]
    return "\n".join(line for line in lines if line)


def prompt_hash(prompt: str) -> str:
    return hashlib.sha256(normalize_prompt(prompt).encode("utf-8")).hexdigest()


def creative_signature_hash(signature: Any) -> str:
    """Hash the creative surface while excluding shared asset boilerplate."""
    if hasattr(signature, "model_dump"):
        signature = signature.model_dump(mode="json")
    return stable_hash(signature)


def batch_signature(
    mother_id: str,
    mother_version: int,
    product_ids: Iterable[str],
    special_requirements: str = "",
    per_product_count: int | None = None,
    planner_version: str = "v1",
    publish_purpose: str = "带货",
    cart_enabled: bool = True,
) -> str:
    parts = (
        mother_id,
        mother_version,
        sorted({str(item).strip() for item in product_ids if str(item).strip()}),
        special_requirements.strip(),
        per_product_count,
        planner_version,
    )
    # Keep historical commercial identifiers stable. Cart affects the frozen
    # batch snapshot, but never the cumulative sequence namespace.
    if publish_purpose == "带货" and cart_enabled:
        return stable_hash(*parts)
    return stable_hash(*parts, publish_purpose, cart_enabled)


def deterministic_id(prefix: str, *parts: Any, size: int = 24) -> str:
    return f"{prefix}_{stable_hash(*parts)[:size]}"
