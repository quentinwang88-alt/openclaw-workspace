"""Branch-neutral contracts.

This module deliberately contains no selling-point or organic-theme semantics.
Those concepts belong to their respective bounded contexts.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, Tuple


DIRECT_RESPONSE = "DIRECT_RESPONSE"
SEEDING_ORGANIC = "SEEDING_ORGANIC"
KNOWN_BRANCHES = frozenset({DIRECT_RESPONSE, SEEDING_ORGANIC})


def stable_id(prefix: str, material: Any) -> str:
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, default=str)
    return prefix + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24].upper()


def stable_hash(material: Any) -> str:
    return stable_id("", material).lower()


def _clean_tuple(values: Iterable[Any]) -> Tuple[str, ...]:
    return tuple(str(value or "").strip() for value in values if str(value or "").strip())


@dataclass(frozen=True)
class BranchVersions:
    shared_kernel_version: str
    branch_policy_version: str
    branch_prompt_version: str

    def fingerprint(self, branch_key: str) -> str:
        return stable_hash({"branch_key": branch_key, **asdict(self)})


@dataclass(frozen=True)
class VisualIntentContract:
    """The only business-to-visual handoff shared by all branches."""

    branch_key: str
    intent_id: str
    audience_job: str
    product_role: str
    product_prominence: str
    opening_subject: str
    carrier_requirements: Tuple[str, ...] = field(default_factory=tuple)
    allowed_fact_refs: Tuple[str, ...] = field(default_factory=tuple)
    forbidden_inferences: Tuple[str, ...] = field(default_factory=tuple)
    experience_authority: str = "NONE"
    structure_compatibility: Tuple[str, ...] = field(default_factory=tuple)
    branch_payload: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.branch_key not in KNOWN_BRANCHES:
            raise ValueError(f"UNKNOWN_CONTENT_BRANCH:{self.branch_key}")
        if not self.intent_id or not self.audience_job:
            raise ValueError("VISUAL_INTENT_INCOMPLETE")
        if self.product_role not in {"HERO", "SUPPORTING", "INCIDENTAL"}:
            raise ValueError(f"INVALID_PRODUCT_ROLE:{self.product_role}")
        if self.product_prominence not in {"EARLY", "MID", "LATE"}:
            raise ValueError(f"INVALID_PRODUCT_PROMINENCE:{self.product_prominence}")

    @classmethod
    def create(cls, **values: Any) -> "VisualIntentContract":
        for key in (
            "carrier_requirements",
            "allowed_fact_refs",
            "forbidden_inferences",
            "structure_compatibility",
        ):
            values[key] = _clean_tuple(values.get(key) or ())
        return cls(**values)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PublishPolicyContract:
    branch_key: str
    script_type: str
    publish_purpose: str
    cart_policy: str
    product_id_transport: str
    shoppable_endpoint_allowed: bool
    policy_version: str

    def __post_init__(self) -> None:
        if self.branch_key not in KNOWN_BRANCHES:
            raise ValueError(f"UNKNOWN_CONTENT_BRANCH:{self.branch_key}")
        if self.cart_policy not in {"ALLOWED", "FORBIDDEN"}:
            raise ValueError(f"INVALID_CART_POLICY:{self.cart_policy}")
        if self.cart_policy == "FORBIDDEN" and self.shoppable_endpoint_allowed:
            raise ValueError("FORBIDDEN_CART_CANNOT_USE_SHOPPABLE_ENDPOINT")

    @property
    def cart_enabled_text(self) -> str:
        return "否" if self.cart_policy == "FORBIDDEN" else "是"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def assert_safe_for_sync(self) -> None:
        if self.branch_key != SEEDING_ORGANIC:
            return
        required = (self.script_type, self.publish_purpose, self.policy_version)
        if not all(str(value or "").strip() for value in required):
            raise ValueError("SEEDING_PUBLISH_POLICY_UNAVAILABLE")
        if self.cart_policy != "FORBIDDEN":
            raise ValueError("SEEDING_CART_GUARD_MISSING")
        if self.shoppable_endpoint_allowed:
            raise ValueError("SEEDING_SHOPPABLE_ROUTE_FORBIDDEN")
