"""Small branch registry; executors depend on ports, not branch internals."""
from __future__ import annotations

from typing import Dict, Protocol, Tuple

from .contracts import PublishPolicyContract


class ContentBranchPlugin(Protocol):
    key: str
    script_type: str

    def publish_policy(self) -> PublishPolicyContract: ...

    def runtime_db_name(self) -> str: ...


_REGISTRY: Dict[str, ContentBranchPlugin] = {}


def register_branch(plugin: ContentBranchPlugin, *, replace: bool = False) -> None:
    key = str(plugin.key or "").strip().upper()
    if not key:
        raise ValueError("CONTENT_BRANCH_KEY_REQUIRED")
    if key in _REGISTRY and not replace:
        raise ValueError(f"CONTENT_BRANCH_ALREADY_REGISTERED:{key}")
    _REGISTRY[key] = plugin


def register_builtin_branches() -> None:
    from .direct_response import DirectResponseBranch
    from .organic_seeding import OrganicSeedingBranch

    for plugin in (DirectResponseBranch(), OrganicSeedingBranch()):
        if plugin.key not in _REGISTRY:
            register_branch(plugin)


def get_branch(key: str) -> ContentBranchPlugin:
    register_builtin_branches()
    normalized = str(key or "").strip().upper()
    try:
        return _REGISTRY[normalized]
    except KeyError as exc:
        raise KeyError(f"UNKNOWN_CONTENT_BRANCH:{normalized}") from exc


def list_branches() -> Tuple[str, ...]:
    register_builtin_branches()
    return tuple(sorted(_REGISTRY))
