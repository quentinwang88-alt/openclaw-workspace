"""Planning-flow metadata shared by native-photo planning and validation.

The registry deliberately owns only stable routing facts.  Business-specific
planning, supply and QA remain in their respective services, but every stage
can now resolve source roles from the same contract instead of embedding an
independent A/B/C/D assumption.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


REFERENCE_CONTRACT_FLOW = "reference_contract_v1"
TRAVEL_TWO_STEP_FLOW = "travel_two_step"
LAYERING_TWO_STEP_FLOW = "layering_two_step"
THERMAL_TRANSITION_TWO_STEP_FLOW = "thermal_transition_two_step"

DEFAULT_TRAVEL_SOURCE_ROLES = ("look_a", "look_b", "look_c", "look_d")


class PhotoFlowRegistryError(ValueError):
    pass


@dataclass(frozen=True)
class PhotoFlowHandler:
    """Immutable routing contract for one native-photo planning flow."""

    name: str
    default_source_roles: tuple[str, ...] = ()
    requires_explicit_source_roles: bool = False
    travel_semantics: bool = False
    # ``True`` for every flow whose source roles are an ordered layer stack
    # (``base → mid → outer``).  Downstream stages share the same subtractive
    # generation mechanics for these flows, so they must route on this flag
    # instead of comparing flow id strings in their own branch.
    layered_progression: bool = False

    def resolve_source_roles(self, required_roles: Sequence[Any] = ()) -> tuple[str, ...]:
        explicit = tuple(str(value or "").strip() for value in required_roles or ())
        roles = explicit or self.default_source_roles
        if not roles and self.requires_explicit_source_roles:
            raise PhotoFlowRegistryError(
                f"规划 flow {self.name} 必须由 recipe asset_requirements.required_roles 声明素材角色"
            )
        if not roles or any(not value for value in roles):
            raise PhotoFlowRegistryError(f"规划 flow {self.name} 的素材角色不能为空")
        if len(set(roles)) != len(roles):
            raise PhotoFlowRegistryError(f"规划 flow {self.name} 的素材角色不能重复")
        return roles


_FLOW_HANDLERS: dict[str, PhotoFlowHandler] = {
    REFERENCE_CONTRACT_FLOW: PhotoFlowHandler(
        name=REFERENCE_CONTRACT_FLOW,
        default_source_roles=DEFAULT_TRAVEL_SOURCE_ROLES,
    ),
    TRAVEL_TWO_STEP_FLOW: PhotoFlowHandler(
        name=TRAVEL_TWO_STEP_FLOW,
        default_source_roles=DEFAULT_TRAVEL_SOURCE_ROLES,
        travel_semantics=True,
    ),
    LAYERING_TWO_STEP_FLOW: PhotoFlowHandler(
        name=LAYERING_TWO_STEP_FLOW,
        requires_explicit_source_roles=True,
        layered_progression=True,
    ),
    THERMAL_TRANSITION_TWO_STEP_FLOW: PhotoFlowHandler(
        name=THERMAL_TRANSITION_TWO_STEP_FLOW,
        requires_explicit_source_roles=True,
        layered_progression=True,
    ),
}


def get_photo_flow_handler(flow: Any = "") -> PhotoFlowHandler:
    """Return a registered handler; blank is the historical choice flow."""
    name = str(flow or REFERENCE_CONTRACT_FLOW).strip() or REFERENCE_CONTRACT_FLOW
    handler = _FLOW_HANDLERS.get(name)
    if handler is None:
        raise PhotoFlowRegistryError(f"未知图文规划 flow：{name}")
    return handler


def resolve_required_roles(
    *, planning_flow: Any = "", required_roles: Sequence[Any] = (),
) -> tuple[str, ...]:
    return get_photo_flow_handler(planning_flow).resolve_source_roles(required_roles)


def is_layered_progression_flow(flow: Any = "") -> bool:
    """Return whether ``flow`` freezes an ordered layer stack as its roles.

    Unknown non-empty flow names stay a hard error: routing must never fall
    back silently to a different flow's semantics.
    """
    return get_photo_flow_handler(flow).layered_progression


def is_thermal_transition_flow(flow: Any = "") -> bool:
    """Return whether ``flow`` is the daily hot→cold transition line."""
    name = str(flow or REFERENCE_CONTRACT_FLOW).strip() or REFERENCE_CONTRACT_FLOW
    return name == THERMAL_TRANSITION_TWO_STEP_FLOW


def flow_contract_from(value: Mapping[str, Any] | None) -> tuple[str, tuple[str, ...]]:
    """Resolve flow + roles from a frozen plan/item while preserving old plans."""
    payload = dict(value or {})
    flow = str(
        payload.get("planning_flow")
        or dict(payload.get("style_profile") or {}).get("planning_flow")
        or REFERENCE_CONTRACT_FLOW
    )
    roles = resolve_required_roles(
        planning_flow=flow, required_roles=payload.get("required_roles") or (),
    )
    return flow, roles


def validate_ordered_roles(
    values: Sequence[Mapping[str, Any]], *, planning_flow: Any = "",
    required_roles: Sequence[Any] = (),
) -> tuple[str, ...]:
    """Validate exact ordered role identity and return the canonical roles."""
    expected = resolve_required_roles(
        planning_flow=planning_flow, required_roles=required_roles,
    )
    actual = tuple(str(item.get("role") or "") for item in values or ())
    if actual != expected:
        raise PhotoFlowRegistryError(
            "素材角色必须按冻结顺序提供：" + "/".join(expected)
        )
    return expected


def role_marker(role: Any, index: int) -> str:
    """Keep A/B/C/D presentation for legacy roles; name new roles directly."""
    value = str(role or "").strip()
    suffix = value.removeprefix("look_")
    return suffix.upper() if len(suffix) == 1 else value or str(index + 1)
