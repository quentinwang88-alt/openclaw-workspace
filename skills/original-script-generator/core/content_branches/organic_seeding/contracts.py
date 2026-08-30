"""Business contracts owned exclusively by the organic-seeding branch."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Tuple


SEEDING_OBJECTIVES = frozenset(
    {
        "STYLE_MEMORY",
        "SCENE_ASSOCIATION",
        "CHOICE_EDUCATION",
        "DETAIL_APPRECIATION",
        "PERSONAL_POSITION",
        "DISCUSSION",
    }
)
EXPERIENCE_AUTHORITIES = frozenset(
    {"CURRENT_OBSERVATION", "OPERATOR_CONFIRMED_HISTORY", "NONE"}
)


@dataclass(frozen=True)
class OrganicSeedThemeContract:
    theme_id: str
    objective: str
    viewer_payoff: str
    lived_context: str
    taste_judgment: str
    product_role: str
    product_prominence: str
    memory_residue: str
    product_connection: str
    allowed_fact_refs: Tuple[str, ...] = field(default_factory=tuple)
    experience_authority: str = "NONE"
    confirmed_experience_facts: Tuple[str, ...] = field(default_factory=tuple)
    forbidden_expressions: Tuple[str, ...] = field(default_factory=tuple)
    interaction_ending_allowed: bool = True
    viewer_value_type: str = "AESTHETIC_OBSERVATION"
    rhetorical_family: str = "LIVED_DISCOVERY"
    hook_mechanism: str = "LIVED_MOMENT"
    closing_mode: str = "NATURAL_END"
    capture_mode: str = "ONE_FIXED_PHONE"
    angle_family: str = "GENERAL_OBSERVATION"
    scene_family: str = "DAILY_LIFE"
    action_family: str = "NATURAL_OBSERVATION"
    opening_relation: str = "LIVED_MOMENT_FIRST"
    proof_focus: str = "VISIBLE_PRODUCT_RELATION"
    creative_signature: str = ""
    operator_content_requirement: str = ""
    proof_anchor_refs: Tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.objective not in SEEDING_OBJECTIVES:
            raise ValueError(f"INVALID_SEEDING_OBJECTIVE:{self.objective}")
        if self.experience_authority not in EXPERIENCE_AUTHORITIES:
            raise ValueError(f"INVALID_EXPERIENCE_AUTHORITY:{self.experience_authority}")
        if self.experience_authority == "NONE" and self.confirmed_experience_facts:
            raise ValueError("EXPERIENCE_FACTS_REQUIRE_AUTHORITY")
        if not self.viewer_payoff or not self.product_connection:
            raise ValueError("SEED_THEME_INCOMPLETE")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
