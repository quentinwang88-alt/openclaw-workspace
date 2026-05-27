from __future__ import annotations

import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class SegmentTypeRule:
    risk_level: str
    default_roles: List[str]
    possible_roles: List[str]
    core_allowed: str  # bool or "conditional"
    anchor_strength: str
    require_reference_image: bool
    preferred_generation_type: str
    batch_friendly: str  # bool or "medium"/"low"
    require_frame_consistency: bool = False
    require_tier3_review_for_core: bool = False
    prompt_cn: str = ""
    prompt_description: str = ""
    prompt_scene_default: str = ""
    prompt_action_default: str = ""


@dataclass
class CategoryLightRule:
    forbidden: List[str] = field(default_factory=list)


@dataclass
class EffectiveRoleCondition:
    condition: Dict[str, Any]
    roles: Any  # str or list


@dataclass
class ExportGradeLevel:
    label: str
    description: str
    conditions: List[Dict[str, Any]]


@dataclass
class AISegmentFactoryConfig:
    global_rules: Dict[str, Any] = field(default_factory=dict)
    segment_type_rules: Dict[str, SegmentTypeRule] = field(default_factory=dict)
    category_light_rules: Dict[str, CategoryLightRule] = field(default_factory=dict)
    effective_role_rules: Dict[str, List[EffectiveRoleCondition]] = field(default_factory=dict)
    export_grading: Dict[str, ExportGradeLevel] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path | None = None) -> "AISegmentFactoryConfig":
        if path is None:
            this_dir = Path(__file__).parent
            candidates = [
                this_dir.parent.parent / "config" / "ai_segment_factory_config.yaml",
                this_dir.parent / "config" / "ai_segment_factory_config.yaml",
                Path.cwd() / "config" / "ai_segment_factory_config.yaml",
            ]
            for candidate in candidates:
                if candidate.exists():
                    path = candidate
                    break
            if path is None:
                return cls()
        if not path.exists():
            return cls()

        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        segment_type_rules = {}
        for key, val in (data.get("segment_type_rules") or {}).items():
            prompt = val.get("prompt") or {}
            segment_type_rules[key] = SegmentTypeRule(
                risk_level=val.get("risk_level", "low"),
                default_roles=val.get("default_roles", []),
                possible_roles=val.get("possible_roles", []),
                core_allowed=_to_bool_or_str(val.get("core_allowed", False)),
                anchor_strength=val.get("anchor_strength", "soft"),
                require_reference_image=val.get("require_reference_image", False),
                preferred_generation_type=val.get("preferred_generation_type", "text_to_video"),
                batch_friendly=_to_bool_or_str(val.get("batch_friendly", True)),
                require_frame_consistency=val.get("require_frame_consistency", False),
                require_tier3_review_for_core=val.get("require_tier3_review_for_core", False),
                prompt_cn=prompt.get("cn", key),
                prompt_description=prompt.get("description", ""),
                prompt_scene_default=prompt.get("scene_default", ""),
                prompt_action_default=prompt.get("action_default", ""),
            )

        category_light_rules = {}
        for key, val in (data.get("category_light_rules") or {}).items():
            category_light_rules[key] = CategoryLightRule(forbidden=val.get("forbidden", []))

        effective_role_rules = {}
        for key, rules in (data.get("effective_role_rules") or {}).items():
            effective_role_rules[key] = [EffectiveRoleCondition(condition=r.get("condition", {}), roles=r.get("roles", [])) for r in rules]

        export_grading = {}
        for key, val in (data.get("export_grading") or {}).items():
            export_grading[key] = ExportGradeLevel(
                label=val.get("label", key),
                description=val.get("description", ""),
                conditions=val.get("conditions", []),
            )

        return cls(
            global_rules=data.get("global_rules", {}),
            segment_type_rules=segment_type_rules,
            category_light_rules=category_light_rules,
            effective_role_rules=effective_role_rules,
            export_grading=export_grading,
        )

    def get_segment_type_rule(self, segment_type: str) -> SegmentTypeRule:
        return self.segment_type_rules.get(segment_type, SegmentTypeRule(
            risk_level="medium", default_roles=["scene", "ending"],
            possible_roles=["scene", "ending"], core_allowed=False,
            anchor_strength="soft", require_reference_image=False,
            preferred_generation_type="text_to_video", batch_friendly=True,
        ))

    def get_category_forbidden(self, category: str) -> List[str]:
        rule = self.category_light_rules.get(category)
        return rule.forbidden if rule else []

    def classify_grade(self, anchor_match_level: str, frame_consistency_status: str, product_visibility: str, risk_level: str, mixcut_usability: str) -> str:
        if anchor_match_level == "strict_pass" and frame_consistency_status == "pass" and product_visibility in {"high", "medium"} and risk_level == "low" and mixcut_usability == "yes":
            return "A_core"
        if anchor_match_level in {"soft_pass", "uncertain"} and mixcut_usability == "yes" and risk_level in {"low", "medium"}:
            return "B_scene"
        if anchor_match_level == "uncertain":
            return "C_reference"
        return "D_reject"


def _to_bool_or_str(val: Any) -> Any:
    if val is True:
        return "yes"
    if val is False:
        return "no"
    return str(val)


_DEFAULT_CONFIG: AISegmentFactoryConfig | None = None


def get_config(path: Path | None = None) -> AISegmentFactoryConfig:
    global _DEFAULT_CONFIG
    if _DEFAULT_CONFIG is None or path is not None:
        _DEFAULT_CONFIG = AISegmentFactoryConfig.load(path)
    return _DEFAULT_CONFIG
