from dataclasses import dataclass
from typing import Optional


@dataclass
class CreatorMonitoringResult:
    stat_week: str
    creator_id: int
    store: str
    record_key: str
    primary_tag: str
    secondary_tags: str
    risk_tags: str
    priority_level: str
    rule_version: str
    decision_reason: str
    next_action: str
    owner: Optional[str] = None
