from dataclasses import dataclass
from typing import Optional


@dataclass
class CreatorMaster:
    creator_key: str
    creator_name: str
    platform: str
    country: str
    store: str = ""
    first_seen_week: Optional[str] = None
    latest_seen_week: Optional[str] = None
    owner: Optional[str] = None
    status: str = "active"
    notes: Optional[str] = None
