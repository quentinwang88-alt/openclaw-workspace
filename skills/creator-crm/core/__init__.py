"""Creator CRM 核心 Skills 模块"""

from .assign_campaign_group import assign_campaign_group
from .analyze_creator_vibe import analyze_creator_vibe
from .analyze_creator_vibe_v2 import analyze_creator_vibe_v2

__all__ = [
    "assign_campaign_group",
    "analyze_creator_vibe",
    "analyze_creator_vibe_v2",  # 极简成本方案
]
