from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


@dataclass
class CreatorWeeklyMetrics:
    stat_week: str
    creator_id: int
    store: str
    gmv: Decimal
    order_count: int
    content_action_count: int
    video_count: int
    live_count: int
    shipped_sample_count: int
    refund_rate: Decimal
    commission_rate: Decimal
    gmv_per_action: Decimal
    gmv_per_sample: Decimal
    items_per_order: Decimal
    gmv_wow: Optional[Decimal]
    order_count_wow: Optional[Decimal]
    action_count_wow: Optional[Decimal]
    gmv_per_action_wow: Optional[Decimal]
    refund_rate_wow: Optional[Decimal]
    gmv_4w: Decimal
    order_count_4w: int
    action_count_4w: int
    avg_weekly_gmv_4w: Decimal
    avg_gmv_per_action_4w: Decimal
    avg_refund_rate_4w: Decimal
    gmv_lifetime: Decimal
    order_count_lifetime: int
    weeks_active_lifetime: int
    weeks_with_gmv_lifetime: int
    weeks_with_action_lifetime: int
    action_result_state: str
