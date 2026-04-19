from dataclasses import dataclass
from decimal import Decimal


@dataclass
class CreatorWeeklyClean:
    stat_week: str
    creator_id: int
    import_batch_id: str
    store: str
    gmv: Decimal
    refund_amount: Decimal
    order_count: int
    sold_item_count: int
    refunded_item_count: int
    avg_order_value: Decimal
    avg_daily_sold_item_count: Decimal
    video_count: int
    live_count: int
    estimated_commission: Decimal
    shipped_sample_count: int
    content_action_count: int
    has_action: bool
    has_result: bool
    is_new_creator: bool
