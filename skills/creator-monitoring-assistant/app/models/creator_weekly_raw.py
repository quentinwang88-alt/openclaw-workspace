from dataclasses import dataclass


@dataclass
class CreatorWeeklyRaw:
    import_batch_id: str
    stat_week: str
    source_file_name: str
    creator_name_raw: str
    platform: str
    country: str
    store: str
    gmv_raw: str
    refund_amount_raw: str
    order_count_raw: str
    sold_item_count_raw: str
    refunded_item_count_raw: str
    avg_order_value_raw: str
    avg_daily_sold_item_count_raw: str
    video_count_raw: str
    live_count_raw: str
    estimated_commission_raw: str
    shipped_sample_count_raw: str
    row_hash: str
