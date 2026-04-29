#!/usr/bin/env python3
"""Heat, crowd, priority and price-band scoring for Market Insight v1."""

from __future__ import annotations

from collections import Counter
import math
from pathlib import Path
from typing import Iterable, List

from src.direction_metrics import (
    compute_heat_score_components,
    extract_content_efficiency_signals,
    extract_historical_sales_value,
    heat_score_mode_for_category,
    normalize_optional_metric,
    seasonal_trend_from_ratio,
)
from src.market_insight_models import MarketInsightConfig, MarketInsightProductTag, ProductRankingSnapshot, ScoredProductSnapshot


class MarketInsightScoringEngine(object):
    def __init__(self, taxonomy_dir: Path | None = None):
        self.taxonomy_dir = (
            Path(taxonomy_dir)
            if taxonomy_dir
            else Path(__file__).resolve().parents[1] / "configs" / "market_insight_taxonomies"
        )

    def score_products(
        self,
        snapshots: Iterable[ProductRankingSnapshot],
        tags: Iterable[MarketInsightProductTag],
        config: MarketInsightConfig,
    ) -> List[ScoredProductSnapshot]:
        snapshot_list = list(snapshots)
        tag_list = list(tags)
        if len(snapshot_list) != len(tag_list):
            raise ValueError("snapshots 和 tags 数量不一致")

        category = str(config.default_category or (snapshot_list[0].category if snapshot_list else "")).strip()
        heat_mode = heat_score_mode_for_category(category=category, taxonomy_dir=self.taxonomy_dir)
        sales_norm, gmv_norm = compute_heat_score_components(
            sales_values=[item.sales_7d for item in snapshot_list],
            gmv_values=[item.gmv_7d for item in snapshot_list],
            mode=heat_mode,
        )
        creator_norm = self._minmax([item.creator_count for item in snapshot_list])
        video_norm = self._minmax([item.video_count for item in snapshot_list])
        efficiency_values, efficiency_sources = extract_content_efficiency_signals(
            [self._efficiency_fields(item) for item in snapshot_list]
        )
        efficiency_signals = normalize_optional_metric(efficiency_values)

        results = []
        for index, snapshot in enumerate(snapshot_list):
            heat_score = round((0.7 * sales_norm[index] + 0.3 * gmv_norm[index]) * 100.0, 2)
            efficiency_signal = float(efficiency_signals[index] or 0.0)
            efficiency_source = str(efficiency_sources[index] or "missing")
            crowd_score = round(
                self._crowd_score(
                    creator_norm=creator_norm[index],
                    video_norm=video_norm[index],
                    efficiency_signal=efficiency_signal,
                    efficiency_source=efficiency_source,
                )
                * 100.0,
                2,
            )
            heat_level = self._score_level(heat_score)
            crowd_level = self._score_level(crowd_score)
            priority_level = self._priority_level(heat_level, crowd_level, tag_list[index])
            seasonal_trend_short = self._seasonal_trend(snapshot=snapshot, category=category, horizon="short")
            seasonal_trend_long = self._seasonal_trend(snapshot=snapshot, category=category, horizon="long")
            results.append(
                ScoredProductSnapshot(
                    snapshot=snapshot,
                    tag=tag_list[index],
                    heat_score=heat_score,
                    heat_level=heat_level,
                    crowd_score=crowd_score,
                    crowd_level=crowd_level,
                    priority_level=priority_level,
                    target_price_band=self._price_band(snapshot.price_mid, snapshot.category, config),
                    seasonal_trend=seasonal_trend_long,
                    seasonal_trend_short=seasonal_trend_short,
                    seasonal_trend_long=seasonal_trend_long,
                    content_efficiency_signal=round(efficiency_signal, 4),
                    content_efficiency_source=efficiency_source,
                    market_id=snapshot.market_id or snapshot.country,
                    category_id=snapshot.category_id or snapshot.category,
                )
            )
        return results

    def _efficiency_fields(self, snapshot: ProductRankingSnapshot) -> dict:
        fields = dict(snapshot.raw_fields or {})
        sales_7d = float(snapshot.sales_7d or 0.0)
        video_count = float(snapshot.video_count or 0.0)
        creator_count = float(snapshot.creator_count or 0.0)
        if video_count > 0 and "sales_per_video" not in fields:
            fields["sales_per_video"] = round(sales_7d / video_count, 6)
        if creator_count > 0 and "sales_per_creator" not in fields:
            fields["sales_per_creator"] = round(sales_7d / creator_count, 6)
        return fields

    def summarize_top_terms(self, items: Iterable[ScoredProductSnapshot], key: str, top_k: int = 3) -> List[str]:
        counter = Counter()
        for item in items:
            values = getattr(item.tag, key, [])
            if isinstance(values, list):
                counter.update(values)
            elif values:
                counter.update([values])
        return [name for name, _ in counter.most_common(top_k)]

    def _score_level(self, score: float) -> str:
        if score >= 67:
            return "high"
        if score >= 34:
            return "medium"
        return "low"

    def _priority_level(self, heat_level: str, crowd_level: str, tag: MarketInsightProductTag) -> str:
        if not tag.is_valid_sample:
            return "low"
        if heat_level == "high" and crowd_level in {"low", "medium"}:
            return "high"
        if heat_level == "high" or (heat_level == "medium" and crowd_level != "high"):
            return "medium"
        return "low"

    def _minmax(self, values: List[float]) -> List[float]:
        if not values:
            return []
        minimum = min(values)
        maximum = max(values)
        if maximum <= minimum:
            return [0.5 for _ in values]
        return [(value - minimum) / (maximum - minimum) for value in values]

    def _crowd_score(
        self,
        creator_norm: float,
        video_norm: float,
        efficiency_signal: float,
        efficiency_source: str,
    ) -> float:
        efficiency_norm = max(0.0, min(100.0, float(efficiency_signal or 0.0))) / 100.0
        if efficiency_source == "primary":
            return 0.50 * creator_norm + 0.30 * video_norm + 0.20 * efficiency_norm
        if efficiency_source == "proxy":
            return 0.55 * creator_norm + 0.35 * video_norm + 0.10 * efficiency_norm
        return 0.60 * creator_norm + 0.40 * video_norm

    def _seasonal_trend(self, snapshot: ProductRankingSnapshot, category: str, horizon: str) -> str:
        historical = extract_historical_sales_value(
            raw_fields=dict(snapshot.raw_fields or {}),
            category=category,
            horizon=horizon,
            taxonomy_dir=self.taxonomy_dir,
        )
        if historical is None or historical <= 0:
            return "unclear"
        current = float(snapshot.sales_7d or 0.0)
        return seasonal_trend_from_ratio(current / historical if historical > 0 else None)

    def _price_band(self, price_mid, category: str, config: MarketInsightConfig) -> str:
        if price_mid is None:
            return "unknown"
        cny_value = self._to_cny(price_mid, config)
        if cny_value is None:
            return "unknown"
        step = self._price_band_step_rmb(category=category, config=config)
        lower = math.floor(max(cny_value, 0.0) / step) * step
        upper = lower + step
        return "{left:g}-{right:g} RMB".format(left=lower, right=upper)

    def _to_cny(self, price_mid, config: MarketInsightConfig):
        try:
            value = float(price_mid)
        except (TypeError, ValueError):
            return None
        if value < 0:
            return None
        if config.price_to_cny_rate and config.price_to_cny_rate > 0:
            return round(value * config.price_to_cny_rate, 4)
        scaled = value / max(config.price_scale_divisor, 1.0)
        return round(scaled, 4)

    def _price_band_step_rmb(self, category: str, config: MarketInsightConfig) -> float:
        if config.price_band_step_rmb and config.price_band_step_rmb > 0:
            return float(config.price_band_step_rmb)
        normalized = str(category or "").strip()
        if normalized in {"hair_accessory", "fashion_accessory", "accessory", "small_goods"}:
            return 5.0
        if normalized in {"light_tops", "tops", "clothing", "apparel"}:
            return 20.0
        return 10.0
