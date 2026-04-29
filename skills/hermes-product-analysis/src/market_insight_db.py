#!/usr/bin/env python3
"""SQLite storage for Market Insight runs and direction cards."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List

from src.market_insight_models import (
    MarketDirectionCard,
    MarketInsightProductRunState,
    ProductRankingSnapshot,
    ScoredProductSnapshot,
)
from src.models import StorePositioningCard


class MarketInsightDatabase(object):
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)

    def upsert_direction_card_run(
        self,
        table_id: str,
        run_state: MarketInsightProductRunState,
        snapshots: Iterable[ProductRankingSnapshot],
        scored_items: Iterable[ScoredProductSnapshot],
        direction_cards: Iterable[MarketDirectionCard],
        completed_product_count: int,
        total_product_count: int,
        run_status: str,
        updated_at_epoch: int,
        report_json_path: str,
        report_md_path: str,
        progress_json_path: str,
        report_payload: Dict[str, object],
        report_delivery: Dict[str, object],
        llm_fallback_count: int,
        source_scope: str,
        min_consumable_product_count: int,
        min_consumable_direction_count: int,
    ) -> None:
        snapshot_list = list(snapshots)
        scored_list = list(scored_items)
        cards = list(direction_cards)
        run_id = str(run_state.artifacts_dir)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(self.db_path)) as conn:
            self._ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO market_insight_runs (
                    run_id, table_id, batch_date, country, category, artifacts_dir,
                    report_json_path, report_md_path, progress_json_path,
                    completed_product_count, total_product_count, direction_count,
                    voc_status, run_status, is_latest, updated_at_epoch,
                    source_scope, min_consumable_product_count, min_consumable_direction_count,
                    structured_report_json, report_delivery_json, feishu_doc_token, feishu_doc_url,
                    notification_status, llm_fallback_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    table_id=excluded.table_id,
                    batch_date=excluded.batch_date,
                    country=excluded.country,
                    category=excluded.category,
                    artifacts_dir=excluded.artifacts_dir,
                    report_json_path=excluded.report_json_path,
                    report_md_path=excluded.report_md_path,
                    progress_json_path=excluded.progress_json_path,
                    completed_product_count=excluded.completed_product_count,
                    total_product_count=excluded.total_product_count,
                    direction_count=excluded.direction_count,
                    voc_status=excluded.voc_status,
                    run_status=excluded.run_status,
                    updated_at_epoch=excluded.updated_at_epoch,
                    source_scope=excluded.source_scope,
                    min_consumable_product_count=excluded.min_consumable_product_count,
                    min_consumable_direction_count=excluded.min_consumable_direction_count,
                    structured_report_json=excluded.structured_report_json,
                    report_delivery_json=excluded.report_delivery_json,
                    feishu_doc_token=excluded.feishu_doc_token,
                    feishu_doc_url=excluded.feishu_doc_url,
                    notification_status=excluded.notification_status,
                    llm_fallback_count=excluded.llm_fallback_count
                """,
                (
                    run_id,
                    table_id,
                    run_state.batch_date,
                    run_state.country,
                    run_state.category,
                    run_state.artifacts_dir,
                    report_json_path,
                    report_md_path,
                    progress_json_path,
                    int(completed_product_count),
                    int(total_product_count),
                    len(cards),
                    run_state.voc_status,
                    run_status,
                    0,
                    int(updated_at_epoch),
                    str(source_scope or "official"),
                    int(min_consumable_product_count or 0),
                    int(min_consumable_direction_count or 0),
                    json.dumps(report_payload, ensure_ascii=False),
                    json.dumps(report_delivery, ensure_ascii=False),
                    str(report_delivery.get("feishu_doc_token", "") or ""),
                    str(report_delivery.get("feishu_doc_url", "") or ""),
                    str(report_delivery.get("notification_status", "") or ""),
                    int(llm_fallback_count or 0),
                ),
            )

            if run_status == "completed":
                conn.execute(
                    """
                    UPDATE market_insight_runs
                    SET is_latest = CASE WHEN run_id = ? THEN 1 ELSE 0 END
                    WHERE country = ? AND category = ?
                    """,
                    (run_id, run_state.country, run_state.category),
                )

            snapshot_row_keys = [self._snapshot_row_key(snapshot) for snapshot in snapshot_list]
            if snapshot_row_keys:
                placeholders = ",".join(["?"] * len(snapshot_row_keys))
                conn.execute(
                    "DELETE FROM market_insight_product_snapshots WHERE run_id = ? AND product_row_key NOT IN ({placeholders})".format(
                        placeholders=placeholders
                    ),
                    [run_id] + snapshot_row_keys,
                )
                conn.execute(
                    "DELETE FROM market_insight_product_tags WHERE run_id = ? AND product_row_key NOT IN ({placeholders})".format(
                        placeholders=placeholders
                    ),
                    [run_id] + snapshot_row_keys,
                )
            else:
                conn.execute("DELETE FROM market_insight_product_snapshots WHERE run_id = ?", (run_id,))
                conn.execute("DELETE FROM market_insight_product_tags WHERE run_id = ?", (run_id,))

            for snapshot in snapshot_list:
                conn.execute(
                    """
                    INSERT INTO market_insight_product_snapshots (
                        run_id, product_row_key, batch_date, country, category, product_id,
                        product_name, shop_name, price_min, price_max, price_mid,
                        sales_7d, gmv_7d, creator_count, video_count, listing_days,
                        image_url, product_url, rank_index, raw_category, raw_fields_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, product_row_key) DO UPDATE SET
                        batch_date=excluded.batch_date,
                        country=excluded.country,
                        category=excluded.category,
                        product_id=excluded.product_id,
                        product_name=excluded.product_name,
                        shop_name=excluded.shop_name,
                        price_min=excluded.price_min,
                        price_max=excluded.price_max,
                        price_mid=excluded.price_mid,
                        sales_7d=excluded.sales_7d,
                        gmv_7d=excluded.gmv_7d,
                        creator_count=excluded.creator_count,
                        video_count=excluded.video_count,
                        listing_days=excluded.listing_days,
                        image_url=excluded.image_url,
                        product_url=excluded.product_url,
                        rank_index=excluded.rank_index,
                        raw_category=excluded.raw_category,
                        raw_fields_json=excluded.raw_fields_json
                    """,
                    (
                        run_id,
                        self._snapshot_row_key(snapshot),
                        snapshot.batch_date,
                        snapshot.country,
                        snapshot.category,
                        snapshot.product_id,
                        snapshot.product_name,
                        snapshot.shop_name,
                        self._nullable_float(snapshot.price_min),
                        self._nullable_float(snapshot.price_max),
                        self._nullable_float(snapshot.price_mid),
                        float(snapshot.sales_7d),
                        float(snapshot.gmv_7d),
                        float(snapshot.creator_count),
                        float(snapshot.video_count),
                        self._nullable_int(snapshot.listing_days),
                        snapshot.image_url,
                        snapshot.product_url,
                        int(snapshot.rank_index),
                        snapshot.raw_category,
                        json.dumps(snapshot.raw_fields, ensure_ascii=False),
                    ),
                )

            for scored in scored_list:
                snapshot = scored.snapshot
                tag = scored.tag
                conn.execute(
                    """
                    INSERT INTO market_insight_product_tags (
                        run_id, product_row_key, product_id, rank_index,
                        is_valid_sample, style_cluster, style_tags_secondary_json,
                        product_form, length_form, element_tags_json, value_points_json,
                        scene_tags_json, reason_short, heat_score, heat_level,
                        crowd_score, crowd_level, priority_level, target_price_band,
                        direction_canonical_key, direction_family, direction_tier,
                        seasonal_trend, seasonal_trend_short, seasonal_trend_long,
                        content_efficiency_signal, content_efficiency_source,
                        default_content_route_preference
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, product_row_key) DO UPDATE SET
                        product_id=excluded.product_id,
                        rank_index=excluded.rank_index,
                        is_valid_sample=excluded.is_valid_sample,
                        style_cluster=excluded.style_cluster,
                        style_tags_secondary_json=excluded.style_tags_secondary_json,
                        product_form=excluded.product_form,
                        length_form=excluded.length_form,
                        element_tags_json=excluded.element_tags_json,
                        value_points_json=excluded.value_points_json,
                        scene_tags_json=excluded.scene_tags_json,
                        reason_short=excluded.reason_short,
                        heat_score=excluded.heat_score,
                        heat_level=excluded.heat_level,
                        crowd_score=excluded.crowd_score,
                        crowd_level=excluded.crowd_level,
                        priority_level=excluded.priority_level,
                        target_price_band=excluded.target_price_band,
                        direction_canonical_key=excluded.direction_canonical_key,
                        direction_family=excluded.direction_family,
                        direction_tier=excluded.direction_tier,
                        seasonal_trend=excluded.seasonal_trend,
                        seasonal_trend_short=excluded.seasonal_trend_short,
                        seasonal_trend_long=excluded.seasonal_trend_long,
                        content_efficiency_signal=excluded.content_efficiency_signal,
                        content_efficiency_source=excluded.content_efficiency_source,
                        default_content_route_preference=excluded.default_content_route_preference
                    """,
                    (
                        run_id,
                        self._snapshot_row_key(snapshot),
                        snapshot.product_id,
                        int(snapshot.rank_index),
                        1 if tag.is_valid_sample else 0,
                        tag.style_cluster,
                        json.dumps(tag.style_tags_secondary, ensure_ascii=False),
                        tag.product_form,
                        tag.length_form,
                        json.dumps(tag.element_tags, ensure_ascii=False),
                        json.dumps(tag.value_points, ensure_ascii=False),
                        json.dumps(tag.scene_tags, ensure_ascii=False),
                        tag.reason_short,
                        float(scored.heat_score),
                        scored.heat_level,
                        float(scored.crowd_score),
                        scored.crowd_level,
                        scored.priority_level,
                        scored.target_price_band,
                        scored.direction_canonical_key,
                        scored.direction_family,
                        scored.direction_tier,
                        scored.seasonal_trend,
                        scored.seasonal_trend_short,
                        scored.seasonal_trend_long,
                        float(scored.content_efficiency_signal or 0.0),
                        scored.content_efficiency_source,
                        scored.default_content_route_preference,
                    ),
                )

            card_keys = [str(card.direction_canonical_key or "").strip() for card in cards if str(card.direction_canonical_key or "").strip()]
            if card_keys:
                placeholders = ",".join(["?"] * len(card_keys))
                conn.execute(
                    "DELETE FROM market_direction_cards WHERE run_id = ? AND direction_canonical_key NOT IN ({placeholders})".format(
                        placeholders=placeholders
                    ),
                    [run_id] + card_keys,
                )
            else:
                conn.execute("DELETE FROM market_direction_cards WHERE run_id = ?", (run_id,))

            for card in cards:
                conn.execute(
                    """
                    INSERT INTO market_direction_cards (
                        run_id, direction_canonical_key, direction_instance_id,
                        batch_date, country, category, direction_name,
                        style_cluster, direction_family, direction_item_count,
                        direction_sales_median_7d, direction_video_density_avg, direction_creator_density_avg,
                        direction_tier, seasonal_trend, seasonal_trend_short, seasonal_trend_long,
                        content_efficiency_signal, content_efficiency_source,
                        confidence_sample_score, confidence_consistency_score, confidence_completeness_score,
                        decision_confidence, confidence_reason_tags_json,
                        top_forms_json, form_distribution_json,
                        form_distribution_by_count_json, form_distribution_by_sales_json,
                        top_silhouette_forms_json, top_length_forms_json,
                        silhouette_distribution_by_count_json, silhouette_distribution_by_sales_json,
                        length_distribution_by_count_json, length_distribution_by_sales_json,
                        core_elements_json, scene_tags_json, target_price_bands_json,
                        heat_level, crowd_level, top_value_points_json,
                        default_content_route_preference,
                        representative_products_json, priority_level,
                        selection_advice, avoid_notes, confidence,
                        product_count, average_heat_score, average_crowd_score,
                        direction_key
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, direction_canonical_key) DO UPDATE SET
                        direction_instance_id=excluded.direction_instance_id,
                        batch_date=excluded.batch_date,
                        country=excluded.country,
                        category=excluded.category,
                        direction_name=excluded.direction_name,
                        style_cluster=excluded.style_cluster,
                        direction_family=excluded.direction_family,
                        direction_item_count=excluded.direction_item_count,
                        direction_sales_median_7d=excluded.direction_sales_median_7d,
                        direction_video_density_avg=excluded.direction_video_density_avg,
                        direction_creator_density_avg=excluded.direction_creator_density_avg,
                        direction_tier=excluded.direction_tier,
                        seasonal_trend=excluded.seasonal_trend,
                        seasonal_trend_short=excluded.seasonal_trend_short,
                        seasonal_trend_long=excluded.seasonal_trend_long,
                        content_efficiency_signal=excluded.content_efficiency_signal,
                        content_efficiency_source=excluded.content_efficiency_source,
                        confidence_sample_score=excluded.confidence_sample_score,
                        confidence_consistency_score=excluded.confidence_consistency_score,
                        confidence_completeness_score=excluded.confidence_completeness_score,
                        decision_confidence=excluded.decision_confidence,
                        confidence_reason_tags_json=excluded.confidence_reason_tags_json,
                        top_forms_json=excluded.top_forms_json,
                        form_distribution_json=excluded.form_distribution_json,
                        form_distribution_by_count_json=excluded.form_distribution_by_count_json,
                        form_distribution_by_sales_json=excluded.form_distribution_by_sales_json,
                        top_silhouette_forms_json=excluded.top_silhouette_forms_json,
                        top_length_forms_json=excluded.top_length_forms_json,
                        silhouette_distribution_by_count_json=excluded.silhouette_distribution_by_count_json,
                        silhouette_distribution_by_sales_json=excluded.silhouette_distribution_by_sales_json,
                        length_distribution_by_count_json=excluded.length_distribution_by_count_json,
                        length_distribution_by_sales_json=excluded.length_distribution_by_sales_json,
                        core_elements_json=excluded.core_elements_json,
                        scene_tags_json=excluded.scene_tags_json,
                        target_price_bands_json=excluded.target_price_bands_json,
                        heat_level=excluded.heat_level,
                        crowd_level=excluded.crowd_level,
                        top_value_points_json=excluded.top_value_points_json,
                        default_content_route_preference=excluded.default_content_route_preference,
                        representative_products_json=excluded.representative_products_json,
                        priority_level=excluded.priority_level,
                        selection_advice=excluded.selection_advice,
                        avoid_notes=excluded.avoid_notes,
                        confidence=excluded.confidence,
                        product_count=excluded.product_count,
                        average_heat_score=excluded.average_heat_score,
                        average_crowd_score=excluded.average_crowd_score,
                        direction_key=excluded.direction_key
                    """,
                    (
                        run_id,
                        card.direction_canonical_key,
                        card.direction_instance_id,
                        card.batch_date,
                        card.country,
                        card.category,
                        card.direction_name,
                        card.style_cluster,
                        card.direction_family,
                        int(card.direction_item_count),
                        float(card.direction_sales_median_7d),
                        float(card.direction_video_density_avg),
                        float(card.direction_creator_density_avg),
                        card.direction_tier,
                        card.seasonal_trend,
                        card.seasonal_trend_short,
                        card.seasonal_trend_long,
                        float(card.content_efficiency_signal or 0.0),
                        card.content_efficiency_source,
                        int(card.confidence_sample_score or 0),
                        int(card.confidence_consistency_score or 0),
                        int(card.confidence_completeness_score or 0),
                        card.decision_confidence,
                        json.dumps(card.confidence_reason_tags, ensure_ascii=False),
                        json.dumps(card.top_forms, ensure_ascii=False),
                        json.dumps(card.form_distribution, ensure_ascii=False),
                        json.dumps(card.form_distribution_by_count, ensure_ascii=False),
                        json.dumps(card.form_distribution_by_sales, ensure_ascii=False),
                        json.dumps(card.top_silhouette_forms, ensure_ascii=False),
                        json.dumps(card.top_length_forms, ensure_ascii=False),
                        json.dumps(card.silhouette_distribution_by_count, ensure_ascii=False),
                        json.dumps(card.silhouette_distribution_by_sales, ensure_ascii=False),
                        json.dumps(card.length_distribution_by_count, ensure_ascii=False),
                        json.dumps(card.length_distribution_by_sales, ensure_ascii=False),
                        json.dumps(card.core_elements, ensure_ascii=False),
                        json.dumps(card.scene_tags, ensure_ascii=False),
                        json.dumps(card.target_price_bands, ensure_ascii=False),
                        card.heat_level,
                        card.crowd_level,
                        json.dumps(card.top_value_points, ensure_ascii=False),
                        card.default_content_route_preference,
                        json.dumps(card.representative_products, ensure_ascii=False),
                        card.priority_level,
                        card.selection_advice,
                        card.avoid_notes,
                        float(card.confidence),
                        int(card.product_count),
                        float(card.average_heat_score),
                        float(card.average_crowd_score),
                        card.direction_key,
                    ),
                )
                conn.execute(
                    """
                    UPDATE market_direction_cards
                    SET decision_action = ?,
                        direction_execution_brief_json = ?
                    WHERE run_id = ? AND direction_canonical_key = ?
                    """,
                    (
                        str(getattr(card, "decision_action", "") or ""),
                        json.dumps(getattr(card, "direction_execution_brief", {}) or {}, ensure_ascii=False),
                        run_id,
                        card.direction_canonical_key,
                    ),
                )
            conn.commit()

    def load_latest_direction_cards(self, country: str, category: str) -> List[Dict[str, object]]:
        if not self.db_path.exists():
            return []
        with sqlite3.connect(str(self.db_path)) as conn:
            self._ensure_schema(conn)
            row = conn.execute(
                """
                SELECT run_id
                FROM market_insight_runs
                WHERE country = ? AND category = ? AND run_status = 'completed'
                  AND source_scope = 'official'
                  AND completed_product_count >= min_consumable_product_count
                  AND direction_count >= min_consumable_direction_count
                ORDER BY is_latest DESC, updated_at_epoch DESC
                LIMIT 1
                """,
                (country, category),
            ).fetchone()
            if not row:
                return []
            run_id = str(row[0] or "")
            results = conn.execute(
                """
                SELECT
                    direction_canonical_key, direction_instance_id, batch_date, country, category,
                    direction_name, style_cluster, direction_family, direction_item_count,
                    direction_sales_median_7d, direction_video_density_avg, direction_creator_density_avg,
                    direction_tier, seasonal_trend, seasonal_trend_short, seasonal_trend_long,
                    content_efficiency_signal, content_efficiency_source,
                    confidence_sample_score, confidence_consistency_score, confidence_completeness_score,
                    decision_confidence, confidence_reason_tags_json,
                    top_forms_json, form_distribution_json, form_distribution_by_count_json,
                    form_distribution_by_sales_json, top_silhouette_forms_json, top_length_forms_json,
                    silhouette_distribution_by_count_json, silhouette_distribution_by_sales_json,
                    length_distribution_by_count_json, length_distribution_by_sales_json,
                    core_elements_json, scene_tags_json, target_price_bands_json,
                    heat_level, crowd_level, top_value_points_json, default_content_route_preference, representative_products_json,
                    priority_level, selection_advice, avoid_notes, confidence,
                    product_count, average_heat_score, average_crowd_score, direction_key, decision_action,
                    direction_execution_brief_json
                FROM market_direction_cards
                WHERE run_id = ?
                ORDER BY
                    CASE priority_level WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END DESC,
                    average_heat_score DESC,
                    direction_name ASC
                """,
                (run_id,),
            ).fetchall()
        cards = []
        for record in results:
            cards.append(
                {
                    "direction_canonical_key": str(record[0] or ""),
                    "direction_instance_id": str(record[1] or ""),
                    "batch_date": str(record[2] or ""),
                    "country": str(record[3] or ""),
                    "category": str(record[4] or ""),
                    "direction_name": str(record[5] or ""),
                    "style_cluster": str(record[6] or ""),
                    "direction_family": str(record[7] or ""),
                    "direction_item_count": int(record[8] or 0),
                    "direction_sales_median_7d": float(record[9] or 0.0),
                    "direction_video_density_avg": float(record[10] or 0.0),
                    "direction_creator_density_avg": float(record[11] or 0.0),
                    "direction_tier": str(record[12] or ""),
                    "seasonal_trend": str(record[13] or "unclear"),
                    "seasonal_trend_short": str(record[14] or "unclear"),
                    "seasonal_trend_long": str(record[15] or "unclear"),
                    "content_efficiency_signal": float(record[16] or 0.0),
                    "content_efficiency_source": str(record[17] or "missing"),
                    "confidence_sample_score": int(record[18] or 0),
                    "confidence_consistency_score": int(record[19] or 0),
                    "confidence_completeness_score": int(record[20] or 0),
                    "decision_confidence": str(record[21] or "low"),
                    "confidence_reason_tags": json.loads(str(record[22] or "[]")),
                    "top_forms": json.loads(str(record[23] or "[]")),
                    "form_distribution": json.loads(str(record[24] or "{}")),
                    "form_distribution_by_count": json.loads(str(record[25] or "{}")),
                    "form_distribution_by_sales": json.loads(str(record[26] or "{}")),
                    "top_silhouette_forms": json.loads(str(record[27] or "[]")),
                    "top_length_forms": json.loads(str(record[28] or "[]")),
                    "silhouette_distribution_by_count": json.loads(str(record[29] or "{}")),
                    "silhouette_distribution_by_sales": json.loads(str(record[30] or "{}")),
                    "length_distribution_by_count": json.loads(str(record[31] or "{}")),
                    "length_distribution_by_sales": json.loads(str(record[32] or "{}")),
                    "core_elements": json.loads(str(record[33] or "[]")),
                    "scene_tags": json.loads(str(record[34] or "[]")),
                    "target_price_bands": json.loads(str(record[35] or "[]")),
                    "heat_level": str(record[36] or ""),
                    "crowd_level": str(record[37] or ""),
                    "top_value_points": json.loads(str(record[38] or "[]")),
                    "default_content_route_preference": str(record[39] or ""),
                    "representative_products": json.loads(str(record[40] or "[]")),
                    "priority_level": str(record[41] or ""),
                    "selection_advice": str(record[42] or ""),
                    "avoid_notes": str(record[43] or ""),
                    "confidence": float(record[44] or 0.0),
                    "product_count": int(record[45] or 0),
                    "average_heat_score": float(record[46] or 0.0),
                    "average_crowd_score": float(record[47] or 0.0),
                    "direction_key": str(record[48] or ""),
                    "decision_action": str(record[49] or ""),
                    "direction_execution_brief": json.loads(str(record[50] or "{}")),
                }
            )
        return cards

    def list_recent_completed_runs(self, country: str, category: str, limit: int = 3) -> List[Dict[str, object]]:
        if not self.db_path.exists():
            return []
        with sqlite3.connect(str(self.db_path)) as conn:
            self._ensure_schema(conn)
            rows = conn.execute(
                """
                SELECT run_id, table_id, batch_date, country, category, updated_at_epoch,
                       structured_report_json, direction_count, completed_product_count, source_scope
                FROM market_insight_runs
                WHERE country = ? AND category = ? AND run_status = 'completed'
                ORDER BY updated_at_epoch DESC
                LIMIT ?
                """,
                (country, category, int(limit)),
            ).fetchall()
        results = []
        for row in rows:
            results.append(
                {
                    "run_id": str(row[0] or ""),
                    "table_id": str(row[1] or ""),
                    "batch_date": str(row[2] or ""),
                    "country": str(row[3] or ""),
                    "category": str(row[4] or ""),
                    "updated_at_epoch": int(row[5] or 0),
                    "structured_report_json": json.loads(str(row[6] or "{}")),
                    "direction_count": int(row[7] or 0),
                    "completed_product_count": int(row[8] or 0),
                    "source_scope": str(row[9] or "official"),
                }
            )
        return results

    def load_direction_cards_for_run(self, run_id: str) -> List[Dict[str, object]]:
        if not self.db_path.exists():
            return []
        with sqlite3.connect(str(self.db_path)) as conn:
            self._ensure_schema(conn)
            results = conn.execute(
                """
                SELECT
                    direction_canonical_key, direction_instance_id, batch_date, country, category,
                    direction_name, style_cluster, direction_family, direction_item_count,
                    direction_sales_median_7d, direction_video_density_avg, direction_creator_density_avg,
                    direction_tier, seasonal_trend, seasonal_trend_short, seasonal_trend_long,
                    content_efficiency_signal, content_efficiency_source,
                    confidence_sample_score, confidence_consistency_score, confidence_completeness_score,
                    decision_confidence, confidence_reason_tags_json,
                    top_forms_json, form_distribution_json, form_distribution_by_count_json,
                    form_distribution_by_sales_json, top_silhouette_forms_json, top_length_forms_json,
                    silhouette_distribution_by_count_json, silhouette_distribution_by_sales_json,
                    length_distribution_by_count_json, length_distribution_by_sales_json,
                    core_elements_json, scene_tags_json, target_price_bands_json,
                    heat_level, crowd_level, top_value_points_json, default_content_route_preference, representative_products_json,
                    priority_level, selection_advice, avoid_notes, confidence,
                    product_count, average_heat_score, average_crowd_score, direction_key, decision_action,
                    direction_execution_brief_json
                FROM market_direction_cards
                WHERE run_id = ?
                ORDER BY direction_name ASC
                """,
                (run_id,),
            ).fetchall()
        cards = []
        for record in results:
            cards.append(
                {
                    "direction_canonical_key": str(record[0] or ""),
                    "direction_instance_id": str(record[1] or ""),
                    "batch_date": str(record[2] or ""),
                    "country": str(record[3] or ""),
                    "category": str(record[4] or ""),
                    "direction_name": str(record[5] or ""),
                    "style_cluster": str(record[6] or ""),
                    "direction_family": str(record[7] or ""),
                    "direction_item_count": int(record[8] or 0),
                    "direction_sales_median_7d": float(record[9] or 0.0),
                    "direction_video_density_avg": float(record[10] or 0.0),
                    "direction_creator_density_avg": float(record[11] or 0.0),
                    "direction_tier": str(record[12] or ""),
                    "seasonal_trend": str(record[13] or "unclear"),
                    "seasonal_trend_short": str(record[14] or "unclear"),
                    "seasonal_trend_long": str(record[15] or "unclear"),
                    "content_efficiency_signal": float(record[16] or 0.0),
                    "content_efficiency_source": str(record[17] or "missing"),
                    "confidence_sample_score": int(record[18] or 0),
                    "confidence_consistency_score": int(record[19] or 0),
                    "confidence_completeness_score": int(record[20] or 0),
                    "decision_confidence": str(record[21] or "low"),
                    "confidence_reason_tags": json.loads(str(record[22] or "[]")),
                    "top_forms": json.loads(str(record[23] or "[]")),
                    "form_distribution": json.loads(str(record[24] or "{}")),
                    "form_distribution_by_count": json.loads(str(record[25] or "{}")),
                    "form_distribution_by_sales": json.loads(str(record[26] or "{}")),
                    "top_silhouette_forms": json.loads(str(record[27] or "[]")),
                    "top_length_forms": json.loads(str(record[28] or "[]")),
                    "silhouette_distribution_by_count": json.loads(str(record[29] or "{}")),
                    "silhouette_distribution_by_sales": json.loads(str(record[30] or "{}")),
                    "length_distribution_by_count": json.loads(str(record[31] or "{}")),
                    "length_distribution_by_sales": json.loads(str(record[32] or "{}")),
                    "core_elements": json.loads(str(record[33] or "[]")),
                    "scene_tags": json.loads(str(record[34] or "[]")),
                    "target_price_bands": json.loads(str(record[35] or "[]")),
                    "heat_level": str(record[36] or ""),
                    "crowd_level": str(record[37] or ""),
                    "top_value_points": json.loads(str(record[38] or "[]")),
                    "default_content_route_preference": str(record[39] or ""),
                    "representative_products": json.loads(str(record[40] or "[]")),
                    "priority_level": str(record[41] or ""),
                    "selection_advice": str(record[42] or ""),
                    "avoid_notes": str(record[43] or ""),
                    "confidence": float(record[44] or 0.0),
                    "product_count": int(record[45] or 0),
                    "average_heat_score": float(record[46] or 0.0),
                    "average_crowd_score": float(record[47] or 0.0),
                    "direction_key": str(record[48] or ""),
                    "decision_action": str(record[49] or ""),
                    "direction_execution_brief": json.loads(str(record[50] or "{}")),
                }
            )
        return cards

    def load_latest_scored_products(self, country: str, category: str) -> List[Dict[str, object]]:
        if not self.db_path.exists():
            return []
        with sqlite3.connect(str(self.db_path)) as conn:
            self._ensure_schema(conn)
            row = conn.execute(
                """
                SELECT run_id
                FROM market_insight_runs
                WHERE country = ? AND category = ? AND run_status = 'completed'
                ORDER BY is_latest DESC, updated_at_epoch DESC
                LIMIT 1
                """,
                (country, category),
            ).fetchone()
            if not row:
                return []
            run_id = str(row[0] or "")
            results = conn.execute(
                """
                SELECT
                    s.batch_date, s.country, s.category, s.product_id, s.product_name, s.shop_name,
                    s.price_min, s.price_max, s.price_mid, s.sales_7d, s.gmv_7d, s.creator_count, s.video_count,
                    s.listing_days, s.image_url, s.product_url, s.rank_index, s.raw_category, s.raw_fields_json,
                    t.is_valid_sample, t.style_cluster, t.style_tags_secondary_json, t.product_form, t.length_form,
                    t.element_tags_json, t.value_points_json, t.scene_tags_json, t.reason_short,
                    t.heat_score, t.heat_level, t.crowd_score, t.crowd_level, t.priority_level, t.target_price_band,
                    t.direction_canonical_key, t.direction_family, t.direction_tier,
                    t.seasonal_trend, t.seasonal_trend_short, t.seasonal_trend_long,
                    t.content_efficiency_signal, t.content_efficiency_source,
                    t.default_content_route_preference
                FROM market_insight_product_snapshots s
                JOIN market_insight_product_tags t
                  ON s.run_id = t.run_id AND s.product_row_key = t.product_row_key
                WHERE s.run_id = ?
                ORDER BY s.rank_index ASC
                """,
                (run_id,),
            ).fetchall()
        payload = []
        for record in results:
            payload.append(
                {
                    "snapshot": {
                        "batch_date": str(record[0] or ""),
                        "country": str(record[1] or ""),
                        "category": str(record[2] or ""),
                        "product_id": str(record[3] or ""),
                        "product_name": str(record[4] or ""),
                        "shop_name": str(record[5] or ""),
                        "price_min": self._nullable_float(record[6]),
                        "price_max": self._nullable_float(record[7]),
                        "price_mid": self._nullable_float(record[8]),
                        "sales_7d": float(record[9] or 0.0),
                        "gmv_7d": float(record[10] or 0.0),
                        "creator_count": float(record[11] or 0.0),
                        "video_count": float(record[12] or 0.0),
                        "listing_days": self._nullable_int(record[13]),
                        "image_url": str(record[14] or ""),
                        "product_url": str(record[15] or ""),
                        "rank_index": int(record[16] or 0),
                        "raw_category": str(record[17] or ""),
                        "raw_fields": json.loads(str(record[18] or "{}")),
                    },
                    "tag": {
                        "is_valid_sample": bool(record[19]),
                        "style_cluster": str(record[20] or ""),
                        "style_tags_secondary": json.loads(str(record[21] or "[]")),
                        "product_form": str(record[22] or ""),
                        "length_form": str(record[23] or ""),
                        "element_tags": json.loads(str(record[24] or "[]")),
                        "value_points": json.loads(str(record[25] or "[]")),
                        "scene_tags": json.loads(str(record[26] or "[]")),
                        "reason_short": str(record[27] or ""),
                    },
                    "heat_score": float(record[28] or 0.0),
                    "heat_level": str(record[29] or ""),
                    "crowd_score": float(record[30] or 0.0),
                    "crowd_level": str(record[31] or ""),
                    "priority_level": str(record[32] or ""),
                    "target_price_band": str(record[33] or ""),
                    "direction_canonical_key": str(record[34] or ""),
                    "direction_family": str(record[35] or ""),
                    "direction_tier": str(record[36] or ""),
                    "seasonal_trend": str(record[37] or "unclear"),
                    "seasonal_trend_short": str(record[38] or "unclear"),
                    "seasonal_trend_long": str(record[39] or "unclear"),
                    "content_efficiency_signal": float(record[40] or 0.0),
                    "content_efficiency_source": str(record[41] or "missing"),
                    "default_content_route_preference": str(record[42] or ""),
                }
            )
        return payload

    def load_scored_products_for_run(self, run_id: str) -> List[Dict[str, object]]:
        if not self.db_path.exists():
            return []
        with sqlite3.connect(str(self.db_path)) as conn:
            self._ensure_schema(conn)
            results = conn.execute(
                """
                SELECT
                    s.batch_date, s.country, s.category, s.product_id, s.product_name, s.shop_name,
                    s.price_min, s.price_max, s.price_mid, s.sales_7d, s.gmv_7d, s.creator_count, s.video_count,
                    s.listing_days, s.image_url, s.product_url, s.rank_index, s.raw_category, s.raw_fields_json,
                    t.is_valid_sample, t.style_cluster, t.style_tags_secondary_json, t.product_form, t.length_form,
                    t.element_tags_json, t.value_points_json, t.scene_tags_json, t.reason_short,
                    t.heat_score, t.heat_level, t.crowd_score, t.crowd_level, t.priority_level, t.target_price_band,
                    t.direction_canonical_key, t.direction_family, t.direction_tier,
                    t.seasonal_trend, t.seasonal_trend_short, t.seasonal_trend_long,
                    t.content_efficiency_signal, t.content_efficiency_source,
                    t.default_content_route_preference
                FROM market_insight_product_snapshots s
                JOIN market_insight_product_tags t
                  ON s.run_id = t.run_id AND s.product_row_key = t.product_row_key
                WHERE s.run_id = ?
                ORDER BY s.rank_index ASC
                """,
                (run_id,),
            ).fetchall()
        payload = []
        for record in results:
            payload.append(
                {
                    "snapshot": {
                        "batch_date": str(record[0] or ""),
                        "country": str(record[1] or ""),
                        "category": str(record[2] or ""),
                        "product_id": str(record[3] or ""),
                        "product_name": str(record[4] or ""),
                        "shop_name": str(record[5] or ""),
                        "price_min": self._nullable_float(record[6]),
                        "price_max": self._nullable_float(record[7]),
                        "price_mid": self._nullable_float(record[8]),
                        "sales_7d": float(record[9] or 0.0),
                        "gmv_7d": float(record[10] or 0.0),
                        "creator_count": float(record[11] or 0.0),
                        "video_count": float(record[12] or 0.0),
                        "listing_days": self._nullable_int(record[13]),
                        "image_url": str(record[14] or ""),
                        "product_url": str(record[15] or ""),
                        "rank_index": int(record[16] or 0),
                        "raw_category": str(record[17] or ""),
                        "raw_fields": json.loads(str(record[18] or "{}")),
                    },
                    "tag": {
                        "is_valid_sample": bool(record[19]),
                        "style_cluster": str(record[20] or ""),
                        "style_tags_secondary": json.loads(str(record[21] or "[]")),
                        "product_form": str(record[22] or ""),
                        "length_form": str(record[23] or ""),
                        "element_tags": json.loads(str(record[24] or "[]")),
                        "value_points": json.loads(str(record[25] or "[]")),
                        "scene_tags": json.loads(str(record[26] or "[]")),
                        "reason_short": str(record[27] or ""),
                    },
                    "heat_score": float(record[28] or 0.0),
                    "heat_level": str(record[29] or ""),
                    "crowd_score": float(record[30] or 0.0),
                    "crowd_level": str(record[31] or ""),
                    "priority_level": str(record[32] or ""),
                    "target_price_band": str(record[33] or ""),
                    "direction_canonical_key": str(record[34] or ""),
                    "direction_family": str(record[35] or ""),
                    "direction_tier": str(record[36] or ""),
                    "seasonal_trend": str(record[37] or "unclear"),
                    "seasonal_trend_short": str(record[38] or "unclear"),
                    "seasonal_trend_long": str(record[39] or "unclear"),
                    "content_efficiency_signal": float(record[40] or 0.0),
                    "content_efficiency_source": str(record[41] or "missing"),
                    "default_content_route_preference": str(record[42] or ""),
                }
            )
        return payload

    def upsert_store_positioning_cards(
        self,
        source_table_id: str,
        cards: Iterable[StorePositioningCard],
        updated_at_epoch: int,
    ) -> int:
        card_list = [
            card
            for card in cards
            if str(card.store_id or "").strip() or str(card.card_name or "").strip()
        ]
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(self.db_path)) as conn:
            self._ensure_schema(conn)
            card_keys = [
                self._store_positioning_key(card.store_id or card.card_name, card.country, card.category)
                for card in card_list
            ]
            if source_table_id:
                if card_keys:
                    placeholders = ",".join(["?"] * len(card_keys))
                    conn.execute(
                        "DELETE FROM store_positioning_cards WHERE source_table_id = ? AND store_key NOT IN ({placeholders})".format(
                            placeholders=placeholders
                        ),
                        [source_table_id] + card_keys,
                    )
                else:
                    conn.execute("DELETE FROM store_positioning_cards WHERE source_table_id = ?", (source_table_id,))

            for card in card_list:
                store_key = self._store_positioning_key(card.store_id or card.card_name, card.country, card.category)
                raw_payload = {
                    "store_id": card.store_id,
                    "source_record_id": card.source_record_id,
                    "country": card.country,
                    "category": card.category,
                    "card_name": card.card_name,
                    "style_whitelist": list(card.style_whitelist),
                    "style_blacklist": list(card.style_blacklist),
                    "soft_style_blacklist": list(card.soft_style_blacklist or card.style_blacklist),
                    "hard_style_blacklist": list(card.hard_style_blacklist),
                    "target_price_bands": list(card.target_price_bands),
                    "core_scenes": list(card.core_scenes),
                    "content_tones": list(card.content_tones),
                    "core_value_points": list(card.core_value_points),
                    "target_audience": list(card.target_audience),
                    "selection_principles": list(card.selection_principles),
                    "notes": card.notes,
                }
                conn.execute(
                    """
                    INSERT INTO store_positioning_cards (
                        store_key, source_table_id, source_record_id, store_id, card_name,
                        country, category, style_whitelist_json, style_blacklist_json,
                        soft_style_blacklist_json, hard_style_blacklist_json,
                        target_price_bands_json, core_scenes_json, content_tones_json,
                        core_value_points_json, target_audience_json, selection_principles_json,
                        notes, raw_payload_json, updated_at_epoch
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(store_key) DO UPDATE SET
                        source_table_id=excluded.source_table_id,
                        source_record_id=excluded.source_record_id,
                        store_id=excluded.store_id,
                        card_name=excluded.card_name,
                        country=excluded.country,
                        category=excluded.category,
                        style_whitelist_json=excluded.style_whitelist_json,
                        style_blacklist_json=excluded.style_blacklist_json,
                        soft_style_blacklist_json=excluded.soft_style_blacklist_json,
                        hard_style_blacklist_json=excluded.hard_style_blacklist_json,
                        target_price_bands_json=excluded.target_price_bands_json,
                        core_scenes_json=excluded.core_scenes_json,
                        content_tones_json=excluded.content_tones_json,
                        core_value_points_json=excluded.core_value_points_json,
                        target_audience_json=excluded.target_audience_json,
                        selection_principles_json=excluded.selection_principles_json,
                        notes=excluded.notes,
                        raw_payload_json=excluded.raw_payload_json,
                        updated_at_epoch=excluded.updated_at_epoch
                    """,
                    (
                        store_key,
                        source_table_id,
                        card.source_record_id,
                        card.store_id,
                        card.card_name,
                        card.country,
                        card.category,
                        json.dumps(card.style_whitelist, ensure_ascii=False),
                        json.dumps(card.style_blacklist, ensure_ascii=False),
                        json.dumps(card.soft_style_blacklist or card.style_blacklist, ensure_ascii=False),
                        json.dumps(card.hard_style_blacklist, ensure_ascii=False),
                        json.dumps(card.target_price_bands, ensure_ascii=False),
                        json.dumps(card.core_scenes, ensure_ascii=False),
                        json.dumps(card.content_tones, ensure_ascii=False),
                        json.dumps(card.core_value_points, ensure_ascii=False),
                        json.dumps(card.target_audience, ensure_ascii=False),
                        json.dumps(card.selection_principles, ensure_ascii=False),
                        card.notes,
                        json.dumps(raw_payload, ensure_ascii=False),
                        int(updated_at_epoch),
                    ),
                )
            conn.commit()
        return len(card_list)

    def load_store_positioning_card(
        self,
        store_id: str,
        country: str = "",
        category: str = "",
        card_name: str = "",
    ) -> StorePositioningCard:
        candidates = {
            str(store_id or "").strip(),
            str(card_name or "").strip(),
        }
        candidates = {item for item in candidates if item}
        if not candidates or not self.db_path.exists():
            return StorePositioningCard()

        placeholders = ",".join(["?"] * len(candidates))
        with sqlite3.connect(str(self.db_path)) as conn:
            self._ensure_schema(conn)
            rows = conn.execute(
                """
                SELECT
                    source_record_id, store_id, card_name, country, category,
                    style_whitelist_json, style_blacklist_json, soft_style_blacklist_json, hard_style_blacklist_json, target_price_bands_json,
                    core_scenes_json, content_tones_json, core_value_points_json,
                    target_audience_json, selection_principles_json, notes, updated_at_epoch
                FROM store_positioning_cards
                WHERE store_id IN ({placeholders}) OR card_name IN ({placeholders})
                ORDER BY updated_at_epoch DESC
                """.format(placeholders=placeholders),
                list(candidates) + list(candidates),
            ).fetchall()

        best_row = None
        best_score = -1.0
        normalized_country = str(country or "").strip().upper()
        normalized_category = str(category or "").strip().lower()
        preferred_store_id = str(store_id or "").strip()
        preferred_card_name = str(card_name or "").strip()
        for row in rows:
            row_store_id = str(row[1] or "").strip()
            row_card_name = str(row[2] or "").strip()
            row_country = str(row[3] or "").strip().upper()
            row_category = str(row[4] or "").strip().lower()
            score = 0.0
            if preferred_store_id and row_store_id == preferred_store_id:
                score += 4.0
            elif preferred_card_name and row_card_name == preferred_card_name:
                score += 3.0
            if normalized_country:
                if row_country == normalized_country:
                    score += 2.0
                elif not row_country:
                    score += 0.5
            if normalized_category:
                if row_category == normalized_category:
                    score += 1.5
                elif not row_category:
                    score += 0.25
            if score > best_score:
                best_score = score
                best_row = row

        if not best_row:
            return StorePositioningCard()
        return StorePositioningCard(
            source_record_id=str(best_row[0] or ""),
            store_id=str(best_row[1] or ""),
            card_name=str(best_row[2] or ""),
            country=str(best_row[3] or ""),
            category=str(best_row[4] or ""),
            style_whitelist=self._json_list(best_row[5]),
            style_blacklist=self._json_list(best_row[6]),
            soft_style_blacklist=self._json_list(best_row[7]) or self._json_list(best_row[6]),
            hard_style_blacklist=self._json_list(best_row[8]),
            target_price_bands=self._json_list(best_row[9]),
            core_scenes=self._json_list(best_row[10]),
            content_tones=self._json_list(best_row[11]),
            core_value_points=self._json_list(best_row[12]),
            target_audience=self._json_list(best_row[13]),
            selection_principles=self._json_list(best_row[14]),
            notes=str(best_row[15] or ""),
        )

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_insight_runs (
                run_id TEXT PRIMARY KEY,
                table_id TEXT NOT NULL,
                batch_date TEXT NOT NULL,
                country TEXT NOT NULL,
                category TEXT NOT NULL,
                artifacts_dir TEXT NOT NULL,
                report_json_path TEXT NOT NULL,
                report_md_path TEXT NOT NULL,
                progress_json_path TEXT NOT NULL,
                completed_product_count INTEGER NOT NULL DEFAULT 0,
                total_product_count INTEGER NOT NULL DEFAULT 0,
                direction_count INTEGER NOT NULL DEFAULT 0,
                voc_status TEXT NOT NULL DEFAULT 'skipped',
                run_status TEXT NOT NULL DEFAULT 'completed',
                is_latest INTEGER NOT NULL DEFAULT 0,
                updated_at_epoch INTEGER NOT NULL DEFAULT 0,
                source_scope TEXT NOT NULL DEFAULT 'official',
                min_consumable_product_count INTEGER NOT NULL DEFAULT 100,
                min_consumable_direction_count INTEGER NOT NULL DEFAULT 5,
                structured_report_json TEXT NOT NULL DEFAULT '{}',
                report_delivery_json TEXT NOT NULL DEFAULT '{}',
                feishu_doc_token TEXT NOT NULL DEFAULT '',
                feishu_doc_url TEXT NOT NULL DEFAULT '',
                notification_status TEXT NOT NULL DEFAULT '',
                llm_fallback_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_insight_product_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                product_row_key TEXT NOT NULL,
                batch_date TEXT NOT NULL,
                country TEXT NOT NULL,
                category TEXT NOT NULL,
                product_id TEXT NOT NULL,
                product_name TEXT NOT NULL,
                shop_name TEXT NOT NULL,
                price_min REAL,
                price_max REAL,
                price_mid REAL,
                sales_7d REAL NOT NULL DEFAULT 0.0,
                gmv_7d REAL NOT NULL DEFAULT 0.0,
                creator_count REAL NOT NULL DEFAULT 0.0,
                video_count REAL NOT NULL DEFAULT 0.0,
                listing_days INTEGER,
                image_url TEXT NOT NULL DEFAULT '',
                product_url TEXT NOT NULL DEFAULT '',
                rank_index INTEGER NOT NULL DEFAULT 0,
                raw_category TEXT NOT NULL DEFAULT '',
                raw_fields_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE(run_id, product_row_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_insight_product_tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                product_row_key TEXT NOT NULL,
                product_id TEXT NOT NULL,
                rank_index INTEGER NOT NULL DEFAULT 0,
                is_valid_sample INTEGER NOT NULL DEFAULT 0,
                style_cluster TEXT NOT NULL DEFAULT '',
                style_tags_secondary_json TEXT NOT NULL DEFAULT '[]',
                product_form TEXT NOT NULL DEFAULT '',
                length_form TEXT NOT NULL DEFAULT '',
                element_tags_json TEXT NOT NULL DEFAULT '[]',
                value_points_json TEXT NOT NULL DEFAULT '[]',
                scene_tags_json TEXT NOT NULL DEFAULT '[]',
                reason_short TEXT NOT NULL DEFAULT '',
                heat_score REAL NOT NULL DEFAULT 0.0,
                heat_level TEXT NOT NULL DEFAULT '',
                crowd_score REAL NOT NULL DEFAULT 0.0,
                crowd_level TEXT NOT NULL DEFAULT '',
                priority_level TEXT NOT NULL DEFAULT '',
                target_price_band TEXT NOT NULL DEFAULT '',
                direction_canonical_key TEXT NOT NULL DEFAULT '',
                direction_family TEXT NOT NULL DEFAULT '',
                direction_tier TEXT NOT NULL DEFAULT '',
                seasonal_trend TEXT NOT NULL DEFAULT 'unclear',
                seasonal_trend_short TEXT NOT NULL DEFAULT 'unclear',
                seasonal_trend_long TEXT NOT NULL DEFAULT 'unclear',
                content_efficiency_signal REAL NOT NULL DEFAULT 0.0,
                content_efficiency_source TEXT NOT NULL DEFAULT 'missing',
                default_content_route_preference TEXT NOT NULL DEFAULT '',
                UNIQUE(run_id, product_row_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_direction_cards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                direction_canonical_key TEXT NOT NULL,
                direction_instance_id TEXT NOT NULL,
                batch_date TEXT NOT NULL,
                country TEXT NOT NULL,
                category TEXT NOT NULL,
                direction_name TEXT NOT NULL,
                style_cluster TEXT NOT NULL,
                direction_family TEXT NOT NULL DEFAULT '',
                direction_item_count INTEGER NOT NULL DEFAULT 0,
                direction_sales_median_7d REAL NOT NULL DEFAULT 0.0,
                direction_video_density_avg REAL NOT NULL DEFAULT 0.0,
                direction_creator_density_avg REAL NOT NULL DEFAULT 0.0,
                direction_tier TEXT NOT NULL DEFAULT '',
                seasonal_trend TEXT NOT NULL DEFAULT 'unclear',
                seasonal_trend_short TEXT NOT NULL DEFAULT 'unclear',
                seasonal_trend_long TEXT NOT NULL DEFAULT 'unclear',
                content_efficiency_signal REAL NOT NULL DEFAULT 0.0,
                content_efficiency_source TEXT NOT NULL DEFAULT 'missing',
                confidence_sample_score INTEGER NOT NULL DEFAULT 0,
                confidence_consistency_score INTEGER NOT NULL DEFAULT 0,
                confidence_completeness_score INTEGER NOT NULL DEFAULT 0,
                decision_confidence TEXT NOT NULL DEFAULT 'low',
                decision_action TEXT NOT NULL DEFAULT '',
                confidence_reason_tags_json TEXT NOT NULL DEFAULT '[]',
                top_forms_json TEXT NOT NULL,
                form_distribution_json TEXT NOT NULL,
                form_distribution_by_count_json TEXT NOT NULL DEFAULT '{}',
                form_distribution_by_sales_json TEXT NOT NULL DEFAULT '{}',
                top_silhouette_forms_json TEXT NOT NULL DEFAULT '[]',
                top_length_forms_json TEXT NOT NULL DEFAULT '[]',
                silhouette_distribution_by_count_json TEXT NOT NULL DEFAULT '{}',
                silhouette_distribution_by_sales_json TEXT NOT NULL DEFAULT '{}',
                length_distribution_by_count_json TEXT NOT NULL DEFAULT '{}',
                length_distribution_by_sales_json TEXT NOT NULL DEFAULT '{}',
                core_elements_json TEXT NOT NULL,
                scene_tags_json TEXT NOT NULL,
                target_price_bands_json TEXT NOT NULL,
                heat_level TEXT NOT NULL,
                crowd_level TEXT NOT NULL,
                top_value_points_json TEXT NOT NULL,
                default_content_route_preference TEXT NOT NULL DEFAULT '',
                representative_products_json TEXT NOT NULL,
                priority_level TEXT NOT NULL,
                selection_advice TEXT NOT NULL,
                avoid_notes TEXT NOT NULL,
                confidence REAL NOT NULL DEFAULT 0.0,
                product_count INTEGER NOT NULL DEFAULT 0,
                average_heat_score REAL NOT NULL DEFAULT 0.0,
                average_crowd_score REAL NOT NULL DEFAULT 0.0,
                direction_key TEXT NOT NULL DEFAULT '',
                direction_execution_brief_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE(run_id, direction_canonical_key)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_insight_runs_country_category ON market_insight_runs(country, category, is_latest, updated_at_epoch)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_insight_product_snapshots_run_id ON market_insight_product_snapshots(run_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_insight_product_tags_run_id ON market_insight_product_tags(run_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_direction_cards_run_id ON market_direction_cards(run_id)"
        )
        self._ensure_column(conn, "market_insight_product_tags", "direction_canonical_key", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "market_insight_product_tags", "direction_family", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "market_insight_product_tags", "direction_tier", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "market_insight_product_tags", "length_form", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "market_insight_product_tags", "seasonal_trend", "TEXT NOT NULL DEFAULT 'unclear'")
        self._ensure_column(conn, "market_insight_product_tags", "seasonal_trend_short", "TEXT NOT NULL DEFAULT 'unclear'")
        self._ensure_column(conn, "market_insight_product_tags", "seasonal_trend_long", "TEXT NOT NULL DEFAULT 'unclear'")
        self._ensure_column(conn, "market_insight_product_tags", "content_efficiency_signal", "REAL NOT NULL DEFAULT 0.0")
        self._ensure_column(conn, "market_insight_product_tags", "content_efficiency_source", "TEXT NOT NULL DEFAULT 'missing'")
        self._ensure_column(conn, "market_insight_product_tags", "default_content_route_preference", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "market_insight_runs", "structured_report_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "market_insight_runs", "report_delivery_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "market_insight_runs", "feishu_doc_token", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "market_insight_runs", "feishu_doc_url", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "market_insight_runs", "notification_status", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "market_insight_runs", "llm_fallback_count", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "market_insight_runs", "source_scope", "TEXT NOT NULL DEFAULT 'official'")
        self._ensure_column(conn, "market_insight_runs", "min_consumable_product_count", "INTEGER NOT NULL DEFAULT 100")
        self._ensure_column(conn, "market_insight_runs", "min_consumable_direction_count", "INTEGER NOT NULL DEFAULT 5")
        self._ensure_column(conn, "market_direction_cards", "direction_family", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "market_direction_cards", "direction_item_count", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "market_direction_cards", "direction_sales_median_7d", "REAL NOT NULL DEFAULT 0.0")
        self._ensure_column(conn, "market_direction_cards", "direction_video_density_avg", "REAL NOT NULL DEFAULT 0.0")
        self._ensure_column(conn, "market_direction_cards", "direction_creator_density_avg", "REAL NOT NULL DEFAULT 0.0")
        self._ensure_column(conn, "market_direction_cards", "direction_tier", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "market_direction_cards", "seasonal_trend", "TEXT NOT NULL DEFAULT 'unclear'")
        self._ensure_column(conn, "market_direction_cards", "seasonal_trend_short", "TEXT NOT NULL DEFAULT 'unclear'")
        self._ensure_column(conn, "market_direction_cards", "seasonal_trend_long", "TEXT NOT NULL DEFAULT 'unclear'")
        self._ensure_column(conn, "market_direction_cards", "content_efficiency_signal", "REAL NOT NULL DEFAULT 0.0")
        self._ensure_column(conn, "market_direction_cards", "content_efficiency_source", "TEXT NOT NULL DEFAULT 'missing'")
        self._ensure_column(conn, "market_direction_cards", "confidence_sample_score", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "market_direction_cards", "confidence_consistency_score", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "market_direction_cards", "confidence_completeness_score", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "market_direction_cards", "decision_confidence", "TEXT NOT NULL DEFAULT 'low'")
        self._ensure_column(conn, "market_direction_cards", "decision_action", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "market_direction_cards", "confidence_reason_tags_json", "TEXT NOT NULL DEFAULT '[]'")
        self._ensure_column(conn, "market_direction_cards", "form_distribution_by_count_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "market_direction_cards", "form_distribution_by_sales_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "market_direction_cards", "top_silhouette_forms_json", "TEXT NOT NULL DEFAULT '[]'")
        self._ensure_column(conn, "market_direction_cards", "top_length_forms_json", "TEXT NOT NULL DEFAULT '[]'")
        self._ensure_column(conn, "market_direction_cards", "silhouette_distribution_by_count_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "market_direction_cards", "silhouette_distribution_by_sales_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "market_direction_cards", "length_distribution_by_count_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "market_direction_cards", "length_distribution_by_sales_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column(conn, "market_direction_cards", "default_content_route_preference", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(conn, "market_direction_cards", "direction_execution_brief_json", "TEXT NOT NULL DEFAULT '{}'")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS store_positioning_cards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_key TEXT NOT NULL UNIQUE,
                source_table_id TEXT NOT NULL DEFAULT '',
                source_record_id TEXT NOT NULL DEFAULT '',
                store_id TEXT NOT NULL DEFAULT '',
                card_name TEXT NOT NULL DEFAULT '',
                country TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '',
                style_whitelist_json TEXT NOT NULL DEFAULT '[]',
                style_blacklist_json TEXT NOT NULL DEFAULT '[]',
                soft_style_blacklist_json TEXT NOT NULL DEFAULT '[]',
                hard_style_blacklist_json TEXT NOT NULL DEFAULT '[]',
                target_price_bands_json TEXT NOT NULL DEFAULT '[]',
                core_scenes_json TEXT NOT NULL DEFAULT '[]',
                content_tones_json TEXT NOT NULL DEFAULT '[]',
                core_value_points_json TEXT NOT NULL DEFAULT '[]',
                target_audience_json TEXT NOT NULL DEFAULT '[]',
                selection_principles_json TEXT NOT NULL DEFAULT '[]',
                notes TEXT NOT NULL DEFAULT '',
                raw_payload_json TEXT NOT NULL DEFAULT '{}',
                updated_at_epoch INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_store_positioning_cards_lookup ON store_positioning_cards(store_id, country, category, updated_at_epoch)"
        )
        self._ensure_column(conn, "store_positioning_cards", "soft_style_blacklist_json", "TEXT NOT NULL DEFAULT '[]'")
        self._ensure_column(conn, "store_positioning_cards", "hard_style_blacklist_json", "TEXT NOT NULL DEFAULT '[]'")

    def _snapshot_row_key(self, snapshot: ProductRankingSnapshot) -> str:
        product_id = str(snapshot.product_id or "").strip()
        if product_id:
            return product_id
        return "rank_{index}".format(index=int(snapshot.rank_index or 0))

    def _nullable_float(self, value):
        if value is None or value == "":
            return None
        return float(value)

    def _nullable_int(self, value):
        if value is None or value == "":
            return None
        return int(value)

    def _store_positioning_key(self, store_id: str, country: str, category: str) -> str:
        return "{store}__{country}__{category}".format(
            store=str(store_id or "").strip(),
            country=str(country or "").strip().upper(),
            category=str(category or "").strip().lower(),
        )

    def _json_list(self, value) -> List[str]:
        try:
            payload = json.loads(str(value or "[]"))
        except ValueError:
            payload = []
        if not isinstance(payload, list):
            return []
        return [str(item or "").strip() for item in payload if str(item or "").strip()]

    def _ensure_column(self, conn: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
        columns = {str(row[1] or "") for row in conn.execute("PRAGMA table_info({table})".format(table=table_name)).fetchall()}
        if column_name not in columns:
            conn.execute(
                "ALTER TABLE {table} ADD COLUMN {column} {column_sql}".format(
                    table=table_name,
                    column=column_name,
                    column_sql=column_sql,
                )
            )
