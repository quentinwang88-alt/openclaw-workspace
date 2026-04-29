#!/usr/bin/env python3
"""Artifacts writer for Market Insight v1."""

from __future__ import annotations

import json
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List

from src.market_insight_db import MarketInsightDatabase
from src.market_insight_models import (
    MarketDirectionCard,
    MarketInsightConfig,
    MarketInsightProductRunState,
    MarketInsightRunResult,
    ProductRankingSnapshot,
    ScoredProductSnapshot,
    ShopLandscapeSummary,
    VOCLightSummary,
)
from src.market_insight_quality_gate import evaluate_sample_quality
from src.market_insight_sample_pool import build_direction_sample_pool, build_sample_pool_diagnostics


class MarketInsightWriter(object):
    def __init__(self, artifacts_root: Path):
        self.artifacts_root = Path(artifacts_root)
        self.database = MarketInsightDatabase(self.artifacts_root / "market_insight.db")

    def write_product_run(
        self,
        config: MarketInsightConfig,
        input_mode: str,
        snapshots: Iterable[ProductRankingSnapshot],
        scored_items: Iterable[ScoredProductSnapshot],
        direction_cards: Iterable[MarketDirectionCard],
        report_payload: Dict[str, object],
        report_markdown: str,
        voc_summary: VOCLightSummary,
        report_delivery: Dict[str, object] | None = None,
        llm_fallback_count: int = 0,
    ) -> MarketInsightRunResult:
        snapshot_list = list(snapshots)
        run_state = self.start_product_run(
            config=config,
            input_mode=input_mode,
            first_snapshot=snapshot_list[0],
            total_product_count=len(snapshot_list),
            voc_summary=voc_summary,
        )
        return self.update_product_run(
            config=config,
            run_state=run_state,
            snapshots=snapshot_list,
            scored_items=scored_items,
            direction_cards=direction_cards,
            report_payload=report_payload,
            report_markdown=report_markdown,
            report_delivery=report_delivery,
            completed_product_count=len(snapshot_list),
            total_product_count=len(snapshot_list),
            run_status="completed",
            llm_fallback_count=llm_fallback_count,
        )

    def start_product_run(
        self,
        config: MarketInsightConfig,
        input_mode: str,
        first_snapshot: ProductRankingSnapshot,
        total_product_count: int,
        voc_summary: VOCLightSummary,
        source_scope: str = "official",
    ) -> MarketInsightProductRunState:
        run_dir = self._run_dir(config, country=first_snapshot.country, category=first_snapshot.category, batch_date=first_snapshot.batch_date)
        run_dir.mkdir(parents=True, exist_ok=True)
        run_state = MarketInsightProductRunState(
            table_id=config.table_id,
            table_name=config.table_name,
            input_mode=input_mode,
            batch_date=first_snapshot.batch_date,
            country=first_snapshot.country,
            category=first_snapshot.category,
            artifacts_dir=str(run_dir),
            source_scope=source_scope,
            min_consumable_product_count=int(config.min_consumable_product_count or 0),
            min_consumable_direction_count=int(config.min_consumable_direction_count or 0),
            product_snapshot_path=str(run_dir / "market_insight_product_snapshot.json"),
            product_tags_path=str(run_dir / "market_insight_product_tags.json"),
            direction_cards_path=str(run_dir / "market_direction_cards.json"),
            report_json_path=str(run_dir / "market_insight_report.json"),
            report_md_path=str(run_dir / "market_insight_report.md"),
            report_delivery_path=str(run_dir / "market_insight_report_delivery.json"),
            progress_json_path=str(run_dir / "market_insight_progress.json"),
            voc_status=voc_summary.voc_status,
        )
        self.update_product_run(
            config=config,
            run_state=run_state,
            snapshots=[],
            scored_items=[],
            direction_cards=[],
            report_payload={
                "report_version": "market_insight_report_v1",
                "decision_summary": {
                    "enter": {"items": [], "display_names": [], "total_count": 0, "overflow_count": 0},
                    "watch": {"items": [], "display_names": [], "total_count": 0, "overflow_count": 0},
                    "avoid": {"items": [], "display_names": [], "total_count": 0, "overflow_count": 0},
                },
                "opportunity_direction_cards": [],
                "direction_matrix": {"table_lines": [], "matrix": {}, "observations": []},
                "reverse_signals": {"hidden_risks": [], "hidden_opportunities": []},
                "cross_system_recommendations": {
                    "content_route_recommendations": [],
                    "scoring_weight_recommendations": [],
                },
                "voc_summary": voc_summary.to_dict(),
                "llm_meta": {
                    "used_llm": False,
                    "requested_direction_count": 0,
                    "rendered_direction_count": 0,
                    "fallback_count": 0,
                },
            },
            report_markdown="# 市场洞察报告\n",
            report_delivery={"status": "skipped", "message": "运行中，尚未创建飞书文档"},
            completed_product_count=0,
            total_product_count=total_product_count,
            run_status="running",
            llm_fallback_count=0,
        )
        return run_state

    def resume_product_run(
        self,
        config: MarketInsightConfig,
        input_mode: str,
        first_snapshot: ProductRankingSnapshot,
        total_product_count: int,
        voc_summary: VOCLightSummary,
        source_scope: str = "official",
    ):
        run_dir = self._find_resumable_run_dir(
            config=config,
            country=first_snapshot.country,
            category=first_snapshot.category,
            batch_date=first_snapshot.batch_date,
            source_scope=source_scope,
        )
        if run_dir is None:
            return None
        run_state = self._build_run_state(
            config=config,
            input_mode=input_mode,
            run_dir=run_dir,
            batch_date=first_snapshot.batch_date,
            country=first_snapshot.country,
            category=first_snapshot.category,
            voc_status=voc_summary.voc_status,
            source_scope=source_scope,
        )
        existing_tags_by_row_key = self._load_existing_tags_by_row_key(run_state)
        return run_state, existing_tags_by_row_key

    def update_product_run(
        self,
        config: MarketInsightConfig,
        run_state: MarketInsightProductRunState,
        snapshots: Iterable[ProductRankingSnapshot],
        scored_items: Iterable[ScoredProductSnapshot],
        direction_cards: Iterable[MarketDirectionCard],
        report_payload: Dict[str, object],
        report_markdown: str,
        completed_product_count: int,
        total_product_count: int,
        run_status: str,
        report_delivery: Dict[str, object] | None = None,
        llm_fallback_count: int = 0,
    ) -> MarketInsightRunResult:
        scored_list = list(scored_items)
        snapshot_list = list(snapshots)
        card_list = list(direction_cards)
        self._attach_direction_execution_briefs(card_list, report_payload)
        quality_metrics = evaluate_sample_quality(
            scored_items=scored_list,
            completed_product_count=completed_product_count,
            config=config,
        )
        progress_payload = {
            "table_id": run_state.table_id,
            "table_name": run_state.table_name,
            "input_mode": run_state.input_mode,
            "batch_date": run_state.batch_date,
            "country": run_state.country,
            "category": run_state.category,
            "source_scope": run_state.source_scope,
            "completed_product_count": completed_product_count,
            "total_product_count": total_product_count,
            "direction_count": len(card_list),
            "voc_status": run_state.voc_status,
            "run_status": run_status,
            "report_doc_url": str((report_delivery or {}).get("feishu_doc_url") or ""),
            "notification_status": str((report_delivery or {}).get("notification_status") or ""),
            "updated_at_epoch": int(time.time()),
        }
        progress_payload.update(quality_metrics)
        updated_at_epoch = int(progress_payload["updated_at_epoch"])
        is_consumable = bool(
            run_status == "completed"
            and run_state.source_scope == "official"
            and completed_product_count >= int(run_state.min_consumable_product_count or 0)
            and len(card_list) >= int(run_state.min_consumable_direction_count or 0)
            and bool(quality_metrics.get("quality_gate_passed"))
        )
        progress_payload["is_consumable"] = is_consumable

        Path(run_state.product_snapshot_path).write_text(
            json.dumps([item.to_dict() for item in snapshot_list], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        Path(run_state.product_tags_path).write_text(
            json.dumps([item.to_dict() for item in scored_list], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        Path(run_state.direction_cards_path).write_text(
            json.dumps([item.to_dict() for item in card_list], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        sample_pool_path = Path(run_state.artifacts_dir) / "direction_sample_pool.json"
        sample_pool_rows = build_direction_sample_pool(
            scored_items=scored_list,
            direction_cards=card_list,
            config=config,
        )
        sample_pool_diagnostics_path = Path(run_state.artifacts_dir) / "direction_sample_pool_diagnostics.json"
        sample_pool_diagnostics = build_sample_pool_diagnostics(
            scored_items=scored_list,
            direction_cards=card_list,
            rows=sample_pool_rows,
        )
        sample_pool_path.write_text(
            json.dumps(sample_pool_rows, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        sample_pool_diagnostics_path.write_text(
            json.dumps(sample_pool_diagnostics, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        Path(run_state.report_json_path).write_text(
            json.dumps(report_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        Path(run_state.report_md_path).write_text(report_markdown, encoding="utf-8")
        Path(run_state.report_delivery_path).write_text(
            json.dumps(report_delivery or {}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        Path(run_state.progress_json_path).write_text(
            json.dumps(progress_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        self._write_latest_index(
            country=run_state.country,
            category=run_state.category,
            payload={
                "table_id": config.table_id,
                "batch_date": run_state.batch_date,
                "cards_path": run_state.direction_cards_path,
                "sample_pool_path": str(sample_pool_path),
                "sample_pool_diagnostics_path": str(sample_pool_diagnostics_path),
                "report_json_path": run_state.report_json_path,
                "report_md_path": run_state.report_md_path,
                "progress_json_path": run_state.progress_json_path,
                "price_scale_divisor": config.price_scale_divisor,
                "price_band_edges": config.price_band_edges,
                "source_scope": run_state.source_scope,
                "is_consumable": is_consumable,
                **quality_metrics,
                "min_consumable_product_count": int(run_state.min_consumable_product_count or 0),
                "min_consumable_direction_count": int(run_state.min_consumable_direction_count or 0),
                "voc_status": run_state.voc_status,
                "artifacts_dir": run_state.artifacts_dir,
                "database_path": str(self.database.db_path),
                "completed_product_count": completed_product_count,
                "total_product_count": total_product_count,
                "direction_count": len(card_list),
                "run_status": run_status,
                "report_delivery_path": run_state.report_delivery_path,
                "report_doc_url": str((report_delivery or {}).get("feishu_doc_url") or ""),
                "notification_status": str((report_delivery or {}).get("notification_status") or ""),
                "llm_fallback_count": int(llm_fallback_count),
            },
        )
        self.database.upsert_direction_card_run(
            table_id=config.table_id,
            run_state=run_state,
            snapshots=snapshot_list,
            scored_items=scored_list,
            direction_cards=card_list,
            completed_product_count=completed_product_count,
            total_product_count=total_product_count,
            run_status=run_status,
            updated_at_epoch=updated_at_epoch,
            report_json_path=run_state.report_json_path,
            report_md_path=run_state.report_md_path,
            progress_json_path=run_state.progress_json_path,
            report_payload=report_payload,
            report_delivery=report_delivery or {},
            llm_fallback_count=llm_fallback_count,
            source_scope=run_state.source_scope,
            min_consumable_product_count=int(run_state.min_consumable_product_count or 0),
            min_consumable_direction_count=int(run_state.min_consumable_direction_count or 0),
        )

        return MarketInsightRunResult(
            table_id=run_state.table_id,
            table_name=run_state.table_name,
            input_mode=run_state.input_mode,
            batch_date=run_state.batch_date,
            country=run_state.country,
            category=run_state.category,
            artifacts_dir=run_state.artifacts_dir,
            source_scope=run_state.source_scope,
            product_snapshot_count=completed_product_count,
            total_product_count=total_product_count,
            direction_count=len(card_list),
            is_consumable=is_consumable,
            valid_sample_count=int(quality_metrics.get("valid_sample_count") or 0),
            invalid_sample_count=int(quality_metrics.get("invalid_sample_count") or 0),
            valid_sample_ratio=float(quality_metrics.get("valid_sample_ratio") or 0.0),
            quality_gate_passed=bool(quality_metrics.get("quality_gate_passed")),
            quality_gate_reason=str(quality_metrics.get("quality_gate_reason") or ""),
            shop_summary_generated=False,
            voc_status=run_state.voc_status,
            run_status=run_status,
            report_json_path=run_state.report_json_path,
            report_md_path=run_state.report_md_path,
            report_delivery_path=run_state.report_delivery_path,
            report_doc_url=str((report_delivery or {}).get("feishu_doc_url") or ""),
            notification_status=str((report_delivery or {}).get("notification_status") or ""),
            llm_fallback_count=int(llm_fallback_count),
        )

    def _attach_direction_execution_briefs(
        self,
        direction_cards: List[MarketDirectionCard],
        report_payload: Dict[str, object],
    ) -> None:
        """Persist report-generated task briefs into card artifacts and DB rows."""

        briefs_by_key: Dict[str, Dict[str, Any]] = {}
        for item in list(report_payload.get("direction_actions") or []):
            if not isinstance(item, dict):
                continue
            brief = dict(item.get("direction_execution_brief") or {})
            if not brief:
                continue
            for key in (
                str(brief.get("direction_id") or "").strip(),
                str(brief.get("direction_name") or "").strip(),
                str(item.get("direction") or "").strip(),
            ):
                if key:
                    briefs_by_key[key] = brief
        for brief in list(report_payload.get("direction_execution_briefs") or []):
            if not isinstance(brief, dict):
                continue
            for key in (
                str(brief.get("direction_id") or "").strip(),
                str(brief.get("direction_name") or "").strip(),
            ):
                if key:
                    briefs_by_key[key] = dict(brief)

        for card in direction_cards:
            for key in (
                str(card.direction_canonical_key or "").strip(),
                str(card.direction_instance_id or "").strip(),
                str(card.direction_name or "").strip(),
                str(card.style_cluster or "").strip(),
            ):
                if key and key in briefs_by_key:
                    card.direction_execution_brief = dict(briefs_by_key[key])
                    break

        for card_payload in list(report_payload.get("direction_decision_cards") or []):
            if not isinstance(card_payload, dict):
                continue
            for key in (
                str(card_payload.get("direction_canonical_key") or "").strip(),
                str(card_payload.get("direction_instance_id") or "").strip(),
                str(card_payload.get("direction_name") or "").strip(),
                str(card_payload.get("style_cluster") or "").strip(),
            ):
                if key and key in briefs_by_key:
                    card_payload["direction_execution_brief"] = dict(briefs_by_key[key])
                    break

    def write_shop_run(
        self,
        config: MarketInsightConfig,
        input_mode: str,
        summary: ShopLandscapeSummary,
    ) -> MarketInsightRunResult:
        run_dir = self._run_dir(config, country=summary.country, category=summary.category, batch_date=summary.batch_date)
        run_dir.mkdir(parents=True, exist_ok=True)
        output_path = run_dir / "shop_landscape_summary.json"
        output_path.write_text(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        self._write_latest_index(
            country=summary.country,
            category=summary.category,
            payload={
                "table_id": config.table_id,
                "batch_date": summary.batch_date,
                "shop_summary_path": str(output_path),
                "artifacts_dir": str(run_dir),
            },
        )
        return MarketInsightRunResult(
            table_id=config.table_id,
            table_name=config.table_name,
            input_mode=input_mode,
            batch_date=summary.batch_date,
            country=summary.country,
            category=summary.category,
            artifacts_dir=str(run_dir),
            product_snapshot_count=0,
            direction_count=0,
            shop_summary_generated=True,
            voc_status="skipped",
            report_json_path=str(output_path),
            report_md_path="",
        )

    def _run_dir(self, config: MarketInsightConfig, country: str, category: str, batch_date: str) -> Path:
        timestamp = time.strftime("%H%M%S")
        base_dir = Path(config.output_dir) if config.output_dir else self.artifacts_root
        return base_dir / "{country}__{category}".format(country=country, category=category) / "{batch_date}__{table_id}__{timestamp}".format(
            batch_date=batch_date.replace("-", ""),
            table_id=config.table_id,
            timestamp=timestamp,
        )

    def snapshot_row_key(self, snapshot: ProductRankingSnapshot) -> str:
        product_id = str(snapshot.product_id or "").strip()
        if product_id:
            return product_id
        return "rank_{index}".format(index=int(snapshot.rank_index or 0))

    def _build_run_state(
        self,
        config: MarketInsightConfig,
        input_mode: str,
        run_dir: Path,
        batch_date: str,
        country: str,
        category: str,
        voc_status: str,
        source_scope: str,
    ) -> MarketInsightProductRunState:
        return MarketInsightProductRunState(
            table_id=config.table_id,
            table_name=config.table_name,
            input_mode=input_mode,
            batch_date=batch_date,
            country=country,
            category=category,
            artifacts_dir=str(run_dir),
            source_scope=source_scope,
            min_consumable_product_count=int(config.min_consumable_product_count or 0),
            min_consumable_direction_count=int(config.min_consumable_direction_count or 0),
            product_snapshot_path=str(run_dir / "market_insight_product_snapshot.json"),
            product_tags_path=str(run_dir / "market_insight_product_tags.json"),
            direction_cards_path=str(run_dir / "market_direction_cards.json"),
            report_json_path=str(run_dir / "market_insight_report.json"),
            report_md_path=str(run_dir / "market_insight_report.md"),
            report_delivery_path=str(run_dir / "market_insight_report_delivery.json"),
            progress_json_path=str(run_dir / "market_insight_progress.json"),
            voc_status=voc_status,
        )

    def _find_resumable_run_dir(
        self,
        config: MarketInsightConfig,
        country: str,
        category: str,
        batch_date: str,
        source_scope: str,
    ) -> Path | None:
        base_dir = Path(config.output_dir) if config.output_dir else self.artifacts_root
        bucket = base_dir / "{country}__{category}".format(country=country, category=category)
        if not bucket.exists():
            return None
        prefix = "{batch_date}__{table_id}__".format(
            batch_date=batch_date.replace("-", ""),
            table_id=config.table_id,
        )
        candidates = []
        for candidate in [p for p in bucket.iterdir() if p.is_dir() and p.name.startswith(prefix)]:
            progress_path = candidate / "market_insight_progress.json"
            if not progress_path.exists():
                continue
            try:
                progress_payload = json.loads(progress_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if str(progress_payload.get("run_status") or "").strip() != "running":
                continue
            if str(progress_payload.get("source_scope") or source_scope).strip().lower() != str(source_scope or "").strip().lower():
                continue
            try:
                completed_product_count = int(progress_payload.get("completed_product_count") or 0)
            except (TypeError, ValueError):
                completed_product_count = 0
            try:
                updated_at_epoch = int(progress_payload.get("updated_at_epoch") or 0)
            except (TypeError, ValueError):
                updated_at_epoch = 0
            candidates.append((completed_product_count, updated_at_epoch, candidate.name, candidate))
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
        return candidates[0][3]

    def _load_existing_tags_by_row_key(self, run_state: MarketInsightProductRunState) -> Dict[str, object]:
        tags_path = Path(run_state.product_tags_path)
        if not tags_path.exists():
            return {}
        try:
            payload = json.loads(tags_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if not isinstance(payload, list):
            return {}
        results = {}
        for item in payload:
            if not isinstance(item, dict):
                continue
            snapshot_payload = item.get("snapshot") or {}
            tag_payload = item.get("tag") or {}
            if not isinstance(snapshot_payload, dict) or not isinstance(tag_payload, dict):
                continue
            row_key = self._snapshot_row_key_from_payload(snapshot_payload)
            if not row_key:
                continue
            try:
                results[row_key] = self._tag_from_payload(tag_payload)
            except Exception:
                continue
        return results

    def _snapshot_row_key_from_payload(self, payload: Dict[str, object]) -> str:
        product_id = str(payload.get("product_id") or "").strip()
        if product_id:
            return product_id
        try:
            rank_index = int(payload.get("rank_index") or 0)
        except (TypeError, ValueError):
            rank_index = 0
        return "rank_{index}".format(index=rank_index)

    def _tag_from_payload(self, payload: Dict[str, object]):
        from src.market_insight_models import MarketInsightProductTag

        return MarketInsightProductTag(
            is_valid_sample=bool(payload.get("is_valid_sample")),
            style_cluster=str(payload.get("style_cluster") or payload.get("style_tag_main") or "other"),
            style_tags_secondary=[str(item or "").strip() for item in list(payload.get("style_tags_secondary") or []) if str(item or "").strip()],
            product_form=str(payload.get("product_form") or payload.get("product_form_or_result") or "other"),
            length_form=str(payload.get("length_form") or "other"),
            element_tags=[str(item or "").strip() for item in list(payload.get("element_tags") or []) if str(item or "").strip()],
            value_points=[str(item or "").strip() for item in list(payload.get("value_points", payload.get("buying_motives")) or []) if str(item or "").strip()],
            scene_tags=[str(item or "").strip() for item in list(payload.get("scene_tags") or []) if str(item or "").strip()],
            reason_short=str(payload.get("reason_short") or "")[:40],
        )

    def _write_latest_index(self, country: str, category: str, payload: Dict[str, object]) -> None:
        latest_dir = self.artifacts_root / "latest"
        latest_dir.mkdir(parents=True, exist_ok=True)
        latest_path = latest_dir / "{country}__{category}.json".format(country=country, category=category)
        latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
