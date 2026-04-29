import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_insight_input_mode_detector import PRODUCT_MODE  # noqa: E402
from src.market_insight_models import (  # noqa: E402
    MarketDirectionCard,
    MarketInsightConfig,
    MarketInsightProductRunState,
    MarketInsightProductTag,
    MarketInsightRunResult,
    ProductRankingSnapshot,
    ScoredProductSnapshot,
)
from src.market_insight_pipeline import MarketInsightPipeline  # noqa: E402


def build_snapshot(product_id: str, rank_index: int) -> ProductRankingSnapshot:
    return ProductRankingSnapshot(
        batch_date="2026-04-21",
        batch_id="batch_1",
        country="VN",
        category="hair_accessory",
        product_id=product_id,
        product_name=f"商品{product_id}",
        shop_name="店铺A",
        price_min=10.0,
        price_max=20.0,
        price_mid=15.0,
        sales_7d=10.0 + rank_index,
        gmv_7d=100.0 + rank_index,
        creator_count=2.0,
        video_count=3.0,
        listing_days=5,
        product_images=["https://cdn.example.com/1.jpg"],
        image_url="https://cdn.example.com/1.jpg",
        product_url=f"https://example.com/{product_id}",
        rank_index=rank_index,
    )


def build_tag(reason: str, is_valid_sample: bool = True) -> MarketInsightProductTag:
    return MarketInsightProductTag(
        is_valid_sample=is_valid_sample,
        style_cluster="韩系轻通勤型",
        style_tags_secondary=[],
        product_form="抓夹",
        element_tags=["布艺"],
        value_points=["提升精致度"],
        scene_tags=["出门"],
        reason_short=reason,
    )


class FakeAdapter(object):
    def detect_input_mode(self, config, client):
        return PRODUCT_MODE

    def read_product_snapshots(self, config, client, batch_date_override="", limit=None):
        return [build_snapshot("p1", 1), build_snapshot("p2", 2)]


class FakeTaxonomyLoader(object):
    def load(self, category):
        return {"style_cluster": ["韩系轻通勤型", "other"], "product_form": ["抓夹", "other"]}


class FakeAnalyzer(object):
    def iter_tag_products(self, snapshots, taxonomy, max_workers=1):
        if len(list(snapshots)) == 1:
            yield 0, build_tag("第二条恢复完成")
            return
        yield 1, build_tag("第二条先完成")
        yield 0, build_tag("第一条后完成")


class LowQualityAnalyzer(object):
    def iter_tag_products(self, snapshots, taxonomy, max_workers=1):
        yield 0, build_tag("第一条有效", is_valid_sample=True)
        yield 1, build_tag("第二条无效", is_valid_sample=False)


class FakeScoringEngine(object):
    def score_products(self, snapshots, tags, config):
        results = []
        for snapshot, tag in zip(snapshots, tags):
            results.append(
                ScoredProductSnapshot(
                    snapshot=snapshot,
                    tag=tag,
                    heat_score=80.0,
                    heat_level="high",
                    crowd_score=40.0,
                    crowd_level="medium",
                    priority_level="high",
                    target_price_band="0-50",
                )
            )
        return results


class FakeAggregator(object):
    def build_direction_cards(self, scored_items):
        items = list(scored_items)
        if not items:
            return []
        return [
            MarketDirectionCard(
                direction_canonical_key="VN__hair_accessory__韩系轻通勤型",
                direction_instance_id="2026-04-21__VN__hair_accessory__韩系轻通勤型",
                batch_date="2026-04-21",
                country="VN",
                category="hair_accessory",
                direction_name="韩系轻通勤型",
                style_cluster="韩系轻通勤型",
                top_forms=["抓夹"],
                form_distribution={"抓夹": 100.0},
                core_elements=["布艺"],
                scene_tags=["出门"],
                target_price_bands=["0-50"],
                heat_level="high",
                crowd_level="medium",
                top_value_points=["提升精致度"],
                priority_level="high",
                representative_products=[
                    {"product_id": item.snapshot.product_id, "product_name": item.snapshot.product_name}
                    for item in items
                ],
                selection_advice="优先补韩系轻通勤。",
                avoid_notes="不要只补相似外观。",
                confidence=0.8,
                product_count=len(items),
                average_heat_score=80.0,
                average_crowd_score=40.0,
                direction_key="VN__hair_accessory__韩系轻通勤型",
            )
        ]

class FakeReportGenerator(object):
    def __init__(self):
        self.calls = []

    def generate_report(self, cards, voc_summary, country, category, batch_date, use_llm, **kwargs):
        self.calls.append({"card_count": len(list(cards)), "use_llm": use_llm})
        return (
            {
                "decision_summary": {
                    "enter": {"items": [], "display_names": [], "total_count": 0, "overflow_count": 0},
                    "watch": {"items": [], "display_names": [], "total_count": 0, "overflow_count": 0},
                    "avoid": {"items": [], "display_names": [], "total_count": 0, "overflow_count": 0},
                }
            },
            "# report\n",
            {"fallback_count": 0},
        )


class FakeReportPublisher(object):
    def __init__(self):
        self.calls = []

    def publish(self, report_markdown, report_payload, country, category, batch_date, report_output):
        self.calls.append(
            {
                "country": country,
                "category": category,
                "batch_date": batch_date,
                "report_output": dict(report_output or {}),
            }
        )
        return {
            "status": "published",
            "message": "ok",
            "feishu_doc_url": "https://feishu.cn/docx/test",
            "notification_status": "sent",
        }


class FakeWriter(object):
    def __init__(self, resume_tags_by_row_key=None):
        self.completed_counts = []
        self.statuses = []
        self.source_scopes = []
        self.deliveries = []
        self.resume_tags_by_row_key = dict(resume_tags_by_row_key or {})

    def start_product_run(self, config, input_mode, first_snapshot, total_product_count, voc_summary, source_scope="official"):
        self.source_scopes.append(source_scope)
        return MarketInsightProductRunState(
            table_id=config.table_id,
            table_name=config.table_name,
            input_mode=input_mode,
            batch_date=first_snapshot.batch_date,
            country=first_snapshot.country,
            category=first_snapshot.category,
            artifacts_dir="/tmp/test-artifacts",
            source_scope=source_scope,
            min_consumable_product_count=int(config.min_consumable_product_count or 0),
            min_consumable_direction_count=int(config.min_consumable_direction_count or 0),
            product_snapshot_path="/tmp/test-artifacts/snapshots.json",
            product_tags_path="/tmp/test-artifacts/tags.json",
            direction_cards_path="/tmp/test-artifacts/cards.json",
            report_json_path="/tmp/test-artifacts/report.json",
            report_md_path="/tmp/test-artifacts/report.md",
            report_delivery_path="/tmp/test-artifacts/report_delivery.json",
            progress_json_path="/tmp/test-artifacts/progress.json",
            voc_status=voc_summary.voc_status,
        )

    def resume_product_run(self, config, input_mode, first_snapshot, total_product_count, voc_summary, source_scope="official"):
        if not self.resume_tags_by_row_key:
            return None
        self.source_scopes.append(source_scope)
        return (
            MarketInsightProductRunState(
                table_id=config.table_id,
                table_name=config.table_name,
                input_mode=input_mode,
                batch_date=first_snapshot.batch_date,
                country=first_snapshot.country,
                category=first_snapshot.category,
                artifacts_dir="/tmp/test-artifacts",
                source_scope=source_scope,
                min_consumable_product_count=int(config.min_consumable_product_count or 0),
                min_consumable_direction_count=int(config.min_consumable_direction_count or 0),
                product_snapshot_path="/tmp/test-artifacts/snapshots.json",
                product_tags_path="/tmp/test-artifacts/tags.json",
                direction_cards_path="/tmp/test-artifacts/cards.json",
                report_json_path="/tmp/test-artifacts/report.json",
                report_md_path="/tmp/test-artifacts/report.md",
                report_delivery_path="/tmp/test-artifacts/report_delivery.json",
                progress_json_path="/tmp/test-artifacts/progress.json",
                voc_status=voc_summary.voc_status,
            ),
            dict(self.resume_tags_by_row_key),
        )

    def snapshot_row_key(self, snapshot):
        return str(snapshot.product_id or "") or f"rank_{int(snapshot.rank_index or 0)}"

    def update_product_run(
        self,
        config,
        run_state,
        snapshots,
        scored_items,
        direction_cards,
        report_payload,
        report_markdown,
        completed_product_count,
        total_product_count,
        run_status,
        report_delivery=None,
        llm_fallback_count=0,
    ):
        self.completed_counts.append(completed_product_count)
        self.statuses.append(run_status)
        self.deliveries.append(dict(report_delivery or {}))
        return MarketInsightRunResult(
            table_id=config.table_id,
            table_name=config.table_name,
            input_mode=run_state.input_mode,
            batch_date=run_state.batch_date,
            country=run_state.country,
            category=run_state.category,
            artifacts_dir=run_state.artifacts_dir,
            product_snapshot_count=completed_product_count,
            total_product_count=total_product_count,
            direction_count=len(list(direction_cards)),
            voc_status=run_state.voc_status,
            run_status=run_status,
            report_json_path=run_state.report_json_path,
            report_md_path=run_state.report_md_path,
            report_delivery_path=run_state.report_delivery_path,
            report_doc_url=str((report_delivery or {}).get("feishu_doc_url") or ""),
            notification_status=str((report_delivery or {}).get("notification_status") or ""),
            llm_fallback_count=llm_fallback_count,
        )


class FakeSyncer(object):
    def __init__(self):
        self.calls = []

    def maybe_sync(self, run_result):
        self.calls.append((run_result.product_snapshot_count, run_result.run_status))
        return {"ok": True}


class MarketInsightPipelineTest(unittest.TestCase):
    def test_process_table_updates_progress_for_each_completed_sample(self):
        writer = FakeWriter()
        syncer = FakeSyncer()
        report_generator = FakeReportGenerator()
        report_publisher = FakeReportPublisher()
        pipeline = MarketInsightPipeline(
            table_adapter=FakeAdapter(),
            taxonomy_loader=FakeTaxonomyLoader(),
            analyzer=FakeAnalyzer(),
            scoring_engine=FakeScoringEngine(),
            aggregator=FakeAggregator(),
            writer=writer,
            report_generator=report_generator,
            report_publisher=report_publisher,
            progress_syncer=syncer,
        )

        result = pipeline.process_table(
            config=MarketInsightConfig(table_id="table_1", table_name="测试表", enabled=True),
            client=object(),
            max_workers=4,
        )

        self.assertEqual(writer.completed_counts, [1, 2])
        self.assertEqual(writer.statuses, ["running", "completed"])
        self.assertEqual(syncer.calls, [(1, "running"), (2, "completed")])
        self.assertEqual([item["use_llm"] for item in report_generator.calls], [False, True])
        self.assertEqual(len(report_publisher.calls), 1)
        self.assertEqual(result.product_snapshot_count, 2)
        self.assertEqual(result.total_product_count, 2)
        self.assertEqual(result.run_status, "completed")
        self.assertEqual(writer.source_scopes, ["official"])

    def test_completed_report_is_blocked_when_valid_sample_ratio_is_too_low(self):
        writer = FakeWriter()
        report_publisher = FakeReportPublisher()
        pipeline = MarketInsightPipeline(
            table_adapter=FakeAdapter(),
            taxonomy_loader=FakeTaxonomyLoader(),
            analyzer=LowQualityAnalyzer(),
            scoring_engine=FakeScoringEngine(),
            aggregator=FakeAggregator(),
            writer=writer,
            report_generator=FakeReportGenerator(),
            report_publisher=report_publisher,
            progress_syncer=None,
        )

        result = pipeline.process_table(
            config=MarketInsightConfig(
                table_id="table_1",
                table_name="测试表",
                enabled=True,
                min_report_valid_sample_ratio=0.70,
            ),
            client=object(),
            max_workers=1,
        )

        self.assertEqual(result.run_status, "completed")
        self.assertEqual(len(report_publisher.calls), 0)
        self.assertEqual(writer.deliveries[-1]["status"], "blocked")
        self.assertIn("有效样本率", writer.deliveries[-1]["message"])

    def test_process_table_marks_small_limited_run_as_smoke_test(self):
        writer = FakeWriter()
        pipeline = MarketInsightPipeline(
            table_adapter=FakeAdapter(),
            taxonomy_loader=FakeTaxonomyLoader(),
            analyzer=FakeAnalyzer(),
            scoring_engine=FakeScoringEngine(),
            aggregator=FakeAggregator(),
            writer=writer,
            report_generator=FakeReportGenerator(),
            report_publisher=None,
            progress_syncer=None,
        )

        pipeline.process_table(
            config=MarketInsightConfig(
                table_id="table_1",
                table_name="测试表",
                enabled=True,
                min_consumable_product_count=100,
            ),
            client=object(),
            limit=1,
            max_workers=1,
        )

        self.assertEqual(writer.source_scopes, ["smoke_test"])

    def test_process_table_resumes_from_existing_partial_tags(self):
        writer = FakeWriter(resume_tags_by_row_key={"p1": build_tag("第一条已完成")})
        pipeline = MarketInsightPipeline(
            table_adapter=FakeAdapter(),
            taxonomy_loader=FakeTaxonomyLoader(),
            analyzer=FakeAnalyzer(),
            scoring_engine=FakeScoringEngine(),
            aggregator=FakeAggregator(),
            writer=writer,
            report_generator=FakeReportGenerator(),
            report_publisher=None,
            progress_syncer=None,
        )

        result = pipeline.process_table(
            config=MarketInsightConfig(table_id="table_1", table_name="测试表", enabled=True),
            client=object(),
            max_workers=4,
        )

        self.assertEqual(writer.completed_counts, [2])
        self.assertEqual(writer.statuses, ["completed"])
        self.assertEqual(result.product_snapshot_count, 2)
        self.assertEqual(writer.source_scopes, ["official"])


if __name__ == "__main__":
    unittest.main()
