import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.candidate_detail_store import CandidateDetailStore  # noqa: E402
from src.models import (  # noqa: E402
    CandidateTask,
    DecisionReason,
    FeatureAnalysisResult,
    ReadFilterConfig,
    ScoredAnalysisResult,
    TableConfig,
    TableSourceConfig,
)


def build_table_config():
    return TableConfig(
        table_id="manual_table_hair_01",
        table_name="发饰人工选品表",
        enabled=True,
        source_type="manual",
        batch_field="分析批次",
        source=TableSourceConfig(),
        supported_manual_categories=["发饰", "轻上装"],
        read_filter=ReadFilterConfig(status_field="分析状态", pending_values=["待处理", ""]),
        field_map={},
        writeback_map={"analysis_status": "分析状态"},
    )


def build_task(record_id: str, title: str) -> CandidateTask:
    return CandidateTask(
        source_table_id="manual_table_hair_01",
        source_record_id=record_id,
        batch_id="batch_1",
        product_title=title,
        target_market="VN",
        manual_category="发饰",
        final_category="发饰",
        category_confidence="manual",
    )


def build_feature_result() -> FeatureAnalysisResult:
    return FeatureAnalysisResult(
        analysis_category="发饰",
        feature_scores={
            "wearing_change_strength": "高",
            "demo_ease": "高",
            "visual_memory_point": "中",
            "homogenization_risk": "中",
            "title_selling_clarity": "高",
            "info_completeness": "中",
        },
        risk_tag="无明显主要风险",
        risk_note="信息完整",
        brief_observation="适合快速整理头发类内容",
    )


def build_scored_result(core_score_a: float, route_a: str, core_score_b: float, route_b: str) -> ScoredAnalysisResult:
    return ScoredAnalysisResult(
        analysis_category="发饰",
        product_potential="高",
        content_potential="高",
        batch_priority_score=core_score_a,
        suggested_action="优先测试" if route_a == "priority_test" else "低成本试款",
        brief_reason="测试结果",
        market_match_score=82.0,
        market_match_status="matched",
        store_fit_score=76.0,
        content_potential_score=84.0,
        core_score_a=core_score_a,
        route_a=route_a,
        core_score_b=core_score_b,
        route_b=route_b,
        supply_check_status="pass",
        supply_summary="供给证据完整",
        decision_reason=DecisionReason(
            primary_drivers=["core_score_high"],
            supporting_factors=["market_match_ok", "store_fit_ok", "supply_pass"],
            narrative="测试结果",
        ),
        observation_tags=["style_borderline"] if route_a != route_b else [],
    )


class CandidateDetailStoreTest(unittest.TestCase):
    def test_persist_result_writes_ab_reports(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = CandidateDetailStore(root / "candidate_analysis.db")
            table_config = build_table_config()
            run_id = store.start_run(table_config=table_config, record_scope="all", flush_every=1)

            store.persist_result(
                table_config=table_config,
                record_id="rec_1",
                status="已完成分析",
                recognized_category="发饰",
                category_confidence="manual",
                task=build_task("rec_1", "头盔友好大抓夹"),
                feature_result=build_feature_result(),
                scored_result=build_scored_result(82.0, "priority_test", 74.0, "small_test"),
                run_id=run_id,
                visible_status="已完成",
            )
            store.persist_result(
                table_config=table_config,
                record_id="rec_2",
                status="已完成分析",
                recognized_category="发饰",
                category_confidence="manual",
                task=build_task("rec_2", "低饱和纯色发夹"),
                feature_result=build_feature_result(),
                scored_result=build_scored_result(71.0, "small_test", 79.0, "priority_test"),
                run_id=run_id,
                visible_status="已完成",
            )

            reports = store.finish_run(
                run_id=run_id,
                table_config=table_config,
                summary={"processed": 2, "completed": 2, "failed": 0},
                alerts=[{"type": "direction_uncovered_ratio_high", "batch_key": "batch_1"}],
            )

            ab_markdown = Path(reports["ab_diff_markdown_path"]).read_text(encoding="utf-8")
            recent_markdown = Path(reports["recent_diff_markdown_path"]).read_text(encoding="utf-8")
            self.assertIn("A/B 权重差异样本", ab_markdown)
            self.assertIn("rec_1", ab_markdown)
            self.assertIn("最近运行差异报告", recent_markdown)

            with sqlite3.connect(str(root / "candidate_analysis.db")) as conn:
                run_row = conn.execute(
                    "SELECT alert_count, ab_diff_report_path, recent_diff_report_path FROM candidate_analysis_runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                result_row = conn.execute(
                    "SELECT visible_status, market_match_status, route_a, route_b FROM candidate_analysis_results WHERE source_record_id = ?",
                    ("rec_1",),
                ).fetchone()

            self.assertEqual(run_row[0], 1)
            self.assertTrue(str(run_row[1]).endswith(".md"))
            self.assertTrue(str(run_row[2]).endswith(".md"))
            self.assertEqual(result_row[0], "已完成")
            self.assertEqual(result_row[1], "matched")
            self.assertEqual(result_row[2], "priority_test")
            self.assertEqual(result_row[3], "small_test")

    def test_old_schema_is_migrated_before_writing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            db_path = root / "candidate_analysis.db"
            with sqlite3.connect(str(db_path)) as conn:
                conn.execute(
                    """
                    CREATE TABLE candidate_analysis_runs (
                        run_id TEXT PRIMARY KEY,
                        source_table_id TEXT NOT NULL DEFAULT '',
                        started_at_epoch INTEGER NOT NULL DEFAULT 0
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE candidate_analysis_results (
                        source_table_id TEXT NOT NULL,
                        source_record_id TEXT NOT NULL,
                        analysis_status TEXT NOT NULL DEFAULT '',
                        PRIMARY KEY (source_table_id, source_record_id)
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE candidate_analysis_run_results (
                        run_id TEXT NOT NULL,
                        source_table_id TEXT NOT NULL DEFAULT '',
                        source_record_id TEXT NOT NULL,
                        analysis_status TEXT NOT NULL DEFAULT '',
                        PRIMARY KEY (run_id, source_table_id, source_record_id)
                    )
                    """
                )
                conn.commit()

            store = CandidateDetailStore(db_path)
            table_config = build_table_config()
            run_id = store.start_run(table_config=table_config, record_scope="all", flush_every=1)
            store.persist_result(
                table_config=table_config,
                record_id="rec_legacy",
                status="已完成分析",
                recognized_category="发饰",
                category_confidence="manual",
                task=build_task("rec_legacy", "旧库兼容抓夹"),
                feature_result=build_feature_result(),
                scored_result=build_scored_result(78.0, "small_test", 81.0, "priority_test"),
                run_id=run_id,
                visible_status="已完成",
            )

            with sqlite3.connect(str(db_path)) as conn:
                row = conn.execute(
                    "SELECT visible_status, core_score_a, route_b FROM candidate_analysis_run_results WHERE run_id = ? AND source_record_id = ?",
                    (run_id, "rec_legacy"),
                ).fetchone()

            self.assertEqual(row[0], "已完成")
            self.assertEqual(row[1], 78.0)
            self.assertEqual(row[2], "priority_test")


if __name__ == "__main__":
    unittest.main()
