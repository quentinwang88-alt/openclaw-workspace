import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.enums import AnalysisStatus  # noqa: E402
from src.models import CandidateTask, ReadFilterConfig, ScoredAnalysisResult, TableConfig  # noqa: E402
from src.result_writer import ResultWriter  # noqa: E402


def build_config():
    return TableConfig(
        table_id="th_new_store_selection",
        table_name="服装新店选品-选品表",
        enabled=True,
        source_type="manual",
        supported_manual_categories=["发饰", "轻上装"],
        read_filter=ReadFilterConfig(status_field="分析状态", pending_values=["待处理", ""]),
        field_map={"product_images": "产品图"},
        writeback_map={
            "analysis_status": "分析状态",
            "analysis_time": "分析时间",
            "analysis_error": "分析异常",
            "early_rising": "早期起量",
        },
    )


class ResultWriterTest(unittest.TestCase):
    def test_writes_early_rising_checkbox_for_fast_new_product(self):
        task = CandidateTask(
            source_table_id="th_new_store_selection",
            source_record_id="rec_1",
            extra_fields={"7日销量": 302, "上架天数": 75},
        )

        fields = ResultWriter().build_writeback_fields(
            table_config=build_config(),
            status=AnalysisStatus.COMPLETED.value,
            task=task,
            scored_result=ScoredAnalysisResult(
                analysis_category="轻上装",
                product_potential="中",
                content_potential="中",
                batch_priority_score=70,
                suggested_action="低成本试款",
                brief_reason="",
            ),
        )

        self.assertIs(fields["早期起量"], True)

    def test_clears_early_rising_when_age_or_sales_misses_threshold(self):
        task = CandidateTask(
            source_table_id="th_new_store_selection",
            source_record_id="rec_2",
            extra_fields={"7日销量": 49, "上架天数": 75},
        )

        fields = ResultWriter().build_writeback_fields(
            table_config=build_config(),
            status=AnalysisStatus.COMPLETED.value,
            task=task,
            scored_result=ScoredAnalysisResult(
                analysis_category="轻上装",
                product_potential="中",
                content_potential="中",
                batch_priority_score=70,
                suggested_action="低成本试款",
                brief_reason="",
            ),
        )

        self.assertIs(fields["早期起量"], False)


if __name__ == "__main__":
    unittest.main()
