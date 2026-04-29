import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.enums import AnalysisStatus  # noqa: E402
from src.feishu import TableRecord  # noqa: E402
from src.models import (  # noqa: E402
    CategoryIdentificationResult,
    FeatureAnalysisResult,
    PendingAnalysisItem,
    ReadFilterConfig,
    ScoredAnalysisResult,
    TableConfig,
    TableSourceConfig,
    TitleParseResult,
)
from src.pipeline import CandidateAnalysisPipeline  # noqa: E402
from src.result_writer import ResultWriter  # noqa: E402
from src.rule_checker import RuleChecker  # noqa: E402
from src.table_adapter import TableAdapter  # noqa: E402


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
        field_map={
            "product_title": "产品标题",
            "product_images": "产品图片",
            "cost_price": "采购价",
            "target_price": "拟售价",
            "manual_category": "人工类目",
            "product_notes": "产品备注",
            "competitor_notes": "竞品备注",
            "competitor_links": "竞品链接",
            "target_market": "目标国家",
        },
        writeback_map={
            "analysis_status": "分析状态",
            "recognized_category": "识别类目",
            "category_confidence": "识别置信度",
            "title_keyword_tags": "标题关键词",
            "feature_scores_json": "特征打点JSON",
            "risk_tag": "主要风险标签",
            "risk_note": "风险补充说明",
            "product_potential": "产品潜力",
            "content_potential": "内容潜力",
            "market_match_score": "市场匹配分",
            "store_fit_score": "店铺匹配分",
            "content_potential_score": "内容可做性分",
            "batch_priority_score": "批内优先级分",
            "core_score_a": "批内优先级分_A",
            "route_a": "路线决策_A",
            "core_score_b": "批内优先级分_B",
            "route_b": "路线决策_B",
            "supply_check_status": "供给检查状态",
            "supply_summary": "供给摘要",
            "competition_reference_level": "竞争参考等级",
            "competition_confidence": "竞争参考置信度",
            "decision_reason": "决策原因JSON",
            "recommended_content_formulas": "推荐内容公式",
            "reserve_reason": "备用池原因",
            "reserve_created_at": "备用池创建时间",
            "reserve_expires_at": "备用池过期时间",
            "reserve_status": "备用池状态",
            "sample_check_status": "样品检查状态",
            "suggested_action": "建议动作",
            "brief_reason": "简短理由",
            "analysis_time": "分析时间",
            "analysis_error": "分析异常",
            "matched_market_direction_id": "匹配市场方向ID",
            "matched_market_direction_name": "匹配市场方向名称",
            "matched_market_direction_reason": "匹配市场方向理由",
        },
    )


class FakeClient(object):
    def __init__(self, records):
        self.records = list(records)
        self.update_calls = []

    def list_records(self, page_size=100, limit=None):
        items = list(self.records)
        if limit is not None:
            return items[:limit]
        return items

    def update_record_fields(self, record_id, fields):
        self.update_calls.append((record_id, dict(fields)))
        for record in self.records:
            if record.record_id == record_id:
                record.fields.update(fields)
                return
        raise KeyError(record_id)

    def get_tmp_download_url(self, file_token):
        return "https://download.example.com/{token}.jpg".format(token=file_token)


class FakeDetailStore(object):
    def __init__(self):
        self.calls = []

    def persist_result(self, **kwargs):
        self.calls.append(dict(kwargs))


class FakeTitleParser(object):
    def parse_title(self, product_title):
        return TitleParseResult(
            title_keyword_tags=["薄款", "开衫"],
            title_category_hint="轻上装",
            title_category_confidence="high",
        )


class FakeAnalyzer(object):
    def __init__(self, category_result=None, feature_result=None, feature_error=None):
        self.category_result = category_result
        self.feature_result = feature_result
        self.feature_error = feature_error
        self.identify_calls = 0
        self.feature_calls = []

    def identify_category(self, task):
        self.identify_calls += 1
        return self.category_result

    def analyze_features(self, task, final_category):
        self.feature_calls.append(final_category)
        if self.feature_error:
            raise self.feature_error
        return self.feature_result


class FakeScoringEngine(object):
    def score_candidate(self, task, feature_result, market_direction_result=None):
        return ScoredAnalysisResult(
            analysis_category=feature_result.analysis_category,
            product_potential="中",
            content_potential="中",
            batch_priority_score=76,
            suggested_action="低成本试款",
            brief_reason=feature_result.brief_observation,
            market_match_score=78,
            store_fit_score=66,
            content_potential_score=74,
            core_score_a=76,
            route_a="small_test",
            core_score_b=79,
            route_b="priority_test",
            supply_check_status="pass",
            supply_summary="供给证据完整",
            competition_reference_level="medium",
            competition_confidence="low",
            recommended_content_formulas=["镜头对比", "特写卖点"],
            matched_market_direction_id=getattr(market_direction_result, "matched_market_direction_id", ""),
            matched_market_direction_name=getattr(market_direction_result, "matched_market_direction_name", ""),
            matched_market_direction_reason=getattr(market_direction_result, "matched_market_direction_reason", ""),
        )

    def calibrate_group(self, items):
        calibrated = []
        for index, item in enumerate(items):
            action = "优先测试" if index == 0 else "低成本试款"
            calibrated.append(
                PendingAnalysisItem(
                    task=item.task,
                    feature_result=item.feature_result,
                    scored_result=ScoredAnalysisResult(
                        analysis_category=item.scored_result.analysis_category,
                        product_potential=item.scored_result.product_potential,
                        content_potential=item.scored_result.content_potential,
                        batch_priority_score=item.scored_result.batch_priority_score - index * 5,
                        suggested_action=action,
                        brief_reason=item.scored_result.brief_reason,
                        market_match_score=item.scored_result.market_match_score,
                        store_fit_score=item.scored_result.store_fit_score,
                        content_potential_score=item.scored_result.content_potential_score,
                        core_score_a=item.scored_result.core_score_a - index * 5,
                        route_a="priority_test" if index == 0 else "small_test",
                        core_score_b=item.scored_result.core_score_b - index * 5,
                        route_b="priority_test" if index == 0 else "small_test",
                        supply_check_status=item.scored_result.supply_check_status,
                        supply_summary=item.scored_result.supply_summary,
                        competition_reference_level=item.scored_result.competition_reference_level,
                        competition_confidence=item.scored_result.competition_confidence,
                        decision_reason=item.scored_result.decision_reason,
                        recommended_content_formulas=item.scored_result.recommended_content_formulas,
                        reserve_reason=item.scored_result.reserve_reason,
                        reserve_created_at=item.scored_result.reserve_created_at,
                        reserve_expires_at=item.scored_result.reserve_expires_at,
                        reserve_status=item.scored_result.reserve_status,
                        sample_check_status=item.scored_result.sample_check_status,
                        matched_market_direction_id=item.scored_result.matched_market_direction_id,
                        matched_market_direction_name=item.scored_result.matched_market_direction_name,
                        matched_market_direction_reason=item.scored_result.matched_market_direction_reason,
                    ),
                )
            )
        return calibrated


class FakeMarketDirectionMatcher(object):
    def match_candidate(self, task, final_category):
        return type(
            "MatchResult",
            (),
            {
                "matched_market_direction_id": "VN__hair_accessory__头盔友好整理型__抓夹",
                "matched_market_direction_name": "头盔友好整理型抓夹",
                "matched_market_direction_reason": "标题/关键词命中 快速整理头发、低饱和纯色",
            },
        )()


class PipelineTest(unittest.TestCase):
    def test_manual_category_takes_priority_and_writes_v2_result(self):
        records = [
            TableRecord(
                record_id="rec_1",
                fields={
                    "分析状态": "待处理",
                    "分析批次": "batch_1",
                    "产品标题": "薄款针织开衫",
                    "产品图片": ["https://cdn.example.com/1.jpg"],
                    "人工类目": "轻上装",
                },
            ),
            TableRecord(
                record_id="rec_2",
                fields={
                    "分析状态": "待处理",
                    "分析批次": "batch_1",
                    "产品标题": "防晒罩衫",
                    "产品图片": ["https://cdn.example.com/2.jpg"],
                    "人工类目": "轻上装",
                },
            ),
        ]
        client = FakeClient(records=records)
        analyzer = FakeAnalyzer(
            feature_result=FeatureAnalysisResult(
                analysis_category="轻上装",
                feature_scores={
                    "upper_body_change_strength": "中",
                    "camera_readability": "中",
                    "design_signal_strength": "低",
                    "basic_style_escape_strength": "低",
                    "title_selling_clarity": "高",
                    "info_completeness": "中",
                },
                risk_tag="脱离基础款能力弱",
                risk_note="镜头支撑一般",
                brief_observation="更像基础稳款，拉开差距能力有限",
            )
        )
        detail_store = FakeDetailStore()
        pipeline = CandidateAnalysisPipeline(
            table_adapter=TableAdapter(),
            rule_checker=RuleChecker(),
            title_parser=FakeTitleParser(),
            analyzer=analyzer,
            scoring_engine=FakeScoringEngine(),
            result_writer=ResultWriter(detail_store=detail_store),
        )

        summary = pipeline.process_table(build_table_config(), client)

        self.assertEqual(summary["completed"], 2)
        self.assertEqual(analyzer.identify_calls, 0)
        self.assertEqual(analyzer.feature_calls, ["轻上装", "轻上装"])
        self.assertEqual(records[0].fields["分析状态"], "已完成")
        self.assertEqual(records[0].fields["识别类目"], "轻上装")
        self.assertEqual(records[0].fields["识别置信度"], "manual")
        self.assertEqual(records[0].fields["建议动作"], "优先测试")
        self.assertEqual(records[1].fields["建议动作"], "低成本试款")
        self.assertEqual(records[0].fields["市场匹配分"], 78)
        self.assertEqual(records[0].fields["标题关键词"], "")
        self.assertEqual(records[0].fields["路线决策_A"], "")
        self.assertEqual(records[0].fields["特征打点JSON"], "")
        self.assertGreaterEqual(len(detail_store.calls), 2)
        rec_1_calls = [item for item in detail_store.calls if item["record_id"] == "rec_1"]
        self.assertEqual(rec_1_calls[-1]["scored_result"].route_a, "priority_test")

    def test_low_confidence_category_goes_to_manual_confirmation(self):
        record = TableRecord(
            record_id="rec_1",
            fields={
                "分析状态": "待处理",
                "分析批次": "batch_1",
                "产品标题": "未知款式",
                "产品图片": ["https://cdn.example.com/1.jpg"],
            },
        )
        client = FakeClient(records=[record])
        analyzer = FakeAnalyzer(
            category_result=CategoryIdentificationResult(
                predicted_category="轻上装",
                confidence="low",
                reason="标题线索弱，图片也不稳定",
            )
        )
        pipeline = CandidateAnalysisPipeline(
            table_adapter=TableAdapter(),
            rule_checker=RuleChecker(),
            title_parser=FakeTitleParser(),
            analyzer=analyzer,
            scoring_engine=FakeScoringEngine(),
            result_writer=ResultWriter(),
        )

        summary = pipeline.process_table(build_table_config(), client)

        self.assertEqual(summary["completed"], 0)
        self.assertEqual(summary["failed"], 0)
        self.assertEqual(record.fields["分析状态"], "异常中断")
        self.assertEqual(record.fields["识别类目"], "轻上装")
        self.assertEqual(record.fields["识别置信度"], "low")
        self.assertEqual(record.fields["标题关键词"], "")

    def test_completed_result_can_write_market_direction_match_fields(self):
        record = TableRecord(
            record_id="rec_1",
            fields={
                "分析状态": "待处理",
                "分析批次": "batch_1",
                "产品标题": "低饱和蓝色大抓夹",
                "产品图片": ["https://cdn.example.com/1.jpg"],
                "人工类目": "发饰",
                "目标国家": "VN",
            },
        )
        client = FakeClient(records=[record])
        analyzer = FakeAnalyzer(
            feature_result=FeatureAnalysisResult(
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
                risk_note="结构点和整理感都较明确",
                brief_observation="适合做快速整理头发内容",
            )
        )
        detail_store = FakeDetailStore()
        pipeline = CandidateAnalysisPipeline(
            table_adapter=TableAdapter(),
            rule_checker=RuleChecker(),
            title_parser=FakeTitleParser(),
            analyzer=analyzer,
            scoring_engine=FakeScoringEngine(),
            result_writer=ResultWriter(detail_store=detail_store),
            market_direction_matcher=FakeMarketDirectionMatcher(),
        )

        summary = pipeline.process_table(build_table_config(), client)

        self.assertEqual(summary["completed"], 1)
        self.assertEqual(record.fields["匹配市场方向ID"], "")
        self.assertEqual(record.fields["匹配市场方向名称"], "")
        self.assertEqual(record.fields["匹配市场方向理由"], "")
        self.assertEqual(record.fields["分析状态"], "已完成")
        self.assertEqual(
            detail_store.calls[-1]["scored_result"].matched_market_direction_id,
            "VN__hair_accessory__头盔友好整理型__抓夹",
        )
        self.assertEqual(
            detail_store.calls[-1]["scored_result"].matched_market_direction_name,
            "头盔友好整理型抓夹",
        )

    def test_completed_scope_reprocesses_completed_rows_for_v2_backfill(self):
        record = TableRecord(
            record_id="rec_1",
            fields={
                "分析状态": "已完成分析",
                "分析批次": "batch_1",
                "产品标题": "薄款针织开衫",
                "产品图片": ["https://cdn.example.com/1.jpg"],
                "人工类目": "轻上装",
                "产品潜力": "中",
                "内容潜力": "中",
                "建议动作": "低成本试款",
            },
        )
        client = FakeClient(records=[record])
        analyzer = FakeAnalyzer(
            feature_result=FeatureAnalysisResult(
                analysis_category="轻上装",
                feature_scores={
                    "upper_body_change_strength": "高",
                    "camera_readability": "高",
                    "design_signal_strength": "中",
                    "basic_style_escape_strength": "中",
                    "title_selling_clarity": "高",
                    "info_completeness": "高",
                },
                risk_tag="无明显主要风险",
                risk_note="结构点较完整",
                brief_observation="镜头表现和标题支点都较稳",
            )
        )
        pipeline = CandidateAnalysisPipeline(
            table_adapter=TableAdapter(),
            rule_checker=RuleChecker(),
            title_parser=FakeTitleParser(),
            analyzer=analyzer,
            scoring_engine=FakeScoringEngine(),
            result_writer=ResultWriter(),
        )

        summary = pipeline.process_table(build_table_config(), client, record_scope="completed")

        self.assertEqual(summary["processed"], 1)
        self.assertEqual(summary["completed"], 1)
        self.assertEqual(record.fields["分析状态"], "已完成")
        self.assertEqual(record.fields["标题关键词"], "")
        self.assertEqual(record.fields["特征打点JSON"], "")

    def test_progressive_flush_writes_before_final_calibration(self):
        records = [
            TableRecord(
                record_id="rec_1",
                fields={
                    "分析状态": "待处理",
                    "分析批次": "batch_1",
                    "产品标题": "薄款针织开衫",
                    "产品图片": ["https://cdn.example.com/1.jpg"],
                    "人工类目": "轻上装",
                },
            ),
            TableRecord(
                record_id="rec_2",
                fields={
                    "分析状态": "待处理",
                    "分析批次": "batch_1",
                    "产品标题": "防晒罩衫",
                    "产品图片": ["https://cdn.example.com/2.jpg"],
                    "人工类目": "轻上装",
                },
            ),
        ]
        client = FakeClient(records=records)
        analyzer = FakeAnalyzer(
            feature_result=FeatureAnalysisResult(
                analysis_category="轻上装",
                feature_scores={
                    "upper_body_change_strength": "高",
                    "camera_readability": "高",
                    "design_signal_strength": "中",
                    "basic_style_escape_strength": "中",
                    "title_selling_clarity": "高",
                    "info_completeness": "中",
                },
                risk_tag="无明显主要风险",
                risk_note="结构点较完整",
                brief_observation="镜头表现和标题支点都较稳",
            )
        )
        pipeline = CandidateAnalysisPipeline(
            table_adapter=TableAdapter(),
            rule_checker=RuleChecker(),
            title_parser=FakeTitleParser(),
            analyzer=analyzer,
            scoring_engine=FakeScoringEngine(),
            result_writer=ResultWriter(),
        )

        summary = pipeline.process_table(build_table_config(), client, flush_every=1)

        self.assertEqual(summary["completed"], 2)
        self.assertGreaterEqual(len(client.update_calls), 4)
        self.assertEqual(client.update_calls[0][0], "rec_1")
        self.assertEqual(client.update_calls[0][1]["分析状态"], "分析中")
        self.assertEqual(client.update_calls[0][1]["建议动作"], "低成本试款")
        self.assertEqual(client.update_calls[-2][0], "rec_1")
        self.assertEqual(client.update_calls[-2][1]["分析状态"], "已完成")
        self.assertEqual(client.update_calls[-2][1]["建议动作"], "优先测试")


if __name__ == "__main__":
    unittest.main()
