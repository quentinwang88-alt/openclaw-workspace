#!/usr/bin/env python3
"""Structured market-insight report generation and LLM-assisted copy rendering."""

from __future__ import annotations

import json
import re
import subprocess
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from json import JSONDecoder
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml

from src.direction_blocked_check import classify_avoid_signal_confidence, resolve_summary_action
from src.hermes_analyzer import DEFAULT_HERMES_BIN
from src.market_insight_display_names import display_enum, display_list
from src.market_insight_decision_layer import DECISION_ACTION_LABELS, DirectionDecisionLayer
from src.market_insight_models import MarketDirectionCard, VOCLightSummary


SUMMARY_BUCKET_ENTER = "enter"
SUMMARY_BUCKET_WATCH = "watch"
SUMMARY_BUCKET_AVOID = "avoid"

DEFAULT_FAMILY_ORDER_BY_CATEGORY = {
    "hair_accessory": ["审美风格型", "日常场景型", "功能结果型", "形态专用型", "other"],
    "light_tops": ["穿着诉求型", "穿着结果型", "风格气质型", "other"],
}
TIER_ORDER = ["priority", "balanced", "crowded", "low_sample"]
TIER_LABELS = {
    "priority": "priority",
    "balanced": "balanced",
    "crowded": "crowded",
    "low_sample": "low_sample",
}
TIER_DISPLAY_LABELS = {
    "priority": "priority",
    "balanced": "balanced",
    "crowded": "crowded",
    "low_sample": "low_sample",
}

DEFAULT_REPORT_CONFIG = {
    "thresholds": {
        "sales_median_baseline": 250.0,
        "crowded_supply_overhang_sales_median": 300.0,
        "video_density_high": 1.0,
        "video_density_low": 0.3,
        "video_density_enter": 0.5,
        "video_density_crowded_enter": 0.2,
        "video_density_watch_reentry": 0.8,
        "top_item_count_threshold": 100,
    },
    "test_count_by_tier": {
        "priority": "8-12 款",
        "balanced": "4-6 款",
        "low_sample": "2-3 款(观察用)",
        "crowded": "不建议测款",
    },
    "scoring_weight_advice": {
        "审美风格型": {
            "focus_metric": "视觉记忆点",
            "reason": "审美风格型更依赖第一眼识别和差异化外观。",
        },
        "日常场景型": {
            "focus_metric": "场景适配度",
            "reason": "日常场景型更需要稳定覆盖上学、上班、出门等高频场景。",
        },
        "功能结果型": {
            "focus_metric": "演示容易度",
            "reason": "功能结果型更需要通过内容快速证明整理效率或结果变化。",
        },
        "形态专用型": {
            "focus_metric": "佩戴门槛",
            "reason": "形态专用型更受佩戴方式和适配人群限制。",
        },
        "other": {
            "focus_metric": "信息完整度",
            "reason": "other 方向先补齐信息，再决定评分侧重。",
        },
    },
    "scoring_weight_advice_by_category": {
        "hair_accessory": {
            "审美风格型": {
                "focus_metric": "视觉记忆点",
                "reason": "审美风格型更依赖第一眼识别和差异化外观。",
            },
            "日常场景型": {
                "focus_metric": "场景适配度",
                "reason": "日常场景型更需要稳定覆盖上学、上班、出门等高频场景。",
            },
            "功能结果型": {
                "focus_metric": "演示容易度",
                "reason": "功能结果型更需要通过内容快速证明整理效率或结果变化。",
            },
            "形态专用型": {
                "focus_metric": "佩戴门槛",
                "reason": "形态专用型更受佩戴方式和适配人群限制。",
            },
            "other": {
                "focus_metric": "信息完整度",
                "reason": "other 方向先补齐信息，再决定评分侧重。",
            },
        },
        "light_tops": {
            "穿着诉求型": {
                "focus_metric": "场景诉求清晰度",
                "reason": "穿着诉求型更依赖场景需求是否明确，例如空调房、防晒和轻薄实用价值是否能被快速读懂。",
            },
            "穿着结果型": {
                "focus_metric": "上身结果可感知度",
                "reason": "穿着结果型更需要把显比例、遮手臂、叠穿层次等上身结果讲清楚。",
            },
            "风格气质型": {
                "focus_metric": "风格识别度",
                "reason": "风格气质型更依赖第一眼风格感知和整体气质表达的一致性。",
            },
            "other": {
                "focus_metric": "信息完整度",
                "reason": "other 方向先补齐信息，再决定评分侧重。",
            },
        },
    },
    "report": {
        "max_summary_items": 3,
        "llm_max_workers": 4,
    },
}


class MarketInsightReportGenerator(object):
    def __init__(
        self,
        skill_dir: Path,
        config_path: Optional[Path] = None,
        hermes_bin: Optional[str] = None,
        timeout_seconds: int = 180,
        command_runner=None,
    ):
        self.skill_dir = Path(skill_dir)
        self.config = self._load_config(config_path or (self.skill_dir / "configs" / "report_config.yaml"))
        self.decision_layer = DirectionDecisionLayer(self.skill_dir / "configs" / "market_insight_decision_rules.yaml")
        self.prompt_path = self.skill_dir / "prompts" / "market_insight_direction_report_prompt_v1.txt"
        self.matrix_prompt_path = self.skill_dir / "prompts" / "market_insight_matrix_observation_prompt_v1.txt"
        self.renderer = MarketInsightDirectionCopyRenderer(
            prompt_path=self.prompt_path,
            hermes_bin=Path(hermes_bin or DEFAULT_HERMES_BIN).expanduser(),
            timeout_seconds=timeout_seconds,
            command_runner=command_runner or subprocess.run,
        )
        self.matrix_renderer = MarketInsightMatrixObservationRenderer(
            prompt_path=self.matrix_prompt_path,
            hermes_bin=Path(hermes_bin or DEFAULT_HERMES_BIN).expanduser(),
            timeout_seconds=timeout_seconds,
            command_runner=command_runner or subprocess.run,
        )

    def generate_report(
        self,
        cards: Iterable[MarketDirectionCard],
        voc_summary: Optional[VOCLightSummary],
        country: str,
        category: str,
        batch_date: str,
        use_llm: bool = True,
        total_product_count: Optional[int] = None,
        completed_product_count: Optional[int] = None,
        source_scope: str = "",
        quality_gate: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], str, Dict[str, Any]]:
        card_payloads = [self._card_payload(card) for card in cards]
        family_order = self._resolve_family_order(category=category, cards=card_payloads)
        family_groups = self._group_by_family(card_payloads)
        enriched_cards = [self._enrich_card(card, family_groups) for card in card_payloads]
        enriched_cards, decision_layer_summary = self.decision_layer.apply(
            enriched_cards,
            country=country,
            category=category,
            batch_id=batch_date,
        )
        self._apply_batch_comparison(enriched_cards, country=country, category=category, batch_id=batch_date)
        category_baselines = self._build_category_baselines(enriched_cards)
        self._apply_competition_structure_v14(enriched_cards, category_baselines)
        decision_summary = self._build_decision_summary(enriched_cards)
        enter_cards = decision_summary["enter"]["items"]
        watch_cards = [card for card in enriched_cards if str(card.get("decision_action") or "") == "observe"]
        llm_texts, llm_meta = self._render_llm_copy(enter_cards, use_llm=use_llm)
        opportunity_cards = [self._build_opportunity_card(card, llm_texts.get(card["direction_canonical_key"], {})) for card in enter_cards]
        matrix = self._build_direction_matrix(enriched_cards, decision_summary, family_order=family_order, use_llm=use_llm)
        reverse_signals = self._build_reverse_signals(enriched_cards)
        cross_system = self._build_cross_system_recommendations(enriched_cards, category=category)
        watch_direction_table = self._build_watch_direction_table(watch_cards)
        market_regime_assessment = self._build_market_regime_assessment(enriched_cards)
        report_confidence = self._build_report_confidence(
            cards=enriched_cards,
            total_product_count=total_product_count,
            completed_product_count=completed_product_count,
            source_scope=source_scope,
            quality_gate=quality_gate,
        )
        direction_decision_trace = self._build_direction_decision_trace(enriched_cards)
        opportunity_diagnostics = self._build_opportunity_diagnostics(enriched_cards)
        sample_pool_plan = self._build_sample_pool_plan(enriched_cards)
        direction_decision_cards = self._build_direction_decision_cards(enriched_cards)
        v31_action_report = self._build_v31_action_report(
            direction_decision_cards=direction_decision_cards,
            report_confidence=report_confidence,
            opportunity_diagnostics=opportunity_diagnostics,
            country=country,
            category=category,
            batch_date=batch_date,
        )
        consistency = self._evaluate_consistency(enriched_cards, decision_summary, reverse_signals, cross_system)
        consistency_warnings = list(consistency.get("warnings") or [])
        consistency_errors = list(consistency.get("errors") or [])
        llm_meta.update(
            {
                "matrix_observations_used_llm": bool(matrix.get("observation_meta", {}).get("used_llm")),
                "matrix_observations_fallback": bool(matrix.get("observation_meta", {}).get("fallback")),
                "consistency_warning_count": len(consistency_warnings),
                "consistency_error_count": len(consistency_errors),
                "report_publish_blocked": bool(consistency_errors),
            }
        )
        payload = {
            "report_version": "v3.1_action_closed_loop",
            "country": country,
            "category": category,
            "batch_date": batch_date,
            "source_scope": source_scope,
            "report_confidence": report_confidence.get("report_confidence"),
            "confidence_reasons": report_confidence.get("confidence_reasons", []),
            "report_diagnostics": report_confidence,
            "category_baselines": category_baselines,
            "family_order": family_order,
            "market_regime_assessment": market_regime_assessment,
            "business_summary": v31_action_report.get("business_summary", {}),
            "decision_layer_summary": decision_layer_summary,
            "direction_decision_cards": direction_decision_cards,
            "direction_actions": v31_action_report.get("direction_actions", []),
            "direction_execution_briefs": v31_action_report.get("direction_execution_briefs", []),
            "content_baseline_tests": v31_action_report.get("content_baseline_tests", []),
            "data_supplement_actions": v31_action_report.get("data_supplement_actions", []),
            "v31_action_report": v31_action_report,
            "direction_decision_trace": direction_decision_trace,
            "opportunity_diagnostics": opportunity_diagnostics,
            "sample_pool_plan": sample_pool_plan,
            "decision_summary": decision_summary,
            "watch_direction_table": watch_direction_table,
            "opportunity_direction_cards": opportunity_cards,
            "direction_matrix": matrix,
            "reverse_signals": reverse_signals,
            "cross_system_recommendations": cross_system,
            "consistency_warnings": consistency_warnings,
            "consistency_errors": consistency_errors,
            "voc_summary": (voc_summary or VOCLightSummary(voc_status="skipped")).to_dict(),
            "llm_meta": llm_meta,
        }
        markdown = self.render_markdown(payload)
        payload["full_report_markdown"] = self.full_report_renderer(payload)
        if use_llm:
            self._save_decision_history(enriched_cards, country=country, category=category, batch_id=batch_date)
        return payload, markdown, llm_meta

    def render_markdown(self, payload: Dict[str, Any]) -> str:
        return self.business_summary_renderer(payload)

    def business_summary_renderer(self, payload: Dict[str, Any]) -> str:
        if str(payload.get("report_version") or "") == "v3.1_action_closed_loop":
            return self._render_v31_business_report(payload)

        country = str(payload.get("country") or "")
        category = str(payload.get("category") or "")
        batch_date = str(payload.get("batch_date") or "")
        regime = dict(payload.get("market_regime_assessment") or {})
        decision_cards = list(payload.get("direction_decision_cards") or [])
        lines = [
            "# 市场洞察报告｜{country}｜{category}｜{batch_date}".format(country=country, category=category, batch_date=batch_date),
            "",
            "## 0. 批次信息与报告可信度",
        ]
        lines.extend(self._business_batch_info(payload))
        lines.extend([
            "",
            "## 1. 业务结论摘要",
            "当前判断：{value}".format(value=regime.get("regime_label", "结构性观察期")),
            "",
            "一句话结论：{value}".format(value=self._business_one_line_conclusion(payload)),
        ])
        lines.extend(self._business_action_summary_rows(decision_cards))
        lines.extend([
            "",
            "## 2. 竞争中找机会",
            "",
            "机会类型证据：以下每类机会使用独立证据，避免把同一组指标反复包装成不同结论。",
        ])
        for row in self._business_opportunity_rows(decision_cards, payload.get("opportunity_diagnostics") or []):
            lines.extend([
                "",
                "### {opportunity_type}".format(**row),
                "- 相关方向：{directions}".format(**row),
                "- 关键证据：{evidence}".format(**row),
                "- 下一步：{next_step}".format(**row),
            ])
        lines.extend([
            "",
            "## 3. 方向动作总表",
        ])
        for row in self._business_direction_rows(decision_cards, payload.get("direction_decision_trace") or []):
            lines.extend([
                "",
                "### {direction_name}".format(**row),
                "- 方向级动作：{direction_action}".format(**row),
                "- 样本级动作：{sample_action}".format(**row),
                "- 竞争结构：{competition_structure}".format(**row),
                "- 新品信号：{new_signal}".format(**row),
                "- 机会类型：{opportunity_type}".format(**row),
                "- 优先级：{business_priority}".format(**row),
                "- 必须核验问题：{verification_question}".format(**row),
                "- 下一步：{next_step}".format(**row),
                "- 是否进入样本池：{sample_pool_required}".format(**row),
            ])
        watch_rows = list(payload.get("watch_direction_table") or [])
        if watch_rows:
            lines.extend([
                "",
                "### 建议观察方向简表",
            ])
            for row in watch_rows:
                lines.extend([
                    "",
                    "#### {direction_name}".format(**row),
                    "- 当前动作：{current_action}".format(**row),
                    "- 观察原因：{observe_reason}".format(**row),
                    "- 当前关键信号：{current_signal}".format(**row),
                    "- 下一批转入条件：{action_condition}".format(**row),
                    "- 不可转入条件：{block_condition}".format(**row),
                ])
        lines.extend(["", "## 4. 重点方向深拆卡"])
        focus_cards = self._business_focus_cards(decision_cards)
        if focus_cards:
            for item in focus_cards:
                lines.extend([
                    "",
                    "### {direction_name}".format(**item),
                    "当前动作：{current_action}".format(**item),
                    "{action_override_text}".format(**item),
                    "为什么看：{why_watch}".format(**item),
                    "为什么不直接做：{why_not_direct}".format(**item),
                    "机会类型：{opportunity_types}".format(**item),
                    "核心风险：{core_risk}".format(**item),
                    "必须核验问题：{verification_questions}".format(**item),
                    "样本池生成要求：{sample_pool_requirement}".format(**item),
                    "通过条件：{pass_conditions}".format(**item),
                    "淘汰条件：{fail_conditions}".format(**item),
                    "下一步动作：{next_step}".format(**item),
                ])
        else:
            lines.extend(["", "本批暂无需要展开的重点方向。"])
        lines.extend([
            "",
            "## 5. 样本池生成规则",
        ])
        lines.extend(self._render_sample_pool_plan_summary(payload.get("sample_pool_plan") or []))
        lines.extend([
            "",
            "## 6. 本批次不做什么",
            "1. 不建议当前市场 / 类目批量上新。",
            "2. 不建议直接铺普通审美款或同质化款。",
            "3. 不建议基于 other 方向做选品判断。",
            "4. 不建议因为某方向 crowded 就直接放弃，要看新品、内容和头部结构。",
            "5. 内容缺口不等于直接进入；若老品占位、新品窗口弱或头部不可复制，应先拆样本。",
            "6. 不建议在未看样本商品池前启动内容批量生产。",
            "",
            "## 7. 下一步执行清单",
        ])
        for row in self._business_execution_rows(decision_cards, category=category):
            lines.extend([
                "",
                "### {priority}｜{action}".format(**row),
                "- 方向：{directions}".format(**row),
                "- 产出：{output}".format(**row),
            ])
        consistency_warnings = list(payload.get("consistency_warnings") or [])
        consistency_errors = list(payload.get("consistency_errors") or [])
        if consistency_errors:
            lines.extend(["", "附：阻塞性自洽性问题"])
            for warning in consistency_errors:
                lines.append("- {warning}".format(warning=warning))
        if consistency_warnings:
            lines.extend(["", "附：数据自洽性提示"])
            for warning in consistency_warnings:
                lines.append("- {warning}".format(warning=warning))
        matrix = payload.get("direction_matrix") or {}
        display_lines = list(matrix.get("display_lines") or [])
        if display_lines:
            lines.extend(["", "## 附录：方向对比矩阵"])
            lines.extend(display_lines)
        lines.extend(
            [
                "",
                "## 附录：新旧结论差异对账",
                "说明：旧版三桶结果仅用于算法对账，不作为最终动作建议。最终动作以 V2 方向级/样本级动作分层为准。",
            ]
        )
        lines.extend(self._render_legacy_diff_rows(decision_cards))
        lines.extend([
            "",
            "## 附录索引",
            "- 附录 A：方向数据明细（见 full_report_markdown / 结构化 JSON）",
            "- 附录 B：机会类型证据（见 full_report_markdown / 结构化 JSON）",
            "- 附录 C：新旧结论对账（见 full_report_markdown / 结构化 JSON）",
            "- 附录 D：结构化 JSON / 算法诊断（见报告产物 JSON）",
            "",
        ])
        return "\n".join(lines).strip() + "\n"

    def _build_v31_action_report(
        self,
        direction_decision_cards: List[Dict[str, Any]],
        report_confidence: Dict[str, Any],
        opportunity_diagnostics: List[Dict[str, Any]],
        country: str,
        category: str,
        batch_date: str,
    ) -> Dict[str, Any]:
        direction_actions = [self._build_v31_direction_action(card, category=category) for card in direction_decision_cards]
        self._ensure_at_least_one_business_action(direction_actions)
        content_baseline_tests = self._build_v31_content_baseline_tests(direction_actions, direction_decision_cards)
        data_supplement_actions = self._build_v31_data_supplement_actions(content_baseline_tests)
        blocking_issues = self._v31_blocking_issues(direction_actions, opportunity_diagnostics)
        self_consistency_status = "需修复" if blocking_issues else "通过"
        disable_business_summary = bool(blocking_issues)
        business_summary = self._build_v31_business_summary(
            direction_actions=direction_actions,
            report_confidence=report_confidence,
            content_baseline_tests=content_baseline_tests,
            disable_business_summary=disable_business_summary,
        )
        return {
            "report_version": "v3.1_action_closed_loop",
            "country": country,
            "category": category,
            "batch_date": batch_date,
            "business_summary": business_summary,
            "direction_actions": direction_actions,
            "direction_execution_briefs": [item.get("direction_execution_brief") for item in direction_actions if item.get("direction_execution_brief")],
            "content_baseline_tests": content_baseline_tests,
            "data_supplement_actions": data_supplement_actions,
            "diagnostics": {
                "opportunity_type_version": "v3.1",
                "self_consistency_status": self_consistency_status,
                "blocking_issues": blocking_issues,
                "disable_business_summary": disable_business_summary,
                "migration_trace": ["v2_competition_opportunity -> v3.1_action_closed_loop"],
            },
        }

    def _build_v31_direction_action(self, card: Dict[str, Any], category: str = "") -> Dict[str, Any]:
        name = str(card.get("direction_name") or "")
        primary_action, secondary_actions, trace = self._v31_choose_primary_action(card, category=category)
        sample_pool_plan = self._v31_sample_pool_plan_for_action(card, primary_action)
        verification_questions = self._v31_verification_questions(card, primary_action)
        direction_execution_brief = self._v31_direction_execution_brief(card, primary_action, secondary_actions, sample_pool_plan)
        return {
            "direction": name,
            "primary_action": primary_action,
            "primary_action_label": self._v31_action_label(primary_action),
            "secondary_actions": secondary_actions,
            "secondary_action_labels": [self._v31_action_label(action) for action in secondary_actions],
            "p0_vs_p1_decision_trace": trace,
            "primary_opportunity_type": trace.get("primary_opportunity_type"),
            "secondary_opportunity_subtype": trace.get("secondary_opportunity_subtype"),
            "opportunity_evidence": trace.get("opportunity_evidence"),
            "verification_questions": verification_questions,
            "sample_pool_plan": sample_pool_plan,
            "direction_execution_brief": direction_execution_brief,
            "execution_sequence": self._v31_execution_sequence(primary_action, name, secondary_actions),
            "acceptance_criteria": self._v31_acceptance_criteria(primary_action),
            "stop_loss_criteria": self._v31_stop_loss_criteria(primary_action),
            "expected_outputs": self._v31_expected_outputs(primary_action),
            "why": trace.get("why"),
            "this_batch_method": self._v31_this_batch_method(card, primary_action),
            "do_not_do": self._v31_do_not_do(card, primary_action),
            "sample_pool_required": primary_action not in {"P4_no_action"} or "P0_content_baseline_test" in secondary_actions,
            "business_priority": self._v31_priority(primary_action),
            "source_decision_action": str(card.get("decision_action") or ""),
            "source_competition_type": str((card.get("competition_structure") or {}).get("competition_type") or ""),
            "card": card,
        }

    def _v31_choose_primary_action(self, card: Dict[str, Any], category: str = "") -> Tuple[str, List[str], Dict[str, Any]]:
        name = str(card.get("direction_name") or "")
        if name == "other":
            return "P3_classification_review", [], self._v31_trace(
                card=card,
                selected_action="P3_classification_review",
                why_not_other_action="other 不参与业务机会判断，只进入分类复核。",
                primary_type="classification_unclear",
                subtype="classification_review",
                hit_metrics=[{"metric": "direction_name", "value": "other", "threshold": "非业务方向"}],
            )

        demand = dict(card.get("demand_structure") or {})
        age = dict(card.get("product_age_structure") or {})
        capability = dict(card.get("our_capability_fit") or {})
        structure = dict(card.get("competition_structure") or {})
        sample_confidence = self._confidence_rank(str(card.get("sample_confidence") or ""))
        sales_ratio_90d = self._safe_float(age.get("new_90d_sales_share")) or 0.0
        top3_share = self._safe_float(demand.get("top3_sales_share")) or 0.0
        old180_share = self._safe_float(age.get("old_180d_sales_share")) or 0.0
        supply_fit = self._capability_rank(str(capability.get("sourcing_fit") or "medium"))
        replicability = self._capability_rank(str(capability.get("replication") or "medium"))
        decision_action = str(card.get("decision_action") or "")
        competition_type = str(structure.get("competition_type") or "")
        secondary_actions: List[str] = []

        category = str(card.get("category") or card.get("category_id") or category or "")
        source_action = self._v31_action_from_source_decision(card, category=category)
        if source_action is not None:
            return source_action

        p0_hit = (
            sales_ratio_90d >= 0.30
            and top3_share <= 0.40
            and sample_confidence >= 2
            and replicability >= 1
            and supply_fit >= 1
        )
        # 耳环“少女礼物感型”是 V3.1 明确保留的低成本验证窗口：
        # 新品销量占比不必等到 30% 才行动，只要头部不垄断、样本置信度够，
        # 供应链/可复制性改为样本池内快速核验，而不是继续把动作推迟。
        if (
            category == "earrings"
            and name == "少女礼物感型"
            and sales_ratio_90d >= 0.20
            and top3_share <= 0.40
            and sample_confidence >= 2
        ):
            p0_hit = True
        if p0_hit:
            secondary_actions.append("P0_content_baseline_test")
            return "P0_low_cost_test", secondary_actions, self._v31_trace(
                card=card,
                selected_action="P0_low_cost_test",
                why_not_other_action="新品销量占比达到 P0 阈值，Top3 未高度垄断，样本置信度和可复制性足以支持低成本验证，因此不是先拆解或继续观察。",
                primary_type="mature_strong_demand",
                subtype="small_brand_entry_window",
                hit_metrics=[
                    {"metric": "近90天新品销量占比", "value": sales_ratio_90d, "threshold": ">= 0.30"},
                    {"metric": "Top3 销量占比", "value": top3_share, "threshold": "<= 0.40"},
                    {"metric": "样本置信度", "value": card.get("sample_confidence"), "threshold": ">= medium"},
                ],
            )

        p1_new_hit = sales_ratio_90d >= 0.20 and (
            top3_share > 0.40
            or sample_confidence < 2
            or replicability < 1
            or supply_fit < 1
            or decision_action in {"strong_signal_verify", "hidden_candidate"}
        )
        if p1_new_hit:
            if decision_action == "hidden_candidate":
                return "P2_observe_reserve", ["P3_classification_review"], self._v31_trace(
                    card=card,
                    selected_action="P2_observe_reserve",
                    why_not_other_action="有场景线索但样本过少，不能直接进入测款；先作为储备和样本复核。",
                    primary_type="hidden_scene",
                    subtype="content_data_missing",
                    hit_metrics=[
                        {"metric": "样本数", "value": card.get("direction_item_count"), "threshold": "暗线需人工确认"},
                        {"metric": "近90天新品销量占比", "value": sales_ratio_90d, "threshold": ">= 0.20"},
                    ],
                )
            return "P1_new_winner_dissection", [], self._v31_trace(
                card=card,
                selected_action="P1_new_winner_dissection",
                why_not_other_action="新品有信号，但头部、样本、可复制性或供应链仍不够稳，先拆新品赢家再决定是否测款。",
                primary_type="mature_strong_demand" if competition_type == "few_new_winners" else str(card.get("primary_opportunity_type") or "general_observe"),
                subtype="mature_with_new_winners",
                hit_metrics=[
                    {"metric": "近90天新品销量占比", "value": sales_ratio_90d, "threshold": ">= 0.20"},
                    {"metric": "Top3 销量占比", "value": top3_share, "threshold": "P1 允许 > 0.40"},
                    {"metric": "供应链匹配", "value": capability.get("sourcing_fit"), "threshold": "unclear/low 时先拆解"},
                ],
            )

        if old180_share >= 0.60 or competition_type == "old_product_dominated":
            return "P2_old_listing_replacement_check", [], self._v31_trace(
                card=card,
                selected_action="P2_old_listing_replacement_check",
                why_not_other_action="老品占位强，方向需求不等于新品能打进去，先核验老品替代结构。",
                primary_type="old_listing_dominated",
                subtype="mature_red_ocean",
                hit_metrics=[
                    {"metric": "180天以上老品销量占比", "value": old180_share, "threshold": ">= 0.60"},
                    {"metric": "近90天新品销量占比", "value": sales_ratio_90d, "threshold": "越低越说明新品窗口弱"},
                ],
            )

        if decision_action == "study_top_not_enter" or competition_type in {"aesthetic_homogeneous", "head_concentrated"}:
            return "P1_head_sample_dissection", [], self._v31_trace(
                card=card,
                selected_action="P1_head_sample_dissection",
                why_not_other_action="方向可能有需求，但普通款同质化或头部结构不明，先拆 Top 样本，不做方向铺货。",
                primary_type="head_concentrated" if competition_type == "head_concentrated" else "aesthetic_homogeneous",
                subtype="head_winner_market",
                hit_metrics=[
                    {"metric": "竞争结构", "value": competition_type, "threshold": "头部/审美同质方向先拆解"},
                    {"metric": "Top3 销量占比", "value": top3_share, "threshold": "辅助判断头部结构"},
                ],
            )

        if decision_action in {"prioritize_low_cost_test", "cautious_test", "hidden_small_test"}:
            secondary_actions.append("P0_content_baseline_test")
            return "P0_low_cost_test", secondary_actions, self._v31_trace(
                card=card,
                selected_action="P0_low_cost_test",
                why_not_other_action="原方向层已进入验证动作，V3.1 将其收敛为低成本测款并绑定内容基线。",
                primary_type=str(card.get("primary_opportunity_type") or "content_gap"),
                subtype="content_data_missing" if competition_type == "content_gap" else "mature_with_new_winners",
                hit_metrics=[
                    {"metric": "原始建议动作", "value": decision_action, "threshold": "验证动作"},
                    {"metric": "7日销量中位数", "value": card.get("direction_sales_median_7d"), "threshold": "需有成交承接"},
                ],
            )

        return "P2_observe_reserve", [], self._v31_trace(
            card=card,
            selected_action="P2_observe_reserve",
            why_not_other_action="当前新品、内容、价格或供应链证据不足，不占测款资源，只做储备观察。",
            primary_type=str(card.get("primary_opportunity_type") or "low_sample_wait"),
            subtype="content_data_missing" if competition_type == "content_gap" else "mature_red_ocean",
            hit_metrics=[
                {"metric": "近90天新品销量占比", "value": sales_ratio_90d, "threshold": "P0 需 >= 0.30；P1 需 >= 0.20"},
                {"metric": "样本置信度", "value": card.get("sample_confidence"), "threshold": "测款需 >= medium"},
            ],
        )

    def _v31_action_from_source_decision(
        self,
        card: Dict[str, Any],
        category: str = "",
    ) -> Optional[Tuple[str, List[str], Dict[str, Any]]]:
        """For established categories, preserve the direction layer's final action.

        The V3.1 P0/P1 new-winner rules were introduced for the earrings report.
        Re-applying them to older hair/light-top runs can overturn already vetted
        direction decisions, so non-earrings reports use the structured
        `actual_action` / `decision_action` as the source of truth.
        """
        category = str(card.get("category") or card.get("category_id") or category or "")
        if category == "earrings":
            return None
        decision_action = str(card.get("actual_action") or card.get("decision_action") or "")
        if not decision_action:
            return None
        if (
            decision_action == "study_top_not_enter"
            and str(card.get("actionable_new_product_signal") or (card.get("new_product_entry_signal") or {}).get("type") or "") == "old_product_dominated"
        ):
            return "P2_old_listing_replacement_check", [], self._v31_trace(
                card=card,
                selected_action="P2_old_listing_replacement_check",
                why_not_other_action="原方向层因老品占位将动作覆盖为拆头部不直接入场，短版报告落到老品替代核验。",
                primary_type=str(card.get("primary_opportunity_type") or "old_listing_dominated"),
                subtype="old_listing_dominated",
                hit_metrics=[
                    {"metric": "原方向层最终动作", "value": decision_action, "threshold": "source_of_truth"},
                    {"metric": "可行动新品信号", "value": "old_product_dominated", "threshold": "老品占位"},
                ],
            )

        mapping = {
            "prioritize_low_cost_test": ("P0_low_cost_test", ["P0_content_baseline_test"], "原方向层已判定为优先低成本验证，短版报告只做动作分层，不重新判定方向。"),
            "cautious_test": ("P0_cautious_test", ["P0_content_baseline_test"], "原方向层已判定为谨慎切入验证，保留为少量款验证而非方向级铺货。"),
            "hidden_small_test": ("P0_hidden_small_test", [], "原方向层已判定为暗线小样本验证，保留为 1–2 款小样本动作。"),
            "study_top_not_enter": ("P1_head_sample_dissection", [], "原方向层已判定为拆头部不直接入场，短版报告不升级为测款。"),
            "strong_signal_verify": ("P1_strong_signal_verify", [], "原方向层已判定为强信号待核验，先进入样本池核查，不直接测款。"),
            "hidden_candidate": ("P1_hidden_candidate", [], "原方向层已判定为暗线候选，先进入样本池和下批追踪。"),
            "observe": ("P2_observe_reserve", [], "原方向层已判定为持续观察，本批不占用测款资源。"),
            "avoid": ("P4_no_action", [], "原方向层已判定为暂不投入，本批不进入样本池。"),
        }
        mapped = mapping.get(decision_action)
        if not mapped:
            return None
        action, secondary_actions, reason = mapped
        return action, secondary_actions, self._v31_trace(
            card=card,
            selected_action=action,
            why_not_other_action=reason,
            primary_type=str(card.get("primary_opportunity_type") or "general_observe"),
            subtype=str((card.get("competition_structure") or {}).get("competition_type") or "source_action_preserved"),
            hit_metrics=[
                {"metric": "原方向层最终动作", "value": decision_action, "threshold": "source_of_truth"},
                {"metric": "7日销量中位数", "value": card.get("direction_sales_median_7d"), "threshold": "保留原方向层证据"},
            ],
        )

    def _v31_trace(
        self,
        card: Dict[str, Any],
        selected_action: str,
        why_not_other_action: str,
        primary_type: str,
        subtype: str,
        hit_metrics: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        return {
            "selected_action": selected_action,
            "why_not_other_action": why_not_other_action,
            "hit_metrics": hit_metrics,
            "thresholds": [str(item.get("threshold") or "") for item in hit_metrics],
            "primary_opportunity_type": primary_type,
            "secondary_opportunity_subtype": subtype,
            "why": self._v31_why_text(card, selected_action),
            "opportunity_evidence": {
                "trigger_rule": self._v31_action_label(selected_action),
                "hit_metrics": hit_metrics,
                "thresholds": [str(item.get("threshold") or "") for item in hit_metrics],
                "exclusion_checks": [why_not_other_action],
                "business_action": self._v31_action_label(selected_action),
            },
        }

    def _build_v31_business_summary(
        self,
        direction_actions: List[Dict[str, Any]],
        report_confidence: Dict[str, Any],
        content_baseline_tests: List[Dict[str, Any]],
        disable_business_summary: bool,
    ) -> Dict[str, Any]:
        if disable_business_summary:
            return {
                "market_state": "报告需修复",
                "one_sentence_conclusion": "本报告存在规则冲突，暂不输出正式业务结论。",
                "primary_batch_actions": [],
                "secondary_batch_actions": [],
                "do_not_do": ["修复阻塞性自洽性问题前，不使用本报告做经营决策"],
            }
        primary_actions = [item for item in direction_actions if item["primary_action"] in {"P0_low_cost_test", "P1_new_winner_dissection", "P1_head_sample_dissection", "P2_old_listing_replacement_check", "P3_classification_review"}]
        p0 = [item for item in direction_actions if item["primary_action"] == "P0_low_cost_test"]
        p1 = [item for item in direction_actions if item["primary_action"].startswith("P1_")]
        primary_batch_actions = []
        for item in p0[:2]:
            primary_batch_actions.append({
                "action": self._v31_action_label(item["primary_action"]),
                "direction": item["direction"],
                "method": item["this_batch_method"],
            })
        if not primary_batch_actions:
            for item in primary_actions[:2]:
                primary_batch_actions.append({
                    "action": self._v31_action_label(item["primary_action"]),
                    "direction": item["direction"],
                    "method": item["this_batch_method"],
                })
        secondary_batch_actions = []
        for test in content_baseline_tests[:1]:
            secondary_batch_actions.append({
                "action": "内容基线测试",
                "direction": "、".join(test.get("directions") or []),
                "method": "每方向 5–10 条内容，14 天建立点击 / 商品点击 / 转化基线",
            })
        market_state = "成熟红海下的行动验证期" if (p0 or p1) else "高风险观察期"
        if p0:
            one_sentence = "本批不做方向级铺货，但 {names} 进入 4–6 款低成本验证，并同步建立内容基线。".format(
                names="、".join(item["direction"] for item in p0[:2])
            )
        elif p1:
            one_sentence = "本批暂无低成本测款方向，但 {names} 必须进入样本拆解，不能只停留在观察。".format(
                names="、".join(item["direction"] for item in p1[:3])
            )
        else:
            one_sentence = "本批无测款动作；经营动作以分类复核、老品替代核验或内容基线为主。"
        return {
            "market_state": market_state,
            "one_sentence_conclusion": one_sentence,
            "primary_batch_actions": primary_batch_actions,
            "secondary_batch_actions": secondary_batch_actions,
            "do_not_do": self._v31_do_not_do_summary(direction_actions),
            "report_confidence": report_confidence.get("report_confidence"),
            "confidence_reasons": list(report_confidence.get("confidence_reasons") or []),
        }

    def _ensure_at_least_one_business_action(self, direction_actions: List[Dict[str, Any]]) -> None:
        actionable = {"P0_low_cost_test", "P0_content_baseline_test", "P1_new_winner_dissection", "P1_head_sample_dissection", "P2_old_listing_replacement_check", "P3_classification_review"}
        if any(item["primary_action"] in actionable for item in direction_actions):
            return
        candidates = [item for item in direction_actions if item["direction"] != "other"]
        if not candidates:
            return
        candidates[0]["primary_action"] = "P1_head_sample_dissection"
        candidates[0]["primary_action_label"] = self._v31_action_label("P1_head_sample_dissection")
        candidates[0]["why"] = "系统未识别出测款动作，但 V3.1 要求至少形成一个经营动作，因此将最高优先方向转入头部样本拆解。"
        candidates[0]["sample_pool_plan"] = self._v31_sample_pool_plan_for_action(candidates[0]["card"], "P1_head_sample_dissection")

    def _build_v31_content_baseline_tests(self, direction_actions: List[Dict[str, Any]], cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        baseline_dirs = [item["direction"] for item in direction_actions if "P0_content_baseline_test" in item.get("secondary_actions", [])]
        if not baseline_dirs:
            candidates = [
                item for item in direction_actions
                if item["primary_action"] in {"P0_low_cost_test", "P1_new_winner_dissection"}
            ]
            baseline_dirs = [item["direction"] for item in candidates[:2]]
        if not baseline_dirs:
            return []
        return [
            {
                "directions": baseline_dirs[:2],
                "content_per_direction": "5-10",
                "MVP_total_content": "10-20",
                "full_baseline_threshold": 30,
                "duration": "14d",
                "metrics": [
                    "ctr",
                    "completion_rate",
                    "video_to_product_ctr",
                    "video_conversion_rate",
                    "content_cost",
                    "reusable_hook_type",
                ],
                "benchmark_source": "first_batch_internal_baseline",
                "acceptance_criteria": [
                    "至少 1 个方向的核心点击指标 >= benchmark * 1.1",
                    "至少 1 条内容达到自家历史 P75",
                    "明确跑出 1–2 个可复用 hook 类型",
                ],
                "failure_criteria": [
                    "所有内容点击和商品点击均低于 benchmark",
                    "没有任何可复用 hook",
                    "评论 / 点击反馈无法指向商品兴趣",
                ],
                "next_action_if_pass": "升级为 P0_low_cost_test，用验证过的内容钩子配合产品测款。",
                "next_action_if_fail": "该类目内容化能力暂不成立，转入 P2_observe_reserve。",
            }
        ]

    def _build_v31_data_supplement_actions(self, content_baseline_tests: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        paired_action = "P0_content_baseline_test" if content_baseline_tests else "P1_head_sample_dissection"
        return [
            {
                "missing_fields": ["top_video_engagement", "video_to_product_ctr", "internal_test_conversion"],
                "paired_business_action": paired_action,
                "cannot_be_only_action": True,
                "reason": "内容/转化数据用于校准报告，不允许作为本批唯一动作。",
            }
        ]

    def _v31_blocking_issues(self, direction_actions: List[Dict[str, Any]], opportunity_diagnostics: List[Dict[str, Any]]) -> List[str]:
        issues: List[str] = []
        if not direction_actions:
            issues.append("没有方向动作结果。")
        if not any(item["primary_action"] != "P4_no_action" for item in direction_actions):
            issues.append("整份报告没有任何经营动作。")
        for item in direction_actions:
            evidence = dict(item.get("opportunity_evidence") or {})
            if item["primary_action"] in {"P0_low_cost_test", "P1_new_winner_dissection", "P1_head_sample_dissection"} and not evidence.get("hit_metrics"):
                issues.append("{direction} 的机会类型缺少 evidence。".format(direction=item["direction"]))
        return issues

    def _render_v31_business_report(self, payload: Dict[str, Any]) -> str:
        action_report = dict(payload.get("v31_action_report") or {})
        business = dict(action_report.get("business_summary") or payload.get("business_summary") or {})
        direction_actions = list(action_report.get("direction_actions") or payload.get("direction_actions") or [])
        diagnostics = dict(payload.get("report_diagnostics") or {})
        v31_diagnostics = dict(action_report.get("diagnostics") or {})
        execution_plan = self._v141_execution_plan(direction_actions)
        confidence = self._v141_report_confidence(diagnostics)
        lines = [
            "# 市场洞察报告｜{country}｜{category}｜{batch_date}".format(
                country=payload.get("country", ""),
                category=payload.get("category", ""),
                batch_date=payload.get("batch_date", ""),
            ),
            "",
            "## 0. 批次结论",
        ]
        if v31_diagnostics.get("disable_business_summary"):
            lines.extend(["", "## 1. 60秒业务结论", "本报告存在规则冲突，暂不输出正式业务结论。", "冲突项："])
            for issue in list(v31_diagnostics.get("blocking_issues") or []):
                lines.append("- {issue}".format(issue=issue))
            return "\n".join(lines).strip() + "\n"

        lines.extend([
            "当前判断：{value}".format(value=business.get("market_state", "行动验证期")),
            "一句话结论：{value}".format(value=self._v141_one_sentence(execution_plan, business)),
            "本批主攻：{value}".format(value=self._v141_main_focus(execution_plan)),
            "本批不做：{value}".format(
                value="；".join(item.rstrip("。；;") for item in self._v31_do_not_do_summary(direction_actions)[:2])
            ),
            "报告置信度：数据可用性 {data}，决策置信度 {decision}。{reason}".format(
                data=confidence["data_usability"],
                decision=confidence["decision_confidence"],
                reason=confidence["reason"],
            ),
            "",
            "## 1. 本批执行清单",
        ])
        lines.extend(self._render_v141_execution_plan(execution_plan))
        lines.extend(["", "## 2. 关键依据"])
        lines.extend(self._render_v141_key_rationales(execution_plan))
        lines.extend(["", "## 3. 任务参数"])
        lines.extend(self._render_v141_task_parameters())
        lines.extend([
            "",
            "## 4. 附录索引",
            "- 附录 A：完整方向动作卡（见 full_report_markdown / 结构化 JSON）",
            "- 附录 B：方向样本商品池任务明细（见 sample_pool_plan / 样本池子表）",
            "- 附录 C：新旧结论对账（见 full_report_markdown / 结构化 JSON）",
            "- 附录 D：算法诊断与结构化 JSON（见 market_insight_report.json）",
            "",
        ])
        return "\n".join(lines).strip() + "\n"

    def _v141_execution_plan(self, direction_actions: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        must: List[Dict[str, Any]] = []
        optional: List[Dict[str, Any]] = []
        deferred: List[Dict[str, Any]] = []
        for item in sorted(direction_actions, key=self._v141_action_sort_key):
            sample_count = int((item.get("card") or {}).get("direction_item_count") or 0)
            action = str(item.get("primary_action") or "")
            if sample_count < 5 and action not in {"P1_hidden_candidate", "P3_classification_review", "P4_no_action"}:
                deferred.append(self._v141_plan_item(item, "暂缓", "等待补样本"))
                continue
            if action == "P0_low_cost_test":
                must.append(self._v141_plan_item(item, "必须做", "低成本测款 + 内容基线"))
            elif action == "P0_cautious_test":
                must.append(self._v141_plan_item(item, "必须做", "谨慎切入验证"))
            elif action == "P0_hidden_small_test":
                optional.append(self._v141_plan_item(item, "可选做", "暗线小样本验证"))
            elif action == "P1_new_winner_dissection":
                if len([x for x in must if str(x.get("action_code", "")).startswith("P1_")]) < 1 and len(must) < 2:
                    must.append(self._v141_plan_item(item, "必须做", "新品赢家拆解"))
                else:
                    optional.append(self._v141_plan_item(item, "可选做", "新品赢家拆解"))
            elif action == "P1_strong_signal_verify":
                if len([x for x in must if str(x.get("action_code", "")).startswith("P1_")]) < 1 and len(must) < 2:
                    must.append(self._v141_plan_item(item, "必须做", "强信号待核验"))
                else:
                    optional.append(self._v141_plan_item(item, "可选做", "强信号待核验"))
            elif action == "P1_hidden_candidate":
                optional.append(self._v141_plan_item(item, "可选做", "暗线候选追踪"))
            elif action == "P3_classification_review" and str(item.get("direction") or "") == "other":
                if len(must) < 3:
                    must.append(self._v141_plan_item(item, "必须做", "分类复核"))
                else:
                    optional.append(self._v141_plan_item(item, "可选做", "分类复核"))
            elif action == "P2_old_listing_replacement_check":
                optional.append(self._v141_plan_item(item, "可选做", "老品替代核验"))
            elif action in {"P1_head_sample_dissection"}:
                optional.append(self._v141_plan_item(item, "可选做", "头部样本拆解"))
            else:
                deferred.append(self._v141_plan_item(item, "暂缓", "不执行"))

        must = must[:3]
        optional = optional[:2]
        active_ids = {id(item["source"]) for item in must + optional}
        for item in direction_actions:
            if id(item) in active_ids:
                continue
            if not any(row.get("direction") == item.get("direction") for row in deferred):
                deferred.append(self._v141_plan_item(item, "暂缓", "不执行"))
        return {"must": must, "optional": optional, "deferred": deferred}

    def _v141_plan_item(self, item: Dict[str, Any], level: str, action_label: str) -> Dict[str, Any]:
        return {
            "level": level,
            "action": action_label,
            "action_code": str(item.get("primary_action") or ""),
            "direction": str(item.get("direction") or ""),
            "output": self._v141_output_for_action(item, action_label),
            "source": item,
        }

    def _v141_output_for_action(self, item: Dict[str, Any], action_label: str) -> str:
        action = str(item.get("primary_action") or "")
        if action == "P0_low_cost_test":
            return "4–6 款 + 10–20 条内容"
        if action == "P0_cautious_test":
            return "2–3 款 + 差异化内容"
        if action == "P0_hidden_small_test":
            return "1–2 款小样本"
        if action == "P1_new_winner_dissection":
            return "新品样本池 + 可复制点"
        if action == "P1_strong_signal_verify":
            return "Top/新品核验样本 + 可采购性"
        if action == "P1_hidden_candidate":
            return "暗线样本池 + 下批追踪"
        if action == "P1_head_sample_dissection":
            return "Top 样本池 + 可复制点"
        if action == "P2_old_listing_replacement_check":
            return "老品胜出机制"
        if action == "P3_classification_review":
            return "是否拆出新方向"
        return "下批观察"

    def _v141_action_sort_key(self, item: Dict[str, Any]) -> Tuple[int, int, str]:
        action = str(item.get("primary_action") or "")
        direction = str(item.get("direction") or "")
        explicit = {
            "少女礼物感型": 0,
            "大体量气质型": 1,
            "高存在感拍照型": 1,
            "发箍修饰型": 2,
            "other": 3,
            "无耳洞友好型": 4,
            "头盔友好整理型": 4,
            "韩系轻通勤型": 5,
            "盘发效率型": 6,
            "甜感装饰型": 7,
            "珍珠锆石气质型": 8,
            "幸运寓意型": 9,
            "脸型修饰显瘦型": 10,
        }
        action_rank = {
            "P0_low_cost_test": 0,
            "P0_cautious_test": 1,
            "P0_hidden_small_test": 2,
            "P1_new_winner_dissection": 3,
            "P1_strong_signal_verify": 4,
            "P3_classification_review": 5,
            "P2_old_listing_replacement_check": 6,
            "P1_head_sample_dissection": 7,
            "P1_hidden_candidate": 8,
            "P2_observe_reserve": 9,
            "P4_no_action": 10,
        }
        return (action_rank.get(action, 9), explicit.get(direction, 99), direction)

    def _v141_report_confidence(self, diagnostics: Dict[str, Any]) -> Dict[str, str]:
        valid_count = int(diagnostics.get("valid_sample_count") or 0)
        valid_ratio = self._safe_float(diagnostics.get("valid_sample_ratio")) or 0.0
        other_ratio = self._safe_float(diagnostics.get("other_ratio")) or 0.0
        if valid_count >= 250 and valid_ratio >= 0.85:
            data_usability = "高"
        elif valid_count >= 150 and valid_ratio >= 0.70:
            data_usability = "中"
        else:
            data_usability = "低"
        if data_usability == "高" and other_ratio <= 0.10:
            decision_confidence = "中"
            reason = "样本可用于方向判断，但仍不适合直接支持大规模放量。"
        elif data_usability == "中":
            decision_confidence = "中"
            reason = "样本可用于小规模行动规划，需要结合样本池复核。"
        else:
            decision_confidence = "低"
            reason = "样本或有效率不足，优先做复核与低成本动作。"
        return {"data_usability": data_usability, "decision_confidence": decision_confidence, "reason": reason}

    def _v141_one_sentence(self, plan: Dict[str, List[Dict[str, Any]]], business: Dict[str, Any]) -> str:
        must = plan.get("must") or []
        p0 = [item for item in must if item.get("action_code") in {"P0_low_cost_test", "P0_cautious_test", "P0_hidden_small_test"}]
        p1 = [item for item in must if item.get("action_code") == "P1_new_winner_dissection"]
        if p0:
            extra = "，并优先拆解{names}".format(names="、".join(item["direction"] for item in p1[:1])) if p1 else ""
            return "本批不做方向级铺货，主线是{items}{extra}。".format(
                items="、".join("{direction}{action}".format(**item) for item in p0),
                extra=extra,
            )
        if must:
            return "本批无测款动作，主线是{actions}。".format(actions="、".join("{direction}{action}".format(**item) for item in must[:3]))
        return str(business.get("one_sentence_conclusion") or "本批只做观察和复核，不做方向级铺货。")

    def _v141_main_focus(self, plan: Dict[str, List[Dict[str, Any]]]) -> str:
        must = plan.get("must") or []
        if not must:
            return "无测款主攻，先做复核"
        return "；".join("{direction}{action}".format(**item) for item in must[:3])

    def _render_v141_execution_plan(self, plan: Dict[str, List[Dict[str, Any]]]) -> List[str]:
        lines: List[str] = []
        for key, label in [("must", "必须做"), ("optional", "可选做"), ("deferred", "暂缓")]:
            rows = list(plan.get(key) or [])
            if key == "deferred" and rows:
                grouped = self._v141_group_deferred(rows)
                lines.extend(["", "### {label}".format(label=label)])
                for row in grouped:
                    lines.append("- {action}｜{direction}：{output}".format(**row))
                continue
            lines.extend(["", "### {label}".format(label=label)])
            if not rows:
                lines.append("- 无")
                continue
            for row in rows:
                lines.append("- {action}｜{direction}：{output}".format(**row))
        return lines

    def _v141_group_deferred(self, rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        if not rows:
            return []
        directions = [row["direction"] for row in rows if row["direction"]]
        return [{"action": "不执行", "direction": "、".join(directions), "output": "下批观察"}]

    def _render_v141_key_rationales(self, plan: Dict[str, List[Dict[str, Any]]]) -> List[str]:
        rows = (plan.get("must") or []) + (plan.get("optional") or [])
        lines = []
        seen = set()
        for row in rows[:5]:
            item = row["source"]
            direction = row["direction"]
            if direction in seen:
                continue
            seen.add(direction)
            lines.append("- {direction}：{text}".format(direction=direction, text=self._v141_rationale_for_item(item)))
        return lines or ["- 本批暂无强行动依据，先做分类复核与下批观察。"]

    def _v141_rationale_for_item(self, item: Dict[str, Any]) -> str:
        card = dict(item.get("card") or {})
        direction = str(item.get("direction") or "")
        age = dict(card.get("product_age_structure") or {})
        demand = dict(card.get("demand_structure") or {})
        action = str(item.get("primary_action") or "")
        if action == "P0_low_cost_test":
            return "近90天新品销量占比 {new90}，Top3 占比 {top3}，更像小品牌切入窗口，适合小预算验证。".format(
                new90=self._format_percent_metric(age.get("new_90d_sales_share")),
                top3=self._format_percent_metric(demand.get("top3_sales_share")),
            )
        if action == "P0_cautious_test":
            return "成交基础存在，但新品窗口或竞争结构仍有不确定性，适合 2–3 款差异化样本谨慎验证。"
        if action == "P0_hidden_small_test":
            return "场景信号存在但样本仍有限，只适合 1–2 款小样本验证。"
        if action == "P1_new_winner_dissection":
            return "有新品信号，但方向整体未完全打开，先拆新品赢家的视觉结构、主图表达和内容钩子。"
        if action == "P1_strong_signal_verify":
            return "成交信号突出，但样本、价格带或供应链仍需核验，先看 Top/新品样本池再决定是否测款。"
        if action == "P1_hidden_candidate":
            return "暗线场景存在但样本极少，先进入样本池和下批追踪，不直接测款。"
        if action == "P1_head_sample_dissection":
            return "方向有头部或同质化风险，先拆 Top 样本胜出机制，不按方向铺普通款。"
        if action == "P2_old_listing_replacement_check":
            return "佩戴/风格需求存在，但当前老品占位偏强，适合做可选的老品替代核验。"
        if action == "P3_classification_review":
            return "不进入业务机会判断，先做分类复核，避免把归类失败误判成机会。"
        return "当前信号不足以占用主线资源，保留为下批观察。"

    def _render_v141_task_parameters(self) -> List[str]:
        return [
            "### 低成本测款",
            "- 默认量：4–6 款",
            "- 通过：1–2 款点击 / 加购 / 成交高于类目基线",
            "- 止损：全部低于 benchmark",
            "",
            "### 内容基线",
            "- 默认量：10–20 条内容，周期 14 天",
            "- 通过：1 条达到历史 P75，跑出 1–2 个可复用 hook",
            "- 止损：无可复用 hook",
            "",
            "### 新品赢家拆解",
            "- 默认量：新品 10–15 + 头部 5",
            "- 通过：找到 2 个可复制产品结构或内容钩子",
            "- 止损：主要靠达人 / 极低价 / 不可复制主图",
            "",
            "### 老品替代核验",
            "- 默认量：老品 5 + 新品 5",
            "- 通过：找到 2 个可采购新品替代样本",
            "- 止损：老品只靠链接权重，无替代结构",
            "",
            "### 分类复核",
            "- 默认量：Top10 + 新品10",
            "- 通过：拆出新方向或确认剔除",
            "- 止损：无稳定聚类",
        ]

    def _render_v31_required_action(self, item: Dict[str, Any]) -> List[str]:
        return [
            "",
            "### {label}｜{direction}".format(label=item["primary_action_label"], direction=item["direction"]),
            "- 为什么做：{why}".format(why=item.get("why") or "待补"),
            "- 怎么做：{method}".format(method=item.get("this_batch_method") or "待补"),
            "- 通过条件：{value}".format(value="；".join(item.get("acceptance_criteria") or [])),
            "- 止损条件：{value}".format(value="；".join(item.get("stop_loss_criteria") or [])),
        ]

    def _render_v31_execution_card(self, item: Dict[str, Any]) -> List[str]:
        trace = dict(item.get("p0_vs_p1_decision_trace") or {})
        questions = list(item.get("verification_questions") or [])
        lines = [
            "",
            "### {direction}".format(**item),
            "- 当前动作：{action}".format(action=item["primary_action_label"]),
            "- 为什么看：{why}".format(why=item.get("why") or "待补"),
            "- 为什么不是其他动作：{reason}".format(reason=trace.get("why_not_other_action") or "待补"),
            "- 机会类型：{primary} / {subtype}".format(primary=trace.get("primary_opportunity_type") or "待补", subtype=trace.get("secondary_opportunity_subtype") or "待补"),
            "- 核心风险：{risk}".format(risk=item.get("do_not_do") or "待补"),
            "- 样本池生成要求：{value}".format(value=self._v31_plan_text(item.get("sample_pool_plan") or [])),
            "- 下一步动作：{value}".format(value="；".join(item.get("expected_outputs") or [])),
            "",
            "核验问题：",
        ]
        for question in questions[:3]:
            lines.append("- {question}（方法：{method}；负责人：{owner}；时限：{deadline}；未回答则：{fallback}）".format(
                question=question.get("question"),
                method=question.get("answer_method"),
                owner=question.get("answer_owner"),
                deadline=question.get("deadline"),
                fallback=question.get("fallback_if_unanswered"),
            ))
        return lines

    def _render_v31_content_test_brief(self, test: Dict[str, Any]) -> List[str]:
        return [
            "",
            "### P0｜耳环内容基线测试",
            "- 方向：{directions}".format(directions="、".join(test.get("directions") or [])),
            "- 最小执行单元：每方向 {count} 条内容，MVP 总量 {total} 条，周期 {duration}".format(
                count=test.get("content_per_direction"),
                total=test.get("MVP_total_content"),
                duration=test.get("duration"),
            ),
            "- 通过条件：{criteria}".format(criteria="；".join(test.get("acceptance_criteria") or [])),
        ]

    def _render_v31_content_test_detail(self, test: Dict[str, Any]) -> List[str]:
        return [
            "",
            "### 内容基线测试｜{directions}".format(directions="、".join(test.get("directions") or [])),
            "- 内容数量：每方向 {count} 条，MVP 总量 {total} 条，完整基线阈值 {threshold} 条".format(
                count=test.get("content_per_direction"),
                total=test.get("MVP_total_content"),
                threshold=test.get("full_baseline_threshold"),
            ),
            "- 周期：{duration}".format(duration=test.get("duration")),
            "- 指标：{metrics}".format(metrics="、".join(test.get("metrics") or [])),
            "- benchmark 来源：{source}".format(source=test.get("benchmark_source")),
            "- 通过条件：{criteria}".format(criteria="；".join(test.get("acceptance_criteria") or [])),
            "- 失败条件：{criteria}".format(criteria="；".join(test.get("failure_criteria") or [])),
            "- 通过后：{value}".format(value=test.get("next_action_if_pass")),
            "- 失败后：{value}".format(value=test.get("next_action_if_fail")),
        ]

    def _v31_sample_pool_plan_for_action(self, card: Dict[str, Any], primary_action: str) -> List[Dict[str, Any]]:
        mapping = {
            "P0_low_cost_test": [
                ("new_product_sample", 15, "拉近90天新品，筛 4–6 款进入低成本验证"),
                ("similar_shape_candidate", 10, "找相似仿形款，验证可复制性"),
                ("price_band_sample", 5, "确认目标价格带和毛利空间"),
            ],
            "P0_cautious_test": [
                ("top_head_sample", 5, "对照头部样本胜出机制"),
                ("new_product_sample", 5, "筛少量可测新品或相似仿形款"),
                ("content_candidate_product", "2-3", "验证差异化内容切口"),
            ],
            "P0_hidden_small_test": [
                ("hidden_scene_sample", "1-2", "小样本验证暗线场景"),
                ("scene_keyword_sample", 5, "核验场景关键词和内容钩子"),
            ],
            "P1_new_winner_dissection": [
                ("new_product_sample", 15, "拆近90天新品赢家"),
                ("top_head_sample", 5, "对照头部样本胜出机制"),
                ("visual_tag_cluster", "聚类", "聚类视觉特征和主图表达"),
                ("price_band_cluster", "聚类", "聚类价格带和成交承接"),
            ],
            "P1_strong_signal_verify": [
                ("top_head_sample", 10, "核验高成交样本是否可采购"),
                ("new_product_sample", 5, "寻找代表新品或替代样本"),
                ("sourcing_check", "核验", "核验供应链和价格带可行性"),
            ],
            "P1_hidden_candidate": [
                ("hidden_scene_sample", 5, "保留暗线场景样本"),
                ("next_batch_tracking", "追踪", "下批重点追踪样本数和场景词"),
            ],
            "P1_head_sample_dissection": [
                ("top_head_sample", 15, "拆头部为什么赢"),
                ("old_listing_sample", 5, "识别老链接权重和不可复制因素"),
                ("content_or_creator_sample", 5, "拆内容/达人样本"),
            ],
            "P2_old_listing_replacement_check": [
                ("old_head_sample", 15, "拆老品稳定原因"),
                ("new_replacement_sample", 5, "寻找新品替代结构"),
                ("pain_point_sample", 5, "确认用户功能/场景痛点"),
            ],
            "P2_observe_reserve": [
                ("top_observe_sample", 3, "保留方向内高销量观察样本"),
                ("new_observe_sample", 3, "保留近90天观察新品"),
                ("next_batch_tracking", "追踪", "下批重点观察样本数、成交和新品窗口"),
            ],
            "P0_content_baseline_test": [
                ("content_candidate_product", "5-10", "筛内容测试候选品"),
                ("hook_type_plan", "3-5", "设计内容 hook 类型"),
                ("generated_video_plan", "10-20", "生成内容基线测试计划"),
            ],
            "P3_classification_review": [
                ("other_top_sales_sample", 10, "复核 other 高销量样本"),
                ("other_new_sample", 10, "复核 other 新品样本"),
                ("title_keyword_cluster", "聚类", "聚类标题关键词"),
                ("visual_cluster", "聚类", "聚类视觉特征"),
            ],
            "P4_no_action": [
                ("no_action_record", "记录", "记录暂缓原因和重新评估条件"),
            ],
        }
        groups = mapping.get(primary_action, [])
        return [{"sample_type": sample_type, "limit": limit, "purpose": purpose} for sample_type, limit, purpose in groups]

    def _v31_direction_execution_brief(
        self,
        card: Dict[str, Any],
        primary_action: str,
        secondary_actions: List[str],
        sample_pool_plan: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        name = str(card.get("direction_name") or "")
        direction_id = str(card.get("direction_canonical_key") or card.get("direction_instance_id") or name)
        source_action = str(card.get("actual_action") or card.get("decision_action") or "")
        action_map = {
            "P0_low_cost_test": ("prioritize_low_cost_test", "low_cost_test", "test_product_pool"),
            "P0_cautious_test": ("cautious_test", "low_cost_test", "test_product_pool"),
            "P0_hidden_small_test": ("hidden_small_test", "low_cost_test", "manual_review_pool"),
            "P1_new_winner_dissection": ("observe", "new_winner_deep_dive", "new_winner_analysis_pool"),
            "P1_head_sample_dissection": ("study_top_not_enter", "head_dissection", "head_reference_pool"),
            "P1_strong_signal_verify": ("strong_signal_verify", "signal_verify", "manual_review_pool"),
            "P1_hidden_candidate": ("hidden_candidate", "hidden_candidate", "observe_pool"),
            "P2_old_listing_replacement_check": ("observe", "old_product_replace", "replacement_candidate_pool"),
            "P2_observe_reserve": ("observe", "observe", "observe_pool"),
            "P3_classification_review": ("observe", "category_review", "category_review_pool"),
            "P4_no_action": ("avoid", "avoid", "eliminate"),
        }
        default_action, task_type, target_pool = action_map.get(primary_action, ("observe", "observe", "observe_pool"))
        direction_action = source_action or default_action
        value_points = [str(item) for item in list(card.get("top_value_points") or []) if str(item).strip()]
        elements = [str(item) for item in list(card.get("core_elements") or []) if str(item).strip()]
        scenes = [str(item) for item in list(card.get("scene_tags") or []) if str(item).strip()]
        profile = self._v31_direction_signal_profile(name)
        positive_signals = self._v31_brief_positive_signals(
            name=name,
            primary_action=primary_action,
            value_points=value_points,
            elements=elements,
            scenes=scenes,
        )
        requirements = self._v31_brief_requirements(name, primary_action, value_points, elements, scenes)
        return {
            "direction_id": direction_id,
            "direction_name": name,
            "direction_action": direction_action,
            "task_type": task_type,
            "target_pool": target_pool,
            "product_selection_requirements": requirements,
            "positive_signals": positive_signals,
            "negative_signals": self._v31_brief_negative_signals(primary_action, name),
            "sample_pool_requirements": [
                "{sample_type} × {limit}".format(sample_type=item.get("sample_type"), limit=item.get("limit"))
                for item in sample_pool_plan
            ],
            "content_requirements": self._v31_brief_content_requirements(primary_action, secondary_actions),
            "output_requirements": self._v31_brief_output_requirements(primary_action, task_type, target_pool),
            "selection_focus": profile.get("selection_focus", []),
            "upgrade_condition": self._v31_acceptance_criteria(primary_action),
            "stop_condition": self._v31_stop_loss_criteria(primary_action),
            "brief_source": "generated",
            "brief_confidence": "high" if primary_action.startswith("P0_") else "medium",
        }

    def _v31_brief_requirements(
        self,
        name: str,
        primary_action: str,
        value_points: List[str],
        elements: List[str],
        scenes: List[str],
    ) -> List[str]:
        if primary_action == "P0_low_cost_test":
            return [
                "近90天新品优先，或与近90天赢家款具有相似形态",
                "必须具备明确视觉记忆点、场景或价值点",
                "不能只是普通同质款",
                "价格和毛利满足小预算测试",
            ] + (value_points[:2] or elements[:2] or scenes[:2])
        if primary_action == "P1_new_winner_dissection":
            return [
                "近90天新品优先",
                "主图或标题能看出明确造型结构",
                "不要求直接进入测品池，先拆可复制点",
            ] + (elements[:2] or value_points[:2])
        if primary_action == "P2_old_listing_replacement_check":
            return [
                "优先新品或新链接",
                "必须解决同类老品的核心痛点",
                "不能只是低价复制老品",
            ] + (scenes[:2] or value_points[:2])
        if primary_action == "P3_classification_review":
            return ["只用于重新归类，不参与常规选品评分", "优先高销量 other 和新品 other 样本"]
        profile = self._v31_direction_signal_profile(name)
        base = list(profile.get("requirements") or [])
        if not base:
            base = ["按方向匹配、产品本体、内容潜力和差异化判断", "不满足任务规格时进入观察或人工复核"]
        return base[:5]

    def _v31_brief_positive_signals(
        self,
        name: str,
        primary_action: str,
        value_points: List[str],
        elements: List[str],
        scenes: List[str],
    ) -> List[str]:
        profile = self._v31_direction_signal_profile(name)
        signals = list(profile.get("positive_signals") or [])
        for item in value_points + elements + scenes:
            text = str(item or "").strip()
            if text and text not in signals:
                signals.append(text)
        if signals:
            return signals[:8]
        if primary_action == "P3_classification_review":
            return ["高销量但未归类样本", "近90天未归类新品", "可聚类标题/视觉特征"]
        if primary_action in {"P0_low_cost_test", "P0_cautious_test", "P0_hidden_small_test"}:
            return ["近90天新品", "明确视觉记忆点", "可形成内容第一句话", "价格和毛利可小预算验证"]
        if primary_action == "P1_new_winner_dissection":
            return ["近90天新品赢家", "主图结构清晰", "内容钩子可拆解", "供应链可复制线索"]
        if primary_action == "P2_old_listing_replacement_check":
            return ["新品或新链接", "解决老品同类痛点", "具备替代差异", "可采购性待核验"]
        return ["方向匹配清晰", "产品信息完整", "有明确内容表达点", "具备差异化线索"]

    def _v31_brief_negative_signals(self, primary_action: str, name: str = "") -> List[str]:
        profile = self._v31_direction_signal_profile(name)
        mapping = {
            "P0_low_cost_test": ["过度同质", "廉价感明显", "仅靠达人或极低价", "无内容第一句话"],
            "P1_new_winner_dissection": ["只靠达人脸/强账号", "只靠极低价", "供应链难复制"],
            "P2_old_listing_replacement_check": ["和老品完全同质", "佩戴/使用痛点不清", "只是低价复制"],
            "P3_classification_review": ["方向信息不完整", "标题和主图无法归类"],
        }
        negatives = list(mapping.get(primary_action) or [])
        for item in list(profile.get("negative_signals") or []):
            if item not in negatives:
                negatives.append(item)
        if negatives:
            return negatives[:8]
        return ["信息不足", "任务规格不清", "缺少可执行差异"]

    def _v31_brief_content_requirements(self, primary_action: str, secondary_actions: List[str]) -> List[str]:
        if primary_action == "P0_low_cost_test":
            return ["每款 3-5 条内容", "测试周期 7-10 天", "至少形成 1 个可复用 hook"]
        if "P0_content_baseline_test" in secondary_actions:
            return ["每方向 5-10 条内容", "14 天建立内容点击和商品点击基线"]
        if primary_action == "P1_new_winner_dissection":
            return ["输出可复制点 / 不可复制点", "判断是否升级 P0"]
        return []

    def _v31_brief_output_requirements(self, primary_action: str, task_type: str, target_pool: str) -> List[str]:
        if primary_action in {"P0_low_cost_test", "P0_cautious_test", "P0_hidden_small_test"}:
            return ["测款候选清单", "内容 hook 清单", "7-10 天测试结果", "放大/止损结论"]
        if primary_action == "P1_new_winner_dissection" or task_type == "new_winner_deep_dive":
            return ["新品赢家拆解表", "可复制点 / 不可复制点", "是否升级 P0 的判断"]
        if primary_action == "P1_head_sample_dissection" or task_type == "head_dissection":
            return ["头部样本拆解表", "头部可复制性判断", "可绕开切口"]
        if primary_action == "P2_old_listing_replacement_check" or task_type == "old_product_replace":
            return ["老品替代核验表", "新品替代样本", "是否继续追踪"]
        if primary_action == "P3_classification_review" or target_pool == "category_review_pool":
            return ["other 分类复核表", "新方向候选", "剔除样本清单"]
        if task_type in {"observe", "hidden_candidate"}:
            return ["观察样本记录", "下批升级/放弃条件"]
        return ["任务池记录", "人工复核结论"]

    def _v31_direction_signal_profile(self, name: str) -> Dict[str, List[str]]:
        profiles: Dict[str, Dict[str, List[str]]] = {
            "少女礼物感型": {
                "requirements": ["礼物感 / 套装感 / 拍照感至少命中一项", "小体积但有视觉记忆点", "不能像儿童饰品或普通甜美款", "价格适合低成本小礼物"],
                "positive_signals": ["礼物感", "套装感", "拍照上镜", "小体积但有记忆点", "学生互送", "包装/佩戴/拍照三段式内容"],
                "negative_signals": ["过度幼稚", "廉价感明显", "普通花朵/珍珠无记忆点", "只有包装好看但产品弱"],
                "selection_focus": ["近90天新品", "礼物包装", "套装组合", "寓意表达", "价格冲动购买"],
            },
            "甜感装饰型": {
                "requirements": ["甜感明显但不能廉价幼稚", "颜色/形状/材质有记忆点", "适合拍照或约会场景", "不能只是普通布艺/蝴蝶结重复款"],
                "positive_signals": ["甜感明显", "颜色或形状容易被看见", "拍照/约会可表达", "有无发饰对比明显"],
                "negative_signals": ["审美同质化", "廉价感", "幼稚感过强", "普通甜美款无差异"],
                "selection_focus": ["视觉记忆点", "材质细节", "上头效果", "甜感但不夸张"],
            },
            "大体量气质型": {
                "requirements": ["体量感明显但不笨重", "适合长发/厚发或造型完整度表达", "侧后方轮廓清晰", "不能只是普通大抓夹"],
                "positive_signals": ["体量感", "撑起头型", "长发/厚发友好", "提升造型完整度", "侧后方轮廓明显"],
                "negative_signals": ["过重显笨", "普通大抓夹无结构", "只能靠低价", "主图无法看出体量"],
                "selection_focus": ["大抓夹体量", "长发厚发", "发型轮廓", "气质造型"],
            },
            "韩系轻通勤型": {
                "requirements": ["低调但有细节", "适合学生/上班通勤", "基础色/金属/珍珠小细节清晰", "不能只是普通小耳钉/小发夹"],
                "positive_signals": ["低调不幼稚", "通勤百搭", "轻精致细节", "普通穿搭加分", "简洁但不普通"],
                "negative_signals": ["过度基础无记忆点", "韩系标签空泛", "内容只能说好看百搭", "同质化严重"],
                "selection_focus": ["通勤穿搭", "低调细节", "基础色", "轻精致材质"],
            },
            "盘发效率型": {
                "requirements": ["操作简单", "能快速整理头发", "夹得稳或厚发可用", "必须能做前后对比演示"],
                "positive_signals": ["快速整理", "夹稳", "厚发可用", "30秒盘发", "热天出门前整理", "一镜到底操作"],
                "negative_signals": ["操作复杂", "只拍结果不证明过程", "夹不稳", "和老品完全同质"],
                "selection_focus": ["快速盘发", "厚发固定", "操作步骤", "前后对比"],
            },
            "头盔友好整理型": {
                "requirements": ["明确头盔/摩托/通勤场景", "摘头盔后快速恢复发型", "不能泛化成普通盘发效率", "样本少时优先进入暗线追踪"],
                "positive_signals": ["头盔后整理", "摩托通勤", "快速恢复发型", "摘头盔前后对比", "本地生活场景"],
                "negative_signals": ["没有头盔场景证据", "只是普通整理工具", "样本极少且不可采购", "场景无法拍出来"],
                "selection_focus": ["头盔场景", "通勤女生", "摘头盔瞬间", "头发压乱后整理"],
            },
            "发箍修饰型": {
                "requirements": ["能修饰头型/脸型或压碎发", "佩戴前后变化可见", "不能只是普通发箍", "不勒头或舒适性可表达"],
                "positive_signals": ["修饰头型", "显脸小", "压碎发", "不勒头", "戴前戴后对比"],
                "negative_signals": ["普通发箍无变化", "勒头风险", "主图无法证明修饰效果", "样本少且不可采购"],
                "selection_focus": ["头型变化", "碎发控制", "正脸/侧脸对比", "舒适佩戴"],
            },
            "发圈套组型": {
                "requirements": ["多件组合更划算", "颜色搭配日常", "不勒头发或不易掉", "适合扎发/手腕佩戴双场景"],
                "positive_signals": ["多件划算", "颜色日常", "不勒头发", "手腕佩戴", "一周不同搭配"],
                "negative_signals": ["只是低价多件", "颜色杂乱", "无佩戴证明", "材质廉价"],
                "selection_focus": ["套组颜色", "扎发场景", "手腕佩戴", "宿舍/办公室备用"],
            },
            "基础通勤型": {
                "requirements": ["日常上班/上学可用", "低调耐看但不廉价", "多场景搭配", "不能只是无差异基础款"],
                "positive_signals": ["上班上学通勤", "低调耐看", "基础但不廉价", "多场景搭配", "出门前快速整理"],
                "negative_signals": ["普通基础款无购买理由", "内容无第一句话", "同款过多", "价格无优势"],
                "selection_focus": ["通勤场景", "基础色", "耐看材质", "多穿搭适配"],
            },
            "无耳洞友好型": {
                "requirements": ["必须明确无耳洞/耳夹/耳扣/耳骨夹", "佩戴方式清晰", "优先不痛不掉或像真耳钉", "不能和有耳洞耳环混淆"],
                "positive_signals": ["无耳洞", "不痛", "不掉", "像真耳钉", "佩戴动作清晰", "轻晃不掉落"],
                "negative_signals": ["有无耳洞属性不清", "耳夹痛感风险", "固定不稳", "佩戴方式错误"],
                "selection_focus": ["耳夹/耳扣", "佩戴 proof", "不痛不掉", "无耳洞学生党"],
            },
            "高存在感拍照型": {
                "requirements": ["主图视觉存在感强", "形状/颜色/体量有记忆点", "适合拍照/出片场景", "不能只靠达人脸或强账号"],
                "positive_signals": ["高存在感", "侧脸首镜强", "拍照出片", "造型结构明显", "简单衣服加耳环对比"],
                "negative_signals": ["太夸张不日常", "重量感风险", "只靠达人脸", "供应链难复制"],
                "selection_focus": ["大耳圈/大耳坠", "侧脸视觉", "旅行/周末拍照", "强主图结构"],
            },
            "珍珠锆石气质型": {
                "requirements": ["珍珠/锆石质感清晰", "微闪但不俗", "适合通勤/约会/拍照", "不能只是普通珍珠锆石同质款"],
                "positive_signals": ["珍珠质感", "锆石微闪", "轻熟气质", "侧脸近景", "穿搭完成度提升"],
                "negative_signals": ["珍珠比例失真", "锆石过闪显假", "质感不清晰", "普通珍珠锆石无差异"],
                "selection_focus": ["材质质感", "微闪效果", "短款垂坠", "侧脸佩戴"],
            },
            "脸型修饰显瘦型": {
                "requirements": ["长线条/水滴/细长结构明确", "能表达拉长脸部线条", "适合正脸侧脸对比", "不能夸大显脸小效果"],
                "positive_signals": ["修饰脸型", "拉长脸部线条", "显脸小", "侧脸更轻盈", "拨发露耳对比"],
                "negative_signals": ["耳线过长", "比例失真", "显瘦效果无法证明", "样本太少"],
                "selection_focus": ["长款耳坠", "线条型耳环", "正侧脸对比", "脸部线条"],
            },
            "幸运寓意型": {
                "requirements": ["四叶草/爱心/星月/寓意符号明确", "寓意表达轻，不像广告", "适合自戴或小礼物", "本地文化适配不过度"],
                "positive_signals": ["幸运寓意", "四叶草/爱心/星月", "小礼物", "情绪价值", "日常好运感"],
                "negative_signals": ["寓意表达过重", "广告感", "文化适配不清", "符号普通无记忆点"],
                "selection_focus": ["寓意符号", "轻口播解释", "小价格情绪价值", "礼物场景"],
            },
            "other": {
                "requirements": ["只用于重新归类，不参与常规选品评分", "优先高销量 other 和新品 other 样本"],
                "positive_signals": ["高销量未归类样本", "近90天未归类新品", "可聚类标题关键词", "可聚类视觉特征"],
                "negative_signals": ["方向信息不完整", "标题和主图无法归类", "不可直接做业务判断"],
                "selection_focus": ["分类复核", "新方向候选", "剔除无效样本"],
            },
        }
        return profiles.get(name, {})

    def _v31_verification_questions(self, card: Dict[str, Any], primary_action: str) -> List[Dict[str, Any]]:
        name = str(card.get("direction_name") or "")
        question_map = {
            "少女礼物感型": [
                "近90天新品销量集中在哪些视觉特征？",
                "是礼物包装、套装感、寓意感、低价冲动购买，还是单款设计本身？",
                "是否能形成“送人不贵但看起来用心”的短视频钩子？",
            ],
            "韩系轻通勤型": [
                "头部是否有“简洁但不普通”的结构？",
                "是否有明确通勤佩戴理由，例如不夸张、显脸干净、搭衣服？",
                "还是只是普通小耳钉、小圆环、小珍珠？",
            ],
            "无耳洞友好型": [
                "老品赢在真实功能痛点，还是老链接权重？",
                "用户买的是不痛、不掉、不用打耳洞，还是单纯款式？",
                "新品是否能提供更清楚的佩戴 proof？",
            ],
            "高存在感拍照型": [
                "该方向是否真有供给泡沫证据？",
                "它是拍照好看但不成交，还是缺少合适内容承接？",
                "是否存在可被 AI 内容放大的强首镜样本？",
            ],
            "珍珠锆石气质型": [
                "头部差异到底来自材质质感、搭配场景、价格，还是主图精修？",
                "是否存在普通珍珠锆石之外的结构差异？",
                "侧脸近景和微闪展示是否能形成内容 proof？",
            ],
        }
        questions = question_map.get(name) or [
            "该方向 Top 样本是否有可复制产品结构？",
            "代表新品是否能形成明确内容第一句话？",
            "当前供应链是否能找到相似但不完全同质的样本？",
        ]
        return [
            {
                "question": question,
                "answer_method": "manual_review",
                "data_required": ["sample_pool_items", "product_images", "sales_7d", "listing_age_days"],
                "answer_owner": "选品负责人",
                "expected_format": "Top3 结论 + 代表商品 + 是否可复制",
                "deadline": "样本池生成后48小时内",
                "fallback_if_unanswered": "该方向降级为 P2_observe_reserve",
            }
            for question in questions
        ]

    def _v31_execution_sequence(self, primary_action: str, direction_name: str, secondary_actions: List[str]) -> List[Dict[str, str]]:
        if primary_action == "P0_low_cost_test":
            return [
                {"day": "Day 1-2", "task": "生成样本池，筛选 4–6 款近90天新品或相似仿形款"},
                {"day": "Day 2-3", "task": "为每款生成 3–5 条 AI 内容"},
                {"day": "Day 4-10", "task": "小预算上架测试，记录点击、加购、转化"},
                {"day": "Day 11-14", "task": "计算内容 baseline 和商品验证结果，决定放大/止损"},
            ]
        if primary_action == "P0_cautious_test":
            return [
                {"day": "Day 1-2", "task": "筛选 2–3 款差异化样本"},
                {"day": "Day 2-3", "task": "为每款准备低成本内容验证"},
                {"day": "Day 4-10", "task": "小预算验证点击、加购和转化"},
            ]
        if primary_action == "P0_hidden_small_test":
            return [
                {"day": "Day 1-2", "task": "筛选 1–2 款暗线场景样本"},
                {"day": "Day 3-7", "task": "用小样本内容验证场景是否成立"},
            ]
        if primary_action == "P1_strong_signal_verify":
            return [
                {"day": "Day 1-2", "task": "拉 Top / 新品样本池"},
                {"day": "Day 3-4", "task": "核验可采购性、价格带和差异化切口"},
            ]
        if primary_action == "P1_hidden_candidate":
            return [
                {"day": "下一批", "task": "追踪样本数、场景词和可采购样本是否增加"},
            ]
        if primary_action.startswith("P1_"):
            return [
                {"day": "Day 1-2", "task": "生成 Top / 新品样本池"},
                {"day": "Day 2-4", "task": "拆产品结构、价格带、主图表达和内容钩子"},
                {"day": "Day 5", "task": "判断是否升级 P0_low_cost_test"},
            ]
        if primary_action == "P2_old_listing_replacement_check":
            return [
                {"day": "Day 1-2", "task": "拉老品头部和新品替代样本"},
                {"day": "Day 3-4", "task": "拆老品胜出机制和新品替代缺口"},
                {"day": "Day 5", "task": "决定是否进入 P1_new_winner_dissection"},
            ]
        if primary_action == "P3_classification_review":
            return [
                {"day": "Day 1", "task": "拉 other 高销量和新品样本"},
                {"day": "Day 2", "task": "做标题 / 图片 / 场景聚类"},
            ]
        return [{"day": "下一批", "task": "观察核心指标是否改善"}]

    def _v31_acceptance_criteria(self, primary_action: str) -> List[str]:
        mapping = {
            "P0_low_cost_test": [
                "至少 1–2 款点击 / 加购 / 成交明显高于类目基线",
                "至少跑出 1 个可复用内容第一句话",
                "可采购成本满足目标毛利",
            ],
            "P0_cautious_test": [
                "至少 1 款点击 / 加购明显高于类目基线",
                "差异化内容切口可复用",
            ],
            "P0_hidden_small_test": [
                "至少 1 个场景内容钩子有点击或互动反馈",
            ],
            "P1_new_winner_dissection": [
                "至少找到 3 个非同质化新品样本",
                "至少 2 个样本具备明确产品结构或内容钩子",
            ],
            "P1_strong_signal_verify": [
                "至少找到 2 个可采购样本",
                "价格带和差异化切口达到可测试条件",
            ],
            "P1_hidden_candidate": [
                "下一批样本数增加，或人工确认 2 个以上可采购样本",
            ],
            "P1_head_sample_dissection": [
                "至少拆出 2 个可迁移产品共性",
                "至少拆出 2 个可迁移内容结构",
            ],
            "P2_old_listing_replacement_check": [
                "明确老品赢在功能痛点、款式、价格还是链接权重",
                "找到至少 2 个可采购新品替代样本",
            ],
            "P3_classification_review": [
                "other 中能拆出明确新方向，或确认不进入业务判断",
            ],
        }
        return mapping.get(primary_action, ["下一批核心指标改善后再评估"])

    def _v31_stop_loss_criteria(self, primary_action: str) -> List[str]:
        mapping = {
            "P0_low_cost_test": [
                "所有内容点击和商品点击均低于 benchmark",
                "成交主要依赖达人背书、老链接权重或极低价且不可复制",
            ],
            "P0_cautious_test": [
                "少量样本均无法跑出点击或加购信号",
                "差异化切口无法被内容表达",
            ],
            "P0_hidden_small_test": [
                "场景内容无点击或互动反馈",
            ],
            "P1_new_winner_dissection": [
                "新品赢家主要靠不可复制达人/主图/低价",
                "找不到明确产品结构或内容钩子",
            ],
            "P1_strong_signal_verify": [
                "找不到可采购样本或价格带不成立",
            ],
            "P1_hidden_candidate": [
                "下批仍无相似样本或可采购样本",
            ],
            "P1_head_sample_dissection": [
                "头部商品不可采购或价格无优势",
                "头部内容依赖真人达人/强账号权重",
            ],
            "P2_old_listing_replacement_check": [
                "老品胜出主要依赖链接权重，缺少新品替代结构",
            ],
            "P3_classification_review": [
                "other 样本无稳定聚类，继续保留为分类待补",
            ],
        }
        return mapping.get(primary_action, ["下一批仍无新品、内容或供应链证据"])

    def _v31_expected_outputs(self, primary_action: str) -> List[str]:
        mapping = {
            "P0_low_cost_test": ["4–6 款测试清单", "10–20 条内容基线计划", "14 天测试复盘"],
            "P0_cautious_test": ["2–3 款谨慎验证清单", "差异化内容计划", "小预算复盘"],
            "P0_hidden_small_test": ["1–2 款暗线测试清单", "场景内容验证结果"],
            "P1_new_winner_dissection": ["新品赢家拆解表", "可复制/不可复制因素", "是否升级 P0 的判断"],
            "P1_strong_signal_verify": ["强信号核验表", "可采购样本", "是否进入小样本验证"],
            "P1_hidden_candidate": ["暗线样本池", "下批追踪条件"],
            "P1_head_sample_dissection": ["Top 样本拆解表", "头部胜出机制", "绕开切口"],
            "P2_old_listing_replacement_check": ["老品替代核验表", "新品替代样本", "是否继续追踪"],
            "P3_classification_review": ["other 复核样本池", "新方向候选或归类结论"],
        }
        return mapping.get(primary_action, ["下批观察结论"])

    def _v31_this_batch_method(self, card: Dict[str, Any], primary_action: str) -> str:
        name = str(card.get("direction_name") or "")
        if primary_action == "P0_low_cost_test":
            return "选 4–6 款近90天新品或相似仿形款；每款配 3–5 条内容；7–10 天小预算测试。"
        if primary_action == "P0_cautious_test":
            return "选 2–3 款差异化样本；用小预算验证内容切口和成交承接。"
        if primary_action == "P0_hidden_small_test":
            return "选 1–2 款暗线场景样本；先验证场景内容是否成立。"
        if primary_action == "P1_new_winner_dissection":
            return "拉近90天新品样本池，拆视觉结构、价格带、主图表达、内容钩子和供应链可复制性。"
        if primary_action == "P1_strong_signal_verify":
            return "拉 Top / 新品样本池，先核验可采购性、价格带和差异化切口。"
        if primary_action == "P1_hidden_candidate":
            return "进入暗线样本池和下批重点追踪，暂不直接测款。"
        if primary_action == "P1_head_sample_dissection":
            return "拉 Top10 / Top15 样本，拆产品结构、价格、主图、达人内容、老链接权重和场景。"
        if primary_action == "P2_old_listing_replacement_check":
            return "拆老品为什么稳，寻找新品替代结构，不直接铺普通款。"
        if primary_action == "P3_classification_review":
            return "拉 other 高销量和新品样本，做标题 / 图片 / 场景聚类。"
        return "本批不占用测款资源，只记录下一批升级条件。"

    def _v31_do_not_do(self, card: Dict[str, Any], primary_action: str) -> str:
        if primary_action == "P0_low_cost_test":
            return "不做方向级批量铺货，不铺普通同质款。"
        if primary_action == "P0_cautious_test":
            return "不扩大铺货，只用少量差异化样本验证。"
        if primary_action == "P0_hidden_small_test":
            return "不把暗线方向当主线，只做 1–2 款场景验证。"
        if primary_action == "P1_new_winner_dissection":
            return "不盲目跟新品，先确认新品赢家为什么赢。"
        if primary_action == "P1_strong_signal_verify":
            return "不直接测款，先核验样本池和供应链。"
        if primary_action == "P1_hidden_candidate":
            return "不直接进入验证池，只做暗线追踪。"
        if primary_action == "P1_head_sample_dissection":
            return "不按方向铺普通款，只拆可复制头部结构。"
        if primary_action == "P2_old_listing_replacement_check":
            return "不直接复制老链接普通款，先找替代结构。"
        if primary_action == "P3_classification_review":
            return "other 不进入选品判断。"
        return "不占用测款和内容资源。"

    def _v31_do_not_do_summary(self, direction_actions: List[Dict[str, Any]]) -> List[str]:
        return [
            "不做方向级批量铺货。",
            "不把补数据当作唯一动作。",
            "不把 other 方向纳入选品判断。",
            "不铺普通同质化耳环款。",
            "没有样本池核验前，不启动内容批量生产。",
        ]

    def _v31_why_text(self, card: Dict[str, Any], primary_action: str) -> str:
        name = str(card.get("direction_name") or "")
        age = dict(card.get("product_age_structure") or {})
        new90 = self._format_percent_metric(age.get("new_90d_sales_share"))
        top3 = self._format_percent_metric((card.get("demand_structure") or {}).get("top3_sales_share"))
        if primary_action == "P0_low_cost_test":
            return "{name} 近90天新品销量占比 {new90}，Top3 占比 {top3}，更像成熟红海中的小品牌切入窗口，适合低成本验证。".format(name=name, new90=new90, top3=top3)
        if primary_action == "P0_cautious_test":
            return "{name} 具备成交基础，但仍有竞争或新品窗口风险，适合 2–3 款谨慎验证。".format(name=name)
        if primary_action == "P0_hidden_small_test":
            return "{name} 场景信号存在但样本仍有限，只适合 1–2 款小样本验证。".format(name=name)
        if primary_action == "P1_new_winner_dissection":
            return "{name} 有新品信号，但还不能证明方向整体开放，先拆新品赢家为什么能跑出。".format(name=name)
        if primary_action == "P1_strong_signal_verify":
            return "{name} 需求信号强，但样本或供应链仍需核验，先看样本池再决定是否测款。".format(name=name)
        if primary_action == "P1_hidden_candidate":
            return "{name} 属于暗线候选，先进入样本池和下批追踪，不直接测款。".format(name=name)
        if primary_action == "P1_head_sample_dissection":
            return "{name} 头部或审美同质风险较高，先拆头部胜出机制，不直接铺货。".format(name=name)
        if primary_action == "P2_old_listing_replacement_check":
            return "{name} 需求可能存在，但老品占位强，先判断新品替代是否成立。".format(name=name)
        if primary_action == "P3_classification_review":
            return "{name} 结构不清晰，先复核分类，不进入业务机会判断。".format(name=name)
        return "{name} 当前关键证据不足，先观察储备。".format(name=name)

    def _v31_action_label(self, action: str) -> str:
        labels = {
            "P0_low_cost_test": "P0 低成本测款",
            "P0_content_baseline_test": "P0 内容基线测试",
            "P0_cautious_test": "P0 谨慎切入验证",
            "P0_hidden_small_test": "P0 暗线小样本验证",
            "P1_new_winner_dissection": "P1 新品赢家拆解",
            "P1_strong_signal_verify": "P1 强信号待核验",
            "P1_hidden_candidate": "P1 暗线候选",
            "P1_head_sample_dissection": "P1 头部样本拆解",
            "P2_old_listing_replacement_check": "P2 老品替代核验",
            "P2_observe_reserve": "P2 观察储备",
            "P3_classification_review": "P3 分类复核",
            "P4_no_action": "P4 暂不进入",
        }
        return labels.get(action, action or "待补动作")

    def _v31_priority(self, primary_action: str) -> str:
        if primary_action.startswith("P0_"):
            return "P0"
        if primary_action.startswith("P1_"):
            return "P1"
        if primary_action.startswith("P2_"):
            return "P2"
        if primary_action.startswith("P3_"):
            return "P3"
        return "P4"

    def _confidence_rank(self, value: str) -> int:
        normalized = value.strip().lower()
        mapping = {"high": 3, "高": 3, "medium": 2, "中": 2, "low": 1, "低": 1, "insufficient": 0, "不足": 0}
        return mapping.get(normalized, 1)

    def _capability_rank(self, value: str) -> int:
        normalized = value.strip().lower()
        mapping = {"high": 2, "高": 2, "medium": 1, "中": 1, "low": 0, "低": 0, "unknown": 1, "": 1}
        return mapping.get(normalized, 1)

    def _v31_plan_text(self, plans: List[Dict[str, Any]]) -> str:
        return "；".join("{sample_type}×{limit}".format(sample_type=plan.get("sample_type"), limit=plan.get("limit")) for plan in plans) or "无需样本池"

    def _build_report_confidence(
        self,
        cards: List[Dict[str, Any]],
        total_product_count: Optional[int],
        completed_product_count: Optional[int],
        source_scope: str,
        quality_gate: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        quality = dict(quality_gate or {})
        valid_sample_count = int(quality.get("valid_sample_count") or sum(int(card.get("direction_item_count") or 0) for card in cards))
        total_count = int(total_product_count or quality.get("completed_product_count") or completed_product_count or valid_sample_count or 0)
        other_count = sum(
            int(card.get("direction_item_count") or 0)
            for card in cards
            if str(card.get("style_cluster") or card.get("direction_name") or "").strip().lower() == "other"
        )
        other_ratio = float(other_count) / float(valid_sample_count) if valid_sample_count > 0 else 0.0
        direction_count = len(cards)
        valid_ratio = quality.get("valid_sample_ratio")
        if valid_ratio in {None, ""} and total_count > 0:
            valid_ratio = float(valid_sample_count) / float(total_count)

        coverage_level = "高" if valid_sample_count >= 500 else ("中" if valid_sample_count >= 300 else ("低" if valid_sample_count >= 150 else "不足"))
        distribution_stability = "集中" if direction_count <= 5 else ("正常" if direction_count <= 10 else "分散")
        report_status = "可消费" if bool(quality.get("quality_gate_passed", True)) else "需复核"
        if source_scope and source_scope != "official":
            report_status = "实验结果"

        levels = ["不足", "低", "中", "高"]
        score = 2  # 初始为“中”
        reasons: List[str] = []
        if valid_sample_count < 300:
            score -= 1
            reasons.append("有效样本不足 300")
        if other_ratio > 0.20:
            score -= 1
            reasons.append("other 占比超过 20%，方向分类污染较高")
        elif other_ratio > 0.10:
            reasons.append("other 占比超过 10%，存在轻度分类污染")
        if report_status != "可消费":
            score -= 1
            reasons.append("报告状态不是可消费正式结果")
        if source_scope and source_scope != "official":
            score -= 1
            reasons.append("source_scope 不是 official")
        if valid_ratio is not None:
            try:
                if float(valid_ratio) < 0.80:
                    score -= 1
                    reasons.append("有效率低于 80%")
            except (TypeError, ValueError):
                pass
        score = max(0, min(score, len(levels) - 1))
        return {
            "market": cards[0].get("country") if cards else "",
            "category": cards[0].get("category") if cards else "",
            "total_sample_count": total_count,
            "completed_product_count": int(completed_product_count or total_count or 0),
            "valid_sample_count": valid_sample_count,
            "valid_sample_ratio": valid_ratio,
            "direction_count": direction_count,
            "other_count": other_count,
            "other_ratio": other_ratio,
            "sample_coverage_level": coverage_level,
            "direction_distribution_stability": distribution_stability,
            "report_status": report_status,
            "source_scope": source_scope or "unknown",
            "report_confidence": levels[score],
            "confidence_reasons": reasons or ["基础样本与方向分布可支撑当前报告"],
        }

    def _direction_and_sample_action(self, card: Dict[str, Any]) -> Dict[str, Any]:
        action = str(card.get("decision_action") or "")
        name = str(card.get("style_cluster") or card.get("direction_name") or "")
        competition_type = str((card.get("competition_structure") or {}).get("competition_type") or "")
        if name == "other":
            return {
                "direction_action": "方向级归类待补",
                "sample_action": "不进入样本池",
                "sample_pool_required": False,
                "priority": "P3",
            }
        if action == "prioritize_low_cost_test":
            return {"direction_action": "方向级谨慎进入", "sample_action": "样本级低成本验证", "sample_pool_required": True, "priority": "P1"}
        if action in {"cautious_test", "hidden_small_test"}:
            return {"direction_action": "方向级观察", "sample_action": "样本级低成本验证", "sample_pool_required": True, "priority": "P1"}
        if action == "study_top_not_enter":
            return {"direction_action": "方向级观察", "sample_action": "样本级拆头部", "sample_pool_required": True, "priority": "P1"}
        if action in {"strong_signal_verify", "hidden_candidate"}:
            return {"direction_action": "方向级观察", "sample_action": "样本级观察储备", "sample_pool_required": True, "priority": "P2"}
        if action == "avoid":
            return {"direction_action": "方向级暂缓", "sample_action": "不进入样本池", "sample_pool_required": False, "priority": "P3"}
        if competition_type in {"few_new_winners", "price_band_gap", "content_gap"}:
            return {"direction_action": "方向级观察", "sample_action": "样本级观察储备", "sample_pool_required": True, "priority": "P2"}
        return {"direction_action": "方向级观察", "sample_action": "样本级观察储备", "sample_pool_required": False, "priority": "P3"}

    def _build_direction_decision_trace(self, cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        traces = []
        for card in cards:
            action_info = self._direction_and_sample_action(card)
            name = str(card.get("style_cluster") or card.get("direction_name") or "")
            if name == "other":
                traces.append(
                    {
                        "direction": name,
                        **action_info,
                        "main_reasons": ["方向归类不清晰，不参与业务机会判断"],
                        "risks": ["other 方向可能污染机会判断"],
                        "verification_questions": ["这些 other 样本是否能拆出新方向？"],
                    }
                )
                continue
            structure = dict(card.get("competition_structure") or {})
            traces.append(
                {
                    "direction": name,
                    **action_info,
                    "competition_type": structure.get("competition_type"),
                    "primary_opportunity_type": card.get("primary_opportunity_type"),
                    "main_reasons": self._trace_main_reasons(card),
                    "risks": self._trace_risks(card),
                    "verification_questions": self._verification_questions(card),
                }
            )
        return traces

    def _trace_main_reasons(self, card: Dict[str, Any]) -> List[str]:
        structure = dict(card.get("competition_structure") or {})
        age = dict(card.get("product_age_structure") or {})
        reasons = [str(structure.get("opportunity_interpretation") or "结构信号待核验")]
        if age.get("new_90d_sales_share") is not None:
            reasons.append("近90天新品销量占比 {value}".format(value=self._format_percent_metric(age.get("new_90d_sales_share"))))
        if card.get("direction_sales_median_7d") is not None:
            reasons.append("7日销量中位数 {value}".format(value=self._format_metric(card.get("direction_sales_median_7d"))))
        return reasons[:4]

    def _trace_risks(self, card: Dict[str, Any]) -> List[str]:
        risk_tags = list(card.get("risk_tags") or [])
        if not risk_tags:
            return ["需通过样本池确认头部、新品、价格带和内容表达是否可复制"]
        return [display_enum(str(tag), "risk_tag") for tag in risk_tags[:5]]

    def _verification_questions(self, card: Dict[str, Any]) -> List[str]:
        name = str(card.get("style_cluster") or card.get("direction_name") or "")
        competition_type = str((card.get("competition_structure") or {}).get("competition_type") or "")
        if name == "other":
            return ["是否可以从标题/图片/价格带中拆出新方向？", "是否需要补充耳环 taxonomy 或本地关键词？"]
        questions = [
            "Top10 是靠产品结构、价格、主图、达人内容，还是老链接权重胜出？",
            "近90天代表新品是否具备可复制的产品结构？",
        ]
        if competition_type in {"aesthetic_homogeneous", "few_new_winners"}:
            questions.append("头部是否存在非同质化视觉结构或明确佩戴理由？")
        if competition_type in {"content_gap", "local_scene_gap"}:
            questions.append("是否能形成 15 秒短视频第一句话和可证明的佩戴效果？")
        if competition_type == "price_band_gap":
            questions.append("目标价格带是否有足够毛利和稳定供应？")
        return questions[:4]

    def _build_opportunity_diagnostics(self, cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        specs = [
            ("new_product_opportunity", "新品切口", self._opportunity_new_product),
            ("content_opportunity", "内容切口", self._opportunity_content),
            ("scene_opportunity", "场景切口", self._opportunity_scene),
            ("head_differentiation_opportunity", "头部差异化切口", self._opportunity_head),
            ("price_band_opportunity", "价格带切口", self._opportunity_price),
            ("supply_mismatch_opportunity", "供给错配切口", self._opportunity_supply_mismatch),
        ]
        diagnostics = []
        for code, label, builder in specs:
            diagnostics.append(builder(code, label, cards))
        return diagnostics

    def _opportunity_payload(self, code: str, label: str, cards: List[Dict[str, Any]], selected: List[Dict[str, Any]], evidence: List[Dict[str, Any]], business_meaning: str, next_action: str, not_recommended: str = "") -> Dict[str, Any]:
        return {
            "opportunity_type": code,
            "opportunity_label": label,
            "directions": [str(card.get("style_cluster") or card.get("direction_name") or "") for card in selected if str(card.get("style_cluster") or card.get("direction_name") or "") != "other"],
            "evidence": evidence,
            "business_meaning": business_meaning,
            "next_action": next_action,
            "not_recommended_action": not_recommended or "不建议按方向批量铺货",
        }

    def _opportunity_new_product(self, code: str, label: str, cards: List[Dict[str, Any]]) -> Dict[str, Any]:
        selected = [
            card for card in cards
            if str(card.get("style_cluster") or "") != "other"
            and (
                str((card.get("competition_structure") or {}).get("new_product_entry_level") or "") in {"新品进入强", "新品进入中等"}
                or str((card.get("competition_structure") or {}).get("competition_type") or "") == "few_new_winners"
            )
        ]
        selected = sorted(selected, key=lambda c: -float((c.get("product_age_structure") or {}).get("new_90d_sales_share") or 0.0))
        top = selected[0] if selected else {}
        age = dict(top.get("product_age_structure") or {})
        evidence = [
            {"metric": "近90天新品销量占比", "value": age.get("new_90d_sales_share"), "interpretation": "判断新品是否仍能跑出"},
            {"metric": "近90天新品数量占比", "value": age.get("new_90d_count_share"), "interpretation": "判断新品供给是否真实进入"},
            {"metric": "新品信号", "value": (top.get("competition_structure") or {}).get("new_product_entry_level"), "interpretation": "判断是整体新品强还是少数新品赢家"},
        ] if top else [{"metric": "新品结构", "value": "待补", "interpretation": "本批未识别出明确新品切口"}]
        meaning = "新品机会更适合作为代表新品拆解，不等于方向整体开放。"
        return self._opportunity_payload(code, label, cards, selected[:3], evidence, meaning, "生成近90天代表新品样本池", "不铺普通新品")

    def _opportunity_content(self, code: str, label: str, cards: List[Dict[str, Any]]) -> Dict[str, Any]:
        selected = [
            card for card in cards
            if str(card.get("style_cluster") or "") != "other"
            and (
                str((card.get("competition_structure") or {}).get("video_supply_level") or "") == "内容缺口"
                or str((card.get("competition_structure") or {}).get("creator_supply_level") or "") == "达人缺口"
            )
        ]
        selected = sorted(selected, key=lambda c: float(c.get("direction_video_density_avg") or 0.0))
        top = selected[0] if selected else {}
        evidence = [
            {"metric": "视频密度", "value": top.get("direction_video_density_avg"), "interpretation": "判断内容供给是否不足"},
            {"metric": "达人密度", "value": top.get("direction_creator_density_avg"), "interpretation": "判断达人覆盖是否不足"},
            {"metric": "7日销量中位数", "value": top.get("direction_sales_median_7d"), "interpretation": "判断内容缺口是否有成交基础"},
        ] if top else [{"metric": "内容密度", "value": "待补", "interpretation": "本批暂无强内容切口，需要补视频/达人效率数据"}]
        meaning = "内容机会必须同时看销量承接和内容密度，不能只因为视频少就进入。"
        return self._opportunity_payload(code, label, cards, selected[:3], evidence, meaning, "拆内容表达样本和低视频密度高销量样本", "不直接批量生成内容")

    def _opportunity_scene(self, code: str, label: str, cards: List[Dict[str, Any]]) -> Dict[str, Any]:
        selected = [
            card for card in cards
            if str(card.get("style_cluster") or "") != "other"
            and (card.get("scene_tags") or ((card.get("recommended_execution") or {}).get("differentiation_angles") or {}).get("scene_angle"))
        ]
        selected = sorted(selected, key=lambda c: str(c.get("business_priority") or "P9"))
        top = selected[0] if selected else {}
        angles = ((top.get("recommended_execution") or {}).get("differentiation_angles") or {}) if top else {}
        evidence = [
            {"metric": "方向场景标签", "value": "、".join(list(top.get("scene_tags") or [])[:5]), "interpretation": "判断是否有明确使用场景"},
            {"metric": "场景切口", "value": "；".join(list(angles.get("scene_angle") or [])[:3]), "interpretation": "判断是否能形成短视频第一句话"},
            {"metric": "核心价值点", "value": "、".join(list(top.get("top_value_points") or [])[:5]), "interpretation": "判断用户为什么买"},
        ] if top else [{"metric": "场景词", "value": "待补", "interpretation": "缺少明确场景证据"}]
        meaning = "场景机会要落到具体佩戴理由和内容首句，不是方向名字成立就成立。"
        return self._opportunity_payload(code, label, cards, selected[:3], evidence, meaning, "按场景关键词生成样本池", "不做泛审美内容")

    def _opportunity_head(self, code: str, label: str, cards: List[Dict[str, Any]]) -> Dict[str, Any]:
        selected = [
            card for card in cards
            if str(card.get("style_cluster") or "") != "other"
            and (
                str(card.get("decision_action") or "") == "study_top_not_enter"
                or str((card.get("competition_structure") or {}).get("competition_type") or "") in {"aesthetic_homogeneous", "few_new_winners", "old_product_dominated"}
            )
        ]
        selected = sorted(selected, key=lambda c: -float((c.get("demand_structure") or {}).get("top3_sales_share") or 0.0))
        top = selected[0] if selected else {}
        demand = dict(top.get("demand_structure") or {})
        evidence = [
            {"metric": "Top3 销量占比", "value": demand.get("top3_sales_share"), "interpretation": "判断是否为头部赢家结构"},
            {"metric": "均值/中位数", "value": demand.get("mean_median_ratio"), "interpretation": "判断销量分布是否偏斜"},
            {"metric": "动作", "value": display_enum(str(top.get("decision_action") or ""), "decision_action"), "interpretation": "判断是否进入头部拆解而非方向铺货"},
        ] if top else [{"metric": "Top10", "value": "待补", "interpretation": "缺少头部拆解对象"}]
        meaning = "头部差异化机会只回答哪些样本值得拆，不回答方向是否可铺货。"
        return self._opportunity_payload(code, label, cards, selected[:3], evidence, meaning, "生成 Top10 头部样本池并拆可复制/不可复制点", "不按方向铺普通款")

    def _opportunity_price(self, code: str, label: str, cards: List[Dict[str, Any]]) -> Dict[str, Any]:
        selected = [
            card for card in cards
            if str(card.get("style_cluster") or "") != "other"
            and str((card.get("competition_structure") or {}).get("price_band_signal") or "") == "价格带空隙"
        ]
        selected = sorted(selected, key=lambda c: str(c.get("business_priority") or "P9"))
        top = selected[0] if selected else {}
        price = ((top.get("price_band_analysis") or {}).get("recommended_price_band") or {}) if top else {}
        evidence = [
            {"metric": "推荐价格带", "value": price.get("rmb_range") or price.get("label"), "interpretation": "判断机会是否集中在某价格区间"},
            {"metric": "价格带样本数", "value": price.get("sample_count"), "interpretation": "判断价格结论是否可信"},
            {"metric": "价格带销量中位数", "value": price.get("median_sales_7d"), "interpretation": "判断该价格带是否有成交承接"},
        ] if top else [{"metric": "价格带", "value": "待补数据", "interpretation": "缺少方向 × 价格带 × 新品 × 视频密度交叉验证"}]
        meaning = "价格带机会需要和销量、新品、视频密度交叉验证，不能只看目标价格带标签。"
        return self._opportunity_payload(code, label, cards, selected[:3], evidence, meaning, "生成价格带交叉表和价格带代表样本", "不在价格样本不足时强行下结论")

    def _opportunity_supply_mismatch(self, code: str, label: str, cards: List[Dict[str, Any]]) -> Dict[str, Any]:
        selected = [
            card for card in cards
            if str(card.get("style_cluster") or "") != "other"
            and str((card.get("competition_structure") or {}).get("competition_type") or "") in {"supply_bubble", "old_product_dominated", "few_new_winners", "content_gap"}
        ]
        selected = sorted(selected, key=lambda c: str(c.get("business_priority") or "P9"))
        top = selected[0] if selected else {}
        structure = dict(top.get("competition_structure") or {})
        evidence = [
            {"metric": "错配类型", "value": self._competition_type_label(str(structure.get("competition_type") or "")), "interpretation": "识别供给和成交是否错位"},
            {"metric": "需求水平", "value": structure.get("demand_level"), "interpretation": "判断成交承接强弱"},
            {"metric": "内容供给", "value": structure.get("video_supply_level"), "interpretation": "判断内容供给是否过剩或不足"},
        ] if top else [{"metric": "供给错配", "value": "待补", "interpretation": "未识别出明确错配方向"}]
        meaning = "供给错配用于判断看起来热但不能做、或看起来不热但可拆样本的方向。"
        return self._opportunity_payload(code, label, cards, selected[:4], evidence, meaning, "按错配类型决定拆头部、拆新品或观察", "不把热度等同于机会")

    def _build_sample_pool_plan(self, cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        plans = []
        for card in cards:
            name = str(card.get("style_cluster") or card.get("direction_name") or "")
            action_info = self._direction_and_sample_action(card)
            if name == "other":
                plans.append(
                    {
                        "direction": name,
                        "priority": "P3",
                        "sample_pool_required": False,
                        "sample_groups": [
                            {"sample_type": "other_top_sales_sample", "limit": 10, "sort_by": "sales_desc"},
                            {"sample_type": "other_new_sample", "limit": 10, "filters": {"listing_age_days_lte": 90}, "sort_by": "sales_desc"},
                        ],
                        "purpose": "分类复核，不进入业务选品判断。",
                    }
                )
                continue
            if not action_info.get("sample_pool_required"):
                continue
            priority = str(action_info.get("priority") or "P3")
            if priority == "P1":
                groups = [
                    {"sample_type": "top_head_sample", "limit": 10, "sort_by": "sales_desc"},
                    {"sample_type": "new_product_sample", "limit": 10, "filters": {"listing_age_days_lte": 90}, "sort_by": "sales_desc"},
                    {"sample_type": "growth_low_video_sample", "limit": 5, "sort_by": "growth_desc_video_density_asc"},
                    {"sample_type": "structure_diff_sample", "limit": 5, "selection_method": "llm_or_keyword_cluster"},
                ]
            else:
                groups = [
                    {"sample_type": "top_head_sample", "limit": 5, "sort_by": "sales_desc"},
                    {"sample_type": "new_product_sample", "limit": 5, "filters": {"listing_age_days_lte": 90}, "sort_by": "sales_desc"},
                    {"sample_type": "structure_diff_sample", "limit": 3, "selection_method": "llm_or_keyword_cluster"},
                ]
            structure = dict(card.get("competition_structure") or {})
            if str(structure.get("price_band_signal") or "") == "价格带空隙":
                groups.append({"sample_type": "price_band_sample", "limit": 5, "sort_by": "price_band_sales_desc"})
            if card.get("scene_tags"):
                groups.append({"sample_type": "scene_keyword_sample", "limit": 5, "filters": {"scene_keywords": list(card.get("scene_tags") or [])[:5]}})
            plans.append(
                {
                    "direction": name,
                    "priority": priority,
                    "direction_action": action_info.get("direction_action"),
                    "sample_action": action_info.get("sample_action"),
                    "sample_pool_required": True,
                    "sample_groups": groups,
                    "max_deduped_samples": 25 if priority == "P1" else 12,
                    "hypothesis": self._sample_pool_hypothesis(card),
                }
            )
        return plans

    def _sample_pool_hypothesis(self, card: Dict[str, Any]) -> str:
        competition_type = str((card.get("competition_structure") or {}).get("competition_type") or "")
        mapping = {
            "few_new_winners": "验证少数新品赢家是靠产品结构、价格、内容还是达人跑出。",
            "content_gap": "验证是否存在低视频密度但成交强的内容切口。",
            "aesthetic_homogeneous": "验证头部是否具备普通款没有的视觉结构或佩戴理由。",
            "old_product_dominated": "验证老品头部是否存在可替代新品结构。",
            "price_band_gap": "验证目标价格带是否真实承接成交且可采购。",
            "supply_bubble": "验证是否只是内容/达人热闹但成交承接弱。",
        }
        return mapping.get(competition_type, "验证方向内 Top10 与代表新品是否存在可复制切口。")

    def full_report_renderer(self, payload: Dict[str, Any]) -> str:
        regime = dict(payload.get("market_regime_assessment") or {})
        lines = [
            "# 市场洞察报告",
            "",
            "## 零、批次格局判断",
            "当前判断：{value}".format(value=regime.get("regime_label", "待补判断")),
            "判断依据：{value}".format(value=regime.get("regime_reason", "待补判断依据")),
            "投入建议：{value}".format(value=regime.get("investment_advice", "待补投入建议")),
            "",
            "## 一、决策摘要 V1.3",
        ]
        lines.extend(self._render_decision_layer_summary(payload))
        lines.append("")
        decision_cards = list(payload.get("direction_decision_cards") or [])
        if decision_cards:
            lines.extend(["### 方向决策动作卡", ""])
            for item in decision_cards:
                lines.extend(
                    [
                        "#### {direction_name}".format(**item),
                        "方向名称：{direction_name}".format(**item),
                        "主机会类型：{primary_opportunity_type_label}".format(**item),
                        "风险标签：{risk_tags_text}".format(**item),
                        "建议动作：{decision_action_label}".format(**item),
                        "样本置信度：{sample_confidence}".format(**item),
                        "",
                    ]
                )
                lines.extend(self._render_action_decision_lines(item))
                lines.extend(
                    [
                        "",
                        "核心判断：{core_judgement}".format(**item),
                        "主要风险：{main_risk}".format(**item),
                        "",
                        "需求结构与集中度：",
                        "- 7日销量中位数：{median_sales_7d}".format(**item),
                        "- 7日销量均值：{mean_sales_7d}".format(**item),
                        "- 均值/中位数：{mean_median_ratio_text}".format(**item),
                        "- Top3 销量占比：{top3_sales_share_text}".format(**item),
                        "- 超过行动阈值商品占比：{over_threshold_item_ratio_text}".format(**item),
                        "- 集中度判断：{concentration_summary}".format(**item),
                        "",
                        "机会类型证据：",
                        "- 触发规则：{opportunity_rule_matched}".format(**item),
                        "- 命中指标：{opportunity_evidence_text}".format(**item),
                        "- 排除项：{opportunity_why_not_text}".format(**item),
                        "",
                        "建议测法：",
                        "- 测款数量：{test_sku_count}".format(**item),
                        "- 内容路线：{content_route}".format(**item),
                        "- 推荐价格带：{recommended_price_band_text}".format(**item),
                        "- 价格带置信度：{price_band_confidence}".format(**item),
                        "- 产品切口：{product_angles_text}".format(**item),
                        "- 场景切口：{scene_angles_text}".format(**item),
                        "- 内容切口：{content_angles_text}".format(**item),
                        "",
                        "我方能力匹配：",
                        "- AI 内容：{capability_ai_content}".format(**item),
                        "- 复刻能力：{capability_replication}".format(**item),
                        "- 原创演示：{capability_original_demo}".format(**item),
                        "- 本地场景化：{capability_scene_localization}".format(**item),
                        "- 供应链匹配：{capability_sourcing_fit}".format(**item),
                        "- 说明：{capability_rationale}".format(**item),
                        "",
                        "新品进入窗口：",
                        "- 原始新品信号：{raw_new_product_signal_text}".format(**item),
                        "- 可行动新品信号：{actionable_new_product_signal_text}".format(**item),
                        "- 上架时间有效样本数：{valid_age_sample_count}".format(**item),
                        "- 上架时间缺失率：{missing_age_rate}".format(**item),
                        "- 上架时间置信度：{age_confidence}".format(**item),
                        "- 近 30 天新品数量占比：{new_30d_count_share}".format(**item),
                        "- 近 30 天新品销量占比：{new_30d_sales_share}".format(**item),
                        "- 近 90 天新品数量占比：{new_90d_count_share}".format(**item),
                        "- 近 90 天新品销量占比：{new_90d_sales_share}".format(**item),
                        "- 180 天以上老品销量占比：{old_180d_sales_share}".format(**item),
                        "- 判断：{new_product_entry_rationale}".format(**item),
                        "",
                    ]
                )
                lines.extend(self._render_action_condition_section(item))
                missing_metrics = list((item.get("alert") or {}).get("missing_metrics") or [])
                if missing_metrics and item.get("decision_action") not in {"observe", "avoid"}:
                    lines.append("")
                    lines.append("当前缺失指标：{value}".format(value=self._metric_list_text(missing_metrics)))
                lines.append("")
        watch_rows = list(payload.get("watch_direction_table") or [])
        if watch_rows:
            lines.extend(
                [
                    "### 建议观察方向简表",
                    "",
                    "| 方向 | 当前动作 | 观察原因 | 当前关键信号 | 下一批转入条件 | 不可转入条件 |",
                    "|---|---|---|---|---|---|",
                ]
            )
            for row in watch_rows:
                lines.append(
                    "| {direction_name} | {current_action} | {observe_reason} | {current_signal} | {action_condition} | {block_condition} |".format(**row)
                )
            lines.append("")
        lines.append("## 二、方向对比矩阵")

        matrix = payload.get("direction_matrix", {})
        lines.append("")
        lines.extend(matrix.get("display_lines") or matrix.get("table_lines", []))
        lines.extend(["", "结构观察："])
        observations = matrix.get("observations", [])
        if observations:
            for text in observations:
                lines.append("- {text}".format(text=text))
        else:
            lines.append("- 当前矩阵未发现明显偏斜结构。")
        lines.append("")

        reverse_signals = payload.get("reverse_signals", {})
        lines.extend(["## 三、反向信号", "", "### 表面光鲜但暗藏风险"])
        hidden_risks = reverse_signals.get("hidden_risks", [])
        if hidden_risks:
            for item in hidden_risks:
                lines.append("- {direction_name}：{summary}".format(**item))
        else:
            lines.append("当前批次未识别出明显的表面繁荣型风险方向。")
        lines.extend(["", "### 被低估的暗线机会"])
        hidden_opportunities = reverse_signals.get("hidden_opportunities", [])
        if hidden_opportunities:
            for item in hidden_opportunities:
                lines.append("- {direction_name}：{summary}".format(**item))
        else:
            lines.append("当前批次未识别出明确的低估机会方向。")
        lines.append("")

        cross_system = payload.get("cross_system_recommendations", {})
        lines.extend(["## 四、跨系统联动建议", "", "### 复刻流程资源建议"])
        for item in cross_system.get("content_route_recommendations", []):
            lines.append("- {direction_name}：{suggestion}".format(**item))
        if not cross_system.get("content_route_recommendations"):
            lines.append("- 当前无额外内容路线建议。")
        lines.extend(["", "### 选品评分权重建议"])
        for item in cross_system.get("scoring_weight_recommendations", []):
            lines.append("- {direction_family}：{suggestion}".format(**item))
        if not cross_system.get("scoring_weight_recommendations"):
            lines.append("- 当前无额外评分权重建议。")
        lines.append("")
        consistency_warnings = list(payload.get("consistency_warnings") or [])
        consistency_errors = list(payload.get("consistency_errors") or [])
        if consistency_errors:
            lines.extend(["附：阻塞性自洽性问题"])
            for warning in consistency_errors:
                lines.append("- {warning}".format(warning=warning))
            lines.append("")
        if consistency_warnings:
            lines.extend(["附：数据自洽性提示"])
            for warning in consistency_warnings:
                lines.append("- {warning}".format(warning=warning))
            lines.append("")
        lines.extend(
            [
                "## 附录：新旧结论差异对账",
                "说明：旧版三桶结果仅用于算法对账，不作为最终动作建议。最终动作以“决策摘要 V1.3”为准。",
                "",
                "| 方向名称 | 旧版结论 | 新版动作 | 变化原因 |",
                "|---|---|---|---|",
            ]
        )
        lines.extend(self._render_legacy_diff_rows(decision_cards))
        lines.append("")
        lines.extend(
            [
                "说明：方向卡只用于判断“哪个方向值得进入”，不用于替代单品评分。",
                "说明：单品层的材质、版型、价格带适配性，需要由后续单品评分系统承接。",
                "",
            ]
        )
        return "\n".join(lines).strip() + "\n"

    def _build_category_baselines(self, cards: List[Dict[str, Any]]) -> Dict[str, Any]:
        metric_map = {
            "item_count": "direction_item_count",
            "video_density": "direction_video_density_avg",
            "creator_density": "direction_creator_density_avg",
            "sales_median": "direction_sales_median_7d",
        }
        baselines: Dict[str, Any] = {}
        for output_key, source_key in metric_map.items():
            values = [self._safe_float(card.get(source_key)) for card in cards]
            values = [value for value in values if value is not None]
            baselines.update(self._percentile_bundle(output_key, values))
        new90_values = []
        old180_values = []
        for card in cards:
            age = dict(card.get("product_age_structure") or {})
            new90 = self._safe_float(age.get("new_90d_sales_share"))
            old180 = self._safe_float(age.get("old_180d_sales_share"))
            if new90 is not None:
                new90_values.append(new90)
            if old180 is not None:
                old180_values.append(old180)
        baselines.update(self._percentile_bundle("new_90d_sales_share", new90_values))
        baselines.update(self._percentile_bundle("old_180d_sales_share", old180_values))
        return baselines

    def _percentile_bundle(self, name: str, values: List[float]) -> Dict[str, float]:
        return {
            f"{name}_p25": self._percentile(values, 25),
            f"{name}_p50": self._percentile(values, 50),
            f"{name}_p75": self._percentile(values, 75),
            f"{name}_p90": self._percentile(values, 90),
        }

    def _percentile(self, values: List[float], percentile: int) -> float:
        cleaned = sorted(float(value) for value in values if value is not None)
        if not cleaned:
            return 0.0
        if len(cleaned) == 1:
            return cleaned[0]
        position = (len(cleaned) - 1) * (float(percentile) / 100.0)
        lower = int(position)
        upper = min(lower + 1, len(cleaned) - 1)
        weight = position - lower
        return round(cleaned[lower] * (1.0 - weight) + cleaned[upper] * weight, 6)

    def _safe_float(self, value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _apply_competition_structure_v14(self, cards: List[Dict[str, Any]], baselines: Dict[str, Any]) -> None:
        for card in cards:
            name = str(card.get("style_cluster") or card.get("direction_name") or "")
            item_count = self._safe_float(card.get("direction_item_count")) or 0.0
            video_density = self._safe_float(card.get("direction_video_density_avg")) or 0.0
            creator_density = self._safe_float(card.get("direction_creator_density_avg")) or 0.0
            sales_median = self._safe_float(card.get("direction_sales_median_7d")) or 0.0
            age = dict(card.get("product_age_structure") or {})
            new90_sales = self._safe_float(age.get("new_90d_sales_share")) or 0.0
            old180_sales = self._safe_float(age.get("old_180d_sales_share")) or 0.0
            price_band = dict(card.get("price_band_analysis") or {}).get("recommended_price_band") or {}
            if not isinstance(price_band, dict):
                price_band = {}

            item_supply_level = "低样本" if item_count < max(3.0, float(baselines.get("item_count_p25") or 0.0)) else ("商品供给拥挤" if item_count >= float(baselines.get("item_count_p75") or 0.0) else "正常")
            video_supply_level = "内容拥挤" if video_density >= float(baselines.get("video_density_p75") or 0.0) else ("内容缺口" if video_density <= float(baselines.get("video_density_p25") or 0.0) and sales_median >= float(baselines.get("sales_median_p50") or 0.0) else "正常")
            creator_supply_level = "达人拥挤" if creator_density >= float(baselines.get("creator_density_p75") or 0.0) else ("达人缺口" if creator_density <= float(baselines.get("creator_density_p25") or 0.0) and sales_median >= float(baselines.get("sales_median_p50") or 0.0) else "正常")
            if sales_median >= float(baselines.get("sales_median_p75") or 0.0):
                demand_level = "成交强"
            elif sales_median >= float(baselines.get("sales_median_p50") or 0.0):
                demand_level = "成交中等"
            else:
                demand_level = "成交弱"
            if old180_sales >= float(baselines.get("old_180d_sales_share_p75") or 0.0) and old180_sales > 0:
                new_product_entry_level = "老品占位"
            elif new90_sales >= float(baselines.get("new_90d_sales_share_p75") or 0.0) and new90_sales > 0:
                new_product_entry_level = "新品进入强"
            elif new90_sales >= float(baselines.get("new_90d_sales_share_p50") or 0.0) and new90_sales > 0:
                new_product_entry_level = "新品进入中等"
            else:
                new_product_entry_level = "新品进入弱"
            old_product_dominance_level = "老品占位强" if old180_sales >= float(baselines.get("old_180d_sales_share_p75") or 0.0) and old180_sales > 0 else ("老品占位中" if old180_sales >= 0.5 else "老品占位弱")
            price_confidence = str(price_band.get("confidence") or "insufficient")
            sample_count = self._safe_float(price_band.get("sample_count")) or 0.0
            if price_confidence in {"insufficient", "low"} or sample_count < 3:
                price_band_signal = "样本不足"
            elif demand_level in {"成交强", "成交中等"} and item_supply_level != "商品供给拥挤":
                price_band_signal = "价格带空隙"
            else:
                price_band_signal = "价格带拥挤"

            competition_type = self._resolve_competition_type_v14(
                name=name,
                item_supply_level=item_supply_level,
                video_supply_level=video_supply_level,
                creator_supply_level=creator_supply_level,
                demand_level=demand_level,
                new_product_entry_level=new_product_entry_level,
                old_product_dominance_level=old_product_dominance_level,
                price_band_signal=price_band_signal,
                card=card,
            )
            structure = dict(card.get("competition_structure") or {})
            structure.update(
                {
                    "item_supply_level": item_supply_level,
                    "video_supply_level": video_supply_level,
                    "creator_supply_level": creator_supply_level,
                    "demand_level": demand_level,
                    "new_product_entry_level": new_product_entry_level,
                    "old_product_dominance_level": old_product_dominance_level,
                    "price_band_signal": price_band_signal,
                    "competition_type": competition_type,
                    "opportunity_interpretation": self._competition_interpretation_v14(competition_type),
                    "category_baseline_scope": "market_id+category_id",
                }
            )
            card["competition_structure"] = structure
            card["business_priority"] = self._business_priority_for_card(card)
            card["business_next_step"] = self._business_next_step_for_card(card, structure)
            card["business_action_label"] = self._business_action_label_for_card(card)
            if name == "other":
                card["primary_opportunity_type"] = "insufficient_info"
                card["primary_opportunity_type_label"] = "信息不足型 / 方向待归类"
                card["decision_action"] = "observe"
                card["decision_action_label"] = DECISION_ACTION_LABELS.get("observe", "持续观察")
                card["business_priority"] = "P3"
                card["business_next_step"] = "重新归类"
            self._apply_default_angles_v14(card)

    def _resolve_competition_type_v14(self, name: str, item_supply_level: str, video_supply_level: str, creator_supply_level: str, demand_level: str, new_product_entry_level: str, old_product_dominance_level: str, price_band_signal: str, card: Dict[str, Any]) -> str:
        if name == "other":
            return "unclear_structure"
        risk_tags = set(str(tag) for tag in list(card.get("risk_tags") or []))
        family = str(card.get("direction_family") or "")
        if old_product_dominance_level == "老品占位强":
            return "old_product_dominated"
        if new_product_entry_level in {"新品进入强", "新品进入中等"} and demand_level in {"成交强", "成交中等"}:
            return "few_new_winners"
        if video_supply_level == "内容缺口" or creator_supply_level == "达人缺口":
            return "content_gap"
        if demand_level == "成交弱" and (video_supply_level == "内容拥挤" or creator_supply_level == "达人拥挤"):
            return "supply_bubble"
        if price_band_signal == "价格带空隙":
            return "price_band_gap"
        if family in {"审美风格型", "礼物氛围型", "质感气质型", "拍照出片型", "日常通勤型"} or "aesthetic_homogeneous" in risk_tags:
            return "aesthetic_homogeneous"
        if item_supply_level == "低样本" or demand_level == "成交弱":
            return "weak_signal"
        return "unclear_structure"

    def _competition_interpretation_v14(self, competition_type: str) -> str:
        mapping = {
            "old_product_dominated": "老品占位强，不能因为有销量就直接铺货，先拆头部和新品替代空间。",
            "few_new_winners": "少数新品能跑出，但不是方向整体开放，优先看代表新品和头部胜出机制。",
            "content_gap": "成交不弱但内容或达人覆盖偏低，可优先核验内容切口。",
            "supply_bubble": "内容/达人供给偏多但成交承接弱，暂不进入。",
            "aesthetic_homogeneous": "审美表达容易同质化，只拆头部差异，不铺普通款。",
            "price_band_gap": "存在价格带空隙，先核验目标价格带的可采购性和内容承接。",
            "local_scene_gap": "本地场景表达不足，优先补本地关键词和场景内容。",
            "weak_signal": "样本或成交信号偏弱，先观察不占资源。",
            "unclear_structure": "结构不清晰，先补样本或重新归类。",
        }
        return mapping.get(competition_type, "结构待补充解释。")

    def _apply_default_angles_v14(self, card: Dict[str, Any]) -> None:
        name = str(card.get("style_cluster") or card.get("direction_name") or "")
        category = str(card.get("category") or card.get("category_id") or "")
        if category != "earrings" or name == "other":
            return
        defaults = {
            "无耳洞友好型": {
                "product_angle": ["无耳洞也能戴", "耳夹不痛", "佩戴不易掉"],
                "scene_angle": ["临时拍照", "学生党", "没打耳洞也想有耳饰细节"],
                "content_angle": ["夹上前后对比", "轻晃不掉落", "佩戴动作展示"],
            },
            "珍珠锆石气质型": {
                "product_angle": ["珍珠 / 锆石微闪", "亮但不俗", "提升通勤质感"],
                "scene_angle": ["通勤", "约会", "晚上出门"],
                "content_angle": ["侧脸近景", "转头微闪", "普通上衣搭配前后对比"],
            },
            "高存在感拍照型": {
                "product_angle": ["一眼能看到", "有造型感", "拍照出片"],
                "scene_angle": ["周末出门", "拍照", "旅行"],
                "content_angle": ["首镜侧脸强视觉", "转头展示耳环存在感", "简单衣服 + 高存在感耳环对比"],
            },
            "幸运寓意型": {
                "product_angle": ["四叶草 / 爱心 / 星月符号", "小寓意", "礼物感"],
                "scene_angle": ["自戴", "小礼物", "学生互送"],
                "content_angle": ["近景符号展示", "轻口播解释寓意", "日常出门前戴上"],
            },
            "脸型修饰显瘦型": {
                "product_angle": ["拉长脸部线条", "显脸小", "侧脸更有线条"],
                "scene_angle": ["拍照", "约会", "出门搭配"],
                "content_angle": ["正脸 / 侧脸对比", "头发拨到耳后展示", "耳线长度和脸型变化"],
            },
            "韩系轻通勤型": {
                "product_angle": ["低调但有细节", "适合学生/上班通勤", "不夸张"],
                "scene_angle": ["上学前出门", "办公室", "教室日常"],
                "content_angle": ["普通穿搭加耳饰后更完整", "近景展示低调细节", "远景展示整体搭配"],
            },
            "少女礼物感型": {
                "product_angle": ["礼物感强", "小巧但有记忆点", "不廉价幼稚"],
                "scene_angle": ["学生互送", "约会前出门", "节日小礼物"],
                "content_angle": ["包装 / 上耳 / 拍照出片三段式", "近景展示细节", "送礼场景口播"],
            },
        }
        if name not in defaults:
            return
        recommended = dict(card.get("recommended_execution") or {})
        angles = recommended.get("differentiation_angles") or {}
        if not isinstance(angles, dict):
            angles = {}
        for key, values in defaults[name].items():
            current = [str(item).strip() for item in list(angles.get(key) or []) if str(item).strip()]
            if not current or self._angle_text(current) == "待补具体切口" or any("待人工确认" in item for item in current):
                angles[key] = values
        recommended["differentiation_angles"] = angles
        card["recommended_execution"] = recommended

    def _business_priority_for_card(self, card: Dict[str, Any]) -> str:
        name = str(card.get("style_cluster") or card.get("direction_name") or "")
        action = str(card.get("decision_action") or "")
        sample_confidence = str(card.get("sample_confidence") or "")
        if name == "other":
            return "P3"
        if action in {"prioritize_low_cost_test", "cautious_test"}:
            return "P0"
        if action in {"hidden_small_test", "strong_signal_verify", "hidden_candidate"}:
            return "P1"
        if action == "study_top_not_enter":
            return "P1"
        if sample_confidence in {"low", "insufficient"}:
            return "P3"
        return "P2" if action == "observe" else "P3"

    def _business_action_label_for_card(self, card: Dict[str, Any]) -> str:
        action = str(card.get("decision_action") or "")
        if action in {"prioritize_low_cost_test", "cautious_test", "hidden_small_test"}:
            return DECISION_ACTION_LABELS.get(action, action)
        if action in {"strong_signal_verify", "hidden_candidate"}:
            return "优先样本核验"
        if action == "study_top_not_enter":
            return "拆头部"
        if str(card.get("style_cluster") or "") == "other":
            return "归类待补"
        return "观察"

    def _business_next_step_for_card(self, card: Dict[str, Any], structure: Dict[str, Any]) -> str:
        name = str(card.get("style_cluster") or card.get("direction_name") or "")
        if name == "other":
            return "重新归类"
        action = str(card.get("decision_action") or "")
        ctype = str(structure.get("competition_type") or "")
        if action in {"strong_signal_verify", "hidden_candidate"} or ctype in {"few_new_winners", "content_gap", "price_band_gap"}:
            return "看 Top10 + 代表新品"
        if action == "study_top_not_enter" or ctype == "aesthetic_homogeneous":
            return "拆头部，不铺货"
        if str(card.get("sample_confidence") or "") in {"low", "insufficient"}:
            return "等样本"
        if ctype == "old_product_dominated":
            return "拆头部老品和可替代新品"
        return "持续观察下一批信号"

    def _business_batch_info(self, payload: Dict[str, Any]) -> List[str]:
        diagnostics = dict(payload.get("report_diagnostics") or {})
        progress = dict(payload.get("progress") or {})
        cards = list(payload.get("direction_decision_cards") or [])
        card_sample_total = sum(int(card.get("direction_item_count") or 0) for card in cards)
        sample_total = diagnostics.get("total_sample_count") or progress.get("total_product_count") or progress.get("sample_count") or ("方向有效样本合计 {count}".format(count=card_sample_total) if card_sample_total else "见运行进度")
        valid_count = diagnostics.get("valid_sample_count") or progress.get("valid_sample_count") or (card_sample_total if card_sample_total else "见运行进度")
        valid_ratio = diagnostics.get("valid_sample_ratio") if diagnostics.get("valid_sample_ratio") is not None else progress.get("valid_sample_ratio")
        valid_ratio_text = self._format_percent_metric(valid_ratio) if valid_ratio not in {None, ""} else "见运行进度"
        status = diagnostics.get("report_status") or ("可消费" if not payload.get("consistency_errors") else "需复核")
        return [
            "- 市场：{value}".format(value=payload.get("country", "")),
            "- 类目：{value}".format(value=payload.get("category", "")),
            "- 批次日期：{value}".format(value=payload.get("batch_date", "")),
            "- 样本：{value}".format(value=sample_total),
            "- 有效样本：{value}".format(value=valid_count),
            "- 有效率：{value}".format(value=valid_ratio_text),
            "- 方向数：{value}".format(value=len(cards)),
            "- other占比：{value}".format(value=self._format_percent_metric(diagnostics.get("other_ratio"))),
            "- source_scope：{value}".format(value=diagnostics.get("source_scope") or payload.get("source_scope") or "unknown"),
            "- 报告状态：{value}".format(value=status),
            "- 样本覆盖等级：{value}".format(value=diagnostics.get("sample_coverage_level") or "待补"),
            "- 方向分布稳定性：{value}".format(value=diagnostics.get("direction_distribution_stability") or "待补"),
            "- 报告置信度：{value}".format(value=diagnostics.get("report_confidence") or payload.get("report_confidence") or "待补"),
            "- 降级原因：{value}".format(value="；".join(list(diagnostics.get("confidence_reasons") or [])) or "无"),
        ]

    def _business_one_line_conclusion(self, payload: Dict[str, Any]) -> str:
        cards = list(payload.get("direction_decision_cards") or [])
        direction_entry = [c for c in cards if str(c.get("direction_action") or "") in {"方向级优先进入", "方向级谨慎进入"}]
        sample_level = [c for c in cards if str(c.get("sample_action") or "").startswith("样本级") and str(c.get("sample_action") or "") != "样本级观察储备"]
        reserve = [c for c in cards if str(c.get("sample_action") or "") == "样本级观察储备"]
        if not direction_entry and sample_level:
            names = "、".join(str(c.get("direction_name") or "") for c in sample_level[:3])
            return "本批暂无方向级批量进入机会，但可以从 {names} 做样本级拆解，重点验证头部赢家和代表新品是否存在可复制切口。".format(names=names)
        if direction_entry:
            names = "、".join(str(c.get("direction_name") or "") for c in direction_entry[:3])
            return "本批存在少量方向级谨慎进入窗口（{names}），但仍必须绑定样本池、止损条件和人工核验，不建议大类铺货。".format(names=names)
        if reserve:
            return "本批没有明确测品方向，适合先建立样本池观察储备，等待新品、场景或头部差异化证据增强。"
        return "本批暂无直接测品方向，整体以观察和重新归类为主，不建议占用批量内容与上新资源。"

    def _business_action_summary_rows(self, cards: List[Dict[str, Any]]) -> List[str]:
        buckets = [
            ("方向级", "优先进入", lambda c: str(c.get("direction_action") or "") == "方向级优先进入", "可按方向进入，但当前仍需样本池和止损条件约束"),
            ("方向级", "谨慎进入", lambda c: str(c.get("direction_action") or "") == "方向级谨慎进入", "只允许小批量验证，不做类目整体铺货"),
            ("方向级", "观察", lambda c: str(c.get("direction_action") or "") == "方向级观察", "方向层面不铺货，转入样本级判断"),
            ("方向级", "暂缓", lambda c: str(c.get("direction_action") or "") == "方向级暂缓", "当前不进入方向级投入"),
            ("样本级", "低成本验证", lambda c: str(c.get("sample_action") or "") == "样本级低成本验证", "只从 Top10 + 代表新品中挑少量样本验证"),
            ("样本级", "拆头部", lambda c: str(c.get("sample_action") or "") == "样本级拆头部", "看头部为什么赢，暂不铺普通款"),
            ("样本级", "观察储备", lambda c: str(c.get("sample_action") or "") == "样本级观察储备", "不占用测款资源，仅观察下一批信号"),
            ("归类处理", "重新归类", lambda c: str(c.get("direction_action") or "") == "方向级归类待补", "不参与业务机会判断"),
        ]
        rows = []
        used = set()
        for layer, label, predicate, meaning in buckets:
            names = [str(card.get("direction_name") or "") for card in cards if predicate(card)]
            names = [name for name in names if name and name not in used]
            if layer != "方向级":
                used.update(names)
            rows.extend([
                "",
                "### {layer}｜{label}".format(layer=layer, label=label),
                "- 方向：{names}".format(names="、".join(names[:5]) if names else "无"),
                "- 业务含义：{meaning}".format(meaning=meaning),
            ])
        return rows

    def _business_opportunity_rows(self, cards: List[Dict[str, Any]], diagnostics: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        rows = []
        for item in diagnostics:
            directions = list(item.get("directions") or [])
            evidence_items = list(item.get("evidence") or [])
            evidence_parts = []
            for evidence in evidence_items[:3]:
                metric = str(evidence.get("metric") or "指标")
                value = self._format_evidence_value(evidence.get("value"))
                interpretation = str(evidence.get("interpretation") or "")
                evidence_parts.append("{metric}：{value}（{interpretation}）".format(metric=metric, value=value, interpretation=interpretation))
            rows.append(
                {
                    "opportunity_type": str(item.get("opportunity_label") or item.get("opportunity_type") or ""),
                    "directions": "、".join(directions[:4]) if directions else "待补数据",
                    "evidence": "；".join(evidence_parts) if evidence_parts else "待补数据",
                    "next_step": str(item.get("next_action") or "待补下一步"),
                }
            )
        return rows

    def _business_evidence_text(self, card: Dict[str, Any]) -> str:
        structure = dict(card.get("competition_structure") or {})
        age = dict(card.get("product_age_structure") or {})
        parts = [
            str(structure.get("opportunity_interpretation") or "结构信号待补"),
            "近90天新品销量占比 {value}".format(value=self._format_percent_metric(age.get("new_90d_sales_share"))),
            "视频密度 {value}".format(value=self._format_metric(card.get("direction_video_density_avg"))),
        ]
        return "；".join(parts[:3])

    def _business_direction_rows(self, cards: List[Dict[str, Any]], traces: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        trace_by_name = {str(item.get("direction") or ""): item for item in traces}
        rows = []
        sorted_cards = sorted(cards, key=lambda c: (str(c.get("business_priority") or "P9"), str(c.get("direction_name") or "")))
        for card in sorted_cards:
            structure = dict(card.get("competition_structure") or {})
            name = str(card.get("direction_name") or "")
            trace = dict(trace_by_name.get(name) or {})
            questions = list(trace.get("verification_questions") or [])
            rows.append({
                "direction_name": name,
                "direction_action": str(card.get("direction_action") or trace.get("direction_action") or "方向级观察"),
                "sample_action": str(card.get("sample_action") or trace.get("sample_action") or "样本级观察储备"),
                "competition_structure": self._competition_type_label(str(structure.get("competition_type") or "unclear_structure")),
                "new_signal": str(structure.get("new_product_entry_level") or card.get("actionable_new_product_signal_text") or "待补"),
                "opportunity_type": str(card.get("primary_opportunity_type_label") or card.get("primary_opportunity_type") or "待补"),
                "business_priority": str(card.get("business_priority") or "P3"),
                "verification_question": questions[0] if questions else "待补核验问题",
                "next_step": str(card.get("business_next_step") or "持续观察"),
                "sample_pool_required": "是" if bool(card.get("sample_pool_required") or trace.get("sample_pool_required")) else "否",
            })
        return rows

    def _competition_type_label(self, value: str) -> str:
        labels = {
            "old_product_dominated": "老品占位型",
            "few_new_winners": "少数新品赢家型",
            "content_gap": "内容缺口型",
            "supply_bubble": "供给泡沫型",
            "aesthetic_homogeneous": "审美同质型",
            "price_band_gap": "价格带空隙型",
            "local_scene_gap": "本地场景缺口型",
            "weak_signal": "弱信号型",
            "unclear_structure": "结构不清晰",
        }
        return labels.get(value, value or "结构不清晰")

    def _business_focus_cards(self, cards: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        selected = [card for card in cards if str(card.get("business_priority") or "") in {"P0", "P1", "P2"} and str(card.get("direction_name") or "") != "other"]
        if len(selected) < 3:
            selected.extend([card for card in cards if str(card.get("business_priority") or "") == "P2"])
        result = []
        for card in selected[:5]:
            structure = dict(card.get("competition_structure") or {})
            trace_questions = self._verification_questions(card)
            sample_requirement = self._sample_requirement_text(card)
            override_text = ""
            override = dict(card.get("action_override") or {})
            if override.get("is_overridden"):
                rule_text = " ".join(
                    item for item in [
                        str(override.get("override_rule_id") or "").strip(),
                        str(override.get("override_rule_name") or "").strip(),
                    ] if item
                )
                evidence_text = self._action_override_evidence_text(list(override.get("override_evidence") or []))
                override_text = "动作覆盖说明：\n- 默认动作：{from_action}\n- 最终动作：{to_action}\n- 覆盖规则：{rule}\n- 覆盖原因：{reason}\n- 关键证据：{evidence}".format(
                    from_action=DECISION_ACTION_LABELS.get(str(override.get("from_action") or ""), str(override.get("from_action") or "待补")),
                    to_action=DECISION_ACTION_LABELS.get(str(override.get("to_action") or ""), str(override.get("to_action") or "待补")),
                    rule=rule_text or "覆盖规则",
                    reason=str(override.get("override_reason") or "触发动作覆盖规则。"),
                    evidence=evidence_text,
                )
            result.append({
                "direction_name": str(card.get("direction_name") or ""),
                "current_action": str(card.get("business_action_label") or card.get("decision_action_label") or "观察"),
                "action_override_text": override_text,
                "why_watch": str(structure.get("opportunity_interpretation") or self._decision_core_judgement(card))[:160],
                "why_not_direct": self._why_not_direct_text(card),
                "opportunity_types": self._focus_opportunity_types(card),
                "core_risk": self._decision_main_risk(card),
                "verification_questions": "；".join(trace_questions[:3]),
                "sample_pool_requirement": sample_requirement,
                "pass_conditions": self._pass_conditions_text(card),
                "fail_conditions": self._fail_conditions_text(card),
                "next_step": str(card.get("business_next_step") or "看样本池"),
            })
        return result

    def _focus_opportunity_types(self, card: Dict[str, Any]) -> str:
        ctype = str((card.get("competition_structure") or {}).get("competition_type") or "")
        mapping = {
            "few_new_winners": ["新品切口", "头部差异化切口"],
            "content_gap": ["内容切口", "头部差异化切口"],
            "aesthetic_homogeneous": ["头部差异化切口", "场景切口"],
            "old_product_dominated": ["头部差异化切口", "供给错配切口"],
            "price_band_gap": ["价格带切口", "样本级验证"],
            "supply_bubble": ["供给错配切口"],
        }
        values = mapping.get(ctype, ["样本级观察"])
        if card.get("scene_tags"):
            values.append("场景切口")
        return "、".join(dict.fromkeys(values))

    def _why_not_direct_text(self, card: Dict[str, Any]) -> str:
        action = str(card.get("decision_action") or "")
        ctype = str((card.get("competition_structure") or {}).get("competition_type") or "")
        if action == "study_top_not_enter":
            return "当前只说明头部或少数样本值得拆，不代表方向整体开放。"
        if action in {"strong_signal_verify", "hidden_candidate", "observe"}:
            return "样本、价格带、供应链或新品窗口仍需核验，不能直接方向铺货。"
        if ctype in {"aesthetic_homogeneous", "few_new_winners"}:
            return "普通款同质化风险高，必须先看 Top10 与代表新品是否有可复制差异。"
        if action in {"prioritize_low_cost_test", "cautious_test", "hidden_small_test"}:
            return "即使进入验证，也只允许小样本，不代表批量铺货。"
        return "当前证据不足以支持方向级批量进入。"

    def _sample_requirement_text(self, card: Dict[str, Any]) -> str:
        priority = str(card.get("business_priority") or "P3")
        if priority in {"P0", "P1"}:
            return "Top10 10个；近90天代表新品10个；增长快但视频密度低样本5个；差异化结构样本5个。"
        if priority == "P2":
            return "Top10 5个；近90天代表新品5个；差异化结构样本3个。"
        return "仅生成分类复核样本，不进入业务样本池。"

    def _pass_conditions_text(self, card: Dict[str, Any]) -> str:
        action = str(card.get("decision_action") or "")
        if action == "study_top_not_enter":
            return "至少拆出2个可迁移产品共性和2个可迁移内容结构；确认头部商品可采购。"
        if action in {"prioritize_low_cost_test", "cautious_test", "hidden_small_test"}:
            return "至少找到3个非同质化样本；至少2个样本具备明确短视频第一句话；成本满足目标毛利。"
        if action in {"strong_signal_verify", "hidden_candidate", "observe"}:
            return "样本置信度、价格带置信度或可行动新品信号提升到中等以上；供应链匹配不低于中。"
        return "补齐样本和字段后再判断。"

    def _fail_conditions_text(self, card: Dict[str, Any]) -> str:
        action = str(card.get("decision_action") or "")
        if action == "study_top_not_enter":
            return "头部依赖老链接、达人强背书或不可采购；缺少可复制共性。"
        if action in {"prioritize_low_cost_test", "cautious_test", "hidden_small_test"}:
            return "样本主要靠包装/达人/低价，产品结构不可复制；没有明确场景或第一句话。"
        return "下一批仍缺少新品、场景、价格带或供应链证据。"

    def _render_sample_pool_plan_summary(self, plans: List[Dict[str, Any]]) -> List[str]:
        if not plans:
            return ["本批暂无需要生成的业务样本池。"]
        lines = []
        for plan in plans:
            if not plan.get("sample_pool_required") and str(plan.get("direction") or "") != "other":
                continue
            groups = []
            for group in list(plan.get("sample_groups") or [])[:5]:
                groups.append("{sample_type}×{limit}".format(sample_type=str(group.get("sample_type") or ""), limit=str(group.get("limit") or "")))
            lines.extend([
                "",
                "### {direction}".format(direction=str(plan.get("direction") or "")),
                "- 优先级：{priority}".format(priority=str(plan.get("priority") or "")),
                "- 样本池动作：{sample_action}".format(sample_action=str(plan.get("sample_action") or ("分类复核" if str(plan.get("direction") or "") == "other" else "样本池"))),
                "- 样本组：{groups}".format(groups="、".join(groups) if groups else "待补"),
                "- 待验证假设：{hypothesis}".format(hypothesis=str(plan.get("hypothesis") or plan.get("purpose") or "")),
            ])
        return lines

    def _business_execution_rows(self, cards: List[Dict[str, Any]], category: str) -> List[Dict[str, str]]:
        rows = []
        p1_names = [str(card.get("direction_name") or "") for card in cards if str(card.get("business_priority") or "") == "P1" and str(card.get("direction_name") or "") != "other"]
        top_names = [str(card.get("direction_name") or "") for card in cards if str(card.get("decision_action") or "") == "study_top_not_enter"]
        low_sample = [str(card.get("direction_name") or "") for card in cards if str(card.get("sample_confidence") or "") in {"low", "insufficient"}]
        if p1_names:
            rows.append({"priority": "P0", "action": "生成方向样本商品池", "directions": "、".join(p1_names[:5]), "output": "Top10 + 代表新品"})
        if top_names:
            rows.append({"priority": "P1", "action": "拆头部样本", "directions": "、".join(top_names[:5]), "output": "头部胜出机制"})
        if str(category or "") == "earrings":
            rows.append({"priority": "P1", "action": "核验耳环产品/场景/内容切口", "directions": "所有耳环方向（除 other）", "output": "可执行切口清单"})
        rows.append({"priority": "P2", "action": "补本地关键词", "directions": "{category}".format(category=category), "output": "本地语言关键词与内容表达"})
        if low_sample:
            rows.append({"priority": "P2", "action": "等待补样本", "directions": "、".join(low_sample[:5]), "output": "下批观察"})
        if not rows:
            rows.append({"priority": "P3", "action": "持续观察", "directions": "全部方向", "output": "下批信号复核"})
        return rows

    def _resolve_family_order(self, category: str, cards: List[Dict[str, Any]]) -> List[str]:
        normalized_category = str(category or "").strip()
        configured = self.config.get("family_order_by_category", {}) or {}
        category_order = configured.get(normalized_category)
        if isinstance(category_order, list) and category_order:
            order = [str(item or "").strip() for item in category_order if str(item or "").strip()]
        else:
            order = list(DEFAULT_FAMILY_ORDER_BY_CATEGORY.get(normalized_category, DEFAULT_FAMILY_ORDER_BY_CATEGORY["hair_accessory"]))
        seen = set(order)
        for card in cards:
            family = str(card.get("direction_family") or "other").strip() or "other"
            if family not in seen:
                order.append(family)
                seen.add(family)
        if "other" not in seen:
            order.append("other")
        return order

    def _load_config(self, path: Path) -> Dict[str, Any]:
        merged = json.loads(json.dumps(DEFAULT_REPORT_CONFIG, ensure_ascii=False))
        if not path.exists():
            return merged
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(payload, dict):
            return merged
        for key, value in payload.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key].update(value)
            else:
                merged[key] = value
        return merged

    def _card_payload(self, card: MarketDirectionCard) -> Dict[str, Any]:
        if hasattr(card, "to_dict"):
            return dict(card.to_dict())
        return dict(card)

    def _group_by_family(self, cards: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for card in cards:
            family = str(card.get("direction_family") or "other")
            groups[family].append(card)
        return groups

    def _enrich_card(self, card: Dict[str, Any], family_groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        family = str(card.get("direction_family") or "other")
        family_members = family_groups.get(family, [])
        family_item_count = sum(int(item.get("direction_item_count") or 0) for item in family_members) or 1
        item_share = float(card.get("direction_item_count") or 0) / float(family_item_count)
        sales_rank = self._rank_in_family(card, family_members, "direction_sales_median_7d", descending=True)
        video_rank = self._rank_in_family(card, family_members, "direction_video_density_avg", descending=False)
        creator_rank = self._rank_in_family(card, family_members, "direction_creator_density_avg", descending=False)
        form_pick = self._recommend_form(card)
        summary_bucket = self._classify_direction_for_summary(card)
        avoid_signal_confidence = self._avoid_signal_confidence(card)
        resolution = resolve_summary_action(
            summary_bucket=summary_bucket,
            decision_confidence=str(card.get("decision_confidence") or "low"),
            avoid_signal_confidence=avoid_signal_confidence,
        )
        card_with_summary = dict(card)
        card_with_summary["summary_bucket"] = resolution["summary_bucket"]
        route_pick = self._suggest_content_route(card)
        primary_risk = self._primary_risk_factor(card_with_summary)
        enriched = dict(card)
        enriched.update(
            {
                "item_share_in_family": self._format_percent(item_share),
                "sales_rank_in_family": sales_rank,
                "video_rank_in_family": video_rank,
                "creator_rank_in_family": creator_rank,
                "family_item_count": family_item_count,
                "recommended_form": form_pick["recommended_form"],
                "reason_type": form_pick["reason_type"],
                "form_choice_reason": self._form_choice_reason(form_pick["recommended_form"], form_pick["reason_type"]),
                "primary_price_band": self._primary_price_band(card),
                "suggested_test_count": self._suggest_test_count(card_with_summary),
                "content_route_suggestion": route_pick["reason"],
                "content_route_code": route_pick["route"],
                "summary_bucket": resolution["summary_bucket"],
                "primary_risk_factor": primary_risk,
                "is_data_complete": self._is_data_complete(card),
                "avoid_signal_confidence": avoid_signal_confidence,
                "blocked_conflict_warning": str(resolution.get("warning") or ""),
                "blocked_conflict_error": str(resolution.get("error") or ""),
                "report_action_reason_tags": list(resolution.get("reason_tags") or []),
                "decision_confidence_display": self._confidence_display(card),
            }
        )
        return enriched

    def _render_llm_copy(self, cards: List[Dict[str, Any]], use_llm: bool) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Any]]:
        contexts = [card for card in cards if card.get("is_data_complete")]
        if not use_llm or not contexts:
            fallback = {
                card["direction_canonical_key"]: self._default_copy(card)
                for card in cards
            }
            return fallback, {
                "used_llm": False,
                "requested_direction_count": len(cards),
                "rendered_direction_count": len(cards),
                "fallback_count": len(cards),
            }

        rendered: Dict[str, Dict[str, str]] = {}
        fallback_count = 0
        error_details: Dict[str, str] = {}
        max_workers = min(max(int(self.config.get("report", {}).get("llm_max_workers", 4) or 4), 1), max(len(contexts), 1))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.renderer.render, self._llm_context(card)): card
                for card in contexts
            }
            for future in as_completed(futures):
                card = futures[future]
                try:
                    rendered[card["direction_canonical_key"]] = future.result()
                except Exception as exc:
                    error_details[card["direction_canonical_key"]] = str(exc)
        retry_cards = [
            card
            for card in contexts
            if card["direction_canonical_key"] not in rendered
        ]
        for card in retry_cards:
            try:
                rendered[card["direction_canonical_key"]] = self.renderer.render(self._llm_context(card))
                error_details.pop(card["direction_canonical_key"], None)
            except Exception as exc:
                rendered[card["direction_canonical_key"]] = self._default_copy(card)
                error_details[card["direction_canonical_key"]] = str(exc)
                fallback_count += 1
        for card in cards:
            if card["direction_canonical_key"] not in rendered:
                rendered[card["direction_canonical_key"]] = self._default_copy(card)
                fallback_count += 1
        return rendered, {
            "used_llm": True,
            "requested_direction_count": len(cards),
            "rendered_direction_count": len(cards),
            "fallback_count": fallback_count,
            "error_details": error_details,
        }

    def _llm_context(self, card: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "style_cluster": card.get("style_cluster", ""),
            "direction_family": card.get("direction_family", ""),
            "direction_tier": card.get("direction_tier", ""),
            "primary_opportunity_type": card.get("primary_opportunity_type", ""),
            "risk_tags": card.get("risk_tags", []),
            "decision_action": card.get("decision_action", ""),
            "sample_confidence": card.get("sample_confidence", ""),
            "demand_structure": card.get("demand_structure", {}),
            "price_band_analysis": card.get("price_band_analysis", {}),
            "scale_condition": card.get("scale_condition", []),
            "stop_loss_condition": card.get("stop_loss_condition", []),
            "alert": card.get("alert", {}),
            "direction_item_count": card.get("direction_item_count", 0),
            "direction_sales_median_7d": card.get("direction_sales_median_7d", 0.0),
            "direction_video_density_avg": card.get("direction_video_density_avg", 0.0),
            "direction_creator_density_avg": card.get("direction_creator_density_avg", 0.0),
            "sales_rank_in_family": card.get("sales_rank_in_family", ""),
            "video_rank_in_family": card.get("video_rank_in_family", ""),
            "creator_rank_in_family": card.get("creator_rank_in_family", ""),
            "recommended_form": card.get("recommended_form", ""),
            "reason_type": card.get("reason_type", ""),
            "content_route": card.get("content_route_code", ""),
            "route_reason": card.get("content_route_suggestion", ""),
            "top_value_points": card.get("top_value_points", []),
            "scene_tags": card.get("scene_tags", []),
            "target_price_bands": card.get("target_price_bands", []),
            "form_distribution_by_sales": card.get("form_distribution_by_sales", {}),
        }

    def _build_opportunity_card(self, card: Dict[str, Any], llm_copy: Dict[str, str]) -> Dict[str, Any]:
        enriched = dict(card)
        enriched.update(
            {
                "rationale_one_line": llm_copy.get("rationale_one_line", ""),
                "intra_family_comparison": llm_copy.get("intra_family_comparison", ""),
                "risk_note": self._augment_risk_note(card, llm_copy.get("risk_note", "")),
            }
        )
        return enriched

    def _build_direction_decision_cards(self, cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        action_order = {
            "prioritize_low_cost_test": 1,
            "cautious_test": 2,
            "hidden_small_test": 3,
            "strong_signal_verify": 4,
            "hidden_candidate": 5,
            "observe": 6,
            "study_top_not_enter": 7,
            "avoid": 8,
        }
        result = []
        for card in sorted(
            cards,
            key=lambda item: (
                action_order.get(str(item.get("decision_action") or ""), 9),
                -float(item.get("direction_sales_median_7d") or 0.0),
                str(item.get("style_cluster") or ""),
            ),
        ):
            recommended = dict(card.get("recommended_execution") or {})
            risk_tags = list(card.get("risk_tags") or [])
            demand_structure = dict(card.get("demand_structure") or {})
            price_band = dict(card.get("price_band_analysis") or {}).get("recommended_price_band") or {}
            if not isinstance(price_band, dict):
                price_band = {"label": str(price_band or ""), "rmb_range": str(price_band or ""), "confidence": "low"}
            angles = recommended.get("differentiation_angles") or {}
            if not isinstance(angles, dict):
                angles = {"product_angle": list(angles or []), "scene_angle": [], "content_angle": []}
            capability = dict(card.get("our_capability_fit") or {})
            age_structure = dict(card.get("product_age_structure") or {})
            entry_signal = dict(card.get("new_product_entry_signal") or {})
            opportunity_evidence = dict(card.get("opportunity_evidence") or {})
            action_decision = dict(card.get("action_decision") or {})
            action_override = dict(card.get("action_override") or action_decision.get("action_override") or {})
            raw_signal = str(card.get("raw_new_product_signal") or entry_signal.get("type") or "unknown")
            actionable_signal = str(card.get("actionable_new_product_signal") or raw_signal)
            primary_type_label = str(card.get("primary_opportunity_type_label") or card.get("primary_opportunity_type") or "")
            action_label = str(card.get("decision_action_label") or card.get("decision_action") or "")
            action_layer = self._direction_and_sample_action(card)
            result.append(
                {
                    "direction_id": str(card.get("direction_canonical_key") or ""),
                    "direction_name": str(card.get("style_cluster") or card.get("direction_name") or ""),
                    "summary_bucket": str(card.get("summary_bucket") or ""),
                    "primary_opportunity_type": str(card.get("primary_opportunity_type") or ""),
                    "primary_opportunity_type_label": primary_type_label,
                    "risk_tags": risk_tags,
                    "risk_tags_text": display_list(risk_tags, "risk_tag") if risk_tags else "无明显结构化风险标签",
                    "decision_action": str(card.get("decision_action") or ""),
                    "decision_action_label": action_label,
                    "business_priority": str(card.get("business_priority") or "P3"),
                    "business_next_step": str(card.get("business_next_step") or "持续观察"),
                    "business_action_label": str(card.get("business_action_label") or action_label),
                    "direction_action": action_layer["direction_action"],
                    "sample_action": action_layer["sample_action"],
                    "sample_pool_required": bool(action_layer["sample_pool_required"]),
                    "default_action_by_type": str(card.get("default_action_by_type") or action_decision.get("default_action_by_type") or card.get("decision_action") or ""),
                    "actual_action": str(card.get("actual_action") or action_decision.get("actual_action") or card.get("decision_action") or ""),
                    "action_decision": action_decision,
                    "action_override": action_override,
                    "action_overrides": list(card.get("action_overrides") or action_decision.get("action_overrides") or []),
                    "sample_confidence": display_enum(str(card.get("sample_confidence") or ""), "confidence"),
                    "direction_item_count": card.get("direction_item_count", 0),
                    "direction_sales_median_7d": card.get("direction_sales_median_7d", 0.0),
                    "direction_video_density_avg": card.get("direction_video_density_avg", 0.0),
                    "direction_creator_density_avg": card.get("direction_creator_density_avg", 0.0),
                    "demand_structure": dict(card.get("demand_structure") or {}),
                    "competition_structure": dict(card.get("competition_structure") or {}),
                    "price_band_analysis": dict(card.get("price_band_analysis") or {}),
                    "our_capability_fit": dict(card.get("our_capability_fit") or {}),
                    "recommended_execution": recommended,
                    "opportunity_evidence": opportunity_evidence,
                    "opportunity_rule_matched": str(opportunity_evidence.get("rule_matched") or "证据待补"),
                    "opportunity_evidence_text": self._opportunity_evidence_text(opportunity_evidence),
                    "opportunity_why_not_text": self._opportunity_why_not_text(opportunity_evidence),
                    "new_product_structure_tags": list(card.get("new_product_structure_tags") or []),
                    "scale_condition": list(card.get("scale_condition") or []),
                    "stop_loss_condition": list(card.get("stop_loss_condition") or []),
                    "batch_comparison": dict(card.get("batch_comparison") or {}),
                    "alert": dict(card.get("alert") or {}),
                    "test_sku_count": str(recommended.get("test_sku_count") or "待补"),
                    "content_route": str(recommended.get("content_route") or "待补"),
                    "recommended_price_band": price_band,
                    "recommended_price_band_text": self._price_band_text(price_band),
                    "price_band_confidence": display_enum(str(price_band.get("confidence") or "insufficient"), "confidence"),
                    "product_angles_text": self._angle_text(angles.get("product_angle")),
                    "scene_angles_text": self._angle_text(angles.get("scene_angle")),
                    "content_angles_text": self._angle_text(angles.get("content_angle")),
                    "differentiation_angles": angles,
                    "capability_ai_content": display_enum(str(capability.get("ai_content") or "unknown"), "capability"),
                    "capability_replication": display_enum(str(capability.get("replication") or "unknown"), "capability"),
                    "capability_original_demo": display_enum(str(capability.get("original_demo") or "unknown"), "capability"),
                    "capability_scene_localization": display_enum(str(capability.get("scene_localization") or "unknown"), "capability"),
                    "capability_sourcing_fit": display_enum(str(capability.get("sourcing_fit") or "unknown"), "capability"),
                    "capability_rationale": str(capability.get("rationale") or "待补能力说明"),
                    "product_age_structure": age_structure,
                    "new_product_entry_signal": entry_signal,
                    "new_product_entry_signal_type": display_enum(str(entry_signal.get("type") or "unknown"), "new_product_entry_signal"),
                    "raw_new_product_signal": raw_signal,
                    "actionable_new_product_signal": actionable_signal,
                    "raw_new_product_signal_text": display_enum(raw_signal, "new_product_entry_signal"),
                    "actionable_new_product_signal_text": display_enum(actionable_signal, "new_product_entry_signal"),
                    "new_product_entry_rationale": str(card.get("new_product_signal_reason") or entry_signal.get("rationale") or "上架时间样本不足，新品进入判断仅作参考。"),
                    "valid_age_sample_count": str(age_structure.get("valid_age_sample_count") if age_structure.get("valid_age_sample_count") is not None else "待补"),
                    "missing_age_rate": self._format_percent_metric(age_structure.get("missing_age_rate")),
                    "age_confidence": display_enum(str(age_structure.get("age_confidence") or entry_signal.get("confidence") or "insufficient"), "confidence"),
                    "new_30d_count_share": self._format_percent_metric(age_structure.get("new_30d_count_share")),
                    "new_30d_sales_share": self._format_percent_metric(age_structure.get("new_30d_sales_share")),
                    "new_90d_count_share": self._format_percent_metric(age_structure.get("new_90d_count_share")),
                    "new_90d_sales_share": self._format_percent_metric(age_structure.get("new_90d_sales_share")),
                    "old_180d_sales_share": self._format_percent_metric(age_structure.get("old_180d_sales_share")),
                    "median_sales_7d": self._format_metric(demand_structure.get("median_sales_7d")),
                    "mean_sales_7d": self._format_metric(demand_structure.get("mean_sales_7d")),
                    "mean_median_ratio_text": self._format_metric(demand_structure.get("mean_median_ratio")),
                    "top3_sales_share_text": self._format_percent_metric(demand_structure.get("top3_sales_share")),
                    "over_threshold_item_ratio_text": self._format_percent_metric(demand_structure.get("over_threshold_item_ratio")),
                    "concentration_summary": self._concentration_summary(demand_structure, risk_tags),
                    "core_judgement": self._decision_core_judgement(card),
                    "main_risk": self._decision_main_risk(card),
                }
            )
        return result

    def _render_action_decision_lines(self, item: Dict[str, Any]) -> List[str]:
        default_action = str(item.get("default_action_by_type") or item.get("decision_action") or "")
        actual_action = str(item.get("actual_action") or item.get("decision_action") or default_action)
        default_label = DECISION_ACTION_LABELS.get(default_action, default_action or "待补")
        actual_label = DECISION_ACTION_LABELS.get(actual_action, actual_action or "待补")
        override = dict(item.get("action_override") or {})
        if not override.get("is_overridden"):
            return [
                "动作说明：",
                "- 默认动作和最终动作一致：{label}".format(label=actual_label),
            ]

        rule_id = str(override.get("override_rule_id") or "覆盖规则")
        rule_name = str(override.get("override_rule_name") or "")
        reason = str(override.get("override_reason") or "该方向触发动作覆盖规则。")
        evidence_text = self._action_override_evidence_text(list(override.get("override_evidence") or []))
        return [
            "动作覆盖说明：",
            "- 默认动作：{label}".format(label=default_label),
            "- 最终动作：{label}".format(label=actual_label),
            "- 覆盖规则：{rule}".format(rule=" ".join(part for part in [rule_id, rule_name] if part)),
            "- 覆盖原因：{reason}".format(reason=reason),
            "- 关键证据：{evidence}".format(evidence=evidence_text),
        ]

    def _action_override_evidence_text(self, evidence_items: List[Dict[str, Any]]) -> str:
        if not evidence_items:
            return "未记录结构化证据"
        parts = []
        for item in evidence_items[:4]:
            metric = str(item.get("metric") or "指标")
            value = self._format_evidence_value(item.get("value"))
            threshold = str(item.get("threshold") or "")
            conclusion = str(item.get("conclusion") or "")
            text = "{metric} {value}".format(metric=metric, value=value)
            if threshold:
                text = "{text}（阈值 {threshold}）".format(text=text, threshold=threshold)
            if conclusion:
                text = "{text}，{conclusion}".format(text=text, conclusion=conclusion)
            parts.append(text)
        return "；".join(parts)

    def _render_decision_layer_summary(self, payload: Dict[str, Any]) -> List[str]:
        summary = dict(payload.get("decision_layer_summary") or {})
        lines = []
        for action, label in DECISION_ACTION_LABELS.items():
            bucket = dict(summary.get(action) or {})
            names = list(bucket.get("display_names") or [])
            total_count = int(bucket.get("total_count") or 0)
            overflow = int(bucket.get("overflow_count") or 0)
            value = "无"
            if names:
                value = "、".join(names)
                if overflow > 0:
                    value = "{value} 等 {count} 个方向".format(value=value, count=total_count)
            lines.append("{label}：{value}".format(label=label, value=value))
        return lines

    def _render_legacy_diff_rows(self, decision_cards: List[Dict[str, Any]]) -> List[str]:
        if not decision_cards:
            return ["- 无"]
        rows = []
        bucket_labels = {
            SUMMARY_BUCKET_ENTER: "值得立即进入",
            SUMMARY_BUCKET_WATCH: "建议观察",
            SUMMARY_BUCKET_AVOID: "建议避开",
        }
        for item in decision_cards:
            old_label = bucket_labels.get(str(item.get("summary_bucket") or ""), "旧版未归类")
            new_label = str(item.get("decision_action_label") or item.get("decision_action") or "待补")
            rows.extend([
                "",
                "### {name}".format(name=str(item.get("direction_name") or "")),
                "- 旧版结论：{old}".format(old=old_label),
                "- 新版动作：{new}".format(new=new_label),
                "- 变化原因：{reason}".format(reason=self._legacy_diff_reason(item)),
            ])
        return rows

    def _legacy_diff_reason(self, item: Dict[str, Any]) -> str:
        action = str(item.get("decision_action") or "")
        primary_type = str(item.get("primary_opportunity_type") or "")
        risk_tags = set(str(tag) for tag in list(item.get("risk_tags") or []))
        if action == "study_top_not_enter":
            if primary_type == "content_gap" and "old_product_dominated" in risk_tags:
                return "内容缺口明显，但老品占位严重，新品直接进入风险高。"
            if "few_new_winners" in risk_tags:
                return "存在少数新品赢家或头部样本，先拆商品和内容结构，不按方向铺货。"
            return "方向有拆解价值，但不满足直接测款条件。"
        if action == "strong_signal_verify":
            return "需求信号强，但样本、价格带或供应链置信度不足，先进入样本商品池核验。"
        if action == "hidden_candidate":
            return "样本极少但本地场景真实，先进入样本池和下批重点追踪。"
        if action == "observe":
            if "study_top_capacity_limited" in risk_tags:
                return "本批头部拆解资源已满，暂列观察。"
            return "关键信号仍需下一批确认，暂不占用测款和内容资源。"
        if action == "avoid":
            return "需求、竞争或新品窗口不足以支持验证。"
        if action == "cautious_test":
            return "需求基础存在但竞争/同质化风险较高，只做少量款谨慎验证。"
        if action == "prioritize_low_cost_test":
            return "成交基础和内容缺口证据较明确，适合低成本验证。"
        return "按 V1.3 决策层重新分层。"

    def _opportunity_evidence_text(self, evidence: Dict[str, Any]) -> str:
        items = list(evidence.get("evidence_items") or [])
        if not items:
            return "证据不足，需核验"
        parts = []
        for item in items[:4]:
            metric = str(item.get("metric") or "指标")
            direction_value = self._format_evidence_value(item.get("direction_value"))
            baseline_value = self._format_evidence_value(item.get("baseline_value"))
            conclusion = str(item.get("conclusion") or "")
            parts.append("{metric} {direction}（基准 {baseline}）：{conclusion}".format(
                metric=metric,
                direction=direction_value,
                baseline=baseline_value,
                conclusion=conclusion,
            ))
        return "；".join(parts)

    def _opportunity_why_not_text(self, evidence: Dict[str, Any]) -> str:
        items = list(evidence.get("why_not_other_types") or [])
        if not items:
            return "无"
        return "；".join(
            "{type}：{reason}".format(type=str(item.get("type") or "其他类型"), reason=str(item.get("reason") or "未命中"))
            for item in items[:3]
        )

    def _format_evidence_value(self, value: Any) -> str:
        if value is None:
            return "缺失"
        if isinstance(value, float):
            return self._format_metric(value)
        return str(value)

    def _render_action_condition_section(self, item: Dict[str, Any]) -> List[str]:
        action = str(item.get("decision_action") or "")
        if action == "strong_signal_verify":
            return [
                "核验重点：",
                "- Top 商品是否可采购",
                "- 是否存在代表新品",
                "- 价格带是否真实成立",
                "- 使用场景是否明确",
                "- 是否可在 1–2 款内做最小验证",
                "",
                "转入验证条件：",
                "- 至少找到 2 个可采购样本",
                "- 价格带置信度达到中以上",
                "- 供应链匹配达到中以上",
                "- 差异化切口明确",
            ]
        if action == "hidden_candidate":
            return [
                "暗线跟踪重点：",
                "- 下一批样本数是否增加",
                "- 是否出现 2 个以上相似商品",
                "- 本地场景是否在商品标题/主图/内容里明确出现",
                "- 是否能找到可采购样本",
                "",
                "转入暗线小样本验证条件：",
                "- 样本数 >= 3",
                "- 或人工确认 2 个以上可采购样本",
                "- 场景切口明确",
            ]
        if action == "observe":
            return [
                "转入验证条件：",
                "- 可行动新品信号提升到“新品进入信号中等”或以上",
                "- 上架时间置信度、价格带置信度达到“中”或以上",
                "- 供应链匹配不低于“中”",
                "- 差异化切口可具体落到产品 / 场景 / 内容三层",
                "",
                "继续观察条件：",
                "- 可行动新品信号仍为“信号不明确”或“新品进入信号弱”",
                "- 上架时间置信度仍为低 / 不足",
                "- 价格带样本不足或供应链匹配偏低",
                "- 差异化切口仍无法具体化",
            ]
        if action == "study_top_not_enter":
            return [
                "头部拆解通过条件：",
                "- 头部商品可采购性达到中或以上",
                "- 头部内容结构可复刻性达到中或以上",
                "- 至少能拆出 2 个以上可迁移的产品共性",
                "- 至少能拆出 2 个以上可迁移的内容结构",
                "",
                "放弃拆解条件：",
                "- 头部商品不可采购或价格无优势",
                "- 头部内容依赖真人达人/强账号权重，难以迁移",
                "- 头部销量来自单一极端样本，缺少可复制共性",
            ]
        if action == "avoid":
            return [
                "重新观察条件：",
                "- 新品进入窗口、价格带置信度和能力匹配至少两项明显改善",
                "- 出现可拆解的差异化产品或内容切口",
                "",
                "暂不投入原因：",
                "- 当前信号不足以支持测款或内容资源投入",
            ]
        lines = ["放大条件："]
        for condition in item.get("scale_condition", []):
            lines.append("- {text}".format(text=self._condition_text(condition)))
        lines.append("")
        lines.append("止损条件：")
        for condition in item.get("stop_loss_condition", []):
            lines.append("- {text}".format(text=self._condition_text(condition)))
        return lines

    def _decision_core_judgement(self, card: Dict[str, Any]) -> str:
        demand = dict(card.get("demand_structure") or {})
        competition = dict(card.get("competition_structure") or {})
        primary_type = str(card.get("primary_opportunity_type_label") or card.get("primary_opportunity_type") or "")
        median_sales = demand.get("median_sales_7d")
        top3_share = demand.get("top3_sales_share")
        video_density = competition.get("video_density")
        if str(card.get("decision_action") or "") == "study_top_not_enter":
            if "old_product_dominated" in list(card.get("risk_tags") or []):
                return "这个方向有较强成交基础和内容缺口，但新品进入窗口弱、老品占位明显。当前不适合直接铺货，应先拆头部老品的款式、价格、内容结构和可采购性。说明：内容缺口不等于直接进入。"
            return "这个方向当前不直接入场，而是先拆头部样本的款式、价格、内容结构和可采购性，避免把少数头部表现误读成普适机会。"
        if str(card.get("decision_action") or "") == "strong_signal_verify":
            return "这个方向需求信号明显，但样本、价格带或供应链置信度还不足；先进入样本商品池核验，不直接测款。"
        if str(card.get("decision_action") or "") == "hidden_candidate":
            return "这个方向样本极少，但本地场景具备业务意义；先作为暗线候选进入样本池和下批重点追踪。"
        if str(card.get("decision_action") or "") == "observe":
            reasons = list(card.get("observe_reason") or [])
            reason_text = self._observe_reason_text(reasons)
            return "当前进入持续观察，不是因为完全没有机会，而是因为{reason}。下一批重点观察新品进入信号、差异化切口和我方能力匹配是否增强。".format(
                reason=reason_text
            )
        if str(card.get("decision_action") or "") == "avoid":
            return "当前暂不投入，不是因为 crowded 自动等于 avoid，而是需求、供给或新品进入信号不足以支持小样本验证。"
        if str(card.get("primary_opportunity_type") or "") == "content_gap":
            return "这个方向的机会来自有成交基础但内容/达人覆盖还未完全打透，适合用 AI 内容先做低成本验证。"
        if str(card.get("primary_opportunity_type") or "") == "mature_strong_demand":
            return "这个方向需求较强但竞争不低，关键不是抢蓝海，而是找可复制的差异化切口。"
        return "当前被判为{type}，7日销量中位数 {sales}，视频密度 {video}，需要按结构化条件滚动验证。".format(
            type=primary_type or "普通观察型",
            sales=median_sales,
            video=video_density,
        ) + (" Top3 销量占比 {share}。".format(share=top3_share) if top3_share is not None else "")

    def _observe_reason_text(self, reasons: List[str]) -> str:
        mapping = {
            "weak_demand_signal": "成交信号偏弱",
            "price_band_uncertain": "价格带置信度不足",
            "insufficient_differentiation": "差异化切口尚不清晰",
            "missing_capability_fit": "我方能力匹配不足",
            "age_signal_uncertain": "新品进入信号不明确",
            "conflicting_metrics": "销量和竞争指标存在冲突",
            "no_clear_content_route": "内容路线不清晰",
            "study_top_capacity_limited": "本批头部拆解资源已满",
        }
        labels = [mapping.get(item, item) for item in reasons if item]
        return "，且".join(labels[:3]) if labels else "关键行动依据不足"

    def _decision_main_risk(self, card: Dict[str, Any]) -> str:
        risk_tags = list(card.get("risk_tags") or [])
        demand = dict(card.get("demand_structure") or {})
        top3_share = demand.get("top3_sales_share")
        mean_median_ratio = demand.get("mean_median_ratio")
        if "head_concentrated" in risk_tags:
            detail = []
            if top3_share is not None:
                detail.append("Top3 销量占比 {value}".format(value=self._format_percent_metric(top3_share)))
            if mean_median_ratio is not None:
                detail.append("均值/中位数 {value}".format(value=self._format_metric(mean_median_ratio)))
            suffix = "（{detail}）".format(detail="，".join(detail)) if detail else ""
            return "销量可能集中在少数头部样本{suffix}，普通款跟进容易高估普适需求。".format(suffix=suffix)
        if "few_new_winners" in risk_tags:
            return "新品贡献集中在少数赢家上，不适合按方向铺货，应先拆少数新品赢家的共性。"
        if str(card.get("decision_action") or "") == "study_top_not_enter" and "old_product_dominated" in risk_tags:
            return "主要风险在于老品销量和内容资产沉淀较强，新品直接跟进容易低估进入难度。"
        if "sales_distribution_skew" in risk_tags:
            return "销量分布存在偏斜，均值被部分高销量样本拉高，但尚未达到强头部集中口径。"
        if "high_video_density" in risk_tags or "high_creator_density" in risk_tags:
            return "内容或达人竞争密度偏高，复刻容易同质化。"
        if "low_sample" in risk_tags:
            return "样本量偏少，当前不能输出强结论。"
        if "aesthetic_homogeneous" in risk_tags:
            return "审美表达容易同质化，需要明确视觉记忆点。"
        return "主要风险在于方向信号仍需通过内部测款和内容数据验证。"

    def _concentration_summary(self, demand_structure: Dict[str, Any], risk_tags: List[str]) -> str:
        sample_count = int(demand_structure.get("sample_count") or 0)
        top3_share = demand_structure.get("top3_sales_share")
        mean_median_ratio = demand_structure.get("mean_median_ratio")
        if top3_share is None or mean_median_ratio is None:
            return "样本数 {count}，不足以稳定判断头部集中度。".format(count=sample_count)
        if float(top3_share) >= 0.60:
            return "Top3 占比超过 60%，强头部集中，不适合按方向铺货，应先拆头部爆款逻辑。"
        if "head_concentrated" in risk_tags:
            return "存在头部集中信号，需要优先验证普通款是否也能成交。"
        if "few_new_winners" in risk_tags:
            return "新品不多但贡献较高，属于少数新品赢家信号，先拆新品头部，不宜铺货。"
        if "sales_distribution_skew" in risk_tags:
            return "均值/中位数偏高但 Top3 未达头部集中阈值，属于分布偏斜，需避免把少数高销量误判为普适需求。"
        if float(mean_median_ratio) >= 5.0:
            return "均值/中位数极高，可能被极端头部样本拉高，需要谨慎解读。"
        return "暂未出现强头部集中信号，可按方向内主卖点做小样本验证。"

    def _format_metric(self, value: Any) -> str:
        if value is None or value == "":
            return "样本不足"
        try:
            number = float(value)
        except (TypeError, ValueError):
            return str(value)
        if number.is_integer():
            return str(int(number))
        return str(round(number, 4))

    def _format_percent_metric(self, value: Any) -> str:
        if value is None or value == "":
            return "样本不足"
        try:
            return "{value:.1%}".format(value=float(value))
        except (TypeError, ValueError):
            return str(value)

    def _price_band_text(self, price_band: Dict[str, Any]) -> str:
        label = price_band.get("label")
        rmb_range = price_band.get("rmb_range")
        sample_count = price_band.get("sample_count")
        median_sales = price_band.get("median_sales_7d")
        if not label and not rmb_range:
            return "待补价格带"
        label_map = {"low_price": "低价带", "mid_price": "中价带", "high_price": "高价带"}
        return "{label} / {rmb}（样本 {count}，销量中位数 {median}）".format(
            label=label_map.get(str(label or ""), label or "未知"),
            rmb=rmb_range or "待补 RMB 区间",
            count=sample_count if sample_count is not None else "未知",
            median=self._format_metric(median_sales),
        )

    def _angle_text(self, values: Any) -> str:
        items = [str(item) for item in list(values or []) if str(item).strip()]
        return "；".join(items) if items else "待补具体切口"

    def _condition_text(self, condition: Dict[str, Any]) -> str:
        source_names = {
            "internal_test": "内部测款",
            "internal_content": "内部内容数据",
            "manual_or_llm": "人工或模型复核",
        }
        threshold = str(condition.get("threshold", ""))
        threshold = threshold.replace("category_baseline", "类目基准")
        return "{metric} {operator} {threshold}，窗口 {window}，来源 {source}".format(
            metric=self._metric_name(str(condition.get("metric") or "")),
            operator=condition.get("operator", ""),
            threshold=threshold,
            window=condition.get("window", ""),
            source=source_names.get(str(condition.get("metric_source") or ""), condition.get("metric_source", "")),
        )

    def _metric_name(self, value: str) -> str:
        metric_names = {
            "tested_sku_with_sales_count": "有销量测款数",
            "content_ctr": "内容点击率",
            "top_product_copyability": "头部商品可复制性",
            "scene_related_positive_comment_count": "场景相关正向评论数",
            "category_baseline": "类目基准",
        }
        return metric_names.get(str(value or ""), str(value or ""))

    def _metric_list_text(self, values: List[str]) -> str:
        labels = [self._metric_name(str(value)) for value in values if str(value or "").strip()]
        return "、".join(labels) if labels else "无"

    def _apply_batch_comparison(self, cards: List[Dict[str, Any]], country: str, category: str, batch_id: str) -> None:
        previous = self._load_previous_decision_snapshots(country=country, category=category, batch_id=batch_id)
        for card in cards:
            direction_id = str(card.get("direction_canonical_key") or card.get("direction_id") or "")
            old = previous.get(direction_id)
            if not old:
                card["batch_comparison"] = {
                    "last_batch_id": None,
                    "first_seen": True,
                    "sample_count_delta": None,
                    "median_sales_delta": None,
                    "decision_action_change": None,
                    "primary_type_change": None,
                    "consecutive_batches_in_status": 1,
                }
                continue
            old_action = str(old.get("decision_action") or "")
            new_action = str(card.get("decision_action") or "")
            old_type = str(old.get("primary_opportunity_type") or "")
            new_type = str(card.get("primary_opportunity_type") or "")
            consecutive = int(old.get("consecutive_batches_in_status") or 1)
            if old_action == new_action:
                consecutive += 1
            else:
                consecutive = 1
            card["batch_comparison"] = {
                "last_batch_id": old.get("batch_id"),
                "first_seen": False,
                "sample_count_delta": int(card.get("sample_count") or card.get("direction_item_count") or 0) - int(old.get("sample_count") or 0),
                "median_sales_delta": round(float(card.get("direction_sales_median_7d") or 0.0) - float(old.get("median_sales_7d") or 0.0), 2),
                "decision_action_change": None if old_action == new_action else {"from": old_action, "to": new_action},
                "primary_type_change": None if old_type == new_type else {"from": old_type, "to": new_type},
                "consecutive_batches_in_status": consecutive,
            }

    def _load_previous_decision_snapshots(self, country: str, category: str, batch_id: str) -> Dict[str, Dict[str, Any]]:
        path = self._decision_history_path(country=country, category=category)
        if not path.exists():
            return {}
        latest: Dict[str, Dict[str, Any]] = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except ValueError:
                continue
            if str(item.get("batch_id") or "") == str(batch_id or ""):
                continue
            direction_id = str(item.get("direction_id") or "")
            if not direction_id:
                continue
            current = latest.get(direction_id)
            if current is None or str(item.get("batch_id") or "") > str(current.get("batch_id") or ""):
                latest[direction_id] = item
        return latest

    def _save_decision_history(self, cards: List[Dict[str, Any]], country: str, category: str, batch_id: str) -> None:
        path = self._decision_history_path(country=country, category=category)
        path.parent.mkdir(parents=True, exist_ok=True)
        existing_keys = set()
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                try:
                    item = json.loads(line)
                except ValueError:
                    continue
                existing_keys.add((str(item.get("batch_id") or ""), str(item.get("direction_id") or "")))
        rows = []
        for card in cards:
            direction_id = str(card.get("direction_canonical_key") or card.get("direction_id") or "")
            key = (str(batch_id or ""), direction_id)
            if key in existing_keys:
                continue
            comparison = dict(card.get("batch_comparison") or {})
            rows.append(
                {
                    "batch_id": batch_id,
                    "country": country,
                    "category": category,
                    "direction_id": direction_id,
                    "direction_name": str(card.get("style_cluster") or card.get("direction_name") or ""),
                    "sample_count": int(card.get("sample_count") or card.get("direction_item_count") or 0),
                    "median_sales_7d": float(card.get("direction_sales_median_7d") or 0.0),
                    "primary_opportunity_type": str(card.get("primary_opportunity_type") or ""),
                    "decision_action": str(card.get("decision_action") or ""),
                    "risk_tags": list(card.get("risk_tags") or []),
                    "consecutive_batches_in_status": int(comparison.get("consecutive_batches_in_status") or 1),
                }
            )
        if not rows:
            return
        with path.open("a", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _decision_history_path(self, country: str, category: str) -> Path:
        safe_country = re.sub(r"[^A-Za-z0-9_\\-]+", "_", str(country or "").strip() or "unknown")
        safe_category = re.sub(r"[^A-Za-z0-9_\\-]+", "_", str(category or "").strip() or "unknown")
        return self.skill_dir / "artifacts" / "market_insight" / "decision_history" / "{country}__{category}.jsonl".format(
            country=safe_country,
            category=safe_category,
        )

    def _build_decision_summary(self, cards: List[Dict[str, Any]]) -> Dict[str, Any]:
        grouped: Dict[str, List[Dict[str, Any]]] = {
            SUMMARY_BUCKET_ENTER: [],
            SUMMARY_BUCKET_WATCH: [],
            SUMMARY_BUCKET_AVOID: [],
        }
        for card in cards:
            if not card.get("is_data_complete"):
                continue
            grouped[card["summary_bucket"]].append(card)
        for bucket in grouped.values():
            bucket.sort(key=self._decision_sort_key)
        max_items = int(self.config.get("report", {}).get("max_summary_items", 3) or 3)
        summary = {}
        for bucket_name, items in grouped.items():
            shown = items[:max_items]
            summary[bucket_name] = {
                "items": shown,
                "all_items": items,
                "display_names": [item["style_cluster"] for item in shown],
                "total_count": len(items),
                "overflow_count": max(0, len(items) - len(shown)),
            }
        return summary

    def _build_direction_matrix(self, cards: List[Dict[str, Any]], decision_summary: Dict[str, Any], family_order: List[str], use_llm: bool) -> Dict[str, Any]:
        matrix = {family: {tier: [] for tier in TIER_ORDER} for family in family_order}
        for card in cards:
            family = str(card.get("direction_family") or "other")
            tier = str(card.get("direction_tier") or "")
            if family not in matrix:
                matrix[family] = {key: [] for key in TIER_ORDER}
            if tier not in matrix[family]:
                matrix[family][tier] = []
            matrix[family][tier].append(card["style_cluster"])
        header = "| 大类 \\ 层级 | priority | balanced | crowded | low_sample |"
        divider = "|---|---|---|---|---|"
        lines = [header, divider]
        for family in family_order:
            cells = ["、".join(matrix[family].get(tier, [])) for tier in TIER_ORDER]
            lines.append("| {family} | {priority} | {balanced} | {crowded} | {low_sample} |".format(
                family=family,
                priority=cells[0],
                balanced=cells[1],
                crowded=cells[2],
                low_sample=cells[3],
            ))
        display_lines = self._build_direction_matrix_display_lines(matrix, family_order)
        observations, observation_meta = self._matrix_observations(matrix, cards, decision_summary, family_order=family_order, use_llm=use_llm)
        return {
            "table_lines": lines,
            "display_lines": display_lines,
            "matrix": matrix,
            "observations": observations,
            "observation_meta": observation_meta,
        }

    def _build_direction_matrix_display_lines(self, matrix: Dict[str, Dict[str, List[str]]], family_order: List[str]) -> List[str]:
        lines: List[str] = []
        for family in family_order:
            lines.append("### {family}".format(family=family))
            for tier in TIER_ORDER:
                names = list(matrix.get(family, {}).get(tier, []))
                value = "、".join(names) if names else "无"
                lines.append("- {tier}：{value}".format(tier=TIER_DISPLAY_LABELS.get(tier, tier), value=value))
            lines.append("")
        if lines and not lines[-1].strip():
            lines.pop()
        return lines

    def _build_reverse_signals(self, cards: List[Dict[str, Any]]) -> Dict[str, Any]:
        hidden_risks = [self._reverse_signal_summary(card, "risk") for card in self._find_hidden_risks(cards)]
        hidden_opportunities = [self._reverse_signal_summary(card, "opportunity") for card in self._find_hidden_opportunities(cards)]
        return {
            "hidden_risks": hidden_risks,
            "hidden_opportunities": hidden_opportunities,
        }

    def _build_market_regime_assessment(self, cards: List[Dict[str, Any]]) -> Dict[str, Any]:
        action_counts = defaultdict(int)
        low_sample_count = 0
        for card in cards:
            action_counts[str(card.get("decision_action") or "observe")] += 1
            if str(card.get("sample_confidence") or "") in {"low", "insufficient"}:
                low_sample_count += 1
        count_prioritize = action_counts["prioritize_low_cost_test"]
        count_cautious = action_counts["cautious_test"]
        count_hidden = action_counts["hidden_small_test"]
        count_strong_verify = action_counts["strong_signal_verify"]
        count_hidden_candidate = action_counts["hidden_candidate"]
        count_observe = action_counts["observe"]
        count_study_top = action_counts["study_top_not_enter"]
        count_avoid = action_counts["avoid"]
        total = max(len(cards), 1)

        if low_sample_count / total >= 0.5:
            return {
                "regime_label": "信号不足期",
                "regime_code": "insufficient_signal",
                "regime_reason": "本批次低样本或低置信方向占比较高，强行动判断容易被样本噪声放大。",
                "investment_advice": "先补样本和上架时间数据，只允许少量暗线小样本验证，不建议按大方向加码。",
            }
        if count_prioritize >= 2 and count_avoid <= 1:
            return {
                "regime_label": "强进入窗口",
                "regime_code": "strong_entry_window",
                "regime_reason": "本批次已有多个优先低成本验证方向，且暂不投入方向较少，说明存在相对清晰的进入窗口。",
                "investment_advice": "可以集中资源验证优先方向，但仍按放大/止损条件滚动，不做无条件铺货。",
            }
        if count_prioritize >= 1 and count_cautious >= 1 and count_study_top >= 1 and count_avoid == 0:
            return {
                "regime_label": "成熟供给盘下的结构性验证期",
                "regime_code": "mature_supply_structural",
                "regime_reason": "本批次存在少数低成本验证方向和谨慎切入方向，同时也有方向需要拆头部不直接入场，说明市场不是完全没有机会，但普通铺货风险较高。",
                "investment_advice": "不建议按大类整体加码。优先把资源集中到 1 个低成本验证方向、1-2 个谨慎切入方向，以及若干头部拆解方向。",
            }
        if count_avoid + count_observe >= max(3, int(total * 0.6)):
            return {
                "regime_label": "高风险观察期",
                "regime_code": "high_risk_observe",
                "regime_reason": "多数方向仍处于持续观察或暂不投入，说明可执行切口、能力匹配或新品进入窗口尚未形成稳定共识。",
                "investment_advice": "降低整体投入，只保留明确满足置信度和能力匹配条件的方向做小样本验证。",
            }
        if count_prioritize + count_cautious + count_hidden + count_strong_verify + count_hidden_candidate >= 2:
            return {
                "regime_label": "结构化测试窗口",
                "regime_code": "structured_test_window",
                "regime_reason": "本批次有可验证或待核验方向，但动作以低成本、谨慎、暗线或强信号核验为主，说明机会存在但仍需要结构化验证。",
                "investment_advice": "按方向动作分配资源：优先方向小批量验证，观察方向只跟踪信号，不占用内容产能。",
            }
        return {
            "regime_label": "成熟供给盘下的结构性验证期",
            "regime_code": "mature_supply_structural",
            "regime_reason": "本批次方向动作分散在验证、观察和头部拆解之间，说明市场已不是简单蓝海，需要围绕内容缺口、新品窗口和头部可复制性决策。",
            "investment_advice": "不做大类整体加码，优先推进动作明确的方向，其余方向等待下一批信号确认。",
        }

    def _build_watch_direction_table(self, cards: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        rows = []
        for card in cards:
            current_signal, action_condition, block_condition = self._watch_metric_plan(card)
            rows.append(
                {
                    "direction_name": str(card.get("style_cluster") or ""),
                    "current_action": str(card.get("decision_action_label") or card.get("decision_action") or "持续观察"),
                    "observe_reason": self._observe_reason_text(list(card.get("observe_reason") or [])),
                    "current_signal": current_signal,
                    "action_condition": action_condition,
                    "block_condition": block_condition,
                }
            )
        return rows

    def _build_cross_system_recommendations(self, cards: List[Dict[str, Any]], category: str) -> Dict[str, Any]:
        selected_cards = [
            card
            for card in sorted(cards, key=self._decision_sort_key)
            if str(card.get("decision_action") or "") in {
                "prioritize_low_cost_test",
                "cautious_test",
                "hidden_small_test",
                "strong_signal_verify",
                "hidden_candidate",
                "observe",
                "study_top_not_enter",
                "avoid",
            }
        ]
        route_items = []
        for card in selected_cards:
            route = self._suggest_content_route(card)
            action = str(card.get("decision_action") or "")
            if action == "avoid":
                if route["route"] == "original_preferred":
                    suggestion = "当前建议避开；如果必须试，只建议用原创小样本验证，{reason}".format(reason=route["reason"])
                else:
                    suggestion = "当前建议避开，先不要投入批量内容资源，等成交或样本进一步验证。"
            elif action == "observe":
                suggestion = "当前持续观察，不占用内容资源；重点看新品进入信号、差异化切口和能力匹配是否改善。"
            elif action == "hidden_small_test":
                suggestion = "只做 1-2 款小样本内容验证，不做方向铺货；验证新品信号和场景切口是否真实。"
            elif action == "strong_signal_verify":
                suggestion = "当前先做强信号核验，不直接测款；优先查看样本商品池、Top 商品可采购性和价格带真实性。"
            elif action == "hidden_candidate":
                suggestion = "当前作为暗线候选，不占用正式内容资源；进入样本池和下批重点追踪。"
            elif action == "study_top_not_enter":
                suggestion = "只拆头部爆款共性，不直接入场；先判断头部商品是否可采购、可复刻。"
            elif action == "cautious_test":
                suggestion = "少量款谨慎验证，优先控制内容成本；不要直接放大铺货。"
            elif action == "prioritize_low_cost_test":
                suggestion = "4-6 款低成本验证，先建立内容和成交基线。"
            elif route["route"] == "original_preferred":
                suggestion = "建议原创脚本优先，{reason}".format(reason=route["reason"])
            elif route["route"] == "replica_preferred":
                suggestion = "建议优先复刻验证，{reason}".format(reason=route["reason"])
            else:
                suggestion = "建议复刻与原创并行，{reason}".format(reason=route["reason"])
            route_items.append({"direction_name": card["style_cluster"], "suggestion": suggestion})

        category_weight_maps = self.config.get("scoring_weight_advice_by_category", {}) or {}
        weight_map = category_weight_maps.get(str(category or "").strip()) or self.config.get("scoring_weight_advice", {}) or {}
        weight_items = []
        seen_families = set()
        for card in selected_cards:
            family = str(card.get("direction_family") or "other")
            if family in seen_families:
                continue
            seen_families.add(family)
            advice = dict(weight_map.get(family) or weight_map.get("other") or {})
            focus_metric = str(advice.get("focus_metric") or "信息完整度")
            reason = str(advice.get("reason") or "先用稳定可验证指标做筛选。")
            weight_items.append(
                {
                    "direction_family": family,
                    "suggestion": "建议调高“{metric}”权重，{reason}".format(metric=focus_metric, reason=reason),
                }
            )
        return {
            "content_route_recommendations": route_items,
            "scoring_weight_recommendations": weight_items,
        }

    def _rank_in_family(
        self,
        target_card: Dict[str, Any],
        family_members: List[Dict[str, Any]],
        field_name: str,
        descending: bool,
    ) -> str:
        ordered = sorted(
            family_members,
            key=lambda item: (float(item.get(field_name) or 0.0), item.get("style_cluster", "")),
            reverse=descending,
        )
        for index, item in enumerate(ordered, start=1):
            if item.get("direction_canonical_key") == target_card.get("direction_canonical_key"):
                return "{current}/{total}".format(current=index, total=max(len(ordered), 1))
        return "-/-"

    def _classify_direction_for_summary(self, card: Dict[str, Any]) -> str:
        thresholds = self.config.get("thresholds", {}) or {}
        sales_median_baseline = float(thresholds.get("sales_median_baseline", 250.0) or 250.0)
        crowded_supply_overhang_sales_median = float(
            thresholds.get("crowded_supply_overhang_sales_median", 300.0) or 300.0
        )
        video_density_high = float(thresholds.get("video_density_high", 1.0) or 1.0)
        video_density_enter = float(thresholds.get("video_density_enter", 0.5) or 0.5)
        video_density_crowded_enter = float(thresholds.get("video_density_crowded_enter", 0.2) or 0.2)
        top_item_count_threshold = int(thresholds.get("top_item_count_threshold", 100) or 100)
        tier = str(card.get("direction_tier") or "")
        video_density = float(card.get("direction_video_density_avg") or 0.0)
        item_count = int(card.get("direction_item_count") or 0)
        sales_median = float(card.get("direction_sales_median_7d") or 0.0)
        if tier == "crowded":
            if video_density < video_density_crowded_enter and sales_median >= sales_median_baseline:
                return SUMMARY_BUCKET_ENTER
            if video_density > video_density_high:
                return SUMMARY_BUCKET_AVOID
            if item_count > top_item_count_threshold and sales_median < crowded_supply_overhang_sales_median:
                return SUMMARY_BUCKET_AVOID
            return SUMMARY_BUCKET_WATCH
        if tier == "priority":
            return SUMMARY_BUCKET_ENTER
        if tier == "balanced" and video_density < video_density_enter:
            return SUMMARY_BUCKET_ENTER
        if tier == "balanced":
            return SUMMARY_BUCKET_WATCH
        if tier == "low_sample" and sales_median >= sales_median_baseline:
            return SUMMARY_BUCKET_WATCH
        return SUMMARY_BUCKET_AVOID

    def _avoid_signal_confidence(self, card: Dict[str, Any]) -> str:
        thresholds = self.config.get("thresholds", {}) or {}
        return classify_avoid_signal_confidence(
            direction_tier=str(card.get("direction_tier") or ""),
            video_density_avg=float(card.get("direction_video_density_avg") or 0.0),
            item_count=int(card.get("direction_item_count") or 0),
            sales_median_7d=float(card.get("direction_sales_median_7d") or 0.0),
            video_density_high=float(thresholds.get("video_density_high", 1.0) or 1.0),
            video_density_crowded_enter=float(thresholds.get("video_density_crowded_enter", 0.2) or 0.2),
            sales_median_baseline=float(thresholds.get("sales_median_baseline", 250.0) or 250.0),
            crowded_supply_overhang_sales_median=float(
                thresholds.get("crowded_supply_overhang_sales_median", 300.0) or 300.0
            ),
            top_item_count_threshold=int(thresholds.get("top_item_count_threshold", 100) or 100),
        )

    def _recommend_form(self, card: Dict[str, Any]) -> Dict[str, str]:
        sales_dist = dict(card.get("form_distribution_by_sales") or {})
        count_dist = dict(card.get("form_distribution_by_count") or {})
        if sales_dist:
            top_form_by_sales = self._top_form(sales_dist)
            top_form_by_count = self._top_form(count_dist) if count_dist else ""
            if top_form_by_count and top_form_by_sales != top_form_by_count:
                return {
                    "recommended_form": top_form_by_sales,
                    "reason_type": "premium_form",
                }
            return {
                "recommended_form": top_form_by_sales,
                "reason_type": "dominant_form",
            }
        return {
            "recommended_form": self._top_form(count_dist),
            "reason_type": "fallback_by_count",
        }

    def _suggest_test_count(self, card: Dict[str, Any]) -> str:
        if str(card.get("summary_bucket") or "") == SUMMARY_BUCKET_ENTER and str(card.get("direction_tier") or "") == "crowded":
            return "4-6 款"
        tier = str(card.get("direction_tier") or "")
        mapping = self.config.get("test_count_by_tier", {}) or {}
        return str(mapping.get(tier) or "不建议测款")

    def _suggest_content_route(self, card: Dict[str, Any]) -> Dict[str, str]:
        thresholds = self.config.get("thresholds", {}) or {}
        video_density_high = float(thresholds.get("video_density_high", 1.0) or 1.0)
        video_density_low = float(thresholds.get("video_density_low", 0.3) or 0.3)
        video_density_balanced_replica = float(thresholds.get("video_density_balanced_replica", 0.7) or 0.7)
        tier = str(card.get("direction_tier") or "")
        video_density = float(card.get("direction_video_density_avg") or 0.0)
        if tier == "crowded" and video_density > video_density_high:
            return {
                "route": "original_preferred",
                "reason": "视频密度高，复刻易同质化，建议原创差异化。",
            }
        if video_density < video_density_low:
            return {
                "route": "replica_preferred",
                "reason": "内容赛道相对空白，复刻验证更容易建立基线。",
            }
        if tier == "balanced" and video_density < video_density_balanced_replica:
            return {
                "route": "replica_preferred",
                "reason": "当前密度仍可承接复刻验证，先用复刻快速验证需求边界。",
            }
        return {
            "route": "balanced",
            "reason": "先用小预算验证内容方向。",
        }

    def _find_hidden_risks(self, cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        thresholds = self.config.get("thresholds", {}) or {}
        sales_median_baseline = float(thresholds.get("sales_median_baseline", 250.0) or 250.0)
        crowded_supply_overhang_sales_median = float(
            thresholds.get("crowded_supply_overhang_sales_median", 300.0) or 300.0
        )
        top_item_count_threshold = int(thresholds.get("top_item_count_threshold", 100) or 100)
        results: List[Dict[str, Any]] = []
        seen_keys = set()
        for card in cards:
            key = str(card.get("direction_canonical_key") or "")
            if str(card.get("summary_bucket") or "") == SUMMARY_BUCKET_ENTER:
                continue
            if card.get("direction_tier") == "crowded":
                if key not in seen_keys:
                    results.append(card)
                    seen_keys.add(key)
                continue
            if (
                int(card.get("direction_item_count") or 0) > top_item_count_threshold
                and float(card.get("direction_sales_median_7d") or 0.0) < max(sales_median_baseline, crowded_supply_overhang_sales_median)
                and key not in seen_keys
            ):
                results.append(card)
                seen_keys.add(key)
        results.sort(key=lambda item: (-int(item.get("direction_item_count") or 0), item.get("style_cluster", "")))
        return results[:3]

    def _find_hidden_opportunities(self, cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        thresholds = self.config.get("thresholds", {}) or {}
        sales_median_baseline = float(thresholds.get("sales_median_baseline", 250.0) or 250.0)
        video_density_enter = float(thresholds.get("video_density_enter", 0.5) or 0.5)
        video_density_crowded_enter = float(thresholds.get("video_density_crowded_enter", 0.2) or 0.2)
        results = []
        for card in cards:
            if str(card.get("summary_bucket") or "") == SUMMARY_BUCKET_ENTER:
                continue
            video_density = float(card.get("direction_video_density_avg") or 0.0)
            if card.get("direction_tier") == "balanced" and video_density < video_density_enter:
                results.append(card)
            elif video_density < video_density_crowded_enter and float(card.get("direction_sales_median_7d") or 0.0) >= sales_median_baseline:
                results.append(card)
        results.sort(key=self._decision_sort_key)
        return results[:3]

    def _reverse_signal_summary(self, card: Dict[str, Any], mode: str) -> Dict[str, str]:
        direction_name = str(card.get("style_cluster") or "")
        item_count = int(card.get("direction_item_count") or 0)
        sales_median = float(card.get("direction_sales_median_7d") or 0.0)
        video_density = float(card.get("direction_video_density_avg") or 0.0)
        creator_density = float(card.get("direction_creator_density_avg") or 0.0)
        action = str(card.get("decision_action") or "")
        if action == "study_top_not_enter":
            return {
                "direction_name": direction_name,
                "summary": "该方向存在头部或少数新品赢家信号，但不适合按方向铺货。当前动作是拆头部不直接入场，重点拆解头部商品的款式、价格、内容结构和可采购性。",
            }
        if action == "observe":
            return {
                "direction_name": direction_name,
                "summary": "当前存在局部亮点，但由于样本置信度、新品窗口、价格带或能力匹配不足，暂不进入验证池。下一批重点观察新品进入信号和差异化切口。",
            }
        if action == "cautious_test":
            return {
                "direction_name": direction_name,
                "summary": "需求基础存在，但竞争和同质化风险较高。当前只适合少量款谨慎验证，不适合大规模铺货。",
            }
        if action == "strong_signal_verify":
            return {
                "direction_name": direction_name,
                "summary": "当前属于强信号待核验，需求信号明显但不能直接测款。下一步只进入样本商品池，核查 Top 商品、代表新品、价格带和可采购性。",
            }
        if action == "hidden_candidate":
            return {
                "direction_name": direction_name,
                "summary": "当前属于暗线候选，样本仍极少，不直接进入低成本验证。下一步只进入样本商品池和下批重点追踪；若下一批样本数达到 3 个以上，或人工确认 2 个以上可采购样本，再转入暗线小样本验证。",
            }
        if mode == "risk":
            thresholds = self.config.get("thresholds", {}) or {}
            crowded_supply_overhang_sales_median = float(
                thresholds.get("crowded_supply_overhang_sales_median", 300.0) or 300.0
            )
            video_density_high = float(thresholds.get("video_density_high", 1.0) or 1.0)
            top_item_count_threshold = int(thresholds.get("top_item_count_threshold", 100) or 100)
            if str(card.get("direction_tier") or "") == "crowded" and video_density >= video_density_high:
                summary = "商品数 {item_count} 看起来不低，但视频密度 {video_density}、达人密度 {creator_density} 都偏高，继续跟进更容易撞进同质化竞争。".format(
                    item_count=item_count,
                    video_density=video_density,
                    creator_density=creator_density,
                )
            elif item_count > top_item_count_threshold and sales_median < crowded_supply_overhang_sales_median:
                summary = "商品数已经堆到 {item_count}，但 7 日销量中位数只有 {sales_median}，供给扩张速度明显快于稳定成交，继续跟进更容易吃到库存型风险。".format(
                    item_count=item_count,
                    sales_median=sales_median,
                )
            elif str(card.get("direction_tier") or "") == "crowded":
                summary = "虽然视频密度只有 {video_density}，但方向层级已经挤到 crowded，说明成交主要集中在少数样本上，贸然放大更容易高估普适需求。".format(
                    video_density=video_density,
                )
            else:
                summary = "商品数已经堆到 {item_count}，但 7 日销量中位数只有 {sales_median}，说明供给热闹并没有转成稳定成交。".format(
                    item_count=item_count,
                    sales_median=sales_median,
                )
        else:
            action = str(card.get("decision_action") or "")
            if action == "observe":
                summary = "当前存在局部亮点，但由于样本置信度、新品窗口、价格带或能力匹配不足，暂不进入验证池；下一批重点观察新品进入信号和差异化切口。"
            elif action == "hidden_small_test":
                summary = "样本仍不大，但场景或新品窗口存在候选信号，建议只用 1-2 款小样本验证，不做方向铺货。"
            elif action == "hidden_candidate":
                summary = "当前属于暗线候选，样本仍极少，不直接进入验证池；下一步进入样本商品池和下批重点追踪，满足条件后再转入暗线小样本验证。"
            elif action == "strong_signal_verify":
                summary = "当前属于强信号待核验，先进入样本商品池核查，不直接测款。"
            elif str(card.get("direction_tier") or "") == "balanced":
                summary = "虽然目前只在 balanced 档，但视频密度 {video_density} 偏低、销量中位数 {sales_median} 还站得住，说明内容空间还没被挤满。".format(
                    video_density=video_density,
                    sales_median=sales_median,
                )
            elif str(card.get("direction_tier") or "") == "crowded":
                summary = "虽然层级被打到 crowded，但视频密度只有 {video_density}、销量中位数还有 {sales_median}，更像被层级规则低估的内容空白方向。".format(
                    video_density=video_density,
                    sales_median=sales_median,
                )
            else:
                summary = "当前样本还不多，但视频密度只有 {video_density} 且销量中位数达到 {sales_median}，适合作为观察候选，需满足转入条件后再验证。".format(
                    video_density=video_density,
                    sales_median=sales_median,
                )
        return {"direction_name": direction_name, "summary": summary}

    def _matrix_observations(self, matrix: Dict[str, Dict[str, List[str]]], cards: List[Dict[str, Any]], decision_summary: Dict[str, Any], family_order: List[str], use_llm: bool) -> Tuple[List[str], Dict[str, Any]]:
        fallback_observations = self._default_matrix_observations(matrix, cards, decision_summary, family_order)
        # V1.3.1 keeps matrix observations rule-rendered so wording always
        # follows final decision_action instead of drifting back to old buckets.
        return fallback_observations, {"used_llm": bool(use_llm), "fallback": True}

    def _default_matrix_observations(self, matrix: Dict[str, Dict[str, List[str]]], cards: List[Dict[str, Any]], decision_summary: Dict[str, Any], family_order: List[str]) -> List[str]:
        cards_by_action: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for card in cards:
            cards_by_action[str(card.get("decision_action") or "observe")].append(card)
        def names(action: str) -> List[str]:
            return [str(card.get("style_cluster") or "") for card in sorted(cards_by_action.get(action, []), key=self._decision_sort_key)]
        direct = names("prioritize_low_cost_test") + names("cautious_test") + names("hidden_small_test")
        study = names("study_top_not_enter")
        verify = names("strong_signal_verify")
        hidden = names("hidden_candidate")
        observe = names("observe")
        avoid = names("avoid")
        observations = []
        if direct:
            observations.append("可直接进入验证的方向主要是 {names}，这些方向才进入测款或内容验证池。".format(names="、".join(direct[:3])))
        if study:
            details = []
            for card in sorted(cards_by_action.get("study_top_not_enter", []), key=self._decision_sort_key)[:3]:
                if str(card.get("primary_opportunity_type") or "") == "content_gap" and "old_product_dominated" in list(card.get("risk_tags") or []):
                    details.append("{name}：内容缺口明显，但新品进入窗口弱、老品占位明显，先拆头部不直接测款".format(name=str(card.get("style_cluster") or "")))
                else:
                    details.append("{name}：先拆头部样本，不直接铺货".format(name=str(card.get("style_cluster") or "")))
            observations.append("头部拆解方向包括 {details}。".format(details="；".join(details)))
        if verify:
            observations.append("强信号待核验方向为 {names}，需求信号明显但样本、价格带或供应链仍需先核查。".format(names="、".join(verify[:3])))
        if hidden:
            observations.append("暗线候选方向为 {names}，样本极少但具备场景意义，只进入样本池和下批重点追踪。".format(names="、".join(hidden[:3])))
        if observe and len(observations) < 5:
            observations.append("持续观察方向为 {names}，暂不占用内容和测款资源。".format(names="、".join(observe[:3])))
        if avoid and len(observations) < 5:
            observations.append("暂不投入方向为 {names}，当前不进入验证或拆解池。".format(names="、".join(avoid[:3])))
        if not observations:
            observations.append("当前批次暂无可直接验证或拆解方向，优先补样本与字段完整性。")
        return observations[:5]

    def _matrix_llm_context(self, matrix: Dict[str, Dict[str, List[str]]], cards: List[Dict[str, Any]], decision_summary: Dict[str, Any], family_order: List[str]) -> Dict[str, Any]:
        cards_by_family: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for card in cards:
            cards_by_family[str(card.get("direction_family") or "other")].append(card)
        matrix_rows = []
        for family in family_order:
            family_cards = cards_by_family.get(family, [])
            matrix_rows.append(
                {
                    "direction_family": family,
                    "priority": list(matrix.get(family, {}).get("priority", [])),
                    "balanced": list(matrix.get(family, {}).get("balanced", [])),
                    "crowded": list(matrix.get(family, {}).get("crowded", [])),
                    "low_sample": list(matrix.get(family, {}).get("low_sample", [])),
                    "enter": [card["style_cluster"] for card in family_cards if str(card.get("summary_bucket") or "") == SUMMARY_BUCKET_ENTER],
                    "watch": [card["style_cluster"] for card in family_cards if str(card.get("summary_bucket") or "") == SUMMARY_BUCKET_WATCH],
                    "avoid": [card["style_cluster"] for card in family_cards if str(card.get("summary_bucket") or "") == SUMMARY_BUCKET_AVOID],
                }
            )
        return {
            "decision_summary": {
                SUMMARY_BUCKET_ENTER: list(decision_summary.get(SUMMARY_BUCKET_ENTER, {}).get("display_names", [])),
                SUMMARY_BUCKET_WATCH: list(decision_summary.get(SUMMARY_BUCKET_WATCH, {}).get("display_names", [])),
                SUMMARY_BUCKET_AVOID: list(decision_summary.get(SUMMARY_BUCKET_AVOID, {}).get("display_names", [])),
            },
            "matrix_rows": matrix_rows,
        }

    def _summary_line_text(self, decision_summary: Dict[str, Any], bucket: str) -> str:
        payload = decision_summary.get(bucket, {}) or {}
        names = list(payload.get("display_names") or [])
        total_count = int(payload.get("total_count") or 0)
        overflow = int(payload.get("overflow_count") or 0)
        if not names:
            return "无"
        text = "、".join(names)
        if overflow > 0:
            text = "{text} 等 {count} 个方向".format(text=text, count=total_count)
        return text

    def _decision_sort_key(self, card: Dict[str, Any]) -> Tuple[int, float, float, int, str]:
        bucket_order = {
            SUMMARY_BUCKET_ENTER: 3,
            SUMMARY_BUCKET_WATCH: 2,
            SUMMARY_BUCKET_AVOID: 1,
        }
        tier_order = {
            "priority": 4,
            "balanced": 3,
            "crowded": 2,
            "low_sample": 1,
        }
        return (
            -bucket_order.get(str(card.get("summary_bucket") or ""), 0),
            -tier_order.get(str(card.get("direction_tier") or ""), 0),
            -float(card.get("direction_sales_median_7d") or 0.0),
            float(card.get("direction_video_density_avg") or 0.0),
            str(card.get("style_cluster") or ""),
        )

    def _top_form(self, distribution: Dict[str, float]) -> str:
        if not distribution:
            return "待补数据"
        return sorted(distribution.items(), key=lambda item: (-float(item[1] or 0.0), item[0]))[0][0]

    def _form_choice_reason(self, recommended_form: str, reason_type: str) -> str:
        if reason_type == "premium_form":
            return "{form} 在销量占比上强于商品数占比，说明更像少而精的成交承载形态。".format(form=recommended_form)
        if reason_type == "dominant_form":
            return "{form} 同时是供给和销量的主承载形态，适合作为本轮先测主形态。".format(form=recommended_form)
        return "{form} 当前先按商品数主承载形态试，后续再结合真实成交补校准。".format(form=recommended_form)

    def _primary_price_band(self, card: Dict[str, Any]) -> str:
        bands = list(card.get("target_price_bands") or [])
        return str(bands[0] or "") if bands else "待补数据"

    def _primary_risk_factor(self, card: Dict[str, Any]) -> str:
        video_density = float(card.get("direction_video_density_avg") or 0.0)
        video_density_high = float(self.config.get("thresholds", {}).get("video_density_high", 1.0) or 1.0)
        if str(card.get("direction_tier") or "") == "crowded" and video_density >= video_density_high:
            return "内容竞争密度过高"
        if str(card.get("direction_tier") or "") == "low_sample":
            return "样本量偏少"
        if float(card.get("direction_creator_density_avg") or 0.0) >= float(self.config.get("thresholds", {}).get("video_density_high", 1.0) or 1.0):
            return "达人竞争密度偏高"
        if str(card.get("summary_bucket") or "") == SUMMARY_BUCKET_ENTER:
            return "成交可持续性仍需验证"
        return "方向验证仍需补充样本"

    def _watch_metric_plan(self, card: Dict[str, Any]) -> Tuple[str, str, str]:
        age = dict(card.get("product_age_structure") or {})
        price_band = dict(card.get("price_band_analysis") or {}).get("recommended_price_band") or {}
        capability = dict(card.get("our_capability_fit") or {})
        demand = dict(card.get("demand_structure") or {})
        entry_signal = dict(card.get("new_product_entry_signal") or {})
        raw_signal = str(card.get("raw_new_product_signal") or entry_signal.get("type") or "unknown")
        actionable_signal = str(card.get("actionable_new_product_signal") or raw_signal)
        angles = dict((card.get("recommended_execution") or {}).get("differentiation_angles") or {})
        current_signal = (
            "原始新品信号 {raw_signal}，可行动新品信号 {actionable_signal}，上架时间置信度 {age_conf}，价格带置信度 {price_conf}，供应链 {sourcing}，Top3占比 {top3}，均值/中位数 {ratio}。"
        ).format(
            raw_signal=display_enum(raw_signal, "new_product_entry_signal"),
            actionable_signal=display_enum(actionable_signal, "new_product_entry_signal"),
            age_conf=display_enum(str(age.get("age_confidence") or entry_signal.get("confidence") or "insufficient"), "confidence"),
            price_conf=display_enum(str(price_band.get("confidence") or "insufficient"), "confidence"),
            sourcing=display_enum(str(capability.get("sourcing_fit") or "unknown"), "capability"),
            top3=self._format_percent_metric(demand.get("top3_sales_share")),
            ratio=self._format_metric(demand.get("mean_median_ratio")),
        )
        has_angles = bool(angles.get("product_angle") and angles.get("scene_angle") and angles.get("content_angle"))
        action_condition = (
            "下一批需同时满足：可行动新品信号至少达到“新品进入信号中等”；"
            "上架时间置信度达到“中”或以上；价格带置信度达到“中”或以上；供应链匹配不低于“中”；差异化切口可执行"
            "{angle_suffix}。"
        ).format(angle_suffix="" if has_angles else "（当前仍需补具体产品/场景/内容切口）")
        block_condition = (
            "若仍为“新品进入信号弱 / 信号不明确 / 老品占位明显”，或上架时间置信度、价格带置信度不足，"
            "或能力匹配/差异化切口继续不清晰，则不可转入验证。"
        )
        return current_signal, action_condition, block_condition

    def _evaluate_consistency(
        self,
        cards: List[Dict[str, Any]],
        decision_summary: Dict[str, Any],
        reverse_signals: Dict[str, Any],
        cross_system: Dict[str, Any],
    ) -> Dict[str, List[str]]:
        errors: List[str] = []
        warnings: List[str] = []
        enter_names = {
            str(item.get("style_cluster") or "")
            for item in decision_summary.get(SUMMARY_BUCKET_ENTER, {}).get("all_items", [])
        }
        hidden_risk_names = {str(item.get("direction_name") or "") for item in reverse_signals.get("hidden_risks", [])}
        conflict = sorted(name for name in (enter_names & hidden_risk_names) if name)
        if conflict:
            errors.append(
                "严重自相矛盾：方向 {names} 同时进入旧版“进入”桶和“表面光鲜但暗藏风险”。".format(
                    names="、".join(conflict)
                )
            )

        top_count_cards = sorted(
            cards,
            key=lambda item: (-int(item.get("direction_item_count") or 0), str(item.get("style_cluster") or "")),
        )[:3]
        mentioned_names = set()
        for bucket_name in (SUMMARY_BUCKET_ENTER, SUMMARY_BUCKET_WATCH, SUMMARY_BUCKET_AVOID):
            mentioned_names.update(
                str(item.get("style_cluster") or "")
                for item in decision_summary.get(bucket_name, {}).get("all_items", [])
            )
        for item in reverse_signals.get("hidden_risks", []):
            mentioned_names.add(str(item.get("direction_name") or ""))
        for item in reverse_signals.get("hidden_opportunities", []):
            mentioned_names.add(str(item.get("direction_name") or ""))
        for item in cross_system.get("content_route_recommendations", []):
            mentioned_names.add(str(item.get("direction_name") or ""))
        for card in top_count_cards:
            direction_name = str(card.get("style_cluster") or "")
            if direction_name and direction_name not in mentioned_names:
                warnings.append("商品数前三的方向 {name} 在本次报告中没有被任何结论区块提及。".format(name=direction_name))

        for card in cards:
            blocked_error = str(card.get("blocked_conflict_error") or "")
            if blocked_error:
                errors.append(
                    "方向 {name} 的 enter/avoid 判断在当前 confidence 下冲突，规则码：{code}。".format(
                        name=str(card.get("style_cluster") or ""),
                        code=blocked_error,
                    )
                )
            blocked_warning = str(card.get("blocked_conflict_warning") or "")
            if blocked_warning:
                warnings.append(
                    "方向 {name} 存在次级风险信号，但已按 confidence 保留当前决策。".format(
                        name=str(card.get("style_cluster") or ""),
                    )
                )
            if str(card.get("direction_tier") or "") != "crowded":
                continue
            direction_name = str(card.get("style_cluster") or "")
            if str(card.get("summary_bucket") or "") == SUMMARY_BUCKET_ENTER:
                continue
            if str(card.get("summary_bucket") or "") == SUMMARY_BUCKET_AVOID:
                continue
            if direction_name not in hidden_risk_names:
                warnings.append("crowded 方向 {name} 没有进入“建议避开”或“暗藏风险”区块。".format(name=direction_name))
        return {
            "warnings": warnings,
            "errors": errors,
        }

    def _default_copy(self, card: Dict[str, Any]) -> Dict[str, str]:
        return {
            "rationale_one_line": "{family}内{tier}层级方向，销量中位数 {sales}，视频密度 {video_density}。".format(
                family=card.get("direction_family", "other"),
                tier=card.get("direction_tier", ""),
                sales=card.get("direction_sales_median_7d", 0.0),
                video_density=card.get("direction_video_density_avg", 0.0),
            ),
            "intra_family_comparison": "在{family}内销量位次 {sales_rank}，视频密度位次 {video_rank}。".format(
                family=card.get("direction_family", "other"),
                sales_rank=card.get("sales_rank_in_family", "-/-"),
                video_rank=card.get("video_rank_in_family", "-/-"),
            ),
            "risk_note": "若投放不达预期，最可能原因为{risk}。".format(risk=card.get("primary_risk_factor", "方向验证仍需补充样本")),
        }

    def _confidence_display(self, card: Dict[str, Any]) -> str:
        level = str(card.get("decision_confidence") or "low")
        tags = list(card.get("confidence_reason_tags") or [])
        source = str(card.get("content_efficiency_source") or "missing")
        if source == "proxy":
            return "{level}（内容效率信号来自代理指标，置信度已下调）".format(level=level)
        if source == "missing":
            return "{level}（内容效率信号缺失，判断保守）".format(level=level)
        if "signal_conflict" in tags:
            return "{level}（信号冲突：高销量与高拥挤并存）".format(level=level)
        if "sample_low" in tags:
            return "{level}（样本不足）".format(level=level)
        return level

    def _augment_risk_note(self, card: Dict[str, Any], risk_note: str) -> str:
        text = str(risk_note or "").strip()
        source = str(card.get("content_efficiency_source") or "missing")
        if source == "proxy" and "代理指标" not in text:
            text = "{text} 内容效率信号来自代理指标，置信度已下调。".format(text=text).strip()
        elif source == "missing" and "信号缺失" not in text:
            text = "{text} 内容效率信号缺失，当前判断更保守。".format(text=text).strip()
        return re.sub(r"\s+", " ", text).strip()

    def _format_percent(self, value: float) -> str:
        return "{value:.1f}%".format(value=float(value or 0.0) * 100.0)

    def _is_data_complete(self, card: Dict[str, Any]) -> bool:
        required_keys = [
            "direction_item_count",
            "direction_sales_median_7d",
            "direction_video_density_avg",
            "direction_creator_density_avg",
        ]
        for key in required_keys:
            value = card.get(key)
            if value is None:
                return False
        return True


class MarketInsightDirectionCopyRenderer(object):
    def __init__(self, prompt_path: Path, hermes_bin: Path, timeout_seconds: int, command_runner):
        self.prompt_path = Path(prompt_path)
        self.hermes_bin = Path(hermes_bin)
        self.timeout_seconds = int(timeout_seconds)
        self.command_runner = command_runner

    def render(self, context: Dict[str, Any]) -> Dict[str, str]:
        if not self.hermes_bin.exists() or not self.prompt_path.exists():
            raise RuntimeError("LLM 渲染依赖缺失")
        prompt_text = self.prompt_path.read_text(encoding="utf-8").strip()
        query = "{prompt}\n\n【输入数据】\n{payload}".format(
            prompt=prompt_text,
            payload=json.dumps(context, ensure_ascii=False, indent=2),
        )
        completed = self.command_runner(
            [
                str(self.hermes_bin),
                "chat",
                "-Q",
                "--source",
                "tool",
                "-q",
                query,
            ],
            capture_output=True,
            text=True,
            timeout=self.timeout_seconds,
            check=False,
        )
        if getattr(completed, "returncode", 1) != 0:
            stderr = getattr(completed, "stderr", "") or getattr(completed, "stdout", "")
            raise RuntimeError("Hermes 报告文案渲染失败: {error}".format(error=stderr.strip()))
        response_text = self._extract_response_text(getattr(completed, "stdout", ""))
        payload = json.loads(response_text)
        if not isinstance(payload, dict):
            raise RuntimeError("Hermes 报告文案返回必须是 JSON object")
        result = {
            "rationale_one_line": str(payload.get("rationale_one_line", "") or "").strip(),
            "intra_family_comparison": str(payload.get("intra_family_comparison", "") or "").strip(),
            "risk_note": str(payload.get("risk_note", "") or "").strip(),
        }
        result["risk_note"] = self._normalize_risk_note(result["risk_note"])
        for key, limit in {"rationale_one_line": 50, "intra_family_comparison": 80, "risk_note": 60}.items():
            if not result[key]:
                raise RuntimeError("{key} 为空".format(key=key))
            if len(result[key]) > limit:
                result[key] = self._shorten_text(result[key], limit)
            if not result[key]:
                raise RuntimeError("{key} 压缩后为空".format(key=key))
        return result

    def _extract_response_text(self, stdout: str) -> str:
        text = (stdout or "").strip()
        if not text:
            return ""
        if "\nsession_id:" in text:
            text = text.rsplit("\nsession_id:", 1)[0].strip()
        decoder = JSONDecoder()
        for index, char in enumerate(text):
            if char not in "{[":
                continue
            try:
                _, end = decoder.raw_decode(text[index:])
            except ValueError:
                continue
            return text[index : index + end].strip()
        return text

    def _shorten_text(self, text: str, limit: int) -> str:
        normalized = str(text or "").strip()
        if len(normalized) <= limit:
            return normalized
        sentence_endings = "。！？!?"
        candidate = normalized[:limit]
        last_sentence_end = max(candidate.rfind(mark) for mark in sentence_endings)
        if last_sentence_end >= 0:
            shortened = candidate[: last_sentence_end + 1].strip()
            if shortened:
                return shortened
        comma_like = "，、；;,.：:"
        last_clause_end = max(candidate.rfind(mark) for mark in comma_like)
        if last_clause_end >= 0:
            shortened = candidate[:last_clause_end].rstrip("，、；;,.：: ")
            if shortened:
                return "{text}。".format(text=shortened)
        shortened = candidate.rstrip("，、；;,.。！？!？：: ")
        if not shortened:
            shortened = candidate.strip()
        if shortened and shortened[-1] not in sentence_endings:
            shortened = "{text}。".format(text=shortened)
        return shortened

    def _normalize_risk_note(self, text: str) -> str:
        normalized = str(text or "").strip()
        if not normalized:
            return normalized
        normalized = re.sub(r"^(当前路径为|当前建议)(复刻优先|原创优先|复刻与原创并行|优先复刻验证|优先原创验证)[，,、]\s*", "", normalized)
        normalized = re.sub(r"^(内容路线|路线建议)[:：]\s*", "", normalized)
        return normalized.strip()


class MarketInsightMatrixObservationRenderer(MarketInsightDirectionCopyRenderer):
    def render(self, context: Dict[str, Any]) -> List[str]:
        if not self.hermes_bin.exists() or not self.prompt_path.exists():
            raise RuntimeError("LLM 矩阵观察渲染依赖缺失")
        prompt_text = self.prompt_path.read_text(encoding="utf-8").strip()
        query = "{prompt}\n\n【输入数据】\n{payload}".format(
            prompt=prompt_text,
            payload=json.dumps(context, ensure_ascii=False, indent=2),
        )
        completed = self.command_runner(
            [
                str(self.hermes_bin),
                "chat",
                "-Q",
                "--source",
                "tool",
                "-q",
                query,
            ],
            capture_output=True,
            text=True,
            timeout=self.timeout_seconds,
            check=False,
        )
        if getattr(completed, "returncode", 1) != 0:
            stderr = getattr(completed, "stderr", "") or getattr(completed, "stdout", "")
            raise RuntimeError("Hermes 矩阵观察渲染失败: {error}".format(error=stderr.strip()))
        response_text = self._extract_response_text(getattr(completed, "stdout", ""))
        payload = json.loads(response_text)
        if not isinstance(payload, dict):
            raise RuntimeError("Hermes 矩阵观察返回必须是 JSON object")
        observations = payload.get("observations", [])
        if not isinstance(observations, list):
            raise RuntimeError("observations 必须是 list")
        cleaned: List[str] = []
        for item in observations[:3]:
            text = str(item or "").strip()
            if not text:
                continue
            cleaned.append(self._shorten_text(text, 120))
        if not cleaned:
            raise RuntimeError("observations 为空")
        return cleaned
