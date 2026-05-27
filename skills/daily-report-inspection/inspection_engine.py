#!/usr/bin/env python3
"""
巡检规则引擎

基于解析的结构化日报数据，应用巡检规则，产出异常/问题列表。
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional

SKILL_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SKILL_DIR / "config.json"


class InspectionFinding:
    """单条巡检发现"""
    def __init__(
        self,
        category: str,
        label: str,
        severity: str,  # critical / warning / info
        business_line: str,
        detail: str,
        suggestion: str = "",
    ):
        self.category = category
        self.label = label
        self.severity = severity
        self.business_line = business_line
        self.detail = detail
        self.suggestion = suggestion

    def to_dict(self) -> dict:
        return {
            "category": self.category,
            "label": self.label,
            "severity": self.severity,
            "business_line": self.business_line,
            "detail": self.detail,
            "suggestion": self.suggestion,
        }


class InspectionEngine:
    """巡检规则引擎"""

    def __init__(self, config_path: Path = CONFIG_PATH):
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = json.load(f)
        self.rules = self.config.get("inspection", {})

    def inspect(
        self,
        parsed_reports: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        对所有日报执行巡检

        Args:
            parsed_reports: 已解析的日报列表，每个包含 business_line + 结构化字段

        Returns:
            巡检结果 dict，包含 findings 列表和统计信息
        """
        all_findings = []
        all_progress = []

        for report in parsed_reports:
            biz = report.get("business_line", "未知")
            content = report.get("content", "")

            if not report.get("found"):
                all_findings.append(InspectionFinding(
                    category="report_missing",
                    label="日报缺失",
                    severity="critical",
                    business_line=biz,
                    detail=f"{biz}日报未找到: {report.get('match_desc', '')}",
                    suggestion="请检查日报是否已填写、标题格式是否正确、文档是否在固定目录下",
                ).to_dict())
                continue

            if report.get("match_level", 1) >= 3:
                all_findings.append(InspectionFinding(
                    category="report_format",
                    label="标题不规范",
                    severity="warning",
                    business_line=biz,
                    detail=f"{biz}日报标题不规范（{report.get('match_desc', '')}）",
                    suggestion="建议统一日报标题格式，如 '0520 配饰'",
                ).to_dict())

            parsed = report.get("parsed", {})

            # 1. 短视频生成巡检
            gen = parsed.get("short_video_generation", {})
            self._inspect_video_generation(biz, gen, all_findings)

            # 2. 视频发布巡检
            publish = parsed.get("video_publish", {})
            self._inspect_video_publish(biz, publish, all_findings)

            # 3. 上品巡检
            listing = parsed.get("product_listing", {})
            self._inspect_product_listing(biz, listing, gen, publish, all_findings, all_progress)

            # 4. 达人建联巡检
            outreach = parsed.get("creator_outreach", {})
            self._inspect_creator_outreach(biz, outreach, all_findings)

            # 5. 样品审批巡检
            sample = parsed.get("sample_approval", {})
            self._inspect_sample_approval(biz, sample, all_findings)

        # 跨业务线综合分析
        self._cross_business_analysis(all_findings, parsed_reports)

        # 分类汇总
        issues = [f for f in all_findings if f["severity"] in ("critical", "warning")]
        info_items = [f for f in all_findings if f["severity"] == "info"]

        return {
            "findings": all_findings,
            "progress_items": all_progress,
            "issues": issues,
            "info_items": info_items,
            "total_issues": len(issues),
            "has_critical": any(f["severity"] == "critical" for f in all_findings),
        }

    def _inspect_video_generation(
        self, biz: str, gen: dict, findings: list
    ):
        """短视频生成巡检"""
        gen_rules = self.rules.get("video_generation", {})
        gen_count = gen.get("generated_count")
        pending = gen.get("pending_tomorrow_count")
        direct = gen.get("directly_usable_count")
        modified = gen.get("modified_usable_count")
        unusable = gen.get("unusable_count")
        failure_reasons = gen.get("failure_reasons", [])

        # 规则1: 生成积压
        if pending is not None and pending >= gen_rules.get("backlog_threshold", 10):
            if gen_count is None or gen_count == 0:
                findings.append(InspectionFinding(
                    category="video_gen_backlog",
                    label="视频生成积压风险",
                    severity="warning",
                    business_line=biz,
                    detail=f"{biz}今日短视频生成暂无，但明日已有{pending}个视频待生成，存在生成任务积压风险",
                    suggestion=f"优先处理{biz}{pending}个待生成视频，避免积压继续扩大",
                ).to_dict())

        # 规则2: 直接可用率偏低
        if gen_count is not None and gen_count > 0:
            if direct is not None:
                rate = direct / gen_count
                if rate < gen_rules.get("direct_usable_rate_threshold", 0.4):
                    findings.append(InspectionFinding(
                        category="video_usable_rate_low",
                        label="短视频直接可用率偏低",
                        severity="warning",
                        business_line=biz,
                        detail=f"{biz}今日生成{gen_count}个视频，直接可用{direct}个，直接可用率约{int(rate*100)}%",
                        suggestion="需要关注生成质量，排查类目识别和产品锚点问题",
                    ).to_dict())

            # 规则3: 综合可用率偏低
            if direct is not None:
                combined = direct + (modified or 0)
                rate = combined / gen_count
                if rate < gen_rules.get("combined_usable_rate_threshold", 0.6):
                    findings.append(InspectionFinding(
                        category="video_combined_rate_low",
                        label="短视频综合可用率偏低",
                        severity="warning",
                        business_line=biz,
                        detail=f"{biz}今日生成{gen_count}个视频，直接可用{direct}个，修改后可用{modified or 0}个，综合可用率约{int(rate*100)}%",
                        suggestion="需要排查生成链路中的类目识别和商品目标匹配问题",
                    ).to_dict())

        # 规则4: 生成链路跑偏
        runaway_kws = gen_rules.get("runaway_keywords", [])
        matched_reasons = [r for r in failure_reasons if any(kw in r for kw in runaway_kws)]
        if matched_reasons:
            findings.append(InspectionFinding(
                category="video_gen_runaway",
                label="生成链路跑偏",
                severity="warning",
                business_line=biz,
                detail=f"{biz}视频生成出现异常：{'、'.join(matched_reasons[:3])}",
                suggestion="建议优先排查产品锚点、类目识别和商品描述是否缺失",
            ).to_dict())

    def _inspect_video_publish(
        self, biz: str, publish: dict, findings: list
    ):
        """视频发布巡检"""
        stores = publish.get("publish_count_by_store", [])
        failed = publish.get("failed_count")

        # 规则1: 发布断档
        if stores:
            for s in stores:
                if s["count"] == 0:
                    findings.append(InspectionFinding(
                        category="publish_gap",
                        label="内容发布断档",
                        severity="warning",
                        business_line=biz,
                        detail=f"{biz}{s['store_name']}今日视频发布为0",
                        suggestion=f"需要确认{s['store_name']}是素材不足、节奏暂停还是执行漏发",
                    ).to_dict())

        # 规则2: 发布失败
        if failed is not None and failed > 0:
            findings.append(InspectionFinding(
                category="publish_failed",
                label="发布失败",
                severity="warning",
                business_line=biz,
                detail=f"{biz}今日视频发布失败{failed}个",
                suggestion="需要记录失败原因，避免重复失败",
            ).to_dict())

    def _inspect_product_listing(
        self, biz: str, listing: dict, gen: dict, publish: dict,
        findings: list, progress: list
    ):
        """选品及上品巡检"""
        listed = listing.get("listed_count")
        scope = listing.get("product_scope", "")

        # 有效推进记录
        if listed is not None and listed > 0:
            scope_text = f"（{scope}）" if scope else ""
            progress.append({
                "business_line": biz,
                "category": "listing_progress",
                "detail": f"{biz}：{scope_text}上品{listed}个，供给侧有推进",
            })

        # 上品与内容承接不匹配
        if listed is not None and listed > 5:
            gen_count = gen.get("generated_count", 0) or 0
            publish_total = publish.get("total_publish_count", 0) or 0
            if gen_count < 3 and publish_total < 3:
                findings.append(InspectionFinding(
                    category="listing_content_mismatch",
                    label="上品和内容承接不匹配",
                    severity="warning",
                    business_line=biz,
                    detail=f"{biz}{scope}上品{listed}个，但短视频生成/发布明显不足",
                    suggestion="上品后需要主图、短视频和发布动作承接，否则难以转化为销售验证",
                ).to_dict())

    def _inspect_creator_outreach(
        self, biz: str, outreach: dict, findings: list
    ):
        """达人建联巡检"""
        count = outreach.get("outreach_count")
        status = outreach.get("outreach_status", "")
        notes = outreach.get("notes", "")

        # 规则1: 动作断档
        if status == "未进行" or count == 0:
            findings.append(InspectionFinding(
                category="outreach_gap",
                label="达人动作断档",
                severity="warning",
                business_line=biz,
                detail=f"{biz}今日未进行达人建联",
                suggestion="如果当前阶段希望通过达人起量，需要补充建联动作",
            ).to_dict())
            return

        # 规则2: 建联口径异常
        threshold = self.rules.get("creator_outreach", {}).get("anomaly_threshold", 500)
        if count is not None and count > threshold:
            findings.append(InspectionFinding(
                category="outreach_count_anomaly",
                label="建联口径需确认",
                severity="info",
                business_line=biz,
                detail=f"{biz}日报建联数字{count}人明显偏大，{notes or '需要确认这是实际新增触达还是候选达人池数量'}",
                suggestion="建议在日报中区分'实际建联'和'候选达人池'两个口径",
            ).to_dict())

    def _inspect_sample_approval(
        self, biz: str, sample: dict, findings: list
    ):
        """样品审批巡检"""
        apply_count = sample.get("applicant_count")
        approved = sample.get("approved_count")

        # 规则: 有申请但批出为0
        if apply_count is not None and apply_count > 0:
            if approved is None or approved == 0:
                findings.append(InspectionFinding(
                    category="sample_approve_zero",
                    label="样品审批转化为0",
                    severity="warning",
                    business_line=biz,
                    detail=f"{biz}新增样品申请{apply_count}人，但批出样品{approved or 0}人，审批转化异常",
                    suggestion="需要确认审批标准、达人质量或样品策略是否卡住",
                ).to_dict())

    def _cross_business_analysis(
        self, findings: list, reports: List[dict]
    ):
        """跨业务线综合分析"""
        # 检查多个业务线是否同时出现样品审批卡点
        sample_zeros = [
            f for f in findings
            if f["category"] == "sample_approve_zero"
        ]
        if len(sample_zeros) >= 2:
            findings.append(InspectionFinding(
                category="sample_chain_blocked",
                label="样品审批链路集中卡点",
                severity="critical",
                business_line="全局",
                detail=f"{'、'.join(f['business_line'] for f in sample_zeros)}均出现样品申请但批出为0",
                suggestion="可能不是单店问题，而是整体样品审批策略或执行节奏卡住，建议统一复核审批标准",
            ).to_dict())

        # 检查是否有多条业务线发布为0
        publish_gaps = [
            f for f in findings
            if f["category"] == "publish_gap"
        ]
        if len(publish_gaps) >= 3:
            findings.append(InspectionFinding(
                category="publish_chain_blocked",
                label="多业务线发布断档",
                severity="critical",
                business_line="全局",
                detail=f"多个店铺/业务线出现视频发布为0的情况",
                suggestion="需要统一确认内容供给和发布排期是否存在系统性问题",
            ).to_dict())


if __name__ == "__main__":
    engine = InspectionEngine()
    sample_reports = [
        {
            "business_line": "配饰",
            "found": True,
            "match_level": 1,
            "match_desc": "精准匹配: '0520 配饰'",
            "content": "...",
            "parsed": {
                "short_video_generation": {
                    "generated_count": 0,
                    "pending_tomorrow_count": 24,
                },
                "video_publish": {
                    "publish_count_by_store": [
                        {"store_name": "马来配饰", "count": 1},
                        {"store_name": "越南配饰", "count": 0},
                        {"store_name": "越南发夹", "count": 6},
                    ],
                    "failed_count": 0,
                },
                "product_listing": {"listed_count": 31, "product_scope": "泰国本土店"},
                "creator_outreach": {"outreach_count": 7004},
                "sample_approval": {"applicant_count": 19, "approved_count": 1},
            },
        },
        {
            "business_line": "女装",
            "found": True,
            "match_level": 1,
            "match_desc": "精准匹配: '0520 女装'",
            "content": "...",
            "parsed": {
                "short_video_generation": {
                    "generated_count": 9,
                    "directly_usable_count": 3,
                    "modified_usable_count": 2,
                    "unusable_count": 4,
                },
                "video_publish": {
                    "publish_count_by_store": [
                        {"store_name": "泰国女装", "count": 4},
                        {"store_name": "女装店", "count": 3},
                    ],
                },
                "product_listing": {"listed_count": None},
                "creator_outreach": {"outreach_count": 0, "outreach_status": "未进行"},
                "sample_approval": {"applicant_count": 12, "approved_count": 0},
            },
        },
    ]

    result = engine.inspect(sample_reports)
    print(json.dumps(result["findings"], ensure_ascii=False, indent=2))
