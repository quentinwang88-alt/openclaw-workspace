#!/usr/bin/env python3
"""
巡检卡片生成器

基于巡检结果生成 Markdown 格式的巡检摘要卡片。
输出控制在 500-800 字，结论先行，抓关键问题。
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

SKILL_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SKILL_DIR / "config.json"


class SummaryBuilder:
    """巡检卡片生成器"""

    def __init__(self, config_path: Path = CONFIG_PATH):
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = json.load(f)
        limits = self.config.get("inspection", {}).get("summary", {})
        self.max_progress = limits.get("max_progress_items", 5)
        self.max_issues = limits.get("max_issue_items", 5)
        self.max_actions = limits.get("max_action_items", 5)
        self.max_interventions = limits.get("max_intervention_items", 3)

    def build(
        self,
        inspection_result: Dict[str, Any],
        reports: List[Dict[str, Any]],
        target_date: Optional[datetime] = None,
    ) -> str:
        """
        生成巡检摘要 Markdown 卡片

        Args:
            inspection_result: 巡检引擎输出
            reports: 原始日报列表
            target_date: 巡检日期

        Returns:
            Markdown 格式的巡检卡片
        """
        if target_date is None:
            target_date = datetime.now()

        date_str = target_date.strftime("%m%d")
        findings = inspection_result.get("findings", [])
        issues = inspection_result.get("issues", [])
        progress = inspection_result.get("progress_items", [])

        # 汇总关键数据
        all_issues = [f for f in issues if f["severity"] == "warning"]
        critical_issues = [f for f in issues if f["severity"] == "critical"]
        info_items = [f for f in findings if f["severity"] == "info"]

        lines = []

        # 标题
        lines.append(f"# {date_str} 日报巡检摘要")
        lines.append("")

        # ========== 一、今日有效进展 ==========
        lines.append("## 一、今日有效进展")
        lines.append("")
        progress_items = self._build_progress(progress, reports)
        if progress_items:
            for i, item in enumerate(progress_items[: self.max_progress], 1):
                lines.append(f"{i}. {item}")
        else:
            lines.append("今日各业务线无明显有效推进事项。")
        lines.append("")

        # ========== 二、需要重点关注的问题 ==========
        lines.append("## 二、需要重点关注的问题")
        lines.append("")
        issue_items = self._build_issues(all_issues)
        if issue_items:
            for i, item in enumerate(issue_items[: self.max_issues], 1):
                lines.append(f"{i}. {item}")
        else:
            lines.append("今日未发现明显异常，各业务线正常推进。")
        lines.append("")

        # ========== 三、明日建议盯住的动作 ==========
        lines.append("## 三、明日建议盯住的动作")
        lines.append("")
        action_items = self._build_actions(all_issues + info_items, reports)
        if action_items:
            for i, item in enumerate(action_items[: self.max_actions], 1):
                lines.append(f"{i}. {item}")
        else:
            lines.append("明日继续正常推进即可。")
        lines.append("")

        # ========== 四、老板需要介入吗 ==========
        lines.append("## 四、老板需要介入吗")
        lines.append("")
        inter_items = self._build_interventions(critical_issues + all_issues)
        if inter_items:
            lines.append(f"建议介入 {len(inter_items)} 件事：")
            lines.append("")
            for i, item in enumerate(inter_items[: self.max_interventions], 1):
                lines.append(f"{i}. {item}")
        else:
            lines.append("不需要明显介入，明日正常跟进即可。")
        lines.append("")

        # ========== 附加：日报状态 ==========
        missing = [r for r in reports if not r.get("found")]
        if missing:
            lines.append("---")
            lines.append(f"⚠️ *日报缺失：{'、'.join(r['business_line'] for r in missing)}*")

        summary = "\n".join(lines)
        return summary

    def _build_progress(
        self, progress: list, reports: list
    ) -> List[str]:
        """构建有效进展列表"""
        items = list(progress)

        # 从日报中补充进展信息
        for report in reports:
            if not report.get("found"):
                continue
            biz = report["business_line"]
            parsed = report.get("parsed", {})

            # 视频发布有推进
            publish = parsed.get("video_publish", {})
            stores = publish.get("publish_count_by_store", [])
            if stores:
                active_stores = [s for s in stores if s["count"] > 0]
                if active_stores:
                    store_desc = "、".join(
                        f"{s['store_name']}发布{s['count']}个" for s in active_stores[:2]
                    )
                    items.append({
                        "business_line": biz,
                        "detail": f"{biz}：{store_desc}，内容发布有推进",
                    })

            # 达人建联有推进（合理数量）
            outreach = parsed.get("creator_outreach", {})
            oc = outreach.get("outreach_count")
            if oc is not None and 0 < oc <= 500:
                items.append({
                    "business_line": biz,
                    "detail": f"{biz}：达人建联{oc}人，达人渠道有推进",
                })

            # 样品审批有批出
            sample = parsed.get("sample_approval", {})
            apv = sample.get("approved_count")
            if apv is not None and apv > 0:
                items.append({
                    "business_line": biz,
                    "detail": f"{biz}：批出样品{apv}人，样品链路正常推进",
                })

        return [p["detail"] for p in items][: self.max_progress]

    def _build_issues(self, issues: list) -> List[str]:
        """构建问题列表"""
        items = []
        for f in issues:
            detail = f["detail"]
            biz = f["business_line"]
            if biz and biz != "全局" and not detail.startswith(biz):
                detail = f"{biz}：{detail}"
            items.append(detail)
        return items[: self.max_issues]

    def _build_actions(self, issues: list, reports: list) -> List[str]:
        """构建明日建议动作"""
        actions = []
        for f in issues:
            suggestion = f.get("suggestion", "")
            if suggestion and suggestion not in actions:
                actions.append(suggestion)
        return actions[: self.max_actions]

    def _build_interventions(self, critical_and_warning: list) -> List[str]:
        """构建老板需介入事项"""
        # 优先取 critical 级别
        critical = [f for f in critical_and_warning if f["severity"] == "critical"]
        interventions = []

        for f in critical:
            interventions.append(f["detail"])

        # 如果没有 critical，取前几条 warning 中确实需要决策的
        if not interventions:
            decision_issues = [
                f for f in critical_and_warning
                if f["category"] in (
                    "sample_chain_blocked", "video_gen_runaway",
                    "publish_chain_blocked", "sample_approve_zero",
                )
            ]
            for f in decision_issues[: self.max_interventions]:
                interventions.append(f["detail"])

        return interventions[: self.max_interventions]


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(SKILL_DIR))
    from inspection_engine import InspectionEngine

    # Test with sample data
    sample_reports = [
        {
            "business_line": "配饰",
            "found": True,
            "match_level": 1,
            "parsed": {
                "short_video_generation": {"generated_count": 0, "pending_tomorrow_count": 24},
                "video_publish": {
                    "publish_count_by_store": [
                        {"store_name": "马来配饰", "count": 1},
                        {"store_name": "越南配饰", "count": 0},
                        {"store_name": "越南发夹", "count": 6},
                    ]
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
            "parsed": {
                "short_video_generation": {
                    "generated_count": 9, "directly_usable_count": 3,
                    "modified_usable_count": 2, "unusable_count": 4,
                },
                "video_publish": {
                    "publish_count_by_store": [
                        {"store_name": "泰国女装", "count": 4},
                        {"store_name": "女装店", "count": 3},
                    ]
                },
                "product_listing": {},
                "creator_outreach": {"outreach_count": 0, "outreach_status": "未进行"},
                "sample_approval": {"applicant_count": 12, "approved_count": 0},
            },
        },
    ]

    engine = InspectionEngine()
    result = engine.inspect(sample_reports)
    builder = SummaryBuilder()
    summary = builder.build(result, sample_reports)
    print(summary)
