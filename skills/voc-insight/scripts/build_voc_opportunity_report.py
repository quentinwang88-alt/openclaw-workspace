#!/usr/bin/env python3
"""Build standalone VOC opportunity-reference Markdown and CSV artifacts."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


SKILL_DIR = Path(__file__).resolve().parents[1]


CONFIDENCE_LABELS = {
    "signal_only": "线索",
    "emerging_opportunity": "新兴机会",
    "direction_candidate": "方向候选",
    "stable_reference": "稳定参考",
}

TYPE_LABELS = {
    "established_preference": "稳定偏好",
    "pain_gap": "痛点缺口",
    "feature_upgrade": "产品升级",
    "usage_scenario": "场景机会",
    "risk_only": "风险观察",
}


def load_json(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def unique(values: Iterable[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def evidence_examples(card: Dict[str, Any], evidence_map: Dict[str, Dict[str, Any]], limit: int = 3) -> List[Dict[str, Any]]:
    rows = [evidence_map[ref] for ref in card.get("evidence_refs") or [] if ref in evidence_map]
    result: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (row.get("product_id"), row.get("source_text"))
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
        if len(result) >= limit:
            break
    return result


def build_markdown(payload: Dict[str, Any]) -> str:
    cards = payload.get("opportunity_cards") or []
    atomic = payload.get("atomic_evidence") or []
    evidence_map = {row["atomic_evidence_id"]: row for row in atomic}
    summary = payload.get("summary") or {}
    quality = payload.get("quality_gate") or {}
    assessment = payload.get("selection_guidance_assessment") or {}
    ratios = assessment.get("ratios") or {}
    counts = assessment.get("counts") or {}
    source_batches = payload.get("source_batches") or [payload.get("batch_id")]
    limitations = unique(
        limitation for card in cards for limitation in card.get("limitations") or []
    )
    lines = [
        "# VOC 产品机会与数据置信度评估",
        "",
        "- 分析批次：`{}`".format(payload.get("batch_id")),
        "- 来源批次：{}".format("、".join("`{}`".format(item) for item in source_batches if item)),
        "- 范围：`{}` / `{}`".format(payload.get("market"), payload.get("category_key")),
        "- 数据：{} 条有效评论 / {} 条原子证据 / {} 个证据商品".format(
            summary.get("valid_review_count", 0), summary.get("atomic_evidence_count", 0),
            summary.get("evidence_product_count", 0),
        ),
        "- 机会卡：{} 张；结构质量门：**{}**".format(
            len(cards), "通过" if quality.get("passed") else "未通过"
        ),
        "- 使用边界：本报告用于消费者需求、产品方向和规格风险参考，不形成具体商品决策。",
        "",
        "## 结论：目前能否用于选品指导",
        "",
        "- **结论：{}**".format(
            "可以进入选品验证" if assessment.get("can_support_selection_guidance") else "暂不能作为独立的商品级选品依据"
        ),
        "- 综合置信度：**{} / 100（{}）**".format(
            assessment.get("overall_score", 0), assessment.get("overall_confidence", "未知")
        ),
        "- 可用层级：{}。".format(assessment.get("guidance_level") or "仅支持方向参考"),
        "- 注意：结构质量门通过，只表示分析产物结构完整、证据可追溯；不等于样本已经达到选品决策门槛。",
        "",
        "### 置信度指标",
        "",
        "| 指标 | 当前值 | 判断 |",
        "|---|---:|---|",
        "| 原始VOC非空率 | {:.1%} | 文本完整性好 |".format(ratios.get("nonblank_rate", 0)),
        "| 原始VOC去重后保留率 | {:.1%} | 暂未发现明显重复灌水 |".format(ratios.get("dedup_rate", 0)),
        "| 有VOC商品覆盖率 | {:.1%}（{}/{}） | 低于60%选品参考门槛 |".format(
            ratios.get("product_voc_coverage", 0), counts.get("voc_products", 0), counts.get("target_products", 0)
        ),
        "| VOC目标深度完成率 | {:.1%}（{}/{}） | 证据深度仍不足 |".format(
            ratios.get("voc_depth_coverage", 0), counts.get("raw_voc", 0), counts.get("target_voc", 0)
        ),
        "| 新品池VOC覆盖率 | {:.1%}（{}/{}） | 新品方向尚无有效证据 |".format(
            ratios.get("new_pool_voc_coverage", 0), counts.get("new_voc_products", 0), counts.get("new_target_products", 0)
        ),
        "| 核心类目纯度 | {:.1%}（{}个有效商品） | 存在非核心假发/接发商品污染 |".format(
            ratios.get("category_purity", 0), counts.get("relevant_products", 0)
        ),
        "| 有效评论率 | {:.1%} | 已剔除非核心商品评论 |".format(ratios.get("valid_review_rate", 0)),
        "| 有效评论可打标率 | {:.1%} | 大部分有效评论可进入主题分析 |".format(ratios.get("tagged_valid_rate", 0)),
        "",
        "### 当前阻塞项",
        "",
    ]
    for blocker in assessment.get("blockers") or ["未提供选品置信度评估"]:
        lines.append("- {}".format(blocker))
    lines.extend([
        "",
        "### 建议使用方式",
        "",
        "- 可以用于：{}。".format("、".join(assessment.get("allowed_uses") or [])),
        "- 暂不用于：{}。".format("、".join(assessment.get("not_allowed_uses") or [])),
        "- 实际选品时，应把本报告作为需求假设输入，再结合市场增速、竞争强度、价格带、供应可得性和小样验证共同判断。",
        "",
        "## 机会总览",
        "",
        "| 机会 | 类型 | 置信度 | 商品/证据/批次 | 核心产品含义 |",
        "|---|---|---|---:|---|",
    ])
    for card in cards:
        metrics = card.get("metrics") or {}
        lines.append("| {} | {} | `{}` | {}/{}/{} | {} |".format(
            card.get("title_zh"), TYPE_LABELS.get(card.get("opportunity_type"), card.get("opportunity_type")),
            card.get("confidence"), metrics.get("direct_product_count", 0),
            metrics.get("direct_evidence_count", 0), metrics.get("direct_batch_count", 0),
            card.get("opportunity_hypothesis") or "—",
        ))

    lines.extend(["", "## 机会卡", ""])
    for card in cards:
        metrics = card.get("metrics") or {}
        lines.extend([
            "### {}".format(card.get("title_zh")),
            "",
            "- 类型：{}".format(TYPE_LABELS.get(card.get("opportunity_type"), card.get("opportunity_type"))),
            "- 证据强度：`{}`（{}）".format(
                card.get("confidence"), CONFIDENCE_LABELS.get(card.get("confidence"), card.get("confidence"))
            ),
            "- 覆盖：{} 个直接证据商品 / {} 条直接证据 / {} 个来源批次".format(
                metrics.get("direct_product_count", 0), metrics.get("direct_evidence_count", 0),
                metrics.get("direct_batch_count", 0),
            ),
            "- 用户任务：{}".format(card.get("user_job") or "—"),
            "- 未满足需求：{}".format(card.get("unmet_need") or "—"),
            "- 机会假设：{}".format(card.get("opportunity_hypothesis") or "—"),
            "- 必备规格：{}".format("；".join(card.get("must_have_specs") or []) or "待补证据"),
            "- 可选升级：{}".format("；".join(card.get("optional_specs") or []) or "无"),
            "- 规避项：{}".format("；".join(card.get("avoid_specs") or []) or "无"),
            "- 验证检查：{}".format("；".join(card.get("inspection_checks") or []) or "无"),
            "- 支持信号：{}".format("、".join(card.get("supporting_signal_ids") or []) or "无"),
            "- 矛盾/风险信号：{}".format("、".join(card.get("contradicting_signal_ids") or []) or "无"),
            "- 数据限制：{}".format("、".join(card.get("limitations") or []) or "无"),
            "",
            "原始证据：",
            "",
        ])
        for row in evidence_examples(card, evidence_map):
            lines.append("> [{} / {}] {}".format(
                row.get("product_form_label") or row.get("product_form"), row.get("product_id"), row.get("source_text")
            ))
            lines.append("")

    lines.extend([
        "## 规格与风险清单",
        "",
        "| 类型 | 适用形态 | 要求/检查项 | 消费者原因 |",
        "|---|---|---|---|",
    ])
    type_labels = {
        "must_have": "必备规格", "optional_upgrade": "可选升级",
        "avoid": "规避项", "inspection_check": "检查项",
    }
    for item in payload.get("spec_risk_library") or []:
        lines.append("| {} | {} | {} | {} |".format(
            type_labels.get(item.get("item_type"), item.get("item_type")),
            "、".join(item.get("product_forms") or []) or "跨形态",
            item.get("requirement_zh"), item.get("consumer_reason"),
        ))

    lines.extend([
        "",
        "## 数据限制与后续观察",
        "",
    ])
    if limitations:
        for item in limitations:
            lines.append("- `{}`".format(item))
    else:
        lines.append("- 当前无额外限制标记。")
    lines.extend([
        "- 当前为V2首次机会分析，变化状态统一为 `first_observation`。",
        "- 商品ID仅用于定位原始证据，不表示商品优先级。",
        "- 后续批次应按稳定机会ID比较新增、增强、稳定、减弱和消失。",
    ])
    return "\n".join(lines) + "\n"


def json_cell(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def write_cards_csv(path: Path, cards: Sequence[Dict[str, Any]]) -> None:
    fields = [
        "机会ID", "机会标题", "机会类型", "置信度", "产品形态", "用户任务", "未满足需求",
        "机会假设", "必备规格", "可选升级", "规避项", "检查项", "直接商品数", "直接证据数",
        "来源批次数", "最大单商品贡献", "支持信号", "风险信号", "数据限制", "证据引用",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for card in cards:
            metrics = card.get("metrics") or {}
            writer.writerow({
                "机会ID": card.get("opportunity_id"), "机会标题": card.get("title_zh"),
                "机会类型": card.get("opportunity_type"), "置信度": card.get("confidence"),
                "产品形态": "、".join(card.get("product_forms") or []), "用户任务": card.get("user_job"),
                "未满足需求": card.get("unmet_need"), "机会假设": card.get("opportunity_hypothesis"),
                "必备规格": "；".join(card.get("must_have_specs") or []),
                "可选升级": "；".join(card.get("optional_specs") or []),
                "规避项": "；".join(card.get("avoid_specs") or []),
                "检查项": "；".join(card.get("inspection_checks") or []),
                "直接商品数": metrics.get("direct_product_count"), "直接证据数": metrics.get("direct_evidence_count"),
                "来源批次数": metrics.get("direct_batch_count"), "最大单商品贡献": metrics.get("max_product_contribution"),
                "支持信号": ",".join(card.get("supporting_signal_ids") or []),
                "风险信号": ",".join(card.get("contradicting_signal_ids") or []),
                "数据限制": ",".join(card.get("limitations") or []), "证据引用": json_cell(card.get("evidence_refs") or []),
            })


def write_dict_csv(path: Path, rows: Sequence[Dict[str, Any]], fields: Sequence[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(fields), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            prepared = {key: (json_cell(value) if isinstance(value, (list, dict)) else value) for key, value in row.items()}
            writer.writerow(prepared)


def write_artifacts(payload: Dict[str, Any], output_dir: Path) -> Dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    batch_id = str(payload.get("batch_id") or "voc")
    report = output_dir / "{}_voc_opportunity_overview.md".format(batch_id)
    cards_json = output_dir / "{}_voc_opportunity_cards.json".format(batch_id)
    cards_csv = output_dir / "{}_voc_opportunity_cards.csv".format(batch_id)
    spec_csv = output_dir / "{}_voc_spec_risk_library.csv".format(batch_id)
    atomic_csv = output_dir / "{}_voc_atomic_evidence.csv".format(batch_id)
    report.write_text(build_markdown(payload), encoding="utf-8")
    cards_json.write_text(json.dumps(payload.get("opportunity_cards") or [], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_cards_csv(cards_csv, payload.get("opportunity_cards") or [])
    write_dict_csv(spec_csv, payload.get("spec_risk_library") or [], [
        "item_id", "market", "category_key", "product_forms", "item_type", "requirement_zh",
        "consumer_reason", "opportunity_ids", "evidence_refs", "confidence",
    ])
    write_dict_csv(atomic_csv, payload.get("atomic_evidence") or [], [
        "atomic_evidence_id", "evidence_id", "source_batch_id", "market", "category_key", "product_id",
        "product_title", "product_form", "product_form_label", "pool_type", "sample_pool", "evidence_scope",
        "aspect_group", "signal_tag", "polarity", "severity", "opinion_target", "opinion_text",
        "desired_outcome", "controllability", "source_text", "source_start", "source_end", "extraction_method",
        "extractor_version", "quality_flags",
    ])
    return {
        "overview_report": str(report), "opportunity_cards_json": str(cards_json),
        "opportunity_cards_csv": str(cards_csv), "spec_risk_library_csv": str(spec_csv),
        "atomic_evidence_csv": str(atomic_csv),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build standalone VOC opportunity artifacts")
    parser.add_argument("--input-json", required=True)
    parser.add_argument("--output-dir", default=str(SKILL_DIR / "output"))
    args = parser.parse_args()
    payload = load_json(Path(args.input_json))
    artifacts = write_artifacts(payload, Path(args.output_dir))
    print(json.dumps({"success": True, "artifacts": artifacts}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
