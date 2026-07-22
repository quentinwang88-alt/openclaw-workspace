#!/usr/bin/env python3
"""Build a human-readable selection reference from enriched MX wigs VOC JSON."""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


SKILL_DIR = Path(__file__).resolve().parents[1]
DEFAULT_TAXONOMY = SKILL_DIR / "references" / "wigs_voc_taxonomy_v1.json"


def load_json(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def dedupe(values: Iterable[str]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def tag_meta(taxonomy: Dict[str, Any], tag: str) -> Dict[str, Any]:
    return (taxonomy.get("signals") or {}).get(tag) or {
        "label_zh": tag,
        "insight_role": "product_core_selling_point",
        "title_zh": tag,
    }


def is_risk(taxonomy: Dict[str, Any], tag: str) -> bool:
    return tag_meta(taxonomy, tag).get("insight_role") == "risk_guard"


def is_selection_signal(taxonomy: Dict[str, Any], tag: str) -> bool:
    role = tag_meta(taxonomy, tag).get("insight_role")
    return role in {"product_core_selling_point", "offer_selling_point"}


def confidence_action(confidence: str, products: int, voc: int) -> str:
    if confidence in {"form_candidate", "ads_candidate"}:
        return "可进入小批选品验证"
    if confidence == "partial_candidate":
        return "优先小批测款，暂不扩大采购"
    if products >= 2 and voc >= 10:
        return "方向可观察，补足商品数后再决策"
    return "样本不足，只保留为待补样方向"


def product_label(positive_hits: int, risk_hits: int, valid_voc: int) -> str:
    if risk_hits == 0 and positive_hits >= 5 and valid_voc >= 3:
        return "优先验证"
    if risk_hits == 0 and positive_hits >= 3:
        return "可观察"
    if risk_hits > 0 and positive_hits >= risk_hits * 2:
        return "有卖点但需针对风险验货"
    if risk_hits > 0:
        return "谨慎/暂缓"
    return "证据不足"


def examples_for(records: Sequence[Dict[str, Any]], tag: str, limit: int = 2) -> List[str]:
    rows = [record for record in records if tag in (record.get("signal_tags") or [])]
    rows.sort(key=lambda row: 0 if row.get("sentiment") == "positive" else 1)
    return dedupe(str(row.get("voc_text") or "") for row in rows)[:limit]


def build_report(payload: Dict[str, Any], taxonomy: Dict[str, Any]) -> str:
    valid = [record for record in payload.get("records") or [] if record.get("is_valid_voc")]
    draft = payload.get("draft_insights") or {}
    form_artifacts = {
        item["product_form"]: item for item in draft.get("form_artifacts") or []
    }
    by_form: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    by_product: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in valid:
        by_form[record["product_form"]].append(record)
        by_product[record["fastmoss_product_id"]].append(record)

    lines = [
        "# 墨西哥假发 VOC 选品参考",
        "",
        "- 批次：`{}`".format(payload.get("batch_id")),
        "- 数据范围：{} 条有效 VOC / {} 个有评论商品".format(
            len(valid), len(by_product)
        ),
        "- 数据池：仅 classic 商品评论；new 池暂未取得 VOC",
        "- 类目置信度：`{}`，因此本报告用于小批选品验证，不作为大货采购结论".format(
            (draft.get("category_artifact") or {}).get("confidence_level", "observe_only")
        ),
        "",
        "## 一句话结论",
        "",
        "当前最值得优先验证的是 **卡扣接发**；购买理由集中在造型效果、材质质量、自然度和性价比。"
        "马尾接发的自然度与长度反馈不错，但商品数不足。整顶假发、编发发束目前风险证据相对突出，先补样或改善佩戴/材质方案。",
        "",
        "## 商品形态机会",
        "",
        "| 形态 | 商品/VOC | 置信度 | 核心正向信号 | 主要风险 | 选品动作 |",
        "|---|---:|---|---|---|---|",
    ]
    form_order = sorted(
        by_form,
        key=lambda form: (
            0 if (form_artifacts.get(form) or {}).get("confidence_level") == "partial_candidate" else 1,
            -len(by_form[form]),
        ),
    )
    for form in form_order:
        records = by_form[form]
        artifact = form_artifacts.get(form) or {}
        products = len({record["fastmoss_product_id"] for record in records})
        tag_counts = Counter(tag for record in records for tag in record.get("signal_tags") or [])
        positives = [
            tag for tag, _ in tag_counts.most_common()
            if is_selection_signal(taxonomy, tag)
        ][:3]
        risks = [tag for tag, _ in tag_counts.most_common() if is_risk(taxonomy, tag)][:2]
        label = records[0].get("product_form_label") or form
        confidence = artifact.get("confidence_level") or "observe_only"
        lines.append("| {} | {}/{} | `{}` | {} | {} | {} |".format(
            label,
            products,
            len(records),
            confidence,
            "、".join("{}({})".format(tag_meta(taxonomy, tag).get("label_zh", tag), tag_counts[tag]) for tag in positives) or "无稳定信号",
            "、".join("{}({})".format(tag_meta(taxonomy, tag).get("label_zh", tag), tag_counts[tag]) for tag in risks) or "暂无集中风险",
            confidence_action(confidence, products, len(records)),
        ))

    all_counts = Counter(tag for record in valid for tag in record.get("signal_tags") or [])
    lines.extend([
        "",
        "## 核心购买理由与原始证据",
        "",
    ])
    positive_tags = [tag for tag, _ in all_counts.most_common() if is_selection_signal(taxonomy, tag)][:8]
    for tag in positive_tags:
        meta = tag_meta(taxonomy, tag)
        product_count = len({
            record["fastmoss_product_id"] for record in valid if tag in (record.get("signal_tags") or [])
        })
        lines.extend([
            "### {}：{} 条 / {} 个商品".format(meta.get("label_zh", tag), all_counts[tag], product_count),
            "",
            "选品含义：{}。".format(meta.get("title_zh", tag)),
            "",
        ])
        for example in examples_for(valid, tag):
            lines.append("> {}".format(example))
            lines.append("")

    lines.extend(["## 主要风险与验货要求", ""])
    risk_tags = [tag for tag, _ in all_counts.most_common() if is_risk(taxonomy, tag)]
    for tag in risk_tags:
        meta = tag_meta(taxonomy, tag)
        product_count = len({
            record["fastmoss_product_id"] for record in valid if tag in (record.get("signal_tags") or [])
        })
        lines.append("- **{}**：{} 条 / {} 个商品。{}。".format(
            meta.get("label_zh", tag), all_counts[tag], product_count, meta.get("title_zh", tag)
        ))
        for example in examples_for(valid, tag, limit=1):
            lines.append("  - 西语证据：{}".format(example))

    product_rows = []
    for product_id, records in by_product.items():
        tags = [tag for record in records for tag in record.get("signal_tags") or []]
        positive_tags_for_product = [tag for tag in tags if is_selection_signal(taxonomy, tag)]
        risk_tags_for_product = [tag for tag in tags if is_risk(taxonomy, tag)]
        title = str(records[0].get("product_title") or "")
        product_rows.append({
            "product_id": product_id,
            "form": records[0].get("product_form_label") or records[0].get("product_form"),
            "valid_voc": len(records),
            "positive_hits": len(positive_tags_for_product),
            "risk_hits": len(risk_tags_for_product),
            "positive_tags": Counter(positive_tags_for_product),
            "risk_tags": Counter(risk_tags_for_product),
            "title": title,
            "label": product_label(len(positive_tags_for_product), len(risk_tags_for_product), len(records)),
        })
    product_rows.sort(key=lambda row: (
        0 if row["label"] == "优先验证" else 1,
        row["risk_hits"],
        -row["positive_hits"],
    ))
    lines.extend([
        "",
        "## 商品级选品清单",
        "",
        "| 建议 | 商品ID | 形态 | 有效VOC | 正向/风险命中 | 主要依据 |",
        "|---|---|---|---:|---:|---|",
    ])
    for row in product_rows:
        evidence = [
            tag_meta(taxonomy, tag).get("label_zh", tag)
            for tag, _ in row["positive_tags"].most_common(3)
        ]
        risks = [
            tag_meta(taxonomy, tag).get("label_zh", tag)
            for tag, _ in row["risk_tags"].most_common(2)
        ]
        if risks:
            evidence.append("风险：" + "、".join(risks))
        lines.append("| {} | `{}` | {} | {} | {}/{} | {} |".format(
            row["label"], row["product_id"], row["form"], row["valid_voc"],
            row["positive_hits"], row["risk_hits"], "；".join(evidence) or "证据不足"
        ))

    lines.extend([
        "",
        "## 建议的下一轮选品条件",
        "",
        "1. 优先补充卡扣接发：重点找自然衔接、低光泽、色号清晰、卡扣稳定、可梳理的款。",
        "2. 马尾接发先补到至少 5 个有 VOC 商品，再判断自然度和长度是否是稳定方向。",
        "3. 整顶假发必须增加佩戴教程、发根/头套贴合检查，避免真发鼓包和佩戴门槛。",
        "4. 编发发束验货必须检查打结、耐用次数和材质，不能只看低价。",
        "5. 下一轮应补 new 池 VOC；在达到 20 个有效商品、100–150 条评论前，不做类目级大货结论。",
    ])
    return "\n".join(lines) + "\n"


def write_evidence_csv(path: Path, payload: Dict[str, Any], taxonomy: Dict[str, Any]) -> None:
    records = [record for record in payload.get("records") or [] if record.get("is_valid_voc")]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "商品ID", "商品形态", "商品标题", "VOC排名", "情绪", "主题标签",
            "中文标签", "风险标签", "西语原文", "证据ID",
        ])
        writer.writeheader()
        for record in records:
            tags = record.get("signal_tags") or []
            writer.writerow({
                "商品ID": record.get("fastmoss_product_id"),
                "商品形态": record.get("product_form_label"),
                "商品标题": record.get("product_title"),
                "VOC排名": record.get("voc_rank"),
                "情绪": record.get("sentiment"),
                "主题标签": ",".join(tags),
                "中文标签": "、".join(tag_meta(taxonomy, tag).get("label_zh", tag) for tag in tags),
                "风险标签": "、".join(tag_meta(taxonomy, tag).get("label_zh", tag) for tag in tags if is_risk(taxonomy, tag)),
                "西语原文": record.get("voc_text"),
                "证据ID": record.get("evidence_id"),
            })


def main() -> int:
    parser = argparse.ArgumentParser(description="Build MX wigs VOC selection report")
    parser.add_argument("--input-json", required=True)
    parser.add_argument("--taxonomy", default=str(DEFAULT_TAXONOMY))
    parser.add_argument("--output-dir", default=str(SKILL_DIR / "output"))
    args = parser.parse_args()

    payload = load_json(Path(args.input_json))
    taxonomy = load_json(Path(args.taxonomy))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    batch_id = payload.get("batch_id") or "mx_wigs"
    report_path = output_dir / "{}_selection_reference.md".format(batch_id)
    csv_path = output_dir / "{}_selection_evidence.csv".format(batch_id)
    report_path.write_text(build_report(payload, taxonomy), encoding="utf-8")
    write_evidence_csv(csv_path, payload, taxonomy)
    print(json.dumps({
        "success": True,
        "batch_id": batch_id,
        "report": str(report_path),
        "evidence_csv": str(csv_path),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
