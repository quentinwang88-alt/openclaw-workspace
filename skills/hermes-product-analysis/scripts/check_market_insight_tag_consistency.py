#!/usr/bin/env python3
"""Spot-check market insight tagging consistency by re-running sampled products."""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_insight_analyzer import MarketInsightAnalyzer  # noqa: E402
from src.market_insight_models import MarketInsightProductTag, ProductRankingSnapshot  # noqa: E402
from src.market_insight_taxonomy import MarketInsightTaxonomyLoader  # noqa: E402


DEFAULT_DB_PATH = ROOT / "artifacts" / "market_insight" / "market_insight.db"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "validation"
DEFAULT_TH_LIGHT_TOPS_RUN_ID = (
    ROOT
    / "artifacts"
    / "market_insight"
    / "TH__light_tops"
    / "20260422__th_fastmoss_light_tops_product_ranking__085723"
)


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _load_json(text: Any, default):
    raw = _safe_text(text)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _pick_product_images(raw_fields: Dict[str, Any], snapshot_image_url: str) -> List[str]:
    images: List[str] = []

    product_image_field = raw_fields.get("商品图片")
    if isinstance(product_image_field, dict):
        for key in ("link", "text", "url"):
            value = _safe_text(product_image_field.get(key))
            if value.startswith("http://") or value.startswith("https://"):
                images.append(value)
                break
    elif isinstance(product_image_field, str) and product_image_field.startswith(("http://", "https://")):
        images.append(product_image_field)

    if snapshot_image_url.startswith(("http://", "https://")):
        images.append(snapshot_image_url)

    attachment_field = raw_fields.get("图片")
    if isinstance(attachment_field, list):
        for item in attachment_field:
            if not isinstance(item, dict):
                continue
            for key in ("file_url", "download_url", "preview_url", "link", "text", "url"):
                value = _safe_text(item.get(key))
                if not value.startswith(("http://", "https://")):
                    continue
                if "open-apis/drive/v1/medias/" in value:
                    continue
                images.append(value)
                break

    deduped: List[str] = []
    seen = set()
    for image in images:
        if image in seen:
            continue
        seen.add(image)
        deduped.append(image)
    return deduped[:4]


def _build_snapshot(row: sqlite3.Row, category: str) -> ProductRankingSnapshot:
    raw_fields = _load_json(row["raw_fields_json"], {})
    product_images = _pick_product_images(raw_fields, _safe_text(row["image_url"]))
    batch_date = _safe_text(row["batch_date"])
    return ProductRankingSnapshot(
        batch_date=batch_date,
        batch_id="{category}_{date}".format(category=category, date=batch_date.replace("-", "")),
        country=_safe_text(row["country"]),
        category=category,
        product_id=_safe_text(row["product_id"]),
        product_name=_safe_text(row["product_name"]),
        shop_name=_safe_text(row["shop_name"]),
        price_min=row["price_min"],
        price_max=row["price_max"],
        price_mid=row["price_mid"],
        sales_7d=float(row["sales_7d"] or 0.0),
        gmv_7d=float(row["gmv_7d"] or 0.0),
        creator_count=float(row["creator_count"] or 0.0),
        video_count=float(row["video_count"] or 0.0),
        listing_days=row["listing_days"],
        product_images=product_images,
        image_url=_safe_text(row["image_url"]),
        product_url=_safe_text(row["product_url"]),
        rank_index=int(row["rank_index"] or 0),
        raw_category=_safe_text(row["raw_category"]),
        raw_fields=raw_fields,
    )


def _tag_payload_from_row(row: sqlite3.Row) -> MarketInsightProductTag:
    return MarketInsightProductTag(
        is_valid_sample=bool(row["is_valid_sample"]),
        style_cluster=_safe_text(row["style_cluster"]),
        style_tags_secondary=_load_json(row["style_tags_secondary_json"], []),
        product_form=_safe_text(row["product_form"]),
        length_form=_safe_text(row["length_form"]),
        element_tags=_load_json(row["element_tags_json"], []),
        value_points=_load_json(row["value_points_json"], []),
        scene_tags=_load_json(row["scene_tags_json"], []),
        reason_short=_safe_text(row["reason_short"]),
    )


def _set_match(a: Iterable[str], b: Iterable[str]) -> bool:
    return sorted(set(a)) == sorted(set(b))


def _fetch_samples(
    db_path: Path,
    run_id: str,
    style_clusters: List[str],
    sample_per_cluster: int,
    seed: int,
) -> List[sqlite3.Row]:
    query = """
    SELECT
      s.run_id,
      s.product_row_key,
      s.batch_date,
      s.country,
      s.category,
      s.product_id,
      s.product_name,
      s.shop_name,
      s.price_min,
      s.price_max,
      s.price_mid,
      s.sales_7d,
      s.gmv_7d,
      s.creator_count,
      s.video_count,
      s.listing_days,
      s.image_url,
      s.product_url,
      s.rank_index,
      s.raw_category,
      s.raw_fields_json,
      t.is_valid_sample,
      t.style_cluster,
      t.style_tags_secondary_json,
      t.product_form,
      t.length_form,
      t.element_tags_json,
      t.value_points_json,
      t.scene_tags_json,
      t.reason_short
    FROM market_insight_product_snapshots s
    JOIN market_insight_product_tags t
      ON s.run_id = t.run_id AND s.product_row_key = t.product_row_key
    WHERE s.run_id = ?
      AND t.is_valid_sample = 1
      AND t.style_cluster = ?
    """
    sampled_rows: List[sqlite3.Row] = []
    rng = random.Random(seed)
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        for style_cluster in style_clusters:
            rows = conn.execute(query, (run_id, style_cluster)).fetchall()
            if not rows:
                continue
            take = min(sample_per_cluster, len(rows))
            sampled_rows.extend(rng.sample(rows, take))
    sampled_rows.sort(key=lambda row: (_safe_text(row["style_cluster"]), int(row["rank_index"] or 0)))
    return sampled_rows


def _summarize_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(results)
    successful = [item for item in results if not item.get("error")]
    style_matches = sum(1 for item in successful if item["style_cluster_match"])
    form_matches = sum(1 for item in successful if item["product_form_match"])
    length_matches = sum(1 for item in successful if item["length_form_match"])
    element_matches = sum(1 for item in successful if item["element_tags_match"])
    value_matches = sum(1 for item in successful if item["value_points_match"])
    scene_matches = sum(1 for item in successful if item["scene_tags_match"])

    by_direction = defaultdict(list)
    style_shift_counter: Counter[str] = Counter()
    for item in results:
        by_direction[item["original"]["style_cluster"]].append(item)
        if item.get("error"):
            continue
        if item["original"]["style_cluster"] != item["rerun"]["style_cluster"]:
            style_shift_counter[
                "{src} -> {dst}".format(
                    src=item["original"]["style_cluster"],
                    dst=item["rerun"]["style_cluster"],
                )
            ] += 1

    direction_summaries = {}
    for style_cluster, items in by_direction.items():
        ok_items = [item for item in items if not item.get("error")]
        direction_summaries[style_cluster] = {
            "sample_count": len(items),
            "successful_count": len(ok_items),
            "style_cluster_consistency": round(
                sum(1 for item in ok_items if item["style_cluster_match"]) / max(len(ok_items), 1),
                4,
            ),
            "product_form_consistency": round(
                sum(1 for item in ok_items if item["product_form_match"]) / max(len(ok_items), 1),
                4,
            ),
            "value_points_consistency": round(
                sum(1 for item in ok_items if item["value_points_match"]) / max(len(ok_items), 1),
                4,
            ),
            "errors": sum(1 for item in items if item.get("error")),
        }

    return {
        "sample_count": total,
        "successful_count": len(successful),
        "error_count": total - len(successful),
        "style_cluster_consistency": round(style_matches / max(len(successful), 1), 4),
        "product_form_consistency": round(form_matches / max(len(successful), 1), 4),
        "length_form_consistency": round(length_matches / max(len(successful), 1), 4),
        "element_tags_consistency": round(element_matches / max(len(successful), 1), 4),
        "value_points_consistency": round(value_matches / max(len(successful), 1), 4),
        "scene_tags_consistency": round(scene_matches / max(len(successful), 1), 4),
        "direction_summaries": direction_summaries,
        "style_shift_breakdown": dict(style_shift_counter.most_common()),
    }


def _render_markdown(report: Dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# 市场方向卡打标一致性抽查",
        "",
        "- 运行批次：{run_id}".format(run_id=report["run_id"]),
        "- 类目：{category}".format(category=report["category"]),
        "- 抽查方向：{clusters}".format(clusters="、".join(report["style_clusters"])),
        "- 每个方向抽样：{count}".format(count=report["sample_per_cluster"]),
        "- 总样本数：{count}".format(count=summary["sample_count"]),
        "- 成功复打：{count}".format(count=summary["successful_count"]),
        "- 复打失败：{count}".format(count=summary["error_count"]),
        "",
        "## 总体一致率",
        "- 主方向 style_cluster：{value}".format(value=summary["style_cluster_consistency"]),
        "- 产品形态 product_form：{value}".format(value=summary["product_form_consistency"]),
        "- 长度形态 length_form：{value}".format(value=summary["length_form_consistency"]),
        "- 元素标签 element_tags：{value}".format(value=summary["element_tags_consistency"]),
        "- 卖点标签 value_points：{value}".format(value=summary["value_points_consistency"]),
        "- 场景标签 scene_tags：{value}".format(value=summary["scene_tags_consistency"]),
        "",
        "## 分方向结果",
    ]
    for style_cluster, payload in report["summary"]["direction_summaries"].items():
        lines.extend(
            [
                "### {name}".format(name=style_cluster),
                "- 样本数：{sample_count}".format(**payload),
                "- 成功复打：{successful_count}".format(**payload),
                "- 主方向一致率：{style_cluster_consistency}".format(**payload),
                "- 形态一致率：{product_form_consistency}".format(**payload),
                "- 卖点一致率：{value_points_consistency}".format(**payload),
                "- 失败数：{errors}".format(**payload),
                "",
            ]
        )
    if report["summary"]["style_shift_breakdown"]:
        lines.append("## 主方向漂移明细")
        for key, count in report["summary"]["style_shift_breakdown"].items():
            lines.append("- {key}: {count}".format(key=key, count=count))
        lines.append("")
    lines.append("## 样本级结果")
    lines.append("")
    lines.append("| 方向 | 商品ID | 商品名 | 原标签 | 复打标签 | 主方向一致 | 形态一致 | 卖点一致 | 错误 |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for item in report["results"]:
        lines.append(
            "| {direction} | {product_id} | {name} | {original_style} | {rerun_style} | {style_match} | {form_match} | {value_match} | {error} |".format(
                direction=item["original"]["style_cluster"],
                product_id=item["product_id"],
                name=item["product_name"].replace("|", "/"),
                original_style=item["original"]["style_cluster"],
                rerun_style=(item.get("rerun") or {}).get("style_cluster", ""),
                style_match="是" if item.get("style_cluster_match") else "否",
                form_match="是" if item.get("product_form_match") else "否",
                value_match="是" if item.get("value_points_match") else "否",
                error=_safe_text(item.get("error")).replace("|", "/"),
            )
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-run sampled market insight tags and calculate consistency.")
    parser.add_argument("--run-id", default=str(DEFAULT_TH_LIGHT_TOPS_RUN_ID))
    parser.add_argument("--category", default="light_tops")
    parser.add_argument("--style-cluster", action="append", dest="style_clusters")
    parser.add_argument("--sample-per-cluster", type=int, default=10)
    parser.add_argument("--seed", type=int, default=20260423)
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--hermes-bin", default="")
    parser.add_argument("--timeout-seconds", type=int, default=90)
    parser.add_argument("--max-workers", type=int, default=3)
    args = parser.parse_args()

    style_clusters = args.style_clusters or ["薄针织开衫", "简洁轻熟型", "防晒轻罩衫"]
    db_path = Path(args.db_path)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    samples = _fetch_samples(
        db_path=db_path,
        run_id=str(args.run_id),
        style_clusters=style_clusters,
        sample_per_cluster=max(1, int(args.sample_per_cluster)),
        seed=int(args.seed),
    )
    if not samples:
        raise SystemExit("未找到可抽样样本")

    taxonomy = MarketInsightTaxonomyLoader(ROOT / "configs" / "market_insight_taxonomies").load(args.category)
    analyzer = MarketInsightAnalyzer(
        skill_dir=ROOT,
        hermes_bin=args.hermes_bin or None,
        timeout_seconds=int(args.timeout_seconds),
    )

    results: List[Dict[str, Any]] = []
    total = len(samples)

    def _run_single(row: sqlite3.Row) -> Dict[str, Any]:
        snapshot = _build_snapshot(row, category=args.category)
        original = _tag_payload_from_row(row)
        result_payload: Dict[str, Any] = {
            "product_id": snapshot.product_id,
            "product_name": snapshot.product_name,
            "image_inputs": snapshot.product_images,
            "original": original.to_dict(),
        }
        try:
            rerun = analyzer.tag_product(snapshot, taxonomy=taxonomy)
            result_payload["rerun"] = rerun.to_dict()
            result_payload["style_cluster_match"] = original.style_cluster == rerun.style_cluster
            result_payload["product_form_match"] = original.product_form == rerun.product_form
            result_payload["length_form_match"] = original.length_form == rerun.length_form
            result_payload["element_tags_match"] = _set_match(original.element_tags, rerun.element_tags)
            result_payload["value_points_match"] = _set_match(original.value_points, rerun.value_points)
            result_payload["scene_tags_match"] = _set_match(original.scene_tags, rerun.scene_tags)
            result_payload["error"] = ""
        except Exception as exc:  # noqa: BLE001
            result_payload["rerun"] = {}
            result_payload["style_cluster_match"] = False
            result_payload["product_form_match"] = False
            result_payload["length_form_match"] = False
            result_payload["element_tags_match"] = False
            result_payload["value_points_match"] = False
            result_payload["scene_tags_match"] = False
            result_payload["error"] = str(exc)
        return result_payload

    max_workers = max(1, int(args.max_workers))
    if max_workers <= 1:
        for index, row in enumerate(samples, start=1):
            results.append(_run_single(row))
            print("[{done}/{total}] {name}".format(done=index, total=total, name=_safe_text(row["product_name"])), flush=True)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_run_single, row): row for row in samples}
            completed = 0
            for future in as_completed(futures):
                completed += 1
                row = futures[future]
                results.append(future.result())
                print("[{done}/{total}] {name}".format(done=completed, total=total, name=_safe_text(row["product_name"])), flush=True)

    summary = _summarize_results(results)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": str(args.run_id),
        "category": args.category,
        "style_clusters": style_clusters,
        "sample_per_cluster": int(args.sample_per_cluster),
        "seed": int(args.seed),
        "summary": summary,
        "results": results,
    }
    json_path = output_dir / "market_insight_tag_consistency_{timestamp}.json".format(timestamp=timestamp)
    md_path = output_dir / "market_insight_tag_consistency_{timestamp}.md".format(timestamp=timestamp)
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_markdown(report), encoding="utf-8")

    print(json.dumps({"json_path": str(json_path), "md_path": str(md_path), "summary": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
