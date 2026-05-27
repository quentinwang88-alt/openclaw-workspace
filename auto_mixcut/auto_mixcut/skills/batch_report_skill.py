from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from auto_mixcut.core.result import Result

from .context import SkillContext
from .quality_gate_skill import QualityGateSkill


class BatchReportSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def generate(self, batch_id: str) -> Result:
        batch = self.ctx.repo.get("mixcut_batches", "batch_id", batch_id)
        if not batch:
            return Result.fail("BATCH_NOT_FOUND", "batch not found", {"batch_id": batch_id})
        product_id = batch.get("product_id")
        product = self.ctx.repo.get("products", "product_id", product_id) or {}
        task = _latest_task(self.ctx, product_id)
        outputs = self.ctx.repo.list_where("outputs", "batch_id=? ORDER BY variant_no", (batch_id,))
        plans = self.ctx.repo.list_where("render_plans", "batch_id=? ORDER BY variant_no", (batch_id,))
        segments = self.ctx.repo.list_where("segments", "product_id=?", (product_id,))
        latest_tags = _latest_tags(self.ctx, [s["segment_id"] for s in segments])
        qc_results = [QualityGateSkill(self.ctx).check_output(o["output_id"]).data for o in outputs]

        report = {
            "batch": batch,
            "product": {
                "product_id": product.get("product_id"),
                "product_name": product.get("product_name"),
                "market": product.get("market"),
                "category": product.get("category"),
            },
            "task": task,
            "summary": _summary(outputs, plans, qc_results),
            "material": _material_summary(segments, latest_tags, task),
            "outputs": [_output_summary(self.ctx, output, qc_results) for output in outputs],
            "generated_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        report_dir = self.ctx.settings.root_dir / "var" / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        json_path = report_dir / f"{batch_id}_report.json"
        md_path = report_dir / f"{batch_id}_report.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        md_path.write_text(_markdown(report), encoding="utf-8")
        return Result.ok({"batch_id": batch_id, "json_path": str(json_path), "markdown_path": str(md_path), "summary": report["summary"]})


def _latest_task(ctx: SkillContext, product_id: str) -> dict:
    rows = ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
    return rows[0] if rows else {}


def _latest_tags(ctx: SkillContext, segment_ids: list[str]) -> dict[str, dict]:
    latest = {}
    for segment_id in segment_ids:
        rows = ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC", (segment_id,))
        if rows:
            latest[segment_id] = rows[0]
    return latest


def _summary(outputs: list[dict], plans: list[dict], qc_results: list[dict]) -> dict:
    human = Counter(o.get("human_quality_status") or "pending" for o in outputs)
    machine = Counter(q.get("machine_quality_status") or "pending" for q in qc_results)
    templates = Counter(p.get("template_id") for p in plans)
    failed_reasons = Counter()
    for qc in qc_results:
        for reason in qc.get("reasons") or []:
            failed_reasons[reason] += 1
    return {
        "outputs": len(outputs),
        "machine_quality": dict(machine),
        "human_quality": dict(human),
        "templates": dict(templates),
        "top_machine_fail_reasons": dict(failed_reasons.most_common(8)),
    }


def _material_summary(segments: list[dict], latest_tags: dict[str, dict], task: dict) -> dict:
    primary_roles = Counter()
    visibility = Counter()
    hooks = Counter()
    usability = Counter()
    risk = Counter()
    confidence = Counter()
    effective_roles = Counter()
    review_count = 0
    soft_subtitle_count = 0
    usable_count = 0
    for segment in segments:
        tag = latest_tags.get(segment["segment_id"], {})
        primary_roles[tag.get("primary_shot_role") or "untagged"] += 1
        visibility[tag.get("product_visibility") or "unknown"] += 1
        hooks[tag.get("hook_strength") or "unknown"] += 1
        usability[tag.get("mixcut_usability") or "unknown"] += 1
        risk[tag.get("risk_level") or "unknown"] += 1
        confidence[tag.get("confidence") or "unknown"] += 1
        review_count += int(tag.get("needs_human_review") or 0)
        roles = segment.get("effective_roles_json") or []
        usable_count += 1 if roles else 0
        for role in roles:
            effective_roles[role] += 1
        if segment.get("effective_roles_reason") == "soft local-language subtitle issue":
            soft_subtitle_count += 1
    return {
        "material_tier": task.get("material_tier"),
        "allowed_variant_count": task.get("allowed_variant_count"),
        "segments": len(segments),
        "usable_segments": usable_count,
        "needs_human_review_segments": review_count,
        "soft_local_subtitle_segments": soft_subtitle_count,
        "primary_roles": dict(primary_roles),
        "product_visibility": dict(visibility),
        "hook_strength": dict(hooks),
        "mixcut_usability": dict(usability),
        "risk_level": dict(risk),
        "confidence": dict(confidence),
        "effective_roles": dict(effective_roles),
    }


def _output_summary(ctx: SkillContext, output: dict, qc_results: list[dict]) -> dict[str, Any]:
    slots = ctx.repo.list_where("output_segments", "output_id=? ORDER BY slot_index", (output["output_id"],))
    soft_slots = 0
    slot_rows = []
    for slot in slots:
        segment = ctx.repo.get("segments", "segment_id", slot["segment_id"]) or {}
        tags = ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC", (slot["segment_id"],))
        tag = tags[0] if tags else {}
        is_soft = segment.get("effective_roles_reason") == "soft local-language subtitle issue"
        soft_slots += 1 if is_soft else 0
        slot_rows.append(
            {
                "slot": slot.get("slot_index"),
                "role": slot.get("role_used"),
                "segment_id": slot.get("segment_id"),
                "asset_id": slot.get("asset_id"),
                "primary_role": tag.get("primary_shot_role"),
                "visibility": tag.get("product_visibility"),
                "hook": tag.get("hook_strength"),
                "risk": tag.get("risk_level"),
                "soft_local_subtitle": is_soft,
            }
        )
    qc = next((q for q in qc_results if q.get("output_id") == output["output_id"]), {})
    return {
        "output_id": output["output_id"],
        "variant_no": output.get("variant_no"),
        "template_id": output.get("template_id"),
        "machine_quality_status": qc.get("machine_quality_status") or output.get("machine_quality_status"),
        "human_quality_status": output.get("human_quality_status"),
        "soft_local_subtitle_slots": soft_slots,
        "unique_assets": len({s.get("asset_id") for s in slots if s.get("asset_id")}),
        "machine_fail_reasons": qc.get("reasons") or [],
        "slots": slot_rows,
    }


def _markdown(report: dict) -> str:
    summary = report["summary"]
    material = report["material"]
    lines = [
        f"# Auto Mixcut Batch Report",
        "",
        f"- 批次：`{report['batch']['batch_id']}`",
        f"- 商品：`{report['product']['product_id']}` {report['product'].get('product_name') or ''}",
        f"- 输出数量：{summary['outputs']}",
        f"- 机器质检：{summary['machine_quality']}",
        f"- 人工质检：{summary['human_quality']}",
        f"- 模板分布：{summary['templates']}",
        "",
        "## 素材池",
        "",
        f"- 素材等级：{material.get('material_tier')}",
        f"- 允许生成：{material.get('allowed_variant_count')}",
        f"- 片段总数 / 可用片段：{material['segments']} / {material['usable_segments']}",
        f"- 需人工复核片段：{material['needs_human_review_segments']}",
        f"- 本地语言字幕软降级片段：{material['soft_local_subtitle_segments']}",
        f"- effective_roles：{material['effective_roles']}",
        f"- AI 原始镜位：{material['primary_roles']}",
        f"- 可见度：{material['product_visibility']}",
        f"- 风险：{material['risk_level']}",
        "",
        "## 成片",
        "",
    ]
    for output in report["outputs"]:
        lines.append(f"### V{output['variant_no']} `{output['output_id']}`")
        lines.append(f"- 模板：{output['template_id']}")
        lines.append(f"- 机器 / 人工：{output['machine_quality_status']} / {output['human_quality_status']}")
        lines.append(f"- 独立素材数：{output['unique_assets']}，软字幕片段数：{output['soft_local_subtitle_slots']}")
        if output["machine_fail_reasons"]:
            lines.append(f"- 机器失败原因：{'; '.join(output['machine_fail_reasons'])}")
        lines.append("- 槽位：")
        for slot in output["slots"]:
            soft = "，软字幕" if slot["soft_local_subtitle"] else ""
            lines.append(f"  - {slot['slot']}. {slot['role']} / {slot['primary_role']} / {slot['visibility']} / {slot['hook']} / {slot['risk']}{soft} / `{slot['segment_id']}`")
        lines.append("")
    return "\n".join(lines)
