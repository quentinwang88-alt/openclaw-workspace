from __future__ import annotations

from datetime import datetime, timedelta

from auto_mixcut.adapters.feishu import AutoMixcutFeishuClient, datetime_cell, url_cell
from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result
from auto_mixcut.core.storage_paths import require_oss_object_path

from .context import SkillContext


class FeishuReviewSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def sync_task(self, product_id: str) -> Result:
        task = self.ctx.repo.list_where("content_tasks", "product_id=? ORDER BY id DESC", (product_id,))
        if not task:
            return Result.fail("TASK_NOT_FOUND", "task not found", {"product_id": product_id})
        product = self.ctx.repo.get("products", "product_id", product_id) or {}
        latest_output = _latest_output(self.ctx, product_id)
        fields = {
            "商品ID": product_id,
            "商品名称": product.get("product_name"),
            "市场": product.get("market"),
            "类目": product.get("category"),
            "店铺": product.get("shop_id"),
            "优先级": product.get("priority"),
            "任务类型": task[0].get("task_type"),
            "目标生成数量": task[0].get("requested_variant_count"),
            "系统允许生成数量": task[0].get("allowed_variant_count"),
            "实际生成数量": task[0].get("actual_variant_count"),
            "素材等级": task[0].get("material_tier"),
            "素材状态": task[0].get("material_status"),
            "混剪状态": task[0].get("task_status"),
            "锚点状态": product.get("anchor_status"),
            "素材缺口说明": task[0].get("blocked_reason"),
            "失败原因": task[0].get("failure_reason"),
            "最近成片预览": url_cell(_object_url(self.ctx, latest_output.get("output_oss_object_id") if latest_output else None), "最近成片预览"),
        }
        return self._sync("product_task", task[0]["task_id"], "商品内容任务表", days=14, fields=fields)

    def sync_anchor_queue(self, product_id: str) -> Result:
        product = self.ctx.repo.get("products", "product_id", product_id)
        if not product:
            return Result.fail("PRODUCT_NOT_FOUND", "product not found", {"product_id": product_id})
        anchor = product.get("product_anchor_json") or {}
        fields = {
            "商品ID": product_id,
            "商品名称": product.get("product_name"),
            "市场": product.get("market"),
            "类目": product.get("category"),
            "AI生成锚点卡": _json_text(anchor),
            "核心视觉点": "\n".join(anchor.get("core_visual_points") or []),
            "不可错识别点": "\n".join(anchor.get("must_not_change_points") or []),
            "禁用错配项": "\n".join(anchor.get("forbidden_mismatch") or []),
            "适用核心镜头": anchor.get("strict_roles"),
            "人工确认状态": "已确认" if product.get("anchor_status") == "confirmed" else "待确认",
            "确认人": product.get("anchor_confirmed_by"),
            "确认时间": datetime_cell(product.get("anchor_confirmed_at")),
        }
        return self._sync("product_anchor", product_id, "商品锚点卡确认队列", days=14, fields=fields)

    def pull_anchor_confirmations(self, product_id: str | None = None) -> Result:
        if not self.ctx.settings.feishu_enabled:
            return Result.fail("FEISHU_DISABLED", "set AUTO_MIXCUT_FEISHU_ENABLED=1 to pull confirmations")
        synced = self.ctx.repo.list_where(
            "feishu_sync_records",
            "object_type='product_anchor' AND feishu_table='商品锚点卡确认队列' AND feishu_record_id LIKE 'rec%'",
        )
        if product_id:
            synced = [row for row in synced if row.get("object_id") == product_id]
        client = AutoMixcutFeishuClient("商品锚点卡确认队列")
        confirmed = []
        pending = []
        for row in synced:
            fields = client.get_record(row["feishu_record_id"])
            status = _cell_text(fields.get("人工确认状态"))
            if status != "已确认":
                pending.append({"product_id": row["object_id"], "status": status or "未填写"})
                continue
            patch = {}
            manual = _cell_text(fields.get("人工修正内容"))
            if manual:
                patch["product_anchor_json"] = _merge_manual_anchor(self.ctx, row["object_id"], manual)
            patch.update(
                {
                    "anchor_status": "confirmed",
                    "anchor_confirmed_at": datetime.utcnow().isoformat(timespec="seconds"),
                    "anchor_confirmed_by": _cell_text(fields.get("确认人")) or "feishu",
                }
            )
            res = self.ctx.repo.update("products", "product_id", row["object_id"], patch)
            if not res.success:
                return res
            self.ctx.repo.update("content_tasks", "product_id", row["object_id"], {"task_status": "ANCHOR_CONFIRMED"})
            confirmed.append(row["object_id"])
        return Result.ok({"confirmed": confirmed, "pending": pending})

    def sync_review_segments(self, product_id: str) -> Result:
        segments = self.ctx.repo.list_where("segments", "product_id=?", (product_id,))
        rows = []
        for segment in segments:
            latest = self.ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC", (segment["segment_id"],))
            if latest and int(latest[0].get("needs_human_review") or 0) == 1:
                rows.append(latest[0])
        synced = []
        for row in rows:
            segment = self.ctx.repo.get("segments", "segment_id", row["segment_id"])
            if segment and segment["product_id"] == product_id:
                product = self.ctx.repo.get("products", "product_id", product_id) or {}
                fields = {
                    "片段ID": row["segment_id"],
                    "商品ID": product_id,
                    "市场": product.get("market"),
                    "类目": product.get("category"),
                    "片段预览链接": url_cell(_object_url(self.ctx, segment.get("segment_oss_object_id")), "片段预览"),
                    "AI镜头用途": row.get("primary_shot_role"),
                    "AI商品可见度": row.get("product_visibility"),
                    "AI首镜强度": row.get("hook_strength"),
                    "AI可混剪判断": row.get("mixcut_usability"),
                    "AI风险等级": row.get("risk_level"),
                    "AI置信度": row.get("confidence"),
                    "AI判断理由": row.get("reason"),
                    "商品匹配状态": segment.get("product_match_status"),
                    "有效镜位": segment.get("effective_roles_json"),
                    "复核状态": "待复核",
                }
                res = self._sync("segment_review", row["segment_id"], "人工复核队列表", days=7, fields=fields)
                if res.success:
                    synced.append(row["segment_id"])
        return Result.ok({"synced_segments": synced})

    def sync_output_qc(self, batch_id: str) -> Result:
        outputs = self.ctx.repo.list_where("outputs", "batch_id=? AND machine_quality_status='passed'", (batch_id,))
        synced = []
        for output in outputs:
            fields = {
                "输出ID": output.get("output_id"),
                "商品ID": output.get("product_id"),
                "批次ID": output.get("batch_id"),
                "变体编号": output.get("variant_no"),
                "模板ID": output.get("template_id"),
                "视频预览链接": url_cell(_object_url(self.ctx, output.get("output_oss_object_id")), "视频预览"),
                "机器质检状态": output.get("machine_quality_status"),
                "人工质检状态": "待检查",
                "飞书展示到期时间": datetime_cell((datetime.utcnow() + timedelta(days=7)).isoformat(timespec="seconds")),
            }
            video_attachment = _upload_video_to_feishu(self.ctx, output.get("output_oss_object_id"))
            if video_attachment:
                fields["成片文件"] = [video_attachment]
            res = self._sync("output_qc", output["output_id"], "成片质检表", days=7, fields=fields)
            if not res.success:
                return res
            self.ctx.repo.update("outputs", "output_id", output["output_id"], {"feishu_preview_status": "synced", "feishu_record_id": res.data["feishu_record_id"], "preview_expire_at": res.data["expire_at"]})
            synced.append(output["output_id"])
        return Result.ok({"synced_outputs": synced})

    def pull_output_qc(self, batch_id: str | None = None) -> Result:
        if not self.ctx.settings.feishu_enabled:
            return Result.fail("FEISHU_DISABLED", "set AUTO_MIXCUT_FEISHU_ENABLED=1 to pull output QC")
        client = AutoMixcutFeishuClient("成片质检表")
        records = client.list_records(limit=500)
        updated = []
        skipped = []
        for record in records:
            fields = record.fields
            output_id = _cell_text(fields.get("输出ID"))
            if not output_id:
                continue
            output = self.ctx.repo.get("outputs", "output_id", output_id)
            if not output:
                continue
            if batch_id and output.get("batch_id") != batch_id:
                continue
            human_status = _cell_text(fields.get("人工质检状态")) or "待检查"
            publishable = _bool_cell(fields.get("是否可发布")) or human_status in {"可发布", "通过", "发布"}
            status = "passed" if publishable or human_status in {"可发布", "通过", "发布"} else "rejected" if human_status in {"不可发布", "需修改", "驳回"} else "pending"
            res = self.ctx.repo.update("outputs", "output_id", output_id, {"human_quality_status": status})
            if res.success:
                updated.append({"output_id": output_id, "human_quality_status": status, "feishu_status": human_status})
            else:
                skipped.append({"output_id": output_id, "error": res.error.message if res.error else "unknown"})
        return Result.ok({"updated": updated, "skipped": skipped})

    def apply_human_review(self, segment_id: str, overrides: dict, reviewer_id: str = "human") -> Result:
        latest = self.ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC", (segment_id,))
        base = latest[0] if latest else {}
        base.update(overrides)
        base.update({"segment_id": segment_id, "tag_source": "human", "reviewer_id": reviewer_id, "reviewed_at": datetime.utcnow().isoformat(timespec="seconds"), "needs_human_review": 0})
        allowed = {k: base.get(k) for k in ["segment_id", "tag_source", "primary_shot_role", "secondary_roles_json", "product_visibility", "hook_strength", "mixcut_usability", "risk_level", "confidence", "needs_human_review", "reason", "reviewer_id", "reviewed_at"]}
        return self.ctx.repo.insert("segment_tags", allowed)

    def cleanup_expired_previews(self) -> Result:
        now = datetime.utcnow().isoformat(timespec="seconds")
        records = self.ctx.repo.list_where("feishu_sync_records", "expire_at IS NOT NULL AND expire_at<? AND COALESCE(cleanup_status,'')!='cleaned'", (now,))
        for rec in records:
            self.ctx.repo.update("feishu_sync_records", "sync_id", rec["sync_id"], {"cleanup_status": "cleaned"})
        return Result.ok({"cleaned": len(records)})

    def _sync(self, object_type: str, object_id: str, table: str, days: int, fields: dict | None = None) -> Result:
        sync_id = new_id("FS")
        expire_at = (datetime.utcnow() + timedelta(days=days)).isoformat(timespec="seconds")
        feishu_record_id = new_id("FSREC")
        if self.ctx.settings.feishu_enabled:
            try:
                existing = self.ctx.repo.list_where(
                    "feishu_sync_records",
                    "object_type=? AND object_id=? AND feishu_table=? ORDER BY id DESC",
                    (object_type, object_id, table),
                )
                client = AutoMixcutFeishuClient(table)
                if existing and existing[0].get("feishu_record_id", "").startswith("rec"):
                    feishu_record_id = existing[0]["feishu_record_id"]
                    client.update_record(feishu_record_id, fields or {})
                    sync_id = existing[0]["sync_id"]
                else:
                    feishu_record_id = client.create_record(fields or {})
            except Exception as exc:
                return Result.fail("FEISHU_SYNC_FAILED", str(exc), {"object_type": object_type, "object_id": object_id, "table": table})
        res = self.ctx.repo.upsert(
            "feishu_sync_records",
            "sync_id",
            {"sync_id": sync_id, "object_type": object_type, "object_id": object_id, "feishu_table": table, "feishu_record_id": feishu_record_id, "sync_status": "synced", "expire_at": expire_at, "cleanup_status": "pending"},
        )
        return res if not res.success else Result.ok({"sync_id": sync_id, "feishu_record_id": feishu_record_id, "expire_at": expire_at})


def _json_text(value: object) -> str:
    import json

    return json.dumps(value or {}, ensure_ascii=False, indent=2)


def _cell_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ("text", "name", "link", "url"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
        return ""
    if isinstance(value, list):
        return " / ".join(item for item in (_cell_text(v) for v in value) if item)
    return str(value).strip()


def _bool_cell(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = _cell_text(value).lower()
    return text in {"true", "1", "yes", "是", "可发布", "通过"}


def _merge_manual_anchor(ctx: SkillContext, product_id: str, manual_text: str) -> dict:
    product = ctx.repo.get("products", "product_id", product_id) or {}
    anchor = dict(product.get("product_anchor_json") or {})
    anchor["human_revision_text"] = manual_text
    anchor["human_revision_applied_at"] = datetime.utcnow().isoformat(timespec="seconds")
    return anchor


def _object_url(ctx: SkillContext, object_id: str | None) -> str | None:
    if not object_id:
        return None
    obj = ctx.repo.get("oss_objects", "object_id", object_id)
    if not obj:
        return None
    return ctx.oss.signed_url(obj["object_key"])


def _object_file_info(ctx: SkillContext, object_id: str | None) -> dict | None:
    if not object_id:
        return None
    path = require_oss_object_path(ctx, object_id, "feishu_preview")
    if not path or not path.exists():
        return None
    obj = ctx.repo.get("oss_objects", "object_id", object_id)
    if not obj:
        return None
    return {"file_token": "", "name": path.name, "size": path.stat().st_size, "tmp_url": ctx.oss.signed_url(obj["object_key"]) or ""}


def _upload_video_to_feishu(ctx: SkillContext, object_id: str | None) -> dict | None:
    if not object_id:
        return None
    path = require_oss_object_path(ctx, object_id, "feishu_upload")
    if not path or not path.exists():
        return None
    obj = ctx.repo.get("oss_objects", "object_id", object_id)
    if not obj:
        return None
    try:
        from auto_mixcut.adapters.feishu import TABLES
        table_info = TABLES["成片质检表"]
        client_cls = _get_bitable_client()
        client = client_cls(app_token=table_info.app_token, table_id=table_info.table_id)
        content = path.read_bytes()
        result = client.upload_attachment(
            content=content,
            file_name=path.name,
            content_type="video/mp4",
            size=len(content),
            parent_type="bitable_file",
        )
        return result
    except Exception:
        return None


def _get_bitable_client():
    import sys
    from pathlib import Path
    workspace = Path(__file__).resolve().parent.parent.parent.parent.parent
    skill_path = workspace / "skills" / "script-run-manager-sync"
    if str(skill_path) not in sys.path:
        sys.path.insert(0, str(skill_path))
    from core.bitable import FeishuBitableClient
    return FeishuBitableClient


def _latest_output(ctx: SkillContext, product_id: str) -> dict | None:
    rows = ctx.repo.list_where("outputs", "product_id=? ORDER BY id DESC", (product_id,))
    return rows[0] if rows else None
