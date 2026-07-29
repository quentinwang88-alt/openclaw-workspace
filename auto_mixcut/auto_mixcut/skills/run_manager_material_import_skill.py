from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from auto_mixcut.adapters.oss import file_sha256
from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext
from .product_identity_resolver_skill import ProductIdentityResolverSkill


FIELD_ALIASES = {
    "result_status": ["结果回传状态"],
    "video": ["生成视频", "视频附件"],
    "file_name": ["生成视频文件名", "视频文件名"],
    "script_id": ["脚本ID", "内容ID"],
    "internal_key": ["内部脚本键", "稳定脚本键"],
    "product_id": ["产品ID", "商品ID", "产品编码"],
    "canonical_product_id": ["全球产品ID", "canonical_product_id"],
    "local_product_id": ["当地商品ID", "本地商品ID"],
    "store_id": ["店铺ID"],
    "market": ["国家", "市场", "目标国家"],
    "source": ["任务来源", "脚本来源", "来源"],
    "channel": ["渠道", "渠道来源"],
    "model": ["模型"],
    "trace_id": ["最新追踪ID"],
    "platform_task_id": ["平台任务ID"],
    "completed_at": ["完成时间"],
    "policy": ["素材入库策略"],
    "ingest_status": ["素材入库状态"],
    "prompt": ["提示词"],
    "task_name": ["任务名", "视频任务ID"],
}

MIXCUT_TOKENS = ("混剪", "mixcut:", "mixcut_video")


@dataclass(frozen=True)
class MaterialCandidate:
    source_key: str
    record_id: str
    result_index: int
    attachment: dict[str, Any]
    source_flow: str
    script_id: str
    product_id: str
    canonical_product_id: str
    local_product_id: str
    store_id: str
    market: str
    channel: str
    model: str
    trace_id: str
    platform_task_id: str
    completed_at: str
    ingest_policy: str
    source_stage: str
    legacy_flag: int
    fields: dict[str, Any]


class RunManagerMaterialImportSkill:
    def __init__(self, ctx: SkillContext, *, app_token: str, table_id: str, source_system: str = "run_manager"):
        self.ctx = ctx
        self.app_token = app_token
        self.table_id = table_id
        self.source_system = str(source_system or "run_manager").strip()
        self.identity = ProductIdentityResolverSkill(ctx)

    def inspect_record(
        self,
        record_id: str,
        fields: dict[str, Any],
        *,
        cutoff_at: str = "",
        force_legacy: bool = False,
        index_only: bool = False,
    ) -> list[MaterialCandidate]:
        result_status = _field_text(fields, "result_status").lower()
        if result_status not in {"uploaded", "已上传", "已回流", "success", "成功"}:
            return []
        source_marker = " ".join(
            [_field_text(fields, "source"), _field_text(fields, "internal_key"), _field_text(fields, "script_id")]
        ).lower()
        if any(token.lower() in source_marker for token in MIXCUT_TOKENS):
            return []
        policy = _normalize_policy(_field_text(fields, "policy"))
        if policy == "skip":
            return []
        attachments = _attachments(_field_value(fields, "video"))
        if not attachments:
            return []

        task_name = _field_text(fields, "task_name")
        inferred_canonical = infer_canonical_product_id(task_name)
        product_id = _field_text(fields, "product_id") or inferred_canonical
        explicit_canonical = _field_text(fields, "canonical_product_id") or inferred_canonical
        local_product_id = _field_text(fields, "local_product_id")
        store_id = _field_text(fields, "store_id")
        market = _field_text(fields, "market")
        identity = self.identity.resolve(
            product_id=product_id,
            canonical_product_id=explicit_canonical,
            local_product_id=local_product_id,
            store_id=store_id,
            market=market,
        )
        completed_at = _normalize_datetime(_field_value(fields, "completed_at"))
        legacy_flag = int(index_only or _is_before_cutoff(completed_at, cutoff_at))
        if force_legacy or policy == "legacy_activate":
            legacy_flag = 0

        source_flow = normalize_source_flow(_field_text(fields, "source"))
        script_id = _field_text(fields, "script_id")
        trace_id = _field_text(fields, "trace_id")
        platform_task_id = _field_text(fields, "platform_task_id")
        source_stage = str(fields.get("__material_source_stage") or "generated_result").strip()
        source_variant = str(fields.get("__material_source_variant") or "").strip()
        candidates = []
        for index, attachment in enumerate(attachments, start=1):
            source_key = build_source_key(
                app_token=self.app_token,
                table_id=self.table_id,
                record_id=record_id,
                trace_id=trace_id,
                platform_task_id=platform_task_id,
                result_index=index,
                attachment=attachment,
                source_variant=source_variant,
            )
            candidates.append(
                MaterialCandidate(
                    source_key=source_key,
                    record_id=record_id,
                    result_index=index,
                    attachment=attachment,
                    source_flow=source_flow,
                    script_id=script_id,
                    product_id=identity.product_id or product_id,
                    canonical_product_id=identity.canonical_product_id,
                    local_product_id=local_product_id,
                    store_id=store_id,
                    market=market,
                    channel=_field_text(fields, "channel"),
                    model=_field_text(fields, "model"),
                    trace_id=trace_id,
                    platform_task_id=platform_task_id,
                    completed_at=completed_at,
                    ingest_policy=policy,
                    source_stage=source_stage,
                    legacy_flag=legacy_flag,
                    fields=fields,
                )
            )
        return candidates

    def inspect_light_review_record(
        self,
        record_id: str,
        fields: dict[str, Any],
        *,
        cutoff_at: str = "",
        force_legacy: bool = False,
        index_only: bool = False,
    ) -> list[MaterialCandidate]:
        """Adapt one light-video review row to the common generated-material contract."""
        initial_video = fields.get("初始成片")
        final_video = fields.get("最终视频")
        video = initial_video or final_video
        if not _attachments(video):
            return []
        uses_raw_initial = bool(_attachments(initial_video))
        adapted = dict(fields)
        adapted.update(
            {
                "结果回传状态": "uploaded",
                "生成视频": video,
                "产品ID": fields.get("商品ID") or fields.get("产品ID") or "",
                "全球产品ID": fields.get("全球产品ID") or fields.get("商品ID") or "",
                "任务来源": "轻视频复核",
                "脚本ID": fields.get("视频任务ID") or "",
                "渠道": fields.get("生成渠道") or "",
                "模型": fields.get("生成模型") or "",
                "完成时间": fields.get("复核处理时间") or fields.get("最后修改时间") or fields.get("飞书记录创建时间") or "",
                "__material_source_stage": "raw_initial" if uses_raw_initial else "postprocessed_final_fallback",
                # V2 raw identity deliberately differs from the former final-first key.
                # This lets rows already imported from 最终视频 ingest 初始成片 once.
                "__material_source_variant": "light_review_raw_v2" if uses_raw_initial else "",
            }
        )
        return self.inspect_record(
            record_id,
            adapted,
            cutoff_at=cutoff_at,
            force_legacy=force_legacy,
            # A postprocessed final is discoverable for old rows, but is not an
            # active mixcut asset unless an operator explicitly activates it.
            index_only=index_only or not uses_raw_initial,
        )

    def import_candidate(self, client: Any, candidate: MaterialCandidate, *, dry_run: bool = True) -> Result:
        existing = self._existing_registry(candidate.source_key)
        if existing.get("ingest_status") in {"completed", "stored_unbound"} and existing.get("oss_object_id"):
            restored = dict(existing)
            try:
                oss_row = self.ctx.repo.get("oss_objects", "object_id", existing["oss_object_id"]) or {}
            except Exception:
                oss_row = {}
            object_key = str(oss_row.get("object_key") or "")
            if object_key:
                restored["object_key"] = object_key
                restored["preview_url"] = self._preview_url(object_key)
            return Result.ok({"status": "already_imported", "source_key": candidate.source_key, **restored})

        planned_asset_id = str(existing.get("asset_id") or new_id("ASSET"))
        registry = self._registry_row(candidate, planned_asset_id)
        if candidate.legacy_flag:
            registry["ingest_status"] = "legacy_only"
            if dry_run:
                return Result.ok({"status": "would_index_legacy", **registry})
            written = self.ctx.repo.upsert("material_source_registry", "source_key", registry)
            return written if not written.success else Result.ok({"status": "legacy_only", **registry})

        if dry_run:
            return Result.ok({"status": "would_import", **registry})

        self.ctx.repo.upsert("material_source_registry", "source_key", {**registry, "ingest_status": "copying"})
        local_path: Path | None = None
        try:
            content, downloaded_name, content_type, source_size = client.download_attachment_bytes(candidate.attachment)
            file_name = _safe_name(downloaded_name or candidate.attachment.get("name") or f"{candidate.source_key}.mp4")
            local_path = self.ctx.settings.temp_root / "run_manager_materials" / candidate.source_key / file_name
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_bytes(content)
            if local_path.stat().st_size <= 0:
                raise ValueError("downloaded video is empty")
            file_hash = file_sha256(local_path)
            canonical_path = candidate.canonical_product_id or "unbound"
            date_path = _date_path(candidate.completed_at)
            object_key = (
                f"auto_mixcut/materials/generated/{_path_token(canonical_path)}/"
                f"{_path_token(candidate.source_flow)}/{date_path}/{planned_asset_id}{local_path.suffix.lower() or '.mp4'}"
            )
            uploaded = self.ctx.oss.upload(local_path, object_key)
            if not uploaded.success:
                return self._fail(candidate, uploaded.error.message if uploaded.error else "OSS upload failed")
            oss_row = dict(uploaded.data)
            oss_row.update({"object_type": "generated_material", "mime_type": content_type or "video/mp4"})
            oss_written = self.ctx.repo.upsert("oss_objects", "object_id", oss_row)
            if not oss_written.success:
                return self._fail(candidate, oss_written.error.message if oss_written.error else "OSS registry failed")

            registry.update(
                {
                    "ingest_status": "oss_stored",
                    "oss_object_id": oss_row["object_id"],
                    "file_name": file_name,
                    "file_size": int(oss_row.get("file_size") or source_size or len(content)),
                    "file_hash": str(oss_row.get("file_hash") or file_hash),
                    "last_error": "",
                }
            )
            self.ctx.repo.upsert("material_source_registry", "source_key", registry)

            if not candidate.canonical_product_id or not candidate.product_id:
                registry["ingest_status"] = "stored_unbound"
                self.ctx.repo.upsert("material_source_registry", "source_key", registry)
                return Result.ok({"status": "stored_unbound", **registry, "object_key": object_key})

            asset_row = {
                "asset_id": planned_asset_id,
                # The material pool is global-product scoped. Execution/store
                # identifiers remain preserved in material_source_registry.
                "product_id": candidate.canonical_product_id,
                "canonical_product_id": candidate.canonical_product_id,
                "source_type": "ai_generated",
                "source_trust_level": "medium" if candidate.source_stage == "raw_initial" else "low",
                "product_binding_type": "exact_sku",
                "media_type": "video",
                "original_oss_object_id": oss_row["object_id"],
                "file_status": "uploaded",
                "probe_status": "pending",
                "has_watermark": "pending",
                "risk_level": "medium",
                "asset_status": "pending_qc",
                "human_review_status": "pending",
                "source_identity": f"runmgr:{candidate.source_key}",
                "source_flow": candidate.source_flow,
                "source_record_id": candidate.record_id,
                "generation_job_id": candidate.trace_id or candidate.platform_task_id,
                "generation_type": "image_to_video" if candidate.source_stage != "postprocessed_final_fallback" else "postprocessed_fallback",
                "generation_model": candidate.model,
                "generation_channel": candidate.channel,
                "generation_prompt": _field_text(candidate.fields, "prompt"),
                "source_completed_at": candidate.completed_at,
                "visual_scope": "global",
            }
            asset_written = self.ctx.repo.upsert("assets", "asset_id", asset_row)
            if not asset_written.success:
                return self._fail(candidate, asset_written.error.message if asset_written.error else "asset registry failed")
            registry.update({"ingest_status": "completed", "asset_id": planned_asset_id, "last_error": ""})
            registry_written = self.ctx.repo.upsert("material_source_registry", "source_key", registry)
            if not registry_written.success:
                return registry_written
            return Result.ok(
                {
                    "status": "completed",
                    "source_key": candidate.source_key,
                    "asset_id": planned_asset_id,
                    "oss_object_id": oss_row["object_id"],
                    "object_key": object_key,
                    "preview_url": self._preview_url(object_key),
                    "canonical_product_id": candidate.canonical_product_id,
                }
            )
        except Exception as exc:
            return self._fail(candidate, str(exc))
        finally:
            if local_path is not None:
                try:
                    local_path.unlink(missing_ok=True)
                    local_path.parent.rmdir()
                except OSError:
                    pass

    def _registry_row(self, candidate: MaterialCandidate, asset_id: str) -> dict[str, Any]:
        return {
            "source_key": candidate.source_key,
            "source_system": self.source_system,
            "source_record_id": candidate.record_id,
            "source_result_index": candidate.result_index,
            "source_flow": candidate.source_flow,
            "script_id": candidate.script_id,
            "product_id": candidate.product_id,
            "canonical_product_id": candidate.canonical_product_id,
            "local_product_id": candidate.local_product_id,
            "source_market": candidate.market,
            "source_store_id": candidate.store_id,
            "channel": candidate.channel,
            "model": candidate.model,
            "trace_id": candidate.trace_id,
            "platform_task_id": candidate.platform_task_id,
            "completed_at": candidate.completed_at or None,
            "attachment_token": str(candidate.attachment.get("file_token") or ""),
            "file_name": str(candidate.attachment.get("name") or _field_text(candidate.fields, "file_name")),
            "file_size": int(candidate.attachment.get("size") or 0),
            "ingest_policy": candidate.ingest_policy,
            "ingest_status": "discovered",
            "asset_id": asset_id,
            "legacy_flag": candidate.legacy_flag,
            "retry_count": 0,
            "last_error": "",
            "source_payload_json": {
                "task_source": _field_text(candidate.fields, "source"),
                "internal_script_key": _field_text(candidate.fields, "internal_key"),
                "identity": {
                    "product_id": candidate.product_id,
                    "canonical_product_id": candidate.canonical_product_id,
                },
                "source_stage": candidate.source_stage,
            },
        }

    def _existing_registry(self, source_key: str) -> dict[str, Any]:
        try:
            return self.ctx.repo.get("material_source_registry", "source_key", source_key) or {}
        except Exception:
            return {}

    def _fail(self, candidate: MaterialCandidate, message: str) -> Result:
        existing = self._existing_registry(candidate.source_key)
        retry_count = int(existing.get("retry_count") or 0) + 1
        self.ctx.repo.upsert(
            "material_source_registry",
            "source_key",
            {
                **self._registry_row(candidate, str(existing.get("asset_id") or new_id("ASSET"))),
                "ingest_status": "failed",
                "retry_count": retry_count,
                "last_error": message[:2000],
                "oss_object_id": existing.get("oss_object_id") or "",
            },
        )
        return Result.fail("MATERIAL_IMPORT_FAILED", message, {"source_key": candidate.source_key, "record_id": candidate.record_id})

    def _preview_url(self, object_key: str) -> str:
        try:
            value = str(self.ctx.oss.signed_url(object_key, expires_seconds=30 * 86400) or "")
        except Exception:
            return ""
        return value if value.startswith(("http://", "https://")) else ""


def build_source_key(
    *,
    app_token: str,
    table_id: str,
    record_id: str,
    trace_id: str,
    platform_task_id: str,
    result_index: int,
    attachment: dict[str, Any],
    source_variant: str = "",
) -> str:
    result_identity = trace_id or platform_task_id
    if source_variant:
        # Review-table stages can share one trace ID. Attachment identity keeps
        # raw and postprocessed files distinct without changing generic imports.
        result_identity = "|".join(
            [
                str(attachment.get("file_token") or ""),
                str(attachment.get("name") or ""),
                str(attachment.get("size") or ""),
            ]
        )
    if not result_identity:
        result_identity = "|".join(
            [
                str(attachment.get("file_token") or ""),
                str(attachment.get("name") or ""),
                str(attachment.get("size") or ""),
            ]
        )
    seed = "|".join([app_token, table_id, record_id, result_identity, str(result_index), source_variant])
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def normalize_source_flow(value: str) -> str:
    text = str(value or "").strip().lower()
    if "轻量" in text or "轻视频" in text or "口播增强" in text or "补充镜头" in text or "light" in text:
        return "light_video"
    if "养号" in text and "复刻" in text:
        return "nurture_remake"
    if "复刻" in text or "remake" in text:
        return "video_remake"
    if "prompt" in text or "片段" in text:
        return "prompt_package"
    if "原创" in text or "original" in text:
        return "original_script"
    return "generated_video"


def infer_canonical_product_id(task_name: str) -> str:
    """Extract a stable external product code from legacy task-name prefixes."""
    matched = re.match(r"^([0-9]{10,})(?:[._-]|$)", str(task_name or "").strip())
    return matched.group(1) if matched else ""


def _field_value(fields: dict[str, Any], logical: str) -> Any:
    for name in FIELD_ALIASES[logical]:
        if name in fields and fields.get(name) not in (None, "", []):
            return fields.get(name)
    return None


def _field_text(fields: dict[str, Any], logical: str) -> str:
    return _text(_field_value(fields, logical))


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value", "link", "url"):
            if value.get(key):
                return _text(value.get(key))
        return ""
    if isinstance(value, list):
        return " / ".join(part for part in (_text(item) for item in value) if part)
    return str(value).strip()


def _attachments(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict) and value.get("file_token"):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict) and item.get("file_token")]
    return []


def _normalize_policy(value: str) -> str:
    value = str(value or "").strip().lower()
    if value in {"不入库", "skip", "disabled", "禁用"}:
        return "skip"
    if value in {"历史补录", "legacy_activate", "补录"}:
        return "legacy_activate"
    return "auto"


def _normalize_datetime(value: Any) -> str:
    if isinstance(value, (int, float)):
        seconds = float(value) / 1000 if float(value) > 10_000_000_000 else float(value)
        return datetime.fromtimestamp(seconds, tz=timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
    text = _text(value)
    if not text:
        return ""
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
    except ValueError:
        return text


def _is_before_cutoff(completed_at: str, cutoff_at: str) -> bool:
    if not cutoff_at or not completed_at:
        return False
    try:
        completed = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
        cutoff = datetime.fromisoformat(cutoff_at.replace("Z", "+00:00"))
        return completed < cutoff
    except ValueError:
        return False


def _date_path(value: str) -> str:
    try:
        dt = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        dt = datetime.utcnow()
    return dt.strftime("%Y/%m")


def _safe_name(value: str) -> str:
    name = Path(str(value or "video.mp4")).name
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._") or "video.mp4"


def _path_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "unknown")).strip("._") or "unknown"
