"""Model-free, retryable delivery of WSR results to the existing script pool."""
from __future__ import annotations

import hashlib
import json
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable
from wig_success_replication.reference_manifest import validate_reference_manifest, rebind_reference_manifest

POOL_WIKI_TOKEN = "KsX7w8Y8ZiJfnsk2Mtvc7xLun1f"
POOL_TABLE = "tblIvHJ0nsn9WCwi"
SOURCE_KIND = "成功脚本复刻"


def cell_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return "".join(cell_text(v) for v in value)
    if isinstance(value, dict):
        return str(value.get("text") or value.get("name") or "")
    return str(value)


def pool_script_id(prompt_id: str) -> str:
    if not prompt_id or not all(c.isalnum() or c in "_-" for c in prompt_id):
        raise ValueError("invalid source prompt ID")
    return "wsr_" + prompt_id


def resolved_cart(value: Any) -> str:
    if value is True or value == "是":
        return "是"
    if value is False or value == "否":
        return "否"
    raise ValueError("script pool requires an explicit frozen cart setting")


def content_digest(payload: dict) -> str:
    # Publication settings may be overridden in the pool before production;
    # those overrides never rewrite or change the source content identity.
    value = {key: payload.get(key) for key in (
        "prompt_id", "full_prompt", "publish_purpose", "mother_id", "mother_version",
        "product_id", "sequence_no", "handoff_context",
    )}
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def build_pool_fields(payload: dict, product_images: list, persona_images: list) -> dict:
    purpose = payload.get("publish_purpose")
    if purpose not in {"带货", "养号"}:
        raise ValueError("WSR pool export requires 带货/养号 purpose")
    prompt = payload.get("full_prompt") or ""
    if not prompt.strip():
        raise ValueError("empty prompt cannot enter the script pool")
    context = payload.get("handoff_context") or {}
    sequence = int(payload.get("sequence_no") or 0)
    mode = "高保真" if payload.get("replication_mode") == "high_fidelity" else "一般复刻"
    route = str(payload.get("creative_route") or (f"H{sequence}" if sequence <= 3 else f"G{(sequence - 4) % 5 + 1}"))
    product = str(payload.get("product_id") or "")
    title = f"墨西哥｜{purpose}｜{context.get('mother_name') or '成功母版'}｜{product or '无商品'}｜{mode} {route}"
    if context.get("revision_kind"):
        label = "对照测试" if context["revision_kind"] in {"test", "comparison"} else "修订"
        title = f"{label}｜{title}"
    fields = {
        "脚本ID": pool_script_id(str(payload["prompt_id"])), "脚本标题": title,
        "产品编码": product, "店铺ID": str(context.get("store_id") or ""),
        "目标国家": str(context.get("target_country") or "墨西哥"),
        "目标语言": str(context.get("target_language") or "西班牙语"),
        "产品类型": str(context.get("product_type") or "假发"),
        "视频时长": int(context.get("video_duration") or 15),
        "批次ID": str(payload.get("batch_id") or ""),
        "批次ItemID": f"wsr:{payload['prompt_id']}", "批次序号": sequence,
        "完整生产脚本": prompt, "视频生成提示词": prompt,
        "口播_目标语言": str(payload.get("voiceover_text") or ""),
        "口播_中文": str(payload.get("voiceover_zh") or ""),
        "发布用途": purpose, "是否挂车": resolved_cart(payload.get("cart_enabled")),
        "脚本来源": SOURCE_KIND,
        "脚本类型": "养号脚本" if purpose == "养号" else "短视频复刻脚本",
        "视频形态（系统）": "短视频", "处理状态": "待审核", "进入生产": False,
        "产品图片": product_images, "人物参考图（系统）": persona_images,
        "场景摘要": str(payload.get("change_summary") or ""),
    }
    return fields


def register_metadata(fields: dict, payload: dict, db_path: str | None = None) -> dict:
    publisher = Path(__file__).resolve().parents[2] / "short-video-auto-publisher"
    if str(publisher) not in sys.path:
        sys.path.insert(0, str(publisher))
    from app.db import AutoPublishDB
    from app.script_pool import register_script_pool_metadata

    db = AutoPublishDB(Path(db_path)) if db_path else AutoPublishDB()
    context = payload.get("handoff_context") or {}
    first_registration = db.get_script_metadata(cell_text(fields.get("脚本ID"))) is None
    return register_script_pool_metadata(
        db, script_id=cell_text(fields.get("脚本ID")),
        source_record_id=cell_text(fields.get("_record_id")),
        prompt=cell_text(fields.get("视频生成提示词") or fields.get("短视频提示词")),
        product_id=cell_text(fields.get("产品编码")),
        platform_product_id=(context.get("platform_product_id")
                             if first_registration and cell_text(fields.get("产品编码")) == str(payload.get("product_id") or "")
                             else None),
        store_id=cell_text(fields.get("店铺ID")),
        publish_purpose=cell_text(fields.get("发布用途")), cart_enabled=cell_text(fields.get("是否挂车")),
        target_country=cell_text(fields.get("目标国家")), target_language=cell_text(fields.get("目标语言")),
        parent_slot=f"{payload.get('mother_id')}:v{payload.get('mother_version')}",
        direction_label=str(payload.get("creative_route") or ""),
        variant_strength=str(payload.get("replication_mode") or ""),
        product_type=cell_text(fields.get("产品类型")),
        content_family_key=f"wsr:{payload.get('mother_id')}:{payload.get('mother_version')}:{payload.get('product_id')}:{payload.get('publish_purpose')}",
        task_name=cell_text(fields.get("脚本标题")), script_source=SOURCE_KIND,
        content_branch="NURTURE" if cell_text(fields.get("发布用途")) == "养号" else "DIRECT_RESPONSE",
    )


@dataclass
class ScriptPoolWriter:
    client: Any
    repository: Any
    metadata_registrar: Callable = register_metadata
    pool_app_token: str = ""
    table_id: str = POOL_TABLE
    _index: dict | None = field(default=None, init=False, repr=False)
    _attachment_cache: dict = field(default_factory=dict, init=False, repr=False)

    def app_token(self):
        if not self.pool_app_token:
            self.pool_app_token = os.environ.get("WIG_REPLICATION_SCRIPT_POOL_APP_TOKEN", "").strip()
        if not self.pool_app_token:
            data = self.client.request("GET", "/wiki/v2/spaces/get_node", params={"token": POOL_WIKI_TOKEN})
            node = data.get("node") or {}
            if node.get("obj_type") != "bitable" or not node.get("obj_token"):
                raise ValueError("script pool wiki is not a bitable")
            self.pool_app_token = str(node["obj_token"])
        return self.pool_app_token

    @contextmanager
    def _lock(self, prompt_id):
        factory = getattr(self.repository, "connection_factory", None)
        if not callable(factory):
            yield
            return
        connection = factory()
        name = "wsr-pool:" + hashlib.sha256(prompt_id.encode()).hexdigest()[:45]
        acquired = False
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT GET_LOCK(%s, 0) AS acquired", (name,))
                acquired = bool((cursor.fetchone() or {}).get("acquired"))
            if not acquired:
                raise RuntimeError("script pool delivery already running; retry later")
            yield
        finally:
            if acquired:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT RELEASE_LOCK(%s)", (name,))
            connection.close()

    def _find(self, script_id, binding):
        app = self.app_token()
        if binding and binding.target_record_id:
            return self.client.get_record(app, self.table_id, binding.target_record_id)
        if self._index is None:
            self._index = {}
            for row in self.client.list_records(app, self.table_id, page_size=500):
                key = cell_text((row.get("fields") or {}).get("脚本ID"))
                if not key.startswith("wsr_"):
                    continue
                if key in self._index:
                    raise RuntimeError(f"duplicate script pool identity: {key}")
                self._index[key] = row
        return self._index.get(script_id)

    def _save_binding(self, prompt_id, row_id, digest, script_id, metadata):
        from wig_success_replication.repository import ScriptPoolBindingRecord
        self.repository.save_script_pool_binding(ScriptPoolBindingRecord(
            prompt_id=prompt_id, target_record_id=row_id, last_exported_hash=digest,
            script_id=script_id, metadata=metadata,
        ))

    def _attachments(self, values, role, prompt_id, script_id, metadata, expected_assets=None):
        result = []
        cache = metadata.setdefault("attachment_cache", {})
        for attachment in values:
            token = str(attachment.get("file_token") or "")
            if not token:
                raise ValueError(f"{role} reference missing stable file token")
            key = f"{self.app_token()}:{role}:{token}"
            expected = (expected_assets or {}).get(token)
            uploaded = cache.get(key) or self._attachment_cache.get(key)
            if expected and uploaded and uploaded.get("original_sha256") != expected["original_sha256"]:
                uploaded = None
            if not uploaded:
                source = {**attachment, **({"original_sha256": expected["original_sha256"]} if expected else {})}
                uploaded = self.client.copy_attachment_to_base(source, self.app_token())
                if expected and uploaded.get("original_sha256") != expected["original_sha256"]:
                    raise ValueError("reference copy did not verify original SHA256")
                cache[key] = uploaded
                # Preserve progress even if the next upload or record create fails.
                self._save_binding(prompt_id, "", "", script_id, metadata)
            cache[key] = uploaded
            self._attachment_cache[key] = uploaded
            # Provenance belongs in the binding, not unsupported Feishu cell keys.
            result.append({key: value for key, value in uploaded.items() if key in {"file_token", "name", "size", "type"}})
        return result

    def apply(self, operation: str, payload: dict):
        if operation != "upsert_script_pool":
            raise ValueError(f"unsupported pool operation: {operation}")
        prompt_id = str(payload["prompt_id"])
        with self._lock(prompt_id):
            return self._apply(payload)

    def _apply(self, payload):
        prompt_id = str(payload["prompt_id"])
        script_id = pool_script_id(prompt_id)
        digest = content_digest(payload)
        binding = self.repository.get_script_pool_binding(prompt_id)
        existing = self._find(script_id, binding)
        metadata = dict(binding.metadata if binding else {})
        context = payload.get("handoff_context") or {}
        # Revisions live outside cumulative prompt slots. The binding carries
        # their frozen context so normal pool -> run-table sync can resolve it.
        if context:
            metadata["frozen_handoff_context"] = {
                **context, "mother_id": payload.get("mother_id"),
                "mother_version": payload.get("mother_version"),
            }
        source_manifest = context.get("reference_manifest")
        expected_assets = {}
        if source_manifest:
            source_manifest = validate_reference_manifest(source_manifest)
            expected_assets = {asset["file_token"]: asset for asset in source_manifest["reference_assets"]}
            metadata["mother_core_points"] = context.get("mother_core_points") or []
            metadata["prompt_qa"] = context.get("prompt_qa") or {}
        if existing:
            fields = dict(existing.get("fields") or {})
            if cell_text(fields.get("脚本ID")) != script_id:
                raise RuntimeError("pool binding points to a different script")
            if binding and binding.last_exported_hash and binding.last_exported_hash != digest:
                raise RuntimeError("source content changed under the same script ID; create a new revision")
            if not (binding and binding.last_exported_hash) and cell_text(fields.get("视频生成提示词") or fields.get("短视频提示词")) != payload["full_prompt"]:
                raise RuntimeError("unbound pool row conflicts with source content; not overwriting")
            row_id = str(existing["record_id"])
            # A retry never resets human edits, use/reject decisions, or cart overrides.
            status = "existing_preserved"
        else:
            if not context:
                raise ValueError("frozen handoff context is required; use explicit legacy backfill")
            if source_manifest:
                expected_order = [("person_identity", str(item.get("file_token") or "")) for item in context.get("persona_images") or []]
                expected_order += [("product", str(item.get("file_token") or "")) for item in context.get("product_images") or []]
                if expected_order != [(asset["role"], asset["file_token"]) for asset in source_manifest["reference_assets"]]:
                    raise ValueError("pool references differ from compile manifest")
            product_images = self._attachments(context.get("product_images") or [], "product", prompt_id, script_id, metadata, expected_assets)
            persona_images = self._attachments(context.get("persona_images") or [], "persona", prompt_id, script_id, metadata, expected_assets)
            if source_manifest:
                token_map = {}
                for asset in source_manifest["reference_assets"]:
                    role = "persona" if asset["role"] == "person_identity" else "product"
                    token_map[asset["file_token"]] = metadata["attachment_cache"][f"{self.app_token()}:{role}:{asset['file_token']}"]
                metadata["reference_manifest"] = rebind_reference_manifest(source_manifest, token_map)
            fields = build_pool_fields(payload, product_images, persona_images)
            try:
                response = self.client.create_record(self.app_token(), self.table_id, fields)
            except Exception:
                self._index = None
                raise
            row = response.get("record") or response
            row_id = str(row.get("record_id") or "")
            if not row_id:
                # Invalidate the snapshot: a network-ambiguous create is reconciled
                # by the stable script ID on the next retry, never blindly repeated.
                self._index = None
                raise RuntimeError("Feishu create returned no record ID; reconcile before retry")
            if self._index is not None:
                self._index[script_id] = {"record_id": row_id, "fields": fields}
            status = "created"
        self._save_binding(prompt_id, row_id, digest, script_id, metadata)
        registered = self.metadata_registrar({**fields, "_record_id": row_id,
                                              "_first_pool_delivery": status == "created"}, payload)
        return {"status": status, "record_id": row_id, "script_id": script_id,
                "metadata": registered}
