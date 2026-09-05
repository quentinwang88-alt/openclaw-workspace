"""Bind WSR identities to transferred bytes and the actual target tokens."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

WORKSPACE = Path(__file__).resolve().parents[3]
for root in (WORKSPACE, WORKSPACE / "packages" / "wig_success_replication"):
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
from wig_success_replication.reference_manifest import build_reference_manifest, validate_reference_manifest


def load_frozen_wsr_context(script_id: str) -> dict:
    """One parameterized read per selected WSR, never a Feishu table scan."""
    from workspace_support import load_repo_env
    load_repo_env()
    url = os.environ.get("WIG_REPLICATION_DATABASE_URL") or os.environ.get("LIKEU_AI_DATABASE_URL")
    if not url:
        return {"provenance": "final_prompt_only", "reference_manifest": None}
    import pymysql
    from pymysql.cursors import DictCursor
    parsed = urlparse(url)
    if parsed.scheme not in {"mysql", "mysql+pymysql"}:
        raise ValueError("WSR_REFERENCE_DATABASE_URL_INVALID")
    connection = pymysql.connect(host=parsed.hostname, port=parsed.port or 3306,
        user=unquote(parsed.username or ""), password=unquote(parsed.password or ""),
        database=unquote(parsed.path.lstrip("/")), charset=(parse_qs(parsed.query).get("charset") or ["utf8mb4"])[0],
        cursorclass=DictCursor, autocommit=False, connect_timeout=10, read_timeout=20, write_timeout=20)
    try:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT p.handoff_context_json,p.mother_id,p.mother_version,b.metadata_json
                FROM wsr_replication_prompt p LEFT JOIN wsr_script_pool_binding b ON b.prompt_id=p.prompt_id
                WHERE p.prompt_id=%s""", (script_id.removeprefix("wsr_"),))
            row = cursor.fetchone()
            if not row:
                # Explicit revisions have no cumulative prompt slot. Read the
                # single stable binding, never scan a Feishu table or guess a mother.
                cursor.execute("SELECT metadata_json FROM wsr_script_pool_binding WHERE prompt_id=%s AND script_id=%s",
                               (script_id.removeprefix("wsr_"), script_id))
                binding_row = cursor.fetchone()
                if binding_row:
                    metadata = json.loads(binding_row.get("metadata_json") or "{}")
                    frozen = metadata.get("frozen_handoff_context") or {}
                    row = {"handoff_context_json": json.dumps(frozen),
                           "metadata_json": json.dumps(metadata),
                           "mother_id": frozen.get("mother_id"),
                           "mother_version": frozen.get("mother_version")}
    finally:
        connection.close()
    if not row:
        return {"provenance": "final_prompt_only", "reference_manifest": None}
    context = json.loads(row.get("handoff_context_json") or "{}")
    binding = json.loads(row.get("metadata_json") or "{}")
    return {**context, "reference_manifest": binding.get("reference_manifest") or context.get("reference_manifest"),
            "mother_id": row.get("mother_id"), "mother_version": row.get("mother_version"),
            "provenance": "frozen_mother" if context.get("mother_core_points") else "final_prompt_only"}


def bind_transferred_wsr_references(task: Any, fields: dict, mapping: dict, cache: dict, context: dict) -> dict:
    if task.script_source != "成功脚本复刻" or not task.reference_images:
        return fields
    target = fields.get(mapping.get("reference_images")) or []
    if len(target) != len(task.reference_images):
        raise ValueError("WSR_REFERENCE_TRANSFER_COUNT_MISMATCH")
    contract = json.loads(task.persona_contract or "{}")
    roles = contract.get("reference_assets") or []
    if len(roles) != len(target):
        raise ValueError("WSR_REFERENCE_ROLE_COUNT_MISMATCH")
    frozen = context.get("reference_manifest")
    known = validate_reference_manifest(frozen)["reference_assets"] if frozen else []
    frozen_tokens = {asset.get("file_token"): asset for asset in known}
    assets = []
    for index, (source, uploaded, role) in enumerate(zip(task.reference_images, target, roles), 1):
        source_token = str(source.get("file_token") or "")
        transferred = cache.get(source_token) or {}
        digest = transferred.get("_transfer_sha256")
        if not digest or transferred.get("file_token") != uploaded.get("file_token"):
            raise ValueError("WSR_REFERENCE_TRANSFER_HASH_MISSING")
        previous = frozen_tokens.get(source_token)
        if previous and previous.get("role") != role.get("role"):
            raise ValueError("WSR_REFERENCE_ROLE_CHANGED")
        if previous and digest not in {previous.get("derived_sha256"), previous.get("original_sha256")}:
            raise ValueError("WSR_REFERENCE_TRANSFER_BYTES_CHANGED")
        # Tokens change during uploads. A matching digest+role retains raw
        # identity; a genuinely replaced image starts a new identity.
        if previous is None:
            previous = next((asset for asset in known if asset.get("role") == role.get("role") and
                             digest in {asset.get("derived_sha256"), asset.get("original_sha256")}), None)
        assets.append({"index": index, "role": role["role"], "source_file_token": source_token,
            "file_token": uploaded["file_token"], "original_sha256": previous["original_sha256"] if previous else digest,
            "derived_sha256": digest, "transform": previous.get("transform", "identity") if previous and digest == previous.get("derived_sha256") else "identity",
            "name": uploaded.get("name") or source.get("name") or "", "source_record_id": task.source_record_id})
    manifest = build_reference_manifest(assets)
    contract.update(schema_version="2", reference_manifest=manifest, reference_assets=assets,
                    source_reference_fingerprint=manifest["manifest_id"],
                    mother_core_points=context.get("mother_core_points") or [],
                    mother_id=context.get("mother_id"), mother_version=context.get("mother_version"),
                    mother_core_provenance=context.get("provenance") or "final_prompt_only")
    for key in ("frozen_mother_core_points", "effective_checkpoints", "allowed_changes",
                "execution_summary", "generation_provenance", "revision_kind", "parent_prompt_id"):
        if key in context:
            contract[key] = context[key]
    result = dict(fields)
    if not mapping.get("persona_contract"):
        raise ValueError("WSR_REFERENCE_CONTRACT_FIELD_MISSING")
    result[mapping["persona_contract"]] = json.dumps(contract, ensure_ascii=False)
    return result
