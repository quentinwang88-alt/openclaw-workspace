"""Record-scoped first-frame bridge; never imports the other skill's ``core``.

The caller owns the sync process lock and its already-read source snapshot.
Only the explicit production path may invoke the paid first-frame subprocess.
"""
from __future__ import annotations

import json
import hashlib
from pathlib import Path
import subprocess
import sys
import tempfile

from core.sync import extract_attachments, normalize_checkbox, normalize_text


PROTOCOL = "first-frame-record-v1"
RUNNER = Path(__file__).resolve().parents[2] / "original-script-generator" / "scripts" / "run_first_frame_tasks.py"
PATCH_FIELDS = {
    "统一首帧（系统）", "首帧准备状态（系统）", "视觉参考模式（系统）",
}
READY_STATUSES = {"已确认", "CONFIRMED", "READY", "已就绪", "缓存复用"}


def first_frame_source_supported(task) -> bool:
    if task.script_source == "成功脚本复刻":
        return task.script_id.startswith("wsr_")
    return task.script_source in {"原创脚本", "原创生成"} and not task.script_id.startswith("wsr_")


def first_frame_preview_action(task) -> str:
    """A preview must not claim a waiting row can already be created."""
    if not task.reference_preparation_error:
        return ""
    if not first_frame_source_supported(task):
        return f"unsupported(first_frame_source={task.script_source})"
    return "wait(first_frame; sync will generate then sync)"


def generate_first_frame_from_snapshot(record, source_url: str, *, timeout: int = 900) -> dict:
    """Return an identity-checked, allowlisted patch; never rescan the table.

    Failures are not retried here. The first-frame worker owns durable asset
    idempotency and locks shared with its standalone entry point.
    """
    expected_id = normalize_text(record.fields.get("脚本ID"))
    request = {"protocol": PROTOCOL, "source_url": source_url,
               "record": {"record_id": record.record_id, "fields": record.fields}}
    snapshot = getattr(record, "execution_target_snapshot", None)
    if snapshot is not None:
        request["execution_target_snapshot"] = snapshot
    with tempfile.TemporaryDirectory(prefix="script-pool-first-frame-") as directory:
        request_path = Path(directory) / "request.json"
        result_path = Path(directory) / "result.json"
        request_path.write_text(json.dumps(request, ensure_ascii=False), encoding="utf-8")
        try:
            completed = subprocess.run(
                [sys.executable, str(RUNNER), "--record-snapshot", str(request_path),
                 "--result-json", str(result_path)],
                cwd=str(RUNNER.parent.parent), capture_output=True, text=True,
                check=False, timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("FIRST_FRAME_TIMEOUT:首帧处理超时；已保留进入生产勾选，重试时复用已完成资产") from exc
        if not result_path.exists():
            raise RuntimeError(f"FIRST_FRAME_RESULT_MISSING:首帧处理未返回结果（exit={completed.returncode}）")
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except (ValueError, OSError) as exc:
            raise RuntimeError("FIRST_FRAME_RESULT_INVALID:首帧处理结果无法读取") from exc
    if not isinstance(result, dict) or result.get("protocol") != PROTOCOL:
        raise RuntimeError("FIRST_FRAME_PROTOCOL_MISMATCH:首帧结果协议不匹配")
    if result.get("record_id") != record.record_id or result.get("script_id") != expected_id:
        raise RuntimeError("FIRST_FRAME_IDENTITY_MISMATCH:首帧结果与当前脚本不一致，禁止交接")
    if result.get("status") != "ready" or completed.returncode:
        raise RuntimeError(f"FIRST_FRAME_{str(result.get('status') or 'FAILED').upper()}:{result.get('error') or '首帧尚未就绪'}")
    fields = result.get("fields")
    if not isinstance(fields, dict):
        raise RuntimeError("FIRST_FRAME_RESULT_INVALID:缺少字段补丁")
    patch = {key: value for key, value in fields.items() if key in PATCH_FIELDS}
    attachments = patch.get("统一首帧（系统）")
    status = normalize_text(patch.get("首帧准备状态（系统）")).upper()
    if not isinstance(attachments, list) or not attachments or any(
        not isinstance(item, dict) or not item.get("file_token") for item in attachments
    ) or status not in READY_STATUSES:
        raise RuntimeError("FIRST_FRAME_RESULT_NOT_READY:首帧结果缺少可交接附件或就绪状态")
    return patch


def complete_wsr_reference_handoff(task, record, mapping: dict):
    """Keep identity/product references after the opening frame, WSR only."""
    if task.script_source != "成功脚本复刻" or task.reference_preparation_error:
        return task
    if not normalize_checkbox(record.fields.get(mapping.get("first_frame_requested"))):
        return task
    try:
        contract = json.loads(task.persona_contract or "{}")
    except ValueError:
        return task
    assets = contract.get("reference_assets", []) if isinstance(contract, dict) else []
    if not assets or any(asset.get("role") != "composite_first_frame" for asset in assets):
        return task
    grouped = [
        ("composite_first_frame", list(task.reference_images)),
        ("person_identity", extract_attachments(record.fields.get(mapping.get("person_images")))),
        ("product", extract_attachments(record.fields.get(mapping.get("product_images")))),
    ]
    references, roles = [], []
    labels = {"composite_first_frame": "统一首帧构图", "person_identity": "人物身份", "product": "商品外观"}
    for role, images in grouped:
        for image in images:
            references.append(image)
            roles.append({"index": len(references), "role": role})
    indices = "；".join(f"附件 {item['index']}：{labels[item['role']]}" for item in roles)
    note = (
        f"【当前实际附件角色说明｜优先于正文旧图序号】\n"
        f"本次实际上传 {len(references)} 张图，按实际顺序定义如下：{indices}。"
        "下文涉及原始参考图的张数、第几张、前几张人物或后几张商品等编号，仅描述上游素材，"
        "不代表本次上传附件；以本说明的新编号为准，不得据旧编号寻找或错用图。"
        "统一首帧仅负责开场构图；后续人物身份沿用人物参考图、商品外观沿用商品参考图，"
        "后续镜头动作及口播仍严格遵循原脚本，不把全片固定成静态首帧。"
    )
    contract["reference_assets"] = roles
    contract["source_reference_fingerprint"] = hashlib.sha256(
        json.dumps([image["file_token"] for image in references]).encode()
    ).hexdigest()
    contract["source_attachment_indices_superseded"] = True
    contract["execution_reference_note"] = note
    task.reference_images = references
    task.persona_contract = json.dumps(contract, ensure_ascii=False)
    return task


def apply_composite_reference_handoff(task, fields: dict, mapping: dict) -> dict:
    """Override historical image indices only in the downstream run snapshot."""
    if task.script_source != "成功脚本复刻" or task.reference_preparation_error:
        return fields
    try:
        contract = json.loads(task.persona_contract or "{}")
    except ValueError:
        return fields
    note = contract.get("execution_reference_note") if isinstance(contract, dict) else ""
    if not note or not contract.get("source_attachment_indices_superseded"):
        return fields
    result = dict(fields)
    if mapping.get("prompt"):
        result[mapping["prompt"]] = note + "\n\n" + str(fields.get(mapping["prompt"]) or "")
    return result
