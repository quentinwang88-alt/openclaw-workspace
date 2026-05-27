#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402


TEXT = {"type": 1, "ui_type": "Text"}
NUMBER = {"type": 2, "ui_type": "Number"}
SINGLE_SELECT = {"type": 3, "ui_type": "SingleSelect"}
MULTI_SELECT = {"type": 4, "ui_type": "MultiSelect"}
DATETIME = {"type": 5, "ui_type": "DateTime"}
CHECKBOX = {"type": 7, "ui_type": "Checkbox"}
URL = {"type": 15, "ui_type": "Url"}
ATTACHMENT = {"type": 17, "ui_type": "Attachment"}


DEFAULT_URLS = {
    "商品内容任务表": "https://gcngopvfvo0q.feishu.cn/wiki/PO2bwgrGaiOPcnkxXI8cq3fsnzg?table=tblIy2XkKc2144Pm&view=vew84aAgfU",
    "商品锚点卡确认队列": "https://gcngopvfvo0q.feishu.cn/wiki/V35wwjDLYiMFeTkiVFPc7SM5nvd?table=tbl2QRHwF7g9CmaF&view=vewv752AHQ",
    "商品素材上传表": "https://gcngopvfvo0q.feishu.cn/wiki/KhzowIkkbi4Di6kOQRDcvd1NnYe?table=tblBj3UCaBRicSKS&view=vewcSyAc8S",
    "人工复核队列表": "https://gcngopvfvo0q.feishu.cn/wiki/Hha6wcSJKidWbCkNkfocTTINnGg?table=tblEqdnodcuJsrut&view=vewfDtzs9g",
    "成片质检表": "https://gcngopvfvo0q.feishu.cn/wiki/VjuzwPingiuTWxk8yAjc4gQvn8f?table=tbl43RnlocUGin5a&view=vewiKniVVC",
}


def single_select(options: List[str]) -> Dict[str, Any]:
    return {
        "type": SINGLE_SELECT["type"],
        "ui_type": SINGLE_SELECT["ui_type"],
        "property": {"options": [{"name": item, "color": idx % 54} for idx, item in enumerate(options)]},
    }


def multi_select(options: List[str]) -> Dict[str, Any]:
    return {
        "type": MULTI_SELECT["type"],
        "ui_type": MULTI_SELECT["ui_type"],
        "property": {"options": [{"name": item, "color": idx % 54} for idx, item in enumerate(options)]},
    }


MARKETS = ["VN", "TH", "MY", "PH", "ID", "SG"]
CATEGORIES = ["hair_accessories", "earrings", "womens_tops", "scarves", "hats", "general"]
UPLOAD_CATEGORIES = ["hair_accessories", "earrings", "womens_top", "scarf_hat", "other"]
PRIORITIES = ["low", "normal", "high", "urgent"]
TASK_TYPES = ["mixcut", "benchmark", "rerender"]
MATERIAL_TIERS = ["tier_0_not_ready", "tier_1_minimum", "tier_2_standard", "tier_3_full"]
MATERIAL_STATUS = ["not_ready", "ready", "review_required", "blocked", "failed"]
TASK_STATUS = [
    "CREATED",
    "ANCHOR_PENDING",
    "ANCHOR_DRAFTED",
    "ANCHOR_CONFIRMED",
    "RAW_UPLOADED",
    "PROBED",
    "WATERMARK_CHECKED",
    "SEGMENTED",
    "FRAMES_SAMPLED",
    "AI_TAGGED",
    "EFFECTIVE_ROLES_COMPUTED",
    "REVIEW_REQUIRED",
    "REVIEW_SKIPPED",
    "READINESS_CHECKED",
    "RENDER_PLAN_CREATED",
    "RENDERING",
    "RENDERED",
    "MACHINE_QC_PASSED",
    "MACHINE_QC_FAILED",
    "FEISHU_PREVIEW_SYNCED",
    "HUMAN_QC_PASSED",
    "HUMAN_QC_REJECTED",
    "DONE",
    "CLEANED",
    "FAILED",
]
ANCHOR_STATUS = ["pending", "drafted", "confirmed", "rejected", "needs_revision"]
ROLES = ["hero", "detail", "result", "scene", "ending", "unusable"]
VISIBILITY = ["high", "medium", "low"]
HOOK = ["strong", "medium", "weak"]
USABILITY = ["yes", "needs_processing", "no"]
RISK = ["low", "medium", "high"]
CONFIDENCE = ["high", "medium", "low"]
MATCH_STATUS = ["trusted_by_source", "anchor_pass", "uncertain", "mismatch", "human_confirmed"]
REVIEW_STATUS = ["待复核", "已通过", "已修正", "废弃"]
QC_STATUS = ["pending", "passed", "failed", "borderline"]
HUMAN_QC_STATUS = ["待检查", "可发布", "需修改", "不可发布"]
SOURCE_TYPES = ["供应商素材", "自有拍摄", "授权达人素材", "AI生成素材", "竞品素材", "抖音/搬运素材", "程序自动抓取素材", "通用场景素材", "其他"]
PRODUCT_BINDING_TYPES = ["当前商品同款", "同款/高度相似款", "同类目参考", "通用场景", "不确定"]
UPLOAD_STATUS = ["pending_upload", "file_received", "uploading_to_oss", "oss_uploaded", "upload_failed", "local_cleaned", "skipped"]
PROCESS_STATUS = [
    "pending",
    "anchor_missing",
    "probing",
    "probe_failed",
    "watermark_checking",
    "rejected_watermark",
    "segmenting",
    "segment_failed",
    "frame_sampling",
    "ai_tagging",
    "consistency_checking",
    "effective_roles_computing",
    "review_required",
    "usable",
    "scene_only",
    "rejected",
    "completed",
    "failed",
]


TABLE_FIELDS: Dict[str, List[Dict[str, Any]]] = {
    "商品内容任务表": [
        {"name": "商品ID", **TEXT},
        {"name": "商品名称", **TEXT},
        {"name": "市场", **single_select(MARKETS)},
        {"name": "类目", **single_select(CATEGORIES)},
        {"name": "店铺", **TEXT},
        {"name": "优先级", **single_select(PRIORITIES)},
        {"name": "任务类型", **single_select(TASK_TYPES)},
        {"name": "目标生成数量", **NUMBER},
        {"name": "系统允许生成数量", **NUMBER},
        {"name": "实际生成数量", **NUMBER},
        {"name": "素材等级", **single_select(MATERIAL_TIERS)},
        {"name": "素材状态", **single_select(MATERIAL_STATUS)},
        {"name": "混剪状态", **single_select(TASK_STATUS)},
        {"name": "锚点状态", **single_select(ANCHOR_STATUS)},
        {"name": "素材缺口说明", **TEXT},
        {"name": "失败原因", **TEXT},
        {"name": "最近成片预览", **URL},
        {"name": "人工备注", **TEXT},
    ],
    "商品锚点卡确认队列": [
        {"name": "商品ID", **TEXT},
        {"name": "商品名称", **TEXT},
        {"name": "市场", **single_select(MARKETS)},
        {"name": "类目", **single_select(CATEGORIES)},
        {"name": "商品主图", **ATTACHMENT},
        {"name": "AI生成锚点卡", **TEXT},
        {"name": "核心视觉点", **TEXT},
        {"name": "不可错识别点", **TEXT},
        {"name": "禁用错配项", **TEXT},
        {"name": "适用核心镜头", **multi_select(["hero", "detail", "result"])},
        {"name": "人工确认状态", **single_select(["待确认", "已确认", "需修改", "驳回"])},
        {"name": "人工修正内容", **TEXT},
        {"name": "确认人", **TEXT},
        {"name": "确认时间", **DATETIME},
        {"name": "备注", **TEXT},
    ],
    "商品素材上传表": [
        {"name": "商品ID", **TEXT},
        {"name": "市场", **single_select(MARKETS)},
        {"name": "类目", **single_select(UPLOAD_CATEGORIES)},
        {"name": "素材文件", **ATTACHMENT},
        {"name": "素材来源 source_type", **single_select(SOURCE_TYPES)},
        {"name": "商品绑定类型 product_binding_type", **single_select(PRODUCT_BINDING_TYPES)},
        {"name": "备注", **TEXT},
        {"name": "上传状态", **single_select(UPLOAD_STATUS)},
        {"name": "处理状态", **single_select(PROCESS_STATUS)},
        {"name": "素材ID", **TEXT},
        {"name": "OSS路径", **TEXT},
        {"name": "文件类型", **single_select(["image", "video"])},
        {"name": "文件大小", **NUMBER},
        {"name": "时长ms", **NUMBER},
        {"name": "分辨率", **TEXT},
        {"name": "是否有水印", **single_select(["pending", "yes", "no", "unknown"])},
        {"name": "水印类型", **single_select(["TikTok", "Douyin", "平台UI", "用户ID", "明显水印", "其他"])},
        {"name": "片段数量", **NUMBER},
        {"name": "可用片段数量", **NUMBER},
        {"name": "处理失败原因", **TEXT},
        {"name": "最近处理时间", **DATETIME},
        {"name": "是否已清理飞书附件", **single_select(["否", "是", "失败"])},
    ],
    "人工复核队列表": [
        {"name": "片段ID", **TEXT},
        {"name": "商品ID", **TEXT},
        {"name": "市场", **single_select(MARKETS)},
        {"name": "类目", **single_select(CATEGORIES)},
        {"name": "片段预览链接", **URL},
        {"name": "封面图", **ATTACHMENT},
        {"name": "AI镜头用途", **single_select(ROLES)},
        {"name": "AI商品可见度", **single_select(VISIBILITY)},
        {"name": "AI首镜强度", **single_select(HOOK)},
        {"name": "AI可混剪判断", **single_select(USABILITY)},
        {"name": "AI风险等级", **single_select(RISK)},
        {"name": "AI置信度", **single_select(CONFIDENCE)},
        {"name": "AI判断理由", **TEXT},
        {"name": "商品匹配状态", **single_select(MATCH_STATUS)},
        {"name": "有效镜位", **multi_select(["hero", "detail", "result", "scene", "ending"])},
        {"name": "人工修正镜头用途", **single_select(ROLES)},
        {"name": "人工修正商品可见度", **single_select(VISIBILITY)},
        {"name": "人工修正首镜强度", **single_select(HOOK)},
        {"name": "人工修正可混剪", **single_select(USABILITY)},
        {"name": "人工修正风险等级", **single_select(RISK)},
        {"name": "人工商品匹配判断", **single_select(MATCH_STATUS)},
        {"name": "复核状态", **single_select(REVIEW_STATUS)},
        {"name": "备注", **TEXT},
    ],
    "成片质检表": [
        {"name": "输出ID", **TEXT},
        {"name": "商品ID", **TEXT},
        {"name": "批次ID", **TEXT},
        {"name": "变体编号", **NUMBER},
        {"name": "模板ID", **TEXT},
        {"name": "视频预览链接", **URL},
        {"name": "封面图", **ATTACHMENT},
        {"name": "机器质检状态", **single_select(QC_STATUS)},
        {"name": "人工质检状态", **single_select(HUMAN_QC_STATUS)},
        {"name": "是否可发布", **CHECKBOX},
        {"name": "失败原因", **TEXT},
        {"name": "飞书展示到期时间", **DATETIME},
        {"name": "备注", **TEXT},
    ],
}


def resolve_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise RuntimeError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in feishu_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def ensure_table(table_name: str, feishu_url: str, dry_run: bool = False) -> Dict[str, Any]:
    client = resolve_client(feishu_url)
    existing = {field.field_name for field in client.list_fields()}
    existing_fields = {field.field_name: field for field in client.list_fields()}
    created: List[str] = []
    skipped: List[str] = []
    updated: List[str] = []
    failed: List[Dict[str, str]] = []
    for spec in TABLE_FIELDS[table_name]:
        name = spec["name"]
        if name in existing:
            if should_update_select_options(existing_fields[name], spec):
                if not dry_run:
                    update_field_property(client, existing_fields[name].field_id, spec)
                updated.append(name)
            skipped.append(name)
            continue
        if dry_run:
            created.append(name)
            continue
        try:
            client.create_field(
                field_name=name,
                field_type=int(spec["type"]),
                ui_type=str(spec["ui_type"]),
                property=spec.get("property"),
            )
            created.append(name)
        except Exception as exc:  # Keep going so one bad field does not block the rest.
            failed.append({"field": name, "error": str(exc)})
    return {
        "table": table_name,
        "url": feishu_url,
        "app_token": client.app_token,
        "table_id": client.table_id,
        "created": created,
        "skipped": skipped,
        "updated": updated,
        "failed": failed,
    }


def option_names(property_value: Any) -> List[str]:
    if not isinstance(property_value, dict):
        return []
    options = property_value.get("options") or []
    return [str(item.get("name") or "").strip() for item in options if isinstance(item, dict) and item.get("name")]


def should_update_select_options(existing_field: Any, spec: Dict[str, Any]) -> bool:
    if spec.get("ui_type") not in {"SingleSelect", "MultiSelect"}:
        return False
    wanted = option_names(spec.get("property"))
    current = option_names(getattr(existing_field, "property", None))
    return bool(wanted) and wanted != current


def update_field_property(client: FeishuBitableClient, field_id: str, spec: Dict[str, Any]) -> None:
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{client.app_token}/tables/{client.table_id}/fields/{field_id}"
    )
    payload = {
        "field_name": spec["name"],
        "type": spec["type"],
        "ui_type": spec["ui_type"],
        "property": spec.get("property"),
    }
    response = client._request("PUT", url, headers=client._headers(), json=payload)
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"更新字段选项失败: {spec['name']} {result.get('msg')}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    results = [ensure_table(name, url, dry_run=args.dry_run) for name, url in DEFAULT_URLS.items()]
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 1 if any(item["failed"] for item in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
