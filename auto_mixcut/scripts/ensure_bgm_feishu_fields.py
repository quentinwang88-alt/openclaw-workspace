#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402
from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.core.ids import new_id  # noqa: E402
from auto_mixcut.skills.bgm_library_skill import BgmLibrarySkill  # noqa: E402


TEXT = {"type": 1, "ui_type": "Text"}
NUMBER = {"type": 2, "ui_type": "Number"}
SINGLE_SELECT = {"type": 3, "ui_type": "SingleSelect"}
MULTI_SELECT = {"type": 4, "ui_type": "MultiSelect"}
DATETIME = {"type": 5, "ui_type": "DateTime"}
CHECKBOX = {"type": 7, "ui_type": "Checkbox"}
URL = {"type": 15, "ui_type": "Url"}
ATTACHMENT = {"type": 17, "ui_type": "Attachment"}


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


MARKETS = ["越南", "泰国", "马来西亚", "东南亚通用"]
PLACEMENTS = ["自然流量", "广告投放", "店铺视频"]
CATEGORIES = ["发饰", "耳饰", "女装上装", "围巾帽子", "通用时尚"]
MOODS = ["轻甜", "日常干净", "柔和女性化", "时髦", "干净高级", "温暖舒适", "冬季柔和", "夏日清爽", "生活感", "活泼", "简洁干净"]
ENERGY = ["低", "中", "高"]
VOCAL = ["纯音乐", "轻人声", "明显人声", "未知"]
TAG_CONFIDENCE = ["高", "中", "低"]
LICENSE_STATUS = ["待确认", "可用", "限制", "不可用"]
TRACK_STATUS = ["候选", "待授权确认", "已批准", "生效中", "暂停", "已拒绝", "已过期"]
LICENSE_CHECK = ["新鲜", "待复查", "已过期", "未验证"]
ALERT_STATUS = ["待处理", "补充中", "已处理", "暂不处理"]
SOURCE_PLATFORMS = ["TikTok CML", "Artlist", "Epidemic Sound"]
SUBSCRIPTION_PLANS = ["Social", "Pro Music", "Pro Music & SFX", "Business", "未知"]
DOWNLOAD_VERSIONS = ["Full Mix", "Instrumental", "No Vocals", "Stem", "未知"]
FILE_FORMATS = ["MP3", "WAV", "M4A", "未知"]
CLEARLIST_STATUS = ["已加入", "未加入", "待确认", "不适用"]


TABLE_URLS = {
    "BGM素材库": "https://gcngopvfvo0q.feishu.cn/wiki/IFa5w98VBif8j7kIitIcLaqLncb?table=tblgdVFb6GDSPW3E&view=vewTfDXXBH",
    "BGM标杆库": "https://gcngopvfvo0q.feishu.cn/wiki/MQ8mwIdVXid9HyktRdAc3NiTncg?table=tbl9SSkSW0jEFSKX&view=vew0nf5aeN",
    "池子缺口告警": "https://gcngopvfvo0q.feishu.cn/wiki/WsHZwxS3VizKTPkVAYOcpGhtnDh?table=tbl8EJK9kWKVNMgG&view=vewYXOEHTW",
}

PRIMARY_FIELD_NAMES = {
    "BGM素材库": "BGM名称",
    "BGM标杆库": "标杆ID",
    "池子缺口告警": "告警ID",
}


TABLE_FIELDS: Dict[str, List[Dict[str, Any]]] = {
    "BGM素材库": [
        {"name": "BGM名称", **TEXT},
        {"name": "音频文件", **ATTACHMENT},
        {"name": "Artlist链接", **URL},
        {"name": "授权凭证文件", **ATTACHMENT},
        {"name": "备注", **TEXT},
        {"name": "AI情绪标签", **multi_select(MOODS)},
        {"name": "AI适用类目", **multi_select(CATEGORIES)},
        {"name": "AI能量", **single_select(ENERGY)},
        {"name": "AI人声类型", **single_select(VOCAL)},
        {"name": "状态", **single_select(["生效中", "暂停", "待处理"])},
        {"name": "BGM编号", **TEXT},
        {"name": "推荐次数", **NUMBER},
        {"name": "最近推荐时间", **DATETIME},
    ],
    "BGM标杆库": [
        {"name": "标杆ID", **TEXT},
        {"name": "BGM编号", **TEXT},
        {"name": "BGM名称", **TEXT},
        {"name": "Artlist链接", **URL},
        {"name": "主情绪标签", **single_select(MOODS)},
        {"name": "辅助情绪标签", **multi_select(MOODS)},
        {"name": "能量等级", **single_select(ENERGY)},
        {"name": "人声类型", **single_select(VOCAL)},
        {"name": "适用类目", **multi_select(CATEGORIES)},
        {"name": "为什么属于该标签", **TEXT},
        {"name": "负责人确认", **CHECKBOX},
        {"name": "备注", **TEXT},
    ],
    "池子缺口告警": [
        {"name": "告警ID", **TEXT},
        {"name": "类目", **single_select(CATEGORIES)},
        {"name": "情绪标签", **single_select(MOODS)},
        {"name": "无可用BGM次数", **NUMBER},
        {"name": "降级推荐次数", **NUMBER},
        {"name": "缺口原因", **TEXT},
        {"name": "建议补充方向", **TEXT},
        {"name": "告警状态", **single_select(ALERT_STATUS)},
        {"name": "最近命中时间", **DATETIME},
    ],
}


def resolve_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise RuntimeError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


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
    return bool(wanted) and any(item not in current for item in wanted)


def update_field_property(client: FeishuBitableClient, field_id: str, spec: Dict[str, Any]) -> None:
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{client.app_token}/tables/{client.table_id}/fields/{field_id}"
    )
    current_options = option_names(spec.get("property"))
    payload = {
        "field_name": spec["name"],
        "type": spec["type"],
        "ui_type": spec["ui_type"],
    }
    if "property" in spec:
        payload["property"] = {"options": [{"name": item, "color": idx % 54} for idx, item in enumerate(current_options)]}
    response = client._request("PUT", url, headers=client._headers(), json=payload)
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"更新字段选项失败: {spec['name']} {result.get('msg')}")


def delete_field(client: FeishuBitableClient, field_id: str, field_name: str) -> None:
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{client.app_token}/tables/{client.table_id}/fields/{field_id}"
    )
    response = client._request("DELETE", url, headers=client._headers())
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"删除字段失败: {field_name} {result.get('msg')}")


def rename_field(client: FeishuBitableClient, field_id: str, spec: Dict[str, Any]) -> None:
    update_field_property(client, field_id, spec)


def normalize_primary_field(
    client: FeishuBitableClient,
    table_name: str,
    existing_fields: Dict[str, Any],
    dry_run: bool,
) -> Dict[str, Any]:
    primary_target = PRIMARY_FIELD_NAMES.get(table_name)
    if not primary_target or "文本" not in existing_fields:
        return {"renamed_primary": None, "deleted_duplicate_primary": None}
    target_spec = next((spec for spec in TABLE_FIELDS[table_name] if spec["name"] == primary_target), None)
    if not target_spec:
        return {"renamed_primary": None, "deleted_duplicate_primary": None}
    duplicate = existing_fields.get(primary_target)
    if not dry_run and duplicate:
        records = client.list_records(page_size=500)
        for record in records:
            duplicate_value = record.fields.get(primary_target)
            primary_value = record.fields.get("文本")
            if duplicate_value not in (None, "", []) and primary_value in (None, "", []):
                client.update_record_fields(record.record_id, {"文本": duplicate_value})
        delete_field(client, duplicate.field_id, primary_target)
    if not dry_run:
        rename_field(client, existing_fields["文本"].field_id, target_spec)
    return {"renamed_primary": primary_target, "deleted_duplicate_primary": primary_target if duplicate else None}


def ensure_table(table_name: str, dry_run: bool = False, prune_extra: bool = False) -> Dict[str, Any]:
    client = resolve_client(TABLE_URLS[table_name])
    existing_fields = {field.field_name: field for field in client.list_fields()}
    wanted_names = {spec["name"] for spec in TABLE_FIELDS[table_name]}
    created: List[str] = []
    skipped: List[str] = []
    updated: List[str] = []
    deleted: List[str] = []
    failed: List[Dict[str, str]] = []
    primary_result = {"renamed_primary": None, "deleted_duplicate_primary": None}
    if prune_extra:
        try:
            primary_result = normalize_primary_field(client, table_name, existing_fields, dry_run=dry_run)
            if not dry_run and primary_result["renamed_primary"]:
                existing_fields = {field.field_name: field for field in client.list_fields()}
        except Exception as exc:
            failed.append({"field": "文本", "error": str(exc)})
    if prune_extra:
        for name, field in existing_fields.items():
            if name not in wanted_names:
                try:
                    if not dry_run:
                        delete_field(client, field.field_id, name)
                    deleted.append(name)
                except Exception as exc:
                    failed.append({"field": name, "error": str(exc)})
        if deleted and not dry_run:
            existing_fields = {field.field_name: field for field in client.list_fields()}
    for spec in TABLE_FIELDS[table_name]:
        name = spec["name"]
        if name in existing_fields:
            if should_update_select_options(existing_fields[name], spec):
                try:
                    if not dry_run:
                        update_field_property(client, existing_fields[name].field_id, spec)
                    updated.append(name)
                except Exception as exc:
                    failed.append({"field": name, "error": str(exc)})
            skipped.append(name)
            continue
        try:
            if not dry_run:
                client.create_field(
                    field_name=name,
                    field_type=int(spec["type"]),
                    ui_type=str(spec["ui_type"]),
                    property=spec.get("property"),
                )
            created.append(name)
        except Exception as exc:
            failed.append({"field": name, "error": str(exc)})
    return {
        "table": table_name,
        "created": created,
        "updated": updated,
        "deleted": deleted,
        **primary_result,
        "skipped": skipped,
        "failed": failed,
    }


MOOD_TO_CN = {
    "cute_light": "轻甜",
    "daily_clean": "日常干净",
    "soft_feminine": "柔和女性化",
    "fashion_chic": "时髦",
    "premium_clean": "干净高级",
    "warm_cozy": "温暖舒适",
    "winter_soft": "冬季柔和",
    "fresh_summer": "夏日清爽",
    "calm_lifestyle": "生活感",
    "energetic": "活泼",
    "minimal_clean": "简洁干净",
}
CATEGORY_TO_CN = {
    "hair_accessories": "发饰",
    "earrings": "耳饰",
    "womens_top": "女装上装",
    "scarf_hat": "围巾帽子",
    "generic_fashion": "通用时尚",
}
ENERGY_TO_CN = {"low": "低", "medium": "中", "high": "高"}
VOCAL_TO_CN = {"instrumental": "纯音乐", "light_vocal": "轻人声", "vocal": "明显人声", "unknown": "未知"}


def autofill_bgm_tags(dry_run: bool = False) -> Dict[str, Any]:
    client = resolve_client(TABLE_URLS["BGM素材库"])
    bgm = BgmLibrarySkill(build_context())
    updated: List[Dict[str, Any]] = []
    skipped: List[str] = []
    failed: List[Dict[str, str]] = []
    for record in client.list_records(page_size=500):
        fields = dict(record.fields)
        if not fields.get("音频文件"):
            skipped.append(record.record_id)
            continue
        try:
            track = _feishu_bgm_track(fields)
            tag_check = bgm.check_metadata_tags(track)
            values = {
                "BGM编号": fields.get("BGM编号") or track.get("bgm_id") or new_id("BGM"),
                "AI情绪标签": [MOOD_TO_CN.get(item, item) for item in tag_check.get("mood_tags", [])],
                "AI适用类目": [CATEGORY_TO_CN.get(item, item) for item in tag_check.get("category_tags", [])],
                "AI能量": ENERGY_TO_CN.get(tag_check.get("energy_level"), "中"),
                "AI人声类型": VOCAL_TO_CN.get(tag_check.get("vocal_type"), "未知"),
                "状态": fields.get("状态") or "生效中",
            }
            if not fields.get("BGM名称") and track.get("track_name"):
                values["BGM名称"] = track["track_name"]
            if not dry_run:
                client.update_record_fields(record.record_id, values)
            updated.append({"record_id": record.record_id, "fields": values})
        except Exception as exc:
            failed.append({"record_id": record.record_id, "error": str(exc)})
    return {"table": "BGM素材库", "updated": updated, "skipped": skipped, "failed": failed}


def _feishu_bgm_track(fields: Dict[str, Any]) -> Dict[str, Any]:
    file_name = _attachment_name(fields.get("音频文件")) or _text(fields.get("BGM名称")) or "bgm.mp3"
    source_url = _urlish(fields.get("Artlist链接")) or _urlish(fields.get("来源链接"))
    return {
        "bgm_id": _text(fields.get("BGM编号")) or new_id("BGM"),
        "track_name": _text(fields.get("BGM名称")) or Path(file_name).stem,
        "artist_name": _text(fields.get("Artist/来源")),
        "source_platform": "feishu",
        "source_url": source_url,
        "file_name": file_name,
        "official_tags_json": "[]",
        "license_note": _text(fields.get("授权信息")),
    }


def _attachment_name(value: Any) -> str:
    if isinstance(value, list):
        for item in value:
            name = _attachment_name(item)
            if name:
                return name
        return ""
    if isinstance(value, dict):
        return str(value.get("name") or value.get("file_name") or "").strip()
    return ""


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ("text", "name", "value", "link", "url"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
        return ""
    if isinstance(value, list):
        return ", ".join(item for item in (_text(v) for v in value) if item)
    return str(value).strip()


def _urlish(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("link", "url"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
    return _text(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--prune-extra", action="store_true")
    parser.add_argument("--autofill-tags", action="store_true")
    args = parser.parse_args()
    if args.autofill_tags:
        results = [autofill_bgm_tags(dry_run=args.dry_run)]
    else:
        results = [ensure_table(name, dry_run=args.dry_run, prune_extra=args.prune_extra) for name in TABLE_FIELDS]
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 1 if any(item["failed"] for item in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
