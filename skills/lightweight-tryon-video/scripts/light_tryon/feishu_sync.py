from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from .database import LightTryonDB
from .feishu_client import make_client
from .feishu_mappings import (
    CATEGORY_TO_BACKEND,
    ENUM_MAPS_TO_BACKEND,
    ENUM_MAPS_TO_FEISHU,
    MARKET_TO_FEISHU,
    TABLE_MAPPINGS,
)
from .prompting import PROMPT_BUILDER_VERSION, build_prompt
from .review_video_processing import process_review_videos
from .visual_plans import create_confirmed_video_jobs
from .utils import json_dumps, json_loads, normalized_list, now_iso, stable_hash


TEMPLATE_ROLES = ("persona", "scene", "action", "shot_plan", "styling", "subtitle")
SYSTEM_FIELDS = {"sync_status", "last_synced_at", "sync_error", "created_at", "feishu_updated_at"}
CLEARABLE_TEMPLATE_BACKENDS = {"inner_type", "inner_color", "inner_requirements"}
CLEARABLE_SCENE_BACKENDS = {
    "scene_style", "background_type_pool", "background_cleanliness", "edge_decor_pool",
    "decor_count", "decor_position", "key_light_direction",
}
READ_ONLY_TYPES = {1001, 1002}
LIST_BACKENDS = {
    "account_ids", "markets", "reference_images", "fixed_accessories", "applicable_categories",
    "required_anchors", "optional_anchors", "forbidden_elements", "applicable_scenes", "applicable_shot_profiles", "action_steps",
    "free_hand_action", "applicable_product_type", "product_fit", "bottom_color", "bottom_fit", "vibe_tag",
    "selling_point_angle", "applicable_category", "product_images", "abnormal_types", "background_type_pool", "edge_decor_pool",
}

MANUAL_REVIEW_FIELDS = {
    "manual_review_status", "manual_review_reason", "need_regeneration", "regeneration_strategy",
    "publish_status", "published_at", "publish_url", "operator_notes", "raw_video_attachments", "final_video_attachments",
    "generation_channel", "generation_model", "duration_seconds", "generation_rerun",
}

GENERATION_CHANNEL_TO_BACKEND = {"不生成": "no_generate", "自动": "auto", "即梦": "jimeng", "iMini": "imini"}
GENERATION_CHANNEL_TO_FEISHU = {value: key for key, value in GENERATION_CHANNEL_TO_BACKEND.items()}
RUN_MANAGER_STATUS_TO_FEISHU = {
    "not_submitted": "未提交", "pending": "待入队", "queued": "已入队", "generating": "生成中",
    "returned": "已回流", "failed": "失败", "blocked": "阻塞",
}

GENERATION_TO_FEISHU = {
    "pending": "待生成", "generating": "生成中", "success": "生成成功", "failed": "生成失败", "retrying": "重试中",
}
QC_TO_FEISHU = {"pending": "待质检", "passed": "通过", "manual_review": "需人工复核", "failed": "不通过"}
MANUAL_TO_BACKEND = {"待复核": "pending", "通过": "passed", "打回": "rejected", "淘汰": "discarded"}
MANUAL_TO_FEISHU = {value: key for key, value in MANUAL_TO_BACKEND.items()}
REGEN_TO_BACKEND = {"原配置重试": "retry_same", "更换动作": "change_action", "更换场景": "change_scene", "更换搭配": "change_styling", "改写Prompt": "rewrite_prompt", "人工处理": "manual"}
REGEN_TO_FEISHU = {value: key for key, value in REGEN_TO_BACKEND.items()}
PUBLISH_TO_BACKEND = {"未排期": "unscheduled", "待发布": "ready", "已发布": "published", "暂停发布": "paused"}
PUBLISH_TO_FEISHU = {value: key for key, value in PUBLISH_TO_BACKEND.items()}

# 默认模板使用机器枚举；运营表只展示更易读的中文值。未列出的可选枚举初始化时留空，
# 回读也不会用空值覆盖 SQLite 中的成熟配置。
VALUE_TO_FEISHU: dict[str, dict[str, str]] = {
    "brand_overlay_enabled": {"enabled": "启用", "disabled": "停用"},
    "brand_style_preset": {"cream_serif": "奶油衬线", "minimal_sans": "极简无衬线"},
    "brand_primary_color": {"cream_white": "奶油白", "white": "纯白", "light_gold": "浅金", "warm_gray": "暖灰"},
    "scene_type": {"environment": "环境模板", "main_full_body": "主场景全身", "half_body_detail": "同场景半身", "variation_full_body": "轻变化全身", "upper_body_fixed": "高亮上半身固定", "slow_push_in": "缓慢推近"},
    "shot_profile_id": {"SHOT_FULL_FIXED": "全身固定", "SHOT_UPPER_FIXED": "上半身固定", "SHOT_UPPER_THREE_QUARTER": "上半身至大腿固定", "SHOT_UPPER_PUSH_IN": "上半身缓慢推近"},
    "camera_motion": {"fixed": "固定", "push_in": "缓慢推近"},
    "action_type": {"basic_stand": "基础站立", "adjust_hem": "整理衣摆", "side_turn": "轻侧身", "touch_collar": "触碰领口袖口", "hand_in_pocket": "扶腰插兜", "half_step_forward": "半步前移", "upper_detail_combo": "上装复合微动作", "outerwear_detail_combo": "外套复合微动作", "silhouette_combo": "轮廓复合微动作", "custom": "自定义动作"},
    "risk_level": {"low": "低", "medium": "中", "high": "高"},
    "language": {"th": "泰语", "vi": "越南语", "ms": "马来语", "es": "西班牙语", "en": "英语", "zh": "中文"},
    "room_type": {"indoor_tryon_room": "室内试穿空间", "bedroom": "卧室", "bedroom_corner": "卧室角落", "window_side_room": "窗边房间", "modern_cafe": "现代咖啡店", "living_room": "客厅", "other": "其他"},
    "wall_color": {"warm_white": "暖白色", "white": "纯白色", "light_beige": "浅米色"},
    "shot_type": {"full_body": "全身", "three_quarter": "大半身", "upper_body": "大半身", "half_body": "半身"},
    "camera_height": {"waist_level": "腰部高度", "chest_level": "胸口高度", "eye_level": "眼平高度"},
    "camera_angle": {"front_flat": "正面平视", "slight_high": "轻微俯拍", "slight_low": "轻微仰拍"},
    "movement_boundary": {"very_small": "极小范围", "small": "小范围"},
    "movement_level": {"minimal": "极小", "light": "小", "medium": "中"},
    "lighting_style": {"soft_bright_natural": "柔和自然光", "high_key_soft_natural": "自然光加暖白补光", "oblique_soft_with_fill": "45度斜侧柔光加正面补光"},
    "lighting_level": {"bright_not_overexposed": "明亮"},
    "lighting_tone": {"warm_neutral": "略偏暖", "neutral_warm_no_yellow": "浅暖白不偏黄"},
    "body_rotation": {"none_to_very_small": "0度", "none": "0度", "very_small": "15度", "15_to_30_degrees": "30度", "20_to_35_degrees": "30度"},
    "duration_suggestion": {"3": "3秒", "4": "4秒", "5": "5秒", "6": "6秒"},
    "accessory_level": {"none": "无配饰", "minimal": "轻量配饰", "normal": "正常配饰"},
    "footwear_visibility": {"not_required": "不要求入镜", "optional": "可以入镜", "required": "必须入镜"},
    "bottom_type": {"wide_leg_pants": "高腰阔腿裤", "straight_jeans": "直筒牛仔裤", "white_shorts": "白色短裤", "casual_shorts": "休闲短裤", "midi_skirt": "半裙", "matching_set_bottom": "同色套装下装"},
    "outfit_image_status": {"pending": "待生成", "generating": "生成中", "pending_review": "待确认", "confirmed": "已确认", "regenerate": "重新生成", "failed": "生成失败"},
    "plan_status": {"active": "启用", "superseded": "已替代", "disabled": "不生成", "failed": "失败"},
}

VALUE_TO_BACKEND: dict[str, dict[str, str]] = {
    key: {display: backend for backend, display in values.items()} for key, values in VALUE_TO_FEISHU.items()
}

SHOT_PROFILE_TO_FEISHU = {
    "SHOT_FULL_FIXED": "全身固定",
    "SHOT_UPPER_FIXED": "上半身固定",
    "SHOT_UPPER_THREE_QUARTER": "上半身至大腿固定",
    "SHOT_UPPER_PUSH_IN": "上半身缓慢推近",
}
SHOT_PROFILE_TO_BACKEND = {value: key for key, value in SHOT_PROFILE_TO_FEISHU.items()}


def _ordered_sequence(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    parsed = json_loads(value, None)
    if isinstance(parsed, list):
        return [str(item).strip() for item in parsed if str(item).strip()]
    text = str(value or "")
    for separator in ("，", "；", ";", "\n"):
        text = text.replace(separator, ",")
    return [item.strip() for item in text.split(",") if item.strip()]


def _records(client: Any) -> list[dict[str, Any]]:
    result = []
    for item in client.list_records(page_size=500):
        result.append({"record_id": item.record_id, "fields": dict(item.fields or {})})
    return result


def _field_attr(item: Any, name: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(name, default)
    return getattr(item, name, default)


def _cache_template_reference_images(
    db: LightTryonDB,
    client: Any,
    role: str,
    business_id: str,
    attachments: Any,
) -> list[str]:
    values = attachments if isinstance(attachments, list) else []
    target_dir = db.path.parent / "template_images" / role / business_id
    result: list[str] = []
    for index, attachment in enumerate(values, start=1):
        if not isinstance(attachment, dict):
            text = str(attachment or "").strip()
            if text:
                result.append(text)
            continue
        token = str(attachment.get("file_token") or "").strip()
        if token:
            target_dir.mkdir(parents=True, exist_ok=True)
            raw_name = str(attachment.get("name") or f"reference_{index}.jpg")
            suffix = Path(raw_name).suffix or ".jpg"
            path = target_dir / f"{index:02d}_{stable_hash(token, length=16)}{suffix.lower()}"
            if not path.exists() or path.stat().st_size == 0:
                content, _, _, _ = client.download_attachment_bytes(attachment)
                path.write_bytes(content)
            result.append(str(path.resolve()))
            continue
        source = str(attachment.get("url") or attachment.get("tmp_url") or attachment.get("link") or "").strip()
        if source:
            result.append(source)
    return result


def _stable_template_source_payload(payload: dict[str, Any]) -> dict[str, Any]:
    stable = payload.copy()
    for backend in CLEARABLE_TEMPLATE_BACKENDS | CLEARABLE_SCENE_BACKENDS:
        if stable.get(backend) in (None, "", [], {}):
            stable.pop(backend, None)
    for reference_backend in ("reference_images", "brand_logo_images"):
        references = payload.get(reference_backend)
        if isinstance(references, list):
            stable[reference_backend] = [
                {key: item.get(key) for key in ("file_token", "name", "size", "type") if item.get(key) is not None}
                if isinstance(item, dict) else item
                for item in references
            ]
    return stable


def _iso_to_ms(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def _options(spec: dict[str, Any]) -> set[str]:
    return {str(item.get("name")) for item in (spec.get("property") or {}).get("options", []) if item.get("name")}


def _to_feishu_value(backend: str, value: Any, spec: dict[str, Any]) -> Any:
    if value in (None, "", [], {}):
        return None
    field_type = int(spec["type"])
    if field_type in READ_ONLY_TYPES:
        return None
    if field_type == 5:
        return _iso_to_ms(value)
    if field_type == 15:
        if isinstance(value, dict):
            return value
        text = str(value).strip()
        return {"link": text, "text": text} if text else None
    if field_type == 17:
        attachments = value if isinstance(value, list) else []
        kept = [{"file_token": str(item.get("file_token"))} for item in attachments if isinstance(item, dict) and item.get("file_token")]
        return kept or None
    if backend == "status":
        return ENUM_MAPS_TO_FEISHU["status"].get(str(value), str(value))
    if backend == "market":
        return MARKET_TO_FEISHU.get(str(value).upper(), str(value))
    if backend == "generation_channel":
        return GENERATION_CHANNEL_TO_FEISHU.get(str(value), str(value))
    if backend == "duration_seconds" and int(spec["type"]) == 3:
        text = str(value).strip().removesuffix("秒")
        return f"{text}秒" if text in {"8", "10"} else None
    if backend == "run_manager_sync_status":
        return RUN_MANAGER_STATUS_TO_FEISHU.get(str(value), str(value))
    if backend == "markets":
        return [MARKET_TO_FEISHU.get(str(item).upper(), str(item)) for item in normalized_list(value)]
    if backend == "applicable_shot_profiles":
        return [SHOT_PROFILE_TO_FEISHU.get(str(item), str(item)) for item in normalized_list(value)]
    if backend in {"single_sequence", "shot_1", "shot_2", "shot_3", "shot_4", "shot_5"}:
        item = normalized_list(value)[0] if normalized_list(value) else str(value or "")
        return SHOT_PROFILE_TO_FEISHU.get(item, item) or None
    if backend == "fallback_cycle":
        return ", ".join(SHOT_PROFILE_TO_FEISHU.get(str(item), str(item)) for item in _ordered_sequence(value)) or None
    if backend in VALUE_TO_FEISHU:
        value = VALUE_TO_FEISHU[backend].get(str(value), str(value))
    if backend in {"applicable_categories", "applicable_category", "applicable_product_type"}:
        reverse = {value: key for key, value in CATEGORY_TO_BACKEND.items()}
        value = [reverse.get(str(item), str(item)) for item in normalized_list(value) if item != "*"]
    if field_type == 4:
        values = normalized_list(value)
        allowed = _options(spec)
        return [item for item in values if not allowed or item in allowed] or None
    if field_type == 3:
        text = str(value).strip()
        allowed = _options(spec)
        return text if text and (not allowed or text in allowed) else None
    if field_type == 2:
        try:
            return float(value) if "." in str(value) else int(value)
        except (TypeError, ValueError):
            return None
    if isinstance(value, (list, dict)):
        return json_dumps(value)
    return str(value)


def _from_feishu_value(backend: str, value: Any, spec: dict[str, Any]) -> Any:
    if value in (None, "", [], {}):
        return None
    field_type = int(spec["type"])
    if field_type == 15 and isinstance(value, dict):
        return str(value.get("link") or value.get("text") or "")
    if field_type == 17:
        return value if isinstance(value, list) else []
    if backend in ENUM_MAPS_TO_BACKEND:
        return ENUM_MAPS_TO_BACKEND[backend].get(str(value), str(value))
    if backend == "markets":
        mapping = ENUM_MAPS_TO_BACKEND["market"]
        return [mapping.get(str(item), str(item)) for item in normalized_list(value)]
    if backend == "applicable_shot_profiles":
        return [SHOT_PROFILE_TO_BACKEND.get(str(item), str(item)) for item in normalized_list(value)]
    if backend in {"single_sequence", "shot_1", "shot_2", "shot_3", "shot_4", "shot_5"}:
        return SHOT_PROFILE_TO_BACKEND.get(str(value), str(value))
    if backend == "fallback_cycle":
        return [SHOT_PROFILE_TO_BACKEND.get(item, item) for item in _ordered_sequence(value)]
    if backend in VALUE_TO_BACKEND:
        return VALUE_TO_BACKEND[backend].get(str(value), str(value))
    if backend in {"applicable_categories", "applicable_category", "applicable_product_type"}:
        return [CATEGORY_TO_BACKEND.get(str(item), str(item)) for item in normalized_list(value)] or ["*"]
    if backend in LIST_BACKENDS:
        if isinstance(value, list):
            return value
        return normalized_list(value)
    if field_type == 2:
        return value
    return value


def _business_payload(role: str, fields: dict[str, Any]) -> dict[str, Any]:
    mapping = TABLE_MAPPINGS[role]
    specs = {spec["name"]: spec for spec in mapping.fields}
    payload: dict[str, Any] = {}
    for name, backend in mapping.backend_by_field.items():
        if backend in SYSTEM_FIELDS or name not in fields:
            continue
        if role == "styling" and backend in CLEARABLE_TEMPLATE_BACKENDS:
            payload[backend] = str(fields.get(name) or "").strip()
            continue
        if role == "scene" and backend in CLEARABLE_SCENE_BACKENDS:
            raw = fields.get(name)
            payload[backend] = normalized_list(raw) if backend in {"background_type_pool", "edge_decor_pool"} else str(raw or "").strip()
            continue
        converted = _from_feishu_value(backend, fields.get(name), specs[name])
        # 空白可选值不参与 upsert，避免运营表初始化时清空本地模板底座。
        if converted not in (None, "", [], {}):
            payload[backend] = converted
    # 兼容早期运营表中的字段名；新表仍以“场景参考图”为准。
    if role == "scene" and not payload.get("reference_images"):
        legacy_references = fields.get("标准环境参考图")
        if isinstance(legacy_references, list) and legacy_references:
            payload["reference_images"] = legacy_references
    if role == "shot_plan":
        single_profile = payload.get("single_sequence")
        payload["single_sequence"] = [single_profile] if single_profile else []
        payload["five_sequence"] = [
            str(payload.pop(f"shot_{index}", "")).strip() for index in range(1, 6)
            if str(payload.get(f"shot_{index}") or "").strip()
        ]
    return payload


def _template_to_fields(role: str, row: dict[str, Any], *, include_sync: bool = True) -> dict[str, Any]:
    mapping = TABLE_MAPPINGS[role]
    result: dict[str, Any] = {}
    for spec in mapping.fields:
        backend = spec["backend"]
        if backend in {"created_at", "feishu_updated_at"}:
            continue
        value = row.get(backend)
        if role == "shot_plan" and backend.startswith("shot_") and backend[5:].isdigit():
            sequence = _ordered_sequence(row.get("five_sequence"))
            index = int(backend[5:]) - 1
            value = sequence[index] if index < len(sequence) else ""
        if include_sync and backend == "sync_status":
            value = "已同步"
        if include_sync and backend == "last_synced_at":
            value = now_iso()
        if include_sync and backend == "sync_error":
            value = ""
        converted = _to_feishu_value(backend, value, spec)
        if converted is not None:
            result[spec["name"]] = converted
    return result


def inspect_tables(clients: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for role, mapping in TABLE_MAPPINGS.items():
        if role not in clients:
            continue
        fields = clients[role].list_fields()
        records = _records(clients[role])
        result[role] = {
            "expected_title": mapping.title,
            "field_count": len(fields),
            "fields": [_field_attr(item, "field_name") for item in fields],
            "record_count": len(records),
            "business_ids": [row["fields"].get(mapping.primary_field) for row in records if row["fields"].get(mapping.primary_field)],
        }
    return result


def ensure_schema(clients: dict[str, Any], *, dry_run: bool = False) -> dict[str, Any]:
    report: dict[str, Any] = {}
    for role, mapping in TABLE_MAPPINGS.items():
        client = clients.get(role)
        if not client:
            continue
        fields = client.list_fields()
        existing = {_field_attr(item, "field_name"): item for item in fields}
        actions: list[dict[str, Any]] = [{"operation": "rename_table", "to": mapping.title}]
        if role == "review" and "视频时长" not in existing and "目标时长" in existing:
            duration_spec = next(spec for spec in mapping.fields if spec["name"] == "视频时长")
            legacy = existing.pop("目标时长")
            actions.append({"operation": "rename_field", "from": "目标时长", "to": "视频时长"})
            if not dry_run:
                client.update_field(_field_attr(legacy, "field_id"), duration_spec)
            existing["视频时长"] = legacy
        if role == "review" and "飞书记录创建时间" not in existing and "创建时间" in existing:
            created_spec = next(spec for spec in mapping.fields if spec["name"] == "飞书记录创建时间")
            legacy = existing.pop("创建时间")
            actions.append({"operation": "rename_field", "from": "创建时间", "to": "飞书记录创建时间"})
            if not dry_run:
                client.update_field(_field_attr(legacy, "field_id"), created_spec)
            existing["飞书记录创建时间"] = legacy
        primary = existing.get(mapping.primary_field)
        if primary is None and len(fields) == 1 and _field_attr(fields[0], "field_name") == "文本":
            primary_spec = next(spec for spec in mapping.fields if spec["name"] == mapping.primary_field)
            actions.append({"operation": "rename_primary", "from": "文本", "to": mapping.primary_field})
            if not dry_run:
                client.update_field(_field_attr(fields[0], "field_id"), primary_spec)
            existing[mapping.primary_field] = fields[0]
        elif primary is None:
            actions.append({"operation": "create_field", "field": mapping.primary_field})
            if not dry_run:
                spec = next(spec for spec in mapping.fields if spec["name"] == mapping.primary_field)
                client.create_field(spec["name"], spec["type"], spec["ui_type"], spec.get("property"))
        for spec in mapping.fields:
            if spec["name"] == mapping.primary_field:
                continue
            if spec["name"] in existing:
                current = existing[spec["name"]]
                wanted_options = list((spec.get("property") or {}).get("options") or [])
                current_options = list((_field_attr(current, "property", {}) or {}).get("options") or [])
                current_names = {str(item.get("name") or "") for item in current_options}
                missing_options = [item for item in wanted_options if str(item.get("name") or "") not in current_names]
                if missing_options and int(spec.get("type") or 0) in {3, 4}:
                    merged_spec = {**spec, "property": {"options": [*current_options, *missing_options]}}
                    actions.append({
                        "operation": "add_field_options", "field": spec["name"],
                        "options": [item["name"] for item in missing_options],
                    })
                    if not dry_run:
                        client.update_field(_field_attr(current, "field_id"), merged_spec)
                continue
            actions.append({"operation": "create_field", "field": spec["name"], "type": spec["type"]})
            if not dry_run:
                client.create_field(spec["name"], spec["type"], spec["ui_type"], spec.get("property"))
        views = client.list_views()
        view_names = [str(item.get("view_name") or item.get("name") or "") for item in views]
        if views and mapping.view_names[0] not in view_names:
            view_id = str(views[0].get("view_id") or "")
            actions.append({"operation": "rename_view", "to": mapping.view_names[0]})
            if not dry_run and view_id:
                client.rename_view(view_id, mapping.view_names[0])
            view_names[0] = mapping.view_names[0]
        for name in mapping.view_names:
            if name not in view_names:
                actions.append({"operation": "create_view", "view": name})
                if not dry_run:
                    client.create_view(name)
        if not dry_run:
            client.rename_table(mapping.title)
        report[role] = {"title": mapping.title, "actions": actions, "expected_field_count": len(mapping.fields)}
    return {"dry_run": dry_run, "tables": report}


def initialize_template_records(
    db: LightTryonDB,
    clients: dict[str, Any],
    *,
    roles: Iterable[str] | None = None,
    business_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    selected_roles = tuple(roles or (role for role in TEMPLATE_ROLES if role in clients))
    unknown_roles = sorted(set(selected_roles) - set(TEMPLATE_ROLES))
    if unknown_roles:
        raise ValueError(f"不支持的模板类型: {', '.join(unknown_roles)}")
    selected_ids = {str(item).strip() for item in (business_ids or []) if str(item).strip()}
    for role in selected_roles:
        client = clients[role]
        mapping = TABLE_MAPPINGS[role]
        local_rows = [
            row for row in db.list_templates(role)
            if not selected_ids or str(row.get(TABLE_MAPPINGS[role].primary_backend) or "") in selected_ids
        ]
        remote = _records(client)
        by_id: dict[str, list[dict[str, Any]]] = {}
        blanks: list[str] = []
        for record in remote:
            business_id = str(record["fields"].get(mapping.primary_field) or "").strip()
            (by_id.setdefault(business_id, []).append(record) if business_id else blanks.append(record["record_id"]))
        duplicates = [key for key, rows in by_id.items() if len(rows) > 1]
        if duplicates:
            raise ValueError(f"{role} 存在重复业务ID: {', '.join(duplicates)}")
        created = updated = 0
        for row in local_rows:
            business_id = str(row[mapping.primary_backend])
            fields = _template_to_fields(role, row)
            if business_id in by_id:
                client.update_record_fields(by_id[business_id][0]["record_id"], fields)
                updated += 1
            elif blanks:
                client.update_record_fields(blanks.pop(0), fields)
                updated += 1
            else:
                client.batch_create_records([{"fields": fields}])
                created += 1
        result[role] = {"local": len(local_rows), "created": created, "updated": updated, "unused_blank_records": len(blanks)}
    return result


def _batch_id(kind: str) -> str:
    return f"FS_{kind}_{uuid4().hex[:14]}"


def _changed_after(fields: dict[str, Any], value: str | None) -> bool:
    if not value:
        return True
    raw = fields.get("最后修改时间")
    if raw in (None, ""):
        return False
    try:
        cutoff = datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000
        return float(raw) >= cutoff
    except (TypeError, ValueError):
        raise ValueError("--changed-after 必须是 ISO 8601 时间，例如 2026-07-13T09:00:00+08:00")


def pull_templates(
    db: LightTryonDB,
    clients: dict[str, Any],
    *,
    roles: Iterable[str] | None = None,
    business_ids: Iterable[str] | None = None,
    changed_after: str | None = None,
) -> dict[str, Any]:
    batch_id = _batch_id("template_pull")
    db.start_sync_run(batch_id, "template_pull")
    counts = {"created": 0, "updated": 0, "skipped": 0, "failed": 0}
    selected_roles = tuple(roles or (role for role in TEMPLATE_ROLES if role in clients))
    unknown_roles = sorted(set(selected_roles) - set(TEMPLATE_ROLES))
    if unknown_roles:
        raise ValueError(f"不支持的模板类型: {', '.join(unknown_roles)}")
    selected_ids = {str(item).strip() for item in (business_ids or []) if str(item).strip()}
    filtered = 0
    errors: list[str] = []
    try:
        for role in selected_roles:
            mapping = TABLE_MAPPINGS[role]
            client = clients[role]
            seen: set[str] = set()
            for record in _records(client):
                payload = _business_payload(role, record["fields"])
                business_id = str(payload.get(mapping.primary_backend) or "").strip()
                if (selected_ids and business_id not in selected_ids) or not _changed_after(record["fields"], changed_after):
                    filtered += 1
                    continue
                if not business_id:
                    counts["skipped"] += 1
                    continue
                if business_id in seen:
                    raise ValueError(f"{role} 存在重复业务ID: {business_id}")
                seen.add(business_id)
                required = [mapping.primary_backend, mapping.primary_backend.replace("_id", "_name"), "status"]
                if role == "subtitle":
                    if not any(payload.get(name) for name in ("opening_text", "middle_text", "ending_text")):
                        required.append("opening_text")
                elif role == "shot_plan":
                    required.extend(["single_sequence", "five_sequence"])
                else:
                    required.append("prompt_core")
                missing = [name for name in required if not payload.get(name)]
                existing = db.get_template(role, business_id)
                source_hash = stable_hash(_stable_template_source_payload(payload), length=24)
                if missing:
                    message = f"缺少必填字段: {', '.join(missing)}"
                    client.update_record_fields(record["record_id"], {"同步状态": "同步失败", "同步错误信息": message})
                    db.log_sync_item(batch_id, role, record["record_id"], business_id, "validate", "failed", message)
                    counts["failed"] += 1
                    errors.append(f"{role}/{business_id}: {message}")
                    continue
                if existing and existing.get("source_hash") == source_hash:
                    client.update_record_fields(record["record_id"], {"同步状态": "已同步", "最后同步时间": _iso_to_ms(now_iso()), "同步错误信息": ""})
                    db.log_sync_item(batch_id, role, record["record_id"], business_id, "skip", "success")
                    counts["skipped"] += 1
                    continue
                if role == "subtitle":
                    payload.setdefault("market", (payload.get("markets") or [existing.get("market") if existing else "GLOBAL"])[0])
                    payload.setdefault("subtitle_style", payload.get("subtitle_type") or (existing or {}).get("subtitle_style") or "natural")
                # SQLite 的 ON CONFLICT 仍会先检查 INSERT 的 NOT NULL；更新既有模板时先合并完整快照。
                write_payload = {**(existing or {}), **payload}
                if role in {"scene", "persona"} and payload.get("reference_images"):
                    write_payload["reference_images"] = _cache_template_reference_images(
                        db, client, role, business_id, payload.get("reference_images"),
                    )
                if role == "persona" and payload.get("brand_logo_images"):
                    write_payload["brand_logo_images"] = _cache_template_reference_images(
                        db, client, "brand_logo", business_id, payload.get("brand_logo_images"),
                    )
                write_payload.update({
                    "feishu_record_id": record["record_id"], "sync_status": "synced", "last_synced_at": now_iso(),
                    "sync_error": "", "source_hash": source_hash, "source_payload": payload.copy(),
                })
                db.upsert_template(role, write_payload)
                operation = "update" if existing else "create"
                counts["updated" if existing else "created"] += 1
                client.update_record_fields(record["record_id"], {"同步状态": "已同步", "最后同步时间": _iso_to_ms(now_iso()), "同步错误信息": ""})
                db.log_sync_item(batch_id, role, record["record_id"], business_id, operation, "success")
        status = "partial" if counts["failed"] else "success"
        db.finish_sync_run(batch_id, counts, status=status, error="; ".join(errors))
        return {"batch_id": batch_id, "status": status, **counts, "filtered": filtered, "errors": errors}
    except Exception as exc:
        db.finish_sync_run(batch_id, counts, status="failed", error=str(exc))
        raise


def _score(qc: dict[str, Any], *names: str) -> float | None:
    for name in names:
        if qc.get(name) is not None:
            try:
                value = float(qc[name])
                return round(value * 10 if 0 <= value <= 10 else value, 2)
            except (TypeError, ValueError):
                pass
    return None


def _review_system_fields(db: LightTryonDB, job: dict[str, Any]) -> dict[str, Any]:
    product = db.get_product(job["product_id"]) or {}
    qc = job.get("qc_result") or {}
    prompt_payload = job.get("prompt_payload") or {}
    display_prompt = prompt_payload.get("display_prompt") or prompt_payload.get("positive_prompt") or ""
    values = {
        "job_id": job["job_id"], "job_created_at": job.get("created_at") or "", "visual_plan_id": job.get("visual_plan_id") or "",
        "product_id": product.get("source_product_code") or job["product_id"], "product_name": product.get("product_name") or "",
        "product_images": product.get("product_images") or [], "outfit_image_url": job.get("outfit_image_url") or "",
        "outfit_image_path": job.get("outfit_image_path") or "", "outfit_image_version": job.get("outfit_image_version") or "",
        "account_id": job.get("account_id") or product.get("account_id") or "",
        "market": job.get("market"), "variant_no": job.get("variant_no"), "persona_id": job.get("persona_id"),
        "scene_id": job.get("scene_id"), "shot_plan_id": job.get("shot_plan_id"), "shot_profile_id": job.get("shot_profile_id"), "action_id": job.get("action_id"), "styling_id": job.get("styling_id"),
        "subtitle_id": job.get("subtitle_id"), "duration_seconds": job.get("duration_seconds"), "prompt_version": job.get("prompt_version"),
        "template_versions": job.get("template_versions") or {}, "prompt_payload": display_prompt,
        "generation_status": GENERATION_TO_FEISHU.get(str(job.get("generation_status")), str(job.get("generation_status") or "")),
        "generation_error": job.get("last_error") or "", "output_video": job.get("output_video_url") or job.get("output_video_path") or "",
        "run_manager_record_id": job.get("run_manager_record_id") or "",
        "run_manager_sync_status": job.get("run_manager_sync_status") or "not_submitted",
        "run_manager_sync_error": job.get("run_manager_sync_error") or "",
        "run_manager_trace_id": job.get("run_manager_trace_id") or "",
        "output_cover": job.get("output_cover_url") or job.get("output_cover_path") or "",
        "machine_qc_status": QC_TO_FEISHU.get(str(job.get("qc_status")), str(job.get("qc_status") or "")),
        "scene_score": _score(qc, "scene_score", "scene_consistency_score"),
        "persona_score": _score(qc, "persona_score", "persona_consistency_score"),
        "clothing_score": _score(qc, "clothing_score", "product_fidelity_score"),
        "motion_score": _score(qc, "motion_score", "motion_naturalness_score"),
        "realism_score": _score(qc, "realism_score"), "overall_score": _score(qc, "overall_score", "score"),
        "abnormal_types": qc.get("abnormal_types") or qc.get("issues") or [], "machine_qc_notes": qc.get("notes") or qc.get("reason") or "",
        "parent_job_id": job.get("parent_job_id") or "", "regeneration_job_id": job.get("regeneration_job_id") or "",
        "review_version": job.get("review_version") or 0, "review_processed_at": job.get("review_processed_at") or "",
        "published_at": job.get("published_at") or "", "views": job.get("views") or 0, "product_clicks": job.get("product_clicks") or 0,
        "gmv": job.get("gmv") or 0, "metrics_updated_at": job.get("metrics_updated_at") or "",
        "sync_status": "已同步", "last_synced_at": now_iso(), "sync_error": "",
    }
    mapping = TABLE_MAPPINGS["review"]
    result: dict[str, Any] = {}
    for spec in mapping.fields:
        backend = spec["backend"]
        if backend in MANUAL_REVIEW_FIELDS or backend in {"created_at", "feishu_updated_at"}:
            continue
        converted = _to_feishu_value(backend, values.get(backend), spec)
        if converted is not None:
            result[spec["name"]] = converted
    return result


def push_reviews(
    db: LightTryonDB,
    client: Any,
    *,
    limit: int | None = None,
    job_ids: Iterable[str] | None = None,
    changed_after: str | None = None,
) -> dict[str, Any]:
    records = _records(client)
    by_job: dict[str, list[dict[str, Any]]] = {}
    blanks: list[str] = []
    for record in records:
        job_id = str(record["fields"].get("视频任务ID") or "").strip()
        (by_job.setdefault(job_id, []).append(record) if job_id else blanks.append(record["record_id"]))
    duplicates = [job_id for job_id, rows in by_job.items() if len(rows) > 1]
    jobs = db.list_jobs(limit=limit)
    selected_ids = {str(item).strip() for item in (job_ids or []) if str(item).strip()}
    if selected_ids:
        jobs = [job for job in jobs if job["job_id"] in selected_ids]
    if changed_after:
        cutoff = datetime.fromisoformat(changed_after.replace("Z", "+00:00"))
        jobs = [job for job in jobs if datetime.fromisoformat(str(job["updated_at"]).replace("Z", "+00:00")) >= cutoff]
    created = updated = failed = 0
    for job in jobs:
        job_id = job["job_id"]
        fields = _review_system_fields(db, job)
        operator_defaults = {
            "生成渠道": GENERATION_CHANNEL_TO_FEISHU.get(str(job.get("generation_channel") or "no_generate"), "不生成"),
            "生成模型": str(job.get("generation_model") or "Seedance 2.0"),
            "视频时长": int(job.get("duration_seconds") or 8),
            "重新提交生成": False,
        }
        try:
            if job_id in by_job:
                candidates = by_job[job_id]
                bound_record_id = str(job.get("feishu_review_record_id") or "")
                existing_record = next((item for item in candidates if item["record_id"] == bound_record_id), None)
                if existing_record is None and len(candidates) == 1:
                    existing_record = candidates[0]
                if existing_record is None:
                    raise ValueError(f"复核台存在 {len(candidates)} 条重复视频任务ID，且本地没有唯一绑定记录")
                record_id = existing_record["record_id"]
                missing_operator_defaults = {
                    name: value for name, value in operator_defaults.items()
                    if existing_record["fields"].get(name) in (None, "", [])
                }
                client.update_record_fields(record_id, {**fields, **missing_operator_defaults})
                updated += 1
            elif blanks:
                record_id = blanks.pop(0)
                initial = {**fields, **operator_defaults, "人工复核状态": "待复核", "是否需要补生成": "否", "发布状态": "未排期"}
                client.update_record_fields(record_id, initial)
                created += 1
            else:
                initial = {**fields, **operator_defaults, "人工复核状态": "待复核", "是否需要补生成": "否", "发布状态": "未排期"}
                ids = client.batch_create_records([{"fields": initial}])
                record_id = ids[0] if ids else ""
                created += 1
            db.update_review_sync(job_id, record_id, "synced")
        except Exception as exc:
            db.update_review_sync(job_id, "", "failed", str(exc))
            failed += 1
    return {
        "jobs": len(jobs), "created": created, "updated": updated, "failed": failed,
        "duplicate_job_ids": len(duplicates), "unused_blank_records": len(blanks),
    }


def cleanup_review_duplicates(
    db: LightTryonDB,
    client: Any,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in _records(client):
        job_id = str(record["fields"].get("视频任务ID") or "").strip()
        if job_id:
            grouped.setdefault(job_id, []).append(record)
    duplicate_groups = {job_id: rows for job_id, rows in grouped.items() if len(rows) > 1}
    delete_ids: list[str] = []
    items: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for job_id, rows in sorted(duplicate_groups.items()):
        job = db.get_job(job_id)
        if not job:
            blocked.append({"job_id": job_id, "reason": "本地不存在该任务", "record_ids": [row["record_id"] for row in rows]})
            continue
        bound_record_id = str(job.get("feishu_review_record_id") or "")
        keep = next((row for row in rows if row["record_id"] == bound_record_id), None)
        if keep is None:
            blocked.append({
                "job_id": job_id,
                "reason": "本地绑定记录不在重复组中，拒绝猜测删除",
                "bound_record_id": bound_record_id,
                "record_ids": [row["record_id"] for row in rows],
            })
            continue
        removed = [row["record_id"] for row in rows if row["record_id"] != keep["record_id"]]
        delete_ids.extend(removed)
        items.append({"job_id": job_id, "keep_record_id": keep["record_id"], "delete_record_ids": removed})
    deleted = 0
    if delete_ids and not dry_run:
        deleted = int(client.batch_delete_records(delete_ids))
    return {
        "dry_run": dry_run,
        "duplicate_groups": len(duplicate_groups),
        "planned_delete": len(delete_ids),
        "deleted": deleted,
        "blocked_groups": len(blocked),
        "items": items,
        "blocked": blocked,
    }


def _visual_plan_to_fields(row: dict[str, Any]) -> dict[str, Any]:
    mapping = TABLE_MAPPINGS["visual_plan"]
    result: dict[str, Any] = {}
    for spec in mapping.fields:
        backend = spec["backend"]
        if backend in {"created_at", "feishu_updated_at"}:
            continue
        value = row.get(backend)
        if backend == "sync_status":
            value = "已同步"
        elif backend == "last_synced_at":
            value = now_iso()
        elif backend == "sync_error":
            value = ""
        converted = _to_feishu_value(backend, value, spec)
        if converted is not None:
            result[spec["name"]] = converted
    return result


def push_visual_plans(db: LightTryonDB, client: Any) -> dict[str, Any]:
    records = _records(client)
    by_id: dict[str, list[dict[str, Any]]] = {}
    blanks: list[str] = []
    for record in records:
        visual_plan_id = str(record["fields"].get("视觉方案ID") or "").strip()
        (by_id.setdefault(visual_plan_id, []).append(record) if visual_plan_id else blanks.append(record["record_id"]))
    duplicates = [key for key, rows in by_id.items() if len(rows) > 1]
    if duplicates:
        raise ValueError(f"产品视觉方案表存在重复视觉方案ID: {', '.join(duplicates)}")
    created = updated = failed = 0
    plans = db.list_visual_plans()
    for plan in plans:
        plan_id = plan["visual_plan_id"]
        fields = _visual_plan_to_fields(plan)
        try:
            if plan_id in by_id:
                record = by_id[plan_id][0]
                if str(record["fields"].get("穿搭图状态") or "") in {"已确认", "重新生成"}:
                    for name in ("穿搭图状态", "复核反馈", "产品穿搭图URL", "产品穿搭图", "产品穿搭图路径"):
                        fields.pop(name, None)
                client.update_record_fields(record["record_id"], fields)
                record_id = record["record_id"]
                updated += 1
            elif blanks:
                record_id = blanks.pop(0)
                client.update_record_fields(record_id, fields)
                created += 1
            else:
                ids = client.batch_create_records([{"fields": fields}])
                record_id = ids[0] if ids else ""
                created += 1
            db.upsert_visual_plan({**plan, "feishu_record_id": record_id, "sync_status": "synced", "last_synced_at": now_iso(), "sync_error": ""})
        except Exception as exc:
            db.upsert_visual_plan({**plan, "sync_status": "failed", "sync_error": str(exc)})
            failed += 1
    return {"visual_plans": len(plans), "created": created, "updated": updated, "failed": failed}


def pull_visual_plan_reviews(db: LightTryonDB, client: Any) -> dict[str, Any]:
    processed = confirmed = regenerate = created_jobs = failed = skipped = 0
    errors: list[str] = []
    for record in _records(client):
        fields = record["fields"]
        plan_id = str(fields.get("视觉方案ID") or "").strip()
        if not plan_id:
            skipped += 1
            continue
        plan = db.get_visual_plan(plan_id)
        if not plan:
            failed += 1
            errors.append(f"未知视觉方案: {plan_id}")
            continue
        display_status = str(fields.get("穿搭图状态") or "")
        status = VALUE_TO_BACKEND["outfit_image_status"].get(display_status, "")
        feedback = str(fields.get("复核反馈") or "")
        raw_url = fields.get("产品穿搭图URL")
        image_url = str(raw_url.get("link") or raw_url.get("text") or "") if isinstance(raw_url, dict) else str(raw_url or "")
        image_path = str(fields.get("产品穿搭图路径") or "")
        image_attachments = fields.get("产品穿搭图") if isinstance(fields.get("产品穿搭图"), list) else []
        stable_attachments = [
            {key: item.get(key) for key in ("file_token", "name", "size", "type") if item.get(key) is not None}
            for item in image_attachments if isinstance(item, dict)
        ]
        fingerprint = stable_hash(status, feedback, image_url, image_path, stable_attachments, length=24)
        if not status or fingerprint == str(plan.get("review_source_hash") or ""):
            skipped += 1
            continue
        try:
            if image_attachments:
                cached = _cache_template_reference_images(db, client, "visual_plan", plan_id, image_attachments)
                if cached:
                    image_path = cached[0]
            if status == "confirmed":
                db.set_visual_plan_outfit(
                    plan_id, status="confirmed", image_path=image_path, image_url=image_url,
                    image_version=str(plan.get("outfit_image_version") or f"confirmed-{fingerprint[:10]}"),
                    image_attachments=image_attachments, feedback=feedback,
                )
                job_result = create_confirmed_video_jobs(db, plan_id)
                created_jobs += int(job_result["created"])
                confirmed += 1
            elif status == "regenerate":
                db.set_visual_plan_outfit(plan_id, status="regenerate", image_attachments=image_attachments, feedback=feedback)
                regenerate += 1
            else:
                db.set_visual_plan_outfit(plan_id, status=status, image_path=image_path, image_url=image_url, image_attachments=image_attachments, feedback=feedback)
            current = db.get_visual_plan(plan_id) or {}
            db.upsert_visual_plan({**current, "review_source_hash": fingerprint, "feishu_record_id": record["record_id"]})
            client.update_record_fields(record["record_id"], {
                "同步状态": "已同步", "最后同步时间": _iso_to_ms(now_iso()), "同步错误信息": "",
                "视频任务ID": "\n".join((db.get_visual_plan(plan_id) or {}).get("job_ids") or []),
            })
            processed += 1
        except Exception as exc:
            failed += 1
            message = str(exc)
            errors.append(f"{plan_id}: {message}")
            client.update_record_fields(record["record_id"], {"同步状态": "同步失败", "同步错误信息": message[:2000]})
    return {"processed": processed, "confirmed": confirmed, "regenerate": regenerate, "created_jobs": created_jobs, "skipped": skipped, "failed": failed, "errors": errors}


def pull_manual_reviews(
    db: LightTryonDB,
    client: Any,
    *,
    job_ids: Iterable[str] | None = None,
    changed_after: str | None = None,
) -> dict[str, Any]:
    processed = skipped = regenerated = failed = 0
    errors: list[str] = []
    selected_ids = {str(item).strip() for item in (job_ids or []) if str(item).strip()}
    for record in _records(client):
        fields = record["fields"]
        job_id = str(fields.get("视频任务ID") or "").strip()
        if (selected_ids and job_id not in selected_ids) or not _changed_after(fields, changed_after):
            skipped += 1
            continue
        if not job_id:
            skipped += 1
            continue
        job = db.get_job(job_id)
        if not job:
            failed += 1
            errors.append(f"未知任务: {job_id}")
            continue
        manual = {
            "manual_review_status": MANUAL_TO_BACKEND.get(str(fields.get("人工复核状态") or "待复核"), "pending"),
            "manual_review_reason": str(fields.get("人工复核原因") or ""),
            "need_regeneration": "yes" if fields.get("是否需要补生成") == "是" else "no",
            "regeneration_strategy": REGEN_TO_BACKEND.get(str(fields.get("补生成策略") or ""), ""),
            "publish_status": PUBLISH_TO_BACKEND.get(str(fields.get("发布状态") or "未排期"), "unscheduled"),
            "operator_notes": str(fields.get("运营备注") or ""),
        }
        if (
            manual["manual_review_status"] == "pending"
            and manual["need_regeneration"] == "no"
            and manual["publish_status"] == "unscheduled"
            and not manual["manual_review_reason"]
            and not manual["operator_notes"]
        ):
            skipped += 1
            continue
        fingerprint = stable_hash(manual, length=24)
        event = db.get_review_event(job_id, fingerprint)
        if event:
            skipped += 1
            continue
        try:
            child_id = ""
            if manual["need_regeneration"] == "yes":
                if not manual["regeneration_strategy"]:
                    raise ValueError("勾选补生成后必须选择补生成策略")
                child = db.create_regeneration_job(job_id, fingerprint, manual["regeneration_strategy"])
                child_id = child["job_id"]
                payload = build_prompt(db.get_job_context(child_id))
                db.update_prompt(child_id, payload, PROMPT_BUILDER_VERSION)
                regenerated += 1
            version = int(job.get("review_version") or 0) + 1
            db.apply_manual_review(job_id, source_hash=fingerprint, review_version=version, **manual)
            db.record_review_event(
                job_id, fingerprint, feishu_record_id=record["record_id"],
                manual_review_status=manual["manual_review_status"], need_regeneration=manual["need_regeneration"],
                regeneration_strategy=manual["regeneration_strategy"], regeneration_job_id=child_id,
            )
            update = {"复核版本": version, "复核处理时间": _iso_to_ms(now_iso()), "数据同步状态": "已同步", "最后同步时间": _iso_to_ms(now_iso()), "同步错误信息": ""}
            if child_id:
                update["补生成任务ID"] = child_id
            client.update_record_fields(record["record_id"], update)
            processed += 1
        except Exception as exc:
            failed += 1
            message = str(exc)
            errors.append(f"{job_id}: {message}")
            client.update_record_fields(record["record_id"], {"数据同步状态": "同步失败", "同步错误信息": message[:2000]})
    return {"processed": processed, "skipped": skipped, "regenerated": regenerated, "failed": failed, "errors": errors}


def build_clients(endpoints: dict[str, Any]) -> dict[str, Any]:
    return {role: make_client(endpoint) for role, endpoint in endpoints.items()}


def sync_all(db: LightTryonDB, clients: dict[str, Any]) -> dict[str, Any]:
    result = {"templates": pull_templates(db, clients)}
    if "visual_plan" in clients:
        result["visual_plan_pull"] = pull_visual_plan_reviews(db, clients["visual_plan"])
        result["visual_plan_push"] = push_visual_plans(db, clients["visual_plan"])
    result["review_push"] = push_reviews(db, clients["review"])
    result["review_video_processing"] = process_review_videos(db, clients["review"])
    result["review_pull"] = pull_manual_reviews(db, clients["review"])
    return result
