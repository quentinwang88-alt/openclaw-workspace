from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable

from .database import LightTryonDB
from .feishu_mappings import SOURCE_SCRIPT_INPUT_FIELDS, SOURCE_SCRIPT_TRIGGER_FIELDS
from .models import ProductInput
from .visual_plans import create_confirmed_video_jobs, orchestrate_visual_plans
from .utils import normalized_list, now_iso, safe_slug, stable_hash


SOURCE_CONFIG_VERSION = "source-visual-plan-v3"
REQUEST_COUNTS = {
    "不生成": 0,
    "不跑": 0,
    "生成1个": 1,
    "生成5个": 5,
    "每方案1个": 1,
    "每方案5个": 5,
}
COUNTRY_TO_MARKET = {
    "泰国": "TH", "TH": "TH", "Thailand": "TH",
    "越南": "VN", "VN": "VN", "Vietnam": "VN",
    "马来西亚": "MY", "MY": "MY", "Malaysia": "MY",
    "墨西哥": "MX", "MX": "MX", "Mexico": "MX",
}
LANGUAGE_TO_CODE = {
    "泰语": "th", "Thai": "th", "th": "th",
    "越南语": "vi", "Vietnamese": "vi", "vi": "vi",
    "马来语": "ms", "马来西亚语": "ms", "Malay": "ms", "ms": "ms",
    "墨西哥西班牙语": "es", "Mexican Spanish": "es", "西班牙语": "es", "Spanish": "es", "es": "es",
    "英语": "en", "English": "en", "en": "en",
    "中文": "zh", "Chinese": "zh", "zh": "zh",
}
DEFAULT_LANGUAGE = {"TH": "th", "VN": "vi", "MY": "ms", "MX": "es"}
CATEGORY_TO_CODE = {
    "上装": "top", "T恤": "tshirt", "短袖": "tshirt", "针织": "knit_top", "针织衫": "knit_top",
    "背心": "tank_top", "吊带": "tank_top", "衬衫": "shirt", "外套": "outerwear", "裤装": "pants",
    "裙装": "skirt", "连衣裙": "dress", "套装": "set", "连体裤": "jumpsuit", "家居服": "homewear",
}
SCENE_PREFERENCE_TO_ID = {
    "": "", "自动选择": "", "现代简约卧室": "SCENE_A_001", "明亮现代咖啡店": "ENV_CAFE_001",
}
STYLING_PREFERENCE_TO_ID = {
    "": "", "自动选择": "", "白色高腰阔腿裤": "STYLE_001", "经典蓝色直筒牛仔裤": "STYLE_002",
    "白色高腰短裤": "STYLE_003", "纯色休闲短裤": "STYLE_004", "简洁半裙": "STYLE_005",
    "保持商品原套装": "STYLE_006",
}


def _field_attr(item: Any, name: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(name, default)
    return getattr(item, name, default)


def _source_value(fields: dict[str, Any], logical_name: str) -> Any:
    for name in SOURCE_SCRIPT_INPUT_FIELDS[logical_name]:
        value = fields.get(name)
        if value not in (None, "", [], {}):
            return value
    return None


def _text(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("text", "name", "link", "url"):
            if value.get(key):
                return str(value[key]).strip()
    if isinstance(value, list):
        return " / ".join(item for item in (_text(value_item) for value_item in value) if item)
    return str(value).strip()


def _request_count(value: Any) -> int | None:
    normalized = re.sub(r"\s+", "", _text(value))
    if not normalized:
        return None
    if normalized not in REQUEST_COUNTS:
        raise ValueError(f"非法轻量视频生成数量: {_text(value)}")
    return REQUEST_COUNTS[normalized]


def _now_ms() -> int:
    from datetime import datetime

    return int(datetime.now().timestamp() * 1000)


def ensure_source_schema(client: Any, *, db: LightTryonDB | None = None, dry_run: bool = False) -> dict[str, Any]:
    fields = client.list_fields()
    existing = {_field_attr(item, "field_name"): item for item in fields}
    specs = list(SOURCE_SCRIPT_TRIGGER_FIELDS)
    if db is not None:
        dynamic_kinds = (
            ("scene", "scene_name", {"enabled"}),
            ("styling", "styling_name", {"enabled"}),
            ("action", "action_name", {"enabled", "testing"}),
            ("shot_plan", "shot_plan_name", {"enabled", "testing"}),
        )
        for kind, name_key, statuses in dynamic_kinds:
            rows = [row for row in db.list_templates(kind) if row.get("status") in statuses]
            names = [str(row.get(name_key) or "").strip() for row in rows]
            if any(not name for name in names):
                raise ValueError(f"可选择的 {kind} 模板存在空名称")
            duplicates = sorted({name for name in names if names.count(name) > 1})
            if duplicates:
                raise ValueError(f"可选择的 {kind} 模板名称重复: {', '.join(duplicates)}")
        dynamic_options = {
            "轻量视频场景": ["自动选择", *[row["scene_name"] for row in db.list_templates("scene", "enabled")]],
            "轻量视频搭配": ["自动选择", *[row["styling_name"] for row in db.list_templates("styling", "enabled")]],
            "轻量视频动作": ["自动选择", *[row["action_name"] for row in db.list_templates("action") if row.get("status") in {"enabled", "testing"}]],
            "轻量视频镜头方案": ["自动选择", *[row["shot_plan_name"] for row in db.list_templates("shot_plan") if row.get("status") in {"enabled", "testing"}]],
        }
        specs = [
            {**spec, "property": {"options": [{"name": name, "color": index % 54} for index, name in enumerate(dynamic_options[spec["name"]])]}}
            if spec["name"] in dynamic_options else spec
            for spec in specs
        ]
    desired_request = specs[0]
    legacy_fields = [existing[name] for name in ("是否跑轻模型", "轻量视频生成数量") if existing.get(name)]
    current = existing.get("每方案视频数量")
    actions: list[dict[str, Any]] = []
    if len(legacy_fields) > 1 or (legacy_fields and current):
        raise ValueError("原表存在多个轻量视频数量字段，请先人工确认保留哪一个")
    if legacy_fields:
        legacy = legacy_fields[0]
        actions.append({"operation": "migrate_field", "field_id": _field_attr(legacy, "field_id"), "from": _field_attr(legacy, "field_name"), "to": "每方案视频数量"})
        if not dry_run:
            client.update_field(_field_attr(legacy, "field_id"), desired_request)
        existing["每方案视频数量"] = legacy
    elif not current:
        actions.append({"operation": "create_field", "field": desired_request["name"], "type": desired_request["type"]})
        if not dry_run:
            client.create_field(desired_request["name"], desired_request["type"], desired_request["ui_type"], desired_request.get("property"))
    else:
        current_options = list((_field_attr(current, "property", {}) or {}).get("options") or [])
        wanted_options = list((desired_request.get("property") or {}).get("options") or [])
        current_names = {str(item.get("name") or "") for item in current_options}
        missing = [item for item in wanted_options if str(item.get("name") or "") not in current_names]
        type_changed = int(_field_attr(current, "field_type", desired_request["type"]) or 0) != int(desired_request["type"])
        if type_changed or missing:
            actions.append({"operation": "update_request_field", "field": "每方案视频数量", "options": [item["name"] for item in missing]})
            if not dry_run:
                client.update_field(_field_attr(current, "field_id"), {**desired_request, "property": {"options": [*current_options, *missing]}})
    for spec in specs[1:]:
        current_field = existing.get(spec["name"])
        if current_field:
            wanted = list((spec.get("property") or {}).get("options") or [])
            current = list((_field_attr(current_field, "property", {}) or {}).get("options") or [])
            renamed: list[dict[str, str]] = []
            if db is not None and spec["name"] in {"轻量视频场景", "轻量视频搭配"}:
                kind = "scene" if spec["name"] == "轻量视频场景" else "styling"
                aliases = SCENE_PREFERENCE_TO_ID if kind == "scene" else STYLING_PREFERENCE_TO_ID
                id_key, name_key = f"{kind}_id", f"{kind}_name"
                canonical = {row[id_key]: row[name_key] for row in db.list_templates(kind, "enabled")}
                normalized_current: list[dict[str, Any]] = []
                seen_names: set[str] = set()
                for item in current:
                    old_name = str(item.get("name") or "")
                    new_name = canonical.get(aliases.get(old_name, ""), old_name)
                    if new_name in seen_names:
                        continue
                    seen_names.add(new_name)
                    normalized_current.append({**item, "name": new_name})
                    if new_name != old_name:
                        renamed.append({"from": old_name, "to": new_name})
                current = normalized_current
            current_names = {str(item.get("name") or "") for item in current}
            missing = [item for item in wanted if str(item.get("name") or "") not in current_names]
            type_changed = int(_field_attr(current_field, "field_type", spec.get("type") or 0) or 0) != int(spec.get("type") or 0)
            if type_changed:
                actions.append({"operation": "change_field_type", "field": spec["name"], "to": spec["ui_type"]})
            if (type_changed or renamed or missing) and int(spec.get("type") or 0) in {3, 4}:
                if renamed:
                    actions.append({"operation": "rename_field_options", "field": spec["name"], "options": renamed})
                if missing:
                    actions.append({"operation": "add_field_options", "field": spec["name"], "options": [item["name"] for item in missing]})
                if not dry_run:
                    client.update_field(_field_attr(current_field, "field_id"), {**spec, "property": {"options": [*current, *missing]}})
            continue
        actions.append({"operation": "create_field", "field": spec["name"], "type": spec["type"]})
        if not dry_run:
            client.create_field(spec["name"], spec["type"], spec["ui_type"], spec.get("property"))
    return {"dry_run": dry_run, "actions": actions, "expected_fields": [spec["name"] for spec in specs]}


def _resolve_template_preference(
    db: LightTryonDB, kind: str, raw_value: str, aliases: dict[str, str], *, allow_testing: bool = False
) -> str:
    value = str(raw_value or "").strip()
    if not value or value == "自动选择":
        return ""
    alias_id = aliases.get(value, value)
    rows = [row for row in db.list_templates(kind) if row.get("status") == "enabled" or (allow_testing and row.get("status") == "testing")]
    id_key = f"{kind}_id"
    name_key = f"{kind}_name"
    matches = [row for row in rows if alias_id == row[id_key] or value == row[name_key]]
    if not matches:
        label = {"scene": "场景", "styling": "搭配", "action": "动作", "shot_plan": "镜头方案"}.get(kind, kind)
        raise ValueError(f"不支持或未启用的轻量视频{label}: {value}")
    return str(matches[0][id_key])


def _selection_values(value: Any) -> list[str]:
    if isinstance(value, list):
        return [text for text in (_text(item) for item in value) if text]
    return normalized_list(_text(value))


def find_source_records(client: Any, product_ref: str) -> list[dict[str, Any]]:
    wanted = str(product_ref or "").strip()
    if not wanted:
        raise ValueError("product_ref 不能为空")
    matches: list[dict[str, Any]] = []
    reference_fields = tuple(dict.fromkeys((*SOURCE_SCRIPT_INPUT_FIELDS["product_code"], "产品ID")))
    for record in client.list_records(page_size=500):
        fields = dict(record.fields or {})
        refs = {name: _text(fields.get(name)) for name in reference_fields if _text(fields.get(name))}
        if wanted not in refs.values():
            continue
        images = _source_value(fields, "product_images")
        matches.append({
            "record_id": record.record_id,
            "matched_references": refs,
            "product_code": _text(_source_value(fields, "product_code")),
            "top_category": _text(_source_value(fields, "top_category")),
            "target_country": _text(_source_value(fields, "target_country")),
            "target_language": _text(_source_value(fields, "target_language")),
            "product_type": _text(_source_value(fields, "product_type")),
            "account_id": _text(_source_value(fields, "account_id")),
            "scene_preference": _selection_values(_source_value(fields, "scene_preference")),
            "styling_preference": _selection_values(_source_value(fields, "styling_preference")),
            "action_preference": _selection_values(_source_value(fields, "action_preference")),
            "shot_plan_preference": _text(_source_value(fields, "shot_plan_preference")),
            "image_count": len(images) if isinstance(images, list) else 0,
            "request": _text(_source_value(fields, "request")),
            "light_video_status": _text(fields.get("轻量视频状态")),
            "light_video_job_ids": _text(fields.get("轻量视频任务ID")),
            "light_video_error": _text(fields.get("轻量视频错误信息")),
        })
    return matches


def set_source_request(client: Any, product_ref: str, count: int, *, record_id: str | None = None) -> dict[str, Any]:
    if int(count) not in {0, 1, 5}:
        raise ValueError("轻量视频数量只能是 0、1、5")
    matches = find_source_records(client, product_ref)
    if record_id:
        matches = [item for item in matches if item["record_id"] == record_id]
    if not matches:
        raise KeyError(f"找不到产品 {product_ref}" + (f" 的记录 {record_id}" if record_id else ""))
    if len(matches) > 1:
        ids = ", ".join(item["record_id"] for item in matches)
        raise ValueError(f"产品 {product_ref} 匹配到 {len(matches)} 条记录，请用 --record-id 指定: {ids}")
    item = matches[0]
    label = {0: "不生成", 1: "每方案 1 个", 5: "每方案 5 个"}[int(count)]
    client.update_record_fields(item["record_id"], {"每方案视频数量": label})
    return {"record_id": item["record_id"], "product_ref": product_ref, "requested_count": int(count), "value": label}


def _attachment_source(attachment: dict[str, Any]) -> str:
    for key in ("url", "tmp_url", "link"):
        if attachment.get(key):
            return str(attachment[key])
    return ""


def _cache_product_images(client: Any, value: Any, target_dir: Path) -> list[str]:
    attachments = value if isinstance(value, list) else []
    target_dir.mkdir(parents=True, exist_ok=True)
    result: list[str] = []
    for index, attachment in enumerate(attachments, start=1):
        if not isinstance(attachment, dict):
            continue
        token = str(attachment.get("file_token") or "").strip()
        if token:
            raw_name = str(attachment.get("name") or f"image_{index}.jpg")
            suffix = Path(raw_name).suffix if Path(raw_name).suffix else ".jpg"
            path = target_dir / f"{index:02d}_{safe_slug(token, 36)}{suffix.lower()}"
            if not path.exists() or path.stat().st_size == 0:
                content, _, _, _ = client.download_attachment_bytes(attachment)
                path.write_bytes(content)
            result.append(str(path.resolve()))
            continue
        source = _attachment_source(attachment)
        if source:
            result.append(source)
    return result


def _source_payload(record: Any) -> dict[str, Any]:
    fields = dict(record.fields or {})
    images = _source_value(fields, "product_images")
    image_fingerprints = []
    for item in images if isinstance(images, list) else []:
        if isinstance(item, dict):
            image_fingerprints.append({key: item.get(key) for key in ("file_token", "name", "size") if item.get(key) is not None})
    return {
        "source_record_id": record.record_id,
        "request": _text(_source_value(fields, "request")),
        "product_code": _text(_source_value(fields, "product_code")),
        "product_name": _text(_source_value(fields, "product_name")),
        "top_category": _text(_source_value(fields, "top_category")),
        "target_country": _text(_source_value(fields, "target_country")),
        "target_language": _text(_source_value(fields, "target_language")),
        "product_type": _text(_source_value(fields, "product_type")),
        "selling_points": normalized_list(_source_value(fields, "selling_points")),
        "account_id": _text(_source_value(fields, "account_id")),
        "scene_preference": _selection_values(_source_value(fields, "scene_preference")),
        "styling_preference": _selection_values(_source_value(fields, "styling_preference")),
        "action_preference": _selection_values(_source_value(fields, "action_preference")),
        "shot_plan_preference": _text(_source_value(fields, "shot_plan_preference")),
        "images": image_fingerprints,
        "config_version": SOURCE_CONFIG_VERSION,
    }


def _derive_source_status(jobs: list[dict[str, Any]]) -> str:
    if not jobs:
        return "待编排"
    generation = {str(job.get("generation_status") or "pending") for job in jobs}
    if "failed" in generation:
        return "失败"
    if generation != {"success"}:
        return "生成中"
    if all(str(job.get("manual_review_status") or "") == "passed" for job in jobs):
        return "已完成"
    return "待复核"


def _write_source_state(
    client: Any,
    record_id: str,
    *,
    status: str,
    job_ids: Iterable[str] = (),
    error: str = "",
    touch_trigger_time: bool = False,
    visual_plan_ids: Iterable[str] = (),
    estimated_visual_plans: int | None = None,
    estimated_videos: int | None = None,
) -> None:
    payload: dict[str, Any] = {
        "轻量视频状态": status,
        "轻量视频任务ID": "\n".join(job_ids),
        "轻量视频错误信息": str(error)[:2000],
        "视觉方案ID": "\n".join(visual_plan_ids),
    }
    if estimated_visual_plans is not None:
        payload["预计视觉方案数"] = int(estimated_visual_plans)
    if estimated_videos is not None:
        payload["预计视频总数"] = int(estimated_videos)
    if touch_trigger_time:
        payload["轻量视频最近触发时间"] = _now_ms()
    client.update_record_fields(record_id, payload)


def process_source_requests(
    db: LightTryonDB,
    client: Any,
    *,
    record_ids: Iterable[str] | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    selected_ids = {str(item).strip() for item in (record_ids or []) if str(item).strip()}
    candidates: list[tuple[Any, int]] = []
    invalid: list[tuple[Any, Exception]] = []
    for record in client.list_records(page_size=500):
        if selected_ids and record.record_id not in selected_ids:
            continue
        try:
            count = _request_count(_source_value(record.fields or {}, "request"))
        except Exception as exc:
            invalid.append((record, exc))
            continue
        if count is None:
            continue
        candidates.append((record, count))
        if limit and len(candidates) >= int(limit):
            break
    if dry_run:
        return {
            "dry_run": True,
            "candidates": len(candidates),
            "items": [{"record_id": record.record_id, "requested_count": count, "product_code": _text(_source_value(record.fields, "product_code"))} for record, count in candidates],
            "invalid": [{"record_id": record.record_id, "error": str(exc)} for record, exc in invalid],
        }

    summary = {
        "candidates": len(candidates), "created_visual_plans": 0, "active_visual_plans": 0,
        "created_jobs": 0, "existing_jobs": 0, "skipped": 0, "disabled": 0, "failed": len(invalid),
    }
    errors = [{"record_id": record.record_id, "error": str(exc)} for record, exc in invalid]
    for record, count in candidates:
        payload = _source_payload(record)
        source_hash = stable_hash(payload, length=24)
        previous = db.get_source_request(record.record_id)
        changed = not previous or previous.get("source_hash") != source_hash or int(previous.get("requested_count") or 0) != count
        if count == 0:
            existing_jobs = db.list_jobs(source_script_record_id=record.record_id)
            job_ids = [job["job_id"] for job in existing_jobs]
            db.supersede_visual_plans(record.record_id, [])
            visual_plan_ids = [row["visual_plan_id"] for row in db.list_visual_plans(source_record_id=record.record_id)]
            should_write = changed or (previous or {}).get("status") != "disabled" or (previous or {}).get("job_ids") != job_ids
            if should_write:
                _write_source_state(
                    client, record.record_id, status="未触发", job_ids=job_ids, visual_plan_ids=visual_plan_ids,
                    estimated_visual_plans=0, estimated_videos=0, touch_trigger_time=changed,
                )
            db.upsert_source_request({
                "source_record_id": record.record_id, "product_id": (previous or {}).get("product_id") or "",
                "source_product_code": payload["product_code"], "requested_count": 0, "config_version": SOURCE_CONFIG_VERSION,
                "source_hash": source_hash, "source_payload": payload, "status": "disabled", "job_ids": job_ids,
                "visual_plan_ids": visual_plan_ids,
                "last_processed_at": now_iso(), "error_message": "",
            })
            summary["disabled"] += 1
            continue
        record_created_jobs = 0
        try:
            if changed:
                _write_source_state(client, record.record_id, status="待编排", touch_trigger_time=True)
            if payload["top_category"] not in {"女装", "服装", "women_fashion", "womenswear"}:
                raise ValueError(f"轻量试穿仅支持女装，当前一级类目={payload['top_category'] or '空'}")
            if not payload["product_code"]:
                raise ValueError("产品编码为空")
            market = COUNTRY_TO_MARKET.get(payload["target_country"], payload["target_country"].upper())
            if market not in {"TH", "VN", "MY", "MX"}:
                raise ValueError(f"不支持的目标国家: {payload['target_country'] or '空'}")
            raw_language = payload["target_language"].strip().rstrip("）)").strip()
            language = LANGUAGE_TO_CODE.get(raw_language) or DEFAULT_LANGUAGE.get(market, "")
            if not language:
                raise ValueError(f"不支持的目标语言: {payload['target_language'] or '空'}")
            images = _cache_product_images(client, _source_value(record.fields, "product_images"), db.path.parent / "source_images" / record.record_id)
            if not images:
                raise ValueError("产品图片为空或无法缓存")
            internal_product_id = f"OSG_{safe_slug(record.record_id, 48)}"
            category = CATEGORY_TO_CODE.get(payload["product_type"], payload["product_type"] or "top")
            selected_actions = [item for item in payload["action_preference"] if item and item != "自动选择"]
            action_ids = [
                _resolve_template_preference(db, "action", item, {}, allow_testing=True)
                for item in selected_actions
            ]
            shot_plan_id = _resolve_template_preference(
                db, "shot_plan", payload["shot_plan_preference"], {}, allow_testing=True,
            )
            product = ProductInput.from_dict({
                "product_id": internal_product_id,
                "product_name": payload["product_name"] or payload["product_code"],
                "product_title": payload["product_name"] or payload["product_code"],
                "market": market,
                "language": language,
                "category": category,
                "sub_category": payload["product_type"],
                "product_images": images,
                "core_selling_points": payload["selling_points"],
                "recommended_scene_pool": [],
                "recommended_action_pool": action_ids,
                "recommended_styling_pool": [],
                "shot_plan_id": shot_plan_id,
                "target_publish_count": count,
                "account_id": payload["account_id"],
                "source_script_record_id": record.record_id,
                "source_product_code": payload["product_code"],
            })
            db.upsert_product(product)
            before_ids = {row["visual_plan_id"] for row in db.list_visual_plans(source_record_id=record.record_id)}
            visual_plans = orchestrate_visual_plans(
                db,
                internal_product_id,
                source_record_id=record.record_id,
                scene_values=payload["scene_preference"],
                styling_values=payload["styling_preference"],
                per_plan_video_count=count,
                allow_scene_text_fallback=True,
            )
            visual_plan_ids = [row["visual_plan_id"] for row in visual_plans]
            summary["created_visual_plans"] += len(set(visual_plan_ids) - before_ids)
            summary["active_visual_plans"] += len(visual_plans)
            for visual_plan in visual_plans:
                if visual_plan.get("outfit_image_status") == "confirmed":
                    create_result = create_confirmed_video_jobs(db, visual_plan["visual_plan_id"])
                    summary["created_jobs"] += int(create_result["created"])
                    record_created_jobs += int(create_result["created"])
                    summary["existing_jobs"] += int(create_result["existing"])
            jobs = [job for job in db.list_jobs(source_script_record_id=record.record_id) if job.get("visual_plan_id") in visual_plan_ids]
            job_ids = [job["job_id"] for job in jobs]
            outfit_statuses = {str(row.get("outfit_image_status") or "pending") for row in visual_plans}
            if "failed" in outfit_statuses:
                status = "失败"
            elif outfit_statuses & {"pending_review", "regenerate"}:
                status = "待首帧确认"
            elif outfit_statuses & {"pending", "generating"}:
                status = "待首帧生成"
            else:
                status = _derive_source_status(jobs)
            expected_videos = len(visual_plans) * count
            should_write = (
                changed
                or bool(set(visual_plan_ids) - before_ids)
                or (previous or {}).get("status") != status
                or (previous or {}).get("job_ids") != job_ids
                or (previous or {}).get("visual_plan_ids") != visual_plan_ids
                or bool((previous or {}).get("error_message"))
            )
            if should_write:
                _write_source_state(
                    client, record.record_id, status=status, job_ids=job_ids, visual_plan_ids=visual_plan_ids,
                    estimated_visual_plans=len(visual_plans), estimated_videos=expected_videos,
                    touch_trigger_time=changed or bool(set(visual_plan_ids) - before_ids),
                )
            db.upsert_source_request({
                "source_record_id": record.record_id, "product_id": internal_product_id,
                "source_product_code": payload["product_code"], "requested_count": count, "config_version": SOURCE_CONFIG_VERSION,
                "source_hash": source_hash, "source_payload": payload, "status": status, "job_ids": job_ids,
                "visual_plan_ids": visual_plan_ids,
                "last_processed_at": now_iso(), "error_message": "",
            })
            if not changed and not (set(visual_plan_ids) - before_ids) and not record_created_jobs:
                summary["skipped"] += 1
        except Exception as exc:
            message = str(exc)
            if changed or (previous or {}).get("status") != "failed" or (previous or {}).get("error_message") != message:
                _write_source_state(
                    client, record.record_id, status="失败", error=message,
                    visual_plan_ids=(previous or {}).get("visual_plan_ids") or [], touch_trigger_time=changed,
                )
            db.upsert_source_request({
                "source_record_id": record.record_id, "product_id": (previous or {}).get("product_id") or "",
                "source_product_code": payload["product_code"], "requested_count": count, "config_version": SOURCE_CONFIG_VERSION,
                "source_hash": source_hash, "source_payload": payload, "status": "failed", "job_ids": (previous or {}).get("job_ids") or [],
                "visual_plan_ids": (previous or {}).get("visual_plan_ids") or [],
                "last_processed_at": now_iso(), "error_message": message,
            })
            summary["failed"] += 1
            errors.append({"record_id": record.record_id, "error": message})
    return {"dry_run": False, **summary, "errors": errors}
