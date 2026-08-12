#!/usr/bin/env python3
"""Create and seed the Feishu persona template workbench idempotently."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.bitable import FeishuBitableClient, TaskRecord, resolve_wiki_bitable_app_token  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402
from core.production_script_feishu import FieldSpec, ensure_fields, ensure_single_select_options  # noqa: E402
from core.product_type_resolution import normalize_product_type  # noqa: E402

LIGHTWEIGHT_SCRIPTS = (
    SKILL_ROOT.parent / "lightweight-tryon-video" / "scripts"
)
if str(LIGHTWEIGHT_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(LIGHTWEIGHT_SCRIPTS))

from light_tryon.database import LightTryonDB  # noqa: E402


DEFAULT_PERSONA_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "MABkwolpeio1UakHkbgcpJhCncb?table=tblhTNGbDXJL5J4R&view=vewRWDuezE"
)
DEFAULT_STYLING_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "D6GywYTaUixWPDkiwAHcTiMJnmc?table=tblpfrfcCYV9FAGs&view=vewBuZ4Ep4"
)
DEFAULT_DB_PATH = (
    Path(__file__).resolve().parents[2]
    / "lightweight-tryon-video"
    / "var"
    / "light_tryon.sqlite3"
)

STATUS_OPTIONS = ("草稿-待补参考图", "启用", "停用")
COUNTRY_OPTIONS = ("泰国", "美国", "英国", "多国家")
TOP_CATEGORY_OPTIONS = ("女装", "配饰", "通用")
GENDER_OPTIONS = ("女性", "男性", "不指定")
FACE_VISIBILITY_OPTIONS = ("露脸", "手机半遮脸", "不强调脸")
SYNC_STATUS_OPTIONS = ("未同步", "已同步", "同步失败")


def _single(name: str, options: Sequence[str]) -> FieldSpec:
    return FieldSpec(
        name=name,
        field_type=3,
        ui_type="SingleSelect",
        property={"options": [{"name": option} for option in options]},
    )


PERSONA_TEMPLATE_FIELDS: Sequence[FieldSpec] = (
    FieldSpec("人物模板名称（需填写）"),
    _single("模板状态（需填写，仅启用会被流程使用）", STATUS_OPTIONS),
    _single("适用国家（需填写）", COUNTRY_OPTIONS),
    _single("一级类目（需填写）", TOP_CATEGORY_OPTIONS),
    FieldSpec("适用产品类型（需填写，可多填）"),
    FieldSpec("适用展示方式（可选）"),
    _single("性别（需填写）", GENDER_OPTIONS),
    FieldSpec("年龄段（需填写）"),
    FieldSpec("身形气质（需填写）"),
    FieldSpec("身形比例（可选）"),
    _single("脸部可见度（需填写）", FACE_VISIBILITY_OPTIONS),
    FieldSpec("人物参考图（需上传）", 17, "Attachment"),
    FieldSpec("人物身份描述（需填写）"),
    FieldSpec("外貌特征（需填写）"),
    FieldSpec("妆发设定（需填写）"),
    FieldSpec("说话人格（需填写）"),
    FieldSpec("适用展示模式（系统读取）"),
    FieldSpec("人物正向提示（系统读取）"),
    FieldSpec("人物负向提示（系统读取）"),
    FieldSpec("优先级（可选）", 2, "Number"),
    FieldSpec("模板版本（系统，默认V1）"),
    FieldSpec("一致性版本（系统，默认PERSONA_V1）"),
    _single("同步状态（系统）", SYNC_STATUS_OPTIONS),
    FieldSpec("最近同步时间（系统）", 5, "DateTime", {"date_formatter": "yyyy-MM-dd HH:mm", "auto_fill": False}),
    FieldSpec("源快照哈希（系统）"),
    FieldSpec("备注"),
)


SEED_ROWS: Sequence[Dict[str, Any]] = (
    {
        "人物模板ID（需填写）": "TH_APPAREL_COMMUTE_001",
        "人物模板名称（需填写）": "泰国通勤自然分享女生",
        "模板状态（需填写，仅启用会被流程使用）": "草稿-待补参考图",
        "适用国家（需填写）": "泰国",
        "一级类目（需填写）": "女装",
        "适用产品类型（需填写，可多填）": "上装, 外套, 轻上装",
        "适用展示方式（可选）": "PERSON_ON_CAMERA, CREATOR_SELF_SHOT",
        "性别（需填写）": "女性",
        "年龄段（需填写）": "22-30",
        "身形气质（需填写）": "自然匀称，不精修广告感，适合日常通勤穿搭",
        "脸部可见度（需填写）": "露脸",
        "人物身份描述（需填写）": "曼谷年轻通勤女性，下班前或出门前用手机自拍分享穿搭",
        "外貌特征（需填写）": "泰国年轻女性，自然肤质，五官清爽，身材普通偏匀称，不要网红精修脸",
        "妆发设定（需填写）": "黑色或深棕自然长发，轻通勤妆，妆感干净但不过度精致",
        "说话人格（需填写）": "像朋友一样直接分享今天为什么选这件，不夸张表演",
        "适用展示模式（系统读取）": "ON_BODY_RESULT, SCENE_USAGE",
        "人物正向提示（系统读取）": "natural Thai young office woman, casual creator selfie, real skin texture, simple commute styling",
        "人物负向提示（系统读取）": "over-polished commercial model, plastic skin, distorted face, exaggerated influencer pose",
        "优先级（可选）": 80,
        "模板版本（系统，默认V1）": "V1",
        "一致性版本（系统，默认PERSONA_V1）": "PERSONA_V1",
        "同步状态（系统）": "未同步",
        "备注": "补上传人物参考图后再切启用。",
    },
    {
        "人物模板ID（需填写）": "TH_APPAREL_CAFE_001",
        "人物模板名称（需填写）": "泰国咖啡店轻精致女生",
        "模板状态（需填写，仅启用会被流程使用）": "草稿-待补参考图",
        "适用国家（需填写）": "泰国",
        "一级类目（需填写）": "女装",
        "适用产品类型（需填写，可多填）": "上装, 外套, 轻上装",
        "适用展示方式（可选）": "PERSON_ON_CAMERA, CREATOR_SELF_SHOT",
        "性别（需填写）": "女性",
        "年龄段（需填写）": "20-28",
        "身形气质（需填写）": "轻精致但真实，适合咖啡店、书店、精品空间",
        "脸部可见度（需填写）": "露脸",
        "人物身份描述（需填写）": "喜欢探店拍照的泰国年轻女生，在咖啡店或漂亮小店自然分享穿搭",
        "外貌特征（需填写）": "泰国年轻女性，自然甜酷气质，脸部不过度磨皮，比例真实",
        "妆发设定（需填写）": "微卷深色长发或半扎发，轻社交妆，唇色自然",
        "说话人格（需填写）": "语气亲近，有一点小兴奋，但不是广告式夸张",
        "适用展示模式（系统读取）": "ON_BODY_RESULT, DETAIL_SHOW",
        "人物正向提示（系统读取）": "Thai fashion creator in cafe, natural phone selfie, stylish but realistic, warm ambient light",
        "人物负向提示（系统读取）": "luxury ad campaign, glossy studio commercial, unreal filter, unnatural doll face",
        "优先级（可选）": 70,
        "模板版本（系统，默认V1）": "V1",
        "一致性版本（系统，默认PERSONA_V1）": "PERSONA_V1",
        "同步状态（系统）": "未同步",
        "备注": "适合显贵、上镜、探店类卖点。",
    },
    {
        "人物模板ID（需填写）": "TH_SCARF_RESORT_001",
        "人物模板名称（需填写）": "泰国丝巾度假轻熟女生",
        "模板状态（需填写，仅启用会被流程使用）": "草稿-待补参考图",
        "适用国家（需填写）": "泰国",
        "一级类目（需填写）": "配饰",
        "适用产品类型（需填写，可多填）": "丝巾, 围巾",
        "适用展示方式（可选）": "PERSON_ON_CAMERA, NECK_WORN, HAIR_TIE, BAG_ACCENT",
        "性别（需填写）": "女性",
        "年龄段（需填写）": "22-32",
        "身形气质（需填写）": "暖天气轻熟感，适合吊带、短裤、阔腿裤和度假场景",
        "脸部可见度（需填写）": "露脸",
        "人物身份描述（需填写）": "泰国城市度假感女生，出门前用丝巾给基础穿搭增加精致感",
        "外貌特征（需填写）": "自然泰国女性气质，肩颈线条干净，整体真实不僵硬",
        "妆发设定（需填写）": "低饱和妆容，头发自然披散或半扎，适合丝巾点缀",
        "说话人格（需填写）": "轻松介绍一个小搭配技巧，像给姐妹看今天的小心机",
        "适用展示模式（系统读取）": "NECK_WORN, HAIR_TIE, BAG_ACCENT",
        "人物正向提示（系统读取）": "Thai resort casual woman, silk scarf styling, camisole and wide-leg pants, natural sunlight, phone selfie",
        "人物负向提示（系统读取）": "airline uniform look, stiff formal office outfit, overdone beauty filter, extra hands",
        "优先级（可选）": 90,
        "模板版本（系统，默认V1）": "V1",
        "一致性版本（系统，默认PERSONA_V1）": "PERSONA_V1",
        "同步状态（系统）": "未同步",
        "备注": "专门避免丝巾变成航空制服感。",
    },
    {
        "人物模板ID（需填写）": "TH_HEADSCARF_STREET_001",
        "人物模板名称（需填写）": "泰国头巾街头时尚女生",
        "模板状态（需填写，仅启用会被流程使用）": "草稿-待补参考图",
        "适用国家（需填写）": "泰国",
        "一级类目（需填写）": "配饰",
        "适用产品类型（需填写，可多填）": "头巾, 发饰",
        "适用展示方式（可选）": "PERSON_ON_CAMERA, HEAD_WORN",
        "性别（需填写）": "女性",
        "年龄段（需填写）": "19-28",
        "身形气质（需填写）": "轻辣妹、街头、Y2K，但保留真实自拍感",
        "脸部可见度（需填写）": "露脸",
        "人物身份描述（需填写）": "泰国年轻时尚女生，在户外或半户外用头巾完成更有态度的出门造型",
        "外貌特征（需填写）": "年轻泰国女性，自信但不摆拍，脸部真实不过度变形",
        "妆发设定（需填写）": "深色长发或卷发，头巾已佩戴完成，轻辣妹妆容",
        "说话人格（需填写）": "直接、俏皮、像给朋友展示今天这个造型为什么更完整",
        "适用展示模式（系统读取）": "HEAD_WORN",
        "人物正向提示（系统读取）": "Thai street fashion girl, Y2K headscarf look, confident natural selfie, realistic face, trendy warm-weather outfit",
        "人物负向提示（系统读取）": "religious identity implication, costume look, heavy face filter, deformed face, extra hands",
        "优先级（可选）": 95,
        "模板版本（系统，默认V1）": "V1",
        "一致性版本（系统，默认PERSONA_V1）": "PERSONA_V1",
        "同步状态（系统）": "未同步",
        "备注": "头巾必须先用统一首帧，参考图补齐前不要启用。",
    },
)


def _client(url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(url)
    if not info:
        raise ValueError(f"无法解析飞书链接: {url}")
    app_token = info.app_token
    if "/wiki/" in url:
        app_token = resolve_wiki_bitable_app_token(app_token)
    return FeishuBitableClient(app_token, info.table_id)


def _records_by_persona_id(records: Iterable[TaskRecord]) -> Dict[str, TaskRecord]:
    output: Dict[str, TaskRecord] = {}
    for record in records:
        persona_id = str(record.fields.get("人物模板ID（需填写）") or "").strip()
        if persona_id:
            output[persona_id] = record
    return output


def _row_signature(row: Dict[str, Any]) -> str:
    material = {
        key: value
        for key, value in row.items()
        if key not in {"同步状态（系统）", "最近同步时间（系统）", "源快照哈希（系统）"}
    }
    import hashlib

    return hashlib.sha256(
        json.dumps(material, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]


def _text(value: Any) -> str:
    return str(value or "").strip()


def _json(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    text = _text(value)
    if not text:
        return default
    try:
        return json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return default


def _list(value: Any) -> List[str]:
    parsed = _json(value, None)
    if isinstance(parsed, list):
        return [_text(item) for item in parsed if _text(item)]
    text = _text(value)
    if not text:
        return []
    return [item.strip() for item in text.replace("，", ",").split(",") if item.strip()]


def _has_reference_asset(value: Any) -> bool:
    for item in _list(value):
        if item:
            return True
    parsed = _json(value, [])
    if isinstance(parsed, list):
        for item in parsed:
            if isinstance(item, dict) and _text(
                item.get("file_token")
                or item.get("asset_id")
                or item.get("url")
                or item.get("path")
                or item.get("cached_path")
            ):
                return True
    return False


def _gender_label(value: Any) -> str:
    text = _text(value).lower()
    if text in {"female", "woman", "girl", "女性", "女"}:
        return "女性"
    if text in {"male", "man", "男性", "男"}:
        return "男性"
    return "不指定"


def _face_visibility_label(value: Any) -> str:
    text = _text(value).lower()
    if "covered" in text or "遮" in text or "phone" in text:
        return "手机半遮脸"
    if "visible" in text or "face" in text or "露" in text:
        return "露脸"
    return "不强调脸"


def _country_label(markets: Any) -> str:
    market_items = {item.upper() for item in _list(markets)}
    if market_items == {"TH"} or "泰国" in market_items:
        return "泰国"
    if market_items == {"US"} or "美国" in market_items:
        return "美国"
    if market_items == {"UK"} or "英国" in market_items:
        return "英国"
    return "多国家"


def _market_codes(value: Any) -> List[str]:
    text = _text(value).lower()
    if text in {"泰国", "th", "thai", "thailand"}:
        return ["TH"]
    if text in {"美国", "us", "usa", "united states"}:
        return ["US"]
    if text in {"英国", "uk", "gb", "united kingdom"}:
        return ["UK"]
    if text in {"多国家", "通用", "*", "all"}:
        return ["*"]
    return [_text(value)] if _text(value) else ["*"]


def _applicable_categories(value: Any) -> List[str]:
    text = _text(value)
    return ["*"] if text in {"", "通用", "*"} else [text]


def _canonical_product_types(value: Any, top_category: str) -> List[str]:
    output: List[str] = []
    for item in _list(value):
        if item in {"*", "通用", "全部"}:
            output.append("*")
            continue
        canonical = normalize_product_type(item, top_category).canonical_type
        if canonical:
            output.append(canonical)
    return list(dict.fromkeys(output or ["*"]))


def _supported_modes(value: Any, *, top_category: str) -> List[str]:
    modes = [item.upper() for item in _list(value)]
    # The workbench's human-readable proof modes predate the current garment
    # execution code.  They all describe an already-worn apparel result.
    if top_category == "女装" and (
        not modes
        or {"ON_BODY_RESULT", "SCENE_USAGE", "DETAIL_SHOW"} & set(modes)
    ):
        modes.append("GARMENT_WORN")
    return list(dict.fromkeys(modes))


def _presentation_scope(value: Any) -> tuple[List[str], List[str]]:
    values = [item.upper() for item in _list(value)]
    presentation = [
        item for item in values
        if item in {"PERSON_ON_CAMERA", "WEARER_ACTIVE", "MIXED"}
    ]
    capture = [
        item for item in values
        if item in {"CREATOR_SELF_SHOT", "HANDS_PRODUCT_SHARE", "STATIC_PRODUCT_RECORD"}
    ]
    return presentation, capture


def _status_to_backend(value: Any, *, has_reference: bool) -> tuple[str, str]:
    text = _text(value)
    if text == "启用" and has_reference:
        return "enabled", ""
    if text == "启用" and not has_reference:
        return "testing", "启用失败：必须先上传人物参考图"
    if text == "停用":
        return "disabled", ""
    return "testing", ""


def _record_to_db_payload(fields: Dict[str, Any]) -> tuple[Dict[str, Any], str]:
    persona_id = _text(fields.get("人物模板ID（需填写）"))
    persona_name = _text(fields.get("人物模板名称（需填写）"))
    if not persona_id:
        raise ValueError("缺少人物模板ID")
    if not persona_name:
        raise ValueError(f"{persona_id} 缺少人物模板名称")

    references = fields.get("人物参考图（需上传）")
    references = references if isinstance(references, list) else []
    has_reference = _has_reference_asset(references)
    status, warning = _status_to_backend(
        fields.get("模板状态（需填写，仅启用会被流程使用）"),
        has_reference=has_reference,
    )
    top_category = _text(fields.get("一级类目（需填写）")) or "通用"
    product_types = _canonical_product_types(
        fields.get("适用产品类型（需填写，可多填）"), top_category
    )
    modes = _supported_modes(
        fields.get("适用展示模式（系统读取）"),
        top_category=top_category,
    )
    presentations, captures = _presentation_scope(
        fields.get("适用展示方式（可选）")
    )
    source_payload = {
        "applicable_categories": _applicable_categories(top_category),
        "applicable_product_types": product_types,
        "supported_demonstration_modes": modes,
        "supported_presentation_modes": presentations,
        "supported_capture_modes": captures,
        "identity_text": _text(fields.get("人物身份描述（需填写）")),
        "appearance_text": _text(fields.get("外貌特征（需填写）")),
        "body_proportion_text": _text(fields.get("身形比例（可选）")),
        "hair_makeup_text": _text(fields.get("妆发设定（需填写）")),
        "speaking_personality": _text(fields.get("说话人格（需填写）")),
        "workbench_status": _text(
            fields.get("模板状态（需填写，仅启用会被流程使用）")
        ),
    }
    payload = {
        "persona_id": persona_id,
        "persona_name": persona_name,
        "status": status,
        "gender": {
            "女性": "female", "男性": "male", "不指定": "unspecified",
        }.get(_text(fields.get("性别（需填写）")), "unspecified"),
        "age_group": _text(fields.get("年龄段（需填写）")),
        "body_type": _text(fields.get("身形气质（需填写）")),
        "hair_style": "",
        "hair_color": "",
        "skin_tone": "",
        "face_visibility": {
            "露脸": "visible", "手机半遮脸": "phone_partial",
            "不强调脸": "unspecified",
        }.get(_text(fields.get("脸部可见度（需填写）")), "unspecified"),
        "makeup_style": "",
        "vibe": [_text(fields.get("身形气质（需填写）"))]
        if _text(fields.get("身形气质（需填写）")) else [],
        "prompt_core": _text(fields.get("人物正向提示（系统读取）"))
        or _text(fields.get("人物身份描述（需填写）")),
        "prompt_negative": _text(fields.get("人物负向提示（系统读取）")),
        "priority": int(float(fields.get("优先级（可选）") or 0)),
        "markets": _market_codes(fields.get("适用国家（需填写）")),
        "reference_images": references,
        "config_version": _text(fields.get("模板版本（系统，默认V1）")) or "V1",
        "consistency_version": _text(
            fields.get("一致性版本（系统，默认PERSONA_V1）")
        ) or "PERSONA_V1",
        "source_hash": _row_signature(fields),
        "source_payload": source_payload,
        "notes": _text(fields.get("备注")),
    }
    return payload, warning


def sync_to_db(
    client: FeishuBitableClient, *, db_path: Path
) -> Dict[str, Any]:
    """Pull the dedicated persona workbench into the shared local store."""

    db = LightTryonDB(db_path)
    db.init_schema()
    counts = {"updated": 0, "draft_or_disabled": 0, "failed": 0}
    warnings: List[str] = []
    errors: List[str] = []
    now_ms = int(datetime.now().timestamp() * 1000)
    for record in client.list_records(page_size=100):
        fields = dict(record.fields or {})
        try:
            payload, warning = _record_to_db_payload(fields)
            payload["feishu_record_id"] = record.record_id
            payload["last_synced_at"] = datetime.now().astimezone().isoformat()
            payload["sync_status"] = "synced"
            payload["sync_error"] = warning
            db.upsert_template("persona", payload)
            counts["updated"] += 1
            if payload["status"] != "enabled":
                counts["draft_or_disabled"] += 1
            if warning:
                warnings.append(f"{payload['persona_id']}:{warning}")
            client.update_record_fields(record.record_id, {
                "同步状态（系统）": "同步失败" if warning else "已同步",
                "最近同步时间（系统）": now_ms,
                "源快照哈希（系统）": payload["source_hash"],
            })
        except Exception as exc:
            counts["failed"] += 1
            persona_id = _text(fields.get("人物模板ID（需填写）")) or record.record_id
            errors.append(f"{persona_id}:{exc}")
            client.update_record_fields(record.record_id, {
                "同步状态（系统）": "同步失败",
                "最近同步时间（系统）": now_ms,
            })
    return {
        "db_path": str(db_path),
        **counts,
        "warnings": warnings,
        "errors": errors,
    }


def _db_rows(db_path: Path) -> List[Dict[str, Any]]:
    if not db_path.exists():
        raise FileNotFoundError(f"人物模板数据库不存在: {db_path}")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT * FROM persona_templates ORDER BY persona_id").fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows]


def _db_row_to_feishu(row: Dict[str, Any]) -> Dict[str, Any]:
    source = _json(row.get("source_payload"), {})
    if not isinstance(source, dict):
        source = {}
    references_available = _has_reference_asset(row.get("reference_images"))
    raw_status = _text(row.get("status")) or "unknown"
    feishu_status = "启用" if raw_status == "enabled" and references_available else "草稿-待补参考图"
    applicable_categories = _list(source.get("applicable_categories"))
    product_types = _list(source.get("applicable_product_types"))
    supported_modes = _list(source.get("supported_demonstration_modes"))
    identity = _text(source.get("identity_text"))
    appearance = _text(source.get("appearance_text"))
    hair_makeup = _text(source.get("hair_makeup_text"))
    speaking = _text(source.get("speaking_personality"))
    if not product_types:
        product_types = ["*"]
    remarks = _text(row.get("notes"))
    if raw_status == "enabled" and not references_available:
        remarks = (remarks + "；" if remarks else "") + "数据库原状态为 enabled，但缺人物参考图，原创流程不会使用。"
    payload = {
        "人物模板ID（需填写）": _text(row.get("persona_id")),
        "人物模板名称（需填写）": _text(row.get("persona_name")),
        "模板状态（需填写，仅启用会被流程使用）": feishu_status,
        "适用国家（需填写）": _country_label(row.get("markets")),
        "一级类目（需填写）": applicable_categories[0] if applicable_categories else "通用",
        "适用产品类型（需填写，可多填）": ", ".join(product_types),
        "适用展示方式（可选）": ", ".join(supported_modes) or "CREATOR_SELF_SHOT",
        "性别（需填写）": _gender_label(row.get("gender")),
        "年龄段（需填写）": _text(row.get("age_group")) or "young_adult",
        "身形气质（需填写）": _text(row.get("body_type")),
        "身形比例（可选）": _text(source.get("body_proportion_text")),
        "脸部可见度（需填写）": _face_visibility_label(row.get("face_visibility")),
        "人物身份描述（需填写）": identity or _text(row.get("prompt_core")),
        "外貌特征（需填写）": appearance or _text(row.get("prompt_core")),
        "妆发设定（需填写）": hair_makeup or ", ".join(
            item for item in (_text(row.get("hair_color")), _text(row.get("hair_style")), _text(row.get("makeup_style"))) if item
        ),
        "说话人格（需填写）": speaking or "自然真实的朋友式分享",
        "适用展示模式（系统读取）": ", ".join(supported_modes),
        "人物正向提示（系统读取）": _text(row.get("prompt_core")),
        "人物负向提示（系统读取）": _text(row.get("prompt_negative")),
        "优先级（可选）": int(row.get("priority") or 0),
        "模板版本（系统，默认V1）": _text(row.get("config_version")) or "V1",
        "一致性版本（系统，默认PERSONA_V1）": _text(row.get("consistency_version")) or "PERSONA_V1",
        "同步状态（系统）": "已同步",
        "源快照哈希（系统）": _text(row.get("source_hash")) or _row_signature(row),
        "备注": remarks,
    }
    return payload


def ensure_schema(client: FeishuBitableClient) -> None:
    ensure_fields(
        client,
        primary_field_name="人物模板ID（需填写）",
        specs=PERSONA_TEMPLATE_FIELDS,
    )
    ensure_single_select_options(
        client,
        {
            "模板状态（需填写，仅启用会被流程使用）": STATUS_OPTIONS,
            "适用国家（需填写）": COUNTRY_OPTIONS,
            "一级类目（需填写）": TOP_CATEGORY_OPTIONS,
            "性别（需填写）": GENDER_OPTIONS,
            "脸部可见度（需填写）": FACE_VISIBILITY_OPTIONS,
            "同步状态（系统）": SYNC_STATUS_OPTIONS,
        },
    )


def ensure_outfit_persona_options(
    persona_client: FeishuBitableClient,
    *,
    styling_url: str = DEFAULT_STYLING_URL,
) -> Dict[str, Any]:
    """Expose stable persona ids as selectable outfit-template preferences."""

    persona_records = _records_by_persona_id(
        persona_client.list_records(page_size=100)
    )
    personas = sorted(
        (
            persona_id,
            _text(record.fields.get("人物模板名称（需填写）")) or persona_id,
        )
        for persona_id, record in persona_records.items()
    )
    names_seen: Dict[str, int] = {}
    for _, persona_name in personas:
        names_seen[persona_name] = names_seen.get(persona_name, 0) + 1
    duplicate_names = sorted(
        name for name, count in names_seen.items() if count > 1
    )
    if duplicate_names:
        raise ValueError(
            "人物模板名称必须唯一，重复名称: " + ", ".join(duplicate_names)
        )
    persona_ids = [persona_id for persona_id, _ in personas]
    persona_names = [persona_name for _, persona_name in personas]
    styling_client = _client(styling_url)
    field_name = "适配人物模板"
    fields = styling_client.list_fields()
    current = next(
        (
            field for field in fields
            if _text(field.get("field_name")) == field_name
        ),
        None,
    )
    desired_options = [{"name": persona_name} for persona_name in persona_names]
    if current is None:
        styling_client.create_field(
            field_name,
            field_type=4,
            ui_type="MultiSelect",
            property={"options": desired_options},
        )
        return {
            "field": field_name,
            "created": True,
            "added_options": persona_names,
            "persona_name_by_id": dict(personas),
        }

    current_options = list(
        ((current.get("property") or {}).get("options") or [])
    )
    current_by_name = {
        _text(option.get("name")): option
        for option in current_options
        if isinstance(option, dict) and _text(option.get("name"))
    }
    normalized_options: List[Dict[str, Any]] = []
    added_names: List[str] = []
    for persona_id, persona_name in personas:
        # Preserve the Feishu option id while replacing the visible machine
        # id with the operator-facing persona name. Existing selections stay
        # selected because their option identity does not change.
        existing = current_by_name.get(persona_name) or current_by_name.get(
            persona_id
        )
        if existing:
            normalized_options.append({**existing, "name": persona_name})
        else:
            normalized_options.append({"name": persona_name})
            added_names.append(persona_name)
    current_names = set(current_by_name)
    desired_names = set(persona_names)
    if int(current.get("type") or 0) != 4 or current_names != desired_names:
        styling_client.update_field_name(
            _text(current.get("field_id")),
            field_name,
            field_type=4,
            property={"options": normalized_options},
        )
    return {
        "field": field_name,
        "created": False,
        "added_options": added_names,
        "available_persona_ids": persona_ids,
        "available_persona_names": persona_names,
        "persona_name_by_id": dict(personas),
        "migrated_from_ids": sorted(current_names & set(persona_ids)),
    }


def seed_rows(client: FeishuBitableClient, *, update_existing: bool) -> Dict[str, Any]:
    records = _records_by_persona_id(client.list_records(page_size=100))
    created_payloads: List[Dict[str, Any]] = []
    updated: List[str] = []
    skipped: List[str] = []
    for row in SEED_ROWS:
        payload = dict(row)
        payload["源快照哈希（系统）"] = _row_signature(payload)
        persona_id = str(payload["人物模板ID（需填写）"])
        existing = records.get(persona_id)
        if not existing:
            created_payloads.append({"fields": payload})
            continue
        if update_existing:
            client.update_record_fields(existing.record_id, payload)
            updated.append(persona_id)
        else:
            skipped.append(persona_id)
    created = client.batch_create_records(created_payloads)
    return {
        "created": len(created),
        "created_record_ids": created,
        "updated": updated,
        "skipped_existing": skipped,
    }


def sync_from_db(
    client: FeishuBitableClient,
    *,
    db_path: Path,
    update_existing: bool,
) -> Dict[str, Any]:
    records = _records_by_persona_id(client.list_records(page_size=100))
    created_payloads: List[Dict[str, Any]] = []
    updated: List[str] = []
    skipped: List[str] = []
    for row in _db_rows(db_path):
        payload = _db_row_to_feishu(row)
        persona_id = str(payload["人物模板ID（需填写）"])
        if not persona_id:
            continue
        existing = records.get(persona_id)
        if not existing:
            created_payloads.append({"fields": payload})
            continue
        if update_existing:
            client.update_record_fields(existing.record_id, payload)
            updated.append(persona_id)
        else:
            skipped.append(persona_id)
    created = client.batch_create_records(created_payloads)
    return {
        "db_path": str(db_path),
        "created": len(created),
        "created_record_ids": created,
        "updated": updated,
        "skipped_existing": skipped,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_PERSONA_URL)
    parser.add_argument("--no-seed", action="store_true")
    parser.add_argument("--update-existing", action="store_true")
    parser.add_argument("--sync-from-db", action="store_true")
    parser.add_argument("--pull-to-db", action="store_true")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--styling-url", default=DEFAULT_STYLING_URL)
    parser.add_argument("--no-sync-outfit-options", action="store_true")
    args = parser.parse_args()

    client = _client(args.url)
    ensure_schema(client)
    result: Dict[str, Any] = {"schema": "ok"}
    if args.pull_to_db:
        result["db_pull"] = sync_to_db(
            client,
            db_path=Path(args.db_path).expanduser().resolve(),
        )
    if args.sync_from_db:
        result["db_sync"] = sync_from_db(
            client,
            db_path=Path(args.db_path).expanduser().resolve(),
            update_existing=args.update_existing,
        )
    if not args.no_seed:
        result["seed"] = seed_rows(client, update_existing=args.update_existing)
    if not args.no_sync_outfit_options:
        result["outfit_persona_options"] = ensure_outfit_persona_options(
            client,
            styling_url=args.styling_url,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
