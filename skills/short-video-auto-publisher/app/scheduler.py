#!/usr/bin/env python3
"""视频同步、账号同步、排班与结果回写。"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
import hashlib
import json
import importlib.util
import subprocess
import os
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from types import SimpleNamespace

import requests

from app import bgm
from app.db import (
    AutoPublishDB,
    default_video_dir,
    is_nurture_candidate,
    is_opv_initialization_candidate,
)
from app.metadata import infer_country_from_store_id, localized_template_title, sanitize_title, is_title_compatible_with_country
from app.models import AccountConfig, PublishRequest, ScriptMetadata
from app.publishers import BasePublishAdapter, DryRunPublishAdapter
from app.script_pool import publishing_product_id, resolve_cart, register_script_pool_metadata

RUN_MANAGER_FIELD_ALIASES: Dict[str, List[str]] = {
    "task_name": ["任务名", "任务名称"],
    "prompt": ["提示词", "视频提示词"],
    "canonical_script_key": ["内部脚本键", "稳定脚本键", "canonical_script_key"],
    "script_id": ["脚本ID"],
    "store_id": ["店铺ID", "店铺"],
    "product_id": ["产品ID", "商品ID"],
    "canonical_product_id": ["全球产品ID"],
    "script_type": ["脚本类型"],
    "script_source": ["任务来源", "脚本来源", "来源"],
    "publish_purpose": ["发布用途", "用途"],
    "cart_enabled": ["是否挂车", "挂车"],
    "content_branch": ["内容分支"],
    "parent_slot": ["所属母版"],
    "direction_label": ["母版方向"],
    "variant_strength": ["变体强度"],
    "target_country": ["目标国家", "目标语言", "语言"],
    "target_language": ["目标语言", "语言"],
    "product_type": ["产品类型", "品类"],
    "run_video_status": ["跑视频状态", "状态"],
    "publish_enabled": ["是否发布", "是否自动发布"],
    "video_attachment": ["视频附件", "生成视频"],
    "voiceover_requested": ["是否配口播"],
    "voiceover_status": ["口播状态"],
    "voiceover_attachment": ["口播成片"],
    "audio_mode": ["音频模式", "原声模式"],
    "short_video_title": ["短视频标题", "视频标题", "发布标题", "人工标题"],
    "video_link": ["视频链接", "视频附件 / 视频链接"],
    "oss_object_id": ["OSS对象ID"],
    "oss_path": ["OSS路径"],
    "material_asset_id": ["素材ID"],
    "download_status": ["下载状态"],
    "local_file_path": ["本地文件路径"],
    "publish_status": ["发布状态"],
    "account_id": ["分配账号ID"],
    "account_name": ["分配账号名称"],
    "planned_publish_at": ["计划发布时间"],
    "published_at": ["发布时间"],
    "publish_result": ["发布结果"],
    "publish_task_id": ["发布任务ID"],
}

ACCOUNT_FIELD_ALIASES: Dict[str, List[str]] = {
    "account_id": ["账号ID"],
    "account_name": ["账号名称"],
    "store_id": ["店铺ID"],
    "account_status": ["账号状态"],
    "publish_channel": ["发布渠道", "发布平台", "发布方式", "publish_channel"],
    "publish_time_1": ["发布时间1"],
    "publish_time_2": ["发布时间2"],
    "publish_time_3": ["发布时间3"],
    "nurture_enabled": ["是否开启养号"],
    "nurture_daily_count": ["每日养号条数"],
    "nurture_only": ["是否仅养号"],
    "initialization_enabled": ["是否账号初始化", "initialization_enabled"],
    "publish_profile_id": ["发布配置"],
    "provider_connection_uid": ["CreatOK连接UID", "CreatOK 连接UID"],
    "account_timezone": ["账号时区"],
    "delivery_mode": ["CreatOK投递模式", "CreatOK 投递模式", "投递模式"],
    "provider_health": ["CreatOK能力状态", "CreatOK 能力状态"],
    "content_scope": ["内容类型限定", "内容类型", "带货限定"],
    "provider_checked_at": ["能力检查时间"],
    # OPV 定位账号内容配置（2026-09-15）：账号表已有列优先复用（如“视频风格”
    # 列同时服务图文摄影基准），缺列时由 ensure_account_nurture_fields 补建。
    "positioning": ["账号定位", "定位"],
    "default_theme": ["默认主题", "账号默认主题"],
    "expression_mode": ["内容表达"],
    "visual_style": ["视觉风格", "视频风格"],
    "style_image": ["风格图片"],
    "photo_claim_scope": ["图文领取范围"],
    # 自动图文供稿策略（2026-09-16，OPV 自动供稿 Phase 2）：全部为空＝未配置，
    # profile 不加 photo_supply_policy 键，账号行为与旧版完全一致。
    "supply_strategy": ["图文内容策略"],
    "supply_product_mode": ["商品使用方式"],
    "supply_product_codes": ["默认产品编码"],
    "supply_automation": ["图文自动化模式"],
    "supply_daily_limit": ["每日自动生产上限"],
    "supply_preset": ["自动供稿预设"],
    "supply_material_scope": ["素材范围"],
}


def resolve_field_mapping(field_names: Sequence[str], aliases: Dict[str, List[str]]) -> Dict[str, Optional[str]]:
    mapping: Dict[str, Optional[str]] = {}
    for logical_name, candidates in aliases.items():
        mapping[logical_name] = next((candidate for candidate in candidates if candidate in field_names), None)
    return mapping


def normalize_text(raw_value: Any) -> str:
    if raw_value is None:
        return ""
    return str(raw_value).strip()


def _choice_text(raw_value: Any) -> str:
    if isinstance(raw_value, dict):
        for key in ("text", "name", "value"):
            text = normalize_text(raw_value.get(key))
            if text:
                return text
        return ""
    if isinstance(raw_value, list):
        for item in raw_value:
            text = _choice_text(item)
            if text:
                return text
        return ""
    return normalize_text(raw_value)


def normalize_publish_channel(raw_value: Any) -> str:
    text = _choice_text(raw_value)
    if not text:
        return "GeeLark"
    compact = text.strip().lower().replace(" ", "").replace("-", "").replace("_", "")
    if compact in {"neobund", "neobundai", "neobundai"} or "neobund" in compact:
        return "NeoBund"
    if compact in {"geelark", "geelarkcloudphone"} or "geelark" in compact:
        return "GeeLark"
    if "creatok" in compact:
        return "CreatOK"
    if compact in {"manual", "hand", "human", "人工", "手动", "人工发布", "手动发布"}:
        return "手动"
    if compact in {"pause", "paused", "disabled", "disable", "stop", "暂停", "停用", "停止"}:
        return "暂停"
    return text


def normalize_checkbox(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "是", "已勾选", "勾选", "checked"}
    return False


def normalize_creatok_delivery_mode(raw_value: Any) -> str:
    """账号表"CreatOK投递模式"（直接发布/收件箱）→ 存储值 direct_post/inbox。"""
    text = _choice_text(raw_value).strip().lower()
    if not text:
        return ""
    if "inbox" in text or "收件" in text or "待确认" in text:
        return "inbox"
    if "direct" in text or "直接" in text or "post" in text:
        return "direct_post"
    return text


def normalize_content_scope(raw_value: Any) -> str:
    """账号表"内容类型限定"（带货/不带货/全部）→ shoppable/organic/all。"""
    text = _choice_text(raw_value).strip().lower()
    if not text:
        return "all"
    if "不带货" in text or "非带货" in text or "养号" in text or "organic" in text:
        return "organic"
    if "带货" in text or "shoppable" in text:
        return "shoppable"
    if "全部" in text or "all" in text:
        return "all"
    return "all"


def normalize_creatok_health(raw_value: Any) -> str:
    """账号表"CreatOK能力状态"（未知/正常/异常）→ 存储值 unknown/ok/error。"""
    text = _choice_text(raw_value).strip().lower()
    if not text:
        return ""
    if text in {"ok", "ready", "healthy", "正常"}:
        return "ok"
    if text in {"error", "failed", "limited", "异常"}:
        return "error"
    return "unknown"


def should_mark_ai_for_geelark(candidate: Any) -> Optional[bool]:
    """Original/remake videos should be marked as AI-generated in GeeLark."""
    markers = " ".join(
        str(getattr(candidate, attr, "") or "").strip().lower()
        for attr in ("script_source", "publish_purpose", "content_branch")
    )
    if "混剪" in markers or "mixcut" in markers:
        return None
    return True


def is_short_video_remake_candidate(candidate: Any) -> bool:
    markers = " ".join(
        str(getattr(candidate, attr, "") or "").strip()
        for attr in ("script_source", "publish_purpose", "content_branch")
    )
    return "短视频复刻" in markers


def is_non_shoppable_candidate(candidate: Any) -> bool:
    """Cart intent is independent of the nurture pool; seeding stays organic."""
    try:
        return resolve_cart(candidate) == "否"
    except ValueError:
        return True  # Unknown input must never enable product binding.


def account_can_publish_candidate(account: Any, candidate: Any, publish_channel: str = "") -> bool:
    if account is None:
        return False
    channel = normalize_publish_channel(publish_channel or account["publish_channel"])
    if getattr(candidate, "content_type", "video") == "photo":
        if channel != "CreatOK":
            return False
        capability_field = "content_photo_capable" if is_non_shoppable_candidate(candidate) else "shop_photo_capable"
        return capability_field in account.keys() and bool(int(account[capability_field] or 0))
    if channel == "CreatOK":
        capability_field = "content_video_capable" if is_non_shoppable_candidate(candidate) else "shop_video_capable"
        return capability_field in account.keys() and bool(int(account[capability_field] or 0))
    if channel != "NeoBund":
        return True
    if str(account["capability_status"] or "").strip() != "ok":
        return False
    capability_field = "organic_capable" if is_non_shoppable_candidate(candidate) else "shoppable_capable"
    return bool(int(account[capability_field] or 0))


def build_run_manager_seeding_metadata(
    fields: Dict[str, Any],
    mapping: Dict[str, Optional[str]],
    *,
    record_id: str,
) -> Optional[ScriptMetadata]:
    """Build fail-closed metadata for seed scripts that bypass the legacy script table."""

    def value(logical_name: str) -> str:
        field_name = mapping.get(logical_name)
        return normalize_text(fields.get(field_name)) if field_name else ""

    markers = " ".join(
        value(name)
        for name in ("script_type", "script_source", "publish_purpose", "content_branch")
    )
    if not any(marker in markers for marker in ("种草脚本", "种草", "SEEDING_ORGANIC")):
        return None

    script_id = value("script_id")
    canonical_key = value("canonical_script_key")
    store_id = value("store_id")
    if not script_id or not canonical_key or not store_id:
        return None

    source_record_id = record_id
    canonical_parts = canonical_key.split(":", 2)
    if len(canonical_parts) == 3 and canonical_parts[0] == "seeding":
        source_record_id = canonical_parts[1] or record_id

    metadata = ScriptMetadata(
        canonical_script_key=canonical_key,
        script_id=script_id,
        source_record_id=source_record_id,
        script_slot="SEED",
        task_no=value("task_name") or script_id,
        store_id=store_id,
        product_id="",
        parent_slot=value("parent_slot") or "SEED",
        direction_label=value("direction_label") or "种草内容",
        variant_strength=value("variant_strength") or "母版",
        target_country=value("target_country") or infer_country_from_store_id(store_id),
        product_type=value("product_type"),
        content_family_key=canonical_key,
        script_text=value("prompt"),
        short_video_title=sanitize_title(fields.get(mapping.get("short_video_title"))),
        title_source="run_manager_seeding",
        script_source="种草脚本",
        publish_purpose="种草",
        cart_enabled="否",
        content_branch="SEEDING_ORGANIC",
    )
    if metadata.short_video_title:
        return metadata
    return replace(
        metadata,
        short_video_title=localized_template_title(metadata),
        title_source="run_manager_seeding_fallback",
    )


def normalize_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value or "").strip()))
    except (TypeError, ValueError):
        return default


def extract_attachment(raw_value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw_value, list):
        for item in raw_value:
            if isinstance(item, dict) and item.get("file_token"):
                return item
    if isinstance(raw_value, dict) and raw_value.get("file_token"):
        return raw_value
    return None


def select_publish_attachment(
    fields: Dict[str, Any],
    mapping: Dict[str, Optional[str]],
) -> Tuple[Optional[Dict[str, Any]], str]:
    """Select the final publish artifact without leaking raw video.

    When narration was requested, the generated source video is deliberately
    blocked until a completed narration attachment exists.
    """
    requested = normalize_checkbox(fields.get(mapping.get("voiceover_requested")))
    if not requested:
        return extract_attachment(fields.get(mapping.get("video_attachment"))), "generated"
    status = _choice_text(fields.get(mapping.get("voiceover_status")))
    voiceover = extract_attachment(fields.get(mapping.get("voiceover_attachment")))
    if status == "已完成" and voiceover:
        return voiceover, "voiceover"
    return None, "waiting_voiceover"


def normalize_photo_claim_scope(raw_value: Any) -> str:
    """账号表“图文领取范围”（沿用店铺池/仅本账号任务）→ store_pool/own_tasks_only。

    空值返回空串：``upsert_account_configs`` 用 COALESCE 保留库内既有值，
    未配置账号读取时按 store_pool（旧店铺池行为）处理。
    """
    text = _choice_text(raw_value).strip()
    if not text:
        return ""
    if "仅本账号" in text or "own" in text.lower():
        return "own_tasks_only"
    if "店铺池" in text or "store" in text.lower() or "沿用" in text:
        return "store_pool"
    return ""


def normalize_photo_expression_mode(raw_value: Any) -> str:
    """账号表“内容表达”（搭配灵感/实用指南）→ STYLE_INSPIRATION/PRACTICAL_GUIDE。"""
    text = _choice_text(raw_value).strip()
    if not text:
        return ""
    if "实用" in text or "指南" in text or "guide" in text.lower():
        return "PRACTICAL_GUIDE"
    if "灵感" in text or "inspiration" in text.lower():
        return "STYLE_INSPIRATION"
    return ""


def build_photo_content_profile(
    *, positioning: str = "", default_theme: str = "", expression_mode: str = "",
    visual_style: str = "", style_image: Any = None,
    supply_strategy: str = "", supply_product_mode: str = "",
    supply_product_codes: Any = None, supply_automation: str = "",
    supply_daily_limit: Any = None, supply_preset: str = "",
    supply_material_scope: Any = None,
) -> str:
    """把账号表的定位相关列收敛成 OPV 消费的 photo_content_profile JSON。

    全部为空时返回空串（账号未配置定位，保持旧领取行为）。风格图片只保存
    附件身份（file_token/name）指纹；生成侧是否消费由 OPV 决定。

    供给策略列同理：automation 等全空时不写 ``photo_supply_policy`` 键，
    已有序列化输出与旧版逐字节一致。
    """
    profile: Dict[str, Any] = {
        "schema_version": "opv-publish-account-profile-v1",
    }
    if str(positioning or "").strip():
        profile["positioning"] = str(positioning).strip()
    if str(default_theme or "").strip():
        profile["default_theme"] = str(default_theme).strip()
    mode = normalize_photo_expression_mode(expression_mode)
    if mode:
        profile["expression_mode"] = mode
    if str(visual_style or "").strip():
        profile["visual_baseline"] = str(visual_style).strip()
    attachment = extract_attachment(style_image)
    if attachment:
        profile["style_image"] = {
            "file_token": str(attachment.get("file_token") or ""),
            "name": str(attachment.get("name") or ""),
        }
    supply_policy = _build_supply_policy(
        strategy=supply_strategy, product_mode=supply_product_mode,
        product_codes=supply_product_codes, automation=supply_automation,
        daily_limit=supply_daily_limit, preset=supply_preset,
        material_scope=supply_material_scope)
    if supply_policy:
        profile["photo_supply_policy"] = supply_policy
    keys = [key for key in profile if key != "schema_version"]
    if not keys:
        return ""
    return json.dumps(profile, ensure_ascii=False, sort_keys=True)


def _build_supply_policy(
    *, strategy: str = "", product_mode: str = "", product_codes: Any = None,
    automation: str = "", daily_limit: Any = None, preset: str = "",
    material_scope: Any = None,
) -> Dict[str, Any]:
    """账号表供给列 → policy dict；automation 为空或“关闭”且无其他配置 → 空dict（不写键）。"""
    automation_text = str(automation or "").strip()
    codes = [
        str(code).strip() for code in (
            product_codes if isinstance(product_codes, (list, tuple))
            else str(product_codes or "").replace("，", ",").replace("\n", ",").split(",")
        ) if str(code).strip()
    ]
    scope = [
        str(item).strip() for item in (
            material_scope if isinstance(material_scope, (list, tuple))
            else str(material_scope or "").replace("，", ",").replace("\n", ",").split(",")
        ) if str(item).strip()
    ]
    try:
        limit = max(int(str(daily_limit).strip() or 0), 0) if daily_limit is not None else 0
    except (TypeError, ValueError):
        limit = 0
    if (not automation_text or automation_text == "关闭") and not preset \
            and not codes and not scope and not limit:
        return {}
    return {
        "content_strategy": str(strategy or "").strip() or "定位优先",
        "product_mode": str(product_mode or "").strip() or "不指定商品",
        "product_codes": codes,
        "automation": automation_text or "关闭",
        "daily_limit": limit,
        "preset": str(preset or "").strip(),
        "material_scope": scope,
    }


def account_photo_claim_scope(account: Any) -> str:
    """读取账号图文领取范围；未配置/读取失败一律回落店铺池（旧行为）。"""
    if account is None:
        return "store_pool"
    try:
        if "photo_claim_scope" not in account.keys():
            return "store_pool"
        return str(account["photo_claim_scope"] or "").strip() or "store_pool"
    except (AttributeError, TypeError):
        return "store_pool"


def filter_candidates_for_account(
    candidates: List[Any], account_id: str, account: Any,
) -> Tuple[List[Any], Dict[str, int]]:
    """目标账号领取隔离（只影响候选过滤，不改任何渠道/配额规则）。

    - 候选带 ``target_publish_account_id`` 时只能被该账号领取（等待，不转号）；
    - 账号 ``photo_claim_scope=own_tasks_only`` 时不领取未绑定的原生图文公共池
      候选（视频与未配置账号不受影响）。
    """
    scope = account_photo_claim_scope(account)
    kept: List[Any] = []
    stats = {"targeted_for_other": 0, "unbound_photo_skipped": 0}
    for candidate in candidates:
        target = str(getattr(candidate, "target_publish_account_id", "") or "").strip()
        if target and target != str(account_id or "").strip():
            stats["targeted_for_other"] += 1
            continue
        is_photo = str(getattr(candidate, "content_type", "video") or "video") == "photo"
        if (not target and is_photo and scope == "own_tasks_only"):
            stats["unbound_photo_skipped"] += 1
            continue
        kept.append(candidate)
    return kept, stats


def sync_accounts(records: Iterable[Any], mapping: Dict[str, Optional[str]], db: AutoPublishDB) -> int:
    account_rows: Dict[str, List[Dict[str, Any]]] = {}
    binding_rows: Dict[str, List[Dict[str, Any]]] = {}
    for record in records:
        fields = record.fields
        account_id = normalize_text(fields.get(mapping.get("account_id")))
        account_name = normalize_text(fields.get(mapping.get("account_name")))
        store_id = normalize_text(fields.get(mapping.get("store_id")))
        if not account_id or not store_id:
            continue
        channel = normalize_publish_channel(fields.get(mapping.get("publish_channel")))
        row = {
                "publish_channel": channel,
                "content_scope": normalize_content_scope(
                    fields.get(mapping.get("content_scope"))
                ),
                "publish_time_1": normalize_text(fields.get(mapping.get("publish_time_1"))),
                "publish_time_2": normalize_text(fields.get(mapping.get("publish_time_2"))),
                "publish_time_3": normalize_text(fields.get(mapping.get("publish_time_3"))),
                "source_record_id": getattr(record, "record_id", "") or "",
                "account_name": account_name or account_id,
                "store_id": store_id,
                "account_status": normalize_text(fields.get(mapping.get("account_status"))) or "暂停",
                "nurture_enabled": normalize_checkbox(fields.get(mapping.get("nurture_enabled"))),
                "nurture_daily_count": max(normalize_int(fields.get(mapping.get("nurture_daily_count")), 2), 0),
                "nurture_only": normalize_checkbox(fields.get(mapping.get("nurture_only"))),
                "initialization_enabled": normalize_checkbox(fields.get(mapping.get("initialization_enabled"))),
                "publish_profile_id": normalize_text(fields.get(mapping.get("publish_profile_id"))),
                "provider_connection_uid": normalize_text(fields.get(mapping.get("provider_connection_uid"))),
                "account_timezone": normalize_text(fields.get(mapping.get("account_timezone"))),
                "delivery_mode": normalize_creatok_delivery_mode(fields.get(mapping.get("delivery_mode"))),
                "provider_health": normalize_creatok_health(fields.get(mapping.get("provider_health"))),
                "provider_checked_at": normalize_text(fields.get(mapping.get("provider_checked_at"))),
                # 内容定位（同一账号多通道行共享）：行值非空即候选，聚合时取第一个
                # 非空值；全部为空＝未配置定位，保持店铺池旧行为。
                "photo_content_profile": build_photo_content_profile(
                    positioning=normalize_text(fields.get(mapping.get("positioning"))),
                    default_theme=normalize_text(fields.get(mapping.get("default_theme"))),
                    expression_mode=fields.get(mapping.get("expression_mode")),
                    visual_style=normalize_text(fields.get(mapping.get("visual_style"))),
                    style_image=fields.get(mapping.get("style_image")),
                    supply_strategy=normalize_text(fields.get(mapping.get("supply_strategy"))),
                    supply_product_mode=normalize_text(fields.get(mapping.get("supply_product_mode"))),
                    supply_product_codes=fields.get(mapping.get("supply_product_codes")),
                    supply_automation=normalize_text(fields.get(mapping.get("supply_automation"))),
                    supply_daily_limit=fields.get(mapping.get("supply_daily_limit")),
                    supply_preset=normalize_text(fields.get(mapping.get("supply_preset"))),
                    supply_material_scope=fields.get(mapping.get("supply_material_scope")),
                ),
                "photo_claim_scope": normalize_photo_claim_scope(
                    fields.get(mapping.get("photo_claim_scope"))),
        }
        binding_rows.setdefault(account_id, []).append(row)
        account_rows.setdefault(account_id, []).append(row)

    configs: List[AccountConfig] = []
    for account_id, rows in account_rows.items():
        bindings = channel_bindings_for_rows(rows)
        channels = sorted({row["publish_channel"] for row in rows
                           if row["publish_channel"] not in {"", "手动", "暂停"}})
        stores = {row["store_id"] for row in rows if row["store_id"]}
        conflict = len(stores) != 1 or (len(channels) >= 2 and not bindings)
        organic_channel = next(
            (binding["publish_channel"] for binding in bindings
             if binding["content_scope"] == "organic"), "",
        )
        preferred = next(
            (row for row in rows if row["publish_channel"] == organic_channel),
            rows[0],
        )
        provider_row = next(
            (row for row in rows if row["publish_channel"] == "CreatOK"),
            preferred,
        )
        # 内容定位跨通道共享：取第一个非空 profile / scope。冲突（两个非空且
        # 不同）时保留第一个非空值——发布仍可用，OPV 侧解析时会给出唯一来源。
        photo_profile = next(
            (row["photo_content_profile"] for row in rows
             if row.get("photo_content_profile")),
            "",
        )
        photo_scope = next(
            (row["photo_claim_scope"] for row in rows if row.get("photo_claim_scope")),
            "",
        )
        # Common fields are selected deterministically; CreatOK connection and
        # nurture settings belong to the organic binding on dual-channel accounts.
        configs.append(AccountConfig(
            account_id=account_id,
            account_name=next((row["account_name"] for row in rows if row["account_name"]), account_id),
            store_id=next(iter(stores)) if len(stores) == 1 else preferred["store_id"],
            account_status="暂停" if conflict else (
                "可用" if any(row["account_status"] == "可用" for row in rows) else preferred["account_status"]
            ),
            publish_channel=preferred["publish_channel"],
            publish_time_1=preferred["publish_time_1"],
            publish_time_2=preferred["publish_time_2"],
            publish_time_3=preferred["publish_time_3"],
            nurture_enabled=preferred["nurture_enabled"],
            nurture_daily_count=preferred["nurture_daily_count"],
            nurture_only=preferred["nurture_only"],
            initialization_enabled=preferred["initialization_enabled"],
            publish_profile_id=provider_row["publish_profile_id"] or preferred["publish_profile_id"],
            provider_connection_uid=provider_row["provider_connection_uid"],
            account_timezone=provider_row["account_timezone"] or preferred["account_timezone"],
            delivery_mode=provider_row["delivery_mode"],
            provider_health=provider_row["provider_health"],
            provider_checked_at=provider_row["provider_checked_at"],
            photo_content_profile_json=photo_profile,
            photo_claim_scope=photo_scope,
        ))
    written = db.upsert_account_configs(configs)
    _sync_channel_bindings(db, binding_rows)
    return written


def channel_bindings_for_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Resolve one account's bindings, returning [] for an unsafe conflict."""
    channels = {row["publish_channel"] for row in rows if row["publish_channel"]}
    distinct = sorted(c for c in channels if c not in ("手动", "暂停"))
    bindings: List[Dict[str, Any]] = []
    if len(distinct) >= 2:
        scopes = {row["publish_channel"]: row["content_scope"] for row in rows}
        explicit = {channel: scope for channel, scope in scopes.items()
                    if scope in ("shoppable", "organic")}
        if len(explicit) == 1:
            channel, scope = next(iter(explicit.items()))
            complement = "organic" if scope == "shoppable" else "shoppable"
            for other in distinct:
                if other != channel:
                    scopes[other] = complement
            explicit = scopes
        if (len(distinct) == 2 and len(explicit) == len(distinct)
                and len({explicit[c] for c in distinct}) == len(distinct)):
            bindings = [{
                "publish_channel": channel,
                "content_scope": explicit[channel],
                "publish_time_1": row_time(rows, channel, 1),
                "publish_time_2": row_time(rows, channel, 2),
                "publish_time_3": row_time(rows, channel, 3),
                "source_record_id": row_record(rows, channel),
            } for channel in distinct]
            windows = [value for binding in bindings for value in (
                binding["publish_time_1"], binding["publish_time_2"], binding["publish_time_3"]
            ) if value]
            if len(windows) != len(set(windows)):
                return []
    elif len(distinct) == 1:
        only = distinct[0]
        only_scope = next((row["content_scope"] for row in rows
                           if row["publish_channel"] == only
                           and row["content_scope"] in ("shoppable", "organic")), "all")
        bindings = [{
            "publish_channel": only, "content_scope": only_scope,
            "publish_time_1": row_time(rows, only, 1),
            "publish_time_2": row_time(rows, only, 2),
            "publish_time_3": row_time(rows, only, 3),
            "source_record_id": row_record(rows, only),
        }]
    return bindings


def _sync_channel_bindings(db: AutoPublishDB, binding_rows: Dict[str, List[Dict[str, Any]]]) -> None:
    """Persist per-account channel bindings with the dual-channel constraint.

    规则（用户 2026-09-07 决策）：账号选了两个不同渠道时，两个渠道必须发不同
    内容类型（一个带货、一个不带货）。分工由表格「内容类型限定」列决定，不按
    渠道硬编码；只填一边时自动补全另一边。冲突时账号主记录会暂停，绝不回落
    到某个默认渠道。
    """
    for account_id, rows in binding_rows.items():
        db.replace_account_channel_bindings(account_id, channel_bindings_for_rows(rows))


def row_time(rows: List[Dict[str, Any]], channel: str, index: int) -> str:
    for row in rows:
        if row["publish_channel"] == channel:
            return str(row.get(f"publish_time_{index}") or "")
    return ""


def row_record(rows: List[Dict[str, Any]], channel: str) -> str:
    for row in rows:
        if row["publish_channel"] == channel:
            return str(row.get("source_record_id") or "")
    return ""


def _ensure_download_dir(download_dir: Optional[Path]) -> Path:
    path = Path(download_dir) if download_dir else default_video_dir()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _download_from_url(url: str) -> bytes:
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    return response.content


def _oss_source_enabled() -> bool:
    return str(os.environ.get("PUBLISH_OSS_SOURCE_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "on"}


def _download_from_oss(*, object_id: str = "", object_key: str = "") -> bytes:
    root = Path(__file__).resolve().parents[3] / "auto_mixcut"
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from auto_mixcut.core.bootstrap import build_context
    from auto_mixcut.core.storage_paths import resolve_oss_object_path

    ctx = build_context()
    object_id = str(object_id or "").strip().split(" / ", 1)[0]
    object_key = str(object_key or "").strip().split(" / ", 1)[0]
    if object_id:
        resolved = resolve_oss_object_path(ctx, object_id, "publisher")
        if not resolved.success:
            message = resolved.error.message if resolved.error else "OSS object resolve failed"
            raise RuntimeError(message)
        content = Path(str(resolved.data["path"])).read_bytes()
    elif object_key:
        dest = default_video_dir() / "oss_cache" / Path(object_key).name
        downloaded = ctx.oss.download(object_key, dest)
        if not downloaded.success:
            message = downloaded.error.message if downloaded.error else "OSS download failed"
            raise RuntimeError(message)
        content = dest.read_bytes()
    else:
        raise ValueError("missing OSS object source")
    if not content:
        raise ValueError("downloaded OSS video is empty")
    return content


def sync_run_manager_pool_metadata(db, fields, mapping, *, record_id: str) -> Optional[dict]:
    """Recover/refresh explicit WSR metadata without invoking a title model.

    Do not derive platform IDs from the analytical global product reference.
    Existing frozen execution records are left untouched by registration.
    """
    def value(name):
        field_name = mapping.get(name)
        return _choice_text(fields.get(field_name)) if field_name else ""

    script_id = value("script_id")
    source = value("script_source")
    if not script_id.startswith("wsr_") or source != "成功脚本复刻":
        return None
    old = db.get_script_metadata(script_id)
    def current(name, old_name=None):
        incoming = value(name)
        return incoming or (str(old[old_name or name] or "") if old and (old_name or name) in old.keys() else "")

    purpose = current("publish_purpose") or "带货"
    prompt = current("prompt", "script_text")
    target_country = current("target_country")
    # The run-manager currently exposes "西班牙语" rather than a country for
    # the Mexico wig flow.  Do not treat generic Spanish as Mexico: infer it
    # only when the generated script itself explicitly declares Mexico.
    normalized_country = target_country.strip().lower()
    if normalized_country in {"西班牙语", "spanish", "es"} and any(
        marker in prompt.lower() for marker in ("墨西哥", "mexico", "méxico")
    ):
        target_country = "墨西哥"
    store_id = current("store_id")
    if not store_id and target_country == "墨西哥":
        eligible_stores = {
            str(row["store_id"] or "").strip()
            for row in db.list_account_configs(publish_channel="NeoBund")
            if str(row["account_status"] or "") == "可用"
            and bool(row["organic_capable"])
            and infer_country_from_store_id(str(row["store_id"] or "")) == "墨西哥"
        }
        if len(eligible_stores) == 1:
            store_id = next(iter(eligible_stores))
    voiceover_requested = normalize_checkbox(fields.get(mapping.get("voiceover_requested")))
    voiceover_status = _choice_text(fields.get(mapping.get("voiceover_status")))
    audio_mode = value("audio_mode")
    if not audio_mode:
        audio_mode = "clean_voice" if voiceover_requested and voiceover_status == "已完成" else (
            "generated_nonvoice" if not voiceover_requested else "unknown"
        )
    fallback_title = current("short_video_title")
    if not fallback_title and target_country and store_id:
        fallback_title = localized_template_title(SimpleNamespace(
            canonical_script_key=script_id, script_id=script_id,
            target_country=target_country, script_source=source,
            publish_purpose=purpose, content_branch=current("content_branch") or "SUCCESS_SCRIPT_REPLICATION",
        ))
    return register_script_pool_metadata(
        db, script_id=script_id,
        source_record_id=str(old["source_record_id"]) if old else record_id,
        prompt=prompt,
        product_id=current("canonical_product_id", "product_id"),
        platform_product_id=value("product_id") if mapping.get("product_id") else None,
        store_id=store_id, publish_purpose=purpose,
        cart_enabled=current("cart_enabled"), script_source=source,
        content_branch=current("content_branch") or "SUCCESS_SCRIPT_REPLICATION",
        target_country=target_country, target_language=value("target_language"),
        short_video_title=fallback_title, parent_slot=current("parent_slot"),
        direction_label=current("direction_label"), variant_strength=current("variant_strength"),
        product_type=current("product_type"),
        content_family_key=str(old["content_family_key"] or "") if old else "",
        task_name=value("task_name") or (str(old["task_no"] or "") if old else script_id),
        audio_mode=audio_mode,
    )


def sync_videos(
    records: Sequence[Any],
    mapping: Dict[str, Optional[str]],
    db: AutoPublishDB,
    *,
    download_dir: Optional[Path],
    client: Any,
) -> Dict[str, int]:
    stats = {
        "synced": 0,
        "skipped": 0,
        "download_failed": 0,
        "titles_updated": 0,
        "waiting_voiceover": 0,
        "metadata_created": 0,
        "metadata_invalid": 0,
    }
    base_dir = _ensure_download_dir(download_dir)

    for record in records:
        fields = record.fields
        canonical_script_key = normalize_text(fields.get(mapping.get("canonical_script_key")))
        script_id = normalize_text(fields.get(mapping.get("script_id")))
        if not canonical_script_key and not script_id:
            stats["skipped"] += 1
            continue
        run_status = normalize_text(fields.get(mapping.get("run_video_status")))
        publish_enabled = normalize_checkbox(fields.get(mapping.get("publish_enabled")))
        if not publish_enabled:
            stats["skipped"] += 1
            continue
        try:
            pool_result = sync_run_manager_pool_metadata(db, fields, mapping, record_id=record.record_id)
            if pool_result:
                canonical_script_key = pool_result["canonical_script_key"]
            if pool_result and pool_result["status"] == "created":
                stats["metadata_created"] += 1
        except ValueError:
            stats["metadata_invalid"] += 1
            stats["skipped"] += 1
            continue
        metadata = db.get_script_metadata(canonical_script_key or script_id)
        if metadata is None:
            seeding_metadata = build_run_manager_seeding_metadata(
                fields,
                mapping,
                record_id=record.record_id,
            )
            if seeding_metadata is not None:
                db.upsert_script_metadata([seeding_metadata])
                stats["metadata_created"] += 1
                metadata = db.get_script_metadata(seeding_metadata.canonical_script_key)
        if metadata is None:
            stats["skipped"] += 1
            continue
        resolved_canonical_key = normalize_text(metadata["canonical_script_key"])
        resolved_script_id = normalize_text(metadata["script_id"]) or script_id
        existing_asset = db.get_video_asset(resolved_canonical_key or resolved_script_id)
        if existing_asset is not None:
            existing_publish_status = normalize_text(existing_asset["publish_status"])
            existing_download_status = normalize_text(existing_asset["download_status"])
            if existing_publish_status == "已发布" and existing_download_status == "已清理":
                stats["skipped"] += 1
                continue

        attachment, video_variant = select_publish_attachment(fields, mapping)
        if video_variant == "waiting_voiceover":
            db.mark_video_waiting_voiceover(resolved_canonical_key or resolved_script_id)
            stats["waiting_voiceover"] += 1
            stats["skipped"] += 1
            continue
        video_link = normalize_text(fields.get(mapping.get("video_link")))
        oss_object_id = normalize_text(fields.get(mapping.get("oss_object_id")))
        oss_path = normalize_text(fields.get(mapping.get("oss_path")))
        local_file_path = normalize_text(fields.get(mapping.get("local_file_path")))
        if video_variant != "voiceover" and local_file_path and Path(local_file_path).exists():
            resolved_local_path = local_file_path
        else:
            if video_variant == "voiceover":
                source_token = str((attachment or {}).get("file_token") or "voiceover")
                source_suffix = hashlib.sha256(source_token.encode("utf-8")).hexdigest()[:10]
                resolved_local_path = str(base_dir / f"{resolved_script_id}_voiceover_{source_suffix}.mp4")
            else:
                resolved_local_path = str(base_dir / f"{resolved_script_id}.mp4")
            if not Path(resolved_local_path).exists():
                try:
                    if video_variant == "voiceover" and attachment:
                        content, _, _, _ = client.download_attachment_bytes(attachment)
                    elif _oss_source_enabled() and (oss_object_id or oss_path):
                        content = _download_from_oss(object_id=oss_object_id, object_key=oss_path)
                    elif attachment:
                        content, _, _, _ = client.download_attachment_bytes(attachment)
                    elif video_link:
                        content = _download_from_url(video_link)
                    else:
                        stats["download_failed"] += 1
                        continue
                    Path(resolved_local_path).write_bytes(content)
                except Exception:
                    stats["download_failed"] += 1
                    continue

        if video_variant == "voiceover" and attachment:
            source_value = attachment.get("file_token", "")
            source_type = "voiceover_attachment"
        elif _oss_source_enabled() and (oss_object_id or oss_path):
            source_value = oss_object_id or oss_path
            source_type = "oss_object"
        elif attachment:
            source_value = attachment.get("file_token", "")
            source_type = "attachment"
        else:
            source_value = video_link
            source_type = "link"
        requested_publish_status = normalize_text(fields.get(mapping.get("publish_status")))
        if video_variant == "voiceover" and requested_publish_status == "等待口播":
            requested_publish_status = "待排期"
        db.upsert_video_asset(
            canonical_script_key=resolved_canonical_key,
            script_id=resolved_script_id,
            run_manager_record_id=record.record_id,
            video_source_type=source_type,
            video_source_value=str(source_value or ""),
            local_file_path=resolved_local_path,
            download_status="下载成功",
            run_video_status=run_status,
            publish_status=requested_publish_status or "待排期",
        )
        manual_title = sanitize_title(fields.get(mapping.get("short_video_title")))
        if pool_result and (pool_result.get("status") == "frozen" or not is_title_compatible_with_country(manual_title, str(metadata["target_country"] or ""))):
            manual_title = ""
        if manual_title:
            stats["titles_updated"] += db.update_short_video_title(
                canonical_script_key=resolved_canonical_key,
                short_video_title=manual_title,
                title_source="run_manager_manual",
            )
        stats["synced"] += 1
    return stats


@dataclass(frozen=True)
class SchedulingStats:
    slots_created: int = 0
    slots_examined: int = 0
    scheduled: int = 0
    skipped: int = 0
    create_failed: int = 0
    blocked_by_rules: int = 0
    retryable_create_failed: int = 0
    initialization_accounts: int = 0
    initialization_pending: int = 0
    initialization_completed: int = 0
    initialization_required_items: int = 0
    initialization_scheduled_items: int = 0
    initialization_ready_pool_items: int = 0
    initialization_content_gap: int = 0


DEFAULT_SCHEDULE_WINDOW_HOURS = 48
MIN_SUBMIT_LEAD_MINUTES = 30
ACCOUNT_INITIALIZATION_TARGET_COUNT = 3


def _is_auth_error(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    return bool(
        getattr(exc, "auth_failed", False)
        or getattr(response, "status_code", None) == 401
        or any(marker in str(exc).lower() for marker in (
            "401", "invalid_token", "token_kicked_by_login", "authentication is required",
        ))
    )


def _account_adapter(
    publisher: BasePublishAdapter, account_id: str, channel: str = ""
) -> BasePublishAdapter:
    wanted = str(channel or "").strip()
    if wanted:
        for_channel = getattr(publisher, "_adapter_for_channel", None)
        if callable(for_channel):
            return for_channel(wanted)
    route = getattr(publisher, "_adapter_for_account", None)
    return route(account_id) if callable(route) else publisher


def _is_retryable_create_error(error_message: str) -> bool:
    text = str(error_message or "").lower()
    return any(
        marker in text
        for marker in (
            "balance not enough",
            "too many requests",
            "rate limit",
            "timeout",
            "temporarily",
            "temporary",
            "connection",
            "network",
        )
    )


def _validate_opv_upload(candidate: Any, *, account_id: str = "") -> None:
    if not str(candidate.canonical_script_key or "").startswith("opv:"):
        return
    context = json.loads(candidate.script_text or "{}")
    # Load the stdlib-only contract module without importing another app's
    # generic `services` package or altering global sys.path.
    path = Path(__file__).resolve().parents[3] / "packages/organic_photo_video/services/release_gate.py"
    spec = importlib.util.spec_from_file_location("opv_release_gate", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    is_photo = getattr(candidate, "content_type", "video") == "photo"
    if is_photo:
        validator = getattr(module, "validate_photo_upload", None)
        if not callable(validator):
            raise OpvReleaseVerificationError("照片 release 核验尚未接入")
        validator(context, media_paths=list(candidate.media_paths),
                  script_id=candidate.script_id, title=candidate.short_video_title,
                  account_id=account_id)
    else:
        module.validate_upload(context, video_path=candidate.publish_video_value,
                               script_id=candidate.script_id, title=candidate.short_video_title,
                               account_id=account_id)
    # 队列侧目标账号与候选冻结目标必须一致（列与 context JSON 双通道互证）。
    queue_target = str(getattr(candidate, "target_publish_account_id", "") or "")
    context_target = str(context.get("target_publish_account_id") or "")
    if context_target and queue_target and context_target != queue_target:
        raise OpvReleaseVerificationError(
            f"队列目标账号（{queue_target}）与冻结任务目标（{context_target}）不一致"
        )
    if context.get("release_manifest"):
        if is_photo:
            _verify_live_opv_release(candidate.script_id, context["release_manifest"]["manifest_sha256"],
                                     media_kind="native_photo")
        else:
            _verify_live_opv_release(candidate.script_id, context["release_manifest"]["manifest_sha256"])


class OpvReleaseVerificationError(ValueError):
    """Read-only preflight failed; no upload/commit has been attempted."""

    submission_not_sent = True

    def __init__(self, message: str, *, retryable: bool = False):
        super().__init__(message + "；未发送发布请求")
        self.retryable = retryable


def _verify_live_opv_release(task_id: str, manifest_sha256: str, *, media_kind: str = "video") -> None:
    script = Path(__file__).resolve().parents[3] / "packages/organic_photo_video/scripts/check_release_for_upload.py"
    command = [sys.executable, str(script), "--task-id", task_id, "--manifest-sha256", manifest_sha256]
    if media_kind != "video":
        command.extend(["--media-kind", media_kind])
    try:
        result = subprocess.run(
            command,
            capture_output=True, text=True, timeout=20, check=False,
        )
    except subprocess.TimeoutExpired as exc:
        # This is before remote upload/commit: fail closed, not an ambiguous send.
        raise OpvReleaseVerificationError("上传前 release 核验超时（20秒）", retryable=True) from exc
    except OSError as exc:
        raise OpvReleaseVerificationError("上传前 release 核验进程无法启动") from exc
    if result.returncode == 0 and result.stdout.strip() == "release_verified":
        return
    try:
        payload = json.loads(result.stdout)
    except (ValueError, TypeError):
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    messages = {
        "release_mismatch": "当前验收版本或成片不符合冻结发布清单",
        "dependency_unavailable": "核验数据库/网络暂时不可用",
        "verification_error": "核验运行环境或配置异常",
    }
    code = payload.get("code")
    if code not in messages:
        code = "verification_error"
    # Do not persist arbitrary child output: tracebacks may contain DB credentials.
    details = f"code={code}, exit={result.returncode}"
    for field in ("stage", "error_type", "reason"):
        value = payload.get(field)
        if isinstance(value, str) and 0 < len(value) <= 80 and value.replace("_", "").isalnum():
            details += f", {field}={value}"
    errno = payload.get("errno")
    if type(errno) is int:
        details += f", errno={errno}"
    stderr = getattr(result, "stderr", "")
    if isinstance(stderr, str) and stderr:
        details += ", stderr_sha256=" + hashlib.sha256(stderr.encode()).hexdigest()[:16]
    raise OpvReleaseVerificationError(
        f"上传前 release 核验失败：{messages[code]}（{details}）",
        retryable=(code == "dependency_unavailable" and payload.get("retryable") is True),
    )


def reconcile_submissions(db: AutoPublishDB, publisher: BasePublishAdapter) -> Dict[str, int]:
    """Never resubmit an unresolved request, including crashes after remote commit."""
    stats = {"reconciled": 0, "unresolved": 0}
    for row in db.list_unresolved_submissions():
        try:
            context = json.loads(row["submission_context_json"] or "{}")
            if not context:
                raise RuntimeError("历史提交缺少冻结请求标识，需要人工核对")
            resolve = getattr(publisher, "_adapter_for_account", None)
            adapter = resolve(str(row["account_id"])) if callable(resolve) else publisher
            adapter_type = f"{type(adapter).__module__}.{type(adapter).__name__}"
            if context.get("adapter_type") and context["adapter_type"] != adapter_type:
                raise RuntimeError("发布通道已变化，必须使用原通道对账，不得自动重发")
            lookup = getattr(adapter, "reconcile_scheduled_task", None)
            # A returned remote ID is already confirmation of creation. If the
            # local assignment failed, repair it without another POST or lookup.
            task_id = str(context.get("confirmed_task_id") or "")
            if not task_id:
                task_id = lookup(context) if callable(lookup) else ""
            if not task_id:
                raise RuntimeError("平台尚未查到唯一匹配任务；未查到不等于未创建")
            assigned = db.assign_slot(
                slot_id=int(row["slot_id"]), canonical_script_key=str(row["canonical_script_key"]),
                script_id=str(row["script_id"]), publish_task_id=task_id,
                account_id=str(row["account_id"]), account_name=str(row["account_name"]),
                planned_publish_at=datetime.strptime(str(row["scheduled_for"]), "%Y-%m-%d %H:%M:%S"),
                bgm_json=str(context.get("bgm_json") or ""),
            )
            if not assigned:
                raise RuntimeError("远端任务已找到，但本地占位冲突，需要核对；禁止重发")
            stats["reconciled"] += 1
        except Exception as exc:
            stats["unresolved"] += 1
            db.mark_submission_ambiguous(int(row["slot_id"]), str(exc))
    return stats


def schedule_slots(
    db: AutoPublishDB,
    publisher: BasePublishAdapter,
    now: Optional[datetime] = None,
    *,
    window_hours: int = DEFAULT_SCHEDULE_WINDOW_HOURS,
    canonical_keys: Optional[set[str]] = None,
) -> SchedulingStats:
    current_time = now or datetime.now()
    if not isinstance(publisher, DryRunPublishAdapter):
        reconcile_submissions(db, publisher)
    if not isinstance(publisher, DryRunPublishAdapter):
        db.recycle_dryrun_schedules()
    slots_created = db.generate_future_slots(current_time, window_hours=window_hours)
    pending_slots = db.list_pending_slots(current_time, window_hours=window_hours)
    initialization_progress = {
        str(item["account_id"]): item
        for item in db.list_initializing_accounts(
            target_count=ACCOUNT_INITIALIZATION_TARGET_COUNT,
            include_completed=True,
        )
    }

    def pending_slot_priority(slot: Any) -> Tuple[int, int, int, str, str]:
        account_id = str(slot["account_id"] or "")
        progress = initialization_progress.get(account_id)
        needs_content = bool(
            progress
            and progress["status"] != "COMPLETED"
            and int(progress["remaining_count"] or 0) > 0
        )
        return (
            0 if needs_content else 1,
            int(progress["published_count"] or 0) if needs_content else 0,
            int(progress["scheduled_count"] or 0) if needs_content else 0,
            str(slot["scheduled_for"] or ""),
            account_id,
        )

    pending_slots = sorted(pending_slots, key=pending_slot_priority)
    binding_scopes: Dict[str, Dict[str, str]] = {}
    for binding in db.list_account_channel_bindings():
        binding_scopes.setdefault(
            str(binding["account_id"] or ""), {}
        )[str(binding["publish_channel"] or "")] = str(
            binding["content_scope"] or "all"
        )

    scheduled = 0
    skipped = 0
    create_failed = 0
    blocked_by_rules = 0
    retryable_create_failed = 0
    disabled_accounts: set[str] = set()
    auth_blocked_adapters: set[int] = set()
    for slot in pending_slots:
        account_id = str(slot["account_id"] or "")
        account_name = str(slot["account_name"] or "")
        if account_id in disabled_accounts:
            skipped += 1
            continue
        target_time = datetime.strptime(str(slot["scheduled_for"]), "%Y-%m-%d %H:%M:%S")
        if target_time < current_time + timedelta(minutes=MIN_SUBMIT_LEAD_MINUTES):
            skipped += 1
            db.mark_slot_pending_reason(
                int(slot["slot_id"]), reason=f"距发布时间不足 {MIN_SUBMIT_LEAD_MINUTES} 分钟，内容顺延至后续槽位",
            )
            continue
        slot_channel = str(slot["publish_channel_used"] or "").strip()
        try:
            adapter = _account_adapter(publisher, account_id, channel=slot_channel)
        except Exception as exc:
            skipped += 1
            db.mark_slot_pending_reason(int(slot["slot_id"]), reason=f"发布通道路由失败：{exc}")
            continue
        if id(adapter) in auth_blocked_adapters:
            skipped += 1
            db.mark_slot_pending_reason(int(slot["slot_id"]), reason="发布通道认证失效，本轮暂停创建，等待重新登录")
            continue
        candidates = db.list_ready_candidates(str(slot["store_id"] or ""))
        if canonical_keys is not None:
            candidates = [
                candidate for candidate in candidates
                if candidate.canonical_script_key in canonical_keys
            ]
        account = db.get_account_config(account_id)
        candidates = [
            candidate for candidate in candidates
            if account_can_publish_candidate(account, candidate, slot_channel)
        ]
        # 目标账号领取隔离（2026-09-15）：定向候选只归目标账号；配置了
        # 「仅本账号任务」的重点图文账号不再领取未绑定的公共图文池。被过滤
        # 的候选保持待排期等待自己的账号，不转号、不计失败。
        candidates, claim_filter_stats = filter_candidates_for_account(
            candidates, account_id, account)
        slot_scope = binding_scopes.get(account_id, {}).get(slot_channel, "all")
        if slot_scope in ("shoppable", "organic"):
            def _matches_scope(candidate: Any) -> bool:
                organic = is_non_shoppable_candidate(candidate)
                if slot_scope == "shoppable":
                    # shop photo is not enabled yet: photo stays organic-only
                    return (not organic) and getattr(candidate, "content_type", "video") != "photo"
                return organic
            candidates = [candidate for candidate in candidates if _matches_scope(candidate)]
        progress = initialization_progress.get(account_id)
        initialization_enabled = bool(
            account and int(account["initialization_enabled"] or 0)
        )
        initialization_incomplete = bool(
            initialization_enabled
            and progress
            and progress["status"] != "COMPLETED"
        )
        if initialization_incomplete:
            if int(progress["remaining_count"] or 0) <= 0:
                skipped += 1
                db.mark_slot_pending_reason(
                    int(slot["slot_id"]),
                    reason="账号初始化内容已排满，等待实际发布完成",
                )
                continue
            if db.count_scheduled_initialization_for_account_day(account_id, target_time) >= 1:
                skipped += 1
                db.mark_slot_pending_reason(
                    int(slot["slot_id"]),
                    reason="账号初始化当日已达到1条",
                )
                continue
            candidates = [candidate for candidate in candidates if is_opv_initialization_candidate(candidate)]
            used_recipe_ids = db.list_account_initialization_recipe_ids(account_id)
            candidates = sorted(
                candidates,
                key=lambda candidate: (
                    1 if candidate.recipe_id and candidate.recipe_id in used_recipe_ids else 0,
                    candidate.script_id,
                    candidate.canonical_script_key,
                ),
            )
            if not candidates:
                skipped += 1
                blocked_by_rules += 1
                db.mark_slot_pending_reason(
                    int(slot["slot_id"]),
                    reason="初始化内容池不足，等待补充图文养号成片",
                )
                continue
        nurture_enabled = bool(account and int(account["nurture_enabled"] or 0))
        nurture_quota = int(account["nurture_daily_count"] or 2) if account else 0
        nurture_only = bool(account and int(account["nurture_only"] or 0))
        nurture_count = (
            db.count_scheduled_nurture_for_account_day(str(slot["account_id"] or ""), target_time)
            if nurture_enabled and nurture_quota > 0
            else 0
        )
        prefer_nurture = nurture_only or (nurture_enabled and nurture_count < nurture_quota)
        has_nurture_candidate = any(is_nurture_candidate(candidate) for candidate in candidates)
        # 每日养号条数是硬性封顶：当日已排/已发的养号内容达到条数后，
        # 本账号当天不再分配养号内容；仅养号账号的后续槽位留待明日。
        # 账号初始化期除外（初始化内容走独立的每日 1 条限制）。
        nurture_quota_reached = (
            nurture_enabled
            and nurture_quota > 0
            and nurture_count >= nurture_quota
            and not initialization_incomplete
        )
        if initialization_incomplete:
            prefer_nurture = True
            has_nurture_candidate = bool(candidates)
        elif nurture_quota_reached:
            candidates = [candidate for candidate in candidates if not is_nurture_candidate(candidate)]
            prefer_nurture = False
            has_nurture_candidate = False
        elif not nurture_enabled and not nurture_only and slot_scope != "organic":
            # An organic-scoped channel binding is the operator's explicit
            # organic intent; the legacy nurture toggle must not drain the
            # pool of an organic-only channel.
            candidates = [candidate for candidate in candidates if not is_nurture_candidate(candidate)]
        elif nurture_only:
            candidates = [candidate for candidate in candidates if is_nurture_candidate(candidate)]
        elif prefer_nurture:
            candidates = [candidate for candidate in candidates if is_nurture_candidate(candidate)] + [
                candidate for candidate in candidates if not is_nurture_candidate(candidate)
            ]
        else:
            candidates = [candidate for candidate in candidates if not is_nurture_candidate(candidate)] + [
                candidate for candidate in candidates if is_nurture_candidate(candidate)
            ]
        # Diversity is a preference, not an inventory gate. Preserve purpose
        # routing, manual product priority and initialization recipe ordering.
        # Stable sorting retains the existing order when these scores tie.
        candidates = sorted(candidates, key=lambda candidate: (
            0 if is_nurture_candidate(candidate) == prefer_nurture else 1,
            0 if getattr(candidate, "schedule_strategy", "普通") == "优先" else 1,
            1 if initialization_incomplete and candidate.recipe_id in used_recipe_ids else 0,
            db.count_recent_product_for_account(account_id, candidate.product_id, target_time, hours=24),
            int(db.has_recent_family_conflict(str(slot["store_id"] or ""), candidate.content_family_key, target_time, hours=48)),
            int(db.recent_place_conflict(account_id, getattr(candidate, "place", ""), target_time, hours=48)),
        ))
        selected = None
        product_mapping_error = ""
        for candidate in candidates:
            if prefer_nurture and has_nurture_candidate and not is_nurture_candidate(candidate):
                break
            # 提交前双保险：定向候选与本槽账号不一致时绝不占用（防其他入口绕过）。
            candidate_target = str(getattr(candidate, "target_publish_account_id", "") or "").strip()
            if candidate_target and candidate_target != account_id:
                continue
            if db.has_active_script_assignment(candidate.canonical_script_key, exclude_slot_id=int(slot["slot_id"])):
                continue
            if not db.candidate_retry_ready(candidate.canonical_script_key, current_time):
                continue
            try:
                publishing_product_id(candidate)
            except ValueError as exc:
                product_mapping_error = str(exc)
                continue
            if db.recent_place_conflict(account_id, getattr(candidate, "place", ""), target_time, hours=24):
                # 同账号 24 小时内已排/已发同一景点：跳过该候选，
                # 避免信息流中"同景点不同穿搭"被观众当成重复内容。
                continue
            selected = candidate
            break
        if selected is None:
            skipped += 1
            blocked_by_rules += 1
            if initialization_incomplete:
                pending_reason = "初始化内容池不足，等待补充图文养号成片"
            elif claim_filter_stats["targeted_for_other"] and not candidates:
                pending_reason = (
                    "候选均为其他目标账号的绑定内容，本账号不领取；内容等待目标账号自己的槽位"
                )
            elif claim_filter_stats["unbound_photo_skipped"] and not candidates:
                pending_reason = (
                    "本账号图文领取范围为「仅本账号任务」，暂无绑定本账号的图文；"
                    "未绑定公共池内容不领取"
                )
            elif nurture_quota_reached:
                pending_reason = f"今日养号条数已达上限（{nurture_quota}条），等待明日排班"
            else:
                pending_reason = product_mapping_error or "暂未找到符合规则的候选视频，等待后续自动补排"
            db.mark_slot_pending_reason(
                int(slot["slot_id"]),
                reason=pending_reason,
            )
            continue
        music_selection = None
        bgm_json = ""
        publish_video_value = selected.publish_video_value
        publish_channel = normalize_publish_channel(slot_channel or account["publish_channel"])
        music_mode = bgm.resolve_music_mode(selected, publish_channel)
        auto_add_music = music_mode == bgm.MUSIC_MODE_PLATFORM_AUTO
        if music_mode == bgm.MUSIC_MODE_SELECTED:
            try:
                music_selection, bgm_json = bgm.select_platform_bgm(
                    db,
                    publisher,
                    account_id=account_id,
                    candidate=selected,
                    now=current_time,
                )
            except Exception as exc:
                skipped += 1
                retryable_create_failed += 1
                if _is_auth_error(exc):
                    auth_blocked_adapters.add(id(adapter))
                else:
                    db.record_candidate_failure(selected.canonical_script_key, current_time, f"BGM选择失败：{exc}", retryable=not isinstance(exc, ValueError))
                db.mark_slot_pending_reason(
                    int(slot["slot_id"]),
                    reason=f"图文养号 BGM 选择暂时失败，等待重试：{exc}",
                )
                continue
        elif auto_add_music:
            bgm_json = bgm.platform_auto_audit(
                selected, provider=publish_channel, now=current_time,
            )
        elif music_mode == bgm.MUSIC_MODE_LOCAL_MIX:
            try:
                publish_video_value, music_selection, bgm_json = bgm.prepare_local_bgm(
                    db,
                    account_id=account_id,
                    candidate=selected,
                    now=current_time,
                )
            except Exception as exc:
                skipped += 1
                retryable_create_failed += 1
                db.record_candidate_failure(
                    selected.canonical_script_key,
                    current_time,
                    f"BGM本地混音失败：{exc}",
                    retryable=not isinstance(exc, ValueError),
                )
                db.mark_slot_pending_reason(
                    int(slot["slot_id"]),
                    reason=f"CreatOK BGM 本地混音暂时失败，等待重试：{exc}",
                )
                continue
        product_id = publishing_product_id(selected)
        timezone = (
            str(account["account_timezone"] or "").strip()
            if "account_timezone" in account.keys()
            else ""
        )
        photo_context: Dict[str, Any] = {}
        photo_description = ""
        if selected.content_type == "photo":
            photo_context = json.loads(selected.script_text or "{}")
            frozen_copy = ((photo_context.get("release_manifest") or {}).get("copy") or {})
            caption = str(frozen_copy.get("caption") or "").strip()
            hashtags = " ".join(
                str(item).strip() for item in (frozen_copy.get("hashtags") or [])
                if str(item).strip()
            )
            photo_description = " ".join(part for part in (caption, hashtags) if part)
        publish_request = PublishRequest(
            account_id=account_id,
            content_type=selected.content_type,
            commerce_type="shop" if product_id else "organic",
            media_paths=list(selected.media_paths) if selected.content_type == "photo" else [publish_video_value],
            title=selected.short_video_title,
            description=photo_description,
            auto_add_music=auto_add_music,
            publish_at=target_time,
            timezone=timezone,
            script_id=selected.script_id,
            product_id=product_id,
            product_title=selected.product_title,
            ref_video_id=selected.ref_video_id,
            mark_ai=should_mark_ai_for_geelark(selected),
            music_selection=music_selection,
        )
        submission_context = {
            "account_id": account_id, "script_id": selected.script_id,
            "title": selected.short_video_title,
            "description": photo_description,
            "product_id": product_id,
            "scheduled_for": target_time.strftime("%Y-%m-%d %H:%M:%S"),
            "bgm_json": bgm_json,
            "music_mode": music_mode,
            "content_type": publish_request.content_type,
            "publish_channel": publish_channel,
            "media_paths": publish_request.media_paths,
            "target_publish_account_id": str(
                getattr(selected, "target_publish_account_id", "") or ""),
        }
        if selected.content_type == "photo":
            submission_context["release_manifest"] = photo_context["release_manifest"]
        try:
            submission_context["adapter_type"] = f"{type(adapter).__module__}.{type(adapter).__name__}"
            identity = getattr(adapter, "submission_identity", None)
            if callable(identity):
                submission_context.update(identity(account_id=account_id, product_id=submission_context["product_id"]))
            # 渠道专属冻结身份（如 CreatOK 的幂等键/operation_id）必须在 reserve 前落库。
            submission_stamp = getattr(adapter, "build_submission_context", None)
            if callable(submission_stamp):
                submission_context.update(submission_stamp(publish_request))
        except Exception as exc:
            skipped += 1
            if _is_auth_error(exc):
                auth_blocked_adapters.add(id(adapter))
            db.mark_slot_pending_reason(int(slot["slot_id"]), reason=f"发布身份校验失败：{exc}")
            continue
        reserved = db.reserve_slot_for_submission(
            slot_id=int(slot["slot_id"]),
            canonical_script_key=selected.canonical_script_key,
            script_id=selected.script_id,
            account_id=account_id,
            account_name=account_name,
            planned_publish_at=target_time,
            submission_context=submission_context,
        )
        if not reserved:
            skipped += 1
            blocked_by_rules += 1
            continue
        validated_for_upload = False
        try:
            _validate_opv_upload(selected, account_id=account_id)
            validated_for_upload = True
            if selected.content_type == "photo" or publish_channel == "CreatOK":
                task_id = adapter.create_publish_task(publish_request)
            else:
                publish_kwargs = dict(
                    account_id=account_id,
                    video_path=publish_video_value,
                    title=selected.short_video_title,
                    publish_at=target_time,
                    script_id=selected.script_id,
                    product_id=product_id,
                    product_title=selected.product_title,
                    ref_video_id=selected.ref_video_id,
                    mark_ai=should_mark_ai_for_geelark(selected),
                )
                if music_selection is not None:
                    publish_kwargs["music_selection"] = music_selection
                task_id = adapter.create_scheduled_task(**publish_kwargs)
        except Exception as exc:
            create_failed += 1
            skipped += 1
            error_message = str(exc)
            retryable = (exc.retryable if getattr(exc, "submission_not_sent", False)
                         and isinstance(getattr(exc, "retryable", None), bool)
                         else _is_retryable_create_error(error_message))
            if _is_auth_error(exc):
                auth_blocked_adapters.add(id(adapter))
            if (getattr(exc, "submission_ambiguous", False)
                    or (not getattr(exc, "submission_not_sent", False)
                        and isinstance(exc, (TimeoutError, ConnectionError, requests.Timeout, requests.ConnectionError)))
                    or (validated_for_upload and not (
                        isinstance(exc, ValueError) or _is_auth_error(exc)
                        or getattr(exc, "submission_not_sent", False)
                        or 400 <= (getattr(getattr(exc, "response", None), "status_code", 0) or 0) < 500
                        or "balance not enough" in error_message.lower()
                        or "phone env not found" in error_message.lower()
                    ))):
                db.mark_submission_ambiguous(int(slot["slot_id"]), error_message)
                disabled_accounts.add(account_id)
                continue
            db.release_slot_submission_reservation(
                int(slot["slot_id"]),
                reason=f"创建自动发布定时任务失败，已释放本地占位：{error_message}",
            )
            if not _is_auth_error(exc) and "phone env not found" not in error_message.lower():
                db.record_candidate_failure(
                    selected.canonical_script_key, current_time, error_message,
                    retryable=retryable,
                )
            if "phone env not found" in error_message.lower():
                disabled_accounts.add(account_id)
                db.disable_account(account_id, reason=f"发布账号环境不存在，已暂停账号：{error_message}")
            elif _is_auth_error(exc) or retryable:
                retryable_create_failed += 1
                if any(marker in error_message.lower() for marker in ("balance not enough", "too many requests", "rate limit")):
                    disabled_accounts.add(account_id)
                db.mark_slot_pending_reason(int(slot["slot_id"]), reason=f"创建自动发布定时任务暂时失败，等待重试：{error_message}")
            elif getattr(exc, "submission_not_sent", False) is True and not isinstance(exc, ValueError):
                # Read-only preflight failures (release verification, asset
                # preparation) never reached the remote API: permanent slot
                # cancellation on a transient environment error silently
                # swallows future slots (2026-09-09 incident). Park the slot
                # instead; deterministic content problems still exit via the
                # candidate retry cap.
                retryable_create_failed += 1
                db.mark_slot_pending_reason(
                    int(slot["slot_id"]),
                    reason=f"提交前检查暂时失败，等待重试：{error_message}",
                )
            elif getattr(exc, "submission_not_sent", False) is True and isinstance(exc, ValueError):
                # Deterministic preflight rejection (content/contract): the
                # slot can never take this candidate, but a later candidate
                # may pass — keep the slot pending instead of cancelling it.
                db.mark_slot_pending_reason(
                    int(slot["slot_id"]),
                    reason=f"提交前检查未通过，换用其他候选重排：{error_message}",
                )
            else:
                db.cancel_slot(int(slot["slot_id"]), reason=f"创建自动发布定时任务失败：{error_message}")
            continue
        # Persist the returned ID before final assignment. Local writeback
        # failures must leave a recoverable reservation, never a fresh retry.
        try:
            receipt_reader = getattr(adapter, "submission_receipt", None)
            if callable(receipt_reader):
                db.merge_submission_context(
                    int(slot["slot_id"]),
                    receipt_reader(str(submission_context.get("idempotency_key") or "")),
                )
            db.record_confirmed_submission(int(slot["slot_id"]), task_id)
            assigned = db.assign_slot(
                slot_id=int(slot["slot_id"]),
                canonical_script_key=selected.canonical_script_key,
                script_id=selected.script_id,
                publish_task_id=task_id,
                account_id=account_id,
                account_name=account_name,
                planned_publish_at=target_time,
                bgm_json=bgm_json,
            )
        except Exception as exc:
            db.mark_submission_ambiguous(int(slot["slot_id"]), f"远端已创建 {task_id}，本地落账失败：{exc}")
            disabled_accounts.add(account_id)
            skipped += 1
            continue
        if not assigned:
            db.mark_submission_ambiguous(int(slot["slot_id"]), f"远端已创建 {task_id}，本地占位冲突")
            disabled_accounts.add(account_id)
            skipped += 1
            blocked_by_rules += 1
            continue
        scheduled += 1

    supply = db.initialization_supply_summary(target_count=ACCOUNT_INITIALIZATION_TARGET_COUNT)
    return SchedulingStats(
        slots_created=slots_created,
        slots_examined=len(pending_slots),
        scheduled=scheduled,
        skipped=skipped,
        create_failed=create_failed,
        blocked_by_rules=blocked_by_rules,
        retryable_create_failed=retryable_create_failed,
        initialization_accounts=supply["initialization_accounts"],
        initialization_pending=supply["initialization_pending"],
        initialization_completed=supply["initialization_completed"],
        initialization_required_items=supply["required_items"],
        initialization_scheduled_items=supply["scheduled_items"],
        initialization_ready_pool_items=supply["ready_pool_items"],
        initialization_content_gap=supply["content_gap"],
    )


def sync_publish_results(
    db: AutoPublishDB,
    publisher: BasePublishAdapter,
    *,
    failure_grace_hours: int = 12,
) -> Dict[str, int]:
    stats = {"published": 0, "failed": 0, "pending": 0, "errors": 0}
    tasks = db.list_scheduled_tasks()
    now = datetime.now()
    grace = timedelta(hours=max(0, int(failure_grace_hours)))
    auth_blocked_adapters: set[int] = set()
    for task in tasks:
        adapter = publisher
        try:
            scheduled_for = datetime.strptime(str(task["scheduled_for"]), "%Y-%m-%d %H:%M:%S")
            task_id = str(task["publish_task_id"])
            resolve = getattr(publisher, "_adapter_for_task_id", None)
            adapter = resolve(task_id) if callable(resolve) else publisher
            if id(adapter) in auth_blocked_adapters:
                stats["pending"] += 1
                stats["errors"] += 1
                continue
            status = adapter.query_task_status(task_id=task_id, scheduled_for=scheduled_for)
            _refresh_actual_bgm(db, publisher, task)
            outcome = _apply_publish_result(db, task, status, scheduled_for, now, grace)
            stats[outcome] += 1
        except Exception as exc:
            # Query failure is not publication failure. In particular, never
            # release a known task into the ready pool because auth expired.
            stats["pending"] += 1
            stats["errors"] += 1
            if _is_auth_error(exc):
                auth_blocked_adapters.add(id(adapter))
            continue
    return stats


def _apply_publish_result(db, task, status, scheduled_for, now, grace) -> str:
    task_id = str(task["publish_task_id"])
    if status.state == "success":
        is_photo = "media_kind" in task.keys() and task["media_kind"] == "native_photo"
        db.mark_publish_result(
            canonical_script_key=str(task["canonical_script_key"] or ""),
            script_id=str(task["script_id"]), publish_task_id=task_id,
            schedule_status="已发布", publish_status="已发布", publish_result="发布成功",
            published_at=(status.published_at or (None if is_photo else scheduled_for.strftime("%Y-%m-%d %H:%M:%S"))),
            platform_post_id=getattr(status, "platform_post_id", None),
            platform_post_url=getattr(status, "platform_post_url", None),
        )
        return "published"
    if status.state == "failed":
        if not task_id.startswith("neobund:") and scheduled_for + grace > now:
            return "pending"
        db.mark_publish_result(
            canonical_script_key=str(task["canonical_script_key"] or ""),
            script_id=str(task["script_id"]), publish_task_id=task_id,
            schedule_status="发布失败", publish_status="发布失败", publish_result="发布失败",
            error_message=status.error_message,
        )
        return "failed"
    if task_id.startswith("neobund:") and str(task["schedule_status"] or "") == "已取消":
        db.assign_slot(
            slot_id=int(task["slot_id"]), canonical_script_key=str(task["canonical_script_key"] or ""),
            script_id=str(task["script_id"]), publish_task_id=task_id,
            account_id=str(task["account_id"] or ""), account_name=str(task["account_name"] or ""),
            planned_publish_at=scheduled_for,
        )
    return "pending"


def _refresh_actual_bgm(db: AutoPublishDB, publisher: BasePublishAdapter, task: Any) -> None:
    """Record the platform's actual BGM once, without adding a polling loop."""
    def field(name: str) -> Any:
        try:
            return task[name]
        except (KeyError, IndexError, TypeError):
            return ""

    raw = str(field("bgm_json") or "").strip()
    task_id = str(field("publish_task_id") or "")
    if not raw or not task_id.startswith("neobund:"):
        return
    try:
        audit = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return
    actual = audit.get("actual") or {}
    if actual.get("confirmed_at"):
        return
    reader = getattr(publisher, "actual_music_for_task", None)
    if not callable(reader):
        return
    try:
        observed = reader(task_id) or {}
    except Exception:
        return  # result polling remains healthy when music readback is delayed
    music_id = str(observed.get("music_id") or "").strip()
    if not music_id:
        return
    selected_id = str(audit.get("music_id") or "").strip()
    audit["actual"] = {
        "music_id": music_id,
        "title": str(observed.get("title") or ""),
        "author": str(observed.get("author") or ""),
        "confirmed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    audit["music_match"] = (music_id == selected_id)
    db.update_slot_bgm_json(int(field("slot_id")), json.dumps(audit, ensure_ascii=False, sort_keys=True))


def terminate_and_requeue_tasks(
    db: AutoPublishDB,
    publisher: BasePublishAdapter,
    task_ids: Iterable[str],
    *,
    reason: str = "",
) -> Dict[str, Any]:
    resolved_task_ids = [str(item or "").strip() for item in task_ids if str(item or "").strip()]
    results = []
    for task_id in resolved_task_ids:
        slot = db.get_publish_slot_by_task_id(task_id)
        if slot is None:
            results.append({"task_id": task_id, "status": "not_found"})
            continue
        if str(slot["schedule_status"] or "") == "已发布":
            results.append({"task_id": task_id, "status": "already_published"})
            continue
        try:
            termination = publisher.terminate_task(task_id=task_id)
            if termination.state != "terminated":
                raise RuntimeError(f"远端任务未确认终止: {termination.result}")
            requeue = db.requeue_terminated_task(task_id, reason=reason)
            results.append({"task_id": task_id, "status": "requeued", **requeue})
        except Exception as exc:
            results.append({"task_id": task_id, "status": "failed", "error": str(exc)})
    return {
        "requested": len(resolved_task_ids),
        "requeued": sum(1 for item in results if item["status"] == "requeued"),
        "failed": sum(1 for item in results if item["status"] == "failed"),
        "skipped": sum(1 for item in results if item["status"] not in {"requeued", "failed"}),
        "items": results,
    }
