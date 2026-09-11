"""Idempotent, model-free registration of ready-made scripts in the publish DB."""

from __future__ import annotations

from dataclasses import asdict
import re
from types import SimpleNamespace
from typing import Any

from app.models import ScriptMetadata
from app.metadata import is_title_compatible_with_country, sanitize_title


def normalize_cart(value: Any) -> str:
    if isinstance(value, bool):
        return "是" if value else "否"
    text = str(value if value is not None else "").strip().lower()
    if text in {"是", "挂车", "true", "1", "yes"}:
        return "是"
    if text in {"否", "不挂车", "false", "0", "no"}:
        return "否"
    if text in {"", "按用途默认"}:
        return ""
    raise ValueError(f"未知挂车设置：{value}")


def forced_organic(candidate: Any) -> bool:
    markers = " ".join(str(getattr(candidate, name, "") or "") for name in
                       ("script_source", "publish_purpose", "content_branch"))
    return "种草" in markers or "SEEDING_ORGANIC" in markers


def resolve_cart(candidate: Any) -> str:
    if forced_organic(candidate):
        return "否"
    cart = normalize_cart(getattr(candidate, "cart_enabled", ""))
    if cart:
        return cart
    nurture = (getattr(candidate, "publish_purpose", "") == "养号"
               or getattr(candidate, "script_source", "") == "养号复刻"
               or getattr(candidate, "content_branch", "") == "非商品展示型")
    return "否" if nurture else "是"


def reject_internal_product_id(product_id: str) -> None:
    if re.fullmatch(r"S\d+", str(product_id or "").strip(), re.IGNORECASE):
        raise ValueError("内部产品编码不能作为平台商品 ID，请先配置挂车商品映射")


def publishing_product_id(candidate: Any) -> str:
    if resolve_cart(candidate) == "否":
        return ""
    product_id = str(getattr(candidate, "product_id", "") or "").strip()
    if getattr(candidate, "script_pool_registered", False):
        product_id = str(getattr(candidate, "platform_product_id", "") or "").strip()
        if not re.fullmatch(r"[0-9]{10,30}", product_id):
            raise ValueError("明确挂车的总库脚本缺少有效平台商品映射；请填写平台商品 ID 后重试发布")
    if not product_id:
        raise ValueError("明确挂车的视频缺少有效平台商品 ID；请补充商品映射后重试发布")
    reject_internal_product_id(product_id)
    return product_id


def register_script_pool_metadata(
    db, *, script_id: str, source_record_id: str, prompt: str,
    product_id: str = "", platform_product_id: str | None = None, store_id: str = "",
    publish_purpose: str = "带货", cart_enabled: Any = "",
    target_country: str = "", target_language: str = "", short_video_title: str = "",
    parent_slot: str = "", direction_label: str = "", variant_strength: str = "",
    product_type: str = "", content_family_key: str = "", task_name: str = "",
    script_source: str = "成功脚本复刻", content_branch: str = "SUCCESS_SCRIPT_REPLICATION",
    audio_mode: str = "",
) -> dict:
    """Register/update only unexecuted scripts; never change a frozen snapshot.

    product_id is the analytical/internal product reference. The independent
    platform_product_id may be blank at import time; publication validates it.
    None preserves an existing mapping only for the same analytical product;
    explicit empty text or cart=no clears it.
    The caller must pass a DB handle (tests can use a temporary SQLite file).
    """
    script_id = str(script_id or "").strip()
    if not script_id or not source_record_id or not str(prompt or "").strip():
        raise ValueError("总库登记必须提供稳定脚本 ID、来源行 ID 和完整提示词")
    if publish_purpose not in {"带货", "养号", "种草"}:
        raise ValueError(f"未知发布用途：{publish_purpose}")
    if script_source not in {"成功脚本复刻", "原创生成", "原创脚本", "视频复刻", "养号复刻", "人工编写", "种草脚本"}:
        raise ValueError(f"未注册脚本来源：{script_source}")
    cart = resolve_cart(SimpleNamespace(script_source=script_source, publish_purpose=publish_purpose,
                                       content_branch=content_branch, cart_enabled=cart_enabled))
    platform_id = str(platform_product_id or "").strip() if cart == "是" else ""
    now = db._now_text()
    with db._connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        old = conn.execute("SELECT * FROM script_metadata WHERE script_id = ?", (script_id,)).fetchall()
        if len(old) > 1:
            raise ValueError("脚本 ID 对应多个主数据记录，必须先解决身份冲突")
        old = old[0] if old else None
        key = str(old["canonical_script_key"]) if old else script_id
        result = {"canonical_script_key": key, "script_id": script_id}
        frozen = conn.execute(
            "SELECT 1 FROM publish_slots WHERE canonical_script_key = ? AND "
            "(schedule_status IN ('提交中', '已排期', '已发布') OR COALESCE(publish_task_id, '') <> '') LIMIT 1",
            (key,),
        ).fetchone()
        frozen_asset = conn.execute(
            "SELECT 1 FROM video_assets WHERE canonical_script_key = ? AND "
            "(publish_status IN ('提交中', '已排期', '已发布') OR COALESCE(publish_task_id, '') <> '') LIMIT 1",
            (key,),
        ).fetchone()
        if frozen or frozen_asset:
            return dict(result, status="frozen")
        binding = conn.execute("SELECT * FROM script_pool_bindings WHERE canonical_script_key = ?", (key,)).fetchone()
        if cart == "是" and platform_product_id is None and old and binding and str(old["product_id"] or "") == product_id:
            platform_id = str(binding["platform_product_id"] or "")
        result["platform_product_id"] = platform_id
        language = target_language or (str(binding["target_language"] or "") if binding else "")
        country = target_country or (str(old["target_country"] or "") if old else "")
        title = sanitize_title(short_video_title or (str(old["short_video_title"] or "") if old else ""))
        if title and not is_title_compatible_with_country(title, country):
            title = ""
            result["title_status"] = "incompatible_ignored"
        else:
            result["title_status"] = "ready" if title else "missing"
        item = ScriptMetadata(
            canonical_script_key=key, script_id=script_id, source_record_id=source_record_id,
            script_slot=str(old["script_slot"]) if old else script_id,
            task_no=task_name or script_id, store_id=store_id or (str(old["store_id"] or "") if old else ""), product_id=product_id,
            parent_slot=parent_slot, direction_label=direction_label, variant_strength=variant_strength,
            target_country=country, product_type=product_type,
            content_family_key=content_family_key or (str(old["content_family_key"] or "") if old else "") or key, script_text=prompt,
            short_video_title=title,
            title_source=str(old["title_source"] or "script_pool") if old else "script_pool",
            script_source=script_source, publish_purpose=publish_purpose,
            cart_enabled=cart, content_branch=content_branch,
            audio_mode=str(audio_mode or (str(old["audio_mode"] or "") if old else "")),
        )
        values = asdict(item)
        if old and all(str(old[name] or "") == str(value or "") for name, value in values.items()) and binding \
                and binding["platform_product_id"] == platform_id and binding["target_language"] == language:
            return dict(result, status="unchanged")
        columns = list(values)
        conn.execute(
            "INSERT INTO script_metadata (" + ",".join(columns) + ",created_at,updated_at) VALUES (" +
            ",".join("?" for _ in range(len(columns) + 2)) + ") ON CONFLICT(canonical_script_key) DO UPDATE SET " +
            ",".join(f"{name}=excluded.{name}" for name in columns if name != "canonical_script_key") +
            ",updated_at=excluded.updated_at",
            [values[name] for name in columns] + [now, now],
        )
        conn.execute(
            "INSERT INTO script_pool_bindings (canonical_script_key,platform_product_id,target_language,updated_at) "
            "VALUES (?,?,?,?) ON CONFLICT(canonical_script_key) DO UPDATE SET "
            "platform_product_id=excluded.platform_product_id,target_language=excluded.target_language,updated_at=excluded.updated_at",
            (key, platform_id, language, now),
        )
        return dict(result, status="updated" if old else "created")
