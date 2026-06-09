from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from auto_mixcut.core.result import Result

from .context import SkillContext


DEFAULT_BASE_POSITIVE = "真实TikTok生活化风格，单镜头一镜到底，自然手持感，真实光线，4秒。"
DEFAULT_BASE_NEGATIVE_L1 = ["不要切镜", "不要水印", "不要字幕文字", "不要竞品logo", "商品变形", "错品类"]
PROMPT_BANK_SEGMENT_ALIASES = {
    "handheld_product": "product_display",
    "detail_atmosphere": "product_display",
    "tryon_result": "before_go_out",
}
PROMPT_BANK_REQUIRED_CATEGORIES = {"hair_accessories", "scarves_hats", "earrings", "womens_outerwear"}
PROMPT_BANK_REQUIRED_SEGMENTS = {"product_display", "mirror_routine", "home_lifestyle", "before_go_out", "seasonal_scene"}
PRODUCT_ONLY_SEGMENT_TYPES = {"product_still", "unboxing", "flatlay"}
GRADE_VARIANTS = {"A": 4, "B": 2, "C": 2}
PERTURBATION_DIMS = ["camera_motion", "time_light", "composition", "color_tone", "props_env", "micro_arc"]
MAX_PROMPT_CHARS = 1600
RICH_PERTURBATION_CATEGORIES = {"womens_outerwear", "earrings"}
ORIGINAL_SCRIPT_DB_ENV = "ORIGINAL_SCRIPT_GENERATOR_DB_PATH"
DEFAULT_ORIGINAL_SCRIPT_DB = Path.home() / ".openclaw" / "shared" / "data" / "original_script_generator.sqlite3"


class SegmentPromptFactorySkill:
    """Builds prompt packages for external AI video generation. It does not generate media."""

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def build_package(self, material_anchor_brief: dict[str, Any], template_slot: dict[str, Any], batch_seen: set[str] | None = None, persist: bool = True) -> Result:
        brief, local_human = _unwrap_brief(material_anchor_brief)
        validation = _validate_inputs(brief, template_slot)
        if not validation.success:
            return validation
        raw_category = str(brief.get("category") or "").strip()
        category = _normalize_category(self.ctx, raw_category)
        segment_type = str(template_slot.get("segment_type") or "home_lifestyle").strip()
        grade = _grade(template_slot)
        person_framing = _person_framing(template_slot, grade)
        contract_validation = _validate_segment_type_contract(self.ctx, category, segment_type)
        if not contract_validation.success:
            return contract_validation
        prompt_variables = _load_prompt_variables_config(self.ctx)
        pools = prompt_variables.get("variable_pools") or {}
        dedup_config = prompt_variables.get("dedup") or {}
        pool_validation = _validate_perturbation_pool(category, pools, dedup_config, person_framing=person_framing, factory_config=_load_factory_config(self.ctx))
        if not pool_validation.success:
            return pool_validation
        perturbation_slot = {**template_slot, "person_framing": person_framing, "segment_type": segment_type}
        perturbation = _choose_perturbation(brief, perturbation_slot, pools, batch_seen if batch_seen is not None else set(), _load_factory_config(self.ctx))
        hard_anchors = _list(brief.get("hard_anchors"))
        forbidden_actions = _list(brief.get("forbidden_actions")) or _list(brief.get("must_not_show"))
        key_constraints = _list(brief.get("key_visual_constraints")) or _list(brief.get("must_show"))
        prompt_result = _build_prompt_from_bank(self.ctx, category, segment_type, grade, brief, local_human, person_framing, perturbation)
        if not prompt_result.success:
            return prompt_result
        prompt_payload = prompt_result.data
        prompt = prompt_payload["prompt"]

        segment_prompt_id = str(uuid.uuid4())
        package = {
            "segment_prompt_id": segment_prompt_id,
            "segment_script_id": _segment_script_id(segment_prompt_id),
            "product_id": brief.get("product_id"),
            "sku_id": str(template_slot.get("sku_id") or brief.get("sku_id") or "DEFAULT"),
            "reference_image_pack_id": str(template_slot.get("reference_image_pack_id") or brief.get("reference_image_pack_id") or ""),
            "reference_image_version": int(template_slot.get("reference_image_version") or brief.get("reference_image_version") or 0),
            "reference_image_preview_url": str(template_slot.get("reference_image_preview_url") or brief.get("reference_image_preview_url") or ""),
            "reference_image_status": str(template_slot.get("reference_image_status") or brief.get("reference_image_status") or ""),
            "raw_category": raw_category,
            "category": category,
            "template_id": template_slot.get("template_id"),
            "slot_index": int(template_slot.get("slot_index") or 0),
            "slot_role": str(template_slot.get("slot_role") or template_slot.get("role") or "scene"),
            "hook_intent": str(template_slot.get("hook_intent") or _default_hook_intent(template_slot)),
            "prompt_grade": grade,
            "ai_gen_grade": grade,
            "material_qc_grade": None,
            "segment_type": segment_type,
            "person_framing": person_framing,
            "duration_sec": 4,
            "prompt": prompt,
            "persona_context": prompt_payload.get("persona_context") or {},
            "gen_policy": {
                "num_variants": GRADE_VARIANTS[grade],
                "perturbation_seed_group": perturbation,
                "lock_character_ref": _lock_character_ref(grade, person_framing),
                "perturbation_pool_warning": _perturbation_pool_warning(category, pools, batch_seen, dedup_config),
            },
            "anchor_ref": {
                "hard_anchors": hard_anchors,
                "forbidden_actions": forbidden_actions,
                "key_visual_constraints": key_constraints,
            },
            "created_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        post_validation = _post_assembly_validate(self.ctx, package)
        if not post_validation.success:
            return post_validation
        package = post_validation.data
        final_validation = _validate_package(package)
        if not final_validation.success:
            return final_validation
        if not persist:
            return Result.ok(package)
        saved = self.save_package(package)
        return saved if not saved.success else Result.ok(package)

    def build_packages(self, material_anchor_brief: dict[str, Any], template_slot: dict[str, Any], count: int | None = None, persist: bool = True) -> Result:
        seen: set[str] = set()
        first = self.build_package(material_anchor_brief, template_slot, seen, persist=persist)
        if not first.success:
            return first
        target = count or int(first.data["gen_policy"]["num_variants"])
        packages = [first.data]
        for _ in range(1, target):
            item = self.build_package(material_anchor_brief, template_slot, seen, persist=persist)
            if not item.success:
                return item
            packages.append(item.data)
        return Result.ok({"packages": packages})

    def save_package(self, package: dict[str, Any], status: str = "created") -> Result:
        table = _ensure_prompt_package_table(self.ctx)
        if not table.success:
            return table
        row = _package_row(package, status)
        return self.ctx.repo.upsert("segment_prompt_packages", "segment_prompt_id", row)

    def mark_submitted(self, segment_prompt_id: str, provider: str, external_job_id: str = "", feishu_record_id: str = "") -> Result:
        table = _ensure_prompt_package_table(self.ctx)
        if not table.success:
            return table
        package = self.ctx.repo.get("segment_prompt_packages", "segment_prompt_id", segment_prompt_id)
        if not package:
            return Result.fail("PROMPT_PACKAGE_NOT_FOUND", "segment prompt package not found", {"segment_prompt_id": segment_prompt_id})
        return self.ctx.repo.update(
            "segment_prompt_packages",
            "segment_prompt_id",
            segment_prompt_id,
            {"package_status": "submitted", "external_provider": provider, "external_job_id": external_job_id, "feishu_record_id": feishu_record_id},
        )

    def mark_imported(self, segment_prompt_id: str, generated_asset_id: str = "", generated_segment_id: str = "") -> Result:
        table = _ensure_prompt_package_table(self.ctx)
        if not table.success:
            return table
        package = self.ctx.repo.get("segment_prompt_packages", "segment_prompt_id", segment_prompt_id)
        if not package:
            return Result.fail("PROMPT_PACKAGE_NOT_FOUND", "segment prompt package not found", {"segment_prompt_id": segment_prompt_id})
        return self.ctx.repo.update(
            "segment_prompt_packages",
            "segment_prompt_id",
            segment_prompt_id,
            {"package_status": "imported", "generated_asset_id": generated_asset_id, "generated_segment_id": generated_segment_id},
        )


def _unwrap_brief(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    brief = payload.get("material_anchor_brief") if isinstance(payload.get("material_anchor_brief"), dict) else payload
    local_human = payload.get("ai_local_human_brief") if isinstance(payload.get("ai_local_human_brief"), dict) else {}
    return dict(brief or {}), dict(local_human or {})


def _validate_inputs(brief: dict[str, Any], template_slot: dict[str, Any]) -> Result:
    if not str(brief.get("product_id") or "").strip():
        return Result.fail("PRODUCT_ID_REQUIRED", "material_anchor_brief.product_id is required")
    if not str(brief.get("category") or "").strip():
        return Result.fail("CATEGORY_REQUIRED", "material_anchor_brief.category is required")
    if not _list(brief.get("hard_anchors")):
        return Result.fail("HARD_ANCHORS_REQUIRED", "material_anchor_brief.hard_anchors is required for return QC")
    if not isinstance(template_slot, dict):
        return Result.fail("TEMPLATE_SLOT_REQUIRED", "template_slot must be an object")
    return Result.ok()


def _ensure_prompt_package_table(ctx: SkillContext) -> Result:
    try:
        with ctx.repo.connect() as conn:
            if getattr(ctx.repo, "dialect", "sqlite") == "mysql":
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS segment_prompt_packages (
                          id BIGINT PRIMARY KEY AUTO_INCREMENT,
                          segment_prompt_id VARCHAR(128) NOT NULL UNIQUE,
                          segment_script_id VARCHAR(64),
                          product_id VARCHAR(128) NOT NULL,
                          sku_id VARCHAR(128),
                          reference_image_pack_id VARCHAR(256),
                          reference_image_version INT DEFAULT 0,
                          raw_category VARCHAR(128),
                          category VARCHAR(128),
                          template_id VARCHAR(128),
                          slot_index INT,
                          slot_role VARCHAR(64),
                          hook_intent VARCHAR(128),
                          prompt_grade VARCHAR(16),
                          ai_gen_grade VARCHAR(16),
                          material_qc_grade VARCHAR(64),
                          segment_type VARCHAR(128),
                          person_framing VARCHAR(64),
                          duration_sec INT,
                          prompt_package_json JSON,
                          anchor_ref_json JSON,
                          perturbation_seed_json JSON,
                          package_status VARCHAR(64),
                          external_provider VARCHAR(128),
                          external_job_id VARCHAR(256),
                          generated_asset_id VARCHAR(128),
                          generated_segment_id VARCHAR(128),
                          feishu_record_id VARCHAR(128),
                          failure_reason TEXT,
                          created_at DATETIME,
                          updated_at DATETIME
                        )
                        """
                    )
            else:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS segment_prompt_packages (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      segment_prompt_id TEXT NOT NULL UNIQUE,
                      segment_script_id TEXT,
                      product_id TEXT NOT NULL,
                      sku_id TEXT,
                      reference_image_pack_id TEXT,
                      reference_image_version INTEGER DEFAULT 0,
                      raw_category TEXT,
                      category TEXT,
                      template_id TEXT,
                      slot_index INTEGER,
                      slot_role TEXT,
                      hook_intent TEXT,
                      prompt_grade TEXT,
                      ai_gen_grade TEXT,
                      material_qc_grade TEXT,
                      segment_type TEXT,
                      person_framing TEXT,
                      duration_sec INTEGER,
                      prompt_package_json TEXT,
                      anchor_ref_json TEXT,
                      perturbation_seed_json TEXT,
                      package_status TEXT,
                      external_provider TEXT,
                      external_job_id TEXT,
                      generated_asset_id TEXT,
                      generated_segment_id TEXT,
                      feishu_record_id TEXT,
                      failure_reason TEXT,
                      created_at TEXT,
                      updated_at TEXT
                    )
                    """
                )
            _ensure_prompt_package_column(ctx, conn, "segment_script_id", "VARCHAR(64)" if getattr(ctx.repo, "dialect", "sqlite") == "mysql" else "TEXT")
            _ensure_prompt_package_column(ctx, conn, "sku_id", "VARCHAR(128)" if getattr(ctx.repo, "dialect", "sqlite") == "mysql" else "TEXT")
            _ensure_prompt_package_column(ctx, conn, "reference_image_pack_id", "VARCHAR(256)" if getattr(ctx.repo, "dialect", "sqlite") == "mysql" else "TEXT")
            _ensure_prompt_package_column(ctx, conn, "reference_image_version", "INT DEFAULT 0" if getattr(ctx.repo, "dialect", "sqlite") == "mysql" else "INTEGER DEFAULT 0")
            _ensure_prompt_package_column(ctx, conn, "reference_image_preview_url", "TEXT")
            _ensure_prompt_package_column(ctx, conn, "reference_image_status", "VARCHAR(64)" if getattr(ctx.repo, "dialect", "sqlite") == "mysql" else "TEXT")
        return Result.ok()
    except Exception as exc:
        return Result.fail("PROMPT_PACKAGE_TABLE_FAILED", str(exc))


def _ensure_prompt_package_column(ctx: SkillContext, conn: Any, column: str, spec: str) -> None:
    if getattr(ctx.repo, "dialect", "sqlite") == "mysql":
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s AND column_name=%s
                LIMIT 1
                """,
                (ctx.repo.database, "segment_prompt_packages", column),
            )
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE segment_prompt_packages ADD COLUMN {column} {spec}")
        return
    existing = {row["name"] for row in conn.execute("PRAGMA table_info(segment_prompt_packages)").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE segment_prompt_packages ADD COLUMN {column} {spec}")


def _package_row(package: dict[str, Any], status: str) -> dict[str, Any]:
    return {
        "segment_prompt_id": package["segment_prompt_id"],
        "segment_script_id": package.get("segment_script_id") or _segment_script_id(package["segment_prompt_id"]),
        "product_id": package["product_id"],
        "sku_id": package.get("sku_id") or "DEFAULT",
        "reference_image_pack_id": package.get("reference_image_pack_id") or "",
        "reference_image_version": int(package.get("reference_image_version") or 0),
        "reference_image_preview_url": package.get("reference_image_preview_url") or "",
        "reference_image_status": package.get("reference_image_status") or "",
        "raw_category": package.get("raw_category"),
        "category": package["category"],
        "template_id": package.get("template_id"),
        "slot_index": package.get("slot_index"),
        "slot_role": package.get("slot_role"),
        "hook_intent": package.get("hook_intent"),
        "prompt_grade": package.get("prompt_grade") or package.get("ai_gen_grade"),
        "ai_gen_grade": package.get("ai_gen_grade"),
        "material_qc_grade": package.get("material_qc_grade"),
        "segment_type": package.get("segment_type"),
        "person_framing": package.get("person_framing"),
        "duration_sec": package.get("duration_sec"),
        "prompt_package_json": package,
        "anchor_ref_json": package.get("anchor_ref"),
        "perturbation_seed_json": (package.get("gen_policy") or {}).get("perturbation_seed_group"),
        "package_status": status,
    }


def _normalize_category(ctx: SkillContext, raw_category: str) -> str:
    aliases = _load_factory_config(ctx).get("category_normalization", {}).get("aliases", {})
    key = str(raw_category or "").strip()
    return str(aliases.get(key) or key or "generic_fashion")


def _segment_script_id(segment_prompt_id: str) -> str:
    compact = "".join(ch for ch in str(segment_prompt_id or "") if ch.isalnum()).upper()
    return f"SPK-{compact[:8] or uuid.uuid4().hex[:8].upper()}"


def _load_factory_config(ctx: SkillContext) -> dict[str, Any]:
    path = ctx.settings.root_dir / "config" / "ai_segment_factory.yaml"
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except FileNotFoundError:
        return {}


def _post_validation_config(ctx: SkillContext) -> dict[str, Any]:
    return _load_factory_config(ctx).get("post_assembly_validation") or {}


def _load_prompt_bank(ctx: SkillContext) -> dict[str, Any]:
    path = ctx.settings.root_dir / "config" / "prompt_bank.yaml"
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except FileNotFoundError:
        return {}


def _build_prompt_from_bank(
    ctx: SkillContext,
    category: str,
    segment_type: str,
    grade: str,
    brief: dict[str, Any],
    local_human: dict[str, Any],
    person_framing: str,
    perturbation: dict[str, str],
) -> Result:
    bank = _load_prompt_bank(ctx)
    bank_validation = _validate_prompt_bank(bank)
    if not bank_validation.success:
        return bank_validation
    category_bank = ((bank.get("prompt_bank") or {}).get(category) or (bank.get("prompt_bank") or {}).get("womens_outerwear") or {})
    segments = category_bank.get("segments") or {}
    segment_key = segment_type if segment_type in segments else _bank_segment_key(segment_type)
    row = segments.get(segment_key)
    if not row:
        return Result.fail("PROMPT_BANK_ROW_MISSING", "prompt bank row is missing", {"category": category, "segment_type": segment_type, "segment_key": segment_key})

    product_only = person_framing == "product_only"
    selected_anchors = _select_key_anchors(brief, 1 if grade == "C" else 2)
    key_anchor = "、".join(selected_anchors) or str(brief.get("display_family") or category_bank.get("family_word") or "商品")
    family_word = _family_word(category_bank, brief)
    positive_core = _fill_prompt_template(str(row.get("positive_zh") or ""), key_anchor, family_word)
    if product_only:
        key_anchor = _product_only_key_anchor(brief, selected_anchors, family_word)
        positive_core = _product_only_anchor_core(segment_type, key_anchor, family_word)
    persona_context = {} if product_only else _resolve_persona_context(ctx, category, segment_type, brief, perturbation)
    if product_only:
        positive_parts = [
            "真实TikTok商品静物风格，单镜头一镜到底，真实光线，4秒",
            positive_core,
            _grade_positive_suffix(grade),
            _product_only_layer(ctx, category, segment_type),
            _product_only_perturbation_layer(perturbation),
        ]
        motion_arc = _product_only_motion(ctx, category, segment_type, brief, perturbation)
    else:
        positive_parts = [
            str((bank.get("base") or {}).get("positive_prefix") or DEFAULT_BASE_POSITIVE),
            positive_core,
            _grade_positive_suffix(grade),
            _person_layer(ctx, category, segment_type, local_human, persona_context),
            _perturbation_layer(perturbation),
        ]
        motion_arc = _cn_arrow(_motion_from_bank(row, grade) or str(perturbation.get("micro_arc") or "开始静止 -> 轻微变化 -> 清楚停留"))
    negative_l1, negative_l2 = _negative_layers(ctx, bank, category_bank, category, brief, local_human, persona_context, person_framing, segment_type)
    prompt = _assemble_prompt(positive_parts, negative_l1, negative_l2, motion_arc)
    prompt_validation = _validate_prompt_text(prompt, category, segment_type, segment_key, grade, selected_anchors, negative_l1)
    if not prompt_validation.success:
        return prompt_validation
    return Result.ok({"prompt": prompt, "persona_context": persona_context})


def _validate_prompt_bank(bank: dict[str, Any]) -> Result:
    if not bank.get("prompt_bank"):
        return Result.fail("PROMPT_BANK_MISSING", "config/prompt_bank.yaml is required")
    base_l1 = _list((bank.get("base") or {}).get("negative_l1_zh")) or _list((bank.get("base") or {}).get("negative_required")) or DEFAULT_BASE_NEGATIVE_L1
    for required in ["不要切镜", "不要水印", "不要竞品logo", "商品变形", "错品类"]:
        if not any(required in item for item in base_l1):
            return Result.fail("PROMPT_BANK_L1_MISSING", "base negative L1 is missing required redline", {"required": required})
    prompt_bank = bank.get("prompt_bank") or {}
    for category in PROMPT_BANK_REQUIRED_CATEGORIES:
        category_bank = prompt_bank.get(category) or {}
        segments = category_bank.get("segments") or {}
        for segment in PROMPT_BANK_REQUIRED_SEGMENTS:
            row = segments.get(segment) or {}
            template = str(row.get("positive_zh") or "")
            if not row:
                return Result.fail("PROMPT_BANK_CELL_MISSING", "prompt bank must cover 4 categories x 5 segment types", {"category": category, "segment_type": segment})
            if "{关键锚点}" not in template or "{家族词}" not in template:
                return Result.fail("PROMPT_BANK_PLACEHOLDER_MISSING", "positive_zh must include {关键锚点} and {家族词}", {"category": category, "segment_type": segment})
        if category == "womens_outerwear":
            negative_l1 = "；".join(_list(category_bank.get("negative_l1_zh")))
            negative_l2 = "；".join(_list(category_bank.get("negative_l2_zh")))
            if "全身正面导致版型失真" not in negative_l1 or "前后帧衣服不一致" not in negative_l1:
                return Result.fail("PROMPT_BANK_OUTERWEAR_L1_MISSING", "womens_outerwear L1 must include precise full-body distortion guard and 前后帧衣服不一致")
            if "错误廓形" not in negative_l2 or "错误衣长" not in negative_l2:
                return Result.fail("PROMPT_BANK_OUTERWEAR_L2_MISSING", "womens_outerwear L2 must include key mismatch details")
    return Result.ok()


def _bank_segment_key(segment_type: str) -> str:
    if segment_type in PRODUCT_ONLY_SEGMENT_TYPES:
        return "product_display"
    return PROMPT_BANK_SEGMENT_ALIASES.get(segment_type, segment_type)


def _select_key_anchors(brief: dict[str, Any], limit: int) -> list[str]:
    candidates = _list(brief.get("hard_anchors")) or _list(brief.get("display_anchors")) or _list(brief.get("must_show"))
    selected: list[str] = []
    for item in candidates:
        text = _trim_anchor(item)
        if text and text not in selected:
            selected.append(text)
        if len(selected) >= limit:
            break
    return selected


def _trim_anchor(text: str) -> str:
    cleaned = str(text or "").strip(" ，,；;")
    if len(cleaned) <= 28:
        return cleaned
    for sep in ["，", ",", "；", ";", "、"]:
        if sep in cleaned:
            head = cleaned.split(sep, 1)[0].strip()
            if 2 <= len(head) <= 28:
                return head
    return cleaned[:28].rstrip()


def _family_word(category_bank: dict[str, Any], brief: dict[str, Any]) -> str:
    configured = str(category_bank.get("family_word") or "").strip()
    if configured:
        return configured
    value = str(brief.get("display_family") or "").strip()
    if value and _contains_cjk(value):
        return value
    return str(value or "商品")


def _fill_prompt_template(template: str, key_anchor: str, family_word: str) -> str:
    return template.replace("{关键锚点}", key_anchor).replace("{家族词}", family_word)


def _product_only_anchor_core(segment_type: str, key_anchor: str, family_word: str) -> str:
    if segment_type == "unboxing":
        return f"只拍包装与{family_word}，{key_anchor}在开箱露出时清楚可见"
    if segment_type == "flatlay":
        return f"带有{key_anchor}的{family_word}平铺俯拍，完整形状和关键细节清楚"
    return f"带有{key_anchor}的{family_word}纯物展示，完整形状、材质质感和做工细节清楚"


def _product_only_key_anchor(brief: dict[str, Any], selected_anchors: list[str], family_word: str) -> str:
    form = _list(brief.get("product_form")) or _list(brief.get("product_form_description")) or _list(brief.get("product_body_description"))
    if not form:
        subtype = str(brief.get("product_subtype") or "").strip()
        form = [subtype] if subtype else []
    candidates = [*form, *selected_anchors, *_list(brief.get("hard_anchors"))]
    cleaned = [_sanitize_product_only_anchor(item) for item in candidates]
    cleaned = [item for item in _dedupe(cleaned) if item]
    return "、".join(cleaned[:2]) or f"{family_word}本体"


def _sanitize_product_only_anchor(text: Any) -> str:
    value = str(text or "").strip()
    replacements = [
        "佩戴在耳垂外侧",
        "佩戴于耳垂外侧",
        "佩戴在耳侧",
        "佩戴效果",
        "佩戴",
        "耳垂外侧",
        "耳垂",
        "耳侧近景",
        "侧脸裁切",
        "随转头",
        "转头",
        "跟拍",
        "上身效果",
    ]
    for token in replacements:
        value = value.replace(token, "")
    return value.strip(" ，,；、")


def _grade_positive_suffix(grade: str) -> str:
    if grade == "A":
        return "产品清晰为主体，对焦在产品上"
    if grade == "C":
        return "重氛围，产品可不在画面中心"
    return ""


def _motion_from_bank(row: dict[str, Any], grade: str) -> str:
    arcs = row.get("motion_arc_zh") or {}
    return str(arcs.get(grade) or arcs.get("B") or arcs.get("A") or "")


def _product_only_layer(ctx: SkillContext, category: str, segment_type: str) -> str:
    lexicon = _load_factory_config(ctx).get("product_still_lexicon") or {}
    row = lexicon.get(segment_type) or lexicon.get("product_still") or {}
    common = str(row.get("common") or "").strip()
    by_category = row.get("by_category") or {}
    category_text = str(by_category.get(category) or by_category.get(_category_alias(category)) or by_category.get("generic_fashion") or "").strip()
    return "；".join(part for part in [common, category_text] if part)


def _product_only_motion(ctx: SkillContext, category: str, segment_type: str, brief: dict[str, Any], perturbation: dict[str, str]) -> str:
    options = _list((_load_factory_config(ctx).get("product_still_motion") or {}).get(segment_type))
    if not options:
        return _cn_arrow(str(perturbation.get("micro_arc") or "开始静止 -> 缓慢推近 -> 清楚停留"))
    seed = json.dumps({"product_id": brief.get("product_id"), "category": category, "segment_type": segment_type, "perturbation": perturbation}, sort_keys=True, ensure_ascii=False)
    return _cn_arrow(options[_stable_index(seed, "product_only_motion", len(options))])


def _product_only_negative(segment_type: str) -> list[str]:
    if segment_type == "unboxing":
        return ["不要面部", "不要身体", "不要人物出镜", "不要全身"]
    return ["不要人物", "不要人手", "不要面部", "不要身体", "不要穿戴动作"]


def _negative_layers(ctx: SkillContext, bank: dict[str, Any], category_bank: dict[str, Any], category: str, brief: dict[str, Any], local_human: dict[str, Any], persona_context: dict[str, Any] | None = None, person_framing: str = "ai_local", segment_type: str = "") -> tuple[list[str], list[str]]:
    policy = bank.get("negative_policy") or {}
    l2_limit = _positive_int(policy.get("l2_max_items"), 6)
    base_l1 = _list((bank.get("base") or {}).get("negative_l1_zh")) or _list((bank.get("base") or {}).get("negative_required")) or DEFAULT_BASE_NEGATIVE_L1
    category_l1 = _list(category_bank.get("negative_l1_zh"))
    category_l2 = _list(category_bank.get("negative_l2_zh")) or _list(category_bank.get("negative_add_zh"))
    brief_l1, brief_l2 = _classify_negative_items([
        *_list(brief.get("must_not_show")),
        *_list(brief.get("forbidden_actions")),
        *_list(local_human.get("forbidden_performance")),
    ], category)
    product_only = person_framing == "product_only"
    risk_l1, risk_l2 = ([], []) if product_only else _risk_negative_layers(ctx, category)
    performance_l2 = [] if product_only else _list((_load_factory_config(ctx).get("performance_beats") or {}).get("forbidden_performance"))
    original_risk = (persona_context or {}).get("risk_contract") if isinstance((persona_context or {}).get("risk_contract"), dict) else {}
    original_l2 = [] if product_only else [*_list(original_risk.get("forbidden_performance")), *_list(original_risk.get("anti_template_warnings"))]
    product_only_l1 = _product_only_negative(segment_type) if product_only else []
    l1 = _dedupe(_sanitize_negative_items([*base_l1, *category_l1, *brief_l1, *risk_l1, *product_only_l1], category))
    l2 = _dedupe(_sanitize_negative_items([*brief_l2, *risk_l2, *original_l2, *performance_l2, *category_l2], category))[:l2_limit]
    return l1, l2


def _classify_negative_items(items: list[str], category: str = "") -> tuple[list[str], list[str]]:
    redline_tokens = ["不要切镜", "不要转场", "不要分屏", "不要水印", "不要字幕", "不要文字", "竞品", "商品变形", "错品类", "前后帧", "不一致", "全身正面导致版型失真"]
    l1: list[str] = []
    l2: list[str] = []
    for item in items:
        text = _sanitize_negative_item(str(item or "").strip(), category)
        if not text:
            continue
        if any(token in text for token in redline_tokens):
            l1.append(text)
        else:
            l2.append(text)
    return l1, l2


def _assemble_prompt(positive_parts: list[Any], negative_l1: list[str], negative_l2: list[str], motion_arc: str) -> dict[str, str]:
    positive = _compact_prompt(_join_prompt(positive_parts))
    l1 = _dedupe(negative_l1)
    l2 = _dedupe(negative_l2)
    while True:
        negative = _join_prompt([*l1, *l2])
        total_len = len(positive) + len(negative) + len(motion_arc)
        if total_len <= MAX_PROMPT_CHARS:
            return {"positive": positive, "negative": negative, "motion_arc": motion_arc}
        if l2:
            l2.pop()
            continue
        l1_negative = _join_prompt(l1)
        positive_budget = MAX_PROMPT_CHARS - len(l1_negative) - len(motion_arc)
        if positive_budget > 0 and len(positive) > positive_budget:
            positive = positive[:positive_budget].rstrip("，；、 ")
        return {"positive": positive, "negative": l1_negative, "motion_arc": motion_arc}


def _perturbation_layer(perturbation: dict[str, str]) -> str:
    pieces = [str(perturbation.get(dim) or "").strip() for dim in ["camera_motion", "time_light", "composition", "color_tone", "props_env"]]
    return "，".join(piece for piece in pieces if piece)


def _product_only_perturbation_layer(perturbation: dict[str, str]) -> str:
    blocked_tokens = ["过肩", "侧脸", "背影", "模特", "人物", "看向", "眼神", "全身", "上身", "佩戴"]
    fallbacks = {
        "camera_motion": "缓慢推近",
        "time_light": "柔和自然光",
        "composition": "产品居中构图",
        "color_tone": "干净高级灰调",
        "props_env": "干净台面",
    }
    pieces = []
    for dim in ["camera_motion", "time_light", "composition", "color_tone", "props_env"]:
        value = str(perturbation.get(dim) or "").strip()
        if not value or any(token in value for token in blocked_tokens):
            value = fallbacks[dim]
        pieces.append(value)
    return "，".join(piece for piece in pieces if piece)


def _compact_prompt(text: str) -> str:
    compacted = "；".join(part.strip(" ；") for part in str(text or "").split("；") if part.strip(" ；"))
    return compacted if len(compacted) <= MAX_PROMPT_CHARS else compacted[:MAX_PROMPT_CHARS].rstrip("，；、 ")


def _contains_cjk(text: str) -> bool:
    return any("\u4e00" <= char <= "\u9fff" for char in str(text or ""))


def _validate_prompt_text(prompt: dict[str, str], category: str, segment_type: str, segment_key: str, grade: str, selected_anchors: list[str], negative_l1: list[str]) -> Result:
    total_len = len(prompt.get("positive") or "") + len(prompt.get("negative") or "") + len(prompt.get("motion_arc") or "")
    if total_len > MAX_PROMPT_CHARS:
        return Result.fail("PROMPT_TOO_LONG", "prompt positive + negative + motion_arc must be <= 1600 chars", {"chars": total_len})
    if len(selected_anchors) > 2:
        return Result.fail("TOO_MANY_KEY_ANCHORS", "positive prompt can use at most 2 key anchors", {"anchors": selected_anchors})
    positive = prompt.get("positive") or ""
    negative = prompt.get("negative") or ""
    missing_l1 = [item for item in negative_l1 if item and item not in negative]
    if missing_l1:
        return Result.fail("NEGATIVE_L1_MISSING", "negative prompt must keep all L1 redlines", {"missing": missing_l1})
    if category == "earrings" and segment_type == "product_display" and segment_key == "product_display" and grade == "A" and "微距" not in positive:
        return Result.fail("EARRING_A_MACRO_REQUIRED", "earrings A product_display prompt must include 微距")
    return Result.ok()


def _validate_package(package: dict[str, Any]) -> Result:
    if not package.get("segment_prompt_id"):
        return Result.fail("PROMPT_ID_REQUIRED", "segment_prompt_id is required")
    if not str(package.get("segment_script_id") or "").startswith("SPK-"):
        return Result.fail("SEGMENT_SCRIPT_ID_REQUIRED", "segment_script_id is required")
    if not package["anchor_ref"]["hard_anchors"]:
        return Result.fail("HARD_ANCHORS_REQUIRED", "anchor_ref.hard_anchors is required")
    if package["duration_sec"] != 4:
        return Result.fail("DURATION_INVALID", "AI segment prompts must be 4 seconds")
    negative = package["prompt"]["negative"]
    has_no_cut = any(token in negative for token in ["不要切镜", "禁止切镜", "no cut"])
    has_no_watermark = any(token in negative for token in ["不要水印", "禁止水印", "no watermark"])
    if not (has_no_cut and has_no_watermark):
        return Result.fail("NEGATIVE_CONSTRAINT_MISSING", "negative prompt must include no cut/不要切镜 and no watermark/不要水印")
    return Result.ok()


def _load_prompt_variables_config(ctx: SkillContext) -> dict[str, Any]:
    path = ctx.settings.root_dir / "config" / "prompt_variables.yaml"
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except FileNotFoundError:
        return {}


def _load_variable_pools(ctx: SkillContext) -> dict[str, Any]:
    return _load_prompt_variables_config(ctx).get("variable_pools") or {}


def _category_execution_contract(config_or_ctx: dict[str, Any] | SkillContext, category: str) -> dict[str, Any]:
    config = _load_factory_config(config_or_ctx) if isinstance(config_or_ctx, SkillContext) else (config_or_ctx or {})
    contracts = config.get("category_execution_contract") or {}
    normalized = _category_alias(category)
    return dict(contracts.get(normalized) or contracts.get(category) or {})


def _validate_segment_type_contract(ctx: SkillContext, category: str, segment_type: str) -> Result:
    contract = _category_execution_contract(ctx, category)
    if not contract:
        return Result.ok()
    allowed = set(_list(contract.get("allowed_segment_types")))
    forbidden = set(_list(contract.get("forbidden_segment_types")))
    if segment_type in forbidden:
        return Result.fail("SEGMENT_TYPE_FORBIDDEN_BY_CATEGORY", "segment_type is forbidden by category execution contract", {"category": _category_alias(category), "segment_type": segment_type})
    if allowed and segment_type not in allowed:
        return Result.fail("SEGMENT_TYPE_NOT_ALLOWED_BY_CATEGORY", "segment_type is not allowed by category execution contract", {"category": _category_alias(category), "segment_type": segment_type, "allowed": sorted(allowed)})
    return Result.ok()


def _choose_perturbation(brief: dict[str, Any], slot: dict[str, Any], pools: dict[str, Any], seen: set[str], validation_config: dict[str, Any] | None = None) -> dict[str, str]:
    category = _category_alias(str(brief.get("category") or "generic_fashion"))
    validation_config = validation_config or {}
    person_framing = str(slot.get("person_framing") or "")
    pool_key = _perturbation_pool_key(category, person_framing, validation_config)
    pool = pools.get(pool_key)
    if pool is None:
        raise ValueError(f"missing perturbation pool for category={category}, pool_key={pool_key}")
    salt_base = json.dumps({"product_id": brief.get("product_id"), "segment_type": slot.get("segment_type"), "slot_index": slot.get("slot_index")}, sort_keys=True, ensure_ascii=False)
    target_space = _sampling_target_space(str(slot.get("segment_type") or ""), salt_base, validation_config)
    for attempt in range(200):
        seed = f"{salt_base}:{attempt}"
        values = {}
        for dim in PERTURBATION_DIMS:
            options = _list(pool.get(dim)) or [dim.replace("_", " ")]
            options = _filter_sampling_options(options, dim, target_space, validation_config)
            values[dim] = options[_stable_index(seed, dim, len(options))]
        key = json.dumps(values, sort_keys=True, ensure_ascii=False)
        if key not in seen:
            seen.add(key)
            return values
    return values


def _validate_perturbation_pool(category: str, pools: dict[str, Any], dedup_config: dict[str, Any], person_framing: str = "", factory_config: dict[str, Any] | None = None) -> Result:
    normalized = _category_alias(category)
    pool_key = _perturbation_pool_key(normalized, person_framing, factory_config or {})
    if pool_key not in pools:
        return Result.fail("PERTURBATION_POOL_MISSING", "category perturbation pool is required; silent fallback is forbidden", {"category": normalized, "pool_key": pool_key})
    if person_framing == "product_only":
        return Result.ok()
    if normalized not in RICH_PERTURBATION_CATEGORIES:
        return Result.ok()
    pool = pools.get(pool_key) or {}
    min_per_dim = _positive_int(dedup_config.get("rich_pool_min_per_dim"), 6)
    min_scene_light = _positive_int(dedup_config.get("rich_pool_min_scene_light"), 8)
    missing = []
    for dim in PERTURBATION_DIMS:
        count = len(_list(pool.get(dim)))
        minimum = min_scene_light if dim in {"time_light", "props_env"} else min_per_dim
        if count < minimum:
            missing.append({"dim": dim, "count": count, "minimum": minimum})
    if missing:
        return Result.fail("PERTURBATION_POOL_TOO_THIN", "womens_outerwear/earrings perturbation pools must be rich enough", {"category": normalized, "missing": missing})
    return Result.ok()


def _perturbation_pool_warning(category: str, pools: dict[str, Any], seen: set[str] | None, dedup_config: dict[str, Any]) -> dict[str, Any]:
    normalized = _category_alias(category)
    if not seen:
        return {}
    pool = pools.get(normalized) or pools.get("generic_fashion") or {}
    total = _pool_combo_count(pool)
    used = len(seen)
    threshold = _positive_float(dedup_config.get("expansion_warning_ratio"), 0.30)
    if total and used / total >= threshold:
        return {"type": "perturbation_pool_near_exhausted", "used": used, "total_combinations": total, "threshold_ratio": threshold}
    return {}


def _perturbation_pool_key(category: str, person_framing: str, factory_config: dict[str, Any]) -> str:
    if person_framing == "product_only":
        return "product_only_shared"
    contract = _category_execution_contract(factory_config, category)
    return str(contract.get("variable_pool_key") or category).strip() or category


def _pool_combo_count(pool: dict[str, Any]) -> int:
    total = 1
    for dim in PERTURBATION_DIMS:
        total *= max(1, len(_list(pool.get(dim))))
    return total


def _lock_character_ref(grade: str, person_framing: str) -> bool:
    if person_framing == "product_only":
        return False
    if grade in {"A", "B"}:
        return True
    return person_framing == "ai_full_face"


def _positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _positive_float(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _stable_index(seed: str, dim: str, size: int) -> int:
    digest = hashlib.sha256(f"{seed}:{dim}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % max(1, size)


def _category_alias(category: str) -> str:
    return {"scarf_hat": "scarves_hats", "scarves": "scarves_hats", "womens_top": "womens_outerwear"}.get(category, category)


def _grade(slot: dict[str, Any]) -> str:
    value = str(slot.get("ai_gen_grade") or "").strip().upper()
    if value in GRADE_VARIANTS:
        return value
    role = str(slot.get("slot_role") or slot.get("role") or "").strip()
    return "A" if role in {"hero", "result"} and int(slot.get("slot_index") or 0) == 0 else "B" if role == "detail" else "C"


def _person_framing(slot: dict[str, Any], grade: str) -> str:
    value = str(slot.get("person_framing") or "").strip()
    if value == "product_only":
        return "product_only"
    if grade == "A":
        return "ai_local"
    return value if value in {"ai_local", "ai_full_face", "real_preferred"} else "ai_local"


def _default_hook_intent(slot: dict[str, Any]) -> str:
    role = str(slot.get("slot_role") or slot.get("role") or "")
    return {"hero": "product_clarity", "detail": "material_closeup", "result": "tryon_result"}.get(role, "atmosphere")


def _anchor_weight_text(grade: str, hard_anchors: list[str], brief: dict[str, Any]) -> str:
    if grade == "C":
        return str(brief.get("display_family") or brief.get("product_subtype") or "")
    anchors = hard_anchors * (2 if grade == "A" else 1)
    prefix = "商品硬锚点必须清楚且稳定：" if grade == "A" else "商品锚点："
    return prefix + "、".join(anchors)


def _video_content_layer(segment_type: str, grade: str, brief: dict[str, Any], perturbation: dict[str, str]) -> str:
    product = str(brief.get("product_subtype") or brief.get("display_family") or "商品").strip()
    primary = str(brief.get("primary_visual_result") or "").strip()
    camera = str(perturbation.get("camera_motion") or "轻微手持移动")
    scene = str(perturbation.get("props_env") or "真实生活环境")
    light = str(perturbation.get("time_light") or "自然光")
    arc = _cn_arrow(str(perturbation.get("micro_arc") or "开始静止 -> 轻微动作 -> 清楚停留"))
    focus = {
        "A": "商品必须是画面核心，前 1 秒就能看清主体",
        "B": "商品细节必须可识别，画面节奏轻微但稳定",
        "C": "以氛围和生活场景为主，商品自然出现但不抢戏",
    }.get(grade, "商品自然清晰出现")
    segment_goal = {
        "product_display": "拍一段商品展示短片",
        "handheld_product": "拍一段手持商品展示短片",
        "detail_atmosphere": "拍一段商品细节氛围短片",
        "tryon_result": "拍一段上身/佩戴效果短片",
        "mirror_routine": "拍一段镜前整理日常短片",
        "home_lifestyle": "拍一段居家生活氛围短片",
        "before_go_out": "拍一段出门前准备短片",
        "seasonal_scene": "拍一段季节生活场景短片",
    }.get(segment_type, "拍一段真实生活化商品短片")
    primary_text = f"，核心视觉结果是{primary}" if primary else ""
    return f"画面内容：{segment_goal}，主体是{product}{primary_text}；场景为{scene}，{light}，{camera}；动作弧线：{arc}；{focus}"


def _constraint_positive_layer(brief: dict[str, Any]) -> str:
    parts = []
    if brief.get("primary_visual_result"):
        parts.append(f"核心视觉结果：{brief.get('primary_visual_result')}")
    must_show = _list(brief.get("must_show"))
    if must_show:
        parts.append("必须出现：" + "、".join(must_show))
    constraints = _list(brief.get("key_visual_constraints"))
    if constraints:
        parts.append("关键视觉约束：" + "、".join(constraints))
    actions = _list(brief.get("safe_micro_actions"))
    if actions:
        parts.append("安全微动作：" + actions[0])
    return "；".join(parts)


def _category_grade_positive(ctx: SkillContext, category: str, grade: str) -> str:
    rules = ((_load_factory_config(ctx).get("prompt_rules") or {}).get("category_grade_positive") or {})
    return str(((rules.get(category) or {}).get(grade)) or "")


def _category_negative(ctx: SkillContext, category: str) -> list[str]:
    rules = ((_load_factory_config(ctx).get("prompt_rules") or {}).get("category_negative") or {})
    return _list(rules.get(category))


def _select_persona(persona_pool: dict[str, Any], category: str, brief: dict[str, Any], segment_type: str, perturbation: dict[str, str]) -> dict[str, str]:
    dimensions = persona_pool.get("dimensions") or {}
    seed = json.dumps(
        {
            "product_id": brief.get("product_id"),
            "category": category,
            "segment_type": segment_type,
            "perturbation": perturbation,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    gender_options = (dimensions.get("gender_by_category") or {}).get(category) or (dimensions.get("gender_by_category") or {}).get("generic_fashion") or "女"
    hair_options = (dimensions.get("hair_by_category") or {}).get(category) or dimensions.get("hair") or []
    return {
        "年龄段": _pick_config_value(dimensions.get("age_band"), seed, "age_band"),
        "性别": _pick_config_value(gender_options, seed, "gender"),
        "肤色": _pick_config_value(dimensions.get("skin_tone"), seed, "skin_tone"),
        "发型发色": _pick_config_value(hair_options, seed, "hair"),
        "穿搭风格": _pick_config_value(dimensions.get("style"), seed, "style"),
    }


def _select_performance_beat(config: dict[str, Any], segment_type: str) -> dict[str, str]:
    beat_key = str((config.get("beat_by_segment_type") or {}).get(segment_type) or (config.get("beat_by_segment_type") or {}).get(_bank_segment_key(segment_type)) or "observe")
    beat = (config.get("beats") or {}).get(beat_key) or {}
    return {str(key): str(value) for key, value in beat.items()}


def _resolve_persona_context(ctx: SkillContext, category: str, segment_type: str, brief: dict[str, Any], perturbation: dict[str, str]) -> dict[str, Any]:
    factory_config = _load_factory_config(ctx)
    original = _load_original_persona_asset(str(brief.get("product_id") or ""), category)
    if original:
        beat_key = _beat_key_for_segment(factory_config.get("performance_beats") or {}, segment_type)
        persona = _persona_from_original_asset(original.get("asset") or {}, factory_config, category, brief, segment_type, perturbation)
        beat = _beat_from_original_asset(original.get("asset") or {}, beat_key, factory_config)
        if persona and beat:
            return {
                "source": original.get("source") or "original_script_generator",
                "source_product_id": original.get("product_id") or "",
                "source_stage_result_id": original.get("stage_result_id"),
                "source_scope": original.get("scope") or "product",
                "beat_key": beat_key,
                "persona": persona,
                "beat": beat,
                "risk_contract": _risk_contract_from_original_asset(original.get("asset") or {}),
            }
    persona = _select_persona(factory_config.get("persona_pool") or {}, category, brief, segment_type, perturbation)
    beat_key = _beat_key_for_segment(factory_config.get("performance_beats") or {}, segment_type)
    beat = _select_performance_beat(factory_config.get("performance_beats") or {}, segment_type)
    return {
        "source": "config_fallback",
        "source_scope": "fallback",
        "beat_key": beat_key,
        "persona": persona,
        "beat": beat,
        "risk_contract": {},
    }


def _beat_key_for_segment(config: dict[str, Any], segment_type: str) -> str:
    return str((config.get("beat_by_segment_type") or {}).get(segment_type) or (config.get("beat_by_segment_type") or {}).get(_bank_segment_key(segment_type)) or "observe")


def _pick_config_value(value: Any, seed: str, dim: str) -> str:
    options = _list(value)
    if not options:
        return ""
    return options[_stable_index(seed, dim, len(options))]


def _original_script_db_path() -> Path:
    configured = os.environ.get(ORIGINAL_SCRIPT_DB_ENV)
    return Path(configured).expanduser() if configured else DEFAULT_ORIGINAL_SCRIPT_DB


def _load_original_persona_asset(product_id: str, category: str) -> dict[str, Any]:
    db_path = _original_script_db_path()
    if not product_id or not db_path.exists():
        return {}
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT stage_result_id, product_code, output_json
            FROM stage_results
            WHERE stage_name='persona_style_emotion_pack'
              AND status='success'
              AND product_code=?
              AND output_json IS NOT NULL
            ORDER BY stage_result_id DESC
            LIMIT 1
            """,
            (product_id,),
        ).fetchone()
        scope = "product"
        if row is None:
            mapped_keys = _original_category_keys(category)
            row = conn.execute(
                f"""
                SELECT sr.stage_result_id, sr.product_code, sr.output_json
                FROM stage_results sr
                LEFT JOIN pipeline_runs pr ON pr.run_id=sr.run_id
                WHERE sr.stage_name='persona_style_emotion_pack'
                  AND sr.status='success'
                  AND sr.output_json IS NOT NULL
                  AND (
                    pr.product_type IN ({",".join(["?"] * len(mapped_keys))})
                    OR pr.top_category IN ({",".join(["?"] * len(mapped_keys))})
                  )
                ORDER BY sr.stage_result_id DESC
                LIMIT 1
                """,
                (*mapped_keys, *mapped_keys),
            ).fetchone()
            scope = "category"
        conn.close()
        if not row:
            return {}
        asset = json.loads(row["output_json"])
        if not isinstance(asset, dict):
            return {}
        return {
            "source": "original_script_generator",
            "scope": scope,
            "product_id": row["product_code"],
            "stage_result_id": row["stage_result_id"],
            "asset": asset,
        }
    except Exception:
        return {}


def _original_category_keys(category: str) -> list[str]:
    return {
        "earrings": ["耳环", "耳线", "ear_accessory"],
        "hair_accessories": ["发饰", "hair_accessory"],
        "womens_outerwear": ["外套", "上装", "女装", "apparel", "clothing"],
        "scarves_hats": ["围巾", "头巾", "帽子", "apparel_accessory"],
    }.get(category, [category])


def _persona_from_original_asset(
    asset: dict[str, Any],
    factory_config: dict[str, Any],
    category: str,
    brief: dict[str, Any],
    segment_type: str,
    perturbation: dict[str, str],
) -> dict[str, str]:
    # 原创脚本资产只提供人物表现方向，正文仍必须回到混剪片段的5维结构。
    persona = _select_persona(factory_config.get("persona_pool") or {}, category, brief, segment_type, perturbation)
    return _coerce_persona(persona, factory_config)


def _beat_from_original_asset(asset: dict[str, Any], beat_key: str, factory_config: dict[str, Any]) -> dict[str, str]:
    base_beat = _select_performance_beat_by_key(factory_config.get("performance_beats") or {}, beat_key)
    if base_beat:
        return _coerce_beat(base_beat)
    contract = asset.get("human_performance_contract") if isinstance(asset.get("human_performance_contract"), dict) else {}
    index = _beat_index(beat_key)
    expression = _pick_stage_text(contract.get("expression_arc") or asset.get("emotion_progression"), index)
    micro = _pick_stage_text(contract.get("micro_reaction_beats"), index)
    body = _pick_stage_text(contract.get("body_language_beats"), index)
    interaction = _pick_stage_text(contract.get("product_interaction_beats"), index)
    gaze = _gaze_from_original_contract(contract, index)
    return {
        "gaze": gaze,
        "expression": _short_text(expression, 34),
        "micro_reaction": _short_text(micro, 34),
        "body_language": _short_text(body, 34),
        "product_interaction": _short_text(interaction, 34),
    }


def _select_performance_beat_by_key(config: dict[str, Any], beat_key: str) -> dict[str, str]:
    beat = (config.get("beats") or {}).get(beat_key) or {}
    return {str(key): str(value) for key, value in beat.items()}


def _coerce_persona(persona: dict[str, Any], factory_config: dict[str, Any]) -> dict[str, str]:
    pool = factory_config.get("persona_pool") or {}
    dimensions = pool.get("dimensions") or {}
    allowed = {
        "年龄段": set(_list(dimensions.get("age_band"))),
        "肤色": set(_list(dimensions.get("skin_tone"))),
        "发型发色": set(_list(dimensions.get("hair"))),
        "穿搭风格": set(_list(dimensions.get("style"))),
    }
    gender_values: list[str] = []
    for value in (dimensions.get("gender_by_category") or {}).values():
        gender_values.extend(_list(value))
    allowed["性别"] = set(gender_values or ["女", "男", "中性"])
    coerced: dict[str, str] = {}
    for key in ["年龄段", "性别", "肤色", "发型发色", "穿搭风格"]:
        value = str(persona.get(key) or "").strip()
        if value and (not allowed.get(key) or value in allowed[key]):
            coerced[key] = value
    return coerced


def _coerce_beat(beat: dict[str, Any]) -> dict[str, str]:
    return {
        key: _short_text(beat.get(key), 24)
        for key in ["gaze", "expression", "micro_reaction", "body_language", "product_interaction"]
        if str(beat.get(key) or "").strip()
    }


def _risk_contract_from_original_asset(asset: dict[str, Any]) -> dict[str, Any]:
    contract = asset.get("human_performance_contract") if isinstance(asset.get("human_performance_contract"), dict) else {}
    return {
        "forbidden_performance": _list(contract.get("forbidden_performance")),
        "anti_template_warnings": _list(asset.get("anti_template_warnings")),
    }


def _beat_index(beat_key: str) -> int:
    return {"observe": 0, "satisfied": 1, "confirm": 2, "action": 1, "immersive": 0, "mood": 1}.get(beat_key, 0)


def _pick_stage_text(value: Any, index: int) -> str:
    values = _list(value)
    if not values:
        return str(value or "").strip() if value and not isinstance(value, list) else ""
    return values[min(max(index, 0), len(values) - 1)]


def _gaze_from_original_contract(contract: dict[str, Any], index: int) -> str:
    values = _list(contract.get("gaze_plan"))
    if values:
        gaze = values[min(max(index, 0), len(values) - 1)]
        return {
            "mirror": "看镜中整体",
            "camera": "可看镜头",
            "mirror_full_result": "看镜中完整效果",
            "mirror_face_overall": "看镜中脸侧整体",
            "ear_side_detail": "看向耳侧细节",
            "hair_accessory_position": "看向发饰位置",
        }.get(gaze, gaze)
    rule = contract.get("gaze_rule") if isinstance(contract.get("gaze_rule"), dict) else {}
    final_options = _list(rule.get("final_point_options"))
    if final_options and index >= 2:
        return "可看镜头" if "camera" in final_options else final_options[0]
    return ""


def _short_text(value: Any, limit: int) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for sep in ["。", "；", ";", "\n"]:
        if sep in text and text.index(sep) <= limit:
            head = text.split(sep, 1)[0].strip()
            if head:
                return head
    return text if len(text) <= limit else text[:limit].rstrip("，,；; ")


def _risk_negative_layers(ctx: SkillContext, category: str) -> tuple[list[str], list[str]]:
    risk = _load_factory_config(ctx).get("ai_shot_risk") or {}
    registry = risk.get("registry") or {}
    row = registry.get(category) or {}
    l1 = _list(row.get("forbidden")) if risk.get("forbidden_to_negative", True) else []
    l2 = _list(row.get("high_risk")) if risk.get("high_risk_to_negative", True) else []
    return l1, l2


def _sanitize_negative_items(items: list[Any], category: str) -> list[str]:
    sanitized = []
    for item in items:
        text = _sanitize_negative_item(str(item or "").strip(), category)
        if text:
            sanitized.append(text)
    return sanitized


def _sanitize_negative_item(text: str, category: str) -> str:
    if not text:
        return ""
    if _is_face_ban(text):
        if category == "womens_outerwear" and ("全身" in text or "正脸" in text):
            return "全身正面导致版型失真"
        return ""
    return text


def _is_face_ban(text: str) -> bool:
    lowered = str(text or "").lower()
    face_ban_tokens = [
        "ai禁止",
        "禁脸",
        "禁止正脸",
        "不要正脸",
        "不出正脸",
        "不要完整正脸",
        "正脸全身",
        "正脸主导",
        "ai_local时不要正脸",
    ]
    return any(token in lowered or token in text for token in face_ban_tokens)


def _is_overrestrictive_person_item(text: str, category: str) -> bool:
    if category != "womens_outerwear":
        return False
    tokens = ["只允许局部", "局部裁切", "只拍局部", "不要完整上身", "不拍完整上身"]
    return any(token in str(text or "") for token in tokens)


def _is_removed_legacy_phrase(text: str, factory_config: dict[str, Any]) -> bool:
    phrases = _list((factory_config.get("performance_beats") or {}).get("remove_legacy_fixed_phrases"))
    return any(phrase and phrase in str(text or "") for phrase in phrases)


def _person_layer(ctx: SkillContext, category: str, segment_type: str, local_human: dict[str, Any], persona_context: dict[str, Any]) -> str:
    factory_config = _load_factory_config(ctx)
    persona = _coerce_persona(persona_context.get("persona") if isinstance(persona_context.get("persona"), dict) else {}, factory_config)
    beat = _coerce_beat(persona_context.get("beat") if isinstance(persona_context.get("beat"), dict) else {})
    pieces = [
        "人物画像：" + "，".join(f"{key}={value}" for key, value in persona.items() if value),
        "单beat表演：" + "，".join(
            str(beat.get(key) or "")
            for key in ["gaze", "expression", "micro_reaction", "body_language", "product_interaction"]
            if str(beat.get(key) or "").strip()
        ),
    ]
    if category == "womens_outerwear":
        pieces.append("服装片段鼓励完整上身结果镜，保留版型和上身比例")
    extra = [
        _first(local_human.get("gaze_options")),
        _first(local_human.get("micro_behavior_options")),
        _first(local_human.get("body_language_options")),
    ]
    pieces.extend(
        item
        for item in extra
        if item
        and not _is_face_ban(item)
        and not _is_overrestrictive_person_item(item, category)
        and not _is_removed_legacy_phrase(item, factory_config)
    )
    return "；".join(part for part in pieces if part.strip(" ，,；"))


def _motion_arc(perturbation: dict[str, str]) -> str:
    return _cn_arrow(str(perturbation.get("micro_arc") or "开始静止 -> 轻微变化 -> 清楚停留"))


def _join_prompt(parts: list[Any]) -> str:
    return "；".join(_cn_arrow(str(part)).strip(" ，,；") for part in parts if str(part or "").strip())


def _cn_arrow(text: str) -> str:
    return text.replace(" -> ", "，然后").replace("->", "，然后")


def _dedupe(items: list[Any]) -> list[str]:
    cleaned = []
    for item in items:
        text = str(item or "").strip()
        if text and text not in cleaned:
            cleaned.append(text)
    return cleaned


def _post_assembly_validate(ctx: SkillContext, package: dict[str, Any]) -> Result:
    config = _post_validation_config(ctx)
    if not config:
        return Result.ok(package)
    warnings: list[dict[str, Any]] = []
    prompt = dict(package.get("prompt") or {})

    enum_result = _validate_no_enum_leak(prompt, config, warnings)
    if not enum_result.success:
        return enum_result
    prompt = enum_result.data

    motion_result = _validate_motion_arc(prompt, str(package.get("segment_type") or ""), config, warnings)
    if not motion_result.success:
        return motion_result
    prompt = motion_result.data

    l1_result = _validate_l1_negative(ctx, prompt, package, config, warnings)
    if not l1_result.success:
        return l1_result
    prompt = l1_result.data

    anchor_result = _dedup_positive_anchors(prompt, package, warnings)
    if not anchor_result.success:
        return anchor_result
    prompt = anchor_result.data

    scene_result = _validate_scene_space(prompt, str(package.get("segment_type") or ""), config, warnings)
    if not scene_result.success:
        return scene_result
    prompt = scene_result.data

    contract_result = _validate_category_contract_prompt(ctx, prompt, package, config)
    if not contract_result.success:
        return contract_result
    prompt = contract_result.data

    anti_regression_result = _validate_anti_regression(prompt, package, config)
    if not anti_regression_result.success:
        return anti_regression_result

    product_only_result = _validate_product_only_prompt(ctx, prompt, package, config)
    if not product_only_result.success:
        return product_only_result

    prompt = _trim_prompt_package(prompt)
    total_len = len(prompt.get("positive") or "") + len(prompt.get("negative") or "") + len(prompt.get("motion_arc") or "")
    if total_len > MAX_PROMPT_CHARS:
        return Result.fail("PROMPT_TOO_LONG", "prompt cannot be trimmed without cutting L1 negative redlines", {"chars": total_len})
    package["prompt"] = prompt
    package["prompt_validation_warnings"] = warnings
    return Result.ok(package)


def _validate_no_enum_leak(prompt: dict[str, str], config: dict[str, Any], warnings: list[dict[str, Any]]) -> Result:
    reserved = _list(config.get("reserved_enum_tokens"))
    mapping = {str(key): str(value) for key, value in (config.get("enum_to_natural_zh") or {}).items()}
    changed_fields: list[str] = []
    for field in ["positive", "negative", "motion_arc"]:
        text = str(prompt.get(field) or "")
        for token in reserved:
            if token not in text:
                continue
            replacement = mapping.get(token)
            if replacement is None:
                return Result.fail("ENUM_LEAK", "reserved enum token leaked into prompt text", {"field": field, "token": token})
            text = text.replace(token, replacement)
            changed_fields.append(field)
        prompt[field] = _clean_prompt_text(text)
    if changed_fields:
        warnings.append({"code": "enum_token_replaced", "fields": sorted(set(changed_fields))})
    return Result.ok(prompt)


def _validate_category_contract_prompt(ctx: SkillContext, prompt: dict[str, str], package: dict[str, Any], config: dict[str, Any]) -> Result:
    category = _category_alias(str(package.get("category") or ""))
    segment_type = str(package.get("segment_type") or "")
    contract_check = _validate_segment_type_contract(ctx, category, segment_type)
    if not contract_check.success:
        return contract_check
    contract = _category_execution_contract(ctx, category)
    text = "；".join(str(prompt.get(field) or "") for field in ["positive", "negative", "motion_arc"])
    cross_hits = [token for token in _list(contract.get("forbidden_cross_category_words")) if token and token in text]
    if cross_hits:
        return Result.fail("CATEGORY_MOTION_WORD_LEAK", "prompt contains cross-category motion/scene words", {"category": category, "tokens": cross_hits, "segment_type": segment_type})
    perturbation = (package.get("gen_policy") or {}).get("perturbation_seed_group") or {}
    if isinstance(perturbation, dict):
        pools = _load_variable_pools(ctx)
        person_framing = str(package.get("person_framing") or "")
        pool_key = _perturbation_pool_key(category, person_framing, _load_factory_config(ctx))
        pool = pools.get(pool_key) or {}
        invalid = []
        for dim in PERTURBATION_DIMS:
            value = str(perturbation.get(dim) or "").strip()
            if value and value not in _list(pool.get(dim)):
                invalid.append({"dim": dim, "value": value, "pool_key": pool_key})
        if invalid:
            return Result.fail("CATEGORY_PERTURBATION_POOL_MISMATCH", "perturbation values must come from the active category/product-only pool", {"category": category, "invalid": invalid})
    return Result.ok(prompt)


def _validate_motion_arc(prompt: dict[str, str], segment_type: str, config: dict[str, Any], warnings: list[dict[str, Any]]) -> Result:
    expected = str((config.get("motion_arc_by_segment_type") or {}).get(segment_type) or (config.get("motion_arc_by_segment_type") or {}).get(_bank_segment_key(segment_type)) or "")
    if not expected:
        return Result.ok(prompt)
    actual = _classify_motion_arc(str(prompt.get("motion_arc") or ""), config)
    if actual and actual != expected:
        defaults = config.get("motion_arc_defaults") or {}
        replacement = str(defaults.get(expected) or "")
        if replacement:
            prompt["motion_arc"] = _cn_arrow(replacement)
            warnings.append({"code": "fixed_motion_arc", "segment_type": segment_type, "from": actual, "to": expected})
    return Result.ok(prompt)


def _classify_motion_arc(text: str, config: dict[str, Any]) -> str:
    class_tokens = config.get("motion_arc_class_tokens") or {}
    matches: list[str] = []
    for klass, tokens in class_tokens.items():
        if any(str(token) and str(token) in text for token in _list(tokens)):
            matches.append(str(klass))
    if "wearing" in matches:
        return "wearing"
    return matches[0] if matches else ""


def _validate_l1_negative(ctx: SkillContext, prompt: dict[str, str], package: dict[str, Any], config: dict[str, Any], warnings: list[dict[str, Any]]) -> Result:
    expected_l1 = _required_l1_negative(ctx, package, config)
    negative_items = _split_prompt_items(prompt.get("negative") or "")
    missing = [item for item in expected_l1 if item and not any(item in existing for existing in negative_items)]
    if missing:
        return Result.fail("MISSING_L1_NEGATIVE", "negative prompt is missing required L1 redlines", {"missing": missing})
    ordered: list[str] = []
    used: set[int] = set()
    for required in expected_l1:
        for idx, item in enumerate(negative_items):
            if idx not in used and required in item:
                ordered.append(item)
                used.add(idx)
                break
    ordered.extend(item for idx, item in enumerate(negative_items) if idx not in used)
    if ordered != negative_items:
        prompt["negative"] = _join_prompt(ordered)
        warnings.append({"code": "l1_negative_reordered"})
    return Result.ok(prompt)


def _required_l1_negative(ctx: SkillContext, package: dict[str, Any], config: dict[str, Any]) -> list[str]:
    bank = _load_prompt_bank(ctx)
    category = str(package.get("category") or "")
    category_bank = ((bank.get("prompt_bank") or {}).get(category) or {})
    base_l1 = _list(config.get("l1_negative_fixed_ordered")) or _list((bank.get("base") or {}).get("negative_l1_zh")) or DEFAULT_BASE_NEGATIVE_L1
    category_l1_config = _list((config.get("category_l1_negative_fixed_ordered") or {}).get(category))
    category_l1 = category_l1_config or _list(category_bank.get("negative_l1_zh"))
    forbidden_l1, _ = _classify_negative_items(_list((package.get("anchor_ref") or {}).get("forbidden_actions")), category)
    return _dedupe([*base_l1, *category_l1, *forbidden_l1])


def _dedup_positive_anchors(prompt: dict[str, str], package: dict[str, Any], warnings: list[dict[str, Any]]) -> Result:
    positive = str(prompt.get("positive") or "")
    changed = False
    for anchor in _list((package.get("anchor_ref") or {}).get("hard_anchors"))[:2]:
        if len(anchor) < 2:
            continue
        first = positive.find(anchor)
        if first < 0:
            continue
        tail_start = first + len(anchor)
        tail = positive[tail_start:]
        if anchor in tail:
            positive = positive[:tail_start] + tail.replace(anchor, "")
            changed = True
    if changed:
        prompt["positive"] = _clean_prompt_text(positive)
        warnings.append({"code": "dedup_anchor"})
    return Result.ok(prompt)


def _validate_scene_space(prompt: dict[str, str], segment_type: str, config: dict[str, Any], warnings: list[dict[str, Any]]) -> Result:
    conflict_config = config.get("scene_space_conflict") or {}
    if conflict_config.get("enforce_at_post_assembly", True) is False:
        return Result.ok(prompt)
    positive = _scene_space_scan_text(str(prompt.get("positive") or ""))
    matches = _scene_space_matches(positive, config)
    spaces = {item["space"] for item in matches if item["space"] in {"indoor", "outdoor"}}
    if len(spaces) < 2:
        return Result.ok(prompt)
    fixed_space = _fixed_scene_space(segment_type, config)
    keep_space = fixed_space if fixed_space in spaces else _first_scene_space(matches)
    if keep_space not in {"indoor", "outdoor"}:
        return Result.ok(prompt)
    remove_terms = [item["term"] for item in matches if item["space"] in {"indoor", "outdoor"} and item["space"] != keep_space]
    cleaned = positive
    for term in sorted(set(remove_terms), key=len, reverse=True):
        cleaned = cleaned.replace(term, "")
    prompt["positive"] = _clean_prompt_text(cleaned)
    warnings.append({"code": "scene_space_conflict_resolved", "keep_space": keep_space, "removed": sorted(set(remove_terms))})
    return Result.ok(prompt)


def _validate_anti_regression(prompt: dict[str, str], package: dict[str, Any], config: dict[str, Any]) -> Result:
    guard = config.get("anti_regression") or {}
    if guard.get("enabled", True) is False:
        return Result.ok(prompt)
    positive = str(prompt.get("positive") or "")
    reverted_tokens = _list(guard.get("reverted_origin_tokens")) or [
        "来源=原创脚本",
        "人物状态=R3",
        "轻判断型",
        "外观锚点=",
        "发型规则=",
        "穿搭规则=",
        "情绪弧线=",
        "outfit_fit_area",
        "mirror_full_body",
    ]
    hit_tokens = [token for token in reverted_tokens if token and token in positive]
    if hit_tokens:
        return Result.fail(
            "REVERTED_TO_ORIGIN_TEXT",
            "人物画像/beat 退化为原文搬运，必须用精简5维 + 单beat 5字段重生成",
            {"tokens": hit_tokens, "segment_prompt_id": package.get("segment_prompt_id")},
        )
    pattern = str(guard.get("timecode_pattern") or r"\d+\s*-\s*\d+\s*s")
    if re.search(pattern, positive):
        return Result.fail(
            "REVERTED_TO_ORIGIN_TEXT",
            "人物画像/beat 含原创脚本时间码，必须用精简5维 + 单beat 5字段重生成",
            {"pattern": pattern, "segment_prompt_id": package.get("segment_prompt_id")},
        )
    persona_segment = _extract_prompt_segment(positive, "人物画像：")
    garment_tokens = _list(guard.get("garment_in_persona_tokens")) or ["夹克", "外套", "皮质", "拉链", "连衣裙"]
    garment_hits = [token for token in garment_tokens if token and token in persona_segment]
    if garment_hits:
        return Result.fail(
            "REVERTED_TO_ORIGIN_TEXT",
            "人物画像段出现具体服装单品，服装只能由商品锚点描述",
            {"tokens": garment_hits, "segment_prompt_id": package.get("segment_prompt_id")},
        )
    return Result.ok(prompt)


def _validate_product_only_prompt(ctx: SkillContext, prompt: dict[str, str], package: dict[str, Any], config: dict[str, Any]) -> Result:
    if str(package.get("person_framing") or "") != "product_only":
        return Result.ok(prompt)
    segment_type = str(package.get("segment_type") or "")
    if segment_type not in PRODUCT_ONLY_SEGMENT_TYPES:
        return Result.fail("PRODUCT_ONLY_SEGMENT_TYPE_INVALID", "product_only prompt must use product_still/unboxing/flatlay segment_type", {"segment_type": segment_type})
    positive = str(prompt.get("positive") or "")
    tokens = _list((config.get("product_only_guard") or {}).get("person_tokens")) or ["人物画像", "单beat", "看向", "眼神", "侧头", "半步后退", "微笑", "整理商品"]
    if segment_type == "unboxing":
        tokens = [token for token in tokens if token != "手部"]
    hits = [token for token in tokens if token and token in positive]
    if hits:
        return Result.fail("PRODUCT_ONLY_PERSON_LEAK", "product_only 片段混入人物层或人物表演词", {"tokens": hits, "segment_type": segment_type})
    category = _category_alias(str(package.get("category") or ""))
    contract = _category_execution_contract(ctx, category)
    guard = config.get("product_only_guard") or {}
    wear_tokens = _dedupe([*_list(guard.get("wear_effect_tokens")), *_list(contract.get("product_only_wear_effect_tokens"))])
    if segment_type == "unboxing":
        wear_tokens = [token for token in wear_tokens if token not in {"手部", "指尖"}]
    wear_hits = [token for token in wear_tokens if token and token in positive]
    if wear_hits:
        return Result.fail("PRODUCT_ONLY_WEAR_EFFECT_LEAK", "product_only 片段混入佩戴效果或人体部位语境", {"tokens": wear_hits, "segment_type": segment_type, "category": category})
    scene_tokens = _dedupe([*_list(guard.get("scene_tokens")), *_list(contract.get("product_only_scene_tokens"))])
    scene_hits = [token for token in scene_tokens if token and token in positive]
    if scene_hits:
        return Result.fail("PRODUCT_ONLY_SCENE_LEAK", "product_only 片段混入带人场景词", {"tokens": scene_hits, "segment_type": segment_type, "category": category})
    required = _product_only_layer(ctx, str(package.get("category") or ""), segment_type)
    if not required or not any(part and part in positive for part in _split_prompt_items(required)):
        return Result.fail("PRODUCT_ONLY_LAYER_MISSING", "product_only 片段缺少纯物类型层词", {"segment_type": segment_type})
    return Result.ok(prompt)


def _extract_prompt_segment(text: str, prefix: str) -> str:
    for part in str(text or "").split("；"):
        if part.strip().startswith(prefix):
            return part
    return ""


def _scene_space_scan_text(positive: str) -> str:
    parts = []
    for part in str(positive or "").split("；"):
        if "人物画像：" in part or "单beat表演：" in part:
            continue
        parts.append(part)
    return "；".join(parts)


def _sampling_target_space(segment_type: str, seed: str, config: dict[str, Any]) -> str:
    sampling_config = config.get("scene_space_sampling") or {}
    if sampling_config.get("enforce_at_sampling") is False:
        return ""
    fixed = _sampling_space_for_segment(segment_type, config)
    if fixed:
        if fixed == "any":
            return ["indoor", "outdoor"][_stable_index(seed, "scene_space", 2)]
        return fixed
    return ["indoor", "outdoor"][_stable_index(seed, "scene_space", 2)]


def _sampling_space_for_segment(segment_type: str, config: dict[str, Any]) -> str:
    sampling_config = config.get("scene_space_sampling") or {}
    mapping = sampling_config.get("space_by_segment_type") or {}
    if mapping:
        return str(mapping.get(segment_type) or mapping.get(_bank_segment_key(segment_type)) or "").strip()
    return _fixed_scene_space(segment_type, config)


def _fixed_scene_space(segment_type: str, config: dict[str, Any]) -> str:
    mapping = (_post_config_from_any(config).get("segment_type_fixed_space") or {})
    return str(mapping.get(segment_type) or mapping.get(_bank_segment_key(segment_type)) or "").strip()


def _filter_sampling_options(options: list[str], dim: str, target_space: str, config: dict[str, Any]) -> list[str]:
    if dim not in {"time_light", "props_env"} or target_space not in {"indoor", "outdoor"}:
        return options
    filtered = []
    for option in options:
        spaces = _spaces_for_text(option, config)
        if dim == "props_env" and spaces == {target_space}:
            filtered.append(option)
        elif dim == "time_light" and spaces and spaces.issubset({target_space, "neutral"}):
            filtered.append(option)
    return filtered or options


def _scene_space_matches(text: str, config: dict[str, Any]) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for space, terms in _scene_space_terms(config).items():
        if space == "neutral":
            continue
        for term in terms:
            start = text.find(term)
            while start >= 0:
                matches.append({"space": space, "term": term, "start": start})
                start = text.find(term, start + len(term))
    return sorted(matches, key=lambda item: item["start"])


def _first_scene_space(matches: list[dict[str, Any]]) -> str:
    for item in matches:
        if item["space"] in {"indoor", "outdoor"}:
            return str(item["space"])
    return ""


def _spaces_for_text(text: str, config: dict[str, Any]) -> set[str]:
    spaces: set[str] = set()
    for space, terms in _scene_space_terms(config).items():
        if any(term and term in str(text or "") for term in terms):
            spaces.add(space)
    return spaces


def _scene_space_terms(config: dict[str, Any]) -> dict[str, list[str]]:
    lexicon = _post_config_from_any(config).get("scene_light_lexicon") or {}
    terms_by_space: dict[str, list[str]] = {}
    for space, group in lexicon.items():
        terms: list[str] = []
        for values in (group or {}).values():
            terms.extend(_list(values))
        terms_by_space[str(space)] = sorted(_dedupe(terms), key=len, reverse=True)
    return terms_by_space


def _post_config_from_any(config: dict[str, Any]) -> dict[str, Any]:
    return config.get("post_assembly_validation") or config


def _split_prompt_items(text: str) -> list[str]:
    normalized = str(text or "").replace("\n", "；").replace(";", "；")
    return [part.strip(" ，,；") for part in normalized.split("；") if part.strip(" ，,；")]


def _trim_prompt_package(prompt: dict[str, str]) -> dict[str, str]:
    positive = str(prompt.get("positive") or "")
    negative = str(prompt.get("negative") or "")
    motion_arc = str(prompt.get("motion_arc") or "")
    total_len = len(positive) + len(negative) + len(motion_arc)
    if total_len <= MAX_PROMPT_CHARS:
        return prompt
    budget = MAX_PROMPT_CHARS - len(negative) - len(motion_arc)
    if budget > 0:
        prompt["positive"] = _clean_prompt_text(positive[:budget])
    return prompt


def _clean_prompt_text(text: str) -> str:
    cleaned = str(text or "")
    for old, new in [("//", "/"), ("，，", "，"), ("；；", "；"), ("，；", "；"), ("；，", "；"), ("、，", "，"), ("，、", "，")]:
        while old in cleaned:
            cleaned = cleaned.replace(old, new)
    return cleaned.strip(" ，,；、")


def _list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item or "").strip()]
    text = str(value).strip()
    return [text] if text else []


def _first(value: Any) -> str:
    values = _list(value)
    return values[0] if values else ""
