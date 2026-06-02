from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime
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
GRADE_VARIANTS = {"A": 4, "B": 2, "C": 2}
PERTURBATION_DIMS = ["camera_motion", "time_light", "composition", "color_tone", "props_env", "micro_arc"]
MAX_PROMPT_CHARS = 1600
RICH_PERTURBATION_CATEGORIES = {"womens_outerwear", "earrings"}


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
        if category == "womens_outerwear" and person_framing == "ai_full_face":
            return Result.fail(
                "AI_FULL_FACE_FORBIDDEN",
                "womens_outerwear prompts must use ai_local instead of ai_full_face",
                {"product_id": brief.get("product_id"), "template_id": template_slot.get("template_id")},
            )

        prompt_variables = _load_prompt_variables_config(self.ctx)
        pools = prompt_variables.get("variable_pools") or {}
        dedup_config = prompt_variables.get("dedup") or {}
        pool_validation = _validate_perturbation_pool(category, pools, dedup_config)
        if not pool_validation.success:
            return pool_validation
        perturbation = _choose_perturbation(brief, template_slot, pools, batch_seen if batch_seen is not None else set())
        hard_anchors = _list(brief.get("hard_anchors"))
        forbidden_actions = _list(brief.get("forbidden_actions")) or _list(brief.get("must_not_show"))
        key_constraints = _list(brief.get("key_visual_constraints")) or _list(brief.get("must_show"))
        prompt_result = _build_prompt_from_bank(self.ctx, category, segment_type, grade, brief, local_human, person_framing, perturbation)
        if not prompt_result.success:
            return prompt_result
        prompt = prompt_result.data

        segment_prompt_id = str(uuid.uuid4())
        package = {
            "segment_prompt_id": segment_prompt_id,
            "segment_script_id": _segment_script_id(segment_prompt_id),
            "product_id": brief.get("product_id"),
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
    segment_key = _bank_segment_key(segment_type)
    row = (category_bank.get("segments") or {}).get(segment_key)
    if not row:
        return Result.fail("PROMPT_BANK_ROW_MISSING", "prompt bank row is missing", {"category": category, "segment_type": segment_type, "segment_key": segment_key})

    selected_anchors = _select_key_anchors(brief, 1 if grade == "C" else 2)
    key_anchor = "、".join(selected_anchors) or str(brief.get("display_family") or category_bank.get("family_word") or "商品")
    family_word = _family_word(category_bank, brief)
    positive_core = _fill_prompt_template(str(row.get("positive_zh") or ""), key_anchor, family_word)
    positive_parts = [
        str((bank.get("base") or {}).get("positive_prefix") or DEFAULT_BASE_POSITIVE),
        positive_core,
        _grade_positive_suffix(grade),
        _person_layer(person_framing, local_human),
        _perturbation_layer(perturbation),
    ]
    negative_l1, negative_l2 = _negative_layers(bank, category_bank, category, brief, local_human)
    motion_arc = _cn_arrow(_motion_from_bank(row, grade) or str(perturbation.get("micro_arc") or "开始静止 -> 轻微变化 -> 清楚停留"))
    prompt = _assemble_prompt(positive_parts, negative_l1, negative_l2, motion_arc)
    prompt_validation = _validate_prompt_text(prompt, category, segment_key, grade, selected_anchors, negative_l1)
    if not prompt_validation.success:
        return prompt_validation
    return Result.ok(prompt)


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
            if "正脸全身" not in negative_l1 or "前后帧衣服不一致" not in negative_l1:
                return Result.fail("PROMPT_BANK_OUTERWEAR_L1_MISSING", "womens_outerwear L1 must include 正脸全身 and 前后帧衣服不一致")
            if "错误廓形" not in negative_l2 or "错误衣长" not in negative_l2:
                return Result.fail("PROMPT_BANK_OUTERWEAR_L2_MISSING", "womens_outerwear L2 must include key mismatch details")
    return Result.ok()


def _bank_segment_key(segment_type: str) -> str:
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


def _grade_positive_suffix(grade: str) -> str:
    if grade == "A":
        return "产品清晰为主体，对焦在产品上"
    if grade == "C":
        return "重氛围，产品可不在画面中心"
    return ""


def _motion_from_bank(row: dict[str, Any], grade: str) -> str:
    arcs = row.get("motion_arc_zh") or {}
    return str(arcs.get(grade) or arcs.get("B") or arcs.get("A") or "")


def _negative_layers(bank: dict[str, Any], category_bank: dict[str, Any], category: str, brief: dict[str, Any], local_human: dict[str, Any]) -> tuple[list[str], list[str]]:
    policy = bank.get("negative_policy") or {}
    l2_limit = _positive_int(policy.get("l2_max_items"), 6)
    base_l1 = _list((bank.get("base") or {}).get("negative_l1_zh")) or _list((bank.get("base") or {}).get("negative_required")) or DEFAULT_BASE_NEGATIVE_L1
    category_l1 = _list(category_bank.get("negative_l1_zh"))
    category_l2 = _list(category_bank.get("negative_l2_zh")) or _list(category_bank.get("negative_add_zh"))
    brief_l1, brief_l2 = _classify_negative_items([
        *_list(brief.get("must_not_show")),
        *_list(brief.get("forbidden_actions")),
        *_list(local_human.get("forbidden_performance")),
    ])
    l1 = _dedupe([*base_l1, *category_l1, *brief_l1])
    l2 = _dedupe([*brief_l2, *category_l2])[:l2_limit]
    return l1, l2


def _classify_negative_items(items: list[str]) -> tuple[list[str], list[str]]:
    redline_tokens = ["不要切镜", "不要水印", "不要字幕", "不要文字", "竞品", "正脸全身", "商品变形", "错品类", "前后帧", "不一致"]
    l1: list[str] = []
    l2: list[str] = []
    for item in items:
        text = str(item or "").strip()
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


def _compact_prompt(text: str) -> str:
    compacted = "；".join(part.strip(" ；") for part in str(text or "").split("；") if part.strip(" ；"))
    return compacted if len(compacted) <= MAX_PROMPT_CHARS else compacted[:MAX_PROMPT_CHARS].rstrip("，；、 ")


def _contains_cjk(text: str) -> bool:
    return any("\u4e00" <= char <= "\u9fff" for char in str(text or ""))


def _validate_prompt_text(prompt: dict[str, str], category: str, segment_key: str, grade: str, selected_anchors: list[str], negative_l1: list[str]) -> Result:
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
    if category == "earrings" and segment_key == "product_display" and grade == "A" and "微距" not in positive:
        return Result.fail("EARRING_A_MACRO_REQUIRED", "earrings A product_display prompt must include 微距")
    if category == "womens_outerwear" and "正脸全身" not in negative:
        return Result.fail("OUTERWEAR_NEGATIVE_REQUIRED", "womens_outerwear negative prompt must include 正脸全身")
    return Result.ok()


def _validate_package(package: dict[str, Any]) -> Result:
    if not package.get("segment_prompt_id"):
        return Result.fail("PROMPT_ID_REQUIRED", "segment_prompt_id is required")
    if not str(package.get("segment_script_id") or "").startswith("SPK-"):
        return Result.fail("SEGMENT_SCRIPT_ID_REQUIRED", "segment_script_id is required")
    if not package["anchor_ref"]["hard_anchors"]:
        return Result.fail("HARD_ANCHORS_REQUIRED", "anchor_ref.hard_anchors is required")
    if package["category"] == "womens_outerwear" and package["person_framing"] == "ai_full_face":
        return Result.fail("AI_FULL_FACE_FORBIDDEN", "womens_outerwear prompts cannot use ai_full_face")
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


def _choose_perturbation(brief: dict[str, Any], slot: dict[str, Any], pools: dict[str, Any], seen: set[str]) -> dict[str, str]:
    category = str(brief.get("category") or "generic_fashion")
    pool = pools.get(category) or pools.get(_category_alias(category)) or pools.get("generic_fashion") or {}
    salt_base = json.dumps({"product_id": brief.get("product_id"), "segment_type": slot.get("segment_type"), "slot_index": slot.get("slot_index")}, sort_keys=True, ensure_ascii=False)
    for attempt in range(200):
        seed = f"{salt_base}:{attempt}"
        values = {}
        for dim in PERTURBATION_DIMS:
            options = _list(pool.get(dim)) or [dim.replace("_", " ")]
            values[dim] = options[_stable_index(seed, dim, len(options))]
        key = json.dumps(values, sort_keys=True, ensure_ascii=False)
        if key not in seen:
            seen.add(key)
            return values
    return values


def _validate_perturbation_pool(category: str, pools: dict[str, Any], dedup_config: dict[str, Any]) -> Result:
    normalized = _category_alias(category)
    if normalized not in RICH_PERTURBATION_CATEGORIES:
        return Result.ok()
    pool = pools.get(normalized) or {}
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


def _pool_combo_count(pool: dict[str, Any]) -> int:
    total = 1
    for dim in PERTURBATION_DIMS:
        total *= max(1, len(_list(pool.get(dim))))
    return total


def _lock_character_ref(grade: str, person_framing: str) -> bool:
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
    if grade == "A":
        return "ai_local"
    value = str(slot.get("person_framing") or "").strip()
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


def _person_layer(person_framing: str, local_human: dict[str, Any]) -> str:
    if person_framing == "real_preferred":
        return ""
    if person_framing == "ai_full_face":
        return "保持同一个人物身份和脸部一致性，锁定角色参考"
    pieces = [
        _first(local_human.get("gaze_options")),
        _first(local_human.get("micro_behavior_options")),
        _first(local_human.get("body_language_options")),
        "只允许局部身体入镜，不要完整正脸，优先背影、侧面、手部、衣服局部或配饰局部",
    ]
    return "，".join(piece for piece in pieces if piece)


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
