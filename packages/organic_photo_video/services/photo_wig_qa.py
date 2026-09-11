"""MX wig QA: deterministic checks, group-level visual verdicts, es-MX copy.

MX-only module.  Design limits from the handoff: free technical checks are
deterministic; there is exactly ONE group-level visual check per generated
group (one targeted repair, one re-check); final-page checks reuse the
existing rendering technical inspection plus a thin deterministic MX layer.
No thresholds here invent face scores or platform traffic predictions.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

from domain.photo_contracts import validate_copy
from services.locale_quality import copy_locale_issues
from services.photo_request_factory import fingerprint
from services.photo_wig_flow import MX_WIG_SOURCE_ROLES


class PhotoWigQaError(ValueError):
    pass


WIG_GROUP_QA_SCHEMA_VERSION = "opv-wig-group-qa-v1"

_HAIR_DISTINCT_KEYS = ("length", "texture", "color", "parting", "silhouette")
_OVERLAY_TEXT_MAX_CHARS = 28
_TITLE_MAX_CHARS = 72
_LETTER_PREFIX = {
    "hair_b": "B · ",
    "hair_c": "C · ",
    "hair_d": "D · ",
}
_CTA_MARKER = "¿A, B, C o D?"


# ---------------------------------------------------------------------------
# Plan/copy deterministic checks
# ---------------------------------------------------------------------------


def check_wig_plan(plan_item: Mapping[str, Any]) -> List[str]:
    """Deterministic plan/copy contract for one wig-choice post item."""
    errors: List[str] = []
    options = list(plan_item.get("options") or [])
    roles = [str(item.get("role") or "") for item in options]
    if roles != list(MX_WIG_SOURCE_ROLES):
        errors.append(f"options roles must be {list(MX_WIG_SOURCE_ROLES)}, got {roles}")
        return errors
    signatures = {
        tuple(str(item.get(key) or "") for key in _HAIR_DISTINCT_KEYS)
        for item in options
    }
    if len(signatures) < len(options):
        errors.append("四个发型选项存在实质重复（长度/卷度/发色/分缝/轮廓完全相同）")
    for option in options:
        if not str(option.get("label_es") or "").strip():
            errors.append(f"{option.get('role')} 缺少 label_es")
        if len(str(option.get("label_es") or "")) > 22:
            errors.append(f"{option.get('role')} label_es 超过 22 字符")
    copy_block = dict(plan_item.get("copy") or {})
    errors.extend(validate_copy(copy_block, expected_slide_count=4))
    errors.extend(copy_locale_issues(copy_block, "es-MX"))
    title = str(copy_block.get("title") or "").strip()
    if len(title) > _TITLE_MAX_CHARS:
        errors.append(f"title 超过 {_TITLE_MAX_CHARS} 字符")
    slide_texts = [str(value) for value in copy_block.get("slide_texts") or []]
    if len(slide_texts) == 4:
        if not slide_texts[0].rstrip().endswith("A"):
            errors.append("slide_texts[0] 必须以小号 A 标识结尾（… · A）")
        for role, index in (("hair_b", 1), ("hair_c", 2), ("hair_d", 3)):
            if not slide_texts[index].startswith(_LETTER_PREFIX[role]):
                errors.append(f"slide_texts[{index + 1}] 必须以 “{_LETTER_PREFIX[role]}” 开头")
        if _CTA_MARKER not in slide_texts[3]:
            errors.append("slide_texts[3] 必须包含选择 CTA（¿A, B, C o D?）")
        for index, text in enumerate(slide_texts, 1):
            if len(text) > _OVERLAY_TEXT_MAX_CHARS:
                errors.append(
                    f"slide_texts[{index}] 超过 {_OVERLAY_TEXT_MAX_CHARS} 字符（当前 {len(text)}）"
                )
    return errors


def check_wig_sources(sources: Sequence[Mapping[str, Any]]) -> List[str]:
    """Free technical checks over the four generated sources."""
    errors: List[str] = []
    roles = [str(item.get("role") or "") for item in sources]
    if roles != list(MX_WIG_SOURCE_ROLES):
        errors.append(f"sources roles must be {list(MX_WIG_SOURCE_ROLES)}, got {roles}")
        return errors
    hashes = []
    for item in sources:
        path = Path(str(item.get("path") or ""))
        digest = str(item.get("sha256") or "")
        if not path.is_file() or not digest:
            errors.append(f"{item.get('role')} 文件缺失或没有哈希")
            continue
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != digest:
            errors.append(f"{item.get('role')} 文件内容与哈希不一致")
        hashes.append(actual)
    if len(set(hashes)) != len(hashes):
        errors.append("四张源图存在重复文件，不能冒充四个不同发型")
    return errors


def check_wig_rendered_package(
    package_manifest: Mapping[str, Any], plan_item: Mapping[str, Any]
) -> List[str]:
    """Thin MX layer over the existing rendering technical inspection.

    The exporter already enforces counts/indexes/font-fit/sha invariance; this
    verifies the MX semantics of the frozen output: 4 pages in A→D order, page
    texts equal the frozen es-MX slide_texts, page 1 keeps the title+A mark and
    page 4 keeps the CTA.  It never runs another visual model pass.
    """
    errors: List[str] = []
    slides = list(package_manifest.get("slides") or [])
    if len(slides) != 4:
        errors.append(f"成片必须 4 页，当前 {len(slides)}")
        return errors
    if [int(item.get("index") or 0) for item in slides] != [1, 2, 3, 4]:
        errors.append("成片页序必须是 1..4（A→D）")
    copy_block = dict(package_manifest.get("copy") or {})
    plan_copy = dict(plan_item.get("copy") or {})
    for key in ("title", "caption"):
        if str(copy_block.get(key) or "") != str(plan_copy.get(key) or ""):
            errors.append(f"成片 {key} 与冻结计划不一致")
    if [str(value) for value in copy_block.get("slide_texts") or []] != [
        str(value) for value in plan_copy.get("slide_texts") or []
    ]:
        errors.append("成片页文字与冻结 slide_texts 不一致")
    return errors


def check_wig_batch_sources(source_groups: Sequence[Sequence[Mapping[str, Any]]]) -> None:
    """Cross-item uniqueness: two posts may not ship the identical 4 images."""
    signatures: set = set()
    for index, sources in enumerate(source_groups, 1):
        signature = tuple(str(item.get("sha256") or "") for item in sources)
        if signature in signatures:
            raise PhotoWigQaError(f"第 {index} 篇与前一篇使用了完全相同的四张发型素材")
        signatures.add(signature)


def font_glyph_issues(layout_snapshot: Mapping[str, Any],
                      texts: Sequence[str]) -> List[str]:
    """Every es-MX glyph (accents, ¿¡) must exist in the configured font."""
    candidates = [
        str(value) for value in (layout_snapshot.get("render_options") or {}).get(
            "font_candidates") or []
    ]
    if not candidates:
        return ["布局缺少 font_candidates，无法校验西语字形"]
    needed = {char for text in texts for char in str(text)}
    from PIL import ImageFont

    for candidate in candidates:
        path = Path(candidate)
        if not path.is_file():
            continue
        try:
            font = ImageFont.truetype(str(path), 40)
        except OSError:
            continue
        missing = {
            char for char in needed
            if char.strip() and not font.getmask(char).getbbox()
        }
        if missing:
            return [f"字体 {path.name} 缺少西语字形：{''.join(sorted(missing))}"]
        return []
    return ["font_candidates 中没有可用字体"]


# ---------------------------------------------------------------------------
# Normalizers for model verdicts (strict; no default pass)
# ---------------------------------------------------------------------------


def normalize_wig_group_qa(raw: Any, *, roles: Sequence[str]) -> Dict[str, Any]:
    """Strictly normalize the group visual verdict; failures carry evidence."""
    if not isinstance(raw, Mapping):
        raise PhotoWigQaError("假发组级质检结果必须是 JSON 对象")
    entries = raw.get("roles")
    if not isinstance(entries, list):
        raise PhotoWigQaError("假发组级质检缺少 roles 列表")
    by_role: Dict[str, Any] = {}
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise PhotoWigQaError("roles 条目必须是对象")
        role = str(entry.get("role") or "")
        if role not in roles:
            raise PhotoWigQaError(f"质检结果包含未知角色 {role!r}")
        passed = entry.get("passed")
        if not isinstance(passed, bool):
            raise PhotoWigQaError(f"{role} 的 passed 必须是布尔值")
        flags = [str(value) for value in entry.get("hard_flags") or [] if str(value)]
        notes = str(entry.get("notes") or "")
        by_role[role] = {"passed": passed, "hard_flags": flags, "notes": notes}
    missing = [role for role in roles if role not in by_role]
    if missing:
        raise PhotoWigQaError(f"假发组级质检缺少页面结果：{'、'.join(missing)}")
    known_flags = {
        "face_changed", "deformity", "hair_artifact", "hairstyle_mismatch",
        "duplicate_of",
    }
    failed_roles: List[str] = []
    issues: Dict[str, List[str]] = {}
    for role in roles:
        verdict = by_role[role]
        problems = [flag for flag in verdict["hard_flags"] if flag in known_flags]
        if problems:
            verdict["passed"] = False
        if not verdict["passed"]:
            failed_roles.append(role)
            issues[role] = problems or ["qa_failed_without_flag"]
            if not problems:
                issues[role].append(verdict["notes"] or "未给出硬伤标记但 passed=false")
    return {
        "schema_version": WIG_GROUP_QA_SCHEMA_VERSION,
        "passed": not failed_roles,
        "failed_roles": failed_roles,
        "issues": issues,
        "roles": by_role,
        "group_notes": str(raw.get("group_notes") or ""),
    }


def normalize_wig_copy_review(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise PhotoWigQaError("西语文案审校结果必须是 JSON 对象")
    passed = raw.get("passed")
    if not isinstance(passed, bool):
        raise PhotoWigQaError("西语文案审校 passed 必须是布尔值")
    issues = [str(value) for value in raw.get("issues") or [] if str(value)]
    return {"passed": passed, "issues": issues, "notes": str(raw.get("notes") or "")}


# ---------------------------------------------------------------------------
# Reviewer facade used by the supply service
# ---------------------------------------------------------------------------


class WigGroupQaReviewer:
    """Bridges PhotoWigSupplyService to the vision service MX methods."""

    def __init__(self, vision_service: Any, copy_review: bool = True):
        self.vision = vision_service
        self.copy_review = copy_review

    def review_group(self, *, persona_reference_path: str,
                     sources: Sequence[Mapping[str, Any]],
                     plan_item: Mapping[str, Any]) -> Dict[str, Any]:
        return self.vision.review_wig_group(
            persona_image_path=persona_reference_path,
            sources=sources, plan_item=plan_item,
        )

    def review_copy(self, *, plan_item: Mapping[str, Any]) -> Dict[str, Any]:
        if not self.copy_review:
            return {"passed": True, "issues": [], "notes": "copy review disabled"}
        return self.vision.review_wig_copy_semantics(
            copy_block=dict(plan_item.get("copy") or {}), plan_item=plan_item,
        )
