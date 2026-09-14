"""Pure validation shared by photo configuration, planning and preflight."""
from __future__ import annotations

from typing import Any, Mapping
import re


LABEL_PLACEHOLDER = re.compile(r"\{\{label_([a-d])\}\}")
TIKTOK_PHOTO_TITLE_MAX_UTF16 = 90
TIKTOK_PHOTO_DESCRIPTION_MAX_UTF16 = 4000


def _utf16_units(value: str) -> int:
    return len(str(value or "").encode("utf-16-le")) // 2


def clamp_utf16(value: str, limit: int, *, boundary: str = " ") -> str:
    """Trim machine-authored text to ``limit`` UTF-16 units, at a word boundary.

    Never splits a surrogate pair (the unit width of each character is measured
    as it is consumed).  After cutting, the tail is backed up to the last space
    so a sentence is not left severed mid-phrase.
    """
    text = str(value or "").strip()
    if limit <= 0 or _utf16_units(text) <= limit:
        return text
    used = 0
    kept: list[str] = []
    for char in text:
        width = len(char.encode("utf-16-le")) // 2
        if used + width > limit:
            break
        kept.append(char)
        used += width
    clipped = "".join(kept).rstrip()
    if boundary and boundary in clipped:
        head = clipped.rsplit(boundary, 1)[0].rstrip()
        if head:
            clipped = head
    return clipped.rstrip(" ,;:·-–—…")


def normalize_publish_copy(copy_block: Any) -> Any:
    """Clamp model-authored copy into the TikTok publish contract.

    Only fields a language model writes freely are clamped (``title``, and
    ``caption`` when caption + hashtags overflow).  Human-authored configuration
    — copy packs, review templates — stays strict on purpose: a config error
    must keep failing loudly instead of being silently trimmed.
    """
    if not isinstance(copy_block, Mapping):
        return copy_block
    normalized = dict(copy_block)
    title = normalized.get("title")
    if isinstance(title, str):
        normalized["title"] = clamp_utf16(title, TIKTOK_PHOTO_TITLE_MAX_UTF16)
    caption = normalized.get("caption")
    hashtags = normalized.get("hashtags")
    if isinstance(caption, str) and isinstance(hashtags, list):
        tags = " ".join(str(tag).strip() for tag in hashtags if isinstance(tag, str))
        description = " ".join(part for part in (caption.strip(), tags) if part)
        if _utf16_units(description) > TIKTOK_PHOTO_DESCRIPTION_MAX_UTF16:
            # Hashtags carry the reach signal, so the caption absorbs the cut.
            budget = TIKTOK_PHOTO_DESCRIPTION_MAX_UTF16 - _utf16_units(tags) - (1 if tags else 0)
            normalized["caption"] = clamp_utf16(caption, budget)
    return normalized


def planned_copy_contract_errors(copy_block: Any) -> list[str]:
    """Publish-contract checks that must already hold on a *planned* copy.

    Deliberately narrower than :func:`validate_copy`: a planned copy is still
    incomplete, and the theme legitimately supplies the fields the model left
    out (caption, hashtags) plus the assembled ``slide_texts``.  Checking the
    publish-length limits here lets an over-long machine-written title be
    rejected — or clamped — *before* the paid asset stage, instead of at freeze
    time once four images have already been billed.
    """
    if not isinstance(copy_block, Mapping):
        return ["copy must be an object"]
    errors: list[str] = []
    title = copy_block.get("title")
    if title is not None:
        if not isinstance(title, str) or not title.strip():
            errors.append("copy.title must be a non-empty localized string")
        elif _utf16_units(title.strip()) > TIKTOK_PHOTO_TITLE_MAX_UTF16:
            errors.append(
                f"copy.title exceeds TikTok's {TIKTOK_PHOTO_TITLE_MAX_UTF16} UTF-16 unit limit"
            )
    caption = copy_block.get("caption")
    if caption is not None and (not isinstance(caption, str) or not caption.strip()):
        errors.append("copy.caption must be a non-empty localized string")
    hashtags = copy_block.get("hashtags")
    if hashtags is not None:
        if not isinstance(hashtags, list) or not all(
                isinstance(tag, str) and tag.startswith("#") and len(tag) > 1
                for tag in hashtags):
            errors.append("copy.hashtags must be a list of hashtag strings")
        elif isinstance(caption, str):
            description = " ".join(part for part in (
                caption.strip(),
                " ".join(str(tag).strip() for tag in hashtags),
            ) if part)
            if _utf16_units(description) > TIKTOK_PHOTO_DESCRIPTION_MAX_UTF16:
                errors.append(
                    f"copy caption and hashtags exceed TikTok's "
                    f"{TIKTOK_PHOTO_DESCRIPTION_MAX_UTF16} UTF-16 unit limit"
                )
    return errors


def placeholder_errors(value: Any, *, allow_labels: bool = False) -> list[str]:
    """Templates may contain only the four known labels; frozen copy has none."""
    if isinstance(value, str):
        remainder = LABEL_PLACEHOLDER.sub("", value) if allow_labels else value
        return ["copy contains an unresolved or unsupported placeholder"] if "{{" in remainder or "}}" in remainder else []
    if isinstance(value, Mapping):
        return [error for item in value.values() for error in placeholder_errors(item, allow_labels=allow_labels)]
    if isinstance(value, list):
        return [error for item in value for error in placeholder_errors(item, allow_labels=allow_labels)]
    return []


def _equal(left: Any, right: Any) -> bool:
    return type(left) is type(right) and left == right


def validate_variables(schema: Mapping[str, Any], values: Mapping[str, Any]) -> list[str]:
    errors = []
    if not isinstance(schema, Mapping) or not isinstance(values, Mapping):
        return ["variables schema and values must be objects"]
    errors.extend(f"unknown recipe variable {key!r}" for key in values if key not in schema)
    for key, rule in schema.items():
        if not isinstance(rule, Mapping):
            errors.append(f"variable {key!r} rule must be an object")
            continue
        kind = rule.get("type", "enum" if "enum" in rule else "string")
        allowed = rule.get("values", rule.get("enum"))
        if kind not in {"enum", "set", "fixed_set", "string", "integer", "number", "boolean"}:
            errors.append(f"variable {key!r} has unsupported type {kind!r}")
            continue
        if kind in {"enum", "set", "fixed_set"} and (
            not isinstance(allowed, list) or not allowed
            or any(not isinstance(v, (str, int, float, bool)) or v is None for v in allowed)
            or any(_equal(v, prior) for i, v in enumerate(allowed) for prior in allowed[:i])
        ):
            errors.append(f"variable {key!r} requires unique scalar values")
            continue
        if key not in values:
            if rule.get("required"):
                errors.append(f"recipe variable {key!r} is required")
            continue
        value = values[key]
        valid = True
        if kind == "enum":
            valid = any(_equal(value, option) for option in allowed)
        elif kind in {"set", "fixed_set"}:
            valid = isinstance(value, list) and bool(value)
            if valid:
                valid = (all(any(_equal(v, option) for option in allowed) for v in value)
                         and not any(_equal(v, prior) for i, v in enumerate(value) for prior in value[:i]))
                if kind == "fixed_set":
                    valid = valid and len(value) == len(allowed)
        elif kind == "string":
            valid = isinstance(value, str) and bool(value.strip())
        elif kind == "integer":
            valid = type(value) is int
        elif kind == "number":
            valid = type(value) in {int, float}
        elif kind == "boolean":
            valid = type(value) is bool
        if not valid:
            errors.append(f"recipe variable {key!r} violates {kind} allowed values/type")
    return errors


def validate_copy(copy_block: Any, *, allow_placeholders: bool = False,
                  expected_slide_count: int = 5) -> list[str]:
    if not isinstance(copy_block, Mapping):
        return ["copy must be an object"]
    errors = placeholder_errors(copy_block, allow_labels=allow_placeholders)
    for key in ("title", "caption"):
        if not isinstance(copy_block.get(key), str) or not copy_block[key].strip():
            errors.append(f"copy.{key} must be a non-empty localized string")
    hashtags = copy_block.get("hashtags")
    if not isinstance(hashtags, list) or not all(isinstance(v, str) and v.startswith("#") and len(v) > 1 for v in hashtags):
        errors.append("copy.hashtags must be a list of hashtag strings")
    texts = copy_block.get("slide_texts")
    if (not isinstance(texts, list) or len(texts) != expected_slide_count
            or not all(isinstance(v, str) and v.strip() for v in texts)):
        errors.append(
            f"localized copy.slide_texts must contain {expected_slide_count} strings"
        )
    title = copy_block.get("title")
    if isinstance(title, str) and _utf16_units(title.strip()) > TIKTOK_PHOTO_TITLE_MAX_UTF16:
        errors.append(f"copy.title exceeds TikTok's {TIKTOK_PHOTO_TITLE_MAX_UTF16} UTF-16 unit limit")
    if isinstance(copy_block.get("caption"), str) and isinstance(hashtags, list):
        description = " ".join(
            part for part in (
                copy_block["caption"].strip(),
                " ".join(str(tag).strip() for tag in hashtags if isinstance(tag, str)),
            ) if part
        )
        if _utf16_units(description) > TIKTOK_PHOTO_DESCRIPTION_MAX_UTF16:
            errors.append(
                f"copy caption and hashtags exceed TikTok's {TIKTOK_PHOTO_DESCRIPTION_MAX_UTF16} UTF-16 unit limit"
            )
    return errors


def validate_execution_profiles(spec: Mapping[str, Any], *, require_profiles: bool = True) -> list[str]:
    errors = []
    requirements = spec.get("asset_requirements")
    profiles = spec.get("execution_profiles")
    if not require_profiles and requirements is None and profiles is None:
        return []  # Legacy explicit requests remain readable.
    # Only the explicit mx_wig_choice_v1 flow freezes four-slide copy; every
    # other spec keeps the five-slide default untouched.
    expected_slides = 4 if spec.get("execution_flow") == "mx_wig_choice_v1" else 5
    if not isinstance(requirements, Mapping):
        return ["asset_requirements must be an object"]
    if not isinstance(requirements.get("required_tags", {}), Mapping):
        errors.append("asset_requirements.required_tags must be an object")
    if not isinstance(requirements.get("required_pairs", []), list):
        errors.append("asset_requirements.required_pairs must be a list")
    match_keys = spec.get("asset_match_keys")
    if (not isinstance(match_keys, list) or not all(isinstance(k, str) and k in (spec.get("variables_schema") or {}) for k in match_keys)
            or len(set(match_keys)) != len(match_keys)):
        errors.append("asset_match_keys must explicitly name unique visual variables")
    roles = requirements.get("required_roles")
    if not isinstance(roles, list) or not roles or not all(isinstance(v, str) and v for v in roles) or len(set(roles)) != len(roles):
        errors.append("asset_requirements.required_roles must contain unique ordered source roles")
    for pair in requirements.get("required_pairs") if isinstance(requirements.get("required_pairs"), list) else []:
        if (not isinstance(pair, Mapping) or not isinstance(pair.get("relation"), str)
                or not isinstance(pair.get("roles"), list) or len(pair["roles"]) < 2
                or any(role not in (roles or []) for role in pair["roles"])):
            errors.append("asset_requirements.required_pairs must reference declared roles and relation")
    if not isinstance(profiles, list) or not profiles:
        return errors + ["execution_profiles must contain at least one content plan"]
    seen = set()
    for profile in profiles:
        if not isinstance(profile, Mapping):
            errors.append("execution profile must be an object")
            continue
        identity = profile.get("profile_id")
        if not isinstance(identity, str) or not identity or identity in seen:
            errors.append("execution profile_id must be non-empty and unique")
        else:
            seen.add(identity)
        errors.extend(validate_variables(spec.get("variables_schema") or {}, profile.get("variables")))
        keys = profile.get("asset_set_keys")
        if not isinstance(keys, list) or not keys or not all(isinstance(k, str) and k for k in keys) or len(set(keys)) != len(keys):
            errors.append(f"profile {identity} requires unique stable asset_set_keys")
        # ``markets`` 可选：声明后该档位只服务这些市场（同一配方里 TH/VN 两档
        # 变量相同、只有素材集键不同，必须能按市场收敛）。不声明 = 对所有市场
        # 开放，因此既有 TH/MX 配方不受影响。
        profile_markets = profile.get("markets")
        if profile_markets is not None and (
                not isinstance(profile_markets, list) or not profile_markets
                or any(not isinstance(item, str) or not item for item in profile_markets)
                or len(set(profile_markets)) != len(profile_markets)):
            errors.append(
                f"profile {identity} markets must be a non-empty list of unique market codes"
            )
        variants = profile.get("copy_variants")
        copy_pack_id = profile.get("copy_pack_id")
        if (not isinstance(variants, list) or not variants) and not (
            isinstance(copy_pack_id, str) and copy_pack_id.strip()
        ):
            errors.append(f"profile {identity} requires at least one copy variant")
        elif isinstance(variants, list) and variants:
            copy_ids = set()
            for variant in variants:
                if not isinstance(variant, Mapping):
                    errors.append("copy variant must be an object")
                    continue
                copy_id = variant.get("copy_id")
                if not isinstance(copy_id, str) or not copy_id or copy_id in copy_ids:
                    errors.append(f"profile {identity} copy_id must be non-empty and unique")
                else:
                    copy_ids.add(copy_id)
                errors.extend(validate_copy(
                    variant.get("copy"), allow_placeholders=True,
                    expected_slide_count=expected_slides,
                ))
    return errors
