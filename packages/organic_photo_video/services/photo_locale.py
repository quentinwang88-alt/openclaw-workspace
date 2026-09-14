"""Resolve the labels owned by a Locale Pack and the facts owned by a catalog.

Phase 2 of the VN scarf cross-market plan moves every publish label out of the
recipe and its planning policy: the Locale Pack owns moments / destinations /
temperature bands / per-family copy, and the Destination Catalog owns semantic
travel facts (climate, seasons, snow policy) with no language at all.

The legacy v1 recipe still carries its inline ``label_th``,
``destination_labels_th`` and ``temperature_labels_th`` tables, so every
resolver here treats ``locale_pack=None`` as "read the legacy inline tables".
That keeps TH V2 output byte-identical to the pre-Phase-2 baseline while a
country-agnostic recipe v2 reads exactly the same labels from a pack.

Resolvers are pure: no IO, no network, no Feishu, no filesystem access.
"""
from __future__ import annotations

import copy
from typing import Any, Mapping

__all__ = [
    "PhotoLocaleError",
    "LOCALE_LABEL_KINDS",
    "complete_look_copy",
    "destination_entry",
    "destination_labels",
    "destinations_for_country",
    "family_copy",
    "locale_pack_labels",
    "profile_copy_variants",
    "snow_scene_allowed",
    "temperature_labels",
    "travel_copy_template",
    "travel_copy_tokens",
    "travel_moment_labels",
]

# Legacy inline table names kept verbatim for the None-locale_pack path.
LEGACY_MOMENT_LABEL_FIELD = "label_th"
LEGACY_DESTINATION_LABEL_FIELD = "destination_labels_th"
LEGACY_TEMPERATURE_LABEL_FIELD = "temperature_labels_th"

LOCALE_LABEL_KINDS = ("travel_moments", "destinations", "temperature_bands", "generic")


class PhotoLocaleError(ValueError):
    """Raised when a bound locale pack or destination catalog is incomplete."""


def locale_pack_labels(locale_pack: Mapping[str, Any] | None, kind: str) -> dict[str, str]:
    """Return one label family from a locale pack, failing loudly when missing."""
    if kind not in LOCALE_LABEL_KINDS:
        raise PhotoLocaleError(f"未知的 locale 标签族：{kind}")
    labels = dict((locale_pack or {}).get("labels") or {})
    block = labels.get(kind)
    if not isinstance(block, Mapping):
        raise PhotoLocaleError(f"locale pack 缺少 labels.{kind}")
    return {str(key): str(value) for key, value in block.items()}


def travel_moment_labels(
    travel_contract: Mapping[str, Any] | None, *, locale_pack: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """Map ``travel_moment`` keys to their published label.

    ``locale_pack=None`` reproduces the legacy inline ``label_th`` table exactly.
    """
    if locale_pack is None:
        return {
            str(item.get("key") or ""): str(item.get(LEGACY_MOMENT_LABEL_FIELD) or "")
            for item in (travel_contract or {}).get("moments") or []
        }
    return locale_pack_labels(locale_pack, "travel_moments")


def destination_labels(
    travel_contract: Mapping[str, Any] | None, *, locale_pack: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """Map destination keys to their published label."""
    if locale_pack is None:
        return {
            str(key): str(value)
            for key, value in dict(
                (travel_contract or {}).get(LEGACY_DESTINATION_LABEL_FIELD) or {}
            ).items()
        }
    return locale_pack_labels(locale_pack, "destinations")


def temperature_labels(
    travel_contract: Mapping[str, Any] | None, *, locale_pack: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """Map temperature-band keys to their published label."""
    if locale_pack is None:
        return {
            str(key): str(value)
            for key, value in dict(
                (travel_contract or {}).get(LEGACY_TEMPERATURE_LABEL_FIELD) or {}
            ).items()
        }
    return locale_pack_labels(locale_pack, "temperature_bands")


def family_copy(
    family: Mapping[str, Any] | None, *, locale_pack: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return ``{title, cover, caption, look_labels}`` for one policy family.

    ``locale_pack=None`` reads the legacy inline ``title_th``/``cover_th``/
    ``caption_th`` fields and the per-look ``display_label``.
    """
    family = dict(family or {})
    if locale_pack is None:
        return {
            "title": str(family.get("title_th") or ""),
            "cover": str(family.get("cover_th") or ""),
            "caption": str(family.get("caption_th") or ""),
            "look_labels": {
                str(look.get("role") or ""): str(look.get("display_label") or "")
                for look in family.get("looks") or []
            },
        }
    families = dict((locale_pack or {}).get("family_copy") or {})
    entry = families.get(str(family.get("family_id") or ""))
    if not isinstance(entry, Mapping):
        raise PhotoLocaleError(
            f"locale pack 未提供 {family.get('family_id') or '未命名方案'} 的 family_copy"
        )
    return {
        "title": str(entry.get("title") or ""),
        "cover": str(entry.get("cover") or ""),
        "caption": str(entry.get("caption") or ""),
        "look_labels": {
            str(key): str(value)
            for key, value in dict(entry.get("look_labels") or {}).items()
        },
    }


def complete_look_copy(locale_pack: Mapping[str, Any] | None) -> list[dict[str, str]]:
    """Return the neutral COMPLETE_LOOK copy variants owned by a locale pack.

    ``COMPLETE_LOOK`` re-uses the uploaded four outfits, so it has no policy
    family to hang copy on: the planner used to carry four Thai title/cover/
    caption triples inline.  They now live in the Locale Pack under
    ``complete_look_copy`` so a VN plan cannot leak Thai copy.  The inline
    constants stay untouched for the ``locale_pack=None`` path.
    """
    entries = (locale_pack or {}).get("complete_look_copy")
    if not isinstance(entries, list) or not entries:
        raise PhotoLocaleError("locale pack 缺少 complete_look_copy")
    variants: list[dict[str, str]] = []
    for position, entry in enumerate(entries, 1):
        if not isinstance(entry, Mapping):
            raise PhotoLocaleError(f"complete_look_copy 第 {position} 条必须是对象")
        variant = {key: str(entry.get(key) or "") for key in ("title", "cover", "caption")}
        if not all(variant.values()):
            raise PhotoLocaleError(
                f"complete_look_copy 第 {position} 条的 title/cover/caption 不能为空"
            )
        variants.append(variant)
    return variants


def profile_copy_variants(
    profile: Mapping[str, Any] | None, *, locale: str,
) -> list[dict[str, Any]]:
    """Return the audited publish-copy variants this profile offers for ``locale``.

    ``config/loader.load_content_recipe_file`` fans a country-agnostic recipe's
    ``locale_copy_packs`` out into a per-profile ``copy_variants_by_locale`` map
    and then sets ``copy_variants`` to ``sorted(by_locale)[0]`` — alphabetically
    ``th-TH`` for every recipe that ships Thai.  Reading ``copy_variants``
    therefore hands a VN task the *Thai* template body while ``{destination}`` /
    ``{temperature}`` are already filled with Vietnamese labels, which is how a
    VN post ends up flagged "contains Thai characters for locale vi-VN".

    Language follows the market: read the entry for this task's locale.  A
    profile that carries no ``copy_variants_by_locale`` (every v1 recipe, whose
    loader path keys copy on ``copy_pack_id`` instead) and a task with no
    declared locale both keep the legacy default, so TH is byte-identical.

    A recipe that *does* declare this locale but whose profile ships no active
    rows for it is a configuration gap, not a reason to publish another
    language: it fails loudly.
    """
    profile = dict(profile or {})
    by_locale = profile.get("copy_variants_by_locale")
    legacy = copy.deepcopy(list(profile.get("copy_variants") or []))
    if not isinstance(by_locale, Mapping) or not by_locale:
        return legacy
    wanted = str(locale or "")
    if not wanted:
        return legacy
    entry = by_locale.get(wanted)
    if isinstance(entry, Mapping) and entry.get("copy_variants"):
        # 深拷贝：调用方会把这份文案冻进请求并被哈希，不该与配方配置共享可变对象。
        return copy.deepcopy(list(entry["copy_variants"]))
    raise PhotoLocaleError(
        f"profile {profile.get('profile_id') or '未命名档位'} 没有 {wanted} 的发布文案"
        f"（已配置：{'、'.join(sorted(str(key) for key in by_locale))}）"
    )


def travel_copy_template(
    locale_pack: Mapping[str, Any] | None, index: int, *, topic_zh: str = "",
) -> dict[str, Any] | None:
    """Neutral degraded-copy template owned by a Locale Pack.

    Review fix (2026-09-13), P0-2.  When the model's topic copy fails the
    publish-language check the pipeline degrades to a template.  That template
    used to be Thai unconditionally (``travel_topic["thai_fallback"]`` plus
    ``ลุค``/``คุณชอบลุคไหน?`` literals), so a VN task would have published Thai.

    ``None`` means "no pack bound": the caller then keeps the legacy inline Thai
    template, so TH V2 stays byte-identical.  With a pack bound the result can
    never contain Thai — it is the pack's own ``generic`` labels plus one
    rotated ``complete_look_copy`` caption.
    """
    if not locale_pack:
        return None
    labels = locale_pack_labels(locale_pack, "generic")
    variants = complete_look_copy(locale_pack)
    entry = variants[(max(int(index), 1) - 1) % len(variants)]
    return {
        "title": str(labels.get("title") or entry["title"] or topic_zh),
        "cover": str(labels.get("cover") or entry["cover"] or topic_zh),
        "caption": str(entry["caption"] or ""),
        "cta": str(labels.get("cta") or ""),
        "look_label": str(labels.get("look_label") or "Look {letter}"),
        "hashtags": [],
    }


def travel_copy_tokens(
    variables: Mapping[str, Any] | None, *,
    travel_contract: Mapping[str, Any] | None = None,
    locale_pack: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """Resolve the audited ``{destination}`` / ``{temperature}`` copy tokens."""
    variables = dict(variables or {})
    return {
        "destination": destination_labels(
            travel_contract, locale_pack=locale_pack
        ).get(str(variables.get("destination") or ""), ""),
        "temperature": temperature_labels(
            travel_contract, locale_pack=locale_pack
        ).get(str(variables.get("temperature_band") or ""), ""),
    }


def destination_entry(
    catalog: Mapping[str, Any] | None, destination_id: Any,
) -> dict[str, Any] | None:
    """Return one destination entry, or ``None`` when the catalog lacks it."""
    wanted = str(destination_id or "")
    if not wanted:
        return None
    for entry in (catalog or {}).get("destinations") or []:
        if str(entry.get("destination_id") or "") == wanted:
            return dict(entry)
    return None


def destinations_for_country(
    catalog: Mapping[str, Any] | None, country: Any,
) -> tuple[str, ...]:
    """Destination ids declared for one ISO alpha-2 country, in catalog order."""
    wanted = str(country or "")
    return tuple(
        str(entry.get("destination_id") or "")
        for entry in (catalog or {}).get("destinations") or []
        if str(entry.get("destination_country") or "") == wanted
    )


def snow_scene_allowed(
    catalog: Mapping[str, Any] | None, destination_id: Any, *, requested: bool = False,
) -> bool:
    """Whether a snow scene may be generated for this destination.

    ``FORBIDDEN`` never allows snow; ``DEFAULT`` always does; the catalog default
    (``OPTIONAL_NOT_DEFAULT``) allows it only when the caller explicitly asks,
    so cool/winter cities such as Tokyo or Shanghai never default to snow.
    """
    entry = destination_entry(catalog, destination_id)
    if entry is None:
        raise PhotoLocaleError(f"目的地目录中不存在 {destination_id}")
    policy = str(
        entry.get("snow_scene_policy")
        or (catalog or {}).get("default_snow_scene_policy")
        or "OPTIONAL_NOT_DEFAULT"
    )
    if policy == "FORBIDDEN":
        return False
    if policy == "DEFAULT":
        return True
    return bool(requested)
