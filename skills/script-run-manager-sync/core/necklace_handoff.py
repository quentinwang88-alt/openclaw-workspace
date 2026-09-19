"""NECKLACE_MIXED_V1 final-consumer handoff gate.

Development contract (2026-09-19), section 8: the *final* sync consumer has to
verify, for rows the frozen source identifies as ``NECKLACE_MIXED_V1``, the
actual delivered prompt together with its hash and the shared/local render
versions.  A row whose text no longer matches the audited text, whose necklace
audit is missing or older than the version this consumer understands, or whose
audit is a known ``FAIL`` must come back as a per-row error -- no target task,
no un-checking the source row, no model regeneration.

Why this module exists at all
-----------------------------
The export path already refuses to enable production for a row whose prompt
contradicts its own frozen contract, but that verdict travels in *editable
table columns*.  Section 8 says so explicitly: do not lean on a table marker
that can be deleted.  The durable authority is the frozen batch item, so this
module reads the identity by ``script_id`` out of the original-script database
and judges the row against **that**, never against a marker column.

Read-only by construction
-------------------------
* The connection is opened with SQLite's ``mode=ro`` URI, so a missing database
  can never be created and this gate can never write.
* No import of the original-script skill's ``core`` package: both skills own a
  top-level ``core`` and importing across them is exactly the coupling the
  isolation runner warns about.  The handful of shapes needed here (the
  contract key, the shot-header line, the prompt hash) are re-stated, and a
  test pins each literal against the generator's source so the two cannot
  drift apart silently.
* A ``MySQL`` deployment is *not* an obstacle here even though
  ``ORIGINAL_SCRIPT_GENERATOR_DATABASE_URL`` is set in production: that variable
  governs ``PipelineStorage``, while ``BatchStorage`` -- the only writer of
  ``original_content_item``, the table read here -- always opens the SQLite
  file.  A genuinely unreadable frozen source is still reported rather than
  guessed at, and the prompt-signature tripwire keeps the ``NECKLACE_MIXED_V1``
  rows that *look* like necklace films out of production instead of letting
  them through unverified.

Nothing in here changes the behaviour of any other category: a row that is not
identified as ``NECKLACE_MIXED_V1`` returns an empty verdict.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

__all__ = [
    "NECKLACE_HANDOFF_PROFILE",
    "NECKLACE_HANDOFF_AUDIT_VERSION",
    "NECKLACE_HANDOFF_SHARED_AUDIT_VERSION",
    "NECKLACE_HANDOFF_SHOT_SIGNATURE",
    "NECKLACE_HANDOFF_PROMPT_HASH_LENGTH",
    "NECKLACE_HANDOFF_LOCAL_PROMPT_HASH_LENGTH",
    "NECKLACE_HANDOFF_TEMPLATE_ID",
    "NECKLACE_HANDOFF_ERRORS",
    "default_necklace_db_path",
    "necklace_v1_prompt_signature",
    "load_necklace_frozen_identity",
    "load_necklace_frozen_identity_by_public_id",
    "reset_necklace_handoff_cache",
    "check_necklace_handoff",
]


NECKLACE_HANDOFF_PROFILE = "NECKLACE_MIXED_V1"
NECKLACE_HANDOFF_TEMPLATE_ID = "NMX_01_WEAR_DETAIL_STATIC"

# The necklace audit revision this consumer is able to judge.  Kept in step with
# ``core.necklace_mixed_profile.NECKLACE_PROMPT_AUDIT_VERSION`` in the
# original-script skill; the drift guard in the test suite pins both literals.
NECKLACE_HANDOFF_AUDIT_VERSION = "necklace-final-prompt-audit-v1"

# The shared execution-audit revision the necklace gate is written against
# (``core.accessory_mixed_templates.EXECUTION_AUDIT_VERSION`` in the
# original-script skill).  Pinned for the same reason: section 8 asks the final
# consumer to verify the *shared* version too, and "shared" has to mean a
# concrete revision rather than "whatever is there".
NECKLACE_HANDOFF_SHARED_AUDIT_VERSION = "mixed-execution-audit-v1"

# The ordered module list of the one V1 template.  It is *not* the time ranges:
# AMX_C_DETAIL_FIRST shares 4/3/4/4 with the necklace template, so durations
# cannot tell the two apart.  Verified against all three rotations.
NECKLACE_HANDOFF_SHOT_SIGNATURE: Tuple[str, ...] = (
    "WORN_DETAIL",
    "WORN_RELATION",
    "HANDHELD_PRODUCT",
    "STATIC_PRODUCT",
)

# ``core.accessory_mixed_templates._prompt_hash`` -- sha1 of the delivered text,
# first 20 hex characters.  The necklace layer uses its own digest
# (``core.necklace_mixed_profile.audit_necklace_final_prompt``: sha256, first 16
# characters), so the two hashes are *not* comparable with each other and each
# one is recomputed with its own algorithm instead.
NECKLACE_HANDOFF_PROMPT_HASH_LENGTH = 20
NECKLACE_HANDOFF_LOCAL_PROMPT_HASH_LENGTH = 16

NECKLACE_HANDOFF_UNVERIFIED = "NECKLACE_HANDOFF_UNVERIFIED"
NECKLACE_HANDOFF_PROMPT_CHANGED = "NECKLACE_HANDOFF_PROMPT_CHANGED"
NECKLACE_HANDOFF_AUDIT_MISSING = "NECKLACE_HANDOFF_AUDIT_MISSING"
NECKLACE_HANDOFF_STALE_LOCAL_VERSION = "NECKLACE_HANDOFF_STALE_LOCAL_VERSION"
NECKLACE_HANDOFF_AUDIT_FAILED = "NECKLACE_HANDOFF_AUDIT_FAILED"
NECKLACE_HANDOFF_VERSION_MISMATCH = "NECKLACE_HANDOFF_VERSION_MISMATCH"

NECKLACE_HANDOFF_ERRORS: Tuple[str, ...] = (
    NECKLACE_HANDOFF_UNVERIFIED,
    NECKLACE_HANDOFF_PROMPT_CHANGED,
    NECKLACE_HANDOFF_AUDIT_MISSING,
    NECKLACE_HANDOFF_STALE_LOCAL_VERSION,
    NECKLACE_HANDOFF_AUDIT_FAILED,
    NECKLACE_HANDOFF_VERSION_MISMATCH,
)

MIXED_TEMPLATE_CONTRACT_KEY = "mixed_template_contract"

# ``core.accessory_mixed_templates._FINAL_SHOT_HEADER_RE``, re-stated for the
# read-only signature check.  Only the role group is used.
_SHOT_HEADER_RE = re.compile(
    r"^【(?:拍摄片段|连续内容段|片段)\d{2}｜[^｜]+｜(?P<role>[^】]*)】$"
)

# Per-process memo, keyed by (database, lookup key).  ``run_pipeline`` rebuilds
# a single row at a time in a few places, and the original-script table has no
# index on ``script_id``, so a repeated lookup would otherwise rescan the table.
_IDENTITY_CACHE: Dict[Tuple[str, str], Optional[Dict[str, Any]]] = {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _prompt_hash(prompt: Any) -> str:
    """The shared audit's digest (``sha1``, first 20 hex characters)."""

    return hashlib.sha1(_text(prompt).encode("utf-8")).hexdigest()[
        :NECKLACE_HANDOFF_PROMPT_HASH_LENGTH
    ]


def _local_prompt_hash(prompt: Any) -> str:
    """The necklace audit's digest (``sha256``, first 16 hex characters)."""

    return hashlib.sha256(_text(prompt).encode("utf-8")).hexdigest()[
        :NECKLACE_HANDOFF_LOCAL_PROMPT_HASH_LENGTH
    ]


def _dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def reset_necklace_handoff_cache() -> None:
    """Drop the per-process identity memo (tests and long-lived workers)."""

    _IDENTITY_CACHE.clear()


def default_necklace_db_path() -> Path:
    """Where the frozen batch items live.

    Mirrors ``BatchStorage._connect`` / ``run_first_frame_tasks._load_script``
    of the original-script skill: an explicit ``ORIGINAL_SCRIPT_GENERATOR_DB_PATH``
    wins, otherwise the shared data root.

    Note what is deliberately *not* consulted: ``ORIGINAL_SCRIPT_GENERATOR_DATABASE_URL``.
    That variable selects MySQL for ``PipelineStorage``, but ``BatchStorage`` --
    the only writer of ``original_content_item``, the table this gate reads --
    always opens the SQLite file and never looks at it.  Treating that URL as
    "the frozen source is elsewhere" would have made this gate permanently
    unavailable in exactly the environment it has to protect.
    """

    override = _text(os.environ.get("ORIGINAL_SCRIPT_GENERATOR_DB_PATH"))
    if override:
        return Path(override)
    root = _text(os.environ.get("OPENCLAW_SHARED_DATA_DIR")) or str(
        Path.home() / ".openclaw" / "shared" / "data"
    )
    return Path(root) / "original_script_generator.sqlite3"


def necklace_v1_prompt_signature(prompt: Any) -> Tuple[str, ...]:
    """The ordered module list of a delivered prompt, or ``()``.

    Read off the per-shot header lines.  A tripwire only: it can hold a row
    back, never admit one, so it is deliberately not treated as identity.
    """

    modules: List[str] = []
    for raw in _text(prompt).splitlines():
        match = _SHOT_HEADER_RE.match(raw.strip())
        if not match:
            continue
        for module in match.group("role").split("+"):
            module = module.strip()
            if module and module not in modules:
                modules.append(module)
    return tuple(modules)


def _connect(db_path: Path) -> sqlite3.Connection:
    # ``mode=ro``: never create the file, never write to it.
    connection = sqlite3.connect(
        f"file:{db_path}?mode=ro", uri=True, timeout=10
    )
    connection.row_factory = sqlite3.Row
    return connection


def _frozen_contract_holders(
    script: Dict[str, Any], result: Dict[str, Any], frozen_package: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Every place a frozen mixed contract can sit, longest-lived first.

    Same order as ``production_script_renderer._frozen_mixed_contract_for_item``
    so the two never disagree about which contract a delivered prompt belongs to.
    """

    brief = _dict(script.get("video_generation_brief")) or script
    return [
        _dict(brief.get("category_execution_extension")),
        script,
        result,
        frozen_package,
    ]


def _identity_from_result_json(result_json: Any) -> Optional[Dict[str, Any]]:
    try:
        result = json.loads(result_json or "{}")
    except (TypeError, ValueError):
        return None
    if not isinstance(result, dict):
        return None
    script = _dict(result.get("script"))
    if not script:
        return None
    frozen_package: Dict[str, Any] = {}
    raw_package = result.get("frozen_direction_package_json")
    if isinstance(raw_package, str):
        try:
            frozen_package = _dict(json.loads(raw_package))
        except (TypeError, ValueError):
            frozen_package = {}
    elif isinstance(raw_package, dict):
        frozen_package = dict(raw_package)

    contract: Dict[str, Any] = {}
    for holder in _frozen_contract_holders(script, result, frozen_package):
        candidate = holder.get(MIXED_TEMPLATE_CONTRACT_KEY)
        if isinstance(candidate, dict) and candidate:
            contract = candidate
            break
    block = _dict(_dict(contract.get("necklace_contract")))
    return {
        "is_necklace_v1": _text(contract.get("feature_profile"))
        == NECKLACE_HANDOFF_PROFILE,
        "template_id": _text(contract.get("template_id")),
        "feature_version": contract.get("feature_version"),
        "template_version": contract.get("template_version"),
        "profile_config_hash": _text(block.get("profile_config_hash")),
        "complete_script_id": _text(script.get("complete_script_id")),
        "render_validation": _dict(result.get("render_validation")),
    }


def load_necklace_frozen_identity(
    script_id: str, *, db_path: Optional[Path] = None
) -> Optional[Dict[str, Any]]:
    """The frozen identity behind ``script_id``, or ``None`` when unavailable.

    ``None`` means "this consumer could not read the frozen source" -- an
    unreadable database and a genuinely absent row are deliberately the same
    answer, because neither supports a verdict about the delivered prompt.
    """

    script_id = _text(script_id)
    if not script_id:
        return None
    path = db_path if db_path is not None else default_necklace_db_path()
    # ``script_id`` may be the internal batch id or the public
    # ``complete_script_id``; cache the two lookups separately.
    for cache_key, query in (
        (
            f"script_id:SCRIPT_READY:{script_id}",
            "SELECT result_json FROM original_content_item "
            "WHERE script_id=? AND status='SCRIPT_READY' "
            "ORDER BY updated_at DESC LIMIT 1",
        ),
        (
            f"script_id:any:{script_id}",
            "SELECT result_json FROM original_content_item "
            "WHERE script_id=? ORDER BY updated_at DESC LIMIT 1",
        ),
    ):
        identity = _cached_identity(path, cache_key, query, (script_id,))
        if identity is not None:
            return identity
    return None


def load_necklace_frozen_identity_by_public_id(
    script_id: str, *, db_path: Optional[Path] = None
) -> Optional[Dict[str, Any]]:
    """Second chance for the workbench-facing ``complete_script_id``.

    Expensive (the table has no index on the public id), so it is only reached
    for a prompt that already looks like a necklace film.
    """

    script_id = _text(script_id)
    if not script_id:
        return None
    path = db_path if db_path is not None else default_necklace_db_path()
    cache_key = f"complete_script_id:{script_id}"
    if _cache_key(path, cache_key) in _IDENTITY_CACHE:
        return _IDENTITY_CACHE[_cache_key(path, cache_key)]
    found: Optional[Dict[str, Any]] = None
    try:
        with _connect(path) as connection:
            rows = connection.execute(
                "SELECT result_json FROM original_content_item "
                "WHERE status='SCRIPT_READY' AND result_json IS NOT NULL "
                "ORDER BY updated_at DESC"
            ).fetchall()
        for row in rows:
            identity = _identity_from_result_json(row["result_json"])
            if identity and identity.get("complete_script_id") == script_id:
                found = identity
                break
    except sqlite3.Error:
        found = None
    _IDENTITY_CACHE[_cache_key(path, cache_key)] = found
    return found


def _cache_key(path: Path, cache_key: str) -> Tuple[str, str]:
    return (str(path), cache_key)


def _cached_identity(
    path: Path, cache_key: str, query: str, params: Tuple[Any, ...]
) -> Optional[Dict[str, Any]]:
    key = _cache_key(path, cache_key)
    if key in _IDENTITY_CACHE:
        return _IDENTITY_CACHE[key]
    found: Optional[Dict[str, Any]] = None
    try:
        with _connect(path) as connection:
            row = connection.execute(query, params).fetchone()
        if row is not None:
            found = _identity_from_result_json(row["result_json"])
    except sqlite3.Error:
        found = None
    _IDENTITY_CACHE[key] = found
    return found


def _verify(identity: Dict[str, Any], prompt: Any) -> str:
    validation = _dict(identity.get("render_validation"))
    audit = validation.get("necklace_audit")
    if not isinstance(audit, dict) or not audit:
        return (
            f"{NECKLACE_HANDOFF_AUDIT_MISSING}:冻结身份里没有项链 V1 的最终提示词检查，"
            "无法确认这条稿是通过新版项链检查的；请重新导出后再同步"
        )
    audit = dict(audit)

    audited_version = _text(audit.get("version"))
    if audited_version != NECKLACE_HANDOFF_AUDIT_VERSION:
        return (
            f"{NECKLACE_HANDOFF_STALE_LOCAL_VERSION}:项链检查版本为 "
            f"{audited_version or '空'}，本消费者只承认 "
            f"{NECKLACE_HANDOFF_AUDIT_VERSION}；请重新导出后再同步"
        )

    frozen_hash = _text(validation.get("prompt_hash"))
    if not frozen_hash:
        return (
            f"{NECKLACE_HANDOFF_PROMPT_CHANGED}:冻结身份没有记录最终提示词 hash，"
            "无法确认当前文本就是通过检查的那一版"
        )
    delivered_hash = _prompt_hash(prompt)
    if delivered_hash != frozen_hash:
        return (
            f"{NECKLACE_HANDOFF_PROMPT_CHANGED}:当前提示词与通过检查的文本不一致"
            f"（当前 {delivered_hash} / 冻结 {frozen_hash}）；不生成目标任务，"
            "请修正并重新导出"
        )
    # The necklace layer binds the text with its own digest; both layers have to
    # be pointing at the string that is on the row right now.
    if _text(audit.get("prompt_hash")) != _local_prompt_hash(prompt):
        return (
            f"{NECKLACE_HANDOFF_PROMPT_CHANGED}:项链检查绑定的交付文本与当前行不一致"
            "（项链检查未覆盖当前这一版文本）；不生成目标任务，请修正并重新导出"
        )

    if _text(validation.get("status")) == "FAIL" or _text(audit.get("status")) == "FAIL":
        return (
            f"{NECKLACE_HANDOFF_AUDIT_FAILED}:冻结身份记录该行最终提示词检查为 FAIL；"
            "已知冲突未修正前不生成目标任务"
        )

    shared_renderer = _text(validation.get("renderer_version"))
    local_renderer = _text(audit.get("renderer_version"))
    if not shared_renderer or not local_renderer or shared_renderer != local_renderer:
        return (
            f"{NECKLACE_HANDOFF_VERSION_MISMATCH}:共享渲染版本 {shared_renderer or '空'} "
            f"与项链局部渲染版本 {local_renderer or '空'} 不一致；"
            "请重新导出后再同步"
        )
    shared_audit_version = _text(validation.get("version"))
    if shared_audit_version != NECKLACE_HANDOFF_SHARED_AUDIT_VERSION:
        return (
            f"{NECKLACE_HANDOFF_STALE_LOCAL_VERSION}:共享执行检查版本为 "
            f"{shared_audit_version or '空'}，本消费者只承认 "
            f"{NECKLACE_HANDOFF_SHARED_AUDIT_VERSION}；请重新导出后再同步"
        )

    contract_hash = _text(identity.get("profile_config_hash"))
    audit_contract_hash = _text(audit.get("profile_config_hash"))
    if contract_hash and audit_contract_hash != contract_hash:
        return (
            f"{NECKLACE_HANDOFF_STALE_LOCAL_VERSION}:通过检查的是另一个项链 profile 版本"
            f"（检查 {audit_contract_hash or '空'} / 冻结合同 {contract_hash}）；"
            "请重新导出后再同步"
        )

    contract_version = identity.get("feature_version")
    if contract_version is not None and audit.get("feature_version") != contract_version:
        return (
            f"{NECKLACE_HANDOFF_STALE_LOCAL_VERSION}:项链 feature_version 不一致"
            f"（检查 {audit.get('feature_version')} / 冻结合同 {contract_version}）"
        )

    contract_template_version = identity.get("template_version")
    if (
        contract_template_version is not None
        and audit.get("template_version") != contract_template_version
    ):
        return (
            f"{NECKLACE_HANDOFF_STALE_LOCAL_VERSION}:项链模板版本不一致"
            f"（检查 {audit.get('template_version')} / 冻结合同 {contract_template_version}）"
        )
    return ""


def check_necklace_handoff(
    *,
    script_id: str,
    prompt: Any,
    db_path: Optional[Path] = None,
) -> str:
    """``""`` when the row may continue, otherwise the per-row refusal reason.

    Never raises.  A failure of the gate itself must not turn into a new failure
    mode for the other categories, and it must not silently admit a necklace
    film either -- so a crash refuses only the prompts that carry the necklace
    shot signature and leaves every other row exactly as it was.
    """

    try:
        return _check_necklace_handoff(
            script_id=script_id, prompt=prompt, db_path=db_path
        )
    except Exception as exc:  # noqa: BLE001 - see the docstring
        if necklace_v1_prompt_signature(prompt) != NECKLACE_HANDOFF_SHOT_SIGNATURE:
            return ""
        return (
            f"{NECKLACE_HANDOFF_UNVERIFIED}:项链交接检查无法执行"
            f"（{type(exc).__name__}: {exc}）；不生成目标任务"
        )


def _check_necklace_handoff(
    *,
    script_id: str,
    prompt: Any,
    db_path: Optional[Path] = None,
) -> str:
    """Only rows the frozen source identifies as necklace V1 are judged at all.

    Rows whose delivered prompt carries the necklace shot signature while the
    frozen source cannot be read are refused as unverified.  Everything else,
    including every other category, keeps the existing sync behaviour.
    """

    looks_like_necklace = (
        necklace_v1_prompt_signature(prompt) == NECKLACE_HANDOFF_SHOT_SIGNATURE
    )
    identity = load_necklace_frozen_identity(script_id, db_path=db_path)
    if identity is None and looks_like_necklace:
        identity = load_necklace_frozen_identity_by_public_id(
            script_id, db_path=db_path
        )
    if identity is None:
        if not looks_like_necklace:
            return ""
        return (
            f"{NECKLACE_HANDOFF_UNVERIFIED}:该行提示词带 {NECKLACE_HANDOFF_PROFILE} "
            f"镜头签名，但按 script_id（{_text(script_id)}）读不到冻结身份，"
            "无法核对最终提示词 hash 与渲染版本；不生成目标任务"
        )
    if not identity.get("is_necklace_v1"):
        if not looks_like_necklace:
            return ""
        return (
            f"{NECKLACE_HANDOFF_UNVERIFIED}:该行提示词带 {NECKLACE_HANDOFF_PROFILE} "
            "镜头签名，但冻结身份里没有项链 V1 合同；不生成目标任务"
        )
    return _verify(identity, prompt)
