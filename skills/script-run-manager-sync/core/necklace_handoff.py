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
  file.  A genuinely unreadable frozen source is reported as such rather than
  guessed at.

Identity, and what is *not* identity (round 2, 2026-09-20)
---------------------------------------------------------
The first revision consulted the prompt's shot-header modules **before** it
would look the row up by its public id, so a real delivered film -- whose
headers carry narrative roles (``HOOK``/``PROOF``/``ENDING``), never module
names -- missed the fallback and was admitted unverified.  Identity is now
resolved from the workbench's own identifiers only: the internal ``script_id``
column, the public ``complete_script_id`` inside ``result_json`` and the
``batch_item_id``.  All three are queried **unconditionally**, and the
shot-header signature is demoted to a diagnostic that can only ever hold a row
back.

The lookup also reports *which* of four things happened -- ``FOUND``,
``NOT_FOUND``, ``SOURCE_UNAVAILABLE``, ``AMBIGUOUS`` -- because a single
``None`` used to read "no such row", "cannot read the database at all" and
"several rows disagree" as "confirmed not a necklace", which is the one
conclusion none of them supports.

Nothing in here changes the behaviour of any other category: a row that is
resolved to something other than ``NECKLACE_MIXED_V1`` returns an empty
verdict, exactly as before.
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
    "NECKLACE_DELIVERY_SNAPSHOT_SCHEMA",
    "DELIVERY_SNAPSHOT_KEY",
    "NECKLACE_LOOKUP_FOUND",
    "NECKLACE_LOOKUP_NOT_FOUND",
    "NECKLACE_LOOKUP_SOURCE_UNAVAILABLE",
    "NECKLACE_LOOKUP_AMBIGUOUS",
    "NECKLACE_LOOKUP_STATUSES",
    "default_necklace_db_path",
    "necklace_v1_prompt_signature",
    "necklace_product_type_declared",
    "resolve_necklace_frozen_identity",
    "prime_necklace_identity_lookup",
    "reset_necklace_handoff_cache",
    "check_necklace_handoff",
]


NECKLACE_HANDOFF_PROFILE = "NECKLACE_MIXED_V1"
NECKLACE_HANDOFF_TEMPLATE_ID = "NMX_01_WEAR_DETAIL_STATIC"

# The necklace audit revision this consumer is able to judge.  Kept in step with
# ``core.necklace_mixed_profile.NECKLACE_PROMPT_AUDIT_VERSION`` in the
# original-script skill; the drift guard in the test suite pins both literals.
#
# Why the revision moved to v2 (2026-09-19): v1 judged two things against the
# wrong authority and refused a correct film.  It compared each shot's frozen
# ``action`` with the delivered 画面事件 / 人物动作, which the generation model
# writes in its own words, and it required the four ``商品必须可见`` lines to be
# *equal* even though the renderer derives each one from that shot's own anchors.
# v2 reads the shot's 本段手机构图 line (renderer-owned, byte-identical to the
# frozen framing) and compares the four anchor lines by the clause they share.
# A row frozen with a v1 audit is therefore not evidence about the current
# checks: it may have been stamped PASS without either check meaning anything.
NECKLACE_HANDOFF_AUDIT_VERSION = "necklace-final-prompt-audit-v2"

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
# Round 2: the frozen identity no longer carries the delivered text by itself --
# the *snapshot* written by the current exporter does, and the consumer reads
# that.  A row whose only audit is the one written at generation time cannot be
# judged: re-rendering produces a new verdict (v1 FAIL -> v2 PASS is a real and
# expected case) and only the exporter can record which text that verdict was
# about.  ``SNAPSHOT_MISMATCH`` covers a snapshot that exists but does not
# describe *this* row, script and frozen source.
NECKLACE_HANDOFF_SNAPSHOT_MISSING = "NECKLACE_HANDOFF_SNAPSHOT_MISSING"
NECKLACE_HANDOFF_SNAPSHOT_MISMATCH = "NECKLACE_HANDOFF_SNAPSHOT_MISMATCH"
# The frozen source itself could not be read, or several rows match and they do
# not agree.  Neither is evidence of anything, so neither may be read as "not a
# necklace row".
NECKLACE_HANDOFF_SOURCE_UNAVAILABLE = "NECKLACE_HANDOFF_SOURCE_UNAVAILABLE"
NECKLACE_HANDOFF_SOURCE_AMBIGUOUS = "NECKLACE_HANDOFF_SOURCE_AMBIGUOUS"

NECKLACE_HANDOFF_ERRORS: Tuple[str, ...] = (
    NECKLACE_HANDOFF_UNVERIFIED,
    NECKLACE_HANDOFF_PROMPT_CHANGED,
    NECKLACE_HANDOFF_AUDIT_MISSING,
    NECKLACE_HANDOFF_STALE_LOCAL_VERSION,
    NECKLACE_HANDOFF_AUDIT_FAILED,
    NECKLACE_HANDOFF_VERSION_MISMATCH,
    NECKLACE_HANDOFF_SNAPSHOT_MISSING,
    NECKLACE_HANDOFF_SNAPSHOT_MISMATCH,
    NECKLACE_HANDOFF_SOURCE_UNAVAILABLE,
    NECKLACE_HANDOFF_SOURCE_AMBIGUOUS,
)

# --- Delivery snapshot ------------------------------------------------------
# ``core.production_script_renderer.build_delivery_snapshot`` (generator side)
# writes this under ``result_json["delivery_snapshot"]`` on export.  It is the
# one place where "the text the operator can see in the table" and "the verdict
# the current renderer reached about it" are stored together; the older
# ``result_json["render_validation"]`` stays where it is as lineage, but it
# describes the *generation-time* render and can be several revisions behind.
NECKLACE_DELIVERY_SNAPSHOT_SCHEMA = "necklace-delivery-snapshot-v1"
DELIVERY_SNAPSHOT_KEY = "delivery_snapshot"

# --- Identity lookup outcomes ----------------------------------------------
# Four outcomes, not one ``None``.
NECKLACE_LOOKUP_FOUND = "FOUND"
NECKLACE_LOOKUP_NOT_FOUND = "NOT_FOUND"
NECKLACE_LOOKUP_SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
NECKLACE_LOOKUP_AMBIGUOUS = "AMBIGUOUS"
NECKLACE_LOOKUP_STATUSES: Tuple[str, ...] = (
    NECKLACE_LOOKUP_FOUND,
    NECKLACE_LOOKUP_NOT_FOUND,
    NECKLACE_LOOKUP_SOURCE_UNAVAILABLE,
    NECKLACE_LOOKUP_AMBIGUOUS,
)

#: 产品类型 values that *claim* the necklace profile.  A claim is not evidence:
#: it is never used to admit a row, cannot turn a historical necklace into a V1
#: row, and is consulted only while the frozen identity is unresolved -- in
#: which case the row is held back rather than waved through.
_NECKLACE_DECLARED_TOKENS: Tuple[str, ...] = ("项链", "necklace")

MIXED_TEMPLATE_CONTRACT_KEY = "mixed_template_contract"

# ``core.accessory_mixed_templates._FINAL_SHOT_HEADER_RE``, re-stated for the
# read-only signature check.  Only the role group is used.
_SHOT_HEADER_RE = re.compile(
    r"^【(?:拍摄片段|连续内容段|片段)\d{2}｜[^｜]+｜(?P<role>[^】]*)】$"
)

# Selects the row by any of the three identifiers the workbench can hand us.
# ``json_valid`` guards the JSON extraction: one malformed ``result_json``
# anywhere in the table would otherwise abort the whole query.
_ROW_COLUMNS = "batch_item_id, script_id, status, result_json"
_ROW_PREDICATE = (
    "batch_item_id = ? OR script_id = ? "
    "OR (json_valid(result_json) "
    "    AND json_extract(result_json, '$.script.complete_script_id') = ?)"
)
_ROW_SQL = (
    f"SELECT {_ROW_COLUMNS} FROM original_content_item WHERE {_ROW_PREDICATE}"
)

# One query per this many keys when a whole sync round is primed at once.  Keeps
# the placeholder count far below SQLite's ``SQLITE_MAX_VARIABLE_NUMBER``.
_PRIME_CHUNK = 120

# Per-process memo of *single-key* lookups, keyed by (database, key).  The table
# has no index on ``script_id`` or on the JSON public id, so a repeated lookup
# would otherwise rescan it.
#
# Scope: one sync round, not one process.  ``prime_necklace_identity_lookup`` and
# ``build_original_batch_sync_tasks`` clear it at the start of a round, because a
# negative answer cached before a re-export would keep refusing a row that has
# since been fixed -- the exact "re-export forever" loop this round exists to
# remove.
_CANDIDATE_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}


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
    """Drop the per-round identity memo (tests, and the sync runner).

    Called at the start of every sync round: a *negative* or stale answer must
    not outlive the round that produced it, or a row that has been re-exported
    would keep being refused by a cached verdict from before the re-export.
    """

    _CANDIDATE_CACHE.clear()


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

    Diagnostic only.  It is **not** a lookup condition and it cannot admit a
    row: the delivered headers carry narrative roles (``HOOK``/``PROOF``/
    ``ENDING``) in production, so in practice this returns ``()`` for every real
    film -- which is exactly why the identity query must not wait for it.  Where
    it is still consulted, it can only hold a row back.
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


def necklace_product_type_declared(product_type: Any) -> bool:
    """Whether the row's 产品类型 column *claims* the necklace profile.

    A claim, never a verdict.  Used for one purpose: when the frozen identity
    cannot be resolved, a row that claims to be a necklace is held back instead
    of being waved through as "probably not our business".  It can not admit a
    row, and it deliberately can not promote a historical necklace to V1.
    """

    text = _text(product_type).lower()
    return any(token in text for token in _NECKLACE_DECLARED_TOKENS)


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
        # The export-time authority (round 2).  Generation time cannot produce
        # it: only the exporter knows which text it actually handed the table.
        "delivery_snapshot": _dict(result.get(DELIVERY_SNAPSHOT_KEY)),
    }


def _identity_from_row(row: Any) -> Optional[Dict[str, Any]]:
    identity = _identity_from_result_json(row["result_json"])
    if identity is None:
        return None
    identity["batch_item_id"] = _text(row["batch_item_id"])
    identity["row_status"] = _text(row["status"])
    return identity


def _row_keys(row: Any, identity: Optional[Dict[str, Any]] = None) -> List[str]:
    """Every identifier this row answers to."""

    keys = [_text(row["batch_item_id"]), _text(row["script_id"])]
    if identity is not None:
        keys.append(_text(identity.get("complete_script_id")))
    return [key for key in keys if key]


def _cache_key(path: Any, key: str) -> Tuple[str, str]:
    return (str(path), f"k:{key}")


def _query_rows(path: Path, keys: Sequence[str]) -> Tuple[Optional[List[Any]], str]:
    """``(rows, error)`` -- one query for all ``keys``.

    ``error`` non-empty means the *source itself* could not be read, which is
    reported to the caller as ``SOURCE_UNAVAILABLE`` and never as "no match".
    """

    clauses = " OR ".join([f"({_ROW_PREDICATE})"] * len(keys))
    sql = (
        f"SELECT {_ROW_COLUMNS} FROM original_content_item WHERE {clauses}"
    )
    params: List[Any] = []
    for key in keys:
        params.extend([key, key, key])
    connection: Optional[sqlite3.Connection] = None
    try:
        connection = _connect(path)
        return list(connection.execute(sql, params).fetchall()), ""
    except sqlite3.Error as exc:
        return None, f"{type(exc).__name__}: {exc}"
    finally:
        # ``with connection`` commits and rolls back but does **not** close, so
        # a round of a few hundred rows would leak one file handle per query.
        if connection is not None:
            connection.close()


def _candidate_bucket() -> Dict[str, Any]:
    return {"candidates": [], "seen": []}


def _collect_candidate(bucket: Dict[str, Any], identity: Dict[str, Any]) -> None:
    marker = _text(identity.get("batch_item_id")) or (
        f"?{len(bucket['candidates'])}"
    )
    if marker in bucket["seen"]:
        return
    bucket["seen"].append(marker)
    bucket["candidates"].append(identity)


def _ready_identity(row: Any) -> Optional[Dict[str, Any]]:
    """The identity of a row that actually delivered a script, else ``None``."""

    if _text(row["status"]) != "SCRIPT_READY":
        # A row that never reached SCRIPT_READY holds no delivered script, so it
        # is not the frozen identity behind a delivered prompt.
        return None
    return _identity_from_row(row)


def prime_necklace_identity_lookup(
    keys: Iterable[Any], *, db_path: Optional[Path] = None
) -> int:
    """Resolve every identifier in ``keys`` in as few queries as possible.

    A sync round knows all of its rows up front, so it can pay one query per
    ``_PRIME_CHUNK`` identifiers instead of one per row.  Returns the number of
    identifiers actually queried; identifiers already answered in this round are
    skipped, and an unreadable source returns ``-1`` so the caller can report
    ``SOURCE_UNAVAILABLE`` rather than a false "no match".
    """

    path = db_path if db_path is not None else default_necklace_db_path()
    wanted: List[str] = []
    for raw in keys or ():
        key = _text(raw)
        if key and key not in wanted:
            wanted.append(key)
    pending = [
        key for key in wanted if _cache_key(path, key) not in _CANDIDATE_CACHE
    ]
    if not pending:
        return 0
    queried = 0
    for start in range(0, len(pending), _PRIME_CHUNK):
        chunk = pending[start : start + _PRIME_CHUNK]
        rows, error = _query_rows(path, chunk)
        if error:
            return -1
        buckets: Dict[str, Dict[str, Any]] = {
            key: _candidate_bucket() for key in chunk
        }
        for row in rows:
            identity = _ready_identity(row)
            if identity is None:
                continue
            for key in _row_keys(row, identity):
                bucket = buckets.get(key)
                if bucket is not None:
                    _collect_candidate(bucket, identity)
        for key in chunk:
            _CANDIDATE_CACHE[_cache_key(path, key)] = {
                "error": "",
                "candidates": buckets[key]["candidates"],
            }
            queried += 1
    return queried


def resolve_necklace_frozen_identity(
    *,
    script_id: str = "",
    batch_item_id: str = "",
    db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Resolve the frozen identity behind a workbench row.

    Returns ``{"status", "identity", "matches", "detail", "conflict_is_necklace"}``
    where ``status`` is one of the four ``NECKLACE_LOOKUP_*`` outcomes.

    The internal ``script_id`` and the public ``complete_script_id`` are both
    queried, **unconditionally** -- never gated on what the prompt text looks
    like.  ``batch_item_id``, when the table carries it, narrows the answer to
    one item.  Several matching rows that disagree are reported as
    ``AMBIGUOUS``; "most recently updated" is not a tie-break, because silently
    picking one would let two different frozen sources answer for one delivered
    text.
    """

    path = db_path if db_path is not None else default_necklace_db_path()
    keys: List[str] = []
    for raw in (_text(script_id), _text(batch_item_id)):
        if raw and raw not in keys:
            keys.append(raw)
    if not keys:
        return {
            "status": NECKLACE_LOOKUP_NOT_FOUND,
            "identity": None,
            "matches": [],
            "detail": "该行没有脚本ID与批次ItemID，无法定位冻结来源",
            "conflict_is_necklace": False,
        }
    queried = prime_necklace_identity_lookup(keys, db_path=path)
    if queried < 0:
        # Re-probe once for the operator-facing reason; the failure itself is
        # never memoised, so this is a fresh read and not a cached verdict.
        _rows, error = _query_rows(path, keys[:1])
        return {
            "status": NECKLACE_LOOKUP_SOURCE_UNAVAILABLE,
            "identity": None,
            "matches": [],
            "detail": error or "冻结来源无法读取",
            "conflict_is_necklace": False,
        }

    candidates: List[Dict[str, Any]] = []
    matched: List[str] = []
    for key in keys:
        entry = _CANDIDATE_CACHE.get(_cache_key(path, key)) or {}
        for identity in entry.get("candidates") or []:
            marker = _text(identity.get("batch_item_id")) or _text(
                identity.get("complete_script_id")
            )
            if marker in matched:
                continue
            matched.append(marker)
            candidates.append(identity)

    if not candidates:
        return {
            "status": NECKLACE_LOOKUP_NOT_FOUND,
            "identity": None,
            "matches": [],
            "detail": (
                f"按 script_id/batch_item_id({'/'.join(keys)}) 在冻结来源里找不到 "
                "SCRIPT_READY 条目"
            ),
            "conflict_is_necklace": False,
        }

    target_item = _text(batch_item_id)
    if target_item:
        narrowed = [
            identity
            for identity in candidates
            if _text(identity.get("batch_item_id")) == target_item
        ]
        if narrowed:
            candidates = narrowed
            matched = [target_item]

    if len(candidates) > 1:
        return {
            "status": NECKLACE_LOOKUP_AMBIGUOUS,
            "identity": None,
            "matches": matched,
            "detail": (
                f"{len(candidates)} 个冻结条目同时命中 "
                f"({'、'.join(matched)})，无法确定这条交付文本属于哪一个"
            ),
            "conflict_is_necklace": any(
                identity.get("is_necklace_v1") for identity in candidates
            ),
        }
    identity = candidates[0]
    return {
        "status": NECKLACE_LOOKUP_FOUND,
        "identity": identity,
        "matches": matched,
        "detail": "",
        "conflict_is_necklace": bool(identity.get("is_necklace_v1")),
    }


def _verify(identity: Dict[str, Any], prompt: Any) -> str:
    """Judge the row against the *latest delivery snapshot*, or refuse.

    Round 1 read ``result_json["render_validation"]`` -- the verdict written at
    **generation** time.  That verdict is about a render that may no longer be
    the one on the row: re-exporting produces a fresh audit (v1 ``FAIL`` -> v2
    ``PASS`` is the real case that exposed this) and the table text is updated
    while the stored audit is not.  The consumer then demanded a re-export that
    could never satisfy it.

    So the authority is the snapshot the exporter writes *after* it renders the
    text it is about to publish.  Without one there is nothing that ties the
    text on the row to any verdict, and this consumer refuses rather than
    inventing a ``PASS``: the fix is a deterministic re-export, which writes the
    snapshot.
    """

    snapshot = _dict(identity.get("delivery_snapshot"))
    if not snapshot:
        return (
            f"{NECKLACE_HANDOFF_SNAPSHOT_MISSING}:冻结身份里没有最新交付快照"
            "（delivery_snapshot），无法确认表内文本就是通过当前检查的那一版；"
            "请重新导出后再同步"
        )
    if _text(snapshot.get("schema_version")) != NECKLACE_DELIVERY_SNAPSHOT_SCHEMA:
        return (
            f"{NECKLACE_HANDOFF_SNAPSHOT_MISMATCH}:交付快照版本为 "
            f"{_text(snapshot.get('schema_version')) or '空'}，本消费者只认 "
            f"{NECKLACE_DELIVERY_SNAPSHOT_SCHEMA}；请重新导出后再同步"
        )
    if _text(snapshot.get("feature_profile")) != NECKLACE_HANDOFF_PROFILE:
        return (
            f"{NECKLACE_HANDOFF_SNAPSHOT_MISMATCH}:交付快照的 feature_profile 为 "
            f"{_text(snapshot.get('feature_profile')) or '空'}，与冻结身份的 "
            f"{NECKLACE_HANDOFF_PROFILE} 不一致；请重新导出后再同步"
        )
    # Same item, same script, same frozen source.  Any disagreement means the
    # snapshot describes a different delivery, so it is not evidence about this
    # row -- and picking the snapshot anyway would let one row's PASS cover
    # another row's text.
    for field, label in (
        ("batch_item_id", "批次ItemID"),
        ("complete_script_id", "公开脚本ID"),
    ):
        snapshot_value = _text(snapshot.get(field))
        identity_value = _text(identity.get(field))
        if snapshot_value and identity_value and snapshot_value != identity_value:
            return (
                f"{NECKLACE_HANDOFF_SNAPSHOT_MISMATCH}:交付快照的{label}为 "
                f"{snapshot_value}，与冻结身份的 {identity_value} 不是同一条；"
                "请重新导出后再同步"
            )
    frozen_hash = _text(identity.get("profile_config_hash"))
    snapshot_frozen = _text(snapshot.get("frozen_contract_hash"))
    if not snapshot_frozen or (frozen_hash and snapshot_frozen != frozen_hash):
        return (
            f"{NECKLACE_HANDOFF_SNAPSHOT_MISMATCH}:交付快照的冻结合同 hash 为 "
            f"{snapshot_frozen or '空'}，冻结身份为 {frozen_hash or '空'}，"
            "两者不是同一次冻结；请重新导出后再同步"
        )

    audited_text = _text(snapshot.get("prompt_text"))
    if not audited_text:
        return (
            f"{NECKLACE_HANDOFF_SNAPSHOT_MISMATCH}:交付快照没有记录实际导出的完整"
            "提示词，无法确认表内文本；请重新导出后再同步"
        )
    if _prompt_hash(prompt) != _prompt_hash(audited_text):
        current = _prompt_hash(prompt)
        return (
            f"{NECKLACE_HANDOFF_PROMPT_CHANGED}:当前提示词与通过检查的交付文本不一致"
            f"（当前 {current} / 快照 {_prompt_hash(audited_text)}）；不生成目标任务，"
            "请修正并重新导出"
        )
    return _verify_validation(
        identity, _dict(snapshot.get("render_validation")), audited_text
    )


def _verify_validation(
    identity: Dict[str, Any], validation: Dict[str, Any], audited_text: str
) -> str:
    """The two-layer audit check, bound to ``audited_text``.

    ``audited_text`` -- not the row text -- is what the hashes inside the
    validation must match.  The row text has already been shown to be the same
    string, so this is what makes the PASS an assertion about *this* delivery
    rather than about a render that happened to share a hash prefix.
    """

    audit = validation.get("necklace_audit")
    if not isinstance(audit, dict) or not audit:
        return (
            f"{NECKLACE_HANDOFF_AUDIT_MISSING}:交付快照里没有项链 V1 的最终提示词检查，"
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
            f"{NECKLACE_HANDOFF_PROMPT_CHANGED}:交付快照没有记录最终提示词 hash，"
            "无法确认快照文本就是通过检查的那一版"
        )
    if _prompt_hash(audited_text) != frozen_hash:
        return (
            f"{NECKLACE_HANDOFF_SNAPSHOT_MISMATCH}:交付快照内记录的共享 hash 与快照"
            f"文本不一致（文本 {_prompt_hash(audited_text)} / 记录 {frozen_hash}）；"
            "快照已损坏，请重新导出"
        )
    # The necklace layer binds the text with its own digest; both layers have to
    # be pointing at the string the snapshot carries.
    if _text(audit.get("prompt_hash")) != _local_prompt_hash(audited_text):
        return (
            f"{NECKLACE_HANDOFF_SNAPSHOT_MISMATCH}:项链检查绑定的交付文本与快照文本"
            "不一致（项链检查未覆盖快照里这一版文本）；请重新导出"
        )

    if _text(validation.get("status")) == "FAIL" or _text(audit.get("status")) == "FAIL":
        return (
            f"{NECKLACE_HANDOFF_AUDIT_FAILED}:交付快照记录该行最终提示词检查为 FAIL；"
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
    batch_item_id: str = "",
    product_type: str = "",
    db_path: Optional[Path] = None,
) -> str:
    """``""`` when the row may continue, otherwise the per-row refusal reason.

    Never raises.  A failure of the gate itself must not turn into a new failure
    mode for the other categories, and it must not silently admit a necklace
    film either -- so a crash holds back the rows that carry a necklace signal
    (the shot signature, or a 产品类型 that claims 项链) and leaves every other
    row exactly as it was.
    """

    try:
        return _check_necklace_handoff(
            script_id=script_id,
            prompt=prompt,
            batch_item_id=batch_item_id,
            product_type=product_type,
            db_path=db_path,
        )
    except Exception as exc:  # noqa: BLE001 - see the docstring
        if not _carries_necklace_signal(prompt=prompt, product_type=product_type):
            return ""
        return (
            f"{NECKLACE_HANDOFF_UNVERIFIED}:项链交接检查无法执行"
            f"（{type(exc).__name__}: {exc}）；不生成目标任务"
        )


def _carries_necklace_signal(*, prompt: Any, product_type: Any) -> bool:
    """Whether the row gives any reason to think it may be a necklace V1 film.

    Both signals are refusal-side only.  Neither can admit a row, and the
    signature is deliberately *not* an identity query condition -- in production
    the delivered headers carry narrative roles, so it is normally empty.
    """

    return (
        necklace_v1_prompt_signature(prompt) == NECKLACE_HANDOFF_SHOT_SIGNATURE
        or necklace_product_type_declared(product_type)
    )


def _check_necklace_handoff(
    *,
    script_id: str,
    prompt: Any,
    batch_item_id: str = "",
    product_type: str = "",
    db_path: Optional[Path] = None,
) -> str:
    """Resolve the identity first, then judge -- never the other way round.

    The identity query runs for **every** row, whatever its prompt looks like:
    the delivered headers do not name modules, so gating the public-id lookup on
    the prompt signature meant real necklace rows were never looked up at all
    and were admitted unverified.

    Everything that cannot be resolved to a definite non-necklace answer, on a
    row that claims to be a necklace, is refused.  A row that resolves to
    something other than necklace V1 keeps the existing sync behaviour, which is
    also how the historical (pre-V1) necklace batches stay unaffected.
    """

    signature_only = necklace_v1_prompt_signature(prompt) == (
        NECKLACE_HANDOFF_SHOT_SIGNATURE
    )
    declared = necklace_product_type_declared(product_type)
    resolution = resolve_necklace_frozen_identity(
        script_id=script_id, batch_item_id=batch_item_id, db_path=db_path
    )
    status = _text(resolution.get("status"))
    identity = resolution.get("identity")

    if status == NECKLACE_LOOKUP_SOURCE_UNAVAILABLE:
        if not (declared or signature_only):
            return ""
        return (
            f"{NECKLACE_HANDOFF_SOURCE_UNAVAILABLE}:无法读取冻结来源，读不到这条稿的"
            f"身份，也就无法核对最终提示词 hash 与渲染版本"
            f"（{_text(resolution.get('detail'))}）；不生成目标任务"
        )
    if status == NECKLACE_LOOKUP_AMBIGUOUS:
        if not (declared or resolution.get("conflict_is_necklace")):
            return ""
        return (
            f"{NECKLACE_HANDOFF_SOURCE_AMBIGUOUS}:冻结来源里有多个条目同时命中"
            f"（{_text(resolution.get('detail'))}）；无法确定这条交付文本属于哪一个，"
            "不生成目标任务"
        )
    if status == NECKLACE_LOOKUP_NOT_FOUND or identity is None:
        if declared:
            return (
                f"{NECKLACE_HANDOFF_UNVERIFIED}:该行产品类型声明为项链，但按 "
                f"script_id（{_text(script_id)}）"
                f"{'/批次ItemID（' + _text(batch_item_id) + '）' if _text(batch_item_id) else ''}"
                f"读不到冻结身份（{_text(resolution.get('detail'))}）；"
                "不生成目标任务，请核对冻结来源或重新导出"
            )
        if signature_only:
            return (
                f"{NECKLACE_HANDOFF_UNVERIFIED}:该行提示词带 {NECKLACE_HANDOFF_PROFILE} "
                f"镜头签名，但按 script_id（{_text(script_id)}）读不到冻结身份，"
                "无法核对最终提示词 hash 与渲染版本；不生成目标任务"
            )
        return ""
    if not identity.get("is_necklace_v1"):
        if signature_only:
            return (
                f"{NECKLACE_HANDOFF_UNVERIFIED}:该行提示词带 {NECKLACE_HANDOFF_PROFILE} "
                "镜头签名，但冻结身份里没有项链 V1 合同；不生成目标任务"
            )
        # Resolved, and it is not this profile: the historical necklace batches
        # and every other category keep the sync behaviour they had.
        return ""
    return _verify(identity, prompt)

