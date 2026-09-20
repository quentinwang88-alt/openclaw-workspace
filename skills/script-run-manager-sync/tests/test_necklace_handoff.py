"""NECKLACE_MIXED_V1 handoff gate: section 8 of the development contract.

The gate has to answer one question per row -- "is the prompt on this row the
one the necklace audit actually cleared, judged by the shared *and* the local
render version?" -- and it has to answer it from the frozen source rather than
from a table marker that an operator can edit or delete.

These tests build a throwaway original-script database, so nothing here reads or
writes the production file.  The prompt fixtures are byte-for-byte renders of
the real templates (captured from the renderer), not hand-written lookalikes:
`AMX_C_DETAIL_FIRST` and the necklace template share 4/3/4/4 seconds, and a
fixture that ignored that would let a misleading detector pass.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

from core.bitable import TableRecord
from core.necklace_handoff import (
    DELIVERY_SNAPSHOT_KEY,
    NECKLACE_DELIVERY_SNAPSHOT_SCHEMA,
    NECKLACE_HANDOFF_AUDIT_FAILED,
    NECKLACE_HANDOFF_AUDIT_MISSING,
    NECKLACE_HANDOFF_AUDIT_VERSION,
    NECKLACE_HANDOFF_ERRORS,
    NECKLACE_HANDOFF_PROFILE,
    NECKLACE_HANDOFF_PROMPT_CHANGED,
    NECKLACE_HANDOFF_SHARED_AUDIT_VERSION,
    NECKLACE_HANDOFF_SHOT_SIGNATURE,
    NECKLACE_HANDOFF_SNAPSHOT_MISMATCH,
    NECKLACE_HANDOFF_SNAPSHOT_MISSING,
    NECKLACE_HANDOFF_SOURCE_AMBIGUOUS,
    NECKLACE_HANDOFF_SOURCE_UNAVAILABLE,
    NECKLACE_HANDOFF_STALE_LOCAL_VERSION,
    NECKLACE_HANDOFF_TEMPLATE_ID,
    NECKLACE_HANDOFF_UNVERIFIED,
    NECKLACE_HANDOFF_VERSION_MISMATCH,
    NECKLACE_LOOKUP_AMBIGUOUS,
    NECKLACE_LOOKUP_FOUND,
    NECKLACE_LOOKUP_NOT_FOUND,
    NECKLACE_LOOKUP_SOURCE_UNAVAILABLE,
    check_necklace_handoff,
    default_necklace_db_path,
    necklace_product_type_declared,
    necklace_v1_prompt_signature,
    prime_necklace_identity_lookup,
    reset_necklace_handoff_cache,
    resolve_necklace_frozen_identity,
)
from core.original_batch_source import (
    build_original_batch_sync_tasks,
    resolve_original_batch_field_mapping,
)

SKILLS_ROOT = Path(__file__).resolve().parents[2]
GENERATOR_ROOT = SKILLS_ROOT / "original-script-generator"

RENDERER_VERSION = "production-script-renderer-v5-speakable-semantic-mainline"
PROFILE_HASH = "cafe0123456789ab"

# Real delivered headers, copied from the renderer's own output.  The shipped
# form is 【片段NN｜...】 (the generator's header pattern also accepts
# 拍摄片段/连续内容段; that is exercised separately below).  AMX_C keeps the
# necklace template's 4/3/4/4 rhythm; only the module order differs, and that is
# exactly the point.
NMX_HEADERS = (
    "【片段01｜0-4s｜WORN_DETAIL+WORN_RELATION+HANDHELD_PRODUCT+STATIC_PRODUCT】",
    "【片段02｜4-7s｜WORN_DETAIL+WORN_RELATION+HANDHELD_PRODUCT+STATIC_PRODUCT】",
    "【片段03｜7-11s｜WORN_DETAIL+WORN_RELATION+HANDHELD_PRODUCT+STATIC_PRODUCT】",
    "【片段04｜11-15s｜WORN_DETAIL+WORN_RELATION+HANDHELD_PRODUCT+STATIC_PRODUCT】",
)
# The long-form prefix the same pattern accepts, kept so a renderer that switches
# to it cannot silently stop matching.
NMX_HEADERS_LONG_FORM = tuple(
    header.replace("【片段", "【拍摄片段") for header in NMX_HEADERS
)
AMX_NAME_FIRST = "【片段01｜0-3s｜WORN_DETAIL+HANDHELD_PRODUCT+STATIC_PRODUCT+WORN_RELATION】"
AMX_DETAIL_FIRST_4_3_4_4 = (
    "【片段01｜0-4s｜STATIC_PRODUCT+HANDHELD_PRODUCT+WORN_DETAIL+WORN_RELATION】"
)
PLAIN_PROMPT = "【脚本ID】\n- S1\n\n这是一条耳饰的最终提示词，没有任何镜块头。"


def nmx_prompt(extra: str = "", headers=None) -> str:
    headers = headers or NMX_HEADERS
    body = "\n".join(
        f"{header}\n画面事件：第 {index} 段画面。"
        for index, header in enumerate(headers, start=1)
    )
    return f"{body}\n{extra}" if extra else body


def shared_hash(prompt: str) -> str:
    """Independent restatement of the shared audit's digest."""

    return hashlib.sha1(prompt.strip().encode("utf-8")).hexdigest()[:20]


def local_hash(prompt: str) -> str:
    """Independent restatement of the necklace audit's digest."""

    return hashlib.sha256(prompt.strip().encode("utf-8")).hexdigest()[:16]


def contract_block(
    *,
    profile: str = NECKLACE_HANDOFF_PROFILE,
    template_id: str = NECKLACE_HANDOFF_TEMPLATE_ID,
    feature_version: object = 1,
    template_version: object = 1,
    profile_hash: str = PROFILE_HASH,
) -> dict:
    block = {
        "template_id": template_id,
        "execution_profile": "ACCESSORY_MIXED_TEMPLATE_V1",
        "feature_profile": profile,
        "feature_version": feature_version,
        "template_version": template_version,
    }
    if profile == NECKLACE_HANDOFF_PROFILE:
        block["necklace_contract"] = {
            "subtype": "SINGLE_LAYER_SINGLE_PENDANT",
            "interaction_mode": "NONE",
            "profile_config_hash": profile_hash,
        }
    return block


def validation_payload(
    prompt: str,
    *,
    contract=None,
    audit: object = "default",
    shared_status: str = "PASS",
    shared_version: str = NECKLACE_HANDOFF_SHARED_AUDIT_VERSION,
    renderer_version: str = RENDERER_VERSION,
    local_renderer_version: str = None,
    local_audit_version: str = NECKLACE_HANDOFF_AUDIT_VERSION,
    profile_hash: str = PROFILE_HASH,
    feature_version: object = 1,
    template_version: object = 1,
):
    """A ``render_validation`` blob shaped like the renderer's own output."""

    if audit == "default":
        audit = {
            "version": local_audit_version,
            "status": "PASS",
            "issues": [],
            "issue_count": 0,
            "checked_shots": 4,
            "prompt_hash": local_hash(prompt),
            "renderer_version": local_renderer_version or renderer_version,
            "feature_version": feature_version,
            "profile_config_hash": profile_hash,
            "template_version": template_version,
            "reason": "OK",
        }
    if audit is None:
        return {
            "version": shared_version,
            "status": shared_status,
            "issues": [],
            "prompt_hash": shared_hash(prompt),
            "renderer_version": renderer_version,
        }
    payload = {
        "version": shared_version,
        "status": shared_status,
        "issues": [],
        "prompt_hash": shared_hash(prompt),
        "renderer_version": renderer_version,
        "necklace_audit": audit,
    }
    return payload


def contract_profile_hash(contract) -> str:
    """``mixed_template_contract.necklace_contract.profile_config_hash``."""

    block = (contract or {}).get("necklace_contract") or {}
    return str(block.get("profile_config_hash") or "")


def delivery_snapshot_block(
    prompt: str,
    *,
    complete_script_id: str = "S1",
    batch_item_id: str = "",
    profile_hash: str = PROFILE_HASH,
    validation=None,
    schema_version: str = NECKLACE_DELIVERY_SNAPSHOT_SCHEMA,
    feature_profile: str = NECKLACE_HANDOFF_PROFILE,
    published_text: str = None,
) -> dict:
    """The export-time snapshot, shaped like the renderer's own output.

    ``published_text`` is what the exporter actually handed to the table, which
    is normally ``prompt``.  Passing a different string models the case the
    snapshot exists for: the row text and the snapshot have drifted apart.
    """

    return {
        "schema_version": schema_version,
        "batch_item_id": batch_item_id,
        "internal_script_id": f"{complete_script_id}-internal",
        "complete_script_id": complete_script_id,
        "feature_profile": feature_profile,
        "frozen_contract_hash": profile_hash,
        "source_content_hash": "0" * 16,
        "prompt_text": prompt if published_text is None else published_text,
        "render_validation": (
            validation
            if validation is not None
            else validation_payload(prompt, profile_hash=profile_hash)
        ),
        "created_at": "2026-09-20T09:00:00",
    }


def result_json(
    prompt: str,
    *,
    complete_script_id: str = "S1",
    contract=None,
    validation=None,
    snapshot: object = "default",
    batch_item_id: str = "",
) -> str:
    """A frozen row.  ``snapshot=None`` models a row exported before round 2."""

    contract = contract if contract is not None else contract_block()
    validation = (
        validation
        if validation is not None
        else validation_payload(prompt, contract=contract)
    )
    payload = {
        "status": "SUCCESS",
        "script": {
            "complete_script_id": complete_script_id,
            "video_generation_brief": {
                "category_execution_extension": {"mixed_template_contract": contract}
            },
        },
        "render_validation": validation,
    }
    if snapshot == "default":
        snapshot = delivery_snapshot_block(
            prompt,
            complete_script_id=complete_script_id,
            batch_item_id=batch_item_id,
            profile_hash=contract_profile_hash(contract) or PROFILE_HASH,
            validation=validation,
        )
    if snapshot is not None:
        payload[DELIVERY_SNAPSHOT_KEY] = snapshot
    return json.dumps(payload, ensure_ascii=False)


class _FakeSource:
    """A throwaway original-script database with the columns the gate reads."""

    def __init__(self, root: Path):
        self.path = root / "gen.sqlite3"
        connection = sqlite3.connect(self.path)
        connection.execute(
            "CREATE TABLE original_content_item ("
            "batch_item_id TEXT PRIMARY KEY, script_id TEXT, status TEXT, "
            "result_json TEXT, updated_at TEXT)"
        )
        connection.commit()
        connection.close()

    def add(
        self,
        script_id: str,
        payload: str,
        *,
        status: str = "SCRIPT_READY",
        batch_item_id: str = "",
        updated_at: str = "2026-09-19 10:00:00",
    ) -> None:
        connection = sqlite3.connect(self.path)
        connection.execute(
            "INSERT INTO original_content_item "
            "(batch_item_id, script_id, status, result_json, updated_at) "
            "VALUES (?,?,?,?,?)",
            (batch_item_id or f"{script_id}-row", script_id, status, payload, updated_at),
        )
        connection.commit()
        connection.close()

    def update(self, script_id: str, payload: str, *, batch_item_id: str = "") -> None:
        """A re-export: same row, new frozen payload."""

        connection = sqlite3.connect(self.path)
        connection.execute(
            "UPDATE original_content_item SET result_json=?, updated_at=? "
            "WHERE batch_item_id=?",
            (payload, "2026-09-20 09:00:00", batch_item_id or f"{script_id}-row"),
        )
        connection.commit()
        connection.close()

    def stored(self, script_id: str) -> dict:
        connection = sqlite3.connect(self.path)
        row = connection.execute(
            "SELECT result_json FROM original_content_item WHERE script_id=?",
            (script_id,),
        ).fetchone()
        connection.close()
        return json.loads(row[0]) if row else {}


class PromptSignatureTest(unittest.TestCase):
    """The tripwire, and the reason durations cannot be used as one."""

    def test_the_necklace_prompt_carries_the_pinned_signature(self):
        self.assertEqual(
            necklace_v1_prompt_signature(nmx_prompt()), NECKLACE_HANDOFF_SHOT_SIGNATURE
        )

    def test_no_existing_rotation_carries_the_necklace_signature(self):
        for label, header in (
            ("AMX_A_WORN_FIRST", AMX_NAME_FIRST),
            ("AMX_C_DETAIL_FIRST", AMX_DETAIL_FIRST_4_3_4_4),
        ):
            with self.subTest(rotation=label):
                self.assertNotEqual(
                    necklace_v1_prompt_signature(f"{header}\n画面事件：一段画面。"),
                    NECKLACE_HANDOFF_SHOT_SIGNATURE,
                )

    def test_a_prompt_without_shot_headers_has_no_signature(self):
        self.assertEqual(necklace_v1_prompt_signature(PLAIN_PROMPT), ())
        self.assertEqual(necklace_v1_prompt_signature(""), ())
        self.assertEqual(necklace_v1_prompt_signature(None), ())

    def test_the_signature_ignores_the_time_ranges(self):
        # AMX_C_DETAIL_FIRST really does run 0-4/4-7/7-11/11-15, so a detector
        # built on durations alone would flag a correct earring film here.
        slow = AMX_DETAIL_FIRST_4_3_4_4.replace("0-4s", "0-4s")
        self.assertIn("0-4s", slow)
        self.assertNotEqual(
            necklace_v1_prompt_signature(slow), NECKLACE_HANDOFF_SHOT_SIGNATURE
        )

    def test_the_two_audit_layers_use_different_digests(self):
        # Why the gate recomputes each hash with its own algorithm instead of
        # comparing them: they can never be equal.
        prompt = nmx_prompt()
        self.assertEqual(len(shared_hash(prompt)), 20)
        self.assertEqual(len(local_hash(prompt)), 16)
        self.assertNotEqual(shared_hash(prompt), local_hash(prompt))

    def test_every_header_prefix_the_renderer_may_use_is_recognized(self):
        for prefix in ("片段", "拍摄片段", "连续内容段"):
            headers = tuple(
                header.replace("【片段", f"【{prefix}") for header in NMX_HEADERS
            )
            with self.subTest(prefix=prefix):
                self.assertEqual(
                    necklace_v1_prompt_signature(nmx_prompt(headers=headers)),
                    NECKLACE_HANDOFF_SHOT_SIGNATURE,
                )

    def test_the_pinned_literals_still_exist_in_the_generator_source(self):
        # Drift guard: the consumer pins a profile name, a template id and two
        # audit revisions.  If the generator renames one of them, the gate has
        # to be updated in the same change rather than silently stopping to
        # match anything.
        if not GENERATOR_ROOT.exists():
            self.skipTest("original-script-generator 不在本工作区")
        sources = {
            "config/necklace_mixed_v1.json": SKILLS_ROOT
            / "original-script-generator/config/necklace_mixed_v1.json",
            "core/necklace_mixed_profile.py": GENERATOR_ROOT
            / "core/necklace_mixed_profile.py",
            "core/accessory_mixed_templates.py": GENERATOR_ROOT
            / "core/accessory_mixed_templates.py",
        }
        blob = "\n".join(
            path.read_text(encoding="utf-8") for path in sources.values()
        )
        for literal in (
            NECKLACE_HANDOFF_PROFILE,
            NECKLACE_HANDOFF_TEMPLATE_ID,
            NECKLACE_HANDOFF_AUDIT_VERSION,
            NECKLACE_HANDOFF_SHARED_AUDIT_VERSION,
        ):
            with self.subTest(literal=literal):
                self.assertIn(literal, blob)

    def test_the_shot_header_pattern_still_matches_the_generator_renders(self):
        if not GENERATOR_ROOT.exists():
            self.skipTest("original-script-generator 不在本工作区")
        source = (GENERATOR_ROOT / "core/accessory_mixed_templates.py").read_text(
            encoding="utf-8"
        )
        # The generator's own header pattern lists every prefix it may render.
        for alternative in ("拍摄片段", "连续内容段", "片段"):
            with self.subTest(alternative=alternative):
                self.assertIn(alternative, source)
        for header in NMX_HEADERS + NMX_HEADERS_LONG_FORM + (
            AMX_NAME_FIRST,
            AMX_DETAIL_FIRST_4_3_4_4,
        ):
            with self.subTest(header=header):
                self.assertTrue(necklace_v1_prompt_signature(header))


class NecklaceHandoffGateTest(unittest.TestCase):
    """The verdicts themselves, one contract failure at a time."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.source = _FakeSource(self.root)
        reset_necklace_handoff_cache()

    def tearDown(self):
        reset_necklace_handoff_cache()
        self._tmp.cleanup()

    def verdict(self, prompt: str, script_id: str = "S1") -> str:
        return check_necklace_handoff(
            script_id=script_id, prompt=prompt, db_path=self.source.path
        )

    def test_a_clean_necklace_row_passes(self):
        prompt = nmx_prompt()
        self.source.add("S1", result_json(prompt))
        self.assertEqual(self.verdict(prompt), "")

    def test_an_edited_prompt_is_refused(self):
        prompt = nmx_prompt()
        self.source.add("S1", result_json(prompt))
        reason = self.verdict(nmx_prompt("画面事件：被人手补了一句。"))
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_PROMPT_CHANGED), reason)

    def test_a_local_binding_that_misses_the_published_text_is_refused(self):
        # The snapshot itself is intact; the necklace layer's own digest points
        # at a different string, so its PASS is not an assertion about what was
        # published.  That is a snapshot mismatch, not an edit.
        prompt = nmx_prompt()
        validation = validation_payload(prompt)
        validation["necklace_audit"]["prompt_hash"] = local_hash("另一版文本")
        self.source.add("S1", result_json(prompt, validation=validation))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_SNAPSHOT_MISMATCH), reason)

    def test_a_row_exported_before_the_snapshot_existed_is_refused(self):
        # Round 1 shipped rows whose only verdict is the generation-time audit.
        # That verdict is about an earlier render, and after a re-export it is
        # not about the text on the row at all -- so it cannot admit the row.
        prompt = nmx_prompt()
        self.source.add("S1", result_json(prompt, snapshot=None))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_SNAPSHOT_MISSING), reason)
        self.assertIn("重新导出", reason)

    def test_a_snapshot_from_another_item_is_refused(self):
        # One row's PASS must never cover another row's text.
        prompt = nmx_prompt()
        snapshot = delivery_snapshot_block(prompt, batch_item_id="OCI_OTHER")
        self.source.add(
            "S1", result_json(prompt, snapshot=snapshot, batch_item_id="OCI_MINE")
        )
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_SNAPSHOT_MISMATCH), reason)

    def test_a_snapshot_frozen_against_another_contract_is_refused(self):
        prompt = nmx_prompt()
        snapshot = delivery_snapshot_block(prompt, profile_hash="deadbeefdeadbeef")
        self.source.add("S1", result_json(prompt, snapshot=snapshot))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_SNAPSHOT_MISMATCH), reason)

    def test_a_snapshot_from_an_unknown_revision_is_refused(self):
        prompt = nmx_prompt()
        snapshot = delivery_snapshot_block(
            prompt, schema_version="necklace-delivery-snapshot-v0"
        )
        self.source.add("S1", result_json(prompt, snapshot=snapshot))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_SNAPSHOT_MISMATCH), reason)
        self.assertIn(NECKLACE_DELIVERY_SNAPSHOT_SCHEMA, reason)

    def test_a_snapshot_without_the_published_text_is_refused(self):
        prompt = nmx_prompt()
        snapshot = delivery_snapshot_block(prompt)
        snapshot["prompt_text"] = ""
        self.source.add("S1", result_json(prompt, snapshot=snapshot))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_SNAPSHOT_MISMATCH), reason)

    def test_a_snapshot_that_published_other_text_is_refused(self):
        prompt = nmx_prompt()
        snapshot = delivery_snapshot_block(prompt, published_text=nmx_prompt("补了一句"))
        self.source.add("S1", result_json(prompt, snapshot=snapshot))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_PROMPT_CHANGED), reason)

    def test_the_generation_time_audit_no_longer_decides(self):
        # F4, stated directly.  The row still carries the generation-time
        # v1/FAIL blob, and a re-export wrote a fresh v2/PASS snapshot over the
        # text that is actually on the row.  Reading the stale blob is what
        # produced the "demand a re-export, then refuse the re-export" loop.
        prompt = nmx_prompt()
        stale = validation_payload(
            prompt, local_audit_version="necklace-final-prompt-audit-v1"
        )
        stale["necklace_audit"]["status"] = "FAIL"
        payload = result_json(
            prompt,
            validation=stale,
            snapshot=delivery_snapshot_block(
                prompt, validation=validation_payload(prompt)
            ),
        )
        self.assertIn("necklace-final-prompt-audit-v1", payload)
        self.source.add("S1", payload)
        self.assertEqual(self.verdict(prompt), "")
        # The stale verdict is lineage, not garbage: re-exporting appends the
        # snapshot rather than deleting the history.
        stored = self.source.stored("S1")
        self.assertEqual(
            stored["render_validation"]["necklace_audit"]["version"],
            "necklace-final-prompt-audit-v1",
        )
        self.assertEqual(
            stored[DELIVERY_SNAPSHOT_KEY]["render_validation"]["necklace_audit"]["version"],
            NECKLACE_HANDOFF_AUDIT_VERSION,
        )

    def test_two_frozen_items_that_share_a_public_id_are_ambiguous(self):
        prompt = nmx_prompt()
        for item in ("OCI_A", "OCI_B"):
            self.source.add(
                f"SCRIPT_{item}",
                result_json(
                    prompt, complete_script_id="SCSCRIPT_SHARED", batch_item_id=item
                ),
            )
        reason = check_necklace_handoff(
            script_id="SCSCRIPT_SHARED", prompt=prompt, db_path=self.source.path
        )
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_SOURCE_AMBIGUOUS), reason)

    def test_an_ambiguous_pair_of_other_categories_is_not_our_business(self):
        prompt = PLAIN_PROMPT
        contract = contract_block(
            profile="", template_id="AMX_A_WORN_FIRST", profile_hash=""
        )
        for item in ("OCI_A", "OCI_B"):
            self.source.add(
                f"SCRIPT_{item}",
                result_json(
                    prompt,
                    complete_script_id="SCSCRIPT_SHARED",
                    contract=contract,
                    batch_item_id=item,
                ),
            )
        self.assertEqual(
            check_necklace_handoff(
                script_id="SCSCRIPT_SHARED", prompt=prompt, db_path=self.source.path
            ),
            "",
        )

    def test_a_row_that_claims_necklace_without_a_frozen_identity_is_held_back(self):
        # The claim can only hold a row back, and it is consulted only when the
        # identity cannot be read.  "Could not read it" is never "not a necklace".
        reason = check_necklace_handoff(
            script_id="S_absent",
            prompt=PLAIN_PROMPT,
            product_type="项链",
            db_path=self.source.path,
        )
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_UNVERIFIED), reason)

    def test_a_claim_of_necklace_cannot_promote_a_non_necklace_row(self):
        prompt = PLAIN_PROMPT
        contract = contract_block(
            profile="", template_id="AMX_A_WORN_FIRST", profile_hash=""
        )
        self.source.add("S1", result_json(prompt, contract=contract))
        self.assertEqual(
            check_necklace_handoff(
                script_id="S1",
                prompt=prompt,
                product_type="金色项链",
                db_path=self.source.path,
            ),
            "",
        )

    def test_the_declaration_helper_only_reads_the_column(self):
        for value in ("项链", "NECKLACE", " 项链 ", "金色项链", "necklace-01"):
            with self.subTest(value=value):
                self.assertTrue(necklace_product_type_declared(value))
        for value in ("", None, "耳饰", "手链", "戒指", "发饰", "女装"):
            with self.subTest(value=value):
                self.assertFalse(necklace_product_type_declared(value))

    def test_the_declared_code_list_covers_every_refusal_of_this_round(self):
        self.assertEqual(
            len(NECKLACE_HANDOFF_ERRORS), len(set(NECKLACE_HANDOFF_ERRORS))
        )
        self.assertEqual(
            set(NECKLACE_HANDOFF_ERRORS),
            {
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
            },
        )

    def test_a_row_without_the_new_necklace_audit_is_refused(self):
        prompt = nmx_prompt()
        self.source.add("S1", result_json(prompt, validation=validation_payload(prompt, audit=None)))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_AUDIT_MISSING), reason)

    def test_an_older_necklace_audit_revision_is_refused(self):
        prompt = nmx_prompt()
        validation = validation_payload(prompt, local_audit_version="necklace-final-prompt-audit-v0")
        self.source.add("S1", result_json(prompt, validation=validation))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_STALE_LOCAL_VERSION), reason)

    def test_a_row_audited_by_the_previous_revision_is_refused(self):
        # The realistic upgrade case, not a synthetic one: rows frozen before the
        # v1 -> v2 correction carry a v1 stamp, and v1 compared the generated
        # prose against the frozen action and demanded four equal anchor lines.
        # Such a PASS is not evidence about the current checks, so the gate has to
        # keep those rows out rather than grandfather them in.
        prompt = nmx_prompt()
        validation = validation_payload(prompt, local_audit_version="necklace-final-prompt-audit-v1")
        self.source.add("S1", result_json(prompt, validation=validation))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_STALE_LOCAL_VERSION), reason)
        self.assertIn(NECKLACE_HANDOFF_AUDIT_VERSION, reason)

    def test_an_older_shared_audit_revision_is_refused(self):
        prompt = nmx_prompt()
        validation = validation_payload(prompt, shared_version="mixed-execution-audit-v0")
        self.source.add("S1", result_json(prompt, validation=validation))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_STALE_LOCAL_VERSION), reason)

    def test_a_known_fail_is_refused(self):
        prompt = nmx_prompt()
        validation = validation_payload(prompt)
        validation["necklace_audit"]["status"] = "FAIL"
        self.source.add("S1", result_json(prompt, validation=validation))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_AUDIT_FAILED), reason)

    def test_a_shared_fail_is_refused(self):
        prompt = nmx_prompt()
        validation = validation_payload(prompt, shared_status="FAIL")
        self.source.add("S1", result_json(prompt, validation=validation))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_AUDIT_FAILED), reason)

    def test_a_local_renderer_version_that_disagrees_is_refused(self):
        prompt = nmx_prompt()
        validation = validation_payload(
            prompt, local_renderer_version="production-script-renderer-v6-other"
        )
        self.source.add("S1", result_json(prompt, validation=validation))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_VERSION_MISMATCH), reason)

    def test_a_missing_renderer_version_is_refused(self):
        prompt = nmx_prompt()
        validation = validation_payload(prompt, renderer_version="")
        self.source.add("S1", result_json(prompt, validation=validation))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_VERSION_MISMATCH), reason)

    def test_an_audit_cleared_against_another_profile_revision_is_refused(self):
        prompt = nmx_prompt()
        validation = validation_payload(prompt, profile_hash="0000000000000000")
        self.source.add("S1", result_json(prompt, validation=validation))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_STALE_LOCAL_VERSION), reason)

    def test_a_feature_version_mismatch_is_refused(self):
        prompt = nmx_prompt()
        validation = validation_payload(prompt, feature_version=2)
        self.source.add("S1", result_json(prompt, validation=validation))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_STALE_LOCAL_VERSION), reason)

    def test_a_template_version_mismatch_is_refused(self):
        prompt = nmx_prompt()
        validation = validation_payload(prompt, template_version=2)
        self.source.add("S1", result_json(prompt, validation=validation))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_STALE_LOCAL_VERSION), reason)

    def test_the_version_fields_are_compared_without_coercion(self):
        # The generator stamps these straight from the contract, so a string in
        # the audit against an integer in the contract is a real disagreement,
        # not formatting noise.
        prompt = nmx_prompt()
        validation = validation_payload(prompt, template_version="1")
        self.source.add("S1", result_json(prompt, validation=validation))
        reason = self.verdict(prompt)
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_STALE_LOCAL_VERSION), reason)

    def test_every_refusal_names_one_of_the_declared_codes(self):
        prompt = nmx_prompt()
        cases = {
            "edited": (nmx_prompt("改了"), result_json(nmx_prompt())),
            "no_audit": (
                prompt,
                result_json(prompt, validation=validation_payload(prompt, audit=None)),
            ),
            "no_snapshot": (prompt, result_json(prompt, snapshot=None)),
            "snapshot_mismatch": (
                prompt,
                result_json(
                    prompt,
                    snapshot=delivery_snapshot_block(
                        prompt, profile_hash="0000000000000000"
                    ),
                ),
            ),
            "audit_failed": (
                prompt,
                result_json(
                    prompt,
                    snapshot=delivery_snapshot_block(
                        prompt,
                        validation={
                            **validation_payload(prompt),
                            "necklace_audit": {
                                **validation_payload(prompt)["necklace_audit"],
                                "status": "FAIL",
                            },
                        },
                    ),
                ),
            ),
        }
        for label, (prompt, payload) in cases.items():
            with self.subTest(case=label):
                script_id = f"S_{label}"
                self.source.add(script_id, payload)
                reason = check_necklace_handoff(
                    script_id=script_id, prompt=prompt, db_path=self.source.path
                )
                self.assertTrue(
                    any(reason.startswith(code) for code in NECKLACE_HANDOFF_ERRORS),
                    reason,
                )


class OtherCategoriesAreUntouchedTest(unittest.TestCase):
    """Nothing here may change the sync behaviour of anything but necklace V1."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        reset_necklace_handoff_cache()

    def tearDown(self):
        reset_necklace_handoff_cache()
        self._tmp.cleanup()

    def test_an_unknown_script_id_with_a_plain_prompt_is_never_refused(self):
        missing = self.root / "does_not_exist.sqlite3"
        self.assertEqual(
            check_necklace_handoff(
                script_id="S_earring", prompt=PLAIN_PROMPT, db_path=missing
            ),
            "",
        )
        self.assertFalse(missing.exists(), "只读打开不得创建数据库文件")

    def test_an_earring_row_in_the_frozen_source_is_never_refused(self):
        source = _FakeSource(self.root)
        prompt = PLAIN_PROMPT
        contract = contract_block(
            profile="", template_id="AMX_A_WORN_FIRST", profile_hash=""
        )
        source.add("S1", result_json(prompt, contract=contract, validation=validation_payload(prompt)))
        self.assertEqual(
            check_necklace_handoff(script_id="S1", prompt=prompt, db_path=source.path), ""
        )

    def test_an_unopenable_frozen_source_is_not_a_verdict_about_the_row(self):
        # A frozen source that cannot be opened says nothing about the row, so
        # it may not be read as "no such row" -- that reading is what let a
        # necklace row through in round 1.  A missing file is the same case as
        # a corrupt one: the query failed, so the answer is unavailable.
        missing = self.root / "does_not_exist.sqlite3"
        self.assertEqual(
            resolve_necklace_frozen_identity(
                script_id="S_mystery", db_path=missing
            )["status"],
            NECKLACE_LOOKUP_SOURCE_UNAVAILABLE,
        )
        reason = check_necklace_handoff(
            script_id="S_mystery", prompt=nmx_prompt(), db_path=missing
        )
        self.assertTrue(
            reason.startswith(NECKLACE_HANDOFF_SOURCE_UNAVAILABLE), reason
        )

    def test_a_readable_source_without_the_row_is_not_our_business(self):
        # The contrast: a source that *was* read, and holds no matching row, is
        # a definite answer -- and a row that does not look like a necklace then
        # keeps the sync behaviour it always had.
        source = _FakeSource(self.root)
        self.assertEqual(
            resolve_necklace_frozen_identity(
                script_id="S_mystery", db_path=source.path
            )["status"],
            NECKLACE_LOOKUP_NOT_FOUND,
        )
        self.assertEqual(
            check_necklace_handoff(
                script_id="S_mystery", prompt=PLAIN_PROMPT, db_path=source.path
            ),
            "",
        )
        # ...while a necklace-looking prompt on the same missing row is held
        # back, because nothing can confirm it.
        self.assertTrue(
            check_necklace_handoff(
                script_id="S_mystery", prompt=nmx_prompt(), db_path=source.path
            ).startswith(NECKLACE_HANDOFF_UNVERIFIED)
        )

    def test_a_necklace_signature_on_a_non_necklace_contract_is_refused(self):
        source = _FakeSource(self.root)
        prompt = nmx_prompt()
        source.add(
            "S1",
            result_json(
                prompt,
                contract=contract_block(profile="", template_id="AMX_A_WORN_FIRST", profile_hash=""),
                validation=validation_payload(prompt),
            ),
        )
        reason = check_necklace_handoff(
            script_id="S1", prompt=prompt, db_path=source.path
        )
        self.assertTrue(reason.startswith(NECKLACE_HANDOFF_UNVERIFIED), reason)

    def test_a_non_ready_row_is_not_a_frozen_identity(self):
        source = _FakeSource(self.root)
        prompt = nmx_prompt()
        source.add("S1", result_json(prompt), status="PLANNED")
        # A row that never delivered a script cannot be the frozen identity
        # behind a delivered prompt.  Round 1 had an unfiltered fallback that
        # resolved it anyway; that is what let a necklace-looking prompt be
        # judged against a row which had produced nothing.
        self.assertEqual(
            resolve_necklace_frozen_identity(script_id="S1", db_path=source.path)[
                "status"
            ],
            NECKLACE_LOOKUP_NOT_FOUND,
        )
        self.assertTrue(
            check_necklace_handoff(
                script_id="S1", prompt=prompt, db_path=source.path
            ).startswith(NECKLACE_HANDOFF_UNVERIFIED)
        )
        # A row with no necklace signal is still left alone.
        self.assertEqual(
            check_necklace_handoff(
                script_id="S1", prompt=PLAIN_PROMPT, db_path=source.path
            ),
            "",
        )

    def test_the_public_script_id_is_looked_up_unconditionally(self):
        # F1 in one test.  The workbench exports ``complete_script_id`` while the
        # table's own column holds an internal ``SCRIPT*`` id, and the delivered
        # headers carry narrative roles -- so a lookup that waited for the prompt
        # to look like a necklace never ran, and the row was admitted unverified.
        # The prompt here is deliberately plain: the lookup must not care.
        source = _FakeSource(self.root)
        prompt = PLAIN_PROMPT
        source.add(
            "INTERNAL_ID",
            result_json(prompt, complete_script_id="SCSCRIPT_PUBLIC_1"),
        )
        resolution = resolve_necklace_frozen_identity(
            script_id="SCSCRIPT_PUBLIC_1", db_path=source.path
        )
        self.assertEqual(resolution["status"], NECKLACE_LOOKUP_FOUND)
        self.assertTrue(resolution["identity"]["is_necklace_v1"])
        self.assertEqual(
            check_necklace_handoff(
                script_id="SCSCRIPT_PUBLIC_1", prompt=prompt, db_path=source.path
            ),
            "",
        )
        # ...and the same row with edited text does not.
        self.assertTrue(
            check_necklace_handoff(
                script_id="SCSCRIPT_PUBLIC_1",
                prompt=f"{prompt}\n补了一句。",
                db_path=source.path,
            ).startswith(NECKLACE_HANDOFF_PROMPT_CHANGED)
        )

    def test_the_internal_and_public_ids_reach_the_same_row(self):
        source = _FakeSource(self.root)
        prompt = nmx_prompt()
        source.add("SCRIPT_INTERNAL", result_json(prompt, complete_script_id="SCSCRIPT_PUBLIC"))
        for key in ("SCRIPT_INTERNAL", "SCSCRIPT_PUBLIC"):
            with self.subTest(key=key):
                resolution = resolve_necklace_frozen_identity(
                    script_id=key, db_path=source.path
                )
                self.assertEqual(resolution["status"], NECKLACE_LOOKUP_FOUND)
                self.assertEqual(
                    resolution["identity"]["batch_item_id"], "SCRIPT_INTERNAL-row"
                )

    def test_an_unreadable_source_does_not_break_the_gate(self):
        # "The source could not be read" is now its own outcome.  Round 1
        # collapsed it into the same ``None`` as "no such row", which reads as
        # "confirmed not a necklace" -- and that admitted the row.
        broken = self.root / "not_a_database.sqlite3"
        broken.write_text("这不是数据库", encoding="utf-8")
        self.assertEqual(
            check_necklace_handoff(
                script_id="S1", prompt=PLAIN_PROMPT, db_path=broken
            ),
            "",
        )
        self.assertEqual(
            resolve_necklace_frozen_identity(script_id="S1", db_path=broken)["status"],
            NECKLACE_LOOKUP_SOURCE_UNAVAILABLE,
        )
        for label, extra in (
            ("带镜头签名", {"prompt": nmx_prompt()}),
            ("产品类型声明项链", {"prompt": PLAIN_PROMPT, "product_type": "项链"}),
        ):
            with self.subTest(signal=label):
                reason = check_necklace_handoff(
                    script_id="S1", db_path=broken, **extra
                )
                self.assertTrue(
                    reason.startswith(NECKLACE_HANDOFF_SOURCE_UNAVAILABLE), reason
                )

    def test_default_path_follows_the_isolated_environment(self):
        with mock.patch.dict(
            os.environ,
            {
                "ORIGINAL_SCRIPT_GENERATOR_DB_PATH": "",
                "OPENCLAW_SHARED_DATA_DIR": str(self.root),
            },
            clear=False,
        ):
            self.assertEqual(
                default_necklace_db_path(),
                self.root / "original_script_generator.sqlite3",
            )
        with mock.patch.dict(
            os.environ,
            {"ORIGINAL_SCRIPT_GENERATOR_DB_PATH": str(self.root / "iso.sqlite3")},
            clear=False,
        ):
            self.assertEqual(default_necklace_db_path(), self.root / "iso.sqlite3")

    def test_the_mysql_url_does_not_hide_the_frozen_source(self):
        # BatchStorage always opens the SQLite file; the MySQL variable belongs
        # to PipelineStorage.  Treating it as "the source is elsewhere" would
        # disable the gate in the very environment it protects.
        with mock.patch.dict(
            os.environ,
            {
                "ORIGINAL_SCRIPT_GENERATOR_DATABASE_URL": "mysql+pymysql://u:p@h/db",
                "ORIGINAL_SCRIPT_GENERATOR_DB_PATH": str(self.root / "iso.sqlite3"),
            },
            clear=False,
        ):
            self.assertEqual(default_necklace_db_path(), self.root / "iso.sqlite3")
            with mock.patch.dict(os.environ, {}, clear=False):
                os.environ["ORIGINAL_SCRIPT_GENERATOR_DB_PATH"] = ""
                os.environ["OPENCLAW_SHARED_DATA_DIR"] = str(self.root)
                self.assertEqual(
                    default_necklace_db_path(),
                    self.root / "original_script_generator.sqlite3",
                )

    def test_identity_lookup_memoises_within_a_round_only(self):
        source = _FakeSource(self.root)
        prompt = nmx_prompt()
        source.add("S1", result_json(prompt))
        first = resolve_necklace_frozen_identity(script_id="S1", db_path=source.path)
        self.assertEqual(first["status"], NECKLACE_LOOKUP_FOUND)
        # A deleted row must not change the answer inside one round: the memo
        # exists because the table has no index on script_id.
        connection = sqlite3.connect(source.path)
        connection.execute("DELETE FROM original_content_item")
        connection.commit()
        connection.close()
        self.assertEqual(
            resolve_necklace_frozen_identity(script_id="S1", db_path=source.path),
            first,
        )
        # ...but the round boundary clears it.  A memo outliving the round is
        # exactly what kept a re-exported row refused forever.
        reset_necklace_handoff_cache()
        self.assertEqual(
            resolve_necklace_frozen_identity(script_id="S1", db_path=source.path)[
                "status"
            ],
            NECKLACE_LOOKUP_NOT_FOUND,
        )

    def test_priming_answers_a_whole_batch_and_never_re_asks(self):
        source = _FakeSource(self.root)
        prompt = nmx_prompt()
        for index in range(3):
            source.add(
                f"S{index}", result_json(prompt, complete_script_id=f"SCSCRIPT_{index}")
            )
        keys = ["S0", "S1", "S2", "SCSCRIPT_0", "SCSCRIPT_1", "SCSCRIPT_2"]
        self.assertEqual(prime_necklace_identity_lookup(keys, db_path=source.path), 6)
        # Every key is answered, so the second call must not touch the table.
        self.assertEqual(prime_necklace_identity_lookup(keys, db_path=source.path), 0)
        self.assertEqual(
            prime_necklace_identity_lookup(["S_absent"], db_path=source.path), 1
        )
        self.assertEqual(
            resolve_necklace_frozen_identity(
                script_id="S_absent", db_path=source.path
            )["status"],
            NECKLACE_LOOKUP_NOT_FOUND,
        )

    def test_priming_reports_an_unreadable_source_as_minus_one(self):
        broken = self.root / "not_a_database.sqlite3"
        broken.write_text("这不是数据库", encoding="utf-8")
        self.assertEqual(prime_necklace_identity_lookup(["S1"], db_path=broken), -1)
        # The failure itself is never memoised: a fresh probe still reports it.
        self.assertEqual(prime_necklace_identity_lookup(["S1"], db_path=broken), -1)


class SyncHandoffIntegrationTest(unittest.TestCase):
    """The gate as the sync actually reaches it: one row in, one error out."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.source = _FakeSource(self.root)
        reset_necklace_handoff_cache()
        self._env = mock.patch.dict(
            os.environ,
            {"ORIGINAL_SCRIPT_GENERATOR_DB_PATH": str(self.source.path)},
            clear=False,
        )
        self._env.start()

    def tearDown(self):
        self._env.stop()
        reset_necklace_handoff_cache()
        self._tmp.cleanup()

    def build(
        self,
        *,
        script_id: str,
        prompt: str,
        errors=None,
        record_id: str = "rec1",
        batch_item_id: str = "",
        product_type: str = "",
    ):
        fields = {
            "脚本ID": script_id,
            "产品编码": "P1",
            "短视频提示词": prompt,
            "进入生产": True,
        }
        if batch_item_id:
            fields["批次ItemID"] = batch_item_id
        if product_type:
            fields["产品类型"] = product_type
        mapping = resolve_original_batch_field_mapping(list(fields))
        return build_original_batch_sync_tasks(
            [TableRecord(record_id, fields)], mapping, errors=errors
        )

    def test_a_clean_necklace_row_still_produces_its_task(self):
        prompt = nmx_prompt()
        self.source.add("S1", result_json(prompt))
        tasks = self.build(script_id="S1", prompt=prompt)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].prompt_text, prompt)

    def test_every_refusal_keeps_the_task_out_and_reports_the_row(self):
        prompt = nmx_prompt()
        broken = {
            NECKLACE_HANDOFF_PROMPT_CHANGED: (nmx_prompt("补了一句"), result_json(prompt)),
            NECKLACE_HANDOFF_AUDIT_MISSING: (
                prompt,
                result_json(prompt, validation=validation_payload(prompt, audit=None)),
            ),
            NECKLACE_HANDOFF_STALE_LOCAL_VERSION: (
                prompt,
                result_json(
                    prompt,
                    validation=validation_payload(
                        prompt, local_audit_version="necklace-final-prompt-audit-v0"
                    ),
                ),
            ),
            NECKLACE_HANDOFF_AUDIT_FAILED: (
                prompt,
                result_json(
                    prompt,
                    validation={**validation_payload(prompt), "status": "FAIL"},
                ),
            ),
            NECKLACE_HANDOFF_VERSION_MISMATCH: (
                prompt,
                result_json(
                    prompt,
                    validation=validation_payload(
                        prompt, local_renderer_version="production-script-renderer-v6"
                    ),
                ),
            ),
            NECKLACE_HANDOFF_SNAPSHOT_MISSING: (
                prompt,
                result_json(prompt, snapshot=None),
            ),
            NECKLACE_HANDOFF_SNAPSHOT_MISMATCH: (
                prompt,
                result_json(
                    prompt,
                    snapshot=delivery_snapshot_block(
                        prompt, profile_hash="0000000000000000"
                    ),
                ),
            ),
        }
        mapping = resolve_original_batch_field_mapping(
            ["脚本ID", "产品编码", "短视频提示词", "进入生产"]
        )
        for expected, (row_prompt, payload) in broken.items():
            with self.subTest(code=expected):
                script_id = f"S_{expected}"
                self.source.add(script_id, payload)
                errors = {}
                tasks = build_original_batch_sync_tasks(
                    [
                        TableRecord(
                            "rec1",
                            {
                                "脚本ID": script_id,
                                "产品编码": "P1",
                                "短视频提示词": row_prompt,
                                "进入生产": True,
                            },
                        )
                    ],
                    mapping,
                    errors=errors,
                )
                self.assertEqual(tasks, [])
                self.assertIn("rec1", errors)
                self.assertTrue(errors["rec1"].startswith(expected), errors["rec1"])

    def test_a_non_necklace_row_is_byte_identical_to_the_ungated_behaviour(self):
        prompt = "【脚本ID】\n- S9\n\n耳饰最终提示词。"
        self.source.add("S9", result_json(prompt))
        gated = self.build(script_id="S9", prompt=prompt)
        with mock.patch(
            "core.necklace_handoff.check_necklace_handoff", return_value=""
        ):
            uncovered = self.build(script_id="S9", prompt=prompt)
        self.assertEqual(len(gated), 1)
        self.assertEqual(
            json.dumps(gated[0].__dict__, ensure_ascii=False, sort_keys=True, default=str),
            json.dumps(uncovered[0].__dict__, ensure_ascii=False, sort_keys=True, default=str),
        )

    def test_a_missing_gate_module_does_not_block_other_categories(self):
        prompt = "【脚本ID】\n- S9\n\n耳饰最终提示词。"
        with mock.patch.dict("sys.modules", {"core.necklace_handoff": None}):
            tasks = self.build(script_id="S9", prompt=prompt)
        self.assertEqual(len(tasks), 1)

    def test_a_crashing_gate_holds_back_only_necklace_looking_prompts(self):
        # The safety net lives inside ``check_necklace_handoff`` itself, so the
        # only way to exercise it honestly is to break one of its own steps.
        # The row is a real necklace row, so the crash happens *after* the
        # identity resolved -- which is the case the net exists for.
        plain = "【脚本ID】\n- S9\n\n耳饰最终提示词。"
        self.source.add("S9", result_json(nmx_prompt()))
        with mock.patch(
            "core.necklace_handoff._verify", side_effect=RuntimeError("boom")
        ):
            self.assertEqual(
                check_necklace_handoff(
                    script_id="S9", prompt=plain, db_path=self.source.path
                ),
                "",
            )
            held_back = check_necklace_handoff(
                script_id="S9", prompt=nmx_prompt(), db_path=self.source.path
            )
        self.assertTrue(held_back.startswith(NECKLACE_HANDOFF_UNVERIFIED), held_back)

    def test_a_crashing_gate_does_not_break_the_sync_for_other_categories(self):
        plain = "【脚本ID】\n- S9\n\n耳饰最终提示词。"
        with mock.patch(
            "core.necklace_handoff._check_necklace_handoff",
            side_effect=RuntimeError("boom"),
        ):
            errors = {}
            tasks = self.build(script_id="S9", prompt=plain, errors=errors)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(errors, {})

    def test_the_gate_leaves_the_source_checkbox_and_regeneration_alone(self):
        # A refusal is reported through the existing preflight channel only: no
        # task, no un-checking 进入生产, no model call.  The sync never rewrites
        # the source row's checkbox for a preflight error.
        prompt = nmx_prompt()
        self.source.add("S1", result_json(nmx_prompt("改过")))
        errors = {}
        tasks = self.build(script_id="S1", prompt=prompt, errors=errors)
        self.assertEqual(tasks, [])
        self.assertIn("rec1", errors)

    def test_without_an_error_collector_the_gate_still_raises(self):
        prompt = nmx_prompt()
        self.source.add("S1", result_json(nmx_prompt("改过")))
        with self.assertRaises(ValueError) as caught:
            self.build(script_id="S1", prompt=prompt)
        self.assertIn(NECKLACE_HANDOFF_PROMPT_CHANGED, str(caught.exception))

    def test_a_re_export_is_seen_by_the_next_round_in_the_same_process(self):
        # F4 at the level the sync actually runs.  Round 1 refuses for want of a
        # snapshot; the operator re-exports; round 2 -- same process, same row --
        # must see the new snapshot.  A memo that outlived the round would keep
        # refusing the row that was just fixed, forever.
        prompt = nmx_prompt()
        self.source.add("S1", result_json(prompt, snapshot=None))
        first = {}
        self.assertEqual(self.build(script_id="S1", prompt=prompt, errors=first), [])
        self.assertTrue(
            first["rec1"].startswith(NECKLACE_HANDOFF_SNAPSHOT_MISSING), first
        )
        self.source.update("S1", result_json(prompt))
        second = {}
        tasks = self.build(script_id="S1", prompt=prompt, errors=second)
        self.assertEqual(len(tasks), 1, second)
        self.assertEqual(second, {})

    def test_only_the_public_id_going_through_the_adapter_is_enough(self):
        # The workbench hands the adapter the public id; the frozen row answers
        # to it.  Nothing about the prompt is consulted to decide to look it up.
        prompt = PLAIN_PROMPT
        self.source.add(
            "SCRIPT_INTERNAL",
            result_json(
                prompt, complete_script_id="SCSCRIPT_PUBLIC", batch_item_id="OCI_1"
            ),
            batch_item_id="OCI_1",
        )
        tasks = self.build(
            script_id="SCSCRIPT_PUBLIC", prompt=prompt, batch_item_id="OCI_1"
        )
        self.assertEqual(len(tasks), 1)
        edited = {}
        self.assertEqual(
            self.build(
                script_id="SCSCRIPT_PUBLIC",
                prompt=f"{prompt}\n补了一句。",
                batch_item_id="OCI_1",
                errors=edited,
            ),
            [],
        )
        self.assertTrue(
            edited["rec1"].startswith(NECKLACE_HANDOFF_PROMPT_CHANGED), edited
        )

    def test_a_declared_necklace_row_without_a_frozen_identity_is_held_back(self):
        # Through the adapter, including the 产品类型 column reading.
        errors = {}
        tasks = self.build(
            script_id="S_absent", prompt=PLAIN_PROMPT, product_type="项链", errors=errors
        )
        self.assertEqual(tasks, [])
        self.assertTrue(
            errors["rec1"].startswith(NECKLACE_HANDOFF_UNVERIFIED), errors
        )

    def test_an_unreadable_frozen_source_holds_back_a_declared_necklace_row(self):
        broken = self.root / "not_a_database.sqlite3"
        broken.write_text("这不是数据库", encoding="utf-8")
        self._env.stop()
        self._env = mock.patch.dict(
            os.environ,
            {"ORIGINAL_SCRIPT_GENERATOR_DB_PATH": str(broken)},
            clear=False,
        )
        self._env.start()
        errors = {}
        tasks = self.build(
            script_id="S_absent", prompt=PLAIN_PROMPT, product_type="项链", errors=errors
        )
        self.assertEqual(tasks, [])
        self.assertTrue(
            errors["rec1"].startswith(NECKLACE_HANDOFF_SOURCE_UNAVAILABLE), errors
        )


if __name__ == "__main__":
    unittest.main()
