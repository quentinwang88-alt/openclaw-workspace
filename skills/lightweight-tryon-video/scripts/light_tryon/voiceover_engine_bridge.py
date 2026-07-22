from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any, Iterable

from .utils import normalized_list, stable_hash


DEFAULT_VOICEOVER_ENGINE_ROOT = Path.home() / "voiceover_copy_engine"


def _load_engine(
    root: str | Path | None = None,
    *,
    db_path: str | Path | None = None,
    model_command: str = "",
    qc_model_command: str = "",
):
    engine_root = Path(root or DEFAULT_VOICEOVER_ENGINE_ROOT).expanduser().resolve()
    if not (engine_root / "voiceover_copy_engine").is_dir():
        raise RuntimeError(f"找不到现有口播系统: {engine_root}")
    if str(engine_root) not in sys.path:
        sys.path.insert(0, str(engine_root))
    from voiceover_copy_engine.config import Settings
    from voiceover_copy_engine.engine import VoiceoverEngine

    settings = Settings(
        db_path=Path(db_path).expanduser().resolve() if db_path else engine_root / "var" / "voiceover.sqlite",
        config_dir=engine_root / "config",
        model_command=str(model_command or ""),
        qc_model_command=str(qc_model_command or ""),
    )
    return VoiceoverEngine(settings)


def load_active_voiceover_hooks(
    root: str | Path | None = None,
    *,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Read governed ACTIVE hook archetypes from the existing voiceover library."""

    engine = _load_engine(root, db_path=db_path)
    hooks: list[dict[str, Any]] = []
    for row in engine.hooks.list_archetypes("ACTIVE"):
        mechanisms = normalized_list(row.get("attention_mechanisms_json"))
        hook_id = str(row.get("archetype_id") or "")
        allowed_focuses = ["color"] if hook_id in {"BINARY_COMPARISON", "PARTICIPATION_CHOICE"} else []
        hooks.append({
            "hook_id": hook_id,
            "hook_name": str(row.get("archetype_name_zh") or row.get("archetype_id") or ""),
            "hook_type": mechanisms[0] if mechanisms else "governed_archetype",
            "priority": 1.0,
            "weight": 1.0,
            "required_evidence": [],
            "source": "voiceover_copy_engine.hook_archetypes",
            "source_version": int(row.get("current_version") or 1),
            "risk_tags": normalized_list(row.get("hard_risks_json")),
            "allowed_visual_focuses": allowed_focuses,
        })
    if not hooks:
        raise RuntimeError("现有口播系统没有 ACTIVE 钩子原型")
    return hooks


def load_product_voiceover_usage(
    product_id: str,
    root: str | Path | None = None,
    *,
    db_path: str | Path | None = None,
    legacy_narrative_db: Any | None = None,
) -> dict[str, Any]:
    """Read shared cross-batch hook/claim usage for long-video planning."""

    engine = _load_engine(root, db_path=db_path)
    if legacy_narrative_db is not None:
        product = legacy_narrative_db.get_product(str(product_id or "")) or {}
        canonical = str(product.get("source_product_code") or product_id or "").strip()
        engine.content_memory.register_product_identity(
            canonical,
            [str(product_id or ""), str(product.get("product_id") or "")],
            source="lightweight_tryon_video",
        )
        _ensure_narrative_batch_ids(
            legacy_narrative_db,
            str(product_id or ""),
            canonical_product_id=canonical,
        )
        _backfill_narrative_voiceover_history(engine, legacy_narrative_db, product_id)
    return engine.content_memory.strategy_usage(str(product_id or "").strip(), limit=50)


def _ensure_narrative_batch_ids(
    db: Any,
    product_id: str,
    *,
    canonical_product_id: str,
) -> None:
    for variant in db.list_narrative_variants(product_id):
        if str(variant.get("production_batch_id") or "").strip():
            continue
        plan_version = str(variant.get("plan_version") or "legacy")
        batch = db.get_or_create_content_batch(
            product_id,
            f"narrative:{plan_version}",
            canonical_product_id=canonical_product_id or product_id,
        )
        db.update_narrative_variant(
            str(variant["variant_id"]), production_batch_id=str(batch["batch_id"])
        )


def _backfill_narrative_voiceover_history(engine: Any, db: Any, product_id: str) -> int:
    """Import successful legacy narrative responses whose old engine DB is gone."""

    from voiceover_copy_engine.services.content_memory import content_fingerprint

    added = 0
    for variant in db.list_narrative_variants(str(product_id or "")):
        response = variant.get("voiceover_response") if isinstance(variant.get("voiceover_response"), dict) else {}
        chinese = str(response.get("chinese_translation") or "").strip()
        target = str(response.get("voiceover_text") or "").strip()
        if not chinese or str(response.get("status") or "") != "READY_FOR_TTS":
            continue
        strategy = db.get_content_strategy(str(variant.get("strategy_group_id") or "")) or {}
        job_id = str(response.get("voiceover_engine_job_id") or f"legacy:{variant['variant_id']}")
        existing = engine.repo.list(
            "product_voiceover_content_ledger", "job_id=?", (job_id,), limit=1
        )
        selling_points = [
            value
            for value in [
                str(strategy.get("primary_selling_point") or ""),
                *normalized_list(strategy.get("secondary_selling_points")),
            ]
            if value
        ]
        if existing:
            current_context = (
                existing[0].get("source_context_json")
                if isinstance(existing[0].get("source_context_json"), dict)
                else {}
            )
            changes: dict[str, Any] = {}
            if selling_points and current_context.get("selling_points") != selling_points:
                changes["source_context_json"] = {
                    **current_context,
                    "selling_points": selling_points,
                }
            batch_id = str(
                variant.get("production_batch_id")
                or variant.get("plan_version")
                or "legacy"
            )
            if str(existing[0].get("batch_id") or "") != batch_id:
                changes["batch_id"] = batch_id
            if changes:
                engine.repo.update(
                    "product_voiceover_content_ledger",
                    "ledger_id",
                    existing[0]["ledger_id"],
                    changes,
                )
            continue
        beats = response.get("beats") if isinstance(response.get("beats"), list) else []
        claim_ids: list[str] = []
        for beat in beats:
            if not isinstance(beat, dict):
                continue
            for claim_id in normalized_list(beat.get("required_evidence")):
                if claim_id not in claim_ids:
                    claim_ids.append(claim_id)
        hook_text = str((beats[0] if beats else {}).get("chinese_translation") or "")
        engine.repo.insert("product_voiceover_content_ledger", {
            "ledger_id": "PVL_LTV_" + stable_hash(job_id, variant["variant_id"], length=16).upper(),
            "product_id": engine.content_memory.canonical_product_id(str(product_id)),
            "job_id": job_id,
            "draft_id": "",
            "video_id": str(response.get("voiceover_engine_video_id") or ""),
            "source_system": "lightweight_tryon_video",
            "source_object_ref": f"light-tryon://narrative/{variant['variant_id']}",
            "content_format": str(variant.get("format_type") or "enhanced_18_24"),
            "batch_id": str(
                variant.get("production_batch_id")
                or variant.get("plan_version")
                or "legacy"
            ),
            "source_context_json": {
                "variant_id": variant.get("variant_id"),
                "strategy_group_id": variant.get("strategy_group_id"),
                "selling_points": selling_points,
                "legacy_backfill": True,
            },
            "hook_archetype_id": str(strategy.get("hook_id") or ""),
            "claim_ids": claim_ids,
            "claim_sequence_json": claim_ids,
            "hook_text": hook_text,
            "target_text": target,
            "chinese_translation": chinese,
            "script_fingerprint": content_fingerprint(chinese or target),
            "hook_fingerprint": content_fingerprint(hook_text),
            "status": "committed",
            "counted": True,
            "expires_at": None,
            "created_at": variant.get("created_at"),
            "updated_at": variant.get("updated_at") or variant.get("created_at"),
        })
        added += 1
    return added


def run_voiceover_engine_variant(
    product: dict[str, Any],
    strategy: dict[str, Any],
    variant: dict[str, Any],
    timeline: dict[str, Any],
    *,
    root: str | Path | None = None,
    db_path: str | Path | None = None,
    model_command: str = "",
    qc_model_command: str = "",
) -> dict[str, Any]:
    """Run the existing evidence-first copy engine and adapt READY_FOR_TTS.

    The bridge does not write new copy itself. It only supplies the light-video
    product, governed hook ID and objective visual timeline to the current
    voiceover service.
    """

    engine = _load_engine(
        root,
        db_path=db_path,
        model_command=model_command,
        qc_model_command=qc_model_command,
    )
    product_id = str(product.get("product_id") or "").strip()
    variant_id = str(variant.get("variant_id") or "").strip()
    duration = int(variant.get("target_duration_seconds") or 0)
    if not product_id or not variant_id:
        raise ValueError("口播桥接缺少 product_id 或 variant_id")
    engine.content_memory.register_product_identity(
        str(product.get("source_product_code") or product_id),
        [product_id],
        source="lightweight_tryon_video",
    )
    slots = _normalize_visual_slots(timeline.get("visual_slots") or [], duration, variant_id=variant_id)
    duration_ms = int(timeline.get("duration_ms") or duration * 1000)
    if abs(duration_ms - duration * 1000) > 1000:
        raise ValueError("客观画面时间轴与增强型视频目标时长不一致")

    hook_id = str(strategy.get("hook_id") or "").strip()
    active_hook_ids = {
        str(row.get("archetype_id") or "") for row in engine.hooks.list_archetypes("ACTIVE")
    }
    if hook_id not in active_hook_ids:
        raise ValueError(f"策略钩子不是现有口播库的 ACTIVE 原型: {hook_id}")

    verified_points = normalized_list(product.get("core_selling_points"))
    if not verified_points:
        raise ValueError("商品缺少上游已确认卖点，不能生成口播")
    claim_ids = _ensure_verified_claims(engine, product_id, verified_points, variant_id)
    selected_claim_ids = _match_strategy_claims(
        engine,
        claim_ids,
        str(strategy.get("primary_selling_point") or ""),
        normalized_list(strategy.get("secondary_selling_points")),
        max_claims=_duration_claim_limit(engine, duration),
    )
    if not selected_claim_ids:
        raise ValueError("主卖点没有映射到现有口播卖点概念，不能降级成自由发挥")

    source_ref = f"light-tryon://narrative/{variant_id}"
    content_format = str(variant.get("format_type") or "enhanced_18_24")
    batch_id = str(variant.get("production_batch_id") or variant.get("plan_version") or "")
    source_context = {
        "variant_id": variant_id,
        "plan_version": variant.get("plan_version"),
        "production_batch_id": batch_id,
        "strategy_group_id": variant.get("strategy_group_id"),
        "source_job_id": variant.get("source_job_id"),
    }
    video = _get_or_register_video(
        engine,
        product_id,
        source_ref,
        duration_ms,
        variant_id,
        timeline,
        content_format=content_format,
        batch_id=batch_id,
        source_context=source_context,
    )
    analysis = engine.videos.analyze(
        video["video_id"],
        {
            "mainline_summary": str(timeline.get("mainline_summary") or "轻量试穿画面组合"),
            "visual_slots": slots,
            "overall_confidence": float(timeline.get("overall_confidence") or 0.8),
            "uncertainties": normalized_list(timeline.get("uncertainties")),
        },
    )
    job = _get_or_create_job(
        engine,
        product_id=product_id,
        video_id=video["video_id"],
        duration=duration,
        hook_id=hook_id,
        claim_ids=selected_claim_ids,
        market=str(product.get("market") or "TH"),
        language=str(product.get("language") or "th"),
        content_format=content_format,
        batch_id=batch_id,
        source_context=source_context,
    )
    ready = engine.voiceovers.run(job["job_id"])
    if ready.get("status") != "READY_FOR_TTS":
        qc = ready.get("qc") or (ready.get("draft") or {}).get("qc_json") or {}
        codes = [str(item.get("code") or "") for item in qc.get("issues") or []]
        raise ValueError(
            "现有口播流程未通过 READY_FOR_TTS 门禁"
            + (f": {','.join(code for code in codes if code)}" if codes else "")
        )
    claim_role_map = {
        claim_id: _claim_shot_roles(
            str((engine.repo.get(
                "claim_concepts",
                "concept_id",
                (engine.repo.get("product_claims", "claim_id", claim_id) or {}).get("concept_id"),
            ) or {}).get("canonical_key") or "")
        )
        for claim_id in selected_claim_ids
    }
    return _adapt_ready_package(ready, analysis, variant_id, claim_role_map=claim_role_map)


def _ensure_verified_claims(
    engine: Any,
    product_id: str,
    selling_points: Iterable[str],
    variant_id: str,
) -> list[str]:
    source = engine.claims.add_source(product_id, {
        "raw_text": "；".join(normalized_list(selling_points)),
        "source_type": "light_tryon_verified_product_input",
        "source_ref": f"light-tryon://verified-selling-points/{product_id}",
        "operator_priority": "core",
        "created_by": "light-tryon-voiceover-bridge",
    })
    claims = engine.claims.normalize(source["claim_source_id"])
    claims.extend(_ensure_exact_alias_claims(
        engine,
        product_id,
        source["claim_source_id"],
        normalized_list(selling_points),
        existing_claims=claims,
        variant_id=variant_id,
    ))
    output: list[str] = []
    for claim in claims:
        if not claim.get("concept_id") or claim.get("allowed_strength") == "forbidden":
            continue
        if claim.get("verification_status") != "VERIFIED":
            claim = engine.claims.review(claim["claim_id"], {
                "action": "VERIFY",
                "allowed_strength": claim.get("allowed_strength") or "soft_only",
                "reviewed_by": "light-tryon-upstream-verified",
                "review_note": f"复用轻视频商品卖点输入；增强变体 {variant_id}",
            })
        output.append(str(claim["claim_id"]))
    if not output:
        raise ValueError("商品卖点没有形成可用的现有口播概念")
    return output


def _ensure_exact_alias_claims(
    engine: Any,
    product_id: str,
    claim_source_id: str,
    selling_points: list[str],
    *,
    existing_claims: list[dict[str, Any]],
    variant_id: str,
) -> list[dict[str, Any]]:
    """Add only exact governed aliases omitted by an older normalization pass.

    The source is already declared upstream-verified. This does not infer a new
    claim or write copy; it only materializes an explicit alias from the active
    claim taxonomy so newly added concepts can apply to an existing product.
    """

    present = {str(row.get("concept_id") or "") for row in existing_claims}
    added: list[dict[str, Any]] = []
    strength_by_risk = {"low": "factual", "medium": "moderate", "high": "soft_only"}
    concepts = engine.repo.list("claim_concepts", "active=1", order_by="canonical_key")
    for concept in concepts:
        concept_id = str(concept.get("concept_id") or "")
        if not concept_id or concept_id in present:
            continue
        aliases = [alias for alias in normalized_list(concept.get("aliases_json")) if len(alias.strip()) >= 2]
        source_span = next(
            (
                alias
                for point in selling_points
                for alias in sorted(aliases, key=len, reverse=True)
                if alias.lower() in point.lower()
            ),
            "",
        )
        if not source_span:
            continue
        existing = engine.repo.list(
            "product_claims",
            "product_id=? AND concept_id=? AND verification_status='VERIFIED'",
            (product_id, concept_id),
            limit=1,
        )
        if existing:
            added.extend(existing)
            present.add(concept_id)
            continue
        row = {
            "claim_id": "PCL_LTV_" + stable_hash(product_id, concept_id, claim_source_id, length=16).upper(),
            "product_id": product_id,
            "claim_source_id": claim_source_id,
            "concept_id": concept_id,
            "source_span": source_span,
            "canonical_claim_zh": str(concept.get("concept_name") or source_span),
            "claim_type": str(concept.get("concept_type") or "feature"),
            "claim_theme": str(concept.get("claim_theme") or "style"),
            "verification_status": "VERIFIED",
            "evidence_requirement": "source_plus_video",
            "allowed_strength": strength_by_risk.get(str(concept.get("default_risk_level") or "medium"), "moderate"),
            "operator_priority": "core",
            "risk_tags_json": [],
            "normalizer_version": "governed-exact-alias-v1",
            "normalizer_confidence": 1.0,
            "reviewed_by": "light-tryon-upstream-verified",
            "review_note": f"受控词库完整别名命中；增强变体 {variant_id}",
        }
        engine.repo.insert("product_claims", row)
        added.append({**row, "canonical_key": concept.get("canonical_key"), "concept_name": concept.get("concept_name")})
        present.add(concept_id)
    return added


def _match_primary_claims(engine: Any, claim_ids: list[str], primary: str) -> list[str]:
    """Backward-compatible primary-only selector."""

    return _match_strategy_claims(engine, claim_ids, primary, [], max_claims=1)


def _match_strategy_claims(
    engine: Any,
    claim_ids: list[str],
    primary: str,
    secondary_points: list[str],
    *,
    max_claims: int,
) -> list[str]:
    """Select governed claims in strategy order without inventing extra facts.

    The primary point always gets first priority. For longer videos, secondary
    points may contribute additional exact taxonomy matches up to the duration
    profile's bundle limit. One secondary phrase can intentionally expose more
    than one governed fact (for example, ``短款立领``).
    """

    selected: list[str] = []
    selected_concepts: set[str] = set()

    def add_claim(claim_id: str) -> bool:
        if not claim_id or claim_id in selected:
            return False
        claim = engine.repo.get("product_claims", "claim_id", claim_id) or {}
        concept = engine.repo.get("claim_concepts", "concept_id", claim.get("concept_id")) or {}
        key = str(concept.get("canonical_key") or "")
        redundancy_group = {
            "cropped_length": "length_and_waist",
            "waist_definition": "length_and_waist",
        }.get(key, key)
        if redundancy_group and redundancy_group in selected_concepts:
            return False
        selected.append(claim_id)
        if redundancy_group:
            selected_concepts.add(redundancy_group)
        return True

    primary_matches = _claims_matching_point(engine, claim_ids, str(primary or ""))
    if primary_matches:
        add_claim(primary_matches[0])
    secondary_matches = [
        _claims_matching_point(engine, claim_ids, point)
        for point in normalized_list(secondary_points)
    ]
    # Give every configured secondary point one opportunity before taking a
    # second concept from a compound phrase such as “短款立领”. This prevents a
    # broad earlier phrase from crowding out a later explicit detail.
    for matches in secondary_matches:
        if matches:
            add_claim(matches[0])
        if len(selected) >= max(1, int(max_claims)):
            return selected
    for matches in [primary_matches, *secondary_matches]:
        for claim_id in matches:
            add_claim(claim_id)
            if len(selected) >= max(1, int(max_claims)):
                return selected
    return selected


def _claims_matching_point(engine: Any, claim_ids: list[str], point: str) -> list[str]:
    primary_text = str(point or "").strip().lower()
    if not primary_text:
        return []
    matched: list[tuple[int, str]] = []
    for claim_id in claim_ids:
        claim = engine.repo.get("product_claims", "claim_id", claim_id) or {}
        concept = engine.repo.get("claim_concepts", "concept_id", claim.get("concept_id")) or {}
        candidates = normalized_list(concept.get("aliases_json")) + [
            str(claim.get("source_span") or ""),
            str(claim.get("canonical_claim_zh") or ""),
            str(concept.get("concept_name") or ""),
        ]
        scores = [
            len(token.strip())
            for token in candidates
            if token.strip() and token.strip().lower() in primary_text
        ]
        if primary_text in " ".join(candidates).lower():
            scores.append(len(primary_text))
        if scores:
            matched.append((max(scores), claim_id))
    matched.sort(key=lambda item: (-item[0], claim_ids.index(item[1])))
    return [claim_id for _, claim_id in matched]


def _duration_claim_limit(engine: Any, duration: int) -> int:
    profile = (
        getattr(engine.voiceovers, "config", {}).get("durations", {}).get("profiles", {}).get(str(duration), {})
    )
    return max(1, int(profile.get("max_bundles") or 1))


def _get_or_register_video(
    engine: Any,
    product_id: str,
    source_ref: str,
    duration_ms: int,
    variant_id: str,
    timeline: dict[str, Any],
    *,
    content_format: str,
    batch_id: str,
    source_context: dict[str, Any],
) -> dict[str, Any]:
    rows = engine.repo.list(
        "videos",
        "product_id=? AND source_object_ref=?",
        (product_id, source_ref),
        order_by="created_at DESC",
        limit=1,
    )
    if rows:
        metadata = rows[0].get("metadata_json") if isinstance(rows[0].get("metadata_json"), dict) else {}
        metadata = {
            **metadata,
            **source_context,
            "content_format": content_format,
            "batch_id": batch_id,
            "timeline_source": "observed_media_assets",
        }
        engine.repo.update("videos", "video_id", rows[0]["video_id"], {"metadata_json": metadata})
        return engine.repo.get("videos", "video_id", rows[0]["video_id"])
    return engine.videos.register({
        "product_id": product_id,
        "source_system": "lightweight_tryon_video",
        "source_object_ref": source_ref,
        "duration_ms": duration_ms,
        "content_hash": stable_hash(variant_id, timeline, length=64),
        "metadata": {
            **source_context,
            "variant_id": variant_id,
            "content_format": content_format,
            "batch_id": batch_id,
            "timeline_source": "observed_media_assets",
        },
    })


def _get_or_create_job(
    engine: Any,
    *,
    product_id: str,
    video_id: str,
    duration: int,
    hook_id: str,
    claim_ids: list[str],
    market: str,
    language: str,
    content_format: str,
    batch_id: str,
    source_context: dict[str, Any],
) -> dict[str, Any]:
    rows = engine.repo.list(
        "voiceover_jobs",
        "product_id=? AND video_id=? AND target_duration_sec=? AND forced_hook_id=?",
        (product_id, video_id, duration, hook_id),
        order_by="created_at DESC",
        limit=1,
    )
    if rows:
        requested = {str(value) for value in claim_ids}
        existing = {str(value) for value in normalized_list(rows[0].get("forced_claim_ids"))}
        if requested == existing and _job_uses_current_timing_policy(engine, rows[0], duration):
            engine.repo.update("voiceover_jobs", "job_id", rows[0]["job_id"], {
                "content_format": content_format,
                "batch_id": batch_id,
                "source_context_json": source_context,
            })
            return engine.repo.get("voiceover_jobs", "job_id", rows[0]["job_id"])
    locale = "th-TH" if language.lower() in {"th", "th-th", "thai", "泰语"} else language
    return engine.voiceovers.create_job({
        "product_id": product_id,
        "video_id": video_id,
        "target_country": market or "TH",
        "target_locale": locale,
        "target_duration_sec": duration,
        "account_style_id": "TH_FASHION_FRIENDLY_01",
        "forced_hook_id": hook_id,
        "forced_claim_ids": claim_ids,
        "content_format": content_format,
        "batch_id": batch_id,
        "source_context": source_context,
    })


def _job_uses_current_timing_policy(engine: Any, job: dict[str, Any], duration: int) -> bool:
    """Avoid reusing READY drafts created before long-form CTA/density fixes."""

    if int(duration) < 18:
        return True
    drafts = engine.voiceovers.drafts(str(job.get("job_id") or ""))
    if not drafts:
        return False
    plan = drafts[0].get("plan_json") or {}
    beats = plan.get("beats") or []
    if not beats:
        return False
    cta = beats[-1]
    has_reserved_cta = int(cta.get("start_ms") or duration * 1000) <= duration * 1000 - 5000
    qc = drafts[0].get("qc_json") or {}
    estimated = float((qc.get("duration_estimate") or {}).get("estimated_sec") or 0)
    return has_reserved_cta and estimated >= 17.0


def _normalize_visual_slots(rows: list[Any], duration: int, *, variant_id: str = "") -> list[dict[str, Any]]:
    if not rows:
        raise ValueError("口播生成前必须提供实际成片或粗剪片的客观 visual_slots")
    result: list[dict[str, Any]] = []
    # visual_slots.slot_id is globally unique in the existing voiceover DB, not
    # merely unique inside one analysis. A fresh namespace also makes a safe
    # retry possible when the same variant is re-analysed after new shots arrive.
    slot_namespace = stable_hash(variant_id, uuid.uuid4().hex, length=10)
    for index, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            raise ValueError("visual_slots 每一项必须是对象")
        start_ms = int(raw.get("start_ms") if raw.get("start_ms") is not None else float(raw.get("start_seconds") or 0) * 1000)
        end_ms = int(raw.get("end_ms") if raw.get("end_ms") is not None else float(raw.get("end_seconds") or 0) * 1000)
        result.append({
            "slot_id": f"LTVS_{slot_namespace}_{index:02d}",
            "start_ms": start_ms,
            "end_ms": end_ms,
            "visual_event": str(raw.get("visual_event") or "").strip(),
            "observations": normalized_list(raw.get("observations")),
            "event_tags": normalized_list(raw.get("event_tags")),
            "speakable_facts": normalized_list(raw.get("speakable_facts")),
            "recommended_line_function": str(raw.get("recommended_line_function") or "proof"),
            "product_visibility": str(raw.get("product_visibility") or "medium"),
            "confidence": float(raw.get("confidence") or 0.8),
            "source_frame_indexes": normalized_list(raw.get("source_frame_indexes")),
        })
    if any(not row["visual_event"] for row in result):
        raise ValueError("visual_slots 缺少客观 visual_event")
    if result[-1]["end_ms"] > duration * 1000:
        raise ValueError("visual_slots 超出目标视频时长")
    return result


def _adapt_ready_package(
    ready: dict[str, Any],
    analysis: dict[str, Any],
    variant_id: str,
    *,
    claim_role_map: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    slots = {str(row.get("slot_id") or ""): row for row in analysis.get("visual_slots") or []}
    beats: list[dict[str, Any]] = []
    for index, line in enumerate(ready.get("lines") or [], start=1):
        function = str(line.get("function") or "proof").lower()
        role = _line_role(function)
        visual_refs = normalized_list(line.get("visual_refs"))
        visual_text = "；".join(
            str(slots.get(ref, {}).get("visual_event") or "") for ref in visual_refs if ref in slots
        )
        evidence_refs = normalized_list(line.get("claim_refs"))
        required_roles: list[str] = []
        for claim_id in evidence_refs:
            for shot_role in (claim_role_map or {}).get(claim_id, []):
                if shot_role not in required_roles:
                    required_roles.append(shot_role)
        if not required_roles:
            required_roles = ["main_wear_upper"] if role in {"hook", "cta", "decision"} else []
        beats.append({
            "beat_id": f"B{index}",
            "role": role,
            "speech_text": str(line.get("target_text") or "").strip(),
            "chinese_translation": str(line.get("chinese_translation") or "").strip(),
            "visual_intent": visual_text,
            "required_shot_roles": required_roles,
            "required_evidence": evidence_refs,
            "visual_refs": visual_refs,
            "priority": "required",
            "suggested_start_ms": int(line.get("start_ms") or 0),
            "suggested_end_ms": int(line.get("end_ms") or 0),
        })
    return {
        "status": ready.get("status"),
        "voiceover_text": str(ready.get("target_text") or "").strip(),
        "chinese_translation": str(ready.get("chinese_translation") or "").strip(),
        "beats": beats,
        "voiceover_engine_job_id": ready.get("job_id"),
        "voiceover_engine_video_id": (ready.get("video") or {}).get("video_id"),
        "voiceover_engine_analysis_id": (ready.get("video") or {}).get("analysis_id"),
        "voiceover_engine_qc": ready.get("qc") or {},
        "voiceover_engine_duration": ready.get("duration") or {},
        "variant_id": variant_id,
        "downstream_rewritten": False,
    }


def _claim_shot_roles(canonical_key: str) -> list[str]:
    mapping = {
        "soft_ivory_color": ["color_upper", "main_wear_upper"],
        "color_options": ["wear_hold_color"],
        "color_style_mood": ["wear_hold_color"],
        "cropped_length": ["detail_waistline", "fit_turn"],
        "waist_definition": ["detail_waistline", "fit_turn"],
        "stand_collar": ["detail_neckline"],
        "zip_closure": ["detail_closure"],
        "metal_snap_detail": ["detail_closure", "detail_neckline"],
        "decorative_pocket": ["detail_closure"],
        "seam_detail": ["detail_fabric"],
        "matte_texture": ["detail_fabric"],
        "cuff_hardware": ["detail_sleeve"],
        "lined_interior": ["detail_closure"],
    }
    return mapping.get(canonical_key, ["main_wear_upper"])


def _line_role(function: str) -> str:
    if "hook" in function:
        return "hook"
    if "cta" in function or "action" in function:
        return "cta"
    if "result" in function or "decision" in function:
        return "decision"
    if "color" in function:
        return "color"
    if "detail" in function:
        return "detail"
    return "proof"
