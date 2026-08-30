"""Isolated PLAN -> VISUAL -> VOICEOVER -> QC pipeline for organic seeding."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from core.llm_client import OriginalScriptLLMClient

from ..contracts import BranchVersions, SEEDING_ORGANIC, stable_hash, stable_id
from ..product_evidence import (
    build_product_visual_evidence_prompt,
    merge_product_visual_evidence,
    normalize_product_visual_evidence,
)
from ..storage import BranchArtifactStorage
from ..video_execution import ORGANIC_LIFESTYLE_PROFILE, compile_video_execution_brief
from .contracts import OrganicSeedThemeContract
from .claim_adapter import CentralClaimProvider, OrganicClaimAdapter
from .creative_qc import evaluate_organic_creative
from .normalizers import normalize_visual_blueprint, normalize_voiceover
from .planner import allocate_seed_themes, compile_visual_intent, normalize_product_truth
from .prompts import build_organic_visual_prompt, build_organic_voiceover_prompt
from .qc import evaluate_organic_batch, evaluate_organic_script, evaluate_organic_visual
from .retention_contract import compile_retention_contract
from .renderer import render_complete_script, render_video_prompt
from .topic_engine import allocate_topic_contracts
from .story_planner import build_organic_context_snapshot, plan_organic_stories


VERSIONS = BranchVersions(
    shared_kernel_version="short-video-shared-kernel-v6-governed-product-claims",
    branch_policy_version="organic-seeding-policy-v9.1-story-soft-selection",
    branch_prompt_version="organic-seeding-prompts-v9.1-native-spoken-brief",
)


def default_db_path() -> Path:
    explicit = str(os.environ.get("ORGANIC_SEEDING_DB_PATH") or "").strip()
    if explicit:
        return Path(explicit).expanduser()
    root = Path(
        os.environ.get(
            "OPENCLAW_SHARED_DATA_DIR",
            str(Path.home() / ".openclaw" / "shared" / "data"),
        )
    )
    return root / "organic_seeding_generator.sqlite3"


def _preview_visual(
    theme: OrganicSeedThemeContract,
    duration: float,
    retention_contract: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    retention = dict(retention_contract or {})
    opening_seconds = min(2.0, max(1.2, round(duration * 0.12, 1)))
    middle_seconds = round((duration - opening_seconds) / 3, 1)
    ending_seconds = round(duration - opening_seconds - middle_seconds * 2, 1)
    payoff_focus = "PRIMARY" if theme.product_role in {"HERO", "SUPPORTING"} else "SECONDARY"
    return {
        "schema_version": "organic-visual-blueprint-v5-preview",
        "script_title": theme.memory_residue,
        "creative_design": {"creator": "待模型设计", "scene": theme.lived_context, "lived_moment": theme.lived_context},
        "opening_design": {
            "subject": "话题张力",
            "visible_action": retention.get("first_800ms_visual_move") or "从动作中点进入",
            "viewer_value": theme.viewer_payoff,
            "first_frame_tension": retention.get("first_frame_job") or "等待被解释的视觉关系",
            "first_3s_open_loop": retention.get("first_3s_open_loop") or "先给结果，稍后解释",
        },
        "capture_units": [
            {"unit_id": "C1", "duration_seconds": opening_seconds, "narrative_job": "LIVED_MOMENT", "product_focus": "BACKGROUND", "product_interaction": "NONE", "shot_size": "MEDIUM", "body_coverage": "HEAD_TO_HIP", "camera_relation": "FIXED_TRIPOD", "setup_id": "SETUP_1", "shot": "动作中点关系景", "camera_action": "固定机位", "subject_action": "从未完成动作中进入画面", "authorized_props": [], "visible_zones": ["FRONT"], "product_evidence": "", "evidence_refs": [], "fact_refs": [], "information_gain": "立即建立等待解释的视觉关系"},
            {"unit_id": "C2", "duration_seconds": middle_seconds, "narrative_job": "RELATION", "product_focus": "SECONDARY", "product_interaction": "NATURAL_USE", "shot_size": "FULL", "body_coverage": "HEAD_TO_KNEE", "camera_relation": "FIXED_TRIPOD", "setup_id": "SETUP_1", "shot": "整体关系景", "camera_action": "普通直接切镜", "subject_action": theme.product_connection, "authorized_props": [], "visible_zones": ["FULL_BODY"], "product_evidence": "", "evidence_refs": [], "fact_refs": [], "information_gain": "第一次看清人物、商品和整体造型关系"},
            {"unit_id": "C3", "duration_seconds": middle_seconds, "narrative_job": "RELATION", "product_focus": payoff_focus, "product_interaction": "NONE", "shot_size": "MEDIUM", "body_coverage": "HEAD_TO_HIP", "camera_relation": "FIXED_TRIPOD", "setup_id": "SETUP_1", "shot": "判断依据关系景", "camera_action": "普通直接切镜", "subject_action": "用一个新角度完成开场问题的回答", "authorized_props": [], "visible_zones": ["FRONT", "SIDE"], "product_evidence": "", "evidence_refs": [], "fact_refs": [], "information_gain": "回答开场悬念"},
            {"unit_id": "C4", "duration_seconds": ending_seconds, "narrative_job": "ATMOSPHERE", "product_focus": "SECONDARY", "product_interaction": "NONE", "shot_size": "FULL", "body_coverage": "HEAD_TO_KNEE", "camera_relation": "FIXED_TRIPOD", "setup_id": "SETUP_1", "shot": "自然结果景", "camera_action": "普通直接切镜", "subject_action": "带着已经完成的判断回到生活动作", "authorized_props": [], "visible_zones": ["FULL_BODY"], "product_evidence": "", "evidence_refs": [], "fact_refs": [], "information_gain": "给出完整结果与讨论落点"},
        ],
        "product_role_realization": {"role": theme.product_role, "first_prominent_unit": "C2", "memory_residue": theme.memory_residue},
        "commerce_elements": {"price": False, "promotion": False, "purchase_cta": False, "cart_reference": False},
    }


def _preview_voiceover(
    theme: OrganicSeedThemeContract,
    language: str,
    duration: float,
    topic_contract: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    topic = dict(topic_contract or {})
    return {
        "schema_version": "organic-voiceover-v1-preview",
        "target_text": f"PREVIEW_ONLY ({language})",
        "chinese_translation": f"先说结论：{topic.get('topic_thesis') or theme.viewer_payoff}。这里保留为个人审美观察。",
        "estimated_duration_seconds": min(duration, 10),
        "hook_surface": topic.get("attention_mechanism") or "结果先行",
        "topic_realization": topic.get("topic_thesis") or theme.viewer_payoff,
        "closing_mode": "OPEN_DISCUSSION" if theme.interaction_ending_allowed else "NATURAL_END",
        "used_fact_refs": list(theme.allowed_fact_refs[:1]),
        "experience_claims": [],
        "viewer_payoff_realization": theme.viewer_payoff,
        "commerce_elements": {"price": False, "promotion": False, "purchase_cta": False, "cart_reference": False},
    }


def _item_product_truth(
    product_truth: Dict[str, Any], allowed_fact_refs: Sequence[str]
) -> Dict[str, Any]:
    """Expose only this story's core value and optional support fact."""
    allowed = {str(value or "").strip() for value in allowed_fact_refs if str(value or "").strip()}
    output = dict(product_truth)
    output["facts"] = [
        dict(item) for item in product_truth.get("facts") or []
        if isinstance(item, dict) and str(item.get("fact_id") or "").strip() in allowed
    ]
    # The model receives governed canonical facts, never the central catalogue
    # row or any direct-response projection fields.
    output["organic_claim_catalog"] = []
    snapshot = dict(product_truth.get("governed_claim_snapshot") or {})
    if snapshot:
        snapshot = {
            "schema_version": snapshot.get("schema_version"),
            "status": snapshot.get("status"),
            "snapshot_at": snapshot.get("snapshot_at"),
            "claims": [],
        }
    output["governed_claim_snapshot"] = snapshot
    return output


class OrganicSeedingPipeline:
    def __init__(
        self,
        *,
        llm_client: Optional[OriginalScriptLLMClient] = None,
        storage: Optional[BranchArtifactStorage] = None,
        claim_provider: Optional[CentralClaimProvider] = None,
        claim_adapter: Optional[OrganicClaimAdapter] = None,
    ) -> None:
        self.llm = llm_client
        self.storage = storage or BranchArtifactStorage(default_db_path())
        self.claim_provider = claim_provider or CentralClaimProvider()
        self.claim_adapter = claim_adapter or OrganicClaimAdapter()

    def _ensure_llm(self) -> OriginalScriptLLMClient:
        if self.llm is None:
            self.llm = OriginalScriptLLMClient(route="primary")
        return self.llm

    def _save_stage(
        self, *, run_id: str, item_id: str, item_index: int, stage_key: str,
        payload: Dict[str, Any], attempt: int = 1, status: str = "READY",
    ) -> None:
        self.storage.save_stage_artifact(
            {
                "run_id": run_id,
                "item_id": item_id,
                "item_index": item_index,
                "stage_key": stage_key,
                "attempt": attempt,
                "status": status,
                "payload": payload,
            }
        )

    def run(
        self,
        *,
        request_id: str,
        product_context: Dict[str, Any],
        count: int,
        theme_inputs: Sequence[Dict[str, Any]] = (),
        image_paths: Sequence[str] = (),
        duration_seconds: float = 15,
        preview_only: bool = False,
        structure_contracts: Sequence[Dict[str, Any]] = (),
        item_indices: Sequence[int] = (),
        governed_claim_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        base_truth = normalize_product_truth(product_context)
        claim_snapshot = dict(
            governed_claim_snapshot
            or product_context.get("governed_claim_snapshot")
            or self.claim_provider.snapshot(base_truth["product_code"])
        )
        truth = base_truth
        interest_engine_enabled = str(
            os.environ.get("ORGANIC_SEEDING_INTEREST_ENGINE_V1", "1")
        ).strip().lower() not in {"0", "false", "off", "no"}
        identity_snapshot = {
            "branch_key": SEEDING_ORGANIC,
            "request_id": request_id,
            "product_truth_input": truth,
            "governed_claim_snapshot": claim_snapshot,
            "count": max(1, min(20, int(count))),
            "theme_inputs": list(theme_inputs),
            "image_paths": [str(value) for value in image_paths],
            "duration_seconds": duration_seconds,
            "structure_contracts": list(structure_contracts),
            "interest_engine_enabled": interest_engine_enabled,
            "execution_mode": "PREVIEW_ONLY" if preview_only else "FORMAL",
            "versions": VERSIONS.__dict__,
        }
        input_hash = stable_hash(identity_snapshot)
        run_id = stable_id(
            "SEED_RUN_",
            {
                "branch": SEEDING_ORGANIC,
                "request_id": request_id,
                "input_hash": input_hash,
                "versions": VERSIONS.__dict__,
            },
        )
        evidence_status: Dict[str, Any] = {"status": "NOT_NEEDED", "anchors": []}
        existing_run = self.storage.get_run(run_id)
        if existing_run:
            try:
                frozen_request = json.loads(existing_run.get("request_json") or "{}")
                frozen_truth = frozen_request.get("product_truth") or {}
                if frozen_truth.get("visual_anchors"):
                    truth = normalize_product_truth(frozen_truth)
                    evidence_status = frozen_request.get("product_visual_evidence") or {
                        "status": "REUSED",
                        "anchors": truth["visual_anchors"],
                    }
            except (TypeError, ValueError):
                pass
        if not preview_only and image_paths and not truth["facts"] and not truth["visual_anchors"]:
            try:
                evidence = normalize_product_visual_evidence(
                    self._ensure_llm().call_json(
                        build_product_visual_evidence_prompt(truth),
                        image_paths=list(image_paths),
                        max_tokens=1800,
                    )
                )
                truth = merge_product_visual_evidence(truth, evidence)
                evidence_status = {
                    "status": "READY" if truth["visual_anchors"] else "EMPTY",
                    "anchors": truth["visual_anchors"],
                }
            except Exception as exc:
                evidence_status = {"status": "DEGRADED", "anchors": [], "error": str(exc)}
        # Product-image evidence is resolved before central claims are appended,
        # so a claim-rich SKU does not accidentally skip visual anchor analysis.
        enriched_context = self.claim_adapter.enrich_product_context(truth, claim_snapshot)
        truth = normalize_product_truth(enriched_context)
        organic_context = build_organic_context_snapshot(truth, theme_inputs)
        story_plan = plan_organic_stories(organic_context, count=count)
        story_spines = list(story_plan.get("selected") or [])
        themes = allocate_seed_themes(
            truth,
            count=count,
            theme_inputs=theme_inputs,
            story_spines=story_spines,
        )
        allocated_topics = allocate_topic_contracts(
            themes, truth, story_spines=story_spines
        ) if interest_engine_enabled else []
        topic_contracts = [topic.to_dict() for topic in allocated_topics]
        retention_contracts = [
            compile_retention_contract(
                theme=theme,
                topic=allocated_topics[index],
                duration_seconds=duration_seconds,
                story_spine=story_spines[index] if index < len(story_spines) else {},
            )
            for index, theme in enumerate(themes)
        ] if interest_engine_enabled else [{} for _ in themes]
        selected_indices = tuple(sorted(set(int(value) for value in item_indices)))
        if any(value < 1 or value > len(themes) for value in selected_indices):
            raise ValueError("SEEDING_ITEM_INDEX_OUT_OF_RANGE")
        requested_indices = selected_indices or tuple(range(1, len(themes) + 1))
        content_snapshot = {
            "branch_key": SEEDING_ORGANIC,
            "request_id": request_id,
            "product_truth": truth,
            "product_visual_evidence": evidence_status,
            "governed_claim_snapshot": claim_snapshot,
            "organic_claim_catalog": truth.get("organic_claim_catalog") or [],
            "organic_context_snapshot": organic_context.to_dict(),
            "story_plan": story_plan,
            "themes": [theme.to_dict() for theme in themes],
            "interest_engine_enabled": interest_engine_enabled,
            "topic_contracts": topic_contracts,
            "retention_contracts": retention_contracts,
            "duration_seconds": duration_seconds,
            "structure_contracts": list(structure_contracts),
            "versions": VERSIONS.__dict__,
        }
        request_snapshot = {
            **content_snapshot,
            # Execution selection is audit metadata, not content identity.
            # Full runs and exact retries must address the same script slots.
            "requested_item_indices": list(requested_indices),
        }
        self.storage.start_run(
            {
                "run_id": run_id,
                "branch_key": SEEDING_ORGANIC,
                "request_id": request_id,
                "product_code": truth["product_code"],
                "input_hash": input_hash,
                "shared_kernel_version": VERSIONS.shared_kernel_version,
                "branch_policy_version": VERSIONS.branch_policy_version,
                "branch_prompt_version": VERSIONS.branch_prompt_version,
                "request": request_snapshot,
            }
        )
        results: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []
        for index, theme in enumerate(themes, 1):
            if index not in requested_indices:
                continue
            structure = structure_contracts[(index - 1) % len(structure_contracts)] if structure_contracts else {}
            story_spine = story_spines[index - 1] if index <= len(story_spines) else {}
            item_truth = _item_product_truth(truth, theme.allowed_fact_refs)
            topic_contract = topic_contracts[index - 1] if interest_engine_enabled else {}
            retention_contract = retention_contracts[index - 1]
            intent = compile_visual_intent(
                theme,
                carrier_requirements=structure.get("carrier_requirements") or (),
                structure_compatibility=(structure.get("structure_id") or structure.get("macro_family_key") or "",),
                topic_contract=topic_contract,
                retention_contract=retention_contract,
                story_spine=story_spine,
            )
            item_id = stable_id(
                "SEED_ITEM_",
                {"run_id": run_id, "index": index, "intent_id": intent.intent_id, "branch": SEEDING_ORGANIC},
            )
            visual: Dict[str, Any] = {}
            voiceover: Dict[str, Any] = {}
            qc: Dict[str, Any] = {}
            video_execution_brief: Dict[str, Any] = {}
            provenance: Dict[str, Any] = {}
            try:
                if interest_engine_enabled:
                    self._save_stage(
                        run_id=run_id, item_id=item_id, item_index=index,
                        stage_key="TOPIC_PLAN",
                        payload={
                            "topic_contract": topic_contract,
                            "retention_contract": retention_contract,
                        },
                    )
                self._save_stage(
                    run_id=run_id, item_id=item_id, item_index=index,
                    stage_key="PLAN", payload={
                        "theme": theme.to_dict(),
                        "intent": intent.to_dict(),
                        "topic_contract": topic_contract,
                        "retention_contract": retention_contract,
                        "story_spine": story_spine,
                        "item_product_truth": item_truth,
                    },
                )
                if preview_only:
                    visual = normalize_visual_blueprint(
                        _preview_visual(theme, duration_seconds, retention_contract)
                    )
                    voiceover = normalize_voiceover(
                        _preview_voiceover(
                            theme, truth["target_language"], duration_seconds, topic_contract
                        ),
                        target_language=truth["target_language"],
                        duration_seconds=duration_seconds,
                    )
                    provenance = {"mode": "PREVIEW_ONLY", "model": "NONE"}
                else:
                    visual = normalize_visual_blueprint(self._ensure_llm().call_json(
                        build_organic_visual_prompt(
                            product_truth=item_truth, intent=intent, duration_seconds=duration_seconds,
                            topic_contract=topic_contract,
                            retention_contract=retention_contract,
                            story_spine=story_spine,
                        ),
                        image_paths=list(image_paths),
                        max_tokens=5200,
                    ))
                    visual_pre_qc = evaluate_organic_visual(
                        theme=theme,
                        visual_blueprint=visual,
                        product_truth=item_truth,
                        target_language=truth["target_language"],
                        duration_seconds=duration_seconds,
                    )
                    self._save_stage(
                        run_id=run_id, item_id=item_id, item_index=index,
                        stage_key="VISUAL", payload={"output": visual, "quality": visual_pre_qc},
                    )
                    if not visual_pre_qc["passed"]:
                        visual = normalize_visual_blueprint(self._ensure_llm().call_json(
                            build_organic_visual_prompt(
                                product_truth=item_truth,
                                intent=intent,
                                duration_seconds=duration_seconds,
                                topic_contract=topic_contract,
                                retention_contract=retention_contract,
                                story_spine=story_spine,
                                commerce_repair_hits=visual_pre_qc.get("matched_commerce_hits"),
                                quality_repair_reasons=visual_pre_qc["hard_errors"],
                            ),
                            image_paths=list(image_paths),
                            max_tokens=5200,
                        ))
                        visual_pre_qc = evaluate_organic_visual(
                            theme=theme,
                            visual_blueprint=visual,
                            product_truth=item_truth,
                            target_language=truth["target_language"],
                            duration_seconds=duration_seconds,
                        )
                        self._save_stage(
                            run_id=run_id, item_id=item_id, item_index=index,
                            stage_key="VISUAL", attempt=2,
                            payload={"output": visual, "quality": visual_pre_qc},
                            status="READY" if visual_pre_qc["passed"] else "BLOCKED",
                        )
                        provenance["visual_repair"] = {
                            "attempted": True,
                            "passed": visual_pre_qc["passed"],
                        }
                    if not visual_pre_qc["passed"]:
                        raise ValueError(";".join(visual_pre_qc["hard_errors"]))
                    voiceover = normalize_voiceover(self._ensure_llm().call_json(
                        build_organic_voiceover_prompt(
                            product_truth=item_truth,
                            theme=theme,
                            visual_blueprint=visual,
                            target_language=truth["target_language"] or "目标国家当地语言",
                            duration_seconds=duration_seconds,
                            topic_contract=topic_contract,
                            retention_contract=retention_contract,
                            story_spine=story_spine,
                        ),
                        image_paths=[],
                        max_tokens=2600,
                    ), target_language=truth["target_language"], duration_seconds=duration_seconds)
                    provenance = {
                        "mode": "FORMAL_MODEL",
                        "model": getattr(self.llm, "primary_model", "primary"),
                        "reasoning_effort": getattr(self.llm, "primary_reasoning_effort", ""),
                        **({"visual_repair": provenance["visual_repair"]} if "visual_repair" in provenance else {}),
                    }
                if preview_only:
                    visual_pre_qc = evaluate_organic_visual(
                        theme=theme, visual_blueprint=visual, product_truth=item_truth,
                        target_language="", duration_seconds=duration_seconds,
                    )
                video_execution_brief = compile_video_execution_brief(
                    product_truth=item_truth,
                    visual_blueprint=visual,
                    duration_seconds=duration_seconds,
                    render_profile=ORGANIC_LIFESTYLE_PROFILE,
                    attention_arc={
                        key: retention_contract.get(key)
                        for key in (
                            "first_frame_job",
                            "first_800ms_visual_move",
                            "first_3s_open_loop",
                            "payoff_window_seconds",
                            "payoff",
                            "comment_trigger",
                        )
                        if retention_contract.get(key) not in (None, "", [])
                    },
                    audio_arc=retention_contract.get("audio_arc") or {},
                )
                qc = evaluate_organic_script(
                    theme=theme,
                    visual_blueprint=visual,
                    voiceover=voiceover,
                    target_language="" if preview_only else truth["target_language"],
                    video_execution_brief=video_execution_brief,
                    duration_seconds=duration_seconds,
                    visual_pre_qc=visual_pre_qc,
                    story_spine=story_spine,
                )
                self._save_stage(
                    run_id=run_id, item_id=item_id, item_index=index,
                    stage_key="VOICEOVER", attempt=1,
                    payload={"output": voiceover, "quality": qc},
                    status="READY" if qc["passed"] else "REPAIR_REQUIRED",
                )
                # Creative thinness is a ranking signal, not a rewrite trigger.
                # Only safety/authority/language/duration hard failures receive
                # the single controlled voiceover repair.
                voice_repair_needed = bool(qc["hard_errors"])
                if not preview_only and voice_repair_needed:
                    hits = list(qc["matched_commerce_hits"])
                    voice_hits = [hit for hit in hits if str(hit.get("field_path") or "").startswith("voiceover.")]
                    reasons = list(qc["hard_errors"])
                    voiceover = normalize_voiceover(self._ensure_llm().call_json(
                        build_organic_voiceover_prompt(
                            product_truth=item_truth, theme=theme,
                            visual_blueprint=visual,
                            target_language=truth["target_language"] or "目标国家当地语言",
                            duration_seconds=duration_seconds,
                            topic_contract=topic_contract,
                            retention_contract=retention_contract,
                            story_spine=story_spine,
                            commerce_repair_hits=voice_hits,
                            quality_repair_reasons=reasons,
                        ),
                        image_paths=[], max_tokens=2600,
                    ), target_language=truth["target_language"], duration_seconds=duration_seconds)
                    qc = evaluate_organic_script(
                        theme=theme, visual_blueprint=visual, voiceover=voiceover,
                        target_language=truth["target_language"],
                        video_execution_brief=video_execution_brief,
                        duration_seconds=duration_seconds,
                        visual_pre_qc=visual_pre_qc,
                        story_spine=story_spine,
                    )
                    provenance["voice_repair"] = {
                        "attempted": True,
                        "scope": "VOICEOVER",
                        "reasons": reasons,
                        "initial_hits": hits,
                        "passed": qc["passed"],
                    }
                    if voice_hits:
                        provenance["commerce_repair"] = provenance["voice_repair"]
                if interest_engine_enabled:
                    creative_review = evaluate_organic_creative(
                        topic_contract=topic_contract,
                        retention_contract=retention_contract,
                        visual_blueprint=visual,
                        voiceover=voiceover,
                        story_spine=story_spine,
                    )
                    if preview_only:
                        creative_review["overall_grade"] = "NOT_APPLICABLE"
                        creative_review["production_recommendation"] = "PREVIEW_ONLY"
                    qc["creative_review"] = creative_review
                    dimensions = qc.get("quality_dimensions")
                    if isinstance(dimensions, dict):
                        dimensions["creative_quality"] = (
                            "PENDING_HUMAN_REVIEW" if preview_only
                            else creative_review["overall_grade"]
                        )
                        dimensions["opening_grade"] = creative_review["dimensions"]["opening_strength"]
                        dimensions["topic_clarity_grade"] = creative_review["dimensions"]["topic_clarity"]
                self._save_stage(
                    run_id=run_id, item_id=item_id, item_index=index,
                    stage_key="VOICEOVER", payload={"output": voiceover, "quality": qc},
                    attempt=2 if provenance.get("voice_repair") else 1,
                    status="READY" if qc["passed"] else "BLOCKED",
                )
                if not qc["passed"]:
                    raise ValueError(";".join(qc["hard_errors"]))
                result = {
                    "item_id": item_id,
                    "item_index": index,
                    "branch_key": SEEDING_ORGANIC,
                    "script_id": stable_id("SEED_SCRIPT_", {"item_id": item_id, "prompt_version": VERSIONS.branch_prompt_version}),
                    "seed_theme": theme.to_dict(),
                    "story_spine": story_spine,
                    "organic_claim_contract": {
                        "core_claim_ref": story_spine.get("core_claim_ref", ""),
                        "support_fact_ref": story_spine.get("support_fact_ref", ""),
                        "allowed_fact_refs": list(theme.allowed_fact_refs),
                        "evidence_mode": story_spine.get("evidence_mode", ""),
                        "allowed_strength": story_spine.get("allowed_strength", ""),
                    },
                    "topic_contract": topic_contract,
                    "retention_contract": retention_contract,
                    "visual_intent": intent.to_dict(),
                    "visual_blueprint": visual,
                    "video_execution_brief": video_execution_brief,
                    "voiceover": voiceover,
                    "quality": qc,
                    "provenance": provenance,
                    "structure_reference": structure,
                }
                result["complete_script"] = render_complete_script(result)
                result["video_generation_prompt"] = render_video_prompt(result)
                self._save_stage(
                    run_id=run_id, item_id=item_id, item_index=index,
                    stage_key="FINAL", payload={
                        "quality": qc,
                        "video_execution_brief": video_execution_brief,
                        "video_generation_prompt": result["video_generation_prompt"],
                    },
                )
                results.append(result)
                self.storage.save_item(
                    {"item_id": item_id, "run_id": run_id, "branch_key": SEEDING_ORGANIC, "item_index": index, "intent_id": intent.intent_id, "status": "SCRIPT_READY", "intent": intent.to_dict(), "result": result}
                )
            except Exception as exc:
                error = str(exc)
                failure_evidence = {
                    "visual_blueprint": visual,
                    "topic_contract": topic_contract,
                    "retention_contract": retention_contract,
                    "video_execution_brief": video_execution_brief,
                    "voiceover": voiceover,
                    "quality": qc,
                    "provenance": provenance,
                }
                failures.append({
                    "item_id": item_id, "item_index": index, "error": error,
                    "quality": qc,
                })
                self.storage.save_item(
                    {"item_id": item_id, "run_id": run_id, "branch_key": SEEDING_ORGANIC, "item_index": index, "intent_id": intent.intent_id, "status": "SCRIPT_FAILED", "intent": intent.to_dict(), "result": failure_evidence, "error_code": error.split(";", 1)[0], "error_message": error}
                )
        batch_quality = evaluate_organic_batch(results)
        for result in results:
            result["quality"]["batch_diversity"] = batch_quality
            dimensions = result["quality"].get("quality_dimensions")
            if isinstance(dimensions, dict):
                dimensions["distinctness_grade"] = batch_quality["grade"]
            self.storage.save_item(
                {
                    "item_id": result["item_id"],
                    "run_id": run_id,
                    "branch_key": SEEDING_ORGANIC,
                    "item_index": result["item_index"],
                    "intent_id": (result.get("visual_intent") or {}).get("intent_id", ""),
                    "status": "SCRIPT_READY",
                    "intent": result.get("visual_intent") or {},
                    "result": result,
                }
            )
        status = "COMPLETED" if len(results) == len(requested_indices) else ("PARTIAL" if results else "FAILED")
        from . import OrganicSeedingBranch

        output = {
            "schema_version": "organic-seeding-run-v1",
            "run_id": run_id,
            "request_id": request_id,
            "branch_key": SEEDING_ORGANIC,
            "status": status,
            "requested_count": len(requested_indices),
            "requested_item_indices": list(requested_indices),
            "ready_count": len(results),
            "failed_count": len(failures),
            "input_hash": input_hash,
            "versions": VERSIONS.__dict__,
            "publish_policy": OrganicSeedingBranch().publish_policy().to_dict(),
            "batch_quality": batch_quality,
            "items": results,
            "failures": failures,
        }
        self.storage.finish_run(run_id, status, output, failures[0]["error"] if failures else "")
        return output
