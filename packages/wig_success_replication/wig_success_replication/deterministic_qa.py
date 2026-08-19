"""Deterministic completeness, identity and creative-novelty checks."""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass
from typing import Any

from .hashing import creative_signature_hash, prompt_hash
from .models import CreativeSignature, ProductFactCard, ReplicationCompileOutput, ReplicationPrompt, VariantPlanItem


BASE_REQUIRED_FRAGMENTS = ("只参考脸部", "产品图片", "不上传参考视频")
NON_SELF_CONTAINED = ("同原脚本", "其余同母版", "参考原脚本", "其他同上", "其余不变")
INTERNAL_CONTROL_PHRASES = (
    "条件锁",
    "当前禁止执行",
    "等待核验",
    "等待事实核验",
    "未确认时不得",
    "不得作为成片",
    "仅供内部",
    "风险登记",
)
CORE_ANCHORS = {"conflict", "early_product_reveal", "strong_contrast", "proof", "cta"}


@dataclass(frozen=True)
class PromptQAResult:
    prompt: ReplicationPrompt
    digest: str
    issues: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.issues


def _normalized_text(value: str) -> str:
    return re.sub(r"[^0-9a-záéíóúüñ]+", "", value.casefold())


def _similarity(left: str, right: str) -> float:
    a, b = _normalized_text(left), _normalized_text(right)
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b, autojunk=False).ratio()


def _legacy_voiceover(full_prompt: str) -> str:
    candidates = re.findall(r"[“\"]([^”\"]{20,})[”\"]", full_prompt)
    if not candidates:
        return ""
    spanish_markers = (" amiga", " cabello", " peluca", " mira", "tiktok")
    return max(
        candidates,
        key=lambda value: (sum(marker in f" {value.casefold()}" for marker in spanish_markers), len(value)),
    )


def _stored_signature(row: Any) -> dict[str, Any]:
    value = getattr(row, "creative_signature", None) or {}
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return value if isinstance(value, dict) else {}


def _stored_voiceover(row: Any) -> str:
    signature = _stored_signature(row)
    return str(signature.get("voiceover_text") or "") or _legacy_voiceover(str(getattr(row, "full_prompt", "")))


def _opening_key(signature: CreativeSignature | dict[str, Any]) -> tuple[str, str, str]:
    if isinstance(signature, CreativeSignature):
        values = (signature.hook_type, signature.opening_action, signature.reveal_method)
    else:
        values = (
            str(signature.get("hook_type") or ""),
            str(signature.get("opening_action") or ""),
            str(signature.get("reveal_method") or ""),
        )
    return tuple(_normalized_text(value) for value in values)


def _order_change_count(reference: list[str], candidate: list[str]) -> int:
    total = max(len(reference), len(candidate))
    return sum(
        index >= len(reference)
        or index >= len(candidate)
        or _normalized_text(reference[index]) != _normalized_text(candidate[index])
        for index in range(total)
    )


def inspect_prompt(
    prompt: ReplicationPrompt,
    product: ProductFactCard,
    *,
    required_fragments: tuple[str, ...] = BASE_REQUIRED_FRAGMENTS,
) -> PromptQAResult:
    text = prompt.full_prompt.strip()
    folded = text.casefold()
    issues: list[str] = []
    if not text:
        issues.append("empty_full_prompt")
    for fragment in required_fragments:
        if fragment.casefold() not in folded:
            issues.append(f"missing_required_fragment:{fragment}")
    for phrase in NON_SELF_CONTAINED:
        if phrase.casefold() in folded:
            issues.append(f"non_self_contained:{phrase}")
    for phrase in INTERNAL_CONTROL_PHRASES:
        if phrase.casefold() in folded:
            issues.append(f"internal_control_language:{phrase}")
    for claim in product.forbidden_claims:
        if claim.strip() and claim.strip().casefold() in folded:
            issues.append(f"forbidden_claim:{claim.strip()}")
    voiceover = _normalized_text(prompt.creative_signature.voiceover_text)
    if voiceover and voiceover not in _normalized_text(text):
        issues.append("signature_voiceover_not_in_full_prompt")
    return PromptQAResult(prompt=prompt, digest=prompt_hash(text), issues=tuple(issues))


def inspect_compile_output(
    output: ReplicationCompileOutput,
    plan: list[VariantPlanItem],
    product: ProductFactCard,
    *,
    existing_prompts: list[Any] | None = None,
) -> list[PromptQAResult]:
    existing_prompts = existing_prompts or []
    expected = {(item.variant_key, item.mutation_key): item for item in plan}
    by_sequence = {item.sequence_no: item for item in output.outputs}
    seen: set[tuple[str, str]] = set()
    seen_signature_hashes = {
        creative_signature_hash(value)
        for row in existing_prompts
        if (value := _stored_signature(row)) and not value.get("legacy")
    }
    seen_general_openings = {
        _opening_key(value)
        for row in existing_prompts
        if getattr(row, "replication_mode", "") == "general"
        and (value := _stored_signature(row))
        and all(_opening_key(value))
    }
    seen_general_voiceovers = [
        _stored_voiceover(row)
        for row in existing_prompts
        if getattr(row, "replication_mode", "") == "general" and _stored_voiceover(row)
    ]

    reference_voiceover = ""
    reference_proof_order: list[str] = []
    for row in existing_prompts:
        if getattr(row, "sequence_no", 0) == 1:
            reference_voiceover = _stored_voiceover(row)
            reference_proof_order = list(_stored_signature(row).get("proof_order") or [])
            break
    if not reference_voiceover and 1 in by_sequence:
        reference_voiceover = by_sequence[1].creative_signature.voiceover_text
        reference_proof_order = list(by_sequence[1].creative_signature.proof_order)

    results: list[PromptQAResult] = []
    for item in output.outputs:
        key = (item.variant_key, item.mutation_key)
        base = inspect_prompt(item, product)
        issues = list(base.issues)
        if key in seen:
            issues.append("duplicate_variant_mutation_key")
        seen.add(key)
        planned = expected.get(key)
        if planned is None:
            issues.append("unplanned_variant")
        else:
            if item.variant_type != planned.variant_type:
                issues.append("variant_type_mismatch")
            if item.sequence_no != planned.sequence_no:
                issues.append("sequence_no_mismatch")
            if item.replication_mode != planned.replication_mode:
                issues.append("replication_mode_mismatch")
            if item.creative_route != planned.creative_route:
                issues.append("creative_route_mismatch")
            if not set(planned.change_dimensions).issubset(item.creative_signature.changed_dimensions):
                issues.append("planned_change_dimensions_missing")

        signature = item.creative_signature
        if set(signature.core_anchors) != CORE_ANCHORS:
            issues.append("core_anchors_incomplete")
        signature_digest = creative_signature_hash(signature)
        if signature_digest in seen_signature_hashes:
            issues.append("duplicate_creative_signature")
        seen_signature_hashes.add(signature_digest)

        if item.replication_mode == "high_fidelity" and item.sequence_no in {2, 3} and reference_voiceover:
            similarity = _similarity(reference_voiceover, signature.voiceover_text)
            if similarity >= 0.985:
                issues.append("high_fidelity_voiceover_is_verbatim")
            elif similarity < 0.70:
                issues.append(f"high_fidelity_voiceover_too_different:{similarity:.3f}")

        if item.replication_mode == "general":
            if len(set(signature.changed_dimensions)) < 3:
                issues.append("general_changed_dimensions_below_3")
            if len(signature.proof_actions) < 2:
                issues.append("general_proof_actions_below_2")
            if reference_voiceover:
                similarity = _similarity(reference_voiceover, signature.voiceover_text)
                if similarity > 0.70:
                    issues.append(f"general_voiceover_too_similar_to_h1:{similarity:.3f}")
                elif similarity < 0.40:
                    issues.append(f"general_voiceover_lost_mother_semantics:{similarity:.3f}")
            if reference_proof_order and _order_change_count(reference_proof_order, signature.proof_order) < 2:
                issues.append("general_proof_order_changes_below_2")
            elif not reference_proof_order and len(signature.proof_order) < 2:
                issues.append("general_proof_order_below_2")
            opening = _opening_key(signature)
            if opening in seen_general_openings:
                issues.append("duplicate_general_opening_signature")
            seen_general_openings.add(opening)
            for prior in seen_general_voiceovers:
                similarity = _similarity(prior, signature.voiceover_text)
                if similarity > 0.75:
                    issues.append(f"general_voiceover_too_similar_to_existing:{similarity:.3f}")
                    break
            seen_general_voiceovers.append(signature.voiceover_text)

        results.append(PromptQAResult(item, base.digest, tuple(issues)))

    # Missing planned outputs are observed by the caller because their grouped
    # result list remains empty. Keep explicit markers only for diagnostics.
    missing = set(expected) - seen
    if missing:
        marker = _marker("missing", f"missing_outputs:{len(missing)}")
        results.append(marker)
    if len(output.outputs) != len(plan) and not missing:
        results.append(_marker("count", "output_count_mismatch"))
    return results


def _marker(kind: str, issue: str) -> PromptQAResult:
    signature = CreativeSignature(
        core_anchors=["conflict", "early_product_reveal", "strong_contrast", "proof", "cta"],
        hook_type=kind,
        opening_action=kind,
        reveal_method=kind,
        voiceover_text=kind,
        timing_pattern=[kind],
        proof_actions=[kind],
        proof_order=[kind],
        cta_expression=kind,
        ending_composition=kind,
        changed_dimensions=["baseline"],
    )
    prompt = ReplicationPrompt(
        variant_type=kind,
        sequence_no=1,
        replication_mode="high_fidelity",
        creative_route="H1",
        variant_key=kind,
        mutation_key=kind,
        change_summary=kind,
        full_prompt=kind,
        creative_signature=signature,
        qa={"passed": False, "issues": []},
    )
    return PromptQAResult(prompt, "", (issue,))
