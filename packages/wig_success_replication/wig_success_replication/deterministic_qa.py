"""Deterministic completeness, identity and creative-novelty checks."""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass
from typing import Any

from .hashing import creative_signature_hash, prompt_hash
from .core_points import obvious_omissions
from .models import CreativeSignature, ProductFactCard, ReplicationCompileOutput, ReplicationPrompt, VariantPlanItem


# Asset policy belongs to the verified execution manifest, not magic words.
BASE_REQUIRED_FRAGMENTS: tuple[str, ...] = ()
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
    assessment_scope: str = "mechanical_checks_only"
    semantic_status: str = "not_evaluated"

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


def creative_history_snapshot(row: Any) -> dict[str, Any]:
    """Return model-facing history, enriching legacy rows from prompt text."""
    signature = dict(_stored_signature(row))
    voiceover = _stored_voiceover(row)
    if voiceover and not signature.get("voiceover_text"):
        signature["voiceover_text"] = voiceover
    return signature


def text_similarity(left: str, right: str) -> float:
    """Public normalization-compatible similarity used by evaluation tooling."""
    return _similarity(left, right)


def _opening_key(signature: CreativeSignature | dict[str, Any]) -> tuple[str, str, str]:
    if isinstance(signature, CreativeSignature):
        values = (signature.hook_type, signature.opening_action, signature.reveal_method)
    else:
        values = (
            str(signature.get("hook_type") or ""),
            str(signature.get("opening_action") or ""),
            str(signature.get("reveal_method") or ""),
        )
    # Signatures are commonly Chinese descriptions, unlike Spanish voiceover.
    # ASCII-only normalization would collapse every Chinese opening to empty.
    return tuple(re.sub(r"[\W_]+", "", value.casefold(), flags=re.UNICODE) for value in values)


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
    if re.search(r"(?:前|后)[一二两三四五六七八九十0-9]+张.{0,24}(?:人物|产品|脸部|商品)", text):
        issues.append("fixed_reference_index_in_prompt")
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
    publish_purpose: str = "带货",
    mother_contract: Any = None,
) -> list[PromptQAResult]:
    existing_prompts = existing_prompts or []
    expected = {(item.variant_key, item.mutation_key): item for item in plan}
    seen: set[tuple[str, str]] = set()
    seen_content = {prompt_hash(str(getattr(row, "full_prompt", ""))) for row in existing_prompts}
    results: list[PromptQAResult] = []
    for item in output.outputs:
        key = (item.variant_key, item.mutation_key)
        base = inspect_prompt(item, product)
        issues = list(base.issues)
        issues.extend(obvious_omissions(item.full_prompt, mother_contract))
        if key in seen:
            issues.append("duplicate_variant_mutation_key")
        seen.add(key)
        planned = expected.get(key)
        if planned is None:
            issues.append("unplanned_variant")
        else:
            for field in ("variant_type", "sequence_no", "replication_mode", "creative_route"):
                if getattr(item, field) != getattr(planned, field):
                    issues.append(f"{field}_mismatch")
        # Exact copied content is a mechanical defect. Semantic variety cannot
        # be certified by self-reported dimensions, signatures, or percentages.
        if base.digest in seen_content:
            issues.append("duplicate_prompt_content")
        seen_content.add(base.digest)
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
