"""Visual-model QA orchestration for individual shots and the five-shot set.

The model provider stays behind ``adapter.review`` so production can reuse the
workspace's approved vision route.  Media QC and visual QA remain separate:
file dimensions alone never claim product/persona consistency.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from domain.statuses import QC_FAILED, QC_PASSED, TASK_IMAGE_REVIEW

SINGLE_DIMENSIONS = (
    "product_fidelity",
    "persona_fidelity",
    "look_fidelity",
    "scene_fit",
    "creator_realism",
)
GROUP_DIMENSIONS = (
    "product_consistency",
    "persona_consistency",
    "look_consistency",
    "scene_consistency",
    "shot_diversity",
)
PASS_SCORE = 80
HARD_FAIL_KEYS = (
    "product_fidelity",
    "product_consistency",
    "persona_fidelity",
    "persona_consistency",
)
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
CREATOR_CRM_DIR = WORKSPACE_ROOT / "skills" / "creator-crm"


class VisualQaError(RuntimeError):
    pass


@dataclass
class VisualQaDecision:
    passed: bool
    score: Optional[int]
    dimensions: Dict[str, int]
    reason_codes: List[str] = field(default_factory=list)
    notes: str = ""
    provider: str = ""
    model: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)
    next_actions: List[str] = field(default_factory=list)


@dataclass
class VisualQaReport:
    task_id: str
    passed: bool
    single: Dict[int, VisualQaDecision]
    group: VisualQaDecision


class VisualQaService:
    """Run real adapter reviews and persist auditable structured results."""

    def __init__(self, repository, adapter):
        if adapter is None or not callable(getattr(adapter, "review", None)):
            raise VisualQaError("a visual QA adapter with review(payload) is required")
        self._repository = repository
        self._adapter = adapter

    def review_task(self, task_id: str) -> VisualQaReport:
        task = self._repository.get_task(task_id)
        if task is None:
            raise VisualQaError(f"task {task_id} not found")
        if task.task_status != TASK_IMAGE_REVIEW:
            raise VisualQaError(
                f"task {task_id} status {task.task_status!r} is not image_review"
            )
        shots = self._latest(self._repository.list_shots(task_id))
        if len(shots) != 5 or any(not shot.image_url for shot in shots):
            raise VisualQaError("visual QA requires five generated image paths")

        quality_contract = (task.plan_json or {}).get("quality_contract") or {}
        single_dimensions, single_rules = self._dimension_rules(
            quality_contract, "single", SINGLE_DIMENSIONS
        )
        group_dimensions, group_rules = self._dimension_rules(
            quality_contract, "group", GROUP_DIMENSIONS
        )
        independent = bool(quality_contract)

        single: Dict[int, VisualQaDecision] = {}
        for shot in shots:
            payload = {
                "scope": "single_shot",
                "task_id": task_id,
                "slot_index": shot.slot_index,
                "image_paths": [shot.image_url],
                "product": task.product_snapshot_json.get("product", {}),
                "plan": task.plan_json or {},
                "required_dimensions": list(single_dimensions),
                "dimension_rules": single_rules,
                "decision_policy": (
                    "independent_dimensions" if independent else "legacy_average"
                ),
                "hard_fail_keys": [
                    key for key, rule in single_rules.items()
                    if rule.get("hard_gate")
                ] or ["product_fidelity"],
            }
            decision = normalize_decision(
                self._adapter.review(payload),
                single_dimensions,
                dimension_rules=single_rules if independent else None,
            )
            single[shot.slot_index] = decision
            previous = shot.qa_json or {}
            self._repository.update_shot_qa(
                shot.shot_id,
                qa_status=QC_PASSED if decision.passed else QC_FAILED,
                qa_json={
                    **previous,
                    "visual_model_qa": decision_to_dict(decision),
                },
                failure_detail=None if decision.passed else ",".join(decision.reason_codes),
            )

        if all(decision.passed for decision in single.values()):
            group = normalize_decision(
                self._adapter.review(
                    {
                        "scope": "five_shot_group",
                        "task_id": task_id,
                        "image_paths": [shot.image_url for shot in shots],
                        "product": task.product_snapshot_json.get("product", {}),
                        "plan": task.plan_json or {},
                        "required_dimensions": list(group_dimensions),
                        "dimension_rules": group_rules,
                        "decision_policy": (
                            "independent_dimensions"
                            if independent else "legacy_average"
                        ),
                        "hard_fail_keys": [
                            key for key, rule in group_rules.items()
                            if rule.get("hard_gate")
                        ] or ["product_consistency"],
                    }
                ),
                group_dimensions,
                dimension_rules=group_rules if independent else None,
            )
        else:
            group = VisualQaDecision(
                passed=False,
                score=0,
                dimensions={key: 0 for key in group_dimensions},
                reason_codes=["single_shot_qa_failed"],
                notes="group review skipped until every single shot passes",
            )
        existing = task.group_qa_json or {}
        self._repository.update_task_plan(
            task_id,
            group_qa_json={
                **existing,
                "stage_c_visual": {
                    "decision": "passed" if group.passed else "failed",
                    "single": {
                        str(slot): decision_to_dict(decision)
                        for slot, decision in single.items()
                    },
                    "group": decision_to_dict(group),
                },
            },
        )
        return VisualQaReport(task_id, group.passed, single, group)

    @staticmethod
    def _dimension_rules(
        quality_contract: Mapping[str, Any],
        scope: str,
        legacy_dimensions: Sequence[str],
    ) -> Tuple[Tuple[str, ...], Dict[str, Dict[str, Any]]]:
        configured = quality_contract.get("dimensions") or {}
        if not isinstance(configured, Mapping) or not configured:
            return tuple(legacy_dimensions), {}
        rules: Dict[str, Dict[str, Any]] = {}
        for key, raw in configured.items():
            rule = dict(raw or {}) if isinstance(raw, Mapping) else {}
            scopes = list(rule.get("scope") or ["single", "group"])
            if scope in scopes:
                rules[str(key)] = rule
        if not rules:
            raise VisualQaError(
                f"quality profile has no dimensions for scope {scope!r}"
            )
        return tuple(rules), rules

    @staticmethod
    def _latest(shots):
        by_slot = {}
        for shot in shots:
            current = by_slot.get(shot.slot_index)
            if current is None or shot.shot_version > current.shot_version:
                by_slot[shot.slot_index] = shot
        return [by_slot[index] for index in sorted(by_slot)]




# Module-level cache with namespace-hijack recovery: the openai-image skill
# also exposes a "core" package, so whichever imports last wins sys.modules.
# Caching the client class after first import makes both sides coexist.
_CREATOR_CRM_CACHE: Dict[str, Any] = {}


def _load_creator_crm_client():
    import sys

    if _CREATOR_CRM_CACHE.get("factory") is None:
        if str(CREATOR_CRM_DIR) not in sys.path:
            sys.path.insert(0, str(CREATOR_CRM_DIR))
        try:
            from core.llm_analyzer import LLMClient
        except ModuleNotFoundError:
            for root in ("core", "app"):
                for name in [
                    m
                    for m in list(sys.modules)
                    if m == root or m.startswith(root + ".")
                ]:
                    del sys.modules[name]
            if str(CREATOR_CRM_DIR) not in sys.path:
                sys.path.insert(0, str(CREATOR_CRM_DIR))
            from core.llm_analyzer import LLMClient
        _CREATOR_CRM_CACHE["factory"] = LLMClient
    return _CREATOR_CRM_CACHE["factory"]

class CreatorCrmVisualQaAdapter:
    """Production adapter reusing creator-crm's configured vision LLM route."""

    def __init__(self, client=None):
        self._client = client

    def _load_client(self):
        if self._client is None:
            self._client = _load_creator_crm_client()()
        return self._client

    def review(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        generated = [str(path) for path in payload.get("image_paths") or []]
        product = payload.get("product") or {}
        product_refs = [
            str(path)
            for path in product.get("reference_images") or []
            if isinstance(path, str) and Path(path).is_file()
        ]
        persona = ((payload.get("plan") or {}).get("persona") or {}).get("snapshot") or {}
        persona_refs = [
            str(path)
            for path in persona.get("local_reference_images")
            or persona.get("reference_images")
            or []
            if isinstance(path, str) and Path(path).is_file()
        ]
        images = product_refs + persona_refs + generated
        if not generated or not all(Path(path).is_file() for path in generated):
            raise VisualQaError("visual QA generated image path missing")
        dimensions = list(payload.get("required_dimensions") or [])
        prompt = self._prompt(
            payload, len(product_refs), len(persona_refs), dimensions
        )
        client = self._load_client()
        response = client.chat_with_multiple_images(images, prompt, max_tokens=1800)
        result = client.parse_json_response(response)
        if not isinstance(result, dict):
            raise VisualQaError("vision model returned non-object JSON")
        result.setdefault("provider", "creator-crm-vision-route")
        result.setdefault("model", str(getattr(client, "_working_model", "") or getattr(client, "model", "")))
        return result

    @staticmethod
    def _prompt(
        payload: Mapping[str, Any],
        product_ref_count: int,
        persona_ref_count: int,
        dimensions: List[str],
    ) -> str:
        scope = str(payload.get("scope") or "")
        plan = payload.get("plan") or {}
        contract = {
            "scope": scope,
            "image_order": {
                "product_reference_images_first": product_ref_count,
                "persona_reference_images_next": persona_ref_count,
                "generated_images_after": len(payload.get("image_paths") or []),
            },
            "plan_anchors": {
                "persona": plan.get("persona"),
                "look": plan.get("look"),
                "scene": plan.get("scene"),
                "shots": plan.get("shots"),
                "product_facts": plan.get("product_facts"),
                "outfit_plan": plan.get("outfit_plan"),
                "outfit_states": plan.get("outfit_states"),
                "recipe_execution": plan.get("recipe_execution"),
            },
            "score_dimensions": dimensions,
            "dimension_rules": payload.get("dimension_rules") or {},
            "decision_policy": payload.get("decision_policy") or "legacy_average",
            "hard_fail_keys": list(payload.get("hard_fail_keys") or []),
        }
        return (
            "你是跨境女装图文内容视觉质检员。严格比较商品参考图、人物参考图与生成图，"
            "并按计划锚点判断人物、穿搭、场景、真实创作者感和镜头差异。"
            "商品颜色、版型、结构、图案或材质明显漂移时，对商品相关维度打低于70分。"
            "生成图人物若与人物参考图不是同一身份，或五张图人物身份明显漂移，"
            "对应人物维度必须低于70分。不要因画面好看掩盖商品或人物错误。"
            "只输出一个 JSON 对象，不要 Markdown。"
            "JSON 格式必须是："
            '{"passed":true,"dimensions":{"维度名":0},'
            '"reason_codes":[],"notes":"简短证据"}。'
            "所有指定维度必须给出0-100整数。若 decision_policy=independent_dimensions，"
            "每个维度必须分别达到其 min_score，禁止用平均分掩盖失败维度；否则使用旧平均分规则。"
            "\n质检合同：" + json.dumps(contract, ensure_ascii=False)
        )


def normalize_decision(
    raw: Mapping[str, Any],
    dimensions,
    *,
    dimension_rules: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> VisualQaDecision:
    if not isinstance(raw, Mapping):
        raise VisualQaError("visual QA adapter must return an object")
    scores: Dict[str, int] = {}
    raw_scores = raw.get("dimensions") or {}
    for key in dimensions:
        value = raw_scores.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise VisualQaError(f"visual QA dimension {key!r} must be numeric")
        scores[key] = max(0, min(100, int(round(value))))
    rules = dict(dimension_rules or {})
    next_actions: List[str] = []
    if rules:
        failures = []
        for key, score_value in scores.items():
            rule = dict(rules.get(key) or {})
            threshold = int(rule.get("min_score") or PASS_SCORE)
            if score_value < threshold:
                failures.append(key)
                action = str(rule.get("fail_action") or "regenerate_offending_slot")
                if action not in next_actions:
                    next_actions.append(action)
        score = None
        passed = bool(raw.get("passed", not failures)) and not failures
    else:
        score = int(round(sum(scores.values()) / len(scores)))
        hard_fail = any(scores[key] < 70 for key in HARD_FAIL_KEYS if key in scores)
        passed = (
            bool(raw.get("passed", score >= PASS_SCORE))
            and score >= PASS_SCORE
            and not hard_fail
        )
    return VisualQaDecision(
        passed=passed,
        score=score,
        dimensions=scores,
        reason_codes=[str(v) for v in raw.get("reason_codes") or []],
        notes=str(raw.get("notes") or ""),
        provider=str(raw.get("provider") or ""),
        model=str(raw.get("model") or ""),
        raw=dict(raw),
        next_actions=next_actions,
    )


def decision_to_dict(decision: VisualQaDecision) -> Dict[str, Any]:
    return {
        "passed": decision.passed,
        "score": decision.score,
        "dimensions": decision.dimensions,
        "reason_codes": decision.reason_codes,
        "notes": decision.notes,
        "provider": decision.provider,
        "model": decision.model,
        "next_actions": decision.next_actions,
    }
