"""Deterministic batch-level diversity planning for OPV workbench tasks.

This module performs no model calls and no visual scoring.  It only arranges
operator-approved assets within the account allow-lists, and freezes the
result into the existing product/plan JSON contract.
"""

from __future__ import annotations

import hashlib
import json
import copy
from collections import Counter
from itertools import product as combinations
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Tuple

from services.asset_compatibility import compatibility_errors, scene_preference_mismatch, uses_pure_color
from services.asset_readiness import local_files
from services.look_selection import look_candidate_refs, supports_dynamic_items, is_dress_recipe, prepare_look_for_policy
from services.styling_normalizer import outfit_fingerprint, outfit_visual_features
from services.multi_look_planner import MULTI_LOOK_RECIPE_ID, compatible_multi_look_candidates, history_look_usage, select_multi_look_sequence, sequence_axes


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = (
    PACKAGE_ROOT / "config" / "batch_diversity" / "OPV_BATCH_DIVERSITY_V1.json"
)


def load_batch_diversity_config(path: Path | None = None) -> Dict[str, Any]:
    return json.loads((path or DEFAULT_CONFIG_PATH).read_text(encoding="utf-8"))


def shot_grammar_profile(grammar_id: str) -> Dict[str, Any]:
    config = load_batch_diversity_config()
    raw = (config.get("shot_grammars") or {}).get(str(grammar_id)) or {}
    return dict(raw)


def infer_visible_silhouette(look: Mapping[str, Any], look_ref: str = "") -> str:
    return outfit_visual_features(look)["silhouette"]


@dataclass(frozen=True)
class BatchDiversityAssignment:
    spec: Any
    product_snapshot: Dict[str, Any]
    metadata: Dict[str, Any]


class BatchDiversityPlanner:
    """Arrange a batch with stable offsets and best-effort asset diversity."""

    def __init__(self, repository, asset_reader, *, config_path: Path | None = None):
        self.repository = repository
        self.asset_reader = asset_reader
        self.config = load_batch_diversity_config(config_path)
        self._accounts = {}
        self._recipes = {}
        self._look_snapshots = {}
        self._prepared_looks = {}
        self._pool_diagnostics = {}

    def _look(self, ref):
        if ref not in self._look_snapshots:
            self._look_snapshots[ref] = self.asset_reader.get_look(ref)
        return self._look_snapshots[ref]

    def _account(self, account_id):
        if account_id not in self._accounts:
            self._accounts[account_id] = copy.deepcopy(self.repository.get_account_profile(account_id))
        account = self._accounts[account_id]
        if account is None:
            raise ValueError(f"账户不存在：{account_id}")
        return account

    @staticmethod
    def _offset(record_id: str, axis: str, size: int) -> int:
        if size <= 1:
            return 0
        digest = hashlib.sha256(f"{record_id}:{axis}".encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % size

    @staticmethod
    def _rotate(values: Sequence[Any], offset: int) -> List[Any]:
        items = list(values)
        return items[offset:] + items[:offset] if items else []

    @staticmethod
    def _compatible_look(snapshot: Mapping[str, Any], product: Mapping[str, Any]) -> bool:
        return not compatibility_errors(snapshot, product)

    def _persona_pool(self, spec: Any) -> List[str]:
        account = self._account(spec.account_id)
        allowed = list((account.operating_rules_json or {}).get("allowed_persona_refs") or [])
        values = allowed or [spec.persona_ref or account.persona_ref_id]
        refs = [str(value) for value in values if str(value).strip()]
        getter = getattr(self.asset_reader, "get_persona", None)
        if getter is not None:
            candidates = []
            minimum = max(1, int((account.operating_rules_json or {}).get(
                "persona_min_local_refs_active" if account.status == "active" else "persona_min_local_refs_testing",
                3 if account.status == "active" else 1,
            )))
            for ref in refs:
                try:
                    snapshot = getter(ref)
                except (KeyError, ValueError, RuntimeError):
                    continue
                if len(local_files(snapshot.get("local_reference_images"))) < minimum:
                    continue
                if account.status == "active" and snapshot.get("status") != "enabled":
                    continue
                markets = {str(m).upper() for m in snapshot.get("markets") or []}
                if markets and str(spec.market).upper() not in markets:
                    continue
                candidates.append(ref)
            refs = candidates
        if not refs:
            raise ValueError("人物池没有满足当前账户参考图门槛的素材")
        return refs

    def _look_pool(self, spec: Any, product: Mapping[str, Any]) -> List[Tuple[str, str]]:
        account = self._account(spec.account_id)
        rules = account.operating_rules_json or {}
        explicit = str(product.get("planned_look_ref") or product.get("explicit_look_ref") or "")
        refs = look_candidate_refs(account, self.asset_reader, explicit_ref=explicit)
        # Legacy preset refs remain a preference; frozen/operator refs are exact.
        if spec.look_ref and not explicit and rules.get("look_selection_mode", "allowlist") == "allowlist":
            refs = [spec.look_ref] + [value for value in refs if value != spec.look_ref]
        excluded = set(rules.get("exclude_look_refs") or [])
        refs = [ref for ref in refs if ref not in excluded]
        diagnostic = {"selection_mode": rules.get("look_selection_mode", "allowlist"),
                      "candidate_count": len(refs), "account_allowlist_count": len(account.allowed_look_refs_json or []),
                      "excluded_look_refs": sorted(excluded), "rejections": {}, "explicit_look_ref": explicit}
        output: List[Tuple[str, str]] = []
        for ref in refs:
            try:
                effective_account = account
                if spec.recipe_id == MULTI_LOOK_RECIPE_ID:
                    effective_account = copy.copy(account)
                    effective_account.operating_rules_json = {**rules, "presentation_profile": {"background_mode": "solid_color"}}
                snapshot = prepare_look_for_policy(effective_account, self._look(ref))
                self._prepared_looks[(spec.account_id, ref)] = snapshot
            except (KeyError, ValueError, RuntimeError):
                diagnostic["rejections"][ref] = ["unreadable"]
                continue
            look_recipe = snapshot.get("recipe") or {}
            required = ("top_inner", "bottom", "footwear")
            if (supports_dynamic_items(rules) or spec.recipe_id == MULTI_LOOK_RECIPE_ID) and is_dress_recipe(look_recipe):
                required = ("footwear",) if look_recipe.get("onepiece") or look_recipe.get("dress") else ("top_inner", "footwear")
            if (account.status == "active" and snapshot.get("status") != "enabled") or (
                (account.status == "active" or rules.get("look_selection_mode") == "auto_library" or spec.recipe_id == MULTI_LOOK_RECIPE_ID)
                and any(not look_recipe.get(k) for k in required)
            ):
                diagnostic["rejections"][ref] = ["active_requires_enabled_complete_recipe"]
                continue
            errors = compatibility_errors(snapshot, product)
            if (spec.recipe_id == "RECIPE_OUTFIT_BREAKDOWN_V1"
                    and is_dress_recipe(snapshot.get("recipe")) and not supports_dynamic_items(rules)):
                errors.append("legacy_three_item_board")
            if errors:
                diagnostic["rejections"][ref] = errors
                continue
            output.append((str(ref), infer_visible_silhouette(snapshot, str(ref))))
        diagnostic["compatible_look_refs"] = [ref for ref, _ in output]
        diagnostic["product_recipe_compatible_count"] = len(output)
        self._pool_diagnostics = diagnostic
        if not output:
            raise ValueError("没有可读取且匹配商品的穿搭模板，不能随机补造")
        # Prefer a different visible silhouette before another template with the
        # same silhouette, while retaining deterministic account order.
        output.sort(key=lambda item: (item[1], refs.index(item[0]) if item[0] in refs else 999))
        return output

    def _scene_pool(self, spec: Any) -> List[Tuple[str, str]]:
        account = self._account(spec.account_id)
        if spec.scene_ref:
            # An explicit preset scene is part of the theme's semantic contract
            # (for example cafe-date must stay in the cafe).  Diversity may only
            # move between zones inside that scene.
            refs = [spec.scene_ref]
        else:
            refs = list(
                account.allowed_scene_refs_json or account.core_scene_refs_json or []
            )
        zones = self.config.get("scene_zones") or {}
        output: List[Tuple[str, str]] = []
        for ref in refs or [spec.scene_ref]:
            if not ref:
                continue
            getter = getattr(self.asset_reader, "get_scene", None)
            if getter is not None:
                try:
                    snapshot = getter(ref)
                except (KeyError, ValueError, RuntimeError):
                    continue
                if account.status == "active" and snapshot.get("status") != "enabled":
                    continue
            configured = list(zones.get(ref) or ["主区域"])
            output.extend((str(ref), str(zone)) for zone in configured)
        if not output:
            raise ValueError("没有可读取的允许场景")
        return output

    def _hook_pool(self, spec: Any) -> List[str]:
        if spec.recipe_id not in self._recipes:
            self._recipes[spec.recipe_id] = self.repository.get_content_recipe(spec.recipe_id)
        recipe = self._recipes[spec.recipe_id]
        hooks = list(getattr(recipe, "hook_types_json", None) or [])
        if spec.hook_strategy and spec.hook_strategy in hooks:
            hooks = [spec.hook_strategy] + [value for value in hooks if value != spec.hook_strategy]
        return hooks or [str(spec.hook_strategy or "")]

    def plan(
        self,
        *,
        record_id: str,
        specs: Sequence[Any],
        products: Sequence[Mapping[str, Any]],
    ) -> List[BatchDiversityAssignment]:
        if len(specs) != len(products):
            raise ValueError("spec/product batch length mismatch")
        self._accounts.clear()
        self._recipes.clear()
        self._look_snapshots.clear()
        self._prepared_looks.clear()
        grammar_ids = list((self.config.get("shot_grammars") or {}).keys()) or ["G1"]
        seen = set()
        usage = Counter()
        history_by_account = {}
        history_getter = getattr(self.repository, "list_recent_diversity_axes", None)
        for spec in specs:
            if spec.account_id not in history_by_account:
                history_by_account[spec.account_id] = list(history_getter(
                    spec.account_id, exclude_source_record_id=record_id, limit=100
                ) or []) if history_getter is not None else []
        scene_occurrences: Dict[str, int] = {}
        assignments: List[BatchDiversityAssignment] = []
        for index, (base_spec, raw_product) in enumerate(zip(specs, products), start=1):
            product = dict(raw_product)
            persona_pool = self._persona_pool(base_spec)
            look_pool = self._look_pool(base_spec, product)
            pool_diagnostics = copy.deepcopy(self._pool_diagnostics)
            pure_color = uses_pure_color(self._account(base_spec.account_id).operating_rules_json) or base_spec.recipe_id == MULTI_LOOK_RECIPE_ID
            scene_pool = self._scene_pool(base_spec)
            hook_pool = self._hook_pool(base_spec)
            personas = self._rotate(
                persona_pool, self._offset(record_id, "persona", len(persona_pool))
            )
            looks = self._rotate(
                look_pool, self._offset(record_id, "look", len(look_pool))
            )
            scenes = self._rotate(
                scene_pool, self._offset(record_id, "scene", len(scene_pool))
            )
            hooks = self._rotate(
                hook_pool, self._offset(record_id, "hook", len(hook_pool))
            )
            grammars = self._rotate(
                grammar_ids, self._offset(record_id, "grammar", len(grammar_ids))
            )
            explicit_scene = str(base_spec.scene_ref or "")
            if explicit_scene:
                scene_sequence_index = scene_occurrences.get(explicit_scene, 0)
                scene_occurrences[explicit_scene] = scene_sequence_index + 1
            else:
                scene_sequence_index = index - 1
            chosen = None
            fallback_used = False
            look_snapshots = {ref: self._prepared_looks[(base_spec.account_id, ref)] for ref, _ in looks}
            look_fingerprints = {ref: outfit_fingerprint(snapshot) for ref, snapshot in look_snapshots.items()}
            history = [dict(row) for row in history_by_account[base_spec.account_id]]
            for row in history:
                row.setdefault("silhouette_key", row.get("visible_silhouette", ""))
                # Older histories may expose only a ref. New RDS histories derive
                # features from frozen snapshots, not today's mutable library.
                if not row.get("outfit_fingerprint") and row.get("look_ref") in look_snapshots:
                    row["outfit_fingerprint"] = look_fingerprints[row["look_ref"]]
            history_usage = Counter((k, str(v)) for row in history for k, v in row.items())
            for row in history:
                for axis in ("look_ref", "outfit_fingerprint", "silhouette_key"):
                    if row.get("alternate_" + axis) and row.get("alternate_" + axis) != row.get(axis):
                        history_usage[(axis, str(row["alternate_" + axis]))] += 1
            # New sequence histories account for every visible page; replace
            # only these axes to avoid double-counting the primary look.
            sequence_history = history_look_usage(history)
            for axis in ("look_ref", "outfit_fingerprint", "silhouette_key"):
                for counter_key in list(history_usage):
                    if counter_key[0] == axis:
                        del history_usage[counter_key]
            history_usage.update(sequence_history)
            candidates = []
            # Enumerate compatible combinations, then balance actual axes.
            # A frozen batch manifest persists this choice before side effects.
            for rank, (persona, (look_ref, silhouette), (scene_ref, scene_zone), grammar_id, hook) in enumerate(
                combinations(personas, looks, scenes, grammars, hooks)
            ):
                if compatibility_errors(look_snapshots[look_ref], product, scene_ref, pure_color=pure_color):
                    continue
                axes = {
                    "reference_pack_id": str(product.get("reference_pack_id") or ""),
                    "reference_pack_version": int(product.get("reference_pack_version") or 0),
                    "persona_ref": persona,
                    "look_ref": look_ref,
                    "silhouette_key": silhouette,
                    "visible_silhouette": silhouette,
                    "outfit_fingerprint": look_fingerprints[look_ref],
                    "scene_ref": scene_ref,
                    "scene_zone": scene_zone,
                    "shot_grammar": grammar_id,
                    "recipe_id": str(base_spec.recipe_id),
                    "hook_strategy": hook,
                }
                key = json.dumps(axes, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                balance_axes = ("persona_ref", "look_ref", "scene_ref", "scene_zone", "shot_grammar", "hook_strategy")
                historical_repeat = sum(all(str(row.get(k, "")) == str(axes.get(k, "")) for k in balance_axes)
                                        for row in history)
                score = (
                    usage[(base_spec.account_id, "outfit_fingerprint", axes["outfit_fingerprint"])],
                    usage[(base_spec.account_id, "silhouette_key", silhouette)],
                    history_usage[("outfit_fingerprint", axes["outfit_fingerprint"])],
                    history_usage[("silhouette_key", silhouette)],
                    key in seen,
                    historical_repeat,
                    sum(usage[(base_spec.account_id, k, str(axes[k]))] for k in balance_axes),
                    sum(history_usage[(k, str(axes[k]))] for k in balance_axes),
                    int(scene_preference_mismatch(look_snapshots[look_ref], scene_ref)) if pure_color else 0,
                    rank,
                )
                candidates.append((score, key, (axes, persona, look_ref, scene_ref, grammar_id, hook)))
            if not candidates:
                raise ValueError("允许池中没有商品、穿搭和场景兼容的组合，请调整素材或预设")
            score, key, chosen = min(candidates, key=lambda item: item[0])
            fallback_used = bool(score[4])
            axes, persona, look_ref, scene_ref, grammar_id, hook = chosen
            for k, value in axes.items():
                usage[(base_spec.account_id, k, str(value))] += 1
            alternate = None
            if base_spec.recipe_id == "RECIPE_VISUAL_TRANSFORM_V1":
                alternate_pool = looks
                if product.get("planned_look_ref") or product.get("explicit_look_ref"):
                    # An explicit FINAL selection does not forbid choosing the
                    # other look from the same account's compatible library.
                    alternate_product = {k: v for k, v in product.items() if k not in {"planned_look_ref", "explicit_look_ref"}}
                    alternate_pool = self._look_pool(base_spec, alternate_product)
                    look_snapshots.update({ref: self._prepared_looks[(base_spec.account_id, ref)] for ref, _ in alternate_pool})
                # Count the other visible look as real usage as well. Merely
                # changing a bag is not a second major outfit.
                final_major = outfit_fingerprint(look_snapshots[look_ref], include_accessories=False)
                alternates = [ref for ref, _ in alternate_pool if
                              outfit_fingerprint(look_snapshots[ref], include_accessories=False) != final_major
                              and not compatibility_errors(look_snapshots[ref], product, scene_ref, pure_color=pure_color)]
                if alternates:
                    def alternate_score(ref):
                        fp = outfit_fingerprint(look_snapshots[ref])
                        silhouette = infer_visible_silhouette(look_snapshots[ref], ref)
                        return (usage[(base_spec.account_id, "outfit_fingerprint", fp)],
                                usage[(base_spec.account_id, "silhouette_key", silhouette)],
                                history_usage[("outfit_fingerprint", fp)], refs_order[ref])
                    refs_order = {ref: i for i, (ref, _) in enumerate(alternate_pool)}
                    alternate = min(alternates, key=alternate_score)
                    for axis, value in (("look_ref", alternate),
                                        ("outfit_fingerprint", outfit_fingerprint(look_snapshots[alternate])),
                                        ("silhouette_key", infer_visible_silhouette(look_snapshots[alternate], alternate))):
                        axes["alternate_" + axis] = value
                        usage[(base_spec.account_id, axis, value)] += 1
                product["planned_alternate_look_ref"] = alternate or look_ref
                product["planned_alternate_look_snapshot"] = copy.deepcopy(look_snapshots[alternate or look_ref])
                product["alternate_selection"] = {"same_look": not bool(alternate),
                                                   "reason": "least_used_distinct_major_outfit" if alternate else "no_distinct_compatible_look"}
            multi_selection = {}
            if base_spec.recipe_id == MULTI_LOOK_RECIPE_ID:
                sequence_candidates, sequence_rejections = compatible_multi_look_candidates(
                    self._account(base_spec.account_id), self.asset_reader, product, scene_ref)
                current_usage = Counter({(axis, value): count for (account_id, axis, value), count in usage.items()
                                         if account_id == base_spec.account_id})
                sequence, multi_selection = select_multi_look_sequence(
                    sequence_candidates, first_ref=look_ref, usage=current_usage, history=history_usage)
                product["planned_look_sequence"] = sequence
                product["multi_look_selection"] = {**multi_selection, "rejections": sequence_rejections}
                axes["look_sequence"] = sequence_axes(sequence)
                axes["actual_look_count"] = len(sequence)
                for entry in sequence[1:]:
                    for axis in ("look_ref", "outfit_fingerprint", "silhouette_key"):
                        usage[(base_spec.account_id, axis, str(entry[axis]))] += 1
            eligible_refs = {value[2][2] for value in candidates}
            pool_diagnostics.update({"scene_compatible_count": len(eligible_refs),
                                     "scene_compatible_look_refs": sorted(eligible_refs),
                                     "distinct_outfit_count": len({outfit_fingerprint(look_snapshots[ref]) for ref in eligible_refs}),
                                     "distinct_silhouette_count": len({infer_visible_silhouette(look_snapshots[ref]) for ref in eligible_refs}),
                                     "scene_policy": "soft_preference" if pure_color else "hard_compatibility"})
            grammar = (self.config.get("shot_grammars") or {}).get(grammar_id) or {}
            suffixes = list(grammar.get("title_suffixes_th") or [])
            suffix = (
                suffixes[((index - 1) // max(len(grammar_ids), 1)) % len(suffixes)]
                if suffixes else ""
            )
            signature = hashlib.sha256(json.dumps(axes, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
            metadata = {
                "profile_id": str(self.config.get("profile_id") or ""),
                "sequence_index": index,
                "history_records_considered": len(history),
                "historical_repeat_count": score[5],
                "rotation_policy": "outfit_first_least_used_v3",
                "combination_key": signature,
                "axes": axes,
                "fallback_used": fallback_used,
                "pool_diagnostics": pool_diagnostics,
                "diversity_degraded": bool(score[0] or fallback_used or (product.get("alternate_selection") or {}).get("same_look") or multi_selection.get("degradation_reasons")),
                "degradation_reasons": (["compatible_outfits_reused_after_pool_exhaustion"] if score[0] else [])
                                       + (["no_distinct_alternate_look"] if (product.get("alternate_selection") or {}).get("same_look") else []),
            }
            if multi_selection:
                metadata["multi_look_selection"] = multi_selection
                metadata["degradation_reasons"].extend(multi_selection.get("degradation_reasons") or [])
            seen.add(key)
            product.update({
                "planned_persona_ref": persona,
                "planned_look_ref": look_ref,
                "planned_scene_ref": scene_ref,
                "planned_scene_zone": axes["scene_zone"],
                "planned_shot_grammar": grammar_id,
                "planned_visible_silhouette": axes["silhouette_key"],
                "planned_title_suffix": suffix,
                "batch_diversity": metadata,
            })
            assignments.append(BatchDiversityAssignment(
                spec=replace(
                    base_spec,
                    persona_ref=persona,
                    look_ref=look_ref,
                    scene_ref=scene_ref,
                    hook_strategy=hook,
                ),
                product_snapshot=product,
                metadata=metadata,
            ))
        return assignments
