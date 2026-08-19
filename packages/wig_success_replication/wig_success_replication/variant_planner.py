"""Deterministic cumulative V1.2 replication planning."""

from __future__ import annotations

from collections.abc import Iterable

from .models import VariantPlanItem


PLANNER_VERSION = "v1.2-fidelity-general"


class VariantPlanner:
    """Plan stable sequence slots, not per-batch throwaway variants.

    For every mother version and product, slots 1-3 are high fidelity. Every
    later slot is a general remake that changes at least three creative
    dimensions. Existing sequence numbers are omitted so increasing the target
    count only generates the missing tail.
    """

    SAME_DEFAULT = 12
    CROSS_DEFAULT = 4
    HIGH_FIDELITY_COUNT = 3

    GENERAL_ROUTES = (
        (
            "G1",
            "general_hook_rebuild",
            ("hook", "voiceover", "ending"),
            ("重构首镜冲突表达", "改写口播措辞", "改变结尾构图"),
        ),
        (
            "G2",
            "general_reveal_rebuild",
            ("reveal", "timing", "voiceover"),
            ("更换产品揭晓转场", "重新分配时间节点", "改写口播措辞"),
        ),
        (
            "G3",
            "general_proof_rebuild",
            ("proof_selection", "proof_order", "voiceover"),
            ("重选证明动作组合", "重排证明动作顺序", "改写口播措辞"),
        ),
        (
            "G4",
            "general_cta_rebuild",
            ("visual_shell", "cta", "ending"),
            ("更换叙事场景与人物外壳", "重写CTA表达", "改变结尾构图"),
        ),
        (
            "G5",
            "general_pacing_rebuild",
            ("timing", "proof_order", "cta"),
            ("改变镜头节奏与数量", "重排证明动作顺序", "重写CTA表达"),
        ),
    )

    def target_count(self, relationship: str, count: int | None = None) -> int:
        if relationship not in {"same_product", "cross_product"}:
            raise ValueError(f"unsupported relationship: {relationship}")
        total = count if count is not None else (
            self.SAME_DEFAULT if relationship == "same_product" else self.CROSS_DEFAULT
        )
        if not 1 <= total <= 20:
            raise ValueError("target count must be between 1 and 20")
        return total

    def plan(
        self,
        relationship: str,
        count: int | None = None,
        *,
        existing_sequences: Iterable[int] = (),
    ) -> list[VariantPlanItem]:
        target = self.target_count(relationship, count)
        existing = {int(value) for value in existing_sequences if int(value) > 0}
        return [
            self._item(sequence_no, relationship)
            for sequence_no in range(1, target + 1)
            if sequence_no not in existing
        ]

    def _item(self, sequence_no: int, relationship: str) -> VariantPlanItem:
        relationship_rule = (
            "保持来源产品视觉身份"
            if relationship == "same_product"
            else "用人工选定目标产品图替换商品外观，不做匹配度复核"
        )
        if sequence_no == 1:
            return VariantPlanItem(
                variant_type="high_fidelity_h1",
                sequence_no=sequence_no,
                replication_mode="high_fidelity",
                creative_route="H1",
                variant_key="V01",
                mutation_key="h1_baseline",
                change_dimensions=["baseline"],
                core_mutations=["基准高保真复刻"],
                instruction=f"基准复刻母版结构、原口播与关键动作；{relationship_rule}",
            )
        if sequence_no == 2:
            return VariantPlanItem(
                variant_type="high_fidelity_h2",
                sequence_no=sequence_no,
                replication_mode="high_fidelity",
                creative_route="H2",
                variant_key="V02",
                mutation_key="h2_shell_voiceover",
                change_dimensions=["visual_shell", "voiceover"],
                core_mutations=["人物与场景外壳轻变", "局部口播改写"],
                instruction=(
                    "保持核心顺序、商品利益和证明链；更换场景/服装/机位，并改写15%-25%口播，"
                    f"不得与H1整段逐字相同；{relationship_rule}"
                ),
            )
        if sequence_no == 3:
            return VariantPlanItem(
                variant_type="high_fidelity_h3",
                sequence_no=sequence_no,
                replication_mode="high_fidelity",
                creative_route="H3",
                variant_key="V03",
                mutation_key="h3_hook_reveal_voiceover",
                change_dimensions=["hook", "reveal", "voiceover"],
                core_mutations=["钩子动作轻变", "揭晓方向轻变", "局部口播改写"],
                instruction=(
                    "保持成功机制和主要证明链；改变首镜问题展示、揭晓运动方向及部分口播，"
                    f"不得与H1/H2整段逐字相同；{relationship_rule}"
                ),
            )

        route, variant_type, dimensions, mutations = self.GENERAL_ROUTES[(sequence_no - 4) % len(self.GENERAL_ROUTES)]
        return VariantPlanItem(
            variant_type=variant_type,
            sequence_no=sequence_no,
            replication_mode="general",
            creative_route=route,
            variant_key=f"V{sequence_no:02d}",
            mutation_key=f"{route.lower()}_{sequence_no:02d}",
            change_dimensions=list(dimensions),
            core_mutations=list(mutations),
            instruction=(
                "只锁定冲突、前段产品揭晓、强反差、至少两个证明动作和CTA五个成功机制；"
                f"本条必须真实改变{len(dimensions)}个维度：{'、'.join(mutations)}；{relationship_rule}"
            ),
        )
