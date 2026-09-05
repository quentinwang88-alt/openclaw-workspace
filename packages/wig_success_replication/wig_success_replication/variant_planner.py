"""Deterministic cumulative V1.2 replication planning."""

from __future__ import annotations

from collections.abc import Iterable

from .models import VariantPlanItem
from .publishing import resolve_publish_settings


PLANNER_VERSION = "v1.3-mechanism-first"


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
            ("hook", "proof_selection", "proof_order", "voiceover"),
            ("更换首镜冲突表达", "重选证明动作组合", "重排证明动作顺序", "改写口播措辞"),
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
            ("reveal", "timing", "proof_order", "cta"),
            ("更换产品揭晓方式", "改变镜头节奏与数量", "重排证明动作顺序", "重写CTA表达"),
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
        publish_purpose: str = "带货",
    ) -> list[VariantPlanItem]:
        target = self.target_count(relationship, count)
        existing = {int(value) for value in existing_sequences if int(value) > 0}
        purpose, _ = resolve_publish_settings(publish_purpose)
        return [
            self._nurture_item(sequence_no, relationship) if purpose == "养号"
            else self._item(sequence_no, relationship)
            for sequence_no in range(1, target + 1)
            if sequence_no not in existing
        ]

    def _nurture_item(self, sequence_no: int, relationship: str) -> VariantPlanItem:
        item = self._item(sequence_no, relationship)
        if sequence_no == 1:
            dimensions = ["baseline"]
            mutations = ["保留母版开场、关键动作、视觉变化或内容回报"]
        elif sequence_no == 2:
            dimensions = ["visual_shell", "timing"]
            mutations = ["人物场景外壳轻变", "动作停顿与节奏轻变"]
        elif sequence_no == 3:
            dimensions = ["hook", "reveal", "ending"]
            mutations = ["开场动作轻变", "视觉变化呈现轻变", "结尾构图轻变"]
        else:
            routes = (
                (["hook", "visual_shell", "ending"], ["重构开场", "更换场景外壳", "重构结尾"]),
                (["reveal", "timing", "visual_shell"], ["重构变化揭晓", "重排节奏", "更换场景外壳"]),
                (["hook", "timing", "ending"], ["重构关键动作开场", "改变动作节奏", "更换回报构图"]),
                (["visual_shell", "reveal", "ending"], ["重构场景", "改变变化呈现", "重构非销售结尾"]),
                (["hook", "reveal", "timing"], ["更换开场动作", "改变视觉揭晓", "重构镜头节奏"]),
            )
            dimensions, mutations = routes[(sequence_no - 4) % len(routes)]
        return item.model_copy(update={
            "change_dimensions": dimensions,
            "core_mutations": mutations,
            "instruction": (
                "养号用途：继承母版自身的开场、关键动作、视觉变化或内容回报，不套用带货五锚点；"
                "不强制销售CTA、商品证明或口播，原母版无口播时保持无口播。"
                f"本条变化：{'、'.join(mutations)}；"
                + ("高保真保留核心机制与主要结构，可简化基准拍法。" if sequence_no <= 3 else "一般复刻默认结合多个相关维度改变表达，不凑维度数。")
                + "产品外观以人工选定产品图为准，不做产品匹配复核。"
            ),
        })

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
                instruction=f"保留核心机制、叙事顺序与口播主要语义；基准拍法可作等效简化，不硬锁微动作与小数秒；{relationship_rule}",
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
                    "保持母版本身的核心机制和主要回报；优先轻变场景/服装/机位及已有口播措辞，"
                    f"不为凑变化补口播或新证明；{relationship_rule}"
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
                    "保持成功机制与因果关系；优先轻变开场、揭晓拍法及已有口播表达，"
                    f"不以字面改写比例判定质量；{relationship_rule}"
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
                "只保留母版本身的核心吸引机制、因果与内容回报，不强套商业五锚点；"
                f"默认从相关维度调整：{'、'.join(mutations)}，无对应机制时跳过，不为凑数新增内容；{relationship_rule}"
            ),
        )
