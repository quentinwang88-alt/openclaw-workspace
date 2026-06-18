"""钩子素材补充 skill（改动3B）。

职责：预检库里有没有钩子型首镜素材 → 缺口则接现有 AI 补素材闭环
（AISupplementWorkbenchSkill.sync_for_product → sync_workbench → 飞书工作台
→ segment-package-worker 提交即梦 → result-uploader 抓取回流 →
process_prompt_package_returns 导入+后处理）。

不重复造生成/提交/抓取链路，只做"钩子型缺口判定"这一层精确化，
然后把缺口翻译成现有闭环认识的 gap_text 格式（"AI补素材: hero首镜N"）。

种草线和投流线都受益：补回的 AI 钩子素材进公共池，无归属。
不进 orchestrator.run_product 主流水线，由补钩子入口显式调用。
"""
from __future__ import annotations

from typing import Any

from auto_mixcut.core.result import Result
from auto_mixcut.skills.ai_supplement_workbench_skill import AISupplementWorkbenchSkill
from auto_mixcut.skills.context import SkillContext
from auto_mixcut.skills.render_plan_skill import precheck_hook_coverage


class HookSupplementSkill:
    """补钩子素材编排 skill（改动3B）。

    与现有 AISupplementWorkbenchSkill 的区别：
    - 现有逻辑看"首镜容量够不够"（hero 片段数×2），不看片段有没有视觉钩子。
    - 本 skill 用 precheck_hook_coverage 精确判定"有没有钩子型首镜素材"
     （hook_strength ∈ {strong,medium} 且 hook_visual_type ≠ none）。
    - 发现缺口后，翻译成 "AI补素材: hero首镜N" 格式，交给现有闭环全链路处理。
    """

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def run(self, product_id: str, required_count: int = 1) -> Result:
        """预检钩子覆盖 → 缺口则接现有 AI 补素材闭环。

        返回：
          - ok=True, skipped="hook_sufficient"：库里有足够钩子素材，无需补。
          - ok=True, delegated=True：已把缺口交给 AISupplementWorkbenchSkill，
            后续提交/抓取/回流由现有心跳和 worker 自动完成。
          - ok=False：预检或委派失败。
        """
        # 1. 预检钩子覆盖
        check = precheck_hook_coverage(self.ctx, product_id, required_count=required_count)
        if check["ok"]:
            return Result.ok({
                "product_id": product_id,
                "skipped": "hook_sufficient",
                "current_candidates": check.get("current_candidates", 0),
            })

        # 2. 翻译缺口为现有闭环认识的 gap_text
        gap = check["gap"]
        shortfall = int(gap.get("shortfall") or 1)
        # 现有 _gap_slots 识别 "AI补素材: hero首镜N" → 产出 hero/product_display/product_clarity slot
        gap_text = f"AI补素材: hero首镜{min(max(shortfall, 1), 6)}"

        # 3. 交给现有 AI 补素材闭环（sync_for_product → sync_workbench → 飞书工作台）
        #    sync_for_product 会把 gap_text 写入 content_tasks.blocked_reason，
        #    同步锚点卡+任务卡到飞书，调 sync_workbench 生成提示词包并写飞书工作台。
        #    后续提交即梦/抓取回流由 run_ai_supplement_heartbeat 或 worker 自动完成。
        supplement = AISupplementWorkbenchSkill(self.ctx).sync_for_product(
            product_id, gap_text=gap_text,
        )
        if not supplement.success:
            return supplement

        return Result.ok({
            "product_id": product_id,
            "delegated": True,
            "gap_text": gap_text,
            "precheck_gap": gap,
            "supplement_result": supplement.data,
        })
