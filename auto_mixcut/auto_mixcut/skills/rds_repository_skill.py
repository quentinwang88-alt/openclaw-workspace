from __future__ import annotations

from pathlib import Path

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext


class RDSRepositorySkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def init_db(self) -> Result:
        return self.ctx.repo.migrate(self.ctx.settings.root_dir / "migrations" / "001_sqlite_init.sql")

    def create_product_task(
        self,
        product_id: str,
        product_name: str,
        market: str,
        category: str,
        requested_count: int,
        shop_id: str = "",
        priority: str = "normal",
    ) -> Result:
        product = self.ctx.repo.upsert(
            "products",
            "product_id",
            {
                "product_id": product_id,
                "product_name": product_name,
                "market": market,
                "category": category,
                "shop_id": shop_id,
                "priority": priority,
                "anchor_status": "pending",
                "product_status": "active",
            },
        )
        if not product.success:
            return product
        task_id = new_id("TASK")
        task = self.ctx.repo.upsert(
            "content_tasks",
            "task_id",
            {
                "task_id": task_id,
                "product_id": product_id,
                "task_type": "mixcut",
                "requested_variant_count": requested_count,
                "task_status": "CREATED",
                "anchor_status_at_task_start": "pending",
                "created_by": "cli",
            },
        )
        return task if not task.success else Result.ok({"task_id": task_id, "product_id": product_id})
