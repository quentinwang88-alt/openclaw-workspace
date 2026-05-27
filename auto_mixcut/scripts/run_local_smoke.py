#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path

from auto_mixcut.agent.orchestrator import AutoMixcutOrchestratorAgent
from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.feishu_review_skill import FeishuReviewSkill
from auto_mixcut.skills.oss_storage_skill import OSSStorageSkill
from auto_mixcut.skills.product_anchor_skill import ProductAnchorSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill


def main() -> int:
    root = Path("/private/tmp/auto_mixcut_local_smoke")
    os.environ.setdefault("AUTO_MIXCUT_ROOT", str(Path(__file__).resolve().parents[1]))
    os.environ.setdefault("AUTO_MIXCUT_DB", str(root / "auto_mixcut.sqlite"))
    os.environ.setdefault("AUTO_MIXCUT_OSS_ROOT", str(root / "oss"))
    os.environ.setdefault("AUTO_MIXCUT_TEMP_ROOT", str(root / "tmp"))
    os.environ.setdefault("AUTO_MIXCUT_MOCK_FFMPEG", "1")
    os.environ.setdefault("AUTO_MIXCUT_FEISHU_ENABLED", "1")

    ctx = build_context()
    result = {"steps": []}

    def step(name: str, res):
        result["steps"].append({"name": name, "result": res.to_dict()})
        if not res.success:
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
            raise SystemExit(1)
        return res

    step("init_db", RDSRepositorySkill(ctx).init_db())
    product_id = "VN_HAIR_SMOKE_001"
    step(
        "create_task",
        RDSRepositorySkill(ctx).create_product_task(
            product_id=product_id,
            product_name="Smoke Test Pearl Bow Hair Clip",
            market="VN",
            category="hair_accessories",
            requested_count=2,
            shop_id="smoke_shop",
            priority="normal",
        ),
    )
    step("draft_anchor", ProductAnchorSkill(ctx).draft_anchor(product_id))
    step("sync_anchor_draft", FeishuReviewSkill(ctx).sync_anchor_queue(product_id))
    step("confirm_anchor", ProductAnchorSkill(ctx).confirm_anchor(product_id, reviewer="auto_smoke"))
    step("sync_anchor_confirmed", FeishuReviewSkill(ctx).sync_anchor_queue(product_id))

    asset_dir = root / "assets"
    asset_dir.mkdir(parents=True, exist_ok=True)
    for index in range(7):
        path = asset_dir / f"asset_{index:02d}.mp4"
        path.write_bytes(f"mock smoke asset {index}".encode("utf-8"))
        step(
            f"upload_asset_{index:02d}",
            OSSStorageSkill(ctx).upload_asset(
                product_id=product_id,
                file_path=str(path),
                source_type="self_shot",
                source_trust_level="high",
                product_binding_type="exact_sku",
            ),
        )

    step("run_product", AutoMixcutOrchestratorAgent(ctx).run_product(product_id, requested_count=2, auto_confirm_anchor=True))
    outputs = ctx.repo.list_where("outputs", "product_id=?", (product_id,))
    sync_records = ctx.repo.list_where("feishu_sync_records", "1=1")
    result["summary"] = {
        "product_id": product_id,
        "db_path": str(ctx.settings.db_path),
        "oss_root": str(ctx.settings.oss_root),
        "outputs": [item["output_id"] for item in outputs],
        "feishu_sync_records": [
            {
                "object_type": item["object_type"],
                "object_id": item["object_id"],
                "feishu_table": item["feishu_table"],
                "feishu_record_id": item["feishu_record_id"],
            }
            for item in sync_records
        ],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
