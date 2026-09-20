"""复审验收：用真实冻结计划走真实 exporter 重排既有成片（零生图）。

区别于 relayout_guide_samples（手填文案直调 renderer——只能验证渲染能力）：
本脚本读取冻结批次请求的 layout_snapshot（模板）与 revision 计划的
slides（page_text/color_chips/overlay_text），通过 NativePhotoProductionFlow
的 exporter 路径重排，使最终页 QA、manifest 指纹走真实代码。

用途：温度样片（recvvJiswTWVUO，修复 F3 前冻结、无 pages→解释区回装）
与配色样片（recvvK1eGxPvNm，pages 完整）的版式复核。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
for value in (str(WORKSPACE_ROOT), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from repositories.rds_repository import RdsRepository  # noqa: E402
from services.photo_package import (  # noqa: E402
    NativePhotoProductionFlow, PhotoPackageError,
)


def reexport(record_id: str) -> dict:
    repository = RdsRepository.from_env()
    batch = repository.get_production_batch(record_id)
    if batch is None:
        raise SystemExit(f"{record_id}: 无冻结批次")
    entry = ((batch.manifest_json or {}).get("entries") or [{}])[0]
    request = entry.get("request") or {}
    template = request.get("layout_snapshot")
    tasks = repository.list_tasks_by_source_prefix(
        "feishu_opv", record_id + ":")
    if not tasks:
        raise SystemExit(f"{record_id}: 无已建任务")
    task = tasks[0]
    output_root = Path.home() / ".openclaw/shared/data/organic_photo_video/photo_packages"
    flow = NativePhotoProductionFlow(
        repository, None, output_root=output_root)
    # 跳过生图（任务已 image_review），只走 exporter + 最终页 QA
    task_row = repository.get_task(task.task_id)
    manifest = flow.exporter.export(
        task.task_id, template=template)
    pages = []
    for slide in manifest.get("slides") or []:
        pages.append({"index": slide.get("index"), "path": slide.get("path")})
    return {
        "record_id": record_id, "task_id": task.task_id,
        "pages": pages,
        "qa": manifest.get("travel_final_page_qa") or {},
    }


if __name__ == "__main__":
    targets = sys.argv[1:] or ["recvvK1eGxPvNm", "recvvJiswTWVUO"]
    for record_id in targets:
        try:
            report = reexport(record_id)
            print(json.dumps(report, ensure_ascii=False, indent=1))
        except PhotoPackageError as exc:
            print(f"{record_id}: {exc}")
