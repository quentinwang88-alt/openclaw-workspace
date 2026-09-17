#!/usr/bin/env python3
"""自动图文供稿执行脚本（Phase 2）。

用法（包根目录）：
  python3 scripts/run_auto_photo_supply.py            # dry-run：只读盘点，不建行不调模型
  python3 scripts/run_auto_photo_supply.py --apply    # 正式建行（写飞书任务表）

正式建行 = 在飞书任务表创建「执行=是」的行，由现有扫描器消费（与运营手动建行
同构）。账号需先在账号管理表配置 图文自动化模式/自动供稿预设（sync_accounts
同步后生效）。--apply 需要 OPV_PHOTO_VISION_* 环境变量（选材调用）。
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
BITABLE_SKILL = WORKSPACE_ROOT / "skills" / "script-run-manager-sync"
for value in (str(WORKSPACE_ROOT), str(BITABLE_SKILL), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402

from services.auto_photo_supply import AutoPhotoSupply  # noqa: E402
from services.material_analysis import MaterialLedger  # noqa: E402
from services.material_source import MaterialSource  # noqa: E402
from services.publish_account_profile import (  # noqa: E402
    SUPPLY_AUTOMATION_OFF, build_default_resolver)

DEFAULT_WIKI_TOKEN = "TR10wxEXHiCYIhk8clActVdenpc"
DEFAULT_TABLE_ID = "tblj3x846gU3rshB"


def build_vision_client():
    from services.photo_reference_vision import _DoubaoVisionClient

    api_url = os.environ.get("OPV_PHOTO_VISION_API_URL", "").strip()
    api_key = os.environ.get("OPV_PHOTO_VISION_API_KEY", "").strip()
    model = os.environ.get("OPV_PHOTO_VISION_MODEL", "").strip()
    missing = [n for n, v in (
        ("OPV_PHOTO_VISION_API_URL", api_url),
        ("OPV_PHOTO_VISION_API_KEY", api_key),
        ("OPV_PHOTO_VISION_MODEL", model),
    ) if not v]
    if missing:
        sys.exit(f"--apply 需要环境变量：{', '.join(missing)}")
    return _DoubaoVisionClient(api_url=api_url, api_key=api_key, model=model), model


def build_product_snapshot_resolver():
    """指定商品模式：复用现有商品解析器（RDS 商品参考包）读真实品类/款色。

    解析失败由供给层明确暂停该任务（不降级为自由搭配）；RDS 环境缺失时
    返回 None（指定商品的账号本轮暂停并记缺口）。
    """
    try:
        from repositories.rds_repository import RdsRepository
        from services.product_reference_resolver import ProductReferenceResolver
    except Exception:  # noqa: BLE001 - 环境缺失＝显式不可用，不假装成功
        return None
    try:
        repository = RdsRepository.from_env()
    except Exception:  # noqa: BLE001
        return None
    resolver = ProductReferenceResolver(repository)

    def _resolve(code: str):
        snapshot = resolver.resolve_snapshot(code)
        return snapshot

    return _resolve


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="正式建行（默认 dry-run）")
    parser.add_argument("--wiki-token", default=DEFAULT_WIKI_TOKEN)
    parser.add_argument("--table-id", default=DEFAULT_TABLE_ID)
    parser.add_argument("--ledger", default="")
    args = parser.parse_args()

    resolver = build_default_resolver()
    accounts = []
    for handle in resolver.options():
        binding = resolver.resolve(handle)
        if binding.supply_policy.get("automation") != SUPPLY_AUTOMATION_OFF:
            accounts.append(binding)
    if not accounts:
        print("没有开启自动供稿的账号（账号管理表配置 图文自动化模式 后 sync-accounts 生效）")
        return 0

    model = os.environ.get("OPV_PHOTO_VISION_MODEL", "").strip()
    vision_client = None
    if args.apply:
        vision_client, model = build_vision_client()

    client = FeishuBitableClient(
        resolve_wiki_bitable_app_token(args.wiki_token), args.table_id)
    from services.material_analysis import MaterialAnalyzer
    analyzer = None
    if vision_client is not None:
        # 方案 §6.4：0 候选时的有限补分析走正式入口（复用 Doubao 与额度）
        analyzer = MaterialAnalyzer(
            MaterialSource(), MaterialLedger(args.ledger or None),
            vision_client, model=model)
    supply = AutoPhotoSupply(
        client=client,
        source=MaterialSource(),
        ledger=MaterialLedger(args.ledger or None),
        vision_client=vision_client,
        analyzer=analyzer,
        model=model,
        product_snapshot_resolver=build_product_snapshot_resolver(),
    )
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"== 自动供稿 {mode}｜开启账号 {len(accounts)} 个 ==")
    results = supply.run(accounts, apply=args.apply)
    for result in results:
        line = f"[{result.status}] {result.account_name or result.account_id}"
        if result.detail:
            line += f"｜{result.detail}"
        print(line)
        for slot in result.slots:
            print(f"    slot{slot.slot} → {slot.status}｜{slot.detail}"
                  f"｜主参考 {slot.main_note_id or '—'}")
    if not args.apply:
        print("\n（dry-run 未做任何写操作；正式执行加 --apply）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
