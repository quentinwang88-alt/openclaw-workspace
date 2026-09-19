"""GC 闭环验收（方案 §9.2）：创建两条新教程真实任务行。

新任务一：旅行攻略 + 东京 + 指定商品（tocrystal66，主题来自账号默认）。
新任务二：配色教程 + 同商品 + 纯色背景（wn0didnad6，主题行级显式选择）。

一次性行创建：参考图附件取该商品参考包的店铺自有图；执行=false——
由验收流程在核对冻结叙事后另行勾选执行，避免建行即付费。
"""
from __future__ import annotations

import argparse
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

MARKER = "GC-V2闭环验收"

REF_DIR = Path(
    "/Users/likeu3/.openclaw/shared/data/longform_original_video/"
    "product_1737141103233042426_references")
PRODUCT = "1737141103233042426"
REF_FILES = ["ref_01.jpg", "ref_02.jpg"]

ROWS = [
    {
        "生产预设": "图文｜TH｜旅行穿搭",
        "生成篇数": 1,
        "店铺": "THFZ01",
        "目标账号（可选）": "tocrystal66",
        "产品编码": PRODUCT,
        "参考图类型": "风格参考",
        "旅行地点（可选）": "东京",
        "旅行国家": "日本",
        "备注": f"{MARKER}任务一：旅行攻略+东京+指定商品；主题走账号默认，验证冻结叙事后手动执行",
    },
    {
        "生产预设": "图文｜TH｜旅行穿搭",
        "生成篇数": 1,
        "店铺": "THFZ01",
        "目标账号（可选）": "wn0didnad6",
        "图文主题": "配色教程",
        "产品编码": PRODUCT,
        "参考图类型": "风格参考",
        "视觉预设（可选）": "纯色搭配解析",
        "备注": f"{MARKER}任务二：配色教程+同商品+纯色背景；主题走任务显式选择，验证冻结叙事后手动执行",
    },
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wiki-token", default="TR10wxEXHiCYIhk8clActVdenpc")
    parser.add_argument("--table-id", default="tblj3x846gU3rshB")
    parser.add_argument("--apply", action="store_true", help="正式建行（默认 dry-run）")
    args = parser.parse_args()
    client = FeishuBitableClient(
        resolve_wiki_bitable_app_token(args.wiki_token), args.table_id)
    for rec in client.list_records(page_size=500):
        note = str((rec.fields or {}).get("备注") or "")
        if MARKER in note:
            print(f"已有验收行：{rec.record_id}｜{note[:60]}")
            return 0
    for name in REF_FILES:
        path = REF_DIR / name
        if not path.is_file():
            print(f"缺少参考图：{path}")
            return 2
    mode = "APPLY" if args.apply else "DRY-RUN"
    for row in ROWS:
        print(f"[{mode}] {row['备注'][:40]}")
        if not args.apply:
            continue
        attachments = []
        for name in REF_FILES:
            with open(REF_DIR / name, "rb") as handle:
                payload = handle.read()
            attachments.append(client.upload_attachment(
                payload, name, "image/jpeg"))
        response = client._request(
            "POST",
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/{client.app_token}"
            f"/tables/{client.table_id}/records",
            headers=client._headers(),
            json={"fields": {**row, "参考图（可选）": attachments}},
        )
        created = response.json()
        if created.get("code") != 0:
            print(f"  建行失败：{created.get('msg')}")
            return 2
        record_id = ((created.get("data") or {}).get("record") or {}).get("record_id")
        print(f"  → {record_id}")
    if not args.apply:
        print("\n（dry-run 未做任何写操作；正式建行加 --apply）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
