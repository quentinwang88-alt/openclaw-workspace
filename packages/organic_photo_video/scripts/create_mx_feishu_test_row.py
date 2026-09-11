"""Phase-2 acceptance: create exactly ONE labeled MX wig test row in Feishu.

One-off, operator-visible production write, authorized for the Phase-2
run-through.  Fields are the plain operator surface (no JSON columns).
Idempotent-ish: refuses to create a second row when an open test row with the
same marker already exists.
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

MARKER = "MX-V2-Phase2验收"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wiki-token", default="TR10wxEXHiCYIhk8clActVdenpc")
    parser.add_argument("--table-id", default="tblj3x846gU3rshB")
    parser.add_argument("--preset", default="图文｜MX｜四选一发型")
    parser.add_argument("--quantity", type=int, default=1)
    parser.add_argument("--store", default="MXJF01")
    parser.add_argument("--execute", action="store_true", default=True)
    args = parser.parse_args()

    client = FeishuBitableClient(
        resolve_wiki_bitable_app_token(args.wiki_token), args.table_id)
    for record in client.list_records(page_size=500):
        fields = record.fields or {}
        note = str(fields.get("备注") or "")
        progress = str(fields.get("进度") or "")
        if MARKER in note and progress not in {"已完成", "发布失败", ""}:
            print(f"existing open test row: {record.record_id} progress={progress}")
            return 0
    fields = {
        "生产预设": args.preset,
        "生成篇数": args.quantity,
        "店铺": args.store,
        "执行": bool(args.execute),
        "备注": f"{MARKER}：离线验收已通过的首篇跑通行；真实发布仍需人工确认。",
    }
    response = client._request(
        "POST",
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{client.app_token}"
        f"/tables/{client.table_id}/records",
        headers=client._headers(),
        json={"fields": fields},
    )
    result = response.json()
    if result.get("code") != 0:
        print(f"FAIL: {result.get('msg')}")
        return 2
    record_id = (result.get("data") or {}).get("record", {}).get("record_id")
    print(f"created test row: {record_id} preset={args.preset} "
          f"quantity={args.quantity} store={args.store} execute=True")
    return 0


if __name__ == "__main__":
    sys.exit(main())
