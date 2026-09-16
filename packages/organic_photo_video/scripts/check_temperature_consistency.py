#!/usr/bin/env python3
"""封面×温度一致性抽查（2026-09-16 温度修复的第 3 层）。

对已完成行：从「完整文案（中文）」解析声明的温度带，取封面成片让
Doubao 判定画面主单品的适用温度区间，重叠即 pass。运营/开发可对任意
行抽查；接入自动 QA 门禁需要独立的冻结批次设计，另行立项。

用法（包根目录）：
  python3 scripts/check_temperature_consistency.py --record-id <rid> [--record-id ...]
"""
from __future__ import annotations

import argparse
import os
import sys
import urllib.request
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
BITABLE_SKILL = WORKSPACE_ROOT / "skills" / "script-run-manager-sync"
for value in (str(WORKSPACE_ROOT), str(BITABLE_SKILL), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from core.bitable import (  # noqa: E402
    FeishuBitableClient, get_tenant_access_token, resolve_wiki_bitable_app_token)

from services.material_adapter import (  # noqa: E402
    parse_thermal_band, thermal_overlap, thermal_window)

WIKI_TOKEN = "TR10wxEXHiCYIhk8clActVdenpc"
TABLE_ID = "tblj3x846gU3rshB"


def text_value(value):
    if isinstance(value, list):
        return "".join(
            item.get("text", "") if isinstance(item, dict) else str(item)
            for item in value)
    return str(value or "")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--record-id", action="append", required=True)
    args = ap.parse_args()

    from services.photo_reference_vision import _DoubaoVisionClient
    client = _DoubaoVisionClient(
        api_url=os.environ["OPV_PHOTO_VISION_API_URL"],
        api_key=os.environ["OPV_PHOTO_VISION_API_KEY"],
        model=os.environ["OPV_PHOTO_VISION_MODEL"])
    table = FeishuBitableClient(
        resolve_wiki_bitable_app_token(WIKI_TOKEN), TABLE_ID)
    token = get_tenant_access_token()

    prompt = """这是泰国女装穿搭图文的封面成片。只回答严格 JSON：
{"main_items": ["画面最外层主单品（1-2 件，含材质厚度印象）"],
 "garment_weight": "light | mid | heavy",
 "suitable_min_c": 整数,
 "suitable_max_c": 整数,
 "reason": "一句话"}
判定基准：heavy（羽绒/棉服/厚呢大衣）≈0-12°C；mid（夹克/开衫/风衣/薄针织叠穿）
≈10-22°C；light（短袖/单层衬衫/吊带）≈18-32°C。只看画面，不猜品牌。"""

    failures = 0
    for record in table.list_records(page_size=500):
        if record.record_id not in args.record_id:
            continue
        copy_zh = text_value(record.fields.get("完整文案（中文）"))
        band = parse_thermal_band(copy_zh)
        finals = record.fields.get("预览/成片") or []
        title = text_value(record.fields.get("内容方案摘要")).split("\n")[0][:40]
        if not finals:
            print(f"[{record.record_id}] 无成片，跳过｜{title}")
            continue
        if band is None:
            print(f"[{record.record_id}] 文案未声明温度带，跳过｜{title}")
            continue
        att = finals[0]
        req = urllib.request.Request(
            att["url"], headers={"Authorization": f"Bearer {token}"})
        local = Path(f"/tmp/thermal_check_{record.record_id}.jpg")
        local.write_bytes(urllib.request.urlopen(req, timeout=60).read())
        response = client.chat_with_multiple_images([local], prompt, 500)
        from services.photo_reference_vision import parse_vision_envelope
        result = parse_vision_envelope(response)
        lo = int(result.get("suitable_min_c") or 0)
        hi = int(result.get("suitable_max_c") or 0)
        verdict = "PASS" if thermal_overlap((lo, hi), band) else "FAIL"
        if verdict == "FAIL":
            failures += 1
        print(f"[{record.record_id}] {verdict}｜文案声明 {band[0]}-{band[1]}°C"
              f"｜封面主单品 {result.get('main_items')} → 适用 {lo}-{hi}°C"
              f"（{result.get('garment_weight')}）｜{result.get('reason')}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
