"""模板优化修复三：已有源图零生图重排两种信息布局（交付样张，§9.3-1）。

- 374（recvvIvN6J9Yvl 配色教程）：源图 × v2 配色侧栏（主图70%＋示意色卡）。
- 371（recvvGl06csZ3L 旅行攻略）：源图 × v2 底部解释区（主图74%＋文字区）。

页文字取各自冻结计划的四页文案（与原成片同源）；色卡取该页实际穿搭的
中文颜色描述映射示意色值（程序示意，不伪装实测色）。不调用任何模型。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
for value in (str(PACKAGE_ROOT),):
    if value not in sys.path:
        sys.path.insert(0, value)

from PIL import Image  # noqa: E402

from services.photo_structured_layout import (  # noqa: E402
    render_structured_page_v2,
)

ROOT = Path.home() / ".openclaw/shared/data/organic_photo_video"
OUT = Path("/tmp/gc_relayout")
OUT.mkdir(parents=True, exist_ok=True)

TEMPLATE = json.loads(
    (PACKAGE_ROOT / "config/layouts/PHOTO_STRUCTURED_CLEAN_V1.json")
    .read_text(encoding="utf-8"))["render_options"]

#: 每页文字：取冻结计划的四页文案（与原成片一致，重新断行为 headline/body）。
COLOR_PAGES = [
    {"kicker": "โตเกียว",
     "headline": "ต่อสีอ่อน",
     "body": "เสื้อ กางเกง และรองเท้าโทนครีมช่วยให้สีต่อเนื่องใต้แจ็กเก็ต",
     "chips": [
         {"name_zh": "奶白", "hex": "#F0EAE0", "role": "top_inner"},
         {"name_zh": "浅丹宁蓝", "hex": "#7C93AC", "role": "bottom"},
     ]},
    {"kicker": "",
     "headline": "ตัดสว่าง–เข้ม",
     "body": "ฟ้าเดนิมกับกากีอ่อนช่วยให้สีน้ำตาลดูนุ่มและไม่ทึบ",
     "chips": [
         {"name_zh": "奶油白", "hex": "#F2EDE4", "role": "top_inner"},
         {"name_zh": "浅卡其", "hex": "#C9B48A", "role": "bottom"},
     ]},
    {"kicker": "",
     "headline": "ใช้สีเข้มสร้างมิติ",
     "body": "เสื้อและกางเกงสีชาร์โคลทำให้แจ็กเก็ตน้ำตาลดูเด่นขึ้น",
     "chips": [
         {"name_zh": "炭灰", "hex": "#4A4A48", "role": "top_inner"},
         {"name_zh": "深灰", "hex": "#3E3F41", "role": "bottom"},
     ]},
    {"kicker": "",
     "headline": "ย้ำสีเป็นจุด",
     "body": "รองเท้าน้ำตาลรับกับแจ็กเก็ต คั่นกลางด้วยสีอ่อน",
     "chips": [
         {"name_zh": "灰红棕", "hex": "#9A6A52", "role": "outerwear"},
         {"name_zh": "棕色", "hex": "#8B5E3C", "role": "shoes"},
     ]},
]

GUIDE_PAGES = [
    {"kicker": "โตเกียว", "headline": "ชั้นในบาง ถอดง่าย",
     "body": "เสื้อบางกับยีนส์ทรงตรงช่วยให้ถอดแจ็กเก็ตพาดแขนได้สบาย"},
    {"kicker": "", "headline": "สีอ่อนต่อเนื่อง",
     "body": "เสื้อและกระโปรงสีครีมช่วยลดการแบ่งช่วงสีใต้แจ็กเก็ต"},
    {"kicker": "", "headline": "ชายกระโปรงต่อกับบูต",
     "body": "ให้ชายยาวคลุมปากบูตเล็กน้อยและไม่แบ่งช่วงขาเป็นสองท่อน"},
    {"kicker": "", "headline": "ย่างก้าวสบายทั้งวัน",
     "body": "รองเท้าพื้นนุ่มกับทรงกางเกงที่ไม่กองช่วยให้เดินทั้งวันไม่เมื่อย"},
]


def source_images(item_dir: Path):
    return sorted(
        path for path in item_dir.glob("photo_style_*_P*_v1.png")
    )[:4]


def relayout(record: str, pages, with_chips: bool, prefix: str):
    item = ROOT / "style_reference_supply" / f"{record}_item_1"
    images = source_images(item)
    if len(images) != 4:
        raise SystemExit(f"{record}: 需要 4 张源图，找到 {len(images)}")
    report = []
    for index, (path, page) in enumerate(zip(images, pages), 1):
        image = Image.open(path).convert("RGB")
        payload = {
            "text": {
                "kicker": page["kicker"],
                "headline": page["headline"],
                "body": page["body"],
            },
            "color_chips": page.get("chips", []) if with_chips else [],
        }
        info = render_structured_page_v2(
            image, payload, TEMPLATE, index=index, cover_index=1, total=4)
        out_path = OUT / f"{prefix}_p{index}.jpg"
        image.save(out_path, "JPEG", quality=92)
        report.append({"page": index, "layout": info["layout_kind"],
                       "zones": info["zones"], "path": str(out_path)})
        print(f"{prefix} P{index}: {info['layout_kind']} → {out_path}")
    (OUT / f"{prefix}_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    relayout("recvvIvN6J9Yvl", COLOR_PAGES, with_chips=True, prefix="color374")
    relayout("recvvGl06csZ3L", GUIDE_PAGES, with_chips=False, prefix="guide371")
    print("完成：零生图重排样张在 /tmp/gc_relayout/")
