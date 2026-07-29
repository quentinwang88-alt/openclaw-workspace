#!/usr/bin/env python3
"""Idempotently configure the human short-video script library in Feishu."""

from __future__ import annotations

from run_pipeline import DEFAULT_MANUAL_SOURCE_FEISHU_URL, DEFAULT_SOURCE_FEISHU_URL, resolve_feishu_config
from core.bitable import FeishuBitableClient


STORE_OPTIONS = (
    "THPS01", "THFZ01", "VNPS01", "MYPS01", "VNFJ02", "THFZ02", "THWJ04",
    "THBTPS1", "THBT01", "THBT02", "THFJ01", "MXC001", "MXJF001",
)


def _template_field(client: FeishuBitableClient, field_name: str, fallback_type: int, fallback_ui: str):
    return next(
        (field for field in client.list_fields() if field.field_name == field_name),
        type("Template", (), {"field_type": fallback_type, "ui_type": fallback_ui, "property": None})(),
    )


def main() -> None:
    manual_app, manual_table = resolve_feishu_config(DEFAULT_MANUAL_SOURCE_FEISHU_URL)
    original_app, original_table = resolve_feishu_config(DEFAULT_SOURCE_FEISHU_URL)
    client = FeishuBitableClient(manual_app, manual_table)
    original_client = FeishuBitableClient(original_app, original_table)
    fields = client.list_fields()
    names = {field.field_name for field in fields}

    if "脚本ID" not in names:
        legacy = next((field for field in fields if field.field_name == "文本"), None)
        if legacy:
            try:
                client.update_field(legacy.field_id, field_name="脚本ID")
                print("已将主字段【文本】改为【脚本ID】")
            except Exception:
                # 飞书部分表格禁止通过 API 重命名主字段；保留它即可，系统 ID 使用独立字段。
                client.create_field("脚本ID", field_type=1, ui_type="Text")
                print("主字段【文本】不可通过接口重命名，已创建独立字段【脚本ID】")
        else:
            client.create_field("脚本ID", field_type=1, ui_type="Text")
            print("已创建字段【脚本ID】")

    original_fields = original_client.list_fields()
    original_by_name = {field.field_name: field for field in original_fields}
    image_template = original_by_name.get("产品图片")
    checkbox_template = original_by_name.get("是否可同步母版")
    datetime_template = original_by_name.get("轻量视频最近触发时间")

    field_specs = [
        ("脚本", 1, "Text", None),
        ("用途", 3, "SingleSelect", {"options": [{"name": "带货"}, {"name": "非带货"}]}),
        ("产品ID", 1, "Text", None),
        ("店铺", 3, "SingleSelect", {"options": [{"name": item} for item in STORE_OPTIONS]}),
        (
            "图片",
            getattr(image_template, "field_type", 17),
            getattr(image_template, "ui_type", "Attachment"),
            getattr(image_template, "property", None),
        ),
        (
            "同步",
            getattr(checkbox_template, "field_type", 7),
            getattr(checkbox_template, "ui_type", "Checkbox"),
            getattr(checkbox_template, "property", None),
        ),
        (
            "立即同步",
            getattr(checkbox_template, "field_type", 7),
            getattr(checkbox_template, "ui_type", "Checkbox"),
            getattr(checkbox_template, "property", None),
        ),
        ("语言", 1, "Text", None),
        ("时长", 2, "Number", None),
        ("状态", 1, "Text", None),
        (
            "时间",
            getattr(datetime_template, "field_type", 5),
            getattr(datetime_template, "ui_type", "DateTime"),
            getattr(datetime_template, "property", None),
        ),
    ]

    names = set(client.list_field_names())
    for field_name, field_type, ui_type, property_value in field_specs:
        if field_name in names:
            continue
        client.create_field(field_name, field_type=field_type, ui_type=ui_type, property=property_value)
        print(f"已创建字段【{field_name}】")

    print("人工脚本库字段已就绪；已有空白记录保持不变，不会触发同步。")


if __name__ == "__main__":
    main()
