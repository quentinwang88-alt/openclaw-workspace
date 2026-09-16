#!/usr/bin/env python3
"""Idempotently install the compact OPV Feishu workbench schema."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
BITABLE_SKILL = WORKSPACE_ROOT / "skills" / "script-run-manager-sync"
for value in (str(BITABLE_SKILL), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402
from services.feishu_workflow import (  # noqa: E402
    FIELD_EXECUTE, FIELD_NOTES, FIELD_OUTPUT, FIELD_PRESET, FIELD_PRODUCT, FIELD_STORE,
    FIELD_PROGRESS, FIELD_REVIEW, FIELD_QUANTITY, FIELD_QUANTITY_LEGACY, FIELD_CONFIRM_PUBLISH,
    FIELD_PHOTO_SUMMARY, FIELD_PHOTO_INPUT, FIELD_PHOTO_INPUT_LEGACY,
    FIELD_PRODUCT_REFERENCE, FIELD_PHOTO_ASSET_STATUS, FIELD_REFERENCE,
    FIELD_REFERENCE_TYPE, FIELD_CONTENT_THEME, FIELD_MUSIC_MODE,
    FIELD_CONTENT_REQUIREMENT, FIELD_TARGET_ACCOUNT, FIELD_FULL_COPY_ZH,
    FIELD_VISUAL_PRESET,
    FIELD_TEMPERATURE_BAND, FIELD_THERMAL_SENSITIVITY, FIELD_TEMPERATURE_SCENE,
    FIELD_TRANSITION_SCENE, FIELD_TRANSITION_SENSITIVITY, FIELD_DRESS_CODE,
    FIELD_LAYER_BASE_REFERENCE, FIELD_LAYER_MID_REFERENCE, FIELD_LAYER_OUTER_REFERENCE,
    TEMPERATURE_BAND_OPTIONS, THERMAL_SENSITIVITY_OPTIONS, TEMPERATURE_SCENE_OPTIONS,
    TRANSITION_SCENE_OPTIONS, DRESS_CODE_OPTIONS,
    FIELD_REVIEW_MODE, FIELD_REVIEW_STAGE, FIELD_RETRY_REVIEW, FIELD_REVIEW_TOKEN,
    ProductionPresetCatalog,
)
from services.photo_reference import REFERENCE_TYPE_OPTIONS  # noqa: E402
from services.photo_theme import THEME_OPTIONS  # noqa: E402
from services.visual_preset import visual_preset_options  # noqa: E402

#: 暂不下发到线上「图文主题」下拉的选项（2026-09-15 精简）：冷热切换/温度穿搭
#: 只服务分层线，而分层预设当前全部停用；解析能力不受影响（THEME_OPTIONS
#: 仍是全集），将来分层线恢复时把名字从这里移除并重跑本脚本即可。
HIDDEN_THEME_OPTIONS = ("冷热切换", "温度穿搭")


def visible_theme_options():
    return [name for name in THEME_OPTIONS if name not in HIDDEN_THEME_OPTIONS]


def options(values):
    return {"options": [{"name": value} for value in values]}


def target_account_options():
    """目标账号下拉选项：仓库种子配置（确定性，供本地 schema 回归）。

    运营可在飞书手动补充选项；生成侧按严格解析兜底（不存在即报错，
    不静默回退公共池）。此处不读发布器库，保持离线可测。
    """
    handles = set()
    try:
        from services.publish_account_profile import PublishAccountProfileResolver
        handles.update(PublishAccountProfileResolver().options())
    except Exception:
        pass
    return sorted(handles)


def publish_store_options():
    routes = json.loads(
        (PACKAGE_ROOT / "config" / "main_publish_routes.json").read_text(encoding="utf-8")
    ).get("routes", {})
    return sorted({
        str(route.get("default_store_id") or "").strip()
        for route in routes.values()
        if str(route.get("default_store_id") or "").strip()
    })


def rename_field(client, field, target_name: str) -> None:
    """Feishu validates the full field contract even for a primary rename."""
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{client.app_token}/"
        f"tables/{client.table_id}/fields/{field.field_id}"
    )
    payload = {
        "field_name": target_name,
        "type": field.field_type,
        "ui_type": field.ui_type or "Text",
    }
    if field.property is not None:
        payload["property"] = field.property
    response = client._request("PUT", url, headers=client._headers(), json=payload)
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"重命名主字段失败：{result.get('msg')}")


def add_missing_select_options(client, field, desired) -> bool:
    existing = list((field.property or {}).get("options") or [])
    names = {str(item.get("name") or "") for item in existing}
    missing = [value for value in desired if value not in names]
    if not missing:
        return False
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{client.app_token}/"
        f"tables/{client.table_id}/fields/{field.field_id}"
    )
    payload = {
        "field_name": field.field_name,
        "type": field.field_type,
        "ui_type": field.ui_type or "SingleSelect",
        "property": {**dict(field.property or {}), "options": existing + [{"name": value} for value in missing]},
    }
    response = client._request("PUT", url, headers=client._headers(), json=payload)
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"补充字段选项失败【{field.field_name}】：{result.get('msg')}")
    return True


def merge_attachments(*groups):
    """Keep attachment order and deduplicate by Feishu file token."""
    output, seen = [], set()
    for group in groups:
        for item in group or []:
            if not isinstance(item, dict):
                continue
            identity = str(item.get("file_token") or item.get("name") or item)
            if identity in seen:
                continue
            seen.add(identity)
            output.append(item)
    return output


def consolidate_reference_field(client, by_name, *, dry_run: bool) -> dict:
    """Move legacy product-reference attachments, then retire the old field."""
    legacy = by_name.get(FIELD_PRODUCT_REFERENCE)
    if legacy is None:
        return {"legacy_field_present": False, "records_to_migrate": 0, "deleted": False}
    records = client.list_records(page_size=500)
    updates = []
    for record in records:
        old = list(record.fields.get(FIELD_PRODUCT_REFERENCE) or [])
        if not old:
            continue
        current = list(record.fields.get(FIELD_REFERENCE) or [])
        merged = merge_attachments(current, old)
        if merged != current:
            updates.append({"record_id": record.record_id, "fields": {FIELD_REFERENCE: merged}})
    if dry_run:
        return {
            "legacy_field_present": True, "records_scanned": len(records),
            "records_to_migrate": len(updates), "deleted": False,
        }
    for start in range(0, len(updates), 500):
        client.batch_update_records(updates[start:start + 500])
    # Delete only after every old attachment has been copied to the unified field.
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{client.app_token}/"
        f"tables/{client.table_id}/fields/{legacy.field_id}"
    )
    response = client._request("DELETE", url, headers=client._headers())
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"删除旧商品参考图字段失败：{result.get('msg')}")
    final_names = set(client.list_field_names())
    if FIELD_PRODUCT_REFERENCE in final_names or FIELD_REFERENCE not in final_names:
        raise RuntimeError("统一参考图字段回读校验失败")
    return {
        "legacy_field_present": True, "records_scanned": len(records),
        "records_migrated": len(updates), "deleted": True,
    }


def task_field_specs(catalog) -> list:
    """Return the shipped task-table field definitions (local definition only).

    Kept as a module-level function so the declared schema can be inspected and
    regression-tested without touching the online Bitable.
    """
    return [
        (FIELD_STORE, 3, "SingleSelect", options(publish_store_options())),
        (FIELD_PRESET, 3, "SingleSelect", options(catalog.names)),
        (FIELD_EXECUTE, 7, "Checkbox", None),
        (FIELD_QUANTITY, 2, "Number", {"formatter": "0"}),
        (FIELD_CONTENT_THEME, 3, "SingleSelect", options(visible_theme_options())),
        (FIELD_CONTENT_REQUIREMENT, 1, "Text", None),
        # 目标发布账号（真实 TikTok handle）：留空＝沿用店铺公共池；选择后
        # 内容冻结该账号定位并只能被该账号领取。选项来自种子配置/账号表，
        # 运营可在飞书手动补充。
        (FIELD_TARGET_ACCOUNT, 3, "SingleSelect", options(target_account_options())),
        # 视觉预设（高级/可选）：留空＝继承账号默认（无绑定时入口默认只在
        # Phase 3 接线后影响画面）。背景方式/摄影/排版的统一入口，不再拆成
        # 多个下拉。
        (FIELD_VISUAL_PRESET, 3, "SingleSelect", options(visual_preset_options())),
        (FIELD_TEMPERATURE_BAND, 3, "SingleSelect", options(TEMPERATURE_BAND_OPTIONS)),
        (FIELD_THERMAL_SENSITIVITY, 3, "SingleSelect", options(THERMAL_SENSITIVITY_OPTIONS)),
        (FIELD_TEMPERATURE_SCENE, 3, "SingleSelect", options(TEMPERATURE_SCENE_OPTIONS)),
        # 日常冷热切换线：切换场景 / 体感 / 着装要求。仅在本地 schema 定义中
        # 声明，是否同步到线上表由使用者显式执行，本任务不触发写入。
        (FIELD_TRANSITION_SCENE, 3, "SingleSelect", options(TRANSITION_SCENE_OPTIONS)),
        (FIELD_TRANSITION_SENSITIVITY, 3, "SingleSelect", options(THERMAL_SENSITIVITY_OPTIONS)),
        (FIELD_DRESS_CODE, 3, "SingleSelect", options(DRESS_CODE_OPTIONS)),
        (FIELD_LAYER_BASE_REFERENCE, 17, "Attachment", None),
        (FIELD_LAYER_MID_REFERENCE, 17, "Attachment", None),
        (FIELD_LAYER_OUTER_REFERENCE, 17, "Attachment", None),
        ("旅行地点（可选）", 1, "Text", None),
        ("重拍 Look（可选）", 1, "Text", None),
        (FIELD_REFERENCE_TYPE, 3, "SingleSelect", options(REFERENCE_TYPE_OPTIONS)),
        (FIELD_REFERENCE, 17, "Attachment", None),
        (FIELD_PHOTO_SUMMARY, 1, "Text", None),
        # 完整文案（中文）：最终成片包口径的标题/正文/标签/逐页文字中文回写，
        # 按文案指纹缓存翻译；翻译失败显示占位，可独立补跑。
        (FIELD_FULL_COPY_ZH, 1, "Text", None),
        (FIELD_PHOTO_INPUT, 17, "Attachment", None),
        (FIELD_PHOTO_ASSET_STATUS, 1, "Text", None),
        # 来源标记（2026-09-16 自动供稿）：机器可读的行来源指纹（如
        # auto_supply|日期|账号），供供稿幂等对账。专用列的原因：备注会被
        # 工作流在生成过程中覆写，不能承载持久标记；本列工作流只读不写。
        ("来源标记", 1, "Text", None),
        (FIELD_PROGRESS, 3, "SingleSelect", options([
            "待执行", "生成中", "待审核", "已完成", "需处理",
            "待排班", "已排期", "提交中", "发布中", "已发布", "发布失败",
        ])),
        (FIELD_OUTPUT, 17, "Attachment", None),
        (FIELD_REVIEW, 3, "SingleSelect", options([
            "待审核", "无需审核", "通过", "重做P1", "重做P2", "重做P3", "重做P4",
            "重做P5", "重做成片", "整组重做", "排期发布",
        ])),
        (FIELD_CONFIRM_PUBLISH, 7, "Checkbox", None),
        (FIELD_MUSIC_MODE, 1, "Text", None),
        (FIELD_REVIEW_MODE, 3, "SingleSelect", options(["自动审核", "人工确认"])),
        (FIELD_REVIEW_STAGE, 3, "SingleSelect", options(["锚点审核", "组图审核", "成片终审", "图文终审", "素材审核", "已验收", "处理中", "锚点技术检查", "组图技术检查", "成片技术检查", "技术完成"])),
        (FIELD_RETRY_REVIEW, 7, "Checkbox", None),
        (FIELD_REVIEW_TOKEN, 1, "Text", None),
        (FIELD_NOTES, 1, "Text", None),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wiki-token", default="TR10wxEXHiCYIhk8clActVdenpc")
    parser.add_argument("--table-id", default="tblj3x846gU3rshB")
    parser.add_argument("--preset-only", help="append one known preset option only; do not alter other fields")
    parser.add_argument(
        "--store-only", action="store_true",
        help="create/update only the publish-store selector; do not alter other fields",
    )
    parser.add_argument(
        "--consolidate-reference-field", action="store_true",
        help="copy legacy 商品参考图 attachments into 参考图（可选）, then delete the legacy field",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    client = FeishuBitableClient(resolve_wiki_bitable_app_token(args.wiki_token), args.table_id)
    catalog = ProductionPresetCatalog()
    fields = client.list_fields()
    by_name = {field.field_name: field for field in fields}
    if args.consolidate_reference_field:
        report = consolidate_reference_field(client, by_name, dry_run=args.dry_run)
        print(json.dumps({"dry_run": args.dry_run, **report}, ensure_ascii=False, indent=2))
        return 0
    if args.preset_only:
        if args.preset_only not in catalog.names or FIELD_PRESET not in by_name:
            raise ValueError("known preset and existing production-preset field required")
        field = by_name[FIELD_PRESET]
        before = {str(item.get("name")) for item in (field.property or {}).get("options", [])}
        changed = False
        if not args.dry_run:
            changed = add_missing_select_options(client, field, [args.preset_only])
            checked = next(f for f in client.list_fields() if f.field_id == field.field_id)
            after = {str(item.get("name")) for item in (checked.property or {}).get("options", [])}
            if not before.issubset(after) or args.preset_only not in after:
                raise RuntimeError("preset append readback mismatch")
        print(json.dumps({"preset": args.preset_only, "dry_run": args.dry_run,
            "missing": args.preset_only not in before, "appended": changed, "record_writes": 0}, ensure_ascii=False))
        return 0
    if args.store_only:
        desired = publish_store_options()
        field = by_name.get(FIELD_STORE)
        if args.dry_run:
            existing = {str(item.get("name") or "") for item in ((field.property if field else {}) or {}).get("options", [])}
            print(json.dumps({
                "field": FIELD_STORE, "dry_run": True, "exists": field is not None,
                "desired_options": desired, "missing_options": sorted(set(desired) - existing),
                "record_writes": 0,
            }, ensure_ascii=False, indent=2))
            return 0
        if field is None:
            client.create_field(FIELD_STORE, 3, "SingleSelect", options(desired))
            action = "created"
        else:
            action = "options_appended" if add_missing_select_options(client, field, desired) else "unchanged"
        checked = next(item for item in client.list_fields() if item.field_name == FIELD_STORE)
        actual = {str(item.get("name") or "") for item in (checked.property or {}).get("options", [])}
        if not set(desired).issubset(actual):
            raise RuntimeError("店铺字段回读校验失败")
        print(json.dumps({
            "field": FIELD_STORE, "action": action, "options": sorted(actual),
            "record_writes": 0,
        }, ensure_ascii=False, indent=2))
        return 0
    if args.dry_run:
        print(json.dumps({"dry_run": True, "existing_fields": sorted(by_name), "writes": 0}, ensure_ascii=False))
        return 0
    created = []
    renamed = []
    if FIELD_PRODUCT not in by_name:
        primary = fields[0]
        rename_field(client, primary, FIELD_PRODUCT)
        renamed.append({"from": primary.field_name, "to": FIELD_PRODUCT})
        by_name[FIELD_PRODUCT] = primary
    if FIELD_PHOTO_INPUT not in by_name and FIELD_PHOTO_INPUT_LEGACY in by_name:
        old = by_name[FIELD_PHOTO_INPUT_LEGACY]
        rename_field(client, old, FIELD_PHOTO_INPUT)
        renamed.append({"from": FIELD_PHOTO_INPUT_LEGACY, "to": FIELD_PHOTO_INPUT})
        by_name[FIELD_PHOTO_INPUT] = old
    if FIELD_QUANTITY not in by_name and FIELD_QUANTITY_LEGACY in by_name:
        old = by_name[FIELD_QUANTITY_LEGACY]
        rename_field(client, old, FIELD_QUANTITY)
        renamed.append({"from": FIELD_QUANTITY_LEGACY, "to": FIELD_QUANTITY})
        by_name[FIELD_QUANTITY] = old
    specs = task_field_specs(catalog)
    for name, field_type, ui_type, property_value in specs:
        if name in by_name:
            field = by_name[name]
            wanted = [item["name"] for item in (property_value or {}).get("options", [])]
            if wanted and add_missing_select_options(client, field, wanted):
                created.append(f"{name}:补充选项")
            continue
        client.create_field(name, field_type, ui_type, property_value)
        created.append(name)
    final_fields = client.list_fields()
    print(json.dumps({
        "renamed": renamed,
        "created": created,
        "field_names": [field.field_name for field in final_fields],
        "request_count": client.request_count,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
