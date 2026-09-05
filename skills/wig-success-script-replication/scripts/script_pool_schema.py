#!/usr/bin/env python3
"""Non-destructive, hash-confirmed extension of the existing script workbench."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from ensure_feishu_schema import (
    FeishuAdmin, SchemaError, _load_credentials, _verify_receipt, _write_receipt, plan_hash,
)

POOL_WIKI = "https://gcngopvfvo0q.feishu.cn/wiki/KsX7w8Y8ZiJfnsk2Mtvc7xLun1f"
POOL_TABLE = "tblIvHJ0nsn9WCwi"
RECEIPT = Path("/tmp/wsr-script-pool-schema-plan.json")
FIELDS = (
    ("发布用途", 3, ["带货", "养号", "种草"], "【系统带入】脚本内容用途；修改任务用途只影响新生成结果，不将旧脚本自动改写为另一用途。"),
    ("脚本来源", 3, ["原创生成", "成功脚本复刻", "视频复刻", "人工编写"], "【系统维护】生成来源，和发布用途、是否挂车分别管理。"),
    ("是否挂车", 3, ["是", "否"], "【系统带入，可在进入生产前调整】养号默认否，带货默认是；有关联产品不等于挂车。已排期/提交任务不会随本字段自动更改。"),
    ("人物参考图（系统）", 17, [], "【系统同步，无需重复上传】人物图只控制人物身份；产品图片单独控制商品。"),
)
EXTENSIONS = {
    "脚本类型": ["短视频复刻脚本", "养号脚本"],
    "视频形态（系统）": ["短视频"],
}
VISIBLE = {
    "脚本ID", "脚本标题", "发布用途", "脚本来源", "是否挂车", "产品编码", "店铺ID",
    "目标国家", "目标语言", "视频时长", "产品图片", "人物参考图（系统）", "口播_目标语言",
    "口播_中文", "完整生产脚本", "视频生成提示词", "短视频提示词", "处理状态", "进入生产",
    "审核意见", "同步结果", "同步时间", "生成首帧（需勾选）", "统一首帧（系统）",
    "首帧准备状态（系统）", "运行任务ID",
}
VIEWS = {
    "总库·全部脚本": [],
    "总库·墨西哥复刻": [("脚本来源", "is", ["成功脚本复刻"]), ("目标国家", "is", ["墨西哥"])],
    "总库·养号内容": [("发布用途", "is", ["养号"])],
    "总库·待送生产": [("处理状态", "is", ["待审核"]), ("处理状态", "is", ["已选用"])],
}


def view_definition(name):
    return {"filters": VIEWS[name], "visible": sorted(VISIBLE),
            "conjunction": "or" if name == "总库·待送生产" else "and"}


def snapshot(admin, app):
    base = f"/bitable/v1/apps/{app}/tables/{POOL_TABLE}"
    views = admin.list_all(base + "/views")
    # The list endpoint omits properties; GET each owned view before comparing.
    for view in views:
        if (view.get("view_name") or view.get("name")) in VIEWS:
            detail = admin.request("GET", base + "/views/" + view["view_id"])
            view.update(detail.get("view") or {})
    return {
        "fields": admin.list_all(base + "/fields"),
        "views": views,
    }


def comparable_property(prop):
    filters = prop.get("filter_info") or {}
    conditions = []
    for item in filters.get("conditions") or []:
        value = item.get("value")
        try:
            value = json.loads(value) if isinstance(value, str) else value
        except ValueError:
            pass
        conditions.append({"field_id": item.get("field_id"), "operator": item.get("operator"), "value": value})
    return {"hidden_fields": sorted(prop.get("hidden_fields") or []),
            "filter_info": {"conjunction": filters.get("conjunction") or "and", "conditions": conditions}}


def view_property(name, fields):
    by_name = {f["field_name"]: f for f in fields}
    conditions = []
    for field_name, operator, values in VIEWS[name]:
        field = by_name[field_name]
        if field["type"] in (3, 4):
            options = {o["name"]: o["id"] for o in field.get("property", {}).get("options", [])}
            values = [options[v] for v in values]
        conditions.append({"field_id": field["field_id"], "operator": operator,
                           "value": json.dumps(values, ensure_ascii=False)})
    return {
        "hidden_fields": [f["field_id"] for f in fields if f["field_name"] not in VISIBLE],
        "filter_info": {"conjunction": view_definition(name)["conjunction"], "conditions": conditions},
    }


def build_pool_plan(state):
    fields = {f["field_name"]: f for f in state["fields"]}
    actions = []
    for name, kind, options, description in FIELDS:
        existing = fields.get(name)
        if existing and existing["type"] != kind:
            raise SchemaError(f"field type drift: {name}")
        if not existing:
            payload = {"field_name": name, "type": kind,
                       "description": {"text": description, "disable_sync": False}}
            if options:
                payload["property"] = {"options": [{"name": v} for v in options]}
            actions.append({"op": "create-field", "body": payload})
        elif options:
            present = [o["name"] for o in (existing.get("property") or {}).get("options", [])]
            missing = [o for o in options if o not in present]
            if missing:
                prop = dict(existing.get("property") or {})
                prop["options"] = prop.get("options", []) + [{"name": v} for v in missing]
                actions.append({"op": "extend-field", "id": existing["field_id"],
                                "body": {"field_name": name, "type": kind, "property": prop}})
    for name, values in EXTENSIONS.items():
        existing = fields.get(name)
        if not existing or existing["type"] != 3:
            raise SchemaError(f"required existing single-select field missing: {name}")
        prop = dict(existing.get("property") or {})
        present = [o["name"] for o in prop.get("options", [])]
        missing = [v for v in values if v not in present]
        if missing:
            prop["options"] = prop.get("options", []) + [{"name": v} for v in missing]
            actions.append({"op": "extend-field", "id": existing["field_id"],
                            "body": {"field_name": name, "type": 3, "property": prop}})
    views = {v.get("view_name") or v.get("name"): v for v in state["views"]}
    for name in VIEWS:
        view = views.get(name)
        if not view:
            actions.append({"op": "create-view", "name": name})
        # Resolve IDs after field creation. Only our four views are configured;
        # existing operator-owned views are never renamed or hidden.
        if actions or view is None or any(n not in fields for n, *_ in FIELDS):
            actions.append({"op": "configure-view", "name": name, "definition": view_definition(name)})
        else:
            wanted = view_property(name, state["fields"])
            current = view.get("property") or {}
            if comparable_property(current) != comparable_property(wanted):
                actions.append({"op": "configure-view", "name": name, "definition": view_definition(name)})
    return actions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-plan", default="")
    parser.add_argument("--receipt", type=Path, default=RECEIPT)
    args = parser.parse_args()
    admin = FeishuAdmin(*_load_credentials())
    app = admin.resolve_app_token(POOL_WIKI)
    state = snapshot(admin, app)
    actions = build_pool_plan(state)
    digest = plan_hash(app, actions)
    print(json.dumps({"app_token": app, "table_id": POOL_TABLE, "plan_hash": digest,
                      "actions": actions, "mode": "apply" if args.apply else "dry-run"}, ensure_ascii=False))
    if not args.apply:
        _write_receipt(args.receipt, app, digest)
        return 0
    if args.confirm_plan != digest:
        raise SchemaError("plan changed; run dry-run again")
    _verify_receipt(args.receipt, app, digest)
    base = f"/bitable/v1/apps/{app}/tables/{POOL_TABLE}"
    for action in actions:
        if action["op"] == "create-field":
            admin.request("POST", base + "/fields", body=action["body"])
        elif action["op"] == "extend-field":
            admin.request("PUT", base + "/fields/" + action["id"], body=action["body"])
        elif action["op"] == "create-view":
            admin.request("POST", base + "/views", body={"view_name": action["name"], "view_type": "grid"})
    fresh = snapshot(admin, app)
    for action in actions:
        if action["op"] == "configure-view":
            name = action["name"]
            view = next(v for v in fresh["views"] if (v.get("view_name") or v.get("name")) == name)
            admin.request("PATCH", base + "/views/" + view["view_id"],
                          body={"property": view_property(name, fresh["fields"])})
    verified = snapshot(admin, app)
    remaining = build_pool_plan(verified)
    print(json.dumps({"applied": len(actions), "remaining_actions": remaining}, ensure_ascii=False))
    return 1 if remaining else 0


if __name__ == "__main__":
    raise SystemExit(main())
