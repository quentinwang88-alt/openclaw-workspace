#!/usr/bin/env python3
"""只读入口清单：预设目录 + 「预设→配方→流程→主题→语言→排版→输出」对照。

运营日常入口就是飞书任务表的「生产预设」下拉，但下拉本身只说名字。这个脚本把
"这条线归谁管、主题能选什么、文案用哪种语言、出几张什么版式、发到哪家店" 从
config/ 与配方里**读出来**打一张对照表，用于排查，不构成第二份执行配置。

原则：
- 只读；不加载环境、不连数据库、不调用模型、不写远端。
- 一切取值来自 config/ 与配方文件；没有一处是脚本自己"拍"的业务规则
  （主题缺省规则直接调 ``services.feishu_workflow.theme_is_optional``）。
- 输出必须可复现：不写时间戳、不做集合迭代，顺序全部显式排序。

用法（包目录内）：
    /usr/bin/python3 scripts/photo_entry_catalog.py
    /usr/bin/python3 scripts/photo_entry_catalog.py --group trial
    /usr/bin/python3 scripts/photo_entry_catalog.py --output docs/OPV_ENTRY_CATALOG_20260914.md
    /usr/bin/python3 scripts/photo_entry_catalog.py --check docs/OPV_ENTRY_CATALOG_20260914.md
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

CONFIG_DIR = PACKAGE_ROOT / "config"

from config.loader import load_content_recipes  # noqa: E402
from services.feishu_workflow import (  # noqa: E402
    PRESET_ENTRY_GROUPS, ProductionPresetCatalog, preset_entry_group, theme_is_optional,
)
from services.photo_content_planner import (  # noqa: E402
    get_planning_flow, load_planning_policy, recipe_has_planning_policy,
)
from services.photo_flow_registry import (  # noqa: E402
    is_layered_progression_flow, is_thermal_transition_flow,
)

GROUP_LABELS = {
    "production": "运营日常入口",
    "trial": "已配置待验收",
    "legacy": "保留的历史入口",
}


def _unique(values) -> list:
    seen, result = set(), []
    for value in values:
        text = str(value or "")
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def _publish_routes() -> dict:
    path = CONFIG_DIR / "main_publish_routes.json"
    if not path.is_file():
        return {}
    return dict(json.loads(path.read_text(encoding="utf-8")).get("routes") or {})


def _recipe_index() -> dict:
    return {recipe.recipe_id: recipe for recipe in load_content_recipes(CONFIG_DIR / "recipes")}


def _recipe_view(recipe_id: str, recipes: dict) -> dict:
    """一条配方在对照表里需要呈现的全部事实；缺失时为 None 而不是猜。"""
    recipe = recipes.get(recipe_id)
    if recipe is None:
        return {"recipe_id": recipe_id, "exists": False}
    spec = dict(recipe.recipe_spec_json or {})
    story = list(recipe.story_structure_json or [])
    planning_flow = get_planning_flow(recipe_id)
    locale_packs = {
        str(key): str(value) for key, value in (spec.get("locale_copy_packs") or {}).items()
    }
    theme_keys: list = []
    if recipe_has_planning_policy(recipe_id):
        policy = load_planning_policy(recipe_id)
        theme_keys = [str(value) for value in policy.get("supported_theme_keys") or []]
    return {
        "recipe_id": recipe_id,
        "exists": True,
        "media_kind": str(spec.get("media_kind") or ""),
        "shot_count": recipe.shot_count,
        "anchor_slot": recipe.anchor_slot,
        "theme_types": [str(value) for value in spec.get("theme_types") or []],
        "product_modes": [str(value) for value in spec.get("product_modes") or []],
        "planning_flow": planning_flow,
        "theme_keys": theme_keys,
        "theme_optional": theme_is_optional(
            locale_pack=(locale_packs or None),
            layering_flow=is_layered_progression_flow(planning_flow),
            thermal_transition_flow=is_thermal_transition_flow(planning_flow),
            planning_flow=planning_flow,
        ),
        "locale_packs": locale_packs,
        "layout_variants": _unique(item.get("layout_variant") for item in story),
        "roles": _unique(item.get("role") for item in story),
    }


def build_payload(config_dir: Path = CONFIG_DIR) -> dict:
    catalog = ProductionPresetCatalog(config_dir / "feishu_production_presets.json")
    recipes = _recipe_index()
    routes = _publish_routes()
    presets = []
    for raw in json.loads(
            (config_dir / "feishu_production_presets.json").read_text(encoding="utf-8")
    )["presets"]:
        name = str(raw["name"])
        tasks = []
        for task in raw.get("tasks") or []:
            recipe = _recipe_view(str(task.get("recipe_id") or ""), recipes)
            market = str(task.get("market") or "")
            store = str((routes.get(market) or {}).get("default_store_id") or "")
            tasks.append({
                "account_id": str(task.get("account_id") or ""),
                "market": market,
                "language": str(task.get("language") or ""),
                "hook_strategy": str(task.get("hook_strategy") or ""),
                "theme_id": str(task.get("theme_id") or ""),
                "store_id": store,
                "recipe": recipe,
            })
        presets.append({
            "name": name,
            "entry_group": preset_entry_group(raw),
            "status": str(raw.get("status", "active")),
            "enabled": str(raw.get("status", "active")) == "active",
            "media_kind": str(raw.get("media_kind") or ""),
            "category_key": str(raw.get("category_key") or ""),
            "routing_policy": str(raw.get("routing_policy") or ""),
            "tasks_from": [str(value) for value in raw.get("tasks_from") or []],
            "tasks": tasks,
        })
    # 目录顺序固定为「分组顺序 → 配置里出现顺序」，方便与 JSON 对照。
    presets.sort(key=lambda item: PRESET_ENTRY_GROUPS.index(item["entry_group"]))
    return {
        "groups": list(PRESET_ENTRY_GROUPS),
        "presets": presets,
        "publish_routes": routes,
    }


def _yes_no(flag: bool) -> str:
    return "是" if flag else "否"


def _list_or_dash(values) -> str:
    return "、".join(f"`{value}`" for value in values) if values else "—"


def render_groups(payload: dict, group: str = None) -> str:
    rows = [item for item in payload["presets"]
            if group is None or item["entry_group"] == group]
    lines = ["| 分组 | 预设 | 状态 | 可用 | 类型 | 市场 | 配方 |",
             "|---|---|---|---|---|---|---|"]
    for item in rows:
        markets = _unique(task["market"] for task in item["tasks"])
        recipe_ids = _unique(task["recipe"]["recipe_id"] for task in item["tasks"])
        if not recipe_ids and item["tasks_from"]:
            recipe_ids = [f"（候选项：{'、'.join(item['tasks_from'])}）"]
        lines.append("| {} | {} | {} | {} | {} | {} | {} |".format(
            item["entry_group"], item["name"], item["status"], "✅" if item["enabled"] else "⛔",
            item["media_kind"] or "—", "、".join(markets) or "—",
            "、".join(f"`{value}`" for value in recipe_ids) or "—",
        ))
    return "\n".join(lines)


def render_mapping(payload: dict, group: str = None) -> str:
    blocks = []
    for item in payload["presets"]:
        if group is not None and item["entry_group"] != group:
            continue
        head = [
            f"### {item['name']}",
            "",
            f"分组 `{item['entry_group']}`（{GROUP_LABELS[item['entry_group']]}）；"
            f"状态 `{item['status']}`；可用：{_yes_no(item['enabled'])}；"
            f"类目 `{item['category_key'] or '—'}`；路由策略 `{item['routing_policy'] or '—'}`",
            "",
        ]
        if not item["tasks"]:
            head.append(
                "按 `selection: deterministic_one` 从候选项里选一条："
                + _list_or_dash(item["tasks_from"]) + "。"
            )
            head.append("")
            blocks.append("\n".join(head))
            continue
        for index, task in enumerate(item["tasks"], start=1):
            recipe = task["recipe"]
            body = [f"- **任务 {index}**：账号 `{task['account_id']}`；"
                    f"市场/语言 `{task['market']}`/`{task['language']}`；"
                    f"钩子 `{task['hook_strategy'] or '—'}`"]
            if not recipe["exists"]:
                body.append(f"  - 配方 `{recipe['recipe_id']}`：**配置文件缺失**")
                continue
            body.append(
                f"  - 配方：`{recipe['recipe_id']}`（{recipe['media_kind'] or '历史类型'}，"
                f"{recipe['shot_count']} 页，锚点槽位 {recipe['anchor_slot']}）"
            )
            if task["theme_id"]:
                body.append(f"  - 预设自带主题：`{task['theme_id']}`")
            body.append(f"  - 规划流程：`{recipe['planning_flow'] or '—'}`")
            body.append(f"  - 主题范围：{_list_or_dash(recipe['theme_keys'])}")
            body.append("  - 主题缺省："
                        + ("可空（文案已由语言包承担）" if recipe["theme_optional"] else "必填"))
            if recipe["locale_packs"]:
                pairs = "；".join(f"`{key}` → `{value}`"
                                 for key, value in sorted(recipe["locale_packs"].items()))
                body.append(f"  - 文案来源：语言包 {pairs}")
            else:
                body.append("  - 文案来源：v1 内联（随「图文主题」携带 th-TH 文案）")
            body.append(f"  - 排版：版式 {_list_or_dash(recipe['layout_variants'])}；"
                        f"角色 {_list_or_dash(recipe['roles'])}")
            body.append(
                f"  - 输出形式：`{recipe['media_kind'] or 'video'}` {recipe['shot_count']} 页；"
                f"发布店铺（{task['market']}）"
                + (f"=`{task['store_id']}`" if task["store_id"] else "**未配置路由**")
            )
        blocks.append("\n".join(head + body))
    return "\n\n".join(blocks)


HEADER = """# OPV 入口清单：预设目录与「预设→配方→流程→主题→语言→排版→输出」对照

> 本文件由 `scripts/photo_entry_catalog.py` 生成，**请勿手改**。配置改动后重新生成：
> `PYTHONPATH=. /usr/bin/python3 scripts/photo_entry_catalog.py --output <本文件>`
>
> 用途：排查「这条线归谁管、主题能选什么、文案用哪种语言、出几张什么版式、发到哪家店」。
> 所有取值都从 `config/` 与配方文件里读出来，**不是**第二份执行配置。

## 口径

- `entry_group` 只控制目录与视图：`production`=运营日常入口、`trial`=已配置待验收、
  `legacy`=保留的历史入口。**能否使用仍由预设 `status` 决定**，非 `active` 一律拒绝。
- 「主题范围」＝该配方规划策略的 `supported_theme_keys`；空则说明该配方不接规划策略。
- 「主题缺省」＝`services/feishu_workflow.theme_is_optional`（唯一归属，工作流与本文共用）。
- 「文案来源」：声明了 `locale_copy_packs` 的国家无关配方由语言包提供；v1 配方随
  「图文主题」携带内联泰语。

## 1. 预设目录

"""


def render_markdown(payload: dict, group: str = None) -> str:
    parts = [HEADER, render_groups(payload, group), "",
             "## 2. 预设 → 配方 → 流程 → 主题 → 语言 → 排版 → 输出", ""]
    parts.append(render_mapping(payload, group))
    parts.append("")
    return "\n".join(parts)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-dir", type=Path, default=CONFIG_DIR)
    parser.add_argument("--group", choices=list(PRESET_ENTRY_GROUPS), default=None,
                        help="只看某一组")
    parser.add_argument("--output", type=Path, default=None, help="写入文件（默认打印）")
    parser.add_argument("--check", type=Path, default=None,
                        help="与已有文件比对，不一致时退出码 1（不改文件）")
    parser.add_argument("--json", action="store_true", help="打印结构化结果")
    args = parser.parse_args()

    payload = build_payload(args.config_dir)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    text = render_markdown(payload, args.group)
    if args.check is not None:
        if not args.check.is_file():
            print(f"❌ 找不到 {args.check}；先跑一次 --output 生成", file=sys.stderr)
            return 1
        if args.check.read_text(encoding="utf-8") != text:
            print(f"❌ {args.check} 与当前配置不一致；重新生成：\n"
                  f"   PYTHONPATH=. /usr/bin/python3 scripts/photo_entry_catalog.py "
                  f"--output {args.check}", file=sys.stderr)
            return 1
        print(f"✅ {args.check} 与当前配置一致")
        return 0
    if args.output is not None:
        args.output.write_text(text, encoding="utf-8")
        print(f"✅ 已写入 {args.output}")
        return 0
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
