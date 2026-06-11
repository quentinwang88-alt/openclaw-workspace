#!/usr/bin/env python3
"""
category_router —— 国家×类目 → 配置包 路由(★当前只预留位置)

设计(已与用户确认):
  - 现在只建路由表骨架 + 注册内头巾一行,不实现复杂多路由逻辑。
  - 未来加类目 = 在 CATEGORY_REGISTRY 加一行 + 在 packs/ 放一个配置包目录,
    引擎代码 0 改动。
  - 引擎层不含任何"内头巾"字样;类目专属性全部沉淀在 packs/<pack>/ 配置包里。

数据约定对齐:
  - SQLite 走 base_skill.get_shared_sqlite_path(),统一放 ~/.openclaw/shared/data/
  - 配置包用 YAML,放 packs/<pack_id>/
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple, Any

try:
    import yaml  # PyYAML
except ImportError as e:  # pragma: no cover
    raise ImportError("category_router 需要 PyYAML,请先 pip install pyyaml") from e


ENGINE_DIR = Path(__file__).resolve().parent
SKILL_ROOT = ENGINE_DIR.parent           # script-generator-engine/
PACKS_DIR = SKILL_ROOT / "packs"


# ------------------------------------------------------------------------------
# ★ 路由表 —— 当前只注册内头巾一行(预留位置)
# (country, category) -> pack 目录名(相对 packs/)
# ------------------------------------------------------------------------------
CATEGORY_REGISTRY: Dict[Tuple[str, str], str] = {
    ("MY", "hesturi"): "hesturi_my",   # 内头巾 · 马来 = 第一个配置包
    # 未来扩展示例(现在留空):
    # ("VN", "earrings"): "earrings_vn",
    # ("MY", "womens_tops"): "womens_tops_my",
}


def resolve_pack_dir(country: str, category: str) -> Path:
    """根据 国家×类目 解析配置包目录;未注册则报清晰错误。"""
    key = (country.upper(), category.lower())
    if key not in CATEGORY_REGISTRY:
        registered = ", ".join(f"{c}/{cat}" for c, cat in CATEGORY_REGISTRY)
        raise ValueError(
            f"未注册的 国家×类目: {key}。"
            f"已注册: [{registered}]。"
            f"请先在 packs/ 添加配置包,并在 CATEGORY_REGISTRY 注册一行。"
        )
    pack_dir = PACKS_DIR / CATEGORY_REGISTRY[key]
    if not pack_dir.is_dir():
        raise FileNotFoundError(f"配置包目录不存在: {pack_dir}")
    return pack_dir


def load_pack(country: str, category: str) -> Dict[str, Any]:
    """
    加载指定 国家×类目 的完整配置包,返回合并后的配置 dict。
    结构:
      {
        "pack": <pack.yaml 内容>,
        "pain_hooks": <pain_hooks.yaml>,
        "raw_texture": <raw_texture_lexicon.yaml>,
        "scene_pool": <scene_pool.yaml>,
        "_pack_dir": Path,
      }
    """
    pack_dir = resolve_pack_dir(country, category)
    pack_meta = _load_yaml(pack_dir / "pack.yaml")

    files = pack_meta.get("files", {})
    bundle: Dict[str, Any] = {"pack": pack_meta, "_pack_dir": pack_dir}

    # 按 pack.yaml 的 files 清单加载(缺失的可选文件跳过)
    loaders = {
        "pain_hooks": files.get("pain_hooks", "pain_hooks.yaml"),
        "raw_texture": files.get("raw_texture", "raw_texture_lexicon.yaml"),
        "scene_pool": files.get("scene_pool", "scene_pool.yaml"),
        "prompt_overrides": files.get("prompt_overrides", "prompt_overrides.yaml"),
        "performance_profiles": files.get("performance_profiles", "performance_profiles.yaml"),
    }
    for key, fname in loaders.items():
        fpath = pack_dir / fname
        bundle[key] = _load_yaml(fpath) if fpath.is_file() else None

    return bundle


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"配置文件不存在: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def list_registered() -> Dict[str, str]:
    """列出已注册的 国家×类目(便于调试/扩展)。"""
    return {f"{c}/{cat}": pack for (c, cat), pack in CATEGORY_REGISTRY.items()}


if __name__ == "__main__":
    print("已注册 国家×类目 -> 配置包:")
    for k, v in list_registered().items():
        print(f"  {k}  ->  packs/{v}")
    print("\n加载内头巾配置包自检:")
    b = load_pack("MY", "hesturi")
    hooks = (b.get("pain_hooks") or {}).get("hooks", {})
    scenes = (b.get("scene_pool") or {}).get("scenes", {})
    levels = (b.get("raw_texture") or {}).get("authenticity_levels", {})
    personas = (b.get("performance_profiles") or {}).get("personas", {})
    routing = (b.get("pain_hooks") or {}).get("selling_point_routing", {}).get("rules", [])
    overrides = b.get("prompt_overrides") or {}
    print(f"  痛点钩子: {len(hooks)} 个 -> {list(hooks)}")
    print(f"  卖点路由规则: {len(routing)} 条")
    print(f"  场景: {len(scenes)} 个 -> {list(scenes)}")
    print(f"  人物画像: {len(personas)} 个 -> {list(personas)}")
    print(f"  真实感档: {list(levels)}  默认={b['raw_texture'].get('default_level')}")
    print(f"  prompt 覆盖块: {len([k for k in overrides if k != 'meta'])} 个")
    print("✅ 分层骨架 + 配置包加载正常")
