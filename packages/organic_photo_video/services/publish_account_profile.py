"""真实发布账号的内容定位配置（photo_content_profile）。

内部 ``OPV_*`` 账号是生产资料（人物、素材、市场），真实 TikTok 账号才是
投递目标。本模块把「目标账号 → 内容定位」解析成一个可冻结的小对象：

- 账号管理表（经发布器 ``sync_accounts`` 同步进 ``account_configs``）是运营
  维护的唯一来源；
- ``config/publish_accounts/*.json`` 是仓库内的种子/兜底配置，供测试与账号
  表尚未补列时使用，字段同构；
- 解析结果连同指纹一起冻结进任务快照与 release manifest，续跑只消费冻结值。

生成差异一律由主题/类目/表达/视觉配置驱动；本模块不提供任何按账号名称写
生成分支的入口。
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

PROFILE_SCHEMA_VERSION = "opv-publish-account-profile-v1"

EXPRESSION_STYLE_INSPIRATION = "STYLE_INSPIRATION"
EXPRESSION_PRACTICAL_GUIDE = "PRACTICAL_GUIDE"
#: ``""`` 保持既有输出（造型短名称 + 选择 CTA），旧任务与未配置账号不受影响。
EXPRESSION_MODES = {
    EXPRESSION_STYLE_INSPIRATION: "搭配灵感",
    EXPRESSION_PRACTICAL_GUIDE: "实用指南",
}

CLAIM_SCOPE_STORE_POOL = "store_pool"
CLAIM_SCOPE_OWN_TASKS_ONLY = "own_tasks_only"
CLAIM_SCOPES = (CLAIM_SCOPE_STORE_POOL, CLAIM_SCOPE_OWN_TASKS_ONLY)

# 指纹只覆盖会影响生成/领取行为的字段；账号名称等展示字段变化不算配置变化。
_FINGERPRINT_FIELDS = (
    "default_theme", "positioning", "expression_mode", "visual_baseline",
    "photo_claim_scope", "style_image", "default_visual_preset",
)

_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


class PublishAccountProfileError(RuntimeError):
    """目标账号解析失败；必须暴露给运营，不允许静默回退店铺公共池。"""


def normalize_expression_mode(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for mode, label in EXPRESSION_MODES.items():
        if text in {mode, label}:
            return mode
    compact = text.lower().replace("-", "_").replace(" ", "_")
    if compact in {"style_inspiration", "inspiration"}:
        return EXPRESSION_STYLE_INSPIRATION
    if compact in {"practical_guide", "guide"}:
        return EXPRESSION_PRACTICAL_GUIDE
    raise PublishAccountProfileError(
        f"内容表达只支持：{'、'.join(EXPRESSION_MODES.values())}；收到 {text}"
    )


def normalize_photo_claim_scope(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return CLAIM_SCOPE_STORE_POOL
    compact = text.lower().replace("-", "_").replace(" ", "_")
    if compact in {"store_pool", "store", "店铺池", "沿用店铺池", "店铺公共池"}:
        return CLAIM_SCOPE_STORE_POOL
    if compact in {"own_tasks_only", "own", "仅本账号任务", "仅本账号"}:
        return CLAIM_SCOPE_OWN_TASKS_ONLY
    raise PublishAccountProfileError(
        "图文领取范围只支持：沿用店铺池 / 仅本账号任务；收到 " + text
    )


def normalize_profile_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """把文件/账号表里的松散值收敛成冻结用 profile dict（缺省键保持空值）。"""
    raw = dict(payload or {})
    style_image = raw.get("style_image")
    if isinstance(style_image, Mapping):
        style_image = {
            "file_token": str(style_image.get("file_token") or ""),
            "name": str(style_image.get("name") or ""),
        }
        style_image = {k: v for k, v in style_image.items() if v} or None
    elif style_image in (None, "", []):
        style_image = None
    else:
        raise PublishAccountProfileError("风格图片必须是附件对象（file_token/name）")
    profile = {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "default_theme": str(raw.get("default_theme") or "").strip(),
        "positioning": str(raw.get("positioning") or "").strip(),
        "expression_mode": normalize_expression_mode(raw.get("expression_mode")),
        "visual_baseline": str(raw.get("visual_baseline") or "").strip(),
        "photo_claim_scope": normalize_photo_claim_scope(raw.get("photo_claim_scope")),
        "default_visual_preset": str(raw.get("default_visual_preset") or "").strip(),
        "style_image": style_image,
    }
    return profile


def profile_fingerprint(profile: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        {key: (dict(profile.get(key) or {}) if isinstance(profile.get(key), Mapping)
               else profile.get(key))
         for key in _FINGERPRINT_FIELDS},
        ensure_ascii=False, sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class PublishAccountBinding:
    account_id: str
    account_name: str
    store_id: str
    target_country: str
    profile: Dict[str, Any] = field(default_factory=dict)
    source: str = ""

    @property
    def fingerprint(self) -> str:
        return profile_fingerprint(self.profile)

    @property
    def claim_scope(self) -> str:
        return str(self.profile.get("photo_claim_scope") or CLAIM_SCOPE_STORE_POOL)

    @property
    def target_publish_account_id(self) -> str:
        return self.account_id


class PublishAccountProfileResolver:
    """目标账号 → 绑定信息。账号表同步值优先，仓库种子配置兜底。"""

    def __init__(
        self,
        *,
        publisher_db: Any = None,
        profiles: Optional[Mapping[str, Mapping[str, Any]]] = None,
        config_dir: Optional[Path] = None,
    ) -> None:
        self._publisher_db = publisher_db
        self._explicit_profiles = {
            str(key): normalize_profile_payload(value)
            for key, value in dict(profiles or {}).items()
        }
        self._config_dir = config_dir

    # ------------------------------------------------------------------
    def resolve(self, account_id: str) -> PublishAccountBinding:
        handle = str(account_id or "").strip()
        if not handle:
            raise PublishAccountProfileError("目标账号为空")
        errors: List[str] = []
        row = self._publisher_account_row(handle)
        if row is not None:
            return self._binding_from_publisher_row(handle, row)
        errors.append("发布器账号配置中不存在该账号")
        file_payload = self._file_profile(handle)
        if file_payload is not None:
            return self._binding_from_file(handle, file_payload)
        errors.append("仓库 config/publish_accounts 中也没有该账号的种子配置")
        raise PublishAccountProfileError(
            f"目标账号 {handle} 无法解析：{'；'.join(errors)}。"
            "请先在账号管理表补齐该账号并运行账号同步，或修正任务表中的目标账号"
        )

    def options(self) -> List[str]:
        """任务表「目标账号」可选项（账号表 ∪ 仓库种子配置）。"""
        values: List[str] = []
        for handle in self._publisher_account_ids():
            if handle and handle not in values:
                values.append(handle)
        for handle in self._file_profiles():
            if handle and handle not in values:
                values.append(handle)
        return sorted(values)

    # ------------------------------------------------------------------
    def _binding_from_publisher_row(self, handle: str, row: Mapping[str, Any]) -> PublishAccountBinding:
        raw_profile = {}
        text = str(row["photo_content_profile_json"] or ""
                   ) if "photo_content_profile_json" in row.keys() else ""
        if text.strip():
            try:
                loaded = json.loads(text)
            except json.JSONDecodeError as exc:
                raise PublishAccountProfileError(
                    f"目标账号 {handle} 的 photo_content_profile 不是有效 JSON：{exc}"
                ) from exc
            if not isinstance(loaded, Mapping):
                raise PublishAccountProfileError(
                    f"目标账号 {handle} 的 photo_content_profile 必须是对象")
            raw_profile = dict(loaded)
        # 仓库种子只补缺省键，不覆盖账号表已维护的值（账号表是唯一来源）。
        file_raw = self._file_profile(handle) or {}
        file_profile = (
            dict(file_raw.get("profile") or {})
            if isinstance(file_raw.get("profile"), Mapping) else dict(file_raw))
        merged = {**{k: v for k, v in file_profile.items() if v not in ("", None)},
                  **{k: v for k, v in raw_profile.items() if v not in ("", None)}}
        profile = normalize_profile_payload(merged)
        store_id = str(row["store_id"] or "").strip()
        if not store_id:
            raise PublishAccountProfileError(f"目标账号 {handle} 缺少店铺绑定")
        return PublishAccountBinding(
            account_id=handle,
            account_name=str(row["account_name"] or handle),
            store_id=store_id,
            target_country=_country_for_store(store_id, {**file_raw, **raw_profile}),
            profile=profile,
            source="publisher_db",
        )

    def _binding_from_file(self, handle: str, payload: Mapping[str, Any]) -> PublishAccountBinding:
        payload = dict(payload)
        store_id = str(payload.get("store_id") or "").strip()
        if not store_id:
            raise PublishAccountProfileError(f"目标账号 {handle} 种子配置缺少 store_id")
        profile = normalize_profile_payload(payload.get("profile") or payload)
        return PublishAccountBinding(
            account_id=handle,
            account_name=str(payload.get("account_name") or handle),
            store_id=store_id,
            target_country=str(payload.get("target_country") or
                               _country_for_store(store_id, payload)),
            profile=profile,
            source="file",
        )

    def _publisher_account_row(self, handle: str) -> Optional[Any]:
        db = self._publisher_db
        getter = getattr(db, "get_account_config", None)
        if not callable(getter):
            return None
        try:
            return getter(handle)
        except Exception:  # 发布器库不可用/未初始化：留给文件配置兜底
            return None

    def _publisher_account_ids(self) -> List[str]:
        db = self._publisher_db
        lister = getattr(db, "list_account_configs", None)
        if not callable(lister):
            return []
        try:
            rows = lister() or []
        except Exception:
            return []
        return [str(row["account_id"] or "") for row in rows]

    def _file_profiles(self) -> Dict[str, Dict[str, Any]]:
        payloads: Dict[str, Dict[str, Any]] = {}
        directory = self._config_dir or _default_config_dir()
        if not directory.is_dir():
            return payloads
        for path in sorted(directory.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, Mapping):
                continue
            handle = str(payload.get("account_id") or path.stem).strip()
            if handle:
                payloads[handle] = dict(payload)
        return payloads

    def _file_profile(self, handle: str) -> Optional[Dict[str, Any]]:
        if handle in self._explicit_profiles:
            return {"account_id": handle, "profile": self._explicit_profiles[handle]}
        return self._file_profiles().get(handle)


def _default_config_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "config" / "publish_accounts"


def _country_for_store(store_id: str, payload: Mapping[str, Any]) -> str:
    explicit = str(payload.get("target_country") or "").strip().upper()
    if explicit:
        return explicit
    routes_path = Path(__file__).resolve().parents[1] / "config" / "main_publish_routes.json"
    try:
        routes = json.loads(routes_path.read_text(encoding="utf-8")).get("routes") or {}
    except (OSError, json.JSONDecodeError):
        routes = {}
    for country, route in routes.items():
        if str((route or {}).get("default_store_id") or "") == store_id:
            return str(country).upper()
    return ""


def build_default_resolver() -> PublishAccountProfileResolver:
    """生产默认解析器：连接发布器 SQLite（复用主队列桥的路径规则）。"""
    import os
    import sys

    package_root = Path(__file__).resolve().parents[1]
    publisher_root = package_root.parents[1] / "skills" / "short-video-auto-publisher"
    db = None
    if publisher_root.is_dir():
        if str(publisher_root) not in sys.path:
            sys.path.insert(0, str(publisher_root))
        try:
            from app.db import AutoPublishDB  # noqa: E402

            path = Path(
                os.environ.get("SHORT_VIDEO_AUTO_PUBLISH_DB_PATH")
                or (Path(os.environ.get(
                    "OPENCLAW_SHARED_DATA_DIR",
                    str(Path.home() / ".openclaw/shared/data"),
                )) / "short_video_auto_publish.sqlite3")
            )
            db = AutoPublishDB(path) if path.exists() else None
        except Exception:
            db = None
    return PublishAccountProfileResolver(publisher_db=db)
