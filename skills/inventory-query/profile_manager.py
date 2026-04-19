#!/usr/bin/env python3
"""
BigSeller 多账号 Profile 管理器

职责：
1. 保留 config/api_config.json 作为当前激活账号的运行配置
2. 在 config/profiles/ 下维护多个命名账号配置
3. 通过 profile_state.json 记录当前激活账号
4. 基于 muc_token 中的 uid/puid 自动匹配已登记账号
"""

import base64
import json
import re
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


PROFILE_METADATA_KEY = "_profile"
DEFAULT_PROFILE_NAME = "default"


class InventoryProfileError(RuntimeError):
    """Profile 管理相关错误。"""


class InventoryProfileManager:
    """库存查询多账号配置管理器。"""

    def __init__(self, config_dir: Optional[str] = None):
        if config_dir is None:
            config_dir = Path(__file__).parent / "config"

        self.config_dir = Path(config_dir)
        self.legacy_config_path = self.config_dir / "api_config.json"
        self.profile_state_path = self.config_dir / "profile_state.json"
        self.profiles_dir = self.config_dir / "profiles"
        self.profiles_dir.mkdir(parents=True, exist_ok=True)
        self._bootstrap()

    @staticmethod
    def normalize_profile_name(name: str) -> str:
        """规范化 profile 名称，同时保留中文店铺名。"""
        normalized = (name or "").strip()
        normalized = re.sub(r'[<>:"/\\|?*]+', "-", normalized)
        normalized = re.sub(r"\s+", "-", normalized)
        normalized = normalized.strip(".-")

        if not normalized:
            raise InventoryProfileError("profile 名称不能为空")

        return normalized

    @staticmethod
    def parse_cookie_string(cookie_string: str) -> Dict[str, str]:
        """解析 Cookie 字符串。"""
        cookies = {}

        if not cookie_string:
            return cookies

        for raw_part in cookie_string.split(";"):
            part = raw_part.strip()
            if not part or "=" not in part:
                continue

            name, value = part.split("=", 1)
            name = name.strip()
            value = value.strip()
            if name and value:
                cookies[name] = value

        return cookies

    @staticmethod
    def normalize_warehouse_ids(warehouse_ids) -> List[int]:
        """标准化 warehouseIds，去重并过滤无效值。"""
        normalized = []
        seen = set()

        for item in warehouse_ids or []:
            try:
                value = int(item)
            except (TypeError, ValueError):
                continue

            if value in seen:
                continue

            seen.add(value)
            normalized.append(value)

        return normalized

    @staticmethod
    def decode_jwt_token(token: str) -> Optional[Dict]:
        """解码 JWT payload。"""
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return None

            payload = parts[1]
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += "=" * padding

            decoded = base64.urlsafe_b64decode(payload)
            return json.loads(decoded)
        except Exception:
            return None

    def _bootstrap(self):
        """将旧的单配置模式迁移到 profile 模式。"""
        state = self._read_state()
        existing_profiles = self.list_profile_names()
        active_profile = state.get("active_profile")

        if not active_profile:
            if self.legacy_config_path.exists():
                active_profile = DEFAULT_PROFILE_NAME
                if not self.profile_exists(active_profile):
                    config = self._read_json(self.legacy_config_path)
                    self._write_profile_config(active_profile, config)
                self._write_state({"active_profile": active_profile})
            elif existing_profiles:
                active_profile = existing_profiles[0]
                self._write_state({"active_profile": active_profile})
                self._sync_legacy_from_profile(active_profile)
            else:
                return

        if active_profile and not self.profile_exists(active_profile):
            if self.legacy_config_path.exists():
                config = self._read_json(self.legacy_config_path)
                self._write_profile_config(active_profile, config)
            elif existing_profiles:
                fallback = existing_profiles[0]
                self._write_state({"active_profile": fallback})
                self._sync_legacy_from_profile(fallback)
                active_profile = fallback
            else:
                return

        if active_profile and not self.legacy_config_path.exists():
            self._sync_legacy_from_profile(active_profile)

    def _read_json(self, path: Path) -> dict:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write_json(self, path: Path, data: dict):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _read_state(self) -> dict:
        if not self.profile_state_path.exists():
            return {}
        return self._read_json(self.profile_state_path)

    def _write_state(self, state: dict):
        state = deepcopy(state)
        state["updated_at"] = datetime.now().isoformat()
        self._write_json(self.profile_state_path, state)

    def _strip_profile_metadata(self, config: dict) -> dict:
        clean_config = deepcopy(config)
        clean_config.pop(PROFILE_METADATA_KEY, None)
        return clean_config

    def _extract_identity_from_payload(self, payload: dict) -> Dict[str, Optional[str]]:
        identity = {
            "account_uid": None,
            "account_puid": None,
            "request_id": None,
        }

        if not payload:
            return identity

        info_raw = payload.get("info")
        if isinstance(info_raw, str):
            try:
                info = json.loads(info_raw)
            except Exception:
                info = {}
        else:
            info = {}

        identity["account_uid"] = str(info.get("uid")) if info.get("uid") is not None else None
        identity["account_puid"] = str(info.get("puid")) if info.get("puid") is not None else None
        identity["request_id"] = info.get("requestId")
        return identity

    def extract_identity_from_auth(
        self,
        token: Optional[str] = None,
        cookie_string: Optional[str] = None
    ) -> Dict[str, Optional[str]]:
        """从 token 或完整 Cookie 提取账号身份。"""
        if not token and cookie_string:
            cookies = self.parse_cookie_string(cookie_string)
            token = cookies.get("muc_token")

        payload = self.decode_jwt_token(token) if token else None
        return self._extract_identity_from_payload(payload or {})

    def extract_identity_from_config(self, config: dict) -> Dict[str, Optional[str]]:
        """从配置文件中提取账号身份。"""
        cookie = (
            config.get("api", {})
            .get("headers", {})
            .get("cookie", "")
        )
        return self.extract_identity_from_auth(cookie_string=cookie)

    def _build_profile_metadata(self, name: str, config: dict, existing_meta: Optional[dict] = None) -> dict:
        existing_meta = deepcopy(existing_meta or {})
        clean_config = self._strip_profile_metadata(config)
        identity = self.extract_identity_from_config(clean_config)
        warehouse_ids = self.normalize_warehouse_ids(
            clean_config.get("api", {})
            .get("payload_template", {})
            .get("warehouseIds", [])
        )

        metadata = {
            "name": name,
            "display_name": existing_meta.get("display_name", name),
            "updated_at": datetime.now().isoformat(),
            "default_country": clean_config.get("default_country"),
            "warehouse_ids": warehouse_ids,
            "account_uid": identity.get("account_uid"),
            "account_puid": identity.get("account_puid"),
            "request_id": identity.get("request_id"),
        }

        if existing_meta.get("notes"):
            metadata["notes"] = existing_meta["notes"]

        return metadata

    def clear_auth_fields(self, config: dict) -> dict:
        """克隆新账号 profile 时清空认证信息，避免误沿用旧账号 Cookie。"""
        clean_config = self._strip_profile_metadata(config)
        headers = clean_config.setdefault("api", {}).setdefault("headers", {})

        for key in list(headers.keys()):
            if key.lower() in {"cookie", "authorization"}:
                headers[key] = ""

        return clean_config

    def _write_profile_config(self, name: str, config: dict):
        clean_name = self.normalize_profile_name(name)
        profile_path = self.get_profile_path(clean_name)
        existing_meta = {}
        if profile_path.exists():
            existing_meta = self._read_json(profile_path).get(PROFILE_METADATA_KEY, {})

        clean_config = self._strip_profile_metadata(config)
        profile_config = deepcopy(clean_config)
        profile_config[PROFILE_METADATA_KEY] = self._build_profile_metadata(
            clean_name,
            clean_config,
            existing_meta=existing_meta
        )
        self._write_json(profile_path, profile_config)

    def _sync_legacy_from_profile(self, name: str):
        clean_name = self.normalize_profile_name(name)
        config = self.load_profile_config(clean_name)
        self._write_json(self.legacy_config_path, config)

    def list_profile_names(self) -> List[str]:
        return sorted(
            path.stem
            for path in self.profiles_dir.glob("*.json")
            if ".backup." not in path.name
        )

    def list_profiles(self) -> List[dict]:
        profiles = []
        active = self.get_active_profile_name()
        for name in self.list_profile_names():
            raw_config = self._read_json(self.get_profile_path(name))
            metadata = raw_config.get(PROFILE_METADATA_KEY, {})
            profiles.append({
                "name": name,
                "active": name == active,
                "metadata": metadata,
            })
        return profiles

    def profile_exists(self, name: str) -> bool:
        return self.get_profile_path(name).exists()

    def get_profile_path(self, name: str) -> Path:
        clean_name = self.normalize_profile_name(name)
        return self.profiles_dir / f"{clean_name}.json"

    def get_active_profile_name(self) -> Optional[str]:
        return self._read_state().get("active_profile")

    def get_config_path(self, profile: Optional[str] = None) -> Path:
        """默认返回 legacy 运行配置；显式传 profile 时返回对应 profile。"""
        if profile:
            profile_path = self.get_profile_path(profile)
            if not profile_path.exists():
                raise FileNotFoundError(f"profile 不存在: {profile}")
            return profile_path
        return self.legacy_config_path

    def load_profile_config(self, name: str) -> dict:
        path = self.get_profile_path(name)
        if not path.exists():
            raise FileNotFoundError(f"profile 不存在: {name}")
        return self._strip_profile_metadata(self._read_json(path))

    def load_active_config(self) -> dict:
        if not self.legacy_config_path.exists():
            raise FileNotFoundError(
                f"当前激活配置不存在: {self.legacy_config_path}\n"
                "请先创建或切换一个 profile"
            )
        return self._read_json(self.legacy_config_path)

    def create_profile(self, name: str, from_profile: Optional[str] = None, activate: bool = False) -> str:
        clean_name = self.normalize_profile_name(name)
        if self.profile_exists(clean_name):
            raise InventoryProfileError(f"profile 已存在: {clean_name}")

        if from_profile:
            source_config = self.load_profile_config(from_profile)
        else:
            source_config = self.load_active_config()

        source_config = self.clear_auth_fields(source_config)
        self._write_profile_config(clean_name, source_config)
        if activate:
            self.activate_profile(clean_name)
        return clean_name

    def save_profile(self, name: str, config: dict, activate: bool = False) -> str:
        clean_name = self.normalize_profile_name(name)
        self._write_profile_config(clean_name, config)
        if activate:
            self._write_state({"active_profile": clean_name})
            self._write_json(self.legacy_config_path, self._strip_profile_metadata(config))
        return clean_name

    def set_profile_warehouse_ids(self, name: str, warehouse_ids) -> List[int]:
        """更新指定 profile 的 warehouseIds；如果它是当前激活账号，同时刷新运行配置。"""
        clean_name = self.normalize_profile_name(name)
        config = self.load_profile_config(clean_name)
        normalized_ids = self.normalize_warehouse_ids(warehouse_ids)
        config.setdefault("api", {}).setdefault("payload_template", {})["warehouseIds"] = normalized_ids
        is_active = self.get_active_profile_name() == clean_name
        self.save_profile(clean_name, config, activate=is_active)
        return normalized_ids

    def save_active_profile(self, name: str) -> str:
        """将当前激活配置命名保存为一个 profile，并切换为该 profile。"""
        clean_name = self.normalize_profile_name(name)
        current_config = self.load_active_config()
        active_before = self.get_active_profile_name()

        self.save_profile(clean_name, current_config, activate=True)

        if (
            active_before == DEFAULT_PROFILE_NAME
            and clean_name != DEFAULT_PROFILE_NAME
            and self.profile_exists(DEFAULT_PROFILE_NAME)
        ):
            default_path = self.get_profile_path(DEFAULT_PROFILE_NAME)
            try:
                default_path.unlink()
            except OSError:
                pass

        return clean_name

    def activate_profile(self, name: str) -> str:
        clean_name = self.normalize_profile_name(name)
        if not self.profile_exists(clean_name):
            raise FileNotFoundError(f"profile 不存在: {clean_name}")

        self._sync_legacy_from_profile(clean_name)
        self._write_state({"active_profile": clean_name})
        return clean_name

    def sync_active_profile_from_legacy(self):
        active = self.get_active_profile_name()
        if not active or not self.legacy_config_path.exists():
            return
        self._write_profile_config(active, self.load_active_config())

    def get_profile_identity(self, name: str) -> Dict[str, Optional[str]]:
        path = self.get_profile_path(name)
        if not path.exists():
            return {}
        raw_config = self._read_json(path)
        metadata = raw_config.get(PROFILE_METADATA_KEY, {})
        return {
            "account_uid": metadata.get("account_uid"),
            "account_puid": metadata.get("account_puid"),
            "request_id": metadata.get("request_id"),
        }

    @staticmethod
    def identities_differ(left: Dict[str, Optional[str]], right: Dict[str, Optional[str]]) -> bool:
        keys = ("account_uid", "account_puid")
        shared = [key for key in keys if left.get(key) and right.get(key)]
        if not shared:
            return False
        return any(left.get(key) != right.get(key) for key in shared)

    def match_profile_for_auth(
        self,
        token: Optional[str] = None,
        cookie_string: Optional[str] = None
    ) -> Optional[str]:
        """根据 uid/puid 自动匹配已登记 profile。"""
        incoming = self.extract_identity_from_auth(token=token, cookie_string=cookie_string)
        if not incoming.get("account_uid") and not incoming.get("account_puid"):
            return None

        best_match = None
        best_score = 0
        ambiguous = False

        for profile in self.list_profiles():
            metadata = profile.get("metadata", {})
            score = 0

            if incoming.get("account_uid") and metadata.get("account_uid") == incoming.get("account_uid"):
                score += 1
            if incoming.get("account_puid") and metadata.get("account_puid") == incoming.get("account_puid"):
                score += 1

            if score > best_score:
                best_match = profile["name"]
                best_score = score
                ambiguous = False
            elif score and score == best_score:
                ambiguous = True

        if ambiguous:
            return None

        return best_match if best_score > 0 else None


def _print_profile_summary(manager: InventoryProfileManager):
    active = manager.get_active_profile_name()
    print(f"当前激活 profile: {active or '未设置'}")

    profiles = manager.list_profiles()
    if not profiles:
        print("暂无已保存 profile")
        return

    print("\n已保存 profile:")
    for profile in profiles:
        metadata = profile["metadata"]
        marker = "*" if profile["active"] else " "
        warehouse_ids = metadata.get("warehouse_ids") or []
        warehouse_text = ",".join(str(item) for item in warehouse_ids) if warehouse_ids else "-"
        print(
            f"{marker} {profile['name']}  "
            f"uid={metadata.get('account_uid') or '-'}  "
            f"puid={metadata.get('account_puid') or '-'}  "
            f"country={metadata.get('default_country') or '-'}  "
            f"warehouseIds={warehouse_text}"
        )


def main():
    import sys

    manager = InventoryProfileManager()
    argv = sys.argv[1:]

    if not argv:
        print("BigSeller Profile 管理工具")
        print("\n用法:")
        print("  python profile_manager.py status")
        print("  python profile_manager.py list")
        print("  python profile_manager.py save-active <profile_name>")
        print("  python profile_manager.py create <profile_name> [--activate]")
        print("  python profile_manager.py switch <profile_name>")
        sys.exit(1)

    command = argv[0].lower()

    try:
        if command in {"status", "list"}:
            _print_profile_summary(manager)
            return

        if command == "save-active":
            if len(argv) < 2:
                raise InventoryProfileError("请提供 profile_name")
            name = manager.save_active_profile(argv[1])
            print(f"✅ 当前激活配置已保存为 profile: {name}")
            _print_profile_summary(manager)
            return

        if command == "create":
            if len(argv) < 2:
                raise InventoryProfileError("请提供 profile_name")
            activate = "--activate" in argv[2:]
            name = manager.create_profile(argv[1], activate=activate)
            print(f"✅ 已创建 profile 模板: {name}")
            print("   已自动清空旧账号认证信息，等待你切到对应 BigSeller 账号后重新同步")
            if activate:
                print(f"✅ 已切换到 profile: {name}")
            _print_profile_summary(manager)
            return

        if command == "switch":
            if len(argv) < 2:
                raise InventoryProfileError("请提供 profile_name")
            name = manager.activate_profile(argv[1])
            print(f"✅ 已切换到 profile: {name}")
            _print_profile_summary(manager)
            return

        raise InventoryProfileError(f"未知命令: {command}")

    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
