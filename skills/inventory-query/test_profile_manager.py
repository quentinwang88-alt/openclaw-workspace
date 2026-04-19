#!/usr/bin/env python3
"""
测试多账号 Profile 管理功能
"""

import base64
import json
import tempfile
from pathlib import Path

from profile_manager import InventoryProfileManager


def make_fake_token(uid: str, puid: str) -> str:
    """构造一个可解析的 JWT 风格 token。"""
    header = {"typ": "JWT", "alg": "HS256"}
    payload = {
        "sub": "user",
        "exp": 1893456000,
        "iat": 1890864000,
        "info": json.dumps({
            "uid": uid,
            "puid": puid,
            "requestId": f"req-{uid}-{puid}"
        }, ensure_ascii=False)
    }

    def encode(obj: dict) -> str:
        raw = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

    return f"{encode(header)}.{encode(payload)}.signature"


def make_config(token: str, warehouse_ids=None, country: str = "thailand") -> dict:
    warehouse_ids = warehouse_ids or [54952]
    return {
        "default_country": country,
        "api": {
            "headers": {
                "cookie": (
                    f"JSESSIONID=SESSION-{warehouse_ids[0]}; "
                    f"muc_token={token}; "
                    "language=zh_CN;"
                )
            },
            "payload_template": {
                "warehouseIds": warehouse_ids
            }
        }
    }


def main():
    print("\n🧪 Profile 管理功能测试\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        config_dir = Path(tmpdir)
        legacy_path = config_dir / "api_config.json"

        token_a = make_fake_token("1001", "2001")
        token_b = make_fake_token("1002", "2002")
        legacy_path.write_text(
            json.dumps(make_config(token_a, warehouse_ids=[111]), ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

        manager = InventoryProfileManager(config_dir)

        print("测试 1: 启动时迁移 legacy 配置")
        assert manager.get_active_profile_name() == "default"
        assert manager.profile_exists("default")
        print("✅ 已自动创建 default profile")

        print("\n测试 2: 保存当前账号为命名 profile")
        saved_name = manager.save_active_profile("店铺A")
        assert saved_name == "店铺A"
        assert manager.get_active_profile_name() == "店铺A"
        assert manager.profile_exists("店铺A")
        print("✅ 已保存并切换到 店铺A")

        print("\n测试 3: 创建第二个 profile 并保持独立配置")
        created_name = manager.create_profile("店铺B")
        assert created_name == "店铺B"
        manager.save_profile("店铺B", make_config(token_b, warehouse_ids=[222]), activate=False)
        assert manager.profile_exists("店铺B")
        print("✅ 已创建 店铺B，并写入独立 warehouseIds / token")

        print("\n测试 4: 基于 token 自动匹配已登记 profile")
        matched = manager.match_profile_for_auth(token=token_b)
        assert matched == "店铺B"
        print("✅ token 能正确匹配到 店铺B")

        print("\n测试 5: 切换 profile 时同步 legacy 运行配置")
        manager.activate_profile("店铺B")
        current_config = json.loads(legacy_path.read_text(encoding="utf-8"))
        cookie = current_config["api"]["headers"]["cookie"]
        assert "muc_token=" in cookie and token_b in cookie
        assert current_config["api"]["payload_template"]["warehouseIds"] == [222]
        print("✅ 切换后 legacy api_config.json 已同步为 店铺B")

        print("\n测试 6: 更新 warehouseIds 时同步 metadata 和运行配置")
        updated_ids = manager.set_profile_warehouse_ids("店铺B", [333, "333", 444, None])
        assert updated_ids == [333, 444]
        current_config = json.loads(legacy_path.read_text(encoding="utf-8"))
        assert current_config["api"]["payload_template"]["warehouseIds"] == [333, 444]
        listed_names = manager.list_profile_names()
        assert "店铺B" in listed_names
        print("✅ warehouseIds 更新后，profile metadata 与 legacy 配置保持一致")

    print("\n🎉 Profile 管理功能测试通过")


if __name__ == "__main__":
    main()
