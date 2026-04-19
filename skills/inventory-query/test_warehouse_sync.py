#!/usr/bin/env python3
"""
测试 warehouseIds 自动同步逻辑
"""

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from profile_manager import InventoryProfileManager
from token_manager import TokenManager
from test_profile_manager import make_config, make_fake_token


def main():
    print("\n🧪 warehouseIds 自动同步测试\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        config_dir = Path(tmpdir)
        legacy_path = config_dir / "api_config.json"
        token = make_fake_token("3001", "4001")
        legacy_path.write_text(
            json.dumps(make_config(token, warehouse_ids=[111]), ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

        manager = InventoryProfileManager(config_dir)
        manager.save_active_profile("店铺C")

        token_manager = TokenManager(
            config_path=str(manager.get_profile_path("店铺C")),
            profile="店铺C",
            config_dir=str(config_dir)
        )

        sample_response = {
            "code": 0,
            "data": {
                "page": {
                    "rows": [
                        {"sku": "AX001", "warehouseId": 54952}
                    ]
                },
                "qo": {
                    "warehouseIds": [54952, "54952"]
                }
            }
        }

        extracted = TokenManager.extract_warehouse_ids_from_inventory_response(sample_response)
        assert extracted == [54952]
        print("✅ 能从接口响应中正确提取 warehouseIds")

        token_manager.discover_warehouse_ids = lambda timeout=None: {
            "success": True,
            "warehouse_ids": [54952],
            "sample_sku": "AX001"
        }
        result = token_manager.sync_profile_warehouse_ids("店铺C")
        assert result["success"]
        assert result["changed"]
        assert result["warehouse_ids"] == [54952]

        profile_config = manager.load_profile_config("店铺C")
        assert profile_config["api"]["payload_template"]["warehouseIds"] == [54952]
        print("✅ 自动同步会把探测到的 warehouseIds 写回 profile")

    print("\n🎉 warehouseIds 自动同步测试通过")


if __name__ == "__main__":
    main()
