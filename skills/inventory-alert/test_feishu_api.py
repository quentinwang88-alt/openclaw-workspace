#!/usr/bin/env python3
"""测试飞书多维表格 API 连接"""

import json
from pathlib import Path
from feishu_api import FeishuBitableAPI

def test_feishu_api():
    """测试飞书 API 连接和基本功能"""
    
    # 加载配置
    config_path = Path(__file__).parent / "config" / "alert_config.json"
    
    if not config_path.exists():
        print(f"✗ 配置文件不存在: {config_path}")
        print("  请复制 config/alert_config.example.json 并填写配置")
        return False
    
    with open(config_path) as f:
        config = json.load(f)
    
    feishu_config = config.get("feishu", {})
    app_id = feishu_config.get("app_id")
    app_secret = feishu_config.get("app_secret")
    
    if not app_id or not app_secret:
        print("✗ 配置文件中缺少 feishu.app_id 或 feishu.app_secret")
        return False
    
    print("飞书多维表格 API 测试")
    print("=" * 50)
    print()
    
    # 测试 1: 获取 access_token
    print("测试 1: 获取 access_token")
    try:
        api = FeishuBitableAPI(app_id, app_secret)
        token = api.get_access_token()
        print(f"✓ 成功获取 access_token: {token[:20]}...")
        print()
    except Exception as e:
        print(f"✗ 获取 access_token 失败: {e}")
        print()
        return False
    
    # 测试 2: 列出记录（如果配置了 bitable 信息）
    bitable_config = feishu_config.get("bitable", {})
    app_token = bitable_config.get("app_token")
    table_id = bitable_config.get("table_id")
    
    if app_token and table_id:
        print("测试 2: 列出多维表格记录")
        print(f"  app_token: {app_token}")
        print(f"  table_id: {table_id}")
        try:
            data = api.list_records(app_token, table_id, page_size=5)
            items = data.get("items", [])
            print(f"✓ 成功获取记录，共 {len(items)} 条（前 5 条）")
            
            if items:
                print("\n  示例记录:")
                for i, item in enumerate(items[:2], 1):
                    record_id = item.get("record_id")
                    fields = item.get("fields", {})
                    print(f"    {i}. record_id: {record_id}")
                    print(f"       字段: {list(fields.keys())}")
            print()
        except Exception as e:
            print(f"✗ 列出记录失败: {e}")
            print()
    else:
        print("测试 2: 跳过（未配置 bitable.app_token 和 bitable.table_id）")
        print()
    
    # 测试 3: 创建测试记录（可选）
    if app_token and table_id:
        print("测试 3: 创建测试记录")
        test_fields = {
            "SKU编码": "TEST001",
            "SKU名称": "测试商品",
            "当前库存": 100,
            "日均销量": 10.5,
            "预计可售天数": 9,
            "建议采购数量": 63,
            "紧急程度": "⏰ 库存预警"
        }
        
        try:
            result = api.create_record(app_token, table_id, test_fields)
            record_id = result.get("record", {}).get("record_id")
            print(f"✓ 成功创建测试记录: {record_id}")
            print()
            
            # 删除测试记录
            print("测试 4: 删除测试记录")
            try:
                api.delete_record(app_token, table_id, record_id)
                print(f"✓ 成功删除测试记录: {record_id}")
                print()
            except Exception as e:
                print(f"✗ 删除测试记录失败: {e}")
                print(f"  请手动删除记录: {record_id}")
                print()
        except Exception as e:
            print(f"✗ 创建测试记录失败: {e}")
            print()
            print("  可能的原因:")
            print("  1. 字段名称不匹配（请检查多维表格的字段名）")
            print("  2. 应用权限不足（需要 bitable:record 权限）")
            print("  3. app_token 或 table_id 不正确")
            print()
    
    print("=" * 50)
    print("测试完成！")
    print()
    
    if app_token and table_id:
        print(f"查看表格: https://gcngopvfvo0q.feishu.cn/base/{app_token}")
    
    return True


if __name__ == "__main__":
    test_feishu_api()
