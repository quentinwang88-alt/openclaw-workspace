#!/usr/bin/env python3
"""为结构路由测试创建4条测试记录（复制基线记录并清理输出字段）。"""
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

# 基线记录映射
BASELINE_RECORDS = {
    "1736444730937804794": "recvpvtFiX04lC",
    "1734482585843304442": "recvqg6J8C09Ja",
    "1736446411937318906": "recvqg6Nipqp9R",
    "1734257377321977850": "recvpZZUFLxQzj",
}

WIKI_TOKEN = "ZezEwZ7cKiUyeakdlI3cUuU1nRf"
TABLE_ID = "tblHRLMr9b3fvxBw"

# 需要保留的输入字段（飞书实际字段名）
INPUT_FIELD_NAMES = [
    "产品编码", "商品ID", "目标国家", "目标语言",
    "一级类目", "产品类型", "产品卖点说明", "产品参数信息",
    "产品图片", "店铺",
]

# 需要清空的输出字段
OUTPUT_FIELD_NAMES = [
    "Final_S1_JSON", "Final_S2_JSON", "Final_S3_JSON", "Final_S4_JSON",
    "EXP_S1_JSON", "EXP_S2_JSON", "EXP_S3_JSON", "EXP_S4_JSON",
    "脚本_S1_JSON", "脚本_S2_JSON", "脚本_S3_JSON", "脚本_S4_JSON",
    "脚本_S1", "脚本_S2", "脚本_S3", "脚本_S4",
    "脚本_S1_质检_JSON", "脚本_S2_质检_JSON", "脚本_S3_质检_JSON", "脚本_S4_质检_JSON",
    "视频提示词_S1_JSON", "视频提示词_S2_JSON", "视频提示词_S3_JSON", "视频提示词_S4_JSON",
    "视频提示词_S1", "视频提示词_S2", "视频提示词_S3", "视频提示词_S4",
    "锚点卡_JSON", "首镜策略_JSON", "三套策略_JSON",
    "错误信息", "执行日志", "阶段耗时", "输入哈希", "输出摘要",
    "最近执行时间",
]


def get_token(app_id: str, app_secret: str) -> str:
    r = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret},
        timeout=30,
    )
    result = r.json()
    if result.get("code") != 0:
        raise RuntimeError(f"获取 token 失败: {result.get('msg')}")
    return result["tenant_access_token"]


def resolve_app_token(wiki_token: str, token: str) -> str:
    r = requests.get(
        "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
        headers={"Authorization": f"Bearer {token}"},
        params={"token": wiki_token},
        timeout=30,
    )
    result = r.json()
    if result.get("code") != 0:
        raise RuntimeError(f"解析 wiki token 失败: {result.get('msg')}")
    return result["data"]["node"]["obj_token"]


def get_fields(app_token: str, table_id: str, token: str) -> List[Dict]:
    r = requests.get(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields",
        headers={"Authorization": f"Bearer {token}"},
        params={"page_size": 500},
        timeout=30,
    )
    result = r.json()
    if result.get("code") != 0:
        raise RuntimeError(f"获取字段失败: {result.get('msg')}")
    return result.get("data", {}).get("items", [])


def get_record(app_token: str, table_id: str, record_id: str, token: str) -> Dict:
    r = requests.get(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    result = r.json()
    if result.get("code") != 0:
        raise RuntimeError(f"获取记录 {record_id} 失败: {result.get('msg')}")
    return result["data"]["record"]


def create_record(app_token: str, table_id: str, fields: Dict, token: str) -> str:
    r = requests.post(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"fields": fields},
        timeout=30,
    )
    result = r.json()
    if result.get("code") != 0:
        raise RuntimeError(f"创建记录失败: {result.get('msg')}")
    return result["data"]["record"]["record_id"]


def main():
    config_path = Path.home() / ".openclaw" / "openclaw.json"
    with open(config_path) as f:
        config = json.load(f)
    app_id = config["channels"]["feishu"]["appId"]
    app_secret = config["channels"]["feishu"]["appSecret"]

    token = get_token(app_id, app_secret)
    print("✅ 认证成功")

    app_token = resolve_app_token(WIKI_TOKEN, token)
    print(f"✅ app_token: {app_token}")

    fields_def = get_fields(app_token, TABLE_ID, token)
    field_names = [f["field_name"] for f in fields_def]
    print(f"✅ 表格有 {len(field_names)} 个字段")

    results = {}

    for product_code, baseline_id in BASELINE_RECORDS.items():
        print(f"\n--- 处理产品 {product_code} (基线: {baseline_id}) ---")

        record = get_record(app_token, TABLE_ID, baseline_id, token)
        old_fields = record.get("fields", {})

        # 构建新字段：保留输入字段，清空输出字段
        new_fields = {}
        for fn in old_fields:
            if fn in INPUT_FIELD_NAMES:
                new_fields[fn] = old_fields[fn]

        # 设置测试任务编号
        test_task_no = f"结构路由测试_{product_code}_20260724"
        new_fields["任务编号"] = test_task_no

        # 取消勾选生成变体
        new_fields["生成变体"] = False

        # 设置任务状态为非待执行（防止被巡检抓走）
        # 使用一个不会被巡检匹配到的状态
        if "任务状态" in field_names:
            new_fields["任务状态"] = "测试中-暂停"

        print(f"  新任务编号: {test_task_no}")
        print(f"  保留字段: {list(new_fields.keys())}")
        print(f"  清空字段数: {len(OUTPUT_FIELD_NAMES)}")

        try:
            new_record_id = create_record(app_token, TABLE_ID, new_fields, token)
            results[product_code] = {
                "baseline_record_id": baseline_id,
                "test_record_id": new_record_id,
                "task_no": test_task_no,
            }
            print(f"  ✅ 测试记录创建成功: {new_record_id}")
        except Exception as e:
            print(f"  ❌ 创建失败: {e}")
            results[product_code] = {
                "baseline_record_id": baseline_id,
                "test_record_id": None,
                "task_no": test_task_no,
                "error": str(e),
            }

    print("\n" + "=" * 60)
    print("创建结果汇总:")
    print(json.dumps(results, ensure_ascii=False, indent=2))

    manifest_path = Path(__file__).parent / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n✅ 结果已保存到 {manifest_path}")


if __name__ == "__main__":
    main()
