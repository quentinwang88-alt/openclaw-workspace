#!/usr/bin/env python3
"""对备选商品进行风格打标 - 分批处理版"""

import sys
import json
import requests
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).parent))

from core.feishu import FeishuBitableClient, parse_feishu_bitable_url, resolve_wiki_bitable_app_token
import config as cfg

FEISHU_URL = cfg.DEFAULT_FEISHU_URL
FIELD_STYLE = "产品风格"
FIELD_RECOMMEND = "是否推荐"
FIELD_REASON = "详细原因"

LLM_BASE_URL = cfg.DEFAULT_LLM_BASE_URL
LLM_API_KEY = cfg.DEFAULT_LLM_API_KEY
LLM_MODEL = cfg.DEFAULT_LLM_MODEL

STYLE_PROMPT = """你是一个专业的发饰风格分析师。请分析以下发饰商品的风格，并给出推荐意见。

商品信息:
- 商品名称: {product_name}
- 中文名称: {chinese_name}
- 子类目: {subcategory}
- 售价: {price}

请从以下风格中选择最匹配的一项:
1. 简约通勤风 - 低调、百搭、适合日常通勤
2. 甜美少女风 - 可爱、粉嫩、适合年轻女性
3. 优雅轻奢风 - 精致、高品质、适合正式场合
4. 复古文艺风 - 复古元素、文艺气息
5. 潮流个性风 - 时尚前卫、个性鲜明
6. 儿童幼态风 - 卡通、童趣、适合儿童或幼态审美
7. 运动休闲风 - 运动元素、休闲舒适
8. 其它

请按以下JSON格式输出:
{{"产品风格": "风格名称", "是否推荐": "推荐/不推荐", "详细原因": "说明理由"}}

注意：重点目标风格是"简约通勤风"、"甜美少女风"、"优雅轻奢风"，这些风格推荐纳入备选池；"儿童幼态风"一般不建议纳入发饰备选池。"""

def is_backup(fields: Dict) -> bool:
    v = fields.get("是否列入备选")
    return v == True or v == "true" or v == "是"

def needs_style(fields: Dict) -> bool:
    s = fields.get(FIELD_STYLE, "")
    # 需要重新打标的情况：空值、未标记、未明确描述
    return not s or s == "未标记" or "未明确" in s or "不足以判断" in s

def call_llm(prompt: str) -> str:
    headers = {"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type": "application/json"}
    data = {"model": LLM_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0.3}
    resp = requests.post(f"{LLM_BASE_URL}/chat/completions", headers=headers, json=data, timeout=60)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]

def parse_response(text: str) -> Dict[str, str]:
    try:
        text = text.strip()
        if text.startswith("```"):
            text = text[text.find("{"):text.rfind("}")+1]
        data = json.loads(text)
        return {
            FIELD_STYLE: data.get("产品风格", ""),
            FIELD_RECOMMEND: data.get("是否推荐", ""),
            FIELD_REASON: data.get("详细原因", "")
        }
    except Exception as e:
        return {}

def process_batch(client: FeishuBitableClient, records: List[Dict], batch_num: int, total_batches: int):
    """处理一批记录"""
    print(f"\n📦 批次 {batch_num}/{total_batches} ({len(records)} 条)")
    updated = 0
    errors = 0

    for i, item in enumerate(records, 1):
        name = item['chinese_name'] or item['product_name'][:30]
        print(f"  [{i}/{len(records)}] {name}...", end=" ")

        prompt = STYLE_PROMPT.format(
            product_name=item["product_name"],
            chinese_name=item["chinese_name"],
            subcategory=item["subcategory"],
            price=item["price"]
        )

        try:
            response = call_llm(prompt)
            result = parse_response(response)

            if result.get(FIELD_STYLE):
                client.update_record_fields(
                    record_id=item["record_id"],
                    fields=result
                )
                print(f"✅ {result.get(FIELD_STYLE)}")
                updated += 1
            else:
                print(f"⚠️ 无结果")
                errors += 1
        except Exception as e:
            print(f"❌ {e}")
            errors += 1

    return updated, errors

def main():
    if not LLM_API_KEY:
        print("❌ 错误: 未配置 LLM API Key")
        return

    # 初始化
    info = parse_feishu_bitable_url(FEISHU_URL)
    app_token = resolve_wiki_bitable_app_token(info.app_token) if info.is_wiki else info.app_token
    client = FeishuBitableClient(app_token=app_token, table_id=info.table_id)

    # 读取记录
    print("📖 读取表格记录...")
    records = client.list_records(limit=None)
    print(f"✅ 共 {len(records)} 条")

    # 筛选备选商品
    to_process = []
    for r in records:
        f = r.fields
        if is_backup(f) and needs_style(f):
            to_process.append({
                "record_id": r.record_id,
                "product_name": f.get("商品名称", ""),
                "chinese_name": f.get("中文名称", ""),
                "subcategory": f.get("子类目", ""),
                "price": f.get("售价", "")
            })

    print(f"🎯 需打标: {len(to_process)} 条")

    if not to_process:
        print("✅ 无需处理")
        return

    # 分批处理
    batch_size = 20
    total_updated = 0
    total_errors = 0
    total_batches = (len(to_process) - 1) // batch_size + 1

    for batch_idx in range(total_batches):
        start = batch_idx * batch_size
        end = min(start + batch_size, len(to_process))
        batch = to_process[start:end]

        updated, errors = process_batch(client, batch, batch_idx + 1, total_batches)
        total_updated += updated
        total_errors += errors

        print(f"   批次完成: 更新 {updated}, 失败 {errors}")
        print(f"   累计: 更新 {total_updated}, 失败 {total_errors}")

    print(f"\n🎉 全部完成! 成功: {total_updated}, 失败: {total_errors}")

if __name__ == "__main__":
    main()
