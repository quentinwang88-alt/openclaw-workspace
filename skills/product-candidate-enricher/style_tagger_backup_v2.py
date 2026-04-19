#!/usr/bin/env python3
"""对备选商品进行风格打标 - 简化版"""

import sys
import json
import os
import requests
from pathlib import Path
from typing import Dict

sys.path.insert(0, str(Path(__file__).parent))

from core.feishu import FeishuBitableClient, parse_feishu_bitable_url, resolve_wiki_bitable_app_token

FEISHU_URL = "https://gcngopvfvo0q.feishu.cn/wiki/CtGxwJpTEifSh5kIVtgcM2vCnLf?table=tblKhPn64Q266tRz&view=vewmWdRUHq"
FIELD_STYLE = "产品风格"
FIELD_RECOMMEND = "是否推荐"
FIELD_REASON = "详细原因"

# LLM配置 - 从 config.py 读取
import config as cfg
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

请按以下JSON格式输出（不要包含markdown代码块标记）:
{{
  "产品风格": "风格名称",
  "是否推荐": "推荐" 或 "不推荐",
  "详细原因": "简要说明推荐理由，包括风格特征和适用场景"
}}

注意：重点目标风格是"简约通勤风"、"甜美少女风"、"优雅轻奢风"，这些风格推荐纳入备选池；"儿童幼态风"一般不建议纳入发饰备选池。"""

def is_backup_record(fields: Dict) -> bool:
    """检查是否为备选记录"""
    is_backup = fields.get("是否列入备选")
    return is_backup == True or is_backup == "true" or is_backup == "是"

def needs_style_tagging(fields: Dict) -> bool:
    """检查是否需要风格打标"""
    style = fields.get(FIELD_STYLE, "")
    return not style or style == "未标记"

def call_llm(prompt: str) -> str:
    """调用LLM"""
    headers = {
        "Authorization": f"Bearer {LLM_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": LLM_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3
    }
    resp = requests.post(f"{LLM_BASE_URL}/chat/completions", headers=headers, json=data, timeout=60)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]

def parse_llm_response(response: str) -> Dict[str, str]:
    """解析LLM响应"""
    try:
        text = response.strip()
        if text.startswith("```json"):
            text = text[7:]
        if text.startswith("```"):
            text = text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

        data = json.loads(text)
        return {
            FIELD_STYLE: data.get("产品风格", ""),
            FIELD_RECOMMEND: data.get("是否推荐", ""),
            FIELD_REASON: data.get("详细原因", "")
        }
    except Exception as e:
        print(f"  ⚠️ 解析失败: {e}")
        return {}

def main():
    global LLM_API_KEY, LLM_BASE_URL, LLM_MODEL

    if not LLM_API_KEY:
        print("❌ 错误: 未找到 LLM API Key")
        return

    # 初始化飞书客户端
    info = parse_feishu_bitable_url(FEISHU_URL)
    app_token = resolve_wiki_bitable_app_token(info.app_token) if info.is_wiki else info.app_token
    client = FeishuBitableClient(app_token=app_token, table_id=info.table_id)

    # 获取记录
    print("📖 读取表格记录...")
    records = client.list_records(limit=None)
    print(f"✅ 共读取 {len(records)} 条记录")

    # 筛选需要打标的备选记录
    to_process = []
    for record in records:
        fields = record.fields
        if is_backup_record(fields) and needs_style_tagging(fields):
            to_process.append({
                "record_id": record.record_id,
                "product_name": fields.get("商品名称", ""),
                "chinese_name": fields.get("中文名称", ""),
                "subcategory": fields.get("子类目", ""),
                "price": fields.get("售价", "")
            })

    print(f"\n🎯 需要风格打标: {len(to_process)} 条")

    if not to_process:
        print("✅ 没有需要打标的记录")
        return

    # 处理记录
    updated = 0
    errors = 0

    for i, item in enumerate(to_process, 1):
        name = item['chinese_name'] or item['product_name'][:30]
        print(f"\n🔄 [{i}/{len(to_process)}] {name}...")

        prompt = STYLE_PROMPT.format(
            product_name=item["product_name"],
            chinese_name=item["chinese_name"],
            subcategory=item["subcategory"],
            price=item["price"]
        )

        try:
            response = call_llm(prompt)
            result = parse_llm_response(response)

            if result and result.get(FIELD_STYLE):
                client.update_record_fields(
                    record_id=item["record_id"],
                    fields=result
                )
                print(f"  ✅ {result.get(FIELD_STYLE)} | {result.get(FIELD_RECOMMEND)}")
                updated += 1
            else:
                print(f"  ⚠️ 无有效结果")
                errors += 1

        except Exception as e:
            print(f"  ❌ 失败: {e}")
            errors += 1

    print(f"\n🎉 完成! 成功: {updated}, 失败: {errors}")

if __name__ == "__main__":
    main()
