#!/usr/bin/env python3
"""对备选商品进行风格打标"""

import sys
import json
import os
from pathlib import Path
from typing import List, Dict, Any

sys.path.insert(0, str(Path(__file__).parent))

from core.feishu import FeishuBitableClient, parse_feishu_bitable_url, resolve_wiki_bitable_app_token
from core.llm import CandidateLLMClient

FEISHU_URL = "https://gcngopvfvo0q.feishu.cn/wiki/CtGxwJpTEifSh5kIVtgcM2vCnLf?table=tblKhPn64Q266tRz&view=vewmWdRUHq"
FIELD_STYLE = "产品风格"
FIELD_RECOMMEND = "是否推荐"
FIELD_REASON = "详细原因"

# 从环境变量或配置文件读取LLM配置
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://api.openai.com/v1")
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")

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

def parse_llm_response(response: str) -> Dict[str, str]:
    """解析LLM响应"""
    try:
        # 清理响应文本
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
        print(f"  ⚠️ 解析LLM响应失败: {e}")
        return {}

def main():
    # 初始化客户端
    info = parse_feishu_bitable_url(FEISHU_URL)
    app_token = resolve_wiki_bitable_app_token(info.app_token) if info.is_wiki else info.app_token

    client = FeishuBitableClient(app_token=app_token, table_id=info.table_id)

    # 初始化LLM客户端
    if not LLM_API_KEY:
        print("❌ 错误: 未设置 LLM_API_KEY 环境变量")
        print("请设置环境变量: export LLM_API_KEY='your-api-key'")
        return

    llm = CandidateLLMClient(
        base_url=LLM_BASE_URL,
        api_key=LLM_API_KEY,
        model=LLM_MODEL,
        subcategories=["发夹", "发簪", "发带", "发箍", "其它"]
    )

    # 获取所有记录
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

    print(f"\n🎯 需要风格打标的备选商品: {len(to_process)} 条")

    if not to_process:
        print("✅ 没有需要打标的记录")
        return

    # 批量处理
    updated = 0
    errors = 0

    for i, item in enumerate(to_process, 1):
        print(f"\n🔄 [{i}/{len(to_process)}] 处理: {item['chinese_name'] or item['product_name'][:30]}...")

        # 构建prompt
        prompt = STYLE_PROMPT.format(
            product_name=item["product_name"],
            chinese_name=item["chinese_name"],
            subcategory=item["subcategory"],
            price=item["price"]
        )

        try:
            # 调用LLM - 使用 translate_and_tag 方法
            response = llm.translate_and_tag(
                product_name=item["product_name"],
                product_category=item["subcategory"],
                country="VN",
                image_url=""
            )
            # 由于 translate_and_tag 返回的是 CandidateLLMResult，我们需要用 prompt 方式
            # 这里改用直接 HTTP 调用
            import requests
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
            response_text = resp.json()["choices"][0]["message"]["content"]
            result = parse_llm_response(response_text)

            if result:
                # 更新记录
                client.update_record_fields(
                    record_id=item["record_id"],
                    fields=result
                )
                print(f"  ✅ 已更新: {result.get(FIELD_STYLE)} | {result.get(FIELD_RECOMMEND)}")
                updated += 1
            else:
                print(f"  ⚠️ 未能解析结果")
                errors += 1

        except Exception as e:
            print(f"  ❌ 处理失败: {e}")
            errors += 1

    print(f"\n🎉 完成!")
    print(f"   成功更新: {updated} 条")
    print(f"   失败: {errors} 条")

if __name__ == "__main__":
    main()
