#!/usr/bin/env python3
"""Generate lightweight batch outreach drafts for entry-screen creators.

This script intentionally delegates message strategy to creator-profile-card's
`generate_unified_message()` instead of duplicating outreach prompts/rules here.
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

SKILL_DIR = Path(__file__).resolve().parents[1]
WORKSPACE_DIR = SKILL_DIR.parents[1]
PROFILE_CARD_DIR = WORKSPACE_DIR / "skills" / "creator-profile-card"

sys.path.insert(0, str(SKILL_DIR))
sys.path.insert(0, str(PROFILE_CARD_DIR))

from core.feishu_reader import CreatorRecord, FeishuBitableReader  # noqa: E402
from run_pipeline import resolve_feishu_config  # noqa: E402
from app.services.message_generator import generate_unified_message  # noqa: E402


DEFAULT_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/base/"
    "FdzGbM1b4aXG2zsr6rncFWEln3g?table=tbluJnxKyXquWcEC&view=vewPrkWWaW"
)

OUT_DIR = SKILL_DIR / "output" / "entry_outreach_drafts"
DEFAULT_VERSION = "entry_batch_TH_v1_creator_profile_card"


def get_access_token() -> str:
    reader = FeishuBitableReader()
    return reader._get_access_token()


def update_record_fields(app_token: str, table_id: str, record_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
    response = requests.put(
        url,
        headers={"Authorization": f"Bearer {get_access_token()}", "Content-Type": "application/json"},
        json={"fields": fields},
        timeout=30,
    )
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"飞书回写失败: {json.dumps(result, ensure_ascii=False)}")
    return result


def resolve_bitable_url(feishu_url: str) -> Tuple[str, str]:
    """Resolve base/wiki bitable URL to (app_token, table_id)."""
    table_match = re.search(r"[?&]table=([^&]+)", feishu_url)
    if not table_match:
        raise ValueError(f"飞书 URL 缺少 table 参数: {feishu_url}")
    table_id = table_match.group(1)

    base_match = re.search(r"/base/([a-zA-Z0-9]+)", feishu_url)
    if base_match:
        return base_match.group(1), table_id

    wiki_match = re.search(r"/wiki/([a-zA-Z0-9]+)", feishu_url)
    if not wiki_match:
        return resolve_feishu_config(feishu_url)

    response = requests.get(
        "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
        headers={"Authorization": f"Bearer {get_access_token()}"},
        params={"token": wiki_match.group(1)},
        timeout=30,
    )
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"解析 wiki token 失败: {json.dumps(result, ensure_ascii=False)}")
    return result["data"]["node"]["obj_token"], table_id


def fetch_raw_records(app_token: str, table_id: str) -> List[Dict[str, Any]]:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    records: List[Dict[str, Any]] = []
    page_token = None
    while True:
        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {get_access_token()}"},
            params=params,
            timeout=30,
        )
        result = response.json()
        if result.get("code") != 0:
            raise RuntimeError(f"读取 records 失败: {json.dumps(result, ensure_ascii=False)}")
        data = result.get("data", {})
        records.extend(data.get("items", []))
        if not data.get("has_more"):
            break
        page_token = data.get("page_token")
    return records


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        return str(value.get("link") or value.get("url") or value.get("text") or value.get("name") or "").strip()
    if isinstance(value, list):
        return "、".join(as_text(item) for item in value if item is not None)
    return str(value).strip()


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_handle(value: Any) -> str:
    text = as_text(value).strip()
    if not text:
        return ""
    if "tiktok.com/@" in text:
        text = text.split("tiktok.com/@", 1)[1]
    text = text.split("?", 1)[0].strip("/")
    if text.startswith("@"):
        text = text[1:]
    return text.lower()


def should_generate(
    record: CreatorRecord,
    include_manual: bool = False,
    force: bool = False,
    batch_id: str = "",
) -> Tuple[bool, str]:
    fields = record.raw_fields or {}
    decision = as_text(fields.get("准入决策"))
    draft = as_text(fields.get("批量建联话术"))
    send_status = as_text(fields.get("建联发送状态"))
    generate_status = as_text(fields.get("话术生成状态"))
    record_batch_id = as_text(fields.get("建联批次号"))

    if batch_id and record_batch_id != batch_id:
        return False, f"建联批次号={record_batch_id or '空'}"

    if decision == "通过":
        pass
    elif include_manual and decision == "人工查看":
        pass
    else:
        return False, f"准入决策={decision or '空'}"

    if not force and draft:
        return False, "已有批量建联话术"
    if not force and send_status in {"已发送", "已回复", "不跟进"}:
        return False, f"建联发送状态={send_status}"
    if not force and generate_status == "已生成":
        return False, "话术生成状态=已生成"

    return True, ""


def infer_creator_tier(entry_score: float) -> str:
    if entry_score >= 4.4:
        return "A 类"
    if entry_score >= 4.1:
        return "B 类"
    if entry_score >= 3.8:
        return "C 类"
    return "D 类"


def build_profile_context(record: CreatorRecord, relationship: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    fields = record.raw_fields or {}
    relationship = relationship or {}
    entry_score = as_float(fields.get("准入评分"))
    fit_category = as_text(fields.get("适配类目") or fields.get("主子类") or fields.get("主大类"))
    content_type = as_text(fields.get("内容类型")) or infer_content_type(fit_category)

    return {
        "creator_url": record.tk_url or (f"https://www.tiktok.com/@{record.tk_handle}" if record.tk_handle else ""),
        "history_relation": relationship.get("history_relation") or "陌生",
        "relationship_stage": relationship.get("relationship_stage") or "冷",
        "current_action": relationship.get("current_action") or "主动新品邀约",
        "creator_content_mode": as_text(fields.get("达人擅长内容形式")) or "短视频",
        "content_type": content_type,
        "visual_style": infer_visual_style(content_type),
        "observable_style": build_observable_style(content_type, fit_category),
        "creator_tier": infer_creator_tier(entry_score),
        "fit_category": fit_category,
        "entry_score": entry_score,
    }


def infer_content_type(text: str) -> str:
    if any(keyword in text for keyword in ["发饰", "妆发", "美妆", "护肤", "香水"]):
        return "妆发"
    if any(keyword in text for keyword in ["首饰", "耳饰", "项链", "戒指", "手链"]):
        return "首饰试戴"
    if any(keyword in text for keyword in ["居家", "家居", "收纳"]):
        return "居家生活"
    return "穿搭"


def infer_visual_style(content_type: str) -> str:
    if content_type in {"妆发", "首饰试戴"}:
        return "自拍近景"
    if content_type == "居家生活":
        return "家中生活流"
    return "镜前半身"


def build_observable_style(content_type: str, fit_category: str) -> str:
    if fit_category:
        return f"内容以{content_type or '穿搭'}为主，适配{fit_category}方向"
    return f"内容以{content_type or '穿搭'}为主"


def choose_product_context(fit_category: str) -> Tuple[str, str, str]:
    text = fit_category or ""
    if any(keyword in text for keyword in ["发饰", "妆发"]):
        return "日常发饰新品", "发饰", "轻巧百搭、适合出门前近景整理发型"
    if any(keyword in text for keyword in ["耳饰", "首饰", "项链", "戒指", "手链"]):
        return "日常配饰新品", "配饰", "上脸/上身效果清楚、适合近景试戴"
    if any(keyword in text for keyword in ["居家", "睡衣"]):
        return "居家休闲轻上装", "轻上装", "日常好搭、居家和出门都能自然展示"
    if any(keyword in text for keyword in ["女装", "裙"]):
        return "日常女装新品", "女装", "日常好搭、适合镜前试穿和一衣多搭"
    return "宽松轻薄开衫", "轻上装", "日常好搭、适合镜前试穿和出门前换装"


def build_product_context(record: CreatorRecord, args: argparse.Namespace) -> Tuple[str, str, str]:
    fields = record.raw_fields or {}
    if args.product_name or args.product_category or args.selling_points:
        fallback_name, fallback_category, fallback_points = choose_product_context(as_text(fields.get("适配类目")))
        return (
            args.product_name or fallback_name,
            args.product_category or fallback_category,
            args.selling_points or fallback_points,
        )

    table_name = as_text(fields.get("计划建联产品"))
    table_category = as_text(fields.get("计划建联产品类目"))
    table_points = as_text(fields.get("计划建联产品卖点"))
    if table_name or table_category or table_points:
        fallback_name, fallback_category, fallback_points = choose_product_context(as_text(fields.get("适配类目")))
        return (
            table_name or fallback_name,
            table_category or fallback_category,
            table_points or fallback_points,
        )

    return choose_product_context(as_text(fields.get("适配类目")))


def infer_product_category(text: str) -> str:
    if any(keyword in text for keyword in ["发饰", "头饰", "发夹", "蝴蝶结"]):
        return "发饰"
    if any(keyword in text for keyword in ["耳饰", "耳环", "项链", "戒指", "手链", "首饰"]):
        return "配饰"
    if any(keyword in text for keyword in ["裙", "连衣裙"]):
        return "女装"
    if any(keyword in text for keyword in ["开衫", "上衣", "针织", "衬衫", "T恤", "背心", "外套"]):
        return "轻上装"
    return "轻上装"


def clean_attachment_name(name: str) -> str:
    text = Path(name).stem
    text = re.sub(r"^(card_|产品图_|商品图_)", "", text)
    text = re.sub(r"[_\\-]+", " ", text).strip()
    return text


def build_product_index(product_table_url: str) -> Dict[str, Tuple[str, str, str]]:
    if not product_table_url:
        return {}

    app_token, table_id = resolve_bitable_url(product_table_url)
    records = fetch_raw_records(app_token, table_id)
    index: Dict[str, Tuple[str, str, str]] = {}

    for item in records:
        fields = item.get("fields") or {}
        batch_id = as_text(fields.get("建联批次号"))
        if not batch_id:
            continue

        product_name = as_text(fields.get("本次带货产品"))
        if not product_name:
            attachments = fields.get("生成卡片") or fields.get("产品图片") or []
            if isinstance(attachments, list):
                for item in attachments:
                    if isinstance(item, dict) and item.get("name"):
                        product_name = clean_attachment_name(item.get("name", ""))
                        if product_name:
                            break
        if not product_name:
            product_name = "本轮计划建联产品"

        extra_text = " ".join([
            product_name,
            as_text(fields.get("卖点补充")),
            as_text(fields.get("生成卡片")),
            as_text(fields.get("产品图片")),
        ])
        product_category = infer_product_category(extra_text)
        selling_parts = []
        if fields.get("卖点补充"):
            selling_parts.append(as_text(fields.get("卖点补充")))
        if fields.get("佣金比例"):
            selling_parts.append(f"佣金{as_text(fields.get('佣金比例'))}")
        selling_points = "、".join(selling_parts) or "日常好搭、适合低成本短视频展示"

        index[batch_id] = (product_name, product_category, selling_points)

    print(f"📦 产品批次索引: {len(index)} 个批次")
    return index


def generate_one(
    record: CreatorRecord,
    version: str,
    product_context: Tuple[str, str, str],
    relationship: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    context = build_profile_context(record, relationship=relationship)
    product_name, product_category, selling_points = product_context

    result = generate_unified_message(
        creator_url=context["creator_url"],
        current_action=context["current_action"],
        relationship_stage=context["relationship_stage"],
        history_relation=context["history_relation"],
        creator_content_mode=context["creator_content_mode"],
        market="TH",
        target_language="泰语",
        content_type=context["content_type"],
        visual_style=context["visual_style"],
        observable_style=context["observable_style"],
        personalization_level="L1",
        recommended_product=product_name,
        product_category=product_category,
        selling_points=selling_points,
        creator_tier=context["creator_tier"],
        fit_categories=context["fit_category"],
    )

    result["draft_version"] = version
    result["selected_product_name"] = product_name
    result["selected_product_category"] = product_category
    result["selected_selling_points"] = selling_points
    result["creator_tier"] = context["creator_tier"]
    result["relationship_stage"] = context["relationship_stage"]
    result["history_relation"] = context["history_relation"]
    result["current_action"] = context["current_action"]
    return result


def build_relationship_index(relationship_table_url: str) -> Dict[str, Dict[str, str]]:
    if not relationship_table_url:
        return {}

    app_token, table_id = resolve_bitable_url(relationship_table_url)
    records = fetch_raw_records(app_token, table_id)
    index: Dict[str, Dict[str, str]] = {}

    for item in records:
        fields = item.get("fields") or {}
        candidate_handles = [
            fields.get("tk_handle"),
            fields.get("TikTok账号"),
            fields.get("TikTok 账号"),
            fields.get("达人账号"),
            fields.get("达人用户名"),
            fields.get("达人昵称"),
            fields.get("达人链接"),
            fields.get("TikTok链接"),
        ]
        relation = {
            "history_relation": as_text(fields.get("历史关系")) or "陌生",
            "relationship_stage": as_text(fields.get("关系阶段")) or "冷",
            "current_action": as_text(fields.get("当前动作")) or "主动新品邀约",
        }
        for value in candidate_handles:
            handle = normalize_handle(value)
            if handle:
                index[handle] = relation
    print(f"🔎 关系表索引: {len(index)} 个达人")
    return index


def get_relationship(record: CreatorRecord, relationship_index: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    for value in (record.tk_handle, record.tk_url):
        handle = normalize_handle(value)
        if handle and handle in relationship_index:
            return relationship_index[handle]
    return {"history_relation": "陌生", "relationship_stage": "冷", "current_action": "主动新品邀约"}


def iter_targets(records: Iterable[CreatorRecord], include_manual: bool, force: bool, batch_id: str) -> List[CreatorRecord]:
    targets = []
    skipped = []
    for record in records:
        ok, reason = should_generate(record, include_manual=include_manual, force=force, batch_id=batch_id)
        if ok:
            targets.append(record)
        else:
            skipped.append({"record_id": record.record_id, "tk_handle": record.tk_handle, "reason": reason})
    print(f"📌 可生成话术: {len(targets)}；跳过: {len(skipped)}")
    return targets


def render_creator_template(template: str, record: CreatorRecord) -> str:
    handle = record.tk_handle or "creator"
    replacements = {
        "@creator_name": f"@{handle}",
        "creator_name": handle,
        "{creator_name}": handle,
        "{达人昵称}": handle,
    }
    output = template
    for source, target in replacements.items():
        output = output.replace(source, target)
    return output


def strip_batch_greeting(message: str) -> str:
    """Remove leading greeting/name from reusable batch outreach copy."""
    text = as_text(message).strip()
    if not text:
        return ""

    patterns = [
        r"^(?:哈喽|你好|您好|嗨|Hi|Hello|Hey)\s*[@＠]?[A-Za-z0-9_.\-\u4e00-\u9fff]+[～~，,、!\s-]*",
        r"^(?:สวัสดีค่ะ|สวัสดีครับ|สวัสดี|หวัดดีค่ะ|หวัดดีครับ)\s*[@＠]?[A-Za-z0-9_.\-]+[～~，,、!\s-]*",
        r"^[@＠][A-Za-z0-9_.\-]+[～~，,、!\s-]*",
    ]
    for pattern in patterns:
        text = re.sub(pattern, "", text, count=1, flags=re.IGNORECASE).strip()
    return text


def generate_batch_template(
    representative: CreatorRecord,
    version: str,
    product_context: Tuple[str, str, str],
    relationship: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    pseudo_record = CreatorRecord(
        record_id="batch_template",
        tk_handle="creator_name",
        tk_url="https://www.tiktok.com/@creator_name",
        kalodata_url=None,
        video_screenshots=None,
        raw_fields=representative.raw_fields or {},
    )
    return generate_one(
        pseudo_record,
        version=version,
        product_context=product_context,
        relationship=relationship,
    )


def template_key(
    relationship: Dict[str, str],
    product_context: Tuple[str, str, str],
) -> Tuple[str, str, str, str, str, str]:
    return (
        relationship.get("current_action", "主动新品邀约"),
        relationship.get("relationship_stage", "冷"),
        relationship.get("history_relation", "陌生"),
        product_context[0],
        product_context[1],
        product_context[2],
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate entry-screen batch outreach drafts.")
    parser.add_argument("--feishu-url", default=DEFAULT_FEISHU_URL)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--include-manual", action="store_true", help="also include 准入决策=人工查看")
    parser.add_argument("--force", action="store_true", help="regenerate even if draft exists")
    parser.add_argument("--write", action="store_true", help="write drafts back to Feishu")
    parser.add_argument("--version", default=DEFAULT_VERSION)
    parser.add_argument("--batch-id", default="", help="only process records with this 建联批次号")
    parser.add_argument("--relationship-table-url", default="", help="optional creator maintenance table for relationship lookup")
    parser.add_argument("--product-table-url", default="", help="optional planned outreach product table URL")
    parser.add_argument("--product-name", default="", help="planned outreach product name for this batch")
    parser.add_argument("--product-category", default="", help="planned outreach product category for this batch")
    parser.add_argument("--selling-points", default="", help="planned outreach product selling points for this batch")
    parser.add_argument(
        "--per-creator-message",
        action="store_true",
        help="generate one LLM draft per creator; default with --batch-id is one reusable batch template",
    )
    args = parser.parse_args()

    app_token, table_id = resolve_bitable_url(args.feishu_url)
    reader = FeishuBitableReader(app_token, table_id)
    records = reader.read_records(page_size=100)
    relationship_index = build_relationship_index(args.relationship_table_url)
    product_index = build_product_index(args.product_table_url)
    targets = iter_targets(records, include_manual=args.include_manual, force=args.force, batch_id=args.batch_id)

    if args.offset:
        targets = targets[args.offset:]
    if args.limit:
        targets = targets[:args.limit]

    print(f"🚀 本次处理: {len(targets)} 条；write={args.write}")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    result_path = OUT_DIR / f"drafts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"

    success = 0
    failed = 0
    template_cache: Dict[Tuple[str, str, str, str, str, str], Dict[str, Any]] = {}
    use_batch_template = bool(args.batch_id and not args.per_creator_message)
    if use_batch_template:
        print("🧩 批次模板模式：同一批次 + 同一关系/动作路由复用一套话术")

    with result_path.open("w", encoding="utf-8") as file:
        for index, record in enumerate(targets, 1):
            started = time.time()
            print(f"\n[{index}/{len(targets)}] {record.tk_handle} ({record.record_id})")
            try:
                relationship = get_relationship(record, relationship_index)
                product_context = product_index.get(args.batch_id) or build_product_context(record, args)
                if use_batch_template:
                    key = template_key(relationship, product_context)
                    if key not in template_cache:
                        print(
                            "  🧩 生成路由模板: "
                            f"action={key[0]}, stage={key[1]}, history={key[2]}, "
                            f"product={product_context[0]} ({product_context[1]})"
                        )
                        template_cache[key] = generate_batch_template(
                            record,
                            version=args.version,
                            product_context=product_context,
                            relationship=relationship,
                        )
                    batch_result = template_cache[key]
                    result = dict(batch_result)
                    result["message_cn_for_operator"] = strip_batch_greeting(
                        render_creator_template(as_text(batch_result.get("message_cn_for_operator")), record)
                    )
                    result["message_local"] = strip_batch_greeting(
                        render_creator_template(as_text(batch_result.get("message_local")), record)
                    )
                    result["used_batch_template"] = True
                    result["relationship_stage"] = relationship.get("relationship_stage", "冷")
                    result["history_relation"] = relationship.get("history_relation", "陌生")
                else:
                    result = generate_one(
                        record,
                        version=args.version,
                        product_context=product_context,
                        relationship=relationship,
                    )
                draft_cn = as_text(result.get("message_cn_for_operator"))
                draft_local = as_text(result.get("message_local"))
                should_not_send = bool(result.get("should_not_send"))

                if should_not_send or not (draft_cn or draft_local):
                    status = "无需生成"
                    fields = {
                        "话术生成状态": status,
                        "话术版本": args.version,
                        "话术生成原因": as_text(result.get("should_not_send_reason") or result.get("error")),
                        "建联批次号": args.batch_id or as_text((record.raw_fields or {}).get("建联批次号")),
                        "计划建联产品": product_context[0],
                        "计划建联产品类目": product_context[1],
                        "计划建联产品卖点": product_context[2],
                        "进入维护状态": "待回复",
                    }
                else:
                    status = "已生成"
                    fields = {
                        "批量建联话术": draft_cn,
                        "批量建联话术本地语言": draft_local,
                        "话术生成状态": status,
                        "话术版本": args.version,
                        "话术质量分": as_float(result.get("quality_score")),
                        "话术生成原因": as_text(result.get("raw_output", {}).get("why_this_message") or result.get("why_this_message")),
                        "建联批次号": args.batch_id or as_text((record.raw_fields or {}).get("建联批次号")),
                        "计划建联产品": product_context[0],
                        "计划建联产品类目": product_context[1],
                        "计划建联产品卖点": product_context[2],
                        "建联发送状态": "待发送",
                        "进入维护状态": "待回复",
                    }

                if args.write:
                    update_record_fields(app_token, table_id, record.record_id, fields)

                success += 1
                print(f"  ✅ {status} quality={result.get('quality_score', '')} len={len(draft_local or draft_cn)}")
                row = {
                    "record_id": record.record_id,
                    "tk_handle": record.tk_handle,
                    "success": True,
                    "write": args.write,
                    "elapsed_sec": round(time.time() - started, 1),
                    "fields": fields,
                    "result": result,
                }
            except Exception as exc:
                failed += 1
                print(f"  ❌ 失败: {exc}")
                row = {
                    "record_id": record.record_id,
                    "tk_handle": record.tk_handle,
                    "success": False,
                    "write": args.write,
                    "elapsed_sec": round(time.time() - started, 1),
                    "error": str(exc),
                }
                if args.write:
                    try:
                        update_record_fields(app_token, table_id, record.record_id, {
                            "话术生成状态": "生成失败",
                            "话术生成原因": str(exc)[:1000],
                        })
                    except Exception:
                        pass

            file.write(json.dumps(row, ensure_ascii=False) + "\n")
            file.flush()

    print(json.dumps({
        "success": success,
        "failed": failed,
        "result_path": str(result_path),
    }, ensure_ascii=False, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
