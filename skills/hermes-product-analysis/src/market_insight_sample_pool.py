#!/usr/bin/env python3
"""Build direction-level top/new product sample pools for market insight."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional, Tuple

from src.market_insight_decision_layer import DECISION_ACTION_LABELS, OPPORTUNITY_TYPE_LABELS
from src.market_insight_display_names import display_enum
from src.market_insight_models import MarketDirectionCard, MarketInsightConfig, ScoredProductSnapshot
from src.product_age import assign_age_bucket


MANUAL_FIELD_NAMES = {
    "人工判断状态",
    "人工备注",
    "是否加入选品池",
    "是否加入内容测试池",
    "是否需要找同款",
    "是否需要拆视频",
    "优先级",
    "负责人",
    "更新时间",
}

AGE_BUCKET_LABELS = {
    "d0_30": "0-30天",
    "d31_90": "31-90天",
    "d91_180": "91-180天",
    "d181_365": "181-365天",
    "d365_plus": "365天以上",
    "unknown": "未知",
}

ACTION_ORDER = {
    "优先低成本验证": 1,
    "谨慎切入验证": 2,
    "暗线小样本验证": 3,
    "拆头部不直接入场": 4,
    "持续观察": 5,
    "暂不投入": 6,
}


def build_direction_sample_pool(
    scored_items: Iterable[ScoredProductSnapshot],
    direction_cards: Iterable[MarketDirectionCard | Dict[str, Any]],
    config: Optional[MarketInsightConfig] = None,
) -> List[Dict[str, Any]]:
    """Return Feishu-ready sample rows for every direction card.

    Each product appears at most once per direction. A product can carry multiple
    sample types, e.g. both "头部Top10" and "代表新品".
    """

    cards = [_as_card_dict(card) for card in direction_cards]
    card_by_key = {
        str(card.get("direction_canonical_key") or card.get("direction_instance_id") or "").strip(): card
        for card in cards
    }
    valid_items = [item for item in scored_items if item.tag.is_valid_sample and str(item.direction_canonical_key or "").strip()]
    grouped: Dict[str, List[ScoredProductSnapshot]] = {}
    for item in valid_items:
        grouped.setdefault(str(item.direction_canonical_key or "").strip(), []).append(item)

    rows: List[Dict[str, Any]] = []
    for card in cards:
        direction_key = str(card.get("direction_canonical_key") or card.get("direction_instance_id") or "").strip()
        direction_items = _dedupe_items(grouped.get(direction_key, []))
        if not direction_items:
            continue

        row_by_key: Dict[str, Dict[str, Any]] = {}

        top_items = sorted(
            direction_items,
            key=lambda item: (-_num(item.snapshot.sales_7d), int(item.snapshot.rank_index or 0)),
        )[:10]
        for rank, item in enumerate(top_items, start=1):
            row = _ensure_row(row_by_key, card, item, config=config)
            _append_type(row, "头部Top10")
            row["头部排名"] = rank
            if _age_days(item) is not None and int(_age_days(item) or 0) > 180:
                _append_type(row, "老品占位头部")

        new_items, new_scope = _select_representative_new_items(direction_items, card)
        for rank, item in enumerate(new_items, start=1):
            row = _ensure_row(row_by_key, card, item, config=config)
            _append_type(row, "代表新品")
            row["新品代表排名"] = rank
            row["新品口径"] = new_scope
            if _is_few_new_winners_direction(card):
                _append_type(row, "少数新品赢家")

        for row in row_by_key.values():
            if _should_mark_differentiation_candidate(row, card):
                _append_type(row, "差异化候选")
            rows.append(row)

    rows.sort(
        key=lambda row: (
            ACTION_ORDER.get(str(row.get("方向动作") or ""), 99),
            str(row.get("方向名称") or ""),
            0 if "头部Top10" in list(row.get("样本类型") or []) else 1,
            int(row.get("头部排名") or 999),
            int(row.get("新品代表排名") or 999),
            -_num(row.get("7日销量")),
        )
    )
    for row in rows:
        row["样本排名"] = _sample_rank_label(row)
    return rows


def build_sample_pool_diagnostics(
    scored_items: Iterable[ScoredProductSnapshot],
    direction_cards: Iterable[MarketDirectionCard | Dict[str, Any]],
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build a compact audit log to catch wrong source/field mapping early."""

    scored_list = list(scored_items)
    cards = [_as_card_dict(card) for card in direction_cards]
    valid_items = [item for item in scored_list if item.tag.is_valid_sample]
    product_keys = [_product_unique_key(item) for item in valid_items]
    duplicate_count = len(product_keys) - len(set(product_keys))
    row_direction_names = {str(row.get("方向名称") or "").strip() for row in rows if str(row.get("方向名称") or "").strip()}
    card_direction_names = {str(card.get("direction_name") or card.get("style_cluster") or "").strip() for card in cards if str(card.get("direction_name") or card.get("style_cluster") or "").strip()}
    missing_directions = sorted(card_direction_names - row_direction_names)
    return {
        "data_source_check": "商品级明细" if valid_items else "缺少商品级明细",
        "direction_count": len(cards),
        "product_detail_count": len(valid_items),
        "written_sample_count": len(rows),
        "top10_sample_count": sum(1 for row in rows if "头部Top10" in list(row.get("样本类型") or [])),
        "representative_new_sample_count": sum(1 for row in rows if "代表新品" in list(row.get("样本类型") or [])),
        "missing_product_image_count": sum(1 for row in rows if not row.get("商品主图") and not row.get("商品主图URL")),
        "missing_price_count": sum(1 for row in rows if row.get("价格") in (None, "")),
        "missing_sales_7d_count": sum(1 for row in rows if row.get("7日销量") in (None, "")),
        "missing_listing_age_count": sum(1 for row in rows if row.get("上架天数") in (None, "")),
        "missing_fastmoss_link_count": sum(1 for row in rows if not str(row.get("FastMoss链接") or "").strip()),
        "deduped_product_count": max(0, duplicate_count),
        "abnormal_direction_count": len(missing_directions),
        "abnormal_directions": missing_directions,
    }


def _as_card_dict(card: MarketDirectionCard | Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(card, dict):
        return dict(card)
    return card.to_dict()


def _dedupe_items(items: List[ScoredProductSnapshot]) -> List[ScoredProductSnapshot]:
    results: List[ScoredProductSnapshot] = []
    seen = set()
    for item in items:
        key = _product_unique_key(item)
        if key in seen:
            continue
        seen.add(key)
        results.append(item)
    return results


def _product_unique_key(item: ScoredProductSnapshot) -> str:
    snapshot = item.snapshot
    for value in (snapshot.product_id, snapshot.product_url):
        text = str(value or "").strip()
        if text:
            return text
    return "{name}__{shop}__{price}".format(
        name=str(snapshot.product_name or "").strip(),
        shop=str(snapshot.shop_name or "").strip(),
        price=str(snapshot.price_mid or "").strip(),
    )


def _ensure_row(
    row_by_key: Dict[str, Dict[str, Any]],
    card: Dict[str, Any],
    item: ScoredProductSnapshot,
    config: Optional[MarketInsightConfig] = None,
) -> Dict[str, Any]:
    key = _sample_unique_key(card, item)
    if key not in row_by_key:
        row_by_key[key] = _build_base_row(card=card, item=item, sample_key=key, config=config)
    return row_by_key[key]


def _sample_unique_key(card: Dict[str, Any], item: ScoredProductSnapshot) -> str:
    direction_key = str(card.get("direction_canonical_key") or card.get("direction_instance_id") or "").strip()
    return "{batch}__{direction}__{product}".format(
        batch=str(item.snapshot.batch_id or item.snapshot.batch_date or "").strip(),
        direction=direction_key,
        product=_product_unique_key(item),
    )


def _sample_rank_label(row: Dict[str, Any]) -> str:
    parts = []
    if row.get("头部排名") not in (None, ""):
        parts.append("Top{rank}".format(rank=int(row.get("头部排名") or 0)))
    if row.get("新品代表排名") not in (None, ""):
        parts.append("新品{rank}".format(rank=int(row.get("新品代表排名") or 0)))
    return " / ".join(parts)


def _build_base_row(
    card: Dict[str, Any],
    item: ScoredProductSnapshot,
    sample_key: str,
    config: Optional[MarketInsightConfig] = None,
) -> Dict[str, Any]:
    snapshot = item.snapshot
    age_days = _age_days(item)
    age_bucket = item.age_bucket or snapshot.age_bucket or assign_age_bucket(age_days)
    image_urls = _image_urls(item)
    image_attachments = _image_attachments(item)
    direction_action = str(card.get("decision_action") or "")
    opportunity_type = str(card.get("primary_opportunity_type") or "")
    price_cny = _price_cny(snapshot.price_mid, config)
    context = _direction_context(card, item)
    head_context = _head_context(card, item, context)
    video_density = _density(snapshot.video_count, snapshot.sales_7d)
    creator_density = _density(snapshot.creator_count, snapshot.sales_7d)
    top3_share = _nested_float(card, ["demand_structure", "top3_sales_share"])
    return {
        "样本唯一键": sample_key,
        "批次ID": str(snapshot.batch_id or ""),
        "批次日期": str(snapshot.batch_date or card.get("batch_date") or ""),
        "国家": str(snapshot.country or card.get("country") or ""),
        "一级类目": str(snapshot.category or card.get("category") or ""),
        "方向ID": str(card.get("direction_canonical_key") or card.get("direction_instance_id") or ""),
        "方向名称": str(card.get("direction_name") or card.get("style_cluster") or ""),
        "方向动作": DECISION_ACTION_LABELS.get(direction_action, direction_action),
        "主机会类型": OPPORTUNITY_TYPE_LABELS.get(opportunity_type, opportunity_type),
        "关联方向记录": str(card.get("direction_canonical_key") or card.get("direction_instance_id") or ""),
        "样本类型": [],
        "样本排名": "",
        "头部排名": None,
        "新品代表排名": None,
        "商品ID": str(snapshot.product_id or ""),
        "商品标题": str(snapshot.product_name or ""),
        "店铺名称": str(snapshot.shop_name or ""),
        "FastMoss链接": str(snapshot.product_url or ""),
        "平台商品链接": str(snapshot.product_url or ""),
        "商品主图": image_attachments,
        "商品主图URL": image_urls[0] if image_urls else str(snapshot.image_url or ""),
        "商品图片组URL": "、".join(image_urls),
        "价格": price_cny if price_cny is not None else snapshot.price_mid,
        "价格币种": str(getattr(config, "source_currency", "") or ""),
        "折算人民币价格": price_cny,
        "价格带": str(item.target_price_band or ""),
        "价格带置信度": display_enum(_price_band_confidence(card), "confidence"),
        "7日销量": _num(snapshot.sales_7d),
        "7日销售额": _num(snapshot.gmv_7d),
        "30日销量": _raw_number(snapshot.raw_fields, ["30日销量", "30天销量", "近30天销量"]),
        "累计销量": _raw_number(snapshot.raw_fields, ["累计销量", "总销量", "历史销量"]),
        "评论数": _raw_number(snapshot.raw_fields, ["评论数", "评价数"]),
        "视频数量": _num(snapshot.video_count),
        "达人数量": _num(snapshot.creator_count),
        "视频密度": round(video_density, 4),
        "达人密度": round(creator_density, 4),
        "Top3销量占比所在方向": round(top3_share, 4) if top3_share is not None else None,
        "是否超过行动阈值": _num(snapshot.sales_7d) >= _sales_action_threshold(card),
        "上架日期": str(snapshot.listing_datetime or _raw_text(snapshot.raw_fields, ["预估商品上架时间", "上架日期", "上架时间"])),
        "上架天数": age_days,
        "商品年龄分桶": AGE_BUCKET_LABELS.get(str(age_bucket or "unknown"), str(age_bucket or "未知")),
        "新品口径": "",
        "商品形态": str(item.tag.product_form or ""),
        "发饰细分类": str(item.tag.product_form or ""),
        "风格标签": _clean_list([item.tag.style_cluster] + list(item.tag.style_tags_secondary or [])),
        "功能标签": _clean_list(item.tag.value_points),
        "场景标签": _clean_list(item.tag.scene_tags),
        "价格带标签": str(item.target_price_band or ""),
        "是否方向主承载形态": _is_dominant_form(card, item),
        "核心使用场景": context["core_use_scene"],
        "用户痛点": context["user_pain_point"],
        "核心价值点": context["core_value_points"],
        "购买理由": context["purchase_reasons"],
        "内容可表达点": context["content_expression_points"],
        "差异化机会": context["differentiation_opportunity"],
        "不建议跟进原因": context["not_follow_reason"],
        "头部胜出机制": head_context["head_win_mechanism"],
        "可复制点": head_context["replicable_points"],
        "不可复制点": head_context["non_replicable_points"],
        "可绕开切口": head_context["bypass_angle"],
        "头部可复制性评分": head_context["copyability_score"],
        "是否建议进入小样本测试": head_context["suggest_small_test"],
        "人工判断状态": "待查看",
    }


def _select_representative_new_items(
    direction_items: List[ScoredProductSnapshot],
    card: Dict[str, Any],
) -> Tuple[List[ScoredProductSnapshot], str]:
    new_90 = [item for item in direction_items if _age_days(item) is not None and int(_age_days(item) or 0) <= 90]
    candidates = new_90
    scope = "90天新品"
    if len(candidates) < 3:
        candidates = [item for item in direction_items if _age_days(item) is not None and int(_age_days(item) or 0) <= 180]
        scope = "180天补充样本" if candidates else ""
    if not candidates:
        return [], ""

    sorted_candidates = sorted(
        candidates,
        key=lambda item: (-_new_representative_score(item), int(item.snapshot.rank_index or 0)),
    )[:10]
    selected: List[ScoredProductSnapshot] = []
    seen_forms = set()
    seen_bands = set()
    for item in sorted_candidates:
        form = str(item.tag.product_form or "other")
        band = str(item.target_price_band or "unknown")
        if len(selected) >= 5:
            break
        if len(selected) >= 3 and form in seen_forms and band in seen_bands:
            continue
        selected.append(item)
        seen_forms.add(form)
        seen_bands.add(band)
    if len(selected) < min(3, len(sorted_candidates)):
        for item in sorted_candidates:
            if item not in selected:
                selected.append(item)
            if len(selected) >= min(5, len(sorted_candidates)):
                break
    return selected[:5], scope


def _new_representative_score(item: ScoredProductSnapshot) -> float:
    sales = _num(item.snapshot.sales_7d)
    age = _age_days(item)
    freshness = 1.0 if age is None else max(0.0, 1.0 - min(float(age), 180.0) / 180.0)
    distinctiveness = 1.0 if (item.tag.element_tags or item.tag.scene_tags or item.tag.value_points) else 0.5
    return sales * 0.65 + freshness * 100.0 * 0.20 + distinctiveness * 100.0 * 0.15


def _append_type(row: Dict[str, Any], sample_type: str) -> None:
    values = list(row.get("样本类型") or [])
    if sample_type not in values:
        values.append(sample_type)
    row["样本类型"] = values


def _direction_context(card: Dict[str, Any], item: ScoredProductSnapshot) -> Dict[str, Any]:
    name = str(card.get("direction_name") or card.get("style_cluster") or item.tag.style_cluster or "")
    family = str(card.get("direction_family") or "")
    if name == "other":
        return {
            "core_use_scene": "方向信息不完整，先人工确认具体场景。",
            "user_pain_point": "当前方向归类较泛，无法稳定判断用户痛点。",
            "core_value_points": [],
            "purchase_reasons": [],
            "content_expression_points": [],
            "differentiation_opportunity": "先确认具体商品形态和场景，再判断差异化机会。",
            "not_follow_reason": "方向信息不完整，不建议直接跟进。",
        }
    if "发箍" in name:
        return _context(
            "出门前整理刘海/碎发、洗脸化妆、拍照前整理头顶轮廓",
            "碎发乱、头型不够饱满、普通发箍勒头或显脸大",
            ["修饰头型", "压碎发", "显脸小", "不勒头"],
            ["低价解决头型和碎发整理需求", "可同时覆盖居家和出门场景"],
            ["戴前戴后脸型/头型变化", "侧脸/正脸对比", "头顶蓬松度和碎发控制展示"],
            "优先找修饰脸型、压碎发、不勒头的细分款。",
        )
    if "发圈" in name:
        return _context(
            "上学/出门随手扎、宿舍/办公室备用、不同衣服颜色搭配",
            "发圈容易勒头、掉发、颜色难搭或单个不划算",
            ["多件组合划算", "颜色日常", "不勒头发", "可手腕佩戴"],
            ["低价多件降低试错", "一组覆盖多套穿搭"],
            ["一组多色快速切换", "一周不同搭配", "手腕佩戴 + 扎发双场景"],
            "优先看颜色组合、材质质感和不勒头表达，不做普通低价堆量。",
        )
    if any(token in name for token in ["盘发", "头盔", "整理"]) or family == "功能结果型":
        return _context(
            "热天出门前快速盘发；戴头盔后头发压乱快速整理；上班/上学前赶时间整理头发",
            "头发乱、热、赶时间、不会复杂盘发或普通夹子固定不稳",
            ["快速整理", "夹得稳", "不勒头皮", "操作简单"],
            ["低价解决高频整理需求", "演示效果直观，容易被内容放大"],
            ["30秒前后对比", "一镜到底操作", "头发乱到整齐的变化"],
            "优先找厚发可用、头盔后整理、热天快速出门等具体场景切口。",
        )
    if "大体量" in name:
        return _context(
            "出门前提升整体造型；约会/拍照/通勤穿搭的发型收尾",
            "长发披散太乱、普通小发夹撑不起造型、抓夹显廉价或笨重",
            ["体量感明显", "撑起发量和头型", "提升造型完整度"],
            ["用一个发饰快速让穿搭更完整", "适合长发/厚发用户"],
            ["披发前后对比", "半身穿搭 + 发型完成度变化", "侧后方展示轮廓"],
            "避开纯大但笨重的款，找体量、质感和轮廓兼顾的样本。",
        )
    if "韩系" in name:
        return _context(
            "上学前出门、办公室/教室日常、普通T恤/衬衫/针织搭配",
            "普通发饰太夸张或幼稚，通勤场景需要低调但有细节",
            ["低调不幼稚", "轻精致", "通勤百搭"],
            ["低价提升日常穿搭完成度", "适合学生和上班场景"],
            ["普通穿搭加发饰后更完整", "镜前3秒整理发型", "近景细节 + 远景搭配"],
            "优先看基础色、金属/珍珠小细节和低调通勤表达。",
        )
    if "甜感" in name:
        return _context(
            "拍照、约会、周末出门、学生轻甜搭配",
            "普通甜美发饰容易廉价、幼稚或没有记忆点",
            ["甜感明显", "视觉记忆点", "拍照装饰感"],
            ["价格可接受，可作为日常搭配小物", "适合拍照和约会场景"],
            ["有无发饰对比", "近景颜色和细节压镜", "甜感但不夸张的上头效果"],
            "不铺普通甜美款，只拆有视觉记忆点且不廉价的款。",
        )
    if "少女礼物" in name:
        return _context(
            "学生朋友互送、约会前搭配、日常素色穿搭增加细节",
            "普通发夹太廉价、太幼稚、不够有礼物感",
            ["礼物感", "提升精致度", "小体积记忆点"],
            ["学生价位可接受", "适合作为小礼物或日常搭配"],
            ["包装 / 上头效果 / 拍照出片三段式", "素色穿搭加发夹后变精致"],
            "找礼物感但不幼稚、学生价位但不廉价的差异化样本。",
        )
    return _context(
        "上学、上班、出门等日常搭配场景",
        "普通基础款容易同质化，缺少明确购买理由",
        _clean_list(item.tag.value_points) or ["日常百搭"],
        ["低价降低试错", "可覆盖高频日常场景"],
        ["有无搭配对比", "近景细节展示", "日常场景快速使用"],
        "优先寻找比普通基础款更具体的产品/场景/内容切口。",
    )


def _context(scene: str, pain: str, values: List[str], reasons: List[str], content: List[str], opportunity: str) -> Dict[str, Any]:
    return {
        "core_use_scene": scene,
        "user_pain_point": pain,
        "core_value_points": values,
        "purchase_reasons": reasons,
        "content_expression_points": content,
        "differentiation_opportunity": opportunity,
        "not_follow_reason": "",
    }


def _head_context(card: Dict[str, Any], item: ScoredProductSnapshot, context: Dict[str, Any]) -> Dict[str, Any]:
    mechanisms = []
    age_days = _age_days(item)
    if age_days is not None and int(age_days) > 180:
        mechanisms.extend(["老品权重", "评论/销量沉淀"])
    if _num(item.snapshot.sales_7d) >= _sales_action_threshold(card):
        mechanisms.append("产品力强")
    if item.tag.value_points:
        mechanisms.append("内容表达强")
    if item.tag.element_tags:
        mechanisms.append("视觉差异强")
    if any(token in str(card.get("direction_name") or "") for token in ["盘发", "头盔", "整理"]):
        mechanisms.append("功能演示清晰")
    if not mechanisms:
        mechanisms.append("待人工确认")

    action = str(card.get("decision_action") or "")
    suggest_small_test = "是" if action in {"prioritize_low_cost_test", "cautious_test", "hidden_small_test"} else "否，先拆解"
    if action == "observe":
        suggest_small_test = "否，持续观察"
    if action == "avoid":
        suggest_small_test = "否，暂不跟进"
    non_replicable = []
    if age_days is not None and int(age_days) > 180:
        non_replicable.append("老链接权重不可复制")
    if _num(item.snapshot.creator_count) > 0:
        non_replicable.append("达人资源或内容沉淀未必可复制")
    if not non_replicable:
        non_replicable.append("需人工确认头部优势是否来自单一资源")
    return {
        "head_win_mechanism": _clean_list(mechanisms),
        "replicable_points": _clean_list(context.get("content_expression_points") or [])[:3],
        "non_replicable_points": non_replicable,
        "bypass_angle": str(context.get("differentiation_opportunity") or ""),
        "copyability_score": _copyability_score(item, age_days),
        "suggest_small_test": suggest_small_test,
    }


def _copyability_score(item: ScoredProductSnapshot, age_days: Optional[int]) -> str:
    score = 0
    if item.tag.value_points:
        score += 1
    if item.tag.element_tags:
        score += 1
    if age_days is not None and int(age_days) <= 180:
        score += 1
    if _num(item.snapshot.creator_count) <= 3:
        score += 1
    if score >= 3:
        return "高"
    if score >= 2:
        return "中"
    return "低"


def _should_mark_differentiation_candidate(row: Dict[str, Any], card: Dict[str, Any]) -> bool:
    sample_types = set(row.get("样本类型") or [])
    if "代表新品" in sample_types:
        return True
    if str(card.get("decision_action") or "") in {"prioritize_low_cost_test", "cautious_test", "hidden_small_test"}:
        return "老品占位头部" not in sample_types
    return False


def _is_few_new_winners_direction(card: Dict[str, Any]) -> bool:
    if str(card.get("primary_opportunity_type") or "") == "few_new_winners":
        return True
    signal = card.get("new_product_entry_signal") or {}
    if isinstance(signal, dict) and str(signal.get("type") or "") == "few_new_winners":
        return True
    return "few_new_winners" in set(card.get("risk_tags") or [])


def _is_dominant_form(card: Dict[str, Any], item: ScoredProductSnapshot) -> bool:
    top_forms = [str(value or "").strip() for value in list(card.get("top_forms") or []) if str(value or "").strip()]
    return bool(top_forms and str(item.tag.product_form or "").strip() == top_forms[0])


def _image_urls(item: ScoredProductSnapshot) -> List[str]:
    values = list(item.snapshot.product_images or [])
    if item.snapshot.image_url:
        values.insert(0, item.snapshot.image_url)
    for attachment in _image_attachments(item):
        url = str(attachment.get("url") or attachment.get("tmp_url") or "").strip()
        if url:
            values.append(url)
    results = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in results:
            results.append(text)
    return results


def _image_attachments(item: ScoredProductSnapshot) -> List[Dict[str, Any]]:
    values: List[Any] = []
    raw_product_images = getattr(item.snapshot, "raw_product_images", None)
    if raw_product_images not in (None, "", []):
        values.append(raw_product_images)
    raw_fields = dict(getattr(item.snapshot, "raw_fields", {}) or {})
    for key in ("图片", "商品图片", "商品主图", "产品图片", "主图"):
        value = raw_fields.get(key)
        if value not in (None, "", []):
            values.append(value)
    attachments: List[Dict[str, Any]] = []
    seen = set()
    for value in values:
        candidates = value if isinstance(value, list) else [value]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            file_token = str(candidate.get("file_token") or "").strip()
            if not file_token or file_token in seen:
                continue
            seen.add(file_token)
            attachment = {
                "file_token": file_token,
                "name": str(candidate.get("name") or file_token),
            }
            for optional_key in ("type", "size", "url", "tmp_url"):
                if candidate.get(optional_key) not in (None, ""):
                    attachment[optional_key] = candidate.get(optional_key)
            attachments.append(attachment)
    return attachments


def _age_days(item: ScoredProductSnapshot) -> Optional[int]:
    for value in (item.product_age_days, item.snapshot.product_age_days, item.snapshot.listing_days):
        if value is None or value == "":
            continue
        try:
            return max(0, int(float(value)))
        except (TypeError, ValueError):
            continue
    return None


def _price_cny(price_mid: Any, config: Optional[MarketInsightConfig]) -> Optional[float]:
    if price_mid is None:
        return None
    try:
        value = float(price_mid)
    except (TypeError, ValueError):
        return None
    if config and config.price_to_cny_rate and config.price_to_cny_rate > 0:
        return round(value * float(config.price_to_cny_rate), 4)
    divisor = float(getattr(config, "price_scale_divisor", 1.0) or 1.0) if config else 1.0
    return round(value / max(divisor, 1.0), 4)


def _price_band_confidence(card: Dict[str, Any]) -> str:
    analysis = card.get("price_band_analysis") or {}
    if not isinstance(analysis, dict):
        return ""
    recommended = analysis.get("recommended_price_band") or {}
    if isinstance(recommended, dict):
        return str(recommended.get("confidence") or analysis.get("price_band_confidence") or "")
    return str(analysis.get("price_band_confidence") or "")


def _sales_action_threshold(card: Dict[str, Any]) -> float:
    threshold = _nested_float(card, ["demand_structure", "sales_action_threshold"])
    return threshold if threshold is not None else 250.0


def _nested_float(payload: Dict[str, Any], path: List[str]) -> Optional[float]:
    value: Any = payload
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _density(count: Any, sales: Any) -> float:
    return _num(count) / max(_num(sales), 1.0)


def _num(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _raw_text(raw_fields: Dict[str, Any], keys: List[str]) -> str:
    for key in keys:
        value = raw_fields.get(key)
        if value not in (None, "", []):
            return str(value)
    return ""


def _raw_number(raw_fields: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for key in keys:
        value = raw_fields.get(key)
        if value in (None, "", []):
            continue
        text = str(value).replace(",", "")
        try:
            return float(text)
        except ValueError:
            continue
    return None


def _clean_list(values: Iterable[Any]) -> List[str]:
    results = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def rows_to_json(rows: List[Dict[str, Any]]) -> str:
    return json.dumps(rows, ensure_ascii=False, indent=2)
