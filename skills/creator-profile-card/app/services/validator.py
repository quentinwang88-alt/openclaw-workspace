"""
校验服务：枚举校验、置信度校验、证据校验、话术风险检查、低质量话术检测。

V1.1 新增：低质量话术判定规则（模板句检测、泛化形容词检测、按类目场景词检测）。
"""
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from ..config import (
    CONFIDENCE_AUTO_WRITE,
    CONFIDENCE_WRITE_WITH_REVIEW,
    MIN_COVERS_FULL_CONFIDENCE,
    MIN_COVERS_ANY_AUTO,
)


# ── 固定枚举 ──────────────────────────────────────────────

ENUM_ACTIVITY = {"高", "中", "低", "停更", ""}
ENUM_CONTENT_TYPE = {
    "穿搭", "妆发", "首饰试戴", "好物分享",
    "居家生活", "口播种草", "直播切片", "其他",
}
ENUM_VISUAL_STYLE = {
    "自拍近景", "镜前半身", "全身穿搭",
    "桌面展示", "家中生活流", "户外街拍", "直播间感",
}
ENUM_FIT_CATEGORIES = {
    "发饰", "耳饰", "项链", "围巾", "帽子",
    "轻上装", "女装", "暂无",
}
ENUM_ACTION = {"精准沟通", "半自动沟通", "暂缓", "放弃"}

# ── 话术风险词 ────────────────────────────────────────────

RISK_WORDS = [
    "我翻了你很多视频",
    "我看了你所有视频",
    "我一直关注你",
    "你肯定能卖爆",
    "保证出单",
    "一定会火",
    "最高佣金",
    "稳赚",
    "必须尽快回复",
    "你的粉丝一定喜欢",
    "保你爆单",
    "百分百出单",
    "绝对赚钱",
    "不赚你找我",
]


@dataclass
class ValidationResult:
    field_name: str
    value: Any
    is_valid: bool
    action: str  # "write" | "write_with_review" | "reject" | "skip"
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


def check_enum(value: str, allowed: set, field_name: str) -> ValidationResult:
    """检查值是否在固定枚举中。"""
    if value == "" or value is None:
        return ValidationResult(
            field_name=field_name, value=value, is_valid=True,
            action="skip", warnings=[f"{field_name} 为空，跳过写入"],
        )
    if value not in allowed:
        return ValidationResult(
            field_name=field_name, value=value, is_valid=False,
            action="reject",
            errors=[f"{field_name}='{value}' 不在允许的枚举 {allowed} 中"],
        )
    return ValidationResult(
        field_name=field_name, value=value, is_valid=True, action="write",
    )


def check_confidence(
    confidence: float,
    field_name: str,
    cover_count: int = 20,
) -> ValidationResult:
    """检查置信度是否达到阈值。"""
    effective_auto_threshold = CONFIDENCE_AUTO_WRITE
    effective_warn_threshold = CONFIDENCE_WRITE_WITH_REVIEW

    if cover_count < MIN_COVERS_ANY_AUTO:
        return ValidationResult(
            field_name=field_name, value=confidence, is_valid=True,
            action="reject",
            warnings=[f"封面数({cover_count})<{MIN_COVERS_ANY_AUTO}，{field_name} 需人工复核"],
        )

    if cover_count < MIN_COVERS_FULL_CONFIDENCE:
        effective_auto_threshold = min(0.75, CONFIDENCE_AUTO_WRITE)

    if confidence >= effective_auto_threshold:
        return ValidationResult(
            field_name=field_name, value=confidence, is_valid=True, action="write",
        )
    elif confidence >= effective_warn_threshold:
        return ValidationResult(
            field_name=field_name, value=confidence, is_valid=True,
            action="write_with_review",
            warnings=[f"{field_name} 置信度({confidence})在 [{effective_warn_threshold}, {effective_auto_threshold})，标记人工复核"],
        )
    else:
        return ValidationResult(
            field_name=field_name, value=confidence, is_valid=True,
            action="reject",
            warnings=[f"{field_name} 置信度({confidence})<{effective_warn_threshold}，不自动写入"],
        )


def check_evidence(
    text: str,
    field_name: str,
    min_cover_refs: int = 3,
) -> ValidationResult:
    """检查证据中是否引用了足够多的封面编号。"""
    if not text or not text.strip():
        return ValidationResult(
            field_name=field_name, value=text, is_valid=False,
            action="reject",
            errors=[f"{field_name} 缺少证据引用"],
        )

    import re
    numbers = set()
    for m in re.finditer(r'(\d+)', text):
        num = int(m.group(1))
        if 1 <= num <= 20:
            numbers.add(num)

    if len(numbers) < min_cover_refs:
        return ValidationResult(
            field_name=field_name, value=text, is_valid=True,
            action="write_with_review",
            warnings=[f"{field_name} 只引用了 {len(numbers)} 个封面编号，要求至少 {min_cover_refs} 个"],
        )

    return ValidationResult(
        field_name=field_name, value=text, is_valid=True, action="write",
    )


def check_fit_categories(
    categories: List[Dict[str, Any]],
    field_name: str = "适配类目",
) -> ValidationResult:
    """检查适配类目。"""
    if not categories:
        return ValidationResult(
            field_name=field_name, value=categories, is_valid=True, action="skip",
        )

    values = [c.get("value", "") for c in categories if c.get("value")]
    if len(values) > 2:
        return ValidationResult(
            field_name=field_name, value=values, is_valid=False,
            action="reject",
            errors=[f"适配类目最多 2 个，AI 返回了 {len(values)} 个: {values}"],
        )

    errors = []
    for v in values:
        if v not in ENUM_FIT_CATEGORIES:
            errors.append(f"适配类目 '{v}' 不在允许的枚举中")

    if errors:
        return ValidationResult(
            field_name=field_name, value=values, is_valid=False,
            action="reject", errors=errors,
        )

    return ValidationResult(
        field_name=field_name, value=values, is_valid=True, action="write",
    )


def check_risk_words(text: str) -> List[str]:
    """检查文本中的风险词，返回命中的风险词列表。"""
    hits = []
    for word in RISK_WORDS:
        if word in text:
            hits.append(word)
    return hits


def check_message_risk(
    message_cn: str,
    message_local: str = "",
) -> Dict[str, Any]:
    """检查私信话术风险。"""
    risk_check = {
        "has_overpromise": False,
        "has_monitoring_feeling": False,
        "uses_unprovided_policy": False,
    }

    overpromise_words = [
        "爆单", "必出单", "保证", "一定会火", "肯定能卖", "稳赚",
        "百分百", "绝对", "不赚你找我",
    ]
    monitoring_words = [
        "我翻了你很多视频", "我看了你所有视频", "我一直关注你",
        "我每天都在看", "我跟踪了你的",
    ]

    combined = f"{message_cn}\n{message_local}"
    for w in overpromise_words:
        if w in combined:
            risk_check["has_overpromise"] = True
            break
    for w in monitoring_words:
        if w in combined:
            risk_check["has_monitoring_feeling"] = True
            break

    return risk_check


# ── V1.1 低质量话术判定 ────────────────────────────────────

# 高风险模板句式（正则匹配）
TEMPLATE_PATTERNS = [
    r"看到你[^，。]*，想和[^，。]*合作",
    r"想邀请你[^，。]*推广",
    r"想和您洽谈[^，。]*可能性",
    r"相关品类[^，。]*合作",
    r"相关产品[^，。]*推广",
    r"合作意向",
    r"合作可能性",
    r"想和你沟通[^，。]*合作",
    r"期待和[^，。]*沟通[^，。]*合作",
    r"邀请你试穿我们的[^，。]*$",  # 以"邀请你试穿我们的XXX"结束，没有拍摄场景
]

# 泛化形容词（可以作为辅助但不能作为主要依据）
GENERIC_ADJECTIVES = [
    "很有质感",
    "很受欢迎",
    "很有个人特色",
    "清新接地气",
    "日常休闲风",
    "风格很好",
]

# 按类目划分的必要场景词
SCENE_WORDS_BY_CATEGORY = {
    "女装": {
        "出门前", "空调房", "通勤", "镜前", "试穿",
        "上身效果", "一衣多搭", "遮手臂", "遮肚子",
        "修饰身形", "居家也能穿", "出门也能穿",
    },
    "轻上装": {
        "出门前", "空调房", "通勤", "镜前", "试穿",
        "上身效果", "一衣多搭", "遮手臂", "遮肚子",
        "修饰身形", "居家也能穿", "出门也能穿",
    },
    "发饰": {
        "整理发型", "半扎发", "出门前", "戴头盔后",
        "约会前", "通勤妆发", "近景试戴",
    },
    "耳饰": {
        "近景试戴", "侧脸", "通勤搭配", "一套衣服换配饰",
        "日常妆容", "镜前搭配",
    },
    "项链": {
        "近景试戴", "通勤搭配", "一套衣服换配饰",
        "镜前搭配", "日常妆容",
    },
    "围巾": {
        "出门前", "通勤", "镜前", "试穿",
        "一衣多搭", "居家也能穿", "出门也能穿",
    },
    "帽子": {
        "出门前", "通勤", "户外街拍", "镜前搭配",
    },
    "好物分享": {
        "桌面展示", "居家生活", "出门前", "随手拍",
    },
}


def _check_template_patterns(text: str) -> List[str]:
    """检查文本是否命中模板句式。返回命中的句式列表。"""
    import re
    hits = []
    for pattern in TEMPLATE_PATTERNS:
        if re.search(pattern, text):
            hits.append(pattern)
    return hits


def _check_generic_adjectives(text: str) -> List[str]:
    """检查文本中的泛化形容词。返回命中的词列表。"""
    hits = []
    for word in GENERIC_ADJECTIVES:
        if word in text:
            hits.append(word)
    return hits


def _check_scene_words(text: str, categories: List[str]) -> bool:
    """检查文本是否至少包含一个与类目匹配的场景词。"""
    all_scene_words: set = set()
    for cat in categories:
        if cat in SCENE_WORDS_BY_CATEGORY:
            all_scene_words.update(SCENE_WORDS_BY_CATEGORY[cat])
    if not all_scene_words:
        return True  # 没有规则则不检测
    for sw in all_scene_words:
        if sw in text:
            return True
    return False


def calculate_message_quality(
    message_cn: str,
    message_local: str = "",
    content_opportunity: Optional[Dict[str, Any]] = None,
    product_category: str = "",
) -> Dict[str, Any]:
    """V1.1 低质量话术自动评分 + 判定。

    满分 10 分，低于 8 分需重写。

    Args:
        message_cn: 中文运营参考版。
        message_local: 目标语言私信版。
        content_opportunity: 内容机会卡 dict。
        product_category: 商品类目。

    Returns:
        {
            "quality_score": int,
            "breakdown": {...},
            "too_general": bool,
            "issues": [...],
            "passed": bool (≥8),
        }
    """
    combined = f"{message_cn}\n{message_local}"
    opp = content_opportunity or {}

    breakdown = {
        "specific_observation": 0,  # 2 分
        "product_specificity": 0,   # 2 分
        "shooting_scene": 0,        # 2 分
        "creator_benefit": 0,       # 1 分
        "low_pressure_cta": 0,      # 1 分
        "non_template": 0,          # 1 分
        "risk_control": 0,          # 1 分
    }
    issues: List[str] = []

    # 1. 具体观察 (2 分)
    obs_detail = opp.get("observable_detail", {}).get("value", "")
    obs_evidence = opp.get("observable_detail", {}).get("evidence", "")
    if obs_evidence and "第" in obs_evidence:
        breakdown["specific_observation"] += 1
    if obs_detail and any(kw in combined for kw in ["镜前", "半身", "穿搭", "全身", "近景", "家中", "户外", "宽松", "大码", "休闲"]):
        breakdown["specific_observation"] += 1
    if breakdown["specific_observation"] == 0:
        issues.append("缺少对达人视频封面的具体画面观察")

    # 2. 商品具体度 (2 分)
    specific_keywords = ["薄开衫", "宽松上衣", "轻防晒", "大码套装", "日常衬衫", "珍珠发夹",
                         "开衫", "薄外套", "短袖", "长袖", "衬衫", "套头", "发夹", "耳环",
                         "项链", "围巾", "帽", "套装"]
    if any(kw in combined for kw in specific_keywords):
        breakdown["product_specificity"] += 1
    if opp.get("product_fit_reason", {}).get("value", ""):
        breakdown["product_specificity"] += 1
    if breakdown["product_specificity"] == 0:
        issues.append("商品描述太泛（只说'女装''轻上装'等类目词）")

    # 3. 拍摄场景 (2 分)
    categories = [product_category] if product_category else []
    has_scene = _check_scene_words(combined, categories)
    if opp.get("recommended_shooting_scene", {}).get("value", ""):
        breakdown["shooting_scene"] += 1
    if has_scene:
        breakdown["shooting_scene"] += 1
    if breakdown["shooting_scene"] == 0:
        issues.append("缺少低成本拍摄场景建议")

    # 4. 达人收益感 (1 分)
    benefit_keywords = ["不用复杂", "随手拍", "顺手", "简单", "轻松", "方便", "不费", "不复杂", "自然"]
    if any(kw in combined for kw in benefit_keywords):
        breakdown["creator_benefit"] = 1
    else:
        issues.append("未体现对达人创作负担的降低")

    # 5. 低压力 CTA (1 分)
    cta_keywords = ["如果", "你觉得", "感兴趣", "发你款式", "发你看看", "合适的话", "看看", "参考"]
    if any(kw in combined for kw in cta_keywords):
        breakdown["low_pressure_cta"] = 1
    else:
        issues.append("缺少低压力 CTA（如'如果你觉得合适，我发你款式'）")

    # 6. 非模板化 (1 分)
    template_hits = _check_template_patterns(combined)
    generic_hits = _check_generic_adjectives(combined)
    if not template_hits and len(generic_hits) <= 1:
        breakdown["non_template"] = 1
    else:
        issues.append(f"命中模板句式或泛化词: {template_hits + generic_hits}")

    # 7. 风险控制 (1 分)
    risk = check_message_risk(message_cn, message_local)
    has_risk = any(risk.values())
    if not has_risk:
        breakdown["risk_control"] = 1
    else:
        issues.append(f"风险检查未通过: {[k for k, v in risk.items() if v]}")

    quality_score = sum(breakdown.values())
    too_general = (
        bool(template_hits) or
        (breakdown["specific_observation"] == 0 and breakdown["shooting_scene"] == 0)
    )

    return {
        "quality_score": quality_score,
        "breakdown": breakdown,
        "too_general": too_general,
        "template_hits": template_hits,
        "generic_hits": generic_hits,
        "issues": issues,
        "passed": quality_score >= 7,
    }


def get_max_rewrite_count() -> int:
    """返回最大重写次数。"""
    return 2


# ── V1.1 关系维护话术质量评分 ──

def calculate_maintenance_quality(
    message_cn: str,
    message_local: str = "",
    history_relation: str = "",
) -> Dict[str, Any]:
    """V1.1 关系维护话术自动评分。

    满分 10 分，低于 8 分需重写。

    Args:
        message_cn: 中文运营参考版。
        message_local: 目标语言私信版。
        history_relation: 历史关系（用于判断开场是否合理）。

    Returns:
        {
            "quality_score": int,
            "breakdown": {...},
            "issues": [...],
            "passed": bool (≥8),
        }
    """
    combined = f"{message_cn}\n{message_local}"

    breakdown = {
        "real_observation": 0,       # 2 分
        "no_sales_purpose": 0,       # 2 分
        "relationship_natural": 0,   # 2 分
        "promise_better_match": 0,   # 1.5 分
        "low_pressure_close": 0,     # 1 分
        "no_creepy_no_flatter": 0,   # 1 分
        "natural_tone": 0,           # 0.5 分
    }
    issues: List[str] = []

    # 1. 有真实公开内容观察 (2 分)
    observation_keywords = ["镜前", "半身", "穿搭", "全身", "近景", "居家", "好物", "视频",
                            "封面", "内容", "账号", "更新"]
    if any(kw in combined for kw in observation_keywords):
        breakdown["real_observation"] = 2
    elif len(combined.strip()) > 20:
        breakdown["real_observation"] = 1
        issues.append("缺少对达人内容的真实观察")
    else:
        issues.append("话术太短，缺少内容观察")

    # 2. 无带货目的感 (2 分)
    product_words = ["发饰", "耳饰", "项链", "开衫", "上衣", "套装", "佣金", "寄样", "带货",
                     "推广", "种草", "试穿", "款式", "商品", "SKU"]
    has_product = any(w in combined for w in product_words)
    if not has_product:
        breakdown["no_sales_purpose"] = 2
    else:
        issues.append(f"检测到商品相关词汇，关系维护不应推品")

    # 3. 关系修复自然 (2 分)
    relationship_keywords = ["支持", "感谢", "后续", "匹配", "贴合", "适合你", "打招呼",
                              "问候", "节奏", "调性", "账号"]
    if any(kw in combined for kw in relationship_keywords):
        breakdown["relationship_natural"] = 2
    elif len(combined) > 30:
        breakdown["relationship_natural"] = 1
        issues.append("关系维护语气不够自然友好")
    else:
        issues.append("关系维护太短，缺乏温度")

    # 4. 表达后续精准匹配 (1.5 分)
    better_match_keywords = ["更精准", "更认真", "更贴合", "不适合", "不合适的", "合适", "特别贴合"]
    if any(kw in combined for kw in better_match_keywords):
        breakdown["promise_better_match"] = 1.5
    else:
        breakdown["promise_better_match"] = 0
        # 不扣光，给 0.5 如果至少表达了后续会注意
        if any(kw in combined for kw in ["后续", "以后", "之后", "后面"]):
            breakdown["promise_better_match"] = 0.5
        else:
            issues.append("未表达后续会更精准匹配")

    # 5. 低压力结尾 (1 分)
    low_pressure_keywords = ["不用特意回复", "不用回复", "不用回", "不着急", "轻松",
                              "随便", "随意", "没事", "不用麻烦"]
    if any(kw in combined for kw in low_pressure_keywords):
        breakdown["low_pressure_close"] = 1
    elif any(kw in combined for kw in ["问候", "打招呼", "say hi"]):
        breakdown["low_pressure_close"] = 0.5
    else:
        issues.append("缺少明确的低压力结尾")

    # 6. 无监控感/无尬夸 (1 分)
    monitoring_words = ["我翻了你很多视频", "我看了你所有视频", "我一直关注你",
                         "我每天都在看", "我跟踪了你"]
    flattery_words = ["很有质感", "很受欢迎", "很有个人特色", "清新接地气"]
    has_monitoring = any(w in combined for w in monitoring_words)
    has_flattery = sum(1 for w in flattery_words if w in combined)
    if not has_monitoring and has_flattery <= 1:
        breakdown["no_creepy_no_flatter"] = 1
    elif has_monitoring:
        issues.append("检测到监控感表达")
    else:
        breakdown["no_creepy_no_flatter"] = 0.5

    # 7. 语气自然 (0.5 分)
    formal_words = ["洽谈", "合作意向", "相关推广", "相关产品", "期待尽快"]
    has_formal = any(w in combined for w in formal_words)
    if not has_formal and "～" in combined or "～" in message_cn:
        breakdown["natural_tone"] = 0.5
    elif not has_formal:
        breakdown["natural_tone"] = 0.5
    else:
        issues.append("语气偏商务，不够自然")

    quality_score = sum(breakdown.values())

    return {
        "quality_score": quality_score,
        "breakdown": breakdown,
        "issues": issues,
        "passed": quality_score >= 7,
    }


def check_follow_up_risk(
    message_cn: str,
    message_local: str = "",
) -> Dict[str, Any]:
    """V1.1 轻跟进话术风险检查。

    Returns:
        {
            "has_pressure": bool,
            "has_urgency": bool,
            "has_new_offer_bait": bool,
            "has_monitoring_feeling": bool,
            "issues": [...],
        }
    """
    combined = f"{message_cn}\n{message_local}"
    issues = []

    # 施压表达
    pressure_words = ["尽快", "急", "限时", "马上", "快点", "赶紧", "必须", "一定"]
    has_pressure = any(w in combined for w in pressure_words)
    if has_pressure:
        issues.append("检测到施压表达")

    # 催促
    urgency_words = ["还没收到你回复", "请尽快回复", "麻烦回复", "回复一下"]
    has_urgency = any(w in combined for w in urgency_words)
    if has_urgency:
        issues.append("检测到催促表达")

    # 追加利益施压
    bait_words = ["又上了新的", "新款", "佣金更高", "优惠更大", "限时优惠", "折扣"]
    has_bait = any(w in combined for w in bait_words)
    if has_bait:
        issues.append("检测到追加利益施压")

    # 监控感
    monitoring_words = ["我翻了你很多视频", "我看了你所有视频", "我一直关注你"]
    has_monitoring = any(w in combined for w in monitoring_words)

    return {
        "has_pressure": has_pressure,
        "has_urgency": has_urgency,
        "has_new_offer_bait": has_bait,
        "has_monitoring_feeling": has_monitoring,
        "issues": issues,
        "passed": not any([has_pressure, has_urgency, has_bait, has_monitoring]),
    }


def validate_profile_output(
    raw_output: Dict[str, Any],
    cover_count: int = 20,
    has_publish_time: bool = True,
    has_product_pool: bool = False,
) -> Tuple[Dict[str, Any], List[ValidationResult], bool]:
    """全面校验 AI 画像输出。

    Returns:
        (可写入的字段 dict, 校验结果列表, 是否需要人工复核)
    """
    results: List[ValidationResult] = []
    writable: Dict[str, Any] = {}
    manual_review = False

    # 1. 活跃度
    act_val = (raw_output.get("activity") or {}).get("value", "")
    act_conf = (raw_output.get("activity") or {}).get("confidence", 0)
    act_ev = (raw_output.get("activity") or {}).get("evidence", "")

    r = check_enum(act_val, ENUM_ACTIVITY, "活跃度")
    results.append(r)
    if not has_publish_time:
        results.append(ValidationResult(
            field_name="活跃度", value=act_val, is_valid=True,
            action="skip", warnings=["无发布时间信息，活跃度留空"],
        ))
    elif r.action in ("write", "write_with_review"):
        r_conf = check_confidence(act_conf, "活跃度", cover_count)
        results.append(r_conf)
        if r_conf.action in ("write", "write_with_review"):
            writable["活跃度"] = act_val
            if r_conf.action == "write_with_review":
                manual_review = True

    # 2. 内容类型
    ct_val = (raw_output.get("content_type") or {}).get("value", "")
    ct_conf = (raw_output.get("content_type") or {}).get("confidence", 0)
    ct_ev = (raw_output.get("content_type") or {}).get("evidence", "")

    r = check_enum(ct_val, ENUM_CONTENT_TYPE, "内容类型")
    results.append(r)
    if r.action != "reject":
        r_conf = check_confidence(ct_conf, "内容类型", cover_count)
        results.append(r_conf)
        r_ev = check_evidence(ct_ev, "内容类型证据")
        results.append(r_ev)
        if r_conf.action in ("write", "write_with_review") and r_ev.action != "reject":
            writable["内容类型"] = ct_val
            if r_conf.action == "write_with_review" or r_ev.action == "write_with_review":
                manual_review = True
    else:
        manual_review = True

    # 3. 画面风格
    vs_val = (raw_output.get("visual_style") or {}).get("value", "")
    vs_conf = (raw_output.get("visual_style") or {}).get("confidence", 0)
    vs_ev = (raw_output.get("visual_style") or {}).get("evidence", "")

    r = check_enum(vs_val, ENUM_VISUAL_STYLE, "画面风格")
    results.append(r)
    if r.action != "reject":
        r_conf = check_confidence(vs_conf, "画面风格", cover_count)
        results.append(r_conf)
        r_ev = check_evidence(vs_ev, "画面风格证据")
        results.append(r_ev)
        if r_conf.action in ("write", "write_with_review") and r_ev.action != "reject":
            writable["画面风格"] = vs_val
            if r_conf.action == "write_with_review" or r_ev.action == "write_with_review":
                manual_review = True
    else:
        manual_review = True

    # 4. 适配类目
    fc = raw_output.get("fit_categories", [])
    r = check_fit_categories(fc)
    results.append(r)
    if r.action in ("write", "write_with_review"):
        fc_values = [c.get("value", "") for c in fc if c.get("value")]
        writable["适配类目"] = fc_values
        # 检查每个类目的置信度
        for c in fc:
            if c.get("confidence", 0) < CONFIDENCE_WRITE_WITH_REVIEW:
                manual_review = True
    elif r.action == "reject":
        manual_review = True

    # 5. 推荐商品/品类
    rp = raw_output.get("recommended_product_or_category") or {}
    rp_val = rp.get("value", "")
    rp_conf = rp.get("confidence", 0)

    if rp_val:
        if not has_product_pool and any(kw in rp_val for kw in ["SKU", "sku", "货号"]):
            results.append(ValidationResult(
                field_name="推荐商品/品类", value=rp_val, is_valid=False,
                action="reject",
                errors=["无商品候选池但推荐了具体 SKU，已拦截"],
            ))
            manual_review = True
        else:
            r_conf = check_confidence(rp_conf, "推荐商品/品类", cover_count)
            results.append(r_conf)
            if r_conf.action in ("write", "write_with_review"):
                writable["推荐商品/品类"] = rp_val
                if r_conf.action == "write_with_review":
                    manual_review = True

    # 6. 沟通切入点
    ca = raw_output.get("communication_angle") or {}
    ca_val = ca.get("value", "")
    ca_conf = ca.get("confidence", 0)

    if ca_val:
        risks = check_risk_words(ca_val)
        if risks:
            results.append(ValidationResult(
                field_name="沟通切入点", value=ca_val, is_valid=False,
                action="reject",
                errors=[f"命中风险词: {risks}"],
            ))
            manual_review = True
        else:
            r_conf = check_confidence(ca_conf, "沟通切入点", cover_count)
            results.append(r_conf)
            if r_conf.action in ("write", "write_with_review"):
                writable["沟通切入点"] = ca_val
                if r_conf.action == "write_with_review":
                    manual_review = True

    # 7. 建议动作（仅记录，不写入当前动作字段）
    sa = raw_output.get("suggested_action") or {}
    sa_val = sa.get("value", "")
    r = check_enum(sa_val, ENUM_ACTION, "建议动作")
    results.append(r)

    # 8. AI 自身标记
    if raw_output.get("manual_review_required"):
        manual_review = True
        reasons = raw_output.get("manual_review_reasons", [])
        results.append(ValidationResult(
            field_name="AI 自检", value=reasons, is_valid=True,
            action="write_with_review",
            warnings=[f"AI 标记需人工复核: {reasons}"],
        ))

    return writable, results, manual_review


# ── 样品批前沟通质量评分 ──

def calculate_sample_nurture_quality(
    message_cn: str, message_local: str = "",
    content_mode: str = "", applied_product: str = "",
    content_type: str = "", visual_style: str = "",
    has_live_evidence: bool = False,
) -> Dict[str, Any]:
    cn_len = len(message_cn)
    combined = f"{message_cn}\n{message_local}"
    issues = []
    b = dict.fromkeys(["product_short", "opening", "style", "selling", "mode", "human", "cta", "risk"], 0.0)

    b["product_short"] = 1 if len((applied_product or "").replace(" ", "")) <= 16 else 0.5
    b["opening"] = 1 if any(w in message_cn[:10] for w in ["看到你", "这款", "你申请", "这个款"]) else 0.5

    natural = ["你平时", "你的内容", "你常用", "你的穿搭", "你的日常"]
    analytical = ["真实上身效果", "展示得特别", "呈现得很清晰", "超级适配", "高度匹配"]
    b["style"] = 1.5 if any(w in combined for w in natural) and not any(w in combined for w in analytical) else 1.0

    effect = ["显高", "显瘦", "显比例", "显脸小", "三七分", "小个子", "遮肉"]
    if any(w in combined for w in effect):
        issues.append("效果承诺"); b["selling"] = 0
    elif any(w in combined for w in ["版型", "设计", "细节", "面料", "撞色", "V领", "翻领", "袖口", "短款", "蝴蝶结", "搭配"]):
        b["selling"] = 1.5
    elif any(w in combined for w in ["可以拍", "展示", "试穿"]):
        b["selling"] = 1.0

    if content_mode == "直播" and not has_live_evidence:
        b["mode"] = 0 if any(w in combined for w in ["你直播里", "直播时", "你直播间", "看到你直播"]) else (issues.append("无直播证据但假装看过"), 1.0)[1] if not any(w in combined for w in ["如果你", "放进直播"]) else 1.5
    else:
        b["mode"] = 1.5

    a_hits = sum(1 for w in ["超级适配", "高度匹配", "特别清楚", "特别清晰", "适配你的", "给你列几个", "麻烦告知", "整体搭配效果"] if w in combined)
    h_hits = sum(1 for w in ["我觉得", "看起来", "感觉", "应该", "不用改", "你原来的", "我帮你", "我这边"] if w in combined)
    if a_hits > 0:
        b["human"] = max(0, 1.0 - a_hits * 0.5); issues.append("偏分析感")
    elif h_hits >= 2: b["human"] = 2.0
    elif h_hits >= 1: b["human"] = 1.5
    else: b["human"] = 1.0

    b["cta"] = 1 if any(w in combined for w in ["我帮你", "我这边", "优先安排", "你觉得", "不用急"]) else 0.5
    risk = sum(1 for w in ["保证出单", "一定会火", "爆款", "稳赚", "必须发布", "我翻了你", "看了你所有", "封面", "截图", "分析了你的"] if w in combined)
    b["risk"] = 0.5 if risk == 0 else (issues.append("风险词"), 0)[1]
    if cn_len > 150: b["human"] = min(b["human"], 0.5); issues.append(f"过长({cn_len}字)")

    score = round(sum(b.values()), 1)
    return {"quality_score": score, "breakdown": b, "issues": issues, "passed": score >= 8 and b["human"] >= 1.0}
