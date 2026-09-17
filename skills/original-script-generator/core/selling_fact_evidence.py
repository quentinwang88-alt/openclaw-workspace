"""One fact-evidence record per *selected* selling argument (plan §4 / C1).

方案原文（节选）：

> C1：事实适配。每个所选卖点保留原文、来源、商品图片版本、事实类型、适配结论。
> 未知部件仅排除依赖该部件的主题；明确冲突需解决，不让生成模型自行选择一方。
> 人工运营输入提供意图，不自动构成真实体验记录。
> 外观与装饰事实：原图或可靠规格支持。
> 审美感受："我喜欢这种层次""看起来柔和"可以自然表达。
> 搭配建议：与实际冻结发型／穿搭兼容，不假装已经展示多个场景。
> 性能、材质、佩戴体验：需要相应来源；外观看起来轻盈不等于重量轻或久戴舒适。
> "我试了一天""随手一夹就完成"：需要真实体验／过程依据，不由生成画面反向证明商品性能。

这个模块只管一件事：拿到一个已经选中的卖点，说清它的**事实类型**、**来源够不够**、
**允许说到什么程度**，以及**商品图片版本是哪一版**。刻意保持增量：

* import 无副作用、不碰数据库。
* 记录本身是纯新增字段，任何既有调用方的取值都不变。
* 唯一会**过滤**候选的 ``eligible`` 只在
  ``ORIGINAL_SCRIPT_SELLING_FACT_EVIDENCE_ENABLED`` 打开时被消费，默认关。

分级词表、档位、结论取值都只在本模块定义，分类方与消费方不会各自漂移。

关于部件：关键词命中只能**报告**"这句话提到了某个部件"，不足以断言"这个主题依赖
该部件"。所以只有两种情况会因部件排除候选——已确认结构**明确否证**该部件（明确冲突，
必须解决），或上游把该部件声明为**必要依赖**且它尚未确认。其余情形只记录、不拦。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


SELLING_FACT_EVIDENCE_ENV = "ORIGINAL_SCRIPT_SELLING_FACT_EVIDENCE_ENABLED"
SELLING_FACT_EVIDENCE_SCHEMA = "selling-fact-evidence-v1"

_TRUE_TOKENS = {"1", "true", "yes", "on"}

# ── 事实类型：方案 C1 的五条一一对应 ────────────────────────────────────
TIER_APPEARANCE = "APPEARANCE_FACT"
TIER_AESTHETIC = "AESTHETIC_PREFERENCE"
TIER_STYLING = "STYLING_SUGGESTION"
TIER_PERFORMANCE = "PRODUCT_PERFORMANCE"
TIER_EXPERIENCE = "PERSONAL_EXPERIENCE"
FACT_EVIDENCE_TIERS: Tuple[str, ...] = (
    TIER_EXPERIENCE,
    TIER_PERFORMANCE,
    TIER_APPEARANCE,
    TIER_STYLING,
    TIER_AESTHETIC,
)
#: 越靠前风险越高；一条原文命中多档时以最高档为准（口径由最严的那档决定）。
TIER_RISK_ORDER: Dict[str, int] = {
    tier: index for index, tier in enumerate(FACT_EVIDENCE_TIERS)
}

TIER_REQUIREMENTS: Dict[str, str] = {
    TIER_APPEARANCE: "PRODUCT_IMAGE_OR_SPEC",
    TIER_AESTHETIC: "NONE",
    TIER_STYLING: "COMPATIBLE_PLANNED_LOOK",
    TIER_PERFORMANCE: "SPEC_OR_TEST_SOURCE",
    TIER_EXPERIENCE: "CONFIRMED_EXPERIENCE",
}
TIER_LABELS_ZH: Dict[str, str] = {
    TIER_APPEARANCE: "外观与装饰事实",
    TIER_AESTHETIC: "审美感受",
    TIER_STYLING: "搭配建议",
    TIER_PERFORMANCE: "性能·材质·佩戴体验",
    TIER_EXPERIENCE: "真实使用体验",
}

# ── 体验授权：与 ``content_branches/organic_seeding`` 同取值域 ──────────
EXPERIENCE_NONE = "NONE"
EXPERIENCE_CURRENT_OBSERVATION = "CURRENT_OBSERVATION"
EXPERIENCE_OPERATOR_CONFIRMED_HISTORY = "OPERATOR_CONFIRMED_HISTORY"
EXPERIENCE_AUTHORITIES: Tuple[str, ...] = (
    EXPERIENCE_CURRENT_OBSERVATION,
    EXPERIENCE_OPERATOR_CONFIRMED_HISTORY,
    EXPERIENCE_NONE,
)

# ── 适配结论 ────────────────────────────────────────────────────────────
VERDICT_ADAPTED = "ADAPTED"
VERDICT_CEILING = "ADAPTED_WITH_CEILING"
VERDICT_NEEDS_SOURCE = "NEEDS_SOURCE"
VERDICT_UNKNOWN_PART = "EXCLUDED_UNKNOWN_PART"
VERDICT_CONFLICT = "EXCLUDED_UNRESOLVED_CONFLICT"
FACT_EVIDENCE_VERDICTS: Tuple[str, ...] = (
    VERDICT_ADAPTED,
    VERDICT_CEILING,
    VERDICT_NEEDS_SOURCE,
    VERDICT_UNKNOWN_PART,
    VERDICT_CONFLICT,
)
#: 只有这两种结论会让候选不参选（且仅在开关打开时被消费）。
BLOCKING_VERDICTS: Tuple[str, ...] = (VERDICT_UNKNOWN_PART, VERDICT_CONFLICT)

# ── 部件三态：复用 ``accessory_mixed_templates`` 的既有取值域 ───────────
PART_STATE_VERIFIED = "VERIFIED"
PART_STATE_ABSENT = "ABSENT"
PART_STATE_UNKNOWN = "UNKNOWN"

# ── 判定词表（单一来源；全部是"措辞里出现了这类主张"的线索，不是事实判定）──
#
# 词表只决定"这句话属于哪一档"，不决定"这句话真假"。真假由来源字段、商品图片
# 版本和部件三态决定——把两者混在一起就是拿关键词当事实。

_EXPERIENCE_TERMS: Tuple[str, ...] = (
    "试了", "试过", "试用", "亲测", "实测", "用了一天", "用了很久", "用了一段时间",
    "一直在用", "回购", "每次", "每天戴", "随手一夹", "一夹完成", "一夹就",
    "夹一下就", "一戴就", "省步骤", "无需繁琐", "不用繁琐", "轻松搞定", "秒变",
    "我试", "戴着睡", "不夹头皮",
)
_EXPERIENCE_TERMS_EN: Tuple[str, ...] = (
    "i tried", "i've used", "wore it for", "use it every day", "repurchased",
    "one clip", "instantly", "effortless",
)

_PERFORMANCE_TERMS: Tuple[str, ...] = (
    "很轻", "轻巧", "不重", "重量", "分量", "不勒", "不夹", "舒适", "透气",
    "牢固", "稳固", "不易掉", "不掉", "防滑", "久戴", "长时间戴", "可调节", "弹性",
    "耐用", "抗过敏", "不过敏",
    # 夹持力／发量适配：这类句子读起来像"适合什么发质"，实质是商品性能主张
    # （"发量多也能稳稳夹住"= 夹持力）。真实目录里就这么写，漏掉它会掉进兜底的
    # 审美档而被当成可以自然表达的喜好。
    "稳稳", "夹住", "夹得", "夹牢", "牢牢", "抓力", "发量",
)
_PERFORMANCE_TERMS_EN: Tuple[str, ...] = (
    "lightweight", "featherlight", "comfortable", "breathable", "non-slip",
    "durable", "all-day", "hypoallergenic", "holds", "grip", "stays put",
)

#: 材质／工艺断言：外观看起来是什么 ≠ 它就是什么材质。命中后若没有可核对的图片
#: 版本也没有规格来源，只能保留"看起来／呈现出"的观感，不能写成材质事实。
_MATERIAL_TERMS: Tuple[str, ...] = (
    "纱", "雪纺", "真丝", "丝绸", "缎", "棉", "麻", "针织", "绒", "皮", "金属",
    "合金", "钛", "银", "不锈钢", "水晶", "珍珠", "贝珠", "亚克力", "树脂", "醋酸",
    "哑光", "珐琅", "电镀", "镶钻",
)
_MATERIAL_TERMS_EN: Tuple[str, ...] = (
    "gauze", "chiffon", "silk", "satin", "cotton", "leather", "metal", "alloy",
    "titanium", "silver", "crystal", "pearl", "acetate", "resin", "rhinestone",
)

#: 单字材质词被删掉后常留下病句（"双层纱质蝴蝶造型" → "双层质蝴蝶造型"），因为
#: 被禁的断言是"纱质"而不是那个字。删除时把这些紧邻的限定成分一起带走。
#: 只删不换 —— 删完不会生成原文里没有的说法。
_MATERIAL_TRAILING: Tuple[str, ...] = ("质", "感", "料", "面料", "材质", "质地")

_APPEARANCE_TERMS: Tuple[str, ...] = (
    "造型", "形状", "轮廓", "弧形", "层次", "双层", "单层", "立体", "镂空", "浮雕",
    "纹理", "排列", "大小", "比例", "色", "花朵", "蝴蝶", "珠", "几何", "对称",
    "亮面", "雾面", "半透明", "透光", "光泽",
)
_APPEARANCE_TERMS_EN: Tuple[str, ...] = (
    "shape", "silhouette", "layered", "double-layer", "texture", "matte",
    "glossy", "translucent", "gradient", "cut-out",
)

_STYLING_TERMS: Tuple[str, ...] = (
    "搭配", "百搭", "适合", "场合", "场景", "通勤", "日常", "约会", "度假", "旅行",
    "拍照", "婚礼", "伴娘", "上班", "发型", "丸子头", "马尾", "高颅顶", "盘发",
    "刘海", "半扎",
)
_STYLING_TERMS_EN: Tuple[str, ...] = (
    "outfit", "pair with", "goes with", "occasion", "everyday", "commute",
    "date", "vacation", "travel", "wedding", "hairstyle", "bun", "ponytail",
)

#: 只有真正的**使用场景**才能算"堆叠"。造型词（发型/马尾/高颅顶）和搭配动词
#: （适合/百搭）单独出现不是多场景，把它们数进去会误报。
_SCENE_TERMS: Tuple[str, ...] = (
    "通勤", "日常", "约会", "度假", "旅行", "拍照", "婚礼", "伴娘", "上班", "聚会",
    "出街", "逛街", "运动", "海边",
)
_SCENE_TERMS_EN: Tuple[str, ...] = (
    "everyday", "commute", "date", "vacation", "travel", "wedding", "party",
    "work", "beach",
)

#: 并列到几个使用场景就算"堆叠"。方案要的是"不假装已经展示多个场景"，只能这样落地。
_MULTI_SCENARIO_MIN = 2

#: 造型堆叠。方案 §5 花朵 B 条要求"冻结明确的完成结果"，而真实目录里出现过
#: "适合半扎发、法式盘发、高马尾等多种发型""可盘发、可马尾、可半扎发、可包包头"——
#: 这些是**造型**不是场景，所以上面的场景判据看不见它们，但它们同样在承诺
#: "我已经演示过多个结果"。一次成片只有一个冻结造型，所以并列多个造型同样要被
#: 记成上限（eligible 不变，只收窄口径）。
_HAIRSTYLE_TERMS: Tuple[str, ...] = (
    # 不要放"扎发"：它是"半扎发"的子串，会把一个造型数成两个。
    "半扎", "盘发", "马尾", "丸子头", "高颅顶", "编发", "发包", "包包头",
    "发髻", "披发", "低髻", "低盘", "麻花辫", "鱼骨辫",
)
_HAIRSTYLE_TERMS_EN: Tuple[str, ...] = (
    "half-up", "updo", "ponytail", "bun", "braid", "topknot", "hairdo",
)
_MULTI_LOOK_MIN = 2

#: 上游显式声明"这一条确实要讲多个造型／多套搭配"的字段名。
_MULTI_LOOK_AUTH_FIELDS: Tuple[str, ...] = (
    "multi_look_authorized",
    "multi_hairstyle_authorized",
)

_AESTHETIC_TERMS: Tuple[str, ...] = (
    "美", "唯美", "仙", "温柔", "高级", "精致", "可爱", "甜", "少女", "气质",
    "时髦", "复古", "清爽", "干净", "氛围", "喜欢", "好看", "质感",
)
_AESTHETIC_TERMS_EN: Tuple[str, ...] = (
    "beautiful", "elegant", "cute", "sweet", "chic", "vintage", "aesthetic",
    "lovely", "i like", "soft",
)

#: 观感框架。方案原话把"我喜欢这种层次""看起来柔和"归入**可以自然表达**的审美感受，
#: 而"层次"本身又是一个外观词。所以带观感框架、且没有材质／性能／体验断言的句子，
#: 归审美档而不是外观档——否则方案自己举的两个例子都会被要求"原图或可靠规格支持"。
_IMPRESSION_TERMS: Tuple[str, ...] = (
    "我喜欢", "我觉得", "我看", "看起来", "看着", "显得", "感觉", "观感",
    "氛围感", "给我", "有种",
)
_IMPRESSION_TERMS_EN: Tuple[str, ...] = (
    "i like", "i love", "looks", "feels", "seems", "gives me", "to me",
)

#: 上游可以显式声明"这个主题必须依赖某个部件"的字段名。
_DECLARED_REQUIRED_PART_FIELDS: Tuple[str, ...] = (
    "required_parts", "requires_parts", "required_part_keys",
)


def selling_fact_evidence_enabled() -> bool:
    """Whether ``eligible`` may keep a candidate out of selection.

    Off by default: the record is additive, the filter is not.
    """

    return _text(os.environ.get(SELLING_FACT_EVIDENCE_ENV)).lower() in _TRUE_TOKENS


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple, set)):
        return " ".join(_text(item) for item in value if _text(item))
    return str(value).strip()


def _matched(text: str, terms: Sequence[str]) -> List[str]:
    haystack = _text(text).lower()
    if not haystack:
        return []
    return [term for term in terms if term.lower() in haystack]


def _dedupe(values: Iterable[str]) -> List[str]:
    seen: Dict[str, None] = {}
    for value in values:
        text = _text(value)
        if text and text not in seen:
            seen[text] = None
    return list(seen)


def strip_forbidden_wording(text: Any, terms: Sequence[str]) -> str:
    """Remove banned wording from a sentence without substituting anything.

    The mainline quotes the operator's own sentence as its core value, and that
    sentence can itself contain wording the same contract bans -- measurements
    found ``双层纱质蝴蝶造型，超唯美`` carrying ``纱`` in its own
    ``forbidden_wording``.  Passing it on verbatim lets the central voiceover
    write the material claim the ceiling just refused.

    Only deletion, never replacement: the result cannot assert anything the
    source did not.  A single-character material hit takes its qualifier with it
    (``纱质`` rather than a stranded ``纱``), because that is the phrase that was
    actually banned.  Returns "" when nothing usable is left, so callers can fall
    back instead of shipping a fragment.
    """

    out = _text(text)
    if not out:
        return ""
    for term in sorted({_text(item) for item in terms if _text(item)}, key=len, reverse=True):
        if term not in out:
            continue
        for suffix in _MATERIAL_TRAILING:
            out = out.replace(term + suffix, "")
        out = out.replace(term, "")
    out = re.sub(r"[，,、]{2,}", "，", out)
    out = re.sub(r"[。．.，,、]{2,}", "。", out)
    out = out.strip("，,、。．. 　")
    return out


def _argument_source_text(argument: Mapping[str, Any]) -> Tuple[str, str]:
    """The operator's own sentence, and which field it came from.

    ``source_operator_expression`` is the reviewed whole-cell wording;
    ``operator_expression`` is its normalized per-segment counterpart.  Preferring
    the reviewed sentence keeps the record auditable back to what a human wrote.
    """

    for field in (
        "source_operator_expression",
        "operator_expression",
        "core_value",
        "canonical_selling_point",
        "primary_selling_point",
        "text",
    ):
        value = _text(argument.get(field))
        if value:
            return value, field
    return "", ""


def product_image_version(assets: Optional[Iterable[Any]]) -> Dict[str, Any]:
    """Which product images this argument was reviewed against.

    方案要求身份纠错绑定 SKU 与图片版本，所以这里给的是一个**可复算的版本记录**，
    而不是"有图"这个布尔值。拿不到资产时明确记 ``UNKNOWN`` —— 不编造版本号，
    也不把它当成"已核对"。
    """

    rows: List[Mapping[str, Any]] = []
    for asset in assets or []:
        if not isinstance(asset, Mapping):
            continue
        role = _text(asset.get("role")).upper()
        if role and role != "PRODUCT_REFERENCE":
            continue
        rows.append(asset)
    tokens = [_text(row.get("file_token")) for row in rows]
    digests = [_text(row.get("sha256")) for row in rows]
    authority = next(
        (_text(row.get("authority")) for row in rows if _text(row.get("authority"))), ""
    )
    if not rows or (not any(tokens) and not any(digests)):
        return {
            "schema_version": SELLING_FACT_EVIDENCE_SCHEMA,
            "status": "UNKNOWN",
            "reason": "NO_PRODUCT_REFERENCE_ASSET",
            "record_id": "",
            "count": 0,
            "file_tokens": [],
            "sha256": [],
            "authority": authority or "UNRECORDED",
        }
    material = json.dumps(
        {"tokens": tokens, "sha256": digests}, ensure_ascii=False, sort_keys=True
    )
    return {
        "schema_version": SELLING_FACT_EVIDENCE_SCHEMA,
        "status": "AVAILABLE",
        "reason": "",
        "record_id": "PIV_" + hashlib.sha256(material.encode()).hexdigest()[:16],
        "count": len(rows),
        "file_tokens": tokens,
        "sha256": digests,
        "authority": authority or "OPERATION_TASK_PRODUCT_IMAGES",
    }


def resolve_part_evidence_map(
    product_type: str, top_category: str = ""
) -> Dict[str, Dict[str, str]]:
    """Three-state evidence for the parts this product's gated actions need.

    Reuses the mixed-template registry rather than a second Chinese keyword table.
    ``normalize_product_type`` does not accept bare English canonical names, so the
    direct registry lookup inside ``resolve_mixed_zone`` has to run first.

    No evidence is *not* evidence of absence: an unresolvable product type returns
    an empty map and every caller then reports nothing about parts.
    """

    if not _text(product_type) and not _text(top_category):
        return {}
    try:
        from core.accessory_mixed_templates import (
            resolve_mixed_zone,
            resolve_part_evidence_map as _registry_part_evidence,
        )
    except Exception:  # noqa: BLE001 - evidence recording must never break a plan
        return {}
    try:
        _zone, canonical = resolve_mixed_zone(product_type, top_category)
    except Exception:  # noqa: BLE001
        return {}
    if not _text(canonical):
        return {}
    try:
        return {
            _text(key): dict(value)
            for key, value in (_registry_part_evidence(_text(canonical)) or {}).items()
            if isinstance(value, Mapping)
        }
    except Exception:  # noqa: BLE001
        return {}


def _declared_required_parts(argument: Mapping[str, Any]) -> List[str]:
    declared: List[str] = []
    for field in _DECLARED_REQUIRED_PART_FIELDS:
        value = argument.get(field)
        if isinstance(value, (list, tuple, set)):
            declared.extend(_text(item) for item in value)
        elif _text(value):
            declared.append(_text(value))
    return _dedupe(declared)


def _part_label(part_key: str) -> str:
    """The registry's human label for one part.

    Reports get read by people, and "has_pendant" is not a part name.  Falls back
    to the key so a missing label never hides which part caused a verdict.
    """

    key = _text(part_key)
    if not key:
        return ""
    try:
        from core.accessory_mixed_templates import _part_terminology

        return _text((_part_terminology(key) or {}).get("part_label")) or key
    except Exception:  # noqa: BLE001
        return key


def _mentioned_parts(
    text: str, parts: Mapping[str, Mapping[str, Any]]
) -> List[Dict[str, str]]:
    """Parts this sentence names, with the registry's state for each.

    ``_part_terminology`` returns ``positive_terms`` / ``negative_terms`` rather
    than a flat list, and a negative term contains the positive one ("无吊坠"
    names the pendant in order to deny it).  So a positive hit *inside* a
    negative term is not an assertion -- otherwise "素链，无吊坠" would be read as
    claiming a pendant that the same sentence denies.
    """

    text = _text(text)
    if not text or not parts:
        return []
    try:
        from core.accessory_mixed_templates import _part_terminology
    except Exception:  # noqa: BLE001
        return []
    hits: List[Dict[str, str]] = []
    for part_key, evidence in parts.items():
        try:
            terms = _part_terminology(part_key) or {}
        except Exception:  # noqa: BLE001
            terms = {}
        positives = [_text(item) for item in (terms.get("positive_terms") or []) if _text(item)]
        negatives = [_text(item) for item in (terms.get("negative_terms") or []) if _text(item)]
        positive_hits = _matched(text, positives)
        negative_hits = _matched(text, negatives)
        if not positive_hits and not negative_hits:
            continue
        asserted = [
            term
            for term in positive_hits
            if not any(term in negative for negative in negative_hits)
        ]
        hits.append(
            {
                "part_key": _text(part_key),
                "part_label": _part_label(part_key),
                "state": _text(evidence.get("state")).upper(),
                "source": _text(evidence.get("source")),
                "basis": "SOURCE_TEXT_TERM_MATCH",
                "asserted": bool(asserted),
                "matched_terms": _dedupe(positive_hits + negative_hits),
                "negated_terms": negative_hits,
            }
        )
    return hits


def classify_fact_evidence(
    argument: Mapping[str, Any],
    *,
    product_type: str = "",
    top_category: str = "",
    parts_evidence: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    """Which of the five fact tiers this argument's wording actually is.

    A sentence can touch several tiers ("双层纱质蝴蝶造型，超唯美" is appearance
    *and* aesthetic); ``tier`` reports the highest-risk one, because that is the
    tier whose requirement governs what may be said.
    """

    data = argument if isinstance(argument, Mapping) else {}
    text, field = _argument_source_text(data)
    matches = {
        "experience": _dedupe(
            _matched(text, _EXPERIENCE_TERMS) + _matched(text, _EXPERIENCE_TERMS_EN)
        ),
        "performance": _dedupe(
            _matched(text, _PERFORMANCE_TERMS) + _matched(text, _PERFORMANCE_TERMS_EN)
        ),
        "material": _dedupe(
            _matched(text, _MATERIAL_TERMS) + _matched(text, _MATERIAL_TERMS_EN)
        ),
        "appearance": _dedupe(
            _matched(text, _APPEARANCE_TERMS) + _matched(text, _APPEARANCE_TERMS_EN)
        ),
        "styling": _dedupe(
            _matched(text, _STYLING_TERMS) + _matched(text, _STYLING_TERMS_EN)
        ),
        "aesthetic": _dedupe(
            _matched(text, _AESTHETIC_TERMS) + _matched(text, _AESTHETIC_TERMS_EN)
        ),
    }
    present: List[str] = []
    if matches["experience"]:
        present.append(TIER_EXPERIENCE)
    if matches["performance"]:
        present.append(TIER_PERFORMANCE)
    if matches["material"] or matches["appearance"]:
        present.append(TIER_APPEARANCE)
    if matches["styling"]:
        present.append(TIER_STYLING)
    if matches["aesthetic"]:
        present.append(TIER_AESTHETIC)
    # A claim mapped onto a scenario/audience concept is a styling suggestion even
    # when its normalized wording dropped the scene words.
    if _text(data.get("claim_type")).lower() in {"scenario", "audience"}:
        if TIER_STYLING not in present:
            present.append(TIER_STYLING)
    if not present:
        present.append(TIER_AESTHETIC)
    impression = _dedupe(
        _matched(text, _IMPRESSION_TERMS) + _matched(text, _IMPRESSION_TERMS_EN)
    )
    if (
        impression
        and TIER_APPEARANCE in present
        and not matches["material"]
        and not matches["performance"]
        and not matches["experience"]
    ):
        # 方案把它自己的例子（"我喜欢这种层次"）放在"可以自然表达"里，所以带观感
        # 框架、又只提了外观特征时要归审美档。一旦出现材质断言就不能降档：材质是
        # 事实主张，不是观感。
        present = [tier for tier in present if tier != TIER_APPEARANCE]
        if not present:
            present.append(TIER_AESTHETIC)
    present.sort(key=lambda tier: TIER_RISK_ORDER.get(tier, 99))
    tier = present[0]

    parts = parts_evidence if parts_evidence is not None else resolve_part_evidence_map(
        product_type, top_category
    )
    mentioned = _mentioned_parts(text, parts or {})
    # A declared dependency may only be judged when there *is* part evidence.
    # With an empty map, "unknown" would be an invented verdict -- the honest
    # record is an empty state plus a reason saying no evidence was available.
    declared_states: List[Dict[str, str]] = []
    for part_key in _declared_required_parts(data):
        registry = (parts or {}).get(part_key) or {}
        state = _text(registry.get("state")).upper()
        declared_states.append(
            {
                "part_key": part_key,
                "state": state,
                "source": _text(registry.get("source")),
                "basis": "DECLARED_BY_ARGUMENT" if state else "NO_PART_EVIDENCE",
                "part_label": _part_label(part_key),
            }
        )
    return {
        "schema_version": SELLING_FACT_EVIDENCE_SCHEMA,
        "tier": tier,
        "tier_label_zh": TIER_LABELS_ZH.get(tier, ""),
        "tiers": present,
        "tier_basis": {
            "text": text,
            "source_field": field,
            "matched": {
                **{key: value for key, value in matches.items() if value},
                **({"impression": impression} if impression else {}),
            },
            "claim_type": _text(data.get("claim_type")),
            "claim_theme": _text(data.get("claim_theme")),
            "allowed_strength": _text(data.get("allowed_strength")),
            "evidence_requirement": _text(data.get("evidence_requirement")),
        },
        "requires": TIER_REQUIREMENTS.get(tier, "NONE"),
        "mentioned_parts": mentioned,
        "declared_required_parts": declared_states,
        "part_evidence_available": bool(parts),
    }


def _experience_authority(argument: Mapping[str, Any]) -> str:
    """Whether a human actually recorded an experience for this product.

    Operator input states *intent*.  方案原话："人工运营输入提供意图，不自动构成真实
    体验记录" —— 所以 operator_input 来源默认 **不是** 体验授权，除非上游显式给出
    一个位于既有取值域内的授权值。
    """

    declared = _text(argument.get("experience_authority")).upper()
    if declared in EXPERIENCE_AUTHORITIES:
        return declared
    return EXPERIENCE_NONE


def _looks_authorized(argument: Mapping[str, Any]) -> bool:
    """Whether this candidate is allowed to promise several styling results.

    Two separate flags grant it: a multi-scenario authorisation ("这产品真的适合
    通勤和度假两种场合"), and the narrower multi-look one for hairstyle stacking.
    Neither is inferred -- an absent flag means one result, which is what a single
    15-second film can actually show.

    A multi-scenario authorisation implies the look one: if several occasions were
    approved, several styling results were too.  Not the other way round -- "能盘发
    也能半扎" says nothing about which occasions may be shown.
    """

    if bool(argument.get("multi_scenario_authorized")):
        return True
    for field in _MULTI_LOOK_AUTH_FIELDS:
        if bool(argument.get(field)):
            return True
    return False


def detect_unsourced_experience(
    text: Any, *, experience_authority: str = EXPERIENCE_NONE
) -> Dict[str, Any]:
    """Experience wording that no human actually recorded, found in *finished* copy.

    ``classify_fact_evidence`` reads a selling argument before anything is written.
    This reads what a script actually says, so C3's third review bucket ("建议与审美
    不伪装为实测") has the same word list instead of a second one.  It reports, it
    does not rewrite: the caller decides whether this goes to the one-shot revision.
    """

    matched = _dedupe(
        _matched(_text(text), _EXPERIENCE_TERMS)
        + _matched(_text(text), _EXPERIENCE_TERMS_EN)
    )
    authority = _text(experience_authority).upper() or EXPERIENCE_NONE
    authorised = authority in EXPERIENCE_AUTHORITIES and authority != EXPERIENCE_NONE
    return {
        "schema_version": SELLING_FACT_EVIDENCE_SCHEMA,
        "authority": authority,
        "authorised": authorised,
        "requires_source": bool(matched) and not authorised,
        "matched_wording": matched,
    }


def _part_conflicts(evidence: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Part problems that must be surfaced, split by whether they may block.

    Only ``ABSENT`` is a *contradiction* -- the confirmed structure says the part
    is not there, so an argument that asserts it cannot be resolved by adding
    source.  方案要求这类冲突"必须解决，不让生成模型自行选择一方"，所以它拦下
    候选。``UNKNOWN`` 只有在候选**自己声明**依赖该部件时才排除，因为关键词命中
    不足以证明主题依赖它。
    """

    conflicts: List[Dict[str, Any]] = []
    for part in evidence.get("mentioned_parts") or []:
        if not isinstance(part, Mapping):
            continue
        if _text(part.get("state")).upper() != PART_STATE_ABSENT:
            continue
        if not part.get("asserted"):
            # The sentence names the part only to deny it ("素链，无吊坠"), which
            # agrees with the confirmed structure rather than contradicting it.
            continue
        conflicts.append(
            {
                "kind": "PART_CONTRADICTED",
                "part_key": _text(part.get("part_key")),
                "part_label": _text(part.get("part_label"))
                or _part_label(_text(part.get("part_key"))),
                "part_state": PART_STATE_ABSENT,
                "basis": _text(part.get("basis")),
                "matched_terms": list(part.get("matched_terms") or []),
                "resolution": "ARGUMENT_EXCLUDED",
            }
        )
    for part in evidence.get("declared_required_parts") or []:
        if not isinstance(part, Mapping):
            continue
        state = _text(part.get("state")).upper()
        if not state or state == PART_STATE_VERIFIED:
            # No evidence, or the structure confirms the part: nothing to exclude.
            continue
        conflicts.append(
            {
                "kind": "PART_NOT_CONFIRMED",
                "part_key": _text(part.get("part_key")),
                "part_label": _text(part.get("part_label"))
                or _part_label(_text(part.get("part_key"))),
                "part_state": state,
                "basis": _text(part.get("basis")),
                "resolution": "THEME_EXCLUDED",
            }
        )
    return conflicts


def adaptation_verdict(
    argument: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    image: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """What the evidence actually permits, and what it forbids.

    结论不是"能不能用"，而是"用哪个口径"。方案要的是"无无源体验升级"：一个只由
    运营意图支撑的体验句，可以降级成可见结果描述，但不能原样当成实测。
    """

    data = argument if isinstance(argument, Mapping) else {}
    facts = evidence if isinstance(evidence, Mapping) else {}
    basis = facts.get("tier_basis") if isinstance(facts.get("tier_basis"), Mapping) else {}
    matched = basis.get("matched") if isinstance(basis.get("matched"), Mapping) else {}
    tier = _text(facts.get("tier"))
    authority = _experience_authority(data)
    image_status = _text((image or {}).get("status")) or "UNKNOWN"

    conflicts = _part_conflicts(facts)
    contradictions = [item for item in conflicts if item["kind"] == "PART_CONTRADICTED"]
    unresolved_parts = [item for item in conflicts if item["kind"] == "PART_NOT_CONFIRMED"]
    if contradictions or unresolved_parts:
        blocking = contradictions[0] if contradictions else unresolved_parts[0]
        part_name = _text(blocking.get("part_label")) or _text(blocking.get("part_key"))
        return {
            "schema_version": SELLING_FACT_EVIDENCE_SCHEMA,
            "verdict": VERDICT_CONFLICT if contradictions else VERDICT_UNKNOWN_PART,
            "eligible": False,
            "reason": (
                f"依赖的部件「{part_name}」与已确认结构冲突，必须先解决而不是让模型自选"
                if contradictions
                else f"候选声明的必要部件「{part_name}」尚未确认，只排除依赖它的主题"
            ),
            "experience_authority": authority,
            "allowed_wording": "",
            "forbidden_wording": [],
            "conflicts": conflicts,
        }

    if tier == TIER_EXPERIENCE:
        if authority == EXPERIENCE_NONE:
            return {
                "schema_version": SELLING_FACT_EVIDENCE_SCHEMA,
                "verdict": VERDICT_NEEDS_SOURCE,
                "eligible": True,
                "reason": "过程／使用体验主张没有真实体验依据，不能由生成画面反向证明",
                "experience_authority": authority,
                "allowed_wording": "只写眼前可见的装饰结果与比例关系，不写使用过程或步骤多少",
                "forbidden_wording": list(matched.get("experience") or []),
                "conflicts": [],
            }
        # 有体验授权就按原强度表达；这一步只放行"有人真的记过这件事"，
        # 不代表口播可以外推成商品性能（那要另走性能档的来源要求）。
        return {
            "schema_version": SELLING_FACT_EVIDENCE_SCHEMA,
            "verdict": VERDICT_ADAPTED,
            "eligible": True,
            "reason": "已有真实体验依据，可按经历口径表达",
            "experience_authority": authority,
            "allowed_wording": "可按已确认的经历口径表达个人体验，不外推为商品性能",
            "forbidden_wording": [],
            "conflicts": [],
        }

    if tier == TIER_PERFORMANCE:
        return {
            "schema_version": SELLING_FACT_EVIDENCE_SCHEMA,
            "verdict": VERDICT_NEEDS_SOURCE,
            "eligible": True,
            "reason": (
                "性能·材质·佩戴体验需要规格或测试来源；"
                "外观看起来轻盈不等于重量轻或久戴舒适"
            ),
            "experience_authority": authority,
            "allowed_wording": "只用「看起来／呈现出」的观感口径，不下重量、材质或久戴结论",
            "forbidden_wording": list(matched.get("performance") or []),
            "conflicts": [],
        }

    if tier == TIER_APPEARANCE:
        material = list(matched.get("material") or [])
        # 造型与层次属于外观事实，商品图或可靠规格即可支撑；材质名词不一样——
        # 方案把"材质"与性能、佩戴体验并列，要求"相应来源"，而商品图是外观来源，
        # 证明不了纤维成分。所以材质断言不因"有图"而放开，只保留观感口径
        # （方案 §5 蝴蝶 B 条：未核实材质时不把半透明观感升级为特定材质事实）。
        if material:
            return {
                "schema_version": SELLING_FACT_EVIDENCE_SCHEMA,
                "verdict": VERDICT_NEEDS_SOURCE,
                "eligible": True,
                "reason": "材质属于需要规格来源的主张；商品图只能说明看起来如何，不能证明成分",
                "experience_authority": authority,
                "allowed_wording": "只描述可见的造型、层次与排列；材质只按可见观感表述，不下成分结论",
                "forbidden_wording": material,
                "conflicts": [],
            }
        if image_status != "AVAILABLE":
            return {
                "schema_version": SELLING_FACT_EVIDENCE_SCHEMA,
                "verdict": VERDICT_NEEDS_SOURCE,
                "eligible": True,
                "reason": "外观与装饰事实需要原图或可靠规格支持，本次没有可核对的商品图片版本",
                "experience_authority": authority,
                "allowed_wording": "保留已确认的结构描述，不新增未核对的外观细节",
                "forbidden_wording": [],
                "conflicts": [],
            }
        return {
            "schema_version": SELLING_FACT_EVIDENCE_SCHEMA,
            "verdict": VERDICT_ADAPTED,
            "eligible": True,
            "reason": "外观与装饰事实有商品图片版本可核对",
            "experience_authority": authority,
            # 走到这里说明原文里没有材质词（材质在前一个分支就要求来源了），放开
            # 的是图片真正支撑得住的造型、层次与排列。
            "allowed_wording": "可描述图片支持的造型、层次、排列与可见观感",
            "forbidden_wording": [],
            "conflicts": [],
        }

    if tier == TIER_STYLING:
        source_text = _text(basis.get("text"))
        scenes = _dedupe(
            _matched(source_text, _SCENE_TERMS) + _matched(source_text, _SCENE_TERMS_EN)
        )
        looks = _dedupe(
            _matched(source_text, _HAIRSTYLE_TERMS)
            + _matched(source_text, _HAIRSTYLE_TERMS_EN)
        )
        stacked: List[Dict[str, Any]] = []
        if len(scenes) >= _MULTI_SCENARIO_MIN and not bool(
            data.get("multi_scenario_authorized")
        ):
            stacked.append(
                {
                    "kind": "MULTI_SCENARIO_UNAUTHORIZED",
                    "stacked_scenes": scenes,
                    "allowed_scenarios": 1,
                    "resolution": "KEEP_ONE",
                }
            )
        if len(looks) >= _MULTI_LOOK_MIN and not _looks_authorized(data):
            stacked.append(
                {
                    "kind": "MULTI_LOOK_UNAUTHORIZED",
                    "stacked_looks": looks,
                    "allowed_looks": 1,
                    "resolution": "KEEP_ONE",
                }
            )
        if stacked:
            # 这里不能把场景词／造型词本身列进 forbidden_wording：单个场景词是对的，
            # 错的只是并列堆叠。词级禁用会把"约会是可用的搭配建议"一起误杀。
            allowed_bits = []
            if any(item["kind"] == "MULTI_SCENARIO_UNAUTHORIZED" for item in stacked):
                allowed_bits.append("一个与实际冻结场景兼容的搭配")
            if any(item["kind"] == "MULTI_LOOK_UNAUTHORIZED" for item in stacked):
                allowed_bits.append("一个与实际冻结造型一致的完成结果")
            return {
                "schema_version": SELLING_FACT_EVIDENCE_SCHEMA,
                "verdict": VERDICT_CEILING,
                "eligible": True,
                "reason": (
                    "原文并列了多个"
                    + "／".join(
                        "使用场景"
                        if item["kind"] == "MULTI_SCENARIO_UNAUTHORIZED"
                        else "造型结果"
                        for item in stacked
                    )
                    + "，但本次没有多场景授权，不能包装成已经演示多搭"
                ),
                "experience_authority": authority,
                "allowed_wording": "只说" + ("、".join(allowed_bits)) + "，不并列多个",
                "forbidden_wording": [],
                "conflicts": stacked,
            }
        return {
            "schema_version": SELLING_FACT_EVIDENCE_SCHEMA,
            "verdict": VERDICT_ADAPTED,
            "eligible": True,
            "reason": "搭配建议与实际冻结造型兼容",
            "experience_authority": authority,
            "allowed_wording": "可表达一个已计划的搭配建议",
            "forbidden_wording": [],
            "conflicts": [],
        }

    return {
        "schema_version": SELLING_FACT_EVIDENCE_SCHEMA,
        "verdict": VERDICT_ADAPTED,
        "eligible": True,
        "reason": "审美感受可以自然表达，不伪装为实测",
        "experience_authority": authority,
        "allowed_wording": "可用第一人称表达喜好与观感",
        "forbidden_wording": [],
        "conflicts": [],
    }


def record_selected_argument(
    argument: Mapping[str, Any],
    *,
    product_reference_assets: Optional[Iterable[Any]] = None,
    product_type: str = "",
    top_category: str = "",
    parts_evidence: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    """C1 的交付物：一个所选卖点的原文／来源／图片版本／事实类型／适配结论。"""

    data = argument if isinstance(argument, Mapping) else {}
    text, field = _argument_source_text(data)
    image = product_image_version(product_reference_assets)
    evidence = classify_fact_evidence(
        data,
        product_type=product_type,
        top_category=top_category,
        parts_evidence=parts_evidence,
    )
    adaptation = adaptation_verdict(data, evidence, image=image)
    return {
        "schema_version": SELLING_FACT_EVIDENCE_SCHEMA,
        "source_text": text,
        "source_text_field": field,
        "source": {
            "source": _text(data.get("source")),
            "source_ref": _text(data.get("source_ref")),
            "source_claim_ids": [
                _text(item) for item in (data.get("source_claim_ids") or []) if _text(item)
            ],
            "source_argument_id": _text(data.get("source_argument_id"))
            or _text(data.get("argument_id")),
            "authority": _text(data.get("authorization_source")) or _text(data.get("authority")),
            "mapping_status": _text(data.get("mapping_status")),
            "verification_status": _text(data.get("verification_status")),
            "source_type": _text(data.get("source_type")),
        },
        "product_image_version": image,
        "fact_type": {
            "tier": _text(evidence.get("tier")),
            "tier_label_zh": _text(evidence.get("tier_label_zh")),
            "tiers": list(evidence.get("tiers") or []),
            "requires": _text(evidence.get("requires")),
            "tier_basis": dict(evidence.get("tier_basis") or {}),
            "mentioned_parts": list(evidence.get("mentioned_parts") or []),
            "declared_required_parts": list(evidence.get("declared_required_parts") or []),
            "part_evidence_available": bool(evidence.get("part_evidence_available")),
        },
        "adaptation": dict(adaptation),
    }
