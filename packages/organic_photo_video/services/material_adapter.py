"""商品与主题匹配、参考适配（自动图文供稿 Phase 1，方案 §六/§七/§八）。

选材两步（§七）：
1. ``narrow_candidates``：程序根据分析缓存、商品与使用历史缩小候选（确定性、零成本）；
2. ``select_reference``：Doubao 阅读少量候选**文字摘要**确定主参考、必要补充与采用方式。

硬规则（§六）：指定商品时"先商品后参考"——商品身份/颜色/版型以本店资料为准，
参考只能借鉴配套、比例与表达；不把参考中的其他商品冒充本篇商品（一衣多穿尤其）。
不确定或无合适候选：返回 None 并记录素材缺口，不静默降级、不强行选材。
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from services.material_source import MaterialPackage

# ---------------------------------------------------------------------------
# 主题选材策略（§七 表格编码为数据；只规定：需要什么/优先什么/可借鉴什么）
# ---------------------------------------------------------------------------

THEME_SELECTION_STRATEGIES: Dict[str, Dict[str, Any]] = {
    "一衣多穿": {
        "prefer_structures": ["same_item_multiway"],
        "needs": ["同一核心单品出现在多套搭配中"],
        "prefer": ["同核心单品的多套搭配，页序连续"],
        "fallback": ["独立合集可借鉴单套表达，但不得把不同商品冒充同一件"],
        "forbidden": ["不同商品冒充同一件"],
    },
    "旅行·温度穿搭": {
        "prefer_structures": ["layered", "same_item_multiway"],
        "needs": ["层搭、穿脱、活动线索"],
        "prefer": ["层搭顺序清楚、有穿脱示范或活动场景线索"],
        "fallback": ["普通街拍可借鉴搭配比例，但叙事需自建"],
        "forbidden": ["凭“冬季”等标签推断温度适用性"],
    },
    "温度穿搭·逐层加衣": {
        "prefer_structures": ["layered"],
        "needs": ["基础层到外层的连续组图"],
        "prefer": ["分层连续、每层可辨认"],
        "fallback": ["多套独立造型只能借鉴单品组合，不替代分层叙事"],
        "forbidden": ["以独立造型合集替代分层"],
    },
    "旅行·配色参考": {
        "prefer_structures": [],
        "needs": ["配色关系清楚"],
        "prefer": ["配色关系明确、色块干净"],
        "fallback": ["单套搭配可取其配色"],
        "forbidden": ["改变指定商品颜色"],
    },
    "旅行·拍照穿搭": {
        "prefer_structures": [],
        "needs": ["人物、服装、场景关系清楚"],
        "prefer": ["人物与场景构图关系清楚、出片感强"],
        "fallback": ["室内棚拍可借鉴造型与站位"],
        "forbidden": ["复制错误目的地"],
    },
    "四选一": {
        "prefer_structures": ["comparison"],
        "needs": ["可比较、差异明确的多个造型"],
        "prefer": ["差异维度清楚的多造型对比"],
        "fallback": ["独立合集可用，但同造型多角度不算多个选项"],
        "forbidden": ["同造型多角度凑数"],
    },
    "显高搭配": {
        "prefer_structures": [],
        "needs": ["腰线、衣长、裤鞋关系清楚"],
        "prefer": ["腰线/衣长/裤鞋关系在图中可辨认"],
        "fallback": ["全身照清晰即可借鉴比例"],
        "forbidden": ["编造显高数值"],
    },
}

DEFAULT_STRATEGY = {
    "prefer_structures": [],
    "needs": [],
    "prefer": ["穿搭表达清晰完整"],
    "fallback": [],
    "forbidden": [],
}


def strategy_for(theme: str) -> Dict[str, Any]:
    """按主题名匹配策略；未登记主题回落默认（不新建主题体系，§七）。"""
    if not theme:
        return DEFAULT_STRATEGY
    if theme in THEME_SELECTION_STRATEGIES:
        return THEME_SELECTION_STRATEGIES[theme]
    for key, value in THEME_SELECTION_STRATEGIES.items():
        if key in theme or theme in key:
            return value
    return DEFAULT_STRATEGY


# ---------------------------------------------------------------------------
# 温度带与热学适配（2026-09-16 文案×品类失配修复）
# 背景：主题「凉爽旅行」执行档在文案里声明 15–22°C，但商品轮换与参考
# 选材都没有温度概念——蓬松棉服照样进凉爽主题。此处的工具供供给选品
# 门禁、初筛降权与终选 prompt 共用；规则式、不调用模型。
# ---------------------------------------------------------------------------

#: 主题 → 本篇温度带（°C 含端点）。只映射可明确判定的主题（含显示别名）；
#: 未映射主题不启用热学检查，行为与旧版一致。
THEME_THERMAL_BANDS = {
    "凉爽旅行": (15, 22),
    "旅行穿搭": (15, 22),   # photo_theme 显示别名 → 凉爽旅行
}

#: 品类/单品关键词 → 适用温度窗口（°C）。自上而下首个命中生效；
#: 判定输入=商品名+品类+variant 的拼串，命中不了=无窗口（不拦，交人工）。
CATEGORY_THERMAL_WINDOWS: tuple = (
    (("羽绒", "蓬松", "棉服", "厚外套", "派克", "puffer", "down jacket"), 5, 12),
    (("大衣", "coat"), 8, 16),
    (("外套", "夹克", "开衫", "西装", "风衣", "卫衣", "连帽衫", "jacket"), 12, 20),
    (("长袖", "衬衫", "针织", "毛衣", "帽衫", "长袖T"), 15, 24),
    (("短袖", "背心", "吊带", "短裤", "短裙", "连衣裙", "半裙", "夏裙"), 22, 32),
)

_BAND_RE = re.compile(r"(\d{1,2})\s*[-–~至]\s*(\d{1,2})\s*°?C?", re.I)


def parse_thermal_band(text) -> Optional[tuple]:
    """从文案/标签里解析温度区间，如「15-22°C」「15–22度」→ (15, 22)。"""
    m = _BAND_RE.search(str(text or ""))
    if not m:
        return None
    lo, hi = int(m.group(1)), int(m.group(2))
    return (lo, hi) if lo < hi else None


def thermal_window(*texts) -> Optional[tuple]:
    """按关键词给单品/商品归类热学窗口；无命中返回 None（不拦）。"""
    blob = " ".join(str(t or "") for t in texts if t)
    for keywords, lo, hi in CATEGORY_THERMAL_WINDOWS:
        if any(str(k) in blob for k in keywords):
            return (lo, hi)
    return None


def thermal_overlap(a: tuple, b: tuple) -> bool:
    return a[0] <= b[1] and b[0] <= a[1]


def theme_thermal_band(theme_label) -> Optional[tuple]:
    return THEME_THERMAL_BANDS.get(str(theme_label or "").strip())


#: 标题季节标签（规则式）：cold=秋冬厚装主导，warm=夏装主导，cool=春秋。
_COLD_WORDS = ("羽绒", "棉服", "保暖", "过冬", "冬季", "冬天", "雪地", "厚外套", "加绒", "羽绒服")
_WARM_WORDS = ("夏天", "夏季", "海岛", "清凉", "短袖", "吊带", "泳", "度假防晒")
_COOL_WORDS = ("秋冬", "秋天", "秋季", "秋日", "初秋", "初冬", "外套", "夹克", "开衫")


def title_season_tag(title) -> str:
    text = str(title or "")
    if any(w in text for w in _COLD_WORDS):
        return "cold"
    if any(w in text for w in _WARM_WORDS):
        return "warm"
    if any(w in text for w in _COOL_WORDS):
        return "cool"
    return ""


@dataclass
class ProductBrief:
    """本篇商品摘要（Phase 2 供稿脚本从现有商品资料构建；§六）。"""

    product_id: str
    category: str                      # 如 开衫/棉服/衬衫（用于候选匹配）
    color: str = ""
    form: str = ""                     # 版型
    key_features: List[str] = field(default_factory=list)

    def summary_line(self) -> str:
        parts = [f"商品编码 {self.product_id}", self.category]
        if self.color:
            parts.append(f"颜色 {self.color}")
        if self.form:
            parts.append(f"版型 {self.form}")
        if self.key_features:
            parts.append("特点：" + "、".join(self.key_features))
        return "；".join(parts)


@dataclass
class Candidate:
    note_id: str
    title: str
    theme: str
    review_status: str
    analysis: Dict[str, Any]
    score: float = 0.0
    reasons: List[str] = field(default_factory=list)


@dataclass
class SelectionResult:
    main_note_id: str
    supplement_note_ids: List[str] = field(default_factory=list)
    adoption: str = "overall"          # overall | outfit_only | visual_only | narrative_only
    rationale: str = ""
    rejected: List[Dict[str, str]] = field(default_factory=list)
    # 页级选材（Phase 1）：[{note_id, seq, purpose}]，只含主参考笔记。
    # purpose ∈ outfit_detail（搭配/单品细节）| full_outfit（完整穿搭页）|
    # visual_tone（色调/氛围参考）。narrative_only 时必须为空。
    pages: List[Dict[str, Any]] = field(default_factory=list)


def narrow_candidates(
    packages: Sequence[MaterialPackage],
    analyses: Dict[str, Dict[str, Any]],
    *,
    theme: str = "",
    product: Optional[ProductBrief] = None,
    recent_note_ids: Sequence[str] = (),
    limit: int = 8,
    temperature_band: Optional[tuple] = None,
) -> List[Candidate]:
    """程序初筛：只消费分析缓存文字，不调用模型。

    排序依据（§七）：主题可用性 > 商品可用性 > 人工已选加成 > 账号风格完整性
    （以组图完整/结构明确代理）> 互动量（仅辅助）。
    """
    strategy = strategy_for(theme)
    prefer_structures = set(strategy.get("prefer_structures") or [])
    recent = set(recent_note_ids)
    product_category = (product.category or "").strip() if product else ""

    candidates: List[Candidate] = []
    for package in packages:
        analysis = analyses.get(package.note_id)
        if not analysis:
            continue
        if not analysis.get("consumable", False):
            continue
        # 审美硬门槛：镜面自拍/随手拍/画质差不进候选。生成链会继承参考的
        # 画面风格，这类素材必然带偏成片（宁可无候选也不将就）。
        quality = str(analysis.get("photography_quality") or "")
        style = str(analysis.get("shoot_style") or "")
        if quality == "poor" or style in {"mirror_selfie", "casual_phone_selfie"}:
            continue
        score = 0.0
        reasons: List[str] = []

        if quality == "good":
            score += 0.5
            reasons.append("成片质量好（构图光线干净）")

        if temperature_band:
            season = title_season_tag(package.title)
            if season == "cold" and not thermal_overlap((5, 12), temperature_band):
                score -= 3.0
                reasons.append("秋冬厚装主导笔记，与本篇温度带不符（强降权）")
            elif season == "warm" and not thermal_overlap((22, 32), temperature_band):
                score -= 3.0
                reasons.append("夏季主导笔记，与本篇温度带不符（强降权）")

        structure = str(analysis.get("set_structure") or "")
        if prefer_structures and structure in prefer_structures:
            score += 3.0
            reasons.append(f"组图结构匹配主题（{structure}）")
        elif prefer_structures:
            reasons.append(f"组图结构非主题首选（{structure or '未知'}）")

        if product_category:
            core_text = " ".join(
                str((item or {}).get("item") or "")
                for item in (analysis.get("core_items") or [])
            )
            if product_category and product_category in core_text:
                score += 2.5
                reasons.append("核心单品含本篇商品品类")
            elif core_text:
                score -= 0.5  # 不硬排除：品类表达可能不同，交由 Doubao 终判
                reasons.append("核心单品未见本篇商品品类（待模型终判）")

        if package.review_status == "selected":
            score += 1.5
            reasons.append("人工已选")

        page_roles = analysis.get("page_roles") or []
        if page_roles:
            score += 0.5
            reasons.append(f"页角色明确（{len(page_roles)} 页）")

        like = package.like_count or 0
        collected = package.collected_count or 0
        score += min(1.0, (like + collected) / 20000.0)  # 互动量仅弱信号
        reasons.append(f"互动 {like}/{collected}")

        if package.note_id in recent:
            score -= 3.0
            reasons.append("近期已使用（强降权）")

        candidates.append(Candidate(
            note_id=package.note_id,
            title=package.title or analysis.get("note_topic") or "",
            theme=package.theme,
            review_status=package.review_status,
            analysis=analysis,
            score=score,
            reasons=reasons,
        ))

    candidates.sort(key=lambda c: c.score, reverse=True)
    return candidates[:limit]


_SELECT_PROMPT = """你是穿搭图文的参考选材器。根据候选素材摘要，为本篇生产选择参考。

本篇主题：{theme}
主题策略：优先 {strategy_prefer}；可借鉴 {strategy_fallback}；禁止 {strategy_forbidden}
{product_line}
要求：
- 只选一篇主参考；本轮不支持补充参考，supplement_note_ids 恒为空数组。
- 审美门槛：绝对不要选择镜面自拍（mirror_selfie）、随手拍/游客照（casual_phone_selfie）
  或 photography_quality=poor 的素材；优先全身完整、光线干净、背景整洁、
  构图专业的博主级出片——生成画面会直接继承参考的拍摄质感。
{band_line}- 采用方式（adoption）语义必须严格执行，并据此做页级选材（pages）：
  · outfit_only：只借鉴单品组合/层次/比例/配色关系。pages 只选搭配与单品
    细节页（purpose=outfit_detail），环境主导的大场景页一律不选；若该笔记
    没有可用的细节页，pages 返回空数组（只用文字化搭配信息，不发原图）。
  · visual_only：只借鉴色调、光线、摄影气质。pages 选最能代表色调氛围的
    1-2 页（purpose=visual_tone），不选人物特写为主的页。
  · narrative_only：只借鉴选题、页面角色与解释顺序，不发送任何原图，
    pages 必须为空数组。
  · overall：综合灵感，重新形成本篇计划，不等于逐页沿用。pages 挑不超过
    3 页（purpose 按实际用途标注），禁止整组逐页照搬。
- pages 的 seq 必须来自候选摘要里标注的页码，不得虚构。
- 指定商品时：商品身份/颜色/版型以本店资料为准，参考只借鉴配套、比例与表达。
- 不确定或没有合适候选时，main_note_id 填空字符串，不要勉强选择。

候选摘要：
{candidates_text}

输出严格 JSON（不要其他文字）：
{{
 "main_note_id": "主参考笔记ID或空字符串",
 "supplement_note_ids": [],
 "adoption": "overall | outfit_only | visual_only | narrative_only 之一",
 "pages": [{{"seq": 页码, "purpose": "outfit_detail | full_outfit | visual_tone"}}],
 "rationale": "选择理由与采用方式（两三句）",
 "rejected": [{{"note_id": "ID", "reason": "不采用原因"}}]
}}
"""


def select_reference(
    client: Any,
    candidates: Sequence[Candidate],
    *,
    theme: str = "",
    product: Optional[ProductBrief] = None,
    max_tokens: int = 900,
    temperature_band: Optional[tuple] = None,
) -> Optional[SelectionResult]:
    """Doubao 终选：读文字摘要定主参考/补充/采用方式。无合适候选返回 None。"""
    if not candidates:
        return None
    strategy = strategy_for(theme)
    lines = []
    for cand in candidates:
        analysis = cand.analysis
        core = "、".join(
            str((item or {}).get("item") or "")
            for item in (analysis.get("core_items") or [])
        )[:80]
        page_seq_roles = "、".join(
            f"{pr.get('seq')}:{pr.get('role')}"
            for pr in (analysis.get("page_roles") or [])[:10]
        )
        lines.append(
            f"- {cand.note_id}｜标题:{cand.title[:40]}｜结构:{analysis.get('set_structure')}"
            f"｜单品:{core}｜配色:{','.join(analysis.get('palette') or [])}"
            f"｜拍摄:{analysis.get('shoot_style') or '未知'}/{analysis.get('photography_quality') or '未知'}"
            f"｜页码(角色):{page_seq_roles or '未知'}"
            f"｜主题:{str(analysis.get('note_topic') or '')[:40]}"
        )
    product_line = f"本篇商品：{product.summary_line()}" if product else "本篇不指定商品（自由搭配）"
    band_line = (
        f"- 温度带约束：本篇温度区间 {temperature_band[0]}–{temperature_band[1]}°C；"
        "pages 不得选择羽绒/棉服/厚外套等冬装搭配为主的页，也不得选择夏装为主的页。\n"
        if temperature_band else "")
    prompt = _SELECT_PROMPT.format(
        theme=theme or "（参考优先：由素材提炼选题）",
        strategy_prefer="；".join(strategy.get("prefer") or []) or "—",
        strategy_fallback="；".join(strategy.get("fallback") or []) or "—",
        strategy_forbidden="；".join(strategy.get("forbidden") or []) or "—",
        product_line=product_line,
        band_line=band_line,
        candidates_text="\n".join(lines),
    )
    from services.photo_reference_vision import parse_vision_envelope
    response = client.chat_with_multiple_images([], prompt, max_tokens)
    parsed = parse_vision_envelope(response)
    main = str(parsed.get("main_note_id") or "").strip()
    if not main or main not in {c.note_id for c in candidates}:
        return None
    valid_ids = {c.note_id for c in candidates}
    supplements = [
        nid for nid in (parsed.get("supplement_note_ids") or [])
        if str(nid) in valid_ids and str(nid) != main
    ]
    adoption = str(parsed.get("adoption") or "overall")
    if adoption not in {"overall", "outfit_only", "visual_only", "narrative_only"}:
        adoption = "overall"
    # 页级选材：只接受主参考笔记里真实存在的页码，按 adoption 语义约束
    main_analysis = {}
    for cand in candidates:
        if cand.note_id == main:
            main_analysis = cand.analysis
            break
    known_seqs = {
        int(pr.get("seq") or 0)
        for pr in (main_analysis.get("page_roles") or [])
        if int(pr.get("seq") or 0) > 0
    }
    pages: List[Dict[str, Any]] = []
    for item in (parsed.get("pages") or []):
        if not isinstance(item, dict):
            continue
        try:
            seq = int(item.get("seq") or 0)
        except (TypeError, ValueError):
            continue
        purpose = str(item.get("purpose") or "")
        if purpose not in {"outfit_detail", "full_outfit", "visual_tone"}:
            purpose = "full_outfit"
        if seq > 0 and seq in known_seqs:
            pages.append({"note_id": main, "seq": seq, "purpose": purpose})
    if adoption == "narrative_only":
        pages = []
    # 去重（同页多用途保留第一条）
    seen = set()
    pages = [p for p in pages if not (p["seq"] in seen or seen.add(p["seq"]))]
    return SelectionResult(
        main_note_id=main,
        supplement_note_ids=supplements,
        adoption=adoption,
        rationale=str(parsed.get("rationale") or ""),
        pages=pages,
        rejected=[
            {"note_id": str(item.get("note_id") or ""), "reason": str(item.get("reason") or "")}
            for item in (parsed.get("rejected") or []) if isinstance(item, dict)
        ],
    )
