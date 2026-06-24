#!/usr/bin/env python3
"""voc-insight LLM 表达润色脚本（独立于确定性规则层，V2 — 收紧越权）。

V2 新增：
- 落库前先 DDL（CREATE TABLE + ALTER TABLE 补漏列）
- voc_insight_polish 带规则字段（confidence / hook_eligible / claim_validation 等）
- 候选过滤：默认只润色当前 usecase 可用的卖点；risk/observe 走 flag
- LLM 输出后二次校验（claim 白名单 / 长度 / risk 不得出卖点 hook）
- --llm-fake 支持测试

Usage:
  python3 scripts/polish_voc_insight.py --batch-id FM_TH_HAIRCLIP_20260622_165607 \\
    --scope category --usecase ads_mixcut --limit 5 --write

  测试（fake client）:
  python3 tests/test_polish.py
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

SKILL_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SKILL_ROOT)
sys.path.insert(0, os.path.join(SKILL_ROOT, "references"))

# 必须在 import llm_polish_client 之前 .env
_WORKSPACE_ROOT = os.path.dirname(os.path.dirname(SKILL_ROOT))
_ENV_PATH = os.path.join(_WORKSPACE_ROOT, ".env")
if os.path.exists(_ENV_PATH):
    for _line in open(_ENV_PATH, encoding="utf-8"):
        _line = _line.strip()
        if not _line or _line.startswith("#") or "=" not in _line:
            continue
        _k, _v = _line.split("=", 1)
        _k, _v = _k.strip(), _v.strip()
        if len(_v) >= 2 and _v[0] == _v[-1] and _v[0] in "'\"":
            _v = _v[1:-1]
        if _k and _k not in os.environ:
            os.environ[_k] = _v

try:
    import pymysql
except ImportError:
    pymysql = None  # type: ignore

from core.llm_polish_client import get_llm_client, PolishLLMClient  # noqa: E402
from references.polish_prompts import (  # noqa: E402
    build_polish_prompt, system_prompt_for, market_lang_name,
)


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

RISK_ONLY_TYPES = {"fulfillment_issue", "pain_point", "risk_guard"}
OBSERVE_ONLY = "observe_only"
VIDEO_USAGE_LANES = {"video_hook", "video_support"}
MIN_ADS_VIDEO_FIT_SCORE = 70
CONFIDENCE_RANK = {
    OBSERVE_ONLY: 0,
    "partial_candidate": 1,
    "form_candidate": 2,
    "ads_candidate": 3,
    "category_candidate": 3,
}

LEN_LIMITS = {
    "title_zh": 25,
    "local_voice": 80,
    "hook": 80,
}

# 每个 signal_tag 的表达白名单（中文）
CLAIM_WHITELIST: Dict[str, List[str]] = {
    "appearance_cute_color": [
        "好看", "可爱", "颜色", "造型", "上头", "颜值", "款", "美", "萌",
        "น่า", "สวย", "น่ารัก", "สี", "ตรงปก", "cute", "pretty",
    ],
    "hold_quality": [
        "夹", "稳", "质感", "耐用", "固定", "牢", "紧", "หนีบ", "แน่น",
        "แข็ง", "คงทน", "คุณภาพ", "ติด", "ทน",
    ],
    "fast_shipping": [
        "发货快", "到货快", "物流快", "很快", "ส่งไว", "เร็ว", "จัดส่ง",
    ],
    "value_quantity": [
        "数量", "多", "性价比", "组合装", "套装", "便宜", "价格", "值",
        "量", "คุ้ม", "เยอะ", "ราคา", "ชุด", "หลาย",
    ],
    "slow_shipping": [
        "物流慢", "发货慢", "等", "慢", "รอนาน", "ส่งช้า", "นาน", "ช้า",
    ],
    "fulfillment_missing": [
        "少发", "漏发", "不完整", "ขาด", "ไม่ครบ",
    ],
}

# claim 黑名单 — 任何 insight 都不可下这类功能性断言
# 除非对应 signal_tag 有明确授权
FORBIDDEN_CLAIMS: Dict[str, List[re.Pattern]] = {
    "general": [
        re.compile(p, re.IGNORECASE) for p in [
            r"不疼", r"不痛", r"无痛", r"防滑", r"防水", r"防汗",
            r"不掉色", r"不褪色", r"不伤发", r"不伤害头发", r"护发",
            r"养发", r"修复", r"持久[一1]整天", r"全天[不无]",
            r"过敏", r"抗过敏", r"无刺激",
            r"ไม่เจ็บ", r"ไม่ปวด", r"ไม่ดึงผม", r"ไม่ทำร้ายผม",
            r"ผมไม่เสีย", r"ไม่เสียผม", r"ไม่ทำให้ผมเสีย", r"ทั้งวัน",
            r"ตลอดวัน", r"ไม่หลุดทั้งวัน", r"all\\s*day", r"pain[- ]?free",
            r"no\\s*pain", r"doesn'?t\\s*hurt", r"no\\s*damage",
        ]
    ],
}


# --------------------------------------------------------------------------- #
# DDL
# --------------------------------------------------------------------------- #

POLISH_DDL = """CREATE TABLE IF NOT EXISTS voc_insight_polish (
  polish_id VARCHAR(191) NOT NULL PRIMARY KEY,
  batch_id VARCHAR(191) NOT NULL,
  run_id VARCHAR(191) NOT NULL,
  insight_id VARCHAR(191) NOT NULL,
  scope VARCHAR(32) NOT NULL,
  scope_key VARCHAR(191) NOT NULL,
  usecase VARCHAR(64) NOT NULL,
  market VARCHAR(32) NOT NULL,
  model VARCHAR(128) NOT NULL,
  title_zh_original VARCHAR(512) NULL,
  local_voice_original VARCHAR(512) NULL,
  title_zh_polished VARCHAR(512) NULL,
  local_voice_polished VARCHAR(512) NULL,
  hooks_json JSON,
  reason_zh TEXT,
  raw_llm_json LONGTEXT,
  polish_status VARCHAR(64) NOT NULL,
  error_message TEXT,
  confidence VARCHAR(64) NULL,
  insight_type VARCHAR(64) NULL,
  insight_role VARCHAR(64) NULL,
  recommended_usecases_json JSON,
  not_for_usecases_json JSON,
  risk_notes_json JSON,
  evidence_refs_json JSON,
  hook_eligible TINYINT DEFAULT 0,
  claim_validation_status VARCHAR(32) DEFAULT 'pending',
  claim_validation_notes_json JSON,
  created_at VARCHAR(64) NOT NULL,
  updated_at VARCHAR(64) NOT NULL,
  UNIQUE KEY uq_voc_polish (run_id, insight_id, usecase),
  INDEX idx_voc_polish_batch (batch_id, insight_id),
  INDEX idx_voc_polish_hook_eligible (batch_id, usecase, hook_eligible, claim_validation_status, polish_status)
) CHARACTER SET utf8mb4"""

POLISH_COLUMNS_V2 = [
    ("confidence", "VARCHAR(64) NULL"),
    ("insight_type", "VARCHAR(64) NULL"),
    ("insight_role", "VARCHAR(64) NULL"),
    ("recommended_usecases_json", "JSON"),
    ("not_for_usecases_json", "JSON"),
    ("risk_notes_json", "JSON"),
    ("evidence_refs_json", "JSON"),
    ("hook_eligible", "TINYINT DEFAULT 0"),
    ("claim_validation_status", "VARCHAR(32) DEFAULT 'pending'"),
    ("claim_validation_notes_json", "JSON"),
]

POLISH_INDEXES_V2 = [
    (
        "idx_voc_polish_hook_eligible",
        "voc_insight_polish",
        "batch_id, usecase, hook_eligible, claim_validation_status, polish_status",
    ),
]


def _create_index_if_not_exists(cur, index_name: str, table: str, columns: str) -> Optional[str]:
    try:
        cur.execute("SELECT 1 FROM information_schema.statistics "
                    "WHERE table_schema=DATABASE() AND table_name=%s AND index_name=%s LIMIT 1",
                    (table, index_name))
        if cur.fetchone():
            return None
        cur.execute("CREATE INDEX {} ON {} ({})".format(index_name, table, columns))
        return "+ index {}".format(index_name)
    except Exception as e:
        return "! index {} failed: {}".format(index_name, e)


# --------------------------------------------------------------------------- #
# DB helpers
# --------------------------------------------------------------------------- #

def connect_db(url: Optional[str] = None):
    if pymysql is None:
        raise RuntimeError("pymysql not installed. Run: python3 -m pip install --user pymysql")
    url = url or os.environ.get("VOC_INSIGHT_DATABASE_URL") or os.environ.get("LIKEU_AI_DATABASE_URL") \
        or os.environ.get("HERMES_AGENT_DATABASE_URL") or ""
    if not url:
        raise RuntimeError("No database URL. Set LIKEU_AI_DATABASE_URL or pass --database-url.")
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    return pymysql.connect(
        host=parsed.hostname, port=parsed.port or 3306,
        user=unquote(parsed.username or ""), password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        charset=query.get("charset", ["utf8mb4"])[0],
        connect_timeout=20, read_timeout=60, write_timeout=60, autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_schema(conn) -> List[str]:
    """CREATE TABLE IF NOT EXISTS + ALTER TABLE 补漏列 + 补索引。返回变更摘要。"""
    cur = conn.cursor()
    changes: List[str] = []
    # 1. CREATE TABLE
    cur.execute(POLISH_DDL)
    # 2. ALTER TABLE 补漏列
    cur.execute("SELECT COLUMN_NAME AS column_name FROM information_schema.columns "
                "WHERE table_schema=DATABASE() AND table_name='voc_insight_polish'")
    existing = {r["column_name"] for r in cur.fetchall()}
    for col_name, col_type in POLISH_COLUMNS_V2:
        if col_name in existing:
            continue
        sql = "ALTER TABLE voc_insight_polish ADD COLUMN {} {}".format(col_name, col_type)
        try:
            cur.execute(sql)
            changes.append("+ column {}".format(col_name))
        except Exception as e:
            changes.append("! column {} failed: {}".format(col_name, e))
    # 3. 补索引
    for index_name, table, columns in POLISH_INDEXES_V2:
        change = _create_index_if_not_exists(cur, index_name, table, columns)
        if change:
            changes.append(change)
    conn.commit()
    return changes


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def jload(v: Any) -> Any:
    if v is None or v == "":
        return None
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v)
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# Load artifacts
# --------------------------------------------------------------------------- #

def load_form_artifacts(conn, batch_id: str, product_form: Optional[str]) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    if product_form:
        cur.execute(
            "SELECT artifact_id, run_id, batch_id, market, category_key, product_form, "
            "confidence_level, product_count, voc_count, insight_payload_json "
            "FROM voc_form_insight_artifact WHERE batch_id=%s AND product_form=%s "
            "ORDER BY voc_count DESC",
            (batch_id, product_form),
        )
    else:
        cur.execute(
            "SELECT artifact_id, run_id, batch_id, market, category_key, product_form, "
            "confidence_level, product_count, voc_count, insight_payload_json "
            "FROM voc_form_insight_artifact WHERE batch_id=%s ORDER BY voc_count DESC",
            (batch_id,),
        )
    rows = cur.fetchall()
    out = []
    for r in rows:
        insights = jload(r.pop("insight_payload_json")) or []
        r["insights"] = insights
        out.append(r)
    return out


def load_category_artifact(conn, batch_id: str) -> Optional[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        "SELECT a.artifact_id, a.run_id, a.batch_id, a.market, a.category_key, a.confidence_level, "
        "a.product_count, a.voc_count, a.insight_payload_json "
        "FROM voc_category_insight_artifact a "
        "JOIN voc_insight_run r ON r.run_id=a.run_id "
        "WHERE a.batch_id=%s AND r.scope='category' "
        "ORDER BY a.updated_at DESC LIMIT 1",
        (batch_id,),
    )
    r = cur.fetchone()
    if not r:
        return None
    r["insights"] = jload(r.pop("insight_payload_json")) or []
    r["product_form"] = None
    return r


# --------------------------------------------------------------------------- #
# Candidate filtering (V2)
# --------------------------------------------------------------------------- #

def _insight_usecase_allowed(insight: Dict[str, Any], usecase: str) -> bool:
    """该 insight 是否可用于当前 usecase（不在 not_for 里）。"""
    not_for = set(insight.get("not_for_usecases") or [])
    return usecase not in not_for


def _usecase_in_recommended(insight: Dict[str, Any], usecase: str) -> bool:
    rec = set(insight.get("recommended_usecases") or [])
    return usecase in rec


def _ads_video_provable(insight: Dict[str, Any]) -> bool:
    lane = str(insight.get("usage_lane") or "").strip()
    score = int(insight.get("video_fit_score") or 0)
    return lane in VIDEO_USAGE_LANES and score >= MIN_ADS_VIDEO_FIT_SCORE


def filter_candidates(
    candidates: List[Dict[str, Any]],
    usecase: str,
    include_risk: bool = False,
    include_observe: bool = False,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """返回 (polishable, skipped)。

    默认只润色「当前 usecase 可用」的卖点：
    - insight_type 不是 risk_only
    - confidence != observe_only
    - usecase 在 recommended_usecases 里
    - ads_mixcut 只允许 product_core_selling_point 进入主 hook 池

    开启 --include-risk-notes 追加 risk 类（但 hook_eligible=false）。
    开启 --include-observe 追加 observe_only 类（但 hook_eligible=false）。
    """
    polished: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for c in candidates:
        ins = c["insight"]
        itype = ins.get("insight_type", "")
        role = ins.get("insight_role") or "product_core_selling_point"
        conf = ins.get("confidence", "")
        risk_type = itype in RISK_ONLY_TYPES
        is_observe = conf == OBSERVE_ONLY

        if risk_type:
            if include_risk:
                c["hook_eligible"] = False
                polished.append(c)
            else:
                c["skip_reason"] = "risk_type_excluded: {}".format(itype)
                skipped.append(c)
            continue

        if is_observe:
            if include_observe:
                c["hook_eligible"] = False
                polished.append(c)
            else:
                c["skip_reason"] = "observe_only_excluded"
                skipped.append(c)
            continue

        if not _usecase_in_recommended(ins, usecase):
            c["skip_reason"] = "usecase_not_recommended: {}".format(usecase)
            skipped.append(c)
            continue

        if usecase == "ads_mixcut" and role != "product_core_selling_point":
            c["skip_reason"] = "role_not_ads_hook: {}".format(role)
            skipped.append(c)
            continue

        if usecase == "ads_mixcut" and not _ads_video_provable(ins):
            c["skip_reason"] = "not_video_provable: lane={} score={}".format(
                ins.get("usage_lane") or "", ins.get("video_fit_score") or 0
            )
            skipped.append(c)
            continue

        c["hook_eligible"] = True
        polished.append(c)
    return polished, skipped


# --------------------------------------------------------------------------- #
# Claim validation (V2)
# --------------------------------------------------------------------------- #

def _check_lengths(hooks: List[str], title_zh: str, local_voice: str) -> List[str]:
    notes = []
    if len(title_zh) > LEN_LIMITS["title_zh"]:
        notes.append("title_zh too long ({} > {})".format(len(title_zh), LEN_LIMITS["title_zh"]))
    if len(local_voice) > LEN_LIMITS["local_voice"]:
        notes.append("local_voice too long ({} > {})".format(len(local_voice), LEN_LIMITS["local_voice"]))
    for i, h in enumerate(hooks):
        if len(h) > LEN_LIMITS["hook"]:
            notes.append("hook[{}] too long ({} > {})".format(i, len(h), LEN_LIMITS["hook"]))
    return notes


def _check_risk_no_selling(hooks: List[str], title_zh: str, itype: str) -> List[str]:
    """risk 类不得生成卖点型 hooks / title。"""
    if itype not in RISK_ONLY_TYPES:
        return []
    notes = []
    selling_keywords = ["推荐", "必买", "种草", "超值", "好用", "强推", "入手"]
    for i, h in enumerate(hooks):
        for kw in selling_keywords:
            if kw in h:
                notes.append("risk hook[{}] contains selling keyword '{}'".format(i, kw))
    for kw in selling_keywords:
        if kw in title_zh:
            notes.append("risk title_zh contains selling keyword '{}'".format(kw))
    return notes


def _check_price_value_context(hooks: List[str], title_zh: str, itype: str) -> List[str]:
    """price_value 必须体现组合装/多件装前提。"""
    if itype != "price_value":
        return []
    notes = []
    ctx_words = ["组合", "套装", "多件", "多款", "多色", "set", "ชุด", "หลาย", "กี่", "คละ"]
    if not any(w in title_zh for w in ctx_words):
        notes.append("price_value title_zh missing multi-pack context")
    all_hook_text = " | ".join(hooks)
    if not any(w in all_hook_text for w in ctx_words):
        notes.append("price_value hooks missing multi-pack context")
    return notes


def _check_unauthorized_claims(
    hooks: List[str], title_zh: str, reason_zh: str,
    signal_tags: List[str], itype: str,
) -> List[str]:
    """检查是否出现了信号标签未授权的功能性 claim。"""
    notes = []
    # 收集允许词
    allowed: List[str] = []
    for tag in signal_tags:
        allowed.extend(CLAIM_WHITELIST.get(tag, []))
    # 合并待检文本
    combined = " | ".join([title_zh, reason_zh] + hooks)
    # 黑名单命中
    for cat, patterns in FORBIDDEN_CLAIMS.items():
        for pat in patterns:
            for i, h in enumerate(hooks):
                if pat.search(h):
                    notes.append("unauthorized: hook[{}] matches forbidden '{}'".format(i, pat.pattern))
            if pat.search(title_zh):
                notes.append("unauthorized: title_zh matches forbidden '{}'".format(pat.pattern))
    return notes


def validate_claims(
    polish: Dict[str, Any],
    insight: Dict[str, Any],
    hook_eligible: bool,
) -> Dict[str, Any]:
    """对润色结果做二次校验，返回 (validation_status, notes)。"""
    hooks = polish.get("hooks") or []
    title_zh = polish.get("title_zh_polished") or ""
    local_voice = polish.get("local_voice_polished") or ""
    reason_zh = polish.get("reason_zh") or ""
    itype = insight.get("insight_type", "")
    role = insight.get("insight_role") or "product_core_selling_point"
    signal_tags = insight.get("signal_tags") or []
    conf = insight.get("confidence", "")

    notes: List[str] = []

    # 1. JSON 完整性（call_json 已保证，这里不重复，但可追加）
    if not title_zh.strip():
        notes.append("missing title_zh_polished")
    if not hooks:
        notes.append("hooks empty or missing")

    # 2. 长度
    notes.extend(_check_lengths(hooks, title_zh, local_voice))

    # 3. risk 不得生成卖点型
    notes.extend(_check_risk_no_selling(hooks, title_zh, itype))

    # 4. observe_only 不得 hook_eligible
    if conf == OBSERVE_ONLY and hook_eligible:
        notes.append("observe_only but hook_eligible=true")

    # 4b. ADS 主 hook 池只允许产品核心卖点。
    if hook_eligible and role != "product_core_selling_point":
        notes.append("non_core_role but hook_eligible=true: {}".format(role))

    # 4c. ADS 主 hook 池只允许能被镜头证明的洞察。
    if hook_eligible and not _ads_video_provable(insight):
        notes.append("not_video_provable but hook_eligible=true: lane={} score={}".format(
            insight.get("usage_lane") or "", insight.get("video_fit_score") or 0
        ))

    # 5. 无证据 claim
    notes.extend(_check_unauthorized_claims(hooks, title_zh, reason_zh, signal_tags, itype))

    # 6. price_value 上下文
    notes.extend(_check_price_value_context(hooks, title_zh, itype))

    has_errors = bool(notes)
    status = "failed" if has_errors else "passed"
    return {
        "claim_validation_status": status,
        "claim_validation_notes": notes,
        "claim_validation_notes_json": notes,
    }


# --------------------------------------------------------------------------- #
# Polish
# --------------------------------------------------------------------------- #

def polish_insight(client, insight: Dict[str, Any], scope: str,
                   scope_key: str, usecase: str, market: str) -> Dict[str, Any]:
    """调 LLM 润色单条 insight。client 可以是 PolishLLMClient 或 FakePolishClient。"""
    market_lang = market_lang_name(market)
    sys_prompt = system_prompt_for(usecase)
    user_prompt = build_polish_prompt(insight, market_lang)
    raw = client.call_json(user_prompt, system_prompt=sys_prompt, max_tokens=1024, max_retries=3)
    return {
        "title_zh_polished": str(raw.get("title_zh") or "").strip(),
        "local_voice_polished": str(raw.get("local_voice") or "").strip(),
        "hooks": [str(h).strip() for h in (raw.get("hooks") or []) if str(h).strip()],
        "reason_zh": str(raw.get("reason_zh") or "").strip(),
        "raw_llm_json": raw,
        "polish_status": "completed",
        "error_message": None,
    }


# --------------------------------------------------------------------------- #
# Fake LLM client (for tests)
# --------------------------------------------------------------------------- #

_FAKE_RESPONSES: Dict[str, Dict[str, Any]] = {
    "selling_appearance_cute_color": {
        "title_zh": "款色讨喜，买家反复夸好看",
        "local_voice": "สวย น่ารัก สีตรงปกเลยค่ะ",
        "hooks": [
            "ติดผมแล้วน่ารักมาก สีตรงปกจริงๆ",
            "เพื่อนทักว่าสวย น่ารักดีมาก",
        ],
        "reason_zh": "35款商品122条VOC提到款式与颜色正向反馈",
    },
    "selling_hold_quality": {
        "title_zh": "夹得稳！质感耐用不松",
        "local_voice": "หนีบแน่น คุณภาพดีใช้ได้จริง",
        "hooks": [
            "ไปทำงานหนีบไม่หลุด คุณภาพดี",
            "ไม่หลุดง่าย หนีบแน่นทนทานค่ะ",
        ],
        "reason_zh": "19款商品31条VOC提到夹得稳、耐用",
    },
    "selling_fast_shipping": {
        "title_zh": "发货快，辅助信任标签",
        "local_voice": "ส่งไวมากค่ะ",
        "hooks": ["สั่งวันนี้ได้ของเร็วมาก คุ้มค่ะ"],
        "reason_zh": "12款商品16条VOC反馈发货快",
    },
    "selling_value_quantity": {
        "title_zh": "组合装量大价优！性价比高",
        "local_voice": "ได้เยอะมาก คุ้มค่าเลย",
        "hooks": [
            "ซื้อเป็นชุดได้เยอะ คุ้มกว่าซื้อแยก",
            "หลายชิ้น ใช้ได้นานเลยค่ะ",
        ],
        "reason_zh": "12款商品17条VOC提及数量多、性价比",
    },
    "pain_slow_shipping": {
        "title_zh": "物流偏慢？耐心等的买家才适合",
        "local_voice": "รอของนานไปหน่อยนะคะ",
        "hooks": [
            "คนรีบห้ามสั่งนะ รอนานหน่อย",
            "ถ้าเร่งใช้แนะนำหาร้านที่ส่งไวกว่า",
        ],
        "reason_zh": "9个商品13条买家反馈提到物流较慢",
    },
    "pain_fulfillment_missing": {
        "title_zh": "组合装有少发风险需注意",
        "local_voice": "ได้ของไม่ครบบ้างนะคะ",
        "hooks": ["เช็กจำนวนก่อนเปิดซองกันด้วยนะ"],
        "reason_zh": "存在少发/漏发个别买家反馈",
    },
    # --- 故意违规的 fake 响应（用于测试校验逻辑） ---
    "selling_appearance_cute_color_BAD_CLAIM": {
        "title_zh": "不疼不伤发！仙女夹闭眼入",
        "local_voice": "ไม่เจ็บเลย",
        "hooks": ["หนีบแล้วไม่เจ็บผมเลย ไม่ดึงผม ดีมาก"],
        "reason_zh": "买家说不疼不伤发",
    },
    "selling_hold_quality_OVERLEN": {
        "title_zh": "夹得稳质感耐用不松垮非常好用推荐入手种草必买超值爆款",
        "local_voice": "x" * 90,
        "hooks": ["x" * 100],
        "reason_zh": "ok",
    },
    "pain_slow_shipping_BAD_SELLING": {
        "title_zh": "物流慢但超值！必买好物推荐种草",
        "local_voice": "รอของนานแต่คุ้ม ดีมาก",
        "hooks": ["แนะนำเลย คุ้มมาก แนะนำทุกร้าน", "强推入手不亏"],
        "reason_zh": "虽然物流慢但是值得推荐",
    },
    "selling_value_quantity_NO_MULTIPACK": {
        "title_zh": "价格很值！超实惠的",
        "local_voice": "คุ้มค่าราคาดีค่ะ",
        "hooks": ["ราคาดี สีสวย คุ้มค่าค่ะ"],
        "reason_zh": "价格实惠值得买",
    },
}


class FakePolishClient:
    """测试用 fake LLM 客户端，按 insight_id 返回预设响应。"""
    def __init__(self, model: str = "fake", responses: Optional[Dict[str, Dict]] = None):
        self._model = model
        self._responses = responses or _FAKE_RESPONSES
        self.using_responses_api = False  # type: ignore
        self.model = model
        self.api_url = "fake://test"

    def call_json(self, prompt: str, system_prompt: Optional[str] = None,
                  max_tokens: int = 1024, max_retries: int = 1) -> Dict[str, Any]:
        # prompt 里包含 insight_id，提取出来匹配
        for iid in self._responses:
            if iid in prompt:
                return dict(self._responses[iid])
        # default fallback
        return {"title_zh": "fake_title", "local_voice": "fake_voice",
                "hooks": ["fake hook 1", "fake hook 2"], "reason_zh": "fake reason"}


# --------------------------------------------------------------------------- #
# Persist
# --------------------------------------------------------------------------- #

def persist_polish(conn, batch_id: str, run_id: str, insight: Dict[str, Any],
                   scope: str, scope_key: str, usecase: str, market: str,
                   model: str, polish: Dict[str, Any],
                   hook_eligible: bool,
                   claim_validation_status: str,
                   claim_validation_notes_json: Any) -> str:
    polish_id = "{}__{}__{}__{}".format(run_id, insight["insight_id"], scope, usecase)
    ts = now_iso()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO voc_insight_polish "
        "(polish_id, batch_id, run_id, insight_id, scope, scope_key, usecase, market, model, "
        "title_zh_original, local_voice_original, title_zh_polished, local_voice_polished, "
        "hooks_json, reason_zh, raw_llm_json, polish_status, error_message, "
        "confidence, insight_type, insight_role, recommended_usecases_json, not_for_usecases_json, "
        "risk_notes_json, evidence_refs_json, hook_eligible, "
        "claim_validation_status, claim_validation_notes_json, created_at, updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE model=VALUES(model), title_zh_polished=VALUES(title_zh_polished), "
        "local_voice_polished=VALUES(local_voice_polished), hooks_json=VALUES(hooks_json), "
        "reason_zh=VALUES(reason_zh), raw_llm_json=VALUES(raw_llm_json), "
        "polish_status=VALUES(polish_status), error_message=VALUES(error_message), "
        "confidence=VALUES(confidence), insight_type=VALUES(insight_type), insight_role=VALUES(insight_role), "
        "recommended_usecases_json=VALUES(recommended_usecases_json), "
        "not_for_usecases_json=VALUES(not_for_usecases_json), "
        "risk_notes_json=VALUES(risk_notes_json), evidence_refs_json=VALUES(evidence_refs_json), "
        "hook_eligible=VALUES(hook_eligible), "
        "claim_validation_status=VALUES(claim_validation_status), "
        "claim_validation_notes_json=VALUES(claim_validation_notes_json), updated_at=VALUES(updated_at)",
        (polish_id, batch_id, run_id, insight["insight_id"], scope, scope_key, usecase, market,
         model, insight.get("title_zh", ""), insight.get("local_voice", ""),
         polish["title_zh_polished"], polish["local_voice_polished"],
         json.dumps(polish["hooks"], ensure_ascii=False), polish["reason_zh"],
         json.dumps(polish["raw_llm_json"], ensure_ascii=False),
         polish["polish_status"], polish["error_message"],
         insight.get("confidence"), insight.get("insight_type"), insight.get("insight_role", "product_core_selling_point"),
         json.dumps(insight.get("recommended_usecases") or [], ensure_ascii=False),
         json.dumps(insight.get("not_for_usecases") or [], ensure_ascii=False),
         json.dumps(insight.get("risk_notes") or [], ensure_ascii=False),
         json.dumps(insight.get("evidence_refs") or [], ensure_ascii=False),
         1 if hook_eligible else 0,
         claim_validation_status,
         json.dumps(claim_validation_notes_json, ensure_ascii=False),
         ts, ts),
    )
    return polish_id


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #

def collect_candidates(artifacts: List[Dict[str, Any]], args: argparse.Namespace
                       ) -> List[Dict[str, Any]]:
    seen: set = set()
    candidates: List[Dict[str, Any]] = []
    for art in artifacts:
        for ins in art.get("insights") or []:
            key = (ins.get("insight_id"), art.get("product_form") or art.get("category_key") or "")
            if key in seen:
                continue
            seen.add(key)
            candidates.append({
                "insight": ins,
                "scope": "category" if art.get("product_form") is None else "form",
                "scope_key": art.get("product_form") or art.get("category_key") or "",
                "run_id": art["run_id"],
                "market": art.get("market") or args.market or "UNKNOWN",
            })
    return candidates


def run_polish(args: argparse.Namespace) -> Dict[str, Any]:
    scope = args.scope
    usecase = args.usecase or "general"
    conn = connect_db(args.database_url)
    results: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    skipped_candidates: List[Dict[str, Any]] = []
    schema_changes: List[str] = []
    try:
        # ── 0. ensure DDL (before any persist) ──
        if args.write:
            schema_changes = ensure_schema(conn)

        # ── 1. load artifacts ──
        artifacts: List[Dict[str, Any]] = []
        if scope in ("form", "product"):
            artifacts = load_form_artifacts(conn, args.batch_id, args.product_form)
        if scope in ("category", "product"):
            cat = load_category_artifact(conn, args.batch_id)
            if cat:
                artifacts.append(cat)
        if not artifacts:
            raise RuntimeError("No artifacts found for batch {} scope {}. Run run_voc_insight.py --write first.".format(
                args.batch_id, scope))

        # ── 2. collect + filter candidates ──
        all_candidates = collect_candidates(artifacts, args)
        candidates, skipped_candidates = filter_candidates(
            all_candidates, usecase,
            include_risk=bool(args.include_risk_notes),
            include_observe=bool(args.include_observe),
        )
        if args.limit and args.limit > 0:
            candidates = candidates[:args.limit]

        # ── 3. LLM polish ──
        if args.llm_fake:
            client = FakePolishClient(model=args.llm_model or "fake")
        else:
            client = get_llm_client(args.llm_api_url, args.llm_api_key, args.llm_model)
        backend = "fake" if args.llm_fake else ("codex" if client.using_responses_api else "chat")

        for idx, c in enumerate(candidates, 1):
            ins = c["insight"]
            hook_eligible = c.get("hook_eligible", False)
            print("  [{}/{}] {} ({}) hook_eligible={} ...".format(
                idx, len(candidates), ins.get("insight_id"), c["scope"], hook_eligible),
                  file=sys.stderr, flush=True)
            try:
                polish = polish_insight(client, ins, c["scope"], c["scope_key"], usecase, c["market"])

                # ── V2: claim validation ──
                validation = validate_claims(polish, ins, hook_eligible)
                if validation["claim_validation_status"] == "failed" and hook_eligible:
                    hook_eligible = False

                entry = {
                    "polish_id": "{}__{}__{}__{}".format(c["run_id"], ins["insight_id"], c["scope"], usecase),
                    "batch_id": args.batch_id,
                    "run_id": c["run_id"],
                    "insight_id": ins["insight_id"],
                    "scope": c["scope"],
                    "scope_key": c["scope_key"],
                    "usecase": usecase,
                    "market": c["market"],
                    "model": client.model,
                    "hook_eligible": hook_eligible,
                    "title_zh_original": ins.get("title_zh", ""),
                    "local_voice_original": ins.get("local_voice", ""),
                    "claim_validation_status": validation["claim_validation_status"],
                    "claim_validation_notes": validation["claim_validation_notes"],
                    "confidence": ins.get("confidence"),
                    "insight_type": ins.get("insight_type"),
                    "insight_role": ins.get("insight_role", "product_core_selling_point"),
                    "recommended_usecases_json": ins.get("recommended_usecases"),
                    "not_for_usecases_json": ins.get("not_for_usecases"),
                    "risk_notes_json": ins.get("risk_notes"),
                    "evidence_refs_json": ins.get("evidence_refs"),
                    **polish,
                }
                results.append(entry)
                if args.write:
                    persist_polish(conn, args.batch_id, c["run_id"], ins, c["scope"],
                                   c["scope_key"], usecase, c["market"], client.model, polish,
                                   entry["hook_eligible"],
                                   validation["claim_validation_status"],
                                   validation["claim_validation_notes"])
            except Exception as e:
                errors.append({"insight_id": ins.get("insight_id"), "scope": c["scope"], "error": str(e)})
                if args.write:
                    polish_err = {
                        "title_zh_polished": "", "local_voice_polished": "", "hooks": [],
                        "reason_zh": "", "raw_llm_json": {}, "polish_status": "error",
                        "error_message": str(e),
                    }
                    try:
                        persist_polish(conn, args.batch_id, c["run_id"], ins, c["scope"],
                                       c["scope_key"], usecase, c["market"], client.model, polish_err,
                                       False, "pending", [])
                    except Exception:
                        pass
            if idx < len(candidates):
                time.sleep(args.sleep)

        if args.write:
            conn.commit()

        summary = {
            "batch_id": args.batch_id,
            "scope": scope,
            "usecase": usecase,
            "backend": backend,
            "model": client.model,
            "api_url": client.api_url if not args.llm_fake else "fake://",
            "total_candidates": len(all_candidates),
            "filtered_out": len(skipped_candidates),
            "polishable": len(candidates),
            "polished": len([r for r in results if r.get("polish_status") == "completed"]),
            "hook_eligible_count": len([r for r in results if r.get("hook_eligible")]),
            "claim_passed": len([r for r in results if r.get("claim_validation_status") == "passed"]),
            "claim_failed": len([r for r in results if r.get("claim_validation_status") == "failed"]),
            "errors": len(errors),
            "written": args.write,
            "schema_changes": schema_changes,
        }
        return {
            "summary": summary,
            "results": results,
            "skipped": skipped_candidates,
            "errors": errors,
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="voc-insight LLM 表达润色 V2")
    p.add_argument("--batch-id", required=True)
    p.add_argument("--scope", default="form", choices=["form", "category", "product"])
    p.add_argument("--product-form", default=None)
    p.add_argument("--usecase", default=None, choices=["ads_mixcut", "creator_brief", "content_copy", "selection", "general"])
    p.add_argument("--market", default=None)
    p.add_argument("--limit", type=int, default=0, help="只润色前 N 条（0=全部）")
    p.add_argument("--sleep", type=float, default=0.5)
    p.add_argument("--write", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--pretty", action="store_true")
    p.add_argument("--database-url", default=None)
    p.add_argument("--llm-api-url", default=None)
    p.add_argument("--llm-api-key", default=None)
    p.add_argument("--llm-model", default=None)
    # V2 flags
    p.add_argument("--include-risk-notes", action="store_true", help="允许润色 risk/pain/fulfillment 类（hook_eligible=false）")
    p.add_argument("--include-observe", action="store_true", help="允许润色 observe_only 类（hook_eligible=false）")
    p.add_argument("--llm-fake", action="store_true", help="使用 fake LLM client 测试")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_polish(args)
    except Exception as e:
        print("ERROR: {}".format(e), file=sys.stderr)
        return 1
    indent = 2 if args.pretty else None
    print(json.dumps(result, ensure_ascii=False, indent=indent, default=str))
    s = result["summary"]
    print("\n=== polish summary ===", file=sys.stderr)
    print("  backend={} model={} candidates={} filtered={} polishable={} polished={}".format(
        s["backend"], s["model"], s["total_candidates"], s["filtered_out"], s["polishable"], s["polished"]), file=sys.stderr)
    print("  hook_eligible={} claim_passed={} claim_failed={} errors={} written={}".format(
        s["hook_eligible_count"], s["claim_passed"], s["claim_failed"], s["errors"], s["written"]), file=sys.stderr)
    return 0 if not result["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
