"""素材分析与筛选缓存（自动图文供稿 Phase 1，方案 §五/§十）。

流程：程序预检（文件/格式/去重/已分析）→ Doubao 看图分析（缩图、分批、保序、
程序化合并）→ 结果写入消费侧台账缓存。同一素材（版本指纹 × 模型 × 分析版本）
的分析跨账号复用，不重复付费调用。

台账为消费侧独立 SQLite（默认 packages/organic_photo_video/var/，
可用 OPV_MATERIAL_LEDGER_DB 覆盖），与素材库（只读）和 OPV 主库（RDS）互不侵扰。
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from services.material_source import MaterialSource
from services.photo_reference_vision import parse_vision_envelope

ANALYSIS_VERSION = "material-analysis-v3"
DEFAULT_MODEL_ENV = "OPV_PHOTO_VISION_MODEL"
DEFAULT_MAX_IMAGE_EDGE = 768
DEFAULT_JPEG_QUALITY = 82
DEFAULT_BATCH_SIZE = 6

LEDGER_SCHEMA = """
CREATE TABLE IF NOT EXISTS material_analysis_cache (
    material_fingerprint TEXT NOT NULL,
    model                TEXT NOT NULL,
    analysis_version     TEXT NOT NULL,
    note_id              TEXT NOT NULL,
    result_json          TEXT NOT NULL,
    image_count          INTEGER NOT NULL,
    calls                INTEGER NOT NULL DEFAULT 0,
    prompt_tokens        INTEGER NOT NULL DEFAULT 0,
    completion_tokens    INTEGER NOT NULL DEFAULT 0,
    duration_ms          INTEGER NOT NULL DEFAULT 0,
    retries              INTEGER NOT NULL DEFAULT 0,
    created_at           TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (material_fingerprint, model, analysis_version)
);
CREATE TABLE IF NOT EXISTS analysis_call_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ran_at TEXT NOT NULL DEFAULT (datetime('now')),
    purpose TEXT NOT NULL,
    note_id TEXT,
    model TEXT NOT NULL,
    images INTEGER NOT NULL DEFAULT 0,
    prompt_tokens INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    error TEXT
);
CREATE TABLE IF NOT EXISTS material_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    note_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    task_ref TEXT,
    adoption TEXT,
    used_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS material_gaps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    scope TEXT NOT NULL,
    reason TEXT NOT NULL,
    detail TEXT
);
CREATE TABLE IF NOT EXISTS material_demands (
    demand_key TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    hit_count INTEGER NOT NULL DEFAULT 1,
    first_seen TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS budget_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    budget_day TEXT NOT NULL,
    purpose TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'reserved',
    note TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_budget_day_purpose ON budget_attempts (budget_day, purpose);
CREATE TABLE IF NOT EXISTS supply_slots (
    account_id TEXT NOT NULL,
    supply_date TEXT NOT NULL,
    slot INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'reserved',
    record_id TEXT,
    product_code TEXT,
    main_note_id TEXT,
    adoption TEXT,
    note TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (account_id, supply_date, slot)
);
CREATE TABLE IF NOT EXISTS analysis_failures (
    material_fingerprint TEXT NOT NULL,
    model TEXT NOT NULL,
    analysis_version TEXT NOT NULL,
    note_id TEXT NOT NULL,
    error TEXT,
    failed_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (material_fingerprint, model, analysis_version)
);
"""


def default_ledger_path() -> Path:
    override = os.environ.get("OPV_MATERIAL_LEDGER_DB")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[1] / "var" / "material_consumer.sqlite3"


class MaterialAnalysisBudgetError(RuntimeError):
    """当日调用额度用尽（方案 A3）。"""


class MaterialLedger:
    """消费侧台账：分析缓存、调用成本、使用历史、素材缺口。"""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = Path(db_path) if db_path else default_ledger_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(LEDGER_SCHEMA)
        # 既有库迁移（2026-09-16 租约修复）：supply_slots 加 owner/lease_until
        columns = {row[1] for row in self._conn.execute(
            "PRAGMA table_info(supply_slots)")}
        for column in ("owner", "lease_until"):
            if column not in columns:
                self._conn.execute(
                    f"ALTER TABLE supply_slots ADD COLUMN {column}"
                    " TEXT NOT NULL DEFAULT ''")
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ---- 素材需求台账（方案 C1：缺口即异步采集需求，相同需求合并）----
    def record_demand(self, demand: Dict[str, Any]) -> None:
        import hashlib as _h
        key = _h.sha256("|".join(str(demand.get(k) or "") for k in (
            "theme_direction", "purposes", "product_form",
            "destination_country", "destination_use")).encode(
                "utf-8")).hexdigest()[:24]
        payload = json.dumps(demand, ensure_ascii=False)
        with self._conn:
            self._conn.execute(
                "INSERT INTO material_demands (demand_key, payload)"
                " VALUES (?,?) ON CONFLICT(demand_key) DO UPDATE SET"
                " hit_count=hit_count+1, last_seen=datetime('now'),"
                " payload=excluded.payload", (key, payload))

    def list_demands(self, *, within_days: int = 14) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT payload, hit_count, last_seen FROM material_demands"
            " WHERE last_seen >= datetime('now', ?)"
            " ORDER BY hit_count DESC, last_seen DESC",
            (f"-{int(within_days)} days",)).fetchall()
        out = []
        for row in rows:
            try:
                data = json.loads(row["payload"])
            except (TypeError, ValueError):
                continue
            data["hit_count"] = int(row["hit_count"])
            data["last_seen"] = str(row["last_seen"])
            out.append(data)
        return out

    # ---- 逐调用额度预留（方案 A3：短事务检查+预留，多 worker 原子）----
    @staticmethod
    def budget_today() -> str:
        """预算日窗口：按配置时区计算（OPV_BUDGET_TIMEZONE，默认亚洲/曼谷）。"""
        import os
        from datetime import datetime
        from zoneinfo import ZoneInfo
        tz_name = os.environ.get("OPV_BUDGET_TIMEZONE") or "Asia/Bangkok"
        try:
            return datetime.now(ZoneInfo(tz_name)).date().isoformat()
        except Exception:  # noqa: BLE001 - 非法时区名回落本地日
            return datetime.now().date().isoformat()

    def reserve_budget(self, *, purpose: str, cap: int, note: str = "") -> bool:
        """检查并预留一次调用额度。成功/已发起失败/结果未知都占额度；
        只有确认未发起的取消可 release。BEGIN IMMEDIATE 保证跨 worker 原子。"""
        day = self.budget_today()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM budget_attempts"
                " WHERE budget_day=? AND purpose=?"
                " AND state IN ('reserved','consumed','unknown')",
                (day, purpose)).fetchone()
            if int(row["n"]) >= int(cap):
                self._conn.execute("ROLLBACK")
                return False
            self._conn.execute(
                "INSERT INTO budget_attempts (budget_day, purpose, state, note)"
                " VALUES (?,?,'reserved',?)", (day, purpose, str(note)[:120]))
            self._conn.execute("COMMIT")
            return True
        except Exception:
            try:
                self._conn.execute("ROLLBACK")
            except Exception:
                pass
            raise

    def settle_budget(self, *, purpose: str, state: str, note: str = "") -> None:
        """结算本人最近一次 reserved：consumed（成功/失败但已发起）或
        unknown（结果未知）；只有未发起才允许 release（删除预留）。"""
        day = self.budget_today()
        if state == "release":
            self._conn.execute(
                "DELETE FROM budget_attempts WHERE id=("
                " SELECT id FROM budget_attempts WHERE budget_day=? AND purpose=?"
                " AND state='reserved' ORDER BY id DESC LIMIT 1)",
                (day, purpose))
        else:
            self._conn.execute(
                "UPDATE budget_attempts SET state=?, note=?,"
                " updated_at=datetime('now') WHERE id=("
                " SELECT id FROM budget_attempts WHERE budget_day=? AND purpose=?"
                " AND state='reserved' ORDER BY id DESC LIMIT 1)",
                (state, str(note)[:120], day, purpose))
        self._conn.commit()

    def budget_usage(self, purpose: str = "") -> Dict[str, int]:
        day = self.budget_today()
        if purpose:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM budget_attempts"
                " WHERE budget_day=? AND purpose=?"
                " AND state IN ('reserved','consumed','unknown')",
                (day, purpose)).fetchone()
            return {"attempts": int(row["n"])}
        rows = self._conn.execute(
            "SELECT purpose, COUNT(*) AS n FROM budget_attempts"
            " WHERE budget_day=? AND state IN ('reserved','consumed','unknown')"
            " GROUP BY purpose", (day,)).fetchall()
        return {str(r["purpose"]): int(r["n"]) for r in rows}

    # ---- 调用预算（方案 §9：失败重试也计入额度）----
    def daily_call_usage(self, today: str) -> Dict[str, int]:
        row = self._conn.execute(
            "SELECT COUNT(*) AS calls, COALESCE(SUM(images),0) AS images,"
            " COALESCE(SUM(prompt_tokens+completion_tokens),0) AS tokens"
            " FROM analysis_call_log"
            " WHERE status='ok' AND ran_at >= ?", (str(today) + " 00:00:00",)).fetchone()
        return {"calls": int(row["calls"]), "images": int(row["images"]),
                "tokens": int(row["tokens"])}

    def log_supply_call(self, *, purpose: str, model: str,
                        prompt_tokens: int = 0, completion_tokens: int = 0,
                        status: str = "ok", error: str = "") -> None:
        """终选等供给侧调用入账（复用 analysis_call_log，note_id 留空）。"""
        self._conn.execute(
            "INSERT INTO analysis_call_log (purpose, note_id, model, images,"
            " prompt_tokens, completion_tokens, duration_ms, status, error)"
            " VALUES (?, '', ?, 0, ?, ?, 0, ?, ?)",
            (str(purpose), str(model), int(prompt_tokens),
             int(completion_tokens), str(status), str(error)[:200]))
        self._conn.commit()

    # ---- 分析缓存 ----
    def get_cached_analysis(
        self, fingerprint: str, model: str, analysis_version: str
    ) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT result_json FROM material_analysis_cache"
            " WHERE material_fingerprint=? AND model=? AND analysis_version=?",
            (fingerprint, model, analysis_version),
        ).fetchone()
        return json.loads(row["result_json"]) if row else None

    def put_cached_analysis(
        self,
        *,
        fingerprint: str,
        model: str,
        analysis_version: str,
        note_id: str,
        result: Dict[str, Any],
        image_count: int,
        calls: int,
        prompt_tokens: int,
        completion_tokens: int,
        duration_ms: int,
        retries: int = 0,
    ) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO material_analysis_cache"
            " (material_fingerprint, model, analysis_version, note_id, result_json,"
            "  image_count, calls, prompt_tokens, completion_tokens, duration_ms, retries)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (fingerprint, model, analysis_version, note_id,
             json.dumps(result, ensure_ascii=False), image_count, calls,
             prompt_tokens, completion_tokens, duration_ms, retries),
        )
        self._conn.commit()

    def analyzed_note_ids(self, model: str, analysis_version: str) -> set:
        rows = self._conn.execute(
            "SELECT note_id FROM material_analysis_cache"
            " WHERE model=? AND analysis_version=?",
            (model, analysis_version),
        )
        return {r["note_id"] for r in rows.fetchall()}

    # ---- 成本与使用 ----
    def log_call(
        self, *, purpose: str, note_id: Optional[str], model: str, images: int,
        prompt_tokens: int, completion_tokens: int, duration_ms: int,
        status: str, error: Optional[str] = None,
    ) -> None:
        self._conn.execute(
            "INSERT INTO analysis_call_log"
            " (purpose, note_id, model, images, prompt_tokens, completion_tokens,"
            "  duration_ms, status, error) VALUES (?,?,?,?,?,?,?,?,?)",
            (purpose, note_id, model, images, prompt_tokens, completion_tokens,
             duration_ms, status, error),
        )
        self._conn.commit()

    def record_usage(self, *, note_id: str, account_id: str,
                     task_ref: Optional[str] = None, adoption: Optional[str] = None) -> None:
        self._conn.execute(
            "INSERT INTO material_usage (note_id, account_id, task_ref, adoption)"
            " VALUES (?,?,?,?)",
            (note_id, account_id, task_ref, adoption),
        )
        self._conn.commit()

    def recent_note_ids(self, *, account_id: Optional[str] = None, days: int = 14) -> set:
        rows = self._conn.execute(
            "SELECT DISTINCT note_id FROM material_usage"
            " WHERE used_at >= datetime('now', ?)"
            + (" AND account_id=?" if account_id else ""),
            tuple(filter(None, [f'-{int(days)} days', account_id])),
        ).fetchall()
        return {r["note_id"] for r in rows}

    def record_gap(self, *, scope: str, reason: str, detail: Optional[str] = None) -> None:
        self._conn.execute(
            "INSERT INTO material_gaps (scope, reason, detail) VALUES (?,?,?)",
            (scope, reason, detail),
        )
        self._conn.commit()

    # ---- 供稿名额（Phase 2） ----
    def reserve_slot(self, account_id: str, supply_date: str, slot: int) -> str:
        """预留名额。返回 newly_reserved / reserved（此前已预留未完成）/ created。"""
        row = self._conn.execute(
            "SELECT status FROM supply_slots WHERE account_id=? AND supply_date=? AND slot=?",
            (account_id, supply_date, slot),
        ).fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO supply_slots (account_id, supply_date, slot, status)"
                " VALUES (?,?,?,'reserved')",
                (account_id, supply_date, slot),
            )
            self._conn.commit()
            return "newly_reserved"
        return str(row["status"] or "reserved")

    # ---- 名额执行租约（2026-09-16 评审 §D：名额唯一键≠锁）----
    # 状态机：reserved（无主）→ running（持租执行）→ created（终态）。
    # 失败释放回 reserved，可重试；租约过期的名额可被其他 worker 原子接管。
    def acquire_slot_lease(
        self, account_id: str, supply_date: str, slot: int, *,
        owner: str, lease_seconds: int = 600,
    ) -> str:
        """原子获取单名额执行权。acquired / held_by_other / already_created。"""
        self._conn.execute(
            "INSERT OR IGNORE INTO supply_slots (account_id, supply_date, slot, status)"
            " VALUES (?,?,?,'reserved')", (account_id, supply_date, slot))
        cursor = self._conn.execute(
            "UPDATE supply_slots"
            " SET owner=?, lease_until=datetime('now','+{} seconds'),"
            "     status='running', updated_at=datetime('now')".format(
                int(max(30, lease_seconds)))
            + " WHERE account_id=? AND supply_date=? AND slot=? AND status!='created'"
              " AND (owner='' OR owner=? OR lease_until=''"
              "      OR lease_until < datetime('now'))",
            (owner, account_id, supply_date, slot, owner))
        self._conn.commit()
        if cursor.rowcount:
            return "acquired"
        row = self._conn.execute(
            "SELECT status FROM supply_slots"
            " WHERE account_id=? AND supply_date=? AND slot=?",
            (account_id, supply_date, slot)).fetchone()
        return ("already_created" if str(row["status"]) == "created"
                else "held_by_other") if row else "held_by_other"

    def renew_slot_lease(
        self, account_id: str, supply_date: str, slot: int, *,
        owner: str, lease_seconds: int = 600,
    ) -> bool:
        """长调用前续租；仅当仍由本 worker 持有时生效。"""
        cursor = self._conn.execute(
            "UPDATE supply_slots"
            " SET lease_until=datetime('now','+{} seconds'), updated_at=datetime('now')"
            .format(int(max(30, lease_seconds)))
            + " WHERE account_id=? AND supply_date=? AND slot=? AND owner=?"
              " AND status='running'",
            (account_id, supply_date, slot, owner))
        self._conn.commit()
        return bool(cursor.rowcount)

    def release_slot_lease(
        self, account_id: str, supply_date: str, slot: int, *, owner: str,
    ) -> None:
        """失败释放：回到 reserved 等待下轮；只有本 worker 持有时可释放。"""
        self._conn.execute(
            "UPDATE supply_slots SET status='reserved', owner='', lease_until='',"
            " updated_at=datetime('now')"
            " WHERE account_id=? AND supply_date=? AND slot=? AND owner=?"
            " AND status='running'",
            (account_id, supply_date, slot, owner))
        self._conn.commit()

    def complete_slot(self, account_id: str, supply_date: str, slot: int, *,
                      record_id: str, product_code: str = "", main_note_id: str = "",
                      adoption: str = "", note: str = "", owner: str = "") -> bool:
        """完成名额。带 owner 时校验持有权：丢失租约的 worker 不能落终态。"""
        cursor = self._conn.execute(
            "UPDATE supply_slots SET status='created', record_id=?, product_code=?,"
            " main_note_id=?, adoption=?, note=?, owner='', lease_until='',"
            " updated_at=datetime('now')"
            " WHERE account_id=? AND supply_date=? AND slot=?"
            + (" AND (owner='' OR owner=?)" if owner else ""),
            (record_id, product_code, main_note_id, adoption, note,
             account_id, supply_date, slot)
            + ((owner,) if owner else ()),
        )
        self._conn.commit()
        return bool(cursor.rowcount)
        self._conn.commit()

    def slots_for(self, account_id: str, supply_date: Optional[str] = None):
        if supply_date:
            rows = self._conn.execute(
                "SELECT * FROM supply_slots WHERE account_id=? AND supply_date=?"
                " ORDER BY slot", (account_id, supply_date),
            )
        else:
            rows = self._conn.execute(
                "SELECT * FROM supply_slots WHERE account_id=?"
                " ORDER BY updated_at DESC", (account_id,),
            )
        return rows.fetchall()

    def product_usage_counts(self, account_id: str) -> Dict[str, int]:
        rows = self._conn.execute(
            "SELECT product_code, COUNT(*) n FROM supply_slots"
            " WHERE account_id=? AND status='created' AND product_code!=''"
            " GROUP BY product_code", (account_id,),
        ).fetchall()
        return {r["product_code"]: r["n"] for r in rows}

    # ---- 分析失败冷却（避免反复付费重试同一失败素材） ----
    def record_analysis_failure(self, *, fingerprint: str, model: str,
                                analysis_version: str, note_id: str,
                                error: str) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO analysis_failures"
            " (material_fingerprint, model, analysis_version, note_id, error, failed_at)"
            " VALUES (?,?,?,?,?,datetime('now'))",
            (fingerprint, model, analysis_version, note_id, error[:300]),
        )
        self._conn.commit()

    def recent_analysis_failure(self, fingerprint: str, model: str,
                                analysis_version: str,
                                within_hours: int = 24) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM analysis_failures"
            " WHERE material_fingerprint=? AND model=? AND analysis_version=?"
            " AND failed_at >= datetime('now', ?)",
            (fingerprint, model, analysis_version, f"-{int(within_hours)} hours"),
        ).fetchone()
        return row is not None

    def cost_summary(self) -> Dict[str, int]:
        row = self._conn.execute(
            "SELECT COUNT(*) calls, COALESCE(SUM(images),0) images,"
            " COALESCE(SUM(prompt_tokens),0) pt, COALESCE(SUM(completion_tokens),0) ct"
            " FROM analysis_call_log WHERE status='ok'"
        ).fetchone()
        return {k: row[k] for k in row.keys()}


# ---------------------------------------------------------------------------
# Doubao 分析
# ---------------------------------------------------------------------------

@dataclass
class AnalysisOutcome:
    note_id: str
    fingerprint: str
    cached: bool
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


_BATCH_PROMPT_HEADER = """你是小红书穿搭图文素材分析器。阅读这组按原始顺序排列的笔记图片（第 {start}-{end} 页，共 {total} 页），输出严格 JSON（不要输出其他文字）。
笔记标题：{title}
规则：
- 标题只表达作者主张，不作为商品事实/保暖效果；判定以图片可见内容为准。
- 只描述画面可见内容；不确定的字段填空字符串/空数组，不要猜。
- 不推断适穿温度、保暖性能、面料材质。
- 全部用中文。"""

_BATCH_PROMPT_BODY = """
输出 JSON 结构：
{
 "note_topic": "这篇笔记的核心主题（一句话）",
 "set_structure": "independent_collection | same_item_multiway | layered | comparison | mixed 之一；注意：同一套造型的多角度/特写照片不算多搭，应归 comparison 或 independent_collection 并在 pages 里用 outfit_set_id 标明同组",
 "core_items": [{"item": "单品名", "role": "核心/配角"}],
 "outfit_relations": "单品之间的搭配关系（比例/配色/层次，一句话）",
 "page_roles": [{"seq": 页码, "role": "cover | full_outfit | detail | explanation | other 之一"}],
 "pages": [{"seq": 页码, "outfit_summary": "该页可见搭配的一句话摘要（含上下装与鞋履关系）", "outfit_set_id": "同套造型分组号（同一套的不同角度同号；独立搭配各自编号，整数）", "variation": "与同组其他页的实际差异（换个角度/局部特写/无差异）"}],
 "palette": ["主色", "辅色"],
 "photography": "摄影特征（街拍/棚拍/室内，景别）",
 "background": "背景/场景特征",
 "shoot_style": "拍摄方式：mirror_selfie（镜面自拍）| casual_phone_selfie（随手手机自拍/游客照）| street_snap（街拍）| studio（棚拍/精修）| indoor（室内他拍）| outdoor_other（户外其他）之一",
 "photography_quality": "poor（画质差/场景脏乱/自拍/随手拍）| normal（及格的生活实拍）| good（构图光线干净的博主级出片）之一",
 "purpose_usability": {
   "outfit": {"usable": true/false, "reason": "搭配关系是否清楚可文字化借鉴（看不清衣服则 false）"},
   "visual": {"usable": true/false, "reason": "色调/光线/构图是否值得视觉借鉴（摄影差/自拍感重则 false）"},
   "narrative": {"usable": true/false, "reason": "选题与页面递进是否值得叙事借鉴（纯广告/文字长图则 false）"}
 }
}
purpose_usability 按用途独立判定：摄影差只影响 visual，衣服搭配清楚则 outfit 仍可用；
非穿搭内容（纯测评/广告/文字长图）三种都不可用。"""


def _usage(response: Any) -> tuple:
    usage = (response or {}).get("usage") or {}
    return int(usage.get("prompt_tokens") or 0), int(usage.get("completion_tokens") or 0)


def _downscale(paths: Sequence[Path], workdir: Path, max_edge: int, quality: int) -> List[Path]:
    """缩图用于分析，控制 token 与流量；保持顺序。"""
    from PIL import Image  # 延迟导入：仅分析路径需要 Pillow

    out: List[Path] = []
    for idx, src in enumerate(paths, start=1):
        dst = workdir / f"p{idx:02d}.jpg"
        with Image.open(src) as im:
            im = im.convert("RGB")
            if max(im.size) > max_edge:
                scale = max_edge / max(im.size)
                im = im.resize((max(1, round(im.width * scale)),
                                max(1, round(im.height * scale))))
            im.save(dst, "JPEG", quality=quality)
        out.append(dst)
    return out


def _rebase_page_roles(roles: Any, global_start: int, batch_len: int) -> List[Dict[str, Any]]:
    """把批内页角色映射到全局页码：批内 1..batch_len → global_start..；异常值按位置回退。"""
    rebased: Dict[int, str] = {}
    for position, role in enumerate(roles or []):
        if not isinstance(role, dict):
            continue
        local = role.get("seq")
        if isinstance(local, int) and 1 <= local <= batch_len:
            seq = global_start + local - 1
        else:
            seq = global_start + position
        rebased[seq] = str(role.get("role") or "other")
    return [{"seq": seq, "role": name} for seq, name in sorted(rebased.items())]


def _rebase_page_details(pages: Any, global_start: int, batch_len: int) -> List[Dict[str, Any]]:
    """v3 逐页搭配摘要同规则重定位批内页码（字段保留原样，仅修 seq）。"""
    rebased: Dict[int, Dict[str, Any]] = {}
    for position, page in enumerate(pages or []):
        if not isinstance(page, dict):
            continue
        local = page.get("seq")
        if isinstance(local, int) and 1 <= local <= batch_len:
            seq = global_start + local - 1
        else:
            seq = global_start + position
        item = dict(page)
        item["seq"] = seq
        rebased[seq] = item
    return [rebased[seq] for seq in sorted(rebased)]


def _merge_batch_results(batches: List[Dict[str, Any]]) -> Dict[str, Any]:
    """确定性合并分批结果：保序拼接页角色，全局字段取首批并标注页区间来源。"""
    if len(batches) == 1:
        merged = dict(batches[0])
        # 单批早退也要派生兼容键（v3 prompt 不再输出 consumable——
        # 2026-09-17 修复：单批笔记缺键导致 preview/旧消费方读到 None）
        single_usability = merged.get("purpose_usability")
        if isinstance(single_usability, dict) and single_usability:
            flags = [bool((single_usability.get(name) or {}).get("usable"))
                     for name in ("outfit", "visual", "narrative")
                     if isinstance(single_usability.get(name), dict)]
            if flags:
                merged["consumable"] = any(flags)
        return merged
    page_roles: List[Dict[str, Any]] = []
    for chunk in batches:
        for role in chunk.get("page_roles") or []:
            if isinstance(role, dict) and role.get("seq") is not None:
                page_roles.append({"seq": int(role["seq"]), "role": str(role.get("role") or "other")})
    page_roles.sort(key=lambda r: r["seq"])
    first = batches[0]
    core_items = []
    seen = set()
    for chunk in batches:
        for item in chunk.get("core_items") or []:
            name = str((item or {}).get("item") or "").strip()
            if name and name not in seen:
                seen.add(name)
                core_items.append({"item": name, "role": str((item or {}).get("role") or "")})
    structures = {str(c.get("set_structure") or "") for c in batches} - {""}
    palette: List[str] = []
    for chunk in batches:
        for color in chunk.get("palette") or []:
            if color and color not in palette:
                palette.append(str(color))
    consumable = all(bool(c.get("consumable")) for c in batches)
    reasons = [str(c.get("consumable_reason") or "") for c in batches if c.get("consumable_reason")]
    # v3 按用途可用性合并：任一批该用途可用即整篇可用（一篇可被不同任务以
    # 不同用途借鉴，取代 v2 整篇 consumable 审美排除——附B-2）
    usability: Dict[str, Dict[str, Any]] = {}
    for purpose in ("outfit", "visual", "narrative"):
        entries = []
        for chunk in batches:
            mapping = chunk.get("purpose_usability")
            entry = (mapping or {}).get(purpose) if isinstance(mapping, dict) else None
            if isinstance(entry, dict):
                entries.append(entry)
        if entries:
            usability[purpose] = {
                "usable": any(bool(e.get("usable")) for e in entries),
                "reason": next(
                    (str(e.get("reason") or "") for e in entries
                     if str(e.get("reason") or "")), ""),
            }
    # 兼容键：v3 缓存下 consumable = 任一用途可用（摄影差不再是整篇否决）
    if usability:
        consumable = any(bool(v.get("usable")) for v in usability.values())
    pages: List[Dict[str, Any]] = []
    seen_seqs = set()
    for chunk in batches:
        for page in chunk.get("pages") or []:
            if isinstance(page, dict) and int(page.get("seq") or 0) not in seen_seqs:
                seen_seqs.add(int(page.get("seq") or 0))
                pages.append({
                    "seq": int(page.get("seq") or 0),
                    "outfit_summary": str(page.get("outfit_summary") or "")[:120],
                    "outfit_set_id": page.get("outfit_set_id"),
                    "variation": str(page.get("variation") or "")[:60],
                })
    pages.sort(key=lambda p: p["seq"])
    # 审美维度合并：质量取最差批（一页拉胯即整体拉胯）；拍摄方式不一致记 mixed
    qualities = [str(c.get("photography_quality") or "") for c in batches]
    photography_quality = ""
    if "poor" in qualities:
        photography_quality = "poor"
    else:
        for q in qualities:
            if q:
                photography_quality = q
                break
    styles = {str(c.get("shoot_style") or "") for c in batches} - {""}
    shoot_style = styles.pop() if len(styles) == 1 else ("mixed" if styles else "")
    return {
        "note_topic": str(first.get("note_topic") or ""),
        "core_items": core_items,
        "outfit_relations": "；".join(
            dict.fromkeys(str(c.get("outfit_relations") or "") for c in batches if c.get("outfit_relations"))
        ),
        "set_structure": structures.pop() if len(structures) == 1 else "mixed",
        "page_roles": page_roles,
        "palette": palette[:6],
        "photography": str(first.get("photography") or ""),
        "background": str(first.get("background") or ""),
        "shoot_style": shoot_style,
        "photography_quality": photography_quality,
        "purpose_usability": usability,
        "pages": pages,
        "consumable": consumable,
        "consumable_reason": "；".join(reasons[:2]) or ("分批全部可用" if consumable else "部分批次不可用"),
    }


class MaterialAnalyzer:
    """预检 + Doubao 分析 + 缓存。"""

    def __init__(
        self,
        source: MaterialSource,
        ledger: MaterialLedger,
        client: Any,
        *,
        model: str,
        analysis_version: str = ANALYSIS_VERSION,
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_image_edge: int = DEFAULT_MAX_IMAGE_EDGE,
        jpeg_quality: int = DEFAULT_JPEG_QUALITY,
        max_tokens: int = 1600,
    ):
        self.source = source
        self.ledger = ledger
        self.client = client          # 需提供 chat_with_multiple_images(paths, prompt, max_tokens)
        self.model = model
        self.analysis_version = analysis_version
        self.batch_size = max(1, batch_size)
        self.max_image_edge = max_image_edge
        self.jpeg_quality = jpeg_quality
        self.max_tokens = max_tokens

    # ---- 单篇 ----
    def analyze_note(self, note_id: str, *, force: bool = False) -> AnalysisOutcome:
        package = self.source.get(note_id)
        if package is None:
            return AnalysisOutcome(note_id, "", cached=False, error="素材不存在")
        if not package.complete:
            return AnalysisOutcome(
                note_id, package.version_fingerprint, cached=False,
                error="素材不完整（文件缺失或抓取未完成）")
        fingerprint = package.version_fingerprint
        if not force:
            cached = self.ledger.get_cached_analysis(
                fingerprint, self.model, self.analysis_version)
            if cached is not None:
                return AnalysisOutcome(note_id, fingerprint, cached=True, result=cached)

        paths = [img.path for img in package.images]
        import tempfile
        with tempfile.TemporaryDirectory(prefix="opv_material_") as tmp:
            scaled = _downscale(
                paths, Path(tmp), self.max_image_edge, self.jpeg_quality)
            batches = [scaled[i:i + self.batch_size]
                       for i in range(0, len(scaled), self.batch_size)]
            results: List[Dict[str, Any]] = []
            calls = retries = 0
            pt_total = ct_total = 0
            started = time.time()
            last_error: Optional[str] = None
            for batch_index, batch in enumerate(batches):
                start_seq = batch_index * self.batch_size + 1
                prompt = (
                    _BATCH_PROMPT_HEADER.format(
                        start=start_seq,
                        end=start_seq + len(batch) - 1,
                        total=len(paths),
                        title=str(package.title or "（无标题）")[:80],
                    )
                    + _BATCH_PROMPT_BODY
                )
                ok, response = self._call_once(note_id, batch, prompt)
                calls += 1
                self.ledger.settle_budget(
                    purpose="analysis",
                    state="consumed" if ok else "unknown",
                    note=note_id)
                if not ok:
                    # 每批最多重试一次，仍失败则本篇放弃（不形成重试循环）
                    ok, response = self._call_once(note_id, batch, prompt)
                    calls += 1
                    retries += 1
                if not ok:
                    last_error = "模型调用失败（重试一次后仍失败）"
                    break
                pt, ct = _usage(response)
                pt_total += pt
                ct_total += ct
                try:
                    parsed = parse_vision_envelope(response)
                except Exception as exc:  # noqa: BLE001
                    last_error = f"模型输出解析失败：{exc}"
                    break
                # 模型按批内相对页码输出；用已知的全局批次区间重定位，保证合并后保序
                parsed["page_roles"] = _rebase_page_roles(
                    parsed.get("page_roles"), start_seq, len(batch))
                parsed["pages"] = _rebase_page_details(
                    parsed.get("pages"), start_seq, len(batch))
                results.append(parsed)
            duration_ms = int((time.time() - started) * 1000)
            if last_error or not results:
                self.ledger.record_gap(
                    scope=f"analysis:{note_id}", reason="analyze_failed",
                    detail=last_error or "无有效批次结果")
                # 失败入冷却：24h 内 analyze_pending 不再付费重试（force 可越过）
                self.ledger.record_analysis_failure(
                    fingerprint=fingerprint, model=self.model,
                    analysis_version=self.analysis_version, note_id=note_id,
                    error=last_error or "无有效批次结果")
                return AnalysisOutcome(note_id, fingerprint, cached=False, error=last_error)
            merged = _merge_batch_results(results)
            self.ledger.put_cached_analysis(
                fingerprint=fingerprint, model=self.model,
                analysis_version=self.analysis_version, note_id=note_id,
                result=merged, image_count=len(paths), calls=calls,
                prompt_tokens=pt_total, completion_tokens=ct_total,
                duration_ms=duration_ms, retries=retries)
            return AnalysisOutcome(note_id, fingerprint, cached=False, result=merged)

    def _call_once(self, note_id: str, batch: List[Path], prompt: str):
        # 方案 A3：分析调用逐次原子预留（缓存命中不会走到这里）
        cap = int(os.environ.get("OPV_SUPPLY_DAILY_CALL_CAP") or 300)
        if not self.ledger.reserve_budget(purpose="analysis", cap=cap,
                                          note=note_id):
            raise MaterialAnalysisBudgetError(
                f"当日分析调用额度已满（{cap} 次），本轮中止")
        try:
            return self._call_once_inner(note_id, batch, prompt)
        except Exception:
            self.ledger.settle_budget(purpose="analysis", state="consumed",
                                      note="失败仍计额度")
            raise

    def _call_once_inner(self, note_id: str, batch: List[Path], prompt: str):
        started = time.time()
        try:
            response = self.client.chat_with_multiple_images(
                [str(p) for p in batch], prompt, self.max_tokens)
        except Exception as exc:  # noqa: BLE001
            self.ledger.log_call(
                purpose="material_analysis", note_id=note_id, model=self.model,
                images=len(batch), prompt_tokens=0, completion_tokens=0,
                duration_ms=int((time.time() - started) * 1000),
                status="error", error=str(exc)[:300])
            return False, None
        pt, ct = _usage(response)
        self.ledger.log_call(
            purpose="material_analysis", note_id=note_id, model=self.model,
            images=len(batch), prompt_tokens=pt, completion_tokens=ct,
            duration_ms=int((time.time() - started) * 1000), status="ok")
        return True, response

    # ---- 批量 ----
    def analyze_pending(self, *, limit: int = 5, force: bool = False,
                        consecutive_fail_stop: int = 2) -> List[AnalysisOutcome]:
        packages = self.source.list_packages(require_complete=True)
        analyzed = self.ledger.analyzed_note_ids(self.model, self.analysis_version)
        todo = [
            p for p in packages
            if (force or p.note_id not in analyzed)
            and (force or not self.ledger.recent_analysis_failure(
                p.version_fingerprint, self.model, self.analysis_version))
        ][:max(0, limit)]
        outcomes: List[AnalysisOutcome] = []
        fails = 0
        for package in todo:
            outcome = self.analyze_note(package.note_id, force=force)
            outcomes.append(outcome)
            if outcome.error:
                fails += 1
                if fails >= consecutive_fail_stop:
                    break
            else:
                fails = 0
        return outcomes
