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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from services.material_source import MaterialPackage, MaterialSource
from services.photo_reference_vision import parse_vision_envelope

ANALYSIS_VERSION = "material-analysis-v1"
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
"""


def default_ledger_path() -> Path:
    override = os.environ.get("OPV_MATERIAL_LEDGER_DB")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[1] / "var" / "material_consumer.sqlite3"


class MaterialLedger:
    """消费侧台账：分析缓存、调用成本、使用历史、素材缺口。"""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = Path(db_path) if db_path else default_ledger_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(LEDGER_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

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

    def complete_slot(self, account_id: str, supply_date: str, slot: int, *,
                      record_id: str, product_code: str = "", main_note_id: str = "",
                      adoption: str = "", note: str = "") -> None:
        self._conn.execute(
            "UPDATE supply_slots SET status='created', record_id=?, product_code=?,"
            " main_note_id=?, adoption=?, note=?, updated_at=datetime('now')"
            " WHERE account_id=? AND supply_date=? AND slot=?",
            (record_id, product_code, main_note_id, adoption, note,
             account_id, supply_date, slot),
        )
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
规则：
- 只描述画面可见内容；不确定的字段填空字符串/空数组，不要猜。
- 不推断适穿温度、保暖性能、面料材质。
- 全部用中文。"""

_BATCH_PROMPT_BODY = """
输出 JSON 结构：
{
 "note_topic": "这篇笔记的核心主题（一句话）",
 "core_items": [{"item": "单品名", "role": "核心/配角"}],
 "outfit_relations": "单品之间的搭配关系（比例/配色/层次，一句话）",
 "set_structure": "same_item_multiway | independent_collection | layered | comparison | other 之一",
 "page_roles": [{"seq": 页码, "role": "cover | full_outfit | detail | explanation | other 之一"}],
 "palette": ["主色", "辅色"],
 "photography": "摄影特征（街拍/棚拍/室内，景别）",
 "background": "背景/场景特征",
 "consumable": true/false,
 "consumable_reason": "为什么可/不可作为穿搭生成参考（一句话）"
}
consumable 判定标准：图片清晰、穿搭可辨认、以服装表达为主（非纯测评/广告/文字长图）。"""


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


def _merge_batch_results(batches: List[Dict[str, Any]], total_pages: int) -> Dict[str, Any]:
    """确定性合并分批结果：保序拼接页角色，全局字段取首批并标注页区间来源。"""
    if len(batches) == 1:
        return batches[0]
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
                    )
                    + _BATCH_PROMPT_BODY
                )
                ok, response = self._call_once(note_id, batch, prompt)
                calls += 1
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
                results.append(parsed)
            duration_ms = int((time.time() - started) * 1000)
            if last_error or not results:
                self.ledger.record_gap(
                    scope=f"analysis:{note_id}", reason="analyze_failed",
                    detail=last_error or "无有效批次结果")
                return AnalysisOutcome(note_id, fingerprint, cached=False, error=last_error)
            merged = _merge_batch_results(results, len(paths))
            self.ledger.put_cached_analysis(
                fingerprint=fingerprint, model=self.model,
                analysis_version=self.analysis_version, note_id=note_id,
                result=merged, image_count=len(paths), calls=calls,
                prompt_tokens=pt_total, completion_tokens=ct_total,
                duration_ms=duration_ms, retries=retries)
            return AnalysisOutcome(note_id, fingerprint, cached=False, result=merged)

    def _call_once(self, note_id: str, batch: List[Path], prompt: str):
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
        todo = [p for p in packages if force or p.note_id not in analyzed][:max(0, limit)]
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
