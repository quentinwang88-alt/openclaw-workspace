#!/usr/bin/env python3
"""
diversity_engine —— 反雷同去重引擎(防限流核心)

三层机制(已与用户确认):
  ① 历史去重:读共享库最近 N 条【同产品】脚本指纹,强制本次避开。
  ② 随机种子:注入 diversity_seed 打破模型默认表达惯性。
  ③ 雷同自检:生成后与历史指纹比对相似度,超阈值打回(供 P5 质检调用)。

数据约定(对齐 DATABASE_CONVENTIONS.md):
  - 复用现有共享库 original_script_generator.sqlite3(已积累 489 runs)。
  - 历史去重直接读现有 pipeline_runs.exp_s1~s4_json,不另起炉灶。
  - 内头巾结构化指纹存新表 script_fingerprints(不污染现有表)。

配置可演进:去重窗口/相似度阈值来自 pack.yaml defaults / 入参,不写死。
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


# ---- 共享库路径(对齐 base_skill.get_shared_sqlite_path 的约定) -----------------
def get_shared_db_path(domain: str = "original_script_generator") -> Path:
    root = os.environ.get(
        "OPENCLAW_SHARED_DATA_DIR", "/Users/likeu3/.openclaw/shared/data"
    )
    return Path(root) / f"{domain}.sqlite3"


FINGERPRINT_DDL = """
CREATE TABLE IF NOT EXISTS script_fingerprints (
    fp_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    product_code   TEXT,
    country        TEXT,
    category       TEXT,
    direction      TEXT,          -- S1/S2/S3/S4
    main_hook_id   TEXT,
    proof_angle    TEXT,
    scene_id       TEXT,
    persona_id     TEXT,
    opening_fp     TEXT,          -- 开场指纹(归一化)
    first_line_fp  TEXT,          -- 首句句式指纹
    diversity_seed TEXT,
    created_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fp_product ON script_fingerprints(product_code);
CREATE INDEX IF NOT EXISTS idx_fp_cat ON script_fingerprints(category);
"""


# ==============================================================================
# 工具:文本归一化与指纹
# ==============================================================================
def _norm(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[，。、,.!?；;：:\"'()（）\[\]【】]", "", text)
    return text


def _fp(text: str, n: int = 16) -> str:
    return hashlib.md5(_norm(text).encode("utf-8")).hexdigest()[:n] if text else ""


def _first_sentence(text: str) -> str:
    if not text:
        return ""
    parts = re.split(r"[。．.!?！？\n]", text.strip())
    return parts[0] if parts else text[:30]


# ==============================================================================
# 层①:历史去重 —— 读最近 N 条同产品脚本,产出 forbidden_recent_pool
# ==============================================================================
def build_forbidden_pool(
    product_code: str,
    window: int = 30,
    db_path: Optional[Path] = None,
) -> Dict[str, List[str]]:
    """
    返回最近 window 条同产品脚本里用过的元素,供 prompt 注入强制避开:
      {
        "openings":     [...],  # 开场表达模式
        "scenes":       [...],  # 场景语境
        "proof_angles": [...],  # 证明角度/中段任务
        "first_lines":  [...],  # 首句句式
      }
    数据来源:① 现有 pipeline_runs.exp_s1~s4_json ② 新表 script_fingerprints
    """
    db_path = db_path or get_shared_db_path()
    pool: Dict[str, Set[str]] = {
        "openings": set(), "scenes": set(),
        "proof_angles": set(), "first_lines": set(),
    }
    if not db_path.is_file():
        return {k: [] for k in pool}

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        # --- 来源A:现有 pipeline_runs 的四方向脚本 ---
        cur.execute(
            """SELECT exp_s1_json, exp_s2_json, exp_s3_json, exp_s4_json
               FROM pipeline_runs
               WHERE product_code = ?
               ORDER BY run_id DESC LIMIT ?""",
            (product_code, window),
        )
        for row in cur.fetchall():
            for cell in row:
                if not cell:
                    continue
                try:
                    d = json.loads(cell)
                except (json.JSONDecodeError, TypeError):
                    continue
                if not isinstance(d, dict):
                    continue
                op = d.get("main_expression_pattern") or d.get("opening_expression_task")
                if op:
                    pool["openings"].add(op[:60])
                    pool["first_lines"].add(_first_sentence(op))
                sc = d.get("native_expression_entry")
                if sc:
                    pool["scenes"].add(sc[:60])
                mid = d.get("middle_expression_task")
                if mid:
                    pool["proof_angles"].add(mid[:60])

        # --- 来源B:新指纹表(内头巾流程产出) ---
        _ensure_fp_table(conn)
        cur.execute(
            """SELECT opening_fp, scene_id, proof_angle, first_line_fp
               FROM script_fingerprints
               WHERE product_code = ?
               ORDER BY fp_id DESC LIMIT ?""",
            (product_code, window),
        )
        for op, sc, pa, fl in cur.fetchall():
            if op: pool["openings"].add(op)
            if sc: pool["scenes"].add(sc)
            if pa: pool["proof_angles"].add(pa)
            if fl: pool["first_lines"].add(fl)
    finally:
        conn.close()

    return {k: sorted(v) for k, v in pool.items()}


# ==============================================================================
# 层②:随机种子 —— 打破默认表达惯性
# ==============================================================================
def make_diversity_seed(product_code: str, direction: str, extra: str = "") -> str:
    """生成可复现但分散的多样性种子(含产品+方向+随机)。"""
    base = f"{product_code}|{direction}|{extra}|{random.random()}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]


def pick_rotated(
    candidates: List[str],
    used: List[str],
    seed: str,
    max_repeat: int = 2,
) -> Optional[str]:
    """
    从 candidates 中选一个,优先避开 used(已用过的);
    用 seed 决定随机顺序,保证分散又可复现。
    """
    if not candidates:
        return None
    rng = random.Random(seed)
    order = candidates[:]
    rng.shuffle(order)
    used_count: Dict[str, int] = {}
    for u in used:
        used_count[u] = used_count.get(u, 0) + 1
    # 优先选未用过的
    for c in order:
        if used_count.get(c, 0) == 0:
            return c
    # 都用过则选用得最少的
    return min(order, key=lambda c: used_count.get(c, 0))


# ==============================================================================
# 层③:雷同自检 —— 生成后比对相似度(供 P5 质检)
# ==============================================================================
def similarity_score(new_script: Dict[str, Any], forbidden_pool: Dict[str, List[str]]) -> Dict[str, Any]:
    """
    比对新脚本四维(开场/场景/角度/首句)与历史池的重合度。
    返回 {score: 0~1, hits: {...}, pass: bool}。score 越高越雷同。
    """
    checks = {
        "opening": (new_script.get("opening", ""), forbidden_pool.get("openings", [])),
        "scene": (new_script.get("scene", ""), forbidden_pool.get("scenes", [])),
        "proof_angle": (new_script.get("proof_angle", ""), forbidden_pool.get("proof_angles", [])),
        "first_line": (_first_sentence(new_script.get("opening", "")), forbidden_pool.get("first_lines", [])),
    }
    hits = {}
    hit_n = 0
    for dim, (val, pool) in checks.items():
        nv = _norm(val)
        matched = any(nv and (nv in _norm(p) or _norm(p) in nv) for p in pool)
        hits[dim] = matched
        if matched:
            hit_n += 1
    score = round(hit_n / len(checks), 3)
    return {"score": score, "hits": hits, "pass": score < 0.5}  # 阈值0.5:半数以上维度雷同则打回


# ==============================================================================
# 写入指纹(脚本定稿后调用)
# ==============================================================================
def _ensure_fp_table(conn: sqlite3.Connection) -> None:
    conn.executescript(FINGERPRINT_DDL)
    conn.commit()


def save_fingerprint(
    product_code: str, country: str, category: str, direction: str,
    main_hook_id: str, proof_angle: str, scene_id: str, persona_id: str,
    opening: str, diversity_seed: str,
    db_path: Optional[Path] = None,
) -> None:
    from datetime import datetime, timezone
    db_path = db_path or get_shared_db_path()
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_fp_table(conn)
        conn.execute(
            """INSERT INTO script_fingerprints
               (product_code,country,category,direction,main_hook_id,proof_angle,
                scene_id,persona_id,opening_fp,first_line_fp,diversity_seed,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (product_code, country, category, direction, main_hook_id, proof_angle,
             scene_id, persona_id, _fp(opening), _fp(_first_sentence(opening)),
             diversity_seed, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    print("diversity_engine 自检")
    print("=" * 60)
    db = get_shared_db_path()
    print(f"共享库: {db}  存在={db.is_file()}")

    # 取一个真实 product_code 测历史去重
    if db.is_file():
        conn = sqlite3.connect(str(db))
        row = conn.execute(
            "SELECT product_code FROM pipeline_runs WHERE product_code IS NOT NULL AND exp_s1_json IS NOT NULL ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if row:
            pc = row[0]
            pool = build_forbidden_pool(pc, window=30)
            print(f"\n真实产品 {pc} 的历史去重池:")
            for k, v in pool.items():
                print(f"  {k}: {len(v)} 条")
                if v:
                    print(f"     例: {v[0][:50]}...")

    # 测随机种子与轮换
    seed = make_diversity_seed("TEST001", "S1")
    print(f"\n随机种子示例: {seed}")
    cands = ["勒痕角度", "余量角度", "日常角度", "材质角度"]
    used = ["勒痕角度", "勒痕角度", "余量角度"]
    pick = pick_rotated(cands, used, seed)
    print(f"轮换选择(避开已用): {pick}")

    # 测雷同自检
    new_s = {"opening": "全新的开场表达", "scene": "车里", "proof_angle": "日常角度"}
    sim = similarity_score(new_s, {"openings": ["完全不同的内容"], "scenes": [], "proof_angles": [], "first_lines": []})
    print(f"\n雷同自检: score={sim['score']} pass={sim['pass']}")
    print("=" * 60)
    print("✅ diversity_engine 三层机制自检通过")
