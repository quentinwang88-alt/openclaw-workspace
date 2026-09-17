"""外部自动供稿的执行合同（Phase 1）。

背景（2026-09-16 复盘）：自动供稿把第三方 reference_only 原图写进
「完整穿搭素材」字段，被 COMPLETE_LOOK 路径无条件当作完整穿搭资产
（原图直用成片底图），且 adoption / 目的地 / 页级选材从未进入执行链。

本模块把供稿意图在**建行前**持久化为可恢复的合同（消费侧台账库同库），
飞书行只作为合同的展示面；执行侧（feishu_workflow）付费前凭
record_id 反查合同，查不到即报接线错误，不再静默按普通附件处理。

不改动既有 supply_slots 语义；合同是新增的独立事实源。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

#: 执行策略版本：adoption 语义 / 页级供图 / 目的地与商品约束的解析规则。
#: 任何影响执行行为的改动都必须升版本（影响合同指纹与缓存续跑判断）。
SUPPLY_POLICY_VERSION = "external-reference-exec-v2"

#: 与 auto_photo_supply 的来源标记前缀保持一致（单一事实源见那边导出）。
MARKER_PREFIX = "auto_supply"

CONTRACT_SCHEMA = """
CREATE TABLE IF NOT EXISTS external_supply_contracts (
    contract_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    supply_date TEXT NOT NULL,
    slot INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'intent',
    record_id TEXT,
    source_type TEXT NOT NULL DEFAULT 'xhs_reference',
    authorization TEXT NOT NULL DEFAULT 'reference_only',
    adoption TEXT NOT NULL DEFAULT '',
    main_note_id TEXT NOT NULL DEFAULT '',
    main_note_title TEXT NOT NULL DEFAULT '',
    selected_pages TEXT NOT NULL DEFAULT '[]',
    product TEXT NOT NULL DEFAULT '',
    destination TEXT NOT NULL DEFAULT '',
    temperature_band TEXT NOT NULL DEFAULT '',
    topic_statement TEXT NOT NULL DEFAULT '',
    effective_brief TEXT NOT NULL DEFAULT '',
    content_requirement TEXT NOT NULL DEFAULT '',
    policy_version TEXT NOT NULL,
    contract_fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (account_id, supply_date, slot)
);
"""


def default_contract_store_path() -> Path:
    """合同与消费侧台账同库（默认包内 var/，可用环境变量覆盖）。"""
    from services.material_analysis import default_ledger_path
    return default_ledger_path()


def contract_fingerprint(
    *, adoption: str, main_note_id: str, selected_pages: List[Dict[str, Any]],
    product: Dict[str, Any], destination: Dict[str, Any],
    policy_version: str, temperature_band: str = "",
    topic_statement: str = "",
) -> str:
    """合同指纹：adoption / 选中来源与页面 / 商品 / 目的地 / 温度带 /
    本篇主张 / 策略版本。

    视觉输入任一维度变化都必须得到新指纹（方案 §七），下游不得把
    指纹不同的新执行当作文案小改去复用旧 staging。
    """
    payload = json.dumps({
        "adoption": str(adoption or ""),
        "main_note_id": str(main_note_id or ""),
        "pages": [
            {"note_id": str(p.get("note_id") or ""), "seq": int(p.get("seq") or 0),
             "sha256": str(p.get("sha256") or ""), "purpose": str(p.get("purpose") or "")}
            for p in (selected_pages or [])
        ],
        "product": {k: str((product or {}).get(k) or "")
                    for k in ("code", "name", "category", "variant")},
        "destination": {k: str((destination or {}).get(k) or "")
                        for k in ("country", "place")},
        "temperature_band": str(temperature_band or ""),
        "topic_statement": str(topic_statement or "")[:200],
        "policy_version": str(policy_version or ""),
    }, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class ExternalSupplyContractStore:
    """external_supply_contracts 的最小存取（同库独立连接，短事务）。"""

    def __init__(self, path: Optional[str] = None):
        self.path = str(path or default_contract_store_path() or
                        os.environ.get("OPV_MATERIAL_LEDGER_DB") or
                        (Path(__file__).resolve().parents[1] / "var" /
                         "material_consumer.sqlite3"))
        self._conn: Optional[sqlite3.Connection] = None
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.path, timeout=30)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _ensure_schema(self) -> None:
        conn = self._connect()
        with conn:
            conn.executescript(CONTRACT_SCHEMA)
            # 既有库迁移：temperature_band / topic_statement / effective_brief 列
            columns = {row[1] for row in conn.execute(
                "PRAGMA table_info(external_supply_contracts)")}
            for column in ("temperature_band", "topic_statement", "effective_brief"):
                if column not in columns:
                    conn.execute(
                        "ALTER TABLE external_supply_contracts"
                        f" ADD COLUMN {column} TEXT NOT NULL DEFAULT ''")

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ---- 写 ----
    def persist_intent(self, contract: Dict[str, Any]) -> Dict[str, Any]:
        """建行前持久化供稿意图；同 (account, date, slot) 已存在则原样返回。

        幂等恢复：进程在 batch_create 前崩溃 → 下轮读到 intent 合同，
        继续走建行；已 attach record_id 的合同不重复建行（由 supply 对账）。
        """
        account_id = str(contract["account_id"])
        supply_date = str(contract["supply_date"])
        slot = int(contract["slot"])
        contract_id = f"{account_id}|{supply_date}|{slot}"
        conn = self._connect()
        with conn:
            existing = conn.execute(
                "SELECT * FROM external_supply_contracts WHERE contract_id=?",
                (contract_id,)).fetchone()
            if existing is not None:
                return self._to_dict(existing)
            conn.execute(
                "INSERT INTO external_supply_contracts ("
                " contract_id, account_id, supply_date, slot, status,"
                " source_type, authorization, adoption, main_note_id,"
                " main_note_title, selected_pages, product, destination,"
                " temperature_band, topic_statement, effective_brief,"
                " content_requirement, policy_version, contract_fingerprint)"
                " VALUES (?,?,?,?, 'intent',?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (contract_id, account_id, supply_date, slot,
                 str(contract.get("source_type") or "xhs_reference"),
                 str(contract.get("authorization") or "reference_only"),
                 str(contract.get("adoption") or ""),
                 str(contract.get("main_note_id") or ""),
                 str(contract.get("main_note_title") or "")[:120],
                 json.dumps(contract.get("selected_pages") or [],
                            ensure_ascii=False),
                 json.dumps(contract.get("product") or {}, ensure_ascii=False),
                 json.dumps(contract.get("destination") or {}, ensure_ascii=False),
                 str(contract.get("temperature_band") or ""),
                 str(contract.get("topic_statement") or "")[:300],
                 json.dumps(contract.get("effective_brief") or {},
                            ensure_ascii=False),
                 str(contract.get("content_requirement") or "")[:2000],
                 str(contract.get("policy_version") or SUPPLY_POLICY_VERSION),
                 str(contract.get("contract_fingerprint") or "")))
        return self.get(contract_id) or dict(contract, contract_id=contract_id)

    def supersede_intent(self, contract: Dict[str, Any]) -> None:
        """冻结输入已不可执行（如主参考被拒）时，用新合同覆盖未绑定行的 intent。"""
        contract_id = str(contract.get("contract_id")
                          or f"{contract.get('account_id')}|{contract.get('supply_date')}|{contract.get('slot')}")
        conn = self._connect()
        with conn:
            conn.execute(
                "UPDATE external_supply_contracts SET"
                " adoption=?, main_note_id=?, main_note_title=?, selected_pages=?,"
                " product=?, destination=?, temperature_band=?, topic_statement=?,"
                " effective_brief=?, content_requirement=?,"
                " policy_version=?, contract_fingerprint=?, updated_at=datetime('now')"
                " WHERE contract_id=? AND status='intent'",
                (str(contract.get("adoption") or ""),
                 str(contract.get("main_note_id") or ""),
                 str(contract.get("main_note_title") or "")[:120],
                 json.dumps(contract.get("selected_pages") or [], ensure_ascii=False),
                 json.dumps(contract.get("product") or {}, ensure_ascii=False),
                 json.dumps(contract.get("destination") or {}, ensure_ascii=False),
                 str(contract.get("temperature_band") or ""),
                 str(contract.get("topic_statement") or "")[:300],
                 json.dumps(contract.get("effective_brief") or {}, ensure_ascii=False),
                 str(contract.get("content_requirement") or "")[:2000],
                 str(contract.get("policy_version") or SUPPLY_POLICY_VERSION),
                 str(contract.get("contract_fingerprint") or ""),
                 contract_id))

    def mark_submitting(self, contract_id: str) -> None:
        """外部建行请求发送前写提交意图（方案 §7.1）。

        intent → submitting；空响应/超时/进程中断后保持 submitting，
        不释放为可重新提交。attach_record（成功绑定）与 supersede
        （人工新 revision）是仅有的出口。
        """
        conn = self._connect()
        with conn:
            conn.execute(
                "UPDATE external_supply_contracts"
                " SET status='submitting', updated_at=datetime('now')"
                " WHERE contract_id=? AND status='intent'", (contract_id,))

    def list_pending_submissions(self) -> List[Dict[str, Any]]:
        """所有 submitting 合同（跨日）：每轮对账入口。"""
        rows = self._connect().execute(
            "SELECT * FROM external_supply_contracts WHERE status='submitting'"
            " ORDER BY updated_at").fetchall()
        return [self._to_dict(row) for row in rows]

    def attach_record(self, contract_id: str, record_id: str) -> None:
        conn = self._connect()
        with conn:
            conn.execute(
                "UPDATE external_supply_contracts"
                " SET status='created', record_id=?, updated_at=datetime('now')"
                " WHERE contract_id=?", (record_id, contract_id))

    # ---- 读 ----
    def get(self, contract_id: str) -> Optional[Dict[str, Any]]:
        row = self._connect().execute(
            "SELECT * FROM external_supply_contracts WHERE contract_id=?",
            (contract_id,)).fetchone()
        return self._to_dict(row) if row else None

    def find_by_record(self, record_id: str) -> Optional[Dict[str, Any]]:
        row = self._connect().execute(
            "SELECT * FROM external_supply_contracts WHERE record_id=?",
            (str(record_id or "").strip(),)).fetchone()
        return self._to_dict(row) if row else None

    @staticmethod
    def _to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        data = {key: row[key] for key in row.keys()}
        for key in ("selected_pages", "product", "destination", "effective_brief"):
            try:
                data[key] = json.loads(data.get(key) or ("{}" if key != "selected_pages" else "[]"))
            except (TypeError, ValueError):
                data[key] = {} if key != "selected_pages" else []
        return data
