from __future__ import annotations

from datetime import datetime, timedelta
import os
import socket
from typing import Any

from auto_mixcut.core.result import Result

from .context import SkillContext


def product_run_lock_key(product_id: str) -> str:
    return f"product:{str(product_id or '').strip()}"


def default_product_run_owner(prefix: str = "worker") -> str:
    return f"{prefix}:{socket.gethostname()}:{os.getpid()}"


class ProductRunLockSkill:
    """Product-level lease used by scanner, guard and AI heartbeat.

    The lock is intentionally product-scoped, not product+shop scoped: for the
    mixcut factory, two processes touching the same product can race even when
    they came from different table rows or shop fields.
    """

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def acquire(self, product_id: str, owner: str = "", shop_id: str = "", ttl_minutes: int = 60) -> Result:
        product_id = str(product_id or "").strip()
        if not product_id:
            return Result.fail("PRODUCT_ID_REQUIRED", "product_id is required")
        ensured = ensure_product_run_lock_table(self.ctx)
        if not ensured.success:
            return ensured

        owner = str(owner or "").strip() or default_product_run_owner("product_lock")
        lock_key = product_run_lock_key(product_id)
        now = _db_ts(self.ctx, datetime.utcnow())
        expires = _db_ts(self.ctx, datetime.utcnow() + timedelta(minutes=max(5, int(ttl_minutes or 60))))
        dialect = getattr(self.ctx.repo, "dialect", "sqlite")
        try:
            with self.ctx.repo.connect() as conn:
                if dialect == "mysql":
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM mixcut_task_locks WHERE lock_key=%s AND (expires_at<=%s OR status!='running')", (lock_key, now))
                        cur.execute("SELECT * FROM mixcut_task_locks WHERE lock_key=%s", (lock_key,))
                        current = cur.fetchone()
                        if current:
                            if str(current.get("owner") or "") == owner:
                                cur.execute("UPDATE mixcut_task_locks SET heartbeat_at=%s, expires_at=%s, updated_at=%s WHERE lock_key=%s AND owner=%s", (now, expires, now, lock_key, owner))
                                current["expires_at"] = expires
                            return _current_lock_result(current, owner)
                        cur.execute(
                            """
                            INSERT INTO mixcut_task_locks
                              (lock_key, product_id, shop_id, owner, status, locked_at, expires_at, heartbeat_at, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, 'running', %s, %s, %s, %s, %s)
                            """,
                            (lock_key, product_id, shop_id, owner, now, expires, now, now, now),
                        )
                else:
                    conn.execute("DELETE FROM mixcut_task_locks WHERE lock_key=? AND (expires_at<=? OR status!='running')", (lock_key, now))
                    current_row = conn.execute("SELECT * FROM mixcut_task_locks WHERE lock_key=?", (lock_key,)).fetchone()
                    if current_row:
                        current = dict(current_row)
                        if str(current.get("owner") or "") == owner:
                            conn.execute("UPDATE mixcut_task_locks SET heartbeat_at=?, expires_at=?, updated_at=? WHERE lock_key=? AND owner=?", (now, expires, now, lock_key, owner))
                            current["expires_at"] = expires
                        return _current_lock_result(current, owner)
                    conn.execute(
                        """
                        INSERT INTO mixcut_task_locks
                          (lock_key, product_id, shop_id, owner, status, locked_at, expires_at, heartbeat_at, created_at, updated_at)
                        VALUES (?, ?, ?, ?, 'running', ?, ?, ?, ?, ?)
                        """,
                        (lock_key, product_id, shop_id, owner, now, expires, now, now, now),
                    )
            return Result.ok({"acquired": True, "lock_key": lock_key, "owner": owner, "expires_at": expires, "release_on_exit": True})
        except Exception as exc:
            current = _get_current_lock(self.ctx, lock_key)
            if current and str(current.get("owner") or "") == owner and str(current.get("status") or "") == "running":
                heartbeat = self.heartbeat(product_id, owner=owner, ttl_minutes=ttl_minutes)
                if heartbeat.success:
                    data = heartbeat.data or {}
                    return Result.ok({**data, "acquired": True, "reentrant": True, "release_on_exit": False})
            return Result.ok(
                {
                    "acquired": False,
                    "lock_key": lock_key,
                    "owner": owner,
                    "held_by": (current or {}).get("owner"),
                    "expires_at": (current or {}).get("expires_at"),
                    "error": str(exc),
                }
            )

    def heartbeat(self, product_id: str, owner: str = "", ttl_minutes: int = 60) -> Result:
        product_id = str(product_id or "").strip()
        owner = str(owner or "").strip()
        if not product_id or not owner:
            return Result.fail("LOCK_HEARTBEAT_REQUIRED", "product_id and owner are required")
        lock_key = product_run_lock_key(product_id)
        now = _db_ts(self.ctx, datetime.utcnow())
        expires = _db_ts(self.ctx, datetime.utcnow() + timedelta(minutes=max(5, int(ttl_minutes or 60))))
        dialect = getattr(self.ctx.repo, "dialect", "sqlite")
        try:
            with self.ctx.repo.connect() as conn:
                if dialect == "mysql":
                    with conn.cursor() as cur:
                        cur.execute("UPDATE mixcut_task_locks SET heartbeat_at=%s, expires_at=%s, updated_at=%s WHERE lock_key=%s AND owner=%s AND status='running'", (now, expires, now, lock_key, owner))
                        updated = cur.rowcount
                else:
                    cur = conn.execute("UPDATE mixcut_task_locks SET heartbeat_at=?, expires_at=?, updated_at=? WHERE lock_key=? AND owner=? AND status='running'", (now, expires, now, lock_key, owner))
                    updated = cur.rowcount
            return Result.ok({"acquired": bool(updated), "lock_key": lock_key, "owner": owner, "expires_at": expires, "release_on_exit": False})
        except Exception as exc:
            return Result.fail("PRODUCT_RUN_LOCK_HEARTBEAT_FAILED", str(exc), {"lock_key": lock_key, "owner": owner})

    def release(self, product_id: str, owner: str = "") -> Result:
        product_id = str(product_id or "").strip()
        owner = str(owner or "").strip()
        if not product_id or not owner:
            return Result.ok({"released": False, "reason": "missing_product_or_owner"})
        lock_key = product_run_lock_key(product_id)
        now = _db_ts(self.ctx, datetime.utcnow())
        dialect = getattr(self.ctx.repo, "dialect", "sqlite")
        try:
            with self.ctx.repo.connect() as conn:
                if dialect == "mysql":
                    with conn.cursor() as cur:
                        cur.execute("UPDATE mixcut_task_locks SET status='released', updated_at=%s WHERE lock_key=%s AND owner=%s", (now, lock_key, owner))
                        released = cur.rowcount
                else:
                    cur = conn.execute("UPDATE mixcut_task_locks SET status='released', updated_at=? WHERE lock_key=? AND owner=?", (now, lock_key, owner))
                    released = cur.rowcount
            return Result.ok({"released": int(released or 0), "lock_key": lock_key, "owner": owner})
        except Exception as exc:
            return Result.fail("PRODUCT_RUN_LOCK_RELEASE_FAILED", str(exc), {"lock_key": lock_key, "owner": owner})


def ensure_product_run_lock_table(ctx: SkillContext) -> Result:
    dialect = getattr(ctx.repo, "dialect", "sqlite")
    try:
        with ctx.repo.connect() as conn:
            if dialect == "mysql":
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS mixcut_task_locks (
                          lock_key VARCHAR(191) NOT NULL PRIMARY KEY,
                          product_id VARCHAR(128) NOT NULL,
                          shop_id VARCHAR(128),
                          owner VARCHAR(191) NOT NULL,
                          status VARCHAR(32) NOT NULL,
                          locked_at DATETIME,
                          expires_at DATETIME,
                          heartbeat_at DATETIME,
                          created_at DATETIME,
                          updated_at DATETIME,
                          KEY idx_mixcut_task_locks_product (product_id),
                          KEY idx_mixcut_task_locks_status (status, expires_at)
                        )
                        """
                    )
            else:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS mixcut_task_locks (
                      lock_key TEXT PRIMARY KEY,
                      product_id TEXT NOT NULL,
                      shop_id TEXT,
                      owner TEXT NOT NULL,
                      status TEXT NOT NULL,
                      locked_at TEXT,
                      expires_at TEXT,
                      heartbeat_at TEXT,
                      created_at TEXT,
                      updated_at TEXT
                    )
                    """
                )
        return Result.ok({"table": "mixcut_task_locks"})
    except Exception as exc:
        return Result.fail("PRODUCT_RUN_LOCK_SCHEMA_FAILED", str(exc), {"table": "mixcut_task_locks"})


def _current_lock_result(current: dict[str, Any], owner: str) -> Result:
    if str(current.get("owner") or "") == owner:
        return Result.ok(
            {
                "acquired": True,
                "reentrant": True,
                "release_on_exit": False,
                "lock_key": current.get("lock_key"),
                "owner": owner,
                "expires_at": current.get("expires_at"),
            }
        )
    return Result.ok(
        {
            "acquired": False,
            "lock_key": current.get("lock_key"),
            "owner": owner,
            "held_by": current.get("owner"),
            "expires_at": current.get("expires_at"),
        }
    )


def _get_current_lock(ctx: SkillContext, lock_key: str) -> dict[str, Any]:
    try:
        return ctx.repo.get("mixcut_task_locks", "lock_key", lock_key) or {}
    except Exception:
        return {}


def _db_ts(ctx: SkillContext, value: datetime) -> str:
    if getattr(ctx.repo, "dialect", "sqlite") == "mysql":
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value.isoformat(timespec="seconds")
