"""RDS (MySQL 8) repository for the OPV tables.

Design rules
------------
- The connection URL comes from ``ORGANIC_PHOTO_VIDEO_DATABASE_URL`` (or
  ``LIKEU_AI_DATABASE_URL``); it is never logged and never appears in errors.
- ``create_task_idempotent`` relies on the unique key
  ``uq_opv_task_idempotency``: a duplicate insert returns the existing row
  with ``created=False`` instead of raising.
- Status transitions are guarded both in Python (domain.statuses) and in SQL
  (``WHERE task_status = <expected>``), so concurrent workers cannot skip
  states; a zero-rowcount update raises :class:`StaleStatusError`.
- The repository only needs a DBAPI-ish connection factory, which keeps the
  unit tests free of a real database.
"""

from __future__ import annotations

import os
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, unquote, urlparse

import pymysql
import pymysql.cursors

from domain import statuses
from domain.models import (
    AccountProfile,
    ContentPackage,
    ContentRecipe,
    ContentShot,
    ContentTask,
    FeishuOutbox,
    QualityProfile,
    QualityReview,
    RenderProfile,
    LookFeedback,
    MarketPack,
    MetricSnapshot,
    PublishRecord,
    ProductReferencePack,
    ProductionBatch,
    RenderPreset,
    ThemeCatalog,
    TaskRevision,
    VideoRender,
    dump_json,
    load_json_value,
)

DUPLICATE_KEY_ERROR = 1062


class RepositoryError(RuntimeError):
    pass


class StaleStatusError(RepositoryError):
    """Rowcount 0 on a guarded transition; the row moved under us."""


def database_url(env: Optional[Dict[str, str]] = None) -> str:
    source = os.environ if env is None else env
    value = (
        source.get("ORGANIC_PHOTO_VIDEO_DATABASE_URL")
        or source.get("LIKEU_AI_DATABASE_URL")
        or ""
    ).strip()
    if not value:
        raise RepositoryError(
            "ORGANIC_PHOTO_VIDEO_DATABASE_URL or LIKEU_AI_DATABASE_URL is required"
        )
    return value


def connect_from_url(url: str):
    """Open a pymysql connection with dict rows. The URL is never printed."""
    parsed = urlparse(url)
    if parsed.scheme not in {"mysql", "mysql+pymysql"}:
        raise RepositoryError("only mysql or mysql+pymysql URLs are supported")
    query = parse_qs(parsed.query)
    return pymysql.connect(
        host=parsed.hostname or "localhost",
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=unquote(parsed.path.lstrip("/")),
        charset=(query.get("charset") or ["utf8mb4"])[0],
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
        read_timeout=60,
        write_timeout=60,
    )


def _placeholders(count: int) -> str:
    return ",".join(["%s"] * count)


class RdsRepository:
    """Thin typed data-access layer over the OPV tables."""

    def __init__(self, connect_fn: Callable[[], Any]):
        self._connect_fn = connect_fn

    @classmethod
    def from_env(cls, env: Optional[Dict[str, str]] = None) -> "RdsRepository":
        url = database_url(env)
        return cls(lambda: connect_from_url(url))

    # ------------------------------------------------------------------
    # Execution helpers
    # ------------------------------------------------------------------

    def _run(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        *,
        fetch: Optional[str] = None,
        commit: bool = False,
    ) -> Any:
        connection = self._connect_fn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params or ())
                if fetch == "one":
                    result = cursor.fetchone()
                elif fetch == "all":
                    result = cursor.fetchall()
                else:
                    result = None
            if commit:
                connection.commit()
            return result
        finally:
            connection.close()

    def _run_scoped(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        *,
        commit: bool = False,
    ) -> Tuple[Any, int]:
        """Execute and return (result, rowcount) for guarded writes."""
        connection = self._connect_fn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params or ())
                rowcount = cursor.rowcount
            if commit:
                connection.commit()
            return None, rowcount
        finally:
            connection.close()

    def _fetch_one(self, sql: str, params: Optional[Sequence[Any]] = None):
        return self._run(sql, params, fetch="one")

    def _fetch_all(self, sql: str, params: Optional[Sequence[Any]] = None):
        return self._run(sql, params, fetch="all") or []

    def _upsert(self, table: str, key_column: str, row: Dict[str, Any]) -> None:
        update_columns = [c for c in row if c != key_column]
        sql = (
            f"INSERT INTO {table} ({','.join(row)}) "
            f"VALUES ({_placeholders(len(row))}) "
            "ON DUPLICATE KEY UPDATE "
            + ",".join(f"{c}=%s" for c in update_columns)
        )
        params = [row[c] for c in row] + [row[c] for c in update_columns]
        self._run(sql, params, commit=True)

    @staticmethod
    def _is_duplicate(exc: Exception) -> bool:
        return (
            isinstance(exc, pymysql.err.IntegrityError)
            and len(exc.args) > 0
            and exc.args[0] == DUPLICATE_KEY_ERROR
        )

    # ------------------------------------------------------------------
    # Reference data: market pack / theme / render preset / account
    # ------------------------------------------------------------------

    def upsert_market_pack(self, pack: MarketPack) -> None:
        self._upsert("opv_market_pack", "market_pack_id", pack.to_row())

    def get_market_pack(self, market_pack_id: str) -> Optional[MarketPack]:
        row = self._fetch_one(
            "SELECT * FROM opv_market_pack WHERE market_pack_id=%s", [market_pack_id]
        )
        return MarketPack.from_row(row) if row else None

    def get_market_pack_version(self, pack_key: str, pack_version: int) -> Optional[MarketPack]:
        row = self._fetch_one(
            "SELECT * FROM opv_market_pack WHERE pack_key=%s AND pack_version=%s",
            [pack_key, pack_version],
        )
        return MarketPack.from_row(row) if row else None

    def upsert_theme(self, theme: ThemeCatalog) -> None:
        self._upsert("opv_theme_catalog", "theme_id", theme.to_row())

    def get_theme(self, theme_id: str) -> Optional[ThemeCatalog]:
        row = self._fetch_one(
            "SELECT * FROM opv_theme_catalog WHERE theme_id=%s", [theme_id]
        )
        return ThemeCatalog.from_row(row) if row else None

    def list_themes(self, status: Optional[str] = None) -> List[ThemeCatalog]:
        if status:
            rows = self._fetch_all(
                "SELECT * FROM opv_theme_catalog WHERE status=%s ORDER BY theme_id",
                [status],
            )
        else:
            rows = self._fetch_all(
                "SELECT * FROM opv_theme_catalog ORDER BY theme_id"
            )
        return [ThemeCatalog.from_row(row) for row in rows]

    def upsert_render_preset(self, preset: RenderPreset) -> None:
        self._upsert("opv_render_preset", "render_preset_id", preset.to_row())

    def get_render_preset(self, render_preset_id: str) -> Optional[RenderPreset]:
        row = self._fetch_one(
            "SELECT * FROM opv_render_preset WHERE render_preset_id=%s",
            [render_preset_id],
        )
        return RenderPreset.from_row(row) if row else None

    def set_render_preset_status(self, render_preset_id: str, status: str) -> None:
        if status not in {"draft", "active", "deprecated"}:
            raise RepositoryError(f"invalid render preset status {status!r}")
        self._run(
            "UPDATE opv_render_preset SET status=%s WHERE render_preset_id=%s",
            [status, render_preset_id],
            commit=True,
        )

    def upsert_account_profile(self, account: AccountProfile) -> None:
        self._upsert("opv_account_profile", "account_id", account.to_row())

    def get_account_profile(self, account_id: str) -> Optional[AccountProfile]:
        row = self._fetch_one(
            "SELECT * FROM opv_account_profile WHERE account_id=%s", [account_id]
        )
        return AccountProfile.from_row(row) if row else None

    def list_account_profiles(self) -> List[AccountProfile]:
        rows = self._fetch_all(
            "SELECT * FROM opv_account_profile ORDER BY account_id"
        )
        return [AccountProfile.from_row(row) for row in rows]

    # ------------------------------------------------------------------
    # Product reference packs
    # ------------------------------------------------------------------

    def upsert_product_reference_pack(self, pack: ProductReferencePack) -> None:
        row = pack.to_row()
        connection = self._connect_fn()
        try:
            with connection.cursor() as cursor:
                if pack.is_default:
                    cursor.execute(
                        "UPDATE opv_product_reference_pack SET is_default=0 "
                        "WHERE product_id=%s",
                        [pack.product_id],
                    )
                update_columns = [column for column in row if column != "pack_id"]
                cursor.execute(
                    f"INSERT INTO opv_product_reference_pack ({','.join(row)}) "
                    f"VALUES ({_placeholders(len(row))}) ON DUPLICATE KEY UPDATE "
                    + ",".join(f"{column}=%s" for column in update_columns),
                    [row[column] for column in row]
                    + [row[column] for column in update_columns],
                )
            connection.commit()
        finally:
            connection.close()

    def get_product_reference_pack(
        self, pack_id: str
    ) -> Optional[ProductReferencePack]:
        row = self._fetch_one(
            "SELECT * FROM opv_product_reference_pack WHERE pack_id=%s", [pack_id]
        )
        return ProductReferencePack.from_row(row) if row else None

    def list_product_reference_packs(
        self, product_id: str
    ) -> List[ProductReferencePack]:
        rows = self._fetch_all(
            "SELECT * FROM opv_product_reference_pack WHERE product_id=%s "
            "ORDER BY is_default DESC, pack_version DESC, pack_id",
            [product_id],
        )
        return [ProductReferencePack.from_row(row) for row in rows]

    # ------------------------------------------------------------------
    # Content task: idempotent creation and guarded transitions
    # ------------------------------------------------------------------

    def create_task_idempotent(self, task: ContentTask) -> Tuple[ContentTask, bool]:
        """Insert a task; on duplicate idempotency key return the existing row.

        Returns ``(task, created)``. ``created=False`` means an earlier call
        with the same idempotency_key already created the task.
        """
        row = task.to_row()
        columns = ",".join(row)
        sql = f"INSERT INTO opv_content_task ({columns}) VALUES ({_placeholders(len(row))})"
        try:
            self._run(sql, [row[c] for c in row], commit=True)
        except Exception as exc:  # noqa: BLE001 - narrowed below
            if self._is_duplicate(exc):
                existing = self.get_task_by_idempotency_key(task.idempotency_key)
                if existing is not None:
                    return existing, False
                raise RepositoryError(
                    "duplicate task key reported but no existing row found for "
                    f"task_id={task.task_id}"
                ) from exc
            raise
        created = self.get_task(task.task_id)
        if created is None:  # pragma: no cover - defensive
            raise RepositoryError(f"task {task.task_id} disappeared after insert")
        return created, True

    def get_task(self, task_id: str) -> Optional[ContentTask]:
        row = self._fetch_one("SELECT * FROM opv_content_task WHERE task_id=%s", [task_id])
        return ContentTask.from_row(row) if row else None

    def get_task_by_idempotency_key(self, idempotency_key: str) -> Optional[ContentTask]:
        row = self._fetch_one(
            "SELECT * FROM opv_content_task WHERE idempotency_key=%s", [idempotency_key]
        )
        return ContentTask.from_row(row) if row else None

    def update_task_product_snapshot(
        self, task_id: str, product_snapshot_json: Dict[str, Any]
    ) -> None:
        """Refresh an intake-only draft's product snapshot before planning.

        Idempotency normally freezes the product block.  The sole exception is
        an untouched ``draft``/``intake`` row left by a failed first planning
        attempt: it has no plan or generated media yet, so retrying with the
        current authoritative image-pack snapshot is safe and prevents stale
        compatibility metadata from surviving a repair.
        """
        self._run(
            "UPDATE opv_content_task SET product_snapshot_json=%s "
            "WHERE task_id=%s AND task_status='draft' AND current_stage='intake'",
            [dump_json(product_snapshot_json), task_id],
            commit=True,
        )

    def list_tasks_by_source_prefix(
        self, source_type: str, source_record_prefix: str
    ) -> List[ContentTask]:
        """Return tasks owned by one external workbench record."""
        rows = self._fetch_all(
            "SELECT * FROM opv_content_task WHERE source_type=%s "
            "AND source_record_id LIKE %s ORDER BY created_at, task_id",
            [source_type, f"{source_record_prefix}%"],
        )
        return [ContentTask.from_row(row) for row in rows]

    def get_latest_product_snapshot(self, product_id: str) -> Optional[Dict[str, Any]]:
        """Reuse the latest immutable product block as the Feishu intake source."""
        row = self._fetch_one(
            "SELECT product_snapshot_json FROM opv_content_task "
            "WHERE product_id=%s ORDER BY created_at DESC LIMIT 1",
            [product_id],
        )
        if not row:
            return None
        snapshot = load_json_value(row.get("product_snapshot_json"), {})
        product = snapshot.get("product") if isinstance(snapshot, dict) else None
        if not isinstance(product, dict) or not product.get("reference_images"):
            return None
        return product

    def list_product_snapshot_candidates(
        self, product_id: str, *, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Return non-failed historical product blocks for one-time pack bootstrap."""
        rows = self._fetch_all(
            "SELECT task_id, task_status, source_type, source_record_id, "
            "product_snapshot_json, created_at FROM opv_content_task "
            "WHERE product_id=%s AND task_status<>%s "
            "ORDER BY created_at DESC LIMIT %s",
            [product_id, statuses.TASK_FAILED, int(limit)],
        )
        results: List[Dict[str, Any]] = []
        for row in rows:
            snapshot = load_json_value(row.get("product_snapshot_json"), {})
            product = snapshot.get("product") if isinstance(snapshot, dict) else None
            if not isinstance(product, dict) or not product.get("reference_images"):
                continue
            results.append({
                "task_id": row.get("task_id"),
                "task_status": row.get("task_status"),
                "source_type": row.get("source_type"),
                "source_record_id": row.get("source_record_id"),
                "created_at": row.get("created_at"),
                "product": product,
            })
        return results

    def transition_task(
        self,
        task_id: str,
        from_status: str,
        to_status: str,
        *,
        failure_code: Optional[str] = None,
        failure_detail: Optional[str] = None,
        increment_retry: bool = False,
    ) -> ContentTask:
        """Guarded status transition; both Python and SQL sides are checked.

        When ``to_status`` is ``failed``, ``failure_code``/``failure_detail``
        are persisted and ``increment_retry`` bumps ``retry_count``.
        """
        statuses.task_ensure_transition(from_status, to_status)
        target_stage = statuses.STAGE_FOR_STATUS.get(to_status, statuses.STAGE_INTAKE)
        sets = ["task_status=%s", "current_stage=%s"]
        params: List[Any] = [to_status, target_stage]
        if to_status == statuses.TASK_FAILED:
            sets.append("failure_code=%s")
            params.append(failure_code or "unknown")
            sets.append("failure_detail=%s")
            params.append(failure_detail)
            if increment_retry:
                sets.append("retry_count=retry_count+1")
            if from_status == statuses.TASK_READY_TO_PUBLISH:
                sets.append("started_at=COALESCE(started_at, started_at)")
        else:
            sets.append("failure_code=NULL")
            sets.append("failure_detail=NULL")
            if from_status == statuses.TASK_DRAFT and to_status != statuses.TASK_DRAFT:
                sets.append("started_at=UTC_TIMESTAMP(6)")
        params.extend([task_id, from_status])
        _, rowcount = self._run_scoped(
            f"UPDATE opv_content_task SET {','.join(sets)} "
            "WHERE task_id=%s AND task_status=%s",
            params,
            commit=True,
        )
        if rowcount == 0:
            current = self.get_task(task_id)
            actual = current.task_status if current else "<missing>"
            raise StaleStatusError(
                f"task {task_id} expected status {from_status!r} but is {actual!r}"
            )
        updated = self.get_task(task_id)
        if updated is None:  # pragma: no cover - defensive
            raise RepositoryError(f"task {task_id} disappeared after transition")
        return updated

    def update_task_plan(
        self,
        task_id: str,
        *,
        plan_json: Optional[Dict[str, Any]] = None,
        copy_json: Optional[Dict[str, Any]] = None,
        group_qa_json: Optional[Dict[str, Any]] = None,
        theme_id: Optional[str] = None,
        topic_text: Optional[str] = None,
        selected_render_id: Optional[str] = None,
        recipe_id: Optional[str] = None,
        recipe_version: Optional[int] = None,
        content_goal: Optional[str] = None,
        hook_strategy: Optional[str] = None,
        outfit_plan_json: Optional[Dict[str, Any]] = None,
        storyboard_version: Optional[str] = None,
        content_package_id: Optional[str] = None,
        product_facts_json: Optional[Dict[str, Any]] = None,
        workflow_version: Optional[int] = None,
    ) -> None:
        sets: List[str] = []
        params: List[Any] = []
        if plan_json is not None:

            sets.append("plan_json=%s")
            params.append(dump_json(plan_json))
        if copy_json is not None:

            sets.append("copy_json=%s")
            params.append(dump_json(copy_json))
        if group_qa_json is not None:

            sets.append("group_qa_json=%s")
            params.append(dump_json(group_qa_json))
        if theme_id is not None:
            sets.append("theme_id=%s")
            params.append(theme_id)
        if topic_text is not None:
            sets.append("topic_text=%s")
            params.append(topic_text)
        if selected_render_id is not None:
            sets.append("selected_render_id=%s")
            params.append(selected_render_id)
        if recipe_id is not None:
            sets.append("recipe_id=%s")
            params.append(recipe_id)
        if recipe_version is not None:
            sets.append("recipe_version=%s")
            params.append(recipe_version)
        if content_goal is not None:
            sets.append("content_goal=%s")
            params.append(content_goal)
        if hook_strategy is not None:
            sets.append("hook_strategy=%s")
            params.append(hook_strategy)
        if outfit_plan_json is not None:
            sets.append("outfit_plan_json=%s")
            params.append(dump_json(outfit_plan_json))
        if storyboard_version is not None:
            sets.append("storyboard_version=%s")
            params.append(storyboard_version)
        if content_package_id is not None:
            sets.append("content_package_id=%s")
            params.append(content_package_id)
        if product_facts_json is not None:
            sets.append("product_facts_json=%s")
            params.append(dump_json(product_facts_json))
        if workflow_version is not None:
            sets.append("workflow_version=%s")
            params.append(int(workflow_version))
        if not sets:
            return
        params.append(task_id)
        self._run(
            f"UPDATE opv_content_task SET {','.join(sets)} WHERE task_id=%s",
            params,
            commit=True,
        )

    # ------------------------------------------------------------------
    # Workflow V2: immutable planning revisions and scoped quality reviews
    # ------------------------------------------------------------------

    def get_task_revision(self, revision_id: str) -> Optional[TaskRevision]:
        row = self._fetch_one(
            "SELECT * FROM opv_task_revision WHERE revision_id=%s", [revision_id]
        )
        return TaskRevision.from_row(row) if row else None

    def list_task_revisions(self, task_id: str) -> List[TaskRevision]:
        rows = self._fetch_all(
            "SELECT * FROM opv_task_revision WHERE task_id=%s ORDER BY revision_no",
            [task_id],
        )
        return [TaskRevision.from_row(row) for row in rows]

    def list_recent_diversity_axes(self, account_id: str, *, exclude_source_record_id: str = "", limit: int = 100) -> List[Dict[str, Any]]:
        from services.styling_normalizer import outfit_fingerprint, outfit_visual_features
        clauses, params = ["t.account_id=%s", "t.plan_json IS NOT NULL",
                           "(t.task_status NOT IN ('failed','cancelled','canceled') OR EXISTS "
                           "(SELECT 1 FROM opv_video_render r WHERE r.task_id=t.task_id AND r.qc_status='passed'))"], [account_id]
        if exclude_source_record_id:
            clauses.append("(t.source_record_id IS NULL OR (t.source_record_id<>%s AND t.source_record_id NOT LIKE %s))")
            params.extend([exclude_source_record_id.rstrip(":"), exclude_source_record_id.rstrip(":") + ":%"])
        rows = self._fetch_all(
            "SELECT t.task_id,t.source_record_id,t.task_status,t.plan_json,t.created_at FROM opv_content_task t WHERE "
            + " AND ".join(clauses) + " ORDER BY t.created_at DESC LIMIT %s", [*params, min(max(int(limit), 1), 1000)],
        )
        output = []
        for row in rows:
            plan = load_json_value(row.get("plan_json"), {})
            axes = {**((plan.get("batch_diversity") or {}).get("axes") or {}),
                    **((plan.get("content_signature") or {}).get("axes") or {})}
            snapshot = (plan.get("look") or {}).get("snapshot") or {}
            if plan.get("outfit_sequence"):
                from services.multi_look_planner import sequence_axes
                axes.setdefault("look_sequence", sequence_axes(plan["outfit_sequence"]))
                axes.setdefault("actual_look_count", len(plan["outfit_sequence"]))
            if not axes and not snapshot.get("recipe"):
                continue
            if snapshot.get("recipe"):
                axes.setdefault("outfit_fingerprint", outfit_fingerprint(snapshot))
                axes.setdefault("silhouette_key", outfit_visual_features(snapshot)["silhouette"])
            else:
                axes.setdefault("silhouette_key", axes.get("visible_silhouette", ""))
            alternate = (plan.get("product_snapshot") or {}).get("planned_alternate_look_snapshot") or {}
            if alternate.get("recipe"):
                axes.setdefault("alternate_look_ref", alternate.get("ref_id", ""))
                axes.setdefault("alternate_outfit_fingerprint", outfit_fingerprint(alternate))
                axes.setdefault("alternate_silhouette_key", outfit_visual_features(alternate)["silhouette"])
            if axes:
                output.append({"task_id": row["task_id"], "source_record_id": row.get("source_record_id"),
                               "created_at": row.get("created_at"), "task_status": row.get("task_status"), **axes})
        return output

    def get_production_batch(self, source_record_id: str, source_type: str = "feishu_opv") -> Optional[ProductionBatch]:
        row = self._fetch_one("SELECT * FROM opv_production_batch WHERE source_type=%s AND source_record_id=%s", [source_type, source_record_id])
        return ProductionBatch.from_row(row) if row else None

    def create_production_batch_idempotent(self, batch: ProductionBatch) -> ProductionBatch:
        row = batch.to_row()
        try:
            self._run(f"INSERT INTO opv_production_batch ({','.join(row)}) VALUES ({_placeholders(len(row))})", [row[k] for k in row], commit=True)
        except Exception as exc:
            if not self._is_duplicate(exc):
                raise
        result = self.get_production_batch(batch.source_record_id, batch.source_type)
        if result is None:
            raise RepositoryError("batch disappeared after creation")
        return result

    def update_batch_manifest(self, batch_id: str, *, expected_lock_version: int, manifest_json: Dict[str, Any]) -> None:
        _, count = self._run_scoped(
            "UPDATE opv_production_batch SET manifest_json=%s,lock_version=lock_version+1 WHERE batch_id=%s AND lock_version=%s",
            [dump_json(manifest_json), batch_id, expected_lock_version], commit=True,
        )
        if count != 1:
            raise StaleStatusError("batch manifest changed")

    def queue_batch_projection(self, batch_id: str, fields: Dict[str, Any]) -> None:
        self._run("UPDATE opv_production_batch SET pending_fields_json=%s WHERE batch_id=%s", [dump_json(fields), batch_id], commit=True)

    def acknowledge_batch_projection(self, batch_id: str, fields: Dict[str, Any]) -> None:
        self._run("UPDATE opv_production_batch SET pending_fields_json=NULL WHERE batch_id=%s AND pending_fields_json=CAST(%s AS JSON)", [batch_id, dump_json(fields)], commit=True)

    def claim_batch_run(self, batch_id: str, *, owner: str, lease_seconds: int = 120) -> bool:
        _, count = self._run_scoped(
            "UPDATE opv_production_batch SET run_owner=%s,lease_until=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL %s SECOND),batch_status='running' "
            "WHERE batch_id=%s AND (run_owner IS NULL OR lease_until<UTC_TIMESTAMP(6))",
            [owner, lease_seconds, batch_id], commit=True,
        )
        return count == 1

    def heartbeat_batch_run(self, batch_id: str, *, owner: str, lease_seconds: int = 120) -> bool:
        _, count = self._run_scoped(
            "UPDATE opv_production_batch SET lease_until=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL %s SECOND) WHERE batch_id=%s AND run_owner=%s",
            [lease_seconds, batch_id, owner], commit=True,
        )
        return count == 1

    def finish_batch_run(self, batch_id: str, *, owner: str, status: str = "waiting") -> None:
        self._run("UPDATE opv_production_batch SET run_owner=NULL,lease_until=NULL,batch_status=%s WHERE batch_id=%s AND run_owner=%s", [status, batch_id, owner], commit=True)

    def create_revision_and_activate(
        self, revision: TaskRevision, *, expected_task_row_version: int,
        transition_to: Optional[str] = None,
    ) -> ContentTask:
        """Atomically create a working revision and move the task pointer.

        The task row version is a fencing token: a stale planner or a late
        rework result cannot quietly replace the current working revision.
        """
        connection = self._connect_fn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT row_version,task_status,active_revision_id,content_package_id FROM opv_content_task WHERE task_id=%s FOR UPDATE",
                    [revision.task_id],
                )
                task_row = cursor.fetchone()
                if not task_row:
                    raise RepositoryError(f"task {revision.task_id} not found")
                actual = int(task_row.get("row_version") or 1)
                if actual != int(expected_task_row_version):
                    raise StaleStatusError(
                        f"task {revision.task_id} expected row_version "
                        f"{expected_task_row_version}, got {actual}"
                    )
                if transition_to:
                    statuses.task_ensure_transition(task_row["task_status"], transition_to)
                    if task_row.get("active_revision_id") != revision.parent_revision_id:
                        raise StaleStatusError("rework parent is no longer active")
                row = revision.to_row()
                cursor.execute(
                    f"INSERT INTO opv_task_revision ({','.join(row)}) "
                    f"VALUES ({_placeholders(len(row))})",
                    [row[key] for key in row],
                )
                fields = "workflow_version=2,active_revision_id=%s,row_version=row_version+1"
                params = [revision.revision_id]
                if transition_to:
                    fields += ",task_status=%s,current_stage=%s,plan_json=%s,group_qa_json=%s"
                    params.extend([transition_to, statuses.STAGE_FOR_STATUS[transition_to],
                                   dump_json(revision.plan_snapshot_json.get("plan") or {}), dump_json({})])
                cursor.execute("UPDATE opv_content_task SET " + fields + " WHERE task_id=%s AND row_version=%s", [*params, revision.task_id, actual])
                if cursor.rowcount != 1:
                    raise StaleStatusError("task revision pointer changed during creation")
                if transition_to and task_row.get("content_package_id"):
                    selected = revision.asset_manifest_json.get("selected") or {}
                    cursor.execute(
                        "UPDATE opv_content_package SET status='generating',qa_summary_json=%s,selected_image_ids_json=%s WHERE content_package_id=%s",
                        [dump_json({}), dump_json([v for k, v in sorted(selected.items()) if k.startswith("shot:")]), task_row["content_package_id"]],
                    )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        task = self.get_task(revision.task_id)
        if task is None:  # pragma: no cover - defensive
            raise RepositoryError(f"task {revision.task_id} disappeared after revision creation")
        return task

    def update_revision_manifest(
        self,
        revision_id: str,
        *,
        expected_lock_version: int,
        asset_manifest_json: Dict[str, Any],
        selection_hash: str,
    ) -> TaskRevision:
        _, rowcount = self._run_scoped(
            "UPDATE opv_task_revision SET asset_manifest_json=%s, selection_hash=%s, "
            "lock_version=lock_version+1 WHERE revision_id=%s AND lock_version=%s "
            "AND revision_status='working'",
            [
                dump_json(asset_manifest_json), selection_hash, revision_id,
                int(expected_lock_version),
            ],
            commit=True,
        )
        if rowcount != 1:
            raise StaleStatusError(
                f"revision {revision_id} selection changed or is no longer working"
            )
        revision = self.get_task_revision(revision_id)
        if revision is None:  # pragma: no cover - defensive
            raise RepositoryError(f"revision {revision_id} disappeared after update")
        return revision

    def insert_quality_review(self, review: QualityReview) -> None:
        row = review.to_row()
        connection = self._connect_fn()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT task_id FROM opv_task_revision WHERE revision_id=%s", [review.revision_id])
                owner = cursor.fetchone()
                if not owner:
                    raise StaleStatusError("review revision is missing")
                cursor.execute("SELECT active_revision_id FROM opv_content_task WHERE task_id=%s FOR UPDATE", [owner["task_id"]])
                active = cursor.fetchone()
                cursor.execute("SELECT revision_status FROM opv_task_revision WHERE revision_id=%s FOR UPDATE", [review.revision_id])
                revision = cursor.fetchone()
                if not active or active.get("active_revision_id") != review.revision_id or not revision or revision.get("revision_status") != "working":
                    raise StaleStatusError("review revision is no longer active and working")
                cursor.execute(f"INSERT INTO opv_quality_review ({','.join(row)}) VALUES ({_placeholders(len(row))})", [row[key] for key in row])
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def list_quality_reviews(
        self, revision_id: str, *, scope: Optional[str] = None,
        target_id: Optional[str] = None,
    ) -> List[QualityReview]:
        clauses = ["revision_id=%s"]
        params: List[Any] = [revision_id]
        if scope is not None:
            clauses.append("scope=%s")
            params.append(scope)
        if target_id is not None:
            clauses.append("target_id=%s")
            params.append(target_id)
        rows = self._fetch_all(
            "SELECT * FROM opv_quality_review WHERE " + " AND ".join(clauses)
            + " ORDER BY created_at, review_id",
            params,
        )
        return [QualityReview.from_row(row) for row in rows]

    def release_revision(
        self, task_id: str, revision_id: str, *, expected_task_row_version: int,
        render_id: Optional[str] = None, review_id: Optional[str] = None,
        expected_selection_hash: Optional[str] = None, expected_input_snapshot_hash: Optional[str] = None,
    ) -> ContentTask:
        """Freeze the revision and pin it as the only releasable task output."""
        connection = self._connect_fn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT active_revision_id,row_version FROM opv_content_task "
                    "WHERE task_id=%s FOR UPDATE", [task_id]
                )
                row = cursor.fetchone()
                if not row or row.get("active_revision_id") != revision_id:
                    raise StaleStatusError("active revision changed before release")
                if int(row.get("row_version") or 1) != int(expected_task_row_version):
                    raise StaleStatusError("task row_version changed before release")
                if expected_selection_hash is not None or expected_input_snapshot_hash is not None:
                    cursor.execute("SELECT selection_hash,input_snapshot_hash FROM opv_task_revision WHERE revision_id=%s FOR UPDATE", [revision_id])
                    selected = cursor.fetchone()
                    if not selected or selected.get("selection_hash") != expected_selection_hash or selected.get("input_snapshot_hash") != expected_input_snapshot_hash:
                        raise StaleStatusError("revision inputs changed during terminal review")
                cursor.execute(
                    "UPDATE opv_task_revision SET revision_status='released' "
                    "WHERE revision_id=%s AND revision_status='working'", [revision_id]
                )
                if cursor.rowcount != 1:
                    raise StaleStatusError("revision is not releasable")
                if review_id:
                    cursor.execute("SELECT * FROM opv_quality_review WHERE revision_id=%s AND scope='render' AND target_id=%s ORDER BY created_at DESC,review_id DESC LIMIT 1 FOR UPDATE", [revision_id, render_id])
                    latest = cursor.fetchone()
                    from services.release_gate import trusted_review
                    if not latest or latest.get("review_id") != review_id or latest.get("decision") not in {"passed", "waived"} or not trusted_review(QualityReview.from_row(latest)):
                        raise StaleStatusError("terminal review changed before release")
                if render_id:
                    cursor.execute("UPDATE opv_video_render SET publish_ready=1 WHERE render_id=%s AND origin_revision_id=%s AND qc_status='passed'", [render_id, revision_id])
                    if cursor.rowcount != 1:
                        raise StaleStatusError("render is not releasable")
                cursor.execute(
                    "UPDATE opv_content_task SET released_revision_id=%s, selected_render_id=COALESCE(%s, selected_render_id), "
                    "row_version=row_version+1 WHERE task_id=%s AND row_version=%s",
                    [revision_id, render_id, task_id, int(expected_task_row_version)],
                )
                if cursor.rowcount != 1:
                    raise StaleStatusError("task changed while releasing revision")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        task = self.get_task(task_id)
        if task is None:  # pragma: no cover - defensive
            raise RepositoryError(f"task {task_id} disappeared after release")
        return task

    # ------------------------------------------------------------------
    # Content shots
    # ------------------------------------------------------------------

    def insert_shot(self, shot: ContentShot) -> None:
        row = shot.to_row()
        self._run(
            f"INSERT INTO opv_content_shot ({','.join(row)}) "
            f"VALUES ({_placeholders(len(row))})",
            [row[c] for c in row],
            commit=True,
        )

    def get_shot(self, shot_id: str) -> Optional[ContentShot]:
        row = self._fetch_one("SELECT * FROM opv_content_shot WHERE shot_id=%s", [shot_id])
        return ContentShot.from_row(row) if row else None

    def list_shots(self, task_id: str) -> List[ContentShot]:
        rows = self._fetch_all(
            "SELECT * FROM opv_content_shot WHERE task_id=%s ORDER BY slot_index, shot_version",
            [task_id],
        )
        return [ContentShot.from_row(row) for row in rows]

    def list_latest_shots(self, task_id: str) -> List[ContentShot]:
        """Latest shot version per slot (slot 1..N), selected flag wins ties."""
        by_slot: Dict[int, ContentShot] = {}
        for shot in self.list_shots(task_id):
            current = by_slot.get(shot.slot_index)
            if current is None or (shot.shot_version, shot.is_selected) > (
                current.shot_version,
                current.is_selected,
            ):
                by_slot[shot.slot_index] = shot
        return [by_slot[slot] for slot in sorted(by_slot)]

    def update_shot_status(
        self, shot_id: str, from_status: str, to_status: str
    ) -> None:
        statuses.ensure_transition(statuses.SHOT_STATUS_TRANSITIONS, from_status, to_status)
        _, rowcount = self._run_scoped(
            "UPDATE opv_content_shot SET shot_status=%s WHERE shot_id=%s AND shot_status=%s",
            [to_status, shot_id, from_status],
            commit=True,
        )
        if rowcount == 0:
            raise StaleStatusError(
                f"shot {shot_id} expected status {from_status!r} for transition"
            )

    def update_shot_generation_result(
        self,
        shot_id: str,
        *,
        generation_provider: str,
        generation_model: str,
        generation_request_id: Optional[str],
        image_oss_object_id: Optional[str],
        image_url: Optional[str],
        image_sha256: Optional[str],
        image_width: Optional[int],
        image_height: Optional[int],
    ) -> None:
        self._run(
            "UPDATE opv_content_shot SET generation_provider=%s, generation_model=%s, "
            "generation_request_id=%s, image_oss_object_id=%s, image_url=%s, "
            "image_sha256=%s, image_width=%s, image_height=%s WHERE shot_id=%s",
            [
                generation_provider,
                generation_model,
                generation_request_id,
                image_oss_object_id,
                image_url,
                image_sha256,
                image_width,
                image_height,
                shot_id,
            ],
            commit=True,
        )

    def update_shot_qa(
        self,
        shot_id: str,
        *,
        qa_status: str,
        qa_json: Optional[Dict[str, Any]],
        failure_detail: Optional[str] = None,
    ) -> None:
        if qa_status not in statuses.SHOT_QA_STATUSES:
            raise RepositoryError(f"unknown shot qa_status {qa_status!r}")

        self._run(
            "UPDATE opv_content_shot SET qa_status=%s, qa_json=%s, failure_detail=%s "
            "WHERE shot_id=%s",
            [qa_status, dump_json(qa_json) if qa_json is not None else None, failure_detail, shot_id],
            commit=True,
        )

    def set_shot_selected(self, shot_id: str, selected: bool) -> None:
        self._run(
            "UPDATE opv_content_shot SET is_selected=%s WHERE shot_id=%s",
            [1 if selected else 0, shot_id],
            commit=True,
        )

    def select_shot_version(self, task_id: str, slot_index: int, shot_id: str) -> None:
        """Select exactly one version for a slot in one transaction."""
        connection = self._connect_fn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE opv_content_shot SET is_selected=0 "
                    "WHERE task_id=%s AND slot_index=%s",
                    [task_id, int(slot_index)],
                )
                cursor.execute(
                    "UPDATE opv_content_shot SET is_selected=1 "
                    "WHERE task_id=%s AND slot_index=%s AND shot_id=%s",
                    [task_id, int(slot_index), shot_id],
                )
                if cursor.rowcount != 1:
                    raise RepositoryError(
                        f"shot {shot_id} is not slot {slot_index} of task {task_id}"
                    )
            connection.commit()
        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Video renders
    # ------------------------------------------------------------------

    def insert_render(self, render: VideoRender) -> None:
        row = render.to_row()
        try:
            self._run(
                f"INSERT INTO opv_video_render ({','.join(row)}) "
                f"VALUES ({_placeholders(len(row))})",
                [row[c] for c in row],
                commit=True,
            )
        except Exception as exc:  # noqa: BLE001
            if self._is_duplicate(exc):
                existing = self.latest_render(render.task_id)
                if existing is not None:
                    return
            raise

    def get_render(self, render_id: str) -> Optional[VideoRender]:
        row = self._fetch_one("SELECT * FROM opv_video_render WHERE render_id=%s", [render_id])
        return VideoRender.from_row(row) if row else None

    def list_renders(self, task_id: str) -> List[VideoRender]:
        rows = self._fetch_all(
            "SELECT * FROM opv_video_render WHERE task_id=%s ORDER BY render_version DESC",
            [task_id],
        )
        return [VideoRender.from_row(row) for row in rows]

    def latest_render(self, task_id: str) -> Optional[VideoRender]:
        renders = self.list_renders(task_id)
        return renders[0] if renders else None

    def update_render_status(
        self, render_id: str, from_status: str, to_status: str
    ) -> None:
        statuses.ensure_transition(statuses.RENDER_STATUS_TRANSITIONS, from_status, to_status)
        sets = "render_status=%s"
        params: List[Any] = [to_status, render_id, from_status]
        if to_status == statuses.RENDER_RENDERING:
            sets += ", started_at=UTC_TIMESTAMP(6)"
        if to_status == statuses.RENDER_COMPLETED:
            sets += ", completed_at=UTC_TIMESTAMP(6)"
        _, rowcount = self._run_scoped(
            f"UPDATE opv_video_render SET {sets} "
            "WHERE render_id=%s AND render_status=%s",
            params,
            commit=True,
        )
        if rowcount == 0:
            raise StaleStatusError(
                f"render {render_id} expected status {from_status!r} for transition"
            )

    def update_render_output(
        self,
        render_id: str,
        *,
        duration_ms: Optional[int],
        output_oss_object_id: Optional[str],
        output_url: Optional[str],
        output_sha256: Optional[str],
        output_metadata_json: Optional[Dict[str, Any]],
        qc_status: str,
        qc_json: Optional[Dict[str, Any]],
        publish_ready: bool,
    ) -> None:

        self._run(
            "UPDATE opv_video_render SET duration_ms=%s, output_oss_object_id=%s, "
            "output_url=%s, output_sha256=%s, output_metadata_json=%s, qc_status=%s, "
            "qc_json=%s, publish_ready=%s WHERE render_id=%s",
            [
                duration_ms,
                output_oss_object_id,
                output_url,
                output_sha256,
                dump_json(output_metadata_json) if output_metadata_json is not None else None,
                qc_status,
                dump_json(qc_json) if qc_json is not None else None,
                1 if publish_ready else 0,
                render_id,
            ],
            commit=True,
        )

    def set_render_publish_ready(self, render_id: str, publish_ready: bool) -> None:
        self._run(
            "UPDATE opv_video_render SET publish_ready=%s WHERE render_id=%s",
            [1 if publish_ready else 0, render_id], commit=True,
        )

    # ------------------------------------------------------------------
    # Publish records
    # ------------------------------------------------------------------

    def insert_publish_record(self, record: PublishRecord) -> None:
        row = record.to_row()
        self._run(
            f"INSERT INTO opv_publish_record ({','.join(row)}) "
            f"VALUES ({_placeholders(len(row))})",
            [row[c] for c in row],
            commit=True,
        )

    def get_publish_record(self, publish_id: str) -> Optional[PublishRecord]:
        row = self._fetch_one(
            "SELECT * FROM opv_publish_record WHERE publish_id=%s", [publish_id]
        )
        return PublishRecord.from_row(row) if row else None

    def get_publish_record_by_render(self, render_id: str) -> Optional[PublishRecord]:
        row = self._fetch_one(
            "SELECT * FROM opv_publish_record WHERE render_id=%s", [render_id]
        )
        return PublishRecord.from_row(row) if row else None

    def list_publish_records_by_account(
        self, account_id: str, *, limit: int = 50
    ) -> List[PublishRecord]:
        rows = self._fetch_all(
            "SELECT * FROM opv_publish_record WHERE account_id=%s "
            "ORDER BY created_at DESC LIMIT %s",
            [account_id, int(limit)],
        )
        return [PublishRecord.from_row(row) for row in rows]

    def list_publish_records_by_task(self, task_id: str) -> List[PublishRecord]:
        rows = self._fetch_all(
            "SELECT * FROM opv_publish_record WHERE task_id=%s "
            "ORDER BY created_at DESC",
            [task_id],
        )
        return [PublishRecord.from_row(row) for row in rows]

    def list_publish_records_due(
        self,
        publish_status: str,
        *,
        due_before: Any,
        limit: int = 100,
    ) -> List[PublishRecord]:
        rows = self._fetch_all(
            "SELECT * FROM opv_publish_record WHERE publish_status=%s "
            "AND planned_publish_at IS NOT NULL AND planned_publish_at<=%s "
            "ORDER BY planned_publish_at LIMIT %s",
            [publish_status, due_before, int(limit)],
        )
        return [PublishRecord.from_row(row) for row in rows]

    _UNSET = object()

    def update_publish_result(
        self,
        publish_id: str,
        *,
        publish_status: Any = _UNSET,
        external_post_id: Any = _UNSET,
        external_post_url: Any = _UNSET,
        planned_publish_at: Any = _UNSET,
        submitted_at: Any = _UNSET,
        published_at: Any = _UNSET,
        platform_metadata_json: Any = _UNSET,
        failure_detail: Any = _UNSET,
    ) -> None:
        """Partial update: only provided fields are written; omitted columns
        keep their values, so a failure-path update never wipes planning
        metadata. Pass a value explicitly (even None) to overwrite."""
        candidates = {
            "publish_status": publish_status,
            "external_post_id": external_post_id,
            "external_post_url": external_post_url,
            "planned_publish_at": planned_publish_at,
            "submitted_at": submitted_at,
            "published_at": published_at,
            "platform_metadata_json": (
                dump_json(platform_metadata_json)
                if isinstance(platform_metadata_json, dict)
                else platform_metadata_json
            ),
            "failure_detail": failure_detail,
        }
        sets: List[str] = []
        params: List[Any] = []
        for column, value in candidates.items():
            if value is RdsRepository._UNSET:
                continue
            sets.append(f"{column}=%s")
            params.append(value)
        if not sets:
            return
        params.append(publish_id)
        self._run(
            f"UPDATE opv_publish_record SET {','.join(sets)} WHERE publish_id=%s",
            params,
            commit=True,
        )

    # ------------------------------------------------------------------
    # Metric snapshots (upsert per publish + window)
    # ------------------------------------------------------------------

    def upsert_metric_snapshot(self, snapshot: MetricSnapshot) -> None:
        row = snapshot.to_row()
        row.pop("metric_id", None)
        update_columns = [
            "captured_after_hours",
            "captured_at",
            "source_type",
            "view_count",
            "like_count",
            "comment_count",
            "share_count",
            "save_count",
            "profile_visit_count",
            "follower_gain_count",
            "avg_watch_time_ms",
            "completion_rate",
            "engagement_rate",
            "raw_metrics_json",
        ]
        sql = (
            f"INSERT INTO opv_metric_snapshot ({','.join(row)}) "
            f"VALUES ({_placeholders(len(row))}) "
            "ON DUPLICATE KEY UPDATE "
            + ",".join(f"{c}=%s" for c in update_columns)
        )
        params = [row[c] for c in row] + [row[c] for c in update_columns]
        self._run(sql, params, commit=True)

    def list_metric_snapshots(self, publish_id: str) -> List[MetricSnapshot]:
        rows = self._fetch_all(
            "SELECT * FROM opv_metric_snapshot WHERE publish_id=%s "
            "ORDER BY captured_after_hours",
            [publish_id],
        )
        return [MetricSnapshot.from_row(row) for row in rows]

    # ------------------------------------------------------------------
    # Look feedback
    # ------------------------------------------------------------------

    def insert_look_feedback(self, feedback: LookFeedback) -> None:
        row = feedback.to_row()
        self._run(
            f"INSERT INTO opv_look_feedback ({','.join(row)}) "
            f"VALUES ({_placeholders(len(row))})",
            [row[c] for c in row],
            commit=True,
        )

    def commit_group_approval(
        self,
        *,
        task_id: str,
        shots: Sequence[ContentShot],
        feedback: LookFeedback,
        group_qa_json: Dict[str, Any],
        package_fields: Dict[str, Any],
        expected_revision_id: Optional[str] = None,
        expected_selection_hash: Optional[str] = None,
        group_review_id: Optional[str] = None,
    ) -> None:
        """Atomically approve a five-shot group and advance task/package.

        This intentionally is a domain-specific transaction instead of a
        generic unit-of-work abstraction: the approval gate is the one place
        where partial writes previously produced an unrecoverable mixed state.
        """

        if len(shots) != 5:
            raise RepositoryError("group approval requires exactly five shots")
        shot_ids = [shot.shot_id for shot in shots]
        connection = self._connect_fn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT task_status, content_package_id, workflow_version, active_revision_id FROM opv_content_task "
                    "WHERE task_id=%s FOR UPDATE",
                    [task_id],
                )
                task_row = cursor.fetchone()
                if not task_row or task_row.get("task_status") != statuses.TASK_IMAGE_REVIEW:
                    actual = task_row.get("task_status") if task_row else "<missing>"
                    raise StaleStatusError(
                        f"task {task_id} expected image_review but is {actual!r}"
                    )

                v2 = int(task_row.get("workflow_version") or 1) >= 2 or bool(task_row.get("active_revision_id"))
                if v2:
                    if (not expected_revision_id or not expected_selection_hash or not group_review_id
                            or task_row.get("active_revision_id") != expected_revision_id):
                        raise StaleStatusError("V2 group approval requires the current revision and review guards")
                    cursor.execute("SELECT * FROM opv_task_revision WHERE revision_id=%s FOR UPDATE", [expected_revision_id])
                    revision_row = cursor.fetchone()
                    if not revision_row:
                        raise StaleStatusError("V2 approval revision is missing")
                    revision = TaskRevision.from_row(revision_row)
                    from services.workflow_v2 import RevisionAssetResolver, canonical_hash, review_actor
                    selected = revision.asset_manifest_json.get("selected") or {}
                    if (revision.task_id != task_id or revision.revision_status != "working"
                            or revision.selection_hash != expected_selection_hash
                            or canonical_hash(selected) != expected_selection_hash
                            or canonical_hash(revision.plan_snapshot_json) != revision.input_snapshot_hash):
                        raise StaleStatusError("V2 approval inputs or selection changed")
                    keys = [f"shot:{index}" for index in range(1, 6)]
                    selected_ids = [selected.get(key) for key in keys]
                    if len(set(selected_ids)) != 5 or set(selected_ids) != set(shot_ids):
                        raise StaleStatusError("V2 approval does not match the selected shot set")
                    fingerprint = RevisionAssetResolver.fingerprint(revision, keys, extra={
                        "quality_contract": (revision.plan_snapshot_json.get("plan") or {}).get("quality_contract") or {}})
                    cursor.execute(
                        "SELECT * FROM opv_quality_review WHERE revision_id=%s AND scope=%s AND target_id=%s "
                        "AND input_fingerprint=%s ORDER BY created_at DESC, review_id DESC LIMIT 1 FOR UPDATE",
                        [expected_revision_id, "group", "group", fingerprint],
                    )
                    review_row = cursor.fetchone()
                    from services.release_gate import trusted_review
                    if (not review_row or review_row.get("review_id") != group_review_id
                            or review_row.get("decision") not in {"passed", "waived"}
                            or not trusted_review(QualityReview.from_row(review_row))):
                        raise StaleStatusError("V2 approval group review is missing, superseded or not passed")
                    cursor.execute(
                        f"SELECT * FROM opv_content_shot WHERE task_id=%s AND shot_id IN ({_placeholders(len(shot_ids))}) FOR UPDATE",
                        [task_id, *shot_ids],
                    )
                    locked = {row["shot_id"]: ContentShot.from_row(row) for row in cursor.fetchall()}
                    if set(locked) != set(shot_ids):
                        raise StaleStatusError("V2 approval shot rows changed")
                    from services.media_qc import MediaQcError, require_shot_media_qc
                    for index, asset_id in enumerate(selected_ids, 1):
                        shot = locked[asset_id]
                        asset = RevisionAssetResolver.selected(revision, f"shot:{index}")
                        if shot.slot_index != index or shot.image_sha256 != asset.get("sha256"):
                            raise StaleStatusError("V2 approval shot hash or slot changed")
                        try:
                            require_shot_media_qc(shot)
                        except (MediaQcError, OSError) as exc:
                            raise StaleStatusError("V2 approval technical media QC is missing or stale") from exc

                cursor.execute(
                    "UPDATE opv_content_shot SET is_selected=0 WHERE task_id=%s",
                    [task_id],
                )
                qa_clause = "" if v2 else " AND qa_status=%s"
                cursor.execute(
                    "UPDATE opv_content_shot SET is_selected=1, shot_status=%s "
                    f"WHERE task_id=%s AND shot_id IN ({_placeholders(len(shot_ids))}) "
                    "AND shot_status IN (%s,%s)" + qa_clause,
                    [
                        statuses.SHOT_APPROVED,
                        task_id,
                        *shot_ids,
                        statuses.SHOT_GENERATED,
                        statuses.SHOT_APPROVED,
                        *([] if v2 else [statuses.QC_PASSED]),
                    ],
                )
                if cursor.rowcount != len(shot_ids):
                    raise StaleStatusError(
                        f"task {task_id} shot set changed during approval"
                    )

                feedback_row = feedback.to_row()
                cursor.execute(
                    f"INSERT INTO opv_look_feedback ({','.join(feedback_row)}) "
                    f"VALUES ({_placeholders(len(feedback_row))})",
                    [feedback_row[column] for column in feedback_row],
                )

                package_id = task_row.get("content_package_id")
                if package_id:
                    json_fields = {
                        "selected_image_ids_json",
                        "hashtags_json",
                        "qa_summary_json",
                    }
                    allowed_fields = {
                        "cover_image_id",
                        "cover_title",
                        "caption",
                        "selected_image_ids_json",
                        "hashtags_json",
                        "qa_summary_json",
                    }
                    invalid = set(package_fields) - allowed_fields
                    if invalid:
                        raise RepositoryError(
                            f"unknown atomic package fields: {sorted(invalid)}"
                        )
                    sets = ["status=%s"]
                    params: List[Any] = [statuses.PACKAGE_READY]
                    for key, value in package_fields.items():
                        sets.append(f"{key}=%s")
                        params.append(
                            dump_json(value) if key in json_fields else value
                        )
                    params.extend(
                        [package_id, statuses.PACKAGE_QA_REVIEW]
                    )
                    cursor.execute(
                        f"UPDATE opv_content_package SET {','.join(sets)} "
                        "WHERE content_package_id=%s AND status=%s",
                        params,
                    )
                    if cursor.rowcount != 1:
                        raise StaleStatusError(
                            f"content package {package_id} is not in qa_review"
                        )

                cursor.execute(
                    "UPDATE opv_content_task SET group_qa_json=%s, task_status=%s, "
                    "current_stage=%s WHERE task_id=%s AND task_status=%s",
                    [
                        dump_json(group_qa_json),
                        statuses.TASK_RENDERING,
                        statuses.STAGE_RENDERING,
                        task_id,
                        statuses.TASK_IMAGE_REVIEW,
                    ],
                )
                if cursor.rowcount != 1:
                    raise StaleStatusError(
                        f"task {task_id} changed during approval commit"
                    )
            connection.commit()
        except Exception:
            rollback = getattr(connection, "rollback", None)
            if callable(rollback):
                rollback()
            raise
        finally:
            connection.close()

    def get_look_feedback(self, feedback_id: str) -> Optional[LookFeedback]:
        row = self._fetch_one(
            "SELECT * FROM opv_look_feedback WHERE feedback_id=%s", [feedback_id]
        )
        return LookFeedback.from_row(row) if row else None

    def list_look_feedback(
        self,
        *,
        decision: Optional[str] = None,
        promotion_status: Optional[str] = None,
        limit: int = 100,
    ) -> List[LookFeedback]:
        clauses: List[str] = []
        params: List[Any] = []
        if decision:
            clauses.append("decision=%s")
            params.append(decision)
        if promotion_status:
            clauses.append("promotion_status=%s")
            params.append(promotion_status)
        where = f"WHERE {' AND '.join(clauses)} " if clauses else ""
        params.append(int(limit))
        rows = self._fetch_all(
            f"SELECT * FROM opv_look_feedback {where}ORDER BY created_at DESC LIMIT %s",
            params,
        )
        return [LookFeedback.from_row(row) for row in rows]

    # ------------------------------------------------------------------
    # Capability upgrade: content recipes / profiles / content packages
    # ------------------------------------------------------------------

    def upsert_content_recipe(self, recipe: ContentRecipe) -> None:
        self._upsert("opv_content_recipe", "recipe_id", recipe.to_row())

    def get_content_recipe(self, recipe_id: str) -> Optional[ContentRecipe]:
        row = self._fetch_one(
            "SELECT * FROM opv_content_recipe WHERE recipe_id=%s", [recipe_id]
        )
        return ContentRecipe.from_row(row) if row else None

    def list_content_recipes(self, status: Optional[str] = None) -> List[ContentRecipe]:
        if status:
            rows = self._fetch_all(
                "SELECT * FROM opv_content_recipe WHERE status=%s ORDER BY recipe_id",
                [status],
            )
        else:
            rows = self._fetch_all(
                "SELECT * FROM opv_content_recipe ORDER BY recipe_id"
            )
        return [ContentRecipe.from_row(row) for row in rows]

    def upsert_render_profile(self, profile: RenderProfile) -> None:
        self._upsert("opv_render_profile", "render_profile_id", profile.to_row())

    def get_render_profile(self, render_profile_id: str) -> Optional[RenderProfile]:
        row = self._fetch_one(
            "SELECT * FROM opv_render_profile WHERE render_profile_id=%s",
            [render_profile_id],
        )
        return RenderProfile.from_row(row) if row else None

    def upsert_quality_profile(self, profile: QualityProfile) -> None:
        self._upsert("opv_quality_profile", "quality_profile_id", profile.to_row())

    def get_quality_profile(self, quality_profile_id: str) -> Optional[QualityProfile]:
        row = self._fetch_one(
            "SELECT * FROM opv_quality_profile WHERE quality_profile_id=%s",
            [quality_profile_id],
        )
        return QualityProfile.from_row(row) if row else None

    def insert_content_package(self, package: ContentPackage) -> None:
        row = package.to_row()
        self._run(
            f"INSERT INTO opv_content_package ({','.join(row)}) "
            f"VALUES ({_placeholders(len(row))})",
            [row[c] for c in row],
            commit=True,
        )

    def create_content_package_idempotent(
        self, package: ContentPackage
    ) -> Tuple[ContentPackage, bool]:
        """Create one package per task; the database unique key arbitrates races."""
        try:
            self.insert_content_package(package)
            return self.get_content_package(package.content_package_id) or package, True
        except Exception as exc:  # noqa: BLE001
            if not self._is_duplicate(exc):
                raise
            existing = self.get_content_package_by_task(package.task_id)
            if existing is None:
                raise RepositoryError(
                    f"duplicate package reported for task {package.task_id}, but row missing"
                ) from exc
            return existing, False

    def get_content_package(self, content_package_id: str) -> Optional[ContentPackage]:
        row = self._fetch_one(
            "SELECT * FROM opv_content_package WHERE content_package_id=%s",
            [content_package_id],
        )
        return ContentPackage.from_row(row) if row else None

    def get_content_package_by_task(self, task_id: str) -> Optional[ContentPackage]:
        row = self._fetch_one(
            "SELECT * FROM opv_content_package WHERE task_id=%s "
            "ORDER BY created_at DESC LIMIT 1",
            [task_id],
        )
        return ContentPackage.from_row(row) if row else None

    def update_content_package(
        self, content_package_id: str, **fields: Any
    ) -> None:
        from domain.models import dump_json

        json_fields = {
            "selected_image_ids_json",
            "hashtags_json",
            "render_ids_json",
            "generation_lineage_json",
            "qa_summary_json",
        }
        sets: List[str] = []
        params: List[Any] = []
        for key, value in fields.items():
            if key not in {
                "product_snapshot_id", "recipe_id", "theme_id", "outfit_plan_id",
                "storyboard_version", "selected_image_ids_json", "cover_image_id",
                "cover_title", "caption", "render_ids_json", "qa_summary_json",
                "content_fingerprint", "generation_lineage_json", "status",
                "hashtags_json", "selected_image_ids_json",
            }:
                raise RepositoryError(f"unknown content package column {key!r}")
            params.append(dump_json(value) if key in json_fields else value)
            sets.append(f"{key}=%s")
        if not sets:
            return
        params.append(content_package_id)
        self._run(
            f"UPDATE opv_content_package SET {','.join(sets)} WHERE content_package_id=%s",
            params,
            commit=True,
        )

    # ------------------------------------------------------------------
    # Feishu outbox (holding by default; release is an explicit operation)
    # ------------------------------------------------------------------

    def enqueue_outbox(self, entry: FeishuOutbox) -> Tuple[FeishuOutbox, bool]:
        """Idempotent enqueue keyed by (aggregate_type, aggregate_id, operation)."""
        existing = self._fetch_one(
            "SELECT * FROM opv_feishu_outbox WHERE aggregate_type=%s AND aggregate_id=%s "
            "AND operation=%s",
            [entry.aggregate_type, entry.aggregate_id, entry.operation],
        )
        if existing:
            return FeishuOutbox.from_row(existing), False
        row = entry.to_row()
        try:
            self._run(
                f"INSERT INTO opv_feishu_outbox ({','.join(row)}) "
                f"VALUES ({_placeholders(len(row))})",
                [row[c] for c in row],
                commit=True,
            )
        except Exception as exc:  # noqa: BLE001
            if self._is_duplicate(exc):
                again = self._fetch_one(
                    "SELECT * FROM opv_feishu_outbox WHERE aggregate_type=%s AND "
                    "aggregate_id=%s AND operation=%s",
                    [entry.aggregate_type, entry.aggregate_id, entry.operation],
                )
                if again:
                    return FeishuOutbox.from_row(again), False
            raise
        created = self.get_outbox(entry.outbox_id)
        if created is None:  # pragma: no cover - defensive
            raise RepositoryError(f"outbox {entry.outbox_id} disappeared after insert")
        return created, True

    def get_outbox(self, outbox_id: str) -> Optional[FeishuOutbox]:
        row = self._fetch_one(
            "SELECT * FROM opv_feishu_outbox WHERE outbox_id=%s", [outbox_id]
        )
        return FeishuOutbox.from_row(row) if row else None

    def list_outbox(
        self,
        status: str,
        *,
        limit: int = 100,
        due_before: Optional[Any] = None,
    ) -> List[FeishuOutbox]:
        params: List[Any] = [status]
        due_clause = ""
        if due_before is not None:
            due_clause = " AND (not_before IS NULL OR not_before <= %s) "
            params.append(due_before)
        params.append(int(limit))
        rows = self._fetch_all(
            "SELECT * FROM opv_feishu_outbox WHERE status=%s" + due_clause +
            " ORDER BY created_at LIMIT %s",
            params,
        )
        return [FeishuOutbox.from_row(row) for row in rows]

    def update_outbox_status(
        self,
        outbox_id: str,
        from_status: str,
        to_status: str,
        *,
        last_error: Optional[str] = None,
        increment_attempts: bool = False,
        synced_at: Optional[Any] = None,
    ) -> None:
        statuses.ensure_transition(statuses.OUTBOX_STATUS_TRANSITIONS, from_status, to_status)
        sets = ["status=%s"]
        params: List[Any] = [to_status]
        sets.append("last_error=%s")
        params.append(last_error)
        if increment_attempts:
            sets.append("attempts=attempts+1")
        if synced_at is not None:
            sets.append("synced_at=%s")
            params.append(synced_at)
        params.extend([outbox_id, from_status])
        _, rowcount = self._run_scoped(
            f"UPDATE opv_feishu_outbox SET {','.join(sets)} "
            "WHERE outbox_id=%s AND status=%s",
            params,
            commit=True,
        )
        if rowcount == 0:
            raise StaleStatusError(
                f"outbox {outbox_id} expected status {from_status!r} for transition"
            )
