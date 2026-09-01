"""RDS (MySQL 8) repository for the 11 ``opv_*`` tables.

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
    RenderProfile,
    LookFeedback,
    MarketPack,
    MetricSnapshot,
    PublishRecord,
    RenderPreset,
    ThemeCatalog,
    VideoRender,
    dump_json,
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
        if not sets:
            return
        params.append(task_id)
        self._run(
            f"UPDATE opv_content_task SET {','.join(sets)} WHERE task_id=%s",
            params,
            commit=True,
        )

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

    _UNSET = object()

    def update_publish_result(
        self,
        publish_id: str,
        *,
        publish_status: Any = _UNSET,
        external_post_id: Any = _UNSET,
        external_post_url: Any = _UNSET,
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
                    "SELECT task_status, content_package_id FROM opv_content_task "
                    "WHERE task_id=%s FOR UPDATE",
                    [task_id],
                )
                task_row = cursor.fetchone()
                if not task_row or task_row.get("task_status") != statuses.TASK_IMAGE_REVIEW:
                    actual = task_row.get("task_status") if task_row else "<missing>"
                    raise StaleStatusError(
                        f"task {task_id} expected image_review but is {actual!r}"
                    )

                cursor.execute(
                    "UPDATE opv_content_shot SET is_selected=0 WHERE task_id=%s",
                    [task_id],
                )
                cursor.execute(
                    "UPDATE opv_content_shot SET is_selected=1, shot_status=%s "
                    f"WHERE task_id=%s AND shot_id IN ({_placeholders(len(shot_ids))}) "
                    "AND shot_status=%s AND qa_status=%s",
                    [
                        statuses.SHOT_APPROVED,
                        task_id,
                        *shot_ids,
                        statuses.SHOT_GENERATED,
                        statuses.QC_PASSED,
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
