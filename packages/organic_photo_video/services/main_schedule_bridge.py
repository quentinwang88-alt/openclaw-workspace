"""Bridge completed OPV renders into the existing non-shoppable scheduler."""

from __future__ import annotations

import json
import os
import sqlite3
import copy
from pathlib import Path
import sys
from typing import Any, Dict, Optional


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
PUBLISHER_ROOT = WORKSPACE_ROOT / "skills" / "short-video-auto-publisher"
if str(PUBLISHER_ROOT) not in sys.path:
    sys.path.insert(0, str(PUBLISHER_ROOT))

from app.db import AutoPublishDB  # noqa: E402
from app.models import ScriptMetadata  # noqa: E402
from services.neobund_music import task_content_profile_inputs  # noqa: E402


class MainScheduleBridgeError(RuntimeError):
    pass


def assert_main_queue_rework_allowed(task_id: str, *, db_path: Optional[Path] = None) -> None:
    """Read-only guard: never instantiate AutoPublishDB or migrate production DB."""
    path = Path(db_path or os.environ.get("SHORT_VIDEO_AUTO_PUBLISH_DB_PATH") or
                (Path(os.environ.get("OPENCLAW_SHARED_DATA_DIR", str(Path.home() / ".openclaw/shared/data")))
                 / "short_video_auto_publish.sqlite3"))
    if not path.exists():
        return
    with sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True) as conn:
        asset = conn.execute(
            "SELECT publish_status FROM video_assets WHERE canonical_script_key=?",
            (f"opv:{task_id}",),
        ).fetchone()
        slot = conn.execute(
            "SELECT slot_id FROM publish_slots WHERE canonical_script_key=? "
            "AND schedule_status IN ('提交中','提交结果不明','已排期','已发布') LIMIT 1",
            (f"opv:{task_id}",),
        ).fetchone()
    if slot or (asset and str(asset[0] or "") not in {"", "已取消"}):
        raise MainScheduleBridgeError("任务已进入主发布队列；请先核对并解除发布占用，再返工")


class MainScheduleBridge:
    def __init__(
        self,
        repository: Any,
        *,
        db: Optional[AutoPublishDB] = None,
        route_path: Optional[Path] = None,
    ) -> None:
        self.repository = repository
        self.db = db or AutoPublishDB()
        path = route_path or PACKAGE_ROOT / "config" / "main_publish_routes.json"
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        self.routes = payload.get("routes") or {}

    def _store_id(self, country: str) -> str:
        route = self.routes.get(str(country or "").strip().upper()) or {}
        store_id = str(route.get("default_store_id") or "").strip()
        if not store_id:
            raise MainScheduleBridgeError(
                f"未配置 {country or '未知国家'} 的主短视频排班店铺，任务保留待处理"
            )
        return store_id

    @staticmethod
    def _product_snapshot(task: Any) -> Dict[str, Any]:
        snapshot = task.product_snapshot_json or {}
        product = snapshot.get("product") if isinstance(snapshot, dict) else {}
        if not isinstance(product, dict):
            return {}
        return product

    @classmethod
    def _product_type(cls, task: Any) -> str:
        snapshot = task.product_snapshot_json or {}
        product = cls._product_snapshot(task)
        return str(
            product.get("category")
            or product.get("product_type")
            or snapshot.get("category")
            or "apparel"
        ).strip()

    @classmethod
    def _source_product_id(cls, task: Any) -> str:
        """Use the immutable snapshot that actually produced the rendered video."""
        product = cls._product_snapshot(task)
        return str(product.get("product_id") or task.product_id or "").strip()

    @classmethod
    def _publish_title(cls, task: Any, country: str) -> str:
        copy_block = task.copy_json or {}
        title = str(copy_block.get("title") or copy_block.get("caption") or "").strip()
        if not title:
            return "Daily outfit ideas"
        if str(country or "").strip().upper() != "TH":
            return title
        product = cls._product_snapshot(task)
        frozen_name = str(product.get("product_name") or "").strip()
        if not frozen_name or frozen_name not in title:
            return title
        category = str(
            product.get("category") or product.get("product_type") or ""
        ).strip().lower()
        localized = {
            "outerwear": "เสื้อตัวนอกตัวนี้",
            "dress": "เดรสตัวนี้",
            "top": "เสื้อตัวนี้",
            "bottom": "กางเกงตัวนี้",
        }.get(category, "ไอเทมชิ้นนี้")
        return title.replace(frozen_name, localized)

    def _bgm_mood_hints(self, task: Any) -> list[str]:
        plan = task.plan_json or {}
        direct = plan.get("bgm_mood_hints") if isinstance(plan, dict) else None
        if isinstance(direct, list) and direct:
            return [str(item).strip() for item in direct if str(item).strip()]
        plan_theme_id = ""
        if isinstance(plan, dict) and isinstance(plan.get("theme"), dict):
            plan_theme_id = str(plan["theme"].get("id") or "").strip()
        theme_id = plan_theme_id or str(task.theme_id or "").strip()
        getter = getattr(self.repository, "get_theme", None)
        theme = getter(theme_id) if callable(getter) and theme_id else None
        rules = getattr(theme, "content_plan_rules_json", {}) or {}
        raw = rules.get("bgm_mood_hints") if isinstance(rules, dict) else []
        return [str(item).strip() for item in (raw or []) if str(item).strip()]

    @staticmethod
    def _script_slot(task_id: str) -> str:
        """Return a retry-stable slot unique to one OPV task.

        A Feishu production row can own several rendered videos.  The main
        scheduler's identity is ``(source_record_id, script_slot)``, so the
        historical constant ``OPV`` collided on the second video in a row.
        Keep the Feishu record as the grouping key and put the immutable OPV
        task id in the slot dimension.
        """
        normalized = str(task_id or "").strip()
        if not normalized:
            raise MainScheduleBridgeError("OPV 任务缺少稳定 task_id")
        return f"OPV:{normalized}"

    def enqueue_task(self, task_id: str, *, feishu_record_id: str = "") -> Dict[str, str]:
        task = self.repository.get_task(task_id)
        if task is None:
            raise MainScheduleBridgeError(f"找不到 OPV 任务：{task_id}")
        render = (
            self.repository.get_render(task.selected_render_id)
            if task.selected_render_id
            else self.repository.latest_render(task_id)
        )
        from services.workflow_v2 import workflow_v2_enabled
        if workflow_v2_enabled(task) and (
            not task.released_revision_id
            or task.active_revision_id != task.released_revision_id
            or render is None
            or render.origin_revision_id != task.released_revision_id
            or task.selected_render_id != render.render_id
            or not render.publish_ready
        ):
            raise MainScheduleBridgeError(
                f"OPV 任务 {task_id} 没有已冻结并审核放行的 Workflow V2 成片"
            )
        release_manifest = None
        if workflow_v2_enabled(task):
            from services.release_gate import freeze_release, ReleaseGateError
            try:
                release_manifest = freeze_release(self.repository, task, render)
            except (ReleaseGateError, OSError) as exc:
                raise MainScheduleBridgeError(str(exc)) from exc
            revision = self.repository.get_task_revision(task.released_revision_id)
            # Publication inputs follow the released snapshot, never mutable task fields.
            task = copy.copy(task)
            task.plan_json = copy.deepcopy(revision.plan_snapshot_json.get("plan") or {})
            task.product_snapshot_json = copy.deepcopy(revision.plan_snapshot_json.get("product_snapshot") or {})
            task.copy_json = copy.deepcopy(release_manifest["copy"])
        if render is None or not str(render.output_url or "").strip():
            raise MainScheduleBridgeError(f"OPV 任务 {task_id} 没有可排班的成片")
        video_path = Path(str(render.output_url)).expanduser()
        if not video_path.is_file():
            raise MainScheduleBridgeError(f"OPV 成片文件不存在：{video_path}")

        country = str(task.target_country or "").strip().upper()
        store_id = self._store_id(country)
        title = self._publish_title(task, country)
        source_record_id = str(feishu_record_id or task.feishu_record_id or task.source_record_id or task_id)
        canonical_key = f"opv:{task_id}"
        script_slot = self._script_slot(task_id)
        bgm_inputs = task_content_profile_inputs(task)
        context = {
            "schema_version": "opv-main-publish-v1",
            "source_product_id": self._source_product_id(task),
            "theme_id": str(task.theme_id or ""),
            "recipe_id": str(
                getattr(task, "recipe_id", "")
                or (((task.plan_json or {}).get("recipe") or {}).get("id") or "")
            ),
            "content_package_id": str(task.content_package_id or ""),
            "bgm_mood_hints": self._bgm_mood_hints(task),
            "bgm_content_template": bgm_inputs["content_template"],
            "bgm_rhythm_preference": bgm_inputs["rhythm_preference"],
            "audio_mode": "silent_source_platform_bgm",
            "video_duration_ms": int(render.duration_ms or 10_000),
            "feishu_record_id": source_record_id,
        }
        if release_manifest:
            context.update(schema_version="opv-main-publish-v2", workflow_version=2,
                           release_manifest=release_manifest, publish_title=title)
            existing = self.db.get_script_metadata(canonical_key)
            if existing:
                old = json.loads(existing["script_text"] or "{}")
                if old.get("release_manifest") != release_manifest:
                    raise MainScheduleBridgeError("任务已有不同 release 的发布记录，不能覆盖冻结队列")
        metadata = ScriptMetadata(
            canonical_script_key=canonical_key,
            script_id=task_id,
            source_record_id=source_record_id,
            script_slot=script_slot,
            task_no=task_id,
            store_id=store_id,
            product_id="",
            parent_slot="OPV",
            direction_label=str(task.theme_id or "图文养号"),
            variant_strength="成片",
            target_country=country,
            product_type=self._product_type(task),
            content_family_key=str(render.output_sha256 or task.content_package_id or task_id),
            script_text=json.dumps(context, ensure_ascii=False, sort_keys=True),
            short_video_title=title,
            title_source="opv_copy",
            script_source="图文养号",
            publish_purpose="养号",
            cart_enabled="否",
            content_branch="非商品展示型",
        )
        self.db.upsert_script_metadata([metadata])
        self.db.upsert_video_asset(
            canonical_script_key=canonical_key,
            script_id=task_id,
            run_manager_record_id=source_record_id,
            video_source_type="opv_render",
            video_source_value=str(video_path),
            local_file_path=str(video_path),
            download_status="下载成功",
            run_video_status="已完成",
            publish_status="待排期",
        )
        return {
            "task_id": task_id,
            "canonical_script_key": canonical_key,
            "store_id": store_id,
            "publish_type": "organic_nurture",
            "status": "待排期",
        }

    def get_task_state(self, task_id: str) -> Dict[str, str]:
        canonical_key = f"opv:{task_id}"
        asset = self.db.get_video_asset(canonical_key)
        if asset is None:
            return {"task_id": task_id, "status": ""}
        slot = self.db.get_active_script_assignment(canonical_key)
        bgm_title = ""
        if slot is not None:
            try:
                bgm_payload = json.loads(str(slot["bgm_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                bgm_payload = {}
            bgm_title = str(bgm_payload.get("music_title") or "")
        return {
            "task_id": task_id,
            "status": str(asset["publish_status"] or ""),
            "account_name": str(slot["account_name"] or "") if slot else str(asset["account_name"] or ""),
            "planned_publish_at": str(slot["scheduled_for"] or "") if slot else str(asset["planned_publish_at"] or ""),
            "bgm_title": bgm_title,
            "error_message": str(asset["error_message"] or ""),
        }

    def assert_rework_allowed(self, task_id: str) -> None:
        assert_main_queue_rework_allowed(task_id, db_path=self.db.db_path)
