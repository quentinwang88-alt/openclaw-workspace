"""OPV publish flow (Stage F): NeoBund Organic submission with hot BGM.

Human gate (V1 hard rule): machine never bypasses human confirmation —
``prepare_publish`` requires an operator name and moves the task only from
``video_review``.

Anti-duplicate rules (MODEL_HANDOFF 14):

- NeoBund commit responses that are empty/ambiguous NEVER trigger a second
  commit. The wrapped adapter already polls the task list by remark/title/
  scheduled time; on true ambiguity the OPV task stays in ``publishing`` with
  ``needs_requery`` recorded until ``confirm_result`` resolves it.
- Submission requires a resolved audio decision: either a captured-fields BGM
  selection or an explicit silent/no-BGM fallback. This prevents publishing a
  silent video by accident while music fields are uncaptured.

NeoBund hot-music LIST contract was captured from real traffic on 2026-08-31
(``TRENDING_SEARCH_CAPTURED = True``; see NeoBundTrendingMusicSource). The
organic COMMIT music-attach fields are still uncaptured
(``MUSIC_FIELDS_CAPTURED = False``): selecting a BGM works, but submission
with an attached BGM refuses until that second capture lands.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from domain import statuses
from domain.models import PublishRecord, generate_prefixed_id, utc_now
from domain.statuses import (
    PUBLISH_PREPARING,
    PUBLISH_PUBLISHED,
    PUBLISH_READY,
    PUBLISH_SUBMITTED,
    TASK_PUBLISH_PREPARING,
    TASK_PUBLISHING,
    TASK_PUBLISHED,
    TASK_READY_TO_PUBLISH,
    TASK_VIDEO_REVIEW,
)
from services import neobund_music

# Captured 2026-08-31 from real traffic (HAR, POST /np/shoppable/video/commit
# with a hot-list song attached): the organic commit carries
#   musicId / musicTitle / musicAuthor / musicUrl / musicCoverUrl /
#   musicSoundVolume / videoOriginalSoundVolume (0-100, frontend default 50).
# musicUrl/musicCoverUrl are the play_url/cover urls from the search response
# (signed; re-fetch the list at submit time for fresh urls).
# Also observed: isAigc spelling, postType/brandContentToggle/
# brandOrganicToggle/disableComment/disableDuet/disableStitch (all 0).
MUSIC_FIELDS_CAPTURED = True
DEFAULT_MUSIC_VOLUME = 50


class PublishFlowError(RuntimeError):
    pass


class NeoBundMusicFieldsPendingError(PublishFlowError):
    """Raised until the real NeoBund trending-music request is captured."""

    def __init__(self) -> None:
        super().__init__(
            "NeoBund organic commit 的音乐字段尚未抓包确认（选中音乐如何随任务提交："
            "音乐 ID/使用片段/原声与音乐音量）。按 MODEL_HANDOFF 红线禁止猜测字段；"
            "请在 NeoBund 页面带音乐完整创建一条定时任务时抓包 commit 请求，"
            "确认字段后翻转 MUSIC_FIELDS_CAPTURED。"
        )


# Captured 2026-08-31 from real traffic (HAR):
#   POST /np/shoppable/image/search/music
#   body: {"type": 2, "keyword": "hot", "page_size": 20,
#          "language": "th-TH", "region": "TH",
#          "page_token": "20", "search_id": "<client tracking id>"}  (pagination)
#   resp: {"music": [{"id", "title", "author", "duration"(seconds, str),
#                     "cover_thumb", "play_url"}...],
#          "next_page_token": "60", "has_more": "True", "search_id"}
TRENDING_SEARCH_CAPTURED = True
# Client base_url already ends with /np (e.g. https://www.neobund.ai/np);
# captured HAR path /np/shoppable/image/search/music minus that prefix.
TRENDING_SEARCH_PATH = "/shoppable/image/search/music"
LOCALE_BY_COUNTRY = {"TH": "th-TH", "VN": "vi-VN", "MX": "es-MX"}
DEFAULT_LOCALE = "en-US"
TRENDING_PAGE_SIZE = 20
TRENDING_MAX_PAGES = 3
# Search pools for a rhythm-strong shortlist (operator feedback 2026-08-31):
# the generic hot list plus an explicit dance pool, (keyword, max_pages) pairs.
SEARCH_POOLS = (("hot", 2), ("dance", 1))

# Title-keyword lexicon -> mood tags used by the selection policy. Titles are
# the only signal NeoBund exposes (no BPM field), so strong-beat candidates
# are recognized lexically.
MOOD_LEXICON = (
    (("dance", "dancing", "เต้น", "remix", "edm", "bass", "beat", "house", "techno", "bounce"), ("dance", "high_energy")),
    (("hot", "trend", "viral", "party", "แรง"), ("high_energy",)),
    (("bright", "happy", "fun", "pop", "sunny"), ("bright",)),
    (("lofi", "acoustic", "piano", "slow", "sad", "chill", "calm"), ("soft", "calm")),
)


class NeoBundTrendingMusicSource:
    """Fetch the account-market trending music list from NeoBund.

    ``client`` is a ``NeoBundClient``-like object exposing
    ``post(path, payload) -> dict`` (auth handled by the client, same as the
    existing short-video-auto-publisher adapter). Account scoping happens
    implicitly through the logged-in session; commit-time binding is a
    separate, still-uncaptured contract (MUSIC_FIELDS_CAPTURED).
    """

    def __init__(self, client=None):
        self._client = client

    def fetch(
        self,
        account_id: str,
        country: str,
        *,
        language: Optional[str] = None,
    ) -> List[neobund_music.BgmCandidate]:
        if self._client is None:
            raise PublishFlowError(
                "NeoBund music source has no client configured; wire a "
                "NeoBundClient (cookie/token auth) into NeoBundTrendingMusicSource"
            )
        region = str(country or "").strip().upper()
        locale = language or LOCALE_BY_COUNTRY.get(region, DEFAULT_LOCALE)
        candidates: List[neobund_music.BgmCandidate] = []
        seen_ids: set = set()
        for keyword, max_pages in SEARCH_POOLS:
            self._search_pool(
                region, locale, keyword, max_pages, candidates, seen_ids
            )
        return candidates

    def search_keyword(
        self,
        account_id: str,
        country: str,
        *,
        keyword: str,
        language: Optional[str] = None,
        pages: int = 2,
    ) -> List[neobund_music.BgmCandidate]:
        """Targeted search (e.g. by song title) using the same endpoint."""
        if self._client is None:
            raise PublishFlowError("NeoBund music source has no client configured")
        region = str(country or "").strip().upper()
        locale = language or LOCALE_BY_COUNTRY.get(region, DEFAULT_LOCALE)
        candidates: List[neobund_music.BgmCandidate] = []
        seen_ids: set = set()
        self._search_pool(
            region, locale, str(keyword or "").strip(), int(pages), candidates, seen_ids
        )
        return candidates

    def _search_pool(
        self,
        region: str,
        locale: str,
        keyword: str,
        max_pages: int,
        candidates: List[neobund_music.BgmCandidate],
        seen_ids: set,
    ) -> None:
        page_token: Optional[str] = None
        search_id = self._new_search_id()
        for _page in range(max(1, int(max_pages))):
            payload: Dict[str, Any] = {
                "type": 2,
                "keyword": keyword,
                "page_size": TRENDING_PAGE_SIZE,
                "language": locale,
                "region": region,
            }
            if page_token is not None:
                payload["page_token"] = str(page_token)
                payload["search_id"] = search_id
            response = self._client.post(TRENDING_SEARCH_PATH, payload)
            if not isinstance(response, dict):
                raise PublishFlowError("music search returned a non-object payload")
            items = response.get("music") or []
            for item in items:
                if not isinstance(item, dict):
                    continue
                music_id = str(item.get("id") or "").strip()
                title = str(item.get("title") or "").strip()
                if not music_id or not title or music_id in seen_ids:
                    continue
                seen_ids.add(music_id)
                candidates.append(
                    neobund_music.BgmCandidate(
                        music_id=music_id,
                        title=title,
                        rank=len(candidates) + 1,
                        duration_ms=self._duration_ms(item.get("duration")),
                        mood_tags=self._mood_tags(title),
                        # Signed CDN urls live only in raw (memory); they
                        # are never persisted into publish payloads.
                        raw={
                            "author": str(item.get("author") or ""),
                            "play_url": self._first_url(item.get("play_url")),
                            "cover_url": self._first_url(item.get("cover_thumb")),
                            "pool": keyword,
                        },
                    )
                )
            has_more = str(response.get("has_more", "")).strip().lower() == "true"
            next_token = str(response.get("next_page_token") or "").strip()
            if not has_more or not next_token:
                break
            page_token = next_token

    @staticmethod
    def _mood_tags(title: str) -> tuple:
        text = str(title or "").lower()
        tags: List[str] = []
        for keywords, tag_pair in MOOD_LEXICON:
            if any(keyword in text for keyword in keywords):
                for tag in tag_pair:
                    if tag not in tags:
                        tags.append(tag)
        return tuple(tags)

    @staticmethod
    def _duration_ms(value: Any) -> Optional[int]:
        try:
            seconds = float(str(value).strip())
        except (TypeError, ValueError):
            return None
        return int(seconds * 1000) if seconds > 0 else None

    @staticmethod
    def _first_url(container: Any) -> str:
        if isinstance(container, dict):
            urls = container.get("url_list") or []
            if urls:
                return str(urls[0] or "")
        return ""

    @staticmethod
    def _new_search_id() -> str:
        return utc_now().strftime("%Y%m%d%H%M%S") + uuid.uuid4().hex[:20].upper()


class OpvPublishFlow:
    def __init__(
        self,
        repository,
        adapter=None,
        music_source: Optional[NeoBundTrendingMusicSource] = None,
        clock=None,
    ):
        self._repository = repository
        self._adapter = adapter
        self._music_source = music_source or NeoBundTrendingMusicSource()
        self._clock = clock or utc_now

    # ------------------------------------------------------------------
    # Stage F-1: human-approved publish preparation
    # ------------------------------------------------------------------

    def prepare_publish(
        self,
        task_id: str,
        *,
        operator: str,
        planned_publish_at: Optional[datetime] = None,
    ) -> PublishRecord:
        if not operator or not operator.strip():
            raise PublishFlowError("operator is required (human publish gate)")
        task = self._require_task(task_id)
        if task.task_status != TASK_VIDEO_REVIEW:
            raise PublishFlowError(
                f"task {task_id} status {task.task_status!r} is not video_review"
            )
        render_id = task.selected_render_id
        render = self._repository.get_render(render_id or "")
        if render is None or not render.publish_ready:
            raise PublishFlowError(
                f"task {task_id} has no publish-ready render; run Phase 2 first"
            )
        existing = self._repository.get_publish_record_by_render(render.render_id)
        if existing is not None:
            raise PublishFlowError(
                f"render {render.render_id} already has publish record "
                f"{existing.publish_id}"
            )

        self._repository.transition_task(
            task_id, TASK_VIDEO_REVIEW, TASK_PUBLISH_PREPARING
        )
        plan = task.plan_json or {}
        caption_snapshot = {
            "title": (task.copy_json or {}).get("title", ""),
            "caption": (task.copy_json or {}).get("caption", ""),
            "hashtags": (task.copy_json or {}).get("hashtags", []),
            "cover_text": (task.copy_json or {}).get("cover_text", ""),
            "theme_id": (plan.get("theme") or {}).get("id", ""),
            "copy_status": (task.copy_json or {}).get("copy_status", ""),
        }
        record = PublishRecord(
            publish_id=generate_prefixed_id("opv_pub"),
            task_id=task_id,
            render_id=render.render_id,
            account_id=task.account_id,
            caption_snapshot_json=caption_snapshot,
            cover_shot_id=self._cover_shot_id(task_id),
            operator_name=operator,
        )
        self._repository.insert_publish_record(record)
        record = self._repository.get_publish_record(record.publish_id) or record

        self._refresh_bgm_selection(record, planned_publish_at=planned_publish_at)
        return self._repository.get_publish_record(record.publish_id) or record

    def _refresh_bgm_selection(
        self, record: PublishRecord, *, planned_publish_at: Optional[datetime]
    ) -> None:
        """Select hot BGM inside the 30-120 min pre-publish window.

        Selection failure downgrades to a recorded pending payload and never
        blocks the publish chain (MODEL_HANDOFF recovery: BGM 失败只降级).
        """
        metadata = dict(record.platform_metadata_json or {})
        task = self._require_task(record.task_id)
        country = task.target_country
        auth_id = task.account_id
        try:
            candidates = self._music_source.fetch(
                auth_id, country, language=task.target_locale
            )
            video_ms = self._render_duration(record.render_id)
            mood_hints = self._mood_hints(task)
            ranked = neobund_music.select_top(
                candidates,
                mood_hints=mood_hints,
                video_duration_ms=video_ms,
                use_counts=self._recent_use_counts(auth_id),
            )
            if not ranked:
                raise PublishFlowError(
                    "no eligible BGM candidate after dedup filtering"
                )
            audio_payload = neobund_music.build_bgm_payload(
                ranked,
                country=country,
                auth_id=auth_id,
                selected_at=self._clock().isoformat(timespec="seconds"),
            )
        except NeoBundMusicFieldsPendingError:
            audio_payload = neobund_music.pending_capture_payload(
                country=country, auth_id=auth_id
            )
        except Exception as exc:  # noqa: BLE001 - BGM degradation path
            audio_payload = neobund_music.pending_capture_payload(
                country=country, auth_id=auth_id
            )
            audio_payload["status"] = "selection_failed"
            audio_payload["fallback_reason"] = str(exc)[:300]
        metadata["audio"] = audio_payload
        metadata["planned_publish_at"] = (
            planned_publish_at.isoformat(timespec="seconds")
            if planned_publish_at
            else None
        )
        self._repository.update_publish_result(
            record.publish_id,
            publish_status=record.publish_status,
            platform_metadata_json=metadata,
        )

    # ------------------------------------------------------------------
    # Stage F-2: arm (ready_to_publish) with a resolved audio decision
    # ------------------------------------------------------------------

    def refresh_bgm_selection(self, task_id: str, *, planned_publish_at=None):
        """Re-run BGM selection on the existing record (publish-window time).

        Songs rotate out of the hot list, so the selection is refreshed inside
        the 30-120 min pre-publish window instead of being locked at planning.
        """
        task = self._require_task(task_id)
        record = self._require_record_for_task(task_id)
        self._refresh_bgm_selection(record, planned_publish_at=planned_publish_at)
        return self._repository.get_publish_record(record.publish_id) or record

    def arm_for_publish(
        self, task_id: str, *, allow_without_bgm: bool = False
    ):
        task = self._require_task(task_id)
        if task.task_status != TASK_PUBLISH_PREPARING:
            raise PublishFlowError(
                f"task {task_id} status {task.task_status!r} is not publish_preparing"
            )
        record = self._require_record_for_task(task_id)
        audio = (record.platform_metadata_json or {}).get("audio") or {}
        resolved = bool((audio.get("selected") or {}).get("music_id"))
        unresolved = audio.get("status") in (
            "pending_field_capture",
            "selection_failed",
        )
        if not resolved:
            if unresolved and not allow_without_bgm:
                raise PublishFlowError(
                    f"audio decision unresolved (status={audio.get('status')!r}): "
                    "resolve the BGM selection (capture commit fields and "
                    "re-prepare) or pass allow_without_bgm=True to publish "
                    "silent (no_bgm)"
                )
            if not unresolved and not allow_without_bgm:
                raise PublishFlowError("audio decision unresolved")
            audio["audio_strategy"] = "no_bgm"
            audio["fallback_reason"] = "operator_choice_silent"
            metadata = dict(record.platform_metadata_json or {})
            metadata["audio"] = audio
            self._repository.update_publish_result(
                record.publish_id,
                publish_status=record.publish_status,
                platform_metadata_json=metadata,
            )
        self._repository.transition_task(
            task_id, TASK_PUBLISH_PREPARING, TASK_READY_TO_PUBLISH
        )
        return self._require_task(task_id)

    # ------------------------------------------------------------------
    # Stage F-3: submit / confirm (never duplicate on ambiguity)
    # ------------------------------------------------------------------

    def submit(self, task_id: str, *, publish_at: datetime, mark_ai: Optional[bool] = None):
        if self._adapter is None:
            raise PublishFlowError("no NeoBund adapter configured")
        task = self._require_task(task_id)
        if task.task_status != TASK_READY_TO_PUBLISH:
            raise PublishFlowError(
                f"task {task_id} status {task.task_status!r} is not "
                "ready_to_publish; run prepare_publish + arm_for_publish first"
            )
        record = self._require_record_for_task(task_id)
        audio = (record.platform_metadata_json or {}).get("audio") or {}
        selected = (audio.get("selected") or {}).get("music_id")
        if not selected and audio.get("audio_strategy") != "no_bgm":
            raise PublishFlowError(
                "refusing to submit: audio decision unresolved "
                "(select a BGM via prepare_publish, or fall back to no_bgm "
                "explicitly at arm time)"
            )

        music_selection = None
        if selected:
            if not MUSIC_FIELDS_CAPTURED:  # pragma: no cover - capture gate
                raise PublishFlowError(
                    "selected BGM exists but the organic commit music fields "
                    "are not captured"
                )
            music_selection = self._fresh_music_selection(
                task, str(selected), audio
            )

        render = self._repository.get_render(record.render_id)
        if render is None or not render.output_url:
            raise PublishFlowError("render output missing; cannot submit")
        if self._repository.get_publish_record_by_render(render.render_id) is None:
            raise PublishFlowError("publish record missing")

        self._repository.transition_task(
            task_id, TASK_READY_TO_PUBLISH, TASK_PUBLISHING
        )
        self._guard_record_transition(record, PUBLISH_READY)
        self._transition_record(record.publish_id, PUBLISH_READY, PUBLISH_PREPARING)
        self._transition_record(record.publish_id, PUBLISH_PREPARING, PUBLISH_SUBMITTED)

        # NeoBund videoTitle is the TikTok post caption: send the full
        # caption (title + one-liner + hashtags) composed by copy_writer.
        copy_block = task.copy_json or {}
        video_title = (
            str(copy_block.get("caption") or copy_block.get("title") or "").strip()
            or task.task_id
        )

        try:
            external_task_id = self._adapter.create_scheduled_task(
                account_id=task.account_id,
                video_path=render.output_url,
                title=video_title,
                publish_at=publish_at,
                script_id=task.task_id,
                mark_ai=mark_ai,
                music_selection=music_selection,
            )
        except Exception as exc:  # noqa: BLE001 - adapter boundary
            self._repository.transition_task(
                task_id,
                TASK_PUBLISHING,
                statuses.TASK_FAILED,
                failure_code="neobund_submit_failed",
                failure_detail=str(exc)[:500],
                increment_retry=True,
            )
            self._repository.update_publish_result(
                self._record_id_for_task(task_id),
                publish_status=statuses.PUBLISH_FAILED,
                failure_detail=str(exc)[:500],
            )
            raise

        metadata = self._metadata_for_task(task_id)
        metadata["neobund_task_id"] = external_task_id
        metadata["submitted_at"] = self._clock().isoformat(timespec="seconds")
        metadata["needs_requery"] = not external_task_id
        metadata["scheduled_release_time"] = publish_at.strftime("%Y-%m-%d %H:%M:%S")
        if music_selection:
            metadata["music_attached"] = {
                "music_id": music_selection["music_id"],
                "music_title": music_selection["music_title"],
            }
        self._repository.update_publish_result(
            self._record_id_for_task(task_id),
            publish_status=PUBLISH_SUBMITTED,
            external_post_id=external_task_id or None,
            platform_metadata_json=metadata,
        )
        return external_task_id

    def _merge_actual_music(self, task, metadata: Dict[str, Any]) -> None:
        """Best-effort actual-music回读 (opv-bgm-v1 requires plan vs actual).

        The NeoBund task list record is the source of truth for what was
        really attached after commit; failures never block state resolution.
        """
        finder = getattr(self._adapter, "find_organic_task_record", None)
        if not callable(finder):
            return
        try:
            item = finder(
                script_id=task.task_id,
                video_title=(task.copy_json or {}).get("title", ""),
                scheduled_for=str(metadata.get("scheduled_release_time") or ""),
            ) or {}
        except Exception:  # noqa: BLE001 - requery is best-effort
            return
        music_id = str(item.get("musicId") or "").strip()
        if not music_id:
            return
        audio = dict(metadata.get("audio") or {})
        audio["actual"] = {
            "music_id": music_id,
            "title": str(item.get("musicTitle") or ""),
            "author": str(item.get("musicAuthor") or ""),
            "confirmed_at": self._clock().isoformat(timespec="seconds"),
        }
        metadata["audio"] = audio

    def _fresh_music_selection(self, task, selected_music_id: str, audio: Dict[str, Any]) -> Dict[str, Any]:
        """Build the commit music selection from the persisted selection.

        The signed play_url captured at selection time stays valid ~1 day and
        submit happens within the 30-120 min window, so we reuse it directly.
        Only when the url is missing (legacy record) do we fall back to
        re-finding the track via pools / title search.
        """
        selected_block = audio.get("selected") or {}
        music_url = str(selected_block.get("music_url") or "").strip()
        if music_url:
            return {
                "music_id": selected_music_id,
                "music_title": str(selected_block.get("title") or ""),
                "music_author": str(selected_block.get("author") or ""),
                "music_url": music_url,
                "music_cover_url": str(selected_block.get("music_cover_url") or ""),
                "music_sound_volume": int(
                    audio.get("music_sound_volume", DEFAULT_MUSIC_VOLUME)
                ),
                "video_original_sound_volume": int(
                    audio.get("video_original_sound_volume", DEFAULT_MUSIC_VOLUME)
                ),
            }

        # Legacy path: no persisted url; try pools, then a title search.
        refind_errors: List[str] = []
        match = None
        try:
            fresh = self._music_source.fetch(
                task.account_id, task.target_country, language=task.target_locale
            )
            match = next(
                (c for c in fresh if c.music_id == selected_music_id), None
            )
        except Exception as exc:  # noqa: BLE001 - refind is best-effort
            refind_errors.append(str(exc)[:200])
        if match is None:
            selected_title = str(selected_block.get("title") or "").strip()
            if selected_title:
                try:
                    by_title = self._music_source.search_keyword(
                        task.account_id,
                        task.target_country,
                        keyword=selected_title,
                        language=task.target_locale,
                    )
                    match = next(
                        (c for c in by_title if c.music_id == selected_music_id), None
                    )
                except Exception as exc:  # noqa: BLE001
                    refind_errors.append(str(exc)[:200])
        if match is None:
            detail = "; ".join(refind_errors) or "track not in current pools"
            raise PublishFlowError(
                f"selected BGM {selected_music_id} has no persisted url and "
                f"cannot be re-found ({detail}); re-run prepare_publish to "
                "pick a fresh song or fall back to no_bgm"
            )
        return {
            "music_id": match.music_id,
            "music_title": match.title,
            "music_author": str(match.raw.get("author") or ""),
            "music_url": str(match.raw.get("play_url") or ""),
            "music_cover_url": str(match.raw.get("cover_url") or ""),
            "music_sound_volume": int(
                audio.get("music_sound_volume", DEFAULT_MUSIC_VOLUME)
            ),
            "video_original_sound_volume": int(
                audio.get("video_original_sound_volume", DEFAULT_MUSIC_VOLUME)
            ),
        }

    def confirm_result(self, task_id: str, *, published_at: Optional[datetime] = None):
        """Re-query NeoBund for the submitted task; resolve ambiguity only here."""
        if self._adapter is None:
            raise PublishFlowError("no NeoBund adapter configured")
        task = self._require_task(task_id)
        if task.task_status != TASK_PUBLISHING:
            raise PublishFlowError(
                f"task {task_id} status {task.task_status!r} is not publishing"
            )
        record = self._require_record_for_task(task_id)
        metadata = dict(record.platform_metadata_json or {})
        external_task_id = str(metadata.get("neobund_task_id") or "")
        self._merge_actual_music(task, metadata)
        # submit stores scheduled_release_time as a string; the adapter needs
        # a datetime (used for matching and as the published_at fallback).
        scheduled_for = None
        scheduled_raw = str(
            metadata.get("scheduled_release_time")
            or metadata.get("planned_publish_at")
            or ""
        ).strip()
        if scheduled_raw:
            try:
                scheduled_for = datetime.strptime(scheduled_raw[:19], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                scheduled_for = None
        status = self._adapter.query_task_status(
            task_id=external_task_id,
            scheduled_for=scheduled_for,
        )
        state = str(getattr(status, "state", status) or "").strip().lower()
        # prefer the platform-reported publish time over "now"
        platform_published_at = getattr(status, "published_at", None)
        moment = published_at or platform_published_at or self._clock()
        if state in ("published", "success", "done"):
            self._transition_record(
                record.publish_id,
                PUBLISH_SUBMITTED,
                PUBLISH_PUBLISHED,
                published_at=moment,
                platform_metadata_json=metadata,
            )
            self._repository.transition_task(
                task_id, TASK_PUBLISHING, TASK_PUBLISHED
            )
            return "published"
        if state in ("failed", "error"):
            self._transition_record(
                record.publish_id,
                PUBLISH_SUBMITTED,
                statuses.PUBLISH_FAILED,
                failure_detail=str(getattr(status, "detail", "") or "")[:500],
            )
            self._repository.transition_task(
                task_id,
                TASK_PUBLISHING,
                statuses.TASK_FAILED,
                failure_code="neobund_publish_failed",
                failure_detail=str(getattr(status, "detail", "") or "")[:500],
            )
            return "failed"
        # still ambiguous: stay in publishing, never resubmit blindly
        metadata["needs_requery"] = True
        metadata["last_checked_at"] = self._clock().isoformat(timespec="seconds")
        self._repository.update_publish_result(
            record.publish_id,
            publish_status=PUBLISH_SUBMITTED,
            platform_metadata_json=metadata,
        )
        return "pending"

    # ------------------------------------------------------------------

    def _recent_use_counts(self, account_id: str) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        cutoff = self._clock() - timedelta(days=neobund_music.DEDUP_WINDOW_DAYS)
        for record in self._repository.list_publish_records_by_account(account_id):
            audio = (record.platform_metadata_json or {}).get("audio") or {}
            selected = (audio.get("selected") or {}).get("music_id") or ""
            chosen_at = ((audio.get("selected") or {}).get("selected_at")) or ""
            if not selected or not chosen_at:
                continue
            try:
                moment = datetime.fromisoformat(chosen_at)
            except ValueError:
                continue
            if moment >= cutoff:
                counts[selected] = counts.get(selected, 0) + 1
        return counts

    def _mood_hints(self, task) -> List[str]:
        theme_id = ((task.plan_json or {}).get("theme") or {}).get("id")
        theme = self._repository.get_theme(theme_id) if theme_id else None
        if theme is None:
            return []
        return list(theme.content_plan_rules_json.get("bgm_mood_hints", []))

    def _render_duration(self, render_id: str) -> int:
        render = self._repository.get_render(render_id)
        if render is None or not render.duration_ms:
            return 12500  # preset default
        return int(render.duration_ms)

    def _cover_shot_id(self, task_id: str) -> Optional[str]:
        shots = self._repository.list_shots(task_id)
        selected = [s for s in shots if s.is_selected]
        return selected[0].shot_id if selected else None

    def _require_task(self, task_id: str):
        task = self._repository.get_task(task_id)
        if task is None:
            raise PublishFlowError(f"task {task_id} not found")
        return task

    def _require_record_for_task(self, task_id: str) -> PublishRecord:
        task = self._require_task(task_id)
        render = self._repository.get_render(task.selected_render_id or "")
        record = (
            self._repository.get_publish_record_by_render(render.render_id)
            if render
            else None
        )
        if record is None:
            raise PublishFlowError(f"task {task_id} has no publish record")
        return record

    def _record_id_for_task(self, task_id: str) -> str:
        return self._require_record_for_task(task_id).publish_id

    def _metadata_for_task(self, task_id: str) -> Dict[str, Any]:
        return dict(self._require_record_for_task(task_id).platform_metadata_json or {})

    @staticmethod
    def _guard_record_transition(record: PublishRecord, expected: str) -> None:
        if record.publish_status != expected:
            raise PublishFlowError(
                f"publish record {record.publish_id} status "
                f"{record.publish_status!r} != {expected!r}"
            )

    def _transition_record(
        self, publish_id: str, from_status: str, to_status: str, **extra_fields
    ) -> None:
        statuses.ensure_transition(statuses.PUBLISH_STATUS_TRANSITIONS, from_status, to_status)
        current = self._repository.get_publish_record(publish_id)
        if current is None or current.publish_status != from_status:
            actual = current.publish_status if current else "<missing>"
            raise PublishFlowError(
                f"publish record {publish_id} expected {from_status!r}, is {actual!r}"
            )
        self._repository.update_publish_result(
            publish_id, publish_status=to_status, **extra_fields
        )
