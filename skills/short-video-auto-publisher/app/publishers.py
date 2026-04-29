#!/usr/bin/env python3
"""自动发布适配层。"""

from __future__ import annotations

import hashlib
import json
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl, urlparse, urlunparse

import requests

from app.models import PublishTaskStatus


class BasePublishAdapter(ABC):
    @abstractmethod
    def create_scheduled_task(
        self,
        *,
        account_id: str,
        video_path: str,
        title: str,
        publish_at: datetime,
        script_id: str,
        product_id: str = "",
        product_title: str = "",
        ref_video_id: str = "",
    ) -> str:
        raise NotImplementedError

    @abstractmethod
    def query_task_status(self, *, task_id: str, scheduled_for: datetime) -> PublishTaskStatus:
        raise NotImplementedError


class DryRunPublishAdapter(BasePublishAdapter):
    def create_scheduled_task(
        self,
        *,
        account_id: str,
        video_path: str,
        title: str,
        publish_at: datetime,
        script_id: str,
        product_id: str = "",
        product_title: str = "",
        ref_video_id: str = "",
    ) -> str:
        digest = hashlib.md5(f"{account_id}:{script_id}:{publish_at.isoformat()}".encode("utf-8")).hexdigest()[:10]
        return f"dryrun-{digest}"

    def query_task_status(self, *, task_id: str, scheduled_for: datetime) -> PublishTaskStatus:
        if datetime.now() >= scheduled_for:
            return PublishTaskStatus(state="success", result="发布成功", published_at=scheduled_for.strftime("%Y-%m-%d %H:%M:%S"))
        return PublishTaskStatus(state="pending", result="待执行")


class HttpPublishAdapter(BasePublishAdapter):
    def __init__(self, base_url: str, token: str = ""):
        self.base_url = str(base_url or "").rstrip("/")
        self.token = token

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def create_scheduled_task(
        self,
        *,
        account_id: str,
        video_path: str,
        title: str,
        publish_at: datetime,
        script_id: str,
        product_id: str = "",
        product_title: str = "",
        ref_video_id: str = "",
    ) -> str:
        response = requests.post(
            f"{self.base_url}/scheduled-tasks",
            headers=self._headers(),
            json={
                "account_id": account_id,
                "video_path": video_path,
                "title": title,
                "publish_at": publish_at.isoformat(),
                "script_id": script_id,
                "product_id": product_id,
                "product_title": product_title,
                "ref_video_id": ref_video_id,
            },
            timeout=60,
        )
        response.raise_for_status()
        payload: Dict[str, Any] = response.json()
        task_id = str(payload.get("task_id") or payload.get("id") or "").strip()
        if not task_id:
            raise RuntimeError(f"发布 API 未返回 task_id: {payload}")
        return task_id

    def query_task_status(self, *, task_id: str, scheduled_for: datetime) -> PublishTaskStatus:
        response = requests.get(
            f"{self.base_url}/scheduled-tasks/{task_id}",
            headers=self._headers(),
            timeout=60,
        )
        response.raise_for_status()
        payload: Dict[str, Any] = response.json()
        state = str(payload.get("state") or payload.get("status") or "pending").strip().lower()
        if state in {"success", "published", "done"}:
            return PublishTaskStatus(
                state="success",
                result="发布成功",
                published_at=str(payload.get("published_at") or ""),
            )
        if state in {"failed", "error"}:
            return PublishTaskStatus(
                state="failed",
                result="发布失败",
                error_message=str(payload.get("error_message") or payload.get("message") or ""),
            )
        return PublishTaskStatus(state="pending", result="待执行")


def _deep_get(payload: Any, dotted_path: str) -> Any:
    current = payload
    for part in str(dotted_path or "").split("."):
        if not part:
            continue
        if isinstance(current, list):
            if not part.isdigit():
                return None
            index = int(part)
            if index < 0 or index >= len(current):
                return None
            current = current[index]
            continue
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


class GeeLarkPublishAdapter(BasePublishAdapter):
    def __init__(
        self,
        *,
        token: str,
        endpoint: str = "https://openapi.geelark.cn/open/v1/task/add",
        upload_endpoint: str = "https://openapi.geelark.cn/open/v1/upload/getUrl",
        auth_header: str = "Authorization",
        auth_scheme: str = "Bearer",
        plan_name_field: str = "planName",
        remark_field: str = "remark",
        task_type_field: str = "taskType",
        list_field: str = "list",
        task_type_value: int = 1,
        env_id_field: str = "envId",
        video_field: str = "video",
        schedule_at_field: str = "scheduleAt",
        video_desc_field: str = "videoDesc",
        product_id_field: str = "productId",
        product_title_field: str = "productTitle",
        ref_video_id_field: str = "refVideoId",
        upload_file_type_field: str = "fileType",
        status_endpoint: str = "",
        status_method: str = "GET",
        status_task_id_field: str = "ids",
        task_id_paths: Optional[str] = None,
        upload_url_paths: Optional[str] = None,
        resource_url_paths: Optional[str] = None,
        status_value_paths: Optional[str] = None,
        success_values: Optional[str] = None,
        failure_values: Optional[str] = None,
        published_at_paths: Optional[str] = None,
        error_message_paths: Optional[str] = None,
        extra_body_json: str = "",
        request_max_retries: int = 2,
        request_retry_backoff_seconds: int = 1,
    ):
        self.token = str(token or "").strip()
        self.endpoint = str(endpoint or "").strip()
        self.upload_endpoint = str(upload_endpoint or "").strip()
        self.auth_header = str(auth_header or "Authorization").strip()
        self.auth_scheme = str(auth_scheme or "Bearer").strip()
        self.plan_name_field = str(plan_name_field or "planName").strip()
        self.remark_field = str(remark_field or "remark").strip()
        self.task_type_field = str(task_type_field or "taskType").strip()
        self.list_field = str(list_field or "list").strip()
        self.task_type_value = int(task_type_value)
        self.env_id_field = str(env_id_field or "envId").strip()
        self.video_field = str(video_field or "video").strip()
        self.schedule_at_field = str(schedule_at_field or "scheduleAt").strip()
        self.video_desc_field = str(video_desc_field or "videoDesc").strip()
        self.product_id_field = str(product_id_field or "productId").strip()
        self.product_title_field = str(product_title_field or "productTitle").strip()
        self.ref_video_id_field = str(ref_video_id_field or "refVideoId").strip()
        self.upload_file_type_field = str(upload_file_type_field or "fileType").strip()
        self.status_endpoint = str(status_endpoint or "").strip()
        self.status_method = str(status_method or "GET").strip().upper()
        self.status_task_id_field = str(status_task_id_field or "task_id").strip()
        self.task_id_paths = [
            item.strip()
            for item in str(task_id_paths or "data.taskIds.0,taskIds.0,task_id,id,data.task_id,data.id").split(",")
            if item.strip()
        ]
        self.upload_url_paths = [item.strip() for item in str(upload_url_paths or "data.uploadUrl,uploadUrl").split(",") if item.strip()]
        self.resource_url_paths = [item.strip() for item in str(resource_url_paths or "data.resourceUrl,resourceUrl").split(",") if item.strip()]
        self.status_value_paths = [item.strip() for item in str(status_value_paths or "data.items.0.status,items.0.status,data.status,status").split(",") if item.strip()]
        self.success_values = {item.strip().lower() for item in str(success_values or "success,published,done,3").split(",") if item.strip()}
        self.failure_values = {item.strip().lower() for item in str(failure_values or "failed,error,-1,4,5,7").split(",") if item.strip()}
        self.published_at_paths = [item.strip() for item in str(published_at_paths or "").split(",") if item.strip()]
        self.error_message_paths = [item.strip() for item in str(error_message_paths or "data.items.0.failDesc,items.0.failDesc,data.failDesc,failDesc,message,error_message,data.message,data.error_message").split(",") if item.strip()]
        self.extra_body = self._parse_extra_body(extra_body_json)
        self.request_max_retries = max(0, int(request_max_retries))
        self.request_retry_backoff_seconds = max(1, int(request_retry_backoff_seconds))

    @staticmethod
    def _parse_extra_body(extra_body_json: str) -> Dict[str, Any]:
        text = str(extra_body_json or "").strip()
        if not text:
            return {}
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError("GeeLark extra body 必须是 JSON 对象")
        return data

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.token:
            if self.auth_scheme:
                headers[self.auth_header] = f"{self.auth_scheme} {self.token}"
            else:
                headers[self.auth_header] = self.token
        return headers

    @staticmethod
    def _guess_file_type(file_path: str) -> str:
        suffix = Path(file_path).suffix.lower().lstrip(".")
        if suffix == "jpeg":
            return "jpg"
        if not suffix:
            raise RuntimeError(f"无法从文件名推断 GeeLark fileType: {file_path}")
        return suffix

    def _extract_by_paths(self, payload: Dict[str, Any], paths: list[str], label: str) -> str:
        for path in paths:
            value = _deep_get(payload, path)
            if value not in (None, ""):
                return str(value).strip()
        raise RuntimeError(f"GeeLark {label} 缺失: {payload}")

    def _request_with_retry(self, method: str, url: str, *, use_env_proxy: bool = True, **kwargs):
        last_error: Optional[Exception] = None
        for attempt in range(self.request_max_retries + 1):
            try:
                if use_env_proxy:
                    return requests.request(method, url, **kwargs)
                session = requests.Session()
                session.trust_env = False
                try:
                    return session.request(method, url, **kwargs)
                finally:
                    session.close()
            except requests.exceptions.SSLError as exc:
                last_error = exc
                if attempt >= self.request_max_retries:
                    raise
                time.sleep(self.request_retry_backoff_seconds * (attempt + 1))
            except requests.exceptions.RequestException as exc:
                last_error = exc
                if attempt >= self.request_max_retries:
                    raise
                time.sleep(self.request_retry_backoff_seconds * (attempt + 1))
        if last_error is not None:
            raise last_error
        raise RuntimeError("GeeLark request failed without explicit exception")

    def _raise_for_status_with_body(self, response: requests.Response, context: str) -> None:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            body = ""
            try:
                body = str(response.text or "").strip()
            except Exception:
                body = ""
            if body:
                body = body[:1000]
                raise requests.HTTPError(f"{context}: {exc}; response_body={body}", response=response) from exc
            raise requests.HTTPError(f"{context}: {exc}", response=response) from exc

    @staticmethod
    def _extract_upload_headers(payload: Dict[str, Any], *, file_size: int) -> Dict[str, str]:
        upload_headers: Dict[str, str] = {}
        for path in ("data.headers", "headers", "data.uploadHeaders", "uploadHeaders", "data.putHeaders", "putHeaders"):
            value = _deep_get(payload, path)
            if isinstance(value, dict):
                for key, header_value in value.items():
                    key_text = str(key or "").strip()
                    if not key_text or header_value in (None, ""):
                        continue
                    upload_headers[key_text] = str(header_value)
        upload_headers["Content-Length"] = str(file_size)
        return upload_headers

    @staticmethod
    def _guess_content_type(file_path: str) -> str:
        suffix = Path(file_path).suffix.lower()
        mapping = {
            ".mp4": "video/mp4",
            ".mov": "video/quicktime",
            ".m4v": "video/x-m4v",
            ".avi": "video/x-msvideo",
            ".webm": "video/webm",
        }
        return mapping.get(suffix, "application/octet-stream")

    @staticmethod
    def _is_presigned_upload_url(url: str) -> bool:
        parsed = urlparse(str(url or "").strip())
        query_keys = {key.lower() for key, _ in parse_qsl(parsed.query, keep_blank_values=True)}
        signature_markers = {"signature", "ossaccesskeyid", "x-amz-signature", "x-amz-algorithm", "x-oss-signature-version"}
        return bool(query_keys & signature_markers)

    @staticmethod
    def _normalize_upload_url(url: str) -> str:
        parsed = urlparse(str(url or "").strip())
        normalized_path = parsed.path.replace("%2F", "/").replace("%2f", "/")
        if normalized_path == parsed.path:
            return str(url or "").strip()
        return urlunparse(parsed._replace(path=normalized_path))

    def _upload_local_file(self, file_path: str) -> str:
        resolved_path = Path(file_path)
        file_type = self._guess_file_type(file_path)
        upload_meta_response = self._request_with_retry(
            "POST",
            self.upload_endpoint,
            headers=self._headers(),
            json={self.upload_file_type_field: file_type},
            timeout=60,
        )
        self._raise_for_status_with_body(upload_meta_response, "GeeLark 获取上传地址失败")
        upload_meta_payload: Dict[str, Any] = upload_meta_response.json()
        raw_upload_url = self._extract_by_paths(upload_meta_payload, self.upload_url_paths, "uploadUrl")
        upload_url = self._normalize_upload_url(raw_upload_url)
        resource_url = self._extract_by_paths(upload_meta_payload, self.resource_url_paths, "resourceUrl")
        upload_headers = self._extract_upload_headers(upload_meta_payload, file_size=resolved_path.stat().st_size)
        upload_payload = resolved_path.read_bytes()

        is_presigned = self._is_presigned_upload_url(upload_url)
        upload_uses_env_proxy = not is_presigned and not str(upload_url or "").strip().lower().startswith("http://")
        headers_with_default_type = dict(upload_headers)
        added_default_content_type = not any(key.lower() == "content-type" for key in headers_with_default_type)
        if added_default_content_type:
            headers_with_default_type["Content-Type"] = self._guess_content_type(file_path)

        attempt_headers = [headers_with_default_type]
        if added_default_content_type:
            attempt_headers.append(dict(upload_headers))

        attempt_urls = [upload_url]
        if is_presigned and upload_url.lower().startswith("http://"):
            attempt_urls.append("https://" + upload_url[len("http://") :])

        last_error: Optional[Exception] = None
        seen_attempts: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
        for attempt_url in attempt_urls:
            for headers in attempt_headers:
                attempt_key = (
                    attempt_url,
                    tuple(sorted((str(key), str(value)) for key, value in headers.items())),
                )
                if attempt_key in seen_attempts:
                    continue
                seen_attempts.add(attempt_key)
                try:
                    upload_response = self._request_with_retry(
                        "PUT",
                        attempt_url,
                        data=upload_payload,
                        headers=headers,
                        timeout=300,
                        use_env_proxy=upload_uses_env_proxy,
                    )
                    self._raise_for_status_with_body(upload_response, f"GeeLark 上传视频失败: {resolved_path.name}")
                    return resource_url
                except requests.HTTPError as exc:
                    last_error = exc
                    error_text = str(exc)
                    if "SignatureDoesNotMatch" not in error_text and "Not Implemented" not in error_text:
                        raise
                except requests.RequestException as exc:
                    last_error = exc
        if last_error is not None:
            raise last_error
        return resource_url

    def create_scheduled_task(
        self,
        *,
        account_id: str,
        video_path: str,
        title: str,
        publish_at: datetime,
        script_id: str,
        product_id: str = "",
        product_title: str = "",
        ref_video_id: str = "",
    ) -> str:
        resolved_video_path = str(video_path or "").strip()
        if resolved_video_path.startswith(("http://", "https://")):
            video_url = resolved_video_path
        elif Path(resolved_video_path).exists():
            video_url = self._upload_local_file(resolved_video_path)
        else:
            raise RuntimeError(f"GeeLark 视频路径不可用，既不是 URL 也不是本地文件: {video_path}")
        top_level = dict(self.extra_body.get("top_level", {})) if isinstance(self.extra_body.get("top_level"), dict) else {}
        item_level = dict(self.extra_body.get("item", {})) if isinstance(self.extra_body.get("item"), dict) else {}
        item_payload: Dict[str, Any] = {
            self.schedule_at_field: int(publish_at.timestamp()),
            self.env_id_field: account_id,
            self.video_field: video_url,
            self.video_desc_field: title,
        }
        if str(product_id or "").strip():
            item_payload[self.product_id_field] = str(product_id).strip()
        if str(product_title or "").strip():
            item_payload[self.product_title_field] = str(product_title).strip()
        if str(ref_video_id or "").strip():
            item_payload[self.ref_video_id_field] = str(ref_video_id).strip()
        item_payload.update(item_level)
        payload: Dict[str, Any] = {
            self.plan_name_field: title[:100] if title else f"publish-{script_id}",
            self.remark_field: script_id[:200],
            self.task_type_field: self.task_type_value,
            self.list_field: [item_payload],
        }
        payload.update(top_level)
        response = self._request_with_retry(
            "POST",
            self.endpoint,
            headers=self._headers(),
            json=payload,
            timeout=60,
        )
        response.raise_for_status()
        result: Dict[str, Any] = response.json()
        for path in self.task_id_paths:
            value = _deep_get(result, path)
            if value not in (None, ""):
                return str(value).strip()
        raise RuntimeError(f"GeeLark task-add 未返回任务 ID: {result}")

    def query_task_status(self, *, task_id: str, scheduled_for: datetime) -> PublishTaskStatus:
        if not self.status_endpoint:
            return PublishTaskStatus(state="pending", result="待执行")

        if self.status_method == "POST":
            json_payload: Dict[str, Any]
            if self.status_task_id_field == "ids":
                json_payload = {self.status_task_id_field: [task_id]}
            else:
                json_payload = {self.status_task_id_field: task_id}
            response = self._request_with_retry(
                "POST",
                self.status_endpoint,
                headers=self._headers(),
                json=json_payload,
                timeout=60,
            )
        else:
            response = self._request_with_retry(
                "GET",
                self.status_endpoint,
                headers=self._headers(),
                params={self.status_task_id_field: task_id},
                timeout=60,
            )
        response.raise_for_status()
        result: Dict[str, Any] = response.json()

        state = ""
        for path in self.status_value_paths:
            value = _deep_get(result, path)
            if value not in (None, ""):
                state = str(value).strip().lower()
                break
        published_at = ""
        for path in self.published_at_paths:
            value = _deep_get(result, path)
            if value not in (None, ""):
                published_at = str(value).strip()
                break
        error_message = ""
        for path in self.error_message_paths:
            value = _deep_get(result, path)
            if value not in (None, ""):
                error_message = str(value).strip()
                break

        if state in self.success_values:
            return PublishTaskStatus(state="success", result="发布成功", published_at=published_at)
        if state in self.failure_values:
            return PublishTaskStatus(state="failed", result="发布失败", error_message=error_message)
        return PublishTaskStatus(state="pending", result="待执行")
