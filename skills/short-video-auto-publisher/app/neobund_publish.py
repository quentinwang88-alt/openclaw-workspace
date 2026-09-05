#!/usr/bin/env python3
"""NeoBund TikTok Publish adapter."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import hmac
import json
import mimetypes
from pathlib import Path
import secrets
import shutil
import string
import subprocess
import time
import unicodedata
from typing import Any, Callable, Dict, Iterable, Optional
from urllib.parse import quote, urlparse
import uuid

import requests

from app.models import PublishTaskStatus
from app.publishers import BasePublishAdapter, _deep_get
from app.neobund_auth import TRUSTED_HOSTS, NeoBundAuthError, persist_validated_credentials


NEOBUND_TASK_PREFIX = "neobund:"


class AmbiguousPublishError(RuntimeError):
    """Commit may have reached the platform; never release/retry automatically."""

    submission_ambiguous = True


class PreCommitUploadError(ValueError):
    """Asset upload failed before any publishing commit could be called."""

    submission_not_sent = True

    def __init__(self, exc: Exception):
        status = getattr(getattr(exc, "response", None), "status_code", None)
        self.retryable = isinstance(exc, (TimeoutError, ConnectionError, requests.Timeout,
                                         requests.ConnectionError)) or status in {408, 429, 500, 502, 503, 504}
        # Upload exceptions may embed signed URLs/temporary credentials.
        super().__init__(f"NeoBund 上传阶段失败（{type(exc).__name__}"
                         + (f", HTTP {status}" if isinstance(status, int) else "")
                         + "）；发布提交尚未调用，未发送发布请求")


def _first_value(payload: Any, paths: Iterable[str], default: Any = None) -> Any:
    for path in paths:
        value = _deep_get(payload, path)
        if value not in (None, ""):
            return value
    return default


def _maybe_int(value: str) -> int | str:
    text = str(value or "").strip()
    if text.isdigit():
        try:
            return int(text)
        except ValueError:
            return text
    return text


def _sanitize_product_link_title(value: str, *, max_length: int = 30) -> str:
    chars = []
    previous_space = True
    for char in str(value or "").strip():
        category = unicodedata.category(char)
        if char.isalnum() or (category.startswith("M") and chars):
            chars.append(char)
            previous_space = False
        elif char.isspace() and not previous_space:
            chars.append(" ")
            previous_space = True
        elif not previous_space:
            chars.append(" ")
            previous_space = True
    sanitized = " ".join("".join(chars).split())
    return sanitized[:max_length].strip()


@dataclass(frozen=True)
class NeoBundUploadResult:
    file_id: str
    key: str
    bucket_name: str
    url: str = ""


class NeoBundClient:
    def __init__(
        self,
        *,
        base_url: str = "https://www.neobund.ai/np",
        access_token: str = "",
        cookie: str = "",
        country_code: str = "CN",
        language: str = "en",
        shoppable_commit_path: str = "/shoppable/video/commit",
        shoppable_list_path: str = "/shoppable/video/list",
        organic_commit_path: str = "/shoppable/video/commit",
        organic_list_path: str = "/shoppable/video/list",
        timeout: int = 300,
        session: Optional[requests.Session] = None,
        auth_refresh_provider: Optional[Callable[[], Dict[str, str]]] = None,
        auth_config_path: str = "",
    ):
        self.base_url = str(base_url or "https://www.neobund.ai/np").rstrip("/")
        self.access_token = str(access_token or "").strip()
        self.cookie = str(cookie or "").strip()
        self.country_code = str(country_code or "CN").strip() or "CN"
        self.language = str(language or "en").strip() or "en"
        self.shoppable_commit_path = str(shoppable_commit_path or "/shoppable/video/commit").strip()
        self.shoppable_list_path = str(shoppable_list_path or "/shoppable/video/list").strip()
        self.organic_commit_path = str(organic_commit_path or "/shoppable/video/commit").strip()
        self.organic_list_path = str(organic_list_path or "/shoppable/video/list").strip()
        self.timeout = max(1, int(timeout or 300))
        self.session = session or requests.Session()
        self.auth_refresh_provider = auth_refresh_provider
        self.auth_config_path = str(auth_config_path or "")
        self._auth_refresh_attempted = False
        self.auth_failed = False
        self.auth_persistence_error = ""

    def _url(self, path: str) -> str:
        text = str(path or "").strip()
        if text.startswith(("http://", "https://")):
            return text
        return f"{self.base_url}/{text.lstrip('/')}"

    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": self.language,
            "Content-Type": "application/json",
            "countryCode": self.country_code,
        }
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        if self.cookie:
            headers["Cookie"] = self.cookie
        if extra:
            headers.update(extra)
        return headers

    @staticmethod
    def _raise_for_status_with_body(response: requests.Response, context: str) -> None:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            body = ""
            try:
                body = str(response.text or "").strip()
            except Exception:
                body = ""
            if body:
                raise requests.HTTPError(f"{context}: {exc}; response_body={body[:1000]}", response=response) from exc
            raise requests.HTTPError(f"{context}: {exc}", response=response) from exc

    @staticmethod
    def _auth_rejected(response: requests.Response) -> bool:
        if response.status_code == 401:
            return True
        try:
            payload = response.json()
        except (ValueError, TypeError):
            return False
        if not isinstance(payload, dict):
            return False
        # Only explicit auth rejection permits replaying a POST. Timeouts/5xx do not.
        code = str(payload.get("code", payload.get("status", "")))
        return code == "401"

    def _request_once(self, method: str, path: str, *, params=None, json_body=None):
        return self.session.request(
            method.upper(), self._url(path), headers=self._headers(), params=params,
            json=json_body, timeout=self.timeout, allow_redirects=False,
        )

    def _refresh_auth_once(self) -> None:
        if self._auth_refresh_attempted or self.auth_refresh_provider is None:
            self.auth_failed = True
            raise NeoBundAuthError("NeoBund 登录失效，本轮已暂停，请在专用窗口重新登录")
        self._auth_refresh_attempted = True
        previous = (self.access_token, self.cookie)
        try:
            endpoint = urlparse(self.base_url)
            if endpoint.scheme != "https" or endpoint.hostname not in TRUSTED_HOSTS:
                raise ValueError("untrusted refresh endpoint")
            credentials = self.auth_refresh_provider()
            self.access_token = str(credentials.get("access_token") or "").strip()
            self.cookie = str(credentials.get("cookie") or "").strip()
            if not self.access_token and not self.cookie:
                raise ValueError("empty credentials")
            # Use a non-recursive read-only probe before either persistence or replay.
            response = self._request_once("GET", "/tk/auth/list", params={"current": 1, "size": 1, "queryScope": 1})
            if self._auth_rejected(response):
                raise ValueError("auth rejected")
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or not payload:
                raise ValueError("invalid auth probe")
            code = str(payload.get("code", "200"))
            if code not in {"0", "200", "00000"} or payload.get("success") is False:
                raise ValueError("auth probe unsuccessful")
        except Exception:
            self.access_token, self.cookie = previous
            self.auth_failed = True
            # Never include browser/provider/HTTP exception data: it may contain credentials.
            raise NeoBundAuthError("NeoBund 登录同步或验证失败，本轮已暂停，请重新登录") from None
        if self.auth_config_path:
            try:
                persist_validated_credentials(self.auth_config_path, credentials)
            except Exception:
                # Session is valid now; a disk error must not turn a rejected POST into
                # an uncertain submission or cause an unnecessary second refresh.
                self.auth_persistence_error = "NeoBund 凭证已验证，但本机配置保存失败"

    def request(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None, json_body: Any = None) -> Any:
        if self.auth_failed:
            raise NeoBundAuthError("NeoBund 登录失效，本轮已暂停，请在专用窗口重新登录")
        response = self._request_once(method, path, params=params, json_body=json_body)
        if self._auth_rejected(response):
            self._refresh_auth_once()
            response = self._request_once(method, path, params=params, json_body=json_body)
            if self._auth_rejected(response):
                self.auth_failed = True
                raise NeoBundAuthError("NeoBund 刷新登录后仍被拒绝，本轮已暂停")
        self._raise_for_status_with_body(response, f"NeoBund {method.upper()} {path} 失败")
        if not str(response.text or "").strip():
            return {}
        return response.json()

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self.request("GET", path, params=params)

    def post(self, path: str, payload: Dict[str, Any]) -> Any:
        return self.request("POST", path, json_body=payload)

    def list_creator_accounts(self, *, current: int = 1, size: int = 100, username: str = "") -> Any:
        params: Dict[str, Any] = {"current": current, "size": size}
        if username:
            params["username"] = username
        return self.get("/auth/creator/list", params)

    def list_tiktok_accounts(self, *, current: int = 1, size: int = 100, username: str = "") -> Any:
        params: Dict[str, Any] = {"current": current, "size": size, "queryScope": 1}
        if username:
            params["username"] = username
        return self.get("/tk/auth/list", params)

    def list_products(self, *, auth_id: str, current: int = 1, size: int = 100, product_title: str = "", tt_product_id: str = "") -> Any:
        params: Dict[str, Any] = {"authId": _maybe_int(auth_id), "current": current, "size": size}
        if product_title:
            params["productTitle"] = product_title
        if tt_product_id:
            params["ttProductId"] = str(tt_product_id).strip()
        return self.get("/auth/creator/product/list", params)

    def get_upload_token(self, *, file_name: str, file_type: str, asset_type: int) -> Any:
        return self.get("/file/token", {"fileName": file_name, "fileType": file_type, "type": asset_type})

    def save_file_record(self, payload: Dict[str, Any]) -> Any:
        return self.post("/file/upload", payload)

    def commit_shoppable_video(self, payload: Dict[str, Any]) -> Any:
        return self.post(self.shoppable_commit_path, payload)

    def list_shoppable_videos(self, params: Dict[str, Any]) -> Any:
        return self.get(self.shoppable_list_path, params)

    def commit_organic_video(self, payload: Dict[str, Any]) -> Any:
        return self.post(self.organic_commit_path, payload)

    def list_organic_videos(self, params: Dict[str, Any]) -> Any:
        return self.get(self.organic_list_path, params)

    def terminate_video(self, task_id: str) -> Any:
        return self.get("/shoppable/video/terminate", {"taskId": _maybe_int(task_id)})


class NeoBundS3Uploader:
    def __init__(self, *, timeout: int = 300, session: Optional[requests.Session] = None):
        self.timeout = max(1, int(timeout or 300))
        self.session = session or requests.Session()

    @staticmethod
    def _content_type(path: Path) -> str:
        guessed, _ = mimetypes.guess_type(str(path))
        return guessed or "application/octet-stream"

    @staticmethod
    def _sha256_file(path: Path) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _sign(key: bytes, message: str) -> bytes:
        return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()

    @classmethod
    def _signature_key(cls, secret_key: str, date_stamp: str, region: str) -> bytes:
        key_date = cls._sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
        key_region = cls._sign(key_date, region)
        key_service = cls._sign(key_region, "s3")
        return cls._sign(key_service, "aws4_request")

    @staticmethod
    def _extract_credentials(payload: Any) -> Dict[str, str]:
        values = {
            "region": _first_value(payload, ("region", "data.region")),
            "access_key_id": _first_value(payload, ("accessKeyId", "access_key_id", "data.accessKeyId", "data.access_key_id")),
            "secret_access_key": _first_value(payload, ("secretAccessKey", "secret_access_key", "data.secretAccessKey", "data.secret_access_key")),
            "session_token": _first_value(payload, ("sessionToken", "session_token", "data.sessionToken", "data.session_token")),
            "bucket_name": _first_value(payload, ("bucketName", "bucket", "data.bucketName", "data.bucket")),
            "key_prefix": _first_value(payload, ("keyPrefix", "prefix", "data.keyPrefix", "data.prefix"), ""),
            "base_url": _first_value(payload, ("baseUrl", "baseURL", "url", "data.baseUrl", "data.baseURL", "data.url"), ""),
        }
        missing = [key for key in ("region", "access_key_id", "secret_access_key", "bucket_name") if not values.get(key)]
        if missing:
            raise RuntimeError(f"NeoBund S3 临时凭证缺少字段 {missing}: {payload}")
        return {key: str(value or "") for key, value in values.items()}

    @staticmethod
    def _make_key(path: Path, key_prefix: str) -> str:
        suffix = path.suffix.lower()
        alphabet = string.ascii_letters + string.digits
        random_tail = "".join(secrets.choice(alphabet) for _ in range(secrets.choice(range(6, 9))))
        return f"{key_prefix}{uuid.uuid4()}-{random_tail}{suffix}"

    def upload_file(self, file_path: str, token_payload: Any) -> Dict[str, Any]:
        path = Path(file_path)
        if not path.exists() or not path.is_file():
            raise RuntimeError(f"NeoBund 上传视频路径不可用: {file_path}")

        credentials = self._extract_credentials(token_payload)
        region = credentials["region"]
        bucket = credentials["bucket_name"]
        access_key = credentials["access_key_id"]
        secret_key = credentials["secret_access_key"]
        session_token = credentials["session_token"]
        key = self._make_key(path, credentials["key_prefix"])
        content_type = self._content_type(path)
        payload_hash = self._sha256_file(path)

        now = datetime.utcnow()
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        host = f"{bucket}.s3.{region}.amazonaws.com"
        canonical_uri = "/" + quote(key, safe="/~")
        url = f"https://{host}{canonical_uri}"

        headers = {
            "content-type": content_type,
            "host": host,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        }
        if session_token:
            headers["x-amz-security-token"] = session_token

        signed_header_names = sorted(headers)
        canonical_headers = "".join(f"{name}:{headers[name]}\n" for name in signed_header_names)
        signed_headers = ";".join(signed_header_names)
        canonical_request = "\n".join(
            [
                "PUT",
                canonical_uri,
                "",
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )
        credential_scope = f"{date_stamp}/{region}/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        signature = hmac.new(
            self._signature_key(secret_key, date_stamp, region),
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        request_headers = {
            **headers,
            "Authorization": (
                "AWS4-HMAC-SHA256 "
                f"Credential={access_key}/{credential_scope}, "
                f"SignedHeaders={signed_headers}, Signature={signature}"
            ),
            "Content-Length": str(path.stat().st_size),
        }

        with open(path, "rb") as handle:
            response = self.session.put(url, data=handle, headers=request_headers, timeout=self.timeout)
        NeoBundClient._raise_for_status_with_body(response, f"NeoBund S3 上传失败: {path.name}")

        base_url = credentials["base_url"]
        public_url = f"{base_url}{key}" if base_url else url
        return {
            "bucketName": bucket,
            "fileName": path.name,
            "fileSize": path.stat().st_size,
            "fileType": content_type,
            "key": key,
            "type": 2,
            "url": public_url,
        }


class NeoBundPublishAdapter(BasePublishAdapter):
    def __init__(
        self,
        *,
        base_url: str = "https://www.neobund.ai/np",
        access_token: str = "",
        cookie: str = "",
        account_id_map: Optional[Dict[str, Any]] = None,
        is_precheck: int = 0,
        timeout: int = 300,
        shoppable_commit_path: str = "/shoppable/video/commit",
        shoppable_list_path: str = "/shoppable/video/list",
        organic_commit_path: str = "/shoppable/video/commit",
        organic_list_path: str = "/shoppable/video/list",
        ai_generated_field: str = "isAIGC",
        client: Optional[NeoBundClient] = None,
        uploader: Optional[NeoBundS3Uploader] = None,
        task_id_prefix: str = NEOBUND_TASK_PREFIX,
        auth_refresh_provider: Optional[Callable[[], Dict[str, str]]] = None,
        auth_config_path: str = "",
    ):
        self.client = client or NeoBundClient(
            base_url=base_url,
            access_token=access_token,
            cookie=cookie,
            shoppable_commit_path=shoppable_commit_path,
            shoppable_list_path=shoppable_list_path,
            organic_commit_path=organic_commit_path,
            organic_list_path=organic_list_path,
            timeout=timeout,
            auth_refresh_provider=auth_refresh_provider,
            auth_config_path=auth_config_path,
        )
        self.uploader = uploader or NeoBundS3Uploader(timeout=timeout)
        self.account_id_map = account_id_map or {}
        self.is_precheck = 1 if int(is_precheck or 0) else 0
        self.task_id_prefix = task_id_prefix
        self.ai_generated_field = str(ai_generated_field or "isAIGC").strip()
        self._auth_id_cache: Dict[str, str] = {}

    @staticmethod
    def _is_quicktime_container(path: Path) -> bool:
        try:
            with open(path, "rb") as handle:
                header = handle.read(16)
        except OSError:
            return False
        return len(header) >= 12 and header[4:8] == b"ftyp" and header[8:12] == b"qt  "

    @staticmethod
    def _find_ffmpeg() -> str:
        candidates = [
            shutil.which("ffmpeg"),
            str(Path.home() / ".local/bin/ffmpeg"),
            "/opt/homebrew/bin/ffmpeg",
            "/usr/local/bin/ffmpeg",
            "/usr/bin/ffmpeg",
        ]
        for candidate in candidates:
            if candidate and Path(candidate).exists():
                return candidate
        return ""

    @classmethod
    def _find_ffprobe(cls) -> str:
        ffmpeg = cls._find_ffmpeg()
        candidates = [
            str(Path(ffmpeg).with_name("ffprobe")) if ffmpeg else "",
            shutil.which("ffprobe"),
            "/opt/homebrew/bin/ffprobe",
            "/usr/local/bin/ffprobe",
            "/usr/bin/ffprobe",
        ]
        for candidate in candidates:
            if candidate and Path(candidate).exists():
                return candidate
        return ""

    @classmethod
    def _neobund_mp4_path(cls, path: Path) -> Path:
        if path.name.endswith(".neobund.mp4"):
            return path.with_name(f"{path.stem}.remux.mp4")
        return path.with_name(f"{path.stem}.neobund.mp4")

    @classmethod
    def _neobund_standardized_path(cls, path: Path) -> Path:
        if path.name.endswith(".neobund.mp4"):
            return path.with_name(f"{path.stem}.standard.mp4")
        return path.with_name(f"{path.stem}.neobund-standard.mp4")

    @staticmethod
    def _frame_rate(value: Any) -> float:
        text = str(value or "").strip()
        if not text:
            return 0.0
        try:
            if "/" in text:
                numerator, denominator = text.split("/", 1)
                return float(numerator) / float(denominator)
            return float(text)
        except (TypeError, ValueError, ZeroDivisionError):
            return 0.0

    @classmethod
    def _video_needs_standardization(cls, path: Path) -> bool:
        ffprobe = cls._find_ffprobe()
        if not ffprobe:
            return False
        try:
            result = subprocess.run(
                [ffprobe, "-v", "error", "-print_format", "json", "-show_streams", str(path)],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode != 0:
                return False
            payload = json.loads(result.stdout or "{}")
        except (OSError, subprocess.SubprocessError, ValueError, json.JSONDecodeError):
            return False
        streams = payload.get("streams", []) if isinstance(payload, dict) else []
        video = next((item for item in streams if isinstance(item, dict) and item.get("codec_type") == "video"), None)
        if not isinstance(video, dict):
            return False
        codec = str(video.get("codec_name") or "").lower()
        fps = cls._frame_rate(video.get("avg_frame_rate") or video.get("r_frame_rate"))
        return codec != "h264" or fps < 24 or fps > 60

    @classmethod
    def _ensure_neobund_mp4(cls, path: Path) -> Path:
        if not cls._is_quicktime_container(path):
            return path
        target = cls._neobund_mp4_path(path)
        if target.exists() and target.stat().st_size > 0 and target.stat().st_mtime >= path.stat().st_mtime and not cls._is_quicktime_container(target):
            return target
        ffmpeg = cls._find_ffmpeg()
        if not ffmpeg:
            raise RuntimeError(f"NeoBund 视频容器为 QuickTime，需要 ffmpeg 预处理但未找到: {path}")
        command = [
            ffmpeg,
            "-y",
            "-i",
            str(path),
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-dn",
            "-sn",
            "-map_metadata",
            "-1",
            "-c",
            "copy",
            "-movflags",
            "+faststart",
            "-brand",
            "mp42",
            str(target),
        ]
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            stderr = str(result.stderr or result.stdout or "").strip()
            raise RuntimeError(f"NeoBund 视频容器预处理失败: {path}; {stderr[:1000]}")
        if not target.exists() or target.stat().st_size <= 0:
            raise RuntimeError(f"NeoBund 视频容器预处理未生成可用文件: {target}")
        if cls._is_quicktime_container(target):
            raise RuntimeError(f"NeoBund 视频容器预处理后仍是 QuickTime: {target}")
        return target

    @classmethod
    def _ensure_neobund_compatible_video(cls, path: Path) -> Path:
        if not cls._video_needs_standardization(path):
            return path
        target = cls._neobund_standardized_path(path)
        if target.exists() and target.stat().st_size > 0 and target.stat().st_mtime >= path.stat().st_mtime:
            if not cls._video_needs_standardization(target):
                return target
        ffmpeg = cls._find_ffmpeg()
        if not ffmpeg:
            raise RuntimeError(f"NeoBund 视频帧率异常，需要 ffmpeg 标准化但未找到: {path}")
        command = [
            ffmpeg,
            "-y",
            "-i",
            str(path),
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-dn",
            "-sn",
            "-map_metadata",
            "-1",
            "-vf",
            "fps=30,format=yuv420p",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "20",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-movflags",
            "+faststart",
            str(target),
        ]
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            stderr = str(result.stderr or result.stdout or "").strip()
            raise RuntimeError(f"NeoBund 视频帧率标准化失败: {path}; {stderr[:1000]}")
        if not target.exists() or target.stat().st_size <= 0:
            raise RuntimeError(f"NeoBund 视频帧率标准化未生成可用文件: {target}")
        if cls._video_needs_standardization(target):
            raise RuntimeError(f"NeoBund 视频帧率标准化后仍不兼容: {target}")
        return target

    def _resolve_auth_id(self, account_id: str, *, content_type: str = "shoppable") -> str:
        raw_account_id = str(account_id or "").strip()
        mapped = self.account_id_map.get(raw_account_id)
        if isinstance(mapped, dict):
            mapped = mapped.get("neobund_auth_id") or mapped.get("authId") or mapped.get("id")
        resolved = str(mapped or raw_account_id).strip()
        if not resolved:
            raise RuntimeError(f"NeoBund authId 为空: account_id={account_id}")
        if resolved.isdigit():
            return resolved
        cache_key = f"{content_type}:{resolved}"
        cached = self._auth_id_cache.get(cache_key)
        if cached:
            return cached
        if content_type == "organic":
            accounts = self.client.list_tiktok_accounts(current=1, size=200)
        else:
            accounts = self.client.list_creator_accounts(current=1, size=200)
        records = accounts.get("records", []) if isinstance(accounts, dict) else []
        for item in records:
            if not isinstance(item, dict):
                continue
            candidates = {
                str(item.get("authId") or "").strip(),
                str(item.get("id") or "").strip(),
                str(item.get("username") or "").strip(),
                str(item.get("creatorUsername") or "").strip(),
                str(item.get("creatorNickname") or "").strip(),
                str(item.get("remark") or "").strip(),
            }
            if resolved in candidates:
                if content_type == "organic":
                    quota_status = str(item.get("quotaStatus") or "").strip()
                    if quota_status and quota_status != "1":
                        raise RuntimeError(
                            f"NeoBund Organic 账号当前不可发布: account_id={account_id}, quotaStatus={quota_status}"
                        )
                auth_id = str(item.get("authId") or item.get("id") or "").strip()
                if auth_id:
                    self._auth_id_cache[cache_key] = auth_id
                    return auth_id
        raise RuntimeError(f"NeoBund 未找到账号 ID/用户名对应的 authId: {account_id}")
        return resolved

    def list_accounts(self) -> Any:
        return self.client.list_creator_accounts()

    def list_products(self, *, auth_id: str, product_id: str = "") -> Any:
        return self.client.list_products(auth_id=self._resolve_auth_id(auth_id, content_type="shoppable"), tt_product_id=product_id)

    def _resolve_product_title(self, *, auth_id: str, product_id: str, product_title: str = "") -> str:
        explicit_title = str(product_title or "").strip()
        if explicit_title:
            return _sanitize_product_link_title(explicit_title) or str(product_id or "").strip()[:30]
        tt_product_id = str(product_id or "").strip()
        if not tt_product_id:
            return ""
        try:
            payload = self.client.list_products(auth_id=auth_id, tt_product_id=tt_product_id, current=1, size=20)
        except Exception:
            return tt_product_id[:30]
        records = payload.get("records", []) if isinstance(payload, dict) else []
        for item in records:
            if not isinstance(item, dict):
                continue
            item_id = str(item.get("id") or item.get("ttProductId") or "").strip()
            if item_id and item_id != tt_product_id:
                continue
            title = str(item.get("title") or item.get("productTitle") or item.get("name") or "").strip()
            if title:
                return _sanitize_product_link_title(title) or tt_product_id[:30]
        return tt_product_id[:30]

    def upload_video(self, video_path: str) -> NeoBundUploadResult:
        path = Path(str(video_path or "").strip())
        if not path.exists() or not path.is_file():
            raise RuntimeError(f"NeoBund 第一阶段需要可访问的本地视频文件: {video_path}")
        path = self._ensure_neobund_mp4(path)
        path = self._ensure_neobund_compatible_video(path)
        file_type = NeoBundS3Uploader._content_type(path)
        token_payload = self.client.get_upload_token(file_name=path.name, file_type=file_type, asset_type=2)
        upload_payload = self.uploader.upload_file(str(path), token_payload)
        save_payload = {
            "bucketName": upload_payload["bucketName"],
            "fileName": upload_payload["fileName"],
            "fileSize": upload_payload["fileSize"],
            "fileType": upload_payload["fileType"],
            "key": upload_payload["key"],
            "type": 2,
        }
        result = self.client.save_file_record(save_payload)
        file_id = str(_first_value(result, ("fileId", "attachFileId", "id", "data.fileId", "data.attachFileId", "data.id"), "")).strip()
        if not file_id:
            raise RuntimeError(f"NeoBund 文件上传登记未返回 fileId: {result}")
        url = str(_first_value(result, ("url", "data.url"), upload_payload.get("url", "")) or "")
        return NeoBundUploadResult(
            file_id=file_id,
            key=str(upload_payload["key"]),
            bucket_name=str(upload_payload["bucketName"]),
            url=url,
        )

    def _upload_before_commit(self, video_path: str) -> NeoBundUploadResult:
        try:
            return self.upload_video(video_path)
        except NeoBundAuthError:
            raise
        except Exception as exc:
            raise PreCommitUploadError(exc) from None

    @staticmethod
    def _extract_commit_task_id(result: Any) -> str:
        return str(
            _first_value(
                result,
                (
                    "id",
                    "taskId",
                    "task_id",
                    "data.id",
                    "data.taskId",
                    "data.task_id",
                    "records.0.id",
                    "data.records.0.id",
                ),
                "",
            )
        ).strip()

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
        mark_ai: Optional[bool] = None,
        music_selection: Optional[Dict[str, Any]] = None,
    ) -> str:
        from app.script_pool import reject_internal_product_id
        reject_internal_product_id(product_id)
        tt_product_id = str(product_id or "").strip()
        if music_selection and tt_product_id:
            raise ValueError(
                "music_selection 仅支持 Organic 非带货发布；带货任务请勿传 music_selection"
            )
        if not tt_product_id:
            auth_id = self._resolve_auth_id(account_id, content_type="organic")
            return self._create_organic_scheduled_task(
                auth_id=auth_id,
                video_path=video_path,
                title=title,
                publish_at=publish_at,
                script_id=script_id,
                mark_ai=mark_ai,
                music_selection=music_selection,
            )

        auth_id = self._resolve_auth_id(account_id, content_type="shoppable")
        return self._create_shoppable_scheduled_task(
            auth_id=auth_id,
            video_path=video_path,
            title=title,
            publish_at=publish_at,
            script_id=script_id,
            mark_ai=mark_ai,
            product_id=tt_product_id,
            product_title=product_title,
        )

    @staticmethod
    def _music_commit_fields(music_selection: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Map a validated music selection onto organic commit fields.

        Field names captured from real NeoBund traffic 2026-08-31
        (POST /np/shoppable/video/commit with a hot-list song attached):
        musicId / musicTitle / musicAuthor / musicUrl / musicCoverUrl /
        musicSoundVolume / videoOriginalSoundVolume (both 0-100, frontend
        default 50).
        """
        if not music_selection:
            return {}
        required = ("music_id", "music_title", "music_author", "music_url")
        missing = [
            key
            for key in required
            if not str(music_selection.get(key) or "").strip()
        ]
        if missing:
            raise ValueError(f"music_selection 缺少必填字段: {missing}")
        fields: Dict[str, Any] = {
            "musicId": str(music_selection["music_id"]),
            "musicTitle": str(music_selection["music_title"]),
            "musicAuthor": str(music_selection["music_author"]),
            "musicUrl": str(music_selection["music_url"]),
            "musicSoundVolume": int(music_selection.get("music_sound_volume", 50)),
            "videoOriginalSoundVolume": int(
                music_selection.get("video_original_sound_volume", 50)
            ),
        }
        cover_url = str(music_selection.get("music_cover_url") or "").strip()
        if cover_url:
            fields["musicCoverUrl"] = cover_url
        return fields

    def _create_shoppable_scheduled_task(
        self,
        *,
        auth_id: str,
        video_path: str,
        title: str,
        publish_at: datetime,
        script_id: str,
        mark_ai: Optional[bool],
        product_id: str,
        product_title: str = "",
    ) -> str:
        tt_product_id = str(product_id or "").strip()
        if not tt_product_id:
            raise RuntimeError(f"NeoBund 发布缺少 TikTok Shop 商品 ID: script_id={script_id}")

        upload_result = self._upload_before_commit(video_path)
        resolved_product_title = self._resolve_product_title(auth_id=auth_id, product_id=tt_product_id, product_title=product_title)
        payload: Dict[str, Any] = {
            "authId": _maybe_int(auth_id),
            "authType": 1,
            "ttProductId": tt_product_id,
            "productTitle": resolved_product_title,
            "videoTitle": str(title or "").strip()[:4000],
            "scheduledReleaseTime": publish_at.strftime("%Y-%m-%d %H:%M:%S"),
            "attachFileId": _maybe_int(upload_result.file_id),
            "isPrecheck": self.is_precheck,
            "remark": str(script_id or Path(video_path).name).strip()[:100],
        }
        if mark_ai is not None and self.ai_generated_field:
            payload[self.ai_generated_field] = bool(mark_ai)
        return self._commit_once(payload, content_type="shoppable")

    def _create_organic_scheduled_task(
        self,
        *,
        auth_id: str,
        video_path: str,
        title: str,
        publish_at: datetime,
        script_id: str,
        mark_ai: Optional[bool],
        music_selection: Optional[Dict[str, Any]] = None,
    ) -> str:
        upload_result = self._upload_before_commit(video_path)
        payload: Dict[str, Any] = {
            "authId": _maybe_int(auth_id),
            "authType": 2,
            "videoTitle": str(title or "").strip()[:4000],
            "scheduledReleaseTime": publish_at.strftime("%Y-%m-%d %H:%M:%S"),
            "attachFileId": _maybe_int(upload_result.file_id),
            "isPrecheck": self.is_precheck,
            "remark": str(script_id or Path(video_path).name).strip()[:100],
            # Captured 2026-08-31 from real NeoBund frontend traffic; the
            # commit rejects payloads missing these organic post options.
            "postType": 1,
            "brandContentToggle": 0,
            "brandOrganicToggle": 0,
            "disableComment": 0,
            "disableDuet": 0,
            "disableStitch": 0,
        }
        if mark_ai is not None and self.ai_generated_field:
            payload[self.ai_generated_field] = 1 if mark_ai else 0
        payload.update(self._music_commit_fields(music_selection))
        return self._commit_once(payload, content_type="organic")

    def _commit_once(self, payload: Dict[str, Any], *, content_type: str) -> str:
        commit = (self.client.commit_organic_video if content_type == "organic"
                  else self.client.commit_shoppable_video)
        error = "接口未返回任务 ID"
        try:
            task_id = self._extract_commit_task_id(commit(payload))
            if task_id:
                return f"{self.task_id_prefix}{task_id}"
        except NeoBundAuthError:
            # HTTP/JSON 401 means the platform explicitly rejected the submission.
            raise
        except Exception as exc:
            # A transport or server exception after send is not proof of rejection.
            error = str(exc)
        try:
            task_id = self._find_committed_task_id_with_retry(
                auth_id=str(payload["authId"]), script_id=str(payload["remark"]),
                video_title=str(payload["videoTitle"]),
                product_id=str(payload.get("ttProductId") or ""),
                scheduled_for=payload["scheduledReleaseTime"], content_type=content_type,
            )
            if task_id:
                return f"{self.task_id_prefix}{task_id}"
        except Exception as exc:
            error = f"{error}; 对账失败: {exc}"
        raise AmbiguousPublishError(f"NeoBund 提交结果不明，保留占位，禁止自动重发: {error}")

    def reconcile_scheduled_task(self, context: Dict[str, Any]) -> str:
        """Read-only lookup using the frozen request identity; empty means unknown."""
        content_type = "shoppable" if context.get("product_id") else "organic"
        auth_id = str(context.get("auth_id") or self._resolve_auth_id(
            str(context["account_id"]), content_type=content_type))
        task_id = self._find_committed_task_id(
            auth_id=auth_id, script_id=str(context["script_id"]),
            video_title=str(context["title"]), product_id=str(context.get("product_id") or ""),
            scheduled_for=str(context["scheduled_for"]), content_type=content_type,
        )
        return f"{self.task_id_prefix}{task_id}" if task_id else ""

    def submission_identity(self, *, account_id: str, product_id: str) -> Dict[str, str]:
        content_type = "shoppable" if product_id else "organic"
        return {"auth_id": self._resolve_auth_id(account_id, content_type=content_type),
                "content_type": content_type}

    def _find_committed_task_id_with_retry(
        self,
        *,
        auth_id: str,
        script_id: str,
        video_title: str,
        product_id: str,
        scheduled_for: str,
        content_type: str,
    ) -> str:
        # NeoBund can accept a task but return an empty body before its task list
        # becomes consistent. Poll the list instead of submitting a duplicate.
        for delay_seconds in (0, 1, 2, 4, 8):
            if delay_seconds:
                time.sleep(delay_seconds)
            task_id = self._find_committed_task_id(
                auth_id=auth_id,
                script_id=script_id,
                video_title=video_title,
                product_id=product_id,
                scheduled_for=scheduled_for,
                content_type=content_type,
            )
            if task_id:
                return task_id
        return ""

    def _find_committed_task_id(
        self,
        *,
        auth_id: str,
        script_id: str,
        video_title: str,
        product_id: str,
        scheduled_for: str,
        content_type: str = "shoppable",
    ) -> str:
        params = {
            "current": 1,
            "size": 50,
            "authId": _maybe_int(auth_id),
            "authType": 2 if content_type == "organic" else 1,
        }
        if content_type == "organic":
            payload = self.client.list_organic_videos(params)
        else:
            payload = self.client.list_shoppable_videos(params)
        records = payload.get("records", []) if isinstance(payload, dict) else []
        matches = set()
        for item in records:
            if not isinstance(item, dict):
                continue
            item_id = str(item.get("id") or item.get("taskId") or item.get("task_id") or "").strip()
            if not item_id:
                continue
            if str(item.get("scheduledReleaseTime") or "").strip() != scheduled_for:
                continue
            if product_id and str(item.get("ttProductId") or "").strip() != product_id:
                continue
            remark = str(item.get("remark") or "").strip()
            item_title = str(item.get("videoTitle") or item.get("postTitle") or "").strip()
            if (remark == str(script_id).strip() if script_id else item_title == video_title):
                matches.add(item_id)
        if len(matches) > 1:
            raise AmbiguousPublishError("平台存在多个相同业务标识的任务，需要人工核对")
        return next(iter(matches), "")

    def find_organic_task_record(
        self,
        *,
        script_id: str,
        video_title: str,
        scheduled_for: str,
        auth_id: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Return the full organic task record (includes actual music fields).

        Same matching rules as _find_committed_task_id (remark==script_id or
        title match + identical scheduledReleaseTime); auth_id narrows the
        query when known but is omitted from the request when empty.
        """
        params: Dict[str, Any] = {"current": 1, "size": 50, "authType": 2}
        if str(auth_id or "").strip():
            params["authId"] = _maybe_int(str(auth_id).strip())
        payload = self.client.list_organic_videos(params)
        records = payload.get("records", []) if isinstance(payload, dict) else []
        wanted_scheduled = str(scheduled_for or "").strip()
        wanted_script = str(script_id or "").strip()
        wanted_title = str(video_title or "").strip()
        for item in records:
            if not isinstance(item, dict):
                continue
            if wanted_scheduled and str(item.get("scheduledReleaseTime") or "").strip() != wanted_scheduled:
                continue
            remark = str(item.get("remark") or "").strip()
            item_title = str(item.get("videoTitle") or item.get("postTitle") or "").strip()
            if wanted_script and remark == wanted_script:
                return item
            if wanted_title and item_title == wanted_title:
                return item
        return None

    def actual_music_for_task(self, task_id: str) -> Optional[Dict[str, str]]:
        """Read back the music actually attached to an organic NeoBund task.

        No inferred start offset is returned because the captured platform
        record does not expose one.  Callers use this only to verify that the
        selected song was not silently replaced after submission.
        """
        resolved = self._strip_task_prefix(task_id)
        if not resolved:
            return None
        item = self._load_task_item(resolved)
        returned_id = str(item.get("id") or item.get("taskId") or item.get("videoId") or "").strip()
        if returned_id and returned_id != resolved:
            return None
        music_id = str(item.get("musicId") or "").strip()
        if not music_id:
            return None
        return {
            "music_id": music_id,
            "title": str(item.get("musicTitle") or ""),
            "author": str(item.get("musicAuthor") or ""),
        }

    def _strip_task_prefix(self, task_id: str) -> str:
        text = str(task_id or "").strip()
        if text.startswith(self.task_id_prefix):
            return text[len(self.task_id_prefix) :]
        return text

    @staticmethod
    def _extract_task_item(payload: Any, task_id: str) -> Dict[str, Any]:
        requested_id = str(task_id or "").strip()
        if isinstance(payload, dict):
            records = payload.get("records") or _deep_get(payload, "data.records")
            if isinstance(records, list):
                for item in records:
                    if not isinstance(item, dict):
                        continue
                    item_id = str(item.get("id") or item.get("taskId") or item.get("task_id") or "").strip()
                    if not requested_id or item_id == requested_id:
                        return item
                # List endpoints may ignore an id filter.  Returning a different
                # record here can permanently mark the requested task published.
                return {}
            if any(key in payload for key in ("id", "taskId", "task_id", "status")):
                item_id = str(payload.get("id") or payload.get("taskId") or payload.get("task_id") or "").strip()
                return payload if not requested_id or item_id == requested_id else {}
        if isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                item_id = str(item.get("id") or item.get("taskId") or item.get("task_id") or "").strip()
                if not requested_id or item_id == requested_id:
                    return item
        return {}

    @staticmethod
    def _parse_task_status(item: Dict[str, Any], scheduled_for: datetime) -> PublishTaskStatus:
        if not item:
            return PublishTaskStatus(state="pending", result="待执行")

        raw_status = _first_value(item, ("status", "taskStatus", "state"), "")
        status_text = str(raw_status or "").strip()
        normalized = status_text.lower()
        error_message = str(
            _first_value(
                item,
                (
                    "errorMessage",
                    "error_message",
                    "failReason",
                    "failDesc",
                    "message",
                    "precheckIssues.0.suggestions",
                    "goodQualityCheckIssues.0.suggestions",
                ),
                "",
            )
            or ""
        ).strip()
        published_at = str(
            _first_value(
                item,
                ("publishedAt", "publishTime", "publishedTime", "releaseTime", "updateTime", "data.publishedAt"),
                "",
            )
            or ""
        ).strip()

        failure_markers = {"failed", "fail", "error", "rejected", "terminated", "cancelled", "canceled", "-1"}
        success_markers = {"success", "published", "done", "completed", "complete", "finish", "finished"}
        if normalized in success_markers:
            return PublishTaskStatus(state="success", result="发布成功", published_at=published_at or scheduled_for.strftime("%Y-%m-%d %H:%M:%S"))
        if normalized in failure_markers:
            return PublishTaskStatus(state="failed", result="发布失败", error_message=error_message)

        precheck_result = str(item.get("precheckResult") or "").strip().upper()
        quality_result = str(item.get("goodQualityCheckResult") or "").strip().upper()
        if precheck_result == "FAIL" or quality_result == "FAIL":
            return PublishTaskStatus(state="failed", result="发布失败", error_message=error_message or "NeoBund/TikTok 预检未通过")

        try:
            numeric_status = int(float(status_text))
        except (TypeError, ValueError):
            numeric_status = 0
        if numeric_status == 800:
            return PublishTaskStatus(
                state="failed", result="发布失败",
                error_message=error_message or "NeoBund 任务已终止",
            )
        if 0 < numeric_status < 350:
            return PublishTaskStatus(state="pending", result="待执行")
        if numeric_status == 350:
            if error_message:
                return PublishTaskStatus(state="failed", result="发布失败", error_message=error_message)
            return PublishTaskStatus(state="success", result="发布成功", published_at=published_at or scheduled_for.strftime("%Y-%m-%d %H:%M:%S"))
        remote_video_id = str(
            _first_value(item, ("videoId", "ttVideoId", "platformVideoId"), "") or ""
        ).strip()
        if numeric_status == 500 and remote_video_id and not error_message:
            # Completed organic tasks expose the TikTok video id even though
            # the NeoBund row does not expose a separate published timestamp.
            return PublishTaskStatus(
                state="success", result="发布成功",
                published_at=published_at or scheduled_for.strftime("%Y-%m-%d %H:%M:%S"),
            )
        if numeric_status > 350:
            return PublishTaskStatus(
                state="unknown", result="状态待核实",
                error_message=error_message or f"NeoBund 未知任务状态: {numeric_status}",
            )
        return PublishTaskStatus(state="pending", result="待执行")

    def query_task_status(self, *, task_id: str, scheduled_for: datetime) -> PublishTaskStatus:
        resolved_task_id = self._strip_task_prefix(task_id)
        if not resolved_task_id:
            return PublishTaskStatus(state="pending", result="待执行")
        item: Dict[str, Any] = {}
        shoppable_error: Optional[Exception] = None
        try:
            payload = self.client.list_shoppable_videos({"id": _maybe_int(resolved_task_id)})
            item = self._extract_task_item(payload, resolved_task_id)
        except Exception as exc:
            shoppable_error = exc
        if not item:
            try:
                payload = self.client.list_organic_videos({"id": _maybe_int(resolved_task_id)})
                item = self._extract_task_item(payload, resolved_task_id)
            except Exception as organic_error:
                if shoppable_error is not None:
                    raise shoppable_error
                raise organic_error
        return self._parse_task_status(item, scheduled_for)

    @staticmethod
    def _task_is_terminated(item: Dict[str, Any]) -> bool:
        status = str(item.get("status") or item.get("taskStatus") or "").strip().lower()
        error_message = str(
            item.get("errorMessage") or item.get("failReason") or item.get("message") or ""
        ).strip().lower()
        return status in {"800", "terminated", "cancelled", "canceled"} or "terminated" in error_message

    def _load_task_item(self, task_id: str) -> Dict[str, Any]:
        payload = self.client.list_organic_videos({"id": _maybe_int(task_id)})
        return self._extract_task_item(payload, task_id)

    def terminate_task(self, *, task_id: str) -> PublishTaskStatus:
        resolved_task_id = self._strip_task_prefix(task_id)
        if not resolved_task_id:
            raise ValueError("NeoBund 终止任务缺少 task_id")
        item = self._load_task_item(resolved_task_id)
        if self._task_is_terminated(item):
            return PublishTaskStatus(state="terminated", result="已终止")
        try:
            self.client.terminate_video(resolved_task_id)
        except requests.HTTPError:
            item = self._load_task_item(resolved_task_id)
            if self._task_is_terminated(item):
                return PublishTaskStatus(state="terminated", result="已终止")
            raise
        for delay_seconds in (0, 1, 2):
            if delay_seconds:
                time.sleep(delay_seconds)
            item = self._load_task_item(resolved_task_id)
            if self._task_is_terminated(item):
                return PublishTaskStatus(state="terminated", result="已终止")
        raise RuntimeError(f"NeoBund 终止任务后状态未确认: task_id={task_id}")

    def query_task_statuses(self, tasks: Iterable[Any]) -> Dict[str, PublishTaskStatus]:
        statuses: Dict[str, PublishTaskStatus] = {}
        for task in tasks:
            task_id = str(task["publish_task_id"] or "").strip()
            if not task_id.startswith(self.task_id_prefix):
                statuses[task_id] = PublishTaskStatus(state="pending", result="待执行")
                continue
            try:
                scheduled_for = datetime.strptime(str(task["scheduled_for"]), "%Y-%m-%d %H:%M:%S")
                statuses[task_id] = self.query_task_status(task_id=task_id, scheduled_for=scheduled_for)
            except Exception as exc:
                statuses[task_id] = PublishTaskStatus(
                    state="unknown", result="查询暂不可用", error_message=str(exc))
        return statuses
