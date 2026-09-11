#!/usr/bin/env python3
"""CreatOK TikTok Publish adapter.

通过官方 `creatok` CLI（npm 全局包 @creatok/cli）调用发布能力，不依赖浏览器
登录态或调试端口。API Key 只在子进程环境变量中传递，绝不进入命令参数、
数据库、飞书或日志。

安全约定（与任务说明一致）：
- 提交前必须把 idempotency_key / operation_id 写入 submission_context（由
  `build_submission_context` 生成，scheduler 在 reserve 前落库）；
- 提交超时或结果未知时只能用原 idempotency_key 做 `publish status` 恢复，
  禁止重新生成 key 后盲目提交；
- Organic Video 与 Organic Photo 的 publish envelope 已通过正式账号验证；
  TikTok Shop 发布仍在契约确认前关闭。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import time
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from app.models import PublishRequest, PublishTaskStatus
from app.publishers import BasePublishAdapter


CREATOK_TASK_PREFIX = "creatok:"

# CLI 官方要求的环境变量名（stable channel）；profiles 表可指向别名环境变量，
# runner 会把别名变量的值映射到标准变量名注入子进程。
CREATOK_CLI_ENV = "CREATOK_API_KEY"

# 默认 CLI 安装名（npm 全局）；可用 publisher_profiles.cli_path 覆盖。
CREATOK_DEFAULT_CLI = "creatok"

# ---------------------------------------------------------------------------
# CreatOK CLI 子命令映射（本模块唯一真源）
#
# v0.14.0 起 `publish` 改组为 `publish <group> <command>`，且 connection
# capabilities/creator-info 与 job get 的参数由 flag 改为位置参数。
# 本模块所有 CLI 调用一律引用下面的常量，不再写散落的字符串字面量；
# CLI 下次调整命令结构时，只改这一张表并同步 tests/test_creatok_publish.py。
#
#   v0.13.0（旧，端点已 404，仅存档）            -> v0.14.0（现行）
#   publish connections                         -> publish connection list
#   publish capabilities --connection <uid>     -> publish connection capabilities <uid>
#   publish creator-info --connection <uid>     -> publish connection creator-info <uid>
#   publish prepare-video --connection <uid>    -> publish video prepare --connection <uid>
#   publish submit --file <f>                   -> publish job submit --file <f>
#   publish status --job-id <id>                -> publish job get <id>
#   publish status --idempotency-key <k>        -> publish job get --idempotency-key <k>
#   assets create / assets list 两版之间未变。
# ---------------------------------------------------------------------------
CREATOK_CMD_DOCTOR: Tuple[str, ...] = ("doctor",)
CREATOK_CMD_CONNECTION_LIST: Tuple[str, ...] = ("publish", "connection", "list")
CREATOK_CMD_CONNECTION_CAPABILITIES: Tuple[str, ...] = ("publish", "connection", "capabilities")
CREATOK_CMD_CONNECTION_CREATOR_INFO: Tuple[str, ...] = ("publish", "connection", "creator-info")
CREATOK_CMD_VIDEO_PREPARE: Tuple[str, ...] = ("publish", "video", "prepare")
CREATOK_CMD_JOB_SUBMIT: Tuple[str, ...] = ("publish", "job", "submit")
CREATOK_CMD_JOB_GET: Tuple[str, ...] = ("publish", "job", "get")
CREATOK_CMD_ASSET_CREATE: Tuple[str, ...] = ("assets", "create")
CREATOK_CMD_ASSET_LIST: Tuple[str, ...] = ("assets", "list")

# 状态映射（任务说明第八节）。
# queued/scheduled/processing → 已排期；waiting_confirmation/inbox → 待账号确认；
# succeeded/published → 已发布；failed 且 result_unknown=false → 发布失败；
# result_unknown=true → 提交中（继续对账）；canceled → 已取消。
_READY_STATES = {"queued", "scheduled", "processing", "pending", "create_pending", "running"}

# 用于从 CLI 错误文本中模糊处理可能泄漏的凭据片段（防御式脱敏）。
_API_KEY_PATTERN = re.compile(r"\bok_[A-Za-z0-9_-]{6,}\b|\b[A-Za-z0-9+/]{24,}={0,2}\b")




# 账号ID → 连接UID 自动发现的进程内缓存（creator-info 查询较慢，避免重复调用）
_CREATOK_HANDLE_CACHE: Dict[str, Any] = {"handles": {}, "loaded": False}


def discover_connection_uid(cli: "CreatOKCLI", account_id: str) -> str:
    """按 TikTok 用户名（账号表账号ID）在 CreatOK 连接中自动发现连接 UID。"""
    wanted = str(account_id or "").strip().lower()
    if not wanted:
        return ""
    if not _CREATOK_HANDLE_CACHE["loaded"]:
        handles: Dict[str, str] = {}
        payload = cli.run([*CREATOK_CMD_CONNECTION_LIST])
        connections = (
            (payload.get("data") or {}).get("connections")
            or payload.get("connections")
            or []
        )
        for connection in connections:
            if not isinstance(connection, dict):
                continue
            uid = str(connection.get("connection_uid") or "").strip()
            names = {
                str(connection.get(key) or "").strip().lower()
                for key in ("username", "display_name")
            }
            names.discard("")
            if uid:
                # creator_username 是 TikTok 真实 handle，账号表账号ID即它；
                # 连接列表的 display_name 是运营可读名，两者都参与匹配。
                try:
                    info = cli.run(
                        [*CREATOK_CMD_CONNECTION_CREATOR_INFO, uid]
                    )
                    nested = (info.get("data") or info) or {}
                    creator_username = str(
                        nested.get("creator_username") or ""
                    ).strip().lower()
                except Exception:  # noqa: BLE001 - 单连接失败不阻塞发现
                    creator_username = ""
                if creator_username:
                    names.add(creator_username)
            for name in names:
                if name and name not in handles:
                    handles[name] = uid
        _CREATOK_HANDLE_CACHE["handles"] = handles
        _CREATOK_HANDLE_CACHE["loaded"] = True
    return _CREATOK_HANDLE_CACHE["handles"].get(wanted, "")

class CreatOKError(RuntimeError):
    """CLI 调用失败；消息已脱敏，可安全入库/日志。"""


class CreatOKAuthError(CreatOKError):
    """API Key 缺失或无效（error.kind == auth 或 doctor api_key_configured=false）。"""


class CreatOKResultUnknownError(CreatOKError):
    """上游结果未知（提交超时 / result_unknown=true）；禁止自动重发，只能对账。"""

    submission_ambiguous = True


class CreatOKSubmissionNotReadyError(RuntimeError):
    """当前发布类型或排期条件尚未满足，拒绝创建发布任务。"""

    submission_not_sent = True
    retryable = True


class CreatOKPreSubmitError(CreatOKSubmissionNotReadyError):
    """媒体上传/解析等提交前阶段失败；发布请求未曾发出，可安全重试。"""


CLI_CALL_TIMEOUT = 180


def _sanitize_error_text(text: str) -> str:
    """去掉错误文本中可能的凭据片段，避免 Key 经日志/飞书泄露。"""
    return _API_KEY_PATTERN.sub("***", str(text or ""))[:4000]


def _deep_get(payload: Any, dotted_path: str) -> Any:
    current = payload
    for part in str(dotted_path or "").split("."):
        if not part:
            continue
        if isinstance(current, list):
            current = current[0] if current else None
            if not isinstance(current, dict):
                return None
        elif not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _media_fingerprint(media_paths: Sequence[str]) -> str:
    """请求媒体指纹：本地文件按内容哈希；URL/路径串按原文哈希。

    用于 submission_context.media_sha256，标识"同一批介质"以冻结身份。
    """
    digest = hashlib.sha256()
    for path in media_paths or []:
        digest.update(str(path).encode("utf-8", errors="replace"))
        try:
            p = Path(path)
            if p.is_file():
                digest.update(b"\0")
                with p.open("rb") as fh:
                    block = fh.read(1024 * 1024)
                    while block:
                        digest.update(block)
                        block = fh.read(1024 * 1024)
        except OSError:
            pass
    return digest.hexdigest()


def _file_sha256(path: str) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def prepare_tiktok_compatible_video(source_path: str) -> Dict[str, Any]:
    """Create a stable CFR/H.264/AAC derivative for CreatOK Content Posting."""
    source = Path(source_path).expanduser().resolve()
    if not source.is_file():
        raise CreatOKPreSubmitError(f"CreatOK 视频文件不存在：{source}")
    ffmpeg = shutil.which("ffmpeg") or "/Users/likeu3/.local/bin/ffmpeg"
    ffprobe = shutil.which("ffprobe") or str(Path(ffmpeg).with_name("ffprobe"))
    if not Path(ffmpeg).is_file() and not shutil.which("ffmpeg"):
        raise CreatOKPreSubmitError("找不到 ffmpeg，无法生成 TikTok 兼容视频")
    try:
        probe = subprocess.run(
            [ffprobe, "-v", "error", "-print_format", "json", "-show_streams", str(source)],
            capture_output=True, text=True, timeout=30, check=False,
        )
        payload = json.loads(probe.stdout or "{}") if probe.returncode == 0 else {}
    except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
        raise CreatOKPreSubmitError("无法读取待发布视频编码信息") from exc
    streams = list(payload.get("streams") or [])
    if not any(item.get("codec_type") == "video" for item in streams):
        raise CreatOKPreSubmitError("待发布文件没有可解码的视频流")
    has_audio = any(item.get("codec_type") == "audio" for item in streams)
    original_sha = _file_sha256(str(source))
    root = Path(os.environ.get(
        "CREATOK_COMPAT_MEDIA_DIR",
        str(Path.home() / ".openclaw/shared/data/short_video_auto_publish_creatok_compat"),
    )).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    output = root / f"{source.stem}_{original_sha[:16]}_tiktok.mp4"
    if not output.is_file():
        temporary = output.with_suffix(".tmp.mp4")
        command = [ffmpeg, "-y", "-v", "error", "-i", str(source)]
        if has_audio:
            command.extend(["-map", "0:v:0", "-map", "0:a:0"])
        else:
            command.extend([
                "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
                "-map", "0:v:0", "-map", "1:a:0", "-shortest",
            ])
        command.extend([
            "-c:v", "libx264", "-profile:v", "high", "-level:v", "4.0",
            "-pix_fmt", "yuv420p", "-r", "30", "-vsync", "cfr",
            "-c:a", "aac", "-b:a", "128k", "-ar", "44100", "-ac", "2",
            "-movflags", "+faststart", "-map_metadata", "-1", str(temporary),
        ])
        try:
            rendered = subprocess.run(command, capture_output=True, text=True, timeout=300, check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise CreatOKPreSubmitError("TikTok 兼容转码进程失败") from exc
        if rendered.returncode != 0 or not temporary.is_file():
            raise CreatOKPreSubmitError(
                "TikTok 兼容转码失败：" + _sanitize_error_text(rendered.stderr[-600:])
            )
        temporary.replace(output)
    verified = subprocess.run(
        [ffmpeg, "-v", "error", "-i", str(output), "-f", "null", "-"],
        capture_output=True, text=True, timeout=120, check=False,
    )
    if verified.returncode != 0:
        raise CreatOKPreSubmitError("TikTok 兼容副本完整解码失败")
    return {
        "path": str(output.resolve()), "original_path": str(source),
        "original_sha256": original_sha, "upload_sha256": _file_sha256(str(output)),
        "compatibility_profile": "h264-high-l4-yuv420p-cfr30-aac44k-faststart-v1",
        "source_had_audio": has_audio,
    }


def create_idempotency_key(request: PublishRequest) -> str:
    """稳定幂等键：同一请求在任何进程/时间重算结果一致。"""
    identity = "|".join(
        [
            str(request.account_id or ""),
            str(request.script_id or ""),
            request.publish_at.strftime("%Y-%m-%d %H:%M:%S") if request.publish_at else "",
            str(request.timezone or ""),
            str(request.content_type or ""),
            str(request.commerce_type or ""),
            str(request.delivery_mode or ""),
            str(request.title or ""),
            str(request.description or ""),
            "1" if request.auto_add_music else "0",
            json.dumps(request.music_selection or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            "" if request.mark_ai is None else ("1" if request.mark_ai else "0"),
            _media_fingerprint(request.media_paths),
        ]
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _job_status(payload: Dict[str, Any]) -> Tuple[str, bool]:
    """从 CLI 响应提取 job 状态与 result_unknown。

    返回 (status_lower, result_unknown)。
    """
    job = _deep_get(payload, "data.job") or _deep_get(payload, "data") or payload
    if not isinstance(job, dict):
        return ("", False)
    status = str(job.get("status") or job.get("state") or "").strip().lower()
    result_unknown = bool(
        _deep_get(payload, "error.result_unknown")
        or _deep_get(payload, "data.job.result_unknown")
        or job.get("result_unknown")
    )
    return (status, result_unknown)


def parse_task_status(payload: Dict[str, Any]) -> PublishTaskStatus:
    status, result_unknown = _job_status(payload)
    if result_unknown or status == "":
        return PublishTaskStatus(state="unknown", result="结果未知，继续对账")
    if status in _READY_STATES:
        return PublishTaskStatus(state="pending", result="已排期")
    if status in {"waiting_confirmation", "inbox"}:
        return PublishTaskStatus(state="pending_confirmation", result="待账号确认")
    if status in {"succeeded", "published", "success", "done", "complete"}:
        published_at = _deep_get(payload, "data.published_at") or _deep_get(payload, "data.job.published_at")
        return PublishTaskStatus(
            state="success",
            result="发布成功",
            published_at=str(published_at or "") or None,
        )
    if status in {"failed", "failed_publish", "error"}:
        reason = _deep_get(payload, "error.reason") or job_error_reason(payload)
        return PublishTaskStatus(
            state="failed",
            result="发布失败",
            error_message=_sanitize_error_text(str(reason or "")),
        )
    if status in {"canceled", "cancelled", "canceled_by_user", "cancel"}:
        return PublishTaskStatus(state="terminated", result="已取消")
    return PublishTaskStatus(state="pending", result="待执行")


def job_error_reason(payload: Dict[str, Any]) -> str:
    for path in (
        "error.message",
        "error.reason",
        "data.job.fail_reason",
        "data.job.error",
        "data.error_message",
        "data.job.error_message",
    ):
        value = _deep_get(payload, path)
        if isinstance(value, dict):
            value = value.get("message") or value.get("reason")
        if value:
            return str(value)
    return ""


# ------------------------------------------------------------------ Envelope

# 官方契约支持的时区枚举（CLI v0.13.0 实测；非 IANA 风格，发布前需 IANA → GMT+ 转换）。
CREATOK_SUPPORTED_TIMEZONES = ("GMT+8", "EST", "PST", "BRT", "CST")
PHOTO_TITLE_MAX_UTF16 = 90
PHOTO_DESCRIPTION_MAX_UTF16 = 4000


def utf16_units(value: str) -> int:
    return len(str(value or "").encode("utf-16-le")) // 2


def validate_content_photo_metadata(request: PublishRequest) -> None:
    title = str(request.title or "").strip()
    description = str(request.description or "").strip()
    if utf16_units(title) > PHOTO_TITLE_MAX_UTF16:
        raise CreatOKPreSubmitError(
            f"TikTok 图文标题超过 {PHOTO_TITLE_MAX_UTF16} 个 UTF-16 单位"
        )
    if utf16_units(description) > PHOTO_DESCRIPTION_MAX_UTF16:
        raise CreatOKPreSubmitError(
            f"TikTok 图文正文超过 {PHOTO_DESCRIPTION_MAX_UTF16} 个 UTF-16 单位"
        )


def creatok_schedule_timezone(iana_timezone: str) -> str:
    """IANA 时区 → CreatOK 排期时区枚举（显示语义；实际时刻由 schedule.at 绝对时间戳决定）。

    实测枚举仅支持 GMT+8/EST/PST/BRT/CST；亚洲站（含泰国/越南 GMT+7）统一映射 GMT+8，
    schedule.at 按账号 IANA 时区换算为绝对 UTC 毫秒，发布时刻不受 timezone 显示值影响。
    """
    text = str(iana_timezone or "").strip()
    if text.upper() in {"EST", "PST", "BRT", "CST", "GMT+8"}:
        return text.upper()
    if text.upper() in {"EET", "UTC", "GMT", "GMT+0"}:
        return "EST" if text.upper() in {"EET"} else "GMT+8"
    # 亚洲/默认一律 GMT+8（东南亚账号主流时区）
    return "GMT+8"


def publish_at_epoch_ms(publish_at: datetime, iana_timezone: str) -> int:
    """按账号 IANA 时区解读 naive publish_at，换算成绝对 UTC 毫秒时间戳。

    解决 naive 时间混淆问题：发布槽位（如 15:00）以账号本地时区（Asia/Bangkok）计，
    换算为绝对时间后，CreatOK 的 schedule.at 是唯一决定发布时刻的值。
    """
    zone_name = str(iana_timezone or "").strip()
    if zone_name:
        try:
            from zoneinfo import ZoneInfo
            normalized = publish_at if publish_at.tzinfo else publish_at.replace(tzinfo=ZoneInfo(zone_name))
            return int(normalized.timestamp() * 1000)
        except Exception:
            pass
    # 无 IANA 或解析失败：按本机时区解释（与旧行为一致）
    return int(publish_at.timestamp() * 1000)


class CreatOKEnvelopeBuilder:
    """按官方契约组装 publish envelope。

    已通过正式账号采集 fixture 固化的契约：
    - kind=tiktok_shop_video（Shop 带货视频，media.file_id 来自 prepare-video）
    - kind=tiktok_content_video（Content Posting 视频，media.object_key 直接用资产；
      post_mode 变体仅 DIRECT_POST；可选 schedule_time（毫秒 epoch）排期）
    - kind=tiktok_content_photo（Content Posting 原生图文，支持标题、正文、
      多图、排期以及 TikTok 自动推荐音乐）
    尚未接入：Shop Photo（当前连接无 capability）。
    """

    def build_content_video_envelope(
        self,
        *,
        request: PublishRequest,
        connection_uid: str,
        operation_id: str,
        idempotency_key: str,
        object_key: str,
        file_name: str,
        privacy_level: str = "PUBLIC_TO_EVERYONE",
        disable_comment: bool = False,
        disable_duet: bool = False,
        disable_stitch: bool = False,
        brand_content_toggle: bool = False,
        brand_organic_toggle: bool = False,
        schedule_at_ms: Optional[int] = None,
        schedule_timezone: str = "GMT+8",
    ) -> Dict[str, Any]:
        provider_options: Dict[str, Any] = {
            "post_mode": request.delivery_mode.upper() if request.delivery_mode else "DIRECT_POST",
            "privacy_level": privacy_level,
            "disable_comment": bool(disable_comment),
            "disable_duet": bool(disable_duet),
            "disable_stitch": bool(disable_stitch),
            "brand_content_toggle": bool(brand_content_toggle),
            "brand_organic_toggle": bool(brand_organic_toggle),
        }
        envelope: Dict[str, Any] = {
            "kind": "tiktok_content_video",
            "connection_uid": connection_uid,
            "idempotency_key": idempotency_key,
            "media": {"object_key": object_key, "file_name": file_name},
            "provider_options": provider_options,
        }
        if schedule_at_ms is not None:
            envelope["schedule"] = {
                "at": int(schedule_at_ms),
                "timezone": str(schedule_timezone or "GMT+8"),
            }
        return envelope

    def build_content_photo_envelope(
        self,
        *,
        request: PublishRequest,
        connection_uid: str,
        operation_id: str,
        idempotency_key: str,
        object_keys: Sequence[str],
        privacy_level: str = "PUBLIC_TO_EVERYONE",
        disable_comment: bool = False,
        auto_add_music: Optional[bool] = None,
        brand_content_toggle: bool = False,
        brand_organic_toggle: bool = False,
        schedule_at_ms: Optional[int] = None,
        schedule_timezone: str = "GMT+8",
    ) -> Dict[str, Any]:
        if not object_keys:
            raise CreatOKError("CreatOK 图文发布必须至少提供一张图片 object_key")
        validate_content_photo_metadata(request)
        resolved_auto_music = request.auto_add_music if auto_add_music is None else bool(auto_add_music)
        envelope: Dict[str, Any] = {
            "kind": "tiktok_content_photo",
            "connection_uid": connection_uid,
            "idempotency_key": idempotency_key,
            "media": {"object_keys": list(object_keys)},
            "provider_options": {
                "post_mode": request.delivery_mode.upper() if request.delivery_mode else "DIRECT_POST",
                "privacy_level": privacy_level,
                "disable_comment": bool(disable_comment),
                "auto_add_music": bool(resolved_auto_music),
                "brand_content_toggle": bool(brand_content_toggle),
                "brand_organic_toggle": bool(brand_organic_toggle),
                "title": str(request.title or "").strip(),
                "description": str(request.description or "").strip(),
            },
        }
        if schedule_at_ms is not None:
            envelope["schedule"] = {
                "at": int(schedule_at_ms),
                "timezone": str(schedule_timezone or "GMT+8"),
            }
        return envelope

    def build_shop_video_envelope(
        self,
        *,
        request: PublishRequest,
        connection_uid: str,
        operation_id: str,
        idempotency_key: str,
        file_id: str,
        product_title: str = "",
    ) -> Dict[str, Any]:
        if request.timezone not in CREATOK_SUPPORTED_TIMEZONES:
            raise CreatOKError(
                f"CreatOK 不支持时区 {request.timezone}；请改用：{'、'.join(CREATOK_SUPPORTED_TIMEZONES)}"
            )
        if not request.product_id:
            raise CreatOKError("CreatOK Shop 发布必须提供平台商品 ID（product_id）")
        return {
            "kind": "tiktok_shop_video",
            "connection_uid": connection_uid,
            "idempotency_key": idempotency_key,
            "timezone": request.timezone,
            "media": {"file_id": file_id, "file_kind": "video_file"},
            "provider_options": {
                "product_id": str(request.product_id or "").strip(),
                "product_title": str(product_title or request.product_title or "").strip(),
                "title": request.title,
            },
        }


class CreatOKCLI:
    """creatok CLI 的进程封装。Key 仅经环境变量传递，绝不进 argv。"""

    def __init__(
        self,
        *,
        cli_path: str = CREATOK_DEFAULT_CLI,
        api_key_env_name: str = CREATOK_CLI_ENV,
        timeout: int = CLI_CALL_TIMEOUT,
    ):
        self.cli_path = cli_path or CREATOK_DEFAULT_CLI
        self.api_key_env_name = api_key_env_name or CREATOK_CLI_ENV
        self.timeout = timeout

    def resolve_api_key(self) -> str:
        key = os.environ.get(self.api_key_env_name, "").strip()
        if not key:
            raise CreatOKAuthError(
                f"未配置 CreatOK API Key（环境变量 {self.api_key_env_name}）；"
                "请在 creatok.ai/app/workspace/api-keys 生成并配置"
            )
        return key

    def run(self, args: Sequence[str], *, timeout: Optional[int] = None) -> Dict[str, Any]:
        key = self.resolve_api_key()
        env = dict(os.environ)
        # CLI 只认标准变量名；别名环境变量在此映射进子进程。
        env[CREATOK_CLI_ENV] = key
        try:
            proc = subprocess.run(
                [self.cli_path, *args],
                capture_output=True,
                text=True,
                env=env,
                timeout=timeout or self.timeout,
                shell=False,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise CreatOKResultUnknownError(
                f"creatok CLI 请求超时（{args[1] if len(args) > 1 else args[0]}），结果未知，禁止自动重发"
            ) from exc
        except FileNotFoundError as exc:
            raise CreatOKError(f"creatok CLI 不可用：{self.cli_path} 未找到或不在 PATH") from exc

        stdout = proc.stdout or ""
        stderr = proc.stderr or ""
        payload = self._parse_json(stdout, stderr, args)
        self._raise_on_error(payload or {}, args)
        return payload or {}

    def _parse_json(self, stdout: str, stderr: str, args: Sequence[str]) -> Optional[Dict[str, Any]]:
        if stdout.strip():
            try:
                parsed = json.loads(stdout.strip())
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                pass
        raise CreatOKError(
            f"creatok CLI 输出无法解析为 JSON（命令 {self._command_label(args)}）："
            f"{_sanitize_error_text(stderr or stdout)[:400]}"
        )

    @staticmethod
    def _command_label(args: Sequence[str]) -> str:
        return " ".join(str(a) for a in args[:2])

    def _raise_on_error(self, payload: Dict[str, Any], args: Sequence[str]) -> None:
        ok = payload.get("ok")
        status = payload.get("status")
        if ok is True or status == "succeeded":
            return
        error = payload.get("error") or {}
        kind = str(error.get("kind") or "").strip()
        message = _sanitize_error_text(
            error.get("message") or error.get("reason") or f"CLI 返回错误（status={status}）"
        )
        request_id = str(error.get("request_id") or payload.get("request_id") or "")
        if kind == "auth":
            raise CreatOKAuthError(f"CreatOK 鉴权失败：{message}")
        if error.get("result_unknown") or payload.get("data.job.result_unknown"):
            raise CreatOKResultUnknownError(f"CreatOK 结果未知：{message}")
        raise CreatOKError(f"creatok CLI 执行失败（{self._command_label(args)}）"
                           + (f"，request_id={request_id}" if request_id else "")
                           + f"：{message}")


class CreatOKPublishAdapter(BasePublishAdapter):
    """CreatOK 渠道适配器：连接发现、能力同步、资产生成、任务提交与对账。"""

    def __init__(
        self,
        *,
        cli: Optional[CreatOKCLI] = None,
        account_connections: Optional[Dict[str, str]] = None,
        account_timezones: Optional[Dict[str, str]] = None,
        envelope_builder: Optional[Callable[[PublishRequest, Dict[str, Any], str, str, Dict[str, Any]], Dict[str, Any]]] = None,
        video_preparer: Optional[Callable[[str], Dict[str, Any]]] = None,
        asset_verifier: Optional[Callable[[str, str], Dict[str, Any]]] = None,
        on_connection_resolved: Optional[Callable[[str, str], None]] = None,
    ):
        self.cli = cli or CreatOKCLI()
        self.account_connections = dict(account_connections or {})
        self.account_timezones = dict(account_timezones or {})
        self.on_connection_resolved = on_connection_resolved
        # 默认 builder 已覆盖 Organic Video / Organic Photo / Shop Video；
        # 可注入自定义 builder 做契约测试，未支持的发布类型仍会在提交前拒绝。
        self.envelope_builder = envelope_builder
        self.video_preparer = video_preparer or prepare_tiktok_compatible_video
        self.asset_verifier = asset_verifier or self._verify_uploaded_asset
        self._frozen_contexts: Dict[str, Dict[str, Any]] = {}
        self._submission_receipts: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------ 发现

    def doctor(self) -> Dict[str, Any]:
        payload = self.cli.run([*CREATOK_CMD_DOCTOR])
        if not payload.get("data", {}).get("api_key_configured"):
            raise CreatOKAuthError("CreatOK doctor：API Key 未配置或无效")
        return payload

    def list_connections(
        self,
        *,
        capability: str = "",
        platform: str = "",
        operation_id: str = "",
    ) -> List[Dict[str, Any]]:
        args = [*CREATOK_CMD_CONNECTION_LIST]
        if capability:
            args.extend(["--capability", capability])
        if platform:
            args.extend(["--platform", platform])
        if operation_id:
            args.extend(["--operation-id", operation_id])
        payload = self.cli.run(args)
        data = payload.get("data") or {}
        connections = data.get("connections") or data.get("items") or []
        return [item for item in connections if isinstance(item, dict)]

    def get_capabilities(self, *, connection_uid: str, operation_id: str = "") -> Dict[str, Any]:
        args = [*CREATOK_CMD_CONNECTION_CAPABILITIES, connection_uid]
        if operation_id:
            args.extend(["--operation-id", operation_id])
        return self.cli.run(args)

    def get_creator_info(self, *, connection_uid: str, operation_id: str = "") -> Dict[str, Any]:
        args = [*CREATOK_CMD_CONNECTION_CREATOR_INFO, connection_uid]
        if operation_id:
            args.extend(["--operation-id", operation_id])
        return self.cli.run(args)

    # ------------------------------------------------------------------ 资产

    def upload_asset(self, *, asset_type: str, file_path: str, operation_id: str = "") -> str:
        """上传本地文件为正式 Asset，返回对象键（object_key，如 uploads/videos/...）。

        实测：submit 的 media.object_key 直接接受 assets create 返回的裸对象键
        （官方文档"the key is at data.asset.object_key"）；assets list 的签名
        media_url 是下载视图，不可用于提交（会报 asset_not_accessible）。
        assets 命令组不支持 --operation-id（仅 publish 命令组支持），参数保留为占位。
        """
        if asset_type not in {"video", "image"}:
            raise CreatOKError(f"不支持的资产类型：{asset_type}")
        return self.upload_asset_details(
            asset_type=asset_type, file_path=file_path, operation_id=operation_id,
        )["object_key"]

    def upload_asset_details(self, *, asset_type: str, file_path: str,
                             operation_id: str = "", verify: bool = False) -> Dict[str, Any]:
        if asset_type not in {"video", "image"}:
            raise CreatOKError(f"不支持的资产类型：{asset_type}")
        args = [*CREATOK_CMD_ASSET_CREATE, "--type", asset_type, "--file", str(file_path)]
        payload = self.cli.run(args)
        object_key = _deep_get(payload, "data.asset.object_key") or _deep_get(payload, "data.object_key")
        asset_uid = _deep_get(payload, "data.asset.uid") or _deep_get(payload, "data.uid")
        if not object_key:
            raise CreatOKError("assets create 未返回 object_key")
        details = {
            "asset_uid": str(asset_uid or ""), "object_key": str(object_key),
            "local_path": str(Path(file_path).expanduser().resolve()),
            "local_sha256": (_file_sha256(file_path) if Path(file_path).is_file() else ""),
        }
        if verify:
            if not asset_uid:
                raise CreatOKPreSubmitError("CreatOK 视频上传未返回 asset_uid，无法做回读校验")
            details.update(self.asset_verifier(str(asset_uid), str(file_path)))
        return details

    def _verify_uploaded_asset(self, asset_uid: str, local_path: str) -> Dict[str, Any]:
        """Download the just-created signed asset and compare its exact bytes."""
        expected_sha = _file_sha256(local_path)
        target = None
        for attempt in range(3):
            target = next(
                (item for item in self.list_assets() if str(item.get("uid") or "") == asset_uid),
                None,
            )
            if target and target.get("media_url"):
                break
            if attempt < 2:
                time.sleep(1)
        if not target or not target.get("media_url"):
            raise CreatOKPreSubmitError(f"CreatOK 资产 {asset_uid} 上传后无法回读")
        digest = hashlib.sha256()
        size = 0
        try:
            with urllib.request.urlopen(str(target["media_url"]), timeout=60) as response:
                while True:
                    block = response.read(1024 * 1024)
                    if not block:
                        break
                    size += len(block)
                    digest.update(block)
        except Exception as exc:
            raise CreatOKPreSubmitError(f"CreatOK 资产 {asset_uid} 回读下载失败") from exc
        downloaded_sha = digest.hexdigest()
        if downloaded_sha != expected_sha or size != Path(local_path).stat().st_size:
            raise CreatOKPreSubmitError(
                f"CreatOK 资产 {asset_uid} 回读字节与本地不一致，已阻止提交"
            )
        return {
            "upload_verified": True, "verified_sha256": downloaded_sha,
            "verified_size_bytes": size,
        }

    def list_assets(self) -> List[Dict[str, Any]]:
        payload = self.cli.run([*CREATOK_CMD_ASSET_LIST])
        return [item for item in (payload.get("data") or {}).get("items") or [] if isinstance(item, dict)]

    def resolve_asset_media_url(self, asset_uid: str) -> str:
        """从 assets list 取正式签名 media_url。

        assets create 返回的裸 uploads/... 路径不可用于提交（TikTok 无法下载，
        实测 job 卡在 PROCESSING_DOWNLOAD / precheck 报 Video id is invalid）；
        提交与 prepare-video 都必须用带签名的 media_url。
        """
        urls = self.resolve_asset_media_urls([asset_uid])
        if not urls:
            raise CreatOKError(f"资产 {asset_uid} 未找到或正式 media_url 不可用")
        return urls[0]

    def resolve_asset_media_urls(self, asset_uids: Sequence[str]) -> List[str]:
        """一次性解析多资产签名 URL，按输入顺序返回；任一缺失即抛错。"""
        allowed = {str(uid or "").strip() for uid in asset_uids if str(uid or "").strip()}
        if not allowed:
            return []
        found: Dict[str, str] = {}
        for item in self.list_assets():
            uid = str(item.get("uid") or "").strip()
            if uid in allowed and uid not in found:
                url = str(item.get("media_url") or "").strip()
                if url:
                    found[uid] = url
        urls = [found[uid] for uid in (str(u or "").strip() for u in asset_uids) if uid in found]
        if len(urls) != len(asset_uids):
            missing = [uid for uid in asset_uids if str(uid or "").strip() not in found]
            raise CreatOKError(f"资产签名 URL 缺失：{missing}")
        return urls

    def prepare_shop_video(self, *, connection_uid: str, asset_json: Dict[str, Any], operation_id: str = "") -> str:
        """把正式视频 Asset 转成 TikTok Shop file_id（Shop Video 专用）。"""
        import tempfile
        args = [*CREATOK_CMD_VIDEO_PREPARE, "--connection", connection_uid]
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
            json.dump(asset_json, fh, ensure_ascii=False)
            tmp_path = fh.name
        try:
            args.extend(["--file", tmp_path])
            if operation_id:
                args.extend(["--operation-id", operation_id])
            payload = self.cli.run(args)
        finally:
            os.unlink(tmp_path)
        file_id = _deep_get(payload, "data.file_id") or _deep_get(payload, "data.video_file_id")
        if not file_id:
            raise CreatOKError("prepare-video 未返回 file_id")
        return str(file_id)

    # ------------------------------------------------------------------ 幂等

    def build_submission_context(self, request: PublishRequest) -> Dict[str, Any]:
        """提交前必须写入 submission_context 的冻结身份（由 scheduler 在 reserve 前合并）。"""
        idempotency_key = create_idempotency_key(request)
        if idempotency_key in self._frozen_contexts:
            return dict(self._frozen_contexts[idempotency_key])
        context = {
            "provider": "CreatOK",
            "operation_id": uuid.uuid4().hex,
            "idempotency_key": idempotency_key,
            "content_type": request.content_type,
            "commerce_type": request.commerce_type,
            "timezone": request.timezone,
            "media_sha256": _media_fingerprint(request.media_paths),
            "connection_uid": self.connection_uid_for(request.account_id),
            "music_mode": (
                "platform_auto" if request.auto_add_music
                else str((request.music_selection or {}).get("mode") or "no_bgm")
            ),
            "music_selection": dict(request.music_selection or {}),
            "ai_generated": request.mark_ai,
            "provider_ai_label_status": (
                "unsupported_by_creatok_v0.13.0"
                if request.content_type == "photo" and request.mark_ai is not None
                else "not_applicable"
            ),
        }
        self._frozen_contexts[idempotency_key] = dict(context)
        return context

    def submission_receipt(self, idempotency_key: str) -> Dict[str, Any]:
        return dict(self._submission_receipts.get(str(idempotency_key or ""), {}))

    def submission_identity(self, *, account_id: str, product_id: str = "") -> Dict[str, Any]:
        # 兼容旧调度路径：稳定幂等键的最终值由 request 决定，这里返回空；
        # scheduler 已在 reserve 前调用 build_submission_context 冻结身份。
        return {
            "provider": "CreatOK",
            "connection_uid": self.connection_uid_for(account_id),
        }

    # ------------------------------------------------------------------ 提交

    # Organic 排期窗口：60 秒 ~ 7 天（官方能力，超出保留待排期，不标失败）。
    ORGANIC_MIN_SCHEDULE_SECONDS = 60
    ORGANIC_MAX_SCHEDULE_DAYS = 7
    # Shop 无排期契约，保持关闭直到官方确认发布时点语义。
    SHOP_SUBMISSION_ALLOWED = False

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
        """兼容旧签名：单视频请求翻译为 PublishRequest 后统一提交。"""
        request = PublishRequest(
            account_id=account_id,
            content_type="video",
            commerce_type="shop" if product_id else "organic",
            media_paths=[video_path],
            title=title,
            publish_at=publish_at,
            timezone=self.account_timezones.get(account_id, ""),
            script_id=script_id,
            product_id=product_id,
            product_title=product_title,
            ref_video_id=ref_video_id,
            mark_ai=mark_ai,
            music_selection=music_selection,
        )
        return self.create_publish_task(request)

    def _validate_organic_window(self, request: PublishRequest) -> None:
        """排期窗口校验：60 秒 ~ 7 天；窗口外必须保留待排期（不标失败、不拒绝）。"""
        delta = request.publish_at - datetime.now()
        seconds = delta.total_seconds()
        if seconds < self.ORGANIC_MIN_SCHEDULE_SECONDS or seconds > self.ORGANIC_MAX_SCHEDULE_DAYS * 86400:
            raise CreatOKSubmissionNotReadyError(
                f"CreatOK Organic 排期窗口为 60 秒 ~ 7 天，当前为 {seconds:.0f} 秒，任务未提交（保留待排期）"
            )

    def _submit_envelope(self, envelope: Dict[str, Any], operation_id: str) -> Dict[str, Any]:
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
            json.dump(envelope, fh, ensure_ascii=False)
            tmp_path = fh.name
        try:
            return self.cli.run([*CREATOK_CMD_JOB_SUBMIT, "--file", tmp_path, "--operation-id", operation_id])
        finally:
            os.unlink(tmp_path)

    def connection_uid_for(self, account_id: str) -> str:
        """解析账号的 CreatOK 连接 UID：先查映射表，未命中时按 TikTok 用户名
        自动发现（账号表的账号ID即 TikTok handle），命中后回填映射。

        自动发现避免手工填写连接 UID；无法匹配时给出可操作的错误提示。
        """
        uid = str(self.account_connections.get(account_id, "") or "").strip()
        if uid:
            return uid
        discovered = discover_connection_uid(self.cli, account_id)
        if not discovered:
            handles = sorted(
                handle
                for handle in (_CREATOK_HANDLE_CACHE.get("handles") or {}).values()
            )
            raise CreatOKError(
                f"账号 {account_id} 未找到匹配的 CreatOK 连接；"
                f"已连接的 TikTok 账号: {handles or '（无）'}。"
                "请先在 CreatOK 页面为该 TikTok 账号完成授权，或核对账号表账号ID。"
            )
        self.account_connections[account_id] = discovered
        if callable(self.on_connection_resolved):
            try:
                self.on_connection_resolved(account_id, discovered)
            except Exception:  # noqa: BLE001 - 回填失败不影响提交
                pass
        return discovered

    def create_publish_task(self, request: PublishRequest) -> str:
        connection_uid = self.connection_uid_for(request.account_id)
        if request.commerce_type != "organic" or request.content_type not in ("video", "photo"):
            raise CreatOKSubmissionNotReadyError(
                f"CreatOK {request.commerce_type}/{request.content_type} 尚未接入（Shop 无排期契约确认前关闭），任务未提交"
            )
        self._validate_organic_window(request)
        builder = self.envelope_builder or CreatOKEnvelopeBuilder()
        if request.content_type == "photo":
            validate_content_photo_metadata(request)
        context = self.build_submission_context(request)
        idempotency_key = context["idempotency_key"]
        operation_id = context["operation_id"]
        asset_type = "video" if request.content_type == "video" else "image"
        upload_paths = list(request.media_paths)
        compatibility: Dict[str, Any] = {}
        if request.content_type == "video":
            compatibility = dict(self.video_preparer(str(request.media_paths[0])))
            upload_paths = [str(compatibility["path"])]
        try:
            uploaded_assets = [
                self.upload_asset_details(
                    asset_type=asset_type, file_path=str(media_path),
                    operation_id=operation_id, verify=request.content_type == "video",
                )
                for media_path in upload_paths
            ]
            object_keys = [item["object_key"] for item in uploaded_assets]
        except CreatOKError as exc:
            # 上传/解析阶段失败：发布提交从未发出，必须允许安全重试。
            raise CreatOKPreSubmitError(f"CreatOK 媒体准备失败：{exc}") from exc
        file_name = Path(str(upload_paths[0])).name
        schedule_ms = publish_at_epoch_ms(request.publish_at, request.timezone)
        schedule_tz = creatok_schedule_timezone(request.timezone)
        if request.content_type == "video":
            envelope = builder.build_content_video_envelope(
                request=request,
                connection_uid=connection_uid,
                operation_id=operation_id,
                idempotency_key=idempotency_key,
                object_key=object_keys[0],
                file_name=file_name,
                schedule_at_ms=schedule_ms,
                schedule_timezone=schedule_tz,
            )
        else:
            envelope = builder.build_content_photo_envelope(
                request=request,
                connection_uid=connection_uid,
                operation_id=operation_id,
                idempotency_key=idempotency_key,
                object_keys=object_keys,
                schedule_at_ms=schedule_ms,
                schedule_timezone=schedule_tz,
            )
        receipt = {
            "actual_operation_id": operation_id,
            "upload_assets": uploaded_assets,
            "compatibility_media": compatibility,
            "envelope": envelope,
            "envelope_sha256": hashlib.sha256(json.dumps(
                envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")).hexdigest(),
        }
        self._submission_receipts[idempotency_key] = receipt
        payload = self._submit_envelope(envelope, operation_id)
        job = (payload.get("data") or {}).get("job") or {}
        job_id = job.get("job_id") or _deep_get(payload, "data.job_id")
        if not job_id:
            raise CreatOKResultUnknownError("CreatOK submit 未返回 job_id，结果未知，禁止自动重发")
        self._submission_receipts[idempotency_key] = {
            **receipt,
            "creatok_request_id": str(payload.get("request_id") or ""),
            "creatok_job_id": str(job_id),
            "provider_submission_id": str(job.get("provider_submission_id") or ""),
            "provider_initial_state": str(job.get("state") or job.get("status") or ""),
        }
        return f"{CREATOK_TASK_PREFIX}{job_id}"

    # ------------------------------------------------------------------ 查询

    @staticmethod
    def strip_task_prefix(task_id: str) -> str:
        text = str(task_id or "").strip()
        if text.startswith(CREATOK_TASK_PREFIX):
            return text[len(CREATOK_TASK_PREFIX):]
        return text

    def query_task_status(self, *, task_id: str, scheduled_for: datetime) -> PublishTaskStatus:
        job_id = self.strip_task_prefix(task_id)
        if not job_id:
            return PublishTaskStatus(state="pending", result="待执行")
        payload = self.cli.run([*CREATOK_CMD_JOB_GET, job_id])
        status = parse_task_status(payload)
        if status.state == "failed":
            # 失败明细需要可靠指标，失败时不回池以保证安全。
            pass
        return status

    def query_task_statuses(self, tasks: Iterable[Any]) -> Dict[str, PublishTaskStatus]:
        statuses: Dict[str, PublishTaskStatus] = {}
        for task in tasks:
            task_id = str(task.get("publish_task_id") or "")
            if not task_id.startswith(CREATOK_TASK_PREFIX):
                # 防串渠道：非本渠道任务一律返回 pending，绝不误判。
                statuses[task_id] = PublishTaskStatus(state="pending", result="待执行")
                continue
            scheduled_for = datetime.strptime(str(task.get("scheduled_for")), "%Y-%m-%d %H:%M:%S")
            statuses[task_id] = self.query_task_status(task_id=task_id, scheduled_for=scheduled_for)
        return statuses

    def reconcile_scheduled_task(self, context: Dict[str, Any]) -> str:
        """只读对账：用冻结的 idempotency_key 查询任务，返回 creatok:<job_id> 或空串。

        空串表示"未查到"，不等于"未创建"——调用方必须保持提交中占位。
        """
        idempotency_key = str(context.get("idempotency_key") or "").strip()
        if not idempotency_key:
            # 兜底：旧 context 没有幂等键时，尝试按 job 归属恢复（不重发）。
            return ""
        operation_id = str(context.get("operation_id") or "").strip() or uuid.uuid4().hex
        args = [*CREATOK_CMD_JOB_GET, "--idempotency-key", idempotency_key]
        if operation_id:
            args.extend(["--operation-id", operation_id])
        payload = self.cli.run(args)
        job_id = _deep_get(payload, "data.job_id") or _deep_get(payload, "data.job.job_id") or _deep_get(payload, "data.id")
        if not job_id:
            return ""
        return f"{CREATOK_TASK_PREFIX}{job_id}"

    def terminate_task(self, *, task_id: str) -> PublishTaskStatus:
        # CLI v0.14.0 的 publish job 组只有 submit/get/retry，无公开取消命令；
        # retry 是显式重发，不属于本地取消语义，不得自动调用。
        raise NotImplementedError("CreatOK CLI 当前未提供公开取消命令，不执行本地取消语义")
