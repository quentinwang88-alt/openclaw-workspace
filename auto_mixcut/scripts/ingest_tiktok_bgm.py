#!/usr/bin/env python3
"""Extract TikTok post audio into the existing Feishu BGM library.

Only records with a ``TK来源链接`` and ``提取状态=待提取`` are processed.
Records with an empty extraction status are also eligible when no audio file
has been attached yet. The extracted track is always kept license-restricted
until a human confirms the usage rights in the existing authorization fields.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List


WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
REPO_ROOT = Path(__file__).resolve().parents[1]
DOWNLOADER = WORKSPACE / "skills" / "xiaohongshu-video-downloader" / "scripts" / "download_xhs_video.py"
DEFAULT_TABLE_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "IFa5w98VBif8j7kIitIcLaqLncb?table=tblgdVFb6GDSPW3E&view=vewTfDXXBH"
)
DEFAULT_CHROME_CDP_URL = "http://127.0.0.1:9222"

sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402
from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.domain.markets import canonical_market  # noqa: E402
from auto_mixcut.skills.bgm_audio_analysis_skill import analyze_audio_file  # noqa: E402
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill  # noqa: E402


STATUS_PENDING = "待提取"
STATUS_PROCESSING = "处理中"
STATUS_DONE = "已完成"
STATUS_REVIEW = "需人工处理"
STATUS_FAILED = "失败"


@dataclass(frozen=True)
class IngestResult:
    record_id: str
    status: str
    item_id: str = ""
    file_name: str = ""
    vocal_type: str = ""
    error: str = ""

    def to_dict(self) -> Dict[str, str]:
        return {
            "record_id": self.record_id,
            "status": self.status,
            "item_id": self.item_id,
            "file_name": self.file_name,
            "vocal_type": self.vocal_type,
            "error": self.error,
        }


def text_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ("link", "url", "text", "name", "value"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
        return ""
    if isinstance(value, list):
        return ", ".join(item for item in (text_value(v) for v in value) if item)
    return str(value).strip()


def bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return text_value(value).lower() in {"1", "true", "yes", "y", "是", "已勾选"}


def sync_country_preferences(records: Iterable[Any]) -> Dict[str, int]:
    """Mirror lightweight Feishu selection preferences into the recommendation DB."""
    ctx = build_context()
    init = RDSRepositorySkill(ctx).init_db()
    if not init.success:
        raise RuntimeError(init.message or "BGM 数据库初始化失败")

    updated = 0
    missing = 0
    for record in records:
        fields = dict(record.fields)
        bgm_id = text_value(fields.get("BGM编号"))
        if not bgm_id:
            continue
        if not ctx.repo.get("bgm_tracks", "bgm_id", bgm_id):
            missing += 1
            continue
        ctx.repo.update(
            "bgm_tracks",
            "bgm_id",
            bgm_id,
            {
                "country": canonical_market(text_value(fields.get("国家"))),
                "priority_use": 1 if bool_value(fields.get("是否优先使用")) else 0,
            },
        )
        updated += 1
    return {"updated": updated, "missing": missing}


def should_process(fields: Dict[str, Any], *, force: bool = False) -> bool:
    if not text_value(fields.get("TK来源链接")):
        return False
    if force:
        return True
    status = text_value(fields.get("提取状态"))
    if status == STATUS_PENDING:
        return True
    return not status and not fields.get("音频文件")


def make_bgm_id(item_id: str, source_url: str) -> str:
    identity = item_id if item_id and item_id != "direct" else hashlib.sha256(source_url.encode("utf-8")).hexdigest()[:16]
    return f"BGM_TK_{identity}"[:64]


def safe_track_name(title: Any, item_id: str) -> str:
    value = text_value(title).replace("\n", " ").strip()
    if value:
        return value[:120]
    return f"TikTok {item_id or 'BGM'}"


def find_binary(name: str) -> str:
    candidates = [shutil.which(name), f"/Users/likeu3/.local/bin/{name}"]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return str(candidate)
    raise RuntimeError(f"缺少 {name}，无法处理音频")


def parse_downloader_json(stdout: str) -> Dict[str, Any]:
    stdout = stdout.strip()
    if not stdout:
        raise RuntimeError("下载器未返回结果")
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("下载器返回了无法解析的结果") from exc
    if not isinstance(payload, dict) or not payload.get("ok"):
        raise RuntimeError("下载器未成功下载 TikTok 视频")
    return payload


def download_tiktok_video(source_url: str, output_path: Path, cookie_file: str = "") -> Dict[str, Any]:
    if not DOWNLOADER.exists():
        raise RuntimeError(f"TikTok 下载器不存在: {DOWNLOADER}")
    metadata_cmd = [
        sys.executable,
        str(DOWNLOADER),
        source_url,
        "--print-json",
        "--metadata-only",
    ]
    if cookie_file:
        metadata_cmd.extend(["--cookie-file", cookie_file])
    metadata_proc = subprocess.run(metadata_cmd, text=True, capture_output=True, timeout=90)
    if metadata_proc.returncode == 0:
        metadata = parse_downloader_json(metadata_proc.stdout)
        music = metadata.get("music")
        if isinstance(music, dict) and text_value(music.get("play_url")):
            return metadata

    browser_metadata_cmd = list(metadata_cmd)
    browser_metadata_cmd.extend(["--cdp-url", DEFAULT_CHROME_CDP_URL])
    browser_metadata_proc = subprocess.run(browser_metadata_cmd, text=True, capture_output=True, timeout=90)
    if browser_metadata_proc.returncode == 0:
        browser_metadata = parse_downloader_json(browser_metadata_proc.stdout)
        music = browser_metadata.get("music")
        if isinstance(music, dict) and text_value(music.get("play_url")):
            return browser_metadata

    cmd = [
        sys.executable,
        str(DOWNLOADER),
        source_url,
        "--output",
        str(output_path),
        "--print-json",
    ]
    if cookie_file:
        cmd.extend(["--cookie-file", cookie_file])
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=180)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "TikTok 视频下载失败").strip()
        raise RuntimeError(detail[-800:])
    return parse_downloader_json(proc.stdout)


def normalize_audio(source_path: Path, audio_path: Path) -> None:
    ffmpeg = find_binary("ffmpeg")
    cmd = [
        ffmpeg,
        "-y",
        "-v",
        "error",
        "-i",
        str(source_path),
        "-map",
        "0:a:0",
        "-vn",
        "-af",
        "loudnorm=I=-16:LRA=11:TP=-1.5",
        "-ar",
        "48000",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        str(audio_path),
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=180)
    if proc.returncode != 0 or not audio_path.exists() or audio_path.stat().st_size <= 0:
        detail = (proc.stderr or "源视频没有可提取的音轨").strip()
        raise RuntimeError(detail[-800:])


def download_music_asset(url: str, output_path: Path) -> None:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            ),
            "Referer": "https://www.tiktok.com/",
            "Accept": "*/*",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response, output_path.open("wb") as handle:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            handle.write(chunk)
    if not output_path.exists() or output_path.stat().st_size <= 0:
        raise RuntimeError("TikTok 音乐资产下载结果为空")


def select_audio_source(metadata: Dict[str, Any], video_path: Path, tmp_dir: Path) -> tuple[Path, bool, str]:
    """Prefer TikTok's separate music asset; fall back to the mixed video audio."""
    music = metadata.get("music")
    if isinstance(music, dict) and text_value(music.get("play_url")):
        music_path = tmp_dir / "tiktok_music_asset.bin"
        download_music_asset(text_value(music.get("play_url")), music_path)
        # User-created original sounds can contain speech/effects and need review.
        return music_path, bool(music.get("original")), text_value(music.get("title"))
    # The complete video audio can contain voice-over and environmental sound.
    return video_path, True, ""


def resolve_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise RuntimeError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def status_updates(
    *,
    fields: Dict[str, Any],
    attachment: Dict[str, Any],
    item_id: str,
    title: Any,
    vocal_type: str,
    needs_review: bool,
) -> Dict[str, Any]:
    updates: Dict[str, Any] = {
        "音频文件": [attachment],
        "提取状态": STATUS_REVIEW if needs_review else STATUS_DONE,
        "AI人声类型": {
            "instrumental": "纯音乐",
            "light_vocal": "轻人声",
            "vocal": "明显人声",
            "unknown": "未知",
        }.get(vocal_type, "未知"),
    }
    if not text_value(fields.get("BGM编号")):
        updates["BGM编号"] = make_bgm_id(item_id, text_value(fields.get("TK来源链接")))
    if not text_value(fields.get("BGM名称")):
        updates["BGM名称"] = safe_track_name(title, item_id)
    return updates


def process_record(
    client: FeishuBitableClient,
    record: Any,
    *,
    cookie_file: str = "",
    dry_run: bool = False,
    downloader: Callable[[str, Path, str], Dict[str, Any]] = download_tiktok_video,
) -> IngestResult:
    fields = dict(record.fields)
    source_url = text_value(fields.get("TK来源链接"))
    if dry_run:
        return IngestResult(record_id=record.record_id, status=STATUS_PENDING)

    client.update_record_fields(record.record_id, {"提取状态": STATUS_PROCESSING})
    try:
        with tempfile.TemporaryDirectory(prefix="auto_mixcut_tk_bgm_") as tmp:
            tmp_dir = Path(tmp)
            video_path = tmp_dir / "source.mp4"
            metadata = downloader(source_url, video_path, cookie_file)
            item_id = text_value(metadata.get("item_id")) or "direct"
            audio_path = tmp_dir / f"{make_bgm_id(item_id, source_url)}.m4a"
            source_path, needs_review, music_title = select_audio_source(metadata, video_path, tmp_dir)
            normalize_audio(source_path, audio_path)
            analysis = analyze_audio_file(audio_path)
            vocal_type = text_value(analysis["audio_suggested_tags"].get("vocal_type")) or "unknown"
            content = audio_path.read_bytes()
            attachment = client.upload_attachment(
                content=content,
                file_name=audio_path.name,
                content_type=mimetypes.guess_type(audio_path.name)[0] or "audio/mp4",
                size=len(content),
                parent_type="bitable_file",
            )
            updates = status_updates(
                fields=fields,
                attachment=attachment,
                item_id=item_id,
                title=music_title or metadata.get("title"),
                vocal_type=vocal_type,
                needs_review=needs_review,
            )
            client.update_record_fields(record.record_id, updates)
            return IngestResult(
                record_id=record.record_id,
                status=updates["提取状态"],
                item_id=item_id,
                file_name=audio_path.name,
                vocal_type=vocal_type,
            )
    except Exception as exc:
        try:
            client.update_record_fields(record.record_id, {"提取状态": STATUS_FAILED})
        except Exception:
            pass
        return IngestResult(record_id=record.record_id, status=STATUS_FAILED, error=str(exc))


def select_records(records: Iterable[Any], *, record_id: str = "", force: bool = False, limit: int = 0) -> List[Any]:
    selected = []
    for record in records:
        if record_id and record.record_id != record_id:
            continue
        if not should_process(dict(record.fields), force=force):
            continue
        selected.append(record)
        if limit and len(selected) >= limit:
            break
    return selected


def main() -> int:
    parser = argparse.ArgumentParser(description="从 TikTok 视频链接提取音轨并写入飞书 BGM 素材库")
    parser.add_argument("--feishu-url", default=DEFAULT_TABLE_URL)
    parser.add_argument("--record-id", default="", help="只处理指定飞书记录")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--cookie-file", default="", help="可选的 TikTok Cookie 文本文件")
    parser.add_argument("--force", action="store_true", help="忽略当前提取状态重新处理")
    parser.add_argument("--dry-run", action="store_true", help="只列出候选记录，不下载或回写")
    parser.add_argument("--preferences-only", action="store_true", help="只同步国家和优先使用设置，不提取音频")
    args = parser.parse_args()

    client = resolve_client(args.feishu_url)
    records = client.list_records(page_size=500)
    preference_sync: Dict[str, Any]
    if args.dry_run:
        preference_sync = {"skipped": True}
    else:
        try:
            preference_sync = sync_country_preferences(records)
        except Exception as exc:
            # Preference sync must not prevent pending TikTok links from being ingested.
            preference_sync = {"error": str(exc)}
    selected = [] if args.preferences_only else select_records(
        records,
        record_id=args.record_id,
        force=args.force,
        limit=max(0, args.limit),
    )
    results = [
        process_record(client, record, cookie_file=args.cookie_file, dry_run=args.dry_run)
        for record in selected
    ]
    payload = {
        "selected": len(selected),
        "completed": sum(item.status == STATUS_DONE for item in results),
        "review_required": sum(item.status == STATUS_REVIEW for item in results),
        "failed": sum(item.status == STATUS_FAILED for item in results),
        "preference_sync": preference_sync,
        "results": [item.to_dict() for item in results],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 1 if payload["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
