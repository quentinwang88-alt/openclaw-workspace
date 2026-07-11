#!/usr/bin/env python3
"""Download a Xiaohongshu or TikTok video from a post URL or direct video URL."""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)
XHS_REFERER = "https://www.xiaohongshu.com/"
TIKTOK_REFERER = "https://www.tiktok.com/"


@dataclass
class Candidate:
    url: str
    label: str
    score: int


def build_headers(cookie: str | None = None, referer: str = XHS_REFERER) -> dict[str, str]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": referer,
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def request_bytes(url: str, headers: dict[str, str], timeout: int = 30) -> bytes:
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def request_text(url: str, headers: dict[str, str]) -> str:
    data = request_bytes(url, headers)
    return data.decode("utf-8", errors="replace")


def request_text_and_url(url: str, headers: dict[str, str]) -> tuple[str, str]:
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", errors="replace"), response.geturl()


def detect_platform(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if ".mp4" in url:
        return "direct"
    if "tiktok.com" in host or "tiktokcdn" in host or "ttwstatic" in host or "byteoversea" in host:
        return "tiktok"
    if "xiaohongshu.com" in host or "xhslink.com" in host or "xhscdn.com" in host:
        return "xhs"
    if "/video/" in path and re.search(r"/video/\d+", path):
        return "tiktok"
    return "xhs"


def normalize_page_text(text: str) -> str:
    text = html.unescape(text)
    replacements = {
        "\\u002F": "/",
        "\\/": "/",
        "\\u0026": "&",
        "\\u003D": "=",
        "\\u003F": "?",
        '\\"': '"',
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return text


def decode_url(value: str) -> str:
    value = html.unescape(value)
    value = value.replace("\\u002F", "/").replace("\\/", "/").replace("\\u0026", "&")
    value = value.replace("\\u003D", "=").replace("\\u003F", "?")
    return value.rstrip("\\")


def maybe_json_string(value: str) -> str:
    try:
        return json.loads(f'"{value}"')
    except json.JSONDecodeError:
        return value


def extract_note_id(url: str, text: str) -> str:
    parsed = urllib.parse.urlparse(url)
    for part in reversed([p for p in parsed.path.split("/") if p]):
        if re.fullmatch(r"[0-9a-fA-F]{16,32}", part):
            return part
    match = re.search(r'"noteId"\s*:\s*"([^"]+)"', text)
    if match:
        return maybe_json_string(match.group(1))
    return str(int(time.time()))


def extract_tiktok_id(url: str, text: str) -> str:
    parsed = urllib.parse.urlparse(url)
    match = re.search(r"/video/(\d+)", parsed.path)
    if match:
        return match.group(1)
    for pattern in (r'"id"\s*:\s*"(\d{12,24})"', r'"item_id"\s*:\s*(?:"|\u0022)?(\d{12,24})'):
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return str(int(time.time()))


def extract_xhs_title(text: str) -> str | None:
    preferred_patterns = [
        r'"type"\s*:\s*"video"\s*,\s*"title"\s*:\s*"([^"]{1,240})"',
        r'"noteId"\s*:\s*"[^"]+"\s*,.*?"title"\s*:\s*"([^"]{1,240})"',
    ]
    for pattern in preferred_patterns:
        match = re.search(pattern, text, flags=re.DOTALL)
        if match:
            title = maybe_json_string(match.group(1)).strip()
            if title and not is_generic_title(title):
                return title

    for key in ("title", "desc"):
        for match in re.finditer(rf'"{key}"\s*:\s*"([^"]{{1,240}})"', text):
            title = maybe_json_string(match.group(1)).strip()
            if title and not is_generic_title(title):
                return title
    return None


def extract_tiktok_title(data: object, text: str, item_id: str) -> str | None:
    for item in walk_values(data):
        if not isinstance(item, dict):
            continue
        if str(item.get("id", "")) == item_id or str(item.get("itemId", "")) == item_id:
            value = item.get("desc") or item.get("title")
            if isinstance(value, str):
                title = value.strip()
                if title and len(title) > 3 and not is_generic_title(title):
                    return title
        if {"author", "video", "desc"}.issubset(item.keys()):
            value = item.get("desc")
            if isinstance(value, str):
                title = value.strip()
                if title and len(title) > 3 and not is_generic_title(title):
                    return title

    for item in walk_values(data):
        if isinstance(item, dict):
            for key in ("desc", "title"):
                value = item.get(key)
                if isinstance(value, str):
                    title = value.strip()
                    if title and len(title) > 3 and not is_generic_title(title):
                        return title
    for pattern in (r'"desc"\s*:\s*"([^"]{1,500})"', r'"title"\s*:\s*"([^"]{1,240})"'):
        match = re.search(pattern, text)
        if match:
            title = maybe_json_string(match.group(1)).strip()
            if title and not is_generic_title(title):
                return title
    return None


def is_generic_title(value: str) -> bool:
    generic_fragments = (
        "小红书_沪ICP备",
        "搜索小红书",
        "小红书网页版",
        "TikTok - Make Your Day",
        "Download TikTok",
    )
    return any(fragment in value for fragment in generic_fragments)


def sanitize_filename(value: str, max_len: int = 80) -> str:
    value = re.sub(r"[\\/:*?\"<>|\r\n\t]+", "_", value).strip(" ._")
    value = re.sub(r"\s+", " ", value)
    return (value[:max_len].strip() or "xiaohongshu-video")


def score_candidate(url: str, label: str) -> int:
    score = 0
    if label == "hd_screencast_stream":
        score += 100
    elif label in {"masterUrl", "master_url", "default_screencast_stream"}:
        score += 70
    elif label == "mp4":
        score += 30
    elif label == "tiktok_downloadAddr":
        score += 95
    elif label == "tiktok_playAddr":
        score += 90
    elif label == "tiktok_bitrate":
        score += 85
    elif label.startswith("tiktok_"):
        score += 75
    if "sns-video" in url:
        score += 15
    if "tiktokcdn" in url or "byteoversea" in url:
        score += 15
    if "sign=" in url:
        score += 10
    if "sns-bak" in url:
        score -= 10
    return score


def add_candidate(
    candidates: list[Candidate],
    seen: set[str],
    url: str,
    label: str,
    *,
    require_mp4: bool = True,
) -> None:
    url = decode_url(url)
    if not url.startswith(("http://", "https://")):
        return
    if require_mp4 and ".mp4" not in url:
        return
    if url in seen:
        return
    seen.add(url)
    candidates.append(Candidate(url=url, label=label, score=score_candidate(url, label)))


def extract_xhs_candidates(text: str) -> list[Candidate]:
    normalized = normalize_page_text(text)
    candidates: list[Candidate] = []
    seen: set[str] = set()

    keyed = re.compile(
        r'"(?P<label>hd_screencast_stream|default_screencast_stream|masterUrl|master_url)"\s*:\s*"(?P<url>https?://[^"]+?\.mp4(?:\?[^"]*)?)"'
    )
    for match in keyed.finditer(normalized):
        add_candidate(candidates, seen, match.group("url"), match.group("label"))

    loose = re.compile(r"https?://[^\"'<>\s]+?\.mp4(?:\?[^\"'<>\s]+)?")
    for match in loose.finditer(normalized):
        add_candidate(candidates, seen, match.group(0), "mp4")

    candidates.sort(key=lambda item: item.score, reverse=True)
    return candidates


def walk_values(value: object):
    yield value
    if isinstance(value, dict):
        for child in value.values():
            yield from walk_values(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_values(child)


def extract_embedded_json(text: str) -> list[object]:
    blocks: list[object] = []
    patterns = [
        r'<script[^>]+id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>',
        r'<script[^>]+id="SIGI_STATE"[^>]*>(.*?)</script>',
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.DOTALL):
            raw = html.unescape(match.group(1)).strip()
            if not raw:
                continue
            try:
                blocks.append(json.loads(raw))
            except json.JSONDecodeError:
                pass
    return blocks


def looks_like_tiktok_video_url(url: str) -> bool:
    lowered = url.lower()
    if "mime_type=video_mp4" in lowered or "mime_type=video" in lowered:
        return True
    if "/video/tos/" in lowered or "/obj/tos-" in lowered:
        return True
    if "is_play_url=1" in lowered and ("tiktok" in lowered or "byte" in lowered):
        return True
    return False


def add_tiktok_url(candidates: list[Candidate], seen: set[str], url: str, label: str) -> None:
    url = decode_url(url)
    if looks_like_tiktok_video_url(url):
        add_candidate(candidates, seen, url, label, require_mp4=False)


def collect_tiktok_candidates_from_json(value: object, candidates: list[Candidate], seen: set[str], parent_key: str = "") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            key_label = f"{parent_key}.{key}" if parent_key else key
            lower_key = key.lower()
            if isinstance(child, str):
                if lower_key in {"playaddr", "downloadaddr", "playurl", "url"}:
                    label = "tiktok_downloadAddr" if lower_key == "downloadaddr" else "tiktok_playAddr"
                    add_tiktok_url(candidates, seen, child, label)
                elif child.startswith(("http://", "https://")):
                    add_tiktok_url(candidates, seen, child, f"tiktok_{key}")
            elif isinstance(child, list) and lower_key in {"urllist", "url_list", "urls"}:
                for item in child:
                    if isinstance(item, str):
                        label = "tiktok_bitrate" if "bitrate" in parent_key.lower() else "tiktok_playAddr"
                        add_tiktok_url(candidates, seen, item, label)
            collect_tiktok_candidates_from_json(child, candidates, seen, key_label)
    elif isinstance(value, list):
        for child in value:
            collect_tiktok_candidates_from_json(child, candidates, seen, parent_key)


def extract_tiktok_candidates(text: str) -> tuple[list[Candidate], list[object]]:
    normalized = normalize_page_text(text)
    candidates: list[Candidate] = []
    seen: set[str] = set()
    json_blocks = extract_embedded_json(text)
    for block in json_blocks:
        collect_tiktok_candidates_from_json(block, candidates, seen)

    keyed = re.compile(
        r'"(?P<label>playAddr|downloadAddr|playUrl)"\s*:\s*"(?P<url>https?://[^"]+)"'
    )
    for match in keyed.finditer(normalized):
        add_tiktok_url(candidates, seen, match.group("url"), f"tiktok_{match.group('label')}")

    loose = re.compile(r"https?://[^\"'<>\s]+")
    for match in loose.finditer(normalized):
        add_tiktok_url(candidates, seen, match.group(0), "tiktok_url")

    candidates.sort(key=lambda item: item.score, reverse=True)
    return candidates, json_blocks


def load_cookie(args: argparse.Namespace) -> str | None:
    if args.cookie:
        return args.cookie
    if args.cookie_file:
        return Path(args.cookie_file).read_text(encoding="utf-8").strip()
    return None


def download_file(url: str, path: Path, headers: dict[str, str]) -> int:
    tmp_path = path.with_suffix(path.suffix + ".part")
    request_headers = dict(headers)
    request_headers["Accept"] = "*/*"
    request = urllib.request.Request(url, headers=request_headers)
    total = 0
    with urllib.request.urlopen(request, timeout=60) as response, tmp_path.open("wb") as handle:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            handle.write(chunk)
            total += len(chunk)
    if total <= 0:
        raise RuntimeError("downloaded file is empty")
    os.replace(tmp_path, path)
    return total


def find_ffprobe() -> str | None:
    for name in ("ffprobe", "/Users/likeu3/.local/bin/ffprobe"):
        found = shutil.which(name) if "/" not in name else (name if Path(name).exists() else None)
        if found:
            return found
    return None


def probe_video(path: Path) -> dict[str, object]:
    ffprobe = find_ffprobe()
    if not ffprobe:
        return {}
    cmd = [
        ffprobe,
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=codec_name,width,height,duration",
        "-show_entries",
        "format=duration,size",
        "-of",
        "json",
        str(path),
    ]
    result = subprocess.run(cmd, text=True, capture_output=True, check=True)
    return json.loads(result.stdout or "{}")


def probe_has_video(probe: dict[str, object]) -> bool:
    streams = probe.get("streams")
    return isinstance(streams, list) and len(streams) > 0


def output_path_for(args: argparse.Namespace, item_id: str, title: str | None, platform: str) -> Path:
    if args.output:
        return Path(args.output).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    if args.title_filename and title:
        stem = sanitize_filename(f"{title}_{item_id}")
    else:
        prefix = "tk" if platform == "tiktok" else "xhs"
        stem = f"{prefix}_{item_id}"
    return out_dir / f"{stem}.mp4"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download a Xiaohongshu or TikTok video.")
    parser.add_argument("url", help="Xiaohongshu/TikTok post URL or direct video URL")
    parser.add_argument("--out-dir", default="~/Downloads", help="Output directory when --output is not set")
    parser.add_argument("--output", help="Exact output MP4 path")
    parser.add_argument("--cookie", help="Optional Cookie header for pages requiring login")
    parser.add_argument("--cookie-file", help="Path to a file containing a Cookie header")
    parser.add_argument("--title-filename", action="store_true", help="Include note title in generated filename")
    parser.add_argument("--print-json", action="store_true", help="Print machine-readable JSON result")
    parser.add_argument("--no-verify", action="store_true", help="Skip ffprobe validation")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cookie = load_cookie(args)
    platform = detect_platform(args.url)
    referer = TIKTOK_REFERER if platform == "tiktok" else XHS_REFERER
    headers = build_headers(cookie, referer=referer)

    page_text = ""
    title = None
    item_id = "direct"

    if ".mp4" in args.url:
        candidates = [Candidate(url=args.url, label="direct", score=999)]
    else:
        page_text, final_url = request_text_and_url(args.url, headers)
        normalized = normalize_page_text(page_text)
        if platform == "tiktok":
            item_id = extract_tiktok_id(final_url, normalized)
            candidates, json_blocks = extract_tiktok_candidates(page_text)
            title = extract_tiktok_title(json_blocks, normalized, item_id)
        else:
            item_id = extract_note_id(final_url, normalized)
            title = extract_xhs_title(normalized)
            candidates = extract_xhs_candidates(page_text)

    if not candidates:
        raise SystemExit(f"No downloadable video stream was found in the {platform} page.")

    out_path = output_path_for(args, item_id, title, platform)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    errors: list[str] = []
    selected: Candidate | None = None
    size = 0
    probe: dict[str, object] = {}
    for candidate in candidates:
        try:
            size = download_file(candidate.url, out_path, headers)
            if not args.no_verify:
                probe = probe_video(out_path)
                if not probe_has_video(probe):
                    try:
                        out_path.unlink()
                    except FileNotFoundError:
                        pass
                    raise RuntimeError("downloaded response did not contain a video stream")
            selected = candidate
            break
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, RuntimeError) as exc:
            errors.append(f"{candidate.label}: {exc}")

    if selected is None:
        raise SystemExit("All video stream candidates failed:\n" + "\n".join(errors))

    if args.no_verify:
        probe = {}
    result = {
        "ok": True,
        "platform": platform,
        "path": str(out_path),
        "size": size,
        "item_id": item_id,
        "note_id": item_id if platform == "xhs" else None,
        "title": title,
        "selected_label": selected.label,
        "selected_url": selected.url,
        "candidate_count": len(candidates),
        "probe": probe,
    }

    if args.print_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"Downloaded: {out_path}")
        print(f"Size: {size} bytes")
        if title:
            print(f"Title: {title}")
        if probe:
            print(json.dumps(probe, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
