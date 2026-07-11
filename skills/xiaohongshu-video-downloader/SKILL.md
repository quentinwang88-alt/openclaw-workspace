---
name: xiaohongshu-video-downloader
description: Download videos from Xiaohongshu/RED and TikTok post links into local MP4 files. Use when the user gives a xiaohongshu.com/explore, xiaohongshu.com/discovery/item, xhslink.com, tiktok.com/@user/video, vm.tiktok.com, or direct CDN video URL and asks to download, save, fetch, archive, or prepare the video for later OpenClaw workflows.
---

# Xiaohongshu/TikTok Video Downloader

## Quick Start

Use the bundled downloader first; it auto-detects Xiaohongshu vs TikTok, parses page data, extracts video streams, downloads the best candidate, and verifies the resulting file with `ffprobe` when available.

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/xiaohongshu-video-downloader/scripts/download_xhs_video.py '<xiaohongshu-or-tiktok-url>'
```

Default output directory: `~/Downloads`.

Useful options:

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/xiaohongshu-video-downloader/scripts/download_xhs_video.py '<url>' --out-dir /path/to/dir
python3 /Users/likeu3/.openclaw/workspace/skills/xiaohongshu-video-downloader/scripts/download_xhs_video.py '<url>' --output /path/to/file.mp4
python3 /Users/likeu3/.openclaw/workspace/skills/xiaohongshu-video-downloader/scripts/download_xhs_video.py '<url>' --print-json
```

## Workflow

1. Run `scripts/download_xhs_video.py` with the user-provided link.
2. Prefer `--print-json` when another automation needs the path, platform, title, item id, duration, or dimensions.
3. If the downloader reports that no video URL was found, fetch the page in a real browser session and retry with a fresh copied link. Xiaohongshu `xsec_token` values and TikTok signed playback URLs can expire.
4. If TikTok returns subtitle/tiny placeholder responses, keep verification enabled; the script will skip non-video responses and try the next candidate.
5. If the page requires login or blocks server-side access, ask the user to open the note/post in their logged-in Chrome and provide a refreshed public URL, or manually save the direct video URL from browser devtools and pass that URL to the script.

## Notes

- The script uses only Python standard library modules.
- It sends browser-like headers and platform-specific referers.
- For Xiaohongshu, it tries HD/default/backup MP4 streams.
- For TikTok, it reads embedded page JSON and tries `downloadAddr`, `playAddr`, and bitrate stream URLs.
- It writes to a `.part` file first and only replaces the final MP4 after a successful download.
- When verification is enabled, it rejects downloads that do not contain a video stream.
- Respect copyright and platform terms; only download content the user is allowed to save or process.
