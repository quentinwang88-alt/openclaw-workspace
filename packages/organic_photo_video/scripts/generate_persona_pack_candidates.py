#!/usr/bin/env python3
"""Generate candidate persona packs for TH human-scene production (one-shot ops).

Paid CreatOK CLI calls; candidates are NOT production assets until the user
picks one and it is imported into persona_templates (TH persona realism
handoff 6.1: scanner must never auto-promote candidates).

Identity chaining: the front-face image is generated text-only, then reused
as --ref for the other roles so all four images share one identity.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
for value in (str(WORKSPACE_ROOT),):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

ROLES = [
    ("01_face_front_neutral", "3:4", []),
    ("02_face_three_quarter", "3:4", ["01_face_front_neutral"]),
    ("03_body_full_neutral", "9:16", ["01_face_front_neutral"]),
    ("04_body_full_motion", "9:16", ["01_face_front_neutral", "03_body_full_neutral"]),
]

REALISM_CLAUSE = (
    "真实手机摄影质感：自然光线，真实皮肤纹理和毛孔，保留自然不对称；"
    "无美颜磨皮、无娃娃眼、无塑料皮肤；头颈保持水平不向肩膀倾斜；"
    "表情自然放松，不刻意微笑；像同行朋友用手机随手拍的照片，不是影棚写真。"
)

ROLE_PROMPTS = {
    "01_face_front_neutral": (
        "半身胸像：正面平视镜头，头颈垂直水平，表情中性放松，嘴唇自然闭合，"
        "眼睛自然大小，眉毛自然；肩部放松；浅暖色纯色墙面背景。"
    ),
    "02_face_three_quarter": (
        "半身胸像：脸部转向三分之四侧面角度，头颈保持水平不歪头，"
        "与参考图完全相同的同一人物，表情自然放松，光线与参考图一致。"
    ),
    "03_body_full_neutral": (
        "完整全身照：自然直立站姿，重心落在一条腿上另一条略向前，"
        "肩膀放松，手臂自然垂放微弯；穿简单修身白T恤、浅色牛仔裤和小白鞋，"
        "全身比例从头顶到鞋底完整可见；室内自然光或街边背景。"
    ),
    "04_body_full_motion": (
        "完整全身行走抓拍：自然行走的中间时刻，手臂随步伐自然摆动，"
        "视线看向前方行进方向，与参考图完全相同的同一人物和同一套穿搭，"
        "头颈顺着行走方向保持水平；真实街道背景，可以有轻微动态感。"
    ),
}


def generate_role(binary: str, out_dir: Path, candidate: str, role: str,
                  aspect: str, refs: list[str], base_desc: str,
                  quality: str, poll_timeout: int) -> dict:
    output_name = f"{role}.png"
    target = out_dir / output_name
    if target.is_file() and target.stat().st_size > 0:
        return {"file": str(target), "skipped": True}
    run_dir = out_dir / f"_{role}_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    prompt = (
        f"生成一张真实照片：{base_desc}。{ROLE_PROMPTS[role]}{REALISM_CLAUSE}"
    )
    options = {
        "model": "gpt-image-2-official", "resolution": "1K",
        "quality": quality, "n": 1, "aspect_ratio": aspect,
    }
    command = [
        binary, "image", "generate", "--prompt", prompt,
        "--options", json.dumps(options), "--out", str(run_dir),
        "--timeout", str(poll_timeout),
    ]
    if refs:
        command += ["--ref", ",".join(str(out_dir / f"{name}.png") for name in refs)]
    completed = subprocess.run(command, capture_output=True, text=True,
                               timeout=poll_timeout + 120)
    envelope = {}
    try:
        envelope = json.loads(completed.stdout)
    except json.JSONDecodeError:
        raise SystemExit(f"{candidate}/{role}: CLI 输出非 JSON：{completed.stdout[:300]} {completed.stderr[:300]}")
    if not envelope.get("ok"):
        raise SystemExit(f"{candidate}/{role}: 生成失败 {envelope.get('error')}")
    images = (envelope.get("data") or {}).get("result") or {}
    urls = images.get("images") or []
    if not urls or not urls[0].get("url"):
        raise SystemExit(f"{candidate}/{role}: envelope 无图片 URL")
    import urllib.request
    with urllib.request.urlopen(urls[0]["url"], timeout=120) as response:  # noqa: S310
        target.write_bytes(response.read())
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    return {
        "file": str(target), "sha256": digest, "task_id": envelope.get("task_id"),
        "url": urls[0]["url"].split("?", 1)[0], "refs": refs,
        "prompt": prompt, "options": options,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", required=True, help="候选编号，如 A")
    parser.add_argument("--base-desc", required=True, help="人物身份描述（同一候选四张共用）")
    parser.add_argument("--out-root", default=str(
        Path.home() / ".openclaw/workspace/shared/data/persona_templates/_candidates_20260907"
    ))
    parser.add_argument("--quality", default="medium")
    parser.add_argument("--binary", default="creatok")
    parser.add_argument("--poll-timeout", type=int, default=300)
    args = parser.parse_args()

    if not os.environ.get("CREATOK_API_KEY"):
        raise SystemExit("CREATOK_API_KEY 未配置")
    out_dir = Path(args.out_root) / f"TH_APPAREL_REAL_01_{args.candidate}"
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "candidates_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.is_file() else {}

    for role, aspect, refs in ROLES:
        print(f"[{args.candidate}] generating {role} ...", flush=True)
        manifest[role] = generate_role(
            args.binary, out_dir, args.candidate, role, aspect, refs,
            args.base_desc, args.quality, args.poll_timeout,
        )
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=1),
                                 encoding="utf-8")
    print(json.dumps({k: v.get("file") for k, v in manifest.items()}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
