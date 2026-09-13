"""Probe the 1route image channel end to end (paid, but tiny).

Confirms three things before the channel is trusted in production:

1. credential + base URL reach the relay at all (plain text-to-image);
2. reference-image edits work in the configured edit mode (``multipart`` vs
   ``json``) — this is the shape OPV actually uses;
3. the returned image passes the same 9:16 QC gate the production chain runs.

Nothing touches Feishu, RDS or the publish queue. The key is only ever read
from the environment (never printed, never written).

Run (from packages/organic_photo_video):

  export OPV_ONEROUTE_API_KEY="sk-..."   # or keep it in the workspace .env
  /usr/bin/python3 scripts/probe_oneroute_channel.py            # both probes
  /usr/bin/python3 scripts/probe_oneroute_channel.py --generate # text-to-image only
  /usr/bin/python3 scripts/probe_oneroute_channel.py --edit     # reference edits only

Exit code 0 = the probed path works; 1 = it does not (details printed).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
sys.path.insert(0, str(WORKSPACE_ROOT))
from workspace_support import load_repo_env  # noqa: E402

load_repo_env()
# PACKAGE_ROOT must stay ahead of WORKSPACE_ROOT so "config"/"services"
# resolve to this package, not to workspace-root lookalikes.
sys.path.insert(0, str(PACKAGE_ROOT))

from services.image_generator import (  # noqa: E402
    OneRouteImageGenerator,
    ShotGenerationRequest,
    channel_health_report,
    prepare_sunburst_primary_reference,
    read_image_dimensions,
)


def build_request(output_dir: str, prompt: str) -> ShotGenerationRequest:
    return ShotGenerationRequest(
        task_id="probe_oneroute",
        slot_index=1,
        slot_role="hero",
        shot_version=1,
        plan_shot={"slot_index": 1, "slot_role": "hero", "purpose": "channel probe"},
        product={},
        persona_snapshot={},
        look_snapshot={},
        scene_snapshot={},
        output_dir=output_dir,
        prompt_override=prompt,
    )


def make_reference(path: Path) -> Path:
    """A tiny square 9:16-mismatched reference, so canvas normalization is exercised."""
    from PIL import Image

    Image.new("RGB", (480, 480), (208, 190, 176)).save(path, format="PNG")
    return path


def report(label: str, outcome, *, expect_ok: bool) -> bool:
    print(f"\n=== {label} ===")
    print(f"  ok         : {outcome.ok}")
    print(f"  provider   : {outcome.provider}")
    print(f"  model      : {outcome.model}")
    print(f"  request_id : {outcome.request_id}")
    if outcome.width and outcome.height:
        print(f"  size       : {outcome.width}x{outcome.height}")
    if outcome.image_path:
        print(f"  image      : {outcome.image_path}")
    if outcome.error:
        print(f"  error      : {outcome.error}")
        print(f"  error_kind : {outcome.error_kind or 'n/a'}")
    if outcome.raw:
        trimmed = {
            key: value for key, value in outcome.raw.items()
            if key not in {"response"}
        }
        print(f"  raw        : {json.dumps(trimmed, ensure_ascii=False, default=str)}")
        response = outcome.raw.get("response")
        if response:
            print(f"  response   : {json.dumps(response, ensure_ascii=False, default=str)[:300]}")
    if outcome.raw.get("model_attempts"):
        print(f"  ladder     : {json.dumps(outcome.raw['model_attempts'], ensure_ascii=False)}")
    return outcome.ok if expect_ok else not outcome.ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", action="store_true", help="只探文本生图")
    parser.add_argument("--edit", action="store_true", help="只探参考图改图")
    parser.add_argument(
        "--edit-mode", choices=("multipart", "json"), default="",
        help="覆盖 OPV_ONEROUTE_EDIT_MODE，用于试出中转站接受的形态",
    )
    parser.add_argument("--model", default="", help="覆盖默认模型")
    args = parser.parse_args()
    run_generate = args.generate or not args.edit
    run_edit = args.edit or not args.generate

    generator = OneRouteImageGenerator(
        model=args.model or "",
        edit_mode=args.edit_mode or "",
    )
    print("1route channel probe")
    print(f"  base_url   : {generator.base_url}")
    print(f"  ladder     : {generator.model_ladder()}")
    print(f"  edit_mode  : {generator.edit_mode} / field={generator.json_reference_field}")
    print(f"  timeout    : {generator.timeout}s")
    if not generator.api_key:
        print("\n[FAIL] OPV_ONEROUTE_API_KEY 未配置：")
        print("       在 workspace 根 .env 写入一行 OPV_ONEROUTE_API_KEY=...")
        return 1
    print("  api_key    : configured (未回显)")

    results = []
    with tempfile.TemporaryDirectory(prefix="oneroute_probe_") as tmp:
        if run_generate:
            outcome = generator.generate_shot(
                build_request(
                    tmp,
                    "一张干净的纯灰背景产品静物照：一只白色陶瓷马克杯，柔和自然光，"
                    "真实材质细节，竖屏 9:16，无文字无水印。",
                )
            )
            results.append(("text-to-image", report("A. 文本生图", outcome, expect_ok=True)))

        if run_edit:
            reference = make_reference(Path(tmp) / "probe_reference.png")
            request = build_request(
                tmp,
                "以输入参考图为准，生成一张竖屏 9:16 的真实穿搭创作者照片："
                "纯色浅灰背景，自然光，真实皮肤与衣物纹理，无文字无水印。",
            )
            request.product = {"reference_images": [str(reference)]}
            outcome = generator.generate_shot(request)
            results.append(("reference-edit", report("B. 参考图改图", outcome, expect_ok=True)))
            if outcome.raw.get("sunburst_canvas_normalized"):
                print("  note       : Sunburst 主参考图已归一化到 1080x1920 画布")

    print("\n=== 通道健康 ===")
    print(json.dumps(channel_health_report(), ensure_ascii=False))

    failed = [name for name, ok in results if not ok]
    if failed:
        print(f"\n[FAIL] 未通过：{', '.join(failed)}")
        print("排查建议：")
        print("  - 缺少 OPV_ONEROUTE_API_KEY → 写进 workspace 根 .env")
        print("  - 401/403 → 密钥无效或额度归属不对")
        print("  - 404 on /v1/images/edits → 该中转站不吃 multipart，"
              "改用 --edit-mode json 再试")
        print("  - 响应无图片字段 → 用 raw.response 看实际返回结构，"
              "必要时调整 OPV_ONEROUTE_JSON_REFERENCE_FIELD")
        return 1
    print("\n[OK] 1route 通道探活通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
