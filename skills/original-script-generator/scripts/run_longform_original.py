#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.longform.contracts import stable_id, validate_master_contract
from core.longform.audio import finalize_with_voiceover
from core.longform.assets import freeze_reference_assets
from core.longform.keyframes import build_keyframe_contracts
from core.longform.h3_gateway import H3Gateway, write_segment_request
from core.longform.media import extract_bridge_candidates, merge_segments, select_bridge_candidate
from core.longform.model import generate_master_contract
from core.longform.planner import compile_longform_plan
from core.longform.storage import DEFAULT_ASSET_ROOT, DEFAULT_DB_PATH, LongformStorage
from core.longform.source_adapter import add_supporting_scripts, source_from_complete_script
from core.longform.review import export_review_bundle
from core.longform.voiceover import (
    DEFAULT_MODEL_COMMAND, calibrate_longform_voiceover_with_edge,
    run_longform_voiceover,
)
from core.longform.workflow import run_to_final


def _json(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _segment_row(row: dict, segment_id: str) -> dict:
    for item in row.get("segments") or []:
        if item.get("segment_id") == segment_id:
            return item
    raise SystemExit(f"找不到片段{segment_id}")


def _ordered_segment_rows(row: dict) -> list[dict]:
    segments = list(row.get("segments") or [])
    if not 2 <= len(segments) <= 3:
        raise SystemExit("长视频任务必须包含2至3个有序片段")
    return segments


def _segment_position(row: dict, segment_id: str) -> int:
    for index, item in enumerate(_ordered_segment_rows(row)):
        if str(item.get("segment_id")) == segment_id:
            return index
    raise SystemExit(f"找不到片段{segment_id}")


def _bridge_candidate_root(asset_root: str, job_id: str,
                           previous_id: str, current_id: str) -> Path:
    return (
        Path(asset_root) / job_id / "bridge_candidates"
        / f"{previous_id}_to_{current_id}"
    )


def _boundary_to(plan: dict, target_segment_id: str) -> dict:
    for item in dict(plan.get("bridge_contract") or {}).get("boundaries") or []:
        if str(item.get("to_segment") or "") == target_segment_id:
            return dict(item)
    return {}


def _parse_planned_frames(values: list[str], legacy_end_frame: str = "") -> dict[str, str]:
    result: dict[str, str] = {}
    if legacy_end_frame:
        result["K1"] = legacy_end_frame
    for raw in values:
        key, separator, path = str(raw or "").partition("=")
        normalized = key.strip().upper().replace("_PLANNED", "")
        if not separator or normalized not in {"K1", "K2"} or not path.strip():
            raise SystemExit("--planned-frame 必须使用 K1=/绝对路径 或 K2=/绝对路径")
        result[normalized] = path.strip()
    return result


def _parse_scene_entries(values: list[str]) -> dict[str, str]:
    result = {}
    for raw in values:
        segment_id, separator, path = str(raw or "").partition("=")
        normalized = segment_id.strip().upper()
        if not separator or normalized not in {"B", "C"} or not path.strip():
            raise SystemExit("--scene-entry 必须使用 B=/绝对路径 或 C=/绝对路径")
        result[normalized] = path.strip()
    return result


def _segment_entry_reference_paths(keyframes: dict, k0_path: str) -> list[str]:
    """K0 owns composition; raw product/persona references correct identity drift."""

    result = [str(Path(k0_path).resolve())]
    manifest = dict(keyframes.get("frozen_reference_assets") or {})
    preferred_roles = {"PRODUCT_REFERENCE", "PERSONA_REFERENCE"}
    for item in manifest.get("assets") or []:
        if not isinstance(item, dict) or str(item.get("role") or "") not in preferred_roles:
            continue
        path = Path(str(item.get("local_path") or "")).expanduser()
        resolved = str(path.resolve()) if path.is_file() else ""
        if resolved and resolved not in result:
            result.append(resolved)
    return result


def _k0_reference_paths(keyframes: dict) -> list[str]:
    """Resolve frozen identity references for K0 with legacy-safe fallback."""

    manifest = dict(keyframes.get("frozen_reference_assets") or {})
    assets = [item for item in manifest.get("assets") or [] if isinstance(item, dict)]
    product = [item for item in assets if str(item.get("role") or "") == "PRODUCT_REFERENCE"]
    persona = [item for item in assets if str(item.get("role") or "") == "PERSONA_REFERENCE"]
    composite = [
        item for item in assets
        if str(item.get("role") or "") == "COMPOSITE_FIRST_FRAME"
    ]
    # A raw product image owns product appearance. The historical composite is
    # excluded when raw product evidence exists, otherwise it remains a soft
    # non-blocking fallback and may be paired with a persona identity image.
    ordered = product + persona if product else composite + persona
    result: list[str] = []
    for item in ordered:
        path = Path(str(item.get("local_path") or "")).expanduser()
        resolved = str(path.resolve()) if path.is_file() else ""
        if resolved and resolved not in result:
            result.append(resolved)
    return result


def _reference_mode_guidance(keyframes: dict) -> str:
    manifest = dict(keyframes.get("frozen_reference_assets") or {})
    mode = str(manifest.get("reference_mode") or "")
    if not mode:
        roles = {
            str(item.get("role") or "")
            for item in manifest.get("assets") or [] if isinstance(item, dict)
        }
        if "PRODUCT_REFERENCE" not in roles and "COMPOSITE_FIRST_FRAME" in roles:
            mode = "COMPOSITE_FALLBACK"
    if mode == "COMPOSITE_FALLBACK":
        return (
            "当前仅有历史合成首帧作为降级参考：它只提供构图和人物状态，不构成商品结构真值；"
            "未知的闭合件保持中性，不新增或强化拉链、纽扣、按扣等具体结构。\n"
        )
    return ""


def _register_initial_keyframes(storage: LongformStorage, row: dict, plan: dict,
                                job_id: str, k0_path: str,
                                planned_values: list[str], legacy_end_frame: str = "",
                                scene_entry_values: list[str] | None = None,
                                *, auto_generate_scene_entries: bool = False,
                                auto_generate_k0: bool = False,
                                asset_root: str = str(DEFAULT_ASSET_ROOT)) -> dict:
    keyframes = json.loads(row.get("keyframe_package_json") or "{}")
    if (not k0_path or not Path(k0_path).is_file()) and auto_generate_k0:
        from scripts.run_first_frame_tasks import _generate_image
        k0_contract = dict(keyframes.get("K0") or {})
        prompt = _reference_mode_guidance(keyframes) + str(
            k0_contract.get("prompt") or ""
        ).strip()
        if not prompt:
            raise SystemExit("缺少 K0 首帧提示词")
        output_dir = Path(asset_root) / job_id / "first_frame"
        output_dir.mkdir(parents=True, exist_ok=True)
        k0_path = str(_generate_image(
            prompt=prompt,
            reference_paths=_k0_reference_paths(keyframes),
            output_dir=output_dir,
            asset_id=stable_id("LFK0_", {"job_id": job_id, "prompt": prompt}),
        ))
    if not k0_path or not Path(k0_path).is_file():
        raise SystemExit("K0 文件不存在；请提供 --start-frame 或使用 --auto-generate-k0")
    planned = _parse_planned_frames(planned_values, legacy_end_frame)
    segments = list(plan.get("segments") or [])
    boundaries = list(dict(plan.get("bridge_contract") or {}).get("boundaries") or [])
    expected = [
        f"K{index}" for index, boundary in enumerate(boundaries, 1)
        if str(boundary.get("boundary_mode") or "CONTINUOUS") == "CONTINUOUS"
    ]
    missing = [key for key in expected if key not in planned]
    if missing:
        raise SystemExit("缺少计划桥接帧: " + ", ".join(f"{key}_PLANNED" for key in missing))
    resolved = {key: str(Path(value).resolve()) for key, value in planned.items()}
    missing_files = [key for key in expected if not Path(resolved[key]).is_file()]
    if missing_files:
        raise SystemExit("计划桥接帧文件不存在: " + ", ".join(missing_files))
    k0 = str(Path(k0_path).resolve())
    scene_entries = _parse_scene_entries(scene_entry_values or [])
    for boundary in boundaries:
        if str(boundary.get("boundary_mode") or "CONTINUOUS") != "DISCONTINUOUS_CUT":
            continue
        target_id = str(boundary.get("to_segment") or "").upper()
        path = scene_entries.get(target_id, "")
        if not path and auto_generate_scene_entries:
            from scripts.run_first_frame_tasks import _generate_image
            contract = dict(keyframes.get(f"S{target_id}_ENTRY") or {})
            prompt = str(contract.get("prompt") or "").strip()
            if not prompt:
                raise SystemExit(f"缺少片段{target_id}场景进入帧提示词")
            output_dir = Path(asset_root) / job_id / "scene_entries" / target_id
            output_dir.mkdir(parents=True, exist_ok=True)
            generated = _generate_image(
                prompt=prompt,
                reference_paths=_segment_entry_reference_paths(keyframes, k0),
                output_dir=output_dir,
                asset_id=stable_id("LFSE_", {"job_id": job_id, "segment": target_id, "prompt": prompt}),
            )
            path = str(generated)
        if not path or not Path(path).is_file():
            raise SystemExit(
                f"片段{target_id}需要独立片段进入帧；请提供 --scene-entry {target_id}=/路径，"
                "或使用 --auto-generate-scene-entry"
            )
        scene_entries[target_id] = str(Path(path).resolve())
    for index, segment in enumerate(segments):
        segment_id = str(segment["segment_id"])
        fields = {}
        if index == 0:
            fields.update(status="KEYFRAMES_READY", start_frame_path=k0)
        if index < len(segments) - 1:
            boundary = boundaries[index]
            if str(boundary.get("boundary_mode") or "CONTINUOUS") == "CONTINUOUS":
                fields["end_frame_path"] = resolved[f"K{index + 1}"]
        if index > 0 and segment_id in scene_entries:
            fields.update(status="BRIDGE_READY", start_frame_path=scene_entries[segment_id])
        storage.update_segment(job_id, segment_id, **fields)
    storage.update_job(job_id, "KEYFRAMES_READY")
    return {
        "job_id": job_id, "status": "KEYFRAMES_READY", "K0": k0,
        **{f"{key}_PLANNED": resolved[key] for key in expected},
        **{f"S{key}_ENTRY": value for key, value in scene_entries.items()},
    }


def _registered_h3_frames(row: dict, segment_id: str) -> tuple[str, str]:
    """Return only persisted keyframes after enforcing the paid-submit state."""

    segment = _segment_row(row, segment_id)
    position = _segment_position(row, segment_id)
    status = str(segment.get("status") or "")
    allowed = (
        {"KEYFRAMES_READY", "SUBMITTED"}
        if position == 0
        else {"BRIDGE_READY", "SUBMITTED"}
    )
    if status not in allowed:
        required = "KEYFRAMES_READY" if position == 0 else "BRIDGE_READY"
        raise SystemExit(
            f"片段{segment_id}当前状态为 {status or 'UNKNOWN'}，必须先达到 {required}"
        )
    start_frame = str(segment.get("start_frame_path") or "")
    needs_end_frame = str(segment.get("generation_mode") or "") == "first_last"
    end_frame = str(segment.get("end_frame_path") or "") if needs_end_frame else ""
    required_paths = [start_frame] + ([end_frame] if needs_end_frame else [])
    if any(not value or not Path(value).is_file() for value in required_paths):
        raise SystemExit(f"片段{segment_id}登记的关键帧缺失或文件不存在")
    return start_frame, end_frame


def _registered_output_video(row: dict, segment_id: str, provided: str = "") -> str:
    segment = _segment_row(row, segment_id)
    if str(segment.get("status") or "") != "READY":
        raise SystemExit(f"片段{segment_id}尚未 READY")
    registered = str(segment.get("output_video_path") or "")
    if not registered or not Path(registered).is_file():
        raise SystemExit(f"片段{segment_id}登记的视频文件不存在")
    if provided and Path(provided).resolve() != Path(registered).resolve():
        raise SystemExit(f"片段{segment_id}只能使用已登记的输出视频，禁止临时覆盖")
    return registered


def main() -> int:
    parser = argparse.ArgumentParser(description="隔离的 20-45 秒原创视频可变分段旁路")
    parser.add_argument("command", choices=(
        "plan", "voiceover", "h3-prepare", "h3-submit", "h3-query", "h3-download",
        "register-keyframes", "extract-bridge", "select-bridge", "merge", "show",
        "export-review", "preflight", "auto-select-bridge", "finalize", "run-to-final",
        "report",
    ))
    parser.add_argument("--input", help="长视频主合同 JSON")
    parser.add_argument("--source-script-id", default="", help="复用现有完整脚本作为主语义与生产世界")
    parser.add_argument("--supporting-script-id", action="append", default=[], help="同产品已批准支持语义，最多读取两个")
    parser.add_argument("--product-code", default="")
    parser.add_argument("--job-id", default="")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--asset-root", default=str(DEFAULT_ASSET_ROOT))
    parser.add_argument("--model-command", default="")
    parser.add_argument("--use-model", action="store_true", help="用现有原创模型从冻结输入生成长视频主合同")
    parser.add_argument("--from-complete-script", action="store_true", help="把现有完整脚本只读投影为长视频冻结输入")
    parser.add_argument("--duration", type=int, default=27)
    parser.add_argument(
        "--scene-mode", choices=("single", "auto", "multi"),
        default=None,
    )
    parser.add_argument("--blueprint-model", default="gpt-5.6-sol")
    parser.add_argument("--blueprint-reasoning", default="high")
    parser.add_argument("--video-a", default="")
    parser.add_argument("--video-b", default="")
    parser.add_argument("--video-c", default="")
    parser.add_argument("--segment", choices=("A", "B", "C"), default="")
    parser.add_argument("--start-frame", default="")
    parser.add_argument("--end-frame", default="")
    parser.add_argument(
        "--planned-frame", action="append", default=[],
        help="计划桥接帧，可重复使用：K1=/abs/K1.png、K2=/abs/K2.png",
    )
    parser.add_argument(
        "--scene-entry", action="append", default=[],
        help="跨场景片段进入帧，可重复使用：B=/abs/scene_b.png、C=/abs/scene_c.png",
    )
    parser.add_argument("--auto-generate-scene-entry", action="store_true")
    parser.add_argument(
        "--auto-generate-k0", action="store_true",
        help="未提供 K0 时，用冻结的商品/人物参考自动生成统一首帧",
    )
    parser.add_argument("--bridge-frame", default="")
    parser.add_argument("--allow-real-submit", action="store_true")
    parser.add_argument("--allow-external-tts", action="store_true")
    parser.add_argument("--manual-bridge", action="store_true")
    parser.add_argument("--poll-interval-seconds", type=int, default=30)
    parser.add_argument("--max-wait-seconds", type=int, default=1200)
    parser.add_argument("--output", default="")
    parser.add_argument("--summary-only", action="store_true")
    args = parser.parse_args()

    storage = LongformStorage(args.db)
    storage.ensure_schema()
    if args.command == "preflight":
        gateway = H3Gateway(state_root=Path(args.asset_root) / "h3_state")
        output = {
            "h3": gateway.preflight(require_api_key=args.allow_real_submit),
            "asset_root": str(Path(args.asset_root).expanduser().resolve()),
            "db": str(Path(args.db).expanduser().resolve()),
            "paid_submit_requested": args.allow_real_submit,
            "external_tts_requested": args.allow_external_tts,
        }
    elif args.command == "plan":
        if not args.input and not args.source_script_id:
            parser.error("plan 需要 --input 或 --source-script-id")
        primary_script = None
        if args.source_script_id:
            from scripts.run_first_frame_tasks import _load_script
            primary_script = _load_script(args.source_script_id)
            source = source_from_complete_script(
                primary_script, duration_seconds=args.duration, product_code=args.product_code
            )
            source["scene_mode"] = args.scene_mode or (
                "auto" if os.environ.get("LONGFORM_MULTISCENE_V1_ENABLED", "0").strip().lower()
                in {"1", "true", "yes", "on"} else "single"
            )
            supporting_scripts = [_load_script(value) for value in args.supporting_script_id[:2]]
            source = add_supporting_scripts(source, supporting_scripts, limit=2)
            if not args.use_model:
                parser.error("--source-script-id 必须同时使用 --use-model")
        else:
            source = _json(args.input)
            if args.scene_mode:
                source["scene_mode"] = args.scene_mode
        if args.from_complete_script:
            source = source_from_complete_script(
                source, duration_seconds=args.duration, product_code=args.product_code
            )
            source["scene_mode"] = args.scene_mode or (
                "auto" if os.environ.get("LONGFORM_MULTISCENE_V1_ENABLED", "0").strip().lower()
                in {"1", "true", "yes", "on"} else "single"
            )
            if not args.use_model:
                parser.error("--from-complete-script 必须同时使用 --use-model，由模型补充长视频拍摄单元")
        master = (
            generate_master_contract(
                source, model=args.blueprint_model,
                reasoning_effort=args.blueprint_reasoning,
            )
            if args.use_model else validate_master_contract(source)
        )
        plan = compile_longform_plan(master)
        job_id = args.job_id or stable_id("LFJ_", {"contract": master["contract_id"], "plan": plan["plan_id"]})
        source_script_id = str(
            (master.get("source_lineage") or {}).get("source_script_id")
            or args.source_script_id
            or ""
        )
        frozen_assets = freeze_reference_assets(
            job_id=job_id,
            asset_root=args.asset_root,
            source_script_id=source_script_id,
            materials=(source, primary_script or {}),
        )
        master["frozen_reference_assets"] = frozen_assets
        keyframes = build_keyframe_contracts(master, plan)
        storage.save_plan(job_id, master, plan, keyframes)
        output = {
            "job_id": job_id, "status": "PLANNED", "plan": plan,
            "keyframes": keyframes, "frozen_reference_assets": frozen_assets,
        }
    else:
        if not args.job_id:
            parser.error(f"{args.command} 需要 --job-id")
        row = storage.get_job(args.job_id)
        if not row:
            raise SystemExit(f"找不到 job: {args.job_id}")
        master = json.loads(row["master_contract_json"])
        plan = json.loads(row["plan_json"])
        if args.command == "show":
            output = row
        elif args.command == "export-review":
            output = export_review_bundle(
                row, Path(args.asset_root) / args.job_id / "text_review"
            )
        elif args.command == "report":
            output = json.loads(row.get("execution_report_json") or "{}")
            if not output:
                output = {
                    "job_id": args.job_id, "status": row.get("status"),
                    "segments": row.get("segments"),
                    "merged_video_path": row.get("merged_video_path"),
                    "final_video_path": row.get("final_video_path"),
                }
        elif args.command == "voiceover":
            kwargs = {"model_command": args.model_command} if args.model_command else {}
            voiceover = run_longform_voiceover(master, plan, **kwargs)
            storage.update_job(args.job_id, "VOICEOVER_READY", voiceover_json=json.dumps(voiceover, ensure_ascii=False))
            output = voiceover
        elif args.command == "register-keyframes":
            output = _register_initial_keyframes(
                storage, row, plan, args.job_id, args.start_frame,
                args.planned_frame, args.end_frame, args.scene_entry,
                auto_generate_scene_entries=args.auto_generate_scene_entry,
                auto_generate_k0=args.auto_generate_k0,
                asset_root=args.asset_root,
            )
        elif args.command in {"h3-prepare", "h3-submit"}:
            segment_id = args.segment or "A"
            segment = next(item for item in plan["segments"] if item["segment_id"] == segment_id)
            if args.command == "h3-submit":
                voiceover = json.loads(row.get("voiceover_json") or "{}")
                if not voiceover.get("target_text"):
                    raise SystemExit("付费H3提交前必须先完成统一口播")
                if not args.allow_external_tts:
                    raise SystemExit("付费H3提交前必须显式允许Edge TTS实测：--allow-external-tts")
                voiceover = calibrate_longform_voiceover_with_edge(
                    master, plan, voiceover,
                    Path(args.asset_root) / args.job_id / "voiceover_preflight",
                    model_command=(args.model_command or DEFAULT_MODEL_COMMAND),
                )
                storage.update_job(
                    args.job_id, "VOICEOVER_TTS_READY",
                    voiceover_json=json.dumps(voiceover, ensure_ascii=False),
                )
            if args.start_frame or args.end_frame:
                parser.error(
                    "h3-prepare/h3-submit 只使用数据库已登记关键帧；"
                    "请先执行 register-keyframes 或 select-bridge"
                )
            start_frame, end_frame = _registered_h3_frames(row, segment_id)
            request = H3Gateway.build_segment_request(
                segment, start_frame=start_frame, end_frame=end_frame
            )
            request_path = write_segment_request(
                Path(args.asset_root) / args.job_id / "h3" / f"segment_{segment_id}.json", request
            )
            gateway = H3Gateway(state_root=Path(args.asset_root) / args.job_id / "h3_state")
            if args.command == "h3-prepare":
                prepared = gateway.prepare(request_path)
                output = {"job_id": args.job_id, "segment": segment_id,
                          "request_path": request_path, "prepared": prepared}
            else:
                submitted = gateway.submit(request_path, allow_real_submit=args.allow_real_submit)
                task_id = submitted.get("taskId") or ""
                storage.update_segment(
                    args.job_id, segment_id, status="SUBMITTED", platform_task_id=task_id,
                    submit_fingerprint=submitted.get("fingerprint") or "",
                    platform_response_json=json.dumps(submitted, ensure_ascii=False),
                    start_frame_path=start_frame, end_frame_path=end_frame,
                )
                storage.update_job(args.job_id, f"{segment_id}_SUBMITTED")
                output = submitted
        elif args.command in {"h3-query", "h3-download"}:
            segment_id = args.segment or "A"
            segment_row = _segment_row(row, segment_id)
            task_id = segment_row.get("platform_task_id") or ""
            if not task_id:
                raise SystemExit(f"片段{segment_id}没有 platform_task_id")
            gateway = H3Gateway(state_root=Path(args.asset_root) / args.job_id / "h3_state")
            if args.command == "h3-query":
                response = gateway.query(task_id)
                storage.update_segment(
                    args.job_id, segment_id, platform_response_json=json.dumps(response, ensure_ascii=False)
                )
                output = response
            else:
                target = args.output or str(
                    Path(args.asset_root) / args.job_id / f"segment_{segment_id}.mp4"
                )
                response = gateway.download(task_id, target)
                storage.update_segment(
                    args.job_id, segment_id, status="READY", output_video_path=target,
                    platform_response_json=json.dumps(response, ensure_ascii=False),
                )
                storage.update_job(args.job_id, f"{segment_id}_READY")
                output = response
        elif args.command == "extract-bridge":
            source_id = args.segment or "A"
            segments = _ordered_segment_rows(row)
            source_index = _segment_position(row, source_id)
            if source_index >= len(segments) - 1:
                raise SystemExit("最终片段之后没有桥接目标")
            target_id = str(segments[source_index + 1]["segment_id"])
            if str(_boundary_to(plan, target_id).get("boundary_mode") or "CONTINUOUS") != "CONTINUOUS":
                raise SystemExit(f"{source_id}→{target_id}是跨场景硬切，不抽取实际尾帧")
            provided = {"A": args.video_a, "B": args.video_b, "C": args.video_c}.get(source_id, "")
            source_video = _registered_output_video(row, source_id, provided)
            root = _bridge_candidate_root(args.asset_root, args.job_id, source_id, target_id)
            frames = extract_bridge_candidates(source_video, root)
            storage.update_job(args.job_id, "BRIDGE_CANDIDATES_READY")
            output = {
                "job_id": args.job_id, "from_segment": source_id,
                "to_segment": target_id, "bridge_candidates": frames,
                "next": f"选择一张实际尾帧后，才允许提交片段{target_id}",
            }
        elif args.command == "select-bridge":
            target_id = args.segment or "B"
            segments = _ordered_segment_rows(row)
            target_index = _segment_position(row, target_id)
            if target_index == 0:
                raise SystemExit("片段A使用统一首帧，不接受桥接帧")
            source_id = str(segments[target_index - 1]["segment_id"])
            if str(_boundary_to(plan, target_id).get("boundary_mode") or "CONTINUOUS") != "CONTINUOUS":
                raise SystemExit(f"{source_id}→{target_id}是跨场景硬切，必须使用场景进入帧")
            if not args.bridge_frame or not Path(args.bridge_frame).is_file():
                parser.error("select-bridge 需要已存在的 --bridge-frame")
            source_segment = _segment_row(row, source_id)
            target_segment = _segment_row(row, target_id)
            if str(source_segment.get("status") or "") != "READY":
                raise SystemExit(f"必须先从已完成片段{source_id}提取桥接候选")
            if str(target_segment.get("status") or "") in {"SUBMITTED", "READY"}:
                raise SystemExit(f"片段{target_id}已提交或完成，不能更换桥接帧")
            bridge_frame = str(Path(args.bridge_frame).resolve())
            candidate_root = _bridge_candidate_root(
                args.asset_root, args.job_id, source_id, target_id
            ).resolve()
            try:
                Path(bridge_frame).relative_to(candidate_root)
            except ValueError as exc:
                raise SystemExit("实际桥接帧必须来自本作业对应片段的候选目录") from exc
            storage.update_segment(
                args.job_id, target_id, status="BRIDGE_READY", start_frame_path=bridge_frame
            )
            storage.update_job(args.job_id, "BRIDGE_READY")
            output = {
                "job_id": args.job_id, "status": "BRIDGE_READY",
                "from_segment": source_id, "to_segment": target_id,
                f"K{target_index}_ACTUAL": bridge_frame,
            }
        elif args.command == "auto-select-bridge":
            target_id = args.segment or "B"
            segments = _ordered_segment_rows(row)
            target_index = _segment_position(row, target_id)
            if target_index == 0:
                raise SystemExit("片段A使用统一首帧，不接受桥接帧")
            source_id = str(segments[target_index - 1]["segment_id"])
            if str(_boundary_to(plan, target_id).get("boundary_mode") or "CONTINUOUS") != "CONTINUOUS":
                raise SystemExit(f"{source_id}→{target_id}是跨场景硬切，不能自动选尾帧")
            provided = {"A": args.video_a, "B": args.video_b, "C": args.video_c}.get(source_id, "")
            source_video = _registered_output_video(row, source_id, provided)
            target_segment = _segment_row(row, target_id)
            if str(target_segment.get("status") or "") in {"SUBMITTED", "READY"}:
                raise SystemExit(f"片段{target_id}已提交或完成，不能更换桥接帧")
            root = _bridge_candidate_root(args.asset_root, args.job_id, source_id, target_id)
            candidates = sorted(str(path) for path in root.glob("bridge_*.jpg"))
            if not candidates:
                candidates = extract_bridge_candidates(source_video, root)
            selection = select_bridge_candidate(candidates)
            storage.update_segment(
                args.job_id, target_id, status="BRIDGE_READY",
                start_frame_path=selection["selected"],
                platform_response_json=json.dumps({"bridge_selection": selection}, ensure_ascii=False),
            )
            storage.update_job(args.job_id, "BRIDGE_READY")
            output = {
                "job_id": args.job_id, "status": "BRIDGE_READY",
                "from_segment": source_id, "to_segment": target_id,
                f"K{target_index}_ACTUAL": selection["selected"],
                "selection": selection,
            }
        elif args.command == "merge":
            provided = {"A": args.video_a, "B": args.video_b, "C": args.video_c}
            videos = [
                _registered_output_video(
                    row, str(segment["segment_id"]),
                    provided.get(str(segment["segment_id"]), ""),
                )
                for segment in _ordered_segment_rows(row)
            ]
            target = args.output or str(Path(args.asset_root) / args.job_id / "merged_silent.mp4")
            merged = merge_segments(videos, target)
            storage.update_job(args.job_id, "MERGED", merged_video_path=merged)
            output = {"job_id": args.job_id, "status": "MERGED", "merged_video_path": merged,
                      "audio_policy": "等待统一 TTS/BGM 后混音"}
        elif args.command == "finalize":
            merged = str(row.get("merged_video_path") or "")
            if not merged or not Path(merged).is_file():
                raise SystemExit("必须先完成 merge")
            voiceover = json.loads(row.get("voiceover_json") or "{}")
            target = args.output or str(Path(args.asset_root) / args.job_id / "final_video.mp4")
            finalization = finalize_with_voiceover(
                merged, voiceover, target, allow_external_tts=args.allow_external_tts,
                segment_plan=plan.get("segments") or [],
            )
            storage.update_job(
                args.job_id, "FINAL_READY",
                final_video_path=finalization["final_video_path"],
            )
            final_row = storage.get_job(args.job_id) or {}
            review = export_review_bundle(
                final_row, Path(args.asset_root) / args.job_id / "text_review"
            )
            output = {
                "job_id": args.job_id, "status": "FINAL_READY",
                **finalization, "review": review,
            }
        elif args.command == "run-to-final":
            first_segment = _ordered_segment_rows(row)[0]
            if str(first_segment.get("status") or "") == "PLANNED" and (
                args.start_frame or args.end_frame or args.planned_frame
            ):
                _register_initial_keyframes(
                    storage, row, plan, args.job_id, args.start_frame,
                    args.planned_frame, args.end_frame, args.scene_entry,
                    auto_generate_scene_entries=args.auto_generate_scene_entry,
                    auto_generate_k0=args.auto_generate_k0,
                    asset_root=args.asset_root,
                )
            output = run_to_final(
                storage, args.job_id, asset_root=args.asset_root,
                allow_real_submit=args.allow_real_submit,
                allow_external_tts=args.allow_external_tts,
                manual_bridge=args.manual_bridge,
                poll_interval_seconds=max(5, args.poll_interval_seconds),
                max_wait_seconds=max(30, args.max_wait_seconds),
                voiceover_model_command=(args.model_command or DEFAULT_MODEL_COMMAND),
            )
        else:
            raise SystemExit(f"尚未实现的命令: {args.command}")
    if args.summary_only and isinstance(output, dict):
        if args.command == "plan":
            output = {
                "job_id": output.get("job_id"),
                "status": output.get("status"),
                "plan_id": (output.get("plan") or {}).get("plan_id"),
                "target_duration_seconds": (output.get("plan") or {}).get("target_duration_seconds"),
                "segments": [
                    {
                        "segment_id": item.get("segment_id"),
                        "duration_seconds": item.get("duration_seconds"),
                        "capture_unit_count": len(item.get("capture_units") or []),
                        "generation_mode": item.get("generation_mode"),
                    }
                    for item in (output.get("plan") or {}).get("segments") or []
                ],
                "keyframe_package_id": (output.get("keyframes") or {}).get("keyframe_package_id"),
            }
        elif args.command == "show":
            output = {
                "job_id": output.get("job_id"), "product_code": output.get("product_code"),
                "status": output.get("status"),
                "segments": [
                    {
                        "segment_id": item.get("segment_id"),
                        "status": item.get("status"),
                        "duration_seconds": item.get("duration_seconds"),
                        "generation_mode": item.get("generation_mode"),
                        "platform_task_id": item.get("platform_task_id"),
                        "output_video_path": item.get("output_video_path"),
                    }
                    for item in output.get("segments") or []
                ],
                "merged_video_path": output.get("merged_video_path"),
                "final_video_path": output.get("final_video_path"),
            }
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    if args.command == "preflight" and not bool((output.get("h3") or {}).get("ready")):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
