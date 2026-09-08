#!/usr/bin/env python3
"""Persona effect A/B/C experiment (one-shot ops; paid CreatOK calls).

Independent output directory, no account rebind, no publishing, no automatic
regeneration (each condition generated exactly once; technical failures are
retried and logged separately).

Groups:
  candidates : 4 half-body persona candidates (medium quality)
  fullbody   : full-body reference derived from the selected candidate
  A baseline : current persona  + current photography contract
  B prompt   : current persona  + new photography copy
  C persona  : new persona      + same copy as B
"""
from __future__ import annotations

import argparse
import hashlib
import os
import json
import subprocess
import sys
import time
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
for value in (str(WORKSPACE_ROOT),):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

EXPERIMENT_DIR = Path.home() / ".openclaw/workspace/shared/data/organic_photo_video/persona_experiment_20260907"
PERSONA_PACK = Path.home() / ".openclaw/workspace/shared/data/persona_templates/TH_APPAREL_REAL_01_001"

MODEL = "gpt-image-2-official"
RESOLUTION = "1K"
CANDIDATE_QUALITY = "medium"   # 候选/全身参考：选人需要脸部细节
COMPARISON_QUALITY = "low"     # 对照组：与生产档位一致，反映真实效果
ASPECT = "9:16"

# 冻结穿搭：取 recvuxcl7g53XE look_a 的实际计划
FROZEN_OUTFIT = {
    "outerwear": "深蓝色短款牛仔夹克",
    "top_inner": "米白色高领针织打底衫",
    "bottom": "深蓝色高腰阔腿牛仔裤",
    "shoes": "酒红色平底鞋",
}

# 冻结场景（两对照组场景）
SCENES = {
    "day": {
        "label": "日间街边",
        "scene_zh": "白天城市街边人行道，背景有街道、店铺和少量行人，自然日光",
        "pose_zh": "正面全身自然站立，重心落在一条腿上，鞋履完整入镜",
    },
    "dusk": {
        "label": "傍晚街边",
        "scene_zh": "傍晚城市街边人行道，暖色街灯与傍晚天光混合，背景街道虚化",
        "pose_zh": "全身轻微侧身站立并轻微回望镜头方向，脸部无遮挡，鞋履完整入镜",
    },
}

# 冻结人物身份段
CURRENT_PERSONA_DESC = (
    "泰国年轻女性，肤色自然冷白透亮、好皮肤，眼睛保持自然大小（杏仁眼、不放大），"
    "真实皮肤纹理，黑长直发，气质干净清爽，像会认真搭配的日常时尚博主"
)

# 冻结摄影段
PHOTO_CURRENT = (  # A：当前生产人物摄影合同（与 human_presentation_contract_lines 输出一致）
    "头颈保持自然直立，不向左右肩膀倾斜；肩线放松自然下压。\n"
    "动作必须有明确重心，承重腿、迈步腿、髋部和手臂协调受力。\n"
    "手臂有自然弯曲和动态，不紧贴身体像人台。\n"
    "真实皮肤保留毛孔、细小纹理和自然不对称；避免玻璃眼、塑料皮肤和固定上扬的嘴角。\n"
    "像同行朋友用手机随手抓拍的瞬间，不是刻意摆拍：机位可轻微偏离正面（侧机位或略低角度），构图不必完全居中，允许轻微不对称。\n"
    "背景保留真实生活内容：行人、船只或街道元素可以自然入画（可轻微虚化），前景栏杆或植物可以形成自然遮挡；不要影棚式的干净空旷。\n"
    "允许自然的不完美：碎发和发丝被风吹动、衣物自然褶皱和垂坠、表情有微小动态；不要过度整洁的目录感。\n"
    "肤色在人物参考图基础上自然提亮为冷白透亮，像真实的好皮肤；保持毛孔和质感，不要苍白假白或滤镜白。\n"
    "眼睛保持人物参考图的自然大小和形状（杏仁眼、不放大不改眼型），眼神放松，不刻意睁大。\n"
    "人物参考图只约束身份（脸、发型、身材比例）；不得复制人物参考图里的头部角度、表情、手势或姿势。"
)
PHOTO_NEW = (  # B/C：按交接方案 §4 的新摄影片段
    "自然精致的日常穿搭照片。人物妆发整洁，眼神有精神，表情放松，可有轻微自然笑意。"
    "保留真实皮肤纹理与自然面部结构，面部曝光清楚，光线符合现场环境。"
    "整体具有朋友帮助拍摄的自然感和穿搭照片的美感。\n"
    "保留真实皮肤细节和自然不对称，避免塑料皮肤、夸大的眼睛和僵硬笑容。\n"
    "人物参考图只约束身份（脸、发型、身材比例）；不得复制人物参考图里的头部角度、表情、手势或姿势。"
)

BASE_NEGATIVE = (
    "不要文字、字幕、水印、Logo 杜撰；不要尺寸标注；不要多余人物或多余肢体；"
    "不要畸形手指；不要塑料皮肤；不要异常放大的眼睛；不要僵硬笑容。"
)


def build_comparison_prompt(*, scene_key: str, persona_desc: str, photo_copy: str) -> str:
    scene = SCENES[scene_key]
    outfit = FROZEN_OUTFIT
    return (
        "生成一张竖屏 9:16 的真实穿搭创作者照片（手机竖屏拍摄的原生记录感）。\n"
        "前两张参考图是同一人物的身份参考（脸部与身材的唯一权威来源）；"
        "人物外貌、发型和肤色严格以参考图为准，不得复制参考图中的头部角度、表情或姿势。\n"
        f"【人物身份】{persona_desc}\n"
        "【冻结穿搭】外套：" + outfit["outerwear"] + "；内搭：" + outfit["top_inner"]
        + "；下装：" + outfit["bottom"] + "；鞋履：" + outfit["shoes"] + "。"
        "四件单品必须全部出现且不得替换。\n"
        f"【场景】{scene['scene_zh']}。\n"
        f"【构图与动作】{scene['pose_zh']}。\n"
        "【人物摄影】\n" + photo_copy + "\n"
        "【通用负向】" + BASE_NEGATIVE + "。只输出一张完整画面。"
    )


CANDIDATES = {
    "cand_1_fresh": {
        "label": "清爽亲和",
        "desc": "20 岁出头的东亚年轻女性，黑色长直发中分，干净清爽的邻家亲和感，"
                "自然深色眉毛，眼神明亮温和",
    },
    "cand_2_gentle": {
        "label": "温柔精致",
        "desc": "25 岁左右的东亚年轻女性，深棕色自然微卷中长发，柔和圆润的脸型，"
                "温柔精致的气质，妆容轻薄干净",
    },
    "cand_3_chic": {
        "label": "自然时髦",
        "desc": "24 岁左右的东亚年轻女性，深色锁骨发自然外翻，五官立体但柔和，"
                "都市松弛时髦感，像日常也认真打理造型的时尚编辑",
    },
    "cand_4_bright": {
        "label": "明朗有精神",
        "desc": "22 岁左右的东亚年轻女性，高马尾发型，眉眼明亮有神，"
                "明朗有精神的元气感，笑容自然不夸张",
    },
}

CANDIDATE_COMMON = (
    "半身胸像人像照片：正面平视镜头，头颈自然水平，构图接近（人物占画面中部，"
    "头顶留少量空间），柔和均匀的日光，简单浅暖色纯色背景。"
    "成年女性穿搭创作者，五官协调，眼神自然有精神，妆容轻薄，发型经过简单整理，表情放松。"
    "保留真实皮肤细节和自然不对称，避免塑料皮肤、夸大的眼睛和僵硬笑容。"
    "像真实手机拍摄的人像，不是影棚精修。"
)

FULLBODY_PROMPT = (
    "基于参考图生成同一人物的全身照片：完整全身从头顶到鞋底入镜，"
    "自然直立站姿，重心落在一条腿上，另一条略向前；"
    "穿简单修身白色T恤、浅蓝色直筒牛仔裤和白色平底运动鞋；"
    "脸部、发型与参考图完全一致；自然日光，简单浅色街边背景；"
    "真实手机摄影质感，保留真实皮肤纹理，不美颜不磨皮。"
)


def generate(image_path: str, prompt: str, refs: list[str], quality: str,
             record: dict, key: str, allow_retry: bool = True) -> None:
    options = {"model": MODEL, "resolution": RESOLUTION, "quality": quality,
               "n": 1, "aspect_ratio": ASPECT}
    command = ["creatok", "image", "generate", "--prompt", prompt,
               "--options", json.dumps(options), "--out", str(Path(image_path).parent),
               "--timeout", "300"]
    if refs:
        command += ["--ref", ",".join(refs)]
    attempts = 0
    started = time.time()
    while True:
        attempts += 1
        try:
            completed = subprocess.run(command, capture_output=True, text=True, timeout=390)
            envelope = json.loads(completed.stdout)
        except (subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
            envelope = {"ok": False, "error": {"message": f"{type(exc).__name__}: {exc}"}}
        if envelope.get("ok"):
            urls = ((envelope.get("data") or {}).get("result") or {}).get("images") or []
            if urls and urls[0].get("url"):
                import urllib.request
                data = urllib.request.urlopen(urls[0]["url"], timeout=120).read()
                Path(image_path).write_bytes(data)
                record[key] = {
                    "file": image_path, "task_id": envelope.get("task_id"),
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "quality": quality, "refs": [str(r) for r in refs],
                    "attempts": attempts, "elapsed_s": round(time.time() - started, 1),
                    "retry_note": "technical retry only" if attempts > 1 else "",
                }
                print(f"  {key}: OK ({attempts} attempt(s), {time.time()-started:.0f}s)", flush=True)
                return
        note = (envelope.get("error") or {}).get("message") or "unknown"
        if allow_retry and attempts < 3:
            print(f"  {key}: technical failure ({note[:80]}), retry {attempts}/3", flush=True)
            time.sleep(3)
            continue
        record[key] = {"file": image_path, "error": note, "attempts": attempts,
                       "elapsed_s": round(time.time() - started, 1)}
        print(f"  {key}: FAILED after {attempts} attempts: {note[:120]}", flush=True)
        return


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", required=True,
                        choices=["candidates", "fullbody", "comparison"],
                        help="分段执行：候选 → 全身参考 → 六张对照")
    parser.add_argument("--selected", default="cand_2_gentle",
                        help="选定的候选 key（fullbody/comparison 阶段使用）")
    args = parser.parse_args()

    if not os.environ.get("CREATOK_API_KEY"):
        raise SystemExit("CREATOK_API_KEY 未配置")

    manifest_path = EXPERIMENT_DIR / "experiment_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    record = (json.loads(manifest_path.read_text(encoding="utf-8"))
              if manifest_path.is_file() else {})
    record.setdefault("frozen", {
        "model": MODEL, "resolution": RESOLUTION,
        "candidate_quality": CANDIDATE_QUALITY,
        "comparison_quality": COMPARISON_QUALITY,
        "aspect": ASPECT,
        "frozen_outfit": FROZEN_OUTFIT,
        "scenes": SCENES,
        "persona_pack": str(PERSONA_PACK),
        "note": "独立实验；不改账号绑定；不发布；每个条件只生成一次（技术故障重试单独记录）",
    })

    if args.stage == "candidates":
        for key, meta in CANDIDATES.items():
            image_path = str(EXPERIMENT_DIR / f"{key}.png")
            if Path(image_path).is_file():
                print(f"  {key}: exists, skip")
                continue
            print(f"[candidates] {meta['label']} ...", flush=True)
            prompt = meta["desc"] + "。" + CANDIDATE_COMMON
            generate(image_path, prompt, [], CANDIDATE_QUALITY, record, key)
            manifest_path.write_text(json.dumps(record, ensure_ascii=False, indent=1),
                                     encoding="utf-8")

    elif args.stage == "fullbody":
        mother = str(EXPERIMENT_DIR / f"{args.selected}.png")
        image_path = str(EXPERIMENT_DIR / f"{args.selected}_fullbody.png")
        if not Path(mother).is_file():
            raise SystemExit(f"母图不存在：{mother}")
        if not Path(image_path).is_file():
            print(f"[fullbody] {args.selected} ...", flush=True)
            generate(image_path, FULLBODY_PROMPT, [mother], CANDIDATE_QUALITY,
                     record, f"{args.selected}_fullbody")
        record["selected_candidate"] = args.selected
        manifest_path.write_text(json.dumps(record, ensure_ascii=False, indent=1),
                                 encoding="utf-8")

    elif args.stage == "comparison":
        selected = args.selected
        current_refs = [
            str(PERSONA_PACK / "01_face_front_neutral.png"),
            str(PERSONA_PACK / "03_body_full_neutral.png"),
        ]
        new_refs = [
            str(EXPERIMENT_DIR / f"{selected}.png"),
            str(EXPERIMENT_DIR / f"{selected}_fullbody.png"),
        ]
        groups = {
            "A_baseline_current": {"refs": current_refs,
                                   "persona": CURRENT_PERSONA_DESC,
                                   "photo": PHOTO_CURRENT},
            "B_prompt_new": {"refs": current_refs,
                             "persona": CURRENT_PERSONA_DESC,
                             "photo": PHOTO_NEW},
            "C_persona_new": {"refs": new_refs,
                              "persona": CANDIDATES[selected]["desc"],
                              "photo": PHOTO_NEW},
        }
        for group, cfg in groups.items():
            for scene_key in ("day", "dusk"):
                key = f"{group}_{scene_key}"
                image_path = str(EXPERIMENT_DIR / f"{key}.png")
                if Path(image_path).is_file():
                    print(f"  {key}: exists, skip")
                    continue
                print(f"[comparison] {key} ...", flush=True)
                prompt = build_comparison_prompt(
                    scene_key=scene_key, persona_desc=cfg["persona"],
                    photo_copy=cfg["photo"],
                )
                record.setdefault("prompts", {})[key] = prompt
                generate(image_path, prompt, cfg["refs"], COMPARISON_QUALITY,
                         record, key)
                manifest_path.write_text(json.dumps(record, ensure_ascii=False, indent=1),
                                         encoding="utf-8")
    manifest_path.write_text(json.dumps(record, ensure_ascii=False, indent=1),
                             encoding="utf-8")
    print("stage done:", args.stage)
    return 0


if __name__ == "__main__":
    sys.exit(main())
