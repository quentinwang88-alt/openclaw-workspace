"""
Sync BGM tracks from Feishu table, download audio files, and run LLM audio tagging.
Usage: python3 scripts/sync_and_tag_bgm.py
"""
from __future__ import annotations

import json, os, sys, hashlib
from pathlib import Path

WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
AUTO_MIXCUT = WORKSPACE / "auto_mixcut"
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))
sys.path.insert(0, str(AUTO_MIXCUT))

from core.bitable import FeishuBitableClient  # noqa: E402
from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.skills.bgm_library_skill import _parse_json_safe  # noqa: E402
from auto_mixcut.skills.bgm_audio_analysis_skill import BgmAudioAnalysisSkill  # noqa: E402
from auto_mixcut.skills.bgm_tag_fusion_skill import BgmTagFusionSkill  # noqa: E402
from auto_mixcut.skills.bgm_tagging_skill import BgmTaggingSkill  # noqa: E402
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill  # noqa: E402

APP_TOKEN = "IFa5w98VBif8j7kIitIcLaqLncb"
TABLE_ID = "tblgdVFb6GDSPW3E"


def main():
    print("Connecting to Feishu...")
    client = FeishuBitableClient(app_token=APP_TOKEN, table_id=TABLE_ID)
    records = client.list_records(limit=500)
    print(f"Found {len(records)} records")

    ctx = build_context()
    init = RDSRepositorySkill(ctx).init_db()
    if not init.success:
        print(json.dumps(init.to_dict(), ensure_ascii=False, indent=2))
        return 1

    synced, skipped = 0, 0
    for rec in records:
        fields = rec.fields
        bgm_id = _cell_text(fields.get("BGM编号")) or fields.get("BGM编号", "")
        if isinstance(bgm_id, dict):
            bgm_id = str(bgm_id.get("text", ""))
        if not bgm_id:
            bgm_id = _make_bgm_id(fields)

        row = {
            "bgm_id": bgm_id,
            "bgm_tag_status": "untagged",
            "tag_review_required": 0,
        }
        row["track_name"] = _cell_text(fields.get("BGM名称")) or "Unknown"
        row["artist_name"] = _cell_text(fields.get("Artist/来源")) or ""
        row["source_platform"] = "feishu"
        row["source_url"] = _cell_text(fields.get("来源链接")) or ""
        row["official_tags_json"] = "[]"
        row["license_note"] = _cell_text(fields.get("授权信息")) or ""
        row["default_volume"] = 0.2
        row["fade_in_ms"] = 500
        row["fade_out_ms"] = 800
        row["suitable_for_intro"] = 1
        row["loop_friendly"] = 0
        row["voiceover_friendly"] = 1

        duration = fields.get("时长(ms)")
        if duration is not None:
            if isinstance(duration, (int, float)):
                row["duration_ms"] = int(duration)
            else:
                row["duration_ms"] = int(_cell_text(duration) or "0")

        audio_downloaded = _download_audio(client, rec, ctx, bgm_id)
        if audio_downloaded:
            row["oss_object_id"] = audio_downloaded["oss_object_id"]
            row["local_file_path"] = audio_downloaded["local_path"]

        result = ctx.repo.upsert("bgm_tracks", "bgm_id", row)
        if result.success:
            synced += 1
        else:
            print(f"  WARN upsert failed for {bgm_id}: {result.error.message if result.error else '?'}")
            skipped += 1

    print(f"Synced: {synced}  Skipped: {skipped}")

    all_tracks = ctx.repo.list_where("bgm_tracks", "1=1")
    print(f"Total bgm_tracks in DB: {len(all_tracks)}")
    for t in all_tracks:
        local = t.get("local_file_path", "") or ""
        has_file = "AUDIO" if local and Path(local).exists() else "no-file"
        print(f"  {t['bgm_id']:22s} | {t.get('track_name','')[:30]:30s} | {t.get('bgm_tag_status',''):8s} | {has_file}")

    print("\nRunning BGM audio signal analysis...")
    audio = BgmAudioAnalysisSkill(ctx).analyze_all(only_missing=True, apply_tags=False)
    audio_data = audio.data or {}
    audio_errors = [item for item in audio_data.get("results", []) if not item.get("success")]
    print(f"Analyzed: {audio_data.get('count', 0)}  Errors: {len(audio_errors)}")
    for err in audio_errors[:10]:
        print(f"  AUDIO ERROR {err.get('error', {}).get('code')}: {err.get('error', {}).get('message')}")

    print("\nRunning BGM audio tagging...")
    tagger = BgmTaggingSkill(ctx)
    result = tagger.calibrate_all(force=False, max_concurrency=2)
    d = result.data
    print(f"Tagged: {d.get('tagged',0)}  Skipped: {d.get('skipped',0)}  Errors: {len(d.get('errors',[]))}")
    for err in d.get("errors", []):
        print(f"  ERROR {err['bgm_id']}: {err['error']}")

    print("\nFusing metadata and audio tags...")
    fusion = BgmTagFusionSkill(ctx).fuse_all()
    fusion_data = fusion.data or {}
    print(f"Fused: {fusion_data.get('count', 0)}")

    print("\nResults after tagging:")
    tagged = ctx.repo.list_where("bgm_tracks", "1=1")
    for t in tagged:
        mood = _parse_json_safe(t.get("mood_tags_json"), [])
        cat = _parse_json_safe(t.get("category_tags_json"), [])
        conf = t.get("tag_confidence", "")
        status = t.get("bgm_tag_status", "")
        energy = t.get("energy_level", "")
        vocal = t.get("vocal_type", "")
        reason = (t.get("bgm_tag_reason") or "")[:80]
        print(f"  {t['bgm_id']:22s} | mood={str(mood):30s} | energy={energy:6s} | vocal={vocal:12s} | conf={conf:6s} | {status}")
        if reason:
            print(f"    reason: {reason}")

    logs = ctx.repo.list_where("llm_call_logs", "call_type=?", ("bgm_metadata_tagging",))
    s_ok = sum(1 for l in logs if l.get("result_status") in {"success", "mock_success"})
    s_fail = sum(1 for l in logs if l.get("result_status") == "failed")
    total_lat = sum(int(l.get("latency_ms", 0) or 0) for l in logs if l.get("result_status") in {"success"})
    print(f"\nllm_call_logs: {len(logs)} total | {s_ok} ok | {s_fail} failed | avg latency={total_lat//max(s_ok,1)}ms")

    print("\nDone.")
    return 0


def _download_audio(client, rec, ctx, bgm_id: str) -> dict | None:
    for field_name in ["音频文件", "BGM文件", "音频"]:
        val = rec.fields.get(field_name)
        if not val:
            continue
        if isinstance(val, list) and val:
            item = val[0]
        elif isinstance(val, dict):
            item = val
        else:
            continue
        if isinstance(item, dict) and (item.get("file_token") or item.get("attachment_id")):
            try:
                raw, fname, ctype, fsize = client.download_attachment_bytes(item)
                suffix = fname.rsplit(".", 1)[-1] if "." in fname else "mp3"
            except Exception:
                return None

            local_dir = ctx.settings.oss_root / "auto_mixcut" / "bgm_library" / "raw"
            local_dir.mkdir(parents=True, exist_ok=True)
            local_path = local_dir / f"{bgm_id}.{suffix}"
            local_path.write_bytes(raw)

            object_key = f"auto_mixcut/bgm_library/raw/{bgm_id}.{suffix}"
            upload = ctx.oss.upload(local_path, object_key)
            oss_id = upload.data.get("object_id", "") if upload.success else ""

            return {"oss_object_id": oss_id, "local_path": str(local_path)}
    return None


def _make_bgm_id(fields: dict) -> str:
    name = _cell_text(fields.get("BGM名称")) or "UNKNOWN"
    return "BGM_" + hashlib.sha256(name.encode()).hexdigest()[:12].upper()


def _cell_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ("text", "name", "link", "url"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
        return ""
    if isinstance(value, list):
        items = [_cell_text(v) for v in value if _cell_text(v)]
        return ", ".join(items) if items else ""
    return str(value).strip()


if __name__ == "__main__":
    raise SystemExit(main())
