from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.quality_gate_skill import QualityGateSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from auto_mixcut.skills.render_skill import RenderSkill
from auto_mixcut.skills.voiceover_mixcut_orchestrator_skill import VoiceoverMixcutOrchestratorSkill


@pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="ffmpeg is required")
def test_real_voiceover_render_is_non_publishing_and_keeps_lineage():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        os.environ["AUTO_MIXCUT_ROOT"] = str(Path(__file__).resolve().parents[1])
        os.environ["AUTO_MIXCUT_DB"] = str(root / "db.sqlite")
        os.environ["AUTO_MIXCUT_OSS_ROOT"] = str(root / "oss")
        os.environ["AUTO_MIXCUT_OSS_PROVIDER"] = "local"
        os.environ["AUTO_MIXCUT_TEMP_ROOT"] = str(root / "tmp")
        os.environ["AUTO_MIXCUT_MOCK_FFMPEG"] = "0"
        os.environ["AUTO_MIXCUT_SKIP_INLINE_FEISHU_SYNC"] = "1"
        ctx = build_context()
        initialized = RDSRepositorySkill(ctx).init_db()
        assert initialized.success, initialized.to_dict()
        ctx.repo.upsert("products", "product_id", {"product_id": "P1", "product_name": "Product", "market": "TH", "category": "apparel", "anchor_status": "confirmed"})
        ctx.repo.upsert("content_tasks", "task_id", {"task_id": "T1", "product_id": "P1", "content_mode": "voiceover", "target_language": "th", "target_duration_ms": 3000})
        ctx.repo.upsert("mixcut_batches", "batch_id", {"batch_id": "B1", "product_id": "P1", "task_id": "T1", "batch_status": "planned"})
        roles = [("hero", "完整上身效果"), ("detail", "拉链门襟细节"), ("result", "完整穿着结果")]
        for index, (role, reason) in enumerate(roles, start=1):
            source = root / f"source_{index}.mp4"
            subprocess.run(
                ["ffmpeg", "-y", "-f", "lavfi", "-i", f"color=c={'red' if index == 1 else 'green' if index == 2 else 'blue'}:s=1080x1920:d=1.5:r=30", "-c:v", "libx264", "-pix_fmt", "yuv420p", str(source)],
                check=True,
                capture_output=True,
            )
            uploaded = ctx.oss.upload(source, f"test/segments/S{index}.mp4")
            assert uploaded.success
            ctx.repo.upsert("oss_objects", "object_id", dict(uploaded.data, object_type="segment", mime_type="video/mp4"))
            ctx.repo.upsert("assets", "asset_id", {"asset_id": f"A{index}", "product_id": "P1", "asset_status": "ready", "has_watermark": "no", "source_type": "light_video", "source_flow": "light_video", "source_trust_level": "high", "product_binding_type": "exact_sku"})
            ctx.repo.upsert("segments", "segment_id", {"segment_id": f"S{index}", "asset_id": f"A{index}", "product_id": "P1", "segment_oss_object_id": uploaded.data["object_id"], "duration_ms": 1500, "segment_status": "qc_passed", "source_type": "light_video", "source_trust_level": "high", "product_binding_type": "exact_sku", "product_match_status": "trusted_by_source", "effective_roles_json": [role]})
            ctx.repo.insert("segment_tags", {"segment_id": f"S{index}", "primary_shot_role": role, "secondary_roles_json": [], "product_visibility": "high", "hook_strength": "strong", "mixcut_usability": "yes", "risk_level": "low", "confidence": "high", "reason": reason})
        voice = root / "voice.m4a"
        subprocess.run(
            ["ffmpeg", "-y", "-f", "lavfi", "-i", "sine=frequency=440:duration=3", "-af", "volume=4", "-c:a", "aac", str(voice)],
            check=True,
            capture_output=True,
        )
        voice_upload = ctx.oss.upload(voice, "test/voice/V1.m4a")
        assert voice_upload.success
        ctx.repo.upsert("oss_objects", "object_id", dict(voice_upload.data, object_type="voiceover", mime_type="audio/mp4"))
        timeline = [
            {"beat_id": "B1", "role": "hook", "speech_text": "hook", "start_ms": 0, "end_ms": 1000, "required_shot_roles": ["main_wear_upper"]},
            {"beat_id": "B2", "role": "proof", "speech_text": "拉链细节", "start_ms": 1000, "end_ms": 2000, "required_shot_roles": ["detail_closure"]},
            {"beat_id": "B3", "role": "decision", "speech_text": "result", "start_ms": 2000, "end_ms": 3000, "required_shot_roles": []},
        ]
        prepared = VoiceoverMixcutOrchestratorSkill(ctx).prepare_render_plan(
            task_id="T1",
            batch_id="B1",
            variant_no=1,
            voiceover_variant_id="V1",
            voiceover_oss_object_id=voice_upload.data["object_id"],
            tts_timeline=timeline,
            beat_plan=timeline,
            hook_id="H1",
            primary_selling_point="closure",
        )
        assert prepared.success, prepared.to_dict()
        rendered = RenderSkill(ctx).render_plan(prepared.data["render_plan"]["render_plan_id"])
        assert rendered.success, rendered.to_dict()
        output = ctx.repo.get("outputs", "output_id", rendered.data["output_id"])
        assert output["content_mode"] == "voiceover"
        assert output["publish_task_id"] is None
        assert output["publish_result"] is None
        assert len(ctx.repo.list_where("output_material_usage", "output_id=?", (output["output_id"],))) == 3
        checked = QualityGateSkill(ctx).check_output(output["output_id"])
        assert checked.success, checked.to_dict()
        assert checked.data["machine_quality_status"] == "passed", checked.data
