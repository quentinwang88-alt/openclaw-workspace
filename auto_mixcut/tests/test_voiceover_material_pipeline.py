from __future__ import annotations

import os
import tempfile
from pathlib import Path

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.output_material_usage_skill import OutputMaterialUsageSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from auto_mixcut.skills.voiceover_material_adapter_skill import VoiceoverMaterialAdapterSkill
from auto_mixcut.skills.voiceover_render_plan_bridge_skill import VoiceoverRenderPlanBridgeSkill
from light_tryon.voiceover_visual_match_core import apply_key_match_policy, voiceover_intervals


def _context(tmp_path: Path):
    os.environ["AUTO_MIXCUT_ROOT"] = str(Path(__file__).resolve().parents[1])
    os.environ["AUTO_MIXCUT_DB"] = str(tmp_path / "db.sqlite")
    os.environ["AUTO_MIXCUT_OSS_ROOT"] = str(tmp_path / "oss")
    os.environ["AUTO_MIXCUT_OSS_PROVIDER"] = "local"
    os.environ["AUTO_MIXCUT_TEMP_ROOT"] = str(tmp_path / "tmp")
    os.environ["AUTO_MIXCUT_MOCK_FFMPEG"] = "1"
    ctx = build_context()
    result = RDSRepositorySkill(ctx).init_db()
    assert result.success, result.to_dict()
    return ctx


def test_output_material_usage_is_aggregated_and_idempotent():
    with tempfile.TemporaryDirectory() as directory:
        ctx = _context(Path(directory))
        ctx.repo.upsert("content_tasks", "task_id", {"task_id": "TASK1", "product_id": "P1", "target_language": "th"})
        ctx.repo.upsert("mixcut_batches", "batch_id", {"batch_id": "B1", "product_id": "P1", "task_id": "TASK1"})
        ctx.repo.upsert("outputs", "output_id", {"output_id": "O1", "batch_id": "B1", "product_id": "P1", "render_status": "rendered"})
        ctx.repo.upsert("assets", "asset_id", {"asset_id": "A1", "product_id": "P1", "source_flow": "light_video", "source_record_id": "REC1"})
        for slot, role, start, end, segment_id in [(1, "hero", 0, 1000, "S1"), (2, "detail", 1000, 2500, "S2")]:
            ctx.repo.insert("output_segments", {"output_id": "O1", "segment_id": segment_id, "asset_id": "A1", "slot_index": slot, "role_used": role, "start_ms_in_output": start, "end_ms_in_output": end})
        skill = OutputMaterialUsageSkill(ctx)
        first = skill.refresh_output("O1")
        second = skill.refresh_output("O1")
        assert first.success and second.success
        rows = ctx.repo.list_where("output_material_usage", "output_id=?", ("O1",))
        assert len(rows) == 1
        assert rows[0]["segment_count"] == 2
        assert rows[0]["used_duration_ms"] == 2500
        assert rows[0]["roles_json"] == ["detail", "hero"]
        assert rows[0]["is_core_material"] == 1
        assert rows[0]["is_first_slot"] == 1
        assert rows[0]["target_language"] == "th"


def test_shared_voiceover_matcher_and_render_bridge_use_rds_materials():
    with tempfile.TemporaryDirectory() as directory:
        ctx = _context(Path(directory))
        ctx.repo.upsert("products", "product_id", {"product_id": "P1", "product_name": "Product"})
        for index, (role, reason) in enumerate((("hero", "完整上身展示"), ("detail", "拉链与领口细节")), start=1):
            asset_id = f"A{index}"
            segment_id = f"S{index}"
            ctx.repo.upsert("assets", "asset_id", {"asset_id": asset_id, "product_id": "P1", "asset_status": "watermark_skipped" if index == 1 else "ready", "has_watermark": "no", "source_flow": "light_video"})
            ctx.repo.upsert("segments", "segment_id", {"segment_id": segment_id, "asset_id": asset_id, "product_id": "P1", "duration_ms": 1800, "segment_status": "qc_passed", "effective_roles_json": [role], "product_match_status": "trusted_by_source"})
            ctx.repo.insert("segment_tags", {"segment_id": segment_id, "primary_shot_role": role, "secondary_roles_json": [], "product_visibility": "high", "mixcut_usability": "yes", "risk_level": "low", "reason": reason})
        timeline = [
            {"beat_id": "B1", "role": "hook", "speech_text": "先看上身效果", "start_ms": 0, "end_ms": 1000, "required_shot_roles": ["main_wear_upper"]},
            {"beat_id": "B2", "role": "proof", "speech_text": "拉链和领口细节很清楚", "start_ms": 1000, "end_ms": 2000, "required_shot_roles": ["detail_closure"]},
        ]
        matched = VoiceoverMaterialAdapterSkill(ctx).match("P1", timeline, 2000)
        assert matched.success, matched.to_dict()
        assert not matched.data["evidence_gaps"]
        assert [clip["asset_id"] for clip in matched.data["clips"]] == ["A1", "A2"]
        bridged = VoiceoverRenderPlanBridgeSkill(ctx).create(
            batch_id="B1",
            product_id="P1",
            variant_no=1,
            voiceover_variant_id="V1",
            voiceover_oss_object_id="VOICE1",
            tts_timeline=timeline,
            match_result=matched.data,
            beat_plan=timeline,
            hook_id="H1",
            primary_selling_point="拉链细节",
        )
        assert bridged.success, bridged.to_dict()
        saved = ctx.repo.get("render_plans", "render_plan_id", bridged.data["render_plan_id"])
        assert saved["content_mode"] == "voiceover"
        assert saved["match_plan_version"] == "voiceover-visual-match-core-v2-key-evidence"
        assert len(saved["plan_json"]["segments"]) == 2


def test_key_match_policy_keeps_only_hook_and_one_selling_point_as_hard_evidence():
    lines = [
        {"beat_id": "B1", "role": "hook", "required_shot_roles": ["detail_fabric"]},
        {"beat_id": "B2", "role": "proof", "required_shot_roles": ["detail_fabric"]},
        {"beat_id": "B3", "role": "proof", "required_shot_roles": ["detail_closure"]},
        {"beat_id": "B4", "role": "cta", "required_shot_roles": ["main_wear_upper"]},
    ]

    planned = apply_key_match_policy(lines, primary_selling_point="拉链设计")

    assert [row["match_priority"] for row in planned] == ["hook", "normal", "key", "ending"]
    assert [row["required_shot_roles"] for row in planned] == [["main_wear_upper"], [], ["detail_closure"], []]
    assert planned[1]["original_required_shot_roles"] == ["detail_fabric"]


def test_normal_voiceover_beat_uses_generic_material_without_evidence_gap():
    with tempfile.TemporaryDirectory() as directory:
        ctx = _context(Path(directory))
        ctx.repo.upsert("products", "product_id", {"product_id": "P1", "product_name": "Product"})
        for index, role in enumerate(("hero", "result"), start=1):
            asset_id = f"A{index}"
            segment_id = f"S{index}"
            ctx.repo.upsert("assets", "asset_id", {"asset_id": asset_id, "product_id": "P1", "asset_status": "ready", "has_watermark": "no"})
            ctx.repo.upsert("segments", "segment_id", {"segment_id": segment_id, "asset_id": asset_id, "product_id": "P1", "duration_ms": 2000, "segment_status": "qc_passed", "effective_roles_json": [role], "product_match_status": "trusted_by_source"})
            ctx.repo.insert("segment_tags", {"segment_id": segment_id, "primary_shot_role": role, "secondary_roles_json": [], "product_visibility": "high", "mixcut_usability": "yes", "risk_level": "low", "reason": "普通展示"})
        timeline = [
            {"beat_id": "B1", "role": "hook", "speech_text": "展示产品", "start_ms": 0, "end_ms": 1000},
            {"beat_id": "B2", "role": "proof", "speech_text": "面料舒服", "start_ms": 1000, "end_ms": 2000, "required_shot_roles": ["detail_fabric"]},
            {"beat_id": "B3", "role": "proof", "speech_text": "日常好搭", "start_ms": 2000, "end_ms": 3000, "required_shot_roles": ["detail_sleeve"]},
        ]

        matched = VoiceoverMaterialAdapterSkill(ctx).match("P1", timeline, 3000, primary_selling_point="面料质感")

        assert matched.success, matched.to_dict()
        assert matched.data["key_beat_id"] == "B2"
        assert len(matched.data["evidence_gaps"]) == 1
        assert matched.data["evidence_gaps"][0]["beat_id"] == "B2"
        assert all(gap["beat_id"] != "B3" for gap in matched.data["evidence_gaps"])


def test_voiceover_matcher_does_not_reuse_two_segments_from_the_same_asset():
    with tempfile.TemporaryDirectory() as directory:
        ctx = _context(Path(directory))
        ctx.repo.upsert("products", "product_id", {"product_id": "P1", "product_name": "Product"})
        for asset_id in ("A1", "A2"):
            ctx.repo.upsert(
                "assets",
                "asset_id",
                {
                    "asset_id": asset_id,
                    "product_id": "P1",
                    "asset_status": "ready",
                    "has_watermark": "no",
                    "source_flow": "light_video",
                },
            )
        for segment_id, asset_id, visual_phash in (("S1", "A1", "HASH1"), ("S2", "A1", "HASH2"), ("S3", "A2", "HASH3")):
            ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": segment_id,
                    "asset_id": asset_id,
                    "product_id": "P1",
                    "duration_ms": 1800,
                    "segment_status": "qc_passed",
                    "effective_roles_json": ["hero"],
                    "product_match_status": "trusted_by_source",
                    "visual_phash": visual_phash,
                },
            )
            ctx.repo.insert(
                "segment_tags",
                {
                    "segment_id": segment_id,
                    "primary_shot_role": "hero",
                    "secondary_roles_json": [],
                    "product_visibility": "high",
                    "mixcut_usability": "yes",
                    "risk_level": "low",
                    "reason": "完整上身展示",
                },
            )
        timeline = [
            {"beat_id": "B1", "role": "hook", "speech_text": "先看上身", "start_ms": 0, "end_ms": 1000},
            {"beat_id": "B2", "role": "proof", "speech_text": "日常展示", "start_ms": 1000, "end_ms": 2000},
            {"beat_id": "B3", "role": "decision", "speech_text": "收尾展示", "start_ms": 2000, "end_ms": 3000},
        ]
        matched = VoiceoverMaterialAdapterSkill(ctx).match("P1", timeline, 3000)
        assert matched.success, matched.to_dict()
        assert len(matched.data["clips"]) == 3
        assert len({clip["asset_id"] for clip in matched.data["clips"]}) == 2
        assert all(
            left["asset_id"] != right["asset_id"]
            for left, right in zip(matched.data["clips"], matched.data["clips"][1:])
        )
        assert any(row["reason"] == "material_reuse_fallback" for row in matched.data["match_warnings"])


def test_voiceover_intervals_use_the_longest_candidate_for_each_evidence_role():
    candidates = [
        {
            "segment_key": "FABRIC",
            "asset_id": "A1",
            "start_ms": 0,
            "end_ms": 3000,
            "primary_shot_role": "detail",
            "secondary_roles": ["detail_fabric"],
            "reason": "材质近景",
        },
        {
            "segment_key": "NECKLINE",
            "asset_id": "A2",
            "start_ms": 0,
            "end_ms": 4041,
            "primary_shot_role": "detail",
            "secondary_roles": ["detail_neckline"],
            "reason": "立领近景",
        },
    ]
    lines = [
        {"block_id": "B1", "speech_text": "材质", "start_ms": 0, "end_ms": 3292, "required_shot_roles": ["detail_fabric"]},
        {"block_id": "B2", "speech_text": "立领", "start_ms": 3292, "end_ms": 6402, "required_shot_roles": ["detail_neckline"]},
    ]

    intervals = voiceover_intervals(lines, 6402, candidates)

    assert [row["beat_id"] for row in intervals] == ["B1", "B1", "B2"]
    assert [row["duration_ms"] for row in intervals] == [1646, 1646, 3110]
