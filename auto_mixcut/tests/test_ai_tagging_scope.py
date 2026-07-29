from auto_mixcut.skills.ai_tagging_skill import _apply_tag_scope, _normalize_tag_scope
from auto_mixcut.skills.render_plan_skill import _core_segment_reuse_cap


def test_asset_representative_selects_one_untagged_segment_per_asset():
    segments = [
        {"segment_id": "A2", "asset_id": "A", "start_ms": 3000},
        {"segment_id": "A1", "asset_id": "A", "start_ms": 0},
        {"segment_id": "B1", "asset_id": "B", "start_ms": 0},
        {"segment_id": "B2", "asset_id": "B", "start_ms": 3000},
    ]

    selected = _apply_tag_scope(
        segments,
        {"A2": {"segment_id": "A2"}},
        scope="asset_representative",
        force=False,
    )

    assert [item["segment_id"] for item in selected] == ["B1"]


def test_asset_representative_force_selects_first_segment_for_every_asset():
    segments = [
        {"segment_id": "A2", "asset_id": "A", "start_ms": 3000},
        {"segment_id": "A1", "asset_id": "A", "start_ms": 0},
        {"segment_id": "B1", "asset_id": "B", "start_ms": 0},
    ]

    selected = _apply_tag_scope(
        segments,
        {"A2": {"segment_id": "A2"}},
        scope="asset_representative",
        force=True,
    )

    assert [item["segment_id"] for item in selected] == ["A1", "B1"]
    assert _normalize_tag_scope("asset") == "asset_representative"
    assert _normalize_tag_scope("anything_else") == "all_segments"


def test_core_segments_are_unique_until_fill_mode_is_explicitly_enabled():
    assert _core_segment_reuse_cap({"reuse_mode": "strict"}) == 1
    assert _core_segment_reuse_cap({"reuse_mode": "fill_target"}) > 1
