from auto_mixcut.skills.segment_skill import _segment_windows


def test_short_generated_video_keeps_its_full_duration():
    assert _segment_windows(4042, "video") == [(0, 4042)]


def test_image_keeps_the_existing_three_second_policy():
    assert _segment_windows(5000, "image") == [(0, 3000)]


def test_long_video_still_uses_regular_windows():
    assert _segment_windows(8000, "video") == [(300, 3300), (3300, 6300)]
