#!/usr/bin/env python3
"""Read-only audit of current NeoBund country music pools and OPV ranking."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
for value in (str(WORKSPACE_ROOT), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from repositories.rds_repository import RdsRepository  # noqa: E402
from services import neobund_music  # noqa: E402
from services import bgm_audio  # noqa: E402
from services.neobund_publisher import OpvPublishFlow  # noqa: E402
from services.neobund_wiring import build_opv_neobund  # noqa: E402


DEFAULT_THEMES = [
    "THEME_TH_PETITE_PROPORTION_V1",
    "THEME_TH_CAFE_DATE_V1",
    "THEME_TH_ONE_PIECE_MULTIWAY_V1",
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account-id", default="OPV_TH_TEST_001")
    parser.add_argument("--theme-id", action="append", default=[])
    parser.add_argument("--video-ms", type=int, default=10000)
    parser.add_argument(
        "--analyze-audio", action="store_true",
        help="download/cache candidate previews and rank using local beat analysis",
    )
    args = parser.parse_args()

    repository = RdsRepository.from_env()
    account = repository.get_account_profile(args.account_id)
    if account is None:
        raise RuntimeError(f"account not found: {args.account_id}")
    wiring = build_opv_neobund(repository=repository)
    flow = OpvPublishFlow(
        repository, adapter=wiring.adapter, music_source=wiring.music_source
    )
    use_counts = flow._recent_use_counts(account.account_id)
    rankings = []
    candidate_counts = {}
    for theme_id in args.theme_id or DEFAULT_THEMES:
        theme = repository.get_theme(theme_id)
        if theme is None:
            continue
        hints = list(theme.content_plan_rules_json.get("bgm_mood_hints") or [])
        candidates = wiring.music_source.fetch(
            account.account_id,
            account.target_country,
            language=account.default_locale,
            mood_hints=hints,
        )
        profile = neobund_music.derive_content_profile(
            f"{theme_id} {theme.theme_name} {theme.content_plan_rules_json}", hints
        )
        if args.analyze_audio and profile["rhythm_preference"] == "strong":
            candidates = bgm_audio.enrich_candidates(candidates)
        candidate_counts[theme_id] = len(candidates)
        top = neobund_music.select_top(
            candidates,
            mood_hints=hints,
            video_duration_ms=args.video_ms,
            use_counts=use_counts,
            top_n=3,
            rhythm_preference=profile["rhythm_preference"],
            require_audio_analysis=(
                args.analyze_audio and profile["rhythm_preference"] == "strong"
            ),
        )
        rankings.append({
            "theme_id": theme_id,
            "mood_hints": hints,
            "profile": profile,
            "audio_analyzed": bool(args.analyze_audio),
            "top": [
                {
                    "music_id": item.music_id,
                    "title": item.title,
                    "score": score,
                    "pool": item.raw.get("pool"),
                    "pool_rank": item.raw.get("pool_rank"),
                    "mood_tags": list(item.mood_tags),
                    "recent_uses": int(use_counts.get(item.music_id, 0)),
                    "audio_analysis": item.raw.get("audio_analysis") or {},
                }
                for item, score in top
            ],
        })
    print(json.dumps({
        "mode": "read_only",
        "account_id": account.account_id,
        "country": account.target_country,
        "locale": account.default_locale,
        "candidate_counts": candidate_counts,
        "rankings": rankings,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
